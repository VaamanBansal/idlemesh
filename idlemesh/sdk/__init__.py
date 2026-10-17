"""User-facing API.

    mesh = Mesh()                                        # local worker processes only
    mesh = Mesh(nodes=["192.168.1.20:7450"], secret=s)   # plus agents on other laptops
    results = mesh.map(price_path, seeds, chunk_size=1000)

With nodes, chunks run on the remote agents and the local process pool at the
same time, all fed from one ChunkQueue; results come back in input order.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from idlemesh.coordinator.queue import ChunkQueue
from idlemesh.coordinator.scheduler import Scheduler
from idlemesh.protocol import Chunk, dumps, loads, split


def _run_serialized(blob: bytes) -> list[Any]:
    chunk: Chunk = loads(blob)
    return chunk.run()


class Mesh:
    def __init__(
        self,
        workers: int | None = None,
        nodes: list[str] | None = None,
        secret: str | None = None,
        inflight_per_node: int | None = None,
        node_timeout: float = 10.0,
        handshake_timeout: float = 3.0,
        ping_interval: float = 1.0,
        chunk_timeout: float | None = None,
        scheduler: Scheduler | None = None,
        remote_slowdown_limit: float = 2.0,
    ) -> None:
        """
        nodes: "host:port" of agents started with `idlemesh serve`.
        secret: shared mesh secret; defaults to the IDLEMESH_SECRET env var.
        inflight_per_node: chunks queued on each node (default: its workers + 1).
        node_timeout: release a node's chunks after this long without any reply.
        handshake_timeout: give up on a node that hasn't answered the first PING.
        chunk_timeout: optionally release a node holding one chunk longer than this.
        remote_slowdown_limit: skip a node whose estimated cost exceeds local by this factor.
        """
        self.workers = workers or os.cpu_count() or 2
        self.queue = ChunkQueue()
        self.nodes = list(nodes or [])
        self.secret = secret or os.environ.get("IDLEMESH_SECRET")
        if self.nodes and not self.secret:
            raise ValueError("remote nodes need a shared secret: secret=... or IDLEMESH_SECRET")
        self.inflight_per_node = inflight_per_node
        self.node_timeout = node_timeout
        self.handshake_timeout = handshake_timeout
        self.ping_interval = ping_interval
        self.chunk_timeout = chunk_timeout
        self.scheduler = scheduler or Scheduler()
        self.remote_slowdown_limit = remote_slowdown_limit
        self.last_stats: dict[str, int] = {}  # node -> chunks it completed in the last map()

    def map(self, fn: Callable[[Any], Any], inputs: list[Any], chunk_size: int = 100) -> list[Any]:
        if self.nodes:
            try:
                from idlemesh.sdk.remote import RemoteRun
            except ImportError as err:
                raise ImportError("remote nodes need pyzmq: pip install 'idlemesh[net]'") from err
            return RemoteRun(self).map(fn, inputs, chunk_size)
        job_id = uuid.uuid4().hex[:8]
        chunks = split(job_id, fn, list(inputs), chunk_size)
        self.queue.submit(chunks)
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            futures = {}
            while (c := self.queue.lease("local")) is not None:
                futures[c.chunk_id] = pool.submit(_run_serialized, dumps(c))
            for cid, fut in futures.items():
                self.queue.complete(cid, fut.result())
        return self.queue.gather(job_id)


def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a function as mesh-runnable. Adds `fn.map(inputs, ...)`."""

    def _map(inputs, chunk_size: int = 100, mesh: Mesh | None = None):
        return (mesh or Mesh()).map(fn, inputs, chunk_size=chunk_size)

    fn.map = _map  # type: ignore[attr-defined]
    return fn
