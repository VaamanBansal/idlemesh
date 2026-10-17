"""User-facing API.

    mesh = Mesh()                                        # local worker processes only
    mesh = Mesh(discover=True)                           # + agents found on the LAN
    mesh = Mesh(nodes=["192.168.1.20:7450"], secret=s)   # + agents at known addresses
    results = mesh.map(price_path, seeds, chunk_size=1000)

With nodes, chunks run on the remote agents and the local process pool at the
same time, all fed from one ChunkQueue; results come back in input order.
"""

from __future__ import annotations

import os
import uuid
import weakref
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from idlemesh.coordinator.queue import ChunkQueue
from idlemesh.coordinator.scheduler import Scheduler
from idlemesh.protocol import Chunk, dumps, loads, parse_node, split

if TYPE_CHECKING:
    from idlemesh.agent.discovery import Browser
    from idlemesh.sdk.discover import Discovery


ProgressCallback = Callable[[int, int, dict[str, int]], None]  # (done, total, per_node)


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
        discover: bool = False,
        mesh: str | None = None,
        discover_timeout: float = 2.0,
        browser: Browser | None = None,
    ) -> None:
        """
        nodes: "host[:port]" of agents started with `idlemesh serve` (port defaults to 7450).
        discover: also find agents on the LAN over mDNS, before and during each map().
        mesh: only use discovered agents in this mesh (default: $IDLEMESH_MESH or "default").
        discover_timeout: how long the first map() browses before it starts.
        browser: custom advertisement source (tests); defaults to zeroconf.
        secret: shared mesh secret; defaults to the IDLEMESH_SECRET env var.
        inflight_per_node: chunks queued on each node (default: its workers + 1).
        node_timeout: release a node's chunks after this long without any reply.
        handshake_timeout: give up on a node that hasn't answered the first PING.
        chunk_timeout: optionally release a node holding one chunk longer than this.
        remote_slowdown_limit: skip a node whose estimated cost exceeds local by this factor.
        """
        self.workers = workers or os.cpu_count() or 2
        self.queue = ChunkQueue()
        self.nodes = [f"{h}:{p}" for h, p in (parse_node(n) for n in (nodes or []))]
        self.discover = discover
        self.mesh_name = mesh or os.environ.get("IDLEMESH_MESH") or "default"
        self.discover_timeout = discover_timeout
        self._browser = browser
        self._discovery: Discovery | None = None
        self._warned: set[str] = set()  # handshake warnings already shown by this Mesh
        self.secret = secret or os.environ.get("IDLEMESH_SECRET")
        if (self.nodes or discover) and not self.secret:
            raise ValueError("remote nodes need a shared secret: secret=... or IDLEMESH_SECRET")
        self.inflight_per_node = inflight_per_node
        self.node_timeout = node_timeout
        self.handshake_timeout = handshake_timeout
        self.ping_interval = ping_interval
        self.chunk_timeout = chunk_timeout
        self.scheduler = scheduler or Scheduler()
        self.remote_slowdown_limit = remote_slowdown_limit
        self.last_stats: dict[str, int] = {}  # node -> chunks it completed in the last map()

    def discovery(self) -> Discovery:
        """The LAN browser, started on first use and kept running until close()."""
        if self._discovery is None:
            from idlemesh.sdk.discover import Discovery

            browser = self._browser
            if browser is None:
                try:
                    import zeroconf  # noqa: F401
                except ImportError as err:
                    raise ImportError("discovery needs: pip install 'idlemesh[net]'") from err
                from idlemesh.agent.discovery import ZeroconfBrowser

                browser = ZeroconfBrowser()
            self._discovery = Discovery(browser, self.mesh_name)
            weakref.finalize(self, self._discovery.stop)
        self._discovery.ensure_started(self.discover_timeout)
        return self._discovery

    def close(self) -> None:
        """Stop LAN discovery (only needed with discover=True)."""
        if self._discovery is not None:
            self._discovery.stop()

    def __enter__(self) -> Mesh:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def map(
        self,
        fn: Callable[[Any], Any],
        inputs: list[Any],
        chunk_size: int = 100,
        on_progress: ProgressCallback | None = None,
    ) -> list[Any]:
        """Apply `fn` to every input; results come back in input order.

        on_progress(done, total, per_node) is called after each chunk completes, with
        chunk counts; per_node maps "local" or a node's "host:port" to chunks it ran.
        """
        if self.nodes or self.discover:
            try:
                from idlemesh.sdk.remote import RemoteRun
            except ImportError as err:
                raise ImportError("remote nodes need pyzmq: pip install 'idlemesh[net]'") from err
            discovery = self.discovery() if self.discover else None
            return RemoteRun(self, discovery, on_progress).map(fn, inputs, chunk_size)
        job_id = uuid.uuid4().hex[:8]
        chunks = split(job_id, fn, list(inputs), chunk_size)
        self.queue.submit(chunks)
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            futures = {}
            while (c := self.queue.lease("local")) is not None:
                futures[c.chunk_id] = pool.submit(_run_serialized, dumps(c))
            if on_progress is None:
                for cid, fut in futures.items():
                    self.queue.complete(cid, fut.result())
            else:
                cids = {fut: cid for cid, fut in futures.items()}
                for done, fut in enumerate(as_completed(cids), start=1):
                    self.queue.complete(cids[fut], fut.result())
                    on_progress(done, len(chunks), {"local": done})
        return self.queue.gather(job_id)


def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a function as mesh-runnable. Adds `fn.map(inputs, ...)`."""

    def _map(inputs, chunk_size: int = 100, mesh: Mesh | None = None):
        return (mesh or Mesh()).map(fn, inputs, chunk_size=chunk_size)

    fn.map = _map  # type: ignore[attr-defined]
    return fn
