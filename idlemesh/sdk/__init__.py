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

import psutil

from idlemesh.coordinator.queue import ChunkQueue
from idlemesh.coordinator.scheduler import Scheduler
from idlemesh.protocol import Chunk, dumps, format_bytes, loads, parse_memory, parse_node, split
from idlemesh.sdk.stats import RunStats

if TYPE_CHECKING:
    from multiprocessing.context import BaseContext

    from idlemesh.agent.discovery import Browser
    from idlemesh.sdk.discover import Discovery


ProgressCallback = Callable[[int, int, RunStats], None]  # (done, total, per_node)


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
        mp_context: BaseContext | None = None,
        speculative: bool = True,
    ) -> None:
        """
        mp_context: multiprocessing context for the local worker pool. None keeps
            Python's default for local-only runs (remote runs always use a fork-safe
            one); pass idlemesh._mp.pool_context() when calling from a threaded process.
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
        speculative: near the end of a job, run backup copies of slow remote chunks on idle
            workers and keep whichever finishes first. Turn it off (False) if your function
            has side effects (writes files, sends emails, charges cards): with backups, a
            chunk can run twice.
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
        self.mp_context = mp_context
        self.speculative = speculative
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
        cost: Callable[[Any], float] | None = None,
        memory: int | str | None = None,
    ) -> list[Any]:
        """Apply `fn` to every input; results come back in input order.

        on_progress(done, total, per_node) is called after each chunk completes, with
        chunk counts; per_node (a RunStats) maps "local" or a node's "host:port" to the
        chunks it ran, and carries backups and measured seconds per chunk.
        cost(input) -> float estimates how expensive an input is relative to others;
            the most expensive chunks are started first, so long tasks don't finish last.
        memory: RAM one task needs ("512MB", "4GB" or bytes). Each laptop only runs as
            many tasks at once as fit in its free RAM; if none can fit one, map() raises
            MemoryError before running anything.
        """
        need = parse_memory(memory)
        job_id = uuid.uuid4().hex[:8]
        items = list(inputs)
        chunks = split(job_id, fn, items, chunk_size)
        if cost is not None:
            for c in chunks:
                c.cost = max(float(sum(cost(x) for x in c.inputs)), 1e-9)
            chunks.sort(key=lambda c: c.cost, reverse=True)  # .index keeps input order
        if self.nodes or self.discover:
            try:
                from idlemesh.sdk.remote import RemoteRun
            except ImportError as err:
                raise ImportError("remote nodes need pyzmq: pip install 'idlemesh[net]'") from err
            discovery = self.discovery() if self.discover else None
            return RemoteRun(self, discovery, on_progress, memory=need).map(chunks, job_id)
        workers = self.workers
        if need:
            free = psutil.virtual_memory().available
            workers = min(workers, free // need)
            if workers < 1:
                raise MemoryError(
                    f"This laptop can't fit one task needing {format_bytes(need)}: "
                    f"{format_bytes(free)} free. Lower memory= or free some RAM."
                )
        self.queue.submit(chunks)
        with ProcessPoolExecutor(max_workers=workers, mp_context=self.mp_context) as pool:
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
                    on_progress(done, len(chunks), RunStats({"local": done}))
        return self.queue.gather(job_id)


def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a function as mesh-runnable. Adds `fn.map(inputs, ...)`."""

    def _map(inputs, chunk_size: int = 100, mesh: Mesh | None = None):
        return (mesh or Mesh()).map(fn, inputs, chunk_size=chunk_size)

    fn.map = _map  # type: ignore[attr-defined]
    return fn
