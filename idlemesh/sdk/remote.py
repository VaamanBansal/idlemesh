"""Coordinator side of remote execution: local pool + remote agents, one queue.

One event loop drives everything:
  1. keep the local process pool full (it is just another worker),
  2. keep each live node at `inflight_per_node` chunks, unless the scheduler says
     that node is clearly slower than running the chunk locally,
  3. poll the DEALER sockets and the local futures,
  4. ping nodes with work in flight; a node silent for `node_timeout` (or with a
     chunk older than `chunk_timeout`) is released, and its chunks are requeued.
A remote ERROR fails the whole job with the agent-side traceback (no retry).
"""

from __future__ import annotations

import dataclasses
import logging
import time
import uuid
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import zmq

from idlemesh.coordinator.scheduler import NodeInfo
from idlemesh.protocol import Chunk, Codec, Message, MsgType, RemoteError, dumps, loads, split

if TYPE_CHECKING:
    from idlemesh.sdk import Mesh

log = logging.getLogger(__name__)

REFUSED_BACKOFF_S = 10.0
EWMA = 0.3


@dataclass
class _Node:
    addr: str
    sock: zmq.Socket
    started: float
    ready: bool = False  # answered the handshake PING
    alive: bool = True
    workers: int = 1
    capacity: int = 1
    rtt_s: float = 0.005
    last_heard: float = 0.0
    last_ping: float = 0.0
    paused_until: float = 0.0
    pings: dict[str, float] = field(default_factory=dict)  # ping id -> sent at
    inflight: dict[str, float] = field(default_factory=dict)  # chunk_id -> sent at


def _run_serialized(blob: bytes) -> list[Any]:
    chunk: Chunk = loads(blob)
    return chunk.run()


class RemoteRun:
    def __init__(self, mesh: Mesh) -> None:
        self.mesh = mesh
        self.queue = mesh.queue
        self.codec = Codec(mesh.secret)
        self.ctx = zmq.Context.instance()
        self.est_runtime_s = 1.0  # learned from local chunk timings
        self.nodes: list[_Node] = []
        self.local: dict[Future, tuple[str, float]] = {}  # future -> (chunk_id, started)
        self.stats: dict[str, int] = {}

    # -- setup / teardown --------------------------------------------------------
    def _connect(self) -> None:
        now = time.monotonic()
        for addr in self.mesh.nodes:
            sock = self.ctx.socket(zmq.DEALER)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(f"tcp://{addr}")
            node = _Node(addr, sock, started=now, last_heard=now)
            self.nodes.append(node)
            self._ping(node, now)

    def _close(self, node: _Node) -> None:
        node.alive = False
        node.sock.close()

    def _ping(self, node: _Node, now: float) -> None:
        pid = "ping-" + uuid.uuid4().hex[:8]
        node.pings[pid] = now
        node.last_ping = now
        node.sock.send_multipart(self.codec.encode(MsgType.PING, pid))

    def _release(self, node: _Node, why: str) -> None:
        n = self.queue.release_node(node.addr)
        node.inflight.clear()
        log.warning("node %s %s; requeued %d chunk(s)", node.addr, why, n)
        self._close(node)

    # -- main entry --------------------------------------------------------------
    def map(self, fn: Callable[[Any], Any], inputs: list[Any], chunk_size: int) -> list[Any]:
        job_id = uuid.uuid4().hex[:8]
        chunks = split(job_id, fn, list(inputs), chunk_size)
        self.queue.submit(chunks)
        pool = ProcessPoolExecutor(max_workers=self.mesh.workers)
        try:
            self._connect()
            while not self.queue.is_done(job_id, len(chunks)):
                self._step(pool)
            results = self.queue.gather(job_id)
        finally:
            for node in self.nodes:
                if node.alive:
                    self._close(node)
            pool.shutdown(wait=False, cancel_futures=True)
            self.queue.drop_job(job_id)
            self.mesh.last_stats = dict(self.stats)
        return results

    def _step(self, pool: ProcessPoolExecutor) -> None:
        now = time.monotonic()
        self._fill_local(pool)
        self._fill_remote(now)
        live = [n for n in self.nodes if n.alive]
        if live:
            poller = zmq.Poller()
            for n in live:
                poller.register(n.sock, zmq.POLLIN)
            for sock, _ in poller.poll(timeout=10):
                node = next(n for n in live if n.sock is sock)
                self._drain(node)
        elif self.local:
            wait(list(self.local), timeout=0.05, return_when=FIRST_COMPLETED)
        else:
            time.sleep(0.01)
        self._reap_local()
        self._check_health(time.monotonic())

    # -- local worker ------------------------------------------------------------
    def _fill_local(self, pool: ProcessPoolExecutor) -> None:
        while len(self.local) < self.mesh.workers:
            c = self.queue.lease("local")
            if c is None:
                return
            self.local[pool.submit(_run_serialized, dumps(c))] = (c.chunk_id, time.monotonic())

    def _reap_local(self) -> None:
        for fut in [f for f in self.local if f.done()]:
            cid, started = self.local.pop(fut)
            result = fut.result()  # a local exception propagates, as in local-only mode
            self.est_runtime_s += EWMA * ((time.monotonic() - started) - self.est_runtime_s)
            if self.queue.complete(cid, result, node_id="local"):
                self.stats["local"] = self.stats.get("local", 0) + 1

    # -- remote workers ----------------------------------------------------------
    def _worth_sending(self, chunk: Chunk, node: _Node) -> bool:
        """Skip a node when its estimated cost is clearly worse than running locally."""
        probe = dataclasses.replace(chunk, est_runtime_s=self.est_runtime_s)
        backlog = self.queue.pending_count() / max(self.mesh.workers, 1)
        local = NodeInfo("local", is_local=True, queue_wait_s=backlog * self.est_runtime_s)
        queued = max(0, len(node.inflight) - node.workers + 1) / node.workers
        remote = NodeInfo(node.addr, latency_s=node.rtt_s, queue_wait_s=queued * self.est_runtime_s)
        sched = self.mesh.scheduler
        limit = self.mesh.remote_slowdown_limit
        return sched.expected_cost(probe, remote) <= limit * sched.expected_cost(probe, local)

    def _fill_remote(self, now: float) -> None:
        for node in self.nodes:
            if not (node.alive and node.ready) or now < node.paused_until:
                continue
            while len(node.inflight) < node.capacity:
                nxt = self.queue.peek()
                if nxt is None or not self._worth_sending(nxt, node):
                    break
                c = self.queue.lease(node.addr)
                assert c is not None
                node.inflight[c.chunk_id] = now
                node.sock.send_multipart(self.codec.encode(MsgType.RUN, c.chunk_id, dumps(c)))

    def _drain(self, node: _Node) -> None:
        while node.alive:
            try:
                frames = node.sock.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            msg = self.codec.decode(frames)  # HMAC + age + nonce checked BEFORE unpickling
            if msg is not None:
                self._on_message(node, msg)

    def _on_message(self, node: _Node, msg: Message) -> None:
        now = time.monotonic()
        node.last_heard = now
        if msg.type is MsgType.PONG:
            sent = node.pings.pop(msg.chunk_id, None)
            if sent is None:
                return
            node.rtt_s += EWMA * ((now - sent) - node.rtt_s) if node.ready else (now - sent)
            info = loads(msg.payload)
            if not node.ready:
                node.ready = True
                node.workers = max(int(info.get("workers", 1)), 1)
                node.capacity = self.mesh.inflight_per_node or node.workers + 1
                log.info("node %s ready (%d workers)", node.addr, node.workers)
            if not info.get("lendable", True):
                node.paused_until = now + REFUSED_BACKOFF_S
        elif msg.type is MsgType.RESULT:
            if node.inflight.pop(msg.chunk_id, None) is None:
                return
            if self.queue.complete(msg.chunk_id, loads(msg.payload), node_id=node.addr):
                self.stats[node.addr] = self.stats.get(node.addr, 0) + 1
        elif msg.type is MsgType.ERROR:
            if msg.chunk_id not in node.inflight:
                return
            err = loads(msg.payload)
            if err.get("kind") == "refused":
                # Owner is using the machine: not a job failure. Back off and rerun elsewhere.
                self._release_refused(node, now)
                return
            self._raise_remote(node, msg.chunk_id, err)

    def _release_refused(self, node: _Node, now: float) -> None:
        n = self.queue.release_node(node.addr)
        node.inflight.clear()
        node.paused_until = now + REFUSED_BACKOFF_S
        log.info("node %s is busy; requeued %d chunk(s), retrying it later", node.addr, n)

    def _raise_remote(self, node: _Node, chunk_id: str, err: dict) -> None:
        cause = None
        if err.get("exc"):
            try:
                cause = loads(err["exc"])
            except Exception:  # exception type not importable here; the traceback still is
                cause = None
        tb = err.get("traceback", "")
        message = f"chunk {chunk_id} failed on {node.addr}: {err.get('message', '')}"
        if tb:
            message += f"\n\nRemote traceback:\n{tb}"
        raise RemoteError(message, tb) from cause

    def _check_health(self, now: float) -> None:
        for node in self.nodes:
            if not node.alive:
                continue
            if not node.ready:
                if now - node.started > self.mesh.handshake_timeout:
                    log.warning(
                        "node %s did not answer the handshake (unreachable, not running "
                        "`idlemesh serve`, or a different secret); running without it",
                        node.addr,
                    )
                    self._close(node)
                continue
            if not node.inflight:
                continue
            if now - node.last_heard > self.mesh.node_timeout:
                self._release(node, f"silent for {now - node.last_heard:.1f}s")
                continue
            timeout = self.mesh.chunk_timeout
            if timeout is not None and now - min(node.inflight.values()) > timeout:
                self._release(node, f"has held a chunk longer than {timeout:.0f}s")
                continue
            if now - node.last_ping > self.mesh.ping_interval:
                self._ping(node, now)
