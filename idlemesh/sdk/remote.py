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
import ipaddress
import logging
import socket
import time
import uuid
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import zmq

from idlemesh._mp import pool_context
from idlemesh.coordinator.scheduler import NodeInfo
from idlemesh.protocol import (
    PROTOCOL_VERSION,
    Chunk,
    Codec,
    Message,
    MsgType,
    RemoteError,
    dumps,
    loads,
    parse_node,
    python_version,
    split,
)
from idlemesh.sdk.discover import protocol_mismatch, python_mismatch

if TYPE_CHECKING:
    from idlemesh.sdk import Mesh
    from idlemesh.sdk.discover import Discovery

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
    name: str = ""  # the agent's real name; only known after the signed PONG
    node_id: str = ""
    pings: dict[str, float] = field(default_factory=dict)  # ping id -> sent at
    inflight: dict[str, float] = field(default_factory=dict)  # chunk_id -> sent at

    @property
    def label(self) -> str:
        return f"{self.name} ({self.addr})" if self.name else self.addr


def _run_serialized(blob: bytes) -> list[Any]:
    chunk: Chunk = loads(blob)
    return chunk.run()


def _explicit_keys(nodes: list[str]) -> set[str]:
    """Explicit node addresses, plus their resolved IPs, for de-duplicating discoveries."""
    keys = set(nodes)
    for addr in nodes:
        host, port = parse_node(addr)
        try:
            ipaddress.IPv4Address(host)
        except ValueError:
            try:
                keys.add(f"{socket.gethostbyname(host)}:{port}")
            except OSError:
                pass
    return keys


class RemoteRun:
    def __init__(
        self,
        mesh: Mesh,
        discovery: Discovery | None = None,
        on_progress: Callable[[int, int, dict[str, int]], None] | None = None,
    ) -> None:
        self.mesh = mesh
        self.discovery = discovery
        self.on_progress = on_progress
        self.total = 0
        self.queue = mesh.queue
        self.codec = Codec(mesh.secret)
        self.ctx = zmq.Context.instance()
        self.est_runtime_s = 1.0  # learned from local chunk timings
        self.nodes: list[_Node] = []
        self.local: dict[Future, tuple[str, float]] = {}  # future -> (chunk_id, started)
        self.stats: dict[str, int] = {}
        self._explicit = _explicit_keys(mesh.nodes) if discovery else set(mesh.nodes)
        self._tried: dict[str, int] = {}  # discovered addr -> advert generation we dialled

    # -- setup / teardown --------------------------------------------------------
    def _connect(self) -> None:
        now = time.monotonic()
        for addr in self.mesh.nodes:
            self._open(addr, now)
        self._add_discovered(now)

    def _open(self, addr: str, now: float) -> _Node:
        sock = self.ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.connect(f"tcp://{addr}")
        node = _Node(addr, sock, started=now, last_heard=now)
        self.nodes.append(node)
        self._ping(node, now)
        return node

    def _add_discovered(self, now: float) -> None:
        """Dial agents discovery found since the last step (including mid-job arrivals).
        An address that failed is only retried after its advertisement reappears."""
        if self.discovery is None:
            return
        alive = {n.addr for n in self.nodes if n.alive}
        for cand in self.discovery.candidates():
            addr = cand.addr
            if addr in self._explicit or addr in alive or self._tried.get(addr) == cand.gen:
                continue
            self._tried[addr] = cand.gen
            node = self._open(addr, now)
            node.node_id = cand.advert.node_id
            log.info("discovered agent %s at %s", cand.advert.node_id, addr)

    def _close(self, node: _Node) -> None:
        node.alive = False
        node.sock.close()

    def _ping(self, node: _Node, now: float) -> None:
        pid = "ping-" + uuid.uuid4().hex[:8]
        node.pings[pid] = now
        node.last_ping = now
        hello = {"python": python_version(), "protocol": PROTOCOL_VERSION}
        node.sock.send_multipart(self.codec.encode(MsgType.PING, pid, dumps(hello)))

    def _release(self, node: _Node, why: str) -> None:
        n = self.queue.release_node(node.addr)
        node.inflight.clear()
        log.warning("node %s %s; requeued %d chunk(s)", node.label, why, n)
        self._close(node)

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self.mesh._warned:
            self.mesh._warned.add(key)
            log.warning(message)

    # -- main entry --------------------------------------------------------------
    def map(self, fn: Callable[[Any], Any], inputs: list[Any], chunk_size: int) -> list[Any]:
        job_id = uuid.uuid4().hex[:8]
        chunks = split(job_id, fn, list(inputs), chunk_size)
        self.total = len(chunks)
        self.queue.submit(chunks)
        ctx = self.mesh.mp_context or pool_context()
        pool = ProcessPoolExecutor(max_workers=self.mesh.workers, mp_context=ctx)
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

    def _count(self, who: str) -> None:
        self.stats[who] = self.stats.get(who, 0) + 1
        if self.on_progress is not None:
            self.on_progress(sum(self.stats.values()), self.total, dict(self.stats))

    def _step(self, pool: ProcessPoolExecutor) -> None:
        now = time.monotonic()
        self._add_discovered(now)
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
                self._count("local")

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
            if not node.ready and not self._handshake(node, info):
                return
            if not info.get("lendable", True):
                node.paused_until = now + REFUSED_BACKOFF_S
        elif msg.type is MsgType.RESULT:
            if node.inflight.pop(msg.chunk_id, None) is None:
                return
            if self.queue.complete(msg.chunk_id, loads(msg.payload), node_id=node.addr):
                self._count(node.addr)
        elif msg.type is MsgType.ERROR:
            if msg.chunk_id not in node.inflight:
                return
            err = loads(msg.payload)
            if err.get("kind") == "refused":
                # Owner is using the machine: not a job failure. Back off and rerun elsewhere.
                self._release_refused(node, now)
                return
            self._raise_remote(node, msg.chunk_id, err)

    def _handshake(self, node: _Node, info: dict) -> bool:
        """First PONG: check versions, learn the agent's name. False = node not used."""
        node.name = str(info.get("name") or "")
        node.node_id = str(info.get("node_id") or node.node_id)
        protocol = int(info.get("protocol", 1))  # Phase 1 agents didn't send one
        theirs = str(info.get("python", ""))
        if protocol != PROTOCOL_VERSION:
            self._warn_once(f"{node.addr}:proto", protocol_mismatch(node.label, protocol))
            self._close(node)
            return False
        if theirs != python_version():
            self._warn_once(f"{node.addr}:py:{theirs}", python_mismatch(node.label, theirs))
            self._close(node)
            return False
        port = node.addr.rsplit(":", 1)[1]
        for other in self.nodes:
            same_agent = other.node_id == node.node_id and other.addr.rsplit(":", 1)[1] == port
            if other is not node and other.alive and other.ready and same_agent:
                log.info("%s is the same agent as %s; using one connection", node.addr, other.addr)
                self._close(node)
                return False
        node.ready = True
        node.workers = max(int(info.get("workers", 1)), 1)
        node.capacity = self.mesh.inflight_per_node or node.workers + 1
        log.info("node %s ready (%d workers)", node.label, node.workers)
        return True

    def _release_refused(self, node: _Node, now: float) -> None:
        n = self.queue.release_node(node.addr)
        node.inflight.clear()
        node.paused_until = now + REFUSED_BACKOFF_S
        log.info("node %s is busy; requeued %d chunk(s), retrying it later", node.label, n)

    def _raise_remote(self, node: _Node, chunk_id: str, err: dict) -> None:
        cause = None
        if err.get("exc"):
            try:
                cause = loads(err["exc"])
            except Exception:  # exception type not importable here; the traceback still is
                cause = None
        tb = err.get("traceback", "")
        message = f"chunk {chunk_id} failed on {node.label}: {err.get('message', '')}"
        if tb:
            message += f"\n\nRemote traceback:\n{tb}"
        raise RemoteError(message, tb) from cause

    def _check_health(self, now: float) -> None:
        for node in self.nodes:
            if not node.alive:
                continue
            if not node.ready:
                if now - node.started > self.mesh.handshake_timeout:
                    who = f"agent {node.node_id} at {node.addr}" if node.node_id else node.addr
                    log.warning(
                        "node %s did not answer the handshake (unreachable, not running "
                        "`idlemesh serve`, or a different secret); running without it",
                        who,
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
