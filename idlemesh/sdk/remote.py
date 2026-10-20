"""Coordinator side of remote execution: local pool + remote agents, one queue.

One event loop drives everything. Each step it:
  1. connects agents found by discovery,
  2. keeps the local process pool full (it is just another worker), within RAM,
  3. sends each live node work: batches sized so network overhead stays around 10%
     of compute, a prefetch beyond its worker count only while there's plenty of
     work, and near the end of the job only chunks it is expected to finish sooner
     than this laptop would (judged with the p90 round trip),
  4. when nothing is pending and local workers are idle, starts backup copies of
     the most overdue remote chunks; the first result wins,
  5. polls sockets and local futures, pings every node, and releases nodes that
     go silent (their chunks are requeued unless a backup is still running).

A chunk's ERROR fails the job with the agent-side traceback, unless another copy
of that chunk already succeeded.
"""

from __future__ import annotations

import dataclasses
import ipaddress
import logging
import math
import socket
import time
import uuid
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import psutil
import zmq

from idlemesh._mp import pool_context, terminate_pool
from idlemesh.coordinator.scheduler import NodeInfo
from idlemesh.coordinator.speed import BandwidthTracker, RttTracker, SpeedTracker
from idlemesh.protocol import (
    PROTOCOL_VERSION,
    Chunk,
    Codec,
    Message,
    MsgType,
    RemoteError,
    dumps,
    format_bytes,
    loads,
    pack_run,
    parse_node,
    python_version,
    unpack_results,
)
from idlemesh.sdk.discover import protocol_mismatch, python_mismatch
from idlemesh.sdk.stats import RunStats

if TYPE_CHECKING:
    from idlemesh.sdk import Mesh
    from idlemesh.sdk.discover import Discovery

log = logging.getLogger(__name__)

REFUSED_BACKOFF_S = 10.0
BUSY_BACKOFF_S = 5.0
DEFAULT_SECONDS_PER_UNIT = 1.0  # before anything is measured (the old fixed estimate)
TARGET_OVERHEAD = 0.10  # aim for network time <= 10% of compute time
MAX_BATCH = 32
MEMORY_RECHECK_S = 1.0
NO_REPLY = "no reply (wrong secret, port blocked, or not running `idlemesh serve`)"
NOT_LENDABLE = "not lendable at start of run (busy, or on battery without force)"
NO_RAM = "busy: not enough free RAM"
LOST_TO_BACKUPS = "its chunks were finished first by backup copies"
NOT_NEEDED = "not needed: other laptops could finish every chunk sooner"
STILL_CONNECTING = "joined after the run finished (still connecting)"
FAST_TOLERANCE = 0.15  # laptops within 15% of the fastest count as "fast" for placement


@dataclass
class _Sent:
    batch: str
    sent: float  # monotonic
    backup: bool = False


@dataclass
class _Batch:
    chunk_ids: list[str]
    sent: float  # perf_counter
    nbytes: int


@dataclass
class _Node:
    addr: str
    sock: zmq.Socket
    started: float
    ready: bool = False  # answered the handshake PING
    alive: bool = True
    workers: int = 1
    last_heard: float = 0.0
    last_ping: float = 0.0
    paused_until: float = 0.0  # refused (owner busy) or BUSY (no RAM): back off until then
    name: str = ""  # the agent's real name; only known after the signed PONG
    node_id: str = ""
    ram_total: int | None = None
    ram_available: int | None = None
    mem_slots: float = math.inf  # concurrent chunks that fit in its RAM
    skipped: str | None = None  # why this node gets no work in this job
    why: str | None = None  # latest reason it couldn't take work (reported if it ran nothing)
    duplicate: bool = False  # a second connection to an agent we already use
    sent: int = 0  # chunks (or backup copies) sent to it
    rtt: RttTracker = field(default_factory=RttTracker)
    bw: BandwidthTracker = field(default_factory=BandwidthTracker)
    pings: dict[str, float] = field(default_factory=dict)  # ping id -> sent at
    inflight: dict[str, _Sent] = field(default_factory=dict)  # chunk_id -> where/when
    batches: dict[str, _Batch] = field(default_factory=dict)  # batch id -> its chunks

    def rtt_p90(self) -> float:
        now = time.perf_counter()
        return self.rtt.p90(waiting=[now - sent for sent in self.pings.values()])

    @property
    def label(self) -> str:
        return f"{self.name} ({self.addr})" if self.name else self.addr

    @property
    def short(self) -> str:
        return self.name or self.addr


@dataclass
class _Local:
    chunk_id: str
    started: float
    backup: bool = False


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
    PLACE_BY_SPEED = True  # longest chunks to the fastest laptop (tests switch it off)

    def __init__(
        self,
        mesh: Mesh,
        discovery: Discovery | None = None,
        on_progress: Callable[[int, int, RunStats], None] | None = None,
        memory: int = 0,
    ) -> None:
        self.mesh = mesh
        self.discovery = discovery
        self.on_progress = on_progress
        self.memory = memory
        self.speculative = mesh.speculative
        self.total = 0
        self.queue = mesh.queue
        self.codec = Codec(mesh.secret)
        self.ctx = zmq.Context.instance()
        self.speed = SpeedTracker()
        self.nodes: list[_Node] = []
        self.local: dict[Future, _Local] = {}
        self.stats = RunStats()
        self.units: dict[str, float] = {}  # chunk_id -> relative cost
        self.blobs: dict[str, bytes] = {}  # chunk_id -> serialized chunk (sent once, reused)
        self.backups: dict[str, str] = {}  # chunk_id -> where its backup copy runs
        self.local_slots = mesh.workers
        self._mem_checked = -math.inf
        self._explicit = _explicit_keys(mesh.nodes) if discovery else set(mesh.nodes)
        self._tried: dict[str, int] = {}  # discovered addr -> advert generation we dialled

    # -- setup / teardown ------------------------------------------------------------
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
        node.pings[pid] = time.perf_counter()
        node.last_ping = now
        hello = {"python": python_version(), "protocol": PROTOCOL_VERSION}
        node.sock.send_multipart(self.codec.encode(MsgType.PING, pid, dumps(hello)))

    def _release(self, node: _Node, why: str) -> None:
        n = self.queue.release_node(node.addr)
        self._forget_inflight(node)
        log.warning("node %s %s; requeued %d chunk(s)", node.label, why, n)
        node.why = f"stopped answering during the run ({why})"
        self._close(node)

    def _forget_inflight(self, node: _Node) -> None:
        for cid in node.inflight:
            if self.backups.get(cid) == node.addr:
                del self.backups[cid]
        node.inflight.clear()
        node.batches.clear()

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self.mesh._warned:
            self.mesh._warned.add(key)
            log.warning(message)

    # -- main entry ---------------------------------------------------------------------
    def map(self, chunks: list[Chunk], job_id: str) -> list[Any]:
        self.total = len(chunks)
        self.units = {c.chunk_id: max(c.cost, 1e-9) for c in chunks}
        self.has_cost = len({c.cost for c in chunks}) > 1
        self.queue.submit(chunks)
        ctx = self.mesh.mp_context or pool_context()
        pool = ProcessPoolExecutor(max_workers=self.mesh.workers, mp_context=ctx)
        try:
            self._refresh_local_memory(time.monotonic(), force=True)
            self._connect()
            while not self.queue.is_done(job_id, len(chunks)):
                self._step(pool)
            results = self.queue.gather(job_id)
        finally:
            for node in self.nodes:
                if node.alive:
                    self._close(node)
            if any(not f.done() for f in self.local):
                terminate_pool(pool)  # losing backup copies: don't leave them burning CPU
            else:
                pool.shutdown(wait=False, cancel_futures=True)
            self.queue.drop_job(job_id)
            self._publish_speeds()
            self._explain_idle_nodes()
            self.mesh.last_stats = self.stats.copy()
        return results

    def _count(self, who: str, chunk_id: str) -> None:
        self.stats[who] = self.stats.get(who, 0) + 1
        if self.backups.get(chunk_id) == who:
            self.stats.backups_won += 1
        self.backups.pop(chunk_id, None)
        self.blobs.pop(chunk_id, None)
        if self.on_progress is not None:
            self._publish_speeds()
            self.on_progress(sum(self.stats.values()), self.total, self.stats.copy())

    def _explain_idle_nodes(self) -> None:
        """Every laptop that completed nothing gets a reason in stats.skipped; laptops that
        did complete chunks are dropped from it (a skip earlier in the run was temporary)."""
        explained: dict[str, str] = {}
        for node in self.nodes:
            if node.duplicate or self.stats.get(node.addr, 0) > 0:
                continue
            if node.why:
                reason = node.why
            elif not node.ready:
                reason = STILL_CONNECTING if node.alive else NO_REPLY
            elif node.sent:
                reason = LOST_TO_BACKUPS
            else:
                reason = NOT_NEEDED
            explained[node.short] = reason
        self.stats.skipped = explained

    def _publish_speeds(self) -> None:
        mean_units = sum(self.units.values()) / max(len(self.units), 1)
        self.stats.seconds_per_chunk = {
            ("local" if k == "local" else k): round(v * mean_units, 4)
            for k, v in self.speed.snapshot().items()
        }

    def _step(self, pool: ProcessPoolExecutor) -> None:
        now = time.monotonic()
        self._add_discovered(now)
        self._refresh_local_memory(now)
        self._check_memory_feasible()
        self._fill_local(pool)
        self._update_skips()
        self._fill_remote(now)
        if self.speculative:
            self._launch_backups(pool, now)
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

    # -- estimates ------------------------------------------------------------------------
    def _cpu(self, who: str) -> float:
        """Compute seconds per unit of cost on `who`; falls back to local, then the default."""
        return self.speed.per_unit(who) or self.speed.per_unit("local") or DEFAULT_SECONDS_PER_UNIT

    def _blob(self, chunk: Chunk) -> bytes:
        blob = self.blobs.get(chunk.chunk_id)
        if blob is None:
            blob = self.blobs[chunk.chunk_id] = dumps(chunk)
        return blob

    def _total_workers(self) -> int:
        remote = sum(n.workers for n in self.nodes if n.alive and n.ready and not n.skipped)
        return self.local_slots + remote

    def _endgame(self) -> bool:
        """Few chunks left: no prefetching, and remote nodes must beat this laptop."""
        return self.queue.pending_count() <= self._total_workers()

    def _local_wait(self) -> float:
        """Seconds until a local worker could start one more chunk."""
        if len(self.local) < self.local_slots:
            return 0.0
        if not self._endgame():
            backlog = self.queue.pending_count() / max(self.local_slots, 1)
            return backlog * self._cpu("local")
        now = time.monotonic()
        remaining = [
            max(0.0, self.units.get(t.chunk_id, 1.0) * self._cpu("local") - (now - t.started))
            for t in self.local.values()
        ]
        return min(remaining, default=0.0)

    def _remote_time(self, node: _Node, chunk: Chunk, nbytes: int, p90: bool) -> float:
        """Expected seconds until `chunk` sent to `node` now would come back (Scheduler)."""
        units = self.units.get(chunk.chunk_id, 1.0)
        probe = dataclasses.replace(
            chunk, est_runtime_s=units * self._cpu("local"), payload_bytes=nbytes
        )
        busy = max(0, len(node.inflight) - node.workers + 1)
        info = NodeInfo(
            node.addr,
            speed=self._cpu("local") / self._cpu(node.addr),
            bandwidth_bps=node.bw.bps,
            latency_s=node.rtt_p90() if p90 else node.rtt.mean(),
            queue_wait_s=busy / node.workers * units * self._cpu(node.addr),
        )
        return self.mesh.scheduler.expected_cost(probe, info)

    def _local_time(self, chunk: Chunk) -> float:
        units = self.units.get(chunk.chunk_id, 1.0)
        info = NodeInfo("local", is_local=True, queue_wait_s=self._local_wait())
        probe = dataclasses.replace(chunk, est_runtime_s=units * self._cpu("local"))
        return self.mesh.scheduler.expected_cost(probe, info)

    def _worth_sending(self, chunk: Chunk, node: _Node) -> bool:
        nbytes = len(self._blob(chunk))
        if self._endgame():
            # Near the end the job finishes when its slowest chunk does: only send a chunk
            # away if, even with a slow (p90) round trip, it comes back sooner.
            return self._remote_time(node, chunk, nbytes, p90=True) < self._local_time(chunk)
        limit = self.mesh.remote_slowdown_limit
        return self._remote_time(node, chunk, nbytes, p90=False) <= limit * self._local_time(chunk)

    def _batch_size(self, node: _Node, room: int) -> int:
        """Chunks per RUN: enough that the round trip is <= 10% of compute; 1 for long tasks."""
        measured = self.speed.samples(node.addr) or self.speed.samples("local")
        if room <= 1 or len(node.rtt) == 0 or not measured:
            return 1  # nothing measured yet: one chunk per message, as before
        mean_units = sum(self.units.values()) / max(len(self.units), 1)
        per_chunk = mean_units * self._cpu(node.addr)
        size = math.ceil(node.rtt.mean() / (TARGET_OVERHEAD * max(per_chunk, 1e-6)))
        fair = max(1, self.queue.pending_count() // max(1, self._ready_count()))
        return max(1, min(size, MAX_BATCH, room, fair))

    def _ready_count(self) -> int:
        return sum(1 for n in self.nodes if n.alive and n.ready and not n.skipped)

    def _capacity(self, node: _Node) -> int:
        """Most chunks this node may hold right now."""
        if self._endgame():
            cap = node.workers  # never queue a chunk behind a busy remote worker
        else:
            batch = self._batch_size(node, MAX_BATCH)
            cap = self.mesh.inflight_per_node or node.workers + batch
        return int(min(cap, node.mem_slots))

    # -- skipping nodes whose network costs more than they help -------------------------------
    def _update_skips(self) -> None:
        if not (self.speed.samples("local") or any(self.speed.samples(n.addr) for n in self.nodes)):
            return  # nothing measured yet: keep the old behaviour
        nxt = self.queue.peek()
        mean_units = sum(self.units.values()) / max(len(self.units), 1)
        if nxt is not None:
            nbytes = len(self._blob(nxt))
        elif self.blobs:
            nbytes = len(next(iter(self.blobs.values())))
        else:
            return
        for node in self.nodes:
            if not (node.alive and node.ready) or len(node.rtt) == 0:
                continue
            compute = mean_units * self._cpu(node.addr)
            latency = node.rtt_p90() / MAX_BATCH  # even with the biggest batch
            transfer = nbytes / node.bw.bps
            reason = None
            if latency + transfer > compute:
                reason = (
                    "network too slow for these large inputs"
                    if transfer >= latency
                    else "network delay too high for these short tasks"
                )
            if reason and not node.skipped:
                node.skipped = reason
                node.why = f"skipped: {reason}"
                self.stats.skipped[node.short] = reason
                log.warning("skipped %s: %s", node.short, reason)
            elif not reason and node.skipped:
                node.skipped = None
                self.stats.skipped.pop(node.short, None)
                log.info("using %s again: network now fast enough for this job", node.short)

    # -- memory ----------------------------------------------------------------------------
    def _refresh_local_memory(self, now: float, force: bool = False) -> None:
        if not self.memory or (not force and now - self._mem_checked < MEMORY_RECHECK_S):
            return
        self._mem_checked = now
        fits = len(self.local) + psutil.virtual_memory().available // self.memory
        self.local_slots = int(max(0, min(self.mesh.workers, fits)))

    def _check_memory_feasible(self) -> None:
        """If no laptop can fit even one task, fail before anything runs."""
        if not self.memory or self.local_slots > 0 or self.local:
            return
        if any(n.alive and not n.ready for n in self.nodes):
            return  # still waiting for a handshake (and its RAM report)
        if any(n.alive and n.ready and n.mem_slots >= 1 for n in self.nodes):
            return
        if any(n.inflight for n in self.nodes):
            return
        free = [f"this laptop has {format_bytes(psutil.virtual_memory().available)} free"]
        for n in self.nodes:
            if n.ram_available is not None:
                free.append(f"{n.short} has {format_bytes(n.ram_available)} free")
        raise MemoryError(
            f"No laptop can fit one task needing {format_bytes(self.memory)}: "
            + ", ".join(free)
            + ". Lower memory= or free some RAM."
        )

    # -- local worker -----------------------------------------------------------------------
    def _end_for(self, who: str) -> str:
        """Which end of the pending queue (most expensive first) `who` should take from.

        With cost= estimates, the most expensive chunks go to the fastest laptops and
        slower ones get the cheap chunks, so the job doesn't end waiting on a big task
        on a slow laptop. Before a remote laptop is measured it starts with cheap chunks
        (they come back fast and measure it); this laptop starts with the expensive ones.
        """
        if not (self.has_cost and self.PLACE_BY_SPEED):
            return "front"
        mine = self.speed.per_unit(who)
        if mine is None:
            return "front" if who == "local" else "back"
        measured = [self.speed.per_unit("local")]
        measured += [
            self.speed.per_unit(n.addr) for n in self.nodes if n.alive and n.ready and not n.skipped
        ]
        fastest = min(v for v in measured if v is not None)
        return "front" if mine <= fastest * (1 + FAST_TOLERANCE) else "back"

    def _fill_local(self, pool: ProcessPoolExecutor) -> None:
        while len(self.local) < self.local_slots:
            c = self.queue.lease("local", end=self._end_for("local"))
            if c is None:
                return
            self._submit_local(pool, c, backup=False)

    def _submit_local(self, pool: ProcessPoolExecutor, chunk: Chunk, backup: bool) -> None:
        fut = pool.submit(_run_serialized, self._blob(chunk))
        self.local[fut] = _Local(chunk.chunk_id, time.monotonic(), backup)

    def _reap_local(self) -> None:
        for fut in [f for f in self.local if f.done()]:
            task = self.local.pop(fut)
            exc = fut.exception()
            if exc is not None:
                if self.queue.is_completed(task.chunk_id):
                    continue  # the other copy already succeeded
                raise exc  # a local exception propagates, as in local-only mode
            self.speed.add(
                "local", time.monotonic() - task.started, self.units.get(task.chunk_id, 1.0)
            )
            if self.queue.complete(task.chunk_id, fut.result(), node_id="local"):
                self._count("local", task.chunk_id)

    # -- remote workers ------------------------------------------------------------------------
    def _fill_remote(self, now: float) -> None:
        for node in self.nodes:
            if not (node.alive and node.ready) or now < node.paused_until or node.skipped:
                continue
            room = self._capacity(node) - len(node.inflight)
            if room <= 0:
                continue
            size = self._batch_size(node, room)
            end = self._end_for(node.addr)
            batch: list[Chunk] = []
            while len(batch) < size:
                nxt = self.queue.peek(end)
                if nxt is None or not self._worth_sending(nxt, node):
                    break
                c = self.queue.lease(node.addr, end=end)
                assert c is not None
                batch.append(c)
            if batch:
                self._send_batch(node, batch, now, backup=False)

    def _send_batch(self, node: _Node, chunks: list[Chunk], now: float, backup: bool) -> None:
        bid = "b-" + uuid.uuid4().hex[:10]
        payload = pack_run(self.memory, [(c.chunk_id, self._blob(c)) for c in chunks])
        node.batches[bid] = _Batch([c.chunk_id for c in chunks], time.perf_counter(), len(payload))
        for c in chunks:
            node.inflight[c.chunk_id] = _Sent(bid, now, backup)
        node.sent += len(chunks)
        node.sock.send_multipart(self.codec.encode(MsgType.RUN, bid, payload))

    # -- backups ------------------------------------------------------------------------------
    def _launch_backups(self, pool: ProcessPoolExecutor, now: float) -> None:
        """Nothing pending and spare workers: duplicate the most overdue remote chunks."""
        if self.queue.pending_count():
            return
        candidates = []
        for node in self.nodes:
            for cid, sent in node.inflight.items():
                if sent.backup or cid in self.backups or self.queue.is_completed(cid):
                    continue
                units = self.units.get(cid, 1.0)
                expected = node.rtt_p90() + units * self._cpu(node.addr)
                candidates.append((now - sent.sent - expected, cid, node, units, expected))
        candidates.sort(key=lambda t: t[0], reverse=True)  # most overdue first
        for overdue, cid, holder, units, expected in candidates:
            # Time the original still needs, by its node's measured speed. Once it's overdue
            # that estimate is evidently too low, so fall back to a full expected run.
            remaining = expected if overdue > 0 else -overdue
            if len(self.local) < self.local_slots:
                if overdue > 0 or units * self._cpu("local") < remaining:
                    chunk = self.queue.lease_backup(cid, "local")
                    if chunk is not None:
                        self._submit_local(pool, chunk, backup=True)
                        self._note_backup(cid, "local", holder)
                continue
            for other in self.nodes:
                if other is holder or not (other.alive and other.ready) or other.skipped:
                    continue
                if now < other.paused_until:
                    continue
                if len(other.inflight) >= min(other.workers, other.mem_slots):
                    continue  # only a free worker: a backup must never wait in a queue
                theirs = other.rtt_p90() + units * self._cpu(other.addr)
                if theirs < remaining:
                    chunk = self.queue.lease_backup(cid, other.addr)
                    if chunk is not None:
                        self._send_batch(other, [chunk], now, backup=True)
                        self._note_backup(cid, other.addr, holder)
                    break

    def _note_backup(self, cid: str, where: str, holder: _Node) -> None:
        self.backups[cid] = where
        self.stats.backups_launched += 1
        log.info("backup copy of chunk %s started on %s (original on %s)", cid, where, holder.short)

    # -- messages ------------------------------------------------------------------------------
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
            node.rtt.add(time.perf_counter() - sent)
            info = loads(msg.payload)
            if not node.ready and not self._handshake(node, info):
                return
            self._update_ram(node, info.get("ram_total"), info.get("ram_available"))
            if not info.get("lendable", True):
                node.paused_until = now + REFUSED_BACKOFF_S
                node.why = NOT_LENDABLE
        elif msg.type is MsgType.RESULT:
            self._on_results(node, msg)
        elif msg.type is MsgType.BUSY:
            self._on_busy(node, msg, now)
        elif msg.type is MsgType.ERROR:
            batch = node.batches.get(msg.chunk_id)
            if batch is None:
                return
            err = loads(msg.payload)
            if err.get("kind") == "refused":
                # Owner is using the machine: not a job failure. Back off and rerun elsewhere.
                self._release_refused(node, now)
                return
            self._raise_remote(node, batch.chunk_ids[0], err)

    def _on_results(self, node: _Node, msg: Message) -> None:
        batch = node.batches.pop(msg.chunk_id, None)
        if batch is None:
            return
        agent_seconds, replies = unpack_results(msg.payload)
        total = time.perf_counter() - batch.sent
        network = max(total - agent_seconds - node.rtt.mean(default=0.0), 1e-4)
        node.bw.add(batch.nbytes + len(msg.payload), network)
        for reply in replies:
            if node.inflight.pop(reply.chunk_id, None) is None:
                continue
            if not reply.ok:
                if self.queue.is_completed(reply.chunk_id):
                    continue  # the other copy already succeeded
                self._raise_remote(node, reply.chunk_id, loads(reply.blob))
            self.speed.add(node.addr, reply.seconds, self.units.get(reply.chunk_id, 1.0))
            if self.queue.complete(reply.chunk_id, loads(reply.blob), node_id=node.addr):
                self._count(node.addr, reply.chunk_id)

    def _on_busy(self, node: _Node, msg: Message, now: float) -> None:
        batch = node.batches.pop(msg.chunk_id, None)
        if batch is None:
            return
        info = loads(msg.payload) if msg.payload else {}
        requeued = 0
        for cid in batch.chunk_ids:
            sent = node.inflight.pop(cid, None)
            if sent is not None and sent.backup and self.backups.get(cid) == node.addr:
                del self.backups[cid]
            requeued += self.queue.release_chunk(cid, node.addr)
        self._update_ram(node, node.ram_total, info.get("ram_available"))
        node.paused_until = now + BUSY_BACKOFF_S
        node.why = NO_RAM
        log.info(
            "%s has no RAM for more tasks right now; requeued %d chunk(s), retrying in %.0fs",
            node.short,
            requeued,
            BUSY_BACKOFF_S,
        )

    def _update_ram(self, node: _Node, total: int | None, available: int | None) -> None:
        if total is not None:
            node.ram_total = int(total)
        if available is None:
            return
        node.ram_available = int(available)
        if self.memory:
            node.mem_slots = len(node.inflight) + node.ram_available // self.memory
            if node.mem_slots < 1:
                node.why = NO_RAM

    def _handshake(self, node: _Node, info: dict) -> bool:
        """First PONG: check versions, learn the agent's name. False = node not used."""
        node.name = str(info.get("name") or "")
        node.node_id = str(info.get("node_id") or node.node_id)
        protocol = int(info.get("protocol", 1))  # Phase 1 agents didn't send one
        theirs = str(info.get("python", ""))
        if protocol != PROTOCOL_VERSION:
            self._warn_once(f"{node.addr}:proto", protocol_mismatch(node.label, protocol))
            older = "older" if protocol < PROTOCOL_VERSION else "newer"
            node.why = f"{older} IdleMesh (protocol {protocol}, you have {PROTOCOL_VERSION})"
            self._close(node)
            return False
        if theirs != python_version():
            self._warn_once(f"{node.addr}:py:{theirs}", python_mismatch(node.label, theirs))
            node.why = f"Python {theirs}, you have {python_version()}"
            self._close(node)
            return False
        port = node.addr.rsplit(":", 1)[1]
        for other in self.nodes:
            same_agent = other.node_id == node.node_id and other.addr.rsplit(":", 1)[1] == port
            if other is not node and other.alive and other.ready and same_agent:
                log.info("%s is the same agent as %s; using one connection", node.addr, other.addr)
                node.duplicate = True
                self._close(node)
                return False
        node.ready = True
        node.workers = max(int(info.get("workers", 1)), 1)
        log.info("node %s ready (%d workers)", node.label, node.workers)
        return True

    def _release_refused(self, node: _Node, now: float) -> None:
        n = self.queue.release_node(node.addr)
        self._forget_inflight(node)
        node.paused_until = now + REFUSED_BACKOFF_S
        node.why = NOT_LENDABLE
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
                    node.why = NO_REPLY
                    self._close(node)
                continue
            if node.inflight:
                if now - node.last_heard > self.mesh.node_timeout:
                    self._release(node, f"silent for {now - node.last_heard:.1f}s")
                    continue
                timeout = self.mesh.chunk_timeout
                oldest = min(s.sent for s in node.inflight.values())
                if timeout is not None and now - oldest > timeout:
                    self._release(node, f"has held a chunk longer than {timeout:.0f}s")
                    continue
            # Ping every node all the time: RTT p90 and free RAM stay current.
            if now - node.last_ping > self.mesh.ping_interval:
                self._ping(node, now)
