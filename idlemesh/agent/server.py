"""Agent: accept signed chunks over ZeroMQ and run them in a subprocess pool.

    idlemesh serve --port 7450 --secret <shared secret>

The agent never unpickles a chunk in its own process. It forwards the raw bytes
to a pool worker, so a job that crashes (segfault, os._exit, OOM kill) only takes
down that worker; the agent replies with an ERROR and rebuilds the pool.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from typing import Any

import psutil
import zmq

from idlemesh._mp import pool_context
from idlemesh.agent.discovery import Advertiser, AgentAdvert, make_advert, node_id
from idlemesh.agent.idle import IdleMonitor
from idlemesh.protocol import (
    PROTOCOL_VERSION,
    ChunkReply,
    Codec,
    MsgType,
    dumps,
    format_bytes,
    loads,
    pack_results,
    python_version,
    unpack_run,
)

log = logging.getLogger(__name__)

LENDABLE_RECHECK_S = 10.0


def _run_blob(blob: bytes) -> tuple[bool, bytes, float]:
    """Runs in a pool worker: unpickle a chunk, run it, return (ok, pickled results or
    error, compute seconds)."""
    t0 = time.perf_counter()
    try:
        chunk = loads(blob)
        return True, dumps(chunk.run()), time.perf_counter() - t0
    except BaseException as exc:  # noqa: BLE001 - report everything back to the caller
        try:
            exc_blob = dumps(exc)
        except Exception:
            exc_blob = None
        err = {
            "kind": "exception",
            "message": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
            "exc": exc_blob,
        }
        return False, dumps(err), time.perf_counter() - t0


@dataclass
class _Batch:
    ident: bytes
    received: float  # perf_counter when the RUN arrived
    expected: int
    replies: list[ChunkReply] = field(default_factory=list)


def _noop() -> None:
    return None


class Agent:
    def __init__(
        self,
        port: int,
        secret: str,
        workers: int | None = None,
        force: bool = False,
        host: str = "*",
        monitor: IdleMonitor | None = None,
        name: str | None = None,
        mesh: str = "default",
        advertise: bool = True,
    ) -> None:
        self.port = port
        self.host = host
        self.name = name or socket.gethostname()  # only ever sent in the signed PONG
        self.mesh = mesh
        self.advertise = advertise
        self.node_id = node_id(create=True)
        self._warned_python: set[str] = set()
        self._stop = threading.Event()
        self.started = threading.Event()  # set once listening (for in-process callers)
        self.chunks_served = 0  # chunks this agent has finished for others
        self.codec = Codec(secret)
        self.workers = workers or os.cpu_count() or 2
        self.force = force
        self.monitor = monitor or IdleMonitor()
        self._lendable = True
        self._lendable_checked = 0.0
        self._pool: ProcessPoolExecutor | None = None
        self._pool_gen = 0
        # future -> (router identity, chunk_id, pool generation)
        self._running: dict[Future, tuple[str, str, int]] = {}  # -> (batch, chunk, pool gen)
        self._batches: dict[str, _Batch] = {}

    def advert(self) -> AgentAdvert:
        """What this agent broadcasts over mDNS. Deliberately excludes name and lendable."""
        return make_advert(self.port, self.mesh, self.workers)

    # -- lendability -------------------------------------------------------------
    def lendable(self) -> bool:
        """Whether this machine may take work. Only re-measured while the agent is idle:
        while chunks run, the CPU load is our own and would make us refuse ourselves."""
        if self.force:
            return True
        now = time.monotonic()
        if not self._running and now - self._lendable_checked > LENDABLE_RECHECK_S:
            self._lendable = self.monitor.snapshot().lendable()
            self._lendable_checked = now
        return self._lendable

    # -- pool --------------------------------------------------------------------
    def _new_pool(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)
        self._pool = ProcessPoolExecutor(max_workers=self.workers, mp_context=pool_context())
        self._pool_gen += 1
        for f in [self._pool.submit(_noop) for _ in range(self.workers)]:
            f.result()  # spawn workers now, not on the first chunk

    def _submit(self, blob: bytes) -> Future:
        assert self._pool is not None
        try:
            return self._pool.submit(_run_blob, blob)
        except BrokenProcessPool:
            self._new_pool()
            return self._pool.submit(_run_blob, blob)

    # -- main loop ---------------------------------------------------------------
    def stop(self) -> None:
        """Ask serve_forever() (running in another thread) to shut down cleanly."""
        self._stop.set()

    def serve_forever(self) -> None:
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 0)
        advertiser = None
        try:
            sock.bind(f"tcp://{self.host}:{self.port}")
            self._new_pool()
            self.lendable()
            if self.advertise:
                advertiser = Advertiser(self.advert())
                advertiser.start()
            print(
                f"idlemesh agent '{self.name}' (id {self.node_id}) listening on "
                f"tcp://{self.host}:{self.port} (workers={self.workers}, force={self.force}, "
                f"mesh={self.mesh!r}, advertised={'yes' if advertiser else 'no'})",
                flush=True,
            )
            self.started.set()
            while not self._stop.is_set():
                if sock.poll(50):
                    while True:
                        try:
                            frames = sock.recv_multipart(zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        self._handle(sock, frames)
                self._reap(sock)
        finally:
            if advertiser is not None:
                advertiser.stop()  # goodbye packets, so peers drop us right away
            sock.close()
            if self._pool is not None:
                self._pool.shutdown(wait=False, cancel_futures=True)

    def _send(self, sock: zmq.Socket, ident: bytes, mtype: MsgType, cid: str, payload: Any) -> None:
        sock.send_multipart([ident, *self.codec.encode(mtype, cid, dumps(payload))])

    def _handle(self, sock: zmq.Socket, frames: list[bytes]) -> None:
        if len(frames) < 2:
            return
        ident, rest = frames[0], frames[1:]
        msg = self.codec.decode(rest)  # HMAC + age + nonce checked BEFORE any unpickling
        if msg is None:
            return
        if msg.type is MsgType.PING:
            self._check_peer_python(msg.payload)
            ram = psutil.virtual_memory()
            info = {
                "workers": self.workers,
                "lendable": self.lendable(),
                "python": python_version(),
                "protocol": PROTOCOL_VERSION,
                "name": self.name,
                "node_id": self.node_id,
                "ram_total": ram.total,
                "ram_available": ram.available,
            }
            self._send(sock, ident, MsgType.PONG, msg.chunk_id, info)
        elif msg.type is MsgType.RUN:
            self._run(sock, ident, msg.chunk_id, msg.payload)
        # RESULT / ERROR / PONG / BUSY are client-bound; ignore them here.

    def _run(self, sock: zmq.Socket, ident: bytes, batch_id: str, payload: bytes) -> None:
        if not self.lendable():
            log.info("refusing batch %s: machine is not lendable", batch_id)
            refusal = {"kind": "refused", "message": "machine is busy or on battery"}
            self._send(sock, ident, MsgType.ERROR, batch_id, refusal)
            return
        try:
            memory, chunks = unpack_run(payload)  # framing only: nothing is unpickled here
        except ValueError:
            log.warning("dropped malformed RUN %s", batch_id)
            return
        available = psutil.virtual_memory().available
        if memory and available < memory * len(chunks):
            log.info(
                "batch %s needs %s, only %s free: replying BUSY",
                batch_id,
                format_bytes(memory * len(chunks)),
                format_bytes(available),
            )
            self._send(sock, ident, MsgType.BUSY, batch_id, {"ram_available": available})
            return
        self._batches[batch_id] = _Batch(ident, time.perf_counter(), len(chunks))
        for cid, blob in chunks:
            self._running[self._submit(blob)] = (batch_id, cid, self._pool_gen)

    def _check_peer_python(self, payload: bytes) -> None:
        """Warn (once per version) when a client on another Python pings us.
        Phase 1 clients send an empty PING payload, which is fine."""
        if not payload:
            return
        try:
            theirs = str(loads(payload).get("python", ""))
        except Exception:
            return
        if theirs and theirs != python_version() and theirs not in self._warned_python:
            self._warned_python.add(theirs)
            log.warning(
                "a client on Python %s contacted this agent, which runs Python %s; it will "
                "not send work here. Use the same Python version on both laptops.",
                theirs,
                python_version(),
            )

    def _reap(self, sock: zmq.Socket) -> None:
        for fut in [f for f in self._running if f.done()]:
            batch_id, cid, gen = self._running.pop(fut)
            exc = fut.exception()
            if exc is None:
                ok, blob, seconds = fut.result()
                self.chunks_served += ok
                reply = ChunkReply(cid, ok, blob, seconds)
            else:
                if isinstance(exc, BrokenProcessPool) and gen == self._pool_gen:
                    log.warning("a worker process died; restarting the pool")
                    self._new_pool()
                crash = {
                    "kind": "crash",
                    "message": f"worker process died while running chunk {cid}: {exc!r}",
                    "traceback": "",
                    "exc": None,
                }
                reply = ChunkReply(cid, False, dumps(crash), 0.0)
            batch = self._batches.get(batch_id)
            if batch is None:
                continue
            batch.replies.append(reply)
            if len(batch.replies) == batch.expected:
                del self._batches[batch_id]
                elapsed = time.perf_counter() - batch.received
                payload = pack_results(elapsed, batch.replies)
                frames = self.codec.encode(MsgType.RESULT, batch_id, payload)
                sock.send_multipart([batch.ident, *frames])
