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
import time
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any

import zmq

from idlemesh._mp import pool_context
from idlemesh.agent.discovery import Advertiser, AgentAdvert, make_advert, node_id
from idlemesh.agent.idle import IdleMonitor
from idlemesh.protocol import PROTOCOL_VERSION, Codec, MsgType, dumps, loads, python_version

log = logging.getLogger(__name__)

LENDABLE_RECHECK_S = 10.0


def _run_blob(blob: bytes) -> tuple[bool, bytes]:
    """Runs in a pool worker: unpickle a chunk, run it, return pickled results or error."""
    try:
        chunk = loads(blob)
        return True, dumps(chunk.run())
    except BaseException as exc:  # noqa: BLE001 - report everything back to the caller
        try:
            exc_blob = dumps(exc)
        except Exception:
            exc_blob = None
        return False, dumps(
            {
                "kind": "exception",
                "message": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
                "exc": exc_blob,
            }
        )


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
        self.codec = Codec(secret)
        self.workers = workers or os.cpu_count() or 2
        self.force = force
        self.monitor = monitor or IdleMonitor()
        self._lendable = True
        self._lendable_checked = 0.0
        self._pool: ProcessPoolExecutor | None = None
        self._pool_gen = 0
        # future -> (router identity, chunk_id, pool generation)
        self._running: dict[Future, tuple[bytes, str, int]] = {}

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
    def serve_forever(self) -> None:
        ctx = zmq.Context.instance()
        sock = ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(f"tcp://{self.host}:{self.port}")
        self._new_pool()
        self.lendable()
        advertiser = None
        if self.advertise:
            advertiser = Advertiser(self.advert())
            advertiser.start()
        print(
            f"idlemesh agent '{self.name}' (id {self.node_id}) listening on "
            f"tcp://{self.host}:{self.port} (workers={self.workers}, force={self.force}, "
            f"mesh={self.mesh!r}, advertised={'yes' if advertiser else 'no'})",
            flush=True,
        )
        try:
            while True:
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
            info = {
                "workers": self.workers,
                "lendable": self.lendable(),
                "python": python_version(),
                "protocol": PROTOCOL_VERSION,
                "name": self.name,
                "node_id": self.node_id,
            }
            self._send(sock, ident, MsgType.PONG, msg.chunk_id, info)
        elif msg.type is MsgType.RUN:
            if not self.lendable():
                log.info("refusing chunk %s: machine is not lendable", msg.chunk_id)
                refusal = {"kind": "refused", "message": "machine is busy or on battery"}
                self._send(sock, ident, MsgType.ERROR, msg.chunk_id, refusal)
                return
            self._running[self._submit(msg.payload)] = (ident, msg.chunk_id, self._pool_gen)
        # RESULT / ERROR / PONG are client-bound; ignore them here.

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
            ident, cid, gen = self._running.pop(fut)
            exc = fut.exception()
            if exc is None:
                ok, blob = fut.result()
                mtype = MsgType.RESULT if ok else MsgType.ERROR
                sock.send_multipart([ident, *self.codec.encode(mtype, cid, blob)])
                continue
            if isinstance(exc, BrokenProcessPool) and gen == self._pool_gen:
                log.warning("a worker process died; restarting the pool")
                self._new_pool()
            crash = {
                "kind": "crash",
                "message": f"worker process died while running chunk {cid}: {exc!r}",
                "traceback": "",
                "exc": None,
            }
            self._send(sock, ident, MsgType.ERROR, cid, crash)
