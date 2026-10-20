import os
import queue
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor

import psutil
import pytest


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    """Never read or write the real ~/.idlemesh (node ID) during tests."""
    home = tmp_path / "client-home"
    monkeypatch.setenv("IDLEMESH_HOME", str(home))
    return home


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def kill_tree(proc):
    try:
        parent = psutil.Process(proc.pid)
        procs = [*parent.children(recursive=True), parent]
    except psutil.NoSuchProcess:
        return
    for p in procs:
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(procs, timeout=10)


@pytest.fixture
def kill():
    return kill_tree


@pytest.fixture
def agent(tmp_path):
    """Start `idlemesh serve` on localhost (no mDNS); returns ("127.0.0.1:port", proc)."""
    started = []

    def start(secret="test-secret", workers=2):
        port = free_port()
        cmd = [sys.executable, "-m", "idlemesh.cli", "serve", "--host", "127.0.0.1"]
        cmd += ["--port", str(port), "--secret", secret, "--workers", str(workers), "--force"]
        cmd += ["--no-advertise", "--name", f"test-agent-{port}"]
        env = {**os.environ, "IDLEMESH_TEST_AGENT": "1", "IDLEMESH_HOME": str(tmp_path / "agent")}
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env
        )
        started.append(proc)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(ln) for ln in proc.stdout], daemon=True).start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if "listening" in lines.get(timeout=1):
                    return f"127.0.0.1:{port}", proc
            except queue.Empty:
                if proc.poll() is not None:
                    break
        raise RuntimeError("agent did not start")

    yield start
    for proc in started:
        kill_tree(proc)


class FakeAgent:
    """An in-process agent speaking the real protocol, with knobs for scheduler tests.

    delay:      seconds added before every reply (so the round trip is about this)
    jitter:     list of extra PONG delays, cycled (e.g. mostly fast with a slow tail)
    bandwidth:  bytes/s; replies wait (bytes in + bytes out) / bandwidth
    slow:       compute time multiplier (2.0 = this laptop is twice as slow)
    ram:        bytes reported as available
    busy:       reply BUSY to every RUN
    silent:     answer PINGs but never return results
    lendable:   what the PONG says (False = owner busy or on battery)
    Records every batch received in `batches` (lists of chunk indexes).
    """

    def __init__(self, secret, workers=2, delay=0.0, jitter=None, bandwidth=None, slow=1.0,
                 ram=16 * 1024**3, busy=False, silent=False, name="fake-agent",
                 lendable=True):  # fmt: skip
        import itertools
        import uuid

        import zmq

        from idlemesh.protocol import Codec

        self.codec = Codec(secret)
        self.workers, self.delay, self.bandwidth, self.slow = workers, delay, bandwidth, slow
        self.jitter = itertools.cycle(jitter or [0.0])
        self.ram, self.busy, self.silent, self.name = ram, busy, silent, name
        self.lendable = lendable
        self.node_id = uuid.uuid4().hex[:12]
        self.batches: list[list[int]] = []
        self.runs = 0  # RUN messages received
        self.sock = zmq.Context.instance().socket(zmq.ROUTER)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.port = self.sock.bind_to_random_port("tcp://127.0.0.1")
        self.addr = f"127.0.0.1:{self.port}"
        self.pool = ThreadPoolExecutor(workers)
        self._done = queue.Queue()  # (ident, batch id, received, payload_in_len, replies)
        self._outbox = []  # (due, frames)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(10)
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _later(self, seconds, ident, frames):
        import heapq

        heapq.heappush(self._outbox, (time.monotonic() + seconds, id(frames), [ident, *frames]))

    def _run_batch(self, ident, bid, received, size_in, chunks):
        from idlemesh.protocol import ChunkReply, dumps, loads

        def one(cid, blob):
            t0 = time.perf_counter()
            result = loads(blob).run()
            spent = time.perf_counter() - t0
            if self.slow > 1:
                time.sleep(spent * (self.slow - 1))
            return ChunkReply(cid, True, dumps(result), time.perf_counter() - t0)

        try:
            futures = [self.pool.submit(one, cid, blob) for cid, blob in chunks]
            replies = [f.result() for f in futures]
        except (RuntimeError, CancelledError):  # the test finished and shut us down
            return
        self._done.put((ident, bid, received, size_in, replies))

    def _serve(self):
        import heapq

        from idlemesh.protocol import (
            PROTOCOL_VERSION,
            MsgType,
            dumps,
            loads,
            pack_results,
            python_version,
            unpack_run,
        )

        while not self._stop.is_set():
            if self.sock.poll(5):
                ident, *frames = self.sock.recv_multipart()
                msg = self.codec.decode(frames)
                if msg is not None and msg.type is MsgType.PING:
                    info = {"workers": self.workers, "lendable": self.lendable,
                            "python": python_version(),
                            "protocol": PROTOCOL_VERSION, "name": self.name,
                            "node_id": self.node_id, "ram_total": self.ram,
                            "ram_available": self.ram}  # fmt: skip
                    reply = self.codec.encode(MsgType.PONG, msg.chunk_id, dumps(info))
                    self._later(self.delay + next(self.jitter), ident, reply)
                elif msg is not None and msg.type is MsgType.RUN:
                    self.runs += 1
                    _, chunks = unpack_run(msg.payload)
                    self.batches.append([loads(blob).index for _, blob in chunks])
                    if self.busy:
                        reply = self.codec.encode(MsgType.BUSY, msg.chunk_id, dumps({}))
                        self._later(self.delay, ident, reply)
                    elif not self.silent:
                        args = (ident, msg.chunk_id, time.perf_counter(), len(msg.payload), chunks)
                        threading.Thread(target=self._run_batch, args=args, daemon=True).start()
            while not self._done.empty():
                ident, bid, received, size_in, replies = self._done.get()
                payload = pack_results(time.perf_counter() - received, replies)
                wait_s = self.delay
                if self.bandwidth:
                    wait_s += (size_in + len(payload)) / self.bandwidth
                self._later(wait_s, ident, self.codec.encode(MsgType.RESULT, bid, payload))
            while self._outbox and self._outbox[0][0] <= time.monotonic():
                self.sock.send_multipart(heapq.heappop(self._outbox)[2])
        self.sock.close()

    def received(self):
        return [i for b in self.batches for i in b]


@pytest.fixture
def fake_agent():
    started = []

    def make(secret="test-secret", **kwargs):
        a = FakeAgent(secret, **kwargs).__enter__()
        started.append(a)
        return a

    yield make
    for a in started:
        a.__exit__()
