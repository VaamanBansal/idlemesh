"""Remote execution against a real `idlemesh serve` subprocess on localhost."""

import logging
import os
import queue
import socket
import subprocess
import sys
import threading
import time

import cloudpickle
import pytest

zmq = pytest.importorskip("zmq")
psutil = pytest.importorskip("psutil")

from idlemesh import Mesh  # noqa: E402
from idlemesh.protocol import Codec, MsgType, RemoteError  # noqa: E402

# The agent can't import this test module, so ship its functions by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

SECRET = "test-secret"


def slow_square(x):
    time.sleep(0.05)
    return x * x


def slower_square(x):
    time.sleep(0.2)
    return x * x


def fail_on_agent(x):
    time.sleep(0.05)
    if os.environ.get("IDLEMESH_TEST_AGENT"):
        raise ValueError(f"boom on agent with x={x}")
    return x


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _kill_tree(proc):
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
def agent():
    started = []

    def start(secret=SECRET, workers=2):
        port = _free_port()
        cmd = [sys.executable, "-m", "idlemesh.cli", "serve", "--host", "127.0.0.1"]
        cmd += ["--port", str(port), "--secret", secret, "--workers", str(workers), "--force"]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "IDLEMESH_TEST_AGENT": "1"},
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
        _kill_tree(proc)


def test_remote_map_returns_correct_results(agent):
    addr, _ = agent()
    mesh = Mesh(workers=1, nodes=[addr], secret=SECRET)
    assert mesh.map(slow_square, range(40), chunk_size=2) == [x * x for x in range(40)]
    assert mesh.last_stats.get(addr, 0) > 0, "remote agent did no work"
    assert sum(mesh.last_stats.values()) == 20


def test_wrong_secret_is_rejected_and_job_finishes_locally(agent):
    addr, _ = agent()
    mesh = Mesh(workers=2, nodes=[addr], secret="not-the-secret", handshake_timeout=1.0)
    assert mesh.map(slow_square, range(20), chunk_size=2) == [x * x for x in range(20)]
    assert addr not in mesh.last_stats
    assert mesh.last_stats == {"local": 10}


def test_killing_agent_mid_job_still_completes(agent):
    addr, proc = agent()
    mesh = Mesh(workers=1, nodes=[addr], secret=SECRET, node_timeout=1.0, ping_interval=0.2)
    killer = threading.Timer(1.5, _kill_tree, args=(proc,))
    killer.start()
    try:
        out = mesh.map(slower_square, range(40), chunk_size=1)
    finally:
        killer.cancel()
    assert out == [x * x for x in range(40)]
    assert proc.poll() is not None, "agent was not killed during the job"


def test_remote_exception_reaches_caller_with_traceback(agent):
    addr, _ = agent()
    mesh = Mesh(workers=1, nodes=[addr], secret=SECRET)
    with pytest.raises(RemoteError) as info:
        mesh.map(fail_on_agent, range(20), chunk_size=1)
    err = info.value
    assert "ValueError: boom on agent" in str(err)
    assert "fail_on_agent" in err.remote_traceback
    assert "Traceback (most recent call last)" in err.remote_traceback
    assert isinstance(err.__cause__, ValueError)


def test_codec_rejects_replayed_message(caplog):
    sender, receiver = Codec(SECRET), Codec(SECRET)
    frames = sender.encode(MsgType.RUN, "abc", b"payload")
    msg = receiver.decode(frames)
    assert msg is not None and msg.payload == b"payload"
    with caplog.at_level(logging.WARNING):
        assert receiver.decode(frames) is None
    assert "replayed" in caplog.text


def test_codec_rejects_stale_timestamp_with_clock_warning(caplog):
    now = 1_000_000.0
    sender = Codec(SECRET, clock=lambda: now)
    receiver = Codec(SECRET, clock=lambda: now + 301)
    with caplog.at_level(logging.WARNING):
        assert receiver.decode(sender.encode(MsgType.PING, "p")) is None
    assert "clocks may be out of sync" in caplog.text
    future_receiver = Codec(SECRET, clock=lambda: now - 301)
    assert future_receiver.decode(sender.encode(MsgType.PING, "p")) is None
    assert Codec(SECRET, clock=lambda: now + 299).decode(sender.encode(MsgType.PING, "p"))


def test_codec_rejects_wrong_secret_and_tampering():
    frames = Codec(SECRET).encode(MsgType.RUN, "abc", b"payload")
    assert Codec("other").decode(frames) is None
    tampered = list(frames)
    tampered[4] = b"evil"
    assert Codec(SECRET).decode(tampered) is None
    assert Codec(SECRET).decode(frames[:5]) is None
