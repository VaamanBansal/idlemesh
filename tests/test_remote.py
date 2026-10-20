"""Remote execution against a real `idlemesh serve` subprocess on localhost."""

import logging
import os
import sys
import threading
import time

import cloudpickle
import pytest

zmq = pytest.importorskip("zmq")

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


def test_killing_agent_mid_job_still_completes(agent, kill):
    addr, proc = agent()
    mesh = Mesh(workers=1, nodes=[addr], secret=SECRET, node_timeout=1.0, ping_interval=0.2)
    killer = threading.Timer(1.5, kill, args=(proc,))
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


def test_on_progress_counts_per_node_with_remote_agent(agent):
    addr, _ = agent()
    calls = []
    mesh = Mesh(workers=1, nodes=[addr], secret=SECRET)
    out = mesh.map(slow_square, range(40), chunk_size=2, on_progress=lambda *a: calls.append(a))
    assert out == [x * x for x in range(40)]
    assert [c[0] for c in calls] == list(range(1, 21))  # 20 chunks, one call each, in order
    assert all(total == 20 for _, total, _ in calls)
    assert all(sum(per_node.values()) == done for done, _, per_node in calls)
    assert calls[-1][2] == mesh.last_stats and calls[-1][2].get(addr, 0) > 0


def test_real_agent_runs_batches_and_replies_busy_when_ram_is_short(agent):
    import zmq

    from idlemesh.protocol import dumps, loads, pack_run, split, unpack_results

    addr, _ = agent()
    codec = Codec(SECRET)
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{addr}")
    try:
        chunks = split("j", slow_square, [2, 3, 4], chunk_size=1)
        batch = pack_run(0, [(c.chunk_id, dumps(c)) for c in chunks])
        sock.send_multipart(codec.encode(MsgType.RUN, "batch-1", batch))
        assert sock.poll(20_000)
        msg = codec.decode(sock.recv_multipart())
        assert msg.type is MsgType.RESULT and msg.chunk_id == "batch-1"
        agent_seconds, replies = unpack_results(msg.payload)
        got = {r.chunk_id: loads(r.blob) for r in replies if r.ok}
        assert got == {c.chunk_id: [c.inputs[0] ** 2] for c in chunks}
        assert all(r.seconds >= 0.05 for r in replies)  # each chunk's own compute time
        assert agent_seconds >= max(r.seconds for r in replies)

        too_big = pack_run(1024**5, [(chunks[0].chunk_id, dumps(chunks[0]))])  # 1 PB
        sock.send_multipart(codec.encode(MsgType.RUN, "batch-2", too_big))
        assert sock.poll(10_000)
        msg = codec.decode(sock.recv_multipart())
        assert msg.type is MsgType.BUSY and msg.chunk_id == "batch-2"
        assert loads(msg.payload)["ram_available"] > 0
    finally:
        sock.close()
