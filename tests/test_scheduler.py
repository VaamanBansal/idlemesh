"""Phase 3: backups, measured speed, prefetch, cost order, memory and the network."""

import functools
import os
import sys
import time
import uuid
from collections import Counter

import cloudpickle
import pytest

pytest.importorskip("zmq")

from idlemesh import Mesh  # noqa: E402
from idlemesh._mp import pool_context  # noqa: E402
from idlemesh.protocol import split  # noqa: E402
from idlemesh.sdk.remote import RemoteRun, _Node  # noqa: E402

cloudpickle.register_pickle_by_value(sys.modules[__name__])

SECRET = "test-secret"


def nap(seconds, x):
    time.sleep(seconds)
    return x * x


def started_at(x):
    t = time.perf_counter_ns()  # system-wide clock: comparable across worker processes
    time.sleep(0.02)
    return x, t


def mark_and_square(folder, x):
    """Leaves one file per execution, so duplicates (backup copies) can be counted."""
    with open(os.path.join(folder, f"{x}-{uuid.uuid4().hex}"), "w"):
        pass
    time.sleep(0.15)
    return x * x


def payload_len(x):
    time.sleep(0.02)
    return len(x)


def timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return out, time.perf_counter() - t0


# -- backups ---------------------------------------------------------------------------


def test_slow_laptop_never_makes_the_job_meaningfully_slower(fake_agent):
    work = functools.partial(nap, 0.2)
    inputs = list(range(16))
    expected = [x * x for x in inputs]
    local_only = Mesh(workers=2, mp_context=pool_context())  # no fork() in a threaded process
    alone, t_alone = timed(lambda: local_only.map(work, inputs, chunk_size=1))
    slow = fake_agent(SECRET, workers=2, slow=10.0)  # 2 s per task instead of 0.2 s
    mesh = Mesh(workers=2, nodes=[slow.addr], secret=SECRET)
    out, t_mesh = timed(lambda: mesh.map(work, inputs, chunk_size=1))
    assert alone == expected and out == expected  # first result wins; order kept
    assert t_mesh <= 1.10 * t_alone + 0.5, (t_mesh, t_alone, mesh.last_stats)
    assert slow.received(), "the slow laptop was never used, so nothing was tested"
    assert mesh.last_stats.backups_launched >= 1
    assert mesh.last_stats.backups_won >= 1
    assert sum(mesh.last_stats.values()) == len(inputs)  # each chunk credited once


def test_speculative_false_never_runs_a_chunk_twice(fake_agent, tmp_path):
    inputs = list(range(12))

    def run(speculative, folder):
        folder.mkdir()
        agent = fake_agent(SECRET, workers=2, slow=8.0)
        mesh = Mesh(workers=2, nodes=[agent.addr], secret=SECRET, speculative=speculative)
        out = mesh.map(functools.partial(mark_and_square, str(folder)), inputs, chunk_size=1)
        assert out == [x * x for x in inputs]
        runs = Counter(name.split("-")[0] for name in os.listdir(folder))
        return runs, mesh.last_stats

    runs, stats = run(False, tmp_path / "off")
    assert set(runs.values()) == {1}, runs
    assert stats.backups_launched == 0
    runs, stats = run(True, tmp_path / "on")  # control: the same job does duplicate work
    assert stats.backups_launched >= 1 and max(runs.values()) == 2


# -- measured speed and prefetch --------------------------------------------------------------


def test_speed_tracking_converges_for_fast_and_slow_laptops(fake_agent):
    fast = fake_agent(SECRET, workers=2, name="fast")
    slow = fake_agent(SECRET, workers=2, slow=4.0, name="slow")
    mesh = Mesh(workers=1, nodes=[fast.addr, slow.addr], secret=SECRET, speculative=False)
    mesh.map(functools.partial(nap, 0.05), range(60), chunk_size=1)
    spc = mesh.last_stats.seconds_per_chunk
    # Ratios, not absolute times: CI runners (macOS especially) overshoot short sleeps.
    assert 0.6 < spc[fast.addr] / spc["local"] < 1.6, spc  # same 1x work as this laptop
    assert 2.5 < spc[slow.addr] / spc[fast.addr] < 6, spc  # 4x slower laptop


def test_prefetch_only_while_plenty_of_work_remains():
    mesh = Mesh(workers=2, nodes=["127.0.0.1:1"], secret=SECRET)
    run = RemoteRun(mesh)
    node = _Node("127.0.0.1:1", sock=None, started=0.0, ready=True, workers=4)
    run.nodes = [node]
    mesh.queue.submit(split("j", abs, list(range(20)), chunk_size=1))
    assert run._total_workers() == 6
    assert run._capacity(node) == 5  # 20 pending > 6 workers: one chunk of prefetch
    while mesh.queue.pending_count() > 6:
        mesh.queue.lease("local")
    assert run._capacity(node) == 4  # end of job: never queue behind a busy worker


# -- cost ------------------------------------------------------------------------------------


def test_cost_dispatches_expensive_first_and_keeps_input_order():
    inputs = [3, 9, 1, 7, 5]
    mesh = Mesh(workers=1, mp_context=pool_context())  # no fork() in a threaded process
    out = mesh.map(started_at, inputs, chunk_size=1, cost=lambda x: x)
    assert [x for x, _ in out] == inputs  # results in input order
    by_start = [x for x, _ in sorted(out, key=lambda r: r[1])]
    assert by_start == sorted(inputs, reverse=True)  # executed most expensive first


# -- memory ----------------------------------------------------------------------------------


def test_node_without_enough_ram_gets_no_chunks(fake_agent):
    small = fake_agent(SECRET, ram=100 * 1024**2)  # 100 MB free
    mesh = Mesh(workers=2, nodes=[small.addr], secret=SECRET)
    out = mesh.map(functools.partial(nap, 0.02), range(10), chunk_size=1, memory="1GB")
    assert out == [x * x for x in range(10)]
    assert small.runs == 0
    assert mesh.last_stats == {"local": 10}


def test_impossible_memory_requirement_fails_before_running(fake_agent):
    with pytest.raises(MemoryError, match="can't fit one task"):
        Mesh(workers=2).map(abs, range(4), memory="1PB")
    agent = fake_agent(SECRET, ram=8 * 1024**3, name="friend")
    with pytest.raises(MemoryError, match="friend has 8.0 GB free"):
        Mesh(workers=2, nodes=[agent.addr], secret=SECRET).map(abs, range(4), memory="1PB")
    assert agent.runs == 0


def test_busy_reply_requeues_the_chunk(fake_agent, caplog):
    import logging

    busy = fake_agent(SECRET, busy=True)
    mesh = Mesh(workers=1, nodes=[busy.addr], secret=SECRET)
    with caplog.at_level(logging.INFO):
        out = mesh.map(functools.partial(nap, 0.1), range(12), chunk_size=1, memory="1MB")
    assert out == [x * x for x in range(12)]
    assert busy.runs >= 1  # it was offered work and said BUSY
    assert mesh.last_stats == {"local": 12}
    assert "no RAM for more tasks" in caplog.text


# -- network ---------------------------------------------------------------------------------


def test_short_tasks_are_batched_on_a_high_latency_link(fake_agent):
    far = fake_agent(SECRET, workers=2, delay=0.1)
    mesh = Mesh(workers=1, nodes=[far.addr], secret=SECRET, ping_interval=0.2)
    out = mesh.map(functools.partial(nap, 0.005), range(200), chunk_size=1)
    assert out == [x * x for x in range(200)]
    assert max(len(b) for b in far.batches) > 1, far.batches


def test_long_tasks_are_never_batched(fake_agent):
    far = fake_agent(SECRET, workers=2, delay=0.1)
    mesh = Mesh(workers=1, nodes=[far.addr], secret=SECRET, ping_interval=0.2)
    mesh.map(functools.partial(nap, 1.2), range(6), chunk_size=1)
    assert far.batches and all(len(b) == 1 for b in far.batches), far.batches


def test_high_jitter_laptop_gets_no_end_of_job_chunks(fake_agent):
    # Usually answers in 10 ms but one ping in four takes 1.5 s: fine on average, a bad
    # place for the last chunks of a job.
    jittery = fake_agent(SECRET, workers=2, jitter=[0.01, 0.01, 0.01, 1.5])
    mesh = Mesh(workers=2, nodes=[jittery.addr], secret=SECRET, ping_interval=0.05,
                speculative=False)  # fmt: skip
    out = mesh.map(functools.partial(nap, 0.1), range(30), chunk_size=1)
    assert out == [x * x for x in range(30)]
    got = jittery.received()
    assert got, "the jittery laptop was never used, so nothing was tested"
    last = set(range(30 - 4, 30))  # pending <= all workers (2 local + 2 remote)
    assert not last & set(got), got


def test_large_inputs_stay_local_on_a_slow_link(fake_agent):
    slow_link = fake_agent(SECRET, workers=2, bandwidth=1_000_000)  # 1 MB/s
    big = [bytes(2_000_000) for _ in range(20)]  # 2 MB each
    mesh = Mesh(workers=2, nodes=[slow_link.addr], secret=SECRET)
    out = mesh.map(payload_len, big, chunk_size=1)
    assert out == [2_000_000] * 20
    assert mesh.last_stats.get("local", 0) >= 16, mesh.last_stats
    assert "large inputs" in mesh.last_stats.skipped.get(slow_link.name, ""), mesh.last_stats

    small = list(range(20))  # control: the same link is fine for small inputs
    mesh.map(functools.partial(nap, 0.1), small, chunk_size=1)
    assert slow_link.name not in mesh.last_stats.skipped


def sleep_for(seconds):
    time.sleep(seconds)
    return seconds


def test_longest_tasks_go_to_the_fastest_laptop(fake_agent, monkeypatch):
    durations = [0.6] * 8 + [0.05] * 32  # a few long tasks and many short ones

    def run(place_by_speed):
        monkeypatch.setattr(RemoteRun, "PLACE_BY_SPEED", place_by_speed)
        fast = fake_agent(SECRET, workers=2, name="fast")
        slow = fake_agent(SECRET, workers=2, slow=4.0, name="slow")
        mesh = Mesh(workers=1, nodes=[fast.addr, slow.addr], secret=SECRET, speculative=False,
                    mp_context=pool_context())  # fmt: skip
        out, seconds = timed(lambda: mesh.map(sleep_for, durations, chunk_size=1, cost=float))
        assert out == durations
        return (
            seconds,
            [durations[i] for i in fast.received()],
            [durations[i] for i in slow.received()],
        )

    t_on, on_fast, on_slow = run(True)
    assert on_slow, "the slow laptop was never used, so nothing was tested"
    assert 0.6 not in on_slow  # every long task went to a fast laptop
    assert sum(on_fast) / len(on_fast) > sum(on_slow) / len(on_slow)
    t_off, _, off_slow = run(False)
    assert 0.6 in off_slow  # without the rule the slow laptop does get long tasks...
    assert t_on < t_off - 0.5, (t_on, t_off)  # ...and the job takes clearly longer
