from datetime import datetime, timedelta

import pytest

from idlemesh import Mesh
from idlemesh.agent.idle import IdleSnapshot
from idlemesh.coordinator import ChunkQueue, Ledger, NodeInfo, Scheduler
from idlemesh.predictor import AlwaysIdle, HourOfWeekPredictor
from idlemesh.protocol import split


def square(x):
    return x * x


def test_split_covers_all_inputs():
    chunks = split("j", square, list(range(10)), chunk_size=3)
    assert [len(c.inputs) for c in chunks] == [3, 3, 3, 1]
    with pytest.raises(ValueError):
        split("j", square, [1], chunk_size=0)


def test_queue_requeues_on_node_loss():
    q = ChunkQueue()
    chunks = split("j", square, list(range(4)), chunk_size=2)
    q.submit(chunks)
    a = q.lease("friend")
    assert q.release_node("friend") == 1
    assert q.pending_count() == 2
    for _ in range(2):
        c = q.lease("me")
        q.complete(c.chunk_id, c.run())
    assert q.is_done("j", 2)
    assert q.gather("j") == [0, 1, 4, 9]
    assert a.attempts == 2


def test_mesh_map_local():
    assert Mesh(workers=2).map(square, range(20), chunk_size=6) == [x * x for x in range(20)]


def test_scheduler_keeps_data_heavy_work_local():
    s = Scheduler()
    [chunk] = split("j", square, [0], chunk_size=1)
    chunk.est_runtime_s, chunk.payload_bytes = 1.0, 500_000_000  # 500 MB
    local = NodeInfo("me", speed=1.0, is_local=True)
    fast_remote = NodeInfo("friend", speed=3.0, bandwidth_bps=10e6)
    assert s.choose(chunk, [local, fast_remote]).node_id == "me"
    chunk.payload_bytes = 1_000  # tiny payload -> remote wins
    assert s.choose(chunk, [local, fast_remote]).node_id == "friend"


def test_predictor_learns_busy_hours():
    p = HourOfWeekPredictor()
    start = datetime(2026, 1, 5)
    samples = []
    for d in range(14):
        for m in range(24 * 60):
            t = start + timedelta(days=d, minutes=m)
            samples.append((t, not (18 <= t.hour < 22)))  # busy every evening
    p.fit("n", samples)
    morning = datetime(2026, 1, 21, 9)
    before_evening = datetime(2026, 1, 21, 17, 30)
    assert p.predict_idle("n", 3600, start=morning) > 0.9
    assert p.predict_idle("n", 3600, start=before_evening) < 0.2
    assert AlwaysIdle().predict_idle("n", 1e9) == 1.0


def test_ledger_and_idle_policy():
    led = Ledger(starting_credits=10)
    led.record("me", "friend", 4)
    assert (led.balance("me"), led.balance("friend")) == (6, 14)
    assert IdleSnapshot(5, False, None, None).lendable()
    assert not IdleSnapshot(5, True, 80, None).lendable()
    assert not IdleSnapshot(90, False, None, None).lendable()


def test_simulator_prediction_reduces_waste():
    from idlemesh.simulator.simulate import main

    out = main(seed=1)
    assert out["predict"] < out["naive"]
