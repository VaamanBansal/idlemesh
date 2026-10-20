"""Phase 3 data structures: duplicate leases and the speed/RTT/bandwidth trackers."""

from idlemesh.coordinator import ChunkQueue
from idlemesh.coordinator.speed import BandwidthTracker, RttTracker, SpeedTracker
from idlemesh.protocol import split


def square(x):
    return x * x


def test_backup_first_result_wins_and_late_duplicate_is_ignored():
    q = ChunkQueue()
    q.submit(split("j", square, [1, 2], chunk_size=1))
    a = q.lease("friend")
    assert q.lease_backup(a.chunk_id, "local") is a
    assert q.lease_backup(a.chunk_id, "third") is None  # at most two copies
    assert q.complete(a.chunk_id, [1], node_id="local") is True
    assert q.complete(a.chunk_id, [999], node_id="friend") is False  # late copy, no error
    b = q.lease("local")
    q.complete(b.chunk_id, [4], node_id="local")
    assert q.gather("j") == [1, 4]


def test_releasing_one_copy_keeps_the_other_running():
    q = ChunkQueue()
    q.submit(split("j", square, [3], chunk_size=1))
    c = q.lease("friend")
    q.lease_backup(c.chunk_id, "local")
    assert q.release_node("friend") == 0  # the backup is still running: no requeue
    assert q.pending_count() == 0
    assert q.complete(c.chunk_id, [9], node_id="local")
    assert q.gather("j") == [9]


def test_release_chunk_requeues_without_counting_an_attempt():
    q = ChunkQueue()
    q.submit(split("j", square, [3, 4], chunk_size=1))
    c = q.lease("friend")
    assert c.attempts == 1
    assert q.release_chunk(c.chunk_id, "friend") is True
    assert q.peek() is c and c.attempts == 0


def test_speed_tracker_converges_and_normalises_by_cost():
    s = SpeedTracker()
    for _ in range(20):
        s.add("fast", 0.1)
        s.add("slow", 2.0, units=4)  # 4 units of cost in 2 s -> 0.5 s per unit
    assert abs(s.per_unit("fast") - 0.1) < 1e-9
    assert abs(s.per_unit("slow") - 0.5) < 1e-9
    assert s.per_unit("unknown") is None


def test_rtt_p90_ignores_the_mean_and_sees_spikes():
    r = RttTracker()
    assert r.p90(default=0.05) == 0.05
    for _ in range(9):
        r.add(0.01)
    r.add(1.0)
    assert r.mean() < 0.11
    assert r.p90() == 0.01  # 1 slow reply in 10 is the 10th percentile tail
    for _ in range(3):
        r.add(1.0)
    assert r.p90() == 1.0


def test_bandwidth_ignores_small_transfers():
    b = BandwidthTracker(default_bps=10e6)
    b.add(1000, 0.5)  # too small to say anything about bandwidth
    assert not b.measured and b.bps == 10e6
    b.add(2_000_000, 2.0)
    assert b.measured and abs(b.bps - 1e6) < 1
