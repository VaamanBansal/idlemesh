"""Cost-model scheduler: send each chunk wherever it will finish first.

    remote_time = latency + payload / bandwidth + runtime / speed + queue_wait
    local_time  = runtime / local_speed + local_queue_wait

With an availability predictor attached, a node's expected time is inflated by
the risk that it gets reclaimed before the chunk finishes (in which case the work
is wasted and must be redone elsewhere).
"""

from __future__ import annotations

from dataclasses import dataclass

from idlemesh.predictor.base import Predictor
from idlemesh.protocol import Chunk


@dataclass
class NodeInfo:
    node_id: str
    speed: float = 1.0  # relative compute speed (benchmark score / baseline)
    bandwidth_bps: float = 10e6  # bytes/sec to this node; inf for the local machine
    latency_s: float = 0.005
    queue_wait_s: float = 0.0
    is_local: bool = False
    idle: bool = True


class Scheduler:
    def __init__(self, predictor: Predictor | None = None, risk_floor: float = 0.05) -> None:
        self.predictor = predictor
        self.risk_floor = risk_floor

    def estimate(self, chunk: Chunk, node: NodeInfo) -> float:
        compute = chunk.est_runtime_s / max(node.speed, 1e-9)
        if node.is_local:
            return compute + node.queue_wait_s
        transfer = chunk.payload_bytes / max(node.bandwidth_bps, 1.0)
        return node.latency_s + transfer + compute + node.queue_wait_s

    def expected_cost(self, chunk: Chunk, node: NodeInfo) -> float:
        t = self.estimate(chunk, node)
        if self.predictor is None or node.is_local:
            return t
        p_ok = self.predictor.predict_idle(node.node_id, t)
        p_ok = max(p_ok, self.risk_floor)
        # Expected attempts until success on this node ~ 1/p_ok (geometric).
        return t / p_ok

    def choose(self, chunk: Chunk, nodes: list[NodeInfo]) -> NodeInfo:
        candidates = [n for n in nodes if n.idle or n.is_local]
        if not candidates:
            raise RuntimeError("no available nodes")
        return min(candidates, key=lambda n: self.expected_cost(chunk, n))
