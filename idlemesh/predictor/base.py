"""Availability predictor interface.

Contract: given a node and a duration, return the probability the node stays idle
(and connected) for that long starting now. Implementations run ON THE OWNER'S
MACHINE; only the returned probability is shared with the coordinator.
"""

from __future__ import annotations

from typing import Protocol


class Predictor(Protocol):
    def predict_idle(self, node_id: str, duration_s: float) -> float: ...


class AlwaysIdle:
    """Naive baseline: assume every idle node stays idle forever."""

    def predict_idle(self, node_id: str, duration_s: float) -> float:
        return 1.0
