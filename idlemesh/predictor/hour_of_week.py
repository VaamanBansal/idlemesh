"""Weekly-pattern availability model (empirical survival).

Each node logs one sample per minute: was the machine idle? For every logged
minute we precompute how many consecutive idle minutes followed it (its "idle
run"). To predict whether the laptop stays free for the next d minutes, we look
at the same time of week (+/- a small tolerance) in every past week and count
how often the idle run lasted at least d minutes. A Beta prior keeps estimates
sane when there is little history (cold start).

Simple, explainable, private (runs on the owner's machine), and a strong
baseline. Anything implementing `predict_idle` can replace it.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta

MINUTES_PER_WEEK = 7 * 24 * 60
EPOCH = datetime(2000, 1, 3)  # a Monday


def _week_and_minute(t: datetime) -> tuple[int, int]:
    total = int((t.replace(tzinfo=None) - EPOCH).total_seconds() // 60)
    return total // MINUTES_PER_WEEK, total % MINUTES_PER_WEEK


class HourOfWeekPredictor:
    def __init__(
        self,
        prior: float = 0.5,
        prior_weight: float = 2.0,
        tolerance_min: int = 15,
        step_min: int = 5,
        clock=datetime.now,
    ) -> None:
        self.prior = prior
        self.prior_weight = prior_weight
        self.tolerance_min = tolerance_min
        self.step_min = step_min
        self.clock = clock
        # node -> (week, minute_of_week) -> idle run length in minutes (0 if busy)
        self._runs: dict[str, dict[tuple[int, int], int]] = defaultdict(dict)

    def fit(self, node_id: str, samples: list[tuple[datetime, bool]]) -> None:
        """samples: (timestamp, idle) at one-minute resolution, any order."""
        samples = sorted(samples)
        run = 0
        next_t: datetime | None = None
        for t, idle in reversed(samples):
            contiguous = next_t is not None and (next_t - t) <= timedelta(minutes=1)
            run = (run + 1 if contiguous else 1) if idle else 0
            self._runs[node_id][_week_and_minute(t)] = run
            next_t = t

    def predict_idle(self, node_id: str, duration_s: float, start: datetime | None = None) -> float:
        start = start or self.clock()
        need = max(1, math.ceil(duration_s / 60))
        _, mow = _week_and_minute(start)
        runs = self._runs.get(node_id, {})
        weeks = {w for (w, _) in runs}
        hits = trials = 0
        for w in weeks:
            for off in range(-self.tolerance_min, self.tolerance_min + 1, self.step_min):
                key = (w, (mow + off) % MINUTES_PER_WEEK)
                if key in runs:
                    trials += 1
                    hits += runs[key] >= need
        return (hits + self.prior * self.prior_weight) / (trials + self.prior_weight)
