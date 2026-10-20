"""Running measurements the scheduler uses: compute speed, round trips, bandwidth.

All of them start empty; callers fall back to their old defaults until a value
is available.
"""

from __future__ import annotations

import math
from collections import deque

EWMA_ALPHA = 0.3


class Ewma:
    """Exponentially weighted average; `value` is None until the first sample."""

    def __init__(self, alpha: float = EWMA_ALPHA) -> None:
        self.alpha = alpha
        self.value: float | None = None
        self.samples = 0

    def add(self, x: float) -> None:
        self.value = x if self.value is None else self.value + self.alpha * (x - self.value)
        self.samples += 1


class SpeedTracker:
    """Seconds per unit of work, per node ("local" for this laptop's workers).

    A unit is one chunk, or one unit of `cost=` when the job gives costs, so a heavy
    chunk doesn't make a laptop look slow.
    """

    def __init__(self) -> None:
        self._by_node: dict[str, Ewma] = {}

    def add(self, node: str, seconds: float, units: float = 1.0) -> None:
        if units > 0 and seconds >= 0:
            self._by_node.setdefault(node, Ewma()).add(seconds / units)

    def per_unit(self, node: str) -> float | None:
        e = self._by_node.get(node)
        return e.value if e else None

    def samples(self, node: str) -> int:
        e = self._by_node.get(node)
        return e.samples if e else 0

    def snapshot(self) -> dict[str, float]:
        return {n: e.value for n, e in self._by_node.items() if e.value is not None}


class RttTracker:
    """Recent round-trip times of one node: mean for steady state, p90 for decisions
    near the end of a job, where one slow reply decides when the job finishes."""

    def __init__(self, window: int = 50) -> None:
        self._samples: deque[float] = deque(maxlen=window)

    def add(self, seconds: float) -> None:
        self._samples.append(seconds)

    def __len__(self) -> int:
        return len(self._samples)

    def mean(self, default: float = 0.005) -> float:
        return sum(self._samples) / len(self._samples) if self._samples else default

    def p90(self, default: float = 0.005) -> float:
        if not self._samples:
            return default
        ordered = sorted(self._samples)
        return ordered[min(len(ordered) - 1, math.ceil(0.9 * len(ordered)) - 1)]


class BandwidthTracker:
    """Bytes per second to one node, from (bytes moved, time spent moving them).
    Only transfers large enough to dominate their own latency are counted."""

    MIN_BYTES = 64 * 1024

    def __init__(self, default_bps: float = 10e6) -> None:
        self.default_bps = default_bps
        self._ewma = Ewma()

    def add(self, nbytes: int, seconds: float) -> None:
        if nbytes >= self.MIN_BYTES and seconds > 0:
            self._ewma.add(nbytes / seconds)

    @property
    def bps(self) -> float:
        return self._ewma.value if self._ewma.value is not None else self.default_bps

    @property
    def measured(self) -> bool:
        return self._ewma.value is not None
