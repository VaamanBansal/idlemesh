"""Replay synthetic laptop usage and measure wasted work.

Three friends' laptops get synthetic weekly routines (classes, evenings, sleep)
with noise. Two weeks are used to train the predictor, the third is replayed.
Long chunks are placed either naively (any idle node) or prediction-aware, and we
count compute-minutes lost when an owner reclaims their laptop mid-chunk.

    python -m idlemesh.simulator.simulate
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from idlemesh.predictor.hour_of_week import HourOfWeekPredictor

START = datetime(2026, 1, 5)  # a Monday


# Each owner has a weekly routine of busy blocks: (weekdays, start_hour, end_hour).
ROUTINES: dict[str, list[tuple[range, float, float]]] = {
    "night-owl": [(range(7), 20, 26)],  # 8 PM to 2 AM daily
    "student": [(range(5), 9, 13), (range(7), 18, 22)],  # classes + evenings
    "gamer": [(range(5, 7), 12, 23), (range(5), 19, 23)],  # weekend days + weeknights
}


def trace(node: str, days: int, start: datetime, rng: random.Random):
    """One sample per minute: (timestamp, idle).

    Busy blocks follow the owner's routine with +/-30 min jitter, and each day
    has 0-2 unplanned busy sessions of 10-60 minutes (someone opens the laptop).
    """
    busy = [False] * (days * 24 * 60)

    def mark(a: float, b: float) -> None:
        for m in range(max(0, int(a)), min(len(busy), int(b))):
            busy[m] = True

    for d in range(days):
        day0 = d * 24 * 60
        wd = (start + timedelta(days=d)).weekday()
        for weekdays, h0, h1 in ROUTINES[node]:
            if wd in weekdays:
                mark(day0 + h0 * 60 + rng.gauss(0, 30), day0 + h1 * 60 + rng.gauss(0, 30))
        for _ in range(rng.choice([0, 1, 1, 2])):
            s = day0 + rng.uniform(0, 24 * 60)
            mark(s, s + rng.uniform(10, 60))
    return [(start + timedelta(minutes=m), not b) for m, b in enumerate(busy)]


def run(
    policy: str,
    pred: HourOfWeekPredictor,
    live: dict,
    rng: random.Random,
    chunk_min: int = 45,
    threshold: float = 0.6,
):
    nodes = list(live)
    minutes = len(live[nodes[0]])
    busy_until = {n: 0 for n in nodes}
    done = wasted = 0
    for m in range(0, minutes - chunk_min, 5):
        for n in nodes:
            if busy_until[n] > m or not live[n][m][1]:
                continue
            t = live[n][m][0]
            if policy == "predict" and pred.predict_idle(n, chunk_min * 60, start=t) < threshold:
                continue
            window = [live[n][m + k][1] for k in range(chunk_min)]
            if all(window):
                done += chunk_min
                busy_until[n] = m + chunk_min
            else:
                lost = window.index(False)
                wasted += lost
                busy_until[n] = m + lost
    return done, wasted


def main(seed: int = 0) -> dict:
    rng = random.Random(seed)
    names = ["night-owl", "student", "gamer"]
    pred = HourOfWeekPredictor()
    for n in names:
        pred.fit(n, trace(n, 14, START, rng))
    test_start = START + timedelta(days=14)
    live = {n: trace(n, 7, test_start, rng) for n in names}

    out = {}
    print(f"{'policy':<10}{'useful min':>12}{'wasted min':>12}{'waste %':>10}")
    for policy in ("naive", "predict"):
        done, wasted = run(policy, pred, live, rng)
        pct = 100 * wasted / max(done + wasted, 1)
        out[policy] = pct
        print(f"{policy:<10}{done:>12}{wasted:>12}{pct:>9.1f}%")
    return out


if __name__ == "__main__":
    main()
