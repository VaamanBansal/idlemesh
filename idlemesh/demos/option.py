"""Price a European call option by Monte Carlo; check it against Black-Scholes."""

from __future__ import annotations

import math
import random

S0, K, R, SIGMA, T = 100.0, 105.0, 0.05, 0.2, 1.0

#           seeds, paths per seed
_SIZES = {"small": (48, 20_000), "medium": (192, 100_000), "large": (768, 100_000)}


def discounted_payoff_mean(task: tuple[int, int]) -> float:
    """Average discounted payoff over `paths` simulated prices, seeded by `seed`."""
    seed, paths = task
    rng = random.Random(seed)
    drift = (R - 0.5 * SIGMA**2) * T
    vol = SIGMA * math.sqrt(T)
    total = 0.0
    for _ in range(paths):
        st = S0 * math.exp(drift + vol * rng.gauss(0, 1))
        total += max(st - K, 0.0)
    return math.exp(-R * T) * total / paths


def black_scholes_call() -> float:
    d1 = (math.log(S0 / K) + (R + 0.5 * SIGMA**2) * T) / (SIGMA * math.sqrt(T))
    d2 = d1 - SIGMA * math.sqrt(T)

    def n(x: float) -> float:
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    return S0 * n(d1) - K * math.exp(-R * T) * n(d2)


def make_inputs(size: str) -> list[tuple[int, int]]:
    seeds, paths = _SIZES[size]
    return [(seed, paths) for seed in range(seeds)]


def summarize(results: list[float]) -> dict:
    mc, bs = sum(results) / len(results), black_scholes_call()
    return {
        "answer": f"Option price {mc:.4f} (Black-Scholes {bs:.4f})",
        "monte_carlo": mc,
        "black_scholes": bs,
    }


def same(a: list[float], b: list[float]) -> bool:
    from idlemesh.demos import close

    return len(a) == len(b) and all(close(x, y) for x, y in zip(a, b, strict=True))


def _demo():
    from idlemesh.demos import Demo

    return Demo(
        name="option",
        title="Option pricing",
        description="Monte Carlo price of a European call, checked against Black-Scholes",
        fn=discounted_payoff_mean,
        make_inputs=make_inputs,
        chunk_size=4,
        summarize=summarize,
        same=same,
    )


DEMO = _demo()
