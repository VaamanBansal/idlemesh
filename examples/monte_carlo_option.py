"""Price a European call option by Monte Carlo, spread across the mesh.

python examples/monte_carlo_option.py
"""

import math
import random
import time

from idlemesh import Mesh

S0, K, R, SIGMA, T = 100.0, 105.0, 0.05, 0.2, 1.0
PATHS_PER_SEED = 20_000


def discounted_payoff_mean(seed: int) -> float:
    rng = random.Random(seed)
    drift = (R - 0.5 * SIGMA**2) * T
    vol = SIGMA * math.sqrt(T)
    total = 0.0
    for _ in range(PATHS_PER_SEED):
        st = S0 * math.exp(drift + vol * rng.gauss(0, 1))
        total += max(st - K, 0.0)
    return math.exp(-R * T) * total / PATHS_PER_SEED


def black_scholes_call() -> float:
    d1 = (math.log(S0 / K) + (R + 0.5 * SIGMA**2) * T) / (SIGMA * math.sqrt(T))
    d2 = d1 - SIGMA * math.sqrt(T)
    n = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))  # noqa: E731
    return S0 * n(d1) - K * math.exp(-R * T) * n(d2)


if __name__ == "__main__":
    seeds = list(range(64))
    t0 = time.perf_counter()
    estimates = Mesh().map(discounted_payoff_mean, seeds, chunk_size=4)
    elapsed = time.perf_counter() - t0
    print(f"Monte Carlo price: {sum(estimates) / len(estimates):.4f}")
    print(f"Black-Scholes:     {black_scholes_call():.4f}")
    print(f"{len(seeds) * PATHS_PER_SEED:,} paths in {elapsed:.2f}s")
