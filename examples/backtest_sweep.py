"""Backtest sweep: 184 moving-average crossover strategies, each tested on
100 simulated stocks over 20 years, run across laptops.

Usage:
    python examples/backtest_sweep.py                         # your laptop only
    python examples/backtest_sweep.py 10.253.147.30:7450      # with a friend's laptop
"""

import math
import random
import sys
import time

from idlemesh import Mesh

N_ASSETS = 100  # simulated stocks
N_DAYS = 5040  # 20 years of trading days each
TRADING_DAYS = 252


def backtest(params):
    """Run one fast/slow moving-average crossover strategy on every simulated stock."""
    fast, slow = params
    daily_returns = []
    for asset in range(N_ASSETS):
        # Same prices for every strategy: the seed depends only on the asset.
        rng = random.Random(asset)
        drift = rng.uniform(-0.0002, 0.0006)
        vol = rng.uniform(0.01, 0.03)
        prices = [100.0]
        for _ in range(N_DAYS - 1):
            prices.append(prices[-1] * math.exp(drift + vol * rng.gauss(0, 1)))

        fast_sum = sum(prices[:fast])
        slow_sum = sum(prices[:slow])
        position = 0
        for t in range(slow, N_DAYS):
            if position:
                daily_returns.append(prices[t] / prices[t - 1] - 1)
            else:
                daily_returns.append(0.0)
            fast_sum += prices[t] - prices[t - fast]
            slow_sum += prices[t] - prices[t - slow]
            position = 1 if fast_sum / fast > slow_sum / slow else 0

    mean = sum(daily_returns) / len(daily_returns)
    var = sum((r - mean) ** 2 for r in daily_returns) / len(daily_returns)
    sharpe = mean / math.sqrt(var) * math.sqrt(TRADING_DAYS) if var > 0 else 0.0
    return fast, slow, sharpe


def run(nodes):
    grid = [(f, s) for f in range(5, 55, 5) for s in range(20, 220, 10) if f < s]
    mesh = Mesh(nodes=nodes) if nodes else Mesh()
    t0 = time.perf_counter()
    results = mesh.map(backtest, grid, chunk_size=4)
    elapsed = time.perf_counter() - t0
    best = max(results, key=lambda r: r[2])
    return elapsed, len(grid), best, getattr(mesh, "last_stats", None)


if __name__ == "__main__":
    nodes = sys.argv[1:]

    elapsed, n, best, _ = run([])
    print(f"Your laptop alone:    {elapsed:6.1f}s  ({n} strategies)")

    if nodes:
        elapsed2, _, best2, stats = run(nodes)
        print(f"With friend's laptop: {elapsed2:6.1f}s  -> {elapsed / elapsed2:.2f}x faster")
        print(f"Chunks per node: {stats}")
        assert best2 == best, "remote and local results differ!"

    fast, slow, sharpe = best
    print(f"Best strategy: {fast}-day / {slow}-day crossover, Sharpe {sharpe:.2f}")
