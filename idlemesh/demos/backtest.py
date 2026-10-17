"""Backtest sweep: moving-average crossover strategies on simulated stocks."""

from __future__ import annotations

import math
import random

TRADING_DAYS = 252

#        (fast windows, slow windows, stocks, trading days)
_SIZES = {
    "small": (range(5, 30, 5), range(20, 110, 10), 25, 1260),  # 43 strategies, 25 stocks, 5 years
    "medium": (range(5, 55, 5), range(20, 220, 10), 100, 5040),  # 184 strategies, 20 years
    "large": (range(5, 55, 5), range(20, 220, 10), 200, 5040),
}


def backtest(task: tuple[int, int, int, int]) -> tuple[int, int, float]:
    """Run one fast/slow crossover strategy on every simulated stock; return its Sharpe."""
    fast, slow, n_assets, n_days = task
    daily_returns = []
    for asset in range(n_assets):
        # Same prices for every strategy: the seed depends only on the asset.
        rng = random.Random(asset)
        drift = rng.uniform(-0.0002, 0.0006)
        vol = rng.uniform(0.01, 0.03)
        prices = [100.0]
        for _ in range(n_days - 1):
            prices.append(prices[-1] * math.exp(drift + vol * rng.gauss(0, 1)))

        fast_sum = sum(prices[:fast])
        slow_sum = sum(prices[:slow])
        position = 0
        for t in range(slow, n_days):
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


def make_inputs(size: str) -> list[tuple[int, int, int, int]]:
    fasts, slows, n_assets, n_days = _SIZES[size]
    return [(f, s, n_assets, n_days) for f in fasts for s in slows if f < s]


def summarize(results: list[tuple[int, int, float]]) -> dict:
    fast, slow, sharpe = max(results, key=lambda r: r[2])
    return {
        "answer": f"Best strategy: {fast}-day / {slow}-day crossover, Sharpe {sharpe:.2f}",
        "strategies": len(results),
    }


def same(a: list, b: list) -> bool:
    from idlemesh.demos import close

    return len(a) == len(b) and all(
        x[:2] == y[:2] and close(x[2], y[2]) for x, y in zip(a, b, strict=True)
    )


def _demo():
    from idlemesh.demos import Demo

    return Demo(
        name="backtest",
        title="Backtest sweep",
        description="Moving-average crossover strategies on simulated stocks",
        fn=backtest,
        make_inputs=make_inputs,
        chunk_size=4,
        summarize=summarize,
        same=same,
    )


DEMO = _demo()
