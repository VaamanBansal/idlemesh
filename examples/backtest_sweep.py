"""Backtest sweep: 184 moving-average crossover strategies, each tested on
100 simulated stocks over 20 years, run across laptops.

The workload lives in idlemesh/demos/backtest.py (shared with `idlemesh ui`).

Usage:
    python examples/backtest_sweep.py                         # your laptop only
    python examples/backtest_sweep.py 10.253.147.30:7450      # with a friend's laptop
"""

import sys
import time

from idlemesh import Mesh
from idlemesh.demos.backtest import DEMO


def run(nodes):
    grid = DEMO.make_inputs("medium")
    mesh = Mesh(nodes=nodes) if nodes else Mesh()
    t0 = time.perf_counter()
    results = mesh.map(DEMO.fn, grid, chunk_size=DEMO.chunk_size)
    elapsed = time.perf_counter() - t0
    return elapsed, len(grid), results, getattr(mesh, "last_stats", None)


if __name__ == "__main__":
    nodes = sys.argv[1:]

    elapsed, n, results, _ = run([])
    print(f"Your laptop alone:    {elapsed:6.1f}s  ({n} strategies)")

    if nodes:
        elapsed2, _, results2, stats = run(nodes)
        print(f"With friend's laptop: {elapsed2:6.1f}s  -> {elapsed / elapsed2:.2f}x faster")
        print(f"Chunks per node: {stats}")
        assert DEMO.same(results, results2), "remote and local results differ!"

    print(DEMO.summarize(results)["answer"])
