"""Hyperparameter search for a neural network, run across laptops.

Trains a small neural network (scikit-learn MLPClassifier) to recognise
handwritten digits, trying 54 combinations of settings. Each combination is
trained and scored with 5-fold cross-validation, so 270 models are trained
in total. The digits dataset ships with scikit-learn, so nothing is downloaded.

The workload lives in idlemesh/demos/ml.py (shared with `idlemesh ui`).

Both laptops need scikit-learn installed (same version is safest):
    pip install scikit-learn

Usage:
    python examples/ml_hyperparameter_search.py                         # your laptop only
    python examples/ml_hyperparameter_search.py 10.253.147.30:7450      # with a friend's laptop
"""

import sys
import time

from idlemesh import Mesh
from idlemesh.demos.ml import DEMO


def run(nodes):
    grid = DEMO.make_inputs("medium")
    mesh = Mesh(nodes=nodes) if nodes else Mesh()
    t0 = time.perf_counter()
    results = mesh.map(DEMO.fn, grid, chunk_size=DEMO.chunk_size, cost=DEMO.cost)
    elapsed = time.perf_counter() - t0
    return elapsed, len(grid), results, getattr(mesh, "last_stats", None)


if __name__ == "__main__":
    ok, why = DEMO.available()
    if not ok:
        sys.exit(why)
    nodes = sys.argv[1:]

    elapsed, n, results, _ = run([])
    print(f"Your laptop alone:    {elapsed:6.1f}s  ({n} configurations, {n * 5} models)")

    if nodes:
        elapsed2, _, results2, stats = run(nodes)
        print(f"With friend's laptop: {elapsed2:6.1f}s  -> {elapsed / elapsed2:.2f}x faster")
        print(f"Tasks per node: {stats}")
        same = DEMO.same(results, results2)
        print("Results match the local run" if same else "WARNING: results differ from local run")

    print(DEMO.summarize(results)["answer"])
