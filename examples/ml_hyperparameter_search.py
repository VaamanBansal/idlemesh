"""Hyperparameter search for a neural network, run across laptops.

Trains a small neural network (scikit-learn MLPClassifier) to recognise
handwritten digits, trying 54 combinations of settings. Each combination is
trained and scored with 5-fold cross-validation, so 270 models are trained
in total. The digits dataset ships with scikit-learn, so nothing is downloaded.

Both laptops need scikit-learn installed (same version is safest):
    pip install scikit-learn

Usage:
    python examples/ml_hyperparameter_search.py                         # your laptop only
    python examples/ml_hyperparameter_search.py 10.253.147.30:7450      # with a friend's laptop
"""

import itertools
import sys
import time

from idlemesh import Mesh


def train_and_score(params):
    """Train one network configuration and return its cross-validated accuracy."""
    import warnings

    from sklearn.datasets import load_digits
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.model_selection import cross_val_score
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from threadpoolctl import threadpool_limits

    hidden, alpha, lr = params
    X, y = load_digits(return_X_y=True)
    model = make_pipeline(
        StandardScaler(),
        MLPClassifier(
            hidden_layer_sizes=hidden,
            alpha=alpha,
            learning_rate_init=lr,
            max_iter=300,
            random_state=0,
        ),
    )
    # One thread per task: IdleMesh already runs one task per CPU core.
    with threadpool_limits(1), warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        scores = cross_val_score(model, X, y, cv=5)
    return hidden, alpha, lr, float(scores.mean())


def run(nodes):
    grid = list(
        itertools.product(
            [(64,), (128,), (256,), (512,), (128, 64), (256, 128)],  # network shape
            [1e-4, 1e-3, 1e-2],  # regularisation strength
            [1e-3, 3e-3, 1e-2],  # learning rate
        )
    )
    mesh = Mesh(nodes=nodes) if nodes else Mesh()
    t0 = time.perf_counter()
    results = mesh.map(train_and_score, grid, chunk_size=1)
    elapsed = time.perf_counter() - t0
    best = max(results, key=lambda r: r[3])
    return elapsed, len(grid), best, results, getattr(mesh, "last_stats", None)


if __name__ == "__main__":
    nodes = sys.argv[1:]

    elapsed, n, best, results, _ = run([])
    print(f"Your laptop alone:    {elapsed:6.1f}s  ({n} configurations, {n * 5} models)")

    if nodes:
        elapsed2, _, _, results2, stats = run(nodes)
        print(f"With friend's laptop: {elapsed2:6.1f}s  -> {elapsed / elapsed2:.2f}x faster")
        print(f"Tasks per node: {stats}")
        same = len(results) == len(results2) and all(
            abs(a[3] - b[3]) < 1e-9 for a, b in zip(results, results2, strict=True)
        )
        print("Results match the local run" if same else "WARNING: results differ from local run")

    hidden, alpha, lr, acc = best
    print(f"Best: hidden layers {hidden}, alpha {alpha}, learning rate {lr} -> {acc:.2%} accuracy")
