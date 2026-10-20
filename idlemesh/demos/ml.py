"""Hyperparameter search for a small neural network (scikit-learn MLPClassifier).

Needs scikit-learn on every laptop (same version is safest). The digits dataset
ships with scikit-learn, so nothing is downloaded.
"""

from __future__ import annotations

import itertools

_SHAPES = [(64,), (128,), (256,), (512,), (128, 64), (256, 128)]
_ALPHAS = [1e-4, 1e-3, 1e-2]
_RATES = [1e-3, 3e-3, 1e-2]

#        (shapes, alphas, rates, CV folds, max iterations)
_SIZES = {
    "small": (_SHAPES[:2], _ALPHAS, _RATES[:1], 3, 200),  # 6 configurations x 3 folds = 18 tasks
    "medium": (_SHAPES, _ALPHAS, _RATES, 5, 300),  # 54 configurations x 5 folds = 270 tasks
    "large": (_SHAPES, _ALPHAS, _RATES, 10, 300),  # 54 configurations x 10 folds = 540 tasks
}

# One task is one (configuration, fold) pair: training a single model. That's ~10x more
# tasks than one-configuration-per-task for the same total work, so a job no longer ends
# waiting on one long task on a slow laptop. combine() puts the folds back together.


def train_and_score(task):
    """Train one configuration on one cross-validation fold; return its test accuracy.

    Uses the same split cross_val_score(cv=n) uses for a classifier (StratifiedKFold,
    no shuffling), so the averaged folds equal cross_val_score's result exactly.
    """
    import warnings

    from sklearn.datasets import load_digits
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.model_selection import StratifiedKFold
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from threadpoolctl import threadpool_limits

    hidden, alpha, lr, cv, max_iter, fold = task
    X, y = load_digits(return_X_y=True)
    train, test = list(StratifiedKFold(n_splits=cv).split(X, y))[fold]
    model = make_pipeline(
        StandardScaler(),
        MLPClassifier(
            hidden_layer_sizes=hidden,
            alpha=alpha,
            learning_rate_init=lr,
            max_iter=max_iter,
            random_state=0,
        ),
    )
    # One thread per task: IdleMesh already runs one task per CPU core.
    with threadpool_limits(1), warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        model.fit(X[train], y[train])
        score = model.score(X[test], y[test])
    return hidden, alpha, lr, fold, float(score)


def cost(task) -> float:
    """Relative training time of one fold. Measured on the 54 medium configurations:
    time grows with the number of hidden units and falls with the learning rate (a higher
    rate converges sooner); alpha made no difference."""
    hidden, _alpha, lr, _cv, max_iter, _fold = task
    return (sum(hidden) / lr) ** 0.4 * max_iter / 300


def make_inputs(size: str) -> list[tuple]:
    shapes, alphas, rates, cv, max_iter = _SIZES[size]
    return [
        (h, a, r, cv, max_iter, fold)
        for h, a, r in itertools.product(shapes, alphas, rates)
        for fold in range(cv)
    ]


def combine(results: list[tuple]) -> list[tuple]:
    """Fold scores -> one (hidden, alpha, lr, mean accuracy) per configuration, in input
    order. numpy's mean over the folds in fold order, as cross_val_score(...).mean()."""
    import numpy as np

    folds: dict[tuple, dict[int, float]] = {}
    for hidden, alpha, lr, fold, score in results:
        folds.setdefault((hidden, alpha, lr), {})[fold] = score
    return [
        (*config, float(np.array([scores[f] for f in sorted(scores)]).mean()))
        for config, scores in folds.items()
    ]


def summarize(results: list[tuple]) -> dict:
    hidden, alpha, lr, acc = max(results, key=lambda r: r[3])
    return {
        "answer": (
            f"Best model: hidden layers {hidden}, alpha {alpha}, learning rate {lr} "
            f"-> {acc:.2%} accuracy"
        ),
        "configurations": len(results),
    }


def same(a: list, b: list) -> bool:
    from idlemesh.demos import close

    return len(a) == len(b) and all(
        x[:3] == y[:3] and close(x[3], y[3]) for x, y in zip(a, b, strict=True)
    )


def _demo():
    from idlemesh.demos import Demo

    return Demo(
        name="ml",
        title="ML hyperparameter search",
        description="Neural network settings for handwritten digits, cross-validated",
        fn=train_and_score,
        make_inputs=make_inputs,
        chunk_size=1,
        summarize=summarize,
        same=same,
        requires=("sklearn", "threadpoolctl"),
        install_hint="pip install scikit-learn on every laptop",
        cost=cost,
        combine=combine,
    )


DEMO = _demo()
