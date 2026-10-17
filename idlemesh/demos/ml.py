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
    "small": (_SHAPES[:2], _ALPHAS, _RATES[:1], 3, 200),  # 6 configurations
    "medium": (_SHAPES, _ALPHAS, _RATES, 5, 300),  # 54 configurations, 270 models
    "large": (_SHAPES, _ALPHAS, _RATES, 10, 300),  # 54 configurations, 540 models
}


def train_and_score(task):
    """Train one network configuration and return its cross-validated accuracy."""
    import warnings

    from sklearn.datasets import load_digits
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.model_selection import cross_val_score
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from threadpoolctl import threadpool_limits

    hidden, alpha, lr, cv, max_iter = task
    X, y = load_digits(return_X_y=True)
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
        scores = cross_val_score(model, X, y, cv=cv)
    return hidden, alpha, lr, float(scores.mean())


def make_inputs(size: str) -> list[tuple]:
    shapes, alphas, rates, cv, max_iter = _SIZES[size]
    return [(h, a, r, cv, max_iter) for h, a, r in itertools.product(shapes, alphas, rates)]


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
    )


DEMO = _demo()
