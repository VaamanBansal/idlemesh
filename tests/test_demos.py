"""Built-in demos: the ML search split into (configuration, fold) tasks."""

import itertools

import pytest

from idlemesh.demos import ml


@pytest.mark.parametrize("size", ["small", "medium", "large"])
def test_ml_tasks_are_one_per_configuration_and_fold(size):
    shapes, alphas, rates, cv, _ = ml._SIZES[size]
    tasks = ml.make_inputs(size)
    configs = list(itertools.product(shapes, alphas, rates))
    assert len(tasks) == len(configs) * cv  # same total work: every model, once
    for config in configs:
        folds = sorted(t[5] for t in tasks if t[:3] == config)
        assert folds == list(range(cv))
    assert {"small": 18, "medium": 270, "large": 540}[size] == len(tasks)


def test_ml_cost_is_per_fold():
    task = ((128,), 1e-3, 1e-3, 5, 300, 0)
    assert ml.cost(task) == ml.cost((*task[:5], 4))  # every fold of a model costs the same
    assert ml.cost(((512,), 1e-3, 1e-3, 5, 300, 0)) > ml.cost(task)  # bigger network
    assert ml.cost(((128,), 1e-3, 1e-2, 5, 300, 0)) < ml.cost(task)  # faster learning rate


def test_ml_folds_combine_to_exactly_cross_val_score():
    pytest.importorskip("sklearn")
    import warnings

    from sklearn.datasets import load_digits
    from sklearn.exceptions import ConvergenceWarning
    from sklearn.model_selection import cross_val_score
    from sklearn.neural_network import MLPClassifier
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    tasks = [t for t in ml.make_inputs("small") if t[1] in (1e-4, 1e-2) and t[0] == (64,)]
    combined = ml.combine([ml.train_and_score(t) for t in tasks])
    X, y = load_digits(return_X_y=True)
    for hidden, alpha, lr, accuracy in combined:
        model = make_pipeline(
            StandardScaler(),
            MLPClassifier(hidden_layer_sizes=hidden, alpha=alpha, learning_rate_init=lr,
                          max_iter=200, random_state=0),
        )  # fmt: skip
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            expected = float(cross_val_score(model, X, y, cv=3).mean())
        assert accuracy == expected  # bit-for-bit, not just close


def test_combine_keeps_configuration_order_and_ignores_result_order():
    pytest.importorskip("numpy")
    folds = [((64,), 1, 1, 1, 0.5), ((32,), 1, 1, 0, 0.9), ((64,), 1, 1, 0, 0.7)]
    assert ml.combine(folds) == [((64,), 1, 1, pytest.approx(0.6)), ((32,), 1, 1, 0.9)]
