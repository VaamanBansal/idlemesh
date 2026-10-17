# Contributing to IdleMesh

Thanks for helping out!

## Setup

```bash
git clone https://github.com/VaamanBansal/idlemesh.git
cd idlemesh
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Before opening a PR

```bash
ruff check .
ruff format .
pytest
```

## Good places to start

- Keyboard/mouse idle time for your OS in `idlemesh/agent/idle.py` (approaches are listed in the module docstring)
- A new availability predictor: implement `predict_idle(node_id, duration_s) -> float` and compare it in the simulator
- New examples in `examples/`: batch image processing, hyperparameter sweeps, backtests

Keep PRs focused on one change, and add a test for new behaviour.
