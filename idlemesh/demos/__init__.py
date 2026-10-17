"""Built-in demo workloads, shared by `idlemesh ui` and the example scripts.

Each demo's inputs carry their own size settings, so a remote agent only needs
`idlemesh` installed to run them (the functions are imported, not shipped).
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

SIZES = ("small", "medium", "large")


@dataclass(frozen=True)
class Demo:
    name: str
    title: str
    description: str
    fn: Callable[[Any], Any]
    make_inputs: Callable[[str], list]  # size -> inputs
    chunk_size: int
    summarize: Callable[[list], dict]  # results -> {"answer": str, ...}
    same: Callable[[list, list], bool]  # do two runs agree?
    requires: tuple[str, ...] = ()  # importable modules needed on every laptop
    install_hint: str = ""

    def available(self) -> tuple[bool, str]:
        missing = [m for m in self.requires if importlib.util.find_spec(m) is None]
        if missing:
            return False, self.install_hint or f"needs {', '.join(missing)}"
        return True, ""


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def _registry() -> dict[str, Demo]:
    from idlemesh.demos import backtest, ml, option

    return {d.name: d for d in (option.DEMO, backtest.DEMO, ml.DEMO)}


DEMOS: dict[str, Demo] = _registry()
