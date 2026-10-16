"""Fair-share credit ledger: donating compute earns credits, borrowing spends them."""

from __future__ import annotations

from collections import defaultdict


class Ledger:
    def __init__(self, starting_credits: float = 100.0) -> None:
        self._balance: dict[str, float] = defaultdict(lambda: starting_credits)

    def record(self, borrower: str, donor: str, compute_seconds: float) -> None:
        if borrower == donor:
            return
        self._balance[borrower] -= compute_seconds
        self._balance[donor] += compute_seconds

    def balance(self, user: str) -> float:
        return self._balance[user]

    def priority(self, user: str) -> float:
        """Higher balance -> higher priority. Never fully starve anyone."""
        return max(self._balance[user], 1.0)
