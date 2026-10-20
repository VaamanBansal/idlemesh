"""What a map() call reports about itself (`mesh.last_stats`, and on_progress)."""

from __future__ import annotations


class RunStats(dict):
    """Chunks completed per laptop ("local" or "host:port"), plus run details.

    It is a plain dict of counts (so `stats == {"local": 3}` and `sum(stats.values())`
    keep working) with extra attributes:
      backups_launched / backups_won: speculative copies started, and how many finished first
      seconds_per_chunk: measured compute seconds per chunk, per laptop
      skipped: laptops not used for this job, with the reason
    Only the copy that finished first is counted for a chunk.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.backups_launched = 0
        self.backups_won = 0
        self.seconds_per_chunk: dict[str, float] = {}
        self.skipped: dict[str, str] = {}

    def copy(self) -> RunStats:
        out = RunStats(self)
        out.backups_launched = self.backups_launched
        out.backups_won = self.backups_won
        out.seconds_per_chunk = dict(self.seconds_per_chunk)
        out.skipped = dict(self.skipped)
        return out

    def __repr__(self) -> str:
        return (
            f"RunStats({dict.__repr__(self)}, backups_launched={self.backups_launched}, "
            f"backups_won={self.backups_won}, seconds_per_chunk={self.seconds_per_chunk}, "
            f"skipped={self.skipped})"
        )
