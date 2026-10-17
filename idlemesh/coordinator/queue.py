"""In-memory chunk queue with lease-based fault tolerance.

A chunk handed to a node is "leased". If the node's heartbeat lapses or its owner
reclaims the machine, the lease is released and the chunk goes back on the queue.
Phase 2 swaps the in-memory store for SQLite so the queue survives restarts.
"""

from __future__ import annotations

import time
from collections import deque

from idlemesh.protocol import Chunk


class ChunkQueue:
    def __init__(self, max_attempts: int = 5) -> None:
        self._pending: deque[Chunk] = deque()
        self._leased: dict[str, tuple[Chunk, str, float]] = {}  # chunk_id -> (chunk, node, t)
        self._results: dict[str, dict[int, list]] = {}
        self.max_attempts = max_attempts

    def submit(self, chunks: list[Chunk]) -> None:
        for c in chunks:
            self._results.setdefault(c.job_id, {})
            self._pending.append(c)

    def lease(self, node_id: str) -> Chunk | None:
        if not self._pending:
            return None
        chunk = self._pending.popleft()
        chunk.attempts += 1
        self._leased[chunk.chunk_id] = (chunk, node_id, time.monotonic())
        return chunk

    def peek(self) -> Chunk | None:
        """The chunk `lease()` would hand out next, without leasing it."""
        return self._pending[0] if self._pending else None

    def complete(self, chunk_id: str, result: list, node_id: str | None = None) -> bool:
        """Record a result. With `node_id`, a result from a node that no longer holds
        the lease (it was released as dead and the chunk re-run) is ignored."""
        if node_id is not None:
            lease = self._leased.get(chunk_id)
            if lease is None or lease[1] != node_id:
                return False
        chunk, _, _ = self._leased.pop(chunk_id)
        self._results[chunk.job_id][chunk.index] = result
        return True

    def release_node(self, node_id: str) -> int:
        """Requeue every chunk leased to a node that died or was reclaimed."""
        lost = [cid for cid, (_, n, _) in self._leased.items() if n == node_id]
        for cid in lost:
            chunk, _, _ = self._leased.pop(cid)
            if chunk.attempts >= self.max_attempts:
                raise RuntimeError(f"chunk {cid} failed {chunk.attempts} times")
            self._pending.appendleft(chunk)
        return len(lost)

    def pending_count(self) -> int:
        return len(self._pending)

    def leased_count(self) -> int:
        return len(self._leased)

    def is_done(self, job_id: str, n_chunks: int) -> bool:
        return len(self._results.get(job_id, {})) == n_chunks

    def drop_job(self, job_id: str) -> None:
        """Forget every pending, leased and finished chunk of a job (e.g. after it failed)."""
        self._pending = deque(c for c in self._pending if c.job_id != job_id)
        for cid in [cid for cid, (c, _, _) in self._leased.items() if c.job_id == job_id]:
            del self._leased[cid]
        self._results.pop(job_id, None)

    def gather(self, job_id: str) -> list:
        parts = self._results[job_id]
        return [x for i in sorted(parts) for x in parts[i]]
