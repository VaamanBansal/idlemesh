"""In-memory chunk queue with lease-based fault tolerance.

A chunk handed to a node is "leased". If the node's heartbeat lapses or its owner
reclaims the machine, the lease is released and the chunk goes back on the queue.

Near the end of a job a chunk can be leased to a second place as a backup
(`lease_backup`). The first result completes it; a later duplicate is ignored. A
released copy only sends the chunk back to the queue if no other copy is running.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from idlemesh.protocol import Chunk

MAX_COPIES = 2


@dataclass
class Lease:
    chunk: Chunk
    holders: dict[str, float] = field(default_factory=dict)  # node -> leased at (monotonic)


class ChunkQueue:
    def __init__(self, max_attempts: int = 5) -> None:
        self._pending: deque[Chunk] = deque()
        self._leased: dict[str, Lease] = {}  # chunk_id -> lease
        self._results: dict[str, dict[int, list]] = {}
        self._done: set[str] = set()  # chunk ids already completed (late copies are ignored)
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
        self._leased[chunk.chunk_id] = Lease(chunk, {node_id: time.monotonic()})
        return chunk

    def lease_backup(self, chunk_id: str, node_id: str) -> Chunk | None:
        """Lease an already-running chunk to a second node. None if it can't be backed up."""
        lease = self._leased.get(chunk_id)
        if lease is None or node_id in lease.holders or len(lease.holders) >= MAX_COPIES:
            return None
        lease.holders[node_id] = time.monotonic()
        return lease.chunk

    def peek(self) -> Chunk | None:
        """The chunk `lease()` would hand out next, without leasing it."""
        return self._pending[0] if self._pending else None

    def holders(self, chunk_id: str) -> dict[str, float]:
        lease = self._leased.get(chunk_id)
        return dict(lease.holders) if lease else {}

    def leased(self) -> dict[str, Lease]:
        """Running chunks (chunk_id -> lease). A view: don't modify."""
        return self._leased

    def complete(self, chunk_id: str, result: list, node_id: str | None = None) -> bool:
        """Record a result; True if it was the first. With `node_id`, a result from a node
        that doesn't hold the chunk (released as dead and re-run elsewhere) is ignored,
        and so is any copy after the first."""
        if chunk_id in self._done:
            return False
        lease = self._leased.get(chunk_id)
        if lease is None or (node_id is not None and node_id not in lease.holders):
            return False
        del self._leased[chunk_id]
        self._done.add(chunk_id)
        self._results[lease.chunk.job_id][lease.chunk.index] = result
        return True

    def is_completed(self, chunk_id: str) -> bool:
        return chunk_id in self._done

    def release_chunk(self, chunk_id: str, node_id: str) -> bool:
        """Take one chunk back from one node (e.g. it replied BUSY). The chunk returns to
        the front of the queue unless another copy is still running. True if requeued."""
        lease = self._leased.get(chunk_id)
        if lease is None or node_id not in lease.holders:
            return False
        del lease.holders[node_id]
        if lease.holders:
            return False
        del self._leased[chunk_id]
        lease.chunk.attempts -= 1  # not a failure: the node never ran it
        self._pending.appendleft(lease.chunk)
        return True

    def release_node(self, node_id: str) -> int:
        """Drop every copy a dead or reclaimed node held. Chunks with no other copy
        running go back to the front of the queue; returns how many were requeued."""
        requeued = 0
        for cid in [cid for cid, lease in self._leased.items() if node_id in lease.holders]:
            lease = self._leased[cid]
            del lease.holders[node_id]
            if lease.holders:
                continue  # a backup copy is still running elsewhere
            del self._leased[cid]
            if lease.chunk.attempts >= self.max_attempts:
                raise RuntimeError(f"chunk {cid} failed {lease.chunk.attempts} times")
            self._pending.appendleft(lease.chunk)
            requeued += 1
        return requeued

    def pending_count(self) -> int:
        return len(self._pending)

    def leased_count(self) -> int:
        return len(self._leased)

    def is_done(self, job_id: str, n_chunks: int) -> bool:
        return len(self._results.get(job_id, {})) == n_chunks

    def drop_job(self, job_id: str) -> None:
        """Forget every pending, leased and finished chunk of a job (e.g. after it failed)."""
        self._pending = deque(c for c in self._pending if c.job_id != job_id)
        for cid in [cid for cid, lease in self._leased.items() if lease.chunk.job_id == job_id]:
            del self._leased[cid]
        self._results.pop(job_id, None)

    def gather(self, job_id: str) -> list:
        parts = self._results[job_id]
        return [x for i in sorted(parts) for x in parts[i]]
