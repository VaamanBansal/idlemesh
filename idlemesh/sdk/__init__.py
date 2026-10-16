"""User-facing API.

    mesh = Mesh()                       # local mode today; remote nodes from Phase 1
    results = mesh.map(price_path, seeds, chunk_size=1000)

Right now `Mesh` runs chunks through the real queue + scheduler on local worker
processes, so code written against it keeps working once remote agents land.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from idlemesh.coordinator.queue import ChunkQueue
from idlemesh.protocol import Chunk, dumps, loads, split


def _run_serialized(blob: bytes) -> list[Any]:
    chunk: Chunk = loads(blob)
    return chunk.run()


class Mesh:
    def __init__(self, workers: int | None = None) -> None:
        self.workers = workers or os.cpu_count() or 2
        self.queue = ChunkQueue()

    def map(self, fn: Callable[[Any], Any], inputs: list[Any], chunk_size: int = 100) -> list[Any]:
        job_id = uuid.uuid4().hex[:8]
        chunks = split(job_id, fn, list(inputs), chunk_size)
        self.queue.submit(chunks)
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            futures = {}
            while (c := self.queue.lease("local")) is not None:
                futures[c.chunk_id] = pool.submit(_run_serialized, dumps(c))
            for cid, fut in futures.items():
                self.queue.complete(cid, fut.result())
        return self.queue.gather(job_id)


def task(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a function as mesh-runnable. Adds `fn.map(inputs, ...)`."""

    def _map(inputs, chunk_size: int = 100, mesh: Mesh | None = None):
        return (mesh or Mesh()).map(fn, inputs, chunk_size=chunk_size)

    fn.map = _map  # type: ignore[attr-defined]
    return fn
