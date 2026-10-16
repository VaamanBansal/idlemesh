"""Wire format for shipping work between coordinator and agents.

Functions and arguments are serialized with cloudpickle so closures and lambdas
work. NOTE: unpickling executes code, so agents must only accept chunks from a
coordinator they trust. Sandboxing (Phase 5) adds a second layer of defence.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import cloudpickle


@dataclass
class Chunk:
    """One unit of schedulable work: a function applied to a slice of inputs."""

    job_id: str
    index: int
    fn: Callable[..., Any]
    inputs: list[Any]
    est_runtime_s: float = 1.0
    payload_bytes: int = 0
    chunk_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    attempts: int = 0

    def run(self) -> list[Any]:
        return [self.fn(x) for x in self.inputs]


def dumps(obj: Any) -> bytes:
    return cloudpickle.dumps(obj)


def loads(data: bytes) -> Any:
    return cloudpickle.loads(data)


def split(job_id: str, fn: Callable[..., Any], inputs: list[Any], chunk_size: int) -> list[Chunk]:
    """Split a job's inputs into fixed-size chunks so a lost node loses little work."""
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    chunks = []
    for i in range(0, len(inputs), chunk_size):
        part = inputs[i : i + chunk_size]
        chunks.append(
            Chunk(
                job_id=job_id,
                index=i // chunk_size,
                fn=fn,
                inputs=part,
                payload_bytes=len(dumps(part)),
            )
        )
    return chunks
