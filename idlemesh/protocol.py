"""Wire format for shipping work between coordinator and agents.

Functions and arguments are serialized with cloudpickle so closures and lambdas
work. NOTE: unpickling executes code, so agents must only accept chunks from a
coordinator they trust. Sandboxing (Phase 5) adds a second layer of defence.

On the network every message is six ZeroMQ frames:

    [type, chunk_id, timestamp, nonce, payload, hmac]

`hmac` is HMAC-SHA256 over the first five frames (each length-prefixed) keyed
with the mesh's shared secret. `Codec.decode` checks the signature, then the
timestamp (at most `MAX_AGE_S` old or ahead), then that the nonce hasn't been
seen before. Only then does it hand the payload back. It never unpickles
anything itself: callers unpickle the payload after `decode` accepts it.
"""

from __future__ import annotations

import hashlib
import heapq
import hmac
import logging
import secrets
import struct
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import cloudpickle

log = logging.getLogger(__name__)

MAX_AGE_S = 300.0  # reject messages more than 5 minutes old (or in the future)


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


class MsgType(str, Enum):
    RUN = "RUN"
    RESULT = "RESULT"
    ERROR = "ERROR"
    PING = "PING"
    PONG = "PONG"


@dataclass
class Message:
    type: MsgType
    chunk_id: str
    payload: bytes


class RemoteError(RuntimeError):
    """A chunk raised inside a remote agent. `remote_traceback` is the agent-side trace."""

    def __init__(self, message: str, remote_traceback: str = "") -> None:
        super().__init__(message)
        self.remote_traceback = remote_traceback


def _mac(key: bytes, frames: list[bytes]) -> bytes:
    h = hmac.new(key, digestmod=hashlib.sha256)
    for f in frames:
        h.update(len(f).to_bytes(8, "big"))  # length prefix: no shifting bytes between frames
        h.update(f)
    return h.digest()


class Codec:
    """Signs outgoing messages and verifies incoming ones (HMAC, age, replay).

    One instance per socket owner. It remembers the nonces it has accepted until
    they are too old to pass the timestamp check anyway, so memory stays bounded.
    """

    def __init__(
        self, secret: str, max_age_s: float = MAX_AGE_S, clock: Callable[[], float] = time.time
    ) -> None:
        if not secret:
            raise ValueError("a shared secret is required")
        self._key = secret.encode()
        self.max_age_s = max_age_s
        self._clock = clock
        self._seen: set[bytes] = set()
        self._expiry: list[tuple[float, bytes]] = []  # min-heap of (forget_after, nonce)

    def encode(self, mtype: MsgType, chunk_id: str, payload: bytes = b"") -> list[bytes]:
        body = [
            mtype.value.encode(),
            chunk_id.encode(),
            struct.pack(">d", self._clock()),
            secrets.token_bytes(16),
            payload,
        ]
        return [*body, _mac(self._key, body)]

    def decode(self, frames: list[bytes]) -> Message | None:
        """Return the message, or None (and log why) if it must be dropped."""
        if len(frames) != 6:
            log.warning("dropped malformed message (%d frames)", len(frames))
            return None
        *body, sig = frames
        if not hmac.compare_digest(sig, _mac(self._key, body)):
            log.warning("dropped message with bad signature (wrong secret?)")
            return None
        raw_type, raw_id, raw_ts, nonce, payload = body
        try:
            mtype = MsgType(raw_type.decode())
            chunk_id = raw_id.decode()
            (sent_at,) = struct.unpack(">d", raw_ts)
        except (ValueError, UnicodeDecodeError, struct.error):
            log.warning("dropped signed message with an invalid header")
            return None
        now = self._clock()
        skew = now - sent_at
        if abs(skew) > self.max_age_s:
            log.warning(
                "dropped %s message %.0fs %s (limit %.0fs): the two laptops' clocks may be "
                "out of sync. Turn on automatic time sync on both machines.",
                mtype.value,
                abs(skew),
                "old" if skew > 0 else "in the future",
                self.max_age_s,
            )
            return None
        while self._expiry and self._expiry[0][0] < now:
            self._seen.discard(heapq.heappop(self._expiry)[1])
        if nonce in self._seen:
            log.warning("dropped replayed %s message (nonce already seen)", mtype.value)
            return None
        self._seen.add(nonce)
        heapq.heappush(self._expiry, (sent_at + self.max_age_s, nonce))
        return Message(mtype, chunk_id, payload)
