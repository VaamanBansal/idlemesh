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
import ipaddress
import logging
import re
import secrets
import struct
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import cloudpickle

log = logging.getLogger(__name__)

MAX_AGE_S = 300.0  # reject messages more than 5 minutes old (or in the future)
PROTOCOL_VERSION = 3  # 1 = Phase 1; 2 = Phase 2 (versions in handshake); 3 = batches, BUSY, RAM
DEFAULT_PORT = 7450

_HOSTNAME = re.compile(
    r"^(?=.{1,253}$)[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*\.?$"
)


def python_version() -> str:
    """`major.minor` of this interpreter. cloudpickle needs it to match on both ends."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def parse_node(addr: str) -> tuple[str, int]:
    """Parse "host" or "host:port" (port defaults to 7450). Raises ValueError if malformed."""
    if not isinstance(addr, str) or not addr.strip():
        raise ValueError(f"empty node address: {addr!r}")
    text = addr.strip()
    if "://" in text:
        raise ValueError(f"node address {addr!r} should be host[:port], without a scheme")
    if text.startswith("[") or text.count(":") > 1:
        raise ValueError(f"node address {addr!r} looks like IPv6, which isn't supported yet")
    host, sep, port_s = text.partition(":")
    port = DEFAULT_PORT
    if sep:
        if not port_s.isdigit():
            raise ValueError(f"node address {addr!r} has an invalid port {port_s!r}")
        port = int(port_s)
        if not 1 <= port <= 65535:
            raise ValueError(f"node address {addr!r} has a port outside 1-65535")
    try:
        ipaddress.IPv4Address(host)
    except ValueError:
        if not _HOSTNAME.match(host) or host.replace(".", "").isdigit():
            raise ValueError(f"node address {addr!r} has an invalid host {host!r}") from None
    return host, port


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
    cost: float = 1.0  # relative cost from map(cost=...); bigger chunks are dispatched first

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
    BUSY = "BUSY"  # agent can't fit the batch in RAM right now; client requeues it (v3)


@dataclass
class Message:
    type: MsgType
    chunk_id: str
    payload: bytes


# -- memory -----------------------------------------------------------------------
_UNITS = {"": 1, "B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
_MEMORY = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMGTP]?)(?:I?B)?\s*$", re.IGNORECASE)


def parse_memory(value: int | float | str | None) -> int:
    """RAM one task needs, in bytes: 4_000_000_000, "512MB", "4GB", "1.5 GiB" (units of 1024)."""
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError(f"memory must be bytes or a string like '4GB', not {value!r}")
    if isinstance(value, int | float):
        if value < 0:
            raise ValueError("memory can't be negative")
        return int(value)
    m = _MEMORY.match(str(value))
    if not m:
        raise ValueError(f"can't read memory {value!r}: use bytes or e.g. '512MB', '4GB'")
    return int(float(m.group(1)) * _UNITS[m.group(2).upper()])


def format_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# -- batched RUN / RESULT payloads (v3) -------------------------------------------------
# Plain length-prefixed framing, so the agent can read the memory requirement and split
# a batch without unpickling anything in its main process.


@dataclass
class ChunkReply:
    chunk_id: str
    ok: bool
    blob: bytes  # pickled results, or a pickled error dict when not ok
    seconds: float  # compute time inside the worker


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data, self.pos = data, 0

    def take(self, n: int) -> bytes:
        if n < 0 or self.pos + n > len(self.data):
            raise ValueError("truncated payload")
        out = self.data[self.pos : self.pos + n]
        self.pos += n
        return out

    def unpack(self, fmt: str) -> tuple:
        return struct.unpack(fmt, self.take(struct.calcsize(fmt)))

    def text(self) -> str:
        (n,) = self.unpack(">H")
        return self.take(n).decode()

    def blob(self) -> bytes:
        (n,) = self.unpack(">Q")
        return self.take(n)

    def done(self) -> None:
        if self.pos != len(self.data):
            raise ValueError("trailing bytes in payload")


def _text(s: str) -> bytes:
    b = s.encode()
    return struct.pack(">H", len(b)) + b


def pack_run(memory: int, chunks: list[tuple[str, bytes]]) -> bytes:
    """RUN payload: memory per task (bytes, 0 = not given), then (chunk_id, chunk blob)s."""
    parts = [struct.pack(">QI", memory, len(chunks))]
    for cid, blob in chunks:
        parts += [_text(cid), struct.pack(">Q", len(blob)), blob]
    return b"".join(parts)


def unpack_run(payload: bytes) -> tuple[int, list[tuple[str, bytes]]]:
    r = _Reader(payload)
    memory, n = r.unpack(">QI")
    chunks = [(r.text(), r.blob()) for _ in range(n)]
    r.done()
    return memory, chunks


def pack_results(agent_seconds: float, replies: list[ChunkReply]) -> bytes:
    """RESULT payload: the agent's time from receiving the batch to replying, then one
    reply per chunk (its own compute time lets the client separate network from work)."""
    parts = [struct.pack(">dI", agent_seconds, len(replies))]
    for r in replies:
        parts += [_text(r.chunk_id), struct.pack(">?dQ", r.ok, r.seconds, len(r.blob)), r.blob]
    return b"".join(parts)


def unpack_results(payload: bytes) -> tuple[float, list[ChunkReply]]:
    r = _Reader(payload)
    agent_seconds, n = r.unpack(">dI")
    replies = []
    for _ in range(n):
        cid = r.text()
        ok, seconds, size = r.unpack(">?dQ")
        replies.append(ChunkReply(cid, ok, r.take(size), seconds))
    r.done()
    return agent_seconds, replies


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
