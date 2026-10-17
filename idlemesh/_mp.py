"""Process-pool start method for processes that also run background threads.

The agent and the remote coordinator run ZeroMQ I/O threads (and zeroconf threads
with discovery on). fork() in a multi-threaded process can deadlock the child if
another thread holds a lock at that moment, so where fork is the default (Linux
before Python 3.14) use forkserver instead. Windows and macOS already use spawn.
"""

from __future__ import annotations

import multiprocessing
from multiprocessing.context import BaseContext


def pool_context() -> BaseContext:
    ctx = multiprocessing.get_context()
    if ctx.get_start_method() == "fork":
        return multiprocessing.get_context("forkserver")
    return ctx
