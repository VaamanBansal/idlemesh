# Phase 3 spec: a smarter scheduler

Goal: a run with friends' laptops is **never meaningfully slower** than running alone, and is as fast as the mesh allows. Real two-laptop tests on campus Wi-Fi motivated this:

| Run (ML hyperparameter search, compare both) | Alone | With friend | Speedup |
|---|---|---|---|
| medium (friend: 16 workers, sent 17 chunks) | 20.2 s | 23.9 s | 0.85× |
| large | 42.9 s | 41.3 s | 1.04× |

Cause: few, long tasks (~8–20 s each). The local laptop finished its share and sat idle while the friend's slower cores (probably on battery) finished the last chunks, and the friend's queued "+1" prefetch chunk had to wait for a free worker before even starting. The job ends when the slowest laptop finishes.

## Requirements

1. **Speculative backup tasks.** When the pending queue is empty and the local pool has idle workers while chunks are still running on other laptops, start duplicates of those remote chunks locally (most-overdue first, judged by each node's measured seconds-per-chunk) and use whichever result arrives first.
   - The queue must allow a chunk to be leased to two places; the first result completes it, and the late duplicate is ignored without error.
   - If an idle remote node is available instead, a backup may also go there.
   - Errors: a remote ERROR still fails the job as today, unless the other copy already succeeded.
   - `Mesh(speculative=True)` by default; `speculative=False` for functions with side effects. Document this.
   - Report backups launched and backups that won in `mesh.last_stats` (and credit chunk counts only to the copy that won).
2. **Measured speed per laptop.** Track an exponentially weighted average of seconds-per-chunk for each node and for local workers, measured from send to result (so it includes network time). Use it in the scheduler's cost estimate and to decide how many chunks a slow node gets near the end of a job. Before any measurement, fall back to the current behaviour.
3. **Prefetch only while there is plenty of work.** Send a node more chunks than it has workers only while the pending queue is larger than the total number of workers across the mesh. Near the end, never queue a chunk behind a busy worker on a remote laptop.
4. **Longest tasks first.** Add an optional `Mesh.map(..., cost=fn)` where `fn(input) -> float` estimates relative cost. Chunks are dispatched in descending estimated cost; results still come back in input order. Use it in the built-in ML demo (bigger networks cost more) and the backtest demo (longer slow window costs more).
5. **Memory-aware placement.** Add `Mesh.map(..., memory="4GB")` (bytes, or strings like "512MB"/"4GB") meaning the RAM one task needs.
   - Agents report total and available RAM (psutil) in the PONG and refresh it with each PONG.
   - A node only gets as many concurrent chunks as fit in its available RAM; the same rule limits local workers.
   - If no laptop, including this one, can fit a single task, raise a clear error before starting.
   - An agent that receives a chunk it can't fit (RAM changed) replies with a BUSY message; the client requeues the chunk and backs off that node briefly. This needs a protocol version bump.
6. **UI updates.**
   - "Laptops found" gains a free-RAM column.
   - Live progress shows measured seconds-per-chunk per laptop and the number of backup tasks launched/won.
   - If a "with found laptops" or "compare both" run ends with no chunks completed on any other laptop, show a clear warning ("No other laptop ran any work — check Laptops found and the log") instead of a speedup.
7. **Tests.**
   - A deliberately slow fake agent (e.g. a test agent that sleeps before replying): a mesh run finishes no more than ~10% slower than local-only, and backups are reported.
   - First result wins; the late duplicate is ignored; results are correct and in order.
   - Prefetch stops when the pending queue is smaller than total workers.
   - Speed tracking converges for a fast and a slow fake node.
   - `cost=` dispatch order is descending while results stay in input order.
   - Memory: chunks are held back from a node without enough RAM; impossible requirement raises the clear error; BUSY reply requeues.
   - `speculative=False` never runs a chunk twice.
   - UI: the no-remote-work warning appears.
   - All existing tests keep passing.
8. **README.** Tick Phase 3, describe backups, speed tracking, `cost=` and `memory=`, and add a "Before / after Phase 3" table with the two rows above and empty "after" cells for me to fill in from a real rerun.
9. **Checks.** `ruff check .`, `ruff format --check .` and `pytest` all pass.

## Out of scope

Active bandwidth benchmarking on join, GPU scheduling, owner-activity detection (Phase 4), credits.

## Way of working

- Branch `phase-3-scheduler`. Show a short plan (files, how leasing changes for duplicates, protocol change) and wait for approval before coding.
- Small commits; push, open a PR to main, and watch CI until it's green on every OS and Python version.

## Done when

Rerunning **ML large · compare both** with the same two laptops is clearly faster than alone, and a run where the friend's laptop is deliberately slow (e.g. on battery, few workers) is never meaningfully slower than alone.
