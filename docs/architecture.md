# Architecture

IdleMesh has three roles. Any laptop can play any of them.

## Node agent (every laptop)

- **Discovery** (`agent/discovery.py`): advertises `_idlemesh._tcp.local.` over mDNS, so peers find each other with no configuration.
- **Idle monitor** (`agent/idle.py`): decides whether the machine is *lendable*: low CPU load, not on battery, and (Phase 4) no recent keyboard or mouse input.
- **Activity log + predictor** (`predictor/`): one idle/busy sample per minute, kept locally. The predictor answers `predict_idle(node, duration)`, and only that probability is sent to the coordinator.
- **Runner** (Phase 1/5): executes chunks, first as a subprocess, later inside Docker with resource limits.

## Coordinator (the laptop that submits work)

- **Chunk queue** (`coordinator/queue.py`): jobs are split into chunks. A chunk handed to a node is *leased*; if the node's heartbeat lapses or its owner reclaims it, `release_node()` puts its chunks back at the front of the queue. Chunks that fail `max_attempts` times surface as errors.
- **Scheduler** (`coordinator/scheduler.py`): estimates completion time per node:

  ```
  local:  runtime / speed + queue_wait
  remote: latency + payload / bandwidth + runtime / speed + queue_wait
  ```

  With a predictor attached, a remote node's cost becomes `time / P(stays idle)`, the expected time including retries if the node gets reclaimed (geometric model). A small `risk_floor` stops a single unlucky estimate from excluding a node forever.
- **Ledger** (`coordinator/ledger.py`): compute-seconds donated minus compute-seconds borrowed. Priority is proportional to balance, with a floor so nobody starves.

## SDK

`Mesh.map(fn, inputs, chunk_size)` and the `@task` decorator. Code written against the SDK today (local process pool) keeps working unchanged when remote execution lands.

## The availability predictor

`HourOfWeekPredictor` is an empirical survival model:

1. From the minute-level log, compute for each minute the length of the idle run that followed it.
2. To predict "idle for the next *d* minutes starting now", look at the same time of week (±15 min) in every past week and count how often the idle run was at least *d*.
3. Blend with a Beta prior so a new node (cold start) gets a neutral 0.5 instead of a wild guess.

It's simple, explainable, cheap, and private. Anything implementing `predict_idle` can replace it.

## Simulator

`idlemesh simulate` generates three synthetic weekly routines (busy blocks with ±30 min jitter, plus 0–2 unplanned sessions a day), trains on two weeks, replays the third, and compares naive placement with prediction-aware placement of 45-minute chunks.
