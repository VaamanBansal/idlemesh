# Phase 1 spec: remote execution between laptops

Goal: `Mesh(nodes=["192.168.1.20:7450"]).map(fn, inputs, chunk_size=...)` runs chunks on the remote laptop and on local worker processes at the same time, and returns results in input order. Use hardcoded addresses; mDNS discovery comes in Phase 2, so don't wire it in yet.

## Requirements

1. **Agent server.** Add `idlemesh serve --port 7450 --secret <s>` to `cli.py`. It uses a pyzmq ROUTER socket, receives serialized Chunks, runs each one in a subprocess pool (so a crashing job doesn't kill the agent), and replies with the result or a serialized error and traceback. It refuses work when `IdleMonitor` says the machine isn't lendable, unless `--force` is passed.
2. **Message format,** in `idlemesh/protocol.py`. Each message has a type (`RUN`, `RESULT`, `ERROR`, `PING`, `PONG`), a `chunk_id` and a payload. Every message carries an HMAC-SHA256 signature using the shared secret. Verify the HMAC **before** unpickling and drop anything that fails. Unpickling runs code, so this check is mandatory.
3. **Client side.** `Mesh` accepts `nodes=[...]` and `secret=...` (falling back to the `IDLEMESH_SECRET` environment variable), uses a DEALER socket per node, and keeps a configurable number of chunks in flight per node.
   - All chunks go through the existing `ChunkQueue` with leases.
   - If a node doesn't answer within a timeout or doesn't answer `PING`, call `queue.release_node()` so its chunks get re-run locally or on another node.
   - Keep the local process pool as a worker alongside remote nodes.
   - A remote `ERROR` fails the job with the original traceback; it is **not** retried.
   - `Mesh()` with no nodes must behave exactly as it does today.
4. **Scheduler.** Use the existing `Scheduler` only to decide whether a node gets more chunks: skip a node when its estimated cost is clearly worse than local. A simple version is fine; full benchmarking is Phase 3.
5. **Tests,** all on localhost:
   - Start an agent in a subprocess and check that `map()` returns correct results.
   - A wrong secret is rejected and the job still finishes locally.
   - Killing the agent mid-job still produces complete, correct results.
   - An exception raised inside a remote fn reaches the caller with its traceback.
   - Keep the existing tests passing.
6. **Dependencies.** Add pyzmq to the dependencies the tests need, and make CI install `.[dev,net]`.
7. **Example.** Add a `--nodes host:port[,host:port]` flag to `examples/monte_carlo_option.py`.
8. **README.** Mark remote execution as working, add a "Try it on two laptops" section with the exact commands, and tick Phase 1 in the roadmap.
9. **Checks.** Run `ruff check .`, `ruff format --check .` and `pytest`, and make sure all three pass.

## Way of working

- Before writing code, show a short plan of the files you'll change and the message flow, and wait for approval.
- Make small commits as you go.

## Done when

- The Monte Carlo example run with `--nodes <friend's IP>:7450` gives the same price as a local run, and finishes faster.
- Closing the friend's laptop mid-job still produces the correct result.
