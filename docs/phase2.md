# Phase 2 spec: zero-config discovery

Goal: laptops find each other on the LAN automatically. A friend runs `idlemesh serve`, and on your laptop `Mesh(discover=True).map(...)` finds and uses their laptop with no IP address typed anywhere. Explicit `nodes=[...]` keeps working exactly as in Phase 1.

## Requirements

1. **Agents advertise themselves.** `idlemesh serve` advertises `_idlemesh._tcp.local.` over mDNS by default, using `idlemesh/agent/discovery.py` (zeroconf). `--no-advertise` turns it off.
   - TXT properties: node name (default: hostname, override with `--name`), worker count, Python `major.minor`, IdleMesh version, protocol version, mesh name, and whether the machine is currently lendable.
   - Re-publish the lendable flag when it changes (check every ~10 s).
   - **Never** put the secret, or anything derived from it, in the advertisement.
   - Unregister cleanly on Ctrl+C / shutdown.
2. **Mesh names.** `idlemesh serve --mesh <name>` (default `default`). Clients only use agents in the same mesh, so two friend groups on one Wi-Fi don't collide. Mesh name also comes from `$IDLEMESH_MESH`.
3. **Client discovery.** `Mesh(discover=True, mesh="default", discover_timeout=2.0)`:
   - Browse for `discover_timeout` seconds before starting, and merge the results with any explicit `nodes`, de-duplicated by address. Never discover your own laptop's agent if one is running on it.
   - Keep browsing during `map()`. Agents that appear mid-job join and start receiving chunks. Agents that disappear are handled by the existing timeout/`release_node()` path.
   - Skip agents whose advertised Python `major.minor` or protocol version differs from the client's, and log one clear warning per skipped agent, e.g. `Skipping aahav-laptop (10.253.147.30:7450): it runs Python 3.13, you run 3.10. Use the same Python version on both laptops.`
   - If discovery finds nothing, log a hint: `No IdleMesh agents found in mesh "default". Campus and office Wi-Fi often block discovery; try a phone hotspot, or pass nodes=["IP:7450"] explicitly.` Then run locally as usual.
4. **Python version check in the handshake too.** Add Python `major.minor` to the PING/PONG handshake so explicitly listed nodes get the same check and the same clear message.
5. **Address parsing.** A node address with no port defaults to 7450. Malformed addresses raise a clear `ValueError` (not a ZeroMQ error).
6. **CLI.** Add `idlemesh nodes [--mesh NAME] [--timeout S]`: browse and print a table of agents found (name, address, workers, Python version, lendable, compatible with this machine yes/no). Replace the old Phase 0 `idlemesh join` command with this; remove `join` or make it an alias that explains `serve` is now the command to lend a laptop.
7. **Tests.**
   - Put discovery behind a small interface so the client logic is unit-tested with a fake browser: agent appears before start, appears mid-job, disappears mid-job, wrong mesh, Python version mismatch, nothing found (hint logged).
   - Address parsing: default port, IPv4 with port, hostname with port, malformed input.
   - Handshake version mismatch with an explicit node.
   - One real zeroconf round-trip test on localhost, skipped automatically when multicast isn't available (CI runners may block it).
   - Keep all existing tests passing.
8. **README.**
   - Update "Try it on two laptops" to use `Mesh(discover=True)` and `idlemesh nodes`, keeping the explicit-IP method as the fallback.
   - Add a troubleshooting note: discovery uses multicast UDP port 5353; campus/office Wi-Fi often blocks it; a phone hotspot or explicit IPs work around it; on Windows allow Python through the firewall on private networks.
   - Note that both laptops must run the same Python `major.minor`.
   - Tick Phase 2 in the roadmap, and move the SQLite-backed queue from Phase 2 to Later (deferred: `map()` runs in-process, so persistence isn't needed yet).
9. **Checks.** `ruff check .`, `ruff format --check .` and `pytest` all pass.

## Out of scope

Benchmarking and memory-aware placement (Phase 3), keyboard/mouse idle detection (Phase 4), encryption, SQLite persistence.

## Way of working

- Work on a branch `phase-2-discovery`.
- Before writing code, show a short plan of the files you'll change and wait for approval.
- Make small commits. When done, push, open a PR to main, and watch CI until it's green on every OS and Python version.

## Done when

- On a phone hotspot with two laptops: friend runs `idlemesh serve --force`; on your laptop `idlemesh nodes` lists the friend's laptop, and a script using `Mesh(discover=True)` sends it chunks with no IP typed.
- On campus Wi-Fi where discovery is blocked, the hint appears and explicit `nodes=[...]` still works.
- A Python version mismatch produces the clear message instead of a cloudpickle error.
