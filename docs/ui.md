# Spec: basic testing UI (`idlemesh ui`)

Goal: make two-laptop testing point-and-click. One command opens a local web page where you can lend this laptop, see the other laptops on the network, run a built-in demo, and watch chunks being split between laptops live. Build this **after Phase 2 is merged** (it uses discovery).

## Requirements

1. **Command.** `idlemesh ui [--port 8765]` starts a small local web server (FastAPI + uvicorn, already in the `net` extra) and opens the page in the default browser. Plain HTML/CSS/JS served from the package, with no build step and no CDN downloads, so it works offline.
2. **Security.**
   - Bind to `127.0.0.1` only: the page is for the person at this laptop, never reachable from the Wi-Fi.
   - The UI can only run the built-in demos below. It never accepts code, file paths or shell commands from the browser.
   - The secret is typed into a password field, kept in memory only, never written to disk, logs or the page after entry.
3. **Page sections** (one page, works at laptop width):
   - **Setup:** password field for the shared secret, mesh name (default `default`).
   - **Lend this laptop:** a toggle that starts/stops the agent in-process (same as `idlemesh serve`), with a worker-count slider (1 to CPU count, default all), a "force" checkbox (ignore the lendable check, for testing), this laptop's IP addresses, and a live count of chunks this laptop has run for others.
   - **Laptops found:** live table from discovery (refresh every few seconds): name (once the signed handshake succeeds), address, workers, Python version, lendable, round-trip latency, and a compatible/incompatible badge with the reason (wrong Python version, wrong secret, old IdleMesh). An "Add by IP" box for networks where discovery is blocked.
   - **Run a demo:** pick one of three built-in workloads, choose "this laptop only", "with found laptops" or "compare both" (runs local then mesh, like the demo scripts), and press Run.
   - **Live progress:** a progress bar, per-laptop chunk counts updating as results arrive, elapsed time, and when finished: both times, the speedup, whether results matched, and the demo's answer (option price / best strategy / best model).
   - **Log:** the last ~50 log lines (warnings like the Python-mismatch and discovery hints included).
4. **Built-in demos,** moved into `idlemesh/demos/` so the UI and the scripts share them:
   - `simulate`: the simple random-sum workload from the README.
   - `backtest`: the moving-average crossover sweep (184 strategies, 100 simulated stocks, 20 years).
   - `ml`: the scikit-learn MLP hyperparameter search (54 configurations, 5-fold CV). Optional: if scikit-learn isn't installed, show it disabled with "pip install scikit-learn on every laptop".
   - Add a "size" option per demo (small / medium / large) so short tests and real benchmarks are both one click.
5. **Progress hook in the SDK.** Add an optional `on_progress` callback to `Mesh.map()` (called with done count, total, and per-node counts as each chunk completes). The UI uses it; scripts can too. No behaviour change when it isn't passed.
6. **Live updates** from server to page over a WebSocket (or server-sent events): node list, progress and log lines. The page must recover if the server restarts (reconnect, then refresh state).
7. **Tests.**
   - API tests with FastAPI's TestClient: start/stop lending, run a small demo locally, progress reaches 100 %, results reported.
   - The server only binds to 127.0.0.1.
   - The run endpoint rejects anything other than the built-in demo names.
   - The secret never appears in any API response or log line.
   - `on_progress` is called once per chunk with correct counts.
   - Existing tests keep passing.
8. **README.** Add a "Testing with the UI" section near the top of the two-laptop guide (both laptops run `idlemesh ui`; friend toggles "Lend this laptop"; you press Run), with a screenshot. Mark the basic dashboard as done in the features table; the fuller Phase 6 dashboard (credits, history, charts) stays on the roadmap.
9. **Checks.** `ruff check .`, `ruff format --check .` and `pytest` pass.

## Out of scope

Charts and history, credits view, remote access to the UI, visual polish beyond clean and readable, mobile layout.

## Way of working

- Branch `basic-ui`. Show a short plan (files, API endpoints, page layout) and wait for approval before coding.
- Small commits; push, open a PR to main, and watch CI until it's green.

## Done when

On two laptops, each runs `idlemesh ui`; the friend enters the secret and toggles "Lend this laptop"; on your laptop the friend's laptop appears in "Laptops found" as compatible, and pressing Run on "compare both" shows live per-laptop progress and finishes with the speedup and "results match".
