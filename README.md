# IdleMesh

**Borrow your friends' idle laptops over Wi-Fi: zero config, sandboxed, and smart about when they'll need them back.**

[![CI](https://github.com/VaamanBansal/idlemesh/actions/workflows/ci.yml/badge.svg)](https://github.com/VaamanBansal/idlemesh/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-pre--alpha-orange)

IdleMesh lets a small group of people who trust each other (roommates, a study group, a lab) pool the spare CPU and GPU capacity of their laptops over a local network. No cloud and no fees. Your friend's laptop just has to be on the same Wi-Fi.

```python
import random

from idlemesh import Mesh


def simulate(seed):
    rng = random.Random(seed)  # any expensive, independent computation
    return sum(rng.random() for _ in range(1_000_000))


results = Mesh().map(simulate, range(1_000), chunk_size=10)               # this laptop only
results = Mesh(discover=True).map(simulate, range(1_000), chunk_size=10)  # + friends' laptops on the Wi-Fi
```

> **Status:** pre-alpha. The core pieces (job chunking, fault-tolerant queue, cost-model scheduler, availability predictor, credit ledger, simulator) work and are tested. **Remote execution works** between laptops on the same LAN, and laptops **find each other automatically** over mDNS (or you can give addresses explicitly).

## Why

Most of the time, most laptops are idle. Tools like Ray and Dask can spread work across machines, but they're built for clusters you control. Personal laptops behave differently:

- **They come and go.** Lids close, Wi-Fi drops, batteries die.
- **Their owners come back.** A job must get out of the way the moment someone starts typing.
- **Hardware is mixed.** One friend has an NVIDIA GPU and 16 GB of RAM, one has a MacBook, one has an old 8 GB ThinkPad.
- **The network is slow.** Wi-Fi is hundreds of times slower than a GPU's PCIe bus, so remote work only pays off for compute-heavy, data-light tasks.

IdleMesh is built around those constraints.

## Features

| Feature | What it does | Status |
|---|---|---|
| Chunked jobs | Large jobs are split into small chunks, so a lost laptop loses only a little work | ✅ |
| Fault-tolerant queue | Chunks are *leased*; if a node disappears, its chunks are requeued | ✅ |
| Cost-model scheduler | Runs each chunk wherever it will finish first: `latency + transfer + compute + queue wait`, with measured speed, round trips and bandwidth | ✅ |
| Backup tasks | Near the end of a job, idle workers run copies of slow chunks; the first result wins, so a slow laptop can't hold the job up | ✅ |
| Availability predictor | Learns each laptop's weekly routine and avoids putting long chunks on machines about to get busy | ✅ |
| Simulator | Replays usage traces to measure how much work the predictor saves | ✅ |
| Credit ledger | Lend compute to earn credits, borrow to spend them; credits set priority | ✅ |
| Idle detection | CPU load and battery state today; keyboard/mouse idle time next | 🟡 |
| Memory-aware scheduling | Tasks declare how much RAM they need (`map(..., memory="12GB")`); each laptop only runs as many as fit in its free RAM | ✅ |
| Memory protection | Caps each job's RAM and pauses or stops it when the owner's own apps need memory back | ⏳ Phase 4 |
| Zero-config discovery | Laptops find each other over mDNS; nobody types an IP address. Only an opaque ID is broadcast, never your name | ✅ |
| Remote execution | Ship chunks to peer laptops over a signed ZeroMQ channel; a lost laptop's chunks are rerun elsewhere | ✅ |
| Sandboxing | Docker with CPU, memory and filesystem limits | ⏳ Phase 5 |
| GPU chunks | CuPy/PyTorch on Linux or WSL2 with NVIDIA | ⏳ Phase 5 |
| Testing UI | `idlemesh ui`: lend this laptop, see laptops found, run built-in demos and watch chunks split between laptops live | ✅ |
| Dashboard | Fuller view with credits, history and charts | ⏳ Phase 6 |
| Shared data cache | Each laptop keeps one in-memory copy of a dataset that many chunks reuse, instead of re-sending it over Wi-Fi | 💭 Later |

## Quickstart

```bash
git clone https://github.com/VaamanBansal/idlemesh.git
cd idlemesh
pip install -e ".[dev]"

idlemesh status                          # is this machine lendable right now?
idlemesh simulate                        # naive vs prediction-aware scheduling
python examples/monte_carlo_option.py    # price an option across worker processes
pytest
```

## Try it on two laptops

Both laptops need to be on the same Wi-Fi, run the same Python `major.minor` (for example both 3.12; `python --version` shows it), and share the same secret. Anyone with the secret can run code on the agent laptop, so pick a long random one and only share it with people you trust.

**On both laptops:**

```bash
git clone https://github.com/VaamanBansal/idlemesh.git
cd idlemesh
pip install -e ".[net]"
```

### Testing with the UI

The quickest way to try two laptops is the built-in web page. It runs on each laptop, only for the person sitting at it (it listens on `127.0.0.1`, never on the Wi-Fi).

**On both laptops**, after installing as above:

```bash
idlemesh ui
```

Your browser opens the page. If it doesn't, open the link printed in the terminal (it contains a private token; don't share it).

1. **Both:** type the same shared secret in **Setup** and press **Save**. Leave the mesh name as `default` unless your group uses its own.
2. **Your friend:** switch on **Lend this laptop**. (If Windows asks about the firewall, click **Allow** for private networks.)
3. **You:** wait for your friend's laptop to show up under **Laptops found** as *compatible*. If it doesn't appear, your Wi-Fi may block discovery: type their IP into **Add by IP**.
4. **You:** pick a demo (option pricing, backtest sweep or ML hyperparameter search) and a size, choose **Compare both**, and press **Run**.

You'll see a live progress bar and per-laptop chunk counts. When it finishes, the page shows both times, the speedup, whether the results matched, and the demo's answer.

![The idlemesh ui page during a two-laptop run](docs/images/ui.png)
<!-- TODO: replace docs/images/ui.png with a screenshot from a real two-laptop "Compare both" run. -->

The ML demo needs scikit-learn on **every** laptop (`pip install scikit-learn`); the page shows it greyed out until it's installed. The secret you type stays in memory: it isn't saved to disk, logged, or shown on the page again.

The rest of this section does the same thing from the command line and from Python.

Set the secret in the terminal you'll use (an environment variable keeps it out of your shell history and the process list):

```bash
export IDLEMESH_SECRET="correct-horse-battery-staple-42"      # macOS / Linux
$env:IDLEMESH_SECRET = "correct-horse-battery-staple-42"      # Windows PowerShell
```

**On the friend's laptop (the one lending compute):**

```bash
idlemesh serve
```

It listens on port 7450 and announces itself on the LAN. Allow incoming connections if the firewall asks (on Windows, allow Python on **private** networks). The agent refuses work while the laptop is busy or on battery; add `--force` to skip that check while testing. Use `--name` to choose the name your mesh sees (default: the hostname) and `--mesh NAME` if your group uses its own mesh name.

**On your laptop**, check that you can see it:

```bash
idlemesh nodes
```

```
ID            NAME          ADDRESS             WORKERS  PYTHON  LENDABLE  COMPATIBLE
3f9a0c1b2d4e  aahav-laptop  192.168.1.20:7450   8        3.12    yes       yes
```

Then run work on it:

```python
# mesh_demo.py
import random

from idlemesh import Mesh


def simulate(seed):
    rng = random.Random(seed)
    return sum(rng.random() for _ in range(2_000_000))


if __name__ == "__main__":
    with Mesh(discover=True) as mesh:  # finds agents on the LAN; reads IDLEMESH_SECRET
        results = mesh.map(simulate, range(400), chunk_size=10)
        print(len(results), "results;", "chunks per node:", mesh.last_stats)
```

```bash
python mesh_demo.py
```

Chunks run on the friend's laptop and on your own worker processes at the same time, and results come back in input order. Discovery keeps running during `map()`: a laptop that starts `idlemesh serve` mid-job joins in, and one that closes its lid has its chunks rerun elsewhere. If the function raises an exception on the friend's laptop, `map()` raises `idlemesh.protocol.RemoteError` with the remote traceback.

**If discovery doesn't find anything**, give the address explicitly instead. Find it with `ipconfig` (Windows), `ipconfig getifaddr en0` (macOS) or `hostname -I` (Linux):

```python
mesh = Mesh(nodes=["192.168.1.20"])                  # port defaults to 7450; "192.168.1.20:7451" also works
mesh = Mesh(nodes=["192.168.1.20"], discover=True)   # or both: explicit nodes plus whatever is found
```

Notes:

- Define your function in the script you run (or in a package installed on both laptops). Functions from other local modules are sent by reference, so the agent must be able to import them; `cloudpickle.register_pickle_by_value(module)` ships a module by value instead.
- Both laptops must run the same Python `major.minor`, because cloudpickle's format differs between versions. A mismatched laptop is skipped with a message like `Skipping aahav-laptop (192.168.1.20:7450): it runs Python 3.13, you run 3.10. Use the same Python version on both laptops.`
- Every message is signed (HMAC-SHA256) and timestamped. If the two laptops' clocks differ by more than 5 minutes, messages are rejected and the agent logs a warning saying so: turn on automatic time sync.
- Traffic is signed, not encrypted: someone else on the Wi-Fi can read inputs and results, but can't forge or replay jobs.

### Troubleshooting discovery

- Discovery uses mDNS: multicast UDP on port **5353**. **Campus and office Wi-Fi often block it** (and often block laptops from reaching each other at all). A **phone hotspot** works around both; explicit `nodes=["IP"]` work whenever the laptops can reach each other.
- **Windows:** the first time `idlemesh serve` (or `idlemesh nodes`) runs, allow Python through the firewall on **private** networks, and make sure the Wi-Fi network is set to *Private*, not *Public*.
- `idlemesh nodes` lists agents but `NAME` and `LENDABLE` show `?`: set `IDLEMESH_SECRET`, because those are only shared with mesh members. `? (no reply)` with the secret set means a different secret, or a firewall blocking port 7450.
- Two friend groups on one Wi-Fi: give each its own `--mesh NAME` (or `IDLEMESH_MESH`) so they don't use each other's laptops.

## Smart about when friends need their laptops back

Every laptop keeps a minute-by-minute log of whether it was idle. From that log, the predictor estimates *"what's the chance this machine stays free for the next 45 minutes, given it's Friday at 11 PM?"* The scheduler then inflates each node's expected cost by the risk of being reclaimed:

```
expected_cost = estimated_time / P(stays idle for that long)
```

So long chunks drift toward reliable machines, and short chunks can go anywhere.

**Privacy:** the activity log never leaves the owner's laptop. Only the probability is shared (see [Privacy](#privacy)).

Results from `idlemesh simulate` (three synthetic laptop routines; two weeks of training, one week replayed; 45-minute chunks):

| Policy | Wasted compute (seed 0) | Range over 5 seeds |
|---|---|---|
| Naive (any idle node) | 3.7% | 3.7–4.8% |
| Prediction-aware | 1.7% | 1.7–2.7% |

Prediction roughly **halves the work lost to owners reclaiming their laptops**, at the cost of placing somewhat fewer long chunks. The simulator is the place to explore that trade-off.

## A scheduler that keeps up with slow laptops

A job finishes when its slowest chunk does. Early two-laptop tests showed the problem: with few, long tasks, one laptop finished its share and sat idle while the friend's slower laptop worked through the last chunks.

| Run (ML hyperparameter search, compare both) | Alone | With friend | Speedup |
|---|---|---|---|
| medium (friend: 16 workers, sent 17 chunks) | 20.2 s | 23.9 s | 0.85× |
| large | 42.9 s | 41.3 s | 1.04× |

The scheduler now does five things about it:

- **Backup tasks.** When nothing is left to hand out and this laptop has idle workers while chunks are still running elsewhere, it starts copies of the most overdue ones (judged by each laptop's measured speed) and keeps whichever result arrives first. An idle, faster friend can take a backup too. `mesh.last_stats.backups_launched` and `.backups_won` say how often this happened.
- **Measured speed.** Each laptop's compute time per chunk is measured from the agent's own timings and kept as a running average (`mesh.last_stats.seconds_per_chunk`). Slow laptops get fewer chunks near the end, and none that this laptop would finish sooner.
- **No queueing near the end.** A laptop only gets more chunks than it has workers while there's plenty of work left; near the end, no chunk ever waits behind a busy worker on another laptop.
- **Longest tasks first.** `map(..., cost=fn)` takes your estimate of how expensive each input is and starts the expensive ones first, so the long tasks aren't the ones still running at the end. Results still come back in input order. The built-in ML demo uses it; its cost function was fitted to measured training times (bigger networks and smaller learning rates take longer). `cost=` only helps when task times vary: in the backtest demo every strategy takes the same time, so it has none.
- **Network awareness.** Round trips are measured continuously and the 90th percentile, not the average, decides end-of-job placement, so a laptop that's usually quick but sometimes stalls isn't trusted with the last chunks. Short tasks over a slow link are sent several per message (aiming for network time ≤ 10% of compute); long tasks always go one at a time. Bandwidth is measured too, so jobs with large inputs prefer nearby laptops or stay local. A laptop whose network costs more than it helps is skipped for that job, with the reason in the log and the UI (e.g. `skipped aahav-laptop: network delay too high for these short tasks`).

```python
mesh = Mesh(discover=True)
results = mesh.map(train, configs, chunk_size=1, cost=lambda cfg: cfg.epochs * cfg.width)
print(mesh.last_stats)                    # chunks per laptop
print(mesh.last_stats.backups_won)        # backup copies that finished first
print(mesh.last_stats.seconds_per_chunk)  # measured speed per laptop
```

> **Functions with side effects:** with backups, a chunk can run twice (the slower copy's result is thrown away). If your function writes files, sends messages or charges money, use `Mesh(speculative=False)`.

Measured speedups, before and after, are in [Measured results](#measured-results).

## Measured results

Real runs on campus Wi-Fi with three laptops (this laptop, `LAPTOP-427SH62D` and Aahav's), Phase 3 merged, every demo at **Large · Compare both**. Results matched the local run every time.

> These numbers were measured **before** the ML demo was split into one task per (configuration, fold) pair, so ML Large was 54 tasks of 15–28 s each instead of today's 540 tasks of about 1.5–3 s, and before longest tasks were steered to the fastest laptop. With 54 tasks and about 56 workers across the three laptops, the ML job was bounded by its longest tasks landing on the slower laptops (the lower bound on these laptops was about 23–24 s, against the 29.6 s measured). Both changes target exactly that; new numbers will replace these after a rerun.

| Demo | Tasks | Alone | Mesh | Speedup | Per-laptop chunks (s/chunk) |
|---|---|---|---|---|---|
| ML hyperparameter search | 54 (15–28 s each) | 42.3 s | 29.6 s | 1.43× | this laptop 24 (14.39), LAPTOP-427SH62D 16 (22.19), Aahav 14 (23.27); backups 15 launched, 1 won |
| Option pricing | 192 (~0.3 s) | 3.5 s | 2.6 s | 1.37× | this laptop 126 (0.30), LAPTOP-427SH62D 44 (0.38), Aahav 22 (0.63) |
| Backtest sweep | 46 (~6 s) | 11.3 s | 8.3 s | 1.36× | this laptop 24 (6.08), LAPTOP-427SH62D 22 (6.43); Aahav ran nothing; backups 19 launched, 11 won |

Aahav's laptop was compatible but ran nothing in the backtest run, and the page didn't say why. Every laptop that runs nothing now gets a reason in `mesh.last_stats.skipped` and on the page (for example "not lendable at start of run" or "skipped: network delay too high for these short tasks").

### Before / after Phase 3

Two laptops (this laptop and Aahav's), same Wi-Fi, ML hyperparameter search at **Large · Compare both** (54 tasks, before the per-fold split):

| | Alone | With friend | Speedup |
|---|---|---|---|
| Before Phase 3 | 42.9 s | 41.3 s | 1.04× |
| After Phase 3 | 44.0 s | 32.9 s | 1.34× |

Other two-laptop runs after Phase 3 reached 1.19× and 1.34×. Aahav's laptop went from 37.5 to 28.2 s per task, and to 23.3 s once plugged in. A three-laptop run with Aahav on battery got 1.24×.

### Tips for good speedups

- **Keep lending laptops plugged in, and on Windows set Power mode to *Best performance*** (Settings → System → Power & battery). One laptop went from 23 s to 28+ s per task on battery.
- **Aim for at least ~5× more tasks than total workers across the mesh.** The scheduler can only balance work it hasn't handed out yet: with about as many tasks as workers, the job ends when the slowest laptop's last task does. Smaller tasks (a smaller `chunk_size`, or splitting work the way the ML demo now splits folds) give it room.
- **Very short jobs (a few seconds) gain less.** Starting worker processes and the first round trips cost a fixed second or so, whatever the job size.
- Give `cost=` when task times vary a lot, so the longest tasks start first and go to the fastest laptop.

## Memory-aware scheduling (not RAM sharing)

IdleMesh does **not** let your laptop use a friend's RAM as its own. Over Wi-Fi, a round trip takes milliseconds while local RAM answers in about 100 nanoseconds, so remote memory would be slower than your own SSD.

Instead, IdleMesh **runs the job where the RAM is**. You say how much memory one task needs, each laptop reports its free RAM, and the scheduler only runs as many tasks on a laptop at once as fit:

```python
results = Mesh(discover=True).map(backtest, params, chunk_size=1, memory="12GB")
```

If your laptop has 8 GB free and a task needs 12, the tasks run on the friend with 16. If no laptop can fit even one task, `map()` raises `MemoryError` before running anything, listing each laptop's free RAM. If a laptop's free RAM drops after a task was sent, its agent replies BUSY and the task goes elsewhere. `memory=` accepts bytes or strings like `"512MB"` and `"4GB"`. Capping a running task's memory so it backs off when the owner needs RAM comes in Phase 4.

## How it works

```
┌──────────── your laptop ────────────┐        ┌──────── friend's laptop ────────┐
│  your code ── Mesh.map() ──┐        │        │  agent                          │
│                            ▼        │  LAN   │   ├─ discovery (mDNS)           │
│  coordinator                        │◄──────►│   ├─ idle monitor               │
│   ├─ chunk queue (leases)           │        │   ├─ activity log + predictor   │
│   ├─ scheduler (cost model + risk)  │        │   └─ sandboxed runner           │
│   └─ credit ledger                  │        └─────────────────────────────────┘
└─────────────────────────────────────┘
```

See [docs/architecture.md](docs/architecture.md) for details.

## Repository layout

```
idlemesh/
  agent/        discovery, idle monitor, `serve` agent (sandbox runner coming)
  coordinator/  chunk queue, cost-model scheduler, credit ledger
  predictor/    predict_idle() interface + hour-of-week model
  sdk/          Mesh.map() and @task; remote.py drives agents + the local pool;
                discover.py decides which discovered agents to use
  demos/        built-in workloads (option, backtest, ml) shared by the UI and examples
  ui/           `idlemesh ui`: local web page (FastAPI + plain HTML/JS)
  simulator/    trace replay for comparing scheduling policies
  protocol.py   chunk format, serialization, signed message codec
  cli.py        idlemesh status | serve | nodes | ui | simulate
examples/       option pricing, backtest sweep, ML hyperparameter search
docs/           architecture and security notes
tests/
```

## Roadmap

- [x] **Phase 0:** repo, CI, core data structures
- [x] **Phase 1:** remote execution between two laptops (ZeroMQ job channel)
- [x] **Phase 2:** mDNS discovery wired to the coordinator, heartbeats, Python/protocol version checks
- [x] Basic testing UI (`idlemesh ui`)
- [x] **Phase 3: smarter scheduler.** Backup tasks at the end of a job, measured per-laptop speed, no queued chunks near the end, longest tasks first (`cost=`), memory-aware placement (`memory=`), network-aware batching and placement (RTT p90, bandwidth). See [docs/phase3.md](docs/phase3.md).
- [ ] **Phase 3.5: model workers.** `setup=` loads a model once per worker and keeps it in memory across tasks; `per_node=` limits concurrent tasks per laptop; the scheduler counts load time; memory is released when idle or when lending stops; a failed `setup` marks that laptop as unable to run the job instead of failing it.
- [ ] **Phase 4: respecting owners and sharing fairly.** Keyboard/mouse idle detection on Linux, macOS and Windows; pause on owner activity; per-job memory caps that back off when the owner needs RAM; credits, and fair turns between several people using the mesh at once.
- [ ] **Phase 4.5:** live activity logging feeding the availability predictor
- [ ] **Phase 5: shared group models and result cache.** A laptop can host a model for the whole group (`idlemesh host`); everyone's tasks that need it go there and take fair turns; results are cached under a fingerprint of function, input, model version and settings, so work one group member already did is never recomputed. Built for small teams running the same experiments, e.g. one laptop hosting a vision-language model and another hosting an LLM for a shared RAG project.
- [ ] **Phase 6:** GPU awareness (NVIDIA and Apple Silicon) and `requires={"gpu": True}`; Docker sandboxing
- [ ] **Phase 7:** fuller web dashboard (credits, history, charts), more examples, PyPI release
- [ ] **Later:** distributed training of one small/medium model by syncing rarely (local SGD / DiLoCo-style weight averaging over Wi-Fi)
- [ ] **Later:** shared in-memory data cache so chunks reuse datasets instead of re-transferring them
- [ ] **Later:** SQLite-backed queue (deferred: `map()` runs in-process, so there's nothing to persist yet)

**Scope:** IdleMesh speeds up batch work across a few trusted laptops: simulations, sweeps, and batch inference with models each laptop can hold. It doesn't split a single model across machines or serve live traffic.

## How it compares

| | IdleMesh | Ray / Dask | HTCondor | BOINC | Exo |
|---|---|---|---|---|---|
| Target | A few friends' laptops | Clusters you control | Institutional pools | Anonymous volunteers | Local LLM inference |
| Setup | Zero config (mDNS) | Head node + config | Admin setup | Project server | Zero config |
| Owner returns to machine | Pause + predict | Not a concern | Evicts jobs | Pauses | Not a concern |
| Predicts availability | ✅ | ❌ | ❌ | ❌ | ❌ |
| Memory-aware placement | ✅ | ✅ | ✅ | ✅ | ✅ |

IdleMesh doesn't try to replace any of these. If you have a real cluster, use Ray. IdleMesh is for the case where "the cluster" is three laptops in a hostel room.

## Privacy

**Broadcast to everyone on the network** while `idlemesh serve` runs. This goes out over mDNS, so any device on the same Wi-Fi can read it, including people outside your mesh:

| Field | Example | Why |
|---|---|---|
| Node ID | `3f9a0c1b2d4e` | A random ID generated once per machine and stored in `~/.idlemesh/node_id`. It says nothing about the machine or its owner; delete the file to get a new one |
| IP address and port | `192.168.1.20:7450` | So peers can connect |
| Mesh name | `default` | So friend groups sharing a Wi-Fi don't collide. Don't put anything personal in it |
| Python and IdleMesh versions | `3.12`, `0.1.0`, protocol `2` | So incompatible laptops are skipped with a clear message |
| Worker count | `8` | So peers know how much work to send |

**Never broadcast:** your hostname, the `--name` you chose, whether the laptop is idle or busy right now, the secret or anything derived from it, and your activity log. IdleMesh doesn't announce your hostname, but your operating system may do so on its own; that's outside IdleMesh's control.

**Shared only with people who have the secret**, in the signed handshake reply when they ping your agent: the node name (`--name`, default the hostname), whether the laptop is lendable right now, and its worker count and versions. `idlemesh nodes` fills in those columns only when you have the secret. Messages are signed, not encrypted, so someone capturing Wi-Fi traffic while a mesh member pings you could read that reply.

**Never leaves your laptop:** the minute-by-minute activity log. Only availability probabilities computed from it are used for scheduling.

## Security

IdleMesh ships Python functions between machines with `cloudpickle`. **Unpickling runs code**, so only join a mesh with people you trust. Every message is signed with the mesh's shared secret (HMAC-SHA256), checked for age and replay, and verified *before* anything is unpickled. Docker sandboxing (Phase 5) adds a second layer. See [docs/security.md](docs/security.md).

## Contributing

Contributions are welcome. Look for issues labelled `good first issue`. Some ideas:

- Keyboard/mouse idle time on your OS (`idlemesh/agent/idle.py`)
- A better availability predictor (anything implementing `predict_idle`)
- Apple Metal support for GPU chunks
- A terminal UI dashboard
- Free-RAM reporting and memory-aware placement in the scheduler

Run `ruff check . && ruff format --check . && pytest` before opening a PR.

## License

[MIT](LICENSE)
