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


results = Mesh().map(price_paths, range(1_000), chunk_size=10)
```

> **Status:** pre-alpha. The core pieces (job chunking, fault-tolerant queue, cost-model scheduler, availability predictor, credit ledger, simulator) work and are tested, but they currently run on **local worker processes**. Remote execution across laptops arrives in Phase 1 of the [roadmap](#roadmap).

## Why

Most of the time, most laptops are idle. Tools like Ray and Dask can spread work across machines, but they're built for clusters you control. Personal laptops behave differently:

- **They come and go.** Lids close, Wi-Fi drops, batteries die.
- **Their owners come back.** A job must get out of the way the moment someone starts typing.
- **Hardware is mixed.** One friend has an NVIDIA GPU, one has a MacBook, one has an old ThinkPad.
- **The network is slow.** Wi-Fi is hundreds of times slower than a GPU's PCIe bus, so remote work only pays off for compute-heavy, data-light tasks.

IdleMesh is built around those constraints.

## Features

| Feature | What it does | Status |
|---|---|---|
| Chunked jobs | Large jobs are split into small chunks, so a lost laptop loses only a little work | ✅ |
| Fault-tolerant queue | Chunks are *leased*; if a node disappears, its chunks are requeued | ✅ |
| Cost-model scheduler | Runs each chunk wherever it will finish first: `latency + transfer + compute + queue wait` | ✅ |
| Availability predictor | Learns each laptop's weekly routine and avoids putting long chunks on machines about to get busy | ✅ |
| Simulator | Replays usage traces to measure how much work the predictor saves | ✅ |
| Credit ledger | Lend compute to earn credits, borrow to spend them; credits set priority | ✅ |
| Idle detection | CPU load and battery state today; keyboard/mouse idle time next | 🟡 |
| Zero-config discovery | Laptops find each other over mDNS; nobody types an IP address | 🟡 |
| Remote execution | Ship chunks to peer laptops | ⏳ Phase 1 |
| Sandboxing | Docker with CPU, memory and filesystem limits | ⏳ Phase 5 |
| GPU chunks | CuPy/PyTorch on Linux or WSL2 with NVIDIA | ⏳ Phase 5 |
| Dashboard | Live view of nodes, chunks, credits and speedup | ⏳ Phase 6 |

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

To try LAN discovery, install the networking extras on two machines on the same Wi-Fi:

```bash
pip install -e ".[net]"
idlemesh join --name my-laptop
```

## Smart about when friends need their laptops back

Every laptop keeps a minute-by-minute log of whether it was idle. From that log, the predictor estimates *"what's the chance this machine stays free for the next 45 minutes, given it's Friday at 11 PM?"* The scheduler then inflates each node's expected cost by the risk of being reclaimed:

```
expected_cost = estimated_time / P(stays idle for that long)
```

So long chunks drift toward reliable machines, and short chunks can go anywhere.

**Privacy:** the activity log never leaves the owner's laptop. Only the probability is shared.

Results from `idlemesh simulate` (three synthetic laptop routines; two weeks of training, one week replayed; 45-minute chunks):

| Policy | Wasted compute (seed 0) | Range over 5 seeds |
|---|---|---|
| Naive (any idle node) | 3.7% | 3.7–4.8% |
| Prediction-aware | 1.7% | 1.7–2.7% |

Prediction roughly **halves the work lost to owners reclaiming their laptops**, at the cost of placing somewhat fewer long chunks. The simulator is the place to explore that trade-off.

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
  agent/        discovery, idle monitor (sandbox runner coming)
  coordinator/  chunk queue, cost-model scheduler, credit ledger
  predictor/    predict_idle() interface + hour-of-week model
  sdk/          Mesh.map() and @task
  simulator/    trace replay for comparing scheduling policies
  protocol.py   chunk format and serialization
  cli.py        idlemesh status | join | simulate
examples/       Monte Carlo option pricing
docs/           architecture and security notes
tests/
```

## Roadmap

- [x] **Phase 0:** repo, CI, core data structures
- [ ] **Phase 1:** remote execution between two laptops (ZeroMQ job channel)
- [ ] **Phase 2:** mDNS discovery wired to the coordinator, heartbeats, SQLite-backed queue
- [ ] **Phase 3:** on-join benchmarking of CPU speed and link bandwidth
- [ ] **Phase 4:** keyboard/mouse idle detection on Linux, macOS and Windows; pause on owner activity
- [ ] **Phase 4.5:** live activity logging feeding the predictor
- [ ] **Phase 5:** Docker sandboxing and GPU chunks
- [ ] **Phase 6:** web dashboard, more examples, PyPI release

## How it compares

| | IdleMesh | Ray / Dask | HTCondor | BOINC | Exo |
|---|---|---|---|---|---|
| Target | A few friends' laptops | Clusters you control | Institutional pools | Anonymous volunteers | Local LLM inference |
| Setup | Zero config (mDNS) | Head node + config | Admin setup | Project server | Zero config |
| Owner returns to machine | Pause + predict | Not a concern | Evicts jobs | Pauses | Not a concern |
| Predicts availability | ✅ | ❌ | ❌ | ❌ | ❌ |

IdleMesh doesn't try to replace any of these. If you have a real cluster, use Ray. IdleMesh is for the case where "the cluster" is three laptops in a hostel room.

## Security

IdleMesh ships Python functions between machines with `cloudpickle`. **Unpickling runs code**, so only join a mesh with people you trust. Docker sandboxing (Phase 5) adds a second layer. See [docs/security.md](docs/security.md).

## Contributing

Contributions are welcome. Look for issues labelled `good first issue`. Some ideas:

- Keyboard/mouse idle time on your OS (`idlemesh/agent/idle.py`)
- A better availability predictor (anything implementing `predict_idle`)
- Apple Metal support for GPU chunks
- A terminal UI dashboard

Run `ruff check . && ruff format --check . && pytest` before opening a PR.

## License

[MIT](LICENSE)
