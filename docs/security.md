# Security model

IdleMesh is designed for **small groups of people who trust each other**, not for strangers on the internet.

## Threats and mitigations

| Risk | Today | Planned |
|---|---|---|
| A peer sends malicious code | Functions are unpickled with `cloudpickle`, which **executes code**. Only join meshes with people you trust. | Phase 5: run chunks in Docker with no host mounts, CPU/memory limits and an image allowlist |
| Someone on the same Wi-Fi sends jobs uninvited | Every message (both directions) carries an HMAC-SHA256 over its type, chunk id, timestamp, nonce and payload, keyed with the shared mesh secret. Unsigned or wrongly signed messages are dropped **before** unpickling | Per-peer keys so one person can be removed without rotating everyone's secret |
| Someone records and replays a job | Messages older (or newer) than 5 minutes are rejected with a clock-sync warning; nonces seen within that window are remembered and repeats are rejected | |
| Someone on the Wi-Fi reads inputs and results | Not protected: messages are signed, not encrypted | CurveZMQ encryption on the job channel |
| A job slows down the owner's laptop | Agent refuses work when CPU is busy or on battery | Phase 4: pause within seconds of keyboard/mouse activity; per-job resource caps |
| Usage habits leak to friends | Activity log stays on the owner's machine; only a probability is shared | Optional coarsening/noise on shared probabilities |

## Reporting

Please report security issues privately through GitHub's "Report a vulnerability" button rather than opening a public issue.
