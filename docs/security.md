# Security model

IdleMesh is designed for **small groups of people who trust each other**, not for strangers on the internet.

## Threats and mitigations

| Risk | Today | Planned |
|---|---|---|
| A peer sends malicious code | Functions are unpickled with `cloudpickle`, which **executes code**. Only join meshes with people you trust. | Phase 5: run chunks in Docker with no host mounts, CPU/memory limits and an image allowlist |
| Someone on the same Wi-Fi joins uninvited | Nothing yet | Shared mesh secret + mutual authentication on the job channel |
| A job slows down the owner's laptop | Agent refuses work when CPU is busy or on battery | Phase 4: pause within seconds of keyboard/mouse activity; per-job resource caps |
| Usage habits leak to friends | Activity log stays on the owner's machine; only a probability is shared | Optional coarsening/noise on shared probabilities |

## Reporting

Please report security issues privately through GitHub's "Report a vulnerability" button rather than opening a public issue.
