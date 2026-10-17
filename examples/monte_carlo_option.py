"""Price a European call option by Monte Carlo, spread across the mesh.

The workload lives in idlemesh/demos/option.py (shared with `idlemesh ui`).

python examples/monte_carlo_option.py
"""

import time

from idlemesh import Mesh
from idlemesh.demos.option import DEMO, black_scholes_call

if __name__ == "__main__":
    tasks = [(seed, 20_000) for seed in range(64)]  # 64 seeds x 20,000 paths
    t0 = time.perf_counter()
    estimates = Mesh().map(DEMO.fn, tasks, chunk_size=DEMO.chunk_size)
    elapsed = time.perf_counter() - t0
    print(f"Monte Carlo price: {sum(estimates) / len(estimates):.4f}")
    print(f"Black-Scholes:     {black_scholes_call():.4f}")
    print(f"{sum(paths for _, paths in tasks):,} paths in {elapsed:.2f}s")
