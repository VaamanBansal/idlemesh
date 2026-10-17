"""Command line: `idlemesh status | join | serve | simulate`."""

from __future__ import annotations

import argparse
import logging
import os
import socket


def cmd_status(_: argparse.Namespace) -> None:
    from idlemesh.agent.idle import IdleMonitor

    snap = IdleMonitor().snapshot()
    print(f"host:        {socket.gethostname()}")
    print(f"cpu:         {snap.cpu_percent:.1f}%")
    print(f"on battery:  {snap.on_battery}")
    print(f"lendable:    {snap.lendable()}")


def cmd_join(args: argparse.Namespace) -> None:
    try:
        from idlemesh.agent.discovery import advertise
    except ImportError as err:
        raise SystemExit("networking extras missing: pip install 'idlemesh[net]'") from err
    zc, _ = advertise(args.name or socket.gethostname(), args.port)
    print(f"Advertising on the LAN as '{args.name or socket.gethostname()}'. Ctrl+C to leave.")
    try:
        import time

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        zc.close()


def cmd_serve(args: argparse.Namespace) -> None:
    try:
        from idlemesh.agent.server import Agent
    except ImportError as err:
        raise SystemExit("networking extras missing: pip install 'idlemesh[net]'") from err
    secret = args.secret or os.environ.get("IDLEMESH_SECRET")
    if not secret:
        raise SystemExit("a shared secret is required: --secret or the IDLEMESH_SECRET env var")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        agent = Agent(args.port, secret, workers=args.workers, force=args.force, host=args.host)
        agent.serve_forever()
    except KeyboardInterrupt:
        pass


def cmd_simulate(args: argparse.Namespace) -> None:
    from idlemesh.simulator.simulate import main as sim_main

    sim_main(seed=args.seed)


def main() -> None:
    p = argparse.ArgumentParser(prog="idlemesh")
    sub = p.add_subparsers(dest="cmd", required=True)
    st = sub.add_parser("status", help="show whether this machine is lendable")
    st.set_defaults(f=cmd_status)
    j = sub.add_parser("join", help="advertise this machine on the LAN")
    j.add_argument("--name")
    j.add_argument("--port", type=int, default=7450)
    j.set_defaults(f=cmd_join)
    sv = sub.add_parser("serve", help="run chunks sent by other laptops")
    sv.add_argument("--port", type=int, default=7450)
    sv.add_argument("--host", default="*", help="interface to listen on (default: all)")
    sv.add_argument("--secret", help="shared mesh secret (default: $IDLEMESH_SECRET)")
    sv.add_argument("--workers", type=int, help="worker processes (default: CPU count)")
    sv.add_argument("--force", action="store_true", help="accept work even when not lendable")
    sv.set_defaults(f=cmd_serve)
    s = sub.add_parser("simulate", help="compare naive vs prediction-aware scheduling")
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(f=cmd_simulate)
    args = p.parse_args()
    args.f(args)


if __name__ == "__main__":
    main()
