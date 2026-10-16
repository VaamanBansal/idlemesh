"""Command line: `idlemesh status | join | simulate`."""

from __future__ import annotations

import argparse
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
    s = sub.add_parser("simulate", help="compare naive vs prediction-aware scheduling")
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(f=cmd_simulate)
    args = p.parse_args()
    args.f(args)


if __name__ == "__main__":
    main()
