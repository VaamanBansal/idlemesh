"""Command line: `idlemesh status | join | serve | simulate`."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket


def cmd_status(_: argparse.Namespace) -> None:
    from idlemesh.agent.idle import IdleMonitor

    snap = IdleMonitor().snapshot()
    print(f"host:        {socket.gethostname()}")
    print(f"cpu:         {snap.cpu_percent:.1f}%")
    print(f"on battery:  {snap.on_battery}")
    print(f"lendable:    {snap.lendable()}")


def cmd_join(_: argparse.Namespace) -> None:
    raise SystemExit(
        "`idlemesh join` has been replaced.\n"
        "  To lend this laptop:      idlemesh serve   (it advertises itself on the LAN)\n"
        "  To see laptops near you:  idlemesh nodes"
    )


def cmd_serve(args: argparse.Namespace) -> None:
    try:
        from idlemesh.agent.server import Agent
    except ImportError as err:
        raise SystemExit("networking extras missing: pip install 'idlemesh[net]'") from err
    secret = args.secret or os.environ.get("IDLEMESH_SECRET")
    if not secret:
        raise SystemExit("a shared secret is required: --secret or the IDLEMESH_SECRET env var")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    mesh = args.mesh or os.environ.get("IDLEMESH_MESH") or "default"

    def _stop(*_):
        raise KeyboardInterrupt  # unwind serve_forever so it unregisters from mDNS

    signal.signal(signal.SIGTERM, _stop)
    agent = Agent(
        args.port,
        secret,
        workers=args.workers,
        force=args.force,
        host=args.host,
        name=args.name,
        mesh=mesh,
        advertise=not args.no_advertise,
    )
    try:
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
    j = sub.add_parser("join")  # Phase 0 command, kept only to point people at serve/nodes
    j.add_argument("--name")
    j.add_argument("--port")
    j.set_defaults(f=cmd_join)
    sv = sub.add_parser("serve", help="run chunks sent by other laptops")
    sv.add_argument("--port", type=int, default=7450)
    sv.add_argument("--host", default="*", help="interface to listen on (default: all)")
    sv.add_argument("--secret", help="shared mesh secret (default: $IDLEMESH_SECRET)")
    sv.add_argument("--workers", type=int, help="worker processes (default: CPU count)")
    sv.add_argument("--force", action="store_true", help="accept work even when not lendable")
    sv.add_argument(
        "--name", help="name shown to mesh members (default: hostname); never broadcast"
    )
    sv.add_argument("--mesh", help="mesh to join (default: $IDLEMESH_MESH or 'default')")
    sv.add_argument(
        "--no-advertise", action="store_true", help="don't announce this agent via mDNS"
    )
    sv.set_defaults(f=cmd_serve)
    s = sub.add_parser("simulate", help="compare naive vs prediction-aware scheduling")
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(f=cmd_simulate)
    args = p.parse_args()
    args.f(args)


if __name__ == "__main__":
    main()
