"""Command line: `idlemesh status | serve | nodes | ui | simulate`."""

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


def list_agents(browser, mesh: str, timeout: float, secret: str | None) -> dict:
    """Browse for `timeout` s, then (with the secret) ping each agent for its name and
    lendability. Returns {"rows": [...], "other_meshes": set, "own_id": str | None}."""
    import threading
    import time

    from idlemesh.agent.discovery import local_networks, node_id, pick_address
    from idlemesh.protocol import PROTOCOL_VERSION, python_version

    found, lock = {}, threading.Lock()

    def on_event(kind, instance, advert):
        with lock:
            if kind == "removed" or advert is None:
                found.pop(instance, None)
            else:
                found[instance] = advert

    browser.start(on_event)
    try:
        time.sleep(timeout)
    finally:
        browser.stop()
    with lock:
        adverts = list(found.values())
    nets, own = local_networks(), node_id(create=False)
    other_meshes = {a.mesh for a in adverts if a.mesh != mesh}
    rows = []
    for a in sorted((a for a in adverts if a.mesh == mesh), key=lambda a: a.node_id):
        ok_py, ok_proto = a.python == python_version(), a.protocol == PROTOCOL_VERSION
        if ok_py and ok_proto:
            compatible = "yes"
        elif not ok_proto:
            compatible = f"no (IdleMesh {a.version})"
        else:
            compatible = f"no (Python {a.python})"
        rows.append(
            {
                "id": a.node_id + (" (this laptop)" if a.node_id == own else ""),
                "name": "?",
                "address": f"{pick_address(a.addresses, nets)}:{a.port}",
                "workers": str(a.workers),
                "python": a.python,
                "lendable": "?",
                "compatible": compatible,
            }
        )
    if secret and rows:
        _probe(rows, secret)
    return {"rows": rows, "other_meshes": other_meshes, "own_id": own}


def _probe(rows: list[dict], secret: str, timeout: float = 2.0) -> None:
    """PING every agent at once; fill in name and lendable from the signed PONGs."""
    import time

    import zmq

    from idlemesh.protocol import PROTOCOL_VERSION, Codec, MsgType, dumps, loads, python_version

    codec, ctx, poller, socks = Codec(secret), zmq.Context.instance(), zmq.Poller(), {}
    hello = dumps({"python": python_version(), "protocol": PROTOCOL_VERSION})
    for row in rows:
        s = ctx.socket(zmq.DEALER)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(f"tcp://{row['address']}")
        s.send_multipart(codec.encode(MsgType.PING, "nodes", hello))
        poller.register(s, zmq.POLLIN)
        socks[s] = row
        row["lendable"] = "? (no reply)"
    deadline, pending = time.monotonic() + timeout, set(socks)
    while pending and (left := deadline - time.monotonic()) > 0:
        for s, _ in poller.poll(int(left * 1000)):
            msg = codec.decode(s.recv_multipart())
            if msg is None or msg.type is not MsgType.PONG:
                continue
            info = loads(msg.payload)
            socks[s]["name"] = str(info.get("name") or "?")
            socks[s]["lendable"] = "yes" if info.get("lendable") else "no"
            pending.discard(s)
    for s in socks:
        s.close()


def cmd_nodes(args: argparse.Namespace) -> None:
    try:
        from idlemesh.agent.discovery import ZeroconfBrowser
    except ImportError as err:
        raise SystemExit("networking extras missing: pip install 'idlemesh[net]'") from err
    from idlemesh.sdk.discover import no_agents_hint

    mesh = args.mesh or os.environ.get("IDLEMESH_MESH") or "default"
    secret = args.secret or os.environ.get("IDLEMESH_SECRET")
    print(f'Looking for IdleMesh agents in mesh "{mesh}" for {args.timeout:g}s...')
    out = list_agents(ZeroconfBrowser(), mesh, args.timeout, secret)
    rows = out["rows"]
    if not rows:
        print(no_agents_hint(mesh, out["other_meshes"], "--mesh NAME"))
        return
    cols = ["id", "name", "address", "workers", "python", "lendable", "compatible"]
    widths = {c: max(len(c), *(len(r[c]) for r in rows)) for c in cols}
    print("  ".join(c.upper().ljust(widths[c]) for c in cols))
    for r in rows:
        print("  ".join(r[c].ljust(widths[c]) for c in cols))
    if not secret:
        print(
            "\nName and lendable are only shared with mesh members: "
            "set IDLEMESH_SECRET (or pass --secret) to see them."
        )
    elif any(r["lendable"].startswith("? (no reply") for r in rows):
        print("\nNo reply means a different secret, or a firewall blocking the agent's port.")
    if out["other_meshes"]:
        names = ", ".join(sorted(out["other_meshes"]))
        print(f"Also on this network, other meshes: {names}")


def cmd_ui(args: argparse.Namespace) -> None:
    try:
        from idlemesh.ui.server import serve_ui
    except ImportError as err:
        raise SystemExit("the UI needs the networking extras: pip install 'idlemesh[net]'") from err
    serve_ui(port=args.port, open_browser=not args.no_browser)


def cmd_simulate(args: argparse.Namespace) -> None:
    from idlemesh.simulator.simulate import main as sim_main

    sim_main(seed=args.seed)


def main() -> None:
    p = argparse.ArgumentParser(prog="idlemesh")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="{status,serve,nodes,ui,simulate}")
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
    nd = sub.add_parser("nodes", help="list IdleMesh agents on this network")
    nd.add_argument("--mesh", help="mesh to list (default: $IDLEMESH_MESH or 'default')")
    nd.add_argument("--timeout", type=float, default=3.0, help="seconds to browse (default 3)")
    nd.add_argument("--secret", help="shared mesh secret (default: $IDLEMESH_SECRET)")
    nd.set_defaults(f=cmd_nodes)
    ui = sub.add_parser("ui", help="open a local web page to lend this laptop and run demos")
    ui.add_argument("--port", type=int, default=8765, help="local port (default 8765)")
    ui.add_argument("--no-browser", action="store_true", help="don't open the browser")
    ui.set_defaults(f=cmd_ui)  # deliberately no --host: the UI only ever binds to 127.0.0.1
    s = sub.add_parser("simulate", help="compare naive vs prediction-aware scheduling")
    s.add_argument("--seed", type=int, default=0)
    s.set_defaults(f=cmd_simulate)
    args = p.parse_args()
    args.f(args)


if __name__ == "__main__":
    main()
