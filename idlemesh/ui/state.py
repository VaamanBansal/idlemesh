"""Everything behind `idlemesh ui`, kept in memory.

The shared secret lives only in `UIState._secret`. `snapshot()` (what the page
and the API see) never includes it, only whether one is set; it is never logged
or written to disk.
"""

from __future__ import annotations

import collections
import logging
import os
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from idlemesh._mp import pool_context
from idlemesh.agent.discovery import (
    AgentAdvert,
    Browser,
    local_networks,
    node_id,
    pick_address,
)
from idlemesh.demos import DEMOS, SIZES
from idlemesh.protocol import PROTOCOL_VERSION, parse_node, python_version

log = logging.getLogger("idlemesh.ui")

MESH_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")
NO_REPLY_AFTER_S = 2.5  # a ping unanswered this long marks the laptop "no reply"


NO_REMOTE_WORK = "No other laptop ran any work — check Laptops found and the log"


def _why_not_used(peer: dict) -> str:
    """Reason a laptop in Laptops found wasn't given to a run, from its status badge."""
    return {
        "checking": "still being checked when the run started",
        "unknown": "secret not set when the run started",
    }.get(peer["status"], peer["reason"])


def _per_laptop_rows(stats, labels: dict[str, str]) -> list[dict]:
    """Chunks done and measured seconds per chunk, per laptop, for the progress panel."""
    spc = getattr(stats, "seconds_per_chunk", {})
    keys = list(dict.fromkeys([*stats.keys(), *spc.keys()]))
    return [
        {
            "label": labels.get(k, k),
            "count": stats.get(k, 0),
            "seconds_per_chunk": round(spc[k], 3) if k in spc else None,
        }
        for k in keys
    ]


class UIError(Exception):
    """A request the UI refuses, with a message for the person (HTTP 400)."""


class Busy(UIError):
    """A demo is already running (HTTP 409)."""


class LogBuffer(logging.Handler):
    """The last N log lines from the idlemesh loggers, for the page's Log section."""

    def __init__(self, size: int = 50) -> None:
        super().__init__(logging.INFO)
        self.lines: collections.deque[dict] = collections.deque(maxlen=size)

    def emit(self, record: logging.LogRecord) -> None:
        when = time.strftime("%H:%M:%S", time.localtime(record.created))
        self.lines.append({"time": when, "level": record.levelname, "message": self.format(record)})


@dataclass
class ProbeResult:
    name: str | None = None
    node_id: str | None = None
    lendable: bool | None = None
    python: str | None = None
    protocol: int | None = None
    workers: int | None = None
    ram_total: int | None = None
    ram_available: int | None = None
    rtt_ms: float | None = None
    pinged_at: float | None = None  # monotonic time of the oldest unanswered ping
    replied_at: float | None = None


class UIState:
    def __init__(
        self,
        browser: Browser | None = None,
        agent_port: int = 7450,
        advertise: bool = True,
        probe_interval: float = 3.0,
        local_workers: int | None = None,  # worker processes on this laptop (default: all CPUs)
    ) -> None:
        self._lock = threading.RLock()
        self._secret: str | None = None  # memory only; never in snapshot(), logs or disk
        self.mesh = "default"
        self.agent_port = agent_port
        self.advertise = advertise
        self.probe_interval = probe_interval
        self.local_workers = local_workers
        self.cpus = os.cpu_count() or 1
        # lending
        self._agent = None
        self._agent_thread: threading.Thread | None = None
        self._lend_settings = {"workers": self.cpus, "force": False}
        self._lend_error: str | None = None
        # laptops found
        self._adverts: dict[str, AgentAdvert] = {}  # mDNS instance -> advert
        self._manual: list[str] = []  # "host:port" added by hand
        self._probes: dict[str, ProbeResult] = {}  # "host:port" -> last handshake
        # demo runs
        self._run: dict[str, Any] | None = None
        # log
        self.log_buffer = LogBuffer()
        self.log_buffer.setFormatter(logging.Formatter("%(message)s"))
        pkg_logger = logging.getLogger("idlemesh")
        pkg_logger.addHandler(self.log_buffer)
        if pkg_logger.level == logging.NOTSET or pkg_logger.level > logging.INFO:
            pkg_logger.setLevel(logging.INFO)
        # background work
        self._closed = threading.Event()
        self._browser = browser
        if self._browser is not None:
            self._browser.start(self._on_advert)
        self._prober = threading.Thread(
            target=self._probe_loop, name="idlemesh-ui-probe", daemon=True
        )
        self._prober.start()

    # -- setup ----------------------------------------------------------------------
    def setup(self, secret: str | None, mesh: str) -> None:
        if not MESH_NAME.match(mesh):
            raise UIError("Mesh name: letters, digits, '.', '_' or '-', up to 63 characters.")
        with self._lock:
            changing = (secret is not None and secret != self._secret) or mesh != self.mesh
            if changing and self._agent is not None:
                raise UIError("Switch lending off before changing the secret or mesh name.")
            if secret is not None:
                if not secret:
                    raise UIError("The secret can't be empty.")
                self._secret = secret
            if mesh != self.mesh:
                self.mesh = mesh
                self._probes.clear()
        log.info("Settings saved (mesh %r).", mesh)

    # -- lending --------------------------------------------------------------------
    def set_lending(self, on: bool, workers: int | None = None, force: bool = False) -> None:
        if not on:
            self._stop_agent()
            return
        from idlemesh.agent.server import Agent

        with self._lock:
            if self._agent is not None:
                return
            if not self._secret:
                raise UIError("Enter the shared secret first.")
            workers = max(1, min(workers or self.cpus, self.cpus))
            self._lend_settings = {"workers": workers, "force": force}
            self._lend_error = None
            agent = Agent(
                self.agent_port,
                self._secret,
                workers=workers,
                force=force,
                mesh=self.mesh,
                advertise=self.advertise,
            )
            thread = threading.Thread(
                target=self._serve, args=(agent,), name="idlemesh-ui-agent", daemon=True
            )
            self._agent, self._agent_thread = agent, thread
        thread.start()
        deadline = time.monotonic() + 60
        while not agent.started.wait(0.1):
            if not thread.is_alive() or time.monotonic() > deadline:
                self._stop_agent()
                raise UIError(self._lend_error or "Lending didn't start in time.")
        log.info("Lending this laptop: %d workers, port %d.", workers, self.agent_port)

    def _serve(self, agent) -> None:
        try:
            agent.serve_forever()
        except Exception as exc:  # e.g. port already in use
            msg = f"Could not start lending on port {self.agent_port}: {exc}"
            if "in use" in str(exc).lower():
                msg += " (is `idlemesh serve` already running?)"
            self._lend_error = msg
            log.error(msg)
        finally:
            with self._lock:
                if self._agent is agent:
                    self._agent = None

    def _stop_agent(self) -> None:
        with self._lock:
            agent, thread = self._agent, self._agent_thread
            self._agent = None
        if agent is not None:
            agent.stop()
            if thread is not None:
                thread.join(15)
            log.info("Stopped lending this laptop (%d chunks run for others).", agent.chunks_served)

    # -- laptops found ----------------------------------------------------------------
    def _on_advert(self, kind: str, instance: str, advert: AgentAdvert | None) -> None:
        with self._lock:
            if kind == "removed" or advert is None:
                self._adverts.pop(instance, None)
            else:
                self._adverts[instance] = advert

    def add_peer(self, address: str) -> str:
        try:
            host, port = parse_node(address)
        except ValueError as exc:
            raise UIError(str(exc)) from None
        addr = f"{host}:{port}"
        with self._lock:
            if addr not in self._manual:
                self._manual.append(addr)
        log.info("Added %s by address.", addr)
        return addr

    def remove_peer(self, address: str) -> None:
        with self._lock:
            if address in self._manual:
                self._manual.remove(address)
                self._probes.pop(address, None)

    def _targets(self) -> dict[str, AgentAdvert | None]:
        """Addresses to show and ping: discovered agents in our mesh, plus manual ones."""
        nets = local_networks()
        with self._lock:
            out: dict[str, AgentAdvert | None] = {}
            for adv in self._adverts.values():
                if adv.mesh == self.mesh:
                    out[f"{pick_address(adv.addresses, nets)}:{adv.port}"] = adv
            for addr in self._manual:
                out.setdefault(addr, None)
            return out

    def _probe_loop(self) -> None:
        """Every probe_interval, PING each laptop with the secret; record the PONGs."""
        import zmq

        from idlemesh.protocol import Codec, MsgType, dumps, loads

        ctx = zmq.Context.instance()
        socks: dict[str, zmq.Socket] = {}
        pending: dict[str, tuple[str, float]] = {}  # ping id -> (address, perf_counter at send)
        codec, codec_key, next_round = None, None, 0.0
        hello = dumps({"python": python_version(), "protocol": PROTOCOL_VERSION})

        def close_all():
            for s in socks.values():
                s.close()
            socks.clear()
            pending.clear()

        try:
            while not self._closed.is_set():
                secret = self._secret
                if secret != codec_key:  # secret changed: start over with fresh sockets
                    close_all()
                    codec, codec_key = (Codec(secret) if secret else None), secret
                targets = self._targets()
                for addr in [a for a in socks if a not in targets]:
                    socks.pop(addr).close()
                now = time.monotonic()
                if codec is not None and now >= next_round:
                    next_round = now + self.probe_interval
                    for addr in targets:
                        sock = socks.get(addr)
                        if sock is None:
                            sock = socks[addr] = ctx.socket(zmq.DEALER)
                            sock.setsockopt(zmq.LINGER, 0)
                            sock.connect(f"tcp://{addr}")
                        pid = "ui-" + uuid.uuid4().hex[:10]
                        pending[pid] = (addr, time.perf_counter())
                        sock.send_multipart(codec.encode(MsgType.PING, pid, hello))
                        with self._lock:
                            probe = self._probes.setdefault(addr, ProbeResult())
                            if probe.pinged_at is None:
                                probe.pinged_at = now
                if not socks:
                    self._closed.wait(0.2)
                    continue
                poller = zmq.Poller()
                for s in socks.values():
                    poller.register(s, zmq.POLLIN)
                for sock, _ in poller.poll(200):
                    try:
                        frames = sock.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        continue
                    msg = codec.decode(frames) if codec else None
                    if msg is None or msg.type is not MsgType.PONG or msg.chunk_id not in pending:
                        continue
                    addr, sent = pending.pop(msg.chunk_id)
                    info = loads(msg.payload)
                    with self._lock:
                        self._probes[addr] = ProbeResult(
                            name=str(info.get("name") or "") or None,
                            node_id=info.get("node_id"),
                            lendable=bool(info.get("lendable")),
                            python=info.get("python"),
                            protocol=int(info.get("protocol", 1)),
                            workers=info.get("workers"),
                            ram_total=info.get("ram_total"),
                            ram_available=info.get("ram_available"),
                            rtt_ms=(time.perf_counter() - sent)
                            * 1000,  # monotonic ticks ~15 ms on Windows
                            replied_at=time.monotonic(),
                        )
                cutoff = time.perf_counter() - 10
                for pid in [p for p, (_, t) in pending.items() if t < cutoff]:
                    del pending[pid]
        finally:
            close_all()

    def _peer_rows(self) -> list[dict]:
        own_id = node_id(create=False)
        own_ips = {str(n.ip) for n in local_networks()}
        ours = python_version()
        rows = []
        now = time.monotonic()
        with self._lock:
            secret_set = bool(self._secret)
            probes = dict(self._probes)
            manual = set(self._manual)
        for addr, adv in self._targets().items():
            p = probes.get(addr, ProbeResult())
            # pinged_at is the oldest unanswered ping; every signed PONG clears it.
            unanswered = p.pinged_at is not None and now - p.pinged_at > NO_REPLY_AFTER_S
            fresh = p.replied_at is not None and now - p.replied_at < self.probe_interval * 3
            python = p.python or (adv.python if adv else None)
            protocol = p.protocol if p.protocol is not None else (adv.protocol if adv else None)
            nid = p.node_id or (adv.node_id if adv else None)
            host = addr.rsplit(":", 1)[0]
            if (own_id and nid == own_id) or host in own_ips:
                status, reason = "self", "this laptop"
            elif protocol is not None and protocol != PROTOCOL_VERSION:
                status = "incompatible"
                reason = (
                    f"{'older' if protocol < PROTOCOL_VERSION else 'newer'} IdleMesh "
                    f"(protocol {protocol}, you have {PROTOCOL_VERSION})"
                )
            elif python and python != ours:
                status, reason = "incompatible", f"Python {python}, you have {ours}"
            elif not secret_set:
                status, reason = "unknown", "enter the secret to check"
            elif unanswered:
                status, reason = "noreply", "no reply: wrong secret or port blocked"
            elif fresh:
                status, reason = "compatible", "compatible"
            else:
                status, reason = "checking", "checking..."
            rows.append(
                {
                    "address": addr,
                    "name": p.name if secret_set else None,
                    "workers": p.workers or (adv.workers if adv else None),
                    "ram_free": p.ram_available if secret_set else None,
                    "ram_total": p.ram_total if secret_set else None,
                    "python": python,
                    "lendable": p.lendable if secret_set else None,
                    "rtt_ms": round(p.rtt_ms, 1) if p.rtt_ms is not None and secret_set else None,
                    "status": status,
                    "reason": reason,
                    "manual": addr in manual,
                    "discovered": adv is not None,
                }
            )
        return rows

    # -- demo runs --------------------------------------------------------------------
    def start_run(self, demo_name: str, size: str, mode: str) -> None:
        demo = DEMOS[demo_name]
        ok, why = demo.available()
        if not ok:
            raise UIError(f"{demo.title} isn't available: {why}.")
        nodes: list[str] = []
        labels = {"local": "this laptop"}
        not_used: list[str] = []
        seen_at_start: set[str] = set()
        if mode != "local":
            if not self._secret:
                raise UIError("Enter the shared secret first.")
            rows = self._peer_rows()
            peers = [p for p in rows if p["status"] == "compatible"]
            not_used = [
                f"{p['name'] or p['address']}: not used: {_why_not_used(p)}"
                for p in rows
                if p["status"] not in ("compatible", "self")
            ]
            seen_at_start = {p["address"] for p in rows}
            if not peers:
                raise UIError(
                    "No compatible laptops found yet. Check that the other laptop is lending "
                    "with the same secret and mesh, or add it by IP."
                )
            nodes = [p["address"] for p in peers]
            labels.update({p["address"]: p["name"] or p["address"] for p in peers})
        with self._lock:
            if self._run is not None and not self._run["finished"]:
                raise Busy("A demo is already running.")
            run = {
                "id": uuid.uuid4().hex[:8],
                "demo": demo_name,
                "title": demo.title,
                "size": size,
                "mode": mode,
                "phase": None,
                "done": 0,
                "total": 0,
                "per_node": [],
                "started": time.time(),
                "phase_started": time.time(),
                "results": {},
                "speedup": None,
                "match": None,
                "answer": None,
                "error": None,
                "backups_launched": 0,
                "backups_won": 0,
                "skipped": list(not_used),
                "no_remote_work": False,
                "finished": False,
            }
            self._run = run
        secret = self._secret
        thread = threading.Thread(
            target=self._execute,
            args=(run, demo, nodes, labels, secret, not_used, seen_at_start),
            name="idlemesh-ui-run",
            daemon=True,
        )
        thread.start()
        log.info("Running %s (%s, %s).", demo.title, size, mode)

    def _execute(self, run, demo, nodes, labels, secret, not_used, seen_at_start) -> None:
        from idlemesh.sdk import Mesh

        phases = {"local": ["local"], "mesh": ["mesh"], "compare": ["local", "mesh"]}[run["mode"]]
        outputs = {}
        try:
            inputs = demo.make_inputs(run["size"])
            for phase in phases:
                with self._lock:
                    run.update(phase=phase, done=0, total=0, per_node=[], phase_started=time.time())

                def progress(done, total, stats):
                    rows = _per_laptop_rows(stats, labels)
                    with self._lock:
                        run.update(
                            done=done,
                            total=total,
                            per_node=rows,
                            backups_launched=getattr(stats, "backups_launched", 0),
                            backups_won=getattr(stats, "backups_won", 0),
                            skipped=not_used
                            + [
                                f"{who}: {why}"
                                for who, why in getattr(stats, "skipped", {}).items()
                            ],
                        )

                w = self.local_workers
                ctx = pool_context()  # fork-safe: this process runs server/prober/agent threads
                if phase == "local":
                    mesh = Mesh(w, mp_context=ctx)
                else:
                    mesh = Mesh(w, nodes=nodes, secret=secret, mp_context=ctx)
                t0 = time.perf_counter()
                results = mesh.map(
                    demo.fn,
                    inputs,
                    chunk_size=demo.chunk_size,
                    on_progress=progress,
                    cost=demo.cost,
                )
                seconds = time.perf_counter() - t0
                results = demo.finish(results)  # e.g. ML: fold scores -> one per model
                outputs[phase] = results
                summary = demo.summarize(results)
                remote_chunks = sum(v for k, v in mesh.last_stats.items() if k != "local")
                with self._lock:
                    if phase == "mesh" and mesh.last_stats:
                        run["per_node"] = _per_laptop_rows(mesh.last_stats, labels)
                    run["results"][phase] = {
                        "seconds": round(seconds, 2),
                        "answer": summary["answer"],
                        "per_node": list(run["per_node"]),
                    }
                    run["answer"] = summary["answer"]
                    if phase == "mesh":
                        # Every laptop in Laptops found that ran nothing gets a reason.
                        late = [
                            f"{p['name'] or p['address']}: joined after the run started"
                            for p in self._peer_rows()
                            if p["address"] not in seen_at_start and p["status"] != "self"
                        ]
                        ran_nothing = [f"{w}: {why}" for w, why in mesh.last_stats.skipped.items()]
                        run["skipped"] = not_used + ran_nothing + late
                    if phase == "mesh" and remote_chunks == 0:
                        run["no_remote_work"] = True
                where = "on this laptop" if phase == "local" else "with found laptops"
                log.info("%s %s took %.1fs.", demo.title, where, seconds)
                if phase == "mesh" and remote_chunks == 0:
                    log.warning(NO_REMOTE_WORK)
            if len(outputs) == 2:
                local_s = run["results"]["local"]["seconds"]
                mesh_s = run["results"]["mesh"]["seconds"]
                with self._lock:
                    if not run["no_remote_work"]:
                        run["speedup"] = round(local_s / mesh_s, 2) if mesh_s > 0 else None
                    run["match"] = demo.same(outputs["local"], outputs["mesh"])
                if not run["match"]:
                    log.warning("Results from the mesh run differ from the local run.")
        except Exception as exc:
            first_line = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
            with self._lock:
                run["error"] = f"{type(exc).__name__}: {first_line}"
            log.error("Demo failed: %s: %s", type(exc).__name__, first_line)
        finally:
            with self._lock:
                run["finished"] = True
                run["ended"] = time.time()

    # -- what the page sees ---------------------------------------------------------------
    def snapshot(self) -> dict:
        """Full state for the page. Never contains the secret."""
        peers = self._peer_rows()
        with self._lock:
            agent = self._agent
            run = dict(self._run) if self._run else None
            if run:
                run["results"] = dict(run["results"])
            return {
                "setup": {"secret_set": bool(self._secret), "mesh": self.mesh},
                "machine": {
                    "cpus": self.cpus,
                    "ips": [str(n.ip) for n in local_networks()],
                    "platform": sys.platform,
                    "python": python_version(),
                },
                "lend": {
                    "on": agent is not None,
                    "port": self.agent_port,
                    "workers": self._lend_settings["workers"],
                    "force": self._lend_settings["force"],
                    "chunks_served": agent.chunks_served if agent is not None else 0,
                    "error": self._lend_error,
                },
                "peers": peers,
                "demos": [
                    {
                        "name": d.name,
                        "title": d.title,
                        "description": d.description,
                        "available": d.available()[0],
                        "reason": d.available()[1],
                    }
                    for d in DEMOS.values()
                ],
                "sizes": list(SIZES),
                "run": run,
                "log": list(self.log_buffer.lines),
            }

    def close(self) -> None:
        self._closed.set()
        self._stop_agent()
        if self._browser is not None:
            self._browser.stop()
        self._prober.join(5)
        logging.getLogger("idlemesh").removeHandler(self.log_buffer)
