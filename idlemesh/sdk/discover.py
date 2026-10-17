"""Turn a stream of mDNS advertisements into agents this client may use.

No sockets here: a `Browser` (real zeroconf, or a fake in tests) calls
`on_event`, possibly from its own thread, and the coordinator's event loop reads
`candidates()`. Filtering rules:
  - other meshes are ignored (two friend groups can share one Wi-Fi),
  - this laptop's own agent is ignored (same node ID, or one of our own IPs),
  - agents on another Python major.minor or protocol version are skipped, with
    one clear warning per agent.
"""

from __future__ import annotations

import ipaddress
import logging
import threading
import time
from dataclasses import dataclass

from idlemesh.agent.discovery import (
    AgentAdvert,
    Browser,
    EventKind,
    local_networks,
    node_id,
    pick_address,
)
from idlemesh.protocol import PROTOCOL_VERSION, python_version

log = logging.getLogger(__name__)


@dataclass
class Candidate:
    advert: AgentAdvert
    host: str
    port: int
    gen: int  # bumps whenever the agent (re)appears, so a failed agent can be retried

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


def no_agents_hint(mesh: str, other_meshes: set[str]) -> str:
    hint = (
        f'No IdleMesh agents found in mesh "{mesh}". Campus and office Wi-Fi often block '
        'discovery; try a phone hotspot, or pass nodes=["IP:7450"] explicitly.'
    )
    if other_meshes:
        names = ", ".join(sorted(f'"{m}"' for m in other_meshes))
        hint += f" (Found agents in other meshes: {names}; pass mesh=... to use them.)"
    return hint


def python_mismatch(label: str, theirs: str) -> str:
    return (
        f"Skipping {label}: it runs Python {theirs}, you run {python_version()}. "
        "Use the same Python version on both laptops."
    )


def protocol_mismatch(label: str, theirs: int, version: str = "") -> str:
    their_desc = f"IdleMesh {version} (protocol {theirs})" if version else f"protocol {theirs}"
    return (
        f"Skipping {label}: it runs {their_desc}, you run protocol {PROTOCOL_VERSION}. "
        "Install the same IdleMesh version on both laptops."
    )


class Discovery:
    def __init__(
        self,
        browser: Browser,
        mesh: str,
        local_nets: list[ipaddress.IPv4Interface] | None = None,
        own_id: str | None = None,
    ) -> None:
        self.browser = browser
        self.mesh = mesh
        self._local = local_networks() if local_nets is None else local_nets
        self._own_ips = {str(n.ip) for n in self._local}
        self._own_id = own_id if own_id is not None else node_id(create=False)
        self._lock = threading.Lock()
        self._by_instance: dict[str, Candidate] = {}
        self._gen = 0
        self._warned: set[str] = set()
        self._seen_in_mesh: set[str] = set()  # node IDs of other laptops in our mesh
        self.other_meshes: set[str] = set()
        self._started = False

    # -- lifecycle -----------------------------------------------------------------
    def ensure_started(self, wait_s: float) -> None:
        """Start browsing; the first time, wait `wait_s` and log a hint if nothing turned up."""
        if self._started:
            return
        self._started = True
        self.browser.start(self.on_event)
        time.sleep(wait_s)
        with self._lock:
            found_nothing = not self._seen_in_mesh
            others = set(self.other_meshes)
        if found_nothing:
            log.warning(no_agents_hint(self.mesh, others))

    def stop(self) -> None:
        if self._started:
            self.browser.stop()
            self._started = False

    # -- events (may arrive on the browser's thread) -------------------------------
    def on_event(self, kind: EventKind, instance: str, advert: AgentAdvert | None) -> None:
        with self._lock:
            if kind == "removed" or advert is None:
                self._by_instance.pop(instance, None)
                return
            if not self._accept(advert):
                self._by_instance.pop(instance, None)
                return
            host = pick_address(advert.addresses, self._local)
            prev = self._by_instance.get(instance)
            if prev is not None and (prev.host, prev.port) == (host, advert.port):
                prev.advert = advert
                return
            self._gen += 1
            self._by_instance[instance] = Candidate(advert, host, advert.port, self._gen)

    def _accept(self, advert: AgentAdvert) -> bool:
        if advert.mesh != self.mesh:
            self.other_meshes.add(advert.mesh)
            return False
        if advert.node_id == self._own_id or self._own_ips.intersection(advert.addresses):
            log.debug("ignoring this laptop's own agent %s", advert.instance)
            return False
        self._seen_in_mesh.add(advert.node_id)
        label = f"agent {advert.node_id} ({advert.addresses[0]}:{advert.port})"
        if advert.protocol != PROTOCOL_VERSION:
            self._warn_once(
                advert.node_id, protocol_mismatch(label, advert.protocol, advert.version)
            )
            return False
        if advert.python != python_version():
            self._warn_once(advert.node_id, python_mismatch(label, advert.python))
            return False
        return True

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            log.warning(message)

    # -- read side ------------------------------------------------------------------
    def candidates(self) -> list[Candidate]:
        with self._lock:
            return list(self._by_instance.values())
