"""Zero-config LAN discovery over mDNS (requires `pip install idlemesh[net]`).

Each agent advertises `_idlemesh._tcp.local.`; the coordinator browses for it, so
nobody types an IP address.

Privacy: mDNS is broadcast to everyone on the network, not just your mesh. The
advertisement therefore carries only an opaque random node ID (generated once per
machine), the mesh name, versions and worker count. The node's real name and
whether it is lendable right now are only sent in the signed PONG, to peers that
know the mesh secret.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import secrets
import socket
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

import psutil

from idlemesh.protocol import PROTOCOL_VERSION, python_version

log = logging.getLogger(__name__)

SERVICE_TYPE = "_idlemesh._tcp.local."
TXT_KEYS = ("id", "mesh", "py", "proto", "ver", "workers")  # nothing else is ever advertised
_NODE_ID = re.compile(r"^[0-9a-f]{12}$")

EventKind = Literal["added", "updated", "removed"]
OnEvent = Callable[[EventKind, str, "AgentAdvert | None"], None]


# -- identity -------------------------------------------------------------------
def idlemesh_home() -> Path:
    return Path(os.environ.get("IDLEMESH_HOME") or Path.home() / ".idlemesh")


def node_id(create: bool = True) -> str | None:
    """This machine's opaque node ID, stored in ~/.idlemesh/node_id.

    Random, so it reveals nothing about the machine or its owner. Delete the file
    to get a new one. With create=False, returns None if none exists yet.
    """
    path = idlemesh_home() / "node_id"
    try:
        existing = path.read_text().strip()
        if _NODE_ID.match(existing):
            return existing
    except OSError:
        pass
    if not create:
        return None
    new = secrets.token_hex(6)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(new + "\n")
    return new


# -- addresses --------------------------------------------------------------------
def local_networks() -> list[ipaddress.IPv4Interface]:
    """Non-loopback, non-link-local IPv4 interfaces of this machine."""
    nets = []
    for addrs in psutil.net_if_addrs().values():
        for a in addrs:
            if a.family != socket.AF_INET or not a.netmask:
                continue
            iface = ipaddress.IPv4Interface(f"{a.address}/{a.netmask}")
            if not (iface.ip.is_loopback or iface.ip.is_link_local):
                nets.append(iface)
    return nets


def pick_address(addresses: tuple[str, ...], local: list[ipaddress.IPv4Interface]) -> str:
    """Prefer an address on the same subnet as one of ours (skips VPN/WSL/Docker adapters)."""
    for addr in addresses:
        ip = ipaddress.IPv4Address(addr)
        if any(ip in n.network for n in local):
            return addr
    return addresses[0]


# -- advertisement ------------------------------------------------------------------
@dataclass(frozen=True)
class AgentAdvert:
    instance: str  # mDNS instance name, e.g. "idlemesh-3f9a0c1b2d4e-7450"
    node_id: str
    mesh: str
    python: str
    protocol: int
    version: str
    workers: int
    addresses: tuple[str, ...]
    port: int

    def txt(self) -> dict[str, str]:
        return {
            "id": self.node_id,
            "mesh": self.mesh,
            "py": self.python,
            "proto": str(self.protocol),
            "ver": self.version,
            "workers": str(self.workers),
        }

    @classmethod
    def from_txt(
        cls, instance: str, props: dict, addresses: tuple[str, ...], port: int
    ) -> AgentAdvert | None:
        def get(key: str) -> str:
            raw = props.get(key.encode(), props.get(key))
            return raw.decode() if isinstance(raw, bytes) else str(raw or "")

        try:
            return cls(
                instance=instance,
                node_id=get("id"),
                mesh=get("mesh"),
                python=get("py"),
                protocol=int(get("proto")),
                version=get("ver"),
                workers=int(get("workers")),
                addresses=addresses,
                port=port,
            )
        except ValueError:
            log.debug("ignoring malformed advertisement %s", instance)
            return None


def make_advert(
    port: int, mesh: str, workers: int, addresses: list[str] | None = None
) -> AgentAdvert:
    from idlemesh import __version__  # imported lazily: idlemesh/__init__ imports the SDK

    nid = node_id(create=True)
    assert nid is not None
    if addresses is None:
        addresses = [str(n.ip) for n in local_networks()] or ["127.0.0.1"]
    return AgentAdvert(
        instance=f"idlemesh-{nid}-{port}",
        node_id=nid,
        mesh=mesh,
        python=python_version(),
        protocol=PROTOCOL_VERSION,
        version=__version__,
        workers=workers,
        addresses=tuple(addresses),
        port=port,
    )


def build_service_info(advert: AgentAdvert):
    from zeroconf import ServiceInfo

    return ServiceInfo(
        SERVICE_TYPE,
        f"{advert.instance}.{SERVICE_TYPE}",
        addresses=[socket.inet_aton(a) for a in advert.addresses],
        port=advert.port,
        properties=advert.txt(),
        server=f"idlemesh-{advert.node_id}.local.",  # never the machine's hostname
        host_ttl=120,
        other_ttl=120,  # zeroconf's default (75 min) keeps crashed agents visible far too long
    )


class Advertiser:
    """Registers an agent on mDNS; `stop()` unregisters it (sends goodbye packets)."""

    def __init__(self, advert: AgentAdvert) -> None:
        self.advert = advert
        self._zc = None
        self._info = None

    def start(self) -> None:
        from zeroconf import Zeroconf

        self._zc = Zeroconf()
        self._info = build_service_info(self.advert)
        self._zc.register_service(self._info, allow_name_change=True)

    def stop(self) -> None:
        if self._zc is None:
            return
        try:
            self._zc.unregister_service(self._info)
        finally:
            self._zc.close()
            self._zc = None


# -- browsing -----------------------------------------------------------------------
class Browser(Protocol):
    """Source of agent advertisements. `on_event` may be called from another thread."""

    def start(self, on_event: OnEvent) -> None: ...

    def stop(self) -> None: ...


class ZeroconfBrowser:
    def __init__(self) -> None:
        self._zc = None
        self._browser = None

    def start(self, on_event: OnEvent) -> None:
        from zeroconf import IPVersion, ServiceBrowser, ServiceStateChange, Zeroconf

        self._zc = Zeroconf()

        def handler(zeroconf, service_type, name, state_change):
            instance = name.removesuffix("." + service_type)
            if state_change is ServiceStateChange.Removed:
                on_event("removed", instance, None)
                return
            info = zeroconf.get_service_info(service_type, name)
            if info is None:
                return
            addresses = tuple(info.parsed_addresses(IPVersion.V4Only))
            if not addresses:
                return
            advert = AgentAdvert.from_txt(instance, info.properties, addresses, info.port)
            if advert is not None:
                kind = "added" if state_change is ServiceStateChange.Added else "updated"
                on_event(kind, instance, advert)

        self._browser = ServiceBrowser(self._zc, SERVICE_TYPE, handlers=[handler])

    def stop(self) -> None:
        if self._browser is not None:
            self._browser.cancel()
            self._browser = None
        if self._zc is not None:
            self._zc.close()
            self._zc = None
