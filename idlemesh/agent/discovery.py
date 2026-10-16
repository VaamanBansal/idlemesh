"""Zero-config LAN discovery over mDNS (requires `pip install idlemesh[net]`).

Each agent advertises `_idlemesh._tcp.local.`; the coordinator browses for it, so
nobody ever types an IP address.
"""

from __future__ import annotations

import socket
from collections.abc import Callable

SERVICE_TYPE = "_idlemesh._tcp.local."


def _local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def advertise(node_id: str, port: int, properties: dict[str, str] | None = None):
    from zeroconf import ServiceInfo, Zeroconf

    zc = Zeroconf()
    info = ServiceInfo(
        SERVICE_TYPE,
        f"{node_id}.{SERVICE_TYPE}",
        addresses=[socket.inet_aton(_local_ip())],
        port=port,
        properties=properties or {},
    )
    zc.register_service(info)
    return zc, info


def browse(on_found: Callable[[str, str, int], None]):
    from zeroconf import ServiceBrowser, ServiceStateChange, Zeroconf

    zc = Zeroconf()

    def handler(zeroconf, service_type, name, state_change):
        if state_change is ServiceStateChange.Added:
            info = zeroconf.get_service_info(service_type, name)
            if info and info.addresses:
                on_found(name.split(".")[0], socket.inet_ntoa(info.addresses[0]), info.port)

    return zc, ServiceBrowser(zc, SERVICE_TYPE, handlers=[handler])
