"""Phase 2: discovery, version checks, address parsing, and what is broadcast."""

import hashlib
import ipaddress
import logging
import socket
import struct
import sys
import threading
import time
import uuid

import cloudpickle
import pytest

zmq = pytest.importorskip("zmq")
pytest.importorskip("zeroconf")

from idlemesh import Mesh  # noqa: E402
from idlemesh.agent.discovery import (  # noqa: E402
    TXT_KEYS,
    Advertiser,
    AgentAdvert,
    ZeroconfBrowser,
    build_service_info,
    make_advert,
    pick_address,
)
from idlemesh.cli import list_agents  # noqa: E402
from idlemesh.protocol import (  # noqa: E402
    PROTOCOL_VERSION,
    Codec,
    MsgType,
    dumps,
    loads,
    parse_node,
    python_version,
)
from idlemesh.sdk.discover import Discovery  # noqa: E402

cloudpickle.register_pickle_by_value(sys.modules[__name__])

SECRET = "test-secret"
LAN = [ipaddress.IPv4Interface("192.168.1.10/24")]
OTHER_PY = "2.7"


def slow_square(x):
    time.sleep(0.1)
    return x * x


class FakeBrowser:
    """Stands in for zeroconf: the test decides when agents appear and disappear."""

    def __init__(self, *adverts):
        self.initial = list(adverts)
        self.on_event = None
        self.stopped = False

    def start(self, on_event):
        self.on_event = on_event
        for a in self.initial:
            on_event("added", a.instance, a)

    def stop(self):
        self.stopped = True

    def add(self, a):
        self.on_event("added", a.instance, a)

    def remove(self, a):
        self.on_event("removed", a.instance, None)


def advert(port=7450, node="aaaaaaaaaaaa", mesh="default", py=None, proto=PROTOCOL_VERSION,
           addresses=("192.168.1.20",)):  # fmt: skip
    return AgentAdvert(
        instance=f"idlemesh-{node}-{port}",
        node_id=node,
        mesh=mesh,
        python=py or python_version(),
        protocol=proto,
        version="0.1.0.dev0",
        workers=2,
        addresses=tuple(addresses),
        port=port,
    )


def discovery(*adverts, own_id="ffffffffffff"):
    d = Discovery(FakeBrowser(*adverts), "default", local_nets=LAN, own_id=own_id)
    d.ensure_started(0)
    return d


# -- address parsing ---------------------------------------------------------------


@pytest.mark.parametrize(
    "addr, expected",
    [
        ("192.168.1.20", ("192.168.1.20", 7450)),
        ("192.168.1.20:9000", ("192.168.1.20", 9000)),
        ("aahav-laptop.local:7451", ("aahav-laptop.local", 7451)),
        ("localhost", ("localhost", 7450)),
    ],
)
def test_parse_node_valid(addr, expected):
    assert parse_node(addr) == expected


@pytest.mark.parametrize(
    "addr",
    ["", "  ", "host:", "host:abc", "host:0", "host:70000", "http://1.2.3.4", "a b", "::1",
     "[::1]:7450", "999.1.1.1", "-bad", "1.2.3.4:7450:1"],
)  # fmt: skip
def test_parse_node_malformed_raises_value_error(addr):
    with pytest.raises(ValueError):
        parse_node(addr)


def test_mesh_rejects_malformed_node_before_any_networking():
    with pytest.raises(ValueError, match="invalid port"):
        Mesh(nodes=["192.168.1.20:abc"], secret=SECRET)
    assert Mesh(nodes=["192.168.1.20"], secret=SECRET).nodes == ["192.168.1.20:7450"]


# -- discovery filtering (no sockets) ------------------------------------------------


def test_compatible_agent_in_mesh_becomes_candidate():
    d = discovery(advert(addresses=("172.20.0.1", "192.168.1.20")))
    [c] = d.candidates()
    assert c.addr == "192.168.1.20:7450"  # same-subnet address preferred over vEthernet/VPN


def test_wrong_mesh_is_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        d = discovery(advert(mesh="other-group"))
    assert d.candidates() == []
    assert d.other_meshes == {"other-group"}
    assert '"other-group"' in caplog.text  # hint mentions the mesh we did find


def test_python_mismatch_skipped_with_one_clear_warning(caplog):
    a = advert(py=OTHER_PY)
    with caplog.at_level(logging.WARNING):
        d = discovery(a)
        d.on_event("updated", a.instance, a)  # repeated adverts don't repeat the warning
    assert d.candidates() == []
    expected = (
        f"Skipping agent aaaaaaaaaaaa (192.168.1.20:7450): it runs Python {OTHER_PY}, "
        f"you run {python_version()}. Use the same Python version on both laptops."
    )
    assert caplog.text.count(expected) == 1


def test_protocol_mismatch_skipped():
    assert discovery(advert(proto=PROTOCOL_VERSION + 1)).candidates() == []


def test_own_agent_is_never_discovered():
    by_id = advert(node="ffffffffffff", addresses=("192.168.1.99",))
    by_ip = advert(node="bbbbbbbbbbbb", addresses=("192.168.1.10",))  # one of our IPs
    assert discovery(by_id, by_ip).candidates() == []


def test_nothing_found_logs_hint(caplog):
    with caplog.at_level(logging.WARNING):
        discovery()
    assert 'No IdleMesh agents found in mesh "default"' in caplog.text
    assert "phone hotspot" in caplog.text


def test_removed_agent_reappearing_gets_new_generation():
    a = advert()
    d = discovery(a)
    [first] = d.candidates()
    d.on_event("removed", a.instance, None)
    assert d.candidates() == []
    d.browser.add(a)
    [again] = d.candidates()
    assert again.gen > first.gen


def test_pick_address_falls_back_to_first():
    assert pick_address(("10.9.9.9", "10.8.8.8"), LAN) == "10.9.9.9"


# -- discovery end to end (real agent, fake browser) -----------------------------------


def _local_advert(addr, node="cccccccccccc"):
    host, port = addr.split(":")
    return advert(port=int(port), node=node, addresses=(host,))


def test_agent_found_before_start_receives_chunks(agent):
    addr, _ = agent(secret=SECRET)
    browser = FakeBrowser(_local_advert(addr))
    with Mesh(workers=1, discover=True, browser=browser, discover_timeout=0.1, secret=SECRET) as m:
        assert m.map(slow_square, range(20), chunk_size=1) == [x * x for x in range(20)]
        assert m.last_stats.get(addr, 0) > 0
    assert browser.stopped


def test_explicit_and_discovered_same_agent_is_used_once(agent, caplog):
    addr, _ = agent(secret=SECRET)
    browser = FakeBrowser(_local_advert(addr))
    mesh = Mesh(workers=1, nodes=[addr], discover=True, browser=browser, discover_timeout=0.1,
                secret=SECRET)  # fmt: skip
    with caplog.at_level(logging.INFO), mesh:
        assert mesh.map(slow_square, range(10), chunk_size=1) == [x * x for x in range(10)]
    assert "discovered agent" not in caplog.text


def test_agent_appearing_mid_job_joins(agent):
    addr, _ = agent(secret=SECRET)
    browser = FakeBrowser()
    mesh = Mesh(workers=1, discover=True, browser=browser, discover_timeout=0.1, secret=SECRET)
    timer = threading.Timer(0.8, browser.add, args=(_local_advert(addr),))
    timer.start()
    try:
        with mesh:
            out = mesh.map(slow_square, range(40), chunk_size=1)
    finally:
        timer.cancel()
    assert out == [x * x for x in range(40)]
    assert mesh.last_stats.get(addr, 0) > 0, "agent that appeared mid-job got no work"


def test_agent_disappearing_mid_job_still_completes(agent, kill):
    addr, proc = agent(secret=SECRET)
    a = _local_advert(addr)
    browser = FakeBrowser(a)
    mesh = Mesh(workers=1, discover=True, browser=browser, discover_timeout=0.1, secret=SECRET,
                node_timeout=1.0, ping_interval=0.2)  # fmt: skip

    def vanish():
        kill(proc)
        browser.remove(a)

    timer = threading.Timer(1.0, vanish)
    timer.start()
    try:
        with mesh:
            out = mesh.map(slow_square, range(40), chunk_size=1)
    finally:
        timer.cancel()
    assert out == [x * x for x in range(40)]
    assert proc.poll() is not None


def test_discover_with_nothing_found_runs_locally(caplog):
    with caplog.at_level(logging.WARNING):
        with Mesh(workers=2, discover=True, browser=FakeBrowser(), discover_timeout=0,
                  secret=SECRET) as m:  # fmt: skip
            assert m.map(slow_square, range(6), chunk_size=2) == [x * x for x in range(6)]
    assert m.last_stats == {"local": 3}
    assert "No IdleMesh agents found" in caplog.text


# -- handshake version check with an explicit node --------------------------------------


class FakeAgent:
    """A ROUTER socket on a thread that answers PING with a canned PONG."""

    def __init__(self, info):
        self.info = info
        self.sock = zmq.Context.instance().socket(zmq.ROUTER)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.port = self.sock.bind_to_random_port("tcp://127.0.0.1")
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def _serve(self):
        codec = Codec(SECRET)
        while not self.stop.is_set():
            if not self.sock.poll(50):
                continue
            ident, *frames = self.sock.recv_multipart()
            msg = codec.decode(frames)
            if msg is not None and msg.type is MsgType.PING:
                reply = codec.encode(MsgType.PONG, msg.chunk_id, dumps(self.info))
                self.sock.send_multipart([ident, *reply])
        self.sock.close()

    def __enter__(self):
        self.thread.start()
        return f"127.0.0.1:{self.port}"

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(5)


@pytest.mark.parametrize(
    "info, expected",
    [
        (
            {"workers": 1, "lendable": True, "python": OTHER_PY, "protocol": PROTOCOL_VERSION,
             "name": "old-laptop"},
            "Skipping old-laptop (127.0.0.1:{port}): it runs Python " + OTHER_PY
            + ", you run {py}. Use the same Python version on both laptops.",
        ),
        (
            {"workers": 1, "lendable": True},  # what a Phase 1 agent sends
            "Skipping 127.0.0.1:{port}: it runs protocol 1, you run protocol {proto}.",
        ),
    ],
)  # fmt: skip
def test_handshake_version_mismatch_explicit_node(info, expected, caplog):
    with FakeAgent(info) as addr, caplog.at_level(logging.WARNING):
        mesh = Mesh(workers=2, nodes=[addr], secret=SECRET)
        assert mesh.map(slow_square, range(6), chunk_size=2) == [x * x for x in range(6)]
    port = addr.split(":")[1]
    assert expected.format(port=port, py=python_version(), proto=PROTOCOL_VERSION) in caplog.text
    assert mesh.last_stats == {"local": 3}


# -- privacy: what goes on the air ---------------------------------------------------------


def test_txt_record_has_no_hostname_name_lendable_or_secret():
    from idlemesh.agent.server import Agent

    secret = "super-secret-value-123"
    agent = Agent(7450, secret, name="vaamans-gaming-laptop", force=True)
    info = build_service_info(agent.advert())
    txt = {k.decode(): (v or b"").decode() for k, v in info.properties.items()}
    assert set(txt) == set(TXT_KEYS) == {"id", "mesh", "py", "proto", "ver", "workers"}
    on_air = " ".join([info.name, info.server, *txt, *txt.values()]).lower()
    forbidden = [
        socket.gethostname().lower(),
        "vaamans-gaming-laptop",
        "lendable",
        secret,
        hashlib.sha256(secret.encode()).hexdigest()[:12],
    ]
    for word in forbidden:
        assert word not in on_air, f"{word!r} is broadcast over mDNS"


def test_nodes_listing_without_secret_marks_name_and_lendable_unknown(agent):
    addr, _ = agent(secret=SECRET)
    a = _local_advert(addr)
    out = list_agents(FakeBrowser(a), "default", 0, secret=None)
    [row] = out["rows"]
    assert (row["id"], row["python"], row["compatible"]) == (a.node_id, python_version(), "yes")
    assert row["name"] == "?" and row["lendable"] == "?"
    out = list_agents(FakeBrowser(a), "default", 0, secret=SECRET)
    [row] = out["rows"]
    assert row["name"] == f"test-agent-{addr.split(':')[1]}" and row["lendable"] == "yes"


# -- one real zeroconf round trip ------------------------------------------------------------


def _multicast_works():
    group = "239.255.77.77"
    recv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        recv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        recv.bind(("", 0))
        port = recv.getsockname()[1]
        mreq = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0"))
        recv.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        recv.settimeout(1.0)
        send.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        send.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        send.sendto(b"idlemesh-probe", (group, port))
        return recv.recvfrom(64)[0] == b"idlemesh-probe"
    except OSError:
        return False
    finally:
        recv.close()
        send.close()


def test_real_zeroconf_round_trip():
    if not _multicast_works():
        pytest.skip("multicast is not available on this machine/runner")
    mesh = f"test-{uuid.uuid4().hex[:8]}"
    adv = make_advert(port=45678, mesh=mesh, workers=3)
    found = threading.Event()
    seen = {}

    def on_event(kind, instance, a):
        if a is not None and a.mesh == mesh:
            seen["advert"] = a
            found.set()

    advertiser, browser = Advertiser(adv), ZeroconfBrowser()
    advertiser.start()
    try:
        browser.start(on_event)
        assert found.wait(15), "advertised agent was not discovered"
    finally:
        browser.stop()
        advertiser.stop()
    got = seen["advert"]
    assert (got.node_id, got.port, got.workers) == (adv.node_id, 45678, 3)
    assert (got.python, got.protocol) == (python_version(), PROTOCOL_VERSION)
    assert loads(dumps(got)) == got
