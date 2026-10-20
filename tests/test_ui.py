"""`idlemesh ui`: API, security, demos and the progress hook end to end."""

import importlib.util
import logging
import socket
import sys
import threading
import time

import pytest

pytest.importorskip("fastapi")
zmq = pytest.importorskip("zmq")

from fastapi.testclient import TestClient  # noqa: E402

from idlemesh.demos import DEMOS, SIZES  # noqa: E402
from idlemesh.protocol import Codec, MsgType, loads  # noqa: E402
from idlemesh.ui.server import HOST, create_app, load_token, make_server  # noqa: E402
from idlemesh.ui.state import UIState  # noqa: E402

TOKEN = "test-token-0123456789abcdef"
SECRET = "ui-secret-Zq81-do-not-leak"  # distinctive, so a leak can't hide


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def ui():
    state = UIState(browser=None, agent_port=_free_port(), advertise=False, probe_interval=0.2)
    client = TestClient(create_app(state, TOKEN), base_url="http://127.0.0.1")
    client.headers["X-IdleMesh-Token"] = TOKEN
    yield state, client
    state.close()


def wait_for(client, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = client.get("/api/state").json()
        if predicate(s):
            return s
        time.sleep(0.1)
    raise AssertionError(f"timed out; last state: {s}")


def finished(s):
    return s["run"] is not None and s["run"]["finished"]


# -- security -------------------------------------------------------------------------


def test_every_request_needs_the_token(ui):
    state, _ = ui
    anon = TestClient(create_app(state, TOKEN), base_url="http://127.0.0.1")
    assert anon.get("/api/state").status_code == 403
    assert anon.get("/").status_code == 403
    assert anon.post("/api/lend", json={"on": True}).status_code == 403
    run = {"demo": "option", "size": "small", "mode": "local"}
    assert anon.post("/api/run", json=run).status_code == 403
    assert anon.get("/api/state", headers={"X-IdleMesh-Token": "wrong"}).status_code == 403
    assert anon.get(f"/?token={TOKEN}").status_code == 200
    page = anon.get(f"/?token={TOKEN}")
    assert page.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]


def test_foreign_host_header_is_rejected(ui):
    state, _ = ui
    rebinding = TestClient(create_app(state, TOKEN), base_url="http://attacker.example")
    assert rebinding.get("/api/state", headers={"X-IdleMesh-Token": TOKEN}).status_code == 400


def test_server_binds_only_to_localhost(ui):
    state, _ = ui
    server = make_server(state, TOKEN, _free_port())
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 15
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert server.started
        bound = {sock.getsockname()[0] for srv in server.servers for sock in srv.sockets}
        assert HOST == "127.0.0.1"
        assert bound == {"127.0.0.1"}
    finally:
        server.should_exit = True
        thread.join(10)


def test_cli_has_no_host_option(monkeypatch, capsys):
    from idlemesh import cli

    monkeypatch.setattr(sys, "argv", ["idlemesh", "ui", "--host", "0.0.0.0"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2
    assert "unrecognized arguments: --host" in capsys.readouterr().err


@pytest.mark.parametrize(
    "body",
    [
        {"demo": "os.system", "size": "small", "mode": "local"},
        {"demo": "../../etc/passwd", "size": "small", "mode": "local"},
        {"demo": "option; rm -rf ~", "size": "small", "mode": "local"},
        {"demo": "option", "size": "huge", "mode": "local"},
        {"demo": "option", "size": "small", "mode": "shell"},
        {"demo": "option", "size": "small", "mode": "local", "code": "print(1)"},
        {"demo": "option", "size": "small", "mode": "local", "fn": "lambda x: x"},
        {"demo": "option"},
        {},
    ],
)
def test_run_accepts_only_builtin_demos(ui, body):
    _, client = ui
    r = client.post("/api/run", json=body)
    assert r.status_code == 422
    assert client.get("/api/state").json()["run"] is None


def test_secret_never_appears_in_responses_or_logs(ui, caplog):
    state, client = ui
    seen = []

    def record(r):
        seen.append(r.text)
        return r

    with caplog.at_level(logging.DEBUG):
        record(client.post("/api/setup", json={"secret": SECRET, "mesh": "default"}))
        record(client.post("/api/setup", json={"secret": SECRET + "x" * 2000, "mesh": "default"}))
        record(client.post("/api/setup", json={"secret": SECRET, "mesh": "bad mesh!"}))
        record(client.post("/api/setup", json={"secret": SECRET, "mesh": "default", "x": 1}))
        record(client.post("/api/lend", json={"on": True, "workers": 1, "force": True}))
        record(client.post("/api/setup", json={"secret": SECRET + "2", "mesh": "default"}))
        record(client.post("/api/peers", json={"address": "not an address"}))
        record(client.post("/api/run", json={"demo": "option", "size": "small", "mode": "local"}))
        final = wait_for(client, finished)
        seen.append(str(final))
        record(client.post("/api/lend", json={"on": False}))
        seen.append(client.get("/api/state").text)
    assert final["setup"]["secret_set"] is True
    everything = "\n".join(
        [*seen, caplog.text, *(line["message"] for line in state.log_buffer.lines)]
    )
    assert SECRET not in everything
    assert "ui-secret" not in everything


# -- lending ---------------------------------------------------------------------------


def test_lending_needs_secret_then_starts_and_stops(ui):
    state, client = ui
    r = client.post("/api/lend", json={"on": True})
    assert r.status_code == 400 and "secret" in r.json()["detail"]
    client.post("/api/setup", json={"secret": SECRET, "mesh": "default"})
    assert (
        client.post("/api/lend", json={"on": True, "workers": 1, "force": True}).status_code == 204
    )
    lend = client.get("/api/state").json()["lend"]
    assert lend["on"] and lend["workers"] == 1 and lend["force"]

    # The in-process agent answers a signed PING on its port.
    codec = Codec(SECRET)
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://127.0.0.1:{state.agent_port}")
    try:
        sock.send_multipart(codec.encode(MsgType.PING, "t"))
        assert sock.poll(10_000)
        msg = codec.decode(sock.recv_multipart())
        assert msg.type is MsgType.PONG and loads(msg.payload)["workers"] == 1
    finally:
        sock.close()

    r = client.post("/api/setup", json={"secret": "other", "mesh": "default"})
    assert r.status_code == 400 and "lending off" in r.json()["detail"]
    assert client.post("/api/lend", json={"on": False}).status_code == 204
    assert client.get("/api/state").json()["lend"]["on"] is False
    with socket.socket() as s:  # the port is free again, so lending can be switched back on
        s.bind(("127.0.0.1", state.agent_port))


# -- demo runs --------------------------------------------------------------------------


def test_local_demo_reaches_100_percent_and_reports_results(ui):
    _, client = ui
    run = {"demo": "option", "size": "small", "mode": "local"}
    assert client.post("/api/run", json=run).status_code == 202
    r = wait_for(client, finished)["run"]
    assert r["error"] is None
    assert r["total"] > 0 and r["done"] == r["total"]
    assert r["per_node"] == [{"label": "this laptop", "count": r["total"]}]
    assert r["results"]["local"]["seconds"] > 0
    assert "Option price" in r["answer"] and "Black-Scholes" in r["answer"]


def test_mesh_mode_without_compatible_laptops_explains_why(ui):
    _, client = ui
    client.post("/api/setup", json={"secret": SECRET, "mesh": "default"})
    r = client.post("/api/run", json={"demo": "option", "size": "small", "mode": "compare"})
    assert r.status_code == 400 and "No compatible laptops" in r.json()["detail"]


def test_second_run_while_one_is_running_is_refused(ui):
    _, client = ui
    first = {"demo": "backtest", "size": "medium", "mode": "local"}
    assert client.post("/api/run", json=first).status_code == 202
    again = {"demo": "option", "size": "small", "mode": "local"}
    assert client.post("/api/run", json=again).status_code == 409
    wait_for(client, finished)


def test_compare_with_found_laptop(ui, agent):
    state, client = ui
    state.local_workers = 1  # so the found laptop is sure to get chunks on a many-core machine
    addr, _ = agent(secret=SECRET)
    client.post("/api/setup", json={"secret": SECRET, "mesh": "default"})
    assert client.post("/api/peers", json={"address": addr}).status_code == 201
    s = wait_for(client, lambda s: s["peers"] and s["peers"][0]["status"] == "compatible")
    peer = s["peers"][0]
    port = addr.split(":")[1]
    assert peer["name"] == f"test-agent-{port}" and peer["lendable"] is True
    assert peer["rtt_ms"] is not None

    run = {"demo": "option", "size": "small", "mode": "compare"}
    assert client.post("/api/run", json=run).status_code == 202
    r = wait_for(client, finished)["run"]
    assert r["error"] is None
    assert set(r["results"]) == {"local", "mesh"}
    assert r["match"] is True and r["speedup"] is not None
    mesh_labels = {n["label"] for n in r["results"]["mesh"]["per_node"]}
    assert f"test-agent-{port}" in mesh_labels


def test_wrong_secret_peer_shows_no_reply(ui, agent):
    _, client = ui
    addr, _ = agent(secret=SECRET)
    client.post("/api/setup", json={"secret": "a-different-secret", "mesh": "default"})
    client.post("/api/peers", json={"address": addr})
    s = wait_for(client, lambda s: s["peers"] and s["peers"][0]["status"] == "noreply")
    assert "wrong secret" in s["peers"][0]["reason"]
    assert s["peers"][0]["name"] is None


# -- demos and token ---------------------------------------------------------------------


def test_demos_have_every_size_and_ml_reports_missing_sklearn(monkeypatch):
    assert set(DEMOS) == {"option", "backtest", "ml"}
    for demo in DEMOS.values():
        for size in SIZES:
            assert demo.make_inputs(size), (demo.name, size)
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name, *a: None if name == "sklearn" else real(name, *a)
    )
    ok, why = DEMOS["ml"].available()
    assert not ok and why == "pip install scikit-learn on every laptop"


def test_token_survives_restart(isolated_home):
    first = load_token()
    assert len(first) >= 32 and load_token() == first
    assert (isolated_home / "ui_token").exists()


def test_ui_runs_use_a_fork_safe_pool(ui, monkeypatch):
    """The UI process runs threads, so its worker pools must never use fork()."""
    import idlemesh.sdk

    contexts = []
    real_mesh = idlemesh.sdk.Mesh

    def spy(*args, **kwargs):
        contexts.append(kwargs.get("mp_context"))
        return real_mesh(*args, **kwargs)

    monkeypatch.setattr(idlemesh.sdk, "Mesh", spy)
    _, client = ui
    client.post("/api/run", json={"demo": "option", "size": "small", "mode": "local"})
    wait_for(client, finished)
    assert contexts and all(c is not None for c in contexts)
    assert all(c.get_start_method() != "fork" for c in contexts)
