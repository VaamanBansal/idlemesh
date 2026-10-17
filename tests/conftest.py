import os
import queue
import socket
import subprocess
import sys
import threading
import time

import psutil
import pytest


@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    """Never read or write the real ~/.idlemesh (node ID) during tests."""
    home = tmp_path / "client-home"
    monkeypatch.setenv("IDLEMESH_HOME", str(home))
    return home


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def kill_tree(proc):
    try:
        parent = psutil.Process(proc.pid)
        procs = [*parent.children(recursive=True), parent]
    except psutil.NoSuchProcess:
        return
    for p in procs:
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(procs, timeout=10)


@pytest.fixture
def kill():
    return kill_tree


@pytest.fixture
def agent(tmp_path):
    """Start `idlemesh serve` on localhost (no mDNS); returns ("127.0.0.1:port", proc)."""
    started = []

    def start(secret="test-secret", workers=2):
        port = free_port()
        cmd = [sys.executable, "-m", "idlemesh.cli", "serve", "--host", "127.0.0.1"]
        cmd += ["--port", str(port), "--secret", secret, "--workers", str(workers), "--force"]
        cmd += ["--no-advertise", "--name", f"test-agent-{port}"]
        env = {**os.environ, "IDLEMESH_TEST_AGENT": "1", "IDLEMESH_HOME": str(tmp_path / "agent")}
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env
        )
        started.append(proc)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(ln) for ln in proc.stdout], daemon=True).start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if "listening" in lines.get(timeout=1):
                    return f"127.0.0.1:{port}", proc
            except queue.Empty:
                if proc.poll() is not None:
                    break
        raise RuntimeError("agent did not start")

    yield start
    for proc in started:
        kill_tree(proc)
