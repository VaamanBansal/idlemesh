"""`idlemesh ui`: a local web page for two-laptop testing.

Security:
  - binds to 127.0.0.1 only (there is no option to change that);
  - every request needs a random token (opened in the browser for you, printed in
    the terminal), so other websites in your browser can't drive the UI;
  - requests whose Host header isn't 127.0.0.1/localhost are refused (DNS rebinding);
  - the page can only run the built-in demos: requests are validated against fixed
    names and extra fields are rejected;
  - the secret stays in memory: never returned by the API, logged, or written to disk.
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import os
import secrets
import threading
import time
import webbrowser
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

from idlemesh.agent.discovery import idlemesh_home
from idlemesh.demos import DEMOS
from idlemesh.ui.state import Busy, UIError, UIState

log = logging.getLogger("idlemesh.ui")

STATIC = Path(__file__).parent / "static"
HOST = "127.0.0.1"
ALLOWED_HOSTS = ["127.0.0.1", "localhost"]
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
    ),
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-store",
}

DemoName = Literal["option", "backtest", "ml"]
assert set(DemoName.__args__) == set(DEMOS), "keep DemoName in sync with idlemesh.demos"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SetupIn(_Strict):
    secret: str | None = Field(default=None, max_length=1024)  # None = keep the current one
    mesh: str = Field(default="default", max_length=63)


class LendIn(_Strict):
    on: bool
    workers: int | None = Field(default=None, ge=1, le=4096)
    force: bool = False


class PeerIn(_Strict):
    address: str = Field(max_length=300)


class RunIn(_Strict):
    demo: DemoName
    size: Literal["small", "medium", "large"]
    mode: Literal["local", "mesh", "compare"]


def create_app(state: UIState, token: str) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=ALLOWED_HOSTS)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        for key, value in SECURITY_HEADERS.items():
            response.headers.setdefault(key, value)
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError):
        # FastAPI's default echoes the submitted values back, which could include the secret.
        errors = [
            {"field": ".".join(str(p) for p in e.get("loc", ())[1:]), "message": e.get("msg")}
            for e in exc.errors()
        ]
        return JSONResponse({"detail": errors}, status_code=422)

    def require_token(request: Request) -> None:
        supplied = request.headers.get("x-idlemesh-token") or request.query_params.get("token")
        if not supplied or not secrets.compare_digest(supplied, token):
            raise HTTPException(
                403, "Missing or wrong token. Open the URL printed by `idlemesh ui`."
            )

    @app.get("/", dependencies=[Depends(require_token)])
    def index():
        return FileResponse(STATIC / "index.html", media_type="text/html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    api = APIRouter(prefix="/api", dependencies=[Depends(require_token)])

    def call(fn, *args):
        try:
            return fn(*args)
        except Busy as exc:
            raise HTTPException(409, str(exc)) from None
        except UIError as exc:
            raise HTTPException(400, str(exc)) from None

    @api.get("/state")
    def get_state():
        return state.snapshot()

    @api.post("/setup", status_code=204)
    def post_setup(body: SetupIn):
        call(state.setup, body.secret, body.mesh)

    @api.post("/lend", status_code=204)
    def post_lend(body: LendIn):
        call(state.set_lending, body.on, body.workers, body.force)

    @api.post("/peers", status_code=201)
    def post_peer(body: PeerIn):
        return {"address": call(state.add_peer, body.address)}

    @api.delete("/peers/{address}", status_code=204)
    def delete_peer(address: str):
        state.remove_peer(address)

    @api.post("/run", status_code=202)
    def post_run(body: RunIn):
        call(state.start_run, body.demo, body.size, body.mode)

    @api.get("/events")
    async def events(request: Request):
        """Server-sent events: the full snapshot whenever it changes (EventSource reconnects)."""

        async def stream():
            last, last_sent = None, 0.0
            while not await request.is_disconnected():
                snap = json.dumps(await asyncio.to_thread(state.snapshot))
                now = time.monotonic()
                if snap != last:
                    yield f"data: {snap}\n\n"
                    last, last_sent = snap, now
                elif now - last_sent > 15:
                    yield ": keep-alive\n\n"
                    last_sent = now
                await asyncio.sleep(0.5)

        return StreamingResponse(stream(), media_type="text/event-stream")

    app.include_router(api)
    return app


def load_token() -> str:
    """A random token, kept in ~/.idlemesh/ui_token so an open page survives a restart."""
    path = idlemesh_home() / "ui_token"
    try:
        existing = path.read_text().strip()
        if len(existing) >= 32:
            return existing
    except OSError:
        pass
    token = secrets.token_urlsafe(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return token


def make_server(state: UIState, token: str, port: int):
    """The uvicorn server `idlemesh ui` runs: always 127.0.0.1, no access log."""
    import uvicorn

    config = uvicorn.Config(
        create_app(state, token), host=HOST, port=port, access_log=False, log_level="warning"
    )
    return uvicorn.Server(config)


def serve_ui(port: int = 8765, open_browser: bool = True) -> None:
    # The UI runs worker pools from a process that also runs threads (web server,
    # ZeroMQ, zeroconf); fork() there can deadlock, so use forkserver where fork is
    # the default (Linux before Python 3.14).
    if multiprocessing.get_context().get_start_method() == "fork":
        multiprocessing.set_start_method("forkserver", force=True)

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    token = load_token()
    try:
        from idlemesh.agent.discovery import ZeroconfBrowser

        browser = ZeroconfBrowser()
    except ImportError:
        browser = None
    state = UIState(browser=browser)
    server = make_server(state, token, port)
    url = f"http://{HOST}:{port}/?token={token}"
    print(f"IdleMesh UI: {url}", flush=True)
    print("Open that link if your browser doesn't open by itself. Ctrl+C to stop.", flush=True)

    if open_browser:

        def open_when_ready():
            deadline = time.monotonic() + 15
            while not server.started and time.monotonic() < deadline:
                time.sleep(0.1)
            if server.started:
                webbrowser.open(url)

        threading.Thread(target=open_when_ready, daemon=True).start()
    try:
        server.run()
    finally:
        state.close()
