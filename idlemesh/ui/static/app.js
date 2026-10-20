"use strict";
// IdleMesh UI. All data from the server (including names other laptops report) is
// inserted with textContent, never innerHTML.

const token = new URLSearchParams(location.search).get("token") || "";
const $ = (id) => document.getElementById(id);
let state = null;
let builtDemos = false;

// ---------------------------------------------------------------- API helpers
async function api(method, path, body) {
  const res = await fetch(`/api/${path}`, {
    method,
    headers: { "Content-Type": "application/json", "X-IdleMesh-Token": token },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    let msg = `Request failed (${res.status})`;
    try {
      const j = await res.json();
      msg = typeof j.detail === "string"
        ? j.detail
        : j.detail.map((e) => `${e.field || "request"}: ${e.message}`).join("; ");
    } catch (_) { /* keep the generic message */ }
    throw new Error(msg);
  }
  return res.status === 204 ? null : res.json();
}

function note(el, msg, ok = false) {
  el.textContent = msg || "";
  el.className = "note " + (ok ? "ok" : "bad");
}

function el(tag, text, cls) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = String(text);
  if (cls) e.className = cls;
  return e;
}

// ---------------------------------------------------------------- live updates
let source = null;
function connect() {
  if (source) source.close();
  source = new EventSource(`/api/events?token=${encodeURIComponent(token)}`);
  source.onopen = () => setConnected(true);
  source.onmessage = (e) => {
    state = JSON.parse(e.data);
    render();
  };
  source.onerror = () => {
    setConnected(false);
    // EventSource retries by itself on network errors; if the browser gave up, start over.
    if (source.readyState === EventSource.CLOSED) setTimeout(connect, 2000);
  };
}

function setConnected(ok) {
  const c = $("conn");
  c.textContent = ok ? "connected" : "reconnecting…";
  c.className = "pill" + (ok ? " ok" : "");
}

// ---------------------------------------------------------------- rendering
function fmtBytes(n) {
  if (n === null || n === undefined) return "?";
  const gb = n / 1024 ** 3;
  return gb >= 1 ? `${gb.toFixed(1)} GB` : `${Math.round(n / 1024 ** 2)} MB`;
}

function fmtSeconds(s) {
  if (s === null || s === undefined) return "-";
  return s < 60 ? `${s.toFixed(1)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
}

function render() {
  if (!state) return;
  renderSetup();
  renderLend();
  renderPeers();
  renderDemos();
  renderRun();
  renderLog();
}

function renderSetup() {
  const { setup, lend } = state;
  $("secret-status").textContent = setup.secret_set ? "Secret set ✓" : "No secret yet";
  if (document.activeElement !== $("mesh")) $("mesh").value = setup.mesh;
  for (const id of ["secret", "mesh"]) $(id).disabled = lend.on;
  $("setup-form").querySelector("button").disabled = lend.on;
  $("setup-form").title = lend.on ? "Switch lending off to change these." : "";
}

function renderLend() {
  const { lend, machine, setup } = state;
  const toggle = $("lend-toggle");
  toggle.checked = lend.on;
  toggle.disabled = !setup.secret_set && !lend.on;
  $("lend-label").textContent = lend.on ? "On: lending this laptop" : "Off";
  const w = $("workers");
  w.max = String(machine.cpus);
  if (!w.dataset.touched || lend.on) w.value = String(lend.workers || machine.cpus);
  $("workers-out").textContent = `${w.value} of ${machine.cpus}`;
  w.disabled = lend.on;
  $("force").disabled = lend.on;
  if (lend.on) $("force").checked = lend.force;
  $("ips").textContent = machine.ips.length ? machine.ips.join(", ") : "no network";
  $("python").textContent = machine.python;
  $("served").textContent = String(lend.chunks_served);
  const hint = $("firewall-hint");
  hint.hidden = !lend.on;
  hint.textContent =
    machine.platform === "darwin"
      ? "If macOS asks whether to accept incoming network connections, click Allow."
      : "If Windows asks about the firewall, click Allow (Private networks).";
  if (lend.on && !["win32", "darwin"].includes(machine.platform)) hint.hidden = true;
  if (lend.error) note($("lend-note"), lend.error);
}

function renderPeers() {
  const body = $("peers");
  body.replaceChildren();
  for (const p of state.peers) {
    const tr = document.createElement("tr");
    tr.append(
      el("td", p.name ?? "?"),
      el("td", p.address),
      el("td", p.workers ?? "?"),
      el("td", fmtBytes(p.ram_free)),
      el("td", p.python ?? "?"),
      el("td", p.lendable === null ? "?" : p.lendable ? "yes" : "no"),
      el("td", p.rtt_ms === null ? "-" : `${p.rtt_ms} ms`),
    );
    const status = el("td");
    status.append(el("span", p.reason, `badge ${p.status}`));
    tr.append(status);
    const actions = el("td");
    if (p.manual) {
      const rm = el("button", "Remove", "link");
      rm.type = "button";
      rm.addEventListener("click", () =>
        api("DELETE", `peers/${encodeURIComponent(p.address)}`).catch((e) => note($("peer-note"), e.message)));
      actions.append(rm);
    }
    tr.append(actions);
    body.append(tr);
  }
  $("peers-empty").hidden = state.peers.length > 0;
}

function renderDemos() {
  const box = $("demos");
  if (!builtDemos) {
    for (const d of state.demos) {
      const label = el("label", null, "choice");
      const radio = el("input");
      radio.type = "radio";
      radio.name = "demo";
      radio.value = d.name;
      const text = el("span");
      text.append(el("strong", d.title), el("small", d.description), el("small", "", "why"));
      label.append(radio, text);
      box.append(label);
    }
    const size = $("size");
    for (const s of state.sizes) {
      const o = el("option", s[0].toUpperCase() + s.slice(1));
      o.value = s;
      size.append(o);
    }
    size.value = "small";
    const first = box.querySelector("input:not(:disabled)");
    if (first) first.checked = true;
    builtDemos = true;
  }
  for (const d of state.demos) {
    const radio = box.querySelector(`input[value="${d.name}"]`);
    radio.disabled = !d.available;
    radio.closest(".choice").classList.toggle("disabled", !d.available);
    radio.closest(".choice").querySelector(".why").textContent = d.available ? "" : d.reason;
    if (radio.checked && !d.available) radio.checked = false;
  }
  if (!box.querySelector("input:checked")) {
    const first = box.querySelector("input:not(:disabled)");
    if (first) first.checked = true;
  }
  const running = state.run && !state.run.finished;
  $("run").disabled = running;
  $("run").textContent = running ? "Running…" : "Run";
}

function renderRun() {
  const run = state.run;
  $("progress-idle").hidden = !!run;
  $("progress").hidden = !run;
  if (!run) return;
  const where = { local: "this laptop only", mesh: "with found laptops", compare: "compare both" }[run.mode];
  $("run-title").textContent = `${run.title} · ${run.size} · ${where}`;
  $("run-phase").textContent = run.finished
    ? (run.error ? "failed" : "finished")
    : run.phase === "local" ? "running on this laptop…" : "running with found laptops…";
  const pct = run.total ? Math.round((100 * run.done) / run.total) : 0;
  $("bar-fill").style.width = `${run.finished && !run.error ? 100 : pct}%`;
  $("bar").setAttribute("aria-valuenow", String(pct));
  $("run-count").textContent = run.total ? `${run.done} of ${run.total} chunks (${pct}%)` : "starting…";
  updateElapsed();

  const list = $("per-node");
  list.replaceChildren();
  for (const n of run.per_node) {
    const li = el("li");
    li.append(el("span", `${n.label}: `), el("strong", n.count));
    if (n.seconds_per_chunk !== null && n.seconds_per_chunk !== undefined) {
      li.append(el("span", ` (${n.seconds_per_chunk.toFixed(2)} s/chunk)`, "muted"));
    }
    list.append(li);
  }

  const backups = $("run-backups");
  backups.hidden = !run.backups_launched;
  backups.textContent = `Backup tasks: ${run.backups_launched} launched, ${run.backups_won} won`;
  const skipped = $("run-skipped");
  const idle = run.skipped || [];
  skipped.replaceChildren(
    ...(idle.length ? [el("li", "Laptops that ran nothing:", "muted")] : []),
    ...idle.map((s) => el("li", s)),
  );
  skipped.hidden = !(run.skipped || []).length;

  const done = run.finished && !run.error;
  $("run-result").hidden = !done;
  if (done) {
    const rows = $("result-rows");
    rows.replaceChildren();
    const add = (k, v) => {
      const tr = el("tr");
      tr.append(el("td", k), el("td", v));
      rows.append(tr);
    };
    if (run.results.local) add("This laptop only", fmtSeconds(run.results.local.seconds));
    if (run.results.mesh) add("With found laptops", fmtSeconds(run.results.mesh.seconds));
    if (run.speedup !== null && !run.no_remote_work) add("Speedup", `${run.speedup}×`);
    if (run.match !== null) add("Results", run.match ? "✓ results match" : "✗ results differ");
    $("run-answer").textContent = run.answer || "";
    const warning = $("run-warning");
    warning.hidden = !run.no_remote_work;
    warning.textContent = run.no_remote_work
      ? "No other laptop ran any work — check Laptops found and the log"
      : "";
  }
  const err = $("run-error");
  err.hidden = !run.error;
  err.textContent = run.error || "";
}

function updateElapsed() {
  const run = state && state.run;
  if (!run) return;
  const end = run.finished ? run.ended : Date.now() / 1000;
  $("run-elapsed").textContent = `elapsed ${fmtSeconds(Math.max(0, end - run.started))}`;
}

function renderLog() {
  const log = $("log");
  const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 20;
  log.replaceChildren();
  for (const line of state.log) {
    const li = el("li", null, line.level);
    li.append(el("time", line.time), document.createTextNode(line.message));
    log.append(li);
  }
  if (atBottom) log.scrollTop = log.scrollHeight;
}

// ---------------------------------------------------------------- actions
function wire() {
  $("setup-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const secret = $("secret").value;
    try {
      await api("POST", "setup", { secret: secret || null, mesh: $("mesh").value.trim() });
      $("secret").value = ""; // the page never keeps the secret
      note($("setup-note"), "Saved.", true);
    } catch (err) {
      note($("setup-note"), err.message);
    }
  });

  $("workers").addEventListener("input", (e) => {
    e.target.dataset.touched = "1";
    $("workers-out").textContent = `${e.target.value} of ${e.target.max}`;
  });

  $("lend-toggle").addEventListener("change", async (e) => {
    const on = e.target.checked;
    e.target.disabled = true;
    note($("lend-note"), on ? "Starting…" : "Stopping…", true);
    try {
      await api("POST", "lend", { on, workers: Number($("workers").value), force: $("force").checked });
      note($("lend-note"), "");
    } catch (err) {
      e.target.checked = !on;
      note($("lend-note"), err.message);
    } finally {
      e.target.disabled = false;
    }
  });

  $("peer-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const r = await api("POST", "peers", { address: $("peer-address").value.trim() });
      $("peer-address").value = "";
      note($("peer-note"), `Added ${r.address}. Checking it…`, true);
    } catch (err) {
      note($("peer-note"), err.message);
    }
  });

  $("run").addEventListener("click", async () => {
    const demo = document.querySelector('input[name="demo"]:checked');
    if (!demo) return note($("run-note"), "Pick a demo first.");
    try {
      await api("POST", "run", { demo: demo.value, size: $("size").value, mode: $("mode").value });
      note($("run-note"), "");
    } catch (err) {
      note($("run-note"), err.message);
    }
  });
}

wire();
connect();
setInterval(updateElapsed, 250);
