# sa/api.py
#
# FastAPI operator control surface for the SA.
#
# ----- What FastAPI is -----
# FastAPI is a Python web framework. It lets you define URL routes (like
# /policy, /emergency-stop) that a browser or curl can call. When the
# operator opens a browser tab and clicks a button, the browser sends an
# HTTP request to FastAPI, and FastAPI runs the corresponding Python function.
#
# FastAPI is chosen here because:
#   1. It is lightweight — it does not require a separate process; it runs
#      on a background thread inside the SA process alongside the MQTT loop.
#   2. It produces a usable HTML interface without any JavaScript framework.
#   3. Its async support means it does not block the MQTT callback threads.
#
# ----- What the operator sees -----
# The web UI is a single HTML page served at http://<node-ip>:<port>/.
# It has five sections:
#
#   [System Status]  — current emergency stop state, DCA liveness, last policy version.
#   [Escalation Queue] — list of SA_NOTIFICATION messages received from DCAs,
#                        newest first, with severity and deployment context.
#   [Domain Status Reports] — latest DOMAIN_STATUS_REPORT from each DCA.
#   [Policy Override] — a text area where the operator pastes a JSON policy
#                       document, then clicks "Apply". The SA validates it and
#                       either applies it or shows the validation error.
#   [Control Buttons] — EMERGENCY STOP and RESUME buttons.
#
# All state (escalation queue, current mode, policy) is held in the
# OperatorState object which is passed in from main.py. The FastAPI routes
# read from and write to this object via simple Python calls.
#
# ----- Thread safety -----
# OperatorState is shared between the MQTT callback thread and the FastAPI
# HTTP handler thread. A single threading.Lock protects all mutations.
#
import json
import threading
import time
from collections import deque
from typing import Optional

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

log = structlog.get_logger(__name__)

# Maximum escalation records kept in memory (newest wins on overflow).
MAX_ESCALATIONS = 200


class OperatorState:
    """
    Shared mutable state between the MQTT agent thread and the FastAPI thread.
    All reads and writes must acquire the lock.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # True when EMERGENCY_STOP has been issued and RESUME has not yet been sent.
        self.emergency_stop_active: bool = False
        # Escalation queue: list of dicts, newest appended last, displayed newest first.
        self.escalations: deque = deque(maxlen=MAX_ESCALATIONS)
        # Latest DOMAIN_STATUS_REPORT per DCA domain_id.
        self.domain_status: dict = {}     # domain_id → payload dict
        # Current policy (as loaded / last applied).
        self.current_policy: dict = {}
        # DCA liveness: domain_id → { "last_seen_ms": int, "failed": bool }
        self.dca_liveness: dict = {}

    # ---- Thread-safe accessors -------------------------------------------

    def add_escalation(self, domain_id: str, payload: dict) -> None:
        with self._lock:
            payload["_received_ms"] = int(time.time() * 1000)
            payload["_domain_id"]   = domain_id
            self.escalations.append(payload)

    def update_domain_status(self, domain_id: str, payload: dict) -> None:
        with self._lock:
            self.domain_status[domain_id] = payload

    def update_dca_liveness(self, domain_id: str, failed: bool) -> None:
        with self._lock:
            self.dca_liveness[domain_id] = {
                "last_seen_ms": int(time.time() * 1000),
                "failed": failed,
            }

    def set_policy(self, policy: dict) -> None:
        with self._lock:
            self.current_policy = policy

    def set_emergency_stop(self, active: bool) -> None:
        with self._lock:
            self.emergency_stop_active = active

    def snapshot(self) -> dict:
        """Return a thread-safe snapshot of the full state."""
        with self._lock:
            return {
                "emergency_stop_active": self.emergency_stop_active,
                "escalations":           list(self.escalations)[-50:],  # latest 50
                "domain_status":         dict(self.domain_status),
                "dca_liveness":          dict(self.dca_liveness),
                "current_policy":        dict(self.current_policy),
            }


# ---- Request / response models -------------------------------------------

class PolicyOverrideRequest(BaseModel):
    policy_json: str  # The operator pastes a JSON string into the textarea.


class ForcedActionRequest(BaseModel):
    domain_id: str
    action_type: str    # e.g. "SCALE_TO", "RESTART_AGENT"
    target: Optional[str] = None
    parameters: Optional[dict] = None


# ---- App factory ---------------------------------------------------------

def build_app(
    state: OperatorState,
    on_policy_override,     # callable(proposed: dict) → dict (written policy)
    on_emergency_stop,      # callable() → None
    on_resume,              # callable() → None
    on_forced_action,       # callable(domain_id, action_type, target, params) → None
) -> FastAPI:
    """
    Build and return the FastAPI application.

    Callbacks are injected from main.py so the API layer has no direct
    dependency on MQTT or Kubernetes clients — it calls Python functions
    and trusts main.py to do the right thing.
    """

    app = FastAPI(
        title="MAS Supervision Agent",
        description="Operator control surface for the Multi-Agent System",
        version="1.0.0",
        docs_url="/docs",
        redoc_url=None,
    )

    # ---- HTML page -------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def index():
        """Serve the main operator interface page."""
        snapshot = state.snapshot()
        return _render_html(snapshot)

    # ---- JSON API endpoints ----------------------------------------------

    @app.get("/api/state")
    async def get_state():
        """Return current SA state as JSON (useful for debugging or dashboards)."""
        return JSONResponse(content=state.snapshot())

    @app.post("/api/policy")
    async def post_policy(request: PolicyOverrideRequest):
        """
        Policy override endpoint.
        The operator pastes a JSON document; the SA validates and applies it.
        Returns the applied policy on success, or a 422 with the validation error.
        """
        try:
            proposed = json.loads(request.policy_json)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400,
                                detail=f"Invalid JSON: {e}")
        try:
            applied = on_policy_override(proposed)
            state.set_policy(applied)
            return {"status": "ok", "policy_version": applied.get("policy_version")}
        except Exception as e:
            raise HTTPException(status_code=422, detail=str(e))

    @app.post("/api/emergency-stop")
    async def post_emergency_stop():
        """Issue EMERGENCY_STOP to all DCAs."""
        if state.emergency_stop_active:
            return {"status": "already_active"}
        on_emergency_stop()
        state.set_emergency_stop(True)
        return {"status": "emergency_stop_issued"}

    @app.post("/api/resume")
    async def post_resume():
        """Issue RESUME to all DCAs, restoring autonomous operation."""
        if not state.emergency_stop_active:
            return {"status": "not_in_emergency_stop"}
        on_resume()
        state.set_emergency_stop(False)
        return {"status": "resume_issued"}

    @app.post("/api/forced-action")
    async def post_forced_action(request: ForcedActionRequest):
        """
        Forced action: validated against safety constraints, then routed to DCA.
        Safety validation is performed inside the callback (main.py).
        """
        try:
            on_forced_action(
                request.domain_id,
                request.action_type,
                request.target,
                request.parameters or {},
            )
            return {"status": "ok", "domain_id": request.domain_id,
                    "action_type": request.action_type}
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/escalations")
    async def get_escalations():
        """Return the escalation queue (latest 50 entries)."""
        snap = state.snapshot()
        return JSONResponse(content={"escalations": snap["escalations"]})

    @app.get("/healthz")
    async def healthz():
        """Liveness probe endpoint for Kubernetes."""
        return {"status": "ok"}

    return app


# ---- HTML renderer -------------------------------------------------------

def _render_html(snapshot: dict) -> str:
    """
    Generate the operator interface HTML page from the current state snapshot.

    This is intentionally plain HTML with inline CSS — no JavaScript
    framework, no build step. The page auto-refreshes every 15 seconds so
    the operator always sees the latest state without any interactive
    push mechanism.
    """
    stop_active = snapshot["emergency_stop_active"]
    escalations = snapshot["escalations"]
    dca_liveness = snapshot["dca_liveness"]
    policy = snapshot["current_policy"]
    domain_status = snapshot["domain_status"]

    # ---- Emergency stop banner -------------------------------------------
    if stop_active:
        banner = (
            '<div style="background:#c0392b;color:#fff;padding:16px;font-size:1.2em;'
            'text-align:center;font-weight:bold;">'
            '🚨 EMERGENCY STOP ACTIVE — All autonomous scaling is suspended'
            '</div>'
        )
    else:
        banner = (
            '<div style="background:#27ae60;color:#fff;padding:8px;'
            'text-align:center;">✓ System operating normally</div>'
        )

    # ---- DCA liveness rows -----------------------------------------------
    dca_rows = ""
    if not dca_liveness:
        dca_rows = '<tr><td colspan="3"><em>No DCA heartbeats received yet</em></td></tr>'
    for domain_id, info in dca_liveness.items():
        status_str = "❌ FAILED" if info["failed"] else "✅ ALIVE"
        last_ms = info.get("last_seen_ms", 0)
        last_s  = round((time.time() * 1000 - last_ms) / 1000, 1)
        dca_rows += (
            f"<tr><td>{domain_id}</td>"
            f"<td>{status_str}</td>"
            f"<td>{last_s}s ago</td></tr>"
        )

    # ---- Escalation rows -------------------------------------------------
    esc_rows = ""
    if not escalations:
        esc_rows = '<tr><td colspan="5"><em>No escalations received</em></td></tr>'
    for esc in reversed(escalations):  # newest first
        received_ms = esc.get("_received_ms", 0)
        ago_s = round((time.time() * 1000 - received_ms) / 1000, 1)
        esc_type   = esc.get("event_type", esc.get("escalation_type", "UNKNOWN"))
        domain_id  = esc.get("_domain_id", "?")
        dep        = esc.get("deployment", "—")
        ns         = esc.get("namespace", "—")
        esc_rows += (
            f"<tr>"
            f"<td>{ago_s}s ago</td>"
            f"<td>{domain_id}</td>"
            f"<td><strong>{esc_type}</strong></td>"
            f"<td>{ns}/{dep}</td>"
            f"<td><code style='font-size:0.8em'>{_truncate(str(esc), 120)}</code></td>"
            f"</tr>"
        )

    # ---- Domain status section -------------------------------------------
    ds_section = ""
    for domain_id, ds in domain_status.items():
        ds_section += (
            f"<h4>Domain: {domain_id}</h4>"
            f"<pre style='background:#f8f8f8;padding:8px;overflow:auto;max-height:200px'>"
            f"{json.dumps(ds, indent=2)}</pre>"
        )
    if not ds_section:
        ds_section = "<p><em>No domain status reports received yet</em></p>"

    # ---- Policy display --------------------------------------------------
    policy_json = json.dumps(policy, indent=2) if policy else "{}"
    policy_version = policy.get("policy_version", "—")

    # ---- Control button state -------------------------------------------
    stop_disabled  = "disabled" if stop_active  else ""
    resume_disabled = "disabled" if not stop_active else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="15">
  <title>MAS Supervision Agent</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 16px; }}
    h2 {{ border-bottom: 2px solid #333; padding-bottom: 4px; }}
    h3 {{ color: #444; }}
    table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; }}
    th {{ background: #333; color: #fff; padding: 8px; text-align: left; }}
    td {{ padding: 6px 8px; border: 1px solid #ddd; vertical-align: top; }}
    tr:nth-child(even) {{ background: #fafafa; }}
    .card {{ background: #fff; border-radius: 6px; padding: 16px;
             box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 20px; }}
    button {{ padding: 10px 20px; font-size: 1em; cursor: pointer;
              border: none; border-radius: 4px; }}
    .btn-stop   {{ background: #c0392b; color: #fff; }}
    .btn-resume {{ background: #27ae60; color: #fff; }}
    .btn-apply  {{ background: #2980b9; color: #fff; }}
    button:disabled {{ opacity: 0.4; cursor: not-allowed; }}
    textarea {{ width: 100%; height: 280px; font-family: monospace;
                font-size: 0.85em; border: 1px solid #ccc; padding: 8px; }}
    .msg {{ padding: 8px; margin: 8px 0; border-radius: 4px; }}
    .msg-ok  {{ background: #d5f5e3; color: #1e8449; }}
    .msg-err {{ background: #fadbd8; color: #922b21; }}
    pre {{ margin: 0; white-space: pre-wrap; word-break: break-all; }}
  </style>
  <script>
    // Form submission via fetch so the page does not reload fully.
    async function postAction(url, body, successMsg) {{
      try {{
        const res = await fetch(url, {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify(body),
        }});
        const data = await res.json();
        if (res.ok) {{
          document.getElementById('msg').className = 'msg msg-ok';
          document.getElementById('msg').textContent = successMsg + ' — ' + JSON.stringify(data);
        }} else {{
          document.getElementById('msg').className = 'msg msg-err';
          document.getElementById('msg').textContent = 'Error: ' + (data.detail || JSON.stringify(data));
        }}
      }} catch(e) {{
        document.getElementById('msg').className = 'msg msg-err';
        document.getElementById('msg').textContent = 'Network error: ' + e;
      }}
    }}

    function applyPolicy() {{
      const pj = document.getElementById('policy-textarea').value;
      postAction('/api/policy', {{policy_json: pj}}, 'Policy applied');
    }}

    function emergencyStop() {{
      if (!confirm('Issue EMERGENCY STOP? All autonomous scaling will be suspended.')) return;
      postAction('/api/emergency-stop', {{}}, 'Emergency stop issued');
    }}

    function resume() {{
      if (!confirm('Issue RESUME? Autonomous scaling will be restored.')) return;
      postAction('/api/resume', {{}}, 'Resume issued');
    }}
  </script>
</head>
<body>
  {banner}
  <div class="container">
    <h2>MAS Supervision Agent — Operator Interface</h2>
    <p style="color:#888;font-size:0.85em">
      Page auto-refreshes every 15 seconds. Policy version: <strong>{policy_version}</strong>
    </p>

    <div id="msg" class="msg" style="display:none"></div>

    <!-- ---- DCA Liveness ---- -->
    <div class="card">
      <h3>DCA Liveness</h3>
      <table>
        <thead><tr><th>Domain ID</th><th>Status</th><th>Last heartbeat</th></tr></thead>
        <tbody>{dca_rows}</tbody>
      </table>
    </div>

    <!-- ---- Escalation Queue ---- -->
    <div class="card">
      <h3>Escalation Queue (latest 50)</h3>
      <table>
        <thead>
          <tr>
            <th>Age</th><th>Domain</th><th>Type</th>
            <th>Deployment</th><th>Detail</th>
          </tr>
        </thead>
        <tbody>{esc_rows}</tbody>
      </table>
    </div>

    <!-- ---- Domain Status Reports ---- -->
    <div class="card">
      <h3>Domain Status Reports</h3>
      {ds_section}
    </div>

    <!-- ---- Control Buttons ---- -->
    <div class="card">
      <h3>Emergency Controls</h3>
      <p>
        <button class="btn-stop"   onclick="emergencyStop()" {stop_disabled}>
          🚨 EMERGENCY STOP
        </button>
        &nbsp;&nbsp;
        <button class="btn-resume" onclick="resume()" {resume_disabled}>
          ▶ RESUME
        </button>
      </p>
      <p style="color:#888;font-size:0.85em">
        EMERGENCY STOP suspends all autonomous HPA patching across all RSA instances.
        RESUME restores autonomous operation. Both commands are issued at MQTT QoS 2.
      </p>
    </div>

    <!-- ---- Policy Override ---- -->
    <div class="card">
      <h3>Policy Override (Domain 1)</h3>
      <p style="color:#888;font-size:0.85em">
        Paste a complete, valid policy JSON document below, then click Apply.
        The SA validates it before writing. Validation errors are shown above.
      </p>
      <textarea id="policy-textarea">{policy_json}</textarea>
      <br><br>
      <button class="btn-apply" onclick="applyPolicy()">Apply Policy</button>
    </div>

  </div>
</body>
</html>"""
    return html


def _truncate(s: str, max_len: int) -> str:
    return s if len(s) <= max_len else s[:max_len] + "…"


# ---- Server runner -------------------------------------------------------

def run_server(app: FastAPI, host: str, port: int) -> None:
    """
    Start the uvicorn server. This function BLOCKS — call it on a
    dedicated daemon thread from main.py.
    """
    uvicorn.run(app, host=host, port=port, log_level="warning")
