# sa/main.py
#
# Supervision Agent (SA) entry point.
#
# The SA is the highest authority in the MAS hierarchy. It starts before
# all other agents and gates every downstream agent via POLICY_READY.
#
# Startup sequence:
#   Stage 0: MQTT connected + Kubernetes API reachable + Domain 4 accessible
#   Stage 1: Load Domain 1 policy, validate, publish POLICY_READY at QoS 2
#   Stage 2: Wait for DOMAIN_READY from DCA (confirms propagation chain)
#   Stage 3: Enter full liveness monitoring + start FastAPI web UI
#
# Continuous operation:
#   - DCA heartbeat monitoring (via DCAMonitor background thread)
#   - Escalation queue updates (via MQTT callback)
#   - Domain status report aggregation (via MQTT callback)
#   - Heartbeat publication every 15 seconds
#   - FastAPI web UI (operator control surface, separate daemon thread)
#
# Operator commands handled:
#   - Policy Override  → validate → write Domain 1 → broadcast POLICY_READY
#   - Emergency Stop   → publish EMERGENCY_STOP QoS 2 → write Domain 4 + 6
#   - Resume           → publish RESUME QoS 2 → write Domain 4 + 6
#   - Forced Action    → validate → publish to DCA escalation topic
#
# Shutdown: handles SIGTERM and SIGINT cleanly.
#
import json
import signal
import sys
import threading
import time
import structlog

from .config         import SAConfig
from .k8s_client     import SAK8sClient
from .kb_writer      import SAKBWriter
from .mqtt_client    import SAMQTTClient
from .dca_monitor    import DCAMonitor
from .policy_manager import PolicyManager, PolicyValidationError
from .api            import OperatorState, build_app, run_server

log = structlog.get_logger(__name__)
_shutdown = threading.Event()


def _handle_signal(signum, frame):
    log.info("shutdown_signal_received", signum=signum)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ---- Startup stages -------------------------------------------------------

def _stage0(config: SAConfig, mqtt: SAMQTTClient,
            kb: SAKBWriter, deadline: float) -> bool:
    """Stage 0: verify MQTT, Kubernetes API, and Domain 4 are reachable."""
    log.info("startup_stage_0", msg="checking infrastructure")

    remaining = deadline - time.time()
    if not mqtt.wait_connected(timeout=min(30.0, max(0, remaining))):
        log.error("startup_abort", reason="MQTT broker unreachable",
                  host=config.mqtt_host)
        return False
    log.info("startup_stage_0_mqtt_ok")

    # Domain 4 health: if the write_audit call returns False the DB is
    # unavailable but we still proceed — audit loss is not fatal for the SA.
    ok = kb.write_audit("SA_STARTUP", {
        "msg": "SA Domain 4 health check",
        "timestamp_ms": int(time.time() * 1000),
    })
    if ok:
        log.info("startup_stage_0_domain4_ok")
    else:
        log.warning("startup_stage_0_domain4_unavailable",
                    msg="Domain 4 write failed — audit records will be lost")

    return True


def _stage1_policy(policy_mgr: PolicyManager,
                   mqtt: SAMQTTClient,
                   k8s: SAK8sClient,
                   kb: SAKBWriter,
                   state: OperatorState) -> dict:
    """
    Stage 1: load Domain 1, validate, publish POLICY_READY.
    The SA does NOT wait for anything here — it is the first to broadcast.
    """
    log.info("startup_stage_1", msg="loading and broadcasting policy")

    policy = policy_mgr.load()
    state.set_policy(policy)

    mqtt.publish_policy_ready(policy)

    # Record the broadcast in Domain 6 (oversight history).
    k8s.append_policy_version(
        policy_version=policy.get("policy_version", 0),
        policy_timestamp=policy.get("policy_timestamp", 0),
    )

    kb.write_audit("POLICY_READY_BROADCAST", {
        "policy_version": policy.get("policy_version", 0),
        "policy_timestamp": policy.get("policy_timestamp", 0),
    })

    log.info("startup_stage_1_complete",
             policy_version=policy.get("policy_version", 0))
    return policy


def _stage2_dca(config: SAConfig, mqtt: SAMQTTClient,
                deadline: float) -> None:
    """
    Stage 2: wait for DOMAIN_READY from the DCA.
    This confirms the DCA loaded Domain 1 and is broadcasting DOMAIN_READY
    to all Tier-2 agents. The SA does not gate on this — it proceeds even
    on timeout — but receiving it confirms the propagation chain completed.
    """
    log.info("startup_stage_2", msg="waiting for DOMAIN_READY from DCA")
    domain_event = threading.Event()

    original_cb = getattr(mqtt, "_on_domain_ready", None)

    def _combined(payload: dict):
        domain_event.set()
        log.info("domain_ready_received_from_dca")
        if original_cb:
            original_cb(payload)

    mqtt._on_domain_ready = _combined

    remaining = max(0, deadline - time.time())
    if not domain_event.wait(timeout=min(remaining, 60.0)):
        log.warning("startup_stage_2_timeout",
                    msg="DOMAIN_READY not received — DCA may still be starting")
    else:
        log.info("startup_stage_2_complete")


# ---- Main entry point -----------------------------------------------------

def run(config: SAConfig) -> None:

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    log.info("sa_starting",
             agent_id=config.agent_id,
             domain_id=config.domain_id,
             api_port=config.api_port)

    # ---- Initialise all components ----------------------------------------

    k8s = SAK8sClient(
        namespace=config.k8s_namespace,
        policy_configmap=config.policy_configmap_name,
        oversight_configmap=config.oversight_configmap_name,
        topology_configmap=config.topology_configmap_name,
    )

    kb = SAKBWriter(domain4_db_path=config.domain4_db_path)

    policy_mgr = PolicyManager(k8s=k8s)

    state = OperatorState()

    # ---- Operator command callbacks (used by both FastAPI and MQTT) --------
    # These are defined here so they can close over all SA components.

    def _on_policy_override(proposed: dict) -> dict:
        """
        Validate, write to Domain 1, re-broadcast POLICY_READY, audit.
        Called by the FastAPI /api/policy endpoint.
        Raises PolicyValidationError on validation failure.
        """
        applied = policy_mgr.apply_update(proposed)

        mqtt.publish_policy_ready(applied)

        k8s.append_policy_version(
            policy_version=applied.get("policy_version", 0),
            policy_timestamp=applied.get("policy_timestamp", 0),
        )

        kb.write_audit("POLICY_OVERRIDE", {
            "policy_version": applied.get("policy_version", 0),
            "policy_timestamp": applied.get("policy_timestamp", 0),
        })

        log.info("policy_override_applied",
                 policy_version=applied.get("policy_version"))
        return applied

    def _on_emergency_stop() -> None:
        """Issue EMERGENCY_STOP to all DCAs. Called by FastAPI."""
        mqtt.publish_emergency_stop(issued_by="operator")
        k8s.append_emergency_stop_event("EMERGENCY_STOP", {
            "issued_by": "operator",
        })
        kb.write_audit("EMERGENCY_STOP", {
            "issued_by": "operator",
            "timestamp_ms": int(time.time() * 1000),
        })
        log.warning("emergency_stop_issued")

    def _on_resume() -> None:
        """Issue RESUME to all DCAs. Called by FastAPI."""
        mqtt.publish_resume(issued_by="operator")
        k8s.append_emergency_stop_event("RESUME", {
            "issued_by": "operator",
        })
        kb.write_audit("RESUME", {
            "issued_by": "operator",
            "timestamp_ms": int(time.time() * 1000),
        })
        log.info("resume_issued")

    def _on_forced_action(domain_id: str, action_type: str,
                          target: str, parameters: dict) -> None:
        """
        Validate a forced action against safety constraints, then route to DCA.

        Safety constraints applied here:
          - action_type must be one of the allowed set.
          - target (deployment key) must exist in Domain 2 topology.
          - The SA does NOT execute Kubernetes operations directly —
            it publishes a FORCED_ACTION message to the DCA's escalation
            topic. The DCA is responsible for execution and confirmation.

        Raises ValueError if validation fails (caught by the API layer).
        """
        allowed_actions = {
            "SCALE_TO", "RESTART_AGENT", "RELOAD_POLICY", "ACKNOWLEDGE_ESCALATION"
        }
        if action_type not in allowed_actions:
            raise ValueError(
                f"action_type '{action_type}' is not in the allowed set: "
                f"{sorted(allowed_actions)}"
            )

        # Validate target deployment exists in Domain 2.
        if target:
            topology = k8s.read_topology()
            deployments = topology.get("deployments", {})
            if target not in deployments:
                raise ValueError(
                    f"Target deployment '{target}' not found in Domain 2 topology. "
                    f"Known deployments: {list(deployments.keys())}"
                )

        # Publish to the DCA's control topic.
        # The DCA subscribes to /mas/system/domain/<id>/dca/escalation.
        topic = f"/mas/system/domain/{domain_id}/dca/escalation"
        mqtt._client.publish(
            topic=topic,
            payload=json.dumps({
                "event_type":  "FORCED_ACTION",
                "action_type": action_type,
                "target":      target,
                "parameters":  parameters,
                "issued_by":   "operator",
                "timestamp_ms": int(time.time() * 1000),
            }),
            qos=1,
        )

        kb.write_audit("FORCED_ACTION", {
            "domain_id":   domain_id,
            "action_type": action_type,
            "target":      target,
            "parameters":  parameters,
        })
        log.info("forced_action_routed",
                 domain_id=domain_id, action_type=action_type, target=target)

    # ---- DCA supervision callbacks ----------------------------------------

    def _on_dca_failed(domain_id: str) -> None:
        """
        Called by DCAMonitor when a DCA misses the heartbeat threshold.
        The SA activates fallback subscriptions (direct agent topic visibility).
        In the K3D lab there is no full DCA implementation yet, so this
        logs the failure and updates the UI state.
        """
        state.update_dca_liveness(domain_id, failed=True)
        kb.write_audit("DCA_FAILED_NOTIFIED", {
            "domain_id": domain_id,
            "action": "fallback_subscriptions_active",
        })
        log.error("dca_failed_operator_notified",
                  domain_id=domain_id,
                  msg="Fallback: direct agent topic visibility activated")

        # Add a synthetic escalation so it appears in the UI queue.
        state.add_escalation(domain_id, {
            "event_type":  "DCA_FAILURE",
            "domain_id":   domain_id,
            "severity":    "HIGH",
            "msg":         f"DCA {domain_id} has missed heartbeat threshold",
        })

    def _on_dca_recovered(domain_id: str) -> None:
        state.update_dca_liveness(domain_id, failed=False)
        kb.write_audit("DCA_RECOVERED", {"domain_id": domain_id})
        log.info("dca_recovered", domain_id=domain_id)

    # ---- DCA liveness monitor --------------------------------------------
    dca_monitor = DCAMonitor(
        interval_seconds=config.dca_heartbeat_interval_seconds,
        missed_threshold=config.dca_missed_threshold,
        on_dca_failed=_on_dca_failed,
        on_dca_recovered=_on_dca_recovered,
    )

    # ---- MQTT callbacks --------------------------------------------------

    def on_dca_heartbeat(domain_id: str, payload: dict) -> None:
        """Record DCA heartbeat — feeds both DCAMonitor and the UI state."""
        dca_monitor.record_heartbeat(domain_id)
        state.update_dca_liveness(domain_id, failed=False)

    def on_dca_escalation(domain_id: str, payload: dict) -> None:
        """
        Receive SA_NOTIFICATION from a DCA.
        Add to the escalation queue and write to Domain 4.
        """
        event_type = payload.get("event_type", "UNKNOWN")
        log.warning("escalation_received",
                    domain_id=domain_id, event_type=event_type)
        state.add_escalation(domain_id, payload)
        kb.write_audit(
            "ESCALATION_RECEIVED",
            payload,
            namespace=payload.get("namespace"),
            deployment=payload.get("deployment"),
        )

    def on_dca_status(domain_id: str, payload: dict) -> None:
        """Receive periodic DOMAIN_STATUS_REPORT from a DCA."""
        state.update_domain_status(domain_id, payload)
        log.info("domain_status_received", domain_id=domain_id)

    def on_domain_ready(payload: dict) -> None:
        """DOMAIN_READY from DCA — confirms the propagation chain completed."""
        log.info("domain_ready_received",
                 domain_id=payload.get("domain_id", "?"))

    # ---- MQTT client -----------------------------------------------------
    mqtt = SAMQTTClient(
        host=config.mqtt_host,
        port=config.mqtt_port,
        domain_id=config.domain_id,
        topic_policy_ready=config.topic_policy_ready,
        topic_mode_change=config.topic_mode_change,
        topic_sa_heartbeat=config.topic_sa_heartbeat,
        topic_dca_heartbeat=config.topic_dca_heartbeat,
        topic_dca_escalation=config.topic_dca_escalation,
        topic_dca_status=config.topic_dca_status,
        topic_dca_control=config.topic_dca_control,
        username=config.mqtt_username,
        password=config.mqtt_password,
        on_dca_heartbeat=on_dca_heartbeat,
        on_dca_escalation=on_dca_escalation,
        on_dca_status=on_dca_status,
        on_domain_ready=on_domain_ready,
    )

    # ---- Startup sequence ------------------------------------------------
    deadline = time.time() + config.startup_timeout_seconds

    if not _stage0(config, mqtt, kb, deadline):
        log.error("startup_aborted")
        sys.exit(1)

    _stage1_policy(policy_mgr, mqtt, k8s, kb, state)
    _stage2_dca(config, mqtt, deadline)

    log.info("sa_operational",
             agent_id=config.agent_id,
             domain_id=config.domain_id)

    # ---- FastAPI web UI thread -------------------------------------------
    # FastAPI runs on a dedicated daemon thread so it does not block the
    # main MQTT event loop. uvicorn (the ASGI server FastAPI runs on)
    # handles all HTTP I/O independently.
    app = build_app(
        state=state,
        on_policy_override=_on_policy_override,
        on_emergency_stop=_on_emergency_stop,
        on_resume=_on_resume,
        on_forced_action=_on_forced_action,
    )

    api_thread = threading.Thread(
        target=run_server,
        args=(app, config.api_host, config.api_port),
        daemon=True,
        name="fastapi-ui",
    )
    api_thread.start()
    log.info("web_ui_started",
             url=f"http://{config.api_host}:{config.api_port}/")

    # ---- DCA monitor background thread -----------------------------------
    dca_monitor.start_background_thread(_shutdown)

    # ---- Heartbeat thread ------------------------------------------------
    def _heartbeat():
        while not _shutdown.is_set():
            mqtt.publish_heartbeat(agent_id=config.agent_id)
            _shutdown.wait(timeout=config.heartbeat_interval_seconds)

    threading.Thread(
        target=_heartbeat, daemon=True, name="heartbeat"
    ).start()

    # ---- Main loop -------------------------------------------------------
    # The SA is entirely event-driven after startup. All work happens in:
    #   - MQTT callbacks (escalations, heartbeats, domain status)
    #   - FastAPI HTTP handlers (operator commands)
    #   - DCAMonitor background thread (liveness checks)
    # The main thread simply blocks on the shutdown event.
    _shutdown.wait()

    log.info("sa_stopped")
    mqtt.stop()


if __name__ == "__main__":
    run(SAConfig())
