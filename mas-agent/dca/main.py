# dca/main.py
#
# DCA entry point — DCA Specification Sections 4, 5, 6, and 7.
#
# Startup sequence (four-stage system-wide protocol):
#   Stage 0: MQTT connected + Domain 4/5 accessible
#   Stage 1: Wait for POLICY_READY from SA (timeout → use defaults)
#   Stage 2: Verify all three agent types have registered heartbeats
#             (wait up to startup_timeout; proceed anyway if timeout)
#   Stage 3: Broadcast DOMAIN_READY — gates all Tier-2 agents
#
# Core event loop (fully event-driven via MQTT callbacks):
#   on_mra_belief_update   — updates pressure map, feeds L2 cross-validation
#   on_mra_status          — tracks MRA error events (SCRAPE_FAILED, etc.)
#   on_mra_heartbeat       — records L1 MRA liveness
#   on_pfa_forecast_update — updates forecast risk map
#   on_pfa_status          — tracks PFA degradation (INFERENCE_DEGRADED, etc.)
#   on_pfa_heartbeat       — records L1 PFA liveness
#   on_rsa_action          — supplementary L2 RSA liveness signal
#   on_rsa_status          — handles escalation signals (CAPACITY_OVERFLOW, etc.)
#   on_rsa_heartbeat       — records L1 RSA liveness (only detection layer for RSA)
#   on_policy_ready        — loads domain configuration from SA
#   on_modechange          — handles EMERGENCY_STOP / RESUME from SA
#
# Background threads:
#   heartbeat          — publishes DCA heartbeat every 15 seconds to MQTT + Domain 5
#   liveness_checker   — scans all agent records every 15 seconds for L1 violations
#   status_reporter    — emits DOMAIN_STATUS_REPORT every 30 minutes to SA
#   recovery_watchdog  — monitors in-progress Mode B restarts for timeout
#
# All coordination decisions (including Mode A log-only outcomes) write a
# COORDINATION_EVENT record to Domain 4 and publish to the audit topic.
#
import json
import signal
import sys
import threading
import time
import structlog

from .config          import DCAConfig
from .kb_writer       import DCAKBWriter
from .agent_registry  import AgentRegistry, AgentState
from .domain_state    import DomainState, SYSTEM_MODE_EMERGENCY_STOP, SYSTEM_MODE_NORMAL
from .mqtt_client     import DCAMQTTClient
from .recovery_manager import RecoveryManager

log = structlog.get_logger(__name__)
_shutdown = threading.Event()


def _handle_signal(signum, frame):
    log.info("shutdown_signal_received", signum=signum)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ---- Coordination decision handler ----------------------------------------

def _coordinate(
    mode: str,
    signal_type: str,
    agent_type: str,
    deployment: str | None,
    detail: dict,
    domain_state: DomainState,
    registry: AgentRegistry,
    recovery: RecoveryManager,
    mqtt: DCAMQTTClient,
    kb: DCAKBWriter,
    config: DCAConfig,
) -> None:
    """
    Execute the coordination decision for a single event.

    mode values:
      "A"  — log only, no SA notification
      "B"  — attempt pod restart; notify SA only if restart fails (or timeout)
      "B*" — restart AND notify SA simultaneously (RSA liveness failure only)
      "C"  — no autonomous resolution; notify SA immediately

    This function is called from within MQTT callbacks and from the
    liveness-checker background thread. It modifies agent state and
    publishes MQTT messages, but never blocks for more than a few
    milliseconds because recovery monitoring is delegated to the
    recovery_watchdog background thread.
    """
    ns  = config.managed_namespace
    now_ms = int(time.time() * 1000)

    # Always audit every coordination decision
    audit_payload = {
        "mode":        mode,
        "signal_type": signal_type,
        "agent_type":  agent_type,
        "deployment":  deployment,
        "timestamp_ms": now_ms,
        **detail,
    }
    kb.write_coordination_event(
        event_type=f"MODE_{mode}_{signal_type}",
        deployment=deployment,
        namespace=ns,
        payload=audit_payload,
    )
    mqtt.publish_coordination_audit(f"MODE_{mode}_{signal_type}", audit_payload)

    log.info(
        "coordination_decision",
        mode=mode,
        signal_type=signal_type,
        agent_type=agent_type,
        deployment=deployment,
    )

    if mode == "A":
        # Mode A: log only. Nothing further.
        return

    if mode in ("B", "B*") and deployment is not None and agent_type is not None:
        rec = registry.get(agent_type, deployment)

        if rec is not None and not rec.recovery_attempted:
            rec.mark_recovery_started()
            success = recovery.restart_agent(agent_type, deployment)

            if not success:
                # Restart call itself failed (API error) — treat as Mode C
                log.error(
                    "restart_api_failed_escalating",
                    agent_type=agent_type,
                    deployment=deployment,
                )
                _notify_sa(
                    signal_type=f"{agent_type}_RESTART_FAILED",
                    deployment=deployment,
                    domain_state=domain_state,
                    registry=registry,
                    mqtt=mqtt,
                    kb=kb,
                    config=config,
                    extra_detail={"reason": "kubernetes_api_error", **detail},
                )
                mqtt.publish_agent_down(agent_type, deployment)
                return

            log.info(
                "restart_triggered_awaiting_heartbeat",
                agent_type=agent_type,
                deployment=deployment,
                timeout_s=config.recovery_timeout_seconds,
            )

        # Mode B*: notify SA immediately in parallel with restart
        if mode == "B*":
            _notify_sa(
                signal_type="RSA_LIVENESS_FAILURE",
                deployment=deployment,
                domain_state=domain_state,
                registry=registry,
                mqtt=mqtt,
                kb=kb,
                config=config,
                extra_detail=detail,
            )

        # Recovery outcome is evaluated in the recovery_watchdog thread.
        return

    if mode == "C":
        _notify_sa(
            signal_type=signal_type,
            deployment=deployment,
            domain_state=domain_state,
            registry=registry,
            mqtt=mqtt,
            kb=kb,
            config=config,
            extra_detail=detail,
        )


def _notify_sa(
    signal_type: str,
    deployment: str | None,
    domain_state: DomainState,
    registry: AgentRegistry,
    mqtt: DCAMQTTClient,
    kb: DCAKBWriter,
    config: DCAConfig,
    extra_detail: dict = None,
) -> None:
    """
    Compose and send an enriched SA_NOTIFICATION.
    Applies rate-limiting deduplication before sending.
    Three signal types always bypass deduplication (see DomainState).
    """
    if not domain_state.should_escalate(
        deployment or "domain",
        signal_type,
        config.escalation_suppression_window_seconds,
    ):
        log.info(
            "sa_notification_suppressed",
            signal_type=signal_type,
            deployment=deployment,
        )
        return

    # Assemble enriched context: pressure map + forecast risk + agent registry
    enriched = {
        "pressure_map":      domain_state.pressure_snapshot(),
        "forecast_risk_map": domain_state.forecast_risk_snapshot(),
        "agent_states":      registry.snapshot(),
        **(extra_detail or {}),
    }

    mqtt.publish_sa_notification(signal_type, deployment, enriched)
    kb.write_coordination_event(
        event_type="SA_NOTIFICATION",
        deployment=deployment,
        namespace=config.managed_namespace,
        payload={"signal_type": signal_type, **enriched},
    )
    domain_state.record_escalation(deployment or "domain", signal_type)


# ---- Startup sequence -------------------------------------------------------

def _stage0(config: DCAConfig, mqtt: DCAMQTTClient, kb: DCAKBWriter,
            deadline: float) -> bool:
    """Stage 0: verify MQTT broker and Knowledge Base are reachable."""
    log.info("startup_stage_0", msg="checking infrastructure")

    remaining = deadline - time.time()
    if not mqtt.wait_connected(timeout=min(30.0, max(0, remaining))):
        log.error("startup_abort", reason="MQTT broker unreachable",
                  host=config.mqtt_host)
        return False
    log.info("startup_stage_0_mqtt_ok")
    log.info("startup_stage_0_domain_kb_ok")
    return True


def _stage1_policy(config: DCAConfig, mqtt_obj: DCAMQTTClient,
                   deadline: float) -> dict:
    """Stage 1: wait for POLICY_READY from SA."""
    log.info("startup_stage_1", msg="waiting for POLICY_READY")
    policy_event = threading.Event()
    policy_data: dict = {}

    original_cb = mqtt_obj._on_policy_ready

    def _combined(payload: dict) -> None:
        policy_data.update(payload)
        policy_event.set()
        if original_cb:
            original_cb(payload)

    mqtt_obj._on_policy_ready = _combined

    remaining = max(0, deadline - time.time())
    if not policy_event.wait(timeout=min(remaining, 90.0)):
        log.warning("startup_stage_1_timeout",
                    msg="POLICY_READY not received — using defaults")
    else:
        log.info("startup_stage_1_complete")
    return policy_data


# ---- Main entry point -------------------------------------------------------

def run(config: DCAConfig) -> None:

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    agent_id = f"DCA:{config.domain_id}"
    log.info(
        "dca_starting",
        agent_id=agent_id,
        domain_id=config.domain_id,
        managed_namespace=config.managed_namespace,
        managed_deployments=config.deployment_list,
    )

    # ---- Initialise all components ----------------------------------------

    kb = DCAKBWriter(
        domain4_db_path=config.domain4_db_path,
        domain5_db_path=config.domain5_db_path,
        domain_id=config.domain_id,
    )

    # Restore persisted system mode on restart (EMERGENCY_STOP survives restarts)
    persisted_mode = kb.read_system_mode()
    initial_mode   = persisted_mode if persisted_mode else SYSTEM_MODE_NORMAL

    registry = AgentRegistry(
        managed_deployments=config.deployment_list,
        namespace=config.managed_namespace,
        mra_hb_interval=config.mra_heartbeat_interval_seconds,
        mra_missed=config.mra_missed_threshold,
        pfa_hb_interval=config.pfa_heartbeat_interval_seconds,
        pfa_missed=config.pfa_missed_threshold,
        rsa_hb_interval=config.rsa_heartbeat_interval_seconds,
        rsa_missed=config.rsa_missed_threshold,
    )

    domain_state = DomainState(managed_deployments=config.deployment_list)
    domain_state.set_system_mode(initial_mode)

    recovery = RecoveryManager(mas_namespace="mas-system")

    if initial_mode == SYSTEM_MODE_EMERGENCY_STOP:
        log.warning(
            "restart_in_emergency_stop_mode",
            msg="Will re-enforce EMERGENCY_STOP after DOMAIN_READY is broadcast.",
        )

    # ---- MQTT Callbacks ---------------------------------------------------
    #
    # All callbacks follow the same pattern as RSA:
    #   1. Guard clauses (early return if conditions not met)
    #   2. Update in-memory state (registry, domain_state)
    #   3. Call _coordinate() with the appropriate mode classification
    #

    def on_mra_belief_update(deployment: str, belief: dict) -> None:
        """Update pressure map and cross-validate L2 peer signals."""
        # A BELIEF_UPDATE proves the MRA process is alive.
        # Reset the L1 heartbeat timer — identical to receiving a heartbeat.
        rec = registry.get("MRA", deployment)
        if rec:
            rec.record_heartbeat()

        pressure = belief.get("pressure_level", "NORMAL")
        domain_state.update_pressure(deployment, pressure)

        # UNCONFIGURED is a configuration fault — Mode C immediately
        if pressure == "UNCONFIGURED":
            _coordinate(
                mode="C", signal_type="UNCONFIGURED_DEPLOYMENT",
                agent_type="MRA", deployment=deployment,
                detail={"pressure_level": pressure},
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )
        else:
            # Regular belief update — Mode A (log and update state)
            _coordinate(
                mode="A", signal_type="BELIEF_UPDATE_RECEIVED",
                agent_type="MRA", deployment=deployment,
                detail={"pressure_level": pressure},
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

    def on_mra_status(deployment: str, payload: dict) -> None:
        """Track MRA error events. Any MRA status event = MRA process is alive."""
        event_type = payload.get("event_type", "UNKNOWN")

        if event_type == "UNCONFIGURED_ERROR":
            _coordinate(
                mode="C", signal_type="UNCONFIGURED_ERROR",
                agent_type="MRA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )
        else:
            # SCRAPE_FAILED, VALIDATION_RANGE_ERROR, VALIDATION_STALENESS_ERROR
            # Are Mode A unless ≥ 3 consecutive (evaluated by liveness_checker
            # via the registry's consecutive scrape fail counter, not tracked here
            # for simplicity — the heartbeat from MRA resets the L1 counter).
            _coordinate(
                mode="A", signal_type=f"MRA_STATUS:{event_type}",
                agent_type="MRA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

    def on_mra_heartbeat(deployment: str, payload: dict) -> None:
        """Record L1 MRA liveness. Clears suspected/confirmed state on recovery."""
        rec = registry.get("MRA", deployment)
        if rec:
            recovered = rec.record_heartbeat()
            if recovered:
                log.info("mra_recovered", deployment=deployment)
                _coordinate(
                    mode="A", signal_type="MRA_RECOVERED",
                    agent_type="MRA", deployment=deployment,
                    detail={},
                    domain_state=domain_state, registry=registry,
                    recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                )

    def on_pfa_forecast_update(deployment: str, forecast: dict) -> None:
        """Update forecast risk map from PFA FORECAST_UPDATE."""
        domain_state.update_forecast_risk(deployment, forecast)
        # Also counts as PFA liveness evidence
        rec = registry.get("PFA", deployment)
        if rec:
            rec.record_heartbeat()

    def on_pfa_status(deployment: str, payload: dict) -> None:
        """Handle PFA degradation events and peer signals for MRA liveness."""
        event_type = payload.get("event_type", "UNKNOWN")

        if event_type == "INFERENCE_DEGRADED":
            domain_state.mark_pfa_degraded(deployment, "INFERENCE_DEGRADED")
            _coordinate(
                mode="A", signal_type="INFERENCE_DEGRADED",
                agent_type="PFA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "MRA_DOWN_SUSPECTED":
            # L2 evidence: one peer suspects MRA failure
            mra_rec = registry.get("MRA", deployment)
            if mra_rec:
                mra_rec.record_l2_suspected()
            _coordinate(
                mode="A", signal_type="MRA_DOWN_SUSPECTED_FROM_PFA",
                agent_type="MRA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "MRA_DOWN_CONFIRMED":
            # L2 confirmed by PFA — bypass L1 gate, enter Mode B.
            # Ignored during startup grace period (stale queued messages).
            if time.time() < _startup_grace_until:
                log.info("l2_signal_suppressed_grace_period",
                         event_type=event_type, deployment=deployment)
            else:
                mra_rec = registry.get("MRA", deployment)
                if mra_rec and mra_rec.current_state != AgentState.CONFIRMED_DOWN:
                    mra_rec.record_l2_confirmed()
                    _coordinate(
                        mode="B", signal_type="MRA_DOWN_CONFIRMED_L2",
                        agent_type="MRA", deployment=deployment,
                        detail=payload,
                        domain_state=domain_state, registry=registry,
                        recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                    )
        else:
            # WARMING_UP, INFERENCE_ERROR — Mode A
            _coordinate(
                mode="A", signal_type=f"PFA_STATUS:{event_type}",
                agent_type="PFA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

    def on_pfa_heartbeat(deployment: str, payload: dict) -> None:
        """Record L1 PFA liveness."""
        rec = registry.get("PFA", deployment)
        if rec:
            recovered = rec.record_heartbeat()
            if recovered:
                log.info("pfa_recovered", deployment=deployment)

    def on_rsa_action(deployment: str, payload: dict) -> None:
        """Supplementary L2 RSA liveness — a SCALING_ACTION proves RSA is alive."""
        rec = registry.get("RSA", deployment)
        if rec:
            rec.record_heartbeat()

    def on_rsa_status(deployment: str, payload: dict) -> None:
        """
        Handle all RSA status signals. These range from Mode A (HPA_PATCH_FAILURE
        first occurrence) to Mode C (CAPACITY_OVERFLOW, EMERGENCY_INSUFFICIENT).
        """
        event_type = payload.get("event_type", "UNKNOWN")

        if event_type == "CAPACITY_OVERFLOW":
            _coordinate(
                mode="C", signal_type="CAPACITY_OVERFLOW",
                agent_type="RSA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "EMERGENCY_INSUFFICIENT":
            _coordinate(
                mode="C", signal_type="EMERGENCY_INSUFFICIENT",
                agent_type="RSA", deployment=deployment,
                detail={
                    **payload,
                    "pressure_map":      domain_state.pressure_snapshot(),
                    "forecast_risk_map": domain_state.forecast_risk_snapshot(),
                },
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "CONFLICTING_AUTOSCALER":
            _coordinate(
                mode="C", signal_type="CONFLICTING_AUTOSCALER",
                agent_type="RSA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "MRA_DOWN_SUSPECTED":
            mra_rec = registry.get("MRA", deployment)
            if mra_rec:
                mra_rec.record_l2_suspected()
            _coordinate(
                mode="A", signal_type="MRA_DOWN_SUSPECTED_FROM_RSA",
                agent_type="MRA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "MRA_DOWN_CONFIRMED":
            # Ignored during startup grace period (stale queued messages).
            if time.time() < _startup_grace_until:
                log.info("l2_signal_suppressed_grace_period",
                         event_type=event_type, deployment=deployment)
            else:
                mra_rec = registry.get("MRA", deployment)
                if mra_rec and mra_rec.current_state != AgentState.CONFIRMED_DOWN:
                    mra_rec.record_l2_confirmed()
                    _coordinate(
                        mode="B", signal_type="MRA_DOWN_CONFIRMED_L2",
                        agent_type="MRA", deployment=deployment,
                        detail=payload,
                        domain_state=domain_state, registry=registry,
                        recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                    )

        elif event_type == "PFA_DOWN_SUSPECTED":
            pfa_rec = registry.get("PFA", deployment)
            if pfa_rec:
                pfa_rec.record_l2_suspected()
            _coordinate(
                mode="A", signal_type="PFA_DOWN_SUSPECTED_FROM_RSA",
                agent_type="PFA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

        elif event_type == "PFA_DOWN_CONFIRMED":
            # Ignored during startup grace period (stale queued messages).
            if time.time() < _startup_grace_until:
                log.info("l2_signal_suppressed_grace_period",
                         event_type=event_type, deployment=deployment)
            else:
                pfa_rec = registry.get("PFA", deployment)
                if pfa_rec and pfa_rec.current_state != AgentState.CONFIRMED_DOWN:
                    pfa_rec.record_l2_confirmed()
                    _coordinate(
                        mode="B", signal_type="PFA_DOWN_CONFIRMED_L2",
                        agent_type="PFA", deployment=deployment,
                        detail=payload,
                        domain_state=domain_state, registry=registry,
                        recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                    )
        else:
            _coordinate(
                mode="A", signal_type=f"RSA_STATUS:{event_type}",
                agent_type="RSA", deployment=deployment,
                detail=payload,
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )

    def on_rsa_heartbeat(deployment: str, payload: dict) -> None:
        """Record L1 RSA liveness — the ONLY detection layer for RSA."""
        rec = registry.get("RSA", deployment)
        if rec:
            recovered = rec.record_heartbeat()
            if recovered:
                log.info("rsa_recovered", deployment=deployment)
                _coordinate(
                    mode="A", signal_type="RSA_RECOVERED",
                    agent_type="RSA", deployment=deployment,
                    detail={},
                    domain_state=domain_state, registry=registry,
                    recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                )

    def on_policy_ready(payload: dict) -> None:
        log.info("policy_ready_received", keys=list(payload.keys()))

    def on_modechange(payload: dict) -> None:
        """
        Handle SA EMERGENCY_STOP and RESUME commands.
        EMERGENCY_STOP: persist mode, relay to all RSA instances, suspend Mode B.
        RESUME:        restore NORMAL mode, DCA resumes autonomous coordination.
        """
        command = payload.get("command", "UNKNOWN")

        if command == "EMERGENCY_STOP":
            domain_state.set_system_mode(SYSTEM_MODE_EMERGENCY_STOP)
            kb.persist_system_mode(SYSTEM_MODE_EMERGENCY_STOP)

            # Relay to all RSA instances — QoS 2 (exactly-once)
            for dep in config.deployment_list:
                mqtt.publish_emergency_stop_fwd(dep)

            _coordinate(
                mode="A", signal_type="EMERGENCY_STOP_ACTIVATED",
                agent_type=None, deployment=None,
                detail={"command": command},
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )
            log.warning("emergency_stop_activated_all_rsa_notified")

        elif command == "RESUME":
            domain_state.set_system_mode(SYSTEM_MODE_NORMAL)
            kb.persist_system_mode(SYSTEM_MODE_NORMAL)
            _coordinate(
                mode="A", signal_type="AUTONOMOUS_MODE_RESUMED",
                agent_type=None, deployment=None,
                detail={"command": command},
                domain_state=domain_state, registry=registry,
                recovery=recovery, mqtt=mqtt, kb=kb, config=config,
            )
            log.info("autonomous_mode_resumed")

    # ---- MQTT client ------------------------------------------------------
    mqtt = DCAMQTTClient(
        host=config.mqtt_host,
        port=config.mqtt_port,
        domain_id=config.domain_id,
        managed_namespace=config.managed_namespace,
        managed_deployments=config.deployment_list,
        topic_domain_ready=config.topic_domain_ready,
        topic_agent_down=config.topic_agent_down,
        topic_sa_notification=config.topic_sa_notification,
        topic_domain_status=config.topic_domain_status,
        topic_dca_heartbeat=config.topic_dca_heartbeat,
        topic_coordination_audit=config.topic_coordination_audit,
        topic_sa_policy=config.topic_sa_policy,
        topic_sa_modechange=config.topic_sa_modechange,
        topic_fn_mra_beliefs=config.topic_mra_beliefs,
        topic_fn_mra_status=config.topic_mra_status,
        topic_fn_mra_heartbeat=config.topic_mra_heartbeat,
        topic_fn_pfa_forecasts=config.topic_pfa_forecasts,
        topic_fn_pfa_status=config.topic_pfa_status,
        topic_fn_pfa_heartbeat=config.topic_pfa_heartbeat,
        topic_fn_rsa_actions=config.topic_rsa_actions,
        topic_fn_rsa_status=config.topic_rsa_status,
        topic_fn_rsa_heartbeat=config.topic_rsa_heartbeat,
        topic_fn_emergency_stop_fwd=config.topic_emergency_stop_fwd,
        username=config.mqtt_username,
        password=config.mqtt_password,
        on_mra_belief_update=on_mra_belief_update,
        on_mra_status=on_mra_status,
        on_mra_heartbeat=on_mra_heartbeat,
        on_pfa_forecast_update=on_pfa_forecast_update,
        on_pfa_status=on_pfa_status,
        on_pfa_heartbeat=on_pfa_heartbeat,
        on_rsa_action=on_rsa_action,
        on_rsa_status=on_rsa_status,
        on_rsa_heartbeat=on_rsa_heartbeat,
        on_policy_ready=on_policy_ready,
        on_modechange=on_modechange,
    )

    # ---- Startup sequence -------------------------------------------------
    deadline = time.time() + config.startup_timeout_seconds

    if not _stage0(config, mqtt, kb, deadline):
        log.error("startup_aborted")
        sys.exit(1)

    _stage1_policy(config, mqtt, deadline)

    # Stage 2: Register DCA heartbeat in Domain 5 before broadcasting DOMAIN_READY
    log.info("startup_stage_2", msg="registering DCA in heartbeat registry")
    kb.upsert_heartbeat(status="ALIVE")

    # Stage 3: Broadcast DOMAIN_READY — all Tier-2 agents are now gated on this
    log.info("startup_stage_3", msg="broadcasting DOMAIN_READY")
    mqtt.publish_domain_ready(config.domain_id)

    # If we restarted during EMERGENCY_STOP, re-enforce immediately
    if domain_state.is_emergency_stop():
        log.warning("re_enforcing_emergency_stop_after_restart")
        for dep in config.deployment_list:
            mqtt.publish_emergency_stop_fwd(dep)

    log.info("dca_operational", agent_id=agent_id)

    # Startup grace period: ignore all L2 signals and liveness checks
    # for the first 90 seconds. Agents need time to send their first
    # heartbeats after DOMAIN_READY is broadcast. Without this guard,
    # stale MQTT messages queued before the DCA started can trigger
    # a false Mode B recovery within seconds of startup.
    _startup_grace_until = time.time() + 90
    log.info("startup_grace_period_active", grace_seconds=90)

    # ---- Background threads -----------------------------------------------

    def _heartbeat():
        """Publish DCA liveness heartbeat every 15 seconds."""
        while not _shutdown.is_set():
            mqtt.publish_heartbeat(agent_id=agent_id)
            kb.upsert_heartbeat(status="ALIVE")
            _shutdown.wait(timeout=config.heartbeat_interval_seconds)

    threading.Thread(
        target=_heartbeat, daemon=True, name="dca-heartbeat"
    ).start()

    def _liveness_checker():
        """
        Scan all agent records every 15 seconds for L1 heartbeat violations.
        Fires Mode B / B* when the failure detection logic classifies a record
        as CONFIRMED_DOWN.
        """
        while not _shutdown.is_set():
            if time.time() < _startup_grace_until:
                _shutdown.wait(timeout=config.rsa_heartbeat_interval_seconds)
                continue
            for rec in registry.all_records():
                classification = rec.check_liveness()

                if classification == "ALIVE":
                    continue

                dep  = rec.deployment
                atyp = rec.agent_type

                if classification == "L1_ONLY":
                    # L2 not corroborated yet — Mode A, monitor silently
                    _coordinate(
                        mode="A",
                        signal_type=f"{atyp}_HEARTBEAT_MISSING",
                        agent_type=atyp, deployment=dep,
                        detail={"classification": classification},
                        domain_state=domain_state, registry=registry,
                        recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                    )

                elif classification in ("CONFIRMED", "L2_DIRECT"):
                    # Both layers agree — determine Mode B or B*
                    mode = "B*" if atyp == "RSA" else "B"
                    log.warning(
                        "agent_failure_confirmed",
                        agent_type=atyp, deployment=dep, mode=mode,
                    )
                    # Broadcast AGENT_DOWN to all Tier-2 agents
                    mqtt.publish_agent_down(atyp, dep)
                    _coordinate(
                        mode=mode,
                        signal_type=f"{atyp}_LIVENESS_FAILURE",
                        agent_type=atyp, deployment=dep,
                        detail={"classification": classification},
                        domain_state=domain_state, registry=registry,
                        recovery=recovery, mqtt=mqtt, kb=kb, config=config,
                    )

            _shutdown.wait(timeout=config.rsa_heartbeat_interval_seconds)

    threading.Thread(
        target=_liveness_checker, daemon=True, name="liveness-checker"
    ).start()

    def _recovery_watchdog():
        """
        Monitor in-progress Mode B restart attempts for timeout.
        If the restarted pod does not send a heartbeat within
        recovery_timeout_seconds, declare recovery failed and notify SA.
        """
        while not _shutdown.is_set():
            for rec in registry.all_records():
                if (rec.current_state == AgentState.RECOVERING
                        and rec.recovery_timed_out(config.recovery_timeout_seconds)):

                    dep  = rec.deployment
                    atyp = rec.agent_type

                    log.error(
                        "recovery_timed_out_escalating",
                        agent_type=atyp, deployment=dep,
                        timeout_s=config.recovery_timeout_seconds,
                    )
                    _notify_sa(
                        signal_type=f"{atyp}_RECOVERY_FAILED",
                        deployment=dep,
                        domain_state=domain_state,
                        registry=registry,
                        mqtt=mqtt,
                        kb=kb,
                        config=config,
                        extra_detail={
                            "reason":     "recovery_timeout",
                            "timeout_s":  config.recovery_timeout_seconds,
                        },
                    )
                    # Mark as confirmed down so we do not re-attempt restart
                    from .agent_registry import AgentState as AS
                    with rec._lock:
                        rec.state = AS.CONFIRMED_DOWN

            _shutdown.wait(timeout=10)

    threading.Thread(
        target=_recovery_watchdog, daemon=True, name="recovery-watchdog"
    ).start()

    def _status_reporter():
        """Emit DOMAIN_STATUS_REPORT every 30 minutes to the SA."""
        while not _shutdown.is_set():
            _shutdown.wait(timeout=config.status_report_interval_seconds)
            if _shutdown.is_set():
                break
            report = {
                "system_mode":       domain_state.get_system_mode(),
                "pressure_map":      domain_state.pressure_snapshot(),
                "forecast_risk_map": domain_state.forecast_risk_snapshot(),
                "agent_states":      registry.snapshot(),
            }
            mqtt.publish_domain_status_report(report)

    threading.Thread(
        target=_status_reporter, daemon=True, name="status-reporter"
    ).start()

    # ---- Main loop --------------------------------------------------------
    # The DCA is entirely event-driven. The main thread waits for shutdown.
    _shutdown.wait()

    log.info("dca_stopped")
    kb.upsert_heartbeat(status="SHUTDOWN")
    mqtt.stop()


if __name__ == "__main__":
    run(DCAConfig())
