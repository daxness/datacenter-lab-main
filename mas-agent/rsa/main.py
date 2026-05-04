# rsa/main.py
#
# RSA entry point — RSA Specification Section 4 (Internal Logic) and
# Section 8 (Startup Sequence).
#
# Startup sequence (system-wide four-stage protocol):
#   Stage 0: MQTT connected + Kubernetes API reachable + Domain 5 accessible
#   Stage 1: Wait for POLICY_READY from SA (timeout → use env var defaults)
#   Stage 2: Wait for DOMAIN_READY from DCA (timeout → proceed anyway)
#   Stage 3: Detect conflicting autoscalers, register in Domain 5 heartbeat
#
# Three execution paths (all event-driven via MQTT callbacks):
#
#   PROACTIVE  — triggered by FORECAST_UPDATE where breach_confidence == "HIGH"
#                on at least one metric. Runs 8-stage sizing pipeline.
#                Suppressed during: cooldown active, MRA down, PFA down,
#                INFERENCE_DEGRADED, EMERGENCY_STOP.
#
#   EMERGENCY  — triggered by 2 consecutive CRITICAL BELIEF_UPDATEs.
#                Bypasses sizing pipeline entirely. Scales to max_replicas.
#                Does NOT start or reset the cooldown timer.
#                Suppressed during: MRA down, EMERGENCY_STOP.
#
#   REACTIVE   — triggered by 10 consecutive LOW BELIEF_UPDATEs (5 minutes).
#                Sizes against current observed utilization, not forecasts.
#                Requires both: sustain condition met AND cooldown inactive.
#                Suppressed during: MRA down, EMERGENCY_STOP.
#
# All deliberation outcomes (SCALE_UP, SCALE_DOWN, DO_NOTHING, COOLDOWN_BLOCKED)
# are written to Domain 4 as auditable records.
#
# Heartbeat: published every 15 seconds to MQTT and Domain 5.
# Shutdown: handles SIGTERM and SIGINT cleanly.
#
import math
import json
import signal
import sys
import threading
import time
import structlog

from .config          import RSAConfig
from .kb_writer       import RSAKBWriter
from .cooldown_manager import CooldownManager
from .sizing_pipeline  import SizingPipeline
from .k8s_client      import K8sClient
from .mqtt_client     import RSAMQTTClient
from .peer_monitor    import MRAPeerMonitor, PFAPeerMonitor

log = structlog.get_logger(__name__)
_shutdown = threading.Event()


def _handle_signal(signum, frame):
    log.info("shutdown_signal_received", signum=signum)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ---- Startup sequence -------------------------------------------------------

def _stage0(config: RSAConfig, mqtt: RSAMQTTClient, k8s: K8sClient,
            kb: RSAKBWriter, deadline: float) -> bool:
    """Stage 0: verify MQTT, Kubernetes API, and Domain 5 are reachable."""
    log.info("startup_stage_0", msg="checking infrastructure")

    remaining = deadline - time.time()
    if not mqtt.wait_connected(timeout=min(30.0, max(0, remaining))):
        log.error("startup_abort", reason="MQTT broker unreachable",
                  host=config.mqtt_host)
        return False
    log.info("startup_stage_0_mqtt_ok")

    try:
        replicas = k8s.get_current_replicas()
        log.info("startup_stage_0_k8s_ok", current_replicas=replicas)
    except Exception as e:
        log.error("startup_abort", reason="Kubernetes API unreachable", error=str(e))
        return False

    # Domain 5 connection is verified inside RSAKBWriter.__init__ —
    # if we reach here the connection was established successfully.
    log.info("startup_stage_0_domain5_ok")
    return True


def _stage1_policy(config: RSAConfig, mqtt: RSAMQTTClient,
                   deadline: float) -> dict:
    """Stage 1: wait for POLICY_READY from SA."""
    log.info("startup_stage_1", msg="waiting for POLICY_READY")
    policy_event = threading.Event()
    policy_data: dict = {}

    original_cb = getattr(mqtt, "_on_policy_ready", None)

    def _combined(payload: dict) -> None:
        policy_data.update(payload)
        policy_event.set()
        log.info("policy_ready_received")
        if original_cb:
            original_cb(payload)

    mqtt._on_policy_ready = _combined

    remaining = max(0, deadline - time.time())
    if not policy_event.wait(timeout=min(remaining, 90.0)):
        log.warning("startup_stage_1_timeout",
                    msg="POLICY_READY not received — using env var defaults")
    else:
        log.info("startup_stage_1_complete")

    return policy_data


def _stage2_dca(config: RSAConfig, mqtt: RSAMQTTClient,
                deadline: float) -> None:
    """Stage 2: wait for DOMAIN_READY from DCA."""
    log.info("startup_stage_2", msg="waiting for DOMAIN_READY")
    domain_event = threading.Event()

    original_cb = getattr(mqtt, "_on_domain_ready", None)

    def _combined(payload: dict) -> None:
        domain_event.set()
        log.info("domain_ready_received")
        if original_cb:
            original_cb(payload)

    mqtt._on_domain_ready = _combined

    remaining = max(0, deadline - time.time())
    if not domain_event.wait(timeout=min(remaining, 30.0)):
        log.warning("startup_stage_2_timeout",
                    msg="DOMAIN_READY not received — proceeding anyway")
    else:
        log.info("startup_stage_2_complete")


# ---- Main entry point -------------------------------------------------------

def run(config: RSAConfig) -> None:

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    agent_id = f"RSA:{config.namespace}/{config.deployment_name}"

    log.info(
        "rsa_starting",
        agent_id=agent_id,
        namespace=config.namespace,
        deployment=config.deployment_name,
        domain=config.domain,
    )

    # ---- Initialise all components ----------------------------------------

    kb = RSAKBWriter(
        domain4_db_path=config.domain4_db_path,
        domain5_db_path=config.domain5_db_path,
        namespace=config.namespace,
        deployment=config.deployment_name,
    )

    cooldown = CooldownManager(
        cooldown_seconds=config.cooldown_seconds,
        kb_writer=kb,
    )

    k8s = K8sClient(
        namespace=config.namespace,
        deployment_name=config.deployment_name,
    )

    pipeline = SizingPipeline(
        min_replicas=config.min_replicas,
        max_replicas=config.max_replicas,
        scale_up_delta_threshold=config.scale_up_delta_threshold,
        confidence_margin_min=config.confidence_margin_min,
        confidence_margin_max=config.confidence_margin_max,
        cpu_request_per_replica=config.cpu_request_per_replica,
        memory_request_per_replica=config.memory_request_per_replica,
    )

    # ---- Operational state ------------------------------------------------
    # These flags gate which paths are active. Modified by MQTT callbacks.

    # emergency_stop: set by EMERGENCY_STOP_FWD from DCA (relayed from SA).
    # Suspends ALL Kubernetes patching.
    emergency_stop_active  = threading.Event()

    # mra_down: set when MRA_DOWN_CONFIRMED is received from DCA,
    # or when the RSA's own L2 peer monitor confirms MRA absence.
    # Suspends emergency path, reactive path, and proactive path.
    mra_down               = threading.Event()

    # proactive_suspended: set by INFERENCE_DEGRADED from PFA, or when
    # PFA_DOWN_CONFIRMED is raised. Only suspends the proactive path.
    proactive_suspended    = threading.Event()

    # Per-path state (not shared — mutated only inside their respective callbacks)
    _consecutive_critical  = 0     # emergency path counter
    _consecutive_low       = 0     # reactive path counter
    _low_deferred          = False # reactive: sustain met but cooldown blocked it

    # ---- MQTT callbacks ---------------------------------------------------

    def on_belief_update(belief: dict) -> None:
        nonlocal _consecutive_critical, _consecutive_low, _low_deferred

        if emergency_stop_active.is_set():
            return
        if mra_down.is_set():
            return

        mra_monitor.record_message()
        pressure = belief.get("pressure_level", "NORMAL")

        # ================================================================
        # EMERGENCY PATH — CRITICAL pressure, 2-sample confirmation
        # ================================================================
        if pressure == "CRITICAL":
            _consecutive_critical += 1
            _consecutive_low       = 0
            _low_deferred          = False

            log.warning(
                "critical_pressure_detected",
                consecutive=_consecutive_critical,
                required=config.critical_confirm_count,
            )

            if _consecutive_critical >= config.critical_confirm_count:
                current = k8s.get_current_replicas()

                if current >= config.max_replicas:
                    # Already at max — check if CRITICAL persists after scale
                    if _consecutive_critical >= config.critical_confirm_count * 2:
                        log.error("emergency_insufficient",
                                  max_replicas=config.max_replicas)
                        mqtt.publish_status("EMERGENCY_INSUFFICIENT", {
                            "namespace":    config.namespace,
                            "deployment":   config.deployment_name,
                            "max_replicas": config.max_replicas,
                        })
                        kb.write_audit("EMERGENCY_INSUFFICIENT", {
                            "namespace":     config.namespace,
                            "deployment":    config.deployment_name,
                            "max_replicas":  config.max_replicas,
                            "consecutive":   _consecutive_critical,
                        })
                else:
                    log.error(
                        "emergency_scale",
                        target=config.max_replicas,
                        current=current,
                    )
                    # Emergency path: cooldown is intentionally NOT started
                    k8s.patch_replicas(config.max_replicas)
                    mqtt.publish_scaling_action(
                        trigger="EMERGENCY",
                        action="SCALE_UP",
                        target_replicas=config.max_replicas,
                        current_replicas=current,
                        detail={"pressure_level": "CRITICAL"},
                    )
                    kb.write_audit("SCALING_ACTION", {
                        "trigger":         "EMERGENCY",
                        "action":          "SCALE_UP",
                        "target_replicas": config.max_replicas,
                        "current_replicas": current,
                        "pressure_level":  "CRITICAL",
                        "cooldown_started": False,
                    })

        else:
            _consecutive_critical = 0

        # ================================================================
        # REACTIVE SCALE-DOWN PATH — LOW pressure, 10-sample sustain
        # ================================================================
        if pressure == "LOW":
            _consecutive_low += 1

            log.debug(
                "low_pressure_sustained",
                consecutive=_consecutive_low,
                required=config.low_pressure_sustain_count,
            )

            if _consecutive_low >= config.low_pressure_sustain_count:

                if cooldown.is_active():
                    if not _low_deferred:
                        _low_deferred = True
                        log.info(
                            "reactive_deferred_cooldown",
                            remaining_s=round(cooldown.remaining_seconds(), 1),
                        )
                    return

                _low_deferred = False

                # Reactive sizing: current observed utilization, no forecast
                cm = belief.get("context_metrics", {})
                fm = belief.get("forecast_metrics", {})

                cpu_usage   = fm.get("cpu_usage_millicores", 0.0) or 0.0
                mem_usage   = fm.get("memory_usage_MiB",     0.0) or 0.0
                cpu_request = cm.get("cpu_requests",  config.cpu_request_per_replica) or config.cpu_request_per_replica
                mem_request = cm.get("memory_requests", config.memory_request_per_replica) or config.memory_request_per_replica

                current = k8s.get_current_replicas()

                # Per-pod resource cost re-read at each cycle
                cpu_per_replica = cpu_request / max(current, 1)
                mem_per_replica = mem_request / max(current, 1)

                # Minimum replicas to serve current usage at target utilization
                # Formula: ceil(usage / (per_replica × target_utilization))
                cpu_reps = math.ceil(
                    cpu_usage / max(
                        cpu_per_replica * config.scale_down_target_utilization, 1.0
                    )
                )
                mem_reps = math.ceil(
                    mem_usage / max(
                        mem_per_replica * config.scale_down_target_utilization, 1.0
                    )
                )
                raw_target = max(cpu_reps, mem_reps)

                if raw_target >= current:
                    log.info(
                        "reactive_do_nothing",
                        raw_target=raw_target,
                        current=current,
                    )
                    kb.write_audit("SCALING_ACTION", {
                        "trigger":         "REACTIVE_SCALE_DOWN",
                        "action":          "DO_NOTHING",
                        "current_replicas": current,
                        "raw_target":      raw_target,
                        "reason":          "target_not_below_current",
                    })
                    _consecutive_low = 0
                    return

                target = max(config.min_replicas, min(config.max_replicas, raw_target))

                log.info(
                    "reactive_scale_down",
                    current=current,
                    target=target,
                    cpu_reps=cpu_reps,
                    mem_reps=mem_reps,
                )

                # Persist cooldown BEFORE patch — mandatory ordering
                cooldown.start(action_type="scale_in")
                k8s.patch_replicas(target)

                mqtt.publish_scaling_action(
                    trigger="REACTIVE_SCALE_DOWN",
                    action="SCALE_DOWN",
                    target_replicas=target,
                    current_replicas=current,
                    detail={
                        "cpu_replica_estimate": cpu_reps,
                        "mem_replica_estimate": mem_reps,
                    },
                )
                kb.write_audit("SCALING_ACTION", {
                    "trigger":             "REACTIVE_SCALE_DOWN",
                    "action":             "SCALE_DOWN",
                    "current_replicas":   current,
                    "target_replicas":    target,
                    "cpu_replica_estimate": cpu_reps,
                    "mem_replica_estimate": mem_reps,
                    "cooldown_started":   True,
                })
                _consecutive_low = 0

        else:
            if _consecutive_low > 0:
                log.debug("low_streak_broken", was=_consecutive_low)
            _consecutive_low  = 0
            _low_deferred     = False

    def on_mra_status() -> None:
        """Any MRA status event resets the MRA missed-cycle counter."""
        mra_monitor.record_message()

    def on_forecast_update(forecast: dict) -> None:
        """
        PROACTIVE PATH — triggered by PFA FORECAST_UPDATE.
        Only executes when breach_confidence == "HIGH" on at least one metric.
        HIGH means the P50 trajectory predicts a breach within the horizon.
        """
        if emergency_stop_active.is_set():
            return
        if mra_down.is_set():
            return
        if proactive_suspended.is_set():
            return

        pfa_monitor.record_message()

        # Check whether P50 breach is predicted on either metric
        cpu_breach = forecast.get("cpu", {}).get("breach", {})
        mem_breach = forecast.get("memory", {}).get("breach", {})

        cpu_high = (cpu_breach.get("breach_confidence") == "HIGH")
        mem_high = (mem_breach.get("breach_confidence") == "HIGH")

        if not cpu_high and not mem_high:
            # No P50 breach predicted — no deliberation needed
            return

        if cooldown.is_active():
            log.info("proactive_cooldown_blocked",
                     remaining_s=round(cooldown.remaining_seconds(), 1))
            kb.write_audit("SCALING_ACTION", {
                "trigger":             "PROACTIVE",
                "action":             "DO_NOTHING",
                "reason":             "COOLDOWN_BLOCKED",
                "remaining_cooldown_s": round(cooldown.remaining_seconds(), 1),
            })
            return

        # Extract pressure level from the originating belief object
        originating_belief = forecast.get("originating_belief", {})
        pressure_level = originating_belief.get("pressure_level", "NORMAL")

        current = k8s.get_current_replicas()

        result = pipeline.run(
            forecast_obj=forecast,
            current_replicas=current,
            pressure_level=pressure_level,
        )

        audit = {
            "trigger":             "PROACTIVE",
            "action":             result.decision,
            "pressure_level":     result.pressure_level,
            "quantile_used":      result.quantile_used,
            "margin_applied_pct": round(result.margin_applied * 100, 1),
            "uncertainty_score":  result.uncertainty_score,
            "current_replicas":   current,
            "target_replicas":    result.target_replicas,
            "cpu_delta_pct":      round(result.cpu_delta_pct * 100, 1),
            "mem_delta_pct":      round(result.memory_delta_pct * 100, 1),
            "overflow":           result.overflow,
        }

        if result.decision == "DO_NOTHING":
            kb.write_audit("SCALING_ACTION", audit)
            return

        # Overflow escalation: publish before patch but after we know target
        if result.overflow:
            log.error(
                "capacity_overflow",
                computed=max(result.cpu_replica_estimate, result.memory_replica_estimate),
                max_replicas=config.max_replicas,
            )
            mqtt.publish_status("CAPACITY_OVERFLOW", {
                "namespace":      config.namespace,
                "deployment":     config.deployment_name,
                "computed_target": max(result.cpu_replica_estimate,
                                       result.memory_replica_estimate),
                "clamped_target": result.target_replicas,
            })

        # Persist cooldown BEFORE patch — mandatory ordering per spec
        cooldown.start(action_type="scale_out")
        k8s.patch_replicas(result.target_replicas)

        mqtt.publish_scaling_action(
            trigger="PROACTIVE",
            action="SCALE_UP",
            target_replicas=result.target_replicas,
            current_replicas=current,
            detail={
                "pressure_level":     result.pressure_level,
                "quantile_used":      result.quantile_used,
                "margin_applied_pct": round(result.margin_applied * 100, 1),
                "uncertainty_score":  result.uncertainty_score,
                "cpu_delta_pct":      round(result.cpu_delta_pct * 100, 1),
                "mem_delta_pct":      round(result.memory_delta_pct * 100, 1),
                "overflow":           result.overflow,
            },
        )

        audit["cooldown_started"] = True
        kb.write_audit("SCALING_ACTION", audit)

    def on_pfa_status(payload: dict) -> None:
        """
        Any PFA status event resets the PFA missed-cycle timer.
        INFERENCE_DEGRADED additionally suspends the proactive path.
        """
        pfa_monitor.record_message()
        event_type = payload.get("event_type", "")

        if event_type == "INFERENCE_DEGRADED":
            proactive_suspended.set()
            log.warning("proactive_path_suspended",
                        reason="INFERENCE_DEGRADED")

        elif event_type in ("WARMING_UP", "INFERENCE_ERROR"):
            log.info("pfa_status_received", event_type=event_type)

    def on_policy_ready(payload: dict) -> None:
        log.info("policy_ready_received",
                 keys=list(payload.keys()))

    def on_domain_ready(payload: dict) -> None:
        log.info("domain_ready_received")

    # ---- Peer monitor callbacks -------------------------------------------

    def on_mra_suspected():
        mqtt.publish_status("MRA_DOWN_SUSPECTED", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })

    def on_mra_confirmed():
        mra_down.set()
        mqtt.publish_status("MRA_DOWN_CONFIRMED", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })
        log.error("mra_down_confirmed_all_paths_suspended")

    def on_mra_recovered():
        mra_down.clear()
        log.info("mra_recovered_resuming")

    def on_pfa_suspected():
        mqtt.publish_status("PFA_DOWN_SUSPECTED", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })

    def on_pfa_confirmed():
        proactive_suspended.set()
        mqtt.publish_status("PFA_DOWN_CONFIRMED", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })
        log.error("pfa_down_confirmed_proactive_path_suspended")

    def on_pfa_recovered():
        # Proactive path resumes automatically on next valid FORECAST_UPDATE.
        # We clear the suspension flag here so it unblocks immediately.
        proactive_suspended.clear()
        log.info("pfa_recovered_proactive_path_resumed")

    # ---- Peer monitors ----------------------------------------------------
    mra_monitor = MRAPeerMonitor(
        scrape_interval_seconds=config.mra_scrape_interval_seconds,
        suspected_threshold=config.mra_suspected_threshold,
        confirmed_threshold=config.mra_confirmed_threshold,
        on_suspected=on_mra_suspected,
        on_confirmed=on_mra_confirmed,
        on_recovered=on_mra_recovered,
    )

    pfa_monitor = PFAPeerMonitor(
        suspected_seconds=config.pfa_suspected_seconds,
        confirmed_seconds=config.pfa_confirmed_seconds,
        on_suspected=on_pfa_suspected,
        on_confirmed=on_pfa_confirmed,
        on_recovered=on_pfa_recovered,
    )

    # ---- MQTT client ------------------------------------------------------
    mqtt = RSAMQTTClient(
        host=config.mqtt_host,
        port=config.mqtt_port,
        namespace=config.namespace,
        deployment=config.deployment_name,
        topic_mra_beliefs=config.topic_mra_beliefs,
        topic_mra_status=config.topic_mra_status,
        topic_pfa_forecasts=config.topic_pfa_forecasts,
        topic_pfa_status=config.topic_pfa_status,
        topic_rsa_actions=config.topic_rsa_actions,
        topic_rsa_status=config.topic_rsa_status,
        topic_rsa_heartbeat=config.topic_rsa_heartbeat,
        topic_system_policy=config.topic_system_policy,
        topic_domain_ready=config.topic_domain_ready,
        username=config.mqtt_username,
        password=config.mqtt_password,
        on_belief_update=on_belief_update,
        on_mra_status=on_mra_status,
        on_forecast_update=on_forecast_update,
        on_pfa_status=on_pfa_status,
        on_policy_ready=on_policy_ready,
        on_domain_ready=on_domain_ready,
    )

    # ---- Startup sequence -------------------------------------------------
    deadline = time.time() + config.startup_timeout_seconds

    if not _stage0(config, mqtt, k8s, kb, deadline):
        log.error("startup_aborted")
        sys.exit(1)

    _stage1_policy(config, mqtt, deadline)
    _stage2_dca(config, mqtt, deadline)

    # Stage 3: detect conflicting autoscalers and register in Domain 5
    log.info("startup_stage_3", msg="checking for conflicting autoscalers")
    if k8s.detect_conflicting_autoscaler():
        mqtt.publish_status("CONFLICTING_AUTOSCALER", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })
        kb.write_audit("CONFLICTING_AUTOSCALER", {
            "namespace":  config.namespace,
            "deployment": config.deployment_name,
        })
        log.error("conflicting_autoscaler_escalated")

    kb.upsert_heartbeat(status="ALIVE")
    log.info("startup_stage_3_complete")

    log.info("rsa_operational", agent_id=agent_id,
             namespace=config.namespace, deployment=config.deployment_name)

    # ---- Heartbeat thread -------------------------------------------------
    def _heartbeat():
        while not _shutdown.is_set():
            mqtt.publish_heartbeat(agent_id=agent_id)
            kb.upsert_heartbeat(status="ALIVE")
            _shutdown.wait(timeout=config.heartbeat_interval_seconds)

    threading.Thread(
        target=_heartbeat, daemon=True, name="heartbeat"
    ).start()

    # ---- Background peer monitor threads ----------------------------------
    # MRA monitor: ticks every 30s (aligned with MRA scrape interval)
    def _mra_monitor_loop():
        while not _shutdown.is_set():
            mra_monitor.check()
            _shutdown.wait(timeout=config.mra_scrape_interval_seconds)

    threading.Thread(
        target=_mra_monitor_loop, daemon=True, name="mra-peer-monitor"
    ).start()

    # PFA monitor uses its own background thread
    pfa_monitor.start_background_thread(_shutdown)

    # ---- Main loop --------------------------------------------------------
    # The RSA is entirely event-driven — all work happens in MQTT callbacks.
    # The main thread simply waits for the shutdown signal.
    _shutdown.wait()

    log.info("rsa_stopped")
    kb.upsert_heartbeat(status="SHUTDOWN")
    mqtt.stop()


if __name__ == "__main__":
    run(RSAConfig())

