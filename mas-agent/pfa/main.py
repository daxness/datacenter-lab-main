# pfa/main.py — PFA entry point
import json, signal, sys, threading, time, structlog

from .config import PFAConfig
from .sliding_window import DualSlidingWindow
from .inference_engine import InferenceEngine, compute_uncertainty
from .forecast_builder import build_forecast_update, forecast_to_json
from .mra_monitor import MRAMonitor
from .kb_writer import PFAKBWriter
from .mqtt_client import PFAMQTTClient

log = structlog.get_logger(__name__)
_shutdown = threading.Event()

def _handle_signal(signum, frame):
    log.info("shutdown_signal_received", signum=signum)
    _shutdown.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


def run(config: PFAConfig) -> None:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )

    log.info("pfa_starting", namespace=config.namespace,
             deployment=config.deployment_name)

    # ---- Shared state ----
    window = DualSlidingWindow(
        max_size=config.window_max_size,
        min_inference=config.window_min_inference,
        patch_size=config.patch_size,
    )
    engine = InferenceEngine(
        forecast_steps=config.forecast_steps,
        patch_size=config.patch_size,
    )
    kb = PFAKBWriter(
        domain4_db_path=config.domain4_db_path,
        namespace=config.namespace,
        deployment=config.deployment_name,
    )

    last_inference_time: float       = 0.0
    last_belief: dict                = {}
    last_p50_cpu: list               = []
    last_p50_memory: list            = []
    last_inference_ts_ms: int        = 0
    consecutive_degraded: int        = 0
    inference_suspended: bool        = False
    warming_up_published: bool       = False
    cpu_breach_pct                   = config.cpu_breach_pct
    memory_breach_pct                = config.memory_breach_pct

    policy_event = threading.Event()
    domain_event = threading.Event()

    # ---- Inference cycle ----
    def _run_inference(belief: dict, now: float) -> None:
        nonlocal last_inference_time, last_p50_cpu, last_p50_memory
        nonlocal last_inference_ts_ms

        cpu_input = window.cpu.get_inference_input()
        mem_input = window.memory.get_inference_input()
        if cpu_input is None or mem_input is None:
            return

        try:
            result = engine.run(cpu_input, mem_input)
        except (ValueError, Exception) as e:
            log.error("inference_error", error=str(e))
            mqtt.publish_status("INFERENCE_ERROR", {"detail": str(e)})
            return

        # Accuracy log (compare previous P50 vs actuals)
        if last_p50_cpu and last_inference_ts_ms > 0:
            elapsed = int((now - last_inference_ts_ms/1000) / config.step_duration_seconds)
            kb.write_forecast_accuracy(
                previous_p50_cpu=last_p50_cpu,
                previous_p50_memory=last_p50_memory,
                actual_cpu_window=window.cpu.values,
                actual_memory_window=window.memory.values,
                elapsed_steps=elapsed,
                cycle_timestamp_ms=result.timestamp_ms,
                cpu_uncertainty=compute_uncertainty(result.cpu, window.cpu.fill_ratio),
                memory_uncertainty=compute_uncertainty(result.memory, window.memory.fill_ratio),
            )

        cm = belief.get("context_metrics", {})
        cpu_limit = cm.get("cpu_limits") or 1000.0
        mem_limit = cm.get("memory_limits") or 512.0

        forecast = build_forecast_update(
            result=result,
            cpu_window_fill_ratio=window.cpu.fill_ratio,
            memory_window_fill_ratio=window.memory.fill_ratio,
            cpu_limit_millicores=cpu_limit,
            memory_limit_MiB=mem_limit,
            cpu_breach_pct=cpu_breach_pct,
            memory_breach_pct=memory_breach_pct,
            step_duration_seconds=config.step_duration_seconds,
            originating_belief=belief,
            namespace=config.namespace,
            deployment=config.deployment_name,
        )

        mqtt.publish_forecast(forecast_to_json(forecast))

        last_p50_cpu        = result.cpu.p50.copy()
        last_p50_memory     = result.memory.p50.copy()
        last_inference_time = now
        last_inference_ts_ms = result.timestamp_ms

        log.info("forecast_published",
                 model=result.cpu.model_used,
                 duration_ms=result.inference_duration_ms,
                 p50_breach=forecast["p50_breach_predicted"],
                 cpu_window=window.cpu.size,
                 mem_window=window.memory.size)

    # ---- MQTT callbacks ----
    def on_belief_update(belief: dict) -> None:
        nonlocal last_belief, consecutive_degraded
        nonlocal inference_suspended, warming_up_published

        mra_monitor.record_belief_update()
        last_belief = belief
        window.append_from_belief(belief)

        # Warmup check
        if not window.is_ready:
            if not warming_up_published:
                mqtt.publish_status("WARMING_UP", {
                    "cpu_window_size":    window.cpu.size,
                    "memory_window_size": window.memory.size,
                    "required":          config.window_min_inference,
                })
                log.info("warming_up",
                         cpu_size=window.cpu.size,
                         mem_size=window.memory.size)
                warming_up_published = True
            return
        warming_up_published = False

        # Forward-fill degradation check
        max_fill = max(window.cpu.fill_ratio, window.memory.fill_ratio)
        if max_fill > config.forward_fill_max_ratio:
            consecutive_degraded += 1
            if consecutive_degraded >= config.forward_fill_degraded_cycles and not inference_suspended:
                inference_suspended = True
                mqtt.publish_status("INFERENCE_DEGRADED", {
                    "cpu_fill_ratio":    round(window.cpu.fill_ratio, 3),
                    "memory_fill_ratio": round(window.memory.fill_ratio, 3),
                })
                log.warning("inference_degraded")
            return
        else:
            consecutive_degraded = 0
            if inference_suspended:
                inference_suspended = False
                log.info("inference_resumed")

        if inference_suspended:
            return

        # Inference triggering (Spec Section 4.1)
        pressure = belief.get("pressure_level", "NORMAL")
        now = time.time()
        periodic_due = (now - last_inference_time) >= config.periodic_inference_interval_seconds
        warning_gap_ok = (now - last_inference_time) >= config.min_inference_gap_seconds
        warning_trigger = (pressure == "WARNING" and warning_gap_ok)

        if periodic_due or warning_trigger:
            _run_inference(belief, now)

    def on_mra_status() -> None:
        mra_monitor.record_mra_status_event()

    def on_policy_ready(payload: dict) -> None:
        nonlocal cpu_breach_pct, memory_breach_pct
        th = payload.get("sla_thresholds", {})
        if "cpu_warning_pct" in th:
            cpu_breach_pct = float(th["cpu_warning_pct"])
        if "memory_warning_pct" in th:
            memory_breach_pct = float(th["memory_warning_pct"])
        log.info("policy_ready_received",
                 cpu_breach_pct=cpu_breach_pct,
                 memory_breach_pct=memory_breach_pct)
        policy_event.set()

    def on_domain_ready() -> None:
        log.info("domain_ready_received")
        domain_event.set()

    # ---- MRA monitor callbacks ----
    def on_mra_suspected():
        mqtt.publish_status("MRA_DOWN_SUSPECTED", {})
        log.warning("mra_down_suspected_published")

    def on_mra_confirmed():
        mqtt.publish_status("MRA_DOWN_CONFIRMED", {})
        window.reset()
        log.warning("mra_down_confirmed_window_reset")

    def on_mra_recovered():
        log.info("mra_recovered")

    mra_monitor = MRAMonitor(
        expected_interval_seconds=config.mra_expected_interval_seconds,
        suspected_threshold=config.mra_missed_suspected,
        confirmed_threshold=config.mra_missed_confirmed,
        on_suspected=on_mra_suspected,
        on_confirmed=on_mra_confirmed,
        on_recovered=on_mra_recovered,
    )

    # ---- MQTT client ----
    mqtt = PFAMQTTClient(
        host=config.mqtt_host,
        port=config.mqtt_port,
        namespace=config.namespace,
        deployment=config.deployment_name,
        username=config.mqtt_username,
        password=config.mqtt_password,
        topic_mra_beliefs=config.topic_mra_beliefs,
        topic_mra_status=config.topic_mra_status,
        topic_system_policy=config.topic_system_policy,
        topic_domain_ready=config.topic_domain_ready,
        topic_forecasts=config.topic_forecasts,
        topic_status=config.topic_status,
        topic_heartbeat=config.topic_heartbeat,
        on_belief_update=on_belief_update,
        on_mra_status=on_mra_status,
        on_policy_ready=on_policy_ready,
        on_domain_ready=on_domain_ready,
    )

    # ---- Startup sequence ----
    deadline = time.time() + config.startup_timeout_seconds
    log.info("startup_stage_0", msg="checking MQTT")
    if not mqtt.wait_connected(timeout=min(30., deadline - time.time())):
        log.error("startup_abort", reason="MQTT unreachable")
        sys.exit(1)
    log.info("startup_stage_0_complete")

    log.info("startup_stage_1", msg="waiting for POLICY_READY")
    policy_event.wait(timeout=min(90., deadline - time.time()))
    if not policy_event.is_set():
        log.warning("startup_stage_1_timeout",
                    msg="POLICY_READY not received — using env var defaults")
    else:
        log.info("startup_stage_1_complete")

    log.info("startup_stage_2", msg="waiting for DOMAIN_READY")
    domain_event.wait(timeout=min(30., deadline - time.time()))
    if not domain_event.is_set():
        log.warning("startup_stage_2_timeout",
                    msg="DOMAIN_READY not received — proceeding anyway")
    else:
        log.info("startup_stage_2_complete")

    log.info("startup_stage_3", msg="waiting 5s for MRA subscription")
    _shutdown.wait(timeout=5)
    log.info("startup_stage_3_complete")

    log.info("pfa_operational", msg="listening for MRA belief updates")

    # ---- Background threads ----
    agent_id = f"PFA:{config.namespace}/{config.deployment_name}"

    def _heartbeat():
        while not _shutdown.is_set():
            mqtt.publish_heartbeat(agent_id=agent_id)
            _shutdown.wait(timeout=config.heartbeat_interval_seconds)

    threading.Thread(target=_heartbeat, daemon=True, name="heartbeat").start()
    mra_monitor.start_background_thread(_shutdown)

    # PFA is event-driven — all work happens in MQTT callbacks
    _shutdown.wait()
    log.info("pfa_stopped")
    mqtt.stop()


if __name__ == "__main__":
    run(PFAConfig())
