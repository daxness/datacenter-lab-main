# mra/main.py
#
# MRA entry point — Specification Section 8 (Lifecycle).
#
# Startup sequence (Section 8.1):
#   Stage 0: MQTT connected + Prometheus reachable
#   Stage 1: Wait for POLICY_READY from SA (timeout → use env var defaults)
#   Stage 2: Wait for DOMAIN_READY from DCA (timeout → proceed anyway)
#   Stage 3: 5-second wait for PFA subscription to establish
#
# Main loop (Section 6):
#   Every 30 seconds, unconditionally:
#     Scrape → Preprocess → Validate → Evaluate → Build → Write KB → Publish
#
# Heartbeat: published every 15 seconds on a background thread.
# Shutdown: handles SIGTERM and SIGINT cleanly.
#
import json
import signal
import sys
import threading
import time
import structlog
 
from .config import MRAConfig
from .prometheus_scraper import PrometheusClient, PrometheusQueryError
from .preprocessor import preprocess
from .validator import Validator
from .pressure_evaluator import ResourcePressureEvaluator, PressureLevel
from .belief_builder import build_belief, belief_to_json
from .mqtt_publisher import MQTTPublisher
from .kb_writer import KBWriter
 
log = structlog.get_logger(__name__)
_shutdown = threading.Event()
 
 
def _handle_signal(signum, frame):
    log.info("shutdown_signal_received", signum=signum)
    _shutdown.set()
 
 
signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)
 
 
# ---- Startup sequence --------------------------------------------------------
 
def _stage0_infrastructure(config: MRAConfig, mqtt: MQTTPublisher,
                            prometheus: PrometheusClient, deadline: float) -> bool:
    """Stage 0: verify MQTT broker and Prometheus are reachable."""
    log.info("startup_stage_0", msg="checking infrastructure")
 
    remaining = deadline - time.time()
    if not mqtt.wait_connected(timeout=min(30.0, max(0, remaining))):
        log.error("startup_abort", reason="MQTT broker unreachable",
                  host=config.mqtt_host)
        return False
    log.info("startup_stage_0_mqtt_ok")
 
    # Quick Prometheus health check
    try:
        import requests
        r = requests.get(
            f"{config.prometheus_url}/-/healthy", timeout=5
        )
        r.raise_for_status()
        log.info("startup_stage_0_prometheus_ok", url=config.prometheus_url)
    except Exception as e:
        # Non-fatal — Prometheus may be starting. We continue and let the
        # first scrape cycle reveal the problem.
        log.warning("startup_stage_0_prometheus_check_failed", error=str(e))
 
    return True
 
 
def _stage1_policy(config: MRAConfig, mqtt: MQTTPublisher,
                   deadline: float) -> dict:
    """
    Stage 1: subscribe to POLICY_READY and wait for SA broadcast.
    Returns the policy payload dict if received; empty dict on timeout.
    """
    log.info("startup_stage_1", msg="waiting for POLICY_READY")
    policy_event = threading.Event()
    policy_data: dict = {}
 
    def _on_msg(client, userdata, msg):
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            return
        if msg.topic == config.topic_system_policy:
            policy_data.update(payload)
            policy_event.set()
            log.info("policy_ready_received")
 
    mqtt.on_message_set(_on_msg)
    mqtt.subscribe(config.topic_system_policy, qos=2)
 
    remaining = max(0, deadline - time.time())
    if not policy_event.wait(timeout=min(remaining, 90.0)):
        log.warning("startup_stage_1_timeout",
                    msg="POLICY_READY not received — using env var defaults")
    else:
        log.info("startup_stage_1_complete")
    return policy_data
 
 
def _stage2_dca(config: MRAConfig, mqtt: MQTTPublisher,
                deadline: float) -> None:
    """Stage 2: subscribe to DOMAIN_READY from DCA."""
    log.info("startup_stage_2", msg="waiting for DOMAIN_READY")
    domain_event = threading.Event()
 
    # Re-use the existing on_message callback and extend it
    prev_cb = mqtt._client.on_message
 
    def _on_msg(client, userdata, msg):
        if prev_cb:
            prev_cb(client, userdata, msg)
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            return
        if msg.topic == config.topic_domain_ready:
            domain_event.set()
            log.info("domain_ready_received")
 
    mqtt.on_message_set(_on_msg)
    mqtt.subscribe(config.topic_domain_ready, qos=1)
 
    remaining = max(0, deadline - time.time())
    if not domain_event.wait(timeout=min(remaining, 30.0)):
        log.warning("startup_stage_2_timeout",
                    msg="DOMAIN_READY not received — proceeding anyway")
    else:
        log.info("startup_stage_2_complete")
 
 
def _stage3_pfa(config: MRAConfig) -> None:
    """Stage 3: allow 5 seconds for PFA to establish its MQTT subscription."""
    log.info("startup_stage_3", msg="waiting 5s for PFA subscription")
    _shutdown.wait(timeout=5)
    log.info("startup_stage_3_complete")
 
 
# ---- Main entry point -------------------------------------------------------
 
def run(config: MRAConfig) -> None:
 
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
 
    log.info("mra_starting",
             namespace=config.namespace,
             deployment=config.deployment_name,
             prometheus=config.prometheus_url,
             mqtt_host=config.mqtt_host)
 
    # ---- Initialise all components ----
    prometheus = PrometheusClient(
        base_url=config.prometheus_url,
        namespace=config.namespace,
        deployment_name=config.deployment_name,
        cpu_rate_window=config.cpu_rate_window,
    )
 
    mqtt = MQTTPublisher(
        host=config.mqtt_host,
        port=config.mqtt_port,
        client_id=config.mqtt_client_id,
        topic_beliefs=config.topic_beliefs,
        topic_status=config.topic_status,
        topic_heartbeat=config.topic_heartbeat,
        username=config.mqtt_username,
        password=config.mqtt_password,
    )
 
    kb = KBWriter(
        domain4_db_path=config.domain4_db_path,
        namespace=config.namespace,
        deployment=config.deployment_name,
        metrics_port=config.metrics_port,
    )
 
    validator  = Validator()
    evaluator  = ResourcePressureEvaluator(
        cpu_warning_pct=config.cpu_warning_pct,
        cpu_critical_pct=config.cpu_critical_pct,
        memory_warning_pct=config.memory_warning_pct,
        memory_critical_pct=config.memory_critical_pct,
        warning_consecutive=config.warning_consecutive_threshold,
    )
 
    # ---- Startup sequence ----
    deadline = time.time() + config.startup_timeout_seconds
 
    if not _stage0_infrastructure(config, mqtt, prometheus, deadline):
        log.error("startup_aborted")
        sys.exit(1)
 
    _stage1_policy(config, mqtt, deadline)
    _stage2_dca(config, mqtt, deadline)
    _stage3_pfa(config)
 
    log.info("mra_operational", msg="entering scrape loop")
 
    # ---- Heartbeat thread ----
    agent_id = f"MRA:{config.namespace}/{config.deployment_name}"
 
    def _heartbeat():
        while not _shutdown.is_set():
            mqtt.publish_heartbeat(agent_id=agent_id)
            _shutdown.wait(timeout=config.heartbeat_interval_seconds)
 
    threading.Thread(target=_heartbeat, daemon=True, name="heartbeat").start()
 
    # ---- Main scrape loop ----
    consecutive_scrape_failures = 0
 
    while not _shutdown.is_set():
        cycle_start = time.time()
 
        # Step 1: Scrape
        try:
            raw = prometheus.scrape_all()
            consecutive_scrape_failures = 0
        except PrometheusQueryError as e:
            consecutive_scrape_failures += 1
            log.error("scrape_failed",
                      error=str(e), consecutive=consecutive_scrape_failures)
            mqtt.publish_scrape_failed(detail=str(e))
            if consecutive_scrape_failures >= 3:
                log.error("scrape_failed_persistent",
                          msg="3+ consecutive failures — SA should alert operator")
            _shutdown.wait(timeout=config.scrape_interval_seconds)
            continue
 
        # Step 2: Preprocess
        sample = preprocess(raw, config.namespace, config.deployment_name)
 
        # Step 3: Validate
        result = validator.validate(sample)
 
        # Write audit events from validation failures
        for evt in result.error_events:
            kb.write_domain4(record_type=evt["record_type"], payload=evt)
            sub = evt.get("sub_type", "")
            if sub == "UNCONFIGURED":
                mqtt.publish_validation_error(sub_type=sub, detail=evt["detail"])
 
        if not result.passed:
            log.info("cycle_skipped",
                     outcomes=[o.value for o in result.outcomes])
            _shutdown.wait(timeout=max(
                0, config.scrape_interval_seconds - (time.time() - cycle_start)
            ))
            continue
 
        # Step 4: Pressure evaluation
        s = result.sample
        level = evaluator.evaluate(
            cpu_usage=s.cpu_usage_millicores or 0.0,
            cpu_limit=s.cpu_limits_millicores,
            memory_usage=s.memory_usage_MiB or 0.0,
            memory_limit=s.memory_limits_MiB,
        )
 
        # Step 5: Build belief object
        belief = build_belief(s, level, result.filled_metrics)
 
        # Step 6: Write KB
        kb.write_domain3(belief)
 
        # Write Domain 4 only for non-routine events (Spec Table 7):
        # VALIDATION_RANGE_ERROR, VALIDATION_STALENESS_ERROR are written above.
        # BELIEF_UPDATE with filled values is written here.
        if result.filled_metrics:
            kb.write_domain4(
                record_type="BELIEF_UPDATE_FILLED",
                payload={
                    "namespace": config.namespace,
                    "deployment": config.deployment_name,
                    "filled_metrics": result.filled_metrics,
                    "timestamp_ms": s.timestamp_utc_ms,
                },
            )
 
        # Step 7: Publish
        mqtt.publish_belief_update(belief_to_json(belief))
 
        elapsed = time.time() - cycle_start
        log.info("cycle_complete",
                 pressure=level.value,
                 cpu_mc=round(s.cpu_usage_millicores or 0, 2),
                 mem_MiB=round(s.memory_usage_MiB or 0, 2),
                 filled=result.filled_metrics,
                 elapsed_s=round(elapsed, 3))
 
        _shutdown.wait(timeout=max(
            0, config.scrape_interval_seconds - elapsed
        ))
 
    log.info("mra_stopped")
    mqtt.stop()
 
 
if __name__ == "__main__":
    run(MRAConfig())
