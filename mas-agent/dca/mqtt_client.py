# dca/mqtt_client.py
#
# MQTT client for the DCA — Specification Section 5 and Section 6.
#
# The DCA subscribes to the widest topic set in the system because it
# supervises every MRA, PFA, and RSA instance in its namespace. Topics
# are built dynamically for each managed deployment at connection time.
#
# Subscriptions (what DCA receives — Section 5):
#   Per deployment:
#     /mas/{ns}/{dep}/mra/beliefs             QoS 1 — pressure aggregation + L2 cross-validation
#     /mas/{ns}/{dep}/mra/status              QoS 1 — MRA scrape/validation errors
#     /mas/system/heartbeats/{ns}/{dep}/mra   QoS 1 — L1 MRA liveness
#     /mas/{ns}/{dep}/pfa/forecasts           QoS 1 — forecast risk map update
#     /mas/{ns}/{dep}/pfa/status              QoS 1 — PFA operational state
#     /mas/system/heartbeats/{ns}/{dep}/pfa   QoS 1 — L1 PFA liveness
#     /mas/{ns}/{dep}/rsa/actions             QoS 1 — supplementary L2 RSA liveness
#     /mas/{ns}/{dep}/rsa/status              QoS 1 — CAPACITY_OVERFLOW, EMERGENCY_INSUFFICIENT, etc.
#     /mas/system/heartbeats/{ns}/{dep}/rsa   QoS 1 — L1 RSA liveness (only layer for RSA)
#   System-wide:
#     /mas/system/policy/ready                QoS 2 — SA POLICY_READY (startup gate)
#     /mas/system/modechange                  QoS 2 — SA EMERGENCY_STOP / RESUME
#
# Publications (what DCA sends — Section 6):
#   /mas/system/domain/ready                          QoS 2 — DOMAIN_READY startup broadcast
#   /mas/system/domain/{domain_id}/dca/control        QoS 1 — AGENT_DOWN
#   /mas/{ns}/{dep}/dca/control                       QoS 2 — EMERGENCY_STOP_FWD (per RSA)
#   /mas/system/domain/{domain_id}/dca/escalation     QoS 1 — SA_NOTIFICATION
#   /mas/system/domain/{domain_id}/dca/status         QoS 1 — DOMAIN_STATUS_REPORT
#   /mas/system/heartbeats/dca/{domain_id}            QoS 0 — DCA heartbeat every 15s
#   /mas/system/domain/{domain_id}/dca/audit          QoS 1 — COORDINATION_EVENT
#
# Uses paho-mqtt v2 (CallbackAPIVersion.VERSION2) — identical to all other agents.
#
import json
import threading
import time
import uuid
import paho.mqtt.client as mqtt
import structlog

log = structlog.get_logger(__name__)


class DCAMQTTClient:

    def __init__(
        self,
        host: str,
        port: int,
        domain_id: str,
        managed_namespace: str,
        managed_deployments: list,
        # Topic strings (computed by config.py, passed in to keep this class decoupled)
        topic_domain_ready: str,
        topic_agent_down: str,
        topic_sa_notification: str,
        topic_domain_status: str,
        topic_dca_heartbeat: str,
        topic_coordination_audit: str,
        topic_sa_policy: str,
        topic_sa_modechange: str,
        # Topic builder functions — called per deployment
        topic_fn_mra_beliefs,
        topic_fn_mra_status,
        topic_fn_mra_heartbeat,
        topic_fn_pfa_forecasts,
        topic_fn_pfa_status,
        topic_fn_pfa_heartbeat,
        topic_fn_rsa_actions,
        topic_fn_rsa_status,
        topic_fn_rsa_heartbeat,
        topic_fn_emergency_stop_fwd,
        username: str = "",
        password: str = "",
        # Business-logic callbacks (injected by main.py — no logic in this class)
        on_mra_belief_update=None,
        on_mra_status=None,
        on_mra_heartbeat=None,
        on_pfa_forecast_update=None,
        on_pfa_status=None,
        on_pfa_heartbeat=None,
        on_rsa_action=None,
        on_rsa_status=None,
        on_rsa_heartbeat=None,
        on_policy_ready=None,
        on_modechange=None,
    ):
        self._domain_id           = domain_id
        self._managed_namespace   = managed_namespace
        self._managed_deployments = managed_deployments

        # Static publication topics
        self._topic_domain_ready        = topic_domain_ready
        self._topic_agent_down          = topic_agent_down
        self._topic_sa_notification     = topic_sa_notification
        self._topic_domain_status       = topic_domain_status
        self._topic_dca_heartbeat       = topic_dca_heartbeat
        self._topic_coordination_audit  = topic_coordination_audit
        self._topic_sa_policy           = topic_sa_policy
        self._topic_sa_modechange       = topic_sa_modechange

        # Topic builder functions (per-deployment)
        self._fn_mra_beliefs        = topic_fn_mra_beliefs
        self._fn_mra_status         = topic_fn_mra_status
        self._fn_mra_heartbeat      = topic_fn_mra_heartbeat
        self._fn_pfa_forecasts      = topic_fn_pfa_forecasts
        self._fn_pfa_status         = topic_fn_pfa_status
        self._fn_pfa_heartbeat      = topic_fn_pfa_heartbeat
        self._fn_rsa_actions        = topic_fn_rsa_actions
        self._fn_rsa_status         = topic_fn_rsa_status
        self._fn_rsa_heartbeat      = topic_fn_rsa_heartbeat
        self._fn_emergency_stop_fwd = topic_fn_emergency_stop_fwd

        # Business-logic callbacks (all business logic lives in main.py)
        self._on_mra_belief_update   = on_mra_belief_update
        self._on_mra_status          = on_mra_status
        self._on_mra_heartbeat       = on_mra_heartbeat
        self._on_pfa_forecast_update = on_pfa_forecast_update
        self._on_pfa_status          = on_pfa_status
        self._on_pfa_heartbeat       = on_pfa_heartbeat
        self._on_rsa_action          = on_rsa_action
        self._on_rsa_status          = on_rsa_status
        self._on_rsa_heartbeat       = on_rsa_heartbeat
        self._on_policy_ready        = on_policy_ready
        self._on_modechange          = on_modechange

        # Build reverse-lookup map: topic_string → (agent_type, deployment)
        # This allows O(1) dispatch in _on_message without string matching.
        self._topic_dispatch: dict[str, tuple] = {}
        for dep in managed_deployments:
            self._topic_dispatch[topic_fn_mra_beliefs(dep)]    = ("MRA_BELIEF",    dep)
            self._topic_dispatch[topic_fn_mra_status(dep)]     = ("MRA_STATUS",    dep)
            self._topic_dispatch[topic_fn_mra_heartbeat(dep)]  = ("MRA_HEARTBEAT", dep)
            self._topic_dispatch[topic_fn_pfa_forecasts(dep)]  = ("PFA_FORECAST",  dep)
            self._topic_dispatch[topic_fn_pfa_status(dep)]     = ("PFA_STATUS",    dep)
            self._topic_dispatch[topic_fn_pfa_heartbeat(dep)]  = ("PFA_HEARTBEAT", dep)
            self._topic_dispatch[topic_fn_rsa_actions(dep)]    = ("RSA_ACTION",    dep)
            self._topic_dispatch[topic_fn_rsa_status(dep)]     = ("RSA_STATUS",    dep)
            self._topic_dispatch[topic_fn_rsa_heartbeat(dep)]  = ("RSA_HEARTBEAT", dep)

        self._topic_dispatch[topic_sa_policy]     = ("SA_POLICY",     None)
        self._topic_dispatch[topic_sa_modechange] = ("SA_MODECHANGE", None)

        self._connected = threading.Event()

        client_id = f"dca-{domain_id}-{uuid.uuid4().hex[:8]}"
        self._client = mqtt.Client(
            client_id=client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            clean_session=True,
        )
        if username:
            self._client.username_pw_set(username, password)

        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message    = self._on_message

        log.info("mqtt_connecting", host=host, port=port, client_id=client_id)
        self._client.connect(host, port, keepalive=60)
        self._client.loop_start()

    # ---- Connection callbacks ----------------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._connected.set()
            log.info("mqtt_connected")
            self._subscribe_all(client)
        else:
            log.error("mqtt_connect_failed", reason_code=str(reason_code))

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected.clear()
        log.warning("mqtt_disconnected", reason_code=str(reason_code))

    def _subscribe_all(self, client) -> None:
        """
        Subscribe to all topics for every managed deployment.
        Called on every connect so reconnections automatically restore
        subscriptions without any extra management code (identical to RSA pattern).
        """
        dep_count = 0
        for dep in self._managed_deployments:
            client.subscribe(self._fn_mra_beliefs(dep),    qos=1)
            client.subscribe(self._fn_mra_status(dep),     qos=1)
            client.subscribe(self._fn_mra_heartbeat(dep),  qos=1)
            client.subscribe(self._fn_pfa_forecasts(dep),  qos=1)
            client.subscribe(self._fn_pfa_status(dep),     qos=1)
            client.subscribe(self._fn_pfa_heartbeat(dep),  qos=1)
            client.subscribe(self._fn_rsa_actions(dep),    qos=1)
            client.subscribe(self._fn_rsa_status(dep),     qos=1)
            client.subscribe(self._fn_rsa_heartbeat(dep),  qos=1)
            dep_count += 1

        client.subscribe(self._topic_sa_policy,     qos=2)
        client.subscribe(self._topic_sa_modechange, qos=2)

        log.info(
            "mqtt_subscriptions_registered",
            deployment_count=dep_count,
            total_topics=dep_count * 9 + 2,
        )

    # ---- Message dispatch --------------------------------------------------

    def _on_message(self, client, userdata, msg):
        """
        Pure dispatcher — routes to the correct callback by topic.
        All business logic lives in main.py callbacks.
        """
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            log.warning("mqtt_bad_json", topic=msg.topic)
            return

        entry = self._topic_dispatch.get(msg.topic)
        if entry is None:
            log.debug("mqtt_unrouted_topic", topic=msg.topic)
            return

        kind, deployment = entry

        if kind == "MRA_BELIEF"    and self._on_mra_belief_update:
            self._on_mra_belief_update(deployment, payload)
        elif kind == "MRA_STATUS"   and self._on_mra_status:
            self._on_mra_status(deployment, payload)
        elif kind == "MRA_HEARTBEAT" and self._on_mra_heartbeat:
            self._on_mra_heartbeat(deployment, payload)
        elif kind == "PFA_FORECAST" and self._on_pfa_forecast_update:
            self._on_pfa_forecast_update(deployment, payload)
        elif kind == "PFA_STATUS"   and self._on_pfa_status:
            self._on_pfa_status(deployment, payload)
        elif kind == "PFA_HEARTBEAT" and self._on_pfa_heartbeat:
            self._on_pfa_heartbeat(deployment, payload)
        elif kind == "RSA_ACTION"   and self._on_rsa_action:
            self._on_rsa_action(deployment, payload)
        elif kind == "RSA_STATUS"   and self._on_rsa_status:
            self._on_rsa_status(deployment, payload)
        elif kind == "RSA_HEARTBEAT" and self._on_rsa_heartbeat:
            self._on_rsa_heartbeat(deployment, payload)
        elif kind == "SA_POLICY"    and self._on_policy_ready:
            self._on_policy_ready(payload)
        elif kind == "SA_MODECHANGE" and self._on_modechange:
            self._on_modechange(payload)

    # ---- Connection helpers ------------------------------------------------

    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    # ---- Publications -------------------------------------------------------

    def publish_domain_ready(self, domain_id: str) -> None:
        """
        Broadcast DOMAIN_READY — QoS 2 to guarantee exactly-once delivery.
        This is the startup gate signal for all Tier-2 agents.
        """
        payload = json.dumps({
            "event_type":  "DOMAIN_READY",
            "domain_id":   domain_id,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_domain_ready,
            payload=payload,
            qos=2,
        )
        log.info("domain_ready_published", domain_id=domain_id)

    def publish_agent_down(self, agent_type: str, deployment: str) -> None:
        """
        Broadcast AGENT_DOWN — QoS 1.
        Received by all Tier-2 agents; triggers suspension of processing
        pipelines that depend on the failed agent.
        """
        payload = json.dumps({
            "event_type":  "AGENT_DOWN",
            "agent_type":  agent_type,
            "deployment":  deployment,
            "domain_id":   self._domain_id,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_agent_down,
            payload=payload,
            qos=1,
        )
        log.warning(
            "agent_down_broadcast",
            agent_type=agent_type,
            deployment=deployment,
        )

    def publish_sa_notification(self, signal_type: str, deployment: str | None,
                                 detail: dict) -> None:
        """
        Send enriched notification to the SA — QoS 1.
        Always includes full domain context assembled by main.py.
        """
        payload = json.dumps({
            "event_type":   "SA_NOTIFICATION",
            "signal_type":  signal_type,
            "domain_id":    self._domain_id,
            "deployment":   deployment,
            "timestamp_ms": int(time.time() * 1000),
            **detail,
        })
        self._client.publish(
            topic=self._topic_sa_notification,
            payload=payload,
            qos=1,
        )
        log.warning(
            "sa_notification_sent",
            signal_type=signal_type,
            deployment=deployment,
        )

    def publish_emergency_stop_fwd(self, deployment: str) -> None:
        """
        Forward EMERGENCY_STOP to one RSA instance — QoS 2 (exactly-once).
        Called for every managed deployment when SA issues EMERGENCY_STOP.
        """
        payload = json.dumps({
            "event_type":   "EMERGENCY_STOP_FWD",
            "domain_id":    self._domain_id,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._fn_emergency_stop_fwd(deployment),
            payload=payload,
            qos=2,
        )
        log.warning("emergency_stop_forwarded", deployment=deployment)

    def publish_coordination_audit(self, event_type: str, detail: dict) -> None:
        """Publish audit record to the coordination audit topic (QoS 1)."""
        payload = json.dumps({
            "event_type":   event_type,
            "domain_id":    self._domain_id,
            "timestamp_ms": int(time.time() * 1000),
            **detail,
        })
        self._client.publish(
            topic=self._topic_coordination_audit,
            payload=payload,
            qos=1,
        )

    def publish_domain_status_report(self, report: dict) -> None:
        """Publish 30-minute domain health summary to SA — QoS 1."""
        payload = json.dumps({
            "event_type":   "DOMAIN_STATUS_REPORT",
            "domain_id":    self._domain_id,
            "timestamp_ms": int(time.time() * 1000),
            **report,
        })
        self._client.publish(
            topic=self._topic_domain_status,
            payload=payload,
            qos=1,
        )
        log.info("domain_status_report_published")

    def publish_heartbeat(self, agent_id: str) -> None:
        """DCA liveness signal — QoS 0, every 15 seconds."""
        payload = json.dumps({
            "event_type":   "HEARTBEAT",
            "agent_id":     agent_id,
            "domain_id":    self._domain_id,
            "status":       "ALIVE",
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_dca_heartbeat,
            payload=payload,
            qos=0,
        )
