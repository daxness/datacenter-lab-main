# sa/mqtt_client.py
#
# MQTT client for the SA.
#
# Subscriptions (what SA receives):
#   /mas/system/heartbeats/dca/#       QoS 1 — DCA liveness heartbeats
#   /mas/system/domain/+/dca/escalation QoS 1 — SA_NOTIFICATION from DCAs
#   /mas/system/domain/+/dca/status    QoS 1 — DOMAIN_STATUS_REPORT (30-min summaries)
#   /mas/system/domain/<id>/dca/control QoS 2 — DOMAIN_READY (startup gate)
#
# Publications (what SA sends):
#   /mas/system/policy/ready           QoS 2 — POLICY_READY (startup gate for all agents)
#   /mas/system/modechange             QoS 2 — EMERGENCY_STOP and RESUME
#   /mas/system/heartbeats/sa          QoS 0 — SA liveness signal (every 15s)
#
# Uses paho-mqtt v2 (CallbackAPIVersion.VERSION2) — identical to RSA and PFA.
# UUID suffix on client_id prevents collision on pod restarts.
#
import json
import threading
import time
import uuid
import paho.mqtt.client as mqtt
import structlog

log = structlog.get_logger(__name__)


class SAMQTTClient:

    def __init__(
        self,
        host: str,
        port: int,
        domain_id: str,
        topic_policy_ready: str,
        topic_mode_change: str,
        topic_sa_heartbeat: str,
        topic_dca_heartbeat: str,
        topic_dca_escalation: str,
        topic_dca_status: str,
        topic_dca_control: str,
        username: str = "",
        password: str = "",
        on_dca_heartbeat=None,     # callable(domain_id: str, payload: dict)
        on_dca_escalation=None,    # callable(domain_id: str, payload: dict)
        on_dca_status=None,        # callable(domain_id: str, payload: dict)
        on_domain_ready=None,      # callable(payload: dict)
    ):
        self._domain_id             = domain_id
        self._topic_policy_ready    = topic_policy_ready
        self._topic_mode_change     = topic_mode_change
        self._topic_sa_heartbeat    = topic_sa_heartbeat
        self._topic_dca_heartbeat   = topic_dca_heartbeat
        self._topic_dca_escalation  = topic_dca_escalation
        self._topic_dca_status      = topic_dca_status
        self._topic_dca_control     = topic_dca_control

        # Business-logic callbacks injected by main.py
        self._on_dca_heartbeat  = on_dca_heartbeat
        self._on_dca_escalation = on_dca_escalation
        self._on_dca_status     = on_dca_status
        self._on_domain_ready   = on_domain_ready

        self._connected = threading.Event()

        client_id = f"sa-global-{uuid.uuid4().hex[:8]}"

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

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._connected.set()
            log.info("mqtt_connected")
            # Re-subscribe on every connect so reconnections restore subscriptions.
            client.subscribe(self._topic_dca_heartbeat,  qos=1)
            client.subscribe(self._topic_dca_escalation, qos=1)
            client.subscribe(self._topic_dca_status,     qos=1)
            client.subscribe(self._topic_dca_control,    qos=2)
            log.info("mqtt_subscriptions_registered")
        else:
            log.error("mqtt_connect_failed", reason_code=str(reason_code))

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected.clear()
        log.warning("mqtt_disconnected", reason_code=str(reason_code))

    def _on_message(self, client, userdata, msg):
        """
        Route incoming MQTT messages to the appropriate business-logic callback.
        This method is a pure dispatcher — no logic of its own.
        """
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            log.warning("mqtt_bad_json", topic=msg.topic)
            return

        topic = msg.topic

        # ---- DCA heartbeat: /mas/system/heartbeats/dca/<domain-id>
        # The DCA publishes heartbeats on a topic that ends with its domain-id.
        # We extract the domain_id from the topic suffix.
        if topic.startswith("/mas/system/heartbeats/dca/"):
            domain_id = topic.split("/")[-1]
            if self._on_dca_heartbeat:
                self._on_dca_heartbeat(domain_id, payload)
            return

        # ---- DCA escalation: /mas/system/domain/<id>/dca/escalation
        if topic.endswith("/dca/escalation"):
            parts = topic.split("/")
            # topic format: /mas/system/domain/<id>/dca/escalation
            # index:         0  1   2      3    4   5    6
            domain_id = parts[4] if len(parts) >= 6 else "unknown"
            if self._on_dca_escalation:
                self._on_dca_escalation(domain_id, payload)
            return

        # ---- DCA status report: /mas/system/domain/<id>/dca/status
        if topic.endswith("/dca/status"):
            parts = topic.split("/")
            domain_id = parts[4] if len(parts) >= 6 else "unknown"
            if self._on_dca_status:
                self._on_dca_status(domain_id, payload)
            return

        # ---- DCA control (DOMAIN_READY): /mas/system/domain/<id>/dca/control
        if topic.endswith("/dca/control"):
            if self._on_domain_ready:
                self._on_domain_ready(payload)
            return

    # ---- Connection helpers -----------------------------------------------

    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    # ---- Publications ----------------------------------------------------

    def publish_policy_ready(self, policy: dict) -> None:
        """
        Broadcast POLICY_READY at QoS 2.
        This is the gate signal that allows all DCAs and Tier-2 agents to proceed.
        Called at startup and on every successful policy override.
        """
        payload = json.dumps({
            "event_type":      "POLICY_READY",
            "timestamp_ms":    int(time.time() * 1000),
            "policy_version":  policy.get("policy_version", 0),
            "policy":          policy,
        })
        self._client.publish(
            topic=self._topic_policy_ready,
            payload=payload,
            qos=2,
            retain=True,  # Retain so agents that start later still receive it.
        )
        log.info("policy_ready_published",
                 policy_version=policy.get("policy_version", 0))

    def publish_emergency_stop(self, issued_by: str = "operator") -> None:
        """
        Broadcast EMERGENCY_STOP at QoS 2.
        Activation is grounded in QoS 2 delivery semantics — no application-
        level DCA acknowledgement is expected or waited for.
        """
        payload = json.dumps({
            "event_type":   "EMERGENCY_STOP",
            "issued_by":    issued_by,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_mode_change,
            payload=payload,
            qos=2,
            retain=True,   # Retain so DCAs restarting mid-stop honour it immediately.
        )
        log.warning("emergency_stop_published")

    def publish_resume(self, issued_by: str = "operator") -> None:
        """
        Broadcast RESUME at QoS 2.
        Clears the retained EMERGENCY_STOP by publishing an empty retained message
        first, then RESUME — ensuring DCAs that reconnect after the RESUME do not
        re-enter emergency stop mode.
        """
        # Clear the retained EMERGENCY_STOP.
        self._client.publish(
            topic=self._topic_mode_change,
            payload="",
            qos=2,
            retain=True,
        )
        # Publish RESUME.
        payload = json.dumps({
            "event_type":   "RESUME",
            "issued_by":    issued_by,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_mode_change,
            payload=payload,
            qos=2,
            retain=False,
        )
        log.info("resume_published")

    def publish_heartbeat(self, agent_id: str) -> None:
        """SA liveness signal — QoS 0, every 15 seconds."""
        payload = json.dumps({
            "event_type":   "HEARTBEAT",
            "agent_id":     agent_id,
            "status":       "ALIVE",
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_sa_heartbeat,
            payload=payload,
            qos=0,
        )
