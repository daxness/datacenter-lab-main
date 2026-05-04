# rsa/mqtt_client.py
#
# MQTT client for the RSA — RSA Specification Section 5 and 6.
#
# Subscriptions (what RSA receives):
#   /mas/{ns}/{dep}/mra/beliefs        QoS 1 — BELIEF_UPDATE (emergency + reactive triggers)
#   /mas/{ns}/{dep}/mra/status         QoS 1 — MRA error events (reset MRA missed-cycle counter)
#   /mas/{ns}/{dep}/pfa/forecasts      QoS 1 — FORECAST_UPDATE (proactive trigger)
#   /mas/{ns}/{dep}/pfa/status         QoS 1 — WARMING_UP, INFERENCE_ERROR, INFERENCE_DEGRADED
#   /mas/system/policy/ready           QoS 2 — SA policy broadcast (startup gate)
#   /mas/system/domain/ready           QoS 1 — DCA DOMAIN_READY (startup gate)
#
# Publications (what RSA sends):
#   /mas/{ns}/{dep}/rsa/actions        QoS 1 — SCALING_ACTION (every deliberation outcome)
#   /mas/{ns}/{dep}/rsa/status         QoS 1 — CAPACITY_OVERFLOW, EMERGENCY_INSUFFICIENT,
#                                               CONFLICTING_AUTOSCALER, MRA/PFA DOWN signals
#   /mas/system/heartbeats/...         QoS 0 — liveness signal every 15s
#
# Uses paho-mqtt v2 (CallbackAPIVersion.VERSION2), matching the MRA and PFA.
# UUID suffix on client_id prevents collision on pod restarts.
#
import json
import threading
import time
import uuid
import paho.mqtt.client as mqtt
import structlog

log = structlog.get_logger(__name__)


class RSAMQTTClient:

    def __init__(
        self,
        host: str,
        port: int,
        namespace: str,
        deployment: str,
        topic_mra_beliefs: str,
        topic_mra_status: str,
        topic_pfa_forecasts: str,
        topic_pfa_status: str,
        topic_rsa_actions: str,
        topic_rsa_status: str,
        topic_rsa_heartbeat: str,
        topic_system_policy: str,
        topic_domain_ready: str,
        username: str = "",
        password: str = "",
        on_belief_update=None,
        on_mra_status=None,
        on_forecast_update=None,
        on_pfa_status=None,
        on_policy_ready=None,
        on_domain_ready=None,
    ):
        self._topic_mra_beliefs   = topic_mra_beliefs
        self._topic_mra_status    = topic_mra_status
        self._topic_pfa_forecasts = topic_pfa_forecasts
        self._topic_pfa_status    = topic_pfa_status
        self._topic_rsa_actions   = topic_rsa_actions
        self._topic_rsa_status    = topic_rsa_status
        self._topic_rsa_heartbeat = topic_rsa_heartbeat
        self._topic_policy        = topic_system_policy
        self._topic_domain        = topic_domain_ready

        # Business-logic callbacks injected by main.py
        self._on_belief_update  = on_belief_update
        self._on_mra_status     = on_mra_status
        self._on_forecast_update = on_forecast_update
        self._on_pfa_status     = on_pfa_status
        self._on_policy_ready   = on_policy_ready
        self._on_domain_ready   = on_domain_ready

        self._connected = threading.Event()

        client_id = f"rsa-{namespace}-{deployment}-{uuid.uuid4().hex[:8]}"

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
            # Re-subscribe on every connect so reconnections automatically
            # restore subscriptions without any extra management code.
            client.subscribe(self._topic_mra_beliefs,   qos=1)
            client.subscribe(self._topic_mra_status,    qos=1)
            client.subscribe(self._topic_pfa_forecasts, qos=1)
            client.subscribe(self._topic_pfa_status,    qos=1)
            client.subscribe(self._topic_policy,        qos=2)
            client.subscribe(self._topic_domain,        qos=1)
            log.info("mqtt_subscriptions_registered")
        else:
            log.error("mqtt_connect_failed", reason_code=str(reason_code))

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected.clear()
        log.warning("mqtt_disconnected", reason_code=str(reason_code))

    def _on_message(self, client, userdata, msg):
        """
        Route incoming messages to the appropriate callback.
        Callbacks are defined in main.py and contain all business logic —
        this method is a pure dispatcher with no logic of its own.
        """
        try:
            payload = json.loads(msg.payload)
        except json.JSONDecodeError:
            log.warning("mqtt_bad_json", topic=msg.topic)
            return

        topic = msg.topic

        if topic == self._topic_mra_beliefs:
            if self._on_belief_update:
                self._on_belief_update(payload)

        elif topic == self._topic_mra_status:
            # Any MRA status event resets the missed-cycle counter.
            # The MRA is alive even when it cannot produce valid belief objects.
            if self._on_mra_status:
                self._on_mra_status()

        elif topic == self._topic_pfa_forecasts:
            if self._on_forecast_update:
                self._on_forecast_update(payload)

        elif topic == self._topic_pfa_status:
            if self._on_pfa_status:
                self._on_pfa_status(payload)

        elif topic == self._topic_policy:
            if self._on_policy_ready:
                self._on_policy_ready(payload)

        elif topic == self._topic_domain:
            if self._on_domain_ready:
                self._on_domain_ready(payload)

    # ---- Connection helpers ------------------------------------------------

    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    # ---- Publications -------------------------------------------------------

    def publish_scaling_action(
        self,
        trigger: str,           # "PROACTIVE" | "EMERGENCY" | "REACTIVE_SCALE_DOWN"
        action: str,            # "SCALE_UP" | "SCALE_DOWN" | "DO_NOTHING"
        target_replicas: int | None,
        current_replicas: int,
        detail: dict | None = None,
    ) -> None:
        payload = {
            "event_type":       "SCALING_ACTION",
            "namespace":        self._topic_mra_beliefs.split("/")[2],
            "deployment":       self._topic_mra_beliefs.split("/")[3],
            "trigger":          trigger,
            "action":           action,
            "target_replicas":  target_replicas,
            "current_replicas": current_replicas,
            "timestamp_ms":     int(time.time() * 1000),
        }
        if detail:
            payload.update(detail)
        self._client.publish(
            topic=self._topic_rsa_actions,
            payload=json.dumps(payload),
            qos=1,
            retain=False,
        )
        log.info("scaling_action_published", trigger=trigger, action=action,
                 target=target_replicas, current=current_replicas)

    def publish_status(self, event_type: str, detail: dict) -> None:
        """Generic status publication for escalations and peer signals."""
        payload = json.dumps({
            "event_type":   event_type,
            "timestamp_ms": int(time.time() * 1000),
            **detail,
        })
        self._client.publish(
            topic=self._topic_rsa_status,
            payload=payload,
            qos=1,
        )
        log.warning("rsa_status_published", event_type=event_type)

    def publish_heartbeat(self, agent_id: str) -> None:
        payload = json.dumps({
            "event_type":   "HEARTBEAT",
            "agent_id":     agent_id,
            "status":       "ALIVE",
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_rsa_heartbeat,
            payload=payload,
            qos=0,
        )

