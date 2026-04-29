# pfa/mqtt_client.py
#
# MQTT client for the PFA — Specification Section 5 and 6.
#
# Subscriptions (what PFA receives):
#   /mas/{ns}/{dep}/mra/beliefs   QoS 1 — BELIEF_UPDATE from MRA
#   /mas/{ns}/{dep}/mra/status    QoS 1 — MRA error events
#   /mas/system/policy/ready      QoS 2 — SA policy broadcast
#   /mas/system/domain/ready      QoS 1 — DCA DOMAIN_READY
#
# Publications (what PFA sends):
#   /mas/{ns}/{dep}/pfa/forecasts QoS 1 — FORECAST_UPDATE
#   /mas/{ns}/{dep}/pfa/status    QoS 1 — WARMING_UP, INFERENCE_ERROR, etc.
#   /mas/system/heartbeats/...    QoS 0 — liveness signal
#
import json
import threading
import time
import uuid
import paho.mqtt.client as mqtt
import structlog

log = structlog.get_logger(__name__)


class PFAMQTTClient:

    def __init__(
        self,
        host: str,
        port: int,
        namespace: str,
        deployment: str,
        topic_mra_beliefs: str,
        topic_mra_status: str,
        topic_forecasts: str,
        topic_status: str,
        topic_heartbeat: str,
        topic_system_policy: str,
        topic_domain_ready: str,
        username: str = "",
        password: str = "",
        on_belief_update=None,
        on_mra_status=None,
        on_policy_ready=None,
        on_domain_ready=None,
    ):
        self._topic_forecasts  = topic_forecasts
        self._topic_status     = topic_status
        self._topic_heartbeat  = topic_heartbeat
        self._topic_mra_beliefs = topic_mra_beliefs
        self._topic_mra_status  = topic_mra_status
        self._topic_policy     = topic_system_policy
        self._topic_domain     = topic_domain_ready

        # Callbacks injected by main.py — called when messages arrive
        self._on_belief_update = on_belief_update
        self._on_mra_status    = on_mra_status
        self._on_policy_ready  = on_policy_ready
        self._on_domain_ready  = on_domain_ready

        self._connected = threading.Event()

        # UUID suffix prevents client-ID collision across pod restarts
        client_id = f"pfa-{namespace}-{deployment}-{uuid.uuid4().hex[:8]}"

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
            # Subscribe to all required topics immediately on connect
            # so reconnections automatically re-establish subscriptions
            client.subscribe(self._topic_mra_beliefs, qos=1)
            client.subscribe(self._topic_mra_status,  qos=1)
            client.subscribe(self._topic_policy,       qos=2)
            client.subscribe(self._topic_domain,       qos=1)
            log.info("mqtt_subscriptions_registered")
        else:
            log.error("mqtt_connect_failed", reason_code=str(reason_code))

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected.clear()
        log.warning("mqtt_disconnected", reason_code=str(reason_code))

    def _on_message(self, client, userdata, msg):
        """
        Route incoming messages to the appropriate callback.
        Each callback is defined in main.py and contains the business logic.
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
            if self._on_mra_status:
                self._on_mra_status()

        elif topic == self._topic_policy:
            if self._on_policy_ready:
                self._on_policy_ready(payload)

        elif topic == self._topic_domain:
            if self._on_domain_ready:
                self._on_domain_ready(payload)

    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)

    def publish_forecast(self, forecast_json: str) -> None:
        self._client.publish(
            topic=self._topic_forecasts,
            payload=forecast_json,
            qos=1,
            retain=False,
        )

    def publish_status(self, event_type: str, detail: dict) -> None:
        payload = json.dumps({"event_type": event_type, **detail})
        self._client.publish(
            topic=self._topic_status, payload=payload, qos=1
        )

    def publish_heartbeat(self, agent_id: str) -> None:
        payload = json.dumps({
            "event_type": "HEARTBEAT",
            "agent_id":   agent_id,
            "status":     "ALIVE",
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_heartbeat, payload=payload, qos=0
        )

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
