# mra/mqtt_publisher.py
#
# MQTT client — MRA Specification Section 6 (Table 5).
#
# QoS levels per Table 5:
#   BELIEF_UPDATE    → QoS 1 (at-least-once)
#   SCRAPE_FAILED    → QoS 1
#   VALIDATION_ERROR → QoS 1
#   HEARTBEAT        → QoS 0 (fire-and-forget)
#
# Uses paho-mqtt v2 (CallbackAPIVersion.VERSION2 required for v2 API).
# Connection is async — the main thread does not block on connect.
# wait_connected() is called during the startup sequence.
#
import json
import threading
import time
import paho.mqtt.client as mqtt
import structlog
 
log = structlog.get_logger(__name__)
 
 
class MQTTPublisher:
 
    def __init__(
        self,
        host: str,
        port: int,
        client_id: str,
        topic_beliefs: str,
        topic_status: str,
        topic_heartbeat: str,
        username: str = "",
        password: str = "",
    ):
        self._topic_beliefs   = topic_beliefs
        self._topic_status    = topic_status
        self._topic_heartbeat = topic_heartbeat
        self._connected       = threading.Event()
 
        self._client = mqtt.Client(
            client_id=client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        if username:
            self._client.username_pw_set(username, password)
 
        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect
 
        log.info("mqtt_connecting", host=host, port=port, client_id=client_id)
        self._client.connect_async(host, port, keepalive=20)
        self._client.loop_start()
 
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self._connected.set()
            log.info("mqtt_connected")
        else:
            log.error("mqtt_connect_failed", reason_code=str(reason_code))
 
    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self._connected.clear()
        log.warning("mqtt_disconnected", reason_code=str(reason_code))
 
    def wait_connected(self, timeout: float = 30.0) -> bool:
        return self._connected.wait(timeout=timeout)
 
    def on_message_set(self, callback) -> None:
        self._client.on_message = callback
 
    def subscribe(self, topic: str, qos: int = 1) -> None:
        self._client.subscribe(topic, qos=qos)
 
    def publish_belief_update(self, belief_json: str) -> None:
        self._client.publish(
            topic=self._topic_beliefs,
            payload=belief_json,
            qos=1,
            retain=False,
        )
 
    def publish_scrape_failed(self, detail: str) -> None:
        payload = json.dumps({
            "event_type": "SCRAPE_FAILED",
            "detail": detail,
        })
        self._client.publish(topic=self._topic_status, payload=payload, qos=1)
        log.warning("published_scrape_failed", detail=detail)
 
    def publish_validation_error(self, sub_type: str, detail: str) -> None:
        payload = json.dumps({
            "event_type": "VALIDATION_ERROR",
            "sub_type": sub_type,
            "detail": detail,
        })
        self._client.publish(topic=self._topic_status, payload=payload, qos=1)
 
    def publish_heartbeat(self, agent_id: str, status: str = "ALIVE") -> None:
        payload = json.dumps({
            "event_type": "HEARTBEAT",
            "agent_id": agent_id,
            "status": status,
            "timestamp_ms": int(time.time() * 1000),
        })
        self._client.publish(
            topic=self._topic_heartbeat, payload=payload, qos=0
        )
 
    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
