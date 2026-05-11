# sa/config.py
#
# Central configuration for the Supervision Agent (SA).
#
# The SA is a singleton — one instance per cluster partition. Unlike the
# MRA, PFA, and RSA (which have one instance per managed deployment), the
# SA has no "namespace" or "deployment" identity of its own. Its identity
# is "SA:global" throughout the system.
#
# All values are read from environment variables so the same Docker image
# works in any cluster without a rebuild. Defaults match the K3D/WSL2 lab:
#   - MQTT:     mosquitto.mas-system.svc.cluster.local
#   - Domain 4: /mnt/kb/domain4/audit_log.db
#   - Kubernetes namespace: mas-system
#
# Pattern: frozen dataclass with field(default_factory=...) for env reads.
# This is identical to rsa/config.py — do not deviate.
#
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SAConfig:
    """
    Immutable configuration snapshot loaded once at SA startup.
    frozen=True prevents accidental mutation after the SA is running.
    """

    # ---- Agent identity ----
    # The SA is always "SA:global" — there is exactly one SA per partition.
    agent_id: str = field(
        default_factory=lambda: os.getenv("SA_AGENT_ID", "SA:global")
    )

    # Domain identifier the SA governs (matches DCA domain IDs).
    # In the single-namespace K3D lab this is "worker".
    domain_id: str = field(
        default_factory=lambda: os.getenv("SA_DOMAIN_ID", "worker")
    )

    # ---- MQTT ----
    mqtt_host: str = field(
        default_factory=lambda: os.getenv(
            "MQTT_HOST", "mosquitto.mas-system.svc.cluster.local"
        )
    )
    mqtt_port: int = field(
        default_factory=lambda: int(os.getenv("MQTT_PORT", "1883"))
    )
    mqtt_username: str = field(
        default_factory=lambda: os.getenv("MQTT_USERNAME", "sa")
    )
    mqtt_password: str = field(
        default_factory=lambda: os.getenv("MQTT_PASSWORD", "")
    )

    # ---- MQTT topics (static — SA is global, not deployment-scoped) ----
    @property
    def topic_policy_ready(self) -> str:
        # QoS 2 broadcast consumed by ALL agents. Matches what RSA/MRA/PFA subscribe to.
        return "/mas/system/policy/ready"

    @property
    def topic_mode_change(self) -> str:
        # EMERGENCY_STOP and RESUME land here. DCAs subscribe at QoS 2.
        return "/mas/system/modechange"

    @property
    def topic_sa_heartbeat(self) -> str:
        return "/mas/system/heartbeats/sa"

    @property
    def topic_dca_heartbeat(self) -> str:
        # Wildcard: matches heartbeats from all DCA instances.
        # The DCA publishes on /mas/system/heartbeats/dca/<domain-id>
        return "/mas/system/heartbeats/dca/#"

    @property
    def topic_dca_escalation(self) -> str:
        # SA_NOTIFICATION escalations from all DCAs.
        # Pattern: /mas/system/domain/<domain-id>/dca/escalation
        return "/mas/system/domain/+/dca/escalation"

    @property
    def topic_dca_status(self) -> str:
        # DOMAIN_STATUS_REPORT (30-minute summary) from all DCAs.
        return "/mas/system/domain/+/dca/status"

    @property
    def topic_dca_control(self) -> str:
        # DOMAIN_READY from DCA — the SA uses this to confirm DCA registered.
        return f"/mas/system/domain/{self.domain_id}/dca/control"

    # ---- Kubernetes ----
    k8s_namespace: str = field(
        default_factory=lambda: os.getenv("K8S_NAMESPACE", "mas-system")
    )
    # Name of the ConfigMap that stores Domain 1 (policy).
    policy_configmap_name: str = field(
        default_factory=lambda: os.getenv("POLICY_CONFIGMAP", "mas-policy")
    )
    # Name of the ConfigMap that stores Domain 6 (oversight / version history).
    oversight_configmap_name: str = field(
        default_factory=lambda: os.getenv("OVERSIGHT_CONFIGMAP", "mas-oversight")
    )
    # Name of the ConfigMap that stores Domain 2 (topology — SA reads only).
    topology_configmap_name: str = field(
        default_factory=lambda: os.getenv("TOPOLOGY_CONFIGMAP", "mas-topology")
    )

    # ---- Knowledge Base ----
    domain4_db_path: str = field(
        default_factory=lambda: os.getenv(
            "DOMAIN4_DB_PATH", "/mnt/kb/domain4/audit_log.db"
        )
    )

    # ---- DCA supervision ----
    # A DCA is considered failed after this many consecutive missed heartbeat
    # intervals. Each interval is heartbeat_interval_seconds (15s by default),
    # so 3 × 15 = 45 seconds before failure is declared.
    dca_missed_threshold: int = field(
        default_factory=lambda: int(os.getenv("DCA_MISSED_THRESHOLD", "3"))
    )
    dca_heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("DCA_HEARTBEAT_INTERVAL", "15"))
    )

    # ---- Lifecycle ----
    heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("HEARTBEAT_INTERVAL", "15"))
    )
    startup_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("STARTUP_TIMEOUT", "120"))
    )

    # ---- Web UI ----
    # Port on which the FastAPI operator interface listens inside the pod.
    # Exposed via a NodePort Service so the operator can reach it from a browser.
    api_port: int = field(
        default_factory=lambda: int(os.getenv("SA_API_PORT", "8080"))
    )
    api_host: str = field(
        default_factory=lambda: os.getenv("SA_API_HOST", "0.0.0.0")
    )
