# mra/config.py
#
# Central configuration for the Monitor and Resource Agent.
#
# Every value that differs between environments (Prometheus URL, MQTT host,
# thresholds) is read from environment variables. This means the same Docker
# image works for every MRA instance — only the env vars change between the
# mra-nginx, mra-redis, and mra-stress-ng deployments.
#
# Default values here match your K3D/WSL2 cluster exactly:
#   - Prometheus: monitoring-kube-prometheus-prometheus (from helm install monitoring)
#   - MQTT: mosquitto.mas-system.svc.cluster.local
#   - Domain 4 path: /mnt/kb/domain4/audit_log.db (K3D volume mount)
#
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
 
load_dotenv()
 
 
@dataclass(frozen=True)
class MRAConfig:
    """
    Immutable configuration snapshot. frozen=True prevents accidental
    mutation after the config is loaded at startup.
    """
 
    # ---- Deployment identity ----
    namespace: str = field(
        default_factory=lambda: os.environ["MRA_NAMESPACE"]
    )
    deployment_name: str = field(
        default_factory=lambda: os.environ["MRA_DEPLOYMENT"]
    )
 
    # ---- Prometheus ----
    # Default matches the service name produced by your helm install command:
    #   helm upgrade --install monitoring prometheus-community/kube-prometheus-stack
    prometheus_url: str = field(
        default_factory=lambda: os.getenv(
            "PROMETHEUS_URL",
            "http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090",
        )
    )
    scrape_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("SCRAPE_INTERVAL", "30"))
    )
    cpu_rate_window: str = field(
        default_factory=lambda: os.getenv("CPU_RATE_WINDOW", "2m")
    )
 
    # ---- MQTT ----
    # Service name: mosquitto.mas-system.svc.cluster.local (from mosquitto.yaml)
    mqtt_host: str = field(
        default_factory=lambda: os.getenv(
            "MQTT_HOST", "mosquitto.mas-system.svc.cluster.local"
        )
    )
    mqtt_port: int = field(
        default_factory=lambda: int(os.getenv("MQTT_PORT", "1883"))
    )
    mqtt_client_id: str = field(
        default_factory=lambda: os.getenv(
            "MQTT_CLIENT_ID",
            f"mra-{os.getenv('MRA_NAMESPACE', 'default')}-{os.getenv('MRA_DEPLOYMENT', 'unknown')}",
        )
    )
    mqtt_username: str = field(
        default_factory=lambda: os.getenv("MQTT_USERNAME", "mra")
    )
    mqtt_password: str = field(
        default_factory=lambda: os.getenv("MQTT_PASSWORD", "")
    )
 
    # ---- MQTT topics (computed from namespace + deployment_name) ----
    @property
    def topic_beliefs(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/mra/beliefs"
 
    @property
    def topic_status(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/mra/status"
 
    @property
    def topic_heartbeat(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/mra/heartbeat"
 
    @property
    def topic_system_policy(self) -> str:
        return "/mas/system/policy/ready"
 
    @property
    def topic_domain_ready(self) -> str:
        return "/mas/system/domain/ready"
 
    # ---- Pressure thresholds ----
    # Loaded from Domain 1 policy at Stage 1 of startup.
    # These env var defaults match domain1-policy.yaml exactly.
    cpu_warning_pct: float = field(
        default_factory=lambda: float(os.getenv("CPU_WARNING_PCT", "0.70"))
    )
    cpu_critical_pct: float = field(
        default_factory=lambda: float(os.getenv("CPU_CRITICAL_PCT", "0.85"))
    )
    memory_warning_pct: float = field(
        default_factory=lambda: float(os.getenv("MEMORY_WARNING_PCT", "0.75"))
    )
    memory_critical_pct: float = field(
        default_factory=lambda: float(os.getenv("MEMORY_CRITICAL_PCT", "0.90"))
    )
    warning_consecutive_threshold: int = field(
        default_factory=lambda: int(os.getenv("WARNING_CONSECUTIVE", "2"))
    )
 
    # ---- Knowledge Base ----
    # /mnt/kb is the K3D volume mount defined in k3d-config.yaml volumes:
    domain4_db_path: str = field(
        default_factory=lambda: os.getenv(
            "DOMAIN4_DB_PATH", "/mnt/kb/domain4/audit_log.db"
        )
    )
 
    # ---- Lifecycle ----
    heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("HEARTBEAT_INTERVAL", "15"))
    )
    startup_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("STARTUP_TIMEOUT", "120"))
    )
 
    # ---- Metrics server port (Domain 3 write path) ----
    metrics_port: int = field(
        default_factory=lambda: int(os.getenv("METRICS_PORT", "8000"))
    )
