# rsa/config.py
#
# Central configuration for the Resource Scaling Agent.
#
# Every value that differs between deployments is read from environment
# variables so the same Docker image serves rsa-nginx, rsa-redis, and
# rsa-stress-ng with no rebuild — only env vars change between instances.
#
# Default values match the K3D/WSL2 cluster exactly:
#   - MQTT:     mosquitto.mas-system.svc.cluster.local (from mosquitto.yaml)
#   - Domain 4: /mnt/kb/domain4/audit_log.db
#   - Domain 5: /mnt/kb/domain5/coordination.db
#   - Domain 2: mas-topology ConfigMap in mas-system namespace
#
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class RSAConfig:
    """
    Immutable configuration snapshot loaded once at RSA startup.
    frozen=True prevents accidental mutation after startup.
    """

    # ---- Deployment identity ----
    namespace: str = field(
        default_factory=lambda: os.environ["RSA_NAMESPACE"]
    )
    deployment_name: str = field(
        default_factory=lambda: os.environ["RSA_DEPLOYMENT"]
    )
    domain: str = field(
        default_factory=lambda: os.getenv("RSA_DOMAIN", "worker")
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
        default_factory=lambda: os.getenv("MQTT_USERNAME", "rsa")
    )
    mqtt_password: str = field(
        default_factory=lambda: os.getenv("MQTT_PASSWORD", "")
    )

    # ---- MQTT topics (computed from namespace + deployment_name) ----
    @property
    def topic_mra_beliefs(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/mra/beliefs"

    @property
    def topic_mra_status(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/mra/status"

    @property
    def topic_pfa_forecasts(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/pfa/forecasts"

    @property
    def topic_pfa_status(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/pfa/status"

    @property
    def topic_rsa_actions(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/rsa/actions"

    @property
    def topic_rsa_status(self) -> str:
        return f"/mas/{self.namespace}/{self.deployment_name}/rsa/status"

    @property
    def topic_rsa_heartbeat(self) -> str:
        return f"/mas/system/heartbeats/{self.namespace}/{self.deployment_name}/rsa"

    @property
    def topic_system_policy(self) -> str:
        return "/mas/system/policy/ready"

    @property
    def topic_domain_ready(self) -> str:
        return "/mas/system/domain/ready"

    # ---- Scaling thresholds (proactive path) ----
    # These are loaded from Domain 1 policy at Stage 1 of startup.
    # The env var values here are the fallback defaults.
    scale_up_delta_threshold: float = field(
        default_factory=lambda: float(os.getenv("SCALE_UP_DELTA_THRESHOLD", "0.15"))
    )
    confidence_margin_min: float = field(
        default_factory=lambda: float(os.getenv("CONFIDENCE_MARGIN_MIN", "0.15"))
    )
    confidence_margin_max: float = field(
        default_factory=lambda: float(os.getenv("CONFIDENCE_MARGIN_MAX", "0.30"))
    )

    # ---- Replica bounds (seeded from Domain 2 topology — per-deployment) ----
    min_replicas: int = field(
        default_factory=lambda: int(os.getenv("MIN_REPLICAS", "1"))
    )
    max_replicas: int = field(
        default_factory=lambda: int(os.getenv("MAX_REPLICAS", "6"))
    )

    # ---- Per-replica resource cost (from Domain 2 topology) ----
    # These are the resource REQUEST values per replica, used in the
    # sizing pipeline (Little's Law calculation). Sourced from domain2-topology.yaml.
    cpu_request_per_replica: float = field(
        default_factory=lambda: float(os.getenv("CPU_REQUEST_PER_REPLICA", "50.0"))
    )
    memory_request_per_replica: float = field(
        default_factory=lambda: float(os.getenv("MEMORY_REQUEST_PER_REPLICA", "32.0"))
    )

    # ---- Reactive scale-down ----
    low_pressure_sustain_count: int = field(
        default_factory=lambda: int(os.getenv("LOW_PRESSURE_SUSTAIN_COUNT", "10"))
    )
    scale_down_target_utilization: float = field(
        default_factory=lambda: float(os.getenv("SCALE_DOWN_TARGET_UTILIZATION", "0.60"))
    )

    # ---- Emergency path ----
    critical_confirm_count: int = field(
        default_factory=lambda: int(os.getenv("CRITICAL_CONFIRM_COUNT", "2"))
    )

    # ---- Cooldown ----
    cooldown_seconds: int = field(
        default_factory=lambda: int(os.getenv("COOLDOWN_SECONDS", "60"))
    )

    # ---- MRA peer monitoring (mirrors PFA MRAMonitor thresholds) ----
    mra_scrape_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("MRA_SCRAPE_INTERVAL", "30"))
    )
    mra_suspected_threshold: int = field(
        default_factory=lambda: int(os.getenv("MRA_SUSPECTED_THRESHOLD", "3"))
    )
    mra_confirmed_threshold: int = field(
        default_factory=lambda: int(os.getenv("MRA_CONFIRMED_THRESHOLD", "5"))
    )

    # ---- PFA peer monitoring ----
    # PFA natural max silence = 15 min periodic timer.
    # 17 min = 15 min + 2 min buffer → SUSPECTED
    # 20 min = 17 min + 3 min → CONFIRMED
    pfa_suspected_seconds: int = field(
        default_factory=lambda: int(os.getenv("PFA_SUSPECTED_SECONDS", "1020"))
    )
    pfa_confirmed_seconds: int = field(
        default_factory=lambda: int(os.getenv("PFA_CONFIRMED_SECONDS", "1200"))
    )

    # ---- Knowledge Base ----
    domain4_db_path: str = field(
        default_factory=lambda: os.getenv(
            "DOMAIN4_DB_PATH", "/mnt/kb/domain4/audit_log.db"
        )
    )
    domain5_db_path: str = field(
        default_factory=lambda: os.getenv(
            "DOMAIN5_DB_PATH", "/mnt/kb/domain5/coordination.db"
        )
    )

    # ---- Lifecycle ----
    heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("HEARTBEAT_INTERVAL", "15"))
    )
    startup_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("STARTUP_TIMEOUT", "120"))
    )

