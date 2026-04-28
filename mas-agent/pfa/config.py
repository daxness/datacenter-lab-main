# pfa/config.py
#
# Central configuration for the Predictive Forecasting Agent.
#
# Every value that differs between deployments is read from environment
# variables so the same Docker image serves pfa-nginx, pfa-redis,
# and pfa-stress-ng with no rebuild — only env vars change.
#
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PFAConfig:
    """Immutable configuration snapshot loaded once at PFA startup."""

    # ---- Deployment identity ----
    namespace: str = field(
        default_factory=lambda: os.environ["PFA_NAMESPACE"]
    )
    deployment_name: str = field(
        default_factory=lambda: os.environ["PFA_DEPLOYMENT"]
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
        default_factory=lambda: os.getenv("MQTT_USERNAME", "pfa")
    )
    mqtt_password: str = field(
        default_factory=lambda: os.getenv("MQTT_PASSWORD", "")
    )

    # ---- MQTT topics (Spec Section 5 and 6) ----
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
    def topic_pfa_heartbeat(self) -> str:
        return f"/mas/system/heartbeats/{self.namespace}/{self.deployment_name}/pfa"

    @property
    def topic_system_policy(self) -> str:
        return "/mas/system/policy/ready"

    @property
    def topic_domain_ready(self) -> str:
        return "/mas/system/domain/ready"

    # ---- Sliding window parameters (Spec Section 4.2) ----
    window_max_size: int = field(
        default_factory=lambda: int(os.getenv("WINDOW_MAX_SIZE", "96"))
    )
    window_min_inference: int = field(
        default_factory=lambda: int(os.getenv("WINDOW_MIN_INFERENCE", "32"))
    )
    patch_size: int = field(
        default_factory=lambda: int(os.getenv("PATCH_SIZE", "32"))
    )

    # ---- Inference triggering (Spec Section 4.1) ----
    min_inference_gap_seconds: int = field(
        default_factory=lambda: int(os.getenv("MIN_INFERENCE_GAP", "90"))
    )
    periodic_inference_seconds: int = field(
        default_factory=lambda: int(os.getenv("PERIODIC_INFERENCE", "900"))
    )

    # ---- MRA liveness monitoring (Spec Section 7.1) ----
    mra_scrape_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("MRA_SCRAPE_INTERVAL", "30"))
    )
    mra_suspected_threshold: int = field(
        default_factory=lambda: int(os.getenv("MRA_SUSPECTED_THRESHOLD", "3"))
    )
    mra_confirmed_threshold: int = field(
        default_factory=lambda: int(os.getenv("MRA_CONFIRMED_THRESHOLD", "5"))
    )

    # ---- Data quality (Spec Section 4.2) ----
    max_fill_ratio: float = field(
        default_factory=lambda: float(os.getenv("MAX_FILL_RATIO", "0.5"))
    )
    degraded_consecutive_threshold: int = field(
        default_factory=lambda: int(os.getenv("DEGRADED_THRESHOLD", "3"))
    )

    # ---- Breach thresholds (Spec Section 4.5) ----
    cpu_warning_pct: float = field(
        default_factory=lambda: float(os.getenv("CPU_WARNING_PCT", "0.70"))
    )
    memory_warning_pct: float = field(
        default_factory=lambda: float(os.getenv("MEMORY_WARNING_PCT", "0.75"))
    )

    # ---- Knowledge Base ----
    prometheus_url: str = field(
        default_factory=lambda: os.getenv(
            "PROMETHEUS_URL",
            "http://monitoring-kube-prometheus-prometheus.monitoring.svc.cluster.local:9090",
        )
    )
    domain4_db_path: str = field(
        default_factory=lambda: os.getenv(
            "DOMAIN4_DB_PATH", "/mnt/kb/domain4/audit_log.db"
        )
    )

    # ---- Lifecycle ----
    heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("HEARTBEAT_INTERVAL", "30"))
    )
    startup_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("STARTUP_TIMEOUT", "120"))
    )
