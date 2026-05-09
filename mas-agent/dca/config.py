# dca/config.py
#
# Central configuration for the Domain Coordinator Agent (DCA).
#
# The DCA is a domain-scoped agent (one per Kubernetes namespace), unlike the
# MRA, PFA, and RSA which are deployment-scoped. Its identity is defined by
# the domain it manages (e.g. "worker") rather than by a deployment name.
#
# All values are read from environment variables so the same Docker image
# serves the worker domain DCA with no rebuild. Defaults match the
# K3D/WSL2 cluster exactly.
#
# Convention: frozen=True dataclass, @property topics, os.getenv() with
# string defaults. Identical pattern to RSA and PFA config.py files.
#
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DCAConfig:
    """
    Immutable configuration snapshot loaded once at DCA startup.
    frozen=True prevents accidental mutation after startup.
    """

    # ---- Domain identity ----
    # The DCA manages one Kubernetes namespace. All agents in that namespace
    # are supervised by this DCA instance.
    domain_id: str = field(
        default_factory=lambda: os.getenv("DCA_DOMAIN_ID", "worker")
    )
    managed_namespace: str = field(
        default_factory=lambda: os.getenv("DCA_NAMESPACE", "workloads")
    )
    # Names of deployments this DCA supervises (comma-separated)
    managed_deployments: str = field(
        default_factory=lambda: os.getenv(
            "DCA_MANAGED_DEPLOYMENTS", "stress-ng,nginx,redis"
        )
    )

    @property
    def deployment_list(self) -> list:
        """Return managed_deployments as a Python list."""
        return [d.strip() for d in self.managed_deployments.split(",") if d.strip()]

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
        default_factory=lambda: os.getenv("MQTT_USERNAME", "dca")
    )
    mqtt_password: str = field(
        default_factory=lambda: os.getenv("MQTT_PASSWORD", "")
    )

    # ---- MQTT topics — publications (DCA → all agents) ----
    # DOMAIN_READY: gates all Tier-2 agents from entering operational state.
    @property
    def topic_domain_ready(self) -> str:
        return "/mas/system/domain/ready"

    # AGENT_DOWN: broadcast when an agent failure is confirmed.
    @property
    def topic_agent_down(self) -> str:
        return f"/mas/system/domain/{self.domain_id}/dca/control"

    # SA_NOTIFICATION: enriched escalation forwarded to operator.
    @property
    def topic_sa_notification(self) -> str:
        return f"/mas/system/domain/{self.domain_id}/dca/escalation"

    # DOMAIN_STATUS_REPORT: 30-minute periodic health summary.
    @property
    def topic_domain_status(self) -> str:
        return f"/mas/system/domain/{self.domain_id}/dca/status"

    # DCA heartbeat — allows SA to detect DCA failure independently.
    @property
    def topic_dca_heartbeat(self) -> str:
        return f"/mas/system/heartbeats/dca/{self.domain_id}"

    # COORDINATION_EVENT: audit records written on every coordination decision.
    @property
    def topic_coordination_audit(self) -> str:
        return f"/mas/system/domain/{self.domain_id}/dca/audit"

    # ---- MQTT topics — subscriptions (all agents → DCA) ----
    # These are wildcard topic builders per deployment.
    def topic_mra_beliefs(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/mra/beliefs"

    def topic_mra_status(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/mra/status"

    def topic_mra_heartbeat(self, deployment: str) -> str:
        return f"/mas/system/heartbeats/{self.managed_namespace}/{deployment}/mra"

    def topic_pfa_forecasts(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/pfa/forecasts"

    def topic_pfa_status(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/pfa/status"

    def topic_pfa_heartbeat(self, deployment: str) -> str:
        return f"/mas/system/heartbeats/{self.managed_namespace}/{deployment}/pfa"

    def topic_rsa_actions(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/rsa/actions"

    def topic_rsa_status(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/rsa/status"

    def topic_rsa_heartbeat(self, deployment: str) -> str:
        return f"/mas/system/heartbeats/{self.managed_namespace}/{deployment}/rsa"

    # SA control channel (EMERGENCY_STOP, RESUME, POLICY_READY)
    @property
    def topic_sa_policy(self) -> str:
        return "/mas/system/policy/ready"

    @property
    def topic_sa_modechange(self) -> str:
        return "/mas/system/modechange"

    # EMERGENCY_STOP_FWD: relayed from SA to all RSA instances per deployment.
    def topic_emergency_stop_fwd(self, deployment: str) -> str:
        return f"/mas/{self.managed_namespace}/{deployment}/dca/control"

    # ---- Failure detection thresholds ----
    # L1: heartbeat-based
    mra_heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("MRA_HEARTBEAT_INTERVAL", "30"))
    )
    mra_missed_threshold: int = field(
        # 3 missed heartbeats = 90 seconds
        default_factory=lambda: int(os.getenv("MRA_MISSED_THRESHOLD", "3"))
    )
    pfa_heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("PFA_HEARTBEAT_INTERVAL", "30"))
    )
    pfa_missed_threshold: int = field(
        default_factory=lambda: int(os.getenv("PFA_MISSED_THRESHOLD", "3"))
    )
    rsa_heartbeat_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("RSA_HEARTBEAT_INTERVAL", "15"))
    )
    rsa_missed_threshold: int = field(
        # 3 missed × 15s = 45s. Tighter because RSA has no L2 peer observer.
        default_factory=lambda: int(os.getenv("RSA_MISSED_THRESHOLD", "3"))
    )

    # Mode B recovery: seconds to wait for first post-restart heartbeat
    recovery_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("RECOVERY_TIMEOUT", "60"))
    )

    # ---- Escalation deduplication (rate limiting) ----
    # SA notifications for the same (deployment, signal_type) are suppressed
    # within this window. Three signal types bypass this: EMERGENCY_INSUFFICIENT,
    # RSA liveness failure, CONFLICTING_AUTOSCALER.
    escalation_suppression_window_seconds: int = field(
        default_factory=lambda: int(os.getenv("ESCALATION_SUPPRESSION_WINDOW", "300"))
    )

    # ---- Domain status report cadence ----
    status_report_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("STATUS_REPORT_INTERVAL", "1800"))
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
