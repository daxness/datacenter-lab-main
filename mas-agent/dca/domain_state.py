# dca/domain_state.py
#
# Domain State — DCA Specification Section 4.1.
#
# Maintains the two in-memory belief structures that give the DCA a
# unified, real-time view of the entire managed domain:
#
#   Deployment Pressure Map:
#     Updated from every incoming MRA BELIEF_UPDATE. Holds the current
#     pressure level (NORMAL | WARNING | CRITICAL | UNCONFIGURED) for each
#     managed deployment. The DCA reads this map when composing enriched
#     SA_NOTIFICATIONs so the operator receives full domain context
#     alongside any individual alert.
#
#   Forecast Risk Map:
#     Updated from every incoming PFA FORECAST_UPDATE. Holds the
#     per-deployment PFA operational state, time-to-breach estimates for
#     both CPU and memory, and associated uncertainty scores. The DCA uses
#     this to enrich EMERGENCY_INSUFFICIENT notifications with predictive
#     risk context and to maintain its Forecast Risk Map belief.
#
#   System Mode:
#     Domain-wide operating mode: NORMAL | DEGRADED | EMERGENCY_STOP.
#     Persisted to Domain 5 so a DCA restart during EMERGENCY_STOP
#     immediately re-enforces the stop across all RSA instances.
#     Modified only by SA EMERGENCY_STOP and RESUME commands.
#
# Threading note: all map updates happen inside the MQTT callback thread;
# reads can happen from the background heartbeat-check thread. A single
# RLock protects the entire state structure.
#
import threading
import time
import structlog

log = structlog.get_logger(__name__)

SYSTEM_MODE_NORMAL         = "NORMAL"
SYSTEM_MODE_DEGRADED       = "DEGRADED"
SYSTEM_MODE_EMERGENCY_STOP = "EMERGENCY_STOP"


class DomainState:
    """
    In-memory domain belief state for the DCA.
    All reads and writes are protected by a single re-entrant lock.
    """

    def __init__(self, managed_deployments: list):
        self._lock = threading.RLock()

        # Deployment Pressure Map: {deployment_name: pressure_level_str}
        self._pressure_map: dict[str, str] = {
            dep: "NORMAL" for dep in managed_deployments
        }

        # Forecast Risk Map: {deployment_name: forecast_context_dict}
        self._forecast_risk_map: dict[str, dict] = {
            dep: {
                "pfa_state":            "UNKNOWN",
                "cpu_time_to_breach_s": None,
                "mem_time_to_breach_s": None,
                "cpu_uncertainty":      None,
                "mem_uncertainty":      None,
                "last_updated_ms":      None,
            }
            for dep in managed_deployments
        }

        # System mode — default NORMAL on fresh start
        self._system_mode: str = SYSTEM_MODE_NORMAL

        # Escalation history for rate-limiting SA notifications.
        # Structure: {(deployment, signal_type): last_escalation_timestamp_s}
        self._escalation_history: dict[tuple, float] = {}

    # ---- Pressure Map --------------------------------------------------------

    def update_pressure(self, deployment: str, pressure_level: str) -> None:
        with self._lock:
            previous = self._pressure_map.get(deployment)
            self._pressure_map[deployment] = pressure_level
            if previous != pressure_level:
                log.info(
                    "pressure_level_changed",
                    deployment=deployment,
                    from_level=previous,
                    to_level=pressure_level,
                )

    def get_pressure(self, deployment: str) -> str:
        with self._lock:
            return self._pressure_map.get(deployment, "UNKNOWN")

    def pressure_snapshot(self) -> dict:
        with self._lock:
            return dict(self._pressure_map)

    # ---- Forecast Risk Map ---------------------------------------------------

    def update_forecast_risk(self, deployment: str, forecast_obj: dict) -> None:
        """
        Extract and store risk-relevant fields from a PFA FORECAST_UPDATE.
        The DCA only reads the fields it needs for enrichment — it does not
        store the full forecast object (that belongs to the PFA→RSA pipeline).
        """
        with self._lock:
            cpu_breach = forecast_obj.get("cpu", {}).get("breach", {})
            mem_breach = forecast_obj.get("memory", {}).get("breach", {})

            self._forecast_risk_map[deployment] = {
                "pfa_state":            "OPERATIONAL",
                "cpu_time_to_breach_s": cpu_breach.get("time_to_breach_s"),
                "mem_time_to_breach_s": mem_breach.get("time_to_breach_s"),
                "cpu_uncertainty":      forecast_obj.get("cpu", {}).get("uncertainty"),
                "mem_uncertainty":      forecast_obj.get("memory", {}).get("uncertainty"),
                "last_updated_ms":      int(time.time() * 1000),
            }

    def mark_pfa_degraded(self, deployment: str, reason: str) -> None:
        with self._lock:
            if deployment in self._forecast_risk_map:
                self._forecast_risk_map[deployment]["pfa_state"] = f"DEGRADED:{reason}"

    def forecast_risk_snapshot(self) -> dict:
        with self._lock:
            return {k: dict(v) for k, v in self._forecast_risk_map.items()}

    # ---- System Mode --------------------------------------------------------

    def set_system_mode(self, mode: str) -> None:
        with self._lock:
            previous = self._system_mode
            self._system_mode = mode
            log.warning(
                "system_mode_changed",
                from_mode=previous,
                to_mode=mode,
            )

    def get_system_mode(self) -> str:
        with self._lock:
            return self._system_mode

    def is_emergency_stop(self) -> bool:
        with self._lock:
            return self._system_mode == SYSTEM_MODE_EMERGENCY_STOP

    # ---- Escalation Rate Limiting ------------------------------------------

    def should_escalate(self, deployment: str, signal_type: str,
                        suppression_window_s: int) -> bool:
        """
        Returns True if a SA_NOTIFICATION for this (deployment, signal_type)
        should be sent. Returns False if the same signal was already escalated
        within the suppression window.

        Three signal types always return True regardless of history:
          EMERGENCY_INSUFFICIENT, RSA_LIVENESS_FAILURE, CONFLICTING_AUTOSCALER
        """
        bypass_types = {
            "EMERGENCY_INSUFFICIENT",
            "RSA_LIVENESS_FAILURE",
            "CONFLICTING_AUTOSCALER",
        }
        if signal_type in bypass_types:
            return True

        with self._lock:
            key = (deployment, signal_type)
            last_sent = self._escalation_history.get(key, 0.0)
            now = time.time()
            if (now - last_sent) >= suppression_window_s:
                self._escalation_history[key] = now
                return True
            return False

    def record_escalation(self, deployment: str, signal_type: str) -> None:
        """Explicitly record that an escalation was sent (for bypass types)."""
        with self._lock:
            self._escalation_history[(deployment, signal_type)] = time.time()
