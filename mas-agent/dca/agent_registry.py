# dca/agent_registry.py
#
# Agent Registry — DCA Specification Section 4.1 and Section 4.3.
#
# This module owns the in-memory liveness state of every MRA, PFA, and RSA
# instance supervised by this DCA. It implements the dual-layer failure
# detection mechanism described in the specification:
#
#   L1 — Heartbeat absence:
#     Each agent type has a defined heartbeat interval. When the DCA has not
#     received a heartbeat from an agent for (missed_threshold × interval)
#     seconds, L1 is triggered.
#
#   L2 — Functional publication absence (cross-validation):
#     Peer agents (PFA and RSA for MRA; RSA for PFA) report suspected and
#     confirmed failures via MQTT status events. The DCA cross-validates these
#     reports against L1 evidence before escalating to Mode B.
#
# Failure classification (from spec Section 4.4):
#   Mode A  — transient / low-severity, log only, no SA notification
#   Mode B  — confirmed failure, single pod restart, SA notified only if restart fails
#   Mode B* — RSA failure: restart AND SA notification simultaneously
#   Mode C  — unresolvable, SA notified immediately
#
# Threading note:
#   All state mutations go through a single threading.Lock per deployment.
#   The DCA processes one MQTT message at a time (sequential event loop in
#   mqtt_client.py), but the heartbeat-check background thread runs
#   concurrently, so the lock is required for all reads and writes.
#
import time
import threading
import structlog

log = structlog.get_logger(__name__)


class AgentState:
    """Liveness state constants."""
    ALIVE     = "ALIVE"
    SUSPECTED = "SUSPECTED"
    CONFIRMED_DOWN = "CONFIRMED_DOWN"
    RECOVERING = "RECOVERING"


class AgentRecord:
    """
    Tracks liveness state for a single agent instance
    (one MRA, one PFA, or one RSA per managed deployment).
    """

    def __init__(self, agent_type: str, deployment: str, namespace: str,
                 heartbeat_interval_s: int, missed_threshold: int):
        self.agent_type          = agent_type    # "MRA" | "PFA" | "RSA"
        self.deployment          = deployment
        self.namespace           = namespace
        self.heartbeat_interval  = heartbeat_interval_s
        self.missed_threshold    = missed_threshold

        # Computed failure window in seconds
        self.failure_window_s    = heartbeat_interval_s * missed_threshold

        # Liveness tracking
        self.last_heartbeat_ms   = time.time() * 1000   # initialised to now
        self.state               = AgentState.ALIVE

        # L2 peer evidence counters
        self.l2_suspected_count  = 0   # how many peers have reported SUSPECTED
        self.l2_confirmed        = False  # any peer reported CONFIRMED

        # Recovery tracking
        self.recovery_started_at = None  # wall-clock time of restart attempt
        self.recovery_attempted  = False

        self._lock               = threading.Lock()

    # ---- Heartbeat recording (called by MQTT callback) --------------------

    def record_heartbeat(self) -> bool:
        """
        Record receipt of a heartbeat. Returns True if this heartbeat
        clears a previously degraded state (i.e. the agent recovered).
        """
        with self._lock:
            was_degraded = self.state != AgentState.ALIVE
            self.last_heartbeat_ms   = time.time() * 1000
            self.state               = AgentState.ALIVE
            self.l2_suspected_count  = 0
            self.l2_confirmed        = False
            self.recovery_attempted  = False
            self.recovery_started_at = None
            return was_degraded

    # ---- L2 peer signal recording (called by MQTT callback) ---------------

    def record_l2_suspected(self) -> None:
        """Called when any peer agent reports this agent as SUSPECTED down."""
        with self._lock:
            self.l2_suspected_count += 1

    def record_l2_confirmed(self) -> None:
        """Called when any peer agent reports this agent as CONFIRMED down."""
        with self._lock:
            self.l2_confirmed = True

    # ---- Liveness check (called by background thread) ---------------------

    def check_liveness(self) -> str:
        """
        Evaluate current liveness state and return a mode classification:
          "ALIVE"     — no action needed
          "L1_ONLY"   — L1 triggered, L2 not yet corroborated (Mode A)
          "CONFIRMED" — both layers agree failure is real (Mode B / B*)
          "L2_DIRECT" — L2 confirmed without waiting for L1 (Mode B)
        Called periodically by the DCA's background liveness-check thread.
        """
        with self._lock:
            if self.state == AgentState.RECOVERING:
                return "RECOVERING"

            elapsed_s = (time.time() * 1000 - self.last_heartbeat_ms) / 1000
            l1_triggered = elapsed_s >= self.failure_window_s

            # L2 CONFIRMED from any peer → bypass L1 gate (spec Section 4.3)
            if self.l2_confirmed and self.state != AgentState.CONFIRMED_DOWN:
                self.state = AgentState.CONFIRMED_DOWN
                return "L2_DIRECT"

            # Dual MRA_DOWN_SUSPECTED from BOTH PFA and RSA peers → Mode B
            # (l2_suspected_count >= 2 means both peers reported it)
            if (self.agent_type == "MRA"
                    and self.l2_suspected_count >= 2
                    and self.state != AgentState.CONFIRMED_DOWN):
                self.state = AgentState.CONFIRMED_DOWN
                return "L2_DIRECT"

            # RSA has no peer observers — L1 alone is sufficient to confirm.
            # Per spec Section 4.3: RSA failure is detected exclusively by
            # the DCA via L1 heartbeat supervision.
            if (l1_triggered
                    and self.agent_type == "RSA"
                    and self.state != AgentState.CONFIRMED_DOWN):
                self.state = AgentState.CONFIRMED_DOWN
                return "CONFIRMED"

            # L1 alone triggers L1_ONLY unless already confirmed
            if l1_triggered and self.state == AgentState.ALIVE:
                self.state = AgentState.SUSPECTED
                return "L1_ONLY"

            # Both L1 and at least one L2 SUSPECTED → confirmed
            if (l1_triggered
                    and self.l2_suspected_count >= 1
                    and self.state != AgentState.CONFIRMED_DOWN):
                self.state = AgentState.CONFIRMED_DOWN
                return "CONFIRMED"

            return "ALIVE"

    # ---- Recovery state management ----------------------------------------

    def mark_recovery_started(self) -> None:
        with self._lock:
            self.state               = AgentState.RECOVERING
            self.recovery_started_at = time.time()
            self.recovery_attempted  = True

    def recovery_timed_out(self, timeout_s: int) -> bool:
        """True if recovery was started and the timeout has elapsed."""
        with self._lock:
            if self.recovery_started_at is None:
                return False
            return (time.time() - self.recovery_started_at) >= timeout_s

    @property
    def current_state(self) -> str:
        with self._lock:
            return self.state


class AgentRegistry:
    """
    Domain-level registry of all MRA, PFA, and RSA agent records.
    Holds one AgentRecord per (agent_type, deployment) pair.
    """

    def __init__(self, managed_deployments: list, namespace: str,
                 mra_hb_interval: int, mra_missed: int,
                 pfa_hb_interval: int, pfa_missed: int,
                 rsa_hb_interval: int, rsa_missed: int):

        self._records: dict[tuple, AgentRecord] = {}

        for dep in managed_deployments:
            self._records[("MRA", dep)] = AgentRecord(
                agent_type="MRA", deployment=dep, namespace=namespace,
                heartbeat_interval_s=mra_hb_interval,
                missed_threshold=mra_missed,
            )
            self._records[("PFA", dep)] = AgentRecord(
                agent_type="PFA", deployment=dep, namespace=namespace,
                heartbeat_interval_s=pfa_hb_interval,
                missed_threshold=pfa_missed,
            )
            self._records[("RSA", dep)] = AgentRecord(
                agent_type="RSA", deployment=dep, namespace=namespace,
                heartbeat_interval_s=rsa_hb_interval,
                missed_threshold=rsa_missed,
            )

        log.info(
            "agent_registry_initialised",
            deployments=managed_deployments,
            total_agents=len(self._records),
        )

    def get(self, agent_type: str, deployment: str) -> AgentRecord | None:
        return self._records.get((agent_type, deployment))

    def all_records(self) -> list[AgentRecord]:
        return list(self._records.values())

    def snapshot(self) -> dict:
        """
        Return a serialisable summary of all agent states.
        Used for DOMAIN_STATUS_REPORT construction and audit records.
        """
        return {
            f"{rec.agent_type}:{rec.deployment}": {
                "state":       rec.current_state,
                "agent_type":  rec.agent_type,
                "deployment":  rec.deployment,
            }
            for rec in self.all_records()
        }
