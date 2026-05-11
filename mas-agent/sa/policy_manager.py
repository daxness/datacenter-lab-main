# sa/policy_manager.py
#
# Policy management for the SA.
#
# The SA is the sole writer to Domain 1 (the mas-policy ConfigMap). This
# module owns all policy operations:
#   - Loading the current policy at startup
#   - Validating a proposed policy update for internal consistency
#   - Writing a validated update to Domain 1
#   - Recording the new version in Domain 6 (oversight history)
#
# Validation is applied before ANY write. Specifically, the following rules
# are checked (consistent with the SA Final Design, Section on Policy Authority):
#   1. schema_version must be present and equal to 1.
#   2. sla_thresholds: all four percentage values present and in (0, 1).
#   3. WARNING threshold must be strictly less than CRITICAL threshold,
#      for both CPU and memory independently.
#   4. replica_bounds: every entry must have min >= 1 and max >= min.
#   5. cooldown_duration_seconds >= 0.
#   6. stability_threshold_N >= 1.
#
# A ValidationError is raised (not just logged) if any rule is violated,
# so the API layer can return the specific failure message to the operator.
#
import time
import structlog
from .k8s_client import SAK8sClient

log = structlog.get_logger(__name__)


class PolicyValidationError(Exception):
    """Raised when a proposed policy update fails validation."""
    pass


class PolicyManager:
    """
    Manages policy read, validation, and write for the SA.

    Depends on SAK8sClient for the actual ConfigMap I/O.
    The SA's main.py owns the KBWriter (Domain 4) and calls write_audit
    after every successful policy write — PolicyManager itself does not
    write to Domain 4.
    """

    def __init__(self, k8s: SAK8sClient):
        self._k8s = k8s
        # Cached copy of the current policy. Updated on every write and
        # on startup load. Provides O(1) reads without a Kubernetes API call.
        self._current_policy: dict = {}

    # ---- Load at startup -------------------------------------------------

    def load(self) -> dict:
        """
        Read Domain 1 and cache the result. Called once during Stage 0.
        Returns the policy dict. Raises on Kubernetes API failure.
        """
        self._current_policy = self._k8s.read_policy()
        log.info("policy_loaded",
                 schema_version=self._current_policy.get("schema_version"),
                 policy_version=self._current_policy.get("policy_version"))
        return self._current_policy

    def current(self) -> dict:
        """Return the cached policy (last successfully written or loaded)."""
        return self._current_policy

    # ---- Validation -------------------------------------------------------

    def validate(self, proposed: dict) -> None:
        """
        Validate a proposed policy update.
        Raises PolicyValidationError with a human-readable message on failure.
        All checks are applied; the first failure raises immediately.
        """
        # Rule 1: schema_version
        if proposed.get("schema_version") != 1:
            raise PolicyValidationError(
                "schema_version must be present and equal to 1"
            )

        # Rule 2 & 3: sla_thresholds
        thresholds = proposed.get("sla_thresholds")
        if not isinstance(thresholds, dict):
            raise PolicyValidationError(
                "sla_thresholds must be a JSON object"
            )
        required_keys = [
            "cpu_warning_pct", "cpu_critical_pct",
            "memory_warning_pct", "memory_critical_pct",
        ]
        for key in required_keys:
            val = thresholds.get(key)
            if val is None:
                raise PolicyValidationError(
                    f"sla_thresholds.{key} is missing"
                )
            if not (0 < val < 1):
                raise PolicyValidationError(
                    f"sla_thresholds.{key} = {val} is outside the required range (0, 1)"
                )

        if thresholds["cpu_warning_pct"] >= thresholds["cpu_critical_pct"]:
            raise PolicyValidationError(
                f"cpu_warning_pct ({thresholds['cpu_warning_pct']}) must be "
                f"strictly less than cpu_critical_pct ({thresholds['cpu_critical_pct']})"
            )

        if thresholds["memory_warning_pct"] >= thresholds["memory_critical_pct"]:
            raise PolicyValidationError(
                f"memory_warning_pct ({thresholds['memory_warning_pct']}) must be "
                f"strictly less than memory_critical_pct ({thresholds['memory_critical_pct']})"
            )

        # Rule 4: replica_bounds
        replica_bounds = proposed.get("replica_bounds", {})
        if not isinstance(replica_bounds, dict):
            raise PolicyValidationError("replica_bounds must be a JSON object")
        for dep_key, bounds in replica_bounds.items():
            mn = bounds.get("min")
            mx = bounds.get("max")
            if mn is None or mx is None:
                raise PolicyValidationError(
                    f"replica_bounds.{dep_key}: 'min' and 'max' are required"
                )
            if mn < 1:
                raise PolicyValidationError(
                    f"replica_bounds.{dep_key}.min = {mn} must be >= 1"
                )
            if mx < mn:
                raise PolicyValidationError(
                    f"replica_bounds.{dep_key}.max = {mx} must be >= min = {mn}"
                )

        # Rule 5: cooldown_duration_seconds
        cooldown = proposed.get("cooldown_duration_seconds", 0)
        if cooldown < 0:
            raise PolicyValidationError(
                f"cooldown_duration_seconds = {cooldown} must be >= 0"
            )

        # Rule 6: stability_threshold_N
        stability = proposed.get("stability_threshold_N", 1)
        if stability < 1:
            raise PolicyValidationError(
                f"stability_threshold_N = {stability} must be >= 1"
            )

    # ---- Write -----------------------------------------------------------

    def apply_update(self, proposed: dict) -> dict:
        """
        Validate, version-stamp, write to Domain 1, and update the cache.

        Steps:
          1. Validate the proposed dict.
          2. Stamp policy_version (increment from current) and policy_timestamp.
          3. Write to Domain 1 via SAK8sClient.
          4. Update internal cache.
          5. Return the written policy (with version stamp) for Domain 6 recording.

        Raises PolicyValidationError on validation failure.
        Raises ApiException on Kubernetes write failure.
        """
        self.validate(proposed)

        current_version = self._current_policy.get("policy_version", 0)
        proposed["policy_version"]  = current_version + 1
        proposed["policy_timestamp"] = int(time.time() * 1000)

        self._k8s.write_policy(proposed)
        self._current_policy = proposed

        log.info("policy_applied",
                 policy_version=proposed["policy_version"])
        return proposed
