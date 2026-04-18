# mra/validator.py
#
# Validation pipeline — MRA Specification Section 3.3.
#
# Three checks applied in strict sequential order:
#   0. UNCONFIGURED detection (prerequisite for range check)
#   1. Range check — physically impossible values discarded
#   2. Staleness check — duplicate/out-of-order timestamps discarded
#   3. Completeness check — missing metrics forward-filled from previous cycle
#
# State: one Validator instance per MRA instance (per monitored deployment).
# State is never persisted — resets on restart per Spec Section 8.3.
#
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Optional
import structlog
 
from .preprocessor import NormalisedSample
 
log = structlog.get_logger(__name__)
 
 
class ValidationOutcome(Enum):
    VALID        = "valid"
    RANGE_ERROR  = "range_error"
    STALENESS_ERROR = "staleness_error"
    UNCONFIGURED = "unconfigured"
    COMPLETE     = "complete"
    INCOMPLETE   = "incomplete"
 
 
@dataclass
class ValidationResult:
    passed: bool
    outcomes: list
    filled_metrics: list
    sample: Optional[NormalisedSample]
    error_events: list = field(default_factory=list)
 
 
class Validator:
    """Stateful validator — one instance per MRA."""
 
    def __init__(self):
        self._prev_timestamp_ms: Optional[int] = None
        self._forward_fill: dict = {}
 
    def validate(self, sample: NormalisedSample) -> ValidationResult:
 
        outcomes = []
        error_events = []
 
        # ---- Check 0: UNCONFIGURED ----
        if (
            sample.cpu_limits_millicores is None
            or sample.cpu_limits_millicores <= 0
            or sample.memory_limits_MiB is None
            or sample.memory_limits_MiB <= 0
        ):
            outcomes.append(ValidationOutcome.UNCONFIGURED)
            return ValidationResult(
                passed=False,
                outcomes=outcomes,
                filled_metrics=[],
                sample=None,
                error_events=[{
                    "record_type": "VALIDATION_ERROR",
                    "sub_type": "UNCONFIGURED",
                    "namespace": sample.namespace,
                    "deployment": sample.deployment,
                    "timestamp_ms": sample.timestamp_utc_ms,
                    "detail": "No resource limits configured.",
                }],
            )
 
        # ---- Check 1: Range check ----
        range_failed = False
 
        if sample.cpu_usage_millicores is not None:
            if not (0 <= sample.cpu_usage_millicores <= sample.cpu_limits_millicores):
                log.warning("validation_range_error",
                            metric="cpu", value=sample.cpu_usage_millicores,
                            limit=sample.cpu_limits_millicores,
                            deployment=sample.deployment)
                outcomes.append(ValidationOutcome.RANGE_ERROR)
                error_events.append({
                    "record_type": "VALIDATION_RANGE_ERROR",
                    "namespace": sample.namespace,
                    "deployment": sample.deployment,
                    "timestamp_ms": sample.timestamp_utc_ms,
                    "metric": "cpu_usage_millicores",
                    "value": sample.cpu_usage_millicores,
                    "limit": sample.cpu_limits_millicores,
                })
                range_failed = True
 
        if sample.memory_usage_MiB is not None:
            if not (0 <= sample.memory_usage_MiB <= sample.memory_limits_MiB):
                log.warning("validation_range_error",
                            metric="memory", value=sample.memory_usage_MiB,
                            limit=sample.memory_limits_MiB,
                            deployment=sample.deployment)
                outcomes.append(ValidationOutcome.RANGE_ERROR)
                error_events.append({
                    "record_type": "VALIDATION_RANGE_ERROR",
                    "namespace": sample.namespace,
                    "deployment": sample.deployment,
                    "timestamp_ms": sample.timestamp_utc_ms,
                    "metric": "memory_usage_MiB",
                    "value": sample.memory_usage_MiB,
                    "limit": sample.memory_limits_MiB,
                })
                range_failed = True
 
        if range_failed:
            return ValidationResult(
                passed=False, outcomes=outcomes,
                filled_metrics=[], sample=None, error_events=error_events,
            )
 
        # ---- Check 2: Staleness check ----
        if (self._prev_timestamp_ms is not None
                and sample.timestamp_utc_ms <= self._prev_timestamp_ms):
            log.warning("validation_staleness_error",
                        current=sample.timestamp_utc_ms,
                        prev=self._prev_timestamp_ms,
                        deployment=sample.deployment)
            outcomes.append(ValidationOutcome.STALENESS_ERROR)
            return ValidationResult(
                passed=False, outcomes=outcomes,
                filled_metrics=[], sample=None,
                error_events=[{
                    "record_type": "VALIDATION_STALENESS_ERROR",
                    "namespace": sample.namespace,
                    "deployment": sample.deployment,
                    "timestamp_ms": sample.timestamp_utc_ms,
                    "prev_timestamp_ms": self._prev_timestamp_ms,
                }],
            )
        self._prev_timestamp_ms = sample.timestamp_utc_ms
 
        # ---- Check 3: Completeness check + forward-fill ----
        metrics = {
            "cpu_usage_millicores":    sample.cpu_usage_millicores,
            "memory_usage_MiB":        sample.memory_usage_MiB,
            "cpu_requests_millicores": sample.cpu_requests_millicores,
            "cpu_limits_millicores":   sample.cpu_limits_millicores,
            "memory_requests_MiB":     sample.memory_requests_MiB,
            "memory_limits_MiB":       sample.memory_limits_MiB,
            "replicas_available":      sample.replicas_available,
            "replicas_ready":          sample.replicas_ready,
            "replicas_desired":        sample.replicas_desired,
        }
        filled = []
        for name, val in metrics.items():
            if val is None:
                if name in self._forward_fill:
                    metrics[name] = self._forward_fill[name]
                    filled.append(name)
                    log.info("forward_filled", metric=name,
                             value=self._forward_fill[name],
                             deployment=sample.deployment)
            else:
                self._forward_fill[name] = val
 
        outcomes.append(
            ValidationOutcome.INCOMPLETE if filled else ValidationOutcome.COMPLETE
        )
 
        filled_sample = replace(
            sample,
            cpu_usage_millicores=metrics["cpu_usage_millicores"],
            memory_usage_MiB=metrics["memory_usage_MiB"],
            cpu_requests_millicores=metrics["cpu_requests_millicores"],
            cpu_limits_millicores=metrics["cpu_limits_millicores"],
            memory_requests_MiB=metrics["memory_requests_MiB"],
            memory_limits_MiB=metrics["memory_limits_MiB"],
            replicas_available=metrics["replicas_available"],
            replicas_ready=metrics["replicas_ready"],
            replicas_desired=metrics["replicas_desired"],
        )
 
        return ValidationResult(
            passed=True, outcomes=outcomes,
            filled_metrics=filled, sample=filled_sample,
            error_events=error_events,
        )
 
