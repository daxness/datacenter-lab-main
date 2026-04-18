# mra/preprocessor.py
#
# Preprocessing pipeline — MRA Specification Section 3.1 and 3.2.
#
# Responsibility: convert raw Prometheus values into canonical units
# and attach Kubernetes identity labels to every metric sample.
#
# What this module does:
#   - CPU: already in millicores from PromQL (* 1000) — passed through
#   - Memory: bytes → MiB (bytes / 1024 / 1024)
#   - Timestamps: time.time() * 1000 → UTC Unix epoch in milliseconds
#   - Labels: namespace and deployment_name attached to every sample
#
# What this module does NOT do:
#   - No validation (that is validator.py)
#   - No pressure classification (that is pressure_evaluator.py)
#   - No forward-filling (that is validator.py)
#
import time
from dataclasses import dataclass
from typing import Optional
 
_BYTES_PER_MIB = 1024 * 1024
 
 
@dataclass
class NormalisedSample:
    """
    One scrape cycle's metrics after unit normalisation.
    Field names match the belief object schema (Spec Table 4) exactly.
    None means the metric was absent from Prometheus this cycle.
    """
    namespace: str
    deployment: str
    timestamp_utc_ms: int
 
    # Forecast metrics — the only inputs to the PFA sliding window
    cpu_usage_millicores: Optional[float]
    memory_usage_MiB: Optional[float]
 
    # Context metrics — consumed by RSA and DCA, never by the forecast model
    cpu_requests_millicores: Optional[float]
    cpu_limits_millicores: Optional[float]
    memory_requests_MiB: Optional[float]
    memory_limits_MiB: Optional[float]
    replicas_available: Optional[int]
    replicas_ready: Optional[int]
    replicas_desired: Optional[int]
 
 
def preprocess(raw: dict, namespace: str, deployment: str) -> NormalisedSample:
    """
    Convert raw Prometheus scrape output into a NormalisedSample.
 
    Args:
        raw:        dict returned by PrometheusClient.scrape_all()
        namespace:  Kubernetes namespace of the monitored deployment
        deployment: name of the monitored deployment
    """
    def to_MiB(val: Optional[float]) -> Optional[float]:
        return val / _BYTES_PER_MIB if val is not None else None
 
    return NormalisedSample(
        namespace=namespace,
        deployment=deployment,
        timestamp_utc_ms=int(time.time() * 1000),
 
        # CPU is already in millicores from PromQL (* 1000)
        cpu_usage_millicores=raw.get("cpu_usage_raw"),
 
        # Memory raw values are in bytes — convert to MiB
        memory_usage_MiB=to_MiB(raw.get("memory_usage_raw_bytes")),
        cpu_requests_millicores=raw.get("cpu_requests_raw"),
        cpu_limits_millicores=raw.get("cpu_limits_raw"),
        memory_requests_MiB=to_MiB(raw.get("memory_requests_raw_bytes")),
        memory_limits_MiB=to_MiB(raw.get("memory_limits_raw_bytes")),
 
        # Replica counts are integers — Prometheus returns them as floats
        replicas_available=raw.get("replicas_available"),
        replicas_ready=raw.get("replicas_ready"),
        replicas_desired=raw.get("replicas_desired"),
    )
 
