# mra/belief_builder.py
#
# Belief object construction — MRA Specification Section 5 (Table 4).
#
# Pure data assembly: no logic, no decisions.
# Every field name in the returned dict matches Table 4 exactly because
# downstream agents (PFA, RSA, DCA) parse these field names by name.
#
import json
from .preprocessor import NormalisedSample
from .pressure_evaluator import PressureLevel
 
 
def build_belief(
    sample: NormalisedSample,
    pressure_level: PressureLevel,
    filled_values: list,
) -> dict:
    """
    Assemble the belief object payload.
 
    Field layout matches MRA Specification Table 4 exactly.
    Deviating from these field names will break PFA and RSA parsers.
    """
    return {
        # ---- Identity ----
        "deployment": sample.deployment,
        "namespace":  sample.namespace,
        "timestamp":  sample.timestamp_utc_ms,
 
        # ---- Forecast metrics ----
        # These are the ONLY values that feed the PFA sliding window.
        # Context metrics below are never input to TimesFM or ARIMA.
        "forecast_metrics": {
            "cpu_usage_millicores": sample.cpu_usage_millicores,
            "memory_usage_MiB":     sample.memory_usage_MiB,
        },
 
        # ---- Context metrics ----
        # Consumed by RSA sizing pipeline and DCA diagnostic layer.
        "context_metrics": {
            "cpu_requests":       sample.cpu_requests_millicores,
            "cpu_limits":         sample.cpu_limits_millicores,
            "memory_requests":    sample.memory_requests_MiB,
            "memory_limits":      sample.memory_limits_MiB,
            "replicas_available": sample.replicas_available,
            "replicas_ready":     sample.replicas_ready,
            "replicas_desired":   sample.replicas_desired,
        },
 
        # ---- Completeness signal ----
        # Non-empty list signals that some metrics were forward-filled.
        # RSA and DCA may treat forward-filled beliefs with reduced confidence.
        "filled_values": filled_values,
 
        # ---- Pressure classification ----
        "pressure_level": pressure_level.value,
    }
 
 
def belief_to_json(belief: dict) -> str:
    return json.dumps(belief, ensure_ascii=False, separators=(",", ":"))
