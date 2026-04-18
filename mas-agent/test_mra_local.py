"""
test_mra_local.py
-----------------
Run from mas-agent/ with: python test_mra_local.py
 
Tests all components that do NOT require a live cluster:
  - Pressure evaluator: WARNING/CRITICAL transition logic
  - Validator: range check, staleness check, completeness + forward-fill
  - Preprocessor: unit conversion correctness
  - Belief builder: field names match spec Table 4
 
No Prometheus, no MQTT, no SQLite required.
Run this before building the Docker image to catch logic bugs early.
"""
 
import sys
import time
 
sys.path.insert(0, ".")   # make `mra` importable from mas-agent/
 
from mra.pressure_evaluator import ResourcePressureEvaluator, PressureLevel
from mra.preprocessor import preprocess, NormalisedSample
from mra.validator import Validator, ValidationOutcome
from mra.belief_builder import build_belief, belief_to_json
from mra.pressure_evaluator import PressureLevel
 
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
errors = 0
 
 
def check(label: str, condition: bool) -> None:
    global errors
    if condition:
        print(f"  {PASS}  {label}")
    else:
        print(f"  {FAIL}  {label}")
        errors += 1
 
 
# =============================================================================
print("\n=== Pressure Evaluator ===")
 
ev = ResourcePressureEvaluator(0.70, 0.85, 0.75, 0.90, warning_consecutive=2)
 
# cpu_limit=200m (nginx), memory_limit=64MiB (nginx) — from your workload specs
L = 200.0
M = 64.0
 
# NORMAL — well below WARNING
lv = ev.evaluate(40, L, 20, M)
check("NORMAL at low usage", lv == PressureLevel.NORMAL)
 
# First WARNING crossing — should stay NORMAL (needs 2 consecutive)
lv = ev.evaluate(150, L, 20, M)   # 150/200 = 75% > 70%
check("Still NORMAL on first WARNING crossing", lv == PressureLevel.NORMAL)
 
# Second WARNING crossing — should transition to WARNING
lv = ev.evaluate(150, L, 20, M)
check("WARNING after 2 consecutive crossings", lv == PressureLevel.WARNING)
 
# CRITICAL — immediate, no confirmation required
lv = ev.evaluate(180, L, 20, M)   # 180/200 = 90% > 85%
check("CRITICAL immediate", lv == PressureLevel.CRITICAL)
 
# Recovery test — new evaluator
ev2 = ResourcePressureEvaluator(0.70, 0.85, 0.75, 0.90, warning_consecutive=2)
ev2.evaluate(150, L, 20, M)  # first crossing
ev2.evaluate(150, L, 20, M)  # second → WARNING
check("Setup: in WARNING", ev2.current_level == PressureLevel.WARNING)
 
ev2.evaluate(40, L, 20, M)   # below threshold — 1 of 2 needed for recovery
check("Still WARNING after 1 below-threshold cycle", ev2.current_level == PressureLevel.WARNING)
 
ev2.evaluate(40, L, 20, M)   # second consecutive → recover to NORMAL
check("Recovered to NORMAL after 2 consecutive below-threshold", ev2.current_level == PressureLevel.NORMAL)
 
 
# =============================================================================
print("\n=== Validator ===")
 
def make_sample(cpu=50.0, mem=20.0, cpu_lim=200.0, mem_lim=64.0, ts_offset=0):
    return NormalisedSample(
        namespace="workloads",
        deployment="nginx",
        timestamp_utc_ms=int(time.time() * 1000) + ts_offset,
        cpu_usage_millicores=cpu,
        memory_usage_MiB=mem,
        cpu_requests_millicores=25.0,
        cpu_limits_millicores=cpu_lim,
        memory_requests_MiB=16.0,
        memory_limits_MiB=mem_lim,
        replicas_available=3,
        replicas_ready=3,
        replicas_desired=3,
    )
 
v = Validator()
 
r = v.validate(make_sample())
check("Valid complete sample passes", r.passed)
check("COMPLETE outcome", ValidationOutcome.COMPLETE in r.outcomes)
 
r2 = v.validate(make_sample(cpu=250.0, ts_offset=31000))  # 250 > 200 limit
check("Range error: CPU over limit rejected", not r2.passed)
check("RANGE_ERROR outcome", ValidationOutcome.RANGE_ERROR in r2.outcomes)
 
r3 = v.validate(make_sample(ts_offset=25000))  # 25s later — valid
check("Valid sample after range error passes", r3.passed)
 
# Staleness: same timestamp should be rejected
v2 = Validator()
v2.validate(make_sample(ts_offset=0))
r_stale = v2.validate(make_sample(ts_offset=0))  # same ms
check("Staleness: duplicate timestamp rejected", not r_stale.passed)
check("STALENESS_ERROR outcome", ValidationOutcome.STALENESS_ERROR in r_stale.outcomes)
 
# UNCONFIGURED: no limits
v3 = Validator()
r_unc = v3.validate(make_sample(cpu_lim=0, mem_lim=0))
check("UNCONFIGURED: zero limits rejected", not r_unc.passed)
check("UNCONFIGURED outcome", ValidationOutcome.UNCONFIGURED in r_unc.outcomes)
 
# Forward-fill: pass a sample with None cpu_usage
v4 = Validator()
good = make_sample(ts_offset=0)
v4.validate(good)  # seed the forward-fill cache
 
from dataclasses import replace
missing_cpu = replace(good,
    cpu_usage_millicores=None,
    timestamp_utc_ms=int(time.time() * 1000) + 31000,
)
r_fill = v4.validate(missing_cpu)
check("Forward-fill: None cpu_usage filled from cache", r_fill.passed)
check("Filled metric listed in filled_metrics", "cpu_usage_millicores" in r_fill.filled_metrics)
check("INCOMPLETE outcome", ValidationOutcome.INCOMPLETE in r_fill.outcomes)
 
 
# =============================================================================
print("\n=== Preprocessor ===")
 
raw = {
    "cpu_usage_raw": 75.0,         # millicores (already from PromQL * 1000)
    "memory_usage_raw_bytes": 20971520.0,  # 20 MiB in bytes
    "cpu_requests_raw": 25.0,
    "cpu_limits_raw": 200.0,
    "memory_requests_raw_bytes": 16777216.0,  # 16 MiB
    "memory_limits_raw_bytes": 67108864.0,    # 64 MiB
    "replicas_available": 3,
    "replicas_ready": 3,
    "replicas_desired": 3,
}
 
s = preprocess(raw, "workloads", "nginx")
check("CPU passthrough: 75.0mc", s.cpu_usage_millicores == 75.0)
check("Memory bytes→MiB: 20971520 → 20.0 MiB",
      abs(s.memory_usage_MiB - 20.0) < 0.001)
check("Memory limit bytes→MiB: 67108864 → 64.0 MiB",
      abs(s.memory_limits_MiB - 64.0) < 0.001)
check("Namespace label attached", s.namespace == "workloads")
check("Deployment label attached", s.deployment == "nginx")
check("Timestamp is UTC ms (13 digits)", len(str(s.timestamp_utc_ms)) == 13)
 
 
# =============================================================================
print("\n=== Belief Builder ===")
 
valid_sample = make_sample()
belief = build_belief(valid_sample, PressureLevel.NORMAL, [])
 
# Verify all field names match Spec Table 4 exactly
check("Field: deployment",       "deployment" in belief)
check("Field: namespace",        "namespace" in belief)
check("Field: timestamp",        "timestamp" in belief)
check("Field: forecast_metrics", "forecast_metrics" in belief)
check("Field: context_metrics",  "context_metrics" in belief)
check("Field: filled_values",    "filled_values" in belief)
check("Field: pressure_level",   "pressure_level" in belief)
 
check("forecast_metrics.cpu_usage_millicores present",
      "cpu_usage_millicores" in belief["forecast_metrics"])
check("forecast_metrics.memory_usage_MiB present",
      "memory_usage_MiB" in belief["forecast_metrics"])
check("context_metrics has 7 fields",
      len(belief["context_metrics"]) == 7)
check("pressure_level is 'NORMAL'",
      belief["pressure_level"] == "NORMAL")
 
# Verify JSON serialisation does not crash
j = belief_to_json(belief)
check("Serialises to valid JSON string", isinstance(j, str) and len(j) > 10)
 
 
# =============================================================================
print("")
if errors == 0:
    print(f"\033[92mAll tests passed.\033[0m")
    sys.exit(0)
else:
    print(f"\033[91m{errors} test(s) failed.\033[0m")
    sys.exit(1)
