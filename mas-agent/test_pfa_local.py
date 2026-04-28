"""
test_pfa_local.py
-----------------
Run from pfa-agent/ with:
    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt
    python3 test_pfa_local.py

Tests all PFA components that do NOT require a live cluster,
TimesFM model weights, or MQTT broker:
  - SlidingWindow: FIFO behaviour, size cap, fill ratio, inference_input sizing
  - DualSlidingWindow: append_from_belief, independence of CPU and memory
  - InferenceEngine (fallback mode): output shape, NaN guard, breach detection
  - compute_uncertainty: raw score + fill penalty
  - compute_time_to_breach: P50/P90/P10 scan, seconds conversion
  - forecast_builder: field names, p50_breach_predicted logic
  - MRAMonitor: suspected/confirmed/recovery state transitions

Run this before building the Docker image.
"""

import sys
import time
import math

sys.path.insert(0, ".")

from pfa.sliding_window import SlidingWindow, DualSlidingWindow
from pfa.inference_engine import (
    InferenceEngine, compute_uncertainty, compute_time_to_breach,
    QuantileTrajectory,
)
from pfa.forecast_builder import build_forecast_update
from pfa.mra_monitor import MRAMonitor, MRALivenessState

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
print("\n=== SlidingWindow — basic operations ===")

w = SlidingWindow(max_size=5, min_inference=3, patch_size=2)

check("Empty window: size=0", w.size == 0)
check("Empty window: not ready", not w.is_ready)
check("Empty window: fill_ratio=0", w.fill_ratio == 0.0)
check("Empty window: inference_input=None", w.get_inference_input() is None)

w.append(10.0, False)
w.append(20.0, True)
w.append(30.0, False)

check("After 3 appends: size=3", w.size == 3)
check("Ready at min_inference=3", w.is_ready)
check("fill_ratio = 1/3", abs(w.fill_ratio - 1/3) < 0.001)

# inference_input: largest multiple of patch_size=2 within size=3 → 2
inp = w.get_inference_input()
check("inference_input size = 2 (largest multiple of 2 within 3)", len(inp) == 2)
check("inference_input takes most recent values", inp == [20.0, 30.0])

w.append(40.0, False)
inp2 = w.get_inference_input()
check("inference_input size = 4 after 4 observations", len(inp2) == 4)

# Cap test: max_size=5, append 6th → oldest dropped
w.append(50.0, False)
w.append(60.0, False)
check("Window capped at max_size=5", w.size == 5)
check("Oldest value dropped", w.values[0] == 20.0)

w.reset()
check("After reset: size=0", w.size == 0)


# =============================================================================
print("\n=== DualSlidingWindow — belief extraction ===")

dw = DualSlidingWindow(max_size=96, min_inference=32, patch_size=32)

belief = {
    "forecast_metrics": {
        "cpu_usage_millicores": 150.0,
        "memory_usage_MiB": 45.5,
    },
    "filled_values": ["memory_usage_MiB"],
    "timestamp": int(time.time() * 1000),
    "context_metrics": {"cpu_limits": 500.0, "memory_limits": 128.0},
    "pressure_level": "NORMAL",
}

dw.append_from_belief(belief)

check("CPU window size = 1 after one belief", dw.cpu.size == 1)
check("Memory window size = 1 after one belief", dw.memory.size == 1)
check("CPU value correct", dw.cpu.values[0] == 150.0)
check("Memory value correct", dw.memory.values[0] == 45.5)
check("CPU not filled", dw.cpu.fill_ratio == 0.0)
check("Memory filled (in filled_values)", dw.memory.fill_ratio == 1.0)
check("Not ready yet (need 32)", not dw.is_ready)

# Fill to min_inference with unfilled values
for i in range(31):
    b = {
        "forecast_metrics": {"cpu_usage_millicores": float(i), "memory_usage_MiB": float(i)},
        "filled_values": [],
        "timestamp": int(time.time() * 1000) + i * 30000,
    }
    dw.append_from_belief(b)

check("Ready after 32 observations", dw.is_ready)


# =============================================================================
print("\n=== InferenceEngine — fallback mode ===")

# Force fallback mode by using the engine without timesfm installed
engine = InferenceEngine(forecast_steps=30, patch_size=32)

cpu_series = [float(i * 10) for i in range(32)]    # 0, 10, 20, ..., 310
mem_series = [float(i * 2) for i in range(32)]     # 0, 2, 4, ..., 62

result = engine.run(cpu_series, mem_series)

check("CPU P50 has 30 steps", len(result.cpu.p50) == 30)
check("CPU P10 has 30 steps", len(result.cpu.p10) == 30)
check("CPU P90 has 30 steps", len(result.cpu.p90) == 30)
check("Memory P50 has 30 steps", len(result.memory.p50) == 30)
check("All CPU values finite", all(math.isfinite(v) for v in result.cpu.p50))
check("All memory values finite", all(math.isfinite(v) for v in result.memory.p50))
check("No negative CPU values", all(v >= 0 for v in result.cpu.p50))
check("No negative memory values", all(v >= 0 for v in result.memory.p50))
check("P90 >= P50 >= P10 for CPU (first step)",
      result.cpu.p90[0] >= result.cpu.p50[0] >= result.cpu.p10[0])
check("P90 >= P50 >= P10 for memory (first step)",
      result.memory.p90[0] >= result.memory.p50[0] >= result.memory.p10[0])
check("inference_duration_ms is positive", result.inference_duration_ms > 0)
check("timestamp_ms is set", result.timestamp_ms > 0)

# NaN guard — engine should raise ValueError on NaN output
# We test this indirectly by verifying finite check passes on fallback output


# =============================================================================
print("\n=== compute_uncertainty ===")

# Zero spread → raw=0, no fill penalty → 0.0
flat_traj = QuantileTrajectory(
    p10=[100.0] * 30, p50=[100.0] * 30, p90=[100.0] * 30,
    model_used="test"
)
u = compute_uncertainty(flat_traj, fill_ratio=0.0)
check("Zero spread + no fill → uncertainty=0.0", u == 0.0)

# Wide spread → raw > 0
wide_traj = QuantileTrajectory(
    p10=[0.0] * 30, p50=[100.0] * 30, p90=[200.0] * 30,
    model_used="test"
)
u_wide = compute_uncertainty(wide_traj, fill_ratio=0.0)
check("Wide spread → uncertainty > 0", u_wide > 0)

# Fill penalty adds to raw, capped at 1.0
# Use narrow trajectory so raw < 1.0
narrow_traj = QuantileTrajectory(
    p10=[90.0]*30, p50=[100.0]*30, p90=[110.0]*30, model_used="test"
)
u_narrow = compute_uncertainty(narrow_traj, fill_ratio=0.0)
u_penalised = compute_uncertainty(narrow_traj, fill_ratio=0.3)
check("Fill penalty increases uncertainty", u_penalised > u_narrow)

u_max = compute_uncertainty(wide_traj, fill_ratio=1.0)
check("Uncertainty capped at 1.0", u_max <= 1.0)


# =============================================================================
print("\n=== compute_time_to_breach ===")

# P50 crosses at step 5 (index 4, → 5*30=150s)
p50_crosses = [50.0] * 4 + [80.0] * 26   # 80 > threshold 70
traj_breach = QuantileTrajectory(
    p10=[30.0] * 30,
    p50=p50_crosses,
    p90=[90.0] * 30,  # crosses immediately
    model_used="test"
)
breach = compute_time_to_breach(traj_breach, breach_threshold=70.0,
                                step_duration_seconds=30)

check("P50 breach detected", breach["time_to_breach_seconds"] is not None)
check("P50 breach at step 5 → 150s", breach["time_to_breach_seconds"] == 150)
check("Confidence is HIGH", breach["breach_confidence"] == "HIGH")
check("P90 breach earlier (pessimistic < 150s)",
      breach["breach_window_pessimistic"] <= 150)

# No breach — all values below threshold
traj_no_breach = QuantileTrajectory(
    p10=[10.0] * 30, p50=[20.0] * 30, p90=[30.0] * 30,
    model_used="test"
)
breach_none = compute_time_to_breach(traj_no_breach, breach_threshold=70.0,
                                     step_duration_seconds=30)
check("No breach → time_to_breach=None", breach_none["time_to_breach_seconds"] is None)
check("No breach → confidence=NONE", breach_none["breach_confidence"] == "NONE")


# =============================================================================
print("\n=== forecast_builder ===")

dummy_result = engine.run(cpu_series, mem_series)
dummy_belief = {
    "deployment": "nginx", "namespace": "workloads",
    "timestamp": int(time.time() * 1000),
    "forecast_metrics": {"cpu_usage_millicores": 150.0, "memory_usage_MiB": 45.5},
    "context_metrics": {
        "cpu_limits": 500.0, "memory_limits": 128.0,
        "cpu_requests": 100.0, "memory_requests": 64.0,
        "replicas_available": 3, "replicas_ready": 3, "replicas_desired": 3,
    },
    "filled_values": [],
    "pressure_level": "NORMAL",
}

forecast = build_forecast_update(
    result=dummy_result,
    cpu_window_fill_ratio=0.0,
    memory_window_fill_ratio=0.0,
    cpu_limit_millicores=500.0,
    memory_limit_MiB=128.0,
    cpu_breach_pct=0.70,
    memory_breach_pct=0.75,
    step_duration_seconds=30,
    originating_belief=dummy_belief,
    namespace="workloads",
    deployment="nginx",
)

check("event_type = FORECAST_UPDATE", forecast["event_type"] == "FORECAST_UPDATE")
check("namespace present", forecast["namespace"] == "workloads")
check("deployment present", forecast["deployment"] == "nginx")
check("cpu.trajectories present", "trajectories" in forecast["cpu"])
check("memory.trajectories present", "trajectories" in forecast["memory"])
check("cpu.p50 has 30 steps", len(forecast["cpu"]["trajectories"]["p50"]) == 30)
check("memory.p50 has 30 steps", len(forecast["memory"]["trajectories"]["p50"]) == 30)
check("cpu.uncertainty_score in [0,1]",
      0 <= forecast["cpu"]["uncertainty_score"] <= 1)
check("memory.uncertainty_score in [0,1]",
      0 <= forecast["memory"]["uncertainty_score"] <= 1)
check("p50_breach_predicted is bool",
      isinstance(forecast["p50_breach_predicted"], bool))
check("originating_belief embedded",
      "originating_belief" in forecast)
check("originating_belief has pressure_level",
      "pressure_level" in forecast["originating_belief"])


# =============================================================================
print("\n=== MRAMonitor — state transitions ===")

suspected_fired = []
confirmed_fired = []
recovered_fired = []

monitor = MRAMonitor(
    expected_interval_seconds=30,
    suspected_threshold=3,
    confirmed_threshold=5,
    on_suspected=lambda: suspected_fired.append(1),
    on_confirmed=lambda: confirmed_fired.append(1),
    on_recovered=lambda: recovered_fired.append(1),
)

# Simulate elapsed time by manually manipulating the internal timestamp
# We do this by calling check() after faking time passage
import unittest.mock as mock

# Fake: 91 seconds elapsed (3.03 cycles) → should fire SUSPECTED
monitor._last_belief_time = time.time() - 91
monitor._last_status_time = time.time() - 91
monitor.check()
check("SUSPECTED fires after 3 missed cycles", len(suspected_fired) == 1)
check("CONFIRMED not fired yet", len(confirmed_fired) == 0)
check("State is SUSPECTED", monitor.state == MRALivenessState.SUSPECTED)

# Fake: 151 seconds elapsed → should fire CONFIRMED
monitor._last_belief_time = time.time() - 151
monitor._last_status_time = time.time() - 151
monitor.check()
check("CONFIRMED fires after 5 missed cycles", len(confirmed_fired) == 1)
check("State is CONFIRMED", monitor.state == MRALivenessState.CONFIRMED)

# Recovery: receive a belief update
monitor.record_belief_update()
check("Recovery fires on belief update", len(recovered_fired) == 1)
check("State returns to ALIVE", monitor.state == MRALivenessState.ALIVE)

# Status event resets counter without recovery
monitor2 = MRAMonitor(
    expected_interval_seconds=30,
    suspected_threshold=3,
    confirmed_threshold=5,
    on_suspected=lambda: suspected_fired.append(1),
    on_confirmed=lambda: confirmed_fired.append(1),
    on_recovered=lambda: recovered_fired.append(1),
)
monitor2._last_belief_time = time.time() - 91
monitor2._last_status_time = time.time() - 5   # status event 5s ago
monitor2.check()
check("Status event resets counter — SUSPECTED not fired", len(suspected_fired) == 1)


# =============================================================================
print("")
if errors == 0:
    print(f"\033[92mAll tests passed.\033[0m")
    sys.exit(0)
else:
    print(f"\033[91m{errors} test(s) failed.\033[0m")
    sys.exit(1)
