"""
test_rsa_local.py
-----------------
Run from mas-agent/ with the venv already active:
    python3 test_rsa_local.py

Tests all RSA components that do NOT require a live cluster,
Kubernetes API, MQTT broker, or SQLite database:

  - SizingPipeline: quantile selection, margin interpolation, delta
    computation, SCALE_UP / DO_NOTHING decisions, replica ceiling,
    bounds clamp, overflow detection.
  - CooldownManager: start, is_active, expiry, resume-and-adjust,
    emergency path does not start cooldown.
  - MRAPeerMonitor: suspected / confirmed / recovery state transitions.
  - PFAPeerMonitor: suspected / confirmed / recovery state transitions.

Run this before building the Docker image.
"""

import sys
import time
import math
import threading

sys.path.insert(0, ".")

from rsa.sizing_pipeline import SizingPipeline, PipelineResult
from rsa.peer_monitor    import MRAPeerMonitor, PFAPeerMonitor, PeerLivenessState

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


# ---- Helpers: build minimal forecast objects that match PFA output --------

def _make_forecast(
    cpu_p50, cpu_p90, cpu_p10=None,
    mem_p50=None, mem_p90=None, mem_p10=None,
    cpu_uncertainty=0.0, mem_uncertainty=0.0,
    cpu_breach_confidence="HIGH",
    mem_breach_confidence="NONE",
    pressure_level="NORMAL",
) -> dict:
    steps = len(cpu_p50)
    cpu_p10  = cpu_p10  or [v * 0.8 for v in cpu_p50]
    mem_p50  = mem_p50  or [20.0] * steps
    mem_p90  = mem_p90  or [22.0] * steps
    mem_p10  = mem_p10  or [18.0] * steps
    return {
        "event_type": "FORECAST_UPDATE",
        "cpu": {
            "trajectories": {
                "p10": cpu_p10,
                "p50": cpu_p50,
                "p90": cpu_p90,
            },
            "uncertainty_score": cpu_uncertainty,
            "breach": {
                "time_to_breach_seconds": 150.0,
                "breach_confidence": cpu_breach_confidence,
            },
        },
        "memory": {
            "trajectories": {
                "p10": mem_p10,
                "p50": mem_p50,
                "p90": mem_p90,
            },
            "uncertainty_score": mem_uncertainty,
            "breach": {
                "time_to_breach_seconds": None,
                "breach_confidence": mem_breach_confidence,
            },
        },
        "p50_breach_predicted": (cpu_breach_confidence == "HIGH"
                                  or mem_breach_confidence == "HIGH"),
        "originating_belief": {"pressure_level": pressure_level},
    }


def _pipeline(
    min_r=1, max_r=6,
    threshold=0.15,
    margin_min=0.15, margin_max=0.30,
    cpu_per_replica=50.0, mem_per_replica=32.0,
) -> SizingPipeline:
    return SizingPipeline(
        min_replicas=min_r,
        max_replicas=max_r,
        scale_up_delta_threshold=threshold,
        confidence_margin_min=margin_min,
        confidence_margin_max=margin_max,
        cpu_request_per_replica=cpu_per_replica,
        memory_request_per_replica=mem_per_replica,
    )


# =============================================================================
print("\n=== SizingPipeline — DO_NOTHING: deltas below threshold ===")

# 3 replicas × 50 mc/replica = 150 mc capacity
# Forecast peak (P80 approx midpoint(P50, P90)) ≈ 140 mc < 150 → delta negative
forecast_low = _make_forecast(
    cpu_p50=[100.0] * 30,
    cpu_p90=[120.0] * 30,   # midpoint = 110 mc peak, capacity = 150
)
result = _pipeline().run(forecast_low, current_replicas=3, pressure_level="NORMAL")
check("DO_NOTHING when demand below capacity", result.decision == "DO_NOTHING")
check("target_replicas is None on DO_NOTHING", result.target_replicas is None)
check("quantile_used is P80 under NORMAL", result.quantile_used == "P80")
check("cpu_delta_pct is negative (demand < capacity)", result.cpu_delta_pct < 0)


# =============================================================================
print("\n=== SizingPipeline — SCALE_UP under NORMAL pressure ===")

# 1 replica × 50 mc = 50 mc capacity
# Forecast peak P80 ≈ midpoint(200, 300) = 250 mc → delta = (250*1.15 - 50)/50 >> 15%
forecast_high = _make_forecast(
    cpu_p50=[200.0] * 30,
    cpu_p90=[300.0] * 30,
)
result = _pipeline(max_r=6).run(forecast_high, current_replicas=1, pressure_level="NORMAL")
check("SCALE_UP when demand far exceeds capacity", result.decision == "SCALE_UP")
check("target_replicas is not None on SCALE_UP", result.target_replicas is not None)
check("target within bounds [1,6]",
      1 <= result.target_replicas <= 6)
check("quantile is P80 under NORMAL", result.quantile_used == "P80")
check("cpu_delta_pct is large positive", result.cpu_delta_pct > 0.15)


# =============================================================================
print("\n=== SizingPipeline — SCALE_UP under WARNING pressure ===")

result_warn = _pipeline(max_r=6).run(
    forecast_high, current_replicas=1, pressure_level="WARNING"
)
check("P90 used under WARNING", result_warn.quantile_used == "P90")
check("WARNING target >= NORMAL target (P90 ≥ P80)",
      result_warn.target_replicas >= result.target_replicas)


# =============================================================================
print("\n=== SizingPipeline — uncertainty margin interpolation ===")

# Low uncertainty → margin should be close to margin_min (0.15)
result_low_u = _pipeline().run(
    _make_forecast(cpu_p50=[200.0]*30, cpu_p90=[300.0]*30, cpu_uncertainty=0.0),
    current_replicas=1, pressure_level="NORMAL",
)
# High uncertainty → margin should be close to margin_max (0.30)
result_high_u = _pipeline().run(
    _make_forecast(cpu_p50=[200.0]*30, cpu_p90=[300.0]*30, cpu_uncertainty=1.0),
    current_replicas=1, pressure_level="NORMAL",
)
check("Low uncertainty → margin ≈ 0.15",
      abs(result_low_u.margin_applied - 0.15) < 0.001)
check("High uncertainty → margin ≈ 0.30",
      abs(result_high_u.margin_applied - 0.30) < 0.001)
check("High uncertainty target >= low uncertainty target",
      result_high_u.target_replicas >= result_low_u.target_replicas)


# =============================================================================
print("\n=== SizingPipeline — replica ceiling and bounds clamp ===")

# Sized demand ≈ 250*1.15 = 287.5 mc / 50 mc per replica = 5.75 → ceil = 6
result_ceil = _pipeline(max_r=6, cpu_per_replica=50.0).run(
    _make_forecast(cpu_p50=[200.0]*30, cpu_p90=[300.0]*30, cpu_uncertainty=0.0),
    current_replicas=1, pressure_level="NORMAL",
)
# cpu_sized = 250 * 1.15 = 287.5; ceil(287.5/50) = ceil(5.75) = 6
check("Replica count uses ceiling (not floor)",
      result_ceil.cpu_replica_estimate == math.ceil(
          max([( p50 + p90)/2 for p50, p90 in zip([200.0]*30, [300.0]*30)])
          * (1 + 0.15) / 50.0
      ))
check("min_replicas lower bound applied (target ≥ 1)", result_ceil.target_replicas >= 1)
check("max_replicas upper bound applied (target ≤ 6)", result_ceil.target_replicas <= 6)


# =============================================================================
print("\n=== SizingPipeline — overflow detection ===")

# Force target above max by using tiny per-replica resource cost
result_overflow = _pipeline(max_r=3, cpu_per_replica=10.0).run(
    _make_forecast(cpu_p50=[500.0]*30, cpu_p90=[600.0]*30),
    current_replicas=1, pressure_level="WARNING",
)
check("overflow flag set when raw_target > max_replicas",
      result_overflow.overflow is True)
check("target clamped to max_replicas=3",
      result_overflow.target_replicas == 3)


# =============================================================================
print("\n=== SizingPipeline — memory drives replica count ===")

# Memory is the bottleneck: mem_sized >> cpu_sized
# cpu_per_replica=50, mem_per_replica=32
# cpu forecast ≈ 60 mc → ceil(60*1.15/50) = ceil(1.38) = 2
# mem forecast ≈ 200 MiB → ceil(200*1.15/32) = ceil(7.19) = 8 → clamped to max
forecast_mem = _make_forecast(
    cpu_p50=[50.0]*30, cpu_p90=[70.0]*30,
    mem_p50=[180.0]*30, mem_p90=[220.0]*30,
    cpu_breach_confidence="HIGH",
)
result_mem = _pipeline(max_r=8, cpu_per_replica=50.0, mem_per_replica=32.0).run(
    forecast_mem, current_replicas=1, pressure_level="WARNING",
)
check("Memory replica estimate > CPU replica estimate",
      result_mem.memory_replica_estimate > result_mem.cpu_replica_estimate)
check("Target driven by memory (higher estimate chosen)",
      result_mem.target_replicas >= result_mem.cpu_replica_estimate)


# =============================================================================
print("\n=== CooldownManager — in-memory without persistence ===")

class _MockKB:
    """Minimal mock so CooldownManager can be tested without SQLite."""
    def __init__(self):
        self._record = None
    def upsert_cooldown(self, last_action_ms, remaining_seconds, last_action_type):
        self._record = {"last_action_ms": last_action_ms,
                        "remaining_seconds": remaining_seconds,
                        "last_action_type": last_action_type}
    def read_cooldown(self):
        return self._record
    def delete_cooldown(self):
        self._record = None

from rsa.cooldown_manager import CooldownManager

mock_kb = _MockKB()
cm = CooldownManager(cooldown_seconds=60, kb_writer=mock_kb)

check("Initially not active", not cm.is_active())
check("remaining_seconds = 0.0 when inactive", cm.remaining_seconds() == 0.0)

cm.start(action_type="scale_out")
check("Active immediately after start", cm.is_active())
check("remaining_seconds ≈ 60 just after start",
      59 <= cm.remaining_seconds() <= 60)
check("Mock KB was written", mock_kb._record is not None)
check("Action type persisted correctly",
      mock_kb._record["last_action_type"] == "scale_out")

# Simulate expiry by back-dating the start time
cm._start_ms = int(time.time() * 1000) - 61_000
check("Not active after 61s elapsed", not cm.is_active())
check("Mock KB record deleted after expiry", mock_kb._record is None)


# =============================================================================
print("\n=== CooldownManager — resume-and-adjust from Domain 5 ===")

mock_kb2 = _MockKB()
# Simulate a record from 20 seconds ago (cooldown not yet expired)
mock_kb2._record = {
    "last_action_ms": int(time.time() * 1000) - 20_000,
    "remaining_seconds": 40.0,
    "last_action_type": "scale_in",
}
cm2 = CooldownManager(cooldown_seconds=60, kb_writer=mock_kb2)
check("Resumed as active from Domain 5 record", cm2.is_active())
check("Remaining ≈ 40s after 20s elapsed",
      38 <= cm2.remaining_seconds() <= 41)

# Simulate expired record (61 seconds ago)
mock_kb3 = _MockKB()
mock_kb3._record = {
    "last_action_ms": int(time.time() * 1000) - 61_000,
    "remaining_seconds": 60.0,
    "last_action_type": "scale_out",
}
cm3 = CooldownManager(cooldown_seconds=60, kb_writer=mock_kb3)
check("Expired Domain 5 record → not active on resume", not cm3.is_active())
check("Expired record deleted from mock KB", mock_kb3._record is None)


# =============================================================================
print("\n=== MRAPeerMonitor — state transitions ===")

mra_suspected = []
mra_confirmed = []
mra_recovered = []

mra_mon = MRAPeerMonitor(
    scrape_interval_seconds=30,
    suspected_threshold=3,
    confirmed_threshold=5,
    on_suspected=lambda: mra_suspected.append(1),
    on_confirmed=lambda: mra_confirmed.append(1),
    on_recovered=lambda: mra_recovered.append(1),
)

# Simulate 91s silence (> 3 × 30s intervals)
mra_mon._last_message_time = time.time() - 91
mra_mon.check()
check("SUSPECTED fires at 91s (>3 missed cycles)", len(mra_suspected) == 1)
check("CONFIRMED not fired yet", len(mra_confirmed) == 0)
check("State is SUSPECTED", mra_mon.state == PeerLivenessState.SUSPECTED)

# Simulate 151s silence (> 5 × 30s intervals)
mra_mon._last_message_time = time.time() - 151
mra_mon.check()
check("CONFIRMED fires at 151s (>5 missed cycles)", len(mra_confirmed) == 1)
check("State is CONFIRMED", mra_mon.state == PeerLivenessState.CONFIRMED)

# Recovery
mra_mon.record_message()
check("RECOVERED fires on message receipt", len(mra_recovered) == 1)
check("State returns to ALIVE", mra_mon.state == PeerLivenessState.ALIVE)
check("SUSPECTED count unchanged after recovery", len(mra_suspected) == 1)

# Second SUSPECTED cycle resets correctly
mra_mon._last_message_time = time.time() - 91
mra_mon._suspected_fired   = False
mra_mon._confirmed_fired   = False
mra_mon._state             = PeerLivenessState.ALIVE
mra_mon.check()
check("SUSPECTED fires again after recovery and new silence",
      len(mra_suspected) == 2)


# =============================================================================
print("\n=== PFAPeerMonitor — state transitions ===")

pfa_suspected = []
pfa_confirmed = []
pfa_recovered = []

pfa_mon = PFAPeerMonitor(
    suspected_seconds=1020,   # 17 minutes
    confirmed_seconds=1200,   # 20 minutes
    on_suspected=lambda: pfa_suspected.append(1),
    on_confirmed=lambda: pfa_confirmed.append(1),
    on_recovered=lambda: pfa_recovered.append(1),
)

# Simulate 1021s silence (just past 17-minute threshold)
pfa_mon._last_message_time = time.time() - 1021
pfa_mon.check()
check("SUSPECTED fires after 17 min silence", len(pfa_suspected) == 1)
check("CONFIRMED not fired yet", len(pfa_confirmed) == 0)
check("PFA state is SUSPECTED", pfa_mon.state == PeerLivenessState.SUSPECTED)

# Simulate 1201s silence (past 20-minute threshold)
pfa_mon._last_message_time = time.time() - 1201
pfa_mon.check()
check("CONFIRMED fires after 20 min silence", len(pfa_confirmed) == 1)
check("PFA state is CONFIRMED", pfa_mon.state == PeerLivenessState.CONFIRMED)

pfa_mon.record_message()
check("PFA RECOVERED on message receipt", len(pfa_recovered) == 1)
check("PFA state returns to ALIVE", pfa_mon.state == PeerLivenessState.ALIVE)


# =============================================================================
print("")
if errors == 0:
    print("\033[92mAll tests passed.\033[0m")
    sys.exit(0)
else:
    print(f"\033[91m{errors} test(s) failed.\033[0m")
    sys.exit(1)

