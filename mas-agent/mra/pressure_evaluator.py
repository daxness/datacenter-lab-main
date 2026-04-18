# mra/pressure_evaluator.py
#
# Resource Pressure Evaluator — MRA Specification Section 4.
#
# Classification logic (Section 4.3 transition rules):
#   CRITICAL → immediate on first sample above threshold (no confirmation)
#   WARNING  → requires 2 consecutive samples above threshold
#   NORMAL   → recovery from WARNING requires 2 consecutive samples BELOW threshold
#
# State: consecutive-threshold counters. Never persisted (Spec Section 8.3).
# On restart the counters reset to zero — a WARNING transition may be delayed
# by one additional interval. This is explicitly acceptable per the spec.
#
from enum import Enum
import structlog
 
log = structlog.get_logger(__name__)
 
 
class PressureLevel(str, Enum):
    NORMAL       = "NORMAL"
    WARNING      = "WARNING"
    CRITICAL     = "CRITICAL"
    UNCONFIGURED = "UNCONFIGURED"
 
 
class ResourcePressureEvaluator:
    """
    One instance per MRA (per monitored deployment).
    Maintains consecutive crossing counters across scrape cycles.
    """
 
    def __init__(
        self,
        cpu_warning_pct: float,
        cpu_critical_pct: float,
        memory_warning_pct: float,
        memory_critical_pct: float,
        warning_consecutive: int = 2,
    ):
        self._cpu_warn  = cpu_warning_pct
        self._cpu_crit  = cpu_critical_pct
        self._mem_warn  = memory_warning_pct
        self._mem_crit  = memory_critical_pct
        self._n         = warning_consecutive
 
        # Counters for WARNING threshold crossings
        self._cpu_above_count = 0
        self._mem_above_count = 0
 
        # Counters for recovery below WARNING threshold
        self._cpu_below_count = 0
        self._mem_below_count = 0
 
        self._level = PressureLevel.NORMAL
 
    def evaluate(
        self,
        cpu_usage: float,
        cpu_limit: float,
        memory_usage: float,
        memory_limit: float,
    ) -> PressureLevel:
 
        cpu_ratio = cpu_usage / cpu_limit
        mem_ratio = memory_usage / memory_limit
 
        # ---- CRITICAL: immediate, no confirmation window ----
        if cpu_ratio >= self._cpu_crit or mem_ratio >= self._mem_crit:
            self._cpu_above_count = 0
            self._mem_above_count = 0
            self._cpu_below_count = 0
            self._mem_below_count = 0
            self._level = PressureLevel.CRITICAL
            log.info("pressure_critical",
                     cpu_pct=round(cpu_ratio * 100, 1),
                     mem_pct=round(mem_ratio * 100, 1))
            return PressureLevel.CRITICAL
 
        # ---- Update consecutive counters ----
        if cpu_ratio >= self._cpu_warn:
            self._cpu_above_count += 1
            self._cpu_below_count = 0
        else:
            self._cpu_below_count += 1
            self._cpu_above_count = 0
 
        if mem_ratio >= self._mem_warn:
            self._mem_above_count += 1
            self._mem_below_count = 0
        else:
            self._mem_below_count += 1
            self._mem_above_count = 0
 
        # ---- WARNING: requires N consecutive crossings ----
        cpu_warns = self._cpu_above_count >= self._n
        mem_warns = self._mem_above_count >= self._n
 
        if cpu_warns or mem_warns:
            self._level = PressureLevel.WARNING
            log.info("pressure_warning",
                     cpu_consec=self._cpu_above_count,
                     mem_consec=self._mem_above_count)
            return PressureLevel.WARNING
 
        # ---- Recovery hysteresis ----
        # If currently in WARNING, stay there until N consecutive below threshold
        if self._level == PressureLevel.WARNING:
            if self._cpu_below_count >= self._n and self._mem_below_count >= self._n:
                self._level = PressureLevel.NORMAL
                log.info("pressure_recovered_to_normal")
            return self._level
 
        # ---- NORMAL ----
        self._level = PressureLevel.NORMAL
        return PressureLevel.NORMAL
 
    @property
    def current_level(self) -> PressureLevel:
        return self._level
