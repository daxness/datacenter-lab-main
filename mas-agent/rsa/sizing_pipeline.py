# rsa/sizing_pipeline.py
#
# The RSA's 8-stage proactive sizing pipeline — RSA Specification Section 4
# (Deliberation Cycle, Proactive path).
#
# This pipeline is invoked ONLY on the proactive path, triggered by a
# FORECAST_UPDATE from the PFA where at least one metric carries a
# non-null time_to_breach_seconds value (breach_confidence == "HIGH").
#
# It must NOT be invoked on the emergency path (CRITICAL → scale to max
# immediately, no deliberation) or the reactive scale-down path (current
# utilization sizing, no forecast values).
#
# Pipeline stages:
#   1. Quantile selection      P80 (NORMAL) or P90 (WARNING)
#   2. Uncertainty margin      [15%, 30%] interpolated from PFA uncertainty
#   3. Margin application      sized_demand = forecast_peak × (1 + margin)
#   4. Independent per-metric  CPU and memory evaluated separately
#   5. Delta computation       (sized_demand - current_capacity) / current_capacity
#   6. Decision threshold      SCALE_UP if any delta > +15%, else DO_NOTHING
#   7. Replica estimate        ceil(sized_demand / resource_per_replica) per metric
#   8. Bounds clamp            apply min_replicas / max_replicas
#
# The FORECAST_UPDATE object produced by the PFA has this structure:
#   {
#     "cpu": {
#       "trajectories": {"p10": [...], "p50": [...], "p90": [...]},
#       "uncertainty_score": float,
#       "breach": {"time_to_breach_seconds": float|None, "breach_confidence": str}
#     },
#     "memory": { same structure },
#     "originating_belief": { full MRA belief object }
#   }
#
import math
import structlog
from dataclasses import dataclass

log = structlog.get_logger(__name__)


@dataclass
class PipelineResult:
    """
    Complete output of one sizing pipeline execution.
    All fields are written to the Domain 4 audit log regardless of outcome.
    """
    decision: str                       # "SCALE_UP" | "DO_NOTHING"
    target_replicas: int | None         # None on DO_NOTHING
    current_replicas: int
    pressure_level: str                 # drove quantile choice
    quantile_used: str                  # "P80" | "P90"
    margin_applied: float               # e.g. 0.22 = 22%
    uncertainty_score: float            # higher of cpu/memory PFA uncertainty
    cpu_forecast_peak: float            # peak value from selected quantile
    cpu_sized_demand: float             # after margin (millicores)
    cpu_current_capacity: float         # current_replicas × cpu_per_replica
    cpu_delta_pct: float                # signed delta
    cpu_replica_estimate: int           # ceil(cpu_sized / cpu_per_replica)
    memory_forecast_peak: float
    memory_sized_demand: float
    memory_current_capacity: float
    memory_delta_pct: float
    memory_replica_estimate: int
    overflow: bool = False              # raw target exceeded max_replicas


class SizingPipeline:
    """
    Executes the 8-stage proactive sizing pipeline.

    All parameters are injected at construction from the RSAConfig so
    the pipeline itself is pure, stateless, and testable in isolation.
    """

    def __init__(
        self,
        min_replicas: int,
        max_replicas: int,
        scale_up_delta_threshold: float,    # 0.15 per spec
        confidence_margin_min: float,       # 0.15
        confidence_margin_max: float,       # 0.30
        cpu_request_per_replica: float,     # millicores
        memory_request_per_replica: float,  # MiB
    ):
        self.min_replicas              = min_replicas
        self.max_replicas              = max_replicas
        self.scale_up_delta_threshold  = scale_up_delta_threshold
        self.margin_min                = confidence_margin_min
        self.margin_max                = confidence_margin_max
        self.cpu_per_replica           = cpu_request_per_replica
        self.memory_per_replica        = memory_request_per_replica

    def run(
        self,
        forecast_obj: dict,
        current_replicas: int,
        pressure_level: str,
    ) -> PipelineResult:
        """
        Execute the full 8-stage sizing pipeline.

        Parameters
        ----------
        forecast_obj     : Parsed FORECAST_UPDATE payload from the PFA.
        current_replicas : Current replica count from the Kubernetes API.
        pressure_level   : "NORMAL" or "WARNING" from the originating belief.
        """

        # ── Stage 1: Quantile selection ────────────────────────────────────
        # P80 under NORMAL: breach is predicted but not yet imminent.
        # P90 under WARNING: elevated pressure demands more headroom.
        use_p90 = (pressure_level == "WARNING")
        quantile_key   = "p90" if use_p90 else "p80"
        quantile_label = "P90" if use_p90 else "P80"

        cpu_traj = forecast_obj["cpu"]["trajectories"]
        mem_traj = forecast_obj["memory"]["trajectories"]

        # P80 is not natively output by TimesFM — we approximate it as the
        # midpoint between P50 and P90, which gives a conservative but
        # tractable estimate without requiring a separate quantile output.
        # When the model produces P80 natively in a future revision, replace
        # this with cpu_traj["p80"] directly.
        if use_p90:
            cpu_trajectory = cpu_traj["p90"]
            mem_trajectory = mem_traj["p90"]
        else:
            # P80 approximation: midpoint(P50, P90)
            cpu_trajectory = [
                (p50 + p90) / 2.0
                for p50, p90 in zip(cpu_traj["p50"], cpu_traj["p90"])
            ]
            mem_trajectory = [
                (p50 + p90) / 2.0
                for p50, p90 in zip(mem_traj["p50"], mem_traj["p90"])
            ]

        # We size for the PEAK value within the 15-minute forecast horizon,
        # not the mean, because replicas must cover worst-case demand.
        cpu_forecast_peak = max(cpu_trajectory)
        mem_forecast_peak = max(mem_trajectory)

        # ── Stage 2: Uncertainty margin ────────────────────────────────────
        # Take the higher of the two per-metric uncertainty scores.
        # This is conservative: if either resource has high uncertainty,
        # we apply the wider margin to both.
        cpu_uncertainty = forecast_obj["cpu"].get("uncertainty_score", 0.5)
        mem_uncertainty = forecast_obj["memory"].get("uncertainty_score", 0.5)
        uncertainty = max(
            max(0.0, min(1.0, cpu_uncertainty)),
            max(0.0, min(1.0, mem_uncertainty)),
        )

        # Linear interpolation between margin_min and margin_max
        margin = self.margin_min + uncertainty * (self.margin_max - self.margin_min)

        # ── Stage 3: Margin application ────────────────────────────────────
        cpu_sized = cpu_forecast_peak * (1.0 + margin)
        mem_sized = mem_forecast_peak * (1.0 + margin)

        # ── Stage 4 & 5: Per-metric capacity and delta ─────────────────────
        # Current allocated capacity = current_replicas × resource_per_replica
        cpu_capacity = current_replicas * self.cpu_per_replica
        mem_capacity = current_replicas * self.memory_per_replica

        # Signed delta: positive = demand exceeds provision → candidate for scale-up
        cpu_delta = (cpu_sized - cpu_capacity) / max(cpu_capacity, 1.0)
        mem_delta = (mem_sized - mem_capacity) / max(mem_capacity, 1.0)

        # ── Stage 6: Decision threshold ────────────────────────────────────
        # Scale up if ANY metric's delta exceeds +15%.
        # There is deliberately NO downside delta threshold here:
        # scale-down is exclusively the reactive path's responsibility.
        if cpu_delta <= self.scale_up_delta_threshold and \
           mem_delta <= self.scale_up_delta_threshold:
            log.info(
                "pipeline_do_nothing",
                cpu_delta_pct=round(cpu_delta * 100, 1),
                mem_delta_pct=round(mem_delta * 100, 1),
                threshold_pct=round(self.scale_up_delta_threshold * 100, 1),
                pressure=pressure_level,
                quantile=quantile_label,
            )
            return PipelineResult(
                decision="DO_NOTHING",
                target_replicas=None,
                current_replicas=current_replicas,
                pressure_level=pressure_level,
                quantile_used=quantile_label,
                margin_applied=round(margin, 4),
                uncertainty_score=round(uncertainty, 4),
                cpu_forecast_peak=round(cpu_forecast_peak, 3),
                cpu_sized_demand=round(cpu_sized, 3),
                cpu_current_capacity=round(cpu_capacity, 3),
                cpu_delta_pct=round(cpu_delta, 4),
                cpu_replica_estimate=0,
                memory_forecast_peak=round(mem_forecast_peak, 3),
                memory_sized_demand=round(mem_sized, 3),
                memory_current_capacity=round(mem_capacity, 3),
                memory_delta_pct=round(mem_delta, 4),
                memory_replica_estimate=0,
            )

        # ── Stage 7: Replica estimate ──────────────────────────────────────
        # ceil(sized_demand / resource_per_replica): ceiling ensures we
        # never under-provision due to fractional replica counts.
        cpu_replicas = math.ceil(cpu_sized / max(self.cpu_per_replica, 1.0))
        mem_replicas = math.ceil(mem_sized / max(self.memory_per_replica, 1.0))

        # Take the higher of the two estimates to provide simultaneous
        # headroom for both resources.
        raw_target = max(cpu_replicas, mem_replicas)

        # ── Stage 8: Replica bounds clamp ──────────────────────────────────
        overflow = raw_target > self.max_replicas
        target   = max(self.min_replicas, min(self.max_replicas, raw_target))

        log.info(
            "pipeline_scale_up",
            raw_target=raw_target,
            target_clamped=target,
            overflow=overflow,
            cpu_replicas=cpu_replicas,
            mem_replicas=mem_replicas,
            margin_pct=round(margin * 100, 1),
            quantile=quantile_label,
            pressure=pressure_level,
        )

        return PipelineResult(
            decision="SCALE_UP",
            target_replicas=target,
            current_replicas=current_replicas,
            pressure_level=pressure_level,
            quantile_used=quantile_label,
            margin_applied=round(margin, 4),
            uncertainty_score=round(uncertainty, 4),
            cpu_forecast_peak=round(cpu_forecast_peak, 3),
            cpu_sized_demand=round(cpu_sized, 3),
            cpu_current_capacity=round(cpu_capacity, 3),
            cpu_delta_pct=round(cpu_delta, 4),
            cpu_replica_estimate=cpu_replicas,
            memory_forecast_peak=round(mem_forecast_peak, 3),
            memory_sized_demand=round(mem_sized, 3),
            memory_current_capacity=round(mem_capacity, 3),
            memory_delta_pct=round(mem_delta, 4),
            memory_replica_estimate=mem_replicas,
            overflow=overflow,
        )

