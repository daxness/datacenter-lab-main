import math
import time
import numpy as np
import structlog
from dataclasses import dataclass
from typing import Optional

log = structlog.get_logger(__name__)


@dataclass
class QuantileTrajectory:
    p10: list
    p50: list
    p90: list
    model_used: str


@dataclass
class InferenceResult:
    cpu: QuantileTrajectory
    memory: QuantileTrajectory
    inference_duration_ms: float
    timestamp_ms: int


class InferenceEngine:

    def __init__(self, forecast_steps: int = 30, patch_size: int = 32):
        self._steps = forecast_steps
        self._patch = patch_size
        self._model = None
        self._model_name = "exponential_smoothing_fallback"
        self._load_model()

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------

    def _load_model(self):
        # FIX: body was at column-0 — indented correctly inside the method now
        try:
            import timesfm
            self._model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
                "google/timesfm-2.5-200m-pytorch",
                torch_compile=False,
            )
            self._model.compile(
                timesfm.ForecastConfig(
                    max_context=1024,
                    max_horizon=self._steps,
                    normalize_inputs=True,
                    use_continuous_quantile_head=True,
                    force_flip_invariance=True,
                    infer_is_positive=True,
                    fix_quantile_crossing=True,
                )
            )
            self._model_name = "timesfm-2.5-200m"
            log.info("timesfm_loaded", model=self._model_name)
        except ImportError:
            log.warning("timesfm_not_installed")
        except Exception as e:
            log.error("timesfm_load_failed", error=str(e))
            raise

    # ------------------------------------------------------------------
    # TimesFM inference path
    # ------------------------------------------------------------------

    def _run_timesfm(self, series: list) -> QuantileTrajectory:
        # FIX: body was at column-0 — indented correctly inside the method now
        pf, qf = self._model.forecast(
            horizon=self._steps,
            inputs=[np.array(series, dtype=np.float32)],
        )
        p50 = [max(0.0, float(v)) for v in pf[0].tolist()]
        try:
            p10 = [max(0.0, float(v)) for v in qf[0, :, 1].tolist()]
            p90 = [max(0.0, float(v)) for v in qf[0, :, 9].tolist()]
        except Exception:
            p10 = p50
            p90 = p50
        return QuantileTrajectory(
            p10=p10,
            p50=p50,
            p90=p90,
            model_used=self._model_name,
        )

    # ------------------------------------------------------------------
    # Exponential-smoothing fallback path
    # ------------------------------------------------------------------

    def _run_fallback(self, series: list) -> QuantileTrajectory:
        if not series:
            flat = [0.0] * self._steps
            return QuantileTrajectory(
                p10=flat, p50=flat, p90=flat,
                model_used=self._model_name,
            )
        alpha = 0.3
        s = series[0]
        for v in series[1:]:
            s = alpha * v + (1 - alpha) * s
        recent = series[-min(10, len(series)):]
        std = max(float(np.std(recent)) if len(recent) > 1 else s * 0.1, 1.0)
        p50, p10, p90 = [], [], []
        for step in range(1, self._steps + 1):
            spread = std * math.sqrt(step)
            p50.append(max(0.0, s))
            p10.append(max(0.0, s - 1.28 * spread))
            p90.append(max(0.0, s + 1.28 * spread))
        return QuantileTrajectory(
            p10=p10, p50=p50, p90=p90,
            model_used=self._model_name,
        )

    # ------------------------------------------------------------------
    # Public entry point — called by the PFA on every inference cycle
    # ------------------------------------------------------------------

    def run(
        self,
        cpu_series: list,
        memory_series: list,
    ) -> InferenceResult:
        """
        Execute one inference cycle for both CPU and memory series.

        Dispatches to TimesFM when the model is loaded; falls back to
        exponential smoothing if the model is unavailable or raises an
        unrecoverable exception during inference.

        Parameters
        ----------
        cpu_series:    Ordered list of cpu_usage_millicores observations
                       from the PFA sliding window (oldest → newest).
        memory_series: Ordered list of memory_usage_MiB observations
                       from the PFA sliding window (oldest → newest).

        Returns
        -------
        InferenceResult containing QuantileTrajectory objects for both
        resources, the wall-clock inference duration in milliseconds, and
        a UTC Unix epoch timestamp in milliseconds.
        """
        t0 = time.monotonic()

        if self._model is not None:
            try:
                cpu_traj = self._run_timesfm(cpu_series)
                mem_traj = self._run_timesfm(memory_series)
            except Exception as e:
                log.error(
                    "timesfm_inference_failed_falling_back",
                    error=str(e),
                )
                cpu_traj = self._run_fallback(cpu_series)
                mem_traj = self._run_fallback(memory_series)
        else:
            cpu_traj = self._run_fallback(cpu_series)
            mem_traj = self._run_fallback(memory_series)

        duration_ms = (time.monotonic() - t0) * 1000.0

        return InferenceResult(
            cpu=cpu_traj,
            memory=mem_traj,
            inference_duration_ms=round(duration_ms, 2),
            timestamp_ms=int(time.time() * 1000),
        )


# ----------------------------------------------------------------------
# Module-level utility functions consumed by the PFA after inference
# ----------------------------------------------------------------------

def compute_uncertainty(traj: QuantileTrajectory, fill_ratio: float) -> float:
    """
    Compute a normalised [0, 1] uncertainty score for a single trajectory.

    The raw score is the mean normalised P90–P10 spread across all forecast
    steps, anchored to the P50 central estimate to remain scale-agnostic.
    A proportional penalty is added for the forward-fill ratio present in
    the active sliding window, consistent with Section 4.4 of the PFA spec.
    """
    spreads = [
        (p90 - p10) / max(p50, 1.0)
        for p10, p50, p90 in zip(traj.p10, traj.p50, traj.p90)
    ]
    raw = float(np.mean(spreads)) if spreads else 0.0
    return min(1.0, max(0.0, raw) + fill_ratio)


def compute_time_to_breach(
    traj: QuantileTrajectory,
    breach_threshold: float,
    step_duration_seconds: int = 30,
) -> dict:
    """
    Scan quantile trajectories to derive time-to-breach estimates.

    Consistent with Section 4.5 of the PFA spec:
    - RSA deliberation is triggered only when P50 crosses the threshold.
    - P90 crossing alone yields PARTIAL confidence — no RSA deliberation.
    - No crossing on any trajectory yields NONE confidence.

    Returns a dict with keys:
        time_to_breach_seconds      — P50-based; None if no breach in horizon
        breach_window_pessimistic   — P90-based (earliest plausible breach)
        breach_window_optimistic    — P10-based (latest plausible breach)
        breach_confidence           — HIGH | PARTIAL | NONE
        breach_threshold            — echoed for downstream audit
    """
    def first_crossing(trajectory: list) -> Optional[int]:
        for i, v in enumerate(trajectory):
            if v >= breach_threshold:
                return i
        return None

    def to_seconds(step_index: Optional[int]) -> Optional[float]:
        if step_index is None:
            return None
        return float((step_index + 1) * step_duration_seconds)

    p50_step = first_crossing(traj.p50)
    p90_step = first_crossing(traj.p90)
    p10_step = first_crossing(traj.p10)

    if p50_step is not None:
        confidence = "HIGH"
    elif p90_step is not None:
        confidence = "PARTIAL"
    else:
        confidence = "NONE"

    return {
        "time_to_breach_seconds":    to_seconds(p50_step),
        "breach_window_pessimistic": to_seconds(p90_step),
        "breach_window_optimistic":  to_seconds(p10_step),
        "breach_confidence":         confidence,
        "breach_threshold":          round(breach_threshold, 3),
    }
