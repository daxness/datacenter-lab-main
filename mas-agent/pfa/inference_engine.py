import math, time, numpy as np, structlog
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
    def __init__(self, forecast_steps=30, patch_size=32):
        self._steps = forecast_steps
        self._patch = patch_size
        self._model = None
        self._model_name = "exponential_smoothing_fallback"
        self._load_model()

    def _load_model(self):
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

    def _run_timesfm(self, series):
    import numpy as np

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

    def _run_fallback(self, series):
        if not series:
            flat = [0.0]*self._steps
            return QuantileTrajectory(p10=flat, p50=flat, p90=flat, model_used=self._model_name)
        alpha = 0.3
        s = series[0]
        for v in series[1:]: s = alpha*v + (1-alpha)*s
        recent = series[-min(10,len(series)):]
        std = max(float(np.std(recent)) if len(recent)>1 else s*0.1, 1.0)
        p50, p10, p90 = [], [], []
        for step in range(1, self._steps+1):
            spread = std * math.sqrt(step)
            p50.append(max(0., s))
            p10.append(max(0., s - 1.28*spread))
            p90.append(max(0., s + 1.28*spread))
        return QuantileTrajectory(p10=p10, p50=p50, p90=p90, model_used=self._model_name)

def compute_uncertainty(traj, fill_ratio):
    spreads = [(p90-p10)/max(p50,1.) for p10,p50,p90 in zip(traj.p10,traj.p50,traj.p90)]
    raw = max(0., float(np.mean(spreads)) if spreads else 0.)
    return min(1., raw + fill_ratio)

def compute_time_to_breach(traj, breach_threshold, step_duration_seconds=30):
    def first(trajectory):
        for i,v in enumerate(trajectory):
            if v >= breach_threshold: return i
        return None
    p50s = first(traj.p50); p90s = first(traj.p90); p10s = first(traj.p10)
    def to_sec(s): return (s+1)*step_duration_seconds if s is not None else None
    confidence = "HIGH" if p50s is not None else ("PARTIAL" if p90s is not None else "NONE")
    return {"time_to_breach_seconds": to_sec(p50s),
            "breach_window_pessimistic": to_sec(p90s),
            "breach_window_optimistic": to_sec(p10s),
            "breach_confidence": confidence,
            "breach_threshold": round(breach_threshold, 3)}
