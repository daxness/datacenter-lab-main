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
            self._model = timesfm.TimesFm(
                hparams=timesfm.TimesFmHparams(
                    backend="cpu", per_core_batch_size=1,
                    horizon_len=self._steps, input_patch_len=self._patch,
                    output_patch_len=self._patch, num_layers=20,
                    model_dims=1280, quantiles=[0.1, 0.5, 0.9],
                ),
                checkpoint=timesfm.TimesFmCheckpoint(
                    huggingface_repo_id="google/timesfm-2.0-500m-pytorch"),
            )
            self._model_name = "timesfm-2.0-500m"
            log.info("timesfm_loaded", model=self._model_name)
        except ImportError:
            log.warning("timesfm_not_installed", msg="using fallback")
        except Exception as e:
            log.error("timesfm_load_failed", error=str(e))

    def run(self, cpu_input, memory_input):
        start = time.time()
        if self._model is not None:
            cpu_traj = self._run_timesfm(cpu_input)
            mem_traj = self._run_timesfm(memory_input)
        else:
            cpu_traj = self._run_fallback(cpu_input)
            mem_traj = self._run_fallback(memory_input)
        for metric, traj in [("cpu", cpu_traj), ("memory", mem_traj)]:
            for q, vals in [("p10", traj.p10), ("p50", traj.p50), ("p90", traj.p90)]:
                for v in vals:
                    if not math.isfinite(v):
                        raise ValueError(f"Non-finite in {metric} {q}: {v}")
        return InferenceResult(cpu=cpu_traj, memory=mem_traj,
                               inference_duration_ms=round((time.time()-start)*1000, 2),
                               timestamp_ms=int(time.time()*1000))

    def _run_timesfm(self, series):
        pf, qf = self._model.forecast(inputs=[np.array(series, dtype=np.float32)], freq=[0])
        p10 = [max(0., v) for v in qf[0,:,0].tolist()]
        p50 = [max(0., v) for v in qf[0,:,1].tolist()]
        p90 = [max(0., v) for v in qf[0,:,2].tolist()]
        return QuantileTrajectory(p10=p10, p50=p50, p90=p90, model_used=self._model_name)

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
