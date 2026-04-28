# pfa/forecast_builder.py — assembles the FORECAST_UPDATE object
import json
from .inference_engine import compute_uncertainty, compute_time_to_breach

def build_forecast_update(result, cpu_window_fill_ratio, memory_window_fill_ratio,
                          cpu_limit_millicores, memory_limit_MiB,
                          cpu_breach_pct, memory_breach_pct,
                          step_duration_seconds, originating_belief,
                          namespace, deployment):
    cpu_uncertainty = compute_uncertainty(result.cpu, cpu_window_fill_ratio)
    mem_uncertainty = compute_uncertainty(result.memory, memory_window_fill_ratio)
    cpu_breach_abs = cpu_limit_millicores * cpu_breach_pct
    mem_breach_abs = memory_limit_MiB * memory_breach_pct
    cpu_breach_info = compute_time_to_breach(result.cpu, cpu_breach_abs, step_duration_seconds)
    mem_breach_info = compute_time_to_breach(result.memory, mem_breach_abs, step_duration_seconds)
    p50_breach = (cpu_breach_info["breach_confidence"] == "HIGH" or
                  mem_breach_info["breach_confidence"] == "HIGH")
    return {
        "event_type":   "FORECAST_UPDATE",
        "namespace":    namespace,
        "deployment":   deployment,
        "timestamp_ms": result.timestamp_ms,
        "model_used":   result.cpu.model_used,
        "inference_duration_ms": result.inference_duration_ms,
        "forecast_steps":        len(result.cpu.p50),
        "step_duration_seconds": step_duration_seconds,
        "cpu": {
            "trajectories": {
                "p10": [round(v,4) for v in result.cpu.p10],
                "p50": [round(v,4) for v in result.cpu.p50],
                "p90": [round(v,4) for v in result.cpu.p90],
            },
            "uncertainty_score": round(cpu_uncertainty, 4),
            "fill_ratio":        round(cpu_window_fill_ratio, 4),
            "breach":            cpu_breach_info,
        },
        "memory": {
            "trajectories": {
                "p10": [round(v,4) for v in result.memory.p10],
                "p50": [round(v,4) for v in result.memory.p50],
                "p90": [round(v,4) for v in result.memory.p90],
            },
            "uncertainty_score": round(mem_uncertainty, 4),
            "fill_ratio":        round(memory_window_fill_ratio, 4),
            "breach":            mem_breach_info,
        },
        "p50_breach_predicted": p50_breach,
        "originating_belief":   originating_belief,
    }

def forecast_to_json(forecast):
    return json.dumps(forecast, ensure_ascii=False, separators=(",", ":"))
