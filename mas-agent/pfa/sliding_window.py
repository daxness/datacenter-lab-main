# pfa/sliding_window.py
#
# Sliding window management — PFA Specification Section 4.2.
#
# The PFA maintains exactly TWO independent fixed-length rolling windows,
# one for CPU usage (millicores) and one for memory usage (MiB).
# They are kept strictly separate because TimesFM is univariate — it
# forecasts one time series at a time and cannot model joint CPU/memory
# behaviour. Mixing them would produce meaningless output.
#
# Each window is a Python collections.deque with maxlen=96.
# A deque is a double-ended queue: appending to a full deque automatically
# drops the oldest entry from the left end. This is exactly the FIFO
# (First-In, First-Out) behaviour described in the specification.
#
# Each observation slot stores two values:
#   - value: the metric reading (float)
#   - filled: True if this value came from MRA forward-fill (not a live scrape)
#
# The filled flag is used to compute the forward-fill ratio, which penalises
# the uncertainty score and can trigger INFERENCE_DEGRADED if sustained.
#
from collections import deque
from dataclasses import dataclass
from typing import Optional
import math


@dataclass
class Observation:
    """One timestamped entry in a sliding window."""
    value: float
    filled: bool = False      # True = value was forward-filled by MRA, not live


class SlidingWindow:
    """
    Single fixed-length FIFO window for one metric (CPU or memory).

    One instance per metric per PFA instance.
    Two SlidingWindow objects exist per PFA: one for CPU, one for memory.
    """

    def __init__(self, max_size: int = 96, min_inference: int = 32,
                 patch_size: int = 32):
        """
        Args:
            max_size:       Maximum observations to retain (96 = 48 min at 30s cadence)
            min_inference:  Minimum observations before inference is allowed (32 = 1 patch)
            patch_size:     TimesFM input patch size — inference inputs must be
                            multiples of this value (Spec Section 4.2)
        """
        self._window: deque = deque(maxlen=max_size)
        self._max_size = max_size
        self._min_inference = min_inference
        self._patch_size = patch_size

    def append(self, value: float, filled: bool = False) -> None:
        """
        Add one observation. If the window is full, the oldest entry is
        automatically dropped by the deque's maxlen enforcement.

        Args:
            value:  Metric reading (millicores for CPU, MiB for memory)
            filled: True if this value was forward-filled by the MRA
        """
        self._window.append(Observation(value=value, filled=filled))

    @property
    def size(self) -> int:
        """Current number of observations in the window."""
        return len(self._window)

    @property
    def is_ready(self) -> bool:
        """
        True when the window contains enough observations to run inference.
        Requires at least one full patch (32 observations).
        During warmup (size < 32) the PFA publishes WARMING_UP and does
        not attempt inference.
        """
        return self.size >= self._min_inference

    @property
    def fill_ratio(self) -> float:
        """
        Fraction of current window entries that are forward-filled.
        0.0 = all live scrapes.
        1.0 = all forward-filled (completely stale window).
        Used to penalise uncertainty score and detect INFERENCE_DEGRADED.
        """
        if self.size == 0:
            return 0.0
        filled_count = sum(1 for obs in self._window if obs.filled)
        return filled_count / self.size

    def get_inference_input(self) -> Optional[list]:
        """
        Return the largest prefix of the window whose length is a multiple
        of patch_size (32), taking the most recent observations.

        Spec Section 4.2:
        "The model consumes only the most recent valid segment matching
        the largest multiple of 32 within the current window."

        Examples:
          size=40  → uses last 32 observations  (floor(40/32)*32 = 32)
          size=63  → uses last 32 observations  (floor(63/32)*32 = 32)
          size=64  → uses last 64 observations  (floor(64/32)*32 = 64)
          size=75  → uses last 64 observations  (floor(75/32)*32 = 64)
          size=96  → uses last 96 observations  (floor(96/32)*32 = 96)

        Returns None if the window is not ready for inference.
        """
        if not self.is_ready:
            return None

        n = self.size
        # Largest multiple of patch_size that fits within current window
        usable = (n // self._patch_size) * self._patch_size

        # Convert deque to list, take the most recent `usable` entries
        all_obs = list(self._window)
        selected = all_obs[-usable:]

        return [obs.value for obs in selected]

    def get_fill_ratio_for_input(self) -> float:
        """
        Forward-fill ratio computed only over the inference input slice,
        not the full window. This ensures the uncertainty penalty reflects
        the actual data quality of what the model is seeing.
        """
        if not self.is_ready:
            return 0.0

        n = self.size
        usable = (n // self._patch_size) * self._patch_size
        all_obs = list(self._window)
        selected = all_obs[-usable:]

        if not selected:
            return 0.0
        filled = sum(1 for obs in selected if obs.filled)
        return filled / len(selected)

    def reconstruct_from_history(self, values: list) -> None:
        """
        Populate the window from Domain 3 historical data on restart.

        Spec Section 7.4:
        "All entries reconstructed from Domain 3 are treated as unflagged
        with respect to forward-fill — historical entries cannot be
        retroactively distinguished from live observations."

        So filled=False for all reconstructed entries, which is conservative:
        it does not penalise the uncertainty score for historical data.

        Args:
            values: List of float values in chronological order (oldest first).
                    At most max_size values are used; older ones are discarded.
        """
        self._window.clear()
        # Only keep the most recent max_size values
        for v in values[-self._max_size:]:
            self._window.append(Observation(value=float(v), filled=False))

    @property
    def values(self) -> list:
        """All current values as a plain list (oldest first)."""
        return [obs.value for obs in self._window]

    @property
    def latest_value(self) -> Optional[float]:
        """Most recent observation value, or None if window is empty."""
        if not self._window:
            return None
        return self._window[-1].value

    def reset(self) -> None:
        """Clear the window entirely. Called after confirmed MRA failure."""
        self._window.clear()


# ------------------------------------------------------------------ #
# DualSlidingWindow — manages both windows together (Spec Section 4.2) #
# ------------------------------------------------------------------ #

class DualSlidingWindow:
    """
    Manages the two independent sliding windows (CPU + memory) together.
    This is the object the PFA main loop interacts with.
    Both windows are always updated together from the same belief object.
    """

    def __init__(self, max_size=96, min_inference=32, patch_size=32):
        self.cpu = SlidingWindow(
            max_size=max_size, min_inference=min_inference,
            patch_size=patch_size,
        )
        self.memory = SlidingWindow(
            max_size=max_size, min_inference=min_inference,
            patch_size=patch_size,
        )

    def append_from_belief(self, belief: dict) -> None:
        """
        Extract forecast_metrics from a BELIEF_UPDATE and append to both windows.
        The filled flag is True if the metric appears in belief.filled_values.
        """
        fm = belief.get("forecast_metrics", {})
        filled_list = belief.get("filled_values", [])
        ts = belief.get("timestamp", 0)

        cpu_val = fm.get("cpu_usage_millicores")
        mem_val = fm.get("memory_usage_MiB")

        if cpu_val is not None:
            self.cpu.append(
                value=float(cpu_val),
                filled=("cpu_usage_millicores" in filled_list),
            )
        if mem_val is not None:
            self.memory.append(
                value=float(mem_val),
                filled=("memory_usage_MiB" in filled_list),
            )

    @property
    def is_ready(self) -> bool:
        """Both windows must be ready before inference can run."""
        return self.cpu.is_ready and self.memory.is_ready

    def reset(self) -> None:
        self.cpu.reset()
        self.memory.reset()
