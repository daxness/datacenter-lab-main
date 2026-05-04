# rsa/peer_monitor.py
#
# L2 peer monitoring for MRA and PFA liveness — RSA Specification Section 7.
#
# The RSA acts as an independent observer of both MRA and PFA liveness,
# complementing the DCA's L1 heartbeat tracking. This two-layer architecture
# ensures that even if the DCA fails, the RSA can still detect and report
# peer failures to the surviving coordination layer.
#
# MRA monitoring:
#   The RSA expects a BELIEF_UPDATE every 30 seconds. Any MRA message
#   (belief or status) resets the missed-cycle counter — because receipt
#   of any message proves the MRA process is alive, even if it cannot
#   produce valid belief objects.
#   After 3 missed cycles (90s): publish MRA_DOWN_SUSPECTED → RSA status topic
#   After 5 missed cycles (150s): publish MRA_DOWN_CONFIRMED → self-suspend all action
#
# PFA monitoring:
#   The PFA's maximum natural silence is 15 minutes (periodic inference timer).
#   Any PFA message (forecast or status) resets the elapsed timer.
#   After 17 minutes (15 min + 2 min buffer): PFA_DOWN_SUSPECTED
#   After 20 minutes: PFA_DOWN_CONFIRMED → suspend proactive path only
#
# This module mirrors the MRAMonitor pattern used by the PFA for consistency.
#
import time
import threading
import structlog

log = structlog.get_logger(__name__)


class PeerLivenessState:
    ALIVE     = "ALIVE"
    SUSPECTED = "SUSPECTED"
    CONFIRMED = "CONFIRMED"


class MRAPeerMonitor:
    """
    Tracks MRA liveness from the RSA's perspective.
    Called by the background monitor thread every 30 seconds.
    """

    def __init__(
        self,
        scrape_interval_seconds: int,
        suspected_threshold: int,   # 3 missed cycles
        confirmed_threshold: int,   # 5 missed cycles
        on_suspected,
        on_confirmed,
        on_recovered,
    ):
        self._interval          = scrape_interval_seconds
        self._n_suspect         = suspected_threshold
        self._n_confirm         = confirmed_threshold
        self._on_suspected      = on_suspected
        self._on_confirmed      = on_confirmed
        self._on_recovered      = on_recovered

        self._last_message_time = time.time()
        self._state             = PeerLivenessState.ALIVE
        self._suspected_fired   = False
        self._confirmed_fired   = False
        self._lock              = threading.Lock()

    def record_message(self) -> None:
        """
        Called on any MRA message: BELIEF_UPDATE or any status event.
        Resets the counter and fires recovery callback if previously degraded.
        """
        with self._lock:
            was_degraded = self._state != PeerLivenessState.ALIVE
            self._last_message_time = time.time()
            self._suspected_fired   = False
            self._confirmed_fired   = False
            self._state             = PeerLivenessState.ALIVE
            if was_degraded:
                self._on_recovered()

    def check(self) -> None:
        """
        Called periodically by the background thread.
        Computes missed cycles and fires signals at appropriate thresholds.
        """
        with self._lock:
            now     = time.time()
            elapsed = now - self._last_message_time
            missed  = elapsed / self._interval

            if missed >= self._n_confirm and not self._confirmed_fired:
                self._state           = PeerLivenessState.CONFIRMED
                self._confirmed_fired = True
                log.warning("mra_peer_confirmed_down",
                            elapsed_seconds=round(elapsed, 1))
                self._on_confirmed()

            elif (missed >= self._n_suspect
                  and not self._suspected_fired
                  and not self._confirmed_fired):
                self._state           = PeerLivenessState.SUSPECTED
                self._suspected_fired = True
                log.warning("mra_peer_suspected_down",
                            elapsed_seconds=round(elapsed, 1))
                self._on_suspected()

    @property
    def state(self) -> str:
        return self._state


class PFAPeerMonitor:
    """
    Tracks PFA liveness from the RSA's perspective.
    Uses elapsed wall-clock time rather than missed cycle counts because
    the PFA's publication cadence is variable (WARNING-triggered or
    15-minute periodic) rather than fixed at 30 seconds.
    """

    def __init__(
        self,
        suspected_seconds: int,   # 1020s = 17 minutes
        confirmed_seconds: int,   # 1200s = 20 minutes
        on_suspected,
        on_confirmed,
        on_recovered,
    ):
        self._n_suspect        = suspected_seconds
        self._n_confirm        = confirmed_seconds
        self._on_suspected     = on_suspected
        self._on_confirmed     = on_confirmed
        self._on_recovered     = on_recovered

        self._last_message_time = time.time()
        self._state             = PeerLivenessState.ALIVE
        self._suspected_fired   = False
        self._confirmed_fired   = False
        self._lock              = threading.Lock()

    def record_message(self) -> None:
        """
        Called on any PFA message: FORECAST_UPDATE or any pfa/status event.
        """
        with self._lock:
            was_degraded = self._state != PeerLivenessState.ALIVE
            self._last_message_time = time.time()
            self._suspected_fired   = False
            self._confirmed_fired   = False
            self._state             = PeerLivenessState.ALIVE
            if was_degraded:
                self._on_recovered()

    def check(self) -> None:
        """Called periodically by the background thread."""
        with self._lock:
            elapsed = time.time() - self._last_message_time

            if elapsed >= self._n_confirm and not self._confirmed_fired:
                self._state           = PeerLivenessState.CONFIRMED
                self._confirmed_fired = True
                log.warning("pfa_peer_confirmed_down",
                            elapsed_seconds=round(elapsed, 1))
                self._on_confirmed()

            elif (elapsed >= self._n_suspect
                  and not self._suspected_fired
                  and not self._confirmed_fired):
                self._state           = PeerLivenessState.SUSPECTED
                self._suspected_fired = True
                log.warning("pfa_peer_suspected_down",
                            elapsed_seconds=round(elapsed, 1))
                self._on_suspected()

    def start_background_thread(self, shutdown_event: threading.Event) -> None:
        def _loop():
            while not shutdown_event.is_set():
                self.check()
                shutdown_event.wait(timeout=30)

        threading.Thread(
            target=_loop, daemon=True, name="pfa-peer-monitor"
        ).start()

    @property
    def state(self) -> str:
        return self._state

