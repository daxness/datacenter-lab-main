import time, threading, structlog
log = structlog.get_logger(__name__)

class MRALivenessState:
    ALIVE="ALIVE"; SUSPECTED="SUSPECTED"; CONFIRMED="CONFIRMED"

class MRAMonitor:
    def __init__(self, expected_interval_seconds, suspected_threshold,
                 confirmed_threshold, on_suspected, on_confirmed, on_recovered):
        self._interval=expected_interval_seconds; self._n_suspect=suspected_threshold
        self._n_confirm=confirmed_threshold; self._on_suspected=on_suspected
        self._on_confirmed=on_confirmed; self._on_recovered=on_recovered
        self._last_belief_time=time.time(); self._last_status_time=time.time()
        self._state=MRALivenessState.ALIVE
        self._suspected_fired=False; self._confirmed_fired=False
        self._lock=threading.Lock()

    def record_belief_update(self):
        with self._lock:
            was_degraded = self._state != MRALivenessState.ALIVE
            self._last_belief_time=time.time()
            self._suspected_fired=False; self._confirmed_fired=False
            if was_degraded:
                self._state=MRALivenessState.ALIVE; self._on_recovered()
            else:
                self._state=MRALivenessState.ALIVE

    def record_mra_status_event(self):
        with self._lock:
            self._last_status_time=time.time()
            self._suspected_fired=False; self._confirmed_fired=False

    def check(self):
        with self._lock:
            now=time.time()
            last=max(self._last_belief_time, self._last_status_time)
            missed=( now-last)/self._interval
            if missed>=self._n_confirm and not self._confirmed_fired:
                self._state=MRALivenessState.CONFIRMED; self._confirmed_fired=True
                self._on_confirmed()
            elif missed>=self._n_suspect and not self._suspected_fired and not self._confirmed_fired:
                self._state=MRALivenessState.SUSPECTED; self._suspected_fired=True
                self._on_suspected()

    def start_background_thread(self, shutdown_event):
        def _loop():
            while not shutdown_event.is_set():
                self.check(); shutdown_event.wait(timeout=10)
        threading.Thread(target=_loop, daemon=True, name="mra-monitor").start()

    @property
    def state(self): return self._state
