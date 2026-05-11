# sa/dca_monitor.py
#
# DCA liveness monitor for the SA.
#
# The SA supervises each DCA via heartbeat. A DCA is considered failed after
# dca_missed_threshold consecutive missed heartbeat intervals (default 3 × 15s
# = 45 seconds). This mirrors the DCA's own L1 supervision of Tier-2 agents.
#
# Unlike the RSA's MRAPeerMonitor (which uses a background thread that ticks
# at a fixed interval), the DCA monitor is event-driven: the SA MQTT client
# calls record_heartbeat() every time a DCA heartbeat arrives. A background
# thread ticks every dca_heartbeat_interval_seconds to check for absence.
#
# State machine per DCA:
#   ALIVE      → normal operation
#   SUSPECTED  → (not used here — SA goes directly to FAILED at threshold)
#   FAILED     → on_dca_failed() called; fallback subscriptions activated
#   RECOVERED  → first heartbeat after FAILED; on_dca_recovered() called
#
# Thread safety: a single lock guards the _last_seen dict.
#
import threading
import time
import structlog

log = structlog.get_logger(__name__)


class DCAMonitor:
    """
    Tracks heartbeat liveness for all assigned DCA instances.

    Usage:
        monitor = DCAMonitor(
            interval_seconds=15,
            missed_threshold=3,
            on_dca_failed=my_failed_callback,
            on_dca_recovered=my_recovered_callback,
        )
        monitor.record_heartbeat("worker")   # called by MQTT client
        monitor.start_background_thread(shutdown_event)
    """

    def __init__(
        self,
        interval_seconds: int,
        missed_threshold: int,
        on_dca_failed=None,      # callable(domain_id: str)
        on_dca_recovered=None,   # callable(domain_id: str)
    ):
        self._interval   = interval_seconds
        self._threshold  = missed_threshold
        self._on_failed  = on_dca_failed
        self._on_recovered = on_dca_recovered

        # _last_seen: domain_id → last heartbeat timestamp (float, seconds)
        self._last_seen: dict[str, float] = {}
        # _failed: domain_id → bool (True while in FAILED state)
        self._failed:    dict[str, bool]  = {}

        self._lock = threading.Lock()

    def record_heartbeat(self, domain_id: str) -> None:
        """
        Called by the MQTT client whenever a DCA heartbeat arrives.
        Clears FAILED state if the DCA was previously considered down.
        """
        now = time.time()
        with self._lock:
            was_failed = self._failed.get(domain_id, False)
            self._last_seen[domain_id] = now
            if was_failed:
                self._failed[domain_id] = False
                log.info("dca_recovered", domain_id=domain_id)
                if self._on_recovered:
                    self._on_recovered(domain_id)

    def check(self) -> None:
        """
        Called periodically by the background thread.
        Declares a DCA failed if it has missed >= threshold heartbeats.
        """
        now = time.time()
        deadline = now - (self._interval * self._threshold)

        with self._lock:
            for domain_id, last in list(self._last_seen.items()):
                if last < deadline and not self._failed.get(domain_id, False):
                    self._failed[domain_id] = True
                    elapsed = round(now - last, 1)
                    log.error("dca_failed",
                               domain_id=domain_id,
                               elapsed_since_last_heartbeat_s=elapsed)
                    if self._on_failed:
                        self._on_failed(domain_id)

    def is_failed(self, domain_id: str) -> bool:
        with self._lock:
            return self._failed.get(domain_id, False)

    def known_domains(self) -> list[str]:
        """Return the list of domain IDs the SA has seen at least one heartbeat from."""
        with self._lock:
            return list(self._last_seen.keys())

    def start_background_thread(self, shutdown_event: threading.Event) -> None:
        """
        Starts a daemon thread that calls check() every interval_seconds.
        shutdown_event is the same _shutdown Event used in main.py.
        """
        def _loop():
            while not shutdown_event.is_set():
                self.check()
                shutdown_event.wait(timeout=self._interval)

        t = threading.Thread(target=_loop, daemon=True, name="dca-monitor")
        t.start()
        log.info("dca_monitor_started",
                 interval_s=self._interval,
                 missed_threshold=self._threshold)
