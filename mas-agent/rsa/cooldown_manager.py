# rsa/cooldown_manager.py
#
# In-memory cooldown state manager with synchronous Domain 5 persistence.
#
# The cooldown mechanism has one purpose: preventing oscillation between
# consecutive scaling decisions. Once any scaling action is executed
# (proactive or reactive), no further action is permitted for 60 seconds.
#
# Emergency scaling (CRITICAL path) does NOT start or reset the cooldown.
# This is a deliberate design decision from the RSA specification:
# the cooldown timer protects against oscillation during the recovery
# phase that follows an emergency. Allowing the emergency path to reset
# the cooldown would prevent the RSA from scaling down conservatively
# after the emergency resolves.
#
# On startup, the RSA performs resume-and-adjust: it reads Domain 5 and
# recomputes the remaining cooldown from wall-clock elapsed time, not
# from the stored remaining_seconds field (which may be stale after a
# crash). Records older than the full cooldown duration are deleted.
#
import time
import structlog

log = structlog.get_logger(__name__)


class CooldownManager:

    def __init__(self, cooldown_seconds: int, kb_writer):
        """
        Parameters
        ----------
        cooldown_seconds : Duration of the cooldown window. 60s per spec.
        kb_writer        : RSAKBWriter instance — provides upsert/read/delete
                           for Domain 5 cooldown_state.
        """
        self._duration  = cooldown_seconds
        self._kb        = kb_writer
        self._active    = False
        self._start_ms: int | None = None

        # Resume-and-adjust from Domain 5 on startup
        self._resume()

    def _resume(self) -> None:
        """
        Read Domain 5 on startup to resume any active cooldown that
        survived a crash or clean restart.
        """
        record = self._kb.read_cooldown()
        if record is None:
            log.info("cooldown_resume_none")
            return

        now_ms  = int(time.time() * 1000)
        elapsed = (now_ms - record["last_action_ms"]) / 1000.0

        if elapsed < self._duration:
            self._active   = True
            self._start_ms = record["last_action_ms"]
            log.info(
                "cooldown_resumed",
                elapsed_seconds=round(elapsed, 1),
                remaining_seconds=round(self._duration - elapsed, 1),
                last_action_type=record["last_action_type"],
            )
        else:
            # Expired during downtime — delete the stale record
            self._kb.delete_cooldown()
            log.info("cooldown_resume_expired", elapsed_seconds=round(elapsed, 1))

    def is_active(self) -> bool:
        """
        Returns True if cooldown is blocking scaling actions.
        Also handles natural expiry — clears state when duration has elapsed.
        """
        if not self._active:
            return False

        now_ms  = int(time.time() * 1000)
        elapsed = (now_ms - self._start_ms) / 1000.0

        if elapsed >= self._duration:
            self._expire()
            return False

        return True

    def start(self, action_type: str) -> None:
        """
        Start the cooldown timer after a scaling action is committed.

        This method MUST be called BEFORE the Kubernetes patch is issued.
        The kb_writer.upsert_cooldown call is synchronous and blocking.
        If it raises, the caller must abort the scaling action.

        Parameters
        ----------
        action_type : "scale_out" or "scale_in"
        """
        now_ms = int(time.time() * 1000)

        # Persist first — Kubernetes patch only after this returns
        self._kb.upsert_cooldown(
            last_action_ms=now_ms,
            remaining_seconds=float(self._duration),
            last_action_type=action_type,
        )

        # Update in-memory state
        self._active   = True
        self._start_ms = now_ms

        log.info(
            "cooldown_started",
            action_type=action_type,
            duration_seconds=self._duration,
        )

    def remaining_seconds(self) -> float:
        """Returns remaining cooldown in seconds, or 0.0 if not active."""
        if not self._active or self._start_ms is None:
            return 0.0
        now_ms = int(time.time() * 1000)
        return max(0.0, self._duration - (now_ms - self._start_ms) / 1000.0)

    def _expire(self) -> None:
        self._active   = False
        self._start_ms = None
        self._kb.delete_cooldown()
        log.info("cooldown_expired")

