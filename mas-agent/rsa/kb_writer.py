# rsa/kb_writer.py
#
# Knowledge Base write operations for the RSA — Specification Section 7
# (Table 7) and KB Domains 2 & 5 Storage Backend Specification (Issue 6).
#
# Domain 4 (Audit Log):
#   SQLite WAL at /mnt/kb/domain4/audit_log.db.
#   The RSA writes one audit record per deliberation cycle including
#   do-nothing outcomes. Every scaling action and peer signal is audited.
#   Write protocol: BEGIN DEFERRED → INSERT → COMMIT, retry on SQLITE_BUSY
#   (identical to MRA and PFA patterns for consistency).
#
# Domain 5 (Coordination):
#   SQLite WAL at /mnt/kb/domain5/coordination.db.
#   The RSA is the sole writer to cooldown_state.
#   CRITICAL ordering constraint: the cooldown UPSERT must complete
#   synchronously BEFORE the Kubernetes patch is issued. A crash between
#   patch issuance and cooldown persistence produces post-crash oscillation.
#
import json
import math
import sqlite3
import threading
import time
import structlog

log = structlog.get_logger(__name__)


class RSAKBWriter:
    """
    Handles all Knowledge Base writes for the RSA.
    Thread-safe: independent locks for Domain 4 and Domain 5.
    """

    def __init__(
        self,
        domain4_db_path: str,
        domain5_db_path: str,
        namespace: str,
        deployment: str,
    ):
        self._namespace   = namespace
        self._deployment  = deployment
        self._agent_id    = f"RSA:{namespace}/{deployment}"

        self._d4_path     = domain4_db_path
        self._d5_path     = domain5_db_path

        self._d4_lock     = threading.Lock()
        self._d5_lock     = threading.Lock()

        self._d4_conn     = self._open_db(domain4_db_path, "Domain 4")
        self._d5_conn     = self._open_db(domain5_db_path, "Domain 5")

    # ---- Connection helper ------------------------------------------------

    def _open_db(self, path: str, label: str):
        try:
            conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=1000")
            log.info("db_connected", label=label, path=path)
            return conn
        except sqlite3.OperationalError as e:
            log.error("db_connect_failed", label=label, path=path, error=str(e))
            return None

    # ---- Domain 4 — Audit Log --------------------------------------------

    def write_audit(self, record_type: str, payload: dict) -> bool:
        """
        Append one record to the Domain 4 audit log.
        Returns True on success. Retries on SQLITE_BUSY with backoff.
        """
        if self._d4_conn is None:
            log.error("domain4_no_connection", record_type=record_type)
            return False

        payload_json = json.dumps(payload)

        with self._d4_lock:
            for attempt in range(3):
                try:
                    self._d4_conn.execute("BEGIN DEFERRED")
                    self._d4_conn.execute(
                        """INSERT INTO audit_log
                           (record_type, writer_agent, deployment, namespace,
                            timestamp_ms, payload)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            record_type,
                            self._agent_id,
                            self._deployment,
                            self._namespace,
                            int(time.time() * 1000),
                            payload_json,
                        ),
                    )
                    self._d4_conn.execute("COMMIT")
                    return True
                except sqlite3.OperationalError as e:
                    self._d4_conn.execute("ROLLBACK")
                    if "locked" in str(e) and attempt < 2:
                        wait = (2 ** attempt) * 0.1
                        log.warning("domain4_retry", attempt=attempt + 1, wait=wait)
                        time.sleep(wait)
                    else:
                        log.error("domain4_write_failed",
                                  record_type=record_type, error=str(e))
                        return False
        return False

    # ---- Domain 5 — Cooldown State ---------------------------------------

    def upsert_cooldown(
        self,
        last_action_ms: int,
        remaining_seconds: float,
        last_action_type: str,  # "scale_out" | "scale_in"
    ) -> None:
        """
        Write (or overwrite) the cooldown record for this deployment.

        BLOCKING — the caller MUST NOT issue the Kubernetes patch until
        this method returns without raising. If it raises, abort the
        scaling action rather than proceed without cooldown persistence.

        Uses INSERT OR REPLACE (UPSERT) so the table always holds exactly
        one row per (namespace, deployment) — it never grows unboundedly.
        """
        if self._d5_conn is None:
            raise RuntimeError(
                "COOLDOWN_PERSIST_FAILURE: Domain 5 connection unavailable. "
                "Aborting scaling action to prevent post-crash oscillation."
            )

        now_ms = int(time.time() * 1000)

        with self._d5_lock:
            try:
                self._d5_conn.execute("BEGIN DEFERRED")
                self._d5_conn.execute(
                    """INSERT OR REPLACE INTO cooldown_state
                       (namespace, deployment, last_action_ms, remaining_seconds,
                        last_action_type, updated_ms)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        self._namespace,
                        self._deployment,
                        last_action_ms,
                        remaining_seconds,
                        last_action_type,
                        now_ms,
                    ),
                )
                self._d5_conn.execute("COMMIT")
                log.info(
                    "cooldown_persisted",
                    last_action_type=last_action_type,
                    remaining_seconds=remaining_seconds,
                )
            except Exception as e:
                self._d5_conn.execute("ROLLBACK")
                log.error("cooldown_persist_failure", error=str(e))
                raise RuntimeError(
                    f"COOLDOWN_PERSIST_FAILURE: {e}. "
                    "Aborting scaling action."
                ) from e

    def read_cooldown(self) -> dict | None:
        """
        Read the current cooldown record for this deployment.

        Returns None if no record exists (first run, or cooldown expired
        and record was cleaned up). Used at startup for resume-and-adjust:
        elapsed = now - last_action_ms; remaining = cooldown_duration - elapsed.
        The stored remaining_seconds field is informational only — the RSA
        always recomputes from last_action_ms and wall clock on restart.
        """
        if self._d5_conn is None:
            return None

        with self._d5_lock:
            try:
                row = self._d5_conn.execute(
                    """SELECT namespace, deployment, last_action_ms,
                              remaining_seconds, last_action_type, updated_ms
                       FROM cooldown_state
                       WHERE namespace = ? AND deployment = ?""",
                    (self._namespace, self._deployment),
                ).fetchone()
                if row is None:
                    return None
                keys = [
                    "namespace", "deployment", "last_action_ms",
                    "remaining_seconds", "last_action_type", "updated_ms",
                ]
                return dict(zip(keys, row))
            except Exception as e:
                log.error("cooldown_read_failed", error=str(e))
                return None

    def delete_cooldown(self) -> None:
        """Remove the cooldown record when it expires."""
        if self._d5_conn is None:
            return
        with self._d5_lock:
            try:
                self._d5_conn.execute(
                    "DELETE FROM cooldown_state WHERE namespace=? AND deployment=?",
                    (self._namespace, self._deployment),
                )
                self._d5_conn.commit()
            except Exception as e:
                log.warning("cooldown_delete_failed", error=str(e))

    # ---- Domain 5 — Heartbeat Registry -----------------------------------

    def upsert_heartbeat(self, status: str = "ALIVE") -> None:
        """
        Write (or refresh) this RSA's heartbeat entry in Domain 5.
        Called every 15 seconds by the heartbeat thread and on startup
        to register the RSA's presence before MQTT publishing begins.
        """
        if self._d5_conn is None:
            return

        now_ms = int(time.time() * 1000)
        agent_id = f"RSA:worker:{self._namespace}/{self._deployment}"

        with self._d5_lock:
            try:
                self._d5_conn.execute("BEGIN DEFERRED")
                self._d5_conn.execute(
                    """INSERT OR REPLACE INTO heartbeat_registry
                       (agent_id, agent_type, namespace, deployment,
                        domain, last_heartbeat_ms, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        agent_id, "RSA",
                        self._namespace, self._deployment,
                        "worker", now_ms, status,
                    ),
                )
                self._d5_conn.execute("COMMIT")
            except Exception as e:
                try:
                    self._d5_conn.execute("ROLLBACK")
                except Exception:
                    pass
                log.warning("heartbeat_registry_write_failed", error=str(e))

