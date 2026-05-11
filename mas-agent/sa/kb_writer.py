# sa/kb_writer.py
#
# Knowledge Base write operations for the SA — Domain 4 (Audit Log) only.
#
# The SA writes to Domain 4 for:
#   - Policy overrides (operator-initiated)
#   - Emergency stop activations and restorations
#   - Operator veto decisions
#   - Forced action commands
#   - Agent failure notifications presented to the operator
#
# Write protocol: BEGIN DEFERRED → INSERT → COMMIT, with retry on SQLITE_BUSY.
# This is identical to the RSA and PFA pattern — do not deviate.
#
# Thread safety: a single threading.Lock guards all Domain 4 writes.
# The SA is single-process but the FastAPI server runs in a separate thread,
# so the lock is necessary.
#
import json
import sqlite3
import threading
import time
import structlog

log = structlog.get_logger(__name__)


class SAKBWriter:
    """
    Handles all Knowledge Base writes for the SA.
    Currently: Domain 4 (Audit Log) only.
    Domain 1 and Domain 6 writes are handled by SAK8sClient (ConfigMaps).
    """

    def __init__(self, domain4_db_path: str):
        self._agent_id = "SA:global"
        self._d4_path  = domain4_db_path
        self._d4_lock  = threading.Lock()
        self._d4_conn  = self._open_db(domain4_db_path, "Domain 4")

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

    def write_audit(self, record_type: str, payload: dict,
                    namespace: str = None, deployment: str = None) -> bool:
        """
        Append one record to the Domain 4 audit log.

        record_type examples:
          POLICY_OVERRIDE, EMERGENCY_STOP, RESUME, FORCED_ACTION,
          OPERATOR_VETO, AGENT_FAILURE_NOTIFIED

        namespace / deployment are optional — set them when the record
        pertains to a specific deployment (e.g. escalation records).

        Returns True on success, False on failure.
        Retries up to 3 times with exponential backoff on SQLITE_BUSY.
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
                            deployment,
                            namespace,
                            int(time.time() * 1000),
                            payload_json,
                        ),
                    )
                    self._d4_conn.execute("COMMIT")
                    log.info("audit_written", record_type=record_type)
                    return True
                except sqlite3.OperationalError as e:
                    try:
                        self._d4_conn.execute("ROLLBACK")
                    except Exception:
                        pass
                    if "locked" in str(e) and attempt < 2:
                        wait = (2 ** attempt) * 0.1
                        log.warning("domain4_retry",
                                    attempt=attempt + 1, wait=wait)
                        time.sleep(wait)
                    else:
                        log.error("domain4_write_failed",
                                  record_type=record_type, error=str(e))
                        return False
        return False
