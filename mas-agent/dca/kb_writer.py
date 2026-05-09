# dca/kb_writer.py
#
# Knowledge Base write operations for the DCA — Specification Section 7.7.
#
# Domain 4 (Audit Log):
#   SQLite WAL at /mnt/kb/domain4/audit_log.db.
#   The DCA writes one COORDINATION_EVENT record per coordination decision,
#   regardless of outcome. Every SA escalation is also audited here.
#   Write protocol: BEGIN DEFERRED → INSERT → COMMIT, retry on SQLITE_BUSY.
#   Identical to RSA and MRA patterns for consistency.
#
# Domain 5 (Coordination):
#   SQLite WAL at /mnt/kb/domain5/coordination.db.
#   The DCA reads and writes:
#     - heartbeat_registry: DCA registers its own liveness (UPSERT every 15s)
#     - escalation_records: written on every Mode B/C escalation; updated on SA response
#     - system_mode: persisted so DCA restarts during EMERGENCY_STOP re-enforce the stop
#   The DCA does NOT write to cooldown_state (RSA sole writer) or veto_queue (SA writer).
#
import json
import sqlite3
import threading
import time
import uuid
import structlog

log = structlog.get_logger(__name__)


class DCAKBWriter:
    """
    Handles all Knowledge Base writes for the DCA.
    Thread-safe: independent locks for Domain 4 and Domain 5.
    """

    def __init__(
        self,
        domain4_db_path: str,
        domain5_db_path: str,
        domain_id: str,
    ):
        self._domain_id = domain_id
        self._agent_id  = f"DCA:{domain_id}"

        self._d4_path   = domain4_db_path
        self._d5_path   = domain5_db_path

        self._d4_lock   = threading.Lock()
        self._d5_lock   = threading.Lock()

        self._d4_conn   = self._open_db(domain4_db_path, "Domain 4")
        self._d5_conn   = self._open_db(domain5_db_path, "Domain 5")

    # ---- Connection helper -------------------------------------------------

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

    # ---- Domain 4 — Audit Log (COORDINATION_EVENT) ------------------------

    def write_coordination_event(
        self,
        event_type: str,
        deployment: str | None,
        namespace: str | None,
        payload: dict,
    ) -> bool:
        """
        Write one COORDINATION_EVENT to Domain 4.
        Called on every DCA coordination decision — including Mode A
        (log-only) outcomes. Every decision must be auditable.
        """
        if self._d4_conn is None:
            log.error("domain4_no_connection", event_type=event_type)
            return False

        full_payload = json.dumps({
            "event_type":  event_type,
            "domain_id":   self._domain_id,
            **payload,
        })

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
                            "COORDINATION_EVENT",
                            self._agent_id,
                            deployment,
                            namespace,
                            int(time.time() * 1000),
                            full_payload,
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
                                  event_type=event_type, error=str(e))
                        return False
        return False

    # ---- Domain 5 — DCA Heartbeat Registry --------------------------------

    def upsert_heartbeat(self, status: str = "ALIVE") -> None:
        """
        Write (or refresh) this DCA's heartbeat entry in Domain 5.
        Called every 15 seconds by the heartbeat thread and once at startup.
        Agent ID format for domain-scoped agents: {agent_type}:{domain_id}
        """
        if self._d5_conn is None:
            return

        agent_id = f"DCA:{self._domain_id}"
        now_ms   = int(time.time() * 1000)

        with self._d5_lock:
            try:
                self._d5_conn.execute("BEGIN DEFERRED")
                self._d5_conn.execute(
                    """INSERT OR REPLACE INTO heartbeat_registry
                       (agent_id, agent_type, namespace, deployment,
                        domain, last_heartbeat_ms, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        agent_id, "DCA",
                        None, None,           # DCA is domain-scoped, not deployment-scoped
                        self._domain_id,
                        now_ms, status,
                    ),
                )
                self._d5_conn.execute("COMMIT")
            except Exception as e:
                try:
                    self._d5_conn.execute("ROLLBACK")
                except Exception:
                    pass
                log.warning("heartbeat_registry_write_failed", error=str(e))

    # ---- Domain 5 — Escalation Records ------------------------------------

    def write_escalation_record(
        self,
        escalation_type: str,
        namespace: str | None,
        deployment: str | None,
        target_replicas: int | None = None,
    ) -> str:
        """
        Insert a new escalation record into Domain 5.
        Returns the UUID of the new record so the caller can update it later.
        """
        if self._d5_conn is None:
            log.error("domain5_no_connection", escalation_type=escalation_type)
            return ""

        escalation_id = str(uuid.uuid4())
        now_ms        = int(time.time() * 1000)

        with self._d5_lock:
            try:
                self._d5_conn.execute("BEGIN DEFERRED")
                self._d5_conn.execute(
                    """INSERT INTO escalation_records
                       (escalation_id, escalation_type, namespace, deployment,
                        requesting_domain, target_replicas, status, created_ms)
                       VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)""",
                    (
                        escalation_id,
                        escalation_type,
                        namespace,
                        deployment,
                        self._domain_id,
                        target_replicas,
                        now_ms,
                    ),
                )
                self._d5_conn.execute("COMMIT")
                log.info(
                    "escalation_record_created",
                    escalation_id=escalation_id,
                    escalation_type=escalation_type,
                )
                return escalation_id
            except Exception as e:
                try:
                    self._d5_conn.execute("ROLLBACK")
                except Exception:
                    pass
                log.error("escalation_record_write_failed", error=str(e))
                return ""

    def read_system_mode(self) -> str | None:
        """
        Read the persisted system mode from the heartbeat_registry table.
        We store system mode in the DCA's own heartbeat row as the status field.
        Returns None if no row exists (fresh start → caller uses NORMAL default).
        """
        if self._d5_conn is None:
            return None

        agent_id = f"DCA:{self._domain_id}"
        with self._d5_lock:
            try:
                row = self._d5_conn.execute(
                    "SELECT status FROM heartbeat_registry WHERE agent_id = ?",
                    (agent_id,),
                ).fetchone()
                if row is None:
                    return None
                status = row[0]
                # We encode system mode in the status field with prefix "MODE:"
                if status and status.startswith("MODE:"):
                    return status[5:]
                return None
            except Exception as e:
                log.warning("system_mode_read_failed", error=str(e))
                return None

    def persist_system_mode(self, mode: str) -> None:
        """
        Persist the system mode to Domain 5 so it survives DCA restarts.
        Encoded in the DCA heartbeat row's status field as "MODE:{mode}".
        """
        if self._d5_conn is None:
            return

        agent_id = f"DCA:{self._domain_id}"
        now_ms   = int(time.time() * 1000)

        with self._d5_lock:
            try:
                self._d5_conn.execute("BEGIN DEFERRED")
                self._d5_conn.execute(
                    """INSERT OR REPLACE INTO heartbeat_registry
                       (agent_id, agent_type, namespace, deployment,
                        domain, last_heartbeat_ms, status)
                       VALUES (?, 'DCA', NULL, NULL, ?, ?, ?)""",
                    (agent_id, self._domain_id, now_ms, f"MODE:{mode}"),
                )
                self._d5_conn.execute("COMMIT")
                log.info("system_mode_persisted", mode=mode)
            except Exception as e:
                try:
                    self._d5_conn.execute("ROLLBACK")
                except Exception:
                    pass
                log.error("system_mode_persist_failed", error=str(e))
