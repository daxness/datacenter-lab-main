# mra/kb_writer.py
#
# Knowledge Base write operations — MRA Specification Section 7.
#
# Domain 3 (Metric History):
#   Written by exposing Prometheus gauges on port 8000.
#   Prometheus (running on server-0 in your cluster, via helm-values.yaml)
#   scrapes these gauges every 30 seconds (scrapeInterval: "30s" in helm-values.yaml)
#   and stores the time series in its TSDB.
#   The annotation prometheus.io/scrape: "true" on the MRA pod tells the
#   Prometheus operator to add it to the scrape targets automatically.
#
# Domain 4 (Audit Log):
#   SQLite WAL database at /mnt/kb/domain4/audit_log.db.
#   /mnt/kb is the K3D volume mount → ~/datacenter-lab-main/kb-storage on WSL2.
#   Write protocol follows Specification Issue 4 Section 4.1:
#     BEGIN DEFERRED → INSERT → COMMIT, retry on SQLITE_BUSY.
#
import json
import sqlite3
import threading
import time
import structlog
from typing import Optional
from prometheus_client import Gauge, start_http_server
 
log = structlog.get_logger(__name__)
 
# ---- Domain 3: Prometheus gauges ----
_CPU_GAUGE = Gauge(
    "mas_mra_cpu_usage_millicores",
    "CPU usage in millicores for the monitored deployment",
    ["namespace", "deployment"],
)
_MEM_GAUGE = Gauge(
    "mas_mra_memory_usage_MiB",
    "Working set memory in MiB for the monitored deployment",
    ["namespace", "deployment"],
)
_PRESSURE_GAUGE = Gauge(
    "mas_mra_pressure_level",
    "Pressure level: 0=NORMAL 1=WARNING 2=CRITICAL 3=UNCONFIGURED",
    ["namespace", "deployment"],
)
_PRESSURE_MAP = {"NORMAL": 0, "WARNING": 1, "CRITICAL": 2, "UNCONFIGURED": 3}
 
# Track whether the metrics server has been started
# (multiple KBWriter instances in tests would try to bind the same port)
_metrics_server_started = False
_metrics_server_lock = threading.Lock()
 
 
class KBWriter:
    """
    Handles all Knowledge Base writes for the MRA.
    Thread-safe: a single threading.Lock serialises all Domain 4 writes.
    """
 
    def __init__(
        self,
        domain4_db_path: str,
        namespace: str,
        deployment: str,
        metrics_port: int = 8000,
    ):
        self._namespace   = namespace
        self._deployment  = deployment
        self._db_path     = domain4_db_path
        self._write_lock  = threading.Lock()
        self._labels      = {"namespace": namespace, "deployment": deployment}
 
        # Start Prometheus metrics HTTP server (Domain 3 write path)
        global _metrics_server_started
        with _metrics_server_lock:
            if not _metrics_server_started:
                try:
                    start_http_server(metrics_port)
                    _metrics_server_started = True
                    log.info("metrics_server_started", port=metrics_port)
                except OSError:
                    log.warning("metrics_server_port_busy", port=metrics_port)
 
        # Open persistent Domain 4 SQLite connection
        self._conn = self._open_db()
 
    def _open_db(self) -> Optional[sqlite3.Connection]:
        try:
            conn = sqlite3.connect(
                self._db_path,
                check_same_thread=False,
                timeout=5.0,
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=1000")
            log.info("domain4_connected", path=self._db_path)
            return conn
        except sqlite3.OperationalError as e:
            log.error("domain4_connect_failed", path=self._db_path, error=str(e))
            return None
 
    def write_domain3(self, belief: dict) -> None:
        """Update Prometheus gauges from the current belief object."""
        fm = belief.get("forecast_metrics", {})
        if fm.get("cpu_usage_millicores") is not None:
            _CPU_GAUGE.labels(**self._labels).set(fm["cpu_usage_millicores"])
        if fm.get("memory_usage_MiB") is not None:
            _MEM_GAUGE.labels(**self._labels).set(fm["memory_usage_MiB"])
        pressure = belief.get("pressure_level", "NORMAL")
        _PRESSURE_GAUGE.labels(**self._labels).set(_PRESSURE_MAP.get(pressure, 0))
 
    def write_domain4(self, record_type: str, payload: dict) -> bool:
        """
        Append one record to the Domain 4 audit log.
        Returns True on success, False on permanent failure.
        Retries on SQLITE_BUSY with exponential backoff (100ms, 200ms, 400ms).
        """
        if self._conn is None:
            log.error("domain4_no_connection", record_type=record_type)
            return False
 
        agent_id = f"MRA:{self._namespace}/{self._deployment}"
        payload_json = json.dumps(payload)
 
        with self._write_lock:
            for attempt in range(3):
                try:
                    self._conn.execute("BEGIN DEFERRED")
                    self._conn.execute(
                        """INSERT INTO audit_log
                           (record_type, writer_agent, deployment, namespace,
                            timestamp_ms, payload)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            record_type,
                            agent_id,
                            self._deployment,
                            self._namespace,
                            int(time.time() * 1000),
                            payload_json,
                        ),
                    )
                    self._conn.execute("COMMIT")
                    return True
                except sqlite3.OperationalError as e:
                    self._conn.execute("ROLLBACK")
                    if "database is locked" in str(e) and attempt < 2:
                        wait = (2 ** attempt) * 0.1
                        log.warning("domain4_retry", attempt=attempt + 1, wait=wait)
                        time.sleep(wait)
                    else:
                        log.error("domain4_write_failed",
                                  record_type=record_type, error=str(e))
                        return False
        return False
 
