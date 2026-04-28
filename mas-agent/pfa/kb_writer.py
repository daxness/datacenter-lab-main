import json, math, sqlite3, threading, time, structlog
log = structlog.get_logger(__name__)

class PFAKBWriter:
    def __init__(self, domain4_db_path, namespace, deployment):
        self._db_path=domain4_db_path; self._namespace=namespace
        self._deployment=deployment; self._agent_id=f"PFA:{namespace}/{deployment}"
        self._write_lock=threading.Lock(); self._conn=self._open_db()

    def _open_db(self):
        try:
            conn=sqlite3.connect(self._db_path, check_same_thread=False, timeout=5.)
            conn.execute("PRAGMA journal_mode=WAL"); conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=1000"); log.info("domain4_connected", path=self._db_path)
            return conn
        except sqlite3.OperationalError as e:
            log.error("domain4_connect_failed", error=str(e)); return None

    def write_audit(self, record_type, payload):
        if self._conn is None: return False
        pj=json.dumps(payload)
        with self._write_lock:
            for attempt in range(3):
                try:
                    self._conn.execute("BEGIN DEFERRED")
                    self._conn.execute(
                        "INSERT INTO audit_log (record_type,writer_agent,deployment,namespace,timestamp_ms,payload) VALUES (?,?,?,?,?,?)",
                        (record_type, self._agent_id, self._deployment, self._namespace, int(time.time()*1000), pj))
                    self._conn.execute("COMMIT"); return True
                except sqlite3.OperationalError as e:
                    self._conn.execute("ROLLBACK")
                    if "locked" in str(e) and attempt<2: time.sleep((2**attempt)*0.1)
                    else: log.error("domain4_write_failed", error=str(e)); return False
        return False

    def write_forecast_accuracy(self, previous_p50_cpu, previous_p50_memory,
                                actual_cpu_window, actual_memory_window,
                                elapsed_steps, cycle_timestamp_ms,
                                cpu_uncertainty, memory_uncertainty):
        k=min(elapsed_steps, len(previous_p50_cpu), len(actual_cpu_window),
              len(previous_p50_memory), len(actual_memory_window))
        if k==0: return True
        def rmse(p,a):
            if not p or not a: return 0.
            return round(math.sqrt(sum((x-y)**2 for x,y in zip(p,a))/len(p)),4)
        payload={"cycle_timestamp_ms":cycle_timestamp_ms,"elapsed_step_count":k,
                 "cpu_rmse_millicores":rmse(previous_p50_cpu[:k],actual_cpu_window[-k:]),
                 "memory_rmse_MiB":rmse(previous_p50_memory[:k],actual_memory_window[-k:]),
                 "cpu_uncertainty":round(cpu_uncertainty,4),
                 "memory_uncertainty":round(memory_uncertainty,4),
                 "namespace":self._namespace,"deployment":self._deployment}
        return self.write_audit("FORECAST_ACCURACY", payload)
