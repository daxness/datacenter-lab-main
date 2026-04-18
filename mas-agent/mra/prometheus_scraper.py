# mra/prometheus_scraper.py
#
# Prometheus HTTP API client — MRA Specification Section 2.2.
#
# Implements all nine PromQL queries against the kube-prometheus-stack
# deployed by your helm install. Two data sources are used:
#
#   cAdvisor (via kubelet):   container_cpu_usage_seconds_total
#                             container_memory_working_set_bytes
#   kube-state-metrics:       kube_pod_container_resource_requests
#                             kube_pod_container_resource_limits
#                             kube_deployment_status_replicas_*
#
# Your helm-values.yaml enables both:
#   kubelet.enabled: true          (cAdvisor data)
#   kube-state-metrics pinned v2.13.0
#
# Pod label filter: pod=~"^{deployment}-.*"
# This regex matches Kubernetes ReplicaSet pods which follow the naming
# convention: {deployment-name}-{replicaset-hash}-{pod-hash}
# e.g. nginx-7d8b9c6f5-xk9qp  matches ^nginx-.*
#
import time
import requests
import structlog
from typing import Optional
 
log = structlog.get_logger(__name__)
 
 
class PrometheusQueryError(Exception):
    pass
 
 
class PrometheusClient:
 
    def __init__(
        self,
        base_url: str,
        namespace: str,
        deployment_name: str,
        cpu_rate_window: str = "2m",
        timeout: int = 10,
    ):
        self._base_url = base_url.rstrip("/")
        self._namespace = namespace
        self._deployment = deployment_name
        self._cpu_rate_window = cpu_rate_window
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": f"mas-mra/{namespace}/{deployment_name}"
        })
 
    def _query(self, promql: str) -> list:
        url = f"{self._base_url}/api/v1/query"
        try:
            resp = self._session.get(
                url,
                params={"query": promql, "time": str(time.time())},
                timeout=self._timeout,
            )
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            raise PrometheusQueryError(
                f"Prometheus timeout after {self._timeout}s"
            )
        except requests.exceptions.ConnectionError as e:
            raise PrometheusQueryError(
                f"Cannot reach Prometheus at {self._base_url}: {e}"
            )
        except requests.exceptions.HTTPError as e:
            raise PrometheusQueryError(
                f"Prometheus HTTP {resp.status_code}: {e}"
            )
 
        body = resp.json()
        if body.get("status") != "success":
            raise PrometheusQueryError(
                f"Query failed: {body.get('error', 'unknown error')}"
            )
 
        result_type = body.get("data", {}).get("resultType")
        if result_type not in ("vector", "matrix"):
            raise PrometheusQueryError(
                f"Unexpected result type: {result_type}"
            )
 
        return body["data"]["result"]
 
    def _scalar(self, results: list, fn=sum) -> Optional[float]:
        """
        Extract a single float from query results.
        Multiple rows are combined with fn (default: sum).
        Returns None if results is empty — triggers completeness check.
        """
        if not results:
            return None
        values = []
        for row in results:
            try:
                values.append(float(row["value"][1]))
            except (ValueError, TypeError, KeyError):
                log.warning("prometheus_non_numeric", row=str(row)[:120])
        return fn(values) if values else None
 
    # ---- Nine metric queries (Spec Section 2.2) ----
 
    def get_cpu_usage_millicores(self) -> Optional[float]:
        """
        rate(container_cpu_usage_seconds_total{...}[2m]) * 1000
        Aggregated across all pods of the deployment.
        """
        q = (
            f'sum(rate(container_cpu_usage_seconds_total{{'
            f'container!="",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}}[{self._cpu_rate_window}])) * 1000'
        )
        return self._scalar(self._query(q))
 
    def get_memory_usage_bytes(self) -> Optional[float]:
        """
        container_memory_working_set_bytes — gauge, no rate needed.
        Working set preferred over RSS for Kubernetes OOM evaluation.
        """
        q = (
            f'sum(container_memory_working_set_bytes{{'
            f'container!="",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}})'
        )
        return self._scalar(self._query(q))
 
    def get_cpu_requests_millicores(self) -> Optional[float]:
        q = (
            f'sum(kube_pod_container_resource_requests{{'
            f'resource="cpu",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}}) * 1000'
        )
        return self._scalar(self._query(q))
 
    def get_cpu_limits_millicores(self) -> Optional[float]:
        q = (
            f'sum(kube_pod_container_resource_limits{{'
            f'resource="cpu",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}}) * 1000'
        )
        return self._scalar(self._query(q))
 
    def get_memory_requests_bytes(self) -> Optional[float]:
        q = (
            f'sum(kube_pod_container_resource_requests{{'
            f'resource="memory",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}})'
        )
        return self._scalar(self._query(q))
 
    def get_memory_limits_bytes(self) -> Optional[float]:
        q = (
            f'sum(kube_pod_container_resource_limits{{'
            f'resource="memory",namespace="{self._namespace}",'
            f'pod=~"^{self._deployment}-.*"'
            f'}})'
        )
        return self._scalar(self._query(q))
 
    def get_replicas_available(self) -> Optional[int]:
        q = (
            f'kube_deployment_status_replicas_available{{'
            f'namespace="{self._namespace}",'
            f'deployment="{self._deployment}"'
            f'}}'
        )
        v = self._scalar(self._query(q))
        return int(v) if v is not None else None
 
    def get_replicas_ready(self) -> Optional[int]:
        q = (
            f'kube_deployment_status_replicas_ready{{'
            f'namespace="{self._namespace}",'
            f'deployment="{self._deployment}"'
            f'}}'
        )
        v = self._scalar(self._query(q))
        return int(v) if v is not None else None
 
    def get_replicas_desired(self) -> Optional[int]:
        q = (
            f'kube_deployment_spec_replicas{{'
            f'namespace="{self._namespace}",'
            f'deployment="{self._deployment}"'
            f'}}'
        )
        v = self._scalar(self._query(q))
        return int(v) if v is not None else None
 
    def scrape_all(self) -> dict:
        """
        Run all nine queries. Each query is independent — a failure on one
        sets that value to None without aborting the others.
        The validator's completeness check handles None values via forward-fill.
        """
        queries = {
            "cpu_usage_raw":          self.get_cpu_usage_millicores,
            "memory_usage_raw_bytes": self.get_memory_usage_bytes,
            "cpu_requests_raw":       self.get_cpu_requests_millicores,
            "cpu_limits_raw":         self.get_cpu_limits_millicores,
            "memory_requests_raw_bytes": self.get_memory_requests_bytes,
            "memory_limits_raw_bytes":   self.get_memory_limits_bytes,
            "replicas_available":     self.get_replicas_available,
            "replicas_ready":         self.get_replicas_ready,
            "replicas_desired":       self.get_replicas_desired,
        }
        raw = {}
        for key, fn in queries.items():
            try:
                raw[key] = fn()
            except PrometheusQueryError as e:
                log.warning("query_failed", metric=key, error=str(e))
                raw[key] = None
        return raw
