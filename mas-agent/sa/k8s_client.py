# sa/k8s_client.py
#
# Kubernetes API interactions for the SA.
#
# The SA is the sole writer to two ConfigMaps:
#   mas-policy    (Domain 1) — holds the full policy.json document.
#   mas-oversight (Domain 6) — holds policy version history and emergency
#                              stop event records.
#
# The SA reads one additional ConfigMap:
#   mas-topology  (Domain 2) — read at escalation presentation and forced-
#                              action validation time to confirm deployment identity.
#
# All operations use the in-cluster service account credentials when running
# inside Kubernetes, or fall back to the kubeconfig on the developer machine.
#
# The write protocol for Domain 1 follows the spec (Issue 6, Section 2.4):
#   - Read current resourceVersion first.
#   - Include resourceVersion in the PUT to implement optimistic concurrency.
#   - On 409 Conflict: re-read and retry once.
#
# This module has no MQTT dependency and no SQLite dependency — it is purely
# a Kubernetes API client.
#
import json
import time
import structlog
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException

log = structlog.get_logger(__name__)


class SAK8sClient:
    """
    Wraps all Kubernetes API calls made by the SA.
    Initialised once at startup; shared across all SA modules.
    """

    def __init__(self, namespace: str,
                 policy_configmap: str,
                 oversight_configmap: str,
                 topology_configmap: str):
        self._namespace         = namespace
        self._policy_cm         = policy_configmap
        self._oversight_cm      = oversight_configmap
        self._topology_cm       = topology_configmap

        # Load credentials: in-cluster first, then kubeconfig fallback.
        try:
            k8s_config.load_incluster_config()
            log.info("k8s_credentials", source="in-cluster")
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
            log.info("k8s_credentials", source="kubeconfig")

        self._v1 = client.CoreV1Api()

    # ---- Domain 1 — Policy ConfigMap (sole writer) -----------------------

    def read_policy(self) -> dict:
        """
        Read and parse the current policy.json from Domain 1.
        Returns the parsed dict. Raises on API error.
        """
        cm = self._v1.read_namespaced_config_map(
            name=self._policy_cm,
            namespace=self._namespace,
        )
        raw = cm.data.get("policy.json", "{}")
        return json.loads(raw)

    def write_policy(self, policy: dict) -> None:
        """
        Atomically replace Domain 1 with the given policy dict.
        Uses optimistic concurrency (resourceVersion) to guard against
        simultaneous writes — retries once on 409 Conflict.
        """
        for attempt in range(2):
            try:
                # Read current resourceVersion for optimistic locking.
                cm = self._v1.read_namespaced_config_map(
                    name=self._policy_cm,
                    namespace=self._namespace,
                )
                resource_version = cm.metadata.resource_version

                # Build the updated ConfigMap body.
                cm.data = {"policy.json": json.dumps(policy, indent=2)}
                cm.metadata.resource_version = resource_version

                self._v1.replace_namespaced_config_map(
                    name=self._policy_cm,
                    namespace=self._namespace,
                    body=cm,
                )
                log.info("domain1_policy_written",
                         policy_version=policy.get("policy_version"))
                return

            except ApiException as e:
                if e.status == 409 and attempt == 0:
                    log.warning("domain1_write_conflict",
                                msg="409 conflict — re-reading and retrying")
                    time.sleep(0.2)
                    continue
                log.error("domain1_write_failed",
                          status=e.status, reason=e.reason)
                raise

    # ---- Domain 2 — Topology ConfigMap (read only) -----------------------

    def read_topology(self) -> dict:
        """
        Read and parse topology.json from Domain 2.
        Used at escalation presentation and forced-action validation time.
        Returns empty dict on failure (non-fatal — SA continues operating).
        """
        try:
            cm = self._v1.read_namespaced_config_map(
                name=self._topology_cm,
                namespace=self._namespace,
            )
            raw = cm.data.get("topology.json", "{}")
            return json.loads(raw)
        except ApiException as e:
            log.warning("domain2_read_failed",
                        status=e.status, reason=e.reason)
            return {}

    # ---- Domain 6 — Oversight ConfigMap (sole writer and reader) ---------

    def read_oversight(self) -> dict:
        """
        Read the oversight ConfigMap. Returns empty structure if it does
        not yet exist (first startup before the SA has written anything).
        """
        try:
            cm = self._v1.read_namespaced_config_map(
                name=self._oversight_cm,
                namespace=self._namespace,
            )
            raw = cm.data.get("oversight.json", "{}")
            return json.loads(raw)
        except ApiException as e:
            if e.status == 404:
                log.info("domain6_not_found",
                         msg="oversight ConfigMap absent — will be created on first write")
                return {"policy_versions": [], "emergency_stop_events": []}
            log.warning("domain6_read_failed", status=e.status)
            return {"policy_versions": [], "emergency_stop_events": []}

    def _write_oversight(self, oversight: dict) -> None:
        """
        Internal: write the oversight dict to Domain 6.
        Creates the ConfigMap if it does not exist yet.
        """
        data = {"oversight.json": json.dumps(oversight, indent=2)}

        try:
            # Try to read first to get resourceVersion for the replace.
            cm = self._v1.read_namespaced_config_map(
                name=self._oversight_cm,
                namespace=self._namespace,
            )
            cm.data = data
            self._v1.replace_namespaced_config_map(
                name=self._oversight_cm,
                namespace=self._namespace,
                body=cm,
            )
        except ApiException as e:
            if e.status == 404:
                # First run — create the ConfigMap.
                new_cm = client.V1ConfigMap(
                    metadata=client.V1ObjectMeta(
                        name=self._oversight_cm,
                        namespace=self._namespace,
                        labels={"mas-domain": "6", "mas-component": "oversight"},
                    ),
                    data=data,
                )
                self._v1.create_namespaced_config_map(
                    namespace=self._namespace,
                    body=new_cm,
                )
                log.info("domain6_created")
            else:
                log.error("domain6_write_failed", status=e.status, reason=e.reason)
                raise

    def append_policy_version(self, policy_version: int,
                              policy_timestamp: int) -> None:
        """
        Record a new POLICY_READY broadcast in the Domain 6 version history.
        Called every time the SA broadcasts POLICY_READY.
        """
        oversight = self.read_oversight()
        versions = oversight.get("policy_versions", [])
        versions.append({
            "policy_version": policy_version,
            "broadcast_ms": int(time.time() * 1000),
            "policy_timestamp": policy_timestamp,
        })
        # Retain last 100 version records — prevent unbounded growth.
        oversight["policy_versions"] = versions[-100:]
        self._write_oversight(oversight)
        log.info("domain6_policy_version_recorded",
                 policy_version=policy_version)

    def append_emergency_stop_event(self, event_type: str,
                                    detail: dict) -> None:
        """
        Record an EMERGENCY_STOP or RESUME event in Domain 6.
        Called whenever the operator issues either command.
        """
        oversight = self.read_oversight()
        events = oversight.get("emergency_stop_events", [])
        events.append({
            "event_type": event_type,
            "timestamp_ms": int(time.time() * 1000),
            **detail,
        })
        # Retain last 200 events.
        oversight["emergency_stop_events"] = events[-200:]
        self._write_oversight(oversight)
        log.info("domain6_emergency_event_recorded", event_type=event_type)
