# rsa/k8s_client.py
#
# Kubernetes API wrapper for the RSA.
#
# The RSA is the ONLY agent in the MAS with authority to modify
# infrastructure state. This module encapsulates all Kubernetes API
# calls to a single, auditable surface:
#
#   get_current_replicas()        — read current spec.replicas
#   patch_replicas(target)        — write spec.replicas
#   detect_conflicting_autoscaler() — check for native HPA on this deployment
#
# Why patch spec.replicas directly instead of patching an HPA?
#   The lab environment does not deploy native Kubernetes HPAs on the
#   managed workloads — that is the entire point of the MAS: to replace
#   the native HPA with agent-driven autoscaling. The RSA therefore
#   patches the Deployment spec.replicas directly, which is the
#   authoritative replica count when no HPA is present.
#
# In-cluster vs. local configuration:
#   When running inside a K3D pod, load_incluster_config() reads the
#   ServiceAccount token mounted at /var/run/secrets/kubernetes.io/.
#   When running locally for testing, load_kube_config() reads
#   ~/.kube/config. The in_cluster flag is auto-detected from the
#   KUBERNETES_SERVICE_HOST environment variable.
#
# Retry policy:
#   Kubernetes API calls are retried up to 3 times with exponential
#   backoff (1s, 2s, 4s) on transient failures. This handles brief
#   API server unavailability without blocking the agent loop.
#
import os
import time
import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

log = structlog.get_logger(__name__)

_MAX_RETRIES = 3
_RETRY_BASE_SECONDS = 1.0


class K8sClient:

    def __init__(self, namespace: str, deployment_name: str):
        self._namespace       = namespace
        self._deployment_name = deployment_name

        in_cluster = os.environ.get("KUBERNETES_SERVICE_HOST") is not None
        if in_cluster:
            config.load_incluster_config()
            log.info("k8s_config_in_cluster")
        else:
            config.load_kube_config()
            log.info("k8s_config_local")

        self._apps_v1         = client.AppsV1Api()
        self._autoscaling_v2  = client.AutoscalingV2Api()

    def get_current_replicas(self) -> int:
        """
        Read the current spec.replicas from the Deployment object.
        This is the authoritative source — more reliable than the MRA
        belief object replica field during rapid state transitions.
        Returns 1 as a safe minimum if the field is absent.
        """
        for attempt in range(_MAX_RETRIES):
            try:
                dep = self._apps_v1.read_namespaced_deployment(
                    name=self._deployment_name,
                    namespace=self._namespace,
                )
                replicas = dep.spec.replicas
                return replicas if replicas is not None else 1
            except ApiException as e:
                if attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BASE_SECONDS * (2 ** attempt)
                    log.warning("k8s_get_replicas_retry",
                                attempt=attempt + 1, wait=wait, status=e.status)
                    time.sleep(wait)
                else:
                    log.error("k8s_get_replicas_failed", error=str(e))
                    raise

    def patch_replicas(self, target_replicas: int) -> None:
        """
        Patch the Deployment spec.replicas to target_replicas.

        CALLER RESPONSIBILITY: the cooldown UPSERT in Domain 5 MUST have
        completed successfully BEFORE this method is called. Never call
        this without a preceding CooldownManager.start() on the proactive
        and reactive paths.

        Retries up to 3 times with exponential backoff on ApiException.
        Raises on permanent failure — the caller is responsible for
        auditing the failure and escalating if necessary.
        """
        patch_body = {"spec": {"replicas": target_replicas}}

        for attempt in range(_MAX_RETRIES):
            try:
                self._apps_v1.patch_namespaced_deployment(
                    name=self._deployment_name,
                    namespace=self._namespace,
                    body=patch_body,
                )
                log.info(
                    "k8s_patch_applied",
                    namespace=self._namespace,
                    deployment=self._deployment_name,
                    target_replicas=target_replicas,
                )
                return
            except ApiException as e:
                if attempt < _MAX_RETRIES - 1:
                    wait = _RETRY_BASE_SECONDS * (2 ** attempt)
                    log.warning("k8s_patch_retry",
                                attempt=attempt + 1, wait=wait, status=e.status)
                    time.sleep(wait)
                else:
                    log.error("k8s_patch_failed",
                              target_replicas=target_replicas, error=str(e))
                    raise

    def detect_conflicting_autoscaler(self) -> bool:
        """
        Check whether a native Kubernetes HPA targets this deployment.

        A conflicting HPA continuously overwrites the RSA's replica patches,
        making autonomous control incoherent. If detected, the RSA publishes
        CONFLICTING_AUTOSCALER to the DCA and escalates to the SA.

        Returns True if a conflicting HPA is found.
        """
        try:
            hpas = self._autoscaling_v2.list_namespaced_horizontal_pod_autoscaler(
                namespace=self._namespace,
            )
            for hpa in hpas.items:
                ref = hpa.spec.scale_target_ref
                if (
                    ref.kind == "Deployment"
                    and ref.name == self._deployment_name
                ):
                    log.warning(
                        "conflicting_autoscaler_detected",
                        hpa_name=hpa.metadata.name,
                        deployment=self._deployment_name,
                    )
                    return True
            return False
        except ApiException as e:
            # Non-fatal — inability to check for HPAs does not block startup.
            # The RSA logs the error and proceeds; a conflicting HPA would
            # become apparent from erratic replica count behaviour.
            log.warning("conflicting_autoscaler_check_failed", error=str(e))
            return False

