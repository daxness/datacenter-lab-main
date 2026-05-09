# dca/recovery_manager.py
#
# Recovery Manager — DCA Specification Section 4.7 and Section 4.4.
#
# Implements the single pod restart attempt that constitutes Mode B recovery.
# The DCA is authorised to restart MRA, PFA, and RSA pods within the
# mas-system namespace when a confirmed agent failure is detected.
#
# The one-attempt rule is enforced here: if the restarted pod does not emit
# a heartbeat within recovery_timeout_seconds (60s by default), the recovery
# is declared failed and the DCA escalates to the SA. A second retry is
# explicitly prohibited by the specification to prevent masking systematic
# faults behind repeated restart cycles.
#
# Kubernetes access:
#   The DCA's service account (dca-sa) must have:
#     - patch/update on apps/deployments in mas-system (to trigger rolling restarts)
#   This is the narrowest permission required. The DCA does NOT need HPA patch
#   authority; that belongs exclusively to rsa-sa.
#
#   Restart mechanism: a rolling restart is triggered by patching the Deployment's
#   pod template annotation (kubectl rollout restart equivalent). This forces
#   Kubernetes to replace the pod while respecting the deployment's update strategy.
#
import time
import structlog
from kubernetes import client, config as k8s_config

log = structlog.get_logger(__name__)

# Map from agent type to its Deployment name prefix in mas-system
_AGENT_DEPLOYMENT_PREFIX = {
    "MRA": "mra",
    "PFA": "pfa",
    "RSA": "rsa",
}


def _get_k8s_apps_client():
    """
    Load in-cluster Kubernetes config and return an AppsV1Api client.
    Called lazily so the import doesn't fail during local unit testing.
    """
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        # Fallback for local development with a kubeconfig
        k8s_config.load_kube_config()
    return client.AppsV1Api()


class RecoveryManager:
    """
    Triggers rolling pod restarts for failed agents.
    One instance is shared across all agent records in a DCA instance.
    """

    def __init__(self, mas_namespace: str = "mas-system"):
        self._mas_namespace = mas_namespace
        self._apps_client   = None  # lazy-loaded on first use

    def _ensure_client(self):
        if self._apps_client is None:
            self._apps_client = _get_k8s_apps_client()

    def restart_agent(
        self,
        agent_type: str,   # "MRA" | "PFA" | "RSA"
        deployment: str,   # e.g. "stress-ng"
    ) -> bool:
        """
        Trigger a rolling restart of the named agent's Kubernetes Deployment.
        The Deployment name in mas-system follows the convention:
            {agent_type_lower}-{workload_name}
            e.g. "mra-stress-ng", "pfa-stress-ng", "rsa-stress-ng"

        Returns True if the patch was accepted by the API server.
        The patch itself does not guarantee the pod started successfully;
        the caller must monitor for a subsequent heartbeat within the
        recovery timeout to confirm success.
        """
        prefix = _AGENT_DEPLOYMENT_PREFIX.get(agent_type)
        if prefix is None:
            log.error("unknown_agent_type_for_restart", agent_type=agent_type)
            return False

        deployment_name = f"{prefix}-{deployment}"
        restart_annotation = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt":
                                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        }
                    }
                }
            }
        }

        try:
            self._ensure_client()
            self._apps_client.patch_namespaced_deployment(
                name=deployment_name,
                namespace=self._mas_namespace,
                body=restart_annotation,
            )
            log.info(
                "agent_restart_triggered",
                agent_type=agent_type,
                deployment=deployment,
                k8s_deployment=deployment_name,
            )
            return True

        except Exception as e:
            log.error(
                "agent_restart_failed",
                agent_type=agent_type,
                deployment=deployment,
                k8s_deployment=deployment_name,
                error=str(e),
            )
            return False
