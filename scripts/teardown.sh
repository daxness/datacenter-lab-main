#!/bin/bash
set -e

CLUSTER_NAME="datacenter-lab"

echo ""
echo "======================================"
echo "  Datacenter Lab — Teardown"
echo "======================================"
echo ""

# Confirm before deleting
read -p "  This will DELETE the cluster '$CLUSTER_NAME'. Continue? [y/N] " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "  Aborted."
  exit 0
fi

echo ""
echo "[1/2] Deleting k3d cluster..."
k3d cluster delete "$CLUSTER_NAME"

echo "[2/2] Cleaning up local kubeconfig context..."
kubectl config delete-context "k3d-${CLUSTER_NAME}" 2>/dev/null || true
kubectl config delete-cluster "k3d-${CLUSTER_NAME}" 2>/dev/null || true

echo ""
echo "  Cluster '$CLUSTER_NAME' has been fully removed."
echo ""
