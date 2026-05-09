#!/bin/bash
# scripts/dca-redeploy.sh
#
# Fast development iteration script for the DCA agent.
# Use this after changing Python source files inside mas-agent/dca/
# It does NOT recreate the cluster or redeploy workloads/monitoring.
#
# Run from the repo root: bash scripts/dca-redeploy.sh
#
set -euo pipefail

CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================"
echo "  MAS — Fast DCA Redeploy"
echo "======================================"
echo ""

echo "[1/3] Rebuilding mas-dca:latest image..."
docker build -f "$LAB_DIR/mas-agent/Dockerfile.dca" -t mas-dca:latest "$LAB_DIR/mas-agent/"
echo "      Image built."

echo "[2/3] Loading image into K3D cluster '$CLUSTER_NAME'..."
k3d image import mas-dca:latest --cluster "$CLUSTER_NAME"
echo "      Image imported into all nodes."

echo "[3/3] Rolling restart of DCA deployment..."
kubectl rollout restart deployment/dca-worker -n mas-system

echo "      Waiting for rollout to complete..."
kubectl rollout status deployment/dca-worker -n mas-system --timeout=90s

echo ""
echo "Done. Watch live logs with:"
echo "  kubectl logs -n mas-system -l app=mas-dca -f"
echo ""
kubectl get pods -n mas-system -o wide
