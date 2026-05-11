#!/bin/bash
# scripts/sa-redeploy.sh
#
# Fast development iteration script for the SA agent.
# Use this after changing Python source files inside mas-agent/sa/
# It does NOT recreate the cluster or redeploy workloads/monitoring.
#
# Run from the repo root: bash scripts/sa-redeploy.sh
#
set -euo pipefail

CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================"
echo "  MAS — Fast SA Redeploy"
echo "======================================"
echo ""

echo "[1/3] Rebuilding mas-sa:latest image..."
docker build -f "$LAB_DIR/mas-agent/Dockerfile.sa" -t mas-sa:latest "$LAB_DIR/mas-agent/"
echo "      Image built."

echo "[2/3] Loading image into K3D cluster '$CLUSTER_NAME'..."
k3d image import mas-sa:latest --cluster "$CLUSTER_NAME"
echo "      Image imported into all nodes."

echo "[3/3] Rolling restart of SA deployment..."
kubectl rollout restart deployment/sa -n mas-system

echo "      Waiting for rollout to complete..."
kubectl rollout status deployment/sa -n mas-system --timeout=90s

echo ""
echo "Done. SA is running."
echo ""
echo "  Web UI  →  http://localhost:32080/"
echo "  Logs    →  kubectl logs -n mas-system -l app=mas-sa -f"
echo ""
kubectl get pods -n mas-system -l app=mas-sa -o wide
