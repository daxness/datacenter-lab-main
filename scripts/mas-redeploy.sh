#!/bin/bash
# mas-redeploy.sh
#
# Fast development iteration script.
# Use this after changing Python source files — it does NOT recreate the
# cluster or redeploy workloads/monitoring. It only:
#   1. Rebuilds the MRA Docker image
#   2. Loads the new image into the running K3D cluster
#   3. Rolls the MRA deployments to pick up the new image
#
# Run from the repo root: bash scripts/mas-redeploy.sh
#
set -euo pipefail

CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================"
echo "  MAS — Fast MRA Redeploy"
echo "======================================"
echo ""

echo "[1/3] Rebuilding mas-mra:latest image..."
docker build -t mas-mra:latest "$LAB_DIR/mas-agent/"
echo "      Image built."

echo "[2/3] Loading image into K3D cluster '$CLUSTER_NAME'..."
k3d image import mas-mra:latest --cluster "$CLUSTER_NAME"
echo "      Image imported into all nodes."

echo "[3/3] Rolling restart of MRA deployments..."
kubectl rollout restart deployment/mra-nginx     -n mas-system
kubectl rollout restart deployment/mra-redis     -n mas-system
kubectl rollout restart deployment/mra-stress-ng -n mas-system

echo "      Waiting for rollout to complete..."
kubectl rollout status deployment/mra-nginx     -n mas-system --timeout=60s
kubectl rollout status deployment/mra-redis     -n mas-system --timeout=60s
kubectl rollout status deployment/mra-stress-ng -n mas-system --timeout=60s

echo ""
echo "Done. Watch live logs with:"
echo "  kubectl logs -n mas-system -l app=mas-mra -f --prefix"
echo ""
kubectl get pods -n mas-system -o wide
