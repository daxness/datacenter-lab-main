#!/bin/bash
# pfa-redeploy.sh
#
# Fast development iteration script for the PFA agent.
# Use this after changing Python source files inside pfa-agent/pfa/
# It does NOT recreate the cluster or redeploy workloads/monitoring.
#
# Run from the repo root: bash scripts/pfa-redeploy.sh
#
set -euo pipefail

CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================"
echo "  MAS — Fast PFA Redeploy"
echo "======================================"
echo ""

echo "[1/3] Rebuilding mas-pfa:latest image..."
docker build -f "$LAB_DIR/mas-agent/Dockerfile.pfa" -t mas-pfa:latest "$LAB_DIR/mas-agent/"
echo "      Image built."

echo "[2/3] Loading image into K3D cluster '$CLUSTER_NAME'..."
k3d image import mas-pfa:latest --cluster "$CLUSTER_NAME"
echo "      Image imported into all nodes."

echo "[3/3] Rolling restart of PFA deployments..."
kubectl rollout restart deployment/pfa-nginx     -n mas-system
kubectl rollout restart deployment/pfa-redis     -n mas-system
kubectl rollout restart deployment/pfa-stress-ng -n mas-system

echo "      Waiting for rollout to complete..."
kubectl rollout status deployment/pfa-nginx     -n mas-system --timeout=180s
kubectl rollout status deployment/pfa-redis     -n mas-system --timeout=180s
kubectl rollout status deployment/pfa-stress-ng -n mas-system --timeout=180s

echo ""
echo "Done. Watch live logs with:"
echo "  kubectl logs -n mas-system -l app=mas-pfa -f --prefix"
echo ""
kubectl get pods -n mas-system -o wide
