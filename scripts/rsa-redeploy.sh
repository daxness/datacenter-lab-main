#!/bin/bash
# scripts/rsa-redeploy.sh
#
# Fast development iteration script for the RSA agent.
# Use this after changing Python source files inside mas-agent/rsa/
# It does NOT recreate the cluster or redeploy workloads/monitoring.
#
# Run from the repo root: bash scripts/rsa-redeploy.sh
#
set -euo pipefail

CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo ""
echo "======================================"
echo "  MAS — Fast RSA Redeploy"
echo "======================================"
echo ""

echo "[1/3] Rebuilding mas-rsa:latest image..."
docker build -f "$LAB_DIR/mas-agent/Dockerfile.rsa" -t mas-rsa:latest "$LAB_DIR/mas-agent/"
echo "      Image built."

echo "[2/3] Loading image into K3D cluster '$CLUSTER_NAME'..."
k3d image import mas-rsa:latest --cluster "$CLUSTER_NAME"
echo "      Image imported into all nodes."

echo "[3/3] Rolling restart of RSA deployments..."
kubectl rollout restart deployment/rsa-nginx     -n mas-system
kubectl rollout restart deployment/rsa-redis     -n mas-system
kubectl rollout restart deployment/rsa-stress-ng -n mas-system

echo "      Waiting for rollout to complete..."
kubectl rollout status deployment/rsa-nginx     -n mas-system --timeout=60s
kubectl rollout status deployment/rsa-redis     -n mas-system --timeout=60s
kubectl rollout status deployment/rsa-stress-ng -n mas-system --timeout=60s

echo ""
echo "Done. Watch live logs with:"
echo "  kubectl logs -n mas-system -l app=mas-rsa -f --prefix"
echo ""
kubectl get pods -n mas-system -o wide

