#!/bin/bash
set -euo pipefail
 
CLUSTER_NAME="datacenter-lab"
LAB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
 
echo ""
echo "======================================"
echo "  Datacenter Lab — Full Deploy"
echo "======================================"
echo ""
 
# ── Step 1: Create k3d cluster ───────────────────────────────────────────────
echo "[1/6] Creating k3d cluster..."
k3d cluster create --config "$LAB_DIR/cluster/k3d-config.yaml"
echo "      Cluster created."
 
# ── Step 2: Label and taint nodes ────────────────────────────────────────────
echo "[2/6] Configuring nodes (labels + taints)..."
bash "$LAB_DIR/scheduling/node-config.sh"
echo "      Nodes configured."
 
# ── Step 3: Deploy namespaces + workloads ────────────────────────────────────
echo "[3/6] Deploying namespaces + workloads (nginx, redis, stress-ng)..."
kubectl apply -k "$LAB_DIR/"
echo "      Workloads applied."
 
# ── Step 4: Deploy monitoring stack ──────────────────────────────────────────
echo "[4/6] Deploying monitoring stack (Prometheus + Grafana)..."
helm repo add prometheus-community \
  https://prometheus-community.github.io/helm-charts --force-update
helm repo update
helm upgrade --install monitoring \
  prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values "$LAB_DIR/monitoring/helm-values.yaml" \
  --timeout 10m
echo "      Monitoring stack deployed."
 
# ── Step 5: Build and load MRA image into K3D ────────────────────────────────
echo "[5/6] Building and loading MRA Docker image..."
docker build -t mas-mra:latest "$LAB_DIR/mas-agent/"
# k3d image import pushes the image directly into every cluster node.
# No registry needed. imagePullPolicy: Never in manifests uses this image.
k3d image import mas-mra:latest --cluster "$CLUSTER_NAME"
echo "      MRA image loaded into cluster."
 
# ── Step 6: Deploy MAS infrastructure + MRA agents ───────────────────────────
echo "[6/6] Deploying MAS infrastructure and MRA agents..."
 
# 6a — Namespace
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/mas-namespace.yaml"
 
# 6b — Shared KB storage (PV + PVC)
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/kb-storage.yaml"
 
# 6c — Domain 1 (policy) and Domain 2 (topology) seed ConfigMaps
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/domain1-policy.yaml"
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/domain2-topology.yaml"
 
# 6d — MQTT broker
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/mosquitto.yaml"
echo "      Waiting for MQTT broker to be ready..."
kubectl rollout status deployment/mosquitto -n mas-system --timeout=90s
 
# 6e — KB init job (creates SQLite schemas for Domain 4 and 5)
kubectl apply -f "$LAB_DIR/mas-agent/k8s/infrastructure/kb-init-job.yaml"
echo "      Waiting for KB init job to complete..."
kubectl wait --for=condition=complete job/kb-init -n mas-system --timeout=60s
 
# 6f — RBAC for MRA
kubectl apply -f "$LAB_DIR/mas-agent/k8s/base/rbac.yaml"
 
# 6g — MRA agents (one per monitored deployment)
kubectl apply -f "$LAB_DIR/mas-agent/k8s/base/mra-nginx.yaml"
kubectl apply -f "$LAB_DIR/mas-agent/k8s/base/mra-redis.yaml"
kubectl apply -f "$LAB_DIR/mas-agent/k8s/base/mra-stress-ng.yaml"
 
echo "      MAS infrastructure and MRA agents deployed."
 
# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "[Done] All components deployed."
echo ""
echo "  ┌───────────────────────────────────────────────────────────────────┐"
echo "  │  Grafana     →  http://localhost:32000  (admin / Lab@2024!)       │"
echo "  │  MRA logs    →  kubectl logs -n mas-system -l app=mas-mra -f     │"
echo "  │  KB audit    →  sqlite3 ~/datacenter-lab-main/kb-storage/         │"
echo "  │                         domain4/audit_log.db                      │"
echo "  └───────────────────────────────────────────────────────────────────┘"
echo ""
echo "--- Workloads ---"
kubectl get pods -n workloads -o wide
echo ""
echo "--- Monitoring ---"
kubectl get pods -n monitoring -o wide
echo ""
echo "--- MAS ---"
kubectl get pods -n mas-system -o wide
