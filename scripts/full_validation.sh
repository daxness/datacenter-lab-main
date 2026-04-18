#!/usr/bin/env bash
# =============================================================================
# datacenter-lab — Full System Validation
# Tests: cluster, nodes, taints, labels, workloads, monitoring, networking,
#        scheduling constraints, Prometheus targets, Grafana reachability
# =============================================================================
set -uo pipefail

# ── Colors & helpers ─────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
RESULTS=()

pass() { echo -e "  ${GREEN}✓${NC} $1"; PASS=$((PASS+1)); RESULTS+=("PASS|$1"); }
fail() { echo -e "  ${RED}✗${NC} $1"; FAIL=$((FAIL+1)); RESULTS+=("FAIL|$1"); }
warn() { echo -e "  ${YELLOW}!${NC} $1"; WARN=$((WARN+1));  RESULTS+=("WARN|$1"); }
section() { echo -e "\n${CYAN}${BOLD}── $1 ──${NC}"; }

CLUSTER="datacenter-lab"

# =============================================================================
# 1. TOOLS
# =============================================================================
section "1. Tool availability"

for tool in kubectl helm k3d docker; do
  if command -v $tool &>/dev/null; then
    pass "$tool is installed ($(${tool} version --short 2>/dev/null | head -1 || ${tool} version 2>/dev/null | head -1 || echo 'ok'))"
  else
    fail "$tool not found in PATH"
  fi
done

# =============================================================================
# 2. CLUSTER
# =============================================================================
section "2. Cluster state"

CTX=$(kubectl config current-context 2>/dev/null || echo "none")
if [[ "$CTX" == "k3d-${CLUSTER}" ]]; then
  pass "Active context: $CTX"
else
  fail "Wrong context: '$CTX' (expected k3d-${CLUSTER})"
fi

NODE_COUNT=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "$NODE_COUNT" -eq 4 ]]; then
  pass "4 nodes found"
else
  fail "Expected 4 nodes, got $NODE_COUNT"
fi

NOT_READY=$(kubectl get nodes --no-headers 2>/dev/null | grep -v " Ready" || true | wc -l | tr -d ' ')
if [[ "$NOT_READY" -eq 0 ]]; then
  pass "All nodes are Ready"
else
  fail "$NOT_READY node(s) not Ready"
fi

# k3d containers
K3D_CONTAINERS=$(docker ps --filter "name=k3d-${CLUSTER}" --format "{{.Names}}" 2>/dev/null | wc -l | tr -d ' ')
if [[ "$K3D_CONTAINERS" -eq 5 ]]; then
  pass "5 k3d Docker containers running (server + 3 agents + loadbalancer)"
else
  fail "Expected 5 k3d containers, got $K3D_CONTAINERS"
fi

# =============================================================================
# 3. NODE LABELS & TAINTS
# =============================================================================
section "3. Node labels and taints"

# server-0 label
SERVER_ROLE=$(kubectl get node k3d-${CLUSTER}-server-0 --show-labels 2>/dev/null | grep -o 'role=master' || echo "")
if [[ -n "$SERVER_ROLE" ]]; then
  pass "server-0 has label role=master"
else
  fail "server-0 missing label role=master"
fi

# server-0 taint
TAINT=$(kubectl describe node k3d-${CLUSTER}-server-0 2>/dev/null | grep "NoSchedule" | grep "control-plane" || echo "")
if [[ -n "$TAINT" ]]; then
  pass "server-0 has NoSchedule taint"
else
  fail "server-0 missing NoSchedule taint"
fi

# worker labels
for agent in agent-0 agent-1 agent-2; do
  W_ROLE=$(kubectl get node k3d-${CLUSTER}-${agent} --show-labels 2>/dev/null | grep -o 'role=worker' || echo "")
  if [[ -n "$W_ROLE" ]]; then
    pass "${agent} has label role=worker"
  else
    fail "${agent} missing label role=worker"
  fi
done

# Traefik must NOT be present
TRAEFIK=$(kubectl get pods -A --no-headers 2>/dev/null | grep -i traefik | wc -l | tr -d ' ')
if [[ "$TRAEFIK" -eq 0 ]]; then
  pass "Traefik is disabled (not running)"
else
  fail "Traefik found — should be disabled"
fi

# =============================================================================
# 4. NAMESPACES
# =============================================================================
section "4. Namespaces"

for ns in workloads monitoring; do
  STATUS=$(kubectl get namespace $ns -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [[ "$STATUS" == "Active" ]]; then
    pass "Namespace '$ns' is Active"
  else
    fail "Namespace '$ns' not found or not Active"
  fi
done

# =============================================================================
# 5. WORKLOADS
# =============================================================================
section "5. Workload pods (namespace: workloads)"

TOTAL_PODS=$(kubectl get pods -n workloads --no-headers 2>/dev/null | wc -l | tr -d ' ')
RUNNING_PODS=$(kubectl get pods -n workloads --no-headers 2>/dev/null | grep " Running" | wc -l | tr -d ' ')

if [[ "$TOTAL_PODS" -eq 6 ]]; then
  pass "6 workload pods found (nginx×3, redis×1, stress-ng×2)"
else
  fail "Expected 6 workload pods, got $TOTAL_PODS"
fi

if [[ "$RUNNING_PODS" -eq 6 ]]; then
  pass "All 6 workload pods are Running"
else
  fail "Only $RUNNING_PODS/6 workload pods Running"
fi

# Per-deployment checks
for deploy in nginx redis stress-ng; do
  READY=$(kubectl get deployment $deploy -n workloads -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
  DESIRED=$(kubectl get deployment $deploy -n workloads -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "?")
  if [[ "$READY" == "$DESIRED" ]]; then
    pass "Deployment $deploy: $READY/$DESIRED replicas Ready"
  else
    fail "Deployment $deploy: $READY/$DESIRED replicas Ready"
  fi
done

# =============================================================================
# 6. SCHEDULING CONSTRAINTS
# =============================================================================
section "6. Scheduling constraints"

# No workload pods on server-0
SERVER_WORKLOAD_PODS=$(kubectl get pods -n workloads -o wide --no-headers 2>/dev/null | \
  grep "k3d-${CLUSTER}-server-0" | wc -l | tr -d ' ')
if [[ "$SERVER_WORKLOAD_PODS" -eq 0 ]]; then
  pass "No workload pods scheduled on server-0 (taint respected)"
else
  fail "$SERVER_WORKLOAD_PODS workload pod(s) running on server-0 — taint not respected"
fi

# nginx spread across agents
NGINX_NODES=$(kubectl get pods -n workloads -l app=nginx -o wide --no-headers 2>/dev/null | \
  awk '{print $7}' | sort -u | wc -l | tr -d ' ')
if [[ "$NGINX_NODES" -ge 2 ]]; then
  pass "nginx pods spread across $NGINX_NODES different nodes"
else
  warn "nginx pods on only $NGINX_NODES node(s) — check topology spread"
fi

# stress-ng on different nodes
STRESS_NODES=$(kubectl get pods -n workloads -l app=stress-ng -o wide --no-headers 2>/dev/null | \
  awk '{print $7}' | sort -u | wc -l | tr -d ' ')
if [[ "$STRESS_NODES" -ge 2 ]]; then
  pass "stress-ng pods spread across $STRESS_NODES different nodes"
else
  warn "stress-ng pods on only $STRESS_NODES node(s) — check topology spread"
fi

# =============================================================================
# 7. SERVICES
# =============================================================================
section "7. Services"

for svc in nginx redis; do
  SVC_STATUS=$(kubectl get svc $svc -n workloads -o jsonpath='{.spec.type}' 2>/dev/null || echo "")
  if [[ "$SVC_STATUS" == "ClusterIP" ]]; then
    pass "Service '$svc' exists (ClusterIP)"
  else
    fail "Service '$svc' not found or wrong type"
  fi
done

# Test nginx reachability from within cluster
NGINX_IP=$(kubectl get svc nginx -n workloads -o jsonpath='{.spec.clusterIP}' 2>/dev/null || echo "")
NGINX_CURL=$(kubectl run curl-test --image=curlimages/curl:latest --rm -it --restart=Never \
  --timeout=30s -q -- curl -s -o /dev/null -w "%{http_code}" http://${NGINX_IP}:80/ 2>/dev/null || echo "000")
if [[ "$NGINX_CURL" == "200" ]]; then
  pass "nginx service reachable from within cluster (HTTP 200)"
else
  warn "nginx HTTP check returned '$NGINX_CURL' (may need a moment to settle)"
fi

# =============================================================================
# 8. MONITORING STACK
# =============================================================================
section "8. Monitoring stack (namespace: monitoring)"

MONITORING_PODS=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | wc -l | tr -d ' ')
MONITORING_RUNNING=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | grep " Running" | wc -l | tr -d ' ')

if [[ "$MONITORING_PODS" -gt 0 ]]; then
  pass "$MONITORING_PODS monitoring pods found"
else
  fail "No monitoring pods found"
fi

if [[ "$MONITORING_PODS" -eq "$MONITORING_RUNNING" ]]; then
  pass "All $MONITORING_RUNNING monitoring pods Running"
else
  fail "$MONITORING_RUNNING/$MONITORING_PODS monitoring pods Running"
fi

# Check key components
for component in prometheus grafana; do
  POD=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | grep -i "$component" | grep "Running" | head -1 || echo "")
  if [[ -n "$POD" ]]; then
    pass "$component pod is Running"
  else
    fail "$component pod not Running"
  fi
done

# node-exporter — should be on all 4 nodes
NODE_EXPORTER_COUNT=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=prometheus-node-exporter \
  --no-headers 2>/dev/null | grep "Running" | wc -l | tr -d ' ')
if [[ "$NODE_EXPORTER_COUNT" -eq 4 ]]; then
  pass "node-exporter running on all 4 nodes (DaemonSet)"
else
  warn "node-exporter running on $NODE_EXPORTER_COUNT/4 nodes"
fi

# kube-state-metrics
KSM=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=kube-state-metrics \
  --no-headers 2>/dev/null | grep "Running" | wc -l | tr -d ' ')
if [[ "$KSM" -ge 1 ]]; then
  pass "kube-state-metrics is Running"
else
  fail "kube-state-metrics not Running"
fi

# AlertManager must be disabled
AM=$(kubectl get pods -n monitoring --no-headers 2>/dev/null | grep -i alertmanager | wc -l | tr -d ' ')
if [[ "$AM" -eq 0 ]]; then
  pass "AlertManager is disabled (as configured)"
else
  warn "AlertManager found — expected it to be disabled"
fi

# =============================================================================
# 9. MONITORING PLACEMENT (server-0 only)
# =============================================================================
section "9. Monitoring pod placement"

GRAFANA_NODE=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana \
  -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || echo "")
if [[ "$GRAFANA_NODE" == "k3d-${CLUSTER}-server-0" ]]; then
  pass "Grafana is on server-0"
else
  fail "Grafana on wrong node: $GRAFANA_NODE"
fi

PROM_NODE=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=prometheus \
  -o jsonpath='{.items[0].spec.nodeName}' 2>/dev/null || echo "")
if [[ "$PROM_NODE" == "k3d-${CLUSTER}-server-0" ]]; then
  pass "Prometheus is on server-0"
else
  fail "Prometheus on wrong node: $PROM_NODE"
fi

# =============================================================================
# 10. PROMETHEUS TARGETS
# =============================================================================
section "10. Prometheus targets"

# Port-forward in background
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus \
  19090:9090 &>/dev/null &
PF_PID=$!
sleep 4

TARGETS_JSON=$(curl -s http://localhost:19090/api/v1/targets 2>/dev/null || echo "{}")
kill $PF_PID 2>/dev/null || true

UP_COUNT=$(echo "$TARGETS_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(sum(1 for t in d.get('data',{}).get('activeTargets',[]) if t.get('health')=='up'))" \
  2>/dev/null || echo "0")
DOWN_COUNT=$(echo "$TARGETS_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(sum(1 for t in d.get('data',{}).get('activeTargets',[]) if t.get('health')!='up'))" \
  2>/dev/null || echo "?")

if [[ "$UP_COUNT" -gt 0 ]]; then
  pass "$UP_COUNT Prometheus targets UP"
else
  warn "Could not verify Prometheus targets (port-forward may have failed)"
fi

if [[ "$DOWN_COUNT" == "0" ]]; then
  pass "No Prometheus targets DOWN"
elif [[ "$DOWN_COUNT" == "?" ]]; then
  warn "Could not count DOWN targets"
else
  fail "$DOWN_COUNT Prometheus target(s) DOWN"
fi

# =============================================================================
# 11. GRAFANA REACHABILITY
# =============================================================================
section "11. Grafana reachability"

GRAFANA_HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/login 2>/dev/null || echo "000")
if [[ "$GRAFANA_HTTP" == "200" ]]; then
  pass "Grafana login page reachable at localhost:3000 (HTTP 200)"
elif [[ "$GRAFANA_HTTP" == "302" ]]; then
  pass "Grafana reachable at localhost:3000 (HTTP 302 redirect — normal)"
else
  # Try port 32000
  GRAFANA_HTTP2=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:32000/login 2>/dev/null || echo "000")
  if [[ "$GRAFANA_HTTP2" =~ ^(200|302)$ ]]; then
    pass "Grafana reachable at localhost:32000 (HTTP $GRAFANA_HTTP2)"
  else
    warn "Grafana not reachable on :3000 or :32000 — may need port-forward"
  fi
fi

# =============================================================================
# 12. RESOURCE USAGE SANITY
# =============================================================================
section "12. Resource usage (top)"

if kubectl top nodes &>/dev/null 2>&1; then
  CPU_WARN=$(kubectl top nodes --no-headers 2>/dev/null | awk '{gsub(/%/,"",$3); if($3+0>90) print $1}' || echo "")
  if [[ -z "$CPU_WARN" ]]; then
    pass "No node above 90% CPU"
  else
    warn "High CPU on: $CPU_WARN"
  fi
  MEM_WARN=$(kubectl top nodes --no-headers 2>/dev/null | awk '{gsub(/%/,"",$5); if($5+0>90) print $1}' || echo "")
  if [[ -z "$MEM_WARN" ]]; then
    pass "No node above 90% memory"
  else
    warn "High memory on: $MEM_WARN"
  fi
else
  warn "kubectl top not available (metrics-server may need a moment)"
fi

# =============================================================================
# 13. KUSTOMIZE OVERLAY
# =============================================================================
section "13. Kustomize overlay"

if kubectl kustomize overlays/high-load &>/dev/null 2>&1; then
  OVERLAY_REPLICAS=$(kubectl kustomize overlays/high-load 2>/dev/null | \
    grep -A2 'name: stress-ng' | grep 'replicas' | awk '{print $2}' || echo "?")
  pass "high-load overlay renders (stress-ng replicas → $OVERLAY_REPLICAS)"
else
  warn "Could not render high-load overlay (run from datacenter-lab-main/ root)"
fi

# =============================================================================
# SUMMARY
# =============================================================================
TOTAL=$((PASS+FAIL+WARN))
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"
echo -e "${BOLD}  Validation Summary${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"
echo -e "  Total checks : $TOTAL"
echo -e "  ${GREEN}Passed${NC}       : $PASS"
echo -e "  ${YELLOW}Warnings${NC}     : $WARN"
echo -e "  ${RED}Failed${NC}       : $FAIL"
echo -e "${BOLD}═══════════════════════════════════════════════${NC}"

if [[ $FAIL -eq 0 && $WARN -eq 0 ]]; then
  echo -e "\n  ${GREEN}${BOLD}✓ All checks passed — cluster is fully healthy.${NC}\n"
elif [[ $FAIL -eq 0 ]]; then
  echo -e "\n  ${YELLOW}${BOLD}! No failures, but $WARN warning(s) to review.${NC}\n"
else
  echo -e "\n  ${RED}${BOLD}✗ $FAIL check(s) failed — review output above.${NC}\n"
fi

exit $FAIL
