# datacenter-lab

A Kubernetes-based data center simulation built with **k3d** inside a Xubuntu 24.04 LTS VM on VMware.

Reimplements a full GNS3 + k3s production-style setup using lightweight Docker-based nodes, with a complete monitoring stack and scheduling constraints.

---

## Architecture

```
Host OS (Windows/Linux)
└── VMware VM (Xubuntu 24.04)
    └── Docker
        └── k3d cluster: datacenter-lab
            ├── server-0     → control-plane (Prometheus, Grafana)
            ├── agent-0      → worker (workloads)
            ├── agent-1      → worker (workloads)
            └── agent-2      → worker (workloads)
```

### Node Roles

| Node | Role | Taint | Workloads |
|---|---|---|---|
| server-0 | control-plane | NoSchedule | Prometheus, Grafana, Operator, KSM |
| agent-0 | worker | none | nginx, redis, stress-ng |
| agent-1 | worker | none | nginx, redis, stress-ng |
| agent-2 | worker | none | nginx, redis, stress-ng |

---

## Folder Structure

```
datacenter-lab/
├── deploy.sh                    ← Full one-shot bootstrap
├── teardown.sh                  ← Cluster teardown
├── cluster/
│   └── k3d-config.yaml          ← k3d cluster definition
├── namespaces/
│   ├── workloads-ns.yaml
│   └── monitoring-ns.yaml
├── workloads/
│   ├── nginx-deployment.yaml
│   ├── nginx-service.yaml
│   ├── redis-deployment.yaml
│   ├── redis-service.yaml
│   └── stress-ng-deployment.yaml
└── monitoring/
    └── helm-values.yaml         ← kube-prometheus-stack overrides
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker CE | 24+ | `apt` (NOT Snap) |
| k3d | v5+ | `curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash` |
| kubectl | v1.29+ | `apt` |
| Helm | v3.20+ | `curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash` |

---

## Deploy

```bash
cd ~/datacenter-lab
./deploy.sh
```

The script:
1. Creates the k3d cluster
2. Applies namespaces
3. Labels and taints nodes
4. Deploys workloads (nginx, redis, stress-ng)
5. Deploys monitoring (Prometheus + Grafana via Helm)

---

## Access

### Grafana
| Method | URL |
|---|---|
| From Xubuntu VM | http://localhost:32000 |
| From host OS (same network) | http://192.168.1.15:32000 |
| Via SSH tunnel | `ssh -NL 33000:localhost:32000 raf@192.168.1.15` → http://localhost:33000 |

**Login:** `admin` / `Lab@2024!`

---

## Workloads

| App | Namespace | Replicas | Scheduling |
|---|---|---|---|
| nginx | workloads | 3 | nodeAffinity (workers) + topologySpread |
| redis | workloads | 1 | nodeAffinity (workers) + topologySpread |
| stress-ng | workloads | 2 | nodeAffinity (workers) + topologySpread |

All workloads use:
- `nodeAffinity: required` → workers only
- `topologySpreadConstraints: ScheduleAnyway` → best-effort even spread
- `imagePullPolicy: IfNotPresent`

---

## Monitoring Stack

Deployed via `kube-prometheus-stack` Helm chart.

| Component | Node | Notes |
|---|---|---|
| Prometheus | server-0 | 24h retention |
| Grafana | server-0 | NodePort 32000 |
| Prometheus Operator | server-0 | |
| kube-state-metrics | server-0 | Pinned to v2.13.0 |
| node-exporter | all nodes | DaemonSet |
| AlertManager | disabled | Not needed for lab |

---

## Teardown

```bash
cd ~/datacenter-lab
./teardown.sh
```

Prompts for confirmation before deleting the cluster and cleaning kubeconfig.

---

## Key Design Decisions

- **k3d over k3s+VMs**: ~30s cluster spin-up vs ~10 min; no VXLAN issues; cluster-as-code
- **Monitoring pinned to server-0**: administrative separation from workloads
- **`ScheduleAnyway` over `requiredAntiAffinity`**: prevents pods from getting stuck in `Pending`
- **`helm upgrade --install`**: idempotent — works for both fresh install and re-deploy
- **AlertManager disabled**: reduces RAM overhead; not needed for a metrics-only lab

