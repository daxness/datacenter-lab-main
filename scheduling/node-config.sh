#!/bin/bash
# Node scheduling configuration
# Taint + label declarations for the datacenter-lab cluster

CLUSTER_NAME=datacenter-lab

# Label control-plane
kubectl label node k3d-${CLUSTER_NAME}-server-0 \
  role=master \
  node-role.kubernetes.io/master=true \
  --overwrite

# Label workers
for node in agent-0 agent-1 agent-2; do
  kubectl label node k3d-${CLUSTER_NAME}-${node} \
    role=worker \
    node-role.kubernetes.io/worker=true \
    --overwrite
done

# Taint control-plane
kubectl taint nodes k3d-${CLUSTER_NAME}-server-0 \
  node-role.kubernetes.io/control-plane=true:NoSchedule \
  --overwrite
