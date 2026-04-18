#!/usr/bin/env bash
# Phase 3 – Master Installer: Runs all steps in sequence
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_DIR="${SCRIPT_DIR}/environment-setup"
GREEN='[0;32m'; YELLOW='[1;33m'; NC='[0m'

run_step() {
    local name="$1"; local script="$2"
    echo ""
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}  Running: ${name}${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    bash "${ENV_DIR}/${script}"
}

run_step "Step 1 — Install Docker CE"              "install_docker.sh"
run_step "Step 2 — Install kubectl v1.31.5"        "install_kubectl.sh"
run_step "Step 3 — Install k3d v5.8.3"             "install_k3d.sh"
run_step "Step 4 — kubectl Autocompletion"         "kubectl_autocompletion.sh"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  All steps complete — running validation  ${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
bash "${ENV_DIR}/validation.sh"
