#!/usr/bin/env bash
# Phase 3 – Step 3: Install k3d v5.8.3
set -euo pipefail

RED='[0;31m'; GREEN='[0;32m'; NC='[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

K3D_VERSION="v5.8.3"

info "Installing k3d ${K3D_VERSION}..."
curl -fsSL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | TAG="${K3D_VERSION}" bash

# Verify
info "Verifying k3d version..."
K3D_INSTALLED=$(k3d version | head -1 | awk '{print $3}')
[[ "$K3D_INSTALLED" == "$K3D_VERSION" ]] || error "Version mismatch: expected ${K3D_VERSION}, got ${K3D_INSTALLED}"

echo ""
info "✓ Step 3 complete — k3d ${K3D_INSTALLED} installed successfully."
k3d version
