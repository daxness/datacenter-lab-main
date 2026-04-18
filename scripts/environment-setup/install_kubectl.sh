#!/usr/bin/env bash
# Phase 3 – Step 2: Install kubectl v1.31.5
set -euo pipefail

RED='[0;31m'; GREEN='[0;32m'; NC='[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

KUBECTL_VERSION="v1.31.5"
ARCH="$(uname -m)"
[[ "$ARCH" == "x86_64" ]] && ARCH="amd64"
[[ "$ARCH" == "aarch64" ]] && ARCH="arm64"

info "Downloading kubectl ${KUBECTL_VERSION} (${ARCH})..."
curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" -o /tmp/kubectl

# Verify checksum
info "Verifying checksum..."
curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl.sha256" -o /tmp/kubectl.sha256
echo "$(cat /tmp/kubectl.sha256)  /tmp/kubectl" | sha256sum --check || error "Checksum verification failed!"

info "Installing kubectl to /usr/local/bin..."
sudo install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl
rm /tmp/kubectl /tmp/kubectl.sha256

# Verify
info "Verifying kubectl version..."
INSTALLED=$(kubectl version --client -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['clientVersion']['gitVersion'])")
[[ "$INSTALLED" == "$KUBECTL_VERSION" ]] || error "Version mismatch: expected ${KUBECTL_VERSION}, got ${INSTALLED}"

echo ""
info "✓ Step 2 complete — kubectl ${INSTALLED} installed successfully."
