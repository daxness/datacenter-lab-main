#!/usr/bin/env bash
# Phase 3 – Step 1: Install Docker CE from the official apt repository
set -euo pipefail

RED='[0;31m'; GREEN='[0;32m'; YELLOW='[1;33m'; NC='[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── 1. Remove conflicting packages ──────────────────────────────────────────
info "Removing conflicting Docker packages..."
for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do
    sudo apt-get remove -y "$pkg" 2>/dev/null || true
done

# ── 2. Install dependencies ──────────────────────────────────────────────────
info "Installing dependencies..."
sudo apt-get update -qq
sudo apt-get install -y ca-certificates curl

# ── 3. Add Docker official GPG key ──────────────────────────────────────────
info "Adding Docker GPG key..."
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# ── 4. Add Docker apt repository ────────────────────────────────────────────
info "Adding Docker apt repository..."
sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo apt-get update -qq

# ── 5. Install Docker CE packages ────────────────────────────────────────────
info "Installing Docker CE..."
sudo apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin

# ── 6. Add current user to docker group ──────────────────────────────────────
info "Adding $USER to docker group..."
sudo usermod -aG docker "$USER"

# ── 7. Verify ────────────────────────────────────────────────────────────────
info "Verifying Docker installation..."
# Run docker commands via sg so the new group is active without full logout
sg docker -c "docker version" || error "docker version failed"
sg docker -c "docker run --rm hello-world" || error "hello-world test failed"

echo ""
info "✓ Step 1 complete — Docker CE installed successfully."
warn "NOTE: Log out and back in (or run 'newgrp docker') for group change to take effect in your current shell."
