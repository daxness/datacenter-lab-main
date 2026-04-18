#!/usr/bin/env bash
# Phase 3 – Step 4: Enable kubectl shell autocompletion + alias
set -euo pipefail

GREEN='[0;32m'; YELLOW='[1;33m'; NC='[0m'
info() { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }

BASHRC="$HOME/.bashrc"
MARKER="# kubectl autocompletion – Phase 3"

if grep -qF "$MARKER" "$BASHRC" 2>/dev/null; then
    warn "Autocompletion block already present in $BASHRC — skipping."
else
    info "Adding kubectl autocompletion and alias to $BASHRC..."
    cat >> "$BASHRC" <<'EOF'

# kubectl autocompletion – Phase 3
source <(kubectl completion bash)
alias k=kubectl
complete -o default -F __start_kubectl k
EOF
fi

# Apply immediately
# shellcheck source=/dev/null
source "$BASHRC" 2>/dev/null || true

echo ""
info "✓ Step 4 complete — autocompletion configured. Run 'source ~/.bashrc' or open a new terminal."
