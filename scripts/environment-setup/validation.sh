#!/usr/bin/env bash
# Phase 3 – Validation: Verify all tools are correctly installed
set -euo pipefail

RED='[0;31m'; GREEN='[0;32m'; YELLOW='[1;33m'; NC='[0m'
PASS="${GREEN}[PASS]${NC}"; FAIL="${RED}[FAIL]${NC}"
pass=0; fail=0

check() {
    local desc="$1"; local cmd="$2"; local expected="$3"
    local result
    result=$(eval "$cmd" 2>/dev/null || true)
    if echo "$result" | grep -qF "$expected"; then
        echo -e "${PASS} ${desc}: ${expected}"
        ((pass++))
    else
        echo -e "${FAIL} ${desc}: expected '${expected}', got '${result}'"
        ((fail++))
    fi
}

echo -e "${YELLOW}═══════════════════════════════════════════${NC}"
echo -e "${YELLOW}  Phase 3 — Environment Validation Report  ${NC}"
echo -e "${YELLOW}═══════════════════════════════════════════${NC}"

check "Docker client version"   "docker version --format '{{.Client.Version}}'"  "26."
check "Docker hello-world"      "docker run --rm hello-world 2>&1"                "Hello from Docker"
check "kubectl client version"  "kubectl version --client -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['clientVersion']['gitVersion'])"" "v1.31.5"
check "k3d version"             "k3d version | head -1"                           "v5.8.3"
check "docker group membership" "groups $USER"                                    "docker"

echo ""
echo -e "${YELLOW}───────────────────────────────────────────${NC}"
echo -e "Results: ${GREEN}${pass} passed${NC}  ${RED}${fail} failed${NC}"
[[ $fail -eq 0 ]] && echo -e "${GREEN}✓ All validation checks passed!${NC}"                   || echo -e "${RED}✗ Some checks failed — review output above.${NC}"
