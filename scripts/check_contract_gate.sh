#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
contract_root="${CONTRACT_ROOT:-/home/david/Workspaces/bsv8/BFTP-contract}"

echo "[bitfs-gate] check bftp local contract boundary"
"$repo_root/scripts/check_bftp_contract_boundary.sh"

echo "[bitfs-gate] check main framework function lock"
"$repo_root/scripts/check_function_lock.sh"

echo "[bitfs-gate] check module function lock"
"$repo_root/scripts/check_module_function_lock.sh"

echo "[bitfs-gate] check BFTP-contract gate"
"$contract_root/scripts/check_ci_gate.sh"

echo "[bitfs-gate] all contract gates passed"
