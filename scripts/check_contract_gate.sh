#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
contract_root="${CONTRACT_ROOT:-/home/david/Workspaces/bsv8/BitFS-contract}"

echo "[bitfs-gate] check bitfs local contract boundary"
"$repo_root/scripts/check_bitfs_contract_boundary.sh"

echo "[bitfs-gate] check BitFS-contract gate"
"$contract_root/scripts/check_ci_gate.sh"

echo "[bitfs-gate] all contract gates passed"
