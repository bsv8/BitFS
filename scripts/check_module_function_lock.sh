#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="${WORKSPACE_ROOT:-$(cd "$repo_root/.." && pwd)}"
go_bin="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"

echo "[bitfs-modulelock] check module function signatures"

"$go_bin" run "$workspace_root/BitFS/tools/modulelockcheck" \
  -workspace-root "$workspace_root" \
  -go-bin "$go_bin" \
  -modules indexresolve

echo "[bitfs-modulelock] ok"
