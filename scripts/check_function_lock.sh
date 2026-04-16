#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="${WORKSPACE_ROOT:-$(cd "$repo_root/.." && pwd)}"
go_bin="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"

echo "[bitfs-fnlock] check BitFS+BFTP function signatures"
"$go_bin" run "$workspace_root/BitFS-contract/tools/fnlockcheck" \
  -workspace-root "$workspace_root" \
  -go-bin "$go_bin" \
  -modules bitfs,bftp

echo "[bitfs-fnlock] ok"

