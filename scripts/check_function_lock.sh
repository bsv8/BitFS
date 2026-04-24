#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
go_bin="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"

echo "[bitfs-fnlock] check main framework function signatures"
"$go_bin" run "$repo_root/tools/modulelockcheck" \
  -modules bftp

echo "[bitfs-fnlock] ok"
