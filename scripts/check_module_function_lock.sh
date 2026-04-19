#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace_root="${WORKSPACE_ROOT:-$(cd "$repo_root/.." && pwd)}"
go_bin="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"

echo "[bitfs-modulelock] check module function signatures"

normalized_tags="$(printf '%s' "${BITFS_GO_TAGS:-}" | sed -E 's/[[:space:],]+/,/g; s/^,+//; s/,+$//')"
case ",${normalized_tags}," in
  *",with_indexresolve,"*)
    "$go_bin" run -tags "$normalized_tags" "$workspace_root/BitFS/tools/modulelockcheck" \
      -workspace-root "$workspace_root" \
      -go-bin "$go_bin" \
      -modules indexresolve
    ;;
  *)
    echo "[bitfs-modulelock] skip module function signatures"
    ;;
esac

echo "[bitfs-modulelock] ok"
