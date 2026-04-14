#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-staged}"
RANGE="${2:-}"
REPO_ROOT="$(git rev-parse --show-toplevel)"
CONFIG_PATH="$REPO_ROOT/.guard/guard.json"

GO_BIN="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"
if [ ! -x "$GO_BIN" ]; then
  GO_BIN="$(command -v go || true)"
fi
if [ -z "$GO_BIN" ]; then
  echo "guard: go toolchain not found" >&2
  exit 1
fi

resolve_guard_root() {
  local candidate
  if [ -n "${GUARD_ROOT:-}" ] && [ -d "${GUARD_ROOT}/cmd/guard" ]; then
    echo "$GUARD_ROOT"
    return 0
  fi
  for candidate in \
    "/home/david/Workspaces/guard-private" \
    "$(cd "$REPO_ROOT/.." && pwd)/guard-private"
  do
    if [ -d "$candidate/cmd/guard" ]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

GUARD_ROOT_RESOLVED="$(resolve_guard_root || true)"
if [ -z "$GUARD_ROOT_RESOLVED" ]; then
  echo "guard: guard-private not found, set GUARD_ROOT" >&2
  exit 1
fi

cmd=("$GO_BIN" "-C" "$GUARD_ROOT_RESOLVED" "run" "./cmd/guard" "check" "--repo" "$REPO_ROOT" "--config" "$CONFIG_PATH" "--mode" "$MODE")
if [ "$MODE" = "range" ]; then
  if [ -z "$RANGE" ]; then
    echo "guard: range mode requires REV1..REV2" >&2
    exit 1
  fi
  cmd+=("--range" "$RANGE")
fi

GOWORK=off "${cmd[@]}"
