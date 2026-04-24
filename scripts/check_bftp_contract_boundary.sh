#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "[bftp-boundary] checking forbidden old references..."
if rg -n 'github.com/bsv8/bitfs-contract' . --type go 2>/dev/null | rg -v '_test.go|vendor'; then
  echo "[bftp-boundary] boundary violated: bitfs-contract import is forbidden" >&2
  exit 1
fi

if rg -n 'bitfsv1\b' . --type go 2>/dev/null | rg -v '_test.go|vendor'; then
  echo "[bftp-boundary] boundary violated: bitfsv1 package name is forbidden" >&2
  exit 1
fi

if rg -n 'bitfsprotoid\b' . --type go 2>/dev/null | rg -v '_test.go|vendor'; then
  echo "[bftp-boundary] boundary violated: bitfsprotoid package name is forbidden" >&2
  exit 1
fi

echo "[bftp-boundary] checking required bftp-contract references in clientapp..."
if ! rg -q 'bftpv1 "github.com/bsv8/BFTP-contract/gen/go/v1"' pkg/clientapp/run.go 2>/dev/null; then
  echo "[bftp-boundary] missing BFTP-contract generated types in run.go" >&2
  exit 1
fi

if ! rg -q 'protoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"' pkg/clientapp/run.go 2>/dev/null; then
  echo "[bftp-boundary] missing BFTP-contract protoid in run.go" >&2
  exit 1
fi

echo "[bftp-boundary] ok"