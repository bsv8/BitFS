#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
e2e_root="${E2E_ROOT:-/home/david/Workspaces/bsv8/e2e}"
go_bin="${GO_BIN:-/home/david/.gvm/gos/go1.26.0/bin/go}"
case_name="${1:-gw-edge-invalid}"
expected_detail_file="${2:-gateway_edge_result.json}"

cd "$e2e_root"
"$go_bin" run ./cmd/e2e-runner run "$case_name"

result_dir="$(
  find "$e2e_root/_e2e_result" -maxdepth 2 -type f -name "$expected_detail_file" -printf '%T@ %h\n' \
    | sort -n \
    | tail -n 1 \
    | awk '{print $2}'
)"
if [[ -z "$result_dir" ]]; then
  echo "missing e2e result dir for case: $case_name"
  exit 1
fi

for rel_path in \
  "runner/chain/preflight.json" \
  "runner/chain/postflight.json" \
  "$expected_detail_file"
do
  if [[ ! -f "$result_dir/$rel_path" ]]; then
    echo "missing e2e result file: $result_dir/$rel_path"
    exit 1
  fi
done

echo "e2e runner ok: $case_name"
echo "result dir: $result_dir"
