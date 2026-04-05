#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# 收官固定检查：
# 1. 结算写入口不能再直接调共享写入口。
# 2. 结算写文件里不能再出现手填 SourceType/SourceID。

files=(
  "pkg/clientapp/wallet_accounting.go"
  "pkg/clientapp/db_process_writes.go"
  "pkg/clientapp/db_finance_migration.go"
  "pkg/clientapp/db_init.go"
)

matches="$(rg -n "dbAppendFinBusiness\\(|dbAppendFinProcessEvent\\(" "${files[@]}" || true)"
if [[ -n "$matches" ]] && ! printf '%s\n' "$matches" | awk -F: '$1 != "pkg/clientapp/db_process_writes.go" { bad = 1 } END { exit bad }'; then
  printf '%s\n' "$matches"
  echo "forbidden shared finance write call found"
  exit 1
fi

if rg -n "dbAppendChainPaymentUTXOLinkIfAbsentDB\\(|fact_chain_payment_utxo_links" "pkg/clientapp/wallet_accounting.go" >/dev/null 2>&1; then
  echo "forbidden legacy chain payment utxo link write found"
  exit 1
fi

if rg -n "SourceType:|SourceID:" "${files[@]}" >/dev/null 2>&1; then
  echo "forbidden manual settlement source field found"
  exit 1
fi

echo "settlement cycle guard ok"
