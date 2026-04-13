#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# 约束：
# 1) client/live 协议 ID 不允许再以字面量写在 BitFS 实现层；
# 2) 运行入口必须通过 bitfs-contract/protoid 引用这些协议常量。

echo "[bitfs-boundary] checking protocol id literals..."
if rg -n --glob '!**/*_test.go' 'protocol\.ID\s*=\s*"/bsv-transfer/(client|live)/' pkg/clientapp; then
  echo "[bitfs-boundary] boundary violated: client/live protocol IDs must live in bitfs-contract/pkg/v1/protoid" >&2
  exit 1
fi

echo "[bitfs-boundary] checking runtime import bridge..."
if ! rg -q 'bitfsprotoid "github.com/bsv8/bitfs-contract/pkg/v1/protoid"' pkg/clientapp/run.go; then
  echo "[bitfs-boundary] missing bitfs-contract protoid bridge in run.go" >&2
  exit 1
fi

echo "[bitfs-boundary] checking protocol message ownership..."
if rg -n --glob '!**/*_test.go' '^type (seedGetReq|seedGetResp|directQuoteSubmitReq|directQuoteSubmitResp|directDealAcceptReq|directDealAcceptResp|directTransferPoolOpenReq|directTransferPoolOpenResp|directTransferPoolPayReq|directTransferPoolPayResp|directTransferPoolCloseReq|directTransferPoolCloseResp|liveSegmentDataPB|liveSegmentPB|liveSegmentRefPB|liveSubscribeReq|liveSubscribeResp|liveHeadPushReq|liveHeadPushResp|liveQuoteSegmentPB|liveQuoteSubmitReq|liveQuoteSubmitResp) struct' pkg/clientapp; then
  echo "[bitfs-boundary] boundary violated: direct/live/seed protocol messages must come from bitfs-contract generated types" >&2
  exit 1
fi

if ! rg -q 'bitfsv1 "github.com/bsv8/bitfs-contract/gen/go/v1"' pkg/clientapp/run.go; then
  echo "[bitfs-boundary] missing bitfs-contract generated type bridge in run.go" >&2
  exit 1
fi

echo "[bitfs-boundary] ok"
