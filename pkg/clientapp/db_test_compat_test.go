package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	broadcastmodule "github.com/bsv8/BFTP/pkg/modules/broadcast"
)

// factChainPaymentPayloadTxHex 兼容旧测试里对 tx_hex/txHex 的取值。
// 设计说明：
// - 只做一层很薄的 JSON 解析；
// - 新旧 key 都支持，但缺失时直接报错，避免再默默吞字段。
func factChainPaymentPayloadTxHex(payloadJSON string) (string, error) {
	payloadJSON = strings.TrimSpace(payloadJSON)
	if payloadJSON == "" {
		return "", fmt.Errorf("missing tx_hex")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payloadJSON), &raw); err != nil {
		return "", fmt.Errorf("parse payload json: %w", err)
	}
	for _, key := range []string{"tx_hex", "txHex"} {
		v, ok := raw[key]
		if !ok {
			continue
		}
		var txHex string
		if err := json.Unmarshal(v, &txHex); err != nil {
			return "", fmt.Errorf("parse tx_hex: %w", err)
		}
		txHex = strings.TrimSpace(txHex)
		if txHex == "" {
			break
		}
		return txHex, nil
	}
	return "", fmt.Errorf("missing tx_hex")
}

// saveNodeReachabilityCache 兼容旧名，直接落到 runtime state 的统一入口。
func saveNodeReachabilityCache(store *clientDB, sourceGatewayPubkeyHex string, ann broadcastmodule.NodeReachabilityAnnouncement) error {
	return dbSaveNodeReachabilityCache(context.Background(), store, sourceGatewayPubkeyHex, ann)
}

// loadCachedNodeReachability 兼容旧名，直接读 runtime state 的统一入口。
func loadCachedNodeReachability(store *clientDB, targetNodePubkeyHex string, nowUnix int64) (broadcastmodule.NodeReachabilityAnnouncement, bool, error) {
	return dbLoadCachedNodeReachability(context.Background(), store, targetNodePubkeyHex, nowUnix)
}

// saveSelfNodeReachabilityState 兼容旧名，直接落到 runtime state 的统一入口。
func saveSelfNodeReachabilityState(store *clientDB, state selfNodeReachabilityState) error {
	return dbSaveSelfNodeReachabilityState(context.Background(), store, state)
}

// loadSelfNodeReachabilityState 兼容旧名，直接读 runtime state 的统一入口。
func loadSelfNodeReachabilityState(store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	return dbLoadSelfNodeReachabilityState(context.Background(), store, nodePubkeyHex)
}
