package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	broadcastmodule "github.com/bsv8/BitFS/pkg/clientapp/modules/broadcast"
)

// chainPaymentPayloadTxHex 读取链上支付载荷里的交易 hex。
// 设计说明：
// - 只做一层很薄的 JSON 解析；
// - `tx_hex` 和字段名 `txHex` 都接受，但缺失时直接报错。
func chainPaymentPayloadTxHex(payloadJSON string) (string, error) {
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

// storeNodeReachabilityCache 直接落到 runtime state 的统一入口。
func storeNodeReachabilityCache(store *clientDB, sourceGatewayPubkeyHex string, ann broadcastmodule.NodeReachabilityAnnouncement) error {
	return dbSaveNodeReachabilityCache(context.Background(), store, sourceGatewayPubkeyHex, ann)
}

// loadNodeReachabilityCache 直接读 runtime state 的统一入口。
func loadNodeReachabilityCache(store *clientDB, targetNodePubkeyHex string, nowUnix int64) (broadcastmodule.NodeReachabilityAnnouncement, bool, error) {
	return dbLoadCachedNodeReachability(context.Background(), store, targetNodePubkeyHex, nowUnix)
}

// storeSelfNodeReachabilityState 直接落到 runtime state 的统一入口。
func storeSelfNodeReachabilityState(store *clientDB, state selfNodeReachabilityState) error {
	return dbSaveSelfNodeReachabilityState(context.Background(), store, state)
}

// loadSelfNodeReachabilityState 直接读 runtime state 的统一入口。
func loadSelfNodeReachabilityState(store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	return dbLoadSelfNodeReachabilityState(context.Background(), store, nodePubkeyHex)
}
