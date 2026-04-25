package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
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

// storeNodeReachabilityCache 走 runtime state 的统一入口，实际写入 gatewayclient 模块。
func storeNodeReachabilityCache(store *clientDB, sourceGatewayPubkeyHex string, ann gatewayclient.NodeReachabilityAnnouncement) error {
	return dbSaveNodeReachabilityCache(context.Background(), store, sourceGatewayPubkeyHex, struct {
		NodePubkeyHex   string
		Multiaddrs      []string
		HeadHeight      uint64
		Seq             uint64
		PublishedAtUnix int64
		ExpiresAtUnix   int64
		Signature       []byte
	}{
		NodePubkeyHex:   ann.NodePubkeyHex,
		Multiaddrs:      ann.Multiaddrs,
		HeadHeight:      ann.HeadHeight,
		Seq:             ann.Seq,
		PublishedAtUnix: ann.PublishedAtUnix,
		ExpiresAtUnix:   ann.ExpiresAtUnix,
		Signature:       ann.Signature,
	})
}

// loadNodeReachabilityCache 走 runtime state 的统一入口，实际读取 gatewayclient 模块。
func loadNodeReachabilityCache(store *clientDB, targetNodePubkeyHex string, nowUnix int64) (gatewayclient.NodeReachabilityAnnouncement, bool, error) {
	result, found, err := dbLoadCachedNodeReachability(context.Background(), store, targetNodePubkeyHex, nowUnix)
	if err != nil || !found {
		return gatewayclient.NodeReachabilityAnnouncement{}, found, err
	}
	return gatewayclient.NodeReachabilityAnnouncement{
		NodePubkeyHex:   result.NodePubkeyHex,
		Multiaddrs:      result.Multiaddrs,
		HeadHeight:      result.HeadHeight,
		Seq:             result.Seq,
		PublishedAtUnix: result.PublishedAtUnix,
		ExpiresAtUnix:   result.ExpiresAtUnix,
		Signature:       result.Signature,
	}, true, nil
}

// storeSelfNodeReachabilityState 走 runtime state 的统一入口，实际写入 gatewayclient 模块。
func storeSelfNodeReachabilityState(store *clientDB, state selfNodeReachabilityState) error {
	return dbSaveSelfNodeReachabilityState(context.Background(), store, state)
}

// loadSelfNodeReachabilityState 走 runtime state 的统一入口，实际读取 gatewayclient 模块。
func loadSelfNodeReachabilityState(store *clientDB, nodePubkeyHex string) (selfNodeReachabilityState, bool, error) {
	return dbLoadSelfNodeReachabilityState(context.Background(), store, nodePubkeyHex)
}
