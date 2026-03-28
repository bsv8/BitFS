package clientapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

const (
	// ExternalAssetIndexBaseURLEnv 允许上层运行时统一注入“受保护的外部资产索引入口”。
	// 设计说明：
	// - 客户端主域不直连具体厂商域名，只认一个兼容 1sat-stack 语义的 baseURL；
	// - e2e 或桌面壳后续如果提供共享 guard，可以通过这个环境变量把探测流量统一收口；
	// - 这里先只开放 baseURL 覆盖，认证仍由 guard 或配置本身负责。
	ExternalAssetIndexBaseURLEnv = "BITFS_ASSET_INDEX_API_URL"

	walletAssetGroupListing = "listing"
)

type externalAssetAPIDetector struct {
	baseURL     string
	auth        chainbridge.AuthConfig
	sourceName  string
	client      *http.Client
	minInterval time.Duration

	mu            sync.Mutex
	nextRequestAt time.Time
}

type externalAssetTXO struct {
	Outpoint string                     `json:"outpoint"`
	Satoshis uint64                     `json:"satoshis"`
	Data     map[string]json.RawMessage `json:"data"`
}

func buildWalletAssetDetector(in RunInput) (walletUTXOAssetDetector, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(in.ExternalAPI.AssetIndex.BaseURL), "/")
	if baseURL == "" {
		return nil, nil
	}
	auth := chainbridge.AuthConfig{
		Mode:  strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthMode),
		Name:  strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthName),
		Value: strings.TrimSpace(in.ExternalAPI.AssetIndex.AuthValue),
	}
	if err := auth.Validate(); err != nil {
		return nil, fmt.Errorf("asset index auth invalid: %w", err)
	}
	return &externalAssetAPIDetector{
		baseURL:    baseURL,
		auth:       auth,
		sourceName: "onesat-stack",
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
		minInterval: time.Duration(in.ExternalAPI.AssetIndex.MinIntervalMS) * time.Millisecond,
	}, nil
}

func (d *externalAssetAPIDetector) DetectUTXOAssets(ctx context.Context, _ string, utxos []poolcore.UTXO) (map[string]walletUTXOAssetClassification, error) {
	if d == nil {
		return nil, fmt.Errorf("asset detector is nil")
	}
	outpoints := make([]string, 0, len(utxos))
	for _, u := range utxos {
		if u.Value != 1 {
			continue
		}
		txid := strings.ToLower(strings.TrimSpace(u.TxID))
		if txid == "" {
			continue
		}
		outpoints = append(outpoints, txid+"."+strconv.FormatUint(uint64(u.Vout), 10))
	}
	if len(outpoints) == 0 {
		return map[string]walletUTXOAssetClassification{}, nil
	}
	if err := d.waitTurn(ctx); err != nil {
		return nil, err
	}
	body, err := json.Marshal(outpoints)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+"/txo/outpoints", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := d.auth.Apply(req); err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("asset index http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var items []externalAssetTXO
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, err
	}
	out := make(map[string]walletUTXOAssetClassification, len(items))
	for _, item := range items {
		classification, ok := d.classifyTXO(item)
		if !ok {
			continue
		}
		utxoID, ok := normalizeIndexedOutpoint(item.Outpoint)
		if !ok {
			continue
		}
		out[utxoID] = classification
	}
	return out, nil
}

func (d *externalAssetAPIDetector) waitTurn(ctx context.Context) error {
	if d == nil || d.minInterval <= 0 {
		return nil
	}
	d.mu.Lock()
	readyAt := d.nextRequestAt
	now := time.Now()
	if readyAt.Before(now) {
		readyAt = now
	}
	d.nextRequestAt = readyAt.Add(d.minInterval)
	d.mu.Unlock()
	wait := time.Until(readyAt)
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *externalAssetAPIDetector) classifyTXO(item externalAssetTXO) (walletUTXOAssetClassification, bool) {
	if len(item.Data) == 0 {
		return walletUTXOAssetClassification{}, false
	}
	utxoID, ok := normalizeIndexedOutpoint(item.Outpoint)
	if !ok {
		return walletUTXOAssetClassification{}, false
	}
	assets := make([]walletUTXOAssetBinding, 0, 2)
	reason := "indexed by external asset api"
	if payload, ok := decodeExternalAssetPayload(item.Data["ordlock"]); ok {
		reason = "indexed ordlock output"
		assets = append(assets, walletUTXOAssetBinding{
			UTXOID:        utxoID,
			AssetGroup:    walletAssetGroupListing,
			AssetStandard: "ordlock",
			AssetKey:      "ordlock:" + utxoID,
			QuantityText:  "1",
			SourceName:    d.sourceName,
			Payload:       payload,
		})
	}
	if payload, ok := decodeExternalAssetPayload(item.Data["inscription"]); ok {
		reason = "indexed ordinal output"
		origin := firstNonEmptyStringField(payload, "origin", "outpoint", "id")
		if origin == "" {
			origin = utxoID
		}
		assets = append(assets, walletUTXOAssetBinding{
			UTXOID:        utxoID,
			AssetGroup:    walletAssetGroupOrdinal,
			AssetStandard: "ordinal",
			AssetKey:      "ordinal:" + origin,
			AssetSymbol:   firstNonEmptyStringField(payload, "name", "contentType"),
			QuantityText:  "1",
			SourceName:    d.sourceName,
			Payload:       payload,
		})
	}
	if payload, ok := decodeExternalAssetPayload(item.Data["bsv21"]); ok {
		reason = "indexed bsv21 output"
		tokenID := firstNonEmptyStringField(payload, "id", "tokenId", "origin")
		if tokenID == "" {
			tokenID = utxoID
		}
		assets = append(assets, walletUTXOAssetBinding{
			UTXOID:        utxoID,
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: "bsv21",
			AssetKey:      "bsv21:" + tokenID,
			AssetSymbol:   firstNonEmptyStringField(payload, "sym", "symbol"),
			QuantityText:  firstNonEmptyStringField(payload, "amt", "amount"),
			SourceName:    d.sourceName,
			Payload:       payload,
		})
	}
	if payload, ok := decodeExternalAssetPayload(item.Data["bsv20"]); ok {
		reason = "indexed bsv20 output"
		tick := firstNonEmptyStringField(payload, "tick", "ticker", "sym", "symbol")
		if tick == "" {
			tick = utxoID
		}
		assets = append(assets, walletUTXOAssetBinding{
			UTXOID:        utxoID,
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: "bsv20",
			AssetKey:      "bsv20:" + tick,
			AssetSymbol:   tick,
			QuantityText:  firstNonEmptyStringField(payload, "amt", "amount"),
			SourceName:    d.sourceName,
			Payload:       payload,
		})
	}
	if len(assets) == 0 {
		return walletUTXOAssetClassification{}, false
	}
	return walletUTXOAssetClassification{
		AllocationClass:  walletUTXOAllocationProtectedAsset,
		AllocationReason: reason,
		Assets:           assets,
	}, true
}

func decodeExternalAssetPayload(raw json.RawMessage) (map[string]any, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, false
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func normalizeIndexedOutpoint(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", false
	}
	idx := strings.LastIndex(value, ".")
	if idx <= 0 || idx >= len(value)-1 {
		return "", false
	}
	txid := strings.ToLower(strings.TrimSpace(value[:idx]))
	voutText := strings.TrimSpace(value[idx+1:])
	vout, err := strconv.ParseUint(voutText, 10, 32)
	if err != nil {
		return "", false
	}
	return txid + ":" + strconv.FormatUint(vout, 10), true
}

func firstNonEmptyStringField(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		switch x := value.(type) {
		case string:
			if trimmed := strings.TrimSpace(x); trimmed != "" {
				return trimmed
			}
		case json.Number:
			if trimmed := strings.TrimSpace(x.String()); trimmed != "" {
				return trimmed
			}
		case float64:
			return strconv.FormatInt(int64(x), 10)
		}
	}
	return ""
}
