package clientapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

type walletBSV21WOCCandidate struct {
	ScriptHash string `json:"scriptHash"`
	Data       struct {
		BSV20 struct {
			ID     string                `json:"id"`
			Symbol string                `json:"sym"`
			Amount walletWOCQuantityText `json:"amt"`
		} `json:"bsv20"`
	} `json:"data"`
	Current struct {
		TxID string `json:"txid"`
	} `json:"current"`
}

type walletBSV21WOCUnspentResp struct {
	Tokens []walletBSV21WOCCandidate `json:"tokens"`
}

type walletUTXOBasicRow struct {
	UTXOID           string
	TxID             string
	Vout             uint32
	ValueSatoshi     uint64
	AllocationClass  string
	AllocationReason string
	CreatedAtUnix    int64
}

type walletWOCQuantityText string

func loadWalletBSV21SpendableCandidates(ctx context.Context, store *clientDB, rt *Runtime, address string, assetKey string) ([]walletTokenPreviewCandidate, error) {
	if store == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if rt == nil || rt.WalletChain == nil {
		return nil, fmt.Errorf("wallet chain not initialized")
	}
	address = strings.TrimSpace(address)
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if address == "" || tokenID == "" {
		return []walletTokenPreviewCandidate{}, nil
	}
	rows, err := listWalletUnspentOneSatRows(store, address)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return []walletTokenPreviewCandidate{}, nil
	}
	localCandidates, err := loadWalletBSV21LocalCandidates(store, address, assetKey, rows)
	if err != nil {
		return nil, err
	}
	localSelected := make(map[string]struct{}, len(localCandidates))
	for _, item := range localCandidates {
		localSelected[item.Item.UTXOID] = struct{}{}
	}
	verifiedIncomingCandidates, err := loadWalletBSV21VerifiedIncomingCandidates(ctx, store, rt, address, assetKey, rows, localSelected)
	if err != nil {
		if len(localCandidates) > 0 {
			return localCandidates, nil
		}
		return nil, err
	}
	out := make([]walletTokenPreviewCandidate, 0, len(localCandidates)+len(verifiedIncomingCandidates))
	out = append(out, verifiedIncomingCandidates...)
	out = append(out, localCandidates...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAtUnix != out[j].CreatedAtUnix {
			return out[i].CreatedAtUnix < out[j].CreatedAtUnix
		}
		return out[i].Item.UTXOID < out[j].Item.UTXOID
	})
	return out, nil
}

// loadWalletBSV21VerifiedIncomingCandidates 只补充“不是本地自广播事实”的外来 token 候选。
// 设计说明：
// - 这里表达的是“外来资产验真”边界，而不是把 WOC 本身写成业务真相；
// - 当前系统只有一个外来验真渠道，所以实现上仍然查询 WOC；
// - 一旦将来验真渠道变化，调用方仍然保持“拿已验真的外来候选”这个领域语义。
func loadWalletBSV21VerifiedIncomingCandidates(ctx context.Context, store *clientDB, rt *Runtime, address string, assetKey string, rows []walletUTXOBasicRow, localSelected map[string]struct{}) ([]walletTokenPreviewCandidate, error) {
	items, err := queryWalletBSV21WOCUnspent(ctx, rt, address)
	if err != nil {
		return nil, err
	}
	rowsByTxID := make(map[string][]walletUTXOBasicRow)
	for _, item := range rows {
		rowsByTxID[item.TxID] = append(rowsByTxID[item.TxID], item)
	}
	txHexCache := make(map[string]string)
	txScriptHashCache := make(map[string]map[string][]uint32)
	selected := make(map[string]struct{})
	out := make([]walletTokenPreviewCandidate, 0, len(items))
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	for _, item := range items {
		if strings.TrimSpace(item.Data.BSV20.ID) != tokenID {
			continue
		}
		txid := strings.ToLower(strings.TrimSpace(item.Current.TxID))
		if txid == "" {
			continue
		}
		localRows := rowsByTxID[txid]
		if len(localRows) == 0 {
			continue
		}
		hashMap, err := loadWalletTxOutputScriptHashMap(ctx, rt, txid, txHexCache, txScriptHashCache)
		if err != nil {
			return nil, err
		}
		targetHash := strings.ToLower(strings.TrimSpace(item.ScriptHash))
		if targetHash == "" {
			continue
		}
		candidateVouts := hashMap[targetHash]
		if len(candidateVouts) == 0 {
			continue
		}
		var matched *walletUTXOBasicRow
		for _, vout := range candidateVouts {
			utxoID := txid + ":" + fmt.Sprint(vout)
			if _, exists := selected[utxoID]; exists {
				continue
			}
			for _, row := range localRows {
				if row.Vout == vout {
					rowCopy := row
					matched = &rowCopy
					break
				}
			}
			if matched != nil {
				break
			}
		}
		if matched == nil {
			continue
		}
		if _, exists := localSelected[matched.UTXOID]; exists {
			continue
		}
		selected[matched.UTXOID] = struct{}{}
		quantity := item.Data.BSV20.Amount.String()
		if quantity == "" {
			continue
		}
		parsed, err := parseDecimalText(quantity)
		if err != nil || parsed.intValue == nil || parsed.intValue.Sign() <= 0 {
			return nil, fmt.Errorf("woc bsv21 amount invalid")
		}
		out = append(out, walletTokenPreviewCandidate{
			Item: walletTokenOutputItem{
				UTXOID:           matched.UTXOID,
				WalletAddress:    address,
				TxID:             matched.TxID,
				Vout:             matched.Vout,
				ValueSatoshi:     matched.ValueSatoshi,
				AllocationClass:  matched.AllocationClass,
				AllocationReason: matched.AllocationReason,
				TokenStandard:    "bsv21",
				AssetKey:         assetKey,
				AssetSymbol:      strings.TrimSpace(item.Data.BSV20.Symbol),
				QuantityText:     quantity,
				SourceName:       "woc",
			},
			CreatedAtUnix: matched.CreatedAtUnix,
			Quantity:      parsed,
		})
	}
	return out, nil
}

func (q *walletWOCQuantityText) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(string(data))
	if value == "" || value == "null" {
		*q = ""
		return nil
	}
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return err
		}
		*q = walletWOCQuantityText(strings.TrimSpace(text))
		return nil
	}
	*q = walletWOCQuantityText(value)
	return nil
}

func (q walletWOCQuantityText) String() string {
	return strings.TrimSpace(string(q))
}

func queryWalletBSV21WOCUnspent(ctx context.Context, rt *Runtime, address string) ([]walletBSV21WOCCandidate, error) {
	if rt == nil || rt.WalletChain == nil {
		return nil, fmt.Errorf("wallet chain not initialized")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(rt.WalletChain.BaseURL()), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("wallet chain base url is empty")
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/token/bsv21/"+url.PathEscape(strings.TrimSpace(address))+"/unspent?limit=200&filterMempool=both",
		nil,
	)
	if err != nil {
		return nil, err
	}
	auth := whatsonchain.AuthConfig{
		Mode:  "bearer",
		Value: strings.TrimSpace(rt.runIn.ExternalAPI.WOC.APIKey),
	}
	if strings.TrimSpace(auth.Value) == "" {
		auth.Mode = ""
	}
	if err := auth.Apply(req); err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusNotFound {
		return []walletBSV21WOCCandidate{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("woc bsv21 unspent http %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var parsed walletBSV21WOCUnspentResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return parsed.Tokens, nil
}

func listWalletUnspentOneSatRows(store *clientDB, address string) ([]walletUTXOBasicRow, error) {
	return dbListWalletUnspentOneSatRows(context.Background(), store, address)
}

// loadWalletBSV21LocalCandidates 只认“当前钱包自己广播出来、且目前仍未花费”的 token 输出。
// 设计说明：
// - create / send 不能再把 WOC 当作业务前提；
// - 因此本地自己构造并成功广播的 token 输出，应该直接进入可继续 send 的候选集；
// - 第三方打进来的 token 不走这里，而是留给外来资产验真边界单独补充。
func loadWalletBSV21LocalCandidates(store *clientDB, address string, assetKey string, rows []walletUTXOBasicRow) ([]walletTokenPreviewCandidate, error) {
	if store == nil {
		return nil, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if address == "" || tokenID == "" || len(rows) == 0 {
		return []walletTokenPreviewCandidate{}, nil
	}
	walletID := walletIDByAddress(address)
	localRows, err := loadWalletLocalBroadcastRows(store, walletID, address)
	if err != nil {
		return nil, err
	}
	if len(localRows) == 0 {
		return []walletTokenPreviewCandidate{}, nil
	}
	rowsByUTXOID := make(map[string]walletUTXOBasicRow, len(rows))
	for _, row := range rows {
		rowsByUTXOID[row.UTXOID] = row
	}
	out := make([]walletTokenPreviewCandidate, 0, len(localRows))
	for _, row := range localRows {
		parsed, err := txsdk.NewTransactionFromHex(row.TxHex)
		if err != nil {
			return nil, fmt.Errorf("parse local broadcast tx %s failed: %w", row.TxID, err)
		}
		txid := strings.ToLower(strings.TrimSpace(parsed.TxID().String()))
		if txid == "" {
			continue
		}
		for idx, output := range parsed.Outputs {
			if output == nil || output.LockingScript == nil {
				continue
			}
			utxoID := txid + ":" + fmt.Sprint(idx)
			liveRow, ok := rowsByUTXOID[utxoID]
			if !ok {
				continue
			}
			payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript)
			if !ok || !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
				continue
			}
			candidateTokenID, quantity, symbol, ok := walletLocalBSV21CandidateFromPayload(txid, uint32(idx), payload)
			if !ok || candidateTokenID != tokenID {
				continue
			}
			parsedQty, err := parseDecimalText(quantity)
			if err != nil || parsedQty.intValue == nil || parsedQty.intValue.Sign() <= 0 {
				return nil, fmt.Errorf("local bsv21 amount invalid")
			}
			out = append(out, walletTokenPreviewCandidate{
				Item: walletTokenOutputItem{
					UTXOID:           liveRow.UTXOID,
					WalletAddress:    address,
					TxID:             liveRow.TxID,
					Vout:             liveRow.Vout,
					ValueSatoshi:     liveRow.ValueSatoshi,
					AllocationClass:  liveRow.AllocationClass,
					AllocationReason: liveRow.AllocationReason,
					TokenStandard:    "bsv21",
					AssetKey:         assetKey,
					AssetSymbol:      symbol,
					QuantityText:     quantity,
					SourceName:       "local",
				},
				CreatedAtUnix: liveRow.CreatedAtUnix,
				Quantity:      parsedQty,
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAtUnix != out[j].CreatedAtUnix {
			return out[i].CreatedAtUnix < out[j].CreatedAtUnix
		}
		return out[i].Item.UTXOID < out[j].Item.UTXOID
	})
	return out, nil
}

func walletLocalBSV21CandidateFromPayload(txid string, vout uint32, payload map[string]any) (string, string, string, bool) {
	op := strings.TrimSpace(firstNonEmptyStringField(payload, "op"))
	if op == "" {
		return "", "", "", false
	}
	amount := strings.TrimSpace(firstNonEmptyStringField(payload, "amt"))
	if amount == "" {
		return "", "", "", false
	}
	switch strings.ToLower(op) {
	case "deploy+mint":
		return walletTokenCreateTokenIDFromTxID(txid, vout), amount, strings.TrimSpace(firstNonEmptyStringField(payload, "sym")), true
	case "transfer":
		tokenID := strings.ToLower(strings.TrimSpace(firstNonEmptyStringField(payload, "id")))
		if tokenID == "" {
			return "", "", "", false
		}
		return tokenID, amount, strings.TrimSpace(firstNonEmptyStringField(payload, "sym")), true
	default:
		return "", "", "", false
	}
}

func loadWalletLocalBroadcastRows(store *clientDB, walletID string, address string) ([]walletLocalBroadcastRow, error) {
	return dbLoadWalletLocalBroadcastRows(context.Background(), store, walletID, address)
}

func loadWalletTxOutputScriptHashMap(ctx context.Context, rt *Runtime, txid string, txHexCache map[string]string, hashCache map[string]map[string][]uint32) (map[string][]uint32, error) {
	if cached, ok := hashCache[txid]; ok {
		return cached, nil
	}
	txHex := txHexCache[txid]
	if txHex == "" {
		raw, err := rt.WalletChain.GetTxHex(ctx, txid)
		if err != nil {
			return nil, fmt.Errorf("load woc source tx hex failed: %w", err)
		}
		txHex = strings.ToLower(strings.TrimSpace(raw))
		txHexCache[txid] = txHex
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return nil, fmt.Errorf("parse woc source tx hex failed: %w", err)
	}
	out := make(map[string][]uint32)
	for idx, output := range parsed.Outputs {
		if output == nil || output.LockingScript == nil {
			continue
		}
		hash := sha256.Sum256(output.LockingScript.Bytes())
		key := strings.ToLower(hex.EncodeToString(hash[:]))
		out[key] = append(out[key], uint32(idx))
	}
	hashCache[txid] = out
	return out, nil
}
