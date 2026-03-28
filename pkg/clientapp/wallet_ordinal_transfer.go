package clientapp

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

type walletOrdinalTransferSignRequest struct {
	UTXOID              string `json:"utxo_id"`
	AssetKey            string `json:"asset_key"`
	ToAddress           string `json:"to_address"`
	ExpectedPreviewHash string `json:"expected_preview_hash,omitempty"`
}

type walletAssetActionSignResp struct {
	Ok          bool                     `json:"ok"`
	Code        string                   `json:"code"`
	Message     string                   `json:"message,omitempty"`
	Preview     walletAssetActionPreview `json:"preview"`
	SignedTxHex string                   `json:"signed_tx_hex,omitempty"`
	TxID        string                   `json:"txid,omitempty"`
}

type walletAssetActionSubmitRequest struct {
	SignedTxHex string `json:"signed_tx_hex"`
}

type walletAssetActionSubmitResp struct {
	Ok      bool   `json:"ok"`
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
	TxID    string `json:"txid,omitempty"`
}

type preparedWalletOrdinalTransfer struct {
	Preview            walletAssetActionPreview
	SignedTxHex        string
	TxID               string
	SelectedFeeUTXOIDs []string
	MinerFeeSatoshi    uint64
	ChangeSatoshi      uint64
	Ordinal            walletOrdinalItem
	ToAddress          string
}

func (s *httpAPIServer) handleWalletOrdinalTransferSign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletOrdinalTransferSignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletOrdinalTransferSign(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletOrdinalTransferSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletAssetActionSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletOrdinalTransferSubmit(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildWalletOrdinalTransferSign(r *http.Request, s *httpAPIServer, req walletOrdinalTransferSignRequest) (walletAssetActionSignResp, error) {
	if s == nil || s.rt == nil {
		return walletAssetActionSignResp{}, fmt.Errorf("runtime not initialized")
	}
	toAddress := strings.TrimSpace(req.ToAddress)
	if err := validatePreviewAddress(toAddress); err != nil {
		return walletAssetActionSignResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionSignResp{}, err
	}
	prepared, err := httpDBValue(r.Context(), s, func(db *sql.DB) (preparedWalletOrdinalTransfer, error) {
		return prepareWalletOrdinalTransfer(db, s.rt, address, strings.TrimSpace(req.UTXOID), strings.TrimSpace(req.AssetKey), toAddress)
	})
	if err != nil {
		return walletAssetActionSignResp{}, err
	}
	expected := strings.ToLower(strings.TrimSpace(req.ExpectedPreviewHash))
	if expected == "" {
		return walletAssetActionSignResp{
			Ok:      false,
			Code:    "PREVIEW_REQUIRED",
			Message: "expected preview hash is required",
			Preview: prepared.Preview,
		}, nil
	}
	if !strings.EqualFold(expected, prepared.Preview.PreviewHash) {
		return walletAssetActionSignResp{
			Ok:      false,
			Code:    "PREVIEW_CHANGED",
			Message: "wallet preview changed; retry",
			Preview: prepared.Preview,
		}, nil
	}
	return walletAssetActionSignResp{
		Ok:          true,
		Code:        "OK",
		Message:     "",
		Preview:     prepared.Preview,
		SignedTxHex: prepared.SignedTxHex,
		TxID:        prepared.TxID,
	}, nil
}

func buildWalletOrdinalTransferSubmit(r *http.Request, s *httpAPIServer, req walletAssetActionSubmitRequest) (walletAssetActionSubmitResp, error) {
	if s == nil || s.rt == nil || s.rt.ActionChain == nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("runtime not initialized")
	}
	txHex := strings.ToLower(strings.TrimSpace(req.SignedTxHex))
	if txHex == "" {
		return walletAssetActionSubmitResp{}, fmt.Errorf("signed_tx_hex is required")
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("signed_tx_hex invalid: %w", err)
	}
	localTxID := strings.ToLower(strings.TrimSpace(parsed.TxID().String()))
	broadcastTxID, err := s.rt.ActionChain.Broadcast(txHex)
	if err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("broadcast ordinal transfer failed: %w", err)
	}
	finalTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if finalTxID == "" {
		finalTxID = localTxID
	}
	if err := applyLocalBroadcastWalletTx(s.rt, txHex, "wallet_ordinal_transfer_submit"); err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("project ordinal transfer failed: %w", err)
	}
	return walletAssetActionSubmitResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		TxID:    finalTxID,
	}, nil
}

// prepareWalletOrdinalTransfer 负责把 ordinal 转移从“输出选择”推进到真实交易。
// 设计说明：
// - 当前版本只支持“本钱包持有的 p2pkh ordinal 输出 + plain_bsv 链费输入”；
// - 这样可以先把可签名预览、preview_hash、submit 主线打通，后续再扩展更复杂脚本；
// - 真实签名前必须重建同一笔交易，并让调用方提交 expected_preview_hash，避免页面拿旧预览直接签。
func prepareWalletOrdinalTransfer(db *sql.DB, rt *Runtime, address string, utxoID string, assetKey string, toAddress string) (preparedWalletOrdinalTransfer, error) {
	if db == nil {
		return preparedWalletOrdinalTransfer{}, fmt.Errorf("db is nil")
	}
	if rt == nil {
		return preparedWalletOrdinalTransfer{}, fmt.Errorf("runtime not initialized")
	}
	item, err := loadWalletOrdinalDetail(db, address, strings.TrimSpace(utxoID), strings.TrimSpace(assetKey))
	if err != nil {
		return preparedWalletOrdinalTransfer{}, err
	}
	feeSelection, _, _, err := previewPlainBSVFeeFunding(db, address, item.ValueSatoshi, 1, 1)
	if err != nil {
		return preparedWalletOrdinalTransfer{}, err
	}
	if !feeSelection.Feasible {
		return preparedWalletOrdinalTransfer{}, fmt.Errorf("insufficient plain bsv for ordinal transfer fee")
	}
	feeUTXOs, err := loadWalletUTXOsByID(db, address, feeSelection.SelectedUTXOIDs)
	if err != nil {
		return preparedWalletOrdinalTransfer{}, err
	}
	txHex, txID, fee, change, err := buildWalletOrdinalTransferTx(rt, item, feeUTXOs, toAddress)
	if err != nil {
		return preparedWalletOrdinalTransfer{}, err
	}
	preview := walletAssetActionPreview{
		Action:                    "ordinals.transfer",
		Feasible:                  true,
		CanSign:                   true,
		Summary:                   buildWalletOrdinalTransferPreparedSummary(item.AssetKey, fee),
		DetailLines:               buildWalletOrdinalTransferPreparedLines(item, toAddress, feeSelection.SelectedUTXOIDs, fee, change),
		WarningLevel:              "normal",
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    walletOrdinalTransferFundingNeed(item.ValueSatoshi, fee),
		SelectedAssetUTXOIDs:      []string{item.UTXOID},
		SelectedFeeUTXOIDs:        append([]string(nil), feeSelection.SelectedUTXOIDs...),
		TxID:                      txID,
		PreviewHash:               walletBusinessPreviewHash(mustDecodeHex(txHex)),
		Changes: []walletAssetActionPreviewChange{
			{
				OwnerScope:    "wallet_self",
				AssetGroup:    walletAssetGroupOrdinal,
				AssetStandard: item.AssetStandard,
				AssetKey:      item.AssetKey,
				AssetSymbol:   item.AssetSymbol,
				QuantityText:  "1",
				Direction:     "debit",
				Note:          "ordinal transfer request",
			},
			{
				OwnerScope:    "receiver",
				AssetGroup:    walletAssetGroupOrdinal,
				AssetStandard: item.AssetStandard,
				AssetKey:      item.AssetKey,
				AssetSymbol:   item.AssetSymbol,
				QuantityText:  "1",
				Direction:     "credit",
				Note:          toAddress,
			},
			{
				OwnerScope:    "network_fee",
				AssetGroup:    "bsv",
				AssetStandard: "native",
				AssetKey:      "bsv",
				AssetSymbol:   "BSV",
				QuantityText:  fmt.Sprintf("%d", fee),
				Direction:     "debit",
				Note:          "miner fee",
			},
		},
	}
	if change > 0 {
		preview.Changes = append(preview.Changes, walletAssetActionPreviewChange{
			OwnerScope:    "wallet_self",
			AssetGroup:    "bsv",
			AssetStandard: "native",
			AssetKey:      "bsv",
			AssetSymbol:   "BSV",
			QuantityText:  fmt.Sprintf("%d", change),
			Direction:     "credit",
			Note:          "bsv change",
		})
	}
	return preparedWalletOrdinalTransfer{
		Preview:            preview,
		SignedTxHex:        txHex,
		TxID:               txID,
		SelectedFeeUTXOIDs: append([]string(nil), feeSelection.SelectedUTXOIDs...),
		MinerFeeSatoshi:    fee,
		ChangeSatoshi:      change,
		Ordinal:            item,
		ToAddress:          toAddress,
	}, nil
}

func buildWalletOrdinalTransferTx(rt *Runtime, item walletOrdinalItem, feeUTXOs []poolcore.UTXO, toAddress string) (string, string, uint64, uint64, error) {
	if rt == nil {
		return "", "", 0, 0, fmt.Errorf("runtime not initialized")
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return "", "", 0, 0, err
	}
	walletAddr, err := script.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("parse wallet address: %w", err)
	}
	walletLockScript, err := p2pkh.Lock(walletAddr)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build wallet lock script: %w", err)
	}
	recipientAddr, err := script.NewAddressFromString(strings.TrimSpace(toAddress))
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("parse recipient address: %w", err)
	}
	recipientLockScript, err := p2pkh.Lock(recipientAddr)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build recipient lock script: %w", err)
	}
	prevLockHex := hex.EncodeToString(walletLockScript.Bytes())
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build ordinal unlock template: %w", err)
	}
	txBuilder := txsdk.NewTransaction()
	if err := txBuilder.AddInputFrom(item.TxID, item.Vout, prevLockHex, item.ValueSatoshi, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("add ordinal input failed: %w", err)
	}
	totalInput := item.ValueSatoshi
	for _, utxo := range feeUTXOs {
		if err := txBuilder.AddInputFrom(utxo.TxID, utxo.Vout, prevLockHex, utxo.Value, unlockTpl); err != nil {
			return "", "", 0, 0, fmt.Errorf("add fee input failed: %w", err)
		}
		totalInput += utxo.Value
	}
	txBuilder.AddOutput(&txsdk.TransactionOutput{
		Satoshis:      1,
		LockingScript: recipientLockScript,
	})
	hasChangeOutput := totalInput > 1
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      totalInput - 1,
			LockingScript: walletLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("pre-sign ordinal transfer failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	if totalInput <= 1+fee {
		return "", "", 0, 0, fmt.Errorf("insufficient total input for ordinal transfer fee")
	}
	change := totalInput - 1 - fee
	if hasChangeOutput {
		if change == 0 {
			txBuilder.Outputs = txBuilder.Outputs[:len(txBuilder.Outputs)-1]
		} else {
			txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = change
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("final-sign ordinal transfer failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return strings.ToLower(strings.TrimSpace(txBuilder.Hex())), txID, fee, change, nil
}

func loadWalletUTXOsByID(db *sql.DB, address string, utxoIDs []string) ([]poolcore.UTXO, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if len(utxoIDs) == 0 {
		return []poolcore.UTXO{}, nil
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("wallet address is required")
	}
	walletID := walletIDByAddress(address)
	placeholders := make([]string, 0, len(utxoIDs))
	args := make([]any, 0, len(utxoIDs)+2)
	args = append(args, walletID, address)
	for _, raw := range utxoIDs {
		utxoID := strings.ToLower(strings.TrimSpace(raw))
		if utxoID == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, utxoID)
	}
	if len(placeholders) == 0 {
		return []poolcore.UTXO{}, nil
	}
	rows, err := db.Query(
		`SELECT utxo_id,txid,vout,value_satoshi
		 FROM wallet_utxo
		 WHERE wallet_id=? AND address=? AND state='unspent' AND utxo_id IN (`+strings.Join(placeholders, ",")+`)`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	itemsByID := map[string]poolcore.UTXO{}
	for rows.Next() {
		var utxoID string
		var item poolcore.UTXO
		if err := rows.Scan(&utxoID, &item.TxID, &item.Vout, &item.Value); err != nil {
			return nil, err
		}
		itemsByID[strings.ToLower(strings.TrimSpace(utxoID))] = item
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]poolcore.UTXO, 0, len(utxoIDs))
	for _, raw := range utxoIDs {
		utxoID := strings.ToLower(strings.TrimSpace(raw))
		item, ok := itemsByID[utxoID]
		if !ok {
			return nil, fmt.Errorf("wallet utxo not found: %s", utxoID)
		}
		out = append(out, item)
	}
	return out, nil
}

func buildWalletOrdinalTransferPreparedSummary(assetKey string, fee uint64) string {
	return fmt.Sprintf("将转移 ordinal %s，并已生成可签名交易，预计矿工费 %d sat BSV。", assetKey, fee)
}

func buildWalletOrdinalTransferPreparedLines(item walletOrdinalItem, toAddress string, feeUTXOIDs []string, fee uint64, change uint64) []string {
	lines := []string{
		fmt.Sprintf("Ordinal 标识: %s", item.AssetKey),
		fmt.Sprintf("承载输出: %s", item.UTXOID),
		fmt.Sprintf("接收地址: %s", toAddress),
		fmt.Sprintf("已选链费输出: %d 个", len(feeUTXOIDs)),
		fmt.Sprintf("矿工费: %d sat BSV", fee),
	}
	if change > 0 {
		lines = append(lines, fmt.Sprintf("BSV 找零回钱包: %d sat", change))
	}
	lines = append(lines,
		"状态: 已生成可签名预览，sign 时必须回传 expected_preview_hash。",
		"说明: submit 只负责广播你刚签好的交易，不会重新替页面改交易内容。",
	)
	return lines
}

func walletOrdinalTransferFundingNeed(assetInputSatoshi uint64, fee uint64) uint64 {
	target := uint64(1) + fee
	if assetInputSatoshi >= target {
		return 0
	}
	return target - assetInputSatoshi
}

func mustDecodeHex(raw string) []byte {
	out, err := hex.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return out
}
