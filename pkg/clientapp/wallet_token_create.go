package clientapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	bsvscript "github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

const (
	walletBSV21CreateIconOutputVout   = 0
	walletBSV21CreateDeployOutputVout = 1
	walletBSV21IconContentType        = "application/json"
)

type walletTokenCreatePreviewRequest struct {
	TokenStandard string `json:"token_standard"`
	Symbol        string `json:"symbol"`
	MaxSupply     string `json:"max_supply"`
	Decimals      int    `json:"decimals"`
	Icon          string `json:"icon"`
}

type walletTokenCreateSignRequest struct {
	TokenStandard       string `json:"token_standard"`
	Symbol              string `json:"symbol"`
	MaxSupply           string `json:"max_supply"`
	Decimals            int    `json:"decimals"`
	Icon                string `json:"icon"`
	ExpectedPreviewHash string `json:"expected_preview_hash,omitempty"`
}

type walletTokenCreateInput struct {
	TokenStandard string
	Symbol        string
	MaxSupply     string
	Decimals      int
	Icon          string
}

type preparedWalletTokenCreate struct {
	Preview            walletAssetActionPreview
	SignedTxHex        string
	TxID               string
	TokenID            string
	SelectedFeeUTXOIDs []string
	MinerFeeSatoshi    uint64
	ChangeSatoshi      uint64
}

func (s *httpAPIServer) handleWalletTokenCreatePreview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletTokenCreatePreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenCreatePreview(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenCreateSign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletTokenCreateSignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenCreateSign(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenCreateSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletAssetActionSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenCreateSubmit(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildWalletTokenCreatePreview(r *http.Request, s *httpAPIServer, req walletTokenCreatePreviewRequest) (walletAssetActionPreviewResp, error) {
	input, err := normalizeWalletTokenCreateInput(req.TokenStandard, req.Symbol, req.MaxSupply, req.Decimals, req.Icon)
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	preview, err := httpDBValue(r.Context(), s, func(store *clientDB) (walletAssetActionPreview, error) {
		// 这里运行在 actor 闭包里，禁止再走 actor 入口，避免重入阻塞。
		return previewWalletTokenCreate(store, address, input)
	})
	if err != nil {
		return walletAssetActionPreviewResp{}, err
	}
	if s != nil && s.rt != nil && preview.Feasible {
		prepared, prepareErr := httpDBValue(r.Context(), s, func(store *clientDB) (preparedWalletTokenCreate, error) {
			return prepareWalletTokenCreate(r.Context(), store, s.rt, address, input)
		})
		if prepareErr == nil {
			preview = prepared.Preview
		} else {
			preview.CanSign = false
			preview.WarningLevel = "high"
			preview.DetailLines = append(preview.DetailLines, "状态: 当前预演可行，但真实交易构造失败，暂不能签名。")
			preview.DetailLines = append(preview.DetailLines, "原因: "+prepareErr.Error())
		}
	}
	return walletAssetActionPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: preview,
	}, nil
}

func buildWalletTokenCreateSign(r *http.Request, s *httpAPIServer, req walletTokenCreateSignRequest) (walletAssetActionSignResp, error) {
	if s == nil || s.rt == nil {
		return walletAssetActionSignResp{}, fmt.Errorf("runtime not initialized")
	}
	input, err := normalizeWalletTokenCreateInput(req.TokenStandard, req.Symbol, req.MaxSupply, req.Decimals, req.Icon)
	if err != nil {
		return walletAssetActionSignResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionSignResp{}, err
	}
	prepared, err := httpDBValue(r.Context(), s, func(db *clientDB) (preparedWalletTokenCreate, error) {
		return prepareWalletTokenCreate(r.Context(), db, s.rt, address, input)
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

func buildWalletTokenCreateSubmit(r *http.Request, s *httpAPIServer, req walletAssetActionSubmitRequest) (walletAssetActionSubmitResp, error) {
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
	tokenID, ok := extractBSV21DeployMintTokenIDFromTx(parsed)
	if !ok {
		return walletAssetActionSubmitResp{}, fmt.Errorf("signed_tx_hex is not a bsv21 deploy+mint tx")
	}
	meta := extractBSV21DeployMintStatusMetaFromTx(parsed)
	localTxID := strings.ToLower(strings.TrimSpace(parsed.TxID().String()))
	broadcastTxID, err := s.rt.ActionChain.Broadcast(txHex)
	if err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("broadcast token create failed: %w", err)
	}
	finalTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if finalTxID == "" {
		finalTxID = localTxID
	}
	if err := applyLocalBroadcastWalletTx(r.Context(), s.store, s.rt, txHex, "wallet_token_create_submit"); err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("project token create failed: %w", err)
	}
	addr, err := clientWalletAddress(s.rt)
	if err != nil {
		return walletAssetActionSubmitResp{}, err
	}
	walletID := walletIDByAddress(addr)
	nowUnix := time.Now().Unix()
	factPayload := marshalFactBSV21Payload(map[string]any{
		"source":         "wallet_token_create_submit",
		"token_id":       tokenID,
		"create_txid":    finalTxID,
		"wallet_id":      walletID,
		"address":        strings.TrimSpace(addr),
		"token_standard": "bsv21",
		"symbol":         meta.Symbol,
		"max_supply":     meta.MaxSupply,
		"decimals":       meta.Decimals,
		"icon":           meta.Icon,
	})
	if err := upsertFactBSV21Create(r.Context(), s.store, factBSV21CreateItem{
		TokenID:         tokenID,
		CreateTxID:      finalTxID,
		WalletID:        walletID,
		Address:         strings.TrimSpace(addr),
		TokenStandard:   "bsv21",
		Symbol:          meta.Symbol,
		MaxSupply:       meta.MaxSupply,
		Decimals:        meta.Decimals,
		Icon:            meta.Icon,
		CreatedAtUnix:   nowUnix,
		SubmittedAtUnix: nowUnix,
		UpdatedAtUnix:   nowUnix,
		PayloadJSON:     factPayload,
	}); err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("record token create fact failed: %w", err)
	}
	if err := appendFactBSV21Event(r.Context(), s.store, factBSV21EventItem{
		TokenID:     tokenID,
		EventKind:   "submitted",
		EventAtUnix: nowUnix,
		TxID:        finalTxID,
		Note:        "create tx submitted",
		PayloadJSON: factPayload,
	}); err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("record token create event failed: %w", err)
	}
	return walletAssetActionSubmitResp{
		Ok:      true,
		Code:    "OK",
		Message: "交易已提交，主事实已落库。",
		TxID:    finalTxID,
		TokenID: tokenID,
		Status:  "submitted",
	}, nil
}

func normalizeWalletTokenCreateInput(tokenStandard string, symbol string, maxSupply string, decimals int, icon string) (walletTokenCreateInput, error) {
	standard := normalizeWalletTokenStandard(tokenStandard)
	if standard != "bsv21" {
		return walletTokenCreateInput{}, fmt.Errorf("invalid token standard")
	}
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return walletTokenCreateInput{}, fmt.Errorf("symbol is required")
	}
	if len(symbol) > 32 {
		return walletTokenCreateInput{}, fmt.Errorf("symbol too long")
	}
	maxSupply = normalizePreviewQuantityText(maxSupply)
	parsedSupply, err := parseDecimalText(maxSupply)
	if err != nil || parsedSupply.intValue == nil || parsedSupply.intValue.Sign() <= 0 {
		return walletTokenCreateInput{}, fmt.Errorf("max_supply invalid")
	}
	if parsedSupply.scale != 0 {
		return walletTokenCreateInput{}, fmt.Errorf("max_supply must be an integer")
	}
	if decimals < 0 || decimals > 18 {
		return walletTokenCreateInput{}, fmt.Errorf("decimals invalid")
	}
	icon = strings.ToLower(strings.TrimSpace(icon))
	if icon == "" {
		return walletTokenCreateInput{}, fmt.Errorf("icon is required")
	}
	if !isSeedHashHex(icon) {
		return walletTokenCreateInput{}, fmt.Errorf("icon invalid")
	}
	return walletTokenCreateInput{
		TokenStandard: standard,
		Symbol:        symbol,
		MaxSupply:     maxSupply,
		Decimals:      decimals,
		Icon:          icon,
	}, nil
}

func previewWalletTokenCreate(store *clientDB, address string, input walletTokenCreateInput) (walletAssetActionPreview, error) {
	outputCount, fixedOutputSatoshi := walletBSV21CreateOutputPlan()
	selectedFee, fee, fundingNeed, err := previewPlainBSVFunding(store, address, 0, 0, outputCount, fixedOutputSatoshi)
	if err != nil {
		return walletAssetActionPreview{}, err
	}
	feasible := selectedFee.Feasible
	return walletAssetActionPreview{
		Action:                    "tokens.create",
		Feasible:                  feasible,
		CanSign:                   false,
		Summary:                   buildWalletTokenCreatePreviewSummary(feasible, input.Symbol, input.MaxSupply, fee),
		DetailLines:               buildWalletTokenCreatePreviewLines(address, input, selectedFee.SelectedUTXOIDs, fee),
		WarningLevel:              previewWarningLevel(feasible),
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    fundingNeed,
		SelectedFeeUTXOIDs:        append([]string(nil), selectedFee.SelectedUTXOIDs...),
		Changes: []walletAssetActionPreviewChange{
			{
				OwnerScope:    "network_fee",
				AssetGroup:    "bsv",
				AssetStandard: "native",
				AssetKey:      "bsv",
				AssetSymbol:   "BSV",
				QuantityText:  fmt.Sprintf("%d", fee),
				Direction:     "debit",
				Note:          "miner fee estimate",
			},
		},
	}, nil
}

// prepareWalletTokenCreate 负责把 bsv21 deploy+mint 收口成“可签名交易”。
// 设计说明：
// - 这版只做 deploy+mint，不拆独立 deploy / mint，也不引入第二套钱包接口；
// - `icon` 入参必须提供 seed hash，链上固定写成 `_0 json + _1 deploy+mint`，避免把 bitfs 元数据塞进 token 正文里；
// - submit 之后先刷新 wallet_utxo，本地自己 create 出来的 token 可直接进入后续 send 链路；
// - 外部验真只记录观察进度，不再作为 create / send 的业务前提。
func prepareWalletTokenCreate(ctx context.Context, store *clientDB, rt *Runtime, address string, input walletTokenCreateInput) (preparedWalletTokenCreate, error) {
	if store == nil {
		return preparedWalletTokenCreate{}, fmt.Errorf("db is nil")
	}
	if rt == nil {
		return preparedWalletTokenCreate{}, fmt.Errorf("runtime not initialized")
	}
	outputCount, fixedOutputSatoshi := walletBSV21CreateOutputPlan()
	selectedFee, _, _, err := previewPlainBSVFunding(store, address, 0, 0, outputCount, fixedOutputSatoshi)
	if err != nil {
		return preparedWalletTokenCreate{}, err
	}
	if !selectedFee.Feasible {
		return preparedWalletTokenCreate{}, fmt.Errorf("insufficient plain bsv for miner fee")
	}
	feeUTXOs, err := loadWalletUTXOsByID(store, address, selectedFee.SelectedUTXOIDs)
	if err != nil {
		return preparedWalletTokenCreate{}, err
	}
	txHex, txID, fee, changeSatoshi, err := buildWalletBSV21CreateTx(ctx, store, rt, feeUTXOs, input.Symbol, input.MaxSupply, input.Decimals, input.Icon)
	if err != nil {
		return preparedWalletTokenCreate{}, err
	}
	tokenID := walletTokenCreateTokenIDFromTxID(txID, walletBSV21CreateDeployOutputVout)
	preview := walletAssetActionPreview{
		Action:                    "tokens.create",
		Feasible:                  true,
		CanSign:                   true,
		Summary:                   buildWalletTokenCreatePreparedSummary(input.Symbol, input.MaxSupply, fee),
		DetailLines:               buildWalletTokenCreatePreparedLines(address, input, tokenID, selectedFee.SelectedUTXOIDs, fee, changeSatoshi),
		WarningLevel:              "normal",
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    walletTokenFundingNeed(0, fixedOutputSatoshi, fee),
		SelectedFeeUTXOIDs:        append([]string(nil), selectedFee.SelectedUTXOIDs...),
		TxID:                      txID,
		PreviewHash:               walletOrderPreviewHash(mustDecodeHex(txHex)),
		Changes: []walletAssetActionPreviewChange{
			{
				OwnerScope:    "wallet_self",
				AssetGroup:    walletAssetGroupToken,
				AssetStandard: "bsv21",
				AssetKey:      "bsv21:" + tokenID,
				AssetSymbol:   input.Symbol,
				QuantityText:  input.MaxSupply,
				Direction:     "credit",
				Note:          "deploy+mint output",
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
	if changeSatoshi > 0 {
		preview.Changes = append(preview.Changes, walletAssetActionPreviewChange{
			OwnerScope:    "wallet_self",
			AssetGroup:    "bsv",
			AssetStandard: "native",
			AssetKey:      "bsv",
			AssetSymbol:   "BSV",
			QuantityText:  fmt.Sprintf("%d", changeSatoshi),
			Direction:     "credit",
			Note:          "bsv change",
		})
	}
	return preparedWalletTokenCreate{
		Preview:            preview,
		SignedTxHex:        txHex,
		TxID:               txID,
		TokenID:            tokenID,
		SelectedFeeUTXOIDs: append([]string(nil), selectedFee.SelectedUTXOIDs...),
		MinerFeeSatoshi:    fee,
		ChangeSatoshi:      changeSatoshi,
	}, nil
}

func buildWalletBSV21CreateTx(ctx context.Context, store *clientDB, rt *Runtime, feeUTXOs []poolcore.UTXO, symbol string, maxSupply string, decimals int, icon string) (string, string, uint64, uint64, error) {
	if len(feeUTXOs) == 0 {
		return "", "", 0, 0, fmt.Errorf("selected fee outputs are empty")
	}
	if store == nil {
		return "", "", 0, 0, fmt.Errorf("db is nil")
	}
	if rt == nil {
		return "", "", 0, 0, fmt.Errorf("runtime not initialized")
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return "", "", 0, 0, err
	}
	actor := identity.Actor
	if actor == nil {
		return "", "", 0, 0, fmt.Errorf("runtime not initialized")
	}
	walletAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("parse wallet address: %w", err)
	}
	walletLockScript, err := p2pkh.Lock(walletAddr)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build wallet lock script: %w", err)
	}
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build fee unlock template: %w", err)
	}
	payload, err := buildBSV21DeployMintData(symbol, maxSupply, decimals)
	if err != nil {
		return "", "", 0, 0, err
	}
	iconPayload, err := buildWalletBSV21IconJSONData(icon)
	if err != nil {
		return "", "", 0, 0, err
	}
	if len(iconPayload) == 0 {
		return "", "", 0, 0, fmt.Errorf("icon payload is required")
	}
	txBuilder := txsdk.NewTransaction()
	totalInput := uint64(0)
	walletPrevLockHex := hex.EncodeToString(walletLockScript.Bytes())
	for _, utxo := range feeUTXOs {
		if err := txBuilder.AddInputFrom(utxo.TxID, utxo.Vout, walletPrevLockHex, utxo.Value, unlockTpl); err != nil {
			return "", "", 0, 0, fmt.Errorf("add fee input failed: %w", err)
		}
		totalInput += utxo.Value
	}
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: walletLockScript,
		ContentType:   walletBSV21IconContentType,
		Data:          iconPayload,
	}); err != nil {
		return "", "", 0, 0, fmt.Errorf("build token icon output failed: %w", err)
	}
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: walletLockScript,
		ContentType:   walletTokenContentType,
		Data:          payload,
	}); err != nil {
		return "", "", 0, 0, fmt.Errorf("build token output failed: %w", err)
	}
	requiredOutputs := uint64(2)
	hasChangeOutput := totalInput > requiredOutputs
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      totalInput - requiredOutputs,
			LockingScript: walletLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("pre-sign token create failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	if totalInput <= requiredOutputs+fee {
		return "", "", 0, 0, fmt.Errorf("insufficient total input for miner fee")
	}
	changeSatoshi := totalInput - requiredOutputs - fee
	if hasChangeOutput {
		if changeSatoshi == 0 {
			txBuilder.Outputs = txBuilder.Outputs[:len(txBuilder.Outputs)-1]
		} else {
			txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = changeSatoshi
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("final-sign token create failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return strings.ToLower(strings.TrimSpace(txBuilder.Hex())), txID, fee, changeSatoshi, nil
}

func buildBSV21DeployMintData(symbol string, maxSupply string, decimals int) ([]byte, error) {
	payload := map[string]string{
		"p":   "bsv-20",
		"op":  "deploy+mint",
		"sym": strings.TrimSpace(symbol),
		"amt": strings.TrimSpace(maxSupply),
	}
	if decimals > 0 {
		payload["dec"] = fmt.Sprintf("%d", decimals)
	}
	payload["icon"] = fmt.Sprintf("_%d", walletBSV21CreateIconOutputVout)
	return json.Marshal(payload)
}

func buildWalletBSV21IconJSONData(icon string) ([]byte, error) {
	seedHash := strings.ToLower(strings.TrimSpace(icon))
	if seedHash == "" {
		return nil, fmt.Errorf("icon is required")
	}
	return json.Marshal(map[string]string{
		"p":    "bitfs",
		"type": "hash",
		"hash": seedHash,
	})
}

func walletBSV21CreateOutputPlan() (int, uint64) {
	return 2, 2
}

func walletTokenCreateTokenIDFromTxID(txID string, vout uint32) string {
	return strings.ToLower(strings.TrimSpace(txID)) + "_" + fmt.Sprint(vout)
}

func extractBSV21DeployMintTokenIDFromTx(tx *txsdk.Transaction) (string, bool) {
	if tx == nil || len(tx.Outputs) == 0 {
		return "", false
	}
	txID := strings.ToLower(strings.TrimSpace(tx.TxID().String()))
	if txID == "" {
		return "", false
	}
	for idx, output := range tx.Outputs {
		if output == nil || output.LockingScript == nil {
			continue
		}
		payload, ok := decodeWalletTokenEnvelopePayload(output.LockingScript)
		if !ok {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "op"), "deploy+mint") {
			continue
		}
		return walletTokenCreateTokenIDFromTxID(txID, uint32(idx)), true
	}
	return "", false
}

func buildWalletTokenCreatePreviewSummary(feasible bool, symbol string, maxSupply string, fee uint64) string {
	if feasible {
		return fmt.Sprintf("将创建 BSV21 token %s，总量 %s，预计消耗约 %d sat BSV 链费。", symbol, maxSupply, fee)
	}
	return fmt.Sprintf("无法完成 BSV21 token %s 的创建预演，当前 plain BSV 不足以承担链费。", symbol)
}

func buildWalletTokenCreatePreparedSummary(symbol string, maxSupply string, fee uint64) string {
	return fmt.Sprintf("将创建 BSV21 token %s，总量 %s，并已生成可签名交易，矿工费 %d sat BSV。", symbol, maxSupply, fee)
}

func buildWalletTokenCreatePreviewLines(address string, input walletTokenCreateInput, feeUTXOIDs []string, fee uint64) []string {
	lines := []string{
		fmt.Sprintf("Token 标准: %s", input.TokenStandard),
		fmt.Sprintf("Token 符号: %s", input.Symbol),
		fmt.Sprintf("最大供应量: %s", input.MaxSupply),
		fmt.Sprintf("Decimals: %d", input.Decimals),
		fmt.Sprintf("归属地址: %s", address),
		fmt.Sprintf("已选链费输出: %d 个", len(feeUTXOIDs)),
		fmt.Sprintf("矿工费估算: %d sat BSV", fee),
	}
	lines = append(lines, fmt.Sprintf("Icon Seed Hash: %s", input.Icon))
	lines = append(lines, "Icon 写入: 交易会生成 `_0` 的 bitfs json 输出，deploy+mint 放在 `_1`，icon 字段引用 `_0`。")
	lines = append(lines,
		"说明: 这版固定构造 `_0 json + _1 deploy+mint`，一次铸满，token 输出固定归当前钱包。",
		"说明: submit 成功后会记录一条外部验真状态，但它不是 create / send 的业务前提。",
		"说明: 后续可继续 send；外部观察面只用于报告追踪。",
	)
	return lines
}

func buildWalletTokenCreatePreparedLines(address string, input walletTokenCreateInput, tokenID string, feeUTXOIDs []string, fee uint64, changeSatoshi uint64) []string {
	lines := []string{
		fmt.Sprintf("Token 标准: %s", input.TokenStandard),
		fmt.Sprintf("Token 符号: %s", input.Symbol),
		fmt.Sprintf("最大供应量: %s", input.MaxSupply),
		fmt.Sprintf("Decimals: %d", input.Decimals),
		fmt.Sprintf("归属地址: %s", address),
		fmt.Sprintf("Token ID: %s", tokenID),
		fmt.Sprintf("Asset Key: bsv21:%s", tokenID),
		fmt.Sprintf("已选链费输出: %d 个", len(feeUTXOIDs)),
		fmt.Sprintf("矿工费: %d sat BSV", fee),
	}
	lines = append(lines, fmt.Sprintf("Icon Seed Hash: %s", input.Icon))
	lines = append(lines, "Icon 写入: 交易会生成 `_0` 的 bitfs json 输出，deploy+mint 放在 `_1`，icon 字段引用 `_0`。")
	if changeSatoshi > 0 {
		lines = append(lines, fmt.Sprintf("BSV 找零回钱包: %d sat", changeSatoshi))
	}
	lines = append(lines,
		"状态: 已生成可签名预览，sign 时必须回传 expected_preview_hash。",
		"说明: submit 成功后会记录一条外部验真状态；它不阻塞后续 send。",
	)
	return lines
}
