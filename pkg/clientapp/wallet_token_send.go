package clientapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	bsvscript "github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

const walletTokenContentType = "application/bsv-20"

type walletTokenSendSignRequest struct {
	TokenStandard       string `json:"token_standard"`
	AssetKey            string `json:"asset_key"`
	AmountText          string `json:"amount_text"`
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
	TokenID string `json:"token_id,omitempty"`
	Status  string `json:"status,omitempty"`
}

type preparedWalletTokenSend struct {
	Preview            walletAssetActionPreview
	SignedTxHex        string
	TxID               string
	SelectedFeeUTXOIDs []string
	MinerFeeSatoshi    uint64
	ChangeSatoshi      uint64
}

func (s *httpAPIServer) handleWalletTokenSendSign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletTokenSendSignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenSendSign(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *httpAPIServer) handleWalletTokenSendSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	var req walletAssetActionSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	resp, err := buildWalletTokenSendSubmit(r, s, req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func buildWalletTokenSendSign(r *http.Request, s *httpAPIServer, req walletTokenSendSignRequest) (walletAssetActionSignResp, error) {
	if s == nil {
		return walletAssetActionSignResp{}, fmt.Errorf("http api server is nil")
	}
	return TriggerWalletTokenSendSign(r.Context(), httpStore(s), s.rt, WalletTokenSendSignRequest{
		TokenStandard:       req.TokenStandard,
		AssetKey:            req.AssetKey,
		AmountText:          req.AmountText,
		ToAddress:           req.ToAddress,
		ExpectedPreviewHash: req.ExpectedPreviewHash,
	})
}

func buildWalletTokenSendSubmit(r *http.Request, s *httpAPIServer, req walletAssetActionSubmitRequest) (walletAssetActionSubmitResp, error) {
	if s == nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("http api server is nil")
	}
	return TriggerWalletTokenSendSubmit(r.Context(), httpStore(s), s.rt, WalletAssetActionSubmitRequest{
		SignedTxHex: req.SignedTxHex,
	})
}

// recordWalletTokenSendRetryTask 记录 token 发送失败后的重试任务痕迹。
// 设计说明：
// - 广播后事实写失败不能只告警，必须留下能被运维追踪的重试记录；
// - 当前不新增数据库迁移，所以先落到 orchestrator 日志；
// - 这不是“继续成功”的旁路，而是明确的失败后补救痕迹。
func recordWalletTokenSendRetryTask(rt *Runtime, txid string, stage string, cause error, txHex string) {
	if rt == nil {
		return
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	stage = strings.TrimSpace(stage)
	errText := ""
	if cause != nil {
		errText = strings.TrimSpace(cause.Error())
	}
	entry := orchestratorLogEntry{
		EventType:      orchestratorEventTaskRetry,
		Source:         "wallet_token_send",
		AggregateKey:   "tx:" + txid,
		IdempotencyKey: "wallet_token_send_retry:" + txid,
		CommandType:    "wallet.token_send.submit",
		TaskStatus:     "retry_scheduled",
		ErrorMessage:   errText,
		Payload: map[string]any{
			"txid":   txid,
			"stage":  stage,
			"tx_hex": strings.TrimSpace(txHex),
		},
	}
	if rt.orch != nil {
		rt.orch.logEvent(entry)
		return
	}
	if rt.store != nil {
		dbAppendOrchestratorLog(context.Background(), rt.store, entry)
	}
}

// prepareWalletTokenSend 负责把 bsv21 tokens.send 收口成“可签名交易”。
// 设计说明：
// - create / send 都不能把 WOC 当作业务前提；
// - 当前可发送持仓由“两路证据”组成：本地自己广播出来的 token 输出 + 当前唯一外来验真渠道确认过的第三方 token；
// - 这样本地自有链路可继续前进，而外来 token 的验真边界也不会和某个具体实现名绑死。
func prepareWalletTokenSend(ctx context.Context, store *clientDB, rt *Runtime, address string, standard string, assetKey string, amountText string, toAddress string) (preparedWalletTokenSend, error) {
	if store == nil {
		return preparedWalletTokenSend{}, fmt.Errorf("db is nil")
	}
	if rt == nil {
		return preparedWalletTokenSend{}, fmt.Errorf("runtime not initialized")
	}
	requested, err := parseDecimalText(amountText)
	if err != nil || requested.intValue == nil || requested.intValue.Sign() <= 0 {
		return preparedWalletTokenSend{}, fmt.Errorf("amount_text invalid")
	}
	if requested.scale != 0 {
		return preparedWalletTokenSend{}, fmt.Errorf("%s amount_text must be an integer", standard)
	}
	if _, err := requireFreshTipState(ctx, store); err != nil {
		return preparedWalletTokenSend{}, err
	}
	if _, err := requireHealthyUTXOSyncState(ctx, store, rt); err != nil {
		return preparedWalletTokenSend{}, err
	}
	// fact_token_lots 为唯一 token 选源口径
	candidates, err := loadWalletTokenSpendableCandidatesFromFact(ctx, store, rt, address, standard, assetKey)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	if len(candidates) == 0 {
		return preparedWalletTokenSend{}, fmt.Errorf("no token carrier inputs found for token send")
	}
	assetSymbol := ""
	if len(candidates) > 0 {
		assetSymbol = strings.TrimSpace(candidates[0].Item.AssetSymbol)
	}
	if standard != "bsv21" {
		return preparedWalletTokenSend{}, fmt.Errorf("invalid token standard")
	}
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if tokenID == "" {
		return preparedWalletTokenSend{}, fmt.Errorf("bsv21 token id is required")
	}
	selected, selectedTotal, _, selectedAssetSymbol, err := selectWalletTokenPreviewCandidates(candidates, requested)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	if strings.TrimSpace(selectedAssetSymbol) != "" {
		assetSymbol = selectedAssetSymbol
	}
	if compareDecimalText(selectedTotal, amountText) < 0 {
		return preparedWalletTokenSend{}, fmt.Errorf("insufficient token balance")
	}
	changeText, err := subtractDecimalText(selectedTotal, amountText)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	tokenOutputCount := walletTokenOutputCount(changeText)
	selectedFee, _, _, err := previewPlainBSVFunding(store, address, uint64(len(selected)), len(selected), tokenOutputCount, uint64(tokenOutputCount))
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	if !selectedFee.Feasible {
		return preparedWalletTokenSend{}, fmt.Errorf("insufficient plain bsv for miner fee")
	}
	feeUTXOs, err := loadWalletUTXOsByID(store, address, selectedFee.SelectedUTXOIDs)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	txHex, txID, fee, changeSatoshi, err := buildWalletBSV21SendTx(ctx, store, rt, selected, feeUTXOs, assetKey, amountText, changeText, toAddress)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	preview := walletAssetActionPreview{
		Action:                    "tokens.send",
		Feasible:                  true,
		CanSign:                   true,
		Summary:                   buildWalletTokenSendPreparedSummary(amountText, assetSymbol, len(selected), fee),
		DetailLines:               buildWalletTokenSendPreparedLines(standard, assetKey, amountText, assetSymbol, toAddress, len(selected), selectedFee.SelectedUTXOIDs, fee, changeText, changeSatoshi),
		WarningLevel:              "normal",
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    walletTokenFundingNeed(uint64(len(selected)), uint64(tokenOutputCount), fee),
		SelectedAssetUTXOIDs:      collectSelectedTokenUTXOIDs(selected),
		SelectedFeeUTXOIDs:        append([]string(nil), selectedFee.SelectedUTXOIDs...),
		TxID:                      txID,
		PreviewHash:               walletOrderPreviewHash(mustDecodeHex(txHex)),
		Changes: []walletAssetActionPreviewChange{
			{
				OwnerScope:    "wallet_self",
				AssetGroup:    walletAssetGroupToken,
				AssetStandard: standard,
				AssetKey:      assetKey,
				AssetSymbol:   assetSymbol,
				QuantityText:  amountText,
				Direction:     "debit",
				Note:          "token send request",
			},
			{
				OwnerScope:    "receiver",
				AssetGroup:    walletAssetGroupToken,
				AssetStandard: standard,
				AssetKey:      assetKey,
				AssetSymbol:   assetSymbol,
				QuantityText:  amountText,
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
	if isPositiveDecimalText(changeText) {
		preview.Changes = append(preview.Changes, walletAssetActionPreviewChange{
			OwnerScope:    "wallet_self",
			AssetGroup:    walletAssetGroupToken,
			AssetStandard: standard,
			AssetKey:      assetKey,
			AssetSymbol:   assetSymbol,
			QuantityText:  changeText,
			Direction:     "credit",
			Note:          "token change",
		})
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
	return preparedWalletTokenSend{
		Preview:            preview,
		SignedTxHex:        txHex,
		TxID:               txID,
		SelectedFeeUTXOIDs: append([]string(nil), selectedFee.SelectedUTXOIDs...),
		MinerFeeSatoshi:    fee,
		ChangeSatoshi:      changeSatoshi,
	}, nil
}

func buildWalletBSV21SendTx(ctx context.Context, store *clientDB, rt *Runtime, selected []walletTokenPreviewCandidate, feeUTXOs []poolcore.UTXO, assetKey string, amountText string, changeText string, toAddress string) (string, string, uint64, uint64, error) {
	if len(selected) == 0 {
		return "", "", 0, 0, fmt.Errorf("selected token outputs are empty")
	}
	if rt == nil {
		return "", "", 0, 0, fmt.Errorf("runtime not initialized")
	}
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if tokenID == "" {
		return "", "", 0, 0, fmt.Errorf("bsv21 token id is required")
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
	recipientAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(toAddress))
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("parse recipient address: %w", err)
	}
	recipientLockScript, err := p2pkh.Lock(recipientAddr)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build recipient lock script: %w", err)
	}
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("build token unlock template: %w", err)
	}
	txBuilder := txsdk.NewTransaction()
	totalInput := uint64(0)
	for _, item := range selected {
		lockingScriptHex, lockErr := resolveWalletOutputLockingScriptHex(ctx, rt, item.Item.TxID, item.Item.Vout)
		if lockErr != nil {
			return "", "", 0, 0, lockErr
		}
		if err := txBuilder.AddInputFrom(item.Item.TxID, item.Item.Vout, lockingScriptHex, item.Item.ValueSatoshi, unlockTpl); err != nil {
			return "", "", 0, 0, fmt.Errorf("add token input failed: %w", err)
		}
		totalInput += item.Item.ValueSatoshi
	}
	walletPrevLockHex := hex.EncodeToString(walletLockScript.Bytes())
	for _, utxo := range feeUTXOs {
		if err := txBuilder.AddInputFrom(utxo.TxID, utxo.Vout, walletPrevLockHex, utxo.Value, unlockTpl); err != nil {
			return "", "", 0, 0, fmt.Errorf("add fee input failed: %w", err)
		}
		totalInput += utxo.Value
	}
	recipientData, err := buildBSV21TransferData(assetKey, amountText)
	if err != nil {
		return "", "", 0, 0, err
	}
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: recipientLockScript,
		ContentType:   walletTokenContentType,
		Data:          recipientData,
	}); err != nil {
		return "", "", 0, 0, fmt.Errorf("build token recipient output failed: %w", err)
	}
	if isPositiveDecimalText(changeText) {
		changeData, err := buildBSV21TransferData(assetKey, changeText)
		if err != nil {
			return "", "", 0, 0, err
		}
		if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
			LockingScript: walletLockScript,
			ContentType:   walletTokenContentType,
			Data:          changeData,
		}); err != nil {
			return "", "", 0, 0, fmt.Errorf("build token change output failed: %w", err)
		}
	}
	tokenOutputCount := len(txBuilder.Outputs)
	fixedOutputSatoshi := uint64(tokenOutputCount)
	hasChangeOutput := totalInput > fixedOutputSatoshi
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      totalInput - fixedOutputSatoshi,
			LockingScript: walletLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("pre-sign token send failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	if totalInput <= fixedOutputSatoshi+fee {
		return "", "", 0, 0, fmt.Errorf("insufficient total input for miner fee")
	}
	changeSatoshi := totalInput - fixedOutputSatoshi - fee
	if hasChangeOutput {
		if changeSatoshi == 0 {
			txBuilder.Outputs = txBuilder.Outputs[:len(txBuilder.Outputs)-1]
		} else {
			txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = changeSatoshi
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("final-sign token send failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return strings.ToLower(strings.TrimSpace(txBuilder.Hex())), txID, fee, changeSatoshi, nil
}

func resolveWalletOutputLockingScriptHex(ctx context.Context, rt *Runtime, txid string, vout uint32) (string, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return "", fmt.Errorf("txid is required")
	}
	if rt == nil || rt.WalletChain == nil {
		return "", fmt.Errorf("wallet chain not initialized")
	}
	// 设计说明：
	// - 这里只从链上原始交易回读锁定脚本，不再回看本地广播表；
	// - 本地广播表属于 quote/pay 的链支付读域，BSV21 send 不能再碰它；
	// - 若源交易还没上链，这里就直接失败，避免把“未确认本地状态”误当成可签名事实。
	sourceTxHex, err := rt.WalletChain.GetTxHex(ctx, txid)
	if err != nil {
		return "", fmt.Errorf("load source tx hex failed: %w", err)
	}
	parsed, err := txsdk.NewTransactionFromHex(sourceTxHex)
	if err != nil {
		return "", fmt.Errorf("parse source tx hex failed: %w", err)
	}
	return walletOutputLockingScriptHexFromTx(parsed, vout)
}

func walletOutputLockingScriptHexFromTx(tx *txsdk.Transaction, vout uint32) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("source tx is nil")
	}
	if int(vout) >= len(tx.Outputs) {
		return "", fmt.Errorf("source output index out of range")
	}
	if tx.Outputs[vout] == nil || tx.Outputs[vout].LockingScript == nil {
		return "", fmt.Errorf("source output locking script missing")
	}
	return hex.EncodeToString(tx.Outputs[vout].LockingScript.Bytes()), nil
}

func buildBSV21TransferData(assetKey string, amountText string) ([]byte, error) {
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if tokenID == "" {
		return nil, fmt.Errorf("bsv21 token id is required")
	}
	payload := map[string]string{
		"p":   "bsv-20",
		"op":  "transfer",
		"id":  tokenID,
		"amt": strings.TrimSpace(amountText),
	}
	return json.Marshal(payload)
}

func walletBSV21TokenIDFromAssetKey(assetKey string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(assetKey), "bsv21:"))
}

func extractBSV21TokenIDsFromTx(tx *txsdk.Transaction) []string {
	if tx == nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, 1)
	for _, output := range tx.Outputs {
		if output == nil || output.LockingScript == nil {
			continue
		}
		payload, ok := decodeWalletTokenTransferPayload(output.LockingScript)
		if !ok {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
			continue
		}
		if !strings.EqualFold(firstNonEmptyStringField(payload, "op"), "transfer") {
			continue
		}
		tokenID := strings.TrimSpace(firstNonEmptyStringField(payload, "id"))
		if tokenID == "" {
			continue
		}
		if _, exists := seen[tokenID]; exists {
			continue
		}
		seen[tokenID] = struct{}{}
		out = append(out, tokenID)
	}
	return out
}

func decodeWalletTokenEnvelopePayload(lockingScript *bsvscript.Script) (map[string]any, bool) {
	if lockingScript == nil {
		return nil, false
	}
	ops, err := lockingScript.ParseOps()
	if err != nil || len(ops) < 8 {
		return nil, false
	}
	if ops[0].Op != bsvscript.OpFALSE || ops[1].Op != bsvscript.OpIF || ops[3].Op != bsvscript.Op1 || ops[5].Op != bsvscript.Op0 || ops[7].Op != bsvscript.OpENDIF {
		return nil, false
	}
	if string(ops[2].Data) != txsdk.OrdinalsPrefix || string(ops[4].Data) != walletTokenContentType {
		return nil, false
	}
	var payload map[string]any
	if err := json.Unmarshal(ops[6].Data, &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func decodeWalletTokenTransferPayload(lockingScript *bsvscript.Script) (map[string]any, bool) {
	payload, ok := decodeWalletTokenEnvelopePayload(lockingScript)
	if !ok {
		return nil, false
	}
	if !strings.EqualFold(firstNonEmptyStringField(payload, "p"), "bsv-20") {
		return nil, false
	}
	if !strings.EqualFold(firstNonEmptyStringField(payload, "op"), "transfer") {
		return nil, false
	}
	return payload, true
}

func walletTokenOutputCount(changeText string) int {
	count := 1
	if isPositiveDecimalText(changeText) {
		count++
	}
	return count
}

func collectSelectedTokenUTXOIDs(selected []walletTokenPreviewCandidate) []string {
	out := make([]string, 0, len(selected))
	for _, item := range selected {
		out = append(out, item.Item.UTXOID)
	}
	return out
}

func buildWalletTokenSendPreparedSummary(amountText string, assetSymbol string, selectedCount int, fee uint64) string {
	return fmt.Sprintf("将发送 %s %s，并已生成可签名交易，预计选择 %d 个承载输出，矿工费 %d sat BSV。", amountText, firstNonEmptyString(assetSymbol, "token"), selectedCount, fee)
}

func buildWalletTokenSendPreparedLines(standard string, assetKey string, amountText string, assetSymbol string, toAddress string, selectedCount int, feeUTXOIDs []string, fee uint64, changeText string, changeSatoshi uint64) []string {
	lines := []string{
		fmt.Sprintf("Token 标准: %s", standard),
		fmt.Sprintf("资产标识: %s", assetKey),
		fmt.Sprintf("发送数量: %s %s", amountText, firstNonEmptyString(assetSymbol, assetKey)),
		fmt.Sprintf("接收地址: %s", toAddress),
		fmt.Sprintf("已选承载输出: %d 个", selectedCount),
		fmt.Sprintf("已选链费输出: %d 个", len(feeUTXOIDs)),
		fmt.Sprintf("矿工费: %d sat BSV", fee),
	}
	if isPositiveDecimalText(changeText) {
		lines = append(lines, fmt.Sprintf("资产找零: %s %s", changeText, firstNonEmptyString(assetSymbol, assetKey)))
	}
	if changeSatoshi > 0 {
		lines = append(lines, fmt.Sprintf("BSV 找零回钱包: %d sat", changeSatoshi))
	}
	lines = append(lines, "状态: 已生成可签名预览，sign 时必须回传 expected_preview_hash。")
	lines = append(lines, "说明: 当前 bsv21 持仓由“本地可信 + 当前唯一外来验真渠道”两路证据共同组成。")
	return lines
}

func walletBSV21SubmitMessage(tx *txsdk.Transaction) string {
	if len(extractBSV21TokenIDsFromTx(tx)) == 0 {
		return ""
	}
	return "交易已广播，本地钱包已先投影；后续外部观测是否追平，不影响这次 send 已提交。"
}

func walletTokenFundingNeed(assetInputSatoshi uint64, fixedOutputSatoshi uint64, fee uint64) uint64 {
	target := fixedOutputSatoshi + fee
	if assetInputSatoshi >= target {
		return 0
	}
	return target - assetInputSatoshi
}
