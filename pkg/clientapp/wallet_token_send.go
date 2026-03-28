package clientapp

import (
	"context"
	"database/sql"
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

type preparedWalletTokenSend struct {
	Preview            walletAssetActionPreview
	SignedTxHex        string
	TxID               string
	SelectedFeeUTXOIDs []string
	MinerFeeSatoshi    uint64
	OverlayFeeSatoshi  uint64
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
	if s == nil || s.rt == nil {
		return walletAssetActionSignResp{}, fmt.Errorf("runtime not initialized")
	}
	standard := normalizeWalletTokenStandard(req.TokenStandard)
	if standard == "" {
		return walletAssetActionSignResp{}, fmt.Errorf("invalid token standard")
	}
	if err := validatePreviewAddress(strings.TrimSpace(req.ToAddress)); err != nil {
		return walletAssetActionSignResp{}, err
	}
	address, err := resolveWalletAddressForHTTP(r.Context(), s)
	if err != nil {
		return walletAssetActionSignResp{}, err
	}
	prepared, err := httpDBValue(r.Context(), s, func(db *sql.DB) (preparedWalletTokenSend, error) {
		return prepareWalletTokenSend(r.Context(), db, s.rt, address, standard, strings.TrimSpace(req.AssetKey), normalizePreviewQuantityText(req.AmountText), strings.TrimSpace(req.ToAddress))
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

func buildWalletTokenSendSubmit(r *http.Request, s *httpAPIServer, req walletAssetActionSubmitRequest) (walletAssetActionSubmitResp, error) {
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
		return walletAssetActionSubmitResp{}, fmt.Errorf("broadcast token send failed: %w", err)
	}
	finalTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if finalTxID == "" {
		finalTxID = localTxID
	}
	var overlayErr error
	if tokenIDs := extractBSV21TokenIDsFromTx(parsed); len(tokenIDs) > 0 {
		provider := resolveWalletBSV21Provider(s.rt)
		if provider == nil {
			overlayErr = fmt.Errorf("bsv21 provider not configured")
		} else {
			for _, tokenID := range tokenIDs {
				if err := provider.SubmitTx(r.Context(), tokenID, parsed); err != nil {
					overlayErr = err
					break
				}
			}
		}
	}
	if err := applyLocalBroadcastWalletTx(s.rt, txHex, "wallet_token_send_submit"); err != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("project token send failed: %w", err)
	}
	if overlayErr != nil {
		return walletAssetActionSubmitResp{}, fmt.Errorf("overlay submit failed after broadcast: %w", overlayErr)
	}
	return walletAssetActionSubmitResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		TxID:    finalTxID,
	}, nil
}

// prepareWalletTokenSend 负责把 tokens.send 统一收口成“可签名交易”。
// 设计说明：
// - bsv20 / bsv21 继续共用 wallet.tokens.send 接口面，不分叉第二套命令；
// - 查询面仍只从 wallet_utxo / wallet_utxo_assets 选候选输出；
// - 但 bsv21 会在真实构造前补齐 token details、overlay validate、overlay fee 三步，避免把本地投影误当最终事实。
func prepareWalletTokenSend(ctx context.Context, db *sql.DB, rt *Runtime, address string, standard string, assetKey string, amountText string, toAddress string) (preparedWalletTokenSend, error) {
	if db == nil {
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
	candidates, err := loadWalletTokenPreviewCandidates(db, address, standard, assetKey)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	assetSymbol := ""
	if len(candidates) > 0 {
		assetSymbol = strings.TrimSpace(candidates[0].Item.AssetSymbol)
	}
	var bsv21Details walletBSV21TokenDetails
	var overlayFee uint64
	switch standard {
	case "bsv20":
	case "bsv21":
		provider := resolveWalletBSV21Provider(rt)
		if provider == nil {
			return preparedWalletTokenSend{}, fmt.Errorf("bsv21 provider not configured")
		}
		tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
		if tokenID == "" {
			return preparedWalletTokenSend{}, fmt.Errorf("bsv21 token id is required")
		}
		details, err := provider.GetTokenDetails(ctx, tokenID)
		if err != nil {
			return preparedWalletTokenSend{}, fmt.Errorf("load bsv21 token details failed: %w", err)
		}
		bsv21Details = details
		if !details.Status.IsActive {
			return preparedWalletTokenSend{}, fmt.Errorf("bsv21 token is not active")
		}
		if strings.TrimSpace(details.Status.FeeAddress) == "" {
			return preparedWalletTokenSend{}, fmt.Errorf("bsv21 fee address is missing")
		}
		if sym := strings.TrimSpace(details.Token.Symbol); sym != "" {
			assetSymbol = sym
		}
		validated, err := provider.ValidateOutputs(ctx, tokenID, collectSelectedTokenUTXOIDs(candidates))
		if err != nil {
			return preparedWalletTokenSend{}, fmt.Errorf("validate bsv21 outputs failed: %w", err)
		}
		validOutpoints := make(map[string]struct{}, len(validated))
		for _, item := range validated {
			if normalized, ok := normalizeWalletOverlayOutpoint(item.Outpoint); ok {
				validOutpoints[normalized] = struct{}{}
			}
		}
		filtered := make([]walletTokenPreviewCandidate, 0, len(candidates))
		for _, item := range candidates {
			if _, ok := validOutpoints[item.Item.UTXOID]; ok {
				filtered = append(filtered, item)
			}
		}
		candidates = filtered
	default:
		return preparedWalletTokenSend{}, fmt.Errorf("invalid token standard")
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
	if standard == "bsv21" {
		overlayFee = bsv21Details.Status.FeePerOutput * uint64(tokenOutputCount)
	}
	selectedFee, _, _, err := previewPlainBSVFunding(db, address, uint64(len(selected)), len(selected), tokenOutputCount+boolToInt(overlayFee > 0), uint64(tokenOutputCount)+overlayFee)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	if !selectedFee.Feasible {
		return preparedWalletTokenSend{}, fmt.Errorf("insufficient plain bsv for token send fee")
	}
	feeUTXOs, err := loadWalletUTXOsByID(db, address, selectedFee.SelectedUTXOIDs)
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	var txHex string
	var txID string
	var fee uint64
	var changeSatoshi uint64
	switch standard {
	case "bsv20":
		txHex, txID, fee, changeSatoshi, err = buildWalletBSV20SendTx(ctx, db, rt, selected, feeUTXOs, assetKey, amountText, changeText, toAddress)
	case "bsv21":
		txHex, txID, fee, changeSatoshi, overlayFee, err = buildWalletBSV21SendTx(ctx, db, rt, selected, feeUTXOs, assetKey, amountText, changeText, toAddress)
	default:
		err = fmt.Errorf("invalid token standard")
	}
	if err != nil {
		return preparedWalletTokenSend{}, err
	}
	preview := walletAssetActionPreview{
		Action:                    "tokens.send",
		Feasible:                  true,
		CanSign:                   true,
		Summary:                   buildWalletTokenSendPreparedSummary(amountText, assetSymbol, len(selected), fee),
		DetailLines:               buildWalletTokenSendPreparedLines(standard, assetKey, amountText, assetSymbol, toAddress, len(selected), selectedFee.SelectedUTXOIDs, fee, overlayFee, changeText, changeSatoshi),
		WarningLevel:              "normal",
		EstimatedNetworkFeeBSVSat: fee,
		FeeFundingTargetBSVSat:    walletTokenFundingNeed(uint64(len(selected)), uint64(tokenOutputCount)+overlayFee, fee),
		SelectedAssetUTXOIDs:      collectSelectedTokenUTXOIDs(selected),
		SelectedFeeUTXOIDs:        append([]string(nil), selectedFee.SelectedUTXOIDs...),
		TxID:                      txID,
		PreviewHash:               walletBusinessPreviewHash(mustDecodeHex(txHex)),
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
	if overlayFee > 0 {
		preview.Changes = append(preview.Changes, walletAssetActionPreviewChange{
			OwnerScope:    "network_fee",
			AssetGroup:    "bsv",
			AssetStandard: "native",
			AssetKey:      "bsv",
			AssetSymbol:   "BSV",
			QuantityText:  fmt.Sprintf("%d", overlayFee),
			Direction:     "debit",
			Note:          "overlay fee",
		})
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
		OverlayFeeSatoshi:  overlayFee,
		ChangeSatoshi:      changeSatoshi,
	}, nil
}

func buildWalletBSV20SendTx(ctx context.Context, db *sql.DB, rt *Runtime, selected []walletTokenPreviewCandidate, feeUTXOs []poolcore.UTXO, assetKey string, amountText string, changeText string, toAddress string) (string, string, uint64, uint64, error) {
	if len(selected) == 0 {
		return "", "", 0, 0, fmt.Errorf("selected token outputs are empty")
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return "", "", 0, 0, err
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
		lockingScriptHex, lockErr := resolveWalletOutputLockingScriptHex(ctx, db, rt, item.Item.TxID, item.Item.Vout)
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
	recipientData, err := buildBSV20TransferData(assetKey, amountText)
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
		changeData, err := buildBSV20TransferData(assetKey, changeText)
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
	hasChangeOutput := totalInput > uint64(tokenOutputCount)
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      totalInput - uint64(tokenOutputCount),
			LockingScript: walletLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, fmt.Errorf("pre-sign token send failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	if totalInput <= uint64(tokenOutputCount)+fee {
		return "", "", 0, 0, fmt.Errorf("insufficient total input for token send fee")
	}
	changeSatoshi := totalInput - uint64(tokenOutputCount) - fee
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

func buildWalletBSV21SendTx(ctx context.Context, db *sql.DB, rt *Runtime, selected []walletTokenPreviewCandidate, feeUTXOs []poolcore.UTXO, assetKey string, amountText string, changeText string, toAddress string) (string, string, uint64, uint64, uint64, error) {
	if len(selected) == 0 {
		return "", "", 0, 0, 0, fmt.Errorf("selected token outputs are empty")
	}
	if rt == nil {
		return "", "", 0, 0, 0, fmt.Errorf("runtime not initialized")
	}
	provider := resolveWalletBSV21Provider(rt)
	if provider == nil {
		return "", "", 0, 0, 0, fmt.Errorf("bsv21 provider not configured")
	}
	tokenID := walletBSV21TokenIDFromAssetKey(assetKey)
	if tokenID == "" {
		return "", "", 0, 0, 0, fmt.Errorf("bsv21 token id is required")
	}
	details, err := provider.GetTokenDetails(ctx, tokenID)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("load bsv21 token details failed: %w", err)
	}
	if !details.Status.IsActive {
		return "", "", 0, 0, 0, fmt.Errorf("bsv21 token is not active")
	}
	if strings.TrimSpace(details.Status.FeeAddress) == "" {
		return "", "", 0, 0, 0, fmt.Errorf("bsv21 fee address is missing")
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return "", "", 0, 0, 0, err
	}
	walletAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("parse wallet address: %w", err)
	}
	walletLockScript, err := p2pkh.Lock(walletAddr)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("build wallet lock script: %w", err)
	}
	recipientAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(toAddress))
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("parse recipient address: %w", err)
	}
	recipientLockScript, err := p2pkh.Lock(recipientAddr)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("build recipient lock script: %w", err)
	}
	feeAddr, err := bsvscript.NewAddressFromString(strings.TrimSpace(details.Status.FeeAddress))
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("parse bsv21 fee address: %w", err)
	}
	feeLockScript, err := p2pkh.Lock(feeAddr)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("build bsv21 fee lock script: %w", err)
	}
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("build token unlock template: %w", err)
	}
	txBuilder := txsdk.NewTransaction()
	totalInput := uint64(0)
	for _, item := range selected {
		lockingScriptHex, lockErr := resolveWalletOutputLockingScriptHex(ctx, db, rt, item.Item.TxID, item.Item.Vout)
		if lockErr != nil {
			return "", "", 0, 0, 0, lockErr
		}
		if err := txBuilder.AddInputFrom(item.Item.TxID, item.Item.Vout, lockingScriptHex, item.Item.ValueSatoshi, unlockTpl); err != nil {
			return "", "", 0, 0, 0, fmt.Errorf("add token input failed: %w", err)
		}
		totalInput += item.Item.ValueSatoshi
	}
	walletPrevLockHex := hex.EncodeToString(walletLockScript.Bytes())
	for _, utxo := range feeUTXOs {
		if err := txBuilder.AddInputFrom(utxo.TxID, utxo.Vout, walletPrevLockHex, utxo.Value, unlockTpl); err != nil {
			return "", "", 0, 0, 0, fmt.Errorf("add fee input failed: %w", err)
		}
		totalInput += utxo.Value
	}
	recipientData, err := buildBSV21TransferData(assetKey, amountText)
	if err != nil {
		return "", "", 0, 0, 0, err
	}
	if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
		LockingScript: recipientLockScript,
		ContentType:   walletTokenContentType,
		Data:          recipientData,
	}); err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("build token recipient output failed: %w", err)
	}
	if isPositiveDecimalText(changeText) {
		changeData, err := buildBSV21TransferData(assetKey, changeText)
		if err != nil {
			return "", "", 0, 0, 0, err
		}
		if err := txBuilder.Inscribe(&bsvscript.InscriptionArgs{
			LockingScript: walletLockScript,
			ContentType:   walletTokenContentType,
			Data:          changeData,
		}); err != nil {
			return "", "", 0, 0, 0, fmt.Errorf("build token change output failed: %w", err)
		}
	}
	tokenOutputCount := len(txBuilder.Outputs)
	overlayFee := details.Status.FeePerOutput * uint64(tokenOutputCount)
	if overlayFee > 0 {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      overlayFee,
			LockingScript: feeLockScript,
		})
	}
	fixedOutputSatoshi := uint64(tokenOutputCount) + overlayFee
	hasChangeOutput := totalInput > fixedOutputSatoshi
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      totalInput - fixedOutputSatoshi,
			LockingScript: walletLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("pre-sign token send failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	if totalInput <= fixedOutputSatoshi+fee {
		return "", "", 0, 0, 0, fmt.Errorf("insufficient total input for token send fee")
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
		return "", "", 0, 0, 0, fmt.Errorf("final-sign token send failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return strings.ToLower(strings.TrimSpace(txBuilder.Hex())), txID, fee, changeSatoshi, overlayFee, nil
}

func resolveWalletOutputLockingScriptHex(ctx context.Context, db *sql.DB, rt *Runtime, txid string, vout uint32) (string, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return "", fmt.Errorf("txid is required")
	}
	localHex, err := loadWalletLocalBroadcastHex(db, txid)
	if err != nil {
		return "", err
	}
	if localHex != "" {
		parsed, err := txsdk.NewTransactionFromHex(localHex)
		if err != nil {
			return "", fmt.Errorf("parse local broadcast tx failed: %w", err)
		}
		if int(vout) >= len(parsed.Outputs) {
			return "", fmt.Errorf("source output index out of range")
		}
		if parsed.Outputs[vout] == nil || parsed.Outputs[vout].LockingScript == nil {
			return "", fmt.Errorf("source output locking script missing")
		}
		return hex.EncodeToString(parsed.Outputs[vout].LockingScript.Bytes()), nil
	}
	if rt == nil || rt.WalletChain == nil {
		return "", fmt.Errorf("wallet chain not initialized")
	}
	detail, err := rt.WalletChain.GetTxHash(ctx, txid)
	if err != nil {
		return "", fmt.Errorf("load source tx failed: %w", err)
	}
	for _, out := range detail.Vout {
		if out.N == vout {
			lockingScriptHex := strings.ToLower(strings.TrimSpace(out.ScriptPubKey.Hex))
			if lockingScriptHex == "" {
				return "", fmt.Errorf("source output locking script missing")
			}
			return lockingScriptHex, nil
		}
	}
	return "", fmt.Errorf("source output not found")
}

func loadWalletLocalBroadcastHex(db *sql.DB, txid string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	var txHex string
	err := db.QueryRow(`SELECT tx_hex FROM wallet_local_broadcast_txs WHERE txid=?`, txid).Scan(&txHex)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(txHex)), nil
}

func buildBSV20TransferData(assetKey string, amountText string) ([]byte, error) {
	tick := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(assetKey), "bsv20:"))
	if tick == "" {
		return nil, fmt.Errorf("bsv20 tick is required")
	}
	payload := map[string]string{
		"p":    "bsv-20",
		"op":   "transfer",
		"tick": tick,
		"amt":  strings.TrimSpace(amountText),
	}
	return json.Marshal(payload)
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

func decodeWalletTokenTransferPayload(lockingScript *bsvscript.Script) (map[string]any, bool) {
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

func normalizeWalletOverlayOutpoint(raw string) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", false
	}
	for _, sep := range []string{".", ":", "_"} {
		idx := strings.LastIndex(value, sep)
		if idx <= 0 || idx >= len(value)-1 {
			continue
		}
		txid := strings.ToLower(strings.TrimSpace(value[:idx]))
		vout := strings.TrimSpace(value[idx+1:])
		if txid == "" || vout == "" {
			continue
		}
		return txid + ":" + vout, true
	}
	return "", false
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

func buildWalletTokenSendPreparedLines(standard string, assetKey string, amountText string, assetSymbol string, toAddress string, selectedCount int, feeUTXOIDs []string, fee uint64, overlayFee uint64, changeText string, changeSatoshi uint64) []string {
	lines := []string{
		fmt.Sprintf("Token 标准: %s", standard),
		fmt.Sprintf("资产标识: %s", assetKey),
		fmt.Sprintf("发送数量: %s %s", amountText, firstNonEmptyString(assetSymbol, assetKey)),
		fmt.Sprintf("接收地址: %s", toAddress),
		fmt.Sprintf("已选承载输出: %d 个", selectedCount),
		fmt.Sprintf("已选链费输出: %d 个", len(feeUTXOIDs)),
		fmt.Sprintf("矿工费: %d sat BSV", fee),
	}
	if overlayFee > 0 {
		lines = append(lines, fmt.Sprintf("Overlay 费用: %d sat BSV", overlayFee))
	}
	if isPositiveDecimalText(changeText) {
		lines = append(lines, fmt.Sprintf("资产找零: %s %s", changeText, firstNonEmptyString(assetSymbol, assetKey)))
	}
	if changeSatoshi > 0 {
		lines = append(lines, fmt.Sprintf("BSV 找零回钱包: %d sat", changeSatoshi))
	}
	lines = append(lines, "状态: 已生成可签名预览，sign 时必须回传 expected_preview_hash。")
	if standard == "bsv21" {
		lines = append(lines, "说明: 本次构造已补齐 token details、overlay validate、overlay submit 依赖链。")
	}
	return lines
}

func walletTokenFundingNeed(assetInputSatoshi uint64, fixedOutputSatoshi uint64, fee uint64) uint64 {
	target := fixedOutputSatoshi + fee
	if assetInputSatoshi >= target {
		return 0
	}
	return target - assetInputSatoshi
}
