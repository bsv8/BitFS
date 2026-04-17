package clientapp

import (
	"context"
	"fmt"
	"strings"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
)

// 这些导出别名用于“控制桥”和“HTTP handler”复用同一套数据结构。
// 设计说明：
// - 不复制结构体，避免两套字段长期漂移；
// - 业务语义仍由原始 wallet 模块定义，这里只做触发入口导出。
type WalletTokenSendPreviewRequest = walletTokenSendPreviewRequest
type WalletTokenSendSignRequest = walletTokenSendSignRequest
type WalletAssetActionSubmitRequest = walletAssetActionSubmitRequest
type WalletAssetActionPreviewResp = walletAssetActionPreviewResp
type WalletAssetActionSignResp = walletAssetActionSignResp
type WalletAssetActionSubmitResp = walletAssetActionSubmitResp

// TriggerWalletTokenSendPreview 是 bsv21 发送预演头部入口。
// 设计说明：
// - 给 managed obs control 和 HTTP 共用，避免预演逻辑拆成两套；
// - 这里不依赖 http request，只依赖显式传入的 store + runtime。
func TriggerWalletTokenSendPreview(ctx context.Context, store *clientDB, rt *Runtime, req WalletTokenSendPreviewRequest) (WalletAssetActionPreviewResp, error) {
	if rt == nil {
		return WalletAssetActionPreviewResp{}, fmt.Errorf("runtime not initialized")
	}
	store = pickWalletTriggerStore(store, rt)
	if store == nil {
		return WalletAssetActionPreviewResp{}, fmt.Errorf("client db is nil")
	}
	standard := normalizeWalletTokenStandard(req.TokenStandard)
	if standard == "" {
		return WalletAssetActionPreviewResp{}, fmt.Errorf("invalid token standard")
	}
	assetKey := strings.TrimSpace(req.AssetKey)
	if assetKey == "" {
		return WalletAssetActionPreviewResp{}, fmt.Errorf("asset_key is required")
	}
	amountText := normalizePreviewQuantityText(req.AmountText)
	requestedAmount, err := parseDecimalText(amountText)
	if err != nil || requestedAmount.intValue == nil || requestedAmount.intValue.Sign() <= 0 {
		return WalletAssetActionPreviewResp{}, fmt.Errorf("amount_text invalid")
	}
	toAddress := strings.TrimSpace(req.ToAddress)
	if err := validatePreviewAddress(toAddress); err != nil {
		return WalletAssetActionPreviewResp{}, err
	}
	address, err := resolveWalletAddressForRuntime(ctx, store, rt)
	if err != nil {
		return WalletAssetActionPreviewResp{}, err
	}
	preview, err := previewWalletTokenSend(ctx, store, rt, address, standard, assetKey, amountText, toAddress)
	if err != nil {
		return WalletAssetActionPreviewResp{}, err
	}
	if preview.Feasible {
		prepared, prepareErr := prepareWalletTokenSend(ctx, store, rt, address, standard, assetKey, amountText, toAddress)
		if prepareErr == nil {
			preview = prepared.Preview
		} else {
			preview.CanSign = false
			preview.WarningLevel = "high"
			preview.DetailLines = append(preview.DetailLines, "状态: 当前预演可行，但真实交易构造失败，暂不能签名。")
			preview.DetailLines = append(preview.DetailLines, "原因: "+prepareErr.Error())
		}
	}
	return WalletAssetActionPreviewResp{
		Ok:      true,
		Code:    "OK",
		Message: "",
		Preview: preview,
	}, nil
}

// TriggerWalletTokenSendSign 是 bsv21 发送签名头部入口。
// 设计说明：
// - 必须带 expected_preview_hash，防止用户确认后条件变化；
// - 与 HTTP 入口保持同一校验口径。
func TriggerWalletTokenSendSign(ctx context.Context, store *clientDB, rt *Runtime, req WalletTokenSendSignRequest) (WalletAssetActionSignResp, error) {
	if rt == nil {
		return WalletAssetActionSignResp{}, fmt.Errorf("runtime not initialized")
	}
	store = pickWalletTriggerStore(store, rt)
	if store == nil {
		return WalletAssetActionSignResp{}, fmt.Errorf("client db is nil")
	}
	standard := normalizeWalletTokenStandard(req.TokenStandard)
	if standard == "" {
		return WalletAssetActionSignResp{}, fmt.Errorf("invalid token standard")
	}
	if err := validatePreviewAddress(strings.TrimSpace(req.ToAddress)); err != nil {
		return WalletAssetActionSignResp{}, err
	}
	address, err := resolveWalletAddressForRuntime(ctx, store, rt)
	if err != nil {
		return WalletAssetActionSignResp{}, err
	}
	prepared, err := prepareWalletTokenSend(
		ctx,
		store,
		rt,
		address,
		standard,
		strings.TrimSpace(req.AssetKey),
		normalizePreviewQuantityText(req.AmountText),
		strings.TrimSpace(req.ToAddress),
	)
	if err != nil {
		return WalletAssetActionSignResp{}, err
	}
	expected := strings.ToLower(strings.TrimSpace(req.ExpectedPreviewHash))
	if expected == "" {
		return WalletAssetActionSignResp{
			Ok:      false,
			Code:    "PREVIEW_REQUIRED",
			Message: "expected preview hash is required",
			Preview: prepared.Preview,
		}, nil
	}
	if !strings.EqualFold(expected, prepared.Preview.PreviewHash) {
		return WalletAssetActionSignResp{
			Ok:      false,
			Code:    "PREVIEW_CHANGED",
			Message: "wallet preview changed; retry",
			Preview: prepared.Preview,
		}, nil
	}
	return WalletAssetActionSignResp{
		Ok:          true,
		Code:        "OK",
		Message:     "",
		Preview:     prepared.Preview,
		SignedTxHex: prepared.SignedTxHex,
		TxID:        prepared.TxID,
	}, nil
}

// TriggerWalletTokenSendSubmit 是 bsv21 发送广播头部入口。
// 设计说明：
// - 广播成功后必须做本地投影和事实收口；
// - 任一步失败都返回错误，不能报“成功但稍后补账”。
func TriggerWalletTokenSendSubmit(ctx context.Context, store *clientDB, rt *Runtime, req WalletAssetActionSubmitRequest) (WalletAssetActionSubmitResp, error) {
	if rt == nil || rt.ActionChain == nil {
		return WalletAssetActionSubmitResp{}, fmt.Errorf("runtime not initialized")
	}
	store = pickWalletTriggerStore(store, rt)
	if store == nil {
		return WalletAssetActionSubmitResp{}, fmt.Errorf("client db is nil")
	}
	txHex := strings.ToLower(strings.TrimSpace(req.SignedTxHex))
	if txHex == "" {
		return WalletAssetActionSubmitResp{}, fmt.Errorf("signed_tx_hex is required")
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return WalletAssetActionSubmitResp{}, fmt.Errorf("signed_tx_hex invalid: %w", err)
	}
	localTxID := strings.ToLower(strings.TrimSpace(parsed.TxID().String()))
	broadcastTxID, err := rt.ActionChain.Broadcast(txHex)
	if err != nil {
		return WalletAssetActionSubmitResp{}, fmt.Errorf("broadcast token send failed: %w", err)
	}
	finalTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if finalTxID == "" {
		finalTxID = localTxID
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, rt, txHex, "wallet_token_send_submit"); err != nil {
		recordWalletTokenSendRetryTask(rt, finalTxID, "local_broadcast_projection", err, txHex)
		return WalletAssetActionSubmitResp{}, fmt.Errorf("project token send failed for txid %s: %w", finalTxID, err)
	}
	if err := appendBSV21TokenSendAccountingAfterBroadcast(ctx, store, rt, txHex, finalTxID); err != nil {
		recordWalletTokenSendRetryTask(rt, finalTxID, "fact_write", err, txHex)
		return WalletAssetActionSubmitResp{}, fmt.Errorf("append token send accounting failed for txid %s: %w", finalTxID, err)
	}
	return WalletAssetActionSubmitResp{
		Ok:      true,
		Code:    "OK",
		Message: walletBSV21SubmitMessage(parsed),
		TxID:    finalTxID,
	}, nil
}

func pickWalletTriggerStore(store *clientDB, rt *Runtime) *clientDB {
	if store != nil {
		return store
	}
	if rt != nil {
		return rt.DB()
	}
	return nil
}

func resolveWalletAddressForRuntime(ctx context.Context, store *clientDB, rt *Runtime) (string, error) {
	if rt != nil {
		addr, err := clientWalletAddress(rt)
		if err == nil && strings.TrimSpace(addr) != "" {
			return strings.TrimSpace(addr), nil
		}
	}
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return dbResolveWalletAddress(ctx, store)
}
