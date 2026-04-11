package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/fundalloc"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
)

// WalletBSVTransferRequest 是钱包普通 BSV 转账的业务输入。
// 设计说明：
// - 这是钱包业务入口，不是测试工具；
// - 上层只给出收款地址和金额，剩下的选币、组交易、签名、广播、投影都在这里收口；
// - 这样 e2e 只需要触发业务，不需要再自己拼交易。
type WalletBSVTransferRequest struct {
	ToAddress     string `json:"to_address"`
	AmountSatoshi uint64 `json:"amount_satoshi"`
}

// WalletBSVTransferResult 是普通 BSV 转账的执行结果。
// 设计说明：
// - success / failure 都保留足够的业务事实，方便 e2e 结果文件直接看；
// - 失败时保留 preview 性质的细节，不让上层再去猜“为什么没法签”。
type WalletBSVTransferResult struct {
	Ok              bool     `json:"ok"`
	Code            string   `json:"code"`
	Message         string   `json:"message,omitempty"`
	FromAddress     string   `json:"from_address,omitempty"`
	ToAddress       string   `json:"to_address,omitempty"`
	AmountSatoshi   uint64   `json:"amount_satoshi,omitempty"`
	TxID            string   `json:"txid,omitempty"`
	SelectedUTXOIDs []string `json:"selected_utxo_ids,omitempty"`
	SelectedCount   int      `json:"selected_count,omitempty"`
	MinerFeeSatoshi uint64   `json:"miner_fee_satoshi,omitempty"`
	ChangeSatoshi   uint64   `json:"change_satoshi,omitempty"`
	PreviewHash     string   `json:"preview_hash,omitempty"`
	DetailLines     []string `json:"detail_lines,omitempty"`
}

type walletBSVTransferPrepared struct {
	Result      WalletBSVTransferResult
	signedTxHex string
}

// TriggerWalletBSVTransfer 触发普通 BSV 转账。
// 设计说明：
// - 这里直接走业务层，不借助外部工具去拼交易；
// - 成功时会把本地钱包视图投影回去，保证调用方看到的是同一条业务链路；
// - 余额不足、地址非法、运行时未初始化都直接失败，不做旁路兜底。
func TriggerWalletBSVTransfer(ctx context.Context, store *clientDB, rt *Runtime, req WalletBSVTransferRequest) (WalletBSVTransferResult, error) {
	prepared, err := prepareWalletBSVTransfer(ctx, store, rt, req)
	if err != nil {
		return prepared.Result, err
	}
	if prepared.signedTxHex == "" {
		return prepared.Result, fmt.Errorf("signed tx hex is empty")
	}
	broadcastTxID, err := rt.ActionChain.Broadcast(prepared.signedTxHex)
	if err != nil {
		prepared.Result.Ok = false
		prepared.Result.Code = "BROADCAST_FAILED"
		prepared.Result.Message = "broadcast wallet bsv transfer failed"
		prepared.Result.TxID = prepared.Result.TxID
		return prepared.Result, fmt.Errorf("%s: %w", prepared.Result.Message, err)
	}
	localTxID := prepared.Result.TxID
	finalTxID := strings.ToLower(strings.TrimSpace(broadcastTxID))
	if finalTxID == "" {
		finalTxID = localTxID
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, rt, prepared.signedTxHex, "wallet_bsv_transfer_submit"); err != nil {
		prepared.Result.Ok = false
		prepared.Result.Code = "LOCAL_PROJECTION_FAILED"
		prepared.Result.Message = "project wallet bsv transfer failed"
		prepared.Result.TxID = finalTxID
		return prepared.Result, fmt.Errorf("%s for txid %s: %w", prepared.Result.Message, finalTxID, err)
	}
	prepared.Result.Ok = true
	prepared.Result.Code = "OK"
	prepared.Result.Message = "wallet bsv transfer submitted"
	prepared.Result.TxID = finalTxID
	return prepared.Result, nil
}

func prepareWalletBSVTransfer(ctx context.Context, store *clientDB, rt *Runtime, req WalletBSVTransferRequest) (walletBSVTransferPrepared, error) {
	if store == nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "STORE_NOT_INITIALIZED", Message: "store not initialized"}}, fmt.Errorf("store not initialized")
	}
	if rt == nil || rt.ActionChain == nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "RUNTIME_NOT_INITIALIZED", Message: "runtime not initialized"}}, fmt.Errorf("runtime not initialized")
	}
	if req.AmountSatoshi == 0 {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "INVALID_AMOUNT", Message: "amount_satoshi must be greater than zero"}}, fmt.Errorf("amount_satoshi must be greater than zero")
	}
	toAddress := strings.TrimSpace(req.ToAddress)
	if err := validatePreviewAddress(toAddress); err != nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "INVALID_ADDRESS", Message: err.Error(), ToAddress: toAddress, AmountSatoshi: req.AmountSatoshi}}, err
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "RUNTIME_NOT_INITIALIZED", Message: err.Error()}}, err
	}
	fromAddress := strings.TrimSpace(actor.Addr)
	if fromAddress == "" {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "RUNTIME_NOT_INITIALIZED", Message: "wallet address is empty"}}, fmt.Errorf("wallet address is empty")
	}

	// 钱包选币必须串行，避免两个业务同时挑中同一批 plain BSV 输入。
	rt.walletAllocMu.Lock()
	defer rt.walletAllocMu.Unlock()

	candidates, err := dbListSpendableBSVUTXOs(ctx, store, strings.ToLower(strings.TrimSpace(actor.PubHex)))
	if err != nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "BALANCE_LOAD_FAILED", Message: err.Error(), FromAddress: fromAddress, ToAddress: toAddress, AmountSatoshi: req.AmountSatoshi}}, fmt.Errorf("load spendable plain bsv utxos failed: %w", err)
	}
	available := totalPlainBSVAmount(candidates)
	if available == 0 {
		result := WalletBSVTransferResult{
			Ok:            false,
			Code:          "INSUFFICIENT_BALANCE",
			Message:       "insufficient plain bsv balance",
			FromAddress:   fromAddress,
			ToAddress:     toAddress,
			AmountSatoshi: req.AmountSatoshi,
			DetailLines: []string{
				"发送地址: " + fromAddress,
				"接收地址: " + toAddress,
				fmt.Sprintf("发送金额: %d sat", req.AmountSatoshi),
				"可用 plain BSV: 0 sat",
				"状态: plain BSV 不足，不能继续选币。",
			},
		}
		return walletBSVTransferPrepared{Result: result}, fmt.Errorf("insufficient plain bsv balance: have=0 need>=%d", req.AmountSatoshi+1)
	}

	const maxAttempt = 4
	feeGuess := uint64(1)
	var selected []fundalloc.Candidate
	var fee uint64
	var change uint64
	var signedTxHex string
	var txid string
	for attempt := 0; attempt < maxAttempt; attempt++ {
		_ = attempt
		target := req.AmountSatoshi + feeGuess
		selection, selErr := fundalloc.SelectPlainBSVForTarget(candidatesToFundingCandidates(candidates), target)
		if selErr != nil {
			result := WalletBSVTransferResult{
				Ok:            false,
				Code:          "INSUFFICIENT_BALANCE",
				Message:       "insufficient plain bsv balance",
				FromAddress:   fromAddress,
				ToAddress:     toAddress,
				AmountSatoshi: req.AmountSatoshi,
				DetailLines: []string{
					"发送地址: " + fromAddress,
					"接收地址: " + toAddress,
					fmt.Sprintf("发送金额: %d sat", req.AmountSatoshi),
					fmt.Sprintf("可用 plain BSV: %d sat", available),
					fmt.Sprintf("当前估算需要: >= %d sat", target),
					"状态: plain BSV 不足，不能继续组交易。",
				},
			}
			return walletBSVTransferPrepared{Result: result}, fmt.Errorf("insufficient plain bsv balance: have=%d need>=%d", available, target)
		}
		selected = append([]fundalloc.Candidate(nil), selection.Selected...)
		total := selection.TotalSatoshi
		outputCount := 1
		if total > req.AmountSatoshi {
			outputCount = 2
		}
		fee = estimateMinerFeeSatPerKB(estimateProvisionalP2PKHSize(len(selected), outputCount), walletAssetPreviewFeeRateSatPerKB)
		if total < req.AmountSatoshi+fee {
			feeGuess = req.AmountSatoshi + fee - total
			continue
		}
		change = total - req.AmountSatoshi - fee
		outputCount = 1
		if change > 0 {
			outputCount = 2
		}
		finalFee := estimateMinerFeeSatPerKB(estimateProvisionalP2PKHSize(len(selected), outputCount), walletAssetPreviewFeeRateSatPerKB)
		if total < req.AmountSatoshi+finalFee {
			feeGuess = req.AmountSatoshi + finalFee - total
			continue
		}
		fee = finalFee
		change = total - req.AmountSatoshi - fee
		builder, buildErr := buildWalletBSVTransferTx(actor, selected, toAddress, req.AmountSatoshi, change)
		if buildErr != nil {
			return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "BUILD_FAILED", Message: buildErr.Error(), FromAddress: fromAddress, ToAddress: toAddress, AmountSatoshi: req.AmountSatoshi}}, buildErr
		}
		signedTxHex = builder.signedTxHex
		txid = builder.txid
		previewHashBytes := []byte(strings.TrimSpace(signedTxHex))
		if rawTx, decodeErr := hex.DecodeString(strings.TrimSpace(signedTxHex)); decodeErr == nil && len(rawTx) > 0 {
			previewHashBytes = rawTx
		}
		previewHash := walletBusinessPreviewHash(previewHashBytes)
		result := WalletBSVTransferResult{
			Ok:              true,
			Code:            "OK",
			Message:         "wallet bsv transfer prepared",
			FromAddress:     fromAddress,
			ToAddress:       toAddress,
			AmountSatoshi:   req.AmountSatoshi,
			TxID:            txid,
			SelectedUTXOIDs: selectedCandidateIDs(selected),
			SelectedCount:   len(selected),
			MinerFeeSatoshi: fee,
			ChangeSatoshi:   change,
			PreviewHash:     previewHash,
			DetailLines: []string{
				"发送地址: " + fromAddress,
				"接收地址: " + toAddress,
				fmt.Sprintf("发送金额: %d sat", req.AmountSatoshi),
				fmt.Sprintf("已选输入: %d 个", len(selected)),
				fmt.Sprintf("矿工费: %d sat", fee),
				fmt.Sprintf("找零: %d sat", change),
				"状态: 已生成可签名交易，下一步会直接广播并投影。",
			},
		}
		return walletBSVTransferPrepared{Result: result, signedTxHex: signedTxHex}, nil
	}

	return walletBSVTransferPrepared{Result: WalletBSVTransferResult{
		Ok:            false,
		Code:          "INSUFFICIENT_BALANCE",
		Message:       "insufficient plain bsv balance",
		FromAddress:   fromAddress,
		ToAddress:     toAddress,
		AmountSatoshi: req.AmountSatoshi,
		DetailLines: []string{
			"发送地址: " + fromAddress,
			"接收地址: " + toAddress,
			fmt.Sprintf("发送金额: %d sat", req.AmountSatoshi),
			fmt.Sprintf("可用 plain BSV: %d sat", available),
			"状态: 反复估算后仍然无法拼出足够的 plain BSV 交易。",
		},
	}}, fmt.Errorf("insufficient plain bsv balance: have=%d need>=%d", available, req.AmountSatoshi+feeGuess)
}

type walletBSVTransferTx struct {
	signedTxHex string
	txid        string
}

func buildWalletBSVTransferTx(actor *poolcore.Actor, selected []fundalloc.Candidate, toAddress string, amountSatoshi uint64, changeSatoshi uint64) (walletBSVTransferTx, error) {
	if actor == nil {
		return walletBSVTransferTx{}, fmt.Errorf("runtime not initialized")
	}
	if len(selected) == 0 {
		return walletBSVTransferTx{}, fmt.Errorf("selected plain bsv inputs are empty")
	}
	toAddress = strings.TrimSpace(toAddress)
	if toAddress == "" {
		return walletBSVTransferTx{}, fmt.Errorf("to_address is required")
	}
	fromAddr, err := script.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("parse sender address failed: %w", err)
	}
	senderLockScript, err := p2pkh.Lock(fromAddr)
	if err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("build sender lock script failed: %w", err)
	}
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("build sender unlock template failed: %w", err)
	}
	txBuilder := txsdk.NewTransaction()
	total := uint64(0)
	prevLockHex := hex.EncodeToString(senderLockScript.Bytes())
	for _, item := range selected {
		if err := txBuilder.AddInputFrom(item.TxID, item.Vout, prevLockHex, item.ValueSatoshi, unlockTpl); err != nil {
			return walletBSVTransferTx{}, fmt.Errorf("add transfer input failed: %w", err)
		}
		total += item.ValueSatoshi
	}
	if err := txBuilder.PayToAddress(toAddress, amountSatoshi); err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("build transfer output failed: %w", err)
	}
	hasChangeOutput := changeSatoshi > 0
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      changeSatoshi,
			LockingScript: senderLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("pre-sign wallet transfer failed: %w", err)
	}
	fee := total - amountSatoshi
	if hasChangeOutput {
		if total < amountSatoshi+changeSatoshi {
			return walletBSVTransferTx{}, fmt.Errorf("selected inputs are insufficient for change output")
		}
		fee = total - amountSatoshi - changeSatoshi
	}
	if fee == 0 {
		fee = estimateMinerFeeSatPerKB(txBuilder.Size(), walletAssetPreviewFeeRateSatPerKB)
	}
	if total < amountSatoshi+fee {
		return walletBSVTransferTx{}, fmt.Errorf("insufficient plain bsv balance after fee estimation")
	}
	if hasChangeOutput {
		txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = total - amountSatoshi - fee
	} else if total > amountSatoshi+fee {
		changeSatoshi = total - amountSatoshi - fee
		if changeSatoshi > 0 {
			txBuilder.AddOutput(&txsdk.TransactionOutput{
				Satoshis:      changeSatoshi,
				LockingScript: senderLockScript,
			})
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return walletBSVTransferTx{}, fmt.Errorf("final-sign wallet transfer failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return walletBSVTransferTx{
		signedTxHex: strings.ToLower(strings.TrimSpace(txBuilder.Hex())),
		txid:        txID,
	}, nil
}

func candidatesToFundingCandidates(in []bsvUTXOEntry) []fundalloc.Candidate {
	out := make([]fundalloc.Candidate, 0, len(in))
	for _, item := range in {
		out = append(out, fundalloc.Candidate{
			ID:               item.UTXOID,
			TxID:             item.TxID,
			Vout:             item.Vout,
			ValueSatoshi:     uint64(item.ValueSatoshi),
			CreatedAtUnix:    item.CreatedAtUnix,
			ProtectionClass:  fundalloc.ProtectionPlainBSV,
			ProtectionReason: strings.TrimSpace(item.Note),
		})
	}
	return out
}

func totalPlainBSVAmount(in []bsvUTXOEntry) uint64 {
	var total uint64
	for _, item := range in {
		if strings.ToLower(strings.TrimSpace(item.CarrierType)) != "plain_bsv" {
			continue
		}
		if strings.ToLower(strings.TrimSpace(item.UTXOState)) != "unspent" {
			continue
		}
		total += uint64(item.ValueSatoshi)
	}
	return total
}

func selectedCandidateIDs(in []fundalloc.Candidate) []string {
	out := make([]string, 0, len(in))
	for _, item := range in {
		out = append(out, strings.ToLower(strings.TrimSpace(item.ID)))
	}
	return out
}
