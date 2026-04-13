package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

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

type walletBSVTransferSubmissionResult struct {
	Result        WalletBSVTransferResult
	SignedTxHex   string
	BroadcastTxID string
}

// TriggerWalletBSVTransfer 只保留给结算层内部执行。
// 设计说明：
// - 这不是新的业务入口，外层不能直接拿它当“下单”；
// - 它只负责把一笔已经准备好的普通 BSV 转账广播出去，并回写本地钱包视图；
// - 结算层要先写 biz_/settle_，再调用这里完成实际链上动作。
func TriggerWalletBSVTransfer(ctx context.Context, store *clientDB, rt *Runtime, req WalletBSVTransferRequest) (WalletBSVTransferResult, error) {
	prepared, err := prepareWalletBSVTransfer(ctx, store, rt, req)
	if err != nil {
		return prepared.Result, err
	}
	submitted, err := submitWalletBSVTransferPrepared(ctx, store, rt, prepared)
	if err != nil {
		return submitted.Result, err
	}
	return submitted.Result, nil
}

func submitWalletBSVTransferPrepared(ctx context.Context, store *clientDB, rt *Runtime, prepared walletBSVTransferPrepared) (walletBSVTransferSubmissionResult, error) {
	if prepared.signedTxHex == "" {
		return walletBSVTransferSubmissionResult{Result: prepared.Result}, fmt.Errorf("signed tx hex is empty")
	}
	if rt == nil || rt.ActionChain == nil {
		return walletBSVTransferSubmissionResult{Result: prepared.Result}, fmt.Errorf("runtime not initialized")
	}
	broadcastTxID, err := rt.ActionChain.Broadcast(prepared.signedTxHex)
	if err != nil {
		prepared.Result.Ok = false
		prepared.Result.Code = "BROADCAST_FAILED"
		prepared.Result.Message = "broadcast wallet bsv transfer failed"
		prepared.Result.TxID = prepared.Result.TxID
		return walletBSVTransferSubmissionResult{Result: prepared.Result, SignedTxHex: prepared.signedTxHex}, fmt.Errorf("%s: %w", prepared.Result.Message, err)
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
		return walletBSVTransferSubmissionResult{Result: prepared.Result, SignedTxHex: prepared.signedTxHex, BroadcastTxID: finalTxID}, fmt.Errorf("%s for txid %s: %w", prepared.Result.Message, finalTxID, err)
	}
	prepared.Result.Ok = true
	prepared.Result.Code = "OK"
	prepared.Result.Message = "wallet bsv transfer submitted"
	prepared.Result.TxID = finalTxID
	return walletBSVTransferSubmissionResult{
		Result:        prepared.Result,
		SignedTxHex:   prepared.signedTxHex,
		BroadcastTxID: finalTxID,
	}, nil
}

// executeBizOrderPayBSVSettlement 结算层内部执行器。
// 设计说明：
// - 先跑真实钱包转账，再把链上结果回写到 order_settlements / order_settlement_events；
// - 同一个 order_id + idempotency_key 重复提交时，不会再生成第二笔支付；
// - 前台最终看的是 biz_ + settle_ 聚合，不再看钱包直发结果。
func executeBizOrderPayBSVSettlement(ctx context.Context, store *clientDB, rt *Runtime, orderID string, businessID string, settlementID string, idempotencyKey string, toAddress string, amountSatoshi uint64, fromPartyID string, toPartyID string) (BizOrderPayBSVResponse, error) {
	resp := BizOrderPayBSVResponse{
		Ok:             false,
		Code:           "PENDING",
		OrderID:        strings.TrimSpace(businessID),
		SettlementID:   strings.TrimSpace(settlementID),
		IdempotencyKey: strings.TrimSpace(idempotencyKey),
		Status:         "pending",
	}
	if ctx == nil {
		return resp, fmt.Errorf("context is required")
	}
	if store == nil {
		return resp, fmt.Errorf("client db is nil")
	}
	if rt == nil || rt.ActionChain == nil {
		return resp, fmt.Errorf("runtime not initialized")
	}

	// 幂等重复：先原子抢占执行权，只有把 pending 改成 processing 的请求才能继续打链上。
	existing, claimed, err := claimBusinessSettlementExecutionTx(ctx, store, businessID)
	if err != nil {
		return resp, err
	}
	if !claimed {
		existingStatus := strings.ToLower(strings.TrimSpace(existing.Status))
		summary, sumErr := GetFrontOrderSettlementSummary(ctx, store, orderID)
		if sumErr == nil {
			resp.FrontOrderSummary = &summary
			resp.TargetAmountSatoshi = summary.Summary.TotalTargetSatoshi
			resp.SettledAmountSatoshi = summary.Summary.SettledAmountSatoshi
		}
		resp.Ok = existingStatus == "settled" || existingStatus == "submitted" || existingStatus == "submitted_unknown_projection"
		resp.Code = strings.ToUpper(existingStatus)
		resp.Status = existingStatus
		resp.Message = "existing settlement returned"
		if txid, txErr := GetBusinessSettlementChainTxID(ctx, store, existing); txErr == nil {
			resp.TxID = txid
		}
		return resp, nil
	}

	prepared, err := prepareWalletBSVTransfer(ctx, store, rt, WalletBSVTransferRequest{
		ToAddress:     toAddress,
		AmountSatoshi: amountSatoshi,
	})
	if err != nil {
		msg := strings.ToLower(strings.TrimSpace(err.Error()))
		if strings.Contains(msg, "insufficient plain bsv balance") {
			now := time.Now().Unix()
			eventPayload := map[string]any{
				"order_id":        orderID,
				"settlement_id":   settlementID,
				"idempotency_key": idempotencyKey,
				"to_address":      toAddress,
				"amount_satoshi":  amountSatoshi,
				"status":          "waiting_fund",
				"error":           err.Error(),
			}
			if txErr := store.Tx(ctx, func(tx *sql.Tx) error {
				if writeErr := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
					ProcessID:         "proc_" + businessID,
					SourceType:        "biz_order_pay_bsv",
					SourceID:          settlementID,
					AccountingScene:   "wallet_transfer",
					AccountingSubType: "pay_bsv",
					EventType:         "wallet_transfer_prepare",
					Status:            "failed",
					OccurredAtUnix:    now,
					IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
					Note:              "wallet transfer waiting for funds",
					Payload:           eventPayload,
				}); writeErr != nil {
					return writeErr
				}
				return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
					OrderID:           businessID,
					SettlementID:      settlementID,
					BusinessStatus:    "waiting_fund",
					SettlementStatus:  "waiting_fund",
					SettlementMethod:  string(SettlementMethodChain),
					TargetType:        "",
					TargetID:          "",
					ErrorMessage:      err.Error(),
					SettlementPayload: eventPayload,
					UpdatedAtUnix:     now,
				})
			}); txErr != nil {
				return resp, txErr
			}
			summary, _ := GetFrontOrderSettlementSummary(ctx, store, orderID)
			resp.Ok = false
			resp.Code = "WAITING_FUND"
			resp.Message = err.Error()
			resp.Status = "waiting_fund"
			resp.FrontOrderSummary = &summary
			resp.TargetAmountSatoshi = summary.Summary.TotalTargetSatoshi
			resp.SettledAmountSatoshi = summary.Summary.SettledAmountSatoshi
			return resp, nil
		}
		now := time.Now().Unix()
		failedPayload := map[string]any{
			"order_id":        orderID,
			"settlement_id":   settlementID,
			"idempotency_key": idempotencyKey,
			"to_address":      toAddress,
			"amount_satoshi":  amountSatoshi,
			"status":          "failed",
			"error":           err.Error(),
		}
		if txErr := store.Tx(ctx, func(tx *sql.Tx) error {
			if writeErr := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
				ProcessID:         "proc_" + businessID,
				SourceType:        "biz_order_pay_bsv",
				SourceID:          settlementID,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "pay_bsv",
				EventType:         "wallet_transfer_prepare",
				Status:            "failed",
				OccurredAtUnix:    now,
				IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
				Note:              "wallet transfer prepare failed",
				Payload:           failedPayload,
			}); writeErr != nil {
				return writeErr
			}
			return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
				OrderID:           businessID,
				SettlementID:      settlementID,
				BusinessStatus:    "failed",
				SettlementStatus:  "failed",
				SettlementMethod:  string(SettlementMethodChain),
				TargetType:        "",
				TargetID:          "",
				ErrorMessage:      err.Error(),
				SettlementPayload: failedPayload,
				UpdatedAtUnix:     now,
			})
		}); txErr != nil {
			return resp, txErr
		}
		return resp, err
	}

	submitted, submitErr := submitWalletBSVTransferPrepared(ctx, store, rt, prepared)
	now := time.Now().Unix()
	finalStatus := "submitted"
	finalCode := "OK"
	finalMessage := "wallet transfer submitted"
	processStatus := "submitted"
	if submitErr != nil {
		switch submitted.Result.Code {
		case "LOCAL_PROJECTION_FAILED":
			finalStatus = "submitted_unknown_projection"
			finalCode = "SUBMITTED_UNKNOWN_PROJECTION"
			finalMessage = "broadcast succeeded but local projection failed"
			processStatus = "submitted_unknown_projection"
		case "BROADCAST_FAILED":
			finalStatus = "failed"
			finalCode = "BROADCAST_FAILED"
			finalMessage = submitted.Result.Message
			processStatus = "failed"
		default:
			finalStatus = "failed"
			finalCode = "SUBMIT_FAILED"
			finalMessage = submitErr.Error()
			processStatus = "failed"
			submitted.Result.Ok = false
			submitted.Result.Code = finalCode
			submitted.Result.Message = finalMessage
		}
	}

	payload := map[string]any{
		"order_id":          orderID,
		"settlement_id":     settlementID,
		"idempotency_key":   idempotencyKey,
		"amount_satoshi":    amountSatoshi,
		"miner_fee_sat":     submitted.Result.MinerFeeSatoshi,
		"change_sat":        submitted.Result.ChangeSatoshi,
		"to_address":        toAddress,
		"txid":              submitted.BroadcastTxID,
		"selected_utxo_ids": append([]string(nil), submitted.Result.SelectedUTXOIDs...),
	}

	if finalStatus == "failed" {
		if err := store.Tx(ctx, func(tx *sql.Tx) error {
			if err := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
				ProcessID:         "proc_" + businessID,
				SourceType:        "biz_order_pay_bsv",
				SourceID:          settlementID,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "pay_bsv",
				EventType:         "wallet_transfer_submit",
				Status:            processStatus,
				OccurredAtUnix:    now,
				IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
				Note:              finalMessage,
				Payload:           payload,
			}); err != nil {
				return err
			}
			return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
				OrderID:           businessID,
				SettlementID:      settlementID,
				BusinessStatus:    finalStatus,
				SettlementStatus:  finalStatus,
				SettlementMethod:  string(SettlementMethodChain),
				TargetType:        "",
				TargetID:          "",
				ErrorMessage:      finalMessage,
				SettlementPayload: payload,
				UpdatedAtUnix:     now,
			})
		}); err != nil {
			return resp, err
		}
		summary, _ := GetFrontOrderSettlementSummary(ctx, store, orderID)
		_ = dbUpdateFrontOrderStatus(ctx, store, orderID, summary.Summary.OverallStatus)
		resp.Ok = false
		resp.Code = finalCode
		resp.Message = finalMessage
		resp.Status = finalStatus
		resp.TxID = submitted.BroadcastTxID
		resp.TargetAmountSatoshi = summary.Summary.TotalTargetSatoshi
		resp.SettledAmountSatoshi = summary.Summary.SettledAmountSatoshi
		resp.FrontOrderSummary = &summary
		return resp, nil
	}

	if finalStatus == "submitted_unknown_projection" {
		payload["status"] = "submitted_unknown_projection"
		payload["error"] = submitted.Result.Message
		if err := store.Tx(ctx, func(tx *sql.Tx) error {
			if err := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
				ProcessID:         "proc_" + businessID,
				SourceType:        "biz_order_pay_bsv",
				SourceID:          settlementID,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "pay_bsv",
				EventType:         "wallet_transfer_submit",
				Status:            "submitted_unknown_projection",
				OccurredAtUnix:    now,
				IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
				Note:              "broadcast succeeded but local projection failed",
				Payload:           payload,
			}); err != nil {
				return err
			}
			return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
				OrderID:           businessID,
				SettlementID:      settlementID,
				BusinessStatus:    "submitted_unknown_projection",
				SettlementStatus:  "submitted_unknown_projection",
				SettlementMethod:  string(SettlementMethodChain),
				TargetType:        "",
				TargetID:          "",
				ErrorMessage:      submitted.Result.Message,
				SettlementPayload: payload,
				UpdatedAtUnix:     now,
			})
		}); err != nil {
			return resp, err
		}
		finalStatus = "submitted_unknown_projection"
		finalCode = "SUBMITTED_UNKNOWN_PROJECTION"
		finalMessage = "broadcast succeeded but local projection failed"
		processStatus = "submitted_unknown_projection"
	} else {
		chainPaymentPayload := map[string]any{
			"order_id":          orderID,
			"settlement_id":     settlementID,
			"idempotency_key":   idempotencyKey,
			"amount_satoshi":    amountSatoshi,
			"miner_fee_sat":     submitted.Result.MinerFeeSatoshi,
			"change_sat":        submitted.Result.ChangeSatoshi,
			"to_address":        toAddress,
			"txid":              submitted.BroadcastTxID,
			"selected_utxo_ids": append([]string(nil), submitted.Result.SelectedUTXOIDs...),
		}
		ownerPubkeyHex := strings.ToLower(strings.TrimSpace(rt.ClientID()))
		if ownerPubkeyHex == "" {
			clientActor, actorErr := buildClientActorFromRunInput(rt.runIn)
			if actorErr != nil {
				return resp, fmt.Errorf("client id is required for settlement records")
			}
			ownerPubkeyHex = strings.ToLower(strings.TrimSpace(clientActor.PubHex))
		}
		if ownerPubkeyHex == "" {
			return resp, fmt.Errorf("client id is required for settlement records")
		}
		selectedUTXOIDs := append([]string(nil), submitted.Result.SelectedUTXOIDs...)
		if len(selectedUTXOIDs) == 0 {
			return resp, fmt.Errorf("selected utxo ids are required for settlement records")
		}
		if err := store.Tx(ctx, func(tx *sql.Tx) error {
			channelID, err := dbUpsertChainChannelWithSettlementPaymentAttempt(ctx, tx, chainPaymentEntry{
				TxID:                 submitted.BroadcastTxID,
				PaymentSubType:       "biz_order_pay_bsv",
				Status:               processStatus,
				WalletInputSatoshi:   int64(amountSatoshi + submitted.Result.MinerFeeSatoshi + submitted.Result.ChangeSatoshi),
				WalletOutputSatoshi:  int64(submitted.Result.ChangeSatoshi),
				NetAmountSatoshi:     -int64(amountSatoshi + submitted.Result.MinerFeeSatoshi),
				OccurredAtUnix:       now,
				SubmittedAtUnix:      now,
				WalletObservedAtUnix: now,
				FromPartyID:          strings.TrimSpace(fromPartyID),
				ToPartyID:            strings.TrimSpace(toPartyID),
				Payload:              chainPaymentPayload,
			},
				"chain_direct_pay",
				"fact_settlement_channel_chain_direct_pay",
				"payment_attempt_chain_direct_pay",
				"bind chain direct pay channel id",
			)
			if err != nil {
				return err
			}
			payload["chain_direct_pay_id"] = channelID
			chainPaymentPayload["chain_direct_pay_id"] = channelID
			settlementPaymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceCtx(ctx, tx, "chain_direct_pay", fmt.Sprintf("%d", channelID))
			if err != nil {
				return fmt.Errorf("resolve settlement payment attempt for chain direct pay: %w", err)
			}
			if err := dbAppendBSVSettlementRecordsForCycleTx(ctx, tx, settlementPaymentAttemptID, ownerPubkeyHex, selectedUTXOIDs, now, chainPaymentPayload); err != nil {
				return err
			}
			if err := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
				ProcessID:         "proc_" + businessID,
				SourceType:        "biz_order_pay_bsv",
				SourceID:          settlementID,
				AccountingScene:   "wallet_transfer",
				AccountingSubType: "pay_bsv",
				EventType:         "wallet_transfer_submit",
				Status:            processStatus,
				OccurredAtUnix:    now,
				IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
				Note:              finalMessage,
				Payload:           payload,
			}); err != nil {
				return err
			}
			return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
				OrderID:           businessID,
				SettlementID:      settlementID,
				BusinessStatus:    finalStatus,
				SettlementStatus:  finalStatus,
				SettlementMethod:  string(SettlementMethodChain),
				TargetType:        "chain_direct_pay",
				TargetID:          fmt.Sprintf("%d", channelID),
				ErrorMessage:      "",
				SettlementPayload: payload,
				UpdatedAtUnix:     now,
			})
		}); err != nil {
			failedPayload := map[string]any{
				"order_id":          orderID,
				"settlement_id":     settlementID,
				"idempotency_key":   idempotencyKey,
				"amount_satoshi":    amountSatoshi,
				"miner_fee_sat":     submitted.Result.MinerFeeSatoshi,
				"change_sat":        submitted.Result.ChangeSatoshi,
				"to_address":        toAddress,
				"txid":              submitted.BroadcastTxID,
				"selected_utxo_ids": append([]string(nil), submitted.Result.SelectedUTXOIDs...),
				"status":            "failed",
				"error":             err.Error(),
			}
			_ = store.Tx(ctx, func(tx *sql.Tx) error {
				if writeErr := dbAppendFinProcessEvent(ctx, tx, finProcessEventEntry{
					ProcessID:         "proc_" + businessID,
					SourceType:        "biz_order_pay_bsv",
					SourceID:          settlementID,
					AccountingScene:   "wallet_transfer",
					AccountingSubType: "pay_bsv",
					EventType:         "wallet_transfer_submit",
					Status:            "failed",
					OccurredAtUnix:    now,
					IdempotencyKey:    "biz_order_pay_bsv:" + idempotencyKey,
					Note:              "write settlement outcome failed",
					Payload:           failedPayload,
				}); writeErr != nil {
					return writeErr
				}
				return dbUpdateBusinessSettlementOutcomeTx(ctx, tx, businessSettlementOutcomeEntry{
					OrderID:           businessID,
					SettlementID:      settlementID,
					BusinessStatus:    "failed",
					SettlementStatus:  "failed",
					SettlementMethod:  string(SettlementMethodChain),
					TargetType:        "",
					TargetID:          "",
					ErrorMessage:      err.Error(),
					SettlementPayload: failedPayload,
					UpdatedAtUnix:     now,
				})
			})
			resp.Ok = false
			resp.Code = "FAILED"
			resp.Message = err.Error()
			resp.Status = "failed"
			resp.TxID = submitted.BroadcastTxID
			return resp, fmt.Errorf("write settlement outcome failed: %w", err)
		}
	}

	summary, _ := GetFrontOrderSettlementSummary(ctx, store, orderID)
	_ = dbUpdateFrontOrderStatus(ctx, store, orderID, summary.Summary.OverallStatus)

	submitted.Result.Ok = finalStatus == "submitted" || finalStatus == "submitted_unknown_projection"
	submitted.Result.Code = finalCode
	submitted.Result.Message = finalMessage
	submitted.Result.TxID = submitted.BroadcastTxID

	resp.Ok = submitted.Result.Ok
	resp.Code = finalCode
	resp.Message = finalMessage
	resp.Status = finalStatus
	resp.TxID = submitted.BroadcastTxID
	resp.TargetAmountSatoshi = summary.Summary.TotalTargetSatoshi
	resp.SettledAmountSatoshi = summary.Summary.SettledAmountSatoshi
	resp.FrontOrderSummary = &summary
	return resp, nil
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

	ownerKey := strings.ToLower(strings.TrimSpace(actor.PubHex))
	candidates, err := dbListSpendableBSVUTXOs(ctx, store, ownerKey)
	if err != nil {
		return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "BALANCE_LOAD_FAILED", Message: err.Error(), FromAddress: fromAddress, ToAddress: toAddress, AmountSatoshi: req.AmountSatoshi}}, fmt.Errorf("load spendable plain bsv utxos failed: %w", err)
	}
	// 兼容现网历史数据：部分链上同步记录把 owner_pubkey_hex 落成了地址小写。
	// 业务仍以公钥为主键，但这里做一次地址回退，避免“余额在库里却选不到币”。
	addressKey := strings.ToLower(strings.TrimSpace(fromAddress))
	if len(candidates) == 0 && addressKey != "" && addressKey != ownerKey {
		fallback, fallbackErr := dbListSpendableBSVUTXOs(ctx, store, addressKey)
		if fallbackErr != nil {
			return walletBSVTransferPrepared{Result: WalletBSVTransferResult{Ok: false, Code: "BALANCE_LOAD_FAILED", Message: fallbackErr.Error(), FromAddress: fromAddress, ToAddress: toAddress, AmountSatoshi: req.AmountSatoshi}}, fmt.Errorf("load spendable plain bsv utxos by address fallback failed: %w", fallbackErr)
		}
		candidates = fallback
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
		previewHash := walletOrderPreviewHash(previewHashBytes)
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
