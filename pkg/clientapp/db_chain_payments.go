package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementchannelchainassetcreate"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementchannelchaindirectpay"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementchannelchainquotepay"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/walletutxo"
)

type sqlConn interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// SQLConn 是客户端运行时对外暴露的最小 SQL 能力。
// 设计说明：
// - 只暴露能力，不暴露具体实现；
// - 外部调用方只能拿这个能力做读写回调，不能直接碰 *sql.DB。
type SQLConn = sqlConn

// chainPaymentEntry fact_settlement_channel_chain_quote_pay 写入条目
// 设计说明：链支付事实只走显式业务提交入口，后续写入都靠事实表主键收口。
type chainPaymentEntry struct {
	TxID                 string
	PaymentSubType       string
	Status               string
	WalletInputSatoshi   int64
	WalletOutputSatoshi  int64
	NetAmountSatoshi     int64
	BlockHeight          int64
	OccurredAtUnix       int64
	SubmittedAtUnix      int64
	WalletObservedAtUnix int64
	FromPartyID          string
	ToPartyID            string
	Payload              any
}

// chainPaymentUTXOLinkEntry 统一结算链路的 UTXO 明细条目。
// 设计说明：这里是 chain_payment 事实的内部明细，不再依赖已下线的 settle tx 拆分表。
type chainPaymentUTXOLinkEntry struct {
	ChainPaymentID int64
	UTXOID         string
	IOSide         string
	UTXORole       string
	AssetKind      string
	TokenID        string
	TokenStandard  string
	AmountSatoshi  int64
	QuantityText   string
	CreatedAtUnix  int64
	Note           string
	Payload        any
}

// dbUpsertChainPayment 按 txid  upsert fact_settlement_channel_chain_quote_pay
// 同一个 txid 重复写入不会生成多条记录

// dbUpsertChainPaymentWithSettlementPaymentAttempt 走同一个上下文包装，但会补 settlement_payment_attempt。

// settlementPaymentAttemptStateForChainPayment 把 payment 状态收口到 settlement payment attempt 状态。
// 设计说明：
// - confirmed 才直接落 confirmed；
// - submitted / observed / 空值 先落 pending，等后续确认再升级；
// - failed 直接记 failed，避免把失败单误算成已确认扣账。
func settlementPaymentAttemptStateForChainPayment(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "confirmed":
		return "confirmed"
	case "failed":
		return "failed"
	default:
		return "pending"
	}
}

// dbUpsertChainPaymentEntTx 在 ent 事务内写链支付事实并回补 cycle。
func dbUpsertChainPaymentEntTx(ctx context.Context, tx EntWriteRoot, e chainPaymentEntry, writeSettlementPaymentAttempt bool) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return 0, fmt.Errorf("txid is required")
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	submittedAt := e.SubmittedAtUnix
	walletObservedAt := e.WalletObservedAtUnix

	// 先尝试查询已存在的记录
	existing, err := tx.FactSettlementChannelChainQuotePay.Query().
		Where(factsettlementchannelchainquotepay.TxidEQ(txid)).
		Only(ctx)
	if err == nil {
		existingSubmittedAt := existing.SubmittedAtUnix
		existingWalletObservedAt := existing.WalletObservedAtUnix
		if submittedAt < existingSubmittedAt {
			submittedAt = existingSubmittedAt
		}
		if walletObservedAt < existingWalletObservedAt {
			walletObservedAt = existingWalletObservedAt
		}
		// 存在则更新
		_, err = tx.FactSettlementChannelChainQuotePay.UpdateOneID(existing.ID).
			SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetWalletInputSatoshi(e.WalletInputSatoshi).
			SetWalletOutputSatoshi(e.WalletOutputSatoshi).
			SetNetAmountSatoshi(e.NetAmountSatoshi).
			SetBlockHeight(e.BlockHeight).
			SetOccurredAtUnix(occurredAt).
			SetSubmittedAtUnix(submittedAt).
			SetWalletObservedAtUnix(walletObservedAt).
			SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
			SetToPartyID(strings.TrimSpace(e.ToPartyID)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err != nil {
			return 0, err
		}
		if writeSettlementPaymentAttempt {
			settlementState := settlementPaymentAttemptStateForChainPayment(e.Status)
			update := tx.FactSettlementPaymentAttempts.UpdateOneID(existing.SettlementPaymentAttemptID).
				SetSourceType("chain_quote_pay").
				SetSourceID(fmt.Sprintf("%d", existing.ID)).
				SetState(settlementState).
				SetGrossAmountSatoshi(e.WalletInputSatoshi).
				SetGateFeeSatoshi(0).
				SetNetAmountSatoshi(e.NetAmountSatoshi).
				SetOccurredAtUnix(occurredAt).
				SetNote("auto-created from chain quote pay").
				SetPayloadJSON(mustJSONString(e.Payload))
			if settlementState == "confirmed" {
				update = update.SetConfirmedAtUnix(occurredAt)
			}
			if _, err := update.Save(ctx); err != nil {
				return 0, fmt.Errorf("update settlement payment attempt for existing chain quote pay %d: %w", existing.ID, err)
			}
		}
		return int64(existing.ID), nil
	}
	if err != nil && !gen.IsNotFound(err) {
		return 0, err
	}

	settlementState := settlementPaymentAttemptStateForChainPayment(e.Status)
	paymentAttemptToken := "pending:chain_quote_pay:" + txid
	settlementPaymentAttemptID, err := dbUpsertSettlementPaymentAttemptEntTx(ctx, tx,
		fmt.Sprintf("payment_attempt_chain_quote_pay_%s", txid),
		"chain_quote_pay",
		paymentAttemptToken,
		settlementState,
		e.WalletInputSatoshi, 0, e.NetAmountSatoshi,
		0, occurredAt, "pre-bind chain quote pay channel", e.Payload,
	)
	if err != nil {
		return 0, fmt.Errorf("create settlement payment attempt shell for chain quote pay: %w", err)
	}

	// 不存在则插入
	channel, err := tx.FactSettlementChannelChainQuotePay.Create().
		SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
		SetTxid(txid).
		SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
		SetStatus(strings.TrimSpace(e.Status)).
		SetWalletInputSatoshi(e.WalletInputSatoshi).
		SetWalletOutputSatoshi(e.WalletOutputSatoshi).
		SetNetAmountSatoshi(e.NetAmountSatoshi).
		SetBlockHeight(e.BlockHeight).
		SetOccurredAtUnix(occurredAt).
		SetSubmittedAtUnix(submittedAt).
		SetWalletObservedAtUnix(walletObservedAt).
		SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
		SetToPartyID(strings.TrimSpace(e.ToPartyID)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		SetUpdatedAtUnix(now).
		Save(ctx)
	if err != nil {
		return 0, err
	}
	paymentID := int64(channel.ID)

	if writeSettlementPaymentAttempt {
		if _, err := tx.FactSettlementPaymentAttempts.UpdateOneID(settlementPaymentAttemptID).
			SetSourceType("chain_quote_pay").
			SetSourceID(fmt.Sprintf("%d", paymentID)).
			SetNote("bind chain quote pay channel id").
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx); err != nil {
			return 0, fmt.Errorf("bind settlement payment attempt source id for chain quote pay %d: %w", paymentID, err)
		}
	}

	return paymentID, nil
}

func dbUpsertChainChannelWithSettlementPaymentAttempt(ctx context.Context, tx EntWriteRoot, e chainPaymentEntry, sourceType string, paymentAttemptPrefix string, bindNote string) (int64, int64, error) {
	if tx == nil {
		return 0, 0, fmt.Errorf("tx is nil")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return 0, 0, fmt.Errorf("txid is required")
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	submittedAt := e.SubmittedAtUnix
	walletObservedAt := e.WalletObservedAtUnix

	state := settlementPaymentAttemptStateForChainPayment(e.Status)
	paymentAttemptToken := "pending:" + sourceType + ":" + txid

	switch sourceType {
	case "chain_direct_pay":
		existing, err := tx.FactSettlementChannelChainDirectPay.Query().
			Where(factsettlementchannelchaindirectpay.TxidEQ(txid)).
			Only(ctx)
		if err == nil {
			if submittedAt < existing.SubmittedAtUnix {
				submittedAt = existing.SubmittedAtUnix
			}
			if walletObservedAt < existing.WalletObservedAtUnix {
				walletObservedAt = existing.WalletObservedAtUnix
			}
			if _, err := tx.FactSettlementChannelChainDirectPay.UpdateOneID(existing.ID).
				SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
				SetStatus(strings.TrimSpace(e.Status)).
				SetWalletInputSatoshi(e.WalletInputSatoshi).
				SetWalletOutputSatoshi(e.WalletOutputSatoshi).
				SetNetAmountSatoshi(e.NetAmountSatoshi).
				SetBlockHeight(e.BlockHeight).
				SetOccurredAtUnix(occurredAt).
				SetSubmittedAtUnix(submittedAt).
				SetWalletObservedAtUnix(walletObservedAt).
				SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
				SetToPartyID(strings.TrimSpace(e.ToPartyID)).
				SetPayloadJSON(mustJSONString(e.Payload)).
				SetUpdatedAtUnix(now).
				Save(ctx); err != nil {
				return 0, 0, err
			}
			update := tx.FactSettlementPaymentAttempts.UpdateOneID(existing.SettlementPaymentAttemptID).
				SetSourceType(sourceType).
				SetSourceID(fmt.Sprintf("%d", existing.ID)).
				SetState(state).
				SetGrossAmountSatoshi(e.WalletInputSatoshi).
				SetGateFeeSatoshi(0).
				SetNetAmountSatoshi(e.NetAmountSatoshi).
				SetOccurredAtUnix(occurredAt).
				SetNote(bindNote).
				SetPayloadJSON(mustJSONString(e.Payload))
			if state == "confirmed" {
				update = update.SetConfirmedAtUnix(occurredAt)
			}
			if _, err := update.Save(ctx); err != nil {
				return 0, 0, err
			}
			return int64(existing.ID), existing.SettlementPaymentAttemptID, nil
		}
		if err != nil && !gen.IsNotFound(err) {
			return 0, 0, err
		}
		settlementPaymentAttemptID, err := dbUpsertSettlementPaymentAttemptEntTx(ctx, tx,
			fmt.Sprintf("%s_%s", paymentAttemptPrefix, txid),
			sourceType, paymentAttemptToken, state,
			e.WalletInputSatoshi, 0, e.NetAmountSatoshi, 0, occurredAt, "pre-bind channel row", e.Payload,
		)
		if err != nil {
			return 0, 0, err
		}
		node, err := tx.FactSettlementChannelChainDirectPay.Create().
			SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
			SetTxid(txid).
			SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetWalletInputSatoshi(e.WalletInputSatoshi).
			SetWalletOutputSatoshi(e.WalletOutputSatoshi).
			SetNetAmountSatoshi(e.NetAmountSatoshi).
			SetBlockHeight(e.BlockHeight).
			SetOccurredAtUnix(occurredAt).
			SetSubmittedAtUnix(submittedAt).
			SetWalletObservedAtUnix(walletObservedAt).
			SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
			SetToPartyID(strings.TrimSpace(e.ToPartyID)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err != nil {
			return 0, 0, err
		}
		if _, err := tx.FactSettlementPaymentAttempts.UpdateOneID(settlementPaymentAttemptID).
			SetSourceType(sourceType).
			SetSourceID(fmt.Sprintf("%d", node.ID)).
			SetNote(bindNote).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx); err != nil {
			return 0, 0, err
		}
		return int64(node.ID), settlementPaymentAttemptID, nil
	case "chain_asset_create":
		existing, err := tx.FactSettlementChannelChainAssetCreate.Query().
			Where(factsettlementchannelchainassetcreate.TxidEQ(txid)).
			Only(ctx)
		if err == nil {
			if submittedAt < existing.SubmittedAtUnix {
				submittedAt = existing.SubmittedAtUnix
			}
			if walletObservedAt < existing.WalletObservedAtUnix {
				walletObservedAt = existing.WalletObservedAtUnix
			}
			if _, err := tx.FactSettlementChannelChainAssetCreate.UpdateOneID(existing.ID).
				SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
				SetStatus(strings.TrimSpace(e.Status)).
				SetWalletInputSatoshi(e.WalletInputSatoshi).
				SetWalletOutputSatoshi(e.WalletOutputSatoshi).
				SetNetAmountSatoshi(e.NetAmountSatoshi).
				SetBlockHeight(e.BlockHeight).
				SetOccurredAtUnix(occurredAt).
				SetSubmittedAtUnix(submittedAt).
				SetWalletObservedAtUnix(walletObservedAt).
				SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
				SetToPartyID(strings.TrimSpace(e.ToPartyID)).
				SetPayloadJSON(mustJSONString(e.Payload)).
				SetUpdatedAtUnix(now).
				Save(ctx); err != nil {
				return 0, 0, err
			}
			update := tx.FactSettlementPaymentAttempts.UpdateOneID(existing.SettlementPaymentAttemptID).
				SetSourceType(sourceType).
				SetSourceID(fmt.Sprintf("%d", existing.ID)).
				SetState(state).
				SetGrossAmountSatoshi(e.WalletInputSatoshi).
				SetGateFeeSatoshi(0).
				SetNetAmountSatoshi(e.NetAmountSatoshi).
				SetOccurredAtUnix(occurredAt).
				SetNote(bindNote).
				SetPayloadJSON(mustJSONString(e.Payload))
			if state == "confirmed" {
				update = update.SetConfirmedAtUnix(occurredAt)
			}
			if _, err := update.Save(ctx); err != nil {
				return 0, 0, err
			}
			return int64(existing.ID), existing.SettlementPaymentAttemptID, nil
		}
		if err != nil && !gen.IsNotFound(err) {
			return 0, 0, err
		}
		settlementPaymentAttemptID, err := dbUpsertSettlementPaymentAttemptEntTx(ctx, tx,
			fmt.Sprintf("%s_%s", paymentAttemptPrefix, txid),
			sourceType, paymentAttemptToken, state,
			e.WalletInputSatoshi, 0, e.NetAmountSatoshi, 0, occurredAt, "pre-bind channel row", e.Payload,
		)
		if err != nil {
			return 0, 0, err
		}
		node, err := tx.FactSettlementChannelChainAssetCreate.Create().
			SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
			SetTxid(txid).
			SetPaymentSubtype(strings.TrimSpace(e.PaymentSubType)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetWalletInputSatoshi(e.WalletInputSatoshi).
			SetWalletOutputSatoshi(e.WalletOutputSatoshi).
			SetNetAmountSatoshi(e.NetAmountSatoshi).
			SetBlockHeight(e.BlockHeight).
			SetOccurredAtUnix(occurredAt).
			SetSubmittedAtUnix(submittedAt).
			SetWalletObservedAtUnix(walletObservedAt).
			SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
			SetToPartyID(strings.TrimSpace(e.ToPartyID)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err != nil {
			return 0, 0, err
		}
		if _, err := tx.FactSettlementPaymentAttempts.UpdateOneID(settlementPaymentAttemptID).
			SetSourceType(sourceType).
			SetSourceID(fmt.Sprintf("%d", node.ID)).
			SetNote(bindNote).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx); err != nil {
			return 0, 0, err
		}
		return int64(node.ID), settlementPaymentAttemptID, nil
	default:
		return 0, 0, fmt.Errorf("unsupported chain channel source_type: %s", sourceType)
	}
}

// dbGetChainPaymentByTxID 按 txid 查 fact_settlement_channel_chain_quote_pay
func dbGetChainPaymentByTxID(ctx context.Context, store *clientDB, txid string) (int64, error) {
	if store == nil {
		return 0, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (int64, error) {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			return 0, fmt.Errorf("txid is required")
		}
		node, err := root.FactSettlementChannelChainQuotePay.Query().
			Where(factsettlementchannelchainquotepay.TxidEQ(txid)).
			Only(ctx)
		if err != nil {
			return 0, err
		}
		return int64(node.ID), nil
	})
}

// dbGetChainPaymentByID 按 id 查 fact_settlement_channel_chain_quote_pay（验证存在性）

func dbWalletUTXOValueConn(ctx context.Context, tx EntWriteRoot, utxoID string) (int64, bool, error) {
	if tx == nil {
		return 0, false, fmt.Errorf("tx is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return 0, false, fmt.Errorf("utxo_id is required")
	}
	row, err := tx.WalletUtxo.Query().
		Where(walletutxo.UtxoIDEQ(utxoID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return row.ValueSatoshi, true, nil
}

// recordChainPaymentAccountingAfterBroadcast 业务提交成功后，一次写完链支付已知财务内容。
// 设计说明：
// - 这里不等钱包同步，不走旧的事后回写；
// - 只接受已经广播成功的真实交易；
// - 输入金额来自 wallet_utxo，输出角色来自交易本身。
func recordChainPaymentAccountingAfterBroadcast(ctx context.Context, store ClientStore, rt *Runtime, txHex string, txID string, paymentSubType string, processSubType string, fromPartyID string, toPartyID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if rt == nil {
		return fmt.Errorf("runtime not initialized")
	}
	txHex = strings.ToLower(strings.TrimSpace(txHex))
	txID = strings.ToLower(strings.TrimSpace(txID))
	if txHex == "" {
		return fmt.Errorf("tx_hex is required")
	}
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	walletAddr, err := clientWalletAddress(rt)
	if err != nil {
		return err
	}
	walletAddr = strings.TrimSpace(walletAddr)
	if walletAddr == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletScriptHex, err := walletAddressLockScriptHex(walletAddr)
	if err != nil {
		return err
	}
	parsed, err := txsdk.NewTransactionFromHex(txHex)
	if err != nil {
		return fmt.Errorf("parse tx hex: %w", err)
	}
	now := time.Now().Unix()
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		inputFacts, grossInput, err := collectWalletInputFactsTx(ctx, tx, parsed, txID, "chain_payment input")
		if err != nil {
			return err
		}
		if len(inputFacts) == 0 {
			return fmt.Errorf("no wallet input facts found for txid %s", txID)
		}
		changeBack := int64(0)
		counterpartyOut := int64(0)
		for _, out := range parsed.Outputs {
			if out == nil {
				continue
			}
			amount := int64(out.Satoshis)
			if amount <= 0 {
				continue
			}
			outScriptHex := strings.ToLower(strings.TrimSpace(hex.EncodeToString(out.LockingScript.Bytes())))
			if outScriptHex != "" && walletScriptHexMatchesAddressControl(outScriptHex, walletScriptHex) {
				changeBack += amount
				continue
			}
			counterpartyOut += amount
		}
		minerFee := grossInput - changeBack - counterpartyOut
		if minerFee < 0 {
			minerFee = 0
		}
		netAmount := changeBack - grossInput
		channelID, err := dbUpsertChainPaymentEntTx(ctx, tx, chainPaymentEntry{
			TxID:                 txID,
			PaymentSubType:       paymentSubType,
			Status:               "confirmed",
			WalletInputSatoshi:   grossInput,
			WalletOutputSatoshi:  changeBack,
			NetAmountSatoshi:     netAmount,
			OccurredAtUnix:       now,
			SubmittedAtUnix:      now,
			WalletObservedAtUnix: now,
			FromPartyID:          strings.TrimSpace(fromPartyID),
			ToPartyID:            strings.TrimSpace(toPartyID),
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}, true)
		if err != nil {
			return fmt.Errorf("upsert chain payment failed: %w", err)
		}
		settlementPaymentAttemptID, err := dbGetSettlementPaymentAttemptBySourceEntTx(ctx, tx, "chain_quote_pay", fmt.Sprintf("%d", channelID))
		if err != nil {
			return fmt.Errorf("resolve settlement payment attempt for chain payment: %w", err)
		}
		businessID := "biz_chain_payment_" + txID
		if err := dbAppendSettlementPaymentAttemptFinBusiness(ctx, tx, settlementPaymentAttemptID, finBusinessEntry{
			OrderID:           businessID,
			BusinessRole:      "process",
			AccountingScene:   "peer_call",
			AccountingSubType: strings.TrimSpace(processSubType),
			FromPartyID:       strings.TrimSpace(fromPartyID),
			ToPartyID:         strings.TrimSpace(toPartyID),
			Status:            "posted",
			OccurredAtUnix:    now,
			IdempotencyKey:    "chain_payment:" + txID,
			Note:              "chain payment broadcast accounting",
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}); err != nil {
			return fmt.Errorf("append order settlement failed: %w", err)
		}
		// 旧 tx 拆解/UTXO 明细层已下线，这里只保留 order_settlements、chain_payment 和流程事件。
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, tx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         "proc_chain_payment_" + txID,
			AccountingScene:   "peer_call",
			AccountingSubType: strings.TrimSpace(processSubType),
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    now,
			IdempotencyKey:    "chain_payment_event:" + txID,
			Note:              "chain payment accounting event",
			Payload: map[string]any{
				"txid":            txID,
				"payment_subtype": paymentSubType,
				"process_subtype": processSubType,
			},
		}); err != nil {
			return fmt.Errorf("append order_settlement_events failed: %w", err)
		}
		return nil
	})
}

func collectWalletInputFactsTx(ctx context.Context, tx EntWriteRoot, txObj *txsdk.Transaction, txID string, note string) ([]string, int64, error) {
	if tx == nil {
		return nil, 0, fmt.Errorf("tx is nil")
	}
	if txObj == nil {
		return nil, 0, fmt.Errorf("transaction is nil")
	}
	out := make([]string, 0, len(txObj.Inputs))
	var gross int64
	for _, inp := range txObj.Inputs {
		if inp == nil || inp.SourceTXID == nil {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(inp.SourceTXID.String())) + ":" + fmt.Sprint(inp.SourceTxOutIndex)
		value, ok, err := dbWalletUTXOValueConn(ctx, tx, utxoID)
		if err != nil {
			return nil, 0, fmt.Errorf("lookup wallet input value for %s failed: %w", utxoID, err)
		}
		if !ok {
			return nil, 0, fmt.Errorf("wallet input value missing for %s", utxoID)
		}
		gross += value
		out = append(out, utxoID)
	}
	return out, gross, nil
}
