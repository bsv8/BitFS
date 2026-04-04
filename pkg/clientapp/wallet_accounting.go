package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// finBusinessEntry 业务记录写入条目
// 第六次迭代起只使用主口径字段
// 设计说明：
//   - 唯一口径：SourceType/SourceID/AccountingScene/AccountingSubType
//
// 第五阶段新增：BusinessRole 显式表达业务角色（formal | process）
type finBusinessEntry struct {
	BusinessID   string
	BusinessRole string // "formal" | "process"

	// 主口径 - 唯一模型字段
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubType string

	FromPartyID    string
	ToPartyID      string
	Status         string
	OccurredAtUnix int64
	IdempotencyKey string
	Note           string
	Payload        any
}

type finTxBreakdownEntry struct {
	BusinessID         string
	TxID               string
	TxRole             string
	GrossInputSatoshi  int64
	ChangeBackSatoshi  int64
	ExternalInSatoshi  int64
	CounterpartyOutSat int64
	MinerFeeSatoshi    int64
	NetOutSatoshi      int64
	NetInSatoshi       int64
	CreatedAtUnix      int64
	Note               string
	Payload            any
}

type finBusinessTxEntry struct {
	BusinessID    string
	TxID          string
	TxRole        string
	CreatedAtUnix int64
	Note          string
	Payload       any
}

type finTxUTXOLinkEntry struct {
	BusinessID    string
	TxID          string
	UTXOID        string
	IOSide        string
	UTXORole      string
	AmountSatoshi int64
	CreatedAtUnix int64
	Note          string
	Payload       any
}

// chainPaymentUTXOFact 是钱包链记账上游显式传入的 UTXO 明细。
// 设计说明：
//   - recordWalletChainAccounting 只负责落事实，不再根据汇总值猜测 link
//   - 这里只接能稳定识别出来的本地钱包 UTXO
type chainPaymentUTXOFact struct {
	UTXOID        string
	IOSide        string
	UTXORole      string
	AmountSatoshi int64
	Note          string
	Payload       any
}

// walletChainAccountingInput 是钱包链事实写入的显式输入。
// 设计说明：
//   - 交易汇总字段继续保留，用于 fact_chain_payments / settle_tx_breakdown
//   - UTXO 明细必须由上游显式传入，不能从 payload 临时猜
//   - ProcessEvents 允许在同一笔事务里一并落库
type walletChainAccountingInput struct {
	TxID            string
	Category        string
	WalletInputSat  int64
	WalletOutputSat int64
	NetSat          int64
	Payload         any
	UTXOFacts       []chainPaymentUTXOFact
	ProcessEvents   []finProcessEventEntry
}

// finProcessEventEntry 流程事件写入条目
// 第六次迭代起只使用主口径字段
// 设计说明：
//   - 唯一口径：SourceType/SourceID/AccountingScene/AccountingSubType
type finProcessEventEntry struct {
	ProcessID string

	// 主口径 - 唯一模型字段
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubType string

	EventType      string
	Status         string
	OccurredAtUnix int64
	IdempotencyKey string
	Note           string
	Payload        any
}

func mustJSONString(v any) string {
	if v == nil {
		return "{}"
	}
	if raw, ok := v.(rawJSONPayload); ok {
		s := strings.TrimSpace(string(raw))
		if s != "" {
			return s
		}
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

type feePoolOpenAccountingInput struct {
	BusinessID        string
	SpendTxID         string
	BaseTxID          string
	BaseTxHex         string
	ClientLockScript  string
	PoolAmountSatoshi uint64
	FromPartyID       string
	ToPartyID         string
}

type directPoolOpenAccountingInput struct {
	SessionID         string
	DealID            string
	BaseTxID          string
	BaseTxHex         string
	ClientLockScript  string
	PoolAmountSatoshi uint64
	SellerPubHex      string
}

func walletChainAccountingRoleAllowed(role string) bool {
	switch strings.TrimSpace(role) {
	case "wallet_input", "wallet_change", "external_in", "fee_pool_settle":
		return true
	default:
		return false
	}
}

func recordWalletChainAccountingConn(db sqlConn, in walletChainAccountingInput) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	txid := strings.ToLower(strings.TrimSpace(in.TxID))
	if txid == "" {
		return fmt.Errorf("txid is required")
	}

	paymentSubType := "unknown"
	status := "confirmed"
	fromParty := "wallet:self"
	toParty := "wallet:self"
	externalIn := int64(0)
	counterpartyOut := int64(0)
	netIn := int64(0)
	netOut := int64(0)
	changeBack := int64(0)

	switch strings.ToUpper(strings.TrimSpace(in.Category)) {
	case "CHANGE":
		paymentSubType = "internal_change"
		changeBack = in.WalletOutputSat
	case "REPAYMENT":
		paymentSubType = "external_in"
		externalIn = in.NetSat
		if externalIn < 0 {
			externalIn = 0
		}
		netIn = externalIn
		fromParty = "external:unknown"
	case "THIRD_PARTY":
		paymentSubType = "external_out"
		counterpartyOut = in.WalletInputSat - in.WalletOutputSat
		if counterpartyOut < 0 {
			counterpartyOut = -in.NetSat
		}
		if counterpartyOut < 0 {
			counterpartyOut = 0
		}
		netOut = counterpartyOut
		toParty = "external:unknown"
	case "FEE_POOL":
		paymentSubType = "fee_pool_settle"
		changeBack = in.WalletOutputSat
	default:
		if in.NetSat > 0 {
			paymentSubType = "external_in"
			externalIn = in.NetSat
			netIn = in.NetSat
			fromParty = "external:unknown"
		} else if in.NetSat < 0 {
			paymentSubType = "external_out"
			counterpartyOut = -in.NetSat
			netOut = -in.NetSat
			toParty = "external:unknown"
		}
	}
	now := time.Now().Unix()

	chainPaymentID, err := dbUpsertChainPaymentDB(db, chainPaymentEntry{
		TxID:                txid,
		PaymentSubType:      paymentSubType,
		Status:              status,
		WalletInputSatoshi:  in.WalletInputSat,
		WalletOutputSatoshi: in.WalletOutputSat,
		NetAmountSatoshi:    in.NetSat,
		BlockHeight:         0,
		OccurredAtUnix:      now,
		FromPartyID:         fromParty,
		ToPartyID:           toParty,
		Payload:             in.Payload,
	})
	if err != nil {
		obs.Error("bitcast-client", "wallet_accounting_chain_payment_failed", map[string]any{"error": err.Error(), "txid": txid})
		return fmt.Errorf("upsert chain_payment failed: %w", err)
	}

	// 第二步整改：source_type = "chain_payment", source_id = fact_chain_payments.id
	businessID := "biz_wallet_chain_" + txid
	sourceType := "chain_payment"
	sourceID := fmt.Sprintf("%d", chainPaymentID)

	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		BusinessRole:      "process", // 钱包过程财务对象
		SourceType:        sourceType,
		SourceID:          sourceID,
		AccountingScene:   "wallet_transfer",
		AccountingSubType: paymentSubType,
		FromPartyID:       fromParty,
		ToPartyID:         toParty,
		Status:            "posted",
		OccurredAtUnix:    now,
		IdempotencyKey:    "wallet_chain:" + txid,
		Note:              "wallet chain sync accounting",
		Payload:           in.Payload,
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_settle_businesses_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
		return fmt.Errorf("append settle_businesses failed: %w", err)
	}
	if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               txid,
		TxRole:             paymentSubType,
		GrossInputSatoshi:  in.WalletInputSat,
		ChangeBackSatoshi:  changeBack,
		ExternalInSatoshi:  externalIn,
		CounterpartyOutSat: counterpartyOut,
		MinerFeeSatoshi:    0,
		NetOutSatoshi:      netOut,
		NetInSatoshi:       netIn,
		Note:               "wallet chain derived breakdown",
		Payload:            in.Payload,
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
		return fmt.Errorf("append settle_tx_breakdown failed: %w", err)
	}

	for _, fact := range in.UTXOFacts {
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			return fmt.Errorf("utxo_id is required")
		}
		ioSide := strings.TrimSpace(fact.IOSide)
		utxoRole := strings.TrimSpace(fact.UTXORole)
		if ioSide == "" || utxoRole == "" {
			return fmt.Errorf("io_side and utxo_role are required")
		}
		if !walletChainAccountingRoleAllowed(utxoRole) {
			continue
		}
		exists, err := dbWalletUTXOExistsConn(db, utxoID)
		if err != nil {
			return fmt.Errorf("check wallet_utxo existence failed: %w", err)
		}
		if !exists {
			continue
		}
		if err := dbAppendChainPaymentUTXOLinkIfAbsentDB(db, chainPaymentUTXOLinkEntry{
			ChainPaymentID: chainPaymentID,
			UTXOID:         utxoID,
			IOSide:         ioSide,
			UTXORole:       utxoRole,
			AmountSatoshi:  fact.AmountSatoshi,
			CreatedAtUnix:  now,
			Note:           fact.Note,
			Payload:        fact.Payload,
		}); err != nil {
			return fmt.Errorf("append chain_payment_utxo_link failed: %w", err)
		}
	}

	// Step 4 出项关联：对 input UTXO 写入 fact_asset_consumptions
	// 设计说明：
	// - 只处理 input 方向的 UTXO，按 utxo_id 查 source_flow_id
	// - unknown UTXO 没有 source flow，跳过不产生消耗记录
	// - 幂等：同一 source_flow_id + chain_payment_id 不会重复写
	if err := dbAppendAssetConsumptionsForChainPayment(db, chainPaymentID, buildChainPaymentUTXOLinksFromFacts(in.UTXOFacts, now), now); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_asset_consumption_failed", map[string]any{"error": err.Error(), "txid": txid})
		return fmt.Errorf("append asset consumptions for chain payment failed: %w", err)
	}

	for _, e := range in.ProcessEvents {
		event := e
		if strings.TrimSpace(event.SourceType) == "" {
			event.SourceType = sourceType
		}
		if strings.TrimSpace(event.SourceID) == "" {
			event.SourceID = sourceID
		}
		if event.OccurredAtUnix <= 0 {
			event.OccurredAtUnix = now
		}
		if err := dbAppendFinProcessEvent(db, event); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_process_event_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
			return fmt.Errorf("append settle_process_events failed: %w", err)
		}
	}
	return nil
}

func recordWalletChainAccounting(db *sql.DB, in walletChainAccountingInput) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	// 先把事实放进同一笔事务里，避免主表成功但 link 或财务表缺一块。
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin wallet chain accounting tx failed: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback()
	}()

	// 先写链上支付事实主表，再补关系和财务表，全部在同一笔事务里。
	if err := recordWalletChainAccountingConn(tx, in); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit wallet chain accounting failed: %w", err)
	}
	committed = true
	return nil
}

// buildChainPaymentUTXOLinksFromFacts 把 UTXO facts 转换成 UTXO link entries
// 用于出项关联写入时复用同一数据结构
func buildChainPaymentUTXOLinksFromFacts(facts []chainPaymentUTXOFact, now int64) []chainPaymentUTXOLinkEntry {
	out := make([]chainPaymentUTXOLinkEntry, 0, len(facts))
	for _, f := range facts {
		out = append(out, chainPaymentUTXOLinkEntry{
			UTXOID:        strings.ToLower(strings.TrimSpace(f.UTXOID)),
			IOSide:        strings.TrimSpace(f.IOSide),
			UTXORole:      strings.TrimSpace(f.UTXORole),
			AmountSatoshi: f.AmountSatoshi,
			CreatedAtUnix: now,
			Note:          f.Note,
			Payload:       f.Payload,
		})
	}
	return out
}
