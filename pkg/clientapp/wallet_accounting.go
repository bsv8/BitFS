package clientapp

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// finBusinessEntry 业务记录写入条目
// 第六次迭代起只使用主口径字段
// 设计说明：
//   - 唯一口径：SourceType/SourceID/AccountingScene/AccountingSubType
type finBusinessEntry struct {
	BusinessID string

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

func recordWalletChainAccounting(db *sql.DB, txid string, category string, walletInSat int64, walletOutSat int64, netSat int64, payload map[string]any) {
	if db == nil {
		return
	}
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return
	}
	businessID := "biz_wallet_chain_" + txid
	sceneSubType := "internal"
	fromParty := "wallet:self"
	toParty := "wallet:self"
	externalIn := int64(0)
	counterpartyOut := int64(0)
	netIn := int64(0)
	netOut := int64(0)
	changeBack := int64(0)

	switch strings.ToUpper(strings.TrimSpace(category)) {
	case "CHANGE":
		sceneSubType = "internal_change"
		changeBack = walletOutSat
	case "REPAYMENT":
		sceneSubType = "external_in"
		externalIn = netSat
		if externalIn < 0 {
			externalIn = 0
		}
		netIn = externalIn
		fromParty = "external:unknown"
	case "THIRD_PARTY":
		sceneSubType = "external_out"
		counterpartyOut = walletInSat - walletOutSat
		if counterpartyOut < 0 {
			counterpartyOut = -netSat
		}
		if counterpartyOut < 0 {
			counterpartyOut = 0
		}
		netOut = counterpartyOut
		toParty = "external:unknown"
	case "FEE_POOL":
		sceneSubType = "fee_pool_settle"
		changeBack = walletOutSat
	default:
		if netSat > 0 {
			sceneSubType = "external_in"
			externalIn = netSat
			netIn = netSat
			fromParty = "external:unknown"
		} else if netSat < 0 {
			sceneSubType = "external_out"
			counterpartyOut = -netSat
			netOut = -netSat
			toParty = "external:unknown"
		}
	}

	// 收口标记：wallet_chain 的 source_type 当前是抽象业务名
	// 待办：当 wallet_chain 事实层设计完成后，应指向明确的事实实体
	// source_type/source_id 原则：只允许表示"真实事实来源"，不兼任业务分类
	if err := dbAppendFinBusiness(db, finBusinessEntry{
		BusinessID:        businessID,
		SourceType:        "wallet_chain",
		SourceID:          txid,
		AccountingScene:   "wallet_transfer",
		AccountingSubType: sceneSubType,
		FromPartyID:       fromParty,
		ToPartyID:         toParty,
		Status:            "posted",
		OccurredAtUnix:    time.Now().Unix(),
		IdempotencyKey:    "wallet_chain:" + txid,
		Note:              "wallet chain sync accounting",
		Payload:           payload,
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
		return
	}
	if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
		BusinessID:         businessID,
		TxID:               txid,
		TxRole:             sceneSubType,
		GrossInputSatoshi:  walletInSat,
		ChangeBackSatoshi:  changeBack,
		ExternalInSatoshi:  externalIn,
		CounterpartyOutSat: counterpartyOut,
		MinerFeeSatoshi:    0,
		NetOutSatoshi:      netOut,
		NetInSatoshi:       netIn,
		Note:               "wallet chain derived breakdown",
		Payload:            payload,
	}); err != nil {
		obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "wallet_chain", "txid": txid})
	}
}
