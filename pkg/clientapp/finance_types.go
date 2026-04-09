package clientapp

import (
	"encoding/json"
	"strings"
)

// finBusinessEntry 业务财务写入条目。
// 设计说明：
// - 这是业务主线的统一写入模型；
// - 调用方必须显式提供业务身份、来源和结算语义；
// - 不在这里猜业务类型，也不在这里补旧账。
type finBusinessEntry struct {
	BusinessID   string
	BusinessRole string // formal | process

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

// finTxBreakdownEntry 交易拆解写入条目。
// 设计说明：
// - 只保存提交成功时已经能确定的金额拆分；
// - gross/change/counterparty/fee 都必须在业务提交点算清。
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

// finTxUTXOLinkEntry 交易与 UTXO 的关系写入条目。
// 设计说明：
// - 只描述事实，不推断业务；
// - 角色和方向由真实提交时的交易结构决定。
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

// finProcessEventEntry 流程事件写入条目。
// 设计说明：
// - 用于和业务主线同一时刻落账；
// - 不接受 wallet_chain 之类的旧来源口径。
type finProcessEventEntry struct {
	ProcessID string

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

// feePoolOpenAccountingInput 费用池 open 过程的提交时参数。
// 设计说明：这里只保留提交当下可确定的信息，不承载后置回写职责。
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

// directPoolOpenAccountingInput 直连池 open 过程的提交时参数。
// 设计说明：只作为真实业务入口的输入载体，不再给钱包观察层复用。
type directPoolOpenAccountingInput struct {
	SessionID         string
	DealID            string
	BaseTxID          string
	BaseTxHex         string
	ClientLockScript  string
	PoolAmountSatoshi uint64
	SellerPubHex      string
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
