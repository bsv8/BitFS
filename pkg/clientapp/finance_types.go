package clientapp

import (
	"encoding/json"
	"strings"
)

// finBusinessEntry 业务结算事实写入条目。
// 设计说明：
// - 这一条同时承载业务主事实和结算出口；
// - 调用方必须显式提供业务身份、来源、结算方式和目标；
// - 不在这里猜业务类型，也不在这里补旧账。
type finBusinessEntry struct {
	OrderID      string
	BusinessRole string // formal | process

	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubType string

	FromPartyID string
	ToPartyID   string
	Status      string

	OccurredAtUnix int64
	IdempotencyKey string
	Note           string
	Payload        any

	SettlementID           string
	SettlementMethod       string // pool | chain
	SettlementStatus       string
	SettlementTargetType   string
	SettlementTargetID     string
	SettlementErrorMessage string
	SettlementPayload      any
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
