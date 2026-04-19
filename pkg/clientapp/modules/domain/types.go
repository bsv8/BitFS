package domain

import (
	"context"
	"time"
)

// DomainRegisterNameParams 对应域名注册入口入参。
type TriggerDomainRegisterNameParams struct {
	ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
	ResolverAddr      string `json:"resolver_addr,omitempty"`
	Name              string `json:"name"`
	TargetPubkeyHex   string `json:"target_pubkey_hex"`
}

type TriggerDomainRegisterNameResult struct {
	Ok                         bool   `json:"ok"`
	Code                       string `json:"code"`
	Message                    string `json:"message,omitempty"`
	FrontOrderID               string `json:"front_order_id,omitempty"`
	Name                       string `json:"name,omitempty"`
	OwnerPubkeyHex             string `json:"owner_pubkey_hex,omitempty"`
	TargetPubkeyHex            string `json:"target_pubkey_hex,omitempty"`
	ExpireAtUnix               int64  `json:"expire_at_unix,omitempty"`
	LockExpiresAtUnix          int64  `json:"lock_expires_at_unix,omitempty"`
	RegisterTxID               string `json:"register_txid,omitempty"`
	QueryFeeChargedSatoshi     uint64 `json:"query_fee_charged_satoshi,omitempty"`
	RegisterLockChargedSatoshi uint64 `json:"register_lock_charged_satoshi,omitempty"`
	RegisterPriceSatoshi       uint64 `json:"register_price_satoshi,omitempty"`
	RegisterSubmitFeeSatoshi   uint64 `json:"register_submit_fee_satoshi,omitempty"`
	TotalRegisterPaySatoshi    uint64 `json:"total_register_pay_satoshi,omitempty"`
}

type TriggerDomainRegisterLockParams struct {
	ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
	ResolverAddr      string `json:"resolver_addr,omitempty"`
	Name              string `json:"name"`
	TargetPubkeyHex   string `json:"target_pubkey_hex"`
}

type TriggerDomainRegisterLockResult struct {
	Ok                         bool   `json:"ok"`
	Code                       string `json:"code"`
	Message                    string `json:"message,omitempty"`
	Name                       string `json:"name,omitempty"`
	OwnerPubkeyHex             string `json:"owner_pubkey_hex,omitempty"`
	TargetPubkeyHex            string `json:"target_pubkey_hex,omitempty"`
	LockExpiresAtUnix          int64  `json:"lock_expires_at_unix,omitempty"`
	QueryFeeChargedSatoshi     uint64 `json:"query_fee_charged_satoshi,omitempty"`
	RegisterLockChargedSatoshi uint64 `json:"register_lock_charged_satoshi,omitempty"`
	RegisterPriceSatoshi       uint64 `json:"register_price_satoshi,omitempty"`
	RegisterSubmitFeeSatoshi   uint64 `json:"register_submit_fee_satoshi,omitempty"`
	TotalRegisterPaySatoshi    uint64 `json:"total_register_pay_satoshi,omitempty"`
}

type TriggerDomainPrepareRegisterParams struct {
	ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
	ResolverAddr      string `json:"resolver_addr,omitempty"`
	Name              string `json:"name"`
	TargetPubkeyHex   string `json:"target_pubkey_hex"`
}

type TriggerDomainPrepareRegisterResult struct {
	Ok                         bool   `json:"ok"`
	Code                       string `json:"code"`
	Message                    string `json:"message,omitempty"`
	Name                       string `json:"name,omitempty"`
	OwnerPubkeyHex             string `json:"owner_pubkey_hex,omitempty"`
	TargetPubkeyHex            string `json:"target_pubkey_hex,omitempty"`
	LockExpiresAtUnix          int64  `json:"lock_expires_at_unix,omitempty"`
	RegisterTxID               string `json:"register_txid,omitempty"`
	RegisterTxHex              string `json:"register_tx_hex,omitempty"`
	QueryFeeChargedSatoshi     uint64 `json:"query_fee_charged_satoshi,omitempty"`
	RegisterLockChargedSatoshi uint64 `json:"register_lock_charged_satoshi,omitempty"`
	RegisterPriceSatoshi       uint64 `json:"register_price_satoshi,omitempty"`
	RegisterSubmitFeeSatoshi   uint64 `json:"register_submit_fee_satoshi,omitempty"`
	TotalRegisterPaySatoshi    uint64 `json:"total_register_pay_satoshi,omitempty"`
}

type TriggerDomainSubmitPreparedRegisterParams struct {
	ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
	ResolverAddr      string `json:"resolver_addr,omitempty"`
	RegisterTxHex     string `json:"register_tx_hex"`
}

type TriggerDomainSubmitPreparedRegisterResult struct {
	Ok              bool   `json:"ok"`
	Code            string `json:"code"`
	Message         string `json:"message,omitempty"`
	Name            string `json:"name,omitempty"`
	OwnerPubkeyHex  string `json:"owner_pubkey_hex,omitempty"`
	TargetPubkeyHex string `json:"target_pubkey_hex,omitempty"`
	ExpireAtUnix    int64  `json:"expire_at_unix,omitempty"`
	RegisterTxID    string `json:"register_txid,omitempty"`
	RegisterTxHex   string `json:"register_tx_hex,omitempty"`
}

type TriggerDomainSetTargetParams struct {
	ResolverPubkeyHex string `json:"resolver_pubkey_hex"`
	ResolverAddr      string `json:"resolver_addr,omitempty"`
	Name              string `json:"name"`
	TargetPubkeyHex   string `json:"target_pubkey_hex"`
}

type TriggerDomainSetTargetResult struct {
	Ok                      bool   `json:"ok"`
	Code                    string `json:"code"`
	Message                 string `json:"message,omitempty"`
	Name                    string `json:"name,omitempty"`
	OwnerPubkeyHex          string `json:"owner_pubkey_hex,omitempty"`
	TargetPubkeyHex         string `json:"target_pubkey_hex,omitempty"`
	ExpireAtUnix            int64  `json:"expire_at_unix,omitempty"`
	QueryFeeChargedSatoshi  uint64 `json:"query_fee_charged_satoshi,omitempty"`
	SetTargetChargedSatoshi uint64 `json:"set_target_charged_satoshi,omitempty"`
	TotalChargedSatoshi     uint64 `json:"total_charged_satoshi,omitempty"`
}

type DomainQueryResponse struct {
	Success           bool
	Status            string
	Error             string
	ChargedAmount     uint64
	Available         bool
	Registered        bool
	Locked            bool
	LockExpiresAtUnix int64
	RegisterLockFee   uint64
	SetTargetFee      uint64
	OwnerPubkeyHex    string
	ExpireAtUnix      int64
}

type DomainRegisterLockResponse struct {
	Success           bool
	Status            string
	Error             string
	ChargedAmount     uint64
	LockExpiresAtUnix int64
	SignedQuoteJSON   []byte
}

type DomainRegisterSubmitResponse struct {
	Success         bool
	Status          string
	Error           string
	Name            string
	OwnerPubkeyHex  string
	TargetPubkeyHex string
	ExpireAtUnix    int64
	RegisterTxID    string
}

type DomainSetTargetResponse struct {
	Success         bool
	Status          string
	Error           string
	ChargedAmount   uint64
	Name            string
	OwnerPubkeyHex  string
	TargetPubkeyHex string
	ExpireAtUnix    int64
}

// ResolveResult 表示域名解析结果。
// 这里只保留最核心的公钥 hex，不把协议壳做厚。
type ResolveResult struct {
	Domain    string `json:"domain"`
	PubkeyHex string `json:"pubkey_hex"`
	Provider  string `json:"provider,omitempty"`
}

// Resolver 是最小解析能力。
type Resolver interface {
	ResolveDomainToPubkey(ctx context.Context, domain string) (string, error)
}

// Provider 是可注册到基座的解析钩子。
type Provider interface {
	ResolveDomainToPubkey(ctx context.Context, domain string) (string, error)
}

type Hook func(context.Context, string) (string, error)

type DomainRegisterQuote struct {
	QuoteID                  string
	Name                     string
	OwnerPubkeyHex           string
	TargetPubkeyHex          string
	PayToAddress             string
	RegisterPriceSatoshi     uint64
	RegisterSubmitFeeSatoshi uint64
	TotalPaySatoshi          uint64
	LockExpiresAtUnix        int64
	TermSeconds              uint64
}

type BuiltDomainRegisterTx struct {
	RawTx           []byte
	TxID            string
	MinerFeeSatoshi uint64
	ChangeSatoshi   uint64
}

type FrontOrderEntry struct {
	FrontOrderID      string
	FrontType         string
	FrontSubtype      string
	OwnerPubkeyHex    string
	TargetObjectType  string
	TargetObjectID    string
	FrontOrderNote    string
	FrontOrderPayload any
	CreatedAtUnix     int64
	UpdatedAtUnix     int64
	Status            string
}

type BusinessEntry struct {
	BusinessID        string
	BusinessRole      string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubType string
	FromPartyID       string
	ToPartyID         string
	BusinessNote      string
	BusinessPayload   any
	CreatedAtUnix     int64
	UpdatedAtUnix     int64
	Status            string
}

type BusinessSettlementEntry struct {
	SettlementID           string
	BusinessID             string
	SettlementMethod       string
	Status                 string
	SettlementTargetType   string
	SettlementTargetID     string
	SettlementErrorMessage string
	SettlementPayload      any
	CreatedAtUnix          int64
	UpdatedAtUnix          int64
}

type BusinessTriggerEntry struct {
	TriggerID      string
	BusinessID     string
	SettlementID   string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
	TriggerNote    string
	TriggerPayload any
	CreatedAtUnix  int64
}

type CreateBusinessWithFrontTriggerAndPendingSettlementInput struct {
	FrontOrderID         string
	FrontType            string
	FrontSubtype         string
	OwnerPubkeyHex       string
	TargetObjectType     string
	TargetObjectID       string
	FrontOrderNote       string
	FrontOrderPayload    any
	BusinessID           string
	BusinessRole         string
	SourceType           string
	SourceID             string
	AccountingScene      string
	AccountingSubType    string
	FromPartyID          string
	ToPartyID            string
	BusinessNote         string
	BusinessPayload      any
	TriggerID            string
	TriggerType          string
	TriggerIDValue       string
	TriggerRole          string
	TriggerNote          string
	TriggerPayload       any
	SettlementID         string
	SettlementMethod     SettlementMethod
	SettlementTargetType string
	SettlementTargetID   string
	SettlementPayload    any
}

type SettlementMethod string

const (
	SettlementMethodPool  SettlementMethod = "pool"
	SettlementMethodChain SettlementMethod = "chain"
)

func (m SettlementMethod) Valid() error {
	switch m {
	case SettlementMethodPool, SettlementMethodChain:
		return nil
	default:
		return NewError(CodeBadRequest, "invalid settlement_method: "+string(m))
	}
}

func nowUnix() int64 { return time.Now().Unix() }
