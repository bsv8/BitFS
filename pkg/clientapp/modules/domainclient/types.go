package domainclient

import "context"

// 业务主流程入参和返回值。
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

type ResolveResult struct {
	Domain    string `json:"domain"`
	PubkeyHex string `json:"pubkey_hex"`
	Provider  string `json:"provider,omitempty"`
}

type Resolver interface {
	ResolveDomainToPubkeyDirect(ctx context.Context, domain string) (string, error)
}

type Provider interface {
	ResolveDomainToPubkeyDirect(ctx context.Context, domain string) (string, error)
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

type BusinessSettlementEntry struct {
	SettlementID         string
	SettlementMethod     string
	SettlementTargetType string
	SettlementTargetID   string
	SettlementPayload    any
	CreatedAtUnix        int64
	UpdatedAtUnix        int64
	Status               string
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
	SettlementMethod     string
	SettlementTargetType string
	SettlementTargetID   string
	SettlementPayload    any
}

type FrontOrderSettlementSummary struct {
	FrontOrderID string                      `json:"front_order_id"`
	Businesses   []BusinessSettlementSummary `json:"businesses"`
	Summary      SettlementTotalSummary      `json:"summary"`
}

type BusinessSettlementSummary struct {
	BusinessID           string `json:"business_id"`
	SellerPubHex         string `json:"seller_pub_hex"`
	TotalTargetSatoshi   uint64 `json:"total_target_satoshi"`
	SettledAmountSatoshi uint64 `json:"settled_amount_satoshi"`
	PendingAmountSatoshi uint64 `json:"pending_amount_satoshi"`
}

type SettlementTotalSummary struct {
	OverallStatus        string `json:"overall_status"`
	TotalTargetSatoshi   uint64 `json:"total_target_satoshi"`
	SettledAmountSatoshi uint64 `json:"settled_amount_satoshi"`
	PendingAmountSatoshi uint64 `json:"pending_amount_satoshi"`
}
