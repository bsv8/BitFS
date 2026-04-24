package moduleapi

import (
	"context"
)

// QuotedServicePayer is for remote paid call scenarios:
// - resolve_call domain resolution paid call
// - domain/gateway paid call
//
// Design:
// - Business layer calls facade only, not directly importing payment module;
// - Module implements quote/pay/retry/receipt flow;
// - Idempotency key implemented by module, not via global settlement attempt.
type QuotedServicePayer interface {
	PayQuotedService(ctx context.Context, scheme string, quote []byte, request ServiceCallRequest, targetPeer string) (ServiceCallResponse, []byte, error)
}

// ServiceCoverageSession is for listen renew scenarios:
// - listen renew periodic billing
// - pool rotation
// - service coverage state management
type ServiceCoverageSession interface {
	EnsureCoverage(ctx context.Context, scheme string, targetPeer string, requiredDuration uint64) (sessionRef string, coverageExpiresAt int64, err error)
	RenewCoverage(ctx context.Context, sessionRef string, additionalDuration uint64) (int64, error)
	RotateCoverage(ctx context.Context, scheme string, sessionRef string, newAmount uint64) (string, error)
	CloseCoverage(ctx context.Context, sessionRef string, reason string) error
	OnCoverageStateChange(callback func(sessionRef, oldState, newState string, timestamp int64))
}

// TradeSession is for c2c file purchase/chunk payment/arbitration scenarios:
// - pool_2of3_v1 module registers this
// - Module manages open pool, chunk payment, close, arbitration
type TradeSession interface {
	OpenTradeSession(ctx context.Context, scheme string, tradeID string, buyer, seller, arbiter string, totalAmount uint64, chunkCount uint32) (sessionRef string, initialEntry TradeEntry, err error)
	AppendTradeEntry(ctx context.Context, sessionRef string, entry TradeEntry) (TradeEntry, error)
	OpenArbitration(ctx context.Context, sessionRef string, chunkIndex uint32, evidence string) (caseID string, err error)
	CloseTradeSession(ctx context.Context, sessionRef string, finalEntry TradeEntry) (SettlementResult, error)
	OnTradeStateChange(callback func(sessionRef, oldState, newState string, timestamp int64))
}

// ServiceCallRequest is the raw service call request.
type ServiceCallRequest struct {
	Route       string
	Body        []byte
	ContentType string
}

// ServiceCallResponse is the raw service call response.
type ServiceCallResponse struct {
	OK          bool
	Code        string
	Message     string
	ContentType string
	Body        []byte
}

// TradeEntry is a chunk payment entry.
type TradeEntry struct {
	ChunkIndex    uint32
	AmountSatoshi uint64
	State         string

	PaymentTxID     string
	PaymentAtUnix   int64
	MerkleProof     []byte

	ArbitrationCaseID string
	ArbitratedAtUnix  int64
	AwardTxID        string
}

// SettlementResult is the settlement result when trade session closes.
type SettlementResult struct {
	TotalPaidSatoshi     uint64
	TotalRefundedSatoshi uint64
	FinalTxID            string
	ClosedAtUnix         int64
}

// PaymentReceipt is the common interface for payment receipts.
type PaymentReceipt interface {
	Scheme() string
	Bytes() []byte
}

// CoverageState is the service coverage session state.
type CoverageState string

const (
	CoverageStateInactive  CoverageState = "inactive"
	CoverageStateActive    CoverageState = "active"
	CoverageStateRotating  CoverageState = "rotating"
	CoverageStateClosing   CoverageState = "closing"
	CoverageStateClosed    CoverageState = "closed"
)

// TradeState is the trade session state.
type TradeState string

const (
	TradeStateOpen        TradeState = "open"
	TradeStateInProgress  TradeState = "in_progress"
	TradeStateArbitrating TradeState = "arbitrating"
	TradeStateCompleted   TradeState = "completed"
	TradeStateAborted     TradeState = "aborted"
)
