package domainclient

import (
	"context"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// BusinessStore 只承载域名业务链落库能力。
type BusinessStore interface {
	CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, input CreateBusinessWithFrontTriggerAndPendingSettlementInput) error
	UpsertFrontOrder(ctx context.Context, entry FrontOrderEntry) error
	UpsertBusiness(ctx context.Context, entry BusinessEntry) error
	UpsertBusinessSettlement(ctx context.Context, entry BusinessSettlementEntry) error
	AppendBusinessTrigger(ctx context.Context, entry BusinessTriggerEntry) error
	GetChainPaymentByTxID(ctx context.Context, txID string) (int64, error)
	UpdateOrderSettlement(ctx context.Context, settlementID, status, targetType, targetID, errMsg string, updatedAtUnix int64) error
}

// RuntimePorts 只承载域名流程运行时能力。
type RuntimePorts interface {
	ClientID(ctx context.Context) (string, error)
	EnsureDomainPeerConnected(ctx context.Context, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error)
	TriggerDomainQueryName(ctx context.Context, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (DomainQueryResponse, error)
	TriggerDomainRegisterLock(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, registerLockFee uint64) (DomainRegisterLockResponse, error)
	VerifyRegisterQuote(resolverPubkeyHex string, raw []byte) (DomainRegisterQuote, error)
	BuildDomainRegisterTx(ctx context.Context, signedQuoteJSON []byte, quote DomainRegisterQuote) (BuiltDomainRegisterTx, error)
	TriggerDomainRegisterSubmit(ctx context.Context, resolverPubkeyHex string, registerTx []byte) (DomainRegisterSubmitResponse, error)
	TriggerDomainSetTarget(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, setTargetFee uint64) (DomainSetTargetResponse, error)
	ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error
	RecordChainPaymentAccountingAfterBroadcast(ctx context.Context, rawTx []byte, txID string, accountingScene string, accountingSubType string, fromPartyID string, toPartyID string) error
	WalletAllocMutex() sync.Locker
	ResolveDomainToPubkey(ctx context.Context, domain string) (string, error)
	GetFrontOrderSettlementSummary(ctx context.Context, frontOrderID string) (FrontOrderSettlementSummary, error)
}
