package moduleapi

import (
	"context"
)

// PaymentFacade is the unified entry for business layer to call payment capabilities.
type PaymentFacade interface {
	PayQuotedService(ctx context.Context, scheme string, quote []byte, request ServiceCallRequest, targetPeer string) (ServiceCallResponse, []byte, error)
	EnsureCoverage(ctx context.Context, scheme string, targetPeer string, requiredDuration uint64) (sessionRef string, coverageExpiresAt int64, err error)
	RenewCoverage(ctx context.Context, sessionRef string, additionalDuration uint64) (int64, error)
	RotateCoverage(ctx context.Context, sessionRef string, newAmount uint64) (string, error)
	CloseCoverage(ctx context.Context, sessionRef string, reason string) error
	OpenTradeSession(ctx context.Context, scheme string, tradeID string, buyer, seller, arbiter string, totalAmount uint64, chunkCount uint32) (sessionRef string, initialEntry TradeEntry, err error)
	AppendTradeEntry(ctx context.Context, sessionRef string, entry TradeEntry) (TradeEntry, error)
	OpenArbitration(ctx context.Context, sessionRef string, chunkIndex uint32, evidence string) (caseID string, err error)
	CloseTradeSession(ctx context.Context, sessionRef string, finalEntry TradeEntry) (SettlementResult, error)
	GetRegisteredSchemes() []string
	GetSchemeAbilities(scheme string) []string
}

type paymentFacade struct {
	registry PaymentRegistry
}

func newPaymentFacade(registry PaymentRegistry) PaymentFacade {
	return &paymentFacade{registry: registry}
}

func NewPaymentFacade(registry PaymentRegistry) PaymentFacade {
	return newPaymentFacade(registry)
}

func (f *paymentFacade) PayQuotedService(ctx context.Context, scheme string, quote []byte, request ServiceCallRequest, targetPeer string) (ServiceCallResponse, []byte, error) {
	if f.registry == nil {
		return ServiceCallResponse{}, nil, UnavailableScheme(scheme)
	}
	return f.registry.DispatchQuotedServicePayer(ctx, scheme, quote, request, targetPeer)
}

func (f *paymentFacade) EnsureCoverage(ctx context.Context, scheme string, targetPeer string, requiredDuration uint64) (string, int64, error) {
	if f.registry == nil {
		return "", 0, UnavailableScheme(scheme)
	}
	result, err := f.registry.DispatchServiceCoverageSession(ctx, scheme, "ensure", EnsureCoverageArgs{
		TargetPeer:       targetPeer,
		RequiredDuration: requiredDuration,
	})
	if err != nil {
		return "", 0, err
	}
	if r, ok := result.(EnsureCoverageResult); ok {
		return r.SessionRef, r.CoverageExpiresAt, nil
	}
	return "", 0, ErrInternal
}

func (f *paymentFacade) RenewCoverage(ctx context.Context, sessionRef string, additionalDuration uint64) (int64, error) {
	if f.registry == nil {
		return 0, SessionNotFound(sessionRef)
	}
	result, err := f.registry.DispatchServiceCoverageSession(ctx, "", "renew", RenewCoverageArgs{
		SessionRef:         sessionRef,
		AdditionalDuration: additionalDuration,
	})
	if err != nil {
		return 0, err
	}
	if exp, ok := result.(int64); ok {
		return exp, nil
	}
	return 0, ErrInternal
}

func (f *paymentFacade) RotateCoverage(ctx context.Context, sessionRef string, newAmount uint64) (string, error) {
	if f.registry == nil {
		return "", SessionNotFound(sessionRef)
	}
	result, err := f.registry.DispatchServiceCoverageSession(ctx, "", "rotate", RotateCoverageArgs{
		SessionRef: sessionRef,
		NewAmount:  newAmount,
	})
	if err != nil {
		return "", err
	}
	if r, ok := result.(RotateCoverageResult); ok {
		return r.NewSessionRef, nil
	}
	return "", ErrInternal
}

func (f *paymentFacade) CloseCoverage(ctx context.Context, sessionRef string, reason string) error {
	if f.registry == nil {
		return SessionNotFound(sessionRef)
	}
	_, err := f.registry.DispatchServiceCoverageSession(ctx, "", "close", CloseCoverageArgs{
		SessionRef: sessionRef,
		Reason:     reason,
	})
	return err
}

func (f *paymentFacade) OpenTradeSession(ctx context.Context, scheme string, tradeID string, buyer, seller, arbiter string, totalAmount uint64, chunkCount uint32) (string, TradeEntry, error) {
	if f.registry == nil {
		return "", TradeEntry{}, UnavailableScheme(scheme)
	}
	result, err := f.registry.DispatchTradeSession(ctx, scheme, "open", OpenTradeSessionArgs{
		TradeID:     tradeID,
		Buyer:       buyer,
		Seller:      seller,
		Arbiter:     arbiter,
		TotalAmount: totalAmount,
		ChunkCount:  chunkCount,
	})
	if err != nil {
		return "", TradeEntry{}, err
	}
	if r, ok := result.(OpenTradeSessionResult); ok {
		return r.SessionRef, r.InitialEntry, nil
	}
	return "", TradeEntry{}, ErrInternal
}

func (f *paymentFacade) AppendTradeEntry(ctx context.Context, sessionRef string, entry TradeEntry) (TradeEntry, error) {
	if f.registry == nil {
		return TradeEntry{}, SessionNotFound(sessionRef)
	}
	result, err := f.registry.DispatchTradeSession(ctx, "", "append", AppendTradeEntryArgs{
		SessionRef: sessionRef,
		Entry:      entry,
	})
	if err != nil {
		return TradeEntry{}, err
	}
	if e, ok := result.(TradeEntry); ok {
		return e, nil
	}
	return TradeEntry{}, ErrInternal
}

func (f *paymentFacade) OpenArbitration(ctx context.Context, sessionRef string, chunkIndex uint32, evidence string) (string, error) {
	if f.registry == nil {
		return "", SessionNotFound(sessionRef)
	}
	result, err := f.registry.DispatchTradeSession(ctx, "", "arbitrate", OpenArbitrationArgs{
		SessionRef: sessionRef,
		ChunkIndex: chunkIndex,
		Evidence:   evidence,
	})
	if err != nil {
		return "", err
	}
	if r, ok := result.(OpenArbitrationResult); ok {
		return r.CaseID, nil
	}
	return "", ErrInternal
}

func (f *paymentFacade) CloseTradeSession(ctx context.Context, sessionRef string, finalEntry TradeEntry) (SettlementResult, error) {
	if f.registry == nil {
		return SettlementResult{}, SessionNotFound(sessionRef)
	}
	result, err := f.registry.DispatchTradeSession(ctx, "", "close", CloseTradeSessionArgs{
		SessionRef: sessionRef,
		FinalEntry: finalEntry,
	})
	if err != nil {
		return SettlementResult{}, err
	}
	if s, ok := result.(SettlementResult); ok {
		return s, nil
	}
	return SettlementResult{}, ErrInternal
}

func (f *paymentFacade) GetRegisteredSchemes() []string {
	if f.registry == nil {
		return nil
	}
	return f.registry.GetRegisteredSchemes()
}

func (f *paymentFacade) GetSchemeAbilities(scheme string) []string {
	if f.registry == nil {
		return nil
	}
	return f.registry.GetSchemeAbility(scheme)
}

var ErrInternal = NewPaymentError(CodePaymentInternalError, "internal error")
