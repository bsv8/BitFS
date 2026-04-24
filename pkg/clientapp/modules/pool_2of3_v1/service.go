package pool_2of3_v1

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type service struct {
	host  moduleapi.Host
	store Store
}

func newService(host moduleapi.Host, store Store) *service {
	return &service{
		host:  host,
		store: store,
	}
}

func (s *service) getPaymentHost() (moduleapi.PaymentHost, error) {
	h, ok := s.host.(moduleapi.PaymentHost)
	if !ok {
		return nil, fmt.Errorf("host does not implement PaymentHost")
	}
	return h, nil
}

func (s *service) OpenTradeSession(ctx context.Context, scheme string, tradeID string, buyer, seller, arbiter string, totalAmount uint64, chunkCount uint32) (string, moduleapi.TradeEntry, error) {
	if ctx == nil {
		return "", moduleapi.TradeEntry{}, fmt.Errorf("ctx is required")
	}
	if scheme == "" {
		return "", moduleapi.TradeEntry{}, moduleapi.UnavailableScheme(scheme)
	}
	if tradeID == "" {
		return "", moduleapi.TradeEntry{}, fmt.Errorf("tradeID is required")
	}
	if buyer == "" || seller == "" || arbiter == "" {
		return "", moduleapi.TradeEntry{}, fmt.Errorf("buyer, seller, arbiter are all required")
	}

	for _, hexStr := range []string{buyer, seller, arbiter} {
		if err := validatePubKeyHex(hexStr); err != nil {
			return "", moduleapi.TradeEntry{}, fmt.Errorf("invalid pubkey hex %q: %w", hexStr, err)
		}
	}

	now := time.Now().Unix()
	sessionRef := newSessionRef()
	chunkAmount := totalAmount / uint64(chunkCount)

	session := TradeSessionRow{
		Ref:                   sessionRef,
		TradeID:               tradeID,
		BuyerHex:              buyer,
		SellerHex:             seller,
		ArbiterHex:            arbiter,
		TotalAmountSatoshi:    totalAmount,
		PaidAmountSatoshi:     0,
		RefundedAmountSatoshi: 0,
		ChunkCount:            chunkCount,
		State:                 "open",
		CreatedAtUnix:         now,
		UpdatedAtUnix:         now,
	}

	if err := s.store.OpenSession(ctx, session); err != nil {
		return "", moduleapi.TradeEntry{}, fmt.Errorf("persist session failed: %w", err)
	}

	initialEntry := moduleapi.TradeEntry{
		ChunkIndex:    0,
		AmountSatoshi: chunkAmount,
		State:         "pending",
	}

	return sessionRef, initialEntry, nil
}

func (s *service) AppendTradeEntry(ctx context.Context, sessionRef string, entry moduleapi.TradeEntry) (moduleapi.TradeEntry, error) {
	if ctx == nil {
		return moduleapi.TradeEntry{}, fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return moduleapi.TradeEntry{}, moduleapi.SessionNotFound(sessionRef)
	}

	session, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return moduleapi.TradeEntry{}, fmt.Errorf("get session failed: %w", err)
	}
	if !found {
		return moduleapi.TradeEntry{}, moduleapi.SessionNotFound(sessionRef)
	}

	if session.State != "open" && session.State != "in_progress" {
		return moduleapi.TradeEntry{}, fmt.Errorf("session not open for new entries")
	}

	if entry.ChunkIndex >= session.ChunkCount {
		return moduleapi.TradeEntry{}, fmt.Errorf("chunk index out of range")
	}

	now := time.Now().Unix()
	entryRow := TradeEntryRow{
		SessionRef:     sessionRef,
		ChunkIndex:    entry.ChunkIndex,
		AmountSatoshi: entry.AmountSatoshi,
		State:         "paid",
		PaymentTxid:   entry.PaymentTxID,
		PaidAtUnix:   now,
		CreatedAtUnix: now,
		UpdatedAtUnix: now,
	}

	if err := s.store.AppendEntry(ctx, entryRow); err != nil {
		return moduleapi.TradeEntry{}, fmt.Errorf("persist entry failed: %w", err)
	}

	newPaidAmount := session.PaidAmountSatoshi + entry.AmountSatoshi
	newState := "in_progress"
	if newPaidAmount >= session.TotalAmountSatoshi {
		newState = "completed"
	}

	err = s.store.UpdateSession(ctx, sessionRef, func(row *TradeSessionRow) {
		row.PaidAmountSatoshi = newPaidAmount
		row.State = newState
		row.UpdatedAtUnix = now
	})
	if err != nil {
		return moduleapi.TradeEntry{}, fmt.Errorf("update session failed: %w", err)
	}

	entry.State = "paid"
	entry.PaymentAtUnix = now
	return entry, nil
}

func (s *service) OpenArbitration(ctx context.Context, sessionRef string, chunkIndex uint32, evidence string) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return "", moduleapi.SessionNotFound(sessionRef)
	}
	if evidence == "" {
		return "", fmt.Errorf("evidence is required")
	}

	_, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return "", fmt.Errorf("get session failed: %w", err)
	}
	if !found {
		return "", moduleapi.SessionNotFound(sessionRef)
	}

	caseID := newCaseID()
	now := time.Now().Unix()

	arb := ArbitrationRow{
		SessionRef:    sessionRef,
		ChunkIndex:    chunkIndex,
		CaseID:        caseID,
		State:         "open",
		Evidence:      evidence,
		CreatedAtUnix: now,
		UpdatedAtUnix: now,
	}

	if err := s.store.OpenArbitration(ctx, arb); err != nil {
		return "", fmt.Errorf("persist arbitration failed: %w", err)
	}

	return caseID, nil
}

func (s *service) CloseTradeSession(ctx context.Context, sessionRef string, finalEntry moduleapi.TradeEntry) (moduleapi.SettlementResult, error) {
	if ctx == nil {
		return moduleapi.SettlementResult{}, fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return moduleapi.SettlementResult{}, moduleapi.SessionNotFound(sessionRef)
	}

	session, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return moduleapi.SettlementResult{}, fmt.Errorf("get session failed: %w", err)
	}
	if !found {
		return moduleapi.SettlementResult{}, moduleapi.SessionNotFound(sessionRef)
	}

	entries, err := s.store.GetEntriesBySession(ctx, sessionRef)
	if err != nil {
		return moduleapi.SettlementResult{}, fmt.Errorf("get entries failed: %w", err)
	}

	var totalPaid uint64
	for _, e := range entries {
		if e.State == "paid" || e.State == "completed" {
			totalPaid += e.AmountSatoshi
		}
	}
	if finalEntry.AmountSatoshi > 0 {
		totalPaid += finalEntry.AmountSatoshi
	}

	refunded := session.TotalAmountSatoshi - totalPaid
	now := time.Now().Unix()

	finalState := "closed_without_chain_finality"
	if finalEntry.AwardTxID != "" {
		finalState = "settled"
	}

	err = s.store.UpdateSession(ctx, sessionRef, func(row *TradeSessionRow) {
		row.State = finalState
		row.PaidAmountSatoshi = totalPaid
		row.RefundedAmountSatoshi = refunded
		row.ClosedAtUnix = now
		row.UpdatedAtUnix = now
	})
	if err != nil {
		return moduleapi.SettlementResult{}, fmt.Errorf("update session failed: %w", err)
	}

	return moduleapi.SettlementResult{
		TotalPaidSatoshi:     totalPaid,
		TotalRefundedSatoshi: refunded,
		FinalTxID:            finalEntry.AwardTxID,
		ClosedAtUnix:         now,
	}, nil
}

func (s *service) OnTradeStateChange(callback func(sessionRef, oldState, newState string, timestamp int64)) {
}

func newSessionRef() string {
	return fmt.Sprintf("ts_%d", time.Now().UnixNano())
}

func newCaseID() string {
	return fmt.Sprintf("arb_%d", time.Now().UnixNano())
}

func validatePubKeyHex(hexStr string) error {
	if hexStr == "" {
		return errors.New("empty hex string")
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return fmt.Errorf("hex decode error: %w", err)
	}
	if len(bytes) != 33 {
		return fmt.Errorf("invalid pubkey length: got %d, want 33 (compressed)", len(bytes))
	}
	if bytes[0] != 0x02 && bytes[0] != 0x03 {
		return fmt.Errorf("invalid pubkey prefix: got 0x%02x, want 0x02 or 0x03", bytes[0])
	}
	return nil
}