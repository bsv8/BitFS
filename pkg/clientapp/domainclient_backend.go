package clientapp

import (
	"context"
	"encoding/hex"
	"sync"

	domainclient "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
	"github.com/libp2p/go-libp2p/core/peer"
)

type domainClientBackend struct {
	rt    *Runtime
	store *clientDB
}

func (h *moduleHost) DomainClientBackend() domainclient.Backend {
	if h == nil || h.rt == nil {
		return nil
	}
	store, _ := h.store.(*clientDB)
	if store == nil {
		return nil
	}
	return domainClientBackend{rt: h.rt, store: store}
}

func (b domainClientBackend) ClientID(ctx context.Context) (string, error) {
	if b.rt == nil {
		return "", domainclient.NewError(domainclient.CodeDomainResolverUnavailable, "runtime not initialized")
	}
	identity, err := b.rt.runtimeIdentity()
	if err != nil {
		return "", err
	}
	return identity.ClientID, nil
}

func (b domainClientBackend) EnsureDomainPeerConnected(ctx context.Context, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error) {
	return ensureDomainPeerConnected(ctx, b.store, b.rt, resolverPubkeyHex, resolverAddr)
}

func (b domainClientBackend) TriggerDomainQueryName(ctx context.Context, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (domainclient.DomainQueryResponse, error) {
	out, err := triggerDomainQueryName(ctx, b.store, b.rt, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return domainclient.DomainQueryResponse{}, err
	}
	resp := out.Response
	return domainclient.DomainQueryResponse{
		Success:           resp.Success,
		Status:            resp.Status,
		Error:             resp.Error,
		ChargedAmount:     resp.ChargedAmount,
		Available:         resp.Available,
		Registered:        resp.Registered,
		Locked:            resp.Locked,
		LockExpiresAtUnix: resp.LockExpiresAtUnix,
		RegisterLockFee:   resp.RegisterLockFeeSatoshi,
		SetTargetFee:      resp.SetTargetFeeSatoshi,
		OwnerPubkeyHex:    resp.OwnerPubkeyHex,
		ExpireAtUnix:      resp.ExpireAtUnix,
	}, nil
}

func (b domainClientBackend) TriggerDomainRegisterLock(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, registerLockFee uint64) (domainclient.DomainRegisterLockResponse, error) {
	resp, err := triggerDomainRegisterLock(ctx, b.store, b.rt, resolverPubkeyHex, name, targetPubkeyHex, registerLockFee)
	if err != nil {
		return domainclient.DomainRegisterLockResponse{}, err
	}
	return domainclient.DomainRegisterLockResponse{
		Success:           resp.Success,
		Status:            resp.Status,
		Error:             resp.Error,
		ChargedAmount:     resp.ChargedAmount,
		LockExpiresAtUnix: resp.LockExpiresAtUnix,
		SignedQuoteJSON:   append([]byte(nil), resp.SignedQuoteJSON...),
	}, nil
}

func (b domainClientBackend) VerifyRegisterQuote(resolverPubkeyHex string, raw []byte) (domainclient.DomainRegisterQuote, error) {
	quote, err := verifyRegisterQuote(resolverPubkeyHex, raw)
	if err != nil {
		return domainclient.DomainRegisterQuote{}, err
	}
	return domainclient.DomainRegisterQuote{
		QuoteID:                  quote.QuoteID,
		Name:                     quote.Name,
		OwnerPubkeyHex:           quote.OwnerPubkeyHex,
		TargetPubkeyHex:          quote.TargetPubkeyHex,
		PayToAddress:             quote.PayToAddress,
		RegisterPriceSatoshi:     quote.RegisterPriceSatoshi,
		RegisterSubmitFeeSatoshi: quote.RegisterSubmitFeeSatoshi,
		TotalPaySatoshi:          quote.TotalPaySatoshi,
		LockExpiresAtUnix:        quote.LockExpiresAtUnix,
		TermSeconds:              quote.TermSeconds,
	}, nil
}

func (b domainClientBackend) BuildDomainRegisterTx(ctx context.Context, signedQuoteJSON []byte, quote domainclient.DomainRegisterQuote) (domainclient.BuiltDomainRegisterTx, error) {
	built, err := buildDomainRegisterTxDetailed(ctx, b.store, b.rt, signedQuoteJSON, domainRegisterQuote{
		QuoteID:                  quote.QuoteID,
		Name:                     quote.Name,
		OwnerPubkeyHex:           quote.OwnerPubkeyHex,
		TargetPubkeyHex:          quote.TargetPubkeyHex,
		PayToAddress:             quote.PayToAddress,
		RegisterPriceSatoshi:     quote.RegisterPriceSatoshi,
		RegisterSubmitFeeSatoshi: quote.RegisterSubmitFeeSatoshi,
		TotalPaySatoshi:          quote.TotalPaySatoshi,
		LockExpiresAtUnix:        quote.LockExpiresAtUnix,
		TermSeconds:              quote.TermSeconds,
	})
	if err != nil {
		return domainclient.BuiltDomainRegisterTx{}, err
	}
	return domainclient.BuiltDomainRegisterTx{
		RawTx:           append([]byte(nil), built.RawTx...),
		TxID:            built.TxID,
		MinerFeeSatoshi: built.MinerFeeSatoshi,
		ChangeSatoshi:   built.ChangeSatoshi,
	}, nil
}

func (b domainClientBackend) TriggerDomainRegisterSubmit(ctx context.Context, resolverPubkeyHex string, registerTx []byte) (domainclient.DomainRegisterSubmitResponse, error) {
	resp, err := triggerDomainRegisterSubmit(ctx, b.rt, b.store, resolverPubkeyHex, registerTx)
	if err != nil {
		return domainclient.DomainRegisterSubmitResponse{}, err
	}
	return domainclient.DomainRegisterSubmitResponse{
		Success:         resp.Success,
		Status:          resp.Status,
		Error:           resp.Error,
		Name:            resp.Name,
		OwnerPubkeyHex:  resp.OwnerPubkeyHex,
		TargetPubkeyHex: resp.TargetPubkeyHex,
		ExpireAtUnix:    resp.ExpireAtUnix,
		RegisterTxID:    resp.RegisterTxID,
	}, nil
}

func (b domainClientBackend) TriggerDomainSetTarget(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, setTargetFee uint64) (domainclient.DomainSetTargetResponse, error) {
	resp, err := triggerDomainSetTarget(ctx, b.store, b.rt, resolverPubkeyHex, name, targetPubkeyHex, setTargetFee)
	if err != nil {
		return domainclient.DomainSetTargetResponse{}, err
	}
	return domainclient.DomainSetTargetResponse{
		Success:         resp.Success,
		Status:          resp.Status,
		Error:           resp.Error,
		ChargedAmount:   resp.ChargedAmount,
		Name:            resp.Name,
		OwnerPubkeyHex:  resp.OwnerPubkeyHex,
		TargetPubkeyHex: resp.TargetPubkeyHex,
		ExpireAtUnix:    resp.ExpireAtUnix,
	}, nil
}

func (b domainClientBackend) ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error {
	return applyLocalBroadcastWalletTxBytes(ctx, b.store, b.rt, rawTx, trigger)
}

func (b domainClientBackend) RecordChainPaymentAccountingAfterBroadcast(ctx context.Context, rawTx []byte, txID string, accountingScene string, accountingSubType string, fromPartyID string, toPartyID string) error {
	return recordChainPaymentAccountingAfterBroadcast(ctx, b.store, b.rt, hex.EncodeToString(rawTx), txID, accountingScene, accountingSubType, fromPartyID, toPartyID)
}

func (b domainClientBackend) WalletAllocMutex() sync.Locker {
	if b.rt == nil {
		return nil
	}
	return b.rt.walletAllocMutex()
}

func (b domainClientBackend) ResolveDomainToPubkey(ctx context.Context, domain string) (string, error) {
	return ResolveDomainToPubkey(ctx, b.rt, domain)
}

func (b domainClientBackend) GetFrontOrderSettlementSummary(ctx context.Context, frontOrderID string) (domainclient.FrontOrderSettlementSummary, error) {
	summary, err := GetFrontOrderSettlementSummary(ctx, b.store, frontOrderID)
	if err != nil {
		return domainclient.FrontOrderSettlementSummary{}, err
	}
	out := domainclient.FrontOrderSettlementSummary{
		FrontOrderID: summary.FrontOrderID,
		Businesses:   make([]domainclient.BusinessSettlementSummary, 0, len(summary.Businesses)),
		Summary: domainclient.SettlementTotalSummary{
			OverallStatus:        summary.Summary.OverallStatus,
			TotalTargetSatoshi:   summary.Summary.TotalTargetSatoshi,
			SettledAmountSatoshi: summary.Summary.SettledAmountSatoshi,
			PendingAmountSatoshi: summary.Summary.PendingAmountSatoshi,
		},
	}
	for _, item := range summary.Businesses {
		out.Businesses = append(out.Businesses, domainclient.BusinessSettlementSummary{
			BusinessID:           item.BusinessID,
			SellerPubHex:         item.SellerPubHex,
			TotalTargetSatoshi:   item.TotalTargetSatoshi,
			SettledAmountSatoshi: item.SettledAmountSatoshi,
			PendingAmountSatoshi: item.PendingAmountSatoshi,
		})
	}
	return out, nil
}

func (b domainClientBackend) CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, input domainclient.CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	return CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, b.store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:         input.FrontOrderID,
		FrontType:            input.FrontType,
		FrontSubtype:         input.FrontSubtype,
		OwnerPubkeyHex:       input.OwnerPubkeyHex,
		TargetObjectType:     input.TargetObjectType,
		TargetObjectID:       input.TargetObjectID,
		FrontOrderNote:       input.FrontOrderNote,
		FrontOrderPayload:    input.FrontOrderPayload,
		BusinessID:           input.BusinessID,
		BusinessRole:         input.BusinessRole,
		SourceType:           input.SourceType,
		SourceID:             input.SourceID,
		AccountingScene:      input.AccountingScene,
		AccountingSubType:    input.AccountingSubType,
		FromPartyID:          input.FromPartyID,
		ToPartyID:            input.ToPartyID,
		BusinessNote:         input.BusinessNote,
		BusinessPayload:      input.BusinessPayload,
		TriggerID:            input.TriggerID,
		TriggerType:          input.TriggerType,
		TriggerIDValue:       input.TriggerIDValue,
		TriggerRole:          input.TriggerRole,
		TriggerNote:          input.TriggerNote,
		TriggerPayload:       input.TriggerPayload,
		SettlementID:         input.SettlementID,
		SettlementMethod:     SettlementMethod(input.SettlementMethod),
		SettlementTargetType: input.SettlementTargetType,
		SettlementTargetID:   input.SettlementTargetID,
		SettlementPayload:    input.SettlementPayload,
	})
}

func (b domainClientBackend) UpsertFrontOrder(ctx context.Context, entry domainclient.FrontOrderEntry) error {
	return dbUpsertFrontOrder(ctx, b.store, frontOrderEntry{
		FrontOrderID:     entry.FrontOrderID,
		FrontType:        entry.FrontType,
		FrontSubtype:     entry.FrontSubtype,
		OwnerPubkeyHex:   entry.OwnerPubkeyHex,
		TargetObjectType: entry.TargetObjectType,
		TargetObjectID:   entry.TargetObjectID,
		Status:           entry.Status,
		CreatedAtUnix:    entry.CreatedAtUnix,
		UpdatedAtUnix:    entry.UpdatedAtUnix,
		Note:             entry.FrontOrderNote,
		Payload:          entry.FrontOrderPayload,
	})
}

func (b domainClientBackend) UpsertBusiness(ctx context.Context, entry domainclient.BusinessEntry) error {
	return b.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertSettleRecordTx(ctx, tx, finBusinessEntry{
			OrderID:                entry.BusinessID,
			BusinessRole:           entry.BusinessRole,
			SourceType:             entry.SourceType,
			SourceID:               entry.SourceID,
			AccountingScene:        entry.AccountingScene,
			AccountingSubType:      entry.AccountingSubType,
			FromPartyID:            entry.FromPartyID,
			ToPartyID:              entry.ToPartyID,
			Status:                 entry.Status,
			OccurredAtUnix:         entry.CreatedAtUnix,
			IdempotencyKey:         "bridge:" + entry.BusinessID,
			Note:                   entry.BusinessNote,
			Payload:                entry.BusinessPayload,
			SettlementID:           "",
			SettlementMethod:       "",
			SettlementStatus:       "",
			SettlementTargetType:   "",
			SettlementTargetID:     "",
			SettlementErrorMessage: "",
			SettlementPayload:      nil,
		})
	})
}

func (b domainClientBackend) UpsertBusinessSettlement(ctx context.Context, entry domainclient.BusinessSettlementEntry) error {
	return nil
}

func (b domainClientBackend) AppendBusinessTrigger(ctx context.Context, entry domainclient.BusinessTriggerEntry) error {
	return b.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbAppendBusinessTriggerTx(ctx, tx, businessTriggerEntry{
			TriggerID:      entry.TriggerID,
			OrderID:        entry.BusinessID,
			SettlementID:   entry.SettlementID,
			TriggerType:    entry.TriggerType,
			TriggerIDValue: entry.TriggerIDValue,
			TriggerRole:    entry.TriggerRole,
			CreatedAtUnix:  entry.CreatedAtUnix,
			Note:           entry.TriggerNote,
			Payload:        entry.TriggerPayload,
		})
	})
}

func (b domainClientBackend) GetChainPaymentByTxID(ctx context.Context, txID string) (int64, error) {
	return dbGetChainPaymentByTxID(ctx, b.store, txID)
}

func (b domainClientBackend) UpdateOrderSettlement(ctx context.Context, settlementID, status, targetType, targetID, errMsg string, updatedAtUnix int64) error {
	return nil
}
