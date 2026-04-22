package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/bsv8/BitFS/pkg/clientapp/modules/domain"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/ordersettlements"
	"github.com/libp2p/go-libp2p/core/peer"
)

type domainModuleAdapter struct {
	rt    *Runtime
	store *clientDB
}

func newDomainModuleAdapter(rt *Runtime, store *clientDB) domainModuleAdapter {
	return domainModuleAdapter{rt: rt, store: store}
}

func (a domainModuleAdapter) ClientID(ctx context.Context) (string, error) {
	if a.rt == nil {
		return "", domain.NewError(domain.CodeDomainResolverUnavailable, "runtime not initialized")
	}
	identity, err := a.rt.runtimeIdentity()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(identity.ClientID), nil
}

func (a domainModuleAdapter) CreateBusinessWithFrontTriggerAndPendingSettlement(ctx context.Context, input domain.CreateBusinessWithFrontTriggerAndPendingSettlementInput) error {
	return CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, a.store, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
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

func (a domainModuleAdapter) EnsureDomainPeerConnected(ctx context.Context, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error) {
	return ensureDomainPeerConnected(ctx, a.store, a.rt, resolverPubkeyHex, resolverAddr)
}

func (a domainModuleAdapter) TriggerDomainQueryName(ctx context.Context, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (domain.DomainQueryResponse, error) {
	out, err := triggerDomainQueryName(ctx, a.store, a.rt, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return domain.DomainQueryResponse{}, err
	}
	resp := out.Response
	return domain.DomainQueryResponse{
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

func (a domainModuleAdapter) TriggerDomainRegisterLock(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, registerLockFee uint64) (domain.DomainRegisterLockResponse, error) {
	resp, err := triggerDomainRegisterLock(ctx, a.store, a.rt, resolverPubkeyHex, name, targetPubkeyHex, registerLockFee)
	if err != nil {
		return domain.DomainRegisterLockResponse{}, err
	}
	return domain.DomainRegisterLockResponse{
		Success:           resp.Success,
		Status:            resp.Status,
		Error:             resp.Error,
		ChargedAmount:     resp.ChargedAmount,
		LockExpiresAtUnix: resp.LockExpiresAtUnix,
		SignedQuoteJSON:   append([]byte(nil), resp.SignedQuoteJSON...),
	}, nil
}

func (a domainModuleAdapter) VerifyRegisterQuote(resolverPubkeyHex string, raw []byte) (domain.DomainRegisterQuote, error) {
	quote, err := verifyRegisterQuote(resolverPubkeyHex, raw)
	if err != nil {
		return domain.DomainRegisterQuote{}, err
	}
	return domain.DomainRegisterQuote{
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

func (a domainModuleAdapter) BuildDomainRegisterTx(ctx context.Context, signedQuoteJSON []byte, quote domain.DomainRegisterQuote) (domain.BuiltDomainRegisterTx, error) {
	built, err := buildDomainRegisterTxDetailed(ctx, a.store, a.rt, signedQuoteJSON, domainRegisterQuote{
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
		return domain.BuiltDomainRegisterTx{}, err
	}
	return domain.BuiltDomainRegisterTx{
		RawTx:           append([]byte(nil), built.RawTx...),
		TxID:            built.TxID,
		MinerFeeSatoshi: built.MinerFeeSatoshi,
		ChangeSatoshi:   built.ChangeSatoshi,
	}, nil
}

func (a domainModuleAdapter) TriggerDomainRegisterSubmit(ctx context.Context, resolverPubkeyHex string, registerTx []byte) (domain.DomainRegisterSubmitResponse, error) {
	resp, err := triggerDomainRegisterSubmit(ctx, a.rt, a.store, resolverPubkeyHex, registerTx)
	if err != nil {
		return domain.DomainRegisterSubmitResponse{}, err
	}
	return domain.DomainRegisterSubmitResponse{
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

func (a domainModuleAdapter) TriggerDomainSetTarget(ctx context.Context, resolverPubkeyHex string, name string, targetPubkeyHex string, setTargetFee uint64) (domain.DomainSetTargetResponse, error) {
	resp, err := triggerDomainSetTarget(ctx, a.store, a.rt, resolverPubkeyHex, name, targetPubkeyHex, setTargetFee)
	if err != nil {
		return domain.DomainSetTargetResponse{}, err
	}
	return domain.DomainSetTargetResponse{
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

func (a domainModuleAdapter) ApplyLocalBroadcastWalletTxBytes(ctx context.Context, rawTx []byte, trigger string) error {
	return applyLocalBroadcastWalletTxBytes(ctx, a.store, a.rt, rawTx, trigger)
}

func (a domainModuleAdapter) RecordChainPaymentAccountingAfterBroadcast(ctx context.Context, rawTx []byte, txID string, accountingScene string, accountingSubType string, fromPartyID string, toPartyID string) error {
	return recordChainPaymentAccountingAfterBroadcast(ctx, a.store, a.rt, strings.ToLower(strings.TrimSpace(hex.EncodeToString(rawTx))), txID, accountingScene, accountingSubType, fromPartyID, toPartyID)
}

func (a domainModuleAdapter) WalletAllocMutex() sync.Locker {
	if a.rt == nil {
		return nil
	}
	return a.rt.walletAllocMutex()
}

func (a domainModuleAdapter) UpsertFrontOrder(ctx context.Context, entry domain.FrontOrderEntry) error {
	return dbUpsertFrontOrder(ctx, a.store, frontOrderEntry{
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

func (a domainModuleAdapter) UpsertBusiness(ctx context.Context, entry domain.BusinessEntry) error {
	return a.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
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

func (a domainModuleAdapter) UpsertBusinessSettlement(ctx context.Context, entry domain.BusinessSettlementEntry) error {
	return a.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertSettleRecordSettlementTx(ctx, tx, businessSettlementEntry{
			SettlementID:     entry.SettlementID,
			OrderID:          entry.BusinessID,
			SettlementMethod: entry.SettlementMethod,
			Status:           entry.Status,
			TargetType:       entry.SettlementTargetType,
			TargetID:         entry.SettlementTargetID,
			ErrorMessage:     entry.SettlementErrorMessage,
			CreatedAtUnix:    entry.CreatedAtUnix,
			UpdatedAtUnix:    entry.UpdatedAtUnix,
			Payload:          entry.SettlementPayload,
		})
	})
}

func (a domainModuleAdapter) AppendBusinessTrigger(ctx context.Context, entry domain.BusinessTriggerEntry) error {
	return a.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
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

func (a domainModuleAdapter) GetChainPaymentByTxID(ctx context.Context, txID string) (int64, error) {
	return dbGetChainPaymentByTxID(ctx, a.store, txID)
}

func (a domainModuleAdapter) UpdateOrderSettlement(ctx context.Context, settlementID, status, targetType, targetID, errMsg string, updatedAtUnix int64) error {
	if a.store == nil {
		return fmt.Errorf("client db is nil")
	}
	return a.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.OrderSettlements.Update().
			Where(ordersettlements.SettlementIDEQ(strings.TrimSpace(settlementID))).
			SetSettlementStatus(strings.TrimSpace(status)).
			SetTargetType(strings.TrimSpace(targetType)).
			SetTargetID(strings.TrimSpace(targetID)).
			SetErrorMessage(strings.TrimSpace(errMsg)).
			SetUpdatedAtUnix(updatedAtUnix).
			Save(ctx)
		return err
	})
}
