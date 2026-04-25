package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
	domainclient "github.com/bsv8/BitFS/pkg/clientapp/modules/domainclient"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type domainClientBackend struct {
	rt    *Runtime
	store *clientDB
}

// NewDomainClientBackend 组装 domainclient 需要的最小后端能力。
// 设计说明：
// - 只把已经准备好的 runtime 和 store 绑成能力，不额外暴露 db 或聚合结构体；
// - 受管控制面只在这里拿能力，避免重复拼装业务后端。
func NewDomainClientBackend(rt *Runtime, store *clientDB) domainclient.Backend {
	if rt == nil || store == nil {
		return nil
	}
	return domainClientBackend{rt: rt, store: store}
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

func (b domainClientBackend) ResolveDomainToPubkeyDirect(ctx context.Context, domain string) (string, error) {
	return resolveDomainToPubkeyByCandidates(ctx, b.store, b.rt, domain)
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

// resolveDomainToPubkeyByCandidates 直接向候选 resolver peer 询问域名解析结果。
//
// 设计说明：
// - 这条链不再回到 root 的 provider 调度，避免 provider 自调环；
// - 候选 peer 来自运行配置里的 domain resolver 列表，按顺序逐个尝试；
// - 只要有一个 peer 真正返回解析结果，就直接结束。
func resolveDomainToPubkeyByCandidates(ctx context.Context, store *clientDB, rt *Runtime, rawDomain string) (string, error) {
	if ctx == nil {
		return "", domainclient.NewError(domainclient.CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return "", domainclient.NewError(domainclient.CodeRequestCanceled, ctx.Err().Error())
	}
	if rt == nil || rt.Host == nil {
		return "", domainclient.NewError(domainclient.CodeDomainResolverUnavailable, "domain resolver is unavailable")
	}

	domain, err := domainwire.NormalizeName(rawDomain)
	if err != nil {
		return "", domainclient.NewError(domainclient.CodeBadRequest, err.Error())
	}

	candidates := domainResolveCandidatePubkeys(rt)
	if len(candidates) == 0 {
		return "", domainclient.NewError(domainclient.CodeDomainResolverUnavailable, "domain resolver is unavailable")
	}

	for _, resolverPubkeyHex := range candidates {
		pubkeyHex, err := callDomainResolveCandidate(ctx, store, rt, resolverPubkeyHex, domain)
		if err != nil {
			if ctx.Err() != nil {
				return "", domainclient.NewError(domainclient.CodeRequestCanceled, ctx.Err().Error())
			}
			continue
		}
		pubkeyHex = strings.ToLower(strings.TrimSpace(pubkeyHex))
		if pubkeyHex == "" {
			continue
		}
		if normalizedPubkeyHex, err := normalizeCompressedPubKeyHex(pubkeyHex); err == nil {
			return normalizedPubkeyHex, nil
		}
	}

	return "", domainclient.NewError(domainclient.CodeDomainNotResolved, "domain not resolved")
}

func domainResolveCandidatePubkeys(rt *Runtime) []string {
	if rt == nil {
		return nil
	}
	cfg := rt.ConfigSnapshot()
	seen := make(map[string]struct{})
	out := make([]string, 0, len(cfg.Domain.Resolvers))
	appendCandidate := func(raw string, enabled bool) {
		if !enabled {
			return
		}
		pubkeyHex, err := normalizeCompressedPubKeyHex(strings.ToLower(strings.TrimSpace(raw)))
		if err != nil || pubkeyHex == "" {
			return
		}
		if _, ok := seen[pubkeyHex]; ok {
			return
		}
		seen[pubkeyHex] = struct{}{}
		out = append(out, pubkeyHex)
	}
	for _, node := range cfg.Domain.Resolvers {
		appendCandidate(node.Pubkey, node.Enabled)
	}
	return out
}

func callDomainResolveCandidate(ctx context.Context, store *clientDB, rt *Runtime, resolverPubkeyHex string, domain string) (string, error) {
	payload, err := oldproto.Marshal(&contractmessage.NameRouteReq{Name: domain})
	if err != nil {
		return "", err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          resolverPubkeyHex,
		ProtocolID:  protocol.ID(ncall.ProtoDomainResolveNamePaid),
		ContentType: contractmessage.ContentTypeProto,
		Body:        payload,
		Store:       store,
		Security:    domainSec(rt.rpcTrace),
	})
	if err != nil {
		return "", err
	}
	if !callResp.Ok {
		return "", fmt.Errorf("domain resolve route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp contractmessage.ResolveNamePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return "", fmt.Errorf("decode domain resolve body failed: %w", err)
	}
	pubkeyHex := strings.ToLower(strings.TrimSpace(resp.TargetPubkeyHex))
	if pubkeyHex == "" {
		pubkeyHex = strings.ToLower(strings.TrimSpace(resp.OwnerPubkeyHex))
	}
	if pubkeyHex == "" {
		return "", fmt.Errorf("domain resolve response missing pubkey")
	}
	return pubkeyHex, nil
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

func (b domainClientBackend) UpdateOrderSettlement(ctx context.Context, settlementID, status, targetType, targetID, errMsg string, updatedAtUnix int64) error {
	frontOrderID, _, err := domainRegisterOrderIDsFromSettlementID(settlementID)
	if err != nil {
		return err
	}
	if err := dbUpdateFrontOrderSettlement(ctx, b.store, frontOrderID, status, targetType, targetID, errMsg, updatedAtUnix); err != nil {
		return err
	}
	return nil
}

func domainRegisterOrderIDsFromSettlementID(settlementID string) (string, string, error) {
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return "", "", fmt.Errorf("settlement_id is required")
	}
	const settlementPrefix = "set_domain_reg_"
	if !strings.HasPrefix(settlementID, settlementPrefix) {
		return "", "", fmt.Errorf("unsupported settlement_id: %s", settlementID)
	}
	suffix := strings.TrimPrefix(settlementID, settlementPrefix)
	if strings.TrimSpace(suffix) == "" {
		return "", "", fmt.Errorf("unsupported settlement_id: %s", settlementID)
	}
	return "fo_domain_reg_" + suffix, "biz_domain_reg_" + suffix, nil
}
