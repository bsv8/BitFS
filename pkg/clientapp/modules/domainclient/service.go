package domainclient

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
)

// Backend 是 domainclient 业务需要的最小后端能力。
type Backend interface {
	BusinessStore
	RuntimePorts
}

func triggerDomainRegisterName(ctx context.Context, backend Backend, p TriggerDomainRegisterNameParams) (TriggerDomainRegisterNameResult, error) {
	uniqueSuffix := fmt.Sprintf("%d_%04x", time.Now().UnixNano(), time.Now().UnixNano()&0xFFFF)
	frontOrderID := "fo_domain_reg_" + uniqueSuffix
	businessID := "biz_domain_reg_" + uniqueSuffix
	settlementID := "set_domain_reg_" + uniqueSuffix
	p.Name = strings.ToLower(strings.TrimSpace(p.Name))
	if err := CreateDomainRegisterBusinessChain(ctx, backend, frontOrderID, businessID, settlementID, p); err != nil {
		return TriggerDomainRegisterNameResult{}, err
	}

	prepared, err := TriggerDomainPrepareRegister(ctx, backend, backend, TriggerDomainPrepareRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	if err != nil {
		return TriggerDomainRegisterNameResult{}, err
	}
	out := DomainRegisterNameResultFromPrepared(prepared)
	out.FrontOrderID = frontOrderID
	if !prepared.Ok {
		return out, nil
	}
	submitResp, err := TriggerDomainSubmitPreparedRegister(ctx, backend, backend, TriggerDomainSubmitPreparedRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		RegisterTxHex:     prepared.RegisterTxHex,
	})
	if err != nil {
		return out, err
	}
	out = ApplyDomainRegisterSubmitResult(out, submitResp)
	if !submitResp.Ok {
		if err := FinalizeDomainRegisterSettlement(ctx, backend, settlementID, false, "", submitResp.Message); err != nil {
			return out, err
		}
		return out, nil
	}
	if err := FinalizeDomainRegisterSettlement(ctx, backend, settlementID, true, out.RegisterTxID, ""); err != nil {
		return out, err
	}
	return out, nil
}

func triggerDomainRegisterLock(ctx context.Context, backend Backend, p TriggerDomainRegisterLockParams) (TriggerDomainRegisterLockResult, error) {
	var out TriggerDomainRegisterLockResult
	if ctx == nil {
		return out, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return out, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if backend == nil {
		return out, moduleDisabledErr()
	}
	clientID, err := backend.ClientID(ctx)
	if err != nil {
		return out, err
	}
	if strings.TrimSpace(clientID) == "" {
		return out, NewError(CodeBadRequest, "client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := backend.EnsureDomainPeerConnected(ctx, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := domainwire.NormalizeName(p.Name)
	if err != nil {
		return out, NewError(CodeBadRequest, err.Error())
	}
	targetPubkeyHex := strings.ToLower(strings.TrimSpace(p.TargetPubkeyHex))
	if targetPubkeyHex == "" {
		return out, NewError(CodeBadRequest, "target_pubkey_hex is required")
	}

	queryOut, err := backend.TriggerDomainQueryName(ctx, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut
	out.Name = name
	out.OwnerPubkeyHex = strings.TrimSpace(clientID)
	out.QueryFeeChargedSatoshi = queryResp.ChargedAmount
	if !queryResp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		out.Message = strings.TrimSpace(queryResp.Error)
		return out, fmt.Errorf("domain query failed: status=%s error=%s", queryResp.Status, queryResp.Error)
	}
	if !queryResp.Available {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		if out.Code == "" {
			out.Code = "UNAVAILABLE"
		}
		out.Message = strings.TrimSpace(queryResp.Error)
		if out.Message == "" {
			switch {
			case queryResp.Registered:
				out.Message = "domain already registered"
			case queryResp.Locked:
				out.Message = "domain already locked"
			default:
				out.Message = "domain unavailable"
			}
		}
		out.LockExpiresAtUnix = queryResp.LockExpiresAtUnix
		return out, nil
	}

	lockResp, err := backend.TriggerDomainRegisterLock(ctx, resolverPubkeyHex, name, targetPubkeyHex, queryResp.RegisterLockFee)
	if err != nil {
		return out, fmt.Errorf("domain register lock step failed: %w", err)
	}
	out.RegisterLockChargedSatoshi = lockResp.ChargedAmount
	out.LockExpiresAtUnix = lockResp.LockExpiresAtUnix
	if !lockResp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(lockResp.Status))
		out.Message = strings.TrimSpace(lockResp.Error)
		if out.Message == "" {
			out.Message = "domain register lock failed"
		}
		return out, nil
	}
	quote, err := backend.VerifyRegisterQuote(resolverPubkeyHex, lockResp.SignedQuoteJSON)
	if err != nil {
		return out, err
	}
	if !strings.EqualFold(quote.Name, name) {
		return out, NewError(CodeBadRequest, "register quote name mismatch")
	}
	if !strings.EqualFold(quote.OwnerPubkeyHex, strings.TrimSpace(clientID)) {
		return out, NewError(CodeBadRequest, "register quote owner mismatch")
	}
	if !strings.EqualFold(quote.TargetPubkeyHex, targetPubkeyHex) {
		return out, NewError(CodeBadRequest, "register quote target mismatch")
	}
	out.Ok = true
	out.Code = "LOCKED"
	out.OwnerPubkeyHex = strings.TrimSpace(clientID)
	out.TargetPubkeyHex = targetPubkeyHex
	out.RegisterPriceSatoshi = quote.RegisterPriceSatoshi
	out.RegisterSubmitFeeSatoshi = quote.RegisterSubmitFeeSatoshi
	out.TotalRegisterPaySatoshi = quote.TotalPaySatoshi
	return out, nil
}

func triggerDomainPrepareRegister(ctx context.Context, backend Backend, p TriggerDomainPrepareRegisterParams) (TriggerDomainPrepareRegisterResult, error) {
	var out TriggerDomainPrepareRegisterResult
	if ctx == nil {
		return out, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return out, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if backend == nil {
		return out, moduleDisabledErr()
	}
	clientID, err := backend.ClientID(ctx)
	if err != nil {
		return out, err
	}
	if strings.TrimSpace(clientID) == "" {
		return out, NewError(CodeBadRequest, "client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := backend.EnsureDomainPeerConnected(ctx, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := domainwire.NormalizeName(p.Name)
	if err != nil {
		return out, NewError(CodeBadRequest, err.Error())
	}
	targetPubkeyHex := strings.ToLower(strings.TrimSpace(p.TargetPubkeyHex))
	if targetPubkeyHex == "" {
		return out, NewError(CodeBadRequest, "target_pubkey_hex is required")
	}

	queryOut, err := backend.TriggerDomainQueryName(ctx, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut
	out.Name = name
	out.OwnerPubkeyHex = strings.TrimSpace(clientID)
	out.QueryFeeChargedSatoshi = queryResp.ChargedAmount
	if !queryResp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		out.Message = strings.TrimSpace(queryResp.Error)
		return out, fmt.Errorf("domain query failed: status=%s error=%s", queryResp.Status, queryResp.Error)
	}
	if !queryResp.Available {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		if out.Code == "" {
			out.Code = "UNAVAILABLE"
		}
		out.Message = strings.TrimSpace(queryResp.Error)
		if out.Message == "" {
			switch {
			case queryResp.Registered:
				out.Message = "domain already registered"
			case queryResp.Locked:
				out.Message = "domain already locked"
			default:
				out.Message = "domain unavailable"
			}
		}
		out.LockExpiresAtUnix = queryResp.LockExpiresAtUnix
		return out, nil
	}

	lockResp, err := backend.TriggerDomainRegisterLock(ctx, resolverPubkeyHex, name, targetPubkeyHex, queryResp.RegisterLockFee)
	if err != nil {
		return out, fmt.Errorf("domain register lock step failed: %w", err)
	}
	out.RegisterLockChargedSatoshi = lockResp.ChargedAmount
	out.LockExpiresAtUnix = lockResp.LockExpiresAtUnix
	if !lockResp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(lockResp.Status))
		out.Message = strings.TrimSpace(lockResp.Error)
		if out.Message == "" {
			out.Message = "domain register lock failed"
		}
		return out, nil
	}

	quote, err := backend.VerifyRegisterQuote(resolverPubkeyHex, lockResp.SignedQuoteJSON)
	if err != nil {
		return out, err
	}
	if !strings.EqualFold(quote.Name, name) {
		return out, NewError(CodeBadRequest, "register quote name mismatch")
	}
	if !strings.EqualFold(quote.OwnerPubkeyHex, strings.TrimSpace(clientID)) {
		return out, NewError(CodeBadRequest, "register quote owner mismatch")
	}
	if !strings.EqualFold(quote.TargetPubkeyHex, targetPubkeyHex) {
		return out, NewError(CodeBadRequest, "register quote target mismatch")
	}
	out.RegisterPriceSatoshi = quote.RegisterPriceSatoshi
	out.RegisterSubmitFeeSatoshi = quote.RegisterSubmitFeeSatoshi
	out.TotalRegisterPaySatoshi = quote.TotalPaySatoshi

	if backend.WalletAllocMutex() != nil {
		backend.WalletAllocMutex().Lock()
		defer backend.WalletAllocMutex().Unlock()
	}

	built, err := backend.BuildDomainRegisterTx(ctx, lockResp.SignedQuoteJSON, quote)
	if err != nil {
		return out, err
	}
	out.Ok = true
	out.Code = "PREPARED"
	out.TargetPubkeyHex = targetPubkeyHex
	out.RegisterTxID = built.TxID
	out.RegisterTxHex = hex.EncodeToString(built.RawTx)
	return out, nil
}

func triggerDomainSubmitPreparedRegister(ctx context.Context, backend Backend, p TriggerDomainSubmitPreparedRegisterParams) (TriggerDomainSubmitPreparedRegisterResult, error) {
	var out TriggerDomainSubmitPreparedRegisterResult
	if ctx == nil {
		return out, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return out, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if backend == nil {
		return out, moduleDisabledErr()
	}
	clientID, err := backend.ClientID(ctx)
	if err != nil {
		return out, err
	}
	if strings.TrimSpace(clientID) == "" {
		return out, NewError(CodeBadRequest, "client identity not initialized")
	}
	registerTxHex := strings.ToLower(strings.TrimSpace(p.RegisterTxHex))
	if registerTxHex == "" {
		return out, NewError(CodeBadRequest, "register_tx_hex is required")
	}
	registerTxRaw, err := hex.DecodeString(registerTxHex)
	if err != nil {
		return out, NewError(CodeBadRequest, "register_tx_hex invalid: "+err.Error())
	}
	submitResp, err := backend.TriggerDomainRegisterSubmit(ctx, strings.TrimSpace(p.ResolverPubkeyHex), registerTxRaw)
	if err != nil {
		return out, fmt.Errorf("domain register submit step failed: %w", err)
	}
	out.Code = strings.ToUpper(strings.TrimSpace(submitResp.Status))
	out.Message = strings.TrimSpace(submitResp.Error)
	out.Name = strings.TrimSpace(submitResp.Name)
	out.OwnerPubkeyHex = strings.TrimSpace(submitResp.OwnerPubkeyHex)
	out.TargetPubkeyHex = strings.TrimSpace(submitResp.TargetPubkeyHex)
	out.ExpireAtUnix = submitResp.ExpireAtUnix
	out.RegisterTxID = strings.TrimSpace(submitResp.RegisterTxID)
	out.RegisterTxHex = registerTxHex
	if !submitResp.Success {
		if out.Message == "" {
			out.Message = "domain register submit failed"
		}
		return out, nil
	}
	if out.RegisterTxID == "" {
		out.RegisterTxID = "unknown"
	}
	if err := backend.ApplyLocalBroadcastWalletTxBytes(ctx, registerTxRaw, "domain_register_submit"); err != nil {
		return out, err
	}
	if err := backend.RecordChainPaymentAccountingAfterBroadcast(ctx, registerTxRaw, out.RegisterTxID, "domain_register", "domain_register_submit", "client:self", "resolver:"+strings.ToLower(strings.TrimSpace(p.ResolverPubkeyHex))); err != nil {
		return out, err
	}
	out.Ok = true
	out.Code = "OK"
	return out, nil
}

func triggerDomainSetTarget(ctx context.Context, backend Backend, p TriggerDomainSetTargetParams) (TriggerDomainSetTargetResult, error) {
	var out TriggerDomainSetTargetResult
	if ctx == nil {
		return out, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return out, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if backend == nil {
		return out, moduleDisabledErr()
	}
	clientID, err := backend.ClientID(ctx)
	if err != nil {
		return out, err
	}
	if strings.TrimSpace(clientID) == "" {
		return out, NewError(CodeBadRequest, "client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := backend.EnsureDomainPeerConnected(ctx, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := domainwire.NormalizeName(p.Name)
	if err != nil {
		return out, NewError(CodeBadRequest, err.Error())
	}
	targetPubkeyHex := strings.ToLower(strings.TrimSpace(p.TargetPubkeyHex))
	if targetPubkeyHex == "" {
		return out, NewError(CodeBadRequest, "target_pubkey_hex is required")
	}

	queryOut, err := backend.TriggerDomainQueryName(ctx, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut
	out.Name = name
	out.QueryFeeChargedSatoshi = queryResp.ChargedAmount
	if !queryResp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		out.Message = strings.TrimSpace(queryResp.Error)
		return out, fmt.Errorf("domain query failed: status=%s error=%s", queryResp.Status, queryResp.Error)
	}
	if !queryResp.Registered || queryResp.ExpireAtUnix <= time.Now().Unix() {
		out.Code = strings.ToUpper(strings.TrimSpace(queryResp.Status))
		if out.Code == "" {
			out.Code = "NOT_FOUND"
		}
		out.Message = "domain not registered"
		return out, nil
	}
	if !strings.EqualFold(queryResp.OwnerPubkeyHex, strings.TrimSpace(clientID)) {
		out.Code = "FORBIDDEN"
		out.Message = "only owner can update target"
		out.OwnerPubkeyHex = strings.TrimSpace(queryResp.OwnerPubkeyHex)
		return out, nil
	}
	resp, err := backend.TriggerDomainSetTarget(ctx, resolverPubkeyHex, name, targetPubkeyHex, queryResp.SetTargetFee)
	if err != nil {
		return out, err
	}
	out.SetTargetChargedSatoshi = resp.ChargedAmount
	out.TotalChargedSatoshi = out.QueryFeeChargedSatoshi + out.SetTargetChargedSatoshi
	if !resp.Success {
		out.Code = strings.ToUpper(strings.TrimSpace(resp.Status))
		out.Message = strings.TrimSpace(resp.Error)
		if out.Message == "" {
			out.Message = "domain set target failed"
		}
		return out, nil
	}
	out.Ok = true
	out.Code = "OK"
	out.Name = strings.TrimSpace(resp.Name)
	out.OwnerPubkeyHex = strings.TrimSpace(resp.OwnerPubkeyHex)
	out.TargetPubkeyHex = strings.TrimSpace(resp.TargetPubkeyHex)
	out.ExpireAtUnix = resp.ExpireAtUnix
	return out, nil
}

func DomainRegisterNameResultFromPrepared(prepared TriggerDomainPrepareRegisterResult) TriggerDomainRegisterNameResult {
	return TriggerDomainRegisterNameResult{
		Ok:                         prepared.Ok,
		Code:                       prepared.Code,
		Message:                    prepared.Message,
		Name:                       prepared.Name,
		OwnerPubkeyHex:             prepared.OwnerPubkeyHex,
		TargetPubkeyHex:            prepared.TargetPubkeyHex,
		LockExpiresAtUnix:          prepared.LockExpiresAtUnix,
		RegisterTxID:               prepared.RegisterTxID,
		QueryFeeChargedSatoshi:     prepared.QueryFeeChargedSatoshi,
		RegisterLockChargedSatoshi: prepared.RegisterLockChargedSatoshi,
		RegisterPriceSatoshi:       prepared.RegisterPriceSatoshi,
		RegisterSubmitFeeSatoshi:   prepared.RegisterSubmitFeeSatoshi,
		TotalRegisterPaySatoshi:    prepared.TotalRegisterPaySatoshi,
	}
}

func ApplyDomainRegisterSubmitResult(out TriggerDomainRegisterNameResult, submitResp TriggerDomainSubmitPreparedRegisterResult) TriggerDomainRegisterNameResult {
	if !submitResp.Ok {
		out.Ok = false
		out.Code = strings.ToUpper(strings.TrimSpace(submitResp.Code))
		out.Message = strings.TrimSpace(submitResp.Message)
		if out.Message == "" {
			out.Message = "domain register submit failed"
		}
		return out
	}
	out.Ok = true
	out.Code = "OK"
	out.Name = strings.TrimSpace(submitResp.Name)
	out.OwnerPubkeyHex = strings.TrimSpace(submitResp.OwnerPubkeyHex)
	out.TargetPubkeyHex = strings.TrimSpace(submitResp.TargetPubkeyHex)
	out.ExpireAtUnix = submitResp.ExpireAtUnix
	out.RegisterTxID = strings.TrimSpace(submitResp.RegisterTxID)
	return out
}

// CreateDomainRegisterBusinessChain 落地域名注册主链骨架。
func CreateDomainRegisterBusinessChain(ctx context.Context, store BusinessStore, frontOrderID, businessID, settlementID string, p TriggerDomainRegisterNameParams) error {
	if ctx == nil {
		return NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return moduleDisabledErr()
	}
	name := strings.ToLower(strings.TrimSpace(p.Name))
	resolverPubkeyHex := strings.ToLower(strings.TrimSpace(p.ResolverPubkeyHex))
	targetPubkeyHex := strings.ToLower(strings.TrimSpace(p.TargetPubkeyHex))
	return store.CreateBusinessWithFrontTriggerAndPendingSettlement(ctx, CreateBusinessWithFrontTriggerAndPendingSettlementInput{
		FrontOrderID:     frontOrderID,
		FrontType:        "domain",
		FrontSubtype:     "register",
		OwnerPubkeyHex:   targetPubkeyHex,
		TargetObjectType: "domain_name",
		TargetObjectID:   name,
		FrontOrderNote:   "域名注册: " + name,
		FrontOrderPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"target_pubkey_hex":   targetPubkeyHex,
		},
		BusinessID:        businessID,
		BusinessRole:      "formal",
		SourceType:        "front_order",
		SourceID:          frontOrderID,
		AccountingScene:   "domain",
		AccountingSubType: "register",
		FromPartyID:       "client:self",
		ToPartyID:         "resolver:" + resolverPubkeyHex,
		BusinessNote:      "域名注册费用: " + name,
		BusinessPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"target_pubkey_hex":   targetPubkeyHex,
		},
		TriggerType:    "front_order",
		TriggerIDValue: frontOrderID,
		TriggerRole:    "primary",
		TriggerNote:    "前台订单触发注册",
		TriggerPayload: map[string]any{
			"trigger_reason": "domain_register_initiated",
		},
		SettlementID:         settlementID,
		SettlementMethod:     string(SettlementMethodChain),
		SettlementTargetType: "chain_quote_pay",
		SettlementTargetID:   "",
		SettlementPayload: map[string]any{
			"name":                name,
			"resolver_pubkey_hex": resolverPubkeyHex,
			"status":              "pending",
		},
	})
}

// FinalizeDomainRegisterSettlement 回写域名注册结算状态。
func FinalizeDomainRegisterSettlement(ctx context.Context, store BusinessStore, settlementID string, success bool, txID string, errMsg string) error {
	if ctx == nil {
		return NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return moduleDisabledErr()
	}
	status := "settled"
	targetID := ""
	if success {
		txID = strings.ToLower(strings.TrimSpace(txID))
		if txID == "" {
			return NewError(CodeBadRequest, "txid is required for successful settlement")
		}
		chainPaymentID, err := store.GetChainPaymentByTxID(ctx, txID)
		if err != nil {
			return fmt.Errorf("find fact_settlement_channel_chain_quote_pay.id for txid=%s: %w", txID, err)
		}
		targetID = fmt.Sprintf("%d", chainPaymentID)
	} else {
		status = "failed"
	}
	return store.UpdateOrderSettlement(ctx, settlementID, status, "chain_quote_pay", targetID, strings.TrimSpace(errMsg), time.Now().Unix())
}
