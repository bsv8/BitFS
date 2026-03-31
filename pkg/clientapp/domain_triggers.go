package clientapp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/modules/domain"
	"github.com/bsv8/BFTP/pkg/obs"
	oldproto "github.com/golang/protobuf/proto"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	domainQuotePayloadVersion = "bsv8-domain-register-quote-v1"
)

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

type domainQueryResult struct {
	ResolverPeerID peer.ID
	Response       domainmodule.QueryNamePaidResp
}

type domainRegisterQuote struct {
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

type builtDomainRegisterTx struct {
	RawTx           []byte
	TxID            string
	MinerFeeSatoshi uint64
	ChangeSatoshi   uint64
}

// domain 相关 trigger 的收费边界说明：
// - resolve/query/register_lock/set_target 这类请求，都是在已有 2of2 费用池会话上推进一次 PayConfirm；
// - 只有 register_submit 会把客户端本地构造好的独立注册交易交给 domain 去广播；
// - 因此读这组代码时，要把“费用池状态推进”和“独立注册交易广播”看成两件不同的事。

// TriggerDomainRegisterName 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易 -> 远端提交”。
// 设计说明：
// - 这里返回时，要么注册已经成功写链并拿到服务端回执，要么明确失败且不会偷偷广播；
// - 注册交易在客户端本地构造，但不自行广播，保持“确认资格后才上链”的语义。
func TriggerDomainRegisterName(ctx context.Context, rt *Runtime, p TriggerDomainRegisterNameParams) (TriggerDomainRegisterNameResult, error) {
	prepared, err := TriggerDomainPrepareRegister(ctx, rt, TriggerDomainPrepareRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		Name:              p.Name,
		TargetPubkeyHex:   p.TargetPubkeyHex,
	})
	if err != nil {
		return TriggerDomainRegisterNameResult{}, err
	}
	out := domainRegisterNameResultFromPrepared(prepared)
	if !prepared.Ok {
		return out, nil
	}
	submitResp, err := TriggerDomainSubmitPreparedRegister(ctx, rt, TriggerDomainSubmitPreparedRegisterParams{
		ResolverPubkeyHex: p.ResolverPubkeyHex,
		ResolverAddr:      p.ResolverAddr,
		RegisterTxHex:     prepared.RegisterTxHex,
	})
	if err != nil {
		return out, err
	}
	out = applyDomainRegisterSubmitResult(out, submitResp)
	if !submitResp.Ok {
		return out, nil
	}
	dbAppendWalletFundFlowFromContext(ctx, runtimeStore(rt), walletFundFlowEntry{
		FlowID:          "domain_register:" + out.RegisterTxID,
		FlowType:        "domain_register",
		RefID:           out.Name,
		Stage:           "broadcast",
		Direction:       "out",
		Purpose:         "domain_register_total_payment",
		AmountSatoshi:   -int64(prepared.TotalRegisterPaySatoshi),
		UsedSatoshi:     int64(prepared.TotalRegisterPaySatoshi),
		ReturnedSatoshi: 0,
		RelatedTxID:     out.RegisterTxID,
		Note:            fmt.Sprintf("resolver_pubkey_hex=%s", strings.TrimSpace(p.ResolverPubkeyHex)),
		Payload: map[string]any{
			"name":                        out.Name,
			"owner_pubkey_hex":            out.OwnerPubkeyHex,
			"target_pubkey_hex":           out.TargetPubkeyHex,
			"register_price_satoshi":      prepared.RegisterPriceSatoshi,
			"register_submit_fee_satoshi": prepared.RegisterSubmitFeeSatoshi,
			"total_pay_satoshi":           prepared.TotalRegisterPaySatoshi,
		},
	})
	obs.Business("bitcast-client", "evt_trigger_domain_register_name_end", map[string]any{
		"resolver_pubkey_hex":        strings.TrimSpace(p.ResolverPubkeyHex),
		"name":                       out.Name,
		"target_pubkey_hex":          out.TargetPubkeyHex,
		"register_txid":              out.RegisterTxID,
		"query_fee_charged_satoshi":  out.QueryFeeChargedSatoshi,
		"register_lock_charged_sat":  out.RegisterLockChargedSatoshi,
		"register_total_pay_satoshi": out.TotalRegisterPaySatoshi,
	})
	return out, nil
}

func domainRegisterNameResultFromPrepared(prepared TriggerDomainPrepareRegisterResult) TriggerDomainRegisterNameResult {
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

func applyDomainRegisterSubmitResult(out TriggerDomainRegisterNameResult, submitResp TriggerDomainSubmitPreparedRegisterResult) TriggerDomainRegisterNameResult {
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

// TriggerDomainRegisterLock 通过 domain 系统完成“查询 -> 锁名”，但不提交注册交易。
// 设计意图：
// - 让 e2e 可以验证锁冲突、锁过期等边界，而不绕过真实费用池与签名链路；
// - 返回时若成功，代表服务端已为该 client 持有一个有效锁，可继续在锁期内构造并提交注册交易。
func TriggerDomainRegisterLock(ctx context.Context, rt *Runtime, p TriggerDomainRegisterLockParams) (TriggerDomainRegisterLockResult, error) {
	var out TriggerDomainRegisterLockResult
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(rt.runIn.ClientID) == "" {
		return out, fmt.Errorf("client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := ensureDomainPeerConnected(ctx, rt, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	targetPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.TargetPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("target_pubkey_hex invalid: %w", err)
	}

	queryOut, err := triggerDomainQueryName(ctx, rt, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut.Response
	out.Name = name
	out.OwnerPubkeyHex = strings.TrimSpace(rt.runIn.ClientID)
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

	lockResp, err := triggerDomainRegisterLock(ctx, rt, resolverPubkeyHex, name, targetPubkeyHex, queryResp.RegisterLockFeeSatoshi)
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
	quote, err := verifyRegisterQuote(resolverPubkeyHex, lockResp.SignedQuoteJSON)
	if err != nil {
		return out, err
	}
	if !strings.EqualFold(quote.Name, name) {
		return out, fmt.Errorf("register quote name mismatch")
	}
	if !strings.EqualFold(quote.OwnerPubkeyHex, rt.runIn.ClientID) {
		return out, fmt.Errorf("register quote owner mismatch")
	}
	if !strings.EqualFold(quote.TargetPubkeyHex, targetPubkeyHex) {
		return out, fmt.Errorf("register quote target mismatch")
	}
	out.Ok = true
	out.Code = "LOCKED"
	out.OwnerPubkeyHex = strings.TrimSpace(rt.runIn.ClientID)
	out.TargetPubkeyHex = targetPubkeyHex
	out.RegisterPriceSatoshi = quote.RegisterPriceSatoshi
	out.RegisterSubmitFeeSatoshi = quote.RegisterSubmitFeeSatoshi
	out.TotalRegisterPaySatoshi = quote.TotalPaySatoshi
	return out, nil
}

// TriggerDomainPrepareRegister 通过 domain 系统完成“查询 -> 锁名 -> 本地构交易”，但不提交交易。
// 设计意图：
// - 让 e2e 可以验证 lock_missing、broadcast_failed 等 RegisterSubmit 边界；
// - 返回的 tx_hex 仍然是按正式注册流程构造的真实交易，可直接再提交给 domain。
func TriggerDomainPrepareRegister(ctx context.Context, rt *Runtime, p TriggerDomainPrepareRegisterParams) (TriggerDomainPrepareRegisterResult, error) {
	var out TriggerDomainPrepareRegisterResult
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(rt.runIn.ClientID) == "" {
		return out, fmt.Errorf("client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := ensureDomainPeerConnected(ctx, rt, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	targetPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.TargetPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("target_pubkey_hex invalid: %w", err)
	}

	queryOut, err := triggerDomainQueryName(ctx, rt, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut.Response
	out.Name = name
	out.OwnerPubkeyHex = strings.TrimSpace(rt.runIn.ClientID)
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

	lockResp, err := triggerDomainRegisterLock(ctx, rt, resolverPubkeyHex, name, targetPubkeyHex, queryResp.RegisterLockFeeSatoshi)
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

	quote, err := verifyRegisterQuote(resolverPubkeyHex, lockResp.SignedQuoteJSON)
	if err != nil {
		return out, err
	}
	if !strings.EqualFold(quote.Name, name) {
		return out, fmt.Errorf("register quote name mismatch")
	}
	if !strings.EqualFold(quote.OwnerPubkeyHex, rt.runIn.ClientID) {
		return out, fmt.Errorf("register quote owner mismatch")
	}
	if !strings.EqualFold(quote.TargetPubkeyHex, targetPubkeyHex) {
		return out, fmt.Errorf("register quote target mismatch")
	}
	out.RegisterPriceSatoshi = quote.RegisterPriceSatoshi
	out.RegisterSubmitFeeSatoshi = quote.RegisterSubmitFeeSatoshi
	out.TotalRegisterPaySatoshi = quote.TotalPaySatoshi

	allocMu := rt.walletAllocMutex()
	allocMu.Lock()
	defer allocMu.Unlock()

	registerTxRaw, registerTxID, err := buildDomainRegisterTx(rt, lockResp.SignedQuoteJSON, quote)
	if err != nil {
		return out, err
	}
	out.Ok = true
	out.Code = "PREPARED"
	out.TargetPubkeyHex = targetPubkeyHex
	out.RegisterTxID = registerTxID
	out.RegisterTxHex = hex.EncodeToString(registerTxRaw)
	return out, nil
}

// TriggerDomainSubmitPreparedRegister 把已准备好的正式注册交易提交给 domain。
// 设计意图：
// - 与 TriggerDomainPrepareRegister 配合，覆盖“锁还在 / 锁过期 / 广播失败”三类 RegisterSubmit 行为；
// - 提交成功后，仍然会把本地已广播交易投影回钱包，保持与正式注册入口一致。
func TriggerDomainSubmitPreparedRegister(ctx context.Context, rt *Runtime, p TriggerDomainSubmitPreparedRegisterParams) (TriggerDomainSubmitPreparedRegisterResult, error) {
	var out TriggerDomainSubmitPreparedRegisterResult
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(rt.runIn.ClientID) == "" {
		return out, fmt.Errorf("client identity not initialized")
	}
	resolverPubkeyHex, _, err := ensureDomainPeerConnected(ctx, rt, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	registerTxHex := strings.ToLower(strings.TrimSpace(p.RegisterTxHex))
	if registerTxHex == "" {
		return out, fmt.Errorf("register_tx_hex is required")
	}
	registerTxRaw, err := hex.DecodeString(registerTxHex)
	if err != nil {
		return out, fmt.Errorf("register_tx_hex invalid: %w", err)
	}
	submitResp, err := triggerDomainRegisterSubmit(ctx, rt, resolverPubkeyHex, registerTxRaw)
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
		parsed, parseErr := txsdk.NewTransactionFromBytes(registerTxRaw)
		if parseErr != nil {
			return out, parseErr
		}
		out.RegisterTxID = parsed.TxID().String()
	}
	if err := applyLocalBroadcastWalletTxBytes(rt, registerTxRaw, "domain_register_submit"); err != nil {
		return out, err
	}
	out.Ok = true
	out.Code = "OK"
	return out, nil
}

// TriggerDomainSetTarget 通过 domain 系统更新域名指向。
// 设计说明：
// - 这里先付费查询一次，拿到最新状态与精确 set_target 费率，避免用 publish 最大值粗扣；
// - 只有当前 owner 才允许更新 target。
func TriggerDomainSetTarget(ctx context.Context, rt *Runtime, p TriggerDomainSetTargetParams) (TriggerDomainSetTargetResult, error) {
	var out TriggerDomainSetTargetResult
	if rt == nil || rt.Host == nil || rt.ActionChain == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	if strings.TrimSpace(rt.runIn.ClientID) == "" {
		return out, fmt.Errorf("client identity not initialized")
	}
	resolverPubkeyHex, resolverPeerID, err := ensureDomainPeerConnected(ctx, rt, p.ResolverPubkeyHex, p.ResolverAddr)
	if err != nil {
		return out, err
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	targetPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.TargetPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("target_pubkey_hex invalid: %w", err)
	}

	queryOut, err := triggerDomainQueryName(ctx, rt, resolverPubkeyHex, resolverPeerID, name)
	if err != nil {
		return out, fmt.Errorf("domain query step failed: %w", err)
	}
	queryResp := queryOut.Response
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
	if !strings.EqualFold(queryResp.OwnerPubkeyHex, rt.runIn.ClientID) {
		out.Code = "FORBIDDEN"
		out.Message = "only owner can update target"
		out.OwnerPubkeyHex = strings.TrimSpace(queryResp.OwnerPubkeyHex)
		return out, nil
	}

	resp, err := triggerDomainSetTarget(ctx, rt, resolverPubkeyHex, name, targetPubkeyHex, queryResp.SetTargetFeeSatoshi)
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
	obs.Business("bitcast-client", "evt_trigger_domain_set_target_end", map[string]any{
		"resolver_pubkey_hex":        resolverPubkeyHex,
		"name":                       out.Name,
		"target_pubkey_hex":          out.TargetPubkeyHex,
		"query_fee_charged_satoshi":  out.QueryFeeChargedSatoshi,
		"set_target_charged_satoshi": out.SetTargetChargedSatoshi,
	})
	return out, nil
}

func ensureDomainPeerConnected(ctx context.Context, rt *Runtime, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error) {
	resolverPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(resolverPubkeyHex))
	if err != nil {
		return "", "", fmt.Errorf("resolver_pubkey_hex invalid: %w", err)
	}
	_, resolverPeerID, err := resolveClientTarget(resolverPubkeyHex)
	if err != nil {
		return "", "", err
	}
	if addr := strings.TrimSpace(resolverAddr); addr != "" {
		ai, err := parseAddr(addr)
		if err != nil {
			return "", "", fmt.Errorf("resolver_addr invalid: %w", err)
		}
		if ai.ID != resolverPeerID {
			return "", "", fmt.Errorf("resolver_addr peer id mismatch")
		}
		rt.Host.Peerstore().AddAddrs(ai.ID, ai.Addrs, time.Hour)
		if err := rt.Host.Connect(ctx, *ai); err != nil && rt.Host.Network().Connectedness(ai.ID) != libnetwork.Connected {
			return "", "", fmt.Errorf("connect resolver failed: %w", err)
		}
	}
	if err := ensureTargetPeerReachable(ctx, rt, resolverPubkeyHex, resolverPeerID); err != nil {
		return "", "", err
	}
	return resolverPubkeyHex, resolverPeerID, nil
}

func triggerDomainQueryName(ctx context.Context, rt *Runtime, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (domainQueryResult, error) {
	if rt == nil || rt.Host == nil {
		return domainQueryResult{}, fmt.Errorf("runtime not initialized")
	}
	payload, err := oldproto.Marshal(&domainmodule.NameRouteReq{Name: name})
	if err != nil {
		return domainQueryResult{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          resolverPubkeyHex,
		Route:       domainmodule.RouteDomainV1Query,
		ContentType: ncall.ContentTypeProto,
		Body:        payload,
	})
	if err != nil {
		return domainQueryResult{}, err
	}
	if !callResp.Ok {
		return domainQueryResult{}, fmt.Errorf("domain query route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp domainmodule.QueryNamePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return domainQueryResult{}, fmt.Errorf("decode domain query body failed: %w", err)
	}
	return domainQueryResult{ResolverPeerID: resolverPeerID, Response: resp}, nil
}

func triggerDomainRegisterLock(ctx context.Context, rt *Runtime, resolverPubkeyHex string, name string, targetPubkeyHex string, charge uint64) (domainmodule.RegisterLockPaidResp, error) {
	payload, err := oldproto.Marshal(&domainmodule.NameTargetRouteReq{
		Name:            name,
		TargetPubkeyHex: targetPubkeyHex,
	})
	if err != nil {
		return domainmodule.RegisterLockPaidResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		Route:       domainmodule.RouteDomainV1Lock,
		ContentType: ncall.ContentTypeProto,
		Body:        payload,
	})
	if err != nil {
		return domainmodule.RegisterLockPaidResp{}, err
	}
	if !callResp.Ok {
		return domainmodule.RegisterLockPaidResp{}, fmt.Errorf("domain register lock route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp domainmodule.RegisterLockPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return domainmodule.RegisterLockPaidResp{}, fmt.Errorf("decode domain register lock body failed: %w", err)
	}
	return resp, nil
}

func triggerDomainRegisterSubmit(ctx context.Context, rt *Runtime, resolverPubkeyHex string, registerTx []byte) (domainmodule.RegisterSubmitResp, error) {
	payload, err := oldproto.Marshal(&domainmodule.RegisterSubmitReq{RegisterTx: append([]byte(nil), registerTx...)})
	if err != nil {
		return domainmodule.RegisterSubmitResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		Route:       domainmodule.RouteDomainV1RegisterSubmit,
		ContentType: ncall.ContentTypeProto,
		Body:        payload,
	})
	if err != nil {
		return domainmodule.RegisterSubmitResp{}, err
	}
	if !callResp.Ok {
		return domainmodule.RegisterSubmitResp{}, fmt.Errorf("domain register submit route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var routeResp domainmodule.RegisterSubmitResp
	if err := oldproto.Unmarshal(callResp.Body, &routeResp); err != nil {
		return domainmodule.RegisterSubmitResp{}, fmt.Errorf("decode domain register submit body failed: %w", err)
	}
	return routeResp, nil
}

func triggerDomainSetTarget(ctx context.Context, rt *Runtime, resolverPubkeyHex string, name string, targetPubkeyHex string, charge uint64) (domainmodule.SetTargetPaidResp, error) {
	payload, err := oldproto.Marshal(&domainmodule.NameTargetRouteReq{
		Name:            name,
		TargetPubkeyHex: targetPubkeyHex,
	})
	if err != nil {
		return domainmodule.SetTargetPaidResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		Route:       domainmodule.RouteDomainV1SetTarget,
		ContentType: ncall.ContentTypeProto,
		Body:        payload,
	})
	if err != nil {
		return domainmodule.SetTargetPaidResp{}, err
	}
	if !callResp.Ok {
		return domainmodule.SetTargetPaidResp{}, fmt.Errorf("domain set target route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp domainmodule.SetTargetPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return domainmodule.SetTargetPaidResp{}, fmt.Errorf("decode domain set target body failed: %w", err)
	}
	return resp, nil
}

func verifyRegisterQuote(resolverPubkeyHex string, raw []byte) (domainRegisterQuote, error) {
	var out domainRegisterQuote
	resolverPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(resolverPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("resolver_pubkey_hex invalid: %w", err)
	}
	pubRaw, err := hex.DecodeString(resolverPubkeyHex)
	if err != nil {
		return out, fmt.Errorf("decode resolver_pubkey_hex: %w", err)
	}
	pub, err := crypto.UnmarshalSecp256k1PublicKey(pubRaw)
	if err != nil {
		return out, fmt.Errorf("unmarshal resolver public key: %w", err)
	}
	env, err := domainmodule.VerifyEnvelope(raw, pub.Verify)
	if err != nil {
		return out, err
	}
	if len(env.Fields) != 11 {
		return out, fmt.Errorf("register quote fields length mismatch")
	}
	if strings.TrimSpace(asStringField(env.Fields[0])) != domainQuotePayloadVersion {
		return out, fmt.Errorf("register quote version mismatch")
	}
	out = domainRegisterQuote{
		QuoteID:                  strings.TrimSpace(asStringField(env.Fields[1])),
		Name:                     strings.TrimSpace(asStringField(env.Fields[2])),
		OwnerPubkeyHex:           strings.TrimSpace(asStringField(env.Fields[3])),
		TargetPubkeyHex:          strings.TrimSpace(asStringField(env.Fields[4])),
		PayToAddress:             strings.TrimSpace(asStringField(env.Fields[5])),
		RegisterPriceSatoshi:     asUint64Field(env.Fields[6]),
		RegisterSubmitFeeSatoshi: asUint64Field(env.Fields[7]),
		TotalPaySatoshi:          asUint64Field(env.Fields[8]),
		LockExpiresAtUnix:        asInt64Field(env.Fields[9]),
		TermSeconds:              asUint64Field(env.Fields[10]),
	}
	if out.QuoteID == "" || out.Name == "" || out.OwnerPubkeyHex == "" || out.TargetPubkeyHex == "" || out.PayToAddress == "" {
		return out, fmt.Errorf("register quote missing required fields")
	}
	if out.TotalPaySatoshi == 0 {
		return out, fmt.Errorf("register quote total payment is zero")
	}
	if out.LockExpiresAtUnix <= time.Now().Unix() {
		return out, fmt.Errorf("register quote expired")
	}
	return out, nil
}

func buildDomainRegisterTx(rt *Runtime, signedQuoteJSON []byte, quote domainRegisterQuote) ([]byte, string, error) {
	built, err := buildDomainRegisterTxDetailed(rt, signedQuoteJSON, quote)
	if err != nil {
		return nil, "", err
	}
	return built.RawTx, built.TxID, nil
}

func buildDomainRegisterTxDetailed(rt *Runtime, signedQuoteJSON []byte, quote domainRegisterQuote) (builtDomainRegisterTx, error) {
	if rt == nil || rt.ActionChain == nil {
		return builtDomainRegisterTx{}, fmt.Errorf("runtime chain not initialized")
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return builtDomainRegisterTx{}, err
	}
	utxos, err := getWalletUTXOsFromDB(rt)
	if err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("load wallet utxos from snapshot failed: %w", err)
	}
	if len(utxos) == 0 {
		return builtDomainRegisterTx{}, fmt.Errorf("no utxos for client wallet")
	}
	selected := make([]poolcore.UTXO, 0, len(utxos))
	for _, u := range utxos {
		selected = append(selected, u)
		built, buildErr := buildDomainRegisterTxWithUTXOsDetailed(clientActor, selected, quote.PayToAddress, quote.TotalPaySatoshi, signedQuoteJSON)
		if buildErr == nil {
			return built, nil
		}
		if !strings.Contains(strings.ToLower(buildErr.Error()), "insufficient selected utxos") {
			return builtDomainRegisterTx{}, buildErr
		}
	}
	return builtDomainRegisterTx{}, fmt.Errorf("insufficient wallet utxos for register payment")
}

func buildDomainRegisterTxWithUTXOs(actor *poolcore.Actor, selected []poolcore.UTXO, payToAddress string, payAmount uint64, signedQuoteJSON []byte) ([]byte, string, error) {
	built, err := buildDomainRegisterTxWithUTXOsDetailed(actor, selected, payToAddress, payAmount, signedQuoteJSON)
	if err != nil {
		return nil, "", err
	}
	return built.RawTx, built.TxID, nil
}

func buildDomainRegisterTxWithUTXOsDetailed(actor *poolcore.Actor, selected []poolcore.UTXO, payToAddress string, payAmount uint64, signedQuoteJSON []byte) (builtDomainRegisterTx, error) {
	if actor == nil {
		return builtDomainRegisterTx{}, fmt.Errorf("client actor not initialized")
	}
	if len(selected) == 0 {
		return builtDomainRegisterTx{}, fmt.Errorf("selected utxos is empty")
	}
	if payAmount == 0 {
		return builtDomainRegisterTx{}, fmt.Errorf("register payment amount is zero")
	}
	clientAddr, err := script.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("parse client address failed: %w", err)
	}
	lockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("build client lock script failed: %w", err)
	}
	prevLockHex := hex.EncodeToString(lockScript.Bytes())
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("build client unlock template failed: %w", err)
	}

	total := sumUTXOValue(selected)
	txBuilder := txsdk.NewTransaction()
	for _, u := range selected {
		if err := txBuilder.AddInputFrom(u.TxID, u.Vout, prevLockHex, u.Value, unlockTpl); err != nil {
			return builtDomainRegisterTx{}, fmt.Errorf("add register input failed: %w", err)
		}
	}
	if err := txBuilder.PayToAddress(strings.TrimSpace(payToAddress), payAmount); err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("build register payment output failed: %w", err)
	}
	opReturnScript, err := domainmodule.BuildSignedEnvelopeOpReturnScript(signedQuoteJSON)
	if err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("build register quote op_return failed: %w", err)
	}
	txBuilder.AddOutput(&txsdk.TransactionOutput{
		Satoshis:      0,
		LockingScript: opReturnScript,
	})

	hasChangeOutput := total > payAmount
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      total - payAmount,
			LockingScript: lockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("pre-sign register tx failed: %w", err)
	}
	// 注册交易这里统一按 sat/KB 估算，不再把 0.5 这种费率误读成 sat/B。
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), 0.5)
	if total <= payAmount+fee {
		return builtDomainRegisterTx{}, fmt.Errorf("insufficient selected utxos for register tx fee: have=%d need=%d", total, payAmount+fee)
	}
	change := total - payAmount - fee
	if hasChangeOutput {
		if change == 0 {
			txBuilder.Outputs = txBuilder.Outputs[:len(txBuilder.Outputs)-1]
		} else {
			txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = change
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return builtDomainRegisterTx{}, fmt.Errorf("final-sign register tx failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return builtDomainRegisterTx{
		RawTx:           append([]byte(nil), txBuilder.Bytes()...),
		TxID:            txID,
		MinerFeeSatoshi: fee,
		ChangeSatoshi:   change,
	}, nil
}

func asStringField(v any) string {
	switch x := v.(type) {
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

func asUint64Field(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case uint32:
		return uint64(x)
	case uint:
		return uint64(x)
	case int64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case int:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case json.Number:
		n, _ := x.Int64()
		if n < 0 {
			return 0
		}
		return uint64(n)
	default:
		return 0
	}
}

func asInt64Field(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	default:
		return 0
	}
}
