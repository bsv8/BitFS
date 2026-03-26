package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/domainsvc"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const ProtoResolverResolve protocol.ID = domainsvc.ProtoResolveNamePaid

type TriggerResolverResolveParams struct {
	ResolverPubkeyHex string
	Name              string
}

type resolverResolveResp struct {
	Ok              bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok"`
	Code            string `protobuf:"bytes,2,opt,name=code,proto3" json:"code,omitempty"`
	Message         string `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Name            string `protobuf:"bytes,4,opt,name=name,proto3" json:"name,omitempty"`
	TargetPubkeyHex string `protobuf:"bytes,5,opt,name=target_pubkey_hex,json=targetPubkeyHex,proto3" json:"target_pubkey_hex,omitempty"`
	UpdatedAtUnix   int64  `protobuf:"varint,6,opt,name=updated_at_unix,json=updatedAtUnix,proto3" json:"updated_at_unix,omitempty"`
}

// registerResolverHandlers 现在保留为空壳。
// 设计说明：
// - resolver 已经搬正为独立 domain 系统；
// - BitFS client 不再承载本地 resolver 数据面，只保留调用入口。
func registerResolverHandlers(_ *Runtime) {}

func TriggerResolverResolve(ctx context.Context, rt *Runtime, p TriggerResolverResolveParams) (resolverResolveResp, error) {
	var out resolverResolveResp
	if rt == nil || rt.Host == nil {
		return out, fmt.Errorf("runtime not initialized")
	}
	resolverPubkeyHex, err := normalizeCompressedPubKeyHex(strings.TrimSpace(p.ResolverPubkeyHex))
	if err != nil {
		return out, fmt.Errorf("resolver_pubkey_hex invalid: %w", err)
	}
	name, err := normalizeResolverNameCanonical(p.Name)
	if err != nil {
		return out, err
	}
	_, resolverPeerID, err := resolveClientTarget(resolverPubkeyHex)
	if err != nil {
		return out, err
	}
	if err := ensureTargetPeerReachable(ctx, rt, resolverPubkeyHex, resolverPeerID); err != nil {
		return out, err
	}

	// 单元测试常用最小 runtime 不会装配真实费用池；这条退路只服务包内测试。
	if strings.TrimSpace(rt.runIn.ClientID) == "" || rt.ActionChain == nil {
		return callResolverResolveDirect(ctx, rt, resolverPeerID, name)
	}

	info, gw, err := fetchDomainFeePoolInfo(ctx, rt, resolverPeerID)
	if err != nil {
		return out, err
	}
	charge := info.SingleQueryFeeSatoshi
	if charge == 0 {
		charge = 1
	}

	payMu := rt.feePoolPayMutex(gw.ID.String())
	payMu.Lock()
	defer payMu.Unlock()
	if _, err := ensureDomainFeePoolSessionForChargeLocked(ctx, rt, gw, info, charge); err != nil {
		return out, err
	}

	session, ok := rt.getFeePool(gw.ID.String())
	if !ok || session == nil || strings.TrimSpace(session.SpendTxID) == "" {
		return out, fmt.Errorf("fee pool session missing for resolver=%s", gw.ID.String())
	}
	resolverPub := rt.Host.Peerstore().PubKey(gw.ID)
	if resolverPub == nil {
		return out, fmt.Errorf("missing resolver pubkey")
	}
	raw, err := resolverPub.Raw()
	if err != nil {
		return out, err
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return out, err
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return out, err
	}
	nextSeq := session.Sequence + 1
	nextServerAmount := session.ServerAmount + charge
	updatedTx, err := ce.LoadTx(session.CurrentTxHex, nil, nextSeq, nextServerAmount, serverPub, clientActor.PubKey, session.PoolAmountSat)
	if err != nil {
		return out, err
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return out, err
	}

	var resp domainsvc.ResolveNamePaidResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, domainsvc.ProtoResolveNamePaid, domainSec(rt), domainsvc.ResolveNamePaidReq{
		ClientID:            rt.runIn.ClientID,
		Name:                name,
		SpendTxID:           session.SpendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		ChargeAmountSatoshi: charge,
		Fee:                 session.SpendTxFeeSat,
		ClientSignature:     append([]byte(nil), (*clientSig)...),
		ChargeReason:        "resolver_query_fee",
	}, &resp); err != nil {
		return out, err
	}
	if resp.Success {
		applyFeePoolChargeToSession(session, nextSeq, nextServerAmount, updatedTx.Hex())
		appendWalletFundFlowFromContext(ctx, rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + session.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           session.SpendTxID,
			Stage:           "use_resolver_query",
			Direction:       "out",
			Purpose:         "resolver_query_fee",
			AmountSatoshi:   -int64(charge),
			UsedSatoshi:     int64(charge),
			ReturnedSatoshi: 0,
			RelatedTxID:     strings.TrimSpace(resp.UpdatedTxID),
			Note:            fmt.Sprintf("resolver_pubkey_hex=%s name=%s", resolverPubkeyHex, name),
			Payload:         resp,
		})
	}

	out = resolverResolveResp{
		Ok:              resp.Success,
		Code:            strings.ToUpper(strings.TrimSpace(resp.Status)),
		Message:         strings.TrimSpace(resp.Error),
		Name:            strings.TrimSpace(resp.Name),
		TargetPubkeyHex: strings.TrimSpace(resp.TargetPubkeyHex),
		UpdatedAtUnix:   resp.ExpireAtUnix,
	}
	if out.Code == "" {
		if out.Ok {
			out.Code = "OK"
		} else {
			out.Code = "ERROR"
		}
	}
	if out.Ok {
		obs.Business("bitcast-client", "evt_trigger_domain_resolve_end", map[string]any{
			"resolver_pubkey_hex":    resolverPubkeyHex,
			"name":                   name,
			"target_pubkey_hex":      out.TargetPubkeyHex,
			"charged_amount_satoshi": charge,
			"updated_txid":           strings.TrimSpace(resp.UpdatedTxID),
		})
	}
	return out, nil
}

func fetchDomainFeePoolInfo(ctx context.Context, rt *Runtime, pid peer.ID) (dual2of2.InfoResp, peer.AddrInfo, error) {
	var info dual2of2.InfoResp
	gw := peer.AddrInfo{ID: pid, Addrs: rt.Host.Peerstore().Addrs(pid)}
	if err := p2prpc.CallProto(ctx, rt.Host, pid, dual2of2.ProtoFeePoolInfo, domainSec(rt), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		return dual2of2.InfoResp{}, peer.AddrInfo{}, err
	}
	return info, gw, nil
}

func callResolverResolveDirect(ctx context.Context, rt *Runtime, pid peer.ID, name string) (resolverResolveResp, error) {
	var resp domainsvc.ResolveNamePaidResp
	err := p2prpc.CallProto(ctx, rt.Host, pid, domainsvc.ProtoResolveNamePaid, domainSec(rt), domainsvc.ResolveNamePaidReq{
		Name: name,
	}, &resp)
	if err != nil {
		return resolverResolveResp{}, err
	}
	return resolverResolveResp{
		Ok:              resp.Success,
		Code:            strings.ToUpper(strings.TrimSpace(resp.Status)),
		Message:         strings.TrimSpace(resp.Error),
		Name:            strings.TrimSpace(resp.Name),
		TargetPubkeyHex: strings.TrimSpace(resp.TargetPubkeyHex),
		UpdatedAtUnix:   resp.ExpireAtUnix,
	}, nil
}

func normalizeResolverNameCanonical(raw string) (string, error) {
	value, err := domainsvc.NormalizeName(raw)
	if err != nil {
		return "", err
	}
	if strings.Contains(value, "/") {
		return "", fmt.Errorf("resolver name must not contain /")
	}
	return value, nil
}

func domainSec(rt *Runtime) p2prpc.SecurityConfig {
	network := "test"
	if rt != nil && strings.TrimSpace(rt.runIn.BSV.Network) != "" {
		network = strings.ToLower(strings.TrimSpace(rt.runIn.BSV.Network))
	}
	return p2prpc.SecurityConfig{Domain: "bitcast-domain", Network: network, TTL: 30 * time.Second, Trace: rt.rpcTrace}
}
