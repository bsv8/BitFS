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
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	domainwire "github.com/bsv8/BitFS/pkg/clientapp/modules/domain/domainwire"
	oldproto "github.com/golang/protobuf/proto"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	libnetwork "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type domainQueryResult struct {
	ResolverPeerID peer.ID
	Response       contractmessage.QueryNamePaidResp
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

// recordDomainRegisterAccountingAfterBroadcast 域名注册提交成功后的收口动作。
//
// 设计说明：
// - 先把已广播交易投影回本地钱包事实；
// - 再把链支付账一次写完，避免 finalize 还去追旧的事后补账；
// - 这里是 domain register 专用的提交即入账边界，不再给别的入口偷用。
func recordDomainRegisterAccountingAfterBroadcast(ctx context.Context, store *clientDB, rt *Runtime, registerTxRaw []byte, registerTxID string, resolverPubkeyHex string) error {
	if err := applyLocalBroadcastWalletTxBytes(ctx, store, rt, registerTxRaw, "domain_register_submit"); err != nil {
		return err
	}
	if err := recordChainPaymentAccountingAfterBroadcast(ctx, store, rt, hex.EncodeToString(registerTxRaw), registerTxID, "domain_register", "domain_register_submit", "client:self", "resolver:"+strings.ToLower(strings.TrimSpace(resolverPubkeyHex))); err != nil {
		return err
	}
	return nil
}

func ensureDomainPeerConnected(ctx context.Context, store *clientDB, rt *Runtime, resolverPubkeyHex string, resolverAddr string) (string, peer.ID, error) {
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
	if err := ensureTargetPeerReachable(ctx, store, rt, resolverPubkeyHex, resolverPeerID); err != nil {
		return "", "", err
	}
	return resolverPubkeyHex, resolverPeerID, nil
}

func triggerDomainQueryName(ctx context.Context, store *clientDB, rt *Runtime, resolverPubkeyHex string, resolverPeerID peer.ID, name string) (domainQueryResult, error) {
	if rt == nil || rt.Host == nil {
		return domainQueryResult{}, fmt.Errorf("runtime not initialized")
	}
	payload, err := oldproto.Marshal(&contractmessage.NameRouteReq{Name: name})
	if err != nil {
		return domainQueryResult{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          resolverPubkeyHex,
		ProtocolID:  protocol.ID(ncall.ProtoDomainQueryNamePaid),
		ContentType: contractmessage.ContentTypeProto,
		Body:        payload,
		Store:       store,
	})
	if err != nil {
		return domainQueryResult{}, err
	}
	if !callResp.Ok {
		return domainQueryResult{}, fmt.Errorf("domain query route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp contractmessage.QueryNamePaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return domainQueryResult{}, fmt.Errorf("decode domain query body failed: %w", err)
	}
	return domainQueryResult{ResolverPeerID: resolverPeerID, Response: resp}, nil
}

func triggerDomainRegisterLock(ctx context.Context, store *clientDB, rt *Runtime, resolverPubkeyHex string, name string, targetPubkeyHex string, charge uint64) (contractmessage.RegisterLockPaidResp, error) {
	payload, err := oldproto.Marshal(&contractmessage.NameTargetRouteReq{
		Name:            name,
		TargetPubkeyHex: targetPubkeyHex,
	})
	if err != nil {
		return contractmessage.RegisterLockPaidResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		ProtocolID:  protocol.ID(ncall.ProtoDomainRegisterLock),
		ContentType: contractmessage.ContentTypeProto,
		Body:        payload,
		Store:       store,
	})
	if err != nil {
		return contractmessage.RegisterLockPaidResp{}, err
	}
	if !callResp.Ok {
		return contractmessage.RegisterLockPaidResp{}, fmt.Errorf("domain register lock route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp contractmessage.RegisterLockPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.RegisterLockPaidResp{}, fmt.Errorf("decode domain register lock body failed: %w", err)
	}
	return resp, nil
}

func triggerDomainRegisterSubmit(ctx context.Context, rt *Runtime, store *clientDB, resolverPubkeyHex string, registerTx []byte) (contractmessage.RegisterSubmitResp, error) {
	payload, err := oldproto.Marshal(&contractmessage.RegisterSubmitReq{RegisterTx: append([]byte(nil), registerTx...)})
	if err != nil {
		return contractmessage.RegisterSubmitResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		ProtocolID:  protocol.ID(ncall.ProtoDomainRegisterSubmit),
		ContentType: contractmessage.ContentTypeProto,
		Body:        payload,
		Store:       store,
	})
	if err != nil {
		return contractmessage.RegisterSubmitResp{}, err
	}
	if !callResp.Ok {
		return contractmessage.RegisterSubmitResp{}, fmt.Errorf("domain register submit route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var routeResp contractmessage.RegisterSubmitResp
	if err := oldproto.Unmarshal(callResp.Body, &routeResp); err != nil {
		return contractmessage.RegisterSubmitResp{}, fmt.Errorf("decode domain register submit body failed: %w", err)
	}
	return routeResp, nil
}

func triggerDomainSetTarget(ctx context.Context, store *clientDB, rt *Runtime, resolverPubkeyHex string, name string, targetPubkeyHex string, charge uint64) (contractmessage.SetTargetPaidResp, error) {
	payload, err := oldproto.Marshal(&contractmessage.NameTargetRouteReq{
		Name:            name,
		TargetPubkeyHex: targetPubkeyHex,
	})
	if err != nil {
		return contractmessage.SetTargetPaidResp{}, err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:          strings.TrimSpace(resolverPubkeyHex),
		ProtocolID:  protocol.ID(ncall.ProtoDomainSetTargetPaid),
		ContentType: contractmessage.ContentTypeProto,
		Body:        payload,
		Store:       store,
	})
	if err != nil {
		return contractmessage.SetTargetPaidResp{}, err
	}
	if !callResp.Ok {
		return contractmessage.SetTargetPaidResp{}, fmt.Errorf("domain set target route failed: code=%s message=%s", strings.TrimSpace(callResp.Code), strings.TrimSpace(callResp.Message))
	}
	var resp contractmessage.SetTargetPaidResp
	if err := oldproto.Unmarshal(callResp.Body, &resp); err != nil {
		return contractmessage.SetTargetPaidResp{}, fmt.Errorf("decode domain set target body failed: %w", err)
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
	env, err := domainwire.VerifyEnvelope(raw, pub.Verify)
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

func buildDomainRegisterTx(ctx context.Context, store *clientDB, rt *Runtime, signedQuoteJSON []byte, quote domainRegisterQuote) ([]byte, string, error) {
	built, err := buildDomainRegisterTxDetailed(ctx, store, rt, signedQuoteJSON, quote)
	if err != nil {
		return nil, "", err
	}
	return built.RawTx, built.TxID, nil
}

func buildDomainRegisterTxDetailed(ctx context.Context, store *clientDB, rt *Runtime, signedQuoteJSON []byte, quote domainRegisterQuote) (builtDomainRegisterTx, error) {
	if rt == nil || rt.ActionChain == nil {
		return builtDomainRegisterTx{}, fmt.Errorf("runtime chain not initialized")
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return builtDomainRegisterTx{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return builtDomainRegisterTx{}, fmt.Errorf("runtime not initialized")
	}
	utxos, err := getWalletUTXOsFromDB(ctx, store, rt)
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
	opReturnScript, err := domainwire.BuildSignedEnvelopeOpReturnScript(signedQuoteJSON)
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
