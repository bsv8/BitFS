package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	sighash "github.com/bsv-blockchain/go-sdk/transaction/sighash"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	ncall "github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	"github.com/bsv8/BitFS/pkg/clientapp/infra/pproto"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type peerCallChainTxQuoteBuilt struct {
	GatewayPub       *ec.PublicKey
	QuoteStatus      string
	ServiceQuoteRaw  []byte
	ServiceQuote     payflow.ServiceQuote
	ServiceQuoteHash string
	ChargeReason     string
}

// expectedServiceReceipt payflow receipts schema 已删除，保留结构体定义仅供编译通过
type expectedServiceReceipt struct {
	ServiceType        string
	OfferHash          string
	ResultPayloadBytes []byte
}

type builtPeerCallChainTx struct {
	RawTx           []byte
	TxID            string
	MinerFeeSatoshi uint64
	ChangeSatoshi   uint64
}

func quotePeerCallWithChainTx(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, protoID protocol.ID) (ncall.CallResp, peerCallChainTxQuoteBuilt, error) {
	if rt == nil || rt.Host == nil {
		return ncall.CallResp{}, peerCallChainTxQuoteBuilt{}, fmt.Errorf("runtime not initialized")
	}
	if option == nil {
		return ncall.CallResp{}, peerCallChainTxQuoteBuilt{}, fmt.Errorf("payment option missing")
	}
	quoted, err := requestPeerCallChainTxQuote(ctx, rt, store, peerID, req, option, protoID)
	if err != nil {
		return ncall.CallResp{}, peerCallChainTxQuoteBuilt{}, err
	}
	quantity, unit := describePeerCallQuoteQuantity(quoted.ServiceQuote)
	return ncall.CallResp{
		Ok:             false,
		Code:           "PAYMENT_QUOTED",
		Message:        "payment quote ready",
		PaymentSchemes: []*ncall.PaymentOption{decorateQuotedPeerCallPaymentOption(option, quoted.ServiceQuote.ChargeAmountSatoshi, quoted.ChargeReason, quoted.QuoteStatus, quantity, unit)},
		ServiceQuote:   append([]byte(nil), quoted.ServiceQuoteRaw...),
	}, quoted, nil
}

func requestPeerCallChainTxQuote(ctx context.Context, rt *Runtime, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, protoID protocol.ID) (peerCallChainTxQuoteBuilt, error) {
	if rt == nil || rt.Host == nil {
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("runtime not initialized")
	}
	serviceParamsPayload, err := buildPeerCallServiceParamsPayload(strings.TrimSpace(req.Route), req.Body)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	quotedResp, err := callGatewayRoute(ctx, rt, peerID, protoID, ncall.CallReq{
		To:          buildPeerCallQuoteTarget(req),
		ContentType: strings.TrimSpace(req.ContentType),
		Body:        append([]byte(nil), req.Body...),
	})
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	if strings.TrimSpace(quotedResp.Code) != "PAYMENT_QUOTED" {
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("payment quote unavailable: code=%s message=%s", strings.TrimSpace(quotedResp.Code), strings.TrimSpace(quotedResp.Message))
	}
	if len(quotedResp.PaymentSchemes) == 0 || quotedResp.PaymentSchemes[0] == nil {
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("payment schemes missing")
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, peerID)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	offer := payflow.ServiceOffer{
		ServiceType:          peerCallQuotedServiceType(req.Route),
		ServiceNodePubkeyHex: strings.ToLower(hex.EncodeToString(gatewayPub.Compressed())),
		ClientPubkeyHex:      identity.ClientIDLower,
		RequestParams:        append([]byte(nil), serviceParamsPayload...),
		CreatedAtUnix:        time.Now().Unix(),
	}
	if len(quotedResp.ServiceQuote) == 0 {
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("service quote empty")
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(quotedResp.ServiceQuote, gatewayPub)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, offer, offer.ServiceNodePubkeyHex, identity.ClientID, peerCallQuotedServiceType(req.Route), serviceParamsPayload, time.Now().Unix()); err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	chargeReason := strings.TrimSpace(req.Route)
	if chargeReason == "" {
		chargeReason = strings.TrimSpace(option.Description)
	}
	return peerCallChainTxQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      strings.TrimSpace(quotedResp.PaymentSchemes[0].QuoteStatus),
		ServiceQuoteRaw:  append([]byte(nil), quotedResp.ServiceQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: quoteHash,
		ChargeReason:     chargeReason,
	}, nil
}

func payPeerCallWithChainTxQuote(ctx context.Context, rt *Runtime, store ClientStore, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted peerCallChainTxQuoteBuilt, protoID protocol.ID) (ncall.CallResp, error) {
	built, err := buildPeerCallChainTx(ctx, store, rt, quoted.GatewayPub, quoted.ServiceQuoteRaw, quoted.ServiceQuote)
	if err != nil {
		return ncall.CallResp{}, err
	}
	paymentPayload, err := oldproto.Marshal(&ncall.ChainTxV1Payment{
		RawTx: append([]byte(nil), built.RawTx...),
	})
	if err != nil {
		return ncall.CallResp{}, err
	}
	paidReq := req
	paidReq.PaymentScheme = ncall.PaymentSchemeChainTxV1
	paidReq.PaymentPayload = paymentPayload
	out, err := callGatewayRoute(ctx, rt, peerID, protoID, paidReq)
	if err != nil {
		return ncall.CallResp{}, err
	}
	if strings.TrimSpace(out.PaymentReceiptScheme) != ncall.PaymentSchemeChainTxV1 || len(out.PaymentReceipt) == 0 {
		return ncall.CallResp{}, fmt.Errorf("payment receipt missing")
	}
	var receipt ncall.ChainTxV1Receipt
	if err := oldproto.Unmarshal(out.PaymentReceipt, &receipt); err != nil {
		return ncall.CallResp{}, err
	}
	if txid := strings.ToLower(strings.TrimSpace(receipt.PaymentTxID)); txid != "" && !strings.EqualFold(txid, built.TxID) {
		return ncall.CallResp{}, fmt.Errorf("payment receipt txid mismatch")
	}
	rawStore, ok := any(store).(moduleBootstrapStore)
	if !ok || rawStore == nil {
		return out, fmt.Errorf("submitted_unknown_projection: client store raw capability missing")
	}
	if err := applyLocalBroadcastWalletTxBytes(ctx, rawStore, rt, built.RawTx, "peer_call_chain_tx"); err != nil {
		return out, fmt.Errorf("submitted_unknown_projection: %w", err)
	}
	// 资金流水已迁移到 fact_* 事实表组装
	obs.Business(ServiceName, "evt_trigger_peer_call_chain_tx_end", map[string]any{
		"quote_charge_sat":     quoted.ServiceQuote.ChargeAmountSatoshi,
		"miner_fee_sat":        built.MinerFeeSatoshi,
		"change_satoshi":       built.ChangeSatoshi,
		"payment_receipt_txid": strings.TrimSpace(receipt.PaymentTxID),
	})
	resultPayloadBytes, err := expectedPeerCallResultPayload(req.Route, out.Body)
	if err != nil {
		return ncall.CallResp{}, err
	}
	if err := verifyPeerCallServiceReceipt(rt, peerID, expectedServiceReceipt{
		ServiceType:        peerCallReceiptServiceType(req.Route),
		OfferHash:          quoted.ServiceQuote.OfferHash,
		ResultPayloadBytes: resultPayloadBytes,
	}, out.ServiceReceipt); err != nil {
		return ncall.CallResp{}, err
	}
	if err := recordChainPaymentAccountingAfterBroadcast(ctx, store, rt, hex.EncodeToString(built.RawTx), built.TxID, "quote_pay", "quote_pay", "wallet:self", "external:unknown"); err != nil {
		return ncall.CallResp{}, err
	}
	return out, nil
}

func buildPeerCallChainTx(ctx context.Context, store ClientStore, rt *Runtime, gatewayPub *ec.PublicKey, rawQuote []byte, quote payflow.ServiceQuote) (builtPeerCallChainTx, error) {
	if rt == nil || rt.ActionChain == nil {
		return builtPeerCallChainTx{}, fmt.Errorf("runtime not initialized")
	}
	identity, err := rt.runtimeIdentity()
	if err != nil {
		return builtPeerCallChainTx{}, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return builtPeerCallChainTx{}, fmt.Errorf("runtime not initialized")
	}
	utxos, err := getWalletUTXOsFromDB(ctx, store, rt)
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("load wallet utxos from snapshot failed: %w", err)
	}
	if len(utxos) == 0 {
		return builtPeerCallChainTx{}, fmt.Errorf("no utxos for client wallet")
	}
	selected := make([]poolcore.UTXO, 0, len(utxos))
	for _, u := range utxos {
		selected = append(selected, u)
		built, buildErr := buildPeerCallChainTxWithUTXOs(clientActor, selected, gatewayPub, rawQuote, quote.ChargeAmountSatoshi, identity.IsMainnet)
		if buildErr == nil {
			return built, nil
		}
		if !strings.Contains(strings.ToLower(buildErr.Error()), "insufficient selected utxos") {
			return builtPeerCallChainTx{}, buildErr
		}
	}
	return builtPeerCallChainTx{}, fmt.Errorf("insufficient wallet utxos for peer call payment")
}

func buildPeerCallChainTxWithUTXOs(actor *poolcore.Actor, selected []poolcore.UTXO, gatewayPub *ec.PublicKey, rawQuote []byte, payAmount uint64, isMainnet bool) (builtPeerCallChainTx, error) {
	if actor == nil {
		return builtPeerCallChainTx{}, fmt.Errorf("client actor not initialized")
	}
	if len(selected) == 0 {
		return builtPeerCallChainTx{}, fmt.Errorf("selected utxos is empty")
	}
	if gatewayPub == nil {
		return builtPeerCallChainTx{}, fmt.Errorf("gateway pubkey missing")
	}
	if len(rawQuote) == 0 {
		return builtPeerCallChainTx{}, fmt.Errorf("service quote missing")
	}
	if payAmount == 0 {
		return builtPeerCallChainTx{}, fmt.Errorf("peer call payment amount is zero")
	}
	clientAddr, err := script.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("parse client address failed: %w", err)
	}
	clientLockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("build client lock script failed: %w", err)
	}
	prevLockHex := hex.EncodeToString(clientLockScript.Bytes())
	sigHash := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHash)
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("build client unlock template failed: %w", err)
	}
	gatewayAddr, err := script.NewAddressFromPublicKey(gatewayPub, isMainnet)
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("derive gateway chain address failed: %w", err)
	}
	total := sumUTXOValue(selected)
	txBuilder := txsdk.NewTransaction()
	for _, u := range selected {
		if err := txBuilder.AddInputFrom(u.TxID, u.Vout, prevLockHex, u.Value, unlockTpl); err != nil {
			return builtPeerCallChainTx{}, fmt.Errorf("add peer call input failed: %w", err)
		}
	}
	if err := txBuilder.PayToAddress(gatewayAddr.AddressString, payAmount); err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("build peer call payment output failed: %w", err)
	}
	opReturnScript, err := payflow.BuildDataOpReturnScript(rawQuote)
	if err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("build peer call quote op_return failed: %w", err)
	}
	txBuilder.AddOutput(&txsdk.TransactionOutput{
		Satoshis:      0,
		LockingScript: opReturnScript,
	})
	hasChangeOutput := total > payAmount
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      total - payAmount,
			LockingScript: clientLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return builtPeerCallChainTx{}, fmt.Errorf("pre-sign peer call tx failed: %w", err)
	}
	// 设计说明：
	// - chain_tx_v1 也是客户端本地临时构造的独立交易；
	// - 这里和 domain register 走同一套 sat/KB 估算口径，避免不同业务各自漂移。
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), 0.5)
	if total <= payAmount+fee {
		return builtPeerCallChainTx{}, fmt.Errorf("insufficient selected utxos for peer call tx fee: have=%d need=%d", total, payAmount+fee)
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
		return builtPeerCallChainTx{}, fmt.Errorf("final-sign peer call tx failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return builtPeerCallChainTx{
		RawTx:           append([]byte(nil), txBuilder.Bytes()...),
		TxID:            txID,
		MinerFeeSatoshi: fee,
		ChangeSatoshi:   change,
	}, nil
}

func callGatewayRoute(ctx context.Context, rt *Runtime, peerID peer.ID, protoID protocol.ID, req ncall.CallReq) (ncall.CallResp, error) {
	var out ncall.CallResp
	// 这条链的接收端挂在 gateway 的 proto 域名下，不是 bitfs-node。
	if err := pproto.CallProto(ctx, rt.Host, peerID, protoID, gwSec(rt.rpcTrace), req, &out); err != nil {
		return ncall.CallResp{}, err
	}
	return out, nil
}

func verifyPeerCallServiceReceipt(rt *Runtime, gatewayPeerID peer.ID, expected expectedServiceReceipt, receiptRaw []byte) error {
	if len(receiptRaw) == 0 {
		return fmt.Errorf("service receipt missing")
	}
	receipt, err := payflow.UnmarshalServiceReceipt(receiptRaw)
	if err != nil {
		return fmt.Errorf("decode service receipt failed: %w", err)
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, gatewayPeerID)
	if err != nil {
		return err
	}
	if err := payflow.VerifyServiceReceiptSignature(receipt, gatewayPub); err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ServiceType), strings.TrimSpace(expected.ServiceType)) {
		return fmt.Errorf("service receipt type mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.OfferHash), strings.TrimSpace(expected.OfferHash)) {
		return fmt.Errorf("service receipt offer_hash mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ResultHash), payflow.HashPayloadBytes(expected.ResultPayloadBytes)) {
		return fmt.Errorf("service receipt payload hash mismatch")
	}
	return nil
}

// Group 8: 以下函数依赖已删除的 settlement layer，stub 实现
func describePeerCallQuoteQuantity(quote payflow.ServiceQuote) (string, string) {
	return "1", "call"
}

func decorateQuotedPeerCallPaymentOption(option *ncall.PaymentOption, chargeSat uint64, chargeReason string, quoteStatus string, quantity string, unit string) *ncall.PaymentOption {
	if option == nil {
		return nil
	}
	return &ncall.PaymentOption{
		Scheme:          option.Scheme,
		PaymentDomain:   option.PaymentDomain,
		AmountSatoshi:   chargeSat,
		Description:     fmt.Sprintf("charge:%d reason:%s status:%s qty:%s", chargeSat, chargeReason, quoteStatus, quantity),
		PricingMode:     option.PricingMode,
		ServiceQuantity: option.ServiceQuantity,
	}
}

func buildPeerCallServiceParamsPayload(route string, body []byte) ([]byte, error) {
	return body, nil
}

func buildPeerCallQuoteTarget(req ncall.CallReq) string {
	return strings.TrimSpace(req.Route)
}

func gatewayPublicKeyFromPeer(rt *Runtime, peerID peer.ID) (*ec.PublicKey, error) {
	return nil, fmt.Errorf("gateway public key lookup not available")
}

func peerCallQuotedServiceType(route string) string {
	return strings.TrimSpace(route)
}

func peerCallReceiptServiceType(route string) string {
	return strings.TrimSpace(route)
}

func expectedPeerCallResultPayload(route string, body []byte) ([]byte, error) {
	return body, nil
}
