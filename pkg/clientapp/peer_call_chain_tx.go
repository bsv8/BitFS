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
	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/obs"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type peerCallChainTxQuoteBuilt struct {
	GatewayPub       *ec.PublicKey
	QuoteStatus      string
	ServiceQuoteRaw  []byte
	ServiceQuote     payflow.ServiceQuote
	ServiceQuoteHash string
	ChargeReason     string
}

type builtPeerCallChainTx struct {
	RawTx           []byte
	TxID            string
	MinerFeeSatoshi uint64
	ChangeSatoshi   uint64
}

func quotePeerCallWithChainTx(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption) (ncall.CallResp, peerCallChainTxQuoteBuilt, error) {
	if rt == nil || rt.Host == nil {
		return ncall.CallResp{}, peerCallChainTxQuoteBuilt{}, fmt.Errorf("runtime not initialized")
	}
	if option == nil {
		return ncall.CallResp{}, peerCallChainTxQuoteBuilt{}, fmt.Errorf("payment option missing")
	}
	quoted, err := requestPeerCallChainTxQuote(ctx, rt, store, peerID, req, option)
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

func requestPeerCallChainTxQuote(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption) (peerCallChainTxQuoteBuilt, error) {
	if rt == nil || rt.Host == nil {
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("runtime not initialized")
	}
	serviceParamsPayload, err := buildPeerCallServiceParamsPayload(strings.TrimSpace(req.Route), req.Body)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, peerID)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	offer := payflow.ServiceOffer{
		ServiceType:          peerCallQuotedServiceType(req.Route),
		ServiceNodePubkeyHex: strings.ToLower(hex.EncodeToString(gatewayPub.Compressed())),
		ClientPubkeyHex:      strings.ToLower(strings.TrimSpace(rt.runIn.ClientID)),
		RequestParams:        append([]byte(nil), serviceParamsPayload...),
		CreatedAtUnix:        time.Now().Unix(),
	}
	rawOffer, err := payflow.MarshalServiceOffer(offer)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	resp, err := callNodeServiceQuote(ctx, rt, peerID, poolcore.ServiceQuoteReq{
		ClientID:             rt.runIn.ClientID,
		ServiceOffer:         rawOffer,
		ServiceParamsPayload: append([]byte(nil), serviceParamsPayload...),
	})
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	if !resp.Success {
		msg := strings.TrimSpace(resp.Error)
		if msg == "" {
			msg = "service quote rejected"
		}
		return peerCallChainTxQuoteBuilt{}, fmt.Errorf("service quote rejected: status=%s error=%s", strings.TrimSpace(resp.Status), msg)
	}
	quote, quoteHash, err := poolcore.ParseAndVerifyServiceQuote(resp.ServiceQuote, gatewayPub)
	if err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	if err := poolcore.ValidateServiceQuoteBinding(quote, offer, offer.ServiceNodePubkeyHex, rt.runIn.ClientID, peerCallQuotedServiceType(req.Route), serviceParamsPayload, time.Now().Unix()); err != nil {
		return peerCallChainTxQuoteBuilt{}, err
	}
	chargeReason := strings.TrimSpace(req.Route)
	if chargeReason == "" {
		chargeReason = strings.TrimSpace(option.Description)
	}
	return peerCallChainTxQuoteBuilt{
		GatewayPub:       gatewayPub,
		QuoteStatus:      strings.TrimSpace(resp.Status),
		ServiceQuoteRaw:  append([]byte(nil), resp.ServiceQuote...),
		ServiceQuote:     quote,
		ServiceQuoteHash: quoteHash,
		ChargeReason:     chargeReason,
	}, nil
}

func payPeerCallWithChainTxQuote(ctx context.Context, rt *Runtime, store *clientDB, peerID peer.ID, req ncall.CallReq, option *ncall.PaymentOption, quoted peerCallChainTxQuoteBuilt) (ncall.CallResp, error) {
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
	out, err := callNodeRoute(ctx, rt, peerID, paidReq)
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
	if err := applyLocalBroadcastWalletTxBytes(ctx, store, rt, built.RawTx, "peer_call_chain_tx"); err != nil {
		return ncall.CallResp{}, err
	}
	// 资金流水已迁移到 fact_* 事实表组装
	obs.Business("bitcast-client", "evt_trigger_peer_call_chain_tx_end", map[string]any{
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
	return out, nil
}

func buildPeerCallChainTx(ctx context.Context, store *clientDB, rt *Runtime, gatewayPub *ec.PublicKey, rawQuote []byte, quote payflow.ServiceQuote) (builtPeerCallChainTx, error) {
	if rt == nil || rt.ActionChain == nil {
		return builtPeerCallChainTx{}, fmt.Errorf("runtime not initialized")
	}
	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return builtPeerCallChainTx{}, err
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
		built, buildErr := buildPeerCallChainTxWithUTXOs(clientActor, selected, gatewayPub, rawQuote, quote.ChargeAmountSatoshi, strings.EqualFold(strings.TrimSpace(rt.runIn.BSV.Network), "main"))
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
