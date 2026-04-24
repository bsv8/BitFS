package chain_tx_v1

import (
	"context"
	"crypto/sha256"
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
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type service struct {
	host  moduleapi.Host
	store Store
}

func newService(host moduleapi.Host, store Store) *service {
	return &service{host: host, store: store}
}

func (s *service) PayQuotedService(ctx context.Context, scheme string, quote []byte, request moduleapi.ServiceCallRequest, targetPeer string) (moduleapi.ServiceCallResponse, []byte, error) {
	if ctx == nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("ctx is required")
	}
	if scheme == "" {
		return moduleapi.ServiceCallResponse{}, nil, moduleapi.UnavailableScheme(scheme)
	}
	if request.Route == "" {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("route is required")
	}
	if targetPeer == "" {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("target peer is required")
	}

	paymentHost, ok := s.host.(moduleapi.PaymentHost)
	if !ok {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("host does not implement PaymentHost")
	}

	actor, err := paymentHost.Actor()
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get actor failed: %w", err)
	}

	utxos, err := paymentHost.WalletUTXOs(ctx)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get wallet utxos failed: %w", err)
	}
	if len(utxos) == 0 {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("no wallet utxos available")
	}

	if len(quote) == 0 {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("quote is required")
	}

	peerID, err := peer.Decode(targetPeer)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("decode target peer failed: %w", err)
	}

	gatewayPub, err := s.getGatewayPubkey(ctx, peerID)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get gateway pubkey failed: %w", err)
	}

	quoteInfo, err := s.parseQuote(quote, gatewayPub)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("parse quote failed: %w", err)
	}

	built, err := s.buildChainTx(actor, utxos, gatewayPub, quote, quoteInfo, request.Route)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("build chain tx failed: %w", err)
	}

	paymentPayload, err := oldproto.Marshal(&ncall.ChainTxV1Payment{
		RawTx: built.RawTx,
	})
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("marshal payment payload failed: %w", err)
	}

	paidReq := ncall.CallReq{
		To:            targetPeer,
		Route:         request.Route,
		ContentType:   request.ContentType,
		Body:          request.Body,
		PaymentScheme: ncall.PaymentSchemeChainTxV1,
		PaymentPayload: paymentPayload,
	}
	var paidResp ncall.CallResp
	gatewayPubHex := pubKeyToHex(gatewayPub)
	if err := paymentHost.SendProto(ctx, targetPeer, request.Route, paidReq, &paidResp); err != nil {
		if factErr := s.recordPaymentFact(ctx, built, quoteInfo, "", request.Route, gatewayPubHex, "failed", err.Error()); factErr != nil {
			return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("send payment failed: %s, and record payment fact failed: %w", err.Error(), factErr)
		}
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("send payment failed: %w", err)
	}

	if strings.TrimSpace(paidResp.PaymentReceiptScheme) != ncall.PaymentSchemeChainTxV1 || len(paidResp.PaymentReceipt) == 0 {
		if factErr := s.recordPaymentFact(ctx, built, quoteInfo, "", request.Route, gatewayPubHex, "failed", "receipt missing"); factErr != nil {
			return moduleapi.ServiceCallResponse{
				OK:          false,
				Code:        "PAYMENT_RECEIPT_MISSING",
				Message:     "payment receipt missing or scheme mismatch",
				ContentType: "text/plain",
			}, nil, fmt.Errorf("record payment fact failed: %w", factErr)
		}
		return moduleapi.ServiceCallResponse{
			OK:          false,
			Code:        "PAYMENT_RECEIPT_MISSING",
			Message:     "payment receipt missing or scheme mismatch",
			ContentType: "text/plain",
		}, nil, nil
	}

	var receipt ncall.ChainTxV1Receipt
	if err := oldproto.Unmarshal(paidResp.PaymentReceipt, &receipt); err != nil {
		if factErr := s.recordPaymentFact(ctx, built, quoteInfo, "", request.Route, gatewayPubHex, "failed", "receipt unmarshal failed"); factErr != nil {
			return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("unmarshal receipt failed: %s, and record payment fact failed: %w", err.Error(), factErr)
		}
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("unmarshal receipt failed: %w", err)
	}

	if txid := strings.ToLower(strings.TrimSpace(receipt.PaymentTxID)); txid != "" && !strings.EqualFold(txid, built.TxID) {
		if factErr := s.recordPaymentFact(ctx, built, quoteInfo, "", request.Route, gatewayPubHex, "failed", "txid mismatch"); factErr != nil {
			return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("payment receipt txid mismatch: expected %s, got %s, and record payment fact failed: %w", built.TxID, txid, factErr)
		}
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("payment receipt txid mismatch: expected %s, got %s", built.TxID, txid)
	}

	receiptHash := computeReceiptHash(paidResp.PaymentReceipt)
	if err := s.recordPaymentFact(ctx, built, quoteInfo, receiptHash, request.Route, gatewayPubHex, "submitted", ""); err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("record payment fact failed: %w", err)
	}

	return moduleapi.ServiceCallResponse{
		OK:          true,
		Code:        "OK",
		Message:     "payment successful",
		ContentType: "application/json",
		Body:        paidResp.Body,
	}, paidResp.PaymentReceipt, nil
}

func (s *service) recordPaymentFact(ctx context.Context, built builtChainTx, quoteInfo payflow.ServiceQuote, receiptHash string, route string, gatewayPubHex string, state, errMsg string) error {
	if s.store == nil {
		return fmt.Errorf("store not available")
	}
	quoteHash, err := payflow.HashServiceQuote(quoteInfo)
	if err != nil {
		quoteHash = ""
	}
	now := time.Now().Unix()
	refID := fmt.Sprintf("ctp_%d", now)
	payment := PaymentRow{
		PaymentRefID:   refID,
		TargetPeerHex: gatewayPubHex,
		AmountSatoshi: quoteInfo.ChargeAmountSatoshi,
		State:         state,
		Txid:          built.TxID,
		TxHex:         hex.EncodeToString(built.RawTx),
		MinerFeeSat:   built.MinerFeeSatoshi,
		Route:         route,
		QuoteHash:     quoteHash,
		ReceiptScheme: ncall.PaymentSchemeChainTxV1,
		ReceiptHash:   receiptHash,
		CreatedAtUnix: now,
		UpdatedAtUnix: now,
	}
	if errMsg != "" {
		payment.ErrorMessage = errMsg
	}
	return s.store.RecordPayment(ctx, payment)
}

type builtChainTx struct {
	RawTx           []byte
	TxID            string
	MinerFeeSatoshi uint64
	ChangeSatoshi   uint64
}

func (s *service) getGatewayPubkey(ctx context.Context, peerID peer.ID) (*ec.PublicKey, error) {
	peerHost, ok := s.host.(interface{ GetGatewayPubkey(peer.ID) (*ec.PublicKey, error) })
	if !ok {
		return nil, fmt.Errorf("host does not implement GetGatewayPubkey")
	}
	return peerHost.GetGatewayPubkey(peerID)
}

func (s *service) parseQuote(quote []byte, gatewayPub *ec.PublicKey) (payflow.ServiceQuote, error) {
	parsed, _, err := poolcore.ParseAndVerifyServiceQuote(quote, gatewayPub)
	if err != nil {
		return payflow.ServiceQuote{}, err
	}
	return parsed, nil
}

func (s *service) buildChainTx(actor *poolcore.Actor, utxos []poolcore.UTXO, gatewayPub *ec.PublicKey, rawQuote []byte, quote payflow.ServiceQuote, chargeReason string) (builtChainTx, error) {
	if actor == nil {
		return builtChainTx{}, fmt.Errorf("actor not initialized")
	}
	if len(utxos) == 0 {
		return builtChainTx{}, fmt.Errorf("no utxos available")
	}
	if gatewayPub == nil {
		return builtChainTx{}, fmt.Errorf("gateway pubkey missing")
	}
	if len(rawQuote) == 0 {
		return builtChainTx{}, fmt.Errorf("quote missing")
	}
	if quote.ChargeAmountSatoshi == 0 {
		return builtChainTx{}, fmt.Errorf("charge amount is zero")
	}

	clientAddr, err := script.NewAddressFromString(strings.TrimSpace(actor.Addr))
	if err != nil {
		return builtChainTx{}, fmt.Errorf("parse client address failed: %w", err)
	}
	clientLockScript, err := p2pkh.Lock(clientAddr)
	if err != nil {
		return builtChainTx{}, fmt.Errorf("build client lock script failed: %w", err)
	}
	prevLockHex := hex.EncodeToString(clientLockScript.Bytes())
	sigHashFlag := sighash.Flag(sighash.ForkID | sighash.All)
	unlockTpl, err := p2pkh.Unlock(actor.PrivKey, &sigHashFlag)
	if err != nil {
		return builtChainTx{}, fmt.Errorf("build unlock template failed: %w", err)
	}
	gatewayAddr, err := script.NewAddressFromPublicKey(gatewayPub, actor.Name == "client-main")
	if err != nil {
		return builtChainTx{}, fmt.Errorf("derive gateway address failed: %w", err)
	}

	total := sumUTXOValue(utxos)
	txBuilder := txsdk.NewTransaction()
	for _, u := range utxos {
		if err := txBuilder.AddInputFrom(u.TxID, u.Vout, prevLockHex, u.Value, unlockTpl); err != nil {
			return builtChainTx{}, fmt.Errorf("add input failed: %w", err)
		}
	}
	if err := txBuilder.PayToAddress(gatewayAddr.AddressString, quote.ChargeAmountSatoshi); err != nil {
		return builtChainTx{}, fmt.Errorf("build payment output failed: %w", err)
	}
	opReturnScript, err := payflow.BuildDataOpReturnScript(rawQuote)
	if err != nil {
		return builtChainTx{}, fmt.Errorf("build op_return failed: %w", err)
	}
	txBuilder.AddOutput(&txsdk.TransactionOutput{
		Satoshis:      0,
		LockingScript: opReturnScript,
	})
	hasChangeOutput := total > quote.ChargeAmountSatoshi
	if hasChangeOutput {
		txBuilder.AddOutput(&txsdk.TransactionOutput{
			Satoshis:      total - quote.ChargeAmountSatoshi,
			LockingScript: clientLockScript,
		})
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return builtChainTx{}, fmt.Errorf("pre-sign failed: %w", err)
	}
	fee := estimateMinerFeeSatPerKB(txBuilder.Size(), 0.5)
	if total <= quote.ChargeAmountSatoshi+fee {
		return builtChainTx{}, fmt.Errorf("insufficient utxos for fee: have=%d need=%d", total, quote.ChargeAmountSatoshi+fee)
	}
	change := total - quote.ChargeAmountSatoshi - fee
	if hasChangeOutput {
		if change == 0 {
			txBuilder.Outputs = txBuilder.Outputs[:len(txBuilder.Outputs)-1]
		} else {
			txBuilder.Outputs[len(txBuilder.Outputs)-1].Satoshis = change
		}
	}
	if err := signP2PKHAllInputs(txBuilder, unlockTpl); err != nil {
		return builtChainTx{}, fmt.Errorf("final-sign failed: %w", err)
	}
	txID := strings.ToLower(strings.TrimSpace(txBuilder.TxID().String()))
	return builtChainTx{
		RawTx:           append([]byte(nil), txBuilder.Bytes()...),
		TxID:            txID,
		MinerFeeSatoshi: fee,
		ChangeSatoshi:   change,
	}, nil
}

func sumUTXOValue(utxos []poolcore.UTXO) uint64 {
	var total uint64
	for _, u := range utxos {
		total += u.Value
	}
	return total
}

func signP2PKHAllInputs(tx *txsdk.Transaction, unlockTpl *p2pkh.P2PKH) error {
	for i := range tx.Inputs {
		unlockingScript, err := unlockTpl.Sign(tx, uint32(i))
		if err != nil {
			return err
		}
		tx.Inputs[i].UnlockingScript = unlockingScript
	}
	return nil
}

func estimateMinerFeeSatPerKB(txSize int, feeRate float64) uint64 {
	fee := uint64(float64(txSize) / 1000.0 * feeRate * 1e3)
	if fee == 0 {
		fee = 1
	}
	return fee
}

func pubKeyToHex(pub *ec.PublicKey) string {
	if pub == nil {
		return ""
	}
	return hex.EncodeToString(pub.Compressed())
}

func computeReceiptHash(receiptBytes []byte) string {
	if len(receiptBytes) == 0 {
		return ""
	}
	h := sha256.Sum256(receiptBytes)
	return hex.EncodeToString(h[:])
}