package pool_2of2_v1

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/infra/ncall"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/poolcore"
	libs "github.com/bsv8/MultisigPool/pkg/libs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	"github.com/libp2p/go-libp2p/core/peer"
	oldproto "github.com/golang/protobuf/proto"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type service struct {
	host  moduleapi.Host
	store Store
}

func newService(host moduleapi.Host, store Store) *service {
	return &service{
		host:  host,
		store: store,
	}
}

func (s *service) getPaymentHost() (moduleapi.PaymentHost, error) {
	h, ok := s.host.(moduleapi.PaymentHost)
	if !ok {
		return nil, fmt.Errorf("host does not implement PaymentHost")
	}
	return h, nil
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

	paymentHost, err := s.getPaymentHost()
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, err
	}

	peerID, err := peer.Decode(targetPeer)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("decode target peer failed: %w", err)
	}

	gatewayPub, err := paymentHost.GetGatewayPubkey(peerID)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get gateway pubkey failed: %w", err)
	}

	actor, err := paymentHost.Actor()
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get actor failed: %w", err)
	}

	quoteMsg, err := payflow.UnmarshalServiceQuote(quote)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("unmarshal quote failed: %w", err)
	}

	session, found, err := s.store.GetActiveSession(ctx, targetPeer)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get active session failed: %w", err)
	}
	if !found {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("no active session for peer")
	}

	utxos, err := paymentHost.WalletUTXOs(ctx)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("get wallet utxos failed: %w", err)
	}

	total := sumUTXOValue(utxos)
	if total < quoteMsg.ChargeAmountSatoshi {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("insufficient wallet balance")
	}

	selected := s.selectUTXOs(utxos, quoteMsg.ChargeAmountSatoshi)
	isMainnet := s.isMainnet(actor)

	result, err := ce.BuildDualFeePoolBaseTx(&selected, quoteMsg.ChargeAmountSatoshi, actor.PrivKey, gatewayPub, isMainnet, float64(0.5))
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("build base tx failed: %w", err)
	}

	signedTx, err := ce.SpendTXDualFeePoolClientSign(result.Tx, quoteMsg.ChargeAmountSatoshi, actor.PrivKey, gatewayPub)
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("sign failed: %w", err)
	}

	txID := result.Tx.TxID().String()
	paymentPayload, err := oldproto.Marshal(&ncall.FeePool2of2Payment{
		SpendTxID:           txID,
		SequenceNumber:      session.SequenceNum,
		ChargeAmountSatoshi: quoteMsg.ChargeAmountSatoshi,
		Fee:                 0,
		ClientSignature:     *signedTx,
		ChargeReason:        request.Route,
		ServiceQuote:        quote,
	})
	if err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("marshal payment payload failed: %w", err)
	}

	paidReq := ncall.CallReq{
		To:              targetPeer,
		Route:           request.Route,
		ContentType:     request.ContentType,
		Body:            request.Body,
		PaymentScheme:   ncall.PaymentSchemePool2of2V1,
		PaymentPayload:  paymentPayload,
	}
	var paidResp ncall.CallResp
	if err := paymentHost.SendProto(ctx, targetPeer, request.Route, paidReq, &paidResp); err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("send payment failed: %w", err)
	}

	if strings.TrimSpace(paidResp.PaymentReceiptScheme) != ncall.PaymentSchemePool2of2V1 || len(paidResp.PaymentReceipt) == 0 {
		return moduleapi.ServiceCallResponse{
			OK:          false,
			Code:        "PAYMENT_RECEIPT_MISSING",
			Message:     "payment receipt missing or scheme mismatch",
			ContentType: "text/plain",
		}, nil, nil
	}

	var receipt ncall.FeePool2of2Receipt
	if err := oldproto.Unmarshal(paidResp.PaymentReceipt, &receipt); err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("unmarshal receipt failed: %w", err)
	}

	if receipt.UpdatedTxID == "" {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("receipt updated txid is empty")
	}
	if receipt.ChargedAmountSatoshi != quoteMsg.ChargeAmountSatoshi {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("receipt charged amount mismatch: quote=%d, receipt=%d", quoteMsg.ChargeAmountSatoshi, receipt.ChargedAmountSatoshi)
	}

	now := time.Now().Unix()
	newSequence := session.SequenceNum + 1
	newServerAmount := session.ServerAmountSat + receipt.ChargedAmountSatoshi
	balanceAfterSat := session.ClientAmountSat - quoteMsg.ChargeAmountSatoshi

	entry := EntryRow{
		SessionRef:      session.Ref,
		ChargeReason:   request.Route,
		ChargeAmountSat: quoteMsg.ChargeAmountSatoshi,
		BalanceAfterSat: balanceAfterSat,
		SequenceNum:     newSequence,
		ProofTxID:       receipt.UpdatedTxID,
		ProofAtUnix:     now,
		CreatedAtUnix:   now,
	}
	if err := s.store.ApplyPayment(ctx, session.Ref, newSequence, newServerAmount, receipt.UpdatedTxID, entry); err != nil {
		return moduleapi.ServiceCallResponse{}, nil, fmt.Errorf("apply payment failed: %w", err)
	}

	return moduleapi.ServiceCallResponse{
		OK:          true,
		Code:        "OK",
		Message:     "payment successful",
		ContentType: "application/json",
		Body:        paidResp.Body,
	}, paidResp.PaymentReceipt, nil
}

func (s *service) EnsureCoverage(ctx context.Context, scheme string, targetPeer string, requiredDuration uint64) (string, int64, error) {
	if ctx == nil {
		return "", 0, fmt.Errorf("ctx is required")
	}
	if scheme == "" {
		return "", 0, moduleapi.UnavailableScheme(scheme)
	}
	if targetPeer == "" {
		return "", 0, fmt.Errorf("target peer is required")
	}

	paymentHost, err := s.getPaymentHost()
	if err != nil {
		return "", 0, err
	}

	info, err := paymentHost.PoolInfo(ctx, targetPeer)
	if err != nil {
		return "", 0, fmt.Errorf("get pool info failed: %w", err)
	}

	peerID, err := peer.Decode(targetPeer)
	if err != nil {
		return "", 0, fmt.Errorf("decode target peer failed: %w", err)
	}

	gatewayPub, err := paymentHost.GetGatewayPubkey(peerID)
	if err != nil {
		return "", 0, fmt.Errorf("get gateway pubkey failed: %w", err)
	}

	actor, err := paymentHost.Actor()
	if err != nil {
		return "", 0, fmt.Errorf("get actor failed: %w", err)
	}

	utxos, err := paymentHost.WalletUTXOs(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("get wallet utxos failed: %w", err)
	}
	if len(utxos) == 0 {
		return "", 0, fmt.Errorf("no wallet utxos available")
	}

	targetMinimum := info.MinimumPoolAmountSatoshi
	if targetMinimum < info.SingleCycleFeeSatoshi*10 {
		targetMinimum = info.SingleCycleFeeSatoshi * 10
	}

	selected := s.selectUTXOs(utxos, targetMinimum)
	isMainnet := s.isMainnet(actor)

	result, err := ce.BuildDualFeePoolBaseTx(&selected, targetMinimum, actor.PrivKey, gatewayPub, isMainnet, info.FeeRateSatPerByte)
	if err != nil {
		return "", 0, fmt.Errorf("build base tx failed: %w", err)
	}

	_, err = ce.SpendTXDualFeePoolClientSign(result.Tx, targetMinimum, actor.PrivKey, gatewayPub)
	if err != nil {
		return "", 0, fmt.Errorf("sign failed: %w", err)
	}

	_ = result

	now := time.Now().Unix()
	sessionRef := newSessionRef()
	session := SessionRow{
		Ref:             sessionRef,
		CounterpartyHex: targetPeer,
		SpendTxID:       result.Tx.TxID().String(),
		ClientAmountSat: targetMinimum,
		ServerAmountSat: 0,
		SequenceNum:     1,
		State:           "pending_open",
		CreatedAtUnix:   now,
		UpdatedAtUnix:   now,
	}

	if err := s.store.EnsureSession(ctx, session); err != nil {
		return "", 0, fmt.Errorf("persist session failed: %w", err)
	}

	return sessionRef, session.UpdatedAtUnix + int64(info.BillingCycleSeconds), nil
}

func (s *service) RenewCoverage(ctx context.Context, sessionRef string, additionalDuration uint64) (int64, error) {
	if ctx == nil {
		return 0, fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return 0, moduleapi.SessionNotFound(sessionRef)
	}

	session, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return 0, fmt.Errorf("get session failed: %w", err)
	}
	if !found {
		return 0, moduleapi.SessionNotFound(sessionRef)
	}

	now := time.Now().Unix()
	newExpiry := session.UpdatedAtUnix + int64(additionalDuration)

	err = s.store.UpdateSession(ctx, sessionRef, func(row *SessionRow) {
		row.UpdatedAtUnix = now
	})
	if err != nil {
		return 0, fmt.Errorf("update session failed: %w", err)
	}

	return newExpiry, nil
}

func (s *service) RotateCoverage(ctx context.Context, scheme string, sessionRef string, newAmount uint64) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return "", moduleapi.SessionNotFound(sessionRef)
	}
	if scheme == "" {
		return "", moduleapi.UnavailableScheme(scheme)
	}

	oldSession, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return "", fmt.Errorf("get old session failed: %w", err)
	}
	if !found {
		return "", moduleapi.SessionNotFound(sessionRef)
	}

	targetMinimum := newAmount
	if targetMinimum == 0 {
		targetMinimum = 500000
	}

	newSessionRef, _, err := s.EnsureCoverage(ctx, scheme, oldSession.CounterpartyHex, targetMinimum)
	if err != nil {
		return "", fmt.Errorf("create new session failed: %w", err)
	}

	err = s.store.UpdateSession(ctx, sessionRef, func(row *SessionRow) {
		row.State = "rotated"
		row.ClosedAtUnix = time.Now().Unix()
	})
	if err != nil {
		return "", fmt.Errorf("close old session failed: %w", err)
	}

	return newSessionRef, nil
}

func (s *service) CloseCoverage(ctx context.Context, sessionRef string, reason string) error {
	if ctx == nil {
		return fmt.Errorf("ctx is required")
	}
	if sessionRef == "" {
		return moduleapi.SessionNotFound(sessionRef)
	}

	_, found, err := s.store.GetSessionByRef(ctx, sessionRef)
	if err != nil {
		return fmt.Errorf("get session failed: %w", err)
	}
	if !found {
		return moduleapi.SessionNotFound(sessionRef)
	}

	now := time.Now().Unix()
	err = s.store.UpdateSession(ctx, sessionRef, func(row *SessionRow) {
		row.State = "pending_close"
		row.ClosedAtUnix = now
		row.UpdatedAtUnix = now
	})
	if err != nil {
		return fmt.Errorf("close session failed: %w", err)
	}

	return nil
}

func (s *service) OnCoverageStateChange(callback func(sessionRef, oldState, newState string, timestamp int64)) {
}

func (s *service) selectUTXOs(utxos []poolcore.UTXO, target uint64) []libs.UTXO {
	var selected []libs.UTXO
	var total uint64
	for _, u := range utxos {
		selected = append(selected, libs.UTXO{
			TxID:  u.TxID,
			Vout:  u.Vout,
			Value: u.Value,
		})
		total += u.Value
		if total >= target {
			break
		}
	}
	return selected
}

func (s *service) isMainnet(actor *poolcore.Actor) bool {
	if actor == nil {
		return false
	}
	return strings.Contains(strings.ToLower(actor.Name), "main")
}

func newSessionRef() string {
	return fmt.Sprintf("ps_%d", time.Now().UnixNano())
}

func sumUTXOValue(utxos []poolcore.UTXO) uint64 {
	var total uint64
	for _, u := range utxos {
		total += u.Value
	}
	return total
}