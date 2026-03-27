package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	txsdk "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/infra/payflow"
	"github.com/libp2p/go-libp2p/core/peer"
)

const feePoolSessionStatusSuspicious = "suspicious"

type expectedServiceReceipt struct {
	ServiceType        string
	SpendTxID          string
	SequenceNumber     uint32
	AcceptedChargeHash string
	ResultCode         string
	ResultPayloadBytes []byte
}

func verifyServiceReceiptOrFreeze(ctx context.Context, rt *Runtime, gatewayPeerID peer.ID, session *feePoolSession, mergedCurrentTx []byte, expected expectedServiceReceipt, receiptRaw []byte) error {
	if session == nil {
		return fmt.Errorf("fee pool session missing")
	}
	if len(receiptRaw) == 0 {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_missing")
		return fmt.Errorf("service receipt missing")
	}
	receipt, err := payflow.UnmarshalServiceReceipt(receiptRaw)
	if err != nil {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_decode_failed")
		return fmt.Errorf("decode service receipt failed: %w", err)
	}
	gatewayPub, err := gatewayPublicKeyFromPeer(rt, gatewayPeerID)
	if err != nil {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "gateway_pubkey_missing")
		return err
	}
	if err := payflow.VerifyServiceReceiptSignature(receipt, gatewayPub); err != nil {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_signature_invalid")
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ServiceType), strings.TrimSpace(expected.ServiceType)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_type_mismatch")
		return fmt.Errorf("service receipt type mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ClientPubkeyHex), strings.TrimSpace(rt.runIn.ClientID)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_client_mismatch")
		return fmt.Errorf("service receipt client mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.SpendTxID), strings.TrimSpace(expected.SpendTxID)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_spend_mismatch")
		return fmt.Errorf("service receipt spend_txid mismatch")
	}
	if receipt.SequenceNumber != expected.SequenceNumber {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_sequence_mismatch")
		return fmt.Errorf("service receipt sequence mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.AcceptedChargeHash), strings.TrimSpace(expected.AcceptedChargeHash)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_accept_hash_mismatch")
		return fmt.Errorf("service receipt accepted_charge_hash mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ResultCode), strings.TrimSpace(expected.ResultCode)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_result_code_mismatch")
		return fmt.Errorf("service receipt result code mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(receipt.ResultPayloadHash), payflow.HashPayloadBytes(expected.ResultPayloadBytes)) {
		freezeFeePoolSessionForReceipt(rt, gatewayPeerID, session, mergedCurrentTx, "service_receipt_payload_hash_mismatch")
		return fmt.Errorf("service receipt payload hash mismatch")
	}
	return nil
}

func gatewayPublicKeyFromPeer(rt *Runtime, gatewayPeerID peer.ID) (*ec.PublicKey, error) {
	if rt == nil || rt.Host == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	pub := rt.Host.Peerstore().PubKey(gatewayPeerID)
	if pub == nil {
		return nil, fmt.Errorf("missing gateway pubkey")
	}
	raw, err := pub.Raw()
	if err != nil {
		return nil, err
	}
	return ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
}

func freezeFeePoolSessionForReceipt(rt *Runtime, gatewayPeerID peer.ID, session *feePoolSession, mergedCurrentTx []byte, reason string) {
	if rt == nil || session == nil {
		return
	}
	mergedHex := strings.ToLower(hex.EncodeToString(mergedCurrentTx))
	if mergedHex != "" {
		session.CurrentTxHex = mergedHex
	}
	session.Status = feePoolSessionStatusSuspicious
	session.SuspiciousReason = strings.TrimSpace(reason)
	session.SuspiciousAtUnix = time.Now().Unix()
	rt.setFeePool(gatewayPeerID.String(), session)
	armSuspiciousFeePoolSettlementTask(rt, gatewayPeerID, session)
}

func armSuspiciousFeePoolSettlementTask(rt *Runtime, gatewayPeerID peer.ID, session *feePoolSession) {
	if rt == nil || session == nil || rt.ActionChain == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(rt)
	if scheduler == nil {
		return
	}
	gwID := strings.TrimSpace(gatewayPeerID.String())
	spendTxID := strings.TrimSpace(session.SpendTxID)
	if gwID == "" || spendTxID == "" {
		return
	}
	taskName := "fee_pool_suspicious_settle:" + gwID + ":" + spendTxID
	_ = scheduler.RegisterOrReplacePeriodicTask(context.Background(), periodicTaskSpec{
		Name:      taskName,
		Owner:     "fee_pool",
		Mode:      "dynamic",
		Interval:  15 * time.Second,
		Immediate: true,
		Timeout:   20 * time.Second,
		Run: func(ctx context.Context, trigger string) (map[string]any, error) {
			return runSuspiciousFeePoolSettlement(ctx, rt, gwID, spendTxID)
		},
	})
}

func runSuspiciousFeePoolSettlement(_ context.Context, rt *Runtime, gatewayPeerID string, spendTxID string) (map[string]any, error) {
	if rt == nil || rt.ActionChain == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	session, ok := rt.getFeePool(gatewayPeerID)
	if !ok || session == nil {
		return map[string]any{"status": "session_missing"}, nil
	}
	if !strings.EqualFold(strings.TrimSpace(session.SpendTxID), strings.TrimSpace(spendTxID)) {
		return map[string]any{"status": "session_rotated"}, nil
	}
	if strings.TrimSpace(session.Status) != feePoolSessionStatusSuspicious {
		return map[string]any{"status": "not_suspicious"}, nil
	}
	if strings.TrimSpace(session.FinalTxID) != "" {
		return map[string]any{"status": "already_broadcast", "final_txid": session.FinalTxID}, nil
	}
	expireHeight, hasHeight, err := feePoolSessionExpireHeight(session.CurrentTxHex)
	if err != nil {
		return nil, err
	}
	if !hasHeight {
		return nil, fmt.Errorf("fee pool current tx expire height missing")
	}
	tip, err := rt.ActionChain.GetTipHeight()
	if err != nil {
		return nil, err
	}
	if tip < expireHeight {
		return map[string]any{"status": "waiting_expire_height", "tip_height": tip, "expire_height": expireHeight}, nil
	}
	finalTxID, err := rt.ActionChain.Broadcast(session.CurrentTxHex)
	if err != nil {
		return nil, fmt.Errorf("broadcast suspicious fee pool current tx failed: %w", err)
	}
	if err := applyLocalBroadcastWalletTx(rt, session.CurrentTxHex, "fee_pool_suspicious_expiry_settle"); err != nil {
		return nil, fmt.Errorf("project suspicious fee pool current tx failed: %w", err)
	}
	session.FinalTxID = strings.TrimSpace(finalTxID)
	session.Status = "closed"
	rt.setFeePool(gatewayPeerID, session)
	appendWalletFundFlow(rt.DB, walletFundFlowEntry{
		FlowID:          "fee_pool:" + session.SpendTxID,
		FlowType:        "fee_pool",
		RefID:           session.SpendTxID,
		Stage:           "suspicious_expiry_settle",
		Direction:       "settle",
		Purpose:         "fee_pool_suspicious_expiry_settle",
		AmountSatoshi:   0,
		UsedSatoshi:     int64(session.ServerAmount),
		ReturnedSatoshi: int64(session.ClientAmount),
		RelatedTxID:     session.FinalTxID,
		Note:            strings.TrimSpace(session.SuspiciousReason),
		Payload: map[string]any{
			"gateway_peer_id":    gatewayPeerID,
			"suspicious_at_unix": session.SuspiciousAtUnix,
			"suspicious_reason":  session.SuspiciousReason,
			"expire_height":      expireHeight,
		},
	})
	return map[string]any{"status": "broadcasted", "final_txid": session.FinalTxID, "expire_height": expireHeight, "tip_height": tip}, nil
}

func feePoolSessionExpireHeight(txHex string) (uint32, bool, error) {
	if strings.TrimSpace(txHex) == "" {
		return 0, false, fmt.Errorf("current tx hex required")
	}
	parsed, err := txsdk.NewTransactionFromHex(strings.TrimSpace(txHex))
	if err != nil {
		return 0, false, fmt.Errorf("parse current tx failed: %w", err)
	}
	lockTime := parsed.LockTime
	if lockTime == 0 || lockTime == 0xffffffff {
		return 0, false, nil
	}
	if lockTime >= 500000000 {
		return 0, false, nil
	}
	return lockTime, true, nil
}
