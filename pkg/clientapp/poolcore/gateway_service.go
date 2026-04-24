package poolcore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BitFS/pkg/clientapp/payflow"
	"github.com/bsv8/BitFS/pkg/clientapp/storeactor"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	"github.com/bsv8/MultisigPool/pkg/libs"
)

type GatewayParams struct {
	MinimumPoolAmountSatoshi uint64
	LockBlocks               uint32
	FeeRateSatPerByte        float64
	// PayRejectBeforeExpiryBlocks 控制“临近到期时拒绝继续 pay”的安全窗口（单位：区块）。
	// 取值为 0 时，会使用默认值 1。
	PayRejectBeforeExpiryBlocks uint32

	BillingCycleSeconds      uint32
	SingleCycleFeeSatoshi    uint64
	SinglePublishFeeSatoshi  uint64
	SingleQueryFeeSatoshi    uint64
	RenewNotifyBeforeSeconds uint32
}

type GatewayService struct {
	DB    *sql.DB
	Actor *sqliteactor.Actor
	Chain ChainClient

	ServerPrivHex string
	Params        GatewayParams
	// BoundQuoteChargeReasons 由模块装配阶段注入。
	// 费用池底座只持有“哪些 charge_reason 必须绑定 quote”的策略，不直接依赖具体业务模块。
	BoundQuoteChargeReasons BoundQuoteChargeReasons

	// IsMainnet 控制地址派生与签名脚本等网络相关参数。
	// 说明：BSV 主网/测试网的地址格式不同；公私钥本身不变。
	IsMainnet bool

	mu sync.Mutex
}

const (
	feePoolLockTimeThreshold = uint32(500000000)
	defaultPayGuardBlocks    = uint32(1)
	baseTxSafetyExtraBlocks  = uint32(1)
)

type gatewaySessionLookup struct {
	Row   GatewaySessionRow
	Found bool
}

type gatewayQuoteValidationResult struct {
	Offer     payflow.ServiceOffer
	Quote     payflow.ServiceQuote
	QuoteHash string
}

type gatewayProofPayloadResult struct {
	ProofPayload []byte
	ReceiptProof []byte
}

func (s *GatewayService) Info(clientID string) (InfoResp, error) {
	if _, err := NormalizeClientIDStrict(clientID); err != nil {
		return InfoResp{}, fmt.Errorf("invalid client_pubkey_hex: %w", err)
	}
	return InfoResp{
		Status:                   "ok",
		MinimumPoolAmountSatoshi: s.Params.MinimumPoolAmountSatoshi,
		LockBlocks:               s.Params.LockBlocks,
		FeeRateSatPerByte:        s.Params.FeeRateSatPerByte,
		BillingCycleSeconds:      s.Params.BillingCycleSeconds,
		SingleCycleFeeSatoshi:    s.Params.SingleCycleFeeSatoshi,
		SinglePublishFeeSatoshi:  s.Params.SinglePublishFeeSatoshi,
		SingleQueryFeeSatoshi:    s.Params.SingleQueryFeeSatoshi,
		RenewNotifyBeforeSeconds: s.Params.RenewNotifyBeforeSeconds,
	}, nil
}

func (s *GatewayService) Create(req CreateReq) (CreateResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DB == nil {
		return CreateResp{}, fmt.Errorf("db not initialized")
	}
	if strings.TrimSpace(req.ClientID) == "" {
		return CreateResp{}, fmt.Errorf("client_pubkey_hex required")
	}
	if len(req.SpendTx) == 0 {
		return CreateResp{}, fmt.Errorf("spend_tx required")
	}
	if req.InputAmount == 0 {
		return CreateResp{}, fmt.Errorf("input_amount required")
	}
	if req.SequenceNumber == 0 {
		return CreateResp{}, fmt.Errorf("sequence_number must be >= 1")
	}
	clientBSVPubHex, err := NormalizeClientIDStrict(req.ClientID)
	if err != nil {
		return CreateResp{}, err
	}
	clientPub, err := ec.PublicKeyFromString(clientBSVPubHex)
	if err != nil {
		return CreateResp{}, fmt.Errorf("invalid client secp256k1 pubkey: %w", err)
	}
	serverActor, err := BuildActor("gateway", strings.TrimSpace(s.ServerPrivHex), s.IsMainnet)
	if err != nil {
		return CreateResp{}, err
	}

	spendTxHex := strings.ToLower(hex.EncodeToString(req.SpendTx))
	spendTx, err := tx.NewTransactionFromHex(spendTxHex)
	if err != nil {
		return CreateResp{}, fmt.Errorf("parse spend tx: %w", err)
	}
	expireHeight, hasHeight, err := sessionExpireHeight(spendTxHex)
	if err != nil {
		return CreateResp{}, fmt.Errorf("parse spend tx expire height failed: %w", err)
	}
	if !hasHeight {
		return CreateResp{}, fmt.Errorf("spend tx locktime height required")
	}
	if s.Chain == nil {
		return CreateResp{}, fmt.Errorf("chain not initialized")
	}
	tip, err := s.Chain.GetTipHeight()
	if err != nil {
		return CreateResp{}, fmt.Errorf("query tip height failed: %w", err)
	}
	expect := tip + s.Params.LockBlocks
	if !isHeightWithinTolerance(expect, expireHeight, 1) {
		return CreateResp{}, fmt.Errorf("invalid expire height: tip=%d expect=%d expire=%d", tip, expect, expireHeight)
	}
	clientSig := append([]byte(nil), req.ClientSig...)
	if len(clientSig) == 0 {
		return CreateResp{}, fmt.Errorf("client_signature required")
	}
	ok, err := ce.ServerVerifyClientSpendSig(spendTx, req.InputAmount, serverActor.PubKey, clientPub, &clientSig)
	if err != nil {
		return CreateResp{}, fmt.Errorf("verify client spend sig: %w", err)
	}
	if !ok {
		return CreateResp{}, fmt.Errorf("client spend signature invalid")
	}
	serverSig, err := ce.SpendTXServerSign(spendTx, req.InputAmount, serverActor.PrivKey, clientPub)
	if err != nil {
		return CreateResp{}, fmt.Errorf("server sign spend tx: %w", err)
	}
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(spendTx.Hex(), serverSig, &clientSig)
	if err != nil {
		return CreateResp{}, fmt.Errorf("merge create signatures failed: %w", err)
	}

	spendTxID := spendTx.TxID().String()
	spendTxFee := CalcFeeWithInputAmount(spendTx, req.InputAmount)
	poolAmount := req.InputAmount
	if s.Params.MinimumPoolAmountSatoshi > 0 && poolAmount < s.Params.MinimumPoolAmountSatoshi {
		return CreateResp{}, fmt.Errorf("pool amount %d < minimum_pool_amount %d", poolAmount, s.Params.MinimumPoolAmountSatoshi)
	}
	if req.ServerAmount+spendTxFee > poolAmount {
		return CreateResp{}, fmt.Errorf("invalid initial amounts: server_amount %d + fee %d > pool_amount %d", req.ServerAmount, spendTxFee, poolAmount)
	}

	// 幂等边界：
	// - pending_base_tx / active：允许按同一 spend_txid 重入，便于客户端重试；
	// - frozen / closed / close_submitted 等：必须立刻拒绝，否则客户端会误以为还能继续 base_tx。
	lookup, loadErr := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
		row, found, err := LoadSessionBySpendTxID(db, spendTxID)
		return gatewaySessionLookup{Row: row, Found: found}, err
	})
	if loadErr == nil && lookup.Found {
		old := lookup.Row
		if old.ClientID != clientBSVPubHex {
			return CreateResp{}, fmt.Errorf("spend_txid already exists for another client")
		}
		switch old.LifecycleState {
		case lifecyclePendingBaseTx, lifecycleActive:
		default:
			return CreateResp{}, fmt.Errorf("spend_txid already exists with lifecycle %s", normalizeLifecycleState(old.LifecycleState))
		}
		return CreateResp{
			SpendTxID:     old.SpendTxID,
			ServerSig:     append([]byte(nil), *serverSig...),
			SpendTxFeeSat: old.SpendTxFeeSat,
			PoolAmountSat: old.PoolAmountSat,
		}, nil
	}

	row := GatewaySessionRow{
		SpendTxID:                 spendTxID,
		ClientID:                  clientBSVPubHex,
		ClientBSVCompressedPubHex: clientBSVPubHex,
		ServerBSVCompressedPubHex: strings.ToLower(serverActor.PubHex),
		InputAmountSat:            req.InputAmount,
		PoolAmountSat:             poolAmount,
		SpendTxFeeSat:             spendTxFee,
		Sequence:                  req.SequenceNumber,
		ServerAmountSat:           req.ServerAmount,
		ClientAmountSat:           poolAmount - req.ServerAmount - spendTxFee,
		BaseTxID:                  "",
		FinalTxID:                 "",
		BaseTxHex:                 "",
		CurrentTxHex:              mergedTx.Hex(),
		LifecycleState:            lifecyclePendingBaseTx,
	}
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, InsertSession(db, row)
	}); err != nil {
		return CreateResp{}, err
	}
	obs.Business("bitcast-gateway", "fee_pool_create_ok", map[string]any{
		"spend_txid":   spendTxID,
		"pool_amount":  poolAmount,
		"spend_tx_fee": spendTxFee,
	})
	return CreateResp{
		SpendTxID:     spendTxID,
		ServerSig:     append([]byte(nil), *serverSig...),
		ErrorMessage:  "",
		SpendTxFeeSat: spendTxFee,
		PoolAmountSat: poolAmount,
	}, nil
}

func (s *GatewayService) BaseTx(req BaseTxReq) (BaseTxResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DB == nil || s.Chain == nil {
		return BaseTxResp{}, fmt.Errorf("service not initialized")
	}
	if strings.TrimSpace(req.ClientID) == "" {
		return BaseTxResp{}, fmt.Errorf("client_pubkey_hex required")
	}
	lookup, err := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
		row, found, err := LoadSessionBySpendTxID(db, req.SpendTxID)
		return gatewaySessionLookup{Row: row, Found: found}, err
	})
	if err != nil {
		return BaseTxResp{}, err
	}
	if !lookup.Found {
		return BaseTxResp{Success: false, Status: "not_found", Error: "session not found by spend_txid"}, nil
	}
	row := lookup.Row
	if row.LifecycleState != lifecyclePendingBaseTx && row.LifecycleState != lifecycleActive {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: "session lifecycle does not allow base_tx"}, nil
	}
	if row.BaseTxID != "" && row.LifecycleState == lifecycleActive {
		if err := s.ensureInitialOpenChargeEvent(row); err != nil {
			return BaseTxResp{}, err
		}
		return BaseTxResp{Success: true, Status: "active", BaseTxID: row.BaseTxID}, nil
	}
	tipNow, phase, _, expireHeight, err := s.queryPhase(row)
	if err != nil {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: err.Error()}, nil
	}
	remain := int64(expireHeight) - int64(tipNow)
	guard := int64(guardBlocksOrDefault(s.Params.PayRejectBeforeExpiryBlocks) + baseTxSafetyExtraBlocks)
	if phase != phasePayable || remain <= guard {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: fmt.Sprintf("base tx activation blocked by height window: tip=%d expire=%d remain=%d", tipNow, expireHeight, remain)}, nil
	}

	clientPub, err := ec.PublicKeyFromString(row.ClientBSVCompressedPubHex)
	if err != nil {
		return BaseTxResp{}, fmt.Errorf("invalid stored client pubkey: %w", err)
	}
	serverActor, err := BuildActor("gateway", strings.TrimSpace(s.ServerPrivHex), s.IsMainnet)
	if err != nil {
		return BaseTxResp{}, err
	}

	if len(req.BaseTx) == 0 {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: "base tx required"}, nil
	}
	baseTxHex := strings.ToLower(hex.EncodeToString(req.BaseTx))
	baseTx, err := tx.NewTransactionFromHex(baseTxHex)
	if err != nil {
		return BaseTxResp{}, fmt.Errorf("parse base tx: %w", err)
	}
	multisigScript, err := libs.Lock([]*ec.PublicKey{serverActor.PubKey, clientPub}, 2)
	if err != nil {
		return BaseTxResp{}, fmt.Errorf("build multisig script: %w", err)
	}
	if len(baseTx.Outputs) == 0 {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: "base tx has no outputs"}, nil
	}
	if baseTx.Outputs[0].LockingScript.String() != multisigScript.String() {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: "base tx output[0] locking script mismatch"}, nil
	}
	if baseTx.Outputs[0].Satoshis != row.InputAmountSat {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: fmt.Sprintf("base tx output[0] amount mismatch: got=%d expect=%d", baseTx.Outputs[0].Satoshis, row.InputAmountSat)}, nil
	}
	if bindErr := validateBaseTxMatchesSessionSpend(baseTx, row.CurrentTxHex); bindErr != nil {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: bindErr.Error()}, nil
	}
	baseTxID, err := s.Chain.Broadcast(baseTxHex)
	if err != nil {
		return BaseTxResp{Success: false, Status: row.LifecycleState, Error: "broadcast base tx failed: " + err.Error()}, nil
	}
	row.BaseTxID = baseTxID
	row.BaseTxHex = baseTxHex
	row.LifecycleState = lifecycleActive
	row.FrozenReason = ""
	nowUnix := time.Now().Unix()
	if _, err := gatewayServiceDBTxValue(s, func(sqlTx *sql.Tx) (struct{}, error) {
		if err := FreezeOtherActiveSessionsByClientTx(sqlTx, row.ClientID, row.SpendTxID, "superseded_by_new_pool", nowUnix); err != nil {
			return struct{}{}, err
		}
		if _, err := sqlTx.Exec(
			`UPDATE fee_pool_sessions
			 SET sequence_num=?,server_amount_satoshi=?,client_amount_satoshi=?,base_txid=?,final_txid=?,base_tx_hex=?,current_tx_hex=?,lifecycle_state=?,frozen_reason=?,last_submit_error=?,submit_retry_count=?,last_submit_attempt_at_unix=?,updated_at_unix=?
			 WHERE spend_txid=?`,
			row.Sequence, row.ServerAmountSat, row.ClientAmountSat,
			strings.TrimSpace(row.BaseTxID), strings.TrimSpace(row.FinalTxID),
			row.BaseTxHex, row.CurrentTxHex,
			row.LifecycleState, row.FrozenReason, row.LastSubmitError, row.SubmitRetryCount, row.LastSubmitAttemptAtUnix, nowUnix,
			strings.TrimSpace(row.SpendTxID),
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}); err != nil {
		return BaseTxResp{}, err
	}
	if err := s.ensureInitialOpenChargeEvent(row); err != nil {
		return BaseTxResp{}, err
	}
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, syncServiceStateTx(db, row.ClientID, nil, "session_activated", row.SpendTxID, "")
	}); err != nil {
		return BaseTxResp{}, err
	}
	obs.Business("bitcast-gateway", "fee_pool_base_tx_broadcasted", map[string]any{
		"spend_txid": row.SpendTxID,
		"base_txid":  baseTxID,
	})
	return BaseTxResp{Success: true, Status: "active", BaseTxID: baseTxID}, nil
}

func (s *GatewayService) PayConfirm(req PayConfirmReq) (PayConfirmResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DB == nil {
		return PayConfirmResp{}, fmt.Errorf("db not initialized")
	}
	lookup, err := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
		row, found, err := LoadSessionBySpendTxID(db, req.SpendTxID)
		return gatewaySessionLookup{Row: row, Found: found}, err
	})
	if err != nil {
		return PayConfirmResp{}, err
	}
	if !lookup.Found {
		return PayConfirmResp{Success: false, Status: "not_found", Error: "session not found by spend_txid"}, nil
	}
	row := lookup.Row
	if err := ensureActivePoolGate(row); err != nil {
		return payReject(row.LifecycleState, "not_active_pool", "session lifecycle does not allow pay"), nil
	}
	if !s.isUniqueActiveSession(row.ClientID, row.SpendTxID) {
		return payReject(row.LifecycleState, "not_active_pool", "client has another active pool"), nil
	}
	_, phase, payability, _, err := s.queryPhase(row)
	if err != nil {
		return payReject(row.LifecycleState, "chain_state_unavailable", err.Error()), nil
	}
	if payability != payabilityPayable {
		if phase == phaseNearExpiry {
			return payReject(row.LifecycleState, "pool_near_expiry", "fee pool near expiry"), nil
		}
		return payReject(row.LifecycleState, "pool_should_submit", "fee pool should submit"), nil
	}
	if req.SequenceNumber <= row.Sequence {
		return payReject(row.LifecycleState, "invalid_sequence", fmt.Sprintf("sequence must increase: got=%d current=%d", req.SequenceNumber, row.Sequence)), nil
	}
	if req.ServerAmount <= row.ServerAmountSat {
		return payReject(row.LifecycleState, "invalid_server_amount", fmt.Sprintf("server_amount must increase: got=%d current=%d", req.ServerAmount, row.ServerAmountSat)), nil
	}
	delta := req.ServerAmount - row.ServerAmountSat
	if req.Fee != row.SpendTxFeeSat {
		return payReject(row.LifecycleState, "fee_amount_mismatch", fmt.Sprintf("fee mismatch: got=%d expect=%d", req.Fee, row.SpendTxFeeSat)), nil
	}
	chargeReason := strings.TrimSpace(req.ChargeReason)
	if chargeReason == "" {
		chargeReason = "unspecified"
	}
	var boundQuote payflow.ServiceQuote
	if s.RequiresBoundServiceQuote(chargeReason) {
		if len(req.ServiceQuote) == 0 {
			return payReject(row.LifecycleState, "service_quote_required", "service quote required"), nil
		}
		validation, err := gatewayServiceDBValue(s, func(db *sql.DB) (gatewayQuoteValidationResult, error) {
			offer, quote, quoteHash, err := validateServiceQuoteAgainstPay(db, row, req)
			return gatewayQuoteValidationResult{
				Offer:     offer,
				Quote:     quote,
				QuoteHash: quoteHash,
			}, err
		})
		if err != nil {
			return payReject(row.LifecycleState, "service_quote_invalid", err.Error()), nil
		}
		boundQuote = validation.Quote
	}
	if req.ChargeAmountSatoshi != delta {
		return payReject(row.LifecycleState, "fee_amount_mismatch", fmt.Sprintf("charge amount must equal server_amount delta: charge=%d delta=%d", req.ChargeAmountSatoshi, delta)), nil
	}
	if req.ServerAmount+row.SpendTxFeeSat > row.PoolAmountSat {
		return payReject(row.LifecycleState, "pool_insufficient", fmt.Sprintf("pool cannot cover charge: pool=%d server=%d fee=%d", row.PoolAmountSat, req.ServerAmount, row.SpendTxFeeSat)), nil
	}
	expireBefore, hasBefore, err := sessionExpireHeight(row.CurrentTxHex)
	if err != nil {
		return payReject(row.LifecycleState, "invalid_tx_locktime", "parse current tx expire height failed"), nil
	}
	clientPub, err := ec.PublicKeyFromString(row.ClientBSVCompressedPubHex)
	if err != nil {
		return PayConfirmResp{}, fmt.Errorf("invalid stored client pubkey: %w", err)
	}
	serverActor, err := BuildActor("gateway", strings.TrimSpace(s.ServerPrivHex), s.IsMainnet)
	if err != nil {
		return PayConfirmResp{}, err
	}
	updatedTx, err := ce.LoadTx(
		row.CurrentTxHex,
		nil,
		req.SequenceNumber,
		req.ServerAmount,
		serverActor.PubKey,
		clientPub,
		row.PoolAmountSat,
	)
	if err != nil {
		return payReject(row.LifecycleState, "invalid_tx_update", "rebuild updated tx failed: "+err.Error()), nil
	}
	proofResult, err := gatewayServiceDBValue(s, func(db *sql.DB) (gatewayProofPayloadResult, error) {
		proofPayload, receiptProof, err := buildPayConfirmProofPayload(db, row, req, updatedTx.Hex())
		return gatewayProofPayloadResult{
			ProofPayload: proofPayload,
			ReceiptProof: receiptProof,
		}, err
	})
	if err != nil {
		return payReject(row.LifecycleState, "proof_invalid", err.Error()), nil
	}
	proofPayload := proofResult.ProofPayload
	updatedTxForSign := updatedTx
	if proofPayload != nil {
		updatedTxForSign, err = ce.LoadTxWithProof(
			row.CurrentTxHex,
			nil,
			req.SequenceNumber,
			req.ServerAmount,
			serverActor.PubKey,
			clientPub,
			row.PoolAmountSat,
			proofPayload,
		)
		if err != nil {
			return payReject(row.LifecycleState, "invalid_tx_update", "rebuild proof tx failed: "+err.Error()), nil
		}
	}
	expireAfter, hasAfter, err := sessionExpireHeight(updatedTxForSign.Hex())
	if err != nil {
		return payReject(row.LifecycleState, "invalid_tx_locktime", "parse updated tx expire height failed"), nil
	}
	if hasBefore != hasAfter || (hasBefore && expireBefore != expireAfter) {
		return payReject(row.LifecycleState, "invalid_tx_locktime", fmt.Sprintf("expire height changed: before=%d after=%d", expireBefore, expireAfter)), nil
	}
	if !isSameSpendInput(row.CurrentTxHex, updatedTxForSign.Hex()) {
		return payReject(row.LifecycleState, "invalid_tx_input", "updated tx input outpoint mismatch"), nil
	}
	clientSig := append([]byte(nil), req.ClientSig...)
	if len(clientSig) == 0 {
		return payReject(row.LifecycleState, "signature_required", "signature required"), nil
	}
	ok, err := ce.ServerVerifyClientUpdateSig(updatedTxForSign, serverActor.PubKey, clientPub, &clientSig)
	if err != nil {
		return payReject(row.LifecycleState, "signature_invalid", "verify client update sig failed: "+err.Error()), nil
	}
	if !ok {
		return payReject(row.LifecycleState, "signature_invalid", "client update signature invalid"), nil
	}
	serverSig, err := ce.SpendTXServerSign(updatedTxForSign, row.PoolAmountSat, serverActor.PrivKey, clientPub)
	if err != nil {
		return payReject(row.LifecycleState, "server_sign_failed", "server sign failed: "+err.Error()), nil
	}
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(updatedTxForSign.Hex(), serverSig, &clientSig)
	if err != nil {
		return payReject(row.LifecycleState, "merge_signatures_failed", "merge signatures failed: "+err.Error()), nil
	}
	row.CurrentTxHex = mergedTx.Hex()
	row.Sequence = req.SequenceNumber
	row.ServerAmountSat = req.ServerAmount
	row.ClientAmountSat = row.PoolAmountSat - row.ServerAmountSat - row.SpendTxFeeSat
	nowUnix := time.Now().Unix()
	billingCycleSeconds := s.Params.BillingCycleSeconds
	effectiveUntilUnix := nowUnix
	if chargeReason == "listen_cycle_fee" {
		if req.ChargeAmountSatoshi == 0 {
			return payReject(row.LifecycleState, "service_quote_invalid", "listen cycle charge_amount_satoshi required"), nil
		}
		grantedDuration, err := ListenOfferBudgetToDurationSeconds(req.ChargeAmountSatoshi, s.Params.SingleCycleFeeSatoshi, s.Params.BillingCycleSeconds)
		if err != nil {
			return payReject(row.LifecycleState, "service_quote_invalid", err.Error()), nil
		}
		billingCycleSeconds = uint32(grantedDuration)
		effectiveUntilUnix = nowUnix + int64(grantedDuration)
	}
	if _, err := gatewayServiceDBTxValue(s, func(sqlTx *sql.Tx) (struct{}, error) {
		if err := UpdateSessionTx(sqlTx, row); err != nil {
			return struct{}{}, err
		}
		if err := InsertChargeEventTx(
			sqlTx,
			row.ClientID,
			row.SpendTxID,
			row.Sequence,
			chargeReason,
			req.ChargeAmountSatoshi,
			billingCycleSeconds,
			effectiveUntilUnix,
			row.ClientAmountSat,
		); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}); err != nil {
		return PayConfirmResp{}, err
	}
	updatedTxID := mergedTx.TxID().String()
	obs.Business("bitcast-gateway", "fee_pool_pay_confirm_ok", map[string]any{
		"spend_txid":    row.SpendTxID,
		"updated_txid":  updatedTxID,
		"sequence":      row.Sequence,
		"server_amount": row.ServerAmountSat,
		"reason":        req.ChargeReason,
		"amount":        req.ChargeAmountSatoshi,
	})
	resp := PayConfirmResp{
		Success:           true,
		Status:            row.LifecycleState,
		UpdatedTxID:       updatedTxID,
		Sequence:          row.Sequence,
		ServerAmount:      row.ServerAmountSat,
		ClientAmount:      row.ClientAmountSat,
		MergedCurrentTx:   mergedTx.Bytes(),
		ProofStatePayload: append([]byte(nil), proofPayload...),
	}
	if len(proofPayload) > 0 && strings.TrimSpace(req.ChargeReason) != "" && strings.TrimSpace(boundQuote.OfferHash) != "" {
		serviceReceipt, err := BuildSignedServiceReceipt(s.ServerPrivHex, s.IsMainnet, boundQuote.OfferHash, strings.TrimSpace(req.ChargeReason), nil)
		if err != nil {
			return PayConfirmResp{}, err
		}
		resp.ServiceReceipt = serviceReceipt
	}
	return resp, nil
}

func (s *GatewayService) ensureInitialOpenChargeEvent(row GatewaySessionRow) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("db not initialized")
	}
	if strings.TrimSpace(row.SpendTxID) == "" || strings.TrimSpace(row.ClientID) == "" {
		return nil
	}
	if row.Sequence != 1 || row.ServerAmountSat == 0 {
		return nil
	}
	var exists int
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, db.QueryRow(
			`SELECT COUNT(1) FROM fee_pool_charge_events WHERE client_pubkey_hex=? AND spend_txid=? AND sequence_num=? AND charge_reason=?`,
			row.ClientID, row.SpendTxID, 1, "listen_cycle_fee",
		).Scan(&exists)
	}); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	billingCycleSeconds := s.Params.BillingCycleSeconds
	if billingCycleSeconds == 0 {
		return fmt.Errorf("billing_cycle_seconds must be configured for listen_cycle_fee")
	}
	effectiveUntilUnix := time.Now().Unix() + int64(billingCycleSeconds)
	_, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, InsertChargeEvent(
			db,
			row.ClientID,
			row.SpendTxID,
			1,
			"listen_cycle_fee",
			row.ServerAmountSat,
			billingCycleSeconds,
			effectiveUntilUnix,
			row.ClientAmountSat,
		)
	})
	return err
}

func (s *GatewayService) Close(req CloseReq) (CloseResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DB == nil || s.Chain == nil {
		return CloseResp{}, fmt.Errorf("service not initialized")
	}
	lookup, err := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
		row, found, err := LoadSessionBySpendTxID(db, req.SpendTxID)
		return gatewaySessionLookup{Row: row, Found: found}, err
	})
	if err != nil {
		return CloseResp{}, err
	}
	if !lookup.Found {
		return CloseResp{Success: false, Status: "not_found", Error: "session not found by spend_txid"}, nil
	}
	row := lookup.Row
	if row.LifecycleState == lifecycleClosed && row.FinalTxID != "" {
		return CloseResp{Success: true, Status: "closed", Broadcasted: true, FinalSpendTxID: row.FinalTxID}, nil
	}
	if row.LifecycleState == lifecycleCloseSubmitted && strings.TrimSpace(row.FinalTxID) != "" {
		if err := s.markAcceptedClose(&row, "legacy_close_submitted"); err != nil {
			return CloseResp{}, err
		}
		return CloseResp{Success: true, Status: lifecycleClosed, Broadcasted: true, FinalSpendTxID: row.FinalTxID}, nil
	}
	if row.LifecycleState != lifecycleActive && row.LifecycleState != lifecyclePendingBaseTx && row.LifecycleState != lifecycleShouldSubmit && row.LifecycleState != lifecycleFrozen {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "session lifecycle does not allow close"}, nil
	}
	if req.ServerAmount != row.ServerAmountSat {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: fmt.Sprintf("server_amount mismatch: got=%d expect=%d", req.ServerAmount, row.ServerAmountSat)}, nil
	}
	if req.Fee != row.SpendTxFeeSat {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: fmt.Sprintf("fee mismatch: got=%d expect=%d", req.Fee, row.SpendTxFeeSat)}, nil
	}
	clientPub, err := ec.PublicKeyFromString(row.ClientBSVCompressedPubHex)
	if err != nil {
		return CloseResp{}, fmt.Errorf("invalid stored client pubkey: %w", err)
	}
	serverActor, err := BuildActor("gateway", strings.TrimSpace(s.ServerPrivHex), s.IsMainnet)
	if err != nil {
		return CloseResp{}, err
	}
	finalLockTime := uint32(0xffffffff)
	finalSequence := uint32(0xffffffff)
	finalTx, err := ce.LoadTx(
		row.CurrentTxHex,
		&finalLockTime,
		finalSequence,
		req.ServerAmount,
		serverActor.PubKey,
		clientPub,
		row.PoolAmountSat,
	)
	if err != nil {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "rebuild final tx failed: " + err.Error()}, nil
	}
	clientSig := append([]byte(nil), req.ClientSig...)
	if len(clientSig) == 0 {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "signature required"}, nil
	}
	ok, err := ce.ServerVerifyClientSpendSig(finalTx, row.PoolAmountSat, serverActor.PubKey, clientPub, &clientSig)
	if err != nil {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "verify client final sig failed: " + err.Error()}, nil
	}
	if !ok {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "client final signature invalid"}, nil
	}
	serverSig, err := ce.SpendTXServerSign(finalTx, row.PoolAmountSat, serverActor.PrivKey, clientPub)
	if err != nil {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "server sign final failed: " + err.Error()}, nil
	}
	mergedTx, err := ce.MergeDualPoolSigForSpendTx(finalTx.Hex(), serverSig, &clientSig)
	if err != nil {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "merge final signatures failed: " + err.Error()}, nil
	}
	finalTxID, err := s.Chain.Broadcast(mergedTx.Hex())
	if err != nil {
		return CloseResp{Success: false, Status: row.LifecycleState, Error: "broadcast final tx failed: " + err.Error()}, nil
	}
	nowUnix := time.Now().Unix()
	row.FinalTxID = finalTxID
	row.CurrentTxHex = mergedTx.Hex()
	row.CloseSubmitAtUnix = nowUnix
	if err := s.markAcceptedClose(&row, "cooperative_close_accepted"); err != nil {
		return CloseResp{}, err
	}
	obs.Business("bitcast-gateway", "fee_pool_close_broadcasted", map[string]any{
		"spend_txid":  row.SpendTxID,
		"final_txid":  finalTxID,
		"close_state": row.LifecycleState,
	})
	return CloseResp{Success: true, Status: row.LifecycleState, Broadcasted: true, FinalSpendTxID: finalTxID}, nil
}

func (s *GatewayService) State(req StateReq) (StateResp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DB == nil {
		return StateResp{}, fmt.Errorf("db not initialized")
	}
	clientID, err := NormalizeClientIDStrict(req.ClientID)
	if err != nil {
		return StateResp{}, fmt.Errorf("invalid client_pubkey_hex: %w", err)
	}
	var row GatewaySessionRow
	var found bool
	if strings.TrimSpace(req.SpendTxID) != "" {
		lookup, loadErr := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
			foundRow, found, err := LoadSessionBySpendTxID(db, req.SpendTxID)
			return gatewaySessionLookup{Row: foundRow, Found: found}, err
		})
		row, found, err = lookup.Row, lookup.Found, loadErr
	} else {
		lookup, loadErr := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
			foundRow, found, err := LoadPreferredSessionByClientID(db, clientID)
			return gatewaySessionLookup{Row: foundRow, Found: found}, err
		})
		row, found, err = lookup.Row, lookup.Found, loadErr
		if err == nil && found && row.LifecycleState == lifecycleActive {
			if _, phase, payability, _, pErr := s.queryPhase(row); pErr == nil && phase == phasePayable && payability == payabilityPayable {
				// 命中默认优先池：当前唯一可付费会话。
			} else {
				latestLookup, lErr := gatewayServiceDBValue(s, func(db *sql.DB) (gatewaySessionLookup, error) {
					foundRow, found, err := LoadLatestNonClosedSessionByClientID(db, clientID)
					return gatewaySessionLookup{Row: foundRow, Found: found}, err
				})
				if lErr == nil && latestLookup.Found {
					row = latestLookup.Row
					found = true
				}
			}
		}
	}
	if err != nil {
		return StateResp{}, err
	}
	if !found {
		return StateResp{Status: "not_found"}, nil
	}
	if row.LifecycleState == lifecycleCloseSubmitted && strings.TrimSpace(row.FinalTxID) != "" {
		if fErr := s.markAcceptedClose(&row, "legacy_close_submitted"); fErr != nil {
			obs.Error("bitcast-gateway", "fee_pool_state_finalize_close_failed", map[string]any{
				"spend_txid": row.SpendTxID,
				"error":      fErr.Error(),
			})
		}
	}
	tip, phase, payability, expireHeight, phaseErr := s.queryPhase(row)
	if phaseErr != nil {
		phase = ""
		payability = payabilityBlocked
	}
	currentTx, _ := hex.DecodeString(strings.TrimSpace(row.CurrentTxHex))
	var proofPayload []byte
	if state, ok, pErr := payflow.ExtractProofStateFromTxHex(row.CurrentTxHex); pErr == nil && ok {
		if raw, mErr := payflow.MarshalProofState(state); mErr == nil {
			proofPayload = raw
		}
	}
	return StateResp{
		Status:            row.LifecycleState,
		SpendTxID:         row.SpendTxID,
		BaseTxID:          row.BaseTxID,
		FinalTxID:         row.FinalTxID,
		CurrentTx:         currentTx,
		PoolAmountSat:     row.PoolAmountSat,
		SpendTxFeeSat:     row.SpendTxFeeSat,
		Sequence:          row.Sequence,
		ServerAmountSat:   row.ServerAmountSat,
		ClientAmountSat:   row.ClientAmountSat,
		LifecycleState:    row.LifecycleState,
		Payability:        payability,
		Phase:             phase,
		ExpireHeight:      expireHeight,
		TipHeight:         tip,
		OutpointSpent:     strings.TrimSpace(row.FinalTxID) != "",
		ProofStatePayload: append([]byte(nil), proofPayload...),
	}, nil
}

func (s *GatewayService) RunPassiveCloseLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if err := s.PassiveCloseExpiredOnce(); err != nil {
			obs.Error("bitcast-gateway", "fee_pool_passive_close_tick_failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *GatewayService) PassiveCloseExpiredOnce() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.DB == nil || s.Chain == nil {
		return fmt.Errorf("service not initialized")
	}
	tip, err := s.Chain.GetTipHeight()
	if err != nil {
		return fmt.Errorf("query tip height failed: %w", err)
	}
	// 旧库里可能残留 close_submitted；这里顺手收口为 closed，但新流程不再写入它。
	rows, err := gatewayServiceDBValue(s, func(db *sql.DB) ([]GatewaySessionRow, error) {
		return ListSessionsByLifecycleStates(db, lifecycleActive, lifecycleFrozen, lifecycleShouldSubmit, lifecycleCloseSubmitted)
	})
	if err != nil {
		return err
	}
	for _, row := range rows {
		if row.LifecycleState == lifecycleCloseSubmitted {
			if fErr := s.markAcceptedClose(&row, "legacy_close_submitted"); fErr != nil {
				obs.Error("bitcast-gateway", "fee_pool_close_observe_failed", map[string]any{
					"spend_txid": row.SpendTxID,
					"final_txid": row.FinalTxID,
					"error":      fErr.Error(),
				})
			}
			continue
		}
		expireHeight, hasHeight, parseErr := sessionExpireHeight(row.CurrentTxHex)
		if parseErr != nil {
			obs.Error("bitcast-gateway", "fee_pool_passive_close_parse_failed", map[string]any{
				"spend_txid": row.SpendTxID,
				"error":      parseErr.Error(),
			})
			continue
		}
		if !hasHeight {
			continue
		}
		phase, _ := computePhaseAndPayability(tip, expireHeight, s.Params.PayRejectBeforeExpiryBlocks)
		switch phase {
		case phasePayable:
			if row.LifecycleState != lifecycleActive {
				row.LifecycleState = lifecycleActive
				row.FrozenReason = ""
				if _, uErr := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
					return struct{}{}, UpdateSession(db, row)
				}); uErr != nil {
					return uErr
				}
			}
			continue
		case phaseNearExpiry:
			if row.LifecycleState != lifecycleFrozen {
				row.LifecycleState = lifecycleFrozen
				row.FrozenReason = "near_expiry"
				if _, uErr := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
					return struct{}{}, UpdateSession(db, row)
				}); uErr != nil {
					return uErr
				}
			}
			continue
		}
		// phaseShouldSubmit：统一进入自动提交队列。
		if row.LifecycleState != lifecycleShouldSubmit {
			row.LifecycleState = lifecycleShouldSubmit
			row.FrozenReason = "should_submit"
			if _, uErr := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
				return struct{}{}, UpdateSession(db, row)
			}); uErr != nil {
				return uErr
			}
		}
		row.LastSubmitAttemptAtUnix = time.Now().Unix()
		row.SubmitRetryCount++
		finalTxID, bErr := s.Chain.Broadcast(row.CurrentTxHex)
		if bErr != nil {
			row.LastSubmitError = strings.TrimSpace(bErr.Error())
			if isExternalSettledBroadcastError(bErr) {
				row.LifecycleState = lifecycleSettledExtern
				row.FrozenReason = "broadcast_conflict"
			}
			if _, uErr := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
				return struct{}{}, UpdateSession(db, row)
			}); uErr != nil {
				return uErr
			}
			obs.Error("bitcast-gateway", "fee_pool_passive_close_broadcast_failed", map[string]any{
				"spend_txid":      row.SpendTxID,
				"tip_height":      tip,
				"expire_height":   expireHeight,
				"broadcast_error": bErr.Error(),
			})
			continue
		}
		row.FinalTxID = finalTxID
		row.LastSubmitError = ""
		row.CloseSubmitAtUnix = time.Now().Unix()
		if fErr := s.markAcceptedClose(&row, "passive_close_accepted"); fErr != nil {
			obs.Error("bitcast-gateway", "fee_pool_passive_close_finalize_failed", map[string]any{
				"spend_txid": row.SpendTxID,
				"final_txid": finalTxID,
				"error":      fErr.Error(),
			})
			continue
		}
		obs.Business("bitcast-gateway", "fee_pool_passive_close_broadcasted", map[string]any{
			"spend_txid":    row.SpendTxID,
			"final_txid":    finalTxID,
			"tip_height":    tip,
			"expire_height": expireHeight,
			"close_state":   row.LifecycleState,
		})
	}
	return nil
}

// markAcceptedClose 以“广播被链上后端接受”为关闭真值。
// 设计说明：
// - 协议层只关心 final tx 是否已被 WOC 接受，不再观察客户地址或客户钱包 UTXO；
// - close_observe_* 是历史库字段，这里复用为“关闭完成时间/原因”，避免再引入一套重复字段；
// - close_submitted 只作为旧数据兜底，新逻辑一律直接落 closed。
func (s *GatewayService) markAcceptedClose(row *GatewaySessionRow, acceptReason string) error {
	if s == nil || s.DB == nil || row == nil {
		return fmt.Errorf("service not initialized")
	}
	if strings.TrimSpace(row.FinalTxID) == "" {
		return fmt.Errorf("final_txid required")
	}
	if row.LifecycleState == lifecycleClosed {
		return nil
	}
	nowUnix := time.Now().Unix()
	row.LifecycleState = lifecycleClosed
	row.FrozenReason = ""
	if row.CloseSubmitAtUnix == 0 {
		row.CloseSubmitAtUnix = nowUnix
	}
	row.CloseObservedAtUnix = nowUnix
	row.CloseObserveStatus = "accepted"
	acceptReason = strings.TrimSpace(acceptReason)
	if acceptReason == "" {
		acceptReason = "broadcast_accepted"
	}
	row.CloseObserveReason = acceptReason
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		if err := UpdateSession(db, *row); err != nil {
			return struct{}{}, err
		}
		if err := syncServiceStateTx(db, row.ClientID, nil, "session_closed", row.SpendTxID, ""); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	}); err != nil {
		return err
	}
	return nil
}

func sessionExpireHeight(txHex string) (uint32, bool, error) {
	spendTxHex := strings.TrimSpace(txHex)
	if spendTxHex == "" {
		return 0, false, fmt.Errorf("empty current tx hex")
	}
	parsed, err := tx.NewTransactionFromHex(spendTxHex)
	if err != nil {
		return 0, false, err
	}
	lockTime := parsed.LockTime
	if lockTime == 0 || lockTime == 0xffffffff {
		return 0, false, nil
	}
	if lockTime >= feePoolLockTimeThreshold {
		return 0, false, nil
	}
	return lockTime, true, nil
}

func validateBaseTxMatchesSessionSpend(baseTx *tx.Transaction, currentTxHex string) error {
	if baseTx == nil {
		return fmt.Errorf("base tx required")
	}
	spendTxHex := strings.TrimSpace(currentTxHex)
	if spendTxHex == "" {
		return fmt.Errorf("session current tx required")
	}
	spendTx, err := tx.NewTransactionFromHex(spendTxHex)
	if err != nil {
		return fmt.Errorf("parse session spend tx failed: %w", err)
	}
	if len(spendTx.Inputs) == 0 {
		return fmt.Errorf("session spend tx has no inputs")
	}
	in := spendTx.Inputs[0]
	if in.SourceTXID == nil {
		return fmt.Errorf("session spend tx input[0] source txid missing")
	}
	baseTxID := baseTx.TxID().String()
	if !strings.EqualFold(strings.TrimSpace(in.SourceTXID.String()), strings.TrimSpace(baseTxID)) {
		return fmt.Errorf("base tx does not match spend tx input: spend_input_txid=%s base_txid=%s", in.SourceTXID.String(), baseTxID)
	}
	if in.SourceTxOutIndex != 0 {
		return fmt.Errorf("spend tx input[0] source vout must be 0: got=%d", in.SourceTxOutIndex)
	}
	return nil
}

func (s *GatewayService) queryPhase(row GatewaySessionRow) (uint32, string, string, uint32, error) {
	if s == nil || s.Chain == nil {
		return 0, "", "", 0, fmt.Errorf("chain state unavailable")
	}
	expireHeight, hasHeight, err := sessionExpireHeight(row.CurrentTxHex)
	if err != nil {
		return 0, "", "", 0, fmt.Errorf("parse session spend tx failed: %w", err)
	}
	if !hasHeight {
		return 0, "", "", 0, fmt.Errorf("invalid session expire height")
	}
	tip, err := s.Chain.GetTipHeight()
	if err != nil {
		return 0, "", "", 0, fmt.Errorf("query tip height failed: %w", err)
	}
	phase, payability := computePhaseAndPayability(tip, expireHeight, s.Params.PayRejectBeforeExpiryBlocks)
	return tip, phase, payability, expireHeight, nil
}

func (s *GatewayService) isUniqueActiveSession(clientID string, spendTxID string) bool {
	if s == nil || s.DB == nil {
		return false
	}
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return false
	}
	var n int
	if _, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, db.QueryRow(
			`SELECT COUNT(*) FROM fee_pool_sessions WHERE client_pubkey_hex=? AND lifecycle_state='active' AND spend_txid<>?`,
			clientID,
			strings.TrimSpace(spendTxID),
		).Scan(&n)
	}); err != nil {
		return false
	}
	return n == 0
}

func payReject(status string, code string, message string) PayConfirmResp {
	return PayConfirmResp{
		Success:   false,
		Status:    normalizeLifecycleState(status),
		ErrorCode: strings.TrimSpace(code),
		Error:     strings.TrimSpace(message),
	}
}

func isSameSpendInput(txHexA string, txHexB string) bool {
	a, err := tx.NewTransactionFromHex(strings.TrimSpace(txHexA))
	if err != nil || len(a.Inputs) == 0 || a.Inputs[0].SourceTXID == nil {
		return false
	}
	b, err := tx.NewTransactionFromHex(strings.TrimSpace(txHexB))
	if err != nil || len(b.Inputs) == 0 || b.Inputs[0].SourceTXID == nil {
		return false
	}
	if !strings.EqualFold(a.Inputs[0].SourceTXID.String(), b.Inputs[0].SourceTXID.String()) {
		return false
	}
	return a.Inputs[0].SourceTxOutIndex == b.Inputs[0].SourceTxOutIndex
}

func isHeightWithinTolerance(expect uint32, got uint32, tolerance uint32) bool {
	if expect >= got {
		return expect-got <= tolerance
	}
	return got-expect <= tolerance
}

func isExternalSettledBroadcastError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(msg, "missing inputs") {
		return true
	}
	if strings.Contains(msg, "already spent") {
		return true
	}
	if strings.Contains(msg, "txn-mempool-conflict") {
		return true
	}
	if strings.Contains(msg, "conflict") {
		return true
	}
	return false
}
