package clientapp

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/p2prpc"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
	"github.com/libp2p/go-libp2p/core/peer"
)

func startListenLoops(ctx context.Context, rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.DB == nil {
		return
	}
	if !cfgBool(rt.runIn.Listen.Enabled, true) {
		return
	}
	// 自动触发约束：max_auto_renew_amount=0 视为显式关闭监听资金池自动动作。
	if rt.runIn.Listen.MaxAutoRenewAmount == 0 {
		obs.Error("bitcast-client", "listen_loop_disabled_missing_initial_fund", map[string]any{"gateway": "all"})
		return
	}
	intervalSec := rt.runIn.Listen.TickSeconds
	if intervalSec == 0 {
		intervalSec = 5
	}
	type loopHandle struct {
		cancel context.CancelFunc
		runID  uint64
	}
	var mu sync.Mutex
	running := map[string]loopHandle{}
	var nextRunID uint64

	startOne := func(gw peer.AddrInfo) {
		gwID := gw.ID.String()
		if gwID == "" {
			return
		}
		mu.Lock()
		if _, ok := running[gwID]; ok {
			mu.Unlock()
			return
		}
		loopCtx, cancel := context.WithCancel(ctx)
		nextRunID++
		runID := nextRunID
		running[gwID] = loopHandle{cancel: cancel, runID: runID}
		mu.Unlock()

		go func() {
			defer func() {
				mu.Lock()
				// 仅移除当前实例，避免覆盖新一轮重连创建的 loop。
				if cur, ok := running[gwID]; ok && cur.runID == runID {
					delete(running, gwID)
				}
				mu.Unlock()
			}()
			runListenLoop(loopCtx, rt, gw)
		}()
	}

	stopMissing := func(active map[string]struct{}) {
		mu.Lock()
		defer mu.Unlock()
		for gwID, h := range running {
			if _, ok := active[gwID]; ok {
				continue
			}
			h.cancel()
			delete(running, gwID)
		}
	}

	reconcile := func() {
		if rt.gwManager != nil {
			// 每个 tick 都刷新一次连接，确保新增/短暂掉线后可自动恢复。
			rt.gwManager.RefreshConnections(ctx)
		}
		gws := snapshotHealthyGateways(rt)
		if len(gws) == 0 {
			obs.Info("bitcast-client", "listen_loop_wait_gateway_connection", map[string]any{"reason": "no_connected_gateway"})
		}
		active := make(map[string]struct{}, len(gws))
		for _, gw := range gws {
			gwID := gw.ID.String()
			if gwID == "" {
				continue
			}
			active[gwID] = struct{}{}
			startOne(gw)
		}
		stopMissing(active)
	}

	// 启动立即对齐一次，之后按 tick 秒持续自动对齐。
	reconcile()
	go func() {
		ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				mu.Lock()
				for gwID, h := range running {
					h.cancel()
					delete(running, gwID)
				}
				mu.Unlock()
				return
			case <-ticker.C:
				reconcile()
			}
		}
	}()
}

func snapshotHealthyGateways(rt *Runtime) []peer.AddrInfo {
	if rt == nil {
		return nil
	}
	if rt.gwManager != nil {
		gws := rt.gwManager.GetConnectedGateways()
		if len(gws) > 0 {
			rt.HealthyGWs = gws
			return append([]peer.AddrInfo(nil), gws...)
		}
	}
	if len(rt.HealthyGWs) == 0 {
		return nil
	}
	return append([]peer.AddrInfo(nil), rt.HealthyGWs...)
}

func recordGatewayRuntimeError(rt *Runtime, gw peer.ID, stage string, err error) {
	if rt == nil || gw == "" || err == nil {
		return
	}
	if rt.gwManager != nil {
		rt.gwManager.SetRuntimeError(gw, stage, err)
	}
	appendGatewayEvent(rt.DB, gatewayEventEntry{
		GatewayPeerID: gw.String(),
		Action:        "listen_error",
		AmountSatoshi: 0,
		Payload: map[string]any{
			"stage": stage,
			"error": err.Error(),
		},
	})
}

func clearGatewayRuntimeError(rt *Runtime, gw peer.ID) {
	if rt == nil || gw == "" {
		return
	}
	if rt.gwManager != nil {
		rt.gwManager.ClearRuntimeError(gw)
	}
}

func runListenLoop(ctx context.Context, rt *Runtime, gw peer.AddrInfo) {
	lc := rt.runIn.Listen

	initialFund := lc.MaxAutoRenewAmount
	if initialFund == 0 {
		obs.Error("bitcast-client", "listen_loop_disabled_missing_initial_fund", map[string]any{"gateway": gw.ID.String()})
		return
	}

	// 1) 获取网关握手参数
	var info dual2of2.InfoResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolInfo, gwSec(rt.rpcTrace), dual2of2.InfoReq{ClientID: rt.runIn.ClientID}, &info); err != nil {
		recordGatewayRuntimeError(rt, gw.ID, "fee_pool_info", err)
		obs.Error("bitcast-client", "fee_pool_info_failed", map[string]any{"gateway": gw.ID.String(), "error": err.Error()})
		return
	}
	obs.Business("bitcast-client", "fee_pool_info_ack", map[string]any{
		"gateway":                     gw.ID.String(),
		"billing_cycle_seconds":       info.BillingCycleSeconds,
		"single_cycle_fee_satoshi":    info.SingleCycleFeeSatoshi,
		"single_publish_fee_satoshi":  info.SinglePublishFeeSatoshi,
		"renew_notify_before_seconds": info.RenewNotifyBeforeSeconds,
		"minimum_pool_amount_satoshi": info.MinimumPoolAmountSatoshi,
		"lock_blocks":                 info.LockBlocks,
		"fee_rate_sat_per_byte":       info.FeeRateSatPerByte,
	})

	// 2) 确保本地已有 active 费用池会话（没有则创建）
	sess, err := ensureActiveFeePool(ctx, rt, gw, initialFund, info)
	if err != nil {
		recordGatewayRuntimeError(rt, gw.ID, "fee_pool_open", err)
		obs.Error("bitcast-client", "fee_pool_open_failed", map[string]any{"gateway": gw.ID.String(), "error": err.Error()})
		return
	}
	clearGatewayRuntimeError(rt, gw.ID)

	// 3) 周期扣费：client 侧按 billing_cycle_seconds 定时发起 PayConfirm
	cycleSec := info.BillingCycleSeconds
	if cycleSec == 0 {
		cycleSec = 60
	}
	ticker := time.NewTicker(time.Duration(cycleSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := payOneListenCycle(ctx, rt, gw.ID, sess); err != nil {
				recordGatewayRuntimeError(rt, gw.ID, "fee_pool_pay", err)
				obs.Error("bitcast-client", "fee_pool_listen_pay_failed", map[string]any{"gateway": gw.ID.String(), "error": err.Error()})
			} else {
				clearGatewayRuntimeError(rt, gw.ID)
			}
		}
	}
}

func ensureActiveFeePool(ctx context.Context, rt *Runtime, gw peer.AddrInfo, targetPoolAmount uint64, info dual2of2.InfoResp) (*feePoolSession, error) {
	if rt == nil || rt.Host == nil || rt.DB == nil || rt.Chain == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	gwID := gw.ID.String()
	if existing, ok := rt.getFeePool(gwID); ok && existing != nil && existing.Status == "active" && existing.SpendTxID != "" {
		return existing, nil
	}

	// server BSV 公钥：从 peerstore 提取 raw（必须是 33字节压缩公钥）。
	pub := rt.Host.Peerstore().PubKey(gw.ID)
	if pub == nil {
		return nil, fmt.Errorf("missing gateway public key in peerstore")
	}
	raw, err := pub.Raw()
	if err != nil {
		return nil, fmt.Errorf("read gateway raw pubkey: %w", err)
	}
	serverPubHex := strings.ToLower(hex.EncodeToString(raw))
	serverPub, err := ec.PublicKeyFromString(serverPubHex)
	if err != nil {
		return nil, fmt.Errorf("invalid gateway secp256k1 pubkey: %w", err)
	}

	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return nil, err
	}
	// 钱包 UTXO 分配必须单步串行：从选输入到 base tx 广播成功都在同一临界区。
	allocMu := rt.walletAllocMutex()
	allocMu.Lock()
	defer allocMu.Unlock()

	utxos, err := rt.Chain.GetUTXOs(clientActor.Addr)
	if err != nil {
		return nil, fmt.Errorf("query utxos failed: %w", err)
	}
	if len(utxos) == 0 {
		return nil, fmt.Errorf("no utxos for client address: %s", clientActor.Addr)
	}
	kmutxos := make([]kmlibs.UTXO, 0, len(utxos))
	for _, u := range utxos {
		kmutxos = append(kmutxos, kmlibs.UTXO{TxID: u.TxID, Vout: u.Vout, Value: u.Value})
	}

	poolAmount := targetPoolAmount
	if poolAmount == 0 {
		// target=0 表示“非监听场景，仅用于业务发布扣费”。
		// 这里用小额默认值，避免每次 demand 发布都占用过大本金。
		poolAmount = 200
		if info.SinglePublishFeeSatoshi > 0 && info.SinglePublishFeeSatoshi*20 > poolAmount {
			poolAmount = info.SinglePublishFeeSatoshi * 20
		}
	}
	if info.MinimumPoolAmountSatoshi > 0 && poolAmount < info.MinimumPoolAmountSatoshi {
		poolAmount = info.MinimumPoolAmountSatoshi
	}
	if poolAmount == 0 {
		return nil, fmt.Errorf("pool amount is zero")
	}

	tip, err := rt.Chain.GetTipHeight()
	if err != nil {
		return nil, fmt.Errorf("query tip height failed: %w", err)
	}
	endHeight := tip + info.LockBlocks

	baseResp, err := ce.BuildDualFeePoolBaseTx(&kmutxos, poolAmount, clientActor.PrivKey, serverPub, false, info.FeeRateSatPerByte)
	if err != nil {
		return nil, fmt.Errorf("build base tx failed: %w", err)
	}
	spendTx, clientOpenSig, clientAmount, err := ce.BuildDualFeePoolSpendTX(
		baseResp.Tx,
		poolAmount,
		0,
		endHeight,
		clientActor.PrivKey,
		serverPub,
		false,
		info.FeeRateSatPerByte,
	)
	if err != nil {
		return nil, fmt.Errorf("build spend tx failed: %w", err)
	}
	spendTxBytes, err := hex.DecodeString(spendTx.Hex())
	if err != nil {
		return nil, fmt.Errorf("encode spend tx bytes failed: %w", err)
	}
	baseTxBytes, err := hex.DecodeString(baseResp.Tx.Hex())
	if err != nil {
		return nil, fmt.Errorf("encode base tx bytes failed: %w", err)
	}

	createReq := dual2of2.CreateReq{
		ClientID:       rt.runIn.ClientID,
		SpendTx:        spendTxBytes,
		InputAmount:    baseResp.Amount,
		SequenceNumber: 1,
		ServerAmount:   0,
		ClientSig:      append([]byte(nil), (*clientOpenSig)...),
	}
	var createResp dual2of2.CreateResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolCreate, gwSec(rt.rpcTrace), createReq, &createResp); err != nil {
		return nil, fmt.Errorf("fee_pool.create failed: %w", err)
	}
	if strings.TrimSpace(createResp.SpendTxID) == "" {
		return nil, fmt.Errorf("fee_pool.create invalid response: missing spend_txid")
	}

	baseReq := dual2of2.BaseTxReq{
		ClientID:  rt.runIn.ClientID,
		SpendTxID: createResp.SpendTxID,
		BaseTx:    baseTxBytes,
		ClientSig: append([]byte(nil), (*clientOpenSig)...),
	}
	var baseOut dual2of2.BaseTxResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw.ID, dual2of2.ProtoFeePoolBaseTx, gwSec(rt.rpcTrace), baseReq, &baseOut); err != nil {
		return nil, fmt.Errorf("fee_pool.base_tx failed: %w", err)
	}
	if !baseOut.Success || baseOut.Status != "active" {
		return nil, fmt.Errorf("fee_pool.base_tx rejected: %s", strings.TrimSpace(baseOut.Error))
	}

	s := &feePoolSession{
		GatewayPeerID: gwID,
		SpendTxID:     createResp.SpendTxID,
		BaseTxID:      baseOut.BaseTxID,
		Status:        "active",
		PoolAmountSat: createResp.PoolAmountSat,
		SpendTxFeeSat: createResp.SpendTxFeeSat,
		Sequence:      1,
		ServerAmount:  0,
		ClientAmount:  clientAmount,
		CurrentTxHex:  spendTx.Hex(),

		BillingCycleSeconds:      info.BillingCycleSeconds,
		SingleCycleFeeSatoshi:    info.SingleCycleFeeSatoshi,
		SinglePublishFeeSatoshi:  info.SinglePublishFeeSatoshi,
		RenewNotifyBeforeSeconds: info.RenewNotifyBeforeSeconds,
		MinimumPoolAmountSatoshi: info.MinimumPoolAmountSatoshi,
		LockBlocks:               info.LockBlocks,
		FeeRateSatPerByte:        info.FeeRateSatPerByte,
	}
	rt.setFeePool(gwID, s)

	appendTxHistory(rt.DB, txHistoryEntry{
		GatewayPeerID: gwID,
		EventType:     "fee_pool_open",
		Direction:     "info",
		AmountSatoshi: int64(createResp.PoolAmountSat),
		Purpose:       "fee_pool_open",
		Note:          fmt.Sprintf("spend_txid=%s base_txid=%s pool=%d fee=%d", createResp.SpendTxID, baseOut.BaseTxID, createResp.PoolAmountSat, createResp.SpendTxFeeSat),
		PoolID:        createResp.SpendTxID,
		SequenceNum:   1,
	})
	appendGatewayEvent(rt.DB, gatewayEventEntry{
		GatewayPeerID: gwID,
		Action:        "fee_pool_open",
		PoolID:        createResp.SpendTxID,
		SequenceNum:   1,
		AmountSatoshi: int64(createResp.PoolAmountSat),
		Payload: map[string]any{
			"spend_txid":                 createResp.SpendTxID,
			"base_txid":                  baseOut.BaseTxID,
			"pool_amount_satoshi":        createResp.PoolAmountSat,
			"spend_tx_fee_satoshi":       createResp.SpendTxFeeSat,
			"billing_cycle_seconds":      info.BillingCycleSeconds,
			"single_cycle_fee_satoshi":   info.SingleCycleFeeSatoshi,
			"single_publish_fee_satoshi": info.SinglePublishFeeSatoshi,
			"lock_blocks":                info.LockBlocks,
			"fee_rate_sat_per_byte":      info.FeeRateSatPerByte,
		},
	})
	appendWalletFundFlow(rt.DB, walletFundFlowEntry{
		FlowID:          "fee_pool:" + createResp.SpendTxID,
		FlowType:        "fee_pool",
		RefID:           createResp.SpendTxID,
		Stage:           "open_lock",
		Direction:       "lock",
		Purpose:         "fee_pool_open",
		AmountSatoshi:   int64(createResp.PoolAmountSat),
		UsedSatoshi:     0,
		ReturnedSatoshi: 0,
		RelatedTxID:     baseOut.BaseTxID,
		Note:            fmt.Sprintf("gateway=%s", gwID),
		Payload: map[string]any{
			"base_txid":            baseOut.BaseTxID,
			"pool_amount_satoshi":  createResp.PoolAmountSat,
			"spend_tx_fee_satoshi": createResp.SpendTxFeeSat,
		},
	})

	return s, nil
}

func payOneListenCycle(ctx context.Context, rt *Runtime, gw peer.ID, s *feePoolSession) error {
	if rt == nil || s == nil {
		return fmt.Errorf("session missing")
	}
	if s.Status != "active" {
		return fmt.Errorf("session status %s", s.Status)
	}
	if s.SingleCycleFeeSatoshi == 0 {
		return nil
	}
	if s.ClientAmount < s.SingleCycleFeeSatoshi+s.SpendTxFeeSat {
		obs.Error("bitcast-client", "fee_pool_insufficient", map[string]any{"gateway": gw.String(), "client_amount": s.ClientAmount, "need": s.SingleCycleFeeSatoshi + s.SpendTxFeeSat})
		return fmt.Errorf("insufficient fee pool amount")
	}

	pub := rt.Host.Peerstore().PubKey(gw)
	if pub == nil {
		return fmt.Errorf("missing gateway public key in peerstore")
	}
	raw, err := pub.Raw()
	if err != nil {
		return fmt.Errorf("read gateway raw pubkey: %w", err)
	}
	serverPub, err := ec.PublicKeyFromString(strings.ToLower(hex.EncodeToString(raw)))
	if err != nil {
		return fmt.Errorf("invalid gateway secp256k1 pubkey: %w", err)
	}

	clientActor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return err
	}

	nextSeq := s.Sequence + 1
	nextServerAmount := s.ServerAmount + s.SingleCycleFeeSatoshi
	updatedTx, err := ce.LoadTx(
		s.CurrentTxHex,
		nil,
		nextSeq,
		nextServerAmount,
		serverPub,
		clientActor.PubKey,
		s.PoolAmountSat,
	)
	if err != nil {
		return fmt.Errorf("load updated tx failed: %w", err)
	}
	clientSig, err := ce.ClientDualFeePoolSpendTXUpdateSign(updatedTx, clientActor.PrivKey, serverPub)
	if err != nil {
		return fmt.Errorf("client sign update failed: %w", err)
	}

	req := dual2of2.PayConfirmReq{
		ClientID:            rt.runIn.ClientID,
		SpendTxID:           s.SpendTxID,
		SequenceNumber:      nextSeq,
		ServerAmount:        nextServerAmount,
		Fee:                 s.SpendTxFeeSat,
		ClientSig:           append([]byte(nil), (*clientSig)...),
		ChargeReason:        "listen_cycle_fee",
		ChargeAmountSatoshi: s.SingleCycleFeeSatoshi,
	}
	var out dual2of2.PayConfirmResp
	if err := p2prpc.CallProto(ctx, rt.Host, gw, dual2of2.ProtoFeePoolPayConfirm, gwSec(rt.rpcTrace), req, &out); err != nil {
		return err
	}
	if !out.Success {
		return fmt.Errorf("pay_confirm rejected: %s", strings.TrimSpace(out.Error))
	}

	// client 侧保存“本次更新的 tx hex”（不需要 server merge 结果）。
	s.CurrentTxHex = updatedTx.Hex()
	s.Sequence = out.Sequence
	s.ServerAmount = out.ServerAmount
	s.ClientAmount = out.ClientAmount

	appendTxHistory(rt.DB, txHistoryEntry{
		GatewayPeerID: s.GatewayPeerID,
		EventType:     "pay_confirm",
		Direction:     "debit",
		AmountSatoshi: -int64(s.SingleCycleFeeSatoshi),
		Purpose:       "listen_cycle_fee",
		Note:          fmt.Sprintf("spend_txid=%s seq=%d server_amount=%d updated_txid=%s", s.SpendTxID, out.Sequence, out.ServerAmount, out.UpdatedTxID),
		PoolID:        s.SpendTxID,
		SequenceNum:   out.Sequence,
	})
	appendGatewayEvent(rt.DB, gatewayEventEntry{
		GatewayPeerID: s.GatewayPeerID,
		Action:        "listen_cycle_fee",
		PoolID:        s.SpendTxID,
		SequenceNum:   out.Sequence,
		AmountSatoshi: int64(s.SingleCycleFeeSatoshi),
		Payload:       out,
	})
	appendWalletFundFlow(rt.DB, walletFundFlowEntry{
		FlowID:          "fee_pool:" + s.SpendTxID,
		FlowType:        "fee_pool",
		RefID:           s.SpendTxID,
		Stage:           "use_cycle",
		Direction:       "out",
		Purpose:         "listen_cycle_fee",
		AmountSatoshi:   -int64(s.SingleCycleFeeSatoshi),
		UsedSatoshi:     int64(s.SingleCycleFeeSatoshi),
		ReturnedSatoshi: 0,
		RelatedTxID:     out.UpdatedTxID,
		Note:            fmt.Sprintf("sequence=%d", out.Sequence),
		Payload:         out,
	})
	return nil
}
