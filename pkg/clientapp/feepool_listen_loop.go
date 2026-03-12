package clientapp

import (
	"context"
	"encoding/hex"
	"errors"
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

var (
	errListenFeePoolRotateRequired = errors.New("listen fee pool rotate required")
	errListenFeePoolStop           = errors.New("listen fee pool stopped")
)

func startListenLoops(ctx context.Context, rt *Runtime) {
	if rt == nil || rt.Host == nil || rt.DB == nil {
		return
	}
	if !cfgBool(rt.runIn.Listen.Enabled, true) {
		return
	}
	// 自动触发约束：auto_renew_rounds=0 视为显式关闭监听资金池自动动作。
	if rt.runIn.Listen.AutoRenewRounds == 0 {
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
	// waitRechargeState 记录“该网关是否已输出过等待充值日志”，用于触发式日志节流。
	waitRechargeState := map[string]bool{}

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
		seen := make(map[string]struct{}, len(gws))
		for _, gw := range gws {
			gwID := gw.ID.String()
			if gwID == "" {
				continue
			}
			seen[gwID] = struct{}{}
			if kernel := ensureClientKernel(rt); kernel != nil {
				kernel.tryResumeFeePoolPausedGateway(ctx, gwID)
				if kernel.isFeePoolPaused(gwID) {
					if !waitRechargeState[gwID] {
						obs.Info("bitcast-client", "fee_pool_wait_wallet_recharge", map[string]any{"gateway": gwID})
						waitRechargeState[gwID] = true
					}
					continue
				}
			}
			if waitRechargeState[gwID] {
				obs.Business("bitcast-client", "fee_pool_recharge_detected_resume", map[string]any{"gateway": gwID})
				delete(waitRechargeState, gwID)
			}
			active[gwID] = struct{}{}
			startOne(gw)
		}
		for gwID := range waitRechargeState {
			if _, ok := seen[gwID]; ok {
				continue
			}
			delete(waitRechargeState, gwID)
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

func shouldRunListenBillingLoop(openRes clientKernelResult) bool {
	return openRes.Accepted && strings.TrimSpace(openRes.Status) == "applied"
}

func runListenLoop(ctx context.Context, rt *Runtime, gw peer.AddrInfo) {
	kernel := ensureClientKernel(rt)
	if rt == nil || kernel == nil {
		return
	}
	openRes := kernel.dispatch(ctx, clientKernelCommand{
		CommandType:   clientKernelCommandFeePoolEnsureActive,
		GatewayPeerID: gw.ID.String(),
		RequestedBy:   "listen_loop",
		Payload:       map[string]any{"trigger": "listen_loop_start"},
	})
	// 只有首次建池成功才进入周期 loop；失败/暂停都交给外层 reconcile 再拉起。
	if !shouldRunListenBillingLoop(openRes) {
		return
	}
	cycleSec := uint32(60)
	if sess, ok := rt.getFeePool(gw.ID.String()); ok && sess != nil && sess.BillingCycleSeconds > 0 {
		cycleSec = sess.BillingCycleSeconds
	}
	ticker := time.NewTicker(time.Duration(cycleSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if orch := getClientOrchestrator(rt); orch != nil {
				orch.EmitSignal(orchestratorSignal{
					Source:       "listen_loop",
					Type:         orchestratorSignalFeePoolTick,
					AggregateKey: gw.ID.String(),
					Payload: map[string]any{
						"trigger": "billing_tick",
					},
				})
				continue
			}
			tickRes := kernel.dispatch(ctx, clientKernelCommand{
				CommandType:   clientKernelCommandFeePoolMaintain,
				GatewayPeerID: gw.ID.String(),
				RequestedBy:   "listen_loop",
				Payload: map[string]any{
					"trigger": "billing_tick",
				},
			})
			if tickRes.Status == "paused" {
				return
			}
		}
	}
}

func ensureActiveFeePool(ctx context.Context, rt *Runtime, gw peer.AddrInfo, autoRenewRounds uint64, info dual2of2.InfoResp) (*feePoolSession, error) {
	if rt == nil || rt.Host == nil || rt.DB == nil || rt.Chain == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	gwID := gw.ID.String()
	if existing, ok := rt.getFeePool(gwID); ok && existing != nil && existing.Status == "active" && existing.SpendTxID != "" {
		return existing, nil
	}
	return createFeePoolSession(ctx, rt, gw, autoRenewRounds, info)
}

// createFeePoolSession 在链上创建新的费用池并注册为当前 active 会话。
// 设计说明：监听轮换场景要求“先开新池再关旧池”，因此新池创建流程必须可复用。
func createFeePoolSession(ctx context.Context, rt *Runtime, gw peer.AddrInfo, autoRenewRounds uint64, info dual2of2.InfoResp) (*feePoolSession, error) {
	if rt == nil || rt.Host == nil || rt.DB == nil || rt.Chain == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	gwID := gw.ID.String()
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

	if autoRenewRounds == 0 {
		return nil, fmt.Errorf("auto renew rounds is zero")
	}
	initialServerAmount := info.SingleCycleFeeSatoshi
	poolAmount, err := listenPoolAmountByRounds(autoRenewRounds, info.SingleCycleFeeSatoshi, 0)
	if err != nil {
		return nil, err
	}
	if info.MinimumPoolAmountSatoshi > 0 && poolAmount < info.MinimumPoolAmountSatoshi {
		poolAmount = info.MinimumPoolAmountSatoshi
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
		initialServerAmount,
		endHeight,
		clientActor.PrivKey,
		serverPub,
		false,
		info.FeeRateSatPerByte,
	)
	if err != nil {
		return nil, fmt.Errorf("build spend tx failed: %w", err)
	}
	if clientAmount > poolAmount {
		return nil, fmt.Errorf("invalid spend tx client amount: pool=%d client=%d", poolAmount, clientAmount)
	}
	spendTxFeeSat := dual2of2.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
	requiredPoolAmount, err := listenPoolAmountByRounds(autoRenewRounds, info.SingleCycleFeeSatoshi, spendTxFeeSat)
	if err != nil {
		return nil, err
	}
	if info.MinimumPoolAmountSatoshi > 0 && requiredPoolAmount < info.MinimumPoolAmountSatoshi {
		requiredPoolAmount = info.MinimumPoolAmountSatoshi
	}
	if requiredPoolAmount > poolAmount {
		poolAmount = requiredPoolAmount
		baseResp, err = ce.BuildDualFeePoolBaseTx(&kmutxos, poolAmount, clientActor.PrivKey, serverPub, false, info.FeeRateSatPerByte)
		if err != nil {
			return nil, fmt.Errorf("rebuild base tx failed: %w", err)
		}
		spendTx, clientOpenSig, clientAmount, err = ce.BuildDualFeePoolSpendTX(
			baseResp.Tx,
			poolAmount,
			initialServerAmount,
			endHeight,
			clientActor.PrivKey,
			serverPub,
			false,
			info.FeeRateSatPerByte,
		)
		if err != nil {
			return nil, fmt.Errorf("rebuild spend tx failed: %w", err)
		}
		if clientAmount > poolAmount {
			return nil, fmt.Errorf("invalid rebuilt spend tx client amount: pool=%d client=%d", poolAmount, clientAmount)
		}
		spendTxFeeSat = dual2of2.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
	}
	// 创建即首扣：rounds 的第 1 轮在 open 时已经划拨给网关，后续只需校验剩余轮次资金。
	remainingRounds := autoRenewRounds
	if initialServerAmount >= info.SingleCycleFeeSatoshi && remainingRounds > 0 {
		remainingRounds--
	}
	if remainingRounds > 0 && clientAmount < info.SingleCycleFeeSatoshi+spendTxFeeSat {
		return nil, fmt.Errorf(
			"pool amount underfunded by rounds: rounds=%d client_amount=%d need=%d",
			autoRenewRounds,
			clientAmount,
			info.SingleCycleFeeSatoshi+spendTxFeeSat,
		)
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
		ServerAmount:   initialServerAmount,
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
		ServerAmount:  initialServerAmount,
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
			"initial_server_amount":      initialServerAmount,
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
			"server_amount":        initialServerAmount,
		},
	})
	if initialServerAmount > 0 {
		// open 锁池与首扣是两笔不同业务事件：这里把首扣单独记成 debit。
		appendTxHistory(rt.DB, txHistoryEntry{
			GatewayPeerID: gwID,
			EventType:     "fee_pool_open_debit",
			Direction:     "debit",
			AmountSatoshi: -int64(initialServerAmount),
			Purpose:       "listen_cycle_fee",
			Note:          fmt.Sprintf("spend_txid=%s seq=1 server_amount=%d trigger=open_create", createResp.SpendTxID, initialServerAmount),
			PoolID:        createResp.SpendTxID,
			SequenceNum:   1,
		})
		appendGatewayEvent(rt.DB, gatewayEventEntry{
			GatewayPeerID: gwID,
			Action:        "listen_cycle_fee_open",
			PoolID:        createResp.SpendTxID,
			SequenceNum:   1,
			AmountSatoshi: int64(initialServerAmount),
			Payload: map[string]any{
				"spend_txid":        createResp.SpendTxID,
				"sequence":          1,
				"server_amount":     initialServerAmount,
				"charge_reason":     "listen_cycle_fee",
				"charge_amount_sat": initialServerAmount,
				"trigger":           "open_create",
			},
		})
		appendWalletFundFlow(rt.DB, walletFundFlowEntry{
			FlowID:          "fee_pool:" + createResp.SpendTxID,
			FlowType:        "fee_pool",
			RefID:           createResp.SpendTxID,
			Stage:           "use_open",
			Direction:       "out",
			Purpose:         "listen_cycle_fee",
			AmountSatoshi:   -int64(initialServerAmount),
			UsedSatoshi:     int64(initialServerAmount),
			ReturnedSatoshi: 0,
			RelatedTxID:     createResp.SpendTxID,
			Note:            "sequence=1 trigger=open_create",
			Payload: map[string]any{
				"sequence":          1,
				"charge_reason":     "listen_cycle_fee",
				"charge_amount_sat": initialServerAmount,
			},
		})
	}

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
		return errListenFeePoolRotateRequired
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

// rotateListenFeePool 处理监听费用池轮换：先开新池并切换，再异步单次尝试关闭旧池。
// 注意：旧池 close 失败不自动重试，交给管理平台手动处理。
func rotateListenFeePool(ctx context.Context, rt *Runtime, gw peer.AddrInfo, old *feePoolSession, autoRenewRounds uint64, info dual2of2.InfoResp) (*feePoolSession, error) {
	if rt == nil || old == nil {
		return nil, fmt.Errorf("session missing")
	}
	obs.Business("bitcast-client", "fee_pool_rotate_begin", map[string]any{
		"gateway":         gw.ID.String(),
		"old_spend_txid":  old.SpendTxID,
		"old_client_fund": old.ClientAmount,
		"need":            old.SingleCycleFeeSatoshi + old.SpendTxFeeSat,
	})
	next, err := createFeePoolSession(ctx, rt, gw, autoRenewRounds, info)
	if err != nil {
		if isWalletInsufficientForListen(err) {
			return nil, fmt.Errorf("%w: wallet insufficient for new fee pool: %v", errListenFeePoolStop, err)
		}
		return nil, err
	}
	rt.setFeePool(gw.ID.String(), next)
	obs.Business("bitcast-client", "fee_pool_rotate_switch_active", map[string]any{
		"gateway":        gw.ID.String(),
		"old_spend_txid": old.SpendTxID,
		"new_spend_txid": next.SpendTxID,
	})

	old.Status = "retired"
	go func(oldSpendTxID string, gatewayPeerID string) {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		obs.Business("bitcast-client", "fee_pool_rotate_close_old_begin", map[string]any{
			"gateway":        gatewayPeerID,
			"old_spend_txid": oldSpendTxID,
		})
		_, closeErr := TriggerGatewayFeePoolCloseBySpendTxID(closeCtx, rt, FeePoolCloseBySpendTxIDParams{
			SpendTxID:     oldSpendTxID,
			GatewayPeerID: gatewayPeerID,
		})
		if closeErr != nil {
			obs.Error("bitcast-client", "fee_pool_rotate_close_old_failed", map[string]any{
				"gateway":        gatewayPeerID,
				"old_spend_txid": oldSpendTxID,
				"error":          closeErr.Error(),
			})
			return
		}
		obs.Business("bitcast-client", "fee_pool_rotate_close_old_ok", map[string]any{
			"gateway":        gatewayPeerID,
			"old_spend_txid": oldSpendTxID,
		})
	}(old.SpendTxID, gw.ID.String())

	return next, nil
}

func isWalletInsufficientForListen(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(s, "no utxos") {
		return true
	}
	if strings.Contains(s, "insufficient") {
		return true
	}
	if strings.Contains(s, "not enough") {
		return true
	}
	return false
}

func listenPoolAmountByRounds(rounds uint64, singleCycleFee uint64, spendTxFee uint64) (uint64, error) {
	if rounds == 0 {
		return 0, fmt.Errorf("auto renew rounds is zero")
	}
	unit := singleCycleFee + spendTxFee
	if unit < singleCycleFee {
		return 0, fmt.Errorf("listen fee overflow: single_cycle_fee=%d spend_tx_fee=%d", singleCycleFee, spendTxFee)
	}
	if rounds > ^uint64(0)/unit {
		return 0, fmt.Errorf("listen pool amount overflow: rounds=%d unit=%d", rounds, unit)
	}
	poolAmount := rounds * unit
	if poolAmount == 0 {
		return 0, fmt.Errorf("pool amount is zero")
	}
	return poolAmount, nil
}
