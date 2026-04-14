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
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"
	contractroute "github.com/bsv8/BFTP-contract/pkg/v1/route"
	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	"github.com/bsv8/BFTP/pkg/infra/pproto"
	"github.com/bsv8/BFTP/pkg/obs"
	ce "github.com/bsv8/MultisigPool/pkg/dual_endpoint"
	kmlibs "github.com/bsv8/MultisigPool/pkg/libs"
	oldproto "github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	errListenFeePoolRotateRequired = errors.New("listen fee pool rotate required")
	errListenFeePoolStop           = errors.New("listen fee pool stopped")
)

func startListenLoops(ctx context.Context, rt *Runtime, store *clientDB) {
	if rt == nil || rt.Host == nil || store == nil {
		return
	}
	cfg := rt.ConfigSnapshot()
	if !cfgBool(cfg.Listen.Enabled, true) {
		return
	}
	// 自动触发约束：auto_renew_rounds=0 视为显式关闭监听资金池自动动作。
	if cfg.Listen.AutoRenewRounds == 0 {
		obs.Error("bitcast-client", "listen_loop_disabled_missing_initial_fund", map[string]any{"gateway": "all"})
		return
	}
	intervalSec := cfg.Listen.TickSeconds
	if intervalSec == 0 {
		intervalSec = 5
	}
	scheduler := ensureRuntimeTaskScheduler(rt, store)
	if scheduler == nil {
		return
	}

	type loopHandle struct {
		taskName string
		cycleSec uint32
	}
	var mu sync.Mutex
	running := map[string]loopHandle{}
	// waitRechargeState 记录“该网关是否已输出过等待充值日志”，用于触发式日志节流。
	waitRechargeState := map[string]bool{}

	startOne := func(gw peer.AddrInfo) {
		gwID := gw.ID.String()
		if gwID == "" {
			return
		}
		mu.Lock()
		cur, ok := running[gwID]
		cycleSec := uint32(60)
		if sess, exists := rt.getFeePool(gwID); exists && sess != nil && sess.BillingCycleSeconds > 0 {
			cycleSec = sess.BillingCycleSeconds
		}
		taskName := listenBillingTaskName(gwID)
		needReplace := ok && cur.cycleSec != cycleSec
		if ok && !needReplace {
			mu.Unlock()
			return
		}
		running[gwID] = loopHandle{taskName: taskName, cycleSec: cycleSec}
		mu.Unlock()

		spec := periodicTaskSpec{
			Name:      taskName,
			Owner:     "listen_loop",
			Mode:      "dynamic",
			Interval:  time.Duration(cycleSec) * time.Second,
			Immediate: false,
			Run: func(runCtx context.Context, trigger string) (map[string]any, error) {
				return runListenLoop(runCtx, rt, store, gw, trigger)
			},
		}
		var err error
		if needReplace {
			err = scheduler.RegisterOrReplacePeriodicTask(ctx, spec)
		} else {
			err = scheduler.RegisterPeriodicTask(ctx, spec)
		}
		if err != nil {
			obs.Error("bitcast-client", "listen_billing_task_register_failed", map[string]any{
				"gateway":   gwID,
				"task_name": taskName,
				"error":     err.Error(),
			})
			mu.Lock()
			delete(running, gwID)
			mu.Unlock()
			return
		}
	}

	stopMissing := func(active map[string]struct{}) {
		mu.Lock()
		defer mu.Unlock()
		for gwID, h := range running {
			if _, ok := active[gwID]; ok {
				continue
			}
			scheduler.CancelTask(h.taskName)
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
			if kernel := rt.kernel; kernel != nil {
				kernel.tryResumeFeePoolPausedGateway(ctx, gwID)
				if kernel.isFeePoolPaused(gwID) {
					st := kernel.feePool.getState(gwID)
					if !waitRechargeState[gwID] {
						gatewayID := gatewayBusinessID(rt, gw.ID)
						appendObservedFeePoolState(ctx, store, gatewayID, st, st, gatewayID, time.Now().Unix(), "pause_watch", "fee_pool_pause_observed", nil)
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

	// 监听协调器进入统一任务框架：启动立即对齐，后续按 listen.tick_seconds 周期对齐。
	if err := scheduler.RegisterOrReplacePeriodicTask(ctx, periodicTaskSpec{
		Name:      "listen_loop_reconcile",
		Owner:     "listen_loop",
		Mode:      "static",
		Interval:  time.Duration(intervalSec) * time.Second,
		Immediate: true,
		Run: func(_ context.Context, _ string) (map[string]any, error) {
			reconcile()
			return map[string]any{"action": "reconcile_gateways"}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "listen_reconcile_task_register_failed", map[string]any{"error": err.Error()})
	}
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

func recordGatewayRuntimeError(rt *Runtime, store *clientDB, gw peer.ID, stage string, err error) {
	if rt == nil || gw == "" || err == nil {
		return
	}
	if rt.gwManager != nil {
		rt.gwManager.SetRuntimeError(gw, stage, err)
	}
	dbAppendOrchestratorLog(context.Background(), store, orchestratorLogEntry{
		EventType:     "listen_error",
		Source:        "listen_loop",
		AggregateKey:  "gateway:" + gw.String(),
		GatewayPeerID: gw.String(),
		ErrorMessage:  err.Error(),
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

func runListenLoop(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, trigger string) (map[string]any, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	kernel := rt.kernel
	if kernel == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	openRes := kernel.dispatch(ctx, prepareClientKernelCommand(clientKernelCommand{
		CommandType:   clientKernelCommandFeePoolEnsureActive,
		GatewayPeerID: gw.ID.String(),
		RequestedBy:   "listen_loop",
		Payload:       map[string]any{"trigger": trigger},
	}))
	// 任务触发语义：
	// - ensure_active 失败/暂停交给外层 reconcile 再调度；
	// - 这里只做“一次计费 tick”，周期由统一任务框架负责。
	if !shouldRunListenBillingLoop(openRes) {
		return map[string]any{
			"gateway_pubkey_hex": gw.ID.String(),
			"result":             "skip_not_active",
			"trigger":            trigger,
		}, nil
	}
	if rt != nil && rt.orch != nil {
		rt.orch.EmitSignal(orchestratorSignal{
			Source:       "listen_loop",
			Type:         orchestratorSignalFeePoolTick,
			AggregateKey: gw.ID.String(),
			Payload: map[string]any{
				"trigger": "billing_tick",
			},
		})
		return map[string]any{
			"gateway_pubkey_hex": gw.ID.String(),
			"result":             "signal_emitted",
			"trigger":            "billing_tick",
		}, nil
	}
	tickRes := kernel.dispatch(ctx, prepareClientKernelCommand(clientKernelCommand{
		CommandType:   clientKernelCommandFeePoolMaintain,
		GatewayPeerID: gw.ID.String(),
		RequestedBy:   "listen_loop",
		Payload: map[string]any{
			"trigger": "billing_tick",
		},
	}))
	if tickRes.Status == "paused" {
		return map[string]any{
			"gateway_pubkey_hex": gw.ID.String(),
			"result":             "paused",
			"trigger":            "billing_tick",
		}, nil
	}
	if strings.TrimSpace(tickRes.Status) == "failed" {
		return nil, fmt.Errorf("listen billing tick failed: %s", strings.TrimSpace(tickRes.ErrorMessage))
	}
	return map[string]any{
		"gateway_pubkey_hex": gw.ID.String(),
		"result":             "applied",
		"trigger":            "billing_tick",
	}, nil
}

func listenBillingTaskName(gatewayPeerID string) string {
	gatewayPeerID = strings.TrimSpace(gatewayPeerID)
	if gatewayPeerID == "" {
		gatewayPeerID = "unknown"
	}
	return "listen_billing_tick:" + gatewayPeerID
}

func ensureActiveFeePool(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, autoRenewRounds uint64, info poolcore.InfoResp, commandID string) (*feePoolSession, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return ensureActiveFeePoolWithSecurity(ctx, rt, store, gw, autoRenewRounds, info, gwSec(rt.rpcTrace), commandID)
}

func ensureActiveFeePoolWithSecurity(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, autoRenewRounds uint64, info poolcore.InfoResp, sec pproto.SecurityConfig, commandID string) (*feePoolSession, error) {
	if rt == nil || rt.Host == nil || store == nil || rt.ActionChain == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	gwID := gw.ID.String()
	if existing, ok := rt.getFeePool(gwID); ok && existing != nil && existing.Status == "active" && existing.SpendTxID != "" {
		return existing, nil
	}
	return createFeePoolSessionWithSecurity(ctx, rt, store, gw, autoRenewRounds, info, sec, commandID)
}

// createFeePoolSession 在链上创建新的费用池并注册为当前 active 会话。
// 设计说明：监听轮换场景要求“先开新池再关旧池”，因此新池创建流程必须可复用。
func createFeePoolSession(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, autoRenewRounds uint64, info poolcore.InfoResp, commandID string) (*feePoolSession, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return createFeePoolSessionWithSecurity(ctx, rt, store, gw, autoRenewRounds, info, gwSec(rt.rpcTrace), commandID)
}

func createFeePoolSessionWithSecurity(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, autoRenewRounds uint64, info poolcore.InfoResp, _ pproto.SecurityConfig, commandID string) (*feePoolSession, error) {
	if rt == nil || rt.Host == nil || store == nil || rt.ActionChain == nil {
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

	identity, err := rt.runtimeIdentity()
	if err != nil {
		return nil, err
	}
	clientActor := identity.Actor
	if clientActor == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	clientLockScript := ""
	if addr, addrErr := kmlibs.GetAddressFromPubKey(clientActor.PubKey, identity.IsMainnet); addrErr == nil {
		if lock, lockErr := p2pkh.Lock(addr); lockErr == nil {
			clientLockScript = strings.TrimSpace(lock.String())
		}
	}
	// 钱包 UTXO 分配必须单步串行：从选输入到 base tx 广播成功都在同一临界区。
	allocMu := rt.walletAllocMutex()
	allocMu.Lock()
	defer allocMu.Unlock()

	// Step 6：改为从 fact 口径选源
	utxos, err := listEligiblePlainBSVWalletUTXOsFact(ctx, store, rt)
	if err != nil {
		return nil, fmt.Errorf("load wallet utxos from snapshot failed: %w", err)
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

	tip, err := getTipHeightFromDB(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("load tip height from snapshot failed: %w", err)
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
	spendTxFeeSat := poolcore.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
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
		spendTxFeeSat = poolcore.CalcFeeWithInputAmount(spendTx, baseResp.Amount)
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

	createReq := poolcore.CreateReq{
		ClientID:       identity.ClientID,
		SpendTx:        spendTxBytes,
		InputAmount:    baseResp.Amount,
		SequenceNumber: 1,
		ServerAmount:   initialServerAmount,
		ClientSig:      append([]byte(nil), (*clientOpenSig)...),
	}
	createResp, err := callNodePoolCreate(ctx, rt, gw.ID, createReq)
	if err != nil {
		return nil, fmt.Errorf("fee_pool.create failed: %w", err)
	}
	if strings.TrimSpace(createResp.SpendTxID) == "" {
		return nil, fmt.Errorf("fee_pool.create invalid response: missing spend_txid")
	}
	currentTxHex, err := mergeOpenedFeePoolCurrentTx(spendTx.Hex(), createResp.ServerSig, *clientOpenSig)
	if err != nil {
		return nil, fmt.Errorf("merge fee pool current tx failed: %w", err)
	}

	baseReq := poolcore.BaseTxReq{
		ClientID:  identity.ClientID,
		SpendTxID: createResp.SpendTxID,
		BaseTx:    baseTxBytes,
		ClientSig: append([]byte(nil), (*clientOpenSig)...),
	}
	baseOut, err := callNodePoolBaseTx(ctx, rt, gw.ID, baseReq)
	if err != nil {
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
		CurrentTxHex:  currentTxHex,

		BillingCycleSeconds:      info.BillingCycleSeconds,
		SingleCycleFeeSatoshi:    info.SingleCycleFeeSatoshi,
		SinglePublishFeeSatoshi:  info.SinglePublishFeeSatoshi,
		SingleQueryFeeSatoshi:    info.SingleQueryFeeSatoshi,
		RenewNotifyBeforeSeconds: info.RenewNotifyBeforeSeconds,
		MinimumPoolAmountSatoshi: info.MinimumPoolAmountSatoshi,
		LockBlocks:               info.LockBlocks,
		FeeRateSatPerByte:        info.FeeRateSatPerByte,
	}
	rt.setFeePool(gwID, s)

	dbAppendTxHistory(ctx, store, txHistoryEntry{
		GatewayPeerID: gwID,
		EventType:     "fee_pool_open",
		Direction:     "info",
		AmountSatoshi: int64(createResp.PoolAmountSat),
		Purpose:       "fee_pool_open",
		Note:          fmt.Sprintf("spend_txid=%s base_txid=%s pool=%d fee=%d", createResp.SpendTxID, baseOut.BaseTxID, createResp.PoolAmountSat, createResp.SpendTxFeeSat),
		PoolID:        createResp.SpendTxID,
		SequenceNum:   1,
	})
	_ = dbAppendGatewayEvent(ctx, store, gatewayEventEntry{
		GatewayPeerID: gwID,
		CommandID:     commandID,
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
			"single_query_fee_satoshi":   info.SingleQueryFeeSatoshi,
			"lock_blocks":                info.LockBlocks,
			"fee_rate_sat_per_byte":      info.FeeRateSatPerByte,
		},
	})
	// 资金流水已迁移到 fact_* 事实表组装
	dbRecordFeePoolOpenAccounting(ctx, store, feePoolOpenAccountingInput{
		BusinessID:        "biz_feepool_open_" + strings.TrimSpace(createResp.SpendTxID),
		SpendTxID:         createResp.SpendTxID,
		BaseTxID:          baseOut.BaseTxID,
		BaseTxHex:         baseResp.Tx.Hex(),
		ClientLockScript:  clientLockScript,
		PoolAmountSatoshi: createResp.PoolAmountSat,
		FromPartyID:       "client:self",
		ToPartyID:         "gateway:" + gwID,
	})
	if initialServerAmount > 0 {
		// open 锁池与首扣是两笔不同业务事件：这里把首扣单独记成 debit。
		dbAppendTxHistory(ctx, store, txHistoryEntry{
			GatewayPeerID: gwID,
			EventType:     "fee_pool_open_debit",
			Direction:     "debit",
			AmountSatoshi: -int64(initialServerAmount),
			Purpose:       "listen_cycle_fee",
			Note:          fmt.Sprintf("spend_txid=%s seq=1 server_amount=%d trigger=open_create", createResp.SpendTxID, initialServerAmount),
			PoolID:        createResp.SpendTxID,
			SequenceNum:   1,
		})
		_ = dbAppendGatewayEvent(ctx, store, gatewayEventEntry{
			GatewayPeerID: gwID,
			CommandID:     commandID,
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
		// 资金流水已迁移到 fact_* 事实表组装
	}
	if err := applyLocalBroadcastWalletTx(ctx, store, rt, baseResp.Tx.Hex(), "fee_pool_open_base"); err != nil {
		return nil, fmt.Errorf("project fee pool base tx to wallet utxo failed: %w", err)
	}

	return s, nil
}

func mergeOpenedFeePoolCurrentTx(spendTxHex string, serverSig []byte, clientSig []byte) (string, error) {
	if strings.TrimSpace(spendTxHex) == "" {
		return "", fmt.Errorf("spend tx hex required")
	}
	serverSig = append([]byte(nil), serverSig...)
	clientSig = append([]byte(nil), clientSig...)
	if len(serverSig) == 0 {
		return "", fmt.Errorf("server signature required")
	}
	if len(clientSig) == 0 {
		return "", fmt.Errorf("client signature required")
	}
	merged, err := ce.MergeDualPoolSigForSpendTx(strings.TrimSpace(spendTxHex), &serverSig, &clientSig)
	if err != nil {
		return "", err
	}
	return merged.Hex(), nil
}

func payOneListenCycle(ctx context.Context, rt *Runtime, store *clientDB, gw peer.ID, s *feePoolSession, commandID string) error {
	if rt == nil || s == nil {
		return fmt.Errorf("session missing")
	}
	latest, ok := rt.getFeePool(gw.String())
	if !ok || latest == nil || strings.TrimSpace(latest.SpendTxID) == "" {
		return fmt.Errorf("session missing")
	}
	// 统一以 runtime 当前会话为准，避免轮换后持有旧指针继续扣费。
	s = latest
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
	offerPayment := listenOfferPaymentSatoshi(rt, s)
	rawBody, err := oldproto.Marshal(&contractmessage.ListenCycleReq{
		ProposedPaymentSatoshi: offerPayment,
	})
	if err != nil {
		return err
	}
	callResp, err := TriggerPeerCall(ctx, rt, TriggerPeerCallParams{
		To:                   gw.String(),
		Route:                string(contractroute.RouteBroadcastV1ListenCycle),
		ContentType:          contractmessage.ContentTypeProto,
		Body:                 rawBody,
		Store:                store,
		RequireActiveFeePool: true,
	})
	if err != nil {
		return err
	}
	resp, err := decodeListenCycleRouteResp(callResp)
	if err != nil {
		return err
	}
	if !resp.Success {
		reason := strings.TrimSpace(resp.Error)
		if reason == "" {
			reason = strings.TrimSpace(resp.Status)
		}
		if reason == "" {
			reason = "listen cycle rejected"
		}
		return errors.New(reason)
	}
	latest, ok = rt.getFeePool(gw.String())
	if ok && latest != nil {
		s = latest
	}
	sequence := s.Sequence

	dbAppendTxHistory(ctx, store, txHistoryEntry{
		GatewayPeerID: s.GatewayPeerID,
		EventType:     "peer_call",
		Direction:     "debit",
		AmountSatoshi: -int64(resp.ChargedAmount),
		Purpose:       "listen_cycle_fee",
		Note:          fmt.Sprintf("route=%s spend_txid=%s seq=%d updated_txid=%s", contractroute.RouteBroadcastV1ListenCycle, s.SpendTxID, sequence, resp.UpdatedTxID),
		PoolID:        s.SpendTxID,
		SequenceNum:   sequence,
	})
	_ = dbAppendGatewayEvent(ctx, store, gatewayEventEntry{
		GatewayPeerID: s.GatewayPeerID,
		CommandID:     commandID,
		Action:        "listen_cycle_fee",
		PoolID:        s.SpendTxID,
		SequenceNum:   sequence,
		AmountSatoshi: int64(resp.ChargedAmount),
		Payload: map[string]any{
			"route_resp":                    resp,
			"offer_payment_satoshi":         offerPayment,
			"minimum_cycle_fee_satoshi":     s.SingleCycleFeeSatoshi,
			"minimum_billing_cycle_seconds": s.BillingCycleSeconds,
		},
	})
	dbRecordFeePoolCycleEvent(ctx, store, s.SpendTxID, sequence, resp.ChargedAmount, s.GatewayPeerID)
	return nil
}

func listenOfferPaymentSatoshi(rt *Runtime, s *feePoolSession) uint64 {
	if rt == nil || s == nil {
		return 0
	}
	budget := s.SingleCycleFeeSatoshi
	snapshot := rt.ConfigSnapshot()
	for _, node := range snapshot.Network.Gateways {
		ai, err := parseAddr(node.Addr)
		if err != nil {
			continue
		}
		if strings.EqualFold(ai.ID.String(), strings.TrimSpace(s.GatewayPeerID)) && node.ListenOfferPaymentSatoshi > 0 {
			budget = node.ListenOfferPaymentSatoshi
			break
		}
	}
	if budget == s.SingleCycleFeeSatoshi && rt.ConfigSnapshot().Listen.OfferPaymentSatoshi > 0 {
		budget = rt.ConfigSnapshot().Listen.OfferPaymentSatoshi
	}
	if s.ClientAmount > s.SpendTxFeeSat {
		maxPayable := s.ClientAmount - s.SpendTxFeeSat
		if budget > maxPayable {
			budget = maxPayable
		}
	}
	return budget
}

// rotateListenFeePool 处理监听费用池轮换：先开新池并切换，再异步重试关闭旧池。
func rotateListenFeePool(ctx context.Context, rt *Runtime, store *clientDB, gw peer.AddrInfo, old *feePoolSession, autoRenewRounds uint64, info poolcore.InfoResp, commandID string) (*feePoolSession, error) {
	if rt == nil || old == nil {
		return nil, fmt.Errorf("session missing")
	}
	obs.Business("bitcast-client", "fee_pool_rotate_begin", map[string]any{
		"gateway":         gw.ID.String(),
		"old_spend_txid":  old.SpendTxID,
		"old_client_fund": old.ClientAmount,
		"need":            old.SingleCycleFeeSatoshi + old.SpendTxFeeSat,
	})
	next, err := createFeePoolSession(ctx, rt, store, gw, autoRenewRounds, info, commandID)
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
		obs.Business("bitcast-client", "fee_pool_rotate_close_old_begin", map[string]any{
			"gateway":        gatewayPeerID,
			"old_spend_txid": oldSpendTxID,
		})
		if closeErr := closeOldFeePoolWithRetry(store, rt, oldSpendTxID, gatewayPeerID); closeErr != nil {
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

func closeOldFeePoolWithRetry(store *clientDB, rt *Runtime, oldSpendTxID string, gatewayPeerID string) error {
	const attemptTimeout = 30 * time.Second
	backoffs := []time.Duration{0, 2 * time.Second, 5 * time.Second}
	var lastErr error
	for i, wait := range backoffs {
		if wait > 0 {
			time.Sleep(wait)
		}
		attempt := i + 1
		closeCtx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
		_, err := TriggerGatewayFeePoolCloseBySpendTxID(closeCtx, store, rt, FeePoolCloseBySpendTxIDParams{
			SpendTxID:     oldSpendTxID,
			GatewayPeerID: gatewayPeerID,
		})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		obs.Error("bitcast-client", "fee_pool_rotate_close_old_retry", map[string]any{
			"gateway":        gatewayPeerID,
			"old_spend_txid": oldSpendTxID,
			"attempt":        attempt,
			"error":          err.Error(),
		})
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("close old fee pool failed with unknown error")
	}
	return lastErr
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
