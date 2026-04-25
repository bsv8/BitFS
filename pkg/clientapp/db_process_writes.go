package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procchaintipworkerlogs"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procchainutxoworkerlogs"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient"
	"github.com/bsv8/BitFS/pkg/clientapp/obs"
)

// 设计说明：
// - 这里收口运行期常见的日志、流水、账务写入；
// - 外层业务代码只表达"记什么"，不直接写 SQL；
// - 同一类写入统一走 clientDB，后续要补事务或限流时，只改这里。

type purchaseDoneEntry struct {
	DemandID       string
	SellerPubHex   string
	ArbiterPubHex  string
	ChunkIndex     uint32
	ObjectHash     string
	AmountSatoshi  uint64
	Status         string
	ErrorMessage   string
	CreatedAtUnix  int64
	FinishedAtUnix int64
}

// commandJournalEntry proc_command_journal schema 已删除，保留结构体定义仅供编译通过
type commandJournalEntry struct {
	CommandID     string
	CommandType   string
	GatewayPeerID string
	AggregateID   string
	RequestedBy   string
	RequestedAt   int64
	Accepted      bool
	Status        string
	ErrorCode     string
	ErrorMessage  string
	StateBefore   string
	StateAfter    string
	DurationMS    int64
	TriggerKey    string
	Payload       any
	Result        any
}

// domainEventEntry proc_domain_events schema 完整字段定义
type domainEventEntry struct {
	CommandID     string
	EventName     string
	Source        string
	OccurredAt    int64
	Payload       any
	GatewayPeerID string
	StateBefore   string
	StateAfter    string
}

// stateSnapshotEntry proc_state_snapshots schema 完整字段定义
type stateSnapshotEntry struct {
	SnapshotID    string
	SnapshotType  string
	State         string
	OccurredAt    int64
	Payload       any
	CommandID     string
	GatewayPeerID string
	PauseReason   string
	PauseNeedSat  int64
	PauseHaveSat  int64
	LastError     string
}

// effectLogEntry proc_effect_logs schema 完整字段定义
type effectLogEntry struct {
	EffectID      string
	AggregateID   string
	EffectType    string
	OccurredAt    int64
	Payload       any
	CommandID     string
	GatewayPeerID string
	Stage         string
	Status        string
	ErrorMessage  string
}

// directTransferPoolAllocationFactInput biz_pool_allocations schema 已删除，保留结构体定义仅供编译通过
type directTransferPoolAllocationFactInput struct {
	PoolAllocationID int64
	SessionID        string
	BusinessID       string
	Kind             string
	State            string
	OccurredAt       int64
}

// dbAppendTxHistory 已下线（Group 8 cleanup）
// 旧 settlement layer 已删除，tx_history 兼容事实不再可用
func dbAppendTxHistory(ctx context.Context, store *clientDB, e txHistoryEntry) {
	if store == nil {
		return
	}
	obs.Info(ServiceName, "tx_history_append_deprecated", map[string]any{
		"gateway_peer_id": e.GatewayPeerID,
		"event_type":      e.EventType,
		"note":            "Group 8 cleanup: settlement layer removed",
	})
}

func dbAppendPurchaseDone(ctx context.Context, store *clientDB, e purchaseDoneEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		if strings.TrimSpace(e.DemandID) == "" {
			return fmt.Errorf("demand_id is required")
		}
		if strings.TrimSpace(e.SellerPubHex) == "" {
			return fmt.Errorf("seller_pub_hex is required")
		}
		if strings.TrimSpace(e.ArbiterPubHex) == "" {
			return fmt.Errorf("arbiter_pub_hex is required")
		}
		if e.AmountSatoshi == 0 {
			return fmt.Errorf("amount_satoshi must be positive")
		}
		status := strings.TrimSpace(e.Status)
		if status == "" {
			status = "done"
		}
		now := time.Now().Unix()
		createdAtUnix := e.CreatedAtUnix
		if createdAtUnix <= 0 {
			createdAtUnix = now
		}
		finishedAtUnix := e.FinishedAtUnix
		if finishedAtUnix <= 0 {
			finishedAtUnix = now
		}
		_, err := tx.BizPurchases.Create().
			SetDemandID(strings.TrimSpace(e.DemandID)).
			SetSellerPubHex(strings.ToLower(strings.TrimSpace(e.SellerPubHex))).
			SetArbiterPubHex(strings.ToLower(strings.TrimSpace(e.ArbiterPubHex))).
			SetChunkIndex(int64(e.ChunkIndex)).
			SetObjectHash(strings.ToLower(strings.TrimSpace(e.ObjectHash))).
			SetAmountSatoshi(int64(e.AmountSatoshi)).
			SetStatus(status).
			SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
			SetCreatedAtUnix(createdAtUnix).
			SetFinishedAtUnix(finishedAtUnix).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "purchase_append_failed", map[string]any{
				"error":       err.Error(),
				"demand_id":   strings.TrimSpace(e.DemandID),
				"chunk_index": e.ChunkIndex,
			})
		}
		return err
	})
}

// dbAppendWalletFundFlow 已下线，改为 fact_* 事实表组装查询
// dbAppendWalletFundFlowFromContext 已下线

func auditCommandID(commandID string) (string, error) {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return "", fmt.Errorf("command_id is required")
	}
	return commandID, nil
}

func dbAppendGatewayEvent(ctx context.Context, store *clientDB, e gatewayEventEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	commandID, err := auditCommandID(e.CommandID)
	if err != nil {
		obs.Error(ServiceName, "gateway_event_append_rejected", map[string]any{"error": err.Error(), "action": strings.TrimSpace(e.Action)})
		return err
	}
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	if strings.TrimSpace(e.GatewayPeerID) == "" {
		e.GatewayPeerID = "unknown"
	}
	if strings.TrimSpace(e.Action) == "" {
		e.Action = "unknown"
	}
	err = gw.SaveGatewayEvent(ctx, gatewayclient.GatewayEvent{
		CreatedAtUnix:    time.Now().Unix(),
		GatewayPubkeyHex: strings.TrimSpace(e.GatewayPeerID),
		CommandID:        commandID,
		Action:           strings.TrimSpace(e.Action),
		MsgID:            strings.TrimSpace(e.MsgID),
		SequenceNum:      e.SequenceNum,
		PoolID:           strings.TrimSpace(e.PoolID),
		AmountSatoshi:    e.AmountSatoshi,
		Payload:          []byte(mustJSONString(e.Payload)),
	})
	if err != nil {
		obs.Error(ServiceName, "gateway_event_append_failed", map[string]any{"error": err.Error(), "action": e.Action, "command_id": commandID})
	}
	return err
}

func dbAppendOrchestratorLog(ctx context.Context, store *clientDB, e orchestratorLogEntry) {
	if store == nil {
		return
	}
	_ = store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.ProcOrchestratorLogs.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetEventType(strings.TrimSpace(e.EventType)).
			SetSource(strings.TrimSpace(e.Source)).
			SetSignalType(strings.TrimSpace(e.SignalType)).
			SetAggregateKey(strings.TrimSpace(e.AggregateKey)).
			SetCommandType(strings.TrimSpace(e.CommandType)).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetTaskStatus(strings.TrimSpace(e.TaskStatus)).
			SetRetryCount(int64(e.RetryCount)).
			SetQueueLength(int64(e.QueueLength)).
			SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "orchestrator_log_append_failed", map[string]any{
				"error":      err.Error(),
				"event_type": strings.TrimSpace(e.EventType),
			})
		}
		return nil
	})
}

func dbAppendCommandJournal(ctx context.Context, store *clientDB, e commandJournalEntry) error {
	if store == nil {
		return nil
	}
	commandID, err := auditCommandID(e.CommandID)
	if err != nil {
		obs.Error(ServiceName, "proc_command_journal_append_rejected", map[string]any{"error": err.Error(), "command_type": strings.TrimSpace(e.CommandType)})
		return err
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		accepted := 0
		if e.Accepted {
			accepted = 1
		}
		// trigger_key 是来源链路键，不是命令主键
		// - orchestrator 发起时，trigger_key = orchestrator.idempotency_key
		// - 非 orchestrator 发起时，trigger_key = ''
		_, err := tx.ProcCommandJournal.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetCommandType(strings.TrimSpace(e.CommandType)).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetAggregateID(strings.TrimSpace(e.AggregateID)).
			SetRequestedBy(strings.TrimSpace(e.RequestedBy)).
			SetRequestedAtUnix(e.RequestedAt).
			SetAccepted(int64(accepted)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetErrorCode(strings.TrimSpace(e.ErrorCode)).
			SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
			SetStateBefore(strings.TrimSpace(e.StateBefore)).
			SetStateAfter(strings.TrimSpace(e.StateAfter)).
			SetDurationMs(e.DurationMS).
			SetTriggerKey(strings.TrimSpace(e.TriggerKey)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetResultJSON(mustJSONString(e.Result)).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "proc_command_journal_append_failed", map[string]any{"error": err.Error(), "command_type": e.CommandType})
		}
		return err
	})
}

func dbAppendDomainEvent(ctx context.Context, store *clientDB, e domainEventEntry) error {
	if store == nil {
		return nil
	}
	commandID, err := auditCommandID(e.CommandID)
	if err != nil {
		obs.Error(ServiceName, "domain_event_append_rejected", map[string]any{"error": err.Error(), "event_name": strings.TrimSpace(e.EventName)})
		return err
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.ProcDomainEvents.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetEventName(strings.TrimSpace(e.EventName)).
			SetStateBefore(strings.TrimSpace(e.StateBefore)).
			SetStateAfter(strings.TrimSpace(e.StateAfter)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "domain_event_append_failed", map[string]any{"error": err.Error(), "event_name": e.EventName})
		}
		return err
	})
}

func dbAppendStateSnapshot(ctx context.Context, store *clientDB, e stateSnapshotEntry) error {
	if store == nil {
		return nil
	}
	commandID, err := auditCommandID(e.CommandID)
	if err != nil {
		obs.Error(ServiceName, "state_snapshot_append_rejected", map[string]any{"error": err.Error(), "state": strings.TrimSpace(e.State)})
		return err
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.ProcStateSnapshots.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetState(strings.TrimSpace(e.State)).
			SetPauseReason(strings.TrimSpace(e.PauseReason)).
			SetPauseNeedSatoshi(int64(e.PauseNeedSat)).
			SetPauseHaveSatoshi(int64(e.PauseHaveSat)).
			SetLastError(strings.TrimSpace(e.LastError)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "state_snapshot_append_failed", map[string]any{"error": err.Error(), "state": e.State})
		}
		return err
	})
}

type observedGatewayStateEntry struct {
	GatewayPeerID  string
	SourceRef      string
	ObservedAtUnix int64
	EventName      string
	StateBefore    string
	StateAfter     string
	PauseReason    string
	PauseNeedSat   uint64
	PauseHaveSat   uint64
	LastError      string
	Payload        any
}

func dbAppendObservedGatewayState(ctx context.Context, store *clientDB, e observedGatewayStateEntry) error {
	if store == nil {
		return nil
	}
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	observedAtUnix := e.ObservedAtUnix
	if observedAtUnix <= 0 {
		observedAtUnix = time.Now().Unix()
	}
	err = gw.SaveObservedGatewayState(ctx, gatewayclient.ObservedGatewayState{
		CreatedAtUnix:    time.Now().Unix(),
		GatewayPubkeyHex: strings.TrimSpace(e.GatewayPeerID),
		SourceRef:        strings.TrimSpace(e.SourceRef),
		ObservedAtUnix:   observedAtUnix,
		EventName:        strings.TrimSpace(e.EventName),
		StateBefore:      strings.TrimSpace(e.StateBefore),
		StateAfter:       strings.TrimSpace(e.StateAfter),
		PauseReason:      strings.TrimSpace(e.PauseReason),
		PauseNeedSatoshi: int64(e.PauseNeedSat),
		PauseHaveSatoshi: int64(e.PauseHaveSat),
		LastError:        strings.TrimSpace(e.LastError),
		Payload:          []byte(mustJSONString(e.Payload)),
	})
	if err != nil {
		obs.Error(ServiceName, "observed_gateway_state_append_failed", map[string]any{"error": err.Error(), "event_name": e.EventName})
	}
	return err
}

func dbAppendEffectLog(ctx context.Context, store *clientDB, e effectLogEntry) error {
	if store == nil {
		return nil
	}
	commandID, err := auditCommandID(e.CommandID)
	if err != nil {
		obs.Error(ServiceName, "effect_log_append_rejected", map[string]any{"error": err.Error(), "effect_type": strings.TrimSpace(e.EffectType), "stage": strings.TrimSpace(e.Stage)})
		return err
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		_, err := tx.ProcEffectLogs.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetEffectType(strings.TrimSpace(e.EffectType)).
			SetStage(strings.TrimSpace(e.Stage)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error(ServiceName, "effect_log_append_failed", map[string]any{"error": err.Error(), "effect_type": e.EffectType, "stage": e.Stage})
		}
		return err
	})
}

func dbAppendChainTipWorkerLog(ctx context.Context, store *clientDB, e chainWorkerLogEntry) {
	dbAppendChainWorkerLog(ctx, store, "proc_chain_tip_worker_logs", "chain_tip_worker_log_append_failed", e)
}

func dbAppendChainUTXOWorkerLog(ctx context.Context, store *clientDB, e chainWorkerLogEntry) {
	dbAppendChainWorkerLog(ctx, store, "proc_chain_utxo_worker_logs", "chain_utxo_worker_log_append_failed", e)
}

func dbAppendChainWorkerLog(ctx context.Context, store *clientDB, table string, errorEvent string, e chainWorkerLogEntry) {
	if store == nil {
		return
	}
	_ = store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		if e.TriggeredAtUnix <= 0 {
			e.TriggeredAtUnix = time.Now().Unix()
		}
		if e.StartedAtUnix <= 0 {
			e.StartedAtUnix = e.TriggeredAtUnix
		}
		if e.EndedAtUnix <= 0 {
			e.EndedAtUnix = e.StartedAtUnix
		}
		var err error
		switch strings.TrimSpace(table) {
		case "proc_chain_tip_worker_logs":
			_, err = tx.ProcChainTipWorkerLogs.Create().
				SetTriggeredAtUnix(e.TriggeredAtUnix).
				SetStartedAtUnix(e.StartedAtUnix).
				SetEndedAtUnix(e.EndedAtUnix).
				SetDurationMs(e.DurationMS).
				SetTriggerSource(strings.TrimSpace(e.TriggerSource)).
				SetStatus(strings.TrimSpace(e.Status)).
				SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
				SetResultJSON(mustJSONString(e.Result)).
				Save(ctx)
		case "proc_chain_utxo_worker_logs":
			_, err = tx.ProcChainUtxoWorkerLogs.Create().
				SetTriggeredAtUnix(e.TriggeredAtUnix).
				SetStartedAtUnix(e.StartedAtUnix).
				SetEndedAtUnix(e.EndedAtUnix).
				SetDurationMs(e.DurationMS).
				SetTriggerSource(strings.TrimSpace(e.TriggerSource)).
				SetStatus(strings.TrimSpace(e.Status)).
				SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
				SetResultJSON(mustJSONString(e.Result)).
				Save(ctx)
		default:
			return fmt.Errorf("unknown worker log table: %s", strings.TrimSpace(table))
		}
		if err != nil {
			obs.Error(ServiceName, errorEvent, map[string]any{"error": err.Error()})
			return nil
		}
		dbTrimWorkerLogsEntTx(ctx, tx, table, chainWorkerLogKeepCount)
		return nil
	})
}

func dbTrimWorkerLogsEntTx(ctx context.Context, tx EntWriteRoot, table string, keep int) {
	if tx == nil || strings.TrimSpace(table) == "" || keep <= 0 {
		return
	}
	switch strings.TrimSpace(table) {
	case "proc_chain_tip_worker_logs":
		ids, err := tx.ProcChainTipWorkerLogs.Query().
			Order(procchaintipworkerlogs.ByID(entsql.OrderDesc())).
			Limit(keep).
			IDs(ctx)
		if err != nil {
			obs.Error(ServiceName, "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
			return
		}
		if len(ids) == 0 {
			return
		}
		if _, err := tx.ProcChainTipWorkerLogs.Delete().
			Where(procchaintipworkerlogs.IDNotIn(ids...)).
			Exec(ctx); err != nil {
			obs.Error(ServiceName, "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
		}
	case "proc_chain_utxo_worker_logs":
		ids, err := tx.ProcChainUtxoWorkerLogs.Query().
			Order(procchainutxoworkerlogs.ByID(entsql.OrderDesc())).
			Limit(keep).
			IDs(ctx)
		if err != nil {
			obs.Error(ServiceName, "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
			return
		}
		if len(ids) == 0 {
			return
		}
		if _, err := tx.ProcChainUtxoWorkerLogs.Delete().
			Where(procchainutxoworkerlogs.IDNotIn(ids...)).
			Exec(ctx); err != nil {
			obs.Error(ServiceName, "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
		}
	default:
		obs.Error(ServiceName, "chain_worker_log_trim_failed", map[string]any{"error": "unknown worker log table", "table": table})
	}
}

// dbAppendFinBusinessRowTx 已下线（Group 8 cleanup）
// order_settlements 旧 settlement layer 已删除
func dbAppendFinBusinessRowTx(ctx context.Context, tx EntWriteRoot, e finBusinessEntry, settlementMethod, settlementStatus, settlementTargetType, settlementTargetID, settlementErrorMessage string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	obs.Info(ServiceName, "fin_business_row_compat_noop", map[string]any{
		"order_id":      strings.TrimSpace(e.OrderID),
		"settlement_id": strings.TrimSpace(e.SettlementID),
		"note":          "order_settlements schema removed; compatibility layer is now a no-op",
	})
	return nil
}

// dbAppendFinBusinessTx 已下线（Group 8 cleanup）
func dbAppendFinBusinessTx(ctx context.Context, tx EntWriteRoot, e finBusinessEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	obs.Info(ServiceName, "fin_business_compat_noop", map[string]any{
		"order_id": strings.TrimSpace(e.OrderID),
		"note":     "order_settlements schema removed; compatibility layer is now a no-op",
	})
	return nil
}

// dbAppendSettlementPaymentAttemptFinBusiness 已下线（Group 8 cleanup）
func dbAppendSettlementPaymentAttemptFinBusiness(ctx context.Context, tx EntWriteRoot, settlementPaymentAttemptID int64, e finBusinessEntry) error {
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	obs.Info(ServiceName, "fin_business_payment_attempt_compat_noop", map[string]any{
		"settlement_payment_attempt_id": settlementPaymentAttemptID,
		"order_id":                      strings.TrimSpace(e.OrderID),
		"note":                          "order_settlements schema removed; compatibility layer is now a no-op",
	})
	return nil
}

func dbAppendBusinessUTXOFactIfAbsent(_ any, txRole string) error {
	txRole = strings.TrimSpace(txRole)
	if txRole == "" {
		return fmt.Errorf("tx_role is required for business utxo fact")
	}
	// 旧的拆分/UTXO 明细表已硬切掉，这里只保留业务流程上的幂等占位。
	// 需要更细的 tx 解释时，改查 order_settlements / fact_* 事实表。
	return nil
}

// dbAppendFinProcessEvent 已下线（Group 8 cleanup）
// order_settlement_events 旧 settlement layer 已删除
func dbAppendFinProcessEvent(ctx context.Context, tx EntWriteRoot, e finProcessEventEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	return fmt.Errorf("order_settlement_events schema removed (Group 8 cleanup): process_id=%s", e.ProcessID)
}

// dbAppendSettlementPaymentAttemptFinProcessEvent 已下线（Group 8 cleanup）
func dbAppendSettlementPaymentAttemptFinProcessEvent(ctx context.Context, tx EntWriteRoot, settlementPaymentAttemptID int64, e finProcessEventEntry) error {
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	return fmt.Errorf("order_settlement_events schema removed (Group 8 cleanup): settlement_payment_attempt_id=%d", settlementPaymentAttemptID)
}

// dbApplyDirectTransferBizPoolAccountingTx 已下线（Group 8 cleanup）
// bizpool 和 factsettlementchannelpoolsessionquotepay 已删除
func dbApplyDirectTransferBizPoolAccountingTx(ctx context.Context, tx EntWriteRoot, in directTransferPoolAllocationFactInput, allocationNo int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	return fmt.Errorf("bizpool and fact_settlement_channel_pool_session_quote_pay schemas removed (Group 8 cleanup): session_id=%s", in.SessionID)
}

// directTransferPoolAccountingSource 直连池财务解释统一挂到 settlement_payment_attempt，别再用 session / allocation 漂着。
func directTransferPoolAccountingSource(sessionID string, allocationKind string, sequenceNum uint32) (string, string) {
	return "settlement_payment_attempt", directTransferPoolAllocationID(sessionID, allocationKind, sequenceNum)
}

func directTransferPoolAllocationID(sessionID string, allocationKind string, sequenceNum uint32) string {
	return fmt.Sprintf("%s_%s_%d", sessionID, allocationKind, sequenceNum)
}

// dbRecordFeePoolOpenAccounting 已下线（Group 8 cleanup）
// 旧 settlement layer 已删除
func dbRecordFeePoolOpenAccounting(ctx context.Context, store *clientDB, in feePoolOpenAccountingInput) {
	if store == nil {
		return
	}
	obs.Info(ServiceName, "fee_pool_open_accounting_deprecated", map[string]any{
		"business_id": strings.TrimSpace(in.BusinessID),
		"spend_txid":  strings.TrimSpace(in.SpendTxID),
		"note":        "Group 8 cleanup: settlement layer removed",
	})
}

// dbRecordFeePoolCycleEvent 已下线（Group 8 cleanup）
func dbRecordFeePoolCycleEvent(ctx context.Context, store *clientDB, spendTxID string, sequence uint32, amount uint64, gatewayPeerID string) {
	if store == nil {
		return
	}
	obs.Info(ServiceName, "fee_pool_cycle_event_deprecated", map[string]any{
		"spend_txid": strings.TrimSpace(spendTxID),
		"sequence":   sequence,
		"note":       "Group 8 cleanup: settlement layer removed",
	})
}

// dbRecordFeePoolQuotePayAccounting 已下线（Group 8 cleanup）
// fact_settlement_channel_pool_session_quote_pay 和 fact_pool_session_events 已删除
func dbRecordFeePoolQuotePayAccounting(ctx context.Context, store *clientDB, gatewayPubkeyHex string, spendTxID string, updatedTxID string, updatedTxHex string, chargedAmount uint64, chargeReason string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	obs.Info(ServiceName, "fee_pool_quote_pay_accounting_deprecated", map[string]any{
		"gateway":      strings.TrimSpace(gatewayPubkeyHex),
		"spend_txid":   strings.TrimSpace(spendTxID),
		"updated_txid": strings.TrimSpace(updatedTxID),
		"charged":      chargedAmount,
		"reason":       strings.TrimSpace(chargeReason),
		"note":         "Group 8 cleanup: settlement layer removed",
	})
	return nil
}

// dbRecordFeePoolCloseAccounting 已下线（Group 8 cleanup）
func dbRecordFeePoolCloseAccounting(ctx context.Context, store *clientDB, sessionID string, finalTxID string, finalTxHex string, gatewayPeerID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	return fmt.Errorf("fact_settlement_channel_pool_session_quote_pay and fact_pool_session_events schemas removed (Group 8 cleanup): session_id=%s", sessionID)
}

// dbRecordDirectPoolOpenAccounting 已下线（Group 8 cleanup）
// order_settlements 和 order_settlement_events 已删除
func dbRecordDirectPoolOpenAccounting(ctx context.Context, store *clientDB, in directPoolOpenAccountingInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return fmt.Errorf("order_settlements and order_settlement_events schemas removed (Group 8 cleanup): session_id=%s", strings.TrimSpace(in.SessionID))
}

// dbRecordDirectPoolPayAccounting 已下线（Group 8 cleanup）
func dbRecordDirectPoolPayAccounting(ctx context.Context, store *clientDB, downloadBusinessID string, sessionID string, sequence uint32, amount uint64, relatedTxID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return fmt.Errorf("order_settlement_events schema removed (Group 8 cleanup): session_id=%s", strings.TrimSpace(sessionID))
}

// dbRecordDirectPoolCloseAccounting 已下线（Group 8 cleanup）
func dbRecordDirectPoolCloseAccounting(ctx context.Context, store *clientDB, sessionID string, sequence uint32, finalTxID string, finalTxHex string, sellerAmount uint64, buyerAmount uint64, sellerPeerID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return fmt.Errorf("order_settlements and order_settlement_events schemas removed (Group 8 cleanup): session_id=%s", strings.TrimSpace(sessionID))
}

func dbRecordAccounting(ctx context.Context, store *clientDB, fn func(EntWriteRoot)) {
	if store == nil {
		return
	}
	_ = store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		fn(tx)
		return nil
	})
}

// recordChainPaymentAccountingAfterBroadcast Group 8: 旧 chain payment 账务记录已删除，新支付走 MultisigPool 模块
func recordChainPaymentAccountingAfterBroadcast(ctx context.Context, store any, rt *Runtime, txHex string, txID string, accountingScene string, accountingSubType string, fromPartyID string, toPartyID string) error {
	return nil
}

// dbUpsertBusinessSettlementEntTx Group 8: order_settlements schema 已删除
func dbUpsertBusinessSettlementEntTx(ctx context.Context, tx EntWriteRoot, e any) error {
	return fmt.Errorf("business settlement not available after Group 8 cleanup")
}
