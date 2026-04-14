package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizpool"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factpoolsessionevents"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementchannelpoolsessionquotepay"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/ordersettlementevents"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/ordersettlements"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procchaintipworkerlogs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/procchainutxoworkerlogs"
)

// 设计说明：
// - 这里收口运行期常见的日志、流水、账务写入；
// - 外层业务代码只表达“记什么”，不直接写 SQL；
// - 同一类写入统一走 clientDB，后续要补事务或限流时，只改这里。

type purchaseDoneEntry struct {
	DemandID       string
	SellerPubHex   string
	ArbiterPubHex  string
	ChunkIndex     uint32
	ObjectHash     string
	AmountSatoshi  uint64
	CreatedAtUnix  int64
	FinishedAtUnix int64
}

func dbAppendTxHistory(ctx context.Context, store *clientDB, e txHistoryEntry) {
	if store == nil {
		return
	}
	_ = clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		if strings.TrimSpace(e.GatewayPeerID) == "" {
			e.GatewayPeerID = "unknown"
		}
		if strings.TrimSpace(e.Direction) == "" {
			e.Direction = "info"
		}
		if strings.TrimSpace(e.Purpose) == "" {
			e.Purpose = e.EventType
		}
		now := time.Now().Unix()
		// tx_history 仍是兼容事实事件，写入口径统一收口到常量。
		allocationID := fmt.Sprintf("txhist_%s_%d_%d_%d", strings.TrimSpace(e.GatewayPeerID), now, e.SequenceNum, e.PaymentAttemptIndex)
		payload := mustJSONString(map[string]any{"event_type": e.EventType})
		existing, err := tx.FactPoolSessionEvents.Query().
			Where(factpoolsessionevents.AllocationIDEQ(allocationID)).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetEventKind(PoolFactEventKindTxHistory).
				SetState("confirmed").
				SetDirection(e.Direction).
				SetAmountSatoshi(e.AmountSatoshi).
				SetPurpose(e.Purpose).
				SetNote(e.Note).
				SetMsgID(e.MsgID).
				SetCycleIndex(int64(e.PaymentAttemptIndex)).
				SetTxid(e.PoolID).
				SetGatewayPubkeyHex(e.GatewayPeerID).
				SetCreatedAtUnix(now).
				SetPayloadJSON(payload).
				Save(ctx)
		} else if gen.IsNotFound(err) {
			_, err = tx.FactPoolSessionEvents.Create().
				SetAllocationID(allocationID).
				SetPoolSessionID("").
				SetAllocationNo(0).
				SetAllocationKind(PoolFactEventKindTxHistory).
				SetEventKind(PoolFactEventKindTxHistory).
				SetSequenceNum(int64(e.SequenceNum)).
				SetState("confirmed").
				SetDirection(e.Direction).
				SetAmountSatoshi(e.AmountSatoshi).
				SetPurpose(e.Purpose).
				SetNote(e.Note).
				SetMsgID(e.MsgID).
				SetCycleIndex(int64(e.PaymentAttemptIndex)).
				SetPayeeAmountAfter(0).
				SetPayerAmountAfter(0).
				SetTxid(e.PoolID).
				SetTxHex("").
				SetGatewayPubkeyHex(e.GatewayPeerID).
				SetCreatedAtUnix(now).
				SetPayloadJSON(payload).
				Save(ctx)
		}
		if err != nil {
			obs.Error("bitcast-client", "fact_pool_session_events_append_failed", map[string]any{"error": err.Error(), "event_type": e.EventType})
		}
		return err
	})
}

func dbAppendPurchaseDone(ctx context.Context, store *clientDB, e purchaseDoneEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
			SetStatus("done").
			SetErrorMessage("").
			SetCreatedAtUnix(createdAtUnix).
			SetFinishedAtUnix(finishedAtUnix).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "purchase_append_failed", map[string]any{
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

func dbAppendGatewayEvent(ctx context.Context, store *clientDB, e gatewayEventEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	commandID := strings.TrimSpace(e.CommandID)
	if commandID == "" {
		err := fmt.Errorf("command_id is required")
		obs.Error("bitcast-client", "gateway_event_append_rejected", map[string]any{"error": err.Error(), "action": strings.TrimSpace(e.Action)})
		return err
	}
	err := clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		if strings.TrimSpace(e.GatewayPeerID) == "" {
			e.GatewayPeerID = "unknown"
		}
		if strings.TrimSpace(e.Action) == "" {
			e.Action = "unknown"
		}
		_, err := tx.ProcGatewayEvents.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetGatewayPubkeyHex(e.GatewayPeerID).
			SetCommandID(commandID).
			SetAction(e.Action).
			SetMsgID(e.MsgID).
			SetSequenceNum(int64(e.SequenceNum)).
			SetPoolID(e.PoolID).
			SetAmountSatoshi(e.AmountSatoshi).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "gateway_event_append_failed", map[string]any{"error": err.Error(), "action": e.Action, "command_id": commandID})
		}
		return nil
	})
	return err
}

func dbAppendOrchestratorLog(ctx context.Context, store *clientDB, e orchestratorLogEntry) {
	if store == nil {
		return
	}
	_ = clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
			SetPayloadJSON(mustJSON(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "orchestrator_log_append_failed", map[string]any{
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
	commandID := strings.TrimSpace(e.CommandID)
	if commandID == "" {
		err := fmt.Errorf("command_id is required")
		obs.Error("bitcast-client", "proc_command_journal_append_rejected", map[string]any{"error": err.Error(), "command_type": strings.TrimSpace(e.CommandType)})
		return err
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
			SetPayloadJSON(mustJSON(e.Payload)).
			SetResultJSON(mustJSON(e.Result)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "proc_command_journal_append_failed", map[string]any{"error": err.Error(), "command_type": e.CommandType})
		}
		return err
	})
}

func dbAppendDomainEvent(ctx context.Context, store *clientDB, e domainEventEntry) error {
	if store == nil {
		return nil
	}
	commandID := strings.TrimSpace(e.CommandID)
	if commandID == "" {
		err := fmt.Errorf("command_id is required")
		obs.Error("bitcast-client", "domain_event_append_rejected", map[string]any{"error": err.Error(), "event_name": strings.TrimSpace(e.EventName)})
		return err
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		_, err := tx.ProcDomainEvents.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetEventName(strings.TrimSpace(e.EventName)).
			SetStateBefore(strings.TrimSpace(e.StateBefore)).
			SetStateAfter(strings.TrimSpace(e.StateAfter)).
			SetPayloadJSON(mustJSON(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "domain_event_append_failed", map[string]any{"error": err.Error(), "event_name": e.EventName})
		}
		return err
	})
}

func dbAppendStateSnapshot(ctx context.Context, store *clientDB, e stateSnapshotEntry) error {
	if store == nil {
		return nil
	}
	commandID := strings.TrimSpace(e.CommandID)
	if commandID == "" {
		err := fmt.Errorf("command_id is required")
		obs.Error("bitcast-client", "state_snapshot_append_rejected", map[string]any{"error": err.Error(), "state": strings.TrimSpace(e.State)})
		return err
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		_, err := tx.ProcStateSnapshots.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetState(strings.TrimSpace(e.State)).
			SetPauseReason(strings.TrimSpace(e.PauseReason)).
			SetPauseNeedSatoshi(int64(e.PauseNeedSat)).
			SetPauseHaveSatoshi(int64(e.PauseHaveSat)).
			SetLastError(strings.TrimSpace(e.LastError)).
			SetPayloadJSON(mustJSON(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "state_snapshot_append_failed", map[string]any{"error": err.Error(), "state": e.State})
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
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		observedAtUnix := e.ObservedAtUnix
		if observedAtUnix <= 0 {
			observedAtUnix = time.Now().Unix()
		}
		_, err := tx.ProcObservedGatewayStates.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetSourceRef(strings.TrimSpace(e.SourceRef)).
			SetObservedAtUnix(observedAtUnix).
			SetEventName(strings.TrimSpace(e.EventName)).
			SetStateBefore(strings.TrimSpace(e.StateBefore)).
			SetStateAfter(strings.TrimSpace(e.StateAfter)).
			SetPauseReason(strings.TrimSpace(e.PauseReason)).
			SetPauseNeedSatoshi(int64(e.PauseNeedSat)).
			SetPauseHaveSatoshi(int64(e.PauseHaveSat)).
			SetLastError(strings.TrimSpace(e.LastError)).
			SetPayloadJSON(mustJSON(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "observed_gateway_state_append_failed", map[string]any{"error": err.Error(), "event_name": e.EventName})
		}
		return err
	})
}

func dbAppendEffectLog(ctx context.Context, store *clientDB, e effectLogEntry) error {
	if store == nil {
		return nil
	}
	commandID := strings.TrimSpace(e.CommandID)
	if commandID == "" {
		err := fmt.Errorf("command_id is required")
		obs.Error("bitcast-client", "effect_log_append_rejected", map[string]any{"error": err.Error(), "effect_type": strings.TrimSpace(e.EffectType), "stage": strings.TrimSpace(e.Stage)})
		return err
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		_, err := tx.ProcEffectLogs.Create().
			SetCreatedAtUnix(time.Now().Unix()).
			SetCommandID(commandID).
			SetGatewayPubkeyHex(strings.TrimSpace(e.GatewayPeerID)).
			SetEffectType(strings.TrimSpace(e.EffectType)).
			SetStage(strings.TrimSpace(e.Stage)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetErrorMessage(strings.TrimSpace(e.ErrorMessage)).
			SetPayloadJSON(mustJSON(e.Payload)).
			Save(ctx)
		if err != nil {
			obs.Error("bitcast-client", "effect_log_append_failed", map[string]any{"error": err.Error(), "effect_type": e.EffectType, "stage": e.Stage})
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
	_ = clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
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
				SetResultJSON(mustJSON(e.Result)).
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
				SetResultJSON(mustJSON(e.Result)).
				Save(ctx)
		default:
			return fmt.Errorf("unknown worker log table: %s", strings.TrimSpace(table))
		}
		if err != nil {
			obs.Error("bitcast-client", errorEvent, map[string]any{"error": err.Error()})
			return nil
		}
		dbTrimWorkerLogsEntTx(ctx, tx, table, chainWorkerLogKeepCount)
		return nil
	})
}

func dbTrimWorkerLogsEntTx(ctx context.Context, tx *gen.Tx, table string, keep int) {
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
			obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
			return
		}
		if len(ids) == 0 {
			return
		}
		if _, err := tx.ProcChainTipWorkerLogs.Delete().
			Where(procchaintipworkerlogs.IDNotIn(ids...)).
			Exec(ctx); err != nil {
			obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
		}
	case "proc_chain_utxo_worker_logs":
		ids, err := tx.ProcChainUtxoWorkerLogs.Query().
			Order(procchainutxoworkerlogs.ByID(entsql.OrderDesc())).
			Limit(keep).
			IDs(ctx)
		if err != nil {
			obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
			return
		}
		if len(ids) == 0 {
			return
		}
		if _, err := tx.ProcChainUtxoWorkerLogs.Delete().
			Where(procchainutxoworkerlogs.IDNotIn(ids...)).
			Exec(ctx); err != nil {
			obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
		}
	default:
		obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": "unknown worker log table", "table": table})
	}
}

// dbUpsertSettleRecord 是共享写入口，给前台业务和非结算主线复用。
// 设计边界：
// - 这里只负责把一条财务事实稳定落库，不判断业务链路归属；
// - 结算出口字段由桥接层单独补，不在这里猜；
// - order_settlements 已经是唯一主表，旧来源一律拒绝。

func dbAppendFinBusinessTx(ctx context.Context, tx *gen.Tx, e finBusinessEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.OrderID = strings.TrimSpace(e.OrderID)
	if e.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if strings.TrimSpace(e.SettlementID) == "" {
		e.SettlementID = e.OrderID
	}
	e.BusinessRole = strings.TrimSpace(e.BusinessRole)
	if e.BusinessRole == "" {
		return fmt.Errorf("business_role is required: must be 'formal' or 'process'")
	}
	if e.BusinessRole != "formal" && e.BusinessRole != "process" {
		return fmt.Errorf("business_role must be 'formal' or 'process', got '%s'", e.BusinessRole)
	}
	e.SourceType = strings.ToLower(strings.TrimSpace(e.SourceType))
	e.SourceID = strings.TrimSpace(e.SourceID)
	switch e.SourceType {
	case "":
		return fmt.Errorf("source_type is required")
	case "fee_pool", "pool_allocation", "chain_quote_pay":
		return fmt.Errorf("source_type must be settlement_payment_attempt")
	}
	if e.SourceID == "" {
		return fmt.Errorf("source_id is required")
	}
	settlementMethod := strings.TrimSpace(e.SettlementMethod)
	settlementStatus := strings.TrimSpace(e.SettlementStatus)
	if settlementStatus == "" {
		settlementStatus = strings.TrimSpace(e.Status)
	}
	settlementTargetType := strings.TrimSpace(e.SettlementTargetType)
	settlementTargetID := strings.TrimSpace(e.SettlementTargetID)
	settlementErrorMessage := strings.TrimSpace(e.SettlementErrorMessage)

	return dbAppendFinBusinessRowTx(ctx, tx, e, settlementMethod, settlementStatus, settlementTargetType, settlementTargetID, settlementErrorMessage)
}

func dbAppendFinBusinessRowTx(ctx context.Context, tx *gen.Tx, e finBusinessEntry, settlementMethod, settlementStatus, settlementTargetType, settlementTargetID, settlementErrorMessage string) error {
	existing, err := tx.OrderSettlements.Query().
		Where(ordersettlements.SettlementIDEQ(e.SettlementID)).
		Only(ctx)
	if err == nil {
		if strings.TrimSpace(existing.OrderID) != "" && strings.TrimSpace(existing.OrderID) != e.OrderID {
			return fmt.Errorf("order_id mismatch for settlement_id=%s", e.SettlementID)
		}
		_, err = tx.OrderSettlements.UpdateOneID(existing.ID).
			SetBusinessRole(e.BusinessRole).
			SetSourceType(e.SourceType).
			SetSourceID(e.SourceID).
			SetAccountingScene(strings.TrimSpace(e.AccountingScene)).
			SetAccountingSubtype(strings.TrimSpace(e.AccountingSubType)).
			SetSettlementMethod(settlementMethod).
			SetStatus(strings.TrimSpace(e.Status)).
			SetSettlementStatus(settlementStatus).
			SetAmountSatoshi(0).
			SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
			SetToPartyID(strings.TrimSpace(e.ToPartyID)).
			SetTargetType(settlementTargetType).
			SetTargetID(settlementTargetID).
			SetNote(strings.TrimSpace(e.Note)).
			SetErrorMessage(settlementErrorMessage).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetSettlementPayloadJSON(mustJSONString(e.SettlementPayload)).
			SetUpdatedAtUnix(e.OccurredAtUnix).
			Save(ctx)
		return err
	}
	if err != nil && !gen.IsNotFound(err) {
		return err
	}

	nextSettlementNo := int64(1)
	last, err := tx.OrderSettlements.Query().
		Where(ordersettlements.OrderIDEQ(e.OrderID)).
		Order(ordersettlements.BySettlementNo(entsql.OrderDesc())).
		First(ctx)
	if err == nil {
		nextSettlementNo = last.SettlementNo + 1
	} else if err != nil && !gen.IsNotFound(err) {
		return err
	}

	_, err = tx.OrderSettlements.Create().
		SetSettlementID(e.SettlementID).
		SetOrderID(e.OrderID).
		SetSettlementNo(nextSettlementNo).
		SetBusinessRole(e.BusinessRole).
		SetSourceType(e.SourceType).
		SetSourceID(e.SourceID).
		SetAccountingScene(strings.TrimSpace(e.AccountingScene)).
		SetAccountingSubtype(strings.TrimSpace(e.AccountingSubType)).
		SetSettlementMethod(settlementMethod).
		SetStatus(strings.TrimSpace(e.Status)).
		SetSettlementStatus(settlementStatus).
		SetAmountSatoshi(0).
		SetFromPartyID(strings.TrimSpace(e.FromPartyID)).
		SetToPartyID(strings.TrimSpace(e.ToPartyID)).
		SetTargetType(settlementTargetType).
		SetTargetID(settlementTargetID).
		SetNote(strings.TrimSpace(e.Note)).
		SetErrorMessage(settlementErrorMessage).
		SetPayloadJSON(mustJSONString(e.Payload)).
		SetSettlementPayloadJSON(mustJSONString(e.SettlementPayload)).
		SetCreatedAtUnix(e.OccurredAtUnix).
		SetUpdatedAtUnix(e.OccurredAtUnix).
		Save(ctx)
	return err
}

// dbAppendFinBusiness 是共享写入口，给前台业务和非结算主线复用。
// 设计边界：
// - 这里只负责把一条财务业务事实稳定落库，不判断业务链路归属；
// - 结算链路必须走 dbAppendSettlementPaymentAttemptFinBusiness，不能绕回这个入口；
// - 这里已经收口到 settlement_payment_attempt，旧来源一律拒绝。

// 结算写入专用入口只认 settlement_payment_attempt 主键，调用方不再有机会手填来源口径。
// 设计边界：
// - source_type/source_id 在这里统一生成；
// - 结算链路只传 settlementPaymentAttemptID 和业务字段；
// - 这样才能把“入口可用”和“入口可误用”彻底分开。
func dbAppendSettlementPaymentAttemptFinBusiness(ctx context.Context, tx *gen.Tx, settlementPaymentAttemptID int64, e finBusinessEntry) error {
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	e.SourceType = "settlement_payment_attempt"
	e.SourceID = fmt.Sprintf("%d", settlementPaymentAttemptID)
	return dbAppendFinBusinessTx(ctx, tx, e)
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

// dbAppendFinProcessEvent 是共享写入口，给前台业务和非结算主线复用。
// 设计边界：
// - 这里只负责落一条财务流程事件，不替调用方兜底来源口径；
// - 结算链路里，settlement_payment_attempt 继续走专用封装，biz_order_pay_bsv 直接用 settlement_id 作为来源；
// - 不要在这里塞兼容分支，不然历史口径会重新污染主线；
// - source_id 先按 settlement_id 使用，若是 settlement_payment_attempt 再反查真实 settlement。
func dbAppendFinProcessEvent(ctx context.Context, tx *gen.Tx, e finProcessEventEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.ProcessID = strings.TrimSpace(e.ProcessID)
	if e.ProcessID == "" {
		return fmt.Errorf("process_id is required")
	}
	e.SourceType = strings.ToLower(strings.TrimSpace(e.SourceType))
	e.SourceID = strings.TrimSpace(e.SourceID)
	switch e.SourceType {
	case "":
		return fmt.Errorf("source_type is required")
	case "fee_pool", "pool_allocation", "chain_quote_pay":
		return fmt.Errorf("source_type must be settlement_payment_attempt")
	}
	if e.SourceID == "" {
		return fmt.Errorf("source_id is required")
	}
	settlementID := strings.TrimSpace(e.SourceID)
	orderID := ""
	if settlementID == "" {
		settlementID = strings.TrimSpace(e.ProcessID)
	}
	if strings.TrimSpace(e.SourceType) == "settlement_payment_attempt" {
		resolved, err := tx.OrderSettlements.Query().
			Where(
				ordersettlements.SourceTypeEQ(e.SourceType),
				ordersettlements.SourceIDEQ(e.SourceID),
			).
			Order(ordersettlements.ByUpdatedAtUnix(entsql.OrderDesc()), ordersettlements.BySettlementNo(entsql.OrderDesc())).
			First(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("settlement not found for source_id=%s", e.SourceID)
			}
			return err
		}
		settlementID = strings.TrimSpace(resolved.SettlementID)
		if settlementID == "" {
			return fmt.Errorf("settlement not found for source_id=%s", e.SourceID)
		}
		orderID = strings.TrimSpace(resolved.OrderID)
	}
	if orderID == "" {
		settleRow, err := tx.OrderSettlements.Query().Where(ordersettlements.SettlementIDEQ(settlementID)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("order_settlement not found for settlement_id=%s", settlementID)
			}
			return err
		}
		orderID = strings.TrimSpace(settleRow.OrderID)
		if orderID == "" {
			return fmt.Errorf("order_id is empty for settlement_id=%s", settlementID)
		}
	}

	existing, err := tx.OrderSettlementEvents.Query().
		Where(
			ordersettlementevents.SettlementIDEQ(settlementID),
			ordersettlementevents.EventTypeEQ(strings.TrimSpace(e.EventType)),
			ordersettlementevents.OrderIDEQ(orderID),
		).
		Only(ctx)
	if err == nil {
		_, err = tx.OrderSettlementEvents.UpdateOneID(existing.ID).
			SetProcessID(strings.TrimSpace(e.ProcessID)).
			SetSettlementID(settlementID).
			SetOrderID(orderID).
			SetSourceType(strings.TrimSpace(e.SourceType)).
			SetSourceID(strings.TrimSpace(e.SourceID)).
			SetAccountingScene(strings.TrimSpace(e.AccountingScene)).
			SetAccountingSubtype(strings.TrimSpace(e.AccountingSubType)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetOccurredAtUnix(e.OccurredAtUnix).
			Save(ctx)
		return err
	}
	if err != nil && !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.OrderSettlementEvents.Create().
		SetProcessID(strings.TrimSpace(e.ProcessID)).
		SetSettlementID(settlementID).
		SetOrderID(orderID).
		SetSourceType(strings.TrimSpace(e.SourceType)).
		SetSourceID(strings.TrimSpace(e.SourceID)).
		SetAccountingScene(strings.TrimSpace(e.AccountingScene)).
		SetAccountingSubtype(strings.TrimSpace(e.AccountingSubType)).
		SetEventType(strings.TrimSpace(e.EventType)).
		SetStatus(strings.TrimSpace(e.Status)).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		SetOccurredAtUnix(e.OccurredAtUnix).
		Save(ctx)
	return err
}

// 结算流程事件专用入口只认 settlement_payment_attempt 主键，避免调用方把旧来源带进来。
func dbAppendSettlementPaymentAttemptFinProcessEvent(ctx context.Context, tx *gen.Tx, settlementPaymentAttemptID int64, e finProcessEventEntry) error {
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id must be positive")
	}
	e.SourceType = "settlement_payment_attempt"
	e.SourceID = fmt.Sprintf("%d", settlementPaymentAttemptID)
	return dbAppendFinProcessEvent(ctx, tx, e)
}

// dbApplyDirectTransferBizPoolAccountingTx 统一写 direct_transfer_pool 的业务层池账。
// 设计说明：
// - 这里只负责 biz_pool_allocations 和 biz_pool 快照，不碰 fact 消耗主路径；
// - 调用方仍然可以先写兼容 fact 事件，但真正的划拨账必须从这里落到业务层；
// - 幂等性依赖 allocation_id + (pool_session_id, allocation_kind, sequence_num) 的唯一约束。
func dbApplyDirectTransferBizPoolAccountingTx(ctx context.Context, tx *gen.Tx, in directTransferPoolAllocationFactInput, allocationNo int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	sessionID := strings.TrimSpace(in.SessionID)
	if sessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	kind := strings.TrimSpace(in.AllocationKind)
	if kind == "" {
		return fmt.Errorf("allocation_kind is required")
	}
	allocationID := directTransferPoolAllocationID(sessionID, kind, in.SequenceNum)
	if allocationID == "" {
		return fmt.Errorf("allocation_id is required")
	}
	txID := strings.ToLower(strings.TrimSpace(in.TxID))
	if txID == "" {
		return fmt.Errorf("txid is required")
	}
	txHex := strings.ToLower(strings.TrimSpace(in.TxHex))
	if txHex == "" {
		return fmt.Errorf("tx_hex is required")
	}
	if allocationNo <= 0 {
		return fmt.Errorf("allocation_no must be positive")
	}

	existingPool, err := tx.BizPool.Query().
		Where(bizpool.PoolSessionIDEQ(sessionID)).
		Only(ctx)
	if err == nil {
		existingStatus := strings.ToLower(strings.TrimSpace(existingPool.Status))
		if existingStatus == "closed" && kind != PoolBusinessActionClose {
			return fmt.Errorf("pool session %s is closed", sessionID)
		}
	} else if err != nil && !gen.IsNotFound(err) {
		return err
	}

	session, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
		Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(sessionID)).
		Only(ctx)
	if err != nil {
		return fmt.Errorf("load fact_settlement_channel_pool_session_quote_pay for %s: %w", sessionID, err)
	}

	poolAmountSat := uint64(0)
	if session.PoolAmountSatoshi > 0 {
		poolAmountSat = uint64(session.PoolAmountSatoshi)
	}
	if session.SpendTxFeeSatoshi > 0 {
		poolAmountSat += uint64(session.SpendTxFeeSatoshi)
	}
	cycleFeeSat := uint64(0)
	if session.SpendTxFeeSatoshi > 0 {
		cycleFeeSat = uint64(session.SpendTxFeeSatoshi)
	}
	allocatedSat := in.PayeeAmountAfter
	availableSat := in.PayerAmountAfter
	if kind == PoolBusinessActionOpen {
		allocatedSat = 0
		availableSat = uint64(session.PoolAmountSatoshi)
		if availableSat == 0 && poolAmountSat >= cycleFeeSat {
			availableSat = poolAmountSat - cycleFeeSat
		}
	}
	if availableSat == 0 && poolAmountSat >= allocatedSat+cycleFeeSat {
		availableSat = poolAmountSat - allocatedSat - cycleFeeSat
	}
	sessionStatus := strings.TrimSpace(session.Status)
	if sessionStatus == "" {
		sessionStatus = "active"
	}
	now := time.Now().Unix()
	createdAt := in.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}

	if err := dbUpsertDirectTransferBizPoolAllocationTx(ctx, tx, directTransferBizPoolAllocationInput{
		SessionID:        sessionID,
		AllocationID:     allocationID,
		AllocationNo:     allocationNo,
		AllocationKind:   kind,
		SequenceNum:      in.SequenceNum,
		PayeeAmountAfter: allocatedSat,
		PayerAmountAfter: availableSat,
		TxID:             txID,
		TxHex:            txHex,
		CreatedAtUnix:    createdAt,
	}); err != nil {
		return fmt.Errorf("upsert biz pool allocation for %s: %w", allocationID, err)
	}

	snapshot := directTransferBizPoolSnapshotInput{
		SessionID:          sessionID,
		PoolScheme:         session.PoolScheme,
		CounterpartyPubHex: session.CounterpartyPubkeyHex,
		SellerPubHex:       session.SellerPubkeyHex,
		ArbiterPubHex:      session.ArbiterPubkeyHex,
		GatewayPubHex:      session.GatewayPubkeyHex,
		PoolAmountSat:      poolAmountSat,
		SpendTxFeeSat:      uint64(session.SpendTxFeeSatoshi),
		AllocatedSat:       allocatedSat,
		CycleFeeSat:        cycleFeeSat,
		AvailableSat:       availableSat,
		NextSequenceNum:    in.SequenceNum + 1,
		Status:             sessionStatus,
		OpenBaseTxID:       session.OpenBaseTxid,
		CreatedAtUnix:      session.CreatedAtUnix,
		UpdatedAtUnix:      createdAt,
	}
	switch kind {
	case PoolBusinessActionOpen:
		snapshot.OpenAllocationID = allocationID
	case PoolBusinessActionClose:
		snapshot.CloseAllocationID = allocationID
		if strings.TrimSpace(snapshot.Status) == "active" || strings.TrimSpace(snapshot.Status) == "closing" {
			snapshot.Status = "closed"
		}
	}
	if err := dbUpsertDirectTransferBizPoolSnapshotTx(ctx, tx, snapshot); err != nil {
		return fmt.Errorf("upsert biz pool snapshot for %s: %w", sessionID, err)
	}
	return nil
}

// 直连池财务解释统一挂到 settlement_payment_attempt，别再用 session / allocation 漂着。
func directTransferPoolAccountingSource(sessionID string, allocationKind string, sequenceNum uint32) (string, string) {
	return "settlement_payment_attempt", directTransferPoolAllocationID(sessionID, allocationKind, sequenceNum)
}

func dbRecordFeePoolOpenAccounting(ctx context.Context, store *clientDB, in feePoolOpenAccountingInput) {
	dbRecordAccounting(ctx, store, func(tx *gen.Tx) {
		businessID := strings.TrimSpace(in.BusinessID)
		if businessID == "" {
			businessID = "biz_feepool_open_" + randHex(8)
		}
		baseTxHex := strings.TrimSpace(in.BaseTxHex)
		baseTxID := strings.ToLower(strings.TrimSpace(in.BaseTxID))
		lockScript := strings.TrimSpace(in.ClientLockScript)
		var grossInput, changeBack, lockAmount int64
		if baseTxHex != "" {
			t, err := transaction.NewTransactionFromHex(baseTxHex)
			if err != nil {
				obs.Error("bitcast-client", "fee_pool_open_parse_base_tx_failed", map[string]any{"error": err.Error(), "base_txid": baseTxID})
			} else {
				for _, input := range t.Inputs {
					if input.SourceTxOutput() != nil {
						grossInput += int64(input.SourceTxOutput().Satoshis)
						if input.SourceTXID != nil {
							_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
						}
					}
				}
				for idx, out := range t.Outputs {
					amount := int64(out.Satoshis)
					if idx == 0 {
						lockAmount += amount
						_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
						continue
					}
					if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
						changeBack += amount
						_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
					}
				}
				if lockAmount == 0 {
					lockAmount = int64(in.PoolAmountSatoshi)
				}
			}
		}
		if lockAmount == 0 {
			lockAmount = int64(in.PoolAmountSatoshi)
		}
		minerFee := grossInput - changeBack - lockAmount
		if minerFee < 0 {
			minerFee = 0
		}
		// 这里直接按 chain_payment 反查 settlement_payment_attempt，别再把旧 fee_pool 口径塞回写入口。
		settlementPaymentAttemptID, err := resolveChainPaymentSourceToSettlementPaymentAttempt(ctx, store, strings.TrimSpace(in.SpendTxID))
		if err != nil {
			obs.Error("bitcast-client", "fee_pool_open_record_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
			return
		}
		if err := dbAppendSettlementPaymentAttemptFinBusiness(ctx, tx, settlementPaymentAttemptID, finBusinessEntry{
			OrderID:           businessID,
			BusinessRole:      "process", // 过程财务对象
			AccountingScene:   "fee_pool",
			AccountingSubType: "open",
			FromPartyID:       strings.TrimSpace(in.FromPartyID),
			ToPartyID:         strings.TrimSpace(in.ToPartyID),
			Status:            "posted",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "fee_pool_open:" + strings.TrimSpace(in.SpendTxID),
			Note:              "fee pool open lock",
			Payload: map[string]any{
				"spend_txid": strings.TrimSpace(in.SpendTxID),
				"base_txid":  baseTxID,
			},
		}); err != nil {
			obs.Error("bitcast-client", "fee_pool_open_record_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
			return
		}
		// 旧 tx 拆解层已下线，业务主事实只保留 order_settlements。
	})
}

func dbRecordFeePoolCycleEvent(ctx context.Context, store *clientDB, spendTxID string, sequence uint32, amount uint64, gatewayPeerID string) {
	dbRecordAccounting(ctx, store, func(tx *gen.Tx) {
		processID := "proc_feepool_cycle_" + strings.TrimSpace(spendTxID)
		// 这里直接按 chain_payment 反查 settlement_payment_attempt，保证写入和查询走同一条路。
		settlementPaymentAttemptID, err := resolveChainPaymentSourceToSettlementPaymentAttempt(ctx, store, strings.TrimSpace(spendTxID))
		if err != nil {
			obs.Error("bitcast-client", "fee_pool_cycle_record_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_cycle"})
			return
		}
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, tx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         processID,
			AccountingScene:   "fee_pool",
			AccountingSubType: "cycle_pay",
			EventType:         "update",
			Status:            "applied",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "fee_pool_cycle_event:" + strings.TrimSpace(spendTxID) + ":" + fmt.Sprint(sequence),
			Note:              "fee pool cycle event (offchain)",
			Payload: map[string]any{
				"sequence":           sequence,
				"charge_amount_sat":  amount,
				"gateway_pubkey_hex": strings.TrimSpace(gatewayPeerID),
				"financial_affected": false,
			},
		}); err != nil {
			obs.Error("bitcast-client", "fee_pool_cycle_record_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_cycle"})
		}
	})
}

// dbRecordFeePoolQuotePayAccounting 把费用池 quote/pay 的真实回执落到共享事实表。
// 设计说明：
// - 只写真实支付成功后的结果，不补“模拟账”；
// - pool_session_id 统一用 fee pool 会话的 spend_txid，保证本地 sqlite 可反查；
// - 这里同时写 payment_attempt、channel 和 pool_session_events，满足后续断言与财务回看。
func dbRecordFeePoolQuotePayAccounting(ctx context.Context, store *clientDB, gatewayPubkeyHex string, session *feePoolSession, updatedTxID string, updatedTxHex string, chargedAmount uint64, chargeReason string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if session == nil {
		return fmt.Errorf("fee pool session missing")
	}
	sessionID := strings.TrimSpace(session.SpendTxID)
	if sessionID == "" {
		return fmt.Errorf("fee pool session spend_txid is required")
	}
	updatedTxID = strings.ToLower(strings.TrimSpace(updatedTxID))
	if updatedTxID == "" {
		return fmt.Errorf("updated_txid is required")
	}
	updatedTxHex = strings.ToLower(strings.TrimSpace(updatedTxHex))
	if updatedTxHex == "" {
		return fmt.Errorf("updated_tx_hex is required")
	}
	gatewayPubkeyHex = strings.ToLower(strings.TrimSpace(gatewayPubkeyHex))
	if gatewayPubkeyHex == "" {
		gatewayPubkeyHex = "unknown"
	}
	chargeReason = strings.TrimSpace(chargeReason)
	if chargeReason == "" {
		chargeReason = "demand_publish_fee"
	}
	now := time.Now().Unix()
	sequence := session.Sequence
	if sequence == 0 {
		sequence = 1
	}
	allocationID := directTransferPoolAllocationID(sessionID, "pay", sequence)
	if allocationID == "" {
		return fmt.Errorf("allocation_id is required")
	}
	payload := map[string]any{
		"session_id":                  sessionID,
		"gateway_pubkey_hex":          gatewayPubkeyHex,
		"updated_txid":                updatedTxID,
		"updated_tx_hex":              updatedTxHex,
		"charge_reason":               chargeReason,
		"charged_amount_satoshi":      chargedAmount,
		"sequence_num":                sequence,
		"pool_amount_satoshi":         session.PoolAmountSat,
		"spend_tx_fee_satoshi":        session.SpendTxFeeSat,
		"server_amount_after_satoshi": session.ServerAmount,
		"client_amount_after_satoshi": session.ClientAmount,
		"billing_cycle_seconds":       session.BillingCycleSeconds,
		"single_cycle_fee_satoshi":    session.SingleCycleFeeSatoshi,
		"single_publish_fee_satoshi":  session.SinglePublishFeeSatoshi,
		"single_query_fee_satoshi":    session.SingleQueryFeeSatoshi,
		"minimum_pool_amount_satoshi": session.MinimumPoolAmountSatoshi,
		"lock_blocks":                 session.LockBlocks,
		"fee_rate_sat_per_byte":       session.FeeRateSatPerByte,
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		var (
			settlementPaymentAttemptID int64
			channelID                  int64
		)
		existing, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
			Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(sessionID)).
			Only(ctx)
		if err == nil {
			channelID = int64(existing.ID)
			settlementPaymentAttemptID = existing.SettlementPaymentAttemptID
		} else if err != nil && !gen.IsNotFound(err) {
			return err
		}
		if channelID == 0 {
			pendingSourceID := "pending:pool_session_quote_pay:" + sessionID
			settlementPaymentAttemptID, err = dbUpsertSettlementPaymentAttemptEntTx(ctx, tx,
				"payment_attempt_pool_session_quote_pay_"+sessionID,
				"pool_session_quote_pay",
				pendingSourceID,
				"confirmed",
				int64(chargedAmount), 0, int64(chargedAmount),
				int(sequence),
				now,
				"fee pool quote pay accounting",
				payload,
			)
			if err != nil {
				return err
			}
		}
		if existing != nil {
			_, err = tx.FactSettlementChannelPoolSessionQuotePay.UpdateOneID(existing.ID).
				SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
				SetTxid(updatedTxID).
				SetPoolScheme("2of2").
				SetCounterpartyPubkeyHex("").
				SetSellerPubkeyHex("").
				SetArbiterPubkeyHex("").
				SetGatewayPubkeyHex(gatewayPubkeyHex).
				SetPoolAmountSatoshi(int64(session.PoolAmountSat)).
				SetSpendTxFeeSatoshi(int64(session.SpendTxFeeSat)).
				SetFeeRateSatByte(session.FeeRateSatPerByte).
				SetLockBlocks(int64(session.LockBlocks)).
				SetOpenBaseTxid(strings.ToLower(strings.TrimSpace(session.BaseTxID))).
				SetStatus("confirmed").
				SetUpdatedAtUnix(now).
				Save(ctx)
		} else {
			node, err := tx.FactSettlementChannelPoolSessionQuotePay.Create().
				SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
				SetPoolSessionID(sessionID).
				SetTxid(updatedTxID).
				SetPoolScheme("2of2").
				SetCounterpartyPubkeyHex("").
				SetSellerPubkeyHex("").
				SetArbiterPubkeyHex("").
				SetGatewayPubkeyHex(gatewayPubkeyHex).
				SetPoolAmountSatoshi(int64(session.PoolAmountSat)).
				SetSpendTxFeeSatoshi(int64(session.SpendTxFeeSat)).
				SetFeeRateSatByte(session.FeeRateSatPerByte).
				SetLockBlocks(int64(session.LockBlocks)).
				SetOpenBaseTxid(strings.ToLower(strings.TrimSpace(session.BaseTxID))).
				SetStatus("confirmed").
				SetCreatedAtUnix(now).
				SetUpdatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return err
			}
			channelID = int64(node.ID)
		}
		if settlementPaymentAttemptID > 0 {
			if _, err := tx.FactSettlementPaymentAttempts.UpdateOneID(int64(settlementPaymentAttemptID)).
				SetSourceType("pool_session_quote_pay").
				SetSourceID(fmt.Sprintf("%d", channelID)).
				SetState("confirmed").
				SetOccurredAtUnix(now).
				SetConfirmedAtUnix(now).
				SetNote("fee pool quote pay accounting").
				SetPayloadJSON(mustJSONString(payload)).
				Save(ctx); err != nil {
				return err
			}
		}
		event, err := tx.FactPoolSessionEvents.Query().
			Where(factpoolsessionevents.AllocationIDEQ(allocationID)).
			Only(ctx)
		if err == nil {
			_, err = tx.FactPoolSessionEvents.UpdateOneID(event.ID).
				SetPoolSessionID(sessionID).
				SetAllocationNo(int64(sequence)).
				SetAllocationKind("pay").
				SetEventKind(PoolFactEventKindPoolEvent).
				SetSequenceNum(int64(sequence)).
				SetState("confirmed").
				SetDirection("out").
				SetAmountSatoshi(int64(chargedAmount)).
				SetPurpose(chargeReason).
				SetNote("fee pool quote pay accounting").
				SetMsgID(updatedTxID).
				SetCycleIndex(int64(sequence)).
				SetPayeeAmountAfter(int64(session.ServerAmount)).
				SetPayerAmountAfter(int64(session.ClientAmount)).
				SetTxid(updatedTxID).
				SetTxHex(updatedTxHex).
				SetGatewayPubkeyHex(gatewayPubkeyHex).
				SetCreatedAtUnix(now).
				SetPayloadJSON(mustJSONString(payload)).
				Save(ctx)
			return err
		}
		if err != nil && !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.FactPoolSessionEvents.Create().
			SetAllocationID(allocationID).
			SetPoolSessionID(sessionID).
			SetAllocationNo(int64(sequence)).
			SetAllocationKind("pay").
			SetEventKind(PoolFactEventKindPoolEvent).
			SetSequenceNum(int64(sequence)).
			SetState("confirmed").
			SetDirection("out").
			SetAmountSatoshi(int64(chargedAmount)).
			SetPurpose(chargeReason).
			SetNote("fee pool quote pay accounting").
			SetMsgID(updatedTxID).
			SetCycleIndex(int64(sequence)).
			SetPayeeAmountAfter(int64(session.ServerAmount)).
			SetPayerAmountAfter(int64(session.ClientAmount)).
			SetTxid(updatedTxID).
			SetTxHex(updatedTxHex).
			SetGatewayPubkeyHex(gatewayPubkeyHex).
			SetCreatedAtUnix(now).
			SetPayloadJSON(mustJSONString(payload)).
			Save(ctx)
		return err
	})
}

// dbRecordFeePoolCloseAccounting 把费用池 close 的事实写回共享事实层。
// 设计说明：
// - close 不是新的费用项，只是把现有会话状态收口为 closed；
// - 这里必须幂等，重复 close 不能新增脏行；
// - 允许 tx_hex 为空，但 final_spend_txid 必须有值。
func dbRecordFeePoolCloseAccounting(ctx context.Context, store *clientDB, sessionID string, finalTxID string, finalTxHex string, gatewayPeerID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("pool_session_id is required")
	}
	finalTxID = strings.ToLower(strings.TrimSpace(finalTxID))
	if finalTxID == "" {
		return fmt.Errorf("final_spend_txid is required")
	}
	finalTxHex = strings.ToLower(strings.TrimSpace(finalTxHex))
	gatewayPeerID = strings.ToLower(strings.TrimSpace(gatewayPeerID))
	now := time.Now().Unix()
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		existing, err := tx.FactSettlementChannelPoolSessionQuotePay.Query().
			Where(factsettlementchannelpoolsessionquotepay.PoolSessionIDEQ(sessionID)).
			Only(ctx)
		if err != nil {
			return err
		}
		status := strings.TrimSpace(existing.Status)
		if status == "" {
			status = "closed"
		}
		if gatewayPeerID == "" {
			gatewayPeerID = strings.ToLower(strings.TrimSpace(existing.GatewayPubkeyHex))
		}
		if gatewayPeerID == "" {
			gatewayPeerID = "unknown"
		}
		createdAt := existing.CreatedAtUnix
		if createdAt <= 0 {
			createdAt = now
		}
		if _, err := tx.FactSettlementChannelPoolSessionQuotePay.UpdateOneID(existing.ID).
			SetSettlementPaymentAttemptID(existing.SettlementPaymentAttemptID).
			SetTxid(finalTxID).
			SetPoolScheme(existing.PoolScheme).
			SetCounterpartyPubkeyHex(existing.CounterpartyPubkeyHex).
			SetSellerPubkeyHex(existing.SellerPubkeyHex).
			SetArbiterPubkeyHex(existing.ArbiterPubkeyHex).
			SetGatewayPubkeyHex(gatewayPeerID).
			SetPoolAmountSatoshi(existing.PoolAmountSatoshi).
			SetSpendTxFeeSatoshi(existing.SpendTxFeeSatoshi).
			SetFeeRateSatByte(existing.FeeRateSatByte).
			SetLockBlocks(existing.LockBlocks).
			SetOpenBaseTxid(existing.OpenBaseTxid).
			SetStatus("closed").
			SetCreatedAtUnix(createdAt).
			SetUpdatedAtUnix(now).
			Save(ctx); err != nil {
			return err
		}
		allocationID := "fee_pool_close:" + sessionID
		payload := map[string]any{
			"session_id":         sessionID,
			"gateway_pubkey_hex": gatewayPeerID,
			"final_spend_txid":   finalTxID,
			"status":             "closed",
		}
		event, err := tx.FactPoolSessionEvents.Query().
			Where(factpoolsessionevents.AllocationIDEQ(allocationID)).
			Only(ctx)
		if err == nil {
			_, err = tx.FactPoolSessionEvents.UpdateOneID(event.ID).
				SetPoolSessionID(sessionID).
				SetAllocationNo(1).
				SetAllocationKind("close").
				SetEventKind(PoolFactEventKindPoolEvent).
				SetSequenceNum(1).
				SetState("confirmed").
				SetDirection("out").
				SetAmountSatoshi(0).
				SetPurpose("fee_pool_close").
				SetNote("fee pool close accounting").
				SetMsgID(finalTxID).
				SetCycleIndex(0).
				SetPayeeAmountAfter(0).
				SetPayerAmountAfter(0).
				SetTxid(finalTxID).
				SetTxHex(finalTxHex).
				SetGatewayPubkeyHex(gatewayPeerID).
				SetCreatedAtUnix(now).
				SetPayloadJSON(mustJSONString(payload)).
				Save(ctx)
			return err
		}
		if err != nil && !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.FactPoolSessionEvents.Create().
			SetAllocationID(allocationID).
			SetPoolSessionID(sessionID).
			SetAllocationNo(1).
			SetAllocationKind("close").
			SetEventKind(PoolFactEventKindPoolEvent).
			SetSequenceNum(1).
			SetState("confirmed").
			SetDirection("out").
			SetAmountSatoshi(0).
			SetPurpose("fee_pool_close").
			SetNote("fee pool close accounting").
			SetMsgID(finalTxID).
			SetCycleIndex(0).
			SetPayeeAmountAfter(0).
			SetPayerAmountAfter(0).
			SetTxid(finalTxID).
			SetTxHex(finalTxHex).
			SetGatewayPubkeyHex(gatewayPeerID).
			SetCreatedAtUnix(now).
			SetPayloadJSON(mustJSONString(payload)).
			Save(ctx)
		return err
	})
}

// dbRecordDirectPoolOpenAccounting 【第二阶段：过程财务写入边界】
// 设计说明：
// - 这是 direct_transfer_pool open 阶段的过程财务写入
// - 第二阶段整改：open 不再是正式下载收费 business，改为过程型财务对象
// - 前台业务完成状态以 order_settlements（biz_download_pool_*）为准
// - 本函数记录：order_settlements(过程型) + order_settlement_events + 过程事实
// - ⚠️ 禁止用 process_id 充当 business_id，business_id 必须是稳定业务身份键
func dbRecordDirectPoolOpenAccounting(ctx context.Context, store *clientDB, in directPoolOpenAccountingInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		// 第二阶段整改：open 继续有自己的业务记录，但定性为过程型财务对象
		businessID := "biz_c2c_open_" + strings.TrimSpace(in.SessionID)
		baseTxID := strings.ToLower(strings.TrimSpace(in.BaseTxID))
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(in.SessionID), "open", 1)
		if _, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocID); err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
		}
		settlementPaymentAttemptID, err := resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx, store, allocID)
		if err != nil {
			return fmt.Errorf("resolve settlement payment attempt for pool allocation %s: %w", allocID, err)
		}
		lockScript := strings.TrimSpace(in.ClientLockScript)
		var grossInput, changeBack, lockAmount int64
		if t, err := transaction.NewTransactionFromHex(strings.TrimSpace(in.BaseTxHex)); err == nil {
			for _, input := range t.Inputs {
				if input.SourceTxOutput() == nil {
					continue
				}
				grossInput += int64(input.SourceTxOutput().Satoshis)
				if input.SourceTXID != nil {
					_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
				}
			}
			for idx, out := range t.Outputs {
				amount := int64(out.Satoshis)
				if idx == 0 {
					lockAmount += amount
					_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
					continue
				}
				if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
					changeBack += amount
					_ = dbAppendBusinessUTXOFactIfAbsent(tx, "open_base")
				}
			}
		}
		if lockAmount == 0 {
			lockAmount = int64(in.PoolAmountSatoshi)
		}
		minerFee := grossInput - changeBack - lockAmount
		if minerFee < 0 {
			minerFee = 0
		}
		// 第二阶段整改：open 继续写业务记录，但明确标记为过程型财务对象
		// 注意：这不是正式下载收费 business，正式收费主事实只认 biz_download_pool_*
		if err := dbAppendSettlementPaymentAttemptFinBusiness(ctx, tx, settlementPaymentAttemptID, finBusinessEntry{
			OrderID:           businessID,
			BusinessRole:      "process",                 // 过程财务对象
			AccountingScene:   "direct_transfer_process", // 过程型财务场景
			AccountingSubType: "pool_open_lock",          // 明确是过程动作，不是收费
			FromPartyID:       "client:self",
			ToPartyID:         "seller:" + strings.TrimSpace(in.SellerPubHex),
			Status:            "posted",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "c2c_open:" + strings.TrimSpace(in.SessionID),
			Note:              "direct transfer pool open lock (process fact)",
			Payload: map[string]any{
				"session_id":    strings.TrimSpace(in.SessionID),
				"deal_id":       strings.TrimSpace(in.DealID),
				"base_txid":     baseTxID,
				"allocation_id": allocID,
				"process_type":  "pool_open", // 标记为过程类型
			},
		}); err != nil {
			obs.Error("bitcast-client", "direct_pool_open_record_failed", map[string]any{"error": err.Error(), "scene": "c2c_open_process"})
			return err
		}
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, tx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(in.SessionID),
			AccountingScene:   "fee_pool",
			AccountingSubType: "open",
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "c2c_open_event:" + strings.TrimSpace(in.SessionID),
			Note:              "direct transfer pool open accounting event",
			Payload: map[string]any{
				"session_id":    strings.TrimSpace(in.SessionID),
				"deal_id":       strings.TrimSpace(in.DealID),
				"base_txid":     baseTxID,
				"allocation_id": allocID, // 保留业务键在 payload 中
			},
		}); err != nil {
			obs.Error("bitcast-client", "direct_pool_open_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_open"})
			return err
		}
		// 旧 tx 拆解层已下线，过程事实只保留 order_settlements 和流程事件。
		return nil
	})
}

// dbRecordDirectPoolPayAccounting 【第二阶段：pay 停止写 biz_c2c_pay_*】
// 设计说明：
// - 这是 direct_transfer_pool pay 阶段的过程财务写入
// - 第二阶段整改：pay 不再创建 biz_c2c_pay_*，彻底消除双主线问题
// - 正式下载收费主事实只认 biz_download_pool_*（由 triggerDirectTransferPoolOpen 创建）
// - pay 是正式收费的事实来源，但不再单独新建并列 business
// - pay 只保留：
//   - fin_process_event（过程审计追踪）
//   - order_settlements（业务主事实）
//   - 必要的 fact / wallet 事实
//
// - settlement 回写由 triggerDirectTransferPoolPay 负责更新 biz_download_pool_* 的 settlement
// - ⚠️ 任何代码不得将 biz_c2c_pay_* 作为正式业务读取入口
func dbRecordDirectPoolPayAccounting(ctx context.Context, store *clientDB, downloadBusinessID string, sessionID string, sequence uint32, amount uint64, relatedTxID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(sessionID), "pay", sequence)
		if _, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocID); err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
		}
		settlementPaymentAttemptID, err := resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx, store, allocID)
		if err != nil {
			return fmt.Errorf("resolve settlement payment attempt for pool allocation %s: %w", allocID, err)
		}

		// 过程事件：记录 pay 财务动作，供审计/对账/调试使用
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, tx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(sessionID),
			AccountingScene:   "c2c_transfer",
			AccountingSubType: "chunk_pay",
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "c2c_pay_event:" + strings.TrimSpace(sessionID) + ":" + fmt.Sprint(sequence),
			Note:              "direct transfer chunk pay accounting event",
			Payload: map[string]any{
				"sequence":      sequence,
				"allocation_id": allocID,            // 保留业务键在 payload 中
				"order_id":      downloadBusinessID, // 指向正式下载 order
			},
		}); err != nil {
			obs.Error("bitcast-client", "direct_pool_pay_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_pay"})
			return err
		}

		// 旧 tx 拆解层已下线，这里只保留流程事件。
		return nil
	})
}

// dbRecordDirectPoolCloseAccounting 【第二阶段：过程财务写入边界】
// 设计说明：
// - 这是 direct_transfer_pool close 阶段的过程财务写入
// - 第二阶段整改：close 不再是正式收费 business，改为过程型财务对象
// - 前台业务完成状态以 order_settlements（biz_download_pool_*）为准（在 pay 阶段已更新）
// - 本函数记录：order_settlements(过程型) + order_settlement_events + 过程事实
// - ⚠️ 禁止用 process_id 充当 business_id，business_id 必须是稳定业务身份键
func dbRecordDirectPoolCloseAccounting(ctx context.Context, store *clientDB, sessionID string, sequence uint32, finalTxID string, finalTxHex string, sellerAmount uint64, buyerAmount uint64, sellerPeerID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		finalTxID = strings.ToLower(strings.TrimSpace(finalTxID))
		txHex := strings.TrimSpace(finalTxHex)
		var parsedFinalTx *transaction.Transaction
		if txHex != "" {
			t, err := transaction.NewTransactionFromHex(txHex)
			if err != nil {
				obs.Error("bitcast-client", "direct_pool_close_parse_final_tx_failed", map[string]any{"error": err.Error(), "final_txid": finalTxID})
			} else {
				parsedFinalTx = t
				if finalTxID == "" {
					finalTxID = strings.ToLower(strings.TrimSpace(t.TxID().String()))
				}
			}
		}
		// 第二阶段整改：close 继续有自己的业务记录，但定性为过程型财务对象
		businessID := "biz_c2c_close_" + strings.TrimSpace(sessionID)
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(sessionID), "close", sequence)
		if _, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocID); err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
		}
		settlementPaymentAttemptID, err := resolvePoolAllocationSourceToSettlementPaymentAttempt(ctx, store, allocID)
		if err != nil {
			return fmt.Errorf("resolve settlement payment attempt for pool allocation %s: %w", allocID, err)
		}
		// 第二阶段整改：close 继续写业务记录，但明确标记为过程型财务对象
		// 注意：这不是正式下载收费 business，正式收费主事实只认 biz_download_pool_*
		if err := dbAppendSettlementPaymentAttemptFinBusiness(ctx, tx, settlementPaymentAttemptID, finBusinessEntry{
			OrderID:           businessID,
			BusinessRole:      "process",                 // 过程财务对象
			AccountingScene:   "direct_transfer_process", // 过程型财务场景
			AccountingSubType: "pool_close_settle",       // 明确是过程动作，不是收费
			FromPartyID:       "client:self",
			ToPartyID:         "seller:" + strings.TrimSpace(sellerPeerID),
			Status:            "posted",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "c2c_close:" + strings.TrimSpace(sessionID),
			Note:              "direct transfer settle close (process fact)",
			Payload: map[string]any{
				"seller_amount_satoshi": sellerAmount,
				"buyer_amount_satoshi":  buyerAmount,
				"allocation_id":         allocID,
				"process_type":          "pool_close", // 标记为过程类型
			},
		}); err != nil {
			obs.Error("bitcast-client", "direct_pool_close_record_failed", map[string]any{"error": err.Error(), "scene": "c2c_close_process"})
			return err
		}
		// 过程事件继续使用统一的过程追踪 id
		if err := dbAppendSettlementPaymentAttemptFinProcessEvent(ctx, tx, settlementPaymentAttemptID, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(sessionID),
			AccountingScene:   "c2c_transfer",
			AccountingSubType: "close",
			EventType:         "accounting",
			Status:            "applied",
			OccurredAtUnix:    time.Now().Unix(),
			IdempotencyKey:    "c2c_close_event:" + strings.TrimSpace(sessionID),
			Note:              "direct transfer settle close accounting event",
			Payload: map[string]any{
				"seller_amount_satoshi": sellerAmount,
				"buyer_amount_satoshi":  buyerAmount,
				"allocation_id":         allocID, // 保留业务键在 payload 中
			},
		}); err != nil {
			obs.Error("bitcast-client", "direct_pool_close_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_close"})
			return err
		}
		// 旧 tx 拆解层已下线，过程事实只保留 order_settlements 和流程事件。
		if parsedFinalTx == nil {
			return nil
		}
		for _, in := range parsedFinalTx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			if err := dbAppendBusinessUTXOFactIfAbsent(tx, "close_final"); err != nil {
				return err
			}
		}
		sellerLeft := sellerAmount
		buyerLeft := buyerAmount
		for _, out := range parsedFinalTx.Outputs {
			amount := out.Satoshis
			if amount == 0 {
				continue
			}
			if sellerLeft > 0 && amount == sellerLeft {
				sellerLeft = 0
			} else if buyerLeft > 0 && amount == buyerLeft {
				buyerLeft = 0
			}
			if err := dbAppendBusinessUTXOFactIfAbsent(tx, "close_final"); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbRecordAccounting(ctx context.Context, store *clientDB, fn func(*gen.Tx)) {
	if store == nil {
		return
	}
	_ = clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		fn(tx)
		return nil
	})
}
