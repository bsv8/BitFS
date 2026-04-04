package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv8/BFTP/pkg/obs"
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
	_ = store.Do(ctx, func(db *sql.DB) error {
		if strings.TrimSpace(e.GatewayPeerID) == "" {
			e.GatewayPeerID = "unknown"
		}
		if strings.TrimSpace(e.Direction) == "" {
			e.Direction = "info"
		}
		if strings.TrimSpace(e.Purpose) == "" {
			e.Purpose = e.EventType
		}
		_, err := db.Exec(
			`INSERT INTO tx_history(created_at_unix,gateway_pubkey_hex,event_type,direction,amount_satoshi,purpose,note,pool_id,msg_id,sequence_num,cycle_index) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			e.GatewayPeerID,
			e.EventType,
			e.Direction,
			e.AmountSatoshi,
			e.Purpose,
			e.Note,
			e.PoolID,
			e.MsgID,
			e.SequenceNum,
			e.CycleIndex,
		)
		if err != nil {
			obs.Error("bitcast-client", "tx_history_append_failed", map[string]any{"error": err.Error(), "event_type": e.EventType})
		}
		return nil
	})
}

func dbAppendPurchaseDone(ctx context.Context, store *clientDB, e purchaseDoneEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
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
		_, err := db.Exec(
			`INSERT INTO purchases(
				demand_id,seller_pub_hex,arbiter_pub_hex,chunk_index,object_hash,amount_satoshi,status,error_message,created_at_unix,finished_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			strings.TrimSpace(e.DemandID),
			strings.ToLower(strings.TrimSpace(e.SellerPubHex)),
			strings.ToLower(strings.TrimSpace(e.ArbiterPubHex)),
			e.ChunkIndex,
			strings.ToLower(strings.TrimSpace(e.ObjectHash)),
			e.AmountSatoshi,
			"done",
			"",
			createdAtUnix,
			finishedAtUnix,
		)
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

func dbAppendWalletFundFlow(ctx context.Context, store *clientDB, e walletFundFlowEntry) {
	if store == nil {
		return
	}
	_ = store.Do(ctx, func(db *sql.DB) error {
		e.VisitID = strings.TrimSpace(e.VisitID)
		e.VisitLocator = strings.TrimSpace(e.VisitLocator)
		e.FlowID = strings.TrimSpace(e.FlowID)
		if e.FlowID == "" {
			e.FlowID = "unknown"
		}
		e.FlowType = strings.TrimSpace(e.FlowType)
		if e.FlowType == "" {
			e.FlowType = "unknown"
		}
		e.RefID = strings.TrimSpace(e.RefID)
		e.Stage = strings.TrimSpace(e.Stage)
		if e.Stage == "" {
			e.Stage = "unknown"
		}
		e.Direction = strings.TrimSpace(e.Direction)
		if e.Direction == "" {
			e.Direction = "unknown"
		}
		e.Purpose = strings.TrimSpace(e.Purpose)
		if e.Purpose == "" {
			e.Purpose = "unknown"
		}
		_, err := db.Exec(
			`INSERT INTO wallet_fund_flows(
				created_at_unix,visit_id,visit_locator,flow_id,flow_type,ref_id,stage,direction,purpose,amount_satoshi,used_satoshi,returned_satoshi,related_txid,note,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			e.VisitID,
			e.VisitLocator,
			e.FlowID,
			e.FlowType,
			e.RefID,
			e.Stage,
			e.Direction,
			e.Purpose,
			e.AmountSatoshi,
			e.UsedSatoshi,
			e.ReturnedSatoshi,
			strings.TrimSpace(e.RelatedTxID),
			e.Note,
			mustJSONString(e.Payload),
		)
		if err != nil {
			obs.Error("bitcast-client", "wallet_fund_flow_append_failed", map[string]any{
				"error":   err.Error(),
				"flow_id": e.FlowID,
				"stage":   e.Stage,
			})
		}
		return nil
	})
}

func dbAppendWalletFundFlowFromContext(ctx context.Context, store *clientDB, e walletFundFlowEntry) {
	meta := requestVisitMetaFromContext(ctx)
	if strings.TrimSpace(e.VisitID) == "" {
		e.VisitID = meta.VisitID
	}
	if strings.TrimSpace(e.VisitLocator) == "" {
		e.VisitLocator = meta.VisitLocator
	}
	dbAppendWalletFundFlow(ctx, store, e)
}

func dbAppendWalletLedgerEntry(ctx context.Context, store *clientDB, e walletLedgerEntry) {
	if store == nil {
		return
	}
	_ = store.Do(ctx, func(db *sql.DB) error {
		e.TxID = strings.ToLower(strings.TrimSpace(e.TxID))
		if e.TxID == "" {
			e.TxID = "unknown"
		}
		e.Direction = strings.ToUpper(strings.TrimSpace(e.Direction))
		if e.Direction == "" {
			e.Direction = "UNKNOWN"
		}
		e.Category = strings.ToUpper(strings.TrimSpace(e.Category))
		if e.Category == "" {
			e.Category = "UNKNOWN"
		}
		e.Status = strings.ToUpper(strings.TrimSpace(e.Status))
		if e.Status == "" {
			e.Status = "UNKNOWN"
		}
		if e.OccurredAtUnix <= 0 {
			e.OccurredAtUnix = time.Now().Unix()
		}
		_, err := db.Exec(
			`INSERT INTO wallet_ledger_entries(
				created_at_unix,txid,direction,category,amount_satoshi,counterparty_label,status,block_height,occurred_at_unix,raw_ref_id,note,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			e.TxID,
			e.Direction,
			e.Category,
			e.AmountSatoshi,
			strings.TrimSpace(e.CounterpartyLabel),
			e.Status,
			e.BlockHeight,
			e.OccurredAtUnix,
			strings.TrimSpace(e.RawRefID),
			e.Note,
			mustJSONString(e.Payload),
		)
		if err != nil {
			obs.Error("bitcast-client", "wallet_ledger_entry_append_failed", map[string]any{
				"error":     err.Error(),
				"txid":      e.TxID,
				"direction": e.Direction,
				"category":  e.Category,
			})
		}
		return nil
	})
}

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
	err := store.Do(ctx, func(db *sql.DB) error {
		if strings.TrimSpace(e.GatewayPeerID) == "" {
			e.GatewayPeerID = "unknown"
		}
		if strings.TrimSpace(e.Action) == "" {
			e.Action = "unknown"
		}
		_, err := db.Exec(
			`INSERT INTO gateway_events(created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json) VALUES(?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			e.GatewayPeerID,
			commandID,
			e.Action,
			e.MsgID,
			e.SequenceNum,
			e.PoolID,
			e.AmountSatoshi,
			mustJSONString(e.Payload),
		)
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
	_ = store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO orchestrator_logs(
				created_at_unix,event_type,source,signal_type,aggregate_key,idempotency_key,command_type,gateway_pubkey_hex,task_status,retry_count,queue_length,error_message,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			strings.TrimSpace(e.EventType),
			strings.TrimSpace(e.Source),
			strings.TrimSpace(e.SignalType),
			strings.TrimSpace(e.AggregateKey),
			strings.TrimSpace(e.IdempotencyKey),
			strings.TrimSpace(e.CommandType),
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.TaskStatus),
			e.RetryCount,
			e.QueueLength,
			strings.TrimSpace(e.ErrorMessage),
			mustJSON(e.Payload),
		)
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
		obs.Error("bitcast-client", "command_journal_append_rejected", map[string]any{"error": err.Error(), "command_type": strings.TrimSpace(e.CommandType)})
		return err
	}
	return store.Do(ctx, func(db *sql.DB) error {
		accepted := 0
		if e.Accepted {
			accepted = 1
		}
		// trigger_key 是来源链路键，不是命令主键
		// - orchestrator 发起时，trigger_key = orchestrator.idempotency_key
		// - 非 orchestrator 发起时，trigger_key = ''
		_, err := db.Exec(
			`INSERT INTO command_journal(
				created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			commandID,
			strings.TrimSpace(e.CommandType),
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.AggregateID),
			strings.TrimSpace(e.RequestedBy),
			e.RequestedAt,
			accepted,
			strings.TrimSpace(e.Status),
			strings.TrimSpace(e.ErrorCode),
			strings.TrimSpace(e.ErrorMessage),
			strings.TrimSpace(e.StateBefore),
			strings.TrimSpace(e.StateAfter),
			e.DurationMS,
			strings.TrimSpace(e.TriggerKey),
			mustJSON(e.Payload),
			mustJSON(e.Result),
		)
		if err != nil {
			obs.Error("bitcast-client", "command_journal_append_failed", map[string]any{"error": err.Error(), "command_type": e.CommandType})
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO domain_events(created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json) VALUES(?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			commandID,
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.EventName),
			strings.TrimSpace(e.StateBefore),
			strings.TrimSpace(e.StateAfter),
			mustJSON(e.Payload),
		)
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO state_snapshots(
				created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			commandID,
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.State),
			strings.TrimSpace(e.PauseReason),
			e.PauseNeedSat,
			e.PauseHaveSat,
			strings.TrimSpace(e.LastError),
			mustJSON(e.Payload),
		)
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
	return store.Do(ctx, func(db *sql.DB) error {
		observedAtUnix := e.ObservedAtUnix
		if observedAtUnix <= 0 {
			observedAtUnix = time.Now().Unix()
		}
		_, err := db.Exec(
			`INSERT INTO observed_gateway_states(
				created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.SourceRef),
			observedAtUnix,
			strings.TrimSpace(e.EventName),
			strings.TrimSpace(e.StateBefore),
			strings.TrimSpace(e.StateAfter),
			strings.TrimSpace(e.PauseReason),
			e.PauseNeedSat,
			e.PauseHaveSat,
			strings.TrimSpace(e.LastError),
			mustJSON(e.Payload),
		)
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
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO effect_logs(created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json) VALUES(?,?,?,?,?,?,?,?)`,
			time.Now().Unix(),
			commandID,
			strings.TrimSpace(e.GatewayPeerID),
			strings.TrimSpace(e.EffectType),
			strings.TrimSpace(e.Stage),
			strings.TrimSpace(e.Status),
			strings.TrimSpace(e.ErrorMessage),
			mustJSON(e.Payload),
		)
		if err != nil {
			obs.Error("bitcast-client", "effect_log_append_failed", map[string]any{"error": err.Error(), "effect_type": e.EffectType, "stage": e.Stage})
		}
		return err
	})
}

func dbAppendChainTipWorkerLog(ctx context.Context, store *clientDB, e chainWorkerLogEntry) {
	dbAppendChainWorkerLog(ctx, store, "chain_tip_worker_logs", "chain_tip_worker_log_append_failed", e)
}

func dbAppendChainUTXOWorkerLog(ctx context.Context, store *clientDB, e chainWorkerLogEntry) {
	dbAppendChainWorkerLog(ctx, store, "chain_utxo_worker_logs", "chain_utxo_worker_log_append_failed", e)
}

func dbAppendChainWorkerLog(ctx context.Context, store *clientDB, table string, errorEvent string, e chainWorkerLogEntry) {
	if store == nil {
		return
	}
	_ = store.Do(ctx, func(db *sql.DB) error {
		if e.TriggeredAtUnix <= 0 {
			e.TriggeredAtUnix = time.Now().Unix()
		}
		if e.StartedAtUnix <= 0 {
			e.StartedAtUnix = e.TriggeredAtUnix
		}
		if e.EndedAtUnix <= 0 {
			e.EndedAtUnix = e.StartedAtUnix
		}
		stmt := fmt.Sprintf(
			`INSERT INTO %s(triggered_at_unix,started_at_unix,ended_at_unix,duration_ms,trigger_source,status,error_message,result_json)
			 VALUES(?,?,?,?,?,?,?,?)`,
			strings.TrimSpace(table),
		)
		if _, err := db.Exec(
			stmt,
			e.TriggeredAtUnix,
			e.StartedAtUnix,
			e.EndedAtUnix,
			e.DurationMS,
			strings.TrimSpace(e.TriggerSource),
			strings.TrimSpace(e.Status),
			strings.TrimSpace(e.ErrorMessage),
			mustJSON(e.Result),
		); err != nil {
			obs.Error("bitcast-client", errorEvent, map[string]any{"error": err.Error()})
			return nil
		}
		dbTrimWorkerLogs(db, table, chainWorkerLogKeepCount)
		return nil
	})
}

func dbTrimWorkerLogs(db *sql.DB, table string, keep int) {
	if db == nil || strings.TrimSpace(table) == "" || keep <= 0 {
		return
	}
	stmt := fmt.Sprintf(
		"DELETE FROM %s WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT ?)",
		table,
		table,
	)
	if _, err := db.Exec(stmt, keep); err != nil {
		obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
	}
}

func dbAppendFinBusiness(db sqlConn, e finBusinessEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	// 第七阶段：business_role 必须是正式约束，不允许空值
	e.BusinessRole = strings.TrimSpace(e.BusinessRole)
	if e.BusinessRole == "" {
		return fmt.Errorf("business_role is required: must be 'formal' or 'process'")
	}
	if e.BusinessRole != "formal" && e.BusinessRole != "process" {
		return fmt.Errorf("business_role must be 'formal' or 'process', got '%s'", e.BusinessRole)
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.BusinessID
	}
	_, err := db.Exec(
		`INSERT INTO fin_business(business_id,business_role,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(idempotency_key) DO UPDATE SET
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json,
			source_type=excluded.source_type,
			source_id=excluded.source_id,
			accounting_scene=excluded.accounting_scene,
			accounting_subtype=excluded.accounting_subtype,
			business_role=excluded.business_role`,
		e.BusinessID,
		strings.TrimSpace(e.BusinessRole),
		strings.TrimSpace(e.SourceType),
		strings.TrimSpace(e.SourceID),
		strings.TrimSpace(e.AccountingScene),
		strings.TrimSpace(e.AccountingSubType),
		strings.TrimSpace(e.FromPartyID),
		strings.TrimSpace(e.ToPartyID),
		strings.TrimSpace(e.Status),
		e.OccurredAtUnix,
		e.IdempotencyKey,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func dbAppendFinTxBreakdownIfAbsent(db sqlConn, e finTxBreakdownEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	e.TxRole = strings.TrimSpace(e.TxRole)
	if e.TxRole == "" {
		return fmt.Errorf("tx_role is required for fin_tx_breakdown")
	}
	var existingRole sql.NullString
	err := db.QueryRow(`SELECT tx_role FROM fin_tx_breakdown WHERE business_id=? AND txid=?`, strings.TrimSpace(e.BusinessID), strings.ToLower(strings.TrimSpace(e.TxID))).Scan(&existingRole)
	if err == nil {
		if existingRole.Valid && existingRole.String == e.TxRole {
			return nil
		}
		if !existingRole.Valid {
			return fmt.Errorf("fin_tx_breakdown tx_role is null for (%s,%s), final schema required", e.BusinessID, e.TxID)
		}
		return fmt.Errorf("fin_tx_breakdown role mismatch for (%s,%s): existing=%s, want=%s", e.BusinessID, e.TxID, existingRole.String, e.TxRole)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return dbAppendFinTxBreakdown(db, e)
}

func dbAppendFinTxUTXOLinkIfAbsent(db sqlConn, e finTxUTXOLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_tx_utxo_links WHERE business_id=? AND txid=? AND utxo_id=? AND io_side=? AND utxo_role=?`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		strings.ToLower(strings.TrimSpace(e.UTXOID)),
		strings.TrimSpace(e.IOSide),
		strings.TrimSpace(e.UTXORole),
	).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	return dbAppendFinTxUTXOLink(db, e)
}

func dbAppendFinTxBreakdown(db sqlConn, e finTxBreakdownEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	_, err := db.Exec(
		`INSERT INTO fin_tx_breakdown(
			business_id,txid,tx_role,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		strings.TrimSpace(e.TxRole),
		e.GrossInputSatoshi,
		e.ChangeBackSatoshi,
		e.ExternalInSatoshi,
		e.CounterpartyOutSat,
		e.MinerFeeSatoshi,
		e.NetOutSatoshi,
		e.NetInSatoshi,
		e.CreatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func dbAppendFinTxUTXOLink(db sqlConn, e finTxUTXOLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	_, err := db.Exec(
		`INSERT INTO fin_tx_utxo_links(business_id,txid,utxo_id,io_side,utxo_role,amount_satoshi,created_at_unix,note,payload_json) VALUES(?,?,?,?,?,?,?,?,?)`,
		strings.TrimSpace(e.BusinessID),
		strings.ToLower(strings.TrimSpace(e.TxID)),
		strings.ToLower(strings.TrimSpace(e.UTXOID)),
		strings.TrimSpace(e.IOSide),
		strings.TrimSpace(e.UTXORole),
		e.AmountSatoshi,
		e.CreatedAtUnix,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

func dbAppendBusinessUTXOFactIfAbsent(db sqlConn, txRole string, e finTxUTXOLinkEntry) error {
	txRole = strings.TrimSpace(txRole)
	if txRole == "" {
		return fmt.Errorf("tx_role is required for business utxo fact")
	}
	// 第二轮规则：UTXO 明细必须挂到一条已成立的 TX 财务事实上
	var existingRole sql.NullString
	err := db.QueryRow(`SELECT tx_role FROM fin_tx_breakdown WHERE business_id=? AND txid=?`, strings.TrimSpace(e.BusinessID), strings.ToLower(strings.TrimSpace(e.TxID))).Scan(&existingRole)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("fin_tx_breakdown missing for (%s,%s) before appending utxo link", e.BusinessID, e.TxID)
		}
		return err
	}
	if !existingRole.Valid {
		return fmt.Errorf("fin_tx_breakdown tx_role is null for (%s,%s), cannot append utxo link", e.BusinessID, e.TxID)
	}
	if existingRole.String != txRole {
		return fmt.Errorf("fin_tx_breakdown role mismatch for (%s,%s): existing=%s, want=%s", e.BusinessID, e.TxID, existingRole.String, txRole)
	}
	// 第二轮只写新表
	return dbAppendFinTxUTXOLinkIfAbsent(db, e)
}

func dbAppendFinProcessEvent(db sqlConn, e finProcessEventEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	e.ProcessID = strings.TrimSpace(e.ProcessID)
	if e.ProcessID == "" {
		return fmt.Errorf("process_id is required")
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.ProcessID + ":" + strings.TrimSpace(e.EventType)
	}
	_, err := db.Exec(
		`INSERT INTO fin_process_events(process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(idempotency_key) DO UPDATE SET
			status=excluded.status,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json,
			source_type=excluded.source_type,
			source_id=excluded.source_id,
			accounting_scene=excluded.accounting_scene,
			accounting_subtype=excluded.accounting_subtype`,
		e.ProcessID,
		strings.TrimSpace(e.SourceType),
		strings.TrimSpace(e.SourceID),
		strings.TrimSpace(e.AccountingScene),
		strings.TrimSpace(e.AccountingSubType),
		strings.TrimSpace(e.EventType),
		strings.TrimSpace(e.Status),
		e.OccurredAtUnix,
		e.IdempotencyKey,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// 直连池财务解释统一挂到 allocation，别再用 session 漂着。
func directTransferPoolAccountingSource(sessionID string, allocationKind string, sequenceNum uint32) (string, string) {
	return "pool_allocation", directTransferPoolAllocationID(sessionID, allocationKind, sequenceNum)
}

func dbRecordFeePoolOpenAccounting(ctx context.Context, store *clientDB, in feePoolOpenAccountingInput) {
	dbRecordAccounting(ctx, store, func(db *sql.DB) {
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
				obs.Error("bitcast-client", "wallet_accounting_parse_base_tx_failed", map[string]any{"error": err.Error(), "base_txid": baseTxID})
			} else {
				for _, input := range t.Inputs {
					if input.SourceTxOutput() != nil {
						grossInput += int64(input.SourceTxOutput().Satoshis)
						if input.SourceTXID != nil {
							utxoID := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
							_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
								BusinessID:    businessID,
								TxID:          baseTxID,
								UTXOID:        utxoID,
								IOSide:        "input",
								UTXORole:      "wallet_input",
								AmountSatoshi: int64(input.SourceTxOutput().Satoshis),
								Note:          "fee pool open input",
							})
						}
					}
				}
				for idx, out := range t.Outputs {
					amount := int64(out.Satoshis)
					if idx == 0 {
						lockAmount += amount
						_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
							BusinessID:    businessID,
							TxID:          baseTxID,
							UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
							IOSide:        "output",
							UTXORole:      "pool_lock",
							AmountSatoshi: amount,
							Note:          "fee pool lock output",
						})
						continue
					}
					if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
						changeBack += amount
						_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
							BusinessID:    businessID,
							TxID:          baseTxID,
							UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
							IOSide:        "output",
							UTXORole:      "wallet_change",
							AmountSatoshi: amount,
							Note:          "wallet change output",
						})
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
		// 过渡态标记：fee_pool 的 source_type 当前是抽象业务名，不是最终事实实体
		// 说明：现在用 "fee_pool" 只是过渡写法，等 fee_pool 事实层明确后要改成指向真实事实记录
		// 原则：accounting_* 负责业务分类，source_* 最终只负责事实来源
		if err := dbAppendFinBusiness(db, finBusinessEntry{
			BusinessID:        businessID,
			BusinessRole:      "process", // 过程财务对象
			SourceType:        "fee_pool",
			SourceID:          strings.TrimSpace(in.SpendTxID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
			return
		}
		if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
			BusinessID:         businessID,
			TxID:               baseTxID,
			TxRole:             "open_base",
			GrossInputSatoshi:  grossInput,
			ChangeBackSatoshi:  changeBack,
			ExternalInSatoshi:  0,
			CounterpartyOutSat: lockAmount,
			MinerFeeSatoshi:    minerFee,
			NetOutSatoshi:      lockAmount + minerFee,
			NetInSatoshi:       0,
			Note:               "open lock gross_input-change_back",
			Payload: map[string]any{
				"formula": "net_out = counterparty_out + miner_fee",
			},
		}); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_open"})
		}
	})
}

func dbRecordFeePoolCycleEvent(ctx context.Context, store *clientDB, spendTxID string, sequence uint32, amount uint64, gatewayPeerID string) {
	dbRecordAccounting(ctx, store, func(db *sql.DB) {
		processID := "proc_feepool_cycle_" + strings.TrimSpace(spendTxID)
		// 过渡态标记：fee_pool 的 source_type 当前是抽象业务名，不是最终事实实体
		// 说明：现在用 "fee_pool" 只是过渡写法，等 fee_pool 事实层明确后要改成指向真实事实记录
		// 原则：accounting_* 负责业务分类，source_* 最终只负责事实来源
		if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
			ProcessID:         processID,
			SourceType:        "fee_pool",
			SourceID:          strings.TrimSpace(spendTxID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "fee_pool_cycle"})
		}
	})
}

// dbRecordDirectPoolOpenAccounting 【第二阶段：过程财务写入边界】
// 设计说明：
// - 这是 direct_transfer_pool open 阶段的过程财务写入
// - 第二阶段整改：open 不再是正式下载收费 business，改为过程型财务对象
// - 前台业务完成状态以 business_settlements（biz_download_pool_*）为准
// - 本函数记录：fin_business(过程型) + fin_process_event + fin_tx_breakdown + utxo_fact
// - ⚠️ 禁止用 process_id 充当 business_id，business_id 必须是稳定业务身份键
func dbRecordDirectPoolOpenAccounting(ctx context.Context, store *clientDB, in directPoolOpenAccountingInput) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		// 第二阶段整改：open 继续有自己的 fin_business，但定性为过程型财务对象
		businessID := "biz_c2c_open_" + strings.TrimSpace(in.SessionID)
		sourceType := "pool_allocation"
		baseTxID := strings.ToLower(strings.TrimSpace(in.BaseTxID))
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(in.SessionID), "open", 1)
		sourceID, err := dbGetPoolAllocationIDByAllocationIDDB(db, allocID)
		if err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
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
					utxoID := strings.ToLower(strings.TrimSpace(input.SourceTXID.String())) + ":" + fmt.Sprint(input.SourceTxOutIndex)
					_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
						BusinessID:    businessID, // 第二阶段：utxo 挂到过程型 business
						TxID:          baseTxID,
						UTXOID:        utxoID,
						IOSide:        "input",
						UTXORole:      "wallet_input",
						AmountSatoshi: int64(input.SourceTxOutput().Satoshis),
						Note:          "direct pool open input",
					})
				}
			}
			for idx, out := range t.Outputs {
				amount := int64(out.Satoshis)
				if idx == 0 {
					lockAmount += amount
					_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
						BusinessID:    businessID,
						TxID:          baseTxID,
						UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
						IOSide:        "output",
						UTXORole:      "pool_lock",
						AmountSatoshi: amount,
						Note:          "direct pool lock output",
					})
					continue
				}
				if lockScript != "" && strings.EqualFold(strings.TrimSpace(out.LockingScript.String()), lockScript) {
					changeBack += amount
					_ = dbAppendBusinessUTXOFactIfAbsent(db, "open_base", finTxUTXOLinkEntry{
						BusinessID:    businessID,
						TxID:          baseTxID,
						UTXOID:        baseTxID + ":" + fmt.Sprint(idx),
						IOSide:        "output",
						UTXORole:      "wallet_change",
						AmountSatoshi: amount,
						Note:          "wallet change output",
					})
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
		// 第二阶段整改：open 继续写 fin_business，但明确标记为过程型财务对象
		// 注意：这不是正式下载收费 business，正式收费主事实只认 biz_download_pool_*
		if err := dbAppendFinBusiness(db, finBusinessEntry{
			BusinessID:        businessID,
			BusinessRole:      "process", // 过程财务对象
			SourceType:        sourceType,
			SourceID:          fmt.Sprintf("%d", sourceID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "c2c_open_process"})
			return err
		}
		if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(in.SessionID),
			SourceType:        sourceType,
			SourceID:          fmt.Sprintf("%d", sourceID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_open"})
			return err
		}
		// 第二阶段：tx_breakdown 挂正式的 business_id
		if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
			BusinessID:         businessID,
			TxID:               baseTxID,
			TxRole:             "open_base",
			GrossInputSatoshi:  grossInput,
			ChangeBackSatoshi:  changeBack,
			ExternalInSatoshi:  0,
			CounterpartyOutSat: lockAmount,
			MinerFeeSatoshi:    minerFee,
			NetOutSatoshi:      lockAmount + minerFee,
			NetInSatoshi:       0,
			Note:               "direct open lock gross_input-change_back",
			Payload:            map[string]any{"session_id": strings.TrimSpace(in.SessionID)},
		}); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "c2c_open"})
			return err
		}
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
//   - fin_tx_breakdown（交易拆解，挂到 biz_download_pool_*）
//   - 必要的 utxo fact
//
// - settlement 回写由 triggerDirectTransferPoolPay 负责更新 biz_download_pool_* 的 settlement
// - ⚠️ 任何代码不得将 biz_c2c_pay_* 作为正式业务读取入口
func dbRecordDirectPoolPayAccounting(ctx context.Context, store *clientDB, downloadBusinessID string, sessionID string, sequence uint32, amount uint64, relatedTxID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		sourceType := "pool_allocation"
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(sessionID), "pay", sequence)
		sourceID, err := dbGetPoolAllocationIDByAllocationIDDB(db, allocID)
		if err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
		}

		// 过程事件：记录 pay 财务动作，供审计/对账/调试使用
		if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(sessionID),
			SourceType:        sourceType,
			SourceID:          fmt.Sprintf("%d", sourceID),
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
				"business_id":   downloadBusinessID, // 指向正式下载 business
			},
		}); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_pay"})
			return err
		}

		// 交易拆解：挂到正式下载 business，不再挂过程 business
		if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
			BusinessID:         downloadBusinessID,
			TxID:               strings.TrimSpace(relatedTxID),
			TxRole:             "pay",
			GrossInputSatoshi:  0,
			ChangeBackSatoshi:  0,
			ExternalInSatoshi:  0,
			CounterpartyOutSat: int64(amount),
			MinerFeeSatoshi:    0,
			NetOutSatoshi:      int64(amount),
			NetInSatoshi:       0,
			Note:               "offchain chunk pay",
			Payload:            map[string]any{"sequence": sequence},
		}); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "c2c_pay"})
			return err
		}
		return nil
	})
}

// dbRecordDirectPoolCloseAccounting 【第二阶段：过程财务写入边界】
// 设计说明：
// - 这是 direct_transfer_pool close 阶段的过程财务写入
// - 第二阶段整改：close 不再是正式收费 business，改为过程型财务对象
// - 前台业务完成状态以 business_settlements（biz_download_pool_*）为准（在 pay 阶段已更新）
// - 本函数记录：fin_business(过程型) + fin_process_event + fin_tx_breakdown + utxo_fact
// - ⚠️ 禁止用 process_id 充当 business_id，business_id 必须是稳定业务身份键
func dbRecordDirectPoolCloseAccounting(ctx context.Context, store *clientDB, sessionID string, sequence uint32, finalTxID string, finalTxHex string, sellerAmount uint64, buyerAmount uint64, sellerPeerID string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		finalTxID = strings.ToLower(strings.TrimSpace(finalTxID))
		txHex := strings.TrimSpace(finalTxHex)
		var parsedFinalTx *transaction.Transaction
		if txHex != "" {
			t, err := transaction.NewTransactionFromHex(txHex)
			if err != nil {
				obs.Error("bitcast-client", "wallet_accounting_parse_final_tx_failed", map[string]any{"error": err.Error(), "final_txid": finalTxID})
			} else {
				parsedFinalTx = t
				if finalTxID == "" {
					finalTxID = strings.ToLower(strings.TrimSpace(t.TxID().String()))
				}
			}
		}
		// 第二阶段整改：close 继续有自己的 fin_business，但定性为过程型财务对象
		businessID := "biz_c2c_close_" + strings.TrimSpace(sessionID)
		sourceType := "pool_allocation"
		_, allocID := directTransferPoolAccountingSource(strings.TrimSpace(sessionID), "close", sequence)
		sourceID, err := dbGetPoolAllocationIDByAllocationIDDB(db, allocID)
		if err != nil {
			return fmt.Errorf("resolve pool_allocation source id failed: %w", err)
		}
		// 第二阶段整改：close 继续写 fin_business，但明确标记为过程型财务对象
		// 注意：这不是正式下载收费 business，正式收费主事实只认 biz_download_pool_*
		if err := dbAppendFinBusiness(db, finBusinessEntry{
			BusinessID:        businessID,
			BusinessRole:      "process", // 过程财务对象
			SourceType:        sourceType,
			SourceID:          fmt.Sprintf("%d", sourceID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_business_failed", map[string]any{"error": err.Error(), "scene": "c2c_close_process"})
			return err
		}
		// 过程事件继续使用统一的过程追踪 id
		if err := dbAppendFinProcessEvent(db, finProcessEventEntry{
			ProcessID:         "proc_c2c_transfer_" + strings.TrimSpace(sessionID),
			SourceType:        sourceType,
			SourceID:          fmt.Sprintf("%d", sourceID),
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
			obs.Error("bitcast-client", "wallet_accounting_fin_process_event_failed", map[string]any{"error": err.Error(), "scene": "c2c_close"})
			return err
		}
		if err := dbAppendFinTxBreakdownIfAbsent(db, finTxBreakdownEntry{
			// 第二阶段：tx_breakdown 挂正式的 business_id
			BusinessID:         businessID,
			TxID:               finalTxID,
			TxRole:             "close_final",
			GrossInputSatoshi:  0,
			ChangeBackSatoshi:  int64(buyerAmount),
			ExternalInSatoshi:  0,
			CounterpartyOutSat: 0,
			MinerFeeSatoshi:    0,
			NetOutSatoshi:      0,
			NetInSatoshi:       0,
			Note:               "pool settle return",
			Payload:            map[string]any{"session_id": strings.TrimSpace(sessionID)},
		}); err != nil {
			obs.Error("bitcast-client", "wallet_accounting_fin_breakdown_failed", map[string]any{"error": err.Error(), "scene": "c2c_close"})
			return err
		}
		if parsedFinalTx == nil {
			return nil
		}
		inputValueHint := int64(sellerAmount + buyerAmount)
		for i, in := range parsedFinalTx.Inputs {
			if in.SourceTXID == nil {
				continue
			}
			value := int64(0)
			if i == 0 {
				value = inputValueHint
			}
			utxoID := strings.ToLower(strings.TrimSpace(in.SourceTXID.String())) + ":" + fmt.Sprint(in.SourceTxOutIndex)
			// 第二阶段：utxo 挂正式的 business_id
			if err := dbAppendBusinessUTXOFactIfAbsent(db, "close_final", finTxUTXOLinkEntry{
				BusinessID:    businessID,
				TxID:          finalTxID,
				UTXOID:        utxoID,
				IOSide:        "input",
				UTXORole:      "pool_input",
				AmountSatoshi: value,
				Note:          "direct pool settle input",
			}); err != nil {
				return err
			}
		}
		sellerLeft := sellerAmount
		buyerLeft := buyerAmount
		for idx, out := range parsedFinalTx.Outputs {
			amount := out.Satoshis
			if amount == 0 {
				continue
			}
			utxoRole := "settle_other"
			note := "direct pool settle output"
			if sellerLeft > 0 && amount == sellerLeft {
				utxoRole = "settle_to_seller"
				note = "direct pool settle output to seller"
				sellerLeft = 0
			} else if buyerLeft > 0 && amount == buyerLeft {
				utxoRole = "settle_to_buyer"
				note = "direct pool settle output to buyer"
				buyerLeft = 0
			}
			if err := dbAppendBusinessUTXOFactIfAbsent(db, "close_final", finTxUTXOLinkEntry{
				// 第二阶段：utxo 挂正式的 business_id
				BusinessID:    businessID,
				TxID:          finalTxID,
				UTXOID:        finalTxID + ":" + fmt.Sprint(idx),
				IOSide:        "output",
				UTXORole:      utxoRole,
				AmountSatoshi: int64(amount),
				Note:          note,
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbRecordAccounting(ctx context.Context, store *clientDB, fn func(*sql.DB)) {
	if store == nil {
		return
	}
	_ = store.Do(ctx, func(db *sql.DB) error {
		fn(db)
		return nil
	})
}
