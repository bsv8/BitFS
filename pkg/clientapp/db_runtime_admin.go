package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const singleStepOrchestratorEventPrefix = "__single__:"

type orchestratorLogFilter struct {
	Limit          int
	Offset         int
	EventType      string
	SignalType     string
	Source         string
	GatewayPeerID  string
	IdempotencyKey string
	TaskStatus     string
}

type orchestratorLogPage struct {
	Total int
	Items []orchestratorLogItem
}

type orchestratorLogItem struct {
	EventID          string `json:"event_id"`
	StartedAtUnix    int64  `json:"started_at_unix"`
	EndedAtUnix      int64  `json:"ended_at_unix"`
	StepsCount       int    `json:"steps_count"`
	LatestLogID      int64  `json:"latest_log_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	AggregateKey     string `json:"aggregate_key"`
	CommandType      string `json:"command_type"`
	GatewayPeerID    string `json:"gateway_pubkey_hex"`
	Source           string `json:"source"`
	SignalType       string `json:"signal_type"`
	LatestEventType  string `json:"latest_event_type"`
	LatestTaskStatus string `json:"latest_task_status"`
	LatestRetryCount int    `json:"latest_retry_count"`
	LatestQueueLen   int    `json:"latest_queue_length"`
	LastErrorMessage string `json:"last_error_message"`
}

type orchestratorLogStep struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	EventType      string          `json:"event_type"`
	Source         string          `json:"source"`
	SignalType     string          `json:"signal_type"`
	AggregateKey   string          `json:"aggregate_key"`
	IdempotencyKey string          `json:"idempotency_key"`
	CommandType    string          `json:"command_type"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	TaskStatus     string          `json:"task_status"`
	RetryCount     int             `json:"retry_count"`
	QueueLength    int             `json:"queue_length"`
	ErrorMessage   string          `json:"error_message"`
	Payload        json.RawMessage `json:"payload"`
}

type orchestratorLogDetail struct {
	EventID          string                `json:"event_id"`
	IdempotencyKey   string                `json:"idempotency_key"`
	AggregateKey     string                `json:"aggregate_key"`
	CommandType      string                `json:"command_type"`
	GatewayPeerID    string                `json:"gateway_pubkey_hex"`
	StartedAtUnix    int64                 `json:"started_at_unix"`
	EndedAtUnix      int64                 `json:"ended_at_unix"`
	StepsCount       int                   `json:"steps_count"`
	LatestEventType  string                `json:"latest_event_type"`
	LatestTaskStatus string                `json:"latest_task_status"`
	LastErrorMessage string                `json:"last_error_message"`
	Steps            []orchestratorLogStep `json:"steps"`
}

type schedulerTaskFilter struct {
	Mode        string
	Owner       string
	NamePrefix  string
	Status      string
	InFlightSet bool
	InFlight    bool
	HasErrorSet bool
	HasError    bool
	OrderBy     string
}

type schedulerTaskPage struct {
	EnabledTaskCount int
	InFlightCount    int
	FailureTotal     uint64
	ActiveCount      int
	StoppedCount     int
	FilterCount      int
	Items            []schedulerTaskItem
}

type schedulerTaskItem struct {
	Name              string `json:"name"`
	Owner             string `json:"owner"`
	Mode              string `json:"mode"`
	Status            string `json:"status"`
	IntervalSeconds   int64  `json:"interval_seconds"`
	CreatedAtUnix     int64  `json:"created_at_unix"`
	UpdatedAtUnix     int64  `json:"updated_at_unix"`
	ClosedAtUnix      int64  `json:"closed_at_unix"`
	LastTrigger       string `json:"last_trigger"`
	LastStartedAtUnix int64  `json:"last_started_at_unix"`
	LastEndedAtUnix   int64  `json:"last_ended_at_unix"`
	LastDurationMS    int64  `json:"last_duration_ms"`
	LastError         string `json:"last_error"`
	InFlight          bool   `json:"in_flight"`
	RunCount          uint64 `json:"run_count"`
	SuccessCount      uint64 `json:"success_count"`
	FailureCount      uint64 `json:"failure_count"`
	LastSummary       any    `json:"last_summary"`
}

type schedulerRunFilter struct {
	Limit    int
	Offset   int
	TaskName string
	Owner    string
	Mode     string
	Status   string
}

type schedulerRunPage struct {
	Total int
	Items []schedulerRunItem
}

type schedulerRunItem struct {
	ID            int64  `json:"id"`
	TaskName      string `json:"task_name"`
	Owner         string `json:"owner"`
	Mode          string `json:"mode"`
	Trigger       string `json:"trigger"`
	StartedAtUnix int64  `json:"started_at_unix"`
	EndedAtUnix   int64  `json:"ended_at_unix"`
	DurationMS    int64  `json:"duration_ms"`
	Status        string `json:"status"`
	ErrorMessage  string `json:"error_message"`
	Summary       any    `json:"summary"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type chainWorkerLogFilter struct {
	Limit  int
	Offset int
	Status string
}

type chainWorkerLogPage struct {
	Total int
	Items []chainWorkerLogItem
}

type chainWorkerLogItem struct {
	ID              int64           `json:"id"`
	TriggeredAtUnix int64           `json:"triggered_at_unix"`
	StartedAtUnix   int64           `json:"started_at_unix"`
	EndedAtUnix     int64           `json:"ended_at_unix"`
	DurationMS      int64           `json:"duration_ms"`
	TriggerSource   string          `json:"trigger_source"`
	Status          string          `json:"status"`
	ErrorMessage    string          `json:"error_message"`
	Result          json.RawMessage `json:"result"`
}

type walletUTXOFilter struct {
	Limit    int
	Offset   int
	WalletID string
	Address  string
	State    string
	TxID     string
	Query    string
}

type walletUTXOPage struct {
	Total int
	Items []walletUTXOItem
}

type walletUTXOItem struct {
	UTXOID           string `json:"utxo_id"`
	WalletID         string `json:"wallet_id"`
	Address          string `json:"address"`
	TxID             string `json:"txid"`
	Vout             uint32 `json:"vout"`
	ValueSatoshi     uint64 `json:"value_satoshi"`
	State            string `json:"state"`
	AllocationClass  string `json:"allocation_class"`
	AllocationReason string `json:"allocation_reason"`
	CreatedTxID      string `json:"created_txid"`
	SpentTxID        string `json:"spent_txid"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
	UpdatedAtUnix    int64  `json:"updated_at_unix"`
	SpentAtUnix      int64  `json:"spent_at_unix"`
	Assets           any    `json:"assets,omitempty"`
}

type walletUTXOEventFilter struct {
	Limit         int
	Offset        int
	UTXOID        string
	EventType     string
	RefTxID       string
	RefBusinessID string
	Query         string
}

type walletUTXOEventPage struct {
	Total int
	Items []walletUTXOEventItem
}

type walletUTXOEventItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	UTXOID        string          `json:"utxo_id"`
	EventType     string          `json:"event_type"`
	RefTxID       string          `json:"ref_txid"`
	RefBusinessID string          `json:"ref_business_id"`
	Note          string          `json:"note"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListOrchestratorLogs(ctx context.Context, store *clientDB, f orchestratorLogFilter) (orchestratorLogPage, error) {
	if store == nil {
		return orchestratorLogPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (orchestratorLogPage, error) {
		where := ""
		args := make([]any, 0, 8)
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.SignalType != "" {
			where += " AND signal_type=?"
			args = append(args, f.SignalType)
		}
		if f.Source != "" {
			where += " AND source=?"
			args = append(args, f.Source)
		}
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.IdempotencyKey != "" {
			where += " AND idempotency_key=?"
			args = append(args, f.IdempotencyKey)
		}
		if f.TaskStatus != "" {
			where += " AND task_status=?"
			args = append(args, f.TaskStatus)
		}
		eventIDExpr := "CASE WHEN TRIM(idempotency_key)<>'' THEN idempotency_key ELSE '" + singleStepOrchestratorEventPrefix + "' || CAST(id AS TEXT) END"
		countSQL := "SELECT COUNT(1) FROM (SELECT " + eventIDExpr + " AS event_id FROM orchestrator_logs WHERE 1=1" + where + " GROUP BY 1)"
		var out orchestratorLogPage
		if err := db.QueryRow(countSQL, args...).Scan(&out.Total); err != nil {
			return orchestratorLogPage{}, err
		}
		rows, err := db.Query(`WITH grouped AS (
			SELECT
				`+eventIDExpr+` AS event_id,
				MIN(created_at_unix) AS started_at_unix,
				MAX(created_at_unix) AS ended_at_unix,
				COUNT(1) AS steps_count,
				MAX(id) AS latest_log_id
			FROM orchestrator_logs
			WHERE 1=1`+where+`
			GROUP BY 1
		)
		SELECT
			g.event_id,g.started_at_unix,g.ended_at_unix,g.steps_count,g.latest_log_id,
			l.idempotency_key,l.aggregate_key,l.command_type,l.gateway_pubkey_hex,l.source,l.signal_type,l.event_type,l.task_status,l.retry_count,l.queue_length,l.error_message
		FROM grouped g
		JOIN orchestrator_logs l ON l.id=g.latest_log_id
		ORDER BY g.latest_log_id DESC
		LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return orchestratorLogPage{}, err
		}
		defer rows.Close()
		out.Items = make([]orchestratorLogItem, 0, f.Limit)
		for rows.Next() {
			var it orchestratorLogItem
			if err := rows.Scan(
				&it.EventID, &it.StartedAtUnix, &it.EndedAtUnix, &it.StepsCount, &it.LatestLogID, &it.IdempotencyKey, &it.AggregateKey,
				&it.CommandType, &it.GatewayPeerID, &it.Source, &it.SignalType, &it.LatestEventType, &it.LatestTaskStatus,
				&it.LatestRetryCount, &it.LatestQueueLen, &it.LastErrorMessage,
			); err != nil {
				return orchestratorLogPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return orchestratorLogPage{}, err
		}
		return out, nil
	})
}

func dbGetOrchestratorLogDetail(ctx context.Context, store *clientDB, eventID string) (orchestratorLogDetail, error) {
	if store == nil {
		return orchestratorLogDetail{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (orchestratorLogDetail, error) {
		var (
			rows *sql.Rows
			err  error
		)
		if strings.HasPrefix(eventID, singleStepOrchestratorEventPrefix) {
			rawID := strings.TrimPrefix(eventID, singleStepOrchestratorEventPrefix)
			id, perr := strconv.ParseInt(rawID, 10, 64)
			if perr != nil || id <= 0 {
				return orchestratorLogDetail{}, fmt.Errorf("invalid event_id")
			}
			rows, err = db.Query(
				`SELECT id,created_at_unix,event_type,source,signal_type,aggregate_key,idempotency_key,command_type,gateway_pubkey_hex,task_status,retry_count,queue_length,error_message,payload_json
				FROM orchestrator_logs WHERE id=? ORDER BY id ASC`, id,
			)
		} else {
			rows, err = db.Query(
				`SELECT id,created_at_unix,event_type,source,signal_type,aggregate_key,idempotency_key,command_type,gateway_pubkey_hex,task_status,retry_count,queue_length,error_message,payload_json
				FROM orchestrator_logs WHERE idempotency_key=? ORDER BY id ASC`, eventID,
			)
		}
		if err != nil {
			return orchestratorLogDetail{}, err
		}
		defer rows.Close()
		steps := make([]orchestratorLogStep, 0, 8)
		for rows.Next() {
			var it orchestratorLogStep
			var payload string
			if err := rows.Scan(
				&it.ID, &it.CreatedAtUnix, &it.EventType, &it.Source, &it.SignalType, &it.AggregateKey, &it.IdempotencyKey,
				&it.CommandType, &it.GatewayPeerID, &it.TaskStatus, &it.RetryCount, &it.QueueLength, &it.ErrorMessage, &payload,
			); err != nil {
				return orchestratorLogDetail{}, err
			}
			it.Payload = json.RawMessage(payload)
			steps = append(steps, it)
		}
		if err := rows.Err(); err != nil {
			return orchestratorLogDetail{}, err
		}
		if len(steps) == 0 {
			return orchestratorLogDetail{}, sql.ErrNoRows
		}
		first := steps[0]
		last := steps[len(steps)-1]
		return orchestratorLogDetail{
			EventID:          eventID,
			IdempotencyKey:   strings.TrimSpace(last.IdempotencyKey),
			AggregateKey:     strings.TrimSpace(last.AggregateKey),
			CommandType:      strings.TrimSpace(last.CommandType),
			GatewayPeerID:    strings.TrimSpace(last.GatewayPeerID),
			StartedAtUnix:    first.CreatedAtUnix,
			EndedAtUnix:      last.CreatedAtUnix,
			StepsCount:       len(steps),
			LatestEventType:  strings.TrimSpace(last.EventType),
			LatestTaskStatus: strings.TrimSpace(last.TaskStatus),
			LastErrorMessage: strings.TrimSpace(last.ErrorMessage),
			Steps:            steps,
		}, nil
	})
}

func dbListSchedulerTasks(ctx context.Context, store *clientDB, f schedulerTaskFilter) (schedulerTaskPage, error) {
	if store == nil {
		return schedulerTaskPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (schedulerTaskPage, error) {
		where := make([]string, 0, 8)
		args := make([]any, 0, 8)
		if f.Mode != "" {
			where = append(where, "mode=?")
			args = append(args, f.Mode)
		}
		if f.Owner != "" {
			where = append(where, "owner=?")
			args = append(args, f.Owner)
		}
		if f.Status != "" {
			where = append(where, "status=?")
			args = append(args, f.Status)
		}
		if f.NamePrefix != "" {
			where = append(where, "task_name LIKE ?")
			args = append(args, f.NamePrefix+"%")
		}
		if f.InFlightSet {
			where = append(where, "in_flight=?")
			if f.InFlight {
				args = append(args, 1)
			} else {
				args = append(args, 0)
			}
		}
		if f.HasErrorSet {
			if f.HasError {
				where = append(where, "length(trim(last_error))>0")
			} else {
				where = append(where, "length(trim(last_error))=0")
			}
		}
		whereSQL := ""
		if len(where) > 0 {
			whereSQL = " WHERE " + strings.Join(where, " AND ")
		}
		orderClause := "CASE WHEN status='active' THEN 0 ELSE 1 END ASC, CASE WHEN status='active' THEN last_started_at_unix ELSE COALESCE(NULLIF(closed_at_unix,0),updated_at_unix) END DESC, task_name ASC"
		switch f.OrderBy {
		case "", "default":
		case "name_asc":
			orderClause = "task_name ASC"
		case "updated_desc":
			orderClause = "updated_at_unix DESC, task_name ASC"
		default:
			return schedulerTaskPage{}, fmt.Errorf("invalid order_by")
		}
		var out schedulerTaskPage
		if err := db.QueryRow(`SELECT COUNT(1) FROM scheduler_tasks`).Scan(&out.EnabledTaskCount); err != nil {
			return schedulerTaskPage{}, err
		}
		rows, err := db.Query(
			`SELECT
				task_name,owner,mode,status,interval_seconds,created_at_unix,updated_at_unix,closed_at_unix,
				last_trigger,last_started_at_unix,last_ended_at_unix,last_duration_ms,last_error,in_flight,
				run_count,success_count,failure_count,last_summary_json
			FROM scheduler_tasks`+whereSQL+` ORDER BY `+orderClause,
			args...,
		)
		if err != nil {
			return schedulerTaskPage{}, err
		}
		defer rows.Close()
		out.Items = make([]schedulerTaskItem, 0, 64)
		for rows.Next() {
			var it schedulerTaskItem
			var inFlightInt int64
			var lastSummaryRaw string
			if err := rows.Scan(
				&it.Name, &it.Owner, &it.Mode, &it.Status, &it.IntervalSeconds, &it.CreatedAtUnix, &it.UpdatedAtUnix, &it.ClosedAtUnix,
				&it.LastTrigger, &it.LastStartedAtUnix, &it.LastEndedAtUnix, &it.LastDurationMS, &it.LastError, &inFlightInt,
				&it.RunCount, &it.SuccessCount, &it.FailureCount, &lastSummaryRaw,
			); err != nil {
				return schedulerTaskPage{}, err
			}
			it.InFlight = inFlightInt != 0
			if strings.TrimSpace(lastSummaryRaw) != "" {
				var decoded any
				if err := json.Unmarshal([]byte(lastSummaryRaw), &decoded); err == nil {
					it.LastSummary = decoded
				} else {
					it.LastSummary = map[string]any{}
				}
			} else {
				it.LastSummary = map[string]any{}
			}
			if it.InFlight {
				out.InFlightCount++
			}
			if strings.EqualFold(strings.TrimSpace(it.Status), "active") {
				out.ActiveCount++
			} else {
				out.StoppedCount++
			}
			out.FailureTotal += it.FailureCount
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return schedulerTaskPage{}, err
		}
		if f.Mode != "" {
			out.FilterCount++
		}
		if f.Owner != "" {
			out.FilterCount++
		}
		if f.NamePrefix != "" {
			out.FilterCount++
		}
		if f.Status != "" {
			out.FilterCount++
		}
		if f.InFlightSet {
			out.FilterCount++
		}
		if f.HasErrorSet {
			out.FilterCount++
		}
		return out, nil
	})
}

func dbListSchedulerRuns(ctx context.Context, store *clientDB, f schedulerRunFilter) (schedulerRunPage, error) {
	if store == nil {
		return schedulerRunPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (schedulerRunPage, error) {
		where := make([]string, 0, 6)
		args := make([]any, 0, 8)
		if f.TaskName != "" {
			where = append(where, "task_name=?")
			args = append(args, f.TaskName)
		}
		if f.Owner != "" {
			where = append(where, "owner=?")
			args = append(args, f.Owner)
		}
		if f.Mode != "" {
			where = append(where, "mode=?")
			args = append(args, f.Mode)
		}
		if f.Status != "" {
			where = append(where, "status=?")
			args = append(args, f.Status)
		}
		whereSQL := ""
		if len(where) > 0 {
			whereSQL = " WHERE " + strings.Join(where, " AND ")
		}
		var out schedulerRunPage
		if err := db.QueryRow("SELECT COUNT(1) FROM scheduler_task_runs"+whereSQL, args...).Scan(&out.Total); err != nil {
			return schedulerRunPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,task_name,owner,mode,trigger,started_at_unix,ended_at_unix,duration_ms,status,error_message,summary_json,created_at_unix
			 FROM scheduler_task_runs`+whereSQL+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return schedulerRunPage{}, err
		}
		defer rows.Close()
		out.Items = make([]schedulerRunItem, 0, f.Limit)
		for rows.Next() {
			var it schedulerRunItem
			var summaryRaw string
			if err := rows.Scan(
				&it.ID, &it.TaskName, &it.Owner, &it.Mode, &it.Trigger, &it.StartedAtUnix, &it.EndedAtUnix, &it.DurationMS,
				&it.Status, &it.ErrorMessage, &summaryRaw, &it.CreatedAtUnix,
			); err != nil {
				return schedulerRunPage{}, err
			}
			if strings.TrimSpace(summaryRaw) != "" {
				var summary any
				if err := json.Unmarshal([]byte(summaryRaw), &summary); err == nil {
					it.Summary = summary
				} else {
					it.Summary = map[string]any{}
				}
			} else {
				it.Summary = map[string]any{}
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return schedulerRunPage{}, err
		}
		return out, nil
	})
}

func dbListChainTipWorkerLogs(ctx context.Context, store *clientDB, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	return dbListChainWorkerLogsByTable(ctx, store, "chain_tip_worker_logs", f)
}

func dbListChainUTXOWorkerLogs(ctx context.Context, store *clientDB, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	return dbListChainWorkerLogsByTable(ctx, store, "chain_utxo_worker_logs", f)
}

func dbListChainWorkerLogsByTable(ctx context.Context, store *clientDB, table string, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	if store == nil {
		return chainWorkerLogPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (chainWorkerLogPage, error) {
		where := ""
		args := make([]any, 0, 4)
		if f.Status != "" {
			where = " WHERE status=?"
			args = append(args, f.Status)
		}
		var out chainWorkerLogPage
		if err := db.QueryRow("SELECT COUNT(1) FROM "+table+where, args...).Scan(&out.Total); err != nil {
			return chainWorkerLogPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,triggered_at_unix,started_at_unix,ended_at_unix,duration_ms,trigger_source,status,error_message,result_json
			 FROM `+table+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return chainWorkerLogPage{}, err
		}
		defer rows.Close()
		out.Items = make([]chainWorkerLogItem, 0, f.Limit)
		for rows.Next() {
			var it chainWorkerLogItem
			var result string
			if err := rows.Scan(&it.ID, &it.TriggeredAtUnix, &it.StartedAtUnix, &it.EndedAtUnix, &it.DurationMS, &it.TriggerSource, &it.Status, &it.ErrorMessage, &result); err != nil {
				return chainWorkerLogPage{}, err
			}
			it.Result = json.RawMessage(result)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return chainWorkerLogPage{}, err
		}
		return out, nil
	})
}

func dbListWalletUTXOs(ctx context.Context, store *clientDB, f walletUTXOFilter) (walletUTXOPage, error) {
	if store == nil {
		return walletUTXOPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.WalletID != "" {
			where += " AND wallet_id=?"
			args = append(args, f.WalletID)
		}
		if f.Address != "" {
			where += " AND address=?"
			args = append(args, f.Address)
		}
		if f.State != "" {
			where += " AND state=?"
			args = append(args, f.State)
		}
		if f.TxID != "" {
			where += " AND txid=?"
			args = append(args, f.TxID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (utxo_id LIKE ? OR txid LIKE ? OR address LIKE ? OR created_txid LIKE ? OR spent_txid LIKE ?)"
			args = append(args, like, like, like, like, like)
		}
		var out walletUTXOPage
		if err := db.QueryRow("SELECT COUNT(1) FROM wallet_utxo WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return walletUTXOPage{}, err
		}
		rows, err := db.Query(
			`SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
			 FROM wallet_utxo WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,utxo_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return walletUTXOPage{}, err
		}
		defer rows.Close()
		out.Items = make([]walletUTXOItem, 0, f.Limit)
		for rows.Next() {
			var it walletUTXOItem
			if err := rows.Scan(
				&it.UTXOID, &it.WalletID, &it.Address, &it.TxID, &it.Vout, &it.ValueSatoshi, &it.State, &it.AllocationClass, &it.AllocationReason,
				&it.CreatedTxID, &it.SpentTxID, &it.CreatedAtUnix, &it.UpdatedAtUnix, &it.SpentAtUnix,
			); err != nil {
				return walletUTXOPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return walletUTXOPage{}, err
		}
		return out, nil
	})
}

func dbGetWalletUTXO(ctx context.Context, store *clientDB, utxoID string) (walletUTXOItem, error) {
	if store == nil {
		return walletUTXOItem{}, fmt.Errorf("client db is nil")
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOItem, error) {
		var item walletUTXOItem
		err := db.QueryRow(
			`SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
			 FROM wallet_utxo WHERE utxo_id=?`,
			utxoID,
		).Scan(
			&item.UTXOID, &item.WalletID, &item.Address, &item.TxID, &item.Vout, &item.ValueSatoshi, &item.State, &item.AllocationClass, &item.AllocationReason,
			&item.CreatedTxID, &item.SpentTxID, &item.CreatedAtUnix, &item.UpdatedAtUnix, &item.SpentAtUnix,
		)
		return item, err
	})
	if err != nil {
		return walletUTXOItem{}, err
	}
	assets, err := dbListWalletUTXOAssetRows(ctx, store, utxoID)
	if err == nil && len(assets) > 0 {
		out.Assets = assets
	}
	return out, nil
}

func dbListWalletUTXOEvents(ctx context.Context, store *clientDB, f walletUTXOEventFilter) (walletUTXOEventPage, error) {
	if store == nil {
		return walletUTXOEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOEventPage, error) {
		where := ""
		args := make([]any, 0, 12)
		if f.UTXOID != "" {
			where += " AND utxo_id=?"
			args = append(args, f.UTXOID)
		}
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.RefTxID != "" {
			where += " AND ref_txid=?"
			args = append(args, f.RefTxID)
		}
		if f.RefBusinessID != "" {
			where += " AND ref_business_id=?"
			args = append(args, f.RefBusinessID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (utxo_id LIKE ? OR note LIKE ? OR ref_txid LIKE ? OR ref_business_id LIKE ?)"
			args = append(args, like, like, like, like)
		}
		var out walletUTXOEventPage
		if err := db.QueryRow("SELECT COUNT(1) FROM wallet_utxo_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return walletUTXOEventPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,utxo_id,event_type,ref_txid,ref_business_id,note,payload_json
			 FROM wallet_utxo_events WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return walletUTXOEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]walletUTXOEventItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanWalletUTXOEventItem(rows)
			if err != nil {
				return walletUTXOEventPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return walletUTXOEventPage{}, err
		}
		return out, nil
	})
}

func dbGetWalletUTXOEvent(ctx context.Context, store *clientDB, id int64) (walletUTXOEventItem, error) {
	if store == nil {
		return walletUTXOEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (walletUTXOEventItem, error) {
		row := db.QueryRow(`SELECT id,created_at_unix,utxo_id,event_type,ref_txid,ref_business_id,note,payload_json FROM wallet_utxo_events WHERE id=?`, id)
		return scanWalletUTXOEventItem(row)
	})
}

type scanWalletUTXOEvent interface {
	Scan(dest ...any) error
}

func scanWalletUTXOEventItem(row scanWalletUTXOEvent) (walletUTXOEventItem, error) {
	var out walletUTXOEventItem
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.UTXOID, &out.EventType, &out.RefTxID, &out.RefBusinessID, &out.Note, &payload)
	if err != nil {
		return walletUTXOEventItem{}, err
	}
	out.Payload = json.RawMessage(payload)
	return out, nil
}
