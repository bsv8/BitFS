package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

type commandJournalFilter struct {
	Limit         int
	Offset        int
	CommandTypes  []string
	CommandType   string
	GatewayPeerID string
	Status        string
	CommandID     string
	TriggerKey    string // 按来源链路键过滤，用于关联 orchestrator 日志
	Query         string
}

type commandJournalPage struct {
	Total int
	Items []commandJournalItem
}

type commandJournalItem struct {
	ID            int64  `json:"id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	CommandID     string `json:"command_id"`
	CommandType   string `json:"command_type"`
	GatewayPeerID string `json:"gateway_pubkey_hex"`
	AggregateID   string `json:"aggregate_id"`
	RequestedBy   string `json:"requested_by"`
	RequestedAt   int64  `json:"requested_at_unix"`
	Accepted      bool   `json:"accepted"`
	Status        string `json:"status"`
	ErrorCode     string `json:"error_code"`
	ErrorMessage  string `json:"error_message"`
	StateBefore   string `json:"state_before"`
	StateAfter    string `json:"state_after"`
	DurationMS    int64  `json:"duration_ms"`
	// TriggerKey 是来源链路键，不是命令主键；用于关联 orchestrator_logs.idempotency_key
	TriggerKey string          `json:"trigger_key"`
	Payload    json.RawMessage `json:"payload"`
	Result     json.RawMessage `json:"result"`
}

type domainEventFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	SourceRef     string
	EventName     string
}

type domainEventPage struct {
	Total int
	Items []domainEventItem
}

type domainEventItem struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	CommandID      string          `json:"command_id"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	SourceKind     string          `json:"source_kind"`
	SourceRef      string          `json:"source_ref"`
	ObservedAtUnix int64           `json:"observed_at_unix"`
	EventName      string          `json:"event_name"`
	StateBefore    string          `json:"state_before"`
	StateAfter     string          `json:"state_after"`
	Payload        json.RawMessage `json:"payload"`
}

type stateSnapshotFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	SourceRef     string
	State         string
}

type stateSnapshotPage struct {
	Total int
	Items []stateSnapshotItem
}

type stateSnapshotItem struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	CommandID      string          `json:"command_id"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	SourceKind     string          `json:"source_kind"`
	SourceRef      string          `json:"source_ref"`
	ObservedAtUnix int64           `json:"observed_at_unix"`
	State          string          `json:"state"`
	PauseReason    string          `json:"pause_reason"`
	PauseNeedSat   uint64          `json:"pause_need_satoshi"`
	PauseHaveSat   uint64          `json:"pause_have_satoshi"`
	LastError      string          `json:"last_error"`
	Payload        json.RawMessage `json:"payload"`
}

type observedGatewayStateFilter struct {
	Limit         int
	Offset        int
	GatewayPeerID string
	SourceRef     string
	EventName     string
	State         string
}

type observedGatewayStatePage struct {
	Total int
	Items []observedGatewayStateItem
}

type observedGatewayStateItem struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	SourceRef      string          `json:"source_ref"`
	ObservedAtUnix int64           `json:"observed_at_unix"`
	EventName      string          `json:"event_name"`
	StateBefore    string          `json:"state_before"`
	StateAfter     string          `json:"state_after"`
	PauseReason    string          `json:"pause_reason"`
	PauseNeedSat   uint64          `json:"pause_need_satoshi"`
	PauseHaveSat   uint64          `json:"pause_have_satoshi"`
	LastError      string          `json:"last_error"`
	Payload        json.RawMessage `json:"payload"`
}

type effectLogFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	EffectType    string
	Stage         string
	Status        string
}

type effectLogPage struct {
	Total int
	Items []effectLogItem
}

type effectLogItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	CommandID     string          `json:"command_id"`
	GatewayPeerID string          `json:"gateway_pubkey_hex"`
	EffectType    string          `json:"effect_type"`
	Stage         string          `json:"stage"`
	Status        string          `json:"status"`
	ErrorMessage  string          `json:"error_message"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListCommandJournal(ctx context.Context, store *clientDB, f commandJournalFilter) (commandJournalPage, error) {
	if store == nil {
		return commandJournalPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (commandJournalPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if len(f.CommandTypes) > 0 {
			where += " AND command_type IN (" + sqlPlaceholders(len(f.CommandTypes)) + ")"
			for _, item := range f.CommandTypes {
				args = append(args, item)
			}
		}
		if f.CommandType != "" {
			where += " AND command_type=?"
			args = append(args, f.CommandType)
		}
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.CommandID != "" {
			where += " AND command_id=?"
			args = append(args, f.CommandID)
		}
		if f.TriggerKey != "" {
			where += " AND trigger_key=?"
			args = append(args, f.TriggerKey)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (error_message LIKE ? OR command_id LIKE ? OR requested_by LIKE ? OR state_before LIKE ? OR state_after LIKE ?)"
			args = append(args, like, like, like, like, like)
		}
		var out commandJournalPage
		if err := db.QueryRow("SELECT COUNT(1) FROM command_journal WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return commandJournalPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
			FROM command_journal WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return commandJournalPage{}, err
		}
		defer rows.Close()
		out.Items = make([]commandJournalItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanCommandJournalItem(rows)
			if err != nil {
				return commandJournalPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return commandJournalPage{}, err
		}
		return out, nil
	})
}

func dbGetCommandJournalItem(ctx context.Context, store *clientDB, id int64) (commandJournalItem, error) {
	if store == nil {
		return commandJournalItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (commandJournalItem, error) {
		row := db.QueryRow(
			`SELECT id,created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
			FROM command_journal WHERE id=?`, id,
		)
		return scanCommandJournalItem(row)
	})
}

func dbListDomainEvents(ctx context.Context, store *clientDB, f domainEventFilter) (domainEventPage, error) {
	if store == nil {
		return domainEventPage{}, fmt.Errorf("client db is nil")
	}
	// 这里是命令口径，只查 command 事实；历史 observed 旧行会在迁移阶段搬走。
	return clientDBValue(ctx, store, func(db *sql.DB) (domainEventPage, error) {
		where := ""
		args := make([]any, 0, 4)
		where += " AND source_kind='command'"
		if f.CommandID != "" {
			where += " AND command_id=?"
			args = append(args, f.CommandID)
		}
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.SourceRef != "" {
			where += " AND source_ref=?"
			args = append(args, f.SourceRef)
		}
		if f.EventName != "" {
			where += " AND event_name=?"
			args = append(args, f.EventName)
		}
		var out domainEventPage
		if err := db.QueryRow("SELECT COUNT(1) FROM domain_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return domainEventPage{}, err
		}
		rows, err := db.Query(`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json FROM domain_events WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, append(args, f.Limit, f.Offset)...)
		if err != nil {
			return domainEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]domainEventItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanDomainEventItem(rows)
			if err != nil {
				return domainEventPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return domainEventPage{}, err
		}
		return out, nil
	})
}

func dbGetDomainEventItem(ctx context.Context, store *clientDB, id int64) (domainEventItem, error) {
	if store == nil {
		return domainEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (domainEventItem, error) {
		row := db.QueryRow(`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json FROM domain_events WHERE id=? AND source_kind='command'`, id)
		return scanDomainEventItem(row)
	})
}

func dbListStateSnapshots(ctx context.Context, store *clientDB, f stateSnapshotFilter) (stateSnapshotPage, error) {
	if store == nil {
		return stateSnapshotPage{}, fmt.Errorf("client db is nil")
	}
	// 这里是命令口径，只查 command 事实；历史 observed 旧行会在迁移阶段搬走。
	return clientDBValue(ctx, store, func(db *sql.DB) (stateSnapshotPage, error) {
		where := ""
		args := make([]any, 0, 4)
		where += " AND source_kind='command'"
		if f.CommandID != "" {
			where += " AND command_id=?"
			args = append(args, f.CommandID)
		}
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.SourceRef != "" {
			where += " AND source_ref=?"
			args = append(args, f.SourceRef)
		}
		if f.State != "" {
			where += " AND state=?"
			args = append(args, f.State)
		}
		var out stateSnapshotPage
		if err := db.QueryRow("SELECT COUNT(1) FROM state_snapshots WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return stateSnapshotPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			FROM state_snapshots WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return stateSnapshotPage{}, err
		}
		defer rows.Close()
		out.Items = make([]stateSnapshotItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanStateSnapshotItem(rows)
			if err != nil {
				return stateSnapshotPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return stateSnapshotPage{}, err
		}
		return out, nil
	})
}

func dbGetStateSnapshotItem(ctx context.Context, store *clientDB, id int64) (stateSnapshotItem, error) {
	if store == nil {
		return stateSnapshotItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (stateSnapshotItem, error) {
		row := db.QueryRow(
			`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			FROM state_snapshots WHERE id=? AND source_kind='command'`, id,
		)
		return scanStateSnapshotItem(row)
	})
}

func dbListObservedGatewayStates(ctx context.Context, store *clientDB, f observedGatewayStateFilter) (observedGatewayStatePage, error) {
	if store == nil {
		return observedGatewayStatePage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (observedGatewayStatePage, error) {
		where := ""
		args := make([]any, 0, 4)
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.SourceRef != "" {
			where += " AND source_ref=?"
			args = append(args, f.SourceRef)
		}
		if f.EventName != "" {
			where += " AND event_name=?"
			args = append(args, f.EventName)
		}
		if f.State != "" {
			where += " AND state_after=?"
			args = append(args, f.State)
		}
		var out observedGatewayStatePage
		if err := db.QueryRow("SELECT COUNT(1) FROM observed_gateway_states WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return observedGatewayStatePage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			FROM observed_gateway_states WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return observedGatewayStatePage{}, err
		}
		defer rows.Close()
		out.Items = make([]observedGatewayStateItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanObservedGatewayStateItem(rows)
			if err != nil {
				return observedGatewayStatePage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return observedGatewayStatePage{}, err
		}
		return out, nil
	})
}

func dbGetObservedGatewayStateItem(ctx context.Context, store *clientDB, id int64) (observedGatewayStateItem, error) {
	if store == nil {
		return observedGatewayStateItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (observedGatewayStateItem, error) {
		row := db.QueryRow(
			`SELECT id,created_at_unix,gateway_pubkey_hex,source_ref,observed_at_unix,event_name,state_before,state_after,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
			FROM observed_gateway_states WHERE id=?`, id,
		)
		return scanObservedGatewayStateItem(row)
	})
}

func dbListEffectLogs(ctx context.Context, store *clientDB, f effectLogFilter) (effectLogPage, error) {
	if store == nil {
		return effectLogPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (effectLogPage, error) {
		where := ""
		args := make([]any, 0, 6)
		if f.CommandID != "" {
			where += " AND command_id=?"
			args = append(args, f.CommandID)
		}
		if f.GatewayPeerID != "" {
			where += " AND gateway_pubkey_hex=?"
			args = append(args, f.GatewayPeerID)
		}
		if f.EffectType != "" {
			where += " AND effect_type=?"
			args = append(args, f.EffectType)
		}
		if f.Stage != "" {
			where += " AND stage=?"
			args = append(args, f.Stage)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		var out effectLogPage
		if err := db.QueryRow("SELECT COUNT(1) FROM effect_logs WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return effectLogPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			FROM effect_logs WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return effectLogPage{}, err
		}
		defer rows.Close()
		out.Items = make([]effectLogItem, 0, f.Limit)
		for rows.Next() {
			it, err := scanEffectLogItem(rows)
			if err != nil {
				return effectLogPage{}, err
			}
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return effectLogPage{}, err
		}
		return out, nil
	})
}

func dbGetEffectLogItem(ctx context.Context, store *clientDB, id int64) (effectLogItem, error) {
	if store == nil {
		return effectLogItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (effectLogItem, error) {
		row := db.QueryRow(
			`SELECT id,created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
			FROM effect_logs WHERE id=?`, id,
		)
		return scanEffectLogItem(row)
	})
}

type scanCommandJournal interface {
	Scan(dest ...any) error
}

func scanCommandJournalItem(row scanCommandJournal) (commandJournalItem, error) {
	var out commandJournalItem
	var accepted int
	var payload string
	var result string
	err := row.Scan(
		&out.ID, &out.CreatedAtUnix, &out.CommandID, &out.CommandType, &out.GatewayPeerID, &out.AggregateID, &out.RequestedBy, &out.RequestedAt, &accepted, &out.Status, &out.ErrorCode, &out.ErrorMessage, &out.StateBefore, &out.StateAfter, &out.DurationMS, &out.TriggerKey, &payload, &result,
	)
	if err != nil {
		return commandJournalItem{}, err
	}
	out.Accepted = accepted != 0
	out.Payload = json.RawMessage(payload)
	out.Result = json.RawMessage(result)
	return out, nil
}

type scanDomainEvent interface {
	Scan(dest ...any) error
}

func scanDomainEventItem(row scanDomainEvent) (domainEventItem, error) {
	var out domainEventItem
	var commandID sql.NullString
	var payload string
	var sourceKind sql.NullString
	var sourceRef sql.NullString
	var observedAtUnix int64
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &commandID, &out.GatewayPeerID, &sourceKind, &sourceRef, &observedAtUnix, &out.EventName, &out.StateBefore, &out.StateAfter, &payload)
	if err != nil {
		return domainEventItem{}, err
	}
	if commandID.Valid {
		out.CommandID = commandID.String
	}
	out.SourceKind = sourceKind.String
	out.SourceRef = sourceRef.String
	out.ObservedAtUnix = observedAtUnix
	out.Payload = json.RawMessage(payload)
	return out, nil
}

type scanStateSnapshot interface {
	Scan(dest ...any) error
}

func scanStateSnapshotItem(row scanStateSnapshot) (stateSnapshotItem, error) {
	var out stateSnapshotItem
	var commandID sql.NullString
	var payload string
	var sourceKind sql.NullString
	var sourceRef sql.NullString
	var observedAtUnix int64
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &commandID, &out.GatewayPeerID, &sourceKind, &sourceRef, &observedAtUnix, &out.State, &out.PauseReason, &out.PauseNeedSat, &out.PauseHaveSat, &out.LastError, &payload)
	if err != nil {
		return stateSnapshotItem{}, err
	}
	if commandID.Valid {
		out.CommandID = commandID.String
	}
	out.SourceKind = sourceKind.String
	out.SourceRef = sourceRef.String
	out.ObservedAtUnix = observedAtUnix
	out.Payload = json.RawMessage(payload)
	return out, nil
}

type scanObservedGatewayState interface {
	Scan(dest ...any) error
}

func scanObservedGatewayStateItem(row scanObservedGatewayState) (observedGatewayStateItem, error) {
	var out observedGatewayStateItem
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.GatewayPeerID, &out.SourceRef, &out.ObservedAtUnix, &out.EventName, &out.StateBefore, &out.StateAfter, &out.PauseReason, &out.PauseNeedSat, &out.PauseHaveSat, &out.LastError, &payload)
	if err != nil {
		return observedGatewayStateItem{}, err
	}
	out.Payload = json.RawMessage(payload)
	return out, nil
}

type scanEffectLog interface {
	Scan(dest ...any) error
}

func scanEffectLogItem(row scanEffectLog) (effectLogItem, error) {
	var out effectLogItem
	var payload string
	err := row.Scan(&out.ID, &out.CreatedAtUnix, &out.CommandID, &out.GatewayPeerID, &out.EffectType, &out.Stage, &out.Status, &out.ErrorMessage, &payload)
	if err != nil {
		return effectLogItem{}, err
	}
	out.Payload = json.RawMessage(payload)
	return out, nil
}

func sqlPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}
