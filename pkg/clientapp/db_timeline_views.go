package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// 命令时间线是最终读模型，不让外层自己拼原表语义。
// 列表页仍然以命令日志为主，详情页再把关联事实一次性收口出来。

type commandTimelineFilter struct {
	Limit         int
	Offset        int
	CommandTypes  []string
	CommandType   string
	GatewayPeerID string
	Status        string
	CommandID     string
	TriggerKey    string
	Query         string
}

type commandTimelinePage struct {
	Total int
	Items []commandTimelineItem
}

type commandTimelineItem struct {
	commandJournalItem
	GatewayEvents  []gatewayEventItem  `json:"gateway_events,omitempty"`
	DomainEvents   []domainEventItem   `json:"domain_events,omitempty"`
	StateSnapshots []stateSnapshotItem `json:"state_snapshots,omitempty"`
	EffectLogs     []effectLogItem     `json:"effect_logs,omitempty"`
}

type observedGatewayTimelineFilter struct {
	Limit         int
	Offset        int
	GatewayPeerID string
	SourceRef     string
	EventName     string
	State         string
}

type observedGatewayTimelinePage struct {
	Total int
	Items []observedGatewayTimelineItem
}

type observedGatewayTimelineItem struct {
	observedGatewayStateItem
}

func dbListCommandTimeline(ctx context.Context, store *clientDB, f commandTimelineFilter) (commandTimelinePage, error) {
	if store == nil {
		return commandTimelinePage{}, fmt.Errorf("client db is nil")
	}
	page, err := dbListCommandJournal(ctx, store, commandJournalFilter{
		Limit:         f.Limit,
		Offset:        f.Offset,
		CommandTypes:  f.CommandTypes,
		CommandType:   f.CommandType,
		GatewayPeerID: f.GatewayPeerID,
		Status:        f.Status,
		CommandID:     f.CommandID,
		TriggerKey:    f.TriggerKey,
		Query:         f.Query,
	})
	if err != nil {
		return commandTimelinePage{}, err
	}
	out := commandTimelinePage{Total: page.Total, Items: make([]commandTimelineItem, 0, len(page.Items))}
	for _, item := range page.Items {
		out.Items = append(out.Items, commandTimelineItem{commandJournalItem: item})
	}
	return out, nil
}

func dbGetCommandTimelineItem(ctx context.Context, store *clientDB, id int64) (commandTimelineItem, error) {
	if store == nil {
		return commandTimelineItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (commandTimelineItem, error) {
		journalRow := db.QueryRow(
			`SELECT id,created_at_unix,command_id,command_type,gateway_pubkey_hex,aggregate_id,requested_by,requested_at_unix,accepted,status,error_code,error_message,state_before,state_after,duration_ms,trigger_key,payload_json,result_json
			FROM command_journal WHERE id=?`, id,
		)
		journal, err := scanCommandJournalItem(journalRow)
		if err != nil {
			return commandTimelineItem{}, err
		}
		out := commandTimelineItem{commandJournalItem: journal}
		if err := hydrateCommandTimelineRelations(ctx, db, &out); err != nil {
			return commandTimelineItem{}, err
		}
		return out, nil
	})
}

func hydrateCommandTimelineRelations(ctx context.Context, db *sql.DB, out *commandTimelineItem) error {
	if db == nil || out == nil || strings.TrimSpace(out.CommandID) == "" {
		return nil
	}
	commandID := out.CommandID
	out.GatewayEvents = nil
	out.DomainEvents = nil
	out.StateSnapshots = nil
	out.EffectLogs = nil

	if err := loadGatewayEventsByCommandID(ctx, db, commandID, &out.GatewayEvents); err != nil {
		return err
	}
	if err := loadDomainEventsByCommandID(ctx, db, commandID, &out.DomainEvents); err != nil {
		return err
	}
	if err := loadStateSnapshotsByCommandID(ctx, db, commandID, &out.StateSnapshots); err != nil {
		return err
	}
	if err := loadEffectLogsByCommandID(ctx, db, commandID, &out.EffectLogs); err != nil {
		return err
	}
	return nil
}

func loadGatewayEventsByCommandID(ctx context.Context, db *sql.DB, commandID string, out *[]gatewayEventItem) error {
	rows, err := db.QueryContext(ctx, `SELECT id,created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json
		FROM gateway_events WHERE command_id=? ORDER BY id ASC`, commandID)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := make([]gatewayEventItem, 0, 4)
	for rows.Next() {
		it, err := scanGatewayEventItem(rows)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*out = items
	return nil
}

func loadDomainEventsByCommandID(ctx context.Context, db *sql.DB, commandID string, out *[]domainEventItem) error {
	rows, err := db.QueryContext(ctx, `SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,event_name,state_before,state_after,payload_json
		FROM domain_events WHERE command_id=? AND source_kind='command' ORDER BY id ASC`, commandID)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := make([]domainEventItem, 0, 4)
	for rows.Next() {
		it, err := scanDomainEventItem(rows)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*out = items
	return nil
}

func loadStateSnapshotsByCommandID(ctx context.Context, db *sql.DB, commandID string, out *[]stateSnapshotItem) error {
	rows, err := db.QueryContext(ctx, `SELECT id,created_at_unix,command_id,gateway_pubkey_hex,source_kind,source_ref,observed_at_unix,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
		FROM state_snapshots WHERE command_id=? AND source_kind='command' ORDER BY id ASC`, commandID)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := make([]stateSnapshotItem, 0, 4)
	for rows.Next() {
		it, err := scanStateSnapshotItem(rows)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*out = items
	return nil
}

func loadEffectLogsByCommandID(ctx context.Context, db *sql.DB, commandID string, out *[]effectLogItem) error {
	rows, err := db.QueryContext(ctx, `SELECT id,created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
		FROM effect_logs WHERE command_id=? ORDER BY id ASC`, commandID)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := make([]effectLogItem, 0, 4)
	for rows.Next() {
		it, err := scanEffectLogItem(rows)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	*out = items
	return nil
}

func dbListObservedGatewayTimeline(ctx context.Context, store *clientDB, f observedGatewayTimelineFilter) (observedGatewayTimelinePage, error) {
	if store == nil {
		return observedGatewayTimelinePage{}, fmt.Errorf("client db is nil")
	}
	page, err := dbListObservedGatewayStates(ctx, store, observedGatewayStateFilter{
		Limit:         f.Limit,
		Offset:        f.Offset,
		GatewayPeerID: f.GatewayPeerID,
		SourceRef:     f.SourceRef,
		EventName:     f.EventName,
		State:         f.State,
	})
	if err != nil {
		return observedGatewayTimelinePage{}, err
	}
	out := observedGatewayTimelinePage{Total: page.Total, Items: make([]observedGatewayTimelineItem, 0, len(page.Items))}
	for _, item := range page.Items {
		out.Items = append(out.Items, observedGatewayTimelineItem{observedGatewayStateItem: item})
	}
	return out, nil
}

func dbGetObservedGatewayTimelineItem(ctx context.Context, store *clientDB, id int64) (observedGatewayTimelineItem, error) {
	if store == nil {
		return observedGatewayTimelineItem{}, fmt.Errorf("client db is nil")
	}
	item, err := dbGetObservedGatewayStateItem(ctx, store, id)
	if err != nil {
		return observedGatewayTimelineItem{}, err
	}
	return observedGatewayTimelineItem{observedGatewayStateItem: item}, nil
}
