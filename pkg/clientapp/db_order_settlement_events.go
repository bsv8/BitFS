package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// businessTriggerItem / Entry / Filter / Page 保留旧调用面，但底层已切到 order_settlement_events。
type businessTriggerItem struct {
	TriggerID      string          `json:"trigger_id"`
	OrderID        string          `json:"order_id"`
	TriggerType    string          `json:"trigger_type"`
	TriggerIDValue string          `json:"trigger_id_value"`
	TriggerRole    string          `json:"trigger_role"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type businessTriggerEntry struct {
	TriggerID      string
	OrderID        string
	SettlementID   string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
	CreatedAtUnix  int64
	Note           string
	Payload        any
}

type businessTriggerFilter struct {
	Limit          int
	Offset         int
	TriggerID      string
	OrderID        string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
}

type businessTriggerPage struct {
	Total int
	Items []businessTriggerItem
}

// orderSettlementEventItem 结算事件记录
// 设计：只承载 settlement 生命周期和桥接痕迹，不再保留旧 trigger 表。
type orderSettlementEventItem struct {
	ID                int64           `json:"id"`
	ProcessID         string          `json:"process_id"`
	SettlementID      string          `json:"settlement_id"`
	SourceType        string          `json:"source_type"`
	SourceID          string          `json:"source_id"`
	AccountingScene   string          `json:"accounting_scene"`
	AccountingSubtype string          `json:"accounting_subtype"`
	EventType         string          `json:"event_type"`
	Status            string          `json:"status"`
	IdempotencyKey    string          `json:"idempotency_key"`
	Note              string          `json:"note"`
	OccurredAtUnix    int64           `json:"occurred_at_unix"`
	Payload           json.RawMessage `json:"payload"`
}

// orderSettlementEventEntry 结算事件写入条目
type orderSettlementEventEntry struct {
	ProcessID         string
	SettlementID      string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string
	EventType         string
	Status            string
	IdempotencyKey    string
	Note              string
	OccurredAtUnix    int64
	Payload           any
}

// orderSettlementEventFilter 结算事件查询过滤条件
type orderSettlementEventFilter struct {
	Limit             int
	Offset            int
	ID                int64
	ProcessID         string
	SettlementID      string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string
	EventType         string
	Status            string
}

// orderSettlementEventPage 结算事件分页结果
type orderSettlementEventPage struct {
	Total int
	Items []orderSettlementEventItem
}

// dbAppendOrderSettlementEvent 写入结算事件。
func dbAppendOrderSettlementEvent(ctx context.Context, store *clientDB, e orderSettlementEventEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.SettlementID = strings.TrimSpace(e.SettlementID)
	if e.SettlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	e.EventType = strings.TrimSpace(e.EventType)
	if e.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if e.OccurredAtUnix <= 0 {
		e.OccurredAtUnix = time.Now().Unix()
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db,
			`INSERT INTO order_settlement_events(
				process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,payload_json,occurred_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_id, event_type, idempotency_key) DO UPDATE SET
				process_id=excluded.process_id,
				source_type=excluded.source_type,
				source_id=excluded.source_id,
				accounting_scene=excluded.accounting_scene,
				accounting_subtype=excluded.accounting_subtype,
				status=excluded.status,
				note=excluded.note,
				payload_json=excluded.payload_json,
				occurred_at_unix=excluded.occurred_at_unix`,
			strings.TrimSpace(e.ProcessID),
			e.SettlementID,
			strings.TrimSpace(e.SourceType),
			strings.TrimSpace(e.SourceID),
			strings.TrimSpace(e.AccountingScene),
			strings.TrimSpace(e.AccountingSubtype),
			e.EventType,
			strings.TrimSpace(e.Status),
			e.IdempotencyKey,
			strings.TrimSpace(e.Note),
			mustJSONString(e.Payload),
			e.OccurredAtUnix,
		)
		return err
	})
}

// dbGetOrderSettlementEvent 按 id 查询事件。
func dbGetOrderSettlementEvent(ctx context.Context, store *clientDB, id int64) (orderSettlementEventItem, error) {
	if store == nil {
		return orderSettlementEventItem{}, fmt.Errorf("client db is nil")
	}
	if id <= 0 {
		return orderSettlementEventItem{}, fmt.Errorf("id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (orderSettlementEventItem, error) {
		var item orderSettlementEventItem
		var payload string
		err := QueryRowContext(ctx, db,
			`SELECT id,process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,occurred_at_unix,payload_json
			 FROM order_settlement_events WHERE id=?`,
			id,
		).Scan(
			&item.ID, &item.ProcessID, &item.SettlementID, &item.SourceType, &item.SourceID,
			&item.AccountingScene, &item.AccountingSubtype, &item.EventType, &item.Status,
			&item.IdempotencyKey, &item.Note, &item.OccurredAtUnix, &payload,
		)
		if err != nil {
			return orderSettlementEventItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListOrderSettlementEvents 查询结算事件列表。
func dbListOrderSettlementEvents(ctx context.Context, store *clientDB, f orderSettlementEventFilter) (orderSettlementEventPage, error) {
	if store == nil {
		return orderSettlementEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (orderSettlementEventPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.ID > 0 {
			where += " AND id=?"
			args = append(args, f.ID)
		}
		if f.ProcessID != "" {
			where += " AND process_id=?"
			args = append(args, f.ProcessID)
		}
		if f.SettlementID != "" {
			where += " AND settlement_id=?"
			args = append(args, f.SettlementID)
		}
		if f.SourceType != "" {
			where += " AND source_type=?"
			args = append(args, f.SourceType)
		}
		if f.SourceID != "" {
			where += " AND source_id=?"
			args = append(args, f.SourceID)
		}
		if f.AccountingScene != "" {
			where += " AND accounting_scene=?"
			args = append(args, f.AccountingScene)
		}
		if f.AccountingSubtype != "" {
			where += " AND accounting_subtype=?"
			args = append(args, f.AccountingSubtype)
		}
		if f.EventType != "" {
			where += " AND event_type=?"
			args = append(args, f.EventType)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		var out orderSettlementEventPage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM order_settlement_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return orderSettlementEventPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := QueryContext(ctx, db,
			`SELECT id,process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,occurred_at_unix,payload_json
			 FROM order_settlement_events WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return orderSettlementEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]orderSettlementEventItem, 0, f.Limit)
		for rows.Next() {
			var item orderSettlementEventItem
			var payload string
			if err := rows.Scan(
				&item.ID, &item.ProcessID, &item.SettlementID, &item.SourceType, &item.SourceID,
				&item.AccountingScene, &item.AccountingSubtype, &item.EventType, &item.Status,
				&item.IdempotencyKey, &item.Note, &item.OccurredAtUnix, &payload,
			); err != nil {
				return orderSettlementEventPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, item)
		}
		if err := rows.Err(); err != nil {
			return orderSettlementEventPage{}, err
		}
		return out, nil
	})
}

// dbAppendBusinessTrigger 旧触发器入口，新版本写入 order_settlement_events。
func dbAppendBusinessTrigger(ctx context.Context, store *clientDB, e businessTriggerEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.TriggerID = strings.TrimSpace(e.TriggerID)
	if e.TriggerID == "" {
		return fmt.Errorf("trigger_id is required")
	}
	e.OrderID = strings.TrimSpace(e.OrderID)
	if e.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	settlement, err := dbGetBusinessSettlementByBusinessID(ctx, store, e.OrderID)
	if err != nil {
		return err
	}
	if settlement.SettlementID == "" {
		return fmt.Errorf("settlement not found for order_id=%s", e.OrderID)
	}
	return dbAppendOrderSettlementEvent(ctx, store, orderSettlementEventEntry{
		ProcessID:         e.TriggerID,
		SettlementID:      settlement.SettlementID,
		SourceType:        strings.TrimSpace(e.TriggerType),
		SourceID:          strings.TrimSpace(e.TriggerIDValue),
		AccountingScene:   "",
		AccountingSubtype: strings.TrimSpace(e.TriggerRole),
		EventType:         "bridge_trigger",
		Status:            "linked",
		IdempotencyKey:    e.TriggerID + ":" + e.OrderID + ":" + strings.TrimSpace(e.TriggerType) + ":" + strings.TrimSpace(e.TriggerIDValue) + ":" + strings.TrimSpace(e.TriggerRole),
		Note:              strings.TrimSpace(e.Note),
		Payload: map[string]any{
			"trigger_id":       e.TriggerID,
			"order_id":         e.OrderID,
			"trigger_type":     strings.TrimSpace(e.TriggerType),
			"trigger_id_value": strings.TrimSpace(e.TriggerIDValue),
			"trigger_role":     strings.TrimSpace(e.TriggerRole),
		},
	})
}

// dbGetBusinessTrigger 按 trigger_id 查询旧桥接记录。
func dbGetBusinessTrigger(ctx context.Context, store *clientDB, triggerID string) (businessTriggerItem, error) {
	if store == nil {
		return businessTriggerItem{}, fmt.Errorf("client db is nil")
	}
	triggerID = strings.TrimSpace(triggerID)
	if triggerID == "" {
		return businessTriggerItem{}, fmt.Errorf("trigger_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessTriggerItem, error) {
		var event orderSettlementEventItem
		var payload string
		err := QueryRowContext(ctx, db,
			`SELECT id,process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,occurred_at_unix,payload_json
			 FROM order_settlement_events WHERE process_id=?`,
			triggerID,
		).Scan(
			&event.ID, &event.ProcessID, &event.SettlementID, &event.SourceType, &event.SourceID,
			&event.AccountingScene, &event.AccountingSubtype, &event.EventType, &event.Status,
			&event.IdempotencyKey, &event.Note, &event.OccurredAtUnix, &payload,
		)
		if err != nil {
			return businessTriggerItem{}, err
		}
		event.Payload = json.RawMessage(payload)
		return businessTriggerItem{
			TriggerID:      event.ProcessID,
			OrderID:        settlementOrderIDFromSettlementID(ctx, store, event.SettlementID),
			TriggerType:    event.SourceType,
			TriggerIDValue: event.SourceID,
			TriggerRole:    event.AccountingSubtype,
			CreatedAtUnix:  event.OccurredAtUnix,
			Note:           event.Note,
			Payload:        event.Payload,
		}, nil
	})
}

func settlementOrderIDFromSettlementID(ctx context.Context, store *clientDB, settlementID string) string {
	if store == nil || strings.TrimSpace(settlementID) == "" {
		return ""
	}
	settlementID = strings.TrimSpace(settlementID)
	orderID, err := clientDBValue(ctx, store, func(db *sql.DB) (string, error) {
		var out string
		err := QueryRowContext(ctx, db, `SELECT order_id FROM order_settlements WHERE settlement_id=?`, settlementID).Scan(&out)
		if err != nil {
			return "", err
		}
		return out, nil
	})
	if err != nil {
		return ""
	}
	return orderID
}

// dbListBusinessTriggersByOrderID 按 order_id 查询旧桥接记录列表。
func dbListBusinessTriggersByOrderID(ctx context.Context, store *clientDB, orderID string, limit, offset int) (businessTriggerPage, error) {
	if store == nil {
		return businessTriggerPage{}, fmt.Errorf("client db is nil")
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return businessTriggerPage{}, fmt.Errorf("order_id is required")
	}
	return dbListBusinessTriggers(ctx, store, businessTriggerFilter{
		OrderID: orderID,
		Limit:   limit,
		Offset:  offset,
	})
}

// dbListBusinessTriggers 查询旧桥接记录列表。
func dbListBusinessTriggers(ctx context.Context, store *clientDB, f businessTriggerFilter) (businessTriggerPage, error) {
	if store == nil {
		return businessTriggerPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessTriggerPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.TriggerID != "" {
			where += " AND process_id=?"
			args = append(args, f.TriggerID)
		}
		if f.OrderID != "" {
			where += " AND settlement_id IN (SELECT settlement_id FROM order_settlements WHERE order_id=?)"
			args = append(args, f.OrderID)
		}
		if f.TriggerType != "" {
			where += " AND source_type=?"
			args = append(args, f.TriggerType)
		}
		if f.TriggerIDValue != "" {
			where += " AND source_id=?"
			args = append(args, f.TriggerIDValue)
		}
		if f.TriggerRole != "" {
			where += " AND accounting_subtype=?"
			args = append(args, f.TriggerRole)
		}
		var out businessTriggerPage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM order_settlement_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return businessTriggerPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := QueryContext(ctx, db,
			`SELECT id,process_id,settlement_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,idempotency_key,note,occurred_at_unix,payload_json
			 FROM order_settlement_events WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return businessTriggerPage{}, err
		}
		defer rows.Close()
		out.Items = make([]businessTriggerItem, 0, f.Limit)
		for rows.Next() {
			var event orderSettlementEventItem
			var payload string
			if err := rows.Scan(
				&event.ID, &event.ProcessID, &event.SettlementID, &event.SourceType, &event.SourceID,
				&event.AccountingScene, &event.AccountingSubtype, &event.EventType, &event.Status,
				&event.IdempotencyKey, &event.Note, &event.OccurredAtUnix, &payload,
			); err != nil {
				return businessTriggerPage{}, err
			}
			event.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, businessTriggerItem{
				TriggerID:      event.ProcessID,
				OrderID:        settlementOrderIDFromSettlementID(ctx, store, event.SettlementID),
				TriggerType:    event.SourceType,
				TriggerIDValue: event.SourceID,
				TriggerRole:    event.AccountingSubtype,
				CreatedAtUnix:  event.OccurredAtUnix,
				Note:           event.Note,
				Payload:        event.Payload,
			})
		}
		if err := rows.Err(); err != nil {
			return businessTriggerPage{}, err
		}
		return out, nil
	})
}

// dbListBusinessesByTrigger 按 trigger_type + trigger_id_value 列出关联的 order_id 列表。
func dbListBusinessesByTrigger(ctx context.Context, store *clientDB, triggerType, triggerIDValue string) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	triggerType = strings.TrimSpace(triggerType)
	triggerIDValue = strings.TrimSpace(triggerIDValue)
	if triggerType == "" || triggerIDValue == "" {
		return nil, fmt.Errorf("trigger_type and trigger_id_value are required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) ([]string, error) {
		rows, err := QueryContext(ctx, db,
			`SELECT DISTINCT os.order_id
			 FROM order_settlement_events e
			 JOIN order_settlements os ON os.settlement_id=e.settlement_id
			 WHERE e.source_type=? AND e.source_id=?
			 ORDER BY e.occurred_at_unix DESC`,
			triggerType, triggerIDValue,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var orderID string
			if err := rows.Scan(&orderID); err != nil {
				return nil, err
			}
			out = append(out, orderID)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}
