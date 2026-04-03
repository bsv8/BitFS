package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// businessTriggerItem 业务触发桥接记录
// 职责：表达"哪个前台主对象触发了哪条财务事实"
type businessTriggerItem struct {
	TriggerID      string          `json:"trigger_id"`
	BusinessID     string          `json:"business_id"`
	TriggerType    string          `json:"trigger_type"`
	TriggerIDValue string          `json:"trigger_id_value"`
	TriggerRole    string          `json:"trigger_role"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

// businessTriggerEntry 业务触发桥接写入条目
type businessTriggerEntry struct {
	TriggerID      string
	BusinessID     string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
	CreatedAtUnix  int64
	Note           string
	Payload        any
}

// businessTriggerFilter 业务触发桥接查询过滤条件
type businessTriggerFilter struct {
	Limit          int
	Offset         int
	TriggerID      string
	BusinessID     string
	TriggerType    string
	TriggerIDValue string
	TriggerRole    string
}

// businessTriggerPage 业务触发桥接分页结果
type businessTriggerPage struct {
	Total int
	Items []businessTriggerItem
}

// dbAppendBusinessTrigger 插入业务触发桥接记录
// 设计说明：
//   - 幂等约束在 (business_id, trigger_type, trigger_id_value, trigger_role) 上
//   - 支持"一前台单多条 business"：同一 trigger_type+trigger_id_value 可触发多个不同 business
//   - 同一条 business 下重复桥接同一来源时幂等成功
func dbAppendBusinessTrigger(ctx context.Context, store *clientDB, e businessTriggerEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.TriggerID = strings.TrimSpace(e.TriggerID)
	if e.TriggerID == "" {
		return fmt.Errorf("trigger_id is required")
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = time.Now().Unix()
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO business_triggers(
				trigger_id,business_id,trigger_type,trigger_id_value,trigger_role,created_at_unix,note,payload_json
			) VALUES(?,?,?,?,?,?,?,?)
			ON CONFLICT(business_id, trigger_type, trigger_id_value, trigger_role) DO UPDATE SET
				trigger_id=excluded.trigger_id,
				note=excluded.note,
				payload_json=excluded.payload_json`,
			e.TriggerID,
			e.BusinessID,
			strings.TrimSpace(e.TriggerType),
			strings.TrimSpace(e.TriggerIDValue),
			strings.TrimSpace(e.TriggerRole),
			e.CreatedAtUnix,
			strings.TrimSpace(e.Note),
			mustJSONString(e.Payload),
		)
		return err
	})
}

// dbGetBusinessTrigger 按 trigger_id 查询业务触发桥接
func dbGetBusinessTrigger(ctx context.Context, store *clientDB, triggerID string) (businessTriggerItem, error) {
	if store == nil {
		return businessTriggerItem{}, fmt.Errorf("client db is nil")
	}
	triggerID = strings.TrimSpace(triggerID)
	if triggerID == "" {
		return businessTriggerItem{}, fmt.Errorf("trigger_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessTriggerItem, error) {
		var item businessTriggerItem
		var payload string
		err := db.QueryRow(
			`SELECT trigger_id,business_id,trigger_type,trigger_id_value,trigger_role,created_at_unix,note,payload_json
			 FROM business_triggers WHERE trigger_id=?`,
			triggerID,
		).Scan(
			&item.TriggerID, &item.BusinessID, &item.TriggerType, &item.TriggerIDValue,
			&item.TriggerRole, &item.CreatedAtUnix, &item.Note, &payload,
		)
		if err != nil {
			return businessTriggerItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListBusinessTriggersByBusinessID 按 business_id 查询业务触发桥接列表
func dbListBusinessTriggersByBusinessID(ctx context.Context, store *clientDB, businessID string, limit, offset int) (businessTriggerPage, error) {
	if store == nil {
		return businessTriggerPage{}, fmt.Errorf("client db is nil")
	}
	businessID = strings.TrimSpace(businessID)
	if businessID == "" {
		return businessTriggerPage{}, fmt.Errorf("business_id is required")
	}
	return dbListBusinessTriggers(ctx, store, businessTriggerFilter{
		BusinessID: businessID,
		Limit:      limit,
		Offset:     offset,
	})
}

// dbListBusinessTriggers 查询业务触发桥接列表
func dbListBusinessTriggers(ctx context.Context, store *clientDB, f businessTriggerFilter) (businessTriggerPage, error) {
	if store == nil {
		return businessTriggerPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessTriggerPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.TriggerID != "" {
			where += " AND trigger_id=?"
			args = append(args, f.TriggerID)
		}
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TriggerType != "" {
			where += " AND trigger_type=?"
			args = append(args, f.TriggerType)
		}
		if f.TriggerIDValue != "" {
			where += " AND trigger_id_value=?"
			args = append(args, f.TriggerIDValue)
		}
		if f.TriggerRole != "" {
			where += " AND trigger_role=?"
			args = append(args, f.TriggerRole)
		}
		var out businessTriggerPage
		if err := db.QueryRow("SELECT COUNT(1) FROM business_triggers WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return businessTriggerPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := db.Query(
			`SELECT trigger_id,business_id,trigger_type,trigger_id_value,trigger_role,created_at_unix,note,payload_json
			 FROM business_triggers WHERE 1=1`+where+` ORDER BY created_at_unix DESC,trigger_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return businessTriggerPage{}, err
		}
		defer rows.Close()
		out.Items = make([]businessTriggerItem, 0, f.Limit)
		for rows.Next() {
			var item businessTriggerItem
			var payload string
			if err := rows.Scan(
				&item.TriggerID, &item.BusinessID, &item.TriggerType, &item.TriggerIDValue,
				&item.TriggerRole, &item.CreatedAtUnix, &item.Note, &payload,
			); err != nil {
				return businessTriggerPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, item)
		}
		if err := rows.Err(); err != nil {
			return businessTriggerPage{}, err
		}
		return out, nil
	})
}

// dbListBusinessesByTrigger 按 trigger_type + trigger_id_value 列出关联的 business_id 列表
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
		rows, err := db.Query(
			`SELECT DISTINCT business_id FROM business_triggers 
			 WHERE trigger_type=? AND trigger_id_value=? 
			 ORDER BY created_at_unix DESC`,
			triggerType, triggerIDValue,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var businessID string
			if err := rows.Scan(&businessID); err != nil {
				return nil, err
			}
			out = append(out, businessID)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}
