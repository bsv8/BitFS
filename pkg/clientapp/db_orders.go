package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// frontOrderItem 前台业务单记录
// 职责：表达前台业务主身份，不直接承载支付实现
type frontOrderItem struct {
	FrontOrderID     string          `json:"order_id"`
	FrontType        string          `json:"order_type"`
	FrontSubtype     string          `json:"order_subtype"`
	OwnerPubkeyHex   string          `json:"owner_pubkey_hex"`
	TargetObjectType string          `json:"target_object_type"`
	TargetObjectID   string          `json:"target_object_id"`
	Status           string          `json:"status"`
	CreatedAtUnix    int64           `json:"created_at_unix"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Note             string          `json:"note"`
	Payload          json.RawMessage `json:"payload"`
}

// frontOrderEntry 前台业务单写入条目
type frontOrderEntry struct {
	FrontOrderID     string
	FrontType        string
	FrontSubtype     string
	OwnerPubkeyHex   string
	TargetObjectType string
	TargetObjectID   string
	Status           string
	IdempotencyKey   string
	CreatedAtUnix    int64
	UpdatedAtUnix    int64
	Note             string
	Payload          any
}

// frontOrderFilter 前台业务单查询过滤条件
type frontOrderFilter struct {
	Limit            int
	Offset           int
	FrontOrderID     string
	FrontType        string
	FrontSubtype     string
	OwnerPubkeyHex   string
	TargetObjectType string
	TargetObjectID   string
	Status           string
}

// frontOrderPage 前台业务单分页结果
type frontOrderPage struct {
	Total int
	Items []frontOrderItem
}

// dbUpsertFrontOrder 插入或更新前台业务单
// 幂等设计：同一 order_id 重复写入时更新非主键字段
func dbUpsertFrontOrder(ctx context.Context, store *clientDB, e frontOrderEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.FrontOrderID = strings.TrimSpace(e.FrontOrderID)
	if e.FrontOrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	now := time.Now().Unix()
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = now
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = now
	}
	e.IdempotencyKey = strings.TrimSpace(e.IdempotencyKey)
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.FrontOrderID
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db, 
			`INSERT INTO orders(
				order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,idempotency_key,note,payload_json,created_at_unix,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(order_id) DO UPDATE SET
				order_type=excluded.order_type,
				order_subtype=excluded.order_subtype,
				owner_pubkey_hex=excluded.owner_pubkey_hex,
				target_object_type=excluded.target_object_type,
				target_object_id=excluded.target_object_id,
				status=excluded.status,
				idempotency_key=excluded.idempotency_key,
				updated_at_unix=excluded.updated_at_unix,
				note=excluded.note,
				payload_json=excluded.payload_json`,
			e.FrontOrderID,
			strings.TrimSpace(e.FrontType),
			strings.TrimSpace(e.FrontSubtype),
			strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex)),
			strings.TrimSpace(e.TargetObjectType),
			strings.TrimSpace(e.TargetObjectID),
			strings.TrimSpace(e.Status),
			e.IdempotencyKey,
			strings.TrimSpace(e.Note),
			mustJSONString(e.Payload),
			e.CreatedAtUnix,
			e.UpdatedAtUnix,
		)
		return err
	})
}

// dbGetFrontOrder 按 order_id 查询前台业务单
func dbGetFrontOrder(ctx context.Context, store *clientDB, frontOrderID string) (frontOrderItem, error) {
	if store == nil {
		return frontOrderItem{}, fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return frontOrderItem{}, fmt.Errorf("order_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (frontOrderItem, error) {
		var item frontOrderItem
		var payload string
		err := QueryRowContext(ctx, db, 
			`SELECT order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,note,payload_json
			 FROM orders WHERE order_id=?`,
			frontOrderID,
		).Scan(
			&item.FrontOrderID, &item.FrontType, &item.FrontSubtype, &item.OwnerPubkeyHex,
			&item.TargetObjectType, &item.TargetObjectID, &item.Status,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &item.Note, &payload,
		)
		if err != nil {
			return frontOrderItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListFrontOrders 查询前台业务单列表
func dbListFrontOrders(ctx context.Context, store *clientDB, f frontOrderFilter) (frontOrderPage, error) {
	if store == nil {
		return frontOrderPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (frontOrderPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.FrontOrderID != "" {
			where += " AND order_id=?"
			args = append(args, f.FrontOrderID)
		}
		if f.FrontType != "" {
			where += " AND order_type=?"
			args = append(args, f.FrontType)
		}
		if f.FrontSubtype != "" {
			where += " AND order_subtype=?"
			args = append(args, f.FrontSubtype)
		}
		if f.OwnerPubkeyHex != "" {
			where += " AND owner_pubkey_hex=?"
			args = append(args, strings.ToLower(strings.TrimSpace(f.OwnerPubkeyHex)))
		}
		if f.TargetObjectType != "" {
			where += " AND target_object_type=?"
			args = append(args, f.TargetObjectType)
		}
		if f.TargetObjectID != "" {
			where += " AND target_object_id=?"
			args = append(args, f.TargetObjectID)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		var out frontOrderPage
		if err := QueryRowContext(ctx, db, "SELECT COUNT(1) FROM orders WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return frontOrderPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := QueryContext(ctx, db, 
			`SELECT order_id,order_type,order_subtype,owner_pubkey_hex,target_object_type,target_object_id,status,created_at_unix,updated_at_unix,note,payload_json
			 FROM orders WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,order_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return frontOrderPage{}, err
		}
		defer rows.Close()
		out.Items = make([]frontOrderItem, 0, f.Limit)
		for rows.Next() {
			var item frontOrderItem
			var payload string
			if err := rows.Scan(
				&item.FrontOrderID, &item.FrontType, &item.FrontSubtype, &item.OwnerPubkeyHex,
				&item.TargetObjectType, &item.TargetObjectID, &item.Status,
				&item.CreatedAtUnix, &item.UpdatedAtUnix, &item.Note, &payload,
			); err != nil {
				return frontOrderPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, item)
		}
		if err := rows.Err(); err != nil {
			return frontOrderPage{}, err
		}
		return out, nil
	})
}

// dbUpdateFrontOrderStatus 更新前台业务单状态
func dbUpdateFrontOrderStatus(ctx context.Context, store *clientDB, frontOrderID string, status string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := ExecContext(ctx, db, 
			`UPDATE orders SET status=?, updated_at_unix=? WHERE order_id=?`,
			strings.TrimSpace(status),
			time.Now().Unix(),
			frontOrderID,
		)
		return err
	})
}
