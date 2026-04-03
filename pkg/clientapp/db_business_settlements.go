package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// businessSettlementItem 业务结算出口记录
// 职责：表达一条 business 的统一结算出口
type businessSettlementItem struct {
	SettlementID     string          `json:"settlement_id"`
	BusinessID       string          `json:"business_id"`
	SettlementMethod string          `json:"settlement_method"`
	Status           string          `json:"status"`
	TargetType       string          `json:"target_type"`
	TargetID         string          `json:"target_id"`
	ErrorMessage     string          `json:"error_message"`
	CreatedAtUnix    int64           `json:"created_at_unix"`
	UpdatedAtUnix    int64           `json:"updated_at_unix"`
	Payload          json.RawMessage `json:"payload"`
}

// businessSettlementEntry 业务结算出口写入条目
type businessSettlementEntry struct {
	SettlementID     string
	BusinessID       string
	SettlementMethod string
	Status           string
	TargetType       string
	TargetID         string
	ErrorMessage     string
	CreatedAtUnix    int64
	UpdatedAtUnix    int64
	Payload          any
}

// businessSettlementFilter 业务结算出口查询过滤条件
type businessSettlementFilter struct {
	Limit            int
	Offset           int
	SettlementID     string
	BusinessID       string
	SettlementMethod string
	Status           string
	TargetType       string
	TargetID         string
}

// businessSettlementPage 业务结算出口分页结果
type businessSettlementPage struct {
	Total int
	Items []businessSettlementItem
}

// validateSettlementMethod 验证结算方式是否合法
func validateSettlementMethod(method string) error {
	m := strings.TrimSpace(method)
	if m == string(SettlementMethodPool) || m == string(SettlementMethodChain) {
		return nil
	}
	return fmt.Errorf("invalid settlement_method: %s, must be 'pool' or 'chain'", method)
}

// dbUpsertBusinessSettlement 插入或更新业务结算出口
// 幂等设计：同一 settlement_id 重复写入时更新非主键字段
// 约束：business_id 唯一，一条 business 只对应一条主 settlement
// 校验：settlement_method 只允许 'pool' 或 'chain'
func dbUpsertBusinessSettlement(ctx context.Context, store *clientDB, e businessSettlementEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	e.SettlementID = strings.TrimSpace(e.SettlementID)
	if e.SettlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	e.BusinessID = strings.TrimSpace(e.BusinessID)
	if e.BusinessID == "" {
		return fmt.Errorf("business_id is required")
	}
	if err := validateSettlementMethod(e.SettlementMethod); err != nil {
		return err
	}
	now := time.Now().Unix()
	if e.CreatedAtUnix <= 0 {
		e.CreatedAtUnix = now
	}
	if e.UpdatedAtUnix <= 0 {
		e.UpdatedAtUnix = now
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO business_settlements(
				settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_id) DO UPDATE SET
				settlement_method=excluded.settlement_method,
				status=excluded.status,
				target_type=excluded.target_type,
				target_id=excluded.target_id,
				error_message=excluded.error_message,
				updated_at_unix=excluded.updated_at_unix,
				payload_json=excluded.payload_json`,
			e.SettlementID,
			e.BusinessID,
			strings.TrimSpace(e.SettlementMethod),
			strings.TrimSpace(e.Status),
			strings.TrimSpace(e.TargetType),
			strings.TrimSpace(e.TargetID),
			strings.TrimSpace(e.ErrorMessage),
			e.CreatedAtUnix,
			e.UpdatedAtUnix,
			mustJSONString(e.Payload),
		)
		return err
	})
}

// dbGetBusinessSettlement 按 settlement_id 查询业务结算出口
func dbGetBusinessSettlement(ctx context.Context, store *clientDB, settlementID string) (businessSettlementItem, error) {
	if store == nil {
		return businessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return businessSettlementItem{}, fmt.Errorf("settlement_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessSettlementItem, error) {
		var item businessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE settlement_id=?`,
			settlementID,
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return businessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbGetBusinessSettlementByBusinessID 按 business_id 查询业务结算出口
func dbGetBusinessSettlementByBusinessID(ctx context.Context, store *clientDB, businessID string) (businessSettlementItem, error) {
	if store == nil {
		return businessSettlementItem{}, fmt.Errorf("client db is nil")
	}
	businessID = strings.TrimSpace(businessID)
	if businessID == "" {
		return businessSettlementItem{}, fmt.Errorf("business_id is required")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessSettlementItem, error) {
		var item businessSettlementItem
		var payload string
		err := db.QueryRow(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE business_id=?`,
			businessID,
		).Scan(
			&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
			&item.TargetType, &item.TargetID, &item.ErrorMessage,
			&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
		)
		if err != nil {
			return businessSettlementItem{}, err
		}
		item.Payload = json.RawMessage(payload)
		return item, nil
	})
}

// dbListBusinessSettlementsByTarget 按 target_type + target_id 查询业务结算出口列表
func dbListBusinessSettlementsByTarget(ctx context.Context, store *clientDB, targetType, targetID string, limit, offset int) (businessSettlementPage, error) {
	if store == nil {
		return businessSettlementPage{}, fmt.Errorf("client db is nil")
	}
	return dbListBusinessSettlements(ctx, store, businessSettlementFilter{
		TargetType: targetType,
		TargetID:   targetID,
		Limit:      limit,
		Offset:     offset,
	})
}

// dbListBusinessSettlements 查询业务结算出口列表
func dbListBusinessSettlements(ctx context.Context, store *clientDB, f businessSettlementFilter) (businessSettlementPage, error) {
	if store == nil {
		return businessSettlementPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (businessSettlementPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.SettlementID != "" {
			where += " AND settlement_id=?"
			args = append(args, f.SettlementID)
		}
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.SettlementMethod != "" {
			where += " AND settlement_method=?"
			args = append(args, f.SettlementMethod)
		}
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.TargetType != "" {
			where += " AND target_type=?"
			args = append(args, f.TargetType)
		}
		if f.TargetID != "" {
			where += " AND target_id=?"
			args = append(args, f.TargetID)
		}
		var out businessSettlementPage
		if err := db.QueryRow("SELECT COUNT(1) FROM business_settlements WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return businessSettlementPage{}, err
		}
		if f.Limit <= 0 {
			f.Limit = 20
		}
		rows, err := db.Query(
			`SELECT settlement_id,business_id,settlement_method,status,target_type,target_id,error_message,created_at_unix,updated_at_unix,payload_json
			 FROM business_settlements WHERE 1=1`+where+` ORDER BY updated_at_unix DESC,settlement_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return businessSettlementPage{}, err
		}
		defer rows.Close()
		out.Items = make([]businessSettlementItem, 0, f.Limit)
		for rows.Next() {
			var item businessSettlementItem
			var payload string
			if err := rows.Scan(
				&item.SettlementID, &item.BusinessID, &item.SettlementMethod, &item.Status,
				&item.TargetType, &item.TargetID, &item.ErrorMessage,
				&item.CreatedAtUnix, &item.UpdatedAtUnix, &payload,
			); err != nil {
				return businessSettlementPage{}, err
			}
			item.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, item)
		}
		if err := rows.Err(); err != nil {
			return businessSettlementPage{}, err
		}
		return out, nil
	})
}

// dbUpdateBusinessSettlementStatus 更新业务结算出口状态
func dbUpdateBusinessSettlementStatus(ctx context.Context, store *clientDB, settlementID string, status string, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	settlementID = strings.TrimSpace(settlementID)
	if settlementID == "" {
		return fmt.Errorf("settlement_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE business_settlements SET status=?, error_message=?, updated_at_unix=? WHERE settlement_id=?`,
			strings.TrimSpace(status),
			strings.TrimSpace(errorMessage),
			time.Now().Unix(),
			settlementID,
		)
		return err
	})
}

// dbUpdateBusinessSettlementStatusByBusinessID 按 business_id 更新业务结算出口状态
func dbUpdateBusinessSettlementStatusByBusinessID(ctx context.Context, store *clientDB, businessID string, status string, errorMessage string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	businessID = strings.TrimSpace(businessID)
	if businessID == "" {
		return fmt.Errorf("business_id is required")
	}
	return store.Do(ctx, func(db *sql.DB) error {
		_, err := db.Exec(
			`UPDATE business_settlements SET status=?, error_message=?, updated_at_unix=? WHERE business_id=?`,
			strings.TrimSpace(status),
			strings.TrimSpace(errorMessage),
			time.Now().Unix(),
			businessID,
		)
		return err
	})
}
