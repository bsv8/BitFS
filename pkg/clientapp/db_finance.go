package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// financeBusinessFilter 业务查询过滤条件
// 设计说明：
//   - 主口径（唯一模型）：SourceType/SourceID/AccountingScene/AccountingSubtype
//   - 第六次迭代起新代码只使用主口径字段（旧列在已升级数据库中仍存在但不再引用）
//   - 所有查询统一使用主口径字段
type financeBusinessFilter struct {
	Limit  int
	Offset int

	// 主口径 - 唯一模型字段
	BusinessID        string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string

	Status      string
	FromPartyID string
	ToPartyID   string
	Query       string
}

type financeBusinessPage struct {
	Total int
	Items []financeBusinessItem
}

// financeBusinessItem 业务记录项
// 第六次迭代起只使用主口径字段
// 旧列在已升级数据库中仍存在，但新代码不再引用
type financeBusinessItem struct {
	BusinessID string `json:"business_id"`

	// 主口径 - 唯一模型字段
	SourceType        string `json:"source_type"`
	SourceID          string `json:"source_id"`
	AccountingScene   string `json:"accounting_scene"`
	AccountingSubtype string `json:"accounting_subtype"`

	FromPartyID    string          `json:"from_party_id"`
	ToPartyID      string          `json:"to_party_id"`
	Status         string          `json:"status"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

// financeProcessEventFilter 流程事件查询过滤条件
// 设计说明：
//   - 主口径（唯一模型）：SourceType/SourceID/AccountingScene/AccountingSubtype
//   - 第六次迭代起新代码只使用主口径字段（旧列在已升级数据库中仍存在但不再引用）
//   - 所有查询统一使用主口径字段
type financeProcessEventFilter struct {
	Limit  int
	Offset int

	// 主口径 - 唯一模型字段
	ProcessID         string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string

	EventType string
	Status    string
	Query     string
}

type financeProcessEventPage struct {
	Total int
	Items []financeProcessEventItem
}

// financeProcessEventItem 流程事件记录项
// 第六次迭代起只使用主口径字段
// 旧列在已升级数据库中仍存在，但新代码不再引用
type financeProcessEventItem struct {
	ID        int64  `json:"id"`
	ProcessID string `json:"process_id"`

	// 主口径 - 唯一模型字段
	SourceType        string `json:"source_type"`
	SourceID          string `json:"source_id"`
	AccountingScene   string `json:"accounting_scene"`
	AccountingSubtype string `json:"accounting_subtype"`

	EventType      string          `json:"event_type"`
	Status         string          `json:"status"`
	OccurredAtUnix int64           `json:"occurred_at_unix"`
	IdempotencyKey string          `json:"idempotency_key"`
	Note           string          `json:"note"`
	Payload        json.RawMessage `json:"payload"`
}

type financeBreakdownFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	Query      string
}

type financeBreakdownPage struct {
	Total int
	Items []financeBreakdownItem
}

type financeBreakdownItem struct {
	ID                 int64           `json:"id"`
	BusinessID         string          `json:"business_id"`
	TxID               string          `json:"txid"`
	TxRole             string          `json:"tx_role"`
	GrossInputSatoshi  int64           `json:"gross_input_satoshi"`
	ChangeBackSatoshi  int64           `json:"change_back_satoshi"`
	ExternalInSatoshi  int64           `json:"external_in_satoshi"`
	CounterpartyOutSat int64           `json:"counterparty_out_satoshi"`
	MinerFeeSatoshi    int64           `json:"miner_fee_satoshi"`
	NetOutSatoshi      int64           `json:"net_out_satoshi"`
	NetInSatoshi       int64           `json:"net_in_satoshi"`
	CreatedAtUnix      int64           `json:"created_at_unix"`
	Note               string          `json:"note"`
	Payload            json.RawMessage `json:"payload"`
}

type financeUTXOLinkFilter struct {
	Limit      int
	Offset     int
	BusinessID string
	TxID       string
	UTXOID     string
	TxRole     string
	IOSide     string
	UTXORole   string
	Query      string
}

type financeUTXOLinkPage struct {
	Total int
	Items []financeUTXOLinkItem
}

type financeUTXOLinkItem struct {
	ID            int64           `json:"id"`
	BusinessID    string          `json:"business_id"`
	TxID          string          `json:"txid"`
	UTXOID        string          `json:"utxo_id"`
	TxRole        string          `json:"tx_role"`
	IOSide        string          `json:"io_side"`
	UTXORole      string          `json:"utxo_role"`
	AmountSatoshi int64           `json:"amount_satoshi"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	Note          string          `json:"note"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListFinanceBusinesses(ctx context.Context, store *clientDB, f financeBusinessFilter) (financeBusinessPage, error) {
	if store == nil {
		return financeBusinessPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
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
		if f.Status != "" {
			where += " AND status=?"
			args = append(args, f.Status)
		}
		if f.FromPartyID != "" {
			where += " AND from_party_id=?"
			args = append(args, f.FromPartyID)
		}
		if f.ToPartyID != "" {
			where += " AND to_party_id=?"
			args = append(args, f.ToPartyID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (business_id LIKE ? OR note LIKE ? OR idempotency_key LIKE ? OR source_type LIKE ? OR source_id LIKE ? OR accounting_scene LIKE ? OR accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeBusinessPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_business WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeBusinessPage{}, err
		}
		rows, err := db.Query(
			`SELECT business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_business WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,business_id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBusinessPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBusinessItem, 0, f.Limit)
		for rows.Next() {
			var it financeBusinessItem
			var payload string
			if err := rows.Scan(&it.BusinessID, &it.SourceType, &it.SourceID, &it.AccountingScene, &it.AccountingSubtype, &it.FromPartyID, &it.ToPartyID, &it.Status, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeBusinessPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBusinessPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBusiness(ctx context.Context, store *clientDB, businessID string) (financeBusinessItem, error) {
	if store == nil {
		return financeBusinessItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBusinessItem, error) {
		var out financeBusinessItem
		var payload string
		err := db.QueryRow(
			`SELECT business_id,source_type,source_id,accounting_scene,accounting_subtype,from_party_id,to_party_id,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_business WHERE business_id=?`,
			businessID,
		).Scan(&out.BusinessID, &out.SourceType, &out.SourceID, &out.AccountingScene, &out.AccountingSubtype, &out.FromPartyID, &out.ToPartyID, &out.Status, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeBusinessItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceProcessEvents(ctx context.Context, store *clientDB, f financeProcessEventFilter) (financeProcessEventPage, error) {
	if store == nil {
		return financeProcessEventPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventPage, error) {
		where := ""
		args := make([]any, 0, 16)
		if f.ProcessID != "" {
			where += " AND process_id=?"
			args = append(args, f.ProcessID)
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
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (process_id LIKE ? OR note LIKE ? OR idempotency_key LIKE ? OR source_type LIKE ? OR source_id LIKE ? OR accounting_scene LIKE ? OR accounting_subtype LIKE ?)"
			args = append(args, like, like, like, like, like, like, like)
		}
		var out financeProcessEventPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_process_events WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeProcessEventPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_process_events WHERE 1=1`+where+` ORDER BY occurred_at_unix DESC,id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeProcessEventPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeProcessEventItem, 0, f.Limit)
		for rows.Next() {
			var it financeProcessEventItem
			var payload string
			if err := rows.Scan(&it.ID, &it.ProcessID, &it.SourceType, &it.SourceID, &it.AccountingScene, &it.AccountingSubtype, &it.EventType, &it.Status, &it.OccurredAtUnix, &it.IdempotencyKey, &it.Note, &payload); err != nil {
				return financeProcessEventPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeProcessEventPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceProcessEvent(ctx context.Context, store *clientDB, id int64) (financeProcessEventItem, error) {
	if store == nil {
		return financeProcessEventItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeProcessEventItem, error) {
		var out financeProcessEventItem
		var payload string
		err := db.QueryRow(
			`SELECT id,process_id,source_type,source_id,accounting_scene,accounting_subtype,event_type,status,occurred_at_unix,idempotency_key,note,payload_json
			 FROM fin_process_events WHERE id=?`,
			id,
		).Scan(&out.ID, &out.ProcessID, &out.SourceType, &out.SourceID, &out.AccountingScene, &out.AccountingSubtype, &out.EventType, &out.Status, &out.OccurredAtUnix, &out.IdempotencyKey, &out.Note, &payload)
		if err != nil {
			return financeProcessEventItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

// dbListFinanceBusinessesByPoolAllocationID 按 allocation_id 查财务业务
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 pool_allocations.id 过滤新口径记录。
func dbListFinanceBusinessesByPoolAllocationID(ctx context.Context, store *clientDB, allocationID string, limit, offset int) (financeBusinessPage, error) {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return financeBusinessPage{}, fmt.Errorf("allocation_id is required")
	}
	poolAllocID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return financeBusinessPage{Items: []financeBusinessItem{}}, nil
		}
		return financeBusinessPage{}, err
	}
	return dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:      limit,
		Offset:     offset,
		SourceType: "pool_allocation",
		SourceID:   fmt.Sprintf("%d", poolAllocID),
	})
}

// dbListFinanceProcessEventsByPoolAllocationID 按 allocation_id 查流程事件
// 设计说明：
// - allocation_id 这里只做便利查询输入，不直接落到 source_id；
// - 底层只按 pool_allocations.id 过滤新口径记录。
func dbListFinanceProcessEventsByPoolAllocationID(ctx context.Context, store *clientDB, allocationID string, limit, offset int) (financeProcessEventPage, error) {
	allocationID = strings.TrimSpace(allocationID)
	if allocationID == "" {
		return financeProcessEventPage{}, fmt.Errorf("allocation_id is required")
	}
	poolAllocID, err := dbGetPoolAllocationIDByAllocationID(ctx, store, allocationID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
		}
		return financeProcessEventPage{}, err
	}
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:      limit,
		Offset:     offset,
		SourceType: "pool_allocation",
		SourceID:   fmt.Sprintf("%d", poolAllocID),
	})
}

// dbListFinanceBusinessesByTxID 按 txid 查财务业务
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 chain_payments.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索。
func dbListFinanceBusinessesByTxID(ctx context.Context, store *clientDB, txid string, limit, offset int) (financeBusinessPage, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return financeBusinessPage{}, fmt.Errorf("txid is required")
	}
	// 先查 chain_payments.id，查不到返回空结果
	chainPaymentID, err := dbGetChainPaymentByTxID(ctx, store, txid)
	if err != nil {
		return financeBusinessPage{Items: []financeBusinessItem{}}, nil
	}
	// 使用 chain_payment id 查询
	return dbListFinanceBusinesses(ctx, store, financeBusinessFilter{
		Limit:      limit,
		Offset:     offset,
		SourceType: "chain_payment",
		SourceID:   fmt.Sprintf("%d", chainPaymentID),
	})
}

// dbListFinanceProcessEventsByTxID 按 txid 查流程事件
// 设计说明：
// - txid 这里只做便利查询输入，不直接作为 source_id；
// - 底层只按 chain_payments.id 过滤新口径记录；
// - 查不到 chain_payment 时返回空结果，不模糊搜索。
func dbListFinanceProcessEventsByTxID(ctx context.Context, store *clientDB, txid string, limit, offset int) (financeProcessEventPage, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return financeProcessEventPage{}, fmt.Errorf("txid is required")
	}
	// 先查 chain_payments.id，查不到返回空结果
	chainPaymentID, err := dbGetChainPaymentByTxID(ctx, store, txid)
	if err != nil {
		return financeProcessEventPage{Items: []financeProcessEventItem{}}, nil
	}
	// 使用 chain_payment id 查询
	return dbListFinanceProcessEvents(ctx, store, financeProcessEventFilter{
		Limit:      limit,
		Offset:     offset,
		SourceType: "chain_payment",
		SourceID:   fmt.Sprintf("%d", chainPaymentID),
	})
}

func dbListFinanceBreakdowns(ctx context.Context, store *clientDB, f financeBreakdownFilter) (financeBreakdownPage, error) {
	if store == nil {
		return financeBreakdownPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownPage, error) {
		where := ""
		args := make([]any, 0, 12)
		if f.BusinessID != "" {
			where += " AND business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND txid=?"
			args = append(args, f.TxID)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (business_id LIKE ? OR txid LIKE ? OR note LIKE ?)"
			args = append(args, like, like, like)
		}
		var out financeBreakdownPage
		if err := db.QueryRow("SELECT COUNT(1) FROM fin_tx_breakdown WHERE 1=1"+where, args...).Scan(&out.Total); err != nil {
			return financeBreakdownPage{}, err
		}
		rows, err := db.Query(
			`SELECT id,business_id,txid,tx_role,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
			 FROM fin_tx_breakdown WHERE 1=1`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeBreakdownPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeBreakdownItem, 0, f.Limit)
		for rows.Next() {
			var it financeBreakdownItem
			var payload string
			if err := rows.Scan(
				&it.ID, &it.BusinessID, &it.TxID, &it.TxRole, &it.GrossInputSatoshi, &it.ChangeBackSatoshi, &it.ExternalInSatoshi, &it.CounterpartyOutSat,
				&it.MinerFeeSatoshi, &it.NetOutSatoshi, &it.NetInSatoshi, &it.CreatedAtUnix, &it.Note, &payload,
			); err != nil {
				return financeBreakdownPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeBreakdownPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceBreakdown(ctx context.Context, store *clientDB, id int64) (financeBreakdownItem, error) {
	if store == nil {
		return financeBreakdownItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeBreakdownItem, error) {
		var out financeBreakdownItem
		var payload string
		err := db.QueryRow(
			`SELECT id,business_id,txid,tx_role,gross_input_satoshi,change_back_satoshi,external_in_satoshi,counterparty_out_satoshi,miner_fee_satoshi,net_out_satoshi,net_in_satoshi,created_at_unix,note,payload_json
			 FROM fin_tx_breakdown WHERE id=?`,
			id,
		).Scan(
			&out.ID, &out.BusinessID, &out.TxID, &out.TxRole, &out.GrossInputSatoshi, &out.ChangeBackSatoshi, &out.ExternalInSatoshi, &out.CounterpartyOutSat,
			&out.MinerFeeSatoshi, &out.NetOutSatoshi, &out.NetInSatoshi, &out.CreatedAtUnix, &out.Note, &payload,
		)
		if err != nil {
			return financeBreakdownItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}

func dbListFinanceUTXOLinks(ctx context.Context, store *clientDB, f financeUTXOLinkFilter) (financeUTXOLinkPage, error) {
	if store == nil {
		return financeUTXOLinkPage{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkPage, error) {
		where := ""
		args := make([]any, 0, 12)
		if f.BusinessID != "" {
			where += " AND l.business_id=?"
			args = append(args, f.BusinessID)
		}
		if f.TxID != "" {
			where += " AND l.txid=?"
			args = append(args, f.TxID)
		}
		if f.UTXOID != "" {
			where += " AND l.utxo_id=?"
			args = append(args, f.UTXOID)
		}
		if f.TxRole != "" {
			where += " AND b.tx_role=?"
			args = append(args, f.TxRole)
		}
		if f.IOSide != "" {
			where += " AND l.io_side=?"
			args = append(args, f.IOSide)
		}
		if f.UTXORole != "" {
			where += " AND l.utxo_role=?"
			args = append(args, f.UTXORole)
		}
		if f.Query != "" {
			like := "%" + f.Query + "%"
			where += " AND (l.business_id LIKE ? OR l.txid LIKE ? OR l.utxo_id LIKE ? OR l.note LIKE ? OR b.tx_role LIKE ? OR l.utxo_role LIKE ?)"
			args = append(args, like, like, like, like, like, like)
		}
		var out financeUTXOLinkPage
		if err := db.QueryRow(
			`SELECT COUNT(1)
			   FROM fin_tx_utxo_links l
			   JOIN fin_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE 1=1`+where,
			args...,
		).Scan(&out.Total); err != nil {
			return financeUTXOLinkPage{}, err
		}
		rows, err := db.Query(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,b.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM fin_tx_utxo_links l
			   JOIN fin_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE 1=1`+where+` ORDER BY l.id DESC LIMIT ? OFFSET ?`,
			append(args, f.Limit, f.Offset)...,
		)
		if err != nil {
			return financeUTXOLinkPage{}, err
		}
		defer rows.Close()
		out.Items = make([]financeUTXOLinkItem, 0, f.Limit)
		for rows.Next() {
			var it financeUTXOLinkItem
			var payload string
			if err := rows.Scan(&it.ID, &it.BusinessID, &it.TxID, &it.UTXOID, &it.TxRole, &it.IOSide, &it.UTXORole, &it.AmountSatoshi, &it.CreatedAtUnix, &it.Note, &payload); err != nil {
				return financeUTXOLinkPage{}, err
			}
			it.Payload = json.RawMessage(payload)
			out.Items = append(out.Items, it)
		}
		if err := rows.Err(); err != nil {
			return financeUTXOLinkPage{}, err
		}
		return out, nil
	})
}

func dbGetFinanceUTXOLink(ctx context.Context, store *clientDB, id int64) (financeUTXOLinkItem, error) {
	if store == nil {
		return financeUTXOLinkItem{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db *sql.DB) (financeUTXOLinkItem, error) {
		var out financeUTXOLinkItem
		var payload string
		err := db.QueryRow(
			`SELECT l.id,l.business_id,l.txid,l.utxo_id,b.tx_role,l.io_side,l.utxo_role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json
			   FROM fin_tx_utxo_links l
			   JOIN fin_tx_breakdown b ON b.business_id=l.business_id AND b.txid=l.txid
			  WHERE l.id=?`,
			id,
		).Scan(&out.ID, &out.BusinessID, &out.TxID, &out.UTXOID, &out.TxRole, &out.IOSide, &out.UTXORole, &out.AmountSatoshi, &out.CreatedAtUnix, &out.Note, &payload)
		if err != nil {
			return financeUTXOLinkItem{}, err
		}
		out.Payload = json.RawMessage(payload)
		return out, nil
	})
}
