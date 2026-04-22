package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/orders"
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		existing, err := tx.Orders.Query().
			Where(orders.OrderIDEQ(e.FrontOrderID)).
			Only(ctx)
		if err == nil {
			_, err = existing.Update().
				SetOrderType(strings.TrimSpace(e.FrontType)).
				SetOrderSubtype(strings.TrimSpace(e.FrontSubtype)).
				SetOwnerPubkeyHex(strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))).
				SetTargetObjectType(strings.TrimSpace(e.TargetObjectType)).
				SetTargetObjectID(strings.TrimSpace(e.TargetObjectID)).
				SetStatus(strings.TrimSpace(e.Status)).
				SetUpdatedAtUnix(e.UpdatedAtUnix).
				SetNote(strings.TrimSpace(e.Note)).
				SetPayloadJSON(mustJSONString(e.Payload)).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.Orders.Create().
			SetOrderID(e.FrontOrderID).
			SetOrderType(strings.TrimSpace(e.FrontType)).
			SetOrderSubtype(strings.TrimSpace(e.FrontSubtype)).
			SetOwnerPubkeyHex(strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))).
			SetTargetObjectType(strings.TrimSpace(e.TargetObjectType)).
			SetTargetObjectID(strings.TrimSpace(e.TargetObjectID)).
			SetStatus(strings.TrimSpace(e.Status)).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			SetCreatedAtUnix(e.CreatedAtUnix).
			SetUpdatedAtUnix(e.UpdatedAtUnix).
			Save(ctx)
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
	return readEntValue(ctx, store, func(root EntReadRoot) (frontOrderItem, error) {
		var item frontOrderItem
		row, err := root.Orders.Query().
			Where(orders.OrderIDEQ(frontOrderID)).
			Only(ctx)
		if err != nil {
			return frontOrderItem{}, err
		}
		item.FrontOrderID = row.OrderID
		item.FrontType = row.OrderType
		item.FrontSubtype = row.OrderSubtype
		item.OwnerPubkeyHex = row.OwnerPubkeyHex
		item.TargetObjectType = row.TargetObjectType
		item.TargetObjectID = row.TargetObjectID
		item.Status = row.Status
		item.CreatedAtUnix = row.CreatedAtUnix
		item.UpdatedAtUnix = row.UpdatedAtUnix
		item.Note = row.Note
		item.Payload = json.RawMessage(row.PayloadJSON)
		return item, nil
	})
}

// dbListFrontOrders 查询前台业务单列表

// dbUpdateFrontOrderStatus 更新前台业务单状态
func dbUpdateFrontOrderStatus(ctx context.Context, store *clientDB, frontOrderID string, status string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		affected, err := tx.Orders.Update().
			Where(orders.OrderIDEQ(frontOrderID)).
			SetStatus(strings.TrimSpace(status)).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("order_id %s not found", frontOrderID)
		}
		return nil
	})
}
