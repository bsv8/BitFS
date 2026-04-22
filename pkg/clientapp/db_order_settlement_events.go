package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/ordersettlementevents"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/ordersettlements"
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
	OrderID           string          `json:"order_id"`
	SourceType        string          `json:"source_type"`
	SourceID          string          `json:"source_id"`
	AccountingScene   string          `json:"accounting_scene"`
	AccountingSubtype string          `json:"accounting_subtype"`
	EventType         string          `json:"event_type"`
	Status            string          `json:"status"`
	Note              string          `json:"note"`
	OccurredAtUnix    int64           `json:"occurred_at_unix"`
	Payload           json.RawMessage `json:"payload"`
}

// orderSettlementEventEntry 结算事件写入条目
type orderSettlementEventEntry struct {
	ProcessID         string
	SettlementID      string
	OrderID           string
	SourceType        string
	SourceID          string
	AccountingScene   string
	AccountingSubtype string
	EventType         string
	Status            string
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

// dbGetOrderSettlementEvent 按 id 查询事件。

// dbListOrderSettlementEvents 查询结算事件列表。

// dbAppendBusinessTrigger 旧触发器入口，新版本写入 order_settlement_events。

// dbGetBusinessTrigger 按 trigger_id 查询旧桥接记录。

// dbListBusinessTriggersByOrderID 按 order_id 查询旧桥接记录列表。

// dbListBusinessTriggers 查询旧桥接记录列表。

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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]string, error) {
		events, err := root.OrderSettlementEvents.Query().
			Where(
				ordersettlementevents.SourceTypeEQ(triggerType),
				ordersettlementevents.SourceIDEQ(triggerIDValue),
			).
			Order(
				ordersettlementevents.ByOccurredAtUnix(entsql.OrderDesc()),
				ordersettlementevents.ByID(entsql.OrderDesc()),
			).
			All(ctx)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]struct{}, len(events))
		out := make([]string, 0, len(events))
		for _, event := range events {
			settlement, err := root.OrderSettlements.Query().
				Where(ordersettlements.SettlementIDEQ(strings.TrimSpace(event.SettlementID))).
				Only(ctx)
			if err != nil {
				continue
			}
			orderID := strings.TrimSpace(settlement.OrderID)
			if orderID == "" {
				continue
			}
			if _, ok := seen[orderID]; ok {
				continue
			}
			seen[orderID] = struct{}{}
			out = append(out, orderID)
		}
		return out, nil
	})
}
