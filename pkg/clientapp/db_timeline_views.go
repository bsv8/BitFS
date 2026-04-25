package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	bitfsproccommandjournal "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/proccommandjournal"
	bitfsprocdomainevents "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procdomainevents"
	bitfsproceffectlogs "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/proceffectlogs"
	bitfsprocstatesnapshots "github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procstatesnapshots"
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
	GatewayEvents  []gatewayEventItem  `json:"proc_gateway_events,omitempty"`
	DomainEvents   []domainEventItem   `json:"proc_domain_events,omitempty"`
	StateSnapshots []stateSnapshotItem `json:"proc_state_snapshots,omitempty"`
	EffectLogs     []effectLogItem     `json:"proc_effect_logs,omitempty"`
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
	return readEntValue(ctx, store, func(root EntReadRoot) (commandTimelineItem, error) {
		journal, err := root.ProcCommandJournal.Query().Where(bitfsproccommandjournal.IDEQ(int(id))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return commandTimelineItem{}, sql.ErrNoRows
			}
			return commandTimelineItem{}, err
		}
		out := commandTimelineItem{commandJournalItem: commandJournalItemFromEnt(journal)}
		if err := hydrateCommandTimelineRelationsEnt(ctx, store, &out); err != nil {
			return commandTimelineItem{}, err
		}
		return out, nil
	})
}

func hydrateCommandTimelineRelationsEnt(ctx context.Context, store *clientDB, out *commandTimelineItem) error {
	if store == nil || out == nil || strings.TrimSpace(out.CommandID) == "" {
		return nil
	}
	commandID := out.CommandID
	out.GatewayEvents = nil
	out.DomainEvents = nil
	out.StateSnapshots = nil
	out.EffectLogs = nil

	if err := loadGatewayEventsByCommandIDEnt(ctx, store, commandID, &out.GatewayEvents); err != nil {
		return err
	}
	if err := loadDomainEventsByCommandIDEnt(ctx, store, commandID, &out.DomainEvents); err != nil {
		return err
	}
	if err := loadStateSnapshotsByCommandIDEnt(ctx, store, commandID, &out.StateSnapshots); err != nil {
		return err
	}
	if err := loadEffectLogsByCommandIDEnt(ctx, store, commandID, &out.EffectLogs); err != nil {
		return err
	}
	return nil
}

func loadGatewayEventsByCommandIDEnt(ctx context.Context, store *clientDB, commandID string, out *[]gatewayEventItem) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	gw, err := gatewayClientStoreFromDB(store)
	if err != nil {
		return err
	}
	events, err := gw.ListGatewayEvents(ctx, 0)
	if err != nil {
		return err
	}
	items := make([]gatewayEventItem, 0, len(events))
	for _, event := range events {
		if !strings.EqualFold(strings.TrimSpace(event.CommandID), strings.TrimSpace(commandID)) {
			continue
		}
		items = append(items, gatewayEventItemFromGatewayEvent(event))
	}
	*out = items
	return nil
}

func loadDomainEventsByCommandIDEnt(ctx context.Context, store *clientDB, commandID string, out *[]domainEventItem) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	items, err := readEntValue(ctx, store, func(root EntReadRoot) ([]domainEventItem, error) {
		nodes, err := root.ProcDomainEvents.Query().
			Where(bitfsprocdomainevents.CommandIDEQ(commandID)).
			Order(bitfsprocdomainevents.ByID(entsql.OrderAsc())).
			All(ctx)
		if err != nil {
			return nil, err
		}
		items := make([]domainEventItem, 0, len(nodes))
		for _, node := range nodes {
			items = append(items, domainEventItemFromEnt(node))
		}
		return items, nil
	})
	if err != nil {
		return err
	}
	*out = items
	return nil
}

func loadStateSnapshotsByCommandIDEnt(ctx context.Context, store *clientDB, commandID string, out *[]stateSnapshotItem) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	items, err := readEntValue(ctx, store, func(root EntReadRoot) ([]stateSnapshotItem, error) {
		nodes, err := root.ProcStateSnapshots.Query().
			Where(bitfsprocstatesnapshots.CommandIDEQ(commandID)).
			Order(bitfsprocstatesnapshots.ByID(entsql.OrderAsc())).
			All(ctx)
		if err != nil {
			return nil, err
		}
		items := make([]stateSnapshotItem, 0, len(nodes))
		for _, node := range nodes {
			items = append(items, stateSnapshotItemFromEnt(node))
		}
		return items, nil
	})
	if err != nil {
		return err
	}
	*out = items
	return nil
}

func loadEffectLogsByCommandIDEnt(ctx context.Context, store *clientDB, commandID string, out *[]effectLogItem) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	items, err := readEntValue(ctx, store, func(root EntReadRoot) ([]effectLogItem, error) {
		nodes, err := root.ProcEffectLogs.Query().
			Where(bitfsproceffectlogs.CommandIDEQ(commandID)).
			Order(bitfsproceffectlogs.ByID(entsql.OrderAsc())).
			All(ctx)
		if err != nil {
			return nil, err
		}
		items := make([]effectLogItem, 0, len(nodes))
		for _, node := range nodes {
			items = append(items, effectLogItemFromEnt(node))
		}
		return items, nil
	})
	if err != nil {
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
