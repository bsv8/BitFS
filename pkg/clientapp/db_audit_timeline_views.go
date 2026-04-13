package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// 审计时间线只做读模型汇总，不改底层事实表。
// 统一先映射成同一种 item，再按视角筛选、排序、分页。

type AuditTimelineFilter struct {
	Limit            int
	Offset           int
	GatewayPubkeyHex string
	CommandID        string
}

type AuditTimelinePage struct {
	Total int
	Items []AuditTimelineItem
}

type AuditTimelineItem struct {
	OccurredAtUnix   int64           `json:"occurred_at_unix"`
	FactKind         string          `json:"fact_kind"`
	OriginKind       string          `json:"origin_kind"`
	GatewayPubkeyHex string          `json:"gateway_pubkey_hex,omitempty"`
	CommandID        string          `json:"command_id,omitempty"`
	RecordID         int64           `json:"record_id"`
	Title            string          `json:"title"`
	StateBefore      string          `json:"state_before,omitempty"`
	StateAfter       string          `json:"state_after,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

func ListGatewayAuditTimeline(ctx context.Context, store *clientDB, f AuditTimelineFilter) (AuditTimelinePage, error) {
	return dbListGatewayAuditTimeline(ctx, store, f)
}

func ListCommandAuditTimeline(ctx context.Context, store *clientDB, f AuditTimelineFilter) (AuditTimelinePage, error) {
	return dbListCommandAuditTimeline(ctx, store, f)
}

// ListGatewayAuditTimelineDB 仅给仓外调用方（如 e2e 协调层）保留的兼容入口。
// 设计说明：
// - 运行时主路径统一传入 store 能力，不再让业务直接创建 db 适配器；
// - 这里保留 DB 版本是为了避免跨仓改动耦合，不参与运行时依赖组装。
func ListGatewayAuditTimelineDB(ctx context.Context, db sqlConn, f AuditTimelineFilter) (AuditTimelinePage, error) {
	if db == nil {
		return AuditTimelinePage{}, fmt.Errorf("db is nil")
	}
	rawDB, ok := db.(*sql.DB)
	if !ok {
		return AuditTimelinePage{}, fmt.Errorf("db must be *sql.DB")
	}
	return dbListGatewayAuditTimeline(ctx, clientDBFromDB(rawDB), f)
}

// ListCommandAuditTimelineDB 仅给仓外调用方（如 e2e 协调层）保留的兼容入口。
func ListCommandAuditTimelineDB(ctx context.Context, db sqlConn, f AuditTimelineFilter) (AuditTimelinePage, error) {
	if db == nil {
		return AuditTimelinePage{}, fmt.Errorf("db is nil")
	}
	rawDB, ok := db.(*sql.DB)
	if !ok {
		return AuditTimelinePage{}, fmt.Errorf("db must be *sql.DB")
	}
	return dbListCommandAuditTimeline(ctx, clientDBFromDB(rawDB), f)
}

func dbListGatewayAuditTimeline(ctx context.Context, store *clientDB, f AuditTimelineFilter) (AuditTimelinePage, error) {
	if store == nil {
		return AuditTimelinePage{}, fmt.Errorf("client db is nil")
	}
	gatewayPubkeyHex := strings.TrimSpace(f.GatewayPubkeyHex)
	if gatewayPubkeyHex == "" {
		return AuditTimelinePage{}, fmt.Errorf("gateway_pubkey_hex is required")
	}
	items := make([]AuditTimelineItem, 0, 32)
	var err error
	if items, err = appendGatewayAuditTimelineFromCommandJournal(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendGatewayAuditTimelineFromGatewayEvents(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendGatewayAuditTimelineFromDomainEvents(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendGatewayAuditTimelineFromStateSnapshots(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendGatewayAuditTimelineFromEffectLogs(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendGatewayAuditTimelineFromObservedStates(ctx, store, gatewayPubkeyHex, items); err != nil {
		return AuditTimelinePage{}, err
	}
	sortAuditTimelineItems(items)
	return pageAuditTimelineItems(items, f.Limit, f.Offset), nil
}

func dbListCommandAuditTimeline(ctx context.Context, store *clientDB, f AuditTimelineFilter) (AuditTimelinePage, error) {
	if store == nil {
		return AuditTimelinePage{}, fmt.Errorf("client db is nil")
	}
	commandID := strings.TrimSpace(f.CommandID)
	if commandID == "" {
		return AuditTimelinePage{}, fmt.Errorf("command_id is required")
	}
	if _, _, err := loadCommandAuditTimelineRoot(ctx, store, commandID); err != nil {
		return AuditTimelinePage{}, err
	}
	items := make([]AuditTimelineItem, 0, 32)
	var err error
	if items, err = appendCommandAuditTimelineFromCommandJournal(ctx, store, commandID, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendCommandAuditTimelineFromGatewayEvents(ctx, store, commandID, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendCommandAuditTimelineFromDomainEvents(ctx, store, commandID, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendCommandAuditTimelineFromStateSnapshots(ctx, store, commandID, items); err != nil {
		return AuditTimelinePage{}, err
	}
	if items, err = appendCommandAuditTimelineFromEffectLogs(ctx, store, commandID, items); err != nil {
		return AuditTimelinePage{}, err
	}
	sortAuditTimelineItems(items)
	return pageAuditTimelineItems(items, f.Limit, f.Offset), nil
}

func appendGatewayAuditTimelineFromCommandJournal(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListCommandJournal(ctx, store, commandJournalFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, journal := range page.Items {
		items = append(items, auditTimelineFromCommandJournal(journal))
	}
	return items, nil
}

func appendGatewayAuditTimelineFromGatewayEvents(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListGatewayEvents(ctx, store, gatewayEventFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromGatewayEvent(it))
	}
	return items, nil
}

func appendGatewayAuditTimelineFromDomainEvents(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListDomainEvents(ctx, store, domainEventFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromDomainEvent(it))
	}
	return items, nil
}

func appendGatewayAuditTimelineFromStateSnapshots(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListStateSnapshots(ctx, store, stateSnapshotFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromStateSnapshot(it))
	}
	return items, nil
}

func appendGatewayAuditTimelineFromEffectLogs(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListEffectLogs(ctx, store, effectLogFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromEffectLog(it))
	}
	return items, nil
}

func appendGatewayAuditTimelineFromObservedStates(ctx context.Context, store *clientDB, gatewayPubkeyHex string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListObservedGatewayStates(ctx, store, observedGatewayStateFilter{GatewayPeerID: gatewayPubkeyHex, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromObservedState(it, ""))
	}
	return items, nil
}

func appendCommandAuditTimelineFromCommandJournal(ctx context.Context, store *clientDB, commandID string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListCommandJournal(ctx, store, commandJournalFilter{CommandID: commandID, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, journal := range page.Items {
		items = append(items, auditTimelineFromCommandJournal(journal))
	}
	return items, nil
}

func appendCommandAuditTimelineFromGatewayEvents(ctx context.Context, store *clientDB, commandID string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListGatewayEvents(ctx, store, gatewayEventFilter{CommandID: commandID, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromGatewayEvent(it))
	}
	return items, nil
}

func appendCommandAuditTimelineFromDomainEvents(ctx context.Context, store *clientDB, commandID string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListDomainEvents(ctx, store, domainEventFilter{CommandID: commandID, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromDomainEvent(it))
	}
	return items, nil
}

func appendCommandAuditTimelineFromStateSnapshots(ctx context.Context, store *clientDB, commandID string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListStateSnapshots(ctx, store, stateSnapshotFilter{CommandID: commandID, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromStateSnapshot(it))
	}
	return items, nil
}

func appendCommandAuditTimelineFromEffectLogs(ctx context.Context, store *clientDB, commandID string, items []AuditTimelineItem) ([]AuditTimelineItem, error) {
	page, err := dbListEffectLogs(ctx, store, effectLogFilter{CommandID: commandID, Limit: 1 << 30})
	if err != nil {
		return nil, err
	}
	for _, it := range page.Items {
		items = append(items, auditTimelineFromEffectLog(it))
	}
	return items, nil
}

func loadCommandAuditTimelineRoot(ctx context.Context, store *clientDB, commandID string) (int64, string, error) {
	page, err := dbListCommandJournal(ctx, store, commandJournalFilter{CommandID: commandID, Limit: 1})
	if err != nil {
		return 0, "", err
	}
	if len(page.Items) == 0 {
		return 0, "", sql.ErrNoRows
	}
	item := page.Items[0]
	return item.CreatedAtUnix, strings.TrimSpace(item.GatewayPeerID), nil
}

func auditTimelineFromCommandJournal(it commandJournalItem) AuditTimelineItem {
	return AuditTimelineItem{
		OccurredAtUnix:   it.CreatedAtUnix,
		FactKind:         "proc_command_journal",
		OriginKind:       "command",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(it.CommandID),
		RecordID:         it.ID,
		Title:            auditCommandJournalTitle(it),
		StateBefore:      strings.TrimSpace(it.StateBefore),
		StateAfter:       strings.TrimSpace(it.StateAfter),
		Payload:          auditCommandJournalPayload(it),
	}
}

func auditTimelineFromGatewayEvent(it gatewayEventItem) AuditTimelineItem {
	return AuditTimelineItem{
		OccurredAtUnix:   it.CreatedAtUnix,
		FactKind:         "gateway_event",
		OriginKind:       "command",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(it.CommandID),
		RecordID:         it.ID,
		Title:            auditGatewayEventTitle(it),
		Payload:          cloneJSONPayload(it.Payload),
	}
}

func auditTimelineFromDomainEvent(it domainEventItem) AuditTimelineItem {
	return AuditTimelineItem{
		OccurredAtUnix:   it.CreatedAtUnix,
		FactKind:         "domain_event",
		OriginKind:       "command",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(it.CommandID),
		RecordID:         it.ID,
		Title:            auditDomainEventTitle(it),
		StateBefore:      strings.TrimSpace(it.StateBefore),
		StateAfter:       strings.TrimSpace(it.StateAfter),
		Payload:          cloneJSONPayload(it.Payload),
	}
}

func auditTimelineFromStateSnapshot(it stateSnapshotItem) AuditTimelineItem {
	return AuditTimelineItem{
		OccurredAtUnix:   it.CreatedAtUnix,
		FactKind:         "state_snapshot",
		OriginKind:       "command",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(it.CommandID),
		RecordID:         it.ID,
		Title:            auditStateSnapshotTitle(it),
		StateAfter:       strings.TrimSpace(it.State),
		Payload:          cloneJSONPayload(it.Payload),
	}
}

func auditTimelineFromEffectLog(it effectLogItem) AuditTimelineItem {
	return AuditTimelineItem{
		OccurredAtUnix:   it.CreatedAtUnix,
		FactKind:         "effect_log",
		OriginKind:       "command",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(it.CommandID),
		RecordID:         it.ID,
		Title:            auditEffectLogTitle(it),
		Payload:          cloneJSONPayload(it.Payload),
	}
}

func auditTimelineFromObservedState(it observedGatewayStateItem, commandID string) AuditTimelineItem {
	out := AuditTimelineItem{
		OccurredAtUnix:   observedTimelineOccurredAt(it),
		FactKind:         "observed_gateway_state",
		OriginKind:       "observed",
		GatewayPubkeyHex: strings.TrimSpace(it.GatewayPeerID),
		CommandID:        strings.TrimSpace(commandID),
		RecordID:         it.ID,
		Title:            auditObservedStateTitle(it),
		StateBefore:      strings.TrimSpace(it.StateBefore),
		StateAfter:       strings.TrimSpace(it.StateAfter),
		Payload:          cloneJSONPayload(it.Payload),
	}
	if out.OccurredAtUnix <= 0 {
		out.OccurredAtUnix = it.CreatedAtUnix
	}
	return out
}

func observedTimelineOccurredAt(it observedGatewayStateItem) int64 {
	if it.ObservedAtUnix > 0 {
		return it.ObservedAtUnix
	}
	return it.CreatedAtUnix
}

func auditCommandJournalTitle(it commandJournalItem) string {
	commandType := strings.TrimSpace(it.CommandType)
	status := strings.TrimSpace(it.Status)
	if commandType == "" {
		commandType = "command"
	}
	if status == "" {
		return "命令 " + commandType
	}
	return "命令 " + commandType + " / " + status
}

func auditGatewayEventTitle(it gatewayEventItem) string {
	action := strings.TrimSpace(it.Action)
	if action == "" {
		action = "event"
	}
	return "网关事件 " + action
}

func auditDomainEventTitle(it domainEventItem) string {
	name := strings.TrimSpace(it.EventName)
	if name == "" {
		name = "event"
	}
	return "领域事件 " + name
}

func auditStateSnapshotTitle(it stateSnapshotItem) string {
	state := strings.TrimSpace(it.State)
	if state == "" {
		state = "snapshot"
	}
	return "状态快照 " + state
}

func auditEffectLogTitle(it effectLogItem) string {
	effectType := strings.TrimSpace(it.EffectType)
	stage := strings.TrimSpace(it.Stage)
	if effectType == "" {
		effectType = "effect"
	}
	if stage == "" {
		return "效果日志 " + effectType
	}
	return "效果日志 " + effectType + "/" + stage
}

func auditObservedStateTitle(it observedGatewayStateItem) string {
	eventName := strings.TrimSpace(it.EventName)
	if eventName == "" {
		eventName = "observed"
	}
	return "观察事实 " + eventName
}

func auditCommandJournalPayload(it commandJournalItem) json.RawMessage {
	payload := cloneJSONPayload(it.Payload)
	result := cloneJSONPayload(it.Result)
	out, err := json.Marshal(map[string]json.RawMessage{
		"payload": payload,
		"result":  result,
	})
	if err != nil {
		return json.RawMessage(`{"payload":{},"result":{}}`)
	}
	return json.RawMessage(out)
}

func cloneJSONPayload(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage(`{}`)
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return json.RawMessage(out)
}

func sortAuditTimelineItems(items []AuditTimelineItem) {
	sort.SliceStable(items, func(i, j int) bool {
		a := items[i]
		b := items[j]
		if a.OccurredAtUnix != b.OccurredAtUnix {
			return a.OccurredAtUnix < b.OccurredAtUnix
		}
		if auditTimelineKindPriority(a.FactKind) != auditTimelineKindPriority(b.FactKind) {
			return auditTimelineKindPriority(a.FactKind) < auditTimelineKindPriority(b.FactKind)
		}
		if a.RecordID != b.RecordID {
			return a.RecordID < b.RecordID
		}
		if a.CommandID != b.CommandID {
			return a.CommandID < b.CommandID
		}
		return a.FactKind < b.FactKind
	})
}

func auditTimelineKindPriority(factKind string) int {
	switch strings.TrimSpace(factKind) {
	case "proc_command_journal":
		return 1
	case "gateway_event":
		return 2
	case "domain_event":
		return 3
	case "state_snapshot":
		return 4
	case "effect_log":
		return 5
	case "observed_gateway_state":
		return 6
	default:
		return 99
	}
}

func pageAuditTimelineItems(items []AuditTimelineItem, limit int, offset int) AuditTimelinePage {
	total := len(items)
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = total
	}
	if offset > total {
		return AuditTimelinePage{Total: total, Items: []AuditTimelineItem{}}
	}
	end := offset + limit
	if end > total {
		end = total
	}
	out := make([]AuditTimelineItem, 0, end-offset)
	out = append(out, items[offset:end]...)
	return AuditTimelinePage{Total: total, Items: out}
}
