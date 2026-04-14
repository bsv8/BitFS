package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	bitfsproccommandjournal "github.com/bsv8/bitfs-contract/ent/v1/gen/proccommandjournal"
	bitfsprocdomainevents "github.com/bsv8/bitfs-contract/ent/v1/gen/procdomainevents"
	bitfsproceffectlogs "github.com/bsv8/bitfs-contract/ent/v1/gen/proceffectlogs"
	bitfsprocobservedgatewaystates "github.com/bsv8/bitfs-contract/ent/v1/gen/procobservedgatewaystates"
	bitfsprocstatesnapshots "github.com/bsv8/bitfs-contract/ent/v1/gen/procstatesnapshots"
)

type commandJournalFilter struct {
	Limit         int
	Offset        int
	CommandTypes  []string
	CommandType   string
	GatewayPeerID string
	Status        string
	CommandID     string
	TriggerKey    string // 按来源链路键过滤，用于关联 orchestrator 日志
	Query         string
}

type commandJournalPage struct {
	Total int
	Items []commandJournalItem
}

type commandJournalItem struct {
	ID            int64  `json:"id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	CommandID     string `json:"command_id"`
	CommandType   string `json:"command_type"`
	GatewayPeerID string `json:"gateway_pubkey_hex"`
	AggregateID   string `json:"aggregate_id"`
	RequestedBy   string `json:"requested_by"`
	RequestedAt   int64  `json:"requested_at_unix"`
	Accepted      bool   `json:"accepted"`
	Status        string `json:"status"`
	ErrorCode     string `json:"error_code"`
	ErrorMessage  string `json:"error_message"`
	StateBefore   string `json:"state_before"`
	StateAfter    string `json:"state_after"`
	DurationMS    int64  `json:"duration_ms"`
	// TriggerKey 是来源链路键，不是命令主键；用于关联 proc_orchestrator_logs.idempotency_key
	TriggerKey string          `json:"trigger_key"`
	Payload    json.RawMessage `json:"payload"`
	Result     json.RawMessage `json:"result"`
}

type domainEventFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	EventName     string
}

type domainEventPage struct {
	Total int
	Items []domainEventItem
}

type domainEventItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	CommandID     string          `json:"command_id"`
	GatewayPeerID string          `json:"gateway_pubkey_hex"`
	EventName     string          `json:"event_name"`
	StateBefore   string          `json:"state_before"`
	StateAfter    string          `json:"state_after"`
	Payload       json.RawMessage `json:"payload"`
}

type stateSnapshotFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	State         string
}

type stateSnapshotPage struct {
	Total int
	Items []stateSnapshotItem
}

type stateSnapshotItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	CommandID     string          `json:"command_id"`
	GatewayPeerID string          `json:"gateway_pubkey_hex"`
	State         string          `json:"state"`
	PauseReason   string          `json:"pause_reason"`
	PauseNeedSat  uint64          `json:"pause_need_satoshi"`
	PauseHaveSat  uint64          `json:"pause_have_satoshi"`
	LastError     string          `json:"last_error"`
	Payload       json.RawMessage `json:"payload"`
}

type observedGatewayStateFilter struct {
	Limit         int
	Offset        int
	GatewayPeerID string
	SourceRef     string
	EventName     string
	State         string
}

type observedGatewayStatePage struct {
	Total int
	Items []observedGatewayStateItem
}

type observedGatewayStateItem struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	SourceRef      string          `json:"source_ref"`
	ObservedAtUnix int64           `json:"observed_at_unix"`
	EventName      string          `json:"event_name"`
	StateBefore    string          `json:"state_before"`
	StateAfter     string          `json:"state_after"`
	PauseReason    string          `json:"pause_reason"`
	PauseNeedSat   uint64          `json:"pause_need_satoshi"`
	PauseHaveSat   uint64          `json:"pause_have_satoshi"`
	LastError      string          `json:"last_error"`
	Payload        json.RawMessage `json:"payload"`
}

type effectLogFilter struct {
	Limit         int
	Offset        int
	CommandID     string
	GatewayPeerID string
	EffectType    string
	Stage         string
	Status        string
}

type effectLogPage struct {
	Total int
	Items []effectLogItem
}

type effectLogItem struct {
	ID            int64           `json:"id"`
	CreatedAtUnix int64           `json:"created_at_unix"`
	CommandID     string          `json:"command_id"`
	GatewayPeerID string          `json:"gateway_pubkey_hex"`
	EffectType    string          `json:"effect_type"`
	Stage         string          `json:"stage"`
	Status        string          `json:"status"`
	ErrorMessage  string          `json:"error_message"`
	Payload       json.RawMessage `json:"payload"`
}

func dbListCommandJournal(ctx context.Context, store *clientDB, f commandJournalFilter) (commandJournalPage, error) {
	if store == nil {
		return commandJournalPage{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return commandJournalPage{}, fmt.Errorf("client db ent client is nil")
	}
	q := store.ent.ProcCommandJournal.Query()
	if len(f.CommandTypes) > 0 {
		q = q.Where(bitfsproccommandjournal.CommandTypeIn(f.CommandTypes...))
	}
	if f.CommandType != "" {
		q = q.Where(bitfsproccommandjournal.CommandTypeEQ(f.CommandType))
	}
	if f.GatewayPeerID != "" {
		q = q.Where(bitfsproccommandjournal.GatewayPubkeyHexEQ(f.GatewayPeerID))
	}
	if f.Status != "" {
		q = q.Where(bitfsproccommandjournal.StatusEQ(f.Status))
	}
	if f.CommandID != "" {
		q = q.Where(bitfsproccommandjournal.CommandIDEQ(f.CommandID))
	}
	if f.TriggerKey != "" {
		q = q.Where(bitfsproccommandjournal.TriggerKeyEQ(f.TriggerKey))
	}
	if f.Query != "" {
		q = q.Where(bitfsproccommandjournal.Or(
			bitfsproccommandjournal.ErrorMessageContainsFold(f.Query),
			bitfsproccommandjournal.CommandIDContainsFold(f.Query),
			bitfsproccommandjournal.RequestedByContainsFold(f.Query),
			bitfsproccommandjournal.StateBeforeContainsFold(f.Query),
			bitfsproccommandjournal.StateAfterContainsFold(f.Query),
		))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return commandJournalPage{}, err
	}
	nodes, err := q.Order(bitfsproccommandjournal.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
	if err != nil {
		return commandJournalPage{}, err
	}
	out := commandJournalPage{
		Total: total,
		Items: make([]commandJournalItem, 0, len(nodes)),
	}
	for _, node := range nodes {
		out.Items = append(out.Items, commandJournalItemFromEnt(node))
	}
	return out, nil
}

func dbGetCommandJournalItem(ctx context.Context, store *clientDB, id int64) (commandJournalItem, error) {
	if store == nil {
		return commandJournalItem{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return commandJournalItem{}, fmt.Errorf("client db ent client is nil")
	}
	node, err := store.ent.ProcCommandJournal.Query().Where(bitfsproccommandjournal.IDEQ(int(id))).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return commandJournalItem{}, sql.ErrNoRows
		}
		return commandJournalItem{}, err
	}
	return commandJournalItemFromEnt(node), nil
}

func dbListDomainEvents(ctx context.Context, store *clientDB, f domainEventFilter) (domainEventPage, error) {
	if store == nil {
		return domainEventPage{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return domainEventPage{}, fmt.Errorf("client db ent client is nil")
	}
	q := store.ent.ProcDomainEvents.Query()
	if f.CommandID != "" {
		q = q.Where(bitfsprocdomainevents.CommandIDEQ(f.CommandID))
	}
	if f.GatewayPeerID != "" {
		q = q.Where(bitfsprocdomainevents.GatewayPubkeyHexEQ(f.GatewayPeerID))
	}
	if f.EventName != "" {
		q = q.Where(bitfsprocdomainevents.EventNameEQ(f.EventName))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return domainEventPage{}, err
	}
	nodes, err := q.Order(bitfsprocdomainevents.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
	if err != nil {
		return domainEventPage{}, err
	}
	out := domainEventPage{
		Total: total,
		Items: make([]domainEventItem, 0, len(nodes)),
	}
	for _, node := range nodes {
		out.Items = append(out.Items, domainEventItemFromEnt(node))
	}
	return out, nil
}

func dbGetDomainEventItem(ctx context.Context, store *clientDB, id int64) (domainEventItem, error) {
	if store == nil {
		return domainEventItem{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return domainEventItem{}, fmt.Errorf("client db ent client is nil")
	}
	node, err := store.ent.ProcDomainEvents.Query().Where(bitfsprocdomainevents.IDEQ(int(id))).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return domainEventItem{}, sql.ErrNoRows
		}
		return domainEventItem{}, err
	}
	return domainEventItemFromEnt(node), nil
}

func dbListStateSnapshots(ctx context.Context, store *clientDB, f stateSnapshotFilter) (stateSnapshotPage, error) {
	if store == nil {
		return stateSnapshotPage{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return stateSnapshotPage{}, fmt.Errorf("client db ent client is nil")
	}
	q := store.ent.ProcStateSnapshots.Query()
	if f.CommandID != "" {
		q = q.Where(bitfsprocstatesnapshots.CommandIDEQ(f.CommandID))
	}
	if f.GatewayPeerID != "" {
		q = q.Where(bitfsprocstatesnapshots.GatewayPubkeyHexEQ(f.GatewayPeerID))
	}
	if f.State != "" {
		q = q.Where(bitfsprocstatesnapshots.StateEQ(f.State))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return stateSnapshotPage{}, err
	}
	nodes, err := q.Order(bitfsprocstatesnapshots.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
	if err != nil {
		return stateSnapshotPage{}, err
	}
	out := stateSnapshotPage{
		Total: total,
		Items: make([]stateSnapshotItem, 0, len(nodes)),
	}
	for _, node := range nodes {
		out.Items = append(out.Items, stateSnapshotItemFromEnt(node))
	}
	return out, nil
}

func dbGetStateSnapshotItem(ctx context.Context, store *clientDB, id int64) (stateSnapshotItem, error) {
	if store == nil {
		return stateSnapshotItem{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return stateSnapshotItem{}, fmt.Errorf("client db ent client is nil")
	}
	node, err := store.ent.ProcStateSnapshots.Query().Where(bitfsprocstatesnapshots.IDEQ(int(id))).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return stateSnapshotItem{}, sql.ErrNoRows
		}
		return stateSnapshotItem{}, err
	}
	return stateSnapshotItemFromEnt(node), nil
}

func dbListObservedGatewayStates(ctx context.Context, store *clientDB, f observedGatewayStateFilter) (observedGatewayStatePage, error) {
	if store == nil {
		return observedGatewayStatePage{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return observedGatewayStatePage{}, fmt.Errorf("client db ent client is nil")
	}
	q := store.ent.ProcObservedGatewayStates.Query()
	if f.GatewayPeerID != "" {
		q = q.Where(bitfsprocobservedgatewaystates.GatewayPubkeyHexEQ(f.GatewayPeerID))
	}
	if f.SourceRef != "" {
		q = q.Where(bitfsprocobservedgatewaystates.SourceRefEQ(f.SourceRef))
	}
	if f.EventName != "" {
		q = q.Where(bitfsprocobservedgatewaystates.EventNameEQ(f.EventName))
	}
	if f.State != "" {
		q = q.Where(bitfsprocobservedgatewaystates.StateAfterEQ(f.State))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return observedGatewayStatePage{}, err
	}
	nodes, err := q.Order(bitfsprocobservedgatewaystates.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
	if err != nil {
		return observedGatewayStatePage{}, err
	}
	out := observedGatewayStatePage{
		Total: total,
		Items: make([]observedGatewayStateItem, 0, len(nodes)),
	}
	for _, node := range nodes {
		out.Items = append(out.Items, observedGatewayStateItemFromEnt(node))
	}
	return out, nil
}

func dbGetObservedGatewayStateItem(ctx context.Context, store *clientDB, id int64) (observedGatewayStateItem, error) {
	if store == nil {
		return observedGatewayStateItem{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return observedGatewayStateItem{}, fmt.Errorf("client db ent client is nil")
	}
	node, err := store.ent.ProcObservedGatewayStates.Query().Where(bitfsprocobservedgatewaystates.IDEQ(int(id))).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return observedGatewayStateItem{}, sql.ErrNoRows
		}
		return observedGatewayStateItem{}, err
	}
	return observedGatewayStateItemFromEnt(node), nil
}

func dbListEffectLogs(ctx context.Context, store *clientDB, f effectLogFilter) (effectLogPage, error) {
	if store == nil {
		return effectLogPage{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return effectLogPage{}, fmt.Errorf("client db ent client is nil")
	}
	q := store.ent.ProcEffectLogs.Query()
	if f.CommandID != "" {
		q = q.Where(bitfsproceffectlogs.CommandIDEQ(f.CommandID))
	}
	if f.GatewayPeerID != "" {
		q = q.Where(bitfsproceffectlogs.GatewayPubkeyHexEQ(f.GatewayPeerID))
	}
	if f.EffectType != "" {
		q = q.Where(bitfsproceffectlogs.EffectTypeEQ(f.EffectType))
	}
	if f.Stage != "" {
		q = q.Where(bitfsproceffectlogs.StageEQ(f.Stage))
	}
	if f.Status != "" {
		q = q.Where(bitfsproceffectlogs.StatusEQ(f.Status))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return effectLogPage{}, err
	}
	nodes, err := q.Order(bitfsproceffectlogs.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
	if err != nil {
		return effectLogPage{}, err
	}
	out := effectLogPage{
		Total: total,
		Items: make([]effectLogItem, 0, len(nodes)),
	}
	for _, node := range nodes {
		out.Items = append(out.Items, effectLogItemFromEnt(node))
	}
	return out, nil
}

func dbGetEffectLogItem(ctx context.Context, store *clientDB, id int64) (effectLogItem, error) {
	if store == nil {
		return effectLogItem{}, fmt.Errorf("client db is nil")
	}
	if store.ent == nil {
		return effectLogItem{}, fmt.Errorf("client db ent client is nil")
	}
	node, err := store.ent.ProcEffectLogs.Query().Where(bitfsproceffectlogs.IDEQ(int(id))).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return effectLogItem{}, sql.ErrNoRows
		}
		return effectLogItem{}, err
	}
	return effectLogItemFromEnt(node), nil
}

func commandJournalItemFromEnt(node *gen.ProcCommandJournal) commandJournalItem {
	if node == nil {
		return commandJournalItem{}
	}
	return commandJournalItem{
		ID:            int64(node.ID),
		CreatedAtUnix: node.CreatedAtUnix,
		CommandID:     node.CommandID,
		CommandType:   node.CommandType,
		GatewayPeerID: node.GatewayPubkeyHex,
		AggregateID:   node.AggregateID,
		RequestedBy:   node.RequestedBy,
		RequestedAt:   node.RequestedAtUnix,
		Accepted:      node.Accepted != 0,
		Status:        node.Status,
		ErrorCode:     node.ErrorCode,
		ErrorMessage:  node.ErrorMessage,
		StateBefore:   node.StateBefore,
		StateAfter:    node.StateAfter,
		DurationMS:    node.DurationMs,
		TriggerKey:    node.TriggerKey,
		Payload:       json.RawMessage(node.PayloadJSON),
		Result:        json.RawMessage(node.ResultJSON),
	}
}

func domainEventItemFromEnt(node *gen.ProcDomainEvents) domainEventItem {
	if node == nil {
		return domainEventItem{}
	}
	return domainEventItem{
		ID:            int64(node.ID),
		CreatedAtUnix: node.CreatedAtUnix,
		CommandID:     node.CommandID,
		GatewayPeerID: node.GatewayPubkeyHex,
		EventName:     node.EventName,
		StateBefore:   node.StateBefore,
		StateAfter:    node.StateAfter,
		Payload:       json.RawMessage(node.PayloadJSON),
	}
}

func stateSnapshotItemFromEnt(node *gen.ProcStateSnapshots) stateSnapshotItem {
	if node == nil {
		return stateSnapshotItem{}
	}
	return stateSnapshotItem{
		ID:            int64(node.ID),
		CreatedAtUnix: node.CreatedAtUnix,
		CommandID:     node.CommandID,
		GatewayPeerID: node.GatewayPubkeyHex,
		State:         node.State,
		PauseReason:   node.PauseReason,
		PauseNeedSat:  uint64(node.PauseNeedSatoshi),
		PauseHaveSat:  uint64(node.PauseHaveSatoshi),
		LastError:     node.LastError,
		Payload:       json.RawMessage(node.PayloadJSON),
	}
}

func observedGatewayStateItemFromEnt(node *gen.ProcObservedGatewayStates) observedGatewayStateItem {
	if node == nil {
		return observedGatewayStateItem{}
	}
	return observedGatewayStateItem{
		ID:             int64(node.ID),
		CreatedAtUnix:  node.CreatedAtUnix,
		GatewayPeerID:  node.GatewayPubkeyHex,
		SourceRef:      node.SourceRef,
		ObservedAtUnix: node.ObservedAtUnix,
		EventName:      node.EventName,
		StateBefore:    node.StateBefore,
		StateAfter:     node.StateAfter,
		PauseReason:    node.PauseReason,
		PauseNeedSat:   uint64(node.PauseNeedSatoshi),
		PauseHaveSat:   uint64(node.PauseHaveSatoshi),
		LastError:      node.LastError,
		Payload:        json.RawMessage(node.PayloadJSON),
	}
}

func effectLogItemFromEnt(node *gen.ProcEffectLogs) effectLogItem {
	if node == nil {
		return effectLogItem{}
	}
	return effectLogItem{
		ID:            int64(node.ID),
		CreatedAtUnix: node.CreatedAtUnix,
		CommandID:     node.CommandID,
		GatewayPeerID: node.GatewayPubkeyHex,
		EffectType:    node.EffectType,
		Stage:         node.Stage,
		Status:        node.Status,
		ErrorMessage:  node.ErrorMessage,
		Payload:       json.RawMessage(node.PayloadJSON),
	}
}

type scanCommandJournal interface {
	Scan(dest ...any) error
}

type scanDomainEvent interface {
	Scan(dest ...any) error
}

type scanStateSnapshot interface {
	Scan(dest ...any) error
}

type scanObservedGatewayState interface {
	Scan(dest ...any) error
}

type scanEffectLog interface {
	Scan(dest ...any) error
}
