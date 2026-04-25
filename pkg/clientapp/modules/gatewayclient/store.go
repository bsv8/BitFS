package gatewayclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb/gen/procgatewayevents"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb/gen/procnodereachabilitycache"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb/gen/procobservedgatewaystates"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/gatewayclient/storedb/gen/procselfnodereachabilitystate"
)

// GatewayEvent 是 gateway 事件的存储模型。
// 设计说明：
// - 这里保存的是模块自己的事实，不再让 root 直接摸表；
// - Payload 保留原始 JSON 字节，便于 HTTP/OBS 原样回放；
// - ID 使用 ent 主键，CommandID 保持业务主键语义。
type GatewayEvent struct {
	ID            int64
	CreatedAtUnix  int64
	GatewayPubkeyHex string
	CommandID     string
	Action        string
	MsgID         string
	SequenceNum   uint32
	PoolID        string
	AmountSatoshi int64
	Payload       []byte
}

// ObservedGatewayState 是观察到的 gateway 状态的存储模型。
type ObservedGatewayState struct {
	ID            int64
	CreatedAtUnix int64
	GatewayPubkeyHex string
	SourceRef     string
	ObservedAtUnix int64
	EventName     string
	StateBefore   string
	StateAfter    string
	PauseReason   string
	PauseNeedSatoshi int64
	PauseHaveSatoshi int64
	LastError     string
	Payload       []byte
}

// NodeReachabilityCache 是节点可达性缓存的存储模型。
type NodeReachabilityCache struct {
	ID            int64
	NodePubkeyHex string
	Multiaddrs    []string
	HeadHeight    uint64
	Seq           uint64
	PublishedAtUnix int64
	ExpiresAtUnix int64
	Signature     []byte
	SourceGatewayPubkeyHex string
	UpdatedAtUnix int64
}

// SelfNodeReachabilityState 是本节点可达性状态的存储模型。
type SelfNodeReachabilityState struct {
	ID            int64
	NodePubkeyHex string
	HeadHeight    uint64
	Seq           uint64
	LastAnnouncedAt int64
	ExpiresAtUnix int64
	UpdatedAtUnix int64
}

// Store 是 gatewayclient 模块的持久化能力。
type Store interface {
	SaveGatewayEvent(ctx context.Context, event GatewayEvent) error
	ListGatewayEvents(ctx context.Context, limit int) ([]GatewayEvent, error)
	GetGatewayEvent(ctx context.Context, eventID string) (GatewayEvent, bool, error)

	SaveObservedGatewayState(ctx context.Context, state ObservedGatewayState) error
	ListObservedGatewayStates(ctx context.Context, limit int) ([]ObservedGatewayState, error)
	GetObservedGatewayState(ctx context.Context, stateID string) (ObservedGatewayState, bool, error)

	SaveNodeReachabilityCache(ctx context.Context, cache NodeReachabilityCache) error
	GetNodeReachabilityCache(ctx context.Context, nodePubkeyHex string) (NodeReachabilityCache, bool, error)
	ListNodeReachabilityCaches(ctx context.Context, limit int) ([]NodeReachabilityCache, error)

	SaveSelfNodeReachabilityState(ctx context.Context, state SelfNodeReachabilityState) error
	GetSelfNodeReachabilityState(ctx context.Context, nodePubkeyHex string) (SelfNodeReachabilityState, bool, error)
}

type gatewayClientStore struct {
	store moduleapi.Store
}

// NewGatewayClientStore 把主干已经准备好的 store 包成 gatewayclient 的窄能力。
func NewGatewayClientStore(store moduleapi.Store) any {
	if store == nil {
		return nil
	}
	return &gatewayClientStore{store: store}
}

func newGatewayClientStore(store moduleapi.Store) Store {
	if store == nil {
		return nil
	}
	return &gatewayClientStore{store: store}
}

type readOnlyQuerier struct {
	conn moduleapi.ReadConn
}

func (q readOnlyQuerier) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return nil, fmt.Errorf("read-only connection does not support exec")
}

func (q readOnlyQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return q.conn.QueryContext(ctx, query, args...)
}

func (s *gatewayClientStore) SaveGatewayEvent(ctx context.Context, event GatewayEvent) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("store is nil")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		payloadJSON := jsonBytes(event.Payload)
		_, err := client.ProcGatewayEvents.Create().
			SetCreatedAtUnix(normalizeUnix(event.CreatedAtUnix)).
			SetGatewayPubkeyHex(normalizeLower(event.GatewayPubkeyHex)).
			SetCommandID(strings.TrimSpace(event.CommandID)).
			SetAction(strings.TrimSpace(event.Action)).
			SetMsgID(strings.TrimSpace(event.MsgID)).
			SetSequenceNum(int64(event.SequenceNum)).
			SetPoolID(strings.TrimSpace(event.PoolID)).
			SetAmountSatoshi(event.AmountSatoshi).
			SetPayloadJSON(payloadJSON).
			Save(ctx)
		return err
	})
}

func (s *gatewayClientStore) ListGatewayEvents(ctx context.Context, limit int) ([]GatewayEvent, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	var out []GatewayEvent
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		q := client.ProcGatewayEvents.Query().Order(procgatewayevents.ByCreatedAtUnix(entsql.OrderDesc()), procgatewayevents.ByID(entsql.OrderDesc()))
		if limit > 0 {
			q = q.Limit(limit)
		}
		nodes, err := q.All(ctx)
		if err != nil {
			return err
		}
		out = make([]GatewayEvent, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, gatewayEventFromEnt(node))
		}
		return nil
	})
	return out, err
}

func (s *gatewayClientStore) GetGatewayEvent(ctx context.Context, eventID string) (GatewayEvent, bool, error) {
	if s == nil || s.store == nil {
		return GatewayEvent{}, false, fmt.Errorf("store is nil")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return GatewayEvent{}, false, nil
	}
	var out GatewayEvent
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		if row, err := client.ProcGatewayEvents.Query().Where(procgatewayevents.CommandIDEQ(eventID)).Only(ctx); err == nil {
			out = gatewayEventFromEnt(row)
			return nil
		} else if !gen.IsNotFound(err) {
			return err
		}
		if id, parseErr := strconv.ParseInt(eventID, 10, 64); parseErr == nil {
			row, err := client.ProcGatewayEvents.Query().Where(procgatewayevents.IDEQ(int(id))).Only(ctx)
			if err != nil {
				if gen.IsNotFound(err) {
					return nil
				}
				return err
			}
			out = gatewayEventFromEnt(row)
		}
		return nil
	})
	if err != nil {
		return GatewayEvent{}, false, err
	}
	if out.ID == 0 && strings.TrimSpace(out.CommandID) == "" {
		return GatewayEvent{}, false, nil
	}
	return out, true, nil
}

func (s *gatewayClientStore) SaveObservedGatewayState(ctx context.Context, state ObservedGatewayState) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("store is nil")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		payloadJSON := jsonBytes(state.Payload)
		_, err := client.ProcObservedGatewayStates.Create().
			SetCreatedAtUnix(normalizeUnix(state.CreatedAtUnix)).
			SetGatewayPubkeyHex(normalizeLower(state.GatewayPubkeyHex)).
			SetSourceRef(strings.TrimSpace(state.SourceRef)).
			SetObservedAtUnix(normalizeUnix(state.ObservedAtUnix)).
			SetEventName(strings.TrimSpace(state.EventName)).
			SetStateBefore(strings.TrimSpace(state.StateBefore)).
			SetStateAfter(strings.TrimSpace(state.StateAfter)).
			SetPauseReason(strings.TrimSpace(state.PauseReason)).
			SetPauseNeedSatoshi(state.PauseNeedSatoshi).
			SetPauseHaveSatoshi(state.PauseHaveSatoshi).
			SetLastError(strings.TrimSpace(state.LastError)).
			SetPayloadJSON(payloadJSON).
			Save(ctx)
		return err
	})
}

func (s *gatewayClientStore) ListObservedGatewayStates(ctx context.Context, limit int) ([]ObservedGatewayState, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	var out []ObservedGatewayState
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		q := client.ProcObservedGatewayStates.Query().Order(procobservedgatewaystates.ByObservedAtUnix(entsql.OrderDesc()), procobservedgatewaystates.ByID(entsql.OrderDesc()))
		if limit > 0 {
			q = q.Limit(limit)
		}
		nodes, err := q.All(ctx)
		if err != nil {
			return err
		}
		out = make([]ObservedGatewayState, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, observedGatewayStateFromEnt(node))
		}
		return nil
	})
	return out, err
}

func (s *gatewayClientStore) GetObservedGatewayState(ctx context.Context, stateID string) (ObservedGatewayState, bool, error) {
	if s == nil || s.store == nil {
		return ObservedGatewayState{}, false, fmt.Errorf("store is nil")
	}
	stateID = strings.TrimSpace(stateID)
	if stateID == "" {
		return ObservedGatewayState{}, false, nil
	}
	var out ObservedGatewayState
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		if row, err := client.ProcObservedGatewayStates.Query().Where(procobservedgatewaystates.SourceRefEQ(stateID)).Only(ctx); err == nil {
			out = observedGatewayStateFromEnt(row)
			return nil
		} else if !gen.IsNotFound(err) {
			return err
		}
		if id, parseErr := strconv.ParseInt(stateID, 10, 64); parseErr == nil {
			row, err := client.ProcObservedGatewayStates.Query().Where(procobservedgatewaystates.IDEQ(int(id))).Only(ctx)
			if err != nil {
				if gen.IsNotFound(err) {
					return nil
				}
				return err
			}
			out = observedGatewayStateFromEnt(row)
		}
		return nil
	})
	if err != nil {
		return ObservedGatewayState{}, false, err
	}
	if out.ID == 0 && strings.TrimSpace(out.SourceRef) == "" {
		return ObservedGatewayState{}, false, nil
	}
	return out, true, nil
}

func (s *gatewayClientStore) SaveNodeReachabilityCache(ctx context.Context, cache NodeReachabilityCache) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("store is nil")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		nodePubkeyHex := normalizeLower(cache.NodePubkeyHex)
		row, err := client.ProcNodeReachabilityCache.Query().Where(procnodereachabilitycache.TargetNodePubkeyHexEQ(nodePubkeyHex)).Only(ctx)
		if err == nil {
			_, err = row.Update().
				SetSourceGatewayPubkeyHex(normalizeLower(cache.SourceGatewayPubkeyHex)).
				SetHeadHeight(int64(cache.HeadHeight)).
				SetSeq(int64(cache.Seq)).
				SetMultiaddrsJSON(jsonStringList(cache.Multiaddrs)).
				SetPublishedAtUnix(normalizeUnix(cache.PublishedAtUnix)).
				SetExpiresAtUnix(normalizeUnix(cache.ExpiresAtUnix)).
				SetSignature(append([]byte(nil), cache.Signature...)).
				SetUpdatedAtUnix(normalizeUnix(cache.UpdatedAtUnix)).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = client.ProcNodeReachabilityCache.Create().
			SetTargetNodePubkeyHex(nodePubkeyHex).
			SetSourceGatewayPubkeyHex(normalizeLower(cache.SourceGatewayPubkeyHex)).
			SetHeadHeight(int64(cache.HeadHeight)).
			SetSeq(int64(cache.Seq)).
			SetMultiaddrsJSON(jsonStringList(cache.Multiaddrs)).
			SetPublishedAtUnix(normalizeUnix(cache.PublishedAtUnix)).
			SetExpiresAtUnix(normalizeUnix(cache.ExpiresAtUnix)).
			SetSignature(append([]byte(nil), cache.Signature...)).
			SetUpdatedAtUnix(normalizeUnix(cache.UpdatedAtUnix)).
			Save(ctx)
		return err
	})
}

func (s *gatewayClientStore) GetNodeReachabilityCache(ctx context.Context, nodePubkeyHex string) (NodeReachabilityCache, bool, error) {
	if s == nil || s.store == nil {
		return NodeReachabilityCache{}, false, fmt.Errorf("store is nil")
	}
	nodePubkeyHex = normalizeLower(nodePubkeyHex)
	if nodePubkeyHex == "" {
		return NodeReachabilityCache{}, false, nil
	}
	var out NodeReachabilityCache
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.ProcNodeReachabilityCache.Query().Where(procnodereachabilitycache.TargetNodePubkeyHexEQ(nodePubkeyHex)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		out = nodeReachabilityCacheFromEnt(row)
		return nil
	})
	if err != nil {
		return NodeReachabilityCache{}, false, err
	}
	if strings.TrimSpace(out.NodePubkeyHex) == "" {
		return NodeReachabilityCache{}, false, nil
	}
	return out, true, nil
}

func (s *gatewayClientStore) ListNodeReachabilityCaches(ctx context.Context, limit int) ([]NodeReachabilityCache, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	var out []NodeReachabilityCache
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		q := client.ProcNodeReachabilityCache.Query().Order(procnodereachabilitycache.ByUpdatedAtUnix(entsql.OrderDesc()), procnodereachabilitycache.ByID(entsql.OrderDesc()))
		if limit > 0 {
			q = q.Limit(limit)
		}
		nodes, err := q.All(ctx)
		if err != nil {
			return err
		}
		out = make([]NodeReachabilityCache, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, nodeReachabilityCacheFromEnt(node))
		}
		return nil
	})
	return out, err
}

func (s *gatewayClientStore) SaveSelfNodeReachabilityState(ctx context.Context, state SelfNodeReachabilityState) error {
	if s == nil || s.store == nil {
		return fmt.Errorf("store is nil")
	}
	return s.store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: tx})))
		nodePubkeyHex := normalizeLower(state.NodePubkeyHex)
		row, err := client.ProcSelfNodeReachabilityState.Query().Where(procselfnodereachabilitystate.NodePubkeyHexEQ(nodePubkeyHex)).Only(ctx)
		if err == nil {
			_, err = row.Update().
				SetHeadHeight(int64(state.HeadHeight)).
				SetSeq(int64(state.Seq)).
				SetUpdatedAtUnix(normalizeUnix(state.UpdatedAtUnix)).
				Save(ctx)
			return err
		}
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = client.ProcSelfNodeReachabilityState.Create().
			SetNodePubkeyHex(nodePubkeyHex).
			SetHeadHeight(int64(state.HeadHeight)).
			SetSeq(int64(state.Seq)).
			SetUpdatedAtUnix(normalizeUnix(state.UpdatedAtUnix)).
			Save(ctx)
		return err
	})
}

func (s *gatewayClientStore) GetSelfNodeReachabilityState(ctx context.Context, nodePubkeyHex string) (SelfNodeReachabilityState, bool, error) {
	if s == nil || s.store == nil {
		return SelfNodeReachabilityState{}, false, fmt.Errorf("store is nil")
	}
	nodePubkeyHex = normalizeLower(nodePubkeyHex)
	if nodePubkeyHex == "" {
		return SelfNodeReachabilityState{}, false, nil
	}
	var out SelfNodeReachabilityState
	err := s.store.Read(ctx, func(rc moduleapi.ReadConn) error {
		client := gen.NewClient(gen.Driver(entsql.NewDriver(dialect.SQLite, entsql.Conn{ExecQuerier: readOnlyQuerier{conn: rc}})))
		row, err := client.ProcSelfNodeReachabilityState.Query().Where(procselfnodereachabilitystate.NodePubkeyHexEQ(nodePubkeyHex)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return nil
			}
			return err
		}
		out = selfNodeReachabilityStateFromEnt(row)
		return nil
	})
	if err != nil {
		return SelfNodeReachabilityState{}, false, err
	}
	if strings.TrimSpace(out.NodePubkeyHex) == "" {
		return SelfNodeReachabilityState{}, false, nil
	}
	return out, true, nil
}

func jsonBytes(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}
	if json.Valid(raw) {
		return string(raw)
	}
	out, err := json.Marshal(string(raw))
	if err != nil {
		return "{}"
	}
	return string(out)
}

func jsonStringList(values []string) string {
	out, err := json.Marshal(normalizeStringSlice(values))
	if err != nil {
		return "[]"
	}
	return string(out)
}

func normalizeStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return out
}

func normalizeLower(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func normalizeUnix(v int64) int64 {
	if v > 0 {
		return v
	}
	return time.Now().Unix()
}

func gatewayEventFromEnt(node *gen.ProcGatewayEvents) GatewayEvent {
	if node == nil {
		return GatewayEvent{}
	}
	return GatewayEvent{
		ID:               int64(node.ID),
		CreatedAtUnix:    node.CreatedAtUnix,
		GatewayPubkeyHex: normalizeLower(node.GatewayPubkeyHex),
		CommandID:        strings.TrimSpace(node.CommandID),
		Action:           strings.TrimSpace(node.Action),
		MsgID:            strings.TrimSpace(node.MsgID),
		SequenceNum:      uint32(node.SequenceNum),
		PoolID:           strings.TrimSpace(node.PoolID),
		AmountSatoshi:    node.AmountSatoshi,
		Payload:          []byte(strings.TrimSpace(node.PayloadJSON)),
	}
}

func observedGatewayStateFromEnt(node *gen.ProcObservedGatewayStates) ObservedGatewayState {
	if node == nil {
		return ObservedGatewayState{}
	}
	return ObservedGatewayState{
		ID:                int64(node.ID),
		CreatedAtUnix:     node.CreatedAtUnix,
		GatewayPubkeyHex:  normalizeLower(node.GatewayPubkeyHex),
		SourceRef:         strings.TrimSpace(node.SourceRef),
		ObservedAtUnix:    node.ObservedAtUnix,
		EventName:         strings.TrimSpace(node.EventName),
		StateBefore:       strings.TrimSpace(node.StateBefore),
		StateAfter:        strings.TrimSpace(node.StateAfter),
		PauseReason:       strings.TrimSpace(node.PauseReason),
		PauseNeedSatoshi:  node.PauseNeedSatoshi,
		PauseHaveSatoshi:  node.PauseHaveSatoshi,
		LastError:         strings.TrimSpace(node.LastError),
		Payload:           []byte(strings.TrimSpace(node.PayloadJSON)),
	}
}

func nodeReachabilityCacheFromEnt(node *gen.ProcNodeReachabilityCache) NodeReachabilityCache {
	if node == nil {
		return NodeReachabilityCache{}
	}
	multiaddrs, _ := unmarshalStringSlice(node.MultiaddrsJSON)
	return NodeReachabilityCache{
		ID:                     int64(node.ID),
		NodePubkeyHex:          normalizeLower(node.TargetNodePubkeyHex),
		Multiaddrs:             multiaddrs,
		HeadHeight:             uint64(node.HeadHeight),
		Seq:                    uint64(node.Seq),
		PublishedAtUnix:        node.PublishedAtUnix,
		ExpiresAtUnix:          node.ExpiresAtUnix,
		Signature:              append([]byte(nil), node.Signature...),
		SourceGatewayPubkeyHex: normalizeLower(node.SourceGatewayPubkeyHex),
		UpdatedAtUnix:          node.UpdatedAtUnix,
	}
}

func selfNodeReachabilityStateFromEnt(node *gen.ProcSelfNodeReachabilityState) SelfNodeReachabilityState {
	if node == nil {
		return SelfNodeReachabilityState{}
	}
	return SelfNodeReachabilityState{
		ID:               int64(node.ID),
		NodePubkeyHex:    normalizeLower(node.NodePubkeyHex),
		HeadHeight:       uint64(node.HeadHeight),
		Seq:              uint64(node.Seq),
		UpdatedAtUnix:    node.UpdatedAtUnix,
	}
}

func unmarshalStringSlice(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return normalizeStringSlice(out), nil
}
