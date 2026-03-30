package managedclient

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

const (
	ManagedControlEndpointEnv = "BITFS_MANAGED_CONTROL_ENDPOINT"
	ManagedControlTokenEnv    = "BITFS_MANAGED_CONTROL_TOKEN"
)

type ManagedRuntimeEvent struct {
	Seq            uint64         `json:"seq"`
	RuntimeEpoch   string         `json:"runtime_epoch"`
	Topic          string         `json:"topic"`
	Scope          string         `json:"scope"`
	OccurredAtUnix int64          `json:"occurred_at_unix"`
	Producer       string         `json:"producer"`
	TraceID        string         `json:"trace_id"`
	Payload        map[string]any `json:"payload,omitempty"`
}

type managedControlHello struct {
	Type         string `json:"type"`
	Token        string `json:"token"`
	RuntimeEpoch string `json:"runtime_epoch"`
}

// ManagedControlStream 负责把 runtime 内部事件投递到托管宿主。
//
// 设计说明：
// - BitFS 内部先统一使用进程内消息/obs 挂钩，再由托管层决定是否额外输出机器帧；
// - 纯 CLI 默认不启用 control stream，这样 stdout/stderr 仍然保留给人类阅读；
// - Electron 托管模式改走专用控制通道，避免机器帧和人类日志混在同一条 stdout 里。
type ManagedControlStream interface {
	Emit(topic, scope, producer, traceID string, payload map[string]any)
	ObsSink() obs.Sink
}

func NewManagedControlStreamFromEnv() (ManagedControlStream, error) {
	endpoint := strings.TrimSpace(os.Getenv(ManagedControlEndpointEnv))
	if endpoint == "" {
		return noopManagedControlStream{}, nil
	}
	token := strings.TrimSpace(os.Getenv(ManagedControlTokenEnv))
	if token == "" {
		return nil, fmt.Errorf("managed control token is required")
	}
	network, address, err := parseManagedControlEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial managed control endpoint: %w", err)
	}
	return newSocketManagedControlStream(conn, token)
}

type noopManagedControlStream struct{}

func (noopManagedControlStream) Emit(string, string, string, string, map[string]any) {}

func (noopManagedControlStream) ObsSink() obs.Sink {
	return nil
}

type socketManagedControlStream struct {
	mu           sync.Mutex
	seq          uint64
	runtimeEpoch string
	obsSink      obs.Sink
	conn         net.Conn
	token        string
}

func newSocketManagedControlStream(conn net.Conn, token string) (ManagedControlStream, error) {
	s := &socketManagedControlStream{
		runtimeEpoch: fmt.Sprintf("rt-%d", time.Now().UnixNano()),
		conn:         conn,
		token:        strings.TrimSpace(token),
	}
	s.obsSink = obs.SinkFunc(func(ev obs.Event) {
		s.emitObsEvent(ev)
	})
	if err := s.writeFrameLocked(managedControlHello{
		Type:         "hello",
		Token:        s.token,
		RuntimeEpoch: s.runtimeEpoch,
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write managed control hello: %w", err)
	}
	return s, nil
}

func (s *socketManagedControlStream) ObsSink() obs.Sink {
	return s.obsSink
}

func (s *socketManagedControlStream) Emit(topic, scope, producer, traceID string, payload map[string]any) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	frame := ManagedRuntimeEvent{
		Seq:            s.seq,
		RuntimeEpoch:   s.runtimeEpoch,
		Topic:          topic,
		Scope:          normalizeManagedEventScope(scope),
		OccurredAtUnix: time.Now().Unix(),
		Producer:       strings.TrimSpace(producer),
		TraceID:        strings.TrimSpace(traceID),
		Payload:        cloneManagedEventPayload(payload),
	}
	if frame.Payload == nil {
		frame.Payload = map[string]any{}
	}
	if err := s.writeFrameLocked(frame); err != nil {
		return
	}
}

func (s *socketManagedControlStream) writeFrameLocked(frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	_, err = s.conn.Write(append(raw, '\n'))
	return err
}

func (s *socketManagedControlStream) emitObsEvent(ev obs.Event) {
	rawPayload := map[string]any{
		"level":    strings.TrimSpace(ev.Level),
		"category": strings.TrimSpace(ev.Category),
		"service":  strings.TrimSpace(ev.Service),
		"event":    strings.TrimSpace(ev.Name),
		"fields":   cloneManagedEventPayload(ev.Fields),
	}
	s.Emit("backend.obs", "private", "obs", "", rawPayload)
	for _, mapped := range mapManagedObsEvents(ev) {
		s.Emit(mapped.Topic, mapped.Scope, mapped.Producer, "", mapped.Payload)
	}
}

type mappedManagedEvent struct {
	Topic    string
	Scope    string
	Producer string
	Payload  map[string]any
}

func mapManagedObsEvents(ev obs.Event) []mappedManagedEvent {
	if strings.TrimSpace(ev.Service) != "bitcast-client" {
		return nil
	}
	privatePayload := buildObsDerivedPayload(ev)
	out := make([]mappedManagedEvent, 0, 3)
	switch {
	case strings.TrimSpace(ev.Name) == "wallet_utxo_sync_step":
		out = append(out,
			mappedManagedEvent{
				Topic:    "wallet.sync.changed",
				Scope:    "private",
				Producer: "clientapp.wallet",
				Payload:  privatePayload,
			},
			mappedManagedEvent{
				Topic:    "wallet.changed",
				Scope:    "public",
				Producer: "clientapp.wallet",
				Payload: map[string]any{
					"event":     strings.TrimSpace(ev.Name),
					"level":     strings.TrimSpace(ev.Level),
					"has_error": strings.EqualFold(strings.TrimSpace(ev.Level), obs.LevelError),
					"step":      managedFieldString(ev.Fields, "step"),
					"status":    managedFieldString(ev.Fields, "status"),
				},
			},
		)
	case strings.TrimSpace(ev.Name) == "workspace_scanned" || strings.HasPrefix(strings.TrimSpace(ev.Name), "evt_trigger_workspace_sync_once_"):
		out = append(out, mappedManagedEvent{
			Topic:    "resource.workspace.changed",
			Scope:    "private",
			Producer: "clientapp.workspace",
			Payload:  privatePayload,
		})
	case strings.HasPrefix(strings.TrimSpace(ev.Name), "fs_strategy_"):
		out = append(out, mappedManagedEvent{
			Topic:    "resource.transfer.changed",
			Scope:    "private",
			Producer: "clientapp.fshttp",
			Payload:  privatePayload,
		})
	}
	return out
}

func buildObsDerivedPayload(ev obs.Event) map[string]any {
	payload := map[string]any{
		"level":    strings.TrimSpace(ev.Level),
		"category": strings.TrimSpace(ev.Category),
		"service":  strings.TrimSpace(ev.Service),
		"event":    strings.TrimSpace(ev.Name),
	}
	if len(ev.Fields) > 0 {
		payload["fields"] = cloneManagedEventPayload(ev.Fields)
	}
	return payload
}

func cloneManagedEventPayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func normalizeManagedEventScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "public":
		return "public"
	default:
		return "private"
	}
}

func managedFieldString(fields map[string]any, key string) string {
	if len(fields) == 0 {
		return ""
	}
	value, ok := fields[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func parseManagedControlEndpoint(raw string) (string, string, error) {
	endpoint := strings.TrimSpace(raw)
	if endpoint == "" {
		return "", "", fmt.Errorf("managed control endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("invalid managed control endpoint: %w", err)
	}
	switch strings.TrimSpace(strings.ToLower(u.Scheme)) {
	case "unix":
		if strings.TrimSpace(u.Path) == "" {
			return "", "", fmt.Errorf("managed control unix path is required")
		}
		return "unix", u.Path, nil
	case "tcp":
		if strings.TrimSpace(u.Host) == "" {
			return "", "", fmt.Errorf("managed control tcp host is required")
		}
		return "tcp", u.Host, nil
	default:
		return "", "", fmt.Errorf("unsupported managed control endpoint scheme: %s", u.Scheme)
	}
}
