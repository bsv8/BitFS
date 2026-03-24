package managedclient

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

const (
	managedEventPrefix      = "BITFS_EVENT "
	ManagedControlStreamEnv = "BITFS_MANAGED_CONTROL_STREAM"
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

// ManagedControlStream 负责把 runtime 内部事件投递到托管宿主。
//
// 设计说明：
// - BitFS 内部先统一使用进程内消息/obs 挂钩，再由托管层决定是否额外输出机器帧；
// - 纯 CLI 默认不启用 control stream，这样 stdout/stderr 仍然保留给人类阅读；
// - Electron 托管模式再显式开启 stdout NDJSON，主进程只需要解析固定前缀即可。
type ManagedControlStream interface {
	Emit(topic, scope, producer, traceID string, payload map[string]any)
	ObsSink() obs.Sink
}

func NewManagedControlStreamFromEnv() ManagedControlStream {
	if strings.TrimSpace(os.Getenv(ManagedControlStreamEnv)) != "1" {
		return noopManagedControlStream{}
	}
	return newStdoutManagedControlStream()
}

type noopManagedControlStream struct{}

func (noopManagedControlStream) Emit(string, string, string, string, map[string]any) {}

func (noopManagedControlStream) ObsSink() obs.Sink {
	return nil
}

type stdoutManagedControlStream struct {
	mu           sync.Mutex
	seq          uint64
	runtimeEpoch string
	obsSink      obs.Sink
}

func newStdoutManagedControlStream() ManagedControlStream {
	s := &stdoutManagedControlStream{
		runtimeEpoch: fmt.Sprintf("rt-%d", time.Now().UnixNano()),
	}
	s.obsSink = obs.SinkFunc(func(ev obs.Event) {
		s.emitObsEvent(ev)
	})
	return s
}

func (s *stdoutManagedControlStream) ObsSink() obs.Sink {
	return s.obsSink
}

func (s *stdoutManagedControlStream) Emit(topic, scope, producer, traceID string, payload map[string]any) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return
	}
	frame := ManagedRuntimeEvent{
		Seq:            s.nextSeq(),
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
	raw, err := json.Marshal(frame)
	if err != nil {
		return
	}
	fmt.Fprintf(os.Stdout, "%s%s\n", managedEventPrefix, string(raw))
}

func (s *stdoutManagedControlStream) nextSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.seq
}

func (s *stdoutManagedControlStream) emitObsEvent(ev obs.Event) {
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
