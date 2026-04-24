package pproto

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveLocalRawTraceDir(t *testing.T) {
	got := ResolveLocalRawTraceDir("/tmp/logs/bitfs.log")
	want := filepath.Clean("/tmp/logs/bitfs.rpcraw")
	if got != want {
		t.Fatalf("ResolveLocalRawTraceDir() mismatch: got=%s want=%s", got, want)
	}
}

func TestLocalRawTraceSinkWritesIndexAndBlobs(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "logs", "bitfs.log")
	sink, err := NewLocalRawTraceSink(logPath)
	if err != nil {
		t.Fatalf("NewLocalRawTraceSink() failed: %v", err)
	}
	sink.Handle(TraceEvent{
		TS:                  "2026-03-22T00:00:00Z",
		Direction:           "send",
		ProtoID:             "/demo/1.0.0",
		RequestEnvelopeWire: []byte{0x01, 0x02, 0x03},
		RequestPayloadWire:  []byte("req"),
		ResponsePayloadWire: []byte("resp"),
	})
	if err := sink.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	root := ResolveLocalRawTraceDir(logPath)
	raw, err := os.ReadFile(filepath.Join(root, "events.jsonl"))
	if err != nil {
		t.Fatalf("read events.jsonl failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("events line count mismatch: got=%d want=1", len(lines))
	}
	var ev TraceEvent
	if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
		t.Fatalf("decode event failed: %v", err)
	}
	if ev.RequestEnvelopeSHA256 == "" || ev.RequestEnvelopeBlob == "" {
		t.Fatalf("request envelope blob ref missing: %+v", ev)
	}
	if ev.RequestPayloadSHA256 == "" || ev.RequestPayloadBlob == "" {
		t.Fatalf("request payload blob ref missing: %+v", ev)
	}
	if ev.ResponsePayloadSHA256 == "" || ev.ResponsePayloadBlob == "" {
		t.Fatalf("response payload blob ref missing: %+v", ev)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(ev.RequestEnvelopeBlob))); err != nil {
		t.Fatalf("request envelope blob missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(ev.RequestPayloadBlob))); err != nil {
		t.Fatalf("request payload blob missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(ev.ResponsePayloadBlob))); err != nil {
		t.Fatalf("response payload blob missing: %v", err)
	}
}
