package obs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeAny_FullValues(t *testing.T) {
	type inner struct {
		Pubkey []byte `json:"pubkey"`
		Text   string `json:"text"`
	}
	got := NormalizeAny(map[string]any{
		"bytes": []byte{0x00, 0x11, 0xaa, 0xff},
		"hex":   strings.Repeat("ab", 40),
		"inner": inner{
			Pubkey: []byte{0x01, 0x02, 0x03},
			Text:   "hello",
		},
	})
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("want map, got %T", got)
	}
	if m["bytes"] != "0011aaff" {
		t.Fatalf("bytes should stay full hex, got=%v", m["bytes"])
	}
	if m["hex"] != strings.Repeat("ab", 40) {
		t.Fatalf("hex string should stay full, got=%v", m["hex"])
	}
	innerMap, ok := m["inner"].(map[string]any)
	if !ok {
		t.Fatalf("inner should be a map, got %T", m["inner"])
	}
	if innerMap["pubkey"] != "010203" {
		t.Fatalf("nested bytes should stay full hex, got=%v", innerMap["pubkey"])
	}
	if innerMap["text"] != "hello" {
		t.Fatalf("nested text should stay raw, got=%v", innerMap["text"])
	}
}

func TestNormalizeAny_Cycle(t *testing.T) {
	type node struct {
		Name string `json:"name"`
		Next *node  `json:"next,omitempty"`
	}
	n := &node{Name: "root"}
	n.Next = n
	got := NormalizeAny(n)
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("want map, got %T", got)
	}
	if m["next"] != "<cycle>" {
		t.Fatalf("cycle should be marked, got=%v", m["next"])
	}
}

func TestLogWriteUsesNormalizeAny(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "obs.jsonl")
	if err := Init(logPath, LevelNone); err != nil {
		t.Fatalf("init logger: %v", err)
	}
	defer func() { _ = Close() }()

	Info("svc", "test", map[string]any{
		"txid":  strings.Repeat("cd", 40),
		"bytes": []byte{0x12, 0x34, 0x56, 0x78},
	})
	data, err := os.ReadFile(filepath.Join(dir, "obs.system.jsonl"))
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	var ev Event
	if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if got := ev.Fields["txid"]; got != strings.Repeat("cd", 40) {
		t.Fatalf("txid should stay full, got=%v", got)
	}
	if got := ev.Fields["bytes"]; got != "12345678" {
		t.Fatalf("bytes should stay full hex, got=%v", got)
	}
	if strings.Contains(lines[0], "...") {
		t.Fatalf("log line should not contain ellipsis: %s", lines[0])
	}
}
