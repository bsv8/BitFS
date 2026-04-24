package pproto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// LocalRawTraceSink 把 pproto 收发事件落盘为“索引 + blob”两层结构。
// 设计说明：
// - events.jsonl 只保存索引与解析摘要，便于 grep / diff；
// - 原始 envelope / payload 以 sha256 命名，避免大字段把 JSONL 撑爆；
// - 这是 debug 能力，不参与正常业务语义。
type LocalRawTraceSink struct {
	rootDir  string
	events   string
	blobRoot string

	mu sync.Mutex
	f  *os.File

	ch chan TraceEvent

	closeOnce sync.Once
	done      chan struct{}
}

func NewLocalRawTraceSink(logPath string) (*LocalRawTraceSink, error) {
	rootDir := ResolveLocalRawTraceDir(logPath)
	if strings.TrimSpace(rootDir) == "" {
		return nil, fmt.Errorf("raw trace dir is required")
	}
	s := &LocalRawTraceSink{
		rootDir:  rootDir,
		events:   filepath.Join(rootDir, "events.jsonl"),
		blobRoot: filepath.Join(rootDir, "blobs"),
		ch:       make(chan TraceEvent, 4096),
		done:     make(chan struct{}),
	}
	go s.writer()
	return s, nil
}

// ResolveLocalRawTraceDir 根据日志路径派生 raw 抓包目录。
// 例如：
// - /x/logs/bitfs.log -> /x/logs/bitfs.rpcraw
// - .vault/logs/arbiter-mr.log -> .vault/logs/arbiter-mr.rpcraw
func ResolveLocalRawTraceDir(logPath string) string {
	p := filepath.Clean(strings.TrimSpace(logPath))
	if p == "" || p == "." {
		return "rpcraw"
	}
	dir := filepath.Dir(p)
	base := filepath.Base(p)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	if strings.TrimSpace(stem) == "" {
		stem = "rpc"
	}
	return filepath.Join(dir, stem+".rpcraw")
}

func (s *LocalRawTraceSink) Handle(ev TraceEvent) {
	if s == nil {
		return
	}
	select {
	case s.ch <- ev:
	default:
		// debug 抓包不能反压业务收发；极端压力下允许丢弃。
	}
}

func (s *LocalRawTraceSink) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		close(s.ch)
		<-s.done
		s.mu.Lock()
		f := s.f
		s.f = nil
		s.mu.Unlock()
		if f != nil {
			err = f.Close()
		}
	})
	return err
}

func (s *LocalRawTraceSink) writer() {
	defer close(s.done)
	for ev := range s.ch {
		if err := s.ensureOpen(); err != nil {
			continue
		}
		ev = s.enrichEvent(ev)
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		s.mu.Lock()
		f := s.f
		s.mu.Unlock()
		if f == nil {
			continue
		}
		_, _ = f.Write(append(b, '\n'))
	}
}

func (s *LocalRawTraceSink) ensureOpen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		return nil
	}
	if err := os.MkdirAll(s.blobRoot, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.events), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(s.events, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	return nil
}

func (s *LocalRawTraceSink) enrichEvent(ev TraceEvent) TraceEvent {
	ev.RequestEnvelopeSHA256, ev.RequestEnvelopeBytes, ev.RequestEnvelopeBlob = s.storeBlob(ev.RequestEnvelopeWire)
	ev.RequestPayloadSHA256, ev.RequestPayloadBytes, ev.RequestPayloadBlob = s.storeBlob(ev.RequestPayloadWire)
	ev.ResponsePayloadSHA256, ev.ResponsePayloadBytes, ev.ResponsePayloadBlob = s.storeBlob(ev.ResponsePayloadWire)
	ev.RequestEnvelopeWire = nil
	ev.RequestPayloadWire = nil
	ev.ResponsePayloadWire = nil
	return ev
}

func (s *LocalRawTraceSink) storeBlob(raw []byte) (string, int, string) {
	if len(raw) == 0 {
		return "", 0, ""
	}
	sum := sha256.Sum256(raw)
	sha := hex.EncodeToString(sum[:])
	blobRel := filepath.ToSlash(filepath.Join("blobs", sha+".bin"))
	blobPath := filepath.Join(s.rootDir, filepath.FromSlash(blobRel))
	if _, err := os.Stat(blobPath); err == nil {
		return sha, len(raw), blobRel
	}
	if err := os.WriteFile(blobPath, raw, 0o644); err != nil {
		return sha, len(raw), ""
	}
	return sha, len(raw), blobRel
}
