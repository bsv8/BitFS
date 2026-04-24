package obs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	LevelDebug     = "debug"
	LevelInfo      = "info"
	LevelBusiness  = "business"
	LevelImportant = "important"
	LevelError     = "error"
	LevelNone      = "none"

	CategorySystem   = "system"
	CategoryBusiness = "business"
	CategorySQL      = "sql_trace"

	// ServiceBitFSClient 是 BitFS 客户端统一服务名。
	// 设计说明：
	// - 所有进程内 obs 事件、测试断言和协议 domain 都从这里取值；
	// - 这样改名只改一处，避免不同子项目各自漂移。
	ServiceBitFSClient = "bitfs-client"
)

type Event struct {
	TS       string         `json:"ts"`
	Level    string         `json:"level"`
	Category string         `json:"category"`
	Service  string         `json:"service"`
	Name     string         `json:"name"`
	Fields   map[string]any `json:"fields,omitempty"`
}

type loggerState struct {
	mu             sync.Mutex
	systemFile     *os.File
	businessFile   *os.File
	consoleMinRank int
	nextListenerID int
	listeners      map[int]func(Event)
}

var state = &loggerState{consoleMinRank: rank(LevelBusiness)}

func Init(filePath string, consoleMinLevel string) error {
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.systemFile != nil {
		_ = state.systemFile.Close()
		state.systemFile = nil
	}
	if state.businessFile != nil {
		_ = state.businessFile.Close()
		state.businessFile = nil
	}
	systemPath, businessPath := ResolveSplitFilePaths(filePath)
	if systemPath != "" {
		if err := os.MkdirAll(filepath.Dir(systemPath), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(systemPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		state.systemFile = f
	}
	if businessPath != "" {
		if err := os.MkdirAll(filepath.Dir(businessPath), 0o755); err != nil {
			if state.systemFile != nil {
				_ = state.systemFile.Close()
				state.systemFile = nil
			}
			return err
		}
		f, err := os.OpenFile(businessPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			if state.systemFile != nil {
				_ = state.systemFile.Close()
				state.systemFile = nil
			}
			return err
		}
		state.businessFile = f
	}
	state.consoleMinRank = rank(consoleMinLevel)
	return nil
}

func Close() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	var firstErr error
	if state.systemFile != nil {
		if err := state.systemFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		state.systemFile = nil
	}
	if state.businessFile != nil {
		if err := state.businessFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		state.businessFile = nil
	}
	return firstErr
}

// AddListener 注册一个事件监听器（进程内“消息系统”挂钩点）。
//
// 典型用途：E2E/in-proc 集成测试在不“手把手执行业务动作”的前提下，等待并验证业务流程是否真正发生。
//
// 注意：回调会在写日志之后被调用；回调不应阻塞太久（必要时自行起 goroutine）。
func AddListener(fn func(Event)) (remove func()) {
	if fn == nil {
		return func() {}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.listeners == nil {
		state.listeners = map[int]func(Event){}
	}
	state.nextListenerID++
	id := state.nextListenerID
	state.listeners[id] = fn
	return func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.listeners == nil {
			return
		}
		delete(state.listeners, id)
	}
}

func Info(service, name string, fields map[string]any) {
	write(LevelInfo, CategorySystem, service, name, fields)
}

func Debug(service, name string, fields map[string]any) {
	write(LevelDebug, CategorySystem, service, name, fields)
}

func Business(service, name string, fields map[string]any) {
	write(LevelBusiness, CategoryBusiness, service, name, fields)
}

func Important(service, name string, fields map[string]any) {
	write(LevelImportant, CategoryBusiness, service, name, fields)
}

func Error(service, name string, fields map[string]any) {
	write(LevelError, CategorySystem, service, name, fields)
}

func SQL(service, name string, fields map[string]any) {
	write(LevelInfo, CategorySQL, service, name, fields)
}

func write(level, category, service, name string, fields map[string]any) {
	var compacted map[string]any
	if fields != nil {
		if m, ok := NormalizeAny(fields).(map[string]any); ok {
			compacted = m
		} else {
			compacted = map[string]any{"_": NormalizeAny(fields)}
		}
	}
	ev := Event{
		TS:       time.Now().UTC().Format(time.RFC3339),
		Level:    level,
		Category: category,
		Service:  service,
		Name:     name,
		Fields:   compacted,
	}
	b, _ := json.Marshal(ev)
	line := string(b)

	var ls []func(Event)
	state.mu.Lock()

	if category == CategoryBusiness {
		if state.businessFile != nil {
			_, _ = fmt.Fprintln(state.businessFile, line)
		}
	} else {
		if state.systemFile != nil {
			_, _ = fmt.Fprintln(state.systemFile, line)
		}
	}
	if rank(level) >= state.consoleMinRank {
		fmt.Println(formatConsoleLine(ev))
	}
	if len(state.listeners) > 0 {
		ls = make([]func(Event), 0, len(state.listeners))
		for _, fn := range state.listeners {
			ls = append(ls, fn)
		}
	}
	state.mu.Unlock()

	for _, fn := range ls {
		func() {
			defer func() { _ = recover() }()
			fn(ev)
		}()
	}
}

func formatConsoleLine(ev Event) string {
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(strings.TrimSpace(ev.Level))
	b.WriteString("][")
	b.WriteString(strings.TrimSpace(ev.Category))
	b.WriteString("][")
	b.WriteString(strings.TrimSpace(ev.Service))
	b.WriteString("] ")
	b.WriteString(strings.TrimSpace(ev.Name))
	if len(ev.Fields) == 0 {
		return b.String()
	}
	keys := make([]string, 0, len(ev.Fields))
	for k := range ev.Fields {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(formatConsoleValue(ev.Fields[k]))
	}
	return b.String()
}

func formatConsoleValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return `""`
		}
		if strings.ContainsAny(s, " \t\r\n") {
			return strconv.Quote(s)
		}
		return s
	default:
		raw, err := json.Marshal(x)
		if err != nil {
			return strconv.Quote(fmt.Sprint(x))
		}
		return string(raw)
	}
}

func rank(level string) int {
	switch level {
	case LevelDebug:
		return 10
	case LevelInfo:
		return 20
	case LevelBusiness:
		return 30
	case LevelImportant:
		return 40
	case LevelError:
		return 50
	case LevelNone:
		return 1000
	default:
		return 30
	}
}

// ResolveSplitFilePaths 根据基础日志文件路径生成 system/business 两份 JSONL 路径。
// 例如: /x/bitfs.log -> /x/bitfs.system.jsonl 与 /x/bitfs.business.jsonl
func ResolveSplitFilePaths(filePath string) (systemPath, businessPath string) {
	base := strings.TrimSpace(filePath)
	if base == "" {
		return "", ""
	}
	base = filepath.Clean(base)
	dir := filepath.Dir(base)
	name := filepath.Base(base)
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	if stem == "" {
		stem = name
	}
	return filepath.Join(dir, stem+".system.jsonl"), filepath.Join(dir, stem+".business.jsonl")
}
