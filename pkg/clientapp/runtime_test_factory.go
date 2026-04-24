package clientapp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
)

// newRuntimeForTest 创建最小可测 runtime。
// 设计约束：
// - 测试只通过配置服务改配置，不再直接拼运行态字段；
// - 需要身份时显式传入私钥，让测试更容易暴露缺参；
// - 默认按 test 启动模式，不自动补产品态默认网关。
func newRuntimeForTest(t *testing.T, cfg Config, privHex string, opts ...func(*Runtime)) *Runtime {
	t.Helper()
	runtimeCfg := cloneConfig(cfg)
	if strings.TrimSpace(runtimeCfg.Storage.DataDir) == "" {
		root := t.TempDir()
		runtimeCfg.Storage.DataDir = filepath.Join(root, "data")
		if strings.TrimSpace(runtimeCfg.Index.SQLitePath) == "" {
			runtimeCfg.Index.SQLitePath = filepath.Join(root, "data", "client-index.sqlite")
		}
		if strings.TrimSpace(runtimeCfg.HTTP.ListenAddr) == "" {
			runtimeCfg.HTTP.ListenAddr = "127.0.0.1:0"
		}
		if strings.TrimSpace(runtimeCfg.FSHTTP.ListenAddr) == "" {
			runtimeCfg.FSHTTP.ListenAddr = "127.0.0.1:0"
		}
		if runtimeCfg.Live.Publish.BroadcastWindow == 0 {
			runtimeCfg.Live.Publish.BroadcastWindow = 10
		}
		if runtimeCfg.Live.Publish.BroadcastIntervalSec == 0 {
			runtimeCfg.Live.Publish.BroadcastIntervalSec = 1
		}
	}
	if err := ApplyConfigDefaultsForMode(&runtimeCfg, StartupModeTest); err != nil {
		t.Fatalf("apply test config defaults: %v", err)
	}
	if strings.TrimSpace(runtimeCfg.Index.SQLitePath) == "" {
		runtimeCfg.Index.SQLitePath = filepath.Join(t.TempDir(), "client-index.sqlite")
	}
	configPath := filepath.Join(t.TempDir(), "runtime-config.yaml")
	svc, err := newRuntimeConfigService(runtimeCfg, configPath, StartupModeTest)
	if err != nil {
		t.Fatalf("create runtime config service: %v", err)
	}
	rt := &Runtime{
		ctx:    context.WithoutCancel(t.Context()),
		config: svc,
	}
	if privHex != "" {
		identity, err := buildClientIdentityCaps(runtimeCfg, privHex)
		if err != nil {
			t.Fatalf("build test identity: %v", err)
		}
		rt.identity = identity
	}
	for _, opt := range opts {
		if opt != nil {
			opt(rt)
		}
	}
	return rt
}

func withRuntimeHost(h host.Host) func(*Runtime) {
	return func(rt *Runtime) {
		if rt != nil {
			rt.Host = h
		}
	}
}

func withRuntimeFileStorage(fileStorage fileStorageRuntime) func(*Runtime) {
	return func(rt *Runtime) {
		if rt != nil {
			rt.FileStorage = fileStorage
		}
	}
}

func withRuntimeStore(store *clientDB) func(*Runtime) {
	return func(rt *Runtime) {
		if rt != nil {
			rt.store = store
		}
	}
}

func withRuntimeLiveRuntime(live *liveRuntime) func(*Runtime) {
	return func(rt *Runtime) {
		if rt != nil {
			rt.live = live
		}
	}
}

func withRuntimeConfigPath(path string) func(*Runtime) {
	return func(rt *Runtime) {
		if rt != nil && rt.config != nil {
			rt.config.mu.Lock()
			rt.config.configPath = path
			rt.config.mu.Unlock()
		}
	}
}

func mustUpdateRuntimeConfigMemoryOnly(t *testing.T, rt *Runtime, mutate func(*Config)) {
	t.Helper()
	if rt == nil {
		t.Fatal("runtime is nil")
	}
	if mutate == nil {
		t.Fatal("mutate function is nil")
	}
	cfgSvc := rt.RuntimeConfigService()
	if cfgSvc == nil {
		t.Fatal("runtime config service is nil")
	}
	next := cfgSvc.Snapshot()
	mutate(&next)
	if err := cfgSvc.UpdateMemoryOnly(next); err != nil {
		t.Fatalf("update runtime config: %v", err)
	}
}
