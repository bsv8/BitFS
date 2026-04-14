package clientapp

import "testing"

// mustSetRuntimeIdentity 只给测试用，显式把运行时身份补齐。
// 这样测试不会再靠启动配置隐式推导，也能直接暴露身份缺失问题。
func mustSetRuntimeIdentity(t *testing.T, rt *Runtime) {
	t.Helper()
	if rt == nil {
		t.Fatal("runtime is nil")
	}
	cfg := rt.ConfigSnapshot()
	identity, err := buildClientIdentityCaps(cfg, cfg.Keys.PrivkeyHex)
	if err != nil {
		t.Fatalf("build runtime identity: %v", err)
	}
	rt.identity = identity
}
