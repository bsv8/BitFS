package clientapp

import "testing"

// mustSetRuntimeIdentityFromRunIn 只给测试用，显式把运行时身份补齐。
// 这样测试不会再靠 runIn 隐式推导，也能直接暴露身份缺失问题。
func mustSetRuntimeIdentityFromRunIn(t *testing.T, rt *Runtime) {
	t.Helper()
	if rt == nil {
		t.Fatal("runtime is nil")
	}
	identity, err := buildClientIdentityCaps(rt.runIn)
	if err != nil {
		t.Fatalf("build runtime identity: %v", err)
	}
	rt.identity = identity
}
