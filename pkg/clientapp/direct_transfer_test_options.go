package clientapp

// DirectTransferTestOptions 是 e2e 专用的直连传输测试开关。
// 说明：
// - 只用于测试触发，不用于生产配置；
// - 默认全 false，不影响线上业务路径。
type DirectTransferTestOptions struct {
	ForceRejectPay    bool
	ForceRejectArbFee bool
}

// SetDirectTransferTestOptions 设置运行时直连传输测试开关。
// e2e 调用方可在用例开始前设置，结束后恢复默认值。
func SetDirectTransferTestOptions(rt *Runtime, opts DirectTransferTestOptions) {
	if rt == nil {
		return
	}
	rt.directTransferTestMu.Lock()
	rt.directTransferTestOpts = opts
	rt.directTransferTestMu.Unlock()
}

func (r *Runtime) directTransferTestOptions() DirectTransferTestOptions {
	if r == nil {
		return DirectTransferTestOptions{}
	}
	r.directTransferTestMu.RLock()
	out := r.directTransferTestOpts
	r.directTransferTestMu.RUnlock()
	return out
}
