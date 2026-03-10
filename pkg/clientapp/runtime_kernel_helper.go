package clientapp

import "sync"

var runtimeKernelInitMu sync.Mutex

func ensureClientKernel(rt *Runtime) *clientKernel {
	if rt == nil {
		return nil
	}
	if rt.kernel != nil {
		return rt.kernel
	}
	runtimeKernelInitMu.Lock()
	defer runtimeKernelInitMu.Unlock()
	if rt.kernel == nil {
		rt.kernel = newClientKernel(rt)
	}
	return rt.kernel
}

func getClientOrchestrator(rt *Runtime) *orchestrator {
	if rt == nil {
		return nil
	}
	return rt.orch
}
