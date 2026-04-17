package clientapp

import "context"

func (m *chainMaintainer) appendWorkerLog(ctx context.Context, taskType string, entry chainWorkerLogEntry) {
	if m == nil || m.store == nil {
		return
	}
	if ctx == nil {
		return
	}
	if taskType == chainTaskTip {
		dbAppendChainTipWorkerLog(ctx, m.store, entry)
		return
	}
	dbAppendChainUTXOWorkerLog(ctx, m.store, entry)
}

func runtimeLogContext(rt *Runtime) context.Context {
	if rt == nil || rt.ctx == nil {
		return nil
	}
	return context.WithoutCancel(rt.ctx)
}
