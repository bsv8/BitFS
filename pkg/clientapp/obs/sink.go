package obs

// Sink 是可注入的事件接收器（用于 E2E/集成测试把事件汇聚到统一存储中）。
//
// 注意：错误信息使用英文，避免在多语言环境里混淆解析。
type Sink interface {
	Handle(Event)
}

type SinkFunc func(Event)

func (f SinkFunc) Handle(ev Event) {
	if f == nil {
		return
	}
	f(ev)
}
