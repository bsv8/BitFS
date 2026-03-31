package clientapp

// orchestratorLogEntry 记录调度器运行链路中的关键状态变化，便于后台按事件回放。
type orchestratorLogEntry struct {
	EventType      string
	Source         string
	SignalType     string
	AggregateKey   string
	IdempotencyKey string
	CommandType    string
	GatewayPeerID  string
	TaskStatus     string
	RetryCount     int
	QueueLength    int
	ErrorMessage   string
	Payload        any
}
