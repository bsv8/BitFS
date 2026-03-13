package clientapp

import (
	"database/sql"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

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

func appendOrchestratorLog(db *sql.DB, e orchestratorLogEntry) {
	if db == nil {
		return
	}
	_, err := db.Exec(
		`INSERT INTO orchestrator_logs(
			created_at_unix,event_type,source,signal_type,aggregate_key,idempotency_key,command_type,gateway_pubkey_hex,task_status,retry_count,queue_length,error_message,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		strings.TrimSpace(e.EventType),
		strings.TrimSpace(e.Source),
		strings.TrimSpace(e.SignalType),
		strings.TrimSpace(e.AggregateKey),
		strings.TrimSpace(e.IdempotencyKey),
		strings.TrimSpace(e.CommandType),
		strings.TrimSpace(e.GatewayPeerID),
		strings.TrimSpace(e.TaskStatus),
		e.RetryCount,
		e.QueueLength,
		strings.TrimSpace(e.ErrorMessage),
		mustJSON(e.Payload),
	)
	if err != nil {
		obs.Error("bitcast-client", "orchestrator_log_append_failed", map[string]any{
			"error":      err.Error(),
			"event_type": strings.TrimSpace(e.EventType),
		})
	}
}
