package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	bitfsprocchaintipworkerlogs "github.com/bsv8/bitfs-contract/ent/v1/gen/procchaintipworkerlogs"
	bitfsprocchainutxoworkerlogs "github.com/bsv8/bitfs-contract/ent/v1/gen/procchainutxoworkerlogs"
	bitfsprocorchestratorlogs "github.com/bsv8/bitfs-contract/ent/v1/gen/procorchestratorlogs"
	bitfsprocschedulertaskruns "github.com/bsv8/bitfs-contract/ent/v1/gen/procschedulertaskruns"
	bitfsprocschedulertasks "github.com/bsv8/bitfs-contract/ent/v1/gen/procschedulertasks"
	bitfswalletutxo "github.com/bsv8/bitfs-contract/ent/v1/gen/walletutxo"
)

const singleStepOrchestratorEventPrefix = "__single__:"

type orchestratorLogFilter struct {
	Limit          int
	Offset         int
	EventType      string
	SignalType     string
	Source         string
	GatewayPeerID  string
	IdempotencyKey string
	TaskStatus     string
}

type orchestratorLogPage struct {
	Total int
	Items []orchestratorLogItem
}

type orchestratorLogItem struct {
	EventID          string `json:"event_id"`
	StartedAtUnix    int64  `json:"started_at_unix"`
	EndedAtUnix      int64  `json:"ended_at_unix"`
	StepsCount       int    `json:"steps_count"`
	LatestLogID      int64  `json:"latest_log_id"`
	IdempotencyKey   string `json:"aggregate_key"`
	AggregateKey     string `json:"aggregate_key"`
	CommandType      string `json:"command_type"`
	GatewayPeerID    string `json:"gateway_pubkey_hex"`
	Source           string `json:"source"`
	SignalType       string `json:"signal_type"`
	LatestEventType  string `json:"latest_event_type"`
	LatestTaskStatus string `json:"latest_task_status"`
	LatestRetryCount int    `json:"latest_retry_count"`
	LatestQueueLen   int    `json:"latest_queue_length"`
	LastErrorMessage string `json:"last_error_message"`
}

type orchestratorLogStep struct {
	ID             int64           `json:"id"`
	CreatedAtUnix  int64           `json:"created_at_unix"`
	EventType      string          `json:"event_type"`
	Source         string          `json:"source"`
	SignalType     string          `json:"signal_type"`
	AggregateKey   string          `json:"aggregate_key"`
	IdempotencyKey string          `json:"aggregate_key"`
	CommandType    string          `json:"command_type"`
	GatewayPeerID  string          `json:"gateway_pubkey_hex"`
	TaskStatus     string          `json:"task_status"`
	RetryCount     int             `json:"retry_count"`
	QueueLength    int             `json:"queue_length"`
	ErrorMessage   string          `json:"error_message"`
	Payload        json.RawMessage `json:"payload"`
}

type orchestratorLogDetail struct {
	EventID          string                `json:"event_id"`
	IdempotencyKey   string                `json:"aggregate_key"`
	AggregateKey     string                `json:"aggregate_key"`
	CommandType      string                `json:"command_type"`
	GatewayPeerID    string                `json:"gateway_pubkey_hex"`
	StartedAtUnix    int64                 `json:"started_at_unix"`
	EndedAtUnix      int64                 `json:"ended_at_unix"`
	StepsCount       int                   `json:"steps_count"`
	LatestEventType  string                `json:"latest_event_type"`
	LatestTaskStatus string                `json:"latest_task_status"`
	LastErrorMessage string                `json:"last_error_message"`
	Steps            []orchestratorLogStep `json:"steps"`
}

type schedulerTaskFilter struct {
	Mode        string
	Owner       string
	NamePrefix  string
	Status      string
	InFlightSet bool
	InFlight    bool
	HasErrorSet bool
	HasError    bool
	OrderBy     string
}

type schedulerTaskPage struct {
	EnabledTaskCount int
	InFlightCount    int
	FailureTotal     uint64
	ActiveCount      int
	StoppedCount     int
	FilterCount      int
	Items            []schedulerTaskItem
}

type schedulerTaskItem struct {
	Name              string `json:"name"`
	Owner             string `json:"owner"`
	Mode              string `json:"mode"`
	Status            string `json:"status"`
	IntervalSeconds   int64  `json:"interval_seconds"`
	CreatedAtUnix     int64  `json:"created_at_unix"`
	UpdatedAtUnix     int64  `json:"updated_at_unix"`
	ClosedAtUnix      int64  `json:"closed_at_unix"`
	LastTrigger       string `json:"last_trigger"`
	LastStartedAtUnix int64  `json:"last_started_at_unix"`
	LastEndedAtUnix   int64  `json:"last_ended_at_unix"`
	LastDurationMS    int64  `json:"last_duration_ms"`
	LastError         string `json:"last_error"`
	InFlight          bool   `json:"in_flight"`
	RunCount          uint64 `json:"run_count"`
	SuccessCount      uint64 `json:"success_count"`
	FailureCount      uint64 `json:"failure_count"`
	LastSummary       any    `json:"last_summary"`
}

type schedulerRunFilter struct {
	Limit    int
	Offset   int
	TaskName string
	Owner    string
	Mode     string
	Status   string
}

type schedulerRunPage struct {
	Total int
	Items []schedulerRunItem
}

type schedulerRunItem struct {
	ID            int64  `json:"id"`
	TaskName      string `json:"task_name"`
	Owner         string `json:"owner"`
	Mode          string `json:"mode"`
	Trigger       string `json:"trigger"`
	StartedAtUnix int64  `json:"started_at_unix"`
	EndedAtUnix   int64  `json:"ended_at_unix"`
	DurationMS    int64  `json:"duration_ms"`
	Status        string `json:"status"`
	ErrorMessage  string `json:"error_message"`
	Summary       any    `json:"summary"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

type chainWorkerLogFilter struct {
	Limit  int
	Offset int
	Status string
}

type chainWorkerLogPage struct {
	Total int
	Items []chainWorkerLogItem
}

type chainWorkerLogItem struct {
	ID              int64           `json:"id"`
	TriggeredAtUnix int64           `json:"triggered_at_unix"`
	StartedAtUnix   int64           `json:"started_at_unix"`
	EndedAtUnix     int64           `json:"ended_at_unix"`
	DurationMS      int64           `json:"duration_ms"`
	TriggerSource   string          `json:"trigger_source"`
	Status          string          `json:"status"`
	ErrorMessage    string          `json:"error_message"`
	Result          json.RawMessage `json:"result"`
}

type walletUTXOFilter struct {
	Limit    int
	Offset   int
	WalletID string
	Address  string
	State    string
	TxID     string
	Query    string
}

type walletUTXOPage struct {
	Total int
	Items []walletUTXOItem
}

type walletUTXOItem struct {
	UTXOID                  string `json:"utxo_id"`
	WalletID                string `json:"wallet_id"`
	Address                 string `json:"address"`
	TxID                    string `json:"txid"`
	Vout                    uint32 `json:"vout"`
	ValueSatoshi            uint64 `json:"value_satoshi"`
	State                   string `json:"state"`
	ScriptType              string `json:"script_type"`
	ScriptTypeReason        string `json:"script_type_reason"`
	ScriptTypeUpdatedAtUnix int64  `json:"script_type_updated_at_unix"`
	AllocationClass         string `json:"allocation_class"`
	AllocationReason        string `json:"allocation_reason"`
	CreatedTxID             string `json:"created_txid"`
	SpentTxID               string `json:"spent_txid"`
	CreatedAtUnix           int64  `json:"created_at_unix"`
	UpdatedAtUnix           int64  `json:"updated_at_unix"`
	SpentAtUnix             int64  `json:"spent_at_unix"`
}

func dbListOrchestratorLogs(ctx context.Context, store *clientDB, f orchestratorLogFilter) (orchestratorLogPage, error) {
	if store == nil {
		return orchestratorLogPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (orchestratorLogPage, error) {
		q := root.ProcOrchestratorLogs.Query()
		if f.EventType != "" {
			q = q.Where(bitfsprocorchestratorlogs.EventTypeEQ(f.EventType))
		}
		if f.SignalType != "" {
			q = q.Where(bitfsprocorchestratorlogs.SignalTypeEQ(f.SignalType))
		}
		if f.Source != "" {
			q = q.Where(bitfsprocorchestratorlogs.SourceEQ(f.Source))
		}
		if f.GatewayPeerID != "" {
			q = q.Where(bitfsprocorchestratorlogs.GatewayPubkeyHexEQ(f.GatewayPeerID))
		}
		if f.IdempotencyKey != "" {
			q = q.Where(bitfsprocorchestratorlogs.AggregateKeyEQ(f.IdempotencyKey))
		}
		if f.TaskStatus != "" {
			q = q.Where(bitfsprocorchestratorlogs.TaskStatusEQ(f.TaskStatus))
		}
		rows, err := q.Order(bitfsprocorchestratorlogs.ByID(entsql.OrderDesc())).All(ctx)
		if err != nil {
			return orchestratorLogPage{}, err
		}
		type groupedItem struct {
			item orchestratorLogItem
		}
		grouped := make(map[string]*groupedItem, len(rows))
		for _, row := range rows {
			eventID := strings.TrimSpace(row.AggregateKey)
			if eventID == "" {
				eventID = singleStepOrchestratorEventPrefix + strconv.Itoa(row.ID)
			}
			grp := grouped[eventID]
			if grp == nil {
				grp = &groupedItem{item: orchestratorLogItem{
					EventID:          eventID,
					StartedAtUnix:    row.CreatedAtUnix,
					EndedAtUnix:      row.CreatedAtUnix,
					StepsCount:       0,
					LatestLogID:      int64(row.ID),
					IdempotencyKey:   strings.TrimSpace(row.AggregateKey),
					AggregateKey:     strings.TrimSpace(row.AggregateKey),
					CommandType:      strings.TrimSpace(row.CommandType),
					GatewayPeerID:    strings.TrimSpace(row.GatewayPubkeyHex),
					Source:           strings.TrimSpace(row.Source),
					SignalType:       strings.TrimSpace(row.SignalType),
					LatestEventType:  strings.TrimSpace(row.EventType),
					LatestTaskStatus: strings.TrimSpace(row.TaskStatus),
					LatestRetryCount: int(row.RetryCount),
					LatestQueueLen:   int(row.QueueLength),
					LastErrorMessage: strings.TrimSpace(row.ErrorMessage),
				}}
				grouped[eventID] = grp
			}
			grp.item.StepsCount++
			if int64(row.ID) > grp.item.LatestLogID {
				grp.item.LatestLogID = int64(row.ID)
				grp.item.IdempotencyKey = strings.TrimSpace(row.AggregateKey)
				grp.item.AggregateKey = strings.TrimSpace(row.AggregateKey)
				grp.item.CommandType = strings.TrimSpace(row.CommandType)
				grp.item.GatewayPeerID = strings.TrimSpace(row.GatewayPubkeyHex)
				grp.item.Source = strings.TrimSpace(row.Source)
				grp.item.SignalType = strings.TrimSpace(row.SignalType)
				grp.item.LatestEventType = strings.TrimSpace(row.EventType)
				grp.item.LatestTaskStatus = strings.TrimSpace(row.TaskStatus)
				grp.item.LatestRetryCount = int(row.RetryCount)
				grp.item.LatestQueueLen = int(row.QueueLength)
				grp.item.LastErrorMessage = strings.TrimSpace(row.ErrorMessage)
			}
			if row.CreatedAtUnix < grp.item.StartedAtUnix {
				grp.item.StartedAtUnix = row.CreatedAtUnix
			}
			if row.CreatedAtUnix > grp.item.EndedAtUnix {
				grp.item.EndedAtUnix = row.CreatedAtUnix
			}
		}
		groupedItems := make([]orchestratorLogItem, 0, len(grouped))
		for _, grp := range grouped {
			groupedItems = append(groupedItems, grp.item)
		}
		sort.Slice(groupedItems, func(i, j int) bool {
			if groupedItems[i].LatestLogID == groupedItems[j].LatestLogID {
				return groupedItems[i].EventID > groupedItems[j].EventID
			}
			return groupedItems[i].LatestLogID > groupedItems[j].LatestLogID
		})
		total := len(groupedItems)
		start := f.Offset
		if start < 0 {
			start = 0
		}
		if start > total {
			start = total
		}
		end := total
		if f.Limit >= 0 && start+f.Limit < end {
			end = start + f.Limit
		}
		out := orchestratorLogPage{Total: total, Items: make([]orchestratorLogItem, 0, end-start)}
		out.Items = append(out.Items, groupedItems[start:end]...)
		return out, nil
	})
}

func dbGetOrchestratorLogDetail(ctx context.Context, store *clientDB, eventID string) (orchestratorLogDetail, error) {
	if store == nil {
		return orchestratorLogDetail{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (orchestratorLogDetail, error) {
		var rows []*gen.ProcOrchestratorLogs
		var err error
		if strings.HasPrefix(eventID, singleStepOrchestratorEventPrefix) {
			rawID := strings.TrimPrefix(eventID, singleStepOrchestratorEventPrefix)
			id, perr := strconv.ParseInt(rawID, 10, 64)
			if perr != nil || id <= 0 {
				return orchestratorLogDetail{}, fmt.Errorf("invalid event_id")
			}
			rows, err = root.ProcOrchestratorLogs.Query().
				Where(bitfsprocorchestratorlogs.IDEQ(int(id))).
				Order(bitfsprocorchestratorlogs.ByID()).
				All(ctx)
		} else {
			rows, err = root.ProcOrchestratorLogs.Query().
				Where(bitfsprocorchestratorlogs.AggregateKeyEQ(eventID)).
				Order(bitfsprocorchestratorlogs.ByID()).
				All(ctx)
		}
		if err != nil {
			return orchestratorLogDetail{}, err
		}
		if len(rows) == 0 {
			return orchestratorLogDetail{}, sql.ErrNoRows
		}
		steps := make([]orchestratorLogStep, 0, len(rows))
		for _, row := range rows {
			steps = append(steps, orchestratorLogStep{
				ID:             int64(row.ID),
				CreatedAtUnix:  row.CreatedAtUnix,
				EventType:      row.EventType,
				Source:         row.Source,
				SignalType:     row.SignalType,
				AggregateKey:   row.AggregateKey,
				IdempotencyKey: row.AggregateKey,
				CommandType:    row.CommandType,
				GatewayPeerID:  row.GatewayPubkeyHex,
				TaskStatus:     row.TaskStatus,
				RetryCount:     int(row.RetryCount),
				QueueLength:    int(row.QueueLength),
				ErrorMessage:   row.ErrorMessage,
				Payload:        json.RawMessage(row.PayloadJSON),
			})
		}
		first := steps[0]
		last := steps[len(steps)-1]
		return orchestratorLogDetail{
			EventID:          eventID,
			IdempotencyKey:   strings.TrimSpace(last.IdempotencyKey),
			AggregateKey:     strings.TrimSpace(last.AggregateKey),
			CommandType:      strings.TrimSpace(last.CommandType),
			GatewayPeerID:    strings.TrimSpace(last.GatewayPeerID),
			StartedAtUnix:    first.CreatedAtUnix,
			EndedAtUnix:      last.CreatedAtUnix,
			StepsCount:       len(steps),
			LatestEventType:  strings.TrimSpace(last.EventType),
			LatestTaskStatus: strings.TrimSpace(last.TaskStatus),
			LastErrorMessage: strings.TrimSpace(last.ErrorMessage),
			Steps:            steps,
		}, nil
	})
}

func dbListSchedulerTasks(ctx context.Context, store *clientDB, f schedulerTaskFilter) (schedulerTaskPage, error) {
	if store == nil {
		return schedulerTaskPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (schedulerTaskPage, error) {
		q := root.ProcSchedulerTasks.Query()
		if f.Mode != "" {
			q = q.Where(bitfsprocschedulertasks.ModeEQ(f.Mode))
		}
		if f.Owner != "" {
			q = q.Where(bitfsprocschedulertasks.OwnerEQ(f.Owner))
		}
		if f.Status != "" {
			q = q.Where(bitfsprocschedulertasks.StatusEQ(f.Status))
		}
		if f.NamePrefix != "" {
			q = q.Where(bitfsprocschedulertasks.TaskNameHasPrefix(f.NamePrefix))
		}
		if f.InFlightSet {
			if f.InFlight {
				q = q.Where(bitfsprocschedulertasks.InFlightEQ(1))
			} else {
				q = q.Where(bitfsprocschedulertasks.InFlightEQ(0))
			}
		}
		total, err := root.ProcSchedulerTasks.Query().Count(ctx)
		if err != nil {
			return schedulerTaskPage{}, err
		}
		rows, err := q.All(ctx)
		if err != nil {
			return schedulerTaskPage{}, err
		}
		out := schedulerTaskPage{EnabledTaskCount: total, Items: make([]schedulerTaskItem, 0, len(rows))}
		for _, row := range rows {
			it := schedulerTaskItem{
				Name:              row.TaskName,
				Owner:             row.Owner,
				Mode:              row.Mode,
				Status:            row.Status,
				IntervalSeconds:   row.IntervalSeconds,
				CreatedAtUnix:     row.CreatedAtUnix,
				UpdatedAtUnix:     row.UpdatedAtUnix,
				ClosedAtUnix:      row.ClosedAtUnix,
				LastTrigger:       row.LastTrigger,
				LastStartedAtUnix: row.LastStartedAtUnix,
				LastEndedAtUnix:   row.LastEndedAtUnix,
				LastDurationMS:    row.LastDurationMs,
				LastError:         row.LastError,
				InFlight:          row.InFlight != 0,
				RunCount:          uint64(row.RunCount),
				SuccessCount:      uint64(row.SuccessCount),
				FailureCount:      uint64(row.FailureCount),
			}
			if strings.TrimSpace(row.LastSummaryJSON) != "" {
				var decoded any
				if err := json.Unmarshal([]byte(row.LastSummaryJSON), &decoded); err == nil {
					it.LastSummary = decoded
				} else {
					it.LastSummary = map[string]any{}
				}
			} else {
				it.LastSummary = map[string]any{}
			}
			if f.HasErrorSet {
				hasError := strings.TrimSpace(it.LastError) != ""
				if f.HasError != hasError {
					continue
				}
			}
			if it.InFlight {
				out.InFlightCount++
			}
			if strings.EqualFold(strings.TrimSpace(it.Status), "active") {
				out.ActiveCount++
			} else {
				out.StoppedCount++
			}
			out.FailureTotal += it.FailureCount
			out.Items = append(out.Items, it)
		}
		switch f.OrderBy {
		case "", "default":
			sort.Slice(out.Items, func(i, j int) bool {
				leftActive := strings.EqualFold(strings.TrimSpace(out.Items[i].Status), "active")
				rightActive := strings.EqualFold(strings.TrimSpace(out.Items[j].Status), "active")
				if leftActive != rightActive {
					return leftActive
				}
				leftKey := out.Items[i].LastStartedAtUnix
				if !leftActive {
					leftKey = out.Items[i].ClosedAtUnix
					if leftKey == 0 {
						leftKey = out.Items[i].UpdatedAtUnix
					}
				}
				rightKey := out.Items[j].LastStartedAtUnix
				if !rightActive {
					rightKey = out.Items[j].ClosedAtUnix
					if rightKey == 0 {
						rightKey = out.Items[j].UpdatedAtUnix
					}
				}
				if leftKey != rightKey {
					return leftKey > rightKey
				}
				return out.Items[i].Name < out.Items[j].Name
			})
		case "name_asc":
			sort.Slice(out.Items, func(i, j int) bool { return out.Items[i].Name < out.Items[j].Name })
		case "updated_desc":
			sort.Slice(out.Items, func(i, j int) bool {
				if out.Items[i].UpdatedAtUnix != out.Items[j].UpdatedAtUnix {
					return out.Items[i].UpdatedAtUnix > out.Items[j].UpdatedAtUnix
				}
				return out.Items[i].Name < out.Items[j].Name
			})
		default:
			return schedulerTaskPage{}, fmt.Errorf("invalid order_by")
		}
		if f.Mode != "" {
			out.FilterCount++
		}
		if f.Owner != "" {
			out.FilterCount++
		}
		if f.NamePrefix != "" {
			out.FilterCount++
		}
		if f.Status != "" {
			out.FilterCount++
		}
		if f.InFlightSet {
			out.FilterCount++
		}
		if f.HasErrorSet {
			out.FilterCount++
		}
		return out, nil
	})
}

func dbListSchedulerRuns(ctx context.Context, store *clientDB, f schedulerRunFilter) (schedulerRunPage, error) {
	if store == nil {
		return schedulerRunPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (schedulerRunPage, error) {
		q := root.ProcSchedulerTaskRuns.Query()
		if f.TaskName != "" {
			q = q.Where(bitfsprocschedulertaskruns.TaskNameEQ(f.TaskName))
		}
		if f.Owner != "" {
			q = q.Where(bitfsprocschedulertaskruns.OwnerEQ(f.Owner))
		}
		if f.Mode != "" {
			q = q.Where(bitfsprocschedulertaskruns.ModeEQ(f.Mode))
		}
		if f.Status != "" {
			q = q.Where(bitfsprocschedulertaskruns.StatusEQ(f.Status))
		}
		total, err := q.Count(ctx)
		if err != nil {
			return schedulerRunPage{}, err
		}
		rows, err := q.Order(bitfsprocschedulertaskruns.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
		if err != nil {
			return schedulerRunPage{}, err
		}
		out := schedulerRunPage{Total: total, Items: make([]schedulerRunItem, 0, len(rows))}
		for _, row := range rows {
			var item schedulerRunItem
			item.ID = int64(row.ID)
			item.TaskName = row.TaskName
			item.Owner = row.Owner
			item.Mode = row.Mode
			item.Trigger = row.Trigger
			item.StartedAtUnix = row.StartedAtUnix
			item.EndedAtUnix = row.EndedAtUnix
			item.DurationMS = row.DurationMs
			item.Status = row.Status
			item.ErrorMessage = row.ErrorMessage
			if strings.TrimSpace(row.SummaryJSON) != "" {
				var summary any
				if err := json.Unmarshal([]byte(row.SummaryJSON), &summary); err == nil {
					item.Summary = summary
				} else {
					item.Summary = map[string]any{}
				}
			} else {
				item.Summary = map[string]any{}
			}
			item.CreatedAtUnix = row.CreatedAtUnix
			out.Items = append(out.Items, item)
		}
		return out, nil
	})
}

func dbListChainTipWorkerLogs(ctx context.Context, store *clientDB, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	return dbListChainWorkerLogsByTable(ctx, store, "proc_chain_tip_worker_logs", f)
}

func dbListChainUTXOWorkerLogs(ctx context.Context, store *clientDB, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	return dbListChainWorkerLogsByTable(ctx, store, "proc_chain_utxo_worker_logs", f)
}

func dbListChainWorkerLogsByTable(ctx context.Context, store *clientDB, table string, f chainWorkerLogFilter) (chainWorkerLogPage, error) {
	if store == nil {
		return chainWorkerLogPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (chainWorkerLogPage, error) {
		switch table {
		case "proc_chain_tip_worker_logs":
			q := root.ProcChainTipWorkerLogs.Query()
			if f.Status != "" {
				q = q.Where(bitfsprocchaintipworkerlogs.StatusEQ(f.Status))
			}
			total, err := q.Count(ctx)
			if err != nil {
				return chainWorkerLogPage{}, err
			}
			rows, err := q.Order(bitfsprocchaintipworkerlogs.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
			if err != nil {
				return chainWorkerLogPage{}, err
			}
			out := chainWorkerLogPage{Total: total, Items: make([]chainWorkerLogItem, 0, len(rows))}
			for _, row := range rows {
				out.Items = append(out.Items, chainWorkerLogItem{
					ID:              int64(row.ID),
					TriggeredAtUnix: row.TriggeredAtUnix,
					StartedAtUnix:   row.StartedAtUnix,
					EndedAtUnix:     row.EndedAtUnix,
					DurationMS:      row.DurationMs,
					TriggerSource:   row.TriggerSource,
					Status:          row.Status,
					ErrorMessage:    row.ErrorMessage,
					Result:          json.RawMessage(row.ResultJSON),
				})
			}
			return out, nil
		case "proc_chain_utxo_worker_logs":
			q := root.ProcChainUtxoWorkerLogs.Query()
			if f.Status != "" {
				q = q.Where(bitfsprocchainutxoworkerlogs.StatusEQ(f.Status))
			}
			total, err := q.Count(ctx)
			if err != nil {
				return chainWorkerLogPage{}, err
			}
			rows, err := q.Order(bitfsprocchainutxoworkerlogs.ByID(entsql.OrderDesc())).Limit(f.Limit).Offset(f.Offset).All(ctx)
			if err != nil {
				return chainWorkerLogPage{}, err
			}
			out := chainWorkerLogPage{Total: total, Items: make([]chainWorkerLogItem, 0, len(rows))}
			for _, row := range rows {
				out.Items = append(out.Items, chainWorkerLogItem{
					ID:              int64(row.ID),
					TriggeredAtUnix: row.TriggeredAtUnix,
					StartedAtUnix:   row.StartedAtUnix,
					EndedAtUnix:     row.EndedAtUnix,
					DurationMS:      row.DurationMs,
					TriggerSource:   row.TriggerSource,
					Status:          row.Status,
					ErrorMessage:    row.ErrorMessage,
					Result:          json.RawMessage(row.ResultJSON),
				})
			}
			return out, nil
		default:
			return chainWorkerLogPage{}, fmt.Errorf("unsupported table %s", table)
		}
	})
}

func dbListWalletUTXOs(ctx context.Context, store *clientDB, f walletUTXOFilter) (walletUTXOPage, error) {
	if store == nil {
		return walletUTXOPage{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (walletUTXOPage, error) {
		q := root.WalletUtxo.Query()
		if f.WalletID != "" {
			q = q.Where(bitfswalletutxo.WalletIDEQ(f.WalletID))
		}
		if f.Address != "" {
			q = q.Where(bitfswalletutxo.AddressEQ(f.Address))
		}
		if f.State != "" {
			q = q.Where(bitfswalletutxo.StateEQ(f.State))
		}
		if f.TxID != "" {
			q = q.Where(bitfswalletutxo.TxidEQ(f.TxID))
		}
		rows, err := q.All(ctx)
		if err != nil {
			return walletUTXOPage{}, err
		}
		filtered := make([]*gen.WalletUtxo, 0, len(rows))
		if f.Query != "" {
			qLower := strings.ToLower(strings.TrimSpace(f.Query))
			for _, row := range rows {
				if strings.Contains(strings.ToLower(row.UtxoID), qLower) || strings.Contains(strings.ToLower(row.Txid), qLower) || strings.Contains(strings.ToLower(row.Address), qLower) || strings.Contains(strings.ToLower(row.CreatedTxid), qLower) || strings.Contains(strings.ToLower(row.SpentTxid), qLower) {
					filtered = append(filtered, row)
				}
			}
		} else {
			filtered = rows
		}
		sort.Slice(filtered, func(i, j int) bool {
			if filtered[i].UpdatedAtUnix != filtered[j].UpdatedAtUnix {
				return filtered[i].UpdatedAtUnix > filtered[j].UpdatedAtUnix
			}
			return filtered[i].UtxoID > filtered[j].UtxoID
		})
		total := len(filtered)
		start := 0
		if f.Offset > 0 {
			start = f.Offset
		}
		if start > total {
			start = total
		}
		end := total
		if f.Limit >= 0 && start+f.Limit < end {
			end = start + f.Limit
		}
		out := walletUTXOPage{Total: total, Items: make([]walletUTXOItem, 0, end-start)}
		for _, row := range filtered[start:end] {
			out.Items = append(out.Items, walletUTXOItem{
				UTXOID:                  row.UtxoID,
				WalletID:                row.WalletID,
				Address:                 row.Address,
				TxID:                    row.Txid,
				Vout:                    uint32(row.Vout),
				ValueSatoshi:            uint64(row.ValueSatoshi),
				State:                   row.State,
				ScriptType:              string(normalizeWalletScriptType(row.ScriptType)),
				ScriptTypeReason:        row.ScriptTypeReason,
				ScriptTypeUpdatedAtUnix: row.ScriptTypeUpdatedAtUnix,
				AllocationClass:         walletScriptTypeAllocationClass(row.ScriptType),
				AllocationReason:        row.AllocationReason,
				CreatedTxID:             row.CreatedTxid,
				SpentTxID:               row.SpentTxid,
				CreatedAtUnix:           row.CreatedAtUnix,
				UpdatedAtUnix:           row.UpdatedAtUnix,
				SpentAtUnix:             row.SpentAtUnix,
			})
		}
		return out, nil
	})
}

func dbGetWalletUTXO(ctx context.Context, store *clientDB, utxoID string) (walletUTXOItem, error) {
	if store == nil {
		return walletUTXOItem{}, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (walletUTXOItem, error) {
		row, err := root.WalletUtxo.Query().Where(bitfswalletutxo.UtxoIDEQ(strings.TrimSpace(utxoID))).Only(ctx)
		if err != nil {
			return walletUTXOItem{}, err
		}
		return walletUTXOItem{
			UTXOID:                  row.UtxoID,
			WalletID:                row.WalletID,
			Address:                 row.Address,
			TxID:                    row.Txid,
			Vout:                    uint32(row.Vout),
			ValueSatoshi:            uint64(row.ValueSatoshi),
			State:                   row.State,
			ScriptType:              string(normalizeWalletScriptType(row.ScriptType)),
			ScriptTypeReason:        row.ScriptTypeReason,
			ScriptTypeUpdatedAtUnix: row.ScriptTypeUpdatedAtUnix,
			AllocationClass:         walletScriptTypeAllocationClass(row.ScriptType),
			AllocationReason:        row.AllocationReason,
			CreatedTxID:             row.CreatedTxid,
			SpentTxID:               row.SpentTxid,
			CreatedAtUnix:           row.CreatedAtUnix,
			UpdatedAtUnix:           row.UpdatedAtUnix,
			SpentAtUnix:             row.SpentAtUnix,
		}, nil
	})
}
