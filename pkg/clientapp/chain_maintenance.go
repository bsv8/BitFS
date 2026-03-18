package clientapp

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BFTP/pkg/woc"
)

const (
	chainTipWorkerInterval  = 60 * time.Second
	chainUTXOWorkerInterval = 10 * time.Second
	chainAPITimeout         = 30 * time.Second
	chainWorkerLogKeepCount = 1000
)

const (
	chainTaskTip  = "tip"
	chainTaskUTXO = "utxo"
)

type chainTask struct {
	TaskType      string
	TriggerSource string
	TriggeredAt   int64
}

type chainSchedulerStatus struct {
	StartedAtUnix     int64  `json:"started_at_unix"`
	QueueLength       int    `json:"queue_length"`
	InFlight          bool   `json:"in_flight"`
	InFlightTaskType  string `json:"in_flight_task_type"`
	TaskAppliedCount  uint64 `json:"task_applied_count"`
	TaskFailedCount   uint64 `json:"task_failed_count"`
	TaskSkippedCount  uint64 `json:"task_skipped_count"`
	LastTaskStartedAt int64  `json:"last_task_started_at_unix"`
	LastTaskEndedAt   int64  `json:"last_task_ended_at_unix"`
	LastError         string `json:"last_error"`
}

type chainMaintainer struct {
	rt    *Runtime
	queue chan chainTask

	mu             sync.Mutex
	pendingByType  map[string]bool
	inFlightByType map[string]bool
	status         chainSchedulerStatus
}

type chainTipState struct {
	TipHeight      uint32 `json:"tip_height"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
	LastError      string `json:"last_error"`
	LastUpdatedBy  string `json:"last_updated_by"`
	LastTrigger    string `json:"last_trigger"`
	LastDurationMS int64  `json:"last_duration_ms"`
}

type walletUTXOSyncState struct {
	WalletID       string `json:"wallet_id"`
	Address        string `json:"address"`
	UTXOCount      int    `json:"utxo_count"`
	BalanceSatoshi uint64 `json:"balance_satoshi"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
	LastError      string `json:"last_error"`
	LastUpdatedBy  string `json:"last_updated_by"`
	LastTrigger    string `json:"last_trigger"`
	LastDurationMS int64  `json:"last_duration_ms"`
}

type walletUTXOHistoryCursor struct {
	WalletID            string `json:"wallet_id"`
	Address             string `json:"address"`
	NextConfirmedHeight int64  `json:"next_confirmed_height"`
	NextPageToken       string `json:"next_page_token"`
	AnchorHeight        int64  `json:"anchor_height"`
	RoundTipHeight      int64  `json:"round_tip_height"`
	UpdatedAtUnix       int64  `json:"updated_at_unix"`
	LastError           string `json:"last_error"`
}

type chainWorkerLogEntry struct {
	TriggeredAtUnix int64
	StartedAtUnix   int64
	EndedAtUnix     int64
	DurationMS      int64
	TriggerSource   string
	Status          string
	ErrorMessage    string
	Result          any
}

func newChainMaintainer(rt *Runtime) *chainMaintainer {
	if rt == nil {
		return nil
	}
	return &chainMaintainer{
		rt:            rt,
		queue:         make(chan chainTask, 32),
		pendingByType: map[string]bool{},
		inFlightByType: map[string]bool{
			chainTaskTip:  false,
			chainTaskUTXO: false,
		},
		status: chainSchedulerStatus{
			StartedAtUnix: time.Now().Unix(),
		},
	}
}

func startChainMaintainer(ctx context.Context, rt *Runtime) {
	if rt == nil {
		return
	}
	cm := newChainMaintainer(rt)
	if cm == nil {
		return
	}
	rt.chainMaint = cm
	cm.start(ctx)
}

func getChainMaintainer(rt *Runtime) *chainMaintainer {
	if rt == nil {
		return nil
	}
	return rt.chainMaint
}

func (m *chainMaintainer) start(ctx context.Context) {
	if m == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go m.runScheduler(ctx)
	m.runTipWorker(ctx)
	m.runUTXOWorker(ctx)
	m.enqueue(chainTaskTip, "startup")
	m.enqueue(chainTaskUTXO, "startup")
}

func (m *chainMaintainer) snapshotStatus() chainSchedulerStatus {
	if m == nil {
		return chainSchedulerStatus{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.status
	st.QueueLength = len(m.queue)
	return st
}

func (m *chainMaintainer) runTipWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_tip_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainTipWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskTip, trigger)
			return map[string]any{"task_type": chainTaskTip, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_tip_task_register_failed", map[string]any{"error": err.Error()})
	}
}

func (m *chainMaintainer) runUTXOWorker(ctx context.Context) {
	if m == nil || m.rt == nil {
		return
	}
	scheduler := ensureRuntimeTaskScheduler(m.rt)
	if scheduler == nil {
		return
	}
	if err := scheduler.RegisterPeriodicTask(ctx, periodicTaskSpec{
		Name:      "chain_utxo_sync",
		Owner:     "chain_maintenance",
		Mode:      "static",
		Interval:  chainUTXOWorkerInterval,
		Immediate: false,
		Run: func(_ context.Context, trigger string) (map[string]any, error) {
			m.enqueue(chainTaskUTXO, trigger)
			return map[string]any{"task_type": chainTaskUTXO, "trigger": trigger}, nil
		},
	}); err != nil {
		obs.Error("bitcast-client", "chain_utxo_task_register_failed", map[string]any{"error": err.Error()})
	}
}

func (m *chainMaintainer) enqueue(taskType string, triggerSource string) {
	if m == nil || m.rt == nil || m.rt.DB == nil {
		return
	}
	taskType = strings.TrimSpace(taskType)
	if taskType != chainTaskTip && taskType != chainTaskUTXO {
		return
	}
	now := time.Now().Unix()
	if strings.TrimSpace(triggerSource) == "" {
		triggerSource = "system"
	}
	m.mu.Lock()
	if m.pendingByType[taskType] || m.inFlightByType[taskType] {
		m.status.TaskSkippedCount++
		m.mu.Unlock()
		m.appendWorkerLog(taskType, chainWorkerLogEntry{
			TriggeredAtUnix: now,
			StartedAtUnix:   now,
			EndedAtUnix:     now,
			DurationMS:      0,
			TriggerSource:   triggerSource,
			Status:          "skipped_running",
			ErrorMessage:    "same task is pending or in flight",
			Result: map[string]any{
				"task_type": taskType,
			},
		})
		return
	}
	m.pendingByType[taskType] = true
	m.mu.Unlock()

	task := chainTask{TaskType: taskType, TriggerSource: triggerSource, TriggeredAt: now}
	select {
	case m.queue <- task:
	default:
		m.mu.Lock()
		m.pendingByType[taskType] = false
		m.status.TaskFailedCount++
		m.status.LastError = "chain scheduler queue full"
		m.mu.Unlock()
		m.appendWorkerLog(taskType, chainWorkerLogEntry{
			TriggeredAtUnix: now,
			StartedAtUnix:   now,
			EndedAtUnix:     now,
			DurationMS:      0,
			TriggerSource:   triggerSource,
			Status:          "failed",
			ErrorMessage:    "chain scheduler queue full",
			Result: map[string]any{
				"task_type": taskType,
			},
		})
	}
}

func (m *chainMaintainer) runScheduler(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-m.queue:
			m.runTask(ctx, task)
		}
	}
}

func (m *chainMaintainer) runTask(ctx context.Context, task chainTask) {
	if m == nil || m.rt == nil {
		return
	}
	startedAt := time.Now()
	m.mu.Lock()
	m.pendingByType[task.TaskType] = false
	m.inFlightByType[task.TaskType] = true
	m.status.InFlight = true
	m.status.InFlightTaskType = task.TaskType
	m.status.LastTaskStartedAt = startedAt.Unix()
	m.mu.Unlock()

	defer func() {
		ended := time.Now().Unix()
		m.mu.Lock()
		m.inFlightByType[task.TaskType] = false
		m.status.InFlight = false
		m.status.InFlightTaskType = ""
		m.status.LastTaskEndedAt = ended
		m.mu.Unlock()
	}()

	taskCtx, cancel := context.WithTimeout(ctx, chainAPITimeout)
	defer cancel()

	var err error
	var result map[string]any
	if task.TaskType == chainTaskTip {
		result, err = m.executeTipTask(taskCtx, task)
	} else {
		result, err = m.executeUTXOTask(taskCtx, task)
	}

	endedAt := time.Now()
	entry := chainWorkerLogEntry{
		TriggeredAtUnix: task.TriggeredAt,
		StartedAtUnix:   startedAt.Unix(),
		EndedAtUnix:     endedAt.Unix(),
		DurationMS:      endedAt.Sub(startedAt).Milliseconds(),
		TriggerSource:   task.TriggerSource,
		Result:          result,
	}
	m.mu.Lock()
	if err != nil {
		m.status.TaskFailedCount++
		m.status.LastError = err.Error()
		entry.Status = "failed"
		entry.ErrorMessage = err.Error()
	} else {
		m.status.TaskAppliedCount++
		entry.Status = "success"
		entry.ErrorMessage = ""
	}
	m.mu.Unlock()
	m.appendWorkerLog(task.TaskType, entry)
}

func (m *chainMaintainer) executeTipTask(ctx context.Context, task chainTask) (map[string]any, error) {
	_ = ctx
	if m.rt == nil || m.rt.Chain == nil {
		return map[string]any{"task_type": chainTaskTip}, fmt.Errorf("runtime chain not initialized")
	}
	before, _ := loadChainTipState(m.rt.DB)
	tip, err := m.rt.Chain.GetTipHeight()
	if err != nil {
		updateChainTipStateError(m.rt.DB, err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskTip}, err
	}
	if err := upsertChainTipState(m.rt.DB, tip, "", task.TriggerSource, time.Now().Unix(), 0); err != nil {
		return map[string]any{"task_type": chainTaskTip, "tip_height": tip}, err
	}
	emitted := false
	if before.TipHeight > 0 && tip > before.TipHeight {
		if orch := getClientOrchestrator(m.rt); orch != nil {
			orch.EmitSignal(orchestratorSignal{
				Source:       "chain_tip_worker",
				Type:         orchestratorSignalChainTip,
				AggregateKey: "chain:tip",
				Payload: map[string]any{
					"tip_from": before.TipHeight,
					"tip_to":   tip,
				},
			})
			emitted = true
		}
	}
	return map[string]any{
		"task_type":   chainTaskTip,
		"tip_from":    before.TipHeight,
		"tip_to":      tip,
		"signal_emit": emitted,
	}, nil
}

func (m *chainMaintainer) executeUTXOTask(ctx context.Context, task chainTask) (map[string]any, error) {
	if m.rt == nil || m.rt.Chain == nil {
		return map[string]any{"task_type": chainTaskUTXO}, fmt.Errorf("runtime chain not initialized")
	}
	chain, err := getWalletChainStateClient(m.rt)
	if err != nil {
		return map[string]any{"task_type": chainTaskUTXO}, err
	}
	addr, err := clientWalletAddress(m.rt)
	if err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, "", err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskUTXO}, err
	}
	startedAt := time.Now()
	tip, err := m.rt.Chain.GetTipHeight()
	if err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		updateWalletUTXOHistoryCursorError(m.rt.DB, addr, err.Error())
		return map[string]any{"task_type": chainTaskUTXO, "address": addr}, err
	}
	snapshot, err := collectCurrentWalletSnapshot(ctx, chain, addr)
	if err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		updateWalletUTXOHistoryCursorError(m.rt.DB, addr, err.Error())
		return map[string]any{"task_type": chainTaskUTXO, "address": addr}, err
	}
	cursor, err := loadWalletUTXOHistoryCursor(m.rt.DB, addr)
	if err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		updateWalletUTXOHistoryCursorError(m.rt.DB, addr, err.Error())
		return map[string]any{"task_type": chainTaskUTXO, "address": addr}, err
	}
	cursor.WalletID = walletIDByAddress(addr)
	cursor.Address = addr
	if cursor.AnchorHeight <= 0 {
		cursor.AnchorHeight = snapshot.OldestConfirmedHeight
	}
	if cursor.NextConfirmedHeight <= 0 {
		if cursor.AnchorHeight > 0 {
			cursor.NextConfirmedHeight = cursor.AnchorHeight
		} else {
			cursor.NextConfirmedHeight = int64(tip) + 1
		}
	}
	history, nextCursor, err := collectConfirmedHistoryRange(ctx, chain, addr, cursor, tip)
	if err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		updateWalletUTXOHistoryCursorError(m.rt.DB, addr, err.Error())
		return map[string]any{
			"task_type": chainTaskUTXO,
			"address":   addr,
			"tip":       tip,
		}, err
	}
	durationMS := time.Since(startedAt).Milliseconds()
	if err := reconcileWalletUTXOSet(m.rt.DB, addr, snapshot, history, nextCursor, "", task.TriggerSource, time.Now().Unix(), durationMS); err != nil {
		updateWalletUTXOSyncStateError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		updateWalletUTXOHistoryCursorError(m.rt.DB, addr, err.Error())
		return map[string]any{
			"task_type":      chainTaskUTXO,
			"address":        addr,
			"utxo_count":     snapshot.Count,
			"history_tx_cnt": len(history),
		}, err
	}
	select {
	case <-ctx.Done():
		return map[string]any{
			"task_type":       chainTaskUTXO,
			"address":         addr,
			"utxo_count":      snapshot.Count,
			"balance_satoshi": snapshot.Balance,
		}, ctx.Err()
	default:
	}
	return map[string]any{
		"task_type":       chainTaskUTXO,
		"address":         addr,
		"utxo_count":      snapshot.Count,
		"balance_satoshi": snapshot.Balance,
		"history_tx_cnt":  len(history),
		"cursor_height":   nextCursor.NextConfirmedHeight,
		"anchor_height":   nextCursor.AnchorHeight,
		"tip_height":      tip,
	}, nil
}

func (m *chainMaintainer) appendWorkerLog(taskType string, entry chainWorkerLogEntry) {
	if m == nil || m.rt == nil || m.rt.DB == nil {
		return
	}
	if taskType == chainTaskTip {
		appendChainTipWorkerLog(m.rt.DB, entry)
		return
	}
	appendChainUTXOWorkerLog(m.rt.DB, entry)
}

func clientWalletAddress(rt *Runtime) (string, error) {
	if rt == nil {
		return "", fmt.Errorf("runtime not initialized")
	}
	actor, err := buildClientActorFromRunInput(rt.runIn)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(actor.Addr), nil
}

func loadChainTipState(db *sql.DB) (chainTipState, error) {
	if db == nil {
		return chainTipState{}, fmt.Errorf("db is nil")
	}
	var s chainTipState
	err := db.QueryRow(`SELECT tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms FROM chain_tip_state WHERE id=1`).Scan(
		&s.TipHeight, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return chainTipState{}, nil
		}
		return chainTipState{}, err
	}
	return s, nil
}

func upsertChainTipState(db *sql.DB, tip uint32, lastError string, updatedBy string, updatedAt int64, durationMS int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	_, err := db.Exec(
		`INSERT INTO chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
		 VALUES(1,?,?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
				tip_height=excluded.tip_height,
			updated_at_unix=excluded.updated_at_unix,
			last_error=excluded.last_error,
			last_updated_by=excluded.last_updated_by,
			last_trigger=excluded.last_trigger,
			last_duration_ms=excluded.last_duration_ms`,
		tip,
		updatedAt,
		strings.TrimSpace(lastError),
		"chain_tip_worker",
		strings.TrimSpace(updatedBy),
		durationMS,
	)
	return err
}

func updateChainTipStateError(db *sql.DB, errMsg string, trigger string) {
	now := time.Now().Unix()
	cur, loadErr := loadChainTipState(db)
	if loadErr != nil {
		obs.Error("bitcast-client", "chain_tip_state_load_failed", map[string]any{"error": loadErr.Error()})
		return
	}
	if err := upsertChainTipState(db, cur.TipHeight, errMsg, trigger, now, 0); err != nil {
		obs.Error("bitcast-client", "chain_tip_state_upsert_failed", map[string]any{"error": err.Error()})
	}
}

func loadWalletUTXOSyncState(db *sql.DB, address string) (walletUTXOSyncState, error) {
	if db == nil {
		return walletUTXOSyncState{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOSyncState{}, fmt.Errorf("wallet address is empty")
	}
	var s walletUTXOSyncState
	err := db.QueryRow(
		`SELECT wallet_id,address,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms FROM wallet_utxo_sync_state WHERE address=?`,
		address,
	).Scan(&s.WalletID, &s.Address, &s.UTXOCount, &s.BalanceSatoshi, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS)
	if err != nil {
		if err == sql.ErrNoRows {
			return walletUTXOSyncState{}, nil
		}
		return walletUTXOSyncState{}, err
	}
	return s, nil
}

func loadWalletUTXOHistoryCursor(db *sql.DB, address string) (walletUTXOHistoryCursor, error) {
	if db == nil {
		return walletUTXOHistoryCursor{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOHistoryCursor{}, fmt.Errorf("wallet address is empty")
	}
	var s walletUTXOHistoryCursor
	err := db.QueryRow(
		`SELECT wallet_id,address,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error
		 FROM wallet_utxo_history_cursor WHERE address=?`,
		address,
	).Scan(&s.WalletID, &s.Address, &s.NextConfirmedHeight, &s.NextPageToken, &s.AnchorHeight, &s.RoundTipHeight, &s.UpdatedAtUnix, &s.LastError)
	if err != nil {
		if err == sql.ErrNoRows {
			return walletUTXOHistoryCursor{}, nil
		}
		return walletUTXOHistoryCursor{}, err
	}
	return s, nil
}

func loadWalletUTXOAggregate(db *sql.DB, address string) (int, uint64, error) {
	if db == nil {
		return 0, 0, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return 0, 0, fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	var count int
	var balance uint64
	if err := db.QueryRow(
		`SELECT COUNT(1),COALESCE(SUM(value_satoshi),0) FROM wallet_utxo WHERE wallet_id=? AND address=? AND state='unspent'`,
		walletID, address,
	).Scan(&count, &balance); err != nil {
		return 0, 0, err
	}
	return count, balance, nil
}

type utxoStateRow struct {
	UTXOID        string
	TxID          string
	Vout          uint32
	Value         uint64
	State         string
	CreatedTxID   string
	SpentTxID     string
	CreatedAtUnix int64
}

func walletIDByAddress(address string) string {
	return "wallet:" + strings.ToLower(strings.TrimSpace(address))
}

func appendWalletUTXOEventTx(tx *sql.Tx, utxoID string, eventType string, refTxID string, refBusinessID string, note string, payload any) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	p := "{}"
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			p = string(b)
		}
	}
	_, err := tx.Exec(
		`INSERT INTO wallet_utxo_events(created_at_unix,utxo_id,event_type,ref_txid,ref_business_id,note,payload_json) VALUES(?,?,?,?,?,?,?)`,
		time.Now().Unix(),
		strings.TrimSpace(utxoID),
		strings.TrimSpace(eventType),
		strings.ToLower(strings.TrimSpace(refTxID)),
		strings.TrimSpace(refBusinessID),
		strings.TrimSpace(note),
		p,
	)
	return err
}

type walletHistoryTxRecord struct {
	TxID   string
	Height int64
	Tx     woc.TxDetail
}

type liveWalletSnapshot struct {
	Live                  map[string]dual2of2.UTXO
	ObservedMempoolTxs    []woc.TxDetail
	ConfirmedLiveTxIDs    map[string]struct{}
	Balance               uint64
	Count                 int
	OldestConfirmedHeight int64
}

type walletChainStateClient interface {
	GetUTXOs(address string) ([]woc.UTXO, error)
	GetTipHeight() (uint32, error)
	GetConfirmedHistoryPageContext(ctx context.Context, address string, q woc.ConfirmedHistoryQuery) (woc.ConfirmedHistoryPage, error)
	GetUnconfirmedHistoryContext(ctx context.Context, address string) ([]string, error)
	GetTxDetailContext(ctx context.Context, txid string) (woc.TxDetail, error)
}

func getWalletChainStateClient(rt *Runtime) (walletChainStateClient, error) {
	if rt == nil || rt.Chain == nil {
		return nil, fmt.Errorf("runtime chain not initialized")
	}
	chain, ok := rt.Chain.(walletChainStateClient)
	if !ok {
		return nil, fmt.Errorf("runtime chain missing wallet sync capabilities")
	}
	return chain, nil
}

func reconcileWalletUTXOSet(db *sql.DB, address string, snapshot liveWalletSnapshot, history []walletHistoryTxRecord, cursor walletUTXOHistoryCursor, lastError string, trigger string, updatedAt int64, durationMS int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("wallet address is empty")
	}
	walletID := walletIDByAddress(address)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	rows, err := tx.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
	if err != nil {
		return err
	}
	existing := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if scanErr := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.CreatedTxID, &r.SpentTxID, &r.CreatedAtUnix); scanErr != nil {
			_ = rows.Close()
			return scanErr
		}
		existing[strings.ToLower(strings.TrimSpace(r.UTXOID))] = r
	}
	if err = rows.Close(); err != nil {
		return err
	}
	scriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`UPDATE wallet_utxo SET state='spent',updated_at_unix=?,spent_at_unix=CASE WHEN spent_at_unix>0 THEN spent_at_unix ELSE ? END WHERE wallet_id=? AND address=? AND state<>'spent'`, updatedAt, updatedAt, walletID, address); err != nil {
		return err
	}
	for utxoID, row := range existing {
		if strings.TrimSpace(strings.ToLower(row.State)) != "spent" {
			row.State = "spent"
			existing[utxoID] = row
		}
	}

	for _, hist := range history {
		historyTxID := strings.ToLower(strings.TrimSpace(hist.TxID))
		for _, out := range hist.Tx.Vout {
			utxoID, value, ok := matchWalletOutput(historyTxID, out, scriptHex)
			if !ok {
				continue
			}
			if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, historyTxID, out.N, value, "spent", "", updatedAt); err != nil {
				return err
			}
		}
		for _, in := range hist.Tx.Vin {
			spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
			if _, ok := existing[spentID]; !ok {
				continue
			}
			if err = setWalletUTXOSpentTx(tx, existing, spentID, historyTxID, updatedAt, "confirmed_history_spent"); err != nil {
				return err
			}
		}
	}

	for _, detail := range snapshot.ObservedMempoolTxs {
		mempoolTxID := strings.ToLower(strings.TrimSpace(detail.TxID))
		for _, out := range detail.Vout {
			utxoID, value, ok := matchWalletOutput(mempoolTxID, out, scriptHex)
			if !ok {
				continue
			}
			if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, mempoolTxID, out.N, value, "spent", "", updatedAt); err != nil {
				return err
			}
		}
		for _, in := range detail.Vin {
			spentID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
			if _, ok := existing[spentID]; !ok {
				continue
			}
			if err = setWalletUTXOSpentTx(tx, existing, spentID, mempoolTxID, updatedAt, "mempool_spent"); err != nil {
				return err
			}
		}
	}

	for utxoID, u := range snapshot.Live {
		if err = upsertWalletUTXORowTx(tx, existing, walletID, address, utxoID, strings.ToLower(strings.TrimSpace(u.TxID)), u.Vout, u.Value, "unspent", "", updatedAt); err != nil {
			return err
		}
	}

	if _, err = tx.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
		 VALUES(?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(address) DO UPDATE SET
			wallet_id=excluded.wallet_id,
			utxo_count=excluded.utxo_count,
			balance_satoshi=excluded.balance_satoshi,
			updated_at_unix=excluded.updated_at_unix,
			last_error=excluded.last_error,
			last_updated_by=excluded.last_updated_by,
			last_trigger=excluded.last_trigger,
			last_duration_ms=excluded.last_duration_ms`,
		address, walletID,
		snapshot.Count,
		snapshot.Balance,
		updatedAt,
		strings.TrimSpace(lastError),
		"chain_utxo_worker",
		strings.TrimSpace(trigger),
		durationMS,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`INSERT INTO wallet_utxo_history_cursor(address,wallet_id,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error)
		 VALUES(?,?,?,?,?,?,?,?)
		 ON CONFLICT(address) DO UPDATE SET
			wallet_id=excluded.wallet_id,
			next_confirmed_height=excluded.next_confirmed_height,
			next_page_token=excluded.next_page_token,
			anchor_height=excluded.anchor_height,
			round_tip_height=excluded.round_tip_height,
			updated_at_unix=excluded.updated_at_unix,
			last_error=excluded.last_error`,
		address, walletID, cursor.NextConfirmedHeight, strings.TrimSpace(cursor.NextPageToken), cursor.AnchorHeight, cursor.RoundTipHeight, updatedAt, strings.TrimSpace(cursor.LastError),
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func upsertWalletUTXORowTx(tx *sql.Tx, existing map[string]utxoStateRow, walletID string, address string, utxoID string, txid string, vout uint32, value uint64, state string, spentTxID string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	txid = strings.ToLower(strings.TrimSpace(txid))
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	row, ok := existing[utxoID]
	createdAtUnix := updatedAt
	if ok && row.CreatedAtUnix > 0 {
		createdAtUnix = row.CreatedAtUnix
	}
	spentAtUnix := int64(0)
	if state == "spent" {
		spentAtUnix = updatedAt
	}
	if ok {
		_, err := tx.Exec(
			`UPDATE wallet_utxo
			 SET txid=?,vout=?,value_satoshi=?,state=?,created_txid=?,spent_txid=?,updated_at_unix=?,spent_at_unix=?
			 WHERE utxo_id=?`,
			txid, vout, value, strings.TrimSpace(state), txid, spentTxID, updatedAt, spentAtUnix, utxoID,
		)
		if err != nil {
			return err
		}
		row.TxID = txid
		row.Vout = vout
		row.Value = value
		row.State = state
		row.CreatedTxID = txid
		row.SpentTxID = spentTxID
		existing[utxoID] = row
		return nil
	}
	_, err := tx.Exec(
		`INSERT INTO wallet_utxo(
			utxo_id,wallet_id,address,txid,vout,value_satoshi,state,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		utxoID, walletID, address, txid, vout, value, strings.TrimSpace(state), txid, spentTxID, createdAtUnix, updatedAt, spentAtUnix,
	)
	if err != nil {
		return err
	}
	existing[utxoID] = utxoStateRow{
		UTXOID:        utxoID,
		TxID:          txid,
		Vout:          vout,
		Value:         value,
		State:         state,
		CreatedTxID:   txid,
		SpentTxID:     spentTxID,
		CreatedAtUnix: createdAtUnix,
	}
	return appendWalletUTXOEventTx(tx, utxoID, "detected", txid, "", "utxo detected by chain sync", map[string]any{"state": state})
}

func setWalletUTXOSpentTx(tx *sql.Tx, existing map[string]utxoStateRow, utxoID string, spentTxID string, updatedAt int64, eventType string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	row, ok := existing[utxoID]
	if !ok {
		return nil
	}
	spentTxID = strings.ToLower(strings.TrimSpace(spentTxID))
	prevState := strings.TrimSpace(strings.ToLower(row.State))
	prevSpentTxID := strings.TrimSpace(strings.ToLower(row.SpentTxID))
	if _, err := tx.Exec(`UPDATE wallet_utxo SET state='spent',spent_txid=?,spent_at_unix=?,updated_at_unix=? WHERE utxo_id=?`, spentTxID, updatedAt, updatedAt, utxoID); err != nil {
		return err
	}
	row.State = "spent"
	row.SpentTxID = spentTxID
	existing[utxoID] = row
	if prevState == "spent" && prevSpentTxID == spentTxID {
		return nil
	}
	return appendWalletUTXOEventTx(tx, utxoID, eventType, spentTxID, "", "utxo spent by chain sync", nil)
}

func collectCurrentWalletSnapshot(ctx context.Context, chain walletChainStateClient, address string) (liveWalletSnapshot, error) {
	confirmedUTXOs, err := chain.GetUTXOs(address)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	current := map[string]dual2of2.UTXO{}
	confirmedLiveTxIDs := map[string]struct{}{}
	for _, u := range confirmedUTXOs {
		txid := strings.ToLower(strings.TrimSpace(u.TxID))
		utxoID := txid + ":" + fmt.Sprint(u.Vout)
		current[utxoID] = dual2of2.UTXO{TxID: txid, Vout: u.Vout, Value: u.Value}
		confirmedLiveTxIDs[txid] = struct{}{}
	}
	unconfirmedTxIDs, err := chain.GetUnconfirmedHistoryContext(ctx, address)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	scriptHex, err := walletAddressLockScriptHex(address)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	details, err := loadOrderedTxDetails(ctx, chain, unconfirmedTxIDs)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	for _, detail := range details {
		txid := strings.ToLower(strings.TrimSpace(detail.TxID))
		for _, in := range detail.Vin {
			prevID := strings.ToLower(strings.TrimSpace(in.TxID)) + ":" + fmt.Sprint(in.Vout)
			delete(current, prevID)
			delete(confirmedLiveTxIDs, strings.ToLower(strings.TrimSpace(in.TxID)))
		}
		for _, out := range detail.Vout {
			if strings.TrimSpace(strings.ToLower(out.ScriptPubKey.Hex)) != scriptHex {
				continue
			}
			utxoID := txid + ":" + fmt.Sprint(out.N)
			current[utxoID] = dual2of2.UTXO{
				TxID:  txid,
				Vout:  out.N,
				Value: satoshiFromTxOutputValue(out.Value),
			}
		}
	}
	oldestConfirmedHeight, err := findOldestCurrentConfirmedHeight(ctx, chain, address, confirmedLiveTxIDs)
	if err != nil {
		return liveWalletSnapshot{}, err
	}
	var balance uint64
	for _, u := range current {
		balance += u.Value
	}
	return liveWalletSnapshot{
		Live:                  current,
		ObservedMempoolTxs:    details,
		ConfirmedLiveTxIDs:    confirmedLiveTxIDs,
		Balance:               balance,
		Count:                 len(current),
		OldestConfirmedHeight: oldestConfirmedHeight,
	}, nil
}

func loadOrderedTxDetails(ctx context.Context, chain walletChainStateClient, txids []string) ([]woc.TxDetail, error) {
	unique := map[string]woc.TxDetail{}
	for _, txid := range txids {
		txid = strings.ToLower(strings.TrimSpace(txid))
		if txid == "" {
			continue
		}
		if _, ok := unique[txid]; ok {
			continue
		}
		detail, err := chain.GetTxDetailContext(ctx, txid)
		if err != nil {
			return nil, err
		}
		detail.TxID = txid
		unique[txid] = detail
	}
	pending := map[string]woc.TxDetail{}
	for txid, detail := range unique {
		pending[txid] = detail
	}
	out := make([]woc.TxDetail, 0, len(unique))
	for len(pending) > 0 {
		progressed := false
		for txid, detail := range pending {
			ready := true
			for _, in := range detail.Vin {
				prevTxID := strings.ToLower(strings.TrimSpace(in.TxID))
				if prevTxID == "" || prevTxID == txid {
					continue
				}
				if _, waiting := pending[prevTxID]; waiting {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			out = append(out, detail)
			delete(pending, txid)
			progressed = true
		}
		if progressed {
			continue
		}
		for txid, detail := range pending {
			out = append(out, detail)
			delete(pending, txid)
		}
	}
	return out, nil
}

func findOldestCurrentConfirmedHeight(ctx context.Context, chain walletChainStateClient, address string, txids map[string]struct{}) (int64, error) {
	if len(txids) == 0 {
		return 0, nil
	}
	remaining := map[string]struct{}{}
	for txid := range txids {
		remaining[txid] = struct{}{}
	}
	oldest := int64(0)
	token := ""
	for {
		page, err := chain.GetConfirmedHistoryPageContext(ctx, address, woc.ConfirmedHistoryQuery{
			Order: "desc",
			Limit: 1000,
			Token: token,
		})
		if err != nil {
			return 0, err
		}
		for _, item := range page.Items {
			txid := strings.ToLower(strings.TrimSpace(item.TxID))
			if _, ok := remaining[txid]; !ok {
				continue
			}
			delete(remaining, txid)
			if oldest == 0 || item.Height < oldest {
				oldest = item.Height
			}
		}
		if len(remaining) == 0 || strings.TrimSpace(page.NextPageToken) == "" {
			return oldest, nil
		}
		token = strings.TrimSpace(page.NextPageToken)
	}
}

func collectConfirmedHistoryRange(ctx context.Context, chain walletChainStateClient, address string, cursor walletUTXOHistoryCursor, tip uint32) ([]walletHistoryTxRecord, walletUTXOHistoryCursor, error) {
	next := cursor
	next.RoundTipHeight = int64(tip)
	next.UpdatedAtUnix = time.Now().Unix()
	if next.NextConfirmedHeight <= 0 {
		if next.AnchorHeight > 0 {
			next.NextConfirmedHeight = next.AnchorHeight
		} else {
			next.NextConfirmedHeight = int64(tip) + 1
		}
	}
	if next.NextConfirmedHeight > int64(tip) && strings.TrimSpace(next.NextPageToken) == "" {
		next.LastError = ""
		return nil, next, nil
	}
	out := make([]walletHistoryTxRecord, 0)
	seen := map[string]struct{}{}
	startHeight := next.NextConfirmedHeight
	pageToken := strings.TrimSpace(next.NextPageToken)
	for {
		page, err := chain.GetConfirmedHistoryPageContext(ctx, address, woc.ConfirmedHistoryQuery{
			Order:  "asc",
			Limit:  1000,
			Height: startHeight,
			Token:  pageToken,
		})
		if err != nil {
			return nil, next, err
		}
		for _, item := range page.Items {
			txid := strings.ToLower(strings.TrimSpace(item.TxID))
			if txid == "" {
				continue
			}
			if _, ok := seen[txid]; ok {
				continue
			}
			seen[txid] = struct{}{}
			detail, err := chain.GetTxDetailContext(ctx, txid)
			if err != nil {
				return nil, next, err
			}
			out = append(out, walletHistoryTxRecord{
				TxID:   txid,
				Height: item.Height,
				Tx:     detail,
			})
		}
		if token := strings.TrimSpace(page.NextPageToken); token != "" {
			pageToken = token
			next.NextConfirmedHeight = startHeight
			next.NextPageToken = token
			continue
		}
		next.NextConfirmedHeight = int64(tip) + 1
		next.NextPageToken = ""
		next.LastError = ""
		return out, next, nil
	}
}

func walletAddressLockScriptHex(address string) (string, error) {
	addr, err := script.NewAddressFromString(strings.TrimSpace(address))
	if err != nil {
		return "", fmt.Errorf("parse wallet address failed: %w", err)
	}
	lock, err := p2pkh.Lock(addr)
	if err != nil {
		return "", fmt.Errorf("build wallet lock script failed: %w", err)
	}
	return strings.ToLower(hex.EncodeToString(lock.Bytes())), nil
}

func matchWalletOutput(txid string, out woc.TxOutput, scriptHex string) (string, uint64, bool) {
	if strings.TrimSpace(txid) == "" {
		return "", 0, false
	}
	if strings.TrimSpace(strings.ToLower(out.ScriptPubKey.Hex)) != strings.TrimSpace(strings.ToLower(scriptHex)) {
		return "", 0, false
	}
	return strings.ToLower(strings.TrimSpace(txid)) + ":" + fmt.Sprint(out.N), satoshiFromTxOutputValue(out.Value), true
}

func satoshiFromTxOutputValue(v float64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v*100000000 + 0.5)
}

func updateWalletUTXOSyncStateError(db *sql.DB, address string, errMsg string, trigger string) {
	if db == nil {
		return
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
		 VALUES(?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(address) DO UPDATE SET
			wallet_id=excluded.wallet_id,
			updated_at_unix=excluded.updated_at_unix,
			last_error=excluded.last_error,
			last_updated_by=excluded.last_updated_by,
			last_trigger=excluded.last_trigger,
			last_duration_ms=excluded.last_duration_ms`,
		address, walletID, 0, 0, now, strings.TrimSpace(errMsg), "chain_utxo_worker", strings.TrimSpace(trigger), 0,
	)
	if err != nil {
		obs.Error("bitcast-client", "wallet_utxo_sync_state_upsert_failed", map[string]any{"error": err.Error(), "address": address})
	}
}

func updateWalletUTXOHistoryCursorError(db *sql.DB, address string, errMsg string) {
	if db == nil {
		return
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return
	}
	walletID := walletIDByAddress(address)
	now := time.Now().Unix()
	_, err := db.Exec(
		`INSERT INTO wallet_utxo_history_cursor(address,wallet_id,next_confirmed_height,next_page_token,anchor_height,round_tip_height,updated_at_unix,last_error)
		 VALUES(?,?,?,?,?,?,?,?)
		 ON CONFLICT(address) DO UPDATE SET
			wallet_id=excluded.wallet_id,
			updated_at_unix=excluded.updated_at_unix,
			last_error=excluded.last_error`,
		address, walletID, 0, "", 0, 0, now, strings.TrimSpace(errMsg),
	)
	if err != nil {
		obs.Error("bitcast-client", "wallet_utxo_history_cursor_upsert_failed", map[string]any{"error": err.Error(), "address": address})
	}
}

func getTipHeightFromDB(rt *Runtime) (uint32, error) {
	if rt == nil || rt.DB == nil {
		return 0, fmt.Errorf("runtime not initialized")
	}
	s, err := loadChainTipState(rt.DB)
	if err != nil {
		return 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return 0, fmt.Errorf("chain tip state not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return 0, fmt.Errorf("chain tip state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	return s.TipHeight, nil
}

func getWalletUTXOsFromDB(rt *Runtime) ([]dual2of2.UTXO, error) {
	if rt == nil || rt.DB == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return nil, err
	}
	s, err := loadWalletUTXOSyncState(rt.DB, addr)
	if err != nil {
		return nil, err
	}
	if s.UpdatedAtUnix <= 0 {
		return nil, fmt.Errorf("wallet utxo sync state not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return nil, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	walletID := walletIDByAddress(addr)
	rows, err := rt.DB.Query(`SELECT txid,vout,value_satoshi FROM wallet_utxo WHERE wallet_id=? AND address=? AND state='unspent' ORDER BY value_satoshi ASC,txid ASC,vout ASC`, walletID, addr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]dual2of2.UTXO, 0, s.UTXOCount)
	for rows.Next() {
		var u dual2of2.UTXO
		if err := rows.Scan(&u.TxID, &u.Vout, &u.Value); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func getWalletBalanceFromDB(rt *Runtime) (string, uint64, error) {
	if rt == nil || rt.DB == nil {
		return "", 0, fmt.Errorf("runtime not initialized")
	}
	addr, err := clientWalletAddress(rt)
	if err != nil {
		return "", 0, err
	}
	s, err := loadWalletUTXOSyncState(rt.DB, addr)
	if err != nil {
		return addr, 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return addr, 0, fmt.Errorf("wallet utxo sync state not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return addr, 0, fmt.Errorf("wallet utxo sync state unavailable: %s", strings.TrimSpace(s.LastError))
	}
	walletID := walletIDByAddress(addr)
	var balance uint64
	if err := rt.DB.QueryRow(`SELECT COALESCE(SUM(value_satoshi),0) FROM wallet_utxo WHERE wallet_id=? AND address=? AND state='unspent'`, walletID, addr).Scan(&balance); err != nil {
		return addr, 0, err
	}
	return addr, balance, nil
}

func appendChainTipWorkerLog(db *sql.DB, e chainWorkerLogEntry) {
	if db == nil {
		return
	}
	if e.TriggeredAtUnix <= 0 {
		e.TriggeredAtUnix = time.Now().Unix()
	}
	if e.StartedAtUnix <= 0 {
		e.StartedAtUnix = e.TriggeredAtUnix
	}
	if e.EndedAtUnix <= 0 {
		e.EndedAtUnix = e.StartedAtUnix
	}
	_, err := db.Exec(
		`INSERT INTO chain_tip_worker_logs(triggered_at_unix,started_at_unix,ended_at_unix,duration_ms,trigger_source,status,error_message,result_json)
		 VALUES(?,?,?,?,?,?,?,?)`,
		e.TriggeredAtUnix,
		e.StartedAtUnix,
		e.EndedAtUnix,
		e.DurationMS,
		strings.TrimSpace(e.TriggerSource),
		strings.TrimSpace(e.Status),
		strings.TrimSpace(e.ErrorMessage),
		mustJSON(e.Result),
	)
	if err != nil {
		obs.Error("bitcast-client", "chain_tip_worker_log_append_failed", map[string]any{"error": err.Error()})
		return
	}
	trimWorkerLogs(db, "chain_tip_worker_logs", chainWorkerLogKeepCount)
}

func appendChainUTXOWorkerLog(db *sql.DB, e chainWorkerLogEntry) {
	if db == nil {
		return
	}
	if e.TriggeredAtUnix <= 0 {
		e.TriggeredAtUnix = time.Now().Unix()
	}
	if e.StartedAtUnix <= 0 {
		e.StartedAtUnix = e.TriggeredAtUnix
	}
	if e.EndedAtUnix <= 0 {
		e.EndedAtUnix = e.StartedAtUnix
	}
	_, err := db.Exec(
		`INSERT INTO chain_utxo_worker_logs(triggered_at_unix,started_at_unix,ended_at_unix,duration_ms,trigger_source,status,error_message,result_json)
		 VALUES(?,?,?,?,?,?,?,?)`,
		e.TriggeredAtUnix,
		e.StartedAtUnix,
		e.EndedAtUnix,
		e.DurationMS,
		strings.TrimSpace(e.TriggerSource),
		strings.TrimSpace(e.Status),
		strings.TrimSpace(e.ErrorMessage),
		mustJSON(e.Result),
	)
	if err != nil {
		obs.Error("bitcast-client", "chain_utxo_worker_log_append_failed", map[string]any{"error": err.Error()})
		return
	}
	trimWorkerLogs(db, "chain_utxo_worker_logs", chainWorkerLogKeepCount)
}

func trimWorkerLogs(db *sql.DB, table string, keep int) {
	if db == nil || strings.TrimSpace(table) == "" || keep <= 0 {
		return
	}
	stmt := fmt.Sprintf(
		"DELETE FROM %s WHERE id NOT IN (SELECT id FROM %s ORDER BY id DESC LIMIT ?)",
		table,
		table,
	)
	if _, err := db.Exec(stmt, keep); err != nil {
		obs.Error("bitcast-client", "chain_worker_log_trim_failed", map[string]any{"error": err.Error(), "table": table})
	}
}
