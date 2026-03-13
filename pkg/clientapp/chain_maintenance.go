package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/obs"
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

type chainTipSnapshot struct {
	TipHeight      uint32 `json:"tip_height"`
	UpdatedAtUnix  int64  `json:"updated_at_unix"`
	LastError      string `json:"last_error"`
	LastUpdatedBy  string `json:"last_updated_by"`
	LastTrigger    string `json:"last_trigger"`
	LastDurationMS int64  `json:"last_duration_ms"`
}

type walletUTXOSnapshot struct {
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
	before, _ := loadChainTipSnapshot(m.rt.DB)
	tip, err := m.rt.Chain.GetTipHeight()
	if err != nil {
		updateChainTipSnapshotError(m.rt.DB, err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskTip}, err
	}
	if err := upsertChainTipSnapshot(m.rt.DB, tip, "", task.TriggerSource, time.Now().Unix(), 0); err != nil {
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
	addr, err := clientWalletAddress(m.rt)
	if err != nil {
		updateWalletUTXOSnapshotError(m.rt.DB, "", err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskUTXO}, err
	}
	utxos, err := m.rt.Chain.GetUTXOs(addr)
	if err != nil {
		updateWalletUTXOSnapshotError(m.rt.DB, addr, err.Error(), task.TriggerSource)
		return map[string]any{"task_type": chainTaskUTXO, "address": addr}, err
	}
	var sum uint64
	for _, u := range utxos {
		sum += u.Value
	}
	now := time.Now().Unix()
	if err := reconcileWalletUTXOSet(m.rt.DB, addr, utxos, sum, "", task.TriggerSource, now, 0); err != nil {
		return map[string]any{"task_type": chainTaskUTXO, "address": addr, "utxo_count": len(utxos)}, err
	}
	select {
	case <-ctx.Done():
		return map[string]any{"task_type": chainTaskUTXO, "address": addr, "utxo_count": len(utxos), "balance_satoshi": sum}, ctx.Err()
	default:
	}
	return map[string]any{
		"task_type":       chainTaskUTXO,
		"address":         addr,
		"utxo_count":      len(utxos),
		"balance_satoshi": sum,
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

func loadChainTipSnapshot(db *sql.DB) (chainTipSnapshot, error) {
	if db == nil {
		return chainTipSnapshot{}, fmt.Errorf("db is nil")
	}
	var s chainTipSnapshot
	err := db.QueryRow(`SELECT tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms FROM chain_tip_snapshot WHERE id=1`).Scan(
		&s.TipHeight, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return chainTipSnapshot{}, nil
		}
		return chainTipSnapshot{}, err
	}
	return s, nil
}

func upsertChainTipSnapshot(db *sql.DB, tip uint32, lastError string, updatedBy string, updatedAt int64, durationMS int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	_, err := db.Exec(
		`INSERT INTO chain_tip_snapshot(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
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

func updateChainTipSnapshotError(db *sql.DB, errMsg string, trigger string) {
	now := time.Now().Unix()
	cur, loadErr := loadChainTipSnapshot(db)
	if loadErr != nil {
		obs.Error("bitcast-client", "chain_tip_snapshot_load_failed", map[string]any{"error": loadErr.Error()})
		return
	}
	if err := upsertChainTipSnapshot(db, cur.TipHeight, errMsg, trigger, now, 0); err != nil {
		obs.Error("bitcast-client", "chain_tip_snapshot_upsert_failed", map[string]any{"error": err.Error()})
	}
}

func loadWalletUTXOSnapshot(db *sql.DB, address string) (walletUTXOSnapshot, error) {
	if db == nil {
		return walletUTXOSnapshot{}, fmt.Errorf("db is nil")
	}
	address = strings.TrimSpace(address)
	if address == "" {
		return walletUTXOSnapshot{}, fmt.Errorf("wallet address is empty")
	}
	var s walletUTXOSnapshot
	err := db.QueryRow(
		`SELECT wallet_id,address,utxo_count,balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms FROM wallet_utxo_sync_state WHERE address=?`,
		address,
	).Scan(&s.WalletID, &s.Address, &s.UTXOCount, &s.BalanceSatoshi, &s.UpdatedAtUnix, &s.LastError, &s.LastUpdatedBy, &s.LastTrigger, &s.LastDurationMS)
	if err != nil {
		if err == sql.ErrNoRows {
			return walletUTXOSnapshot{}, nil
		}
		return walletUTXOSnapshot{}, err
	}
	return s, nil
}

type utxoStateRow struct {
	UTXOID     string
	TxID       string
	Vout       uint32
	Value      uint64
	State      string
	OriginType string
}

func walletIDByAddress(address string) string {
	return "wallet:" + strings.ToLower(strings.TrimSpace(address))
}

func inferUTXOOriginByLink(tx *sql.Tx, utxoID string) (string, int, error) {
	var role string
	err := tx.QueryRow(`SELECT role FROM biz_utxo_links WHERE utxo_id=? ORDER BY id DESC LIMIT 1`, utxoID).Scan(&role)
	if err != nil {
		if err == sql.ErrNoRows {
			return "unknown", 0, nil
		}
		return "", 0, err
	}
	role = strings.TrimSpace(strings.ToLower(role))
	switch role {
	case "change":
		return "internal_change", 0, nil
	case "lock":
		return "internal_lock", 0, nil
	case "release":
		return "internal_release", 0, nil
	case "receive":
		return "external_in", 1, nil
	default:
		return "unknown", 0, nil
	}
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

func reconcileWalletUTXOSet(db *sql.DB, address string, utxos []dual2of2.UTXO, balance uint64, lastError string, trigger string, updatedAt int64, durationMS int64) error {
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
	rows, err := tx.Query(`SELECT utxo_id,txid,vout,value_satoshi,state,origin_type FROM wallet_utxo WHERE wallet_id=? AND address=?`, walletID, address)
	if err != nil {
		return err
	}
	existing := map[string]utxoStateRow{}
	for rows.Next() {
		var r utxoStateRow
		if scanErr := rows.Scan(&r.UTXOID, &r.TxID, &r.Vout, &r.Value, &r.State, &r.OriginType); scanErr != nil {
			_ = rows.Close()
			return scanErr
		}
		existing[strings.ToLower(strings.TrimSpace(r.UTXOID))] = r
	}
	if err = rows.Close(); err != nil {
		return err
	}

	seen := map[string]struct{}{}
	for _, u := range utxos {
		txid := strings.ToLower(strings.TrimSpace(u.TxID))
		utxoID := txid + ":" + fmt.Sprint(u.Vout)
		seen[utxoID] = struct{}{}
		if old, ok := existing[utxoID]; ok {
			nextState := "unspent"
			if strings.TrimSpace(strings.ToLower(old.State)) == "reserved" {
				nextState = "reserved"
			}
			if _, err = tx.Exec(
				`UPDATE wallet_utxo
				 SET value_satoshi=?,state=?,updated_at_unix=?,spent_txid=CASE WHEN ?='spent' THEN spent_txid ELSE '' END,spent_at_unix=CASE WHEN ?='spent' THEN spent_at_unix ELSE 0 END
				 WHERE utxo_id=?`,
				u.Value, nextState, updatedAt, nextState, nextState, utxoID,
			); err != nil {
				return err
			}
			if strings.TrimSpace(strings.ToLower(old.State)) == "spent" {
				if err = appendWalletUTXOEventTx(tx, utxoID, "reorg_unspent", txid, "", "utxo reappeared in chain set", map[string]any{"trigger": trigger}); err != nil {
					return err
				}
			}
			continue
		}
		originType, incomeEligible, inferErr := inferUTXOOriginByLink(tx, utxoID)
		if inferErr != nil {
			return inferErr
		}
		if _, err = tx.Exec(
			`INSERT INTO wallet_utxo(
				utxo_id,wallet_id,address,txid,vout,value_satoshi,state,origin_type,income_eligible,created_txid,spent_txid,reserved_by,reserved_at_unix,created_at_unix,updated_at_unix,spent_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			utxoID, walletID, address, txid, u.Vout, u.Value, "unspent", originType, incomeEligible, txid, "", "", 0, updatedAt, updatedAt, 0,
		); err != nil {
			return err
		}
		if err = appendWalletUTXOEventTx(tx, utxoID, "detected", txid, "", "new utxo detected from chain", map[string]any{"trigger": trigger, "origin_type": originType}); err != nil {
			return err
		}
	}

	for utxoID, old := range existing {
		if _, ok := seen[utxoID]; ok {
			continue
		}
		oldState := strings.TrimSpace(strings.ToLower(old.State))
		if oldState == "spent" {
			continue
		}
		if _, err = tx.Exec(
			`UPDATE wallet_utxo SET state='spent',spent_txid=COALESCE(NULLIF(spent_txid,''),''),spent_at_unix=?,updated_at_unix=? WHERE utxo_id=?`,
			updatedAt, updatedAt, utxoID,
		); err != nil {
			return err
		}
		if err = appendWalletUTXOEventTx(tx, utxoID, "spent", "", "", "utxo disappeared from chain unspent set", map[string]any{"trigger": trigger}); err != nil {
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
		len(utxos),
		balance,
		updatedAt,
		strings.TrimSpace(lastError),
		"chain_utxo_worker",
		strings.TrimSpace(trigger),
		durationMS,
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func updateWalletUTXOSnapshotError(db *sql.DB, address string, errMsg string, trigger string) {
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
		obs.Error("bitcast-client", "wallet_utxo_snapshot_upsert_failed", map[string]any{"error": err.Error(), "address": address})
	}
}

func getTipHeightFromDB(rt *Runtime) (uint32, error) {
	if rt == nil || rt.DB == nil {
		return 0, fmt.Errorf("runtime not initialized")
	}
	s, err := loadChainTipSnapshot(rt.DB)
	if err != nil {
		return 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return 0, fmt.Errorf("chain tip snapshot not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return 0, fmt.Errorf("chain tip snapshot unavailable: %s", strings.TrimSpace(s.LastError))
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
	s, err := loadWalletUTXOSnapshot(rt.DB, addr)
	if err != nil {
		return nil, err
	}
	if s.UpdatedAtUnix <= 0 {
		return nil, fmt.Errorf("wallet utxo snapshot not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return nil, fmt.Errorf("wallet utxo snapshot unavailable: %s", strings.TrimSpace(s.LastError))
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
	s, err := loadWalletUTXOSnapshot(rt.DB, addr)
	if err != nil {
		return addr, 0, err
	}
	if s.UpdatedAtUnix <= 0 {
		return addr, 0, fmt.Errorf("wallet utxo snapshot not ready")
	}
	if strings.TrimSpace(s.LastError) != "" {
		return addr, 0, fmt.Errorf("wallet utxo snapshot unavailable: %s", strings.TrimSpace(s.LastError))
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
