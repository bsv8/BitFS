package poolcore

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	serviceStateInactive    = "inactive"
	serviceStateServing     = "serving"
	serviceStatePaused      = "paused"
	serviceStateInterrupted = "interrupted"
)

type serviceStateRow struct {
	ClientID           string
	State              string
	Online             bool
	CoverageActive     bool
	ActiveSessionCount int
	CurrentSpanID      int64
	CurrentPauseID     int64
	CurrentInterruptID int64
	LastTransitionAt   int64
	UpdatedAt          int64
}

func (s *GatewayService) MarkClientOnline(clientID string, peerID string, reason string) error {
	return s.markClientPresence(clientID, peerID, true, reason)
}

func (s *GatewayService) MarkClientOffline(clientID string, peerID string, reason string) error {
	return s.markClientPresence(clientID, peerID, false, reason)
}

func (s *GatewayService) SyncServiceStateByClient(clientID string, reason string, spendTxID string, peerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, syncServiceStateTx(db, clientID, nil, reason, spendTxID, peerID)
	})
	return err
}

func (s *GatewayService) ReconcileServiceStates(reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		rows, err := db.Query(
			`SELECT client_pubkey_hex FROM fee_pool_sessions
			 UNION
			 SELECT client_pubkey_hex FROM fee_pool_client_presence`,
		)
		if err != nil {
			return struct{}{}, err
		}
		defer rows.Close()
		clients := make([]string, 0, 32)
		for rows.Next() {
			var clientID string
			if err := rows.Scan(&clientID); err != nil {
				return struct{}{}, err
			}
			clientID = NormalizeClientIDLoose(clientID)
			if clientID == "" {
				continue
			}
			clients = append(clients, clientID)
		}
		if err := rows.Err(); err != nil {
			return struct{}{}, err
		}
		for _, clientID := range clients {
			if err := syncServiceStateTx(db, clientID, nil, reason, "", ""); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, nil
	})
	return err
}

func (s *GatewayService) markClientPresence(clientID string, peerID string, online bool, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := gatewayServiceDBValue(s, func(db *sql.DB) (struct{}, error) {
		return struct{}{}, syncServiceStateTx(db, clientID, &online, reason, "", peerID)
	})
	return err
}

func syncServiceStateTx(db *sql.DB, clientID string, onlineOverride *bool, reason string, spendTxID string, peerID string) error {
	clientID = NormalizeClientIDLoose(clientID)
	reason = strings.TrimSpace(reason)
	spendTxID = strings.TrimSpace(spendTxID)
	peerID = strings.TrimSpace(peerID)
	if clientID == "" {
		return fmt.Errorf("client_pubkey_hex required")
	}
	if reason == "" {
		reason = "sync"
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	st, err := loadServiceStateTx(tx, clientID)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	if st.ClientID == "" {
		st = serviceStateRow{
			ClientID:           clientID,
			State:              serviceStateInactive,
			Online:             false,
			CoverageActive:     false,
			ActiveSessionCount: 0,
		}
	}

	if onlineOverride != nil {
		if err = upsertPresenceTx(tx, clientID, peerID, *onlineOverride, now); err != nil {
			return err
		}
		st.Online = *onlineOverride
	} else if st.Online == false {
		online, loadErr := loadPresenceOnlineTx(tx, clientID)
		if loadErr != nil {
			return loadErr
		}
		st.Online = online
	}

	activeCount, err := countActiveSessionsByClientTx(tx, clientID)
	if err != nil {
		return err
	}
	st.ActiveSessionCount = activeCount
	st.CoverageActive = activeCount > 0
	latestOpenInterruptID, err := normalizeOpenInterruptionsTx(tx, clientID, st.CoverageActive, now)
	if err != nil {
		return err
	}
	if st.CoverageActive {
		st.CurrentInterruptID = 0
	} else if latestOpenInterruptID > 0 {
		st.CurrentInterruptID = latestOpenInterruptID
	}

	prevState := strings.TrimSpace(st.State)
	if prevState == "" {
		prevState = serviceStateInactive
	}
	nextState := deriveServiceState(prevState, st.CoverageActive, st.Online)
	if nextState == serviceStateInterrupted && st.CurrentInterruptID == 0 {
		interruptID, openErr := openInterruptionTx(tx, st.ClientID, now, reason)
		if openErr != nil {
			return openErr
		}
		st.CurrentInterruptID = interruptID
	}

	if prevState != nextState {
		if err = applyServiceTransitionTx(tx, &st, prevState, nextState, now, reason, spendTxID, peerID); err != nil {
			return err
		}
	}
	st.State = nextState
	st.UpdatedAt = now
	if prevState != nextState {
		st.LastTransitionAt = now
	}

	if err = upsertServiceStateTx(tx, st); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func normalizeOpenInterruptionsTx(tx *sql.Tx, clientID string, coverageActive bool, now int64) (int64, error) {
	rows, err := tx.Query(
		`SELECT id FROM fee_pool_service_interruptions
		 WHERE client_pubkey_hex=? AND status='open'
		 ORDER BY id DESC`,
		clientID,
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	ids := make([]int64, 0, 4)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		if id > 0 {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if coverageActive {
		for _, id := range ids {
			if err := closeInterruptionTx(tx, id, now, "coverage_restored_sync"); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
	keepID := ids[0]
	for _, id := range ids[1:] {
		if err := closeInterruptionTx(tx, id, now, "interruption_dedup_sync"); err != nil {
			return 0, err
		}
	}
	return keepID, nil
}

func deriveServiceState(prevState string, coverageActive bool, online bool) string {
	if coverageActive {
		if online {
			return serviceStateServing
		}
		return serviceStatePaused
	}
	if prevState == serviceStateServing || prevState == serviceStatePaused || prevState == serviceStateInterrupted {
		return serviceStateInterrupted
	}
	return serviceStateInactive
}

func applyServiceTransitionTx(tx *sql.Tx, st *serviceStateRow, prevState string, nextState string, now int64, reason string, spendTxID string, peerID string) error {
	switch nextState {
	case serviceStateServing:
		if prevState == serviceStatePaused && st.CurrentPauseID > 0 {
			if err := closePauseTx(tx, st.CurrentPauseID, now, "client_online"); err != nil {
				return err
			}
			st.CurrentPauseID = 0
		}
		if prevState == serviceStateInterrupted && st.CurrentInterruptID > 0 {
			if err := closeInterruptionTx(tx, st.CurrentInterruptID, now, "coverage_restored"); err != nil {
				return err
			}
			st.CurrentInterruptID = 0
		}
		if st.CurrentSpanID == 0 {
			spanID, err := openSpanTx(tx, st.ClientID, now, spendTxID)
			if err != nil {
				return err
			}
			st.CurrentSpanID = spanID
		}
	case serviceStatePaused:
		if prevState == serviceStateInterrupted && st.CurrentInterruptID > 0 {
			if err := closeInterruptionTx(tx, st.CurrentInterruptID, now, "coverage_restored_offline"); err != nil {
				return err
			}
			st.CurrentInterruptID = 0
		}
		// 仅在已有连续服务段时记录 pause；这样 pause 始终附着在 span 上。
		if st.CurrentSpanID > 0 && st.CurrentPauseID == 0 {
			pauseID, err := openPauseTx(tx, st.ClientID, st.CurrentSpanID, now, reason)
			if err != nil {
				return err
			}
			st.CurrentPauseID = pauseID
		}
	case serviceStateInterrupted:
		if st.CurrentPauseID > 0 {
			if err := closePauseTx(tx, st.CurrentPauseID, now, "coverage_lost"); err != nil {
				return err
			}
			st.CurrentPauseID = 0
		}
		if st.CurrentSpanID > 0 {
			if err := closeSpanTx(tx, st.CurrentSpanID, now, "coverage_lost", spendTxID); err != nil {
				return err
			}
			st.CurrentSpanID = 0
		}
		if st.CurrentInterruptID == 0 {
			interruptID, err := openInterruptionTx(tx, st.ClientID, now, reason)
			if err != nil {
				return err
			}
			st.CurrentInterruptID = interruptID
		}
	case serviceStateInactive:
		if st.CurrentPauseID > 0 {
			if err := closePauseTx(tx, st.CurrentPauseID, now, "inactive"); err != nil {
				return err
			}
			st.CurrentPauseID = 0
		}
		if st.CurrentSpanID > 0 {
			if err := closeSpanTx(tx, st.CurrentSpanID, now, "inactive", spendTxID); err != nil {
				return err
			}
			st.CurrentSpanID = 0
		}
		if st.CurrentInterruptID > 0 {
			if err := closeInterruptionTx(tx, st.CurrentInterruptID, now, "inactive"); err != nil {
				return err
			}
			st.CurrentInterruptID = 0
		}
	default:
		return fmt.Errorf("unsupported service state transition target: %s", nextState)
	}
	return insertServiceEventTx(tx, st.ClientID, "state_transition", prevState, nextState, reason, spendTxID, peerID, now)
}

func loadServiceStateTx(tx *sql.Tx, clientID string) (serviceStateRow, error) {
	var row serviceStateRow
	var onlineInt int
	var coverageInt int
	err := tx.QueryRow(
		`SELECT client_pubkey_hex,state,online,coverage_active,active_session_count,current_span_id,current_pause_id,current_interrupt_id,last_transition_at_unix,updated_at_unix
		 FROM fee_pool_service_state WHERE client_pubkey_hex=?`,
		clientID,
	).Scan(
		&row.ClientID,
		&row.State,
		&onlineInt,
		&coverageInt,
		&row.ActiveSessionCount,
		&row.CurrentSpanID,
		&row.CurrentPauseID,
		&row.CurrentInterruptID,
		&row.LastTransitionAt,
		&row.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return serviceStateRow{}, nil
		}
		return serviceStateRow{}, err
	}
	row.Online = onlineInt == 1
	row.CoverageActive = coverageInt == 1
	return row, nil
}

func loadPresenceOnlineTx(tx *sql.Tx, clientID string) (bool, error) {
	var onlineInt int
	err := tx.QueryRow(`SELECT online FROM fee_pool_client_presence WHERE client_pubkey_hex=?`, clientID).Scan(&onlineInt)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return onlineInt == 1, nil
}

func upsertPresenceTx(tx *sql.Tx, clientID string, peerID string, online bool, now int64) error {
	_ = strings.TrimSpace(peerID)
	onlineInt := 0
	lastOnline := int64(0)
	lastOffline := int64(0)
	if online {
		onlineInt = 1
		lastOnline = now
	} else {
		lastOffline = now
	}
	_, err := tx.Exec(
		`INSERT INTO fee_pool_client_presence(client_pubkey_hex,online,last_online_at_unix,last_offline_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?)
		 ON CONFLICT(client_pubkey_hex) DO UPDATE SET
			online=excluded.online,
			last_online_at_unix=CASE WHEN excluded.last_online_at_unix>0 THEN excluded.last_online_at_unix ELSE fee_pool_client_presence.last_online_at_unix END,
			last_offline_at_unix=CASE WHEN excluded.last_offline_at_unix>0 THEN excluded.last_offline_at_unix ELSE fee_pool_client_presence.last_offline_at_unix END,
			updated_at_unix=excluded.updated_at_unix`,
		clientID, onlineInt, lastOnline, lastOffline, now,
	)
	return err
}

func countActiveSessionsByClientTx(tx *sql.Tx, clientID string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM fee_pool_sessions WHERE client_pubkey_hex=? AND lifecycle_state='active'`, clientID).Scan(&n)
	return n, err
}

func upsertServiceStateTx(tx *sql.Tx, row serviceStateRow) error {
	onlineInt := 0
	if row.Online {
		onlineInt = 1
	}
	coverageInt := 0
	if row.CoverageActive {
		coverageInt = 1
	}
	_, err := tx.Exec(
		`INSERT INTO fee_pool_service_state(client_pubkey_hex,state,online,coverage_active,active_session_count,current_span_id,current_pause_id,current_interrupt_id,last_transition_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(client_pubkey_hex) DO UPDATE SET
			state=excluded.state,
			online=excluded.online,
			coverage_active=excluded.coverage_active,
			active_session_count=excluded.active_session_count,
			current_span_id=excluded.current_span_id,
			current_pause_id=excluded.current_pause_id,
			current_interrupt_id=excluded.current_interrupt_id,
			last_transition_at_unix=excluded.last_transition_at_unix,
			updated_at_unix=excluded.updated_at_unix`,
		row.ClientID, row.State, onlineInt, coverageInt, row.ActiveSessionCount,
		row.CurrentSpanID, row.CurrentPauseID, row.CurrentInterruptID,
		row.LastTransitionAt, row.UpdatedAt,
	)
	return err
}

func openSpanTx(tx *sql.Tx, clientID string, now int64, spendTxID string) (int64, error) {
	res, err := tx.Exec(
		`INSERT INTO fee_pool_service_spans(client_pubkey_hex,start_at_unix,end_at_unix,status,start_spend_txid,end_spend_txid,end_reason,created_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?)`,
		clientID, now, 0, "open", strings.TrimSpace(spendTxID), "", "", now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func closeSpanTx(tx *sql.Tx, spanID int64, now int64, reason string, spendTxID string) error {
	_, err := tx.Exec(
		`UPDATE fee_pool_service_spans
		 SET end_at_unix=?,status='closed',end_spend_txid=?,end_reason=?,updated_at_unix=?
		 WHERE id=?`,
		now, strings.TrimSpace(spendTxID), strings.TrimSpace(reason), now, spanID,
	)
	return err
}

func openPauseTx(tx *sql.Tx, clientID string, spanID int64, now int64, reason string) (int64, error) {
	res, err := tx.Exec(
		`INSERT INTO fee_pool_service_pauses(client_pubkey_hex,span_id,start_at_unix,end_at_unix,status,reason,created_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?)`,
		clientID, spanID, now, 0, "open", strings.TrimSpace(reason), now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func closePauseTx(tx *sql.Tx, pauseID int64, now int64, reason string) error {
	_, err := tx.Exec(
		`UPDATE fee_pool_service_pauses
		 SET end_at_unix=?,status='closed',reason=?,updated_at_unix=?
		 WHERE id=?`,
		now, strings.TrimSpace(reason), now, pauseID,
	)
	return err
}

func openInterruptionTx(tx *sql.Tx, clientID string, now int64, reason string) (int64, error) {
	res, err := tx.Exec(
		`INSERT INTO fee_pool_service_interruptions(client_pubkey_hex,start_at_unix,end_at_unix,status,reason,created_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?)`,
		clientID, now, 0, "open", strings.TrimSpace(reason), now, now,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func closeInterruptionTx(tx *sql.Tx, interruptionID int64, now int64, reason string) error {
	_, err := tx.Exec(
		`UPDATE fee_pool_service_interruptions
		 SET end_at_unix=?,status='closed',reason=?,updated_at_unix=?
		 WHERE id=?`,
		now, strings.TrimSpace(reason), now, interruptionID,
	)
	return err
}

func insertServiceEventTx(tx *sql.Tx, clientID string, eventName string, prevState string, nextState string, reason string, spendTxID string, peerID string, now int64) error {
	_ = strings.TrimSpace(peerID)
	_, err := tx.Exec(
		`INSERT INTO fee_pool_service_events(client_pubkey_hex,event_name,prev_state,next_state,reason,spend_txid,created_at_unix)
		 VALUES(?,?,?,?,?,?,?)`,
		clientID,
		strings.TrimSpace(eventName),
		strings.TrimSpace(prevState),
		strings.TrimSpace(nextState),
		strings.TrimSpace(reason),
		strings.TrimSpace(spendTxID),
		now,
	)
	return err
}
