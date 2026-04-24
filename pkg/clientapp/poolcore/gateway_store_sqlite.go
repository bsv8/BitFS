package poolcore

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type GatewaySessionRow struct {
	SpendTxID string

	ClientID                  string
	ClientBSVCompressedPubHex string
	ServerBSVCompressedPubHex string

	InputAmountSat  uint64
	PoolAmountSat   uint64
	SpendTxFeeSat   uint64
	Sequence        uint32
	ServerAmountSat uint64
	ClientAmountSat uint64

	BaseTxID  string
	FinalTxID string

	BaseTxHex    string
	CurrentTxHex string

	LifecycleState string
	FrozenReason   string

	LastSubmitError         string
	SubmitRetryCount        int
	LastSubmitAttemptAtUnix int64
	CloseSubmitAtUnix       int64
	CloseObservedAtUnix     int64
	CloseObserveStatus      string
	CloseObserveReason      string

	CreatedAt int64
	UpdatedAt int64
}

type ListenerTargetRow struct {
	ClientID string
}

type ServiceOfferRow struct {
	OfferHash              string
	ServiceType            string
	ServiceNodePubkeyHex   string
	ClientPubkeyHex        string
	RequestParams          []byte
	CreatedAtUnix          int64
	LastQuotedAtUnix       int64
	LastQuotedAmountSat    uint64
	LastQuoteExpiresAtUnix int64
}

type ServiceQuotePaymentRow struct {
	QuoteHash       string
	OfferHash       string
	PaymentScheme   string
	PaymentTxID     string
	ClientPubkeyHex string
	AcceptedAtUnix  int64
}

func InitGatewayStore(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS fee_pool_sessions (
			spend_txid TEXT PRIMARY KEY,

			client_pubkey_hex TEXT NOT NULL,
			client_bsv_pubkey_hex TEXT NOT NULL,
			server_bsv_pubkey_hex TEXT NOT NULL,

			input_amount_satoshi INTEGER NOT NULL,
			pool_amount_satoshi INTEGER NOT NULL,
			spend_tx_fee_satoshi INTEGER NOT NULL,
			sequence_num INTEGER NOT NULL,
			server_amount_satoshi INTEGER NOT NULL,
			client_amount_satoshi INTEGER NOT NULL,

			base_txid TEXT NOT NULL,
			final_txid TEXT NOT NULL,

			base_tx_hex TEXT NOT NULL,
			current_tx_hex TEXT NOT NULL,

			lifecycle_state TEXT NOT NULL DEFAULT 'pending_base_tx',
			frozen_reason TEXT NOT NULL DEFAULT '',
			last_submit_error TEXT NOT NULL DEFAULT '',
			submit_retry_count INTEGER NOT NULL DEFAULT 0,
			last_submit_attempt_at_unix INTEGER NOT NULL DEFAULT 0,
			close_submit_at_unix INTEGER NOT NULL DEFAULT 0,
			close_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			close_observe_status TEXT NOT NULL DEFAULT '',
			close_observe_reason TEXT NOT NULL DEFAULT '',

			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_client_pubkey_hex ON fee_pool_sessions(client_pubkey_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_updated_at ON fee_pool_sessions(updated_at_unix DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_charge_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_pubkey_hex TEXT NOT NULL,
			spend_txid TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			charge_reason TEXT NOT NULL,
			charge_amount_satoshi INTEGER NOT NULL,
			billing_cycle_seconds INTEGER NOT NULL DEFAULT 0,
			effective_until_unix INTEGER NOT NULL DEFAULT 0,
			pool_balance_after_satoshi INTEGER NOT NULL DEFAULT 0,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_charge_client_reason ON fee_pool_charge_events(client_pubkey_hex, charge_reason)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_client_presence (
			client_pubkey_hex TEXT PRIMARY KEY,
			online INTEGER NOT NULL,
			last_online_at_unix INTEGER NOT NULL,
			last_offline_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_client_presence_online ON fee_pool_client_presence(online, updated_at_unix DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_service_state (
			client_pubkey_hex TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			online INTEGER NOT NULL,
			coverage_active INTEGER NOT NULL,
			active_session_count INTEGER NOT NULL,
			current_span_id INTEGER NOT NULL,
			current_pause_id INTEGER NOT NULL,
			current_interrupt_id INTEGER NOT NULL,
			last_transition_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_service_state_state ON fee_pool_service_state(state, updated_at_unix DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_service_spans (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_pubkey_hex TEXT NOT NULL,
			start_at_unix INTEGER NOT NULL,
			end_at_unix INTEGER NOT NULL,
			status TEXT NOT NULL,
			start_spend_txid TEXT NOT NULL,
			end_spend_txid TEXT NOT NULL,
			end_reason TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_service_spans_client ON fee_pool_service_spans(client_pubkey_hex, id DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_service_pauses (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_pubkey_hex TEXT NOT NULL,
			span_id INTEGER NOT NULL,
			start_at_unix INTEGER NOT NULL,
			end_at_unix INTEGER NOT NULL,
			status TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_service_pauses_client ON fee_pool_service_pauses(client_pubkey_hex, id DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_service_interruptions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_pubkey_hex TEXT NOT NULL,
			start_at_unix INTEGER NOT NULL,
			end_at_unix INTEGER NOT NULL,
			status TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_service_interruptions_client ON fee_pool_service_interruptions(client_pubkey_hex, id DESC)`,
		`CREATE TABLE IF NOT EXISTS fee_pool_service_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			client_pubkey_hex TEXT NOT NULL,
			event_name TEXT NOT NULL,
			prev_state TEXT NOT NULL,
			next_state TEXT NOT NULL,
			reason TEXT NOT NULL,
			spend_txid TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fee_pool_service_events_client ON fee_pool_service_events(client_pubkey_hex, id DESC)`,
		`CREATE TABLE IF NOT EXISTS gateway_admin_config (
			config_key TEXT PRIMARY KEY,
			config_value TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS service_offers (
			offer_hash TEXT PRIMARY KEY,
			service_type TEXT NOT NULL,
			service_node_pubkey_hex TEXT NOT NULL,
			client_pubkey_hex TEXT NOT NULL,
			request_params BLOB NOT NULL,
			created_at_unix INTEGER NOT NULL,
			last_quoted_at_unix INTEGER NOT NULL DEFAULT 0,
			last_quoted_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			last_quote_expires_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_service_offers_client_created ON service_offers(client_pubkey_hex, created_at_unix DESC)`,
		`CREATE TABLE IF NOT EXISTS service_quote_payments (
			quote_hash TEXT PRIMARY KEY,
			offer_hash TEXT NOT NULL,
			payment_scheme TEXT NOT NULL,
			payment_txid TEXT NOT NULL,
			client_pubkey_hex TEXT NOT NULL,
			accepted_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_service_quote_payments_offer ON service_quote_payments(offer_hash, accepted_at_unix DESC)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}
	// 旧库升级：历史版本的 charge_events 不包含计费补充字段，这里自动补齐列定义。
	if err := ensureChargeEventColumns(db); err != nil {
		return err
	}
	// 旧库升级：补齐 close 观测列，用于“广播后观测 UTXO 再结案”。
	if err := ensureSessionCloseObservationColumns(db); err != nil {
		return err
	}
	// 旧库升级：补齐 lifecycle 状态机列，并回填初始值。
	if err := ensureSessionLifecycleColumns(db); err != nil {
		return err
	}
	if err := ensureSingleActiveIndex(db); err != nil {
		return err
	}
	return nil
}

func UpsertServiceOffer(db *sql.DB, row ServiceOfferRow) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if strings.TrimSpace(row.OfferHash) == "" {
		return fmt.Errorf("offer_hash required")
	}
	_, err := db.Exec(
		`INSERT INTO service_offers(
			offer_hash,service_type,service_node_pubkey_hex,client_pubkey_hex,request_params,created_at_unix,last_quoted_at_unix,last_quoted_amount_satoshi,last_quote_expires_at_unix
		) VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(offer_hash) DO UPDATE SET
			service_type=excluded.service_type,
			service_node_pubkey_hex=excluded.service_node_pubkey_hex,
			client_pubkey_hex=excluded.client_pubkey_hex,
			request_params=excluded.request_params,
			created_at_unix=excluded.created_at_unix,
			last_quoted_at_unix=excluded.last_quoted_at_unix,
			last_quoted_amount_satoshi=excluded.last_quoted_amount_satoshi,
			last_quote_expires_at_unix=excluded.last_quote_expires_at_unix`,
		strings.TrimSpace(row.OfferHash),
		strings.TrimSpace(row.ServiceType),
		strings.ToLower(strings.TrimSpace(row.ServiceNodePubkeyHex)),
		strings.ToLower(strings.TrimSpace(row.ClientPubkeyHex)),
		append([]byte(nil), row.RequestParams...),
		row.CreatedAtUnix,
		row.LastQuotedAtUnix,
		row.LastQuotedAmountSat,
		row.LastQuoteExpiresAtUnix,
	)
	return err
}

func LoadServiceOfferByHash(db *sql.DB, offerHash string) (ServiceOfferRow, bool, error) {
	if db == nil {
		return ServiceOfferRow{}, false, fmt.Errorf("db is nil")
	}
	var row ServiceOfferRow
	err := db.QueryRow(
		`SELECT offer_hash,service_type,service_node_pubkey_hex,client_pubkey_hex,request_params,created_at_unix,last_quoted_at_unix,last_quoted_amount_satoshi,last_quote_expires_at_unix
		   FROM service_offers
		  WHERE offer_hash=?`,
		strings.ToLower(strings.TrimSpace(offerHash)),
	).Scan(
		&row.OfferHash,
		&row.ServiceType,
		&row.ServiceNodePubkeyHex,
		&row.ClientPubkeyHex,
		&row.RequestParams,
		&row.CreatedAtUnix,
		&row.LastQuotedAtUnix,
		&row.LastQuotedAmountSat,
		&row.LastQuoteExpiresAtUnix,
	)
	if err == sql.ErrNoRows {
		return ServiceOfferRow{}, false, nil
	}
	if err != nil {
		return ServiceOfferRow{}, false, err
	}
	return row, true, nil
}

func InsertServiceQuotePayment(db *sql.DB, row ServiceQuotePaymentRow) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if strings.TrimSpace(row.QuoteHash) == "" {
		return fmt.Errorf("quote_hash required")
	}
	if strings.TrimSpace(row.OfferHash) == "" {
		return fmt.Errorf("offer_hash required")
	}
	if strings.TrimSpace(row.PaymentScheme) == "" {
		return fmt.Errorf("payment_scheme required")
	}
	if strings.TrimSpace(row.PaymentTxID) == "" {
		return fmt.Errorf("payment_txid required")
	}
	if strings.TrimSpace(row.ClientPubkeyHex) == "" {
		return fmt.Errorf("client_pubkey_hex required")
	}
	if row.AcceptedAtUnix <= 0 {
		return fmt.Errorf("accepted_at_unix required")
	}
	_, err := db.Exec(
		`INSERT INTO service_quote_payments(
			quote_hash,offer_hash,payment_scheme,payment_txid,client_pubkey_hex,accepted_at_unix
		) VALUES(?,?,?,?,?,?)`,
		strings.ToLower(strings.TrimSpace(row.QuoteHash)),
		strings.ToLower(strings.TrimSpace(row.OfferHash)),
		strings.TrimSpace(row.PaymentScheme),
		strings.ToLower(strings.TrimSpace(row.PaymentTxID)),
		strings.ToLower(strings.TrimSpace(row.ClientPubkeyHex)),
		row.AcceptedAtUnix,
	)
	return err
}

func LoadServiceQuotePayment(db *sql.DB, quoteHash string) (ServiceQuotePaymentRow, bool, error) {
	if db == nil {
		return ServiceQuotePaymentRow{}, false, fmt.Errorf("db is nil")
	}
	var row ServiceQuotePaymentRow
	err := db.QueryRow(
		`SELECT quote_hash,offer_hash,payment_scheme,payment_txid,client_pubkey_hex,accepted_at_unix
		   FROM service_quote_payments
		  WHERE quote_hash=?`,
		strings.ToLower(strings.TrimSpace(quoteHash)),
	).Scan(
		&row.QuoteHash,
		&row.OfferHash,
		&row.PaymentScheme,
		&row.PaymentTxID,
		&row.ClientPubkeyHex,
		&row.AcceptedAtUnix,
	)
	if err == sql.ErrNoRows {
		return ServiceQuotePaymentRow{}, false, nil
	}
	if err != nil {
		return ServiceQuotePaymentRow{}, false, err
	}
	return row, true, nil
}

func ensureChargeEventColumns(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`ALTER TABLE fee_pool_charge_events ADD COLUMN billing_cycle_seconds INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE fee_pool_charge_events ADD COLUMN effective_until_unix INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE fee_pool_charge_events ADD COLUMN pool_balance_after_satoshi INTEGER NOT NULL DEFAULT 0`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			lower := strings.ToLower(strings.TrimSpace(err.Error()))
			if strings.Contains(lower, "duplicate column name") {
				continue
			}
			return err
		}
	}
	return nil
}

func ensureSessionLifecycleColumns(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`ALTER TABLE fee_pool_sessions ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT 'pending_base_tx'`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN frozen_reason TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN last_submit_error TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN submit_retry_count INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN last_submit_attempt_at_unix INTEGER NOT NULL DEFAULT 0`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			lower := strings.ToLower(strings.TrimSpace(err.Error()))
			if strings.Contains(lower, "duplicate column name") {
				continue
			}
			return err
		}
	}
	hasStatus, err := tableHasColumn(db, "fee_pool_sessions", "status")
	if err != nil {
		return err
	}
	if hasStatus {
		if _, err := db.Exec(`
			UPDATE fee_pool_sessions
			SET lifecycle_state=CASE
				WHEN LOWER(TRIM(COALESCE(status,''))) IN ('active','open','running','serving','payable') THEN 'active'
				WHEN LOWER(TRIM(COALESCE(status,''))) IN ('frozen','paused','pause','blocked','hold') THEN 'frozen'
				WHEN LOWER(TRIM(COALESCE(status,''))) IN ('should_submit','near_expiry','expired','should_close','submit_pending') THEN 'should_submit'
				WHEN LOWER(TRIM(COALESCE(status,''))) IN ('settled_external') THEN 'settled_external'
				WHEN LOWER(TRIM(COALESCE(status,''))) IN ('closed','close','settled','done','completed','finalized') THEN 'closed'
				ELSE 'pending_base_tx'
			END
			WHERE TRIM(COALESCE(lifecycle_state,''))=''
			   OR (LOWER(TRIM(COALESCE(lifecycle_state,'')))='pending_base_tx' AND LOWER(TRIM(COALESCE(status,'')))<>'pending_base_tx')
		`); err != nil {
			return err
		}
		if err := rebuildFeePoolSessionsWithoutStatus(db); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_fee_pool_lifecycle ON fee_pool_sessions(lifecycle_state)`); err != nil {
		return err
	}
	return nil
}

func ensureSessionCloseObservationColumns(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`ALTER TABLE fee_pool_sessions ADD COLUMN close_submit_at_unix INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN close_observed_at_unix INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN close_observe_status TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE fee_pool_sessions ADD COLUMN close_observe_reason TEXT NOT NULL DEFAULT ''`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			lower := strings.ToLower(strings.TrimSpace(err.Error()))
			if strings.Contains(lower, "duplicate column name") {
				continue
			}
			return err
		}
	}
	return nil
}

func ensureSingleActiveIndex(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	// 启动修复历史脏数据：同 client 多个 active 时，保留最新 updated_at 的一条。
	if _, err := db.Exec(`
		UPDATE fee_pool_sessions
		SET lifecycle_state='frozen',frozen_reason='startup_multi_active_reconcile'
		WHERE lifecycle_state='active'
		  AND spend_txid IN (
			SELECT spend_txid FROM (
				SELECT spend_txid,
					   ROW_NUMBER() OVER(PARTITION BY client_pubkey_hex ORDER BY updated_at_unix DESC, created_at_unix DESC, spend_txid DESC) AS rn
				FROM fee_pool_sessions
				WHERE lifecycle_state='active'
			) t
			WHERE t.rn > 1
		  )`); err != nil {
		return err
	}
	_, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_fee_pool_client_active ON fee_pool_sessions(client_pubkey_hex) WHERE lifecycle_state='active'`)
	return err
}

func SetGatewayAdminConfig(db *sql.DB, key string, value string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("config key is required")
	}
	_, err := db.Exec(
		`INSERT INTO gateway_admin_config(config_key,config_value,updated_at_unix)
		 VALUES(?,?,?)
		 ON CONFLICT(config_key) DO UPDATE SET
			config_value=excluded.config_value,
			updated_at_unix=excluded.updated_at_unix`,
		key, value, time.Now().Unix(),
	)
	return err
}

func GetGatewayAdminConfig(db *sql.DB, key string) (string, bool, error) {
	if db == nil {
		return "", false, fmt.Errorf("db is nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", false, fmt.Errorf("config key is required")
	}
	var value string
	err := db.QueryRow(`SELECT config_value FROM gateway_admin_config WHERE config_key=?`, key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}
	return value, true, nil
}

func UpsertClientPresence(db *sql.DB, clientID string, _ string, online bool) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return fmt.Errorf("client_pubkey_hex required")
	}
	now := time.Now().Unix()
	onlineInt := 0
	lastOnline := int64(0)
	lastOffline := int64(0)
	if online {
		onlineInt = 1
		lastOnline = now
	} else {
		lastOffline = now
	}
	_, err := db.Exec(
		`INSERT INTO fee_pool_client_presence(client_pubkey_hex,online,last_online_at_unix,last_offline_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?)
		 ON CONFLICT(client_pubkey_hex) DO UPDATE SET
			online=excluded.online,
			last_online_at_unix=CASE WHEN excluded.last_online_at_unix>0 THEN excluded.last_online_at_unix ELSE fee_pool_client_presence.last_online_at_unix END,
			last_offline_at_unix=CASE WHEN excluded.last_offline_at_unix>0 THEN excluded.last_offline_at_unix ELSE fee_pool_client_presence.last_offline_at_unix END,
			updated_at_unix=excluded.updated_at_unix`,
		clientID, onlineInt, lastOnline, lastOffline, now,
	)
	return err
}

func ListServingListenerTargetsByChargeReason(db *sql.DB, reason string) ([]ListenerTargetRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, fmt.Errorf("reason is required")
	}
	rows, err := db.Query(
		`SELECT DISTINCT s.client_pubkey_hex
		 FROM fee_pool_service_state s
		 JOIN fee_pool_client_presence p ON p.client_pubkey_hex=s.client_pubkey_hex
		 JOIN fee_pool_charge_events c ON c.client_pubkey_hex=s.client_pubkey_hex
		 WHERE c.charge_reason=? AND s.state='serving' AND p.online=1`,
		reason,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ListenerTargetRow, 0, 16)
	for rows.Next() {
		var row ListenerTargetRow
		if err := rows.Scan(&row.ClientID); err != nil {
			return nil, err
		}
		row.ClientID = strings.ToLower(strings.TrimSpace(row.ClientID))
		if row.ClientID == "" {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func InsertChargeEvent(db *sql.DB, clientID string, spendTxID string, sequence uint32, reason string, amount uint64, billingCycleSeconds uint32, effectiveUntilUnix int64, poolBalanceAfterSatoshi uint64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	clientID = NormalizeClientIDLoose(clientID)
	spendTxID = strings.TrimSpace(spendTxID)
	reason = strings.TrimSpace(reason)
	if clientID == "" || spendTxID == "" || reason == "" {
		return fmt.Errorf("invalid charge event")
	}
	if effectiveUntilUnix <= 0 {
		return fmt.Errorf("effective_until_unix must be positive")
	}
	_, err := db.Exec(
		`INSERT INTO fee_pool_charge_events(client_pubkey_hex,spend_txid,sequence_num,charge_reason,charge_amount_satoshi,billing_cycle_seconds,effective_until_unix,pool_balance_after_satoshi,created_at_unix) VALUES(?,?,?,?,?,?,?,?,?)`,
		clientID, spendTxID, sequence, reason, amount, billingCycleSeconds, effectiveUntilUnix, poolBalanceAfterSatoshi, time.Now().Unix(),
	)
	return err
}

func InsertChargeEventTx(tx *sql.Tx, clientID string, spendTxID string, sequence uint32, reason string, amount uint64, billingCycleSeconds uint32, effectiveUntilUnix int64, poolBalanceAfterSatoshi uint64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	clientID = NormalizeClientIDLoose(clientID)
	spendTxID = strings.TrimSpace(spendTxID)
	reason = strings.TrimSpace(reason)
	if clientID == "" || spendTxID == "" || reason == "" {
		return fmt.Errorf("invalid charge event")
	}
	if effectiveUntilUnix <= 0 {
		return fmt.Errorf("effective_until_unix must be positive")
	}
	_, err := tx.Exec(
		`INSERT INTO fee_pool_charge_events(client_pubkey_hex,spend_txid,sequence_num,charge_reason,charge_amount_satoshi,billing_cycle_seconds,effective_until_unix,pool_balance_after_satoshi,created_at_unix) VALUES(?,?,?,?,?,?,?,?,?)`,
		clientID, spendTxID, sequence, reason, amount, billingCycleSeconds, effectiveUntilUnix, poolBalanceAfterSatoshi, time.Now().Unix(),
	)
	return err
}

func CountChargeEventsByClientAndReason(db *sql.DB, clientID string, reason string) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	clientID = NormalizeClientIDLoose(clientID)
	reason = strings.TrimSpace(reason)
	if clientID == "" || reason == "" {
		return 0, fmt.Errorf("client_pubkey_hex and reason are required")
	}
	aliases := ClientIDAliasesForQuery(clientID)
	if len(aliases) == 0 {
		return 0, fmt.Errorf("client_pubkey_hex and reason are required")
	}
	var n int
	if len(aliases) == 1 {
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM fee_pool_charge_events WHERE client_pubkey_hex=? AND charge_reason=?`,
			aliases[0], reason,
		).Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM fee_pool_charge_events WHERE client_pubkey_hex IN (?,?) AND charge_reason=?`,
		aliases[0], aliases[1], reason,
	).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func ListClientsByChargeReason(db *sql.DB, reason string) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, fmt.Errorf("reason is required")
	}
	rows, err := db.Query(`SELECT DISTINCT client_pubkey_hex FROM fee_pool_charge_events WHERE charge_reason=?`, reason)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var clientID string
		if err := rows.Scan(&clientID); err != nil {
			return nil, err
		}
		clientID = strings.ToLower(strings.TrimSpace(clientID))
		if clientID == "" {
			continue
		}
		out = append(out, clientID)
	}
	return out, nil
}

const sessionSelectColumns = `spend_txid,client_pubkey_hex,client_bsv_pubkey_hex,server_bsv_pubkey_hex,input_amount_satoshi,pool_amount_satoshi,spend_tx_fee_satoshi,sequence_num,server_amount_satoshi,client_amount_satoshi,base_txid,final_txid,base_tx_hex,current_tx_hex,lifecycle_state,frozen_reason,last_submit_error,submit_retry_count,last_submit_attempt_at_unix,close_submit_at_unix,close_observed_at_unix,close_observe_status,close_observe_reason,created_at_unix,updated_at_unix`

func sessionScanArgs(row *GatewaySessionRow) []any {
	return []any{
		&row.SpendTxID,
		&row.ClientID, &row.ClientBSVCompressedPubHex, &row.ServerBSVCompressedPubHex,
		&row.InputAmountSat, &row.PoolAmountSat, &row.SpendTxFeeSat,
		&row.Sequence, &row.ServerAmountSat, &row.ClientAmountSat,
		&row.BaseTxID, &row.FinalTxID,
		&row.BaseTxHex, &row.CurrentTxHex,
		&row.LifecycleState, &row.FrozenReason, &row.LastSubmitError, &row.SubmitRetryCount, &row.LastSubmitAttemptAtUnix,
		&row.CloseSubmitAtUnix, &row.CloseObservedAtUnix, &row.CloseObserveStatus, &row.CloseObserveReason,
		&row.CreatedAt, &row.UpdatedAt,
	}
}

func LoadLatestSessionByClientID(db *sql.DB, clientID string) (GatewaySessionRow, bool, error) {
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	aliases := ClientIDAliasesForQuery(clientID)
	if len(aliases) == 0 {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	var row GatewaySessionRow
	query := `SELECT ` + sessionSelectColumns + `
			 FROM fee_pool_sessions
			 WHERE client_pubkey_hex=?
			 ORDER BY updated_at_unix DESC
			 LIMIT 1`
	args := []any{aliases[0]}
	if len(aliases) > 1 {
		query = strings.ReplaceAll(query, "client_pubkey_hex=?", "client_pubkey_hex IN (?,?)")
		args = []any{aliases[0], aliases[1]}
	}
	err := db.QueryRow(query, args...).Scan(sessionScanArgs(&row)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return GatewaySessionRow{}, false, nil
		}
		return GatewaySessionRow{}, false, err
	}
	return row, true, nil
}

// LoadPreferredSessionByClientID 优先返回 active 会话；若没有 active，则回退到最新会话。
func LoadPreferredSessionByClientID(db *sql.DB, clientID string) (GatewaySessionRow, bool, error) {
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	aliases := ClientIDAliasesForQuery(clientID)
	if len(aliases) == 0 {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	var row GatewaySessionRow
	// 先找 active，保证 rotate 后默认视图仍指向当前服务中的池。
	queryActive := `SELECT ` + sessionSelectColumns + `
			 FROM fee_pool_sessions
			 WHERE client_pubkey_hex=? AND lifecycle_state='active'
			 ORDER BY updated_at_unix DESC
			 LIMIT 1`
	args := []any{aliases[0]}
	if len(aliases) > 1 {
		queryActive = strings.ReplaceAll(queryActive, "client_pubkey_hex=?", "client_pubkey_hex IN (?,?)")
		args = []any{aliases[0], aliases[1]}
	}
	err := db.QueryRow(queryActive, args...).Scan(sessionScanArgs(&row)...)
	if err == nil {
		return row, true, nil
	}
	if err != sql.ErrNoRows {
		return GatewaySessionRow{}, false, err
	}
	return LoadLatestSessionByClientID(db, clientID)
}

func LoadLatestNonClosedSessionByClientID(db *sql.DB, clientID string) (GatewaySessionRow, bool, error) {
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	aliases := ClientIDAliasesForQuery(clientID)
	if len(aliases) == 0 {
		return GatewaySessionRow{}, false, fmt.Errorf("client_pubkey_hex required")
	}
	var row GatewaySessionRow
	query := `SELECT ` + sessionSelectColumns + `
		FROM fee_pool_sessions
		WHERE client_pubkey_hex=? AND lifecycle_state NOT IN ('closed','settled_external')
		ORDER BY updated_at_unix DESC
		LIMIT 1`
	args := []any{aliases[0]}
	if len(aliases) > 1 {
		query = strings.ReplaceAll(query, "client_pubkey_hex=?", "client_pubkey_hex IN (?,?)")
		args = []any{aliases[0], aliases[1]}
	}
	err := db.QueryRow(query, args...).Scan(sessionScanArgs(&row)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return GatewaySessionRow{}, false, nil
		}
		return GatewaySessionRow{}, false, err
	}
	return row, true, nil
}

func LoadSessionBySpendTxID(db *sql.DB, spendTxID string) (GatewaySessionRow, bool, error) {
	spendTxID = strings.TrimSpace(spendTxID)
	if spendTxID == "" {
		return GatewaySessionRow{}, false, fmt.Errorf("spend_txid required")
	}
	var row GatewaySessionRow
	err := db.QueryRow(
		`SELECT `+sessionSelectColumns+`
			 FROM fee_pool_sessions WHERE spend_txid=?`, spendTxID,
	).Scan(sessionScanArgs(&row)...)
	if err != nil {
		if err == sql.ErrNoRows {
			return GatewaySessionRow{}, false, nil
		}
		return GatewaySessionRow{}, false, err
	}
	return row, true, nil
}

func InsertSession(db *sql.DB, row GatewaySessionRow) error {
	now := time.Now().Unix()
	row.CreatedAt = now
	row.UpdatedAt = now
	row.ClientID = NormalizeClientIDLoose(row.ClientID)
	if row.ClientID == "" {
		return fmt.Errorf("client_pubkey_hex required")
	}
	row.LifecycleState = strings.ToLower(strings.TrimSpace(row.LifecycleState))
	if row.LifecycleState == "" {
		row.LifecycleState = "pending_base_tx"
	}
	_, err := db.Exec(
		`INSERT INTO fee_pool_sessions(spend_txid,client_pubkey_hex,client_bsv_pubkey_hex,server_bsv_pubkey_hex,input_amount_satoshi,pool_amount_satoshi,spend_tx_fee_satoshi,sequence_num,server_amount_satoshi,client_amount_satoshi,base_txid,final_txid,base_tx_hex,current_tx_hex,lifecycle_state,frozen_reason,last_submit_error,submit_retry_count,last_submit_attempt_at_unix,close_submit_at_unix,close_observed_at_unix,close_observe_status,close_observe_reason,created_at_unix,updated_at_unix)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		row.SpendTxID,
		row.ClientID,
		strings.ToLower(strings.TrimSpace(row.ClientBSVCompressedPubHex)),
		strings.ToLower(strings.TrimSpace(row.ServerBSVCompressedPubHex)),
		row.InputAmountSat, row.PoolAmountSat, row.SpendTxFeeSat,
		row.Sequence, row.ServerAmountSat, row.ClientAmountSat,
		strings.TrimSpace(row.BaseTxID), strings.TrimSpace(row.FinalTxID),
		row.BaseTxHex, row.CurrentTxHex,
		row.LifecycleState,
		strings.TrimSpace(row.FrozenReason),
		strings.TrimSpace(row.LastSubmitError),
		row.SubmitRetryCount,
		row.LastSubmitAttemptAtUnix,
		row.CloseSubmitAtUnix,
		row.CloseObservedAtUnix,
		strings.TrimSpace(row.CloseObserveStatus),
		strings.TrimSpace(row.CloseObserveReason),
		row.CreatedAt, row.UpdatedAt,
	)
	return err
}

func UpdateSession(db *sql.DB, row GatewaySessionRow) error {
	row.UpdatedAt = time.Now().Unix()
	row.LifecycleState = strings.ToLower(strings.TrimSpace(row.LifecycleState))
	if row.LifecycleState == "" {
		return fmt.Errorf("lifecycle_state required")
	}
	_, err := db.Exec(
		`UPDATE fee_pool_sessions
		 SET sequence_num=?,server_amount_satoshi=?,client_amount_satoshi=?,base_txid=?,final_txid=?,base_tx_hex=?,current_tx_hex=?,lifecycle_state=?,frozen_reason=?,last_submit_error=?,submit_retry_count=?,last_submit_attempt_at_unix=?,close_submit_at_unix=?,close_observed_at_unix=?,close_observe_status=?,close_observe_reason=?,updated_at_unix=?
		 WHERE spend_txid=?`,
		row.Sequence, row.ServerAmountSat, row.ClientAmountSat,
		strings.TrimSpace(row.BaseTxID), strings.TrimSpace(row.FinalTxID),
		row.BaseTxHex, row.CurrentTxHex,
		row.LifecycleState,
		strings.TrimSpace(row.FrozenReason),
		strings.TrimSpace(row.LastSubmitError),
		row.SubmitRetryCount,
		row.LastSubmitAttemptAtUnix,
		row.CloseSubmitAtUnix,
		row.CloseObservedAtUnix,
		strings.TrimSpace(row.CloseObserveStatus),
		strings.TrimSpace(row.CloseObserveReason),
		row.UpdatedAt,
		strings.TrimSpace(row.SpendTxID),
	)
	return err
}

func UpdateSessionTx(tx *sql.Tx, row GatewaySessionRow) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	row.UpdatedAt = time.Now().Unix()
	row.LifecycleState = strings.ToLower(strings.TrimSpace(row.LifecycleState))
	if row.LifecycleState == "" {
		return fmt.Errorf("lifecycle_state required")
	}
	_, err := tx.Exec(
		`UPDATE fee_pool_sessions
		 SET sequence_num=?,server_amount_satoshi=?,client_amount_satoshi=?,base_txid=?,final_txid=?,base_tx_hex=?,current_tx_hex=?,lifecycle_state=?,frozen_reason=?,last_submit_error=?,submit_retry_count=?,last_submit_attempt_at_unix=?,close_submit_at_unix=?,close_observed_at_unix=?,close_observe_status=?,close_observe_reason=?,updated_at_unix=?
		 WHERE spend_txid=?`,
		row.Sequence, row.ServerAmountSat, row.ClientAmountSat,
		strings.TrimSpace(row.BaseTxID), strings.TrimSpace(row.FinalTxID),
		row.BaseTxHex, row.CurrentTxHex,
		row.LifecycleState,
		strings.TrimSpace(row.FrozenReason),
		strings.TrimSpace(row.LastSubmitError),
		row.SubmitRetryCount,
		row.LastSubmitAttemptAtUnix,
		row.CloseSubmitAtUnix,
		row.CloseObservedAtUnix,
		strings.TrimSpace(row.CloseObserveStatus),
		strings.TrimSpace(row.CloseObserveReason),
		row.UpdatedAt,
		strings.TrimSpace(row.SpendTxID),
	)
	return err
}

func ListSessionsByLifecycleStates(db *sql.DB, states ...string) ([]GatewaySessionRow, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	if len(states) == 0 {
		return nil, fmt.Errorf("states required")
	}
	clean := make([]string, 0, len(states))
	args := make([]any, 0, len(states))
	for _, st := range states {
		v := strings.ToLower(strings.TrimSpace(st))
		if v == "" {
			continue
		}
		clean = append(clean, v)
		args = append(args, v)
	}
	if len(clean) == 0 {
		return nil, fmt.Errorf("states required")
	}
	holders := strings.TrimRight(strings.Repeat("?,", len(clean)), ",")
	rows, err := db.Query(
		`SELECT `+sessionSelectColumns+`
		 FROM fee_pool_sessions
		 WHERE lifecycle_state IN (`+holders+`)`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]GatewaySessionRow, 0, 16)
	for rows.Next() {
		var row GatewaySessionRow
		if err := rows.Scan(sessionScanArgs(&row)...); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func FreezeOtherActiveSessionsByClientTx(tx *sql.Tx, clientID string, keepSpendTxID string, reason string, nowUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	clientID = NormalizeClientIDLoose(clientID)
	if clientID == "" {
		return fmt.Errorf("client_pubkey_hex required")
	}
	_, err := tx.Exec(
		`UPDATE fee_pool_sessions
		 SET lifecycle_state='frozen',frozen_reason=?,updated_at_unix=?
		 WHERE client_pubkey_hex=? AND lifecycle_state='active' AND spend_txid<>?`,
		strings.TrimSpace(reason), nowUnix, clientID, strings.TrimSpace(keepSpendTxID),
	)
	return err
}

func tableHasColumn(db *sql.DB, table string, column string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func rebuildFeePoolSessionsWithoutStatus(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
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
	if _, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS fee_pool_sessions_new (
			spend_txid TEXT PRIMARY KEY,
			client_pubkey_hex TEXT NOT NULL,
			client_bsv_pubkey_hex TEXT NOT NULL,
			server_bsv_pubkey_hex TEXT NOT NULL,
			input_amount_satoshi INTEGER NOT NULL,
			pool_amount_satoshi INTEGER NOT NULL,
			spend_tx_fee_satoshi INTEGER NOT NULL,
			sequence_num INTEGER NOT NULL,
			server_amount_satoshi INTEGER NOT NULL,
			client_amount_satoshi INTEGER NOT NULL,
			base_txid TEXT NOT NULL,
			final_txid TEXT NOT NULL,
			base_tx_hex TEXT NOT NULL,
			current_tx_hex TEXT NOT NULL,
			lifecycle_state TEXT NOT NULL DEFAULT 'pending_base_tx',
			frozen_reason TEXT NOT NULL DEFAULT '',
			last_submit_error TEXT NOT NULL DEFAULT '',
			submit_retry_count INTEGER NOT NULL DEFAULT 0,
			last_submit_attempt_at_unix INTEGER NOT NULL DEFAULT 0,
			close_submit_at_unix INTEGER NOT NULL DEFAULT 0,
			close_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			close_observe_status TEXT NOT NULL DEFAULT '',
			close_observe_reason TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`
		INSERT INTO fee_pool_sessions_new(
			spend_txid,client_pubkey_hex,client_bsv_pubkey_hex,server_bsv_pubkey_hex,
			input_amount_satoshi,pool_amount_satoshi,spend_tx_fee_satoshi,sequence_num,server_amount_satoshi,client_amount_satoshi,
			base_txid,final_txid,base_tx_hex,current_tx_hex,lifecycle_state,frozen_reason,last_submit_error,submit_retry_count,last_submit_attempt_at_unix,
			close_submit_at_unix,close_observed_at_unix,close_observe_status,close_observe_reason,created_at_unix,updated_at_unix
		)
		SELECT
			spend_txid,client_pubkey_hex,client_bsv_pubkey_hex,server_bsv_pubkey_hex,
			input_amount_satoshi,pool_amount_satoshi,spend_tx_fee_satoshi,sequence_num,server_amount_satoshi,client_amount_satoshi,
			base_txid,final_txid,base_tx_hex,current_tx_hex,
			LOWER(TRIM(COALESCE(lifecycle_state,''))),
			COALESCE(frozen_reason,''),
			COALESCE(last_submit_error,''),
			COALESCE(submit_retry_count,0),
			COALESCE(last_submit_attempt_at_unix,0),
			COALESCE(close_submit_at_unix,0),
			COALESCE(close_observed_at_unix,0),
			COALESCE(close_observe_status,''),
			COALESCE(close_observe_reason,''),
			created_at_unix,updated_at_unix
		FROM fee_pool_sessions`); err != nil {
		return err
	}
	if _, err = tx.Exec(`DROP TABLE fee_pool_sessions`); err != nil {
		return err
	}
	if _, err = tx.Exec(`ALTER TABLE fee_pool_sessions_new RENAME TO fee_pool_sessions`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_fee_pool_client_pubkey_hex ON fee_pool_sessions(client_pubkey_hex)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_fee_pool_lifecycle ON fee_pool_sessions(lifecycle_state)`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_fee_pool_updated_at ON fee_pool_sessions(updated_at_unix DESC)`); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}
