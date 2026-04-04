package clientapp

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type legacyWalletChainBackfillFacts struct {
	TxID                string
	PaymentSubType      string
	Status              string
	WalletInputSatoshi  int64
	WalletOutputSatoshi int64
	NetAmountSatoshi    int64
	BlockHeight         int64
	OccurredAtUnix      int64
	FromPartyID         string
	ToPartyID           string
	Payload             any
}

type legacyWalletChainBusinessRow struct {
	BusinessID        string
	SourceID          string
	AccountingSubtype string
	Status            string
	OccurredAtUnix    int64
	FromPartyID       string
	ToPartyID         string
	Note              string
	PayloadJSON       string
}

type legacyWalletChainBreakdownRow struct {
	BusinessID         string
	TxID               string
	TxRole             string
	GrossInputSatoshi  int64
	ChangeBackSatoshi  int64
	ExternalInSatoshi  int64
	CounterpartyOutSat int64
	MinerFeeSatoshi    int64
	NetOutSatoshi      int64
	NetInSatoshi       int64
	CreatedAtUnix      int64
	Note               string
	PayloadJSON        string
}

type legacyWalletChainProcessRow struct {
	ProcessID         string
	SourceID          string
	AccountingSubtype string
	Status            string
	OccurredAtUnix    int64
	Note              string
	PayloadJSON       string
}

// backfillLegacyFinanceSources 负责把历史财务来源回填到新口径。
// 设计说明：
// - 先回填 pool_allocation，再回填 wallet_chain；
// - 两条路径彼此独立，不做万能迁移；
// - 任何一条路径证据不够时，直接报错，不吞。
func backfillLegacyFinanceSources(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := backfillLegacyPoolAllocationFinanceSources(db); err != nil {
		return err
	}
	if err := backfillLegacyWalletChainFinanceSources(db); err != nil {
		return err
	}
	return nil
}

func backfillLegacyPoolAllocationFinanceSources(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}

	ids, err := legacyFinanceDistinctSourceIDs(tx, "pool_allocation")
	if err != nil {
		rollback()
		return fmt.Errorf("collect pool_allocation source ids: %w", err)
	}
	for _, sourceID := range ids {
		if looksLikeIntegerID(sourceID) {
			if !legacyPoolAllocationIDExistsTx(tx, sourceID) {
				rollback()
				return fmt.Errorf("pool_allocation source id %s already looks migrated but pool_allocations row is missing", sourceID)
			}
			continue
		}

		allocationID := sourceID
		poolAllocID, err := dbGetPoolAllocationIDByAllocationIDTx(tx, allocationID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				rollback()
				return fmt.Errorf("pool_allocation source id %s has no pool_allocations mapping", allocationID)
			}
			rollback()
			return fmt.Errorf("resolve pool_allocation mapping for %s: %w", allocationID, err)
		}
		newSourceID := fmt.Sprintf("%d", poolAllocID)
		if err := legacyRewriteFinanceSourceTx(tx, "pool_allocation", allocationID, "pool_allocation", newSourceID); err != nil {
			rollback()
			return fmt.Errorf("rewrite pool_allocation source %s -> %s: %w", allocationID, newSourceID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func backfillLegacyWalletChainFinanceSources(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}

	txIDs, err := legacyFinanceDistinctSourceIDs(tx, "wallet_chain")
	if err != nil {
		rollback()
		return fmt.Errorf("collect wallet_chain source ids: %w", err)
	}
	for _, txid := range txIDs {
		facts, err := resolveLegacyWalletChainFactsTx(tx, txid)
		if err != nil {
			rollback()
			return fmt.Errorf("resolve wallet_chain txid=%s: %w", txid, err)
		}

		chainPaymentID, err := dbUpsertChainPaymentDB(tx, chainPaymentEntry{
			TxID:                txid,
			PaymentSubType:      facts.PaymentSubType,
			Status:              facts.Status,
			WalletInputSatoshi:  facts.WalletInputSatoshi,
			WalletOutputSatoshi: facts.WalletOutputSatoshi,
			NetAmountSatoshi:    facts.NetAmountSatoshi,
			BlockHeight:         facts.BlockHeight,
			OccurredAtUnix:      facts.OccurredAtUnix,
			FromPartyID:         facts.FromPartyID,
			ToPartyID:           facts.ToPartyID,
			Payload:             facts.Payload,
		})
		if err != nil {
			rollback()
			return fmt.Errorf("upsert chain_payment for txid=%s: %w", txid, err)
		}

		newSourceID := fmt.Sprintf("%d", chainPaymentID)
		if err := legacyRewriteFinanceSourceTx(tx, "wallet_chain", txid, "chain_payment", newSourceID); err != nil {
			rollback()
			return fmt.Errorf("rewrite wallet_chain source for txid=%s: %w", txid, err)
		}
	}

	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func legacyFinanceDistinctSourceIDs(tx *sql.Tx, sourceType string) ([]string, error) {
	rows, err := tx.Query(`
		SELECT DISTINCT lower(trim(source_id))
		  FROM fin_business
		 WHERE source_type=?
		   AND trim(source_id)!=''
		UNION
		SELECT DISTINCT lower(trim(source_id))
		  FROM fin_process_events
		 WHERE source_type=?
		   AND trim(source_id)!=''
		ORDER BY 1 ASC`,
		sourceType, sourceType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 16)
	for rows.Next() {
		var sourceID string
		if err := rows.Scan(&sourceID); err != nil {
			return nil, err
		}
		sourceID = strings.TrimSpace(sourceID)
		if sourceID == "" {
			continue
		}
		out = append(out, sourceID)
	}
	return out, rows.Err()
}

func legacyRewriteFinanceSourceTx(tx *sql.Tx, oldSourceType, oldSourceID, newSourceType, newSourceID string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	oldSourceType = strings.TrimSpace(oldSourceType)
	oldSourceID = strings.TrimSpace(oldSourceID)
	newSourceType = strings.TrimSpace(newSourceType)
	newSourceID = strings.TrimSpace(newSourceID)
	if oldSourceType == "" {
		return fmt.Errorf("old source type is required")
	}
	if newSourceType == "" {
		return fmt.Errorf("new source type is required")
	}
	if oldSourceID == "" {
		return fmt.Errorf("old source id is required")
	}
	if newSourceID == "" {
		return fmt.Errorf("new source id is required")
	}

	if _, err := tx.Exec(
		`UPDATE fin_business SET source_type=?, source_id=? WHERE source_type=? AND lower(trim(source_id))=?`,
		newSourceType, newSourceID, oldSourceType, strings.ToLower(oldSourceID),
	); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE fin_process_events SET source_type=?, source_id=? WHERE source_type=? AND lower(trim(source_id))=?`,
		newSourceType, newSourceID, oldSourceType, strings.ToLower(oldSourceID),
	); err != nil {
		return err
	}
	return nil
}

func legacyPoolAllocationIDExistsTx(tx *sql.Tx, sourceID string) bool {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return false
	}
	var exists int
	err := tx.QueryRow(`SELECT 1 FROM pool_allocations WHERE id=?`, sourceID).Scan(&exists)
	return err == nil
}

func resolveLegacyWalletChainFactsTx(tx *sql.Tx, txid string) (legacyWalletChainBackfillFacts, error) {
	txid = strings.ToLower(strings.TrimSpace(txid))
	if txid == "" {
		return legacyWalletChainBackfillFacts{}, fmt.Errorf("txid is required")
	}

	businessRows, err := legacyWalletChainBusinessRowsTx(tx, txid)
	if err != nil {
		return legacyWalletChainBackfillFacts{}, err
	}
	breakdownRows, err := legacyWalletChainBreakdownRowsTx(tx, txid)
	if err != nil {
		return legacyWalletChainBackfillFacts{}, err
	}
	processRows, err := legacyWalletChainProcessRowsTx(tx, txid)
	if err != nil {
		return legacyWalletChainBackfillFacts{}, err
	}

	if len(businessRows) == 0 && len(breakdownRows) == 0 && len(processRows) == 0 {
		return legacyWalletChainBackfillFacts{}, fmt.Errorf("no wallet_chain evidence found")
	}

	facts := legacyWalletChainBackfillFacts{TxID: txid}
	payloads := make([]any, 0, 3)

	if len(breakdownRows) > 0 {
		row, err := collapseLegacyWalletChainBreakdownRows(breakdownRows)
		if err != nil {
			return legacyWalletChainBackfillFacts{}, err
		}
		if subtype := normalizeLegacyWalletChainSubtype(row.TxRole); subtype != "" {
			facts.PaymentSubType = subtype
		}
		facts.WalletInputSatoshi = row.GrossInputSatoshi
		facts.WalletOutputSatoshi = row.ChangeBackSatoshi + row.ExternalInSatoshi
		facts.NetAmountSatoshi = facts.WalletOutputSatoshi - facts.WalletInputSatoshi
		if row.CreatedAtUnix > 0 {
			facts.OccurredAtUnix = row.CreatedAtUnix
		}
		if row.PayloadJSON != "" && row.PayloadJSON != "{}" {
			payloads = append(payloads, rawJSONPayload(row.PayloadJSON))
		}
	}

	if len(businessRows) > 0 {
		row, err := collapseLegacyWalletChainBusinessRows(businessRows)
		if err != nil {
			return legacyWalletChainBackfillFacts{}, err
		}
		if subtype := normalizeLegacyWalletChainSubtype(row.AccountingSubtype); subtype != "" {
			if facts.PaymentSubType != "" && facts.PaymentSubType != subtype {
				return legacyWalletChainBackfillFacts{}, fmt.Errorf("wallet_chain txid=%s payment subtype conflict: breakdown=%s business=%s", txid, facts.PaymentSubType, subtype)
			}
			facts.PaymentSubType = subtype
		}
		if row.Status != "" {
			facts.Status = row.Status
		}
		if row.OccurredAtUnix > 0 {
			facts.OccurredAtUnix = row.OccurredAtUnix
		}
		if row.FromPartyID != "" {
			facts.FromPartyID = row.FromPartyID
		}
		if row.ToPartyID != "" {
			facts.ToPartyID = row.ToPartyID
		}
		if row.PayloadJSON != "" && row.PayloadJSON != "{}" {
			payloads = append(payloads, rawJSONPayload(row.PayloadJSON))
		}
	}

	if len(processRows) > 0 {
		row, err := collapseLegacyWalletChainProcessRows(processRows)
		if err != nil {
			return legacyWalletChainBackfillFacts{}, err
		}
		if subtype := normalizeLegacyWalletChainSubtype(row.AccountingSubtype); subtype != "" && facts.PaymentSubType == "" {
			facts.PaymentSubType = subtype
		}
		if facts.Status == "" && row.Status != "" {
			facts.Status = row.Status
		}
		if facts.OccurredAtUnix <= 0 && row.OccurredAtUnix > 0 {
			facts.OccurredAtUnix = row.OccurredAtUnix
		}
		if row.PayloadJSON != "" && row.PayloadJSON != "{}" {
			payloads = append(payloads, rawJSONPayload(row.PayloadJSON))
		}
	}

	// 只在上层事实仍然缺位时，才允许从 payload 里补数。
	if facts.PaymentSubType == "" || (facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi == 0) {
		for _, payload := range payloads {
			payloadFacts, err := legacyWalletChainFactsFromPayload(payload)
			if err != nil {
				return legacyWalletChainBackfillFacts{}, err
			}
			if facts.PaymentSubType == "" && payloadFacts.PaymentSubType != "" {
				facts.PaymentSubType = payloadFacts.PaymentSubType
			}
			if facts.Status == "" && payloadFacts.Status != "" {
				facts.Status = payloadFacts.Status
			}
			if facts.OccurredAtUnix <= 0 && payloadFacts.OccurredAtUnix > 0 {
				facts.OccurredAtUnix = payloadFacts.OccurredAtUnix
			}
			if facts.BlockHeight <= 0 && payloadFacts.BlockHeight > 0 {
				facts.BlockHeight = payloadFacts.BlockHeight
			}
			if facts.FromPartyID == "" && payloadFacts.FromPartyID != "" {
				facts.FromPartyID = payloadFacts.FromPartyID
			}
			if facts.ToPartyID == "" && payloadFacts.ToPartyID != "" {
				facts.ToPartyID = payloadFacts.ToPartyID
			}
			if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi == 0 &&
				(payloadFacts.WalletInputSatoshi != 0 || payloadFacts.WalletOutputSatoshi != 0 || payloadFacts.NetAmountSatoshi != 0) {
				facts.WalletInputSatoshi = payloadFacts.WalletInputSatoshi
				facts.WalletOutputSatoshi = payloadFacts.WalletOutputSatoshi
				facts.NetAmountSatoshi = payloadFacts.NetAmountSatoshi
			}
			if facts.Payload == nil && payloadFacts.Payload != nil {
				facts.Payload = payloadFacts.Payload
			}
		}
	}

	if facts.PaymentSubType == "" {
		return legacyWalletChainBackfillFacts{}, fmt.Errorf("wallet_chain txid=%s has no payment subtype evidence", txid)
	}
	if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi == 0 {
		return legacyWalletChainBackfillFacts{}, fmt.Errorf("wallet_chain txid=%s has no amount evidence", txid)
	}

	if facts.Status == "" {
		facts.Status = "confirmed"
	}
	if facts.OccurredAtUnix <= 0 {
		facts.OccurredAtUnix = time.Now().Unix()
	}
	if facts.FromPartyID == "" || facts.ToPartyID == "" {
		fromParty, toParty := legacyWalletChainPartiesForSubtype(facts.PaymentSubType)
		if facts.FromPartyID == "" {
			facts.FromPartyID = fromParty
		}
		if facts.ToPartyID == "" {
			facts.ToPartyID = toParty
		}
	}
	if facts.Payload == nil {
		switch {
		case len(businessRows) > 0:
			if payload, ok := firstNonEmptyRawJSON(businessRows[0].PayloadJSON); ok {
				facts.Payload = rawJSONPayload(payload)
			}
		case len(processRows) > 0:
			if payload, ok := firstNonEmptyRawJSON(processRows[0].PayloadJSON); ok {
				facts.Payload = rawJSONPayload(payload)
			}
		case len(breakdownRows) > 0:
			if payload, ok := firstNonEmptyRawJSON(breakdownRows[0].PayloadJSON); ok {
				facts.Payload = rawJSONPayload(payload)
			}
		}
	}
	if facts.Payload == nil {
		facts.Payload = map[string]any{
			"txid":   txid,
			"source": "legacy_wallet_chain_backfill",
		}
	}

	return facts, nil
}

func legacyWalletChainBusinessRowsTx(tx *sql.Tx, txid string) ([]legacyWalletChainBusinessRow, error) {
	rows, err := tx.Query(`
		SELECT business_id,source_id,accounting_subtype,status,occurred_at_unix,from_party_id,to_party_id,note,payload_json
		  FROM fin_business
		 WHERE source_type='wallet_chain'
		   AND lower(trim(source_id))=?
		 ORDER BY occurred_at_unix ASC,business_id ASC`,
		txid,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]legacyWalletChainBusinessRow, 0, 4)
	for rows.Next() {
		var row legacyWalletChainBusinessRow
		if err := rows.Scan(&row.BusinessID, &row.SourceID, &row.AccountingSubtype, &row.Status, &row.OccurredAtUnix, &row.FromPartyID, &row.ToPartyID, &row.Note, &row.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func legacyWalletChainBreakdownRowsTx(tx *sql.Tx, txid string) ([]legacyWalletChainBreakdownRow, error) {
	rows, err := tx.Query(`
		SELECT b.business_id,b.txid,b.tx_role,b.gross_input_satoshi,b.change_back_satoshi,b.external_in_satoshi,b.counterparty_out_satoshi,b.miner_fee_satoshi,b.net_out_satoshi,b.net_in_satoshi,b.created_at_unix,b.note,b.payload_json
		  FROM fin_tx_breakdown b
		 WHERE lower(trim(b.txid))=?
		   AND b.business_id LIKE 'biz_wallet_chain_%'
		 ORDER BY b.id ASC`,
		txid,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]legacyWalletChainBreakdownRow, 0, 4)
	for rows.Next() {
		var row legacyWalletChainBreakdownRow
		if err := rows.Scan(&row.BusinessID, &row.TxID, &row.TxRole, &row.GrossInputSatoshi, &row.ChangeBackSatoshi, &row.ExternalInSatoshi, &row.CounterpartyOutSat, &row.MinerFeeSatoshi, &row.NetOutSatoshi, &row.NetInSatoshi, &row.CreatedAtUnix, &row.Note, &row.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func legacyWalletChainProcessRowsTx(tx *sql.Tx, txid string) ([]legacyWalletChainProcessRow, error) {
	rows, err := tx.Query(`
		SELECT process_id,source_id,accounting_subtype,status,occurred_at_unix,note,payload_json
		  FROM fin_process_events
		 WHERE source_type='wallet_chain'
		   AND lower(trim(source_id))=?
		 ORDER BY occurred_at_unix ASC,process_id ASC`,
		txid,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]legacyWalletChainProcessRow, 0, 4)
	for rows.Next() {
		var row legacyWalletChainProcessRow
		if err := rows.Scan(&row.ProcessID, &row.SourceID, &row.AccountingSubtype, &row.Status, &row.OccurredAtUnix, &row.Note, &row.PayloadJSON); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func collapseLegacyWalletChainBusinessRows(rows []legacyWalletChainBusinessRow) (legacyWalletChainBusinessRow, error) {
	if len(rows) == 0 {
		return legacyWalletChainBusinessRow{}, fmt.Errorf("business rows are empty")
	}
	base := rows[0]
	for _, row := range rows[1:] {
		if !strings.EqualFold(strings.TrimSpace(row.SourceID), strings.TrimSpace(base.SourceID)) {
			return legacyWalletChainBusinessRow{}, fmt.Errorf("wallet_chain business source_id conflict: %s vs %s", base.SourceID, row.SourceID)
		}
		if normalizeLegacyWalletChainSubtype(row.AccountingSubtype) != normalizeLegacyWalletChainSubtype(base.AccountingSubtype) {
			return legacyWalletChainBusinessRow{}, fmt.Errorf("wallet_chain business subtype conflict: %s vs %s", base.AccountingSubtype, row.AccountingSubtype)
		}
		if strings.TrimSpace(row.Status) != "" && strings.TrimSpace(base.Status) != "" && strings.TrimSpace(row.Status) != strings.TrimSpace(base.Status) {
			return legacyWalletChainBusinessRow{}, fmt.Errorf("wallet_chain business status conflict: %s vs %s", base.Status, row.Status)
		}
	}
	return base, nil
}

func collapseLegacyWalletChainBreakdownRows(rows []legacyWalletChainBreakdownRow) (legacyWalletChainBreakdownRow, error) {
	if len(rows) == 0 {
		return legacyWalletChainBreakdownRow{}, fmt.Errorf("breakdown rows are empty")
	}
	base := rows[0]
	for _, row := range rows[1:] {
		if !strings.EqualFold(strings.TrimSpace(row.TxID), strings.TrimSpace(base.TxID)) {
			return legacyWalletChainBreakdownRow{}, fmt.Errorf("wallet_chain breakdown txid conflict: %s vs %s", base.TxID, row.TxID)
		}
		if normalizeLegacyWalletChainSubtype(row.TxRole) != normalizeLegacyWalletChainSubtype(base.TxRole) {
			return legacyWalletChainBreakdownRow{}, fmt.Errorf("wallet_chain breakdown subtype conflict: %s vs %s", base.TxRole, row.TxRole)
		}
		if row.GrossInputSatoshi != base.GrossInputSatoshi || row.ChangeBackSatoshi != base.ChangeBackSatoshi || row.ExternalInSatoshi != base.ExternalInSatoshi || row.CounterpartyOutSat != base.CounterpartyOutSat || row.MinerFeeSatoshi != base.MinerFeeSatoshi || row.NetOutSatoshi != base.NetOutSatoshi || row.NetInSatoshi != base.NetInSatoshi {
			return legacyWalletChainBreakdownRow{}, fmt.Errorf("wallet_chain breakdown amount conflict for txid=%s", base.TxID)
		}
	}
	return base, nil
}

func collapseLegacyWalletChainProcessRows(rows []legacyWalletChainProcessRow) (legacyWalletChainProcessRow, error) {
	if len(rows) == 0 {
		return legacyWalletChainProcessRow{}, fmt.Errorf("process rows are empty")
	}
	base := rows[0]
	for _, row := range rows[1:] {
		if !strings.EqualFold(strings.TrimSpace(row.SourceID), strings.TrimSpace(base.SourceID)) {
			return legacyWalletChainProcessRow{}, fmt.Errorf("wallet_chain process source_id conflict: %s vs %s", base.SourceID, row.SourceID)
		}
		if normalizeLegacyWalletChainSubtype(row.AccountingSubtype) != normalizeLegacyWalletChainSubtype(base.AccountingSubtype) {
			return legacyWalletChainProcessRow{}, fmt.Errorf("wallet_chain process subtype conflict: %s vs %s", base.AccountingSubtype, row.AccountingSubtype)
		}
	}
	return base, nil
}

func collapseLegacyWalletChainBusinessRowsMust(rows []legacyWalletChainBusinessRow) legacyWalletChainBusinessRow {
	row, _ := collapseLegacyWalletChainBusinessRows(rows)
	return row
}

func collapseLegacyWalletChainBreakdownRowsMust(rows []legacyWalletChainBreakdownRow) legacyWalletChainBreakdownRow {
	row, _ := collapseLegacyWalletChainBreakdownRows(rows)
	return row
}

func collapseLegacyWalletChainProcessRowsMust(rows []legacyWalletChainProcessRow) legacyWalletChainProcessRow {
	row, _ := collapseLegacyWalletChainProcessRows(rows)
	return row
}

func legacyWalletChainFactsFromPayload(payload any) (legacyWalletChainBackfillFacts, error) {
	raw := strings.TrimSpace(mustJSONString(payload))
	if raw == "" || raw == "{}" {
		return legacyWalletChainBackfillFacts{}, nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return legacyWalletChainBackfillFacts{}, fmt.Errorf("decode wallet_chain payload failed: %w", err)
	}

	facts := legacyWalletChainBackfillFacts{}
	facts.PaymentSubType = normalizeLegacyWalletChainSubtype(jsonFirstString(obj, "payment_subtype", "accounting_subtype", "tx_role", "category"))
	if subtype := normalizeLegacyWalletChainSubtype(jsonFirstString(obj, "category")); facts.PaymentSubType == "" && subtype != "" {
		facts.PaymentSubType = subtype
	}
	facts.Status = jsonFirstString(obj, "status")
	facts.FromPartyID = jsonFirstString(obj, "from_party_id")
	facts.ToPartyID = jsonFirstString(obj, "to_party_id")
	facts.OccurredAtUnix = jsonFirstInt64(obj, "occurred_at_unix")
	facts.BlockHeight = jsonFirstInt64(obj, "block_height")
	facts.WalletInputSatoshi = jsonFirstInt64(obj, "wallet_input_satoshi", "wallet_input_sat", "gross_input_satoshi")
	facts.WalletOutputSatoshi = jsonFirstInt64(obj, "wallet_output_satoshi", "wallet_output_sat")
	facts.NetAmountSatoshi = jsonFirstInt64(obj, "net_amount_satoshi", "net_sat")

	if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi == 0 {
		grossInput := jsonFirstInt64(obj, "gross_input_satoshi")
		changeBack := jsonFirstInt64(obj, "change_back_satoshi")
		externalIn := jsonFirstInt64(obj, "external_in_satoshi")
		counterpartyOut := jsonFirstInt64(obj, "counterparty_out_satoshi")
		if grossInput != 0 || changeBack != 0 || externalIn != 0 || counterpartyOut != 0 {
			facts.WalletInputSatoshi = grossInput
			facts.WalletOutputSatoshi = changeBack + externalIn
			facts.NetAmountSatoshi = facts.WalletOutputSatoshi - facts.WalletInputSatoshi
		}
	}
	if facts.NetAmountSatoshi == 0 && (facts.WalletInputSatoshi != 0 || facts.WalletOutputSatoshi != 0) {
		switch facts.PaymentSubType {
		case "external_in":
			facts.NetAmountSatoshi = facts.WalletOutputSatoshi
		case "external_out":
			facts.NetAmountSatoshi = -facts.WalletInputSatoshi
		default:
			facts.NetAmountSatoshi = facts.WalletOutputSatoshi - facts.WalletInputSatoshi
		}
	}
	if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi != 0 {
		switch facts.PaymentSubType {
		case "external_in":
			facts.WalletOutputSatoshi = facts.NetAmountSatoshi
		case "external_out":
			facts.WalletInputSatoshi = -facts.NetAmountSatoshi
		}
	}
	if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi != 0 && facts.NetAmountSatoshi == 0 {
		switch facts.PaymentSubType {
		case "external_in":
			facts.NetAmountSatoshi = facts.WalletOutputSatoshi
		case "external_out":
			facts.NetAmountSatoshi = -facts.WalletInputSatoshi
		}
	}
	if facts.WalletInputSatoshi == 0 && facts.WalletOutputSatoshi == 0 && facts.NetAmountSatoshi == 0 {
		return legacyWalletChainBackfillFacts{}, nil
	}
	facts.Payload = rawJSONPayload(raw)
	return facts, nil
}

func normalizeLegacyWalletChainSubtype(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "internal_change", "external_in", "external_out", "fee_pool_settle":
		return strings.ToLower(strings.TrimSpace(raw))
	case "change":
		return "internal_change"
	case "repayment":
		return "external_in"
	case "third_party":
		return "external_out"
	case "fee_pool":
		return "fee_pool_settle"
	case "internal":
		return "internal_change"
	default:
		return ""
	}
}

func legacyWalletChainPartiesForSubtype(subtype string) (string, string) {
	switch normalizeLegacyWalletChainSubtype(subtype) {
	case "external_in":
		return "external:unknown", "wallet:self"
	case "external_out":
		return "wallet:self", "external:unknown"
	default:
		return "wallet:self", "wallet:self"
	}
}

func looksLikeIntegerID(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if _, err := strconv.ParseInt(raw, 10, 64); err != nil {
		return false
	}
	return true
}

func jsonFirstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := asJSONString(v); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					return s
				}
			}
		}
	}
	return ""
}

func jsonFirstInt64(m map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if n, ok := asJSONInt64(v); ok {
				return n
			}
		}
	}
	return 0
}

func asJSONString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case json.Number:
		return t.String(), true
	case fmt.Stringer:
		return t.String(), true
	case float64:
		return strconv.FormatInt(int64(t), 10), true
	case float32:
		return strconv.FormatInt(int64(t), 10), true
	case int:
		return strconv.Itoa(t), true
	case int8:
		return strconv.FormatInt(int64(t), 10), true
	case int16:
		return strconv.FormatInt(int64(t), 10), true
	case int32:
		return strconv.FormatInt(int64(t), 10), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case uint:
		return strconv.FormatUint(uint64(t), 10), true
	case uint8:
		return strconv.FormatUint(uint64(t), 10), true
	case uint16:
		return strconv.FormatUint(uint64(t), 10), true
	case uint32:
		return strconv.FormatUint(uint64(t), 10), true
	case uint64:
		return strconv.FormatUint(t, 10), true
	default:
		return "", false
	}
}

func asJSONInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return 0, false
		}
		return n, true
	case float64:
		return int64(t), true
	case float32:
		return int64(t), true
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case uint:
		return int64(t), true
	case uint8:
		return int64(t), true
	case uint16:
		return int64(t), true
	case uint32:
		return int64(t), true
	case uint64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func firstNonEmptyRawJSON(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return "", false
	}
	return raw, true
}

// backfillFinBusinessRole 【第七阶段：历史数据回填收口】
// 设计说明：
// - 把 fin_business 历史数据按 business_id 前缀补上 business_role
// - biz_download_pool_* -> formal（正式收费对象）
// - biz_c2c_open_* / biz_c2c_close_* -> process（过程财务对象）
// - biz_wallet_chain_* -> process（钱包过程财务对象）
// - biz_feepool_open_* -> process（费用池过程对象）
// - biz_domain_* -> formal（域名正式收费对象）
// - 其他类型保持空值（由写入路径显式指定）
// - 这是过渡逻辑，新写入路径应显式写 business_role
func backfillFinBusinessRole(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 回填正式收费对象 - 下载池
	if _, err := db.Exec(
		`UPDATE fin_business SET business_role='formal'
		 WHERE business_id LIKE 'biz_download_pool_%' AND (business_role='' OR business_role IS NULL)`,
	); err != nil {
		return fmt.Errorf("backfill formal role for biz_download_pool_*: %w", err)
	}

	// 回填正式收费对象 - 域名
	if _, err := db.Exec(
		`UPDATE fin_business SET business_role='formal'
		 WHERE business_id LIKE 'biz_domain_%' AND (business_role='' OR business_role IS NULL)`,
	); err != nil {
		return fmt.Errorf("backfill formal role for biz_domain_*: %w", err)
	}

	// 回填过程财务对象 - 直连池开闭
	if _, err := db.Exec(
		`UPDATE fin_business SET business_role='process'
		 WHERE (business_id LIKE 'biz_c2c_open_%' OR business_id LIKE 'biz_c2c_close_%')
		   AND (business_role='' OR business_role IS NULL)`,
	); err != nil {
		return fmt.Errorf("backfill process role for biz_c2c_*: %w", err)
	}

	// 回填过程财务对象 - 钱包链
	if _, err := db.Exec(
		`UPDATE fin_business SET business_role='process'
		 WHERE business_id LIKE 'biz_wallet_chain_%'
		   AND (business_role='' OR business_role IS NULL)`,
	); err != nil {
		return fmt.Errorf("backfill process role for biz_wallet_chain_*: %w", err)
	}

	// 回填过程财务对象 - 费用池
	if _, err := db.Exec(
		`UPDATE fin_business SET business_role='process'
		 WHERE business_id LIKE 'biz_feepool_%'
		   AND (business_role='' OR business_role IS NULL)`,
	); err != nil {
		return fmt.Errorf("backfill process role for biz_feepool_*: %w", err)
	}

	return nil
}

// CountEmptyBusinessRole 统计还剩多少空 business_role 记录
// 第七阶段新增：用于监控历史回填进度
func CountEmptyBusinessRole(db *sql.DB) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	var count int
	err := db.QueryRow(
		`SELECT COUNT(1) FROM fin_business WHERE business_role='' OR business_role IS NULL`,
	).Scan(&count)
	return count, err
}

// ListEmptyBusinessRoleIDs 列出空 business_role 的记录 ID
// 第七阶段新增：用于识别哪些记录还需要手动处理
func ListEmptyBusinessRoleIDs(db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(
		`SELECT business_id FROM fin_business WHERE business_role='' OR business_role IS NULL ORDER BY business_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
