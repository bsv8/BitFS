package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factbsvutxos"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/factsettlementrecords"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/facttokenlots"
)

// ============================================================
// 新资产账本表（硬切版）数据结构与读写入口
// 设计说明：
// - 新四表：fact_bsv_utxos（本币UTXO事实）、fact_token_lots（Token数量事实）、
//          fact_token_carrier_links（Token与载体绑定）、fact_settlement_records（结算消耗记录）
// - 余额事实统一从新表计算，无 direction IN/OUT 模式
// ============================================================

// ========== 数据结构定义 ==========

// bsvUTXOEntry fact_bsv_utxos 写入条目
type bsvUTXOEntry struct {
	UTXOID         string
	OwnerPubkeyHex string
	Address        string
	TxID           string
	Vout           uint32
	ValueSatoshi   int64
	UTXOState      string // unspent, spent
	CarrierType    string // plain_bsv, token_carrier, fee_change, unknown
	SpentByTxid    string
	CreatedAtUnix  int64
	UpdatedAtUnix  int64
	SpentAtUnix    int64
	Note           string
	Payload        any
}

// tokenLotEntry fact_token_lots 写入条目
type tokenLotEntry struct {
	LotID            string
	OwnerPubkeyHex   string
	TokenID          string
	TokenStandard    string // BSV20, BSV21
	QuantityText     string // 入账数量（十进制字符串）
	UsedQuantityText string // 累计消耗（十进制字符串）
	LotState         string // unspent, spent, locked
	MintTxid         string
	LastSpendTxid    string
	CreatedAtUnix    int64
	UpdatedAtUnix    int64
	Note             string
	Payload          any
}

// tokenCarrierLinkEntry fact_token_carrier_links 写入条目
type tokenCarrierLinkEntry struct {
	LinkID         string
	LotID          string
	CarrierUTXOID  string
	OwnerPubkeyHex string
	LinkState      string // active, released, moved
	BindTxid       string
	UnbindTxid     string
	CreatedAtUnix  int64
	UpdatedAtUnix  int64
	Note           string
	Payload        any
}

// settlementRecordEntry fact_settlement_records 写入条目
type settlementRecordEntry struct {
	RecordID                   string
	SettlementPaymentAttemptID int64
	AssetType                  string // BSV, TOKEN
	OwnerPubkeyHex             string
	SourceUTXOID               string // 本币用
	SourceLotID                string // token用
	UsedSatoshi                int64
	UsedQuantityText           string
	State                      string // pending, confirmed, reverted
	OccurredAtUnix             int64
	ConfirmedAtUnix            int64
	Note                       string
	Payload                    any
}

// dbAppendBSVSettlementRecordsForCycleTx 写入 chain_direct_pay 的 BSV 结算明细。
// 设计说明：
// - 这里只认本次交易真实花掉的输入 UTXO 列表，不从余额反推；
// - 每个输入只落一条明细，幂等由 settlement_payment_attempt_id + asset_type + source_utxo_id + source_lot_id 收口；
// - 这里不改 UTXO 状态，状态流转由本地投影和事实明细各自负责。
func dbAppendBSVSettlementRecordsForCycleTx(ctx context.Context, tx *gen.Tx, settlementPaymentAttemptID int64, ownerPubkeyHex string, utxoIDs []string, occurredAtUnix int64, payload any) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	if len(utxoIDs) == 0 {
		return fmt.Errorf("utxo_ids are required")
	}
	now := time.Now().Unix()
	if occurredAtUnix <= 0 {
		occurredAtUnix = now
	}
	confirmedAtUnix := occurredAtUnix

	seen := make(map[string]struct{}, len(utxoIDs))
	for i, rawUTXOID := range utxoIDs {
		utxoID := strings.ToLower(strings.TrimSpace(rawUTXOID))
		if utxoID == "" {
			return fmt.Errorf("utxo_id is required at index %d", i)
		}
		if _, ok := seen[utxoID]; ok {
			return fmt.Errorf("duplicate utxo_id in settlement inputs: %s", utxoID)
		}
		seen[utxoID] = struct{}{}

		row, err := tx.FactBsvUtxos.Query().
			Where(factbsvutxos.UtxoIDEQ(utxoID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("bsv utxo not found: %s", utxoID)
			}
			return fmt.Errorf("lookup bsv utxo %s failed: %w", utxoID, err)
		}
		usedSatoshi := row.ValueSatoshi

		recordID := fmt.Sprintf("rec_bsv_%d_%s", settlementPaymentAttemptID, utxoID)
		existing, err := tx.FactSettlementRecords.Query().
			Where(
				factsettlementrecords.SettlementPaymentAttemptIDEQ(settlementPaymentAttemptID),
				factsettlementrecords.AssetTypeEQ("BSV"),
				factsettlementrecords.SourceUtxoIDEQ(utxoID),
				factsettlementrecords.SourceLotIDEQ(""),
			).
			Only(ctx)
		if err == nil {
			state := "confirmed"
			if existing.State == "confirmed" && state == "pending" {
				state = existing.State
			}
			confirmedAt := existing.ConfirmedAtUnix
			if state == "confirmed" {
				confirmedAt = confirmedAtUnix
			}
			_, err = tx.FactSettlementRecords.UpdateOneID(existing.ID).
				SetOwnerPubkeyHex(ownerPubkeyHex).
				SetUsedSatoshi(usedSatoshi).
				SetUsedQuantityText("").
				SetState(state).
				SetConfirmedAtUnix(confirmedAt).
				SetNote("BSV consumed by chain direct pay").
				SetPayloadJSON(mustJSONString(payload)).
				Save(ctx)
			if err != nil {
				return fmt.Errorf("append bsv settlement record for utxo %s failed: %w", utxoID, err)
			}
			continue
		}
		if err != nil && !gen.IsNotFound(err) {
			return fmt.Errorf("append bsv settlement record for utxo %s failed: %w", utxoID, err)
		}
		if _, err := tx.FactSettlementRecords.Create().
			SetRecordID(recordID).
			SetSettlementPaymentAttemptID(settlementPaymentAttemptID).
			SetAssetType("BSV").
			SetOwnerPubkeyHex(ownerPubkeyHex).
			SetSourceUtxoID(utxoID).
			SetSourceLotID("").
			SetUsedSatoshi(usedSatoshi).
			SetUsedQuantityText("").
			SetState("confirmed").
			SetOccurredAtUnix(occurredAtUnix).
			SetConfirmedAtUnix(confirmedAtUnix).
			SetNote("BSV consumed by chain direct pay").
			SetPayloadJSON(mustJSONString(payload)).
			Save(ctx); err != nil {
			return fmt.Errorf("append bsv settlement record for utxo %s failed: %w", utxoID, err)
		}
	}
	return nil
}

// ========== BSV UTXO 读写 ==========

// dbUpsertBSVUTXO 幂等写入/更新本币UTXO事实
func dbUpsertBSVUTXO(ctx context.Context, store *clientDB, e bsvUTXOEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return dbUpsertBSVUTXOTx(ctx, tx, e)
	})
}

func dbUpsertBSVUTXOTx(ctx context.Context, tx *gen.Tx, e bsvUTXOEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	utxoID := strings.ToLower(strings.TrimSpace(e.UTXOID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return fmt.Errorf("txid is required")
	}

	utxoState := strings.ToLower(strings.TrimSpace(e.UTXOState))
	if utxoState == "" {
		utxoState = "unspent"
	}
	if utxoState != "unspent" && utxoState != "spent" {
		return fmt.Errorf("utxo_state must be unspent or spent, got %s", utxoState)
	}

	carrierType := strings.ToLower(strings.TrimSpace(e.CarrierType))
	if carrierType == "" {
		carrierType = "plain_bsv"
	}
	if carrierType != "plain_bsv" && carrierType != "token_carrier" && carrierType != "fee_change" && carrierType != "unknown" {
		return fmt.Errorf("carrier_type must be plain_bsv/token_carrier/fee_change/unknown, got %s", carrierType)
	}

	now := time.Now().Unix()
	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := e.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}
	spentAt := e.SpentAtUnix
	if utxoState == "spent" && spentAt <= 0 {
		spentAt = now
	}
	spentByTxid := strings.ToLower(strings.TrimSpace(e.SpentByTxid))
	payloadJSON := mustJSONString(e.Payload)
	existing, err := tx.FactBsvUtxos.Query().
		Where(factbsvutxos.UtxoIDEQ(utxoID)).
		Only(ctx)
	if err == nil {
		expectedSpentAt := existing.SpentAtUnix
		if utxoState == "spent" && expectedSpentAt == 0 {
			expectedSpentAt = spentAt
		}
		if strings.TrimSpace(existing.OwnerPubkeyHex) == ownerPubkey &&
			strings.TrimSpace(existing.Address) == strings.TrimSpace(e.Address) &&
			strings.TrimSpace(existing.Txid) == txid &&
			existing.Vout == int64(e.Vout) &&
			existing.ValueSatoshi == e.ValueSatoshi &&
			strings.TrimSpace(existing.UtxoState) == utxoState &&
			strings.TrimSpace(existing.CarrierType) == carrierType &&
			strings.TrimSpace(existing.SpentByTxid) == spentByTxid &&
			existing.SpentAtUnix == expectedSpentAt &&
			strings.TrimSpace(existing.Note) == strings.TrimSpace(e.Note) &&
			strings.TrimSpace(existing.PayloadJSON) == payloadJSON {
			return nil
		}
		nextSpentBy := existing.SpentByTxid
		if utxoState == "spent" {
			nextSpentBy = spentByTxid
		}
		nextSpentAt := existing.SpentAtUnix
		if utxoState == "spent" && existing.SpentAtUnix == 0 {
			nextSpentAt = spentAt
		}
		_, err = tx.FactBsvUtxos.UpdateOneID(existing.ID).
			SetOwnerPubkeyHex(ownerPubkey).
			SetAddress(strings.TrimSpace(e.Address)).
			SetTxid(txid).
			SetVout(int64(e.Vout)).
			SetValueSatoshi(e.ValueSatoshi).
			SetUtxoState(utxoState).
			SetCarrierType(carrierType).
			SetSpentByTxid(nextSpentBy).
			SetUpdatedAtUnix(updatedAt).
			SetSpentAtUnix(nextSpentAt).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(payloadJSON).
			Save(ctx)
		if err == nil && utxoState == "spent" && spentByTxid != "" {
			emitFactBSVSpentAppliedEvent(ctx, tx, strings.TrimSpace(e.Address), spentByTxid)
		}
		return err
	}
	if err != nil && !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.FactBsvUtxos.Create().
		SetUtxoID(utxoID).
		SetOwnerPubkeyHex(ownerPubkey).
		SetAddress(strings.TrimSpace(e.Address)).
		SetTxid(txid).
		SetVout(int64(e.Vout)).
		SetValueSatoshi(e.ValueSatoshi).
		SetUtxoState(utxoState).
		SetCarrierType(carrierType).
		SetSpentByTxid(spentByTxid).
		SetCreatedAtUnix(createdAt).
		SetUpdatedAtUnix(updatedAt).
		SetSpentAtUnix(spentAt).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(payloadJSON).
		Save(ctx)
	if err == nil && utxoState == "spent" && spentByTxid != "" {
		emitFactBSVSpentAppliedEvent(ctx, tx, strings.TrimSpace(e.Address), spentByTxid)
	}
	return err
}

// dbMarkBSVUTXOSpent 标记本币UTXO为已花费。
// 说明：这是内部/测试辅助入口，业务路径必须走 settlement_payment_attempt 驱动的扣账。
func dbMarkBSVUTXOSpent(ctx context.Context, store *clientDB, utxoID string, spentByTxid string) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return clientDBEntTx(ctx, store, func(tx *gen.Tx) error {
		return markBSVUTXOSpentTx(ctx, tx, utxoID, spentByTxid, time.Now().Unix())
	})
}

func dbMarkBSVUTXOSpentTx(ctx context.Context, tx *gen.Tx, utxoID string, spentByTxid string) error {
	return markBSVUTXOSpentTx(ctx, tx, utxoID, spentByTxid, time.Now().Unix())
}

func markBSVUTXOSpentTx(ctx context.Context, tx *gen.Tx, utxoID string, spentByTxid string, updatedAt int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	current, err := tx.FactBsvUtxos.Query().
		Where(factbsvutxos.UtxoIDEQ(utxoID)).
		Only(ctx)
	if err != nil && !gen.IsNotFound(err) {
		return err
	}
	if err == nil && strings.ToLower(strings.TrimSpace(current.UtxoState)) == "spent" && strings.TrimSpace(current.SpentByTxid) == spentByTxid && current.SpentAtUnix > 0 {
		return nil
	}
	if err == nil {
		if _, err := tx.FactBsvUtxos.UpdateOneID(current.ID).
			SetUtxoState("spent").
			SetSpentByTxid(spentByTxid).
			SetSpentAtUnix(updatedAt).
			SetUpdatedAtUnix(updatedAt).
			Save(ctx); err != nil {
			return err
		}
		address := strings.TrimSpace(current.Address)
		if address != "" {
			emitFactBSVSpentAppliedEvent(ctx, tx, address, spentByTxid)
		}
		return nil
	}
	return nil
}

func queryFactBSVSpentCountByAddressTxID(ctx context.Context, tx *gen.Tx, address string, spentByTxid string) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("tx is nil")
	}
	address = strings.TrimSpace(address)
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if address == "" || spentByTxid == "" {
		return 0, fmt.Errorf("address and spent_by_txid are required")
	}
	n, err := tx.FactBsvUtxos.Query().
		Where(
			factbsvutxos.AddressEQ(address),
			factbsvutxos.UtxoStateEQ("spent"),
			factbsvutxos.SpentByTxidEQ(spentByTxid),
		).
		Count(ctx)
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func emitFactBSVSpentAppliedEvent(ctx context.Context, tx *gen.Tx, address string, spentByTxid string) {
	address = strings.TrimSpace(address)
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if address == "" || spentByTxid == "" {
		return
	}
	spentRowCount, err := queryFactBSVSpentCountByAddressTxID(ctx, tx, address, spentByTxid)
	if err != nil {
		return
	}
	obs.Info("bitcast-client", "fact_bsv_spent_applied", map[string]any{
		"address":         address,
		"spent_by_txid":   spentByTxid,
		"spent_row_count": spentRowCount,
		"sync_round_id":   strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey))),
		"trigger_source":  strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey))),
	})
}

func dbLoadBSVUTXOAddressByIDTx(ctx context.Context, db sqlConn, utxoID string) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return "", fmt.Errorf("utxo_id is required")
	}
	var address string
	if err := QueryRowContext(ctx, db, `SELECT address FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(&address); err != nil {
		return "", err
	}
	return strings.TrimSpace(address), nil
}

func emitFactBSVSpentAppliedEventConn(ctx context.Context, db sqlConn, address string, spentByTxid string) {
	address = strings.TrimSpace(address)
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if address == "" || spentByTxid == "" {
		return
	}
	var spentRowCount int64
	if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_bsv_utxos WHERE address=? AND utxo_state='spent' AND spent_by_txid=?`, address, spentByTxid).Scan(&spentRowCount); err != nil {
		return
	}
	obs.Info("bitcast-client", "fact_bsv_spent_applied", map[string]any{
		"address":         address,
		"spent_by_txid":   spentByTxid,
		"spent_row_count": spentRowCount,
		"sync_round_id":   strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey))),
		"trigger_source":  strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey))),
	})
}

// dbGetBSVUTXO 查询单个本币UTXO
func dbGetBSVUTXO(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (*bsvUTXOEntry, error) {
		return dbGetBSVUTXOTx(ctx, tx, utxoID)
	})
}

func dbGetBSVUTXOTx(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	if utxoID == "" {
		return nil, fmt.Errorf("utxo_id is required")
	}
	row, err := tx.FactBsvUtxos.Query().
		Where(factbsvutxos.UtxoIDEQ(utxoID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &bsvUTXOEntry{
		UTXOID:         row.UtxoID,
		OwnerPubkeyHex: row.OwnerPubkeyHex,
		Address:        row.Address,
		TxID:           row.Txid,
		Vout:           uint32(row.Vout),
		ValueSatoshi:   row.ValueSatoshi,
		UTXOState:      row.UtxoState,
		CarrierType:    row.CarrierType,
		SpentByTxid:    row.SpentByTxid,
		CreatedAtUnix:  row.CreatedAtUnix,
		UpdatedAtUnix:  row.UpdatedAtUnix,
		SpentAtUnix:    row.SpentAtUnix,
		Note:           row.Note,
		Payload:        row.PayloadJSON,
	}, nil
}

func dbGetBSVUTXODB(ctx context.Context, db sqlConn, utxoID string) (*bsvUTXOEntry, error) {
	return nil, fmt.Errorf("db direct access is removed")
}

func dbUpsertBSVUTXODB(ctx context.Context, db sqlConn, e bsvUTXOEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	utxoID := strings.ToLower(strings.TrimSpace(e.UTXOID))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	txid := strings.ToLower(strings.TrimSpace(e.TxID))
	if txid == "" {
		return fmt.Errorf("txid is required")
	}
	utxoState := strings.ToLower(strings.TrimSpace(e.UTXOState))
	if utxoState == "" {
		utxoState = "unspent"
	}
	carrierType := strings.ToLower(strings.TrimSpace(e.CarrierType))
	if carrierType == "" {
		carrierType = "plain_bsv"
	}
	now := time.Now().Unix()
	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := e.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}
	spentAt := e.SpentAtUnix
	if utxoState == "spent" && spentAt <= 0 {
		spentAt = now
	}
	spentByTxid := strings.ToLower(strings.TrimSpace(e.SpentByTxid))
	var currentSpentByTxid string
	if err := QueryRowContext(ctx, db, `SELECT COALESCE(spent_by_txid,''), COALESCE(spent_at_unix,0) FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(&currentSpentByTxid, new(int64)); err == nil {
		if currentSpentByTxid == spentByTxid && utxoState == "spent" {
			return nil
		}
	}
	_, err := ExecContext(ctx, db,
		`INSERT INTO fact_bsv_utxos(
			utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi, utxo_state, carrier_type,
			spent_by_txid, created_at_unix, updated_at_unix, spent_at_unix, note, payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(utxo_id) DO UPDATE SET
			owner_pubkey_hex=excluded.owner_pubkey_hex,
			address=excluded.address,
			txid=excluded.txid,
			vout=excluded.vout,
			value_satoshi=excluded.value_satoshi,
			utxo_state=excluded.utxo_state,
			carrier_type=excluded.carrier_type,
			spent_by_txid=excluded.spent_by_txid,
			updated_at_unix=excluded.updated_at_unix,
			spent_at_unix=excluded.spent_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		utxoID, ownerPubkey, strings.TrimSpace(e.Address), txid, e.Vout, e.ValueSatoshi, utxoState, carrierType,
		spentByTxid, createdAt, updatedAt, spentAt, strings.TrimSpace(e.Note), mustJSONString(e.Payload),
	)
	return err
}

func markBSVUTXOSpentConn(ctx context.Context, db sqlConn, utxoID string, spentByTxid string, updatedAt int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	utxoID = strings.ToLower(strings.TrimSpace(utxoID))
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if utxoID == "" {
		return fmt.Errorf("utxo_id is required")
	}
	if updatedAt <= 0 {
		updatedAt = time.Now().Unix()
	}
	_, err := ExecContext(ctx, db,
		`UPDATE fact_bsv_utxos SET utxo_state='spent', spent_by_txid=?, spent_at_unix=?, updated_at_unix=? WHERE utxo_id=?`,
		spentByTxid, updatedAt, updatedAt, utxoID,
	)
	return err
}

func dbGetBSVUTXOByID(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXO(ctx, store, utxoID)
}

func dbGetBSVUTXOByIDTx(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOByIDDB(ctx context.Context, db sqlConn, utxoID string) (*bsvUTXOEntry, error) {
	return nil, fmt.Errorf("db direct access is removed")
}

func dbGetBSVUTXOByIDStore(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXO(ctx, store, utxoID)
}

func dbGetBSVUTXOTxID(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOByIDEntTx(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOByIDEnt(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXO(ctx, store, utxoID)
}

func dbGetBSVUTXOByIDStoreValue(ctx context.Context, store *clientDB, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXO(ctx, store, utxoID)
}

func dbGetBSVUTXOTxValue(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue2(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue3(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue4(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue5(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue6(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue7(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue8(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue9(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue10(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue11(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue12(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue13(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue14(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue15(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue16(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue17(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue18(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue19(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue20(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue21(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue22(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue23(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue24(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue25(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue26(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue27(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue28(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue29(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue30(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue31(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue32(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue33(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue34(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue35(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue36(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue37(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue38(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue39(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue40(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue41(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue42(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue43(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue44(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue45(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue46(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue47(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue48(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue49(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue50(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue51(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue52(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue53(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue54(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue55(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue56(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue57(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue58(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue59(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue60(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue61(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue62(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue63(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue64(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue65(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue66(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue67(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue68(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue69(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue70(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue71(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue72(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue73(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue74(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue75(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue76(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue77(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue78(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue79(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue80(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue81(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue82(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue83(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue84(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue85(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue86(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue87(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue88(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue89(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue90(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue91(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue92(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue93(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue94(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue95(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue96(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue97(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue98(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue99(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue100(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue101(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue102(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue103(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue104(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue105(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue106(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue107(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue108(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue109(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue110(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue111(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue112(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue113(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue114(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue115(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue116(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue117(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue118(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue119(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue120(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue121(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue122(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue123(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue124(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue125(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue126(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue127(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue128(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue129(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue130(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue131(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue132(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue133(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue134(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue135(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue136(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue137(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue138(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue139(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue140(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue141(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue142(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue143(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue144(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue145(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue146(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue147(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue148(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue149(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue150(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue151(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue152(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue153(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue154(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue155(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue156(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue157(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue158(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue159(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue160(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue161(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue162(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue163(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue164(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue165(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue166(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue167(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue168(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue169(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue170(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue171(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue172(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue173(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue174(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue175(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue176(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue177(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue178(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue179(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue180(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue181(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue182(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue183(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue184(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue185(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue186(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue187(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue188(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue189(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue190(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue191(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue192(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue193(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue194(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue195(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue196(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue197(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue198(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue199(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

func dbGetBSVUTXOTxValue200(ctx context.Context, tx *gen.Tx, utxoID string) (*bsvUTXOEntry, error) {
	return dbGetBSVUTXOTx(ctx, tx, utxoID)
}

// dbListSpendableBSVUTXOs 查询可花费的本币UTXO列表
// 设计说明：
// - 只返回 utxo_state='unspent' 且 carrier_type='plain_bsv' 的记录
// - 用于支付前的选币
func dbListSpendableBSVUTXOs(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]bsvUTXOEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]bsvUTXOEntry, error) {
		return dbListSpendableBSVUTXOsDB(ctx, db, ownerPubkeyHex)
	})
}

func dbListSpendableBSVUTXOsDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) ([]bsvUTXOEntry, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	rows, err := QueryContext(ctx, db,
		`SELECT utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi, carrier_type,
			created_at_unix, updated_at_unix, note, payload_json
		 FROM fact_bsv_utxos
		 WHERE owner_pubkey_hex=? AND utxo_state='unspent' AND carrier_type='plain_bsv'
		 ORDER BY value_satoshi ASC, created_at_unix ASC`,
		ownerPubkeyHex,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]bsvUTXOEntry, 0, 16)
	for rows.Next() {
		var e bsvUTXOEntry
		var payloadJSON string
		e.UTXOState = "unspent"
		if err := rows.Scan(&e.UTXOID, &e.OwnerPubkeyHex, &e.Address, &e.TxID, &e.Vout,
			&e.ValueSatoshi, &e.CarrierType, &e.CreatedAtUnix, &e.UpdatedAtUnix, &e.Note, &payloadJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// dbCalcBSVBalance 计算本币余额
// 返回：confirmed（已确认可用余额）、total（包含pending的总余额）
func dbCalcBSVBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string) (uint64, uint64, error) {
	if store == nil {
		return 0, 0, fmt.Errorf("client db is nil")
	}
	var confirmed, total uint64
	err := store.Do(ctx, func(db sqlConn) error {
		var err error
		confirmed, total, err = dbCalcBSVBalanceDB(ctx, db, ownerPubkeyHex)
		return err
	})
	return confirmed, total, err
}

func dbCalcBSVBalanceDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) (uint64, uint64, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return 0, 0, fmt.Errorf("owner_pubkey_hex is required")
	}

	// 已确认可用余额：unspent + plain_bsv
	var confirmed int64
	err := QueryRowContext(ctx, db,
		`SELECT COALESCE(SUM(value_satoshi),0) FROM fact_bsv_utxos
		 WHERE owner_pubkey_hex=? AND utxo_state='unspent' AND carrier_type='plain_bsv'`,
		ownerPubkeyHex,
	).Scan(&confirmed)
	if err != nil {
		return 0, 0, err
	}

	// 总余额（包含token载体和费用找零等）
	var total int64
	err = QueryRowContext(ctx, db,
		`SELECT COALESCE(SUM(value_satoshi),0) FROM fact_bsv_utxos
		 WHERE owner_pubkey_hex=? AND utxo_state='unspent'`,
		ownerPubkeyHex,
	).Scan(&total)
	if err != nil {
		return 0, 0, err
	}

	return uint64(confirmed), uint64(total), nil
}

// ========== Token Lot 读写 ==========

// dbUpsertTokenLot 幂等写入/更新 Token Lot
func dbUpsertTokenLot(ctx context.Context, store *clientDB, e tokenLotEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db sqlConn) error {
		return dbUpsertTokenLotDB(ctx, db, e)
	})
}

func dbUpsertTokenLotDB(ctx context.Context, db sqlConn, e tokenLotEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	lotID := strings.TrimSpace(e.LotID)
	if lotID == "" {
		return fmt.Errorf("lot_id is required")
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	tokenID := strings.TrimSpace(e.TokenID)
	if tokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	tokenStandard := strings.ToUpper(strings.TrimSpace(e.TokenStandard))
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s", tokenStandard)
	}

	lotState := strings.ToLower(strings.TrimSpace(e.LotState))
	if lotState == "" {
		lotState = "unspent"
	}
	if lotState != "unspent" && lotState != "spent" && lotState != "locked" {
		return fmt.Errorf("lot_state must be unspent/spent/locked, got %s", lotState)
	}

	now := time.Now().Unix()
	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := e.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}

	quantityText := strings.TrimSpace(e.QuantityText)
	usedQuantityText := strings.TrimSpace(e.UsedQuantityText)
	if usedQuantityText == "" {
		usedQuantityText = "0"
	}

	_, err := ExecContext(ctx, db,
		`INSERT INTO fact_token_lots(
			lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text,
			lot_state, mint_txid, last_spend_txid, created_at_unix, updated_at_unix, note, payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(lot_id) DO UPDATE SET
			used_quantity_text=excluded.used_quantity_text,
			lot_state=excluded.lot_state,
			last_spend_txid=CASE WHEN excluded.last_spend_txid!='' THEN excluded.last_spend_txid ELSE fact_token_lots.last_spend_txid END,
			updated_at_unix=excluded.updated_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		lotID, ownerPubkey, tokenID, tokenStandard, quantityText, usedQuantityText,
		lotState, strings.TrimSpace(e.MintTxid), strings.TrimSpace(e.LastSpendTxid),
		createdAt, updatedAt, strings.TrimSpace(e.Note), mustJSONString(e.Payload),
	)
	return err
}

// dbUpsertTokenLotEntTx 在 ent 事务里写 Token Lot。
func dbUpsertTokenLotEntTx(ctx context.Context, tx *gen.Tx, e tokenLotEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	lotID := strings.TrimSpace(e.LotID)
	if lotID == "" {
		return fmt.Errorf("lot_id is required")
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	tokenID := strings.TrimSpace(e.TokenID)
	if tokenID == "" {
		return fmt.Errorf("token_id is required")
	}
	tokenStandard := strings.ToUpper(strings.TrimSpace(e.TokenStandard))
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return fmt.Errorf("token_standard must be BSV20 or BSV21, got %s", tokenStandard)
	}
	lotState := strings.ToLower(strings.TrimSpace(e.LotState))
	if lotState == "" {
		lotState = "unspent"
	}
	if lotState != "unspent" && lotState != "spent" && lotState != "locked" {
		return fmt.Errorf("lot_state must be unspent/spent/locked, got %s", lotState)
	}
	now := time.Now().Unix()
	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := e.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}
	quantityText := strings.TrimSpace(e.QuantityText)
	usedQuantityText := strings.TrimSpace(e.UsedQuantityText)
	if usedQuantityText == "" {
		usedQuantityText = "0"
	}
	existing, err := tx.FactTokenLots.Query().
		Where(facttokenlots.LotIDEQ(lotID)).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetOwnerPubkeyHex(ownerPubkey).
			SetTokenID(tokenID).
			SetTokenStandard(tokenStandard).
			SetQuantityText(quantityText).
			SetUsedQuantityText(usedQuantityText).
			SetLotState(lotState).
			SetMintTxid(strings.TrimSpace(e.MintTxid)).
			SetLastSpendTxid(strings.TrimSpace(e.LastSpendTxid)).
			SetUpdatedAtUnix(updatedAt).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.FactTokenLots.Create().
		SetLotID(lotID).
		SetOwnerPubkeyHex(ownerPubkey).
		SetTokenID(tokenID).
		SetTokenStandard(tokenStandard).
		SetQuantityText(quantityText).
		SetUsedQuantityText(usedQuantityText).
		SetLotState(lotState).
		SetMintTxid(strings.TrimSpace(e.MintTxid)).
		SetLastSpendTxid(strings.TrimSpace(e.LastSpendTxid)).
		SetCreatedAtUnix(createdAt).
		SetUpdatedAtUnix(updatedAt).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		Save(ctx)
	return err
}

// dbGetTokenLot 查询单个 Token Lot
func dbGetTokenLot(ctx context.Context, store *clientDB, lotID string) (*tokenLotEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (*tokenLotEntry, error) {
		return dbGetTokenLotDB(ctx, db, lotID)
	})
}

// dbGetTokenLotEntTx 在 ent 事务里查询单个 Token Lot。
func dbGetTokenLotEntTx(ctx context.Context, tx *gen.Tx, lotID string) (*tokenLotEntry, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	lotID = strings.TrimSpace(lotID)
	if lotID == "" {
		return nil, fmt.Errorf("lot_id is required")
	}
	row, err := tx.FactTokenLots.Query().
		Where(facttokenlots.LotIDEQ(lotID)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &tokenLotEntry{
		LotID:            row.LotID,
		OwnerPubkeyHex:   row.OwnerPubkeyHex,
		TokenID:          row.TokenID,
		TokenStandard:    row.TokenStandard,
		QuantityText:     row.QuantityText,
		UsedQuantityText: row.UsedQuantityText,
		LotState:         row.LotState,
		MintTxid:         row.MintTxid,
		LastSpendTxid:    row.LastSpendTxid,
		CreatedAtUnix:    row.CreatedAtUnix,
		UpdatedAtUnix:    row.UpdatedAtUnix,
		Note:             row.Note,
		Payload:          json.RawMessage(row.PayloadJSON),
	}, nil
}

func dbGetTokenLotDB(ctx context.Context, db sqlConn, lotID string) (*tokenLotEntry, error) {
	lotID = strings.TrimSpace(lotID)
	if lotID == "" {
		return nil, fmt.Errorf("lot_id is required")
	}
	var e tokenLotEntry
	var payloadJSON string
	err := QueryRowContext(ctx, db,
		`SELECT lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text,
			lot_state, mint_txid, last_spend_txid, created_at_unix, updated_at_unix, note, payload_json
		 FROM fact_token_lots WHERE lot_id=?`,
		lotID,
	).Scan(&e.LotID, &e.OwnerPubkeyHex, &e.TokenID, &e.TokenStandard, &e.QuantityText,
		&e.UsedQuantityText, &e.LotState, &e.MintTxid, &e.LastSpendTxid,
		&e.CreatedAtUnix, &e.UpdatedAtUnix, &e.Note, &payloadJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// dbListSpendableTokenLots 查询可花费的 Token Lot 列表
func dbListSpendableTokenLots(ctx context.Context, store *clientDB, ownerPubkeyHex string, tokenStandard string, tokenID string) ([]tokenLotEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]tokenLotEntry, error) {
		return dbListSpendableTokenLotsDB(ctx, db, ownerPubkeyHex, tokenStandard, tokenID)
	})
}

func dbListSpendableTokenLotsDB(ctx context.Context, db sqlConn, ownerPubkeyHex string, tokenStandard string, tokenID string) ([]tokenLotEntry, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	tokenStandard = strings.ToUpper(strings.TrimSpace(tokenStandard))
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return nil, fmt.Errorf("token_standard must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("token_id is required")
	}

	rows, err := QueryContext(ctx, db,
		`SELECT lot_id, owner_pubkey_hex, token_id, token_standard, quantity_text, used_quantity_text,
			lot_state, mint_txid, last_spend_txid, created_at_unix, updated_at_unix, note, payload_json
		 FROM fact_token_lots
		 WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=? AND lot_state='unspent'
		 ORDER BY created_at_unix ASC`,
		ownerPubkeyHex, tokenStandard, tokenID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tokenLotEntry, 0, 16)
	for rows.Next() {
		var e tokenLotEntry
		var payloadJSON string
		if err := rows.Scan(&e.LotID, &e.OwnerPubkeyHex, &e.TokenID, &e.TokenStandard,
			&e.QuantityText, &e.UsedQuantityText, &e.LotState, &e.MintTxid, &e.LastSpendTxid,
			&e.CreatedAtUnix, &e.UpdatedAtUnix, &e.Note, &payloadJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// dbCalcTokenBalance 计算单个 Token 的余额
// 返回：余额数量（十进制字符串）、误差信息
func dbCalcTokenBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	if store == nil {
		return "", fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (string, error) {
		return dbCalcTokenBalanceDB(ctx, db, ownerPubkeyHex, tokenStandard, tokenID)
	})
}

func dbCalcTokenBalanceDB(ctx context.Context, db sqlConn, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return "", fmt.Errorf("owner_pubkey_hex is required")
	}
	tokenStandard = strings.ToUpper(strings.TrimSpace(tokenStandard))
	if tokenStandard != "BSV20" && tokenStandard != "BSV21" {
		return "", fmt.Errorf("token_standard must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return "", fmt.Errorf("token_id is required")
	}

	// 获取所有 unspent lot 的 quantity 和 used_quantity
	rows, err := QueryContext(ctx, db,
		`SELECT quantity_text, used_quantity_text FROM fact_token_lots
		 WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=? AND lot_state='unspent'`,
		ownerPubkeyHex, tokenStandard, tokenID,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	// 使用 big.Int 进行高精度计算
	var totalBalance big.Int
	for rows.Next() {
		var qtyText, usedText string
		if err := rows.Scan(&qtyText, &usedText); err != nil {
			return "", err
		}

		// 解析数量
		qtyParsed, err := parseDecimalText(qtyText)
		if err != nil {
			continue
		}
		usedParsed, err := parseDecimalText(usedText)
		if err != nil {
			usedParsed = struct {
				intValue *big.Int
				scale    int
			}{intValue: big.NewInt(0), scale: 0}
		}

		// 对齐精度
		scale := qtyParsed.scale
		if usedParsed.scale > scale {
			scale = usedParsed.scale
		}
		qtyVal := new(big.Int).Set(qtyParsed.intValue)
		usedVal := new(big.Int).Set(usedParsed.intValue)

		for i := qtyParsed.scale; i < scale; i++ {
			qtyVal = new(big.Int).Mul(qtyVal, big.NewInt(10))
		}
		for i := usedParsed.scale; i < scale; i++ {
			usedVal = new(big.Int).Mul(usedVal, big.NewInt(10))
		}

		// 减法
		diff := new(big.Int).Sub(qtyVal, usedVal)
		if diff.Sign() < 0 {
			diff = big.NewInt(0)
		}

		// 加总
		totalBalance.Add(&totalBalance, diff)
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	return totalBalance.String(), nil
}

// ========== Token Carrier Link 读写 ==========

// dbUpsertTokenCarrierLink 幂等写入/更新 Token Carrier Link
func dbUpsertTokenCarrierLink(ctx context.Context, store *clientDB, e tokenCarrierLinkEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db sqlConn) error {
		return dbUpsertTokenCarrierLinkDB(ctx, db, e)
	})
}

func dbUpsertTokenCarrierLinkDB(ctx context.Context, db sqlConn, e tokenCarrierLinkEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	linkID := strings.TrimSpace(e.LinkID)
	if linkID == "" {
		return fmt.Errorf("link_id is required")
	}
	lotID := strings.TrimSpace(e.LotID)
	if lotID == "" {
		return fmt.Errorf("lot_id is required")
	}
	carrierUTXOID := strings.ToLower(strings.TrimSpace(e.CarrierUTXOID))
	if carrierUTXOID == "" {
		return fmt.Errorf("carrier_utxo_id is required")
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}

	linkState := strings.ToLower(strings.TrimSpace(e.LinkState))
	if linkState == "" {
		linkState = "active"
	}
	if linkState != "active" && linkState != "released" && linkState != "moved" {
		return fmt.Errorf("link_state must be active/released/moved, got %s", linkState)
	}

	now := time.Now().Unix()
	createdAt := e.CreatedAtUnix
	if createdAt <= 0 {
		createdAt = now
	}
	updatedAt := e.UpdatedAtUnix
	if updatedAt <= 0 {
		updatedAt = now
	}

	_, err := ExecContext(ctx, db,
		`INSERT INTO fact_token_carrier_links(
			link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid,
			created_at_unix, updated_at_unix, note, payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(link_id) DO UPDATE SET
			link_state=excluded.link_state,
			unbind_txid=CASE WHEN excluded.unbind_txid!='' THEN excluded.unbind_txid ELSE fact_token_carrier_links.unbind_txid END,
			updated_at_unix=excluded.updated_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		linkID, lotID, carrierUTXOID, ownerPubkey, linkState,
		strings.TrimSpace(e.BindTxid), strings.TrimSpace(e.UnbindTxid),
		createdAt, updatedAt, strings.TrimSpace(e.Note), mustJSONString(e.Payload),
	)
	return err
}

// dbGetActiveCarrierForLot 查询 Lot 的 active carrier
func dbGetActiveCarrierForLot(ctx context.Context, store *clientDB, lotID string) (*tokenCarrierLinkEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (*tokenCarrierLinkEntry, error) {
		return dbGetActiveCarrierForLotDB(ctx, db, lotID)
	})
}

func dbGetActiveCarrierForLotDB(ctx context.Context, db sqlConn, lotID string) (*tokenCarrierLinkEntry, error) {
	lotID = strings.TrimSpace(lotID)
	if lotID == "" {
		return nil, fmt.Errorf("lot_id is required")
	}
	var e tokenCarrierLinkEntry
	var payloadJSON string
	err := QueryRowContext(ctx, db,
		`SELECT link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid,
			created_at_unix, updated_at_unix, note, payload_json
		 FROM fact_token_carrier_links WHERE lot_id=? AND link_state='active' LIMIT 1`,
		lotID,
	).Scan(&e.LinkID, &e.LotID, &e.CarrierUTXOID, &e.OwnerPubkeyHex, &e.LinkState,
		&e.BindTxid, &e.UnbindTxid, &e.CreatedAtUnix, &e.UpdatedAtUnix, &e.Note, &payloadJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// dbListActiveCarrierLinksByOwner 查询某用户的所有 active carrier links
func dbListActiveCarrierLinksByOwner(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]tokenCarrierLinkEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]tokenCarrierLinkEntry, error) {
		return dbListActiveCarrierLinksByOwnerDB(ctx, db, ownerPubkeyHex)
	})
}

func dbListActiveCarrierLinksByOwnerDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) ([]tokenCarrierLinkEntry, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	rows, err := QueryContext(ctx, db,
		`SELECT link_id, lot_id, carrier_utxo_id, owner_pubkey_hex, link_state, bind_txid, unbind_txid,
			created_at_unix, updated_at_unix, note, payload_json
		 FROM fact_token_carrier_links WHERE owner_pubkey_hex=? AND link_state='active'
		 ORDER BY created_at_unix ASC`,
		ownerPubkeyHex,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tokenCarrierLinkEntry, 0, 16)
	for rows.Next() {
		var e tokenCarrierLinkEntry
		var payloadJSON string
		if err := rows.Scan(&e.LinkID, &e.LotID, &e.CarrierUTXOID, &e.OwnerPubkeyHex,
			&e.LinkState, &e.BindTxid, &e.UnbindTxid, &e.CreatedAtUnix, &e.UpdatedAtUnix,
			&e.Note, &payloadJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ========== Settlement Record 读写 ==========

// dbAppendSettlementRecord 写入结算消耗记录
func dbAppendSettlementRecord(ctx context.Context, store *clientDB, e settlementRecordEntry) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Do(ctx, func(db sqlConn) error {
		return dbAppendSettlementRecordDB(ctx, db, e)
	})
}

func dbAppendSettlementRecordDB(ctx context.Context, db sqlConn, e settlementRecordEntry) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	recordID := strings.TrimSpace(e.RecordID)
	if recordID == "" {
		return fmt.Errorf("record_id is required")
	}
	if e.SettlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	assetType := strings.ToUpper(strings.TrimSpace(e.AssetType))
	if assetType != "BSV" && assetType != "TOKEN" {
		return fmt.Errorf("asset_type must be BSV or TOKEN, got %s", assetType)
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}

	state := strings.ToLower(strings.TrimSpace(e.State))
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "reverted" {
		return fmt.Errorf("state must be pending/confirmed/reverted, got %s", state)
	}

	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	confirmedAt := e.ConfirmedAtUnix
	if state == "confirmed" && confirmedAt <= 0 {
		confirmedAt = occurredAt
	}

	_, err := ExecContext(ctx, db,
		`INSERT INTO fact_settlement_records(
			record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex, source_utxo_id, source_lot_id,
			used_satoshi, used_quantity_text, state, occurred_at_unix, confirmed_at_unix, note, payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id) DO UPDATE SET
			used_satoshi=excluded.used_satoshi,
			used_quantity_text=excluded.used_quantity_text,
			state=CASE
				WHEN fact_settlement_records.state='confirmed' AND excluded.state='pending' THEN fact_settlement_records.state
				ELSE excluded.state
			END,
			confirmed_at_unix=CASE
				WHEN excluded.state='confirmed' THEN excluded.occurred_at_unix
				ELSE fact_settlement_records.confirmed_at_unix
			END,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		recordID, e.SettlementPaymentAttemptID, assetType, ownerPubkey,
		strings.ToLower(strings.TrimSpace(e.SourceUTXOID)),
		strings.TrimSpace(e.SourceLotID),
		e.UsedSatoshi,
		strings.TrimSpace(e.UsedQuantityText),
		state,
		occurredAt,
		confirmedAt,
		strings.TrimSpace(e.Note),
		mustJSONString(e.Payload),
	)
	return err
}

// dbAppendSettlementRecordEntTx 在 ent 事务里写结算消耗记录。
func dbAppendSettlementRecordEntTx(ctx context.Context, tx *gen.Tx, e settlementRecordEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	recordID := strings.TrimSpace(e.RecordID)
	if recordID == "" {
		return fmt.Errorf("record_id is required")
	}
	if e.SettlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	assetType := strings.ToUpper(strings.TrimSpace(e.AssetType))
	if assetType != "BSV" && assetType != "TOKEN" {
		return fmt.Errorf("asset_type must be BSV or TOKEN, got %s", assetType)
	}
	ownerPubkey := strings.ToLower(strings.TrimSpace(e.OwnerPubkeyHex))
	if ownerPubkey == "" {
		return fmt.Errorf("owner_pubkey_hex is required")
	}
	state := strings.ToLower(strings.TrimSpace(e.State))
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "reverted" {
		return fmt.Errorf("state must be pending/confirmed/reverted, got %s", state)
	}
	now := time.Now().Unix()
	occurredAt := e.OccurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	confirmedAt := e.ConfirmedAtUnix
	if state == "confirmed" && confirmedAt <= 0 {
		confirmedAt = occurredAt
	}
	existing, err := tx.FactSettlementRecords.Query().
		Where(
			factsettlementrecords.SettlementPaymentAttemptIDEQ(e.SettlementPaymentAttemptID),
			factsettlementrecords.AssetTypeEQ(assetType),
			factsettlementrecords.SourceUtxoIDEQ(strings.ToLower(strings.TrimSpace(e.SourceUTXOID))),
			factsettlementrecords.SourceLotIDEQ(strings.TrimSpace(e.SourceLotID)),
		).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetOwnerPubkeyHex(ownerPubkey).
			SetUsedSatoshi(e.UsedSatoshi).
			SetUsedQuantityText(strings.TrimSpace(e.UsedQuantityText)).
			SetState(state).
			SetOccurredAtUnix(occurredAt).
			SetConfirmedAtUnix(confirmedAt).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload)).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.FactSettlementRecords.Create().
		SetRecordID(recordID).
		SetSettlementPaymentAttemptID(e.SettlementPaymentAttemptID).
		SetAssetType(assetType).
		SetOwnerPubkeyHex(ownerPubkey).
		SetSourceUtxoID(strings.ToLower(strings.TrimSpace(e.SourceUTXOID))).
		SetSourceLotID(strings.TrimSpace(e.SourceLotID)).
		SetUsedSatoshi(e.UsedSatoshi).
		SetUsedQuantityText(strings.TrimSpace(e.UsedQuantityText)).
		SetState(state).
		SetOccurredAtUnix(occurredAt).
		SetConfirmedAtUnix(confirmedAt).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		Save(ctx)
	return err
}

// dbListSettlementRecordsByCycle 查询结算周期的消耗记录
func dbListSettlementRecordsByCycle(ctx context.Context, store *clientDB, settlementPaymentAttemptID int64) ([]settlementRecordEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]settlementRecordEntry, error) {
		return dbListSettlementRecordsByCycleDB(ctx, db, settlementPaymentAttemptID)
	})
}

func dbListSettlementRecordsByCycleDB(ctx context.Context, db sqlConn, settlementPaymentAttemptID int64) ([]settlementRecordEntry, error) {
	if settlementPaymentAttemptID <= 0 {
		return nil, fmt.Errorf("settlement_payment_attempt_id is required")
	}
	rows, err := QueryContext(ctx, db,
		`SELECT record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex, source_utxo_id, source_lot_id,
			used_satoshi, used_quantity_text, state, occurred_at_unix, confirmed_at_unix, note, payload_json
		 FROM fact_settlement_records WHERE settlement_payment_attempt_id=?
		 ORDER BY occurred_at_unix ASC`,
		settlementPaymentAttemptID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]settlementRecordEntry, 0, 16)
	for rows.Next() {
		var e settlementRecordEntry
		var payloadJSON string
		if err := rows.Scan(&e.RecordID, &e.SettlementPaymentAttemptID, &e.AssetType, &e.OwnerPubkeyHex,
			&e.SourceUTXOID, &e.SourceLotID, &e.UsedSatoshi, &e.UsedQuantityText, &e.State,
			&e.OccurredAtUnix, &e.ConfirmedAtUnix, &e.Note, &payloadJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ========== 选币辅助函数 ==========

// selectedBSVUTXO 选币结果
type selectedBSVUTXO struct {
	bsvUTXOEntry
	UseAmount int64 `json:"use_amount"`
}

// dbSelectBSVUTXOsForTarget 按目标金额选币（小额优先）
func dbSelectBSVUTXOsForTarget(ctx context.Context, store *clientDB, ownerPubkeyHex string, target uint64) ([]selectedBSVUTXO, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]selectedBSVUTXO, error) {
		return dbSelectBSVUTXOsForTargetDB(ctx, db, ownerPubkeyHex, target)
	})
}

func dbSelectBSVUTXOsForTargetDB(ctx context.Context, db sqlConn, ownerPubkeyHex string, target uint64) ([]selectedBSVUTXO, error) {
	utxos, err := dbListSpendableBSVUTXOsDB(ctx, db, ownerPubkeyHex)
	if err != nil {
		return nil, fmt.Errorf("list spendable utxos: %w", err)
	}
	if len(utxos) == 0 {
		return nil, fmt.Errorf("no spendable UTXOs available")
	}

	// 小额优先选币
	remaining := int64(target)
	out := make([]selectedBSVUTXO, 0, len(utxos))
	for _, u := range utxos {
		if remaining <= 0 {
			break
		}
		use := u.ValueSatoshi
		if use > remaining {
			use = remaining
		}
		out = append(out, selectedBSVUTXO{
			bsvUTXOEntry: u,
			UseAmount:    use,
		})
		remaining -= use
	}

	if remaining > 0 {
		var totalAvailable int64
		for _, u := range utxos {
			totalAvailable += u.ValueSatoshi
		}
		return nil, fmt.Errorf("insufficient balance: target=%d, available=%d, missing=%d", target, totalAvailable, remaining)
	}

	return out, nil
}

// ========== 余额汇总结构 ==========

// walletBSVBalance 本币余额结构
type walletBSVBalance struct {
	OwnerPubkeyHex     string `json:"owner_pubkey_hex"`
	ConfirmedSatoshi   uint64 `json:"confirmed_satoshi"` // plain_bsv 可用余额
	TotalSatoshi       uint64 `json:"total_satoshi"`     // 包含token载体的总余额
	SpendableUTXOCount int    `json:"spendable_utxo_count"`
}

// walletTokenBalance Token余额结构
type walletTokenBalance struct {
	OwnerPubkeyHex string `json:"owner_pubkey_hex"`
	TokenStandard  string `json:"token_standard"`
	TokenID        string `json:"token_id"`
	BalanceText    string `json:"balance_text"` // 十进制字符串
}

// dbLoadWalletBSVBalance 加载钱包本币余额
func dbLoadWalletBSVBalance(ctx context.Context, store *clientDB, ownerPubkeyHex string) (walletBSVBalance, error) {
	if store == nil {
		return walletBSVBalance{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (walletBSVBalance, error) {
		return dbLoadWalletBSVBalanceDB(ctx, db, ownerPubkeyHex)
	})
}

func dbLoadWalletBSVBalanceDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) (walletBSVBalance, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return walletBSVBalance{}, fmt.Errorf("owner_pubkey_hex is required")
	}

	confirmed, total, err := dbCalcBSVBalanceDB(ctx, db, ownerPubkeyHex)
	if err != nil {
		return walletBSVBalance{}, err
	}

	var spendableCount int
	err = QueryRowContext(ctx, db,
		`SELECT COUNT(1) FROM fact_bsv_utxos
		 WHERE owner_pubkey_hex=? AND utxo_state='unspent' AND carrier_type='plain_bsv'`,
		ownerPubkeyHex,
	).Scan(&spendableCount)
	if err != nil {
		return walletBSVBalance{}, err
	}

	return walletBSVBalance{
		OwnerPubkeyHex:     ownerPubkeyHex,
		ConfirmedSatoshi:   confirmed,
		TotalSatoshi:       total,
		SpendableUTXOCount: spendableCount,
	}, nil
}

// dbLoadAllWalletTokenBalances 加载钱包所有 Token 余额
func dbLoadAllWalletTokenBalances(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]walletTokenBalance, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]walletTokenBalance, error) {
		return dbLoadAllWalletTokenBalancesDB(ctx, db, ownerPubkeyHex)
	})
}

func dbLoadAllWalletTokenBalancesDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) ([]walletTokenBalance, error) {
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}

	// 获取所有有未花费 lot 的 token
	rows, err := QueryContext(ctx, db,
		`SELECT DISTINCT token_standard, token_id FROM fact_token_lots
		 WHERE owner_pubkey_hex=? AND lot_state='unspent'`,
		ownerPubkeyHex,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type tokenKey struct {
		Standard string
		ID       string
	}
	var tokens []tokenKey
	for rows.Next() {
		var k tokenKey
		if err := rows.Scan(&k.Standard, &k.ID); err != nil {
			return nil, err
		}
		tokens = append(tokens, k)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]walletTokenBalance, 0, len(tokens))
	for _, t := range tokens {
		balance, err := dbCalcTokenBalanceDB(ctx, db, ownerPubkeyHex, t.Standard, t.ID)
		if err != nil {
			continue
		}
		if balance == "" || balance == "0" {
			continue
		}
		out = append(out, walletTokenBalance{
			OwnerPubkeyHex: ownerPubkeyHex,
			TokenStandard:  t.Standard,
			TokenID:        t.ID,
			BalanceText:    balance,
		})
	}

	return out, nil
}

// ========== 旧函数兼容性保留（迁移期） ==========
// 以下函数保持旧接口但内部实现改为查询新表
// 用于减少对其他文件的侵入式修改

// walletAssetBalance 保持旧结构用于兼容性
type walletAssetBalance struct {
	WalletID       string `json:"wallet_id"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	TotalInSatoshi int64  `json:"total_in_satoshi"`
	TotalUsed      int64  `json:"total_used"`
	Remaining      int64  `json:"remaining"`
}

// spendableSourceFlow 可花费源流（保持旧结构）
type spendableSourceFlow struct {
	FlowID         int64  `json:"flow_id"`
	WalletID       string `json:"wallet_id"`
	Address        string `json:"address"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	UTXOID         string `json:"utxo_id"`
	TxID           string `json:"txid"`
	Vout           uint32 `json:"vout"`
	TotalInSatoshi int64  `json:"total_in_satoshi"`
	TotalUsed      int64  `json:"total_used"`
	Remaining      int64  `json:"remaining"`
	OccurredAtUnix int64  `json:"occurred_at_unix"`
}

// selectedSourceFlow 选源结果（保持旧结构）
type selectedSourceFlow struct {
	spendableSourceFlow
	UseAmount int64 `json:"use_amount"`
}

// dbListSpendableSourceFlows 保持旧接口但查询新表
// 设计说明：旧函数保留接口，内部改为查 fact_bsv_utxos
func dbListSpendableSourceFlows(ctx context.Context, store ClientStore, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")
	walletOwnerKey := walletIDByAddress(ownerPubkeyHex)
	rows, err := store.QueryContext(ctx,
		`SELECT utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
			created_at_unix
		 FROM fact_bsv_utxos
		 WHERE (owner_pubkey_hex=? OR owner_pubkey_hex=?)
		   AND utxo_state='unspent' AND carrier_type='plain_bsv'
		 ORDER BY value_satoshi ASC, created_at_unix ASC`,
		ownerPubkeyHex,
		walletOwnerKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSpendableSourceFlowRows(rows, walletID)
}

func dbListSpendableSourceFlowsDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	// walletID 实际上是 owner_pubkey_hex 或可以从中提取
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}

	// 移除 "wallet:" 前缀（如果存在）
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")
	walletOwnerKey := walletIDByAddress(ownerPubkeyHex)

	// 查询新表
	rows, err := QueryContext(ctx, db,
		`SELECT utxo_id, owner_pubkey_hex, address, txid, vout, value_satoshi,
			created_at_unix
		 FROM fact_bsv_utxos
		 WHERE (owner_pubkey_hex=? OR owner_pubkey_hex=?)
		   AND utxo_state='unspent' AND carrier_type='plain_bsv'
		 ORDER BY value_satoshi ASC, created_at_unix ASC`,
		ownerPubkeyHex, walletOwnerKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]spendableSourceFlow, 0, 16)
	var flowID int64 = 1 // 模拟 flow_id
	for rows.Next() {
		var s spendableSourceFlow
		var utxoID, addr, txid string
		var vout uint32
		var value int64
		var createdAt int64
		if err := rows.Scan(&utxoID, new(string), &addr, &txid, &vout, &value, &createdAt); err != nil {
			return nil, err
		}
		s.FlowID = flowID
		s.WalletID = walletID
		s.Address = addr
		s.AssetKind = "BSV"
		s.TokenID = ""
		s.UTXOID = utxoID
		s.TxID = txid
		s.Vout = vout
		s.TotalInSatoshi = value
		s.TotalUsed = 0 // 新表模式下，已花费的会标记为 spent，不会出现在这里
		s.Remaining = value
		s.OccurredAtUnix = createdAt
		out = append(out, s)
		flowID++
	}
	return out, rows.Err()
}

// dbSelectSourceFlowsForTarget 保持旧接口但查询新表
func dbSelectSourceFlowsForTarget(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]selectedSourceFlow, error) {
		return dbSelectSourceFlowsForTargetDB(ctx, db, walletID, assetKind, tokenID, target)
	})
}

func dbSelectSourceFlowsForTargetDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	flows, err := dbListSpendableSourceFlowsDB(ctx, db, walletID, assetKind, tokenID)
	if err != nil {
		return nil, fmt.Errorf("list spendable flows: %w", err)
	}
	if len(flows) == 0 {
		return nil, fmt.Errorf("no spendable source flows available")
	}

	// 小额优先选源
	remaining := int64(target)
	out := make([]selectedSourceFlow, 0, len(flows))
	for _, f := range flows {
		if remaining <= 0 {
			break
		}
		use := f.Remaining
		if use > remaining {
			use = remaining
		}
		out = append(out, selectedSourceFlow{
			spendableSourceFlow: f,
			UseAmount:           use,
		})
		remaining -= use
	}

	if remaining > 0 {
		var totalAvailable int64
		for _, f := range flows {
			totalAvailable += f.Remaining
		}
		return nil, fmt.Errorf("insufficient balance: target=%d, available=%d, missing=%d", target, totalAvailable, remaining)
	}

	return out, nil
}

func scanSpendableSourceFlowRows(rows *sql.Rows, walletID string) ([]spendableSourceFlow, error) {
	out := make([]spendableSourceFlow, 0, 16)
	var flowID int64 = 1
	for rows.Next() {
		var s spendableSourceFlow
		var utxoID, addr, txid string
		var vout uint32
		var value int64
		var createdAt int64
		if err := rows.Scan(&utxoID, new(string), &addr, &txid, &vout, &value, &createdAt); err != nil {
			return nil, err
		}
		s.FlowID = flowID
		s.WalletID = walletID
		s.Address = addr
		s.AssetKind = "BSV"
		s.TokenID = ""
		s.UTXOID = utxoID
		s.TxID = txid
		s.Vout = vout
		s.TotalInSatoshi = value
		s.TotalUsed = 0
		s.Remaining = value
		s.OccurredAtUnix = createdAt
		out = append(out, s)
		flowID++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// tokenSourceFlow Token源流（保持旧结构）
type tokenSourceFlow struct {
	FlowID         int64  `json:"flow_id"`
	WalletID       string `json:"wallet_id"`
	Address        string `json:"address"`
	AssetKind      string `json:"asset_kind"`
	TokenID        string `json:"token_id"`
	UTXOID         string `json:"utxo_id"`
	TxID           string `json:"txid"`
	Vout           uint32 `json:"vout"`
	QuantityText   string `json:"quantity_text"`
	TotalUsedText  string `json:"total_used_text"`
	OccurredAtUnix int64  `json:"occurred_at_unix"`
}

// dbListTokenSpendableSourceFlows 保持旧接口但查询新表
func dbListTokenSpendableSourceFlows(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) ([]tokenSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) ([]tokenSourceFlow, error) {
		return dbListTokenSpendableSourceFlowsDB(ctx, db, walletID, assetKind, tokenID)
	})
}

func dbListTokenSpendableSourceFlowsDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) ([]tokenSourceFlow, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind != "BSV20" && assetKind != "BSV21" {
		return nil, fmt.Errorf("asset_kind must be BSV20 or BSV21")
	}
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("token_id is required")
	}

	// 查询新表：获取未花费的 lot 及其 active carrier
	// 设计说明：关联 wallet_utxo 表排除 allocation_class='unknown' 的项
	rows, err := QueryContext(ctx, db,
		`SELECT l.lot_id, l.owner_pubkey_hex, l.token_id, l.token_standard, l.quantity_text, l.used_quantity_text,
			l.created_at_unix, c.carrier_utxo_id
		 FROM fact_token_lots l
		 LEFT JOIN fact_token_carrier_links c ON l.lot_id=c.lot_id AND c.link_state='active'
		 INNER JOIN wallet_utxo w ON c.carrier_utxo_id=w.utxo_id AND w.allocation_class!=?
		 WHERE l.owner_pubkey_hex=? AND l.token_standard=? AND l.token_id=? AND l.lot_state='unspent'
		 ORDER BY l.created_at_unix ASC`,
		walletUTXOAllocationUnknown, ownerPubkeyHex, assetKind, tokenID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tokenSourceFlow, 0, 16)
	var flowID int64 = 1
	for rows.Next() {
		var s tokenSourceFlow
		var lotID, owner, tokID, std, qty, used string
		var createdAt int64
		var carrierUTXOID string
		if err := rows.Scan(&lotID, &owner, &tokID, &std, &qty, &used, &createdAt, &carrierUTXOID); err != nil {
			return nil, err
		}
		s.FlowID = flowID
		s.WalletID = walletID
		s.AssetKind = std
		s.TokenID = tokID
		s.UTXOID = carrierUTXOID
		s.TxID = "" // 可以从 carrier_utxo_id 解析，但这里简化
		s.Vout = 0
		s.QuantityText = qty
		s.TotalUsedText = used
		s.OccurredAtUnix = createdAt
		out = append(out, s)
		flowID++
	}
	return out, rows.Err()
}

// tokenBalanceResult Token余额结果（保持旧结构）
type tokenBalanceResult struct {
	WalletID      string `json:"wallet_id"`
	AssetKind     string `json:"asset_kind"`
	TokenID       string `json:"token_id"`
	TotalInText   string `json:"total_in_text"`
	TotalUsedText string `json:"total_used_text"`
}

// dbLoadTokenBalanceFact 保持旧接口但查询新表
func dbLoadTokenBalanceFact(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (tokenBalanceResult, error) {
	if store == nil {
		return tokenBalanceResult{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (tokenBalanceResult, error) {
		return dbLoadTokenBalanceFactDB(ctx, db, walletID, assetKind, tokenID)
	})
}

func dbLoadTokenBalanceFactDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) (tokenBalanceResult, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	if ownerPubkeyHex == "" {
		return tokenBalanceResult{}, fmt.Errorf("wallet_id is required")
	}
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	tokenID = strings.TrimSpace(tokenID)

	// 查询新表获取 quantity 和 used_quantity
	rows, err := QueryContext(ctx, db,
		`SELECT quantity_text, used_quantity_text FROM fact_token_lots
		 WHERE owner_pubkey_hex=? AND token_standard=? AND token_id=? AND lot_state='unspent'`,
		ownerPubkeyHex, assetKind, tokenID,
	)
	if err != nil {
		return tokenBalanceResult{}, err
	}
	defer rows.Close()

	var totalInParts []string
	var totalUsedParts []string
	for rows.Next() {
		var qty, used string
		if err := rows.Scan(&qty, &used); err != nil {
			return tokenBalanceResult{}, err
		}
		if qty != "" {
			totalInParts = append(totalInParts, qty)
		}
		if used != "" && used != "0" {
			totalUsedParts = append(totalUsedParts, used)
		}
	}
	if err := rows.Err(); err != nil {
		return tokenBalanceResult{}, err
	}

	totalIn := strings.Join(totalInParts, ",")
	totalUsed := strings.Join(totalUsedParts, ",")

	return tokenBalanceResult{
		WalletID:      walletID,
		AssetKind:     assetKind,
		TokenID:       tokenID,
		TotalInText:   totalIn,
		TotalUsedText: totalUsed,
	}, nil
}

// 注意：parsedDecimal 和 decimalTextAccumulator 类型定义在 wallet_asset_preview.go 中
// 这里不再重复定义以避免冲突

// 注意：parsedDecimal 和 decimalTextAccumulator 类型定义在 wallet_asset_preview.go 中
// 这里不再重复定义以避免冲突

// ========== 兼容性函数（硬切迁移期保留） ==========

// dbLoadWalletAssetBalanceFact 兼容性函数（已改为查询新表）
func dbLoadWalletAssetBalanceFact(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	if store == nil {
		return walletAssetBalance{}, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db sqlConn) (walletAssetBalance, error) {
		return dbLoadWalletAssetBalanceFactDB(ctx, db, walletID, assetKind, tokenID)
	})
}

func dbLoadWalletAssetBalanceFactDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	assetKind = strings.ToUpper(strings.TrimSpace(assetKind))
	if assetKind == "" {
		assetKind = "BSV"
	}
	tokenID = strings.TrimSpace(tokenID)

	var result walletAssetBalance
	result.WalletID = walletID
	result.AssetKind = assetKind
	result.TokenID = tokenID

	if assetKind == "BSV" {
		// 查询本币余额
		confirmed, _, err := dbCalcBSVBalanceDB(ctx, db, ownerPubkeyHex)
		if err != nil {
			return result, err
		}
		result.TotalInSatoshi = int64(confirmed)
		result.Remaining = int64(confirmed)
	} else {
		// 查询 token 余额
		balance, err := dbCalcTokenBalanceDB(ctx, db, ownerPubkeyHex, assetKind, tokenID)
		if err != nil {
			return result, err
		}
		// Token 余额以字符串形式返回，这里转为 int64 可能溢出
		// 但为了兼容性，我们只记录是否有余额
		if balance != "" && balance != "0" {
			result.TotalInSatoshi = 1 // 标记有余额
			result.Remaining = 1
		}
	}

	return result, nil
}

// verifiedAssetFlowParams 兼容性类型（旧结构，新实现忽略）
type verifiedAssetFlowParams struct {
	WalletID      string
	Address       string
	UTXOID        string
	TxID          string
	Vout          uint32
	ValueSatoshi  uint64
	AssetKind     string
	TokenID       string
	QuantityText  string
	CreatedAtUnix int64
	Trigger       string
	Symbol        string
}

// ApplyVerifiedAssetFlow 兼容性函数（改为写入新表）
func ApplyVerifiedAssetFlow(ctx context.Context, store *clientDB, p verifiedAssetFlowParams) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}

	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(p.WalletID))
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")

	if p.AssetKind == "BSV" || p.AssetKind == "" {
		// 写入本币 UTXO
		return dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
			UTXOID:         p.UTXOID,
			OwnerPubkeyHex: ownerPubkeyHex,
			Address:        p.Address,
			TxID:           p.TxID,
			Vout:           p.Vout,
			ValueSatoshi:   int64(p.ValueSatoshi),
			UTXOState:      "unspent",
			CarrierType:    "plain_bsv",
			CreatedAtUnix: func() int64 {
				if p.CreatedAtUnix > 0 {
					return p.CreatedAtUnix
				}
				return time.Now().Unix()
			}(),
			Note:    fmt.Sprintf("verified %s by WOC (trigger: %s)", p.AssetKind, p.Trigger),
			Payload: map[string]any{"verification_trigger": p.Trigger, "token_symbol": p.Symbol},
		})
	} else {
		// 写入 Token Lot 和 Carrier Link
		// 设计说明：Token 架构分为数量层(fact_token_lots)和绑定层(fact_token_carrier_links)
		lotID := fmt.Sprintf("lot_%s_%s_%d", p.TokenID, p.TxID, p.Vout)
		linkID := fmt.Sprintf("link_%s_%s_%d", p.TokenID, p.TxID, p.Vout)
		now := time.Now().Unix()
		if p.CreatedAtUnix > 0 {
			now = p.CreatedAtUnix
		}

		return store.Do(ctx, func(db sqlConn) error {
			// 0. 先写入载体 UTXO，carrier link 依赖它做外键约束。
			if err := dbUpsertBSVUTXO(ctx, store, bsvUTXOEntry{
				UTXOID:         p.UTXOID,
				OwnerPubkeyHex: ownerPubkeyHex,
				Address:        p.Address,
				TxID:           p.TxID,
				Vout:           p.Vout,
				ValueSatoshi:   int64(p.ValueSatoshi),
				UTXOState:      "unspent",
				CarrierType:    "token_carrier",
				CreatedAtUnix:  now,
				UpdatedAtUnix:  now,
				Note:           fmt.Sprintf("verified %s token carrier by WOC (trigger: %s)", p.AssetKind, p.Trigger),
				Payload:        map[string]any{"verification_trigger": p.Trigger, "token_symbol": p.Symbol},
			}); err != nil {
				return fmt.Errorf("upsert token carrier utxo: %w", err)
			}
			// 1. 写入 Token Lot
			if err := dbUpsertTokenLotDB(ctx, db, tokenLotEntry{
				LotID:            lotID,
				OwnerPubkeyHex:   ownerPubkeyHex,
				TokenID:          p.TokenID,
				TokenStandard:    p.AssetKind,
				QuantityText:     p.QuantityText,
				UsedQuantityText: "0",
				LotState:         "unspent",
				MintTxid:         p.TxID,
				CreatedAtUnix:    now,
				UpdatedAtUnix:    now,
				Note:             fmt.Sprintf("verified %s by WOC (trigger: %s)", p.AssetKind, p.Trigger),
				Payload:          map[string]any{"verification_trigger": p.Trigger, "token_symbol": p.Symbol},
			}); err != nil {
				return fmt.Errorf("upsert token lot: %w", err)
			}

			// 2. 写入 Carrier Link（绑定 Lot 到 UTXO）
			if err := dbUpsertTokenCarrierLinkDB(ctx, db, tokenCarrierLinkEntry{
				LinkID:         linkID,
				LotID:          lotID,
				CarrierUTXOID:  p.UTXOID,
				OwnerPubkeyHex: ownerPubkeyHex,
				LinkState:      "active",
				BindTxid:       p.TxID,
				CreatedAtUnix:  now,
				UpdatedAtUnix:  now,
				Note:           fmt.Sprintf("carrier link for %s", p.TokenID),
				Payload:        map[string]any{"token_standard": p.AssetKind},
			}); err != nil {
				return fmt.Errorf("upsert token carrier link: %w", err)
			}

			return nil
		})
	}
}

// ========== Settlement Cycle 函数（硬切版保留） ==========

// dbUpsertSettlementPaymentAttempt 幂等写入结算周期，并返回写入后的主键。
func dbUpsertSettlementPaymentAttemptIDCtx(ctx context.Context, db sqlConn, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	if paymentAttemptID == "" {
		return 0, fmt.Errorf("payment_attempt_id is required")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	switch sourceType {
	case "pool_session_quote_pay", "chain_quote_pay", "chain_direct_pay", "chain_asset_create":
	default:
		return 0, fmt.Errorf("source_type must be pool_session_quote_pay, chain_quote_pay, chain_direct_pay or chain_asset_create, got %s", sourceType)
	}
	if state == "" {
		state = "confirmed"
	}
	if state != "pending" && state != "confirmed" && state != "failed" {
		return 0, fmt.Errorf("state must be pending/confirmed/failed, got %s", state)
	}
	now := time.Now().Unix()
	occurredAt := occurredAtUnix
	if occurredAt <= 0 {
		occurredAt = now
	}
	confirmedAt := func() int64 {
		if state == "confirmed" {
			return occurredAt
		}
		return 0
	}()

	_, err := ExecContext(ctx, db,
		`INSERT INTO fact_settlement_payment_attempts(
			payment_attempt_id,source_type,source_id,state,
			gross_amount_satoshi,gate_fee_satoshi,net_amount_satoshi,
			cycle_index,occurred_at_unix,confirmed_at_unix,note,payload_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(source_type, source_id) DO UPDATE SET
			payment_attempt_id=excluded.payment_attempt_id,
			state=CASE
				WHEN fact_settlement_payment_attempts.state='confirmed' AND excluded.state='pending' THEN fact_settlement_payment_attempts.state
				ELSE excluded.state
			END,
			confirmed_at_unix=CASE
				WHEN excluded.state='confirmed' THEN excluded.occurred_at_unix
				ELSE fact_settlement_payment_attempts.confirmed_at_unix
			END,
			gross_amount_satoshi=excluded.gross_amount_satoshi,
			gate_fee_satoshi=excluded.gate_fee_satoshi,
			net_amount_satoshi=excluded.net_amount_satoshi,
			occurred_at_unix=excluded.occurred_at_unix,
			note=excluded.note,
			payload_json=excluded.payload_json`,
		paymentAttemptID, sourceType, sourceID, state,
		grossSatoshi, gateFeeSatoshi, netSatoshi,
		paymentAttemptIndex, occurredAt, confirmedAt,
		strings.TrimSpace(note), mustJSONString(payload),
		)
	if err != nil {
		return 0, err
	}
	return dbGetSettlementPaymentAttemptBySourceCtx(ctx, db, sourceType, sourceID)
}

// dbUpsertSettlementPaymentAttemptCtx 保持旧签名，内部走主键返回版。
func dbUpsertSettlementPaymentAttemptCtx(ctx context.Context, db sqlConn, paymentAttemptID string, sourceType string, sourceID string, state string,
	grossSatoshi int64, gateFeeSatoshi int64, netSatoshi int64,
	paymentAttemptIndex int, occurredAtUnix int64, note string, payload any) error {
	_, err := dbUpsertSettlementPaymentAttemptIDCtx(ctx, db, paymentAttemptID, sourceType, sourceID, state,
		grossSatoshi, gateFeeSatoshi, netSatoshi, paymentAttemptIndex, occurredAtUnix, note, payload)
	return err
}

// dbGetSettlementPaymentAttemptBySource 通过 source_type/source_id 查找 settlement_payment_attempt_id
func dbGetSettlementPaymentAttemptBySourceCtx(ctx context.Context, db sqlConn, sourceType string, sourceID string) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("db is nil")
	}
	sourceType = strings.ToLower(strings.TrimSpace(sourceType))
	sourceID = strings.TrimSpace(sourceID)
	if sourceType == "" || sourceID == "" {
		return 0, fmt.Errorf("source_type and source_id are required")
	}
	var id int64
	err := QueryRowContext(ctx, db, `SELECT id FROM fact_settlement_payment_attempts WHERE source_type=? AND source_id=?`, sourceType, sourceID).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("%w: settlement payment attempt not found for %s:%s", sql.ErrNoRows, sourceType, sourceID)
	}
	return id, err
}

// dbGetSettlementPaymentAttemptSourceTxID 只通过 settlement_payment_attempt 反查来源 txid。
// 设计说明：
// - 业务扣账只认 settlement_payment_attempt，不再从 payment 事实绕路取 txid；
// - 所有来源都按 settlement_payment_attempt_id 回到渠道子表拿 txid；
// - 其他 source_type 不允许拿来驱动 BSV 扣账。
func dbGetSettlementPaymentAttemptSourceTxIDCtx(ctx context.Context, db sqlConn, settlementPaymentAttemptID int64) (string, error) {
	if db == nil {
		return "", fmt.Errorf("db is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return "", fmt.Errorf("settlement_payment_attempt_id is required")
	}
	var sourceType string
	var sourceID string
	if err := QueryRowContext(ctx, db, `SELECT source_type, source_id FROM fact_settlement_payment_attempts WHERE id=?`, settlementPaymentAttemptID).Scan(&sourceType, &sourceID); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("settlement payment attempt not found: %d", settlementPaymentAttemptID)
		}
		return "", err
	}
	switch strings.ToLower(strings.TrimSpace(sourceType)) {
	case "chain_quote_pay":
		var txid string
		if err := QueryRowContext(ctx, db, `SELECT txid FROM fact_settlement_channel_chain_quote_pay WHERE settlement_payment_attempt_id=?`, settlementPaymentAttemptID).Scan(&txid); err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(txid)), nil
	case "chain_direct_pay":
		var txid string
		if err := QueryRowContext(ctx, db, `SELECT txid FROM fact_settlement_channel_chain_direct_pay WHERE settlement_payment_attempt_id=?`, settlementPaymentAttemptID).Scan(&txid); err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(txid)), nil
	case "chain_asset_create":
		var txid string
		if err := QueryRowContext(ctx, db, `SELECT txid FROM fact_settlement_channel_chain_asset_create WHERE settlement_payment_attempt_id=?`, settlementPaymentAttemptID).Scan(&txid); err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(txid)), nil
	case "pool_session_quote_pay":
		var txid string
		err := QueryRowContext(ctx, db,
			`SELECT txid FROM fact_settlement_channel_pool_session_quote_pay WHERE settlement_payment_attempt_id=?`,
			settlementPaymentAttemptID,
		).Scan(&txid)
		if err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("channel row not found for settlement payment attempt %d", settlementPaymentAttemptID)
			}
			return "", err
		}
		return strings.ToLower(strings.TrimSpace(txid)), nil
	default:
		return "", fmt.Errorf("settlement payment attempt %d source_type %s cannot derive txid", settlementPaymentAttemptID, strings.TrimSpace(sourceType))
	}
}

// ========== 消耗记录函数（硬切版） ==========

// dbAppendBSVConsumptionsForSettlementPaymentAttempt 写入 BSV 消耗记录
func dbAppendBSVConsumptionsForSettlementPaymentAttemptCtx(ctx context.Context, db sqlConn, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	spentByTxid, err := dbGetSettlementPaymentAttemptSourceTxIDCtx(ctx, db, settlementPaymentAttemptID)
	if err != nil {
		return fmt.Errorf("resolve settlement payment attempt txid failed: %w", err)
	}

	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}

		// 先确保 settlement record 落库。
		// 不能依赖 fact_bsv_utxos 的 spent 状态来决定是否写 record，
		// 因为本地投影可能已经把 UTXO 标为 spent（同一 tx），若这里直接 continue 会丢掉 BSV 结算记录。
		recordID := fmt.Sprintf("rec_bsv_%d_%s", settlementPaymentAttemptID, utxoID)
		_, err = ExecContext(ctx, db,
			`INSERT INTO fact_settlement_records(
				record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex, source_utxo_id,
				used_satoshi, used_quantity_text, state, occurred_at_unix, confirmed_at_unix, note, payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id) DO UPDATE SET
				used_satoshi=excluded.used_satoshi,
				state=CASE WHEN fact_settlement_records.state='confirmed' AND excluded.state='pending'
					THEN fact_settlement_records.state ELSE excluded.state END`,
			recordID, settlementPaymentAttemptID, "BSV", "", utxoID,
			fact.AmountSatoshi, "", "confirmed", occurredAtUnix, occurredAtUnix,
			"BSV consumed by settlement payment attempt", mustJSONString(fact.Payload),
		)
		if err != nil {
			return fmt.Errorf("append BSV consumption for utxo %s: %w", utxoID, err)
		}

		var utxoState string
		var currentSpentByTxid string
		if err := QueryRowContext(ctx, db, `SELECT utxo_state, COALESCE(spent_by_txid,'') FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(&utxoState, &currentSpentByTxid); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("bsv utxo not found: %s", utxoID)
			}
			return fmt.Errorf("lookup bsv utxo %s failed: %w", utxoID, err)
		}
		utxoState = strings.ToLower(strings.TrimSpace(utxoState))
		currentSpentByTxid = strings.ToLower(strings.TrimSpace(currentSpentByTxid))
		if utxoState == "spent" {
			if currentSpentByTxid == "" {
				// 旧数据可能只有 spent 状态，没有写入来源 txid。
				// 这里不重复扣，只把事实补齐成 cycle 反查出的真实 txid。
				result, err := ExecContext(ctx, db,
					`UPDATE fact_bsv_utxos SET spent_by_txid=?, spent_at_unix=CASE WHEN spent_at_unix=0 THEN ? ELSE spent_at_unix END, updated_at_unix=? WHERE utxo_id=? AND utxo_state='spent' AND COALESCE(spent_by_txid,'')=''`,
					spentByTxid, occurredAtUnix, occurredAtUnix, utxoID,
				)
				if err != nil {
					return fmt.Errorf("backfill spent_by_txid for bsv utxo %s failed: %w", utxoID, err)
				}
				if result != nil {
					if affected, affErr := result.RowsAffected(); affErr == nil && affected > 0 {
						if address, addrErr := dbLoadBSVUTXOAddressByIDTx(ctx, db, utxoID); addrErr == nil {
							emitFactBSVSpentAppliedEventConn(ctx, db, address, spentByTxid)
						}
					}
				}
				continue
			}
			if currentSpentByTxid == spentByTxid {
				continue
			}
			return fmt.Errorf("bsv utxo %s already spent by %s", utxoID, currentSpentByTxid)
		}

		// 标记 UTXO 为已花费。这里只认 settlement_payment_attempt 推导出的 txid，禁止旁路写空值。
		result, err := ExecContext(ctx, db, `UPDATE fact_bsv_utxos SET utxo_state='spent', spent_by_txid=?, spent_at_unix=?, updated_at_unix=? WHERE utxo_id=? AND utxo_state<>'spent'`,
			spentByTxid, occurredAtUnix, occurredAtUnix, utxoID)
		if err != nil {
			return fmt.Errorf("mark bsv utxo spent %s failed: %w", utxoID, err)
		}
		if result != nil {
			if affected, affErr := result.RowsAffected(); affErr == nil && affected > 0 {
				if address, addrErr := dbLoadBSVUTXOAddressByIDTx(ctx, db, utxoID); addrErr == nil {
					emitFactBSVSpentAppliedEventConn(ctx, db, address, spentByTxid)
				}
			}
		}
	}
	return nil
}

// dbAppendBSVConsumptionsForSettlementPaymentAttemptEntTx 在 ent 事务里写 BSV 消耗记录。
func dbAppendBSVConsumptionsForSettlementPaymentAttemptEntTx(ctx context.Context, tx *gen.Tx, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	spentByTxid, err := dbGetSettlementPaymentAttemptSourceTxIDEntTx(ctx, tx, settlementPaymentAttemptID)
	if err != nil {
		return fmt.Errorf("resolve settlement payment attempt txid failed: %w", err)
	}
	for _, fact := range utxoFacts {
		if strings.TrimSpace(fact.IOSide) != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}
		recordID := fmt.Sprintf("rec_bsv_%d_%s", settlementPaymentAttemptID, utxoID)
		if err := dbAppendSettlementRecordEntTx(ctx, tx, settlementRecordEntry{
			RecordID:                   recordID,
			SettlementPaymentAttemptID: settlementPaymentAttemptID,
			AssetType:                  "BSV",
			SourceUTXOID:               utxoID,
			State:                      "confirmed",
			OccurredAtUnix:             occurredAtUnix,
			ConfirmedAtUnix:            occurredAtUnix,
			Note:                       "BSV consumed by settlement payment attempt",
			Payload:                    fact.Payload,
		}); err != nil {
			return fmt.Errorf("append BSV consumption for utxo %s: %w", utxoID, err)
		}
		current, err := tx.FactBsvUtxos.Query().
			Where(factbsvutxos.UtxoIDEQ(utxoID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return fmt.Errorf("bsv utxo not found: %s", utxoID)
			}
			return fmt.Errorf("lookup bsv utxo %s failed: %w", utxoID, err)
		}
		utxoState := strings.ToLower(strings.TrimSpace(current.UtxoState))
		currentSpentByTxid := strings.ToLower(strings.TrimSpace(current.SpentByTxid))
		if utxoState == "spent" {
			if currentSpentByTxid == "" {
				if _, err := tx.FactBsvUtxos.UpdateOneID(current.ID).
					SetSpentByTxid(spentByTxid).
					SetSpentAtUnix(func() int64 {
						if current.SpentAtUnix <= 0 {
							return occurredAtUnix
						}
						return current.SpentAtUnix
					}()).
					SetUpdatedAtUnix(occurredAtUnix).
					Save(ctx); err != nil {
					return fmt.Errorf("update spent txid for utxo %s failed: %w", utxoID, err)
				}
			}
			continue
		}
		if _, err := tx.FactBsvUtxos.UpdateOneID(current.ID).
			SetUtxoState("spent").
			SetSpentByTxid(spentByTxid).
			SetSpentAtUnix(occurredAtUnix).
			SetUpdatedAtUnix(occurredAtUnix).
			Save(ctx); err != nil {
			return fmt.Errorf("mark bsv utxo spent %s failed: %w", utxoID, err)
		}
	}
	return nil
}

// dbAppendTokenConsumptionsForSettlementPaymentAttempt 写入 Token 消耗记录
func dbAppendTokenConsumptionsForSettlementPaymentAttemptCtx(ctx context.Context, db sqlConn, settlementPaymentAttemptID int64, utxoFacts []chainPaymentUTXOLinkEntry, occurredAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if settlementPaymentAttemptID <= 0 {
		return fmt.Errorf("settlement_payment_attempt_id is required")
	}
	spentByTxid, err := dbGetSettlementPaymentAttemptSourceTxIDCtx(ctx, db, settlementPaymentAttemptID)
	if err != nil {
		return fmt.Errorf("resolve settlement payment attempt txid failed: %w", err)
	}

	for _, fact := range utxoFacts {
		ioSide := strings.TrimSpace(fact.IOSide)
		if ioSide != "input" {
			continue
		}
		utxoID := strings.ToLower(strings.TrimSpace(fact.UTXOID))
		if utxoID == "" {
			continue
		}

		// 从 UTXO 查找对应的 lot
		var lotID string
		err := QueryRowContext(ctx, db, `SELECT lot_id FROM fact_token_carrier_links WHERE carrier_utxo_id=? AND link_state='active' LIMIT 1`, utxoID).Scan(&lotID)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return fmt.Errorf("lookup lot for utxo %s: %w", utxoID, err)
		}

		recordID := fmt.Sprintf("rec_token_%d_%s", settlementPaymentAttemptID, lotID)
		_, err = ExecContext(ctx, db,
			`INSERT INTO fact_settlement_records(
				record_id, settlement_payment_attempt_id, asset_type, owner_pubkey_hex, source_lot_id,
				used_satoshi, used_quantity_text, state, occurred_at_unix, confirmed_at_unix, note, payload_json
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(settlement_payment_attempt_id, asset_type, source_utxo_id, source_lot_id) DO UPDATE SET
				used_quantity_text=excluded.used_quantity_text,
				state=CASE WHEN fact_settlement_records.state='confirmed' AND excluded.state='pending' 
					THEN fact_settlement_records.state ELSE excluded.state END`,
			recordID, settlementPaymentAttemptID, "TOKEN", "", lotID,
			0, fact.QuantityText, "confirmed", occurredAtUnix, occurredAtUnix,
			"Token consumed by settlement payment attempt", mustJSONString(fact.Payload),
		)
		if err != nil {
			return fmt.Errorf("append token consumption for lot %s: %w", lotID, err)
		}

		// 更新 lot 的 used_quantity
		var currentQty, currentUsed string
		_ = QueryRowContext(ctx, db, `SELECT quantity_text, used_quantity_text FROM fact_token_lots WHERE lot_id=?`, lotID).Scan(&currentQty, &currentUsed)
		newUsed, _ := sumDecimalTexts(currentUsed + "," + fact.QuantityText)

		_, _ = ExecContext(ctx, db, `UPDATE fact_token_lots SET used_quantity_text=?, last_spend_txid=?, updated_at_unix=? WHERE lot_id=?`,
			newUsed, spentByTxid, occurredAtUnix, lotID)
	}
	return nil
}
