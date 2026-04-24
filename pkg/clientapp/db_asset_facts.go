package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factbsvutxos"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/factsettlementrecords"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/facttokencarrierlinks"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/facttokenlots"
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
func dbAppendBSVSettlementRecordsForCycleTx(ctx context.Context, tx EntWriteRoot, settlementPaymentAttemptID int64, ownerPubkeyHex string, utxoIDs []string, occurredAtUnix int64, payload any) error {
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
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertBSVUTXOTx(ctx, tx, e)
	})
}

func dbUpsertBSVUTXOTx(ctx context.Context, tx EntWriteRoot, e bsvUTXOEntry) error {
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

func queryFactBSVSpentCountByAddressTxID(ctx context.Context, tx EntWriteRoot, address string, spentByTxid string) (int64, error) {
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

func emitFactBSVSpentAppliedEvent(ctx context.Context, tx EntWriteRoot, address string, spentByTxid string) {
	address = strings.TrimSpace(address)
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if address == "" || spentByTxid == "" {
		return
	}
	spentRowCount, err := queryFactBSVSpentCountByAddressTxID(ctx, tx, address, spentByTxid)
	if err != nil {
		return
	}
	obs.Info(ServiceName, "fact_bsv_spent_applied", map[string]any{
		"address":         address,
		"spent_by_txid":   spentByTxid,
		"spent_row_count": spentRowCount,
		"sync_round_id":   strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey))),
		"trigger_source":  strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey))),
	})
}

func dbLoadBSVUTXOAddressByIDTx(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, utxoID string) (string, error) {
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

func emitFactBSVSpentAppliedEventConn(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, address string, spentByTxid string) {
	address = strings.TrimSpace(address)
	spentByTxid = strings.ToLower(strings.TrimSpace(spentByTxid))
	if address == "" || spentByTxid == "" {
		return
	}
	var spentRowCount int64
	if err := QueryRowContext(ctx, db, `SELECT COUNT(1) FROM fact_bsv_utxos WHERE address=? AND utxo_state='spent' AND spent_by_txid=?`, address, spentByTxid).Scan(&spentRowCount); err != nil {
		return
	}
	obs.Info(ServiceName, "fact_bsv_spent_applied", map[string]any{
		"address":         address,
		"spent_by_txid":   spentByTxid,
		"spent_row_count": spentRowCount,
		"sync_round_id":   strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextRoundIDKey))),
		"trigger_source":  strings.TrimSpace(anyToString(ctx.Value(sqlTraceContextTriggerKey))),
	})
}

// dbGetBSVUTXO 查询单个本币UTXO

func dbUpsertBSVUTXODB(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, e bsvUTXOEntry) error {
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
	var (
		currentOwnerPubkeyHex string
		currentAddress        string
		currentTxid           string
		currentVout           int64
		currentValueSatoshi   int64
		currentUTXOState      string
		currentCarrierType    string
		currentSpentByTxid    string
		currentSpentAt        int64
		currentNote           string
		currentPayloadJSON    string
	)
	if err := QueryRowContext(ctx, db, `SELECT owner_pubkey_hex,address,txid,vout,value_satoshi,utxo_state,carrier_type,COALESCE(spent_by_txid,''),created_at_unix,updated_at_unix,COALESCE(spent_at_unix,0),COALESCE(note,''),COALESCE(payload_json,'') FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(
		&currentOwnerPubkeyHex,
		&currentAddress,
		&currentTxid,
		&currentVout,
		&currentValueSatoshi,
		&currentUTXOState,
		&currentCarrierType,
		&currentSpentByTxid,
		new(int64),
		new(int64),
		&currentSpentAt,
		&currentNote,
		&currentPayloadJSON,
	); err == nil {
		if currentOwnerPubkeyHex == ownerPubkey &&
			strings.TrimSpace(currentAddress) == strings.TrimSpace(e.Address) &&
			currentTxid == txid &&
			currentVout == int64(e.Vout) &&
			currentValueSatoshi == e.ValueSatoshi &&
			currentUTXOState == utxoState &&
			currentCarrierType == carrierType &&
			currentSpentByTxid == spentByTxid &&
			currentSpentAt == spentAt &&
			strings.TrimSpace(currentNote) == strings.TrimSpace(e.Note) &&
			currentPayloadJSON == mustJSONString(e.Payload) {
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

func markBSVUTXOSpentConn(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, utxoID string, spentByTxid string, updatedAt int64) error {
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
	result, err := db.ExecContext(ctx,
		`UPDATE fact_bsv_utxos SET utxo_state='spent', spent_by_txid=?, spent_at_unix=?, updated_at_unix=? WHERE utxo_id=?`,
		spentByTxid, updatedAt, updatedAt, utxoID,
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(spentByTxid) == "" {
		return nil
	}
	if result != nil {
		if affected, affErr := result.RowsAffected(); affErr == nil && affected > 0 {
			if address, addrErr := dbLoadBSVUTXOAddressByIDTx(ctx, db, utxoID); addrErr == nil {
				emitFactBSVSpentAppliedEventConn(ctx, db, address, spentByTxid)
			}
		}
	}
	return nil
}

// dbListSpendableBSVUTXOs 查询可花费的本币UTXO列表
// 设计说明：
// - 只返回 utxo_state='unspent' 且 carrier_type='plain_bsv' 的记录
// - 用于支付前的选币
func dbListSpendableBSVUTXOs(ctx context.Context, store *clientDB, ownerPubkeyHex string) ([]bsvUTXOEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]bsvUTXOEntry, error) {
		return dbListSpendableBSVUTXOsEntRoot(ctx, root, ownerPubkeyHex)
	})
}

func dbListSpendableBSVUTXOsEntRoot(ctx context.Context, root EntReadRoot, ownerPubkeyHex string) ([]bsvUTXOEntry, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("owner_pubkey_hex is required")
	}
	rows, err := root.FactBsvUtxos.Query().
		Where(
			factbsvutxos.OwnerPubkeyHexEQ(ownerPubkeyHex),
			factbsvutxos.UtxoStateEQ("unspent"),
			factbsvutxos.CarrierTypeEQ("plain_bsv"),
		).
		Order(factbsvutxos.ByValueSatoshi(), factbsvutxos.ByCreatedAtUnix()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]bsvUTXOEntry, 0, len(rows))
	for _, row := range rows {
		out = append(out, bsvUTXOEntry{
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
			Payload:        json.RawMessage(row.PayloadJSON),
		})
	}
	return out, nil
}

func dbListSpendableBSVUTXOsDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) ([]bsvUTXOEntry, error) {
	return nil, fmt.Errorf("sql fallback removed: use ent path")
}

// dbCalcBSVBalance 计算本币余额
// 返回：confirmed（已确认可用余额）、total（包含pending的总余额）

func dbCalcBSVBalanceEntRoot(ctx context.Context, root EntReadRoot, ownerPubkeyHex string) (uint64, uint64, error) {
	if root == nil {
		return 0, 0, fmt.Errorf("root is nil")
	}
	ownerPubkeyHex = strings.ToLower(strings.TrimSpace(ownerPubkeyHex))
	if ownerPubkeyHex == "" {
		return 0, 0, fmt.Errorf("owner_pubkey_hex is required")
	}
	rows, err := root.FactBsvUtxos.Query().
		Where(
			factbsvutxos.OwnerPubkeyHexEQ(ownerPubkeyHex),
			factbsvutxos.UtxoStateEQ("unspent"),
		).
		All(ctx)
	if err != nil {
		return 0, 0, err
	}
	var confirmed uint64
	var total uint64
	for _, row := range rows {
		v := uint64(row.ValueSatoshi)
		total += v
		if strings.EqualFold(strings.TrimSpace(row.CarrierType), "plain_bsv") {
			confirmed += v
		}
	}
	return confirmed, total, nil
}

func dbCalcBSVBalanceDB(ctx context.Context, db sqlConn, ownerPubkeyHex string) (uint64, uint64, error) {
	return 0, 0, fmt.Errorf("sql fallback removed: use ent path")
}

// ========== Token Lot 读写 ==========

// dbUpsertTokenLot 幂等写入/更新 Token Lot

func dbUpsertTokenLotDB(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, e tokenLotEntry) error {
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
func dbUpsertTokenLotEntTx(ctx context.Context, tx EntWriteRoot, e tokenLotEntry) error {
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

// dbGetTokenLotEntTx 在 ent 事务里查询单个 Token Lot。
func dbGetTokenLotEntTx(ctx context.Context, tx EntWriteRoot, lotID string) (*tokenLotEntry, error) {
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

// dbListSpendableTokenLots 查询可花费的 Token Lot 列表
func dbListSpendableTokenLots(ctx context.Context, store *clientDB, ownerPubkeyHex string, tokenStandard string, tokenID string) ([]tokenLotEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]tokenLotEntry, error) {
		return dbListSpendableTokenLotsEntRoot(ctx, root, ownerPubkeyHex, tokenStandard, tokenID)
	})
}

func dbListSpendableTokenLotsEntRoot(ctx context.Context, root EntReadRoot, ownerPubkeyHex string, tokenStandard string, tokenID string) ([]tokenLotEntry, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
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
	rows, err := root.FactTokenLots.Query().
		Where(
			facttokenlots.OwnerPubkeyHexEQ(ownerPubkeyHex),
			facttokenlots.TokenStandardEQ(tokenStandard),
			facttokenlots.TokenIDEQ(tokenID),
			facttokenlots.LotStateEQ("unspent"),
		).
		Order(facttokenlots.ByCreatedAtUnix()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]tokenLotEntry, 0, len(rows))
	for _, row := range rows {
		out = append(out, tokenLotEntry{
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
		})
	}
	return out, nil
}

func dbListSpendableTokenLotsDB(ctx context.Context, db sqlConn, ownerPubkeyHex string, tokenStandard string, tokenID string) ([]tokenLotEntry, error) {
	return nil, fmt.Errorf("sql fallback removed: use ent path")
}

// dbCalcTokenBalance 计算单个 Token 的余额
// 返回：余额数量（十进制字符串）、误差信息

func dbCalcTokenBalanceEntRoot(ctx context.Context, root EntReadRoot, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	if root == nil {
		return "", fmt.Errorf("root is nil")
	}
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
	rows, err := root.FactTokenLots.Query().
		Where(
			facttokenlots.OwnerPubkeyHexEQ(ownerPubkeyHex),
			facttokenlots.TokenStandardEQ(tokenStandard),
			facttokenlots.TokenIDEQ(tokenID),
			facttokenlots.LotStateEQ("unspent"),
		).
		All(ctx)
	if err != nil {
		return "", err
	}
	var totalBalance big.Int
	for _, row := range rows {
		qtyParsed, err := parseDecimalText(row.QuantityText)
		if err != nil {
			continue
		}
		usedParsed, err := parseDecimalText(row.UsedQuantityText)
		if err != nil {
			usedParsed = struct {
				intValue *big.Int
				scale    int
			}{intValue: big.NewInt(0), scale: 0}
		}
		scale := qtyParsed.scale
		if usedParsed.scale > scale {
			scale = usedParsed.scale
		}
		qtyVal := new(big.Int).Set(qtyParsed.intValue)
		usedVal := new(big.Int).Set(usedParsed.intValue)
		for i := qtyParsed.scale; i < scale; i++ {
			qtyVal.Mul(qtyVal, big.NewInt(10))
		}
		for i := usedParsed.scale; i < scale; i++ {
			usedVal.Mul(usedVal, big.NewInt(10))
		}
		diff := new(big.Int).Sub(qtyVal, usedVal)
		if diff.Sign() < 0 {
			diff = big.NewInt(0)
		}
		totalBalance.Add(&totalBalance, diff)
	}
	return totalBalance.String(), nil
}

func dbCalcTokenBalanceDB(ctx context.Context, db sqlConn, ownerPubkeyHex string, tokenStandard string, tokenID string) (string, error) {
	return "", fmt.Errorf("sql fallback removed: use ent path")
}

// ========== Token Carrier Link 读写 ==========

// dbUpsertTokenCarrierLink 幂等写入/更新 Token Carrier Link

func dbUpsertTokenCarrierLinkDB(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, e tokenCarrierLinkEntry) error {
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

func dbUpsertTokenCarrierLinkEntTx(ctx context.Context, tx EntWriteRoot, e tokenCarrierLinkEntry) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
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
	row, err := tx.FactTokenCarrierLinks.Query().
		Where(facttokencarrierlinks.LinkIDEQ(linkID)).
		Only(ctx)
	if err == nil {
		upd := row.Update().
			SetLotID(lotID).
			SetCarrierUtxoID(carrierUTXOID).
			SetOwnerPubkeyHex(ownerPubkey).
			SetLinkState(linkState).
			SetBindTxid(strings.TrimSpace(e.BindTxid)).
			SetUpdatedAtUnix(updatedAt).
			SetNote(strings.TrimSpace(e.Note)).
			SetPayloadJSON(mustJSONString(e.Payload))
		if nextUnbind := strings.TrimSpace(e.UnbindTxid); nextUnbind != "" {
			upd = upd.SetUnbindTxid(nextUnbind)
		}
		_, err = upd.Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = tx.FactTokenCarrierLinks.Create().
		SetLinkID(linkID).
		SetLotID(lotID).
		SetCarrierUtxoID(carrierUTXOID).
		SetOwnerPubkeyHex(ownerPubkey).
		SetLinkState(linkState).
		SetBindTxid(strings.TrimSpace(e.BindTxid)).
		SetUnbindTxid(strings.TrimSpace(e.UnbindTxid)).
		SetCreatedAtUnix(createdAt).
		SetUpdatedAtUnix(updatedAt).
		SetNote(strings.TrimSpace(e.Note)).
		SetPayloadJSON(mustJSONString(e.Payload)).
		Save(ctx)
	return err
}

// dbGetActiveCarrierForLot 查询 Lot 的 active carrier
func dbGetActiveCarrierForLot(ctx context.Context, store *clientDB, lotID string) (*tokenCarrierLinkEntry, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (*tokenCarrierLinkEntry, error) {
		return dbGetActiveCarrierForLotEntRoot(ctx, root, lotID)
	})
}

func dbGetActiveCarrierForLotEntRoot(ctx context.Context, root EntReadRoot, lotID string) (*tokenCarrierLinkEntry, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	lotID = strings.TrimSpace(lotID)
	if lotID == "" {
		return nil, fmt.Errorf("lot_id is required")
	}
	row, err := root.FactTokenCarrierLinks.Query().
		Where(
			facttokencarrierlinks.LotIDEQ(lotID),
			facttokencarrierlinks.LinkStateEQ("active"),
		).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &tokenCarrierLinkEntry{
		LinkID:         row.LinkID,
		LotID:          row.LotID,
		CarrierUTXOID:  row.CarrierUtxoID,
		OwnerPubkeyHex: row.OwnerPubkeyHex,
		LinkState:      row.LinkState,
		BindTxid:       row.BindTxid,
		UnbindTxid:     row.UnbindTxid,
		CreatedAtUnix:  row.CreatedAtUnix,
		UpdatedAtUnix:  row.UpdatedAtUnix,
		Note:           row.Note,
		Payload:        json.RawMessage(row.PayloadJSON),
	}, nil
}

func dbGetActiveCarrierForLotDB(ctx context.Context, db sqlConn, lotID string) (*tokenCarrierLinkEntry, error) {
	return nil, fmt.Errorf("sql fallback removed: use ent path")
}

// dbListActiveCarrierLinksByOwner 查询某用户的所有 active carrier links

// ========== Settlement Record 读写 ==========

// dbAppendSettlementRecord 写入结算消耗记录

// dbAppendSettlementRecordEntTx 在 ent 事务里写结算消耗记录。
func dbAppendSettlementRecordEntTx(ctx context.Context, tx EntWriteRoot, e settlementRecordEntry) error {
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

// ========== 选币辅助函数 ==========

// selectedBSVUTXO 选币结果
type selectedBSVUTXO struct {
	bsvUTXOEntry
	UseAmount int64 `json:"use_amount"`
}

// dbSelectBSVUTXOsForTarget 按目标金额选币（小额优先）

// 小额优先选币

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

// dbLoadAllWalletTokenBalances 加载钱包所有 Token 余额

// 获取所有有未花费 lot 的 token

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
	return readEntValue(ctx, store, func(root EntReadRoot) ([]spendableSourceFlow, error) {
		return dbListSpendableSourceFlowsEntRoot(ctx, root, walletID, assetKind, tokenID)
	})
}

func dbListSpendableSourceFlowsEntRoot(ctx context.Context, root EntReadRoot, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	ownerPubkeyHex := strings.ToLower(strings.TrimSpace(walletID))
	if ownerPubkeyHex == "" {
		return nil, fmt.Errorf("wallet_id is required")
	}
	ownerPubkeyHex = strings.TrimPrefix(ownerPubkeyHex, "wallet:")
	walletOwnerKey := walletIDByAddress(ownerPubkeyHex)
	rows, err := root.FactBsvUtxos.Query().
		Where(
			factbsvutxos.OwnerPubkeyHexIn(ownerPubkeyHex, walletOwnerKey),
			factbsvutxos.UtxoStateEQ("unspent"),
			factbsvutxos.CarrierTypeEQ("plain_bsv"),
		).
		Order(factbsvutxos.ByValueSatoshi(), factbsvutxos.ByCreatedAtUnix()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]spendableSourceFlow, 0, len(rows))
	var flowID int64 = 1
	for _, row := range rows {
		out = append(out, spendableSourceFlow{
			FlowID:         flowID,
			WalletID:       walletID,
			Address:        row.Address,
			AssetKind:      "BSV",
			TokenID:        "",
			UTXOID:         row.UtxoID,
			TxID:           row.Txid,
			Vout:           uint32(row.Vout),
			TotalInSatoshi: row.ValueSatoshi,
			TotalUsed:      0,
			Remaining:      row.ValueSatoshi,
			OccurredAtUnix: row.CreatedAtUnix,
		})
		flowID++
	}
	return out, nil
}

func dbListSpendableSourceFlowsDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) ([]spendableSourceFlow, error) {
	return nil, fmt.Errorf("sql fallback removed: use ent path")
}

// dbSelectSourceFlowsForTarget 保持旧接口但查询新表
func dbSelectSourceFlowsForTarget(ctx context.Context, store *clientDB, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]selectedSourceFlow, error) {
		flows, err := dbListSpendableSourceFlowsEntRoot(ctx, root, walletID, assetKind, tokenID)
		if err != nil {
			return nil, fmt.Errorf("list spendable flows: %w", err)
		}
		return selectSourceFlowsForTarget(flows, target)
	})
}

func dbSelectSourceFlowsForTargetDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string, target uint64) ([]selectedSourceFlow, error) {
	return nil, fmt.Errorf("sql fallback removed: use ent path")
}

func selectSourceFlowsForTarget(flows []spendableSourceFlow, target uint64) ([]selectedSourceFlow, error) {
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
	return nil, fmt.Errorf("sql fallback removed: use ent path")
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

// 查询新表：获取未花费的 lot 及其 active carrier
// 设计说明：关联 wallet_utxo 表排除 allocation_class='unknown' 的项

// 可以从 carrier_utxo_id 解析，但这里简化

// tokenBalanceResult Token余额结果（保持旧结构）
type tokenBalanceResult struct {
	WalletID      string `json:"wallet_id"`
	AssetKind     string `json:"asset_kind"`
	TokenID       string `json:"token_id"`
	TotalInText   string `json:"total_in_text"`
	TotalUsedText string `json:"total_used_text"`
}

// dbLoadTokenBalanceFact 保持旧接口但查询新表

// 查询新表获取 quantity 和 used_quantity

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
	return readEntValue(ctx, store, func(root EntReadRoot) (walletAssetBalance, error) {
		return dbLoadWalletAssetBalanceFactEntRoot(ctx, root, walletID, assetKind, tokenID)
	})
}

func dbLoadWalletAssetBalanceFactEntRoot(ctx context.Context, root EntReadRoot, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	if root == nil {
		return walletAssetBalance{}, fmt.Errorf("root is nil")
	}
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
		confirmed, _, err := dbCalcBSVBalanceEntRoot(ctx, root, ownerPubkeyHex)
		if err != nil {
			return result, err
		}
		result.TotalInSatoshi = int64(confirmed)
		result.Remaining = int64(confirmed)
		return result, nil
	}
	balance, err := dbCalcTokenBalanceEntRoot(ctx, root, ownerPubkeyHex, assetKind, tokenID)
	if err != nil {
		return result, err
	}
	if balance != "" && balance != "0" {
		result.TotalInSatoshi = 1
		result.Remaining = 1
	}
	return result, nil
}

func dbLoadWalletAssetBalanceFactDB(ctx context.Context, db sqlConn, walletID string, assetKind string, tokenID string) (walletAssetBalance, error) {
	return walletAssetBalance{}, fmt.Errorf("sql fallback removed: use ent path")
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

		return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
			// 0. 先写入载体 UTXO，carrier link 依赖它做外键约束。
			if err := dbUpsertBSVUTXOTx(ctx, tx, bsvUTXOEntry{
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
			if err := dbUpsertTokenLotEntTx(ctx, tx, tokenLotEntry{
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
			if err := dbUpsertTokenCarrierLinkEntTx(ctx, tx, tokenCarrierLinkEntry{
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
