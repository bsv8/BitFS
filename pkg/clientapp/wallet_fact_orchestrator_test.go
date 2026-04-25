package clientapp

import (
	"context"
	"testing"
	"time"
)

func TestApplyConfirmedUTXOChangesKeepsSpentFactMonotonic(t *testing.T) {
	db := openClientappTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	now := time.Now().Unix()

	ownerPubkey := "031111111111111111111111111111111111111111111111111111111111111111"
	spentUTXOID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"
	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         spentUTXOID,
		OwnerPubkeyHex: ownerPubkey,
		Address:        "1monotonic-spent-test",
		TxID:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Vout:           0,
		ValueSatoshi:   2000,
		UTXOState:      "spent",
		CarrierType:    "plain_bsv",
		SpentByTxid:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CreatedAtUnix:  now - 10,
		UpdatedAtUnix:  now - 10,
		SpentAtUnix:    now - 10,
		Note:           "seed spent fact",
	}); err != nil {
		t.Fatalf("seed spent fact: %v", err)
	}

	if err := ApplyConfirmedUTXOChanges(ctx, store, []confirmedUTXOChange{
		{
			UTXOID:           spentUTXOID,
			WalletID:         ownerPubkey,
			Address:          "1monotonic-spent-test",
			TxID:             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Vout:             0,
			Value:            2000,
			ScriptType:       "p2pkh",
			ScriptTypeReason: "confirmed",
			AllocationClass:  "plain_bsv",
			CreatedAtUnix:    now - 20,
		},
		{
			UTXOID:           "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:1",
			WalletID:         ownerPubkey,
			Address:          "1monotonic-change-test",
			TxID:             "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			Vout:             1,
			Value:            1754,
			ScriptType:       "p2pkh",
			ScriptTypeReason: "confirmed",
			AllocationClass:  "plain_bsv",
			CreatedAtUnix:    now - 5,
		},
	}, now); err != nil {
		t.Fatalf("apply confirmed changes: %v", err)
	}

	var state, spentBy string
	if err := db.QueryRowContext(ctx, `SELECT utxo_state, COALESCE(spent_by_txid,''), spent_at_unix FROM fact_bsv_utxos WHERE utxo_id=?`, spentUTXOID).Scan(&state, &spentBy, new(int64)); err != nil {
		t.Fatalf("query spent fact: %v", err)
	}
	if state != "spent" {
		t.Fatalf("state=%q, want spent", state)
	}
	if spentBy != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("spent_by_txid=%q, want original spent txid", spentBy)
	}

	spendable, err := dbListSpendableBSVUTXOs(ctx, store, ownerPubkey)
	if err != nil {
		t.Fatalf("list spendable utxos: %v", err)
	}
	if len(spendable) != 1 {
		t.Fatalf("spendable count=%d, want 1", len(spendable))
	}
	if spendable[0].UTXOID != "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:1" {
		t.Fatalf("spendable utxo=%q, want new change utxo", spendable[0].UTXOID)
	}
}

func TestApplyConfirmedUTXOChangesBackfillsSpentByWalletStateWithoutTimestampMatch(t *testing.T) {
	db := openClientappTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	now := time.Now().Unix()

	addr := "1time-skew-spent-test"
	walletID := walletIDByAddress(addr)
	oldUTXOID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"
	changeUTXOID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:1"
	oldTxID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	changeTxID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		oldUTXOID, walletID, addr, oldTxID, 0, 2000, "spent", "P2PKH", "", now, "plain_bsv", "", oldTxID, changeTxID, now-20, now+1, now+1,
	); err != nil {
		t.Fatalf("seed wallet spent row: %v", err)
	}

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         oldUTXOID,
		OwnerPubkeyHex: addr,
		Address:        addr,
		TxID:           oldTxID,
		Vout:           0,
		ValueSatoshi:   2000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now - 20,
		UpdatedAtUnix:  now - 20,
		Note:           "stale unspent",
	}); err != nil {
		t.Fatalf("seed stale fact utxo: %v", err)
	}

	if err := ApplyConfirmedUTXOChanges(ctx, store, []confirmedUTXOChange{
		{
			UTXOID:           changeUTXOID,
			WalletID:         walletID,
			Address:          addr,
			TxID:             changeTxID,
			Vout:             1,
			Value:            1754,
			ScriptType:       "p2pkh",
			ScriptTypeReason: "confirmed",
			AllocationClass:  "plain_bsv",
			CreatedAtUnix:    now + 1,
		},
	}, now+1); err != nil {
		t.Fatalf("apply confirmed changes: %v", err)
	}

	var state, spentBy string
	if err := db.QueryRowContext(ctx, `SELECT utxo_state, COALESCE(spent_by_txid,'') FROM fact_bsv_utxos WHERE utxo_id=?`, oldUTXOID).Scan(&state, &spentBy); err != nil {
		t.Fatalf("load old fact: %v", err)
	}
	if state != "spent" {
		t.Fatalf("old utxo state=%q, want spent", state)
	}
	if spentBy != changeTxID {
		t.Fatalf("old utxo spent_by_txid=%q, want %s", spentBy, changeTxID)
	}

	spendable, err := dbListSpendableBSVUTXOs(ctx, store, addr)
	if err != nil {
		t.Fatalf("list spendable utxos: %v", err)
	}
	if len(spendable) != 1 {
		t.Fatalf("spendable count=%d, want 1", len(spendable))
	}
	if spendable[0].UTXOID != changeUTXOID {
		t.Fatalf("spendable utxo=%q, want change utxo", spendable[0].UTXOID)
	}
}

func TestApplyConfirmedUTXOChangesBackfillsSpentByTxidForAlreadySpentFact(t *testing.T) {
	db := openClientappTestDB(t)
	store := newClientDB(db, nil)
	ctx := context.Background()
	now := time.Now().Unix()

	addr := "1spent-by-backfill-test"
	walletID := walletIDByAddress(addr)
	oldUTXOID := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd:0"
	changeUTXOID := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee:1"
	oldTxID := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	changeTxID := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	if _, err := db.Exec(`INSERT INTO wallet_utxo(
		utxo_id,wallet_id,address,txid,vout,value_satoshi,state,script_type,script_type_reason,script_type_updated_at_unix,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		oldUTXOID, walletID, addr, oldTxID, 0, 2000, "spent", "P2PKH", "", now, "plain_bsv", "", oldTxID, changeTxID, now-20, now+1, now+1,
	); err != nil {
		t.Fatalf("seed wallet spent row: %v", err)
	}

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         oldUTXOID,
		OwnerPubkeyHex: addr,
		Address:        addr,
		TxID:           oldTxID,
		Vout:           0,
		ValueSatoshi:   2000,
		UTXOState:      "spent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  now - 20,
		UpdatedAtUnix:  now - 20,
		SpentAtUnix:    now - 20,
		Note:           "spent fact without spent_by",
	}); err != nil {
		t.Fatalf("seed spent fact without spent_by: %v", err)
	}

	if err := ApplyConfirmedUTXOChanges(ctx, store, []confirmedUTXOChange{
		{
			UTXOID:           changeUTXOID,
			WalletID:         walletID,
			Address:          addr,
			TxID:             changeTxID,
			Vout:             1,
			Value:            1754,
			ScriptType:       "p2pkh",
			ScriptTypeReason: "confirmed",
			AllocationClass:  "plain_bsv",
			CreatedAtUnix:    now + 1,
		},
	}, now+1); err != nil {
		t.Fatalf("apply confirmed changes: %v", err)
	}

	var state, spentBy string
	if err := db.QueryRowContext(ctx, `SELECT utxo_state, COALESCE(spent_by_txid,'') FROM fact_bsv_utxos WHERE utxo_id=?`, oldUTXOID).Scan(&state, &spentBy); err != nil {
		t.Fatalf("load old fact: %v", err)
	}
	if state != "spent" {
		t.Fatalf("old utxo state=%q, want spent", state)
	}
	if spentBy != changeTxID {
		t.Fatalf("old utxo spent_by_txid=%q, want %s", spentBy, changeTxID)
	}

	spendable, err := dbListSpendableBSVUTXOs(ctx, store, addr)
	if err != nil {
		t.Fatalf("list spendable utxos: %v", err)
	}
	if len(spendable) != 1 {
		t.Fatalf("spendable count=%d, want 1", len(spendable))
	}
	if spendable[0].UTXOID != changeUTXOID {
		t.Fatalf("spendable utxo=%q, want change utxo", spendable[0].UTXOID)
	}
}
