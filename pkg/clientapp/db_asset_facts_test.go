package clientapp

import (
	"context"
	"testing"
	"time"
)

func TestDBUpsertBSVUTXORejectsSpentToUnspent(t *testing.T) {
	db := openClientappTestDB(t)
	ctx := context.Background()
	now := time.Now().Unix()
	utxoID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: "031111111111111111111111111111111111111111111111111111111111111111",
		Address:        "1spent-lock-test",
		TxID:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Vout:           0,
		ValueSatoshi:   2000,
		UTXOState:      "spent",
		CarrierType:    "plain_bsv",
		SpentByTxid:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		CreatedAtUnix:  now - 10,
		UpdatedAtUnix:  now - 10,
		SpentAtUnix:    now - 10,
		Note:           "seed spent",
		Payload: map[string]any{
			"phase": "seed",
		},
	}); err != nil {
		t.Fatalf("seed spent utxo: %v", err)
	}

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: "031111111111111111111111111111111111111111111111111111111111111111",
		Address:        "1spent-lock-test",
		TxID:           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Vout:           0,
		ValueSatoshi:   2000,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		SpentByTxid:    "",
		CreatedAtUnix:  now - 20,
		UpdatedAtUnix:  now - 5,
		SpentAtUnix:    0,
		Note:           "should not override",
		Payload: map[string]any{
			"phase": "override",
		},
	}); err != nil {
		t.Fatalf("rewrite as unspent: %v", err)
	}

	var state, spentBy, note string
	var updatedAt, spentAt int64
	if err := db.QueryRowContext(ctx, `SELECT utxo_state, COALESCE(spent_by_txid,''), COALESCE(note,''), updated_at_unix, COALESCE(spent_at_unix,0) FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(&state, &spentBy, &note, &updatedAt, &spentAt); err != nil {
		t.Fatalf("load utxo: %v", err)
	}
	if state != "spent" {
		t.Fatalf("state=%q, want spent", state)
	}
	if spentBy != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("spent_by_txid=%q, want original spent txid", spentBy)
	}
	if note != "seed spent" {
		t.Fatalf("note=%q, want original note", note)
	}
	if spentAt != now-10 {
		t.Fatalf("spent_at_unix=%d, want %d", spentAt, now-10)
	}
	if updatedAt != now-10 {
		t.Fatalf("updated_at_unix=%d, want %d", updatedAt, now-10)
	}
}

func TestDBUpsertBSVUTXOAllowsSpentMetadataUpdate(t *testing.T) {
	db := openClientappTestDB(t)
	ctx := context.Background()
	now := time.Now().Unix()
	utxoID := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc:1"

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: "031111111111111111111111111111111111111111111111111111111111111111",
		Address:        "1spent-update-test",
		TxID:           "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Vout:           1,
		ValueSatoshi:   1754,
		UTXOState:      "spent",
		CarrierType:    "plain_bsv",
		SpentByTxid:    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		CreatedAtUnix:  now - 30,
		UpdatedAtUnix:  now - 30,
		SpentAtUnix:    now - 30,
		Note:           "old note",
		Payload: map[string]any{
			"phase": "old",
		},
	}); err != nil {
		t.Fatalf("seed spent utxo: %v", err)
	}

	if err := dbUpsertBSVUTXODB(ctx, db, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: "031111111111111111111111111111111111111111111111111111111111111111",
		Address:        "1spent-update-test",
		TxID:           "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Vout:           1,
		ValueSatoshi:   1754,
		UTXOState:      "spent",
		CarrierType:    "plain_bsv",
		SpentByTxid:    "",
		CreatedAtUnix:  now - 30,
		UpdatedAtUnix:  now + 1,
		SpentAtUnix:    0,
		Note:           "new note",
		Payload: map[string]any{
			"phase": "new",
		},
	}); err != nil {
		t.Fatalf("update spent utxo: %v", err)
	}

	var state, spentBy, note, payload string
	var updatedAt, spentAt int64
	if err := db.QueryRowContext(ctx, `SELECT utxo_state, COALESCE(spent_by_txid,''), COALESCE(note,''), COALESCE(payload_json,''), updated_at_unix, COALESCE(spent_at_unix,0) FROM fact_bsv_utxos WHERE utxo_id=?`, utxoID).Scan(&state, &spentBy, &note, &payload, &updatedAt, &spentAt); err != nil {
		t.Fatalf("load utxo: %v", err)
	}
	if state != "spent" {
		t.Fatalf("state=%q, want spent", state)
	}
	if spentBy != "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" {
		t.Fatalf("spent_by_txid=%q, want original spent txid", spentBy)
	}
	if note != "new note" {
		t.Fatalf("note=%q, want new note", note)
	}
	if payload == "" || payload == "{}" {
		t.Fatalf("payload_json not updated: %q", payload)
	}
	if updatedAt != now+1 {
		t.Fatalf("updated_at_unix=%d, want %d", updatedAt, now+1)
	}
	if spentAt == 0 {
		t.Fatalf("spent_at_unix should stay set")
	}
}
