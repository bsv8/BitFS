package clientapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// 验证本地投影路径调用 markBSVUTXOSpentConn 后会发出 fact_bsv_spent_applied 事件。
// 这样 e2e 通过 obs 等待 spent 收敛时，不会因为“只写库不发事件”而卡住。
func TestMarkBSVUTXOSpentConn_EmitsFactSpentEvent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	const (
		address    = "mtest_fact_spent_event_addr_1"
		utxoID     = "fact_spent_event_tx:0"
		spentByTx  = "fact_spent_event_spender_tx"
		ownerPub   = "mtest_fact_spent_event_addr_1"
		sourceTxID = "fact_spent_event_tx"
	)
	if err := dbUpsertBSVUTXODB(context.Background(), db, bsvUTXOEntry{
		UTXOID:         utxoID,
		OwnerPubkeyHex: ownerPub,
		Address:        address,
		TxID:           sourceTxID,
		Vout:           0,
		ValueSatoshi:   5,
		UTXOState:      "unspent",
		CarrierType:    "plain_bsv",
		CreatedAtUnix:  time.Now().Unix(),
		UpdatedAtUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("seed bsv utxo: %v", err)
	}

	eventCh := make(chan obs.Event, 8)
	remove := obs.AddListener(func(ev obs.Event) {
		if ev.Service != ServiceName || ev.Name != "fact_bsv_spent_applied" {
			return
		}
		select {
		case eventCh <- ev:
		default:
		}
	})
	t.Cleanup(remove)

	ctx := sqlTraceContextWithMeta(context.Background(), "round_obs_emit", "wallet_bsv_transfer_submit", "wallet_local_projection", "TestMarkBSVUTXOSpentConn_EmitsFactSpentEvent")
	if err := markBSVUTXOSpentConn(ctx, db, utxoID, spentByTx, time.Now().Unix()); err != nil {
		t.Fatalf("mark bsv utxo spent: %v", err)
	}

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("fact_bsv_spent_applied event not emitted")
		case ev := <-eventCh:
			gotAddress := anyToString(ev.Fields["address"])
			gotSpentBy := anyToString(ev.Fields["spent_by_txid"])
			gotRows := anyToInt64(ev.Fields["spent_row_count"])
			if gotAddress != address || gotSpentBy != spentByTx {
				continue
			}
			if gotRows < 1 {
				t.Fatalf("spent_row_count invalid: %d", gotRows)
			}
			return
		}
	}
}
