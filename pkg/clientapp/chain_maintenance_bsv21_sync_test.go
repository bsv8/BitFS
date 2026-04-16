package clientapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
)

func TestSyncWalletBSV21HoldingsFromUnspent_ClearAndRecover(t *testing.T) {
	t.Parallel()

	db, _ := newOrchestratorTestDB(t)
	store := newClientDB(db, nil)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "3333333333333333333333333333333333333333333333333333333333333333"
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	address, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	updatedAt := time.Now().Unix()
	tokenID := strings.Repeat("a", 64)
	txid := strings.Repeat("b", 64)

	upserted, cleared, err := syncWalletBSV21HoldingsFromUnspent(context.Background(), store, address, []whatsonchain.BSV21TokenUTXO{
		{
			TxID:       txid,
			Vout:       0,
			TokenID:    tokenID,
			AmountText: "10",
		},
	}, updatedAt)
	if err != nil {
		t.Fatalf("syncWalletBSV21HoldingsFromUnspent(seed): %v", err)
	}
	if upserted != 1 || cleared != 0 {
		t.Fatalf("seed result mismatch: upserted=%d cleared=%d", upserted, cleared)
	}

	var lotState string
	var qty string
	if err := db.QueryRow(`SELECT lot_state, quantity_text FROM fact_token_lots WHERE lot_id=?`, walletBSV21SnapshotLotID(tokenID, txid, 0)).Scan(&lotState, &qty); err != nil {
		t.Fatalf("query lot after seed failed: %v", err)
	}
	if lotState != "unspent" || qty != "10" {
		t.Fatalf("seed lot mismatch: lot_state=%s quantity=%s", lotState, qty)
	}
	var linkState string
	if err := db.QueryRow(`SELECT link_state FROM fact_token_carrier_links WHERE link_id=?`, walletBSV21SnapshotLinkID(tokenID, txid, 0)).Scan(&linkState); err != nil {
		t.Fatalf("query link after seed failed: %v", err)
	}
	if linkState != "active" {
		t.Fatalf("seed link_state mismatch: got=%s want=active", linkState)
	}

	upserted, cleared, err = syncWalletBSV21HoldingsFromUnspent(context.Background(), store, address, nil, updatedAt+1)
	if err != nil {
		t.Fatalf("syncWalletBSV21HoldingsFromUnspent(clear): %v", err)
	}
	if upserted != 0 || cleared != 1 {
		t.Fatalf("clear result mismatch: upserted=%d cleared=%d", upserted, cleared)
	}
	if err := db.QueryRow(`SELECT lot_state FROM fact_token_lots WHERE lot_id=?`, walletBSV21SnapshotLotID(tokenID, txid, 0)).Scan(&lotState); err != nil {
		t.Fatalf("query lot after clear failed: %v", err)
	}
	if lotState != "spent" {
		t.Fatalf("clear lot_state mismatch: got=%s want=spent", lotState)
	}
	if err := db.QueryRow(`SELECT link_state FROM fact_token_carrier_links WHERE link_id=?`, walletBSV21SnapshotLinkID(tokenID, txid, 0)).Scan(&linkState); err != nil {
		t.Fatalf("query link after clear failed: %v", err)
	}
	if linkState != "released" {
		t.Fatalf("clear link_state mismatch: got=%s want=released", linkState)
	}

	upserted, cleared, err = syncWalletBSV21HoldingsFromUnspent(context.Background(), store, address, []whatsonchain.BSV21TokenUTXO{
		{
			TxID:       txid,
			Vout:       0,
			TokenID:    tokenID,
			AmountText: "10",
		},
	}, updatedAt+2)
	if err != nil {
		t.Fatalf("syncWalletBSV21HoldingsFromUnspent(recover): %v", err)
	}
	if upserted != 1 || cleared != 0 {
		t.Fatalf("recover result mismatch: upserted=%d cleared=%d", upserted, cleared)
	}
	if err := db.QueryRow(`SELECT lot_state, used_quantity_text FROM fact_token_lots WHERE lot_id=?`, walletBSV21SnapshotLotID(tokenID, txid, 0)).Scan(&lotState, &qty); err != nil {
		t.Fatalf("query lot after recover failed: %v", err)
	}
	if lotState != "unspent" || qty != "0" {
		t.Fatalf("recover lot mismatch: lot_state=%s used_quantity_text=%s", lotState, qty)
	}
	if err := db.QueryRow(`SELECT link_state FROM fact_token_carrier_links WHERE link_id=?`, walletBSV21SnapshotLinkID(tokenID, txid, 0)).Scan(&linkState); err != nil {
		t.Fatalf("query link after recover failed: %v", err)
	}
	if linkState != "active" {
		t.Fatalf("recover link_state mismatch: got=%s want=active", linkState)
	}
}
