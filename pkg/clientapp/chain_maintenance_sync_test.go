package clientapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/woc"
)

type walletSyncMockChain struct {
	confirmedUTXOs        []woc.UTXO
	unconfirmedHistory    []string
	txDetails             map[string]woc.TxDetail
	confirmedHistoryPages map[string]woc.ConfirmedHistoryPage
	tipHeight             uint32
}

func (m *walletSyncMockChain) GetUTXOs(address string) ([]woc.UTXO, error) {
	return append([]woc.UTXO(nil), m.confirmedUTXOs...), nil
}

func (m *walletSyncMockChain) GetTipHeight() (uint32, error) {
	if m.tipHeight == 0 {
		return 100, nil
	}
	return m.tipHeight, nil
}

func (m *walletSyncMockChain) Broadcast(txHex string) (string, error) {
	return "mock-txid", nil
}

func (m *walletSyncMockChain) GetConfirmedHistoryPageContext(ctx context.Context, address string, q woc.ConfirmedHistoryQuery) (woc.ConfirmedHistoryPage, error) {
	_ = ctx
	key := strings.ToLower(strings.TrimSpace(q.Order)) + "|" + strings.TrimSpace(q.Token)
	if page, ok := m.confirmedHistoryPages[key]; ok {
		return page, nil
	}
	return woc.ConfirmedHistoryPage{}, nil
}

func (m *walletSyncMockChain) GetUnconfirmedHistoryContext(ctx context.Context, address string) ([]string, error) {
	_ = ctx
	return append([]string(nil), m.unconfirmedHistory...), nil
}

func (m *walletSyncMockChain) GetTxDetailContext(ctx context.Context, txid string) (woc.TxDetail, error) {
	_ = ctx
	return m.txDetails[strings.ToLower(strings.TrimSpace(txid))], nil
}

func TestCollectCurrentWalletSnapshot_TracksMempoolLifecycle(t *testing.T) {
	t.Parallel()

	rt := &Runtime{runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	scriptHex, err := walletAddressLockScriptHex(addr)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}
	chain := &walletSyncMockChain{
		confirmedUTXOs: []woc.UTXO{
			{TxID: "txc1", Vout: 0, Value: 1000},
			{TxID: "txc2", Vout: 0, Value: 2000},
		},
		unconfirmedHistory: []string{"txm1", "txm2"},
		txDetails: map[string]woc.TxDetail{
			"txm1": {
				TxID: "txm1",
				Vin:  []woc.TxInput{{TxID: "txc2", Vout: 0}},
				Vout: []woc.TxOutput{{N: 0, Value: 0.000015, ScriptPubKey: woc.ScriptPubKey{Hex: scriptHex}}},
			},
			"txm2": {
				TxID: "txm2",
				Vin:  []woc.TxInput{{TxID: "txm1", Vout: 0}},
				Vout: []woc.TxOutput{{N: 1, Value: 0.000012, ScriptPubKey: woc.ScriptPubKey{Hex: scriptHex}}},
			},
		},
		confirmedHistoryPages: map[string]woc.ConfirmedHistoryPage{
			"desc|": {Items: []woc.AddressHistoryItem{{TxID: "txc1", Height: 10}}},
		},
	}

	snapshot, err := collectCurrentWalletSnapshot(context.Background(), chain, addr)
	if err != nil {
		t.Fatalf("collectCurrentWalletSnapshot failed: %v", err)
	}
	if snapshot.Count != 2 {
		t.Fatalf("snapshot count mismatch: got=%d want=2", snapshot.Count)
	}
	if snapshot.Balance != 2200 {
		t.Fatalf("snapshot balance mismatch: got=%d want=2200", snapshot.Balance)
	}
	if snapshot.OldestConfirmedHeight != 10 {
		t.Fatalf("oldest confirmed height mismatch: got=%d want=10", snapshot.OldestConfirmedHeight)
	}
	if _, ok := snapshot.Live["txc1:0"]; !ok {
		t.Fatalf("confirmed live utxo missing: %+v", snapshot.Live)
	}
	if _, ok := snapshot.Live["txm2:1"]; !ok {
		t.Fatalf("mempool live utxo missing: %+v", snapshot.Live)
	}
	if _, ok := snapshot.Live["txc2:0"]; ok {
		t.Fatalf("spent confirmed utxo should be removed: %+v", snapshot.Live)
	}
	if _, ok := snapshot.Live["txm1:0"]; ok {
		t.Fatalf("spent mempool utxo should be removed: %+v", snapshot.Live)
	}
}

func TestReconcileWalletUTXOSet_PersistsSpentTracking(t *testing.T) {
	t.Parallel()

	db := newKernelTestDB(t)
	rt := &Runtime{runIn: RunInput{
		EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
	}}
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress failed: %v", err)
	}
	scriptHex, err := walletAddressLockScriptHex(addr)
	if err != nil {
		t.Fatalf("walletAddressLockScriptHex failed: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, addr, []dual2of2.UTXO{{TxID: "oldtx", Vout: 0, Value: 500}}); err != nil {
		t.Fatalf("seedWalletUTXOsForKernelTest failed: %v", err)
	}

	now := time.Now().Unix()
	snapshot := liveWalletSnapshot{
		Live: map[string]dual2of2.UTXO{
			"curtx:0": {TxID: "curtx", Vout: 0, Value: 3000},
		},
		ObservedMempoolTxs: []woc.TxDetail{
			{
				TxID: "unconf1",
				Vout: []woc.TxOutput{{N: 0, Value: 0.000007, ScriptPubKey: woc.ScriptPubKey{Hex: scriptHex}}},
			},
			{
				TxID: "unconf2",
				Vin:  []woc.TxInput{{TxID: "unconf1", Vout: 0}},
			},
		},
		Balance: 3000,
		Count:   1,
	}
	history := []walletHistoryTxRecord{
		{
			TxID:   "hist1",
			Height: 20,
			Tx: woc.TxDetail{
				TxID: "hist1",
				Vout: []woc.TxOutput{{N: 0, Value: 0.00001, ScriptPubKey: woc.ScriptPubKey{Hex: scriptHex}}},
			},
		},
		{
			TxID:   "hist2",
			Height: 21,
			Tx: woc.TxDetail{
				TxID: "hist2",
				Vin:  []woc.TxInput{{TxID: "hist1", Vout: 0}},
			},
		},
	}
	cursor := walletUTXOHistoryCursor{
		WalletID:            walletIDByAddress(addr),
		Address:             addr,
		AnchorHeight:        20,
		NextConfirmedHeight: 101,
		RoundTipHeight:      100,
		UpdatedAtUnix:       now,
	}

	if err := reconcileWalletUTXOSet(db, addr, snapshot, history, cursor, "", "test", now, 9); err != nil {
		t.Fatalf("reconcileWalletUTXOSet failed: %v", err)
	}

	assertWalletUTXOState(t, db, "oldtx:0", "spent", "")
	assertWalletUTXOState(t, db, "curtx:0", "unspent", "")
	assertWalletUTXOState(t, db, "hist1:0", "spent", "hist2")
	assertWalletUTXOState(t, db, "unconf1:0", "spent", "unconf2")

	state, err := loadWalletUTXOSyncState(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOSyncState failed: %v", err)
	}
	if state.UTXOCount != 1 || state.BalanceSatoshi != 3000 {
		t.Fatalf("unexpected sync state: %+v", state)
	}
	cur, err := loadWalletUTXOHistoryCursor(db, addr)
	if err != nil {
		t.Fatalf("loadWalletUTXOHistoryCursor failed: %v", err)
	}
	if cur.NextConfirmedHeight != 101 || cur.AnchorHeight != 20 {
		t.Fatalf("unexpected history cursor: %+v", cur)
	}
}

func assertWalletUTXOState(t *testing.T, db *sql.DB, utxoID string, wantState string, wantSpentTxID string) {
	t.Helper()
	var state string
	var spentTxID string
	err := db.QueryRow(`SELECT state,spent_txid FROM wallet_utxo WHERE utxo_id=?`, utxoID).Scan(&state, &spentTxID)
	if err != nil {
		t.Fatalf("query wallet_utxo %s failed: %v", utxoID, err)
	}
	if state != wantState || spentTxID != wantSpentTxID {
		t.Fatalf("wallet_utxo %s mismatch: got state=%s spent_txid=%s want state=%s spent_txid=%s", utxoID, state, spentTxID, wantState, wantSpentTxID)
	}
}
