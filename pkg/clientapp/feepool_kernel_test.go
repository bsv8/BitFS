package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/poolcore"
	_ "modernc.org/sqlite"
)

type feePoolKernelMockChain struct {
	utxos []poolcore.UTXO
}

func (m *feePoolKernelMockChain) GetUTXOs(address string) ([]poolcore.UTXO, error) {
	return append([]poolcore.UTXO(nil), m.utxos...), nil
}

func (m *feePoolKernelMockChain) GetTipHeight() (uint32, error) {
	return 100, nil
}

func (m *feePoolKernelMockChain) Broadcast(txHex string) (string, error) {
	return "mock-txid", nil
}

func TestProbeListenOpenNeedAndWallet(t *testing.T) {
	t.Parallel()
	db := newKernelTestDB(t)
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: &feePoolKernelMockChain{
			utxos: []poolcore.UTXO{
				{TxID: "tx1", Vout: 0, Value: 50000},
				{TxID: "tx2", Vout: 1, Value: 48560},
			},
		},
	}
	rt.runIn.Listen.AutoRenewRounds = 100
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("derive wallet address failed: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, addr, []poolcore.UTXO{
		{TxID: "tx1", Vout: 0, Value: 50000},
		{TxID: "tx2", Vout: 1, Value: 48560},
	}); err != nil {
		t.Fatalf("reconcile wallet utxo set failed: %v", err)
	}
	need, have, err := probeListenOpenNeedAndWallet(rt, newClientDB(db, nil), dualInfo(1000, 20))
	if err != nil {
		t.Fatalf("probe listen open need failed: %v", err)
	}
	if need != 100000 {
		t.Fatalf("need mismatch: got=%d want=100000", need)
	}
	if have != 98560 {
		t.Fatalf("have mismatch: got=%d want=98560", have)
	}
}

func TestProbeListenOpenNeedAndWallet_MinimumTakesEffect(t *testing.T) {
	t.Parallel()
	db := newKernelTestDB(t)
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		ActionChain: &feePoolKernelMockChain{
			utxos: []poolcore.UTXO{
				{TxID: "tx1", Vout: 0, Value: 500},
			},
		},
	}
	rt.runIn.Listen.AutoRenewRounds = 1
	rt.runIn.BSV.Network = "test"
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("derive wallet address failed: %v", err)
	}
	if err := seedWalletUTXOsForKernelTest(db, addr, []poolcore.UTXO{
		{TxID: "tx1", Vout: 0, Value: 500},
	}); err != nil {
		t.Fatalf("reconcile wallet utxo set failed: %v", err)
	}
	need, have, err := probeListenOpenNeedAndWallet(rt, newClientDB(db, nil), dualInfo(100, 1000))
	if err != nil {
		t.Fatalf("probe listen open need failed: %v", err)
	}
	if need != 1000 {
		t.Fatalf("need mismatch: got=%d want=1000", need)
	}
	if have != 500 {
		t.Fatalf("have mismatch: got=%d want=500", have)
	}
}

func dualInfo(singleCycle uint64, minimum uint64) poolcore.InfoResp {
	return poolcore.InfoResp{
		SingleCycleFeeSatoshi:    singleCycle,
		MinimumPoolAmountSatoshi: minimum,
	}
}

func newKernelTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply sqlite pragmas failed: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init index db failed: %v", err)
	}
	return db
}

func seedWalletUTXOsForKernelTest(db *sql.DB, address string, utxos []poolcore.UTXO) error {
	var balance uint64
	live := make(map[string]poolcore.UTXO, len(utxos))
	for _, u := range utxos {
		balance += u.Value
		live[strings.ToLower(u.TxID)+":"+fmt.Sprint(u.Vout)] = u
	}
	return SyncWalletAndApplyFacts(
		context.Background(),
		newClientDB(db, nil),
		address,
		liveWalletSnapshot{
			Live:               live,
			ConfirmedLiveTxIDs: map[string]struct{}{},
			Balance:            balance,
			Count:              len(utxos),
		},
		nil,
		walletUTXOSyncCursor{
			WalletID:            walletIDByAddress(address),
			Address:             address,
			NextConfirmedHeight: 1,
			UpdatedAtUnix:       time.Now().Unix(),
		},
		&testWalletScriptEvidenceSource{txHex: testWalletScriptPlainTxHex},
		"kernel-seed",
		"",
		"test",
		time.Now().Unix(),
		0,
	)
}
