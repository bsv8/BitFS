package clientapp

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
	"github.com/bsv8/BFTP/pkg/woc"
	_ "modernc.org/sqlite"
)

type feePoolKernelMockChain struct {
	utxos []woc.UTXO
}

func (m *feePoolKernelMockChain) GetUTXOs(address string) ([]woc.UTXO, error) {
	return append([]woc.UTXO(nil), m.utxos...), nil
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
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		Chain: &feePoolKernelMockChain{
			utxos: []woc.UTXO{
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
	if err := reconcileWalletUTXOSet(db, addr, []dual2of2.UTXO{
		{TxID: "tx1", Vout: 0, Value: 50000},
		{TxID: "tx2", Vout: 1, Value: 48560},
	}, 98560, "", "test", time.Now().Unix(), 0); err != nil {
		t.Fatalf("reconcile wallet utxo set failed: %v", err)
	}
	need, have, err := probeListenOpenNeedAndWallet(rt, dualInfo(1000, 20))
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
		DB: db,
		runIn: RunInput{
			EffectivePrivKeyHex: "1111111111111111111111111111111111111111111111111111111111111111",
		},
		Chain: &feePoolKernelMockChain{
			utxos: []woc.UTXO{
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
	if err := reconcileWalletUTXOSet(db, addr, []dual2of2.UTXO{
		{TxID: "tx1", Vout: 0, Value: 500},
	}, 500, "", "test", time.Now().Unix(), 0); err != nil {
		t.Fatalf("reconcile wallet utxo set failed: %v", err)
	}
	need, have, err := probeListenOpenNeedAndWallet(rt, dualInfo(100, 1000))
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

func dualInfo(singleCycle uint64, minimum uint64) dual2of2.InfoResp {
	return dual2of2.InfoResp{
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
