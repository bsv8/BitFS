package clientapp

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPrepareWalletTokenCreate_FailsFastWhenTipStateStale(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("4", 64)
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	addr, err := clientWalletAddress(rt)
	if err != nil {
		t.Fatalf("clientWalletAddress: %v", err)
	}
	seedTipStateForSyncGuardTest(t, db, time.Now().Unix()-int64(syncStateMaxAge(chainTipWorkerInterval)/time.Second)-2, "")
	seedUTXOSyncStateForSyncGuardTest(t, db, addr, time.Now().Unix(), "")

	input, err := normalizeWalletTokenCreateInput("bsv21", "TST", "1000", 0, strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("normalizeWalletTokenCreateInput: %v", err)
	}

	_, err = prepareWalletTokenCreate(context.Background(), newClientDB(db, nil), rt, addr, input)
	if err == nil {
		t.Fatal("expected sync guard error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "sync state stale") {
		t.Fatalf("unexpected error: %v", err)
	}
}
