package clientapp

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestRequireFreshTipState(t *testing.T) {
	now := time.Now().Unix()
	freshAt := now - int64((syncStateMaxAge(chainTipWorkerInterval)/time.Second)/2)
	staleAt := now - int64(syncStateMaxAge(chainTipWorkerInterval)/time.Second) - 2

	tests := []struct {
		name      string
		seed      bool
		updatedAt int64
		lastErr   string
		wantErr   string
	}{
		{name: "missing", seed: false, wantErr: "sync state not ready"},
		{name: "stale", seed: true, updatedAt: staleAt, wantErr: "sync state stale"},
		{name: "error", seed: true, updatedAt: freshAt, lastErr: "upstream failed", wantErr: "tip sync unhealthy"},
		{name: "ok", seed: true, updatedAt: freshAt},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := newWalletAccountingTestDB(t)
			store := newClientDB(db, nil)
			if tt.seed {
				seedTipStateForSyncGuardTest(t, db, tt.updatedAt, tt.lastErr)
			}
			_, err := requireFreshTipState(context.Background(), store)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRequireHealthyUTXOSyncState(t *testing.T) {
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("4", 64)
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	now := time.Now().Unix()
	freshAt := now - int64((syncStateMaxAge(chainUTXOWorkerInterval)/time.Second)/2)
	staleAt := now - int64(syncStateMaxAge(chainUTXOWorkerInterval)/time.Second) - 2

	tests := []struct {
		name      string
		seed      bool
		updatedAt int64
		lastErr   string
		wantErr   string
	}{
		{name: "missing", seed: false, wantErr: "sync state not ready"},
		{name: "stale", seed: true, updatedAt: staleAt, wantErr: "sync state stale"},
		{name: "error", seed: true, updatedAt: freshAt, lastErr: "http 418: teapot", wantErr: "utxo sync unhealthy"},
		{name: "ok", seed: true, updatedAt: freshAt},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			db := newWalletAccountingTestDB(t)
			store := newClientDB(db, nil)
			addr, err := clientWalletAddress(rt)
			if err != nil {
				t.Fatalf("clientWalletAddress: %v", err)
			}
			if tt.seed {
				seedUTXOSyncStateForSyncGuardTest(t, db, addr, tt.updatedAt, tt.lastErr)
			}
			_, err = requireHealthyUTXOSyncState(context.Background(), store, rt)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func seedTipStateForSyncGuardTest(t *testing.T, db *sql.DB, updatedAt int64, lastErr string) {
	t.Helper()
	if _, err := db.Exec(`INSERT OR REPLACE INTO proc_chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms) VALUES(1,123,?,?,?,?,0)`, updatedAt, strings.TrimSpace(lastErr), "chain_tip_worker", "test"); err != nil {
		t.Fatalf("seed tip state: %v", err)
	}
}

func seedUTXOSyncStateForSyncGuardTest(t *testing.T, db *sql.DB, address string, updatedAt int64, lastErr string) {
	t.Helper()
	walletID := walletIDByAddress(address)
	if _, err := db.Exec(`INSERT OR REPLACE INTO wallet_utxo_sync_state(address,wallet_id,utxo_count,balance_satoshi,plain_bsv_utxo_count,plain_bsv_balance_satoshi,protected_utxo_count,protected_balance_satoshi,unknown_utxo_count,unknown_balance_satoshi,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms,last_sync_round_id,last_failed_step,last_upstream_path,last_http_status)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		address, walletID, 1, 1000, 1, 1000, 0, 0, 0, 0, updatedAt, strings.TrimSpace(lastErr), "chain_utxo_worker", "test", 0, "round-1", "", "", 200); err != nil {
		t.Fatalf("seed utxo sync state: %v", err)
	}
}
