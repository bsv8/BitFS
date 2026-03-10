package clientapp

import (
	"context"
	"strings"
	"testing"
)

func TestKernelDirectDownloadCoreWritesJournal(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db}

	_, err := runDirectDownloadCore(context.Background(), rt, directDownloadCoreParams{
		SeedHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	}, directDownloadCoreHooks{})
	if err == nil {
		t.Fatalf("expected direct download core to fail on incomplete runtime")
	}

	var status, errorCode, aggregateID string
	if qerr := db.QueryRow(
		`SELECT status,error_code,aggregate_id FROM command_journal
		 WHERE command_type=? ORDER BY id DESC LIMIT 1`,
		clientKernelCommandDirectDownloadCore,
	).Scan(&status, &errorCode, &aggregateID); qerr != nil {
		t.Fatalf("query command_journal failed: %v", qerr)
	}
	if strings.TrimSpace(status) != "failed" {
		t.Fatalf("journal status mismatch: got=%s want=failed", status)
	}
	if strings.TrimSpace(errorCode) != "direct_download_core_failed" {
		t.Fatalf("journal error_code mismatch: got=%s want=direct_download_core_failed", errorCode)
	}
	if !strings.HasPrefix(strings.TrimSpace(aggregateID), "seed:") {
		t.Fatalf("journal aggregate_id mismatch: got=%s", aggregateID)
	}
}

func TestKernelTransferByStrategyWritesJournal(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	rt := &Runtime{DB: db}

	_, err := TriggerTransferChunksByStrategy(context.Background(), rt, TransferChunksByStrategyParams{
		DemandID: "dmd_test",
		SeedHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	})
	if err == nil {
		t.Fatalf("expected transfer by strategy to fail on incomplete runtime")
	}

	var status, errorCode, aggregateID string
	if qerr := db.QueryRow(
		`SELECT status,error_code,aggregate_id FROM command_journal
		 WHERE command_type=? ORDER BY id DESC LIMIT 1`,
		clientKernelCommandTransferByStrategy,
	).Scan(&status, &errorCode, &aggregateID); qerr != nil {
		t.Fatalf("query command_journal failed: %v", qerr)
	}
	if strings.TrimSpace(status) != "failed" {
		t.Fatalf("journal status mismatch: got=%s want=failed", status)
	}
	if strings.TrimSpace(errorCode) != "direct_transfer_by_strategy_failed" {
		t.Fatalf("journal error_code mismatch: got=%s want=direct_transfer_by_strategy_failed", errorCode)
	}
	if !strings.HasPrefix(strings.TrimSpace(aggregateID), "seed:") {
		t.Fatalf("journal aggregate_id mismatch: got=%s", aggregateID)
	}
}
