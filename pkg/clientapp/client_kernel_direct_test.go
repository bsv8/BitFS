package clientapp

import (
	"context"
	"strings"
	"testing"
)

func TestKernelDownloadByHashWritesJournal(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	rt := &Runtime{}
	rt.kernel = newClientKernel(rt, newClientDB(db, nil), nil)

	_, err := runDownloadByHash(context.Background(), rt, downloadByHashParams{
		SeedHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	}, downloadByHashHooks{})
	if err == nil {
		t.Fatalf("expected download by hash to fail on incomplete runtime")
	}

	var status, errorCode, aggregateID string
	if qerr := db.QueryRow(
		`SELECT status,error_code,aggregate_id FROM proc_command_journal
		 WHERE command_type=? ORDER BY id DESC LIMIT 1`,
		clientKernelCommandDownloadByHash,
	).Scan(&status, &errorCode, &aggregateID); qerr != nil {
		t.Fatalf("query proc_command_journal failed: %v", qerr)
	}
	if strings.TrimSpace(status) != "failed" {
		t.Fatalf("journal status mismatch: got=%s want=failed", status)
	}
	if strings.TrimSpace(errorCode) != "download_by_hash_failed" {
		t.Fatalf("journal error_code mismatch: got=%s want=download_by_hash_failed", errorCode)
	}
	if !strings.HasPrefix(strings.TrimSpace(aggregateID), "seed:") {
		t.Fatalf("journal aggregate_id mismatch: got=%s", aggregateID)
	}
}

func TestKernelTransferByStrategyWritesJournal(t *testing.T) {
	t.Parallel()
	db := newWalletAPITestDB(t)
	rt := &Runtime{}
	rt.kernel = newClientKernel(rt, newClientDB(db, nil), nil)

	_, err := TriggerTransferChunksByStrategy(context.Background(), newClientDB(db, nil), rt, TransferChunksByStrategyParams{
		DemandID: "dmd_test",
		SeedHash: "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
	})
	if err == nil {
		t.Fatalf("expected transfer by strategy to fail on incomplete runtime")
	}

	var status, errorCode, aggregateID string
	if qerr := db.QueryRow(
		`SELECT status,error_code,aggregate_id FROM proc_command_journal
		 WHERE command_type=? ORDER BY id DESC LIMIT 1`,
		clientKernelCommandTransferByStrategy,
	).Scan(&status, &errorCode, &aggregateID); qerr != nil {
		t.Fatalf("query proc_command_journal failed: %v", qerr)
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

func TestKernelDispatchRejectedWritesJournal(t *testing.T) {
	t.Parallel()

	type tc struct {
		name      string
		cmd       clientKernelCommand
		prepare   func(*clientKernel)
		errorCode string
	}
	cases := []tc{
		{
			name: "command_type_required",
			cmd: clientKernelCommand{
				RequestedBy: "test",
			},
			errorCode: "command_type_required",
		},
		{
			name: "feepool_kernel_missing",
			cmd: clientKernelCommand{
				CommandType:   clientKernelCommandFeePoolEnsureActive,
				GatewayPeerID: "16Uiu2HAmFakeGateway",
			},
			prepare:   func(k *clientKernel) { k.feePool = nil },
			errorCode: "feepool_kernel_missing",
		},
		{
			name: "invalid_stream_id",
			cmd: clientKernelCommand{
				CommandType: clientKernelCommandLivePlanPurchase,
				RequestedBy: "test",
				Payload: map[string]any{
					"stream_id": "bad_stream_id",
				},
			},
			errorCode: "invalid_stream_id",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			db := newWalletAPITestDB(t)
			rt := &Runtime{}
			rt.kernel = newClientKernel(rt, newClientDB(db, nil), nil)
			k := newClientKernel(rt, newClientDB(db, nil), nil)
			if c.prepare != nil {
				c.prepare(k)
			}
			res := k.dispatch(context.Background(), c.cmd)
			if res.Accepted {
				t.Fatalf("dispatch should be rejected")
			}
			if strings.TrimSpace(res.ErrorCode) != c.errorCode {
				t.Fatalf("dispatch error_code mismatch: got=%s want=%s", res.ErrorCode, c.errorCode)
			}

			var accepted int
			var status, gotCode string
			if err := db.QueryRow(
				`SELECT accepted,status,error_code FROM proc_command_journal
				 WHERE error_code=? ORDER BY id DESC LIMIT 1`,
				c.errorCode,
			).Scan(&accepted, &status, &gotCode); err != nil {
				t.Fatalf("query proc_command_journal failed: %v", err)
			}
			if accepted != 0 {
				t.Fatalf("journal accepted mismatch: got=%d want=0", accepted)
			}
			if strings.TrimSpace(status) != "rejected" {
				t.Fatalf("journal status mismatch: got=%s want=rejected", status)
			}
			if strings.TrimSpace(gotCode) != c.errorCode {
				t.Fatalf("journal error_code mismatch: got=%s want=%s", gotCode, c.errorCode)
			}
		})
	}
}
