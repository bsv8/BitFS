package clientapp

import "testing"

func TestResolveFeePoolMaintainCommandType(t *testing.T) {
	t.Parallel()

	rt := &Runtime{
		feePools: map[string]*feePoolSession{},
	}
	gwID := "gw-1"

	if got := resolveFeePoolMaintainCommandType(rt, gwID); got != feePoolCommandEnsureActive {
		t.Fatalf("resolve maintain type mismatch when missing session: got=%s want=%s", got, feePoolCommandEnsureActive)
	}

	rt.setFeePool(gwID, &feePoolSession{
		GatewayPeerID: gwID,
		Status:        "active",
		SpendTxID:     "spend-txid-1",
	})
	if got := resolveFeePoolMaintainCommandType(rt, gwID); got != feePoolCommandCycleTick {
		t.Fatalf("resolve maintain type mismatch when active session exists: got=%s want=%s", got, feePoolCommandCycleTick)
	}

	rt.setFeePool(gwID, &feePoolSession{
		GatewayPeerID: gwID,
		Status:        "retired",
		SpendTxID:     "spend-txid-1",
	})
	if got := resolveFeePoolMaintainCommandType(rt, gwID); got != feePoolCommandEnsureActive {
		t.Fatalf("resolve maintain type mismatch when session inactive: got=%s want=%s", got, feePoolCommandEnsureActive)
	}
}
