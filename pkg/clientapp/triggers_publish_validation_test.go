package clientapp

import (
	"strings"
	"testing"

	"github.com/bsv8/BFTP/pkg/feepool/dual2of2"
)

func TestValidateDemandPublishPaidResp(t *testing.T) {
	t.Run("success false", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success: false,
			Status:  "active",
			Error:   "charge rejected",
		})
		if err == nil {
			t.Fatalf("expected error when success=false")
		}
		if !strings.Contains(err.Error(), "gateway demand publish rejected") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with empty demand id", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "",
		})
		if err == nil {
			t.Fatalf("expected error when demand_id is empty")
		}
		if !strings.Contains(err.Error(), "empty demand_id") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with demand id", func(t *testing.T) {
		err := validateDemandPublishPaidResp(dual2of2.DemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "dmd_x1",
		})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})
}

func TestValidateLiveDemandPublishPaidResp(t *testing.T) {
	t.Run("success false", func(t *testing.T) {
		err := validateLiveDemandPublishPaidResp(dual2of2.LiveDemandPublishPaidResp{
			Success: false,
			Status:  "active",
			Error:   "charge rejected",
		})
		if err == nil {
			t.Fatalf("expected error when success=false")
		}
		if !strings.Contains(err.Error(), "gateway live demand publish rejected") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("success true with empty demand id", func(t *testing.T) {
		err := validateLiveDemandPublishPaidResp(dual2of2.LiveDemandPublishPaidResp{
			Success:  true,
			Status:   "ok",
			DemandID: "",
		})
		if err == nil {
			t.Fatalf("expected error when demand_id is empty")
		}
		if !strings.Contains(err.Error(), "empty demand_id") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestApplyFeePoolChargeToSession(t *testing.T) {
	t.Run("advance session by next seq and server amount", func(t *testing.T) {
		sess := &feePoolSession{
			PoolAmountSat: 30,
			SpendTxFeeSat: 1,
			Sequence:      2,
			ServerAmount:  10,
			ClientAmount:  19,
			CurrentTxHex:  "old_tx_hex",
		}
		applyFeePoolChargeToSession(sess, 3, 15, "new_tx_hex")
		if sess.Sequence != 3 {
			t.Fatalf("sequence mismatch: got=%d want=3", sess.Sequence)
		}
		if sess.ServerAmount != 15 {
			t.Fatalf("server_amount mismatch: got=%d want=15", sess.ServerAmount)
		}
		if sess.ClientAmount != 14 {
			t.Fatalf("client_amount mismatch: got=%d want=14", sess.ClientAmount)
		}
		if sess.CurrentTxHex != "new_tx_hex" {
			t.Fatalf("current_tx_hex mismatch: got=%s want=new_tx_hex", sess.CurrentTxHex)
		}
	})

	t.Run("clamp client amount when pool insufficient", func(t *testing.T) {
		sess := &feePoolSession{
			PoolAmountSat: 5,
			SpendTxFeeSat: 2,
			Sequence:      1,
			ServerAmount:  1,
			ClientAmount:  2,
		}
		applyFeePoolChargeToSession(sess, 2, 10, "tx")
		if sess.ClientAmount != 0 {
			t.Fatalf("client_amount should clamp to zero, got=%d", sess.ClientAmount)
		}
	})
}
