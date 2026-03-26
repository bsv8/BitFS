package clientapp

import "testing"

func TestApplyDomainRegisterSubmitResultFailureClearsOkAndUppercasesCode(t *testing.T) {
	t.Parallel()

	base := domainRegisterNameResultFromPrepared(TriggerDomainPrepareRegisterResult{
		Ok:                         true,
		Code:                       "PREPARED",
		Name:                       "example.eth",
		OwnerPubkeyHex:             "owner",
		TargetPubkeyHex:            "target",
		RegisterTxID:               "prepared_txid",
		QueryFeeChargedSatoshi:     1,
		RegisterLockChargedSatoshi: 1,
		RegisterPriceSatoshi:       1,
		RegisterSubmitFeeSatoshi:   1,
		TotalRegisterPaySatoshi:    2,
	})

	got := applyDomainRegisterSubmitResult(base, TriggerDomainSubmitPreparedRegisterResult{
		Ok:      false,
		Code:    "broadcast_failed",
		Message: "forced reject",
	})

	if got.Ok {
		t.Fatalf("submit failure should clear ok flag")
	}
	if got.Code != "BROADCAST_FAILED" {
		t.Fatalf("code mismatch: got=%s want=BROADCAST_FAILED", got.Code)
	}
	if got.Message != "forced reject" {
		t.Fatalf("message mismatch: got=%s want=forced reject", got.Message)
	}
	if got.RegisterTxID != "prepared_txid" {
		t.Fatalf("prepared register txid should be preserved on submit failure: got=%s", got.RegisterTxID)
	}
}

func TestApplyDomainRegisterSubmitResultSuccessMarksOk(t *testing.T) {
	t.Parallel()

	base := domainRegisterNameResultFromPrepared(TriggerDomainPrepareRegisterResult{
		Ok:              true,
		Code:            "PREPARED",
		Name:            "example.eth",
		OwnerPubkeyHex:  "owner_before",
		TargetPubkeyHex: "target_before",
		RegisterTxID:    "prepared_txid",
	})

	got := applyDomainRegisterSubmitResult(base, TriggerDomainSubmitPreparedRegisterResult{
		Ok:              true,
		Name:            "example.eth",
		OwnerPubkeyHex:  "owner_after",
		TargetPubkeyHex: "target_after",
		ExpireAtUnix:    123,
		RegisterTxID:    "final_txid",
	})

	if !got.Ok {
		t.Fatalf("submit success should mark ok")
	}
	if got.Code != "OK" {
		t.Fatalf("code mismatch: got=%s want=OK", got.Code)
	}
	if got.OwnerPubkeyHex != "owner_after" {
		t.Fatalf("owner mismatch: got=%s want=owner_after", got.OwnerPubkeyHex)
	}
	if got.TargetPubkeyHex != "target_after" {
		t.Fatalf("target mismatch: got=%s want=target_after", got.TargetPubkeyHex)
	}
	if got.RegisterTxID != "final_txid" {
		t.Fatalf("register txid mismatch: got=%s want=final_txid", got.RegisterTxID)
	}
	if got.ExpireAtUnix != 123 {
		t.Fatalf("expire_at mismatch: got=%d want=123", got.ExpireAtUnix)
	}
}
