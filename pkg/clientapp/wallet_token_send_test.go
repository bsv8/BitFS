package clientapp

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWalletTokenSendSubmit_FailWhenAppendTokenConsumptionFailed(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("4", 64)
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	rt.ActionChain = &feePoolKernelMockChain{}
	srv := &httpAPIServer{
		rt:    rt,
		store: newClientDB(db, nil),
	}

	// 这里故意用一笔不包含 token 转移输出的交易，触发新事实入口返回错误。
	req := walletAssetActionSubmitRequest{
		SignedTxHex: "0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000",
	}

	_, err := buildWalletTokenSendSubmit(httptest.NewRequest("POST", "/api/v1/wallet/tokens/send/submit", nil), srv, req)
	if err == nil {
		t.Fatal("expected token send submit to fail when no token carrier inputs can be written")
	}
	if !strings.Contains(err.Error(), "append token send accounting failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAppendBSV21TokenSendAccountingAfterBroadcast_ErrorWhenNoTokenCarrierInputs(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	store := newClientDB(db, nil)
	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = strings.Repeat("4", 64)
	rt := newRuntimeForTest(t, cfg, cfg.Keys.PrivkeyHex)
	err := appendBSV21TokenSendAccountingAfterBroadcast(
		httptest.NewRequest("POST", "/api/v1/wallet/tokens/send/submit", nil).Context(),
		store,
		rt,
		"0100000001000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f0100000000ffffffff02bc020000000000001976a914111111111111111111111111111111111111111188ac22010000000000001976a914222222222222222222222222222222222222222288ac00000000",
		"tx_no_token_carrier_1",
	)
	if err == nil {
		t.Fatal("expected appendBSV21TokenSendAccountingAfterBroadcast to fail when no token carrier inputs are found")
	}
	if !strings.Contains(err.Error(), "no token carrier inputs") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPrepareWalletTokenSend_FailsFastWhenUTXOSyncUnhealthy(t *testing.T) {
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
	seedTipStateForSyncGuardTest(t, db, time.Now().Unix(), "")
	seedUTXOSyncStateForSyncGuardTest(t, db, addr, time.Now().Unix(), "http 418: teapot")

	input, err := normalizeWalletTokenCreateInput("bsv21", "TST", "1000", 0, strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("normalizeWalletTokenCreateInput: %v", err)
	}

	_, err = prepareWalletTokenSend(context.Background(), newClientDB(db, nil), rt, addr, input.TokenStandard, "bsv21:test-token", "1", "mwCwTceJvYV27KXBc3NJZys6CjsgsoeHmf")
	if err == nil {
		t.Fatal("expected sync guard error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "utxo sync unhealthy") {
		t.Fatalf("unexpected error: %v", err)
	}
}
