package clientapp

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWalletTokenSendSubmit_FailWhenAppendTokenConsumptionFailed(t *testing.T) {
	t.Parallel()

	db := newWalletAccountingTestDB(t)
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: strings.Repeat("4", 64),
		},
		ActionChain: &feePoolKernelMockChain{},
	}
	rt.runIn.BSV.Network = "test"
	mustSetRuntimeIdentityFromRunIn(t, rt)
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
	rt := &Runtime{
		runIn: RunInput{
			EffectivePrivKeyHex: strings.Repeat("4", 64),
		},
	}
	rt.runIn.BSV.Network = "test"
	mustSetRuntimeIdentityFromRunIn(t, rt)
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
