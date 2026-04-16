package clientapp

import (
	"context"
	"testing"
)

func TestCollectScriptSpendableUTXOs_MissingCapabilityReturnsError(t *testing.T) {
	t.Parallel()

	chain := testWalletChainClient{baseURL: "http://127.0.0.1:1"}
	_, err := collectScriptSpendableUTXOs(context.Background(), chain, []string{"abcd"})
	if err == nil {
		t.Fatal("expected error when chain lacks script spendable capability")
	}
}
