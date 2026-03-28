package main

import "testing"

func TestResolveCLIAction_WalletTokenBalances(t *testing.T) {
	t.Parallel()

	action, err := resolveCLIAction(cliOptions{walletTokenBalances: true})
	if err != nil {
		t.Fatalf("resolveCLIAction failed: %v", err)
	}
	if action != actionWalletTokenBalances {
		t.Fatalf("action mismatch: got=%q want=%q", action, actionWalletTokenBalances)
	}
}

func TestResolveCLIAction_QueryFlagsMutuallyExclusive(t *testing.T) {
	t.Parallel()

	_, err := resolveCLIAction(cliOptions{
		walletTokenBalances: true,
		walletOrdinals:      true,
	})
	if err == nil {
		t.Fatalf("expected mutually exclusive error")
	}
}
