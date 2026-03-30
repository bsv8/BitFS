package main

import "testing"

func TestResolveCLIAction_New(t *testing.T) {
	t.Parallel()

	action, err := resolveCLIAction(cliOptions{newKey: true})
	if err != nil {
		t.Fatalf("resolveCLIAction failed: %v", err)
	}
	if action != actionNew {
		t.Fatalf("action mismatch: got=%q want=%q", action, actionNew)
	}
}

func TestResolveCLIAction_MutuallyExclusive(t *testing.T) {
	t.Parallel()

	_, err := resolveCLIAction(cliOptions{
		newKey:     true,
		importPath: "/tmp/key.json",
	})
	if err == nil {
		t.Fatalf("expected mutually exclusive error")
	}
}
