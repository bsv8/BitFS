package clientapp

import (
	"strings"
	"testing"
)

func TestClientIDFromPrivHex_StableAndLowercase(t *testing.T) {
	t.Parallel()

	privHex := "1111111111111111111111111111111111111111111111111111111111111111"
	id1, err := clientIDFromPrivHex(privHex)
	if err != nil {
		t.Fatalf("derive id #1: %v", err)
	}
	id2, err := clientIDFromPrivHex(privHex)
	if err != nil {
		t.Fatalf("derive id #2: %v", err)
	}
	if id1 == "" {
		t.Fatalf("derived client_pubkey_hex should not be empty")
	}
	if id1 != id2 {
		t.Fatalf("derived client_pubkey_hex should be stable: %q != %q", id1, id2)
	}
	if id1 != strings.ToLower(id1) {
		t.Fatalf("derived client_pubkey_hex should be lowercase: %q", id1)
	}
	if len(id1) != 66 {
		t.Fatalf("derived client_pubkey_hex should be compressed 33-byte hex, got len=%d", len(id1))
	}
	if !strings.HasPrefix(id1, "02") && !strings.HasPrefix(id1, "03") {
		t.Fatalf("derived client_pubkey_hex should start with 02/03: %q", id1)
	}
}

func TestValidateClientIdentityConsistency_Mismatch(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	cfg.ClientID, _ = clientIDFromPrivHex("2222222222222222222222222222222222222222222222222222222222222222")

	if _, err := buildClientIdentityCaps(NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)); err == nil {
		t.Fatalf("expected mismatch error, got nil")
	}
}

func TestBuildClientIdentityCaps_MismatchRejected(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	cfg.ClientID, _ = clientIDFromPrivHex("2222222222222222222222222222222222222222222222222222222222222222")

	if _, err := buildClientIdentityCaps(NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex)); err == nil {
		t.Fatalf("expected mismatch error, got nil")
	}
}

func TestBuildClientIdentityCaps_MatchAccepted(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	id, err := clientIDFromPrivHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		t.Fatalf("derive client_pubkey_hex: %v", err)
	}
	cfg.ClientID = id

	identity, err := buildClientIdentityCaps(NewRunInputFromConfig(cfg, cfg.Keys.PrivkeyHex))
	if err != nil {
		t.Fatalf("build identity: %v", err)
	}
	if identity == nil || identity.Actor == nil {
		t.Fatalf("identity should not be nil")
	}
	if identity.ClientID != id {
		t.Fatalf("client id mismatch: got=%s want=%s", identity.ClientID, id)
	}
	if identity.ClientIDLower != id {
		t.Fatalf("client id lower mismatch: got=%s want=%s", identity.ClientIDLower, id)
	}
}

func TestRuntimeIdentityMissingFails(t *testing.T) {
	t.Parallel()

	rt := &Runtime{}
	if _, err := rt.runtimeIdentity(); err == nil {
		t.Fatalf("expected runtime identity missing error, got nil")
	}
}
