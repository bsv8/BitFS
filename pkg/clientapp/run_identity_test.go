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

	if err := validateClientIdentityConsistency(cfg); err == nil {
		t.Fatalf("expected mismatch error, got nil")
	}
}

func TestBuildClientActorFromConfig_MismatchRejected(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	cfg.ClientID, _ = clientIDFromPrivHex("2222222222222222222222222222222222222222222222222222222222222222")

	if _, err := buildClientActorFromConfig(cfg); err == nil {
		t.Fatalf("expected mismatch error, got nil")
	}
}

func TestBuildClientActorFromConfig_MatchAccepted(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Keys.PrivkeyHex = "1111111111111111111111111111111111111111111111111111111111111111"
	id, err := clientIDFromPrivHex(cfg.Keys.PrivkeyHex)
	if err != nil {
		t.Fatalf("derive client_pubkey_hex: %v", err)
	}
	cfg.ClientID = id

	actor, err := buildClientActorFromConfig(cfg)
	if err != nil {
		t.Fatalf("build actor: %v", err)
	}
	if actor == nil {
		t.Fatalf("actor should not be nil")
	}
}
