package main

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestEncryptDecryptPrivateKeyEnvelope(t *testing.T) {
	t.Parallel()
	priv, err := generatePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := encryptPrivateKeyEnvelope("bitfs", priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	out, err := decryptPrivateKeyEnvelope("bitfs", env, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt private key failed: %v", err)
	}
	if out != priv {
		t.Fatalf("decrypt mismatch: got=%s want=%s", out, priv)
	}
	if _, err := decryptPrivateKeyEnvelope("bitfs", env, "bad-pass"); err == nil {
		t.Fatalf("expected wrong password error")
	}
}

func TestEncryptedKeyEnvelopeStore(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "runtime-config.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer db.Close()
	priv, err := generatePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := encryptPrivateKeyEnvelope("bitfs", priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	if err := saveEncryptedKeyEnvelope(db, env); err != nil {
		t.Fatalf("save envelope failed: %v", err)
	}
	loaded, exists, err := loadEncryptedKeyEnvelope(db)
	if err != nil {
		t.Fatalf("load envelope failed: %v", err)
	}
	if !exists || loaded == nil {
		t.Fatalf("expected envelope exists")
	}
	out, err := decryptPrivateKeyEnvelope("bitfs", *loaded, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt loaded envelope failed: %v", err)
	}
	if out != priv {
		t.Fatalf("loaded decrypt mismatch: got=%s want=%s", out, priv)
	}
}
