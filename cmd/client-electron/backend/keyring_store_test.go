package main

import (
	"path/filepath"
	"testing"

	"os"
)

func TestEncryptDecryptPrivateKeyEnvelope(t *testing.T) {
	t.Parallel()
	priv, err := generatePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := encryptPrivateKeyEnvelope(priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	out, err := decryptPrivateKeyEnvelope(env, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt private key failed: %v", err)
	}
	if out != priv {
		t.Fatalf("decrypt mismatch: got=%s want=%s", out, priv)
	}
	if _, err := decryptPrivateKeyEnvelope(env, "bad-pass"); err == nil {
		t.Fatalf("expected wrong password error")
	}
}

func TestEncryptedKeyEnvelopeStore(t *testing.T) {
	t.Parallel()
	keyPath := filepath.Join(t.TempDir(), "key.json")
	priv, err := generatePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := encryptPrivateKeyEnvelope(priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	if err := saveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save envelope failed: %v", err)
	}
	loaded, exists, err := loadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		t.Fatalf("load envelope failed: %v", err)
	}
	if !exists || loaded == nil {
		t.Fatalf("expected envelope exists")
	}
	out, err := decryptPrivateKeyEnvelope(*loaded, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt loaded envelope failed: %v", err)
	}
	if out != priv {
		t.Fatalf("loaded decrypt mismatch: got=%s want=%s", out, priv)
	}
}

func TestLoadEncryptedKeyEnvelope_MissingFile(t *testing.T) {
	t.Parallel()
	keyPath := filepath.Join(t.TempDir(), "missing-key.json")
	_, exists, err := loadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		t.Fatalf("load missing envelope failed: %v", err)
	}
	if exists {
		t.Fatalf("expected missing key file")
	}
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("missing key file should not exist, stat err=%v", err)
	}
}
