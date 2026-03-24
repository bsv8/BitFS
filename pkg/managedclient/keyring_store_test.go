package managedclient

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptDecryptPrivateKeyEnvelope(t *testing.T) {
	t.Parallel()

	priv, err := GeneratePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := EncryptPrivateKeyEnvelope(priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	out, err := DecryptPrivateKeyEnvelope(env, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt private key failed: %v", err)
	}
	if out != priv {
		t.Fatalf("decrypt mismatch: got=%s want=%s", out, priv)
	}
	if _, err := DecryptPrivateKeyEnvelope(env, "bad-pass"); err == nil {
		t.Fatalf("expected wrong password error")
	}
}

func TestEncryptedKeyEnvelopeStore(t *testing.T) {
	t.Parallel()

	keyPath := filepath.Join(t.TempDir(), "key.json")
	priv, err := GeneratePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := EncryptPrivateKeyEnvelope(priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	if err := SaveEncryptedKeyEnvelope(keyPath, env); err != nil {
		t.Fatalf("save envelope failed: %v", err)
	}
	loaded, exists, err := LoadEncryptedKeyEnvelope(keyPath)
	if err != nil {
		t.Fatalf("load envelope failed: %v", err)
	}
	if !exists || loaded == nil {
		t.Fatalf("expected envelope exists")
	}
	out, err := DecryptPrivateKeyEnvelope(*loaded, "pass-123456")
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
	_, exists, err := LoadEncryptedKeyEnvelope(keyPath)
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
