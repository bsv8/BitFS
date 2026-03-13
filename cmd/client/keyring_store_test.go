package main

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/bsv8/BitFS/pkg/clientapp"
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

func TestLoadEncryptedKeyEnvelope_MigratesAndDropsLegacyKeyringTable(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "runtime-config.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE keyring_singleton(
		id INTEGER PRIMARY KEY CHECK(id=1),
		cipher_json TEXT NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create legacy keyring table failed: %v", err)
	}
	priv, err := generatePrivateKeyHex()
	if err != nil {
		t.Fatalf("generate private key failed: %v", err)
	}
	env, err := encryptPrivateKeyEnvelope("bitfs", priv, "pass-123456")
	if err != nil {
		t.Fatalf("encrypt private key failed: %v", err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope failed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO keyring_singleton(id,cipher_json,updated_at_unix) VALUES(1,?,?)`, string(raw), int64(456)); err != nil {
		t.Fatalf("insert legacy keyring row failed: %v", err)
	}

	loaded, exists, err := loadEncryptedKeyEnvelope(db)
	if err != nil {
		t.Fatalf("load envelope failed: %v", err)
	}
	if !exists || loaded == nil {
		t.Fatalf("expected envelope exists after migration")
	}
	out, err := decryptPrivateKeyEnvelope("bitfs", *loaded, "pass-123456")
	if err != nil {
		t.Fatalf("decrypt loaded envelope failed: %v", err)
	}
	if out != priv {
		t.Fatalf("loaded decrypt mismatch: got=%s want=%s", out, priv)
	}
	_, exists, err = clientapp.LoadAppConfigValue(db, clientapp.AppConfigKeyEncryptionMasterKeyEnvelope)
	if err != nil {
		t.Fatalf("load app_config key failed: %v", err)
	}
	if !exists {
		t.Fatalf("expected envelope migrated into app_config")
	}
	var one int
	err = db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='keyring_singleton' LIMIT 1`).Scan(&one)
	if err == nil {
		t.Fatalf("legacy keyring_singleton table should be dropped")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("query sqlite_master failed: %v", err)
	}
}
