package clientapp

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigInDB_DoesNotPersistIndexSection(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cfg.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}

	cfg := Config{}
	cfg.BSV.Network = "test"
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Index.Backend = "sqlite"
	cfg.Index.SQLitePath = filepath.Join(t.TempDir(), "runtime-config.sqlite")
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	if err := ApplyConfigDefaults(&cfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if err := SaveConfigInDB(db, cfg); err != nil {
		t.Fatalf("save cfg: %v", err)
	}

	var raw string
	if err := db.QueryRow(`SELECT value FROM app_config WHERE key=?`, AppConfigKeyRuntimeConfigTOML).Scan(&raw); err != nil {
		t.Fatalf("query app config: %v", err)
	}
	if strings.Contains(raw, "[index]") {
		t.Fatalf("index section should not be persisted: %s", raw)
	}
	if strings.Contains(raw, "sqlite_path") {
		t.Fatalf("index sqlite_path should not be persisted: %s", raw)
	}
}

func TestLoadOrInitConfigInDB_FillsDerivedIndex(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "runtime-config.sqlite")
	defaultCfg := Config{}
	defaultCfg.BSV.Network = "test"
	defaultCfg.Storage.WorkspaceDir = t.TempDir()
	defaultCfg.Storage.DataDir = t.TempDir()
	defaultCfg.Index.Backend = "sqlite"
	defaultCfg.Index.SQLitePath = dbPath
	defaultCfg.HTTP.ListenAddr = "127.0.0.1:18080"
	defaultCfg.FSHTTP.ListenAddr = "127.0.0.1:18090"
	defaultCfg.FSHTTP.MaxConcurrentSessions = 4
	defaultCfg.FSHTTP.DownloadWaitTimeoutSeconds = 10
	if err := ApplyConfigDefaults(&defaultCfg); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}

	first, created, err := LoadOrInitConfigInDB(dbPath, defaultCfg)
	if err != nil {
		t.Fatalf("first load/init: %v", err)
	}
	if !created {
		t.Fatalf("first load should create default config")
	}
	if first.Index.Backend != "sqlite" || first.Index.SQLitePath != dbPath {
		t.Fatalf("first index mismatch: %#v", first.Index)
	}

	second, created2, err := LoadOrInitConfigInDB(dbPath, defaultCfg)
	if err != nil {
		t.Fatalf("second load/init: %v", err)
	}
	if created2 {
		t.Fatalf("second load should not create config again")
	}
	if second.Index.Backend != "sqlite" || second.Index.SQLitePath != dbPath {
		t.Fatalf("second index mismatch: %#v", second.Index)
	}
}

func TestEnsureAppConfigKVSchema_MigratesLegacySingleRow(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "legacy-runtime.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE app_config(
		id INTEGER PRIMARY KEY CHECK(id=1),
		config_toml TEXT NOT NULL,
		updated_at_unix INTEGER NOT NULL
	)`); err != nil {
		t.Fatalf("create legacy app_config: %v", err)
	}
	legacyRaw := "[bsv]\nnetwork=\"test\"\n"
	if _, err := db.Exec(`INSERT INTO app_config(id,config_toml,updated_at_unix) VALUES(1,?,?)`, legacyRaw, int64(123)); err != nil {
		t.Fatalf("insert legacy app_config row: %v", err)
	}

	if err := EnsureAppConfigKVSchema(db); err != nil {
		t.Fatalf("ensure app_config kv schema: %v", err)
	}
	gotRaw, exists, err := LoadAppConfigValue(db, AppConfigKeyRuntimeConfigTOML)
	if err != nil {
		t.Fatalf("load migrated runtime config: %v", err)
	}
	if !exists {
		t.Fatalf("expected migrated runtime config exists")
	}
	if gotRaw != legacyRaw {
		t.Fatalf("migrated runtime config mismatch: got=%q want=%q", gotRaw, legacyRaw)
	}
}
