package clientapp

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfigInDB_PersistsOneKeyPerField(t *testing.T) {
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

	var runtimeTomlCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM app_config WHERE key='runtime_config_toml'`).Scan(&runtimeTomlCount); err != nil {
		t.Fatalf("query runtime_config_toml count: %v", err)
	}
	if runtimeTomlCount != 0 {
		t.Fatalf("runtime_config_toml should be removed, got=%d", runtimeTomlCount)
	}
	var httpListen string
	if err := db.QueryRow(`SELECT value FROM app_config WHERE key='http.listen_addr'`).Scan(&httpListen); err != nil {
		t.Fatalf("query http.listen_addr: %v", err)
	}
	if httpListen != "127.0.0.1:18080" {
		t.Fatalf("http.listen_addr mismatch: got=%q", httpListen)
	}
	var indexCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM app_config WHERE key IN ('index.backend','index.sqlite_path')`).Scan(&indexCount); err != nil {
		t.Fatalf("query index key count: %v", err)
	}
	if indexCount != 0 {
		t.Fatalf("index keys should not be persisted, got=%d", indexCount)
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

func TestEnsureAppConfigKVSchema_RejectsLegacySingleRow(t *testing.T) {
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
	if _, err := db.Exec(`INSERT INTO app_config(id,config_toml,updated_at_unix) VALUES(1,?,?)`, "[bsv]\nnetwork=\"test\"\n", int64(123)); err != nil {
		t.Fatalf("insert legacy app_config row: %v", err)
	}

	err = EnsureAppConfigKVSchema(db)
	if err == nil {
		t.Fatalf("expected legacy schema rejected")
	}
	if !strings.Contains(err.Error(), "unsupported app_config schema") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitIndexDB_DirectTransferPoolsColumnsUnique(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}

	rows, err := db.Query(`PRAGMA table_info(direct_transfer_pools)`)
	if err != nil {
		t.Fatalf("query table_info: %v", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		counts[strings.ToLower(strings.TrimSpace(name))]++
	}
	if counts["buyer_pubkey_hex"] != 1 {
		t.Fatalf("buyer_pubkey_hex should appear once, got=%d", counts["buyer_pubkey_hex"])
	}
	if counts["seller_pubkey_hex"] != 1 {
		t.Fatalf("seller_pubkey_hex should appear once, got=%d", counts["seller_pubkey_hex"])
	}
	if counts["arbiter_pubkey_hex"] != 1 {
		t.Fatalf("arbiter_pubkey_hex should appear once, got=%d", counts["arbiter_pubkey_hex"])
	}
}
