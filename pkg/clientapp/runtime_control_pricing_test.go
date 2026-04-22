package clientapp

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCurrentLiveSellerPricingReadsPersistedBase(t *testing.T) {
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "pricing.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	cfg := Config{}
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	cfg.Seller.Pricing.LiveFloorPriceSatPer64K = 900
	cfg.Seller.Pricing.LiveDecayPerMinuteBPS = 12

	if err := dbUpsertPricingAutopilotConfig(t.Context(), NewClientStore(db, nil), PricingConfig{BasePriceSatPer64K: 1600}, time.Now().Unix()); err != nil {
		t.Fatalf("upsert pricing config: %v", err)
	}

	got, err := currentLiveSellerPricing(t.Context(), NewClientStore(db, nil), cfg)
	if err != nil {
		t.Fatalf("current live seller pricing: %v", err)
	}
	if got.BasePriceSatPer64K != 1600 {
		t.Fatalf("base price=%d, want 1600", got.BasePriceSatPer64K)
	}
	if got.FloorPriceSatPer64K != 900 {
		t.Fatalf("floor price=%d, want 900", got.FloorPriceSatPer64K)
	}
	if got.DecayPerMinuteBPS != 12 {
		t.Fatalf("decay=%d, want 12", got.DecayPerMinuteBPS)
	}
}

func TestCurrentSeedLiveSellerPricingReadsPersistedEffectivePrice(t *testing.T) {
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "pricing.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	seedHash := strings.Repeat("ef", 32)
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, filepath.Join(root, "seed.bse"), 4, 1024, "seed.bin", "application/octet-stream"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	cfg := Config{}
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	cfg.Seller.Pricing.LiveFloorPriceSatPer64K = 900
	cfg.Seller.Pricing.LiveDecayPerMinuteBPS = 12

	state := pricingStateSeedSnapshot(sellerSeed{SeedHash: seedHash}, 1600)
	state.EffectivePriceSatPer64K = 7777
	if err := dbUpsertPricingAutopilotState(t.Context(), NewClientStore(db, nil), state, time.Now().Unix()); err != nil {
		t.Fatalf("upsert state: %v", err)
	}

	got, err := currentSeedLiveSellerPricing(t.Context(), NewClientStore(db, nil), cfg, seedHash)
	if err != nil {
		t.Fatalf("current seed live seller pricing: %v", err)
	}
	if got.BasePriceSatPer64K != 7777 {
		t.Fatalf("base price=%d, want 7777", got.BasePriceSatPer64K)
	}
	if got.FloorPriceSatPer64K != 900 {
		t.Fatalf("floor price=%d, want 900", got.FloorPriceSatPer64K)
	}
	if got.DecayPerMinuteBPS != 12 {
		t.Fatalf("decay=%d, want 12", got.DecayPerMinuteBPS)
	}

	quoted := ComputeLiveQuotePrices(sellerSeed{SeedHash: seedHash, ChunkCount: 3}, liveSegmentMeta{PublishedAtUnix: time.Now().Unix()}, got, time.Now())
	if quoted.ChunkPrice != 7777 {
		t.Fatalf("quoted chunk price=%d, want 7777", quoted.ChunkPrice)
	}
	if quoted.SeedPrice != 23331 {
		t.Fatalf("quoted seed price=%d, want 23331", quoted.SeedPrice)
	}
}

func TestTriggerPricingGetStateSynthesizesSeedWithoutPriorState(t *testing.T) {
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "pricing.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	seedHash := strings.Repeat("ab", 32)
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, filepath.Join(root, "seed.bse"), 4, 1024, "seed.bin", "application/octet-stream"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	cfg := Config{}
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1700
	rt, err := NewPricingTestRuntime(t.Context(), db, cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	state, err := TriggerPricingGetState(t.Context(), rt, seedHash)
	if err != nil {
		t.Fatalf("get pricing state: %v", err)
	}
	if state.SeedHash != seedHash {
		t.Fatalf("seed hash=%q, want %q", state.SeedHash, seedHash)
	}
	if state.BasePriceSatPer64K != 1700 {
		t.Fatalf("base price=%d, want 1700", state.BasePriceSatPer64K)
	}
	if state.EffectivePriceSatPer64K != 1700 {
		t.Fatalf("effective price=%d, want 1700", state.EffectivePriceSatPer64K)
	}
}

func TestPricingStateAndAuditsPersistAcrossRuntimeRecreate(t *testing.T) {
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "pricing.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	seedHash := strings.Repeat("cd", 32)
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, filepath.Join(root, "seed.bse"), 4, 1024, "seed.bin", "application/octet-stream"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	cfg := Config{}
	cfg.Storage.DataDir = filepath.Join(root, "data")
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	rt1, err := NewPricingTestRuntime(t.Context(), db, cfg)
	if err != nil {
		t.Fatalf("new runtime 1: %v", err)
	}

	if _, err := TriggerPricingSetBase(t.Context(), rt1, 1600); err != nil {
		t.Fatalf("set base: %v", err)
	}
	if _, err := TriggerPricingResetSeed(t.Context(), rt1, seedHash); err != nil {
		t.Fatalf("reset seed: %v", err)
	}
	if _, err := TriggerPricingFeedSeed(t.Context(), rt1, PricingFeedRequest{SeedHash: seedHash, QueryCount: 40, DealCount: 35}); err != nil {
		t.Fatalf("feed seed: %v", err)
	}
	if _, err := TriggerPricingRunTick(t.Context(), rt1, 1); err != nil {
		t.Fatalf("run tick: %v", err)
	}

	state1, err := TriggerPricingGetState(t.Context(), rt1, seedHash)
	if err != nil {
		t.Fatalf("get state 1: %v", err)
	}
	audits1, err := TriggerPricingGetAudits(t.Context(), rt1, seedHash, 20)
	if err != nil {
		t.Fatalf("get audits 1: %v", err)
	}
	if len(audits1) == 0 {
		t.Fatal("expected audits after tick")
	}

	rt2, err := NewPricingTestRuntime(t.Context(), db, cfg)
	if err != nil {
		t.Fatalf("new runtime 2: %v", err)
	}
	state2, err := TriggerPricingGetState(t.Context(), rt2, seedHash)
	if err != nil {
		t.Fatalf("get state 2: %v", err)
	}
	audits2, err := TriggerPricingGetAudits(t.Context(), rt2, seedHash, 20)
	if err != nil {
		t.Fatalf("get audits 2: %v", err)
	}
	if state2.BasePriceSatPer64K != state1.BasePriceSatPer64K {
		t.Fatalf("base price=%d, want %d", state2.BasePriceSatPer64K, state1.BasePriceSatPer64K)
	}
	if state2.EffectivePriceSatPer64K != state1.EffectivePriceSatPer64K {
		t.Fatalf("effective price=%d, want %d", state2.EffectivePriceSatPer64K, state1.EffectivePriceSatPer64K)
	}
	if len(audits2) == 0 {
		t.Fatal("expected audits after runtime recreate")
	}
	if audits2[0].ReasonCode == "" {
		t.Fatal("audit reason code should not be empty")
	}
}

func TestTriggerPricingSetBasePersistsConfigAndSurvivesReload(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "runtime-config.yaml")
	db, err := sql.Open("sqlite", filepath.Join(root, "pricing.sqlite"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	seedHash := strings.Repeat("aa", 32)
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		seedHash, filepath.Join(root, "seed.bse"), 4, 1024, "seed.bin", "application/octet-stream"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	cfg := Config{}
	cfg.Storage.DataDir = filepath.Join(root, "data")
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	rt := newRuntimeForTest(t, cfg, "", withRuntimeConfigPath(configPath))
	rt.store = newClientDB(db, nil)
	rt.Catalog = newSellerCatalog()
	if seeds, err := dbLoadSeedCatalogSnapshot(t.Context(), rt.store); err != nil {
		t.Fatalf("load seed catalog snapshot: %v", err)
	} else {
		rt.Catalog.Replace(seeds)
	}

	if _, err := TriggerPricingSetBase(t.Context(), rt, 1600); err != nil {
		t.Fatalf("set base: %v", err)
	}
	if _, err := TriggerPricingResetSeed(t.Context(), rt, seedHash); err != nil {
		t.Fatalf("reset seed: %v", err)
	}
	if _, err := TriggerPricingFeedSeed(t.Context(), rt, PricingFeedRequest{SeedHash: seedHash, QueryCount: 40, DealCount: 35}); err != nil {
		t.Fatalf("feed seed: %v", err)
	}
	if _, err := TriggerPricingRunTick(t.Context(), rt, 1); err != nil {
		t.Fatalf("run tick: %v", err)
	}

	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got, want := loaded.Seller.Pricing.LiveBasePriceSatPer64K, uint64(1600); got != want {
		t.Fatalf("config file base=%d, want %d", got, want)
	}

	rt2, err := NewPricingTestRuntime(t.Context(), db, loaded)
	if err != nil {
		t.Fatalf("new runtime from loaded config: %v", err)
	}
	gotCfg, err := TriggerPricingGetConfig(rt2)
	if err != nil {
		t.Fatalf("get config after reload: %v", err)
	}
	if gotCfg.BasePriceSatPer64K != 1600 {
		t.Fatalf("reloaded config base=%d, want 1600", gotCfg.BasePriceSatPer64K)
	}
	if _, err := TriggerPricingSetBase(t.Context(), rt2, 1900); err != nil {
		t.Fatalf("set base after reload: %v", err)
	}
	state, err := TriggerPricingGetState(t.Context(), rt2, seedHash)
	if err != nil {
		t.Fatalf("get state after reload: %v", err)
	}
	if state.BasePriceSatPer64K != 1900 {
		t.Fatalf("reloaded state base=%d, want 1900", state.BasePriceSatPer64K)
	}
	if state.EffectivePriceSatPer64K != 1900 {
		t.Fatalf("reloaded state effective=%d, want 1900", state.EffectivePriceSatPer64K)
	}
}

func TestTriggerPricingSetBaseRejectsMissingConfigPathInProductMode(t *testing.T) {
	cfg := Config{}
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	svc, err := newRuntimeConfigService(cfg, "", StartupModeProduct)
	if err != nil {
		t.Fatalf("new runtime config service: %v", err)
	}
	rt := &Runtime{
		ctx:    t.Context(),
		config: svc,
	}

	before := rt.ConfigSnapshot()
	if _, err := TriggerPricingSetBase(t.Context(), rt, 1600); err == nil {
		t.Fatal("set base should fail without config path in product mode")
	} else if err.Error() != "config path is required in product mode" {
		t.Fatalf("error=%q, want product mode config path failure", err.Error())
	}
	after := rt.ConfigSnapshot()
	if after.Seller.Pricing.LiveBasePriceSatPer64K != before.Seller.Pricing.LiveBasePriceSatPer64K {
		t.Fatalf("base changed on failure: before=%d after=%d", before.Seller.Pricing.LiveBasePriceSatPer64K, after.Seller.Pricing.LiveBasePriceSatPer64K)
	}
}

func TestTriggerPricingSetBaseRollsBackConfigOnDBFailure(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "runtime-config.yaml")
	driverName := "pricing-set-base-fail-" + strings.ReplaceAll(t.Name(), "/", "_") + "-" + time.Now().Format("150405.000000000")
	sql.Register(driverName, pricingSetBaseFailDriver{})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open failing db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := Config{}
	cfg.Seller.Pricing.LiveBasePriceSatPer64K = 1000
	rt := newRuntimeForTest(t, cfg, "", withRuntimeConfigPath(configPath))
	rt.store = newClientDB(db, nil)

	if _, err := TriggerPricingSetBase(t.Context(), rt, 1600); err == nil {
		t.Fatal("set base should fail when db update fails")
	}

	loaded, _, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("load rolled back config: %v", err)
	}
	if loaded.Seller.Pricing.LiveBasePriceSatPer64K != 1000 {
		t.Fatalf("rolled back file base=%d, want 1000", loaded.Seller.Pricing.LiveBasePriceSatPer64K)
	}
	if got := rt.ConfigSnapshot().Seller.Pricing.LiveBasePriceSatPer64K; got != 1000 {
		t.Fatalf("rolled back memory base=%d, want 1000", got)
	}
}

type pricingSetBaseFailDriver struct{}

func (pricingSetBaseFailDriver) Open(name string) (driver.Conn, error) {
	return pricingSetBaseFailConn{}, nil
}

type pricingSetBaseFailConn struct{}

func (pricingSetBaseFailConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare not supported")
}

func (pricingSetBaseFailConn) Close() error { return nil }

func (pricingSetBaseFailConn) Begin() (driver.Tx, error) {
	return pricingSetBaseFailTx{}, nil
}

func (pricingSetBaseFailConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return pricingSetBaseFailTx{}, nil
}

func (pricingSetBaseFailConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("forced db failure")
}

type pricingSetBaseFailTx struct{}

func (pricingSetBaseFailTx) Commit() error   { return nil }
func (pricingSetBaseFailTx) Rollback() error { return nil }
