package managedclient

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/chainbridge"
	"github.com/bsv8/BitFS/pkg/clientapp"
	"github.com/bsv8/WOCProxy/pkg/whatsonchain"
	"github.com/bsv8/WOCProxy/pkg/wocproxy"
)

func TestRuntimeListenOverridesApply(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	overrides := RuntimeListenOverrides{
		HTTPListenAddr:   "127.0.0.1:19181",
		FSHTTPListenAddr: "127.0.0.1:19182",
	}
	overrides.Apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:19181"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:19182"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestRuntimeListenOverridesApply_EmptyKeepsConfig(t *testing.T) {
	t.Parallel()

	cfg := clientapp.Config{}
	cfg.HTTP.ListenAddr = "127.0.0.1:18080"
	cfg.FSHTTP.ListenAddr = "127.0.0.1:18090"

	var overrides RuntimeListenOverrides
	overrides.Apply(&cfg)

	if got, want := cfg.HTTP.ListenAddr, "127.0.0.1:18080"; got != want {
		t.Fatalf("http.listen_addr=%q, want %q", got, want)
	}
	if got, want := cfg.FSHTTP.ListenAddr, "127.0.0.1:18090"; got != want {
		t.Fatalf("fs_http.listen_addr=%q, want %q", got, want)
	}
}

func TestApplyDesktopRuntimeBootstrap_EnablesStartupFullScanWhenSystemHomepageExists(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd",
		},
	}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false

	d.applyDesktopRuntimeBootstrap(&cfg)

	if !cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should be enabled for desktop system homepage bootstrap")
	}
}

func TestApplyDesktopRuntimeBootstrap_NoHomepageKeepsScanConfig(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	var cfg clientapp.Config
	cfg.Scan.StartupFullScan = false

	d.applyDesktopRuntimeBootstrap(&cfg)

	if cfg.Scan.StartupFullScan {
		t.Fatalf("startup_full_scan should stay unchanged when no system homepage bundle is active")
	}
}

func TestHandleNonAPIRequest_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/index.html", nil)
	rec := httptest.NewRecorder()

	var d managedDaemon
	d.handleNonAPIRequest(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusNotFound)
	}

	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response json: %v", err)
	}
	if payload.Error != "not found" {
		t.Fatalf("error=%q, want %q", payload.Error, "not found")
	}
}

func TestSystemHomepageBootstrapHook_AppliesMetadataAndPrice(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(root, "bootstrap.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE seeds(
		seed_hash TEXT PRIMARY KEY,
		seed_file_path TEXT NOT NULL,
		chunk_count INTEGER,
		file_size INTEGER,
		recommended_file_name TEXT NOT NULL DEFAULT '',
		mime_hint TEXT NOT NULL DEFAULT '',
		created_at_unix INTEGER
	)`); err != nil {
		t.Fatalf("create seeds: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE seed_price_state(
		seed_hash TEXT PRIMARY KEY,
		last_buy_unit_price_sat_per_64k INTEGER,
		floor_unit_price_sat_per_64k INTEGER,
		resale_discount_bps INTEGER,
		unit_price_sat_per_64k INTEGER,
		updated_at_unix INTEGER
	)`); err != nil {
		t.Fatalf("create seed_price_state: %v", err)
	}

	seedHash := "7da33adac40556fa6e5c8258f139f01f2a3fb2a22d6c651b07a12e83c04f19fd"
	seedPath := filepath.Join(root, seedHash)
	if err := os.WriteFile(seedPath, []byte("homepage"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,recommended_file_name,mime_hint,created_at_unix) VALUES(?,?,?,?,?,?,?)`,
		seedHash, seedPath, 1, 128, "", "", 1,
	); err != nil {
		t.Fatalf("insert seed: %v", err)
	}

	var cfg clientapp.Config
	cfg.Seller.Pricing.ResaleDiscountBPS = 4321

	d := &managedDaemon{
		cfg: cfg,
		systemHomepage: &systemHomepageState{
			DefaultSeedHash: seedHash,
			SeedHashes:      []string{seedHash},
			FileMetaBySeed: map[string]systemHomepageFileMeta{
				seedHash: {
					OriginalName: "index.html",
					MIME:         "text/html",
				},
			},
		},
	}

	hook := d.systemHomepageBootstrapHook()
	if hook == nil {
		t.Fatalf("expected non-nil bootstrap hook")
	}
	if err := hook(db); err != nil {
		t.Fatalf("run bootstrap hook: %v", err)
	}

	var name, mime string
	if err := db.QueryRow(`SELECT recommended_file_name,mime_hint FROM seeds WHERE seed_hash=?`, seedHash).Scan(&name, &mime); err != nil {
		t.Fatalf("query seed metadata: %v", err)
	}
	if name != "index.html" || mime != "text/html" {
		t.Fatalf("seed metadata mismatch: name=%q mime=%q", name, mime)
	}

	var floorPrice uint64
	if err := db.QueryRow(`SELECT floor_unit_price_sat_per_64k FROM seed_price_state WHERE seed_hash=?`, seedHash).Scan(&floorPrice); err != nil {
		t.Fatalf("query seed price: %v", err)
	}
	if floorPrice != systemHomepageFloorPriceSatPer64K {
		t.Fatalf("seed floor price mismatch: got=%d want=%d", floorPrice, systemHomepageFloorPriceSatPer64K)
	}
}

func TestResolveChainAccessState_DefaultsToEmbeddedProxy(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "proxy"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if !state.WOCProxyEnabled {
		t.Fatal("expected embedded proxy to be enabled")
	}
	if got, want := state.WOCProxyAddr, managedWOCProxyListenAddr; got != want {
		t.Fatalf("woc proxy addr=%q, want %q", got, want)
	}
	if got, want := state.BaseURL, wocproxy.BaseURLForNetwork("http://"+managedWOCProxyListenAddr, "test"); got != want {
		t.Fatalf("base_url=%q, want %q", got, want)
	}
	if got, want := state.MinInterval, time.Second; got != want {
		t.Fatalf("min_interval=%s, want %s", got, want)
	}
}

func TestResolveChainAccessState_UsesInjectedEnvBaseURL(t *testing.T) {
	t.Setenv(chainbridge.FeePoolChainBaseURLEnv, " http://127.0.0.1:33333/v1/bsv/test/ ")

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "injected_env"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if state.WOCProxyEnabled {
		t.Fatal("expected embedded proxy to be disabled when env is injected")
	}
	if got, want := state.BaseURL, "http://127.0.0.1:33333/v1/bsv/test"; got != want {
		t.Fatalf("base_url=%q, want %q", got, want)
	}
	if got, want := state.WOCProxyAddr, ""; got != want {
		t.Fatalf("woc_proxy_addr=%q, want empty", got)
	}
	if state.RouteAuth != (chainbridge.AuthConfig{}) {
		t.Fatalf("route_auth=%+v, want empty", state.RouteAuth)
	}
	if state.WalletAuth != (whatsonchain.AuthConfig{}) {
		t.Fatalf("wallet_auth=%+v, want empty", state.WalletAuth)
	}
}

func TestResolveChainAccessState_UsesPerProviderWOCSettings(t *testing.T) {
	t.Parallel()

	d := &managedDaemon{}
	d.cfg.BSV.Network = "test"
	d.cfg.ExternalAPI.WOC.APIKey = "provider-key"
	d.cfg.ExternalAPI.WOC.MinIntervalMS = 2500

	state := d.resolveChainAccessState()
	if got, want := state.Mode, "direct_woc"; got != want {
		t.Fatalf("mode=%q, want %q", got, want)
	}
	if got, want := state.RouteAuth.Value, "provider-key"; got != want {
		t.Fatalf("route_auth.value=%q, want %q", got, want)
	}
	if got, want := state.MinInterval, 2500*time.Millisecond; got != want {
		t.Fatalf("min_interval=%s, want %s", got, want)
	}
}
