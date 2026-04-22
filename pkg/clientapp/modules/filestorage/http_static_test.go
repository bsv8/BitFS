package filestorage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/BitFS/pkg/clientapp/seedstorage"
	"github.com/libp2p/go-libp2p/core/protocol"
	_ "modernc.org/sqlite"
)

type testStore struct {
	db *sql.DB
}

func (s testStore) Read(ctx context.Context, fn func(moduleapi.ReadConn) error) error {
	return fn(s.db)
}

func (s testStore) WriteTx(ctx context.Context, fn func(moduleapi.WriteTx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(testWriteTx{Tx: tx}); err != nil {
		return err
	}
	return tx.Commit()
}

type testWriteTx struct {
	Tx *sql.Tx
}

func (t testWriteTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, query, args...)
}

func (t testWriteTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, query, args...)
}

func (t testWriteTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func (t testWriteTx) QueryRow(query string, args ...any) *sql.Row {
	return t.Tx.QueryRow(query, args...)
}

func (t testWriteTx) Query(query string, args ...any) (*sql.Rows, error) {
	return t.Tx.Query(query, args...)
}

type testHost struct {
	store  testStore
	config string
	pubkey string

	floorPrice     uint64
	resaleBPS      uint64
	watchEnabled   bool
	rescanInterval uint32
	seed           moduleapi.SeedStorage
	routes         map[string]moduleapi.HTTPHandler
	openHooks      []moduleapi.OpenHook
	closeHooks     []moduleapi.CloseHook
}

func newTestHost(db *sql.DB, configPath string, pubkey string) *testHost {
	return &testHost{
		store:      testStore{db: db},
		config:     configPath,
		pubkey:     pubkey,
		floorPrice: 900,
		resaleBPS:  8000,
		routes:     map[string]moduleapi.HTTPHandler{},
	}
}

func (h *testHost) Store() moduleapi.Store            { return h.store }
func (h *testHost) ConfigPath() string                { return h.config }
func (h *testHost) NodePubkeyHex() string             { return h.pubkey }
func (h *testHost) ClientPubkeyHex() string           { return h.pubkey }
func (h *testHost) FSWatchEnabled() bool              { return h.watchEnabled }
func (h *testHost) FSRescanIntervalSeconds() uint32   { return h.rescanInterval }
func (h *testHost) StartupFullScan() bool             { return false }
func (h *testHost) SellerFloorPriceSatPer64K() uint64 { return h.floorPrice }
func (h *testHost) SellerResaleDiscountBPS() uint64   { return h.resaleBPS }
func (h *testHost) PeerCall(context.Context, moduleapi.PeerCallRequest) (moduleapi.PeerCallResponse, error) {
	return moduleapi.PeerCallResponse{}, fmt.Errorf("peer call is not supported in test host")
}
func (h *testHost) GatewaySnapshot() []moduleapi.PeerNode { return nil }

func (h *testHost) SeedStorage() moduleapi.SeedStorage {
	if h.seed == nil {
		h.seed = seedstorage.NewService(h)
	}
	return h.seed
}

func (h *testHost) InstallModule(spec moduleapi.ModuleSpec) (func(), error) {
	for _, route := range spec.HTTP {
		h.routes[route.Path] = route.Handler
	}
	return func() {
		h.routes = map[string]moduleapi.HTTPHandler{}
	}, nil
}

func (h *testHost) RegisterLibP2P(protocolID protocol.ID, hook moduleapi.LibP2PHook) (func(), error) {
	return func() {}, nil
}

func (h *testHost) RegisterHTTPRoute(path string, handler moduleapi.HTTPHandler) (func(), error) {
	h.routes[path] = handler
	return func() { delete(h.routes, path) }, nil
}

func (h *testHost) RegisterSettingsAction(string, moduleapi.SettingsHook) (func(), error) {
	return func() {}, nil
}
func (h *testHost) RegisterOBSAction(string, moduleapi.OBSControlHook) (func(), error) {
	return func() {}, nil
}
func (h *testHost) RegisterDomainResolveHook(string, moduleapi.DomainResolveHook) (func(), error) {
	return func() {}, nil
}
func (h *testHost) RegisterOpenHook(hook moduleapi.OpenHook) (func(), error) {
	h.openHooks = append(h.openHooks, hook)
	return func() {}, nil
}
func (h *testHost) RegisterCloseHook(hook moduleapi.CloseHook) (func(), error) {
	h.closeHooks = append(h.closeHooks, hook)
	return func() {}, nil
}

func (h *testHost) ServeHTTP(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	path := req.URL.Path
	if strings.HasPrefix(path, "/api") {
		path = strings.TrimPrefix(path, "/api")
	}
	if handler, ok := h.routes[path]; ok {
		handler(rec, req)
		return rec
	}
	rec.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(rec).Encode(map[string]any{"error": "route not found"})
	return rec
}

func initStaticTestSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE biz_workspaces (
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE biz_workspace_files (
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path, file_path)
		)`,
		`CREATE TABLE biz_seeds (
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE biz_seed_pricing_policy (
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE biz_seed_chunk_supply (
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash, chunk_index)
		)`,
		`CREATE TABLE proc_file_downloads (
			seed_hash TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE proc_file_download_chunks (
			seed_hash TEXT NOT NULL DEFAULT ''
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}
}

func insertWorkspaceRoot(t *testing.T, db *sql.DB, workspacePath string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO biz_workspaces(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, workspacePath, 1, 0, time.Now().Unix()); err != nil {
		t.Fatalf("insert workspace root: %v", err)
	}
}

func installStaticModule(t *testing.T, host *testHost) {
	t.Helper()
	cleanup, err := Install(context.Background(), host)
	if err != nil {
		t.Fatalf("install module: %v", err)
	}
	t.Cleanup(cleanup)
}

func postJSON(t *testing.T, h *testHost, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return h.ServeHTTP(req)
}

func getQuery(t *testing.T, h *testHost, path string, q url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api"+path+"?"+q.Encode(), nil)
	return h.ServeHTTP(req)
}

func deleteQuery(t *testing.T, h *testHost, path string, q url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/api"+path+"?"+q.Encode(), nil)
	return h.ServeHTTP(req)
}

func uploadFile(t *testing.T, h *testHost, workspacePath, targetDir, fileName string, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("workspace_path", workspacePath); err != nil {
		t.Fatalf("write workspace_path: %v", err)
	}
	if err := writer.WriteField("target_dir", targetDir); err != nil {
		t.Fatalf("write target_dir: %v", err)
	}
	if fileName != "" {
		if err := writer.WriteField("file_name", fileName); err != nil {
			t.Fatalf("write file_name: %v", err)
		}
	}
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/static/upload", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return h.ServeHTTP(req)
}

func TestStaticHTTPFlowAcrossMultipleRoots(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "static.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws1 := filepath.Join(base, "workspace-a")
	ws2 := filepath.Join(base, "workspace-b")
	if err := os.MkdirAll(filepath.Join(ws1, "docs"), 0o755); err != nil {
		t.Fatalf("mkdir ws1: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(ws2, "public"), 0o755); err != nil {
		t.Fatalf("mkdir ws2: %v", err)
	}
	insertWorkspaceRoot(t, db, ws1)
	insertWorkspaceRoot(t, db, ws2)

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	installStaticModule(t, host)

	if rec := getQuery(t, host, "/v1/admin/static/tree", url.Values{}); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing workspace_path should fail, got=%d body=%s", rec.Code, rec.Body.String())
	}

	if err := os.WriteFile(filepath.Join(ws1, "docs", "a.txt"), []byte("hello ws1"), 0o644); err != nil {
		t.Fatalf("write ws1 file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws2, "public", "b.txt"), []byte("hello ws2"), 0o644); err != nil {
		t.Fatalf("write ws2 file: %v", err)
	}

	tree1 := getQuery(t, host, "/v1/admin/static/tree", url.Values{
		"workspace_path": []string{ws1},
		"path":           []string{"/docs"},
		"recursive":      []string{"true"},
		"max_depth":      []string{"4"},
	})
	if tree1.Code != http.StatusOK {
		t.Fatalf("tree ws1 status mismatch: got=%d body=%s", tree1.Code, tree1.Body.String())
	}
	var treeBody map[string]any
	if err := json.Unmarshal(tree1.Body.Bytes(), &treeBody); err != nil {
		t.Fatalf("decode tree ws1: %v", err)
	}
	items, _ := treeBody["data"].(map[string]any)["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("ws1 tree should have one file, got=%v", treeBody)
	}

	mkdirRec := postJSON(t, host, "/v1/admin/static/mkdir", `{"workspace_path":"`+ws1+`","path":"/docs/newdir"}`)
	if mkdirRec.Code != http.StatusOK {
		t.Fatalf("mkdir status mismatch: got=%d body=%s", mkdirRec.Code, mkdirRec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws1, "docs", "newdir")); err != nil {
		t.Fatalf("mkdir path missing: %v", err)
	}

	uploadRec := uploadFile(t, host, ws1, "/docs/newdir", "upload.txt", []byte("upload body"))
	if uploadRec.Code != http.StatusOK {
		t.Fatalf("upload status mismatch: got=%d body=%s", uploadRec.Code, uploadRec.Body.String())
	}
	uploadPath := filepath.Join(ws1, "docs", "newdir", "upload.txt")
	if _, err := os.Stat(uploadPath); err != nil {
		t.Fatalf("uploaded file missing: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws1, filepath.ToSlash(filepath.Join("docs", "newdir", "upload.txt"))).Scan(new(int)); err != nil {
		t.Fatalf("query uploaded row: %v", err)
	}

	moveRec := postJSON(t, host, "/v1/admin/static/move", `{"workspace_path":"`+ws1+`","source_path":"/docs/newdir/upload.txt","target_dir":"/archive/2026","new_name":"moved.txt"}`)
	if moveRec.Code != http.StatusOK {
		t.Fatalf("move status mismatch: got=%d body=%s", moveRec.Code, moveRec.Body.String())
	}
	movedPath := filepath.Join(ws1, "archive", "2026", "moved.txt")
	if _, err := os.Stat(movedPath); err != nil {
		t.Fatalf("moved file missing: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws1, filepath.ToSlash(filepath.Join("docs", "newdir", "upload.txt"))).Scan(&count); err != nil {
		t.Fatalf("query old row: %v", err)
	}
	if count != 0 {
		t.Fatalf("old workspace row should be removed, got=%d", count)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws1, filepath.ToSlash(filepath.Join("archive", "2026", "moved.txt"))).Scan(&count); err != nil {
		t.Fatalf("query new row: %v", err)
	}
	if count != 1 {
		t.Fatalf("new workspace row should exist, got=%d", count)
	}

	priceSetRec := postJSON(t, host, "/v1/admin/static/price/set", `{"workspace_path":"`+ws1+`","path":"/archive/2026/moved.txt","floor_price_sat_per_64k":15,"resale_discount_bps":8000}`)
	if priceSetRec.Code != http.StatusOK {
		t.Fatalf("price set status mismatch: got=%d body=%s", priceSetRec.Code, priceSetRec.Body.String())
	}
	priceGetRec := getQuery(t, host, "/v1/admin/static/price", url.Values{
		"workspace_path": []string{ws1},
		"path":           []string{"/archive/2026/moved.txt"},
	})
	if priceGetRec.Code != http.StatusOK {
		t.Fatalf("price get status mismatch: got=%d body=%s", priceGetRec.Code, priceGetRec.Body.String())
	}
	var priceBody struct {
		Status string `json:"status"`
		Data   struct {
			Path        string `json:"path"`
			SeedHash    string `json:"seed_hash"`
			Floor       uint64 `json:"floor_unit_price_sat_per_64k"`
			DiscountBPS uint64 `json:"resale_discount_bps"`
		} `json:"data"`
	}
	if err := json.Unmarshal(priceGetRec.Body.Bytes(), &priceBody); err != nil {
		t.Fatalf("decode price body: %v", err)
	}
	if priceBody.Data.Floor != 15 || priceBody.Data.DiscountBPS != 8000 || priceBody.Data.SeedHash == "" {
		t.Fatalf("unexpected price body: %+v", priceBody)
	}

	delRec := deleteQuery(t, host, "/v1/admin/static/entry", url.Values{
		"workspace_path": []string{ws1},
		"path":           []string{"/archive/2026/moved.txt"},
	})
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete status mismatch: got=%d body=%s", delRec.Code, delRec.Body.String())
	}
	if _, err := os.Stat(movedPath); !os.IsNotExist(err) {
		t.Fatalf("moved file should be deleted, err=%v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws1, filepath.ToSlash(filepath.Join("archive", "2026", "moved.txt"))).Scan(&count); err != nil {
		t.Fatalf("query deleted row: %v", err)
	}
	if count != 0 {
		t.Fatalf("deleted workspace row should be removed, got=%d", count)
	}

	tree2 := getQuery(t, host, "/v1/admin/static/tree", url.Values{
		"workspace_path": []string{ws2},
		"path":           []string{"/public"},
		"recursive":      []string{"true"},
	})
	if tree2.Code != http.StatusOK {
		t.Fatalf("tree ws2 status mismatch: got=%d body=%s", tree2.Code, tree2.Body.String())
	}
}

func TestStaticMoveSamePathKeepsSourceFile(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "same-path.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws := filepath.Join(base, "workspace")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	insertWorkspaceRoot(t, db, ws)
	if err := os.WriteFile(filepath.Join(ws, "a.txt"), []byte("same-path"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	seedPath := filepath.Join(base, "seed.bse")
	if err := os.WriteFile(seedPath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("11", 32), 1, 9, seedPath, "", ""); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		ws, "a.txt", strings.Repeat("11", 32), 0); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	installStaticModule(t, host)

	rec := postJSON(t, host, "/v1/admin/static/move", `{"workspace_path":"`+ws+`","source_path":"/a.txt","to_path":"/a.txt","overwrite":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("same-path move should succeed, got=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(filepath.Join(ws, "a.txt")); err != nil {
		t.Fatalf("source file should stay in place: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, ws, "a.txt").Scan(&count); err != nil {
		t.Fatalf("query workspace file: %v", err)
	}
	if count != 1 {
		t.Fatalf("workspace row should remain, got=%d", count)
	}
}

func TestStaticMoveRejectsEmptyTarget(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "empty-target.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws := filepath.Join(base, "workspace")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	insertWorkspaceRoot(t, db, ws)
	if err := os.WriteFile(filepath.Join(ws, "a.txt"), []byte("move"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`,
		strings.Repeat("33", 32), 1, 4, filepath.Join(base, "seed.bse"), "", ""); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		ws, "a.txt", strings.Repeat("33", 32), 0); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	installStaticModule(t, host)

	rec := postJSON(t, host, "/v1/admin/static/move", `{"workspace_path":"`+ws+`","source_path":"/a.txt","overwrite":true}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty target should be rejected, got=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestStaticEntryRejectsWorkspaceRoot(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "delete-root.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws := filepath.Join(base, "workspace")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	insertWorkspaceRoot(t, db, ws)

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	installStaticModule(t, host)

	rec := deleteQuery(t, host, "/v1/admin/static/entry", url.Values{
		"workspace_path": []string{ws},
		"path":           []string{"/"},
		"recursive":      []string{"true"},
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("workspace root delete should be rejected, got=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := os.Stat(ws); err != nil {
		t.Fatalf("workspace root should stay in place: %v", err)
	}
}

func TestStaticPriceSetRollsBackWhenSeedMissing(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "price-rollback.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws := filepath.Join(base, "workspace")
	if err := os.MkdirAll(ws, 0o755); err != nil {
		t.Fatalf("mkdir ws: %v", err)
	}
	insertWorkspaceRoot(t, db, ws)
	filePath := filepath.Join(ws, "a.txt")
	if err := os.WriteFile(filePath, []byte("price"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		ws, "a.txt", strings.Repeat("22", 32), 0); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	installStaticModule(t, host)

	rec := postJSON(t, host, "/v1/admin/static/price/set", `{"workspace_path":"`+ws+`","path":"/a.txt","floor_price_sat_per_64k":15,"resale_discount_bps":8000}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("price set should fail when seed is missing, got=%d body=%s", rec.Code, rec.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM biz_seed_pricing_policy WHERE seed_hash=?`, strings.Repeat("22", 32)).Scan(&count); err != nil {
		t.Fatalf("query pricing policy: %v", err)
	}
	if count != 0 {
		t.Fatalf("pricing policy should roll back, got=%d", count)
	}
}

func TestWatchStartFailureResetsState(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "watch-fail.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	host.watchEnabled = true
	svc, err := newService(host)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	w := newWatchService(svc)

	if err := w.start(context.Background()); err == nil {
		t.Fatalf("watch start should fail without workspace schema")
	}
	if w.watcher != nil {
		t.Fatalf("watcher should be cleared after start failure")
	}
	if w.cancel != nil {
		t.Fatalf("cancel should be cleared after start failure")
	}
	if w.pending != nil {
		t.Fatalf("pending queue should be cleared after start failure")
	}
	if len(w.roots) != 0 {
		t.Fatalf("roots should be cleared after start failure")
	}
}

func TestWatchReloadAddsNewRoot(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	dbPath := filepath.Join(base, "watch.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initStaticTestSchema(t, db)

	ws1 := filepath.Join(base, "workspace-1")
	ws2 := filepath.Join(base, "workspace-2")
	if err := os.MkdirAll(ws1, 0o755); err != nil {
		t.Fatalf("mkdir ws1: %v", err)
	}
	if err := os.MkdirAll(ws2, 0o755); err != nil {
		t.Fatalf("mkdir ws2: %v", err)
	}
	insertWorkspaceRoot(t, db, ws1)

	host := newTestHost(db, filepath.Join(base, "config"), strings.Repeat("ab", 32))
	host.watchEnabled = true
	host.rescanInterval = 1
	svc, err := newService(host)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	if err := svc.open(t.Context()); err != nil {
		t.Fatalf("open watcher: %v", err)
	}
	if svc.watch == nil {
		t.Fatal("watch should be initialized")
	}
	if len(svc.watch.roots) != 1 {
		t.Fatalf("unexpected root count: %d", len(svc.watch.roots))
	}
	insertWorkspaceRoot(t, db, ws2)
	if err := svc.reloadWatcher(t.Context()); err != nil {
		t.Fatalf("reload watcher: %v", err)
	}
	if svc.watch == nil {
		t.Fatal("watch should still be initialized after reload")
	}
	if len(svc.watch.roots) != 2 {
		t.Fatalf("reload should include new root, got=%d", len(svc.watch.roots))
	}
	if err := svc.close(t.Context()); err != nil {
		t.Fatalf("close watcher: %v", err)
	}
}
