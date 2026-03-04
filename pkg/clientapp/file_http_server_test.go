package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseSingleRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		wantOK  bool
		wantErr bool
		start   int64
		end     int64
	}{
		{name: "empty", raw: "", wantOK: false, wantErr: false},
		{name: "single", raw: "bytes=10-20", wantOK: true, start: 10, end: 20},
		{name: "single_with_space", raw: " bytes=0-0 ", wantOK: true, start: 0, end: 0},
		{name: "invalid_unit", raw: "items=1-2", wantErr: true},
		{name: "multi_range", raw: "bytes=0-9,20-29", wantErr: true},
		{name: "open_end_not_supported", raw: "bytes=10-", wantErr: true},
		{name: "suffix_not_supported", raw: "bytes=-500", wantErr: true},
		{name: "bad_start", raw: "bytes=a-10", wantErr: true},
		{name: "bad_end", raw: "bytes=1-a", wantErr: true},
		{name: "end_lt_start", raw: "bytes=10-1", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseSingleRange(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.ok != tt.wantOK {
				t.Fatalf("ok mismatch: got=%v want=%v", got.ok, tt.wantOK)
			}
			if tt.wantOK && (got.start != tt.start || got.end != tt.end) {
				t.Fatalf("range mismatch: got=[%d,%d] want=[%d,%d]", got.start, got.end, tt.start, tt.end)
			}
		})
	}
}

func TestNextPendingChunkPriority(t *testing.T) {
	t.Parallel()
	sess := &fileDownloadSession{
		chunkCount: 8,
		completed: map[uint32]bool{
			1: true,
		},
		runtimeState: defaultFileDownloadRuntimeState(),
	}
	sess.chunkPolicy = newDemandPrefetchBackfillPolicy(demandPrefetchBackfillPolicyConfig{PrefetchDistance: 8})
	sess.chunkPolicy.AddDemand(5, 5)
	sess.chunkPolicy.AddDemand(2, 2)
	pending := map[uint32]bool{
		0: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true,
	}
	inflight := map[uint32]bool{}

	// 有活跃需求时优先选择最小需求块（2）。
	ch, ok := sess.nextPendingChunk(pending, inflight)
	if !ok || ch != 2 {
		t.Fatalf("expected chunk 2, got ok=%v ch=%d", ok, ch)
	}

	// 需求块 2 在途时，选择另一个需求块（5）。
	inflight[2] = true
	ch, ok = sess.nextPendingChunk(pending, inflight)
	if !ok || ch != 5 {
		t.Fatalf("expected chunk 5, got ok=%v ch=%d", ok, ch)
	}

	// 清空需求后，进入预读阶段，从预读游标继续向后选择（当前实现命中 6）。
	sess.chunkPolicy.RemoveDemand(5, 5)
	sess.chunkPolicy.RemoveDemand(2, 2)
	ch, ok = sess.nextPendingChunk(pending, inflight)
	if !ok || ch != 6 {
		t.Fatalf("expected chunk 6, got ok=%v ch=%d", ok, ch)
	}
}

func TestWaitMetaAndChunkDoneRespectContext(t *testing.T) {
	t.Parallel()
	sess := &fileDownloadSession{
		completed: map[uint32]bool{},
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := sess.waitMeta(ctx); err == nil {
			t.Fatalf("waitMeta should return timeout/cancel error")
		}
	}

	{
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := sess.waitChunkDone(ctx, 0); err == nil {
			t.Fatalf("waitChunkDone should return timeout/cancel error")
		}
	}
}

func TestActiveSessionCountLocked(t *testing.T) {
	t.Parallel()
	s := &fileHTTPServer{
		sessions: map[string]*fileDownloadSession{
			"a": {status: "queued"},
			"b": {status: "running"},
			"c": {status: "seed_fetch"},
			"d": {status: "done"},
			"e": {status: "failed"},
		},
	}
	for _, sess := range s.sessions {
		sess.cond = sync.NewCond(&sess.mu)
	}
	if got := s.activeSessionCountLocked(); got != 3 {
		t.Fatalf("active session count mismatch: got=%d want=3", got)
	}
}

func TestFileHTTPServerStatusIdle(t *testing.T) {
	t.Parallel()
	srv, seedHash, _ := newLocalOnlyTestServer(t, []byte("hello"))
	req := httptest.NewRequest(http.MethodGet, "/"+seedHash+"/status", nil)
	rec := httptest.NewRecorder()
	srv.handleRoot(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code mismatch: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got := body["state"]; got != "idle" {
		t.Fatalf("state mismatch: got=%v want=idle", got)
	}
}

func TestFileHTTPServerServeLocalFullAndRange(t *testing.T) {
	t.Parallel()
	content := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	srv, seedHash, _ := newLocalOnlyTestServer(t, content)
	// 使用真实本地文件路径，确保走本地文件返回逻辑。
	dir := t.TempDir()
	localPath := filepath.Join(dir, "movie.bin")
	if err := os.WriteFile(localPath, content, 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	if err := upsertWorkspaceFileForSeed(t, srv.db, seedHash, localPath, int64(len(content))); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	{
		req := httptest.NewRequest(http.MethodGet, "/"+seedHash, nil)
		rec := httptest.NewRecorder()
		srv.handleRoot(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("full get status mismatch: got=%d want=%d", rec.Code, http.StatusOK)
		}
		if rec.Body.String() != string(content) {
			t.Fatalf("full body mismatch: got=%q want=%q", rec.Body.String(), string(content))
		}
	}
	{
		req := httptest.NewRequest(http.MethodGet, "/"+seedHash, nil)
		req.Header.Set("Range", "bytes=10-15")
		rec := httptest.NewRecorder()
		srv.handleRoot(rec, req)
		if rec.Code != http.StatusPartialContent {
			t.Fatalf("range get status mismatch: got=%d want=%d", rec.Code, http.StatusPartialContent)
		}
		if got := rec.Header().Get("Content-Range"); got != "bytes 10-15/36" {
			t.Fatalf("content-range mismatch: got=%q", got)
		}
		if rec.Body.String() != "abcdef" {
			t.Fatalf("range body mismatch: got=%q want=%q", rec.Body.String(), "abcdef")
		}
	}
}

func TestFileHTTPServerRejectMultiRange(t *testing.T) {
	t.Parallel()
	content := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	srv, seedHash, _ := newLocalOnlyTestServer(t, content)
	dir := t.TempDir()
	localPath := filepath.Join(dir, "movie.bin")
	if err := os.WriteFile(localPath, content, 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	if err := upsertWorkspaceFileForSeed(t, srv.db, seedHash, localPath, int64(len(content))); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/"+seedHash, nil)
	req.Header.Set("Range", "bytes=0-9,20-29")
	rec := httptest.NewRecorder()
	srv.handleRoot(rec, req)
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status mismatch: got=%d want=%d", rec.Code, http.StatusRequestedRangeNotSatisfiable)
	}
}

func newLocalOnlyTestServer(t *testing.T, _ []byte) (*fileHTTPServer, string, string) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("sqlite pragmas: %v", err)
	}
	if err := initIndexDB(db); err != nil {
		t.Fatalf("init db: %v", err)
	}
	cfg := &Config{}
	cfg.Storage.DataDir = t.TempDir()
	cfg.Storage.WorkspaceDir = t.TempDir()
	cfg.FSHTTP.DownloadWaitTimeoutSeconds = 1
	cfg.FSHTTP.MaxConcurrentSessions = 4
	cfg.FSHTTP.ListenAddr = "127.0.0.1:0"
	srv := newFileHTTPServer(&Runtime{}, cfg, db, &workspaceManager{cfg: cfg, db: db})
	seedHash := strings.Repeat("a", 64)
	return srv, seedHash, cfg.Storage.WorkspaceDir
}

func upsertWorkspaceFileForSeed(t *testing.T, db *sql.DB, seedHash, path string, size int64) error {
	t.Helper()
	_, err := db.Exec(`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,updated_at_unix) VALUES(?,?,?,?,?)
		ON CONFLICT(path) DO UPDATE SET file_size=excluded.file_size,mtime_unix=excluded.mtime_unix,seed_hash=excluded.seed_hash,updated_at_unix=excluded.updated_at_unix`,
		path, size, time.Now().Unix(), seedHash, time.Now().Unix())
	return err
}
