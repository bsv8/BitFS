package clientapp

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHandleGetFilePlan_LocalResource(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	seedHash, filePath, fileBody := prepareLocalSeedFixture(t, db, "hello bitfs browser")

	reqBody, err := json.Marshal(getFilePlanRequest{
		SeedHashes:   []string{seedHash},
		ResourceKind: "static",
		RootSeedHash: seedHash,
	})
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/get-file/plan", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	srv.handleGetFilePlan(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("plan status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body struct {
		Items []staticResourcePlanItem `json:"items"`
		Total int                      `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Total != 1 || len(body.Items) != 1 {
		t.Fatalf("unexpected items: total=%d len=%d", body.Total, len(body.Items))
	}
	item := body.Items[0]
	if item.Status != "local" || !item.LocalReady {
		t.Fatalf("unexpected item status: %+v", item)
	}
	if item.SeedHash != seedHash {
		t.Fatalf("seed hash mismatch: got=%s want=%s", item.SeedHash, seedHash)
	}
	if item.FileSize != uint64(len(fileBody)) {
		t.Fatalf("file size mismatch: got=%d want=%d", item.FileSize, len(fileBody))
	}
	if item.ChunkCount != 1 {
		t.Fatalf("chunk count mismatch: got=%d want=1", item.ChunkCount)
	}
	if item.RecommendedFileName != filepath.Base(filePath) {
		t.Fatalf("recommended file name mismatch: got=%s want=%s", item.RecommendedFileName, filepath.Base(filePath))
	}
}

func TestHandleGetFileContent_LocalResource(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	seedHash, _, fileBody := prepareLocalSeedFixture(t, db, "<html><body>bitfs</body></html>")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/get-file/content?seed_hash="+seedHash, nil)
	rec := httptest.NewRecorder()
	srv.handleGetFileContent(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("content status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Body.String(); got != fileBody {
		t.Fatalf("content body mismatch: got=%q want=%q", got, fileBody)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content type mismatch: got=%s", ct)
	}
}

func TestHandleGetFileContent_LocalResourceUsesStoredMIMEHint(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	seedHash, filePath, fileBody := prepareLocalSeedFixture(t, db, `(function(){const root=document.getElementById("root");})();`)

	hashPath := filepath.Join(filepath.Dir(filePath), seedHash)
	if err := os.Rename(filePath, hashPath); err != nil {
		t.Fatalf("rename local file: %v", err)
	}
	now := time.Now().Unix()
	if _, err := db.Exec(`UPDATE workspace_files SET path=?,updated_at_unix=? WHERE seed_hash=?`, hashPath, now, seedHash); err != nil {
		t.Fatalf("update workspace path: %v", err)
	}
	if _, err := db.Exec(`UPDATE seeds SET recommended_file_name=?,mime_hint=? WHERE seed_hash=?`, "index.js", "application/javascript", seedHash); err != nil {
		t.Fatalf("update seed meta: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/get-file/content?seed_hash="+seedHash, nil)
	rec := httptest.NewRecorder()
	srv.handleGetFileContent(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("content status mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Body.String(); got != fileBody {
		t.Fatalf("content body mismatch: got=%q want=%q", got, fileBody)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/javascript") {
		t.Fatalf("content type mismatch: got=%s", ct)
	}
}

func TestHandleGetFileStatus_LocalResource(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	seedHash, filePath, fileBody := prepareLocalSeedFixture(t, db, "status local")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/files/get-file/status?seed_hash="+seedHash, nil)
	rec := httptest.NewRecorder()
	srv.handleGetFileStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status code mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body staticResourceStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status body: %v", err)
	}
	if body.Status != "local" || !body.LocalReady {
		t.Fatalf("unexpected status body: %+v", body)
	}
	if body.OutputFilePath != filePath {
		t.Fatalf("output path mismatch: got=%s want=%s", body.OutputFilePath, filePath)
	}
	if body.ContentURI != "bitfs://"+seedHash {
		t.Fatalf("content uri mismatch: got=%s", body.ContentURI)
	}
	if body.FileSize != uint64(len(fileBody)) {
		t.Fatalf("file size mismatch: got=%d want=%d", body.FileSize, len(fileBody))
	}
}

func TestGuessContentType_ExtensionlessWebAssets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		head string
		want string
	}{
		{
			name: strings.Repeat("a", 64),
			head: "(function(){const root=document.getElementById('root');})();",
			want: "application/javascript",
		},
		{
			name: strings.Repeat("d", 64),
			head: `(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;function n(l){const u={};return l.integrity&&(u.integrity=l.integrity),l.referrerPolicy&&(u.referrerPolicy=l.referrerPolicy),u}function r(l){if(l.ep)return;l.ep=!0;const u=n(l);fetch(l.href,u)}})();`,
			want: "application/javascript",
		},
		{
			name: strings.Repeat("b", 64),
			head: ":root{color-scheme:light}body{margin:0}",
			want: "text/css",
		},
		{
			name: strings.Repeat("c", 64),
			head: "<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
			want: "image/svg+xml",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			got := guessContentType(tc.name, []byte(tc.head))
			if !strings.Contains(got, tc.want) {
				t.Fatalf("content type mismatch: got=%s want contains %s", got, tc.want)
			}
		})
	}
}

func TestHandleGetFileEnsure_LocalResource(t *testing.T) {
	t.Parallel()

	db := newWalletAPITestDB(t)
	srv := &httpAPIServer{db: db}
	seedHash, filePath, _ := prepareLocalSeedFixture(t, db, "ensure local")

	reqBody, err := json.Marshal(getFileEnsureRequest{SeedHash: seedHash})
	if err != nil {
		t.Fatalf("marshal ensure req: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/files/get-file/ensure", bytes.NewReader(reqBody))
	rec := httptest.NewRecorder()
	srv.handleGetFileEnsure(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ensure code mismatch: got=%d want=%d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body staticResourceStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode ensure body: %v", err)
	}
	if body.Status != "local" || !body.LocalReady {
		t.Fatalf("unexpected ensure body: %+v", body)
	}
	if body.OutputFilePath != filePath {
		t.Fatalf("output path mismatch: got=%s want=%s", body.OutputFilePath, filePath)
	}
}

func TestSelectBestStaticQuote_OnlyAcceptsCompleteCoverage(t *testing.T) {
	t.Parallel()

	incomplete := DirectQuoteItem{
		SellerPeerID:          "seller-a",
		SeedPrice:             10,
		ChunkPrice:            1,
		ChunkCount:            3,
		AvailableChunkIndexes: []uint32{0, 1},
	}
	complete := DirectQuoteItem{
		SellerPeerID:          "seller-b",
		SeedPrice:             20,
		ChunkPrice:            1,
		ChunkCount:            3,
		AvailableChunkIndexes: []uint32{0, 1, 2},
	}

	best, ok, reason := selectBestStaticQuote([]DirectQuoteItem{incomplete, complete})
	if !ok {
		t.Fatalf("select best quote failed: %s", reason)
	}
	if best.SellerPeerID != complete.SellerPeerID {
		t.Fatalf("unexpected quote selected: got=%s want=%s", best.SellerPeerID, complete.SellerPeerID)
	}
}

func prepareLocalSeedFixture(t *testing.T, db *sql.DB, body string) (string, string, string) {
	t.Helper()

	seedHash := strings.Repeat("ab", 32)
	base := t.TempDir()
	filePath := filepath.Join(base, "page.html")
	if err := os.WriteFile(filePath, []byte(body), 0o644); err != nil {
		t.Fatalf("write local file: %v", err)
	}
	seedPath := filepath.Join(base, seedHash+".seed")
	if err := os.WriteFile(seedPath, make([]byte, 64), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}
	now := time.Now().Unix()
	if _, err := db.Exec(`INSERT INTO seeds(seed_hash,seed_file_path,chunk_count,file_size,created_at_unix) VALUES(?,?,?,?,?)`,
		seedHash, seedPath, 1, len(body), now); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO workspace_files(path,file_size,mtime_unix,seed_hash,updated_at_unix) VALUES(?,?,?,?,?)`,
		filePath, len(body), now, seedHash, now); err != nil {
		t.Fatalf("insert workspace file: %v", err)
	}
	return seedHash, filePath, body
}
