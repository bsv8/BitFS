package clientapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func TestDownloadFileWorkspacePreparePartFileStable(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()

	req := filedownload.PreparePartFileInput{
		SeedHash: env.seedHash,
		FileSize: 128,
	}

	first, err := env.adapter.PreparePartFile(ctx, req)
	if err != nil {
		t.Fatalf("prepare part file failed: %v", err)
	}
	if first.PartFilePath == "" {
		t.Fatal("part file path is empty")
	}
	if _, err := os.Stat(first.PartFilePath); !os.IsNotExist(err) {
		t.Fatalf("part file should not exist yet, got err=%v", err)
	}

	payload := []byte("part-data")
	if err := os.WriteFile(first.PartFilePath, payload, 0o644); err != nil {
		t.Fatalf("write part file failed: %v", err)
	}

	second, err := env.adapter.PreparePartFile(ctx, req)
	if err != nil {
		t.Fatalf("prepare part file twice failed: %v", err)
	}
	if second.PartFilePath != first.PartFilePath {
		t.Fatalf("part path should be stable: got=%s want=%s", second.PartFilePath, first.PartFilePath)
	}

	after, err := os.ReadFile(first.PartFilePath)
	if err != nil {
		t.Fatalf("read part file failed: %v", err)
	}
	if string(after) != string(payload) {
		t.Fatalf("part file should not be cleared: got=%q want=%q", string(after), string(payload))
	}
}

func TestDownloadFileWorkspaceMarkChunkStoredRequiresLayout(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()

	partReq := filedownload.PreparePartFileInput{
		SeedHash: env.seedHash,
		FileSize: 128,
	}
	partFile, err := env.adapter.PreparePartFile(ctx, partReq)
	if err != nil {
		t.Fatalf("prepare part file failed: %v", err)
	}

	err = env.adapter.MarkChunkStored(ctx, filedownload.StoredChunkInput{
		SeedHash:   env.seedHash,
		ChunkIndex: 0,
		ChunkBytes: []byte("chunk-bytes"),
	})
	if err == nil {
		t.Fatal("mark chunk stored should fail without chunk layout")
	}
	if got := err.Error(); got != "chunk layout is required" {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, statErr := os.Stat(partFile.PartFilePath); !os.IsNotExist(statErr) {
		t.Fatalf("part file should not be created on layout failure, got err=%v", statErr)
	}
}

func TestDownloadFileWorkspaceMarkChunkStoredStrictLength(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()
	env.insertSeedMeta(t, 3, 12)

	partFile, err := env.adapter.PreparePartFile(ctx, filedownload.PreparePartFileInput{
		SeedHash: env.seedHash,
		FileSize: 12,
	})
	if err != nil {
		t.Fatalf("prepare part file failed: %v", err)
	}

	cases := []struct {
		name string
		data []byte
	}{
		{name: "short", data: []byte("aaa")},
		{name: "long", data: []byte("aaaaa")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := env.adapter.MarkChunkStored(ctx, filedownload.StoredChunkInput{
				SeedHash:   env.seedHash,
				ChunkIndex: 0,
				ChunkBytes: tc.data,
			})
			if err == nil {
				t.Fatal("mark chunk stored should fail for mismatched length")
			}
			if got := err.Error(); got != "chunk size does not match layout" {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	if _, err := os.Stat(partFile.PartFilePath); !os.IsNotExist(err) {
		t.Fatalf("part file should not be created on length failure, got err=%v", err)
	}
}

func TestDownloadFileWorkspaceCompleteRequiresPartFile(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()
	env.insertSeedMeta(t, 3, 12)

	_, err := env.adapter.CompleteFile(ctx, filedownload.CompleteFileInput{
		SeedHash:            env.seedHash,
		PartFilePath:        filepath.Join(env.dataDir, "downloads", env.seedHash+".part"),
		RecommendedFileName: "",
		MimeType:            "application/octet-stream",
	})
	if err == nil {
		t.Fatal("complete file should fail without part file")
	}
	if got := err.Error(); got != "part file does not exist" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDownloadFileWorkspaceCompleteRegistersWorkspaceFile(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()
	env.insertSeedMeta(t, 3, 12)
	if _, err := env.db.Exec(`UPDATE biz_seeds SET recommended_file_name=?, mime_hint=? WHERE seed_hash=?`, "kept-name.bin", "text/plain", env.seedHash); err != nil {
		t.Fatalf("update seed meta fields failed: %v", err)
	}

	partFile, err := env.adapter.PreparePartFile(ctx, filedownload.PreparePartFileInput{
		SeedHash: env.seedHash,
		FileSize: 12,
	})
	if err != nil {
		t.Fatalf("prepare part file failed: %v", err)
	}

	chunks := [][]byte{
		[]byte("aaaa"),
		[]byte("bbbb"),
		[]byte("cccc"),
	}
	for i, chunk := range chunks {
		if err := env.adapter.MarkChunkStored(ctx, filedownload.StoredChunkInput{
			SeedHash:   env.seedHash,
			ChunkIndex: uint32(i),
			ChunkBytes: chunk,
		}); err != nil {
			t.Fatalf("mark chunk %d failed: %v", i, err)
		}
	}

	partBody, err := os.ReadFile(partFile.PartFilePath)
	if err != nil {
		t.Fatalf("read part file failed: %v", err)
	}
	if string(partBody) != "aaaabbbbcccc" {
		t.Fatalf("part file content mismatch: got=%q", string(partBody))
	}

	afterPrepare, err := env.adapter.PreparePartFile(ctx, filedownload.PreparePartFileInput{
		SeedHash: env.seedHash,
		FileSize: 12,
	})
	if err != nil {
		t.Fatalf("prepare part file again failed: %v", err)
	}
	if afterPrepare.PartFilePath != partFile.PartFilePath {
		t.Fatalf("part path should be stable: got=%s want=%s", afterPrepare.PartFilePath, partFile.PartFilePath)
	}
	afterBody, err := os.ReadFile(afterPrepare.PartFilePath)
	if err != nil {
		t.Fatalf("read part file after second prepare failed: %v", err)
	}
	if string(afterBody) != "aaaabbbbcccc" {
		t.Fatalf("part file should not be cleared after second prepare: got=%q", string(afterBody))
	}

	localFile, err := env.adapter.CompleteFile(ctx, filedownload.CompleteFileInput{
		SeedHash:            env.seedHash,
		PartFilePath:        partFile.PartFilePath,
		RecommendedFileName: "",
		MimeType:            "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("complete file failed: %v", err)
	}
	if filepath.Base(localFile.FilePath) != env.seedHash {
		t.Fatalf("final file name should fall back to seed hash: got=%s", filepath.Base(localFile.FilePath))
	}
	if localFile.FileSize != 12 {
		t.Fatalf("final file size mismatch: got=%d want=%d", localFile.FileSize, 12)
	}

	finalBody, err := os.ReadFile(localFile.FilePath)
	if err != nil {
		t.Fatalf("read final file failed: %v", err)
	}
	if string(finalBody) != "aaaabbbbcccc" {
		t.Fatalf("final file content mismatch: got=%q", string(finalBody))
	}

	foundFile, ok, err := env.adapter.FindCompleteFile(ctx, env.seedHash)
	if err != nil {
		t.Fatalf("find complete file failed: %v", err)
	}
	if !ok {
		t.Fatal("complete file should be found")
	}
	if foundFile.FilePath != localFile.FilePath {
		t.Fatalf("find complete file path mismatch: got=%s want=%s", foundFile.FilePath, localFile.FilePath)
	}

	second, err := env.adapter.CompleteFile(ctx, filedownload.CompleteFileInput{
		SeedHash:            env.seedHash,
		PartFilePath:        partFile.PartFilePath,
		RecommendedFileName: "",
		MimeType:            "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("complete file twice failed: %v", err)
	}
	if second.FilePath != localFile.FilePath {
		t.Fatalf("complete file should be idempotent: got=%s want=%s", second.FilePath, localFile.FilePath)
	}

	var recommendedName, mimeHint string
	if err := env.db.QueryRow(`SELECT recommended_file_name,mime_hint FROM biz_seeds WHERE seed_hash=?`, env.seedHash).Scan(&recommendedName, &mimeHint); err != nil {
		t.Fatalf("load seed meta after complete failed: %v", err)
	}
	if recommendedName != "kept-name.bin" || mimeHint != "text/plain" {
		t.Fatalf("seed meta should not change: got=%q/%q", recommendedName, mimeHint)
	}

	var workspaceCount int
	if err := env.db.QueryRow(`SELECT COUNT(1) FROM biz_workspace_files WHERE seed_hash=?`, env.seedHash).Scan(&workspaceCount); err != nil {
		t.Fatalf("count workspace file rows failed: %v", err)
	}
	if workspaceCount != 1 {
		t.Fatalf("expected 1 workspace row, got %d", workspaceCount)
	}
}

func TestDownloadFileWorkspaceCompleteMissingPartFailsEvenIfRegistered(t *testing.T) {
	env := newDownloadFileWorkspaceAdapterTestEnv(t)
	ctx := context.Background()
	env.insertSeedMeta(t, 3, 12)

	finalPath := filepath.Join(env.workspaceDir, env.seedHash)
	if err := os.WriteFile(finalPath, []byte("aaaabbbbcccc"), 0o644); err != nil {
		t.Fatalf("write final file failed: %v", err)
	}
	if _, err := env.db.Exec(`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`,
		env.workspaceDir, env.seedHash, env.seedHash, 1); err != nil {
		t.Fatalf("insert workspace row failed: %v", err)
	}

	_, err := env.adapter.CompleteFile(ctx, filedownload.CompleteFileInput{
		SeedHash:            env.seedHash,
		PartFilePath:        filepath.Join(env.dataDir, "downloads", env.seedHash+".part"),
		RecommendedFileName: "",
		MimeType:            "application/octet-stream",
	})
	if err == nil {
		t.Fatal("complete file should fail when part file is missing")
	}
	if got := err.Error(); got != "part file does not exist" {
		t.Fatalf("unexpected error: %v", err)
	}
}

type downloadFileWorkspaceAdapterTestEnv struct {
	t            *testing.T
	db           *sql.DB
	store        *clientDB
	adapter      *downloadFileWorkspaceAdapter
	root         string
	dataDir      string
	configDir    string
	workspaceDir string
	pubkeyHex    string
	seedHash     string
}

func newDownloadFileWorkspaceAdapterTestEnv(t *testing.T) downloadFileWorkspaceAdapterTestEnv {
	t.Helper()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(root, "config")
	workspaceDir := filepath.Join(root, "workspace")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir data dir failed: %v", err)
	}
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		t.Fatalf("mkdir workspace dir failed: %v", err)
	}
	pubkeyHex := strings.Repeat("aa", 32)
	if err := os.MkdirAll(filepath.Join(configDir, pubkeyHex, "seeds"), 0o755); err != nil {
		t.Fatalf("mkdir seed dir failed: %v", err)
	}
	dbPath := filepath.Join(dataDir, "client-index.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := ensureClientSchemaOnDB(t.Context(), NewClientStore(db, nil)); err != nil {
		t.Fatalf("ensure schema failed: %v", err)
	}
	store := NewClientStore(db, nil)
	if _, err := dbAddWorkspace(t.Context(), store, workspaceDir, 0); err != nil {
		t.Fatalf("add workspace failed: %v", err)
	}
	env := downloadFileWorkspaceAdapterTestEnv{
		t:            t,
		db:           db,
		store:        store,
		adapter:      newDownloadFileWorkspaceAdapter(store),
		root:         root,
		dataDir:      dataDir,
		configDir:    configDir,
		workspaceDir: workspaceDir,
		pubkeyHex:    pubkeyHex,
		seedHash:     strings.Repeat("a", 64),
	}
	return env
}

func (e downloadFileWorkspaceAdapterTestEnv) insertSeedMeta(t *testing.T, chunkCount uint32, fileSize uint64) {
	t.Helper()
	seedPath := filepath.Join(e.configDir, e.pubkeyHex, "seeds", e.seedHash+".bse")
	if err := os.WriteFile(seedPath, []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed file failed: %v", err)
	}
	if _, err := e.db.Exec(
		`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint)
		 VALUES(?,?,?,?,?,?)
		 ON CONFLICT(seed_hash) DO UPDATE SET
		 chunk_count=excluded.chunk_count,
		 file_size=excluded.file_size,
		 seed_file_path=excluded.seed_file_path,
		 recommended_file_name=excluded.recommended_file_name,
		 mime_hint=excluded.mime_hint`,
		e.seedHash,
		int64(chunkCount),
		int64(fileSize),
		seedPath,
		"",
		"",
	); err != nil {
		t.Fatalf("upsert seed meta failed: %v", err)
	}
}
