package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv8/BFTP/pkg/infra/sqliteactor"
	"github.com/bsv8/BitFS/pkg/clientapp/download/file"
)

func newGetFileByHashTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "getfilebyhash-test.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := applySQLitePragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	if err := ensureClientDBSchemaOnDB(t.Context(), db); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return db
}

// newGetFileByHashTestStoreWithActor 创建带 actor 的 store，用于并发测试。
func newGetFileByHashTestStoreWithActor(t *testing.T) *downloadFileJobStoreAdapter {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "getfilebyhash-actor-test.sqlite")
	opened, err := sqliteactor.Open(dbPath, false)
	if err != nil {
		t.Fatalf("open sqliteactor: %v", err)
	}
	t.Cleanup(func() {
		if opened.Actor != nil {
			_ = opened.Actor.Close()
		}
		_ = opened.DB.Close()
	})

	if err := ensureClientDBSchemaOnDB(t.Context(), opened.DB); err != nil {
		t.Fatalf("schema init failed: %v", err)
	}
	return newDownloadFileJobStoreAdapter(NewClientStore(opened.DB, opened.Actor))
}

type filedownloadLocalFileStore struct {
	seedHash string
}

func (s *filedownloadLocalFileStore) FindCompleteFile(ctx context.Context, seedHash string) (filedownload.LocalFile, bool, error) {
	return filedownload.LocalFile{
		FilePath: "/tmp/" + seedHash[:8] + ".bin",
		FileSize: 1,
		SeedHash: seedHash,
	}, true, nil
}

func (s *filedownloadLocalFileStore) PreparePartFile(ctx context.Context, input filedownload.PreparePartFileInput) (filedownload.PartFile, error) {
	return filedownload.PartFile{PartFilePath: "/tmp/part-" + input.SeedHash[:8] + ".bin", SeedHash: input.SeedHash}, nil
}

func (s *filedownloadLocalFileStore) MarkChunkStored(ctx context.Context, input filedownload.StoredChunkInput) error {
	return nil
}

func (s *filedownloadLocalFileStore) CompleteFile(ctx context.Context, input filedownload.CompleteFileInput) (filedownload.LocalFile, error) {
	return filedownload.LocalFile{
		FilePath: "/tmp/" + input.SeedHash[:8] + ".bin",
		FileSize: 1,
		SeedHash: input.SeedHash,
	}, nil
}

func TestDBStoreCreateJob(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_1",
		SeedHash:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateQueued,
		ChunkCount: 10,
	}

	createdJob, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	retrieved, found := store.GetJob(ctx, "test_job_1")
	if !found {
		t.Fatalf("job not found")
	}
	if retrieved.State != filedownload.StateQueued {
		t.Fatalf("expected state %s, got %s", filedownload.StateQueued, retrieved.State)
	}
	if retrieved.SeedHash != job.SeedHash {
		t.Fatalf("seed hash mismatch")
	}
	if createdJob.JobID != retrieved.JobID {
		t.Fatalf("created job id mismatch")
	}
}

func TestDBStoreFindJobBySeedHash(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()
	seedHash := "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	job := &filedownload.Job{
		JobID:      "test_job_seed",
		SeedHash:   seedHash,
		State:      filedownload.StateRunning,
		ChunkCount: 5,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	foundJob, found, err := store.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		t.Fatalf("find job failed: %v", err)
	}
	if !found {
		t.Fatalf("expected to find job by seed hash")
	}
	if foundJob.JobID != "test_job_seed" {
		t.Fatalf("expected job_id=test_job_seed, got %s", foundJob.JobID)
	}
}

func TestDBStoreUpdateJobState(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_state",
		SeedHash:   "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateQueued,
		ChunkCount: 3,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	if err := store.UpdateJobState(ctx, "test_job_state", filedownload.StateRunning); err != nil {
		t.Fatalf("update job state failed: %v", err)
	}

	retrieved, _ := store.GetJob(ctx, "test_job_state")
	if retrieved.State != filedownload.StateRunning {
		t.Fatalf("expected state=running, got %s", retrieved.State)
	}
}

func TestDBStoreSetOutputPathAndDoneKeepsCounters(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_done",
		SeedHash:   "d3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateRunning,
		ChunkCount: 2,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	if err := store.AppendChunkReport(ctx, "test_job_done", filedownload.ChunkReport{
		ChunkIndex:    0,
		State:         filedownload.ChunkStateStored,
		SellerPubkey:  "seller1",
		ChunkPriceSat: 100,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append chunk report failed: %v", err)
	}

	before, _ := store.GetJob(ctx, "test_job_done")
	time.Sleep(time.Second)

	if err := store.SetOutputPath(ctx, "test_job_done", "/tmp/final-done.bin"); err != nil {
		t.Fatalf("set output path failed: %v", err)
	}
	if err := store.UpdateJobState(ctx, "test_job_done", filedownload.StateDone); err != nil {
		t.Fatalf("update job state to done failed: %v", err)
	}

	retrieved, _ := store.GetJob(ctx, "test_job_done")
	if retrieved.State != filedownload.StateDone {
		t.Fatalf("expected state=done, got %s", retrieved.State)
	}
	if retrieved.OutputFilePath != "/tmp/final-done.bin" {
		t.Fatalf("expected output_file_path=/tmp/final-done.bin, got %s", retrieved.OutputFilePath)
	}
	if retrieved.CompletedChunks != before.CompletedChunks {
		t.Fatalf("expected completed_chunks to stay %d, got %d", before.CompletedChunks, retrieved.CompletedChunks)
	}
	if retrieved.PaidTotalSat != before.PaidTotalSat {
		t.Fatalf("expected paid_total_sat to stay %d, got %d", before.PaidTotalSat, retrieved.PaidTotalSat)
	}
	if retrieved.UpdatedAt.Unix() <= before.UpdatedAt.Unix() {
		t.Fatalf("expected updated_at to move forward, before=%d after=%d", before.UpdatedAt.Unix(), retrieved.UpdatedAt.Unix())
	}
}

func TestDBStoreSetDemandID(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_demand",
		SeedHash:   "d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateQueued,
		ChunkCount: 2,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	if err := store.SetDemandID(ctx, "test_job_demand", "demand_abc123"); err != nil {
		t.Fatalf("set demand id failed: %v", err)
	}

	retrieved, _ := store.GetJob(ctx, "test_job_demand")
	if retrieved.DemandID != "demand_abc123" {
		t.Fatalf("expected demand_id=demand_abc123, got %s", retrieved.DemandID)
	}
}

func TestDBStoreSetError(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_error",
		SeedHash:   "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
		State:      filedownload.StateRunning,
		ChunkCount: 1,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	if err := store.SetError(ctx, "test_job_error", "pool close failed"); err != nil {
		t.Fatalf("set error failed: %v", err)
	}

	retrieved, found := store.GetJob(ctx, "test_job_error")
	if !found {
		t.Fatalf("job not found")
	}
	if retrieved.Error != "pool close failed" {
		t.Fatalf("expected error=pool close failed, got %s", retrieved.Error)
	}
}

func TestDBStoreAppendChunkReport(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_chunk",
		SeedHash:   "e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateRunning,
		ChunkCount: 3,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	chunks := []filedownload.ChunkReport{
		{ChunkIndex: 0, State: filedownload.ChunkStateStored, SellerPubkey: "seller_x", ChunkPriceSat: 100, Selected: true, SpeedBps: 1500},
		{ChunkIndex: 1, State: filedownload.ChunkStateStored, SellerPubkey: "seller_x", ChunkPriceSat: 100, Selected: true, SpeedBps: 2000},
		{ChunkIndex: 2, State: filedownload.ChunkStateFailed, SellerPubkey: "seller_y", ChunkPriceSat: 150, Selected: false, RejectReason: "timeout"},
	}

	for _, chunk := range chunks {
		if err := store.AppendChunkReport(ctx, "test_job_chunk", chunk); err != nil {
			t.Fatalf("append chunk failed: %v", err)
		}
	}

	storedChunks, found := store.ListChunks(ctx, "test_job_chunk")
	if !found {
		t.Fatalf("chunks not found")
	}
	if len(storedChunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(storedChunks))
	}
}

func TestDBStoreAppendChunkReportSortedAndNoDoubleCount(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_chunk_sorted",
		SeedHash:   "e6f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
		State:      filedownload.StateRunning,
		ChunkCount: 2,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	if err := store.AppendChunkReport(ctx, "test_job_chunk_sorted", filedownload.ChunkReport{
		ChunkIndex:    1,
		State:         filedownload.ChunkStateStored,
		SellerPubkey:  "seller_y",
		ChunkPriceSat: 150,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append chunk 1 failed: %v", err)
	}
	if err := store.AppendChunkReport(ctx, "test_job_chunk_sorted", filedownload.ChunkReport{
		ChunkIndex:    0,
		State:         filedownload.ChunkStateStored,
		SellerPubkey:  "seller_x",
		ChunkPriceSat: 100,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append chunk 0 failed: %v", err)
	}
	if err := store.AppendChunkReport(ctx, "test_job_chunk_sorted", filedownload.ChunkReport{
		ChunkIndex:    1,
		State:         filedownload.ChunkStateStored,
		SellerPubkey:  "seller_y",
		ChunkPriceSat: 150,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append duplicate chunk 1 failed: %v", err)
	}

	chunks, found := store.ListChunks(ctx, "test_job_chunk_sorted")
	if !found {
		t.Fatalf("chunks not found")
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0].ChunkIndex != 0 || chunks[1].ChunkIndex != 1 {
		t.Fatalf("expected chunk indexes [0 1], got [%d %d]", chunks[0].ChunkIndex, chunks[1].ChunkIndex)
	}

	retrieved, foundJob := store.GetJob(ctx, "test_job_chunk_sorted")
	if !foundJob {
		t.Fatalf("job not found")
	}
	if retrieved.CompletedChunks != 2 {
		t.Fatalf("expected completed_chunks=2, got %d", retrieved.CompletedChunks)
	}
	if retrieved.PaidTotalSat != 250 {
		t.Fatalf("expected paid_total_sat=250, got %d", retrieved.PaidTotalSat)
	}
}

func TestDBStoreAppendQuote(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_quote",
		SeedHash:   "f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      filedownload.StateQueued,
		ChunkCount: 5,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	quote := filedownload.QuoteReport{
		SellerPubkey:        "seller_quote_1",
		SeedPriceSat:        1000,
		ChunkPriceSat:       100,
		ChunkCount:          5,
		AvailableChunks:     []uint32{0, 1, 2, 3, 4},
		RecommendedFileName: "test.bin",
		MimeType:            "application/octet-stream",
		FileSizeBytes:       1024000,
		QuoteTimestamp:      time.Now().Unix(),
		ExpiresAtUnix:       time.Now().Add(5 * time.Minute).Unix(),
	}

	if err := store.AppendQuote(ctx, "test_job_quote", quote); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}

	storedQuotes, found := store.ListQuotes(ctx, "test_job_quote")
	if !found {
		t.Fatalf("quotes not found")
	}
	if len(storedQuotes) != 1 {
		t.Fatalf("expected 1 quote, got %d", len(storedQuotes))
	}
	if storedQuotes[0].SeedPriceSat != 1000 {
		t.Fatalf("expected seed_price_sat=1000, got %d", storedQuotes[0].SeedPriceSat)
	}
	if len(storedQuotes[0].AvailableChunks) != 5 {
		t.Fatalf("expected 5 available chunks, got %d", len(storedQuotes[0].AvailableChunks))
	}
}

func TestDBStoreSetOutputPath(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_output",
		SeedHash:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
		State:      filedownload.StateDone,
		ChunkCount: 1,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	outputPath := "/workspace/downloads/test.bin"
	if err := store.SetOutputPath(ctx, "test_job_output", outputPath); err != nil {
		t.Fatalf("set output path failed: %v", err)
	}

	retrieved, _ := store.GetJob(ctx, "test_job_output")
	if retrieved.OutputFilePath != outputPath {
		t.Fatalf("expected output_path=%s, got %s", outputPath, retrieved.OutputFilePath)
	}
}

func TestDBStoreListJobs(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		job := &filedownload.Job{
			JobID:      "test_job_list_" + string(rune('a'+i)),
			SeedHash:   "000000000000000000000000000000000000000000000000000000000000000" + string(rune('a'+i)),
			State:      filedownload.StateQueued,
			ChunkCount: uint32(i + 1),
		}
		_, created, err := store.CreateJob(ctx, job)
		if err != nil {
			t.Fatalf("create job failed: %v", err)
		}
		if !created {
			t.Fatalf("expected job to be created")
		}
	}

	jobs := store.ListJobs(ctx)
	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}
}

func TestDBStoreJobNotFound(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	_, found := store.GetJob(ctx, "nonexistent_job")
	if found {
		t.Fatalf("expected job not found")
	}
}

func TestDBStoreChunkDuplicateUpdate(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_dup_chunk",
		SeedHash:   "1111111111111111111111111111111111111111111111111111111111111111",
		State:      filedownload.StateRunning,
		ChunkCount: 2,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	chunk1 := filedownload.ChunkReport{
		ChunkIndex: 0, SellerPubkey: "seller1", ChunkPriceSat: 100, Selected: true, SpeedBps: 1000,
	}
	if err := store.AppendChunkReport(ctx, "test_job_dup_chunk", chunk1); err != nil {
		t.Fatalf("append chunk failed: %v", err)
	}

	chunk2 := filedownload.ChunkReport{
		ChunkIndex: 0, SellerPubkey: "seller2", ChunkPriceSat: 150, Selected: false, RejectReason: "budget",
	}
	if err := store.AppendChunkReport(ctx, "test_job_dup_chunk", chunk2); err != nil {
		t.Fatalf("append chunk update failed: %v", err)
	}

	chunks, _ := store.ListChunks(ctx, "test_job_dup_chunk")
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk after update, got %d", len(chunks))
	}
}

func TestDBStoreQuoteDuplicateSeller(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_dup_quote",
		SeedHash:   "2222222222222222222222222222222222222222222222222222222222222222",
		State:      filedownload.StateQueued,
		ChunkCount: 5,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	quote1 := filedownload.QuoteReport{
		SellerPubkey:   "seller_dup",
		SeedPriceSat:   1000,
		ChunkPriceSat:  100,
		ChunkCount:     5,
		QuoteTimestamp: 1234567890,
	}
	if err := store.AppendQuote(ctx, "test_job_dup_quote", quote1); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}

	quote2 := filedownload.QuoteReport{
		SellerPubkey:   "seller_dup",
		SeedPriceSat:   1200,
		ChunkPriceSat:  90,
		ChunkCount:     5,
		QuoteTimestamp: 1234567899,
	}
	if err := store.AppendQuote(ctx, "test_job_dup_quote", quote2); err != nil {
		t.Fatalf("append quote update failed: %v", err)
	}

	quotes, _ := store.ListQuotes(ctx, "test_job_dup_quote")
	if len(quotes) != 1 {
		t.Fatalf("expected 1 quote after update, got %d", len(quotes))
	}
	if quotes[0].SeedPriceSat != 1200 {
		t.Fatalf("expected seed_price_sat=1200, got %d", quotes[0].SeedPriceSat)
	}
}

func TestDBStoreAvailableChunksJSON(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()

	job := &filedownload.Job{
		JobID:      "test_job_chunks_json",
		SeedHash:   "3333333333333333333333333333333333333333333333333333333333333333",
		State:      filedownload.StateQueued,
		ChunkCount: 10,
	}
	_, created, err := store.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}

	quote := filedownload.QuoteReport{
		SellerPubkey:    "seller_chunks_json",
		SeedPriceSat:    1000,
		ChunkPriceSat:   100,
		ChunkCount:      10,
		AvailableChunks: []uint32{0, 2, 4, 6, 8},
		QuoteTimestamp:  time.Now().Unix(),
		ExpiresAtUnix:   time.Now().Add(5 * time.Minute).Unix(),
	}

	if err := store.AppendQuote(ctx, "test_job_chunks_json", quote); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}

	quotes, _ := store.ListQuotes(ctx, "test_job_chunks_json")
	if len(quotes) != 1 {
		t.Fatalf("expected 1 quote, got %d", len(quotes))
	}
	if len(quotes[0].AvailableChunks) != 5 {
		t.Fatalf("expected 5 available chunks, got %d", len(quotes[0].AvailableChunks))
	}

	availableJSON, err := json.Marshal(quotes[0].AvailableChunks)
	if err != nil {
		t.Fatalf("json marshal available chunks failed: %v", err)
	}
	var restored []uint32
	if err := json.Unmarshal(availableJSON, &restored); err != nil {
		t.Fatalf("json unmarshal available chunks failed: %v", err)
	}
	if len(restored) != 5 {
		t.Fatalf("restored chunks count mismatch: got %d", len(restored))
	}
}

func TestDownloadFileConcurrentSameSeedHashReturnsSameJob(t *testing.T) {
	t.Parallel()

	store := newGetFileByHashTestStoreWithActor(t)
	ctx := context.Background()
	seedHash := "7777777777777777777777777777777777777777777777777777777777777777"
	caps := filedownload.DownloadCaps{
		Jobs:  store,
		Files: &filedownloadLocalFileStore{seedHash: seedHash},
	}

	done := make(chan string, 10)
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result, err := filedownload.StartByHash(ctx, caps, filedownload.StartRequest{
				SeedHash:   seedHash,
				ChunkCount: 3,
			})
			if err != nil {
				t.Errorf("StartByHash failed: %v", err)
				return
			}
			done <- result.JobID
		}(i)
	}

	wg.Wait()
	close(done)

	var firstJobID string
	for jobID := range done {
		if firstJobID == "" {
			firstJobID = jobID
		} else if jobID != firstJobID {
			t.Fatalf("concurrent StartByHash returned different job_ids: %s vs %s", firstJobID, jobID)
		}
	}

	if firstJobID == "" {
		t.Fatalf("no job_id was returned")
	}

	job, found, err := store.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		t.Fatalf("FindJobBySeedHash failed: %v", err)
	}
	if !found {
		t.Fatalf("job not found by seed_hash")
	}
	if job.JobID != firstJobID {
		t.Fatalf("FindJobBySeedHash returned different job: %s vs %s", job.JobID, firstJobID)
	}
}

func TestDBStoreCreateJobDuplicateSeedHashReturnsExisting(t *testing.T) {
	t.Parallel()

	db := newGetFileByHashTestDB(t)
	store := newDownloadFileJobStoreAdapter(NewClientStore(db, nil))
	ctx := context.Background()
	seedHash := "8888888888888888888888888888888888888888888888888888888888888888"

	job1 := &filedownload.Job{
		JobID:      "test_job_dup_seed_1",
		SeedHash:   seedHash,
		State:      filedownload.StateQueued,
		ChunkCount: 5,
	}
	createdJob1, created1, err := store.CreateJob(ctx, job1)
	if err != nil {
		t.Fatalf("first create job failed: %v", err)
	}
	if !created1 {
		t.Fatalf("expected first job to be created")
	}

	job2 := &filedownload.Job{
		JobID:      "test_job_dup_seed_2",
		SeedHash:   seedHash,
		State:      filedownload.StateRunning,
		ChunkCount: 10,
	}
	createdJob2, created2, err := store.CreateJob(ctx, job2)
	if err != nil {
		t.Fatalf("second create job (same seed_hash) failed: %v", err)
	}
	if created2 {
		t.Fatalf("expected second job to NOT be created (seed_hash conflict)")
	}
	if createdJob2.JobID != createdJob1.JobID {
		t.Fatalf("expected same job_id on duplicate seed_hash, got %s vs %s", createdJob2.JobID, createdJob1.JobID)
	}
	if createdJob2.State != filedownload.StateQueued {
		t.Fatalf("expected returned job to have original state, got %s", createdJob2.State)
	}
}

func TestDBStoreConcurrentSameSeedHashStress(t *testing.T) {
	t.Parallel()

	store := newGetFileByHashTestStoreWithActor(t)
	ctx := context.Background()
	seedHash := "9999999999999999999999999999999999999999999999999999999999999999"
	caps := filedownload.DownloadCaps{
		Jobs:  store,
		Files: &filedownloadLocalFileStore{seedHash: seedHash},
	}

	done := make(chan string, 20)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result, err := filedownload.StartByHash(ctx, caps, filedownload.StartRequest{
				SeedHash:   seedHash,
				ChunkCount: uint32(3 + idx%5),
			})
			if err != nil {
				t.Errorf("StartByHash failed: %v", err)
				return
			}
			done <- result.JobID
		}(i)
	}

	wg.Wait()
	close(done)

	var firstJobID string
	for jobID := range done {
		if firstJobID == "" {
			firstJobID = jobID
		} else if jobID != firstJobID {
			t.Fatalf("concurrent StartByHash returned different job_ids: %s vs %s", firstJobID, jobID)
		}
	}

	if firstJobID == "" {
		t.Fatalf("no job_id was returned")
	}
}
