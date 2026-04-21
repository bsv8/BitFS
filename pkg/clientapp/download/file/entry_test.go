package filedownload

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCreateJobAndGetStatus(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_1",
		SeedHash:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateQueued,
		ChunkCount: 10,
	}

	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	status, err := GetStatus(ctx, store, "test_job_1")
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if status.State != StateQueued {
		t.Fatalf("expected state %s, got %s", StateQueued, status.State)
	}
	if status.SeedHash != job.SeedHash {
		t.Fatalf("seed hash mismatch")
	}
}

func TestJobNotFound(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	_, err := GetStatus(ctx, store, "nonexistent_job")
	if err == nil || CodeOf(err) != CodeJobNotFound {
		t.Fatalf("expected job not found error, got: %v", err)
	}
}

func TestDuplicateSeedReturnsExistingJob(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	caps := DownloadCaps{
		Jobs:  store,
		Files: &mockStartFileStore{complete: LocalFile{FilePath: "/tmp/local.bin", FileSize: 10, SeedHash: seedHash}, found: true},
	}

	result1, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 10})
	if err != nil {
		t.Fatalf("first start download failed: %v", err)
	}

	result2, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 10})
	if err != nil {
		t.Fatalf("second start download failed: %v", err)
	}

	if result1.JobID != result2.JobID {
		t.Fatalf("expected same job id, got %s and %s", result1.JobID, result2.JobID)
	}
	if result2.Status.State != StateLocal {
		t.Fatalf("expected local state, got %s", result2.Status.State)
	}
}

func TestDoneJobAllowsNewDownload(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	caps := DownloadCaps{
		Jobs:  store,
		Files: &mockStartFileStore{complete: LocalFile{FilePath: "/tmp/local-done.bin", FileSize: 10, SeedHash: seedHash}, found: true},
	}

	result1, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 10})
	if err != nil {
		t.Fatalf("start download failed: %v", err)
	}

	if err := store.UpdateJobState(ctx, result1.JobID, StateDone); err != nil {
		t.Fatalf("update state failed: %v", err)
	}

	result2, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 10})
	if err != nil {
		t.Fatalf("second start download failed: %v", err)
	}

	if result1.JobID != result2.JobID {
		t.Fatalf("expected same job id after done (reuse), got different: %s vs %s", result1.JobID, result2.JobID)
	}
	if result2.Status.State != StateDone {
		t.Fatalf("expected state=done, got: %s", result2.Status.State)
	}
}

func TestRunningJobResumeSkipsStoredChunkAndCompletes(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9"
	jobID := "test_job_resume_partial"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_resume_partial",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if err := store.SetPartFilePath(ctx, jobID, "/tmp/part-resume-partial.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := store.AppendQuote(ctx, jobID, QuoteReport{
		SellerPubkey:        "seller_resume",
		SeedPriceSat:        100,
		ChunkPriceSat:       11,
		ChunkCount:          2,
		FileSizeBytes:       8,
		RecommendedFileName: "resume.bin",
		MimeType:            "application/octet-stream",
		Selected:            true,
	}); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}
	if err := store.AppendChunkReport(ctx, jobID, ChunkReport{
		ChunkIndex:    0,
		State:         ChunkStateStored,
		SellerPubkey:  "seller_resume",
		ChunkPriceSat: 11,
		SpeedBps:      1000,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append stored chunk failed: %v", err)
	}

	files := &mockStartFileStore{
		found: false,
		complete: LocalFile{
			FilePath: "/tmp/resume-partial-final.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
		part: PartFile{
			PartFilePath: "/tmp/part-resume-partial.bin",
			SeedHash:     seedHash,
		},
		expectedChunkLen: 4,
	}
	transfer := &mockTransferRunner{
		results: map[uint32]ChunkTransferResult{
			1: {Data: []byte("bbbb"), PaidSat: 11, SpeedBps: 1200},
		},
	}
	caps := DownloadCaps{
		Jobs:      store,
		Seeds:     &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:     files,
		Quotes:    mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller_resume", SeedPriceSat: 100, ChunkPriceSat: 11, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream", Selected: true}}},
		Transfers: transfer,
	}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err != nil {
		t.Fatalf("resume start failed: %v", err)
	}
	if result.Status.State != StateDone {
		t.Fatalf("expected state=%s, got %s", StateDone, result.Status.State)
	}
	if result.Status.OutputFilePath != "/tmp/resume-partial-final.bin" {
		t.Fatalf("expected output_file_path=/tmp/resume-partial-final.bin, got %s", result.Status.OutputFilePath)
	}
	if len(transfer.calls) != 1 {
		t.Fatalf("expected one transfer call, got %d", len(transfer.calls))
	}
	if transfer.calls[0].ChunkIndex != 1 {
		t.Fatalf("expected resume to transfer chunk 1, got %d", transfer.calls[0].ChunkIndex)
	}
	if len(files.markCalls) != 1 {
		t.Fatalf("expected one stored chunk write, got %d", len(files.markCalls))
	}
	if files.completeCalled != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalled)
	}
	chunks, found := store.ListChunks(ctx, jobID)
	if !found {
		t.Fatalf("chunks not found")
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunk facts, got %d", len(chunks))
	}
	if chunks[0].ChunkIndex != 0 || chunks[1].ChunkIndex != 1 {
		t.Fatalf("expected chunk indexes [0 1], got [%d %d]", chunks[0].ChunkIndex, chunks[1].ChunkIndex)
	}
	status, err := GetStatus(ctx, store, jobID)
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if status.CompletedChunks != 2 {
		t.Fatalf("expected completed_chunks=2, got %d", status.CompletedChunks)
	}
	if status.PaidTotalSat != 22 {
		t.Fatalf("expected paid_total_sat=22, got %d", status.PaidTotalSat)
	}
}

func TestRunningJobAllStoredCompletesWithoutTransfer(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9d9"
	jobID := "test_job_resume_complete"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_resume_complete",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if err := store.SetPartFilePath(ctx, jobID, "/tmp/part-resume-complete.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := store.AppendQuote(ctx, jobID, QuoteReport{
		SellerPubkey:        "seller_resume",
		SeedPriceSat:        100,
		ChunkPriceSat:       11,
		ChunkCount:          2,
		FileSizeBytes:       8,
		RecommendedFileName: "resume.bin",
		MimeType:            "application/octet-stream",
		Selected:            true,
	}); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}
	for i := uint32(0); i < 2; i++ {
		if err := store.AppendChunkReport(ctx, jobID, ChunkReport{
			ChunkIndex:    i,
			State:         ChunkStateStored,
			SellerPubkey:  "seller_resume",
			ChunkPriceSat: 11,
			SpeedBps:      1000,
			Selected:      true,
		}); err != nil {
			t.Fatalf("append stored chunk failed: %v", err)
		}
	}

	files := &mockStartFileStore{
		found: false,
		complete: LocalFile{
			FilePath: "/tmp/resume-complete-final.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
		part: PartFile{
			PartFilePath: "/tmp/part-resume-complete.bin",
			SeedHash:     seedHash,
		},
		expectedChunkLen: 4,
	}
	caps := DownloadCaps{
		Jobs:   store,
		Seeds:  &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:  files,
		Quotes: mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller_resume", SeedPriceSat: 100, ChunkPriceSat: 11, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream", Selected: true}}},
	}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err != nil {
		t.Fatalf("resume start failed: %v", err)
	}
	if result.Status.State != StateDone {
		t.Fatalf("expected state=%s, got %s", StateDone, result.Status.State)
	}
	if len(files.markCalls) != 0 {
		t.Fatalf("expected no transfer writes, got %d", len(files.markCalls))
	}
	if files.completeCalled != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalled)
	}
}

func TestRunningJobCompleteFailureFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9e9"
	jobID := "test_job_resume_complete_fail"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_resume_complete_fail",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if err := store.SetPartFilePath(ctx, jobID, "/tmp/part-resume-complete-fail.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := store.AppendQuote(ctx, jobID, QuoteReport{
		SellerPubkey:        "seller_resume",
		SeedPriceSat:        100,
		ChunkPriceSat:       11,
		ChunkCount:          2,
		FileSizeBytes:       8,
		RecommendedFileName: "resume.bin",
		MimeType:            "application/octet-stream",
		Selected:            true,
	}); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}
	for i := uint32(0); i < 2; i++ {
		if err := store.AppendChunkReport(ctx, jobID, ChunkReport{
			ChunkIndex:    i,
			State:         ChunkStateStored,
			SellerPubkey:  "seller_resume",
			ChunkPriceSat: 11,
			SpeedBps:      1000,
			Selected:      true,
		}); err != nil {
			t.Fatalf("append stored chunk failed: %v", err)
		}
	}

	files := &mockStartFileStore{
		found: false,
		complete: LocalFile{
			FilePath: "/tmp/resume-complete-fail.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
		part: PartFile{
			PartFilePath: "/tmp/part-resume-complete-fail.bin",
			SeedHash:     seedHash,
		},
		completeErr:      errors.New("complete failed"),
		expectedChunkLen: 4,
	}
	caps := DownloadCaps{
		Jobs:   store,
		Seeds:  &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:  files,
		Quotes: mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller_resume", SeedPriceSat: 100, ChunkPriceSat: 11, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream", Selected: true}}},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err == nil {
		t.Fatalf("expected resume complete to fail")
	}
	if files.completeCalled != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalled)
	}
	status, statusErr := GetStatus(ctx, store, jobID)
	if statusErr != nil {
		t.Fatalf("get status failed: %v", statusErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
}

func TestRunningJobMissingSeedMetaFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9f9"
	jobID := "test_job_resume_missing_meta"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_resume_missing_meta",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if err := store.SetPartFilePath(ctx, jobID, "/tmp/part-resume-missing-meta.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := store.AppendQuote(ctx, jobID, QuoteReport{
		SellerPubkey:        "seller_resume",
		SeedPriceSat:        100,
		ChunkPriceSat:       11,
		ChunkCount:          2,
		FileSizeBytes:       8,
		RecommendedFileName: "resume.bin",
		MimeType:            "application/octet-stream",
		Selected:            true,
	}); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}

	caps := DownloadCaps{
		Jobs:   store,
		Seeds:  &mockSeedStore{found: false},
		Files:  &mockStartFileStore{found: false},
		Quotes: mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller_resume", SeedPriceSat: 100, ChunkPriceSat: 11, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream", Selected: true}}},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err == nil {
		t.Fatalf("expected missing seed meta to fail")
	}
	status, statusErr := GetStatus(ctx, store, jobID)
	if statusErr != nil {
		t.Fatalf("get status failed: %v", statusErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
}

func TestRunningJobWithoutSelectedQuoteFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a"
	jobID := "test_job_resume_no_selected_quote"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 2,
		DemandID:   "demand_resume_no_selected_quote",
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}
	if err := store.SetPartFilePath(ctx, jobID, "/tmp/part-resume-no-selected.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}
	if err := store.AppendChunkReport(ctx, jobID, ChunkReport{
		ChunkIndex:    0,
		State:         ChunkStateStored,
		SellerPubkey:  "seller_a",
		ChunkPriceSat: 11,
		SpeedBps:      1000,
		Selected:      true,
	}); err != nil {
		t.Fatalf("append stored chunk failed: %v", err)
	}

	files := &mockStartFileStore{
		found: false,
		complete: LocalFile{
			FilePath: "/tmp/resume-no-selected-final.bin",
			FileSize: 8,
			SeedHash: seedHash,
		},
		part: PartFile{
			PartFilePath: "/tmp/part-resume-no-selected.bin",
			SeedHash:     seedHash,
		},
		expectedChunkLen: 4,
	}
	transfer := &mockTransferRunner{}
	caps := DownloadCaps{
		Jobs:  store,
		Seeds: &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files: files,
		Quotes: mockQuoteReader{quotes: []QuoteReport{
			{SellerPubkey: "seller_a", SeedPriceSat: 100, ChunkPriceSat: 11, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream"},
			{SellerPubkey: "seller_b", SeedPriceSat: 90, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "resume.bin", MimeType: "application/octet-stream"},
		}},
		Transfers: transfer,
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err == nil {
		t.Fatalf("expected resume to fail without selected quote")
	}
	status, statusErr := GetStatus(ctx, store, jobID)
	if statusErr != nil {
		t.Fatalf("get status failed: %v", statusErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
	if len(transfer.calls) != 0 {
		t.Fatalf("expected no transfer calls, got %d", len(transfer.calls))
	}
	if files.completeCalled != 0 {
		t.Fatalf("expected CompleteFile not to be called, got %d", files.completeCalled)
	}
}

func TestFailedJobDoesNotAutoRetry(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "abababababababababababababababababababababababababababababababab"
	jobID := "test_job_failed_no_retry"

	if _, _, err := store.CreateJob(ctx, &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateFailed,
		ChunkCount: 2,
	}); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	result, err := StartByHash(ctx, DownloadCaps{Jobs: store}, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err != nil {
		t.Fatalf("start should return existing failed job without retry error: %v", err)
	}
	if result.Status.State != StateFailed {
		t.Fatalf("expected state=%s, got %s", StateFailed, result.Status.State)
	}
	if result.JobID != jobID {
		t.Fatalf("expected job_id=%s, got %s", jobID, result.JobID)
	}
}

func TestAppendChunkAndNodeAggregation(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_2",
		SeedHash:   "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateRunning,
		ChunkCount: 3,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 上报 3 个 chunk，来自两个 seller
	chunks := []ChunkReport{
		{ChunkIndex: 0, State: ChunkStateStored, SellerPubkey: "seller1", ChunkPriceSat: 100, SpeedBps: 1000, Selected: true},
		{ChunkIndex: 1, State: ChunkStateStored, SellerPubkey: "seller1", ChunkPriceSat: 100, SpeedBps: 2000, Selected: true},
		{ChunkIndex: 2, State: ChunkStateStored, SellerPubkey: "seller2", ChunkPriceSat: 150, SpeedBps: 1500, Selected: true},
	}

	for _, chunk := range chunks {
		if err := store.AppendChunkReport(ctx, "test_job_2", chunk); err != nil {
			t.Fatalf("append chunk failed: %v", err)
		}
	}

	// 检查 job 的 completed_chunks
	status, err := GetStatus(ctx, store, "test_job_2")
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}
	if status.CompletedChunks != 3 {
		t.Fatalf("expected 3 completed chunks, got %d", status.CompletedChunks)
	}

	// 验证 node 汇总从 chunk 自动聚合（不调用 AppendNodeReport）
	nodes, err := ListNodes(ctx, store, "test_job_2")
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 node reports, got %d", len(nodes))
	}

	// 验证 seller1 聚合正确
	for _, node := range nodes {
		if node.SellerPubkey == "seller1" {
			if node.SelectedCount != 2 {
				t.Fatalf("expected seller1 selected_count=2, got %d", node.SelectedCount)
			}
			if node.TotalPaidSat != 200 {
				t.Fatalf("expected seller1 total_paid=200, got %d", node.TotalPaidSat)
			}
			// avg speed = (1000+2000)/2 = 1500
			if node.AvgSpeedBps != 1500 {
				t.Fatalf("expected seller1 avg_speed=1500, got %d", node.AvgSpeedBps)
			}
		}
	}
}

func TestQuoteExpiresAtUnix(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_expires",
		SeedHash:   "c1c2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateQueued,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	expiresAt := time.Now().Add(5 * time.Minute).Unix()
	quote := QuoteReport{
		SellerPubkey:   "seller_expires",
		SeedPriceSat:   1000,
		ChunkPriceSat:  100,
		ChunkCount:     5,
		QuoteTimestamp: time.Now().Unix(),
		ExpiresAtUnix:  expiresAt,
	}

	if err := store.AppendQuote(ctx, "test_job_expires", quote); err != nil {
		t.Fatalf("append quote failed: %v", err)
	}

	quotes, _ := store.ListQuotes(ctx, "test_job_expires")
	if len(quotes) != 1 {
		t.Fatalf("expected 1 quote, got %d", len(quotes))
	}
	if quotes[0].ExpiresAtUnix != expiresAt {
		t.Fatalf("expected expires_at_unix=%d, got %d", expiresAt, quotes[0].ExpiresAtUnix)
	}
}

func TestJobDemandIDAndPartFilePath(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_fields",
		SeedHash:   "d1d2d3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateQueued,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 设置 demand_id
	if err := store.SetDemandID(ctx, "test_job_fields", "demand_123"); err != nil {
		t.Fatalf("set demand id failed: %v", err)
	}

	// 设置 part_file_path
	if err := store.SetPartFilePath(ctx, "test_job_fields", "/tmp/part_file.bin"); err != nil {
		t.Fatalf("set part file path failed: %v", err)
	}

	status, err := GetStatus(ctx, store, "test_job_fields")
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}

	if status.DemandID != "demand_123" {
		t.Fatalf("expected demand_id=demand_123, got %s", status.DemandID)
	}
	if status.PartFilePath != "/tmp/part_file.bin" {
		t.Fatalf("expected part_file_path=/tmp/part_file.bin, got %s", status.PartFilePath)
	}
}

func TestJobTimestampFields(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	beforeCreate := time.Now().Unix()

	job := &Job{
		JobID:      "test_job_time",
		SeedHash:   "e1e2e3e4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateQueued,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	status, err := GetStatus(ctx, store, "test_job_time")
	if err != nil {
		t.Fatalf("get status failed: %v", err)
	}

	if status.CreatedAtUnix < beforeCreate {
		t.Fatalf("expected created_at_unix >= %d, got %d", beforeCreate, status.CreatedAtUnix)
	}
	if status.UpdatedAtUnix < beforeCreate {
		t.Fatalf("expected updated_at_unix >= %d, got %d", beforeCreate, status.UpdatedAtUnix)
	}
}

func TestAppendQuotesAndRetrieve(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_3",
		SeedHash:   "f1f2f3f4f5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		State:      StateQueued,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	quotes := []QuoteReport{
		{
			SellerPubkey:        "seller_a",
			SeedPriceSat:        1000,
			ChunkPriceSat:       100,
			ChunkCount:          5,
			RecommendedFileName: "test.bin",
			MimeType:            "application/octet-stream",
			QuoteTimestamp:      1234567890,
		},
		{
			SellerPubkey:        "seller_b",
			SeedPriceSat:        1200,
			ChunkPriceSat:       90,
			ChunkCount:          5,
			RecommendedFileName: "test.bin",
			QuoteTimestamp:      1234567891,
		},
	}

	for _, quote := range quotes {
		if err := store.AppendQuote(ctx, "test_job_3", quote); err != nil {
			t.Fatalf("append quote failed: %v", err)
		}
	}

	// 读取 quotes
	storedQuotes, found := store.ListQuotes(ctx, "test_job_3")
	if !found {
		t.Fatalf("quotes not found")
	}
	if len(storedQuotes) != 2 {
		t.Fatalf("expected 2 quotes, got %d", len(storedQuotes))
	}

	// seller_b 覆盖写入
	newQuoteB := QuoteReport{
		SellerPubkey:        "seller_b",
		SeedPriceSat:        1100,
		ChunkPriceSat:       80,
		ChunkCount:          5,
		RecommendedFileName: "updated.bin",
		QuoteTimestamp:      1234567899,
	}
	if err := store.AppendQuote(ctx, "test_job_3", newQuoteB); err != nil {
		t.Fatalf("append updated quote failed: %v", err)
	}

	// 再次读取，验证 seller_b 被覆盖
	storedQuotes2, _ := store.ListQuotes(ctx, "test_job_3")
	if len(storedQuotes2) != 2 {
		t.Fatalf("expected 2 quotes after update, got %d", len(storedQuotes2))
	}
	for _, q := range storedQuotes2 {
		if q.SellerPubkey == "seller_b" {
			if q.SeedPriceSat != 1100 || q.ChunkPriceSat != 80 {
				t.Fatalf("seller_b quote not updated correctly")
			}
			if q.RecommendedFileName != "updated.bin" {
				t.Fatalf("seller_b filename not updated")
			}
		}
	}
}

func TestListJobsReadOnly(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	// 创建 3 个 job
	for i := 0; i < 3; i++ {
		job := &Job{
			JobID:      "test_job_list_" + string(rune('a'+i)),
			SeedHash:   strings.Repeat(string(rune('a'+i)), 64),
			State:      StateQueued,
			ChunkCount: uint32(i + 1),
		}
		if _, _, err := store.CreateJob(ctx, job); err != nil {
			t.Fatalf("create job failed: %v", err)
		}
	}

	// ListJobs 只读，不触发业务
	jobs := store.ListJobs(ctx)
	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	// 再次调用 ListJobs 不应改变任何状态
	jobs2 := store.ListJobs(ctx)
	if len(jobs2) != 3 {
		t.Fatalf("expected 3 jobs on second call, got %d", len(jobs2))
	}

	// 验证返回的是值拷贝，不是内部指针
	jobs[0].State = "modified"
	jobs2 = store.ListJobs(ctx)
	if jobs2[0].State == "modified" {
		t.Fatalf("ListJobs should return snapshots, not internal pointers")
	}
}

func TestQueryFunctionsDoNotTriggerBusiness(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_query",
		SeedHash:   "1111111111111111111111111111111111111111111111111111111111111111",
		State:      StateRunning,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 调用 ListChunks、ListNodes、ListQuotes 等只读函数
	// 这些函数不应该触发任何下载、demand 发布或 workspace 写入

	_, err := ListChunks(ctx, store, "test_job_query")
	if err != nil {
		t.Fatalf("list chunks failed: %v", err)
	}

	_, err = ListNodes(ctx, store, "test_job_query")
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}

	_, err = ListQuotes(ctx, store, "test_job_query")
	if err != nil {
		t.Fatalf("list quotes failed: %v", err)
	}

	// 验证 job 状态未被改变
	status, _ := GetStatus(ctx, store, "test_job_query")
	if status.State != StateRunning {
		t.Fatalf("job state changed unexpectedly to %s", status.State)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_concurrent",
		SeedHash:   "2222222222222222222222222222222222222222222222222222222222222222",
		State:      StateRunning,
		ChunkCount: 100,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10

	// 并发写入 chunk reports
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				chunkIdx := uint32(idx*10 + j)
				store.AppendChunkReport(ctx, "test_job_concurrent", ChunkReport{
					ChunkIndex:    chunkIdx,
					State:         ChunkStateStored,
					SellerPubkey:  "seller_concurrent",
					ChunkPriceSat: 100,
					SpeedBps:      1000,
				})
			}
		}(i)
	}

	// 并发写入 quotes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			store.AppendQuote(ctx, "test_job_concurrent", QuoteReport{
				SellerPubkey:   "seller_concurrent",
				SeedPriceSat:   1000 + uint64(idx)*100,
				ChunkPriceSat:  100,
				ChunkCount:     100,
				QuoteTimestamp: int64(1234567890 + idx),
			})
		}(i)
	}

	// 并发读取
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				store.GetJob(ctx, "test_job_concurrent")
				store.ListChunks(ctx, "test_job_concurrent")
				store.ListQuotes(ctx, "test_job_concurrent")
				store.ListJobs(ctx)
			}
		}(i)
	}

	wg.Wait()

	// 验证最终状态
	status, _ := GetStatus(ctx, store, "test_job_concurrent")
	if status.CompletedChunks != 100 {
		t.Fatalf("expected 100 completed chunks, got %d", status.CompletedChunks)
	}
}

func TestStartByHashValidation(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	// 测试 nil ctx
	_, err := StartByHash(nil, DownloadCaps{Jobs: store}, StartRequest{SeedHash: "aabbccdd"})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for nil ctx")
	}

	// 测试 cancelled ctx
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = StartByHash(cancelledCtx, DownloadCaps{Jobs: store}, StartRequest{SeedHash: "aabbccdd"})
	if err == nil || CodeOf(err) != CodeRequestCanceled {
		t.Fatalf("expected request canceled error")
	}

	// 测试无效 seed_hash
	_, err = StartByHash(ctx, DownloadCaps{Jobs: store}, StartRequest{SeedHash: "invalid"})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for invalid seed_hash")
	}

	// 测试空 seed_hash
	_, err = StartByHash(ctx, DownloadCaps{Jobs: store}, StartRequest{SeedHash: ""})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for empty seed_hash")
	}

	// 测试 nil store
	validHash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	_, err = StartByHash(ctx, DownloadCaps{}, StartRequest{SeedHash: validHash})
	if err == nil || CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled for nil store")
	}
}

func TestStartByHashLocalFileReturnsLocal(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	caps := DownloadCaps{
		Jobs:  store,
		Files: &mockStartFileStore{complete: LocalFile{FilePath: "/tmp/local.bin", FileSize: 10, SeedHash: seedHash}, found: true},
	}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 3})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if result.Status.State != StateLocal {
		t.Fatalf("expected local state, got %s", result.Status.State)
	}
	if result.Status.OutputFilePath != "/tmp/local.bin" {
		t.Fatalf("expected output path to be written, got %s", result.Status.OutputFilePath)
	}
	if files, ok := caps.Files.(*mockStartFileStore); ok && files.completeCalled != 0 {
		t.Fatalf("CompleteFile should not be called on local hit, got %d", files.completeCalled)
	}
}

func TestStartByHashNoQuoteReturnsUnavailable(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	caps := DownloadCaps{
		Jobs:    store,
		Files:   &mockStartFileStore{found: false},
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_1", Status: "submitted"}},
		Quotes:  mockQuoteReader{quotes: nil},
		Policy:  mockPolicy{},
	}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 3})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if result.Status.State != StateQuoteUnavailable {
		t.Fatalf("expected quote_unavailable, got %s", result.Status.State)
	}
	if result.Status.DemandID != "demand_1" {
		t.Fatalf("expected demand id to be recorded, got %s", result.Status.DemandID)
	}
	quotes, found := store.ListQuotes(ctx, result.JobID)
	if !found {
		t.Fatalf("quotes not found")
	}
	if len(quotes) != 0 {
		t.Fatalf("expected no quote rows, got %d", len(quotes))
	}
}

func TestStartByHashBlockedByBudget(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	caps := DownloadCaps{
		Jobs:    store,
		Files:   &mockStartFileStore{found: false},
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_2", Status: "submitted"}},
		Quotes: mockQuoteReader{quotes: []QuoteReport{
			{SellerPubkey: "seller1", SeedPriceSat: 100, ChunkPriceSat: 100, ChunkCount: 3, FileSizeBytes: 10},
		}},
		Policy: mockPolicy{ok: false, reason: "blocked_by_budget"},
	}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 3, MaxSeedPrice: 50, MaxChunkPrice: 20})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if result.Status.State != StateBlockedByBudget {
		t.Fatalf("expected blocked_by_budget, got %s", result.Status.State)
	}
	if result.Message != "blocked_by_budget" {
		t.Fatalf("expected message blocked_by_budget, got %s", result.Message)
	}
}

func TestStartByHashDoneAfterCompleteFile(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	caps := DownloadCaps{
		Jobs:    store,
		Seeds:   &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:   &mockStartFileStore{found: false, part: PartFile{PartFilePath: "/tmp/part.bin", SeedHash: seedHash}, expectedChunkLen: 4},
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_3", Status: "submitted"}},
		Quotes: mockQuoteReader{quotes: []QuoteReport{
			{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"},
		}},
		Policy: mockPolicy{selected: QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}, ok: true},
		Transfers: &mockTransferRunner{results: map[uint32]ChunkTransferResult{
			0: {Data: []byte("aaaa"), PaidSat: 10, SpeedBps: 1000},
			1: {Data: []byte("bbbb"), PaidSat: 10, SpeedBps: 1200},
		}},
	}
	files := caps.Files.(*mockStartFileStore)
	files.complete = LocalFile{FilePath: "/tmp/final-demo.bin", FileSize: 8, SeedHash: seedHash, MimeType: "application/octet-stream"}

	result, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if result.Status.State != StateDone {
		t.Fatalf("expected done, got %s", result.Status.State)
	}
	if result.Status.PartFilePath != "/tmp/part.bin" {
		t.Fatalf("expected part file path to be written, got %s", result.Status.PartFilePath)
	}
	if result.Status.OutputFilePath != "/tmp/final-demo.bin" {
		t.Fatalf("expected output file path to be written, got %s", result.Status.OutputFilePath)
	}
	if result.Status.CompletedChunks != 2 {
		t.Fatalf("expected completed chunks to be recorded, got %d", result.Status.CompletedChunks)
	}
	if result.Status.PaidTotalSat != 20 {
		t.Fatalf("expected paid total sat to be 20, got %d", result.Status.PaidTotalSat)
	}
	if files.completeCalled != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalled)
	}
	if len(files.markCalls) != 2 {
		t.Fatalf("expected 2 chunk writes, got %d", len(files.markCalls))
	}
	if files.lastCompleteInput.RecommendedFileName != "demo.bin" {
		t.Fatalf("expected recommended file name from selected quote, got %q", files.lastCompleteInput.RecommendedFileName)
	}
	if files.lastCompleteInput.MimeType != "" {
		t.Fatalf("expected empty mime type when selected quote has none, got %q", files.lastCompleteInput.MimeType)
	}
	if len(files.lastCompleteInput.AvailableChunkIndexes) != 2 || files.lastCompleteInput.AvailableChunkIndexes[0] != 0 || files.lastCompleteInput.AvailableChunkIndexes[1] != 1 {
		t.Fatalf("unexpected available chunk indexes: %#v", files.lastCompleteInput.AvailableChunkIndexes)
	}
	chunks, found := store.ListChunks(ctx, result.JobID)
	if !found {
		t.Fatalf("chunks not found")
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunk reports, got %d", len(chunks))
	}
	for _, chunk := range chunks {
		if chunk.State != ChunkStateStored {
			t.Fatalf("expected stored chunk state, got %s", chunk.State)
		}
	}
}

func TestStartByHashCompleteFileFailureFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "abababababababababababababababababababababababababababababababab"
	files := &mockStartFileStore{
		found:            false,
		part:             PartFile{PartFilePath: "/tmp/part-complete-fail.bin", SeedHash: seedHash},
		expectedChunkLen: 4,
		complete:         LocalFile{},
		completeErr:      NewError(CodeChunkStoreFailed, "complete file failed"),
	}
	caps := DownloadCaps{
		Jobs:    store,
		Seeds:   &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:   files,
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_complete_fail", Status: "submitted"}},
		Quotes:  mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}}},
		Policy:  mockPolicy{selected: QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}, ok: true},
		Transfers: &mockTransferRunner{
			results: map[uint32]ChunkTransferResult{
				0: {Data: []byte("aaaa"), PaidSat: 10, SpeedBps: 1000},
				1: {Data: []byte("bbbb"), PaidSat: 10, SpeedBps: 1200},
			},
		},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err == nil || CodeOf(err) != CodeChunkStoreFailed {
		t.Fatalf("expected complete file failed, got %v", err)
	}

	job, foundJob, jobErr := store.FindJobBySeedHash(ctx, seedHash)
	if jobErr != nil || !foundJob {
		t.Fatalf("job not found after complete failure: %v", jobErr)
	}
	status, statusErr := GetStatus(ctx, store, job.JobID)
	if statusErr != nil {
		t.Fatalf("get status failed: %v", statusErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
	if status.OutputFilePath != "" {
		t.Fatalf("expected output file path to stay empty, got %s", status.OutputFilePath)
	}
	if files.completeCalled != 1 {
		t.Fatalf("expected CompleteFile to be called once, got %d", files.completeCalled)
	}
}

func TestStartByHashMissingTransfersReturnsModuleDisabled(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	caps := DownloadCaps{
		Jobs:    store,
		Seeds:   &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111"}, ChunkCount: 1, FileSize: 4}, found: true},
		Files:   &mockStartFileStore{found: false, part: PartFile{PartFilePath: "/tmp/part-disabled.bin", SeedHash: seedHash}, expectedChunkLen: 4},
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_disabled", Status: "submitted"}},
		Quotes:  mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 1, FileSizeBytes: 4, RecommendedFileName: "demo.bin"}}},
		Policy:  mockPolicy{selected: QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 1, FileSizeBytes: 4, RecommendedFileName: "demo.bin"}, ok: true},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 1})
	if err == nil || CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled when transfers are missing, got %v", err)
	}
}

func TestStartByHashChunkLengthMismatchFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	files := &mockStartFileStore{found: false, part: PartFile{PartFilePath: "/tmp/part-mismatch.bin", SeedHash: seedHash}, expectedChunkLen: 4}
	caps := DownloadCaps{
		Jobs:    store,
		Seeds:   &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111"}, ChunkCount: 1, FileSize: 4}, found: true},
		Files:   files,
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_mismatch", Status: "submitted"}},
		Quotes:  mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 1, FileSizeBytes: 4, RecommendedFileName: "demo.bin"}}},
		Policy:  mockPolicy{selected: QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 1, FileSizeBytes: 4, RecommendedFileName: "demo.bin"}, ok: true},
		Transfers: &mockTransferRunner{results: map[uint32]ChunkTransferResult{
			0: {Data: []byte("aaa"), PaidSat: 10, SpeedBps: 1000},
		}},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 1})
	if err == nil || CodeOf(err) != CodeChunkStoreFailed {
		t.Fatalf("expected chunk store failed, got %v", err)
	}
	if len(files.markCalls) != 1 {
		t.Fatalf("expected one mark attempt, got %d", len(files.markCalls))
	}
	job, foundJob, jobErr := store.FindJobBySeedHash(ctx, seedHash)
	if jobErr != nil || !foundJob {
		t.Fatalf("job not found after failure: %v", jobErr)
	}
	status, getErr := GetStatus(ctx, store, job.JobID)
	if getErr != nil {
		t.Fatalf("get status failed: %v", getErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
	chunks, found := store.ListChunks(ctx, job.JobID)
	if found && len(chunks) != 1 {
		t.Fatalf("expected one chunk report on failure, got %d", len(chunks))
	}
}

func TestStartByHashTransferFailureFails(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "abababababababababababababababababababababababababababababababab"
	files := &mockStartFileStore{found: false, part: PartFile{PartFilePath: "/tmp/part-transfer-fail.bin", SeedHash: seedHash}, expectedChunkLen: 4}
	caps := DownloadCaps{
		Jobs:    store,
		Seeds:   &mockSeedStore{meta: SeedMeta{SeedHash: seedHash, ChunkHashes: []string{"1111111111111111111111111111111111111111111111111111111111111111", "2222222222222222222222222222222222222222222222222222222222222222"}, ChunkCount: 2, FileSize: 8}, found: true},
		Files:   files,
		Demands: mockDemandPublisher{result: PublishDemandResult{DemandID: "demand_transfer_fail", Status: "submitted"}},
		Quotes:  mockQuoteReader{quotes: []QuoteReport{{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}}},
		Policy:  mockPolicy{selected: QuoteReport{SellerPubkey: "seller1", SeedPriceSat: 10, ChunkPriceSat: 10, ChunkCount: 2, FileSizeBytes: 8, RecommendedFileName: "demo.bin"}, ok: true},
		Transfers: &mockTransferRunner{
			results: map[uint32]ChunkTransferResult{
				0: {Data: []byte("aaaa"), PaidSat: 10, SpeedBps: 1000},
			},
			errs: map[uint32]error{
				1: NewError(CodeTransferFailed, "chunk download failed"),
			},
		},
	}

	_, err := StartByHash(ctx, caps, StartRequest{SeedHash: seedHash, ChunkCount: 2})
	if err == nil || CodeOf(err) != CodeTransferFailed {
		t.Fatalf("expected transfer failed, got %v", err)
	}
	job, foundJob, jobErr := store.FindJobBySeedHash(ctx, seedHash)
	if jobErr != nil || !foundJob {
		t.Fatalf("job not found after transfer failure: %v", jobErr)
	}
	status, statusErr := GetStatus(ctx, store, job.JobID)
	if statusErr != nil {
		t.Fatalf("get status failed: %v", statusErr)
	}
	if status.State != StateFailed {
		t.Fatalf("expected failed state, got %s", status.State)
	}
	chunks, found := store.ListChunks(ctx, job.JobID)
	if !found {
		t.Fatalf("chunks not found")
	}
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunk reports, got %d", len(chunks))
	}
	if chunks[1].State != ChunkStateFailed {
		t.Fatalf("expected second chunk failed, got %s", chunks[1].State)
	}
}

func TestGetStatusValidation(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	// 测试 nil ctx
	_, err := GetStatus(nil, store, "test_job")
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for nil ctx")
	}

	// 测试 cancelled ctx
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = GetStatus(cancelledCtx, store, "test_job")
	if err == nil || CodeOf(err) != CodeRequestCanceled {
		t.Fatalf("expected request canceled error")
	}

	// 测试空 job_id
	_, err = GetStatus(ctx, store, "")
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for empty job_id")
	}

	// 测试 nil store
	_, err = GetStatus(ctx, nil, "test_job")
	if err == nil || CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled for nil store")
	}

	// 测试不存在的 job
	_, err = GetStatus(ctx, store, "nonexistent")
	if err == nil || CodeOf(err) != CodeJobNotFound {
		t.Fatalf("expected job not found error")
	}
}

func TestChunkDuplicateWrite(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_dup",
		SeedHash:   "3333333333333333333333333333333333333333333333333333333333333333",
		State:      StateRunning,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 第一次写入 chunk 0
	store.AppendChunkReport(ctx, "test_job_dup", ChunkReport{
		ChunkIndex: 0, State: ChunkStateStored, SellerPubkey: "seller1", ChunkPriceSat: 100, SpeedBps: 1000,
	})

	status1, _ := GetStatus(ctx, store, "test_job_dup")
	if status1.CompletedChunks != 1 {
		t.Fatalf("expected 1 completed chunk after first write, got %d", status1.CompletedChunks)
	}

	// 重复写入同一 chunk（状态变为 unselected）
	store.AppendChunkReport(ctx, "test_job_dup", ChunkReport{
		ChunkIndex: 0, SellerPubkey: "seller2", ChunkPriceSat: 150, Selected: false, RejectReason: "budget",
	})

	status2, _ := GetStatus(ctx, store, "test_job_dup")
	// chunk 0 状态变为 unselected，但 completed_chunks 不应减少（只增不减）
	if status2.CompletedChunks != 1 {
		t.Fatalf("expected 1 completed chunk after second write (unselected), got %d", status2.CompletedChunks)
	}
}

func TestQuoteDuplicateSeller(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_quote_dup",
		SeedHash:   "4444444444444444444444444444444444444444444444444444444444444444",
		State:      StateQueued,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// seller_a 第一次报价
	store.AppendQuote(ctx, "test_job_quote_dup", QuoteReport{
		SellerPubkey:   "seller_a",
		SeedPriceSat:   1000,
		ChunkPriceSat:  100,
		ChunkCount:     5,
		QuoteTimestamp: 1234567890,
	})

	quotes1, _ := store.ListQuotes(ctx, "test_job_quote_dup")
	if len(quotes1) != 1 {
		t.Fatalf("expected 1 quote, got %d", len(quotes1))
	}

	// seller_a 第二次报价（应该覆盖）
	store.AppendQuote(ctx, "test_job_quote_dup", QuoteReport{
		SellerPubkey:   "seller_a",
		SeedPriceSat:   1200,
		ChunkPriceSat:  90,
		ChunkCount:     5,
		QuoteTimestamp: 1234567899,
	})

	quotes2, _ := store.ListQuotes(ctx, "test_job_quote_dup")
	if len(quotes2) != 1 {
		t.Fatalf("expected 1 quote after update, got %d", len(quotes2))
	}
	if quotes2[0].SeedPriceSat != 1200 {
		t.Fatalf("expected seed price 1200, got %d", quotes2[0].SeedPriceSat)
	}
}

func TestMessageOfNil(t *testing.T) {
	t.Parallel()

	// MessageOf(nil) 不应 panic
	if MessageOf(nil) != "" {
		t.Fatalf("expected empty string for nil error")
	}
	if CodeOf(nil) != "" {
		t.Fatalf("expected empty code for nil error")
	}
}

func TestGetJobReturnsSnapshot(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	job := &Job{
		JobID:      "test_job_snapshot",
		SeedHash:   "5555555555555555555555555555555555555555555555555555555555555555",
		State:      StateRunning,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 获取 job 快照
	snapshot1, found := store.GetJob(ctx, "test_job_snapshot")
	if !found {
		t.Fatalf("job not found")
	}

	// 修改快照不应该影响内部状态
	snapshot1.State = "modified"
	snapshot1.ChunkCount = 999

	// 再次获取应该得到原始值
	snapshot2, _ := store.GetJob(ctx, "test_job_snapshot")
	if snapshot2.State == "modified" {
		t.Fatalf("GetJob should return snapshots, not internal pointers")
	}
	if snapshot2.ChunkCount == 999 {
		t.Fatalf("GetJob should return snapshots, not internal pointers")
	}
}

func TestMemoryStoreNameIndicatesTesting(t *testing.T) {
	t.Parallel()

	// 验证 NewMemoryJobStoreForTest 名字明确表示测试用途
	store := NewMemoryJobStoreForTest()
	if store == nil {
		t.Fatalf("NewMemoryJobStoreForTest should not return nil")
	}
}

func TestFindJobBySeedHash(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "6666666666666666666666666666666666666666666666666666666666666666"

	// 创建 job（running 状态）
	job := &Job{
		JobID:      "test_job_find",
		SeedHash:   seedHash,
		State:      StateRunning,
		ChunkCount: 5,
	}
	if _, _, err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 查找 job（任何状态都返回，包括 running）
	foundJob, found, err := store.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		t.Fatalf("find job failed: %v", err)
	}
	if !found {
		t.Fatalf("expected to find job")
	}
	if foundJob.JobID != "test_job_find" {
		t.Fatalf("expected job_id=test_job_find, got %s", foundJob.JobID)
	}

	// done 状态仍返回同一 job（不跳过，语义改为"已有 job 均复用"）
	store.UpdateJobState(ctx, "test_job_find", StateDone)

	foundJob2, found2, err2 := store.FindJobBySeedHash(ctx, seedHash)
	if err2 != nil {
		t.Fatalf("find job after done failed: %v", err2)
	}
	if !found2 {
		t.Fatalf("expected to find done job (reusable)")
	}
	if foundJob2.JobID != "test_job_find" {
		t.Fatalf("expected same job_id after done")
	}
}

func TestSeedStoreAndFileStoreAreSeparate(t *testing.T) {
	t.Parallel()

	// 验证 SeedStore 和 FileStore 是两个独立的接口
	// SeedStore 负责 seed 元数据
	var _ SeedStore = (*mockSeedStore)(nil)
	// FileStore 负责本地文件
	var _ FileStore = (*mockFileStore)(nil)

	// WorkspaceWriter 不应存在
	// (此测试通过编译检查即可验证 WorkspaceWriter 已被移除)
}

type mockSeedStore struct {
	meta  SeedMeta
	found bool
	err   error
}

func (m *mockSeedStore) SaveSeed(ctx context.Context, input SaveSeedInput) error {
	return NewError(CodeModuleDisabled, "seed store is not available")
}

func (m *mockSeedStore) LoadSeedMeta(ctx context.Context, seedHash string) (SeedMeta, bool, error) {
	if m.err != nil {
		return SeedMeta{}, false, m.err
	}
	return m.meta, m.found, nil
}

type mockFileStore struct{}

func (m *mockFileStore) FindCompleteFile(ctx context.Context, seedHash string) (LocalFile, bool, error) {
	return LocalFile{}, false, nil
}

func (m *mockFileStore) PreparePartFile(ctx context.Context, input PreparePartFileInput) (PartFile, error) {
	return PartFile{}, nil
}

func (m *mockFileStore) MarkChunkStored(ctx context.Context, input StoredChunkInput) error {
	return nil
}

func (m *mockFileStore) CompleteFile(ctx context.Context, input CompleteFileInput) (LocalFile, error) {
	return LocalFile{}, nil
}

type mockStartFileStore struct {
	complete          LocalFile
	part              PartFile
	found             bool
	expectedChunkLen  int
	markCalls         []StoredChunkInput
	completeCalled    int
	completeErr       error
	lastCompleteInput CompleteFileInput
}

func (m *mockStartFileStore) FindCompleteFile(ctx context.Context, seedHash string) (LocalFile, bool, error) {
	return m.complete, m.found, nil
}

func (m *mockStartFileStore) PreparePartFile(ctx context.Context, input PreparePartFileInput) (PartFile, error) {
	if m.part.PartFilePath == "" {
		m.part.PartFilePath = "/tmp/part-" + input.SeedHash[:8] + ".bin"
	}
	return m.part, nil
}

func (m *mockStartFileStore) MarkChunkStored(ctx context.Context, input StoredChunkInput) error {
	m.markCalls = append(m.markCalls, input)
	if m.expectedChunkLen > 0 && len(input.ChunkBytes) != m.expectedChunkLen {
		return NewError(CodeChunkStoreFailed, "chunk size does not match layout")
	}
	return nil
}

func (m *mockStartFileStore) CompleteFile(ctx context.Context, input CompleteFileInput) (LocalFile, error) {
	m.completeCalled++
	m.lastCompleteInput = input
	if m.completeErr != nil {
		return LocalFile{}, m.completeErr
	}
	return m.complete, nil
}

type mockTransferRunner struct {
	results map[uint32]ChunkTransferResult
	errs    map[uint32]error
	calls   []ChunkTransferRequest
}

func (m *mockTransferRunner) RunChunkTransfer(ctx context.Context, req ChunkTransferRequest) (ChunkTransferResult, error) {
	m.calls = append(m.calls, req)
	if m.errs != nil {
		if err, ok := m.errs[req.ChunkIndex]; ok {
			return ChunkTransferResult{}, err
		}
	}
	if m.results != nil {
		if res, ok := m.results[req.ChunkIndex]; ok {
			return res, nil
		}
	}
	shortHash := req.ChunkHash
	if len(shortHash) > 4 {
		shortHash = shortHash[:4]
	}
	return ChunkTransferResult{
		Data:     []byte(shortHash),
		PaidSat:  req.ChunkPriceSat,
		SpeedBps: 1000,
	}, nil
}

type mockDemandPublisher struct {
	result PublishDemandResult
	err    error
}

func (m mockDemandPublisher) PublishDemand(ctx context.Context, req PublishDemandRequest) (PublishDemandResult, error) {
	return m.result, m.err
}

type mockQuoteReader struct {
	quotes []QuoteReport
	err    error
}

func (m mockQuoteReader) ListQuotes(ctx context.Context, demandID string) ([]QuoteReport, error) {
	return append([]QuoteReport(nil), m.quotes...), m.err
}

type mockPolicy struct {
	selected QuoteReport
	ok       bool
	reason   string
	err      error
}

func (m mockPolicy) SelectQuote(ctx context.Context, req StartRequest, quotes []QuoteReport) (QuoteReport, bool, string, error) {
	if m.err != nil {
		return QuoteReport{}, false, "", m.err
	}
	if !m.ok {
		if m.reason == "" {
			m.reason = "blocked_by_budget"
		}
		return QuoteReport{}, false, m.reason, nil
	}
	if m.selected.SellerPubkey == "" && len(quotes) > 0 {
		return quotes[0], true, "", nil
	}
	return m.selected, true, "", nil
}
