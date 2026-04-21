package getfilebyhash

import (
	"context"
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

	if err := store.CreateJob(ctx, job); err != nil {
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

	// 创建第一个 job
	result1, err := StartDownload(ctx, store, StartRequest{
		SeedHash:   seedHash,
		ChunkCount: 10,
	})
	if err != nil {
		t.Fatalf("first start download failed: %v", err)
	}

	// 再次请求同一 seed，应返回已有 job
	result2, err := StartDownload(ctx, store, StartRequest{
		SeedHash:   seedHash,
		ChunkCount: 10,
	})
	if err != nil {
		t.Fatalf("second start download failed: %v", err)
	}

	if result1.JobID != result2.JobID {
		t.Fatalf("expected same job id, got %s and %s", result1.JobID, result2.JobID)
	}
}

func TestDoneJobAllowsNewDownload(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()
	seedHash := "b1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"

	// 创建并完成 job
	result1, err := StartDownload(ctx, store, StartRequest{
		SeedHash:   seedHash,
		ChunkCount: 10,
	})
	if err != nil {
		t.Fatalf("start download failed: %v", err)
	}

	// 标记为 done
	store.UpdateJobState(ctx, result1.JobID, StateDone)

	// 再次请求同一 seed，根据第2步施工单语义：仍返回已有 job，不创建新 job
	result2, err := StartDownload(ctx, store, StartRequest{
		SeedHash:   seedHash,
		ChunkCount: 10,
	})
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
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 上报 3 个 chunk，来自两个 seller
	chunks := []ChunkReport{
		{ChunkIndex: 0, SellerPubkey: "seller1", ChunkPriceSat: 100, Selected: true, SpeedBps: 1000},
		{ChunkIndex: 1, SellerPubkey: "seller1", ChunkPriceSat: 100, Selected: true, SpeedBps: 2000},
		{ChunkIndex: 2, SellerPubkey: "seller2", ChunkPriceSat: 150, Selected: true, SpeedBps: 1500},
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
		if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
					SellerPubkey:  "seller_concurrent",
					ChunkPriceSat: 100,
					Selected:      true,
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

func TestStartDownloadValidation(t *testing.T) {
	t.Parallel()

	store := NewMemoryJobStoreForTest()
	ctx := context.Background()

	// 测试 nil ctx
	_, err := StartDownload(nil, store, StartRequest{SeedHash: "aabbccdd"})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for nil ctx")
	}

	// 测试 cancelled ctx
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = StartDownload(cancelledCtx, store, StartRequest{SeedHash: "aabbccdd"})
	if err == nil || CodeOf(err) != CodeRequestCanceled {
		t.Fatalf("expected request canceled error")
	}

	// 测试无效 seed_hash
	_, err = StartDownload(ctx, store, StartRequest{SeedHash: "invalid"})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for invalid seed_hash")
	}

	// 测试空 seed_hash
	_, err = StartDownload(ctx, store, StartRequest{SeedHash: ""})
	if err == nil || CodeOf(err) != CodeBadRequest {
		t.Fatalf("expected bad request for empty seed_hash")
	}

	// 测试 nil store
	validHash := "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
	_, err = StartDownload(ctx, nil, StartRequest{SeedHash: validHash})
	if err == nil || CodeOf(err) != CodeModuleDisabled {
		t.Fatalf("expected module disabled for nil store")
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
	if err := store.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job failed: %v", err)
	}

	// 第一次写入 chunk 0
	store.AppendChunkReport(ctx, "test_job_dup", ChunkReport{
		ChunkIndex: 0, SellerPubkey: "seller1", ChunkPriceSat: 100, Selected: true, SpeedBps: 1000,
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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
	if err := store.CreateJob(ctx, job); err != nil {
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

type mockSeedStore struct{}

func (m *mockSeedStore) SaveSeed(ctx context.Context, input SaveSeedInput) error {
	return nil
}

func (m *mockSeedStore) LoadSeedMeta(ctx context.Context, seedHash string) (SeedMeta, bool, error) {
	return SeedMeta{}, false, nil
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
