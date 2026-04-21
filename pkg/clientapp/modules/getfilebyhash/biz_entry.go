package getfilebyhash

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// StartDownload 启动下载入口
// 第一步只负责创建 job，不发布 demand（第2步接 runner 时实现真实下载）
// 如果存在活跃 job（queued/running/blocked_by_budget/quote_unavailable/quote_timeout/failed），返回已有 job
func StartDownload(ctx context.Context, store JobStore, req StartRequest) (StartResult, error) {
	var result StartResult

	// 校验 ctx
	if ctx == nil {
		return result, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return result, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return result, NewError(CodeModuleDisabled, "job store is not available")
	}

	// 校验 seed_hash 格式
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if !isValidSeedHash(seedHash) {
		return result, NewError(CodeBadRequest, "invalid seed_hash format")
	}

	// 查询是否存在已有 job（同一 seed_hash 已有 job 时复用，不创建新 job）
	existingJob, found, err := store.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		return result, err
	}
	if found {
		// 存在已有 job（包括 done/local/failed），返回已有 job 不再创建
		result.JobID = existingJob.JobID
		result.Status = statusFromJob(existingJob)
		return result, nil
	}

	// 生成 job_id
	jobID := fmt.Sprintf("gfbh_%d_%s", time.Now().UnixNano(), seedHash[:16])

	// 创建 job（状态为 queued，等待第2步 runner 来更新状态）
	job := &Job{
		JobID:      jobID,
		SeedHash:   seedHash,
		State:      StateQueued,
		ChunkCount: req.ChunkCount,
	}
	if err := store.CreateJob(ctx, job); err != nil {
		return result, err
	}

	result.JobID = jobID
	result.Status = Status{
		JobID:    jobID,
		SeedHash: seedHash,
		State:    StateQueued,
	}

	return result, nil
}

// GetStatus 查询 job 状态（只读查询，不触发业务）
func GetStatus(ctx context.Context, store JobStore, jobID string) (Status, error) {
	var status Status

	// 校验 ctx
	if ctx == nil {
		return status, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return status, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return status, NewError(CodeModuleDisabled, "job store is not available")
	}

	// 校验 job_id
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return status, NewError(CodeBadRequest, "job_id is required")
	}

	job, found := store.GetJob(ctx, jobID)
	if !found {
		return status, NewError(CodeJobNotFound, "job not found: "+jobID)
	}

	status = Status{
		JobID:           job.JobID,
		SeedHash:        job.SeedHash,
		DemandID:        job.DemandID,
		State:           job.State,
		ChunkCount:      job.ChunkCount,
		CompletedChunks: job.CompletedChunks,
		PaidTotalSat:    job.PaidTotalSat,
		OutputFilePath:  job.OutputFilePath,
		PartFilePath:    job.PartFilePath,
		Error:           job.Error,
		CreatedAtUnix:   job.CreatedAt.Unix(),
		UpdatedAtUnix:   job.UpdatedAt.Unix(),
	}
	return status, nil
}

// ListChunks 查询 job 的 chunk 上报（只读查询）
func ListChunks(ctx context.Context, store JobStore, jobID string) ([]ChunkReport, error) {
	// 校验 ctx
	if ctx == nil {
		return nil, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return nil, NewError(CodeModuleDisabled, "job store is not available")
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, NewError(CodeBadRequest, "job_id is required")
	}

	chunks, found := store.ListChunks(ctx, jobID)
	if !found {
		return nil, NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	return chunks, nil
}

// ListNodes 查询 job 的 node 上报（只读查询，自动从 chunk 聚合）
func ListNodes(ctx context.Context, store JobStore, jobID string) ([]NodeReport, error) {
	// 校验 ctx
	if ctx == nil {
		return nil, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return nil, NewError(CodeModuleDisabled, "job store is not available")
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, NewError(CodeBadRequest, "job_id is required")
	}

	chunks, found := store.ListChunks(ctx, jobID)
	if !found {
		return nil, NewError(CodeJobNotFound, "job not found: "+jobID)
	}

	// 从 chunk facts 自动聚合
	return AggregateNodeReports(chunks), nil
}

// ListQuotes 查询 job 的 seller 报价（只读查询）
func ListQuotes(ctx context.Context, store JobStore, jobID string) ([]QuoteReport, error) {
	// 校验 ctx
	if ctx == nil {
		return nil, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return nil, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return nil, NewError(CodeModuleDisabled, "job store is not available")
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, NewError(CodeBadRequest, "job_id is required")
	}

	quotes, found := store.ListQuotes(ctx, jobID)
	if !found {
		return nil, NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	return quotes, nil
}

// statusFromJob 将 job 值转换为 Status
func statusFromJob(job Job) Status {
	return Status{
		JobID:           job.JobID,
		SeedHash:        job.SeedHash,
		DemandID:        job.DemandID,
		State:           job.State,
		ChunkCount:      job.ChunkCount,
		CompletedChunks: job.CompletedChunks,
		PaidTotalSat:    job.PaidTotalSat,
		OutputFilePath:  job.OutputFilePath,
		PartFilePath:    job.PartFilePath,
		Error:           job.Error,
		CreatedAtUnix:   job.CreatedAt.Unix(),
		UpdatedAtUnix:   job.UpdatedAt.Unix(),
	}
}

// isValidSeedHash 校验 seed_hash 格式（64字符十六进制）
func isValidSeedHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	hash = strings.ToLower(hash)
	for _, c := range hash {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// AggregateNodeReports 从 chunk reports 自动聚合 node reports
func AggregateNodeReports(chunks []ChunkReport) []NodeReport {
	nodeMap := make(map[string][]ChunkReport)
	for _, chunk := range chunks {
		nodeMap[chunk.SellerPubkey] = append(nodeMap[chunk.SellerPubkey], chunk)
	}

	nodes := make([]NodeReport, 0, len(nodeMap))
	for sellerPubkey, sellerChunks := range nodeMap {
		node := NodeReport{
			SellerPubkey: sellerPubkey,
			Chunks:       sellerChunks,
		}

		var totalSpeed uint64
		var speedCount uint64
		for _, chunk := range sellerChunks {
			if chunk.Selected {
				node.SelectedCount++
				node.TotalPaidSat += chunk.ChunkPriceSat
			} else {
				node.RejectedCount++
			}
			if chunk.SpeedBps > 0 {
				totalSpeed += chunk.SpeedBps
				speedCount++
			}
		}
		if speedCount > 0 {
			node.AvgSpeedBps = totalSpeed / speedCount
		}
		node.ReportedChunkCount = uint32(len(sellerChunks))

		nodes = append(nodes, node)
	}
	return nodes
}
