package filedownload

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// StartByHash 启动静态文件下载总调度。
// 这一层只串流程，不做具体实现：
// - 能力缺失直接失败，不伪成功；
// - 本地完整文件命中时，直接落到 local；
// - 其余路径只把 demand / quote / policy / part file 的位置定住。
func StartByHash(ctx context.Context, caps DownloadCaps, req StartRequest) (StartResult, error) {
	var result StartResult
	if ctx == nil {
		return result, NewError(CodeBadRequest, "ctx is required")
	}
	if err := ctx.Err(); err != nil {
		return result, NewError(CodeRequestCanceled, err.Error())
	}
	if caps.Jobs == nil {
		return result, NewError(CodeModuleDisabled, "job store is not available")
	}

	seedHash := normalizeSeedHash(req.SeedHash)
	if !isValidSeedHash(seedHash) {
		return result, NewError(CodeBadRequest, "invalid seed_hash format")
	}
	chunkCount := req.ChunkCount

	job, found, err := caps.Jobs.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		return result, err
	}
	if found {
		if shouldReuseExistingJob(job) {
			return resultFromJob(job), nil
		}
	} else {
		job = Job{
			JobID:      buildJobID(seedHash),
			SeedHash:   seedHash,
			State:      StateQueued,
			ChunkCount: chunkCount,
		}
		created, _, err := caps.Jobs.CreateJob(ctx, &job)
		if err != nil {
			return result, err
		}
		job = created
	}

	if caps.Files == nil {
		return result, NewError(CodeModuleDisabled, "file store is not available")
	}

	localFile, foundLocal, err := caps.Files.FindCompleteFile(ctx, seedHash)
	if err != nil {
		return result, err
	}
	if foundLocal {
		if localFile.FilePath != "" {
			if err := caps.Jobs.SetOutputPath(ctx, job.JobID, localFile.FilePath); err != nil {
				return result, err
			}
		}
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateLocal); err != nil {
			return result, err
		}
		if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
			return resultFromJob(refreshed), nil
		}
		job.State = StateLocal
		job.OutputFilePath = localFile.FilePath
		return resultFromJob(job), nil
	}

	if chunkCount == 0 {
		if caps.Seeds == nil {
			return result, NewError(CodeModuleDisabled, "seed store is not available")
		}
		meta, foundMeta, err := caps.Seeds.LoadSeedMeta(ctx, seedHash)
		if err != nil {
			return result, err
		}
		if !foundMeta || meta.ChunkCount == 0 {
			return result, NewError(CodeModuleDisabled, "seed meta is not available")
		}
		chunkCount = meta.ChunkCount
		if err := caps.Jobs.SetChunkCount(ctx, job.JobID, chunkCount); err != nil {
			return result, err
		}
	}

	if caps.Demands == nil {
		return result, NewError(CodeModuleDisabled, "demand publisher is not available")
	}
	if caps.Quotes == nil {
		return result, NewError(CodeModuleDisabled, "quote reader is not available")
	}
	if caps.Policy == nil {
		return result, NewError(CodeModuleDisabled, "download policy is not available")
	}

	demandRes, err := caps.Demands.PublishDemand(ctx, PublishDemandRequest{
		SeedHash:   seedHash,
		ChunkCount: chunkCount,
		GatewayID:  strings.TrimSpace(req.GatewayPubkey),
	})
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(demandRes.DemandID) == "" {
		return result, NewError(CodeModuleDisabled, "demand publisher returned empty demand_id")
	}
	if err := caps.Jobs.SetDemandID(ctx, job.JobID, demandRes.DemandID); err != nil {
		return result, err
	}

	quotes, err := caps.Quotes.ListQuotes(ctx, demandRes.DemandID)
	if err != nil {
		return result, err
	}
	if len(quotes) == 0 {
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateQuoteUnavailable); err != nil {
			return result, err
		}
		if err := caps.Jobs.SetError(ctx, job.JobID, "no quotes available"); err != nil {
			return result, err
		}
		if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
			out := resultFromJob(refreshed)
			out.Message = "no quotes available"
			return out, nil
		}
		job.State = StateQuoteUnavailable
		job.Error = "no quotes available"
		result = resultFromJob(job)
		result.Message = "no quotes available"
		return result, nil
	}

	selectedQuote, ok, reason, err := caps.Policy.SelectQuote(ctx, req, quotes)
	if err != nil {
		return result, err
	}
	if reason == "" {
		reason = "quote_unavailable"
	}

	for _, quote := range quotes {
		quote.Selected = false
		quote.RejectReason = reason
		if ok && quote.SellerPubkey == selectedQuote.SellerPubkey {
			quote.Selected = true
			quote.RejectReason = ""
		}
		if err := caps.Jobs.AppendQuote(ctx, job.JobID, quote); err != nil {
			return result, err
		}
	}
	if !ok {
		nextState := StateQuoteUnavailable
		if strings.Contains(strings.ToLower(reason), "budget") {
			nextState = StateBlockedByBudget
		}
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, nextState); err != nil {
			return result, err
		}
		if err := caps.Jobs.SetError(ctx, job.JobID, reason); err != nil {
			return result, err
		}
		if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
			out := resultFromJob(refreshed)
			out.Message = reason
			return out, nil
		}
		job.State = nextState
		job.Error = reason
		result = resultFromJob(job)
		result.Message = reason
		return result, nil
	}

	if caps.Files == nil {
		return result, NewError(CodeModuleDisabled, "file store is not available")
	}
	partFile, err := caps.Files.PreparePartFile(ctx, PreparePartFileInput{
		SeedHash: seedHash,
		FileSize: selectedQuote.FileSizeBytes,
	})
	if err != nil {
		return result, err
	}
	if err := caps.Jobs.SetPartFilePath(ctx, job.JobID, partFile.PartFilePath); err != nil {
		return result, err
	}
	if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateRunning); err != nil {
		return result, err
	}
	if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
		return result, err
	}

	if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
		return resultFromJob(refreshed), nil
	}
	job.State = StateRunning
	job.PartFilePath = partFile.PartFilePath
	job.Error = ""
	return resultFromJob(job), nil
}

// GetStatus 查询 job 状态（只读查询，不触发业务）
func GetStatus(ctx context.Context, store JobStore, jobID string) (Status, error) {
	var status Status

	if ctx == nil {
		return status, NewError(CodeBadRequest, "ctx is required")
	}
	if ctx.Err() != nil {
		return status, NewError(CodeRequestCanceled, ctx.Err().Error())
	}
	if store == nil {
		return status, NewError(CodeModuleDisabled, "job store is not available")
	}

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return status, NewError(CodeBadRequest, "job_id is required")
	}

	job, found := store.GetJob(ctx, jobID)
	if !found {
		return status, NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	return statusFromJob(job), nil
}

// ListChunks 查询 job 的 chunk 上报（只读查询）
func ListChunks(ctx context.Context, store JobStore, jobID string) ([]ChunkReport, error) {
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
	return AggregateNodeReports(chunks), nil
}

// ListQuotes 查询 job 的 seller 报价（只读查询）
func ListQuotes(ctx context.Context, store JobStore, jobID string) ([]QuoteReport, error) {
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

func normalizeSeedHash(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func shouldReuseExistingJob(job Job) bool {
	switch job.State {
	case StateLocal, StateRunning, StateBlockedByBudget, StateQuoteUnavailable, StateQuoteTimeout, StateFailed, StateDone:
		return true
	default:
		return false
	}
}

func buildJobID(seedHash string) string {
	seedHash = normalizeSeedHash(seedHash)
	if len(seedHash) > 16 {
		seedHash = seedHash[:16]
	}
	return fmt.Sprintf("gfbh_%d_%s", time.Now().UnixNano(), seedHash)
}

func resultFromJob(job Job) StartResult {
	return StartResult{
		JobID:   job.JobID,
		Status:  statusFromJob(job),
		Message: strings.TrimSpace(job.Error),
	}
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
