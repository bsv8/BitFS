package filedownload

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func loadSelectedQuoteForJob(ctx context.Context, jobs JobStore, jobID string) (QuoteReport, error) {
	if jobs == nil {
		return QuoteReport{}, NewError(CodeModuleDisabled, "job store is not available")
	}
	quotes, found := jobs.ListQuotes(ctx, jobID)
	if !found {
		return QuoteReport{}, NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	for _, quote := range quotes {
		if quote.Selected {
			return quote, nil
		}
	}
	return QuoteReport{}, nil
}

func markDownloadJobFailed(ctx context.Context, jobs JobStore, jobID string, cause error) error {
	if jobs == nil {
		return cause
	}
	if cause == nil {
		cause = NewError(CodeDownloadFailed, "download failed")
	}
	var firstErr error
	if err := jobs.SetError(ctx, jobID, MessageOf(cause)); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := jobs.UpdateJobState(ctx, jobID, StateFailed); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		return firstErr
	}
	return cause
}

func markDownloadChunkFailed(ctx context.Context, jobs JobStore, jobID string, chunkIndex uint32, sellerPubkey string, paidSat uint64, cause error) error {
	if jobs == nil {
		return cause
	}
	if cause == nil {
		cause = NewError(CodeDownloadFailed, "chunk transfer failed")
	}
	report := ChunkReport{
		ChunkIndex:    chunkIndex,
		State:         ChunkStateFailed,
		SellerPubkey:  sellerPubkey,
		ChunkPriceSat: paidSat,
		Selected:      false,
		Error:         MessageOf(cause),
		RejectReason:  MessageOf(cause),
	}
	var firstErr error
	if err := jobs.SetError(ctx, jobID, MessageOf(cause)); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := jobs.UpdateJobState(ctx, jobID, StateFailed); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := jobs.AppendChunkReport(ctx, jobID, report); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		return firstErr
	}
	return cause
}

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

func shouldReturnExistingJob(job Job) bool {
	switch job.State {
	case StateLocal, StateBlockedByBudget, StateQuoteUnavailable, StateQuoteTimeout, StateFailed, StateDone:
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

func statusFromJob(job Job) Status {
	return Status{
		JobID:           job.JobID,
		SeedHash:        job.SeedHash,
		FrontOrderID:    job.FrontOrderID,
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
