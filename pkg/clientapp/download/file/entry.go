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
		if shouldReturnExistingJob(job) {
			return resultFromJob(job), nil
		}
		if job.State == StateRunning {
			return resumeRunningJob(ctx, caps, job, req)
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
			failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
			if failErr != nil {
				return result, failErr
			}
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

	if caps.Seeds == nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "seed store is not available"))
		if failErr != nil {
			return result, failErr
		}
		return result, NewError(CodeModuleDisabled, "seed store is not available")
	}
	seedMeta, foundMeta, err := caps.Seeds.LoadSeedMeta(ctx, seedHash)
	if err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if !foundMeta {
		cause := NewError(CodeModuleDisabled, "seed meta is not available")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	if seedMeta.ChunkCount == 0 {
		cause := NewError(CodeBadRequest, "seed chunk count is required")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	if req.ChunkCount > 0 && req.ChunkCount != seedMeta.ChunkCount {
		cause := NewError(CodeBadRequest, "chunk count does not match seed metadata")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	if len(seedMeta.ChunkHashes) != int(seedMeta.ChunkCount) {
		cause := NewError(CodeBadRequest, "chunk hashes do not match chunk count")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	if selectedQuote.FileSizeBytes > 0 && selectedQuote.FileSizeBytes != seedMeta.FileSize {
		cause := NewError(CodeBadRequest, "quote file size does not match seed metadata")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	for _, chunkHash := range seedMeta.ChunkHashes {
		if strings.TrimSpace(chunkHash) == "" {
			cause := NewError(CodeBadRequest, "chunk hash is required")
			failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
			if failErr != nil {
				return result, failErr
			}
			return result, cause
		}
	}

	if caps.Transfers == nil {
		cause := NewError(CodeModuleDisabled, "transfer runner is not available")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	if caps.Files == nil {
		cause := NewError(CodeModuleDisabled, "file store is not available")
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, cause)
		if failErr != nil {
			return result, failErr
		}
		return result, cause
	}
	partFile, err := caps.Files.PreparePartFile(ctx, PreparePartFileInput{
		SeedHash: seedHash,
		FileSize: seedMeta.FileSize,
	})
	if err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.SetPartFilePath(ctx, job.JobID, partFile.PartFilePath); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateRunning); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}

	var completed uint32
	var paidTotal uint64
	completedChunkIndexes := make([]uint32, 0, len(seedMeta.ChunkHashes))
	for chunkIndex, chunkHash := range seedMeta.ChunkHashes {
		transferRes, transferErr := caps.Transfers.RunChunkTransfer(ctx, ChunkTransferRequest{
			DemandID:      demandRes.DemandID,
			SeedHash:      seedHash,
			ChunkHash:     chunkHash,
			SellerPubkey:  selectedQuote.SellerPubkey,
			ChunkIndex:    uint32(chunkIndex),
			ChunkPriceSat: selectedQuote.ChunkPriceSat,
			MaxRetries:    0,
		})
		if transferErr != nil {
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, uint32(chunkIndex), selectedQuote.SellerPubkey, 0, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		if len(transferRes.Data) == 0 {
			transferErr = NewError(CodeTransferFailed, "chunk bytes are empty")
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, uint32(chunkIndex), selectedQuote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		if err := caps.Files.MarkChunkStored(ctx, StoredChunkInput{
			SeedHash:   seedHash,
			ChunkIndex: uint32(chunkIndex),
			ChunkBytes: transferRes.Data,
		}); err != nil {
			transferErr = NewError(CodeChunkStoreFailed, err.Error())
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, uint32(chunkIndex), selectedQuote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		report := ChunkReport{
			ChunkIndex:    uint32(chunkIndex),
			State:         ChunkStateStored,
			SellerPubkey:  selectedQuote.SellerPubkey,
			ChunkPriceSat: transferRes.PaidSat,
			SpeedBps:      transferRes.SpeedBps,
			Selected:      true,
		}
		if err := caps.Jobs.AppendChunkReport(ctx, job.JobID, report); err != nil {
			transferErr = NewError(CodeChunkStoreFailed, err.Error())
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, uint32(chunkIndex), selectedQuote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		completed++
		paidTotal += transferRes.PaidSat
		completedChunkIndexes = append(completedChunkIndexes, uint32(chunkIndex))
	}

	localFile, err = caps.Files.CompleteFile(ctx, CompleteFileInput{
		SeedHash:              seedHash,
		PartFilePath:          partFile.PartFilePath,
		RecommendedFileName:   strings.TrimSpace(selectedQuote.RecommendedFileName),
		MimeType:              strings.TrimSpace(selectedQuote.MimeType),
		AvailableChunkIndexes: append([]uint32(nil), completedChunkIndexes...),
	})
	if err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.SetOutputPath(ctx, job.JobID, localFile.FilePath); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateDone); err != nil {
		failErr := markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		if failErr != nil {
			return result, failErr
		}
		return result, err
	}
	if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
		refreshed.State = StateDone
		refreshed.CompletedChunks = completed
		refreshed.PaidTotalSat = paidTotal
		refreshed.OutputFilePath = localFile.FilePath
		refreshed.PartFilePath = partFile.PartFilePath
		return resultFromJob(refreshed), nil
	}
	job.State = StateDone
	job.PartFilePath = partFile.PartFilePath
	job.CompletedChunks = completed
	job.PaidTotalSat = paidTotal
	job.OutputFilePath = localFile.FilePath
	job.Error = ""
	return resultFromJob(job), nil
}

func resumeRunningJob(ctx context.Context, caps DownloadCaps, job Job, req StartRequest) (StartResult, error) {
	var result StartResult
	if caps.Jobs == nil {
		return result, NewError(CodeModuleDisabled, "job store is not available")
	}
	if caps.Seeds == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "seed store is not available"))
	}
	if caps.Files == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "file store is not available"))
	}

	seedHash := normalizeSeedHash(req.SeedHash)
	if seedHash == "" {
		seedHash = normalizeSeedHash(job.SeedHash)
	}
	if seedHash == "" {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "seed_hash is required"))
	}

	storedChunks, found := caps.Jobs.ListChunks(ctx, job.JobID)
	if !found {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeJobNotFound, "job not found: "+job.JobID))
	}
	storedChunkMap := make(map[uint32]ChunkReport, len(storedChunks))
	for _, chunk := range storedChunks {
		if chunk.State == ChunkStateStored {
			storedChunkMap[chunk.ChunkIndex] = chunk
		}
	}

	seedMeta, foundMeta, err := caps.Seeds.LoadSeedMeta(ctx, seedHash)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if !foundMeta {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "seed meta is not available"))
	}
	if seedMeta.ChunkCount == 0 {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "seed chunk count is required"))
	}
	if req.ChunkCount > 0 && req.ChunkCount != seedMeta.ChunkCount {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "chunk count does not match seed metadata"))
	}
	if len(seedMeta.ChunkHashes) != int(seedMeta.ChunkCount) {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "chunk hashes do not match chunk count"))
	}
	for _, chunkHash := range seedMeta.ChunkHashes {
		if strings.TrimSpace(chunkHash) == "" {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "chunk hash is required"))
		}
	}

	if job.ChunkCount == 0 {
		if err := caps.Jobs.SetChunkCount(ctx, job.JobID, seedMeta.ChunkCount); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
	} else if job.ChunkCount != seedMeta.ChunkCount {
		if err := caps.Jobs.SetChunkCount(ctx, job.JobID, seedMeta.ChunkCount); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
	}

	quote, err := loadSelectedQuoteForJob(ctx, caps.Jobs, job.JobID)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if quote.SellerPubkey == "" {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeQuoteUnavailable, "selected quote is not available"))
	}
	if strings.TrimSpace(job.DemandID) == "" {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "demand id is not available"))
	}
	if quote.FileSizeBytes > 0 && quote.FileSizeBytes != seedMeta.FileSize {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeBadRequest, "quote file size does not match seed metadata"))
	}

	partFilePath := strings.TrimSpace(job.PartFilePath)
	if partFilePath == "" {
		partFile, prepErr := caps.Files.PreparePartFile(ctx, PreparePartFileInput{
			SeedHash: seedHash,
			FileSize: seedMeta.FileSize,
		})
		if prepErr != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, prepErr)
		}
		partFilePath = partFile.PartFilePath
		if err := caps.Jobs.SetPartFilePath(ctx, job.JobID, partFilePath); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
	}

	var completed uint32
	var paidTotal uint64
	for _, chunkReport := range storedChunks {
		if chunkReport.State == ChunkStateStored {
			completed++
			paidTotal += chunkReport.ChunkPriceSat
		}
	}

	completedIndexes := make([]uint32, 0, len(storedChunkMap))
	for chunkIndex := uint32(0); chunkIndex < seedMeta.ChunkCount; chunkIndex++ {
		if _, ok := storedChunkMap[chunkIndex]; ok {
			completedIndexes = append(completedIndexes, chunkIndex)
		}
	}

	if uint32(len(completedIndexes)) == seedMeta.ChunkCount {
		localFile, completeErr := caps.Files.CompleteFile(ctx, CompleteFileInput{
			SeedHash:              seedHash,
			PartFilePath:          partFilePath,
			RecommendedFileName:   strings.TrimSpace(quote.RecommendedFileName),
			MimeType:              strings.TrimSpace(quote.MimeType),
			AvailableChunkIndexes: append([]uint32(nil), completedIndexes...),
		})
		if completeErr != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, completeErr)
		}
		if err := caps.Jobs.SetOutputPath(ctx, job.JobID, localFile.FilePath); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateDone); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
			refreshed.State = StateDone
			refreshed.CompletedChunks = uint32(len(storedChunkMap))
			refreshed.PaidTotalSat = paidTotal
			refreshed.OutputFilePath = localFile.FilePath
			refreshed.PartFilePath = partFilePath
			return resultFromJob(refreshed), nil
		}
		job.State = StateDone
		job.CompletedChunks = uint32(len(storedChunkMap))
		job.PaidTotalSat = paidTotal
		job.OutputFilePath = localFile.FilePath
		job.PartFilePath = partFilePath
		return resultFromJob(job), nil
	}

	if caps.Transfers == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "transfer runner is not available"))
	}

	completedChunkIndexes := append([]uint32(nil), completedIndexes...)
	for chunkIndex, chunkHash := range seedMeta.ChunkHashes {
		idx := uint32(chunkIndex)
		if _, ok := storedChunkMap[idx]; ok {
			continue
		}
		transferRes, transferErr := caps.Transfers.RunChunkTransfer(ctx, ChunkTransferRequest{
			DemandID:      job.DemandID,
			SeedHash:      seedHash,
			ChunkHash:     chunkHash,
			SellerPubkey:  quote.SellerPubkey,
			ChunkIndex:    idx,
			ChunkPriceSat: quote.ChunkPriceSat,
			MaxRetries:    0,
		})
		if transferErr != nil {
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, idx, quote.SellerPubkey, 0, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		if len(transferRes.Data) == 0 {
			transferErr = NewError(CodeTransferFailed, "chunk bytes are empty")
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, idx, quote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		if err := caps.Files.MarkChunkStored(ctx, StoredChunkInput{
			SeedHash:   seedHash,
			ChunkIndex: idx,
			ChunkBytes: transferRes.Data,
		}); err != nil {
			transferErr = NewError(CodeChunkStoreFailed, err.Error())
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, idx, quote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		report := ChunkReport{
			ChunkIndex:    idx,
			State:         ChunkStateStored,
			SellerPubkey:  quote.SellerPubkey,
			ChunkPriceSat: transferRes.PaidSat,
			SpeedBps:      transferRes.SpeedBps,
			Selected:      true,
		}
		if err := caps.Jobs.AppendChunkReport(ctx, job.JobID, report); err != nil {
			transferErr = NewError(CodeChunkStoreFailed, err.Error())
			failErr := markDownloadChunkFailed(ctx, caps.Jobs, job.JobID, idx, quote.SellerPubkey, transferRes.PaidSat, transferErr)
			if failErr != nil {
				return result, failErr
			}
			return result, transferErr
		}
		completed++
		paidTotal += transferRes.PaidSat
		completedChunkIndexes = append(completedChunkIndexes, idx)
	}

	localFile, completeErr := caps.Files.CompleteFile(ctx, CompleteFileInput{
		SeedHash:              seedHash,
		PartFilePath:          partFilePath,
		RecommendedFileName:   strings.TrimSpace(quote.RecommendedFileName),
		MimeType:              strings.TrimSpace(quote.MimeType),
		AvailableChunkIndexes: append([]uint32(nil), completedChunkIndexes...),
	})
	if completeErr != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, completeErr)
	}
	if err := caps.Jobs.SetOutputPath(ctx, job.JobID, localFile.FilePath); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateDone); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if refreshed, ok := caps.Jobs.GetJob(ctx, job.JobID); ok {
		refreshed.State = StateDone
		refreshed.CompletedChunks = completed
		refreshed.PaidTotalSat = paidTotal
		refreshed.OutputFilePath = localFile.FilePath
		refreshed.PartFilePath = partFilePath
		return resultFromJob(refreshed), nil
	}
	job.State = StateDone
	job.PartFilePath = partFilePath
	job.CompletedChunks = completed
	job.PaidTotalSat = paidTotal
	job.OutputFilePath = localFile.FilePath
	job.Error = ""
	return resultFromJob(job), nil
}

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
