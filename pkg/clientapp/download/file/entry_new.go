package filedownload

import (
	"context"
	"strings"
	"time"
)

// StartByHash 是唯一下载主入口。
// 设计说明：
// - 先建 job，再绑 front_order_id；
// - demand、quote、seed、transfer、chunk、落盘全部挂到同一个 job；
// - 本地完整文件命中时直接返回，不走链上路径。
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

	job, found, err := caps.Jobs.FindJobBySeedHash(ctx, seedHash)
	if err != nil {
		return result, err
	}
	if found {
		if shouldReturnExistingJob(job) {
			return resultFromJob(job), nil
		}
		if job.State == StateRunning {
			return resumeRunningJobV2(ctx, caps, job, req)
		}
	} else {
		job = Job{
			JobID:      buildJobID(seedHash),
			SeedHash:   seedHash,
			State:      StateQueued,
			ChunkCount: req.ChunkCount,
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
	if localFile, foundLocal, err := caps.Files.FindCompleteFile(ctx, seedHash); err != nil {
		return result, err
	} else if foundLocal {
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

	frontOrderID, err := ensureDownloadFrontOrderV2(ctx, caps, job, req)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	job.FrontOrderID = frontOrderID

	demandChunkCount := req.ChunkCount
	if demandChunkCount == 0 {
		demandChunkCount = 1
		if caps.Seeds != nil {
			if meta, foundMeta, loadErr := caps.Seeds.LoadSeedMeta(ctx, seedHash); loadErr == nil && foundMeta && meta.ChunkCount > 0 {
				demandChunkCount = meta.ChunkCount
				if err := caps.Jobs.SetChunkCount(ctx, job.JobID, meta.ChunkCount); err != nil {
					return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
				}
			}
		}
	}

	if caps.Demands == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "demand publisher is not available"))
	}
	if caps.Quotes == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "quote reader is not available"))
	}
	quoteWaiter := caps.QuoteWaiter
	if quoteWaiter == nil {
		quoteWaiter = quoteWaiterFromReader{reader: caps.Quotes}
	}
	if quoteWaiter == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "quote waiter is not available"))
	}
	if caps.Policy == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "download policy is not available"))
	}

	demandRes, err := caps.Demands.PublishDemand(ctx, PublishDemandRequest{
		JobID:        job.JobID,
		FrontOrderID: frontOrderID,
		SeedHash:     seedHash,
		ChunkCount:   demandChunkCount,
		GatewayID:    strings.TrimSpace(req.GatewayPubkey),
	})
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if strings.TrimSpace(demandRes.DemandID) == "" {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "demand publisher returned empty demand_id"))
	}
	if err := caps.Jobs.SetDemandID(ctx, job.JobID, demandRes.DemandID); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	job.DemandID = demandRes.DemandID

	intervalSeconds := req.QuoteIntervalSeconds
	if intervalSeconds <= 0 {
		intervalSeconds = 2
	}
	quotes, err := quoteWaiter.WaitQuotes(ctx, QuoteWaitRequest{
		DemandID:         demandRes.DemandID,
		MaxRetry:         req.QuoteMaxRetry,
		Interval:         time.Duration(intervalSeconds) * time.Second,
		MaxSeedPriceSat:  req.MaxSeedPrice,
		MaxChunkPriceSat: req.MaxChunkPrice,
	})
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if len(quotes) == 0 {
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateQuoteTimeout); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		if err := caps.Jobs.SetError(ctx, job.JobID, "quote timeout"); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		job.State = StateQuoteTimeout
		job.Error = "quote timeout"
		return resultFromJob(job), nil
	}

	selection, err := caps.Policy.SelectQuotes(ctx, req, quotes)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := persistDownloadQuotesV2(ctx, caps.Jobs, job.JobID, quotes, selection); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if len(selection.Candidates) == 0 {
		nextState := StateQuoteUnavailable
		if strings.Contains(strings.ToLower(selection.Reason), "budget") {
			nextState = StateBlockedByBudget
		}
		if err := caps.Jobs.UpdateJobState(ctx, job.JobID, nextState); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		if err := caps.Jobs.SetError(ctx, job.JobID, selection.Reason); err != nil {
			return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
		}
		job.State = nextState
		job.Error = selection.Reason
		return resultFromJob(job), nil
	}

	seedMeta, err := resolveDownloadSeedMetaV2(ctx, caps, frontOrderID, seedHash, demandRes.DemandID, selection.Candidates, req, demandChunkCount)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
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
	if err := caps.Jobs.SetChunkCount(ctx, job.JobID, seedMeta.ChunkCount); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := caps.Jobs.UpdateJobState(ctx, job.JobID, StateRunning); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := caps.Jobs.SetError(ctx, job.JobID, ""); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}

	if caps.Transfers == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "transfer runner is not available"))
	}
	transferRes, err := caps.Transfers.RunTransferByStrategy(ctx, StrategyTransferRequest{
		FrontOrderID:    frontOrderID,
		DemandID:        demandRes.DemandID,
		SeedHash:        seedHash,
		ChunkCount:      seedMeta.ChunkCount,
		ArbiterPubHex:   strings.TrimSpace(req.ArbiterPubkeyHex),
		MaxSeedPrice:    req.MaxSeedPrice,
		MaxChunkPrice:   req.MaxChunkPrice,
		Strategy:        strings.TrimSpace(req.Strategy),
		PoolAmountSat:   req.PoolAmountSat,
		MaxChunkRetries: req.MaxChunkRetries,
		Candidates:      append([]QuoteReport(nil), selection.Candidates...),
	})
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	completedIndexes, completed, paidTotal, applyErr := applyTransferResultsV2(ctx, caps, job.JobID, seedHash, seedMeta, selection.Primary, transferRes, nil)
	if applyErr != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, applyErr)
	}

	localFile, err := caps.Files.CompleteFile(ctx, CompleteFileInput{
		SeedHash:              seedHash,
		PartFilePath:          partFilePath,
		RecommendedFileName:   strings.TrimSpace(selection.Primary.RecommendedFileName),
		MimeType:              strings.TrimSpace(selection.Primary.MimeType),
		AvailableChunkIndexes: completedIndexes,
	})
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
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
		refreshed.FrontOrderID = frontOrderID
		refreshed.DemandID = demandRes.DemandID
		return resultFromJob(refreshed), nil
	}
	job.State = StateDone
	job.PartFilePath = partFilePath
	job.CompletedChunks = completed
	job.PaidTotalSat = paidTotal
	job.OutputFilePath = localFile.FilePath
	job.Error = ""
	job.FrontOrderID = frontOrderID
	job.DemandID = demandRes.DemandID
	return resultFromJob(job), nil
}

func resumeRunningJobV2(ctx context.Context, caps DownloadCaps, job Job, req StartRequest) (StartResult, error) {
	var result StartResult
	if caps.Jobs == nil {
		return result, NewError(CodeModuleDisabled, "job store is not available")
	}
	if caps.Files == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "file store is not available"))
	}
	if strings.TrimSpace(job.FrontOrderID) == "" {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "front_order_id is required"))
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
	seedMeta, err := resolveSeedMetaForResumeV2(ctx, caps, seedHash, job.FrontOrderID, job.DemandID, req)
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	if err := caps.Jobs.SetChunkCount(ctx, job.JobID, seedMeta.ChunkCount); err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
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

	quote := QuoteReport{}
	if quotes, foundQuotes := caps.Jobs.ListQuotes(ctx, job.JobID); foundQuotes {
		for _, q := range quotes {
			if q.Selected {
				quote = q
				break
			}
		}
		if quote.SellerPubkey == "" && len(quotes) > 0 {
			quote = quotes[0]
		}
	}

	completedIndexes := make([]uint32, 0, seedMeta.ChunkCount)
	paidTotal := uint64(0)
	for _, chunk := range storedChunks {
		if chunk.State == ChunkStateStored {
			completedIndexes = append(completedIndexes, chunk.ChunkIndex)
			paidTotal += chunk.ChunkPriceSat
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
			refreshed.CompletedChunks = uint32(len(completedIndexes))
			refreshed.PaidTotalSat = paidTotal
			refreshed.OutputFilePath = localFile.FilePath
			refreshed.PartFilePath = partFilePath
			refreshed.FrontOrderID = job.FrontOrderID
			return resultFromJob(refreshed), nil
		}
		job.State = StateDone
		job.CompletedChunks = uint32(len(completedIndexes))
		job.PaidTotalSat = paidTotal
		job.OutputFilePath = localFile.FilePath
		job.PartFilePath = partFilePath
		return resultFromJob(job), nil
	}
	if caps.Transfers == nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeModuleDisabled, "transfer runner is not available"))
	}
	quotes, foundQuotes := caps.Jobs.ListQuotes(ctx, job.JobID)
	if !foundQuotes || len(quotes) == 0 {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, NewError(CodeQuoteUnavailable, "selected quote is not available"))
	}
	transferRes, err := caps.Transfers.RunTransferByStrategy(ctx, StrategyTransferRequest{
		FrontOrderID:    job.FrontOrderID,
		DemandID:        job.DemandID,
		SeedHash:        seedHash,
		ChunkCount:      seedMeta.ChunkCount,
		ArbiterPubHex:   strings.TrimSpace(req.ArbiterPubkeyHex),
		MaxSeedPrice:    req.MaxSeedPrice,
		MaxChunkPrice:   req.MaxChunkPrice,
		Strategy:        strings.TrimSpace(req.Strategy),
		PoolAmountSat:   req.PoolAmountSat,
		MaxChunkRetries: req.MaxChunkRetries,
		Candidates:      append([]QuoteReport(nil), quotes...),
	})
	if err != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, err)
	}
	completedIndexes, completed, paidTotal, applyErr := applyTransferResultsV2(ctx, caps, job.JobID, seedHash, seedMeta, quote, transferRes, storedChunkMap)
	if applyErr != nil {
		return result, markDownloadJobFailed(ctx, caps.Jobs, job.JobID, applyErr)
	}
	localFile, completeErr := caps.Files.CompleteFile(ctx, CompleteFileInput{
		SeedHash:              seedHash,
		PartFilePath:          partFilePath,
		RecommendedFileName:   strings.TrimSpace(quote.RecommendedFileName),
		MimeType:              strings.TrimSpace(quote.MimeType),
		AvailableChunkIndexes: completedIndexes,
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
		refreshed.FrontOrderID = job.FrontOrderID
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

func ensureDownloadFrontOrderV2(ctx context.Context, caps DownloadCaps, job Job, req StartRequest) (string, error) {
	if caps.FrontOrders == nil {
		return "", NewError(CodeModuleDisabled, "front order binder is not available")
	}
	frontOrderID := strings.TrimSpace(job.FrontOrderID)
	if frontOrderID != "" {
		if err := caps.FrontOrders.BindJobFrontOrder(ctx, job.JobID, frontOrderID); err != nil {
			return "", err
		}
		return frontOrderID, nil
	}
	frontOrderID, err := caps.FrontOrders.EnsureFrontOrder(ctx, FrontOrderRequest{
		JobID:          job.JobID,
		SeedHash:       job.SeedHash,
		FrontOrderID:   strings.TrimSpace(req.FrontOrderID),
		Note:           "getfilebyhash",
		OwnerPubkeyHex: "",
		TargetObjectID: job.SeedHash,
	})
	if err != nil {
		return "", err
	}
	if err := caps.Jobs.SetFrontOrderID(ctx, job.JobID, frontOrderID); err != nil {
		return "", err
	}
	if err := caps.FrontOrders.BindJobFrontOrder(ctx, job.JobID, frontOrderID); err != nil {
		return "", err
	}
	return frontOrderID, nil
}

func persistDownloadQuotesV2(ctx context.Context, jobs JobStore, jobID string, quotes []QuoteReport, selection QuoteSelection) error {
	if jobs == nil {
		return NewError(CodeModuleDisabled, "job store is not available")
	}
	rejected := make(map[string]string, len(selection.Rejected))
	for _, quote := range selection.Rejected {
		rejected[strings.ToLower(strings.TrimSpace(quote.SellerPubkey))] = strings.TrimSpace(quote.RejectReason)
	}
	selectedSeller := strings.ToLower(strings.TrimSpace(selection.Primary.SellerPubkey))
	for _, quote := range quotes {
		quote.Selected = strings.EqualFold(quote.SellerPubkey, selectedSeller)
		if reason, ok := rejected[strings.ToLower(strings.TrimSpace(quote.SellerPubkey))]; ok {
			quote.RejectReason = reason
		} else if !quote.Selected {
			quote.RejectReason = ""
		}
		if err := jobs.AppendQuote(ctx, jobID, quote); err != nil {
			return err
		}
	}
	return nil
}

type quoteWaiterFromReader struct {
	reader QuoteReader
}

func (w quoteWaiterFromReader) WaitQuotes(ctx context.Context, req QuoteWaitRequest) ([]QuoteReport, error) {
	if w.reader == nil {
		return nil, NewError(CodeModuleDisabled, "quote reader is not available")
	}
	return w.reader.ListQuotes(ctx, req.DemandID)
}

func resolveDownloadSeedMetaV2(ctx context.Context, caps DownloadCaps, frontOrderID string, seedHash string, demandID string, candidates []QuoteReport, req StartRequest, demandChunkCount uint32) (SeedMeta, error) {
	if caps.Seeds != nil {
		if meta, found, err := caps.Seeds.LoadSeedMeta(ctx, seedHash); err != nil {
			return SeedMeta{}, err
		} else if found && meta.ChunkCount > 0 {
			return meta, nil
		}
	}
	if caps.SeedResolver == nil {
		return SeedMeta{}, NewError(CodeModuleDisabled, "seed resolver is not available")
	}
	meta, err := caps.SeedResolver.ResolveAndSaveSeedMeta(ctx, SeedResolveRequest{
		SeedHash:     seedHash,
		FrontOrderID: frontOrderID,
		DemandID:     demandID,
		Candidates:   append([]QuoteReport(nil), candidates...),
	})
	if err != nil {
		return SeedMeta{}, err
	}
	if meta.ChunkCount == 0 && demandChunkCount > 0 {
		return SeedMeta{}, NewError(CodeBadRequest, "seed chunk count is required")
	}
	if req.ChunkCount > 0 && meta.ChunkCount != req.ChunkCount {
		return SeedMeta{}, NewError(CodeBadRequest, "chunk count does not match seed metadata")
	}
	return meta, nil
}

func resolveSeedMetaForResumeV2(ctx context.Context, caps DownloadCaps, seedHash string, frontOrderID string, demandID string, req StartRequest) (SeedMeta, error) {
	if caps.Seeds != nil {
		if meta, found, err := caps.Seeds.LoadSeedMeta(ctx, seedHash); err != nil {
			return SeedMeta{}, err
		} else if found && meta.ChunkCount > 0 {
			return meta, nil
		}
	}
	if caps.SeedResolver == nil {
		return SeedMeta{}, NewError(CodeModuleDisabled, "seed resolver is not available")
	}
	meta, err := caps.SeedResolver.ResolveAndSaveSeedMeta(ctx, SeedResolveRequest{
		SeedHash:     seedHash,
		FrontOrderID: frontOrderID,
		DemandID:     demandID,
	})
	if err != nil {
		return SeedMeta{}, err
	}
	if req.ChunkCount > 0 && meta.ChunkCount != req.ChunkCount {
		return SeedMeta{}, NewError(CodeBadRequest, "chunk count does not match seed metadata")
	}
	return meta, nil
}

func applyTransferResultsV2(ctx context.Context, caps DownloadCaps, jobID string, seedHash string, seedMeta SeedMeta, quote QuoteReport, transferRes StrategyTransferResult, storedChunkMap map[uint32]ChunkReport) ([]uint32, uint32, uint64, error) {
	if caps.Files == nil {
		return nil, 0, 0, NewError(CodeModuleDisabled, "file store is not available")
	}
	byIndex := make(map[uint32]TransferChunkResult, len(transferRes.Chunks))
	for _, chunk := range transferRes.Chunks {
		byIndex[chunk.ChunkIndex] = chunk
	}
	completed := uint32(0)
	paidTotal := uint64(0)
	completedIndexes := make([]uint32, 0, seedMeta.ChunkCount)
	for idx := uint32(0); idx < seedMeta.ChunkCount; idx++ {
		if storedChunkMap != nil {
			if stored, ok := storedChunkMap[idx]; ok && stored.State == ChunkStateStored {
				completed++
				paidTotal += stored.ChunkPriceSat
				completedIndexes = append(completedIndexes, idx)
				continue
			}
		}
		chunkRes, ok := byIndex[idx]
		if !ok {
			return nil, completed, paidTotal, NewError(CodeTransferFailed, "missing chunk result")
		}
		if len(chunkRes.Bytes) == 0 {
			return nil, completed, paidTotal, NewError(CodeTransferFailed, "chunk bytes are empty")
		}
		if err := caps.Files.MarkChunkStored(ctx, StoredChunkInput{
			SeedHash:   seedHash,
			ChunkIndex: idx,
			ChunkBytes: chunkRes.Bytes,
		}); err != nil {
			return nil, completed, paidTotal, NewError(CodeChunkStoreFailed, err.Error())
		}
		sellerPubkey := chunkRes.SellerPubkey
		if sellerPubkey == "" {
			sellerPubkey = quote.SellerPubkey
		}
		report := ChunkReport{
			ChunkIndex:    idx,
			State:         ChunkStateStored,
			SellerPubkey:  sellerPubkey,
			ChunkPriceSat: chunkRes.PaidSat,
			SpeedBps:      chunkRes.SpeedBps,
			Selected:      true,
		}
		if err := caps.Jobs.AppendChunkReport(ctx, jobID, report); err != nil {
			return nil, completed, paidTotal, NewError(CodeChunkStoreFailed, err.Error())
		}
		completed++
		paidTotal += chunkRes.PaidSat
		completedIndexes = append(completedIndexes, idx)
	}
	return completedIndexes, completed, paidTotal, nil
}
