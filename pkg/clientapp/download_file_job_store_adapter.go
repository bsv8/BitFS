package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	filedownload "github.com/bsv8/BitFS/pkg/clientapp/download/file"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procgetfilebyhashchunks"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procgetfilebyhashjobs"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/procgetfilebyhashquotes"
)

// downloadFileJobStoreAdapter 只把运行入口准备好的 clientDB 适配成 filedownload.JobStore。
// 它不拥有 DB，不创建 DB。
// 设计约束：
// - 只接收已准备好的 store 能力，不自己 newClientDB；
// - 写操作用 ent 事务，读操作走统一 ReadEnt；
// - seed_hash 唯一约束保证并发下去重，冲突时查回已有 job（get-or-create）。
type downloadFileJobStoreAdapter struct {
	store *clientDB
}

func newDownloadFileJobStoreAdapter(store *clientDB) *downloadFileJobStoreAdapter {
	if store == nil {
		return nil
	}
	return &downloadFileJobStoreAdapter{
		store: store,
	}
}

func jobToModule(entJob *gen.ProcGetFileByHashJobs) filedownload.Job {
	frontOrderID := ""
	if entJob.FrontOrderID != nil {
		frontOrderID = strings.TrimSpace(*entJob.FrontOrderID)
	}
	return filedownload.Job{
		JobID:           entJob.JobID,
		SeedHash:        entJob.SeedHash,
		FrontOrderID:    frontOrderID,
		DemandID:        entJob.DemandID,
		State:           entJob.State,
		ChunkCount:      uint32(entJob.ChunkCount),
		CompletedChunks: uint32(entJob.CompletedChunks),
		PaidTotalSat:    uint64(entJob.PaidTotalSat),
		OutputFilePath:  entJob.OutputFilePath,
		PartFilePath:    entJob.PartFilePath,
		Error:           entJob.Error,
		CreatedAt:       time.Unix(entJob.CreatedAtUnix, 0),
		UpdatedAt:       time.Unix(entJob.UpdatedAtUnix, 0),
	}
}

// CreateJob 创建新 job 或返回已有 job（seed_hash 去重）。
// 返回值 (job, created, error)：
//   - job: 实际使用的 job（含 DB 中的完整数据）
//   - created: true=本次新建，false=复用已有 job
//   - error: 创建失败（不含 seed_hash 冲突，冲突时返回已有 job + created=false）
func (s *downloadFileJobStoreAdapter) CreateJob(ctx context.Context, job *filedownload.Job) (filedownload.Job, bool, error) {
	if job == nil {
		return filedownload.Job{}, false, filedownload.NewError(filedownload.CodeBadRequest, "job is nil")
	}
	if job.JobID == "" {
		return filedownload.Job{}, false, filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	now := time.Now().Unix()

	var created bool
	var resultJob filedownload.Job

	err := s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		createdEnt, err := tx.ProcGetFileByHashJobs.Create().
			SetJobID(job.JobID).
			SetSeedHash(job.SeedHash).
			SetDemandID(job.DemandID).
			SetState(job.State).
			SetChunkCount(int64(job.ChunkCount)).
			SetCompletedChunks(int64(job.CompletedChunks)).
			SetPaidTotalSat(int64(job.PaidTotalSat)).
			SetOutputFilePath(job.OutputFilePath).
			SetPartFilePath(job.PartFilePath).
			SetError(job.Error).
			SetCreatedAtUnix(now).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err == nil && strings.TrimSpace(job.FrontOrderID) != "" {
			_, err = tx.ProcGetFileByHashJobs.Update().
				Where(procgetfilebyhashjobs.JobIDEQ(createdEnt.JobID)).
				SetFrontOrderID(strings.TrimSpace(job.FrontOrderID)).
				SetUpdatedAtUnix(now).
				Save(ctx)
		}
		if err != nil {
			if isEntConstraintError(err) {
				// seed_hash 冲突，查回已有 job
				existing, err := tx.ProcGetFileByHashJobs.Query().
					Where(procgetfilebyhashjobs.SeedHashEQ(job.SeedHash)).
					Only(ctx)
				if err != nil {
					if gen.IsNotFound(err) {
						return fmt.Errorf("seed_hash conflict but job not found: %s", job.SeedHash)
					}
					return fmt.Errorf("query existing job after conflict: %w", err)
				}
				resultJob = jobToModule(existing)
				created = false
				return nil
			}
			obs.Error("getfilebyhash", "create_job_db_failed", map[string]any{
				"job_id":    job.JobID,
				"seed_hash": job.SeedHash,
				"error":     err.Error(),
			})
			return fmt.Errorf("create job failed: %w", err)
		}
		resultJob = jobToModule(createdEnt)
		created = true
		return nil
	})
	if err != nil {
		return filedownload.Job{}, false, err
	}
	return resultJob, created, nil
}

func (s *downloadFileJobStoreAdapter) GetJob(ctx context.Context, jobID string) (filedownload.Job, bool) {
	if jobID == "" {
		return filedownload.Job{}, false
	}
	result, err := readEntValue(ctx, s.store, func(root EntReadRoot) (filedownload.Job, error) {
		entJob, err := root.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return filedownload.Job{}, fmt.Errorf("not_found")
			}
			obs.Error("getfilebyhash", "get_job_db_failed", map[string]any{
				"job_id": jobID,
				"error":  err.Error(),
			})
			return filedownload.Job{}, err
		}
		return jobToModule(entJob), nil
	})
	if err != nil {
		if err.Error() == "not_found" {
			return filedownload.Job{}, false
		}
		return filedownload.Job{}, false
	}
	return result, true
}

func (s *downloadFileJobStoreAdapter) FindJobBySeedHash(ctx context.Context, seedHash string) (filedownload.Job, bool, error) {
	if seedHash == "" {
		return filedownload.Job{}, false, fmt.Errorf("seed_hash is required")
	}
	result, err := readEntValue(ctx, s.store, func(root EntReadRoot) (filedownload.Job, error) {
		entJob, err := root.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.SeedHashEQ(seedHash)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return filedownload.Job{}, fmt.Errorf("not_found")
			}
			obs.Error("getfilebyhash", "find_job_db_failed", map[string]any{
				"seed_hash": seedHash,
				"error":     err.Error(),
			})
			return filedownload.Job{}, err
		}
		return jobToModule(entJob), nil
	})
	if err != nil {
		if err.Error() == "not_found" {
			return filedownload.Job{}, false, nil
		}
		return filedownload.Job{}, false, fmt.Errorf("find job failed: %w", err)
	}
	return result, true, nil
}

func (s *downloadFileJobStoreAdapter) UpdateJobState(ctx context.Context, jobID string, state string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}
	if state == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "state is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetState(state).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("update job state failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetChunkCount(ctx context.Context, jobID string, chunkCount uint32) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetChunkCount(int64(chunkCount)).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set chunk_count failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetDemandID(ctx context.Context, jobID string, demandID string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetDemandID(demandID).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set demand_id failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetFrontOrderID(ctx context.Context, jobID string, frontOrderID string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}
	frontOrderID = strings.TrimSpace(frontOrderID)
	if frontOrderID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "front_order_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		entJob, err := tx.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
			}
			return fmt.Errorf("find job failed: %w", err)
		}
		current := ""
		if entJob.FrontOrderID != nil {
			current = strings.TrimSpace(*entJob.FrontOrderID)
		}
		if current != "" && !strings.EqualFold(current, frontOrderID) {
			return fmt.Errorf("front_order_id already bound: %s", current)
		}
		if current == frontOrderID {
			return nil
		}
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetFrontOrderID(frontOrderID).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set front_order_id failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetError(ctx context.Context, jobID string, message string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetError(message).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set error failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetPartFilePath(ctx context.Context, jobID string, partFilePath string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetPartFilePath(partFilePath).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set part_file_path failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) AppendChunkReport(ctx context.Context, jobID string, report filedownload.ChunkReport) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	now := time.Now().Unix()

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		entJob, err := tx.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
			}
			return fmt.Errorf("find job failed: %w", err)
		}

		seedHash := entJob.SeedHash
		countChunk := report.State == filedownload.ChunkStateStored

		existingChunk, err := tx.ProcGetFileByHashChunks.Query().
			Where(
				procgetfilebyhashchunks.JobIDEQ(jobID),
				procgetfilebyhashchunks.ChunkIndexEQ(int64(report.ChunkIndex)),
			).Only(ctx)

		if err != nil && !gen.IsNotFound(err) {
			return fmt.Errorf("check existing chunk: %w", err)
		}

		wasStored := existingChunk != nil && existingChunk.State == filedownload.ChunkStateStored
		if existingChunk != nil {
			_, err = tx.ProcGetFileByHashChunks.Update().
				Where(procgetfilebyhashchunks.IDEQ(existingChunk.ID)).
				SetState(report.State).
				SetSellerPubkeyHex(report.SellerPubkey).
				SetChunkPriceSat(int64(report.ChunkPriceSat)).
				SetSpeedBps(int64(report.SpeedBps)).
				SetSelected(report.Selected).
				SetRejectReason(report.RejectReason).
				SetUpdatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return fmt.Errorf("update chunk report failed: %w", err)
			}
			if countChunk && !wasStored {
				_, err = tx.ProcGetFileByHashJobs.Update().
					Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
					SetCompletedChunks(entJob.CompletedChunks + 1).
					SetPaidTotalSat(entJob.PaidTotalSat + int64(report.ChunkPriceSat)).
					SetUpdatedAtUnix(now).
					Save(ctx)
				if err != nil {
					return fmt.Errorf("update job progress failed: %w", err)
				}
			}
			return nil
		}

		_, err = tx.ProcGetFileByHashChunks.Create().
			SetJobID(jobID).
			SetSeedHash(seedHash).
			SetChunkIndex(int64(report.ChunkIndex)).
			SetState(report.State).
			SetSellerPubkeyHex(report.SellerPubkey).
			SetChunkPriceSat(int64(report.ChunkPriceSat)).
			SetSpeedBps(int64(report.SpeedBps)).
			SetSelected(report.Selected).
			SetRejectReason(report.RejectReason).
			SetUpdatedAtUnix(now).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("append chunk report failed: %w", err)
		}
		if countChunk {
			_, err = tx.ProcGetFileByHashJobs.Update().
				Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
				SetCompletedChunks(entJob.CompletedChunks + 1).
				SetPaidTotalSat(entJob.PaidTotalSat + int64(report.ChunkPriceSat)).
				SetUpdatedAtUnix(now).
				Save(ctx)
			if err != nil {
				return fmt.Errorf("update job progress failed: %w", err)
			}
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) AppendQuote(ctx context.Context, jobID string, quote filedownload.QuoteReport) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	var availableChunksJSON string
	if len(quote.AvailableChunks) > 0 {
		b, _ := json.Marshal(quote.AvailableChunks)
		availableChunksJSON = string(b)
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		entJob, err := tx.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
			}
			return fmt.Errorf("find job failed: %w", err)
		}

		seedHash := entJob.SeedHash

		existingQuote, err := tx.ProcGetFileByHashQuotes.Query().
			Where(
				procgetfilebyhashquotes.JobIDEQ(jobID),
				procgetfilebyhashquotes.SellerPubkeyHexEQ(quote.SellerPubkey),
			).Only(ctx)

		if err != nil && !gen.IsNotFound(err) {
			return fmt.Errorf("check existing quote: %w", err)
		}

		if existingQuote != nil {
			_, err = tx.ProcGetFileByHashQuotes.Update().
				Where(procgetfilebyhashquotes.IDEQ(existingQuote.ID)).
				SetSeedPriceSat(int64(quote.SeedPriceSat)).
				SetChunkPriceSat(int64(quote.ChunkPriceSat)).
				SetChunkCount(int64(quote.ChunkCount)).
				SetAvailableChunksJSON(availableChunksJSON).
				SetRecommendedFileName(quote.RecommendedFileName).
				SetMimeType(quote.MimeType).
				SetFileSizeBytes(int64(quote.FileSizeBytes)).
				SetQuoteTimestamp(quote.QuoteTimestamp).
				SetExpiresAtUnix(quote.ExpiresAtUnix).
				SetSelected(quote.Selected).
				SetRejectReason(quote.RejectReason).
				Save(ctx)
			if err != nil {
				return fmt.Errorf("update quote failed: %w", err)
			}
			return nil
		}

		_, err = tx.ProcGetFileByHashQuotes.Create().
			SetJobID(jobID).
			SetSeedHash(seedHash).
			SetSellerPubkeyHex(quote.SellerPubkey).
			SetSeedPriceSat(int64(quote.SeedPriceSat)).
			SetChunkPriceSat(int64(quote.ChunkPriceSat)).
			SetChunkCount(int64(quote.ChunkCount)).
			SetAvailableChunksJSON(availableChunksJSON).
			SetRecommendedFileName(quote.RecommendedFileName).
			SetMimeType(quote.MimeType).
			SetFileSizeBytes(int64(quote.FileSizeBytes)).
			SetQuoteTimestamp(quote.QuoteTimestamp).
			SetExpiresAtUnix(quote.ExpiresAtUnix).
			SetSelected(quote.Selected).
			SetRejectReason(quote.RejectReason).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("append quote failed: %w", err)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) SetOutputPath(ctx context.Context, jobID string, path string) error {
	if jobID == "" {
		return filedownload.NewError(filedownload.CodeBadRequest, "job_id is required")
	}

	return s.store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		updated, err := tx.ProcGetFileByHashJobs.Update().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			SetOutputFilePath(path).
			SetUpdatedAtUnix(time.Now().Unix()).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("set output_path failed: %w", err)
		}
		if updated == 0 {
			return filedownload.NewError(filedownload.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *downloadFileJobStoreAdapter) ListChunks(ctx context.Context, jobID string) ([]filedownload.ChunkReport, bool) {
	if jobID == "" {
		return nil, false
	}

	// 先验证 job 存在
	_, err := readEntValue(ctx, s.store, func(root EntReadRoot) (struct{}, error) {
		_, err := root.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return struct{}{}, fmt.Errorf("not_found")
			}
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	if err != nil {
		if err.Error() == "not_found" {
			return nil, false
		}
		obs.Error("getfilebyhash", "check_job_exists_failed", map[string]any{
			"job_id": jobID,
			"error":  err.Error(),
		})
		return nil, false
	}

	result, err := readEntValue(ctx, s.store, func(root EntReadRoot) ([]filedownload.ChunkReport, error) {
		entChunks, err := root.ProcGetFileByHashChunks.Query().
			Where(procgetfilebyhashchunks.JobIDEQ(jobID)).
			All(ctx)
		if err != nil {
			obs.Error("getfilebyhash", "list_chunks_db_failed", map[string]any{
				"job_id": jobID,
				"error":  err.Error(),
			})
			return nil, err
		}
		if len(entChunks) == 0 {
			return []filedownload.ChunkReport{}, nil
		}
		chunks := make([]filedownload.ChunkReport, 0, len(entChunks))
		for _, c := range entChunks {
			chunks = append(chunks, filedownload.ChunkReport{
				ChunkIndex:    uint32(c.ChunkIndex),
				State:         c.State,
				SellerPubkey:  c.SellerPubkeyHex,
				ChunkPriceSat: uint64(c.ChunkPriceSat),
				SpeedBps:      uint64(c.SpeedBps),
				Selected:      c.Selected,
				Error:         c.RejectReason,
				RejectReason:  c.RejectReason,
			})
		}
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].ChunkIndex < chunks[j].ChunkIndex
		})
		return chunks, nil
	})
	if err != nil {
		return nil, false
	}
	return result, true
}

func (s *downloadFileJobStoreAdapter) ListQuotes(ctx context.Context, jobID string) ([]filedownload.QuoteReport, bool) {
	if jobID == "" {
		return nil, false
	}

	_, err := readEntValue(ctx, s.store, func(root EntReadRoot) (struct{}, error) {
		_, err := root.ProcGetFileByHashJobs.Query().
			Where(procgetfilebyhashjobs.JobIDEQ(jobID)).
			Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return struct{}{}, fmt.Errorf("not_found")
			}
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	if err != nil {
		if err.Error() == "not_found" {
			return nil, false
		}
		obs.Error("getfilebyhash", "check_job_exists_failed", map[string]any{
			"job_id": jobID,
			"error":  err.Error(),
		})
		return nil, false
	}

	result, err := readEntValue(ctx, s.store, func(root EntReadRoot) ([]filedownload.QuoteReport, error) {
		entQuotes, err := root.ProcGetFileByHashQuotes.Query().
			Where(procgetfilebyhashquotes.JobIDEQ(jobID)).
			All(ctx)
		if err != nil {
			obs.Error("getfilebyhash", "list_quotes_db_failed", map[string]any{
				"job_id": jobID,
				"error":  err.Error(),
			})
			return nil, err
		}
		if len(entQuotes) == 0 {
			return []filedownload.QuoteReport{}, nil
		}
		quotes := make([]filedownload.QuoteReport, 0, len(entQuotes))
		for _, q := range entQuotes {
			var availableChunks []uint32
			if q.AvailableChunksJSON != "" {
				_ = json.Unmarshal([]byte(q.AvailableChunksJSON), &availableChunks)
			}
			quotes = append(quotes, filedownload.QuoteReport{
				SellerPubkey:        q.SellerPubkeyHex,
				SeedPriceSat:        uint64(q.SeedPriceSat),
				ChunkPriceSat:       uint64(q.ChunkPriceSat),
				ChunkCount:          uint32(q.ChunkCount),
				AvailableChunks:     availableChunks,
				RecommendedFileName: q.RecommendedFileName,
				MimeType:            q.MimeType,
				FileSizeBytes:       uint64(q.FileSizeBytes),
				QuoteTimestamp:      q.QuoteTimestamp,
				ExpiresAtUnix:       q.ExpiresAtUnix,
				Selected:            q.Selected,
				RejectReason:        q.RejectReason,
			})
		}
		return quotes, nil
	})
	if err != nil {
		return nil, false
	}
	return result, true
}

func (s *downloadFileJobStoreAdapter) ListJobs(ctx context.Context) []filedownload.Job {
	result, err := readEntValue(ctx, s.store, func(root EntReadRoot) ([]filedownload.Job, error) {
		entJobs, err := root.ProcGetFileByHashJobs.Query().All(ctx)
		if err != nil {
			obs.Error("getfilebyhash", "list_jobs_db_failed", map[string]any{
				"error": err.Error(),
			})
			return nil, err
		}
		if len(entJobs) == 0 {
			return []filedownload.Job{}, nil
		}
		jobs := make([]filedownload.Job, 0, len(entJobs))
		for _, entJob := range entJobs {
			jobs = append(jobs, jobToModule(entJob))
		}
		return jobs, nil
	})
	if err != nil {
		return []filedownload.Job{}
	}
	return result
}

func isEntConstraintError(err error) bool {
	return gen.IsConstraintError(err)
}
