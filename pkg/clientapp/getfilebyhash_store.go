package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/modules/getfilebyhash"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
)

// getFileByHashJobStore DB-backed JobStore 实现（getfilebyhash 模块专用）。
// 设计约束：
// - 只接收已准备好的 store 能力，不自己 newClientDB；
// - 不接收 *sql.DB，只走 clientDB 暴露的 SQL 能力。
type getFileByHashJobStore struct {
	store *clientDB
	ent   *gen.Client
}

func newGetFileByHashJobStore(store *clientDB) *getFileByHashJobStore {
	if store == nil {
		return nil
	}
	return &getFileByHashJobStore{
		store: store,
		ent:   store.ent,
	}
}

func (s *getFileByHashJobStore) CreateJob(ctx context.Context, job *getfilebyhash.Job) error {
	if job == nil {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job is nil")
	}
	if job.JobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		now := time.Now().Unix()
		_, err := db.ExecContext(ctx, `INSERT INTO proc_getfilebyhash_jobs
			(job_id,seed_hash,demand_id,state,chunk_count,completed_chunks,paid_total_sat,
			output_file_path,part_file_path,error,created_at_unix,updated_at_unix)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
			job.JobID, job.SeedHash, job.DemandID, job.State, job.ChunkCount,
			job.CompletedChunks, job.PaidTotalSat, job.OutputFilePath, job.PartFilePath,
			job.Error, now, now)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job already exists: "+job.JobID)
			}
			obs.Error("getfilebyhash", "create_job_db_failed", map[string]any{
				"job_id":   job.JobID,
				"seed_hash": job.SeedHash,
				"error":    err.Error(),
			})
			return fmt.Errorf("create job failed: %w", err)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) GetJob(ctx context.Context, jobID string) (getfilebyhash.Job, bool) {
	var job getfilebyhash.Job
	var demandID, outputFilePath, partFilePath, errMsg sql.NullString
	var completedChunks, paidTotalSat, createdAt, updatedAt int64
	var chunkCount uint32
	var state string

	fetchErr := s.store.Do(ctx, func(db SQLConn) error {
		return db.QueryRowContext(ctx, `SELECT job_id,seed_hash,demand_id,state,chunk_count,
			completed_chunks,paid_total_sat,output_file_path,part_file_path,error,
			created_at_unix,updated_at_unix FROM proc_getfilebyhash_jobs WHERE job_id=?`,
			jobID).Scan(&job.JobID, &job.SeedHash, &demandID, &state, &chunkCount,
			&completedChunks, &paidTotalSat, &outputFilePath, &partFilePath, &errMsg,
			&createdAt, &updatedAt)
	})

	if fetchErr != nil {
		if fetchErr == sql.ErrNoRows {
			return getfilebyhash.Job{}, false
		}
		obs.Error("getfilebyhash", "get_job_db_failed", map[string]any{
			"job_id": jobID,
			"error":  fetchErr.Error(),
		})
		return getfilebyhash.Job{}, false
	}

	job.DemandID = demandID.String
	job.State = state
	job.ChunkCount = chunkCount
	job.CompletedChunks = uint32(completedChunks)
	job.PaidTotalSat = uint64(paidTotalSat)
	job.OutputFilePath = outputFilePath.String
	job.PartFilePath = partFilePath.String
	job.Error = errMsg.String
	job.CreatedAt = time.Unix(createdAt, 0)
	job.UpdatedAt = time.Unix(updatedAt, 0)
	return job, true
}

func (s *getFileByHashJobStore) FindJobBySeedHash(ctx context.Context, seedHash string) (getfilebyhash.Job, bool, error) {
	var job getfilebyhash.Job
	var demandID, outputFilePath, partFilePath, errMsg sql.NullString
	var completedChunks, paidTotalSat, createdAt, updatedAt int64
	var chunkCount uint32
	var state string

	err := s.store.Do(ctx, func(db SQLConn) error {
		return db.QueryRowContext(ctx, `SELECT job_id,seed_hash,demand_id,state,chunk_count,
			completed_chunks,paid_total_sat,output_file_path,part_file_path,error,
			created_at_unix,updated_at_unix FROM proc_getfilebyhash_jobs WHERE seed_hash=?`,
			seedHash).Scan(&job.JobID, &job.SeedHash, &demandID, &state, &chunkCount,
			&completedChunks, &paidTotalSat, &outputFilePath, &partFilePath, &errMsg,
			&createdAt, &updatedAt)
	})

	if err != nil {
		if err == sql.ErrNoRows {
			return getfilebyhash.Job{}, false, nil
		}
		obs.Error("getfilebyhash", "find_job_db_failed", map[string]any{
			"seed_hash": seedHash,
			"error":     err.Error(),
		})
		return getfilebyhash.Job{}, false, fmt.Errorf("find job failed: %w", err)
	}

	job.DemandID = demandID.String
	job.State = state
	job.ChunkCount = chunkCount
	job.CompletedChunks = uint32(completedChunks)
	job.PaidTotalSat = uint64(paidTotalSat)
	job.OutputFilePath = outputFilePath.String
	job.PartFilePath = partFilePath.String
	job.Error = errMsg.String
	job.CreatedAt = time.Unix(createdAt, 0)
	job.UpdatedAt = time.Unix(updatedAt, 0)
	return job, true, nil
}

func (s *getFileByHashJobStore) UpdateJobState(ctx context.Context, jobID string, state string) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}
	if state == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "state is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		result, err := db.ExecContext(ctx, `UPDATE proc_getfilebyhash_jobs
			SET state=?,updated_at_unix=? WHERE job_id=?`,
			state, time.Now().Unix(), jobID)
		if err != nil {
			return fmt.Errorf("update job state failed: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return getfilebyhash.NewError(getfilebyhash.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) SetDemandID(ctx context.Context, jobID string, demandID string) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		result, err := db.ExecContext(ctx, `UPDATE proc_getfilebyhash_jobs
			SET demand_id=?,updated_at_unix=? WHERE job_id=?`,
			demandID, time.Now().Unix(), jobID)
		if err != nil {
			return fmt.Errorf("set demand_id failed: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return getfilebyhash.NewError(getfilebyhash.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) SetPartFilePath(ctx context.Context, jobID string, partFilePath string) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		result, err := db.ExecContext(ctx, `UPDATE proc_getfilebyhash_jobs
			SET part_file_path=?,updated_at_unix=? WHERE job_id=?`,
			partFilePath, time.Now().Unix(), jobID)
		if err != nil {
			return fmt.Errorf("set part_file_path failed: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return getfilebyhash.NewError(getfilebyhash.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) AppendChunkReport(ctx context.Context, jobID string, report getfilebyhash.ChunkReport) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		now := time.Now().Unix()
		_, err := db.ExecContext(ctx, `INSERT INTO proc_getfilebyhash_chunks
			(job_id,seed_hash,chunk_index,state,seller_pubkey_hex,chunk_price_sat,
			speed_bps,selected,reject_reason,updated_at_unix)
			VALUES(?,?,?,?,?,?,?,?,?,?)`,
			jobID, "", report.ChunkIndex, report.State, report.SellerPubkey,
			report.ChunkPriceSat, report.SpeedBps, report.Selected, report.RejectReason, now)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				_, err = db.ExecContext(ctx, `UPDATE proc_getfilebyhash_chunks
					SET state=?,seller_pubkey_hex=?,chunk_price_sat=?,speed_bps=?,
					selected=?,reject_reason=?,updated_at_unix=?
					WHERE job_id=? AND chunk_index=?`,
					report.State, report.SellerPubkey, report.ChunkPriceSat,
					report.SpeedBps, report.Selected, report.RejectReason, now,
					jobID, report.ChunkIndex)
				if err != nil {
					return fmt.Errorf("update chunk report failed: %w", err)
				}
				return nil
			}
			return fmt.Errorf("append chunk report failed: %w", err)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) AppendQuote(ctx context.Context, jobID string, quote getfilebyhash.QuoteReport) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	var availableChunksJSON string
	if len(quote.AvailableChunks) > 0 {
		b, _ := json.Marshal(quote.AvailableChunks)
		availableChunksJSON = string(b)
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		_, err := db.ExecContext(ctx, `INSERT INTO proc_getfilebyhash_quotes
			(job_id,seed_hash,seller_pubkey_hex,seed_price_sat,chunk_price_sat,
			chunk_count,available_chunks_json,recommended_file_name,mime_type,
			file_size_bytes,quote_timestamp,expires_at_unix,selected,reject_reason)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			jobID, "", quote.SellerPubkey, quote.SeedPriceSat, quote.ChunkPriceSat,
			quote.ChunkCount, availableChunksJSON, quote.RecommendedFileName,
			quote.MimeType, quote.FileSizeBytes, quote.QuoteTimestamp,
			quote.ExpiresAtUnix, quote.Selected, quote.RejectReason)
		if err != nil {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				_, err = db.ExecContext(ctx, `UPDATE proc_getfilebyhash_quotes
					SET seed_price_sat=?,chunk_price_sat=?,chunk_count=?,
					available_chunks_json=?,recommended_file_name=?,mime_type=?,
					file_size_bytes=?,quote_timestamp=?,expires_at_unix=?,
					selected=?,reject_reason=?
					WHERE job_id=? AND seller_pubkey_hex=?`,
					quote.SeedPriceSat, quote.ChunkPriceSat, quote.ChunkCount,
					availableChunksJSON, quote.RecommendedFileName, quote.MimeType,
					quote.FileSizeBytes, quote.QuoteTimestamp, quote.ExpiresAtUnix,
					quote.Selected, quote.RejectReason, jobID, quote.SellerPubkey)
				if err != nil {
					return fmt.Errorf("update quote failed: %w", err)
				}
				return nil
			}
			return fmt.Errorf("append quote failed: %w", err)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) SetOutputPath(ctx context.Context, jobID string, path string) error {
	if jobID == "" {
		return getfilebyhash.NewError(getfilebyhash.CodeBadRequest, "job_id is required")
	}

	return s.store.Do(ctx, func(db SQLConn) error {
		result, err := db.ExecContext(ctx, `UPDATE proc_getfilebyhash_jobs
			SET output_file_path=?,updated_at_unix=? WHERE job_id=?`,
			path, time.Now().Unix(), jobID)
		if err != nil {
			return fmt.Errorf("set output_path failed: %w", err)
		}
		rows, _ := result.RowsAffected()
		if rows == 0 {
			return getfilebyhash.NewError(getfilebyhash.CodeJobNotFound, "job not found: "+jobID)
		}
		return nil
	})
}

func (s *getFileByHashJobStore) ListChunks(ctx context.Context, jobID string) ([]getfilebyhash.ChunkReport, bool) {
	var chunks []getfilebyhash.ChunkReport

	err := s.store.Do(ctx, func(db SQLConn) error {
		rows, err := db.QueryContext(ctx, `SELECT job_id,chunk_index,state,seller_pubkey_hex,
			chunk_price_sat,speed_bps,selected,reject_reason
			FROM proc_getfilebyhash_chunks WHERE job_id=?`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var jobIDOut, sellerPubkey, state, rejectReason string
			var chunkIndex, chunkPriceSat, speedBps int64
			var selected bool
			if err := rows.Scan(&jobIDOut, &chunkIndex, &state, &sellerPubkey,
				&chunkPriceSat, &speedBps, &selected, &rejectReason); err != nil {
				return err
			}
			chunks = append(chunks, getfilebyhash.ChunkReport{
				ChunkIndex:    uint32(chunkIndex),
				State:         state,
				SellerPubkey:  sellerPubkey,
				ChunkPriceSat: uint64(chunkPriceSat),
				SpeedBps:      uint64(speedBps),
				Selected:      selected,
				RejectReason:  rejectReason,
			})
		}
		return rows.Err()
	})

	if err != nil {
		if err == sql.ErrNoRows || len(chunks) == 0 {
			return nil, false
		}
		obs.Error("getfilebyhash", "list_chunks_db_failed", map[string]any{
			"job_id": jobID,
			"error":  err.Error(),
		})
		return nil, false
	}

	if len(chunks) == 0 {
		return nil, false
	}
	return chunks, true
}

func (s *getFileByHashJobStore) ListQuotes(ctx context.Context, jobID string) ([]getfilebyhash.QuoteReport, bool) {
	var quotes []getfilebyhash.QuoteReport

	err := s.store.Do(ctx, func(db SQLConn) error {
		rows, err := db.QueryContext(ctx, `SELECT job_id,seller_pubkey_hex,seed_price_sat,
			chunk_price_sat,chunk_count,available_chunks_json,recommended_file_name,
			mime_type,file_size_bytes,quote_timestamp,expires_at_unix,selected,reject_reason
			FROM proc_getfilebyhash_quotes WHERE job_id=?`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var jobIDOut, sellerPubkey, recommendedFileName, mimeType, rejectReason string
			var seedPriceSat, chunkPriceSat, chunkCount, fileSizeBytes int64
			var quoteTimestamp, expiresAtUnix int64
			var selected bool
			var availableChunksJSON sql.NullString
			if err := rows.Scan(&jobIDOut, &sellerPubkey, &seedPriceSat, &chunkPriceSat,
				&chunkCount, &availableChunksJSON, &recommendedFileName, &mimeType,
				&fileSizeBytes, &quoteTimestamp, &expiresAtUnix, &selected, &rejectReason); err != nil {
				return err
			}
			var availableChunks []uint32
			if availableChunksJSON.Valid && availableChunksJSON.String != "" {
				_ = json.Unmarshal([]byte(availableChunksJSON.String), &availableChunks)
			}
			quotes = append(quotes, getfilebyhash.QuoteReport{
				SellerPubkey:        sellerPubkey,
				SeedPriceSat:        uint64(seedPriceSat),
				ChunkPriceSat:       uint64(chunkPriceSat),
				ChunkCount:          uint32(chunkCount),
				AvailableChunks:     availableChunks,
				RecommendedFileName: recommendedFileName,
				MimeType:            mimeType,
				FileSizeBytes:       uint64(fileSizeBytes),
				QuoteTimestamp:      quoteTimestamp,
				ExpiresAtUnix:       expiresAtUnix,
				Selected:            selected,
				RejectReason:        rejectReason,
			})
		}
		return rows.Err()
	})

	if err != nil {
		if err == sql.ErrNoRows || len(quotes) == 0 {
			return nil, false
		}
		obs.Error("getfilebyhash", "list_quotes_db_failed", map[string]any{
			"job_id": jobID,
			"error":  err.Error(),
		})
		return nil, false
	}

	if len(quotes) == 0 {
		return nil, false
	}
	return quotes, true
}

func (s *getFileByHashJobStore) ListJobs(ctx context.Context) []getfilebyhash.Job {
	var jobs []getfilebyhash.Job

	_ = s.store.Do(ctx, func(db SQLConn) error {
		rows, err := db.QueryContext(ctx, `SELECT job_id,seed_hash,demand_id,state,chunk_count,
			completed_chunks,paid_total_sat,output_file_path,part_file_path,error,
			created_at_unix,updated_at_unix FROM proc_getfilebyhash_jobs`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var job getfilebyhash.Job
			var demandID, outputFilePath, partFilePath, errMsg sql.NullString
			var completedChunks, paidTotalSat, createdAt, updatedAt int64
			var chunkCount uint32
			var state string
			if err := rows.Scan(&job.JobID, &job.SeedHash, &demandID, &state, &chunkCount,
				&completedChunks, &paidTotalSat, &outputFilePath, &partFilePath, &errMsg,
				&createdAt, &updatedAt); err != nil {
				continue
			}
			job.DemandID = demandID.String
			job.State = state
			job.ChunkCount = chunkCount
			job.CompletedChunks = uint32(completedChunks)
			job.PaidTotalSat = uint64(paidTotalSat)
			job.OutputFilePath = outputFilePath.String
			job.PartFilePath = partFilePath.String
			job.Error = errMsg.String
			job.CreatedAt = time.Unix(createdAt, 0)
			job.UpdatedAt = time.Unix(updatedAt, 0)
			jobs = append(jobs, job)
		}
		return rows.Err()
	})

	if jobs == nil {
		jobs = []getfilebyhash.Job{}
	}
	return jobs
}