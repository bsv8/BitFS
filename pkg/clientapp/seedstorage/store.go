package seedstorage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func LoadSeedSnapshot(ctx context.Context, store moduleapi.Store, seedHash string) (SeedRecord, bool, error) {
	if store == nil {
		return SeedRecord{}, false, fmt.Errorf("store is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return SeedRecord{}, false, nil
	}
	var out SeedRecord
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		var row SeedRecord
		var seedFilePath string
		if err := rc.QueryRowContext(ctx, `
			SELECT seed_hash, chunk_count, file_size, seed_file_path, recommended_file_name, mime_hint
			  FROM biz_seeds
			 WHERE seed_hash=?`,
			seedHash,
		).Scan(&row.SeedHash, &row.ChunkCount, &row.FileSize, &seedFilePath, &row.RecommendedFileName, &row.MimeHint); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		row.SeedHash = seedHash
		row.RecommendedFileName = sanitizeRecommendedFileName(row.RecommendedFileName)
		row.MimeHint = sanitizeMIMEHint(row.MimeHint)
		if err := rc.QueryRowContext(ctx, `
			SELECT floor_unit_price_sat_per_64k, resale_discount_bps, pricing_source, updated_at_unix
			  FROM biz_seed_pricing_policy
			 WHERE seed_hash=?`,
			seedHash,
		).Scan(&row.FloorPriceSatPer64K, &row.ResaleDiscountBPS, &row.PricingSource, &row.PriceUpdatedAtUnix); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if strings.TrimSpace(seedFilePath) != "" {
			row.SeedFilePath = filepath.Clean(seedFilePath)
			if seedBytes, err := os.ReadFile(row.SeedFilePath); err == nil {
				if meta, err := parseSeedV1(seedBytes); err == nil && meta.ChunkCount == row.ChunkCount {
					row.ChunkHashes = append([]string(nil), meta.ChunkHashes...)
				}
			}
		}
		out = row
		return nil
	})
	if err != nil {
		return SeedRecord{}, false, err
	}
	if strings.TrimSpace(out.SeedHash) == "" {
		return SeedRecord{}, false, nil
	}
	return out, true, nil
}

func (s *Service) ensureSeedDir() (string, error) {
	if s == nil || s.host == nil {
		return "", fmt.Errorf("seed storage host is required")
	}
	return SeedDirPath(s.host.ConfigPath(), s.host.NodePubkeyHex())
}

func (s *Service) EnsureSeedForFile(ctx context.Context, input EnsureSeedInput) (SeedRecord, error) {
	if s == nil || s.host == nil {
		return SeedRecord{}, fmt.Errorf("seed storage host is required")
	}
	store := s.host.Store()
	if store == nil {
		return SeedRecord{}, fmt.Errorf("store is required")
	}
	filePath := strings.TrimSpace(input.FilePath)
	if filePath == "" {
		return SeedRecord{}, fmt.Errorf("file path is required")
	}
	beforeStat, err := os.Stat(filePath)
	if err != nil {
		return SeedRecord{}, err
	}
	if !beforeStat.Mode().IsRegular() {
		return SeedRecord{}, fmt.Errorf("file is not regular")
	}
	seedBytes, seedHash, chunkCount, err := buildSeedV1(filePath)
	if err != nil {
		return SeedRecord{}, err
	}
	afterStat, err := os.Stat(filePath)
	if err != nil {
		return SeedRecord{}, err
	}
	if beforeStat.Size() != afterStat.Size() || !beforeStat.ModTime().Equal(afterStat.ModTime()) {
		return SeedRecord{}, moduleapi.NewError("FILE_NOT_STABLE", "file changed while building seed")
	}
	seedDir, err := s.ensureSeedDir()
	if err != nil {
		return SeedRecord{}, err
	}
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		return SeedRecord{}, err
	}
	seedPath := filepath.Join(seedDir, seedHash+".bse")
	if err := writeIfChanged(seedPath, seedBytes); err != nil {
		return SeedRecord{}, err
	}
	record := SeedRecord{
		SeedHash:            seedHash,
		SeedFilePath:        seedPath,
		ChunkCount:          chunkCount,
		FileSize:            uint64(afterStat.Size()),
		RecommendedFileName: sanitizeRecommendedFileName(input.RecommendedFileName),
		MimeHint:            sanitizeMIMEHint(input.MimeHint),
	}
	now := time.Now().Unix()
	floorUnit := s.host.SellerFloorPriceSatPer64K()
	discountBPS := s.host.SellerResaleDiscountBPS()
	if err := store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		if err := upsertBizSeedTx(ctx, tx, record); err != nil {
			return err
		}
		if err := replaceSeedChunkSupplyTx(ctx, tx, seedHash, contiguousChunkIndexes(chunkCount)); err != nil {
			return err
		}
		return upsertSeedPricingPolicyTx(ctx, tx, seedHash, floorUnit, discountBPS, "system", now)
	}); err != nil {
		return SeedRecord{}, err
	}
	return record, nil
}

func (s *Service) SaveSeed(ctx context.Context, input moduleapi.SeedSaveInput) (SeedRecord, error) {
	if s == nil || s.host == nil {
		return SeedRecord{}, fmt.Errorf("seed storage host is required")
	}
	store := s.host.Store()
	if store == nil {
		return SeedRecord{}, fmt.Errorf("store is required")
	}
	seedBytes := append([]byte(nil), input.SeedBytes...)
	if len(seedBytes) == 0 {
		return SeedRecord{}, fmt.Errorf("seed bytes are required")
	}
	meta, err := parseSeedV1(seedBytes)
	if err != nil {
		return SeedRecord{}, err
	}
	if seedHash := normalizeSeedHashHex(input.SeedHash); seedHash != "" && seedHash != meta.SeedHashHex {
		return SeedRecord{}, fmt.Errorf("seed hash mismatch")
	}
	if input.ChunkCount > 0 && input.ChunkCount != meta.ChunkCount {
		return SeedRecord{}, fmt.Errorf("seed chunk count mismatch")
	}
	if input.FileSize > 0 && input.FileSize != meta.FileSize {
		return SeedRecord{}, fmt.Errorf("seed file size mismatch")
	}
	seedDir, err := s.ensureSeedDir()
	if err != nil {
		return SeedRecord{}, err
	}
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		return SeedRecord{}, err
	}
	seedHash := meta.SeedHashHex
	seedPath := filepath.Join(seedDir, seedHash+".bse")
	if err := writeIfChanged(seedPath, seedBytes); err != nil {
		return SeedRecord{}, err
	}
	record := SeedRecord{
		SeedHash:            seedHash,
		SeedFilePath:        seedPath,
		ChunkCount:          meta.ChunkCount,
		FileSize:            meta.FileSize,
		ChunkHashes:         append([]string(nil), meta.ChunkHashes...),
		RecommendedFileName: sanitizeRecommendedFileName(input.RecommendedFileName),
		MimeHint:            sanitizeMIMEHint(input.MimeHint),
	}
	now := time.Now().Unix()
	floorUnit := s.host.SellerFloorPriceSatPer64K()
	discountBPS := s.host.SellerResaleDiscountBPS()
	if err := store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		if err := upsertBizSeedTx(ctx, tx, record); err != nil {
			return err
		}
		if err := replaceSeedChunkSupplyTx(ctx, tx, seedHash, contiguousChunkIndexes(record.ChunkCount)); err != nil {
			return err
		}
		return upsertSeedPricingPolicyTx(ctx, tx, seedHash, floorUnit, discountBPS, "system", now)
	}); err != nil {
		return SeedRecord{}, err
	}
	return record, nil
}

func (s *Service) CleanupOrphanSeeds(ctx context.Context) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("seed storage host is required")
	}
	store := s.host.Store()
	if store == nil {
		return fmt.Errorf("store is required")
	}
	return store.WriteTx(ctx, func(tx moduleapi.WriteTx) error {
		return cleanupOrphanSeedsTx(ctx, tx)
	})
}

func (s *Service) LoadSeedSnapshot(ctx context.Context, seedHash string) (SeedRecord, bool, error) {
	if s == nil || s.host == nil {
		return SeedRecord{}, false, fmt.Errorf("seed storage host is required")
	}
	return LoadSeedSnapshot(ctx, s.host.Store(), seedHash)
}

func upsertBizSeedTx(ctx context.Context, tx moduleapi.WriteTx, record SeedRecord) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	record.SeedHash = normalizeSeedHashHex(record.SeedHash)
	if record.SeedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	record.RecommendedFileName = sanitizeRecommendedFileName(record.RecommendedFileName)
	record.MimeHint = sanitizeMIMEHint(record.MimeHint)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO biz_seeds(seed_hash, chunk_count, file_size, seed_file_path, recommended_file_name, mime_hint)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(seed_hash) DO UPDATE SET
			chunk_count=excluded.chunk_count,
			file_size=excluded.file_size,
			seed_file_path=excluded.seed_file_path,
			recommended_file_name=excluded.recommended_file_name,
			mime_hint=excluded.mime_hint`,
		record.SeedHash,
		int64(record.ChunkCount),
		int64(record.FileSize),
		record.SeedFilePath,
		record.RecommendedFileName,
		record.MimeHint,
	)
	return err
}

func upsertSeedPricingPolicyTx(ctx context.Context, tx moduleapi.WriteTx, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source != "user" && source != "system" {
		source = "system"
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO biz_seed_pricing_policy(seed_hash, floor_unit_price_sat_per_64k, resale_discount_bps, pricing_source, updated_at_unix)
		VALUES(?,?,?,?,?)
		ON CONFLICT(seed_hash) DO UPDATE SET
			floor_unit_price_sat_per_64k=excluded.floor_unit_price_sat_per_64k,
			resale_discount_bps=excluded.resale_discount_bps,
			pricing_source=CASE
				WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.pricing_source
				ELSE excluded.pricing_source
			END,
			updated_at_unix=excluded.updated_at_unix`,
		seedHash,
		int64(floorUnit),
		int64(discountBPS),
		source,
		updatedAtUnix,
	)
	return err
}

func replaceSeedChunkSupplyTx(ctx context.Context, tx moduleapi.WriteTx, seedHash string, indexes []uint32) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	indexes = normalizeChunkIndexes(indexes)
	if _, err := tx.ExecContext(ctx, `DELETE FROM biz_seed_chunk_supply WHERE seed_hash=?`, seedHash); err != nil {
		return err
	}
	for _, idx := range indexes {
		if _, err := tx.ExecContext(ctx, `INSERT INTO biz_seed_chunk_supply(seed_hash, chunk_index) VALUES(?,?)`, seedHash, int64(idx)); err != nil {
			return err
		}
	}
	return nil
}

func normalizeChunkIndexes(in []uint32) []uint32 {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(in))
	out := make([]uint32, 0, len(in))
	for _, idx := range in {
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		out = append(out, idx)
	}
	return out
}
