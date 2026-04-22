package seedstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// LoadSeedSnapshot 只走种子业务能力，不直接碰底层 store。
func LoadSeedSnapshot(ctx context.Context, store moduleapi.SeedStore, seedHash string) (SeedRecord, bool, error) {
	if store == nil {
		return SeedRecord{}, false, fmt.Errorf("seed store is nil")
	}
	return store.LoadSeedSnapshot(ctx, seedHash)
}

func (s *Service) seedStore() moduleapi.SeedStore {
	if s == nil || s.host == nil {
		return nil
	}
	return s.host.SeedStore()
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
	store := s.seedStore()
	if store == nil {
		return SeedRecord{}, fmt.Errorf("seed store is required")
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
	if err := store.UpsertSeedRecord(ctx, record); err != nil {
		return SeedRecord{}, err
	}
	if err := store.ReplaceSeedChunkSupply(ctx, seedHash, contiguousChunkIndexes(chunkCount)); err != nil {
		return SeedRecord{}, err
	}
	if err := store.UpsertSeedPricingPolicy(ctx, seedHash, s.host.SellerFloorPriceSatPer64K(), s.host.SellerResaleDiscountBPS(), "system", now); err != nil {
		return SeedRecord{}, err
	}
	return record, nil
}

func (s *Service) SaveSeed(ctx context.Context, input moduleapi.SeedSaveInput) (SeedRecord, error) {
	if s == nil || s.host == nil {
		return SeedRecord{}, fmt.Errorf("seed storage host is required")
	}
	store := s.seedStore()
	if store == nil {
		return SeedRecord{}, fmt.Errorf("seed store is required")
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
	if err := store.UpsertSeedRecord(ctx, record); err != nil {
		return SeedRecord{}, err
	}
	if err := store.ReplaceSeedChunkSupply(ctx, seedHash, contiguousChunkIndexes(record.ChunkCount)); err != nil {
		return SeedRecord{}, err
	}
	if err := store.UpsertSeedPricingPolicy(ctx, seedHash, s.host.SellerFloorPriceSatPer64K(), s.host.SellerResaleDiscountBPS(), "system", now); err != nil {
		return SeedRecord{}, err
	}
	return record, nil
}

func (s *Service) CleanupOrphanSeeds(ctx context.Context) error {
	if s == nil || s.host == nil {
		return fmt.Errorf("seed storage host is required")
	}
	store := s.seedStore()
	if store == nil {
		return fmt.Errorf("seed store is required")
	}
	return store.CleanupOrphanSeeds(ctx)
}

func (s *Service) LoadSeedSnapshot(ctx context.Context, seedHash string) (SeedRecord, bool, error) {
	if s == nil || s.host == nil {
		return SeedRecord{}, false, fmt.Errorf("seed storage host is required")
	}
	return LoadSeedSnapshot(ctx, s.seedStore(), seedHash)
}
