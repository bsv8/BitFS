package seedstorage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedchunksupply"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedpricingpolicy"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
)

type seedReadRoot interface {
	BizSeedsQuery() *gen.BizSeedsQuery
	BizSeedPricingPolicyQuery() *gen.BizSeedPricingPolicyQuery
}

type seedWriteRoot interface {
	BizSeedsClient() *gen.BizSeedsClient
	BizSeedPricingPolicyClient() *gen.BizSeedPricingPolicyClient
	BizSeedChunkSupplyClient() *gen.BizSeedChunkSupplyClient
	BizWorkspaceFilesClient() *gen.BizWorkspaceFilesClient
	ProcFileDownloadsClient() *gen.ProcFileDownloadsClient
	ProcFileDownloadChunksClient() *gen.ProcFileDownloadChunksClient
}

// 设计说明：
// - 这里放种子箱子的 DB 入口本体；
// - 上层只拿 SeedStore 能力，不直接摸实现细节。
func DBLoadSeedSnapshot(ctx context.Context, root seedReadRoot, seedHash string) (moduleapi.SeedRecord, bool, error) {
	if root == nil {
		return moduleapi.SeedRecord{}, false, fmt.Errorf("seed root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return moduleapi.SeedRecord{}, false, nil
	}
	seed, err := root.BizSeedsQuery().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return moduleapi.SeedRecord{}, false, nil
		}
		return moduleapi.SeedRecord{}, false, err
	}
	record := moduleapi.SeedRecord{
		SeedHash:            seed.SeedHash,
		SeedFilePath:        filepath.Clean(seed.SeedFilePath),
		ChunkCount:          uint32(seed.ChunkCount),
		FileSize:            uint64(seed.FileSize),
		RecommendedFileName: sanitizeRecommendedFileName(seed.RecommendedFileName),
		MimeHint:            sanitizeMIMEHint(seed.MimeHint),
	}
	if policy, err := root.BizSeedPricingPolicyQuery().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx); err == nil {
		record.FloorPriceSatPer64K = uint64(policy.FloorUnitPriceSatPer64k)
		record.ResaleDiscountBPS = uint64(policy.ResaleDiscountBps)
		record.PricingSource = policy.PricingSource
		record.PriceUpdatedAtUnix = policy.UpdatedAtUnix
	} else if !gen.IsNotFound(err) {
		return moduleapi.SeedRecord{}, false, err
	}
	if record.SeedFilePath != "" {
		if raw, err := os.ReadFile(record.SeedFilePath); err == nil {
			if meta, err := parseSeedV1(raw); err == nil && meta.ChunkCount == record.ChunkCount {
				record.ChunkHashes = append([]string(nil), meta.ChunkHashes...)
			}
		}
	}
	return record, true, nil
}

func DBUpsertSeedRecord(ctx context.Context, root seedWriteRoot, record moduleapi.SeedRecord) error {
	if root == nil {
		return fmt.Errorf("seed root is nil")
	}
	record.SeedHash = normalizeSeedHashHex(record.SeedHash)
	if record.SeedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	record.SeedFilePath = filepath.Clean(strings.TrimSpace(record.SeedFilePath))
	record.RecommendedFileName = sanitizeRecommendedFileName(record.RecommendedFileName)
	record.MimeHint = sanitizeMIMEHint(record.MimeHint)
	existing, err := root.BizSeedsClient().Query().Where(bizseeds.SeedHashEQ(record.SeedHash)).Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetChunkCount(int64(record.ChunkCount)).
			SetFileSize(int64(record.FileSize)).
			SetSeedFilePath(record.SeedFilePath).
			SetRecommendedFileName(record.RecommendedFileName).
			SetMimeHint(record.MimeHint).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = root.BizSeedsClient().Create().
		SetSeedHash(record.SeedHash).
		SetChunkCount(int64(record.ChunkCount)).
		SetFileSize(int64(record.FileSize)).
		SetSeedFilePath(record.SeedFilePath).
		SetRecommendedFileName(record.RecommendedFileName).
		SetMimeHint(record.MimeHint).
		Save(ctx)
	return err
}

func DBUpsertSeedPricingPolicy(ctx context.Context, root seedWriteRoot, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if root == nil {
		return fmt.Errorf("seed root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source != "user" && source != "system" {
		source = "system"
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	existing, err := root.BizSeedPricingPolicyClient().Query().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx)
	if err == nil {
		if existing.PricingSource == "user" && source == "system" {
			return nil
		}
		_, err = existing.Update().
			SetFloorUnitPriceSatPer64k(int64(floorUnit)).
			SetResaleDiscountBps(int64(discountBPS)).
			SetPricingSource(source).
			SetUpdatedAtUnix(updatedAtUnix).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = root.BizSeedPricingPolicyClient().Create().
		SetSeedHash(seedHash).
		SetFloorUnitPriceSatPer64k(int64(floorUnit)).
		SetResaleDiscountBps(int64(discountBPS)).
		SetPricingSource(source).
		SetUpdatedAtUnix(updatedAtUnix).
		Save(ctx)
	return err
}

func DBReplaceSeedChunkSupply(ctx context.Context, root seedWriteRoot, seedHash string, indexes []uint32) error {
	if root == nil {
		return fmt.Errorf("seed root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	indexes = normalizeUint32List(indexes)
	if _, err := root.BizSeedChunkSupplyClient().Delete().Where(bizseedchunksupply.SeedHashEQ(seedHash)).Exec(ctx); err != nil {
		return err
	}
	if len(indexes) == 0 {
		return nil
	}
	builders := make([]*gen.BizSeedChunkSupplyCreate, 0, len(indexes))
	for _, idx := range indexes {
		builders = append(builders, root.BizSeedChunkSupplyClient().Create().
			SetSeedHash(seedHash).
			SetChunkIndex(int64(idx)))
	}
	_, err := root.BizSeedChunkSupplyClient().CreateBulk(builders...).Save(ctx)
	return err
}

func DBDeleteSeedRecords(ctx context.Context, root seedWriteRoot, seedHashes []string) error {
	if root == nil {
		return fmt.Errorf("seed root is nil")
	}
	seedHashes = normalizeSeedHashList(seedHashes)
	if len(seedHashes) == 0 {
		return nil
	}
	if _, err := root.BizSeedPricingPolicyClient().Delete().Where(bizseedpricingpolicy.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := root.BizSeedChunkSupplyClient().Delete().Where(bizseedchunksupply.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := root.BizSeedsClient().Delete().Where(bizseeds.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	return nil
}

func DBCleanupOrphanSeeds(ctx context.Context, root seedWriteRoot) error {
	if root == nil {
		return fmt.Errorf("seed root is nil")
	}
	active := make(map[string]struct{})
	rows, err := root.BizWorkspaceFilesClient().Query().All(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if h := normalizeSeedHashHex(row.SeedHash); h != "" {
			active[h] = struct{}{}
		}
	}
	rows2, err := root.ProcFileDownloadsClient().Query().All(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows2 {
		if h := normalizeSeedHashHex(row.SeedHash); h != "" {
			active[h] = struct{}{}
		}
	}
	rows3, err := root.ProcFileDownloadChunksClient().Query().All(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows3 {
		if h := normalizeSeedHashHex(row.SeedHash); h != "" {
			active[h] = struct{}{}
		}
	}
	seeds, err := root.BizSeedsClient().Query().Order(bizseeds.BySeedHash()).All(ctx)
	if err != nil {
		return err
	}
	hashes := make([]string, 0, len(seeds))
	paths := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seedHash := normalizeSeedHashHex(seed.SeedHash)
		if seedHash == "" {
			continue
		}
		if _, ok := active[seedHash]; ok {
			continue
		}
		hashes = append(hashes, seedHash)
		if path := strings.TrimSpace(seed.SeedFilePath); path != "" {
			paths = append(paths, path)
		}
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return DBDeleteSeedRecords(ctx, root, hashes)
}

func normalizeSeedHashList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		seedHash := normalizeSeedHashHex(raw)
		if seedHash == "" {
			continue
		}
		if _, ok := seen[seedHash]; ok {
			continue
		}
		seen[seedHash] = struct{}{}
		out = append(out, seedHash)
	}
	return out
}

func normalizeUint32List(in []uint32) []uint32 {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[uint32]struct{}, len(in))
	out := make([]uint32, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
