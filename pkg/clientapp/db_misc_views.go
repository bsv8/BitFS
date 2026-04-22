package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseeds"
)

func dbGetSeedChunkCount(ctx context.Context, store *clientDB, seedHash string) (uint32, bool) {
	if store == nil {
		return 0, false
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (uint32, error) {
		row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(normalizeSeedHashHex(seedHash))).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, sql.ErrNoRows
			}
			return 0, err
		}
		return uint32(row.ChunkCount), nil
	})
	if err != nil {
		return 0, false
	}
	return out, true
}

// dbGetSeedChunkCountForPricing 返回种子 chunk_count，用于定价接口。
// 这是 biz_seeds 旧表查询的过渡封装，后续如需迁移到新的文件元数据表，只需修改此函数。
func dbGetSeedChunkCountForPricing(ctx context.Context, store *clientDB, seedHash string) (uint32, error) {
	if store == nil {
		return 0, fmt.Errorf("store is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return 0, fmt.Errorf("seed_hash is required")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (uint32, error) {
		row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
		if err != nil {
			if gen.IsNotFound(err) {
				return 0, sql.ErrNoRows
			}
			return 0, err
		}
		return uint32(row.ChunkCount), nil
	})
}

func dbRecommendedFileNameBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			return "", nil
		}
		if row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx); err == nil {
			if normalized := sanitizeRecommendedFileName(row.RecommendedFileName); normalized != "" {
				return normalized, nil
			}
		}
		fallbackPath, _, err := store.FindLatestWorkspaceFileBySeedHash(ctx, seedHash)
		if err != nil {
			return "", nil
		}
		return sanitizeRecommendedFileName(filepath.Base(strings.TrimSpace(fallbackPath))), nil
	})
	if err != nil {
		return ""
	}
	return out
}

func dbMimeHintBySeedHash(ctx context.Context, store *clientDB, seedHash string) string {
	if store == nil {
		return ""
	}
	out, err := readEntValue(ctx, store, func(root EntReadRoot) (string, error) {
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			return "", nil
		}
		row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
		if err != nil {
			return "", nil
		}
		return sanitizeMIMEHint(row.MimeHint), nil
	})
	if err != nil {
		return ""
	}
	return out
}
