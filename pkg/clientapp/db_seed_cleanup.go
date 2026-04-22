package clientapp

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseedchunksupply"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseedpricingpolicy"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseeds"
)

// orphan seed 清理只做协调，不把模块表真相搬回 seedstorage。
// 设计说明：
// - 先从 filestorage 模块侧收集“仍被使用”的 seed hash；
// - 再在 core 事务里删掉没有被引用的 seed 三表记录；
// - seed 对应的文件路径删除放在事务外做，避免把外部文件系统塞进 DB 事务里。
func cleanupOrphanSeeds(ctx context.Context, store *clientDB) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	active, err := store.CollectActiveSeedHashes(ctx)
	if err != nil {
		return err
	}

	var orphanPaths []string
	var orphanHashes []string
	_, err = writeEntValue(ctx, store, func(root EntWriteRoot) (struct{}, error) {
		seeds, err := root.BizSeeds.Query().All(ctx)
		if err != nil {
			return struct{}{}, err
		}
		for _, seed := range seeds {
			seedHash := normalizeSeedHashHex(seed.SeedHash)
			if seedHash == "" {
				continue
			}
			if _, ok := active[seedHash]; ok {
				continue
			}
			orphanHashes = append(orphanHashes, seedHash)
			if path := strings.TrimSpace(seed.SeedFilePath); path != "" {
				orphanPaths = append(orphanPaths, path)
			}
		}
		if len(orphanHashes) == 0 {
			return struct{}{}, nil
		}
		return struct{}{}, seedstorageDeleteSeedRecords(ctx, root, orphanHashes)
	})
	if err != nil {
		return err
	}
	for _, path := range orphanPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func seedstorageDeleteSeedRecords(ctx context.Context, root EntWriteRoot, seedHashes []string) error {
	if root == nil {
		return fmt.Errorf("core root is nil")
	}
	seedHashes = normalizeSeedHashes(seedHashes)
	if len(seedHashes) == 0 {
		return nil
	}
	if _, err := root.BizSeedPricingPolicy.Delete().Where(bizseedpricingpolicy.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := root.BizSeedChunkSupply.Delete().Where(bizseedchunksupply.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	if _, err := root.BizSeeds.Delete().Where(bizseeds.SeedHashIn(seedHashes...)).Exec(ctx); err != nil {
		return err
	}
	return nil
}
