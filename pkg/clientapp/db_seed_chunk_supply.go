package clientapp

import (
	"context"
	"fmt"

	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseedchunksupply"
)

// biz_seed_chunk_supply 只走这一条 DB 线。
// 设计说明：
// - 这里专门放块供给的读写 helper，避免供给逻辑散进 run.go；
// - 所有入口先统一 seed_hash 规范化，再进 ent；
// - 这张表只表达“哪些 chunk 可用”，不再夹带别的库存语义。

func dbReplaceSeedChunkSupply(ctx context.Context, store *clientDB, seedHash string, availableChunkIndexes []uint32) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbReplaceSeedChunkSupplyTx(ctx, tx, seedHash, availableChunkIndexes)
	})
}

func dbListSeedChunkSupply(ctx context.Context, store *clientDB, seedHash string) ([]uint32, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) ([]uint32, error) {
		return dbListSeedChunkSupplyRoot(ctx, root, seedHash)
	})
}

func dbIsSeedChunkAvailable(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("client db is nil")
	}
	return readEntValue(ctx, store, func(root EntReadRoot) (bool, error) {
		return dbIsSeedChunkAvailableRoot(ctx, root, seedHash, chunkIndex)
	})
}

func dbReplaceSeedChunkSupplyTx(ctx context.Context, tx EntWriteRoot, seedHash string, indexes []uint32) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	indexes = normalizeChunkIndexes(indexes, 0)
	if _, err := tx.BizSeedChunkSupply.Delete().Where(bizseedchunksupply.SeedHashEQ(seedHash)).Exec(ctx); err != nil {
		return err
	}
	if len(indexes) == 0 {
		return nil
	}
	builders := make([]*gen.BizSeedChunkSupplyCreate, 0, len(indexes))
	for _, idx := range indexes {
		builders = append(builders, tx.BizSeedChunkSupply.Create().SetSeedHash(seedHash).SetChunkIndex(int64(idx)))
	}
	if len(builders) == 0 {
		return nil
	}
	_, err := tx.BizSeedChunkSupply.CreateBulk(builders...).Save(ctx)
	return err
}

func dbListSeedChunkSupplyRoot(ctx context.Context, root EntReadRoot, seedHash string) ([]uint32, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return nil, fmt.Errorf("seed_hash required")
	}
	rows, err := root.BizSeedChunkSupply.Query().
		Where(bizseedchunksupply.SeedHashEQ(seedHash)).
		Order(bizseedchunksupply.ByChunkIndex()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]uint32, 0, len(rows))
	for _, row := range rows {
		out = append(out, uint32(row.ChunkIndex))
	}
	return out, nil
}

func dbIsSeedChunkAvailableRoot(ctx context.Context, root EntReadRoot, seedHash string, chunkIndex uint32) (bool, error) {
	if root == nil {
		return false, fmt.Errorf("root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return false, fmt.Errorf("seed_hash required")
	}
	return root.BizSeedChunkSupply.Query().
		Where(bizseedchunksupply.SeedHashEQ(seedHash), bizseedchunksupply.ChunkIndexEQ(int64(chunkIndex))).
		Exist(ctx)
}
