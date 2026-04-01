package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// seed_chunk_supply 只走这一条 DB 线。
// 设计说明：
// - 这里专门放块供给的读写 helper，避免供给逻辑散进 run.go；
// - 所有入口先统一 seed_hash 规范化，再进 SQL；
// - 这张表只表达“哪些 chunk 可用”，不再夹带别的库存语义。

func dbMergeSeedChunkSupply(ctx context.Context, store *clientDB, seedHash string, incoming []uint32, chunkCount uint32) ([]uint32, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBTxValue(ctx, store, func(tx *sql.Tx) ([]uint32, error) {
		existing, err := dbListSeedChunkSupplyTx(tx, seedHash)
		if err != nil {
			return nil, err
		}
		merged := normalizeChunkIndexes(append(existing, incoming...), chunkCount)
		if err := dbReplaceSeedChunkSupplyTx(tx, seedHash, merged); err != nil {
			return nil, err
		}
		return merged, nil
	})
}

func dbReplaceSeedChunkSupply(ctx context.Context, store *clientDB, seedHash string, availableChunkIndexes []uint32) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.Tx(ctx, func(tx *sql.Tx) error {
		return dbReplaceSeedChunkSupplyTx(tx, seedHash, availableChunkIndexes)
	})
}

func dbListSeedChunkSupply(ctx context.Context, store *clientDB, seedHash string) ([]uint32, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) ([]uint32, error) {
		return dbListSeedChunkSupplyQuery(db, seedHash)
	})
}

func dbIsSeedChunkAvailable(ctx context.Context, store *clientDB, seedHash string, chunkIndex uint32) (bool, error) {
	return clientDBValue(ctx, store, func(db *sql.DB) (bool, error) {
		return dbIsSeedChunkAvailableQuery(db, seedHash, chunkIndex)
	})
}

func dbReplaceSeedChunkSupplyTx(tx *sql.Tx, seedHash string, indexes []uint32) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	indexes = normalizeChunkIndexes(indexes, 0)
	if _, err := tx.Exec(`DELETE FROM seed_chunk_supply WHERE seed_hash=?`, seedHash); err != nil {
		return err
	}
	if len(indexes) == 0 {
		return nil
	}
	stmt, err := tx.Prepare(`INSERT INTO seed_chunk_supply(seed_hash,chunk_index) VALUES(?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, idx := range indexes {
		if _, err := stmt.Exec(seedHash, idx); err != nil {
			return err
		}
	}
	return nil
}

func dbListSeedChunkSupplyTx(tx *sql.Tx, seedHash string) ([]uint32, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return nil, fmt.Errorf("seed_hash required")
	}
	return dbListSeedChunkSupplyQuery(tx, seedHash)
}

func dbListSeedChunkSupplyQuery(queryer interface {
	Query(string, ...any) (*sql.Rows, error)
}, seedHash string) ([]uint32, error) {
	rows, err := queryer.Query(`SELECT chunk_index FROM seed_chunk_supply WHERE seed_hash=? ORDER BY chunk_index ASC`, normalizeSeedHashHex(seedHash))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]uint32, 0, 128)
	for rows.Next() {
		var idx uint32
		if err := rows.Scan(&idx); err != nil {
			return nil, err
		}
		out = append(out, idx)
	}
	return out, rows.Err()
}

func dbIsSeedChunkAvailableQuery(queryer interface {
	QueryRow(string, ...any) *sql.Row
}, seedHash string, chunkIndex uint32) (bool, error) {
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return false, fmt.Errorf("seed_hash required")
	}
	var one int
	err := queryer.QueryRow(`SELECT 1 FROM seed_chunk_supply WHERE seed_hash=? AND chunk_index=? LIMIT 1`, seedHash, chunkIndex).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}
