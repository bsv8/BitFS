package seedstorage

import (
	"context"
	"fmt"
	"os"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func cleanupOrphanSeedsTx(ctx context.Context, tx moduleapi.WriteTx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT seed_hash FROM biz_workspace_files`)
	if err != nil {
		return err
	}
	defer rows.Close()

	active := make(map[string]struct{})
	for rows.Next() {
		var seedHash string
		if err := rows.Scan(&seedHash); err != nil {
			return err
		}
		if h := normalizeSeedHashHex(seedHash); h != "" {
			active[h] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	refRows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT seed_hash FROM biz_workspace_files
		UNION
		SELECT DISTINCT seed_hash FROM proc_file_downloads
		UNION
		SELECT DISTINCT seed_hash FROM proc_file_download_chunks`)
	if err != nil {
		return err
	}
	defer refRows.Close()
	for refRows.Next() {
		var seedHash string
		if err := refRows.Scan(&seedHash); err != nil {
			return err
		}
		if h := normalizeSeedHashHex(seedHash); h != "" {
			active[h] = struct{}{}
		}
	}
	if err := refRows.Err(); err != nil {
		return err
	}

	allRows, err := tx.QueryContext(ctx, `SELECT seed_hash, seed_file_path FROM biz_seeds`)
	if err != nil {
		return err
	}
	defer allRows.Close()

	var orphanHashes []string
	var orphanPaths []string
	for allRows.Next() {
		var seedHash string
		var seedFilePath string
		if err := allRows.Scan(&seedHash, &seedFilePath); err != nil {
			return err
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			continue
		}
		if _, ok := active[seedHash]; ok {
			continue
		}
		orphanHashes = append(orphanHashes, seedHash)
		if seedFilePath != "" {
			orphanPaths = append(orphanPaths, seedFilePath)
		}
	}
	if err := allRows.Err(); err != nil {
		return err
	}
	if len(orphanHashes) == 0 {
		return nil
	}
	for _, path := range orphanPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return deleteSeedRowsTx(ctx, tx, orphanHashes)
}

func deleteSeedRowsTx(ctx context.Context, tx moduleapi.WriteTx, seedHashes []string) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHashes = normalizeSeedHashes(seedHashes)
	if len(seedHashes) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(seedHashes))
	args := make([]any, 0, len(seedHashes))
	for _, h := range seedHashes {
		placeholders = append(placeholders, "?")
		args = append(args, h)
	}
	inClause := stringsJoin(placeholders, ",")
	if _, err := tx.ExecContext(ctx, `DELETE FROM biz_seed_pricing_policy WHERE seed_hash IN (`+inClause+`)`, args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM biz_seed_chunk_supply WHERE seed_hash IN (`+inClause+`)`, args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM biz_seeds WHERE seed_hash IN (`+inClause+`)`, args...); err != nil {
		return err
	}
	return nil
}
