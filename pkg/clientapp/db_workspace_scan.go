package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// 设计说明：
// - 工作区扫描属于库存收口逻辑，不放回 run.go；
// - 这里直接吃 clientDB，保证生产路径只走统一入口；
// - 扫描过程会同时修正 biz_workspace_files、biz_seeds 和 biz_seed_chunk_supply 的一致性。
func dbScanAndSyncWorkspace(ctx context.Context, store *clientDB, cfg Config) (map[string]sellerSeed, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	catalog := map[string]sellerSeed{}
	err := store.Tx(ctx, func(tx *sql.Tx) error {
		now := time.Now().Unix()
		seedsDir := filepath.Join(cfg.Storage.DataDir, "biz_seeds")
		type existingRef struct {
			SeedHash string
			Locked   bool
		}
		existing := map[string]existingRef{}

		rowsExists, err := QueryContext(ctx, tx, `SELECT workspace_path,file_path,seed_hash,seed_locked FROM biz_workspace_files`)
		if err != nil {
			return err
		}
		defer rowsExists.Close()
		for rowsExists.Next() {
			var workspacePath, filePath, seedHash string
			var locked int64
			if err := rowsExists.Scan(&workspacePath, &filePath, &seedHash, &locked); err != nil {
				return err
			}
			key := workspacePath + "\x00" + filePath
			existing[key] = existingRef{SeedHash: normalizeSeedHashHex(seedHash), Locked: locked != 0}
		}
		if err := rowsExists.Err(); err != nil {
			return err
		}

		biz_workspaces, err := listEnabledWorkspacePaths(ctx, tx, cfg.Storage.WorkspaceDir)
		if err != nil {
			return err
		}
		seen := map[string]struct{}{}
		for _, workspace := range biz_workspaces {
			workspace = filepath.Clean(strings.TrimSpace(workspace))
			err = filepath.WalkDir(workspace, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if d.IsDir() || !d.Type().IsRegular() {
					return nil
				}
				abs, err := filepath.Abs(path)
				if err != nil {
					return err
				}
				abs = filepath.Clean(abs)
				rel, err := filepath.Rel(workspace, abs)
				if err != nil {
					return err
				}
				fileRel := filepath.ToSlash(filepath.Clean(rel))
				key := workspace + "\x00" + fileRel
				seen[key] = struct{}{}
				st, err := os.Stat(abs)
				if err != nil {
					return err
				}
				if ref, ok := existing[key]; ok && ref.Locked && ref.SeedHash != "" {
					if _, err := ExecContext(ctx, tx, 
						`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked)
						 VALUES(?,?,?,?)
						 ON CONFLICT(workspace_path,file_path) DO UPDATE SET seed_hash=excluded.seed_hash,seed_locked=excluded.seed_locked`,
						workspace, fileRel, ref.SeedHash, 1,
					); err != nil {
						return err
					}
					var chunkCount uint32
					var recommendedName, mimeHint string
					var unitPrice uint64
					if err := QueryRowContext(ctx, tx, `SELECT chunk_count,recommended_file_name,mime_hint FROM biz_seeds WHERE seed_hash=?`, ref.SeedHash).
						Scan(&chunkCount, &recommendedName, &mimeHint); err == nil {
						policy, err := dbLoadSeedPricingPolicyTx(tx, ref.SeedHash)
						if err != nil && !errors.Is(err, sql.ErrNoRows) {
							return err
						}
						unitPrice = policy.FloorPriceSatPer64K
						catalog[ref.SeedHash] = sellerSeed{
							SeedHash:            ref.SeedHash,
							ChunkCount:          chunkCount,
							ChunkPrice:          unitPrice,
							SeedPrice:           unitPrice * uint64(chunkCount),
							RecommendedFileName: sanitizeRecommendedFileName(recommendedName),
							MIMEHint:            sanitizeMIMEHint(mimeHint),
						}
						return nil
					} else if !errors.Is(err, sql.ErrNoRows) {
						return err
					}
				}
				seedBytes, seedHash, chunkCount, err := buildSeedV1(abs)
				if err != nil {
					return err
				}
				seedPath := filepath.Join(seedsDir, strings.ToLower(seedHash)+".bse")
				if err := writeIfChanged(seedPath, seedBytes); err != nil {
					return err
				}
				recommendedName := sanitizeRecommendedFileName(filepath.Base(abs))
				mimeHint := sanitizeMIMEHint(guessContentType(abs, nil))
				if _, err := ExecContext(ctx, tx, 
					`INSERT INTO biz_seeds(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint)
					 VALUES(?,?,?,?,?,?)
					 ON CONFLICT(seed_hash) DO UPDATE SET
					 chunk_count=excluded.chunk_count,
					 file_size=excluded.file_size,
					 seed_file_path=excluded.seed_file_path,
					 recommended_file_name=excluded.recommended_file_name,
					 mime_hint=excluded.mime_hint`,
					seedHash, chunkCount, st.Size(), seedPath, recommendedName, mimeHint,
				); err != nil {
					return err
				}
				if _, err := ExecContext(ctx, tx,
					`INSERT INTO biz_workspace_files(workspace_path,file_path,seed_hash,seed_locked)
					 VALUES(?,?,?,?)
					 ON CONFLICT(workspace_path,file_path) DO UPDATE SET seed_hash=excluded.seed_hash,seed_locked=excluded.seed_locked`,
					workspace, fileRel, seedHash, 0,
				); err != nil {
					return err
				}
				if err := dbReplaceSeedChunkSupplyTx(ctx, tx, seedHash, contiguousChunkIndexes(chunkCount)); err != nil {
					return err
				}
				if err := dbUpsertSeedPricingPolicyTx(tx, seedHash, cfg.Seller.Pricing.FloorPriceSatPer64K, cfg.Seller.Pricing.ResaleDiscountBPS, "system", now); err != nil {
					return err
				}
				catalog[seedHash] = sellerSeed{
					SeedHash:            seedHash,
					ChunkCount:          chunkCount,
					ChunkPrice:          cfg.Seller.Pricing.FloorPriceSatPer64K,
					SeedPrice:           cfg.Seller.Pricing.FloorPriceSatPer64K * uint64(chunkCount),
					RecommendedFileName: recommendedName,
					MIMEHint:            mimeHint,
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		rows, err := QueryContext(ctx, tx, `SELECT workspace_path,file_path FROM biz_workspace_files`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var workspacePath, filePath string
			if err := rows.Scan(&workspacePath, &filePath); err != nil {
				return err
			}
			if _, ok := seen[workspacePath+"\x00"+filePath]; ok {
				continue
			}
			if _, err := ExecContext(ctx, tx, `DELETE FROM biz_workspace_files WHERE workspace_path=? AND file_path=?`, workspacePath, filePath); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}

		orphanRows, err := QueryContext(ctx, tx, `SELECT seed_hash,seed_file_path FROM biz_seeds WHERE seed_hash NOT IN (SELECT DISTINCT seed_hash FROM biz_workspace_files)`)
		if err != nil {
			return err
		}
		defer orphanRows.Close()
		for orphanRows.Next() {
			var seedHash, seedPath string
			if err := orphanRows.Scan(&seedHash, &seedPath); err != nil {
				return err
			}
			_ = os.Remove(seedPath)
			if _, err := ExecContext(ctx, tx, `DELETE FROM biz_seeds WHERE seed_hash=?`, seedHash); err != nil {
				return err
			}
			delete(catalog, seedHash)
		}
		if err := orphanRows.Err(); err != nil {
			return err
		}

		obs.Business("bitcast-client", "workspace_scanned", map[string]any{"seed_count": len(catalog), "workspace_count": len(biz_workspaces)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return catalog, nil
}

func listEnabledWorkspacePaths(ctx context.Context, queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, fallback string) ([]string, error) {
	if queryer == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := QueryContext(ctx, queryer, `SELECT workspace_path FROM biz_workspaces WHERE enabled=1 ORDER BY workspace_path ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		p = filepath.Clean(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		return out, nil
	}
	fallback = filepath.Clean(strings.TrimSpace(fallback))
	if fallback == "" {
		return nil, fmt.Errorf("no workspace configured")
	}
	return []string{fallback}, nil
}
