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
	"github.com/bsv8/bitfs-contract/ent/v1/gen"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizseeds"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspacefiles"
	"github.com/bsv8/bitfs-contract/ent/v1/gen/bizworkspaces"
)

// 设计说明：
// - 工作区扫描属于库存收口逻辑，不放回 run.go；
// - 这里直接吃 clientDB，保证生产路径只走统一入口；
// - 扫描过程会同时修正 biz_workspace_files、biz_seeds 和 biz_seed_chunk_supply 的一致性。
func dbScanAndSyncWorkspace(ctx context.Context, store *clientDB, cfg Config) (map[string]sellerSeed, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBEntTxValue(ctx, store, func(tx *gen.Tx) (map[string]sellerSeed, error) {
		now := time.Now().Unix()
		seedsDir := filepath.Join(cfg.Storage.DataDir, "biz_seeds")
		type existingRef struct {
			SeedHash string
			Locked   bool
		}
		existing := map[string]existingRef{}

		rowsExists, err := tx.BizWorkspaceFiles.Query().All(ctx)
		if err != nil {
			return nil, err
		}
		for _, row := range rowsExists {
			key := row.WorkspacePath + "\x00" + row.FilePath
			existing[key] = existingRef{SeedHash: normalizeSeedHashHex(row.SeedHash), Locked: row.SeedLocked != 0}
		}

		bizWorkspaces, err := listEnabledWorkspacePaths(ctx, tx, cfg.Storage.WorkspaceDir)
		if err != nil {
			return nil, err
		}
		catalog := map[string]sellerSeed{}
		seen := map[string]struct{}{}
		for _, workspace := range bizWorkspaces {
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
					if err := dbUpsertWorkspaceFileTx(ctx, tx, workspace, fileRel, ref.SeedHash, 1); err != nil {
						return err
					}
					seedRow, err := tx.BizSeeds.Query().Where(bizseeds.SeedHashEQ(ref.SeedHash)).Only(ctx)
					if err == nil {
						catalog[ref.SeedHash] = sellerSeed{
							SeedHash:            ref.SeedHash,
							ChunkCount:          uint32(seedRow.ChunkCount),
							FileSize:            uint64(seedRow.FileSize),
							RecommendedFileName: sanitizeRecommendedFileName(seedRow.RecommendedFileName),
							MIMEHint:            sanitizeMIMEHint(seedRow.MimeHint),
						}
						return nil
					}
					if !errors.Is(err, sql.ErrNoRows) {
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
				if err := dbUpsertBizSeedTx(ctx, tx, seedHash, chunkCount, uint64(st.Size()), seedPath, recommendedName, mimeHint); err != nil {
					return err
				}
				if err := dbUpsertWorkspaceFileTx(ctx, tx, workspace, fileRel, seedHash, 0); err != nil {
					return err
				}
				if err := dbReplaceSeedChunkSupplyTx(ctx, tx, seedHash, contiguousChunkIndexes(chunkCount)); err != nil {
					return err
				}
				if err := dbUpsertSeedPricingPolicyTx(ctx, tx, seedHash, cfg.Seller.Pricing.FloorPriceSatPer64K, cfg.Seller.Pricing.ResaleDiscountBPS, "system", now); err != nil {
					return err
				}
				catalog[seedHash] = sellerSeed{
					SeedHash:            seedHash,
					ChunkCount:          chunkCount,
					FileSize:            uint64(st.Size()),
					RecommendedFileName: recommendedName,
					MIMEHint:            mimeHint,
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		rows, err := tx.BizWorkspaceFiles.Query().All(ctx)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if _, ok := seen[row.WorkspacePath+"\x00"+row.FilePath]; ok {
				continue
			}
			if _, err := tx.BizWorkspaceFiles.Delete().
				Where(
					bizworkspacefiles.WorkspacePathEQ(row.WorkspacePath),
					bizworkspacefiles.FilePathEQ(row.FilePath),
				).
				Exec(ctx); err != nil {
				return nil, err
			}
		}

		orphanRows, err := tx.BizSeeds.Query().All(ctx)
		if err != nil {
			return nil, err
		}
		activeSeedHashes, err := dbListActiveSeedHashesTx(ctx, tx)
		if err != nil {
			return nil, err
		}
		activeSet := map[string]struct{}{}
		for _, seedHash := range activeSeedHashes {
			activeSet[seedHash] = struct{}{}
		}
		for _, row := range orphanRows {
			if _, ok := activeSet[normalizeSeedHashHex(row.SeedHash)]; ok {
				continue
			}
			_ = os.Remove(row.SeedFilePath)
			if _, err := tx.BizSeeds.Delete().Where(bizseeds.SeedHashEQ(row.SeedHash)).Exec(ctx); err != nil {
				return nil, err
			}
			delete(catalog, normalizeSeedHashHex(row.SeedHash))
		}
		if err := dbCleanupOrphanSeedStateTx(ctx, tx); err != nil {
			return nil, err
		}

		obs.Business("bitcast-client", "workspace_scanned", map[string]any{"seed_count": len(catalog), "workspace_count": len(bizWorkspaces)})
		return catalog, nil
	})
}

func listEnabledWorkspacePaths(ctx context.Context, tx *gen.Tx, fallback string) ([]string, error) {
	if tx == nil {
		return nil, fmt.Errorf("tx is nil")
	}
	var rows []struct {
		WorkspacePath string `json:"workspace_path,omitempty"`
	}
	err := tx.BizWorkspaces.Query().
		Where(bizworkspaces.EnabledEQ(1)).
		Order(bizworkspaces.ByWorkspacePath()).
		Select(bizworkspaces.FieldWorkspacePath).
		Scan(ctx, &rows)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		p, err := normalizeWorkspacePath(row.WorkspacePath)
		if err != nil || p == "" {
			continue
		}
		out = append(out, p)
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
