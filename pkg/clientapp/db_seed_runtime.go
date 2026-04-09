package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// 设计说明：
// - 卖方报价读取需要同时看 biz_seeds 和 biz_seed_pricing_policy；
// - 这里把“从库里拼成 sellerSeed”收成一个入口，run.go 只拿结果，不拼 SQL。
func dbLoadSellerSeedSnapshot(ctx context.Context, store *clientDB, seedHash string) (sellerSeed, bool, error) {
	if store == nil {
		return sellerSeed{}, false, fmt.Errorf("client db is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return sellerSeed{}, false, nil
	}
	type result struct {
		seed sellerSeed
		ok   bool
	}
	out, err := clientDBValue(ctx, store, func(db *sql.DB) (result, error) {
		var out result
		var unitPrice uint64
		var policyFound bool
		if err := QueryRowContext(ctx, db, 
			`SELECT floor_unit_price_sat_per_64k
			   FROM biz_seed_pricing_policy
			  WHERE seed_hash=?`,
			seedHash,
		).Scan(&unitPrice); err == nil {
			policyFound = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return result{}, err
		}

		var seed sellerSeed
		if err := QueryRowContext(ctx, db, 
			`SELECT seed_hash,chunk_count,file_size,recommended_file_name,mime_hint
			   FROM biz_seeds
			  WHERE seed_hash=?`,
			seedHash,
		).Scan(&seed.SeedHash, &seed.ChunkCount, &seed.FileSize, &seed.RecommendedFileName, &seed.MIMEHint); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return result{}, nil
			}
			return result{}, err
		}
		seed.SeedHash = seedHash
		seed.ChunkPrice = unitPrice
		seed.SeedPrice = unitPrice * uint64(seed.ChunkCount)
		seed.RecommendedFileName = sanitizeRecommendedFileName(seed.RecommendedFileName)
		seed.MIMEHint = sanitizeMIMEHint(seed.MIMEHint)
		if !policyFound {
			seed.ChunkPrice = 0
			seed.SeedPrice = 0
		}
		out.seed = seed
		out.ok = true
		return out, nil
	})
	if err != nil {
		return sellerSeed{}, false, err
	}
	return out.seed, out.ok, nil
}
