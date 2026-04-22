package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

// 设计说明：
// - 这里的种子池只保存“是否存在 + 基础元信息”；
// - 价格不落缓存，主链路只在真正出价时实时计算；
// - 读路径直接读快照，不回退 DB。

type seedCatalogSnapshot struct {
	seeds        map[string]sellerSeed
	loadedAtUnix int64
}

type seedCatalogStats struct {
	SeedCount      int
	CacheHitCount  uint64
	CacheMissCount uint64
	DBQueryCount   uint64
	Stale          bool
	LoadedAtUnix   int64
}

func newSellerCatalog() *sellerCatalog {
	c := &sellerCatalog{}
	c.snapshot.Store(&seedCatalogSnapshot{seeds: map[string]sellerSeed{}})
	return c
}

func cloneSellerSeedMap(in map[string]sellerSeed) map[string]sellerSeed {
	if len(in) == 0 {
		return map[string]sellerSeed{}
	}
	out := make(map[string]sellerSeed, len(in))
	for rawHash, seed := range in {
		seedHash := strings.ToLower(strings.TrimSpace(rawHash))
		if seedHash == "" {
			continue
		}
		seed.SeedHash = seedHash
		seed.SeedPrice = 0
		seed.ChunkPrice = 0
		seed.RecommendedFileName = sanitizeRecommendedFileName(seed.RecommendedFileName)
		seed.MIMEHint = sanitizeMIMEHint(seed.MIMEHint)
		out[seedHash] = seed
	}
	return out
}

func (c *sellerCatalog) currentSnapshot() *seedCatalogSnapshot {
	if c == nil {
		return &seedCatalogSnapshot{seeds: map[string]sellerSeed{}}
	}
	v := c.snapshot.Load()
	if v == nil {
		return &seedCatalogSnapshot{seeds: map[string]sellerSeed{}}
	}
	return v
}

func (c *sellerCatalog) Replace(bizSeeds map[string]sellerSeed) {
	if c == nil {
		return
	}
	c.snapshot.Store(&seedCatalogSnapshot{
		seeds:        cloneSellerSeedMap(bizSeeds),
		loadedAtUnix: time.Now().Unix(),
	})
	c.stale.Store(false)
}

func (c *sellerCatalog) Upsert(seed sellerSeed) {
	if c == nil {
		return
	}
	seedHash := strings.ToLower(strings.TrimSpace(seed.SeedHash))
	if seedHash == "" {
		return
	}
	seed.SeedHash = seedHash
	seed.SeedPrice = 0
	seed.ChunkPrice = 0
	seed.RecommendedFileName = sanitizeRecommendedFileName(seed.RecommendedFileName)
	seed.MIMEHint = sanitizeMIMEHint(seed.MIMEHint)

	prev := c.currentSnapshot()
	next := make(map[string]sellerSeed, len(prev.seeds)+1)
	for k, v := range prev.seeds {
		next[k] = v
	}
	next[seedHash] = seed
	c.snapshot.Store(&seedCatalogSnapshot{seeds: next, loadedAtUnix: prev.loadedAtUnix})
	c.stale.Store(false)
}

func (c *sellerCatalog) Delete(seedHash string) {
	if c == nil {
		return
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return
	}
	prev := c.currentSnapshot()
	if _, ok := prev.seeds[seedHash]; !ok {
		return
	}
	next := make(map[string]sellerSeed, len(prev.seeds)-1)
	for k, v := range prev.seeds {
		if k == seedHash {
			continue
		}
		next[k] = v
	}
	c.snapshot.Store(&seedCatalogSnapshot{seeds: next, loadedAtUnix: prev.loadedAtUnix})
	c.stale.Store(false)
}

func (c *sellerCatalog) MarkStale() {
	if c == nil {
		return
	}
	c.stale.Store(true)
}

func (c *sellerCatalog) IncDBQueries(n uint64) {
	if c == nil || n == 0 {
		return
	}
	c.dbQueries.Add(n)
}

func (c *sellerCatalog) Get(seedHash string) (sellerSeed, bool) {
	if c == nil {
		return sellerSeed{}, false
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		c.cacheMiss.Add(1)
		return sellerSeed{}, false
	}
	snap := c.currentSnapshot()
	seed, ok := snap.seeds[seedHash]
	if ok {
		c.cacheHits.Add(1)
		return seed, true
	}
	c.cacheMiss.Add(1)
	return sellerSeed{}, false
}

func (c *sellerCatalog) Stats() seedCatalogStats {
	if c == nil {
		return seedCatalogStats{}
	}
	snap := c.currentSnapshot()
	return seedCatalogStats{
		SeedCount:      len(snap.seeds),
		CacheHitCount:  c.cacheHits.Load(),
		CacheMissCount: c.cacheMiss.Load(),
		DBQueryCount:   c.dbQueries.Load(),
		Stale:          c.stale.Load(),
		LoadedAtUnix:   snap.loadedAtUnix,
	}
}

func dbLoadSeedCatalogSnapshot(ctx context.Context, store *clientDB) (map[string]sellerSeed, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	type result struct {
		seeds map[string]sellerSeed
	}
	var out result
	err := store.Read(ctx, func(rc moduleapi.ReadConn) error {
		rows, err := rc.QueryContext(ctx, `
			SELECT seed_hash,chunk_count,file_size,recommended_file_name,mime_hint
			  FROM biz_seeds
			 ORDER BY seed_hash ASC`)
		if err != nil {
			return err
		}
		defer rows.Close()

		seeds := map[string]sellerSeed{}
		for rows.Next() {
			var seed sellerSeed
			if err := rows.Scan(&seed.SeedHash, &seed.ChunkCount, &seed.FileSize, &seed.RecommendedFileName, &seed.MIMEHint); err != nil {
				return err
			}
			seed.SeedHash = strings.ToLower(strings.TrimSpace(seed.SeedHash))
			if seed.SeedHash == "" {
				continue
			}
			seed.SeedPrice = 0
			seed.ChunkPrice = 0
			seed.RecommendedFileName = sanitizeRecommendedFileName(seed.RecommendedFileName)
			seed.MIMEHint = sanitizeMIMEHint(seed.MIMEHint)
			seeds[seed.SeedHash] = seed
		}
		out.seeds = seeds
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if out.seeds == nil {
		out.seeds = map[string]sellerSeed{}
	}
	return out.seeds, nil
}

func (m *workspaceManager) LoadSeedCatalogSnapshot(ctx context.Context) (map[string]sellerSeed, error) {
	if m == nil || m.cfg == nil {
		return nil, fmt.Errorf("workspace manager not initialized")
	}
	store := workspaceStore(m)
	if store == nil {
		return nil, fmt.Errorf("workspace manager not initialized")
	}
	if m.catalog != nil {
		m.catalog.IncDBQueries(1)
	}
	seeds, err := dbLoadSeedCatalogSnapshot(ctx, store)
	if err != nil {
		if m.catalog != nil {
			m.catalog.MarkStale()
		}
		return nil, err
	}
	if m.catalog != nil {
		m.catalog.Replace(seeds)
	}
	return seeds, nil
}
