package clientapp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpricingautopilotaudit"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpricingautopilotconfig"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizpricingautopilotstate"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseedpricingpolicy"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseeds"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

func entWriteRootFromWriteTx(conn moduleapi.WriteTx) (EntWriteRoot, error) {
	if conn == nil {
		return nil, fmt.Errorf("write tx is nil")
	}
	tx, ok := conn.(*writeTxConn)
	if !ok || tx == nil || tx.tx == nil {
		return nil, fmt.Errorf("ent write tx is required")
	}
	return newEntWriteRoot(tx.tx), nil
}

// 设计说明：
// - 这里只放定价引擎需要的最小 DB 能力；
// - 状态和审计都落到现有业务库，避免只停留在进程内存。
func dbListPricingSeedHashes(ctx context.Context, store *clientDB, limit int) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit is required")
	}
	var out []string
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		rows, err := root.BizSeeds.Query().
			Order(bizseeds.BySeedHash()).
			Limit(limit).
			All(ctx)
		if err != nil {
			return err
		}
		out = make([]string, 0, len(rows))
		for _, row := range rows {
			seedHash := strings.ToLower(strings.TrimSpace(row.SeedHash))
			if seedHash == "" {
				continue
			}
			out = append(out, seedHash)
		}
		if len(out) == 0 {
			return fmt.Errorf("no seed found")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func dbListAllPricingSeedHashes(ctx context.Context, store *clientDB) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	var out []string
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		rows, err := root.BizSeeds.Query().
			Order(bizseeds.BySeedHash()).
			All(ctx)
		if err != nil {
			return err
		}
		out = make([]string, 0, len(rows))
		for _, row := range rows {
			seedHash := strings.ToLower(strings.TrimSpace(row.SeedHash))
			if seedHash == "" {
				continue
			}
			out = append(out, seedHash)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

const pricingAutopilotConfigKey = "live_base_price_sat_per_64k"

type pricingAutopilotConfigRow struct {
	ConfigKey     string
	PayloadJSON   string
	UpdatedAtUnix int64
}

type pricingAutopilotStateRow struct {
	SeedHash      string
	PayloadJSON   string
	UpdatedAtUnix int64
}

type pricingAutopilotAuditRow struct {
	ID           int64
	SeedHash     string
	PayloadJSON  string
	TickedAtUnix int64
}

func currentLiveSellerPricing(ctx context.Context, store *clientDB, cfg Config) (LiveSellerPricing, error) {
	return currentSeedLiveSellerPricing(ctx, store, cfg, "")
}

// 设计说明：
// - 真实报价优先吃 seed 自己的自动结果；
// - 没有 seed 状态时，再回退到配置里的全局 base。
func currentSeedLiveSellerPricing(ctx context.Context, store *clientDB, cfg Config, seedHash string) (LiveSellerPricing, error) {
	out := LiveSellerPricing{
		BasePriceSatPer64K:  cfg.Seller.Pricing.LiveBasePriceSatPer64K,
		FloorPriceSatPer64K: cfg.Seller.Pricing.LiveFloorPriceSatPer64K,
		DecayPerMinuteBPS:   cfg.Seller.Pricing.LiveDecayPerMinuteBPS,
	}
	if store == nil {
		return out, nil
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash != "" {
		state, ok, err := dbLoadPricingAutopilotState(ctx, store, seedHash)
		if err != nil {
			return LiveSellerPricing{}, err
		}
		if ok && state.EffectivePriceSatPer64K > 0 {
			out.BasePriceSatPer64K = state.EffectivePriceSatPer64K
			return out, nil
		}
	}
	row, ok, err := dbLoadPricingAutopilotConfig(ctx, store)
	if err != nil {
		return LiveSellerPricing{}, err
	}
	if ok && row.BasePriceSatPer64K > 0 {
		out.BasePriceSatPer64K = row.BasePriceSatPer64K
	}
	return out, nil
}

func dbLoadPricingAutopilotConfig(ctx context.Context, store *clientDB) (PricingConfig, bool, error) {
	if store == nil {
		return PricingConfig{}, false, fmt.Errorf("client db is nil")
	}
	type result struct {
		cfg PricingConfig
		ok  bool
	}
	var out result
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		cfg, ok, err := dbLoadPricingAutopilotConfigOnReadRoot(ctx, root)
		if err != nil {
			return err
		}
		out = result{cfg: cfg, ok: ok}
		return nil
	})
	if err != nil {
		return PricingConfig{}, false, err
	}
	return out.cfg, out.ok, nil
}

func dbLoadPricingAutopilotConfigOnReadRoot(ctx context.Context, root EntReadRoot) (PricingConfig, bool, error) {
	if root == nil {
		return PricingConfig{}, false, fmt.Errorf("root is nil")
	}
	row, err := root.BizPricingAutopilotConfig.Query().
		Where(bizpricingautopilotconfig.ConfigKeyEQ(pricingAutopilotConfigKey)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return PricingConfig{}, false, nil
		}
		return PricingConfig{}, false, err
	}
	var cfg PricingConfig
	if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &cfg); err != nil {
		return PricingConfig{}, false, err
	}
	if cfg.BasePriceSatPer64K == 0 {
		cfg.BasePriceSatPer64K = 1000
	}
	return cfg, true, nil
}

func dbLoadPricingAutopilotConfigOnWriteRoot(ctx context.Context, root EntWriteRoot) (PricingConfig, bool, error) {
	if root == nil {
		return PricingConfig{}, false, fmt.Errorf("root is nil")
	}
	row, err := root.BizPricingAutopilotConfig.Query().
		Where(bizpricingautopilotconfig.ConfigKeyEQ(pricingAutopilotConfigKey)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return PricingConfig{}, false, nil
		}
		return PricingConfig{}, false, err
	}
	var cfg PricingConfig
	if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &cfg); err != nil {
		return PricingConfig{}, false, err
	}
	if cfg.BasePriceSatPer64K == 0 {
		cfg.BasePriceSatPer64K = 1000
	}
	return cfg, true, nil
}

func dbLoadPricingAutopilotConfigOnConn(ctx context.Context, conn moduleapi.WriteTx) (PricingConfig, bool, error) {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return PricingConfig{}, false, err
	}
	return dbLoadPricingAutopilotConfigOnWriteRoot(ctx, root)
}

func dbUpsertPricingAutopilotConfig(ctx context.Context, store *clientDB, cfg PricingConfig, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		return dbUpsertPricingAutopilotConfigOnRoot(ctx, root, cfg, updatedAtUnix, string(payload))
	})
}

func dbUpsertPricingAutopilotConfigOnConn(ctx context.Context, conn moduleapi.WriteTx, cfg PricingConfig, updatedAtUnix int64) error {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return dbUpsertPricingAutopilotConfigOnRoot(ctx, root, cfg, updatedAtUnix, string(payload))
}

// dbUpsertPricingAutopilotConfigOnRoot 只负责把全局 base 写进目标 ent 事务。
// 设计说明：
// - set_base 需要先把配置和状态放进同一个事务入口里；
// - 所以这里拆出 root 版本，给事务闭包直接复用；
// - 业务层不要自己拼 SQL，只走这一个收口。
func dbUpsertPricingAutopilotConfigOnRoot(ctx context.Context, root EntWriteRoot, cfg PricingConfig, updatedAtUnix int64, payloadJSON string) error {
	if root == nil {
		return fmt.Errorf("root is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	existing, err := root.BizPricingAutopilotConfig.Query().
		Where(bizpricingautopilotconfig.ConfigKeyEQ(pricingAutopilotConfigKey)).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetPayloadJSON(payloadJSON).
			SetUpdatedAtUnix(updatedAtUnix).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = root.BizPricingAutopilotConfig.Create().
		SetConfigKey(pricingAutopilotConfigKey).
		SetPayloadJSON(payloadJSON).
		SetUpdatedAtUnix(updatedAtUnix).
		Save(ctx)
	return err
}

func dbLoadPricingAutopilotState(ctx context.Context, store *clientDB, seedHash string) (PricingState, bool, error) {
	if store == nil {
		return PricingState{}, false, fmt.Errorf("client db is nil")
	}
	type result struct {
		state PricingState
		ok    bool
	}
	var out result
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		state, ok, err := dbLoadPricingAutopilotStateOnReadRoot(ctx, root, seedHash)
		if err != nil {
			return err
		}
		out = result{state: state, ok: ok}
		return nil
	})
	if err != nil {
		return PricingState{}, false, err
	}
	return out.state, out.ok, nil
}

func dbLoadPricingAutopilotStateOnConn(ctx context.Context, conn moduleapi.WriteTx, seedHash string) (PricingState, bool, error) {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return PricingState{}, false, err
	}
	return dbLoadPricingAutopilotStateOnWriteRoot(ctx, root, seedHash)
}

func loadPricingStateForSeedOnConn(ctx context.Context, conn moduleapi.WriteTx, seedHash string, base uint64, nowUnix int64) (PricingState, bool, error) {
	state, ok, err := dbLoadPricingAutopilotStateOnConn(ctx, conn, seedHash)
	if err != nil {
		return PricingState{}, false, err
	}
	if ok {
		return pricingNormalizeStateForBase(state, base, nowUnix), true, nil
	}
	seed, ok, err := dbLoadSellerSeedSnapshotOnConn(ctx, conn, seedHash)
	if err != nil {
		return PricingState{}, false, err
	}
	if !ok {
		return PricingState{}, false, nil
	}
	state = pricingStateSeedSnapshot(seed, base)
	return pricingNormalizeStateForBase(state, base, nowUnix), true, nil
}

func dbLoadPricingAutopilotStateOnReadRoot(ctx context.Context, root EntReadRoot, seedHash string) (PricingState, bool, error) {
	if root == nil {
		return PricingState{}, false, fmt.Errorf("root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return PricingState{}, false, nil
	}
	row, err := root.BizPricingAutopilotState.Query().
		Where(bizpricingautopilotstate.SeedHashEQ(seedHash)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return PricingState{}, false, nil
		}
		return PricingState{}, false, err
	}
	var state PricingState
	if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &state); err != nil {
		return PricingState{}, false, err
	}
	return state, true, nil
}

func dbLoadPricingAutopilotStateOnWriteRoot(ctx context.Context, root EntWriteRoot, seedHash string) (PricingState, bool, error) {
	if root == nil {
		return PricingState{}, false, fmt.Errorf("root is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return PricingState{}, false, nil
	}
	row, err := root.BizPricingAutopilotState.Query().
		Where(bizpricingautopilotstate.SeedHashEQ(seedHash)).
		Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return PricingState{}, false, nil
		}
		return PricingState{}, false, err
	}
	var state PricingState
	if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &state); err != nil {
		return PricingState{}, false, err
	}
	return state, true, nil
}

func dbListAllPricingSeedHashesOnReadRoot(ctx context.Context, root EntReadRoot) ([]string, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	rows, err := root.BizSeeds.Query().
		Order(bizseeds.BySeedHash()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		seedHash := strings.ToLower(strings.TrimSpace(row.SeedHash))
		if seedHash == "" {
			continue
		}
		out = append(out, seedHash)
	}
	return out, nil
}

func dbListAllPricingSeedHashesOnWriteRoot(ctx context.Context, root EntWriteRoot) ([]string, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	rows, err := root.BizSeeds.Query().
		Order(bizseeds.BySeedHash()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		seedHash := strings.ToLower(strings.TrimSpace(row.SeedHash))
		if seedHash == "" {
			continue
		}
		out = append(out, seedHash)
	}
	return out, nil
}

func dbLoadSellerSeedSnapshotOnReadRoot(ctx context.Context, root EntReadRoot, seedHash string) (sellerSeed, bool, error) {
	if root == nil {
		return sellerSeed{}, false, fmt.Errorf("root is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return sellerSeed{}, false, nil
	}
	var unitPrice uint64
	var policyFound bool
	if policy, err := root.BizSeedPricingPolicy.Query().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx); err == nil {
		unitPrice = uint64(policy.FloorUnitPriceSatPer64k)
		policyFound = true
	} else if !gen.IsNotFound(err) {
		return sellerSeed{}, false, err
	}
	row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return sellerSeed{}, false, nil
		}
		return sellerSeed{}, false, err
	}
	seed := sellerSeed{
		SeedHash:            seedHash,
		ChunkCount:          uint32(row.ChunkCount),
		FileSize:            uint64(row.FileSize),
		ChunkPrice:          unitPrice,
		SeedPrice:           unitPrice * uint64(row.ChunkCount),
		RecommendedFileName: sanitizeRecommendedFileName(row.RecommendedFileName),
		MIMEHint:            sanitizeMIMEHint(row.MimeHint),
	}
	if !policyFound {
		seed.ChunkPrice = 0
		seed.SeedPrice = 0
	}
	return seed, true, nil
}

func dbLoadSellerSeedSnapshotOnWriteRoot(ctx context.Context, root EntWriteRoot, seedHash string) (sellerSeed, bool, error) {
	if root == nil {
		return sellerSeed{}, false, fmt.Errorf("root is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return sellerSeed{}, false, nil
	}
	var unitPrice uint64
	var policyFound bool
	if policy, err := root.BizSeedPricingPolicy.Query().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx); err == nil {
		unitPrice = uint64(policy.FloorUnitPriceSatPer64k)
		policyFound = true
	} else if !gen.IsNotFound(err) {
		return sellerSeed{}, false, err
	}
	row, err := root.BizSeeds.Query().Where(bizseeds.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return sellerSeed{}, false, nil
		}
		return sellerSeed{}, false, err
	}
	seed := sellerSeed{
		SeedHash:            seedHash,
		ChunkCount:          uint32(row.ChunkCount),
		FileSize:            uint64(row.FileSize),
		ChunkPrice:          unitPrice,
		SeedPrice:           unitPrice * uint64(row.ChunkCount),
		RecommendedFileName: sanitizeRecommendedFileName(row.RecommendedFileName),
		MIMEHint:            sanitizeMIMEHint(row.MimeHint),
	}
	if !policyFound {
		seed.ChunkPrice = 0
		seed.SeedPrice = 0
	}
	return seed, true, nil
}

func dbListPricingAutopilotStates(ctx context.Context, store *clientDB, limit int) ([]PricingState, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	var out []PricingState
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		states, err := dbListPricingAutopilotStatesOnRoot(ctx, root, limit)
		if err != nil {
			return err
		}
		out = states
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func dbListPricingAutopilotStatesOnRoot(ctx context.Context, root EntReadRoot, limit int) ([]PricingState, error) {
	if root == nil {
		return nil, fmt.Errorf("root is nil")
	}
	rows, err := root.BizPricingAutopilotState.Query().
		Order(bizpricingautopilotstate.BySeedHash()).
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PricingState, 0, 8)
	for _, row := range rows {
		var state PricingState
		if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &state); err != nil {
			return nil, err
		}
		if strings.TrimSpace(state.SeedHash) == "" {
			continue
		}
		out = append(out, state)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func dbUpsertPricingAutopilotState(ctx context.Context, store *clientDB, state PricingState, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if strings.TrimSpace(state.SeedHash) == "" {
		return fmt.Errorf("seed_hash required")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		return dbUpsertPricingAutopilotStateOnRoot(ctx, root, state, updatedAtUnix, string(payload))
	})
}

func dbUpsertPricingAutopilotStateOnConn(ctx context.Context, conn moduleapi.WriteTx, state PricingState, updatedAtUnix int64) error {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return dbUpsertPricingAutopilotStateOnRoot(ctx, root, state, updatedAtUnix, string(payload))
}

// dbUpsertPricingAutopilotStateOnRoot 负责把单个种子的自动定价状态写进指定 ent 事务。
// 设计说明：
// - set_base 需要在事务里批量回写状态，所以这里提供 conn 版本；
// - 其它路径仍然可以直接走 store 版本，调用面不扩散。
func dbUpsertPricingAutopilotStateOnRoot(ctx context.Context, root EntWriteRoot, state PricingState, updatedAtUnix int64, payloadJSON string) error {
	if root == nil {
		return fmt.Errorf("root is nil")
	}
	if strings.TrimSpace(state.SeedHash) == "" {
		return fmt.Errorf("seed_hash required")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	existing, err := root.BizPricingAutopilotState.Query().
		Where(bizpricingautopilotstate.SeedHashEQ(state.SeedHash)).
		Only(ctx)
	if err == nil {
		_, err = existing.Update().
			SetPayloadJSON(payloadJSON).
			SetUpdatedAtUnix(updatedAtUnix).
			Save(ctx)
		return err
	}
	if !gen.IsNotFound(err) {
		return err
	}
	_, err = root.BizPricingAutopilotState.Create().
		SetSeedHash(state.SeedHash).
		SetPayloadJSON(payloadJSON).
		SetUpdatedAtUnix(updatedAtUnix).
		Save(ctx)
	return err
}

func dbAppendPricingAutopilotAuditOnRoot(ctx context.Context, root EntWriteRoot, item PricingAuditItem) error {
	if root == nil {
		return fmt.Errorf("root is nil")
	}
	if strings.TrimSpace(item.SeedHash) == "" {
		return fmt.Errorf("seed_hash required")
	}
	if strings.TrimSpace(item.ReasonCode) == "" {
		return fmt.Errorf("reason_code required")
	}
	if item.TickedAtUnix <= 0 {
		item.TickedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = root.BizPricingAutopilotAudit.Create().
		SetSeedHash(item.SeedHash).
		SetPayloadJSON(string(payload)).
		SetTickedAtUnix(item.TickedAtUnix).
		Save(ctx)
	return err
}

func dbAppendPricingAutopilotAuditOnConn(ctx context.Context, conn moduleapi.WriteTx, item PricingAuditItem) error {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return err
	}
	return dbAppendPricingAutopilotAuditOnRoot(ctx, root, item)
}

// dbPersistPricingAutopilotBase 以一个事务把全局 base 和相关种子状态一起写回去。
// 设计说明：
// - 这是 set_base 的数据库落点，不给别的流程复用；
// - 如果事务失败，调用方负责把配置文件和内存回滚到旧值；
// - 这里不做"局部成功"处理，避免留下半新半旧的状态。
func dbPersistPricingAutopilotBase(ctx context.Context, store *clientDB, cfg PricingConfig, states []PricingState, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		if err := dbUpsertPricingAutopilotConfigOnRoot(ctx, root, cfg, updatedAtUnix, string(payload)); err != nil {
			return err
		}
		for _, state := range states {
			if strings.TrimSpace(state.SeedHash) == "" {
				continue
			}
			statePayload, err := json.Marshal(state)
			if err != nil {
				return err
			}
			if err := dbUpsertPricingAutopilotStateOnRoot(ctx, root, state, updatedAtUnix, string(statePayload)); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbListPricingAutopilotAudits(ctx context.Context, store *clientDB, seedHash string, limit int) ([]PricingAuditItem, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return []PricingAuditItem{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	var out []PricingAuditItem
	err := store.ReadEnt(ctx, func(root EntReadRoot) error {
		rows, err := root.BizPricingAutopilotAudit.Query().
			Where(bizpricingautopilotaudit.SeedHashEQ(seedHash)).
			Order(
				bizpricingautopilotaudit.ByTickedAtUnix(entsql.OrderDesc()),
				bizpricingautopilotaudit.ByID(entsql.OrderDesc()),
			).
			Limit(limit).
			All(ctx)
		if err != nil {
			return err
		}
		out = make([]PricingAuditItem, 0, len(rows))
		for _, row := range rows {
			var item PricingAuditItem
			if err := json.Unmarshal([]byte(strings.TrimSpace(row.PayloadJSON)), &item); err != nil {
				return err
			}
			if strings.TrimSpace(item.SeedHash) == "" {
				continue
			}
			out = append(out, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func dbListAllPricingSeedHashesOnConn(ctx context.Context, conn moduleapi.WriteTx) ([]string, error) {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return nil, err
	}
	return dbListAllPricingSeedHashesOnWriteRoot(ctx, root)
}

func dbLoadSellerSeedSnapshotOnConn(ctx context.Context, conn moduleapi.WriteTx, seedHash string) (sellerSeed, bool, error) {
	root, err := entWriteRootFromWriteTx(conn)
	if err != nil {
		return sellerSeed{}, false, err
	}
	return dbLoadSellerSeedSnapshotOnWriteRoot(ctx, root, seedHash)
}

func dbPersistPricingAutopilotStateAndAudits(ctx context.Context, store *clientDB, state PricingState, audits []PricingAuditItem, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		if err := dbUpsertPricingAutopilotStateOnRoot(ctx, root, state, updatedAtUnix, string(payload)); err != nil {
			return err
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnRoot(ctx, root, item); err != nil {
				return err
			}
		}
		return nil
	})
}

func dbPersistPricingAutopilotStatesAndAudits(ctx context.Context, store *clientDB, states []PricingState, audits []PricingAuditItem, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	return store.WriteEntTx(ctx, func(root EntWriteRoot) error {
		for _, state := range states {
			if strings.TrimSpace(state.SeedHash) == "" {
				continue
			}
			payload, err := json.Marshal(state)
			if err != nil {
				return err
			}
			if err := dbUpsertPricingAutopilotStateOnRoot(ctx, root, state, updatedAtUnix, string(payload)); err != nil {
				return err
			}
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnRoot(ctx, root, item); err != nil {
				return err
			}
		}
		return nil
	})
}
