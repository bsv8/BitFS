package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

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
	return clientDBValue(ctx, store, func(db SQLConn) ([]string, error) {
		rows, err := QueryContext(ctx, db, `
			SELECT seed_hash
			  FROM biz_seeds
			 ORDER BY seed_hash ASC
			 LIMIT ?`,
			limit,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		out := make([]string, 0, limit)
		for rows.Next() {
			var seedHash string
			if err := rows.Scan(&seedHash); err != nil {
				return nil, err
			}
			seedHash = strings.ToLower(strings.TrimSpace(seedHash))
			if seedHash == "" {
				continue
			}
			out = append(out, seedHash)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no seed found")
		}
		return out, nil
	})
}

func dbListAllPricingSeedHashes(ctx context.Context, store *clientDB) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db SQLConn) ([]string, error) {
		rows, err := QueryContext(ctx, db, `
			SELECT seed_hash
			  FROM biz_seeds
			 ORDER BY seed_hash ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		out := []string{}
		for rows.Next() {
			var seedHash string
			if err := rows.Scan(&seedHash); err != nil {
				return nil, err
			}
			seedHash = strings.ToLower(strings.TrimSpace(seedHash))
			if seedHash == "" {
				continue
			}
			out = append(out, seedHash)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
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
	out, err := clientDBValue(ctx, store, func(db SQLConn) (result, error) {
		cfg, ok, err := dbLoadPricingAutopilotConfigOnConn(ctx, db)
		if err != nil {
			return result{}, err
		}
		return result{cfg: cfg, ok: ok}, nil
	})
	if err != nil {
		return PricingConfig{}, false, err
	}
	return out.cfg, out.ok, nil
}

func dbLoadPricingAutopilotConfigOnConn(ctx context.Context, conn SQLConn) (PricingConfig, bool, error) {
	if conn == nil {
		return PricingConfig{}, false, fmt.Errorf("sql conn is nil")
	}
	var payloadJSON string
	if err := QueryRowContext(ctx, conn, `
		SELECT payload_json
		  FROM biz_pricing_autopilot_config
		 WHERE config_key=?`,
		pricingAutopilotConfigKey,
	).Scan(&payloadJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PricingConfig{}, false, nil
		}
		return PricingConfig{}, false, err
	}
	var cfg PricingConfig
	if err := json.Unmarshal([]byte(strings.TrimSpace(payloadJSON)), &cfg); err != nil {
		return PricingConfig{}, false, err
	}
	if cfg.BasePriceSatPer64K == 0 {
		cfg.BasePriceSatPer64K = 1000
	}
	return cfg, true, nil
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
	_, err = store.ExecContext(ctx, `
		INSERT INTO biz_pricing_autopilot_config(config_key,payload_json,updated_at_unix)
		VALUES(?,?,?)
		ON CONFLICT(config_key) DO UPDATE SET
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		pricingAutopilotConfigKey, string(payload), updatedAtUnix,
	)
	return err
}

// dbUpsertPricingAutopilotConfigOnConn 只负责把全局 base 写进目标 SQL 连接。
// 设计说明：
// - set_base 需要先把配置和状态放进同一个事务入口里；
// - 所以这里拆出 conn 版本，给事务闭包直接复用；
// - 业务层不要自己拼 SQL，只走这一个收口。
func dbUpsertPricingAutopilotConfigOnConn(ctx context.Context, conn SQLConn, cfg PricingConfig, updatedAtUnix int64) error {
	if conn == nil {
		return fmt.Errorf("sql conn is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `
		INSERT INTO biz_pricing_autopilot_config(config_key,payload_json,updated_at_unix)
		VALUES(?,?,?)
		ON CONFLICT(config_key) DO UPDATE SET
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		pricingAutopilotConfigKey, string(payload), updatedAtUnix,
	)
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
	out, err := clientDBValue(ctx, store, func(db SQLConn) (result, error) {
		state, ok, err := dbLoadPricingAutopilotStateOnConn(ctx, db, seedHash)
		if err != nil {
			return result{}, err
		}
		return result{state: state, ok: ok}, nil
	})
	if err != nil {
		return PricingState{}, false, err
	}
	return out.state, out.ok, nil
}

func dbLoadPricingAutopilotStateOnConn(ctx context.Context, conn SQLConn, seedHash string) (PricingState, bool, error) {
	if conn == nil {
		return PricingState{}, false, fmt.Errorf("sql conn is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return PricingState{}, false, nil
	}
	var payloadJSON string
	if err := QueryRowContext(ctx, conn, `
		SELECT payload_json
		  FROM biz_pricing_autopilot_state
		 WHERE seed_hash=?`,
		seedHash,
	).Scan(&payloadJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PricingState{}, false, nil
		}
		return PricingState{}, false, err
	}
	var state PricingState
	if err := json.Unmarshal([]byte(strings.TrimSpace(payloadJSON)), &state); err != nil {
		return PricingState{}, false, err
	}
	return state, true, nil
}

func dbListPricingAutopilotStates(ctx context.Context, store *clientDB, limit int) ([]PricingState, error) {
	if store == nil {
		return nil, fmt.Errorf("client db is nil")
	}
	return clientDBValue(ctx, store, func(db SQLConn) ([]PricingState, error) {
		return dbListPricingAutopilotStatesOnConn(ctx, db, limit)
	})
}

func dbListPricingAutopilotStatesOnConn(ctx context.Context, conn SQLConn, limit int) ([]PricingState, error) {
	if conn == nil {
		return nil, fmt.Errorf("sql conn is nil")
	}
	rows, err := QueryContext(ctx, conn, `
		SELECT payload_json
		  FROM biz_pricing_autopilot_state
		 ORDER BY seed_hash ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]PricingState, 0, 8)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var state PricingState
		if err := json.Unmarshal([]byte(strings.TrimSpace(payload)), &state); err != nil {
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
	return out, rows.Err()
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
	_, err = store.ExecContext(ctx, `
		INSERT INTO biz_pricing_autopilot_state(seed_hash,payload_json,updated_at_unix)
		VALUES(?,?,?)
		ON CONFLICT(seed_hash) DO UPDATE SET
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		state.SeedHash, string(payload), updatedAtUnix,
	)
	return err
}

// dbUpsertPricingAutopilotStateOnConn 负责把单个种子的自动定价状态写进指定 SQL 连接。
// 设计说明：
// - set_base 需要在事务里批量回写状态，所以这里提供 conn 版本；
// - 其它路径仍然可以直接走 store 版本，调用面不扩散。
func dbUpsertPricingAutopilotStateOnConn(ctx context.Context, conn SQLConn, state PricingState, updatedAtUnix int64) error {
	if conn == nil {
		return fmt.Errorf("sql conn is nil")
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
	_, err = conn.ExecContext(ctx, `
		INSERT INTO biz_pricing_autopilot_state(seed_hash,payload_json,updated_at_unix)
		VALUES(?,?,?)
		ON CONFLICT(seed_hash) DO UPDATE SET
			payload_json=excluded.payload_json,
			updated_at_unix=excluded.updated_at_unix`,
		state.SeedHash, string(payload), updatedAtUnix,
	)
	return err
}

func dbAppendPricingAutopilotAuditOnConn(ctx context.Context, conn SQLConn, item PricingAuditItem) error {
	if conn == nil {
		return fmt.Errorf("sql conn is nil")
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
	_, err = conn.ExecContext(ctx, `
		INSERT INTO biz_pricing_autopilot_audit(seed_hash,payload_json,ticked_at_unix)
		VALUES(?,?,?)`,
		item.SeedHash, string(payload), item.TickedAtUnix,
	)
	return err
}

// dbPersistPricingAutopilotBase 以一个事务把全局 base 和相关种子状态一起写回去。
// 设计说明：
// - 这是 set_base 的数据库落点，不给别的流程复用；
// - 如果事务失败，调用方负责把配置文件和内存回滚到旧值；
// - 这里不做“局部成功”处理，避免留下半新半旧的状态。
func dbPersistPricingAutopilotBase(ctx context.Context, store *clientDB, cfg PricingConfig, states []PricingState, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	return store.Tx(ctx, func(conn SQLConn) error {
		if err := dbUpsertPricingAutopilotConfigOnConn(ctx, conn, cfg, updatedAtUnix); err != nil {
			return err
		}
		for _, state := range states {
			if strings.TrimSpace(state.SeedHash) == "" {
				continue
			}
			if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, state, updatedAtUnix); err != nil {
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
	return clientDBValue(ctx, store, func(db SQLConn) ([]PricingAuditItem, error) {
		rows, err := QueryContext(ctx, db, `
			SELECT payload_json
			  FROM biz_pricing_autopilot_audit
			 WHERE seed_hash=?
			 ORDER BY ticked_at_unix DESC, id DESC
			 LIMIT ?`,
			seedHash, limit,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]PricingAuditItem, 0, limit)
		for rows.Next() {
			var payload string
			if err := rows.Scan(&payload); err != nil {
				return nil, err
			}
			var item PricingAuditItem
			if err := json.Unmarshal([]byte(strings.TrimSpace(payload)), &item); err != nil {
				return nil, err
			}
			if strings.TrimSpace(item.SeedHash) == "" {
				continue
			}
			out = append(out, item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func dbListAllPricingSeedHashesOnConn(ctx context.Context, conn SQLConn) ([]string, error) {
	if conn == nil {
		return nil, fmt.Errorf("sql conn is nil")
	}
	rows, err := QueryContext(ctx, conn, `
		SELECT seed_hash
		  FROM biz_seeds
		 ORDER BY seed_hash ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var seedHash string
		if err := rows.Scan(&seedHash); err != nil {
			return nil, err
		}
		seedHash = strings.ToLower(strings.TrimSpace(seedHash))
		if seedHash == "" {
			continue
		}
		out = append(out, seedHash)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func dbLoadSellerSeedSnapshotOnConn(ctx context.Context, conn SQLConn, seedHash string) (sellerSeed, bool, error) {
	if conn == nil {
		return sellerSeed{}, false, fmt.Errorf("sql conn is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return sellerSeed{}, false, nil
	}
	var unitPrice uint64
	var policyFound bool
	if err := QueryRowContext(ctx, conn,
		`SELECT floor_unit_price_sat_per_64k
		   FROM biz_seed_pricing_policy
		  WHERE seed_hash=?`,
		seedHash,
	).Scan(&unitPrice); err == nil {
		policyFound = true
	} else if !errors.Is(err, sql.ErrNoRows) {
		return sellerSeed{}, false, err
	}

	var seed sellerSeed
	if err := QueryRowContext(ctx, conn,
		`SELECT seed_hash,chunk_count,file_size,recommended_file_name,mime_hint
		   FROM biz_seeds
		  WHERE seed_hash=?`,
		seedHash,
	).Scan(&seed.SeedHash, &seed.ChunkCount, &seed.FileSize, &seed.RecommendedFileName, &seed.MIMEHint); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sellerSeed{}, false, nil
		}
		return sellerSeed{}, false, err
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
	return seed, true, nil
}

func dbPersistPricingAutopilotStateAndAudits(ctx context.Context, store *clientDB, state PricingState, audits []PricingAuditItem, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	if updatedAtUnix <= 0 {
		updatedAtUnix = time.Now().Unix()
	}
	return store.Tx(ctx, func(conn SQLConn) error {
		if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, state, updatedAtUnix); err != nil {
			return err
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnConn(ctx, conn, item); err != nil {
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
	return store.Tx(ctx, func(conn SQLConn) error {
		for _, state := range states {
			if strings.TrimSpace(state.SeedHash) == "" {
				continue
			}
			if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, state, updatedAtUnix); err != nil {
				return err
			}
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnConn(ctx, conn, item); err != nil {
				return err
			}
		}
		return nil
	})
}
