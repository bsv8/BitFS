package clientapp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
	"github.com/bsv8/BitFS/pkg/clientapp/moduleapi"
)

type PricingSetBaseResult struct {
	Config PricingConfig `json:"pricing_config"`
}

type PricingStateResult struct {
	State PricingState `json:"pricing_state"`
}

type PricingAuditsResult struct {
	Audits []PricingAuditItem `json:"pricing_audits"`
}

type PricingSeedHashesResult struct {
	SeedHashes []string `json:"seed_hashes"`
}

type PricingRunTickResult struct {
	Config PricingConfig      `json:"pricing_config"`
	States []PricingState     `json:"pricing_states"`
	Audits []PricingAuditItem `json:"pricing_audits"`
}

type PricingReconcileResult struct {
	Config  PricingConfig      `json:"pricing_config"`
	State   PricingState       `json:"pricing_state"`
	Audits  []PricingAuditItem `json:"pricing_audits"`
	Changed bool               `json:"changed"`
}

func (r *Runtime) requirePricingStore() (*clientDB, error) {
	if r == nil || r.store == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	return r.store, nil
}

func (r *Runtime) pricingCurrentBase(ctx context.Context) uint64 {
	base := uint64(1000)
	if r != nil && r.config != nil {
		base = r.config.Snapshot().Seller.Pricing.LiveBasePriceSatPer64K
	}
	if r != nil && r.store != nil && ctx != nil {
		if cfg, ok, err := dbLoadPricingAutopilotConfig(ctx, r.store); err == nil && ok && cfg.BasePriceSatPer64K > 0 {
			base = cfg.BasePriceSatPer64K
		}
	}
	if base == 0 {
		base = 1000
	}
	return base
}

func (r *Runtime) requirePricingSeedExists(ctx context.Context, seedHash string) error {
	if r == nil || r.Catalog == nil {
		return fmt.Errorf("runtime not initialized")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return fmt.Errorf("seed_hash is required")
	}
	_, ok := r.Catalog.Get(seedHash)
	if !ok {
		return fmt.Errorf("seed not found")
	}
	return nil
}

func (r *Runtime) loadPricingStateForSeed(ctx context.Context, seedHash string) (PricingState, bool, error) {
	if r == nil || r.store == nil {
		return PricingState{}, false, fmt.Errorf("runtime not initialized")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return PricingState{}, false, fmt.Errorf("seed_hash is required")
	}
	base := r.pricingCurrentBase(ctx)
	state, ok, err := dbLoadPricingAutopilotState(ctx, r.store, seedHash)
	if err != nil {
		return PricingState{}, false, err
	}
	if ok {
		return pricingNormalizeStateForBase(state, base, time.Now().Unix()), true, nil
	}
	seed, ok, err := dbLoadSellerSeedSnapshot(ctx, r.store, seedHash)
	if err != nil {
		return PricingState{}, false, err
	}
	if !ok {
		return PricingState{}, false, fmt.Errorf("seed not found")
	}
	return pricingStateSeedSnapshot(seed, base), true, nil
}

func loadPricingStateForSeedOnConn(ctx context.Context, conn SQLConn, seedHash string, base uint64, nowUnix int64) (PricingState, bool, error) {
	if conn == nil {
		return PricingState{}, false, fmt.Errorf("sql conn is nil")
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return PricingState{}, false, fmt.Errorf("seed_hash is required")
	}
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
		return PricingState{}, false, fmt.Errorf("seed not found")
	}
	state = pricingStateSeedSnapshot(seed, base)
	return pricingNormalizeStateForBase(state, base, nowUnix), true, nil
}

// rollbackPricingSetBaseState 只负责把运行态配置拉回旧值。
// 设计说明：
// - DB 事务失败后，文件和内存必须尽最大努力回退；
// - 这里先回配置文件，再回内存，避免进程继续跑在新值上；
// - 不吞错误，调用方要把原始失败和回滚失败一起上报。
func rollbackPricingSetBaseState(cfgSvc *runtimeConfigService, oldCfg Config, persistConfig bool) error {
	if cfgSvc == nil {
		return fmt.Errorf("runtime config service is nil")
	}
	if persistConfig {
		if err := cfgSvc.UpdateAndPersist(oldCfg); err != nil {
			if memErr := cfgSvc.UpdateMemoryOnly(oldCfg); memErr != nil {
				return fmt.Errorf("config rollback failed: %v; memory rollback failed: %v", err, memErr)
			}
			return fmt.Errorf("config rollback failed: %v", err)
		}
		return nil
	}
	if err := cfgSvc.UpdateMemoryOnly(oldCfg); err != nil {
		return fmt.Errorf("memory rollback failed: %v", err)
	}
	return nil
}

func TriggerPricingSetBase(ctx context.Context, rt *Runtime, base uint64) (PricingSetBaseResult, error) {
	if rt == nil {
		return PricingSetBaseResult{}, fmt.Errorf("runtime not initialized")
	}
	if base == 0 {
		return PricingSetBaseResult{}, fmt.Errorf("base_price_sat_per_64k is required")
	}
	cfgSvc := rt.RuntimeConfigService()
	if cfgSvc == nil {
		return PricingSetBaseResult{}, fmt.Errorf("runtime config service not initialized")
	}
	mode := cfgSvc.StartupMode()
	configPath := strings.TrimSpace(cfgSvc.ConfigPath())
	persistConfig := configPath != ""
	if !persistConfig && mode == StartupModeProduct {
		err := fmt.Errorf("config path is required in product mode")
		obs.Error(ServiceName, "pricing_control_set_base_rejected", map[string]any{
			"base_price_sat_per_64k": base,
			"startup_mode":           string(mode),
			"config_path":            configPath,
			"error":                  err.Error(),
		})
		return PricingSetBaseResult{}, err
	}
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingSetBaseResult{}, err
	}
	oldCfg := cfgSvc.Snapshot()
	nextCfg := cfgSvc.SnapshotWithSellerLiveBasePrice(base)
	if persistConfig {
		if err := cfgSvc.UpdateAndPersist(nextCfg); err != nil {
			obs.Error(ServiceName, "pricing_control_set_base_config_write_failed", map[string]any{
				"base_price_sat_per_64k": base,
				"startup_mode":           string(mode),
				"config_path":            configPath,
				"error":                  err.Error(),
			})
			return PricingSetBaseResult{}, err
		}
	} else {
		if err := cfgSvc.UpdateMemoryOnly(nextCfg); err != nil {
			obs.Error(ServiceName, "pricing_control_set_base_memory_update_failed", map[string]any{
				"base_price_sat_per_64k": base,
				"startup_mode":           string(mode),
				"config_path":            configPath,
				"error":                  err.Error(),
			})
			return PricingSetBaseResult{}, err
		}
	}
	now := time.Now()
	if err := store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		seedHashes, err := dbListAllPricingSeedHashesOnConn(ctx, wtx)
		if err != nil {
			return err
		}
		if err := dbUpsertPricingAutopilotConfigOnConn(ctx, wtx, PricingConfig{BasePriceSatPer64K: base}, now.Unix()); err != nil {
			return err
		}
		for _, seedHash := range seedHashes {
			state, ok, err := loadPricingStateForSeedOnConn(ctx, wtx, seedHash, base, now.Unix())
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			nextState := pricingAutopilotSetBaseState(state, base, now)
			if err := dbUpsertPricingAutopilotStateOnConn(ctx, wtx, nextState, now.Unix()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		rollbackErr := rollbackPricingSetBaseState(cfgSvc, oldCfg, persistConfig)
		fields := map[string]any{
			"base_price_sat_per_64k": base,
			"startup_mode":           string(mode),
			"config_path":            configPath,
			"error":                  err.Error(),
		}
		if rollbackErr != nil {
			fields["rollback_error"] = rollbackErr.Error()
			obs.Error(ServiceName, "pricing_control_set_base_rollback_failed", fields)
			return PricingSetBaseResult{}, fmt.Errorf("pricing.set_base db update failed: %w; rollback failed: %v", err, rollbackErr)
		}
		obs.Error(ServiceName, "pricing_control_set_base_failed", fields)
		return PricingSetBaseResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_set_base", map[string]any{
		"base_price_sat_per_64k": base,
		"startup_mode":           string(mode),
		"config_path":            configPath,
		"config_persisted":       persistConfig,
	})
	return PricingSetBaseResult{Config: PricingConfig{BasePriceSatPer64K: base}}, nil
}

func TriggerPricingResetSeed(ctx context.Context, rt *Runtime, seedHash string) (PricingStateResult, error) {
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingStateResult{}, err
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return PricingStateResult{}, fmt.Errorf("seed_hash is required")
	}
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingStateResult{}, err
	}
	var seed PricingState
	var audit PricingAuditItem
	now := time.Now()
	base := rt.pricingCurrentBase(ctx)
	err = store.WriteTx(ctx, func(wtx moduleapi.WriteTx) error {
		nextSeed, ok, err := loadPricingStateForSeedOnConn(ctx, wtx, seedHash, base, now.Unix())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seed not found")
		}
		seed, audit = pricingAutopilotResetSeed(nextSeed, base, now)
		if err := dbUpsertPricingAutopilotStateOnConn(ctx, wtx, seed, now.Unix()); err != nil {
			return err
		}
		return dbAppendPricingAutopilotAuditOnConn(ctx, wtx, audit)
	})
	if err != nil {
		return PricingStateResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_reset_seed", map[string]any{"seed_hash": seedHash})
	return PricingStateResult{State: seed}, nil
}

func TriggerPricingFeedSeed(ctx context.Context, rt *Runtime, req PricingFeedRequest) (PricingStateResult, error) {
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingStateResult{}, err
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if seedHash == "" {
		return PricingStateResult{}, fmt.Errorf("seed_hash is required")
	}
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingStateResult{}, err
	}
	var seed PricingState
	now := time.Now()
	base := rt.pricingCurrentBase(ctx)
	err = store.WriteTx(ctx, func(conn moduleapi.WriteTx) error {
		nextSeed, ok, err := loadPricingStateForSeedOnConn(ctx, conn, seedHash, base, now.Unix())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seed not found")
		}
		seed = pricingAutopilotFeedSeed(nextSeed, uint64(req.QueryCount), uint64(req.DealCount), uint64(req.SilenceHours), now)
		return dbUpsertPricingAutopilotStateOnConn(ctx, conn, seed, now.Unix())
	})
	if err != nil {
		return PricingStateResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_feed_seed", map[string]any{
		"seed_hash":     seedHash,
		"query_count":   req.QueryCount,
		"deal_count":    req.DealCount,
		"silence_hours": req.SilenceHours,
	})
	return PricingStateResult{State: seed}, nil
}

func TriggerPricingSetForce(ctx context.Context, rt *Runtime, req ForcePriceRequest) (PricingStateResult, error) {
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingStateResult{}, err
	}
	seedHash := strings.ToLower(strings.TrimSpace(req.SeedHash))
	if seedHash == "" {
		return PricingStateResult{}, fmt.Errorf("seed_hash is required")
	}
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingStateResult{}, err
	}
	if req.PriceSatPer64K == 0 {
		return PricingStateResult{}, fmt.Errorf("force_price_sat_per_64k is required")
	}
	if req.ForceHours == 0 {
		return PricingStateResult{}, fmt.Errorf("force_hours is required")
	}
	var seed PricingState
	var audit PricingAuditItem
	now := time.Now()
	base := rt.pricingCurrentBase(ctx)
	err = store.WriteTx(ctx, func(conn moduleapi.WriteTx) error {
		nextSeed, ok, err := loadPricingStateForSeedOnConn(ctx, conn, seedHash, base, now.Unix())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seed not found")
		}
		seed, audit = pricingAutopilotSetForce(nextSeed, base, req.PriceSatPer64K, req.ForceHours, now)
		if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, seed, now.Unix()); err != nil {
			return err
		}
		return dbAppendPricingAutopilotAuditOnConn(ctx, conn, audit)
	})
	if err != nil {
		return PricingStateResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_set_force", map[string]any{
		"seed_hash":               seedHash,
		"force_price_sat_per_64k": req.PriceSatPer64K,
		"force_hours":             req.ForceHours,
	})
	return PricingStateResult{State: seed}, nil
}

func TriggerPricingReleaseForce(ctx context.Context, rt *Runtime, seedHash string) (PricingStateResult, error) {
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingStateResult{}, err
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return PricingStateResult{}, fmt.Errorf("seed_hash is required")
	}
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingStateResult{}, err
	}
	var seed PricingState
	var audit PricingAuditItem
	now := time.Now()
	base := rt.pricingCurrentBase(ctx)
	err = store.WriteTx(ctx, func(conn moduleapi.WriteTx) error {
		nextSeed, ok, err := loadPricingStateForSeedOnConn(ctx, conn, seedHash, base, now.Unix())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seed not found")
		}
		seed, audit = pricingAutopilotReleaseForce(nextSeed, now)
		if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, seed, now.Unix()); err != nil {
			return err
		}
		return dbAppendPricingAutopilotAuditOnConn(ctx, conn, audit)
	})
	if err != nil {
		return PricingStateResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_release_force", map[string]any{"seed_hash": seedHash})
	return PricingStateResult{State: seed}, nil
}

func TriggerPricingRunTick(ctx context.Context, rt *Runtime, hours uint32) (PricingRunTickResult, error) {
	if rt == nil {
		return PricingRunTickResult{}, fmt.Errorf("runtime not initialized")
	}
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingRunTickResult{}, err
	}
	if hours == 0 {
		hours = 1
	}
	now := time.Now()
	effectiveNow := now.Add(time.Duration(hours) * time.Hour)
	base := rt.pricingCurrentBase(ctx)
	var cfg PricingConfig
	var states []PricingState
	var audits []PricingAuditItem
	if err := store.WriteTx(ctx, func(conn moduleapi.WriteTx) error {
		if row, ok, err := dbLoadPricingAutopilotConfigOnConn(ctx, conn); err != nil {
			return err
		} else if ok && row.BasePriceSatPer64K > 0 {
			base = row.BasePriceSatPer64K
		}
		cfg = PricingConfig{BasePriceSatPer64K: base}
		seedHashes, err := dbListAllPricingSeedHashesOnConn(ctx, conn)
		if err != nil {
			return err
		}
		states = make([]PricingState, 0, len(seedHashes))
		for _, seedHash := range seedHashes {
			state, ok, err := loadPricingStateForSeedOnConn(ctx, conn, seedHash, base, now.Unix())
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			nextState, audit := pricingAutopilotAdvanceState(state, hours, effectiveNow, "pricing.tick")
			states = append(states, nextState)
			if audit != nil {
				audits = append(audits, *audit)
			}
		}
		for _, state := range states {
			if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, state, now.Unix()); err != nil {
				return err
			}
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnConn(ctx, conn, item); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return PricingRunTickResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_run_tick", map[string]any{
		"hours": hours,
	})
	return PricingRunTickResult{
		Config: cfg,
		States: states,
		Audits: audits,
	}, nil
}

func TriggerPricingReconcile(ctx context.Context, rt *Runtime, seedHash string, nowUnix int64) (PricingReconcileResult, error) {
	if rt == nil {
		return PricingReconcileResult{}, fmt.Errorf("runtime not initialized")
	}
	store, err := rt.requirePricingStore()
	if err != nil {
		return PricingReconcileResult{}, err
	}
	seedHash = strings.ToLower(strings.TrimSpace(seedHash))
	if seedHash == "" {
		return PricingReconcileResult{}, fmt.Errorf("seed_hash is required")
	}
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingReconcileResult{}, err
	}
	if nowUnix <= 0 {
		nowUnix = time.Now().Unix()
	}
	base := rt.pricingCurrentBase(ctx)
	var cfg PricingConfig
	var state PricingState
	var audits []PricingAuditItem
	var changed bool
	if err := store.WriteTx(ctx, func(conn moduleapi.WriteTx) error {
		if row, ok, err := dbLoadPricingAutopilotConfigOnConn(ctx, conn); err != nil {
			return err
		} else if ok && row.BasePriceSatPer64K > 0 {
			base = row.BasePriceSatPer64K
		}
		cfg = PricingConfig{BasePriceSatPer64K: base}
		current, ok, err := loadPricingStateForSeedOnConn(ctx, conn, seedHash, base, nowUnix)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("seed not found")
		}
		nextState, nextAudits, nextChanged := pricingAutopilotTriggerReconcile(current, nowUnix)
		state = nextState
		changed = nextChanged
		audits = nextAudits
		if !changed {
			return nil
		}
		if err := dbUpsertPricingAutopilotStateOnConn(ctx, conn, state, nowUnix); err != nil {
			return err
		}
		for _, item := range audits {
			if err := dbAppendPricingAutopilotAuditOnConn(ctx, conn, item); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return PricingReconcileResult{}, err
	}
	obs.Business(ServiceName, "pricing_control_trigger_reconcile", map[string]any{
		"seed_hash": seedHash,
		"now_unix":  nowUnix,
		"changed":   changed,
	})
	return PricingReconcileResult{
		Config:  cfg,
		State:   state,
		Audits:  audits,
		Changed: changed,
	}, nil
}

func TriggerPricingGetConfig(rt *Runtime) (PricingConfig, error) {
	if rt == nil {
		return PricingConfig{}, fmt.Errorf("runtime not initialized")
	}
	if rt.store != nil && rt.ctx != nil {
		if cfg, ok, err := dbLoadPricingAutopilotConfig(rt.ctx, rt.store); err != nil {
			return PricingConfig{}, err
		} else if ok {
			return cfg, nil
		}
	}
	if rt.config == nil {
		return PricingConfig{}, fmt.Errorf("runtime config service not initialized")
	}
	return PricingConfig{BasePriceSatPer64K: rt.config.Snapshot().Seller.Pricing.LiveBasePriceSatPer64K}, nil
}

func TriggerPricingGetState(ctx context.Context, rt *Runtime, seedHash string) (PricingState, error) {
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return PricingState{}, err
	}
	state, ok, err := rt.loadPricingStateForSeed(ctx, seedHash)
	if err != nil {
		return PricingState{}, err
	}
	if !ok {
		return PricingState{}, fmt.Errorf("seed not found")
	}
	return state, nil
}

func TriggerPricingGetAudits(ctx context.Context, rt *Runtime, seedHash string, limit int) ([]PricingAuditItem, error) {
	if err := rt.requirePricingSeedExists(ctx, seedHash); err != nil {
		return nil, err
	}
	items, err := dbListPricingAutopilotAudits(ctx, rt.store, seedHash, limit)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func TriggerPricingListSeeds(ctx context.Context, rt *Runtime, limit int) ([]string, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime not initialized")
	}
	store, err := rt.requirePricingStore()
	if err != nil {
		return nil, err
	}
	return dbListPricingSeedHashes(ctx, store, limit)
}
