package managedclient

import (
	"fmt"
	"strings"

	"github.com/bsv8/BitFS/pkg/clientapp"
)

func (d *managedDaemon) executeManagedPricingControlCommand(req controlCommandRequest) (controlCommandResult, error) {
	rt := d.currentRuntime()
	if rt == nil {
		return controlCommandResult{
			CommandID: req.CommandID,
			Action:    req.Action,
			Result:    "failed",
			Error:     "runtime not initialized",
		}, nil
	}
	ctx := d.rootCtx
	if ctx == nil {
		return controlCommandResult{
			CommandID: req.CommandID,
			Action:    req.Action,
			Result:    "failed",
			Error:     "runtime context is not ready",
		}, nil
	}

	switch req.Action {
	case controlActionPricingSetBase:
		base, ok := pricingPayloadUint64(req.Payload, "base_price_sat_per_64k")
		if !ok || base == 0 {
			return pricingActionFailure(req, d, "base_price_sat_per_64k is required"), nil
		}
		result, err := clientapp.TriggerPricingSetBase(ctx, rt, base)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "updated", map[string]any{"pricing_config": pricingConfigMap(result.Config)}, d), nil

	case controlActionPricingResetSeed:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		result, err := clientapp.TriggerPricingResetSeed(ctx, rt, seedHash)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "reset", map[string]any{"pricing_state": pricingStateMap(result.State)}, d), nil

	case controlActionPricingFeedSeed:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		queryCount, ok := pricingPayloadUint32(req.Payload, "query_count")
		if !ok {
			return pricingActionFailure(req, d, "query_count is required"), nil
		}
		dealCount, ok := pricingPayloadUint32(req.Payload, "deal_count")
		if !ok {
			return pricingActionFailure(req, d, "deal_count is required"), nil
		}
		silenceHours, ok := pricingPayloadUint32(req.Payload, "silence_hours")
		if !ok {
			return pricingActionFailure(req, d, "silence_hours is required"), nil
		}
		result, err := clientapp.TriggerPricingFeedSeed(ctx, rt, clientapp.PricingFeedRequest{
			SeedHash:     seedHash,
			QueryCount:   queryCount,
			DealCount:    dealCount,
			SilenceHours: silenceHours,
		})
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "fed", map[string]any{"pricing_state": pricingStateMap(result.State)}, d), nil

	case controlActionPricingSetForce:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		forcePrice, ok := pricingPayloadUint64(req.Payload, "force_price_sat_per_64k")
		if !ok || forcePrice == 0 {
			return pricingActionFailure(req, d, "force_price_sat_per_64k is required"), nil
		}
		forceHours, ok := pricingPayloadUint32(req.Payload, "force_hours")
		if !ok || forceHours == 0 {
			return pricingActionFailure(req, d, "force_hours is required"), nil
		}
		result, err := clientapp.TriggerPricingSetForce(ctx, rt, clientapp.ForcePriceRequest{
			SeedHash:       seedHash,
			PriceSatPer64K: forcePrice,
			ForceHours:     forceHours,
		})
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "forced", map[string]any{"pricing_state": pricingStateMap(result.State)}, d), nil

	case controlActionPricingReleaseForce:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		result, err := clientapp.TriggerPricingReleaseForce(ctx, rt, seedHash)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "released", map[string]any{"pricing_state": pricingStateMap(result.State)}, d), nil

	case controlActionPricingRunTick:
		hours, ok := pricingPayloadUint32(req.Payload, "hours")
		if !ok {
			return pricingActionFailure(req, d, "hours is required"), nil
		}
		result, err := clientapp.TriggerPricingRunTick(ctx, rt, hours)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "ticked", map[string]any{
			"pricing_config": pricingConfigMap(result.Config),
			"pricing_states": pricingStatesMap(result.States),
			"pricing_audits": pricingAuditsMap(result.Audits),
		}, d), nil

	case controlActionPricingTriggerReconcile:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		nowUnix, _ := pricingPayloadInt64(req.Payload, "now_unix")
		result, err := clientapp.TriggerPricingReconcile(ctx, rt, seedHash, nowUnix)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		resultText := "reconciled"
		if !result.Changed {
			resultText = "skipped"
		}
		return pricingActionSuccess(req, resultText, map[string]any{
			"pricing_config": pricingConfigMap(result.Config),
			"pricing_state":  pricingStateMap(result.State),
			"pricing_audits": pricingAuditsMap(result.Audits),
			"changed":        result.Changed,
		}, d), nil

	case controlActionPricingGetConfig:
		result, err := clientapp.TriggerPricingGetConfig(rt)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "fetched", map[string]any{"pricing_config": pricingConfigMap(result)}, d), nil

	case controlActionPricingGetState:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		result, err := clientapp.TriggerPricingGetState(ctx, rt, seedHash)
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "fetched", map[string]any{"pricing_state": pricingStateMap(result)}, d), nil

	case controlActionPricingGetAudits:
		seedHash, ok := pricingPayloadSeedHash(req.Payload)
		if !ok {
			return pricingActionFailure(req, d, "seed_hash is required"), nil
		}
		limit, ok := pricingPayloadUint32(req.Payload, "limit")
		if !ok || limit == 0 {
			return pricingActionFailure(req, d, "limit is required"), nil
		}
		result, err := clientapp.TriggerPricingGetAudits(ctx, rt, seedHash, int(limit))
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "fetched", map[string]any{"pricing_audits": pricingAuditsMap(result)}, d), nil

	case controlActionPricingListSeeds:
		limit, ok := pricingPayloadUint32(req.Payload, "limit")
		if !ok || limit == 0 {
			return pricingActionFailure(req, d, "limit is required"), nil
		}
		result, err := clientapp.TriggerPricingListSeeds(ctx, rt, int(limit))
		if err != nil {
			return pricingActionFailure(req, d, err.Error()), nil
		}
		return pricingActionSuccess(req, "listed", map[string]any{"seed_hashes": result}, d), nil

	default:
		return controlCommandResult{}, fmt.Errorf("unsupported control action: %s", req.Action)
	}
}

func (d *managedDaemon) currentRuntime() *clientapp.Runtime {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rt
}

func pricingActionSuccess(req controlCommandRequest, result string, payload map[string]any, d *managedDaemon) controlCommandResult {
	out := controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		OK:           true,
		Result:       result,
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
		Payload:      payload,
	}
	return out
}

func pricingActionFailure(req controlCommandRequest, d *managedDaemon, errText string) controlCommandResult {
	return controlCommandResult{
		CommandID:    req.CommandID,
		Action:       req.Action,
		Result:       "failed",
		Error:        strings.TrimSpace(errText),
		BackendPhase: d.currentBackendPhase(),
		RuntimePhase: d.currentRuntimePhase(),
		KeyState:     d.currentKeyState(),
	}
}

func pricingConfigMap(cfg clientapp.PricingConfig) map[string]any {
	return map[string]any{
		"base_price_sat_per_64k": cfg.BasePriceSatPer64K,
	}
}

func pricingStateMap(state clientapp.PricingState) map[string]any {
	return map[string]any{
		"seed_hash":                   strings.ToLower(strings.TrimSpace(state.SeedHash)),
		"pricing_mode":                strings.ToLower(strings.TrimSpace(string(state.Mode))),
		"effective_price_sat_per_64k": state.EffectivePriceSatPer64K,
		"forced_price_sat_per_64k":    state.ForcedPriceSatPer64K,
		"force_until_unix":            state.ForceUntilUnix,
		"base_price_sat_per_64k":      state.BasePriceSatPer64K,
		"last_delta_bps":              state.LastDeltaBPS,
		"max_raise_bps_per_hour":      state.MaxRaiseBPSPerHour,
		"max_drop_bps_per_hour":       state.MaxDropBPSPerHour,
		"price_ceiling_sat_per_64k":   state.PriceCeilingSatPer64K,
		"pending_query_count":         state.PendingQueryCount,
		"pending_deal_count":          state.PendingDealCount,
		"query_rate_per_hour_milli":   state.QueryRatePerHourMilli,
		"deal_rate_per_hour_milli":    state.DealRatePerHourMilli,
		"last_rate_calc_at_unix":      state.LastRateCalcAtUnix,
		"last_calc_window_hours":      state.LastCalcWindowHours,
		"silence_hours":               state.SilenceHours,
	}
}

func pricingStatesMap(items []clientapp.PricingState) []map[string]any {
	if len(items) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, pricingStateMap(item))
	}
	return out
}

func pricingAuditsMap(items []clientapp.PricingAuditItem) []map[string]any {
	if len(items) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"seed_hash":                   strings.ToLower(strings.TrimSpace(item.SeedHash)),
			"pricing_mode":                strings.ToLower(strings.TrimSpace(string(item.Mode))),
			"effective_price_sat_per_64k": item.EffectivePriceSatPer64K,
			"reason_code":                 strings.TrimSpace(item.ReasonCode),
			"ticked_at_unix":              item.TickedAtUnix,
		})
	}
	return out
}

func pricingPayloadSeedHash(payload map[string]any) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}
	raw, ok := payload["seed_hash"]
	if !ok || raw == nil {
		return "", false
	}
	s, ok := raw.(string)
	if !ok {
		return "", false
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "", false
	}
	return s, true
}

func pricingPayloadUint64(payload map[string]any, key string) (uint64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	raw, ok := payload[key]
	if !ok || raw == nil {
		return 0, false
	}
	switch v := raw.(type) {
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case uint16:
		return uint64(v), true
	case uint8:
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	default:
		return 0, false
	}
}

func pricingPayloadUint32(payload map[string]any, key string) (uint32, bool) {
	v, ok := pricingPayloadUint64(payload, key)
	return uint32(v), ok
}

func pricingPayloadInt64(payload map[string]any, key string) (int64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	raw, ok := payload[key]
	if !ok || raw == nil {
		return 0, false
	}
	switch v := raw.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case uint64:
		if v > mathMaxInt64() {
			return int64(mathMaxInt64()), true
		}
		return int64(v), true
	case uint32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func mathMaxInt64() uint64 {
	return ^uint64(0) >> 1
}
