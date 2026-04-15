package clientapp

import (
	"math"
	"time"
)

type PricingMode string

const (
	PricingModeAuto  PricingMode = "auto"
	PricingModeForce PricingMode = "force"
)

type ForcePriceRequest struct {
	SeedHash       string `json:"seed_hash"`
	PriceSatPer64K uint64 `json:"force_price_sat_per_64k"`
	ForceHours     uint32 `json:"force_hours"`
}

type PricingConfig struct {
	BasePriceSatPer64K uint64 `json:"base_price_sat_per_64k"`
}

type PricingFeedRequest struct {
	SeedHash     string `json:"seed_hash"`
	QueryCount   uint32 `json:"query_count"`
	DealCount    uint32 `json:"deal_count"`
	SilenceHours uint32 `json:"silence_hours"`
}

type PricingAuditItem struct {
	SeedHash                string      `json:"seed_hash"`
	Mode                    PricingMode `json:"pricing_mode"`
	EffectivePriceSatPer64K uint64      `json:"effective_price_sat_per_64k"`
	ReasonCode              string      `json:"reason_code"`
	TickedAtUnix            int64       `json:"ticked_at_unix"`
}

type PricingState struct {
	SeedHash                string      `json:"seed_hash"`
	Mode                    PricingMode `json:"pricing_mode"`
	EffectivePriceSatPer64K uint64      `json:"effective_price_sat_per_64k"`
	ForcedPriceSatPer64K    uint64      `json:"forced_price_sat_per_64k"`
	ForceUntilUnix          int64       `json:"force_until_unix"`
	BasePriceSatPer64K      uint64      `json:"base_price_sat_per_64k"`
	LastDeltaBPS            int32       `json:"last_delta_bps"`
	MaxRaiseBPSPerHour      uint32      `json:"max_raise_bps_per_hour"`
	MaxDropBPSPerHour       uint32      `json:"max_drop_bps_per_hour"`
	PriceCeilingSatPer64K   uint64      `json:"price_ceiling_sat_per_64k"`
	PendingQueryCount       uint64      `json:"pending_query_count"`
	PendingDealCount        uint64      `json:"pending_deal_count"`
	QueryRatePerHourMilli   uint64      `json:"query_rate_per_hour_milli"`
	DealRatePerHourMilli    uint64      `json:"deal_rate_per_hour_milli"`
	LastRateCalcAtUnix      int64       `json:"last_rate_calc_at_unix"`
	LastCalcWindowHours     uint32      `json:"last_calc_window_hours"`
	SilenceHours            uint32      `json:"silence_hours"`
}

// 设计说明：
// - 定价算法只保留纯计算函数，不再持有 seed map 或审计缓存；
// - 所有输入都来自调用方读出的 DB 状态，输出也是新的状态快照；
// - 这样控制入口可以直接按“读 DB -> 算 -> 回写 DB”执行。

func pricingStateSeedSnapshot(seed sellerSeed, base uint64) PricingState {
	if base == 0 {
		base = 1000
	}
	nowUnix := time.Now().Unix()
	return pricingNormalizeStateForBase(PricingState{
		SeedHash:                normalizeSeedHashHex(seed.SeedHash),
		Mode:                    PricingModeAuto,
		EffectivePriceSatPer64K: base,
		BasePriceSatPer64K:      base,
		MaxRaiseBPSPerHour:      defaultPricingMaxRaiseBPSPerHour,
		MaxDropBPSPerHour:       defaultPricingMaxDropBPSPerHour,
		PriceCeilingSatPer64K:   pricingCeilingForBase(base),
		LastRateCalcAtUnix:      nowUnix,
	}, base, nowUnix)
}

func pricingNormalizeStateForBase(state PricingState, base uint64, nowUnix int64) PricingState {
	if base == 0 {
		base = 1000
	}
	state.SeedHash = normalizeSeedHashHex(state.SeedHash)
	if state.BasePriceSatPer64K == 0 {
		state.BasePriceSatPer64K = base
	}
	if state.PriceCeilingSatPer64K == 0 {
		state.PriceCeilingSatPer64K = pricingCeilingForBase(state.BasePriceSatPer64K)
	}
	if state.MaxRaiseBPSPerHour == 0 {
		state.MaxRaiseBPSPerHour = defaultPricingMaxRaiseBPSPerHour
	}
	if state.MaxDropBPSPerHour == 0 {
		state.MaxDropBPSPerHour = defaultPricingMaxDropBPSPerHour
	}
	if state.Mode == "" {
		state.Mode = PricingModeAuto
	}
	if state.Mode == PricingModeForce {
		if state.ForcedPriceSatPer64K == 0 {
			if state.EffectivePriceSatPer64K > 0 {
				state.ForcedPriceSatPer64K = state.EffectivePriceSatPer64K
			} else {
				state.ForcedPriceSatPer64K = state.BasePriceSatPer64K
			}
		}
		if state.EffectivePriceSatPer64K == 0 {
			state.EffectivePriceSatPer64K = state.ForcedPriceSatPer64K
		}
	} else if state.EffectivePriceSatPer64K == 0 {
		state.EffectivePriceSatPer64K = state.BasePriceSatPer64K
	}
	if state.LastRateCalcAtUnix == 0 {
		if nowUnix <= 0 {
			nowUnix = time.Now().Unix()
		}
		state.LastRateCalcAtUnix = nowUnix
	}
	return state
}

func pricingAutopilotSetBaseState(state PricingState, base uint64, now time.Time) PricingState {
	if base == 0 {
		base = 1000
	}
	if now.IsZero() {
		now = time.Now()
	}
	state = pricingNormalizeStateForBase(state, base, now.Unix())
	state.BasePriceSatPer64K = base
	state.PriceCeilingSatPer64K = pricingCeilingForBase(base)
	if state.Mode != PricingModeForce {
		state.EffectivePriceSatPer64K = base
		state.LastDeltaBPS = 0
	}
	return pricingNormalizeStateForBase(state, base, now.Unix())
}

func pricingAutopilotResetSeed(state PricingState, base uint64, now time.Time) (PricingState, PricingAuditItem) {
	if now.IsZero() {
		now = time.Now()
	}
	state = pricingAutopilotSetBaseState(state, base, now)
	state.Mode = PricingModeAuto
	state.ForcedPriceSatPer64K = 0
	state.ForceUntilUnix = 0
	state.EffectivePriceSatPer64K = state.BasePriceSatPer64K
	state.LastDeltaBPS = 0
	state.MaxRaiseBPSPerHour = defaultPricingMaxRaiseBPSPerHour
	state.MaxDropBPSPerHour = defaultPricingMaxDropBPSPerHour
	state.PriceCeilingSatPer64K = pricingCeilingForBase(state.BasePriceSatPer64K)
	state.PendingQueryCount = 0
	state.PendingDealCount = 0
	state.QueryRatePerHourMilli = 0
	state.DealRatePerHourMilli = 0
	state.LastRateCalcAtUnix = now.Unix()
	state.LastCalcWindowHours = 0
	state.SilenceHours = 0
	state = pricingNormalizeStateForBase(state, base, now.Unix())
	return state, pricingAuditItem(state, "pricing.reset_seed", now.Unix())
}

func pricingAutopilotFeedSeed(state PricingState, queryCount, dealCount, silenceHours uint64, now time.Time) PricingState {
	if now.IsZero() {
		now = time.Now()
	}
	state = pricingNormalizeStateForBase(state, state.BasePriceSatPer64K, now.Unix())
	state.PendingQueryCount += queryCount
	state.PendingDealCount += dealCount
	if silenceHours > 0 {
		state.SilenceHours = uint32(silenceHours)
	}
	if state.LastRateCalcAtUnix == 0 {
		state.LastRateCalcAtUnix = now.Unix()
	}
	return state
}

func pricingAutopilotSetForce(state PricingState, base uint64, forcePrice uint64, forceHours uint32, now time.Time) (PricingState, PricingAuditItem) {
	if now.IsZero() {
		now = time.Now()
	}
	state = pricingNormalizeStateForBase(state, base, now.Unix())
	state.Mode = PricingModeForce
	state.ForcedPriceSatPer64K = forcePrice
	state.ForceUntilUnix = now.Add(time.Duration(forceHours) * time.Hour).Unix()
	state.EffectivePriceSatPer64K = forcePrice
	state.BasePriceSatPer64K = base
	if state.PriceCeilingSatPer64K == 0 {
		state.PriceCeilingSatPer64K = pricingCeilingForBase(base)
	}
	return state, pricingAuditItem(state, "pricing.force_set", now.Unix())
}

func pricingAutopilotReleaseForce(state PricingState, now time.Time) (PricingState, PricingAuditItem) {
	if now.IsZero() {
		now = time.Now()
	}
	state = pricingNormalizeStateForBase(state, state.BasePriceSatPer64K, now.Unix())
	state.Mode = PricingModeAuto
	state.ForcedPriceSatPer64K = 0
	state.ForceUntilUnix = 0
	state.EffectivePriceSatPer64K = state.BasePriceSatPer64K
	state.LastDeltaBPS = 0
	return state, pricingAuditItem(state, "pricing.force_release", now.Unix())
}

func pricingAutopilotRunTick(states []PricingState, hours uint32, now time.Time) ([]PricingState, []PricingAuditItem) {
	if hours == 0 {
		hours = 1
	}
	if now.IsZero() {
		now = time.Now()
	}
	effectiveNow := now.Add(time.Duration(hours) * time.Hour)
	out := make([]PricingState, 0, len(states))
	audits := make([]PricingAuditItem, 0, len(states))
	for _, state := range states {
		next, audit := pricingAutopilotAdvanceState(state, hours, effectiveNow, "pricing.tick")
		out = append(out, next)
		if audit != nil {
			audits = append(audits, *audit)
		}
	}
	return out, audits
}

func pricingAutopilotTriggerReconcile(state PricingState, nowUnix int64) (PricingState, []PricingAuditItem, bool) {
	if nowUnix <= 0 {
		nowUnix = time.Now().Unix()
	}
	lastCalc := state.LastRateCalcAtUnix
	if lastCalc <= 0 {
		lastCalc = nowUnix
		state.LastRateCalcAtUnix = nowUnix
	}
	windowHours := uint32(0)
	if nowUnix > lastCalc {
		windowHours = uint32((nowUnix - lastCalc) / 3600)
	}
	if windowHours == 0 {
		return state, nil, false
	}
	next, audit := pricingAutopilotAdvanceState(state, windowHours, time.Unix(nowUnix, 0), "pricing.reconcile")
	if audit == nil {
		return next, nil, true
	}
	return next, []PricingAuditItem{*audit}, true
}

func pricingAutopilotAdvanceState(state PricingState, hours uint32, now time.Time, reasonPrefix string) (PricingState, *PricingAuditItem) {
	if now.IsZero() {
		now = time.Now()
	}
	if hours == 0 {
		hours = 1
	}
	state = pricingNormalizeStateForBase(state, state.BasePriceSatPer64K, now.Unix())
	if state.Mode == PricingModeForce && state.ForceUntilUnix > 0 && now.Unix() >= state.ForceUntilUnix {
		state.Mode = PricingModeAuto
		state.ForcedPriceSatPer64K = 0
		state.ForceUntilUnix = 0
	}
	delta := pricingDeltaBPS(
		state.PendingQueryCount,
		state.PendingDealCount,
		uint64(state.SilenceHours),
		hours,
		state.MaxRaiseBPSPerHour,
		state.MaxDropBPSPerHour,
	)
	state.LastDeltaBPS = delta
	state.LastCalcWindowHours = hours
	state.LastRateCalcAtUnix = now.Unix()
	if hours > 0 {
		state.QueryRatePerHourMilli = scalePerHourMilli(state.PendingQueryCount, hours)
		state.DealRatePerHourMilli = scalePerHourMilli(state.PendingDealCount, hours)
	}
	state.PendingQueryCount = 0
	state.PendingDealCount = 0
	state.SilenceHours = 0
	if state.Mode == PricingModeForce {
		state.EffectivePriceSatPer64K = state.ForcedPriceSatPer64K
		item := pricingAuditItem(state, reasonPrefix+".force_hold", now.Unix())
		return state, &item
	}
	next := applyBPS(state.EffectivePriceSatPer64K, delta)
	if next < state.BasePriceSatPer64K {
		next = state.BasePriceSatPer64K
	}
	if state.PriceCeilingSatPer64K > 0 && next > state.PriceCeilingSatPer64K {
		next = state.PriceCeilingSatPer64K
	}
	state.EffectivePriceSatPer64K = next
	reason := reasonPrefix + ".auto_hold"
	switch {
	case delta > 0:
		reason = reasonPrefix + ".rise"
	case delta < 0:
		reason = reasonPrefix + ".drop"
	}
	item := pricingAuditItem(state, reason, now.Unix())
	return state, &item
}

func pricingAuditItem(state PricingState, reason string, tickedAtUnix int64) PricingAuditItem {
	if tickedAtUnix <= 0 {
		tickedAtUnix = time.Now().Unix()
	}
	return PricingAuditItem{
		SeedHash:                state.SeedHash,
		Mode:                    state.Mode,
		EffectivePriceSatPer64K: state.EffectivePriceSatPer64K,
		ReasonCode:              reason,
		TickedAtUnix:            tickedAtUnix,
	}
}

func pricingCeilingForBase(base uint64) uint64 {
	if base == 0 {
		base = 1000
	}
	const multiplier = 10
	if base > math.MaxUint64/multiplier {
		return math.MaxUint64
	}
	return base * multiplier
}

const (
	defaultPricingMaxRaiseBPSPerHour = uint32(800)
	defaultPricingMaxDropBPSPerHour  = uint32(700)
)

func pricingDeltaBPS(queryCount, dealCount, silenceHours uint64, hours uint32, maxRaiseBPSPerHour, maxDropBPSPerHour uint32) int32 {
	if hours == 0 {
		hours = 1
	}
	pressure := int64(dealCount*100) + int64(silenceHours*25) - int64(queryCount*70)
	if dealCount > queryCount {
		pressure += int64((dealCount - queryCount) * 60)
	}
	if queryCount > dealCount {
		pressure -= int64((queryCount - dealCount) * 20)
	}
	if pressure == 0 {
		return 0
	}
	magnitude := absInt64(pressure)/20 + 20
	magnitude *= int64(hours)
	if pressure > 0 {
		if magnitude > int64(maxRaiseBPSPerHour) {
			magnitude = int64(maxRaiseBPSPerHour)
		}
		return int32(magnitude)
	}
	if magnitude > int64(maxDropBPSPerHour) {
		magnitude = int64(maxDropBPSPerHour)
	}
	return -int32(magnitude)
}

func applyBPS(price uint64, deltaBPS int32) uint64 {
	if price == 0 || deltaBPS == 0 {
		return price
	}
	if deltaBPS > 0 {
		multiplier := uint64(10000 + deltaBPS)
		if price > math.MaxUint64/multiplier {
			return math.MaxUint64
		}
		return price * multiplier / 10000
	}
	bps := uint64(-deltaBPS)
	if bps >= 10000 {
		return 0
	}
	return price * (10000 - bps) / 10000
}

func scalePerHourMilli(count uint64, hours uint32) uint64 {
	if count == 0 {
		return 0
	}
	if hours == 0 {
		hours = 1
	}
	return count * 1000 / uint64(hours)
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
