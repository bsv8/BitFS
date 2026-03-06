package clientapp

import (
	"fmt"
	"time"
)

// 设计说明：
// - 静态文件与直播使用不同价速策略。
// - 这里先落最小纯函数，后续再把它接到真实直播购买调度。

type LiveBuyerStrategy struct {
	TargetLagSegments   uint32 `json:"target_lag_segments"`
	MaxBudgetPerMinute  uint64 `json:"max_budget_per_minute"`
	PreferOlderSegments bool   `json:"prefer_older_segments"`
}

type LiveSellerPricing struct {
	BasePriceSatPer64K  uint64 `json:"base_price_sat_per_64k"`
	FloorPriceSatPer64K uint64 `json:"floor_price_sat_per_64k"`
	DecayPerMinuteBPS   uint64 `json:"decay_per_minute_bps"`
}

type LivePurchaseDecision struct {
	Mode                string `json:"mode"`
	Reason              string `json:"reason"`
	TargetSegmentIndex  uint64 `json:"target_segment_index"`
	SeedHash            string `json:"seed_hash"`
	EstimatedChunkPrice uint64 `json:"estimated_chunk_price"`
	EstimatedAgeMinutes uint64 `json:"estimated_age_minutes"`
}

func ComputeLiveUnitPriceSatPer64K(pricing LiveSellerPricing, age time.Duration) uint64 {
	base := pricing.BasePriceSatPer64K
	floor := pricing.FloorPriceSatPer64K
	if base == 0 {
		return floor
	}
	if floor > base {
		floor = base
	}
	decay := pricing.DecayPerMinuteBPS
	if decay == 0 {
		return base
	}
	ageMinutes := uint64(age / time.Minute)
	if ageMinutes == 0 {
		return base
	}
	totalDecay := ageMinutes * decay
	if totalDecay >= 10000 {
		return floor
	}
	price := base * (10000 - totalDecay) / 10000
	if price < floor {
		return floor
	}
	return price
}

func ShouldPreferOlderSegments(strategy LiveBuyerStrategy) bool {
	return strategy.PreferOlderSegments
}

func PlanLivePurchase(snapshot LiveSubscriberSnapshot, haveSegmentIndex int64, strategy LiveBuyerStrategy, pricing LiveSellerPricing, now time.Time) (LivePurchaseDecision, error) {
	recent := normalizeLiveSegmentRefs(snapshot.RecentSegments)
	if len(recent) == 0 {
		return LivePurchaseDecision{}, fmt.Errorf("no recent segments")
	}
	head := recent[len(recent)-1]
	desiredIndex := head.SegmentIndex
	if lag := uint64(strategy.TargetLagSegments); lag > 0 && head.SegmentIndex > lag {
		desiredIndex = head.SegmentIndex - lag
	}
	candidates := make([]LivePurchaseDecision, 0, len(recent))
	for _, seg := range recent {
		if int64(seg.SegmentIndex) <= haveSegmentIndex {
			continue
		}
		ageMinutes := uint64(0)
		if seg.PublishedAtUnix > 0 && now.Unix() > seg.PublishedAtUnix {
			ageMinutes = uint64((now.Unix() - seg.PublishedAtUnix) / 60)
		}
		price := ComputeLiveUnitPriceSatPer64K(pricing, time.Duration(ageMinutes)*time.Minute)
		candidates = append(candidates, LivePurchaseDecision{
			TargetSegmentIndex:  seg.SegmentIndex,
			SeedHash:            seg.SeedHash,
			EstimatedChunkPrice: price,
			EstimatedAgeMinutes: ageMinutes,
		})
	}
	if len(candidates) == 0 {
		return LivePurchaseDecision{}, fmt.Errorf("no segment beyond current progress")
	}
	affordable := func(c LivePurchaseDecision) bool {
		return strategy.MaxBudgetPerMinute == 0 || c.EstimatedChunkPrice <= strategy.MaxBudgetPerMinute
	}
	bestIdx := -1
	for i := len(candidates) - 1; i >= 0; i-- {
		c := candidates[i]
		if c.TargetSegmentIndex > desiredIndex {
			continue
		}
		if affordable(c) {
			bestIdx = i
			break
		}
	}
	if bestIdx >= 0 {
		out := candidates[bestIdx]
		if out.TargetSegmentIndex < desiredIndex {
			out.Mode = "delayed_backfill"
			out.Reason = "older_segment_under_budget"
		} else {
			out.Mode = "target_lag"
			out.Reason = "within_target_lag_and_budget"
		}
		return out, nil
	}
	if strategy.PreferOlderSegments {
		for i := 0; i < len(candidates); i++ {
			if affordable(candidates[i]) {
				out := candidates[i]
				out.Mode = "delayed_backfill"
				out.Reason = "prefer_older_affordable_segment"
				return out, nil
			}
		}
		oldest := candidates[0]
		oldest.Mode = "delayed_backfill"
		oldest.Reason = "budget_tight_oldest_available"
		return oldest, nil
	}
	newest := candidates[len(candidates)-1]
	newest.Mode = "chase_head"
	newest.Reason = "prefer_latest_segment"
	return newest, nil
}

func ComputeLiveQuotePrices(seed sellerSeed, meta liveSegmentMeta, pricing LiveSellerPricing, now time.Time) sellerSeed {
	age := time.Duration(0)
	if meta.PublishedAtUnix > 0 && now.Unix() > meta.PublishedAtUnix {
		age = time.Duration(now.Unix()-meta.PublishedAtUnix) * time.Second
	}
	unit := ComputeLiveUnitPriceSatPer64K(pricing, age)
	multiplier := uint64(1)
	if seed.ChunkPrice > 0 && seed.SeedPrice >= seed.ChunkPrice {
		multiplier = seed.SeedPrice / seed.ChunkPrice
		if multiplier == 0 {
			multiplier = 1
		}
	}
	seed.ChunkPrice = unit
	seed.SeedPrice = unit * multiplier
	return seed
}
