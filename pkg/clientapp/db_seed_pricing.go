package clientapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen"
	"github.com/bsv8/BitFS/pkg/clientapp/coredb/gen/bizseedpricingpolicy"
)

type seedPricingPolicyRow struct {
	SeedHash            string
	FloorPriceSatPer64K uint64
	ResaleDiscountBPS   uint64
	PricingSource       string
	UpdatedAtUnix       int64
}

// 这里只放种子定价策略的读写，避免和运行流程混在一起。

func dbLoadSeedPricingPolicyTx(ctx context.Context, tx EntWriteRoot, seedHash string) (seedPricingPolicyRow, error) {
	if tx == nil {
		return seedPricingPolicyRow{}, fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return seedPricingPolicyRow{}, fmt.Errorf("seed_hash required")
	}
	node, err := tx.BizSeedPricingPolicy.Query().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if gen.IsNotFound(err) {
			return seedPricingPolicyRow{}, sql.ErrNoRows
		}
		return seedPricingPolicyRow{}, err
	}
	return seedPricingPolicyRow{
		SeedHash:            node.SeedHash,
		FloorPriceSatPer64K: uint64(node.FloorUnitPriceSatPer64k),
		ResaleDiscountBPS:   uint64(node.ResaleDiscountBps),
		PricingSource:       node.PricingSource,
		UpdatedAtUnix:       node.UpdatedAtUnix,
	}, nil
}

func dbUpsertSeedPricingPolicy(ctx context.Context, store *clientDB, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		return dbUpsertSeedPricingPolicyTx(ctx, tx, seedHash, floorUnit, discountBPS, source, updatedAtUnix)
	})
}

func dbUpsertSeedPricingPolicyTx(ctx context.Context, tx EntWriteRoot, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source != "user" && source != "system" {
		source = "system"
	}

	existing, err := tx.BizSeedPricingPolicy.Query().Where(bizseedpricingpolicy.SeedHashEQ(seedHash)).Only(ctx)
	if err != nil {
		if !gen.IsNotFound(err) {
			return err
		}
		_, err = tx.BizSeedPricingPolicy.Create().
			SetSeedHash(seedHash).
			SetFloorUnitPriceSatPer64k(int64(floorUnit)).
			SetResaleDiscountBps(int64(discountBPS)).
			SetPricingSource(source).
			SetUpdatedAtUnix(updatedAtUnix).
			Save(ctx)
		return err
	}
	if existing.PricingSource == "user" && source == "system" {
		return nil
	}
	_, err = existing.Update().
		SetFloorUnitPriceSatPer64k(int64(floorUnit)).
		SetResaleDiscountBps(int64(discountBPS)).
		SetPricingSource(source).
		SetUpdatedAtUnix(updatedAtUnix).
		Save(ctx)
	return err
}

func dbSyncSystemSeedPricingPolicies(ctx context.Context, store *clientDB, floorUnit, discountBPS uint64) error {
	if store == nil {
		return fmt.Errorf("client db is nil")
	}
	return store.WriteEntTx(ctx, func(tx EntWriteRoot) error {
		var rows []struct {
			SeedHash string `json:"seed_hash,omitempty"`
		}
		if err := tx.BizSeeds.Query().
			Unique(true).
			Select("seed_hash").
			Scan(ctx, &rows); err != nil {
			return err
		}
		now := time.Now().Unix()
		for _, row := range rows {
			if err := dbUpsertSeedPricingPolicyTx(ctx, tx, row.SeedHash, floorUnit, discountBPS, "system", now); err != nil {
				return err
			}
		}
		return nil
	})
}
