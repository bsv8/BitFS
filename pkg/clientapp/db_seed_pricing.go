package clientapp

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type seedPricingPolicyRow struct {
	SeedHash            string
	FloorPriceSatPer64K uint64
	ResaleDiscountBPS   uint64
	PricingSource       string
	UpdatedAtUnix       int64
}

// 这里只放种子定价策略的读写，避免和运行流程混在一起。
func dbLoadSeedPricingPolicy(db *sql.DB, seedHash string) (seedPricingPolicyRow, error) {
	if db == nil {
		return seedPricingPolicyRow{}, fmt.Errorf("db is nil")
	}
	return dbLoadSeedPricingPolicyQuery(db, seedHash)
}

func dbLoadSeedPricingPolicyTx(tx *sql.Tx, seedHash string) (seedPricingPolicyRow, error) {
	if tx == nil {
		return seedPricingPolicyRow{}, fmt.Errorf("tx is nil")
	}
	return dbLoadSeedPricingPolicyQuery(tx, seedHash)
}

func dbUpsertSeedPricingPolicy(db *sql.DB, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	return dbUpsertSeedPricingPolicyExec(db, seedHash, floorUnit, discountBPS, source, updatedAtUnix)
}

func dbUpsertSeedPricingPolicyTx(tx *sql.Tx, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	return dbUpsertSeedPricingPolicyExec(tx, seedHash, floorUnit, discountBPS, source, updatedAtUnix)
}

func dbLoadSeedPricingPolicyQuery(queryer interface {
	QueryRow(string, ...any) *sql.Row
}, seedHash string) (seedPricingPolicyRow, error) {
	var out seedPricingPolicyRow
	err := queryer.QueryRow(`SELECT seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix FROM biz_seed_pricing_policy WHERE seed_hash=?`, normalizeSeedHashHex(seedHash)).
		Scan(&out.SeedHash, &out.FloorPriceSatPer64K, &out.ResaleDiscountBPS, &out.PricingSource, &out.UpdatedAtUnix)
	if err != nil {
		return seedPricingPolicyRow{}, err
	}
	return out, nil
}

func dbUpsertSeedPricingPolicyExec(execer interface {
	Exec(string, ...any) (sql.Result, error)
}, seedHash string, floorUnit, discountBPS uint64, source string, updatedAtUnix int64) error {
	seedHash = normalizeSeedHashHex(seedHash)
	if seedHash == "" {
		return fmt.Errorf("seed_hash required")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source != "user" && source != "system" {
		source = "system"
	}
	_, err := execer.Exec(
		`INSERT INTO biz_seed_pricing_policy(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix)
		 VALUES(?,?,?,?,?)
		 ON CONFLICT(seed_hash) DO UPDATE SET
		 floor_unit_price_sat_per_64k=CASE
		   WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.floor_unit_price_sat_per_64k
		   ELSE excluded.floor_unit_price_sat_per_64k
		 END,
		 resale_discount_bps=CASE
		   WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.resale_discount_bps
		   ELSE excluded.resale_discount_bps
		 END,
		 pricing_source=CASE
		   WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.pricing_source
		   ELSE excluded.pricing_source
		 END,
		 updated_at_unix=CASE
		   WHEN biz_seed_pricing_policy.pricing_source='user' AND excluded.pricing_source='system' THEN biz_seed_pricing_policy.updated_at_unix
		   ELSE excluded.updated_at_unix
		 END`,
		seedHash, floorUnit, discountBPS, source, updatedAtUnix,
	)
	return err
}

func dbSyncSystemSeedPricingPolicies(db *sql.DB, floorUnit, discountBPS uint64) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`SELECT seed_hash FROM biz_seeds`)
	if err != nil {
		return err
	}
	defer rows.Close()
	seedHashes := make([]string, 0, 64)
	for rows.Next() {
		var seedHash string
		if err := rows.Scan(&seedHash); err != nil {
			return err
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			continue
		}
		seedHashes = append(seedHashes, seedHash)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, seedHash := range seedHashes {
		if err := dbUpsertSeedPricingPolicy(db, seedHash, floorUnit, discountBPS, "system", now); err != nil {
			return err
		}
	}
	return nil
}
