package clientapp

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/bsv8/BFTP/pkg/obs"
)

// runAssetConsumptionHardCutMigration 执行 A 组硬切换迁移。
// 设计说明：
// - 将旧表 fact_asset_consumptions 的数据按资产类型拆分到新表
// - BSV 本币消耗 -> fact_bsv_consumptions
// - Token 消耗（BSV20/BSV21）-> fact_token_consumptions
// - 同时回填 fact_token_utxo_links 建立 Token 与载体 UTXO 的映射
// - 整个过程在单事务内完成，任何失败自动回滚
// - 幂等：重复执行不会重复插入数据
func runAssetConsumptionHardCutMigration(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 检查旧表是否存在数据需要迁移
	hasOldData, err := hasAssetConsumptionMigrationPending(db)
	if err != nil {
		return fmt.Errorf("check migration pending state failed: %w", err)
	}
	if !hasOldData {
		obs.Debug("bitcast-client", "asset_consumption_migration_skipped", map[string]any{
			"reason": "no pending data",
		})
		return nil
	}

	obs.Important("bitcast-client", "asset_consumption_migration_started", map[string]any{
		"phase": "A_group_hard_cut",
	})

	// 单事务包裹整个迁移过程
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	rollback := func() {
		_ = tx.Rollback()
	}

	// 1. 迁移 BSV 消耗记录
	bsvStats, err := migrateBSVConsumptionsTx(tx)
	if err != nil {
		rollback()
		return fmt.Errorf("migrate BSV consumptions failed: %w", err)
	}

	// 2. 迁移 Token 消耗记录
	tokenStats, err := migrateTokenConsumptionsTx(tx)
	if err != nil {
		rollback()
		return fmt.Errorf("migrate token consumptions failed: %w", err)
	}

	// 3. 回填 Token UTXO 链接
	linkStats, err := backfillTokenUTXOLinksTx(tx)
	if err != nil {
		rollback()
		return fmt.Errorf("backfill token UTXO links failed: %w", err)
	}

	// 4. 执行守恒校验
	if err := validateConservationTx(tx, bsvStats, tokenStats); err != nil {
		rollback()
		return fmt.Errorf("conservation validation failed: %w", err)
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	obs.Important("bitcast-client", "asset_consumption_migration_completed", map[string]any{
		"bsv_migrated":   bsvStats.MigratedCount,
		"token_migrated": tokenStats.MigratedCount,
		"links_created":  linkStats.CreatedCount,
	})

	return nil
}

// hasAssetConsumptionMigrationPending 检查是否还有待迁移的数据。
// 设计说明：
// - 只比较"可迁移的旧数据"（有 settlement_cycle_id 且关联到 BSV/Token flow 的记录）
// - 不比较"不可迁移的残留行"（如 settlement_cycle_id 为 NULL 的历史数据）
// - 通过 migration marker 记录已迁移的消耗ID，实现精确幂等
func hasAssetConsumptionMigrationPending(db *sql.DB) (bool, error) {
	// 检查旧表是否存在
	hasOldTable, err := hasTable(db, "fact_asset_consumptions")
	if err != nil {
		return false, fmt.Errorf("check old table existence: %w", err)
	}
	if !hasOldTable {
		return false, nil
	}

	// 检查旧表是否有可迁移的数据（有 settlement_cycle_id 且关联到 flow）
	// 注意：老库可能没有 settlement_cycle_id 列，需要处理这个错误
	var migratableCount int64
	err = db.QueryRow(`
		SELECT COUNT(1) 
		FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE ac.settlement_cycle_id IS NOT NULL
		  AND af.asset_kind IN ('BSV', 'BSV20', 'BSV21')
	`).Scan(&migratableCount)
	if err != nil {
		// 如果 settlement_cycle_id 列不存在，说明还没跑完 settlement_cycle 迁移
		// 此时不应触发 asset consumption 迁移
		if strings.Contains(err.Error(), "no such column") {
			return false, nil
		}
		return false, fmt.Errorf("count migratable old rows: %w", err)
	}
	if migratableCount == 0 {
		return false, nil
	}

	// 检查已迁移的标记数量（consumption_id 以 _mig_ 结尾的记录）
	var migratedBSVCount int64
	if err := db.QueryRow(`
		SELECT COUNT(1) FROM fact_bsv_consumptions 
		WHERE consumption_id LIKE '%_mig_%'
	`).Scan(&migratedBSVCount); err != nil {
		// 新表可能不存在，视为未迁移
		migratedBSVCount = 0
	}

	var migratedTokenCount int64
	if err := db.QueryRow(`
		SELECT COUNT(1) FROM fact_token_consumptions 
		WHERE consumption_id LIKE '%_mig_%'
	`).Scan(&migratedTokenCount); err != nil {
		migratedTokenCount = 0
	}

	// 如果已迁移数量 >= 可迁移数量，说明已完成
	if migratedBSVCount+migratedTokenCount >= migratableCount {
		return false, nil
	}

	return true, nil
}

// bsvMigrationStats BSV 迁移统计
type bsvMigrationStats struct {
	MigratedCount int64
	TotalSatoshi  int64
}

// migrateBSVConsumptionsTx 迁移 BSV 本币消耗记录。
// 从 fact_asset_consumptions 关联 fact_chain_asset_flows 筛选 asset_kind='BSV' 的记录
func migrateBSVConsumptionsTx(tx *sql.Tx) (*bsvMigrationStats, error) {
	stats := &bsvMigrationStats{}

	// 查询需要迁移的 BSV 消耗记录
	// 关联 flow 表确认 asset_kind='BSV'，同时确保 settlement_cycle_id 已回填
	rows, err := tx.Query(`
		SELECT 
			ac.id,
			ac.source_flow_id,
			ac.source_utxo_id,
			ac.chain_payment_id,
			ac.pool_allocation_id,
			ac.settlement_cycle_id,
			ac.state,
			ac.used_satoshi,
			ac.occurred_at_unix,
			ac.confirmed_at_unix,
			ac.note,
			ac.payload_json
		FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE af.asset_kind = 'BSV'
		  AND ac.settlement_cycle_id IS NOT NULL
		ORDER BY ac.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query BSV consumptions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, sourceFlowID int64
		var sourceUTXOID string
		var chainPaymentID, poolAllocationID, settlementCycleID sql.NullInt64
		var state string
		var usedSatoshi int64
		var occurredAt, confirmedAt sql.NullInt64
		var note, payloadJSON string

		if err := rows.Scan(
			&id, &sourceFlowID, &sourceUTXOID,
			&chainPaymentID, &poolAllocationID, &settlementCycleID,
			&state, &usedSatoshi, &occurredAt, &confirmedAt,
			&note, &payloadJSON,
		); err != nil {
			return nil, fmt.Errorf("scan BSV consumption row %d: %w", id, err)
		}

		// 幂等插入：使用 INSERT OR IGNORE 配合唯一键
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO fact_bsv_consumptions(
				consumption_id,
				source_flow_id,
				source_utxo_id,
				chain_payment_id,
				pool_allocation_id,
				settlement_cycle_id,
				state,
				used_satoshi,
				occurred_at_unix,
				confirmed_at_unix,
				note,
				payload_json
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			fmt.Sprintf("bsvcons_mig_%d", id),
			sourceFlowID,
			sourceUTXOID,
			chainPaymentID,
			poolAllocationID,
			settlementCycleID.Int64,
			state,
			usedSatoshi,
			occurredAt.Int64,
			confirmedAt,
			note,
			payloadJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("insert BSV consumption %d: %w", id, err)
		}

		stats.MigratedCount++
		stats.TotalSatoshi += usedSatoshi
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate BSV consumptions: %w", err)
	}

	return stats, nil
}

// tokenMigrationStats Token 迁移统计
type tokenMigrationStats struct {
	MigratedCount int64
	ByStandard    map[string]int64 // BSV20/BSV21
}

// migrateTokenConsumptionsTx 迁移 Token 消耗记录。
// 从 fact_asset_consumptions 关联 fact_chain_asset_flows 筛选 asset_kind IN ('BSV20','BSV21') 的记录
func migrateTokenConsumptionsTx(tx *sql.Tx) (*tokenMigrationStats, error) {
	stats := &tokenMigrationStats{
		ByStandard: make(map[string]int64),
	}

	// 查询需要迁移的 Token 消耗记录
	// 注意：used_quantity_text 必须从旧表 ac 取，表示实际消耗数量
	// 而不是从 af 取（af.quantity_text 是流入数量，可能大于实际消耗）
	rows, err := tx.Query(`
		SELECT 
			ac.id,
			ac.source_flow_id,
			ac.source_utxo_id,
			af.asset_kind,
			af.token_id,
			ac.used_quantity_text,
			ac.chain_payment_id,
			ac.pool_allocation_id,
			ac.settlement_cycle_id,
			ac.state,
			ac.occurred_at_unix,
			ac.confirmed_at_unix,
			ac.note,
			ac.payload_json
		FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE af.asset_kind IN ('BSV20', 'BSV21')
		  AND ac.settlement_cycle_id IS NOT NULL
		ORDER BY ac.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query token consumptions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, sourceFlowID int64
		var sourceUTXOID, assetKind, tokenID, quantityText string
		var chainPaymentID, poolAllocationID, settlementCycleID sql.NullInt64
		var state string
		var occurredAt, confirmedAt sql.NullInt64
		var note, payloadJSON string

		if err := rows.Scan(
			&id, &sourceFlowID, &sourceUTXOID,
			&assetKind, &tokenID, &quantityText,
			&chainPaymentID, &poolAllocationID, &settlementCycleID,
			&state, &occurredAt, &confirmedAt,
			&note, &payloadJSON,
		); err != nil {
			return nil, fmt.Errorf("scan token consumption row %d: %w", id, err)
		}

		// Token 标准统一大写
		standard := strings.ToUpper(assetKind)
		if standard != "BSV20" && standard != "BSV21" {
			continue // 跳过非标准类型
		}

		// 使用旧表 ac.used_quantity_text 作为消耗数量（准确反映实际消耗，非流入数量）
		usedQty := quantityText
		if strings.TrimSpace(usedQty) == "" {
			usedQty = "0"
		}

		// 幂等插入
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO fact_token_consumptions(
				consumption_id,
				source_flow_id,
				source_utxo_id,
				token_id,
				token_standard,
				chain_payment_id,
				pool_allocation_id,
				settlement_cycle_id,
				state,
				used_quantity_text,
				occurred_at_unix,
				confirmed_at_unix,
				note,
				payload_json
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			fmt.Sprintf("tokcons_mig_%d", id),
			sourceFlowID,
			sourceUTXOID,
			tokenID,
			standard,
			chainPaymentID,
			poolAllocationID,
			settlementCycleID.Int64,
			state,
			usedQty,
			occurredAt.Int64,
			confirmedAt,
			note,
			payloadJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("insert token consumption %d: %w", id, err)
		}

		stats.MigratedCount++
		stats.ByStandard[standard]++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate token consumptions: %w", err)
	}

	return stats, nil
}

// linkCreationStats Token UTXO 链接创建统计
type linkCreationStats struct {
	CreatedCount int64
	ByStandard   map[string]int64
}

// backfillTokenUTXOLinksTx 从 fact_chain_asset_flows 回填 Token UTXO 链接。
// 为每个 Token 类型的 asset flow 创建对应的 UTXO 映射记录
func backfillTokenUTXOLinksTx(tx *sql.Tx) (*linkCreationStats, error) {
	stats := &linkCreationStats{
		ByStandard: make(map[string]int64),
	}

	// 查询 Token 类型的 asset flows（IN 方向）
	rows, err := tx.Query(`
		SELECT 
			af.id,
			af.wallet_id,
			af.token_id,
			af.asset_kind,
			af.utxo_id,
			af.quantity_text,
			af.txid,
			af.vout,
			af.occurred_at_unix,
			af.evidence_source
		FROM fact_chain_asset_flows af
		WHERE af.asset_kind IN ('BSV20', 'BSV21')
		  AND af.direction = 'IN'
		  AND NOT EXISTS(
			  SELECT 1 FROM fact_token_utxo_links tl
			  WHERE tl.carrier_flow_id = af.id
		  )
		ORDER BY af.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query token flows: %w", err)
	}
	defer rows.Close()

	now := int64(0) // 使用 flow 的 occurred_at_unix 作为 updated_at_unix

	for rows.Next() {
		var id int64
		var walletID, tokenID, assetKind, utxoID, quantityText, txid string
		var vout int
		var occurredAt int64
		var evidenceSource string

		if err := rows.Scan(
			&id, &walletID, &tokenID, &assetKind,
			&utxoID, &quantityText, &txid, &vout,
			&occurredAt, &evidenceSource,
		); err != nil {
			return nil, fmt.Errorf("scan token flow row %d: %w", id, err)
		}

		standard := strings.ToUpper(assetKind)
		if standard != "BSV20" && standard != "BSV21" {
			continue
		}

		if strings.TrimSpace(quantityText) == "" {
			quantityText = "0"
		}

		now = occurredAt

		// 幂等插入：carrier_flow_id 有唯一约束
		_, err := tx.Exec(`
			INSERT OR IGNORE INTO fact_token_utxo_links(
				link_id,
				wallet_id,
				token_id,
				token_standard,
				carrier_flow_id,
				carrier_utxo_id,
				quantity_text,
				direction,
				txid,
				vout,
				occurred_at_unix,
				updated_at_unix,
				evidence_source,
				note,
				payload_json
			) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			fmt.Sprintf("toklink_mig_%d", id),
			walletID,
			tokenID,
			standard,
			id,
			utxoID,
			quantityText,
			"IN",
			txid,
			vout,
			occurredAt,
			now,
			evidenceSource,
			"Auto-created by A-group migration",
			"{}",
		)
		if err != nil {
			return nil, fmt.Errorf("insert token UTXO link for flow %d: %w", id, err)
		}

		stats.CreatedCount++
		stats.ByStandard[standard]++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate token flows: %w", err)
	}

	return stats, nil
}

// validateConservationTx 执行守恒校验。
// 校验项目：
// 1. 迁移后 BSV 条数与源数据一致
// 2. 迁移后 Token 条数与源数据一致
// 3. BSV 总金额守恒
func validateConservationTx(tx *sql.Tx, bsvStats *bsvMigrationStats, tokenStats *tokenMigrationStats) error {
	// 1. 校验 BSV 条数
	var sourceBSVCount int64
	if err := tx.QueryRow(`
		SELECT COUNT(1) FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE af.asset_kind = 'BSV'
	`).Scan(&sourceBSVCount); err != nil {
		return fmt.Errorf("count source BSV rows: %w", err)
	}

	var migratedBSVCount int64
	if err := tx.QueryRow(`
		SELECT COUNT(1) FROM fact_bsv_consumptions 
		WHERE consumption_id LIKE 'bsvcons_mig_%'
	`).Scan(&migratedBSVCount); err != nil {
		return fmt.Errorf("count migrated BSV rows: %w", err)
	}

	if migratedBSVCount != sourceBSVCount {
		return fmt.Errorf("BSV count mismatch: source=%d, migrated=%d", sourceBSVCount, migratedBSVCount)
	}

	// 2. 校验 Token 条数
	var sourceTokenCount int64
	if err := tx.QueryRow(`
		SELECT COUNT(1) FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE af.asset_kind IN ('BSV20', 'BSV21')
	`).Scan(&sourceTokenCount); err != nil {
		return fmt.Errorf("count source token rows: %w", err)
	}

	var migratedTokenCount int64
	if err := tx.QueryRow(`
		SELECT COUNT(1) FROM fact_token_consumptions 
		WHERE consumption_id LIKE 'tokcons_mig_%'
	`).Scan(&migratedTokenCount); err != nil {
		return fmt.Errorf("count migrated token rows: %w", err)
	}

	if migratedTokenCount != sourceTokenCount {
		return fmt.Errorf("token count mismatch: source=%d, migrated=%d", sourceTokenCount, migratedTokenCount)
	}

	// 3. 校验 BSV 金额守恒（如果源数据有 used_satoshi）
	var sourceBSVTotal sql.NullInt64
	if err := tx.QueryRow(`
		SELECT COALESCE(SUM(ac.used_satoshi), 0) FROM fact_asset_consumptions ac
		INNER JOIN fact_chain_asset_flows af ON ac.source_flow_id = af.id
		WHERE af.asset_kind = 'BSV'
	`).Scan(&sourceBSVTotal); err != nil {
		return fmt.Errorf("sum source BSV satoshi: %w", err)
	}

	var migratedBSVTotal sql.NullInt64
	if err := tx.QueryRow(`
		SELECT COALESCE(SUM(used_satoshi), 0) FROM fact_bsv_consumptions 
		WHERE consumption_id LIKE 'bsvcons_mig_%'
	`).Scan(&migratedBSVTotal); err != nil {
		return fmt.Errorf("sum migrated BSV satoshi: %w", err)
	}

	if sourceBSVTotal.Valid && migratedBSVTotal.Valid {
		if sourceBSVTotal.Int64 != migratedBSVTotal.Int64 {
			return fmt.Errorf("BSV satoshi mismatch: source=%d, migrated=%d",
				sourceBSVTotal.Int64, migratedBSVTotal.Int64)
		}
	}

	obs.Debug("bitcast-client", "asset_consumption_conservation_validated", map[string]any{
		"bsv_count":    migratedBSVCount,
		"token_count":  migratedTokenCount,
		"bsv_satoshi":  migratedBSVTotal.Int64,
	})

	return nil
}
