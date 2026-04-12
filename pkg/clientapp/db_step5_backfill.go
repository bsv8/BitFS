package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bsv8/BFTP/pkg/obs"
)

// ============================================================
// 第五步：历史回填工具
// 职责：把旧表历史数据补录到新主线（biz_front_orders -> business -> settlement）
// 原则：
//   - 幂等：重复执行不产生重复数据
//   - 不覆盖：已存在的新主线数据不被覆盖
//   - 宁可 pending：查不到底层事实时留 pending，不瞎补
// ============================================================

// BackfillDomainRegisterHistory 回填域名注册历史到新主线
// 场景：把历史域名注册支付补成 front_order + business + settlement
// 策略：
//   - 查旧表 fact_settlement_channel_chain_quote_pay 中 subtype='domain_register' 的记录
//   - 为每条记录创建对应的 front_order + business + trigger + settlement
//   - settlement 直接指向已有的 chain_payment
func BackfillDomainRegisterHistory(ctx context.Context, store *clientDB) (*BackfillResult, error) {
	result := &BackfillResult{
		BackfillType: "domain_register",
		StartedAt:    time.Now(),
	}
	if store == nil {
		return result, fmt.Errorf("client db is nil")
	}

	err := store.Do(ctx, func(db *sql.DB) error {
		// 查询所有域名注册相关的 fact_settlement_channel_chain_quote_pay，且还没有对应 settle_records 的
		rows, err := QueryContext(ctx, db, `
			SELECT cp.id, cp.txid, cp.payment_subtype, cp.status, cp.net_amount_satoshi,
			       cp.occurred_at_unix, cp.from_party_id, cp.to_party_id, cp.payload_json
			FROM fact_settlement_channel_chain_quote_pay cp
			LEFT JOIN settle_records sr ON sr.settlement_method='chain'
				AND sr.target_type = 'chain_quote_pay'
				AND sr.target_id = CAST(cp.id AS TEXT)
			WHERE cp.payment_subtype = 'domain_register'
				AND sr.settlement_id IS NULL
			ORDER BY cp.id ASC
		`)
		if err != nil {
			return fmt.Errorf("query fact_settlement_channel_chain_quote_pay: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var cp ChainPaymentItem
			var payload string
			if err := rows.Scan(&cp.ID, &cp.TxID, &cp.PaymentSubType, &cp.Status, &cp.NetAmountSatoshi,
				&cp.OccurredAtUnix, &cp.FromPartyID, &cp.ToPartyID, &payload); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("scan chain_payment: %v", err))
				continue
			}
			cp.Payload = []byte(payload)

			// 构造回填数据
			frontOrderID := fmt.Sprintf("fo_backfill_domain_%d", cp.ID)
			businessID := fmt.Sprintf("biz_backfill_domain_%d", cp.ID)
			settlementID := fmt.Sprintf("set_backfill_domain_%d", cp.ID)
			triggerID := fmt.Sprintf("trg_backfill_domain_%d", cp.ID)

			// 检查 front_order 是否已存在
			var existingFO string
			_ = QueryRowContext(ctx, db, `SELECT front_order_id FROM biz_front_orders WHERE front_order_id=?`, frontOrderID).Scan(&existingFO)
			if existingFO != "" {
				result.Skipped++
				continue
			}

			now := time.Now().Unix()

			// 提取 payload 中的域名信息（如果有）
			targetObjectID := "unknown"
			if payload != "" && payload != "{}" {
				// 简单解析，提取 name 字段
				var payloadMap map[string]interface{}
				if err := json.Unmarshal([]byte(payload), &payloadMap); err == nil {
					if name, ok := payloadMap["name"].(string); ok && name != "" {
						targetObjectID = name
					}
				}
			}

			// 1. 创建 front_order
			if _, err := ExecContext(ctx, db, `
				INSERT INTO biz_front_orders(front_order_id, front_type, front_subtype, owner_pubkey_hex, 
					target_object_type, target_object_id, status, created_at_unix, updated_at_unix, note, payload_json)
				VALUES(?, 'domain', 'register', ?, 'domain_name', ?, 'settled', ?, ?, ?, ?)
				ON CONFLICT(front_order_id) DO NOTHING`,
				frontOrderID, cp.FromPartyID, targetObjectID, now, now,
				"历史回填：域名注册",
				fmt.Sprintf(`{"backfill":true,"chain_payment_id":%d,"txid":"%s"}`, cp.ID, cp.TxID),
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert front_order %s: %v", frontOrderID, err))
				continue
			}

			settlementCycleID, err := resolveChainPaymentSourceToSettlementCycleDB(ctx, db, cp.TxID)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("resolve settlement cycle for chain payment %d: %v", cp.ID, err))
				continue
			}
			settlementStatus := "settled"
			if cp.Status != "confirmed" {
				settlementStatus = "pending"
			}

			// 2. 创建 settle_records（第七阶段整改：显式写 business_role='formal'）
			if _, err := ExecContext(ctx, db, `
				INSERT INTO settle_records(business_id, business_role, source_type, source_id, accounting_scene, accounting_subtype,
					from_party_id, to_party_id, status, occurred_at_unix, idempotency_key, note, payload_json,
					settlement_id, settlement_method, settlement_status, target_type, target_id, error_message, settlement_payload_json, created_at_unix, updated_at_unix)
				VALUES(?, 'formal', 'settlement_cycle', ?, 'domain', 'register', ?, ?, 'posted', ?, ?, ?, ?, ?, 'chain', ?, 'chain_quote_pay', ?, '', ?, ?, ?)
				ON CONFLICT(idempotency_key) DO NOTHING`,
				businessID, fmt.Sprintf("%d", settlementCycleID), cp.FromPartyID, cp.ToPartyID, cp.OccurredAtUnix,
				"backfill:"+businessID, "历史回填：域名注册",
				fmt.Sprintf(`{"backfill":true,"chain_payment_id":%d}`, cp.ID),
				settlementID, settlementStatus, fmt.Sprintf("%d", cp.ID), fmt.Sprintf(`{"backfill":true,"chain_payment_id":%d}`, cp.ID), now, now,
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert settle record %s: %v", businessID, err))
				continue
			}

			// 3. 创建 trigger
			if _, err := ExecContext(ctx, db, `
				INSERT INTO biz_business_triggers(trigger_id, business_id, trigger_type, trigger_id_value, trigger_role, created_at_unix, note, payload_json)
				VALUES(?, ?, 'front_order', ?, 'primary', ?, '历史回填', ?)
				ON CONFLICT(business_id, trigger_type, trigger_id_value, trigger_role) DO NOTHING`,
				triggerID, businessID, frontOrderID, now,
				`{"backfill":true}`,
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert trigger %s: %v", triggerID, err))
				continue
			}

			result.Success++
			obs.Info("bitcast-client", "backfill_domain_register_success", map[string]any{
				"front_order_id":  frontOrderID,
				"business_id":     businessID,
				"chain_quote_pay": cp.TxID[:8] + "..." + cp.TxID[len(cp.TxID)-4:],
			})
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate fact_settlement_channel_chain_quote_pay: %w", err)
		}
		return nil
	})

	result.FinishedAt = time.Now()
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	return result, err
}

// BackfillPoolAllocationHistory 回填池支付历史到新主线
//
// 整改要点（第五步）：
// 1. 粒度对齐：不按每条 allocation 一条 business，而是按 seller 级收费事实回填
//   - 一个 seller 一次下载收费 = 一条 business = 一条 settlement
//   - 只用第一次成功的 pay allocation 代表这条 seller 收费
//
// 2. 补齐前台主链：必须创建 front_order + business_trigger
//   - 回填后的历史必须能通过 GetFrontOrderSettlementSummary 查到
//
// 3. 幂等判断：以 settlement 存在为主要完成标志，逐对象补齐，支持半残修复
//   - 不是 "front_order 存在就整条跳过"
//   - 而是 "settlement 存在才算完成，否则逐层补齐"
//
// 策略：
//   - 按 pool_session_id 分组，找到每个 session 的第一次 pay allocation
//   - 为每个 session 创建完整的主链：front_order -> trigger -> business -> settlement
//   - settlement 指向那条 pay allocation 的 id
//   - 同步回填 biz_pool / biz_pool_allocations，作为新业务快照
func backfillBizPoolFacts(ctx context.Context, db *sql.DB) error {
	sessions, err := QueryContext(ctx, db, `
		SELECT pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
		       pool_amount_satoshi,spend_tx_fee_satoshi,fee_rate_sat_byte,lock_blocks,open_base_txid,status,created_at_unix,updated_at_unix
		FROM fact_settlement_channel_pool_session_quote_pay
		ORDER BY created_at_unix ASC, pool_session_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query fact_settlement_channel_pool_session_quote_pay: %w", err)
	}
	defer sessions.Close()

	for sessions.Next() {
		var session PoolSessionItem
		if err := sessions.Scan(
			&session.PoolSessionID, &session.PoolScheme, &session.CounterpartyPubHex, &session.SellerPubHex, &session.ArbiterPubHex,
			&session.GatewayPubHex, &session.PoolAmountSat, &session.SpendTxFeeSat, &session.FeeRateSatByte, &session.LockBlocks,
			&session.OpenBaseTxID, &session.Status, &session.CreatedAtUnix, &session.UpdatedAtUnix,
		); err != nil {
			return fmt.Errorf("scan fact_settlement_channel_pool_session_quote_pay: %w", err)
		}

		events, err := QueryContext(ctx, db, `
			SELECT allocation_id,allocation_no,allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
			FROM fact_pool_session_events
			WHERE pool_session_id=? AND event_kind=?
			ORDER BY allocation_no ASC
		`, session.PoolSessionID, PoolFactEventKindPoolEvent)
		if err != nil {
			return fmt.Errorf("query fact_pool_session_events for %s: %w", session.PoolSessionID, err)
		}

		var (
			lastSequence      uint32
			lastPayeeAfter    uint64
			lastPayerAfter    uint64
			openAllocationID  string
			closeAllocationID string
			haveEvent         bool
		)
		for events.Next() {
			var item struct {
				AllocationID     string
				AllocationNo     int64
				AllocationKind   string
				SequenceNum      uint32
				PayeeAmountAfter uint64
				PayerAmountAfter uint64
				TxID             string
				TxHex            string
				CreatedAtUnix    int64
			}
			if err := events.Scan(
				&item.AllocationID, &item.AllocationNo, &item.AllocationKind, &item.SequenceNum,
				&item.PayeeAmountAfter, &item.PayerAmountAfter, &item.TxID, &item.TxHex, &item.CreatedAtUnix,
			); err != nil {
				_ = events.Close()
				return fmt.Errorf("scan fact_pool_session_events for %s: %w", session.PoolSessionID, err)
			}
			haveEvent = true
			lastSequence = item.SequenceNum
			lastPayeeAfter = item.PayeeAmountAfter
			lastPayerAfter = item.PayerAmountAfter
			switch strings.TrimSpace(item.AllocationKind) {
			case PoolBusinessActionOpen:
				if openAllocationID == "" {
					openAllocationID = item.AllocationID
				}
			case PoolBusinessActionClose:
				closeAllocationID = item.AllocationID
			}
			if _, err := ExecContext(ctx, db, `
				INSERT INTO biz_pool_allocations(
					allocation_id,pool_session_id,allocation_no,allocation_kind,sequence_num,payee_amount_after,payer_amount_after,txid,tx_hex,created_at_unix
				) VALUES(?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(allocation_id) DO UPDATE SET
					pool_session_id=excluded.pool_session_id,
					allocation_kind=excluded.allocation_kind,
					sequence_num=excluded.sequence_num,
					payee_amount_after=excluded.payee_amount_after,
					payer_amount_after=excluded.payer_amount_after,
					txid=excluded.txid,
					tx_hex=excluded.tx_hex,
					created_at_unix=excluded.created_at_unix`,
				item.AllocationID, session.PoolSessionID, item.AllocationNo, item.AllocationKind, item.SequenceNum,
				item.PayeeAmountAfter, item.PayerAmountAfter, strings.ToLower(strings.TrimSpace(item.TxID)), strings.ToLower(strings.TrimSpace(item.TxHex)), item.CreatedAtUnix,
			); err != nil {
				_ = events.Close()
				return fmt.Errorf("upsert biz_pool_allocations for %s: %w", item.AllocationID, err)
			}
		}
		if err := events.Err(); err != nil {
			_ = events.Close()
			return fmt.Errorf("iterate fact_pool_session_events for %s: %w", session.PoolSessionID, err)
		}
		_ = events.Close()

		nextSequence := uint32(1)
		if haveEvent {
			nextSequence = lastSequence + 1
		}
		poolAmount := uint64(0)
		if session.PoolAmountSat > 0 {
			poolAmount = uint64(session.PoolAmountSat)
		}
		if session.SpendTxFeeSat > 0 {
			poolAmount += uint64(session.SpendTxFeeSat)
		}
		if _, err := ExecContext(ctx, db, `
			INSERT INTO biz_pool(
				pool_session_id,pool_scheme,counterparty_pubkey_hex,seller_pubkey_hex,arbiter_pubkey_hex,gateway_pubkey_hex,
				pool_amount_satoshi,spend_tx_fee_satoshi,allocated_satoshi,cycle_fee_satoshi,available_satoshi,next_sequence_num,
				status,open_base_txid,open_allocation_id,close_allocation_id,created_at_unix,updated_at_unix
			) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(pool_session_id) DO UPDATE SET
				pool_scheme=excluded.pool_scheme,
				counterparty_pubkey_hex=excluded.counterparty_pubkey_hex,
				seller_pubkey_hex=excluded.seller_pubkey_hex,
				arbiter_pubkey_hex=excluded.arbiter_pubkey_hex,
				gateway_pubkey_hex=excluded.gateway_pubkey_hex,
				pool_amount_satoshi=excluded.pool_amount_satoshi,
				spend_tx_fee_satoshi=excluded.spend_tx_fee_satoshi,
				allocated_satoshi=excluded.allocated_satoshi,
				cycle_fee_satoshi=excluded.cycle_fee_satoshi,
				available_satoshi=excluded.available_satoshi,
				next_sequence_num=excluded.next_sequence_num,
				status=excluded.status,
				open_base_txid=CASE WHEN excluded.open_base_txid<>'' THEN excluded.open_base_txid ELSE biz_pool.open_base_txid END,
				open_allocation_id=CASE WHEN excluded.open_allocation_id<>'' THEN excluded.open_allocation_id ELSE biz_pool.open_allocation_id END,
				close_allocation_id=CASE WHEN excluded.close_allocation_id<>'' THEN excluded.close_allocation_id ELSE biz_pool.close_allocation_id END,
				updated_at_unix=excluded.updated_at_unix
			WHERE excluded.next_sequence_num >= biz_pool.next_sequence_num`,
			session.PoolSessionID,
			session.PoolScheme,
			strings.ToLower(strings.TrimSpace(session.CounterpartyPubHex)),
			strings.ToLower(strings.TrimSpace(session.SellerPubHex)),
			strings.ToLower(strings.TrimSpace(session.ArbiterPubHex)),
			strings.ToLower(strings.TrimSpace(session.GatewayPubHex)),
			poolAmount,
			uint64(session.SpendTxFeeSat),
			lastPayeeAfter,
			uint64(session.SpendTxFeeSat),
			lastPayerAfter,
			nextSequence,
			session.Status,
			strings.ToLower(strings.TrimSpace(session.OpenBaseTxID)),
			openAllocationID,
			closeAllocationID,
			session.CreatedAtUnix,
			session.UpdatedAtUnix,
		); err != nil {
			return fmt.Errorf("upsert biz_pool for %s: %w", session.PoolSessionID, err)
		}
	}

	if err := sessions.Err(); err != nil {
		return fmt.Errorf("iterate fact_settlement_channel_pool_session_quote_pay: %w", err)
	}
	return nil
}

func BackfillPoolAllocationHistory(ctx context.Context, store *clientDB) (*BackfillResult, error) {
	result := &BackfillResult{
		BackfillType: "pool_pay_per_seller",
		StartedAt:    time.Now(),
	}
	if store == nil {
		return result, fmt.Errorf("client db is nil")
	}

	err := store.Do(ctx, func(db *sql.DB) error {
		if err := backfillBizPoolFacts(ctx, db); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("backfill biz pool facts: %v", err))
		}
		// 查询每个 pool_session 的第一次 pay allocation
		// 粒度对齐：一个 session 一次下载 = 一条 business（只用第一次 pay）
		rows, err := QueryContext(ctx, db, `
			SELECT 
				pa.id as pay_allocation_id,
				pa.pool_session_id,
				pa.txid,
				pa.payee_amount_after,
				pa.payer_amount_after,
				pa.created_at_unix,
				ps.seller_pubkey_hex,
				ps.counterparty_pubkey_hex,
				ps.pool_amount_satoshi
			FROM fact_pool_session_events pa
			JOIN fact_settlement_channel_pool_session_quote_pay ps ON ps.pool_session_id = pa.pool_session_id
			WHERE pa.event_kind = '`+PoolFactEventKindPoolEvent+`'
				AND pa.allocation_kind = '`+PoolBusinessActionPayLegacy+`'
				AND pa.id = (
					-- 取该 session 的第一次 pay allocation
					SELECT MIN(id) FROM fact_pool_session_events 
					WHERE pool_session_id = pa.pool_session_id 
					AND event_kind = '`+PoolFactEventKindPoolEvent+`'
					AND allocation_kind = '`+PoolBusinessActionPayLegacy+`'
				)
			ORDER BY pa.id ASC
		`)
		if err != nil {
			return fmt.Errorf("query fact_pool_session_events: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var payAlloc struct {
				ID               int64
				PoolSessionID    string
				TxID             string
				PayeeAmountAfter uint64
				PayerAmountAfter uint64
				CreatedAtUnix    int64
				SellerPubHex     string
				BuyerPubHex      string
				PoolAmountSat    int64
			}
			if err := rows.Scan(&payAlloc.ID, &payAlloc.PoolSessionID, &payAlloc.TxID,
				&payAlloc.PayeeAmountAfter, &payAlloc.PayerAmountAfter, &payAlloc.CreatedAtUnix,
				&payAlloc.SellerPubHex, &payAlloc.BuyerPubHex, &payAlloc.PoolAmountSat); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("scan pay allocation: %v", err))
				continue
			}

			// 构造回填数据（seller 级）
			frontOrderID := fmt.Sprintf("fo_backfill_pool_%s", payAlloc.PoolSessionID)
			businessID := fmt.Sprintf("biz_backfill_pool_%s", payAlloc.PoolSessionID)
			settlementID := fmt.Sprintf("set_backfill_pool_%s", payAlloc.PoolSessionID)
			triggerID := fmt.Sprintf("trg_backfill_pool_%s", payAlloc.PoolSessionID)

			// 1. 先查 settlement：以 settlement 存在作为主要完成标志
			var existingSettlement string
			err := QueryRowContext(ctx, db, `
				SELECT settlement_id FROM settle_records 
				WHERE settlement_method = 'pool' AND target_id = ?`,
				fmt.Sprintf("%d", payAlloc.ID),
			).Scan(&existingSettlement)
			if err == nil && existingSettlement != "" {
				// settlement 已存在且 target 正确，认为这条历史已完整回填
				result.Skipped++
				continue
			}

			now := time.Now().Unix()
			completed := true

			// 2. 逐对象补齐：front_order（幂等）
			if _, err := ExecContext(ctx, db, `
				INSERT INTO biz_front_orders(front_order_id, front_type, front_subtype, owner_pubkey_hex, 
					target_object_type, target_object_id, status, created_at_unix, updated_at_unix, note, payload_json)
				VALUES(?, 'download', 'direct_transfer', ?, 'pool_session_quote_pay', ?, 'settled', ?, ?, ?, ?)
				ON CONFLICT(front_order_id) DO NOTHING`,
				frontOrderID, payAlloc.BuyerPubHex, payAlloc.PoolSessionID, now, now,
				"历史回填：池支付",
				fmt.Sprintf(`{"backfill":true,"pool_session_id":"%s","pay_allocation_id":%d}`, payAlloc.PoolSessionID, payAlloc.ID),
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert front_order %s: %v", frontOrderID, err))
				completed = false
			}

			settlementCycleID, err := resolvePoolAllocationSourceToSettlementCycleDB(ctx, db, fmt.Sprintf("%d", payAlloc.ID))
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("resolve settlement cycle for pool allocation %d: %v", payAlloc.ID, err))
				completed = false
				continue
			}

			// 3. 逐对象补齐：直接写入单条 settle_records
			settlementStatus := "settled"
			settlementTargetType := "pool_allocation"
			settlementTargetID := fmt.Sprintf("%d", payAlloc.ID)
			if _, err := ExecContext(ctx, db, `
				INSERT INTO settle_records(business_id, settlement_id, business_role, source_type, source_id, accounting_scene, accounting_subtype,
					from_party_id, to_party_id, status, occurred_at_unix, idempotency_key, note, payload_json,
					settlement_method, settlement_status, target_type, target_id, settlement_payload_json, created_at_unix, updated_at_unix)
				VALUES(?, ?, 'formal', 'settlement_cycle', ?, 'direct_transfer', 'pay', ?, ?, 'posted', ?, ?, ?, ?, 'pool', ?, ?, ?, ?, ?, ?)
				ON CONFLICT(idempotency_key) DO NOTHING`,
				businessID, settlementID, fmt.Sprintf("%d", settlementCycleID), payAlloc.BuyerPubHex, payAlloc.SellerPubHex, payAlloc.CreatedAtUnix,
				"backfill:"+businessID,
				"历史回填：池支付",
				fmt.Sprintf(`{"backfill":true,"pool_session_id":"%s","pay_amount":%d}`, payAlloc.PoolSessionID, payAlloc.PayeeAmountAfter),
				settlementStatus, settlementTargetType, settlementTargetID,
				fmt.Sprintf(`{"backfill":true,"pool_session_id":"%s","pay_allocation_id":%d}`, payAlloc.PoolSessionID, payAlloc.ID),
				now, now,
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert business %s: %v", businessID, err))
				completed = false
			}

			// 4. 逐对象补齐：trigger（幂等）
			if _, err := ExecContext(ctx, db, `
				INSERT INTO biz_business_triggers(trigger_id, business_id, trigger_type, trigger_id_value, trigger_role, created_at_unix, note, payload_json)
				VALUES(?, ?, 'front_order', ?, 'primary', ?, '历史回填', ?)
				ON CONFLICT(business_id, trigger_type, trigger_id_value, trigger_role) DO NOTHING`,
				triggerID, businessID, frontOrderID, now,
				`{"backfill":true}`,
			); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("insert trigger %s: %v", triggerID, err))
				completed = false
			}

			if completed {
				result.Success++
				obs.Info("bitcast-client", "backfill_pool_pay_success", map[string]any{
					"front_order_id":         frontOrderID,
					"business_id":            businessID,
					"pool_session_quote_pay": payAlloc.PoolSessionID[:8] + "...",
					"pay_amount":             payAlloc.PayeeAmountAfter,
				})
			}
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate fact_pool_session_events: %w", err)
		}
		return nil
	})

	result.FinishedAt = time.Now()
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	return result, err
}

// BackfillResult 回填结果统计
type BackfillResult struct {
	BackfillType string        `json:"backfill_type"`
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at"`
	Success      int           `json:"success_count"`
	Skipped      int           `json:"skipped_count"`
	Errors       []string      `json:"errors,omitempty"`
	Duration     time.Duration `json:"duration_ms"`
}

// RunAllBackfills 运行所有回填任务
func RunAllBackfills(ctx context.Context, store *clientDB) map[string]*BackfillResult {
	results := make(map[string]*BackfillResult)

	// 1. 回填域名注册历史
	if result, err := BackfillDomainRegisterHistory(ctx, store); err != nil {
		obs.Error("bitcast-client", "backfill_domain_register_failed", map[string]any{"error": err.Error()})
		results["domain_register"] = result
	} else {
		results["domain_register"] = result
	}

	// 2. 回填池支付历史（按 seller 级粒度）
	if result, err := BackfillPoolAllocationHistory(ctx, store); err != nil {
		obs.Error("bitcast-client", "backfill_pool_allocation_failed", map[string]any{"error": err.Error()})
		results["pool_allocation"] = result
	} else {
		results["pool_allocation"] = result
	}

	return results
}
