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

// ensureClientDBBaseSchema 创建基础表和索引。
// 只负责 CREATE TABLE IF NOT EXISTS 和 CREATE INDEX IF NOT EXISTS，
// 不包含任何依赖前置迁移条件的操作。
func ensureClientDBBaseSchemaCtx(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	stmts := []string{
		// 核心表
		`CREATE TABLE IF NOT EXISTS biz_workspaces(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seeds(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS biz_workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path),
			FOREIGN KEY(workspace_path) REFERENCES biz_workspaces(workspace_path) ON DELETE CASCADE,
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seed_chunk_supply(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index),
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seed_pricing_policy(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS biz_demands(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id)
		)`,

		`CREATE TABLE IF NOT EXISTS biz_purchases(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pub_hex TEXT NOT NULL,
			arbiter_pub_hex TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			object_hash TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			finished_at_unix INTEGER NOT NULL,
			FOREIGN KEY(demand_id) REFERENCES biz_demands(demand_id) ON DELETE CASCADE
		)`,
		// 事件必须挂到真实命令上，历史脏行在迁移阶段先清掉。
		`CREATE TABLE IF NOT EXISTS proc_gateway_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			command_id TEXT NOT NULL,
			action TEXT NOT NULL,
			msg_id TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			pool_id TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
		)`,

		// 直接交易
		`CREATE TABLE IF NOT EXISTS biz_demand_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pub_hex TEXT NOT NULL,
			seed_price_satoshi INTEGER NOT NULL,
			chunk_price_satoshi INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL,
			file_size_bytes INTEGER NOT NULL,
			recommended_file_name TEXT NOT NULL,
			mime_type TEXT NOT NULL,
			available_chunk_bitmap_hex TEXT NOT NULL,
			expires_at_unix INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			FOREIGN KEY(demand_id) REFERENCES biz_demands(demand_id) ON DELETE CASCADE,
			UNIQUE(demand_id, seller_pub_hex)
		)`,
		`CREATE TABLE IF NOT EXISTS biz_demand_quote_arbiters(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			quote_id INTEGER NOT NULL,
			arbiter_pub_hex TEXT NOT NULL,
			FOREIGN KEY(quote_id) REFERENCES biz_demand_quotes(id) ON DELETE CASCADE,
			UNIQUE(quote_id, arbiter_pub_hex)
		)`,
		// 第五步定性：proc_direct_deals 是【协议过程对象】
		// - 职责：保存协议协商/成交上下文（buyer/seller/seed_hash/price 等）
		// - 非支付主事实，不决定业务是否完成
		// - 业务完成状态以 settle_records 为准
		`CREATE TABLE IF NOT EXISTS proc_direct_deals(
			deal_id TEXT PRIMARY KEY,
			demand_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			arbiter_pubkey_hex TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS biz_live_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			stream_id TEXT NOT NULL,
			latest_segment_index INTEGER NOT NULL,
			recent_segments_json TEXT NOT NULL,
			expires_at_unix INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id, seller_pubkey_hex)
		)`,
	}
	stmts = append(stmts, bizPoolSchemaStmts()...)
	stmts = append(stmts, []string{
		// 第五步定性：proc_direct_transfer_pools 是【运行态池状态表】
		// - 职责：保存池协议运行期的动态状态（sequence_num、current_tx_hex、status 等）
		// - 非业务主判断入口，只服务于协议运行期
		// - 业务完成状态以 settle_records 为准，不以此表的 status 为准
		`CREATE TABLE IF NOT EXISTS proc_direct_transfer_pools(
			session_id TEXT PRIMARY KEY,
			deal_id TEXT NOT NULL,
			buyer_pubkey_hex TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			arbiter_pubkey_hex TEXT NOT NULL,
			pool_amount INTEGER NOT NULL,
			spend_tx_fee INTEGER NOT NULL,
			sequence_num INTEGER NOT NULL,
			seller_amount INTEGER NOT NULL,
			buyer_amount INTEGER NOT NULL,
			current_tx_hex TEXT NOT NULL,
			base_tx_hex TEXT NOT NULL,
			base_txid TEXT NOT NULL,
			status TEXT NOT NULL, -- 运行态状态，非业务完成状态
			fee_rate_sat_byte REAL NOT NULL,
			lock_blocks INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		// 直连传输费用池事实层：只保留真实池会话与会话事件，不承载运行时快照职责。
		`CREATE TABLE IF NOT EXISTS fact_settlement_channel_pool_session_quote_pay(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement_cycle_id INTEGER NOT NULL UNIQUE,
			pool_session_id TEXT NOT NULL UNIQUE,
			txid TEXT NOT NULL DEFAULT '',
			pool_scheme TEXT NOT NULL,
			counterparty_pubkey_hex TEXT NOT NULL DEFAULT '',
			seller_pubkey_hex TEXT NOT NULL DEFAULT '',
			arbiter_pubkey_hex TEXT NOT NULL DEFAULT '',
			gateway_pubkey_hex TEXT NOT NULL DEFAULT '',
			pool_amount_satoshi INTEGER NOT NULL,
			spend_tx_fee_satoshi INTEGER NOT NULL,
			fee_rate_sat_byte REAL NOT NULL DEFAULT 0,
			lock_blocks INTEGER NOT NULL DEFAULT 0,
			open_base_txid TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id)
		)`,
		`CREATE TABLE IF NOT EXISTS fact_pool_session_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			allocation_id TEXT NOT NULL,
			pool_session_id TEXT NOT NULL DEFAULT '',
			allocation_no INTEGER NOT NULL DEFAULT 0,
			allocation_kind TEXT NOT NULL DEFAULT '',
			event_kind TEXT NOT NULL DEFAULT '` + PoolFactEventKindPoolEvent + `',
			sequence_num INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL DEFAULT 'confirmed',
			direction TEXT NOT NULL DEFAULT '',
			amount_satoshi INTEGER NOT NULL DEFAULT 0,
			purpose TEXT NOT NULL DEFAULT '',
			note TEXT NOT NULL DEFAULT '',
			msg_id TEXT NOT NULL DEFAULT '',
			cycle_index INTEGER NOT NULL DEFAULT 0,
			payee_amount_after INTEGER NOT NULL DEFAULT 0,
			payer_amount_after INTEGER NOT NULL DEFAULT 0,
			txid TEXT NOT NULL DEFAULT '',
			tx_hex TEXT NOT NULL DEFAULT '',
			gateway_pubkey_hex TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}',
			UNIQUE(allocation_id)
		)`,
		`CREATE TABLE IF NOT EXISTS fact_settlement_channel_chain_quote_pay(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement_cycle_id INTEGER NOT NULL UNIQUE,
			txid TEXT NOT NULL,
			payment_subtype TEXT NOT NULL,
			status TEXT NOT NULL,
			wallet_input_satoshi INTEGER NOT NULL,
			wallet_output_satoshi INTEGER NOT NULL,
			net_amount_satoshi INTEGER NOT NULL,
			block_height INTEGER NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL DEFAULT 0,
			wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id),
			UNIQUE(txid)
		)`,
		`CREATE TABLE IF NOT EXISTS fact_settlement_channel_chain_direct_pay(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement_cycle_id INTEGER NOT NULL UNIQUE,
			txid TEXT NOT NULL,
			payment_subtype TEXT NOT NULL,
			status TEXT NOT NULL,
			wallet_input_satoshi INTEGER NOT NULL,
			wallet_output_satoshi INTEGER NOT NULL,
			net_amount_satoshi INTEGER NOT NULL,
			block_height INTEGER NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL DEFAULT 0,
			wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id),
			UNIQUE(txid)
		)`,
		`CREATE TABLE IF NOT EXISTS fact_settlement_channel_chain_asset_create(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			settlement_cycle_id INTEGER NOT NULL UNIQUE,
			txid TEXT NOT NULL,
			payment_subtype TEXT NOT NULL,
			status TEXT NOT NULL,
			wallet_input_satoshi INTEGER NOT NULL,
			wallet_output_satoshi INTEGER NOT NULL,
			net_amount_satoshi INTEGER NOT NULL,
			block_height INTEGER NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL DEFAULT 0,
			wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0,
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id),
			UNIQUE(txid)
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_local_broadcast_txs(
			txid TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			tx_hex TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			observed_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_pool_session_events_session_kind_seq ON fact_pool_session_events(pool_session_id,allocation_kind,sequence_num) WHERE event_kind='` + PoolFactEventKindPoolEvent + `'`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_pool_session_quote_pay_scheme_status ON fact_settlement_channel_pool_session_quote_pay(pool_scheme,status,updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_pool_session_quote_pay_counterparty ON fact_settlement_channel_pool_session_quote_pay(counterparty_pubkey_hex,status)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_pool_session_quote_pay_txid ON fact_settlement_channel_pool_session_quote_pay(txid)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_session_no ON fact_pool_session_events(pool_session_id,allocation_no DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_kind_seq ON fact_pool_session_events(pool_session_id,event_kind,sequence_num)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_txid ON fact_pool_session_events(txid)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_created ON fact_pool_session_events(created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_local_broadcast_txs_wallet_observed ON wallet_local_broadcast_txs(wallet_id, observed_at_unix, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_quote_pay_occurred ON fact_settlement_channel_chain_quote_pay(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_quote_pay_subtype ON fact_settlement_channel_chain_quote_pay(payment_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_quote_pay_status ON fact_settlement_channel_chain_quote_pay(status, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_direct_pay_occurred ON fact_settlement_channel_chain_direct_pay(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_direct_pay_subtype ON fact_settlement_channel_chain_direct_pay(payment_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_direct_pay_status ON fact_settlement_channel_chain_direct_pay(status, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_asset_create_occurred ON fact_settlement_channel_chain_asset_create(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_asset_create_subtype ON fact_settlement_channel_chain_asset_create(payment_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_channel_chain_asset_create_status ON fact_settlement_channel_chain_asset_create(status, occurred_at_unix DESC)`,

		// 命令日志
		// trigger_key 设计说明：表示"这次命令执行是被哪一条上游触发链路推出来的"
		// - orchestrator 发起时，trigger_key = orchestrator.idempotency_key
		// - 非 orchestrator 发起时，trigger_key = ''
		// 注意：这不是外键，不做 FK 约束；也不是 command_id，而是来源链路键。
		// command_id 是唯一命令号，proc_gateway_events 会直接用它做物理外键。
		`CREATE TABLE IF NOT EXISTS proc_command_journal(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			requested_at_unix INTEGER NOT NULL,
			accepted INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_code TEXT NOT NULL,
			error_message TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_key TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
			gateway_pubkey_hex TEXT NOT NULL,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
		)`,
		`CREATE TABLE IF NOT EXISTS proc_state_snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
			gateway_pubkey_hex TEXT NOT NULL,
			state TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
		)`,
		// 观察事实表
		// 设计说明：这里只承接被动观察到的网关状态，不挂 command_id，不混命令链。
		`CREATE TABLE IF NOT EXISTS proc_observed_gateway_states(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			source_ref TEXT NOT NULL,
			observed_at_unix INTEGER NOT NULL,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_effect_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
			gateway_pubkey_hex TEXT NOT NULL,
			effect_type TEXT NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
		)`,

		// 编排器日志
		`CREATE TABLE IF NOT EXISTS proc_orchestrator_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			source TEXT NOT NULL,
			signal_type TEXT NOT NULL,
			aggregate_key TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			command_type TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			task_status TEXT NOT NULL,
			retry_count INTEGER NOT NULL,
			queue_length INTEGER NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,

		// UTXO 管理
		`CREATE TABLE IF NOT EXISTS wallet_utxo(
			utxo_id TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			value_satoshi INTEGER NOT NULL,
			state TEXT NOT NULL,
			allocation_class TEXT NOT NULL DEFAULT 'plain_bsv',
			allocation_reason TEXT NOT NULL DEFAULT '',
			created_txid TEXT NOT NULL,
			spent_txid TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			spent_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fact_bsv21(
			token_id TEXT PRIMARY KEY,
			create_txid TEXT NOT NULL,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			token_standard TEXT NOT NULL,
			symbol TEXT NOT NULL,
			max_supply TEXT NOT NULL,
			decimals INTEGER NOT NULL,
			icon TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE IF NOT EXISTS fact_bsv21_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_id TEXT NOT NULL,
			event_kind TEXT NOT NULL,
			event_at_unix INTEGER NOT NULL,
			txid TEXT NOT NULL DEFAULT '',
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_sync_state(
			address TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			utxo_count INTEGER NOT NULL,
			balance_satoshi INTEGER NOT NULL,
			plain_bsv_utxo_count INTEGER NOT NULL DEFAULT 0,
			plain_bsv_balance_satoshi INTEGER NOT NULL DEFAULT 0,
			protected_utxo_count INTEGER NOT NULL DEFAULT 0,
			protected_balance_satoshi INTEGER NOT NULL DEFAULT 0,
			unknown_utxo_count INTEGER NOT NULL DEFAULT 0,
			unknown_balance_satoshi INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			last_updated_by TEXT NOT NULL,
			last_trigger TEXT NOT NULL,
			last_duration_ms INTEGER NOT NULL,
			last_sync_round_id TEXT NOT NULL DEFAULT '',
			last_failed_step TEXT NOT NULL DEFAULT '',
			last_upstream_path TEXT NOT NULL DEFAULT '',
			last_http_status INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_sync_cursor(
			address TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			next_confirmed_height INTEGER NOT NULL,
			next_page_token TEXT NOT NULL,
			anchor_height INTEGER NOT NULL,
			round_tip_height INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL
		)`,

		// 财务业务
		// 第六次迭代起新库 schema 不再定义旧字段（老库兼容迁移未做物理删列）
		// 旧列迁移由 ensureFinAccountingSchema 处理
		//
		// settle_records 语义说明（一次性收口）：
		// - 一行同时承载业务事实和结算出口；
		// - business_id 是主键，idempotency_key 必须唯一；
		// - settlement_method='chain' 时，target_type/target_id 只能指向事实层主键，不允许写 txid。
		`CREATE TABLE IF NOT EXISTS settle_records(
			business_id TEXT PRIMARY KEY,
			settlement_id TEXT NOT NULL UNIQUE,
			business_role TEXT NOT NULL DEFAULT '' CHECK(business_role IN ('', 'formal', 'process')),
			source_type TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			accounting_scene TEXT NOT NULL DEFAULT '',
			accounting_subtype TEXT NOT NULL DEFAULT '',
			from_party_id TEXT NOT NULL DEFAULT '',
			to_party_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT '',
			occurred_at_unix INTEGER NOT NULL DEFAULT 0,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			settlement_method TEXT NOT NULL DEFAULT '',
			settlement_status TEXT NOT NULL DEFAULT '',
			target_type TEXT NOT NULL DEFAULT '',
			target_id TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '',
			settlement_payload_json TEXT NOT NULL DEFAULT '{}',
			created_at_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS settle_process_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			process_id TEXT NOT NULL,
			source_type TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			accounting_scene TEXT NOT NULL DEFAULT '',
			accounting_subtype TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		// 前台业务主身份层（第七次迭代新增）
		// 职责：表达前台业务主身份，不直接承载支付实现
		`CREATE TABLE IF NOT EXISTS biz_front_orders(
			front_order_id TEXT PRIMARY KEY,
			front_type TEXT NOT NULL,
			front_subtype TEXT NOT NULL,
			owner_pubkey_hex TEXT NOT NULL,
			target_object_type TEXT NOT NULL,
			target_object_id TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		// 前台到财务桥接层（第七次迭代新增）
		// 职责：表达"哪个前台主对象触发了哪条财务事实"
		// 设计说明：
		//   - 本阶段不强绑 biz_front_orders 外键，允许旧对象直接触发 business
		//   - 支持"一前台单多条 business"：同一 trigger_type+trigger_id_value 可触发多个不同 business
		//   - 幂等约束在 (business_id, trigger_type, trigger_id_value, trigger_role) 上
		`CREATE TABLE IF NOT EXISTS biz_business_triggers(
			trigger_id TEXT PRIMARY KEY,
			business_id TEXT NOT NULL,
			trigger_type TEXT NOT NULL,
			trigger_id_value TEXT NOT NULL,
			trigger_role TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(business_id) REFERENCES settle_records(business_id) ON DELETE CASCADE,
			UNIQUE(business_id, trigger_type, trigger_id_value, trigger_role)
		)`,

		// 链状态
		`CREATE TABLE IF NOT EXISTS proc_chain_tip_state(
			id INTEGER PRIMARY KEY CHECK(id=1),
			tip_height INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			last_updated_by TEXT NOT NULL,
			last_trigger TEXT NOT NULL,
			last_duration_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_chain_tip_worker_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			triggered_at_unix INTEGER NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_source TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_chain_utxo_worker_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			triggered_at_unix INTEGER NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			trigger_source TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,

		// 调度器
		`CREATE TABLE IF NOT EXISTS proc_scheduler_tasks(
			task_name TEXT PRIMARY KEY,
			owner TEXT NOT NULL,
			mode TEXT NOT NULL,
			status TEXT NOT NULL,
			interval_seconds INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			closed_at_unix INTEGER NOT NULL,
			last_trigger TEXT NOT NULL,
			last_started_at_unix INTEGER NOT NULL,
			last_ended_at_unix INTEGER NOT NULL,
			last_duration_ms INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			in_flight INTEGER NOT NULL,
			run_count INTEGER NOT NULL,
			success_count INTEGER NOT NULL,
			failure_count INTEGER NOT NULL,
			last_summary_json TEXT NOT NULL,
			meta_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_scheduler_task_runs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			task_name TEXT NOT NULL,
			owner TEXT NOT NULL,
			mode TEXT NOT NULL,
			trigger TEXT NOT NULL,
			started_at_unix INTEGER NOT NULL,
			ended_at_unix INTEGER NOT NULL,
			duration_ms INTEGER NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			summary_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,

		// 直播相关
		`CREATE TABLE IF NOT EXISTS proc_live_follows(
			stream_id TEXT PRIMARY KEY,
			stream_uri TEXT NOT NULL,
			publisher_pubkey TEXT NOT NULL,
			have_segment_index INTEGER NOT NULL,
			last_bought_segment_index INTEGER NOT NULL,
			last_bought_seed_hash TEXT NOT NULL,
			last_output_file_path TEXT NOT NULL,
			last_quote_seller_pubkey_hex TEXT NOT NULL,
			last_decision_json TEXT NOT NULL,
			status TEXT NOT NULL,
			last_error TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,

		// 文件下载
		`CREATE TABLE IF NOT EXISTS proc_file_downloads(
			seed_hash TEXT PRIMARY KEY,
			file_path TEXT NOT NULL,
			file_size INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL,
			completed_chunks INTEGER NOT NULL,
			paid_sats INTEGER NOT NULL,
			status TEXT NOT NULL,
			demand_id TEXT NOT NULL,
			last_error TEXT NOT NULL,
			status_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_file_download_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			status TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			price_sats INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`,

		// 消息收件箱
		`CREATE TABLE IF NOT EXISTS proc_inbox_messages(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT NOT NULL,
			sender_pubkey_hex TEXT NOT NULL,
			target_input TEXT NOT NULL,
			route TEXT NOT NULL,
			content_type TEXT NOT NULL,
			body_bytes BLOB NOT NULL,
			body_size_bytes INTEGER NOT NULL,
			received_at_unix INTEGER NOT NULL,
			UNIQUE(sender_pubkey_hex,message_id)
		)`,

		// 路由索引
		`CREATE TABLE IF NOT EXISTS proc_published_route_indexes(
			route TEXT PRIMARY KEY,
			seed_hash TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,

		// 节点可达性
		`CREATE TABLE IF NOT EXISTS proc_node_reachability_cache(
			target_node_pubkey_hex TEXT PRIMARY KEY,
			source_gateway_pubkey_hex TEXT NOT NULL,
			head_height INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			multiaddrs_json TEXT NOT NULL,
			published_at_unix INTEGER NOT NULL,
			expires_at_unix INTEGER NOT NULL,
			signature BLOB NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS proc_self_node_reachability_state(
			node_pubkey_hex TEXT PRIMARY KEY,
			head_height INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,

		// 基础索引（不依赖迁移的）
		`CREATE INDEX IF NOT EXISTS idx_biz_workspace_files_seed_hash ON biz_workspace_files(seed_hash, workspace_path, file_path)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_workspace_files_workspace ON biz_workspace_files(workspace_path, file_path)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_seed_chunk_supply_seed ON biz_seed_chunk_supply(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_workspaces_enabled ON biz_workspaces(enabled, workspace_path)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_seed_pricing_policy_updated ON biz_seed_pricing_policy(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_file_downloads_updated ON proc_file_downloads(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_file_download_chunks_seed ON proc_file_download_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_live_quotes_demand ON biz_live_quotes(demand_id, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_node_reachability_cache_expires ON proc_node_reachability_cache(expires_at_unix DESC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_created_at ON fact_pool_session_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_purchases_created_at ON biz_purchases(created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_purchases_demand_created ON biz_purchases(demand_id, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_purchases_seller_created ON biz_purchases(seller_pub_hex, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_purchases_status_created ON biz_purchases(status, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_purchases_history_lookup ON biz_purchases(demand_id, chunk_index, seller_pub_hex, arbiter_pub_hex, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_created_at ON proc_gateway_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_cmd_id ON proc_gateway_events(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_created_at ON proc_command_journal(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_cmd_id ON proc_command_journal(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_gateway ON proc_command_journal(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_trigger_key ON proc_command_journal(trigger_key, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_created_at ON proc_domain_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_cmd_id ON proc_domain_events(command_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_gateway ON proc_domain_events(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_created_at ON proc_state_snapshots(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_cmd_id ON proc_state_snapshots(command_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_gateway ON proc_state_snapshots(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_observed_gateway_states_created_at ON proc_observed_gateway_states(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_observed_gateway_states_gateway ON proc_observed_gateway_states(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_observed_gateway_states_event ON proc_observed_gateway_states(event_name, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_observed_gateway_states_state ON proc_observed_gateway_states(state_after, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_observed_gateway_states_source_ref ON proc_observed_gateway_states(source_ref, observed_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_created_at ON proc_effect_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_cmd_id ON proc_effect_logs(command_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_gateway ON proc_effect_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_orchestrator_logs_created_at ON proc_orchestrator_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_orchestrator_logs_event_type ON proc_orchestrator_logs(event_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_orchestrator_logs_signal_type ON proc_orchestrator_logs(signal_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_orchestrator_logs_gateway ON proc_orchestrator_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_orchestrator_logs_idempotency ON proc_orchestrator_logs(idempotency_key, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`,
		// 前台业务主身份层索引（第七次迭代新增）
		`CREATE INDEX IF NOT EXISTS idx_biz_front_orders_type_status ON biz_front_orders(front_type, status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_front_orders_target ON biz_front_orders(target_object_type, target_object_id)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_front_orders_owner ON biz_front_orders(owner_pubkey_hex, created_at_unix DESC)`,
		// 业务触发桥接层索引（第三次迭代新增）
		`CREATE INDEX IF NOT EXISTS idx_biz_business_triggers_business ON biz_business_triggers(business_id, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_business_triggers_type_value ON biz_business_triggers(trigger_type, trigger_id_value)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_business_triggers_type_value_role ON biz_business_triggers(trigger_type, trigger_id_value, trigger_role)`,
		// 统一结算出口索引（一次性收口）
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_records_idempotency ON settle_records(idempotency_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_records_settlement_id ON settle_records(settlement_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_role ON settle_records(business_role, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_source ON settle_records(source_type, source_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_accounting ON settle_records(accounting_scene, accounting_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_status ON settle_records(status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_method ON settle_records(settlement_method, settlement_status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_target ON settle_records(target_type, target_id)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv21_events_token_id ON fact_bsv21_events(token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv21_events_kind_time ON fact_bsv21_events(event_kind, event_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_sync_cursor_round_tip ON wallet_utxo_sync_cursor(round_tip_height DESC, updated_at_unix DESC)`,
		// 第六次迭代：finance 表索引移到 ensureFinAccountingIndexes 中创建
		// 避免老库迁移时列不存在导致错误
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_process ON settle_process_events(process_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_chain_tip_worker_logs_started ON proc_chain_tip_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_chain_tip_worker_logs_status ON proc_chain_tip_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_chain_utxo_worker_logs_started ON proc_chain_utxo_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_chain_utxo_worker_logs_status ON proc_chain_utxo_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_scheduler_tasks_status ON proc_scheduler_tasks(status, updated_at_unix DESC, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_scheduler_tasks_owner_mode ON proc_scheduler_tasks(owner, mode, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_scheduler_task_runs_task ON proc_scheduler_task_runs(task_name, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_scheduler_task_runs_status ON proc_scheduler_task_runs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_scheduler_task_runs_started ON proc_scheduler_task_runs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_inbox_messages_received_at ON proc_inbox_messages(received_at_unix DESC,id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_published_route_indexes_updated ON proc_published_route_indexes(updated_at_unix DESC,route ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demands_demand_id ON biz_demands(demand_id)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demands_created ON biz_demands(created_at_unix DESC, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quotes_demand_seller ON biz_demand_quotes(demand_id, seller_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quotes_demand_created ON biz_demand_quotes(demand_id, created_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quote_arbiters_quote_arbiter ON biz_demand_quote_arbiters(quote_id, arbiter_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quote_arbiters_arbiter ON biz_demand_quote_arbiters(arbiter_pub_hex, quote_id)`,

		// ============================================================
		// 新资产账本表（硬切版）
		// 设计说明：
		// - 新三表：fact_bsv_utxos（本币UTXO事实）、fact_token_lots（Token数量事实）、fact_token_carrier_links（Token与载体绑定）
		// - 新增 fact_settlement_records（结算消耗记录）、fact_settlement_cycles（结算周期锚点）
		// - 余额事实统一从新表计算，不依赖 direction IN/OUT 模式
		// ============================================================

		// fact_bsv_utxos: 本币UTXO事实表（硬切新增）
		// 设计说明：
		// - 记录钱包拥有的本币UTXO状态（unspent/spent）
		// - 这里只是余额事实，真正扣账只从 settlement_cycle 触发
		// - carrier_type 区分：plain_bsv（纯本币）、token_carrier（token载体1sat）、fee_change（费用找零）、unknown（待确认）
		// - 余额计算：owner_pubkey_hex + utxo_state='unspent' 聚合
		`CREATE TABLE IF NOT EXISTS fact_bsv_utxos(
			utxo_id TEXT PRIMARY KEY,
			owner_pubkey_hex TEXT NOT NULL,
			address TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			value_satoshi INTEGER NOT NULL CHECK(value_satoshi>0),
			utxo_state TEXT NOT NULL CHECK(utxo_state IN ('unspent','spent')),
			carrier_type TEXT NOT NULL CHECK(carrier_type IN ('plain_bsv','token_carrier','fee_change','unknown')),
			spent_by_txid TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			spent_at_unix INTEGER NOT NULL DEFAULT 0,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			UNIQUE(txid, vout)
		)`,
		// fact_bsv_utxos 索引
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_utxos_owner_state ON fact_bsv_utxos(owner_pubkey_hex, utxo_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_utxos_owner_carrier_state ON fact_bsv_utxos(owner_pubkey_hex, carrier_type, utxo_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_utxos_txid_vout ON fact_bsv_utxos(txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_utxos_spent_by ON fact_bsv_utxos(spent_by_txid, spent_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_utxos_created ON fact_bsv_utxos(created_at_unix DESC)`,

		// fact_token_lots: Token数量事实表（硬切新增）
		// 设计说明：
		// - 记录 Token 数量层的流入/消耗/剩余
		// - quantity_text 为十进制字符串，记录入账数量
		// - used_quantity_text 累计消耗数量（十进制字符串）
		// - lot_state: unspent（未消耗）、spent（已消耗完）、locked（锁定中）
		`CREATE TABLE IF NOT EXISTS fact_token_lots(
			lot_id TEXT PRIMARY KEY,
			owner_pubkey_hex TEXT NOT NULL,
			token_id TEXT NOT NULL,
			token_standard TEXT NOT NULL CHECK(token_standard IN ('BSV20','BSV21')),
			quantity_text TEXT NOT NULL,
			used_quantity_text TEXT NOT NULL DEFAULT '0',
			lot_state TEXT NOT NULL CHECK(lot_state IN ('unspent','spent','locked')),
			mint_txid TEXT NOT NULL DEFAULT '',
			last_spend_txid TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}'
		)`,
		// fact_token_lots 索引
		`CREATE INDEX IF NOT EXISTS idx_fact_token_lots_owner_token_state ON fact_token_lots(owner_pubkey_hex, token_standard, token_id, lot_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_lots_owner_state ON fact_token_lots(owner_pubkey_hex, lot_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_lots_token ON fact_token_lots(token_standard, token_id, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_lots_mint_txid ON fact_token_lots(mint_txid)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_lots_last_spend_txid ON fact_token_lots(last_spend_txid)`,

		// fact_token_carrier_links: Token与1sat载体绑定表（硬切新增）
		// 设计说明：
		// - 记录 Token lot 与其 1sat 载体 UTXO 的绑定关系
		// - link_state: active（有效绑定）、released（已释放）、moved（已迁移）
		// - 同一 lot 同一时刻只能有一个 active 绑定（UNIQUE约束）
		`CREATE TABLE IF NOT EXISTS fact_token_carrier_links(
			link_id TEXT PRIMARY KEY,
			lot_id TEXT NOT NULL,
			carrier_utxo_id TEXT NOT NULL,
			owner_pubkey_hex TEXT NOT NULL,
			link_state TEXT NOT NULL CHECK(link_state IN ('active','released','moved')),
			bind_txid TEXT NOT NULL DEFAULT '',
			unbind_txid TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			UNIQUE(lot_id, link_state),
			UNIQUE(carrier_utxo_id, link_state),
			FOREIGN KEY(lot_id) REFERENCES fact_token_lots(lot_id) ON DELETE CASCADE,
			FOREIGN KEY(carrier_utxo_id) REFERENCES fact_bsv_utxos(utxo_id) ON DELETE CASCADE
		)`,
		// fact_token_carrier_links 索引
		`CREATE INDEX IF NOT EXISTS idx_fact_token_carrier_links_owner_state ON fact_token_carrier_links(owner_pubkey_hex, link_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_carrier_links_lot_state ON fact_token_carrier_links(lot_id, link_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_carrier_links_carrier_state ON fact_token_carrier_links(carrier_utxo_id, link_state, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_carrier_links_bind_txid ON fact_token_carrier_links(bind_txid)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_carrier_links_unbind_txid ON fact_token_carrier_links(unbind_txid)`,

		// fact_settlement_records: 结算消耗记录表（硬切新增）
		// 设计说明：
		// - 记录每次结算周期中的资产消耗详情
		// - 这里只记录 cycle 驱动后的扣账结果，不接受直接旁路写 UTXO
		// - asset_type: BSV（本币）、TOKEN（token数量）
		// - source_utxo_id: 本币消耗来源（fact_bsv_utxos.utxo_id）
		// - source_lot_id: token消耗来源（fact_token_lots.lot_id）
		`CREATE TABLE IF NOT EXISTS fact_settlement_records(
			record_id TEXT PRIMARY KEY,
			settlement_cycle_id INTEGER NOT NULL,
			asset_type TEXT NOT NULL CHECK(asset_type IN ('BSV','TOKEN')),
			owner_pubkey_hex TEXT NOT NULL,
			source_utxo_id TEXT NOT NULL DEFAULT '',
			source_lot_id TEXT NOT NULL DEFAULT '',
			used_satoshi INTEGER NOT NULL DEFAULT 0,
			used_quantity_text TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL CHECK(state IN ('pending','confirmed','reverted')),
			occurred_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id),
			UNIQUE(settlement_cycle_id, asset_type, source_utxo_id, source_lot_id)
		)`,
		// fact_settlement_records 索引
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_records_cycle_asset ON fact_settlement_records(settlement_cycle_id, asset_type, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_records_owner_state ON fact_settlement_records(owner_pubkey_hex, state, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_records_source_utxo ON fact_settlement_records(source_utxo_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_records_source_lot ON fact_settlement_records(source_lot_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_records_state_time ON fact_settlement_records(state, occurred_at_unix DESC)`,

		// Step 15: 统一结算锚点 — fact_settlement_cycles
		// 设计说明：
		// - 把 4 个渠道写入统一到结算周期主表
		// - 只保留 source_type/source_id 作为来源锚点
		// - source_type/source_id 做唯一约束，禁止再靠旧事件列兜底
		// - 业务扣账只认 settlement_cycle，不再直接改 fact_bsv_utxos
		`CREATE TABLE IF NOT EXISTS fact_settlement_cycles(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			cycle_id TEXT NOT NULL UNIQUE,
			source_type TEXT NOT NULL CHECK(source_type IN ('pool_session_quote_pay','chain_quote_pay','chain_direct_pay','chain_asset_create')),
			source_id TEXT NOT NULL,
			state TEXT NOT NULL DEFAULT 'confirmed' CHECK(state IN ('pending','confirmed','failed')),
			gross_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			gate_fee_satoshi INTEGER NOT NULL DEFAULT 0,
			net_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			cycle_index INTEGER NOT NULL DEFAULT 0,
			occurred_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			UNIQUE(source_type, source_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_settlement_cycles_source_state ON fact_settlement_cycles(source_type, state, occurred_at_unix DESC)`,

		// Step 9: WOC 证据驱动 token IN 入账 - 待确认队列表
		// 设计说明：
		// - 轮询到可疑 UTXO 先进入此表，状态为 pending
		// - 通过 WOC 查询证据后更新状态，成功则写 fact IN，失败则指数退避重试
		// - 与 wallet_utxo 关联，但独立管理重试周期
		`CREATE TABLE IF NOT EXISTS wallet_utxo_token_verification(
			utxo_id TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			value_satoshi INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'confirmed_bsv20', 'confirmed_bsv21', 'confirmed_plain_bsv', 'failed')),
			woc_response_json TEXT NOT NULL DEFAULT '{}',
			last_check_at_unix INTEGER NOT NULL DEFAULT 0,
			next_retry_at_unix INTEGER NOT NULL DEFAULT 0,
			retry_count INTEGER NOT NULL DEFAULT 0,
			error_message TEXT NOT NULL DEFAULT '',
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_token_verification_status ON wallet_utxo_token_verification(status, next_retry_at_unix ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_token_verification_wallet ON wallet_utxo_token_verification(wallet_id, status)`,
	}...)

	for _, s := range stmts {
		if _, err := ExecContext(ctx, db, s); err != nil {
			return fmt.Errorf("exec schema stmt: %w", err)
		}
	}
	return nil
}

func buildObservedGatewayStateFinalPayloadJSON(eventName string, walletBalance uint64, raw string) (string, error) {
	normalized := observedGatewayStatePayload{
		ObservedReason:       normalizeObservedReasonFromPayload(eventName, raw),
		WalletBalanceSatoshi: walletBalance,
		Extra:                map[string]any{},
	}
	decoded, ok := decodeObservedGatewayStateObject(raw)
	if !ok {
		if strings.TrimSpace(raw) != "" {
			normalized.Extra = map[string]any{"raw": raw}
		}
		data, err := json.Marshal(normalized)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	if reason, ok := decoded["observed_reason"].(string); ok && strings.TrimSpace(reason) != "" {
		normalized.ObservedReason = strings.TrimSpace(reason)
	}
	if balance, ok := decoded["wallet_balance_satoshi"]; ok {
		if v, ok := toUint64(balance); ok {
			normalized.WalletBalanceSatoshi = v
		}
	}
	if extra, ok := decoded["extra"]; ok {
		if extraMap, ok := extra.(map[string]any); ok {
			normalized.Extra = extraMap
		} else {
			normalized.Extra = map[string]any{"value": extra}
		}
	} else {
		normalized.Extra = decoded
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func normalizeObservedReasonFromPayload(eventName, raw string) string {
	if payload, ok := decodeObservedGatewayStateObject(raw); ok {
		if reason, ok := payload["observed_reason"].(string); ok && strings.TrimSpace(reason) != "" {
			return strings.TrimSpace(reason)
		}
	}
	switch {
	case strings.Contains(eventName, "wallet_probe"):
		return "wallet_probe"
	case strings.Contains(eventName, "pause_observed"):
		return "pause_watch"
	case strings.Contains(eventName, "resume"):
		return "runtime_resume_check"
	default:
		return "runtime_resume_check"
	}
}

func decodeObservedGatewayStateObject(raw string) (map[string]any, bool) {
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, false
	}
	return out, true
}

func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case float64:
		if n < 0 || n != float64(uint64(n)) {
			return 0, false
		}
		return uint64(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return 0, false
		}
		return uint64(i), true
	default:
		return 0, false
	}
}

// ensureCommandJournalTriggerKey 确保 proc_command_journal 的 trigger_key 列和索引都存在。
// 设计说明：
// - 第六次迭代新增字段，用于关联 proc_orchestrator_logs.idempotency_key
// - 旧库通过 ALTER TABLE 补齐，新库在基础 schema 中已包含
// - 迁移必须幂等：列存在时也要补索引，避免老库只补了一半
func ensureCommandJournalTriggerKey(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "proc_command_journal")
	if err != nil {
		return fmt.Errorf("inspect proc_command_journal: %w", err)
	}
	if _, ok := cols["trigger_key"]; !ok {
		if _, err := ExecContext(ctx, db, `ALTER TABLE proc_command_journal ADD COLUMN trigger_key TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add trigger_key column: %w", err)
		}
	}
	if _, err := ExecContext(ctx, db, `CREATE INDEX IF NOT EXISTS idx_proc_command_journal_trigger_key ON proc_command_journal(trigger_key, id DESC)`); err != nil {
		return fmt.Errorf("create trigger_key index: %w", err)
	}
	return nil
}

// ensureGatewayEventAndCommandJournalConstraints 先清历史脏数据，再收紧两个表的数据库约束。
// 设计说明：
// - 先清 proc_command_journal / proc_gateway_events 历史脏数据，再补硬约束；
// - proc_command_journal.command_id 本身语义唯一，所以用 UNIQUE 承接 proc_gateway_events 的 FK；
// - 对于不能确认归属的旧 proc_gateway_events，直接删除，不伪造关系；
// - 如果历史 proc_command_journal 里已经出现重复 command_id，直接停下并返回清理信息。
func ensureGatewayEventAndCommandJournalConstraints(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	dups, err := commandJournalDuplicateCommandIDs(ctx, db)
	if err != nil {
		return fmt.Errorf("audit proc_command_journal duplicates: %w", err)
	}
	if len(dups) > 0 {
		return fmt.Errorf("proc_command_journal has duplicate command_id values: %s", strings.Join(dups, ", "))
	}
	if err := cleanupLegacyCommandJournalCommandIDRows(ctx, db); err != nil {
		return fmt.Errorf("cleanup proc_command_journal: %w", err)
	}
	if err := cleanupLegacyGatewayEventCommandIDRows(ctx, db); err != nil {
		return fmt.Errorf("cleanup proc_gateway_events: %w", err)
	}
	if err := ensureCommandJournalCommandIDUnique(ctx, db); err != nil {
		return fmt.Errorf("proc_command_journal unique: %w", err)
	}
	if err := ensureGatewayEventsCommandIDForeignKey(ctx, db); err != nil {
		return fmt.Errorf("proc_gateway_events fk: %w", err)
	}
	if err := ensureGatewayEventsIndexes(ctx, db); err != nil {
		return fmt.Errorf("proc_gateway_events index: %w", err)
	}
	return nil
}

// cleanupLegacyCommandJournalCommandIDRows 清掉历史上 proc_command_journal 里不该留下的 command_id 空值。
func cleanupLegacyCommandJournalCommandIDRows(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	_, err := ExecContext(ctx, db, `DELETE FROM proc_command_journal WHERE trim(coalesce(command_id, '')) = ''`)
	return err
}

// cleanupLegacyGatewayEventCommandIDRows 清掉 proc_gateway_events 里无法进入硬约束阶段的历史脏数据。
// 说明：
// - command_id 为空的行直接删除；
// - command_id 找不到父命令的行也直接删除；
// - 这一步是收紧约束前的真实清理，不做伪造回填。
func cleanupLegacyGatewayEventCommandIDRows(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "proc_gateway_events")
	if err != nil {
		return fmt.Errorf("inspect proc_gateway_events: %w", err)
	}
	if _, ok := cols["command_id"]; !ok {
		if _, err := ExecContext(ctx, db, `ALTER TABLE proc_gateway_events ADD COLUMN command_id TEXT`); err != nil {
			return err
		}
	}
	_, err = ExecContext(ctx, db, `
		DELETE FROM proc_gateway_events
		WHERE trim(coalesce(command_id, '')) = ''
		   OR NOT EXISTS(
				SELECT 1
				FROM proc_command_journal
				WHERE proc_command_journal.command_id = proc_gateway_events.command_id
		   )
	`)
	return err
}

// commandJournalDuplicateCommandIDs 列出 proc_command_journal 中重复的 command_id，供迁移前审计使用。
func commandJournalDuplicateCommandIDs(ctx context.Context, db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := QueryContext(ctx, db, `
		SELECT command_id, COUNT(1)
		FROM proc_command_journal
		WHERE trim(coalesce(command_id, '')) <> ''
		GROUP BY command_id
		HAVING COUNT(1) > 1
		ORDER BY command_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dups := make([]string, 0)
	for rows.Next() {
		var commandID string
		var count int64
		if err := rows.Scan(&commandID, &count); err != nil {
			return nil, err
		}
		dups = append(dups, fmt.Sprintf("%s(x%d)", strings.TrimSpace(commandID), count))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dups, nil
}

// ensureCommandJournalCommandIDUnique 把 proc_command_journal.command_id 收紧成唯一键。
// 设计说明：
// - 先做重复审计，发现重复就直接返回，不硬上约束；
// - 不再重建整张表，直接补唯一索引；这样更稳，也不会在单连接测试里自锁。
func ensureCommandJournalCommandIDUnique(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	dups, err := commandJournalDuplicateCommandIDs(ctx, db)
	if err != nil {
		return err
	}
	if len(dups) > 0 {
		return fmt.Errorf("proc_command_journal has duplicate command_id values: %s", strings.Join(dups, ", "))
	}
	if _, err := ExecContext(ctx, db, `CREATE UNIQUE INDEX IF NOT EXISTS uq_proc_command_journal_command_id ON proc_command_journal(command_id)`); err != nil {
		return err
	}
	return ensureCommandJournalIndexes(ctx, db)
}

// ensureGatewayEventsCommandIDForeignKey 让 proc_gateway_events.command_id 变成 NOT NULL + 物理外键。
func ensureGatewayEventsCommandIDForeignKey(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	notNull, err := tableColumnNotNull(db, "proc_gateway_events", "command_id")
	if err != nil {
		return fmt.Errorf("inspect proc_gateway_events command_id not null: %w", err)
	}
	hasFK, err := tableHasForeignKeyCtx(ctx, db, "proc_gateway_events", "command_id", "proc_command_journal", "command_id")
	if err != nil {
		return fmt.Errorf("inspect proc_gateway_events foreign key: %w", err)
	}
	if notNull && hasFK {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS proc_gateway_events_fk_rebuild`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE proc_gateway_events RENAME TO proc_gateway_events_fk_rebuild`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE proc_gateway_events(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at_unix INTEGER NOT NULL,
		gateway_pubkey_hex TEXT NOT NULL,
		command_id TEXT NOT NULL,
		action TEXT NOT NULL,
		msg_id TEXT NOT NULL,
		sequence_num INTEGER NOT NULL,
		pool_id TEXT NOT NULL,
		amount_satoshi INTEGER NOT NULL,
		payload_json TEXT NOT NULL,
		FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
	)`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `INSERT INTO proc_gateway_events(
		id,created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json
	) SELECT
		id,created_at_unix,gateway_pubkey_hex,command_id,action,msg_id,sequence_num,pool_id,amount_satoshi,payload_json
	FROM proc_gateway_events_fk_rebuild
	WHERE trim(coalesce(command_id, '')) <> ''
	  AND EXISTS(
			SELECT 1
			FROM proc_command_journal
			WHERE proc_command_journal.command_id = proc_gateway_events_fk_rebuild.command_id
	  )
	ORDER BY id ASC`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE proc_gateway_events_fk_rebuild`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=ON`); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return ensureGatewayEventsIndexes(ctx, db)
}

// ensureGatewayEventsIndexes 保证 proc_gateway_events 的查询索引都还在。
func ensureGatewayEventsIndexes(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := ExecContext(ctx, db, `CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_created_at ON proc_gateway_events(created_at_unix DESC)`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, db, `CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_cmd_id ON proc_gateway_events(command_id)`); err != nil {
		return err
	}
	return nil
}

// ensureCommandJournalIndexes 保证 proc_command_journal 的查询索引都还在。
func ensureCommandJournalIndexes(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_created_at ON proc_command_journal(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_cmd_id ON proc_command_journal(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_gateway ON proc_command_journal(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_trigger_key ON proc_command_journal(trigger_key, id DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	if _, err := ExecContext(ctx, db, `CREATE UNIQUE INDEX IF NOT EXISTS uq_proc_command_journal_command_id ON proc_command_journal(command_id)`); err != nil {
		return err
	}
	return nil
}

// tableHasForeignKey 检查表是否已经有指定外键。
func tableHasForeignKeyCtx(ctx context.Context, db *sql.DB, table, fromColumn, parentTable, parentColumn string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := QueryContext(ctx, db, fmt.Sprintf("PRAGMA foreign_key_list(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var seq int
		var fkTable string
		var fkFrom string
		var fkTo string
		var onUpdate string
		var onDelete string
		var match string
		if err := rows.Scan(&id, &seq, &fkTable, &fkFrom, &fkTo, &onUpdate, &onDelete, &match); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(fkFrom), strings.TrimSpace(fromColumn)) &&
			strings.EqualFold(strings.TrimSpace(fkTable), strings.TrimSpace(parentTable)) &&
			strings.EqualFold(strings.TrimSpace(fkTo), strings.TrimSpace(parentColumn)) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// ensureGatewayEventsCommandID 兼容旧名字，避免外层注释误导。
func ensureGatewayEventsCommandID(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	return ensureGatewayEventAndCommandJournalConstraints(ctx, db)
}

type commandLinkedTableAuditReport struct {
	Table               string
	NullCommandIDRows   int64
	BlankCommandIDRows  int64
	OrphanCommandIDRows int64
}

func (r commandLinkedTableAuditReport) DirtyRows() int64 {
	return r.NullCommandIDRows + r.BlankCommandIDRows + r.OrphanCommandIDRows
}

func (r commandLinkedTableAuditReport) String() string {
	return fmt.Sprintf("%s null=%d blank=%d orphan=%d", r.Table, r.NullCommandIDRows, r.BlankCommandIDRows, r.OrphanCommandIDRows)
}

// ensureCommandLinkedTableConstraints 把 proc_domain_events / proc_state_snapshots / proc_effect_logs 收紧到同一命令口径。
// 设计说明：
// - 先审计，再清理，再重建；
// - 空值、空白值、孤儿行都直接删，不补假命令；
// - 只在约束缺失时重建，避免新库重复折腾；
// - 重建后统一补回查询索引，保持读取口径不变。
func ensureCommandLinkedTableConstraints(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	for _, table := range []string{"proc_domain_events", "proc_state_snapshots", "proc_effect_logs"} {
		report, err := auditCommandLinkedTableRows(ctx, db, table)
		if err != nil {
			return fmt.Errorf("audit %s: %w", table, err)
		}
		if report.DirtyRows() > 0 {
			if err := cleanupLegacyCommandLinkedRows(ctx, db, table); err != nil {
				return fmt.Errorf("cleanup %s: %w", table, err)
			}
		}
		needsRebuild, err := commandLinkedTableNeedsRebuild(ctx, db, table)
		if err != nil {
			return fmt.Errorf("inspect %s constraints: %w", table, err)
		}
		if needsRebuild {
			if err := rebuildCommandLinkedTable(ctx, db, table); err != nil {
				return fmt.Errorf("rebuild %s: %w", table, err)
			}
		}
		if err := ensureCommandLinkedTableIndexes(ctx, db, table); err != nil {
			return fmt.Errorf("indexes %s: %w", table, err)
		}
	}
	return nil
}

// auditCommandLinkedTableRows 先把命令链表里的脏数据看清楚，再决定清理和重建。
func auditCommandLinkedTableRows(ctx context.Context, db *sql.DB, table string) (commandLinkedTableAuditReport, error) {
	report := commandLinkedTableAuditReport{Table: strings.TrimSpace(table)}
	if db == nil {
		return report, fmt.Errorf("db is nil")
	}
	if !isCommandLinkedTable(report.Table) {
		return report, fmt.Errorf("unsupported table: %s", table)
	}
	hasTable, err := hasTable(db, report.Table)
	if err != nil {
		return report, fmt.Errorf("check %s table: %w", report.Table, err)
	}
	if !hasTable {
		return report, nil
	}
	row := QueryRowContext(ctx, db, fmt.Sprintf(`
		SELECT
			COALESCE(SUM(CASE WHEN command_id IS NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN command_id IS NOT NULL AND trim(command_id) = '' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN command_id IS NOT NULL AND trim(command_id) <> '' AND NOT EXISTS(
				SELECT 1 FROM proc_command_journal WHERE proc_command_journal.command_id = %s.command_id
			) THEN 1 ELSE 0 END), 0)
		FROM %s`,
		report.Table, report.Table,
	))
	if err := row.Scan(&report.NullCommandIDRows, &report.BlankCommandIDRows, &report.OrphanCommandIDRows); err != nil {
		return report, err
	}
	return report, nil
}

// cleanupLegacyDomainEventCommandRows 删除 proc_domain_events 里不该进入硬约束的旧行。
func cleanupLegacyDomainEventCommandRows(ctx context.Context, db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(ctx, db, "proc_domain_events")
}

// cleanupLegacyStateSnapshotCommandRows 删除 proc_state_snapshots 里不该进入硬约束的旧行。
func cleanupLegacyStateSnapshotCommandRows(ctx context.Context, db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(ctx, db, "proc_state_snapshots")
}

// cleanupLegacyEffectLogCommandRows 删除 proc_effect_logs 里不该进入硬约束的旧行。
func cleanupLegacyEffectLogCommandRows(ctx context.Context, db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(ctx, db, "proc_effect_logs")
}

func cleanupLegacyCommandLinkedRows(ctx context.Context, db *sql.DB, table string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !isCommandLinkedTable(strings.TrimSpace(table)) {
		return fmt.Errorf("unsupported table: %s", table)
	}
	_, err := ExecContext(ctx, db, fmt.Sprintf(`
		DELETE FROM %s
		WHERE command_id IS NULL
		   OR trim(command_id) = ''
		   OR NOT EXISTS(
				SELECT 1
				FROM proc_command_journal
				WHERE proc_command_journal.command_id = %s.command_id
		   )`, table, table))
	return err
}

func isCommandLinkedTable(table string) bool {
	switch strings.TrimSpace(table) {
	case "proc_domain_events", "proc_state_snapshots", "proc_effect_logs":
		return true
	default:
		return false
	}
}

func commandLinkedTableNeedsRebuild(ctx context.Context, db *sql.DB, table string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	if !isCommandLinkedTable(table) {
		return false, fmt.Errorf("unsupported table: %s", table)
	}
	notNull, err := tableColumnNotNull(db, table, "command_id")
	if err != nil {
		return false, fmt.Errorf("inspect %s command_id not null: %w", table, err)
	}
	hasFK, err := tableHasForeignKeyCtx(ctx, db, table, "command_id", "proc_command_journal", "command_id")
	if err != nil {
		return false, fmt.Errorf("inspect %s foreign key: %w", table, err)
	}
	hasCheck, err := tableHasCreateSQLContainsCtx(ctx, db, table, "CHECK(trim(command_id) <> '')")
	if err != nil {
		return false, fmt.Errorf("inspect %s check constraint: %w", table, err)
	}
	if !notNull || !hasFK || !hasCheck {
		return true, nil
	}
	return false, nil
}

func rebuildCommandLinkedTable(ctx context.Context, db *sql.DB, table string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !isCommandLinkedTable(table) {
		return fmt.Errorf("unsupported table: %s", table)
	}
	spec, ok := commandLinkedTableSpec(table)
	if !ok {
		return fmt.Errorf("unsupported table: %s", table)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	tempTable := table + "_command_fk_rebuild"
	if _, err := ExecContext(ctx, tx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, table, tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, spec.CreateSQL); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, spec.InsertSQL(tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, fmt.Sprintf(`DROP TABLE %s`, tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=ON`); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

type commandLinkedTableRebuildSpec struct {
	CreateSQL string
	InsertSQL func(oldTable string) string
}

func commandLinkedTableSpec(table string) (commandLinkedTableRebuildSpec, bool) {
	switch strings.TrimSpace(table) {
	case "proc_domain_events":
		return commandLinkedTableRebuildSpec{
			CreateSQL: `CREATE TABLE proc_domain_events(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				created_at_unix INTEGER NOT NULL,
				command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
				gateway_pubkey_hex TEXT NOT NULL,
				event_name TEXT NOT NULL,
				state_before TEXT NOT NULL,
				state_after TEXT NOT NULL,
				payload_json TEXT NOT NULL,
				FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
			)`,
			InsertSQL: func(oldTable string) string {
				return fmt.Sprintf(`INSERT INTO proc_domain_events(
					id,created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
				) SELECT
					id,created_at_unix,command_id,gateway_pubkey_hex,event_name,state_before,state_after,payload_json
				FROM %s
				WHERE command_id IS NOT NULL
				  AND trim(command_id) <> ''
				  AND EXISTS(
						SELECT 1
						FROM proc_command_journal
						WHERE proc_command_journal.command_id = %s.command_id
				  )
				ORDER BY id ASC`, oldTable, oldTable)
			},
		}, true
	case "proc_state_snapshots":
		return commandLinkedTableRebuildSpec{
			CreateSQL: `CREATE TABLE proc_state_snapshots(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				created_at_unix INTEGER NOT NULL,
				command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
				gateway_pubkey_hex TEXT NOT NULL,
				state TEXT NOT NULL,
				pause_reason TEXT NOT NULL,
				pause_need_satoshi INTEGER NOT NULL,
				pause_have_satoshi INTEGER NOT NULL,
				last_error TEXT NOT NULL,
				payload_json TEXT NOT NULL,
				FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
			)`,
			InsertSQL: func(oldTable string) string {
				return fmt.Sprintf(`INSERT INTO proc_state_snapshots(
					id,created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
				) SELECT
					id,created_at_unix,command_id,gateway_pubkey_hex,state,pause_reason,pause_need_satoshi,pause_have_satoshi,last_error,payload_json
				FROM %s
				WHERE command_id IS NOT NULL
				  AND trim(command_id) <> ''
				  AND EXISTS(
						SELECT 1
						FROM proc_command_journal
						WHERE proc_command_journal.command_id = %s.command_id
				  )
				ORDER BY id ASC`, oldTable, oldTable)
			},
		}, true
	case "proc_effect_logs":
		return commandLinkedTableRebuildSpec{
			CreateSQL: `CREATE TABLE proc_effect_logs(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				created_at_unix INTEGER NOT NULL,
				command_id TEXT NOT NULL CHECK(trim(command_id) <> ''),
				gateway_pubkey_hex TEXT NOT NULL,
				effect_type TEXT NOT NULL,
				stage TEXT NOT NULL,
				status TEXT NOT NULL,
				error_message TEXT NOT NULL,
				payload_json TEXT NOT NULL,
				FOREIGN KEY(command_id) REFERENCES proc_command_journal(command_id)
			)`,
			InsertSQL: func(oldTable string) string {
				return fmt.Sprintf(`INSERT INTO proc_effect_logs(
					id,created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
				) SELECT
					id,created_at_unix,command_id,gateway_pubkey_hex,effect_type,stage,status,error_message,payload_json
				FROM %s
				WHERE command_id IS NOT NULL
				  AND trim(command_id) <> ''
				  AND EXISTS(
						SELECT 1
						FROM proc_command_journal
						WHERE proc_command_journal.command_id = %s.command_id
				  )
				ORDER BY id ASC`, oldTable, oldTable)
			},
		}, true
	default:
		return commandLinkedTableRebuildSpec{}, false
	}
}

func ensureCommandLinkedTableIndexes(ctx context.Context, db *sql.DB, table string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	switch strings.TrimSpace(table) {
	case "proc_domain_events":
		stmts := []string{
			`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_created_at ON proc_domain_events(created_at_unix DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_cmd_id ON proc_domain_events(command_id, id DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_domain_events_gateway ON proc_domain_events(gateway_pubkey_hex, id DESC)`,
		}
		for _, stmt := range stmts {
			if _, err := ExecContext(ctx, db, stmt); err != nil {
				return err
			}
		}
	case "proc_state_snapshots":
		stmts := []string{
			`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_created_at ON proc_state_snapshots(created_at_unix DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_cmd_id ON proc_state_snapshots(command_id, id DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_state_snapshots_gateway ON proc_state_snapshots(gateway_pubkey_hex, id DESC)`,
		}
		for _, stmt := range stmts {
			if _, err := ExecContext(ctx, db, stmt); err != nil {
				return err
			}
		}
	case "proc_effect_logs":
		stmts := []string{
			`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_created_at ON proc_effect_logs(created_at_unix DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_cmd_id ON proc_effect_logs(command_id, id DESC)`,
			`CREATE INDEX IF NOT EXISTS idx_proc_effect_logs_gateway ON proc_effect_logs(gateway_pubkey_hex, id DESC)`,
		}
		for _, stmt := range stmts {
			if _, err := ExecContext(ctx, db, stmt); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported table: %s", table)
	}
	return nil
}

func tableHasCreateSQLContainsCtx(ctx context.Context, db *sql.DB, table, snippet string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	var sqlText sql.NullString
	err := QueryRowContext(ctx, db, `SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(table)).Scan(&sqlText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !sqlText.Valid {
		return false, nil
	}
	return strings.Contains(normalizeSQLWhitespace(strings.ToLower(sqlText.String)), normalizeSQLWhitespace(strings.ToLower(snippet))), nil
}

func normalizeSQLWhitespace(in string) string {
	return strings.NewReplacer(" ", "", "\n", "", "\t", "", "\r", "").Replace(strings.TrimSpace(in))
}

// ensureBizPoolSchema 处理费用池业务表和结算周期的池会话列。
// 设计说明：
// - 新库直接建表；
// - 老库只补列和索引，不回头删旧结构；
// - pool_session_id 先作为查询维度保存，后续写路径再补齐真实值。
func ensureBizPoolSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	stmts := bizPoolSchemaStmts()
	for _, stmt := range stmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	indexStmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_status_updated ON biz_pool(status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_allocations_session_no ON biz_pool_allocations(pool_session_id, allocation_no DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_allocations_session_kind_seq ON biz_pool_allocations(pool_session_id, allocation_kind, sequence_num)`,
	}
	for _, stmt := range indexStmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	return nil
}

// bizPoolSchemaStmts 费用池业务表定义。
// 说明：基础建表和迁移补丁必须共用同一份字符串，避免两处 schema 漂移。
func bizPoolSchemaStmts() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS biz_pool(
			pool_session_id TEXT PRIMARY KEY,
			pool_scheme TEXT NOT NULL,
			counterparty_pubkey_hex TEXT NOT NULL DEFAULT '',
			seller_pubkey_hex TEXT NOT NULL DEFAULT '',
			arbiter_pubkey_hex TEXT NOT NULL DEFAULT '',
			gateway_pubkey_hex TEXT NOT NULL DEFAULT '',
			pool_amount_satoshi INTEGER NOT NULL DEFAULT 0,
			spend_tx_fee_satoshi INTEGER NOT NULL DEFAULT 0,
			allocated_satoshi INTEGER NOT NULL DEFAULT 0,
			cycle_fee_satoshi INTEGER NOT NULL DEFAULT 0,
			available_satoshi INTEGER NOT NULL DEFAULT 0,
			next_sequence_num INTEGER NOT NULL DEFAULT 1,
			status TEXT NOT NULL,
			open_base_txid TEXT NOT NULL DEFAULT '',
			open_allocation_id TEXT NOT NULL DEFAULT '',
			close_allocation_id TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS biz_pool_allocations(
			allocation_id TEXT PRIMARY KEY,
			pool_session_id TEXT NOT NULL,
			allocation_no INTEGER NOT NULL,
			allocation_kind TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			payee_amount_after INTEGER NOT NULL DEFAULT 0,
			payer_amount_after INTEGER NOT NULL DEFAULT 0,
			txid TEXT NOT NULL,
			tx_hex TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			FOREIGN KEY(pool_session_id) REFERENCES biz_pool(pool_session_id) ON DELETE CASCADE,
			UNIQUE(pool_session_id, allocation_kind, sequence_num)
		)`,
	}
}

// ensureFinAccountingSchema 只补财务表的主口径列，不改历史数据行为。
// 设计说明：
// - 新库由 CREATE TABLE 直接带上新列；
// - 老库靠这里补列，避免启动时因缺列失败；
// - 这一批只铺轨，不改任何写入和读取逻辑。
func ensureFinAccountingSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	migrations := []struct {
		table string
		col   string
		stmt  string
	}{
		{table: "settle_records", col: "business_role", stmt: `ALTER TABLE settle_records ADD COLUMN business_role TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "source_type", stmt: `ALTER TABLE settle_records ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "source_id", stmt: `ALTER TABLE settle_records ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "accounting_scene", stmt: `ALTER TABLE settle_records ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "accounting_subtype", stmt: `ALTER TABLE settle_records ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "from_party_id", stmt: `ALTER TABLE settle_records ADD COLUMN from_party_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "to_party_id", stmt: `ALTER TABLE settle_records ADD COLUMN to_party_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "status", stmt: `ALTER TABLE settle_records ADD COLUMN status TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "occurred_at_unix", stmt: `ALTER TABLE settle_records ADD COLUMN occurred_at_unix INTEGER NOT NULL DEFAULT 0`},
		{table: "settle_records", col: "idempotency_key", stmt: `ALTER TABLE settle_records ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "note", stmt: `ALTER TABLE settle_records ADD COLUMN note TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "payload_json", stmt: `ALTER TABLE settle_records ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'`},
		{table: "settle_records", col: "settlement_method", stmt: `ALTER TABLE settle_records ADD COLUMN settlement_method TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "settlement_status", stmt: `ALTER TABLE settle_records ADD COLUMN settlement_status TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "target_type", stmt: `ALTER TABLE settle_records ADD COLUMN target_type TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "target_id", stmt: `ALTER TABLE settle_records ADD COLUMN target_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "error_message", stmt: `ALTER TABLE settle_records ADD COLUMN error_message TEXT NOT NULL DEFAULT ''`},
		{table: "settle_records", col: "settlement_payload_json", stmt: `ALTER TABLE settle_records ADD COLUMN settlement_payload_json TEXT NOT NULL DEFAULT '{}'`},
		{table: "settle_records", col: "created_at_unix", stmt: `ALTER TABLE settle_records ADD COLUMN created_at_unix INTEGER NOT NULL DEFAULT 0`},
		{table: "settle_records", col: "updated_at_unix", stmt: `ALTER TABLE settle_records ADD COLUMN updated_at_unix INTEGER NOT NULL DEFAULT 0`},
		{table: "settle_process_events", col: "source_type", stmt: `ALTER TABLE settle_process_events ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "source_id", stmt: `ALTER TABLE settle_process_events ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "accounting_scene", stmt: `ALTER TABLE settle_process_events ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "accounting_subtype", stmt: `ALTER TABLE settle_process_events ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
	}

	for _, m := range migrations {
		cols, err := tableColumns(db, m.table)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", m.table, err)
		}
		if _, ok := cols[strings.ToLower(m.col)]; ok {
			continue
		}
		if _, err := ExecContext(ctx, db, m.stmt); err != nil {
			return fmt.Errorf("add %s.%s: %w", m.table, m.col, err)
		}
	}
	return nil
}

// ensureFinAccountingIndexes 只创建 finance 新口径查询所需的索引。
// 说明：要放在列补齐之后执行，老库迁移时不能提前碰还不存在的列。
func ensureFinAccountingIndexes(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_records_idempotency ON settle_records(idempotency_key)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_records_settlement_id ON settle_records(settlement_id)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_role ON settle_records(business_role, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_source ON settle_records(source_type, source_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_records_accounting ON settle_records(accounting_scene, accounting_subtype, occurred_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_process_events_idempotency ON settle_process_events(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_source ON settle_process_events(source_type, source_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_accounting ON settle_process_events(accounting_scene, accounting_subtype, occurred_at_unix DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	return nil
}

// normalizeClientDBData 只做当前口径需要的轻量归一化，不做历史迁移。
func normalizeClientDBData(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 这里只保留当前口径必需的轻量约束，避免把旧库收口逻辑继续留在初始化里。
	if _, err := ExecContext(ctx, db, `
		CREATE TRIGGER IF NOT EXISTS chk_settle_records_role_insert
		BEFORE INSERT ON settle_records
		BEGIN
			SELECT CASE
				WHEN NEW.business_role NOT IN ('', 'formal', 'process')
				THEN RAISE(ABORT, 'business_role must be formal or process')
			END;
		END;
	`); err != nil {
		return fmt.Errorf("create business_role insert check trigger: %w", err)
	}
	if _, err := ExecContext(ctx, db, `
		CREATE TRIGGER IF NOT EXISTS chk_settle_records_role_update
		BEFORE UPDATE ON settle_records
		BEGIN
			SELECT CASE
				WHEN NEW.business_role NOT IN ('', 'formal', 'process')
				THEN RAISE(ABORT, 'business_role must be formal or process')
			END;
		END;
	`); err != nil {
		return fmt.Errorf("create business_role update check trigger: %w", err)
	}

	// 公钥格式统一化为压缩公钥 hex
	if err := normalizeClientPubKeyColumns(ctx, db); err != nil {
		return fmt.Errorf("pubkey columns: %w", err)
	}

	return nil
}

// ==================== 以下是当前结构辅助函数实现 ====================

func ensureDemandQuoteCurrentSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	hasQuotes, err := hasTable(db, "biz_demand_quotes")
	if err != nil {
		return fmt.Errorf("check biz_demand_quotes table: %w", err)
	}
	hasArbiters, err := hasTable(db, "biz_demand_quote_arbiters")
	if err != nil {
		return fmt.Errorf("check biz_demand_quote_arbiters table: %w", err)
	}
	if !hasQuotes || !hasArbiters {
		return nil
	}

	quotesNeedFK, err := demandQuoteTableMissingFK(ctx, db, "biz_demand_quotes", "demand_id", "biz_demands", "demand_id")
	if err != nil {
		return fmt.Errorf("inspect biz_demand_quotes foreign keys: %w", err)
	}
	arbitersNeedFK, err := demandQuoteTableMissingFK(ctx, db, "biz_demand_quote_arbiters", "quote_id", "biz_demand_quotes", "id")
	if err != nil {
		return fmt.Errorf("inspect biz_demand_quote_arbiters foreign keys: %w", err)
	}
	if !quotesNeedFK && !arbitersNeedFK {
		return nil
	}
	return rebuildDemandQuoteFKTables(ctx, db, quotesNeedFK, arbitersNeedFK)
}

func ensureDemandQuoteIndexes(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quotes_demand_seller ON biz_demand_quotes(demand_id, seller_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quotes_demand_created ON biz_demand_quotes(demand_id, created_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quote_arbiters_quote_arbiter ON biz_demand_quote_arbiters(quote_id, arbiter_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quote_arbiters_arbiter ON biz_demand_quote_arbiters(arbiter_pub_hex, quote_id)`,
	}
	for _, stmt := range stmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	return nil
}

func rebuildDemandQuoteFKTables(ctx context.Context, db *sql.DB, rebuildQuotes bool, rebuildArbiters bool) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := demandQuoteRejectOrphanRows(ctx, db); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}

	if rebuildQuotes {
		if err := rebuildDemandQuotesTableTx(ctx, tx); err != nil {
			rollback()
			return err
		}
	}
	if rebuildArbiters {
		if err := rebuildDemandQuoteArbitersTableTx(ctx, tx); err != nil {
			rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	if err := ensureDemandQuoteIndexes(ctx, db); err != nil {
		return err
	}
	return nil
}

func demandQuoteRejectOrphanRows(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if has, err := hasTable(db, "biz_demand_quotes"); err != nil {
		return err
	} else if has {
		var quoteID int64
		var demandID string
		err := QueryRowContext(ctx, db, `SELECT id,demand_id FROM biz_demand_quotes
			WHERE NOT EXISTS(SELECT 1 FROM biz_demands WHERE biz_demands.demand_id = biz_demand_quotes.demand_id)
			ORDER BY id ASC LIMIT 1`).Scan(&quoteID, &demandID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			return fmt.Errorf("biz_demand_quotes contains orphan row id=%d demand_id=%s", quoteID, strings.TrimSpace(demandID))
		}
	}
	if has, err := hasTable(db, "biz_demand_quote_arbiters"); err != nil {
		return err
	} else if has {
		var id int64
		var quoteID int64
		var arbiterPubHex string
		err := QueryRowContext(ctx, db, `SELECT id,quote_id,arbiter_pub_hex FROM biz_demand_quote_arbiters
			WHERE NOT EXISTS(SELECT 1 FROM biz_demand_quotes WHERE biz_demand_quotes.id = biz_demand_quote_arbiters.quote_id)
			ORDER BY id ASC LIMIT 1`).Scan(&id, &quoteID, &arbiterPubHex)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			return fmt.Errorf("biz_demand_quote_arbiters contains orphan row id=%d quote_id=%d arbiter_pub_hex=%s", id, quoteID, strings.TrimSpace(arbiterPubHex))
		}
	}
	return nil
}

func rebuildDemandQuotesTableTx(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_demand_quotes RENAME TO biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_demand_quotes(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		demand_id TEXT NOT NULL,
		seller_pub_hex TEXT NOT NULL,
		seed_price_satoshi INTEGER NOT NULL,
		chunk_price_satoshi INTEGER NOT NULL,
		chunk_count INTEGER NOT NULL,
		file_size_bytes INTEGER NOT NULL,
		recommended_file_name TEXT NOT NULL,
		mime_type TEXT NOT NULL,
		available_chunk_bitmap_hex TEXT NOT NULL,
		expires_at_unix INTEGER NOT NULL,
		created_at_unix INTEGER NOT NULL,
		FOREIGN KEY(demand_id) REFERENCES biz_demands(demand_id) ON DELETE CASCADE,
		UNIQUE(demand_id, seller_pub_hex)
	)`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `INSERT INTO biz_demand_quotes(
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) SELECT
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		FROM biz_demand_quotes_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	return nil
}

func rebuildDemandQuoteArbitersTableTx(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_demand_quote_arbiters RENAME TO biz_demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_demand_quote_arbiters(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		quote_id INTEGER NOT NULL,
		arbiter_pub_hex TEXT NOT NULL,
		FOREIGN KEY(quote_id) REFERENCES biz_demand_quotes(id) ON DELETE CASCADE,
		UNIQUE(quote_id, arbiter_pub_hex)
	)`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `INSERT INTO biz_demand_quote_arbiters(
			id,quote_id,arbiter_pub_hex
		) SELECT
			id,quote_id,arbiter_pub_hex
		FROM biz_demand_quote_arbiters_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE biz_demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	return nil
}

func demandQuoteTableMissingFK(ctx context.Context, db *sql.DB, table string, fromColumn string, parentTable string, parentColumn string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := QueryContext(ctx, db, fmt.Sprintf("PRAGMA foreign_key_list(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id       int
			seq      int
			refTable string
			from     string
			to       string
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(refTable), parentTable) &&
			strings.EqualFold(strings.TrimSpace(from), fromColumn) &&
			strings.EqualFold(strings.TrimSpace(to), parentColumn) {
			return false, nil
		}
	}
	return true, rows.Err()
}

func normalizePubHexList(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(in))
	seen := map[string]struct{}{}
	for _, raw := range in {
		pubHex, err := normalizeCompressedPubKeyHex(raw)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[pubHex]; ok {
			continue
		}
		seen[pubHex] = struct{}{}
		out = append(out, pubHex)
	}
	return out, nil
}

// ensureWorkspaceStorageSchema 迁移客户端本地库存的核心五张表。
// 设计说明：
// - 这里不做列级兼容补丁，直接按新模型重建；
// - 老表里的历史数据会一次性搬走，再删除；
// - 这样后续业务代码只面对唯一真相。
func ensureWorkspaceStorageSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := ensureWorkspaceStorageBaseTables(ctx, db); err != nil {
		return err
	}
	legacyWorkspaces, legacyFiles, legacySeeds, legacySupply, legacyPolicy, err := legacyWorkspaceStoragePresent(ctx, db)
	if err != nil {
		return err
	}
	if !legacyWorkspaces && !legacyFiles && !legacySeeds && !legacySupply && !legacyPolicy {
		return nil
	}
	return migrateWorkspaceStorageLegacy(ctx, db)
}

func ensureWorkspaceStorageBaseTables(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS biz_workspaces(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seeds(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS biz_workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path),
			FOREIGN KEY(workspace_path) REFERENCES biz_workspaces(workspace_path) ON DELETE CASCADE,
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seed_chunk_supply(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index),
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS biz_seed_pricing_policy(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(seed_hash) REFERENCES biz_seeds(seed_hash) ON DELETE CASCADE
		)`,
	}
	for _, stmt := range stmts {
		if _, err := ExecContext(ctx, db, stmt); err != nil {
			return err
		}
	}
	return nil
}

func legacyWorkspaceStoragePresent(ctx context.Context, db *sql.DB) (bool, bool, bool, bool, bool, error) {
	legacyWorkspaces := false
	legacyFiles := false
	legacySeeds := false
	legacySupply := false
	legacyPolicy := false

	if cols, err := tableColumns(db, "biz_workspaces"); err != nil {
		return false, false, false, false, false, err
	} else if len(cols) > 0 {
		if _, ok := cols["id"]; ok {
			legacyWorkspaces = true
		}
		if _, ok := cols["path"]; ok {
			legacyWorkspaces = true
		}
		if _, ok := cols["updated_at_unix"]; ok {
			legacyWorkspaces = true
		}
	}
	if cols, err := tableColumns(db, "biz_workspace_files"); err != nil {
		return false, false, false, false, false, err
	} else if len(cols) > 0 {
		if _, ok := cols["path"]; ok {
			legacyFiles = true
		}
		if _, ok := cols["file_size"]; ok {
			legacyFiles = true
		}
		if _, ok := cols["mtime_unix"]; ok {
			legacyFiles = true
		}
		if _, ok := cols["updated_at_unix"]; ok {
			legacyFiles = true
		}
	}
	if cols, err := tableColumns(db, "biz_seeds"); err != nil {
		return false, false, false, false, false, err
	} else if len(cols) > 0 {
		if _, ok := cols["created_at_unix"]; ok {
			legacySeeds = true
		}
	}
	if exists, err := hasTable(db, "seed_available_chunks"); err != nil {
		return false, false, false, false, false, err
	} else if exists {
		legacySupply = true
	}
	if exists, err := hasTable(db, "seed_price_state"); err != nil {
		return false, false, false, false, false, err
	} else if exists {
		legacyPolicy = true
	}
	if exists, err := hasTable(db, "static_file_prices"); err != nil {
		return false, false, false, false, false, err
	} else if exists {
		legacyPolicy = true
	}
	return legacyWorkspaces, legacyFiles, legacySeeds, legacySupply, legacyPolicy, nil
}

func migrateWorkspaceStorageLegacy(ctx context.Context, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_workspaces_new`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_workspace_files_new`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seeds_new`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seed_chunk_supply_new`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seed_pricing_policy_new`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_workspaces_new(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_workspace_files_new(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_seeds_new(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_seed_chunk_supply_new(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `CREATE TABLE biz_seed_pricing_policy_new(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`); err != nil {
		rollback()
		return err
	}
	if err := migrateWorkspaceRowsLegacy(ctx, tx); err != nil {
		rollback()
		return err
	}
	if err := migrateWorkspaceFileRowsLegacy(ctx, tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedRowsLegacy(ctx, tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedChunkSupplyLegacy(ctx, tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedPricingPolicyLegacy(ctx, tx); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS biz_seed_pricing_policy`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS seed_available_chunks`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS seed_price_state`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `DROP TABLE IF EXISTS static_file_prices`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_workspaces_new RENAME TO biz_workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_workspace_files_new RENAME TO biz_workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_seeds_new RENAME TO biz_seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_seed_chunk_supply_new RENAME TO biz_seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `ALTER TABLE biz_seed_pricing_policy_new RENAME TO biz_seed_pricing_policy`); err != nil {
		rollback()
		return err
	}
	if _, err := ExecContext(ctx, tx, `PRAGMA foreign_keys=ON`); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func migrateWorkspaceRowsLegacy(ctx context.Context, tx *sql.Tx) error {
	cols, err := tableColumnsTx(ctx, tx, "biz_workspaces")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok && !containsAny(cols, "id", "path", "updated_at_unix") {
		_, err = ExecContext(ctx, tx, `INSERT OR IGNORE INTO biz_workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) SELECT workspace_path,enabled,max_bytes,created_at_unix FROM biz_workspaces`)
		return err
	}
	rows, err := QueryContext(ctx, tx, `SELECT path,max_bytes,enabled,created_at_unix FROM biz_workspaces`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		var maxBytes int64
		var enabled int64
		var created int64
		if err := rows.Scan(&path, &maxBytes, &enabled, &created); err != nil {
			return err
		}
		abs, err := normalizeWorkspacePath(path)
		if err != nil {
			continue
		}
		if created <= 0 {
			created = 1
		}
		if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, abs, enabled, maxBytes, created); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateWorkspaceFileRowsLegacy(ctx context.Context, tx *sql.Tx) error {
	cols, err := tableColumnsTx(ctx, tx, "biz_workspace_files")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok {
		if _, ok2 := cols["file_path"]; ok2 && !containsAny(cols, "path", "file_size", "mtime_unix", "updated_at_unix") {
			_, err = ExecContext(ctx, tx, `INSERT OR IGNORE INTO biz_workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) SELECT workspace_path,file_path,lower(trim(seed_hash)),COALESCE(seed_locked,0) FROM biz_workspace_files`)
			return err
		}
	}
	rows, err := QueryContext(ctx, tx, `SELECT path,seed_hash,COALESCE(seed_locked,0) FROM biz_workspace_files`)
	if err != nil {
		return err
	}
	defer rows.Close()
	workspaceRoots, err := legacyWorkspaceRoots(ctx, tx)
	if err != nil {
		return err
	}
	for rows.Next() {
		var absPath, seedHash string
		var locked int64
		if err := rows.Scan(&absPath, &seedHash, &locked); err != nil {
			return err
		}
		resolved, ok := resolveWorkspaceRelativePath(absPath, workspaceRoots)
		if !ok {
			continue
		}
		if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, resolved.WorkspacePath, resolved.FilePath, normalizeSeedHashHex(seedHash), locked); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateSeedRowsLegacy(ctx context.Context, tx *sql.Tx) error {
	cols, err := tableColumnsTx(ctx, tx, "biz_seeds")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	rows, err := QueryContext(ctx, tx, `SELECT seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint FROM biz_seeds`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var seedHash string
		var chunkCount int64
		var fileSize int64
		var seedPath, recommendedName, mimeHint string
		if err := rows.Scan(&seedHash, &chunkCount, &fileSize, &seedPath, &recommendedName, &mimeHint); err != nil {
			return err
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			continue
		}
		if chunkCount < 0 {
			chunkCount = 0
		}
		if fileSize < 0 {
			fileSize = 0
		}
		if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_seeds_new(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, chunkCount, fileSize, strings.TrimSpace(seedPath), sanitizeRecommendedFileName(recommendedName), sanitizeMIMEHint(mimeHint)); err != nil {
			return err
		}
	}
	return rows.Err()
}

// 迁移规则：
// - 旧库有 seed_available_chunks 时，只信旧表记录；
// - 某个 seed 在旧表里没有记录，就迁空；
// - 只有旧库没有 seed_available_chunks 时，才按 chunk_count 补全。
func migrateSeedChunkSupplyLegacy(ctx context.Context, tx *sql.Tx) error {
	if _, err := ExecContext(ctx, tx, `DELETE FROM biz_seed_chunk_supply_new`); err != nil {
		return err
	}
	haveLegacySupply := hasTableValue(ctx, tx, "seed_available_chunks")
	rows, err := QueryContext(ctx, tx, `SELECT seed_hash,chunk_count FROM biz_seeds_new`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var seedHash string
		var chunkCount int64
		if err := rows.Scan(&seedHash, &chunkCount); err != nil {
			return err
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			continue
		}
		if haveLegacySupply {
			supplyRows, err := QueryContext(ctx, tx, `SELECT chunk_index FROM seed_available_chunks WHERE seed_hash=? ORDER BY chunk_index ASC`, seedHash)
			if err != nil {
				return err
			}
			for supplyRows.Next() {
				var idx int64
				if err := supplyRows.Scan(&idx); err != nil {
					_ = supplyRows.Close()
					return err
				}
				if idx < 0 {
					continue
				}
				if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, uint32(idx)); err != nil {
					_ = supplyRows.Close()
					return err
				}
			}
			if err := supplyRows.Err(); err != nil {
				_ = supplyRows.Close()
				return err
			}
			_ = supplyRows.Close()
			continue
		}
		if chunkCount < 0 {
			chunkCount = 0
		}
		for _, idx := range contiguousChunkIndexes(uint32(chunkCount)) {
			if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, idx); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func migrateSeedPricingPolicyLegacy(ctx context.Context, tx *sql.Tx) error {
	if _, err := ExecContext(ctx, tx, `DELETE FROM biz_seed_pricing_policy_new`); err != nil {
		return err
	}
	if !hasTableValue(ctx, tx, "seed_price_state") {
		return nil
	}
	now := time.Now().Unix()
	seedRows, err := QueryContext(ctx, tx, `SELECT seed_hash FROM biz_seeds_new`)
	if err != nil {
		return err
	}
	defer seedRows.Close()
	for seedRows.Next() {
		var seedHash string
		if err := seedRows.Scan(&seedHash); err != nil {
			return err
		}
		seedHash = normalizeSeedHashHex(seedHash)
		if seedHash == "" {
			continue
		}
		floor := uint64(0)
		resale := uint64(0)
		source := "system"
		var lastBuy sql.NullInt64
		var floorInt sql.NullInt64
		var resaleInt sql.NullInt64
		if err := QueryRowContext(ctx, tx, `SELECT last_buy_unit_price_sat_per_64k,floor_unit_price_sat_per_64k,resale_discount_bps FROM seed_price_state WHERE seed_hash=?`, seedHash).Scan(&lastBuy, &floorInt, &resaleInt); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return err
		}
		if floorInt.Valid && floorInt.Int64 > 0 {
			floor = uint64(floorInt.Int64)
		}
		if resaleInt.Valid && resaleInt.Int64 >= 0 {
			resale = uint64(resaleInt.Int64)
		}
		if lastBuy.Valid && lastBuy.Int64 > 0 {
			source = "user"
		}
		if floor == 0 {
			continue
		}
		if resale > 10000 {
			resale = 10000
		}
		if _, err := ExecContext(ctx, tx, `INSERT OR REPLACE INTO biz_seed_pricing_policy_new(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix) VALUES(?,?,?,?,?)`, seedHash, floor, resale, source, now); err != nil {
			return err
		}
	}
	return seedRows.Err()
}

func legacyWorkspaceRoots(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := QueryContext(ctx, tx, `SELECT workspace_path FROM biz_workspaces`)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 8)
	for rows.Next() {
		var root string
		if err := rows.Scan(&root); err != nil {
			return nil, err
		}
		if root, err = normalizeWorkspacePath(root); err == nil && root != "" {
			out = append(out, root)
		}
	}
	if len(out) == 0 {
		rows, err = QueryContext(ctx, tx, `SELECT path FROM biz_workspaces`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var root string
			if err := rows.Scan(&root); err != nil {
				return nil, err
			}
			if root, err = normalizeWorkspacePath(root); err == nil && root != "" {
				out = append(out, root)
			}
		}
	}
	return out, nil
}

func tableColumnsTx(ctx context.Context, tx *sql.Tx, table string) (map[string]struct{}, error) {
	rows, err := QueryContext(ctx, tx, fmt.Sprintf("PRAGMA table_info(%s)", strings.TrimSpace(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	return out, rows.Err()
}

func containsAny(cols map[string]struct{}, names ...string) bool {
	for _, name := range names {
		if _, ok := cols[strings.ToLower(strings.TrimSpace(name))]; ok {
			return true
		}
	}
	return false
}

func hasTableValue(ctx context.Context, tx *sql.Tx, table string) bool {
	var one int
	err := QueryRowContext(ctx, tx, `SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(table)).Scan(&one)
	return err == nil
}

// ensureFileDownloadsSchema 处理 proc_file_downloads 表的历史列迁移
func ensureFileDownloadsSchema(ctx context.Context, db *sql.DB) error {
	rows, err := QueryContext(ctx, db, `PRAGMA table_info(proc_file_downloads)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasStatusJSON := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "status_json") {
			hasStatusJSON = true
			break
		}
	}

	if hasStatusJSON {
		return nil
	}
	_, err = ExecContext(ctx, db, `ALTER TABLE proc_file_downloads ADD COLUMN status_json TEXT NOT NULL DEFAULT '{}'`)
	return err
}

// ensureLiveFollowsSchema 处理 proc_live_follows 表的历史列迁移
func ensureLiveFollowsSchema(ctx context.Context, db *sql.DB) error {
	rows, err := QueryContext(ctx, db, `PRAGMA table_info(proc_live_follows)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasLastQuoteSellerPubKey := false
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(strings.TrimSpace(name), "last_quote_seller_pubkey_hex") {
			hasLastQuoteSellerPubKey = true
			break
		}
	}

	if hasLastQuoteSellerPubKey {
		return nil
	}
	_, err = ExecContext(ctx, db, `ALTER TABLE proc_live_follows ADD COLUMN last_quote_seller_pubkey_hex TEXT NOT NULL DEFAULT ''`)
	return err
}

// ensureWalletUTXOSchema 处理 wallet_utxo 表的历史列迁移和表结构重构
func ensureWalletUTXOSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	cols, err := tableColumns(db, "wallet_utxo")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}

	// 检测旧版本表结构（有 origin_type 列的是旧版）
	if _, hasOrigin := cols["origin_type"]; hasOrigin {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = tx.Rollback()
			}
		}()

		if _, err = ExecContext(ctx, tx, `ALTER TABLE wallet_utxo RENAME TO wallet_utxo_legacy_v2`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `CREATE TABLE wallet_utxo(
			utxo_id TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			value_satoshi INTEGER NOT NULL,
			state TEXT NOT NULL,
			allocation_class TEXT NOT NULL DEFAULT 'plain_bsv',
			allocation_reason TEXT NOT NULL DEFAULT '',
			created_txid TEXT NOT NULL,
			spent_txid TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			spent_at_unix INTEGER NOT NULL
		)`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx,
			`INSERT INTO wallet_utxo(
				utxo_id,wallet_id,address,txid,vout,value_satoshi,state,allocation_class,allocation_reason,created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
			)
			SELECT utxo_id,wallet_id,address,txid,vout,value_satoshi,
				CASE WHEN lower(trim(state))='reserved' THEN 'unspent' ELSE state END,
				CASE WHEN value_satoshi=1 THEN 'unknown' ELSE 'plain_bsv' END,
				CASE WHEN value_satoshi=1 THEN 'awaiting external token evidence' ELSE '' END,
				created_txid,spent_txid,created_at_unix,updated_at_unix,spent_at_unix
			FROM wallet_utxo_legacy_v2`,
		); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `DROP TABLE wallet_utxo_legacy_v2`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `DROP INDEX IF EXISTS idx_wallet_utxo_origin`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `CREATE INDEX IF NOT EXISTS idx_wallet_utxo_alloc ON wallet_utxo(wallet_id, address, state, allocation_class, created_at_unix ASC, value_satoshi ASC, txid, vout)`); err != nil {
			return err
		}
		if _, err = ExecContext(ctx, tx, `CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	// 确保新列存在
	cols, err = tableColumns(db, "wallet_utxo")
	if err != nil {
		return err
	}
	if _, ok := cols["allocation_class"]; !ok {
		if _, err := ExecContext(ctx, db, `ALTER TABLE wallet_utxo ADD COLUMN allocation_class TEXT NOT NULL DEFAULT 'plain_bsv'`); err != nil {
			return err
		}
	}
	if _, ok := cols["allocation_reason"]; !ok {
		if _, err := ExecContext(ctx, db, `ALTER TABLE wallet_utxo ADD COLUMN allocation_reason TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}

	// 数据修复
	if _, err := ExecContext(ctx, db, `UPDATE wallet_utxo SET allocation_class=CASE WHEN value_satoshi=1 THEN 'unknown' ELSE 'plain_bsv' END WHERE trim(allocation_class)=''`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, db, `UPDATE wallet_utxo SET allocation_reason='awaiting external token evidence' WHERE state='unspent' AND value_satoshi=1 AND allocation_class='unknown' AND trim(allocation_reason)=''`); err != nil {
		return err
	}
	if _, err := ExecContext(ctx, db, `CREATE INDEX IF NOT EXISTS idx_wallet_utxo_alloc ON wallet_utxo(wallet_id, address, state, allocation_class, created_at_unix ASC, value_satoshi ASC, txid, vout)`); err != nil {
		return err
	}
	return nil
}

func ensureFactChainPaymentTimingSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	for _, tableName := range []string{
		"fact_settlement_channel_chain_quote_pay",
		"fact_settlement_channel_chain_direct_pay",
		"fact_settlement_channel_chain_asset_create",
	} {
		cols, err := tableColumns(db, tableName)
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			continue
		}
		if _, ok := cols["submitted_at_unix"]; !ok {
			if _, err := ExecContext(ctx, db, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN submitted_at_unix INTEGER NOT NULL DEFAULT 0`, tableName)); err != nil {
				return err
			}
		}
		if _, ok := cols["wallet_observed_at_unix"]; !ok {
			if _, err := ExecContext(ctx, db, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0`, tableName)); err != nil {
				return err
			}
		}
	}
	poolCols, err := tableColumns(db, "fact_settlement_channel_pool_session_quote_pay")
	if err != nil {
		return err
	}
	if len(poolCols) > 0 {
		if _, ok := poolCols["txid"]; !ok {
			if _, err := ExecContext(ctx, db, `ALTER TABLE fact_settlement_channel_pool_session_quote_pay ADD COLUMN txid TEXT NOT NULL DEFAULT ''`); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureWalletUTXOSyncStateSchema 处理 wallet_utxo_sync_state 表的历史列迁移
func ensureWalletUTXOSyncStateSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	cols, err := tableColumns(db, "wallet_utxo_sync_state")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}

	migrations := []struct {
		col  string
		stmt string
	}{
		{"last_sync_round_id", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_sync_round_id TEXT NOT NULL DEFAULT ''`},
		{"last_failed_step", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_failed_step TEXT NOT NULL DEFAULT ''`},
		{"last_upstream_path", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_upstream_path TEXT NOT NULL DEFAULT ''`},
		{"last_http_status", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN last_http_status INTEGER NOT NULL DEFAULT 0`},
		{"plain_bsv_utxo_count", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN plain_bsv_utxo_count INTEGER NOT NULL DEFAULT 0`},
		{"plain_bsv_balance_satoshi", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN plain_bsv_balance_satoshi INTEGER NOT NULL DEFAULT 0`},
		{"protected_utxo_count", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN protected_utxo_count INTEGER NOT NULL DEFAULT 0`},
		{"protected_balance_satoshi", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN protected_balance_satoshi INTEGER NOT NULL DEFAULT 0`},
		{"unknown_utxo_count", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN unknown_utxo_count INTEGER NOT NULL DEFAULT 0`},
		{"unknown_balance_satoshi", `ALTER TABLE wallet_utxo_sync_state ADD COLUMN unknown_balance_satoshi INTEGER NOT NULL DEFAULT 0`},
	}

	for _, m := range migrations {
		if _, ok := cols[m.col]; !ok {
			if _, err := ExecContext(ctx, db, m.stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

// normalizeClientPubKeyColumns 把历史库里的旧格式公钥统一迁移为压缩公钥 hex（02/03）
func normalizeClientPubKeyColumns(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	targets := []struct {
		table      string
		column     string
		allowEmpty bool
	}{
		{table: "proc_direct_deals", column: "buyer_pubkey_hex"},
		{table: "proc_direct_deals", column: "seller_pubkey_hex"},
		{table: "proc_direct_transfer_pools", column: "buyer_pubkey_hex"},
		{table: "proc_direct_transfer_pools", column: "seller_pubkey_hex"},
		{table: "fact_settlement_channel_pool_session_quote_pay", column: "counterparty_pubkey_hex", allowEmpty: true},
		{table: "fact_settlement_channel_pool_session_quote_pay", column: "seller_pubkey_hex", allowEmpty: true},
		{table: "fact_settlement_channel_pool_session_quote_pay", column: "arbiter_pubkey_hex", allowEmpty: true},
		{table: "fact_settlement_channel_pool_session_quote_pay", column: "gateway_pubkey_hex", allowEmpty: true},
		{table: "biz_live_quotes", column: "seller_pubkey_hex"},
		{table: "proc_live_follows", column: "last_quote_seller_pubkey_hex", allowEmpty: true},
		{table: "proc_file_download_chunks", column: "seller_pubkey_hex", allowEmpty: true},
	}

	for _, t := range targets {
		exists, err := hasTable(db, t.table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if err := normalizeClientPubKeyColumn(db, t.table, t.column, t.allowEmpty); err != nil {
			return fmt.Errorf("normalize %s.%s failed: %w", t.table, t.column, err)
		}
	}
	return nil
}

func normalizeClientPubKeyColumn(db *sql.DB, table, column string, allowEmpty bool) error {
	rows, err := db.Query(fmt.Sprintf("SELECT rowid,%s FROM %s", strings.TrimSpace(column), strings.TrimSpace(table)))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rowID int64
		var raw string
		if err := rows.Scan(&rowID, &raw); err != nil {
			return err
		}
		raw = strings.TrimSpace(raw)
		if raw == "" && allowEmpty {
			continue
		}
		norm, err := normalizeCompressedPubKeyHexLegacyAware(raw)
		if err != nil {
			if allowEmpty && raw == "" {
				continue
			}
			return err
		}
		if strings.EqualFold(raw, norm) {
			continue
		}
		_, err = db.Exec(
			fmt.Sprintf("UPDATE %s SET %s=? WHERE rowid=?", strings.TrimSpace(table), strings.TrimSpace(column)),
			norm,
			rowID,
		)
		if err == nil {
			continue
		}
		// 处理唯一键冲突：同一业务行已存在新格式时，删除旧格式重复行
		if strings.Contains(strings.ToLower(err.Error()), "unique constraint failed") {
			if _, delErr := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE rowid=?", strings.TrimSpace(table)), rowID); delErr != nil {
				return delErr
			}
			continue
		}
		return err
	}
	return rows.Err()
}

// cleanupLegacyCyclePayFinanceRows 清理不应存在于财务主表的 cycle_pay 过程事件。
// 这里只清理 settle_records 中不该保留的过程型残行，不碰旧表。
func cleanupLegacyCyclePayFinanceRows(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 第六次迭代：检查旧字段是否存在，不存在则跳过（全新数据库无旧数据需要清理）
	cols, err := tableColumns(db, "settle_records")
	if err != nil {
		return fmt.Errorf("inspect settle_records columns: %w", err)
	}
	if _, hasOldSceneType := cols["scene_type"]; !hasOldSceneType {
		// 全新数据库，无旧字段，无需清理
		return nil
	}

	// 设计说明：
	// - sqliteactor 把运行时压成单连接后，事务期间不能再回头走库级 Query；
	// - 这里先在事务外判断旧表是否存在，避免同一函数里"持有 tx 又重新借 db"把自己堵死。
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = ExecContext(ctx, tx,
		`DELETE FROM settle_records
		 WHERE business_id IN (
			 SELECT business_id FROM settle_records
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = ExecContext(ctx, tx,
		`DELETE FROM settle_records
		 WHERE business_id IN (
			 SELECT business_id FROM settle_records
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = ExecContext(ctx, tx, `DELETE FROM settle_records WHERE source_type='fee_pool' AND accounting_subtype='cycle_pay'`); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

// ==================== 辅助函数 ====================

// hasTable 检查表是否存在
func hasTable(db *sql.DB, name string) (bool, error) {
	return hasSchemaObject(db, name)
}

func hasRealTable(db *sql.DB, name string) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(name)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func rejectLegacyClientDBSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	legacyTables := []string{
		strings.Join([]string{"fact", "asset", "consumptions"}, "_"),
		strings.Join([]string{"fact", "pool", "allocations"}, "_"),
		strings.Join([]string{"fact", "tx", "history"}, "_"),
		strings.Join([]string{"fact", "chain", "asset", "flows"}, "_"),
		strings.Join([]string{"fact", "bsv", "consumptions"}, "_"),
		strings.Join([]string{"fact", "token", "consumptions"}, "_"),
		strings.Join([]string{"fact", "token", "utxo", "links"}, "_"),
	}
	for _, table := range legacyTables {
		exists, err := hasRealTable(db, table)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("legacy database schema detected, please rebuild DB")
		}
	}
	return nil
}

func hasSchemaObject(db *sql.DB, name string) (bool, error) {
	var one int
	err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name=? LIMIT 1`, strings.TrimSpace(name)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// isSettleRecordsFinalized 判断结算主表是否已经完成硬切。
// 说明：只要仍然能看到旧表、缺少唯一约束或缺少关键索引，就不能跳过。
func isSettleRecordsFinalized(ctx context.Context, db *sql.DB) (bool, error) {
	hasRecord, err := hasTable(db, "settle_records")
	if err != nil {
		return false, err
	}
	if !hasRecord {
		return false, nil
	}

	cols, err := tableColumns(db, "settle_records")
	if err != nil {
		return false, err
	}
	for _, col := range []string{"settlement_id", "business_role", "idempotency_key", "settlement_method", "settlement_status", "target_type", "target_id"} {
		if _, ok := cols[col]; !ok {
			return false, nil
		}
	}
	hasUnique, err := tableHasUniqueIndexOnColumns(db, "settle_records", []string{"idempotency_key"})
	if err != nil {
		return false, err
	}
	if !hasUnique {
		return false, nil
	}

	for _, indexName := range []string{
		"idx_settle_records_role",
		"idx_settle_records_source",
		"idx_settle_records_accounting",
		"idx_settle_records_status",
		"idx_settle_records_method",
		"idx_settle_records_target",
	} {
		hasIndex, err := tableHasIndex(db, "settle_records", indexName)
		if err != nil {
			return false, err
		}
		if !hasIndex {
			return false, nil
		}
	}

	return true, nil
}

// tableColumns 获取表的所有列名
func tableColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", strings.TrimSpace(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	return out, rows.Err()
}

// tableColumnNotNull 检查指定列是否存在且为 NOT NULL。
func tableColumnNotNull(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(column)) {
			return notnull != 0, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// tableHasIndex 检查指定表是否存在给定索引名。
func tableHasIndex(db *sql.DB, table, indexName string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_list(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(indexName)) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// tableHasUniqueIndexOnColumns 检查指定表是否已经有目标唯一约束。
// 这里接受 SQLite 的隐式唯一索引和显式唯一索引，只看列组合是否一致。
func tableHasUniqueIndexOnColumns(db *sql.DB, table string, columns []string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_list(%s)", strings.TrimSpace(table)))
	if err != nil {
		return false, err
	}

	want := make([]string, 0, len(columns))
	for _, col := range columns {
		want = append(want, strings.ToLower(strings.TrimSpace(col)))
	}

	uniqueIndexes := make([]string, 0, 8)
	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			rows.Close()
			return false, err
		}
		if unique == 0 {
			continue
		}
		uniqueIndexes = append(uniqueIndexes, name)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	if err := rows.Close(); err != nil {
		return false, err
	}

	for _, name := range uniqueIndexes {
		idxCols, err := tableIndexColumns(db, name)
		if err != nil {
			return false, err
		}
		if len(idxCols) != len(want) {
			continue
		}
		match := true
		for i := range want {
			if idxCols[i] != want[i] {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

// tableIndexColumns 读取单个索引覆盖的列顺序。
func tableIndexColumns(db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", strings.TrimSpace(indexName)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type indexCol struct {
		seq  int
		name string
	}
	cols := make([]indexCol, 0, 4)
	for rows.Next() {
		var seqno int
		var cid int
		var name sql.NullString
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		if !name.Valid {
			return nil, fmt.Errorf("index %s has unnamed column at seq %d", indexName, seqno)
		}
		cols = append(cols, indexCol{seq: seqno, name: strings.ToLower(strings.TrimSpace(name.String))})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, len(cols))
	for _, col := range cols {
		if col.seq < 0 || col.seq >= len(cols) {
			return nil, fmt.Errorf("index %s has invalid seq %d", indexName, col.seq)
		}
		out[col.seq] = col.name
	}
	return out, nil
}

// inferFinTxBreakdownTxRole 从业务标识和业务主表推断 tx_role。
func inferFinTxBreakdownTxRole(db *sql.DB, bid, txid string) (string, error) {
	switch {
	case strings.HasPrefix(bid, "biz_feepool_open_") || strings.HasPrefix(bid, "biz_c2c_open_"):
		return "open_base", nil
	case strings.HasPrefix(bid, "biz_c2c_pay_"):
		// 遗留兼容：旧数据可能仍有 biz_c2c_pay_*，新代码不再创建
		return "pay", nil
	case strings.HasPrefix(bid, "biz_c2c_close_"):
		return "close_final", nil
	default:
		return "", fmt.Errorf("cannot infer tx_role for (%s,%s): unknown business type", bid, txid)
	}
}

// rawJSONPayload 用于标记原始 JSON 字符串类型
type rawJSONPayload string
