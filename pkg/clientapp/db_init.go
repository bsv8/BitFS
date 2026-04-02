package clientapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ensureClientDBSchema 是客户端数据库结构就绪的唯一入口。
// 设计说明：
// - 传入已经打开好的 *clientDB，负责把 sqlite 表、索引、迁移、数据规范化全部补齐；
// - 它是"库已打开后的结构就绪"，不是"打开数据库"；
// - 也不负责"运行时每次取 store 顺手检查"；
// - 调用方（如 Run）只需在打开数据库后调用一次即可。
func ensureClientDBSchema(store *clientDB) error {
	if store == nil {
		return fmt.Errorf("store is nil")
	}
	return store.Do(context.Background(), func(db *sql.DB) error {
		return ensureClientDBSchemaOnDB(db)
	})
}

// ensureClientDBSchemaOnDB 从原始 sql.DB 初始化数据库结构。
// 设计说明：
// - 用于测试场景，测试代码直接持有 *sql.DB 而无 actor；
// - 生产代码应使用 ensureClientDBSchema(store *clientDB)。
func ensureClientDBSchemaOnDB(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 1. 基础表和索引（不依赖迁移前置条件）
	if err := ensureClientDBBaseSchema(db); err != nil {
		return fmt.Errorf("base schema: %w", err)
	}

	// 2. 兼容迁移（历史列补齐、老表迁移、老数据搬运）
	if err := migrateClientDBLegacySchema(db); err != nil {
		return fmt.Errorf("legacy migration: %w", err)
	}

	// 3. 数据规范化（历史脏数据口径纠偏）
	if err := normalizeClientDBData(db); err != nil {
		return fmt.Errorf("data normalization: %w", err)
	}

	return nil
}

// initIndexDB 是数据库初始化的旧入口，保留给测试代码兼容使用。
// 设计说明：
// - 新代码应直接使用 ensureClientDBSchema 或 ensureClientDBSchemaOnDB；
// - 该函数仅作为测试兼容层，内部包装新的分层初始化逻辑。
func initIndexDB(db *sql.DB) error {
	return ensureClientDBSchemaOnDB(db)
}

// ensureClientDBBaseSchema 创建基础表和索引。
// 只负责 CREATE TABLE IF NOT EXISTS 和 CREATE INDEX IF NOT EXISTS，
// 不包含任何依赖前置迁移条件的操作。
func ensureClientDBBaseSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	stmts := []string{
		// 核心表
		`CREATE TABLE IF NOT EXISTS workspaces(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS seeds(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path),
			FOREIGN KEY(workspace_path) REFERENCES workspaces(workspace_path) ON DELETE CASCADE,
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS seed_chunk_supply(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index),
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS seed_pricing_policy(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS demands(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id)
		)`,

		// 交易历史
		`CREATE TABLE IF NOT EXISTS tx_history(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			event_type TEXT NOT NULL,
			direction TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			purpose TEXT NOT NULL,
			note TEXT NOT NULL,
			pool_id TEXT NOT NULL,
			msg_id TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			cycle_index INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS purchases(
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
			FOREIGN KEY(demand_id) REFERENCES demands(demand_id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS gateway_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			action TEXT NOT NULL,
			msg_id TEXT NOT NULL,
			sequence_num INTEGER NOT NULL,
			pool_id TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		)`,

		// 直接交易
		`CREATE TABLE IF NOT EXISTS demand_quotes(
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
			FOREIGN KEY(demand_id) REFERENCES demands(demand_id) ON DELETE CASCADE,
			UNIQUE(demand_id, seller_pub_hex)
		)`,
		`CREATE TABLE IF NOT EXISTS demand_quote_arbiters(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			quote_id INTEGER NOT NULL,
			arbiter_pub_hex TEXT NOT NULL,
			FOREIGN KEY(quote_id) REFERENCES demand_quotes(id) ON DELETE CASCADE,
			UNIQUE(quote_id, arbiter_pub_hex)
		)`,
		// 运行期辅助表：只保存 direct transfer 的 deal 上下文，不承载对外事实语义。
		`CREATE TABLE IF NOT EXISTS direct_deals(
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
		`CREATE TABLE IF NOT EXISTS live_quotes(
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
		`CREATE TABLE IF NOT EXISTS direct_transfer_pools(
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
			status TEXT NOT NULL,
			fee_rate_sat_byte REAL NOT NULL,
			lock_blocks INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		// 钱包资金流水
		`CREATE TABLE IF NOT EXISTS wallet_fund_flows(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			visit_id TEXT NOT NULL DEFAULT '',
			visit_locator TEXT NOT NULL DEFAULT '',
			flow_id TEXT NOT NULL,
			flow_type TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			stage TEXT NOT NULL,
			direction TEXT NOT NULL,
			purpose TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			used_satoshi INTEGER NOT NULL,
			returned_satoshi INTEGER NOT NULL,
			related_txid TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,

		// 命令日志
		`CREATE TABLE IF NOT EXISTS command_journal(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
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
			payload_json TEXT NOT NULL,
			result_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS domain_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			event_name TEXT NOT NULL,
			state_before TEXT NOT NULL,
			state_after TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS state_snapshots(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			state TEXT NOT NULL,
			pause_reason TEXT NOT NULL,
			pause_need_satoshi INTEGER NOT NULL,
			pause_have_satoshi INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS effect_logs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			command_id TEXT NOT NULL,
			gateway_pubkey_hex TEXT NOT NULL,
			effect_type TEXT NOT NULL,
			stage TEXT NOT NULL,
			status TEXT NOT NULL,
			error_message TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,

		// 编排器日志
		`CREATE TABLE IF NOT EXISTS orchestrator_logs(
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

		// 钱包账本
		`CREATE TABLE IF NOT EXISTS wallet_ledger_entries(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			txid TEXT NOT NULL,
			direction TEXT NOT NULL,
			category TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			counterparty_label TEXT NOT NULL,
			status TEXT NOT NULL,
			block_height INTEGER NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			raw_ref_id TEXT NOT NULL,
			note TEXT NOT NULL,
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
		`CREATE TABLE IF NOT EXISTS wallet_utxo_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			utxo_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			ref_txid TEXT NOT NULL,
			ref_business_id TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_utxo_assets(
			utxo_id TEXT NOT NULL,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			asset_group TEXT NOT NULL,
			asset_standard TEXT NOT NULL,
			asset_key TEXT NOT NULL,
			asset_symbol TEXT NOT NULL,
			quantity_text TEXT NOT NULL,
			source_name TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(utxo_id, asset_group, asset_standard, asset_key)
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_local_broadcast_txs(
			txid TEXT PRIMARY KEY,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			tx_hex TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			observed_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS wallet_bsv21_create_status(
			token_id TEXT PRIMARY KEY,
			create_txid TEXT NOT NULL,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			token_standard TEXT NOT NULL,
			symbol TEXT NOT NULL,
			max_supply TEXT NOT NULL,
			decimals INTEGER NOT NULL,
			icon TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			submitted_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
			last_check_at_unix INTEGER NOT NULL DEFAULT 0,
			next_auto_check_at_unix INTEGER NOT NULL DEFAULT 0,
			updated_at_unix INTEGER NOT NULL,
			last_check_error TEXT NOT NULL DEFAULT ''
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
		`CREATE TABLE IF NOT EXISTS wallet_utxo_history_cursor(
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
		`CREATE TABLE IF NOT EXISTS fin_business(
			business_id TEXT PRIMARY KEY,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			source_type TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			accounting_scene TEXT NOT NULL DEFAULT '',
			accounting_subtype TEXT NOT NULL DEFAULT '',
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_process_events(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			process_id TEXT NOT NULL,
			scene_type TEXT NOT NULL,
			scene_subtype TEXT NOT NULL,
			source_type TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			accounting_scene TEXT NOT NULL DEFAULT '',
			accounting_subtype TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL,
			status TEXT NOT NULL,
			ref_id TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_tx_breakdown(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			gross_input_satoshi INTEGER NOT NULL,
			change_back_satoshi INTEGER NOT NULL,
			external_in_satoshi INTEGER NOT NULL,
			counterparty_out_satoshi INTEGER NOT NULL,
			miner_fee_satoshi INTEGER NOT NULL,
			net_out_satoshi INTEGER NOT NULL,
			net_in_satoshi INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS fin_business_txs(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			tx_role TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			UNIQUE(business_id,txid)
		)`,
		`CREATE TABLE IF NOT EXISTS fin_tx_utxo_links(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			utxo_id TEXT NOT NULL,
			io_side TEXT NOT NULL,
			utxo_role TEXT NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			UNIQUE(business_id,txid,utxo_id,io_side,utxo_role)
		)`,

		// 链状态
		`CREATE TABLE IF NOT EXISTS chain_tip_state(
			id INTEGER PRIMARY KEY CHECK(id=1),
			tip_height INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			last_error TEXT NOT NULL,
			last_updated_by TEXT NOT NULL,
			last_trigger TEXT NOT NULL,
			last_duration_ms INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chain_tip_worker_logs(
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
		`CREATE TABLE IF NOT EXISTS chain_utxo_worker_logs(
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
		`CREATE TABLE IF NOT EXISTS scheduler_tasks(
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
		`CREATE TABLE IF NOT EXISTS scheduler_task_runs(
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
		`CREATE TABLE IF NOT EXISTS live_follows(
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
		`CREATE TABLE IF NOT EXISTS file_downloads(
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
		`CREATE TABLE IF NOT EXISTS file_download_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			status TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			price_sats INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`,

		// 消息收件箱
		`CREATE TABLE IF NOT EXISTS inbox_messages(
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
		`CREATE TABLE IF NOT EXISTS published_route_indexes(
			route TEXT PRIMARY KEY,
			seed_hash TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,

		// 节点可达性
		`CREATE TABLE IF NOT EXISTS node_reachability_cache(
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
		`CREATE TABLE IF NOT EXISTS self_node_reachability_state(
			node_pubkey_hex TEXT PRIMARY KEY,
			head_height INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,

		// 基础索引（不依赖迁移的）
		`CREATE INDEX IF NOT EXISTS idx_workspace_files_seed_hash ON workspace_files(seed_hash, workspace_path, file_path)`,
		`CREATE INDEX IF NOT EXISTS idx_workspace_files_workspace ON workspace_files(workspace_path, file_path)`,
		`CREATE INDEX IF NOT EXISTS idx_seed_chunk_supply_seed ON seed_chunk_supply(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_workspaces_enabled ON workspaces(enabled, workspace_path)`,
		`CREATE INDEX IF NOT EXISTS idx_seed_pricing_policy_updated ON seed_pricing_policy(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_file_downloads_updated ON file_downloads(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_file_download_chunks_seed ON file_download_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_live_quotes_demand ON live_quotes(demand_id, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_node_reachability_cache_expires ON node_reachability_cache(expires_at_unix DESC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_tx_history_created_at ON tx_history(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_purchases_created_at ON purchases(created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_purchases_demand_created ON purchases(demand_id, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_purchases_seller_created ON purchases(seller_pub_hex, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_purchases_status_created ON purchases(status, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_purchases_history_lookup ON purchases(demand_id, chunk_index, seller_pub_hex, arbiter_pub_hex, created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_gateway_events_created_at ON gateway_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_fund_flows_created_at ON wallet_fund_flows(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_fund_flows_flow_id ON wallet_fund_flows(flow_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_created_at ON command_journal(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_cmd_id ON command_journal(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_command_journal_gateway ON command_journal(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_created_at ON domain_events(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_cmd_id ON domain_events(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_domain_events_gateway ON domain_events(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_state_snapshots_created_at ON state_snapshots(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_state_snapshots_gateway ON state_snapshots(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_created_at ON effect_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_cmd_id ON effect_logs(command_id)`,
		`CREATE INDEX IF NOT EXISTS idx_effect_logs_gateway ON effect_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_created_at ON orchestrator_logs(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_event_type ON orchestrator_logs(event_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_signal_type ON orchestrator_logs(signal_type, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_gateway ON orchestrator_logs(gateway_pubkey_hex, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_orchestrator_logs_idempotency ON orchestrator_logs(idempotency_key, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_created_at ON wallet_ledger_entries(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_occurred_at ON wallet_ledger_entries(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_txid ON wallet_ledger_entries(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_ledger_entries_direction_category ON wallet_ledger_entries(direction, category, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_events_utxo ON wallet_utxo_events(utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_events_business ON wallet_utxo_events(ref_business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_assets_wallet ON wallet_utxo_assets(wallet_id, address, asset_group, asset_standard, asset_key, updated_at_unix DESC, utxo_id ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_assets_utxo ON wallet_utxo_assets(utxo_id, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_local_broadcast_wallet ON wallet_local_broadcast_txs(wallet_id, address, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_bsv21_create_status_state ON wallet_bsv21_create_status(status, next_auto_check_at_unix ASC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_bsv21_create_status_wallet ON wallet_bsv21_create_status(wallet_id, address, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_history_cursor_round_tip ON wallet_utxo_history_cursor(round_tip_height DESC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_scene ON fin_business(scene_type, scene_subtype, occurred_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fin_business_idempotency ON fin_business(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_process_events_scene ON fin_process_events(scene_type, scene_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_process_events_process ON fin_process_events(process_id, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fin_process_events_idempotency ON fin_process_events(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_breakdown_business ON fin_tx_breakdown(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_breakdown_txid ON fin_tx_breakdown(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_txs_business ON fin_business_txs(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_business_txs_txid ON fin_business_txs(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_business ON fin_tx_utxo_links(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_utxo ON fin_tx_utxo_links(utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fin_tx_utxo_links_txid ON fin_tx_utxo_links(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_tip_worker_logs_started ON chain_tip_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_tip_worker_logs_status ON chain_tip_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_utxo_worker_logs_started ON chain_utxo_worker_logs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_chain_utxo_worker_logs_status ON chain_utxo_worker_logs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_status ON scheduler_tasks(status, updated_at_unix DESC, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_owner_mode ON scheduler_tasks(owner, mode, task_name ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_task ON scheduler_task_runs(task_name, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_status ON scheduler_task_runs(status, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_scheduler_task_runs_started ON scheduler_task_runs(started_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_inbox_messages_received_at ON inbox_messages(received_at_unix DESC,id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_published_route_indexes_updated ON published_route_indexes(updated_at_unix DESC,route ASC)`,
		`CREATE INDEX IF NOT EXISTS idx_demands_demand_id ON demands(demand_id)`,
		`CREATE INDEX IF NOT EXISTS idx_demands_created ON demands(created_at_unix DESC, id DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_demand_quotes_demand_seller ON demand_quotes(demand_id, seller_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_demand_quotes_demand_created ON demand_quotes(demand_id, created_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_demand_quote_arbiters_quote_arbiter ON demand_quote_arbiters(quote_id, arbiter_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_demand_quote_arbiters_arbiter ON demand_quote_arbiters(arbiter_pub_hex, quote_id)`,
	}

	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("exec schema stmt: %w", err)
		}
	}
	return nil
}

// migrateClientDBLegacySchema 处理历史列补齐、老表迁移、老数据搬运。
// 这些操作依赖基础表已存在，可能需要 ALTER TABLE 或数据转换。
func migrateClientDBLegacySchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if err := ensureDemandQuoteCurrentSchema(db); err != nil {
		return fmt.Errorf("demand quote schema: %w", err)
	}
	if err := ensureWalletFundFlowsSchema(db); err != nil {
		return fmt.Errorf("wallet_fund_flows: %w", err)
	}
	if err := ensureWorkspaceStorageSchema(db); err != nil {
		return fmt.Errorf("workspace storage: %w", err)
	}
	if err := ensureFileDownloadsSchema(db); err != nil {
		return fmt.Errorf("file_downloads: %w", err)
	}
	if err := ensureFinAccountingSchema(db); err != nil {
		return fmt.Errorf("fin accounting schema: %w", err)
	}
	if err := ensureLiveFollowsSchema(db); err != nil {
		return fmt.Errorf("live_follows: %w", err)
	}
	if err := migrateLegacyChainTables(db); err != nil {
		return fmt.Errorf("legacy chain tables: %w", err)
	}

	// 钱包 UTXO 相关迁移
	// 设计说明：wallet_utxo 的老库可能还没有 allocation_class/allocation_reason，
	// 依赖这两个列的索引必须放到专门的迁移函数里创建。
	if err := ensureWalletUTXOSchema(db); err != nil {
		return fmt.Errorf("wallet_utxo: %w", err)
	}
	if err := ensureWalletUTXOAssetsSchema(db); err != nil {
		return fmt.Errorf("wallet_utxo_assets: %w", err)
	}
	if err := ensureWalletLocalBroadcastSchema(db); err != nil {
		return fmt.Errorf("wallet_local_broadcast_txs: %w", err)
	}
	if err := ensureWalletBSV21CreateStatusSchema(db); err != nil {
		return fmt.Errorf("wallet_bsv21_create_status: %w", err)
	}
	if err := ensureWalletUTXOSyncStateSchema(db); err != nil {
		return fmt.Errorf("wallet_utxo_sync_state: %w", err)
	}

	return nil
}

// ensureFinAccountingSchema 只补财务表的新口径列，不改旧口径行为。
// 设计说明：
// - 新库由 CREATE TABLE 直接带上新列；
// - 老库靠这里补列，避免启动时因缺列失败；
// - 这一批只铺轨，不改任何写入和读取逻辑。
func ensureFinAccountingSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	migrations := []struct {
		table string
		col   string
		stmt  string
	}{
		{table: "fin_business", col: "source_type", stmt: `ALTER TABLE fin_business ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "fin_business", col: "source_id", stmt: `ALTER TABLE fin_business ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "fin_business", col: "accounting_scene", stmt: `ALTER TABLE fin_business ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "fin_business", col: "accounting_subtype", stmt: `ALTER TABLE fin_business ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
		{table: "fin_process_events", col: "source_type", stmt: `ALTER TABLE fin_process_events ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "fin_process_events", col: "source_id", stmt: `ALTER TABLE fin_process_events ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "fin_process_events", col: "accounting_scene", stmt: `ALTER TABLE fin_process_events ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "fin_process_events", col: "accounting_subtype", stmt: `ALTER TABLE fin_process_events ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
	}

	for _, m := range migrations {
		cols, err := tableColumns(db, m.table)
		if err != nil {
			return fmt.Errorf("inspect %s: %w", m.table, err)
		}
		if _, ok := cols[strings.ToLower(m.col)]; ok {
			continue
		}
		if _, err := db.Exec(m.stmt); err != nil {
			return fmt.Errorf("add %s.%s: %w", m.table, m.col, err)
		}
	}
	return nil
}

// normalizeClientDBData 处理历史脏数据口径纠偏，不是结构迁移。
func normalizeClientDBData(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 1. 公钥格式统一化为压缩公钥 hex
	if err := normalizeClientPubKeyColumns(db); err != nil {
		return fmt.Errorf("pubkey columns: %w", err)
	}

	// 2. 口径纠偏：cycle_pay 是过程事件，不应存在于财务主表
	if err := cleanupLegacyCyclePayFinanceRows(db); err != nil {
		return fmt.Errorf("cycle_pay cleanup: %w", err)
	}

	// 3. 老业务 UTXO 链接迁移
	if err := migrateLegacyBizUTXOLinks(db); err != nil {
		return fmt.Errorf("biz_utxo_links migration: %w", err)
	}

	return nil
}

// ==================== 以下是具体的迁移函数实现 ====================

func ensureDemandQuoteCurrentSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	hasQuotes, err := hasTable(db, "demand_quotes")
	if err != nil {
		return fmt.Errorf("check demand_quotes table: %w", err)
	}
	hasArbiters, err := hasTable(db, "demand_quote_arbiters")
	if err != nil {
		return fmt.Errorf("check demand_quote_arbiters table: %w", err)
	}
	if !hasQuotes || !hasArbiters {
		return nil
	}

	quotesNeedFK, err := demandQuoteTableMissingFK(db, "demand_quotes", "demand_id", "demands", "demand_id")
	if err != nil {
		return fmt.Errorf("inspect demand_quotes foreign keys: %w", err)
	}
	arbitersNeedFK, err := demandQuoteTableMissingFK(db, "demand_quote_arbiters", "quote_id", "demand_quotes", "id")
	if err != nil {
		return fmt.Errorf("inspect demand_quote_arbiters foreign keys: %w", err)
	}
	if !quotesNeedFK && !arbitersNeedFK {
		return nil
	}
	return rebuildDemandQuoteFKTables(db, quotesNeedFK, arbitersNeedFK)
}

func ensureDemandQuoteIndexes(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_demand_quotes_demand_seller ON demand_quotes(demand_id, seller_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_demand_quotes_demand_created ON demand_quotes(demand_id, created_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_demand_quote_arbiters_quote_arbiter ON demand_quote_arbiters(quote_id, arbiter_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_demand_quote_arbiters_arbiter ON demand_quote_arbiters(arbiter_pub_hex, quote_id)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func rebuildDemandQuoteFKTables(db *sql.DB, rebuildQuotes bool, rebuildArbiters bool) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := demandQuoteRejectOrphanRows(db); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := tx.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}

	if rebuildQuotes {
		if err := rebuildDemandQuotesTableTx(tx); err != nil {
			rollback()
			return err
		}
	}
	if rebuildArbiters {
		if err := rebuildDemandQuoteArbitersTableTx(tx); err != nil {
			rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	if err := ensureDemandQuoteIndexes(db); err != nil {
		return err
	}
	return nil
}

func demandQuoteRejectOrphanRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if has, err := hasTable(db, "demand_quotes"); err != nil {
		return err
	} else if has {
		var quoteID int64
		var demandID string
		err := db.QueryRow(`SELECT id,demand_id FROM demand_quotes
			WHERE NOT EXISTS(SELECT 1 FROM demands WHERE demands.demand_id = demand_quotes.demand_id)
			ORDER BY id ASC LIMIT 1`).Scan(&quoteID, &demandID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			return fmt.Errorf("demand_quotes contains orphan row id=%d demand_id=%s", quoteID, strings.TrimSpace(demandID))
		}
	}
	if has, err := hasTable(db, "demand_quote_arbiters"); err != nil {
		return err
	} else if has {
		var id int64
		var quoteID int64
		var arbiterPubHex string
		err := db.QueryRow(`SELECT id,quote_id,arbiter_pub_hex FROM demand_quote_arbiters
			WHERE NOT EXISTS(SELECT 1 FROM demand_quotes WHERE demand_quotes.id = demand_quote_arbiters.quote_id)
			ORDER BY id ASC LIMIT 1`).Scan(&id, &quoteID, &arbiterPubHex)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			return fmt.Errorf("demand_quote_arbiters contains orphan row id=%d quote_id=%d arbiter_pub_hex=%s", id, quoteID, strings.TrimSpace(arbiterPubHex))
		}
	}
	return nil
}

func rebuildDemandQuotesTableTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE demand_quotes RENAME TO demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE demand_quotes(
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
		FOREIGN KEY(demand_id) REFERENCES demands(demand_id) ON DELETE CASCADE,
		UNIQUE(demand_id, seller_pub_hex)
	)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO demand_quotes(
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) SELECT
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		FROM demand_quotes_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	return nil
}

func rebuildDemandQuoteArbitersTableTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE demand_quote_arbiters RENAME TO demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE demand_quote_arbiters(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		quote_id INTEGER NOT NULL,
		arbiter_pub_hex TEXT NOT NULL,
		FOREIGN KEY(quote_id) REFERENCES demand_quotes(id) ON DELETE CASCADE,
		UNIQUE(quote_id, arbiter_pub_hex)
	)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO demand_quote_arbiters(
			id,quote_id,arbiter_pub_hex
		) SELECT
			id,quote_id,arbiter_pub_hex
		FROM demand_quote_arbiters_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	return nil
}

func demandQuoteTableMissingFK(db *sql.DB, table string, fromColumn string, parentTable string, parentColumn string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", strings.TrimSpace(table)))
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

// ensureWalletFundFlowsSchema 处理 wallet_fund_flows 表的历史列迁移
func ensureWalletFundFlowsSchema(db *sql.DB) error {
	cols, err := tableColumns(db, "wallet_fund_flows")
	if err != nil {
		return err
	}
	if _, ok := cols["visit_id"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_fund_flows ADD COLUMN visit_id TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if _, ok := cols["visit_locator"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_fund_flows ADD COLUMN visit_locator TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_fund_flows_visit_id ON wallet_fund_flows(visit_id, id DESC)`); err != nil {
		return err
	}
	return nil
}

// ensureWorkspaceStorageSchema 迁移客户端本地库存的核心五张表。
// 设计说明：
// - 这里不做列级兼容补丁，直接按新模型重建；
// - 老表里的旧口径会一次性搬走，再删除；
// - 这样后续业务代码只面对唯一真相。
func ensureWorkspaceStorageSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if err := ensureWorkspaceStorageBaseTables(db); err != nil {
		return err
	}
	legacyWorkspaces, legacyFiles, legacySeeds, legacySupply, legacyPolicy, err := legacyWorkspaceStoragePresent(db)
	if err != nil {
		return err
	}
	if !legacyWorkspaces && !legacyFiles && !legacySeeds && !legacySupply && !legacyPolicy {
		return nil
	}
	return migrateWorkspaceStorageLegacy(db)
}

func ensureWorkspaceStorageBaseTables(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS workspaces(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS seeds(
			seed_hash TEXT PRIMARY KEY,
			chunk_count INTEGER NOT NULL,
			file_size INTEGER NOT NULL,
			seed_file_path TEXT NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS workspace_files(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path),
			FOREIGN KEY(workspace_path) REFERENCES workspaces(workspace_path) ON DELETE CASCADE,
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS seed_chunk_supply(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index),
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS seed_pricing_policy(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			FOREIGN KEY(seed_hash) REFERENCES seeds(seed_hash) ON DELETE CASCADE
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func legacyWorkspaceStoragePresent(db *sql.DB) (bool, bool, bool, bool, bool, error) {
	legacyWorkspaces := false
	legacyFiles := false
	legacySeeds := false
	legacySupply := false
	legacyPolicy := false

	if cols, err := tableColumns(db, "workspaces"); err != nil {
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
	if cols, err := tableColumns(db, "workspace_files"); err != nil {
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
	if cols, err := tableColumns(db, "seeds"); err != nil {
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

func migrateWorkspaceStorageLegacy(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	if _, err := tx.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS workspaces_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS workspace_files_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seeds_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_chunk_supply_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_pricing_policy_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE workspaces_new(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE workspace_files_new(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE seeds_new(
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
	if _, err := tx.Exec(`CREATE TABLE seed_chunk_supply_new(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE seed_pricing_policy_new(
			seed_hash TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			pricing_source TEXT NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`); err != nil {
		rollback()
		return err
	}
	if err := migrateWorkspaceRowsLegacy(tx); err != nil {
		rollback()
		return err
	}
	if err := migrateWorkspaceFileRowsLegacy(tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedRowsLegacy(tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedChunkSupplyLegacy(tx); err != nil {
		rollback()
		return err
	}
	if err := migrateSeedPricingPolicyLegacy(tx); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_pricing_policy`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_available_chunks`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS seed_price_state`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS static_file_prices`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE workspaces_new RENAME TO workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE workspace_files_new RENAME TO workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE seeds_new RENAME TO seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE seed_chunk_supply_new RENAME TO seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE seed_pricing_policy_new RENAME TO seed_pricing_policy`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func migrateWorkspaceRowsLegacy(tx *sql.Tx) error {
	cols, err := tableColumnsTx(tx, "workspaces")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok && !containsAny(cols, "id", "path", "updated_at_unix") {
		_, err = tx.Exec(`INSERT OR IGNORE INTO workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) SELECT workspace_path,enabled,max_bytes,created_at_unix FROM workspaces`)
		return err
	}
	rows, err := tx.Query(`SELECT path,max_bytes,enabled,created_at_unix FROM workspaces`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, abs, enabled, maxBytes, created); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateWorkspaceFileRowsLegacy(tx *sql.Tx) error {
	cols, err := tableColumnsTx(tx, "workspace_files")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok {
		if _, ok2 := cols["file_path"]; ok2 && !containsAny(cols, "path", "file_size", "mtime_unix", "updated_at_unix") {
			_, err = tx.Exec(`INSERT OR IGNORE INTO workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) SELECT workspace_path,file_path,lower(trim(seed_hash)),COALESCE(seed_locked,0) FROM workspace_files`)
			return err
		}
	}
	rows, err := tx.Query(`SELECT path,seed_hash,COALESCE(seed_locked,0) FROM workspace_files`)
	if err != nil {
		return err
	}
	defer rows.Close()
	workspaceRoots, err := legacyWorkspaceRoots(tx)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, resolved.WorkspacePath, resolved.FilePath, normalizeSeedHashHex(seedHash), locked); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateSeedRowsLegacy(tx *sql.Tx) error {
	cols, err := tableColumnsTx(tx, "seeds")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	rows, err := tx.Query(`SELECT seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint FROM seeds`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO seeds_new(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, chunkCount, fileSize, strings.TrimSpace(seedPath), sanitizeRecommendedFileName(recommendedName), sanitizeMIMEHint(mimeHint)); err != nil {
			return err
		}
	}
	return rows.Err()
}

// 迁移规则：
// - 旧库有 seed_available_chunks 时，只信旧表记录；
// - 某个 seed 在旧表里没有记录，就迁空；
// - 只有旧库没有 seed_available_chunks 时，才按 chunk_count 补全。
func migrateSeedChunkSupplyLegacy(tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM seed_chunk_supply_new`); err != nil {
		return err
	}
	haveLegacySupply := hasTableValue(tx, "seed_available_chunks")
	rows, err := tx.Query(`SELECT seed_hash,chunk_count FROM seeds_new`)
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
			supplyRows, err := tx.Query(`SELECT chunk_index FROM seed_available_chunks WHERE seed_hash=? ORDER BY chunk_index ASC`, seedHash)
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
				if _, err := tx.Exec(`INSERT OR REPLACE INTO seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, uint32(idx)); err != nil {
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
			if _, err := tx.Exec(`INSERT OR REPLACE INTO seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, idx); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func migrateSeedPricingPolicyLegacy(tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM seed_pricing_policy_new`); err != nil {
		return err
	}
	if !hasTableValue(tx, "seed_price_state") {
		return nil
	}
	now := time.Now().Unix()
	seedRows, err := tx.Query(`SELECT seed_hash FROM seeds_new`)
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
		if err := tx.QueryRow(`SELECT last_buy_unit_price_sat_per_64k,floor_unit_price_sat_per_64k,resale_discount_bps FROM seed_price_state WHERE seed_hash=?`, seedHash).Scan(&lastBuy, &floorInt, &resaleInt); err != nil {
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO seed_pricing_policy_new(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix) VALUES(?,?,?,?,?)`, seedHash, floor, resale, source, now); err != nil {
			return err
		}
	}
	return seedRows.Err()
}

func legacyWorkspaceRoots(tx *sql.Tx) ([]string, error) {
	rows, err := tx.Query(`SELECT workspace_path FROM workspaces`)
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
		rows, err = tx.Query(`SELECT path FROM workspaces`)
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

func tableColumnsTx(tx *sql.Tx, table string) (map[string]struct{}, error) {
	rows, err := tx.Query(fmt.Sprintf("PRAGMA table_info(%s)", strings.TrimSpace(table)))
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

func hasTableValue(tx *sql.Tx, table string) bool {
	var one int
	err := tx.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(table)).Scan(&one)
	return err == nil
}

// ensureFileDownloadsSchema 处理 file_downloads 表的历史列迁移
func ensureFileDownloadsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(file_downloads)`)
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
	_, err = db.Exec(`ALTER TABLE file_downloads ADD COLUMN status_json TEXT NOT NULL DEFAULT '{}'`)
	return err
}

// ensureLiveFollowsSchema 处理 live_follows 表的历史列迁移
func ensureLiveFollowsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(live_follows)`)
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
	_, err = db.Exec(`ALTER TABLE live_follows ADD COLUMN last_quote_seller_pubkey_hex TEXT NOT NULL DEFAULT ''`)
	return err
}

// migrateLegacyChainTables 处理链相关历史表迁移
func migrateLegacyChainTables(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	exists, err := hasTable(db, "chain_tip_snapshot")
	if err != nil {
		return err
	}
	if exists {
		if _, err := db.Exec(
			`INSERT OR REPLACE INTO chain_tip_state(id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms)
			 SELECT id,tip_height,updated_at_unix,last_error,last_updated_by,last_trigger,last_duration_ms
			 FROM chain_tip_snapshot`,
		); err != nil {
			return err
		}
		if _, err := db.Exec(`DROP TABLE IF EXISTS chain_tip_snapshot`); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_utxo_snapshot`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_utxo_items`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS wallet_chain_tx_raw`); err != nil {
		return err
	}
	return nil
}

// ensureWalletUTXOSchema 处理 wallet_utxo 表的历史列迁移和表结构重构
func ensureWalletUTXOSchema(db *sql.DB) error {
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

		if _, err = tx.Exec(`ALTER TABLE wallet_utxo RENAME TO wallet_utxo_legacy_v2`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE TABLE wallet_utxo(
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
		if _, err = tx.Exec(
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
		if _, err = tx.Exec(`DROP TABLE wallet_utxo_legacy_v2`); err != nil {
			return err
		}
		if _, err = tx.Exec(`DROP INDEX IF EXISTS idx_wallet_utxo_origin`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_utxo_key ON wallet_utxo(address, txid, vout)`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_state ON wallet_utxo(wallet_id, state, value_satoshi DESC, txid, vout)`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_alloc ON wallet_utxo(wallet_id, address, state, allocation_class, created_at_unix ASC, value_satoshi ASC, txid, vout)`); err != nil {
			return err
		}
		if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_txid ON wallet_utxo(txid, vout)`); err != nil {
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
		if _, err := db.Exec(`ALTER TABLE wallet_utxo ADD COLUMN allocation_class TEXT NOT NULL DEFAULT 'plain_bsv'`); err != nil {
			return err
		}
	}
	if _, ok := cols["allocation_reason"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_utxo ADD COLUMN allocation_reason TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}

	// 数据修复
	if _, err := db.Exec(`UPDATE wallet_utxo SET allocation_class=CASE WHEN value_satoshi=1 THEN 'unknown' ELSE 'plain_bsv' END WHERE trim(allocation_class)=''`); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE wallet_utxo SET allocation_reason='awaiting external token evidence' WHERE state='unspent' AND value_satoshi=1 AND allocation_class='unknown' AND trim(allocation_reason)=''`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_alloc ON wallet_utxo(wallet_id, address, state, allocation_class, created_at_unix ASC, value_satoshi ASC, txid, vout)`); err != nil {
		return err
	}
	return nil
}

// ensureWalletUTXOAssetsSchema 处理 wallet_utxo_assets 表的历史索引迁移
func ensureWalletUTXOAssetsSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS wallet_utxo_assets(
		utxo_id TEXT NOT NULL,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		asset_group TEXT NOT NULL,
		asset_standard TEXT NOT NULL,
		asset_key TEXT NOT NULL,
		asset_symbol TEXT NOT NULL,
		quantity_text TEXT NOT NULL,
		source_name TEXT NOT NULL,
		payload_json TEXT NOT NULL,
		updated_at_unix INTEGER NOT NULL,
		PRIMARY KEY(utxo_id, asset_group, asset_standard, asset_key)
	)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_assets_wallet ON wallet_utxo_assets(wallet_id, address, asset_group, asset_standard, asset_key, updated_at_unix DESC, utxo_id ASC)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_assets_utxo ON wallet_utxo_assets(utxo_id, updated_at_unix DESC)`); err != nil {
		return err
	}
	return nil
}

// ensureWalletLocalBroadcastSchema 处理 wallet_local_broadcast_txs 表的历史列迁移
func ensureWalletLocalBroadcastSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS wallet_local_broadcast_txs(
		txid TEXT PRIMARY KEY,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		tx_hex TEXT NOT NULL,
		created_at_unix INTEGER NOT NULL,
		updated_at_unix INTEGER NOT NULL,
		observed_at_unix INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_local_broadcast_wallet ON wallet_local_broadcast_txs(wallet_id, address, updated_at_unix DESC)`); err != nil {
		return err
	}

	cols, err := tableColumns(db, "wallet_local_broadcast_txs")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["observed_at_unix"]; !ok {
		if _, err := db.Exec(`ALTER TABLE wallet_local_broadcast_txs ADD COLUMN observed_at_unix INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	return nil
}

// ensureWalletBSV21CreateStatusSchema 处理 wallet_bsv21_create_status 表的历史列迁移
func ensureWalletBSV21CreateStatusSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS wallet_bsv21_create_status(
		token_id TEXT PRIMARY KEY,
		create_txid TEXT NOT NULL,
		wallet_id TEXT NOT NULL,
		address TEXT NOT NULL,
		token_standard TEXT NOT NULL,
		symbol TEXT NOT NULL,
		max_supply TEXT NOT NULL,
		decimals INTEGER NOT NULL,
		icon TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at_unix INTEGER NOT NULL,
		submitted_at_unix INTEGER NOT NULL,
		confirmed_at_unix INTEGER NOT NULL DEFAULT 0,
		last_check_at_unix INTEGER NOT NULL DEFAULT 0,
		next_auto_check_at_unix INTEGER NOT NULL DEFAULT 0,
		updated_at_unix INTEGER NOT NULL,
		last_check_error TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_bsv21_create_status_state ON wallet_bsv21_create_status(status, next_auto_check_at_unix ASC, updated_at_unix DESC)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_wallet_bsv21_create_status_wallet ON wallet_bsv21_create_status(wallet_id, address, updated_at_unix DESC)`); err != nil {
		return err
	}
	return nil
}

// ensureWalletUTXOSyncStateSchema 处理 wallet_utxo_sync_state 表的历史列迁移
func ensureWalletUTXOSyncStateSchema(db *sql.DB) error {
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
			if _, err := db.Exec(m.stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

// normalizeClientPubKeyColumns 把历史库里的旧格式公钥统一迁移为压缩公钥 hex（02/03）
func normalizeClientPubKeyColumns(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	targets := []struct {
		table      string
		column     string
		allowEmpty bool
	}{
		{table: "direct_deals", column: "buyer_pubkey_hex"},
		{table: "direct_deals", column: "seller_pubkey_hex"},
		{table: "direct_transfer_pools", column: "buyer_pubkey_hex"},
		{table: "direct_transfer_pools", column: "seller_pubkey_hex"},
		{table: "live_quotes", column: "seller_pubkey_hex"},
		{table: "live_follows", column: "last_quote_seller_pubkey_hex", allowEmpty: true},
		{table: "file_download_chunks", column: "seller_pubkey_hex", allowEmpty: true},
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

// cleanupLegacyCyclePayFinanceRows 清理不应存在于财务主表的 cycle_pay 过程事件
func cleanupLegacyCyclePayFinanceRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 设计说明：
	// - sqliteactor 把运行时压成单连接后，事务期间不能再回头走库级 Query；
	// - 这里先在事务外判断旧表是否存在，避免同一函数里"持有 tx 又重新借 db"把自己堵死。
	legacyExists, legacyErr := hasTable(db, "biz_utxo_links")
	if legacyErr != nil {
		return legacyErr
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.Exec(
		`DELETE FROM fin_tx_breakdown
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM fin_tx_utxo_links
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM fin_business_txs
		 WHERE business_id IN (
			 SELECT business_id FROM fin_business
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	// 兼容旧库：如果历史表仍存在，也一起清掉，避免误导后续迁移逻辑
	if legacyExists {
		if _, err = tx.Exec(
			`DELETE FROM biz_utxo_links
			 WHERE business_id IN (
				 SELECT business_id FROM fin_business
				 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
			 )`,
		); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM fin_business WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'`); err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

// migrateLegacyBizUTXOLinks 迁移旧版 biz_utxo_links 表数据到新版结构
func migrateLegacyBizUTXOLinks(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	exists, err := hasTable(db, "biz_utxo_links")
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	rows, err := db.Query(
		`SELECT l.business_id,l.txid,l.utxo_id,l.role,l.amount_satoshi,l.created_at_unix,l.note,l.payload_json,
		        COALESCE(b.scene_type,''),COALESCE(b.scene_subtype,'')
		   FROM biz_utxo_links l
		   LEFT JOIN fin_business b ON b.business_id=l.business_id
		   ORDER BY l.id ASC`,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var businessID string
		var txid string
		var utxoID string
		var role string
		var amount int64
		var createdAtUnix int64
		var note string
		var payload string
		var sceneType string
		var sceneSubtype string
		if err := rows.Scan(&businessID, &txid, &utxoID, &role, &amount, &createdAtUnix, &note, &payload, &sceneType, &sceneSubtype); err != nil {
			return err
		}
		txRole, ioSide, utxoRole := mapLegacyBizUTXORole(sceneType, sceneSubtype, role)
		if err := dbAppendFinBusinessTxIfAbsent(db, finBusinessTxEntry{
			BusinessID:    businessID,
			TxID:          txid,
			TxRole:        txRole,
			CreatedAtUnix: createdAtUnix,
			Note:          note,
			Payload:       rawJSONPayload(payload),
		}); err != nil {
			return err
		}
		if err := dbAppendFinTxUTXOLinkIfAbsent(db, finTxUTXOLinkEntry{
			BusinessID:    businessID,
			TxID:          txid,
			UTXOID:        utxoID,
			IOSide:        ioSide,
			UTXORole:      utxoRole,
			AmountSatoshi: amount,
			CreatedAtUnix: createdAtUnix,
			Note:          note,
			Payload:       rawJSONPayload(payload),
		}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TABLE IF EXISTS biz_utxo_links`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_biz_utxo_links_business`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP INDEX IF EXISTS idx_biz_utxo_links_utxo`); err != nil {
		return err
	}
	return nil
}

// mapLegacyBizUTXORole 映射旧版 biz_utxo_links 角色到新结构
func mapLegacyBizUTXORole(sceneType string, sceneSubtype string, legacyRole string) (string, string, string) {
	sceneType = strings.TrimSpace(strings.ToLower(sceneType))
	sceneSubtype = strings.TrimSpace(strings.ToLower(sceneSubtype))
	legacyRole = strings.TrimSpace(strings.ToLower(legacyRole))

	txRole := "business_tx"
	switch {
	case sceneType == "fee_pool" && sceneSubtype == "open":
		txRole = "open_base"
	case sceneType == "c2c_transfer" && sceneSubtype == "open":
		txRole = "open_base"
	case sceneType == "c2c_transfer" && sceneSubtype == "close":
		txRole = "close_final"
	}

	switch legacyRole {
	case "input":
		return txRole, "input", "wallet_input"
	case "lock":
		return txRole, "output", "pool_lock"
	case "change":
		return txRole, "output", "wallet_change"
	case "settle_input":
		return txRole, "input", "pool_input"
	case "settle_to_seller":
		return txRole, "output", "settle_to_seller"
	case "settle_to_buyer":
		return txRole, "output", "settle_to_buyer"
	default:
		if strings.HasPrefix(legacyRole, "settle_") {
			return txRole, "output", legacyRole
		}
		return txRole, "output", legacyRole
	}
}

// ==================== 辅助函数 ====================

// hasTable 检查表是否存在
func hasTable(db *sql.DB, name string) (bool, error) {
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

// rawJSONPayload 用于标记原始 JSON 字符串类型
type rawJSONPayload string
