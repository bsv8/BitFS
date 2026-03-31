package clientapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
		`CREATE TABLE IF NOT EXISTS workspace_files(path TEXT PRIMARY KEY, file_size INTEGER, mtime_unix INTEGER, seed_hash TEXT NOT NULL, seed_locked INTEGER NOT NULL DEFAULT 0, updated_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS workspaces(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			max_bytes INTEGER NOT NULL,
			enabled INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS seeds(seed_hash TEXT PRIMARY KEY, seed_file_path TEXT NOT NULL, chunk_count INTEGER, file_size INTEGER, recommended_file_name TEXT NOT NULL DEFAULT '', mime_hint TEXT NOT NULL DEFAULT '', created_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS seed_available_chunks(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`,
		`CREATE TABLE IF NOT EXISTS seed_price_state(seed_hash TEXT PRIMARY KEY, last_buy_unit_price_sat_per_64k INTEGER, floor_unit_price_sat_per_64k INTEGER, resale_discount_bps INTEGER, unit_price_sat_per_64k INTEGER, updated_at_unix INTEGER)`,
		`CREATE TABLE IF NOT EXISTS demand_dedup(demand_id TEXT PRIMARY KEY, seed_hash TEXT, created_at_unix INTEGER)`,

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
		`CREATE TABLE IF NOT EXISTS sale_records(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			created_at_unix INTEGER NOT NULL,
			session_id TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			unit_price_sat_per_64k INTEGER NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			buyer_gateway_pubkey_hex TEXT NOT NULL,
			release_token TEXT NOT NULL
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
		`CREATE TABLE IF NOT EXISTS direct_quotes(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			demand_id TEXT NOT NULL,
			seller_pubkey_hex TEXT NOT NULL,
			seed_price INTEGER NOT NULL,
			chunk_price INTEGER NOT NULL,
			chunk_count INTEGER NOT NULL DEFAULT 0,
			file_size INTEGER NOT NULL DEFAULT 0,
			expires_at_unix INTEGER NOT NULL,
			recommended_file_name TEXT NOT NULL DEFAULT '',
			mime_hint TEXT NOT NULL DEFAULT '',
			available_chunk_bitmap_hex TEXT NOT NULL DEFAULT '',
			seller_arbiter_pubkey_hexes_json TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			UNIQUE(demand_id, seller_pubkey_hex)
		)`,
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
		`CREATE TABLE IF NOT EXISTS direct_sessions(
			session_id TEXT PRIMARY KEY,
			deal_id TEXT NOT NULL,
			chunk_price INTEGER NOT NULL,
			paid_chunks INTEGER NOT NULL,
			paid_amount INTEGER NOT NULL,
			released_chunks INTEGER NOT NULL,
			released_amount INTEGER NOT NULL,
			status TEXT NOT NULL,
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
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

		// 静态定价
		`CREATE TABLE IF NOT EXISTS static_file_prices(
			path TEXT PRIMARY KEY,
			floor_unit_price_sat_per_64k INTEGER NOT NULL,
			resale_discount_bps INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL
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
		`CREATE INDEX IF NOT EXISTS idx_workspace_seed_hash ON workspace_files(seed_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_seed_available_chunks_seed ON seed_available_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_workspaces_path ON workspaces(path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_downloads_updated ON file_downloads(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_file_download_chunks_seed ON file_download_chunks(seed_hash,chunk_index)`,
		`CREATE INDEX IF NOT EXISTS idx_live_quotes_demand ON live_quotes(demand_id, created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_node_reachability_cache_expires ON node_reachability_cache(expires_at_unix DESC, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_tx_history_created_at ON tx_history(created_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_sale_records_created_at ON sale_records(created_at_unix DESC)`,
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
		`CREATE INDEX IF NOT EXISTS idx_static_file_prices_updated ON static_file_prices(updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_inbox_messages_received_at ON inbox_messages(received_at_unix DESC,id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_published_route_indexes_updated ON published_route_indexes(updated_at_unix DESC,route ASC)`,
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

	// 各个模块的历史迁移
	if err := ensureDirectQuotesSchema(db); err != nil {
		return fmt.Errorf("direct_quotes: %w", err)
	}
	if err := ensureWalletFundFlowsSchema(db); err != nil {
		return fmt.Errorf("wallet_fund_flows: %w", err)
	}
	if err := ensureSeedsSchema(db); err != nil {
		return fmt.Errorf("seeds: %w", err)
	}
	if err := ensureWorkspaceFilesSchema(db); err != nil {
		return fmt.Errorf("workspace_files: %w", err)
	}
	if err := ensureFileDownloadsSchema(db); err != nil {
		return fmt.Errorf("file_downloads: %w", err)
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

// ensureDirectQuotesSchema 处理 direct_quotes 表的历史列迁移
func ensureDirectQuotesSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(direct_quotes)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasRecommendedFileName := false
	hasMIMEHint := false
	hasAvailableChunkBitmapHex := false
	hasAvailableChunksJSON := false
	hasChunkCount := false
	hasFileSize := false

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
		if strings.EqualFold(strings.TrimSpace(name), "recommended_file_name") {
			hasRecommendedFileName = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "mime_hint") {
			hasMIMEHint = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "available_chunk_bitmap_hex") {
			hasAvailableChunkBitmapHex = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "available_chunk_indexes_json") {
			hasAvailableChunksJSON = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "chunk_count") {
			hasChunkCount = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "file_size") {
			hasFileSize = true
		}
	}

	if !hasChunkCount {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN chunk_count INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !hasFileSize {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if !hasRecommendedFileName {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN recommended_file_name TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasMIMEHint {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN mime_hint TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasAvailableChunkBitmapHex {
		if _, err := db.Exec(`ALTER TABLE direct_quotes ADD COLUMN available_chunk_bitmap_hex TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
		hasAvailableChunkBitmapHex = true
	}

	// 迁移旧格式的 available_chunk_indexes_json 到新格式
	if hasAvailableChunkBitmapHex && hasAvailableChunksJSON {
		rows, err := db.Query(`SELECT id,available_chunk_indexes_json FROM direct_quotes`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var rawJSON string
			if err := rows.Scan(&id, &rawJSON); err != nil {
				return err
			}
			if strings.TrimSpace(rawJSON) == "" {
				continue
			}
			var indexes []uint32
			if err := json.Unmarshal([]byte(rawJSON), &indexes); err != nil {
				continue
			}
			bitmap := chunkBitmapHexFromIndexes(indexes, 0)
			if _, err := db.Exec(`UPDATE direct_quotes SET available_chunk_bitmap_hex=? WHERE id=?`, bitmap, id); err != nil {
				return err
			}
		}
	}
	return nil
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

// ensureSeedsSchema 处理 seeds 表的历史列迁移
func ensureSeedsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(seeds)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasRecommendedFileName := false
	hasMIMEHint := false

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
		if strings.EqualFold(strings.TrimSpace(name), "recommended_file_name") {
			hasRecommendedFileName = true
		}
		if strings.EqualFold(strings.TrimSpace(name), "mime_hint") {
			hasMIMEHint = true
		}
	}

	if !hasRecommendedFileName {
		if _, err := db.Exec(`ALTER TABLE seeds ADD COLUMN recommended_file_name TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	if !hasMIMEHint {
		if _, err := db.Exec(`ALTER TABLE seeds ADD COLUMN mime_hint TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	return nil
}

// ensureWorkspaceFilesSchema 处理 workspace_files 表的历史列迁移
func ensureWorkspaceFilesSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(workspace_files)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasSeedLocked := false
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
		if strings.EqualFold(strings.TrimSpace(name), "seed_locked") {
			hasSeedLocked = true
			break
		}
	}

	if hasSeedLocked {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE workspace_files ADD COLUMN seed_locked INTEGER NOT NULL DEFAULT 0`)
	return err
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
		{table: "direct_quotes", column: "seller_pubkey_hex"},
		{table: "direct_deals", column: "buyer_pubkey_hex"},
		{table: "direct_deals", column: "seller_pubkey_hex"},
		{table: "direct_transfer_pools", column: "buyer_pubkey_hex"},
		{table: "direct_transfer_pools", column: "seller_pubkey_hex"},
		{table: "live_quotes", column: "seller_pubkey_hex"},
		{table: "live_follows", column: "last_quote_seller_pubkey_hex", allowEmpty: true},
		{table: "file_download_chunks", column: "seller_pubkey_hex", allowEmpty: true},
	}

	for _, t := range targets {
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
