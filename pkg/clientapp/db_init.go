package clientapp

import (
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
func ensureClientDBBaseSchema(db *sql.DB) error {
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
		// - 业务完成状态以 settle_business_settlements 为准
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
		// - 业务完成状态以 settle_business_settlements 为准，不以此表的 status 为准
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
		`CREATE TABLE IF NOT EXISTS fact_pool_sessions(
			pool_session_id TEXT PRIMARY KEY,
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
			updated_at_unix INTEGER NOT NULL
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
		`CREATE TABLE IF NOT EXISTS fact_chain_payments(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
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
			UNIQUE(txid)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_pool_session_events_session_kind_seq ON fact_pool_session_events(pool_session_id,allocation_kind,sequence_num) WHERE event_kind='` + PoolFactEventKindPoolEvent + `'`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_sessions_scheme_status ON fact_pool_sessions(pool_scheme,status,updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_sessions_counterparty ON fact_pool_sessions(counterparty_pubkey_hex,status)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_session_no ON fact_pool_session_events(pool_session_id,allocation_no DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_kind_seq ON fact_pool_session_events(pool_session_id,event_kind,sequence_num)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_txid ON fact_pool_session_events(txid)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_pool_session_events_created ON fact_pool_session_events(created_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_payments_occurred ON fact_chain_payments(occurred_at_unix DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_payments_subtype ON fact_chain_payments(payment_subtype, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_payments_status ON fact_chain_payments(status, occurred_at_unix DESC)`,

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
		// settle_businesses 语义说明（第七次迭代收口）：
		// - 一条 settle_businesses = 一条独立收费事实
		// - 失败重试不新建 business，只更新原记录
		// - 退款、冲正、撤销如果产生新的资金动作，必须新建新的 business
		`CREATE TABLE IF NOT EXISTS settle_businesses(
			business_id TEXT PRIMARY KEY,
			business_role TEXT NOT NULL DEFAULT '' CHECK(business_role IN ('', 'formal', 'process')),
			source_type TEXT NOT NULL DEFAULT '',
			source_id TEXT NOT NULL DEFAULT '',
			accounting_scene TEXT NOT NULL DEFAULT '',
			accounting_subtype TEXT NOT NULL DEFAULT '',
			from_party_id TEXT NOT NULL,
			to_party_id TEXT NOT NULL,
			status TEXT NOT NULL,
			occurred_at_unix INTEGER NOT NULL,
			idempotency_key TEXT NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL
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
		`CREATE TABLE IF NOT EXISTS settle_tx_breakdown(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			business_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			tx_role TEXT,
			gross_input_satoshi INTEGER NOT NULL,
			change_back_satoshi INTEGER NOT NULL,
			external_in_satoshi INTEGER NOT NULL,
			counterparty_out_satoshi INTEGER NOT NULL,
			miner_fee_satoshi INTEGER NOT NULL,
			net_out_satoshi INTEGER NOT NULL,
			net_in_satoshi INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL,
			note TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			UNIQUE(business_id, txid)
		)`,
		`CREATE TABLE IF NOT EXISTS settle_tx_utxo_links(
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
			FOREIGN KEY(business_id) REFERENCES settle_businesses(business_id) ON DELETE CASCADE,
			UNIQUE(business_id, trigger_type, trigger_id_value, trigger_role)
		)`,
		// 统一结算出口（第七次迭代新增）
		// 职责：表达一条 business 的统一结算出口
		// 设计约束：本阶段强制一条 business 只对应一条主 settlement
		`CREATE TABLE IF NOT EXISTS settle_business_settlements(
			settlement_id TEXT PRIMARY KEY,
			business_id TEXT NOT NULL,
			settlement_method TEXT NOT NULL,
			status TEXT NOT NULL,
			target_type TEXT NOT NULL,
			target_id TEXT NOT NULL,
			error_message TEXT NOT NULL DEFAULT '',
			created_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(business_id) REFERENCES settle_businesses(business_id) ON DELETE CASCADE,
			UNIQUE(business_id)
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
		// 统一结算出口索引（第三次迭代新增）
		`CREATE INDEX IF NOT EXISTS idx_settle_business_settlements_status ON settle_business_settlements(status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_business_settlements_method ON settle_business_settlements(settlement_method, status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_business_settlements_target ON settle_business_settlements(target_type, target_id)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv21_events_token_id ON fact_bsv21_events(token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv21_events_kind_time ON fact_bsv21_events(event_kind, event_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_wallet_utxo_sync_cursor_round_tip ON wallet_utxo_sync_cursor(round_tip_height DESC, updated_at_unix DESC)`,
		// 第六次迭代：finance 表索引移到 ensureFinAccountingIndexes 中创建
		// 避免老库迁移时列不存在导致错误
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_process ON settle_process_events(process_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_breakdown_business ON settle_tx_breakdown(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_breakdown_txid ON settle_tx_breakdown(txid, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_breakdown_business_txid ON settle_tx_breakdown(business_id, txid)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_utxo_links_business ON settle_tx_utxo_links(business_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_utxo_links_utxo ON settle_tx_utxo_links(utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_tx_utxo_links_txid ON settle_tx_utxo_links(txid, id DESC)`,
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

		// fact_chain_asset_flows: 资产事实主表（支持 IN/OUT 事实）
		// 设计说明：
		// - 记录链上资产流入流出的事实，作为余额计算的可信来源
		// - evidence_source 目前只认 'WOC'（BSV20/BSV21 证据来源）
		// - unknown 资产不入此表，保持口径纯净
		// - 找零不单独建逻辑，由后续 UTXO 流转再次入表
		`CREATE TABLE IF NOT EXISTS fact_chain_asset_flows(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			flow_id TEXT NOT NULL UNIQUE,
			wallet_id TEXT NOT NULL,
			address TEXT NOT NULL,
			direction TEXT NOT NULL CHECK(direction IN ('IN','OUT')),
			asset_kind TEXT NOT NULL CHECK(asset_kind IN ('BSV','BSV20','BSV21')),
			token_id TEXT NOT NULL DEFAULT '',
			utxo_id TEXT NOT NULL,
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL,
			amount_satoshi INTEGER NOT NULL,
			quantity_text TEXT NOT NULL DEFAULT '',
			occurred_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			evidence_source TEXT NOT NULL DEFAULT 'WOC',
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			UNIQUE(wallet_id, utxo_id, direction),
			FOREIGN KEY(utxo_id) REFERENCES wallet_utxo(utxo_id)
		)`,

		// ============================================================
		// 以下三张新表（fact_bsv_consumptions / fact_token_consumptions / fact_token_utxo_links）
		// 是 A 组硬切换迁移后的新写入目标。
		// 设计说明：
		// - 新表按资产类型拆分：BSV 本币、Token（BSV20/BSV21）
		// - Token 表增加 token_id/token_standard/used_quantity_text 字段
		// - 全部挂载 settlement_cycle_id，与 fact_settlement_cycles 形成结算周期关联
		// - 约束完整：外键、唯一键、CHECK 约束全部生效
		// ============================================================

		// fact_bsv_consumptions: 本币消耗事实表（A组硬切换新增）
		// 设计说明：
		// - 记录 BSV 本币的消耗事实
		// - 必须挂载到 settlement_cycle_id
		// - 不再直接绑定 chain_payment_id / pool_allocation_id
		`CREATE TABLE IF NOT EXISTS fact_bsv_consumptions(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			consumption_id TEXT NOT NULL DEFAULT '',
			source_flow_id INTEGER,
			source_utxo_id TEXT NOT NULL DEFAULT '',
			settlement_cycle_id INTEGER NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','confirmed','reverted')),
			used_satoshi INTEGER NOT NULL CHECK(used_satoshi>0),
			occurred_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(source_flow_id) REFERENCES fact_chain_asset_flows(id),
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id)
		)`,
		`CREATE TRIGGER IF NOT EXISTS trg_fact_bsv_consumptions_fill_id
			AFTER INSERT ON fact_bsv_consumptions
			FOR EACH ROW
			WHEN trim(NEW.consumption_id) = ''
			BEGIN
				UPDATE fact_bsv_consumptions
				SET consumption_id = 'bsvcons_' || NEW.id
				WHERE id = NEW.id;
			END`,
		// fact_bsv_consumptions 索引
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_bsv_consumptions_id ON fact_bsv_consumptions(consumption_id)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_consumptions_source ON fact_bsv_consumptions(source_flow_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_consumptions_source_utxo ON fact_bsv_consumptions(source_utxo_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_bsv_consumptions_cycle ON fact_bsv_consumptions(settlement_cycle_id, id DESC)`,

		// fact_token_consumptions: Token消耗事实表（A组硬切换新增）
		// 设计说明：
		// - 记录 Token（BSV20/BSV21）的消耗事实
		// - 增加 token_id/token_standard/used_quantity_text 字段
		// - used_quantity_text 为十进制字符串，记录 token 数量
		`CREATE TABLE IF NOT EXISTS fact_token_consumptions(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			consumption_id TEXT NOT NULL DEFAULT '',
			source_flow_id INTEGER,
			source_utxo_id TEXT NOT NULL DEFAULT '',
			token_id TEXT NOT NULL,
			token_standard TEXT NOT NULL CHECK(token_standard IN ('BSV20','BSV21')),
			settlement_cycle_id INTEGER NOT NULL,
			state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','confirmed','reverted')),
			used_quantity_text TEXT NOT NULL CHECK(length(trim(used_quantity_text))>0),
			occurred_at_unix INTEGER NOT NULL,
			confirmed_at_unix INTEGER,
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(source_flow_id) REFERENCES fact_chain_asset_flows(id),
			FOREIGN KEY(settlement_cycle_id) REFERENCES fact_settlement_cycles(id)
		)`,
		`CREATE TRIGGER IF NOT EXISTS trg_fact_token_consumptions_fill_id
			AFTER INSERT ON fact_token_consumptions
			FOR EACH ROW
			WHEN trim(NEW.consumption_id) = ''
			BEGIN
				UPDATE fact_token_consumptions
				SET consumption_id = 'tokcons_' || NEW.id
				WHERE id = NEW.id;
			END`,
		// fact_token_consumptions 索引
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_token_consumptions_id ON fact_token_consumptions(consumption_id)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_consumptions_source ON fact_token_consumptions(source_flow_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_consumptions_token ON fact_token_consumptions(token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_consumptions_cycle ON fact_token_consumptions(settlement_cycle_id, id DESC)`,

		// fact_token_utxo_links: Token数量与载体UTXO映射表（A组硬切换新增）
		// 设计说明：
		// - 记录 Token 数量与其载体 UTXO 的映射关系
		// - carrier_flow_id 指向 fact_chain_asset_flows.id（载体UTXO的流入事实）
		// - quantity_text 为十进制字符串，记录该 UTXO 承载的 token 数量
		`CREATE TABLE IF NOT EXISTS fact_token_utxo_links(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			link_id TEXT NOT NULL DEFAULT '',
			wallet_id TEXT NOT NULL,
			token_id TEXT NOT NULL,
			token_standard TEXT NOT NULL CHECK(token_standard IN ('BSV20','BSV21')),
			carrier_flow_id INTEGER NOT NULL,
			carrier_utxo_id TEXT NOT NULL,
			quantity_text TEXT NOT NULL CHECK(length(trim(quantity_text))>0),
			direction TEXT NOT NULL CHECK(direction IN ('IN','OUT')),
			txid TEXT NOT NULL,
			vout INTEGER NOT NULL CHECK(vout>=0),
			occurred_at_unix INTEGER NOT NULL,
			updated_at_unix INTEGER NOT NULL,
			evidence_source TEXT NOT NULL DEFAULT '',
			note TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			FOREIGN KEY(carrier_flow_id) REFERENCES fact_chain_asset_flows(id),
			UNIQUE(link_id)
		)`,
		`CREATE TRIGGER IF NOT EXISTS trg_fact_token_utxo_links_fill_id
			AFTER INSERT ON fact_token_utxo_links
			FOR EACH ROW
			WHEN trim(NEW.link_id) = ''
			BEGIN
				UPDATE fact_token_utxo_links
				SET link_id = 'toklink_' || NEW.id
				WHERE id = NEW.id;
			END`,
		// fact_token_utxo_links 索引
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_token_utxo_links_carrier ON fact_token_utxo_links(carrier_flow_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_token_utxo_links_token_utxo_dir ON fact_token_utxo_links(token_id, carrier_utxo_id, direction)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_utxo_links_wallet ON fact_token_utxo_links(wallet_id, token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_utxo_links_token ON fact_token_utxo_links(token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_token_utxo_links_occurred ON fact_token_utxo_links(occurred_at_unix DESC, id DESC)`,

		// fact_chain_asset_flows 索引
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_asset_flows_wallet_asset ON fact_chain_asset_flows(wallet_id, asset_kind, token_id, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_asset_flows_wallet_direction ON fact_chain_asset_flows(wallet_id, direction, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_asset_flows_txid_vout ON fact_chain_asset_flows(txid, vout, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_fact_chain_asset_flows_occurred ON fact_chain_asset_flows(occurred_at_unix DESC, id DESC)`,

		// Step 15: 统一结算锚点 — fact_settlement_cycles
		// 设计说明：
		// - 把 chain_payment / pool_session / wallet chain/token 统一到一个周期结算事实层
		// - 只保留 source_type/source_id 作为来源锚点
		// - source_type/source_id 做唯一约束，禁止再靠旧事件列兜底
		`CREATE TABLE IF NOT EXISTS fact_settlement_cycles(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			cycle_id TEXT NOT NULL UNIQUE,
			source_type TEXT NOT NULL CHECK(source_type IN ('chain_payment','pool_session','chain_bsv','chain_token')),
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
		if _, err := db.Exec(s); err != nil {
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
func ensureCommandJournalTriggerKey(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "proc_command_journal")
	if err != nil {
		return fmt.Errorf("inspect proc_command_journal: %w", err)
	}
	if _, ok := cols["trigger_key"]; !ok {
		if _, err := db.Exec(`ALTER TABLE proc_command_journal ADD COLUMN trigger_key TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add trigger_key column: %w", err)
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_proc_command_journal_trigger_key ON proc_command_journal(trigger_key, id DESC)`); err != nil {
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
func ensureGatewayEventAndCommandJournalConstraints(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	dups, err := commandJournalDuplicateCommandIDs(db)
	if err != nil {
		return fmt.Errorf("audit proc_command_journal duplicates: %w", err)
	}
	if len(dups) > 0 {
		return fmt.Errorf("proc_command_journal has duplicate command_id values: %s", strings.Join(dups, ", "))
	}
	if err := cleanupLegacyCommandJournalCommandIDRows(db); err != nil {
		return fmt.Errorf("cleanup proc_command_journal: %w", err)
	}
	if err := cleanupLegacyGatewayEventCommandIDRows(db); err != nil {
		return fmt.Errorf("cleanup proc_gateway_events: %w", err)
	}
	if err := ensureCommandJournalCommandIDUnique(db); err != nil {
		return fmt.Errorf("proc_command_journal unique: %w", err)
	}
	if err := ensureGatewayEventsCommandIDForeignKey(db); err != nil {
		return fmt.Errorf("proc_gateway_events fk: %w", err)
	}
	if err := ensureGatewayEventsIndexes(db); err != nil {
		return fmt.Errorf("proc_gateway_events index: %w", err)
	}
	return nil
}

// cleanupLegacyCommandJournalCommandIDRows 清掉历史上 proc_command_journal 里不该留下的 command_id 空值。
func cleanupLegacyCommandJournalCommandIDRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	_, err := db.Exec(`DELETE FROM proc_command_journal WHERE trim(coalesce(command_id, '')) = ''`)
	return err
}

// cleanupLegacyGatewayEventCommandIDRows 清掉 proc_gateway_events 里无法进入硬约束阶段的历史脏数据。
// 说明：
// - command_id 为空的行直接删除；
// - command_id 找不到父命令的行也直接删除；
// - 这一步是收紧约束前的真实清理，不做伪造回填。
func cleanupLegacyGatewayEventCommandIDRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "proc_gateway_events")
	if err != nil {
		return fmt.Errorf("inspect proc_gateway_events: %w", err)
	}
	if _, ok := cols["command_id"]; !ok {
		if _, err := db.Exec(`ALTER TABLE proc_gateway_events ADD COLUMN command_id TEXT`); err != nil {
			return err
		}
	}
	_, err = db.Exec(`
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
func commandJournalDuplicateCommandIDs(db *sql.DB) ([]string, error) {
	if db == nil {
		return nil, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(`
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
func ensureCommandJournalCommandIDUnique(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	dups, err := commandJournalDuplicateCommandIDs(db)
	if err != nil {
		return err
	}
	if len(dups) > 0 {
		return fmt.Errorf("proc_command_journal has duplicate command_id values: %s", strings.Join(dups, ", "))
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_proc_command_journal_command_id ON proc_command_journal(command_id)`); err != nil {
		return err
	}
	return ensureCommandJournalIndexes(db)
}

// ensureGatewayEventsCommandIDForeignKey 让 proc_gateway_events.command_id 变成 NOT NULL + 物理外键。
func ensureGatewayEventsCommandIDForeignKey(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	notNull, err := tableColumnNotNull(db, "proc_gateway_events", "command_id")
	if err != nil {
		return fmt.Errorf("inspect proc_gateway_events command_id not null: %w", err)
	}
	hasFK, err := tableHasForeignKey(db, "proc_gateway_events", "command_id", "proc_command_journal", "command_id")
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
	if _, err := tx.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS proc_gateway_events_fk_rebuild`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE proc_gateway_events RENAME TO proc_gateway_events_fk_rebuild`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE proc_gateway_events(
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
	if _, err := tx.Exec(`INSERT INTO proc_gateway_events(
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
	if _, err := tx.Exec(`DROP TABLE proc_gateway_events_fk_rebuild`); err != nil {
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
	return ensureGatewayEventsIndexes(db)
}

// ensureGatewayEventsIndexes 保证 proc_gateway_events 的查询索引都还在。
func ensureGatewayEventsIndexes(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_created_at ON proc_gateway_events(created_at_unix DESC)`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_proc_gateway_events_cmd_id ON proc_gateway_events(command_id)`); err != nil {
		return err
	}
	return nil
}

// ensureCommandJournalIndexes 保证 proc_command_journal 的查询索引都还在。
func ensureCommandJournalIndexes(db *sql.DB) error {
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
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS uq_proc_command_journal_command_id ON proc_command_journal(command_id)`); err != nil {
		return err
	}
	return nil
}

// tableHasForeignKey 检查表是否已经有指定外键。
func tableHasForeignKey(db *sql.DB, table, fromColumn, parentTable, parentColumn string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	rows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", strings.TrimSpace(table)))
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
func ensureGatewayEventsCommandID(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	return ensureGatewayEventAndCommandJournalConstraints(db)
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
func ensureCommandLinkedTableConstraints(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	for _, table := range []string{"proc_domain_events", "proc_state_snapshots", "proc_effect_logs"} {
		report, err := auditCommandLinkedTableRows(db, table)
		if err != nil {
			return fmt.Errorf("audit %s: %w", table, err)
		}
		if report.DirtyRows() > 0 {
			if err := cleanupLegacyCommandLinkedRows(db, table); err != nil {
				return fmt.Errorf("cleanup %s: %w", table, err)
			}
		}
		needsRebuild, err := commandLinkedTableNeedsRebuild(db, table)
		if err != nil {
			return fmt.Errorf("inspect %s constraints: %w", table, err)
		}
		if needsRebuild {
			if err := rebuildCommandLinkedTable(db, table); err != nil {
				return fmt.Errorf("rebuild %s: %w", table, err)
			}
		}
		if err := ensureCommandLinkedTableIndexes(db, table); err != nil {
			return fmt.Errorf("indexes %s: %w", table, err)
		}
	}
	return nil
}

// auditCommandLinkedTableRows 先把命令链表里的脏数据看清楚，再决定清理和重建。
func auditCommandLinkedTableRows(db *sql.DB, table string) (commandLinkedTableAuditReport, error) {
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
	row := db.QueryRow(fmt.Sprintf(`
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
func cleanupLegacyDomainEventCommandRows(db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(db, "proc_domain_events")
}

// cleanupLegacyStateSnapshotCommandRows 删除 proc_state_snapshots 里不该进入硬约束的旧行。
func cleanupLegacyStateSnapshotCommandRows(db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(db, "proc_state_snapshots")
}

// cleanupLegacyEffectLogCommandRows 删除 proc_effect_logs 里不该进入硬约束的旧行。
func cleanupLegacyEffectLogCommandRows(db *sql.DB) error {
	return cleanupLegacyCommandLinkedRows(db, "proc_effect_logs")
}

func cleanupLegacyCommandLinkedRows(db *sql.DB, table string) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !isCommandLinkedTable(strings.TrimSpace(table)) {
		return fmt.Errorf("unsupported table: %s", table)
	}
	_, err := db.Exec(fmt.Sprintf(`
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

func commandLinkedTableNeedsRebuild(db *sql.DB, table string) (bool, error) {
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
	hasFK, err := tableHasForeignKey(db, table, "command_id", "proc_command_journal", "command_id")
	if err != nil {
		return false, fmt.Errorf("inspect %s foreign key: %w", table, err)
	}
	hasCheck, err := tableHasCreateSQLContains(db, table, "CHECK(trim(command_id) <> '')")
	if err != nil {
		return false, fmt.Errorf("inspect %s check constraint: %w", table, err)
	}
	if !notNull || !hasFK || !hasCheck {
		return true, nil
	}
	return false, nil
}

func rebuildCommandLinkedTable(db *sql.DB, table string) error {
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
	if _, err := tx.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		rollback()
		return err
	}
	tempTable := table + "_command_fk_rebuild"
	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, table, tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(spec.CreateSQL); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(spec.InsertSQL(tempTable)); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf(`DROP TABLE %s`, tempTable)); err != nil {
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

func ensureCommandLinkedTableIndexes(db *sql.DB, table string) error {
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
			if _, err := db.Exec(stmt); err != nil {
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
			if _, err := db.Exec(stmt); err != nil {
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
			if _, err := db.Exec(stmt); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported table: %s", table)
	}
	return nil
}

func tableHasCreateSQLContains(db *sql.DB, table, snippet string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("db is nil")
	}
	var sqlText sql.NullString
	err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1`, strings.TrimSpace(table)).Scan(&sqlText)
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
func ensureBizPoolSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	stmts := bizPoolSchemaStmts()
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	indexStmts := []string{
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_status_updated ON biz_pool(status, updated_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_allocations_session_no ON biz_pool_allocations(pool_session_id, allocation_no DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_pool_allocations_session_kind_seq ON biz_pool_allocations(pool_session_id, allocation_kind, sequence_num)`,
	}
	for _, stmt := range indexStmts {
		if _, err := db.Exec(stmt); err != nil {
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
func ensureFinAccountingSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	migrations := []struct {
		table string
		col   string
		stmt  string
	}{
		{table: "settle_businesses", col: "business_role", stmt: `ALTER TABLE settle_businesses ADD COLUMN business_role TEXT NOT NULL DEFAULT ''`},
		{table: "settle_businesses", col: "source_type", stmt: `ALTER TABLE settle_businesses ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "settle_businesses", col: "source_id", stmt: `ALTER TABLE settle_businesses ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_businesses", col: "accounting_scene", stmt: `ALTER TABLE settle_businesses ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "settle_businesses", col: "accounting_subtype", stmt: `ALTER TABLE settle_businesses ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "source_type", stmt: `ALTER TABLE settle_process_events ADD COLUMN source_type TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "source_id", stmt: `ALTER TABLE settle_process_events ADD COLUMN source_id TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "accounting_scene", stmt: `ALTER TABLE settle_process_events ADD COLUMN accounting_scene TEXT NOT NULL DEFAULT ''`},
		{table: "settle_process_events", col: "accounting_subtype", stmt: `ALTER TABLE settle_process_events ADD COLUMN accounting_subtype TEXT NOT NULL DEFAULT ''`},
		{table: "settle_tx_breakdown", col: "tx_role", stmt: `ALTER TABLE settle_tx_breakdown ADD COLUMN tx_role TEXT`},
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

// ensureFinAccountingIndexes 只创建 finance 新口径查询所需的索引。
// 说明：要放在列补齐之后执行，老库迁移时不能提前碰还不存在的列。
func ensureFinAccountingIndexes(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	stmts := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_businesses_idempotency ON settle_businesses(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_businesses_role ON settle_businesses(business_role, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_businesses_source ON settle_businesses(source_type, source_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_businesses_accounting ON settle_businesses(accounting_scene, accounting_subtype, occurred_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_process_events_idempotency ON settle_process_events(idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_source ON settle_process_events(source_type, source_id, occurred_at_unix DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_settle_process_events_accounting ON settle_process_events(accounting_scene, accounting_subtype, occurred_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_settle_tx_breakdown_business_txid ON settle_tx_breakdown(business_id, txid)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// normalizeClientDBData 只做当前口径需要的轻量归一化，不做历史迁移。
func normalizeClientDBData(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 这里只保留当前口径必需的轻量约束，避免把旧库收口逻辑继续留在初始化里。
	if _, err := db.Exec(`
		CREATE TRIGGER IF NOT EXISTS chk_settle_businesses_role_insert
		BEFORE INSERT ON settle_businesses
		BEGIN
			SELECT CASE
				WHEN NEW.business_role NOT IN ('', 'formal', 'process')
				THEN RAISE(ABORT, 'business_role must be formal or process')
			END;
		END;
	`); err != nil {
		return fmt.Errorf("create business_role insert check trigger: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TRIGGER IF NOT EXISTS chk_settle_businesses_role_update
		BEFORE UPDATE ON settle_businesses
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
	if err := normalizeClientPubKeyColumns(db); err != nil {
		return fmt.Errorf("pubkey columns: %w", err)
	}

	return nil
}

// ==================== 以下是当前结构辅助函数实现 ====================

func ensureDemandQuoteCurrentSchema(db *sql.DB) error {
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

	quotesNeedFK, err := demandQuoteTableMissingFK(db, "biz_demand_quotes", "demand_id", "biz_demands", "demand_id")
	if err != nil {
		return fmt.Errorf("inspect biz_demand_quotes foreign keys: %w", err)
	}
	arbitersNeedFK, err := demandQuoteTableMissingFK(db, "biz_demand_quote_arbiters", "quote_id", "biz_demand_quotes", "id")
	if err != nil {
		return fmt.Errorf("inspect biz_demand_quote_arbiters foreign keys: %w", err)
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
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quotes_demand_seller ON biz_demand_quotes(demand_id, seller_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quotes_demand_created ON biz_demand_quotes(demand_id, created_at_unix DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_biz_demand_quote_arbiters_quote_arbiter ON biz_demand_quote_arbiters(quote_id, arbiter_pub_hex)`,
		`CREATE INDEX IF NOT EXISTS idx_biz_demand_quote_arbiters_arbiter ON biz_demand_quote_arbiters(arbiter_pub_hex, quote_id)`,
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
	if has, err := hasTable(db, "biz_demand_quotes"); err != nil {
		return err
	} else if has {
		var quoteID int64
		var demandID string
		err := db.QueryRow(`SELECT id,demand_id FROM biz_demand_quotes
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
		err := db.QueryRow(`SELECT id,quote_id,arbiter_pub_hex FROM biz_demand_quote_arbiters
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

func rebuildDemandQuotesTableTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_demand_quotes RENAME TO biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_demand_quotes(
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
	if _, err := tx.Exec(`INSERT INTO biz_demand_quotes(
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		) SELECT
			id,demand_id,seller_pub_hex,seed_price_satoshi,chunk_price_satoshi,chunk_count,file_size_bytes,recommended_file_name,mime_type,available_chunk_bitmap_hex,expires_at_unix,created_at_unix
		FROM biz_demand_quotes_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE biz_demand_quotes_fk_rebuild`); err != nil {
		return err
	}
	return nil
}

func rebuildDemandQuoteArbitersTableTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("tx is nil")
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_demand_quote_arbiters RENAME TO biz_demand_quote_arbiters_fk_rebuild`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_demand_quote_arbiters(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		quote_id INTEGER NOT NULL,
		arbiter_pub_hex TEXT NOT NULL,
		FOREIGN KEY(quote_id) REFERENCES biz_demand_quotes(id) ON DELETE CASCADE,
		UNIQUE(quote_id, arbiter_pub_hex)
	)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO biz_demand_quote_arbiters(
			id,quote_id,arbiter_pub_hex
		) SELECT
			id,quote_id,arbiter_pub_hex
		FROM biz_demand_quote_arbiters_fk_rebuild ORDER BY id ASC`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE biz_demand_quote_arbiters_fk_rebuild`); err != nil {
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

// ensureWorkspaceStorageSchema 迁移客户端本地库存的核心五张表。
// 设计说明：
// - 这里不做列级兼容补丁，直接按新模型重建；
// - 老表里的历史数据会一次性搬走，再删除；
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
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_workspaces_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_workspace_files_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seeds_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seed_chunk_supply_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seed_pricing_policy_new`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_workspaces_new(
			workspace_path TEXT PRIMARY KEY,
			enabled INTEGER NOT NULL,
			max_bytes INTEGER NOT NULL,
			created_at_unix INTEGER NOT NULL
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_workspace_files_new(
			workspace_path TEXT NOT NULL,
			file_path TEXT NOT NULL,
			seed_hash TEXT NOT NULL,
			seed_locked INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(workspace_path,file_path)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_seeds_new(
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
	if _, err := tx.Exec(`CREATE TABLE biz_seed_chunk_supply_new(
			seed_hash TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			PRIMARY KEY(seed_hash,chunk_index)
		)`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`CREATE TABLE biz_seed_pricing_policy_new(
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
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS biz_seed_pricing_policy`); err != nil {
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
	if _, err := tx.Exec(`ALTER TABLE biz_workspaces_new RENAME TO biz_workspaces`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_workspace_files_new RENAME TO biz_workspace_files`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_seeds_new RENAME TO biz_seeds`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_seed_chunk_supply_new RENAME TO biz_seed_chunk_supply`); err != nil {
		rollback()
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE biz_seed_pricing_policy_new RENAME TO biz_seed_pricing_policy`); err != nil {
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
	cols, err := tableColumnsTx(tx, "biz_workspaces")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok && !containsAny(cols, "id", "path", "updated_at_unix") {
		_, err = tx.Exec(`INSERT OR IGNORE INTO biz_workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) SELECT workspace_path,enabled,max_bytes,created_at_unix FROM biz_workspaces`)
		return err
	}
	rows, err := tx.Query(`SELECT path,max_bytes,enabled,created_at_unix FROM biz_workspaces`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_workspaces_new(workspace_path,enabled,max_bytes,created_at_unix) VALUES(?,?,?,?)`, abs, enabled, maxBytes, created); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateWorkspaceFileRowsLegacy(tx *sql.Tx) error {
	cols, err := tableColumnsTx(tx, "biz_workspace_files")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["workspace_path"]; ok {
		if _, ok2 := cols["file_path"]; ok2 && !containsAny(cols, "path", "file_size", "mtime_unix", "updated_at_unix") {
			_, err = tx.Exec(`INSERT OR IGNORE INTO biz_workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) SELECT workspace_path,file_path,lower(trim(seed_hash)),COALESCE(seed_locked,0) FROM biz_workspace_files`)
			return err
		}
	}
	rows, err := tx.Query(`SELECT path,seed_hash,COALESCE(seed_locked,0) FROM biz_workspace_files`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_workspace_files_new(workspace_path,file_path,seed_hash,seed_locked) VALUES(?,?,?,?)`, resolved.WorkspacePath, resolved.FilePath, normalizeSeedHashHex(seedHash), locked); err != nil {
			return err
		}
	}
	return rows.Err()
}

func migrateSeedRowsLegacy(tx *sql.Tx) error {
	cols, err := tableColumnsTx(tx, "biz_seeds")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	rows, err := tx.Query(`SELECT seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint FROM biz_seeds`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_seeds_new(seed_hash,chunk_count,file_size,seed_file_path,recommended_file_name,mime_hint) VALUES(?,?,?,?,?,?)`, seedHash, chunkCount, fileSize, strings.TrimSpace(seedPath), sanitizeRecommendedFileName(recommendedName), sanitizeMIMEHint(mimeHint)); err != nil {
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
	if _, err := tx.Exec(`DELETE FROM biz_seed_chunk_supply_new`); err != nil {
		return err
	}
	haveLegacySupply := hasTableValue(tx, "seed_available_chunks")
	rows, err := tx.Query(`SELECT seed_hash,chunk_count FROM biz_seeds_new`)
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
				if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, uint32(idx)); err != nil {
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
			if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_seed_chunk_supply_new(seed_hash,chunk_index) VALUES(?,?)`, seedHash, idx); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

func migrateSeedPricingPolicyLegacy(tx *sql.Tx) error {
	if _, err := tx.Exec(`DELETE FROM biz_seed_pricing_policy_new`); err != nil {
		return err
	}
	if !hasTableValue(tx, "seed_price_state") {
		return nil
	}
	now := time.Now().Unix()
	seedRows, err := tx.Query(`SELECT seed_hash FROM biz_seeds_new`)
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
		if _, err := tx.Exec(`INSERT OR REPLACE INTO biz_seed_pricing_policy_new(seed_hash,floor_unit_price_sat_per_64k,resale_discount_bps,pricing_source,updated_at_unix) VALUES(?,?,?,?,?)`, seedHash, floor, resale, source, now); err != nil {
			return err
		}
	}
	return seedRows.Err()
}

func legacyWorkspaceRoots(tx *sql.Tx) ([]string, error) {
	rows, err := tx.Query(`SELECT workspace_path FROM biz_workspaces`)
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
		rows, err = tx.Query(`SELECT path FROM biz_workspaces`)
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

// ensureFileDownloadsSchema 处理 proc_file_downloads 表的历史列迁移
func ensureFileDownloadsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(proc_file_downloads)`)
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
	_, err = db.Exec(`ALTER TABLE proc_file_downloads ADD COLUMN status_json TEXT NOT NULL DEFAULT '{}'`)
	return err
}

// ensureLiveFollowsSchema 处理 proc_live_follows 表的历史列迁移
func ensureLiveFollowsSchema(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(proc_live_follows)`)
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
	_, err = db.Exec(`ALTER TABLE proc_live_follows ADD COLUMN last_quote_seller_pubkey_hex TEXT NOT NULL DEFAULT ''`)
	return err
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

func migrateWalletLocalBroadcastFacts(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	legacyTableName := strings.Join([]string{"wallet", "local", "broadcast", "txs"}, "_")
	exists, err := hasTable(db, legacyTableName)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	cols, err := tableColumns(db, legacyTableName)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	observedCol := "observed_at_unix"
	if _, ok := cols["observed_at_unix"]; !ok {
		observedCol = "0"
	}

	type legacyWalletLocalBroadcastRow struct {
		TxID           string
		WalletID       string
		Address        string
		TxHex          string
		CreatedAtUnix  int64
		UpdatedAtUnix  int64
		ObservedAtUnix int64
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func() {
		_ = tx.Rollback()
	}
	rows, err := tx.Query(fmt.Sprintf(`SELECT txid,wallet_id,address,tx_hex,created_at_unix,updated_at_unix,%s FROM %s`, observedCol, legacyTableName))
	if err != nil {
		rollback()
		return err
	}
	defer rows.Close()

	legacyRows := make([]legacyWalletLocalBroadcastRow, 0, 32)
	for rows.Next() {
		var row legacyWalletLocalBroadcastRow
		if scanErr := rows.Scan(&row.TxID, &row.WalletID, &row.Address, &row.TxHex, &row.CreatedAtUnix, &row.UpdatedAtUnix, &row.ObservedAtUnix); scanErr != nil {
			rollback()
			return scanErr
		}
		legacyRows = append(legacyRows, row)
	}
	if err := rows.Err(); err != nil {
		rollback()
		return err
	}

	for _, row := range legacyRows {
		submittedAt := row.CreatedAtUnix
		if submittedAt <= 0 {
			submittedAt = row.UpdatedAtUnix
		}
		if submittedAt <= 0 {
			submittedAt = time.Now().Unix()
		}
		status := "submitted"
		if row.ObservedAtUnix > 0 {
			status = "observed"
		}
		payload := map[string]any{
			"tx_hex":    strings.ToLower(strings.TrimSpace(row.TxHex)),
			"wallet_id": strings.TrimSpace(row.WalletID),
			"address":   strings.TrimSpace(row.Address),
		}
		if _, err := dbUpsertChainPaymentDB(tx, chainPaymentEntry{
			TxID:                 row.TxID,
			PaymentSubType:       "wallet_local_broadcast",
			Status:               status,
			WalletInputSatoshi:   0,
			WalletOutputSatoshi:  0,
			NetAmountSatoshi:     0,
			BlockHeight:          0,
			OccurredAtUnix:       submittedAt,
			SubmittedAtUnix:      submittedAt,
			WalletObservedAtUnix: row.ObservedAtUnix,
			FromPartyID:          strings.TrimSpace(row.WalletID),
			ToPartyID:            "external:unknown",
			Payload:              payload,
		}); err != nil {
			rollback()
			return fmt.Errorf("migrate legacy wallet broadcast txid=%s: %w", strings.TrimSpace(row.TxID), err)
		}
	}

	if _, err := tx.Exec(`DROP TABLE ` + legacyTableName); err != nil {
		rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

func ensureFactChainPaymentTimingSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cols, err := tableColumns(db, "fact_chain_payments")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if _, ok := cols["submitted_at_unix"]; !ok {
		if _, err := db.Exec(`ALTER TABLE fact_chain_payments ADD COLUMN submitted_at_unix INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, ok := cols["wallet_observed_at_unix"]; !ok {
		if _, err := db.Exec(`ALTER TABLE fact_chain_payments ADD COLUMN wallet_observed_at_unix INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
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
		{table: "proc_direct_deals", column: "buyer_pubkey_hex"},
		{table: "proc_direct_deals", column: "seller_pubkey_hex"},
		{table: "proc_direct_transfer_pools", column: "buyer_pubkey_hex"},
		{table: "proc_direct_transfer_pools", column: "seller_pubkey_hex"},
		{table: "fact_pool_sessions", column: "counterparty_pubkey_hex", allowEmpty: true},
		{table: "fact_pool_sessions", column: "seller_pubkey_hex", allowEmpty: true},
		{table: "fact_pool_sessions", column: "arbiter_pubkey_hex", allowEmpty: true},
		{table: "fact_pool_sessions", column: "gateway_pubkey_hex", allowEmpty: true},
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

// cleanupLegacyCyclePayFinanceRows 清理不应存在于财务主表的 cycle_pay 过程事件
func cleanupLegacyCyclePayFinanceRows(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 第六次迭代：检查旧字段是否存在，不存在则跳过（全新数据库无旧数据需要清理）
	cols, err := tableColumns(db, "settle_businesses")
	if err != nil {
		return fmt.Errorf("inspect settle_businesses columns: %w", err)
	}
	if _, hasOldSceneType := cols["scene_type"]; !hasOldSceneType {
		// 全新数据库，无旧字段，无需清理
		return nil
	}

	// 设计说明：
	// - sqliteactor 把运行时压成单连接后，事务期间不能再回头走库级 Query；
	// - 这里先在事务外判断旧表是否存在，避免同一函数里"持有 tx 又重新借 db"把自己堵死。
	legacyExists, legacyErr := hasTable(db, "settle_biz_utxo_links")
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
		`DELETE FROM settle_tx_breakdown
		 WHERE business_id IN (
			 SELECT business_id FROM settle_businesses
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		`DELETE FROM settle_tx_utxo_links
		 WHERE business_id IN (
			 SELECT business_id FROM settle_businesses
			 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
		 )`,
	); err != nil {
		return err
	}
	// 兼容旧库：如果历史表仍存在，也一起清掉，避免误导后续迁移逻辑
	if legacyExists {
		if _, err = tx.Exec(
			`DELETE FROM settle_biz_utxo_links
			 WHERE business_id IN (
				 SELECT business_id FROM settle_businesses
				 WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'
			 )`,
		); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`DELETE FROM settle_businesses WHERE scene_type='fee_pool' AND scene_subtype='cycle_pay'`); err != nil {
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

func rejectLegacyClientDBSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	legacyTables := []string{
		strings.Join([]string{"fact", "asset", "consumptions"}, "_"),
		strings.Join([]string{"fact", "pool", "allocations"}, "_"),
		strings.Join([]string{"fact", "tx", "history"}, "_"),
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

// isFinTxBreakdownFinalized 判断第二轮收口是否已经完成。
// 说明：只要仍然能看到旧表、可空 tx_role、缺少唯一约束或缺少关键索引，就不能跳过。
func isFinTxBreakdownFinalized(db *sql.DB) (bool, error) {
	hasBreakdown, err := hasTable(db, "settle_tx_breakdown")
	if err != nil {
		return false, err
	}
	if !hasBreakdown {
		return false, nil
	}

	hasOldTable, err := hasTable(db, "settle_business_txs")
	if err != nil {
		return false, err
	}
	if hasOldTable {
		return false, nil
	}

	cols, err := tableColumns(db, "settle_tx_breakdown")
	if err != nil {
		return false, err
	}
	if _, ok := cols["tx_role"]; !ok {
		return false, nil
	}

	hasNotNull, err := tableColumnNotNull(db, "settle_tx_breakdown", "tx_role")
	if err != nil {
		return false, err
	}
	if !hasNotNull {
		return false, nil
	}

	hasUnique, err := tableHasUniqueIndexOnColumns(db, "settle_tx_breakdown", []string{"business_id", "txid"})
	if err != nil {
		return false, err
	}
	if !hasUnique {
		return false, nil
	}

	for _, indexName := range []string{
		"idx_settle_tx_breakdown_business",
		"idx_settle_tx_breakdown_txid",
		"idx_settle_tx_breakdown_business_txid",
	} {
		hasIndex, err := tableHasIndex(db, "settle_tx_breakdown", indexName)
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
	case strings.HasPrefix(bid, "biz_wallet_chain_"):
		var accountingSubtype string
		if err := db.QueryRow(`SELECT accounting_subtype FROM settle_businesses WHERE business_id=?`, bid).Scan(&accountingSubtype); err != nil {
			return "", fmt.Errorf("cannot infer tx_role for (%s,%s): business record not found", bid, txid)
		}
		switch accountingSubtype {
		case "send":
			return "send", nil
		case "receive":
			return "receive", nil
		default:
			return "wallet_chain", nil
		}
	default:
		return "", fmt.Errorf("cannot infer tx_role for (%s,%s): unknown business type", bid, txid)
	}
}

// rawJSONPayload 用于标记原始 JSON 字符串类型
type rawJSONPayload string
