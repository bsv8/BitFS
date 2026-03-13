/**
 * 新的认证状态模型（基于私钥解锁）
 * - no_key: 系统中没有私钥，需要先创建或导入
 * - locked: 有私钥但未解锁，需要输入密码解锁
 * - unlocked: 已解锁，可以正常使用所有 API
 * - error: 发生错误
 * - checking: 正在检查状态
 */
export type AuthState = "no_key" | "locked" | "unlocked" | "error" | "checking";

// ========== 钱包相关类型 ==========
export type TxResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_pubkey_hex: string;
    event_type: string;
    direction: string;
    amount_satoshi: number;
    purpose: string;
    note: string;
  }>;
};

export type WalletLedgerResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    txid: string;
    direction: string;
    category: string;
    amount_satoshi: number;
    counterparty_label: string;
    status: string;
    block_height: number;
    occurred_at_unix: number;
    raw_ref_id: string;
    note: string;
    payload: unknown;
  }>;
};

export type GatewayEventsResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_pubkey_hex: string;
    action: string;
    msg_id?: string;
    sequence_num?: number;
    pool_id?: string;
    amount_satoshi: number;
    payload: unknown;
  }>;
};

// ========== 文件相关类型 ==========
export type SeedItem = {
  seed_hash: string;
  seed_file_path: string;
  chunk_count: number;
  file_size: number;
  unit_price_sat_per_64k: number;
  floor_unit_price_sat_per_64k: number;
  resale_discount_bps: number;
};

export type SeedsResp = {
  total: number;
  items: SeedItem[];
};

export type SalesResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    session_id: string;
    seed_hash: string;
    chunk_index: number;
    unit_price_sat_per_64k: number;
    amount_satoshi: number;
    buyer_gateway_pubkey_hex: string;
  }>;
};

export type FilesResp = {
  total: number;
  items: Array<{
    path: string;
    file_size: number;
    mtime_unix: number;
    seed_hash: string;
    updated_at_unix: number;
  }>;
};

// ========== 网关相关类型 ==========
export type Gateway = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
  transport_peer_id?: string;
  connected?: boolean;
  connectedness?: string;
  in_healthy_gws?: boolean;
  fee_pool_ready?: boolean;
  is_master?: boolean;
  last_error?: string;
  last_connected_at_unix?: number;
  last_runtime_error?: string;
  last_runtime_error_stage?: string;
  last_runtime_error_at_unix?: number;
};

export type GatewaysResp = {
  items: Gateway[];
  total: number;
};

export type Arbiter = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
};

export type ArbitersResp = {
  items: Gateway[];
  total: number;
};

// ========== Direct 交易相关类型 ==========
export type DirectQuote = {
  id: string;
  seed_hash: string;
  chunk_count: number;
  created_at_unix: number;
  expires_at_unix: number;
  quote_satoshi: number;
  seller_pubkey_hex: string;
  seller_addr: string;
  status: string;
};

export type DirectQuotesResp = {
  total: number;
  items: DirectQuote[];
};

export type DirectDeal = {
  id: string;
  quote_id: string;
  seed_hash: string;
  chunk_count: number;
  created_at_unix: number;
  amount_satoshi: number;
  buyer_pubkey_hex: string;
  seller_pubkey_hex: string;
  status: string;
};

export type DirectDealsResp = {
  total: number;
  items: DirectDeal[];
};

export type DirectSession = {
  id: string;
  deal_id: string;
  created_at_unix: number;
  ended_at_unix?: number;
  buyer_pubkey_hex: string;
  seller_pubkey_hex: string;
  seed_hash: string;
  chunk_index: number;
  status: string;
  bytes_transferred: number;
};

export type DirectSessionsResp = {
  total: number;
  items: DirectSession[];
};

export type DirectTransferPool = {
  pool_id: string;
  buyer_pubkey_hex: string;
  seller_pubkey_hex: string;
  created_at_unix: number;
  total_satoshi: number;
  released_satoshi: number;
  status: string;
};

export type DirectTransferPoolsResp = {
  total: number;
  items: DirectTransferPool[];
};

// ========== Live 流相关类型 ==========
export type LiveStream = {
  stream_id: string;
  file_count: number;
  total_bytes: number;
  last_updated_unix: number;
  workspace_roots?: string[];
  live_stream_folder: string;
};

export type LiveStreamsResp = {
  total: number;
  items: LiveStream[];
};

export type LiveStreamDetail = {
  stream_id: string;
  file_count: number;
  total_bytes: number;
  last_updated: number;
  live_root_path: string;
  segment_entries: Array<{
    path: string;
    file_size: number;
    mtime_unix: number;
    updated_at_unix: number;
  }>;
};

export type LiveFollowStatus = {
  following: boolean;
  stream_id?: string;
  started_at_unix?: number;
  current_segment?: string;
  bytes_received?: number;
};

// ========== Admin 管理相关类型 ==========
export type AdminConfig = {
  [key: string]: unknown;
};

export type AdminConfigSchema = {
  total: number;
  items: Array<{
    key: string;
    type: string;
    min_int?: number;
    max_int?: number;
    min_float?: number;
    max_float?: number;
    min_len?: number;
    max_len?: number;
    description?: string;
  }>;
};

// ========== Orchestrator 调度相关类型 ==========
export type OrchestratorEvent = {
  event_id: string;
  started_at_unix: number;
  ended_at_unix: number;
  steps_count: number;
  latest_log_id: number;
  idempotency_key: string;
  aggregate_key: string;
  command_type: string;
  gateway_pubkey_hex: string;
  source: string;
  signal_type: string;
  latest_event_type: string;
  latest_task_status: string;
  latest_retry_count: number;
  latest_queue_length: number;
  last_error_message: string;
};

export type OrchestratorEventsResp = {
  total: number;
  items: OrchestratorEvent[];
};

export type OrchestratorEventStep = {
  id: number;
  created_at_unix: number;
  event_type: string;
  source: string;
  signal_type: string;
  aggregate_key: string;
  idempotency_key: string;
  command_type: string;
  gateway_pubkey_hex: string;
  task_status: string;
  retry_count: number;
  queue_length: number;
  error_message: string;
  payload: unknown;
};

export type OrchestratorEventDetail = {
  event_id: string;
  idempotency_key: string;
  aggregate_key: string;
  command_type: string;
  gateway_pubkey_hex: string;
  started_at_unix: number;
  ended_at_unix: number;
  steps_count: number;
  latest_event_type: string;
  latest_task_status: string;
  last_error_message: string;
  steps: OrchestratorEventStep[];
};

export type OrchestratorStatus = {
  enabled: boolean;
  status: string;
  active_signals?: number;
  pending_tasks?: number;
  [key: string]: unknown;
};

// ========== ClientKernel 命令相关类型 ==========
export type ClientKernelCommand = {
  id: number;
  created_at_unix: number;
  command_id: string;
  command_type: string;
  gateway_pubkey_hex: string;
  aggregate_id: string;
  requested_by: string;
  requested_at_unix: number;
  accepted: boolean;
  status: string;
  error_code: string;
  error_message: string;
  state_before: string;
  state_after: string;
  duration_ms: number;
  payload: unknown;
  result: unknown;
};

export type ClientKernelCommandsResp = {
  total: number;
  items: ClientKernelCommand[];
};

// ========== FeePool 审计相关类型 ==========
export type FeePoolCommand = {
  id: number;
  created_at_unix: number;
  command_id: string;
  command_type: string;
  gateway_pubkey_hex: string;
  aggregate_id: string;
  requested_by: string;
  requested_at_unix: number;
  accepted: boolean;
  status: string;
  error_code: string;
  error_message: string;
  state_before: string;
  state_after: string;
  duration_ms: number;
  payload: unknown;
  result: unknown;
};

export type FeePoolCommandsResp = {
  total: number;
  items: FeePoolCommand[];
};

export type FeePoolEvent = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_pubkey_hex: string;
  event_name: string;
  state_before: string;
  state_after: string;
  payload: unknown;
};

export type FeePoolEventsResp = {
  total: number;
  items: FeePoolEvent[];
};

export type FeePoolState = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_pubkey_hex: string;
  state: string;
  pause_reason: string;
  pause_need_satoshi: number;
  pause_have_satoshi: number;
  last_error: string;
  payload: unknown;
};

export type FeePoolStatesResp = {
  total: number;
  items: FeePoolState[];
};

export type FeePoolEffect = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_pubkey_hex: string;
  effect_type: string;
  stage: string;
  status: string;
  error_message: string;
  payload: unknown;
};

export type FeePoolEffectsResp = {
  total: number;
  items: FeePoolEffect[];
};

// ========== 工作区管理相关类型 ==========
export type Workspace = {
  id: number;
  created_at_unix: number;
  updated_at_unix: number;
  path: string;
  max_bytes: number;
  used_bytes: number;
  enabled: boolean;
};

export type WorkspacesResp = {
  total: number;
  items: Workspace[];
};

export type AdminLiveStorageSummary = {
  stream_count: number;
  file_count: number;
  total_bytes: number;
};

/**
 * 密钥状态响应
 * GET /api/v1/key/status
 */
export type KeyStatusResp = {
  appname: string;
  has_key: boolean;
  unlocked: boolean;
};

/**
 * 创建私钥请求
 * POST /api/v1/key/new
 */
export type KeyNewReq = {
  password: string;
};

/**
 * 导入私钥请求
 * POST /api/v1/key/import
 */
export type KeyImportReq = {
  cipher: Record<string, unknown>;
};

/**
 * 导出私钥响应
 * GET /api/v1/key/export
 */
export type KeyExportResp = {
  cipher: Record<string, unknown>;
};

/**
 * 解锁请求
 * POST /api/v1/key/unlock
 */
export type KeyUnlockReq = {
  password: string;
};

// ========== 静态文件管理类型 ==========
export type StaticItem = {
  name: string;
  path: string;
  type: "dir" | "file";
  size: number;
  mtime_unix: number;
  seed_hash?: string;
  floor_unit_price_sat_per_64k?: number;
  resale_discount_bps?: number;
  price_updated_at_unix?: number;
};

export type StaticTreeResp = {
  root_path: string;
  current_path: string;
  parent_path: string;
  total: number;
  items: StaticItem[];
};

// ========== 文件获取任务类型 ==========
export type FileGetJob = {
  id: string;
  seed_hash: string;
  chunk_count: number;
  gateway_pubkey_hex: string;
  status: string;
  started_at_unix: number;
  ended_at_unix?: number;
  output_file_path?: string;
  error?: string;
  steps: Array<{
    index: number;
    name: string;
    status: string;
    started_at_unix: number;
    ended_at_unix?: number;
    detail?: Record<string, string>;
  }>;
};

export type SeedPriceDraft = {
  floor: string;
  discount: string;
  saving?: boolean;
  message?: string;
};

// 直播管理类型（仅保留后端 API 实际返回的字段）
export type LiveStorageSummary = {
  total_streams: number;
  total_segments: number;
  total_bytes: number;
  oldest_segment_at_unix: number;
  newest_segment_at_unix: number;
};

// 配置管理类型
export type ConfigItem = {
  key: string;
  value: unknown;
};

export type ConfigSchemaItem = {
  key: string;
  type: string;
  min_int?: number;
  max_int?: number;
  min_float?: number;
  max_float?: number;
  min_len?: number;
  max_len?: number;
  description?: string;
};

export type ConfigSchemaResp = {
  items: ConfigSchemaItem[];
  total: number;
};

export type WalletSummaryResp = {
  flow_count: number;
  tx_event_count: number;
  sale_count: number;
  gateway_event_count: number;
  total_in_satoshi: number;
  total_out_satoshi: number;
  total_used_satoshi: number;
  total_returned_satoshi: number;
  net_spent_satoshi: number;
  net_amount_delta_satoshi: number;
  ledger_count: number;
  ledger_total_in_satoshi: number;
  ledger_total_out_satoshi: number;
  ledger_net_satoshi: number;
  wallet_address: string;
  onchain_balance_satoshi: number;
  balance_source: string;
};

export type HashRoute = {
  path: string;
  query: URLSearchParams;
};
  wallet_address: string;
  onchain_balance_satoshi: number;
  balance_source: string;
};

export type HashRoute = {
  path: string;
  query: URLSearchParams;
};

// ========== UTXO 管理相关类型 ==========

/** UTXO 实体 */
export type WalletUTXO = {
  utxo_id: string;
  wallet_id: string;
  address: string;
  txid: string;
  vout: number;
  value_satoshi: number;
  state: string;
  origin_type: string;
  income_eligible: number;
  created_txid: string;
  spent_txid: string;
  reserved_by: string;
  reserved_at_unix: number;
  created_at_unix: number;
  updated_at_unix: number;
  spent_at_unix: number;
};

export type WalletUTXOsResp = {
  total: number;
  limit: number;
  offset: number;
  items: WalletUTXO[];
};

/** UTXO 事件 */
export type UTXOEvent = {
  id: number;
  utxo_id: string;
  event_type: string;
  ref_txid: string;
  ref_business_id: string;
  amount_satoshi: number;
  note: string;
  payload: unknown;
  created_at_unix: number;
};

export type UTXOEventsResp = {
  total: number;
  limit: number;
  offset: number;
  items: UTXOEvent[];
};

// ========== 财务业务相关类型 ==========

/** 财务业务主表 */
export type FinanceBusiness = {
  business_id: string;
  scene_type: string;
  scene_subtype: string;
  from_party_id: string;
  to_party_id: string;
  ref_id: string;
  status: string;
  occurred_at_unix: number;
  idempotency_key: string;
  note: string;
  payload: unknown;
};

export type FinanceBusinessesResp = {
  total: number;
  limit: number;
  offset: number;
  items: FinanceBusiness[];
};

/** 财务分解表 */
export type FinanceBreakdown = {
  id: number;
  business_id: string;
  txid: string;
  gross_input_satoshi: number;
  change_back_satoshi: number;
  external_in_satoshi: number;
  counterparty_out_satoshi: number;
  miner_fee_satoshi: number;
  net_out_satoshi: number;
  net_in_satoshi: number;
  created_at_unix: number;
  note: string;
  payload: unknown;
};

export type FinanceBreakdownsResp = {
  total: number;
  limit: number;
  offset: number;
  items: FinanceBreakdown[];
};

/** 业务-UTXO 关系表 */
export type FinanceUTXOLink = {
  id: number;
  business_id: string;
  txid: string;
  utxo_id: string;
  role: string;
  amount_satoshi: number;
  created_at_unix: number;
  note: string;
  payload: unknown;
};

export type FinanceUTXOLinksResp = {
  total: number;
  limit: number;
  offset: number;
  items: FinanceUTXOLink[];
};

// ========== 调度器任务相关类型 ==========

/** 周期性任务状态 */
export type SchedulerTask = {
  name: string;
  owner: string;
  mode: string;
  interval_seconds: number;
  started_at_unix: number;
  last_trigger: string;
  last_started_at_unix: number;
  last_ended_at_unix: number;
  last_duration_ms: number;
  last_error: string;
  in_flight: boolean;
  run_count: number;
  success_count: number;
  failure_count: number;
};

export type SchedulerTasksResp = {
  now_unix: number;
  summary: {
    task_count: number;
    in_flight_count: number;
    failure_count_total: number;
    enabled_task_count: number;
    filtered_task_count: number;
    active_task_count: number;
    stopped_task_count: number;
    scheduler_available: boolean;
    filter_applied_count: number;
  };
  items: SchedulerTask[];
};

/** 任务执行流水 */
export type SchedulerTaskRun = {
  id: number;
  task_name: string;
  owner: string;
  mode: string;
  trigger: string;
  started_at_unix: number;
  ended_at_unix: number;
  duration_ms: number;
  status: string;
  error_message: string;
  summary: Record<string, unknown>;
  created_at_unix: number;
};

export type SchedulerTaskRunsResp = {
  total: number;
  limit: number;
  offset: number;
  items: SchedulerTaskRun[];
};
