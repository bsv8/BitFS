import React, { useEffect, useState } from "react";

/**
 * 新的认证状态模型（基于私钥解锁）
 * - no_key: 系统中没有私钥，需要先创建或导入
 * - locked: 有私钥但未解锁，需要输入密码解锁
 * - unlocked: 已解锁，可以正常使用所有 API
 * - error: 发生错误
 * - checking: 正在检查状态
 */
type AuthState = "no_key" | "locked" | "unlocked" | "error" | "checking";

// ========== 钱包相关类型 ==========
type TxResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_peer_id: string;
    event_type: string;
    direction: string;
    amount_satoshi: number;
    purpose: string;
    note: string;
  }>;
};

type WalletLedgerResp = {
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

type GatewayEventsResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_peer_id: string;
    action: string;
    msg_id?: string;
    sequence_num?: number;
    pool_id?: string;
    amount_satoshi: number;
    payload: unknown;
  }>;
};

// ========== 文件相关类型 ==========
type SeedItem = {
  seed_hash: string;
  seed_file_path: string;
  chunk_count: number;
  file_size: number;
  unit_price_sat_per_64k: number;
  floor_unit_price_sat_per_64k: number;
  resale_discount_bps: number;
};

type SeedsResp = {
  total: number;
  items: SeedItem[];
};

type SalesResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    session_id: string;
    seed_hash: string;
    chunk_index: number;
    unit_price_sat_per_64k: number;
    amount_satoshi: number;
    buyer_gateway_peer_id: string;
  }>;
};

type FilesResp = {
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
type Gateway = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
  peer_id?: string;
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

type GatewaysResp = {
  items: Gateway[];
  total: number;
};

type Arbiter = {
  id: number;
  enabled: boolean;
  addr: string;
  pubkey: string;
};

type ArbitersResp = {
  items: Gateway[];
  total: number;
};

// ========== Direct 交易相关类型 ==========
type DirectQuote = {
  id: string;
  seed_hash: string;
  chunk_count: number;
  created_at_unix: number;
  expires_at_unix: number;
  quote_satoshi: number;
  seller_peer_id: string;
  seller_addr: string;
  status: string;
};

type DirectQuotesResp = {
  total: number;
  items: DirectQuote[];
};

type DirectDeal = {
  id: string;
  quote_id: string;
  seed_hash: string;
  chunk_count: number;
  created_at_unix: number;
  amount_satoshi: number;
  buyer_peer_id: string;
  seller_peer_id: string;
  status: string;
};

type DirectDealsResp = {
  total: number;
  items: DirectDeal[];
};

type DirectSession = {
  id: string;
  deal_id: string;
  created_at_unix: number;
  ended_at_unix?: number;
  buyer_peer_id: string;
  seller_peer_id: string;
  seed_hash: string;
  chunk_index: number;
  status: string;
  bytes_transferred: number;
};

type DirectSessionsResp = {
  total: number;
  items: DirectSession[];
};

type DirectTransferPool = {
  pool_id: string;
  buyer_peer_id: string;
  seller_peer_id: string;
  created_at_unix: number;
  total_satoshi: number;
  released_satoshi: number;
  status: string;
};

type DirectTransferPoolsResp = {
  total: number;
  items: DirectTransferPool[];
};

// ========== Live 流相关类型 ==========
type LiveStream = {
  stream_id: string;
  file_count: number;
  total_bytes: number;
  last_updated_unix: number;
  workspace_roots?: string[];
  live_stream_folder: string;
};

type LiveStreamsResp = {
  total: number;
  items: LiveStream[];
};

type LiveStreamDetail = {
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

type LiveFollowStatus = {
  following: boolean;
  stream_id?: string;
  started_at_unix?: number;
  current_segment?: string;
  bytes_received?: number;
};

// ========== Admin 管理相关类型 ==========
type AdminConfig = {
  [key: string]: unknown;
};

type AdminConfigSchema = {
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
type OrchestratorEvent = {
  event_id: string;
  started_at_unix: number;
  ended_at_unix: number;
  steps_count: number;
  latest_log_id: number;
  idempotency_key: string;
  aggregate_key: string;
  command_type: string;
  gateway_peer_id: string;
  source: string;
  signal_type: string;
  latest_event_type: string;
  latest_task_status: string;
  latest_retry_count: number;
  latest_queue_length: number;
  last_error_message: string;
};

type OrchestratorEventsResp = {
  total: number;
  items: OrchestratorEvent[];
};

type OrchestratorEventStep = {
  id: number;
  created_at_unix: number;
  event_type: string;
  source: string;
  signal_type: string;
  aggregate_key: string;
  idempotency_key: string;
  command_type: string;
  gateway_peer_id: string;
  task_status: string;
  retry_count: number;
  queue_length: number;
  error_message: string;
  payload: unknown;
};

type OrchestratorEventDetail = {
  event_id: string;
  idempotency_key: string;
  aggregate_key: string;
  command_type: string;
  gateway_peer_id: string;
  started_at_unix: number;
  ended_at_unix: number;
  steps_count: number;
  latest_event_type: string;
  latest_task_status: string;
  last_error_message: string;
  steps: OrchestratorEventStep[];
};

type OrchestratorStatus = {
  enabled: boolean;
  status: string;
  active_signals?: number;
  pending_tasks?: number;
  [key: string]: unknown;
};

// ========== ClientKernel 命令相关类型 ==========
type ClientKernelCommand = {
  id: number;
  created_at_unix: number;
  command_id: string;
  command_type: string;
  gateway_peer_id: string;
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

type ClientKernelCommandsResp = {
  total: number;
  items: ClientKernelCommand[];
};

// ========== FeePool 审计相关类型 ==========
type FeePoolCommand = {
  id: number;
  created_at_unix: number;
  command_id: string;
  command_type: string;
  gateway_peer_id: string;
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

type FeePoolCommandsResp = {
  total: number;
  items: FeePoolCommand[];
};

type FeePoolEvent = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_peer_id: string;
  event_name: string;
  state_before: string;
  state_after: string;
  payload: unknown;
};

type FeePoolEventsResp = {
  total: number;
  items: FeePoolEvent[];
};

type FeePoolState = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_peer_id: string;
  state: string;
  pause_reason: string;
  pause_need_satoshi: number;
  pause_have_satoshi: number;
  last_error: string;
  payload: unknown;
};

type FeePoolStatesResp = {
  total: number;
  items: FeePoolState[];
};

type FeePoolEffect = {
  id: number;
  created_at_unix: number;
  command_id: string;
  gateway_peer_id: string;
  effect_type: string;
  stage: string;
  status: string;
  error_message: string;
  payload: unknown;
};

type FeePoolEffectsResp = {
  total: number;
  items: FeePoolEffect[];
};

// ========== 工作区管理相关类型 ==========
type Workspace = {
  id: number;
  created_at_unix: number;
  updated_at_unix: number;
  path: string;
  max_bytes: number;
  used_bytes: number;
  enabled: boolean;
};

type WorkspacesResp = {
  total: number;
  items: Workspace[];
};

type AdminLiveStorageSummary = {
  stream_count: number;
  file_count: number;
  total_bytes: number;
};

/**
 * 密钥状态响应
 * GET /api/v1/key/status
 */
type KeyStatusResp = {
  appname: string;
  has_key: boolean;
  unlocked: boolean;
};

/**
 * 创建私钥请求
 * POST /api/v1/key/new
 */
type KeyNewReq = {
  password: string;
};

/**
 * 导入私钥请求
 * POST /api/v1/key/import
 */
type KeyImportReq = {
  cipher: Record<string, unknown>;
};

/**
 * 导出私钥响应
 * GET /api/v1/key/export
 */
type KeyExportResp = {
  cipher: Record<string, unknown>;
};

/**
 * 解锁请求
 * POST /api/v1/key/unlock
 */
type KeyUnlockReq = {
  password: string;
};

// ========== 静态文件管理类型 ==========
type StaticItem = {
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

type StaticTreeResp = {
  root_path: string;
  current_path: string;
  parent_path: string;
  total: number;
  items: StaticItem[];
};

// ========== 文件获取任务类型 ==========
type FileGetJob = {
  id: string;
  seed_hash: string;
  chunk_count: number;
  gateway_peer_id: string;
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

type SeedPriceDraft = {
  floor: string;
  discount: string;
  saving?: boolean;
  message?: string;
};

// 直接交易类型
// 直播管理类型（仅保留后端 API 实际返回的字段）
type LiveStorageSummary = {
  total_streams: number;
  total_segments: number;
  total_bytes: number;
  oldest_segment_at_unix: number;
  newest_segment_at_unix: number;
};

// 配置管理类型
type ConfigItem = {
  key: string;
  value: unknown;
};

type ConfigSchemaItem = {
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

type ConfigSchemaResp = {
  items: ConfigSchemaItem[];
  total: number;
};

type WalletSummaryResp = {
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

type HashRoute = {
  path: string;
  query: URLSearchParams;
};

function nowPath(): HashRoute {
  const raw = window.location.hash.replace(/^#/, "");
  const value = raw || "/wallet";
  const [pathPart, queryPart] = value.split("?", 2);
  const path = pathPart || "/wallet";
  const query = new URLSearchParams(queryPart || "");
  return { path, query };
}

function setHash(path: string, query?: URLSearchParams) {
  const q = query && query.toString() ? `?${query.toString()}` : "";
  window.location.hash = `${path}${q}`;
}

// ========== 子组件 ==========
// Modal 对话框组件
function Modal({ title, isOpen, onClose, children }: { title: string, isOpen: boolean, onClose: () => void, children: React.ReactNode }) {
  if (!isOpen) return null;
  return (
    <div className="modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal-dialog">
        <div className="modal-header">
          <h4>{title}</h4>
          <button className="modal-close" onClick={onClose}>×</button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

function GatewayManagerPage({ gateways, onSave, onDelete }: { gateways: GatewaysResp | null, onSave: (id: number | null, data: Gateway) => Promise<void>, onDelete: (id: number) => Promise<void> }) {
  const [editing, setEditing] = useState<{id: number | null, data: Gateway} | null>(null);
  
  if (!gateways) return null;
  
  return (
    <section className="panel">
      <div className="panel-head">
        <h3>网关管理</h3>
        <button className="btn" onClick={() => setEditing({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}>添加网关</button>
      </div>
      <div className="table-wrap"><table><thead><tr><th>ID</th><th>启用</th><th>网络</th><th>健康</th><th>主</th><th>错误</th><th>地址</th><th>Pubkey</th><th>操作</th></tr></thead><tbody>
        {gateways.items.map((g) => {
          // 网络连接状态图标
          let connIcon = "⚫";
          let connTitle = "未知";
          if (g.connectedness === "connected") {
            connIcon = "🟢";
            connTitle = "已连接";
          } else if (g.connectedness === "can_connect") {
            connIcon = "🟡";
            connTitle = "可连接";
          } else if (g.connectedness && g.connectedness !== "not_connected") {
            connIcon = "🔴";
            connTitle = g.connectedness;
          } else if (g.connectedness === "not_connected") {
            connIcon = "🔴";
            connTitle = "未连接";
          }
          // 费用池就绪状态图标（比网络健康更严格）
          const healthIcon = g.fee_pool_ready ? "💚" : (g.last_runtime_error ? "⚠️" : "💔");
          const healthTitle = g.fee_pool_ready ? "费用池就绪" : (g.last_runtime_error ? `费用池异常: ${g.last_runtime_error_stage || "unknown"}` : "未就绪");
          return (
          <tr key={g.id}>
            <td>{g.id}</td>
            <td title={g.enabled ? "已启用" : "已禁用"}>{g.enabled ? "✅" : "⛔"}</td>
            <td title={`${connTitle}${g.last_error ? " | 连接错误: " + g.last_error : ""}${g.last_runtime_error ? " | 运行错误(" + (g.last_runtime_error_stage || "unknown") + "): " + g.last_runtime_error : ""}`}>{connIcon}</td>
            <td title={healthTitle}>{healthIcon}</td>
            <td title={g.is_master ? "主网关(优先下载)" : ""}>{g.is_master ? "👑" : ""}</td>
            <td title={`${g.last_runtime_error_stage ? "[" + g.last_runtime_error_stage + "] " : ""}${g.last_runtime_error || g.last_error || ""}`}>{short((g.last_runtime_error ? `${g.last_runtime_error_stage || "runtime"}: ${g.last_runtime_error}` : (g.last_error || "-")), 28)}</td>
            <td title={g.addr}>{short(g.addr, 30)}</td>
            <td title={g.pubkey}>{short(g.pubkey, 12)}</td>
            <td>
              <button className="btn btn-light" onClick={() => setEditing({ id: g.id, data: { ...g } })}>编辑</button>
              {!g.enabled && <button className="btn btn-light" onClick={() => onDelete(g.id).catch(e => alert(e.message))}>删除</button>}
            </td>
          </tr>
          );
        })}
      </tbody></table></div>
      <Modal 
        title={editing?.id === null ? "添加网关" : "编辑网关"} 
        isOpen={!!editing} 
        onClose={() => setEditing(null)}
      >
        {editing && (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input type="checkbox" checked={editing.data.enabled} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, enabled: e.target.checked } })} /> 
                <span>启用</span>
              </label>
              <input className="input" placeholder="多地址 (/ip4/.../tcp/.../p2p/...)" value={editing.data.addr} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, addr: e.target.value } })} />
              <input className="input" placeholder="公钥 (hex)" value={editing.data.pubkey} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, pubkey: e.target.value } })} />
            </div>
            <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
              <button className="btn" onClick={() => {
                onSave(editing.id, editing.data).then(() => setEditing(null)).catch(e => alert(e.message));
              }}>保存</button>
              <button className="btn btn-light" onClick={() => setEditing(null)}>取消</button>
            </div>
          </>
        )}
      </Modal>
    </section>
  );
}

function ArbiterManagerPage({ arbiters, onSave, onDelete }: { arbiters: ArbitersResp | null, onSave: (id: number | null, data: Arbiter) => Promise<void>, onDelete: (id: number) => Promise<void> }) {
  const [editing, setEditing] = useState<{id: number | null, data: Arbiter} | null>(null);
  
  if (!arbiters) return null;
  
  return (
    <section className="panel">
      <div className="panel-head">
        <h3>仲裁服务器管理</h3>
        <button className="btn" onClick={() => setEditing({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}>添加仲裁服务器</button>
      </div>
      <div className="table-wrap"><table><thead><tr><th>ID</th><th>状态</th><th>地址</th><th>Pubkey</th><th>操作</th></tr></thead><tbody>
        {arbiters.items.map((a) => (
          <tr key={a.id}>
            <td>{a.id}</td>
            <td>{a.enabled ? "✅" : "⛔"}</td>
            <td title={a.addr}>{short(a.addr, 30)}</td>
            <td title={a.pubkey}>{short(a.pubkey, 12)}</td>
            <td>
              <button className="btn btn-light" onClick={() => setEditing({ id: a.id, data: { ...a } })}>编辑</button>
              {!a.enabled && <button className="btn btn-light" onClick={() => onDelete(a.id).catch(e => alert(e.message))}>删除</button>}
            </td>
          </tr>
        ))}
      </tbody></table></div>
      <Modal 
        title={editing?.id === null ? "添加仲裁服务器" : "编辑仲裁服务器"} 
        isOpen={!!editing} 
        onClose={() => setEditing(null)}
      >
        {editing && (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input type="checkbox" checked={editing.data.enabled} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, enabled: e.target.checked } })} /> 
                <span>启用</span>
              </label>
              <input className="input" placeholder="多地址 (/ip4/.../tcp/.../p2p/...)" value={editing.data.addr} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, addr: e.target.value } })} />
              <input className="input" placeholder="公钥 (hex)" value={editing.data.pubkey} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, pubkey: e.target.value } })} />
            </div>
            <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
              <button className="btn" onClick={() => {
                onSave(editing.id, editing.data).then(() => setEditing(null)).catch(e => alert(e.message));
              }}>保存</button>
              <button className="btn btn-light" onClick={() => setEditing(null)}>取消</button>
            </div>
          </>
        )}
      </Modal>
    </section>
  );
}

function StaticFileManager({ 
  staticTree, 
  staticPathHistory,
  staticCurrentPath,
  selectedStaticItems,
  onNavigate,
  onNavigateBack,
  onRefresh,
  onCreateDir,
  onDelete,
  onSetPrice,
  onToggleSelection,
  onUpload
}: { 
  staticTree: StaticTreeResp | null,
  staticPathHistory: string[],
  staticCurrentPath: string,
  selectedStaticItems: Set<string>,
  onNavigate: (path: string) => void,
  onNavigateBack: () => void,
  onRefresh: () => void,
  onCreateDir: (name: string) => void,
  onDelete: (path: string) => void,
  onSetPrice: (path: string, floor: number, discount: number) => void,
  onToggleSelection: (path: string) => void,
  onUpload: (file: File, targetDir: string, overwrite: boolean) => Promise<void>
}) {
  const [newDirName, setNewDirName] = useState("");
  const [showNewDir, setShowNewDir] = useState(false);
  const [editingPrice, setEditingPrice] = useState<{path: string, floor: string, discount: string} | null>(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      await onUpload(file, staticCurrentPath, false);
      onRefresh();
    } catch (err) {
      alert(err instanceof Error ? err.message : "上传失败");
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleCreateDir = () => {
    if (newDirName) {
      onCreateDir(newDirName);
      setNewDirName("");
      setShowNewDir(false);
    }
  };

  const handleSavePrice = () => {
    if (editingPrice) {
      onSetPrice(editingPrice.path, Number(editingPrice.floor), Number(editingPrice.discount));
      setEditingPrice(null);
    }
  };

  if (!staticTree) return null;

  return (
    <section className="panel">
      <div className="panel-head">
        <h3>静态文件管理</h3>
        <div className="filters">
          <button className="btn btn-light" onClick={() => onNavigate("/")}>根目录</button>
          <button className="btn btn-light" disabled={staticCurrentPath === "/"} onClick={onNavigateBack}>返回上级</button>
          <button className="btn" onClick={() => setShowNewDir(true)}>新建文件夹</button>
          <button className="btn" disabled={uploading} onClick={() => fileInputRef.current?.click()}>{uploading ? "上传中..." : "上传文件"}</button>
          <input ref={fileInputRef} type="file" style={{ display: "none" }} onChange={handleFileSelect} />
          <button className="btn btn-light" onClick={onRefresh}>刷新</button>
        </div>
      </div>

      {/* 路径导航 */}
      <div className="path-bar">
        <span>当前路径: </span>
        {staticPathHistory.map((p, i) => (
          <span key={p}>
            {i > 0 && <span style={{ margin: "0 4px" }}>/</span>}
            <button className="btn btn-ghost" style={{ padding: "2px 6px", fontSize: 12 }} onClick={() => onNavigate(p)}>
              {p === "/" ? "根目录" : p.split("/").pop()}
            </button>
          </span>
        ))}
      </div>

      {/* 文件列表 */}
      <div className="table-wrap"><table><thead><tr><th style={{ width: 30 }}></th><th>名称</th><th>类型</th><th>大小</th><th>修改时间</th><th>Seed</th><th>底价</th><th>操作</th></tr></thead><tbody>
        {staticTree.items.length === 0 ? (
          <tr><td colSpan={8} style={{ textAlign: "center", color: "#666" }}>空目录</td></tr>
        ) : (
          staticTree.items.map((item) => (
            <tr key={item.path} className={selectedStaticItems.has(item.path) ? "selected-row" : ""}>
              <td><input type="checkbox" checked={selectedStaticItems.has(item.path)} onChange={() => onToggleSelection(item.path)} /></td>
              <td>
                <span style={{ marginRight: 6 }} className={item.type === "dir" ? "dir-icon" : "file-icon"}>{item.type === "dir" ? "📁" : "📄"}</span>
                {item.type === "dir" ? (
                  <button className="file-name-btn" onClick={() => onNavigate(item.path)}>{item.name}</button>
                ) : (
                  <span style={{ color: "#1a2a3d" }}>{item.name}</span>
                )}
              </td>
              <td>{item.type === "dir" ? "文件夹" : "文件"}</td>
              <td>{item.type === "file" ? formatBytes(item.size) : "-"}</td>
              <td>{t(item.mtime_unix)}</td>
              <td title={item.seed_hash || "-"}>{item.seed_hash ? short(item.seed_hash, 8) : "-"}</td>
              <td>{item.floor_unit_price_sat_per_64k ? sat(item.floor_unit_price_sat_per_64k) : "-"}</td>
              <td>
                {item.type === "file" && (
                  <button className="btn btn-light" onClick={() => setEditingPrice({ path: item.path, floor: String(item.floor_unit_price_sat_per_64k || 10), discount: String(item.resale_discount_bps || 8000) })}>设置价格</button>
                )}
                <button className="btn btn-light" onClick={() => onDelete(item.path)}>删除</button>
              </td>
            </tr>
          ))
        )}
      </tbody></table></div>

      {/* 新建文件夹对话框 */}
      <Modal title="新建文件夹" isOpen={showNewDir} onClose={() => { setShowNewDir(false); setNewDirName(""); }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <label style={{ fontSize: 13, color: "#5f6f85" }}>在当前目录下创建新文件夹</label>
          <input 
            className="input" 
            placeholder="文件夹名称" 
            value={newDirName} 
            onChange={(e) => setNewDirName(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") handleCreateDir(); }}
          />
        </div>
        <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
          <button className="btn" onClick={handleCreateDir}>创建</button>
          <button className="btn btn-light" onClick={() => { setShowNewDir(false); setNewDirName(""); }}>取消</button>
        </div>
      </Modal>

      {/* 设置价格对话框 */}
      <Modal 
        title={editingPrice ? `设置价格: ${short(editingPrice.path, 30)}` : "设置价格"} 
        isOpen={!!editingPrice} 
        onClose={() => setEditingPrice(null)}
      >
        {editingPrice && (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div>
                <label style={{ display: "block", fontSize: 12, color: "#5f6f85", marginBottom: 4 }}>底价 (sat/64k)</label>
                <input 
                  className="input" 
                  type="number" 
                  value={editingPrice.floor} 
                  onChange={(e) => setEditingPrice({ ...editingPrice, floor: e.target.value })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleSavePrice(); }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: 12, color: "#5f6f85", marginBottom: 4 }}>折扣 (bps)</label>
                <input 
                  className="input" 
                  type="number" 
                  value={editingPrice.discount} 
                  onChange={(e) => setEditingPrice({ ...editingPrice, discount: e.target.value })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleSavePrice(); }}
                />
              </div>
            </div>
            <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
              <button className="btn" onClick={handleSavePrice}>保存</button>
              <button className="btn btn-light" onClick={() => setEditingPrice(null)}>取消</button>
            </div>
          </>
        )}
      </Modal>

      <div className="hint" style={{ marginTop: 8 }}>当前目录: {staticTree.current_path} | 选中: {selectedStaticItems.size} 项</div>
    </section>
  );
}

function sat(v: number) {
  return `${Number(v || 0).toLocaleString()} sat`;
}

function t(ts: number) {
  return ts ? new Date(ts * 1000).toLocaleString() : "-";
}

function short(s: string, n = 8) {
  if (!s) return "-";
  if (s.length <= n * 2) return s;
  return `${s.slice(0, n)}...${s.slice(-n)}`;
}

function shortHex(s: string, n = 4) {
  if (!s) return "-";
  if (s.length <= n * 2) return s;
  return `${s.slice(0, n)}...${s.slice(-n)}`;
}

/**
 * 详情表格组件 - 将对象数据渲染为 title:value 表格
 * 长字符串（如 hex、二进制）自动截断显示为 前四...后四 格式
 */
function DetailTable({ data }: { data: Record<string, unknown> | null | undefined })
{
  if (!data || typeof data !== "object") {
    return <div style={{ color: "#6a7d95", padding: "20px 0" }}>无数据</div>;
  }

  const entries = Object.entries(data as Record<string, unknown>);

  if (entries.length === 0) {
    return <div style={{ color: "#6a7d95", padding: "20px 0" }}>空对象</div>;
  }

  return (
    <table className="detail-table">
      <tbody>
        {entries.map(([key, value]) => {
          const { displayValue, title } = formatDetailValue(value);
          return (
            <tr key={key}>
              <td className="detail-key">{key}</td>
              <td className="detail-value" title={title}>{displayValue}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/**
 * 格式化详情字段值
 * - 长 hex 字符串显示为 前四...后四
 * - 嵌套对象递归格式化为 JSON
 * - 数组格式化为 JSON
 */
function formatDetailValue(value: unknown): { displayValue: string; title: string } {
  if (value === null) return { displayValue: "null", title: "null" };
  if (value === undefined) return { displayValue: "undefined", title: "undefined" };

  const type = typeof value;

  if (type === "boolean" || type === "number") {
    const str = String(value);
    return { displayValue: str, title: str };
  }

  if (type === "string") {
    const str = value as string;
    // 长字符串（可能是 hex、txid、hash 等）截断显示
    if (str.length > 20) {
      return { displayValue: shortHex(str, 4), title: str };
    }
    return { displayValue: str, title: str };
  }

  if (type === "object") {
    if (Array.isArray(value)) {
      const json = JSON.stringify(value, null, 2);
      // 数组内容太长时显示摘要
      if (json.length > 100) {
        return { displayValue: `[数组 ${value.length} 项]`, title: json };
      }
      return { displayValue: json, title: json };
    }
    // 嵌套对象
    const json = JSON.stringify(value, null, 2);
    if (json.length > 100) {
      return { displayValue: "{对象}", title: json };
    }
    return { displayValue: json, title: json };
  }

  const str = String(value);
  return { displayValue: str, title: str };
}

function gatewayEventMsg(e: GatewayEventsResp["items"][number]) {
  const msgID = (e.msg_id || "").trim();
  if (msgID) return msgID;
  if (e.payload && typeof e.payload === "object") {
    const payload = e.payload as Record<string, unknown>;
    const stage = typeof payload.stage === "string" ? payload.stage.trim() : "";
    const err = typeof payload.error === "string" ? payload.error.trim() : "";
    if (err) return stage ? `${stage}: ${err}` : err;
  }
  return "-";
}

function toInt(v: string | null, d: number, min = 1, max = 500) {
  const x = Number(v || d);
  if (!Number.isFinite(x)) return d;
  return Math.max(min, Math.min(max, Math.floor(x)));
}

function pageCount(total: number, pageSize: number) {
  return Math.max(1, Math.ceil((total || 0) / pageSize));
}

function Pager({
  total,
  page,
  pageSize,
  onPage,
  onPageSize
}: {
  total: number;
  page: number;
  pageSize: number;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}) {
  const pages = pageCount(total, pageSize);
  return (
    <div className="pager">
      <button className="btn btn-light" disabled={page <= 1} onClick={() => onPage(page - 1)}>上一页</button>
      <span>第 {page} / {pages} 页</span>
      <button className="btn btn-light" disabled={page >= pages} onClick={() => onPage(page + 1)}>下一页</button>
      <select className="input small" value={pageSize} onChange={(e) => onPageSize(Number(e.target.value))}>
        {[10, 20, 50, 100].map((s) => <option key={s} value={s}>{s}/页</option>)}
      </select>
      <span>总计 {total}</span>
    </div>
  );
}

function purposeLabel(purpose: string, eventType: string) {
  const key = (purpose || eventType || "").trim();
  const map: Record<string, string> = {
    listen_bootstrap_topup: "初始化入池",
    auto_renew_topup: "自动续费入池",
    listen_cycle_fee: "侦听周期扣费",
    demand_publish_fee: "需求发布扣费",
    pool_exhausted: "费用池耗尽",
    renew_needed: "续费提醒",
    service_stopped: "服务停止",
    demand_publish_failed: "需求发布失败"
  };
  return map[key] || key || "-";
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

export default function App() {
  // 认证状态
  const [auth, setAuth] = useState<AuthState>("checking");
  const [authErr, setAuthErr] = useState("");
  const [loading, setLoading] = useState(false);
  
  // 密钥状态
  const [keyStatus, setKeyStatus] = useState<KeyStatusResp | null>(null);
  
  // 密码输入（用于解锁/创建）
  const [passwordInput, setPasswordInput] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  
  // 导入的密文
  const [importCipher, setImportCipher] = useState("");
  
  // 视图状态
  const [view, setView] = useState<"unlock" | "create" | "import" | "export">("unlock");

  const [route, setRoute] = useState<HashRoute>(() => nowPath());
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  // 钱包状态
  const [tx, setTx] = useState<TxResp | null>(null);
  const [walletLedger, setWalletLedger] = useState<WalletLedgerResp | null>(null);
  const [gatewayEvents, setGatewayEvents] = useState<GatewayEventsResp | null>(null);
  const [txDetail, setTxDetail] = useState<unknown>(null);
  const [walletLedgerDetail, setWalletLedgerDetail] = useState<unknown>(null);
  const [gatewayEventDetail, setGatewayEventDetail] = useState<Record<string, unknown> | null>(null);
  const [gatewayEventModalOpen, setGatewayEventModalOpen] = useState(false);
  const [walletSummary, setWalletSummary] = useState<WalletSummaryResp | null>(null);

  // 网关状态
  const [gateways, setGateways] = useState<GatewaysResp | null>(null);

  // 仲裁服务器状态
  const [arbiters, setArbiters] = useState<ArbitersResp | null>(null);

  // 文件状态
  const [seeds, setSeeds] = useState<SeedsResp | null>(null);
  const [sales, setSales] = useState<SalesResp | null>(null);
  const [files, setFiles] = useState<FilesResp | null>(null);
  const [seedDrafts, setSeedDrafts] = useState<Record<string, SeedPriceDraft>>({});

  // Direct 交易状态
  const [directQuotes, setDirectQuotes] = useState<DirectQuotesResp | null>(null);
  const [directDeals, setDirectDeals] = useState<DirectDealsResp | null>(null);
  const [directSessions, setDirectSessions] = useState<DirectSessionsResp | null>(null);
  const [directTransferPools, setDirectTransferPools] = useState<DirectTransferPoolsResp | null>(null);

  // Live 流状态
  const [liveStreams, setLiveStreams] = useState<LiveStreamsResp | null>(null);
  const [liveStreamDetail, setLiveStreamDetail] = useState<LiveStreamDetail | null>(null);
  const [liveFollowStatus, setLiveFollowStatus] = useState<LiveFollowStatus | null>(null);
  const [liveStorageSummary, setLiveStorageSummary] = useState<AdminLiveStorageSummary | null>(null);

  // Admin 管理状态
  const [adminConfig, setAdminConfig] = useState<AdminConfig | null>(null);
  const [adminConfigSchema, setAdminConfigSchema] = useState<AdminConfigSchema | null>(null);
  const [configDrafts, setConfigDrafts] = useState<Record<string, string>>({});

  // Orchestrator 调度状态
  const [orchestratorEvents, setOrchestratorEvents] = useState<OrchestratorEventsResp | null>(null);
  const [orchestratorEventDetail, setOrchestratorEventDetail] = useState<OrchestratorEventDetail | null>(null);
  const [orchestratorStatus, setOrchestratorStatus] = useState<OrchestratorStatus | null>(null);
  const [orchestratorModalOpen, setOrchestratorModalOpen] = useState(false);

  // ClientKernel 命令状态
  const [clientKernelCommands, setClientKernelCommands] = useState<ClientKernelCommandsResp | null>(null);
  const [clientKernelCommandDetail, setClientKernelCommandDetail] = useState<ClientKernelCommand | null>(null);
  const [clientKernelModalOpen, setClientKernelModalOpen] = useState(false);

  // FeePool 审计状态
  const [feePoolCommands, setFeePoolCommands] = useState<FeePoolCommandsResp | null>(null);
  const [feePoolCommandDetail, setFeePoolCommandDetail] = useState<FeePoolCommand | null>(null);
  const [feePoolEvents, setFeePoolEvents] = useState<FeePoolEventsResp | null>(null);
  const [feePoolStates, setFeePoolStates] = useState<FeePoolStatesResp | null>(null);
  const [feePoolEffects, setFeePoolEffects] = useState<FeePoolEffectsResp | null>(null);
  const [feePoolAuditModalOpen, setFeePoolAuditModalOpen] = useState(false);
  const [feePoolAuditTab, setFeePoolAuditTab] = useState<"command" | "events" | "states" | "effects">("command");

  // 工作区管理状态
  const [workspaces, setWorkspaces] = useState<WorkspacesResp | null>(null);
  const [workspaceEditing, setWorkspaceEditing] = useState<{id: number | null, data: Partial<Workspace>} | null>(null);

  // 静态文件管理状态
  const [staticTree, setStaticTree] = useState<StaticTreeResp | null>(null);
  const [staticPathHistory, setStaticPathHistory] = useState<string[]>(["/"]);
  const [staticCurrentPath, setStaticCurrentPath] = useState("/");
  const [selectedStaticItems, setSelectedStaticItems] = useState<Set<string>>(new Set());

  // 文件获取任务状态
  const [getFileJob, setGetFileJob] = useState<FileGetJob | null>(null);
  const [getFileSeedHash, setGetFileSeedHash] = useState("");
  const [getFileChunkCount, setGetFileChunkCount] = useState("1");

  // 策略调试日志状态
  const [strategyDebugLogEnabled, setStrategyDebugLogEnabled] = useState<boolean | null>(null);
  const [strategyDebugLogLoading, setStrategyDebugLogLoading] = useState(false);

  useEffect(() => {
    const onHash = () => setRoute(nowPath());
    window.addEventListener("hashchange", onHash);
    if (!window.location.hash) setHash("/finance");
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  /**
   * API 调用函数（新模型 - 无需 token）
   * 未解锁时调用非白名单 API 会返回 423 Locked
   */
  const api = async <T,>(path: string, method = "GET", body?: unknown): Promise<T> => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    const resp = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
    const text = await resp.text();
    let parsed: any = text;
    try { parsed = JSON.parse(text); } catch {}
    if (!resp.ok) throw new Error(typeof parsed === "string" ? parsed : JSON.stringify(parsed, null, 2));
    return parsed as T;
  };

  const updateQuery = (patch: Record<string, string | number | undefined | null>) => {
    const q = new URLSearchParams(route.query.toString());
    for (const [k, v] of Object.entries(patch)) {
      if (v === undefined || v === null || `${v}` === "") q.delete(k);
      else q.set(k, `${v}`);
    }
    if (!q.get("page")) q.set("page", "1");
    if (!q.get("pageSize")) q.set("pageSize", "20");
    setHash(route.path, q);
  };

  // ========== 钱包模块加载函数 ==========
  const loadWalletSummary = async () => {
    try {
      const [txData, summaryData] = await Promise.all([
        api<TxResp>("api/v1/transactions?limit=5&offset=0"),
        api<WalletSummaryResp>("api/v1/wallet/summary")
      ]);
      setTx(txData);
      setWalletSummary(summaryData);
    } catch (e) {
      setTx({ total: 0, items: [] });
      setWalletSummary(null);
      throw e;
    }
  };

  const loadWalletFlows = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const eventType = route.query.get("event_type") || "";
    const direction = route.query.get("direction") || "";
    const purpose = route.query.get("purpose") || "";
    const q = route.query.get("q") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (eventType) params.set("event_type", eventType);
    if (direction) params.set("direction", direction);
    if (purpose) params.set("purpose", purpose);
    if (q) params.set("q", q);
    setTx(await api<TxResp>(`api/v1/transactions?${params.toString()}`));
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    if (detailID > 0) {
      setTxDetail(await api(`api/v1/transactions/detail?id=${detailID}`));
    } else {
      setTxDetail(null);
    }
  };

  const loadWalletLedger = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const direction = route.query.get("direction") || "";
    const category = route.query.get("category") || "";
    const status = route.query.get("status") || "";
    const txid = route.query.get("txid") || "";
    const q = route.query.get("q") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (direction) params.set("direction", direction);
    if (category) params.set("category", category);
    if (status) params.set("status", status);
    if (txid) params.set("txid", txid);
    if (q) params.set("q", q);
    setWalletLedger(await api<WalletLedgerResp>(`api/v1/wallet/ledger?${params.toString()}`));
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    if (detailID > 0) {
      setWalletLedgerDetail(await api(`api/v1/wallet/ledger/detail?id=${detailID}`));
    } else {
      setWalletLedgerDetail(null);
    }
  };

  const loadWalletGatewayEvents = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const gatewayPeerID = route.query.get("gateway_peer_id") || "";
    const action = route.query.get("action") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (gatewayPeerID) params.set("gateway_peer_id", gatewayPeerID);
    if (action) params.set("action", action);
    const list = await api<GatewayEventsResp>(`api/v1/gateways/events?${params.toString()}`);
    setGatewayEvents(list);
    // 网关事件详情通过弹窗显示，不再根据 URL detailId 加载
    setGatewayEventDetail(null);
    setGatewayEventModalOpen(false);
  };

  // 加载网关事件详情并打开弹窗
  const loadGatewayEventDetail = async (id: number) => {
    try {
      const detail = await api<Record<string, unknown>>(`api/v1/gateways/events/detail?id=${id}`);
      setGatewayEventDetail(detail);
      setGatewayEventModalOpen(true);
    } catch (e) {
      alert(e instanceof Error ? e.message : "加载详情失败");
    }
  };

  // ========== 网关模块加载函数 ==========
  const loadGateways = async () => {
    setGateways(await api<GatewaysResp>("api/v1/gateways"));
  };

  const saveGateway = async (id: number | null, data: Gateway) => {
    if (id === null) {
      await api("api/v1/gateways", "POST", data);
    } else {
      await api(`api/v1/gateways?id=${id}`, "PUT", data);
    }
    await loadGateways();
  };

  const deleteGateway = async (id: number) => {
    await api(`api/v1/gateways?id=${id}`, "DELETE");
    await loadGateways();
  };

  // ========== 仲裁服务器管理函数 ==========
  const loadArbiters = async () => {
    setArbiters(await api<ArbitersResp>("api/v1/arbiters"));
  };

  const saveArbiter = async (id: number | null, data: Arbiter) => {
    if (id === null) {
      await api("api/v1/arbiters", "POST", data);
    } else {
      await api(`api/v1/arbiters?id=${id}`, "PUT", data);
    }
    await loadArbiters();
  };

  const deleteArbiter = async (id: number) => {
    await api(`api/v1/arbiters?id=${id}`, "DELETE");
    await loadArbiters();
  };

  // ========== 文件模块加载函数 ==========
  const loadFilesSummary = async () => {
    const [s1, s2] = await Promise.all([
      api<SeedsResp>("api/v1/workspace/seeds?limit=5&offset=0"),
      api<SalesResp>("api/v1/sales?limit=5&offset=0")
    ]);
    setSeeds(s1);
    setSales(s2);
  };

  const loadFileSeeds = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const seedHashLike = route.query.get("seed_hash_like") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (seedHashLike) params.set("seed_hash_like", seedHashLike);
    setSeeds(await api<SeedsResp>(`api/v1/workspace/seeds?${params.toString()}`));
  };

  const loadFileSales = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const seedHash = route.query.get("seed_hash") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (seedHash) params.set("seed_hash", seedHash);
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    const list = await api<SalesResp>(`api/v1/sales?${params.toString()}`);
    setSales(list);
    if (detailID > 0) {
      setSalesDetail(await api(`api/v1/sales/detail?id=${detailID}`));
    } else {
      setSalesDetail(null);
    }
  };

  const [saleDetail, setSalesDetail] = useState<unknown>(null);

  const loadFileIndex = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const pathLike = route.query.get("path_like") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (pathLike) params.set("path_like", pathLike);
    setFiles(await api<FilesResp>(`api/v1/workspace/files?${params.toString()}`));
  };

  // ========== Direct 模块加载函数 ==========
  const loadDirectQuotes = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectQuotes(await api<DirectQuotesResp>(`api/v1/direct/quotes?${params.toString()}`));
  };

  const loadDirectDeals = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectDeals(await api<DirectDealsResp>(`api/v1/direct/deals?${params.toString()}`));
  };

  const loadDirectSessions = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectSessions(await api<DirectSessionsResp>(`api/v1/direct/sessions?${params.toString()}`));
  };

  const loadDirectTransferPools = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectTransferPools(await api<DirectTransferPoolsResp>(`api/v1/direct/transfer-pools?${params.toString()}`));
  };

  // ========== Live 模块加载函数 ==========
  const loadLiveStreams = async () => {
    setLiveStreams(await api<LiveStreamsResp>("api/v1/admin/live/streams"));
  };

  const loadLiveStreamDetail = async (streamId: string) => {
    setLiveStreamDetail(await api<LiveStreamDetail>(`api/v1/admin/live/streams/detail?stream_id=${streamId}`));
  };

  const loadLiveFollowStatus = async () => {
    setLiveFollowStatus(await api<LiveFollowStatus>("api/v1/live/follow/status"));
  };

  const loadLiveStorageSummary = async () => {
    setLiveStorageSummary(await api<AdminLiveStorageSummary>("api/v1/admin/live/storage/summary"));
  };

  const deleteLiveStream = async (streamId: string) => {
    await api(`api/v1/admin/live/streams?stream_id=${streamId}`, "DELETE");
    await loadLiveStreams();
  };

  // ========== Admin 管理加载函数 ==========
  const loadAdminConfig = async () => {
    setAdminConfig(await api<{ config: AdminConfig }>("api/v1/admin/config").then(r => r.config));
  };

  const loadAdminConfigSchema = async () => {
    setAdminConfigSchema(await api<AdminConfigSchema>("api/v1/admin/config/schema"));
  };

  const saveAdminConfig = async (key: string, value: unknown) => {
    try {
      await api("api/v1/admin/config", "POST", {
        items: [{ key, value }]
      });
      await loadAdminConfig();
      setConfigDrafts(prev => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
    } catch (e) {
      alert(e instanceof Error ? e.message : "保存失败");
    }
  };

  const resumeDownload = async (demandId: string) => {
    try {
      await api("api/v1/admin/downloads/resume", "POST", { demand_id: demandId });
      alert("已触发恢复下载");
    } catch (e) {
      alert(e instanceof Error ? e.message : "恢复失败");
    }
  };

  // ========== Orchestrator 调度加载函数 ==========
  const loadOrchestratorLogs = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const eventType = route.query.get("event_type") || "";
    const signalType = route.query.get("signal_type") || "";
    const source = route.query.get("source") || "";
    const gatewayPeerID = route.query.get("gateway_peer_id") || "";
    const taskStatus = route.query.get("task_status") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (eventType) params.set("event_type", eventType);
    if (signalType) params.set("signal_type", signalType);
    if (source) params.set("source", source);
    if (gatewayPeerID) params.set("gateway_peer_id", gatewayPeerID);
    if (taskStatus) params.set("task_status", taskStatus);
    setOrchestratorEvents(await api<OrchestratorEventsResp>(`api/v1/admin/orchestrator/logs?${params.toString()}`));
  };

  const loadOrchestratorLogDetail = async (eventID: string) => {
    const detail = await api<OrchestratorEventDetail>(`api/v1/admin/orchestrator/logs/detail?event_id=${encodeURIComponent(eventID)}`);
    setOrchestratorEventDetail(detail);
    setOrchestratorModalOpen(true);
  };

  const loadOrchestratorStatus = async () => {
    setOrchestratorStatus(await api<OrchestratorStatus>("api/v1/admin/orchestrator/status"));
  };

  // ========== ClientKernel 命令加载函数 ==========
  const loadClientKernelCommands = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const commandType = route.query.get("command_type") || "";
    const gatewayPeerID = route.query.get("gateway_peer_id") || "";
    const status = route.query.get("status") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (commandType) params.set("command_type", commandType);
    if (gatewayPeerID) params.set("gateway_peer_id", gatewayPeerID);
    if (status) params.set("status", status);
    setClientKernelCommands(await api<ClientKernelCommandsResp>(`api/v1/admin/client-kernel/commands?${params.toString()}`));
  };

  const loadClientKernelCommandDetail = async (id: number) => {
    const detail = await api<ClientKernelCommand>(`api/v1/admin/client-kernel/commands/detail?id=${id}`);
    setClientKernelCommandDetail(detail);
    setClientKernelModalOpen(true);
  };

  // ========== FeePool 审计加载函数 ==========
  const loadFeePoolCommands = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const commandType = route.query.get("command_type") || "";
    const gatewayPeerID = route.query.get("gateway_peer_id") || "";
    const status = route.query.get("status") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (commandType) params.set("command_type", commandType);
    if (gatewayPeerID) params.set("gateway_peer_id", gatewayPeerID);
    if (status) params.set("status", status);
    setFeePoolCommands(await api<FeePoolCommandsResp>(`api/v1/admin/feepool/commands?${params.toString()}`));
  };

  const loadFeePoolCommandDetail = async (id: number) => {
    const detail = await api<FeePoolCommand>(`api/v1/admin/feepool/commands/detail?id=${id}`);
    setFeePoolCommandDetail(detail);
    // 同时加载关联数据
    const [events, states, effects] = await Promise.all([
      api<FeePoolEventsResp>(`api/v1/admin/feepool/events?command_id=${detail.command_id}&limit=50`),
      api<FeePoolStatesResp>(`api/v1/admin/feepool/states?command_id=${detail.command_id}&limit=50`),
      api<FeePoolEffectsResp>(`api/v1/admin/feepool/effects?command_id=${detail.command_id}&limit=50`)
    ]);
    setFeePoolEvents(events);
    setFeePoolStates(states);
    setFeePoolEffects(effects);
    setFeePoolAuditTab("command");
    setFeePoolAuditModalOpen(true);
  };

  // ========== 工作区管理加载函数 ==========
  const loadWorkspaces = async () => {
    setWorkspaces(await api<WorkspacesResp>("api/v1/admin/workspaces"));
  };

  const saveWorkspace = async (id: number | null, data: { path?: string; max_bytes?: number; enabled?: boolean }) => {
    if (id === null) {
      await api("api/v1/admin/workspaces", "POST", data);
    } else {
      await api(`api/v1/admin/workspaces?id=${id}`, "PUT", data);
    }
    await loadWorkspaces();
  };

  const deleteWorkspace = async (id: number) => {
    await api(`api/v1/admin/workspaces?id=${id}`, "DELETE");
    await loadWorkspaces();
  };

  // ========== 策略调试日志函数 ==========
  const loadStrategyDebugLog = async () => {
    const resp = await api<{ strategy_debug_log_enabled: boolean }>("api/v1/admin/fs-http/strategy-debug-log");
    setStrategyDebugLogEnabled(resp.strategy_debug_log_enabled);
  };

  const setStrategyDebugLog = async (enabled: boolean) => {
    setStrategyDebugLogLoading(true);
    try {
      await api("api/v1/admin/fs-http/strategy-debug-log", "POST", { enabled });
      setStrategyDebugLogEnabled(enabled);
    } catch (e) {
      alert(e instanceof Error ? e.message : "设置失败");
    } finally {
      setStrategyDebugLogLoading(false);
    }
  };

  // ========== 静态文件管理函数 ==========
  const loadStaticTree = async (path: string) => {
    setBusy(true);
    try {
      const tree = await api<StaticTreeResp>(`api/v1/admin/static/tree?path=${encodeURIComponent(path)}`);
      setStaticTree(tree);
      setStaticCurrentPath(path);
      // 更新路径历史
      const historyIndex = staticPathHistory.indexOf(path);
      if (historyIndex >= 0) {
        setStaticPathHistory(staticPathHistory.slice(0, historyIndex + 1));
      } else {
        setStaticPathHistory([...staticPathHistory, path]);
      }
      setSelectedStaticItems(new Set());
    } catch (e) {
      setErr(e instanceof Error ? e.message : "加载失败");
    } finally {
      setBusy(false);
    }
  };

  const navigateToStaticPath = (path: string) => {
    loadStaticTree(path);
  };

  const navigateBack = () => {
    if (staticPathHistory.length > 1) {
      const newHistory = staticPathHistory.slice(0, -1);
      const parentPath = newHistory[newHistory.length - 1];
      setStaticPathHistory(newHistory);
      loadStaticTree(parentPath);
    }
  };

  const createStaticDir = async (name: string) => {
    const newPath = staticCurrentPath === "/" ? `/${name}` : `${staticCurrentPath}/${name}`;
    await api("api/v1/admin/static/mkdir", "POST", { path: newPath });
    await loadStaticTree(staticCurrentPath);
  };

  const moveStaticItem = async (fromPath: string, toPath: string) => {
    await api("api/v1/admin/static/move", "POST", { from: fromPath, to: toPath });
    await loadStaticTree(staticCurrentPath);
  };

  const deleteStaticEntry = async (path: string) => {
    if (!confirm(`确定删除 ${path} 吗？`)) return;
    await api("api/v1/admin/static/entry", "DELETE", { path });
    await loadStaticTree(staticCurrentPath);
  };

  const toggleStaticItemSelection = (path: string) => {
    const newSelection = new Set(selectedStaticItems);
    if (newSelection.has(path)) {
      newSelection.delete(path);
    } else {
      newSelection.add(path);
    }
    setSelectedStaticItems(newSelection);
  };

  const setStaticItemPrice = async (path: string, floorPrice: number, discountBps: number) => {
    await api("api/v1/admin/static/price/set", "POST", {
      path,
      floor_price_sat_per_64k: floorPrice,
      resale_discount_bps: discountBps
    });
    await loadStaticTree(staticCurrentPath);
  };

  const uploadStaticFile = async (file: File, targetDir: string, overwrite: boolean) => {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("target_dir", targetDir);
    formData.append("overwrite", overwrite ? "1" : "0");
    
    const resp = await fetch("api/v1/admin/static/upload", {
      method: "POST",
      body: formData
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(text);
    }
  };

  // ========== 文件获取任务函数 ==========
  const loadGetFileJob = async () => {
    const jobID = route.query.get("job_id") || "";
    if (!jobID) {
      setGetFileJob(null);
      return;
    }
    setGetFileJob(await api<FileGetJob>(`api/v1/files/get-file/job?id=${encodeURIComponent(jobID)}`));
  };

  const startGetFileJob = async () => {
    const seedHash = getFileSeedHash.trim().toLowerCase();
    const chunkCount = Number(getFileChunkCount || "0");
    if (!seedHash || !chunkCount || chunkCount <= 0) {
      setErr("seed_hash 和 chunk_count 必须有效");
      return;
    }
    const out = await api<{ job_id: string }>("api/v1/files/get-file", "POST", {
      seed_hash: seedHash,
      chunk_count: chunkCount
    });
    updateQuery({ job_id: out.job_id });
  };

  // ========== 种子价格管理函数 ==========
  const setSeedDraft = (seed: SeedItem, patch: Partial<SeedPriceDraft>) => {
    setSeedDrafts((prev) => {
      const cur = prev[seed.seed_hash] ?? {
        floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
        discount: String(seed.resale_discount_bps || 8000)
      };
      return { ...prev, [seed.seed_hash]: { ...cur, ...patch } };
    });
  };

  const saveSeedPrice = async (seed: SeedItem) => {
    const d = seedDrafts[seed.seed_hash] ?? {
      floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
      discount: String(seed.resale_discount_bps || 8000)
    };
    setSeedDraft(seed, { saving: true, message: "" });
    try {
      await api("api/v1/workspace/seeds/price", "POST", {
        seed_hash: seed.seed_hash,
        floor_price_sat_per_64k: Number(d.floor || "0"),
        resale_discount_bps: Number(d.discount || "0")
      });
      setSeedDraft(seed, { saving: false, message: "已更新" });
      await loadFileSeeds();
    } catch (e) {
      setSeedDraft(seed, { saving: false, message: e instanceof Error ? e.message : "更新失败" });
    }
  };

  // ========== 路由处理 ==========
  useEffect(() => {
    if (auth !== "unlocked") return;
    const run = async () => {
      setBusy(true);
      setErr("");
      try {
        switch (route.path) {
          // 资金管理模块
          case "/finance":
            await loadWalletSummary();
            break;
          case "/finance/ledger":
            await loadWalletLedger();
            break;
          case "/finance/flows":
            await loadWalletFlows();
            break;
          case "/finance/gateway-flows":
            await loadWalletGatewayEvents();
            break;
          case "/finance/transfer-pools":
            await loadDirectTransferPools();
            break;
          // 设置模块
          case "/settings/gateways":
            await loadGateways();
            break;
          case "/settings/arbiters":
            await loadArbiters();
            break;
          // 文件模块
          case "/files":
            await Promise.all([loadFilesSummary(), loadGetFileJob()]);
            break;
          case "/files/seeds":
          case "/files/pricing":
            await loadFileSeeds();
            break;
          case "/files/sales":
            await loadFileSales();
            break;
          case "/files/index":
            await loadFileIndex();
            break;
          // Direct 模块
          case "/direct/quotes":
            await loadDirectQuotes();
            break;
          case "/direct/deals":
            await loadDirectDeals();
            break;
          case "/direct/sessions":
            await loadDirectSessions();
            break;
          // Live 模块
          case "/live/streams":
            await loadLiveStreams();
            break;
          case "/live/follow":
            await loadLiveFollowStatus();
            break;
          case "/live/storage":
            await loadLiveStorageSummary();
            break;
          // Admin 管理模块
          case "/admin/orchestrator":
            await Promise.all([loadOrchestratorLogs(), loadOrchestratorStatus()]);
            break;
          case "/admin/client-kernel":
            await loadClientKernelCommands();
            break;
          case "/admin/downloads":
            // 暂无专门 API，仅保留入口
            break;
          case "/admin/feepool":
            await loadFeePoolCommands();
            break;
          case "/admin/workspaces":
            await loadWorkspaces();
            break;
          case "/admin/live":
            await loadLiveStreams();
            break;
          case "/admin/static":
            if (!staticTree) {
              await loadStaticTree("/");
            }
            break;
          case "/admin/config":
            await Promise.all([loadAdminConfig(), loadAdminConfigSchema(), loadStrategyDebugLog()]);
            break;
          default:
            setHash("/finance");
        }
      } catch (e) {
        setErr(e instanceof Error ? e.message : "加载失败");
      } finally {
        setBusy(false);
      }
    };
    void run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth, route.path, route.query.toString()]);

  // 文件任务轮询
  useEffect(() => {
    if (auth !== "unlocked") return;
    if (route.path !== "/files") return;
    const jobID = route.query.get("job_id");
    if (!jobID) return;
    const timer = window.setInterval(() => {
      void loadGetFileJob();
    }, 2000);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth, route.path, route.query.get("job_id")]);

  // 初始化：检查密钥状态
  useEffect(() => {
    const initAuth = async () => {
      setAuth("checking");
      setAuthErr("");
      try {
        await checkKeyStatus();
      } catch (e) {
        setAuth("error");
        setAuthErr(e instanceof Error ? e.message : "初始化状态检查失败");
      }
    };
    void initAuth();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /**
   * 检查密钥状态
   * GET /api/v1/key/status
   */
  const checkKeyStatus = async (): Promise<KeyStatusResp> => {
    const status = await api<KeyStatusResp>("api/v1/key/status", "GET");
    setKeyStatus(status);
    
    if (!status.has_key) {
      setAuth("no_key");
      setView("create");
    } else if (!status.unlocked) {
      setAuth("locked");
      setView("unlock");
    } else {
      setAuth("unlocked");
    }
    
    return status;
  };

  /**
   * 创建新私钥
   * POST /api/v1/key/new
   */
  const createKey = async (password: string, confirm: string) => {
    if (!password) {
      setAuthErr("请输入密码");
      return;
    }
    if (password !== confirm) {
      setAuthErr("两次输入的密码不一致");
      return;
    }
    if (password.length < 8) {
      setAuthErr("密码长度至少 8 位");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await api("api/v1/key/new", "POST", { password } as KeyNewReq);
      // 创建成功后自动解锁
      await unlock(password);
    } catch (e) {
      setAuth("no_key");
      setAuthErr(e instanceof Error ? e.message : "创建私钥失败");
    } finally {
      setLoading(false);
    }
  };

  /**
   * 导入私钥
   * POST /api/v1/key/import
   */
  const importKey = async (cipherJson: string) => {
    if (!cipherJson.trim()) {
      setAuthErr("请输入密文 JSON");
      return;
    }
    
    let cipher: Record<string, unknown>;
    try {
      cipher = JSON.parse(cipherJson);
    } catch {
      setAuthErr("无效的 JSON 格式");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await api("api/v1/key/import", "POST", { cipher } as KeyImportReq);
      // 导入成功后切换到解锁视图
      setAuth("locked");
      setView("unlock");
      setAuthErr("私钥已导入，请输入密码解锁");
    } catch (e) {
      setAuth("no_key");
      setAuthErr(e instanceof Error ? e.message : "导入私钥失败");
    } finally {
      setLoading(false);
    }
  };

  /**
   * 解锁私钥
   * POST /api/v1/key/unlock
   */
  const unlock = async (password: string) => {
    if (!password) {
      setAuthErr("请输入密码");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await api("api/v1/key/unlock", "POST", { password } as KeyUnlockReq);
      setAuth("unlocked");
      setPasswordInput("");
    } catch (e) {
      setAuth("locked");
      setAuthErr(e instanceof Error ? e.message : "解锁失败，密码错误");
    } finally {
      setLoading(false);
    }
  };

  /**
   * 锁定/注销
   * POST /api/v1/key/lock
   */
  const lock = async () => {
    setLoading(true);
    try {
      await api("api/v1/key/lock", "POST");
      setAuth("locked");
      setView("unlock");
      setPasswordInput("");
      // 清空业务状态
      resetBusinessState();
    } catch (e) {
      // 即使 API 失败，也切换到锁定状态
      setAuth("locked");
      setView("unlock");
      resetBusinessState();
    } finally {
      setLoading(false);
    }
  };

  /**
   * 导出私钥
   * GET /api/v1/key/export
   */
  const exportKey = async (): Promise<string | null> => {
    try {
      const resp = await api<KeyExportResp>("api/v1/key/export", "GET");
      return JSON.stringify(resp.cipher, null, 2);
    } catch (e) {
      setAuthErr(e instanceof Error ? e.message : "导出私钥失败");
      return null;
    }
  };

  /**
   * 重置业务状态（锁定时调用）
   */
  const resetBusinessState = () => {
    // 清空所有业务状态
    setTx(null);
    setWalletLedger(null);
    setSeeds(null);
    setSales(null);
    setFiles(null);
    setGatewayEvents(null);
    setGateways(null);
    setDirectQuotes(null);
    setDirectDeals(null);
    setDirectSessions(null);
    setDirectTransferPools(null);
    setLiveStreams(null);
    setLiveStreamDetail(null);
    setLiveFollowStatus(null);
    setLiveStorageSummary(null);
    setAdminConfig(null);
    setAdminConfigSchema(null);
    setStaticTree(null);
    setSeedDrafts({});
    setConfigDrafts({});
    setStaticPathHistory(["/"]);
    setStaticCurrentPath("/");
    setSelectedStaticItems(new Set());
    setTxDetail(null);
    setWalletLedgerDetail(null);
    setSalesDetail(null);
    setGatewayEventDetail(null);
    setGetFileJob(null);
    setOrchestratorEvents(null);
    setOrchestratorEventDetail(null);
    setOrchestratorStatus(null);
    setOrchestratorModalOpen(false);
    setClientKernelCommands(null);
    setClientKernelCommandDetail(null);
    setClientKernelModalOpen(false);
    setFeePoolCommands(null);
    setFeePoolCommandDetail(null);
    setFeePoolAuditModalOpen(false);
    setWalletSummary(null);
    setArbiters(null);
    setWorkspaces(null);
  };

  // 计算当前模块
  const moduleName = route.path.startsWith("/files") ? "files" :
    route.path.startsWith("/settings") ? "settings" :
      route.path.startsWith("/direct") ? "direct" :
        route.path.startsWith("/live") ? "live" :
          route.path.startsWith("/admin") ? "admin" :
            route.path.startsWith("/finance") ? "finance" : "finance";

  const routeQueryText = route.query.toString();
  const routeMeta = routeQueryText ? `?${route.query.size} params` : "";

  // ========== 认证/解锁页面 ==========
  if (auth !== "unlocked") {
    return (
      <div className="login-shell">
        <div className="login-card">
          <p className="eyebrow">BitFS Client {keyStatus?.appname ? `· ${keyStatus.appname}` : ""}</p>
          
          {/* 加载中状态 */}
          {auth === "checking" && (
            <>
              <h1>系统初始化中...</h1>
              <p className="desc">正在检查密钥状态，请稍候</p>
            </>
          )}
          
          {/* 错误状态 */}
          {auth === "error" && (
            <>
              <h1>⚠️ 出错了</h1>
              <p className="desc">{authErr || "系统初始化失败"}</p>
              <button className="btn" onClick={() => void checkKeyStatus()}>重试</button>
            </>
          )}
          
          {/* 无密钥状态 - 需要创建或导入 */}
          {auth === "no_key" && (
            <>
              {view === "create" && (
                <>
                  <h1>🔐 创建新私钥</h1>
                  <p className="desc">系统中没有私钥，请设置密码创建新私钥。<br />密码将用于加密私钥，请妥善保管。</p>
                  <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 20 }}>
                    <input 
                      className="input" 
                      type="password" 
                      value={passwordInput} 
                      onChange={(e) => setPasswordInput(e.target.value)} 
                      placeholder="设置密码（至少8位）"
                      onKeyDown={(e) => { if (e.key === "Enter") createKey(passwordInput, confirmPassword); }}
                    />
                    <input 
                      className="input" 
                      type="password" 
                      value={confirmPassword} 
                      onChange={(e) => setConfirmPassword(e.target.value)} 
                      placeholder="确认密码"
                      onKeyDown={(e) => { if (e.key === "Enter") createKey(passwordInput, confirmPassword); }}
                    />
                    <button
                      className="btn"
                      onClick={() => void createKey(passwordInput, confirmPassword)}
                      disabled={loading}
                    >
                      创建并解锁
                    </button>
                    <button className="btn btn-light" onClick={() => { setView("import"); setAuthErr(""); }}>
                      已有私钥？点击导入
                    </button>
                  </div>
                </>
              )}
              
              {view === "import" && (
                <>
                  <h1>📥 导入私钥</h1>
                  <p className="desc">请粘贴导出的密文 JSON，导入后原密码仍然有效。</p>
                  <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 20 }}>
                    <textarea
                      className="input"
                      style={{ minHeight: 120, fontFamily: "monospace", fontSize: 12 }}
                      value={importCipher}
                      onChange={(e) => setImportCipher(e.target.value)}
                      placeholder={`{\n  "version": 1,\n  "cipher": "..."\n}`}
                    />
                    <button
                      className="btn"
                      onClick={() => void importKey(importCipher)}
                      disabled={loading}
                    >
                      导入私钥
                    </button>
                    <button className="btn btn-light" onClick={() => { setView("create"); setAuthErr(""); }}>
                      返回创建新私钥
                    </button>
                  </div>
                </>
              )}
            </>
          )}
          
          {/* 锁定状态 - 需要输入密码解锁 */}
          {auth === "locked" && (
            <>
              {view === "unlock" && (
                <>
                  <h1>🔒 系统已锁定</h1>
                  <p className="desc">私钥已加密存储，请输入密码解锁以继续使用。</p>
                  <div className="login-row" style={{ marginTop: 20 }}>
                    <input 
                      className="input" 
                      type="password" 
                      value={passwordInput} 
                      onChange={(e) => setPasswordInput(e.target.value)} 
                      placeholder="输入密码"
                      onKeyDown={(e) => { if (e.key === "Enter") unlock(passwordInput); }}
                    />
                    <button
                      className="btn"
                      onClick={() => void unlock(passwordInput)}
                      disabled={loading}
                    >
                      解锁
                    </button>
                  </div>
                  <div style={{ marginTop: 16, display: "flex", gap: 8, justifyContent: "center" }}>
                    <button className="btn btn-light" onClick={() => { setView("export"); setAuthErr(""); }}>
                      导出私钥备份
                    </button>
                  </div>
                </>
              )}
              
              {view === "export" && (
                <>
                  <h1>📤 导出私钥</h1>
                  <p className="desc">以下是加密后的私钥密文，请妥善保存。<br />导出操作不会删除原私钥。</p>
                  <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 20 }}>
                    <textarea
                      className="input"
                      style={{ minHeight: 120, fontFamily: "monospace", fontSize: 12 }}
                      readOnly
                      value={importCipher || "点击获取密文..."}
                      onClick={async () => {
                        const cipher = await exportKey();
                        if (cipher) setImportCipher(cipher);
                      }}
                    />
                    <button
                      className="btn"
                      onClick={async () => {
                        const cipher = await exportKey();
                        if (cipher) {
                          setImportCipher(cipher);
                          // 复制到剪贴板
                          navigator.clipboard.writeText(cipher);
                          alert("密文已复制到剪贴板");
                        }
                      }}
                    >
                      获取并复制密文
                    </button>
                    <button className="btn btn-light" onClick={() => { setView("unlock"); setAuthErr(""); }}>
                      返回解锁
                    </button>
                  </div>
                </>
              )}
            </>
          )}
          
          {authErr ? <p className="err" style={{ marginTop: 16 }}>{authErr}</p> : null}
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <p className="eyebrow">BitFS Ops</p>
          <h2>控制台</h2>
        </div>

        {/* 资金管理大项 */}
        <div className="menu-group">
          <h4>💰 资金管理</h4>
          <button className={route.path === "/finance" ? "menu active" : "menu"} onClick={() => setHash("/finance")}>资金总览</button>
          <button className={route.path === "/finance/ledger" ? "menu active" : "menu"} onClick={() => setHash("/finance/ledger", new URLSearchParams("page=1&pageSize=20"))}>链上账本</button>
          <button className={route.path === "/finance/flows" ? "menu active" : "menu"} onClick={() => setHash("/finance/flows", new URLSearchParams("page=1&pageSize=20"))}>费用池流水</button>
          <button className={route.path === "/finance/gateway-flows" ? "menu active" : "menu"} onClick={() => setHash("/finance/gateway-flows", new URLSearchParams("page=1&pageSize=20"))}>网关资金流</button>
          <button className={route.path === "/finance/transfer-pools" ? "menu active" : "menu"} onClick={() => setHash("/finance/transfer-pools", new URLSearchParams("page=1&pageSize=20"))}>Direct资金池</button>
        </div>

        {/* Direct 交易模块 */}
        <div className="menu-group">
          <h4>🔗 Direct 交易</h4>
          <button className={route.path === "/direct/quotes" ? "menu active" : "menu"} onClick={() => setHash("/direct/quotes", new URLSearchParams("page=1&pageSize=20"))}>报价列表</button>
          <button className={route.path === "/direct/deals" ? "menu active" : "menu"} onClick={() => setHash("/direct/deals", new URLSearchParams("page=1&pageSize=20"))}>成交记录</button>
          <button className={route.path === "/direct/sessions" ? "menu active" : "menu"} onClick={() => setHash("/direct/sessions", new URLSearchParams("page=1&pageSize=20"))}>会话管理</button>
        </div>

        {/* Live 模块 */}
        <div className="menu-group">
          <h4>📡 Live 直播</h4>
          <button className={route.path === "/live/streams" ? "menu active" : "menu"} onClick={() => setHash("/live/streams")}>直播流</button>
          <button className={route.path === "/live/follow" ? "menu active" : "menu"} onClick={() => setHash("/live/follow")}>我的关注</button>
          <button className={route.path === "/live/storage" ? "menu active" : "menu"} onClick={() => setHash("/live/storage")}>存储概览</button>
        </div>

        {/* 文件模块 */}
        <div className="menu-group">
          <h4>📁 文件</h4>
          <button className={route.path === "/files" ? "menu active" : "menu"} onClick={() => setHash("/files")}>模块首页</button>
          <button className={route.path === "/files/seeds" ? "menu active" : "menu"} onClick={() => setHash("/files/seeds", new URLSearchParams("page=1&pageSize=20"))}>种子列表</button>
          <button className={route.path === "/files/pricing" ? "menu active" : "menu"} onClick={() => setHash("/files/pricing", new URLSearchParams("page=1&pageSize=20"))}>价格管理</button>
          <button className={route.path === "/files/sales" ? "menu active" : "menu"} onClick={() => setHash("/files/sales", new URLSearchParams("page=1&pageSize=20"))}>售卖记录</button>
          <button className={route.path === "/files/index" ? "menu active" : "menu"} onClick={() => setHash("/files/index", new URLSearchParams("page=1&pageSize=20"))}>文件索引</button>
        </div>

        {/* Admin 管理模块 */}
        <div className="menu-group">
          <h4>⚙️ 管理</h4>
          <button className={route.path === "/admin/orchestrator" ? "menu active" : "menu"} onClick={() => setHash("/admin/orchestrator", new URLSearchParams("page=1&pageSize=20"))}>调度器</button>
          <button className={route.path === "/admin/client-kernel" ? "menu active" : "menu"} onClick={() => setHash("/admin/client-kernel", new URLSearchParams("page=1&pageSize=20"))}>内核命令</button>
          <button className={route.path === "/admin/feepool" ? "menu active" : "menu"} onClick={() => setHash("/admin/feepool", new URLSearchParams("page=1&pageSize=20"))}>费用池审计</button>
          <button className={route.path === "/admin/workspaces" ? "menu active" : "menu"} onClick={() => setHash("/admin/workspaces")}>工作区管理</button>
          <button className={route.path === "/admin/static" ? "menu active" : "menu"} onClick={() => setHash("/admin/static")}>静态文件</button>
          <button className={route.path === "/admin/live" ? "menu active" : "menu"} onClick={() => setHash("/admin/live")}>Live 管理</button>
          <button className={route.path === "/admin/downloads" ? "menu active" : "menu"} onClick={() => setHash("/admin/downloads")}>下载管理</button>
          <button className={route.path === "/admin/config" ? "menu active" : "menu"} onClick={() => setHash("/admin/config")}>系统配置</button>
        </div>

        {/* 设置模块 */}
        <div className="menu-group">
          <h4>🔧 设置</h4>
          <button className={route.path === "/settings/gateways" ? "menu active" : "menu"} onClick={() => setHash("/settings/gateways")}>网关管理</button>
          <button className={route.path === "/settings/arbiters" ? "menu active" : "menu"} onClick={() => setHash("/settings/arbiters")}>仲裁服务器</button>
        </div>

        <button className="btn btn-ghost" onClick={() => void lock()}>🔒 锁定/注销</button>
      </aside>

      <main className="content">
        <header className="top">
          <h1>
            {moduleName === "finance" ? "💰 资金管理" :
              moduleName === "direct" ? "🔗 Direct 交易" :
                moduleName === "live" ? "📡 Live 直播" :
                  moduleName === "files" ? "📁 文件模块" :
                    moduleName === "admin" ? "⚙️ 系统管理" : "🔧 系统设置"}
          </h1>
          <div className="path" title={`${route.path}${routeQueryText ? `?${routeQueryText}` : ""}`}>
            {route.path}{routeMeta}
          </div>
        </header>

        {busy ? <div className="panel">加载中...</div> : null}
        {err ? <div className="panel err-inline">{err}</div> : null}

        {/* ========== 资金管理模块页面 ========== */}
        {route.path === "/finance" && !busy && !tx ? (
          <section className="panel">
            <h3>资金总览</h3>
            <div className="hint">暂无数据，请检查 API 连接或刷新页面</div>
          </section>
        ) : null}
        {route.path === "/finance" && tx ? (
          <>
            {walletSummary && (
              <section className="stats-grid">
                <article className="stat">
                  <p>链上余额</p>
                  <h3>{sat(walletSummary.onchain_balance_satoshi)}</h3>
                  <div className="hint" style={{fontSize: 11, marginTop: 4}}>
                    {short(walletSummary.wallet_address, 16)}
                    <button className="btn btn-light" style={{marginLeft: 8, padding: "2px 6px", fontSize: 10}} onClick={() => {navigator.clipboard.writeText(walletSummary.wallet_address); alert("地址已复制");}}>复制</button>
                  </div>
                </article>
                <article className="stat">
                  <p>累计流入</p>
                  <h3 style={{color: "#0e8f43"}}>{sat(walletSummary.ledger_total_in_satoshi)}</h3>
                </article>
                <article className="stat">
                  <p>累计流出</p>
                  <h3 style={{color: "#b51c3f"}}>{sat(walletSummary.ledger_total_out_satoshi)}</h3>
                </article>
                <article className="stat">
                  <p>净流入</p>
                  <h3>{sat(walletSummary.ledger_net_satoshi)}</h3>
                </article>
                <article className="stat">
                  <p>费用池总投入</p>
                  <h3>{sat(walletSummary.total_in_satoshi)}</h3>
                </article>
                <article className="stat">
                  <p>费用池已使用</p>
                  <h3>{sat(walletSummary.total_used_satoshi)}</h3>
                </article>
              </section>
            )}
            <section className="panel">
              <h3>最近费用划拨流水（摘要）</h3>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>方向</th><th>金额</th><th>划拨原因</th><th>备注</th></tr></thead><tbody>
                {tx.items.map((it) => <tr key={it.id}><td>{t(it.created_at_unix)}</td><td>{it.direction}</td><td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td><td>{purposeLabel(it.purpose, it.event_type)}</td><td>{it.note || "-"}</td></tr>)}
              </tbody></table></div>
            </section>
          </>
        ) : null}

        {route.path === "/finance/flows" && tx ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const eventType = route.query.get("event_type") || "";
          const direction = route.query.get("direction") || "";
          const purpose = route.query.get("purpose") || "";
          const q = route.query.get("q") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>费用池流水</h3>
                <div className="filters">
                  <input className="input" defaultValue={eventType} placeholder="event_type" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ event_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={direction} placeholder="direction" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ direction: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={purpose} placeholder="purpose" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ purpose: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={q} placeholder="关键词" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ q: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>方向</th><th>金额</th><th>划拨原因</th><th>备注</th><th>网关</th></tr></thead><tbody>
                {tx.items.map((it) => (
                  <tr key={it.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === it.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: it.id })}>
                    <td>{t(it.created_at_unix)}</td>
                    <td>{it.direction}</td>
                    <td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                    <td>{purposeLabel(it.purpose, it.event_type)}</td>
                    <td>{it.note || "-"}</td>
                    <td title={it.gateway_peer_id}>{short(it.gateway_peer_id, 8)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={tx.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {txDetail ? <pre className="detail-pre">{JSON.stringify(txDetail, null, 2)}</pre> : <div className="hint">点击某条记录查看详细信息</div>}
            </section>
          );
        })() : null}

        {route.path === "/finance/ledger" && walletLedger ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const direction = route.query.get("direction") || "";
          const category = route.query.get("category") || "";
          const status = route.query.get("status") || "";
          const txid = route.query.get("txid") || "";
          const q = route.query.get("q") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>链上流水（钱包账本）</h3>
                <div className="filters">
                  <input className="input" defaultValue={direction} placeholder="direction: IN/OUT" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ direction: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={category} placeholder="category" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ category: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={status} placeholder="status: MEMPOOL/CONFIRMED" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={txid} placeholder="txid" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ txid: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={q} placeholder="关键词" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ q: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>方向</th><th>金额</th><th>分类</th><th>状态</th><th>txid</th></tr></thead><tbody>
                {walletLedger.items.map((it) => (
                  <tr key={it.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === it.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: it.id })}>
                    <td>{t(it.occurred_at_unix || it.created_at_unix)}</td>
                    <td>{it.direction}</td>
                    <td className={it.direction === "OUT" ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                    <td>{it.category || "-"}</td>
                    <td>{it.status || "-"}</td>
                    <td title={it.txid}>{shortHex(it.txid || "", 8)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={walletLedger.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {walletLedgerDetail ? <pre className="detail-pre">{JSON.stringify(walletLedgerDetail, null, 2)}</pre> : <div className="hint">点击某条记录查看链上流水详情</div>}
            </section>
          );
        })() : null}

        {route.path === "/finance/gateway-flows" && gatewayEvents ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const action = route.query.get("action") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>网关资金流</h3>
                <div className="filters">
                  <input className="input" defaultValue={gatewayPeerID} placeholder="gateway_peer_id" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={action} placeholder="action" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ action: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>网关</th><th>动作</th><th>金额</th><th>pool</th><th>msg</th></tr></thead><tbody>
                {gatewayEvents.items.map((e) => {
                  const msg = gatewayEventMsg(e);
                  return (
                    <tr key={e.id} onClick={() => loadGatewayEventDetail(e.id)}>
                      <td>{t(e.created_at_unix)}</td>
                      <td title={e.gateway_peer_id}>{short(e.gateway_peer_id, 8)}</td>
                      <td>{e.action}</td>
                      <td>{sat(e.amount_satoshi)}</td>
                      <td>{e.pool_id || "-"}</td>
                      <td title={msg}>{short(msg, 24)}</td>
                    </tr>
                  );
                })}
              </tbody></table></div>
              <Pager total={gatewayEvents.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              <div className="hint">点击某条记录查看事件详情</div>

              {/* 网关事件详情弹窗 */}
              <Modal
                title="网关事件详情"
                isOpen={gatewayEventModalOpen}
                onClose={() => setGatewayEventModalOpen(false)}
              >
                {gatewayEventDetail && (
                  <div className="detail-table-wrap">
                    <DetailTable data={gatewayEventDetail} />
                  </div>
                )}
                <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                  <button className="btn btn-light" onClick={() => setGatewayEventModalOpen(false)}>关闭</button>
                </div>
              </Modal>
            </section>
          );
        })() : null}

        {/* ========== 网关设置页面 ========== */}
        {route.path === "/settings/gateways" && <GatewayManagerPage gateways={gateways} onSave={saveGateway} onDelete={deleteGateway} />}
        {route.path === "/settings/arbiters" && <ArbiterManagerPage arbiters={arbiters} onSave={saveArbiter} onDelete={deleteArbiter} />}
        {/* DEBUG: route.path={route.path}, seeds={!!seeds}, gateways={!!gateways} */}

        {/* ========== 文件模块页面 ========== */}
        {route.path === "/files" && seeds && sales ? (
          <>
            <section className="stats-grid">
              <article className="stat"><p>种子总数</p><h3>{seeds.total}</h3></article>
              <article className="stat"><p>售卖记录总数</p><h3>{sales.total}</h3></article>
              <article className="stat"><p>模块</p><h3>文件管理</h3></article>
            </section>
            <section className="panel">
              <div className="panel-head">
                <h3>Get File（按 hash 拉取）</h3>
                <div className="filters">
                  <button className="btn btn-light" onClick={() => void loadGetFileJob()}>刷新进度</button>
                </div>
              </div>
              <div className="filters">
                <input className="input" placeholder="seed_hash" value={getFileSeedHash} onChange={(e) => setGetFileSeedHash(e.target.value)} />
                <input className="input small" type="number" min={1} value={getFileChunkCount} onChange={(e) => setGetFileChunkCount(e.target.value)} />
                <button className="btn" onClick={() => void startGetFileJob()}>启动拉取任务</button>
              </div>
              {getFileJob ? (
                <div className="job-box">
                  <div className="hint">job_id: {getFileJob.id} | status: {getFileJob.status} | gateway: {getFileJob.gateway_peer_id || "-"}</div>
                  {getFileJob.output_file_path ? <div className="hint">output: {getFileJob.output_file_path}</div> : null}
                  {getFileJob.error ? <div className="err-inline">error: {getFileJob.error}</div> : null}
                  <div className="timeline">
                    {getFileJob.steps.map((st) => (
                      <div key={`${getFileJob.id}-${st.index}`} className={`step ${st.status}`}>
                        <div className="step-head">
                          <strong>{st.index + 1}. {st.name}</strong>
                          <span>{st.status}</span>
                        </div>
                        <div className="hint">{t(st.started_at_unix)} {st.ended_at_unix ? `-> ${t(st.ended_at_unix)}` : ""}</div>
                        {st.detail ? <pre className="detail-pre">{JSON.stringify(st.detail, null, 2)}</pre> : null}
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="hint">输入 seed_hash + chunk_count 后启动任务</div>
              )}
            </section>
            <section className="panel"><h3>最近售卖摘要</h3><div className="table-wrap"><table><thead><tr><th>时间</th><th>Seed</th><th>Session</th><th>成交额</th></tr></thead><tbody>
              {sales.items.map((x) => <tr key={x.id}><td>{t(x.created_at_unix)}</td><td title={x.seed_hash}>{short(x.seed_hash, 10)}</td><td title={x.session_id}>{short(x.session_id, 8)}</td><td className="up">{sat(x.amount_satoshi)}</td></tr>)}
            </tbody></table></div></section>
          </>
        ) : null}

        {/* DEBUG seeds: path={route.path}, cond={(route.path === "/files/seeds" || route.path === "/files/pricing") && !!seeds} */}
        {(route.path === "/files/seeds" || route.path === "/files/pricing") && seeds ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const like = route.query.get("seed_hash_like") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>{route.path === "/files/pricing" ? "价格管理" : "种子列表"}</h3>
                <div className="filters">
                  <input className="input" defaultValue={like} placeholder="seed_hash_like" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ seed_hash_like: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>Seed</th><th>Chunk</th><th>文件大小</th><th>当前单价</th><th>底价</th><th>BPS</th>{route.path === "/files/pricing" ? <th>单独改价</th> : null}</tr></thead><tbody>
                {seeds.items.map((s) => {
                  const d = seedDrafts[s.seed_hash] ?? { floor: String(s.floor_unit_price_sat_per_64k || s.unit_price_sat_per_64k || 10), discount: String(s.resale_discount_bps || 8000) };
                  return <tr key={s.seed_hash}><td><button className="btn btn-ghost copy-btn" title="点击复制" onClick={() => { navigator.clipboard.writeText(s.seed_hash); alert("已复制: " + short(s.seed_hash, 16)); }}>{short(s.seed_hash, 12)}</button></td><td>{s.chunk_count}</td><td>{s.file_size.toLocaleString()} B</td><td>{sat(s.unit_price_sat_per_64k)}</td><td>{sat(s.floor_unit_price_sat_per_64k)}</td><td>{s.resale_discount_bps}</td>{route.path === "/files/pricing" ? <td><div className="filters"><input className="input small" type="number" value={d.floor} onChange={(e) => setSeedDraft(s, { floor: e.target.value })} /><input className="input small" type="number" value={d.discount} onChange={(e) => setSeedDraft(s, { discount: e.target.value })} /><button className="btn" disabled={!!d.saving} onClick={() => void saveSeedPrice(s)}>{d.saving ? "保存中" : "保存"}</button></div>{d.message ? <div className="hint">{d.message}</div> : null}</td> : null}</tr>;
                })}
              </tbody></table></div>
              <Pager total={seeds.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/files/sales" && sales ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const seed = route.query.get("seed_hash") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>售卖记录</h3>
                <div className="filters">
                  <input className="input" defaultValue={seed} placeholder="seed_hash" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ seed_hash: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>Seed</th><th>Session</th><th>Chunk</th><th>单价</th><th>成交额</th><th>买方网关</th></tr></thead><tbody>
                {sales.items.map((s) => (
                  <tr key={s.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === s.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: s.id })}>
                    <td>{t(s.created_at_unix)}</td>
                    <td title={s.seed_hash}>{short(s.seed_hash, 10)}</td>
                    <td title={s.session_id}>{short(s.session_id, 8)}</td>
                    <td>{s.chunk_index}</td>
                    <td>{sat(s.unit_price_sat_per_64k)}</td>
                    <td className="up">{sat(s.amount_satoshi)}</td>
                    <td title={s.buyer_gateway_peer_id}>{short(s.buyer_gateway_peer_id, 8)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={sales.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {saleDetail ? <pre className="detail-pre">{JSON.stringify(saleDetail, null, 2)}</pre> : <div className="hint">点击某条售卖记录查看详细字段</div>}
            </section>
          );
        })() : null}

        {route.path === "/files/index" && files ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const pathLike = route.query.get("path_like") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>文件索引</h3>
                <div className="filters">
                  <input className="input" defaultValue={pathLike} placeholder="path_like" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ path_like: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>路径</th><th>文件大小</th><th>Seed</th><th>更新时间</th></tr></thead><tbody>
                {files.items.map((f) => <tr key={f.path}><td title={f.path}>{f.path}</td><td>{f.file_size.toLocaleString()} B</td><td title={f.seed_hash}>{short(f.seed_hash, 10)}</td><td>{t(f.updated_at_unix)}</td></tr>)}
              </tbody></table></div>
              <Pager total={files.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {/* ========== Direct 交易模块页面 ========== */}
        {route.path === "/direct/quotes" && directQuotes ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          return (
            <section className="panel">
              <div className="panel-head"><h3>报价列表</h3></div>
              <div className="table-wrap"><table><thead><tr><th>ID</th><th>Seed</th><th>Chunks</th><th>报价</th><th>卖方</th><th>状态</th><th>过期时间</th></tr></thead><tbody>
                {directQuotes.items.map((q) => (
                  <tr key={q.id}>
                    <td title={q.id}>{short(q.id, 8)}</td>
                    <td title={q.seed_hash}>{short(q.seed_hash, 8)}</td>
                    <td>{q.chunk_count}</td>
                    <td className="up">{sat(q.quote_satoshi)}</td>
                    <td title={q.seller_peer_id}>{short(q.seller_peer_id, 6)}</td>
                    <td>{q.status}</td>
                    <td>{t(q.expires_at_unix)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={directQuotes.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/direct/deals" && directDeals ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          return (
            <section className="panel">
              <div className="panel-head"><h3>成交记录</h3></div>
              <div className="table-wrap"><table><thead><tr><th>ID</th><th>Quote ID</th><th>Seed</th><th>Chunks</th><th>金额</th><th>买方</th><th>卖方</th><th>状态</th><th>时间</th></tr></thead><tbody>
                {directDeals.items.map((d) => (
                  <tr key={d.id}>
                    <td title={d.id}>{short(d.id, 8)}</td>
                    <td title={d.quote_id}>{short(d.quote_id, 6)}</td>
                    <td title={d.seed_hash}>{short(d.seed_hash, 6)}</td>
                    <td>{d.chunk_count}</td>
                    <td className="up">{sat(d.amount_satoshi)}</td>
                    <td title={d.buyer_peer_id}>{short(d.buyer_peer_id, 6)}</td>
                    <td title={d.seller_peer_id}>{short(d.seller_peer_id, 6)}</td>
                    <td>{d.status}</td>
                    <td>{t(d.created_at_unix)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={directDeals.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/direct/sessions" && directSessions ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          return (
            <section className="panel">
              <div className="panel-head"><h3>会话管理</h3></div>
              <div className="table-wrap"><table><thead><tr><th>ID</th><th>Deal ID</th><th>Seed</th><th>Chunk</th><th>买方</th><th>卖方</th><th>状态</th><th>传输量</th><th>开始时间</th></tr></thead><tbody>
                {directSessions.items.map((s) => (
                  <tr key={s.id}>
                    <td title={s.id}>{short(s.id, 8)}</td>
                    <td title={s.deal_id}>{short(s.deal_id, 6)}</td>
                    <td title={s.seed_hash}>{short(s.seed_hash, 6)}</td>
                    <td>{s.chunk_index}</td>
                    <td title={s.buyer_peer_id}>{short(s.buyer_peer_id, 6)}</td>
                    <td title={s.seller_peer_id}>{short(s.seller_peer_id, 6)}</td>
                    <td>{s.status}</td>
                    <td>{formatBytes(s.bytes_transferred)}</td>
                    <td>{t(s.created_at_unix)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={directSessions.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/finance/transfer-pools" && directTransferPools ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          return (
            <section className="panel">
              <div className="panel-head"><h3>Direct资金池</h3></div>
              <div className="table-wrap"><table><thead><tr><th>Pool ID</th><th>买方</th><th>卖方</th><th>总额</th><th>已释放</th><th>状态</th><th>创建时间</th></tr></thead><tbody>
                {directTransferPools.items.map((p) => (
                  <tr key={p.pool_id}>
                    <td title={p.pool_id}>{short(p.pool_id, 12)}</td>
                    <td title={p.buyer_peer_id}>{short(p.buyer_peer_id, 8)}</td>
                    <td title={p.seller_peer_id}>{short(p.seller_peer_id, 8)}</td>
                    <td>{sat(p.total_satoshi)}</td>
                    <td>{sat(p.released_satoshi)}</td>
                    <td>{p.status}</td>
                    <td>{t(p.created_at_unix)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={directTransferPools.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {/* ========== Live 直播模块页面 ========== */}
        {route.path === "/live/streams" && liveStreams ? (
          <section className="panel">
            <div className="panel-head"><h3>直播流列表</h3></div>
            <div className="table-wrap"><table><thead><tr><th>Stream ID</th><th>文件数</th><th>总大小</th><th>最后更新</th><th>操作</th></tr></thead><tbody>
              {liveStreams.items.map((s) => (
                <tr key={s.stream_id}>
                  <td title={s.stream_id}>{shortHex(s.stream_id, 8)}</td>
                  <td>{s.file_count}</td>
                  <td>{formatBytes(s.total_bytes)}</td>
                  <td>{t(s.last_updated_unix)}</td>
                  <td>
                    <button className="btn btn-light" onClick={() => loadLiveStreamDetail(s.stream_id)}>详情</button>
                    <button className="btn btn-light" onClick={() => deleteLiveStream(s.stream_id).catch(e => alert(e.message))}>删除</button>
                  </td>
                </tr>
              ))}
            </tbody></table></div>
            {liveStreamDetail ? (
              <div className="panel" style={{ marginTop: 16 }}>
                <h4>Stream: {shortHex(liveStreamDetail.stream_id, 8)} 详情</h4>
                <div className="hint">文件数: {liveStreamDetail.file_count} | 总大小: {formatBytes(liveStreamDetail.total_bytes)}</div>
                <div className="table-wrap"><table><thead><tr><th>路径</th><th>大小</th><th>修改时间</th></tr></thead><tbody>
                  {liveStreamDetail.segment_entries.slice(0, 20).map((e, i) => (
                    <tr key={i}>
                      <td title={e.path}>{short(e.path, 40)}</td>
                      <td>{formatBytes(e.file_size)}</td>
                      <td>{t(e.mtime_unix)}</td>
                    </tr>
                  ))}
                </tbody></table></div>
                {liveStreamDetail.segment_entries.length > 20 && <div className="hint">...还有 {liveStreamDetail.segment_entries.length - 20} 个文件</div>}
              </div>
            ) : null}
          </section>
        ) : null}

        {route.path === "/live/follow" && liveFollowStatus ? (
          <section className="panel">
            <div className="panel-head"><h3>我的关注</h3></div>
            <div className="stats-grid">
              <article className="stat">
                <p>状态</p>
                <h3>{liveFollowStatus.following ? "✅ 正在关注" : "⛔ 未关注"}</h3>
              </article>
              {liveFollowStatus.following && (
                <>
                  <article className="stat"><p>Stream ID</p><h3>{shortHex(liveFollowStatus.stream_id || "", 8)}</h3></article>
                  <article className="stat"><p>开始时间</p><h3>{t(liveFollowStatus.started_at_unix || 0)}</h3></article>
                  <article className="stat"><p>已接收</p><h3>{formatBytes(liveFollowStatus.bytes_received || 0)}</h3></article>
                </>
              )}
            </div>
          </section>
        ) : null}

        {route.path === "/live/storage" && liveStorageSummary ? (
          <section className="panel">
            <div className="panel-head"><h3>Live 存储概览</h3></div>
            <div className="stats-grid">
              <article className="stat"><p>直播流数量</p><h3>{liveStorageSummary.stream_count}</h3></article>
              <article className="stat"><p>文件总数</p><h3>{liveStorageSummary.file_count.toLocaleString()}</h3></article>
              <article className="stat"><p>占用空间</p><h3>{formatBytes(liveStorageSummary.total_bytes)}</h3></article>
            </div>
          </section>
        ) : null}

        {/* ========== Admin 管理模块页面 ========== */}
        {route.path === "/admin/downloads" ? (
          <section className="panel">
            <div className="panel-head"><h3>下载管理</h3></div>
            <div className="hint">此功能需要后端 API 支持恢复下载。当前暂无专用 API。</div>
            <div className="filters" style={{ marginTop: 16 }}>
              <input className="input" placeholder="demand_id" id="resumeDemandId" />
              <button className="btn" onClick={() => {
                const demandId = (document.getElementById("resumeDemandId") as HTMLInputElement).value;
                if (demandId) resumeDownload(demandId);
              }}>恢复下载</button>
            </div>
          </section>
        ) : null}

        {route.path === "/admin/live" && liveStreams ? (
          <section className="panel">
            <div className="panel-head"><h3>Live 流管理</h3></div>
            <div className="table-wrap"><table><thead><tr><th>Stream ID</th><th>文件数</th><th>总大小</th><th>最后更新</th><th>操作</th></tr></thead><tbody>
              {liveStreams.items.map((s) => (
                <tr key={s.stream_id}>
                  <td title={s.stream_id}>{shortHex(s.stream_id, 8)}</td>
                  <td>{s.file_count}</td>
                  <td>{formatBytes(s.total_bytes)}</td>
                  <td>{t(s.last_updated_unix)}</td>
                  <td>
                    <button className="btn btn-light" onClick={() => loadLiveStreamDetail(s.stream_id)}>详情</button>
                    <button className="btn btn-light" onClick={() => deleteLiveStream(s.stream_id).catch(e => alert(e.message))}>删除</button>
                  </td>
                </tr>
              ))}
            </tbody></table></div>
          </section>
        ) : null}

        {/* 静态文件管理 - 资源管理器样式 */}
        {route.path === "/admin/static" && <StaticFileManager 
          staticTree={staticTree} 
          staticPathHistory={staticPathHistory}
          staticCurrentPath={staticCurrentPath}
          selectedStaticItems={selectedStaticItems}
          onNavigate={navigateToStaticPath}
          onNavigateBack={navigateBack}
          onRefresh={() => loadStaticTree(staticCurrentPath)}
          onCreateDir={createStaticDir}
          onDelete={deleteStaticEntry}
          onSetPrice={setStaticItemPrice}
          onToggleSelection={toggleStaticItemSelection}
          onUpload={uploadStaticFile}
        />}

        {/* ========== Orchestrator 调度页面 ========== */}
        {route.path === "/admin/orchestrator" && orchestratorEvents ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const eventType = route.query.get("event_type") || "";
          const signalType = route.query.get("signal_type") || "";
          const source = route.query.get("source") || "";
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const taskStatus = route.query.get("task_status") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>调度器 (Orchestrator)</h3>
                <div className="hint">任务调度与信号处理中心</div>
              </div>
              
              {/* 状态概览 */}
              {orchestratorStatus && (
                <div className="stats-grid" style={{ marginBottom: 16 }}>
                  <article className="stat">
                    <p>状态</p>
                    <h3>{orchestratorStatus.enabled ? "✅ 运行中" : "⛔ 未启用"}</h3>
                  </article>
                  <article className="stat">
                    <p>活跃信号</p>
                    <h3>{orchestratorStatus.active_signals ?? "-"}</h3>
                  </article>
                  <article className="stat">
                    <p>待处理任务</p>
                    <h3>{orchestratorStatus.pending_tasks ?? "-"}</h3>
                  </article>
                </div>
              )}

              <div className="filters">
                <input className="input" defaultValue={eventType} placeholder="event_type" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ event_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={signalType} placeholder="signal_type" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ signal_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={source} placeholder="source" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ source: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={gatewayPeerID} placeholder="网关 PeerID" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={taskStatus} placeholder="task_status" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ task_status: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
              </div>
              <div className="table-wrap"><table><thead><tr><th>开始时间</th><th>结束时间</th><th>步骤数</th><th>最新事件</th><th>命令类型</th><th>任务状态</th><th>来源</th><th>信号</th><th>操作</th></tr></thead><tbody>
                {orchestratorEvents.items.map((evt) => (
                  <tr key={evt.event_id}>
                    <td>{t(evt.started_at_unix)}</td>
                    <td>{t(evt.ended_at_unix)}</td>
                    <td>{evt.steps_count}</td>
                    <td>{evt.latest_event_type || "-"}</td>
                    <td>{evt.command_type || "-"}</td>
                    <td>{evt.latest_task_status || "-"}</td>
                    <td title={evt.source}>{short(evt.source, 12)}</td>
                    <td title={evt.signal_type}>{short(evt.signal_type, 10)}</td>
                    <td>
                      <button className="btn btn-light" onClick={() => loadOrchestratorLogDetail(evt.event_id)}>详情</button>
                    </td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={orchestratorEvents.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

              {/* Orchestrator 日志详情弹窗 */}
              <Modal
                title={orchestratorEventDetail ? `调度事件详情: ${orchestratorEventDetail.event_id}` : "调度事件详情"}
                isOpen={orchestratorModalOpen}
                onClose={() => setOrchestratorModalOpen(false)}
              >
                {orchestratorEventDetail && (
                  <div style={{ display: "grid", gap: 12 }}>
                    <div className="detail-table-wrap">
                      <DetailTable data={{
                        event_id: orchestratorEventDetail.event_id,
                        idempotency_key: orchestratorEventDetail.idempotency_key,
                        aggregate_key: orchestratorEventDetail.aggregate_key,
                        command_type: orchestratorEventDetail.command_type,
                        gateway_peer_id: orchestratorEventDetail.gateway_peer_id,
                        started_at: t(orchestratorEventDetail.started_at_unix),
                        ended_at: t(orchestratorEventDetail.ended_at_unix),
                        steps_count: orchestratorEventDetail.steps_count,
                        latest_event_type: orchestratorEventDetail.latest_event_type,
                        latest_task_status: orchestratorEventDetail.latest_task_status,
                        last_error_message: orchestratorEventDetail.last_error_message || "-"
                      }} />
                    </div>
                    <div className="table-wrap"><table><thead><tr><th>时间</th><th>事件</th><th>来源</th><th>信号</th><th>命令</th><th>状态</th><th>重试</th><th>队列</th><th>错误</th></tr></thead><tbody>
                      {orchestratorEventDetail.steps.map((step) => (
                        <tr key={step.id}>
                          <td>{t(step.created_at_unix)}</td>
                          <td>{step.event_type}</td>
                          <td title={step.source}>{short(step.source, 12)}</td>
                          <td title={step.signal_type}>{short(step.signal_type, 10)}</td>
                          <td>{step.command_type || "-"}</td>
                          <td>{step.task_status || "-"}</td>
                          <td>{step.retry_count}</td>
                          <td>{step.queue_length}</td>
                          <td title={step.error_message}>{step.error_message ? short(step.error_message, 16) : "-"}</td>
                        </tr>
                      ))}
                    </tbody></table></div>
                  </div>
                )}
                <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                  <button className="btn btn-light" onClick={() => setOrchestratorModalOpen(false)}>关闭</button>
                </div>
              </Modal>
            </section>
          );
        })() : null}

        {/* ========== ClientKernel 命令页面 ========== */}
        {route.path === "/admin/client-kernel" && clientKernelCommands ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const commandType = route.query.get("command_type") || "";
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const status = route.query.get("status") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>Client Kernel 命令</h3>
                <div className="hint">客户端内核命令调度记录（费用池、直播、工作区、下载等）</div>
              </div>
              <div className="filters">
                <input className="input" defaultValue={commandType} placeholder="命令类型" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ command_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={gatewayPeerID} placeholder="网关 PeerID" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={status} placeholder="状态" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>命令ID</th><th>类型</th><th>网关</th><th>聚合ID</th><th>状态</th><th>耗时</th><th>操作</th></tr></thead><tbody>
                {clientKernelCommands.items.map((cmd) => (
                  <tr key={cmd.id}>
                    <td>{t(cmd.created_at_unix)}</td>
                    <td title={cmd.command_id}>{short(cmd.command_id, 8)}</td>
                    <td>{cmd.command_type}</td>
                    <td title={cmd.gateway_peer_id}>{short(cmd.gateway_peer_id, 8)}</td>
                    <td title={cmd.aggregate_id}>{short(cmd.aggregate_id, 16)}</td>
                    <td>{cmd.status}</td>
                    <td>{cmd.duration_ms}ms</td>
                    <td>
                      <button className="btn btn-light" onClick={() => loadClientKernelCommandDetail(cmd.id)}>详情</button>
                    </td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={clientKernelCommands.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

              {/* Client Kernel 命令详情弹窗 */}
              <Modal
                title={clientKernelCommandDetail ? `命令详情: ${short(clientKernelCommandDetail.command_id, 12)}` : "命令详情"}
                isOpen={clientKernelModalOpen}
                onClose={() => setClientKernelModalOpen(false)}
              >
                {clientKernelCommandDetail && (
                  <div className="detail-table-wrap">
                    <DetailTable data={clientKernelCommandDetail} />
                  </div>
                )}
                <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                  <button className="btn btn-light" onClick={() => setClientKernelModalOpen(false)}>关闭</button>
                </div>
              </Modal>
            </section>
          );
        })() : null}

        {/* ========== FeePool 审计页面 ========== */}
        {route.path === "/admin/feepool" && feePoolCommands ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const commandType = route.query.get("command_type") || "";
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const status = route.query.get("status") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>费用池审计</h3>
                <div className="hint">命令 → 事件 → 状态 → 效果 完整追踪</div>
              </div>
              <div className="filters">
                <input className="input" defaultValue={commandType} placeholder="命令类型" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ command_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={gatewayPeerID} placeholder="网关 PeerID" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <input className="input" defaultValue={status} placeholder="状态" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 }); }} />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>命令ID</th><th>类型</th><th>网关</th><th>状态</th><th>耗时</th><th>操作</th></tr></thead><tbody>
                {feePoolCommands.items.map((cmd) => (
                  <tr key={cmd.id}>
                    <td>{t(cmd.created_at_unix)}</td>
                    <td title={cmd.command_id}>{short(cmd.command_id, 8)}</td>
                    <td>{cmd.command_type}</td>
                    <td title={cmd.gateway_peer_id}>{short(cmd.gateway_peer_id, 8)}</td>
                    <td>{cmd.status}</td>
                    <td>{cmd.duration_ms}ms</td>
                    <td>
                      <button className="btn btn-light" onClick={() => loadFeePoolCommandDetail(cmd.id)}>审计追踪</button>
                    </td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={feePoolCommands.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

              {/* FeePool 审计追踪弹窗 */}
              <Modal
                title={feePoolCommandDetail ? `审计追踪: ${short(feePoolCommandDetail.command_id, 12)}` : "审计追踪"}
                isOpen={feePoolAuditModalOpen}
                onClose={() => setFeePoolAuditModalOpen(false)}
              >
                {feePoolCommandDetail && (
                  <>
                    <div className="tabs" style={{ display: "flex", gap: 4, marginBottom: 16, borderBottom: "1px solid #e5e7eb" }}>
                      {(["command", "events", "states", "effects"] as const).map((tab) => (
                        <button
                          key={tab}
                          className={feePoolAuditTab === tab ? "btn" : "btn btn-light"}
                          style={{ borderRadius: "4px 4px 0 0", marginBottom: -1 }}
                          onClick={() => setFeePoolAuditTab(tab)}
                        >
                          {tab === "command" ? "命令详情" : tab === "events" ? `领域事件 (${feePoolEvents?.total || 0})` : tab === "states" ? `状态快照 (${feePoolStates?.total || 0})` : `效果日志 (${feePoolEffects?.total || 0})`}
                        </button>
                      ))}
                    </div>
                    <div className="detail-table-wrap">
                      {feePoolAuditTab === "command" && <DetailTable data={feePoolCommandDetail} />}
                      {feePoolAuditTab === "events" && feePoolEvents && (
                        <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                          <table><thead><tr><th>时间</th><th>事件名</th><th>状态变更</th></tr></thead><tbody>
                            {feePoolEvents.items.map((e) => (
                              <tr key={e.id}><td>{t(e.created_at_unix)}</td><td>{e.event_name}</td><td>{e.state_before} → {e.state_after}</td></tr>
                            ))}
                          </tbody></table>
                        </div>
                      )}
                      {feePoolAuditTab === "states" && feePoolStates && (
                        <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                          <table><thead><tr><th>时间</th><th>状态</th><th>暂停原因</th><th>需求/拥有</th></tr></thead><tbody>
                            {feePoolStates.items.map((s) => (
                              <tr key={s.id}><td>{t(s.created_at_unix)}</td><td>{s.state}</td><td>{s.pause_reason || "-"}</td><td>{s.pause_need_satoshi} / {s.pause_have_satoshi}</td></tr>
                            ))}
                          </tbody></table>
                        </div>
                      )}
                      {feePoolAuditTab === "effects" && feePoolEffects && (
                        <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                          <table><thead><tr><th>时间</th><th>效果类型</th><th>阶段</th><th>状态</th></tr></thead><tbody>
                            {feePoolEffects.items.map((e) => (
                              <tr key={e.id}><td>{t(e.created_at_unix)}</td><td>{e.effect_type}</td><td>{e.stage}</td><td>{e.status}</td></tr>
                            ))}
                          </tbody></table>
                        </div>
                      )}
                    </div>
                  </>
                )}
                <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                  <button className="btn btn-light" onClick={() => setFeePoolAuditModalOpen(false)}>关闭</button>
                </div>
              </Modal>
            </section>
          );
        })() : null}

        {/* ========== 工作区管理页面 ========== */}
        {route.path === "/admin/workspaces" && workspaces ? (
          <section className="panel">
            <div className="panel-head">
              <h3>工作区管理</h3>
              <button className="btn" onClick={() => setWorkspaceEditing({ id: null, data: { path: "", max_bytes: 10737418240, enabled: true } })}>添加工作区</button>
            </div>
            <div className="table-wrap"><table><thead><tr><th>ID</th><th>路径</th><th>容量上限</th><th>已用空间</th><th>状态</th><th>操作</th></tr></thead><tbody>
              {workspaces.items.map((w) => (
                <tr key={w.id}>
                  <td>{w.id}</td>
                  <td title={w.path}>{w.path}</td>
                  <td>{formatBytes(w.max_bytes)}</td>
                  <td>{formatBytes(w.used_bytes)}</td>
                  <td>{w.enabled ? "✅ 启用" : "⛔ 禁用"}</td>
                  <td>
                    <button className="btn btn-light" onClick={() => setWorkspaceEditing({ id: w.id, data: { ...w } })}>编辑</button>
                    {!w.enabled && <button className="btn btn-light" onClick={() => deleteWorkspace(w.id).catch(e => alert(e.message))}>删除</button>}
                  </td>
                </tr>
              ))}
            </tbody></table></div>

            {/* 工作区编辑弹窗 */}
            <Modal
              title={workspaceEditing?.id === null ? "添加工作区" : "编辑工作区"}
              isOpen={!!workspaceEditing}
              onClose={() => setWorkspaceEditing(null)}
            >
              {workspaceEditing && (
                <>
                  <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                    <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                      <input type="checkbox" checked={workspaceEditing.data.enabled} onChange={(e) => setWorkspaceEditing({ ...workspaceEditing, data: { ...workspaceEditing.data, enabled: e.target.checked } })} />
                      <span>启用</span>
                    </label>
                    <input className="input" placeholder="路径" value={workspaceEditing.data.path || ""} onChange={(e) => setWorkspaceEditing({ ...workspaceEditing, data: { ...workspaceEditing.data, path: e.target.value } })} />
                    <input className="input" type="number" placeholder="容量上限 (bytes)" value={workspaceEditing.data.max_bytes || ""} onChange={(e) => setWorkspaceEditing({ ...workspaceEditing, data: { ...workspaceEditing.data, max_bytes: Number(e.target.value) } })} />
                  </div>
                  <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                    <button className="btn" onClick={() => {
                      saveWorkspace(workspaceEditing.id, workspaceEditing.data).then(() => setWorkspaceEditing(null)).catch(e => alert(e.message));
                    }}>保存</button>
                    <button className="btn btn-light" onClick={() => setWorkspaceEditing(null)}>取消</button>
                  </div>
                </>
              )}
            </Modal>
          </section>
        ) : null}

        {/* 系统配置页面 */}
        {route.path === "/admin/config" && adminConfig && adminConfigSchema ? (() => {
          return (
            <section className="panel">
              <div className="panel-head"><h3>系统配置</h3></div>
              <div className="table-wrap"><table><thead><tr><th>配置项</th><th>当前值</th><th>类型</th><th>范围/说明</th><th>操作</th></tr></thead><tbody>
                {adminConfigSchema.items.map((schema) => {
                  const currentValue = adminConfig[schema.key];
                  const draftValue = configDrafts[schema.key] ?? String(currentValue ?? "");
                  return (
                    <tr key={schema.key}>
                      <td title={schema.key}><code>{schema.key}</code></td>
                      <td><code>{String(currentValue)}</code></td>
                      <td>{schema.type}</td>
                      <td>
                        {schema.type === "int" && schema.min_int !== undefined && schema.max_int !== undefined ? `${schema.min_int} ~ ${schema.max_int}` : 
                          schema.type === "float" && schema.min_float !== undefined && schema.max_float !== undefined ? `${schema.min_float} ~ ${schema.max_float}` :
                            schema.description || "-"}
                      </td>
                      <td>
                        <div className="filters">
                          <input className="input small" value={draftValue} onChange={(e) => setConfigDrafts({ ...configDrafts, [schema.key]: e.target.value })} />
                          <button className="btn btn-light" onClick={() => {
                            let value: unknown = draftValue;
                            if (schema.type === "int") value = Number(draftValue);
                            else if (schema.type === "float") value = Number(draftValue);
                            else if (schema.type === "bool") value = draftValue === "true";
                            saveAdminConfig(schema.key, value);
                          }}>保存</button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody></table></div>
              {/* 策略调试日志开关 */}
              <div className="panel" style={{ marginTop: 16 }}>
                <div className="panel-head"><h4>调试设置</h4></div>
                <div style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 0" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                    <input 
                      type="checkbox" 
                      checked={strategyDebugLogEnabled ?? false} 
                      disabled={strategyDebugLogLoading}
                      onChange={(e) => setStrategyDebugLog(e.target.checked)}
                    />
                    <span>策略调试日志 (strategy_debug_log_enabled)</span>
                  </label>
                  {strategyDebugLogLoading && <span className="hint">保存中...</span>}
                </div>
                <div className="hint">开启后将记录文件下载策略的详细调试信息，用于排查下载问题。</div>
              </div>
            </section>
          );
        })() : null}
      </main>
    </div>
  );
}
