import React, { useEffect, useState } from "react";

type AuthState = "locked" | "checking" | "ready" | "error";

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

type AdminLiveStorageSummary = {
  stream_count: number;
  file_count: number;
  total_bytes: number;
};

type BootstrapStatusResp = {
  needs_bootstrap: boolean;
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
  onUpload,
  token
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
  onUpload: (file: File, targetDir: string, overwrite: boolean) => Promise<void>,
  token: string
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
  const [auth, setAuth] = useState<AuthState>("locked");
  const [authErr, setAuthErr] = useState("");
  const [tokenInput, setTokenInput] = useState("");
  const [token, setToken] = useState("");
  const [needsBootstrap, setNeedsBootstrap] = useState<boolean | null>(null);
  const [bootstrapChecking, setBootstrapChecking] = useState(true);

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

  // 静态文件管理状态
  const [staticTree, setStaticTree] = useState<StaticTreeResp | null>(null);
  const [staticPathHistory, setStaticPathHistory] = useState<string[]>(["/"]);
  const [staticCurrentPath, setStaticCurrentPath] = useState("/");
  const [selectedStaticItems, setSelectedStaticItems] = useState<Set<string>>(new Set());

  // 文件获取任务状态
  const [getFileJob, setGetFileJob] = useState<FileGetJob | null>(null);
  const [getFileSeedHash, setGetFileSeedHash] = useState("");
  const [getFileChunkCount, setGetFileChunkCount] = useState("1");

  useEffect(() => {
    const onHash = () => setRoute(nowPath());
    window.addEventListener("hashchange", onHash);
    if (!window.location.hash) setHash("/wallet");
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const api = async <T,>(path: string, method = "GET", tokenOverride?: string, body?: unknown): Promise<T> => {
    const tk = (tokenOverride ?? token).trim();
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (tk) headers.Authorization = `Bearer ${tk}`;
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
    const [txData, summaryData] = await Promise.all([
      api<TxResp>("api/v1/transactions?limit=5&offset=0"),
      api<WalletSummaryResp>("api/v1/wallet/summary")
    ]);
    setTx(txData);
    setWalletSummary(summaryData);
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
      await api("api/v1/gateways", "POST", undefined, data);
    } else {
      await api(`api/v1/gateways?id=${id}`, "PUT", undefined, data);
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
      await api("api/v1/arbiters", "POST", undefined, data);
    } else {
      await api(`api/v1/arbiters?id=${id}`, "PUT", undefined, data);
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
      await api("api/v1/admin/config", "POST", undefined, {
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
      await api("api/v1/admin/downloads/resume", "POST", undefined, { demand_id: demandId });
      alert("已触发恢复下载");
    } catch (e) {
      alert(e instanceof Error ? e.message : "恢复失败");
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
    await api("api/v1/admin/static/mkdir", "POST", undefined, { path: newPath });
    await loadStaticTree(staticCurrentPath);
  };

  const moveStaticItem = async (fromPath: string, toPath: string) => {
    await api("api/v1/admin/static/move", "POST", undefined, { from: fromPath, to: toPath });
    await loadStaticTree(staticCurrentPath);
  };

  const deleteStaticEntry = async (path: string) => {
    if (!confirm(`确定删除 ${path} 吗？`)) return;
    await api("api/v1/admin/static/entry", "DELETE", undefined, { path });
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
    await api("api/v1/admin/static/price/set", "POST", undefined, {
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
      headers: { Authorization: `Bearer ${token}` },
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
    const out = await api<{ job_id: string }>("api/v1/files/get-file", "POST", undefined, {
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
      await api("api/v1/workspace/seeds/price", "POST", undefined, {
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
    if (auth !== "ready") return;
    const run = async () => {
      setBusy(true);
      setErr("");
      try {
        switch (route.path) {
          // 钱包模块
          case "/wallet":
            await loadWalletSummary();
            break;
          case "/wallet/flows":
            await loadWalletFlows();
            break;
          case "/wallet/ledger":
            await loadWalletLedger();
            break;
          case "/wallet/gateway-events":
            await loadWalletGatewayEvents();
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
          case "/direct/transfer-pools":
            await loadDirectTransferPools();
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
          case "/admin/downloads":
            // 暂无专门 API，仅保留入口
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
            await Promise.all([loadAdminConfig(), loadAdminConfigSchema()]);
            break;
          default:
            setHash("/wallet");
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
    if (auth !== "ready") return;
    if (route.path !== "/files") return;
    const jobID = route.query.get("job_id");
    if (!jobID) return;
    const timer = window.setInterval(() => {
      void loadGetFileJob();
    }, 2000);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth, route.path, route.query.get("job_id")]);

  // 登录相关
  useEffect(() => {
    const initAuth = async () => {
      const saved = localStorage.getItem("bitcast_api_token") ?? "";
      setTokenInput(saved);
      setBootstrapChecking(true);
      try {
        const status = await api<BootstrapStatusResp>("api/v1/bootstrap/status", "GET", "");
        const bootstrapNeeded = !!status.needs_bootstrap;
        setNeedsBootstrap(bootstrapNeeded);
        if (!bootstrapNeeded && saved.trim()) {
          await login(saved.trim(), true);
        }
      } catch (e) {
        setAuth("error");
        setAuthErr(e instanceof Error ? e.message : "初始化状态检查失败");
      } finally {
        setBootstrapChecking(false);
      }
    };
    void initAuth();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const login = async (raw: string, silent = false) => {
    const tk = raw.trim();
    if (!tk) {
      setAuth("error");
      setAuthErr("请输入 token");
      return;
    }
    setAuth("checking");
    setAuthErr("");
    try {
      await api("api/v1/info", "GET", tk);
      localStorage.setItem("bitcast_api_token", tk);
      setToken(tk);
      setNeedsBootstrap(false);
      setAuth("ready");
    } catch (e) {
      localStorage.removeItem("bitcast_api_token");
      setToken("");
      const msg = e instanceof Error ? e.message : "登录失败";
      if (msg.includes("token is not initialized")) {
        setNeedsBootstrap(true);
      }
      setAuth("error");
      setAuthErr(msg);
      if (silent) setTokenInput("");
    }
  };

  const bootstrapToken = async (raw: string) => {
    const tk = raw.trim();
    if (!tk) {
      setAuth("error");
      setAuthErr("请输入 token");
      return;
    }
    setAuth("checking");
    setAuthErr("");
    try {
      await api("api/v1/bootstrap/token", "POST", "", { auth_token: tk });
      await api("api/v1/info", "GET", tk);
      localStorage.setItem("bitcast_api_token", tk);
      setToken(tk);
      setNeedsBootstrap(false);
      setAuth("ready");
    } catch (e) {
      localStorage.removeItem("bitcast_api_token");
      setToken("");
      setAuth("error");
      setAuthErr(e instanceof Error ? e.message : "初始化 token 失败");
    }
  };

  const logout = () => {
    localStorage.removeItem("bitcast_api_token");
    setToken("");
    setTokenInput("");
    setAuth("locked");
    setAuthErr("");
    // 清空所有状态
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
  };

  // 计算当前模块
  const moduleName = route.path.startsWith("/files") ? "files" :
    route.path.startsWith("/settings") ? "settings" :
      route.path.startsWith("/direct") ? "direct" :
        route.path.startsWith("/live") ? "live" :
          route.path.startsWith("/admin") ? "admin" : "wallet";

  const routeQueryText = route.query.toString();
  const routeMeta = routeQueryText ? `?${route.query.size} params` : "";

  // 登录页
  if (auth !== "ready") {
    const bootstrapMode = needsBootstrap === true;
    const title = bootstrapMode ? "BitFS 客户端首次初始化" : "BitFS 客户端管理后台";
    const desc = bootstrapMode ? "当前系统未设置管理 token，请先初始化后再进入控制台。" : "登录后可按模块进入子页面。URL 可刷新复现。";
    const actionLabel = auth === "checking" ? (bootstrapMode ? "初始化中" : "验证中") : (bootstrapMode ? "初始化并登录" : "登录");
    return (
      <div className="login-shell">
        <div className="login-card">
          <p className="eyebrow">Bitcast Client</p>
          <h1>{title}</h1>
          <p className="desc">{desc}</p>
          {bootstrapChecking ? <p className="desc">检查初始化状态中...</p> : null}
          <div className="login-row">
            <input className="input" type="password" value={tokenInput} onChange={(e) => setTokenInput(e.target.value)} placeholder="API Token" />
            <button
              className="btn"
              onClick={() => void (bootstrapMode ? bootstrapToken(tokenInput) : login(tokenInput))}
              disabled={auth === "checking" || bootstrapChecking}
            >
              {actionLabel}
            </button>
          </div>
          {auth === "error" ? <p className="err">{authErr}</p> : null}
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

        {/* 钱包模块 */}
        <div className="menu-group">
          <h4>💰 钱包</h4>
          <button className={route.path === "/wallet" ? "menu active" : "menu"} onClick={() => setHash("/wallet")}>模块首页</button>
          <button className={route.path === "/wallet/flows" ? "menu active" : "menu"} onClick={() => setHash("/wallet/flows", new URLSearchParams("page=1&pageSize=20"))}>费用池流水</button>
          <button className={route.path === "/wallet/ledger" ? "menu active" : "menu"} onClick={() => setHash("/wallet/ledger", new URLSearchParams("page=1&pageSize=20"))}>链上流水</button>
          <button className={route.path === "/wallet/gateway-events" ? "menu active" : "menu"} onClick={() => setHash("/wallet/gateway-events", new URLSearchParams("page=1&pageSize=20"))}>网关事件</button>
        </div>

        {/* Direct 模块 */}
        <div className="menu-group">
          <h4>🔗 Direct 交易</h4>
          <button className={route.path === "/direct/quotes" ? "menu active" : "menu"} onClick={() => setHash("/direct/quotes", new URLSearchParams("page=1&pageSize=20"))}>报价列表</button>
          <button className={route.path === "/direct/deals" ? "menu active" : "menu"} onClick={() => setHash("/direct/deals", new URLSearchParams("page=1&pageSize=20"))}>成交记录</button>
          <button className={route.path === "/direct/sessions" ? "menu active" : "menu"} onClick={() => setHash("/direct/sessions", new URLSearchParams("page=1&pageSize=20"))}>会话管理</button>
          <button className={route.path === "/direct/transfer-pools" ? "menu active" : "menu"} onClick={() => setHash("/direct/transfer-pools", new URLSearchParams("page=1&pageSize=20"))}>资金池</button>
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
          <button className={route.path === "/admin/downloads" ? "menu active" : "menu"} onClick={() => setHash("/admin/downloads")}>下载管理</button>
          <button className={route.path === "/admin/live" ? "menu active" : "menu"} onClick={() => setHash("/admin/live")}>Live 管理</button>
          <button className={route.path === "/admin/static" ? "menu active" : "menu"} onClick={() => setHash("/admin/static")}>静态文件</button>
          <button className={route.path === "/admin/config" ? "menu active" : "menu"} onClick={() => setHash("/admin/config")}>系统配置</button>
        </div>

        {/* 设置模块 */}
        <div className="menu-group">
          <h4>🔧 设置</h4>
          <button className={route.path === "/settings/gateways" ? "menu active" : "menu"} onClick={() => setHash("/settings/gateways")}>网关管理</button>
          <button className={route.path === "/settings/arbiters" ? "menu active" : "menu"} onClick={() => setHash("/settings/arbiters")}>仲裁服务器</button>
        </div>

        <button className="btn btn-ghost" onClick={logout}>登出</button>
      </aside>

      <main className="content">
        <header className="top">
          <h1>
            {moduleName === "wallet" ? "💰 钱包模块" :
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

        {/* ========== 钱包模块页面 ========== */}
        {route.path === "/wallet" && tx ? (
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

        {route.path === "/wallet/flows" && tx ? (() => {
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

        {route.path === "/wallet/ledger" && walletLedger ? (() => {
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

        {route.path === "/wallet/gateway-events" && gatewayEvents ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const action = route.query.get("action") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>网关事件历史</h3>
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

        {route.path === "/direct/transfer-pools" && directTransferPools ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          return (
            <section className="panel">
              <div className="panel-head"><h3>资金池</h3></div>
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
          token={token}
        />}

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
            </section>
          );
        })() : null}
      </main>
    </div>
  );
}
