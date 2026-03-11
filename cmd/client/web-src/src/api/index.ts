/**
 * API 调用封装模块
 * 
 * 基于私钥解锁的新认证模型:
 * - 无需 token，通过私钥是否解锁来控制 API 访问
 * - 未解锁时调用非白名单 API 会返回 423 Locked
 * - 白名单 API 包括密钥相关 API (key/status, key/unlock 等)
 */

import type {
  // 密钥管理类型
  KeyStatusResp,
  KeyExportResp,
  // 网关管理类型
  Gateway,
  GatewaysResp,
  // 仲裁者管理类型
  Arbiter,
  ArbitersResp,
  // 工作区管理类型
  Workspace,
  WorkspacesResp,
  // 静态文件管理类型
  StaticTreeResp,
  // 钱包相关类型
  TxResp,
  WalletSummaryResp,
  WalletLedgerResp,
  GatewayEventsResp,
  DirectTransferPoolsResp,
  // 文件相关类型
  SeedsResp,
  SalesResp,
  FilesResp,
  // Direct 交易类型
  DirectQuotesResp,
  DirectDealsResp,
  DirectSessionsResp,
  // Live 流类型
  LiveStreamsResp,
  LiveStreamDetail,
  LiveFollowStatus,
  AdminLiveStorageSummary,
  // Admin 管理类型
  AdminConfig,
  AdminConfigSchema,
  // Orchestrator 类型
  OrchestratorEventsResp,
  OrchestratorEventDetail,
  OrchestratorStatus,
  // ClientKernel 类型
  ClientKernelCommandsResp,
  ClientKernelCommand,
  // FeePool 类型
  FeePoolCommandsResp,
  FeePoolCommand,
  FeePoolEventsResp,
  FeePoolStatesResp,
  FeePoolEffectsResp,
  // 文件获取任务类型
  FileGetJob,
} from "../types";

/**
 * 基础 API 调用函数
 * 
 * 使用 fetch 发送 HTTP 请求，自动处理 JSON 序列化和错误处理
 * 
 * @param path - API 路径（相对路径，如 "api/v1/key/status"）
 * @param method - HTTP 方法，默认 GET
 * @param body - 请求体对象（JSON 序列化）
 * @returns Promise<T> - 解析后的 JSON 响应
 * @throws Error - 当响应状态码非 2xx 时抛出错误
 */
export async function api<T>(path: string, method = "GET", body?: unknown): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const resp = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const text = await resp.text();
  let parsed: any = text;
  try {
    parsed = JSON.parse(text);
  } catch {
    // 保留原始文本（非 JSON 响应）
  }
  if (!resp.ok) {
    throw new Error(typeof parsed === "string" ? parsed : JSON.stringify(parsed, null, 2));
  }
  return parsed as T;
}

// ========== 密钥管理 API ==========

/**
 * 获取密钥状态
 * GET /api/v1/key/status
 * 
 * 检查系统中是否存在私钥以及是否已解锁
 * 此 API 在白名单中，未解锁时也可调用
 */
export function getKeyStatus(): Promise<KeyStatusResp> {
  return api<KeyStatusResp>("api/v1/key/status");
}

/**
 * 创建新私钥
 * POST /api/v1/key/new
 * 
 * @param password - 用于加密私钥的密码（至少 8 位）
 */
export function createKey(password: string): Promise<void> {
  return api<void>("api/v1/key/new", "POST", { password });
}

/**
 * 导入私钥
 * POST /api/v1/key/import
 * 
 * @param cipher - 加密私钥的密文对象
 */
export function importKey(cipher: Record<string, unknown>): Promise<void> {
  return api<void>("api/v1/key/import", "POST", { cipher });
}

/**
 * 导出私钥
 * GET /api/v1/key/export
 * 
 * 返回加密后的私钥密文，需要系统已解锁
 * 注意：导出后请妥善保管密文
 */
export function exportKey(): Promise<KeyExportResp> {
  return api<KeyExportResp>("api/v1/key/export");
}

/**
 * 解锁私钥
 * POST /api/v1/key/unlock
 * 
 * @param password - 用于解密私钥的密码
 */
export function unlockKey(password: string): Promise<void> {
  return api<void>("api/v1/key/unlock", "POST", { password });
}

/**
 * 锁定私钥 / 注销
 * POST /api/v1/key/lock
 * 
 * 锁定私钥，清除会话状态
 */
export function lockKey(): Promise<void> {
  return api<void>("api/v1/key/lock", "POST");
}

// ========== 网关管理 API ==========

/**
 * 获取网关列表
 * GET /api/v1/gateways
 */
export function getGateways(): Promise<GatewaysResp> {
  return api<GatewaysResp>("api/v1/gateways");
}

/**
 * 保存网关（新增或更新）
 * POST /api/v1/gateways - 新增
 * PUT /api/v1/gateways?id={id} - 更新
 * 
 * @param id - 网关 ID，null 表示新增
 * @param data - 网关数据
 */
export function saveGateway(id: number | null, data: Gateway): Promise<void> {
  if (id === null) {
    return api<void>("api/v1/gateways", "POST", data);
  }
  return api<void>(`api/v1/gateways?id=${id}`, "PUT", data);
}

/**
 * 删除网关
 * DELETE /api/v1/gateways?id={id}
 * 
 * @param id - 网关 ID
 */
export function deleteGateway(id: number): Promise<void> {
  return api<void>(`api/v1/gateways?id=${id}`, "DELETE");
}

// ========== 仲裁者管理 API ==========

/**
 * 获取仲裁服务器列表
 * GET /api/v1/arbiters
 */
export function getArbiters(): Promise<ArbitersResp> {
  return api<ArbitersResp>("api/v1/arbiters");
}

/**
 * 保存仲裁服务器（新增或更新）
 * POST /api/v1/arbiters - 新增
 * PUT /api/v1/arbiters?id={id} - 更新
 * 
 * @param id - 仲裁者 ID，null 表示新增
 * @param data - 仲裁者数据
 */
export function saveArbiter(id: number | null, data: Arbiter): Promise<void> {
  if (id === null) {
    return api<void>("api/v1/arbiters", "POST", data);
  }
  return api<void>(`api/v1/arbiters?id=${id}`, "PUT", data);
}

/**
 * 删除仲裁服务器
 * DELETE /api/v1/arbiters?id={id}
 * 
 * @param id - 仲裁者 ID
 */
export function deleteArbiter(id: number): Promise<void> {
  return api<void>(`api/v1/arbiters?id=${id}`, "DELETE");
}

// ========== 工作区管理 API ==========

/**
 * 获取工作区列表
 * GET /api/v1/admin/workspaces
 */
export function getWorkspaces(): Promise<WorkspacesResp> {
  return api<WorkspacesResp>("api/v1/admin/workspaces");
}

/**
 * 保存工作区（新增或更新）
 * POST /api/v1/admin/workspaces - 新增
 * PUT /api/v1/admin/workspaces?id={id} - 更新
 * 
 * @param id - 工作区 ID，null 表示新增
 * @param data - 工作区数据（部分更新）
 */
export function saveWorkspace(id: number | null, data: Partial<Workspace>): Promise<void> {
  if (id === null) {
    return api<void>("api/v1/admin/workspaces", "POST", data);
  }
  return api<void>(`api/v1/admin/workspaces?id=${id}`, "PUT", data);
}

/**
 * 删除工作区
 * DELETE /api/v1/admin/workspaces?id={id}
 * 
 * @param id - 工作区 ID
 */
export function deleteWorkspace(id: number): Promise<void> {
  return api<void>(`api/v1/admin/workspaces?id=${id}`, "DELETE");
}

// ========== 静态文件管理 API ==========

/**
 * 获取静态文件树
 * GET /api/v1/admin/static/tree?path={path}
 * 
 * @param path - 目录路径，如 "/" 或 "/folder"
 */
export function getStaticTree(path: string): Promise<StaticTreeResp> {
  return api<StaticTreeResp>(`api/v1/admin/static/tree?path=${encodeURIComponent(path)}`);
}

/**
 * 创建静态文件目录
 * POST /api/v1/admin/static/mkdir
 * 
 * @param path - 新目录的完整路径
 */
export function createStaticDir(path: string): Promise<void> {
  return api<void>("api/v1/admin/static/mkdir", "POST", { path });
}

/**
 * 移动静态文件/目录
 * POST /api/v1/admin/static/move
 * 
 * @param fromPath - 源路径
 * @param toPath - 目标路径
 */
export function moveStaticItem(fromPath: string, toPath: string): Promise<void> {
  return api<void>("api/v1/admin/static/move", "POST", { from: fromPath, to: toPath });
}

/**
 * 删除静态文件/目录
 * DELETE /api/v1/admin/static/entry
 * 
 * @param path - 要删除的路径
 */
export function deleteStaticEntry(path: string): Promise<void> {
  return api<void>("api/v1/admin/static/entry", "DELETE", { path });
}

/**
 * 设置静态文件价格
 * POST /api/v1/admin/static/price/set
 * 
 * @param path - 文件路径
 * @param floorPrice - 底价（sat/64k）
 * @param discountBps - 折扣（基点，如 8000 表示 80%）
 */
export function setStaticItemPrice(path: string, floorPrice: number, discountBps: number): Promise<void> {
  return api<void>("api/v1/admin/static/price/set", "POST", {
    path,
    floor_price_sat_per_64k: floorPrice,
    resale_discount_bps: discountBps
  });
}

/**
 * 上传静态文件
 * POST /api/v1/admin/static/upload
 * 
 * 使用 FormData 上传文件，不走 JSON api
 * 
 * @param file - 要上传的文件
 * @param targetDir - 目标目录
 * @param overwrite - 是否覆盖已存在文件
 */
export async function uploadStaticFile(file: File, targetDir: string, overwrite: boolean): Promise<void> {
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
}

// ========== 钱包相关 API ==========

/**
 * 获取交易流水列表
 * GET /api/v1/transactions
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getTransactions(params?: URLSearchParams): Promise<TxResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<TxResp>(`api/v1/transactions${query}`);
}

/**
 * 获取钱包汇总信息
 * GET /api/v1/wallet/summary
 */
export function getWalletSummary(): Promise<WalletSummaryResp> {
  return api<WalletSummaryResp>("api/v1/wallet/summary");
}

/**
 * 获取链上账本列表
 * GET /api/v1/wallet/ledger
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getWalletLedger(params?: URLSearchParams): Promise<WalletLedgerResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<WalletLedgerResp>(`api/v1/wallet/ledger${query}`);
}

/**
 * 获取网关资金事件列表
 * GET /api/v1/gateways/events
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getGatewayEvents(params?: URLSearchParams): Promise<GatewayEventsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<GatewayEventsResp>(`api/v1/gateways/events${query}`);
}

/**
 * 获取 Direct 资金池列表
 * GET /api/v1/direct/transfer-pools
 * 
 * @param params - 查询参数（分页等）
 */
export function getDirectTransferPools(params?: URLSearchParams): Promise<DirectTransferPoolsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<DirectTransferPoolsResp>(`api/v1/direct/transfer-pools${query}`);
}

// ========== 文件相关 API ==========

/**
 * 获取种子列表
 * GET /api/v1/workspace/seeds
 * 
 * @param params - 查询参数（分页、seed_hash_like 等）
 */
export function getSeeds(params?: URLSearchParams): Promise<SeedsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<SeedsResp>(`api/v1/workspace/seeds${query}`);
}

/**
 * 获取售卖记录列表
 * GET /api/v1/sales
 * 
 * @param params - 查询参数（分页、seed_hash 等）
 */
export function getSales(params?: URLSearchParams): Promise<SalesResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<SalesResp>(`api/v1/sales${query}`);
}

/**
 * 获取文件索引列表
 * GET /api/v1/workspace/files
 * 
 * @param params - 查询参数（分页、path_like 等）
 */
export function getFiles(params?: URLSearchParams): Promise<FilesResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<FilesResp>(`api/v1/workspace/files${query}`);
}

/**
 * 更新种子价格
 * POST /api/v1/workspace/seeds/price
 * 
 * @param seedHash - 种子哈希
 * @param floorPrice - 底价（sat/64k）
 * @param discountBps - 折扣（基点）
 */
export function updateSeedPrice(seedHash: string, floorPrice: number, discountBps: number): Promise<void> {
  return api<void>("api/v1/workspace/seeds/price", "POST", {
    seed_hash: seedHash,
    floor_price_sat_per_64k: floorPrice,
    resale_discount_bps: discountBps
  });
}

// ========== Direct 交易 API ==========

/**
 * 获取 Direct 报价列表
 * GET /api/v1/direct/quotes
 * 
 * @param params - 查询参数（分页等）
 */
export function getDirectQuotes(params?: URLSearchParams): Promise<DirectQuotesResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<DirectQuotesResp>(`api/v1/direct/quotes${query}`);
}

/**
 * 获取 Direct 成交记录列表
 * GET /api/v1/direct/deals
 * 
 * @param params - 查询参数（分页等）
 */
export function getDirectDeals(params?: URLSearchParams): Promise<DirectDealsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<DirectDealsResp>(`api/v1/direct/deals${query}`);
}

/**
 * 获取 Direct 会话列表
 * GET /api/v1/direct/sessions
 * 
 * @param params - 查询参数（分页等）
 */
export function getDirectSessions(params?: URLSearchParams): Promise<DirectSessionsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<DirectSessionsResp>(`api/v1/direct/sessions${query}`);
}

// ========== Live 流 API ==========

/**
 * 获取直播流列表
 * GET /api/v1/admin/live/streams
 */
export function getLiveStreams(): Promise<LiveStreamsResp> {
  return api<LiveStreamsResp>("api/v1/admin/live/streams");
}

/**
 * 获取直播流详情
 * GET /api/v1/admin/live/streams/detail?stream_id={streamId}
 * 
 * @param streamId - 直播流 ID
 */
export function getLiveStreamDetail(streamId: string): Promise<LiveStreamDetail> {
  return api<LiveStreamDetail>(`api/v1/admin/live/streams/detail?stream_id=${encodeURIComponent(streamId)}`);
}

/**
 * 获取 Live 关注状态
 * GET /api/v1/live/follow/status
 */
export function getLiveFollowStatus(): Promise<LiveFollowStatus> {
  return api<LiveFollowStatus>("api/v1/live/follow/status");
}

/**
 * 获取 Live 存储汇总信息
 * GET /api/v1/admin/live/storage/summary
 */
export function getLiveStorageSummary(): Promise<AdminLiveStorageSummary> {
  return api<AdminLiveStorageSummary>("api/v1/admin/live/storage/summary");
}

/**
 * 删除直播流
 * DELETE /api/v1/admin/live/streams?stream_id={streamId}
 * 
 * @param streamId - 直播流 ID
 */
export function deleteLiveStream(streamId: string): Promise<void> {
  return api<void>(`api/v1/admin/live/streams?stream_id=${encodeURIComponent(streamId)}`, "DELETE");
}

// ========== Admin 管理 API ==========

/**
 * 获取系统配置
 * GET /api/v1/admin/config
 * 返回结构: { config: AdminConfig }
 */
export async function getAdminConfig(): Promise<AdminConfig> {
  const resp = await api<{ config: AdminConfig }>("api/v1/admin/config");
  return resp.config;
}

/**
 * 获取系统配置模式（schema）
 * GET /api/v1/admin/config/schema
 */
export function getAdminConfigSchema(): Promise<AdminConfigSchema> {
  return api<AdminConfigSchema>("api/v1/admin/config/schema");
}

/**
 * 保存系统配置项
 * POST /api/v1/admin/config
 * 
 * @param key - 配置键
 * @param value - 配置值
 */
export function saveAdminConfig(key: string, value: unknown): Promise<void> {
  return api<void>("api/v1/admin/config", "POST", {
    items: [{ key, value }]
  });
}

/**
 * 恢复下载
 * POST /api/v1/admin/downloads/resume
 * 
 * @param demandId - 需求 ID
 */
export function resumeDownload(demandId: string): Promise<void> {
  return api<void>("api/v1/admin/downloads/resume", "POST", { demand_id: demandId });
}

// ========== Orchestrator API ==========

/**
 * 获取调度器日志列表
 * GET /api/v1/admin/orchestrator/logs
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getOrchestratorLogs(params?: URLSearchParams): Promise<OrchestratorEventsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<OrchestratorEventsResp>(`api/v1/admin/orchestrator/logs${query}`);
}

/**
 * 获取调度器日志详情
 * GET /api/v1/admin/orchestrator/logs/detail?event_id={eventId}
 * 
 * @param eventId - 事件 ID
 */
export function getOrchestratorLogDetail(eventId: string): Promise<OrchestratorEventDetail> {
  return api<OrchestratorEventDetail>(`api/v1/admin/orchestrator/logs/detail?event_id=${encodeURIComponent(eventId)}`);
}

/**
 * 获取调度器状态
 * GET /api/v1/admin/orchestrator/status
 */
export function getOrchestratorStatus(): Promise<OrchestratorStatus> {
  return api<OrchestratorStatus>("api/v1/admin/orchestrator/status");
}

// ========== ClientKernel API ==========

/**
 * 获取 ClientKernel 命令列表
 * GET /api/v1/admin/client-kernel/commands
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getClientKernelCommands(params?: URLSearchParams): Promise<ClientKernelCommandsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<ClientKernelCommandsResp>(`api/v1/admin/client-kernel/commands${query}`);
}

/**
 * 获取 ClientKernel 命令详情
 * GET /api/v1/admin/client-kernel/commands/detail?id={id}
 * 
 * @param id - 命令记录 ID
 */
export function getClientKernelCommandDetail(id: number): Promise<ClientKernelCommand> {
  return api<ClientKernelCommand>(`api/v1/admin/client-kernel/commands/detail?id=${id}`);
}

// ========== FeePool API ==========

/**
 * 获取 FeePool 命令列表
 * GET /api/v1/admin/feepool/commands
 * 
 * @param params - 查询参数（分页、过滤等）
 */
export function getFeePoolCommands(params?: URLSearchParams): Promise<FeePoolCommandsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<FeePoolCommandsResp>(`api/v1/admin/feepool/commands${query}`);
}

/**
 * 获取 FeePool 命令详情
 * GET /api/v1/admin/feepool/commands/detail?id={id}
 * 
 * @param id - 命令记录 ID
 */
export function getFeePoolCommandDetail(id: number): Promise<FeePoolCommand> {
  return api<FeePoolCommand>(`api/v1/admin/feepool/commands/detail?id=${id}`);
}

/**
 * 获取 FeePool 事件列表
 * GET /api/v1/admin/feepool/events
 * 
 * @param params - 查询参数（分页等）
 */
export function getFeePoolEvents(params?: URLSearchParams): Promise<FeePoolEventsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<FeePoolEventsResp>(`api/v1/admin/feepool/events${query}`);
}

/**
 * 获取 FeePool 状态列表
 * GET /api/v1/admin/feepool/states
 * 
 * @param params - 查询参数（分页等）
 */
export function getFeePoolStates(params?: URLSearchParams): Promise<FeePoolStatesResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<FeePoolStatesResp>(`api/v1/admin/feepool/states${query}`);
}

/**
 * 获取 FeePool 效果列表
 * GET /api/v1/admin/feepool/effects
 * 
 * @param params - 查询参数（分页等）
 */
export function getFeePoolEffects(params?: URLSearchParams): Promise<FeePoolEffectsResp> {
  const query = params ? `?${params.toString()}` : "";
  return api<FeePoolEffectsResp>(`api/v1/admin/feepool/effects${query}`);
}

// ========== 文件获取任务 API ==========

/**
 * 获取文件获取任务详情
 * GET /api/v1/files/get-file/job?id={jobId}
 * 
 * @param jobId - 任务 ID
 */
export function getFileGetJob(jobId: string): Promise<FileGetJob> {
  return api<FileGetJob>(`api/v1/files/get-file/job?id=${encodeURIComponent(jobId)}`);
}

/**
 * 启动文件获取任务
 * POST /api/v1/files/get-file
 * 
 * @param seedHash - 种子哈希
 * @param chunkCount - 分片数量
 * @returns 包含 job_id 的对象
 */
export function startFileGetJob(seedHash: string, chunkCount: number): Promise<{ job_id: string }> {
  return api<{ job_id: string }>("api/v1/files/get-file", "POST", {
    seed_hash: seedHash,
    chunk_count: chunkCount
  });
}

// ========== 策略调试日志 API ==========

/**
 * 获取策略调试日志启用状态
 * GET /api/v1/admin/fs-http/strategy-debug-log
 */
export function getStrategyDebugLog(): Promise<{ strategy_debug_log_enabled: boolean }> {
  return api<{ strategy_debug_log_enabled: boolean }>("api/v1/admin/fs-http/strategy-debug-log");
}

/**
 * 设置策略调试日志启用状态
 * POST /api/v1/admin/fs-http/strategy-debug-log
 * 
 * @param enabled - 是否启用
 */
export function setStrategyDebugLog(enabled: boolean): Promise<void> {
  return api<void>("api/v1/admin/fs-http/strategy-debug-log", "POST", { enabled });
}
