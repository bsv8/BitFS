/**
 * App - BitFS 客户端应用入口
 * 
 * 这是应用的根组件，负责：
 * 1. 认证状态管理（通过 useAuth hook）
 * 2. 路由管理（hash 路由）
 * 3. 业务数据加载和状态管理
 * 4. 页面渲染协调
 * 
 * 架构说明：
 * - 认证相关逻辑已抽取到 hooks/useAuth.ts
 * - API 调用已抽取到 api/index.ts
 * - 类型定义已抽取到 types/index.ts
 * - 工具函数已抽取到 utils/index.ts
 * - UI 组件已抽取到 components/*.tsx
 */

import React, { useEffect, useState, useCallback } from "react";

// ========== 类型导入 ==========
import type {
  AuthState,
  HashRoute,
  TxResp,
  WalletLedgerResp,
  GatewayEventsResp,
  WalletSummaryResp,
  SeedsResp,
  SalesResp,
  FilesResp,
  SeedItem,
  SeedPriceDraft,
  Gateway,
  GatewaysResp,
  Arbiter,
  ArbitersResp,
  DirectQuotesResp,
  DirectDealsResp,
  DirectSessionsResp,
  DirectTransferPoolsResp,
  LiveStreamsResp,
  LiveStreamDetail,
  LiveFollowStatus,
  AdminLiveStorageSummary,
  AdminConfig,
  AdminConfigSchema,
  OrchestratorEventsResp,
  OrchestratorEventDetail,
  OrchestratorStatus,
  ClientKernelCommandsResp,
  ClientKernelCommand,
  FeePoolCommandsResp,
  FeePoolCommand,
  FeePoolEventsResp,
  FeePoolStatesResp,
  FeePoolEffectsResp,
  Workspace,
  WorkspacesResp,
  StaticTreeResp,
  FileGetJob,
} from "./types";

// ========== API 导入 ==========
import {
  getTransactions,
  getWalletSummary,
  getWalletLedger,
  getGatewayEvents,
  getGateways,
  saveGateway,
  deleteGateway,
  getArbiters,
  saveArbiter,
  deleteArbiter,
  getSeeds,
  getSales,
  getFiles,
  updateSeedPrice,
  getDirectQuotes,
  getDirectDeals,
  getDirectSessions,
  getDirectTransferPools,
  getLiveStreams,
  getLiveStreamDetail,
  getLiveFollowStatus,
  getLiveStorageSummary,
  deleteLiveStream,
  getAdminConfig,
  getAdminConfigSchema,
  saveAdminConfig,
  resumeDownload,
  getOrchestratorLogs,
  getOrchestratorLogDetail,
  getOrchestratorStatus,
  getClientKernelCommands,
  getClientKernelCommandDetail,
  getFeePoolCommands,
  getFeePoolCommandDetail,
  getFeePoolEvents,
  getFeePoolStates,
  getFeePoolEffects,
  getWorkspaces,
  saveWorkspace,
  deleteWorkspace,
  getStaticTree,
  createStaticDir,
  moveStaticItem,
  deleteStaticEntry,
  setStaticItemPrice,
  uploadStaticFile,
  getFileGetJob,
  startFileGetJob,
  getStrategyDebugLog,
  setStrategyDebugLog,
} from "./api";

// ========== 工具函数导入 ==========
import { sat, t, short, shortHex, formatBytes, nowPath, setHash, toInt } from "./utils";

// ========== 组件导入 ==========
import {
  Modal,
  DetailTable,
  Pager,
  GatewayManagerPage,
  ArbiterManagerPage,
  StaticFileManager,
  MainLayout,
} from "./components";

// ========== Hook 导入 ==========
import { useAuth, setResetBusinessState } from "./hooks/useAuth";

// ========== 页面组件导入 ==========
import UnlockPage from "./pages/UnlockPage";

// ========== 辅助函数 ==========

function gatewayEventMsg(e: GatewayEventsResp["items"][number]): string {
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

function purposeLabel(purpose: string, eventType: string): string {
  const key = (purpose || eventType || "").trim();
  const map: Record<string, string> = {
    listen_bootstrap_topup: "初始化入池",
    auto_renew_topup: "自动续费入池",
    listen_cycle_fee: "侦听周期扣费",
    demand_publish_fee: "需求发布扣费",
    pool_exhausted: "费用池耗尽",
    renew_needed: "续费提醒",
    service_stopped: "服务停止",
    demand_publish_failed: "需求发布失败",
  };
  return map[key] || key || "-";
}

// ========== 主应用组件 ==========

export default function App() {
  // ========== 使用 useAuth Hook ==========
  const {
    auth,
    authErr,
    loading,
    keyStatus,
    passwordInput,
    confirmPassword,
    importCipher,
    view,
    setPasswordInput,
    setConfirmPassword,
    setImportCipher,
    setView,
    checkKeyStatus,
    createKey,
    importKey,
    unlock,
    lock,
    exportKey,
    resetBusinessState,
  } = useAuth();

  // ========== 路由状态 ==========
  const [route, setRoute] = useState<HashRoute>(() => nowPath());
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  // ========== 钱包状态 ==========
  const [tx, setTx] = useState<TxResp | null>(null);
  const [walletLedger, setWalletLedger] = useState<WalletLedgerResp | null>(null);
  const [gatewayEvents, setGatewayEvents] = useState<GatewayEventsResp | null>(null);
  const [txDetail, setTxDetail] = useState<unknown>(null);
  const [walletLedgerDetail, setWalletLedgerDetail] = useState<unknown>(null);
  const [gatewayEventDetail, setGatewayEventDetail] = useState<Record<string, unknown> | null>(null);
  const [gatewayEventModalOpen, setGatewayEventModalOpen] = useState(false);
  const [walletSummary, setWalletSummary] = useState<WalletSummaryResp | null>(null);

  // ========== 网关状态 ==========
  const [gateways, setGateways] = useState<GatewaysResp | null>(null);

  // ========== 仲裁服务器状态 ==========
  const [arbiters, setArbiters] = useState<ArbitersResp | null>(null);

  // ========== 文件状态 ==========
  const [seeds, setSeeds] = useState<SeedsResp | null>(null);
  const [sales, setSales] = useState<SalesResp | null>(null);
  const [files, setFiles] = useState<FilesResp | null>(null);
  const [seedDrafts, setSeedDrafts] = useState<Record<string, SeedPriceDraft>>({});
  const [saleDetail, setSalesDetail] = useState<unknown>(null);

  // ========== Direct 交易状态 ==========
  const [directQuotes, setDirectQuotes] = useState<DirectQuotesResp | null>(null);
  const [directDeals, setDirectDeals] = useState<DirectDealsResp | null>(null);
  const [directSessions, setDirectSessions] = useState<DirectSessionsResp | null>(null);
  const [directTransferPools, setDirectTransferPools] = useState<DirectTransferPoolsResp | null>(null);

  // ========== Live 流状态 ==========
  const [liveStreams, setLiveStreams] = useState<LiveStreamsResp | null>(null);
  const [liveStreamDetail, setLiveStreamDetail] = useState<LiveStreamDetail | null>(null);
  const [liveFollowStatus, setLiveFollowStatus] = useState<LiveFollowStatus | null>(null);
  const [liveStorageSummary, setLiveStorageSummary] = useState<AdminLiveStorageSummary | null>(null);

  // ========== Admin 管理状态 ==========
  const [adminConfig, setAdminConfig] = useState<AdminConfig | null>(null);
  const [adminConfigSchema, setAdminConfigSchema] = useState<AdminConfigSchema | null>(null);
  const [configDrafts, setConfigDrafts] = useState<Record<string, string>>({});

  // ========== Orchestrator 调度状态 ==========
  const [orchestratorEvents, setOrchestratorEvents] = useState<OrchestratorEventsResp | null>(null);
  const [orchestratorEventDetail, setOrchestratorEventDetail] = useState<OrchestratorEventDetail | null>(null);
  const [orchestratorStatus, setOrchestratorStatus] = useState<OrchestratorStatus | null>(null);
  const [orchestratorModalOpen, setOrchestratorModalOpen] = useState(false);

  // ========== ClientKernel 命令状态 ==========
  const [clientKernelCommands, setClientKernelCommands] = useState<ClientKernelCommandsResp | null>(null);
  const [clientKernelCommandDetail, setClientKernelCommandDetail] = useState<ClientKernelCommand | null>(null);
  const [clientKernelModalOpen, setClientKernelModalOpen] = useState(false);

  // ========== FeePool 审计状态 ==========
  const [feePoolCommands, setFeePoolCommands] = useState<FeePoolCommandsResp | null>(null);
  const [feePoolCommandDetail, setFeePoolCommandDetail] = useState<FeePoolCommand | null>(null);
  const [feePoolEvents, setFeePoolEvents] = useState<FeePoolEventsResp | null>(null);
  const [feePoolStates, setFeePoolStates] = useState<FeePoolStatesResp | null>(null);
  const [feePoolEffects, setFeePoolEffects] = useState<FeePoolEffectsResp | null>(null);
  const [feePoolAuditModalOpen, setFeePoolAuditModalOpen] = useState(false);
  const [feePoolAuditTab, setFeePoolAuditTab] = useState<"command" | "events" | "states" | "effects">("command");

  // ========== 工作区管理状态 ==========
  const [workspaces, setWorkspaces] = useState<WorkspacesResp | null>(null);
  const [workspaceEditing, setWorkspaceEditing] = useState<{ id: number | null; data: Partial<Workspace> } | null>(null);

  // ========== 静态文件管理状态 ==========
  const [staticTree, setStaticTree] = useState<StaticTreeResp | null>(null);
  const [staticPathHistory, setStaticPathHistory] = useState<string[]>(["/"]);
  const [staticCurrentPath, setStaticCurrentPath] = useState("/");
  const [selectedStaticItems, setSelectedStaticItems] = useState<Set<string>>(new Set());

  // ========== 文件获取任务状态 ==========
  const [getFileJob, setGetFileJob] = useState<FileGetJob | null>(null);
  const [getFileSeedHash, setGetFileSeedHash] = useState("");
  const [getFileChunkCount, setGetFileChunkCount] = useState("1");

  // ========== 策略调试日志状态 ==========
  const [strategyDebugLogEnabled, setStrategyDebugLogEnabled] = useState<boolean | null>(null);
  const [strategyDebugLogLoading, setStrategyDebugLogLoading] = useState(false);

  // ========== 初始化 Effect：检查密钥状态 ==========
  useEffect(() => {
    void checkKeyStatus();
  }, [checkKeyStatus]);

  // ========== 路由 Effect：监听 hashchange ==========
  useEffect(() => {
    const onHash = () => setRoute(nowPath());
    window.addEventListener("hashchange", onHash);
    if (!window.location.hash) setHash("/finance");
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  // ========== 路由 Effect：根据 route.path 加载数据 ==========
  useEffect(() => {
    if (auth !== "unlocked") return;

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

  // ========== 文件任务轮询 Effect ==========
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

  // ========== 注册业务状态重置函数到 useAuth ==========
  useEffect(() => {
    setResetBusinessState(() => {
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
    });
  }, []);

  // ========== 数据加载函数 ==========

  // ----- 钱包模块 -----
  const loadWalletSummary = async () => {
    try {
      const [txData, summaryData] = await Promise.all([
        getTransactions(new URLSearchParams({ limit: "5", offset: "0" })),
        getWalletSummary(),
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
    setTx(await getTransactions(params));
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    if (detailID > 0) {
      // 详情通过单独 API 获取，这里简化处理
      setTxDetail(await getTransactions(new URLSearchParams({ id: String(detailID) })));
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
    setWalletLedger(await getWalletLedger(params));
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    if (detailID > 0) {
      setWalletLedgerDetail(await getWalletLedger(new URLSearchParams({ id: String(detailID) })));
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
    const list = await getGatewayEvents(params);
    setGatewayEvents(list);
    setGatewayEventDetail(null);
    setGatewayEventModalOpen(false);
  };

  const loadGatewayEventDetail = async (id: number) => {
    try {
      const detail = await getGatewayEvents(new URLSearchParams({ id: String(id) }));
      setGatewayEventDetail(detail as unknown as Record<string, unknown>);
      setGatewayEventModalOpen(true);
    } catch (e) {
      alert(e instanceof Error ? e.message : "加载详情失败");
    }
  };

  // ----- 网关模块 -----
  const loadGateways = async () => {
    setGateways(await getGateways());
  };

  const handleSaveGateway = async (id: number | null, data: Gateway) => {
    await saveGateway(id, data);
    await loadGateways();
  };

  const handleDeleteGateway = async (id: number) => {
    await deleteGateway(id);
    await loadGateways();
  };

  // ----- 仲裁服务器模块 -----
  const loadArbiters = async () => {
    setArbiters(await getArbiters());
  };

  const handleSaveArbiter = async (id: number | null, data: Arbiter) => {
    await saveArbiter(id, data);
    await loadArbiters();
  };

  const handleDeleteArbiter = async (id: number) => {
    await deleteArbiter(id);
    await loadArbiters();
  };

  // ----- 文件模块 -----
  const loadFilesSummary = async () => {
    const [s1, s2] = await Promise.all([
      getSeeds(new URLSearchParams({ limit: "5", offset: "0" })),
      getSales(new URLSearchParams({ limit: "5", offset: "0" })),
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
    setSeeds(await getSeeds(params));
  };

  const loadFileSales = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const seedHash = route.query.get("seed_hash") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (seedHash) params.set("seed_hash", seedHash);
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    const list = await getSales(params);
    setSales(list);
    if (detailID > 0) {
      setSalesDetail(await getSales(new URLSearchParams({ id: String(detailID) })));
    } else {
      setSalesDetail(null);
    }
  };

  const loadFileIndex = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const pathLike = route.query.get("path_like") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (pathLike) params.set("path_like", pathLike);
    setFiles(await getFiles(params));
  };

  const setSeedDraft = (seed: SeedItem, patch: Partial<SeedPriceDraft>) => {
    setSeedDrafts((prev) => {
      const cur = prev[seed.seed_hash] ?? {
        floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
        discount: String(seed.resale_discount_bps || 8000),
      };
      return { ...prev, [seed.seed_hash]: { ...cur, ...patch } };
    });
  };

  const handleSaveSeedPrice = async (seed: SeedItem) => {
    const d = seedDrafts[seed.seed_hash] ?? {
      floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
      discount: String(seed.resale_discount_bps || 8000),
    };
    setSeedDraft(seed, { saving: true, message: "" });
    try {
      await updateSeedPrice(seed.seed_hash, Number(d.floor || "0"), Number(d.discount || "0"));
      setSeedDraft(seed, { saving: false, message: "已更新" });
      await loadFileSeeds();
    } catch (e) {
      setSeedDraft(seed, { saving: false, message: e instanceof Error ? e.message : "更新失败" });
    }
  };

  // ----- Direct 模块 -----
  const loadDirectQuotes = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectQuotes(await getDirectQuotes(params));
  };

  const loadDirectDeals = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectDeals(await getDirectDeals(params));
  };

  const loadDirectSessions = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectSessions(await getDirectSessions(params));
  };

  const loadDirectTransferPools = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    setDirectTransferPools(await getDirectTransferPools(params));
  };

  // ----- Live 模块 -----
  const loadLiveStreams = async () => {
    setLiveStreams(await getLiveStreams());
  };

  const handleLoadLiveStreamDetail = async (streamId: string) => {
    setLiveStreamDetail(await getLiveStreamDetail(streamId));
  };

  const loadLiveFollowStatus = async () => {
    setLiveFollowStatus(await getLiveFollowStatus());
  };

  const loadLiveStorageSummary = async () => {
    setLiveStorageSummary(await getLiveStorageSummary());
  };

  const handleDeleteLiveStream = async (streamId: string) => {
    await deleteLiveStream(streamId);
    await loadLiveStreams();
  };

  // ----- Admin 管理模块 -----
  const loadAdminConfig = async () => {
    setAdminConfig(await getAdminConfig());
  };

  const loadAdminConfigSchema = async () => {
    setAdminConfigSchema(await getAdminConfigSchema());
  };

  const handleSaveAdminConfig = async (key: string, value: unknown) => {
    try {
      await saveAdminConfig(key, value);
      await loadAdminConfig();
      setConfigDrafts((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
    } catch (e) {
      alert(e instanceof Error ? e.message : "保存失败");
    }
  };

  const handleResumeDownload = async (demandId: string) => {
    try {
      await resumeDownload(demandId);
      alert("已触发恢复下载");
    } catch (e) {
      alert(e instanceof Error ? e.message : "恢复失败");
    }
  };

  // ----- Orchestrator 调度模块 -----
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
    setOrchestratorEvents(await getOrchestratorLogs(params));
  };

  const loadOrchestratorLogDetail = async (eventID: string) => {
    const detail = await getOrchestratorLogDetail(eventID);
    setOrchestratorEventDetail(detail);
    setOrchestratorModalOpen(true);
  };

  const loadOrchestratorStatus = async () => {
    setOrchestratorStatus(await getOrchestratorStatus());
  };

  // ----- ClientKernel 命令模块 -----
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
    setClientKernelCommands(await getClientKernelCommands(params));
  };

  const loadClientKernelCommandDetail = async (id: number) => {
    const detail = await getClientKernelCommandDetail(id);
    setClientKernelCommandDetail(detail);
    setClientKernelModalOpen(true);
  };

  // ----- FeePool 审计模块 -----
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
    setFeePoolCommands(await getFeePoolCommands(params));
  };

  const loadFeePoolCommandDetail = async (id: number) => {
    const detail = await getFeePoolCommandDetail(id);
    setFeePoolCommandDetail(detail);
    const [events, states, effects] = await Promise.all([
      getFeePoolEvents(new URLSearchParams({ command_id: detail.command_id, limit: "50" })),
      getFeePoolStates(new URLSearchParams({ command_id: detail.command_id, limit: "50" })),
      getFeePoolEffects(new URLSearchParams({ command_id: detail.command_id, limit: "50" })),
    ]);
    setFeePoolEvents(events);
    setFeePoolStates(states);
    setFeePoolEffects(effects);
    setFeePoolAuditTab("command");
    setFeePoolAuditModalOpen(true);
  };

  // ----- 工作区管理模块 -----
  const loadWorkspaces = async () => {
    setWorkspaces(await getWorkspaces());
  };

  const handleSaveWorkspace = async (id: number | null, data: Partial<Workspace>) => {
    await saveWorkspace(id, data);
    await loadWorkspaces();
  };

  const handleDeleteWorkspace = async (id: number) => {
    await deleteWorkspace(id);
    await loadWorkspaces();
  };

  // ----- 策略调试日志模块 -----
  const loadStrategyDebugLog = async () => {
    const resp = await getStrategyDebugLog();
    setStrategyDebugLogEnabled(resp.strategy_debug_log_enabled);
  };

  const handleSetStrategyDebugLog = async (enabled: boolean) => {
    setStrategyDebugLogLoading(true);
    try {
      await setStrategyDebugLog(enabled);
      setStrategyDebugLogEnabled(enabled);
    } catch (e) {
      alert(e instanceof Error ? e.message : "设置失败");
    } finally {
      setStrategyDebugLogLoading(false);
    }
  };

  // ----- 静态文件管理模块 -----
  const loadStaticTree = async (path: string) => {
    setBusy(true);
    try {
      const tree = await getStaticTree(path);
      setStaticTree(tree);
      setStaticCurrentPath(path);
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

  const handleCreateStaticDir = async (name: string) => {
    const newPath = staticCurrentPath === "/" ? `/${name}` : `${staticCurrentPath}/${name}`;
    await createStaticDir(newPath);
    await loadStaticTree(staticCurrentPath);
  };

  const handleMoveStaticItem = async (fromPath: string, toPath: string) => {
    await moveStaticItem(fromPath, toPath);
    await loadStaticTree(staticCurrentPath);
  };

  const handleDeleteStaticEntry = async (path: string) => {
    if (!confirm(`确定删除 ${path} 吗？`)) return;
    await deleteStaticEntry(path);
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

  const handleSetStaticItemPrice = async (path: string, floorPrice: number, discountBps: number) => {
    await setStaticItemPrice(path, floorPrice, discountBps);
    await loadStaticTree(staticCurrentPath);
  };

  const handleUploadStaticFile = async (file: File, targetDir: string, overwrite: boolean) => {
    await uploadStaticFile(file, targetDir, overwrite);
  };

  // ----- 文件获取任务模块 -----
  const loadGetFileJob = async () => {
    const jobID = route.query.get("job_id") || "";
    if (!jobID) {
      setGetFileJob(null);
      return;
    }
    setGetFileJob(await getFileGetJob(jobID));
  };

  const handleStartGetFileJob = async () => {
    const seedHash = getFileSeedHash.trim().toLowerCase();
    const chunkCount = Number(getFileChunkCount || "0");
    if (!seedHash || !chunkCount || chunkCount <= 0) {
      setErr("seed_hash 和 chunk_count 必须有效");
      return;
    }
    const out = await startFileGetJob(seedHash, chunkCount);
    const q = new URLSearchParams(route.query.toString());
    q.set("job_id", out.job_id);
    setHash(route.path, q);
  };

  // ========== 路由辅助函数 ==========
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

  // ========== 渲染 ==========

  // 未解锁状态：显示解锁页面
  if (auth !== "unlocked") {
    return (
      <UnlockPage
        auth={auth}
        authErr={authErr}
        loading={loading}
        keyStatus={keyStatus}
        view={view}
        passwordInput={passwordInput}
        confirmPassword={confirmPassword}
        importCipher={importCipher}
        setPasswordInput={setPasswordInput}
        setConfirmPassword={setConfirmPassword}
        setImportCipher={setImportCipher}
        setView={setView}
        checkKeyStatus={checkKeyStatus}
        createKey={createKey}
        importKey={importKey}
        unlock={unlock}
        exportKey={exportKey}
      />
    );
  }

  // 已解锁状态：显示主布局 + 页面内容
  return (
    <MainLayout route={route} onLock={lock}>
      {/* 加载和错误提示 */}
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
                <div className="hint" style={{ fontSize: 11, marginTop: 4 }}>
                  {short(walletSummary.wallet_address, 16)}
                  <button
                    className="btn btn-light"
                    style={{ marginLeft: 8, padding: "2px 6px", fontSize: 10 }}
                    onClick={() => {
                      navigator.clipboard.writeText(walletSummary.wallet_address);
                      alert("地址已复制");
                    }}
                  >
                    复制
                  </button>
                </div>
              </article>
              <article className="stat">
                <p>累计流入</p>
                <h3 style={{ color: "#0e8f43" }}>{sat(walletSummary.ledger_total_in_satoshi)}</h3>
              </article>
              <article className="stat">
                <p>累计流出</p>
                <h3 style={{ color: "#b51c3f" }}>{sat(walletSummary.ledger_total_out_satoshi)}</h3>
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
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>方向</th>
                    <th>金额</th>
                    <th>划拨原因</th>
                    <th>备注</th>
                  </tr>
                </thead>
                <tbody>
                  {tx.items.map((it) => (
                    <tr key={it.id}>
                      <td>{t(it.created_at_unix)}</td>
                      <td>{it.direction}</td>
                      <td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                      <td>{purposeLabel(it.purpose, it.event_type)}</td>
                      <td>{it.note || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
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
                <input
                  className="input"
                  defaultValue={eventType}
                  placeholder="event_type"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ event_type: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={direction}
                  placeholder="direction"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ direction: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={purpose}
                  placeholder="purpose"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ purpose: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={q}
                  placeholder="关键词"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ q: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>方向</th>
                    <th>金额</th>
                    <th>划拨原因</th>
                    <th>备注</th>
                    <th>网关</th>
                  </tr>
                </thead>
                <tbody>
                  {tx.items.map((it) => (
                    <tr
                      key={it.id}
                      className={
                        toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === it.id ? "selected-row" : ""
                      }
                      onClick={() => updateQuery({ detailId: it.id })}
                    >
                      <td>{t(it.created_at_unix)}</td>
                      <td>{it.direction}</td>
                      <td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                      <td>{purposeLabel(it.purpose, it.event_type)}</td>
                      <td>{it.note || "-"}</td>
                      <td title={it.gateway_peer_id}>{short(it.gateway_peer_id, 8)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={tx.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            {txDetail ? (
              <pre className="detail-pre">{JSON.stringify(txDetail, null, 2)}</pre>
            ) : (
              <div className="hint">点击某条记录查看详细信息</div>
            )}
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
                <input
                  className="input"
                  defaultValue={direction}
                  placeholder="direction: IN/OUT"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ direction: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={category}
                  placeholder="category"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ category: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={status}
                  placeholder="status: MEMPOOL/CONFIRMED"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={txid}
                  placeholder="txid"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ txid: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={q}
                  placeholder="关键词"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ q: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>方向</th>
                    <th>金额</th>
                    <th>分类</th>
                    <th>状态</th>
                    <th>txid</th>
                  </tr>
                </thead>
                <tbody>
                  {walletLedger.items.map((it) => (
                    <tr
                      key={it.id}
                      className={
                        toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === it.id ? "selected-row" : ""
                      }
                      onClick={() => updateQuery({ detailId: it.id })}
                    >
                      <td>{t(it.occurred_at_unix || it.created_at_unix)}</td>
                      <td>{it.direction}</td>
                      <td className={it.direction === "OUT" ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                      <td>{it.category || "-"}</td>
                      <td>{it.status || "-"}</td>
                      <td title={it.txid}>{shortHex(it.txid || "", 8)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={walletLedger.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            {walletLedgerDetail ? (
              <pre className="detail-pre">{JSON.stringify(walletLedgerDetail, null, 2)}</pre>
            ) : (
              <div className="hint">点击某条记录查看链上流水详情</div>
            )}
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
                <input
                  className="input"
                  defaultValue={gatewayPeerID}
                  placeholder="gateway_peer_id"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <input
                  className="input"
                  defaultValue={action}
                  placeholder="action"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ action: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>网关</th>
                    <th>动作</th>
                    <th>金额</th>
                    <th>pool</th>
                    <th>msg</th>
                  </tr>
                </thead>
                <tbody>
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
                </tbody>
              </table>
            </div>
            <Pager total={gatewayEvents.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            <div className="hint">点击某条记录查看事件详情</div>

            {/* 网关事件详情弹窗 */}
            <Modal title="网关事件详情" isOpen={gatewayEventModalOpen} onClose={() => setGatewayEventModalOpen(false)}>
              {gatewayEventDetail && (
                <div className="detail-table-wrap">
                  <DetailTable data={gatewayEventDetail} />
                </div>
              )}
              <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                <button className="btn btn-light" onClick={() => setGatewayEventModalOpen(false)}>
                  关闭
                </button>
              </div>
            </Modal>
          </section>
        );
      })() : null}

      {/* ========== 网关设置页面 ========== */}
      {route.path === "/settings/gateways" && (
        <GatewayManagerPage gateways={gateways} onSave={handleSaveGateway} onDelete={handleDeleteGateway} />
      )}
      {route.path === "/settings/arbiters" && (
        <ArbiterManagerPage arbiters={arbiters} onSave={handleSaveArbiter} onDelete={handleDeleteArbiter} />
      )}

      {/* ========== 文件模块页面 ========== */}
      {route.path === "/files" && seeds && sales ? (
        <>
          <section className="stats-grid">
            <article className="stat">
              <p>种子总数</p>
              <h3>{seeds.total}</h3>
            </article>
            <article className="stat">
              <p>售卖记录总数</p>
              <h3>{sales.total}</h3>
            </article>
            <article className="stat">
              <p>模块</p>
              <h3>文件管理</h3>
            </article>
          </section>
          <section className="panel">
            <div className="panel-head">
              <h3>Get File（按 hash 拉取）</h3>
              <div className="filters">
                <button className="btn btn-light" onClick={() => void loadGetFileJob()}>
                  刷新进度
                </button>
              </div>
            </div>
            <div className="filters">
              <input
                className="input"
                placeholder="seed_hash"
                value={getFileSeedHash}
                onChange={(e) => setGetFileSeedHash(e.target.value)}
              />
              <input
                className="input small"
                type="number"
                min={1}
                value={getFileChunkCount}
                onChange={(e) => setGetFileChunkCount(e.target.value)}
              />
              <button className="btn" onClick={() => void handleStartGetFileJob()}>
                启动拉取任务
              </button>
            </div>
            {getFileJob ? (
              <div className="job-box">
                <div className="hint">
                  job_id: {getFileJob.id} | status: {getFileJob.status} | gateway: {getFileJob.gateway_peer_id || "-"}
                </div>
                {getFileJob.output_file_path ? <div className="hint">output: {getFileJob.output_file_path}</div> : null}
                {getFileJob.error ? <div className="err-inline">error: {getFileJob.error}</div> : null}
                <div className="timeline">
                  {getFileJob.steps.map((st) => (
                    <div key={`${getFileJob.id}-${st.index}`} className={`step ${st.status}`}>
                      <div className="step-head">
                        <strong>
                          {st.index + 1}. {st.name}
                        </strong>
                        <span>{st.status}</span>
                      </div>
                      <div className="hint">
                        {t(st.started_at_unix)} {st.ended_at_unix ? `-> ${t(st.ended_at_unix)}` : ""}
                      </div>
                      {st.detail ? <pre className="detail-pre">{JSON.stringify(st.detail, null, 2)}</pre> : null}
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="hint">输入 seed_hash + chunk_count 后启动任务</div>
            )}
          </section>
          <section className="panel">
            <h3>最近售卖摘要</h3>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>Seed</th>
                    <th>Session</th>
                    <th>成交额</th>
                  </tr>
                </thead>
                <tbody>
                  {sales.items.map((x) => (
                    <tr key={x.id}>
                      <td>{t(x.created_at_unix)}</td>
                      <td title={x.seed_hash}>{short(x.seed_hash, 10)}</td>
                      <td title={x.session_id}>{short(x.session_id, 8)}</td>
                      <td className="up">{sat(x.amount_satoshi)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}

      {(route.path === "/files/seeds" || route.path === "/files/pricing") && seeds ? (() => {
        const page = toInt(route.query.get("page"), 1);
        const pageSize = toInt(route.query.get("pageSize"), 20);
        const like = route.query.get("seed_hash_like") || "";
        return (
          <section className="panel">
            <div className="panel-head">
              <h3>{route.path === "/files/pricing" ? "价格管理" : "种子列表"}</h3>
              <div className="filters">
                <input
                  className="input"
                  defaultValue={like}
                  placeholder="seed_hash_like"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ seed_hash_like: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Seed</th>
                    <th>Chunk</th>
                    <th>文件大小</th>
                    <th>当前单价</th>
                    <th>底价</th>
                    <th>BPS</th>
                    {route.path === "/files/pricing" ? <th>单独改价</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {seeds.items.map((s) => {
                    const d = seedDrafts[s.seed_hash] ?? {
                      floor: String(s.floor_unit_price_sat_per_64k || s.unit_price_sat_per_64k || 10),
                      discount: String(s.resale_discount_bps || 8000),
                    };
                    return (
                      <tr key={s.seed_hash}>
                        <td>
                          <button
                            className="btn btn-ghost copy-btn"
                            title="点击复制"
                            onClick={() => {
                              navigator.clipboard.writeText(s.seed_hash);
                              alert("已复制: " + short(s.seed_hash, 16));
                            }}
                          >
                            {short(s.seed_hash, 12)}
                          </button>
                        </td>
                        <td>{s.chunk_count}</td>
                        <td>{s.file_size.toLocaleString()} B</td>
                        <td>{sat(s.unit_price_sat_per_64k)}</td>
                        <td>{sat(s.floor_unit_price_sat_per_64k)}</td>
                        <td>{s.resale_discount_bps}</td>
                        {route.path === "/files/pricing" ? (
                          <td>
                            <div className="filters">
                              <input
                                className="input small"
                                type="number"
                                value={d.floor}
                                onChange={(e) => setSeedDraft(s, { floor: e.target.value })}
                              />
                              <input
                                className="input small"
                                type="number"
                                value={d.discount}
                                onChange={(e) => setSeedDraft(s, { discount: e.target.value })}
                              />
                              <button className="btn" disabled={!!d.saving} onClick={() => void handleSaveSeedPrice(s)}>
                                {d.saving ? "保存中" : "保存"}
                              </button>
                            </div>
                            {d.message ? <div className="hint">{d.message}</div> : null}
                          </td>
                        ) : null}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
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
            <div className="panel-head">
              <h3>售卖记录</h3>
              <div className="filters">
                <input
                  className="input"
                  defaultValue={seed}
                  placeholder="seed_hash"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ seed_hash: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>Seed</th>
                    <th>Session</th>
                    <th>Chunk</th>
                    <th>单价</th>
                    <th>成交额</th>
                    <th>买方网关</th>
                  </tr>
                </thead>
                <tbody>
                  {sales.items.map((s) => (
                    <tr
                      key={s.id}
                      className={
                        toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === s.id ? "selected-row" : ""
                      }
                      onClick={() => updateQuery({ detailId: s.id })}
                    >
                      <td>{t(s.created_at_unix)}</td>
                      <td title={s.seed_hash}>{short(s.seed_hash, 10)}</td>
                      <td title={s.session_id}>{short(s.session_id, 8)}</td>
                      <td>{s.chunk_index}</td>
                      <td>{sat(s.unit_price_sat_per_64k)}</td>
                      <td className="up">{sat(s.amount_satoshi)}</td>
                      <td title={s.buyer_gateway_peer_id}>{short(s.buyer_gateway_peer_id, 8)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={sales.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            {saleDetail ? (
              <pre className="detail-pre">{JSON.stringify(saleDetail, null, 2)}</pre>
            ) : (
              <div className="hint">点击某条售卖记录查看详细字段</div>
            )}
          </section>
        );
      })() : null}

      {route.path === "/files/index" && files ? (() => {
        const page = toInt(route.query.get("page"), 1);
        const pageSize = toInt(route.query.get("pageSize"), 20);
        const pathLike = route.query.get("path_like") || "";
        return (
          <section className="panel">
            <div className="panel-head">
              <h3>文件索引</h3>
              <div className="filters">
                <input
                  className="input"
                  defaultValue={pathLike}
                  placeholder="path_like"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") updateQuery({ path_like: (e.target as HTMLInputElement).value, page: 1 });
                  }}
                />
                <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                  查询
                </button>
              </div>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>路径</th>
                    <th>文件大小</th>
                    <th>Seed</th>
                    <th>更新时间</th>
                  </tr>
                </thead>
                <tbody>
                  {files.items.map((f) => (
                    <tr key={f.path}>
                      <td title={f.path}>{f.path}</td>
                      <td>{f.file_size.toLocaleString()} B</td>
                      <td title={f.seed_hash}>{short(f.seed_hash, 10)}</td>
                      <td>{t(f.updated_at_unix)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
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
            <div className="panel-head">
              <h3>报价列表</h3>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Seed</th>
                    <th>Chunks</th>
                    <th>报价</th>
                    <th>卖方</th>
                    <th>状态</th>
                    <th>过期时间</th>
                  </tr>
                </thead>
                <tbody>
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
                </tbody>
              </table>
            </div>
            <Pager total={directQuotes.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
          </section>
        );
      })() : null}

      {route.path === "/direct/deals" && directDeals ? (() => {
        const page = toInt(route.query.get("page"), 1);
        const pageSize = toInt(route.query.get("pageSize"), 20);
        return (
          <section className="panel">
            <div className="panel-head">
              <h3>成交记录</h3>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Quote ID</th>
                    <th>Seed</th>
                    <th>Chunks</th>
                    <th>金额</th>
                    <th>买方</th>
                    <th>卖方</th>
                    <th>状态</th>
                    <th>时间</th>
                  </tr>
                </thead>
                <tbody>
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
                </tbody>
              </table>
            </div>
            <Pager total={directDeals.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
          </section>
        );
      })() : null}

      {route.path === "/direct/sessions" && directSessions ? (() => {
        const page = toInt(route.query.get("page"), 1);
        const pageSize = toInt(route.query.get("pageSize"), 20);
        return (
          <section className="panel">
            <div className="panel-head">
              <h3>会话管理</h3>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Deal ID</th>
                    <th>Seed</th>
                    <th>Chunk</th>
                    <th>买方</th>
                    <th>卖方</th>
                    <th>状态</th>
                    <th>传输量</th>
                    <th>开始时间</th>
                  </tr>
                </thead>
                <tbody>
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
                </tbody>
              </table>
            </div>
            <Pager total={directSessions.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
          </section>
        );
      })() : null}

      {route.path === "/finance/transfer-pools" && directTransferPools ? (() => {
        const page = toInt(route.query.get("page"), 1);
        const pageSize = toInt(route.query.get("pageSize"), 20);
        return (
          <section className="panel">
            <div className="panel-head">
              <h3>Direct资金池</h3>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Pool ID</th>
                    <th>买方</th>
                    <th>卖方</th>
                    <th>总额</th>
                    <th>已释放</th>
                    <th>状态</th>
                    <th>创建时间</th>
                  </tr>
                </thead>
                <tbody>
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
                </tbody>
              </table>
            </div>
            <Pager total={directTransferPools.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
          </section>
        );
      })() : null}

      {/* ========== Live 直播模块页面 ========== */}
      {route.path === "/live/streams" && liveStreams ? (
        <section className="panel">
          <div className="panel-head">
            <h3>直播流列表</h3>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Stream ID</th>
                  <th>文件数</th>
                  <th>总大小</th>
                  <th>最后更新</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {liveStreams.items.map((s) => (
                  <tr key={s.stream_id}>
                    <td title={s.stream_id}>{shortHex(s.stream_id, 8)}</td>
                    <td>{s.file_count}</td>
                    <td>{formatBytes(s.total_bytes)}</td>
                    <td>{t(s.last_updated_unix)}</td>
                    <td>
                      <button className="btn btn-light" onClick={() => handleLoadLiveStreamDetail(s.stream_id)}>
                        详情
                      </button>
                      <button className="btn btn-light" onClick={() => handleDeleteLiveStream(s.stream_id).catch((e) => alert(e.message))}>
                        删除
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {liveStreamDetail ? (
            <div className="panel" style={{ marginTop: 16 }}>
              <h4>Stream: {shortHex(liveStreamDetail.stream_id, 8)} 详情</h4>
              <div className="hint">
                文件数: {liveStreamDetail.file_count} | 总大小: {formatBytes(liveStreamDetail.total_bytes)}
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>路径</th>
                      <th>大小</th>
                      <th>修改时间</th>
                    </tr>
                  </thead>
                  <tbody>
                    {liveStreamDetail.segment_entries.slice(0, 20).map((e, i) => (
                      <tr key={i}>
                        <td title={e.path}>{short(e.path, 40)}</td>
                        <td>{formatBytes(e.file_size)}</td>
                        <td>{t(e.mtime_unix)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {liveStreamDetail.segment_entries.length > 20 && (
                <div className="hint">...还有 {liveStreamDetail.segment_entries.length - 20} 个文件</div>
              )}
            </div>
          ) : null}
        </section>
      ) : null}

      {route.path === "/live/follow" && liveFollowStatus ? (
        <section className="panel">
          <div className="panel-head">
            <h3>我的关注</h3>
          </div>
          <div className="stats-grid">
            <article className="stat">
              <p>状态</p>
              <h3>{liveFollowStatus.following ? "✅ 正在关注" : "⛔ 未关注"}</h3>
            </article>
            {liveFollowStatus.following && (
              <>
                <article className="stat">
                  <p>Stream ID</p>
                  <h3>{shortHex(liveFollowStatus.stream_id || "", 8)}</h3>
                </article>
                <article className="stat">
                  <p>开始时间</p>
                  <h3>{t(liveFollowStatus.started_at_unix || 0)}</h3>
                </article>
                <article className="stat">
                  <p>已接收</p>
                  <h3>{formatBytes(liveFollowStatus.bytes_received || 0)}</h3>
                </article>
              </>
            )}
          </div>
        </section>
      ) : null}

      {route.path === "/live/storage" && liveStorageSummary ? (
        <section className="panel">
          <div className="panel-head">
            <h3>Live 存储概览</h3>
          </div>
          <div className="stats-grid">
            <article className="stat">
              <p>直播流数量</p>
              <h3>{liveStorageSummary.stream_count}</h3>
            </article>
            <article className="stat">
              <p>文件总数</p>
              <h3>{liveStorageSummary.file_count.toLocaleString()}</h3>
            </article>
            <article className="stat">
              <p>占用空间</p>
              <h3>{formatBytes(liveStorageSummary.total_bytes)}</h3>
            </article>
          </div>
        </section>
      ) : null}

      {/* ========== Admin 管理模块页面 ========== */}
      {route.path === "/admin/downloads" ? (
        <section className="panel">
          <div className="panel-head">
            <h3>下载管理</h3>
          </div>
          <div className="hint">此功能需要后端 API 支持恢复下载。当前暂无专用 API。</div>
          <div className="filters" style={{ marginTop: 16 }}>
            <input className="input" placeholder="demand_id" id="resumeDemandId" />
            <button
              className="btn"
              onClick={() => {
                const demandId = (document.getElementById("resumeDemandId") as HTMLInputElement).value;
                if (demandId) handleResumeDownload(demandId);
              }}
            >
              恢复下载
            </button>
          </div>
        </section>
      ) : null}

      {route.path === "/admin/live" && liveStreams ? (
        <section className="panel">
          <div className="panel-head">
            <h3>Live 流管理</h3>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Stream ID</th>
                  <th>文件数</th>
                  <th>总大小</th>
                  <th>最后更新</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {liveStreams.items.map((s) => (
                  <tr key={s.stream_id}>
                    <td title={s.stream_id}>{shortHex(s.stream_id, 8)}</td>
                    <td>{s.file_count}</td>
                    <td>{formatBytes(s.total_bytes)}</td>
                    <td>{t(s.last_updated_unix)}</td>
                    <td>
                      <button className="btn btn-light" onClick={() => handleLoadLiveStreamDetail(s.stream_id)}>
                        详情
                      </button>
                      <button className="btn btn-light" onClick={() => handleDeleteLiveStream(s.stream_id).catch((e) => alert(e.message))}>
                        删除
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* 静态文件管理 */}
      {route.path === "/admin/static" && (
        <StaticFileManager
          staticTree={staticTree}
          staticPathHistory={staticPathHistory}
          staticCurrentPath={staticCurrentPath}
          selectedStaticItems={selectedStaticItems}
          onNavigate={navigateToStaticPath}
          onNavigateBack={navigateBack}
          onRefresh={() => loadStaticTree(staticCurrentPath)}
          onCreateDir={handleCreateStaticDir}
          onDelete={handleDeleteStaticEntry}
          onSetPrice={handleSetStaticItemPrice}
          onToggleSelection={toggleStaticItemSelection}
          onUpload={handleUploadStaticFile}
        />
      )}

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
              <input
                className="input"
                defaultValue={eventType}
                placeholder="event_type"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ event_type: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={signalType}
                placeholder="signal_type"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ signal_type: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={source}
                placeholder="source"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ source: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={gatewayPeerID}
                placeholder="网关 PeerID"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={taskStatus}
                placeholder="task_status"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ task_status: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                查询
              </button>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>开始时间</th>
                    <th>结束时间</th>
                    <th>步骤数</th>
                    <th>最新事件</th>
                    <th>命令类型</th>
                    <th>任务状态</th>
                    <th>来源</th>
                    <th>信号</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
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
                        <button className="btn btn-light" onClick={() => loadOrchestratorLogDetail(evt.event_id)}>
                          详情
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={orchestratorEvents.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

            <Modal
              title={orchestratorEventDetail ? `调度事件详情: ${orchestratorEventDetail.event_id}` : "调度事件详情"}
              isOpen={orchestratorModalOpen}
              onClose={() => setOrchestratorModalOpen(false)}
            >
              {orchestratorEventDetail && (
                <div style={{ display: "grid", gap: 12 }}>
                  <div className="detail-table-wrap">
                    <DetailTable
                      data={{
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
                        last_error_message: orchestratorEventDetail.last_error_message || "-",
                      }}
                    />
                  </div>
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>时间</th>
                          <th>事件</th>
                          <th>来源</th>
                          <th>信号</th>
                          <th>命令</th>
                          <th>状态</th>
                          <th>重试</th>
                          <th>队列</th>
                          <th>错误</th>
                        </tr>
                      </thead>
                      <tbody>
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
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
              <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                <button className="btn btn-light" onClick={() => setOrchestratorModalOpen(false)}>
                  关闭
                </button>
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
              <input
                className="input"
                defaultValue={commandType}
                placeholder="命令类型"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ command_type: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={gatewayPeerID}
                placeholder="网关 PeerID"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={status}
                placeholder="状态"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                查询
              </button>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>命令ID</th>
                    <th>类型</th>
                    <th>网关</th>
                    <th>聚合ID</th>
                    <th>状态</th>
                    <th>耗时</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
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
                        <button className="btn btn-light" onClick={() => loadClientKernelCommandDetail(cmd.id)}>
                          详情
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={clientKernelCommands.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

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
                <button className="btn btn-light" onClick={() => setClientKernelModalOpen(false)}>
                  关闭
                </button>
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
              <input
                className="input"
                defaultValue={commandType}
                placeholder="命令类型"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ command_type: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={gatewayPeerID}
                placeholder="网关 PeerID"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <input
                className="input"
                defaultValue={status}
                placeholder="状态"
                onKeyDown={(e) => {
                  if (e.key === "Enter") updateQuery({ status: (e.target as HTMLInputElement).value, page: 1 });
                }}
              />
              <button className="btn" onClick={() => updateQuery({ page: 1 })}>
                查询
              </button>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>时间</th>
                    <th>命令ID</th>
                    <th>类型</th>
                    <th>网关</th>
                    <th>状态</th>
                    <th>耗时</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {feePoolCommands.items.map((cmd) => (
                    <tr key={cmd.id}>
                      <td>{t(cmd.created_at_unix)}</td>
                      <td title={cmd.command_id}>{short(cmd.command_id, 8)}</td>
                      <td>{cmd.command_type}</td>
                      <td title={cmd.gateway_peer_id}>{short(cmd.gateway_peer_id, 8)}</td>
                      <td>{cmd.status}</td>
                      <td>{cmd.duration_ms}ms</td>
                      <td>
                        <button className="btn btn-light" onClick={() => loadFeePoolCommandDetail(cmd.id)}>
                          审计追踪
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pager total={feePoolCommands.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />

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
                        {tab === "command"
                          ? "命令详情"
                          : tab === "events"
                          ? `领域事件 (${feePoolEvents?.total || 0})`
                          : tab === "states"
                          ? `状态快照 (${feePoolStates?.total || 0})`
                          : `效果日志 (${feePoolEffects?.total || 0})`}
                      </button>
                    ))}
                  </div>
                  <div className="detail-table-wrap">
                    {feePoolAuditTab === "command" && <DetailTable data={feePoolCommandDetail} />}
                    {feePoolAuditTab === "events" && feePoolEvents && (
                      <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>时间</th>
                              <th>事件名</th>
                              <th>状态变更</th>
                            </tr>
                          </thead>
                          <tbody>
                            {feePoolEvents.items.map((e) => (
                              <tr key={e.id}>
                                <td>{t(e.created_at_unix)}</td>
                                <td>{e.event_name}</td>
                                <td>
                                  {e.state_before} → {e.state_after}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    {feePoolAuditTab === "states" && feePoolStates && (
                      <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>时间</th>
                              <th>状态</th>
                              <th>暂停原因</th>
                              <th>需求/拥有</th>
                            </tr>
                          </thead>
                          <tbody>
                            {feePoolStates.items.map((s) => (
                              <tr key={s.id}>
                                <td>{t(s.created_at_unix)}</td>
                                <td>{s.state}</td>
                                <td>{s.pause_reason || "-"}</td>
                                <td>
                                  {s.pause_need_satoshi} / {s.pause_have_satoshi}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                    {feePoolAuditTab === "effects" && feePoolEffects && (
                      <div className="table-wrap" style={{ maxHeight: 400, overflow: "auto" }}>
                        <table>
                          <thead>
                            <tr>
                              <th>时间</th>
                              <th>效果类型</th>
                              <th>阶段</th>
                              <th>状态</th>
                            </tr>
                          </thead>
                          <tbody>
                            {feePoolEffects.items.map((e) => (
                              <tr key={e.id}>
                                <td>{t(e.created_at_unix)}</td>
                                <td>{e.effect_type}</td>
                                <td>{e.stage}</td>
                                <td>{e.status}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </>
              )}
              <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                <button className="btn btn-light" onClick={() => setFeePoolAuditModalOpen(false)}>
                  关闭
                </button>
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
            <button
              className="btn"
              onClick={() =>
                setWorkspaceEditing({ id: null, data: { path: "", max_bytes: 10737418240, enabled: true } })
              }
            >
              添加工作区
            </button>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>路径</th>
                  <th>容量上限</th>
                  <th>已用空间</th>
                  <th>状态</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {workspaces.items.map((w) => (
                  <tr key={w.id}>
                    <td>{w.id}</td>
                    <td title={w.path}>{w.path}</td>
                    <td>{formatBytes(w.max_bytes)}</td>
                    <td>{formatBytes(w.used_bytes)}</td>
                    <td>{w.enabled ? "✅ 启用" : "⛔ 禁用"}</td>
                    <td>
                      <button className="btn btn-light" onClick={() => setWorkspaceEditing({ id: w.id, data: { ...w } })}>
                        编辑
                      </button>
                      {!w.enabled && (
                        <button className="btn btn-light" onClick={() => handleDeleteWorkspace(w.id).catch((e) => alert(e.message))}>
                          删除
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <Modal
            title={workspaceEditing?.id === null ? "添加工作区" : "编辑工作区"}
            isOpen={!!workspaceEditing}
            onClose={() => setWorkspaceEditing(null)}
          >
            {workspaceEditing && (
              <>
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                    <input
                      type="checkbox"
                      checked={workspaceEditing.data.enabled}
                      onChange={(e) =>
                        setWorkspaceEditing({
                          ...workspaceEditing,
                          data: { ...workspaceEditing.data, enabled: e.target.checked },
                        })
                      }
                    />
                    <span>启用</span>
                  </label>
                  <input
                    className="input"
                    placeholder="路径"
                    value={workspaceEditing.data.path || ""}
                    onChange={(e) =>
                      setWorkspaceEditing({
                        ...workspaceEditing,
                        data: { ...workspaceEditing.data, path: e.target.value },
                      })
                    }
                  />
                  <input
                    className="input"
                    type="number"
                    placeholder="容量上限 (bytes)"
                    value={workspaceEditing.data.max_bytes || ""}
                    onChange={(e) =>
                      setWorkspaceEditing({
                        ...workspaceEditing,
                        data: { ...workspaceEditing.data, max_bytes: Number(e.target.value) },
                      })
                    }
                  />
                </div>
                <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
                  <button
                    className="btn"
                    onClick={() => {
                      handleSaveWorkspace(workspaceEditing.id, workspaceEditing.data)
                        .then(() => setWorkspaceEditing(null))
                        .catch((e) => alert(e.message));
                    }}
                  >
                    保存
                  </button>
                  <button className="btn btn-light" onClick={() => setWorkspaceEditing(null)}>
                    取消
                  </button>
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
            <div className="panel-head">
              <h3>系统配置</h3>
            </div>
            <div className="hint" style={{ marginBottom: 12 }}>
              说明：`listen.auto_renew_rounds` 为统一配置项，不区分测试网/主网；`index.*` 为系统派生值，不在此页面开放编辑。
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>配置项</th>
                    <th>当前值</th>
                    <th>类型</th>
                    <th>范围/说明</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {adminConfigSchema.items.map((schema) => {
                    const currentValue = adminConfig[schema.key];
                    const draftValue = configDrafts[schema.key] ?? String(currentValue ?? "");
                    return (
                      <tr key={schema.key}>
                        <td title={schema.key}>
                          <code>{schema.key}</code>
                        </td>
                        <td>
                          <code>{String(currentValue)}</code>
                        </td>
                        <td>{schema.type}</td>
                        <td>
                          {schema.type === "int" && schema.min_int !== undefined && schema.max_int !== undefined
                            ? `${schema.min_int} ~ ${schema.max_int}`
                            : schema.type === "float" && schema.min_float !== undefined && schema.max_float !== undefined
                            ? `${schema.min_float} ~ ${schema.max_float}`
                            : schema.description || "-"}
                        </td>
                        <td>
                          <div className="filters">
                            <input
                              className="input small"
                              value={draftValue}
                              onChange={(e) => setConfigDrafts({ ...configDrafts, [schema.key]: e.target.value })}
                            />
                            <button
                              className="btn btn-light"
                              onClick={() => {
                                let value: unknown = draftValue;
                                if (schema.type === "int") value = Number(draftValue);
                                else if (schema.type === "float") value = Number(draftValue);
                                else if (schema.type === "bool") value = draftValue === "true";
                                handleSaveAdminConfig(schema.key, value);
                              }}
                            >
                              保存
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {/* 策略调试日志开关 */}
            <div className="panel" style={{ marginTop: 16 }}>
              <div className="panel-head">
                <h4>调试设置</h4>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 0" }}>
                <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                  <input
                    type="checkbox"
                    checked={strategyDebugLogEnabled ?? false}
                    disabled={strategyDebugLogLoading}
                    onChange={(e) => handleSetStrategyDebugLog(e.target.checked)}
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
    </MainLayout>
  );
}
