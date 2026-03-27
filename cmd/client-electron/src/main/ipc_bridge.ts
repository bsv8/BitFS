import { BrowserWindow, dialog, ipcMain, webContents } from "electron";
import fs from "node:fs/promises";
import path from "node:path";

import type { BitfsBrowserRuntime } from "./browser_runtime";
import type { BrowserSettingsStore } from "./browser_settings";
import type { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import type { ElectronE2EViewerPolicyStore } from "./e2e_controller";
import type { ElectronE2EObserver } from "./e2e_observer";
import type { BitfsPublicClientStatus, BitfsRuntimeEvent, KeyFileActionResult, ShellErrorReport, ShellState } from "../shared/shell_contract";
import type { ShellAssetPaths } from "./shell_assets";

export function registerShellIPC(
  window: BrowserWindow,
  runtime: BitfsBrowserRuntime,
  supervisor: ManagedClientSupervisor,
  settings: BrowserSettingsStore,
  shellAssets: ShellAssetPaths,
  e2eViewerPolicy?: ElectronE2EViewerPolicyStore | null,
  e2eObserver?: ElectronE2EObserver | null
): void {
  const buildShellState = (): ShellState => {
    const runtimeState = runtime.snapshot();
    const backendState = supervisor.snapshot();
    const settingsState = settings.snapshot();
    return {
      ...runtimeState,
      lastError: runtimeState.lastError || backendState.lastError,
      settingsPageURL: shellAssets.settingsPageURL,
      settingsPreloadPath: shellAssets.settingsPreloadPath,
      userHomeSeedHash: settingsState.userHomeSeedHash,
      sidebarWidthPx: settingsState.sidebarWidthPx,
      activePanel: settingsState.activePanel,
      backend: backendState
    };
  };
  const sendState = (state: ShellState) => {
    broadcastToAllRenderers("bitfs-shell:state", state);
    broadcastToAllRenderers("bitfs-viewer:state", buildPublicClientStatus(state));
  };
  const emitLatestState = () => {
    sendState(buildShellState());
  };
  const emitShellErrorReport = (report: ShellErrorReport) => {
    if (window.isDestroyed() || window.webContents.isDestroyed()) {
      return;
    }
    try {
      window.webContents.send("bitfs-shell:error-report", report);
    } catch {
      // 设计说明：
      // - 错误面板只是辅助沟通链路，窗口如果正在销毁，不该再因为二次发送失败制造额外噪音；
      // - 主错误已经写进 debug log，这里保持 best effort 即可。
    }
  };
  runtime.on("state", emitLatestState);
  runtime.on("event", (event: BitfsRuntimeEvent) => {
    broadcastPrivateEvent(event);
  });
  supervisor.on("state", emitLatestState);
  supervisor.on("event", (event: BitfsRuntimeEvent) => {
    if (event.topic === "wallet.changed") {
      void runtime.refreshVisitAccounting(undefined, "wallet.changed");
    }
    if (event.scope === "public") {
      broadcastPublicEvent(event);
      // 设计说明：
      // - 壳页面属于受信上下文，除了私有事件，也应该能看到公开事件；
      // - 这样壳与 `window.bitfs` 可以共享同一类公开变化信号，避免 topic 一致但壳收不到。
      broadcastPrivateEvent(event);
      return;
    }
    broadcastPrivateEvent(event);
  });
  settings.on("change", emitLatestState);

  ipcMain.handle("bitfs-shell:get-state", () => buildShellState());
  ipcMain.handle("bitfs-shell:open", (_event, rawLocator: string) => {
    debugLogger.log("ipc", "open", { locator: rawLocator });
    return runtime.open(String(rawLocator || ""));
  });
  ipcMain.handle("bitfs-shell:set-budget", (_event, payload: { singleMaxSat: number; pageMaxSat: number }) => {
    debugLogger.log("ipc", "set_budget", payload);
    runtime.setStaticBudget(payload.singleMaxSat, payload.pageMaxSat);
    return buildShellState();
  });
  ipcMain.handle("bitfs-shell:approve-resource", (_event, payload: { seedHash: string }) => {
    debugLogger.log("ipc", "approve_resource", {
      seed_hash: String(payload?.seedHash || "")
    });
    runtime.approveResource(String(payload?.seedHash || ""));
    return buildShellState();
  });
  ipcMain.handle("bitfs-shell:create-key", (_event, payload: { password: string }) => {
    debugLogger.log("ipc", "create_key");
    return supervisor.createKey(String(payload?.password || "")).then(() => buildShellState());
  });
  ipcMain.handle("bitfs-shell:import-key-file", async () => {
    const result = await importKeyFile(window, supervisor, buildShellState);
    return result;
  });
  ipcMain.handle("bitfs-shell:export-key-file", async () => {
    const result = await exportKeyFile(window, supervisor, buildShellState);
    return result;
  });
  ipcMain.handle("bitfs-shell:unlock", (_event, payload: { password: string }) => {
    debugLogger.log("ipc", "unlock");
    return supervisor.unlock(String(payload?.password || "")).then(() => buildShellState());
  });
  ipcMain.handle("bitfs-shell:lock", () => {
    debugLogger.log("ipc", "lock");
    return supervisor.lock().then(() => buildShellState());
  });
  ipcMain.handle("bitfs-shell:restart-backend", () => {
    debugLogger.log("ipc", "restart_backend");
    return supervisor.restart().then(() => buildShellState());
  });
  ipcMain.handle("bitfs-shell:force-close-window", () => {
    debugLogger.log("ipc", "force_close_window", {
      window_id: window.id
    });
    if (!window.isDestroyed()) {
      window.destroy();
    }
    return true;
  });
  ipcMain.handle("bitfs-shell:set-user-homepage", (_event, payload: { seedHash: string }) => {
    debugLogger.log("ipc", "set_user_homepage", {
      seed_hash: String(payload?.seedHash || "")
    });
    settings.setUserHomeSeedHash(String(payload?.seedHash || ""));
    return buildShellState();
  });
  ipcMain.handle("bitfs-shell:clear-user-homepage", () => {
    debugLogger.log("ipc", "clear_user_homepage");
    settings.clearUserHomeSeedHash();
    return buildShellState();
  });
  ipcMain.handle("bitfs-shell:set-sidebar-layout", (_event, payload: { sidebarWidthPx?: number; activePanel?: string }) => {
    debugLogger.log("ipc", "set_sidebar_layout", {
      sidebar_width_px: Number(payload?.sidebarWidthPx || 0),
      active_panel: String(payload?.activePanel || "")
    });
    settings.setSidebarLayout({
      sidebarWidthPx: payload?.sidebarWidthPx,
      activePanel: payload?.activePanel
    });
    return buildShellState();
  });
  ipcMain.handle("bitfs-shell:plan-resources", (_event, payload: { refs: string[]; baseURL?: string }) => {
    debugLogger.log("ipc", "plan_resources", {
      ref_count: Array.isArray(payload?.refs) ? payload.refs.length : 0,
      base_url: String(payload?.baseURL || "")
    });
    return runtime.planRefs(Array.isArray(payload?.refs) ? payload.refs : [], String(payload?.baseURL || ""));
  });
  ipcMain.handle("bitfs-shell:ensure-resource", (_event, payload: { ref: string; baseURL?: string; maxTotalSat?: number }) => {
    debugLogger.log("ipc", "ensure_resource", {
      ref: String(payload?.ref || ""),
      base_url: String(payload?.baseURL || ""),
      max_total_sat: Number(payload?.maxTotalSat || 0)
    });
    const seedHash = runtime.resolveRef(String(payload?.ref || ""), String(payload?.baseURL || ""));
    return runtime.ensureSeed(seedHash, Number(payload?.maxTotalSat || 0));
  });
  ipcMain.handle("bitfs-shell:file-status", (_event, payload: { ref: string; baseURL?: string }) => {
    debugLogger.log("ipc", "file_status", {
      ref: String(payload?.ref || ""),
      base_url: String(payload?.baseURL || "")
    });
    const seedHash = runtime.resolveRef(String(payload?.ref || ""), String(payload?.baseURL || ""));
    return runtime.getFileStatus(seedHash);
  });
  ipcMain.handle("bitfs-shell:client-info", () => {
    debugLogger.log("ipc", "client_info");
    return runtime.getClientInfo();
  });
  ipcMain.handle("bitfs-shell:wallet-summary", () => {
    debugLogger.log("ipc", "wallet_summary");
    return runtime.getWalletSummary();
  });
  // 设计说明：
  // - `window.bitfs` 是给外部 bitfs 页面用的公开协议，不能把壳层内部状态原样透出去；
  // - 因此 viewer 走独立的 IPC 名字空间，只返回已经裁剪过的公开画像字段。
  ipcMain.handle("bitfs-viewer:client-info", () => {
    debugLogger.log("ipc", "viewer_client_info");
    return runtime.getPublicClientInfo();
  });
  ipcMain.handle("bitfs-viewer:client-status", () => {
    debugLogger.log("ipc", "viewer_client_status");
    return buildPublicClientStatus(buildShellState());
  });
  ipcMain.handle("bitfs-viewer:wallet-summary", () => {
    debugLogger.log("ipc", "viewer_wallet_summary");
    return runtime.getPublicWalletSummary();
  });
  ipcMain.handle("bitfs-viewer:wallet-addresses", () => {
    debugLogger.log("ipc", "viewer_wallet_addresses");
    return runtime.getPublicWalletAddresses();
  });
  ipcMain.handle("bitfs-viewer:wallet-history", (_event, payload: { limit?: number; offset?: number; direction?: string }) => {
    debugLogger.log("ipc", "viewer_wallet_history", {
      limit: Number(payload?.limit || 0),
      offset: Number(payload?.offset || 0),
      direction: String(payload?.direction || "")
    });
    return runtime.listPublicWalletHistory(payload || {});
  });
  ipcMain.handle("bitfs-viewer:wallet-sign-business", async (_event, payload: { signer_pubkey_hex?: string; signed_envelope?: unknown }) => {
    const normalized = normalizeViewerWalletBusinessRequest(payload);
    debugLogger.log("ipc", "viewer_wallet_sign_business", {
      signer_pubkey_hex: normalized.signer_pubkey_hex,
      signed_envelope_bytes: estimateViewerWalletBusinessBytes(normalized)
    });
    const preview = await supervisor.requestManagedJSON<{
      ok?: boolean;
      code?: string;
      message?: string;
      preview?: {
        can_sign?: boolean;
        summary?: string;
        detail_lines?: string[];
        preview_hash?: string;
        business_title?: string;
        warning_level?: string;
      };
    }>({
      method: "POST",
      pathname: "/api/v1/wallet/business/preview",
      body: normalized,
      timeout_ms: 30_000,
      headers: runtime.getCurrentVisitHeaders()
    });
    const previewBody = preview?.preview;
    if (!preview || preview.ok !== true || !previewBody) {
      return {
        ok: false,
        code: String(preview?.code || "PREVIEW_FAILED"),
        message: String(preview?.message || "wallet business preview failed"),
        preview: previewBody || null
      };
    }
    const approved = await confirmViewerWalletBusiness(window, {
      title: String(previewBody.business_title || "钱包签名请求"),
      summary: String(previewBody.summary || ""),
      detailLines: Array.isArray(previewBody.detail_lines) ? previewBody.detail_lines.map((item) => String(item || "")) : [],
      canSign: Boolean(previewBody.can_sign),
      warningLevel: String(previewBody.warning_level || "")
    }, e2eViewerPolicy || null);
    if (!approved) {
      return {
        ok: false,
        code: "USER_DENIED",
        message: "wallet signing denied",
        preview: previewBody
      };
    }
    if (!previewBody.can_sign) {
      return {
        ok: false,
        code: "UNKNOWN_TEMPLATE",
        message: "wallet business template not found",
        preview: previewBody
      };
    }
    return supervisor.requestManagedJSON({
      method: "POST",
      pathname: "/api/v1/wallet/business/sign",
      body: {
        ...normalized,
        expected_preview_hash: String(previewBody.preview_hash || "")
      },
      timeout_ms: 30_000,
      headers: runtime.getCurrentVisitHeaders()
    });
  });
  ipcMain.handle("bitfs-viewer:peer-call", (_event, payload: { to?: string; route?: string; content_type?: string; body?: unknown; body_base64?: string; payment_mode?: string; payment_quote_base64?: string }) => {
    const normalized = normalizeViewerPeerCallRequest(payload);
    debugLogger.log("ipc", "viewer_peer_call", {
      to: normalized.to,
      route: normalized.route,
      content_type: normalized.content_type,
      has_body_json: Object.prototype.hasOwnProperty.call(normalized, "body"),
      has_body_base64: Object.prototype.hasOwnProperty.call(normalized, "body_base64")
    });
    return confirmViewerPeerCall(window, normalized, e2eViewerPolicy || null).then((approved) => {
      if (!approved) {
        debugLogger.log("ipc", "viewer_peer_call_denied", {
          to: normalized.to,
          route: normalized.route,
          content_type: normalized.content_type
        });
        return {
          ok: false,
          code: "USER_DENIED",
          message: "peer call permission denied"
        };
      }
      debugLogger.log("ipc", "viewer_peer_call_approved", {
        to: normalized.to,
        route: normalized.route,
        content_type: normalized.content_type
      });
      return supervisor.requestManagedJSON({
        method: "POST",
        pathname: "/api/v1/call",
        body: normalized,
        timeout_ms: 30_000,
        headers: runtime.getCurrentVisitHeaders()
      });
    });
  });
  ipcMain.handle("bitfs-viewer:locator-resolve", (_event, payload: { locator?: string }) => {
    const normalized = normalizeViewerLocatorResolveRequest(payload);
    debugLogger.log("ipc", "viewer_locator_resolve", normalized);
    return runtime.resolveLocator(normalized.locator);
  });
  ipcMain.handle("bitfs-shell:live-latest", (_event, payload: { streamID: string }) => {
    debugLogger.log("ipc", "live_latest", {
      stream_id: String(payload?.streamID || "")
    });
    return runtime.getLiveLatest(String(payload?.streamID || ""));
  });
  ipcMain.handle("bitfs-shell:live-plan", (_event, payload: { streamID: string; haveSegmentIndex: number }) => {
    debugLogger.log("ipc", "live_plan", {
      stream_id: String(payload?.streamID || ""),
      have_segment_index: Number(payload?.haveSegmentIndex || 0)
    });
    return runtime.getLivePlan(String(payload?.streamID || ""), Number(payload?.haveSegmentIndex || 0));
  });
  ipcMain.on("bitfs-shell:did-navigate", (_event, url: string) => {
    debugLogger.log("ipc", "did_navigate", {
      url
    });
    runtime.noteNavigation(url);
  });
  ipcMain.on("bitfs-shell:debug-log", (_event, payload: { scope?: string; event?: string; fields?: Record<string, unknown> }) => {
    debugLogger.log(`renderer.${String(payload?.scope || "shell")}`, String(payload?.event || "log"), payload?.fields);
  });
  ipcMain.on("bitfs-shell:e2e-event", (_event, payload: unknown) => {
    const event = normalizeE2EEventPayload(payload);
    debugLogger.log("shell_e2e", event.name, event.fields);
    e2eObserver?.emit("shell-renderer", event.name, event.fields);
  });
  ipcMain.on("bitfs-shell:report-error", (_event, payload: unknown) => {
    const report = normalizeShellErrorReport(payload);
    debugLogger.log("shell_error", "renderer_reported", {
      source: report.source,
      title: report.title,
      page_url: report.page_url,
      can_stop_current_page: report.can_stop_current_page,
      message: report.message
    });
    e2eObserver?.emit(report.source === "viewer" ? "viewer-preload" : "shell-renderer", "shell.error.reported", {
      report_source: report.source,
      report_title: report.title,
      report_message: report.message,
      report_page_url: report.page_url,
      can_stop_current_page: report.can_stop_current_page
    });
    emitShellErrorReport(report);
  });
  ipcMain.on("bitfs-viewer:e2e-event", (_event, payload: unknown) => {
    const event = normalizeE2EEventPayload(payload);
    debugLogger.log("viewer_e2e", event.name, event.fields);
    e2eObserver?.emit("viewer-page", event.name, event.fields);
  });
  ipcMain.handle("bitfs-settings:request", (_event, payload: { method?: string; path?: string; body?: unknown }) => {
    const normalized = normalizeManagedSettingsRequest(payload);
    debugLogger.log("ipc", "settings_request", {
      method: normalized.method,
      path: normalized.path
    });
    return supervisor.requestManagedJSON({
      method: normalized.method,
      pathname: normalized.path,
      body: normalized.body
    });
  });
  ipcMain.handle("bitfs-settings:pick-directory", async () => {
    const picked = await dialog.showOpenDialog(window, {
      title: "选择工作区目录",
      properties: ["openDirectory", "createDirectory"]
    });
    if (picked.canceled || picked.filePaths.length === 0) {
      return { cancelled: true, path: "" };
    }
    return {
      cancelled: false,
      path: path.resolve(picked.filePaths[0])
    };
  });
  ipcMain.handle("bitfs-settings:upload-static-file", async (_event, payload: { targetDir?: string; overwrite?: boolean }) => {
    const picked = await dialog.showOpenDialog(window, {
      title: "选择要上传的静态文件",
      properties: ["openFile"]
    });
    if (picked.canceled || picked.filePaths.length === 0) {
      return { cancelled: true, path: "", result: null };
    }
    const filePath = path.resolve(picked.filePaths[0]);
    const result = await supervisor.uploadStaticFile<Record<string, unknown>>({
      filePath,
      targetDir: String(payload?.targetDir || "/"),
      overwrite: Boolean(payload?.overwrite)
    });
    return {
      cancelled: false,
      path: filePath,
      result
    };
  });
}

function broadcastPrivateEvent(event: BitfsRuntimeEvent): void {
  broadcastToAllRenderers("bitfs-shell:event", event);
}

function broadcastPublicEvent(event: BitfsRuntimeEvent): void {
  broadcastToAllRenderers("bitfs-viewer:event", event);
}

function broadcastToAllRenderers(channel: string, payload: unknown): void {
  for (const contents of webContents.getAllWebContents()) {
    if (contents.isDestroyed()) {
      continue;
    }
    try {
      contents.send(channel, payload);
    } catch {
      // 某些 DevTools / 已卸载 guest 会抛异常，这里按广播 best effort 处理。
    }
  }
}

function buildPublicClientStatus(state: ShellState): BitfsPublicClientStatus {
  return {
    trusted_protocol: "bitfs://",
    current_url: String(state.currentURL || ""),
    current_root_seed_hash: String(state.currentRootSeedHash || ""),
    wallet_ready: state.backend.phase === "ready" && state.backend.unlocked,
    wallet_unlocked: Boolean(state.backend.unlocked)
  };
}

function normalizeShellErrorReport(payload: unknown): ShellErrorReport {
  const input = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload as Record<string, unknown>
    : {};
  const source = normalizeShellErrorSource(String(input.source || ""));
  const title = String(input.title || "").trim() || defaultShellErrorTitle(source);
  const message = String(input.message || "").trim() || "unknown error";
  return {
    source,
    title,
    message,
    detail: String(input.detail || "").trim(),
    page_url: String(input.page_url || "").trim(),
    occurred_at_unix: Math.max(0, Math.floor(Number(input.occurred_at_unix || 0))) || Math.floor(Date.now() / 1000),
    can_stop_current_page: Boolean(input.can_stop_current_page) && (source === "viewer" || source === "settings")
  };
}

function normalizeShellErrorSource(raw: string): ShellErrorReport["source"] {
  const value = String(raw || "").trim();
  if (value === "viewer" || value === "settings" || value === "shell-renderer") {
    return value;
  }
  return "main-process";
}

function defaultShellErrorTitle(source: ShellErrorReport["source"]): string {
  if (source === "viewer") {
    return "当前页面 JS 错误";
  }
  if (source === "settings") {
    return "设置页 JS 错误";
  }
  if (source === "shell-renderer") {
    return "壳页面 JS 错误";
  }
  return "主进程 JS 错误";
}

function normalizeE2EEventPayload(payload: unknown): { name: string; fields: Record<string, unknown> } {
  const input = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload as Record<string, unknown>
    : {};
  const name = String(input.name || "").trim() || "unknown";
  const rawFields = input.fields;
  const fields = rawFields && typeof rawFields === "object" && !Array.isArray(rawFields)
    ? { ...(rawFields as Record<string, unknown>) }
    : {};
  return { name, fields };
}

async function importKeyFile(
  window: BrowserWindow,
  supervisor: ManagedClientSupervisor,
  buildShellState: () => ShellState
): Promise<KeyFileActionResult> {
  const picked = await dialog.showOpenDialog(window, {
    title: "导入密文私钥",
    properties: ["openFile"],
    filters: [{ name: "JSON", extensions: ["json"] }]
  });
  if (picked.canceled || picked.filePaths.length === 0) {
    return { cancelled: true, filePath: "", state: buildShellState() };
  }
  const filePath = path.resolve(picked.filePaths[0]);
  const raw = await fs.readFile(filePath, "utf8");
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("invalid encrypted key json");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("invalid encrypted key json");
  }
  const cipher = Object.prototype.hasOwnProperty.call(parsed, "cipher")
    ? (parsed as { cipher?: unknown }).cipher
    : parsed;
  if (!cipher || typeof cipher !== "object" || Array.isArray(cipher)) {
    throw new Error("invalid encrypted key json");
  }
  await supervisor.importKeyCipher(cipher as Record<string, unknown>);
  return { cancelled: false, filePath, state: buildShellState() };
}

async function exportKeyFile(
  window: BrowserWindow,
  supervisor: ManagedClientSupervisor,
  buildShellState: () => ShellState
): Promise<KeyFileActionResult> {
  const picked = await dialog.showSaveDialog(window, {
    title: "导出密文私钥",
    defaultPath: "bitfs-encrypted-key.json",
    filters: [{ name: "JSON", extensions: ["json"] }]
  });
  if (picked.canceled || !picked.filePath) {
    return { cancelled: true, filePath: "", state: buildShellState() };
  }
  const filePath = path.resolve(picked.filePath);
  const cipher = await supervisor.exportKeyCipher();
  const payload = JSON.stringify({ cipher }, null, 2) + "\n";
  await fs.writeFile(filePath, payload, { encoding: "utf8", mode: 0o600 });
  return { cancelled: false, filePath, state: buildShellState() };
}

type NormalizedSettingsRequest = {
  method: "GET" | "POST" | "PUT" | "DELETE";
  path: string;
  body?: unknown;
};

const SETTINGS_ALLOWED_PATHS = [
  "/api/v1/info",
  "/api/v1/wallet/summary",
  // 设计说明：
  // - settings 账务页和侧栏访问概述要看同一批后台流水；
  // - 因此设置页必须能读取 wallet_fund_flows 列表与详情，不能只看 summary。
  "/api/v1/wallet/fund-flows",
  "/api/v1/wallet/fund-flows/detail",
  "/api/v1/gateways",
  "/api/v1/gateways/master",
  "/api/v1/gateways/health",
  "/api/v1/arbiters",
  "/api/v1/arbiters/health",
  "/api/v1/admin/config",
  "/api/v1/admin/workspaces",
  "/api/v1/admin/static/tree",
  "/api/v1/admin/static/mkdir",
  "/api/v1/admin/static/move",
  "/api/v1/admin/static/entry",
  "/api/v1/admin/static/price/set",
  "/api/v1/admin/static/price"
] as const;

function normalizeManagedSettingsRequest(payload: { method?: string; path?: string; body?: unknown }): NormalizedSettingsRequest {
  const method = normalizeManagedSettingsMethod(payload?.method);
  const pathValue = String(payload?.path || "").trim();
  if (pathValue === "") {
    throw new Error("managed api path is required");
  }
  if (!pathValue.startsWith("/")) {
    throw new Error("managed api path must start with /");
  }
  if (pathValue.includes("://")) {
    throw new Error("absolute url is not allowed");
  }
  const url = new URL(pathValue, "http://bitfs-shell.local");
  if (url.origin !== "http://bitfs-shell.local") {
    throw new Error("invalid managed api path");
  }
  const normalizedPath = `${url.pathname}${url.search}`;
  const allowed = SETTINGS_ALLOWED_PATHS.some((item) => url.pathname === item || url.pathname.startsWith(`${item}/`));
  if (!allowed) {
    throw new Error("managed api path is not allowed");
  }
  return {
    method,
    path: normalizedPath,
    body: payload?.body
  };
}

function normalizeManagedSettingsMethod(raw: string | undefined): "GET" | "POST" | "PUT" | "DELETE" {
  const method = String(raw || "GET").trim().toUpperCase();
  if (method === "POST" || method === "PUT" || method === "DELETE") {
    return method;
  }
  return "GET";
}

function normalizeViewerPeerCallRequest(payload: { to?: string; route?: string; content_type?: string; body?: unknown; body_base64?: string; payment_mode?: string; payment_quote_base64?: string }): {
  to: string;
  route: string;
  content_type: string;
  body?: unknown;
  body_base64?: string;
  payment_mode?: string;
  payment_quote_base64?: string;
} {
  const to = String(payload?.to || "").trim();
  if (to === "") {
    throw new Error("bitfs target is required");
  }
  const route = String(payload?.route || "").trim();
  if (route === "") {
    throw new Error("bitfs route is required");
  }
  const contentType = String(payload?.content_type || "").trim();
  if (contentType === "") {
    throw new Error("bitfs content_type is required");
  }
  const out: {
    to: string;
    route: string;
    content_type: string;
    body?: unknown;
    body_base64?: string;
    payment_mode?: string;
    payment_quote_base64?: string;
  } = {
    to,
    route,
    content_type: contentType
  };
  const bodyBase64 = String(payload?.body_base64 || "").trim();
  if (bodyBase64 !== "") {
    out.body_base64 = bodyBase64;
    return out;
  }
  if (Object.prototype.hasOwnProperty.call(payload || {}, "body")) {
    out.body = payload?.body;
  }
  const paymentMode = String(payload?.payment_mode || "").trim().toLowerCase();
  if (paymentMode === "quote" || paymentMode === "pay") {
    out.payment_mode = paymentMode;
  }
  const paymentQuoteBase64 = String(payload?.payment_quote_base64 || "").trim();
  if (paymentQuoteBase64 !== "") {
    out.payment_quote_base64 = paymentQuoteBase64;
  }
  return out;
}

function normalizeViewerLocatorResolveRequest(payload: { locator?: string }): { locator: string } {
  const locator = String(payload?.locator || "").trim();
  if (locator === "") {
    throw new Error("bitfs locator is required");
  }
  return { locator };
}

function normalizeViewerWalletBusinessRequest(payload: { signer_pubkey_hex?: string; signed_envelope?: unknown }): {
  signer_pubkey_hex: string;
  signed_envelope: unknown;
} {
  const signerPubkeyHex = String(payload?.signer_pubkey_hex || "").trim().toLowerCase();
  if (signerPubkeyHex === "") {
    throw new Error("signer_pubkey_hex is required");
  }
  if (!Object.prototype.hasOwnProperty.call(payload || {}, "signed_envelope")) {
    throw new Error("signed_envelope is required");
  }
  return {
    signer_pubkey_hex: signerPubkeyHex,
    signed_envelope: payload?.signed_envelope
  };
}

// 设计说明：
// - peer.call 会让网页驱动本地身份向外部节点发消息，风险明显高于 locator.resolve；
// - 这里先走壳弹框逐次确认，后续如果加侧栏策略，也可以复用同一摘要口径。
async function confirmViewerPeerCall(
  window: BrowserWindow,
  payload: { to: string; route: string; content_type: string; body?: unknown; body_base64?: string; payment_mode?: string; payment_quote_base64?: string },
  e2eViewerPolicy: ElectronE2EViewerPolicyStore | null
): Promise<boolean> {
  const policy = e2eViewerPolicy?.snapshot();
  if (policy?.auto_approve_peer_call) {
    debugLogger.log("ipc", "viewer_peer_call_auto_approved", {
      to: payload.to,
      route: payload.route,
      content_type: payload.content_type
    });
    return true;
  }
  const detailLines = [
    `目标: ${payload.to}`,
    `路由: ${payload.route}`,
    `类型: ${payload.content_type}`,
    `大小: ${estimateViewerPeerCallBodyBytes(payload)} bytes`
  ];
  const result = await dialog.showMessageBox(window, {
    type: "question",
    buttons: ["允许发送", "取消"],
    defaultId: 1,
    cancelId: 1,
    noLink: true,
    title: "确认页面发消息",
    message: "当前页面请求使用 window.bitfs.peer.call 向外部节点发送消息。",
    detail: detailLines.join("\n")
  });
  return result.response === 0;
}

async function confirmViewerWalletBusiness(
  window: BrowserWindow,
  payload: { title: string; summary: string; detailLines: string[]; canSign: boolean; warningLevel: string },
  e2eViewerPolicy: ElectronE2EViewerPolicyStore | null
): Promise<boolean> {
  const canSign = payload.canSign;
  const policy = e2eViewerPolicy?.snapshot();
  if (policy?.auto_approve_wallet_business) {
    debugLogger.log("ipc", "viewer_wallet_business_auto_approved", {
      can_sign: canSign,
      warning_level: payload.warningLevel,
      title: payload.title
    });
    return canSign;
  }
  const result = await dialog.showMessageBox(window, {
    type: canSign ? "question" : "warning",
    buttons: canSign ? ["生成签名交易", "取消"] : ["关闭"],
    defaultId: canSign ? 1 : 0,
    cancelId: canSign ? 1 : 0,
    noLink: true,
    title: canSign ? "确认钱包签名请求" : "未知交易请求",
    message: String(payload.summary || (canSign ? "页面请求钱包生成一笔签名交易。" : "未知交易请求，高度注意")),
    detail: payload.detailLines.join("\n")
  });
  return canSign && result.response === 0;
}

function estimateViewerPeerCallBodyBytes(payload: { body?: unknown; body_base64?: string }): number {
  const bodyBase64 = String(payload.body_base64 || "").trim();
  if (bodyBase64 !== "") {
    try {
      return Buffer.from(bodyBase64, "base64").byteLength;
    } catch {
      return 0;
    }
  }
  if (!Object.prototype.hasOwnProperty.call(payload, "body")) {
    return 0;
  }
  try {
    return Buffer.byteLength(JSON.stringify(payload.body ?? null), "utf8");
  } catch {
    return 0;
  }
}

function estimateViewerWalletBusinessBytes(payload: { signed_envelope?: unknown }): number {
  if (!Object.prototype.hasOwnProperty.call(payload, "signed_envelope")) {
    return 0;
  }
  try {
    return Buffer.byteLength(JSON.stringify(payload.signed_envelope ?? null), "utf8");
  } catch {
    return 0;
  }
}
