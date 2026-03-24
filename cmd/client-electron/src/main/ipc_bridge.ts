import { BrowserWindow, dialog, ipcMain, webContents } from "electron";
import fs from "node:fs/promises";
import path from "node:path";

import type { BitfsBrowserRuntime } from "./browser_runtime";
import type { BrowserSettingsStore } from "./browser_settings";
import type { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import type { BitfsPublicClientStatus, BitfsRuntimeEvent, KeyFileActionResult, ShellState } from "../shared/shell_contract";
import type { ShellAssetPaths } from "./shell_assets";

export function registerShellIPC(
  window: BrowserWindow,
  runtime: BitfsBrowserRuntime,
  supervisor: ManagedClientSupervisor,
  settings: BrowserSettingsStore,
  shellAssets: ShellAssetPaths
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
  runtime.on("state", emitLatestState);
  runtime.on("event", (event: BitfsRuntimeEvent) => {
    broadcastPrivateEvent(event);
  });
  supervisor.on("state", emitLatestState);
  supervisor.on("event", (event: BitfsRuntimeEvent) => {
    if (event.scope === "public") {
      broadcastPublicEvent(event);
      return;
    }
    broadcastPrivateEvent(event);
  });
  settings.on("change", emitLatestState);

  ipcMain.handle("bitfs-shell:get-state", () => buildShellState());
  ipcMain.handle("bitfs-shell:open", (_event, seedHash: string) => {
    debugLogger.log("ipc", "open", { seed_hash: seedHash });
    return runtime.openRoot(seedHash);
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
  "/api/v1/gateways",
  "/api/v1/gateways/master",
  "/api/v1/gateways/health",
  "/api/v1/arbiters",
  "/api/v1/arbiters/health",
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
