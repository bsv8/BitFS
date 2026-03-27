import { app, BrowserWindow, protocol, session } from "electron";
import path from "node:path";

import { createAppWindow } from "./app_window";
import { BrowserSettingsStore } from "./browser_settings";
import { BitfsBrowserRuntime } from "./browser_runtime";
import { createBitfsProtocolHandler } from "./bitfs_protocol";
import { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import { ElectronE2EController, ElectronE2EViewerPolicyStore } from "./e2e_controller";
import { ElectronE2EObserver } from "./e2e_observer";
import { registerShellIPC } from "./ipc_bridge";
import type { LocatorVisitContext } from "./locator";
import { isTrustedNavigationURL, isTrustedRequestURL } from "./navigation_guard";
import { resolveShellAssetPaths } from "./shell_assets";
import type { ShellErrorReport, ShellErrorSource } from "../shared/shell_contract";

const e2eConfig = resolveE2EConfig();

let mainWindowCreated = false;
let mainWindow: BrowserWindow | null = null;
let runtime: BitfsBrowserRuntime | null = null;
let supervisor: ManagedClientSupervisor | null = null;
let settings: BrowserSettingsStore | null = null;
let e2eController: ElectronE2EController | null = null;
const e2eViewerPolicy = e2eConfig.enabled ? new ElectronE2EViewerPolicyStore() : null;
const e2eObserver = new ElectronE2EObserver(e2eConfig.enabled);
const pendingShellErrorReports: ShellErrorReport[] = [];

applyEarlyAppConfig();
installProcessErrorGuards();

function installTrustedWorldGuards(): void {
  app.on("web-contents-created", (_event, contents) => {
    contents.on("will-navigate", (navEvent, url) => {
      if (isTrustedNavigationURL(url)) {
        debugLogger.log("guard", "navigation_allowed", {
          url,
          contents_id: contents.id
        });
        return;
      }
      debugLogger.log("guard", "navigation_blocked", {
        url,
        contents_id: contents.id
      });
      navEvent.preventDefault();
    });
    contents.setWindowOpenHandler(({ url }) => {
      if (isTrustedNavigationURL(url)) {
        debugLogger.log("guard", "window_open_allowed", {
          url,
          contents_id: contents.id
        });
        return { action: "allow" };
      }
      debugLogger.log("guard", "window_open_blocked", {
        url,
        contents_id: contents.id
      });
      return { action: "deny" };
    });
  });
  const currentSession = session.defaultSession;
  currentSession.webRequest.onBeforeRequest((details, callback) => {
    const allowed = isTrustedRequestURL(details.url);
    if (!allowed) {
      debugLogger.log("guard", "request_blocked", {
        url: details.url,
        resource_type: details.resourceType,
        method: details.method,
        web_contents_id: details.webContentsId
      });
    }
    callback({ cancel: !allowed });
  });
}

async function bootstrap(): Promise<void> {
  await app.whenReady();
  debugLogger.init({ userDataDir: app.getPath("userData") });
  debugLogger.log("bootstrap", "app_ready", {
    app_path: app.getAppPath(),
    user_data_dir: app.getPath("userData"),
    packaged: app.isPackaged,
    log_file_path: debugLogger.getLogFilePath()
  });
  installTrustedWorldGuards();
  const shellAssets = resolveShellAssetPaths(app.getAppPath());
  settings = new BrowserSettingsStore(app.getPath("userData"));
  debugLogger.log("bootstrap", "settings_ready", {
    viewer_preload_path: shellAssets.viewerPreloadPath,
    settings_preload_path: shellAssets.settingsPreloadPath,
    settings_page_url: shellAssets.settingsPageURL
  });
  supervisor = await ManagedClientSupervisor.create({
    appRootDir: app.getAppPath(),
    packaged: app.isPackaged,
    userDataDir: app.getPath("userData")
  });
  debugLogger.log("bootstrap", "supervisor_created", supervisor.snapshot());
  runtime = new BitfsBrowserRuntime(supervisor.snapshot().apiBase, shellAssets.viewerPreloadPath, {
    resolveNodeLocator: async (locator, visit) => {
      return {
        seedHash: await fetchSeedHashFromNodeRoute(locator.nodePubkeyHex, locator.route, visit)
      };
    },
    resolveResolverLocator: async (locator, visit) => {
      const targetPubkeyHex = await resolveLocatorName(locator.resolverPubkeyHex, locator.name, visit);
      return {
        seedHash: await fetchSeedHashFromNodeRoute(targetPubkeyHex, locator.route, visit),
        targetPubkeyHex
      };
    }
  });
  protocol.handle("bitfs", createBitfsProtocolHandler(runtime));
  debugLogger.log("bootstrap", "protocol_registered", {
    scheme: "bitfs"
  });
  if (!mainWindowCreated) {
    const window = createAppWindow(app.getAppPath());
    mainWindow = window;
    window.on("closed", () => {
      if (mainWindow === window) {
        mainWindow = null;
      }
      mainWindowCreated = false;
    });
    registerShellIPC(window, runtime, supervisor, settings, shellAssets, e2eViewerPolicy, e2eObserver);
    window.webContents.once("did-finish-load", () => {
      flushPendingShellErrorReports(window);
    });
    mainWindowCreated = true;
    debugLogger.log("bootstrap", "main_window_created", {
      window_id: window.id
    });
  }
  if (e2eConfig.enabled && !e2eController) {
    e2eController = new ElectronE2EController({
      runtime,
      supervisor,
      settings,
      getWindow: () => mainWindow,
      cdpPort: e2eConfig.cdpPort,
      viewerPolicy: e2eViewerPolicy || new ElectronE2EViewerPolicyStore(),
      observer: e2eObserver
    });
    await e2eController.start(e2eConfig.controlPort);
  }
  const maybeOpenInitialHomepage = () => {
    if (!runtime || !supervisor || !settings) {
      return;
    }
    if (supervisor.snapshot().phase !== "ready") {
      return;
    }
    if (runtime.snapshot().currentURL !== "") {
      return;
    }
    const target = settings.snapshot().userHomeSeedHash || supervisor.snapshot().defaultHomeSeedHash;
    if (target === "") {
      debugLogger.log("bootstrap", "initial_homepage_missing");
      return;
    }
    debugLogger.log("bootstrap", "initial_homepage_open", {
      seed_hash: target,
      source: settings.snapshot().userHomeSeedHash ? "user" : "client"
    });
    runtime.openRoot(target);
  };
  supervisor.on("state", maybeOpenInitialHomepage);
  settings.on("change", maybeOpenInitialHomepage);
  debugLogger.log("bootstrap", "supervisor_start_requested");
  void supervisor.start();
}

app.on("window-all-closed", () => {
  debugLogger.log("bootstrap", "window_all_closed", {
    platform: process.platform
  });
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", (event) => {
  if (!supervisor) {
    return;
  }
  event.preventDefault();
  const current = supervisor;
  const currentController = e2eController;
  supervisor = null;
  e2eController = null;
  debugLogger.log("bootstrap", "before_quit_stop_supervisor");
  void Promise.resolve()
    .then(async () => {
      if (currentController) {
        await currentController.close();
      }
      await current.stop();
    })
    .finally(() => {
      debugLogger.log("bootstrap", "app_exit");
      app.exit(0);
    });
});

app.on("activate", () => {
  if (mainWindowCreated || runtime === null || supervisor === null || settings === null) {
    return;
  }
  const shellAssets = resolveShellAssetPaths(app.getAppPath());
  const window = createAppWindow(app.getAppPath());
  mainWindow = window;
  window.on("closed", () => {
    if (mainWindow === window) {
      mainWindow = null;
    }
    mainWindowCreated = false;
  });
  registerShellIPC(window, runtime, supervisor, settings, shellAssets, e2eViewerPolicy);
  window.webContents.once("did-finish-load", () => {
    flushPendingShellErrorReports(window);
  });
  mainWindowCreated = true;
  debugLogger.log("bootstrap", "main_window_recreated", {
    window_id: window.id
  });
});

void bootstrap().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  debugLogger.log("bootstrap", "bootstrap_failed", {
    message
  });
  console.error("electron bootstrap failed:", message);
  app.exit(1);
});

function installProcessErrorGuards(): void {
  process.on("uncaughtException", (error: Error) => {
    emitShellErrorReport(buildShellErrorReport(
      "main-process",
      "主进程 JS 错误",
      error?.message || "main process uncaught exception",
      error?.stack || "",
      false
    ));
  });
  process.on("unhandledRejection", (reason: unknown) => {
    emitShellErrorReport(buildShellErrorReport(
      "main-process",
      "主进程 Promise 未处理拒绝",
      extractUnknownErrorMessage(reason, "main process unhandled rejection"),
      extractUnknownErrorDetail(reason),
      false
    ));
  });
}

function emitShellErrorReport(report: ShellErrorReport): void {
  const normalized = normalizeShellErrorReport(report);
  debugLogger.log("shell_error", "main_reported", {
    source: normalized.source,
    title: normalized.title,
    page_url: normalized.page_url,
    can_stop_current_page: normalized.can_stop_current_page,
    message: normalized.message
  });
  e2eObserver.emit("main-process", "shell.error.reported", {
    report_source: normalized.source,
    report_title: normalized.title,
    report_message: normalized.message,
    report_page_url: normalized.page_url,
    can_stop_current_page: normalized.can_stop_current_page
  });
  if (mainWindow && !mainWindow.isDestroyed() && !mainWindow.webContents.isDestroyed()) {
    try {
      mainWindow.webContents.send("bitfs-shell:error-report", normalized);
      return;
    } catch {
      // 窗口销毁或 renderer 尚未就绪时，退回到 pending 队列，等待壳页面完成加载后再交付。
    }
  }
  pendingShellErrorReports.push(normalized);
  if (pendingShellErrorReports.length > 16) {
    pendingShellErrorReports.shift();
  }
}

function flushPendingShellErrorReports(window: BrowserWindow): void {
  if (pendingShellErrorReports.length === 0 || window.isDestroyed() || window.webContents.isDestroyed()) {
    return;
  }
  const queued = pendingShellErrorReports.splice(0, pendingShellErrorReports.length);
  for (const report of queued) {
    try {
      window.webContents.send("bitfs-shell:error-report", report);
    } catch {
      pendingShellErrorReports.unshift(report);
      break;
    }
  }
}

function buildShellErrorReport(
  source: ShellErrorSource,
  title: string,
  message: string,
  detail: string,
  canStopCurrentPage: boolean,
  pageURL = ""
): ShellErrorReport {
  return normalizeShellErrorReport({
    source,
    title,
    message,
    detail,
    page_url: pageURL,
    occurred_at_unix: Math.floor(Date.now() / 1000),
    can_stop_current_page: canStopCurrentPage
  });
}

function normalizeShellErrorReport(report: Partial<ShellErrorReport>): ShellErrorReport {
  const source = normalizeShellErrorSource(report.source);
  const title = String(report.title || "").trim() || defaultShellErrorTitle(source);
  const message = String(report.message || "").trim() || "unknown error";
  return {
    source,
    title,
    message,
    detail: String(report.detail || "").trim(),
    page_url: String(report.page_url || "").trim(),
    occurred_at_unix: Math.max(0, Math.floor(Number(report.occurred_at_unix || 0))) || Math.floor(Date.now() / 1000),
    can_stop_current_page: Boolean(report.can_stop_current_page) && (source === "viewer" || source === "settings")
  };
}

function normalizeShellErrorSource(raw: ShellErrorReport["source"] | undefined): ShellErrorSource {
  if (raw === "viewer" || raw === "settings" || raw === "shell-renderer") {
    return raw;
  }
  return "main-process";
}

function defaultShellErrorTitle(source: ShellErrorSource): string {
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

function extractUnknownErrorMessage(reason: unknown, fallback: string): string {
  if (reason instanceof Error && String(reason.message || "").trim() !== "") {
    return String(reason.message || "").trim();
  }
  if (reason && typeof reason === "object" && !Array.isArray(reason) && "message" in reason) {
    const message = String((reason as { message?: unknown }).message || "").trim();
    if (message !== "") {
      return message;
    }
  }
  const value = String(reason || "").trim();
  return value || fallback;
}

function extractUnknownErrorDetail(reason: unknown): string {
  if (reason instanceof Error) {
    return String(reason.stack || reason.message || "").trim();
  }
  if (reason && typeof reason === "object" && !Array.isArray(reason) && "stack" in reason) {
    const stack = String((reason as { stack?: unknown }).stack || "").trim();
    if (stack !== "") {
      return stack;
    }
  }
  return String(reason || "").trim();
}

async function fetchSeedHashFromNodeRoute(targetPubkeyHex: string, route: string, visit?: LocatorVisitContext): Promise<string> {
  const body = await postJSON("/api/v1/resolve", {
    to: targetPubkeyHex,
    route
  }, visit);
  if (!body.ok) {
    throw new Error(String(body.message || body.error || "node locator resolve failed"));
  }
  const manifest = body.body_json as Record<string, unknown> | undefined;
  const seedHash = String(manifest?.seed_hash || "").trim();
  if (!/^[0-9a-f]{64}$/i.test(seedHash)) {
    throw new Error("node locator resolve returned invalid seed hash");
  }
  return seedHash;
}

async function resolveLocatorName(resolverPubkeyHex: string, name: string, visit?: LocatorVisitContext): Promise<string> {
  // 设计说明：
  // - 壳只把 locator 翻译成“先名字解析，再 node get”的业务链；
  // - 解析服务仍然是普通 node，真正的寻址与重试逻辑放在后端；
  // - 这轮先把 resolve 查询协议接通，收费/注册协议后续继续补。
  const body = await postJSON("/api/v1/resolvers/resolve", {
    resolver_pubkey_hex: resolverPubkeyHex,
    name
  }, visit);
  if (!body.ok) {
    throw new Error(String(body.message || body.error || "resolver resolve failed"));
  }
  const targetPubkeyHex = String(body.target_pubkey_hex || "").trim().toLowerCase();
  if (!/^(02|03)[0-9a-f]{64}$/i.test(targetPubkeyHex)) {
    throw new Error("resolver resolve returned invalid target pubkey hex");
  }
  return targetPubkeyHex;
}

async function postJSON(pathname: string, payload: Record<string, unknown>, visit?: LocatorVisitContext): Promise<Record<string, unknown>> {
  const response = await fetch(`${supervisor!.snapshot().apiBase}${pathname}`, {
    method: "POST",
    headers: buildVisitHeaders(visit),
    body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body: Record<string, unknown> = {};
  if (text.trim() !== "") {
    body = JSON.parse(text) as Record<string, unknown>;
  }
  if (!response.ok) {
    throw new Error(String(body.error || text || `request failed: ${response.status}`));
  }
  return body;
}

function buildVisitHeaders(visit?: LocatorVisitContext): Record<string, string> {
  const headers: Record<string, string> = {
    "content-type": "application/json"
  };
  const visitID = String(visit?.visitID || "").trim();
  const visitLocator = String(visit?.visitLocator || "").trim();
  if (visitID !== "") {
    headers["X-BitFS-Visit-ID"] = visitID;
  }
  if (visitLocator !== "") {
    headers["X-BitFS-Visit-Locator"] = visitLocator;
  }
  return headers;
}

type ElectronE2EConfig = {
  enabled: boolean;
  controlPort: number;
  cdpPort: number;
  userDataDir: string;
};

function resolveE2EConfig(): ElectronE2EConfig {
  const enabled = parseBoolEnv(process.env.BITFS_ELECTRON_E2E);
  const rawUserDataDir = String(process.env.BITFS_ELECTRON_USER_DATA_DIR || "").trim();
  const userDataDir = rawUserDataDir === "" ? "" : path.resolve(rawUserDataDir);
  if (!enabled) {
    return {
      enabled: false,
      controlPort: 0,
      cdpPort: 0,
      userDataDir
    };
  }
  const controlPort = parseRequiredPort(process.env.BITFS_ELECTRON_E2E_PORT, "BITFS_ELECTRON_E2E_PORT");
  const cdpPort = parseRequiredPort(process.env.BITFS_ELECTRON_E2E_CDP_PORT, "BITFS_ELECTRON_E2E_CDP_PORT");
  return {
    enabled: true,
    controlPort,
    cdpPort,
    userDataDir
  };
}

function applyEarlyAppConfig(): void {
  if (e2eConfig.userDataDir !== "") {
    app.setPath("userData", e2eConfig.userDataDir);
  }
  if (e2eConfig.enabled) {
    app.commandLine.appendSwitch("remote-debugging-port", String(e2eConfig.cdpPort));
  }
}

function parseBoolEnv(raw: string | undefined): boolean {
  const value = String(raw || "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function parseRequiredPort(raw: string | undefined, envName: string): number {
  const value = Number.parseInt(String(raw || "").trim(), 10);
  if (!Number.isInteger(value) || value <= 0 || value > 65535) {
    throw new Error(`${envName} is required`);
  }
  return value;
}
