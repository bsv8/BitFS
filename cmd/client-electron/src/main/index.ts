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
      const targetPubkeyHex = await resolveLocatorName(locator.name, visit);
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
    const backend = supervisor.snapshot();
    if (backend.backendPhase !== "available" || backend.runtimePhase !== "ready") {
      return;
    }
    if (runtime.snapshot().currentURL !== "") {
      return;
    }
    const target = settings.snapshot().userHomeSeedHash || backend.defaultHomeSeedHash;
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
  void targetPubkeyHex;
  void route;
  void visit;
  // 设计说明：
  // - 旧版 /api/v1/resolve 已下线，壳层不再直接走 node resolve；
  // - 后续由新的模块化能力提供可替代链路后再接回。
  throw new Error("node route resolve is removed");
}

async function resolveLocatorName(name: string, visit?: LocatorVisitContext): Promise<string> {
  // 设计说明：
  // - 壳层只认 domain，不再保留 resolver 公钥双轨；
  // - 解析服务由后端基座统一分发，壳只消费最终 pubkey；
  // - 出错时直接抛明确错误，不做静默回退。
  debugLogger.log("runtime", "locator_resolve_request", {
    name,
    visit_id: String(visit?.visitID || "").trim(),
    visit_locator: String(visit?.visitLocator || "").trim()
  });
  let body: Record<string, unknown>;
  try {
    body = await postJSON("/api/v1/resolvers/resolve", {
      domain: name
    }, visit);
  } catch (error) {
    debugLogger.log("runtime", "locator_resolve_http_error", {
      name,
      error: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
  debugLogger.log("runtime", "locator_resolve_response", {
    name,
    status: String(body.status || "").trim(),
    code: String((body.error as { code?: unknown } | undefined)?.code || "").trim(),
    message: String((body.error as { message?: unknown } | undefined)?.message || "").trim(),
    pubkey_hex: String((body.data as { pubkey_hex?: unknown } | undefined)?.pubkey_hex || "").trim()
  });
  if (String(body.status || "").trim() !== "ok") {
    const error = body.error && typeof body.error === "object" ? body.error as { code?: unknown; message?: unknown } : {};
    debugLogger.log("runtime", "locator_resolve_rejected", {
      name,
      code: String(error.code || "").trim(),
      message: String(error.message || "").trim()
    });
    throw new Error(String(error.message || "resolver resolve failed"));
  }
  const data = body.data && typeof body.data === "object" ? body.data as { pubkey_hex?: unknown } : {};
  const targetPubkeyHex = String(data.pubkey_hex || "").trim().toLowerCase();
  if (!/^(02|03)[0-9a-f]{64}$/i.test(targetPubkeyHex)) {
    debugLogger.log("runtime", "locator_resolve_invalid_target", {
      name,
      pubkey_hex: targetPubkeyHex
    });
    throw new Error("domain resolve returned invalid pubkey hex");
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

function decodeRouteIndexSeedHash(bodyBase64: string): string {
  if (bodyBase64 === "") {
    throw new Error("node locator resolve body_base64 missing");
  }
  let raw: Buffer;
  try {
    raw = Buffer.from(bodyBase64, "base64");
  } catch {
    throw new Error("node locator resolve body_base64 invalid");
  }
  if (raw.length === 0) {
    throw new Error("node locator resolve proto body empty");
  }
  let offset = 0;
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    if (fieldNumber === 2) {
      return readProtoString(raw, () => offset, (next) => {
        offset = next;
      }).trim().toLowerCase();
    }
    offset = skipProtoField(raw, offset, wireType);
  }
  return "";
}

function readProtoString(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): string {
  return new TextDecoder().decode(readProtoBytes(raw, getOffset, setOffset));
}

function readProtoBytes(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): Uint8Array {
  const size = readProtoVarint(raw, getOffset, setOffset);
  const offset = getOffset();
  const next = offset + size;
  if (size < 0 || next > raw.length) {
    throw new Error("protobuf length-delimited field truncated");
  }
  setOffset(next);
  return raw.subarray(offset, next);
}

function readProtoVarint(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): number {
  let offset = getOffset();
  let result = 0;
  let shift = 0;
  while (offset < raw.length && shift < 35) {
    const value = raw[offset];
    offset += 1;
    result |= (value & 0x7f) << shift;
    if ((value & 0x80) === 0) {
      setOffset(offset);
      return result >>> 0;
    }
    shift += 7;
  }
  throw new Error("protobuf varint truncated");
}

function skipProtoField(raw: Uint8Array, offset: number, wireType: number): number {
  switch (wireType) {
    case 0:
      return skipProtoVarint(raw, offset);
    case 1:
      if (offset + 8 > raw.length) {
        throw new Error("protobuf fixed64 truncated");
      }
      return offset + 8;
    case 2: {
      const nextOffsetRef = { value: offset };
      const size = readProtoVarint(raw, () => nextOffsetRef.value, (next) => {
        nextOffsetRef.value = next;
      });
      const next = nextOffsetRef.value + size;
      if (size < 0 || next > raw.length) {
        throw new Error("protobuf length-delimited skip truncated");
      }
      return next;
    }
    case 5:
      if (offset + 4 > raw.length) {
        throw new Error("protobuf fixed32 truncated");
      }
      return offset + 4;
    default:
      throw new Error(`protobuf wire type unsupported: ${String(wireType)}`);
  }
}

function skipProtoVarint(raw: Uint8Array, offset: number): number {
  let next = offset;
  while (next < raw.length) {
    const value = raw[next];
    next += 1;
    if ((value & 0x80) === 0) {
      return next;
    }
  }
  throw new Error("protobuf varint skip truncated");
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
