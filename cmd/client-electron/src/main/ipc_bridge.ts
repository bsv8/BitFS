import { BrowserWindow, dialog, ipcMain } from "electron";
import fs from "node:fs/promises";
import path from "node:path";

import type { BitfsBrowserRuntime } from "./browser_runtime";
import type { BrowserSettingsStore } from "./browser_settings";
import type { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import type { KeyFileActionResult, ShellState } from "../shared/shell_contract";

export function registerShellIPC(
  window: BrowserWindow,
  runtime: BitfsBrowserRuntime,
  supervisor: ManagedClientSupervisor,
  settings: BrowserSettingsStore
): void {
  const buildShellState = (): ShellState => {
    const runtimeState = runtime.snapshot();
    const backendState = supervisor.snapshot();
    return {
      ...runtimeState,
      lastError: runtimeState.lastError || backendState.lastError,
      userHomeSeedHash: settings.snapshot().userHomeSeedHash,
      backend: backendState
    };
  };
  const sendState = (state: ShellState) => {
    if (!window.isDestroyed()) {
      window.webContents.send("bitfs-shell:state", state);
    }
  };
  const emitLatestState = () => {
    sendState(buildShellState());
  };
  runtime.on("state", emitLatestState);
  supervisor.on("state", emitLatestState);
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
  ipcMain.handle("bitfs-shell:approve-pending", () => {
    debugLogger.log("ipc", "approve_pending");
    runtime.approvePendingResources();
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
