import { app, type BrowserWindow } from "electron";
import http from "node:http";

import { BrowserSettingsStore } from "./browser_settings";
import { BitfsBrowserRuntime } from "./browser_runtime";
import { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import type { ElectronE2EObserver } from "./e2e_observer";

type ElectronE2EControllerInit = {
  runtime: BitfsBrowserRuntime;
  supervisor: ManagedClientSupervisor;
  settings: BrowserSettingsStore;
  getWindow: () => BrowserWindow | null;
  cdpPort: number;
  viewerPolicy: ElectronE2EViewerPolicyStore;
  observer: ElectronE2EObserver;
};

type e2eOpenHomeRequest = {
  seed_hash?: string;
  persist?: boolean;
};

type e2eViewerPolicyPatch = {
  auto_approve_peer_call?: boolean;
  auto_approve_wallet_business?: boolean;
};

export type ElectronE2EViewerPolicySnapshot = {
  auto_approve_peer_call: boolean;
  auto_approve_wallet_business: boolean;
};

// ElectronE2EViewerPolicyStore 让 e2e 用例显式控制 viewer 权限弹框策略。
// 设计说明：
// - e2e 需要走真实 window.bitfs 链路，但原生对话框无法通过 CDP 稳定操作；
// - 因此把“是否自动批准”做成 e2e 控制口可调策略，而不是在业务代码里硬编码跳过；
// - 默认保持 false，避免 e2e 进程一启动就无条件放权。
export class ElectronE2EViewerPolicyStore {
  private autoApprovePeerCall = false;
  private autoApproveWalletBusiness = false;

  snapshot(): ElectronE2EViewerPolicySnapshot {
    return {
      auto_approve_peer_call: this.autoApprovePeerCall,
      auto_approve_wallet_business: this.autoApproveWalletBusiness
    };
  }

  applyPatch(patch: e2eViewerPolicyPatch): ElectronE2EViewerPolicySnapshot {
    if (typeof patch.auto_approve_peer_call === "boolean") {
      this.autoApprovePeerCall = patch.auto_approve_peer_call;
    }
    if (typeof patch.auto_approve_wallet_business === "boolean") {
      this.autoApproveWalletBusiness = patch.auto_approve_wallet_business;
    }
    return this.snapshot();
  }
}

export class ElectronE2EController {
  private readonly runtime: BitfsBrowserRuntime;
  private readonly supervisor: ManagedClientSupervisor;
  private readonly settings: BrowserSettingsStore;
  private readonly getWindow: () => BrowserWindow | null;
  private readonly cdpPort: number;
  private readonly viewerPolicy: ElectronE2EViewerPolicyStore;
  private readonly observer: ElectronE2EObserver;
  private server: http.Server | null = null;

  constructor(init: ElectronE2EControllerInit) {
    this.runtime = init.runtime;
    this.supervisor = init.supervisor;
    this.settings = init.settings;
    this.getWindow = init.getWindow;
    this.cdpPort = init.cdpPort;
    this.viewerPolicy = init.viewerPolicy;
    this.observer = init.observer;
  }

  async start(port: number): Promise<void> {
    if (this.server) {
      return;
    }
    this.server = http.createServer((req, res) => {
      void this.handle(req, res);
    });
    await new Promise<void>((resolve, reject) => {
      const server = this.server;
      if (!server) {
        reject(new Error("e2e controller is not initialized"));
        return;
      }
      server.once("error", reject);
      server.listen(port, "127.0.0.1", () => {
        server.off("error", reject);
        resolve();
      });
    });
    debugLogger.log("e2e", "controller_started", {
      port,
      cdp_port: this.cdpPort
    });
  }

  async close(): Promise<void> {
    if (!this.server) {
      return;
    }
    const server = this.server;
    this.server = null;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    debugLogger.log("e2e", "controller_stopped");
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    try {
      const method = String(req.method || "").toUpperCase();
      const url = new URL(String(req.url || "/"), "http://127.0.0.1");
      if (method === "GET" && url.pathname === "/e2e/status") {
        this.writeJSON(res, 200, this.buildStatus());
        return;
      }
      if (method === "GET" && url.pathname === "/e2e/events") {
        const afterSeq = Number.parseInt(String(url.searchParams.get("after_seq") || "0"), 10);
        this.writeJSON(res, 200, {
          ok: true,
          after_seq: Number.isInteger(afterSeq) && afterSeq > 0 ? afterSeq : 0,
          events: this.observer.eventsAfter(afterSeq),
          state: this.observer.snapshot()
        });
        return;
      }
      if (method === "GET" && url.pathname === "/e2e/wallet-summary") {
        const summary = await this.supervisor.requestManagedJSON<Record<string, unknown>>({
          method: "GET",
          pathname: "/api/v1/wallet/summary",
          timeout_ms: 5_000
        });
        this.writeJSON(res, 200, {
          ok: true,
          wallet_summary: summary
        });
        return;
      }
      if (method === "POST" && url.pathname === "/e2e/unlock") {
        const body = await readJSONBody<{ password?: string }>(req);
        const state = await this.supervisor.unlock(String(body?.password || ""));
        this.writeJSON(res, 200, {
          ok: true,
          backend: state,
          status: this.buildStatus()
        });
        return;
      }
      if (method === "POST" && url.pathname === "/e2e/open-home") {
        const body = await readJSONBody<e2eOpenHomeRequest>(req);
        const requestedSeedHash = normalizeSeedHash(String(body?.seed_hash || ""));
        if (requestedSeedHash !== "" && body?.persist) {
          this.settings.setUserHomeSeedHash(requestedSeedHash);
        }
        const targetSeedHash = requestedSeedHash ||
          this.settings.snapshot().userHomeSeedHash ||
          this.supervisor.snapshot().defaultHomeSeedHash;
        if (targetSeedHash === "") {
          throw new Error("home seed hash is unavailable");
        }
        const urlText = this.runtime.openRoot(targetSeedHash);
        this.writeJSON(res, 200, {
          ok: true,
          target_seed_hash: targetSeedHash,
          url: urlText,
          status: this.buildStatus()
        });
        return;
      }
      if (method === "GET" && url.pathname === "/e2e/viewer-policy") {
        this.writeJSON(res, 200, {
          ok: true,
          viewer_policy: this.viewerPolicy.snapshot()
        });
        return;
      }
      if (method === "POST" && url.pathname === "/e2e/viewer-policy") {
        const body = await readJSONBody<e2eViewerPolicyPatch>(req);
        const viewerPolicy = this.viewerPolicy.applyPatch(body || {});
        debugLogger.log("e2e", "viewer_policy_updated", viewerPolicy);
        this.writeJSON(res, 200, {
          ok: true,
          viewer_policy: viewerPolicy,
          status: this.buildStatus()
        });
        return;
      }
      if (method === "POST" && url.pathname === "/e2e/quit") {
        this.writeJSON(res, 200, { ok: true });
        setImmediate(() => {
          app.quit();
        });
        return;
      }
      this.writeJSON(res, 404, { error: "not found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("e2e", "request_failed", {
        method: String(req.method || ""),
        url: String(req.url || ""),
        message
      });
      this.writeJSON(res, 500, { error: message });
    }
  }

  private buildStatus(): Record<string, unknown> {
    const runtime = this.runtime.snapshot();
    const backend = this.supervisor.snapshot();
    const settings = this.settings.snapshot();
    const window = this.getWindow();
    return {
      ok: true,
      e2e: true,
      pid: process.pid,
      cdp_port: this.cdpPort,
      user_data_dir: app.getPath("userData"),
      log_file_path: debugLogger.getLogFilePath(),
      current_url: runtime.currentURL,
      current_viewer_url: runtime.currentViewerURL,
      current_root_seed_hash: runtime.currentRootSeedHash,
      client_api_base: runtime.clientAPIBase,
      viewer_preload_path: runtime.viewerPreloadPath,
      wallet_ready: backend.phase === "ready" && backend.unlocked,
      main_window_created: window !== null,
      main_window_visible: window ? window.isVisible() : false,
      viewer_policy: this.viewerPolicy.snapshot(),
      observer: this.observer.snapshot(),
      backend,
      settings
    };
  }

  private writeJSON(res: http.ServerResponse, statusCode: number, payload: Record<string, unknown>): void {
    const body = JSON.stringify(payload);
    res.statusCode = statusCode;
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.end(body);
  }
}

async function readJSONBody<T>(req: http.IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  if (chunks.length === 0) {
    return {} as T;
  }
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (raw === "") {
    return {} as T;
  }
  return JSON.parse(raw) as T;
}

function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(value)) {
    return "";
  }
  return value;
}
