import { spawn, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import net from "node:net";
import path from "node:path";

import type { BitfsRuntimeEvent, ManagedClientPhase, ManagedClientState } from "../shared/shell_contract";
import { debugLogger } from "./debug_logger";

const STATUS_POLL_INTERVAL_MS = 1500;
const START_TIMEOUT_MS = 20_000;
const READY_TIMEOUT_MS = 30_000;
const LOG_BUFFER_LIMIT = 80;
const BOOTSTRAP_PREFIX = "BITFS_MANAGED_STATE ";
const MANAGED_EVENT_PREFIX = "BITFS_EVENT ";

type ManagedClientSupervisorInit = {
  appRootDir: string;
  packaged: boolean;
  userDataDir: string;
};

type ManagedClientLaunchConfig = {
  binaryPath: string;
  vaultPath: string;
  network: string;
  apiListenAddr: string;
  apiBase: string;
  fsHTTPListenAddr: string;
  systemHomepageBundleDir: string;
};

type KeyStatusResponse = {
  phase?: ManagedClientPhase;
  has_key?: boolean;
  unlocked?: boolean;
  has_system_home_bundle?: boolean;
  default_home_seed_hash?: string;
  startup_error_service?: string;
  startup_error_listen?: string;
  startup_error_message?: string;
  chain_access_mode?: string;
  wallet_chain_base_url?: string;
  woc_proxy_enabled?: boolean;
  woc_proxy_listen_addr?: string;
  woc_upstream_root_url?: string;
  woc_min_interval?: string;
};

type ExportKeyResponse = {
  cipher?: Record<string, unknown>;
};

type ManagedJSONRequest = {
  method?: string;
  pathname: string;
  body?: unknown;
  headers?: Record<string, string>;
  timeout_ms?: number;
};

type StaticUploadRequest = {
  filePath: string;
  fileName?: string;
  targetDir: string;
  overwrite: boolean;
};

type BootstrapStateEvent = {
  type?: string;
  phase?: ManagedClientPhase;
  startup_error_service?: string;
  startup_error_listen?: string;
  startup_error_message?: string;
  chain_access_mode?: string;
  wallet_chain_base_url?: string;
  woc_proxy_enabled?: boolean;
  woc_proxy_listen_addr?: string;
  woc_upstream_root_url?: string;
  woc_min_interval?: string;
};

export class ManagedClientSupervisor extends EventEmitter {
  private readonly launch: ManagedClientLaunchConfig;
  private child: ChildProcessByStdio<null, Readable, Readable> | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private state: ManagedClientState;
  private startPromise: Promise<ManagedClientState> | null = null;
  private stopping = false;
  private stdoutLineRemainder = "";
  private stderrLineRemainder = "";

  private constructor(launch: ManagedClientLaunchConfig) {
    super();
    this.launch = launch;
    this.state = {
      phase: "stopped",
      hasKey: false,
      unlocked: false,
      hasSystemHomeBundle: false,
      defaultHomeSeedHash: "",
      pid: 0,
      apiBase: launch.apiBase,
      fsHTTPListenAddr: launch.fsHTTPListenAddr,
      vaultPath: launch.vaultPath,
      binaryPath: launch.binaryPath,
      lastError: "",
      startupErrorService: "",
      startupErrorListenAddr: "",
      chainAccessMode: "",
      walletChainBaseURL: "",
      wocProxyEnabled: false,
      wocProxyListenAddr: "",
      wocUpstreamRootURL: "",
      wocMinInterval: "",
      recentLogs: []
    };
  }

  static async create(init: ManagedClientSupervisorInit): Promise<ManagedClientSupervisor> {
    const apiPort = parseOptionalPort(process.env.BITFS_CLIENT_API_PORT) ?? await pickFreePort();
    const fsHTTPPort = parseOptionalPort(process.env.BITFS_CLIENT_FSHTTP_PORT) ?? await pickFreePort();
    const binaryPath = resolveManagedClientBinaryPath(init.appRootDir, init.packaged);
    const vaultPath = path.resolve(
      String(process.env.BITFS_CLIENT_VAULT_DIR || path.join(init.userDataDir, "client-vault"))
    );
    const network = String(process.env.BITFS_CLIENT_NETWORK || "main").trim() || "main";
    const supervisor = new ManagedClientSupervisor({
      binaryPath,
      vaultPath,
      network,
      apiListenAddr: `127.0.0.1:${apiPort}`,
      apiBase: `http://127.0.0.1:${apiPort}`,
      fsHTTPListenAddr: `127.0.0.1:${fsHTTPPort}`,
      systemHomepageBundleDir: resolveSystemHomepageBundleDir(init.appRootDir, init.packaged)
    });
    debugLogger.log("supervisor", "created", {
      binary_path: binaryPath,
      vault_path: vaultPath,
      api_listen_addr: supervisor.launch.apiListenAddr,
      api_base: supervisor.launch.apiBase,
      fs_http_listen_addr: supervisor.launch.fsHTTPListenAddr,
      system_homepage_bundle_dir: supervisor.launch.systemHomepageBundleDir
    });
    return supervisor;
  }

  snapshot(): ManagedClientState {
    return {
      ...this.state,
      recentLogs: [...this.state.recentLogs]
    };
  }

  async start(): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "start_called", {
      phase: this.state.phase,
      has_child: Boolean(this.child)
    });
    if (this.startPromise) {
      return this.startPromise;
    }
    if (this.child) {
      return this.waitForReachableState(START_TIMEOUT_MS);
    }
    this.startPromise = this.startInternal();
    try {
      return await this.startPromise;
    } finally {
      this.startPromise = null;
    }
  }

  async stop(): Promise<void> {
    debugLogger.log("supervisor", "stop_called", {
      pid: this.state.pid,
      phase: this.state.phase
    });
    this.stopping = true;
    this.stopPolling();
    const child = this.child;
    if (!child) {
      this.setState({
        phase: "stopped",
        pid: 0,
        unlocked: false,
        hasSystemHomeBundle: false,
        defaultHomeSeedHash: "",
        lastError: "",
        startupErrorService: "",
        startupErrorListenAddr: ""
      });
      this.stopping = false;
      return;
    }
    const exitPromise = new Promise<void>((resolve) => {
      child.once("exit", () => resolve());
    });
    child.kill("SIGTERM");
    const timedOut = await raceTimeout(exitPromise, 5_000);
    if (timedOut) {
      child.kill("SIGKILL");
      await exitPromise.catch(() => undefined);
    }
    this.child = null;
    this.setState({
      phase: "stopped",
      pid: 0,
      unlocked: false,
      hasSystemHomeBundle: false,
      defaultHomeSeedHash: "",
      lastError: "",
      startupErrorService: "",
      startupErrorListenAddr: ""
    });
    this.stopping = false;
  }

  async restart(): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "restart_called");
    await this.stop();
    return this.start();
  }

  async createKey(password: string): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "create_key_requested");
    await this.ensureReachable();
    await this.postJSON("/api/v1/key/new", { password: requirePassword(password) });
    await this.refreshStatus();
    // 设计说明：
    // - 当前 Go managed API 在 key/new 后通常先落到 hasKey + locked；
    // - renderer 会继续复用同一密码自动解锁；
    // - 这里同时兼容未来 key/new 直接进入 ready 的实现，避免壳层把状态机写死。
    return this.waitForState((state) => state.hasKey && (state.phase === "locked" || state.phase === "ready"), READY_TIMEOUT_MS);
  }

  async importKeyCipher(cipher: Record<string, unknown>): Promise<ManagedClientState> {
    if (!cipher || typeof cipher !== "object" || Array.isArray(cipher)) {
      throw new Error("cipher is required");
    }
    debugLogger.log("supervisor", "import_key_requested", {
      cipher_keys: Object.keys(cipher)
    });
    await this.ensureReachable();
    await this.postJSON("/api/v1/key/import", { cipher });
    await this.refreshStatus();
    return this.waitForState((state) => state.phase === "locked" && state.hasKey, READY_TIMEOUT_MS);
  }

  async exportKeyCipher(): Promise<Record<string, unknown>> {
    debugLogger.log("supervisor", "export_key_requested");
    await this.ensureReachable();
    const response = await this.fetchJSON<ExportKeyResponse>("/api/v1/key/export");
    const cipher = response?.cipher;
    if (!cipher || typeof cipher !== "object" || Array.isArray(cipher)) {
      throw new Error("invalid encrypted key export");
    }
    return cipher;
  }

  async unlock(password: string): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "unlock_requested");
    await this.ensureReachable();
    await this.postJSON("/api/v1/key/unlock", { password: requirePassword(password) });
    await this.refreshStatus();
    return this.waitForState((state) => state.phase === "ready" && state.unlocked, READY_TIMEOUT_MS);
  }

  async lock(): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "lock_requested");
    await this.ensureReachable();
    await this.postJSON("/api/v1/key/lock", {});
    await this.refreshStatus();
    return this.waitForState((state) => state.phase === "locked" && !state.unlocked, READY_TIMEOUT_MS);
  }

  async requestManagedJSON<T>(request: ManagedJSONRequest): Promise<T> {
    await this.ensureReachable();
    return this.fetchJSON<T>(
      request.pathname,
      buildJSONRequestInit(request.method, request.body, request.headers),
      Number(request.timeout_ms || 0)
    );
  }

  async uploadStaticFile<T>(request: StaticUploadRequest): Promise<T> {
    const filePath = path.resolve(String(request.filePath || "").trim());
    const targetDir = String(request.targetDir || "").trim() || "/";
    const overwrite = Boolean(request.overwrite);
    const fileName = String(request.fileName || path.basename(filePath)).trim();
    if (fileName === "") {
      throw new Error("file name is required");
    }
    await this.ensureReachable();
    const data = await fsPromises.readFile(filePath);
    const form = new FormData();
    form.append("file", new Blob([data]), fileName);
    form.append("file_name", fileName);
    form.append("target_dir", targetDir);
    form.append("overwrite", overwrite ? "1" : "0");
    return this.fetchJSON<T>("/api/v1/admin/static/upload", {
      method: "POST",
      body: form
    });
  }

  private async ensureReachable(): Promise<void> {
    const state = await this.start();
    debugLogger.log("supervisor", "ensure_reachable_result", {
      phase: state.phase,
      has_key: state.hasKey,
      unlocked: state.unlocked
    });
    if (state.phase === "error" || state.phase === "startup_error") {
      throw new Error(state.lastError || "managed backend start failed");
    }
  }

  private async startInternal(): Promise<ManagedClientState> {
    if (!fs.existsSync(this.launch.binaryPath)) {
      const message = `managed backend binary not found: ${this.launch.binaryPath}`;
      debugLogger.log("supervisor", "binary_missing", {
        binary_path: this.launch.binaryPath
      });
      this.setState({ phase: "error", lastError: message });
      return this.snapshot();
    }
    if (this.child) {
      return this.waitForReachableState(START_TIMEOUT_MS);
    }
    fs.mkdirSync(this.launch.vaultPath, { recursive: true });
    debugLogger.log("supervisor", "spawn_prepare", {
      binary_path: this.launch.binaryPath,
      vault_path: this.launch.vaultPath,
      cwd: path.dirname(this.launch.vaultPath),
      args: this.buildManagedArgs()
    });
    this.setState({
      phase: "starting",
      hasKey: false,
      unlocked: false,
      hasSystemHomeBundle: false,
      defaultHomeSeedHash: "",
      pid: 0,
      lastError: "",
      startupErrorService: "",
      startupErrorListenAddr: ""
    });
    const child = spawn(this.launch.binaryPath, this.buildManagedArgs(), {
      cwd: path.dirname(this.launch.vaultPath),
      env: buildManagedChildEnv(),
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true
    });
    this.child = child;
    this.attachChildIO(child);
    child.once("error", (error) => {
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("supervisor", "child_error", {
        message
      });
      this.child = null;
      this.stopPolling();
      this.setState({ phase: "error", pid: 0, lastError: message, unlocked: false, hasSystemHomeBundle: false, defaultHomeSeedHash: "" });
    });
    child.once("exit", (code, signal) => {
      this.child = null;
      this.stopPolling();
      if (this.stopping) {
        debugLogger.log("supervisor", "child_stopped", {
          code: code ?? 0,
          signal: signal || ""
        });
        this.setState({ phase: "stopped", pid: 0, unlocked: false });
        return;
      }
      const message = signal
        ? `managed backend exited by signal ${signal}`
        : `managed backend exited with code ${String(code ?? 0)}`;
      debugLogger.log("supervisor", "child_exit_unexpected", {
        code: code ?? 0,
        signal: signal || "",
        message
      });
      this.setState({ phase: "error", pid: 0, unlocked: false, lastError: message, hasSystemHomeBundle: false, defaultHomeSeedHash: "" });
    });
    this.setState({ pid: child.pid ?? 0 });
    debugLogger.log("supervisor", "child_spawned", {
      pid: child.pid ?? 0
    });
    this.startPolling();
    return this.waitForReachableState(START_TIMEOUT_MS);
  }

  private buildManagedArgs(): string[] {
    const args = [
      "-path", this.launch.vaultPath,
      "-network", this.launch.network,
      "-http-listen", this.launch.apiListenAddr,
      "-fs-http-listen", this.launch.fsHTTPListenAddr
    ];
    if (this.launch.systemHomepageBundleDir !== "") {
      args.push("-system-homepage-bundle", this.launch.systemHomepageBundleDir);
    }
    return args;
  }

  private attachChildIO(child: ChildProcessByStdio<null, Readable, Readable>): void {
    child.stdout.on("data", (chunk: Buffer | string) => {
      const extracted = extractLines(chunk, this.stdoutLineRemainder);
      this.stdoutLineRemainder = extracted.remainder;
      const lines = extracted.lines;
      this.appendLogs(lines.filter((line) => !isManagedControlLine(line)));
      for (const line of lines) {
        debugLogger.log("supervisor.stdout", "line", { line });
        this.handleManagedControlLine(line);
      }
    });
    child.stderr.on("data", (chunk: Buffer | string) => {
      const extracted = extractLines(chunk, this.stderrLineRemainder);
      this.stderrLineRemainder = extracted.remainder;
      const lines = extracted.lines;
      this.appendLogs(lines.filter((line) => !isManagedControlLine(line)));
      for (const line of lines) {
        debugLogger.log("supervisor.stderr", "line", { line });
        this.handleManagedControlLine(line);
      }
    });
  }

  private handleManagedControlLine(line: string): void {
    this.handleBootstrapLine(line);
    this.handleManagedEventLine(line);
  }

  private handleBootstrapLine(line: string): void {
    const trimmed = String(line || "").trim();
    if (!trimmed.startsWith(BOOTSTRAP_PREFIX)) {
      return;
    }
    const raw = trimmed.slice(BOOTSTRAP_PREFIX.length);
    try {
      const event = JSON.parse(raw) as BootstrapStateEvent;
      if (String(event.type || "") !== "bootstrap_state") {
        return;
      }
      this.setState({
        phase: event.phase || this.state.phase,
        lastError: String(event.startup_error_message || "").trim(),
        startupErrorService: String(event.startup_error_service || "").trim(),
        startupErrorListenAddr: String(event.startup_error_listen || "").trim(),
        chainAccessMode: String(event.chain_access_mode || "").trim(),
        walletChainBaseURL: String(event.wallet_chain_base_url || "").trim(),
        wocProxyEnabled: Boolean(event.woc_proxy_enabled),
        wocProxyListenAddr: String(event.woc_proxy_listen_addr || "").trim(),
        wocUpstreamRootURL: String(event.woc_upstream_root_url || "").trim(),
        wocMinInterval: String(event.woc_min_interval || "").trim()
      });
    } catch (error) {
      debugLogger.log("supervisor", "bootstrap_line_parse_failed", {
        line: trimmed,
        message: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private handleManagedEventLine(line: string): void {
    const trimmed = String(line || "").trim();
    if (!trimmed.startsWith(MANAGED_EVENT_PREFIX)) {
      return;
    }
    const raw = trimmed.slice(MANAGED_EVENT_PREFIX.length);
    try {
      const event = JSON.parse(raw) as BitfsRuntimeEvent;
      if (!event || typeof event !== "object" || typeof event.topic !== "string" || event.topic.trim() === "") {
        return;
      }
      if (event.topic === "backend.phase.changed" && event.scope === "private") {
        this.applyManagedPhaseEvent(event);
      }
      this.emit("event", event);
    } catch (error) {
      debugLogger.log("supervisor", "managed_event_parse_failed", {
        line: trimmed,
        message: error instanceof Error ? error.message : String(error)
      });
    }
  }

  private applyManagedPhaseEvent(event: BitfsRuntimeEvent): void {
    const payload = event.payload && typeof event.payload === "object" ? event.payload : {};
    this.setState({
      phase: normalizeManagedPhase(payload.phase, this.state.phase),
      unlocked: Boolean(payload.unlocked ?? this.state.unlocked),
      lastError: readStringRecordField(payload, "startup_error_message") || this.state.lastError,
      startupErrorService: readStringRecordField(payload, "startup_error_service") || this.state.startupErrorService,
      startupErrorListenAddr: readStringRecordField(payload, "startup_error_listen") || this.state.startupErrorListenAddr,
      chainAccessMode: readStringRecordField(payload, "chain_access_mode") || this.state.chainAccessMode,
      walletChainBaseURL: readStringRecordField(payload, "wallet_chain_base_url") || this.state.walletChainBaseURL,
      wocProxyEnabled: hasRecordField(payload, "woc_proxy_enabled")
        ? readBooleanRecordField(payload, "woc_proxy_enabled")
        : this.state.wocProxyEnabled,
      wocProxyListenAddr: readStringRecordField(payload, "woc_proxy_listen_addr") || this.state.wocProxyListenAddr,
      wocUpstreamRootURL: readStringRecordField(payload, "woc_upstream_root_url") || this.state.wocUpstreamRootURL,
      wocMinInterval: readStringRecordField(payload, "woc_min_interval") || this.state.wocMinInterval,
      hasSystemHomeBundle: hasRecordField(payload, "has_system_home_bundle")
        ? readBooleanRecordField(payload, "has_system_home_bundle")
        : this.state.hasSystemHomeBundle,
      defaultHomeSeedHash: normalizeSeedHash(readStringRecordField(payload, "default_home_seed_hash") || this.state.defaultHomeSeedHash)
    });
  }

  private appendLogs(lines: string[]): void {
    if (lines.length === 0) {
      return;
    }
    const next = [...this.state.recentLogs];
    for (const line of lines) {
      if (!line) {
        continue;
      }
      next.push(line);
      if (next.length > LOG_BUFFER_LIMIT) {
        next.splice(0, next.length - LOG_BUFFER_LIMIT);
      }
    }
    this.setState({ recentLogs: next });
  }

  private startPolling(): void {
    this.stopPolling();
    debugLogger.log("supervisor", "status_poll_start", {
      interval_ms: STATUS_POLL_INTERVAL_MS
    });
    this.pollTimer = setInterval(() => {
      void this.refreshStatus();
    }, STATUS_POLL_INTERVAL_MS);
  }

  private stopPolling(): void {
    if (!this.pollTimer) {
      return;
    }
    clearInterval(this.pollTimer);
    this.pollTimer = null;
    debugLogger.log("supervisor", "status_poll_stop");
  }

  private async refreshStatus(): Promise<ManagedClientState> {
    if (!this.child) {
      return this.snapshot();
    }
    if (this.state.phase === "startup_error") {
      return this.snapshot();
    }
    try {
      const status = await this.fetchJSON<KeyStatusResponse>("/api/v1/key/status");
      const unlocked = Boolean(status.unlocked);
      const statusPhase = status.phase;
      const phase: ManagedClientPhase = statusPhase || (unlocked ? "ready" : "locked");
      this.setState({
        phase,
        hasKey: Boolean(status.has_key),
        unlocked,
        hasSystemHomeBundle: Boolean(status.has_system_home_bundle),
        defaultHomeSeedHash: normalizeSeedHash(status.default_home_seed_hash),
        pid: this.child.pid ?? 0,
        lastError: String(status.startup_error_message || "").trim(),
        startupErrorService: String(status.startup_error_service || "").trim(),
        startupErrorListenAddr: String(status.startup_error_listen || "").trim(),
        chainAccessMode: String(status.chain_access_mode || "").trim(),
        walletChainBaseURL: String(status.wallet_chain_base_url || "").trim(),
        wocProxyEnabled: Boolean(status.woc_proxy_enabled),
        wocProxyListenAddr: String(status.woc_proxy_listen_addr || "").trim(),
        wocUpstreamRootURL: String(status.woc_upstream_root_url || "").trim(),
        wocMinInterval: String(status.woc_min_interval || "").trim()
      });
      debugLogger.log("supervisor", "status_refreshed", {
        phase,
        has_key: Boolean(status.has_key),
        unlocked,
        has_system_home_bundle: Boolean(status.has_system_home_bundle),
        default_home_seed_hash: normalizeSeedHash(status.default_home_seed_hash),
        pid: this.child.pid ?? 0,
        startup_error_service: String(status.startup_error_service || "").trim(),
        startup_error_listen: String(status.startup_error_listen || "").trim(),
        chain_access_mode: String(status.chain_access_mode || "").trim()
      });
    } catch (error) {
      if (this.state.phase === "starting") {
        debugLogger.log("supervisor", "status_refresh_retrying_during_start", {
          message: error instanceof Error ? error.message : String(error)
        });
        return this.snapshot();
      }
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("supervisor", "status_refresh_failed", {
        message
      });
      this.setState({ lastError: message });
    }
    return this.snapshot();
  }

  private async waitForReachableState(timeoutMs: number): Promise<ManagedClientState> {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      const state = await this.refreshStatus();
      if (state.phase === "locked" || state.phase === "ready" || state.phase === "startup_error") {
        return state;
      }
      if (state.phase === "error") {
        return state;
      }
      await delay(250);
    }
    const message = `managed backend did not become reachable within ${timeoutMs}ms`;
    debugLogger.log("supervisor", "reachability_timeout", {
      timeout_ms: timeoutMs
    });
    this.setState({ phase: "error", lastError: message });
    return this.snapshot();
  }

  private async waitForState(
    predicate: (state: ManagedClientState) => boolean,
    timeoutMs: number
  ): Promise<ManagedClientState> {
    const current = this.snapshot();
    if (predicate(current)) {
      return current;
    }
    return new Promise<ManagedClientState>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        debugLogger.log("supervisor", "wait_state_timeout", {
          timeout_ms: timeoutMs
        });
        reject(new Error(`managed backend state wait timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      const handle = (state: ManagedClientState) => {
        if (!predicate(state)) {
          return;
        }
        cleanup();
        resolve(state);
      };
      const cleanup = () => {
        clearTimeout(timer);
        this.removeListener("state", handle);
      };
      this.on("state", handle);
    });
  }

  private async fetchJSON<T>(pathname: string, init?: RequestInit, timeoutMs = 3_000): Promise<T> {
    const target = `${this.launch.apiBase}${pathname}`;
    const method = String(init?.method || "GET");
    debugLogger.log("supervisor.http", "request", {
      method,
      url: target,
      timeout_ms: timeoutMs
    });
    const response = await fetch(target, {
      ...init,
      signal: AbortSignal.timeout(timeoutMs)
    });
    if (!response.ok) {
      const text = (await response.text()).trim();
      debugLogger.log("supervisor.http", "response_error", {
        method,
        url: target,
        status: response.status,
        body: text
      });
      throw new Error(normalizeManagedHTTPError(text, response.status));
    }
    debugLogger.log("supervisor.http", "response_ok", {
      method,
      url: target,
      status: response.status
    });
    const text = (await response.text()).trim();
    if (text === "") {
      return {} as T;
    }
    try {
      return JSON.parse(text) as T;
    } catch {
      return text as T;
    }
  }

  private async postJSON(pathname: string, body: Record<string, unknown>): Promise<unknown> {
    return this.fetchJSON(pathname, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body)
    });
  }

  private setState(next: Partial<ManagedClientState>): void {
    const previous = this.state;
    this.state = {
      ...this.state,
      ...next
    };
    if (this.state.phase !== "starting") {
      this.stopPolling();
    }
    if (stateChanged(previous, this.state)) {
      debugLogger.log("supervisor", "state_changed", {
        from_phase: previous.phase,
        to_phase: this.state.phase,
        has_key: this.state.hasKey,
        unlocked: this.state.unlocked,
        pid: this.state.pid,
        has_system_home_bundle: this.state.hasSystemHomeBundle,
        default_home_seed_hash: this.state.defaultHomeSeedHash,
        last_error: this.state.lastError
      });
    }
    this.emit("state", this.snapshot());
  }
}

function resolveManagedClientBinaryPath(appRootDir: string, packaged: boolean): string {
  const override = String(process.env.BITFS_CLIENT_BINARY || "").trim();
  if (override) {
    return path.resolve(override);
  }
  if (parseManagedBoolEnv(process.env.BITFS_ELECTRON_E2E)) {
    throw new Error("BITFS_CLIENT_BINARY is required in Electron e2e");
  }
  const platformKey = `${process.platform}-${process.arch}`;
  const binaryName = process.platform === "win32"
    ? "bitfs-client-electron-backend.exe"
    : "bitfs-client-electron-backend";
  if (packaged) {
    return path.join(process.resourcesPath, "bin", platformKey, binaryName);
  }
  return path.join(appRootDir, "resources", "bin", platformKey, binaryName);
}

function buildJSONRequestInit(method: string | undefined, body: unknown, headers?: Record<string, string>): RequestInit {
  const normalizedMethod = String(method || "GET").trim().toUpperCase() || "GET";
  const init: RequestInit = {
    method: normalizedMethod
  };
  const nextHeaders: Record<string, string> = { ...(headers || {}) };
  if (body !== undefined) {
    nextHeaders["content-type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  if (Object.keys(nextHeaders).length > 0) {
    init.headers = nextHeaders;
  }
  return init;
}

function normalizeManagedHTTPError(text: string, status: number): string {
  const value = String(text || "").trim();
  if (value === "") {
    return `request failed: ${status}`;
  }
  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    const message = typeof parsed.error === "string" ? parsed.error.trim() : "";
    if (message !== "") {
      return message;
    }
  } catch {
    // 保留原始文本
  }
  return value;
}

function buildManagedChildEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env.ELECTRON_RUN_AS_NODE;
  env.BITFS_ELECTRON_MANAGED = "1";
  env.BITFS_MANAGED_CONTROL_STREAM = "1";
  return env;
}

function isManagedControlLine(line: string): boolean {
  const trimmed = String(line || "").trim();
  return trimmed.startsWith(BOOTSTRAP_PREFIX) || trimmed.startsWith(MANAGED_EVENT_PREFIX);
}

function parseManagedBoolEnv(raw: string | undefined): boolean {
  const value = String(raw || "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function normalizeManagedPhase(value: unknown, fallback: ManagedClientPhase): ManagedClientPhase {
  const phase = String(value || "").trim();
  switch (phase) {
    case "starting":
    case "startup_error":
    case "locked":
    case "ready":
    case "error":
    case "stopped":
      return phase;
    default:
      return fallback;
  }
}

function readStringRecordField(source: Record<string, unknown>, key: string): string {
  const value = source[key];
  if (typeof value === "string") {
    return value.trim();
  }
  return "";
}

function hasRecordField(source: Record<string, unknown>, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(source, key);
}

function readBooleanRecordField(source: Record<string, unknown>, key: string): boolean {
  const value = source[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "1" || normalized === "true" || normalized === "yes";
  }
  return false;
}

function parseOptionalPort(raw: string | undefined): number | null {
  const value = Number.parseInt(String(raw || "").trim(), 10);
  if (!Number.isInteger(value) || value <= 0 || value > 65535) {
    return null;
  }
  return value;
}

function resolveSystemHomepageBundleDir(appRootDir: string, packaged: boolean): string {
  const override = String(process.env.BITFS_SYSTEM_HOMEPAGE_BUNDLE || "").trim();
  if (override) {
    return path.resolve(override);
  }
  const bundleDir = packaged
    ? path.join(process.resourcesPath, "homepage", "dist-hash")
    : path.join(appRootDir, "hash-homepage", "dist-hash");
  if (!fs.existsSync(path.join(bundleDir, "manifest.json"))) {
    return "";
  }
  return bundleDir;
}

function normalizeSeedHash(raw: string | undefined): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(value)) {
    return "";
  }
  return value;
}

async function pickFreePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("failed to allocate free port")));
        return;
      }
      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

function extractLines(chunk: Buffer | string, remainder: string): { lines: string[]; remainder: string } {
  const text = remainder + String(chunk);
  const parts = text.split(/\r?\n/);
  const nextRemainder = parts.pop() ?? "";
  return {
    lines: parts.map((line) => line.trim()).filter((line) => line !== ""),
    remainder: nextRemainder
  };
}

function requirePassword(password: string): string {
  const value = String(password || "");
  if (value.trim() === "") {
    throw new Error("password is required");
  }
  return value;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function raceTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<boolean> {
  let timedOut = false;
  await Promise.race([
    promise,
    new Promise<void>((resolve) => {
      setTimeout(() => {
        timedOut = true;
        resolve();
      }, timeoutMs);
    })
  ]);
  return timedOut;
}

function stateChanged(previous: ManagedClientState, next: ManagedClientState): boolean {
  return previous.phase !== next.phase ||
    previous.hasKey !== next.hasKey ||
    previous.unlocked !== next.unlocked ||
    previous.hasSystemHomeBundle !== next.hasSystemHomeBundle ||
    previous.defaultHomeSeedHash !== next.defaultHomeSeedHash ||
    previous.pid !== next.pid ||
    previous.lastError !== next.lastError ||
    previous.startupErrorService !== next.startupErrorService ||
    previous.startupErrorListenAddr !== next.startupErrorListenAddr ||
    previous.chainAccessMode !== next.chainAccessMode ||
    previous.walletChainBaseURL !== next.walletChainBaseURL ||
    previous.wocProxyEnabled !== next.wocProxyEnabled ||
    previous.wocProxyListenAddr !== next.wocProxyListenAddr ||
    previous.wocUpstreamRootURL !== next.wocUpstreamRootURL ||
    previous.wocMinInterval !== next.wocMinInterval;
}
