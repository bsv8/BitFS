import { spawn, type ChildProcessByStdio } from "node:child_process";
import type { Readable } from "node:stream";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import net from "node:net";
import path from "node:path";

import type {
  BitfsRuntimeEvent,
  ManagedClientBackendPhase,
  ManagedClientKeyState,
  ManagedClientRuntimePhase,
  ManagedClientState
} from "../shared/shell_contract";
import { debugLogger } from "./debug_logger";
import { ManagedControlChannel } from "./managed_control_channel";

const START_TIMEOUT_MS = 20_000;
const READY_TIMEOUT_MS = 30_000;
const LOG_BUFFER_LIMIT = 80;

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

type ManagedWalletSummaryEnvelopeError = {
  code?: string;
  message?: string;
};

type ManagedWalletSummaryEnvelope<T> = {
  status: string;
  data?: T;
  error?: ManagedWalletSummaryEnvelopeError | string | null;
};

type StaticUploadRequest = {
  filePath: string;
  fileName?: string;
  targetDir: string;
  overwrite: boolean;
};

export class ManagedClientSupervisor extends EventEmitter {
  private readonly launch: ManagedClientLaunchConfig;
  private child: ChildProcessByStdio<null, Readable, Readable> | null = null;
  private controlChannel: ManagedControlChannel | null = null;
  private controlRuntimeEpoch = "";
  private controlLastSeq = 0;
  private state: ManagedClientState;
  private startPromise: Promise<ManagedClientState> | null = null;
  private stopping = false;
  private stdoutLineRemainder = "";
  private stderrLineRemainder = "";

  private constructor(launch: ManagedClientLaunchConfig) {
    super();
    this.launch = launch;
    this.state = {
      backendPhase: "stopped",
      runtimePhase: "stopped",
      keyState: "missing",
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
      runtimeErrorMessage: "",
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
      backend_phase: this.state.backendPhase,
      runtime_phase: this.state.runtimePhase,
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
      backend_phase: this.state.backendPhase,
      runtime_phase: this.state.runtimePhase
    });
    this.stopping = true;
    const child = this.child;
    if (!child) {
      await this.closeControlChannel();
      this.setState({
        backendPhase: "stopped",
        runtimePhase: "stopped",
        keyState: "missing",
        pid: 0,
        hasSystemHomeBundle: false,
        defaultHomeSeedHash: "",
        lastError: "",
        startupErrorService: "",
        startupErrorListenAddr: "",
        runtimeErrorMessage: ""
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
    await this.closeControlChannel();
    this.setState({
      backendPhase: "stopped",
      runtimePhase: "stopped",
      keyState: "missing",
      pid: 0,
      hasSystemHomeBundle: false,
      defaultHomeSeedHash: "",
      lastError: "",
      startupErrorService: "",
      startupErrorListenAddr: "",
      runtimeErrorMessage: ""
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
    return this.waitForState((state) => state.keyState !== "missing", READY_TIMEOUT_MS);
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
    return this.waitForState((state) => state.keyState === "locked", READY_TIMEOUT_MS);
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
    // 设计说明：
    // - `unlock` 现在只要求 daemon 已经接受这次解锁并开始处理；
    // - renderer 再继续监听状态流，看 `runtimePhase` 从 `starting` 走到 `ready` 或 `error`。
    return this.waitForState(
      (state) => state.keyState === "unlocked" && state.runtimePhase !== "stopped",
      READY_TIMEOUT_MS
    );
  }

  async lock(): Promise<ManagedClientState> {
    debugLogger.log("supervisor", "lock_requested");
    await this.ensureReachable();
    await this.postJSON("/api/v1/key/lock", {});
    return this.waitForState((state) => state.keyState !== "unlocked" && state.runtimePhase === "stopped", READY_TIMEOUT_MS);
  }

  async requestManagedJSON<T>(request: ManagedJSONRequest): Promise<T> {
    await this.ensureReachable();
    return this.fetchJSON<T>(
      request.pathname,
      buildJSONRequestInit(request.method, request.body, request.headers),
      Number(request.timeout_ms || 0)
    );
  }

  async requestManagedWalletSummaryJSON<T>(request: ManagedJSONRequest): Promise<T> {
    await this.ensureReachable();
    const pathname = request.pathname;
    const target = `${this.launch.apiBase}${pathname}`;
    const method = String(request.method || "GET");
    const timeoutMs = Number(request.timeout_ms || 0) || 3_000;
    debugLogger.log("supervisor.http", "request", {
      method,
      url: target,
      timeout_ms: timeoutMs
    });
    const response = await fetch(target, {
      ...buildJSONRequestInit(request.method, request.body, request.headers),
      signal: AbortSignal.timeout(timeoutMs)
    });
    const text = (await response.text()).trim();
    let payload: ManagedWalletSummaryEnvelope<T> | null = null;
    if (text !== "") {
      try {
        payload = JSON.parse(text) as ManagedWalletSummaryEnvelope<T>;
      } catch {
        payload = null;
      }
    }
    if (payload && typeof payload === "object") {
      if (String(payload.status || "") === "ok") {
        if (!Object.prototype.hasOwnProperty.call(payload, "data")) {
          debugLogger.log("supervisor.http", "response_error", {
            method,
            url: target,
            status: response.status,
            body: text
          });
          throw new Error("invalid status envelope");
        }
        if (!response.ok) {
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
        return payload.data as T;
      }
      if (String(payload.status || "") === "error") {
        const code = typeof payload.error === "object" && payload.error !== null && !Array.isArray(payload.error)
          ? String(payload.error.code || "").trim()
          : "";
        const message = typeof payload.error === "object" && payload.error !== null && !Array.isArray(payload.error)
          ? String(payload.error.message || "").trim()
          : String(payload.error || "").trim();
        const errorText = code && message ? `${code}: ${message}` : (message || code || `request failed: ${response.status}`);
        debugLogger.log("supervisor.http", "response_error", {
          method,
          url: target,
          status: response.status,
          code,
          body: text
        });
        throw new Error(errorText);
      }
    }
    if (!response.ok) {
      debugLogger.log("supervisor.http", "response_error", {
        method,
        url: target,
        status: response.status,
        body: text
      });
      throw new Error(normalizeManagedHTTPError(text, response.status));
    }
    debugLogger.log("supervisor.http", "response_error", {
      method,
      url: target,
      status: response.status,
      body: text
    });
    throw new Error("invalid status envelope");
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
      backend_phase: state.backendPhase,
      runtime_phase: state.runtimePhase,
      key_state: state.keyState
    });
    if (state.backendPhase === "startup_error") {
      throw new Error(state.lastError || "managed backend start failed");
    }
  }

  private async startInternal(): Promise<ManagedClientState> {
    if (!fs.existsSync(this.launch.binaryPath)) {
      const message = `managed backend binary not found: ${this.launch.binaryPath}`;
      debugLogger.log("supervisor", "binary_missing", {
        binary_path: this.launch.binaryPath
      });
      this.setState({ backendPhase: "startup_error", lastError: message });
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
    await this.closeControlChannel();
    let controlChannel: ManagedControlChannel;
    try {
      controlChannel = await ManagedControlChannel.create();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("supervisor", "control_channel_create_failed", {
        message
      });
      this.setState({ backendPhase: "startup_error", lastError: message });
      return this.snapshot();
    }
    this.controlChannel = controlChannel;
    this.controlRuntimeEpoch = "";
    this.controlLastSeq = 0;
    debugLogger.log("supervisor", "control_channel_ready", {
      endpoint: controlChannel.getEndpoint()
    });
    controlChannel.on("hello", (payload: { runtime_epoch?: string }) => {
      const runtimeEpoch = String(payload?.runtime_epoch || "").trim();
      if (runtimeEpoch === "") {
        return;
      }
      this.controlRuntimeEpoch = runtimeEpoch;
      this.controlLastSeq = 0;
      debugLogger.log("supervisor", "control_channel_hello", {
        runtime_epoch: runtimeEpoch
      });
    });
    controlChannel.on("event", (event: BitfsRuntimeEvent) => {
      this.handleManagedControlEvent(event);
    });
    controlChannel.on("channel_error", (error: Error) => {
      debugLogger.log("supervisor", "control_channel_error", {
        message: error instanceof Error ? error.message : String(error)
      });
    });
    this.setState({
      backendPhase: "starting",
      runtimePhase: "stopped",
      keyState: "missing",
      hasSystemHomeBundle: false,
      defaultHomeSeedHash: "",
      pid: 0,
      lastError: "",
      startupErrorService: "",
      startupErrorListenAddr: "",
      runtimeErrorMessage: ""
    });
    const child = spawn(this.launch.binaryPath, this.buildManagedArgs(), {
      cwd: path.dirname(this.launch.vaultPath),
      env: buildManagedChildEnv(controlChannel),
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
      void this.closeControlChannel().finally(() => {
        this.setState({
          backendPhase: "startup_error",
          runtimePhase: "stopped",
          keyState: "missing",
          pid: 0,
          lastError: message,
          hasSystemHomeBundle: false,
          defaultHomeSeedHash: "",
          runtimeErrorMessage: ""
        });
      });
    });
    child.once("exit", (code, signal) => {
      this.child = null;
      void this.closeControlChannel().finally(() => {
        if (this.stopping) {
          debugLogger.log("supervisor", "child_stopped", {
            code: code ?? 0,
            signal: signal || ""
          });
          this.setState({ backendPhase: "stopped", runtimePhase: "stopped", keyState: "missing", pid: 0, runtimeErrorMessage: "" });
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
        this.setState({
          backendPhase: this.state.backendPhase === "available" ? "stopped" : "startup_error",
          runtimePhase: "stopped",
          keyState: "missing",
          pid: 0,
          lastError: message,
          hasSystemHomeBundle: false,
          defaultHomeSeedHash: "",
          runtimeErrorMessage: ""
        });
      });
    });
    this.setState({ pid: child.pid ?? 0 });
    debugLogger.log("supervisor", "child_spawned", {
      pid: child.pid ?? 0
    });
    return this.waitForReachableState(START_TIMEOUT_MS);
  }

  private buildManagedArgs(): string[] {
    const args = [
      "-config-root", this.launch.vaultPath,
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
      this.appendLogs(lines);
      for (const line of lines) {
        debugLogger.log("supervisor.stdout", "line", { line });
      }
    });
    child.stderr.on("data", (chunk: Buffer | string) => {
      const extracted = extractLines(chunk, this.stderrLineRemainder);
      this.stderrLineRemainder = extracted.remainder;
      const lines = extracted.lines;
      this.appendLogs(lines);
      for (const line of lines) {
        debugLogger.log("supervisor.stderr", "line", { line });
      }
    });
  }

  private handleManagedControlEvent(event: BitfsRuntimeEvent): void {
    const runtimeEpoch = String(event?.runtime_epoch || "").trim();
    if (runtimeEpoch === "") {
      return;
    }
    if (this.controlRuntimeEpoch === "") {
      this.controlRuntimeEpoch = runtimeEpoch;
      this.controlLastSeq = 0;
    }
    if (runtimeEpoch !== this.controlRuntimeEpoch) {
      debugLogger.log("supervisor", "control_event_ignored_epoch_mismatch", {
        runtime_epoch: runtimeEpoch,
        expected_runtime_epoch: this.controlRuntimeEpoch,
        topic: String(event.topic || "")
      });
      return;
    }
    const seq = Number(event.seq || 0);
    if (!Number.isInteger(seq) || seq <= this.controlLastSeq) {
      debugLogger.log("supervisor", "control_event_ignored_stale_seq", {
        seq,
        last_seq: this.controlLastSeq,
        topic: String(event.topic || "")
      });
      return;
    }
    this.controlLastSeq = seq;
    if (event.topic === "backend.snapshot" && event.scope === "private") {
      this.applyManagedSnapshotEvent(event);
    }
    this.emit("event", event);
  }

  private applyManagedSnapshotEvent(event: BitfsRuntimeEvent): void {
    const payload = event.payload && typeof event.payload === "object" ? event.payload : {};
    const startupErrorMessage = readStringRecordField(payload, "startup_error_message");
    const runtimeErrorMessage = readStringRecordField(payload, "runtime_error_message");
    this.setState({
      backendPhase: normalizeManagedBackendPhase(payload.backend_phase, this.state.backendPhase),
      runtimePhase: normalizeManagedRuntimePhase(payload.runtime_phase, this.state.runtimePhase),
      keyState: normalizeManagedKeyState(payload.key_state, this.state.keyState),
      fsHTTPListenAddr: readStringRecordField(payload, "fs_http_listen_addr") || this.state.fsHTTPListenAddr,
      pid: this.child?.pid ?? 0,
      lastError: readStringRecordField(payload, "last_error") || startupErrorMessage || runtimeErrorMessage,
      startupErrorService: readStringRecordField(payload, "startup_error_service"),
      startupErrorListenAddr: readStringRecordField(payload, "startup_error_listen"),
      runtimeErrorMessage,
      chainAccessMode: readStringRecordField(payload, "chain_access_mode"),
      walletChainBaseURL: readStringRecordField(payload, "wallet_chain_base_url"),
      wocProxyEnabled: hasRecordField(payload, "woc_proxy_enabled")
        ? readBooleanRecordField(payload, "woc_proxy_enabled")
        : this.state.wocProxyEnabled,
      wocProxyListenAddr: readStringRecordField(payload, "woc_proxy_listen_addr"),
      wocUpstreamRootURL: readStringRecordField(payload, "woc_upstream_root_url"),
      wocMinInterval: readStringRecordField(payload, "woc_min_interval"),
      hasSystemHomeBundle: hasRecordField(payload, "has_system_home_bundle")
        ? readBooleanRecordField(payload, "has_system_home_bundle")
        : this.state.hasSystemHomeBundle,
      defaultHomeSeedHash: normalizeSeedHash(readStringRecordField(payload, "default_home_seed_hash"))
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

  private async waitForReachableState(timeoutMs: number): Promise<ManagedClientState> {
    const current = this.snapshot();
    if (isReachablePhase(current.backendPhase)) {
      return current;
    }
    try {
      return await this.waitForState((state) => isReachablePhase(state.backendPhase), timeoutMs);
    } catch {
      const message = `managed backend did not publish startup state within ${timeoutMs}ms`;
      debugLogger.log("supervisor", "reachability_timeout", {
        timeout_ms: timeoutMs
      });
      this.setState({ backendPhase: "startup_error", lastError: message });
      return this.snapshot();
    }
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

  private async closeControlChannel(): Promise<void> {
    const channel = this.controlChannel;
    this.controlChannel = null;
    this.controlRuntimeEpoch = "";
    this.controlLastSeq = 0;
    if (!channel) {
      return;
    }
    await channel.close();
  }

  private setState(next: Partial<ManagedClientState>): void {
    const previous = this.state;
    this.state = {
      ...this.state,
      ...next
    };
    if (stateChanged(previous, this.state)) {
      debugLogger.log("supervisor", "state_changed", {
        from_backend_phase: previous.backendPhase,
        to_backend_phase: this.state.backendPhase,
        from_runtime_phase: previous.runtimePhase,
        to_runtime_phase: this.state.runtimePhase,
        key_state: this.state.keyState,
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

function buildManagedChildEnv(controlChannel: ManagedControlChannel): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env.ELECTRON_RUN_AS_NODE;
  env.BITFS_ELECTRON_MANAGED = "1";
  env.BITFS_MANAGED_CONTROL_ENDPOINT = controlChannel.getEndpoint();
  env.BITFS_MANAGED_CONTROL_TOKEN = controlChannel.getToken();
  return env;
}

function parseManagedBoolEnv(raw: string | undefined): boolean {
  const value = String(raw || "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function normalizeManagedBackendPhase(value: unknown, fallback: ManagedClientBackendPhase): ManagedClientBackendPhase {
  const phase = String(value || "").trim();
  switch (phase) {
    case "starting":
    case "available":
    case "startup_error":
    case "stopped":
      return phase;
    default:
      return fallback;
  }
}

function normalizeManagedRuntimePhase(value: unknown, fallback: ManagedClientRuntimePhase): ManagedClientRuntimePhase {
  const phase = String(value || "").trim();
  switch (phase) {
    case "stopped":
    case "starting":
    case "ready":
    case "error":
      return phase;
    default:
      return fallback;
  }
}

function normalizeManagedKeyState(value: unknown, fallback: ManagedClientKeyState): ManagedClientKeyState {
  const state = String(value || "").trim();
  switch (state) {
    case "missing":
    case "locked":
    case "unlocked":
      return state;
    default:
      return fallback;
  }
}

function isReachablePhase(phase: ManagedClientBackendPhase): boolean {
  return phase === "available" || phase === "startup_error";
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
  return previous.backendPhase !== next.backendPhase ||
    previous.runtimePhase !== next.runtimePhase ||
    previous.keyState !== next.keyState ||
    previous.hasSystemHomeBundle !== next.hasSystemHomeBundle ||
    previous.defaultHomeSeedHash !== next.defaultHomeSeedHash ||
    previous.pid !== next.pid ||
    previous.lastError !== next.lastError ||
    previous.startupErrorService !== next.startupErrorService ||
    previous.startupErrorListenAddr !== next.startupErrorListenAddr ||
    previous.runtimeErrorMessage !== next.runtimeErrorMessage ||
    previous.chainAccessMode !== next.chainAccessMode ||
    previous.walletChainBaseURL !== next.walletChainBaseURL ||
    previous.wocProxyEnabled !== next.wocProxyEnabled ||
    previous.wocProxyListenAddr !== next.wocProxyListenAddr ||
    previous.wocUpstreamRootURL !== next.wocUpstreamRootURL ||
    previous.wocMinInterval !== next.wocMinInterval;
}
