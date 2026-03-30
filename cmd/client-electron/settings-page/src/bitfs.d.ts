export type ShellState = {
  currentURL: string;
  currentViewerURL: string;
  currentRootSeedHash: string;
  currentVisit?: {
    visitID: string;
    locator: string;
    status: string;
    startedAtUnix: number;
    lastUpdatedAtUnix: number;
    finishedAtUnix: number;
    note: string;
    totalUsedSatoshi: number;
    totalReturnedSatoshi: number;
    resolverUsedSatoshi: number;
    reachabilityUsedSatoshi: number;
    contentUsedSatoshi: number;
    otherUsedSatoshi: number;
    itemCount: number;
  };
  userHomeSeedHash: string;
  settingsPageURL: string;
  settingsPreloadPath: string;
  backend: {
    backendPhase: string;
    runtimePhase: string;
    keyState: string;
    pid: number;
    apiBase: string;
    vaultPath: string;
    binaryPath: string;
    defaultHomeSeedHash: string;
    chainAccessMode: string;
    walletChainBaseURL: string;
    wocProxyEnabled: boolean;
    wocProxyListenAddr: string;
    wocUpstreamRootURL: string;
    wocMinInterval: string;
    lastError: string;
    runtimeErrorMessage: string;
  };
};

type KeyFileActionResult = {
  cancelled: boolean;
  filePath: string;
  state: ShellState;
};

type DirectoryPickResult = {
  cancelled: boolean;
  path: string;
};

type StaticUploadResult = {
  cancelled: boolean;
  path: string;
  result: Record<string, unknown> | null;
};

type BitfsRuntimeEvent = {
  seq: number;
  runtime_epoch: string;
  topic: string;
  scope: "private" | "public";
  occurred_at_unix: number;
  producer: string;
  trace_id: string;
  payload: Record<string, unknown>;
};

type BitfsSettingsBridge = {
  getShellState(): Promise<ShellState>;
  getWalletSummary(): Promise<Record<string, unknown>>;
  setUserHomepage(seedHash: string): Promise<ShellState>;
  clearUserHomepage(): Promise<ShellState>;
  lock(): Promise<ShellState>;
  restartBackend(): Promise<ShellState>;
  exportKeyFile(): Promise<KeyFileActionResult>;
  request(method: string, path: string, body?: unknown): Promise<unknown>;
  pickDirectory(): Promise<DirectoryPickResult>;
  uploadStaticFile(targetDir: string, overwrite: boolean): Promise<StaticUploadResult>;
  onShellState(listener: (state: ShellState) => void): () => void;
  events: {
    subscribe(topics: string | string[], listener: (event: BitfsRuntimeEvent) => void): () => void;
  };
};

declare global {
  interface Window {
    bitfsSettings: BitfsSettingsBridge;
  }
}

export {};
