export type ShellResourceMode = "local" | "auto" | "approved" | "pending" | "blocked";

export type ShellSidebarPanel = "resources" | "wallet";

export type ShellVisitStatus = "idle" | "opening" | "open" | "failed";

export type BitfsEventScope = "private" | "public";

export type BitfsRuntimeEvent = {
  seq: number;
  runtime_epoch: string;
  topic: string;
  scope: BitfsEventScope;
  occurred_at_unix: number;
  producer: string;
  trace_id: string;
  payload: Record<string, unknown>;
};

export type ShellResource = {
  seedHash: string;
  estimatedTotalSat: number;
  seedPriceSat: number;
  chunkPriceSatPer64K: number;
  fileSize: number;
  chunkCount: number;
  mimeHint: string;
  recommendedFileName: string;
  reason: string;
  mode: ShellResourceMode;
  allowed: boolean;
  planStatus: string;
  localReady: boolean;
  isRoot: boolean;
  discoveryOrder: number;
};

export type ShellVisitAccountingBucket = {
  purpose: string;
  label: string;
  usedSatoshi: number;
  count: number;
};

export type ShellVisitAccounting = {
  visitID: string;
  locator: string;
  status: ShellVisitStatus;
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
  buckets: ShellVisitAccountingBucket[];
};

export type ManagedClientPhase = "starting" | "startup_error" | "locked" | "ready" | "error" | "stopped";

export type ManagedClientState = {
  phase: ManagedClientPhase;
  hasKey: boolean;
  unlocked: boolean;
  hasSystemHomeBundle: boolean;
  defaultHomeSeedHash: string;
  pid: number;
  apiBase: string;
  fsHTTPListenAddr: string;
  vaultPath: string;
  binaryPath: string;
  lastError: string;
  startupErrorService: string;
  startupErrorListenAddr: string;
  chainAccessMode: string;
  walletChainBaseURL: string;
  wocProxyEnabled: boolean;
  wocProxyListenAddr: string;
  wocUpstreamRootURL: string;
  wocMinInterval: string;
  recentLogs: string[];
};

export type KeyFileActionResult = {
  cancelled: boolean;
  filePath: string;
  state: ShellState;
};

export type BitfsPublicClientInfo = {
  trusted_protocol: "bitfs://";
  pubkey_hex: string;
  started_at_unix: number;
  seller_enabled: boolean;
};

export type BitfsPublicClientStatus = {
  trusted_protocol: "bitfs://";
  current_url: string;
  current_root_seed_hash: string;
  wallet_ready: boolean;
  wallet_unlocked: boolean;
};

export type BitfsPublicWalletAddress = {
  address: string;
  encoding: "base58";
  purpose: "default_receive";
  pubkey_hex: string;
};

export type BitfsPublicWalletSummary = {
  trusted_protocol: "bitfs://";
  pubkey_hex: string;
  wallet_address: string;
  addresses: BitfsPublicWalletAddress[];
  balance_satoshi: number;
};

export type BitfsPublicWalletHistoryDirection = "in" | "out" | "unknown";

export type BitfsPublicWalletHistoryItem = {
  id: number;
  txid: string;
  direction: BitfsPublicWalletHistoryDirection;
  amount_satoshi: number;
  status: string;
  block_height: number;
  occurred_at_unix: number;
};

export type BitfsPublicWalletHistoryList = {
  total: number;
  limit: number;
  offset: number;
  items: BitfsPublicWalletHistoryItem[];
};

export type ShellState = {
  currentURL: string;
  currentViewerURL: string;
  currentRootSeedHash: string;
  currentVisit: ShellVisitAccounting;
  staticSingleMaxSat: number;
  staticPageMaxSat: number;
  autoSpentSat: number;
  pendingCount: number;
  resources: ShellResource[];
  lastError: string;
  clientAPIBase: string;
  viewerPreloadPath: string;
  settingsPageURL: string;
  settingsPreloadPath: string;
  userHomeSeedHash: string;
  sidebarWidthPx: number;
  activePanel: ShellSidebarPanel;
  backend: ManagedClientState;
};
