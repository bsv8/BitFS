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

export type BitfsPublicWalletTokenStandard = "bsv20" | "bsv21";

export type BitfsPublicWalletTokenBalanceItem = {
  token_standard: BitfsPublicWalletTokenStandard;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  output_count: number;
  source_name: string;
  updated_at_unix: number;
};

export type BitfsPublicWalletTokenBalanceList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsPublicWalletTokenBalanceItem[];
};

export type BitfsPublicWalletTokenOutputItem = {
  utxo_id: string;
  wallet_address: string;
  txid: string;
  vout: number;
  value_satoshi: number;
  allocation_class: string;
  allocation_reason: string;
  token_standard: BitfsPublicWalletTokenStandard;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  source_name: string;
  updated_at_unix: number;
  payload: unknown;
};

export type BitfsPublicWalletTokenOutputList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsPublicWalletTokenOutputItem[];
};

export type BitfsPublicWalletAssetEventItem = {
  id: number;
  created_at_unix: number;
  utxo_id: string;
  wallet_address: string;
  asset_group: string;
  asset_standard: string;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  source_name: string;
  event_type: string;
  ref_txid: string;
  ref_business_id: string;
  note: string;
  payload: unknown;
};

export type BitfsPublicWalletAssetEventList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsPublicWalletAssetEventItem[];
};

export type BitfsPublicWalletAssetPreviewChange = {
  owner_scope: string;
  asset_group: string;
  asset_standard: string;
  asset_key: string;
  asset_symbol: string;
  quantity_text: string;
  direction: string;
  note: string;
};

export type BitfsPublicWalletAssetPreview = {
  action: string;
  feasible: boolean;
  can_sign: boolean;
  summary: string;
  detail_lines: string[];
  warning_level: string;
  estimated_network_fee_bsv_sat: number;
  fee_funding_target_bsv_sat: number;
  selected_asset_utxo_ids: string[];
  selected_fee_utxo_ids: string[];
  txid: string;
  preview_hash: string;
  changes: BitfsPublicWalletAssetPreviewChange[];
};

export type BitfsPublicWalletAssetPreviewResponse = {
  ok: boolean;
  code: string;
  message: string;
  preview: BitfsPublicWalletAssetPreview;
};

export type BitfsPublicWalletAssetSignResponse = {
  ok: boolean;
  code: string;
  message: string;
  preview: BitfsPublicWalletAssetPreview;
  signed_tx_hex: string;
  txid: string;
};

export type BitfsPublicWalletAssetSubmitResponse = {
  ok: boolean;
  code: string;
  message: string;
  txid: string;
  token_id: string;
  status: string;
};

export type BitfsPublicWalletOrdinalItem = {
  utxo_id: string;
  wallet_address: string;
  txid: string;
  vout: number;
  value_satoshi: number;
  allocation_class: string;
  allocation_reason: string;
  asset_standard: "ordinal";
  asset_key: string;
  asset_symbol: string;
  source_name: string;
  updated_at_unix: number;
  payload: unknown;
};

export type BitfsPublicWalletOrdinalList = {
  wallet_address: string;
  total: number;
  limit: number;
  offset: number;
  items: BitfsPublicWalletOrdinalItem[];
};

export type ShellErrorSource = "main-process" | "shell-renderer" | "viewer" | "settings";

export type ShellErrorReport = {
  source: ShellErrorSource;
  title: string;
  message: string;
  detail: string;
  page_url: string;
  occurred_at_unix: number;
  can_stop_current_page: boolean;
};

export type ElectronE2EEventSource =
  | "main-process"
  | "shell-renderer"
  | "viewer-preload"
  | "viewer-page";

export type ElectronE2EEvent = {
  seq: number;
  name: string;
  source: ElectronE2EEventSource;
  occurred_at_unix: number;
  fields: Record<string, unknown>;
};

export type ElectronE2EObserverState = {
  enabled: boolean;
  last_seq: number;
  report_count: number;
  last_report_source: ShellErrorSource | "";
  last_report_title: string;
  last_report_message: string;
  last_report_page_url: string;
  modal_visible: boolean;
  stop_current_page_visible: boolean;
  viewer_frozen: boolean;
  viewer_frozen_url: string;
  viewer_frozen_reason: string;
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
