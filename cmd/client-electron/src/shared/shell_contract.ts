export type ShellResourceMode = "local" | "auto" | "approved" | "pending" | "blocked";

export type ShellSidebarPanel = "resources" | "wallet";

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

export type ManagedClientPhase = "starting" | "locked" | "ready" | "error" | "stopped";

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
  recentLogs: string[];
};

export type KeyFileActionResult = {
  cancelled: boolean;
  filePath: string;
  state: ShellState;
};

export type ShellState = {
  currentURL: string;
  currentRootSeedHash: string;
  staticSingleMaxSat: number;
  staticPageMaxSat: number;
  autoSpentSat: number;
  pendingCount: number;
  resources: ShellResource[];
  lastError: string;
  clientAPIBase: string;
  viewerPreloadPath: string;
  userHomeSeedHash: string;
  sidebarWidthPx: number;
  activePanel: ShellSidebarPanel;
  backend: ManagedClientState;
};
