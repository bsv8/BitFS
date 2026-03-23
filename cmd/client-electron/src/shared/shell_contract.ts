export type PendingResource = {
  seedHash: string;
  estimatedTotalSat: number;
  fileSize: number;
  chunkCount: number;
  mimeHint: string;
  recommendedFileName: string;
  reason: string;
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
  staticSingleMaxSat: number;
  staticPageMaxSat: number;
  autoSpentSat: number;
  pending: PendingResource[];
  lastError: string;
  clientAPIBase: string;
  viewerPreloadPath: string;
  userHomeSeedHash: string;
  backend: ManagedClientState;
};
