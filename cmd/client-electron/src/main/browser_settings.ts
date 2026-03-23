import { EventEmitter } from "node:events";
import fs from "node:fs";
import path from "node:path";

import type { ShellSidebarPanel } from "../shared/shell_contract";

const HASH_PATTERN = /^[0-9a-f]{64}$/;
const DEFAULT_SIDEBAR_WIDTH_PX = 388;
const MIN_SIDEBAR_WIDTH_PX = 280;
const MAX_SIDEBAR_WIDTH_PX = 560;

type BrowserSettingsFile = {
  user_home_seed_hash?: string;
  sidebar_width_px?: number;
  active_panel?: string;
};

export type BrowserSettingsSnapshot = {
  userHomeSeedHash: string;
  sidebarWidthPx: number;
  activePanel: ShellSidebarPanel;
  settingsPath: string;
};

export class BrowserSettingsStore extends EventEmitter {
  private readonly settingsPath: string;
  private userHomeSeedHash = "";
  private sidebarWidthPx = DEFAULT_SIDEBAR_WIDTH_PX;
  private activePanel: ShellSidebarPanel = "resources";

  constructor(userDataDir: string) {
    super();
    this.settingsPath = path.join(userDataDir, "browser-settings.json");
    this.load();
  }

  snapshot(): BrowserSettingsSnapshot {
    return {
      userHomeSeedHash: this.userHomeSeedHash,
      sidebarWidthPx: this.sidebarWidthPx,
      activePanel: this.activePanel,
      settingsPath: this.settingsPath
    };
  }

  setUserHomeSeedHash(seedHash: string): BrowserSettingsSnapshot {
    this.userHomeSeedHash = normalizeSeedHash(seedHash);
    this.save();
    this.emit("change", this.snapshot());
    return this.snapshot();
  }

  clearUserHomeSeedHash(): BrowserSettingsSnapshot {
    this.userHomeSeedHash = "";
    this.save();
    this.emit("change", this.snapshot());
    return this.snapshot();
  }

  setSidebarLayout(payload: { sidebarWidthPx?: number; activePanel?: string }): BrowserSettingsSnapshot {
    if (payload.sidebarWidthPx !== undefined) {
      this.sidebarWidthPx = normalizeSidebarWidth(payload.sidebarWidthPx);
    }
    if (payload.activePanel !== undefined) {
      this.activePanel = normalizeActivePanel(payload.activePanel);
    }
    this.save();
    this.emit("change", this.snapshot());
    return this.snapshot();
  }

  private load(): void {
    if (!fs.existsSync(this.settingsPath)) {
      return;
    }
    const raw = JSON.parse(fs.readFileSync(this.settingsPath, "utf8")) as BrowserSettingsFile;
    this.userHomeSeedHash = normalizeSeedHash(raw.user_home_seed_hash);
    this.sidebarWidthPx = normalizeSidebarWidth(raw.sidebar_width_px);
    this.activePanel = normalizeActivePanel(raw.active_panel);
  }

  private save(): void {
    fs.mkdirSync(path.dirname(this.settingsPath), { recursive: true });
    const data: BrowserSettingsFile = {
      sidebar_width_px: this.sidebarWidthPx,
      active_panel: this.activePanel
    };
    if (this.userHomeSeedHash !== "") {
      data.user_home_seed_hash = this.userHomeSeedHash;
    }
    fs.writeFileSync(this.settingsPath, JSON.stringify(data, null, 2) + "\n", "utf8");
  }
}

function normalizeSeedHash(raw: unknown): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!HASH_PATTERN.test(value)) {
    return "";
  }
  return value;
}

function normalizeSidebarWidth(raw: unknown): number {
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return DEFAULT_SIDEBAR_WIDTH_PX;
  }
  return Math.min(MAX_SIDEBAR_WIDTH_PX, Math.max(MIN_SIDEBAR_WIDTH_PX, Math.floor(value)));
}

function normalizeActivePanel(raw: unknown): ShellSidebarPanel {
  return String(raw || "").trim() === "wallet" ? "wallet" : "resources";
}
