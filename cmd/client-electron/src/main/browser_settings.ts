import { EventEmitter } from "node:events";
import fs from "node:fs";
import path from "node:path";

const HASH_PATTERN = /^[0-9a-f]{64}$/;

type BrowserSettingsFile = {
  user_home_seed_hash?: string;
};

export type BrowserSettingsSnapshot = {
  userHomeSeedHash: string;
  settingsPath: string;
};

export class BrowserSettingsStore extends EventEmitter {
  private readonly settingsPath: string;
  private userHomeSeedHash = "";

  constructor(userDataDir: string) {
    super();
    this.settingsPath = path.join(userDataDir, "browser-settings.json");
    this.load();
  }

  snapshot(): BrowserSettingsSnapshot {
    return {
      userHomeSeedHash: this.userHomeSeedHash,
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

  private load(): void {
    if (!fs.existsSync(this.settingsPath)) {
      return;
    }
    const raw = JSON.parse(fs.readFileSync(this.settingsPath, "utf8")) as BrowserSettingsFile;
    this.userHomeSeedHash = normalizeSeedHash(raw.user_home_seed_hash);
  }

  private save(): void {
    fs.mkdirSync(path.dirname(this.settingsPath), { recursive: true });
    const data: BrowserSettingsFile = {};
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
