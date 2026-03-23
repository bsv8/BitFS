import fs from "node:fs";
import path from "node:path";

type LogFields = Record<string, unknown> | undefined;

type DebugLoggerInit = {
  userDataDir: string;
};

class DebugLogger {
  private enabled = false;
  private logFilePath = "";

  init(init: DebugLoggerInit): void {
    this.enabled = parseDebugFlag(process.env.BITFS_ELECTRON_DEBUG);
    if (!this.enabled) {
      return;
    }
    const logDir = path.join(path.resolve(init.userDataDir), "logs");
    fs.mkdirSync(logDir, { recursive: true });
    this.logFilePath = path.join(logDir, "electron-debug.log");
    this.log("main", "logger_initialized", {
      log_file_path: this.logFilePath,
      pid: process.pid
    });
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  getLogFilePath(): string {
    return this.logFilePath;
  }

  log(scope: string, event: string, fields?: LogFields): void {
    if (!this.enabled) {
      return;
    }
    const line = formatLogLine(scope, event, fields);
    process.stdout.write(line + "\n");
    if (this.logFilePath !== "") {
      try {
        fs.appendFileSync(this.logFilePath, line + "\n");
      } catch {
        // 设计说明：
        // - 调试日志只能帮助定位问题，不能反过来因为写日志失败把主流程打崩；
        // - stdout 仍然保留，所以即使文件落盘失败，我们也还有终端日志可用。
      }
    }
  }
}

export const debugLogger = new DebugLogger();

function parseDebugFlag(raw: string | undefined): boolean {
  const value = String(raw || "").trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes" || value === "on";
}

function formatLogLine(scope: string, event: string, fields?: LogFields): string {
  const prefix = `[bitfs-electron][${sanitizeToken(scope)}] ${new Date().toISOString()} ${sanitizeToken(event)}`;
  const suffix = serializeFields(fields);
  return suffix === "" ? prefix : `${prefix} ${suffix}`;
}

function serializeFields(fields?: LogFields): string {
  if (!fields) {
    return "";
  }
  const parts: string[] = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value === undefined) {
      continue;
    }
    parts.push(`${sanitizeToken(key)}=${sanitizeValue(value)}`);
  }
  return parts.join(" ");
}

function sanitizeToken(value: string): string {
  return String(value || "").trim().replace(/\s+/g, "_");
}

function sanitizeValue(value: unknown): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return '""';
    }
    return JSON.stringify(trimmed);
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  if (value instanceof Error) {
    return JSON.stringify(value.message || String(value));
  }
  try {
    return JSON.stringify(value);
  } catch {
    return JSON.stringify(String(value));
  }
}
