export function shortHex(raw: string, head = 6, tail = 6): string {
  const value = String(raw || "").trim();
  if (value.length <= head + tail + 3) {
    return value || "-";
  }
  return `${value.slice(0, head)}...${value.slice(-tail)}`;
}

export function formatBytes(value: number): string {
  const size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) {
    return "-";
  }
  if (size < 1024) {
    return `${size} B`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  if (size < 1024 * 1024 * 1024) {
    return `${(size / (1024 * 1024)).toFixed(2)} MB`;
  }
  return `${(size / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function formatSat(value: number): string {
  const sat = Math.max(0, Math.floor(Number(value || 0)));
  return `${sat} sat`;
}

export function formatTime(value: number): string {
  const unix = Number(value || 0);
  if (!Number.isFinite(unix) || unix <= 0) {
    return "-";
  }
  return new Date(unix * 1000).toLocaleString("zh-CN", { hour12: false });
}

export function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  return /^[0-9a-f]{64}$/.test(value) ? value : "";
}
