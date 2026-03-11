/**
 * 工具函数集合
 * 
 * 格式化、字符串处理、URL 操作等通用函数
 */

/**
 * 格式化 satoshi 金额为可读字符串
 */
export function sat(v: number): string {
  return `${Number(v || 0).toLocaleString()} sat`;
}

/**
 * 将 Unix 时间戳格式化为本地时间字符串
 */
export function t(ts: number): string {
  return ts ? new Date(ts * 1000).toLocaleString() : "-";
}

/**
 * 截断长字符串，显示为 前n...后n 格式
 */
export function short(s: string, n = 8): string {
  if (!s) return "-";
  if (s.length <= n * 2) return s;
  return `${s.slice(0, n)}...${s.slice(-n)}`;
}

/**
 * 截断 hex 字符串，显示为 前n...后n 格式
 */
export function shortHex(s: string, n = 4): string {
  if (!s) return "-";
  if (s.length <= n * 2) return s;
  return `${s.slice(0, n)}...${s.slice(-n)}`;
}

/**
 * 将字节数格式化为人类可读字符串 (B, KB, MB, GB, TB)
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

/**
 * 路由相关类型
 */
export type HashRoute = {
  path: string;
  query: URLSearchParams;
};

/**
 * 获取当前 hash 路由
 */
export function nowPath(): HashRoute {
  const raw = window.location.hash.replace(/^#/, "");
  const value = raw || "/wallet";
  const [pathPart, queryPart] = value.split("?", 2);
  const path = pathPart || "/wallet";
  const query = new URLSearchParams(queryPart || "");
  return { path, query };
}

/**
 * 设置 hash 路由
 */
export function setHash(path: string, query?: URLSearchParams) {
  const q = query && query.toString() ? `?${query.toString()}` : "";
  window.location.hash = `${path}${q}`;
}

/**
 * 将字符串转换为整数，带范围和默认值
 */
export function toInt(v: string | null, d: number, min = 1, max = 500): number {
  const x = Number(v || d);
  if (!Number.isFinite(x)) return d;
  return Math.max(min, Math.min(max, Math.floor(x)));
}
