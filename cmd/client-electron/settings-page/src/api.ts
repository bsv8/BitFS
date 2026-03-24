import type { ShellState } from "./bitfs";
import type {
  Arbiter,
  ArbitersResp,
  Gateway,
  GatewaysResp,
  StaticTreeResp,
  WalletSummary,
  Workspace,
  WorkspacesResp
} from "./types";

type RequestMethod = "GET" | "POST" | "PUT" | "DELETE";

async function requestJSON<T>(method: RequestMethod, path: string, body?: unknown): Promise<T> {
  const payload = await window.bitfsSettings.request(method, path, body);
  return payload as T;
}

export function getShellState(): Promise<ShellState> {
  return window.bitfsSettings.getShellState();
}

export function getWalletSummary(): Promise<WalletSummary> {
  return window.bitfsSettings.getWalletSummary() as Promise<WalletSummary>;
}

export function setUserHomepage(seedHash: string): Promise<ShellState> {
  return window.bitfsSettings.setUserHomepage(seedHash);
}

export function clearUserHomepage(): Promise<ShellState> {
  return window.bitfsSettings.clearUserHomepage();
}

export function lockClient(): Promise<ShellState> {
  return window.bitfsSettings.lock();
}

export function restartBackend(): Promise<ShellState> {
  return window.bitfsSettings.restartBackend();
}

export function exportKeyFile() {
  return window.bitfsSettings.exportKeyFile();
}

export function getGateways(): Promise<GatewaysResp> {
  return requestJSON<GatewaysResp>("GET", "/api/v1/gateways");
}

export function saveGateway(id: number | null, data: Gateway): Promise<unknown> {
  if (id === null) {
    return requestJSON("POST", "/api/v1/gateways", data);
  }
  return requestJSON("PUT", `/api/v1/gateways?id=${id}`, data);
}

export function deleteGateway(id: number): Promise<unknown> {
  return requestJSON("DELETE", `/api/v1/gateways?id=${id}`);
}

export function getArbiters(): Promise<ArbitersResp> {
  return requestJSON<ArbitersResp>("GET", "/api/v1/arbiters");
}

export function saveArbiter(id: number | null, data: Arbiter): Promise<unknown> {
  if (id === null) {
    return requestJSON("POST", "/api/v1/arbiters", data);
  }
  return requestJSON("PUT", `/api/v1/arbiters?id=${id}`, data);
}

export function deleteArbiter(id: number): Promise<unknown> {
  return requestJSON("DELETE", `/api/v1/arbiters?id=${id}`);
}

export function getWorkspaces(): Promise<WorkspacesResp> {
  return requestJSON<WorkspacesResp>("GET", "/api/v1/admin/workspaces");
}

export function addWorkspace(path: string, maxBytes: number): Promise<unknown> {
  return requestJSON("POST", "/api/v1/admin/workspaces", {
    path,
    max_bytes: maxBytes
  });
}

export function updateWorkspace(id: number, data: Partial<Workspace>): Promise<unknown> {
  return requestJSON("PUT", `/api/v1/admin/workspaces?id=${id}`, data);
}

export function deleteWorkspace(id: number): Promise<unknown> {
  return requestJSON("DELETE", `/api/v1/admin/workspaces?id=${id}`);
}

export function pickWorkspaceDirectory() {
  return window.bitfsSettings.pickDirectory();
}

export function getStaticTree(path: string): Promise<StaticTreeResp> {
  return requestJSON<StaticTreeResp>("GET", `/api/v1/admin/static/tree?path=${encodeURIComponent(path)}`);
}

export function createStaticDir(path: string): Promise<unknown> {
  return requestJSON("POST", "/api/v1/admin/static/mkdir", { path });
}

export function deleteStaticEntry(path: string, recursive: boolean): Promise<unknown> {
  const query = recursive ? "&recursive=true" : "";
  return requestJSON("DELETE", `/api/v1/admin/static/entry?path=${encodeURIComponent(path)}${query}`);
}

export function setStaticItemPrice(path: string, floorPrice: number, discountBps: number): Promise<unknown> {
  return requestJSON("POST", "/api/v1/admin/static/price/set", {
    path,
    floor_price_sat_per_64k: floorPrice,
    resale_discount_bps: discountBps
  });
}

export function uploadStaticFile(targetDir: string, overwrite: boolean) {
  return window.bitfsSettings.uploadStaticFile(targetDir, overwrite);
}
