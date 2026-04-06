import type { ShellState } from "./bitfs";
import type {
  Arbiter,
  ArbitersResp,
  Gateway,
  GatewaysResp,
  StaticTreeResp,
  WalletFundFlowItem,
  WalletFundFlowListResp,
  WalletSummary,
  Workspace,
  WorkspacesResp
} from "./types";

type RequestMethod = "GET" | "POST" | "PUT" | "DELETE";
type AdminConfigItem = {
  key: string;
  value: unknown;
};

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

export function getWalletFundFlows(query?: {
  limit?: number;
  offset?: number;
  flowID?: string;
  flowType?: string;
  refID?: string;
  stage?: string;
  direction?: string;
  purpose?: string;
  relatedTxID?: string;
  q?: string;
}): Promise<WalletFundFlowListResp> {
  const search = new URLSearchParams();
  if (query?.limit && query.limit > 0) {
    search.set("limit", String(query.limit));
  }
  if (query?.offset && query.offset >= 0) {
    search.set("offset", String(query.offset));
  }
  if (query?.flowID) {
    search.set("flow_id", query.flowID);
  }
  if (query?.flowType) {
    search.set("flow_type", query.flowType);
  }
  if (query?.refID) {
    search.set("ref_id", query.refID);
  }
  if (query?.stage) {
    search.set("stage", query.stage);
  }
  if (query?.direction) {
    search.set("direction", query.direction);
  }
  if (query?.purpose) {
    search.set("purpose", query.purpose);
  }
  if (query?.relatedTxID) {
    search.set("related_txid", query.relatedTxID);
  }
  if (query?.q) {
    search.set("q", query.q);
  }
  const suffix = search.toString();
  return requestJSON<WalletFundFlowListResp>("GET", `/api/v1/wallet/fund-flows${suffix ? `?${suffix}` : ""}`);
}

export function getWalletFundFlowDetail(id: number, flowType?: string): Promise<WalletFundFlowItem> {
  let url = `/api/v1/wallet/fund-flows/detail?id=${id}`;
  if (flowType) {
    url += `&flow_type=${encodeURIComponent(flowType)}`;
  }
  return requestJSON<WalletFundFlowItem>("GET", url);
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

export function getAdminConfig(): Promise<Record<string, unknown>> {
  return requestJSON<{ config: Record<string, unknown> }>("GET", "/api/v1/admin/config").then((resp) => resp.config || {});
}

export function setAdminConfigItems(items: AdminConfigItem[]): Promise<Record<string, unknown>> {
  return requestJSON<{ config: Record<string, unknown> }>("POST", "/api/v1/admin/config", {
    items
  }).then((resp) => resp.config || {});
}

export function addWorkspace(path: string, maxBytes: number): Promise<unknown> {
  return requestJSON("POST", "/api/v1/admin/workspaces", {
    path,
    max_bytes: maxBytes
  });
}

export function updateWorkspace(workspacePath: string, data: Partial<Workspace>): Promise<unknown> {
  return requestJSON("PUT", `/api/v1/admin/workspaces?workspace_path=${encodeURIComponent(workspacePath)}`, data);
}

export function deleteWorkspace(workspacePath: string): Promise<unknown> {
  return requestJSON("DELETE", `/api/v1/admin/workspaces?workspace_path=${encodeURIComponent(workspacePath)}`);
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
