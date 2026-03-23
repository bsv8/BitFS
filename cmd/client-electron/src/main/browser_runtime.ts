import { EventEmitter } from "node:events";

import type { PendingResource } from "../shared/shell_contract";
import { debugLogger } from "./debug_logger";

export type BrowserRuntimeState = {
  currentURL: string;
  staticSingleMaxSat: number;
  staticPageMaxSat: number;
  autoSpentSat: number;
  pending: PendingResource[];
  lastError: string;
  clientAPIBase: string;
  viewerPreloadPath: string;
};

type PlanItem = {
  seed_hash: string;
  status: string;
  reason?: string;
  local_ready: boolean;
  recommended_file_name?: string;
  mime_hint?: string;
  file_size?: number;
  chunk_count?: number;
  seed_price?: number;
  chunk_price_sat_per_64k?: number;
  estimated_total_sat?: number;
};

export type FileStatus = PlanItem & {
  output_file_path?: string;
  content_uri?: string;
};

type ResourceDecision = {
  mode: "local" | "auto" | "approved" | "pending" | "blocked";
  estimatedTotalSat: number;
  plan: PlanItem;
};

export type EnsureSeedResult = FileStatus;

export class BitfsBrowserRuntime extends EventEmitter {
  private readonly clientAPIBase: string;
  private readonly viewerPreloadPath: string;
  private currentURL = "";
  private staticSingleMaxSat = 256;
  private staticPageMaxSat = 2048;
  private autoSpentSat = 0;
  private pending = new Map<string, PendingResource>();
  private decisions = new Map<string, ResourceDecision>();
  private approved = new Set<string>();
  private lastError = "";
  private currentTraceID = "";

  constructor(clientAPIBase: string, viewerPreloadPath: string) {
    super();
    this.clientAPIBase = clientAPIBase.replace(/\/+$/, "");
    this.viewerPreloadPath = viewerPreloadPath;
  }

  snapshot(): BrowserRuntimeState {
    return {
      currentURL: this.currentURL,
      staticSingleMaxSat: this.staticSingleMaxSat,
      staticPageMaxSat: this.staticPageMaxSat,
      autoSpentSat: this.autoSpentSat,
      pending: Array.from(this.pending.values()).sort((a, b) => b.estimatedTotalSat - a.estimatedTotalSat),
      lastError: this.lastError,
      clientAPIBase: this.clientAPIBase,
      viewerPreloadPath: this.viewerPreloadPath
    };
  }

  openRoot(seedHash: string): string {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    this.currentTraceID = createTraceID();
    this.currentURL = `bitfs://${normalized}`;
    this.autoSpentSat = 0;
    this.pending.clear();
    this.decisions.clear();
    this.approved.clear();
    this.lastError = "";
    debugLogger.log("runtime", "open_root", {
      seed_hash: normalized,
      url: this.currentURL,
      trace_id: this.currentTraceID
    });
    this.emitState();
    return this.currentURL;
  }

  noteNavigation(url: string): void {
    const trimmed = String(url || "").trim();
    if (trimmed === "") {
      return;
    }
    this.currentURL = trimmed;
    debugLogger.log("runtime", "note_navigation", {
      url: trimmed,
      trace_id: this.currentTraceID
    });
    this.emitState();
  }

  setStaticBudget(singleMaxSat: number, pageMaxSat: number): void {
    this.staticSingleMaxSat = normalizeBudget(singleMaxSat, 256);
    this.staticPageMaxSat = normalizeBudget(pageMaxSat, 2048);
    debugLogger.log("runtime", "set_static_budget", {
      single_max_sat: this.staticSingleMaxSat,
      page_max_sat: this.staticPageMaxSat,
      trace_id: this.currentTraceID
    });
    this.emitState();
  }

  approvePendingResources(): void {
    const approvedSeeds = Array.from(this.pending.keys());
    for (const [seedHash] of this.pending) {
      this.approved.add(seedHash);
      const decision = this.decisions.get(seedHash);
      if (decision) {
        decision.mode = "approved";
      }
    }
    this.pending.clear();
    debugLogger.log("runtime", "approve_pending_resources", {
      approved_count: approvedSeeds.length,
      approved_seed_hashes: approvedSeeds,
      trace_id: this.currentTraceID
    });
    this.emitState();
  }

  async resolveSeed(seedHash: string): Promise<{ allowed: boolean; seedHash: string; maxTotalSat: number; plan?: PlanItem }> {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    const existing = this.decisions.get(normalized);
    if (existing) {
      debugLogger.log("runtime", "resolve_seed_cached", {
        seed_hash: normalized,
        mode: existing.mode,
        estimated_total_sat: existing.estimatedTotalSat,
        trace_id: this.currentTraceID
      });
      return {
        allowed: existing.mode !== "pending" && existing.mode !== "blocked",
        seedHash: normalized,
        maxTotalSat: existing.estimatedTotalSat,
        plan: existing.plan
      };
    }
    const plans = await this.planSeeds([normalized]);
    const plan = plans[0];
    const decision = this.decisions.get(normalized);
    debugLogger.log("runtime", "resolve_seed_planned", {
      seed_hash: normalized,
      allowed: decision?.mode !== "pending" && decision?.mode !== "blocked",
      mode: decision?.mode || "",
      estimated_total_sat: decision?.estimatedTotalSat || Number(plan?.estimated_total_sat || 0),
      trace_id: this.currentTraceID
    });
    return {
      allowed: decision?.mode !== "pending" && decision?.mode !== "blocked",
      seedHash: normalized,
      maxTotalSat: decision?.estimatedTotalSat || Number(plan?.estimated_total_sat || 0),
      plan
    };
  }

  async fetchContent(seedHash: string, maxTotalSat: number): Promise<Response> {
    const url = new URL(`${this.clientAPIBase}/api/v1/files/get-file/content`);
    url.searchParams.set("seed_hash", seedHash);
    if (maxTotalSat > 0) {
      url.searchParams.set("max_total_price_sat", String(maxTotalSat));
    }
    debugLogger.log("runtime.http", "fetch_content_request", {
      url: url.toString(),
      seed_hash: seedHash,
      max_total_sat: maxTotalSat,
      trace_id: this.currentTraceID
    });
    const response = await fetch(url, { method: "GET" });
    if (!response.ok) {
      const text = await response.text();
      this.lastError = text || `content fetch failed: ${response.status}`;
      debugLogger.log("runtime.http", "fetch_content_error", {
        url: url.toString(),
        seed_hash: seedHash,
        status: response.status,
        body: text,
        trace_id: this.currentTraceID
      });
      this.emitState();
    } else {
      this.lastError = "";
      debugLogger.log("runtime.http", "fetch_content_ok", {
        url: url.toString(),
        seed_hash: seedHash,
        status: response.status,
        content_type: String(response.headers.get("content-type") || ""),
        trace_id: this.currentTraceID
      });
    }
    return response;
  }

  async getFileStatus(seedHash: string): Promise<FileStatus> {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    const status = await this.fetchJSON<FileStatus>(`/api/v1/files/get-file/status?seed_hash=${normalized}`);
    debugLogger.log("runtime", "file_status", {
      seed_hash: normalized,
      status: status.status,
      local_ready: status.local_ready,
      trace_id: this.currentTraceID
    });
    if (status.local_ready || status.status === "local") {
      this.pending.delete(normalized);
      this.decisions.set(normalized, { mode: "local", estimatedTotalSat: 0, plan: status });
      this.emitState();
    }
    return status;
  }

  async planSeeds(seedHashes: string[]): Promise<PlanItem[]> {
    const ordered = normalizeSeedHashList(seedHashes);
    if (ordered.length === 0) {
      return [];
    }
    debugLogger.log("runtime", "plan_seeds_start", {
      seed_hashes: ordered,
      trace_id: this.currentTraceID
    });
    const fetched = new Map<string, PlanItem>();
    const missing: string[] = [];
    for (const seedHash of ordered) {
      const existing = this.decisions.get(seedHash);
      if (existing) {
        fetched.set(seedHash, existing.plan);
        continue;
      }
      missing.push(seedHash);
    }
    if (missing.length > 0) {
      debugLogger.log("runtime", "plan_seeds_fetch_missing", {
        seed_hashes: missing,
        trace_id: this.currentTraceID
      });
      const plans = await this.fetchPlans(missing);
      for (const plan of plans) {
        const seedHash = normalizeSeedHash(plan.seed_hash);
        if (seedHash === "") {
          continue;
        }
        this.applyPlanDecision(seedHash, plan);
        fetched.set(seedHash, plan);
      }
    }
    debugLogger.log("runtime", "plan_seeds_complete", {
      requested_count: ordered.length,
      fetched_count: fetched.size,
      trace_id: this.currentTraceID
    });
    return ordered
      .map((seedHash) => fetched.get(seedHash))
      .filter((plan): plan is PlanItem => Boolean(plan));
  }

  async planRefs(rawRefs: string[], baseURL = ""): Promise<PlanItem[]> {
    const seedHashes = rawRefs.map((ref) => this.resolveRef(ref, baseURL));
    debugLogger.log("runtime", "plan_refs", {
      ref_count: rawRefs.length,
      base_url: baseURL,
      resolved_seed_hashes: seedHashes,
      trace_id: this.currentTraceID
    });
    return this.planSeeds(seedHashes);
  }

  resolveRef(raw: string, baseURL = ""): string {
    const seedHash = resolveBitfsTarget(raw, baseURL);
    debugLogger.log("runtime", "resolve_ref", {
      raw,
      base_url: baseURL,
      seed_hash: seedHash,
      trace_id: this.currentTraceID
    });
    return seedHash;
  }

  async ensureSeed(seedHash: string, maxTotalSat = 0): Promise<EnsureSeedResult> {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    debugLogger.log("runtime.http", "ensure_seed_request", {
      seed_hash: normalized,
      max_total_sat: maxTotalSat,
      trace_id: this.currentTraceID
    });
    const payload: Record<string, unknown> = { seed_hash: normalized };
    if (maxTotalSat > 0) {
      payload.max_total_price_sat = maxTotalSat;
    }
    const result = await this.fetchJSON<EnsureSeedResult>("/api/v1/files/get-file/ensure", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    this.pending.delete(normalized);
    this.approved.add(normalized);
    this.decisions.set(normalized, { mode: "local", estimatedTotalSat: 0, plan: result });
    this.lastError = "";
    debugLogger.log("runtime.http", "ensure_seed_ok", {
      seed_hash: normalized,
      status: result.status,
      local_ready: result.local_ready,
      trace_id: this.currentTraceID
    });
    this.emitState();
    return result;
  }

  async preplanDocumentResources(rootSeedHash: string, html: string): Promise<void> {
    const refs = extractStaticResourceRefsFromHTML(html);
    const filtered = refs.filter((seedHash) => seedHash !== rootSeedHash);
    debugLogger.log("runtime", "preplan_document_resources", {
      root_seed_hash: rootSeedHash,
      ref_count: refs.length,
      filtered_ref_count: filtered.length,
      trace_id: this.currentTraceID
    });
    if (filtered.length === 0) {
      return;
    }
    await this.planSeeds(filtered);
  }

  async maybePreplanDocument(seedHash: string, response: Response): Promise<Response> {
    const contentType = String(response.headers.get("content-type") || "").toLowerCase();
    if (!contentType.includes("text/html")) {
      debugLogger.log("runtime", "preplan_skipped_non_html", {
        seed_hash: seedHash,
        content_type: contentType,
        trace_id: this.currentTraceID
      });
      return response;
    }
    const buffer = await response.arrayBuffer();
    const text = new TextDecoder().decode(buffer);
    try {
      await this.preplanDocumentResources(seedHash, text);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("runtime", "preplan_failed", {
        seed_hash: seedHash,
        message,
        trace_id: this.currentTraceID
      });
      this.setLastError(message);
    }
    debugLogger.log("runtime", "preplan_completed", {
      seed_hash: seedHash,
      content_type: contentType,
      trace_id: this.currentTraceID
    });
    return new Response(buffer, {
      status: response.status,
      statusText: response.statusText,
      headers: new Headers(response.headers)
    });
  }

  setLastError(message: string): void {
    this.lastError = String(message || "").trim();
    debugLogger.log("runtime", "set_last_error", {
      message: this.lastError,
      trace_id: this.currentTraceID
    });
    this.emitState();
  }

  async getClientInfo(): Promise<Record<string, unknown>> {
    debugLogger.log("runtime", "get_client_info", {
      trace_id: this.currentTraceID
    });
    return this.fetchJSON<Record<string, unknown>>("/api/v1/info");
  }

  async getWalletSummary(): Promise<Record<string, unknown>> {
    debugLogger.log("runtime", "get_wallet_summary", {
      trace_id: this.currentTraceID
    });
    return this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/summary");
  }

  async getLiveLatest(streamID: string): Promise<Record<string, unknown>> {
    const normalized = normalizeSeedHash(streamID);
    if (normalized === "") {
      throw new Error("invalid stream id");
    }
    debugLogger.log("runtime", "get_live_latest", {
      stream_id: normalized,
      trace_id: this.currentTraceID
    });
    return this.fetchJSON<Record<string, unknown>>(`/api/v1/live/latest?stream_id=${normalized}`);
  }

  async getLivePlan(streamID: string, haveSegmentIndex: number): Promise<Record<string, unknown>> {
    const normalized = normalizeSeedHash(streamID);
    if (normalized === "") {
      throw new Error("invalid stream id");
    }
    debugLogger.log("runtime", "get_live_plan", {
      stream_id: normalized,
      have_segment_index: Math.max(-1, Math.floor(Number(haveSegmentIndex || -1))),
      trace_id: this.currentTraceID
    });
    return this.fetchJSON<Record<string, unknown>>("/api/v1/live/plan", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        stream_id: normalized,
        have_segment_index: Math.max(-1, Math.floor(Number(haveSegmentIndex || -1)))
      })
    });
  }

  private async fetchPlans(seedHashes: string[]): Promise<PlanItem[]> {
    debugLogger.log("runtime.http", "fetch_plans_request", {
      seed_hashes: seedHashes,
      trace_id: this.currentTraceID
    });
    const response = await fetch(`${this.clientAPIBase}/api/v1/files/get-file/plan`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ seed_hashes: seedHashes, resource_kind: "static" })
    });
    if (!response.ok) {
      const text = await response.text();
      debugLogger.log("runtime.http", "fetch_plans_error", {
        status: response.status,
        body: text,
        trace_id: this.currentTraceID
      });
      throw new Error(text || `plan request failed: ${response.status}`);
    }
    const body = await response.json() as { items?: PlanItem[] };
    if (!Array.isArray(body.items) || body.items.length === 0) {
      debugLogger.log("runtime.http", "fetch_plans_empty", {
        trace_id: this.currentTraceID
      });
      throw new Error("plan response missing items");
    }
    debugLogger.log("runtime.http", "fetch_plans_ok", {
      item_count: body.items.length,
      trace_id: this.currentTraceID
    });
    return body.items;
  }

  private applyPlanDecision(seedHash: string, plan: PlanItem): void {
    if (plan.local_ready || plan.status === "local") {
      debugLogger.log("runtime", "plan_decision_local", {
        seed_hash: seedHash,
        trace_id: this.currentTraceID
      });
      this.pending.delete(seedHash);
      this.decisions.set(seedHash, { mode: "local", estimatedTotalSat: 0, plan });
      this.emitState();
      return;
    }
    const estimatedTotal = Number(plan.estimated_total_sat || 0);
    if (this.approved.has(seedHash)) {
      debugLogger.log("runtime", "plan_decision_approved", {
        seed_hash: seedHash,
        estimated_total_sat: estimatedTotal,
        trace_id: this.currentTraceID
      });
      this.pending.delete(seedHash);
      this.decisions.set(seedHash, { mode: "approved", estimatedTotalSat: estimatedTotal, plan });
      this.emitState();
      return;
    }
    if (plan.status === "quoted" && estimatedTotal <= this.staticSingleMaxSat && this.autoSpentSat + estimatedTotal <= this.staticPageMaxSat) {
      this.autoSpentSat += estimatedTotal;
      debugLogger.log("runtime", "plan_decision_auto", {
        seed_hash: seedHash,
        estimated_total_sat: estimatedTotal,
        auto_spent_sat: this.autoSpentSat,
        trace_id: this.currentTraceID
      });
      this.pending.delete(seedHash);
      this.decisions.set(seedHash, { mode: "auto", estimatedTotalSat: estimatedTotal, plan });
      this.emitState();
      return;
    }
    if (plan.status === "quoted") {
      debugLogger.log("runtime", "plan_decision_pending", {
        seed_hash: seedHash,
        estimated_total_sat: estimatedTotal,
        reason: String(plan.reason || plan.status || "pending approval"),
        trace_id: this.currentTraceID
      });
      this.pending.set(seedHash, {
        seedHash,
        estimatedTotalSat: estimatedTotal,
        fileSize: Number(plan.file_size || 0),
        chunkCount: Number(plan.chunk_count || 0),
        mimeHint: String(plan.mime_hint || ""),
        recommendedFileName: String(plan.recommended_file_name || ""),
        reason: String(plan.reason || plan.status || "pending approval")
      });
      this.decisions.set(seedHash, { mode: "pending", estimatedTotalSat: estimatedTotal, plan });
      this.emitState();
      return;
    }
    debugLogger.log("runtime", "plan_decision_blocked", {
      seed_hash: seedHash,
      status: plan.status,
      reason: String(plan.reason || ""),
      estimated_total_sat: estimatedTotal,
      trace_id: this.currentTraceID
    });
    this.pending.delete(seedHash);
    this.decisions.set(seedHash, { mode: "blocked", estimatedTotalSat: estimatedTotal, plan });
    this.emitState();
  }

  private emitState(): void {
    this.emit("state", this.snapshot());
  }

  private async fetchJSON<T>(pathOrURL: string, init?: RequestInit): Promise<T> {
    const target = pathOrURL.startsWith("http://") || pathOrURL.startsWith("https://")
      ? pathOrURL
      : `${this.clientAPIBase}${pathOrURL}`;
    const method = String(init?.method || "GET");
    debugLogger.log("runtime.http", "request", {
      method,
      url: target,
      trace_id: this.currentTraceID
    });
    const response = await fetch(target, init);
    if (!response.ok) {
      const text = await response.text();
      this.lastError = text || `request failed: ${response.status}`;
      debugLogger.log("runtime.http", "response_error", {
        method,
        url: target,
        status: response.status,
        body: text,
        trace_id: this.currentTraceID
      });
      this.emitState();
      throw new Error(text || `request failed: ${response.status}`);
    }
    const body = await response.json() as T;
    this.lastError = "";
    debugLogger.log("runtime.http", "response_ok", {
      method,
      url: target,
      status: response.status,
      trace_id: this.currentTraceID
    });
    this.emitState();
    return body;
  }
}

function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(value)) {
    return "";
  }
  return value;
}

function normalizeBudget(raw: number, fallback: number): number {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(value));
}

function normalizeSeedHashList(seedHashes: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of seedHashes) {
    const seedHash = normalizeSeedHash(item);
    if (!seedHash || seen.has(seedHash)) {
      continue;
    }
    seen.add(seedHash);
    out.push(seedHash);
  }
  return out;
}

function resolveBitfsTarget(raw: string, baseURL = ""): string {
  const value = String(raw || "").trim();
  if (!value) {
    throw new Error("bitfs target is required");
  }
  if (value.startsWith("bitfs://")) {
    const parsed = parseBitfsURL(value);
    if (parsed) {
      return parsed;
    }
    throw new Error("invalid bitfs target");
  }
  if (value.startsWith("/")) {
    const fromPath = normalizeSeedHash(value.slice(1));
    if (fromPath) {
      return fromPath;
    }
    throw new Error("invalid bitfs target");
  }
  const direct = normalizeSeedHash(value);
  if (direct) {
    return direct;
  }
  if (baseURL) {
    try {
      const parsed = new URL(value, baseURL);
      const seedHash = parseBitfsURL(parsed.toString());
      if (seedHash) {
        return seedHash;
      }
    } catch {
      // ignore
    }
  }
  throw new Error("invalid bitfs target");
}

function parseBitfsURL(rawURL: string): string {
  let url: URL;
  try {
    url = new URL(rawURL);
  } catch {
    return "";
  }
  if (url.protocol !== "bitfs:") {
    return "";
  }
  const pathSeed = normalizeSeedHash(url.pathname.replace(/^\/+/, ""));
  if (pathSeed) {
    return pathSeed;
  }
  return normalizeSeedHash(url.hostname);
}

function extractStaticResourceRefsFromHTML(html: string): string[] {
  const refs = new Set<string>();
  const patterns = [
    /bitfs:\/\/([0-9a-f]{64})/gi,
    /(?:src|href|poster)\s*=\s*["']\/?([0-9a-f]{64})["']/gi,
    /url\(\s*["']?\/?([0-9a-f]{64})["']?\s*\)/gi
  ];
  for (const pattern of patterns) {
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(html)) !== null) {
      const seedHash = normalizeSeedHash(match[1] || "");
      if (seedHash) {
        refs.add(seedHash);
      }
    }
  }
  return Array.from(refs);
}

function createTraceID(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 10)}`;
}
