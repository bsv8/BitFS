import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";

import type {
  BitfsPublicClientInfo,
  BitfsPublicWalletAssetSignResponse,
  BitfsPublicWalletAssetEventItem,
  BitfsPublicWalletAssetEventList,
  BitfsPublicWalletAssetSubmitResponse,
  BitfsPublicWalletAssetPreviewResponse,
  BitfsPublicWalletOrdinalItem,
  BitfsPublicWalletOrdinalList,
  BitfsRuntimeEvent,
  BitfsPublicWalletAddress,
  BitfsPublicWalletHistoryDirection,
  BitfsPublicWalletHistoryItem,
  BitfsPublicWalletHistoryList,
  BitfsPublicWalletTokenBalanceItem,
  BitfsPublicWalletTokenBalanceList,
  BitfsPublicWalletTokenOutputItem,
  BitfsPublicWalletTokenOutputList,
  BitfsPublicWalletBalance,
  ShellResource,
  ShellVisitAccounting,
  ShellVisitAccountingBucket,
  ShellVisitStatus
} from "../shared/shell_contract";
import { debugLogger } from "./debug_logger";
import {
  buildBitfsViewerURL,
  canonicalBitfsLocator,
  type LocatorHandlerSet,
  type LocatorVisitContext,
  type ParsedNodeLocator,
  type ParsedResolverLocator,
  parseLocator
} from "./locator";
import {
  shellAccountingLabelForPurpose,
  shellPurposeUsedInVisitBucket,
  shellVisitBucketForPurpose
} from "./accounting_semantics";

export type BrowserRuntimeState = {
  currentURL: string;
  currentViewerURL: string;
  currentRootSeedHash: string;
  currentVisit: ShellVisitAccounting;
  staticSingleMaxSat: number;
  staticPageMaxSat: number;
  autoSpentSat: number;
  pendingCount: number;
  resources: ShellResource[];
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

type DownloadJobStatus = {
  job_id: string;
  seed_hash: string;
  demand_id?: string;
  state: string;
  chunk_count?: number;
  completed_chunks?: number;
  paid_total_sat?: number;
  output_file_path?: string;
  part_file_path?: string;
  error?: string;
  created_at_unix?: number;
  updated_at_unix?: number;
};

type GetFileByHashEnvelope<T> = {
  status: string;
  data: T;
};

type StatusDataEnvelopeError = {
  code?: string;
  message?: string;
};

type StatusDataEnvelope<T> = {
  status: string;
  data?: T;
  error?: StatusDataEnvelopeError | string | null;
};

type DownloadQuoteItem = {
  seller_pubkey_hex?: string;
  seed_price_sat?: number;
  chunk_price_sat?: number;
  chunk_count?: number;
  available_chunks?: number[];
  recommended_file_name?: string;
  mime_type?: string;
  file_size_bytes?: number;
  quote_timestamp?: number;
  expires_at_unix?: number;
  selected?: boolean;
  reject_reason?: string;
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

export type LocatorResolveResult = {
  kind: "bitfs" | "node" | "resolver";
  locator: string;
  route: string;
  seedHash: string;
  viewerURL: string;
  nodePubkeyHex: string;
  resolverPubkeyHex: string;
  name: string;
};

type WalletFundFlowItem = {
  id: number;
  created_at_unix?: number;
  visit_id?: string;
  visit_locator?: string;
  flow_id?: string;
  flow_type?: string;
  ref_id?: string;
  stage?: string;
  direction?: string;
  purpose?: string;
  amount_satoshi?: number;
  used_satoshi?: number;
  returned_satoshi?: number;
  related_txid?: string;
  note?: string;
};

type WalletFundFlowListResponse = {
  total?: number;
  limit?: number;
  offset?: number;
  items?: WalletFundFlowItem[];
};

export class BitfsBrowserRuntime extends EventEmitter {
  private readonly clientAPIBase: string;
  private readonly viewerPreloadPath: string;
  private locatorHandlers: LocatorHandlerSet;
  private currentURL = "";
  private currentViewerURL = "";
  private currentRootSeedHash = "";
  private staticSingleMaxSat = 256;
  private staticPageMaxSat = 2048;
  private autoSpentSat = 0;
  private pending = new Set<string>();
  private decisions = new Map<string, ResourceDecision>();
  private approved = new Set<string>();
  private discoveryOrder = new Map<string, number>();
  private nextDiscoveryOrder = 1;
  private lastError = "";
  private currentTraceID = "";
  private currentVisit = createEmptyVisitAccounting();
  private currentVisitBaselineFlowID = 0;
  private eventSeq = 0;
  private readonly runtimeEpoch = `shell-${Date.now().toString(36)}`;
  private readonly downloadJobs = new Map<string, string>();

  constructor(clientAPIBase: string, viewerPreloadPath: string, locatorHandlers: LocatorHandlerSet = {}) {
    super();
    this.clientAPIBase = clientAPIBase.replace(/\/+$/, "");
    this.viewerPreloadPath = viewerPreloadPath;
    this.locatorHandlers = locatorHandlers;
  }

  snapshot(): BrowserRuntimeState {
    const resources = Array.from(this.decisions.entries())
      .map(([seedHash, decision]) => this.toShellResource(seedHash, decision))
      .sort((a, b) => {
        if (a.estimatedTotalSat !== b.estimatedTotalSat) {
          return b.estimatedTotalSat - a.estimatedTotalSat;
        }
        if (a.isRoot !== b.isRoot) {
          return a.isRoot ? -1 : 1;
        }
        return a.discoveryOrder - b.discoveryOrder;
      });
    return {
      currentURL: this.currentURL,
      currentViewerURL: this.currentViewerURL,
      currentRootSeedHash: this.currentRootSeedHash,
      currentVisit: cloneVisitAccounting(this.currentVisit),
      staticSingleMaxSat: this.staticSingleMaxSat,
      staticPageMaxSat: this.staticPageMaxSat,
      autoSpentSat: this.autoSpentSat,
      pendingCount: resources.filter((item) => item.mode === "pending").length,
      resources,
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
    const visitID = this.beginVisit(canonicalBitfsLocator(normalized), "open_root");
    this.resetPageSession(canonicalBitfsLocator(normalized), buildBitfsViewerURL(normalized), normalized, "open_root");
    this.finishVisitSuccess(visitID, "bitfs root opened");
    void this.captureVisitBaseline(visitID).then(() => this.refreshVisitAccounting(visitID, "open_root"));
    debugLogger.log("runtime", "open_root", {
      seed_hash: normalized,
      locator: this.currentURL,
      viewer_url: this.currentViewerURL,
      trace_id: this.currentTraceID
    });
    return this.currentURL;
  }

  async open(rawLocator: string): Promise<string> {
    const locator = parseLocator(rawLocator);
    const visitID = this.beginVisit(locator.locator, `open_locator_${locator.kind}`);
    try {
      await this.captureVisitBaseline(visitID);
      switch (locator.kind) {
        case "bitfs":
          this.resetPageSession(locator.locator, locator.viewerURL, locator.seedHash, "open_locator_bitfs");
          this.finishVisitSuccess(visitID, "bitfs locator opened");
          debugLogger.log("runtime", "open_locator_bitfs", {
            locator: locator.locator,
            viewer_url: locator.viewerURL,
            seed_hash: locator.seedHash,
            trace_id: this.currentTraceID
          });
          await this.refreshVisitAccounting(visitID, "open_locator_bitfs");
          return this.currentURL;
        case "node":
          return await this.openNodeLocator(locator, visitID);
        case "resolver":
          return await this.openResolverLocator(locator, visitID);
      }
    } catch (error) {
      this.finishVisitFailure(visitID, error instanceof Error ? error.message : String(error));
      await this.refreshVisitAccounting(visitID, "open_failed");
      throw error;
    }
  }

  async resolveLocator(rawLocator: string): Promise<LocatorResolveResult> {
    const locator = parseLocator(rawLocator);
    switch (locator.kind) {
      case "bitfs":
        return {
          kind: "bitfs",
          locator: locator.locator,
          route: "",
          seedHash: locator.seedHash,
          viewerURL: locator.viewerURL,
          nodePubkeyHex: "",
          resolverPubkeyHex: "",
          name: ""
        };
      case "node": {
        const resolved = await this.resolveNodeLocatorSeed(locator);
        return {
          kind: "node",
          locator: locator.locator,
          route: locator.route,
          seedHash: resolved.seedHash,
          viewerURL: buildBitfsViewerURL(resolved.seedHash),
          nodePubkeyHex: locator.nodePubkeyHex,
          resolverPubkeyHex: "",
          name: ""
        };
      }
      case "resolver": {
        const resolved = await this.resolveResolverLocatorSeed(locator);
        return {
          kind: "resolver",
          locator: locator.locator,
          route: locator.route,
          seedHash: resolved.seedHash,
          viewerURL: buildBitfsViewerURL(resolved.seedHash),
          nodePubkeyHex: resolved.targetPubkeyHex,
          resolverPubkeyHex: locator.resolverPubkeyHex,
          name: locator.name
        };
      }
    }
  }

  async refreshVisitAccounting(visitID = this.currentVisit.visitID, reason = "manual"): Promise<void> {
    const activeVisitID = String(visitID || "").trim();
    if (activeVisitID === "" || this.currentVisit.visitID !== activeVisitID) {
      return;
    }
    const flows = await this.fetchVisitFundFlowsSilent(activeVisitID);
    if (this.currentVisit.visitID !== activeVisitID) {
      return;
    }
    const summary = summarizeVisitFundFlows(this.currentVisit, flows, this.currentVisitBaselineFlowID);
    this.currentVisit = {
      ...this.currentVisit,
      lastUpdatedAtUnix: Math.max(this.currentVisit.lastUpdatedAtUnix, Math.floor(Date.now() / 1000)),
      totalUsedSatoshi: summary.totalUsedSatoshi,
      totalReturnedSatoshi: summary.totalReturnedSatoshi,
      resolverUsedSatoshi: summary.resolverUsedSatoshi,
      reachabilityUsedSatoshi: summary.reachabilityUsedSatoshi,
      contentUsedSatoshi: summary.contentUsedSatoshi,
      otherUsedSatoshi: summary.otherUsedSatoshi,
      itemCount: summary.itemCount,
      buckets: summary.buckets
    };
    debugLogger.log("runtime", "visit_accounting_refreshed", {
      reason,
      visit_id: activeVisitID,
      item_count: this.currentVisit.itemCount,
      total_used_satoshi: this.currentVisit.totalUsedSatoshi,
      total_returned_satoshi: this.currentVisit.totalReturnedSatoshi
    });
    this.emitState();
  }

  getCurrentVisitContext(): LocatorVisitContext {
    return {
      visitID: String(this.currentVisit.visitID || "").trim(),
      visitLocator: String(this.currentVisit.locator || "").trim()
    };
  }

  getCurrentVisitHeaders(): Record<string, string> {
    const visit = this.getCurrentVisitContext();
    const headers: Record<string, string> = {};
    if (visit.visitID !== "") {
      headers["X-BitFS-Visit-ID"] = visit.visitID;
    }
    if (visit.visitLocator !== "") {
      headers["X-BitFS-Visit-Locator"] = visit.visitLocator;
    }
    return headers;
  }

  setLocatorHandlers(handlers: LocatorHandlerSet): void {
    this.locatorHandlers = handlers;
  }

  noteNavigation(url: string): void {
    const trimmed = String(url || "").trim();
    if (trimmed === "") {
      return;
    }
    const nextRootSeedHash = parseBitfsURL(trimmed);
    if (nextRootSeedHash && nextRootSeedHash !== this.currentRootSeedHash) {
      this.resetPageSession(canonicalBitfsLocator(nextRootSeedHash), trimmed, nextRootSeedHash, "document_navigated");
      debugLogger.log("runtime", "note_navigation_reset_page", {
        url: trimmed,
        seed_hash: nextRootSeedHash,
        trace_id: this.currentTraceID
      });
      return;
    }
    this.currentViewerURL = trimmed;
    debugLogger.log("runtime", "note_navigation", {
      url: trimmed,
      locator: this.currentURL,
      trace_id: this.currentTraceID
    });
    this.emitState();
    this.emitRuntimeEvent("shell.page.navigated", {
      url: trimmed,
      current_locator: this.currentURL,
      current_root_seed_hash: this.currentRootSeedHash
    });
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

  approveResource(seedHash: string): void {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    this.approveSeedInternal(normalized, true);
    debugLogger.log("runtime", "approve_resource", {
      seed_hash: normalized,
      trace_id: this.currentTraceID
    });
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
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    const plan = this.decisions.get(normalized)?.plan;
    debugLogger.log("runtime.http", "fetch_content_request", {
      seed_hash: normalized,
      max_total_sat: maxTotalSat,
      trace_id: this.currentTraceID
    });
    const { status, quote } = await this.startDownloadByHash(normalized, plan);
    const terminalStatus = await this.waitForDownloadTerminalStatus(status.job_id);
    if (terminalStatus.state !== "local" && terminalStatus.state !== "done") {
      const message = terminalStatus.error || `content fetch failed: ${terminalStatus.state}`;
      this.lastError = message;
      debugLogger.log("runtime.http", "fetch_content_error", {
        seed_hash: normalized,
        job_id: status.job_id,
        state: terminalStatus.state,
        error: message,
        trace_id: this.currentTraceID
      });
      this.emitState();
      throw new Error(message);
    }
    const filePath = String(terminalStatus.output_file_path || "").trim();
    if (filePath === "") {
      throw new Error("download completed without output file path");
    }
    const body = await readFile(filePath);
    const contentType = guessContentTypeForDownload(filePath, body, quote?.mime_type || plan?.mime_hint || "");
    this.lastError = "";
    debugLogger.log("runtime.http", "fetch_content_ok", {
      seed_hash: normalized,
      job_id: status.job_id,
      output_file_path: filePath,
      content_type: contentType,
      trace_id: this.currentTraceID
    });
    return new Response(body, {
      status: 200,
      headers: {
        "content-type": contentType
      }
    });
  }

  async getFileStatus(seedHash: string): Promise<FileStatus> {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    let status: FileStatus;
    const jobID = this.downloadJobs.get(normalized);
    if (jobID) {
      const job = await this.fetchDownloadStatus(jobID);
      const quotes = await this.fetchDownloadQuotes(jobID);
      status = this.fileStatusFromJob(normalized, job, quotes, this.decisions.get(normalized)?.plan);
    } else {
      const plan = this.decisions.get(normalized)?.plan;
      if (!plan) {
        const plans = await this.planSeeds([normalized]);
        status = plans[0] || {
          seed_hash: normalized,
          status: "quote_unavailable",
          local_ready: false
        };
      } else {
        status = {
          ...plan,
          output_file_path: plan.local_ready ? undefined : undefined,
          content_uri: `bitfs://${normalized}`
        };
      }
    }
    debugLogger.log("runtime", "file_status", {
      seed_hash: normalized,
      status: status.status,
      local_ready: status.local_ready,
      trace_id: this.currentTraceID
    });
    if (status.local_ready || status.status === "local") {
      this.ensureDiscoveryOrder(normalized);
      this.pending.delete(normalized);
      this.decisions.set(normalized, {
        mode: "local",
        estimatedTotalSat: Number(status.estimated_total_sat || 0),
        plan: status
      });
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
    const existing = this.decisions.get(normalized);
    debugLogger.log("runtime.http", "ensure_seed_request", {
      seed_hash: normalized,
      max_total_sat: maxTotalSat,
      trace_id: this.currentTraceID
    });
    const { status, quote } = await this.startDownloadByHash(normalized, existing?.plan);
    const result = this.fileStatusFromJob(normalized, status, quote ? [quote] : [], existing?.plan);
    this.ensureDiscoveryOrder(normalized);
    this.pending.delete(normalized);
    this.approved.add(normalized);
    this.decisions.set(normalized, {
      mode: "local",
      estimatedTotalSat: Number(result.estimated_total_sat || 0),
      plan: result
    });
    this.lastError = "";
    debugLogger.log("runtime.http", "ensure_seed_ok", {
      seed_hash: normalized,
      status: result.status,
      local_ready: result.local_ready,
      trace_id: this.currentTraceID
    });
    this.emitState();
    this.emitResourceEvent(normalized, existing, "local", result);
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

  private async startDownloadByHash(seedHash: string, plan?: PlanItem): Promise<{ status: DownloadJobStatus; quote: DownloadQuoteItem | null; quotes: DownloadQuoteItem[] }> {
    const normalized = normalizeSeedHash(seedHash);
    if (normalized === "") {
      throw new Error("invalid seed hash");
    }
    const payload: Record<string, unknown> = {
      seed_hash: normalized
    };
    const chunkCount = Number(plan?.chunk_count || 0);
    if (chunkCount > 0) {
      payload.chunk_count = chunkCount;
    }
    const seedPrice = Number(plan?.seed_price || 0);
    if (seedPrice > 0) {
      payload.max_seed_price_sat = seedPrice;
    }
    const chunkPrice = Number(plan?.chunk_price_sat_per_64k || 0);
    if (chunkPrice > 0) {
      payload.max_chunk_price_sat = chunkPrice;
    }
    const start = await this.fetchJSON<GetFileByHashEnvelope<{ job_id: string; status: DownloadJobStatus; message?: string }>>("/api/v1/files/getfilebyhash", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    const status = start.data?.status;
    const jobID = String(start.data?.job_id || status?.job_id || "").trim();
    if (jobID === "") {
      throw new Error("getfilebyhash job id is empty");
    }
    this.downloadJobs.set(normalized, jobID);
    const quotes = await this.fetchDownloadQuotes(jobID);
    return {
      status: status || await this.fetchDownloadStatus(jobID),
      quote: this.selectBestDownloadQuote(quotes) || null,
      quotes
    };
  }

  private async fetchDownloadStatus(jobID: string): Promise<DownloadJobStatus> {
    const response = await this.fetchJSON<GetFileByHashEnvelope<DownloadJobStatus>>(`/api/v1/files/getfilebyhash/status?job_id=${encodeURIComponent(jobID)}`);
    return response.data;
  }

  private async waitForDownloadTerminalStatus(jobID: string): Promise<DownloadJobStatus> {
    let last = await this.fetchDownloadStatus(jobID);
    for (let i = 0; i < 30; i += 1) {
      if (last.state === "local" || last.state === "done" || last.state === "failed" || last.state === "blocked_by_budget" || last.state === "quote_unavailable" || last.state === "quote_timeout") {
        return last;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 250));
      last = await this.fetchDownloadStatus(jobID);
    }
    return last;
  }

  private async fetchDownloadQuotes(jobID: string): Promise<DownloadQuoteItem[]> {
    const response = await this.fetchJSON<GetFileByHashEnvelope<DownloadQuoteItem[]>>(`/api/v1/files/getfilebyhash/quotes?job_id=${encodeURIComponent(jobID)}`);
    return Array.isArray(response.data) ? response.data : [];
  }

  private selectBestDownloadQuote(quotes: DownloadQuoteItem[]): DownloadQuoteItem | undefined {
    const filtered = quotes.filter((quote) => Number(quote.chunk_count || 0) > 0);
    if (filtered.length === 0) {
      return undefined;
    }
    filtered.sort((a, b) => {
      const totalA = Number(a.seed_price_sat || 0) + Number(a.chunk_price_sat || 0) * Number(a.chunk_count || 0);
      const totalB = Number(b.seed_price_sat || 0) + Number(b.chunk_price_sat || 0) * Number(b.chunk_count || 0);
      if (totalA !== totalB) {
        return totalA - totalB;
      }
      if (Number(a.chunk_price_sat || 0) !== Number(b.chunk_price_sat || 0)) {
        return Number(a.chunk_price_sat || 0) - Number(b.chunk_price_sat || 0);
      }
      return Number(a.seed_price_sat || 0) - Number(b.seed_price_sat || 0);
    });
    return filtered[0];
  }

  private planItemFromJob(seedHash: string, status: DownloadJobStatus, quotes: DownloadQuoteItem[], fallbackPlan?: PlanItem): PlanItem {
    const bestQuote = this.selectBestDownloadQuote(quotes);
    const localReady = status.state === "local" || status.state === "done" || String(status.output_file_path || "").trim() !== "";
    const fileSize = Number(bestQuote?.file_size_bytes || fallbackPlan?.file_size || 0);
    const chunkCount = Number(status.chunk_count || bestQuote?.chunk_count || fallbackPlan?.chunk_count || 0);
    const seedPrice = Number(bestQuote?.seed_price_sat || fallbackPlan?.seed_price || 0);
    const chunkPrice = Number(bestQuote?.chunk_price_sat || fallbackPlan?.chunk_price_sat_per_64k || 0);
    const estimated = Number(fallbackPlan?.estimated_total_sat || (seedPrice + chunkPrice * chunkCount));
    if (localReady) {
      return {
        seed_hash: seedHash,
        status: "local",
        local_ready: true,
        recommended_file_name: String(bestQuote?.recommended_file_name || fallbackPlan?.recommended_file_name || ""),
        mime_hint: String(bestQuote?.mime_type || fallbackPlan?.mime_hint || ""),
        file_size: fileSize,
        chunk_count: chunkCount,
        seed_price: seedPrice,
        chunk_price_sat_per_64k: chunkPrice,
        estimated_total_sat: estimated
      };
    }
    if (bestQuote) {
      return {
        seed_hash: seedHash,
        status: "quoted",
        local_ready: false,
        recommended_file_name: String(bestQuote.recommended_file_name || ""),
        mime_hint: String(bestQuote.mime_type || ""),
        file_size: fileSize,
        chunk_count: Number(bestQuote.chunk_count || 0),
        seed_price: Number(bestQuote.seed_price_sat || 0),
        chunk_price_sat_per_64k: Number(bestQuote.chunk_price_sat || 0),
        estimated_total_sat: estimated
      };
    }
    return {
      seed_hash: seedHash,
      status: status.state || "quote_unavailable",
      reason: String(status.error || ""),
      local_ready: false,
      recommended_file_name: String(fallbackPlan?.recommended_file_name || ""),
      mime_hint: String(fallbackPlan?.mime_hint || ""),
      file_size: fileSize,
      chunk_count: chunkCount,
      seed_price: seedPrice,
      chunk_price_sat_per_64k: chunkPrice,
      estimated_total_sat: estimated
    };
  }

  private fileStatusFromJob(seedHash: string, status: DownloadJobStatus, quotes: DownloadQuoteItem[], fallbackPlan?: PlanItem): FileStatus {
    const plan = this.planItemFromJob(seedHash, status, quotes, fallbackPlan);
    return {
      ...plan,
      output_file_path: String(status.output_file_path || "").trim() || undefined,
      content_uri: `bitfs://${seedHash}`
    };
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

  async getPublicClientInfo(): Promise<BitfsPublicClientInfo> {
    debugLogger.log("runtime", "get_public_client_info", {
      trace_id: this.currentTraceID
    });
    const info = await this.fetchJSON<Record<string, unknown>>("/api/v1/info");
    return {
      trusted_protocol: "bitfs://",
      pubkey_hex: normalizePubkeyHex(readStringField(info, "pubkey_hex", "client_pubkey_hex")),
      started_at_unix: readIntegerField(info, "started_at_unix"),
      seller_enabled: readBooleanField(info, "seller_enabled")
    };
  }

  async getWalletSummary(): Promise<Record<string, unknown>> {
    debugLogger.log("runtime", "get_wallet_summary", {
      trace_id: this.currentTraceID
    });
    return this.fetchStatusDataJSON<Record<string, unknown>>("/api/v1/wallet/summary");
  }

  async getPublicWalletBalance(): Promise<BitfsPublicWalletBalance> {
    debugLogger.log("runtime", "get_public_wallet_balance", {
      trace_id: this.currentTraceID
    });
    const summary = await this.getWalletSummary();
    // 设计说明：
    // - 公开余额接口只表达“当前最新公开余额”，不把“本轮同步是否刚跑完”这类内部判定暴露给页面；
    // - 因此优先读取本地钱包投影里的 plain BSV 余额，避免首页首次进入先看到 0。
    const balanceKey = Object.prototype.hasOwnProperty.call(summary, "wallet_plain_bsv_balance_satoshi")
      ? "wallet_plain_bsv_balance_satoshi"
      : "onchain_balance_satoshi";
    return {
      balance_satoshi: Math.max(0, readIntegerField(summary, balanceKey))
    };
  }

  async getPublicWalletAddresses(): Promise<BitfsPublicWalletAddress[]> {
    debugLogger.log("runtime", "get_public_wallet_addresses", {
      trace_id: this.currentTraceID
    });
    const [info, summary] = await Promise.all([
      this.fetchJSON<Record<string, unknown>>("/api/v1/info"),
      this.getWalletSummary()
    ]);
    const pubkeyHex = normalizePubkeyHex(readStringField(info, "pubkey_hex", "client_pubkey_hex"));
    const walletAddress = readStringField(summary, "wallet_address");
    if (walletAddress === "") {
      return [];
    }
    return [{
      address: walletAddress,
      encoding: "base58",
      purpose: "default_receive",
      pubkey_hex: pubkeyHex
    }];
  }

  async listPublicWalletHistory(query: { limit?: number; offset?: number; direction?: string }): Promise<BitfsPublicWalletHistoryList> {
    const limit = clampBoundInt(query?.limit, 12, 1, 100);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    const direction = normalizePublicWalletHistoryDirection(String(query?.direction || ""));
    debugLogger.log("runtime", "list_public_wallet_history", {
      limit,
      offset,
      direction,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    // 把前端 in/out 映射成后端 fund_flows 的多方向逗号分隔值
    const directions = historyDirectionToFundFlowDirections(direction);
    if (directions.length > 0) {
      search.set("direction", directions.join(","));
    }
    // 改读 fact_* 事实表组装的 fund-flows API，映射成现有 history 返回结构
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/fund-flows?${search.toString()}`);
    return {
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readHistoryItemsFromFundFlows(payload)
    };
  }

  async listPublicWalletTokenBalances(query: { limit?: number; offset?: number; standard?: string }): Promise<BitfsPublicWalletTokenBalanceList> {
    const limit = clampBoundInt(query?.limit, 50, 1, 500);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    const standard = normalizePublicWalletTokenStandard(String(query?.standard || ""));
    debugLogger.log("runtime", "list_public_wallet_token_balances", {
      limit,
      offset,
      standard,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    if (standard !== "") {
      search.set("standard", standard);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/tokens/balances?${search.toString()}`);
    return {
      wallet_address: readStringField(payload, "wallet_address"),
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readTokenBalanceItems(payload)
    };
  }

  async listPublicWalletTokenOutputs(query: { limit?: number; offset?: number; standard?: string; assetKey?: string; asset_key?: string }): Promise<BitfsPublicWalletTokenOutputList> {
    const limit = clampBoundInt(query?.limit, 50, 1, 500);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    const standard = normalizePublicWalletTokenStandard(String(query?.standard || ""));
    const assetKey = readNormalizedKey(String(query?.assetKey || ""), String(query?.asset_key || ""));
    debugLogger.log("runtime", "list_public_wallet_token_outputs", {
      limit,
      offset,
      standard,
      asset_key: assetKey,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    if (standard !== "") {
      search.set("standard", standard);
    }
    if (assetKey !== "") {
      search.set("asset_key", assetKey);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/tokens/outputs?${search.toString()}`);
    return {
      wallet_address: readStringField(payload, "wallet_address"),
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readTokenOutputItems(payload)
    };
  }

  async getPublicWalletTokenOutputDetail(query: { standard?: string; utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string }): Promise<BitfsPublicWalletTokenOutputItem> {
    const standard = normalizePublicWalletTokenStandard(String(query?.standard || ""));
    const utxoID = readNormalizedKey(String(query?.utxoID || ""), String(query?.utxo_id || "")).toLowerCase();
    const assetKey = readNormalizedKey(String(query?.assetKey || ""), String(query?.asset_key || ""));
    if (utxoID === "" && assetKey === "") {
      throw new Error("token output selector is required");
    }
    debugLogger.log("runtime", "get_public_wallet_token_output_detail", {
      standard,
      utxo_id: utxoID,
      asset_key: assetKey,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    if (standard !== "") {
      search.set("standard", standard);
    }
    if (utxoID !== "") {
      search.set("utxo_id", utxoID);
    }
    if (assetKey !== "") {
      search.set("asset_key", assetKey);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/tokens/outputs/detail?${search.toString()}`);
    return readTokenOutputItem(payload);
  }

  async listPublicWalletTokenEvents(query: { limit?: number; offset?: number; standard?: string; utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string }): Promise<BitfsPublicWalletAssetEventList> {
    const limit = clampBoundInt(query?.limit, 50, 1, 500);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    const standard = normalizePublicWalletTokenStandard(String(query?.standard || ""));
    const utxoID = readNormalizedKey(String(query?.utxoID || ""), String(query?.utxo_id || "")).toLowerCase();
    const assetKey = readNormalizedKey(String(query?.assetKey || ""), String(query?.asset_key || ""));
    debugLogger.log("runtime", "list_public_wallet_token_events", {
      limit,
      offset,
      standard,
      utxo_id: utxoID,
      asset_key: assetKey,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    if (standard !== "") {
      search.set("standard", standard);
    }
    if (utxoID !== "") {
      search.set("utxo_id", utxoID);
    }
    if (assetKey !== "") {
      search.set("asset_key", assetKey);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/tokens/events?${search.toString()}`);
    return {
      wallet_address: readStringField(payload, "wallet_address"),
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readAssetEventItems(payload)
    };
  }

  async listPublicWalletOrdinals(query: { limit?: number; offset?: number }): Promise<BitfsPublicWalletOrdinalList> {
    const limit = clampBoundInt(query?.limit, 50, 1, 500);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    debugLogger.log("runtime", "list_public_wallet_ordinals", {
      limit,
      offset,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/ordinals?${search.toString()}`);
    return {
      wallet_address: readStringField(payload, "wallet_address"),
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readOrdinalItems(payload)
    };
  }

  async getPublicWalletOrdinalDetail(query: { utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string }): Promise<BitfsPublicWalletOrdinalItem> {
    const utxoID = readNormalizedKey(String(query?.utxoID || ""), String(query?.utxo_id || "")).toLowerCase();
    const assetKey = readNormalizedKey(String(query?.assetKey || ""), String(query?.asset_key || ""));
    if (utxoID === "" && assetKey === "") {
      throw new Error("ordinal selector is required");
    }
    debugLogger.log("runtime", "get_public_wallet_ordinal_detail", {
      utxo_id: utxoID,
      asset_key: assetKey,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    if (utxoID !== "") {
      search.set("utxo_id", utxoID);
    }
    if (assetKey !== "") {
      search.set("asset_key", assetKey);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/ordinals/detail?${search.toString()}`);
    return readOrdinalItem(payload);
  }

  async listPublicWalletOrdinalEvents(query: { limit?: number; offset?: number; utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string }): Promise<BitfsPublicWalletAssetEventList> {
    const limit = clampBoundInt(query?.limit, 50, 1, 500);
    const offset = clampBoundInt(query?.offset, 0, 0, 1_000_000);
    const utxoID = readNormalizedKey(String(query?.utxoID || ""), String(query?.utxo_id || "")).toLowerCase();
    const assetKey = readNormalizedKey(String(query?.assetKey || ""), String(query?.asset_key || ""));
    debugLogger.log("runtime", "list_public_wallet_ordinal_events", {
      limit,
      offset,
      utxo_id: utxoID,
      asset_key: assetKey,
      trace_id: this.currentTraceID
    });
    const search = new URLSearchParams();
    search.set("limit", String(limit));
    search.set("offset", String(offset));
    if (utxoID !== "") {
      search.set("utxo_id", utxoID);
    }
    if (assetKey !== "") {
      search.set("asset_key", assetKey);
    }
    const payload = await this.fetchJSON<Record<string, unknown>>(`/api/v1/wallet/ordinals/events?${search.toString()}`);
    return {
      wallet_address: readStringField(payload, "wallet_address"),
      total: Math.max(0, readIntegerField(payload, "total")),
      limit,
      offset,
      items: readAssetEventItems(payload)
    };
  }

  async previewPublicWalletTokenSend(input: { tokenStandard?: string; token_standard?: string; assetKey?: string; asset_key?: string; amountText?: string; amount_text?: string; toAddress?: string; to_address?: string }): Promise<BitfsPublicWalletAssetPreviewResponse> {
    const payload = {
      token_standard: readNormalizedKey(String(input?.tokenStandard || ""), String(input?.token_standard || "")),
      asset_key: readNormalizedKey(String(input?.assetKey || ""), String(input?.asset_key || "")),
      amount_text: readNormalizedKey(String(input?.amountText || ""), String(input?.amount_text || "")),
      to_address: readNormalizedKey(String(input?.toAddress || ""), String(input?.to_address || ""))
    };
    debugLogger.log("runtime", "preview_public_wallet_token_send", {
      token_standard: payload.token_standard,
      asset_key: payload.asset_key,
      amount_text: payload.amount_text,
      to_address: payload.to_address,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/send/preview", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetPreviewResponse(body);
  }

  async previewPublicWalletTokenCreate(input: { tokenStandard?: string; token_standard?: string; symbol?: string; maxSupply?: string; max_supply?: string; decimals?: number; icon?: string }): Promise<BitfsPublicWalletAssetPreviewResponse> {
    const payload = {
      token_standard: readNormalizedKey(String(input?.tokenStandard || ""), String(input?.token_standard || "")),
      symbol: readNormalizedKey(String(input?.symbol || ""), ""),
      max_supply: readNormalizedKey(String(input?.maxSupply || ""), String(input?.max_supply || "")),
      decimals: Math.trunc(Number(input?.decimals ?? 0)),
      icon: readNormalizedKey(String(input?.icon || ""), "")
    };
    debugLogger.log("runtime", "preview_public_wallet_token_create", {
      token_standard: payload.token_standard,
      symbol: payload.symbol,
      max_supply: payload.max_supply,
      decimals: payload.decimals,
      icon: payload.icon,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/create/preview", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetPreviewResponse(body);
  }

  async signPublicWalletTokenCreate(input: { tokenStandard?: string; token_standard?: string; symbol?: string; maxSupply?: string; max_supply?: string; decimals?: number; icon?: string; expectedPreviewHash?: string; expected_preview_hash?: string }): Promise<BitfsPublicWalletAssetSignResponse> {
    const payload = {
      token_standard: readNormalizedKey(String(input?.tokenStandard || ""), String(input?.token_standard || "")),
      symbol: readNormalizedKey(String(input?.symbol || ""), ""),
      max_supply: readNormalizedKey(String(input?.maxSupply || ""), String(input?.max_supply || "")),
      decimals: Math.trunc(Number(input?.decimals ?? 0)),
      icon: readNormalizedKey(String(input?.icon || ""), ""),
      expected_preview_hash: readNormalizedKey(String(input?.expectedPreviewHash || ""), String(input?.expected_preview_hash || ""))
    };
    debugLogger.log("runtime", "sign_public_wallet_token_create", {
      token_standard: payload.token_standard,
      symbol: payload.symbol,
      max_supply: payload.max_supply,
      decimals: payload.decimals,
      icon: payload.icon,
      expected_preview_hash: payload.expected_preview_hash,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/create/sign", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSignResponse(body);
  }

  async submitPublicWalletTokenCreate(input: { signedTxHex?: string; signed_tx_hex?: string }): Promise<BitfsPublicWalletAssetSubmitResponse> {
    const payload = {
      signed_tx_hex: readNormalizedKey(String(input?.signedTxHex || ""), String(input?.signed_tx_hex || ""))
    };
    debugLogger.log("runtime", "submit_public_wallet_token_create", {
      signed_tx_hex_len: payload.signed_tx_hex.length,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/create/submit", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSubmitResponse(body);
  }

  async signPublicWalletTokenSend(input: { tokenStandard?: string; token_standard?: string; assetKey?: string; asset_key?: string; amountText?: string; amount_text?: string; toAddress?: string; to_address?: string; expectedPreviewHash?: string; expected_preview_hash?: string }): Promise<BitfsPublicWalletAssetSignResponse> {
    const payload = {
      token_standard: readNormalizedKey(String(input?.tokenStandard || ""), String(input?.token_standard || "")),
      asset_key: readNormalizedKey(String(input?.assetKey || ""), String(input?.asset_key || "")),
      amount_text: readNormalizedKey(String(input?.amountText || ""), String(input?.amount_text || "")),
      to_address: readNormalizedKey(String(input?.toAddress || ""), String(input?.to_address || "")),
      expected_preview_hash: readNormalizedKey(String(input?.expectedPreviewHash || ""), String(input?.expected_preview_hash || ""))
    };
    debugLogger.log("runtime", "sign_public_wallet_token_send", {
      token_standard: payload.token_standard,
      asset_key: payload.asset_key,
      amount_text: payload.amount_text,
      to_address: payload.to_address,
      expected_preview_hash: payload.expected_preview_hash,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/send/sign", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSignResponse(body);
  }

  async submitPublicWalletTokenSend(input: { signedTxHex?: string; signed_tx_hex?: string }): Promise<BitfsPublicWalletAssetSubmitResponse> {
    const payload = {
      signed_tx_hex: readNormalizedKey(String(input?.signedTxHex || ""), String(input?.signed_tx_hex || ""))
    };
    debugLogger.log("runtime", "submit_public_wallet_token_send", {
      signed_tx_hex_len: payload.signed_tx_hex.length,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/tokens/send/submit", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSubmitResponse(body);
  }

  async previewPublicWalletOrdinalTransfer(input: { utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string; toAddress?: string; to_address?: string }): Promise<BitfsPublicWalletAssetPreviewResponse> {
    const payload = {
      utxo_id: readNormalizedKey(String(input?.utxoID || ""), String(input?.utxo_id || "")),
      asset_key: readNormalizedKey(String(input?.assetKey || ""), String(input?.asset_key || "")),
      to_address: readNormalizedKey(String(input?.toAddress || ""), String(input?.to_address || ""))
    };
    debugLogger.log("runtime", "preview_public_wallet_ordinal_transfer", {
      utxo_id: payload.utxo_id,
      asset_key: payload.asset_key,
      to_address: payload.to_address,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/ordinals/transfer/preview", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetPreviewResponse(body);
  }

  async signPublicWalletOrdinalTransfer(input: { utxoID?: string; utxo_id?: string; assetKey?: string; asset_key?: string; toAddress?: string; to_address?: string; expectedPreviewHash?: string; expected_preview_hash?: string }): Promise<BitfsPublicWalletAssetSignResponse> {
    const payload = {
      utxo_id: readNormalizedKey(String(input?.utxoID || ""), String(input?.utxo_id || "")),
      asset_key: readNormalizedKey(String(input?.assetKey || ""), String(input?.asset_key || "")),
      to_address: readNormalizedKey(String(input?.toAddress || ""), String(input?.to_address || "")),
      expected_preview_hash: readNormalizedKey(String(input?.expectedPreviewHash || ""), String(input?.expected_preview_hash || ""))
    };
    debugLogger.log("runtime", "sign_public_wallet_ordinal_transfer", {
      utxo_id: payload.utxo_id,
      asset_key: payload.asset_key,
      to_address: payload.to_address,
      expected_preview_hash: payload.expected_preview_hash,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/ordinals/transfer/sign", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSignResponse(body);
  }

  async submitPublicWalletOrdinalTransfer(input: { signedTxHex?: string; signed_tx_hex?: string }): Promise<BitfsPublicWalletAssetSubmitResponse> {
    const payload = {
      signed_tx_hex: readNormalizedKey(String(input?.signedTxHex || ""), String(input?.signed_tx_hex || ""))
    };
    debugLogger.log("runtime", "submit_public_wallet_ordinal_transfer", {
      signed_tx_hex_len: payload.signed_tx_hex.length,
      trace_id: this.currentTraceID
    });
    const body = await this.fetchJSON<Record<string, unknown>>("/api/v1/wallet/ordinals/transfer/submit", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
      body: JSON.stringify(payload)
    });
    return readWalletAssetSubmitResponse(body);
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
      headers: {
        "content-type": "application/json",
        ...this.getCurrentVisitHeaders()
      },
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
    const items: PlanItem[] = [];
    for (const seedHash of seedHashes) {
      const plan = this.decisions.get(seedHash)?.plan;
      const { status, quotes } = await this.startDownloadByHash(seedHash, plan);
      const item = this.planItemFromJob(seedHash, status, quotes, plan);
      items.push(item);
    }
    debugLogger.log("runtime.http", "fetch_plans_ok", {
      item_count: items.length,
      trace_id: this.currentTraceID
    });
    return items;
  }

  private applyPlanDecision(seedHash: string, plan: PlanItem): void {
    const existing = this.decisions.get(seedHash);
    this.ensureDiscoveryOrder(seedHash);
    if (plan.local_ready || plan.status === "local") {
      debugLogger.log("runtime", "plan_decision_local", {
        seed_hash: seedHash,
        trace_id: this.currentTraceID
      });
      this.pending.delete(seedHash);
      this.decisions.set(seedHash, {
        mode: "local",
        estimatedTotalSat: Number(plan.estimated_total_sat || 0),
        plan
      });
      this.emitState();
      this.emitResourceEvent(seedHash, existing, "local", plan);
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
      this.emitResourceEvent(seedHash, existing, "approved", plan);
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
      this.pending.add(seedHash);
      this.decisions.set(seedHash, { mode: "pending", estimatedTotalSat: estimatedTotal, plan });
      this.emitState();
      this.emitResourceEvent(seedHash, existing, "pending", plan);
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
    this.emitResourceEvent(seedHash, existing, "blocked", plan);
  }

  private approveSeedInternal(seedHash: string, emitState: boolean): void {
    const decision = this.decisions.get(seedHash);
    if (!decision || decision.mode !== "pending") {
      return;
    }
    this.pending.delete(seedHash);
    this.approved.add(seedHash);
    decision.mode = "approved";
    if (emitState) {
      this.emitState();
    }
    this.emitRuntimeEvent("shell.resource.approval.changed", {
      seed_hash: seedHash,
      mode: decision.mode,
      plan_status: String(decision.plan.status || ""),
      estimated_total_sat: decision.estimatedTotalSat
    });
  }

  private ensureDiscoveryOrder(seedHash: string): void {
    if (this.discoveryOrder.has(seedHash)) {
      return;
    }
    this.discoveryOrder.set(seedHash, this.nextDiscoveryOrder);
    this.nextDiscoveryOrder += 1;
  }

  private toShellResource(seedHash: string, decision: ResourceDecision): ShellResource {
    const plan = decision.plan;
    return {
      seedHash,
      estimatedTotalSat: decision.estimatedTotalSat,
      seedPriceSat: Number(plan.seed_price || 0),
      chunkPriceSatPer64K: Number(plan.chunk_price_sat_per_64k || 0),
      fileSize: Number(plan.file_size || 0),
      chunkCount: Number(plan.chunk_count || 0),
      mimeHint: String(plan.mime_hint || ""),
      recommendedFileName: String(plan.recommended_file_name || ""),
      reason: String(plan.reason || plan.status || ""),
      mode: decision.mode,
      allowed: decision.mode !== "pending" && decision.mode !== "blocked",
      planStatus: String(plan.status || ""),
      localReady: Boolean(plan.local_ready || plan.status === "local"),
      isRoot: seedHash === this.currentRootSeedHash,
      discoveryOrder: this.discoveryOrder.get(seedHash) || 0
    };
  }

  private async openNodeLocator(locator: ParsedNodeLocator, visitID: string): Promise<string> {
    const result = await this.resolveNodeLocatorSeed(locator);
    const seedHash = result.seedHash;
    this.resetPageSession(locator.locator, buildBitfsViewerURL(seedHash), seedHash, "open_locator_node");
    this.finishVisitSuccess(visitID, "node locator opened");
    debugLogger.log("runtime", "open_locator_node", {
      locator: locator.locator,
      node_pubkey_hex: locator.nodePubkeyHex,
      route: locator.route,
      seed_hash: seedHash,
      trace_id: this.currentTraceID
    });
    await this.refreshVisitAccounting(visitID, "open_locator_node");
    return this.currentURL;
  }

  private async openResolverLocator(locator: ParsedResolverLocator, visitID: string): Promise<string> {
    const result = await this.resolveResolverLocatorSeed(locator);
    const seedHash = result.seedHash;
    this.resetPageSession(locator.locator, buildBitfsViewerURL(seedHash), seedHash, "open_locator_resolver");
    this.finishVisitSuccess(visitID, "resolver locator opened");
    debugLogger.log("runtime", "open_locator_resolver", {
      locator: locator.locator,
      resolver_pubkey_hex: locator.resolverPubkeyHex,
      name: locator.name,
      route: locator.route,
      seed_hash: seedHash,
      trace_id: this.currentTraceID
    });
    await this.refreshVisitAccounting(visitID, "open_locator_resolver");
    return this.currentURL;
  }

  private async resolveNodeLocatorSeed(locator: ParsedNodeLocator): Promise<{ seedHash: string }> {
    const handler = this.locatorHandlers.resolveNodeLocator;
    if (!handler) {
      throw new Error("node locator resolve is not implemented yet");
    }
    const result = await handler(locator, this.getCurrentVisitContext());
    const seedHash = normalizeSeedHash(result.seedHash);
    if (seedHash === "") {
      throw new Error("node locator returned invalid seed hash");
    }
    return { seedHash };
  }

  private async resolveResolverLocatorSeed(locator: ParsedResolverLocator): Promise<{ seedHash: string; targetPubkeyHex: string }> {
    const handler = this.locatorHandlers.resolveResolverLocator;
    if (!handler) {
      throw new Error("resolver locator resolve is not implemented yet");
    }
    const result = await handler(locator, this.getCurrentVisitContext());
    const seedHash = normalizeSeedHash(result.seedHash);
    if (seedHash === "") {
      throw new Error("resolver locator returned invalid seed hash");
    }
    return {
      seedHash,
      targetPubkeyHex: String(result.targetPubkeyHex || "").trim().toLowerCase()
    };
  }

  private resetPageSession(locator: string, viewerURL: string, rootSeedHash: string, reason: string): void {
    this.currentTraceID = createTraceID();
    this.currentURL = locator;
    this.currentViewerURL = viewerURL;
    this.currentRootSeedHash = rootSeedHash;
    this.autoSpentSat = 0;
    this.pending.clear();
    this.decisions.clear();
    this.approved.clear();
    this.discoveryOrder.clear();
    this.downloadJobs.clear();
    this.nextDiscoveryOrder = 1;
    this.ensureDiscoveryOrder(rootSeedHash);
    this.lastError = "";
    debugLogger.log("runtime", "reset_page_session", {
      reason,
      locator,
      viewer_url: viewerURL,
      seed_hash: rootSeedHash,
      trace_id: this.currentTraceID
    });
    this.emitState();
    this.emitRuntimeEvent("shell.page.session.reset", {
      reason,
      locator,
      viewer_url: viewerURL,
      current_root_seed_hash: rootSeedHash
    });
  }

  private beginVisit(locator: string, reason: string): string {
    const nowUnix = Math.floor(Date.now() / 1000);
    const visitID = createTraceID();
    // 设计说明：
    // - “访问账目概述”跟随一次 locator 打开尝试，而不是跟随当前页面是否成功切换；
    // - 这样失败访问已经花掉的钱也能留在侧栏，不会被旧页面状态掩盖。
    this.currentVisit = {
      visitID,
      locator: String(locator || "").trim(),
      status: "opening",
      startedAtUnix: nowUnix,
      lastUpdatedAtUnix: nowUnix,
      finishedAtUnix: 0,
      note: reason,
      totalUsedSatoshi: 0,
      totalReturnedSatoshi: 0,
      resolverUsedSatoshi: 0,
      reachabilityUsedSatoshi: 0,
      contentUsedSatoshi: 0,
      otherUsedSatoshi: 0,
      itemCount: 0,
      buckets: []
    };
    this.currentVisitBaselineFlowID = 0;
    this.emitState();
    return visitID;
  }

  private finishVisitSuccess(visitID: string, note: string): void {
    if (this.currentVisit.visitID !== visitID) {
      return;
    }
    const nowUnix = Math.floor(Date.now() / 1000);
    this.currentVisit = {
      ...this.currentVisit,
      status: "open",
      lastUpdatedAtUnix: nowUnix,
      finishedAtUnix: nowUnix,
      note
    };
    this.emitState();
  }

  private finishVisitFailure(visitID: string, note: string): void {
    if (this.currentVisit.visitID !== visitID) {
      return;
    }
    const nowUnix = Math.floor(Date.now() / 1000);
    this.currentVisit = {
      ...this.currentVisit,
      status: "failed",
      lastUpdatedAtUnix: nowUnix,
      finishedAtUnix: nowUnix,
      note: String(note || "").trim() || "visit failed"
    };
    this.emitState();
  }

  private async captureVisitBaseline(visitID: string): Promise<void> {
    if (this.currentVisit.visitID !== visitID) {
      return;
    }
    this.currentVisitBaselineFlowID = await this.fetchLatestWalletFundFlowIDSilent();
  }

  private emitState(): void {
    this.emit("state", this.snapshot());
  }

  private emitResourceEvent(
    seedHash: string,
    previous: ResourceDecision | undefined,
    mode: ResourceDecision["mode"],
    plan: PlanItem
  ): void {
    const payload = {
      seed_hash: seedHash,
      mode,
      plan_status: String(plan.status || ""),
      local_ready: Boolean(plan.local_ready || plan.status === "local"),
      estimated_total_sat: Number(plan.estimated_total_sat || 0),
      is_root: seedHash === this.currentRootSeedHash,
      discovery_order: this.discoveryOrder.get(seedHash) || 0,
      mime_hint: String(plan.mime_hint || ""),
      recommended_file_name: String(plan.recommended_file_name || "")
    };
    if (!previous) {
      this.emitRuntimeEvent("shell.resource.discovered", payload);
      return;
    }
    if (previous.mode !== mode || previous.plan.status !== plan.status || Boolean(previous.plan.local_ready) !== Boolean(plan.local_ready)) {
      this.emitRuntimeEvent("shell.resource.state.changed", payload);
    }
  }

  private emitRuntimeEvent(topic: string, payload: Record<string, unknown>): void {
    this.eventSeq += 1;
    const event: BitfsRuntimeEvent = {
      seq: this.eventSeq,
      runtime_epoch: this.runtimeEpoch,
      topic,
      scope: "private",
      occurred_at_unix: Math.floor(Date.now() / 1000),
      producer: "shell_runtime",
      trace_id: this.currentTraceID,
      payload
    };
    this.emit("event", event);
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
    return body;
  }

  private async fetchJSONSilent<T>(pathOrURL: string, init?: RequestInit): Promise<T> {
    const target = pathOrURL.startsWith("http://") || pathOrURL.startsWith("https://")
      ? pathOrURL
      : `${this.clientAPIBase}${pathOrURL}`;
    const response = await fetch(target, init);
    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || `request failed: ${response.status}`);
    }
    return await response.json() as T;
  }

  private async fetchStatusDataJSON<T>(pathOrURL: string, init?: RequestInit): Promise<T> {
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
    const text = await response.text();
    const raw = text.trim();
    let payload: StatusDataEnvelope<T> | null = null;
    if (raw !== "") {
      try {
        payload = JSON.parse(raw) as StatusDataEnvelope<T>;
      } catch {
        payload = null;
      }
    }
    if (payload && typeof payload === "object") {
      if (String(payload.status || "") === "ok") {
        if (!Object.prototype.hasOwnProperty.call(payload, "data")) {
          this.lastError = "invalid status envelope";
          debugLogger.log("runtime.http", "response_error", {
            method,
            url: target,
            status: response.status,
            body: raw,
            trace_id: this.currentTraceID
          });
          throw new Error("invalid status envelope");
        }
        if (!response.ok) {
          this.lastError = raw || `request failed: ${response.status}`;
          debugLogger.log("runtime.http", "response_error", {
            method,
            url: target,
            status: response.status,
            body: raw,
            trace_id: this.currentTraceID
          });
          throw new Error(raw || `request failed: ${response.status}`);
        }
        this.lastError = "";
        debugLogger.log("runtime.http", "response_ok", {
          method,
          url: target,
          status: response.status,
          trace_id: this.currentTraceID
        });
        return payload.data as T;
      }
      if (String(payload.status || "") === "error") {
        const code = typeof payload.error === "object" && payload.error !== null && !Array.isArray(payload.error)
          ? String(payload.error.code || "").trim()
          : "";
        const message = typeof payload.error === "object" && payload.error !== null && !Array.isArray(payload.error)
          ? String(payload.error.message || "").trim()
          : String(payload.error || "").trim();
        const errorText = code && message ? `${code}: ${message}` : (message || code || `request failed: ${response.status}`);
        this.lastError = errorText;
        debugLogger.log("runtime.http", "response_error", {
          method,
          url: target,
          status: response.status,
          code,
          body: raw,
          trace_id: this.currentTraceID
        });
        throw new Error(errorText);
      }
    }
    if (!response.ok) {
      this.lastError = raw || `request failed: ${response.status}`;
      debugLogger.log("runtime.http", "response_error", {
        method,
        url: target,
        status: response.status,
        body: raw,
        trace_id: this.currentTraceID
      });
      throw new Error(raw || `request failed: ${response.status}`);
    }
    this.lastError = raw === "" ? "" : raw;
    debugLogger.log("runtime.http", "response_error", {
      method,
      url: target,
      status: response.status,
      body: raw,
      trace_id: this.currentTraceID
    });
    throw new Error("invalid status envelope");
  }

  private async fetchLatestWalletFundFlowIDSilent(): Promise<number> {
    try {
      const payload = await this.fetchJSONSilent<WalletFundFlowListResponse>("/api/v1/wallet/fund-flows?limit=1&offset=0");
      const first = Array.isArray(payload.items) ? payload.items[0] : null;
      return Math.max(0, clampBoundInt(first?.id, 0, 0, Number.MAX_SAFE_INTEGER));
    } catch {
      return 0;
    }
  }

  private async fetchVisitFundFlowsSilent(visitID: string): Promise<WalletFundFlowItem[]> {
    if (this.currentVisit.visitID !== visitID) {
      return [];
    }
    try {
      // 设计说明：
      // - “访问会话概述”和 settings 账务明细要看同一批后台业务流水；
      // - 所以这里不能再按时间窗口猜测，而是直接按 visit_id 查 /api/v1/wallet/fund-flows。
      const payload = await this.fetchJSONSilent<WalletFundFlowListResponse>(`/api/v1/wallet/fund-flows?limit=200&offset=0&visit_id=${encodeURIComponent(visitID)}`);
      return Array.isArray(payload.items) ? payload.items : [];
    } catch (error) {
      if (this.currentVisit.visitID === visitID) {
        this.currentVisit = {
          ...this.currentVisit,
          note: `账目刷新失败: ${error instanceof Error ? error.message : String(error)}`,
          lastUpdatedAtUnix: Math.floor(Date.now() / 1000)
        };
        this.emitState();
      }
      return [];
    }
  }
}

function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(value)) {
    return "";
  }
  return value;
}

function normalizePubkeyHex(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!/^[0-9a-f]{66}$/.test(value)) {
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

function clampBoundInt(raw: unknown, fallback: number, min: number, max: number): number {
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(value)));
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

function readStringField(source: Record<string, unknown>, ...keys: string[]): string {
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string") {
      return value.trim();
    }
  }
  return "";
}

function readIntegerField(source: Record<string, unknown>, key: string): number {
  const value = source[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.trunc(parsed);
    }
  }
  return 0;
}

function readBooleanField(source: Record<string, unknown>, key: string): boolean {
  const value = source[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "true" || normalized === "1" || normalized === "yes";
  }
  return false;
}

function normalizePublicWalletHistoryDirection(raw: string): BitfsPublicWalletHistoryDirection {
  const value = raw.trim().toLowerCase();
  if (value === "in") {
    return "in";
  }
  if (value === "out") {
    return "out";
  }
  return "unknown";
}

// fund_flows.direction → history.direction 映射
// IN/credit → in（资金增加）
// out/lock/settle/debit → out（资金减少）
// internal/info/其他 → unknown（内部调账或信息记录）
function fundFlowDirectionToHistory(raw: string): BitfsPublicWalletHistoryDirection {
  const value = raw.trim().toLowerCase();
  switch (value) {
    case "in":
    case "credit":
      return "in";
    case "out":
    case "lock":
    case "settle":
    case "debit":
      return "out";
    default:
      return "unknown";
  }
}

// 前端 in/out 筛选映射到后端 fund_flows 的实际方向值
function historyDirectionToFundFlowDirections(dir: BitfsPublicWalletHistoryDirection): string[] {
  switch (dir) {
    case "in":
      return ["in", "credit"];
    case "out":
      return ["out", "lock", "settle", "debit"];
    default:
      return []; // unknown 不传筛选
  }
}

function normalizePublicWalletTokenStandard(raw: string): "" | "bsv20" | "bsv21" {
  const value = raw.trim().toLowerCase();
  if (value === "bsv20" || value === "bsv21") {
    return value;
  }
  return "";
}

function readHistoryItems(payload: Record<string, unknown>): BitfsPublicWalletHistoryItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletHistoryItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    const item = rawItem as Record<string, unknown>;
    items.push({
      id: Math.max(0, readIntegerField(item, "id")),
      txid: readStringField(item, "txid").toLowerCase(),
      direction: normalizePublicWalletHistoryDirection(readStringField(item, "direction")),
      amount_satoshi: Math.abs(readIntegerField(item, "amount_satoshi")),
      status: readStringField(item, "status").toLowerCase(),
      block_height: Math.max(0, readIntegerField(item, "block_height")),
      occurred_at_unix: Math.max(0, readIntegerField(item, "occurred_at_unix"))
    });
  }
  return items;
}

// 从 fact_* 事实表组装的 fund-flows API 映射成 history 返回结构
// 设计说明：对外保持 history 接口不变，内部改读 fund_flows
function readHistoryItemsFromFundFlows(payload: Record<string, unknown>): BitfsPublicWalletHistoryItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletHistoryItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    const item = rawItem as Record<string, unknown>;
    items.push({
      id: Math.max(0, readIntegerField(item, "id")),
      txid: readStringField(item, "related_txid").toLowerCase(),
      direction: fundFlowDirectionToHistory(readStringField(item, "direction")),
      amount_satoshi: Math.abs(readIntegerField(item, "amount_satoshi")),
      status: "confirmed", // fund_flows 没有 status 字段，默认已确认
      block_height: 0, // fund_flows 没有 block_height 字段
      occurred_at_unix: Math.max(0, readIntegerField(item, "created_at_unix"))
    });
  }
  return items;
}

function readTokenBalanceItems(payload: Record<string, unknown>): BitfsPublicWalletTokenBalanceItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletTokenBalanceItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    const item = rawItem as Record<string, unknown>;
    const standard = normalizePublicWalletTokenStandard(readStringField(item, "token_standard"));
    if (standard === "") {
      continue;
    }
    items.push({
      token_standard: standard,
      asset_key: readStringField(item, "asset_key"),
      asset_symbol: readStringField(item, "asset_symbol"),
      quantity_text: readStringField(item, "quantity_text"),
      output_count: Math.max(0, readIntegerField(item, "output_count")),
      source_name: readStringField(item, "source_name"),
      updated_at_unix: Math.max(0, readIntegerField(item, "updated_at_unix"))
    });
  }
  return items;
}

function readTokenOutputItems(payload: Record<string, unknown>): BitfsPublicWalletTokenOutputItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletTokenOutputItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    try {
      items.push(readTokenOutputItem(rawItem as Record<string, unknown>));
    } catch {
      continue;
    }
  }
  return items;
}

function readTokenOutputItem(payload: Record<string, unknown>): BitfsPublicWalletTokenOutputItem {
  const standard = normalizePublicWalletTokenStandard(readStringField(payload, "token_standard"));
  if (standard === "") {
    throw new Error("invalid token standard in response");
  }
  return {
    utxo_id: readStringField(payload, "utxo_id").toLowerCase(),
    wallet_address: readStringField(payload, "wallet_address"),
    txid: readStringField(payload, "txid").toLowerCase(),
    vout: Math.max(0, readIntegerField(payload, "vout")),
    value_satoshi: Math.max(0, readIntegerField(payload, "value_satoshi")),
    allocation_class: readStringField(payload, "allocation_class"),
    allocation_reason: readStringField(payload, "allocation_reason"),
    token_standard: standard,
    asset_key: readStringField(payload, "asset_key"),
    asset_symbol: readStringField(payload, "asset_symbol"),
    quantity_text: readStringField(payload, "quantity_text"),
    source_name: readStringField(payload, "source_name"),
    updated_at_unix: Math.max(0, readIntegerField(payload, "updated_at_unix")),
    payload: payload.payload ?? null
  };
}

function readAssetEventItems(payload: Record<string, unknown>): BitfsPublicWalletAssetEventItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletAssetEventItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    const item = rawItem as Record<string, unknown>;
    items.push({
      id: Math.max(0, readIntegerField(item, "id")),
      created_at_unix: Math.max(0, readIntegerField(item, "created_at_unix")),
      utxo_id: readStringField(item, "utxo_id").toLowerCase(),
      wallet_address: readStringField(item, "wallet_address"),
      asset_group: readStringField(item, "asset_group"),
      asset_standard: readStringField(item, "asset_standard"),
      asset_key: readStringField(item, "asset_key"),
      asset_symbol: readStringField(item, "asset_symbol"),
      quantity_text: readStringField(item, "quantity_text"),
      source_name: readStringField(item, "source_name"),
      event_type: readStringField(item, "event_type"),
      ref_txid: readStringField(item, "ref_txid").toLowerCase(),
      ref_business_id: readStringField(item, "ref_business_id"),
      note: readStringField(item, "note"),
      payload: item.payload ?? null
    });
  }
  return items;
}

function readWalletAssetPreviewResponse(payload: Record<string, unknown>): BitfsPublicWalletAssetPreviewResponse {
  const rawPreview = payload.preview && typeof payload.preview === "object" && !Array.isArray(payload.preview)
    ? payload.preview as Record<string, unknown>
    : {};
  const rawChanges = Array.isArray(rawPreview.changes) ? rawPreview.changes : [];
  const rawDetailLines = Array.isArray(rawPreview.detail_lines) ? rawPreview.detail_lines : [];
  const rawSelectedAssetUTXOIDs = Array.isArray(rawPreview.selected_asset_utxo_ids) ? rawPreview.selected_asset_utxo_ids : [];
  const rawSelectedFeeUTXOIDs = Array.isArray(rawPreview.selected_fee_utxo_ids) ? rawPreview.selected_fee_utxo_ids : [];
  return {
    ok: readBooleanField(payload, "ok"),
    code: readStringField(payload, "code"),
    message: readStringField(payload, "message"),
    preview: {
      action: readStringField(rawPreview, "action"),
      feasible: readBooleanField(rawPreview, "feasible"),
      can_sign: readBooleanField(rawPreview, "can_sign"),
      summary: readStringField(rawPreview, "summary"),
      detail_lines: rawDetailLines.map((item) => String(item || "").trim()).filter((item) => item !== ""),
      warning_level: readStringField(rawPreview, "warning_level"),
      estimated_network_fee_bsv_sat: Math.max(0, readIntegerField(rawPreview, "estimated_network_fee_bsv_sat")),
      fee_funding_target_bsv_sat: Math.max(0, readIntegerField(rawPreview, "fee_funding_target_bsv_sat")),
      selected_asset_utxo_ids: rawSelectedAssetUTXOIDs.map((item) => String(item || "").trim().toLowerCase()).filter((item) => item !== ""),
      selected_fee_utxo_ids: rawSelectedFeeUTXOIDs.map((item) => String(item || "").trim().toLowerCase()).filter((item) => item !== ""),
      txid: readStringField(rawPreview, "txid").toLowerCase(),
      preview_hash: readStringField(rawPreview, "preview_hash").toLowerCase(),
      changes: rawChanges
        .filter((item) => item && typeof item === "object" && !Array.isArray(item))
        .map((item) => {
          const change = item as Record<string, unknown>;
          return {
            owner_scope: readStringField(change, "owner_scope"),
            asset_group: readStringField(change, "asset_group"),
            asset_standard: readStringField(change, "asset_standard"),
            asset_key: readStringField(change, "asset_key"),
            asset_symbol: readStringField(change, "asset_symbol"),
            quantity_text: readStringField(change, "quantity_text"),
            direction: readStringField(change, "direction"),
            note: readStringField(change, "note")
          };
        })
    }
  };
}

function readWalletAssetSignResponse(payload: Record<string, unknown>): BitfsPublicWalletAssetSignResponse {
  const previewResp = readWalletAssetPreviewResponse(payload);
  return {
    ok: previewResp.ok,
    code: previewResp.code,
    message: previewResp.message,
    preview: previewResp.preview,
    signed_tx_hex: readStringField(payload, "signed_tx_hex").toLowerCase(),
    txid: readStringField(payload, "txid").toLowerCase()
  };
}

function readWalletAssetSubmitResponse(payload: Record<string, unknown>): BitfsPublicWalletAssetSubmitResponse {
  return {
    ok: readBooleanField(payload, "ok"),
    code: readStringField(payload, "code"),
    message: readStringField(payload, "message"),
    txid: readStringField(payload, "txid").toLowerCase(),
    token_id: readStringField(payload, "token_id").toLowerCase(),
    status: readStringField(payload, "status")
  };
}

function readOrdinalItems(payload: Record<string, unknown>): BitfsPublicWalletOrdinalItem[] {
  const rawItems = Array.isArray(payload.items) ? payload.items : [];
  const items: BitfsPublicWalletOrdinalItem[] = [];
  for (const rawItem of rawItems) {
    if (!rawItem || typeof rawItem !== "object" || Array.isArray(rawItem)) {
      continue;
    }
    items.push(readOrdinalItem(rawItem as Record<string, unknown>));
  }
  return items;
}

function readOrdinalItem(payload: Record<string, unknown>): BitfsPublicWalletOrdinalItem {
  return {
    utxo_id: readStringField(payload, "utxo_id").toLowerCase(),
    wallet_address: readStringField(payload, "wallet_address"),
    txid: readStringField(payload, "txid").toLowerCase(),
    vout: Math.max(0, readIntegerField(payload, "vout")),
    value_satoshi: Math.max(0, readIntegerField(payload, "value_satoshi")),
    allocation_class: readStringField(payload, "allocation_class"),
    allocation_reason: readStringField(payload, "allocation_reason"),
    asset_standard: "ordinal",
    asset_key: readStringField(payload, "asset_key"),
    asset_symbol: readStringField(payload, "asset_symbol"),
    source_name: readStringField(payload, "source_name"),
    updated_at_unix: Math.max(0, readIntegerField(payload, "updated_at_unix")),
    payload: payload.payload ?? null
  };
}

function readNormalizedKey(...values: string[]): string {
  for (const raw of values) {
    const value = String(raw || "").trim();
    if (value !== "") {
      return value;
    }
  }
  return "";
}

function createEmptyVisitAccounting(): ShellVisitAccounting {
  return {
    visitID: "",
    locator: "",
    status: "idle",
    startedAtUnix: 0,
    lastUpdatedAtUnix: 0,
    finishedAtUnix: 0,
    note: "尚未开始访问。",
    totalUsedSatoshi: 0,
    totalReturnedSatoshi: 0,
    resolverUsedSatoshi: 0,
    reachabilityUsedSatoshi: 0,
    contentUsedSatoshi: 0,
    otherUsedSatoshi: 0,
    itemCount: 0,
    buckets: []
  };
}

function cloneVisitAccounting(source: ShellVisitAccounting): ShellVisitAccounting {
  return {
    ...source,
    buckets: Array.isArray(source.buckets)
      ? source.buckets.map((item) => ({ ...item }))
      : []
  };
}

function summarizeVisitFundFlows(
  visit: ShellVisitAccounting,
  flows: WalletFundFlowItem[],
  baselineFlowID: number
): {
  totalUsedSatoshi: number;
  totalReturnedSatoshi: number;
  resolverUsedSatoshi: number;
  reachabilityUsedSatoshi: number;
  contentUsedSatoshi: number;
  otherUsedSatoshi: number;
  itemCount: number;
  buckets: ShellVisitAccountingBucket[];
} {
  const startedAtUnix = Math.max(0, Number(visit.startedAtUnix || 0));
  const minID = Math.max(0, Number(baselineFlowID || 0));
  const bucketMap = new Map<string, ShellVisitAccountingBucket>();
  let totalUsedSatoshi = 0;
  let totalReturnedSatoshi = 0;
  let resolverUsedSatoshi = 0;
  let reachabilityUsedSatoshi = 0;
  let contentUsedSatoshi = 0;
  let otherUsedSatoshi = 0;
  let itemCount = 0;
  for (const item of flows) {
    const id = clampBoundInt(item?.id, 0, 0, Number.MAX_SAFE_INTEGER);
    const createdAtUnix = clampBoundInt(item?.created_at_unix, 0, 0, Number.MAX_SAFE_INTEGER);
    if (id <= minID) {
      continue;
    }
    if (startedAtUnix > 0 && createdAtUnix > 0 && createdAtUnix < startedAtUnix) {
      continue;
    }
    const usedSatoshi = Math.max(0, clampBoundInt(item?.used_satoshi, 0, 0, Number.MAX_SAFE_INTEGER));
    const returnedSatoshi = Math.max(0, clampBoundInt(item?.returned_satoshi, 0, 0, Number.MAX_SAFE_INTEGER));
    const purpose = String(item?.purpose || "").trim();
    const bucketKey = shellPurposeUsedInVisitBucket(purpose) ? purpose : "other";
    const bucket = bucketMap.get(bucketKey) || {
      purpose: bucketKey,
      label: visitBucketLabel(bucketKey),
      usedSatoshi: 0,
      count: 0
    };
    bucket.usedSatoshi += usedSatoshi;
    bucket.count += 1;
    bucketMap.set(bucketKey, bucket);
    itemCount += 1;
    totalUsedSatoshi += usedSatoshi;
    totalReturnedSatoshi += returnedSatoshi;
    switch (visitBucketCategory(bucketKey)) {
      case "resolver":
        resolverUsedSatoshi += usedSatoshi;
        break;
      case "reachability":
        reachabilityUsedSatoshi += usedSatoshi;
        break;
      case "content":
        contentUsedSatoshi += usedSatoshi;
        break;
      default:
        otherUsedSatoshi += usedSatoshi;
        break;
    }
  }
  return {
    totalUsedSatoshi,
    totalReturnedSatoshi,
    resolverUsedSatoshi,
    reachabilityUsedSatoshi,
    contentUsedSatoshi,
    otherUsedSatoshi,
    itemCount,
    buckets: Array.from(bucketMap.values()).sort((a, b) => {
      if (a.usedSatoshi !== b.usedSatoshi) {
        return b.usedSatoshi - a.usedSatoshi;
      }
      return a.label.localeCompare(b.label, "zh-CN");
    }).slice(0, 6)
  };
}

function visitBucketCategory(purpose: string): "resolver" | "reachability" | "content" | "other" {
  return shellVisitBucketForPurpose(purpose);
}

function visitBucketLabel(purpose: string): string {
  return shellAccountingLabelForPurpose(purpose);
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

function guessContentTypeForDownload(filePath: string, body: Uint8Array, mimeHint: string): string {
  const hint = String(mimeHint || "").trim().toLowerCase();
  if (hint !== "") {
    return String(mimeHint || "").trim();
  }
  const ext = String(filePath || "").trim().toLowerCase().split(".").pop() || "";
  switch (ext) {
    case "html":
    case "htm":
      return "text/html; charset=utf-8";
    case "js":
    case "mjs":
    case "cjs":
      return "application/javascript; charset=utf-8";
    case "css":
      return "text/css; charset=utf-8";
    case "json":
      return "application/json; charset=utf-8";
    case "svg":
      return "image/svg+xml";
  }
  const head = new TextDecoder().decode(body.slice(0, 512)).trim();
  if (head === "") {
    return "application/octet-stream";
  }
  const lower = head.toLowerCase();
  if (lower.startsWith("<!doctype html") || lower.startsWith("<html")) {
    return "text/html; charset=utf-8";
  }
  if (lower.startsWith("<?xml") || lower.includes("<svg")) {
    return "image/svg+xml";
  }
  if ((lower.startsWith("{") || lower.startsWith("[")) && looksLikeJSONText(head)) {
    return "application/json; charset=utf-8";
  }
  if (looksLikeJavaScriptText(head)) {
    return "application/javascript; charset=utf-8";
  }
  if (looksLikeCSSText(lower)) {
    return "text/css; charset=utf-8";
  }
  return "application/octet-stream";
}

function looksLikeJSONText(text: string): boolean {
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
}

function looksLikeCSSText(lower: string): boolean {
  if (lower === "") {
    return false;
  }
  if (looksLikeJavaScriptText(lower)) {
    return false;
  }
  if (lower.startsWith(":root") || lower.startsWith("@media") || lower.startsWith("@import")) {
    return true;
  }
  if (!lower.includes("{") || !lower.includes("}")) {
    return false;
  }
  if (lower.includes("function") || lower.includes("=>") || lower.includes("const ") || lower.includes("let ") || lower.includes("var ") || lower.includes("return ")) {
    return false;
  }
  return lower.includes(":") && lower.includes(";");
}

function looksLikeJavaScriptText(text: string): boolean {
  const lower = String(text || "").trim().toLowerCase();
  if (lower === "") {
    return false;
  }
  const prefixes = [
    "(function",
    "(()=>",
    "const ",
    "let ",
    "var ",
    "function ",
    "import ",
    "export ",
    "\"use strict\"",
    "'use strict'"
  ];
  for (const prefix of prefixes) {
    if (lower.startsWith(prefix)) {
      return true;
    }
  }
  return lower.includes("=>") ||
    lower.includes("document.") ||
    lower.includes("window.") ||
    lower.includes("createelement(") ||
    lower.includes("queryselector(");
}

function createTraceID(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(16).slice(2, 10)}`;
}
