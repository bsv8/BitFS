import { useEffect, useState } from "react";

type RunStep = {
  name: string;
  ok: boolean;
  detail?: string;
};

type DomainRegisterRunResult = {
  ok: boolean;
  error?: string;
  wallet_pubkey_hex?: string;
  resolver_pubkey_hex?: string;
  domain_name?: string;
  target_pubkey_hex?: string;
  pricing?: BitfsDomainPricing | null;
  query?: BitfsDomainQueryResponse | null;
  lock?: BitfsDomainLockResponse | null;
  sign?: BitfsWalletBusinessSignResponse | null;
  submit?: BitfsDomainRegisterSubmitResponse | null;
  list_owned?: BitfsDomainListOwnedResponse | null;
  resolve_query?: BitfsDomainResolveResponse | null;
  steps: RunStep[];
};

type RunInput = {
  resolverPubkeyHex?: string;
  domainName?: string;
  targetPubkeyHex?: string;
};

declare global {
  interface Window {
    __bitfsE2EDomainRegister?: {
      run: (input?: RunInput) => Promise<DomainRegisterRunResult>;
      getState: () => DomainRegisterRunResult | null;
    };
  }
}

export default function App() {
  const [walletSummary, setWalletSummary] = useState<BitfsWalletSummary | null>(null);
  const [clientInfo, setClientInfo] = useState<BitfsClientInfo | null>(null);
  const [resolverPubkeyHex, setResolverPubkeyHex] = useState("");
  const [domainName, setDomainName] = useState("");
  const [targetPubkeyHex, setTargetPubkeyHex] = useState("");
  const [busy, setBusy] = useState(false);
  const [lastResult, setLastResult] = useState<DomainRegisterRunResult | null>(null);
  const [pageError, setPageError] = useState("");

  useEffect(() => {
    let cancelled = false;
    async function loadWalletState() {
      if (!window.bitfs) {
        return;
      }
      try {
        const [nextWalletSummary, nextClientInfo] = await Promise.all([
          window.bitfs.wallet.summary(),
          window.bitfs.client.info()
        ]);
        if (cancelled) {
          return;
        }
        setWalletSummary(nextWalletSummary);
        setClientInfo(nextClientInfo);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setPageError(error instanceof Error ? error.message : String(error));
      }
    }
    void loadWalletState();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const fallback = normalizePubkeyHex(walletSummary?.pubkey_hex || clientInfo?.pubkey_hex || "");
    if (fallback !== "" && targetPubkeyHex.trim() === "") {
      setTargetPubkeyHex(fallback);
    }
  }, [walletSummary?.pubkey_hex, clientInfo?.pubkey_hex, targetPubkeyHex]);

  useEffect(() => {
    window.__bitfsE2EDomainRegister = {
      run: (input?: RunInput) => runFlow(input || {}),
      getState: () => lastResult
    };
    return () => {
      delete window.__bitfsE2EDomainRegister;
    };
  });

  async function runFlow(input: RunInput): Promise<DomainRegisterRunResult> {
    const bridge = window.bitfs;
    const steps: RunStep[] = [];
    const finish = (result: DomainRegisterRunResult) => {
      setLastResult(result);
      return result;
    };
    if (!bridge) {
      return finish({
        ok: false,
        error: "window.bitfs unavailable",
        steps
      });
    }
    const walletSnapshot = await waitForWalletIdentity(bridge);
    setWalletSummary(walletSnapshot.walletSummary);
    setClientInfo(walletSnapshot.clientInfo);
    const walletSummaryNow = walletSnapshot.walletSummary;
    const clientInfoNow = walletSnapshot.clientInfo;
    const normalizedResolver = normalizePubkeyHex(input.resolverPubkeyHex || resolverPubkeyHex);
    const normalizedName = normalizeDomainName(input.domainName || domainName);
    const normalizedTarget = normalizePubkeyHex(input.targetPubkeyHex || targetPubkeyHex || walletSummaryNow.pubkey_hex || clientInfoNow.pubkey_hex || "");
    const walletPubkeyHex = normalizePubkeyHex(walletSummaryNow.pubkey_hex || clientInfoNow.pubkey_hex || "");
    if (normalizedResolver === "") {
      return finish({ ok: false, error: "resolver pubkey invalid", steps });
    }
    if (normalizedName === "") {
      return finish({ ok: false, error: "domain name invalid", steps });
    }
    if (normalizedTarget === "") {
      return finish({ ok: false, error: "target pubkey invalid", steps });
    }
    if (walletPubkeyHex === "") {
      return finish({ ok: false, error: "wallet pubkey unavailable", steps });
    }
    setBusy(true);
    setPageError("");
    try {
      const capabilityResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "node.v1.capabilities_show",
        contentType: "application/json",
        body: {}
      });
      const capabilityBody = readPeerCallBodyJson<BitfsCapabilitiesShowBody>(capabilityResp);
      const hasDomain = capabilityBody.capabilities.some((item) => item.id === "domain" && Number(item.version || 0) === 1);
      if (!hasDomain) {
        throw new Error("target node does not expose domain v1 capability");
      }
      steps.push({ name: "capabilities_show", ok: true, detail: "domain.v1 available" });

      const pricingResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.pricing",
        contentType: "application/json",
        body: {}
      });
      const pricing = readPeerCallBodyJson<BitfsDomainPricing>(pricingResp);
      steps.push({ name: "pricing", ok: true, detail: formatSat(pricing.register_price_satoshi || 0) });

      const queryResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.query",
        contentType: "application/json",
        body: { name: normalizedName }
      });
      const query = readPeerCallBodyJson<BitfsDomainQueryResponse>(queryResp);
      steps.push({ name: "query", ok: true, detail: String(query.status || "") });
      if (query.available !== true) {
        throw new Error(`domain not available: ${String(query.status || "unknown")}`);
      }

      const lockResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.lock",
        contentType: "application/json",
        body: {
          name: normalizedName,
          target_pubkey_hex: normalizedTarget
        }
      });
      const lock = readPeerCallBodyJson<BitfsDomainLockResponse>(lockResp);
      steps.push({ name: "lock", ok: true, detail: String(lock.status || "") });
      if (!lock.signed_quote_json) {
        throw new Error("domain lock did not return signed quote");
      }

      const sign = await bridge.wallet.signBusinessRequest({
        signerPubkeyHex: normalizedResolver,
        signedEnvelope: normalizeSignedEnvelope(lock.signed_quote_json)
      });
      steps.push({ name: "wallet_sign", ok: Boolean(sign.ok), detail: String(sign.code || sign.message || "") });
      if (!sign.ok || String(sign.signed_tx_hex || "").trim() === "") {
        throw new Error(String(sign.message || sign.code || "wallet signing failed"));
      }

      const submitResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.register_submit",
        contentType: "application/json",
        body: {
          register_tx_hex: String(sign.signed_tx_hex || "")
        }
      });
      const submit = readPeerCallBodyJson<BitfsDomainRegisterSubmitResponse>(submitResp);
      steps.push({ name: "register_submit", ok: Boolean(submit.success), detail: String(submit.status || "") });
      if (!submit.success) {
        throw new Error(String(submit.error_message || submit.status || "register submit failed"));
      }

      const listOwnedResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.list_owned",
        contentType: "application/json",
        body: {
          owner_pubkey_hex: walletPubkeyHex
        }
      });
      const listOwned = readPeerCallBodyJson<BitfsDomainListOwnedResponse>(listOwnedResp);
      const foundOwned = listOwned.items.some((item) => normalizeDomainName(item.name) === normalizedName);
      steps.push({ name: "list_owned", ok: foundOwned, detail: foundOwned ? "owned found" : "owned missing" });
      if (!foundOwned) {
        throw new Error("domain missing from list_owned");
      }

      const resolveResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.resolve",
        contentType: "application/json",
        body: {
          name: normalizedName
        }
      });
      const resolveQuery = readPeerCallBodyJson<BitfsDomainResolveResponse>(resolveResp);
      steps.push({ name: "domain_resolve", ok: normalizePubkeyHex(resolveQuery.target_pubkey_hex || "") === normalizedTarget, detail: String(resolveQuery.status || "") });
      if (normalizePubkeyHex(resolveQuery.target_pubkey_hex || "") !== normalizedTarget) {
        throw new Error("resolved target mismatch");
      }

      return finish({
        ok: true,
        wallet_pubkey_hex: walletPubkeyHex,
        resolver_pubkey_hex: normalizedResolver,
        domain_name: normalizedName,
        target_pubkey_hex: normalizedTarget,
        pricing,
        query,
        lock,
        sign,
        submit,
        list_owned: listOwned,
        resolve_query: resolveQuery,
        steps
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const failed = {
        ok: false,
        error: message,
        wallet_pubkey_hex: walletPubkeyHex,
        resolver_pubkey_hex: normalizedResolver,
        domain_name: normalizedName,
        target_pubkey_hex: normalizedTarget,
        steps
      };
      setPageError(message);
      return finish(failed);
    } finally {
      setBusy(false);
    }
  }

  async function handleRunClick() {
    await runFlow({});
  }

  return (
    <main className="page-shell">
      <section className="card hero-card">
        <p className="eyebrow">BitFS Electron E2E</p>
        <h1>Domain 注册薄 UI 测试页</h1>
        <p className="lead">
          这个页面只负责把
          {" "}
          <code>capabilities_show -&gt; pricing -&gt; query -&gt; lock -&gt; wallet sign -&gt; register_submit -&gt; list_owned -&gt; domain_resolve</code>
          {" "}
          这条线真实跑通，不承担产品首页职责。
        </p>
      </section>

      <section className="card form-card">
        <div className="field-grid">
          <label className="field">
            <span>Domain 节点公钥</span>
            <input data-testid="resolver-input" value={resolverPubkeyHex} onChange={(event) => setResolverPubkeyHex(event.target.value)} placeholder="resolver pubkey hex" />
          </label>
          <label className="field">
            <span>Domain 名字</span>
            <input data-testid="domain-name-input" value={domainName} onChange={(event) => setDomainName(event.target.value)} placeholder="example.david" />
          </label>
          <label className="field">
            <span>Target 公钥</span>
            <input data-testid="target-input" value={targetPubkeyHex} onChange={(event) => setTargetPubkeyHex(event.target.value)} placeholder="target pubkey hex" />
          </label>
        </div>
        <div className="toolbar">
          <button data-testid="run-button" type="button" onClick={() => void handleRunClick()} disabled={busy}>
            {busy ? "运行中..." : "运行 domain 注册链路"}
          </button>
        </div>
        {pageError ? <p data-testid="page-error" className="error-text">{pageError}</p> : null}
      </section>

      <section className="card meta-card">
        <h2>当前钱包</h2>
        <p data-testid="wallet-pubkey" className="mono">{walletSummary?.pubkey_hex || clientInfo?.pubkey_hex || "-"}</p>
        <p data-testid="wallet-balance">余额 {formatSat(walletSummary?.balance_satoshi || 0)}</p>
      </section>

      <section className="card result-card">
        <div className="result-head">
          <h2>运行结果</h2>
          <span data-testid="run-status" className={lastResult?.ok ? "status ok" : "status idle"}>
            {lastResult ? (lastResult.ok ? "成功" : "失败") : "未运行"}
          </span>
        </div>
        {lastResult ? (
          <>
            <p data-testid="result-error" className="error-text">{lastResult.error || ""}</p>
            <p data-testid="result-register-txid" className="mono">{lastResult.submit?.register_txid || ""}</p>
            <p data-testid="result-resolved-seed" className="mono">{lastResult.resolve?.seedHash || ""}</p>
            <p data-testid="result-resolved-target" className="mono">{lastResult.resolve?.nodePubkeyHex || ""}</p>
            <div className="step-list" data-testid="step-list">
              {lastResult.steps.map((step) => (
                <article key={step.name} className={`step-item ${step.ok ? "step-ok" : "step-failed"}`}>
                  <strong>{step.name}</strong>
                  <span>{step.ok ? "ok" : "failed"}</span>
                  <p>{step.detail || ""}</p>
                </article>
              ))}
            </div>
            <pre data-testid="result-json" className="json-box">{JSON.stringify(lastResult, null, 2)}</pre>
          </>
        ) : (
          <p className="muted">等待运行。</p>
        )}
      </section>
    </main>
  );
}

async function waitForWalletIdentity(bridge: BitfsBridge, timeoutMs = 30_000): Promise<{
  walletSummary: BitfsWalletSummary;
  clientInfo: BitfsClientInfo;
}> {
  const startedAt = Date.now();
  let lastWalletSummary: BitfsWalletSummary | null = null;
  let lastClientInfo: BitfsClientInfo | null = null;
  let lastError = "";
  while ((Date.now() - startedAt) < timeoutMs) {
    try {
      const [walletSummary, clientInfo] = await Promise.all([
        bridge.wallet.summary(),
        bridge.client.info()
      ]);
      lastWalletSummary = walletSummary;
      lastClientInfo = clientInfo;
      const walletPubkeyHex = normalizePubkeyHex(walletSummary.pubkey_hex || "");
      const clientPubkeyHex = normalizePubkeyHex(clientInfo.pubkey_hex || "");
      if (walletPubkeyHex !== "" && clientPubkeyHex !== "" && Number(walletSummary.balance_satoshi || 0) > 0) {
        return {
          walletSummary,
          clientInfo
        };
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(250);
  }
  if (lastWalletSummary && lastClientInfo) {
    return {
      walletSummary: lastWalletSummary,
      clientInfo: lastClientInfo
    };
  }
  throw new Error(lastError || "wallet identity unavailable");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function normalizeSignedEnvelope(raw: unknown): unknown {
  if (typeof raw !== "string") {
    return raw;
  }
  let value = raw.trim();
  if (value === "") {
    return raw;
  }
  try {
    return JSON.parse(value);
  } catch {
    const decoded = tryDecodeBase64UTF8(value);
    if (decoded === "") {
      return raw;
    }
    value = decoded.trim();
    try {
      return JSON.parse(value);
    } catch {
      return raw;
    }
  }
}

function tryDecodeBase64UTF8(raw: string): string {
  try {
    const binary = window.atob(raw);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    return new TextDecoder().decode(bytes);
  } catch {
    return "";
  }
}

function normalizePubkeyHex(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  return /^[0-9a-f]{66}$/.test(value) ? value : "";
}

function normalizeDomainName(raw: string): string {
  const value = raw.trim().toLowerCase();
  if (!/^[a-z0-9.-]+$/.test(value)) {
    return "";
  }
  if (value.startsWith(".") || value.endsWith(".") || value.includes("..")) {
    return "";
  }
  return value;
}

function readPeerCallBodyJson<T>(response: BitfsPeerCallResponse): T {
  if (!response || response.ok !== true) {
    throw new Error(String(response?.message || response?.code || "peer call failed"));
  }
  if (!("body_json" in response)) {
    throw new Error("peer call body_json missing");
  }
  return response.body_json as T;
}

function formatSat(value: number): string {
  return `${Math.max(0, Number(value || 0))} sat`;
}
