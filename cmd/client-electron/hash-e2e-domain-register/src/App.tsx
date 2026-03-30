import { useEffect, useState } from "react";

const protoContentType = "application/x-protobuf";

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
  const [walletBalance, setWalletBalance] = useState<BitfsWalletBalance | null>(null);
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
        const [nextWalletBalance, nextClientInfo] = await Promise.all([
          window.bitfs.wallet.balance(),
          window.bitfs.client.info()
        ]);
        if (cancelled) {
          return;
        }
        setWalletBalance(nextWalletBalance);
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
    const fallback = normalizePubkeyHex(clientInfo?.pubkey_hex || "");
    if (fallback !== "" && targetPubkeyHex.trim() === "") {
      setTargetPubkeyHex(fallback);
    }
  }, [clientInfo?.pubkey_hex, targetPubkeyHex]);

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
    setWalletBalance(walletSnapshot.walletBalance);
    setClientInfo(walletSnapshot.clientInfo);
    const clientInfoNow = walletSnapshot.clientInfo;
    const normalizedResolver = normalizePubkeyHex(input.resolverPubkeyHex || resolverPubkeyHex);
    const normalizedName = normalizeDomainName(input.domainName || domainName);
    const normalizedTarget = normalizePubkeyHex(input.targetPubkeyHex || targetPubkeyHex || clientInfoNow.pubkey_hex || "");
    const walletPubkeyHex = normalizePubkeyHex(clientInfoNow.pubkey_hex || "");
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
        contentType: protoContentType
      });
      const capabilityBody = readCapabilitiesShowBody(capabilityResp);
      const hasDomain = capabilityBody.capabilities.some((item) => item.id === "domain" && Number(item.version || 0) === 1);
      if (!hasDomain) {
        throw new Error("target node does not expose domain v1 capability");
      }
      steps.push({ name: "capabilities_show", ok: true, detail: "domain.v1 available" });

      const pricingResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.pricing",
        contentType: protoContentType
      });
      const pricing = readDomainPricingBody(pricingResp);
      steps.push({ name: "pricing", ok: true, detail: formatSat(pricing.register_price_satoshi || 0) });

      const queryResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.query",
        contentType: protoContentType,
        body: encodeProtoNameRouteReq(normalizedName)
      });
      const query = readDomainQueryBody(queryResp);
      steps.push({ name: "query", ok: true, detail: String(query.status || "") });
      if (query.available !== true) {
        throw new Error(`domain not available: ${String(query.status || "unknown")}`);
      }

      const lockResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.lock",
        contentType: protoContentType,
        body: encodeProtoNameTargetRouteReq(normalizedName, normalizedTarget)
      });
      const lock = readDomainLockBody(lockResp);
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
        contentType: protoContentType,
        body: encodeProtoRegisterSubmitReq(String(sign.signed_tx_hex || ""))
      });
      const submit = readDomainRegisterSubmitBody(submitResp);
      steps.push({ name: "register_submit", ok: Boolean(submit.success), detail: String(submit.status || "") });
      if (!submit.success) {
        throw new Error(String(submit.error_message || submit.status || "register submit failed"));
      }

      const listOwnedResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.list_owned",
        contentType: protoContentType,
        body: encodeProtoListOwnedReq(walletPubkeyHex)
      });
      const listOwned = readDomainListOwnedBody(listOwnedResp);
      const foundOwned = listOwned.items.some((item) => normalizeDomainName(item.name) === normalizedName);
      steps.push({ name: "list_owned", ok: foundOwned, detail: foundOwned ? "owned found" : "owned missing" });
      if (!foundOwned) {
        throw new Error("domain missing from list_owned");
      }

      const resolveResp = await bridge.peer.call({
        to: normalizedResolver,
        route: "domain.v1.resolve",
        contentType: protoContentType,
        body: encodeProtoNameRouteReq(normalizedName)
      });
      const resolveQuery = readDomainResolveBody(resolveResp);
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
        <p data-testid="wallet-pubkey" className="mono">{clientInfo?.pubkey_hex || "-"}</p>
        <p data-testid="wallet-balance">余额 {formatSat(walletBalance?.balance_satoshi || 0)}</p>
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
  walletBalance: BitfsWalletBalance;
  clientInfo: BitfsClientInfo;
}> {
  const startedAt = Date.now();
  let lastWalletBalance: BitfsWalletBalance | null = null;
  let lastClientInfo: BitfsClientInfo | null = null;
  let lastError = "";
  while ((Date.now() - startedAt) < timeoutMs) {
    try {
      const [walletBalance, clientInfo] = await Promise.all([
        bridge.wallet.balance(),
        bridge.client.info()
      ]);
      lastWalletBalance = walletBalance;
      lastClientInfo = clientInfo;
      const clientPubkeyHex = normalizePubkeyHex(clientInfo.pubkey_hex || "");
      if (clientPubkeyHex !== "" && Number(walletBalance.balance_satoshi || 0) > 0) {
        return {
          walletBalance,
          clientInfo
        };
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(250);
  }
  if (lastWalletBalance && lastClientInfo) {
    return {
      walletBalance: lastWalletBalance,
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

function readCapabilitiesShowBody(response: BitfsPeerCallResponse): BitfsCapabilitiesShowBody {
  return decodeCapabilitiesShowBody(readPeerCallProtoBytes(response, "capabilities_show"));
}

function readDomainPricingBody(response: BitfsPeerCallResponse): BitfsDomainPricing {
  const raw = readPeerCallProtoBytes(response, "domain pricing");
  let offset = 0;
  const out: BitfsDomainPricing = {};
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.resolve_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.query_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.register_lock_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.register_submit_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.set_target_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.register_price_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readDomainQueryBody(response: BitfsPeerCallResponse): BitfsDomainQueryResponse {
  const raw = readPeerCallProtoBytes(response, "domain query");
  let offset = 0;
  const out: BitfsDomainQueryResponse = {
    success: false,
    status: "",
    name: "",
    available: false,
    locked: false,
    registered: false
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.success = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.status = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.name = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.available = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.locked = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.registered = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 7:
        out.owner_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 8:
        out.target_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 9:
        out.expire_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 10:
        out.lock_expires_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 11:
        out.register_price_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 12:
        out.register_submit_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 13:
        out.register_lock_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 14:
        out.set_target_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 15:
        out.resolve_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 16:
        out.query_fee_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 17:
        out.charged_amount_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 18:
        out.updated_txid = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 20:
        out.error_message = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readDomainLockBody(response: BitfsPeerCallResponse): BitfsDomainLockResponse {
  const raw = readPeerCallProtoBytes(response, "domain lock");
  let offset = 0;
  const out: BitfsDomainLockResponse = {
    success: false,
    status: ""
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.success = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.status = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.name = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.target_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.lock_expires_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.charged_amount_satoshi = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 7:
        out.updated_txid = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 8:
        out.signed_quote_json = encodeBase64Bytes(readProtoBytes(raw, () => offset, (next) => { offset = next; }));
        break;
      case 9:
        out.error_message = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readDomainRegisterSubmitBody(response: BitfsPeerCallResponse): BitfsDomainRegisterSubmitResponse {
  const raw = readPeerCallProtoBytes(response, "domain register_submit");
  let offset = 0;
  const out: BitfsDomainRegisterSubmitResponse = {
    success: false,
    status: ""
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.success = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.status = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.name = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.owner_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.target_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.expire_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 7:
        out.register_txid = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 8:
        out.signed_receipt_json = encodeBase64Bytes(readProtoBytes(raw, () => offset, (next) => { offset = next; }));
        break;
      case 9:
        out.error_message = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readDomainListOwnedBody(response: BitfsPeerCallResponse): BitfsDomainListOwnedResponse {
  const raw = readPeerCallProtoBytes(response, "domain list_owned");
  let offset = 0;
  const out: BitfsDomainListOwnedResponse = {
    success: false,
    status: "",
    owner_pubkey_hex: "",
    items: [],
    total: 0
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.success = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.status = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.owner_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.items.push(decodeOwnedDomainItem(readProtoBytes(raw, () => offset, (next) => { offset = next; })));
        break;
      case 5:
        out.total = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.error_message = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 7:
        out.queried_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readDomainResolveBody(response: BitfsPeerCallResponse): BitfsDomainResolveResponse {
  const raw = readPeerCallProtoBytes(response, "domain resolve");
  let offset = 0;
  const out: BitfsDomainResolveResponse = {
    success: false,
    status: ""
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.success = readProtoBool(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.status = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.name = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.owner_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.target_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.expire_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 10:
        out.error = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function decodeOwnedDomainItem(raw: Uint8Array): BitfsOwnedDomainItem {
  let offset = 0;
  const out: BitfsOwnedDomainItem = {
    name: "",
    owner_pubkey_hex: "",
    target_pubkey_hex: "",
    expire_at_unix: 0
  };
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        out.name = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        out.owner_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 3:
        out.target_pubkey_hex = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 4:
        out.expire_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      case 5:
        out.register_txid = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 6:
        out.updated_at_unix = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return out;
}

function readPeerCallProtoBytes(response: BitfsPeerCallResponse, routeName: string): Uint8Array {
  if (!response || response.ok !== true) {
    throw new Error(String(response?.message || response?.code || "peer call failed"));
  }
  const bodyBase64 = String(response.body_base64 || "").trim();
  if (bodyBase64 === "") {
    throw new Error(`${routeName} body_base64 missing`);
  }
  return decodeBase64Bytes(bodyBase64);
}

function decodeCapabilitiesShowBody(raw: Uint8Array): BitfsCapabilitiesShowBody {
  let offset = 0;
  let nodePubkeyHex = "";
  const capabilities: BitfsCapabilityItem[] = [];
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        nodePubkeyHex = readProtoString(raw, () => offset, (next) => { offset = next; }).toLowerCase();
        break;
      case 2:
        capabilities.push(decodeCapabilityItem(readProtoBytes(raw, () => offset, (next) => { offset = next; })));
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return { node_pubkey_hex: nodePubkeyHex, capabilities };
}

function decodeCapabilityItem(raw: Uint8Array): BitfsCapabilityItem {
  let offset = 0;
  let id = "";
  let version = 0;
  while (offset < raw.length) {
    const tag = readProtoVarint(raw, () => offset, (next) => {
      offset = next;
    });
    const fieldNumber = tag >>> 3;
    const wireType = tag & 0x07;
    switch (fieldNumber) {
      case 1:
        id = readProtoString(raw, () => offset, (next) => { offset = next; });
        break;
      case 2:
        version = readProtoVarint(raw, () => offset, (next) => { offset = next; });
        break;
      default:
        offset = skipProtoField(raw, offset, wireType);
        break;
    }
  }
  return { id, version };
}

function decodeBase64Bytes(raw: string): Uint8Array {
  const bin = window.atob(raw);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

function encodeBase64Bytes(raw: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < raw.length; i += 1) {
    binary += String.fromCharCode(raw[i]);
  }
  return window.btoa(binary);
}

function encodeProtoNameRouteReq(name: string): Uint8Array {
  return concatProtoChunks([
    encodeProtoStringField(1, name)
  ]);
}

function encodeProtoNameTargetRouteReq(name: string, targetPubkeyHex: string): Uint8Array {
  return concatProtoChunks([
    encodeProtoStringField(1, name),
    encodeProtoStringField(2, targetPubkeyHex)
  ]);
}

function encodeProtoRegisterSubmitReq(registerTxHex: string): Uint8Array {
  const txBytes = decodeHexBytes(registerTxHex);
  if (txBytes.length === 0) {
    throw new Error("register tx hex missing");
  }
  return concatProtoChunks([
    encodeProtoBytesField(2, txBytes)
  ]);
}

function encodeProtoListOwnedReq(ownerPubkeyHex: string): Uint8Array {
  return concatProtoChunks([
    encodeProtoStringField(1, ownerPubkeyHex)
  ]);
}

function concatProtoChunks(chunks: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const chunk of chunks) {
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function encodeProtoStringField(fieldNumber: number, value: string): Uint8Array {
  return encodeProtoBytesField(fieldNumber, new TextEncoder().encode(value));
}

function encodeProtoBytesField(fieldNumber: number, value: Uint8Array): Uint8Array {
  const tag = encodeProtoVarint((fieldNumber << 3) | 2);
  const size = encodeProtoVarint(value.length);
  return concatProtoChunks([tag, size, value]);
}

function encodeProtoVarint(value: number): Uint8Array {
  const out: number[] = [];
  let next = Math.max(0, Math.floor(Number(value || 0)));
  do {
    let byte = next & 0x7f;
    next = Math.floor(next / 128);
    if (next > 0) {
      byte |= 0x80;
    }
    out.push(byte);
  } while (next > 0);
  return Uint8Array.from(out);
}

function decodeHexBytes(raw: string): Uint8Array {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "") {
    return new Uint8Array();
  }
  if (!/^[0-9a-f]+$/.test(value) || (value.length % 2) !== 0) {
    throw new Error("register tx hex invalid");
  }
  const out = new Uint8Array(value.length / 2);
  for (let i = 0; i < value.length; i += 2) {
    out[i / 2] = Number.parseInt(value.slice(i, i + 2), 16);
  }
  return out;
}

function readProtoString(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): string {
  return new TextDecoder().decode(readProtoBytes(raw, getOffset, setOffset));
}

function readProtoBytes(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): Uint8Array {
  const size = readProtoVarint(raw, getOffset, setOffset);
  const offset = getOffset();
  const next = offset + size;
  if (size < 0 || next > raw.length) {
    throw new Error("protobuf length-delimited field truncated");
  }
  setOffset(next);
  return raw.subarray(offset, next);
}

function readProtoBool(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): boolean {
  return readProtoVarint(raw, getOffset, setOffset) !== 0;
}

function readProtoVarint(raw: Uint8Array, getOffset: () => number, setOffset: (next: number) => void): number {
  let offset = getOffset();
  let result = 0;
  let shift = 0;
  while (offset < raw.length && shift < 35) {
    const value = raw[offset];
    offset += 1;
    result |= (value & 0x7f) << shift;
    if ((value & 0x80) === 0) {
      setOffset(offset);
      return result >>> 0;
    }
    shift += 7;
  }
  throw new Error("protobuf varint truncated");
}

function skipProtoField(raw: Uint8Array, offset: number, wireType: number): number {
  switch (wireType) {
    case 0:
      return skipProtoVarint(raw, offset);
    case 1:
      if (offset + 8 > raw.length) {
        throw new Error("protobuf fixed64 truncated");
      }
      return offset + 8;
    case 2: {
      const nextOffsetRef = { value: offset };
      const size = readProtoVarint(raw, () => nextOffsetRef.value, (next) => {
        nextOffsetRef.value = next;
      });
      const next = nextOffsetRef.value + size;
      if (size < 0 || next > raw.length) {
        throw new Error("protobuf length-delimited skip truncated");
      }
      return next;
    }
    case 5:
      if (offset + 4 > raw.length) {
        throw new Error("protobuf fixed32 truncated");
      }
      return offset + 4;
    default:
      throw new Error(`protobuf wire type unsupported: ${String(wireType)}`);
  }
}

function skipProtoVarint(raw: Uint8Array, offset: number): number {
  let next = offset;
  while (next < raw.length) {
    const value = raw[next];
    next += 1;
    if ((value & 0x80) === 0) {
      return next;
    }
  }
  throw new Error("protobuf varint skip truncated");
}

function formatSat(value: number): string {
  return `${Math.max(0, Number(value || 0))} sat`;
}
