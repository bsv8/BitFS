import { useEffect, useState } from "react";

type RunStep = {
  name: string;
  ok: boolean;
  detail?: string;
};

type DomainLocatorResolveRunResult = {
  ok: boolean;
  error?: string;
  resolver_pubkey_hex?: string;
  domain_name?: string;
  expected_target_pubkey_hex?: string;
  expected_seed_hash?: string;
  resolve?: BitfsLocatorResolveResult | null;
  steps: RunStep[];
};

type RunInput = {
  resolverPubkeyHex?: string;
  domainName?: string;
  expectedTargetPubkeyHex?: string;
  expectedSeedHash?: string;
};

declare global {
  interface Window {
    __bitfsE2EDomainLocatorResolve?: {
      run: (input?: RunInput) => Promise<DomainLocatorResolveRunResult>;
      getState: () => DomainLocatorResolveRunResult | null;
    };
  }
}

export default function App() {
  const [walletSummary, setWalletSummary] = useState<BitfsWalletSummary | null>(null);
  const [clientInfo, setClientInfo] = useState<BitfsClientInfo | null>(null);
  const [resolverPubkeyHex, setResolverPubkeyHex] = useState("");
  const [domainName, setDomainName] = useState("");
  const [expectedTargetPubkeyHex, setExpectedTargetPubkeyHex] = useState("");
  const [expectedSeedHash, setExpectedSeedHash] = useState("");
  const [busy, setBusy] = useState(false);
  const [lastResult, setLastResult] = useState<DomainLocatorResolveRunResult | null>(null);
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
    window.__bitfsE2EDomainLocatorResolve = {
      run: (input?: RunInput) => runFlow(input || {}),
      getState: () => lastResult
    };
    return () => {
      delete window.__bitfsE2EDomainLocatorResolve;
    };
  });

  async function runFlow(input: RunInput): Promise<DomainLocatorResolveRunResult> {
    const bridge = window.bitfs;
    const steps: RunStep[] = [];
    const finish = (result: DomainLocatorResolveRunResult) => {
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

    const normalizedResolver = normalizePubkeyHex(input.resolverPubkeyHex || resolverPubkeyHex);
    const normalizedName = normalizeDomainName(input.domainName || domainName);
    const normalizedExpectedTarget = normalizePubkeyHex(input.expectedTargetPubkeyHex || expectedTargetPubkeyHex);
    const normalizedExpectedSeed = normalizeSeedHash(input.expectedSeedHash || expectedSeedHash);
    if (normalizedResolver === "") {
      return finish({ ok: false, error: "resolver pubkey invalid", steps });
    }
    if (normalizedName === "") {
      return finish({ ok: false, error: "domain name invalid", steps });
    }
    if (normalizedExpectedTarget === "") {
      return finish({ ok: false, error: "expected target pubkey invalid", steps });
    }
    if (normalizedExpectedSeed === "") {
      return finish({ ok: false, error: "expected seed hash invalid", steps });
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

      const locator = `${normalizedResolver}:${normalizedName}`;
      const resolved = await bridge.locator.resolve(locator);
      const resolvedTarget = normalizePubkeyHex(resolved.nodePubkeyHex || "");
      const resolvedSeed = normalizeSeedHash(resolved.seedHash || "");
      const targetOK = resolvedTarget === normalizedExpectedTarget;
      const seedOK = resolvedSeed === normalizedExpectedSeed;
      steps.push({
        name: "locator_resolve",
        ok: targetOK && seedOK,
        detail: `${resolvedTarget} ${resolvedSeed}`
      });
      if (!targetOK) {
        throw new Error("resolved target mismatch");
      }
      if (!seedOK) {
        throw new Error("resolved seed mismatch");
      }

      return finish({
        ok: true,
        resolver_pubkey_hex: normalizedResolver,
        domain_name: normalizedName,
        expected_target_pubkey_hex: normalizedExpectedTarget,
        expected_seed_hash: normalizedExpectedSeed,
        resolve: resolved,
        steps
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const failed = {
        ok: false,
        error: message,
        resolver_pubkey_hex: normalizedResolver,
        domain_name: normalizedName,
        expected_target_pubkey_hex: normalizedExpectedTarget,
        expected_seed_hash: normalizedExpectedSeed,
        steps
      };
      setPageError(message);
      return finish(failed);
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="page-shell">
      <section className="panel hero-panel">
        <div>
          <p className="eyebrow">BitFS Electron E2E</p>
          <h1>Domain Locator Resolve</h1>
          <p className="hero-copy">
            这页只回归一条壳解析链路，不覆盖注册页流程。
          </p>
          <code>capabilities_show -&gt; locator.resolve</code>
        </div>
      </section>

      <section className="panel input-panel">
        <label>
          <span>Resolver 公钥</span>
          <input value={resolverPubkeyHex} onChange={(event) => setResolverPubkeyHex(event.target.value)} spellCheck={false} />
        </label>
        <label>
          <span>Domain 名字</span>
          <input value={domainName} onChange={(event) => setDomainName(event.target.value)} spellCheck={false} />
        </label>
        <label>
          <span>期望目标公钥</span>
          <input value={expectedTargetPubkeyHex} onChange={(event) => setExpectedTargetPubkeyHex(event.target.value)} spellCheck={false} />
        </label>
        <label>
          <span>期望 Seed Hash</span>
          <input value={expectedSeedHash} onChange={(event) => setExpectedSeedHash(event.target.value)} spellCheck={false} />
        </label>
        <div className="actions">
          <button type="button" disabled={busy} onClick={() => void runFlow({})}>
            {busy ? "解析中..." : "运行 locator.resolve"}
          </button>
        </div>
      </section>

      <section className="panel status-panel">
        <h2>钱包快照</h2>
        <pre>{JSON.stringify({ walletSummary, clientInfo }, null, 2)}</pre>
      </section>

      <section className="panel result-panel">
        <h2>执行结果</h2>
        {pageError ? <p className="error-text">{pageError}</p> : null}
        <pre>{JSON.stringify(lastResult, null, 2)}</pre>
      </section>
    </main>
  );
}

async function waitForWalletIdentity(bridge: BitfsBridge): Promise<{ walletSummary: BitfsWalletSummary; clientInfo: BitfsClientInfo }> {
  let lastError = "";
  for (let i = 0; i < 40; i += 1) {
    try {
      const [walletSummary, clientInfo] = await Promise.all([
        bridge.wallet.summary(),
        bridge.client.info()
      ]);
      const walletPubkeyHex = normalizePubkeyHex(walletSummary.pubkey_hex || clientInfo.pubkey_hex || "");
      if (walletPubkeyHex !== "" && Number(walletSummary.balance_satoshi || 0) > 0) {
        return { walletSummary, clientInfo };
      }
      lastError = "wallet summary not ready";
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(250);
  }
  throw new Error(lastError || "wallet summary unavailable");
}

function readPeerCallBodyJson<T>(resp: BitfsPeerCallResponse): T {
  if (resp.body_json && typeof resp.body_json === "object") {
    return resp.body_json as T;
  }
  throw new Error(String(resp.message || resp.code || "peer call body_json missing"));
}

function normalizeDomainName(raw: string): string {
  return String(raw || "").trim().toLowerCase();
}

function normalizePubkeyHex(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  return /^(02|03)[0-9a-f]{64}$/.test(value) ? value : "";
}

function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  return /^[0-9a-f]{64}$/.test(value) ? value : "";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}
