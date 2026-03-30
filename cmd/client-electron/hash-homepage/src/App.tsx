import { FormEvent, startTransition, useEffect, useState } from "react";

import orbitMarkURL from "./assets/orbit-mark.svg";

const historyLimit = 8;
const walletChangedRefreshSteps = new Set([
  "collect_wallet_snapshot",
  "reconcile_wallet_utxo_set",
  "wallet_sync_round_completed"
]);

type PageMode = "loading" | "connected" | "preview" | "error";

export default function App() {
  const [pageMode, setPageMode] = useState<PageMode>(window.bitfs ? "loading" : "preview");
  const [pageError, setPageError] = useState("");
  const [openTarget, setOpenTarget] = useState("");
  const [domainResolverInput, setDomainResolverInput] = useState("");
  const [domainLookupBusy, setDomainLookupBusy] = useState(false);
  const [domainLookupError, setDomainLookupError] = useState("");
  const [domainLookupResolver, setDomainLookupResolver] = useState("");
  const [ownedDomains, setOwnedDomains] = useState<BitfsOwnedDomainItem[]>([]);
  const [domainPricing, setDomainPricing] = useState<BitfsDomainPricing | null>(null);
  const [domainPricingBusy, setDomainPricingBusy] = useState(false);
  const [domainPricingError, setDomainPricingError] = useState("");
  const [domainNameInput, setDomainNameInput] = useState("");
  const [domainTargetInput, setDomainTargetInput] = useState("");
  const [domainQueryBusy, setDomainQueryBusy] = useState(false);
  const [domainQueryError, setDomainQueryError] = useState("");
  const [domainQueryResult, setDomainQueryResult] = useState<BitfsDomainQueryResponse | null>(null);
  const [domainLockBusy, setDomainLockBusy] = useState(false);
  const [domainLockError, setDomainLockError] = useState("");
  const [domainLockResult, setDomainLockResult] = useState<BitfsDomainLockResponse | null>(null);
  const [domainRegisterSignBusy, setDomainRegisterSignBusy] = useState(false);
  const [domainRegisterSignError, setDomainRegisterSignError] = useState("");
  const [domainRegisterSignResult, setDomainRegisterSignResult] = useState<BitfsWalletBusinessSignResponse | null>(null);
  const [domainRegisterSubmitBusy, setDomainRegisterSubmitBusy] = useState(false);
  const [domainRegisterSubmitError, setDomainRegisterSubmitError] = useState("");
  const [domainRegisterSubmitResult, setDomainRegisterSubmitResult] = useState<BitfsDomainRegisterSubmitResponse | null>(null);
  const [clientInfo, setClientInfo] = useState<BitfsClientInfo | null>(null);
  const [clientStatus, setClientStatus] = useState<BitfsClientStatus | null>(null);
  const [walletBalance, setWalletBalance] = useState<BitfsWalletBalance | null>(null);
  const [walletAddresses, setWalletAddresses] = useState<BitfsWalletAddress[]>([]);
  const [walletHistory, setWalletHistory] = useState<BitfsWalletHistoryList | null>(null);
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    let cancelled = false;
    let inFlight = false;
    let unsubscribeEvents = () => undefined;

    async function loadPublicWalletView(isBackgroundRefresh: boolean) {
      const bridge = window.bitfs;
      if (!bridge) {
        startTransition(() => {
          setPageMode("preview");
          setPageError("");
          setClientInfo(null);
          setClientStatus(null);
          setWalletBalance(null);
          setWalletAddresses([]);
          setWalletHistory(null);
        });
        return;
      }

      if (inFlight) {
        return;
      }
      inFlight = true;

      if (!isBackgroundRefresh) {
        startTransition(() => {
          setPageMode((current) => current === "connected" ? current : "loading");
          setPageError("");
        });
      }

      try {
        const [nextClientInfo, nextClientStatus, nextWalletBalance, nextWalletAddresses, nextWalletHistory] = await Promise.all([
          bridge.client.info(),
          bridge.client.getStatus(),
          bridge.wallet.balance(),
          bridge.wallet.addresses(),
          bridge.wallet.history.list({ limit: historyLimit, offset: 0 })
        ]);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setClientInfo(nextClientInfo);
          setClientStatus(nextClientStatus);
          setWalletBalance(nextWalletBalance);
          setWalletAddresses(nextWalletAddresses);
          setWalletHistory(nextWalletHistory);
          setPageMode("connected");
          setPageError("");
        });
      } catch (error) {
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setPageMode((current) => current === "connected" ? current : "error");
          setPageError(error instanceof Error ? error.message : String(error));
        });
      } finally {
        inFlight = false;
      }
    }

    async function refreshPublicWalletProjection() {
      const bridge = window.bitfs;
      if (!bridge || inFlight) {
        return;
      }
      inFlight = true;
      try {
        const [nextWalletBalance, nextWalletHistory] = await Promise.all([
          bridge.wallet.balance(),
          bridge.wallet.history.list({ limit: historyLimit, offset: 0 })
        ]);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setWalletBalance(nextWalletBalance);
          setWalletHistory(nextWalletHistory);
          setPageMode("connected");
          setPageError("");
        });
      } catch (error) {
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setPageMode((current) => current === "connected" ? current : "error");
          setPageError(error instanceof Error ? error.message : String(error));
        });
      } finally {
        inFlight = false;
      }
    }

    async function refreshPublicClientStatus() {
      const bridge = window.bitfs;
      if (!bridge || inFlight) {
        return;
      }
      inFlight = true;
      try {
        const nextClientStatus = await bridge.client.getStatus();
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setClientStatus(nextClientStatus);
          setPageMode("connected");
          setPageError("");
        });
      } catch (error) {
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setPageMode((current) => current === "connected" ? current : "error");
          setPageError(error instanceof Error ? error.message : String(error));
        });
      } finally {
        inFlight = false;
      }
    }

    void loadPublicWalletView(false);
    if (window.bitfs) {
      unsubscribeEvents = window.bitfs.events.subscribe(["wallet.changed", "client.status.changed"], (runtimeEvent) => {
        const topic = String(runtimeEvent?.topic || "");
        if (topic === "wallet.changed" && shouldRefreshPublicWalletProjection(runtimeEvent)) {
          void refreshPublicWalletProjection();
          return;
        }
        if (topic === "client.status.changed") {
          void refreshPublicClientStatus();
        }
      });
    }
    return () => {
      cancelled = true;
      unsubscribeEvents();
    };
  }, [refreshToken]);

  useEffect(() => {
    if (domainTargetInput.trim() !== "") {
      return;
    }
    const nextTarget = normalizePubkeyHex(clientInfo?.pubkey_hex || "");
    if (nextTarget !== "") {
      setDomainTargetInput(nextTarget);
    }
  }, [clientInfo?.pubkey_hex, domainTargetInput]);

  function shouldRefreshPublicWalletProjection(runtimeEvent: BitfsRuntimeEvent | null | undefined) {
    if (String(runtimeEvent?.topic || "") !== "wallet.changed") {
      return true;
    }
    const payload = runtimeEvent?.payload && typeof runtimeEvent.payload === "object" ? runtimeEvent.payload : {};
    const step = String(payload.step || "").trim();
    if (Boolean(payload.has_error)) {
      return true;
    }
    return walletChangedRefreshSteps.has(step);
  }

  function handleOpenTarget(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = normalizeSeedTarget(openTarget);
    if (normalized === "") {
      setPageError("请输入 64 位 seed hash 或 bitfs://<seed_hash>。");
      return;
    }
    if (!window.bitfs) {
      setPageError("当前环境没有注入 window.bitfs，普通浏览器只能预览页面。");
      return;
    }
    setPageError("");
    window.bitfs.navigation.open(normalized);
  }

  function handleRefresh() {
    setRefreshToken((current) => current + 1);
  }

  async function handleSearchOwnedDomains(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainResolverInput);
    const ownerPubkeyHex = normalizePubkeyHex(clientInfo?.pubkey_hex || "");
    if (resolverPubkeyHex === "") {
      setDomainLookupError("请输入有效的 domain 节点公钥 hex。");
      setOwnedDomains([]);
      return;
    }
    if (!bridge) {
      setDomainLookupError("当前环境没有注入 window.bitfs，普通浏览器无法查询 domain。");
      setOwnedDomains([]);
      return;
    }
    if (ownerPubkeyHex === "") {
      setDomainLookupError("当前钱包公钥还未就绪，暂时不能查询名下 domain。");
      setOwnedDomains([]);
      return;
    }
    setDomainLookupBusy(true);
    setDomainLookupError("");
    try {
      const capabilityResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "node.v1.capabilities_show",
        contentType: "application/json",
        body: {}
      });
      const capabilityBody = readPeerCallBodyJson<BitfsCapabilitiesShowBody>(capabilityResp);
      const domainCapability = capabilityBody.capabilities.find((item) => item.id === "domain" && Number(item.version || 0) === 1);
      if (!domainCapability) {
        throw new Error("目标节点没有声明 domain v1 能力。");
      }

      const listResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "domain.v1.list_owned",
        contentType: "application/json",
        body: { limit: 24 }
      });
      const listBody = readPeerCallBodyJson<BitfsDomainListOwnedResponse>(listResp);
      if (!listBody.success) {
        throw new Error(String(listBody.error_message || listBody.status || "domain list_owned failed"));
      }
      startTransition(() => {
        setDomainLookupResolver(resolverPubkeyHex);
        setOwnedDomains(Array.isArray(listBody.items) ? listBody.items : []);
        setDomainLookupError("");
      });
    } catch (error) {
      startTransition(() => {
        setOwnedDomains([]);
        setDomainLookupError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainLookupBusy(false);
    }
  }

  async function handleLoadDomainPricing() {
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    if (!bridge) {
      setDomainPricingError("当前环境没有注入 window.bitfs，普通浏览器无法读取 domain 定价。");
      return;
    }
    if (resolverPubkeyHex === "") {
      setDomainPricingError("请先输入有效的 domain 节点公钥。");
      return;
    }
    setDomainPricingBusy(true);
    setDomainPricingError("");
    try {
      const pricingResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "domain.v1.pricing",
        contentType: "application/json",
        body: {}
      });
      const pricingBody = readPeerCallBodyJson<BitfsDomainPricing>(pricingResp);
      startTransition(() => {
        setDomainPricing(pricingBody);
        setDomainPricingError("");
      });
    } catch (error) {
      startTransition(() => {
        setDomainPricing(null);
        setDomainPricingError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainPricingBusy(false);
    }
  }

  async function handleDomainQuery(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    const normalizedName = normalizeDomainName(domainNameInput);
    if (!bridge) {
      setDomainQueryError("当前环境没有注入 window.bitfs，普通浏览器无法查询 domain。");
      return;
    }
    if (resolverPubkeyHex === "") {
      setDomainQueryError("请先输入有效的 domain 节点公钥。");
      return;
    }
    if (normalizedName === "") {
      setDomainQueryError("请输入有效的 domain 名字。");
      return;
    }
    setDomainQueryBusy(true);
    setDomainQueryError("");
    setDomainLockResult(null);
    setDomainLockError("");
    setDomainRegisterSignResult(null);
    setDomainRegisterSignError("");
    setDomainRegisterSubmitResult(null);
    setDomainRegisterSubmitError("");
    try {
      const queryResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "domain.v1.query",
        contentType: "application/json",
        body: { name: normalizedName }
      });
      const queryBody = readPeerCallBodyJson<BitfsDomainQueryResponse>(queryResp);
      startTransition(() => {
        setDomainQueryResult(queryBody);
        if (queryBody.target_pubkey_hex && domainTargetInput.trim() === "") {
          setDomainTargetInput(queryBody.target_pubkey_hex);
        }
      });
    } catch (error) {
      startTransition(() => {
        setDomainQueryResult(null);
        setDomainQueryError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainQueryBusy(false);
    }
  }

  async function handleDomainLock() {
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    const normalizedName = normalizeDomainName(domainNameInput);
    const targetPubkeyHex = normalizePubkeyHex(domainTargetInput);
    if (!bridge) {
      setDomainLockError("当前环境没有注入 window.bitfs，普通浏览器无法锁定 domain。");
      return;
    }
    if (resolverPubkeyHex === "") {
      setDomainLockError("请先输入有效的 domain 节点公钥。");
      return;
    }
    if (normalizedName === "") {
      setDomainLockError("请输入有效的 domain 名字。");
      return;
    }
    if (targetPubkeyHex === "") {
      setDomainLockError("请输入有效的 target 公钥。");
      return;
    }
    setDomainLockBusy(true);
    setDomainLockError("");
    setDomainRegisterSignResult(null);
    setDomainRegisterSignError("");
    setDomainRegisterSubmitResult(null);
    setDomainRegisterSubmitError("");
    try {
      const lockResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "domain.v1.lock",
        contentType: "application/json",
        body: {
          name: normalizedName,
          target_pubkey_hex: targetPubkeyHex
        }
      });
      const lockBody = readPeerCallBodyJson<BitfsDomainLockResponse>(lockResp);
      startTransition(() => {
        setDomainLockResult(lockBody);
      });
    } catch (error) {
      startTransition(() => {
        setDomainLockResult(null);
        setDomainLockError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainLockBusy(false);
    }
  }

  async function handleWalletSignDomainRegister() {
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    const signedEnvelope = domainLockResult?.signed_quote_json;
    if (!bridge) {
      setDomainRegisterSignError("当前环境没有注入 window.bitfs，普通浏览器无法请求钱包签名。");
      return;
    }
    if (resolverPubkeyHex === "") {
      setDomainRegisterSignError("请先输入有效的 domain 节点公钥。");
      return;
    }
    if (!signedEnvelope) {
      setDomainRegisterSignError("当前没有可用的 domain 签名报价，请先锁定名字。");
      return;
    }
    setDomainRegisterSignBusy(true);
    setDomainRegisterSignError("");
    setDomainRegisterSubmitResult(null);
    setDomainRegisterSubmitError("");
    try {
      const signResp = await bridge.wallet.signBusinessRequest({
        signerPubkeyHex: resolverPubkeyHex,
        signedEnvelope
      });
      startTransition(() => {
        setDomainRegisterSignResult(signResp);
        if (signResp.ok !== true) {
          setDomainRegisterSignError(String(signResp.message || signResp.code || "wallet sign failed"));
        }
      });
    } catch (error) {
      startTransition(() => {
        setDomainRegisterSignResult(null);
        setDomainRegisterSignError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainRegisterSignBusy(false);
    }
  }

  async function handleDomainRegisterSubmit() {
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    const signedTxHex = String(domainRegisterSignResult?.signed_tx_hex || "").trim().toLowerCase();
    if (!bridge) {
      setDomainRegisterSubmitError("当前环境没有注入 window.bitfs，普通浏览器无法提交注册交易。");
      return;
    }
    if (resolverPubkeyHex === "") {
      setDomainRegisterSubmitError("请先输入有效的 domain 节点公钥。");
      return;
    }
    if (signedTxHex === "") {
      setDomainRegisterSubmitError("当前没有可提交的签名交易，请先请求钱包签名。");
      return;
    }
    setDomainRegisterSubmitBusy(true);
    setDomainRegisterSubmitError("");
    try {
      const submitResp = await bridge.peer.call({
        to: resolverPubkeyHex,
        route: "domain.v1.register_submit",
        contentType: "application/json",
        body: {
          register_tx_hex: signedTxHex
        }
      });
      const submitBody = readPeerCallBodyJson<BitfsDomainRegisterSubmitResponse>(submitResp);
      startTransition(() => {
        setDomainRegisterSubmitResult(submitBody);
        if (submitBody.success) {
          setDomainQueryResult((current) => ({
            success: true,
            status: submitBody.status || "registered",
            name: submitBody.name || current?.name || normalizeDomainName(domainNameInput),
            available: false,
            locked: false,
            registered: true,
            owner_pubkey_hex: submitBody.owner_pubkey_hex || current?.owner_pubkey_hex || currentPubkey,
            target_pubkey_hex: submitBody.target_pubkey_hex || current?.target_pubkey_hex || normalizePubkeyHex(domainTargetInput),
            expire_at_unix: submitBody.expire_at_unix || current?.expire_at_unix,
            lock_expires_at_unix: undefined,
            register_price_satoshi: current?.register_price_satoshi,
            register_submit_fee_satoshi: current?.register_submit_fee_satoshi,
            register_lock_fee_satoshi: current?.register_lock_fee_satoshi,
            set_target_fee_satoshi: current?.set_target_fee_satoshi,
            resolve_fee_satoshi: current?.resolve_fee_satoshi,
            query_fee_satoshi: current?.query_fee_satoshi,
            charged_amount_satoshi: current?.charged_amount_satoshi,
            updated_txid: submitBody.register_txid || current?.updated_txid,
            error_message: submitBody.error_message
          }));
        } else {
          setDomainRegisterSubmitError(String(submitBody.error_message || submitBody.status || "domain register submit failed"));
        }
      });
    } catch (error) {
      startTransition(() => {
        setDomainRegisterSubmitResult(null);
        setDomainRegisterSubmitError(error instanceof Error ? error.message : String(error));
      });
    } finally {
      setDomainRegisterSubmitBusy(false);
    }
  }

  async function handleOpenDomainHome(item: BitfsOwnedDomainItem) {
    const bridge = window.bitfs;
    const resolverPubkeyHex = normalizePubkeyHex(domainLookupResolver || domainResolverInput);
    if (!bridge || resolverPubkeyHex === "") {
      setDomainLookupError("domain 节点公钥还没有准备好。");
      return;
    }
    try {
      setDomainLookupError("");
      const resolved = await bridge.locator.resolve(`${resolverPubkeyHex}:${item.name}`);
      bridge.navigation.open(resolved.seedHash);
    } catch (error) {
      setDomainLookupError(error instanceof Error ? error.message : String(error));
    }
  }

  const currentAddress = walletAddresses[0]?.address || "";
  const currentPubkey = clientInfo?.pubkey_hex || "";
  const walletReady = clientStatus?.wallet_ready ? "已就绪" : "未就绪";
  const walletUnlocked = clientStatus?.wallet_unlocked ? "已解锁" : "未解锁";

  return (
    <div className="page-shell">
      <header className="hero">
        <div className="hero-topline">
          <div className="brand-row">
            <img className="brand-mark" src={orbitMarkURL} alt="BitFS" />
            <div>
              <span className="brand-pill">BitFS Public Wallet API</span>
              <p className="brand-caption">官方示范页只展示 `window.bitfs` 的公开钱包读取能力</p>
            </div>
          </div>
          <button type="button" className="ghost-button" onClick={handleRefresh}>刷新公开视图</button>
        </div>

        <h1>本机钱包公开视图</h1>
        <p className="hero-copy">
          这个首页不是壳层私有面板，而是 `window.bitfs` 对外协议的官方示范。外部 bitfs 页面可以读取余额、地址和历史，
          但本机内部诊断、定价、数据规模与本地路径不会出现在这里。当前也额外示范了通过 `peer.call`
          查询某个 domain 节点上“我名下的域名”。
        </p>

        <div className={`status-strip status-${pageMode}`}>
          <strong>{modeTitle(pageMode)}</strong>
          <span>{modeDescription(pageMode, pageError)}</span>
        </div>

        <form className="search-panel" onSubmit={handleOpenTarget}>
          <label className="sr-only" htmlFor="hash-input">输入资源 hash</label>
          <input
            id="hash-input"
            className="hash-input"
            value={openTarget}
            onChange={(event) => setOpenTarget(event.target.value)}
            placeholder="输入 64 位资源 hash，或 bitfs://<seed_hash>"
          />
          <button type="submit" className="search-button">打开资源</button>
        </form>
        {pageError && pageMode !== "error" ? <p className="inline-error">{pageError}</p> : null}
      </header>

      <main className="content-grid">
        <section className="summary-panel">
          <div className="section-head">
            <span>公开画像</span>
            <strong>钱包总览</strong>
          </div>

          <div className="summary-grid">
            <article className="metric-card">
              <span className="metric-label">链上余额</span>
              <strong className="metric-value" data-testid="homepage-balance">
                {walletBalance ? formatSat(walletBalance.balance_satoshi) : "-"}
              </strong>
              <p className="metric-note">这里只显示公开余额，不包含任何内部统计。</p>
            </article>

            <article className="metric-card">
              <span className="metric-label">默认地址</span>
              <strong className="metric-value mono" title={currentAddress || "-"}>{shortToken(currentAddress)}</strong>
              <p className="metric-note">网页应用拿到地址后，只是更方便读取本来就公开的链上信息。</p>
            </article>

            <article className="metric-card">
              <span className="metric-label">钱包公钥</span>
              <strong className="metric-value mono" title={currentPubkey || "-"}>{shortToken(currentPubkey)}</strong>
              <p className="metric-note">系统里的节点身份统一使用公钥 hex。</p>
            </article>

            <article className="metric-card">
              <span className="metric-label">浏览器授权状态</span>
              <strong className="metric-value">{walletUnlocked}</strong>
              <p className="metric-note">当前钱包 {walletReady}，未来涉及签名与花费时会走单独确认框。</p>
            </article>
          </div>

          <section className="address-panel">
            <div className="subsection-head">
              <span>地址</span>
              <strong>对外公开的接收地址</strong>
            </div>
            {walletAddresses.length === 0 ? (
              <div className="empty-card">当前没有可展示的地址。</div>
            ) : (
              <div className="address-list">
                {walletAddresses.map((item) => (
                  <article key={item.address} className="address-card">
                    <div className="address-label-row">
                      <span className="address-purpose">默认收款地址</span>
                      <span className="address-encoding">{item.encoding}</span>
                    </div>
                    <strong className="address-value mono" title={item.address}>{item.address}</strong>
                    <p className="address-note mono" title={item.pubkey_hex}>{shortToken(item.pubkey_hex)}</p>
                  </article>
                ))}
              </div>
            )}
          </section>

          <section className="domain-panel">
            <div className="subsection-head">
              <span>Domain</span>
              <strong>我名下的域名</strong>
            </div>
            <p className="history-intro">
              输入 domain 节点公钥后，页面会通过 `node.v1.capabilities_show` 与 `domain.v1.list_owned`
              查询当前钱包身份名下的有效域名，并可直接跳到该 domain 的 `index` 首页。
            </p>
            <form className="search-panel domain-search-panel" onSubmit={handleSearchOwnedDomains}>
              <label className="sr-only" htmlFor="domain-resolver-input">输入 domain 节点公钥</label>
              <input
                id="domain-resolver-input"
                className="hash-input"
                value={domainResolverInput}
                onChange={(event) => setDomainResolverInput(event.target.value)}
                placeholder="输入 domain 节点公钥 hex"
              />
              <button type="submit" className="search-button" disabled={domainLookupBusy}>
                {domainLookupBusy ? "查询中..." : "查询我名下域名"}
              </button>
            </form>
            {domainLookupError ? <p className="inline-error">{domainLookupError}</p> : null}
            {ownedDomains.length === 0 ? (
              <div className="empty-card">
                {domainLookupResolver === "" ? "输入 domain 节点公钥后，就可以查询当前钱包名下的域名。" : "当前没有查到有效域名。"}
              </div>
            ) : (
              <div className="domain-list">
                {ownedDomains.map((item) => (
                  <article key={item.name} className="domain-card">
                    <div className="domain-card-head">
                      <strong className="domain-name">{item.name}</strong>
                      <button type="button" className="ghost-button domain-open-button" onClick={() => void handleOpenDomainHome(item)}>
                        打开首页
                      </button>
                    </div>
                    <p className="address-note mono" title={item.target_pubkey_hex}>{shortToken(item.target_pubkey_hex)}</p>
                    <p className="history-meta">到期时间 {formatUnixTime(item.expire_at_unix)}</p>
                  </article>
                ))}
              </div>
            )}

            <section className="domain-register-panel">
              <div className="subsection-head">
                <span>Register</span>
                <strong>查询、锁定与提交注册</strong>
              </div>
              <p className="history-intro">
                现在这块会先走 pricing、query、lock，然后把 domain 返回的签名业务原文交给钱包解释并弹框确认。
                钱包只返回已签名交易，不替页面广播；最后仍然由页面通过 `peer.call` 提交给 `domain.v1.register_submit`。
              </p>
              <div className="domain-pricing-bar">
                <button type="button" className="ghost-button domain-pricing-button" onClick={() => void handleLoadDomainPricing()} disabled={domainPricingBusy}>
                  {domainPricingBusy ? "读取定价中..." : "读取 domain 定价"}
                </button>
                {domainPricing ? (
                  <div className="domain-pricing-grid">
                    <span>查询费 {formatSat(domainPricing.query_fee_satoshi || 0)}</span>
                    <span>锁定费 {formatSat(domainPricing.register_lock_fee_satoshi || 0)}</span>
                    <span>注册价 {formatSat(domainPricing.register_price_satoshi || 0)}</span>
                    <span>提交费 {formatSat(domainPricing.register_submit_fee_satoshi || 0)}</span>
                  </div>
                ) : null}
              </div>
              {domainPricingError ? <p className="inline-error">{domainPricingError}</p> : null}

              <form className="domain-form" onSubmit={handleDomainQuery}>
                <label className="domain-field">
                  <span>Domain 名字</span>
                  <input
                    className="hash-input"
                    value={domainNameInput}
                    onChange={(event) => setDomainNameInput(event.target.value)}
                    placeholder="例如 alice.david"
                  />
                </label>
                <label className="domain-field">
                  <span>Target 公钥</span>
                  <input
                    className="hash-input mono"
                    value={domainTargetInput}
                    onChange={(event) => setDomainTargetInput(event.target.value)}
                    placeholder="默认使用当前钱包公钥"
                  />
                </label>
                <div className="domain-actions">
                  <button type="submit" className="search-button" disabled={domainQueryBusy}>
                    {domainQueryBusy ? "查询中..." : "查询 domain 状态"}
                  </button>
                  <button
                    type="button"
                    className="ghost-button domain-lock-button"
                    onClick={() => void handleDomainLock()}
                    disabled={domainLockBusy || !domainQueryResult || domainQueryResult.available !== true}
                  >
                    {domainLockBusy ? "锁定中..." : "锁定名字"}
                  </button>
                </div>
              </form>
              {domainQueryError ? <p className="inline-error">{domainQueryError}</p> : null}
              {domainLockError ? <p className="inline-error">{domainLockError}</p> : null}
              {domainRegisterSignError ? <p className="inline-error">{domainRegisterSignError}</p> : null}
              {domainRegisterSubmitError ? <p className="inline-error">{domainRegisterSubmitError}</p> : null}

              {domainQueryResult ? (
                <article className="domain-result-card">
                  <div className="domain-result-head">
                    <strong>{domainQueryResult.name || "-"}</strong>
                    <span className={`domain-status status-${normalizeDomainStatus(domainQueryResult.status)}`}>{domainStatusLabel(domainQueryResult.status)}</span>
                  </div>
                  <p className="history-meta">可注册 {domainQueryResult.available ? "是" : "否"} | 已注册 {domainQueryResult.registered ? "是" : "否"} | 已锁定 {domainQueryResult.locked ? "是" : "否"}</p>
                  <p className="history-meta">target {shortToken(domainQueryResult.target_pubkey_hex || "-")}</p>
                  <p className="history-meta">注册价 {formatSat(domainQueryResult.register_price_satoshi || 0)} | 锁定费 {formatSat(domainQueryResult.register_lock_fee_satoshi || 0)}</p>
                  {domainQueryResult.expire_at_unix ? <p className="history-meta">注册到期 {formatUnixTime(domainQueryResult.expire_at_unix)}</p> : null}
                  {domainQueryResult.lock_expires_at_unix ? <p className="history-meta">锁到期 {formatUnixTime(domainQueryResult.lock_expires_at_unix)}</p> : null}
                </article>
              ) : null}

              {domainLockResult ? (
                <article className="domain-result-card">
                  <div className="domain-result-head">
                    <strong>{domainLockResult.name || domainNameInput || "-"}</strong>
                    <span className={`domain-status status-${normalizeDomainStatus(domainLockResult.status || "")}`}>{domainStatusLabel(domainLockResult.status || "")}</span>
                  </div>
                  <p className="history-meta">target {shortToken(domainLockResult.target_pubkey_hex || domainTargetInput || "-")}</p>
                  {domainLockResult.lock_expires_at_unix ? <p className="history-meta">锁到期 {formatUnixTime(domainLockResult.lock_expires_at_unix)}</p> : null}
                  {domainLockResult.updated_txid ? <p className="history-meta mono" title={domainLockResult.updated_txid}>扣费更新 {shortToken(domainLockResult.updated_txid)}</p> : null}
                  <div className="domain-actions register-actions">
                    <button
                      type="button"
                      className="search-button"
                      onClick={() => void handleWalletSignDomainRegister()}
                      disabled={domainRegisterSignBusy || !domainLockResult.signed_quote_json}
                    >
                      {domainRegisterSignBusy ? "等待钱包确认..." : "让钱包解释并签名"}
                    </button>
                    <button
                      type="button"
                      className="ghost-button domain-submit-button"
                      onClick={() => void handleDomainRegisterSubmit()}
                      disabled={domainRegisterSubmitBusy || String(domainRegisterSignResult?.signed_tx_hex || "").trim() === ""}
                    >
                      {domainRegisterSubmitBusy ? "提交中..." : "提交给 domain 注册"}
                    </button>
                  </div>
                </article>
              ) : null}

              {domainRegisterSignResult?.preview ? (
                <article className={`domain-result-card wallet-preview-card warning-${domainRegisterSignResult.preview.warning_level || "normal"}`}>
                  <div className="domain-result-head">
                    <strong>{domainRegisterSignResult.preview.business_title || "钱包解释结果"}</strong>
                    <span className={`domain-status status-${domainRegisterSignResult.ok ? "ok" : "bad_request"}`}>
                      {domainRegisterSignResult.ok ? "已签名" : "未签名"}
                    </span>
                  </div>
                  <p className="history-meta">{domainRegisterSignResult.preview.summary}</p>
                  {Array.isArray(domainRegisterSignResult.preview.detail_lines) ? (
                    <div className="wallet-preview-lines">
                      {domainRegisterSignResult.preview.detail_lines.map((line, index) => (
                        <p key={`${line}-${index}`} className="history-meta">{line}</p>
                      ))}
                    </div>
                  ) : null}
                  {domainRegisterSignResult.preview.txid ? (
                    <p className="history-meta mono" title={domainRegisterSignResult.preview.txid}>
                      待提交交易 {shortToken(domainRegisterSignResult.preview.txid)}
                    </p>
                  ) : null}
                </article>
              ) : null}

              {domainRegisterSubmitResult ? (
                <article className="domain-result-card">
                  <div className="domain-result-head">
                    <strong>{domainRegisterSubmitResult.name || domainNameInput || "-"}</strong>
                    <span className={`domain-status status-${normalizeDomainStatus(domainRegisterSubmitResult.status || "")}`}>
                      {domainStatusLabel(domainRegisterSubmitResult.status || "")}
                    </span>
                  </div>
                  <p className="history-meta">owner {shortToken(domainRegisterSubmitResult.owner_pubkey_hex || currentPubkey || "-")}</p>
                  <p className="history-meta">target {shortToken(domainRegisterSubmitResult.target_pubkey_hex || domainTargetInput || "-")}</p>
                  {domainRegisterSubmitResult.expire_at_unix ? <p className="history-meta">注册到期 {formatUnixTime(domainRegisterSubmitResult.expire_at_unix)}</p> : null}
                  {domainRegisterSubmitResult.register_txid ? (
                    <p className="history-meta mono" title={domainRegisterSubmitResult.register_txid}>
                      注册交易 {shortToken(domainRegisterSubmitResult.register_txid)}
                    </p>
                  ) : null}
                </article>
              ) : null}
            </section>
          </section>
        </section>

        <section className="history-panel">
          <div className="section-head">
            <span>公开历史</span>
            <strong>最近链上出入账</strong>
          </div>
          <p className="history-intro">
            这里展示的是对外公开的钱包历史视图，只保留 `txid`、方向、金额、确认状态和时间，不带内部账务注释。
          </p>

          {walletHistory && walletHistory.items.length > 0 ? (
            <div className="history-list">
              {walletHistory.items.map((item) => (
                <article key={item.id} className="history-row">
                  <div className="history-main">
                    <div className="history-row-head">
                      <span className={`history-direction direction-${item.direction}`}>{historyDirectionLabel(item.direction)}</span>
                      <strong>{formatHistoryAmount(item.direction, item.amount_satoshi)}</strong>
                    </div>
                    <p className="history-meta mono" title={item.txid}>{shortToken(item.txid)}</p>
                    <p className="history-meta">
                      {formatUnixTime(item.occurred_at_unix)} | {historyStatusLabel(item.status)} | 区块高度 {item.block_height || "-"}
                    </p>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="empty-card">
              {pageMode === "preview" ? "普通浏览器预览模式不会注入真实钱包历史。" : "当前没有公开历史记录。"}
            </div>
          )}

          <section className="rules-panel">
            <div className="subsection-head">
              <span>边界</span>
              <strong>公开读取与授权写入</strong>
            </div>
            <div className="rule-list">
              <article className="rule-card">
                <h3>公开读取</h3>
                <p>余额、地址、历史属于公开画像层，网页应用可以直接读取，用来更好地为用户服务。</p>
              </article>
              <article className="rule-card">
                <h3>隐私留在本机</h3>
                <p>本机路径、内部诊断、定价、数据规模与统计不会从 `window.bitfs` 公开桥里泄露出去。</p>
              </article>
              <article className="rule-card">
                <h3>花费再确认</h3>
                <p>未来涉及签名、广播与真实花费时，浏览器会额外弹出确认框，不会静默动用链上资金。</p>
              </article>
            </div>
          </section>
        </section>
      </main>
    </div>
  );
}

function modeTitle(mode: PageMode): string {
  if (mode === "connected") {
    return "已连接本机 BitFS";
  }
  if (mode === "preview") {
    return "普通浏览器预览";
  }
  if (mode === "error") {
    return "公开接口读取失败";
  }
  return "正在读取公开接口";
}

function modeDescription(mode: PageMode, message: string): string {
  if (mode === "connected") {
    return "当前页面正在消费 window.bitfs 的公开钱包协议。";
  }
  if (mode === "preview") {
    return "这里还能在普通浏览器里预览布局，但不会出现真实钱包数据。";
  }
  if (mode === "error") {
    return message || "公开钱包接口返回了错误。";
  }
  return "正在从本机钱包读取公开信息。";
}

function normalizeSeedTarget(raw: string): string {
  const value = raw.trim().toLowerCase();
  if (/^[0-9a-f]{64}$/.test(value)) {
    return value;
  }
  if (/^bitfs:\/\/[0-9a-f]{64}$/.test(value)) {
    return value;
  }
  return "";
}

function normalizePubkeyHex(raw: string): string {
  const value = raw.trim().toLowerCase();
  if (!/^(02|03)[0-9a-f]{64}$/.test(value)) {
    return "";
  }
  return value;
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

function shortToken(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length <= 18) {
    return trimmed || "-";
  }
  return `${trimmed.slice(0, 8)}...${trimmed.slice(-8)}`;
}

function domainStatusLabel(status: string): string {
  const normalized = normalizeDomainStatus(status);
  if (normalized === "available") {
    return "可注册";
  }
  if (normalized === "registered") {
    return "已注册";
  }
  if (normalized === "locked") {
    return "已锁定";
  }
  if (normalized === "expired") {
    return "已过期";
  }
  if (normalized === "ok") {
    return "成功";
  }
  if (normalized === "bad_request") {
    return "请求错误";
  }
  return normalized || "未知状态";
}

function normalizeDomainStatus(status: string): string {
  return status.trim().toLowerCase();
}

function formatSat(value: number): string {
  return `${new Intl.NumberFormat("zh-CN").format(Math.max(0, Math.trunc(value || 0)))} sat`;
}

function formatUnixTime(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "-";
  }
  return new Date(value * 1000).toLocaleString("zh-CN", { hour12: false });
}

function historyDirectionLabel(direction: BitfsWalletHistoryDirection): string {
  if (direction === "in") {
    return "流入";
  }
  if (direction === "out") {
    return "流出";
  }
  return "未知";
}

function formatHistoryAmount(direction: BitfsWalletHistoryDirection, amountSatoshi: number): string {
  const prefix = direction === "in" ? "+" : direction === "out" ? "-" : "";
  return `${prefix}${formatSat(amountSatoshi)}`;
}

function historyStatusLabel(status: string): string {
  const normalized = status.trim().toLowerCase();
  if (normalized === "confirmed") {
    return "已确认";
  }
  if (normalized === "pending") {
    return "待确认";
  }
  if (normalized === "failed") {
    return "失败";
  }
  return normalized || "未知状态";
}
