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
  const [clientInfo, setClientInfo] = useState<BitfsClientInfo | null>(null);
  const [clientStatus, setClientStatus] = useState<BitfsClientStatus | null>(null);
  const [walletSummary, setWalletSummary] = useState<BitfsWalletSummary | null>(null);
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
          setWalletSummary(null);
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
        const [nextClientInfo, nextClientStatus, nextWalletSummary, nextWalletAddresses, nextWalletHistory] = await Promise.all([
          bridge.client.info(),
          bridge.client.getStatus(),
          bridge.wallet.summary(),
          bridge.wallet.addresses(),
          bridge.wallet.history.list({ limit: historyLimit, offset: 0 })
        ]);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setClientInfo(nextClientInfo);
          setClientStatus(nextClientStatus);
          setWalletSummary(nextWalletSummary);
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
        const [nextWalletSummary, nextWalletHistory] = await Promise.all([
          bridge.wallet.summary(),
          bridge.wallet.history.list({ limit: historyLimit, offset: 0 })
        ]);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setWalletSummary(nextWalletSummary);
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

  const currentAddress = walletAddresses[0]?.address || walletSummary?.wallet_address || "";
  const currentPubkey = walletSummary?.pubkey_hex || clientInfo?.pubkey_hex || "";
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
          但本机内部诊断、定价、数据规模与本地路径不会出现在这里。
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
                {walletSummary ? formatSat(walletSummary.balance_satoshi) : "-"}
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

function shortToken(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length <= 18) {
    return trimmed || "-";
  }
  return `${trimmed.slice(0, 8)}...${trimmed.slice(-8)}`;
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
