import { startTransition, useEffect, useState } from "react";

import type { ShellState } from "./bitfs";
import {
  addWorkspace,
  clearUserHomepage,
  createStaticDir,
  deleteArbiter,
  deleteGateway,
  deleteStaticEntry,
  deleteWorkspace,
  exportKeyFile,
  getArbiters,
  getGateways,
  getShellState,
  getStaticTree,
  getWalletSummary,
  getWorkspaces,
  lockClient,
  pickWorkspaceDirectory,
  restartBackend,
  saveArbiter,
  saveGateway,
  setStaticItemPrice,
  setUserHomepage,
  updateWorkspace,
  uploadStaticFile
} from "./api";
import { ArbiterManager } from "./components/ArbiterManager";
import { GatewayManager } from "./components/GatewayManager";
import { StaticFileManager } from "./components/StaticFileManager";
import { WorkspacesManager } from "./components/WorkspacesManager";
import type { ArbitersResp, GatewaysResp, StaticItem, StaticTreeResp, WalletSummary, WorkspacesResp } from "./types";
import { formatSat, normalizeSeedHash, shortHex } from "./utils";

type NavSection = "security" | "network" | "storage";
const walletRefreshTopics = ["backend.phase.changed", "wallet.sync.changed", "wallet.changed", "client.status.changed"];

export default function App() {
  const [activeSection, setActiveSection] = useState<NavSection>("security");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [shellState, setShellState] = useState<ShellState | null>(null);
  const [walletSummary, setWalletSummary] = useState<WalletSummary | null>(null);
  const [gateways, setGateways] = useState<GatewaysResp | null>(null);
  const [arbiters, setArbiters] = useState<ArbitersResp | null>(null);
  const [workspaces, setWorkspaces] = useState<WorkspacesResp | null>(null);
  const [staticTree, setStaticTree] = useState<StaticTreeResp | null>(null);
  const [staticPath, setStaticPath] = useState("/");
  const [homepageDraft, setHomepageDraft] = useState("");

  async function loadAll(nextStaticPath = staticPath) {
    setLoading(true);
    setError("");
    try {
      const [
        nextShellState,
        nextWalletSummary,
        nextGateways,
        nextArbiters,
        nextWorkspaces,
        nextStaticTree
      ] = await Promise.all([
        getShellState(),
        getWalletSummary(),
        getGateways(),
        getArbiters(),
        getWorkspaces(),
        getStaticTree(nextStaticPath)
      ]);
      startTransition(() => {
        setShellState(nextShellState);
        setWalletSummary(nextWalletSummary);
        setGateways(nextGateways);
        setArbiters(nextArbiters);
        setWorkspaces(nextWorkspaces);
        setStaticTree(nextStaticTree);
        setStaticPath(nextStaticTree.current_path || nextStaticPath);
        setHomepageDraft(nextShellState.userHomeSeedHash || nextShellState.backend.defaultHomeSeedHash || "");
      });
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadAll("/");
    const unsubscribeState = window.bitfsSettings.onShellState((state) => {
      setShellState(state);
    });
    const unsubscribeEvents = window.bitfsSettings.events.subscribe(walletRefreshTopics, () => {
      void getWalletSummary().then((summary) => {
        setWalletSummary(summary);
      }).catch(() => undefined);
    });
    return () => {
      unsubscribeEvents();
      unsubscribeState();
    };
  }, []);

  async function runBusyTask(task: () => Promise<void>) {
    setBusy(true);
    setError("");
    try {
      await task();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setBusy(false);
    }
  }

  const homeSource = shellState?.userHomeSeedHash
    ? `浏览器自定义首页: ${shellState.userHomeSeedHash}`
    : shellState?.backend.defaultHomeSeedHash
      ? `客户端默认首页: ${shellState.backend.defaultHomeSeedHash}`
      : "当前没有首页";

  const currentRootSeedHash = shellState?.currentRootSeedHash || "";

  return (
    <div className="settings-shell">
      <aside className="settings-sidebar">
        <div className="brand-block">
          <p className="brand-kicker">BitFS Browser Shell</p>
          <h1>设置中心</h1>
          <p className="brand-copy">
            这是壳内受信页面。它看起来像普通页面，但所有管理动作都仍然走壳内桥，而不是外部 `bitfs://` 页面能力。
          </p>
        </div>

        <nav className="settings-nav">
          <button className={activeSection === "security" ? "nav-button active" : "nav-button"} type="button" onClick={() => setActiveSection("security")}>安全</button>
          <button className={activeSection === "network" ? "nav-button active" : "nav-button"} type="button" onClick={() => setActiveSection("network")}>网络</button>
          <button className={activeSection === "storage" ? "nav-button active" : "nav-button"} type="button" onClick={() => setActiveSection("storage")}>存储</button>
        </nav>
      </aside>

      <main className="settings-main">
        <header className="hero">
          <div>
            <p className="hero-kicker">Managed Client</p>
            <h2>{shellState?.backend.phase === "ready" ? "客户端已就绪" : "客户端未就绪"}</h2>
            <p className="hero-copy">
              {homeSource}
            </p>
          </div>
          <div className="hero-actions">
            <button className="ghost-button" type="button" disabled={busy || loading} onClick={() => void loadAll(staticPath)}>刷新全部</button>
          </div>
        </header>

        {error ? (
          <section className="error-banner">
            <strong>设置页错误</strong>
            <span>{error}</span>
          </section>
        ) : null}

        {loading ? <section className="loading-card">正在装载设置页数据...</section> : null}

        {!loading && activeSection === "security" ? (
          <div className="section-stack">
            <section className="panel">
              <div className="panel-head">
                <div>
                  <p className="panel-kicker">Security</p>
                  <h2>身份与首页</h2>
                </div>
              </div>
              <div className="metric-grid">
                <article className="metric-card">
                  <span>当前页面根 Seed</span>
                  <strong title={currentRootSeedHash || "-"}>{currentRootSeedHash ? shortHex(currentRootSeedHash) : "-"}</strong>
                  <p>如果你想让浏览器启动后直接打开当前页面，可以把它设成自定义首页。</p>
                </article>
                <article className="metric-card">
                  <span>钱包地址</span>
                  <strong title={walletSummary?.wallet_address || "-"}>{walletSummary?.wallet_address || "-"}</strong>
                  <p>这里只展示托管客户端当前钱包地址与公开余额摘要。</p>
                </article>
                <article className="metric-card">
                  <span>链上余额</span>
                  <strong>{formatSat(walletSummary?.onchain_balance_satoshi || 0)}</strong>
                  <p>{walletSummary?.balance_source || "余额来源未提供"}</p>
                </article>
              </div>

              <form
                className="form-stack section-form"
                onSubmit={(event) => {
                  event.preventDefault();
                  const normalized = normalizeSeedHash(homepageDraft);
                  if (normalized === "") {
                    setError("homepage seed hash must be 64 hex");
                    return;
                  }
                  void runBusyTask(async () => {
                    await setUserHomepage(normalized);
                    await loadAll(staticPath);
                  });
                }}
              >
                <label>
                  <span>自定义首页 seed hash</span>
                  <input className="text-input" value={homepageDraft} onChange={(event) => setHomepageDraft(event.target.value)} placeholder="输入 64 位 seed hash" />
                </label>
                <div className="panel-actions">
                  <button className="primary-button" type="submit" disabled={busy}>保存首页</button>
                  <button
                    className="ghost-button"
                    type="button"
                    disabled={busy || currentRootSeedHash === ""}
                    onClick={() => setHomepageDraft(currentRootSeedHash)}
                  >
                    使用当前页面
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    disabled={busy}
                    onClick={() => void runBusyTask(async () => {
                      await clearUserHomepage();
                      await loadAll(staticPath);
                    })}
                  >
                    清除自定义首页
                  </button>
                </div>
              </form>
            </section>

            <section className="panel">
              <div className="panel-head">
                <div>
                  <p className="panel-kicker">Security</p>
                  <h2>托管客户端</h2>
                </div>
              </div>
              <dl className="detail-grid">
                <div><dt>阶段</dt><dd>{shellState?.backend.phase || "-"}</dd></div>
                <div><dt>PID</dt><dd>{shellState?.backend.pid || "-"}</dd></div>
                <div><dt>API</dt><dd>{shellState?.backend.apiBase || "-"}</dd></div>
                <div><dt>Vault</dt><dd>{shellState?.backend.vaultPath || "-"}</dd></div>
                <div><dt>链访问模式</dt><dd>{shellState?.backend.chainAccessMode || "-"}</dd></div>
                <div><dt>Guard Base</dt><dd>{shellState?.backend.walletChainBaseURL || "-"}</dd></div>
              </dl>
              <div className="panel-actions">
                <button
                  className="ghost-button"
                  type="button"
                  disabled={busy}
                  onClick={() => void runBusyTask(async () => {
                    const result = await exportKeyFile();
                    if (result.cancelled) {
                      return;
                    }
                    await loadAll(staticPath);
                  })}
                >
                  导出密文私钥
                </button>
                <button
                  className="ghost-button"
                  type="button"
                  disabled={busy}
                  onClick={() => void runBusyTask(async () => {
                    await restartBackend();
                    await loadAll(staticPath);
                  })}
                >
                  重启托管客户端
                </button>
                <button
                  className="ghost-button danger"
                  type="button"
                  disabled={busy}
                  onClick={() => void runBusyTask(async () => {
                    await lockClient();
                    await loadAll(staticPath);
                  })}
                >
                  锁定客户端
                </button>
              </div>
            </section>
          </div>
        ) : null}

        {!loading && activeSection === "network" ? (
          <div className="section-stack">
            <GatewayManager
              items={gateways?.items || []}
              busy={busy}
              onSave={async (id, data) => {
                await runBusyTask(async () => {
                  await saveGateway(id, data);
                  await loadAll(staticPath);
                });
              }}
              onDelete={async (id) => {
                await runBusyTask(async () => {
                  await deleteGateway(id);
                  await loadAll(staticPath);
                });
              }}
            />
            <ArbiterManager
              items={arbiters?.items || []}
              busy={busy}
              onSave={async (id, data) => {
                await runBusyTask(async () => {
                  await saveArbiter(id, data);
                  await loadAll(staticPath);
                });
              }}
              onDelete={async (id) => {
                await runBusyTask(async () => {
                  await deleteArbiter(id);
                  await loadAll(staticPath);
                });
              }}
            />
          </div>
        ) : null}

        {!loading && activeSection === "storage" ? (
          <div className="section-stack">
            <WorkspacesManager
              items={workspaces?.items || []}
              busy={busy}
              onAdd={async (path, maxBytes) => {
                await runBusyTask(async () => {
                  await addWorkspace(path, maxBytes);
                  await loadAll(staticPath);
                });
              }}
              onUpdate={async (id, maxBytes, enabled) => {
                await runBusyTask(async () => {
                  await updateWorkspace(id, { max_bytes: maxBytes, enabled });
                  await loadAll(staticPath);
                });
              }}
              onDelete={async (id) => {
                await runBusyTask(async () => {
                  await deleteWorkspace(id);
                  await loadAll(staticPath);
                });
              }}
              onPickDirectory={async () => {
                const result = await pickWorkspaceDirectory();
                return result.cancelled ? "" : result.path;
              }}
            />

            <StaticFileManager
              tree={staticTree}
              busy={busy}
              onNavigate={(path) => {
                void runBusyTask(async () => {
                  await loadAll(path);
                });
              }}
              onRefresh={async () => {
                await runBusyTask(async () => {
                  await loadAll(staticPath);
                });
              }}
              onCreateDir={async (path) => {
                await runBusyTask(async () => {
                  await createStaticDir(path);
                  await loadAll(staticPath);
                });
              }}
              onDelete={async (item: StaticItem) => {
                await runBusyTask(async () => {
                  await deleteStaticEntry(item.path, item.type === "dir");
                  await loadAll(staticPath);
                });
              }}
              onSetPrice={async (path, floorPrice, discountBps) => {
                await runBusyTask(async () => {
                  await setStaticItemPrice(path, floorPrice, discountBps);
                  await loadAll(staticPath);
                });
              }}
              onUpload={async (path) => {
                await runBusyTask(async () => {
                  const result = await uploadStaticFile(path, false);
                  if (result.cancelled) {
                    return;
                  }
                  await loadAll(staticPath);
                });
              }}
            />
          </div>
        ) : null}
      </main>
    </div>
  );
}
