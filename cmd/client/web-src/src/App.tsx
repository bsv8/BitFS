import { useEffect, useState } from "react";

type AuthState = "locked" | "checking" | "ready" | "error";

type BalanceResp = {
  summary: {
    pool_remaining_total_satoshi: number;
    server_amount_total_satoshi: number;
  };
  gateways: Array<{
    gateway_peer_id: string;
    pool_remaining_satoshi: number;
    server_amount_satoshi: number;
    remaining_cycle_seconds: number;
    single_cycle_fee_satoshi: number;
    single_publish_fee_satoshi: number;
  }>;
};

type TxResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_peer_id: string;
    event_type: string;
    direction: string;
    amount_satoshi: number;
    purpose: string;
    note: string;
  }>;
};

type SeedItem = {
  seed_hash: string;
  seed_file_path: string;
  chunk_count: number;
  file_size: number;
  unit_price_sat_per_64k: number;
  floor_unit_price_sat_per_64k: number;
  resale_discount_bps: number;
};

type SeedsResp = {
  total: number;
  items: SeedItem[];
};

type SalesResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    session_id: string;
    seed_hash: string;
    chunk_index: number;
    unit_price_sat_per_64k: number;
    amount_satoshi: number;
    buyer_gateway_peer_id: string;
  }>;
};

type FilesResp = {
  total: number;
  items: Array<{
    path: string;
    file_size: number;
    mtime_unix: number;
    seed_hash: string;
    updated_at_unix: number;
  }>;
};

type GatewayEventsResp = {
  total: number;
  items: Array<{
    id: number;
    created_at_unix: number;
    gateway_peer_id: string;
    action: string;
    msg_id?: string;
    sequence_num?: number;
    pool_id?: string;
    amount_satoshi: number;
    payload: unknown;
  }>;
};

type FileGetJob = {
  id: string;
  seed_hash: string;
  chunk_count: number;
  gateway_peer_id: string;
  status: string;
  started_at_unix: number;
  ended_at_unix?: number;
  output_file_path?: string;
  error?: string;
  steps: Array<{
    index: number;
    name: string;
    status: string;
    started_at_unix: number;
    ended_at_unix?: number;
    detail?: Record<string, string>;
  }>;
};

type SeedPriceDraft = {
  floor: string;
  discount: string;
  saving?: boolean;
  message?: string;
};

type HashRoute = {
  path: string;
  query: URLSearchParams;
};

function nowPath(): HashRoute {
  const raw = window.location.hash.replace(/^#/, "");
  const value = raw || "/wallet";
  const [pathPart, queryPart] = value.split("?", 2);
  const path = pathPart || "/wallet";
  const query = new URLSearchParams(queryPart || "");
  return { path, query };
}

function setHash(path: string, query?: URLSearchParams) {
  const q = query && query.toString() ? `?${query.toString()}` : "";
  window.location.hash = `${path}${q}`;
}

function sat(v: number) {
  return `${Number(v || 0).toLocaleString()} sat`;
}

function t(ts: number) {
  return ts ? new Date(ts * 1000).toLocaleString() : "-";
}

function short(s: string, n = 8) {
  if (!s) return "-";
  if (s.length <= n * 2) return s;
  return `${s.slice(0, n)}...${s.slice(-n)}`;
}

function toInt(v: string | null, d: number, min = 1, max = 500) {
  const x = Number(v || d);
  if (!Number.isFinite(x)) return d;
  return Math.max(min, Math.min(max, Math.floor(x)));
}

function pageCount(total: number, pageSize: number) {
  return Math.max(1, Math.ceil((total || 0) / pageSize));
}

function Pager({
  total,
  page,
  pageSize,
  onPage,
  onPageSize
}: {
  total: number;
  page: number;
  pageSize: number;
  onPage: (p: number) => void;
  onPageSize: (s: number) => void;
}) {
  const pages = pageCount(total, pageSize);
  return (
    <div className="pager">
      <button className="btn btn-light" disabled={page <= 1} onClick={() => onPage(page - 1)}>上一页</button>
      <span>第 {page} / {pages} 页</span>
      <button className="btn btn-light" disabled={page >= pages} onClick={() => onPage(page + 1)}>下一页</button>
      <select className="input small" value={pageSize} onChange={(e) => onPageSize(Number(e.target.value))}>
        {[10, 20, 50, 100].map((s) => <option key={s} value={s}>{s}/页</option>)}
      </select>
      <span>总计 {total}</span>
    </div>
  );
}

function purposeLabel(purpose: string, eventType: string) {
  const key = (purpose || eventType || "").trim();
  const map: Record<string, string> = {
    listen_bootstrap_topup: "初始化入池",
    auto_renew_topup: "自动续费入池",
    listen_cycle_fee: "侦听周期扣费",
    demand_publish_fee: "需求发布扣费",
    pool_exhausted: "费用池耗尽",
    renew_needed: "续费提醒",
    service_stopped: "服务停止",
    demand_publish_failed: "需求发布失败"
  };
  return map[key] || key || "-";
}

export default function App() {
  const [auth, setAuth] = useState<AuthState>("locked");
  const [authErr, setAuthErr] = useState("");
  const [tokenInput, setTokenInput] = useState("");
  const [token, setToken] = useState("");

  const [route, setRoute] = useState<HashRoute>(() => nowPath());
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const [balance, setBalance] = useState<BalanceResp | null>(null);
  const [tx, setTx] = useState<TxResp | null>(null);
  const [seeds, setSeeds] = useState<SeedsResp | null>(null);
  const [sales, setSales] = useState<SalesResp | null>(null);
  const [files, setFiles] = useState<FilesResp | null>(null);
  const [gatewayEvents, setGatewayEvents] = useState<GatewayEventsResp | null>(null);
  const [txDetail, setTxDetail] = useState<unknown>(null);
  const [saleDetail, setSaleDetail] = useState<unknown>(null);
  const [gatewayEventDetail, setGatewayEventDetail] = useState<unknown>(null);
  const [getFileJob, setGetFileJob] = useState<FileGetJob | null>(null);
  const [getFileSeedHash, setGetFileSeedHash] = useState("");
  const [getFileChunkCount, setGetFileChunkCount] = useState("1");

  const [seedDrafts, setSeedDrafts] = useState<Record<string, SeedPriceDraft>>({});

  useEffect(() => {
    const onHash = () => setRoute(nowPath());
    window.addEventListener("hashchange", onHash);
    if (!window.location.hash) setHash("/wallet");
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const api = async <T,>(path: string, method = "GET", tokenOverride?: string, body?: unknown): Promise<T> => {
    const tk = (tokenOverride ?? token).trim();
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (tk) headers.Authorization = `Bearer ${tk}`;
    const resp = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
    const text = await resp.text();
    let parsed: any = text;
    try { parsed = JSON.parse(text); } catch {}
    if (!resp.ok) throw new Error(typeof parsed === "string" ? parsed : JSON.stringify(parsed, null, 2));
    return parsed as T;
  };

  const updateQuery = (patch: Record<string, string | number | undefined | null>) => {
    const q = new URLSearchParams(route.query.toString());
    for (const [k, v] of Object.entries(patch)) {
      if (v === undefined || v === null || `${v}` === "") q.delete(k);
      else q.set(k, `${v}`);
    }
    if (!q.get("page")) q.set("page", "1");
    if (!q.get("pageSize")) q.set("pageSize", "20");
    setHash(route.path, q);
  };

  const loadWalletSummary = async () => {
    const [b, x] = await Promise.all([
      api<BalanceResp>("api/v1/balance"),
      api<TxResp>("api/v1/transactions?limit=5&offset=0")
    ]);
    setBalance(b);
    setTx(x);
  };

  const loadWalletFlows = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const eventType = route.query.get("event_type") || "";
    const direction = route.query.get("direction") || "";
    const purpose = route.query.get("purpose") || "";
    const q = route.query.get("q") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (eventType) params.set("event_type", eventType);
    if (direction) params.set("direction", direction);
    if (purpose) params.set("purpose", purpose);
    if (q) params.set("q", q);
    const [b, x] = await Promise.all([
      api<BalanceResp>("api/v1/balance"),
      api<TxResp>(`api/v1/transactions?${params.toString()}`)
    ]);
    setBalance(b);
    setTx(x);
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    if (detailID > 0) {
      setTxDetail(await api(`api/v1/transactions/detail?id=${detailID}`));
    } else {
      setTxDetail(null);
    }
  };

  const loadWalletGateways = async () => {
    setBalance(await api<BalanceResp>("api/v1/balance"));
  };

  const loadWalletGatewayEvents = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const gatewayPeerID = route.query.get("gateway_peer_id") || "";
    const action = route.query.get("action") || "";
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (gatewayPeerID) params.set("gateway_peer_id", gatewayPeerID);
    if (action) params.set("action", action);
    const list = await api<GatewayEventsResp>(`api/v1/gateways/events?${params.toString()}`);
    setGatewayEvents(list);
    if (detailID > 0) {
      setGatewayEventDetail(await api(`api/v1/gateways/events/detail?id=${detailID}`));
    } else {
      setGatewayEventDetail(null);
    }
  };

  const loadFilesSummary = async () => {
    const [s1, s2] = await Promise.all([
      api<SeedsResp>("api/v1/workspace/seeds?limit=5&offset=0"),
      api<SalesResp>("api/v1/sales?limit=5&offset=0")
    ]);
    setSeeds(s1);
    setSales(s2);
  };

  const loadFileSeeds = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const seedHashLike = route.query.get("seed_hash_like") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (seedHashLike) params.set("seed_hash_like", seedHashLike);
    setSeeds(await api<SeedsResp>(`api/v1/workspace/seeds?${params.toString()}`));
  };

  const loadFileSales = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const seedHash = route.query.get("seed_hash") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (seedHash) params.set("seed_hash", seedHash);
    const detailID = toInt(route.query.get("detailId"), 0, 0, 1_000_000_000);
    const list = await api<SalesResp>(`api/v1/sales?${params.toString()}`);
    setSales(list);
    if (detailID > 0) {
      setSaleDetail(await api(`api/v1/sales/detail?id=${detailID}`));
    } else {
      setSaleDetail(null);
    }
  };

  const loadFileIndex = async () => {
    const page = toInt(route.query.get("page"), 1);
    const pageSize = toInt(route.query.get("pageSize"), 20);
    const pathLike = route.query.get("path_like") || "";
    const params = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
    if (pathLike) params.set("path_like", pathLike);
    setFiles(await api<FilesResp>(`api/v1/workspace/files?${params.toString()}`));
  };

  const loadGetFileJob = async () => {
    const jobID = route.query.get("job_id") || "";
    if (!jobID) {
      setGetFileJob(null);
      return;
    }
    setGetFileJob(await api<FileGetJob>(`api/v1/files/get-file/job?id=${encodeURIComponent(jobID)}`));
  };

  const startGetFileJob = async () => {
    const seedHash = getFileSeedHash.trim().toLowerCase();
    const chunkCount = Number(getFileChunkCount || "0");
    if (!seedHash || !chunkCount || chunkCount <= 0) {
      setErr("seed_hash 和 chunk_count 必须有效");
      return;
    }
    const out = await api<{ job_id: string }>("api/v1/files/get-file", "POST", undefined, {
      seed_hash: seedHash,
      chunk_count: chunkCount
    });
    updateQuery({ job_id: out.job_id });
  };

  useEffect(() => {
    if (auth !== "ready") return;
    const run = async () => {
      setBusy(true);
      setErr("");
      try {
        switch (route.path) {
          case "/wallet":
            await loadWalletSummary();
            break;
          case "/wallet/flows":
            await loadWalletFlows();
            break;
          case "/wallet/gateways":
            await loadWalletGateways();
            break;
          case "/wallet/gateway-events":
            await loadWalletGatewayEvents();
            break;
          case "/files":
            await Promise.all([loadFilesSummary(), loadGetFileJob()]);
            break;
          case "/files/seeds":
          case "/files/pricing":
            await loadFileSeeds();
            break;
          case "/files/sales":
            await loadFileSales();
            break;
          case "/files/index":
            await loadFileIndex();
            break;
          default:
            setHash("/wallet");
        }
      } catch (e) {
        setErr(e instanceof Error ? e.message : "加载失败");
      } finally {
        setBusy(false);
      }
    };
    void run();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth, route.path, route.query.toString()]);

  useEffect(() => {
    if (auth !== "ready") return;
    if (route.path !== "/files") return;
    const jobID = route.query.get("job_id");
    if (!jobID) return;
    const timer = window.setInterval(() => {
      void loadGetFileJob();
    }, 2000);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [auth, route.path, route.query.get("job_id")]);

  const login = async (raw: string, silent = false) => {
    const tk = raw.trim();
    if (!tk) {
      setAuth("error");
      setAuthErr("请输入 token");
      return;
    }
    setAuth("checking");
    setAuthErr("");
    try {
      await api("api/v1/info", "GET", tk);
      localStorage.setItem("bitcast_api_token", tk);
      setToken(tk);
      setAuth("ready");
    } catch (e) {
      localStorage.removeItem("bitcast_api_token");
      setToken("");
      setAuth("error");
      setAuthErr(e instanceof Error ? e.message : "登录失败");
      if (silent) setTokenInput("");
    }
  };

  const logout = () => {
    localStorage.removeItem("bitcast_api_token");
    setToken("");
    setTokenInput("");
    setAuth("locked");
    setAuthErr("");
    setBalance(null);
    setTx(null);
    setSeeds(null);
    setSales(null);
    setFiles(null);
    setGatewayEvents(null);
    setSeedDrafts({});
    setTxDetail(null);
    setSaleDetail(null);
    setGatewayEventDetail(null);
    setGetFileJob(null);
  };

  const setSeedDraft = (seed: SeedItem, patch: Partial<SeedPriceDraft>) => {
    setSeedDrafts((prev) => {
      const cur = prev[seed.seed_hash] ?? {
        floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
        discount: String(seed.resale_discount_bps || 8000)
      };
      return { ...prev, [seed.seed_hash]: { ...cur, ...patch } };
    });
  };

  const saveSeedPrice = async (seed: SeedItem) => {
    const d = seedDrafts[seed.seed_hash] ?? {
      floor: String(seed.floor_unit_price_sat_per_64k || seed.unit_price_sat_per_64k || 10),
      discount: String(seed.resale_discount_bps || 8000)
    };
    setSeedDraft(seed, { saving: true, message: "" });
    try {
      await api("api/v1/workspace/seeds/price", "POST", undefined, {
        seed_hash: seed.seed_hash,
        floor_price_sat_per_64k: Number(d.floor || "0"),
        resale_discount_bps: Number(d.discount || "0")
      });
      setSeedDraft(seed, { saving: false, message: "已更新" });
      await loadFileSeeds();
    } catch (e) {
      setSeedDraft(seed, { saving: false, message: e instanceof Error ? e.message : "更新失败" });
    }
  };

  useEffect(() => {
    const saved = localStorage.getItem("bitcast_api_token") ?? "";
    setTokenInput(saved);
    if (saved.trim()) void login(saved.trim(), true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const moduleName = route.path.startsWith("/files") ? "files" : "wallet";
  const routeQueryText = route.query.toString();
  const routeMeta = routeQueryText ? `?${route.query.size} params` : "";

  if (auth !== "ready") {
    return (
      <div className="login-shell">
        <div className="login-card">
          <p className="eyebrow">Bitcast Client</p>
          <h1>钱包与文件管理后台</h1>
          <p className="desc">登录后可按模块进入子页面。URL 可刷新复现。</p>
          <div className="login-row">
            <input className="input" type="password" value={tokenInput} onChange={(e) => setTokenInput(e.target.value)} placeholder="API Token" />
            <button className="btn" onClick={() => void login(tokenInput)} disabled={auth === "checking"}>{auth === "checking" ? "验证中" : "登录"}</button>
          </div>
          {auth === "error" ? <p className="err">{authErr}</p> : null}
        </div>
      </div>
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div>
          <p className="eyebrow">Bitcast Ops</p>
          <h2>Dashboard</h2>
        </div>

        <div className="menu-group">
          <h4>钱包模块</h4>
          <button className={route.path === "/wallet" ? "menu active" : "menu"} onClick={() => setHash("/wallet")}>模块首页摘要</button>
          <button className={route.path === "/wallet/flows" ? "menu active" : "menu"} onClick={() => setHash("/wallet/flows", new URLSearchParams("page=1&pageSize=20"))}>资金流水</button>
          <button className={route.path === "/wallet/gateways" ? "menu active" : "menu"} onClick={() => setHash("/wallet/gateways", new URLSearchParams("page=1&pageSize=20"))}>钱罐状态</button>
          <button className={route.path === "/wallet/gateway-events" ? "menu active" : "menu"} onClick={() => setHash("/wallet/gateway-events", new URLSearchParams("page=1&pageSize=20"))}>网关事件历史</button>
        </div>

        <div className="menu-group">
          <h4>文件模块</h4>
          <button className={route.path === "/files" ? "menu active" : "menu"} onClick={() => setHash("/files")}>模块首页摘要</button>
          <button className={route.path === "/files/seeds" ? "menu active" : "menu"} onClick={() => setHash("/files/seeds", new URLSearchParams("page=1&pageSize=20"))}>种子列表</button>
          <button className={route.path === "/files/pricing" ? "menu active" : "menu"} onClick={() => setHash("/files/pricing", new URLSearchParams("page=1&pageSize=20"))}>价格管理</button>
          <button className={route.path === "/files/sales" ? "menu active" : "menu"} onClick={() => setHash("/files/sales", new URLSearchParams("page=1&pageSize=20"))}>售卖记录</button>
          <button className={route.path === "/files/index" ? "menu active" : "menu"} onClick={() => setHash("/files/index", new URLSearchParams("page=1&pageSize=20"))}>文件索引</button>
        </div>

        <button className="btn btn-ghost" onClick={logout}>登出</button>
      </aside>

      <main className="content">
        <header className="top">
          <h1>{moduleName === "wallet" ? "钱包模块" : "文件模块"}</h1>
          <div className="path" title={`${route.path}${routeQueryText ? `?${routeQueryText}` : ""}`}>
            {route.path}{routeMeta}
          </div>
        </header>

        {busy ? <div className="panel">加载中...</div> : null}
        {err ? <div className="panel err-inline">{err}</div> : null}

        {route.path === "/wallet" && balance && tx ? (
          <>
            <section className="stats-grid">
              <article className="stat"><p>综合费用池余额</p><h3>{sat(balance.summary.pool_remaining_total_satoshi)}</h3></article>
              <article className="stat"><p>累计扣费</p><h3>{sat(balance.summary.server_amount_total_satoshi)}</h3></article>
              <article className="stat"><p>网关数</p><h3>{balance.gateways.length}</h3></article>
            </section>
            <section className="panel">
              <h3>最近费用划拨流水（摘要）</h3>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>方向</th><th>金额</th><th>划拨原因</th><th>备注</th></tr></thead><tbody>
                {tx.items.map((it) => <tr key={it.id}><td>{t(it.created_at_unix)}</td><td>{it.direction}</td><td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td><td>{purposeLabel(it.purpose, it.event_type)}</td><td>{it.note || "-"}</td></tr>)}
              </tbody></table></div>
            </section>
          </>
        ) : null}

        {route.path === "/wallet/flows" && tx ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const eventType = route.query.get("event_type") || "";
          const direction = route.query.get("direction") || "";
          const purpose = route.query.get("purpose") || "";
          const q = route.query.get("q") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>综合费用池划拨流水</h3>
                <div className="filters">
                  <input className="input" defaultValue={eventType} placeholder="event_type" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ event_type: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={direction} placeholder="direction" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ direction: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={purpose} placeholder="purpose" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ purpose: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={q} placeholder="关键词(note/msg/gateway)" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ q: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>方向</th><th>金额</th><th>划拨原因</th><th>备注</th><th>网关</th></tr></thead><tbody>
                {tx.items.map((it) => (
                  <tr key={it.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === it.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: it.id })}>
                    <td>{t(it.created_at_unix)}</td>
                    <td>{it.direction}</td>
                    <td className={it.amount_satoshi < 0 ? "down" : "up"}>{sat(it.amount_satoshi)}</td>
                    <td>{purposeLabel(it.purpose, it.event_type)}</td>
                    <td>{it.note || "-"}</td>
                    <td title={it.gateway_peer_id}>{short(it.gateway_peer_id, 8)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={tx.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {txDetail ? <pre className="detail-pre">{JSON.stringify(txDetail, null, 2)}</pre> : <div className="hint">点击某条记录查看详细信息</div>}
            </section>
          );
        })() : null}

        {route.path === "/wallet/gateways" && balance ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const list = balance.gateways;
          const start = (page - 1) * pageSize;
          const pageItems = list.slice(start, start + pageSize);
          return (
            <section className="panel">
              <h3>综合费用池状态</h3>
              <div className="table-wrap"><table><thead><tr><th>网关</th><th>综合池余额</th><th>累计扣费</th><th>剩余秒数</th><th>侦听单轮费用</th><th>发布最小扣费</th></tr></thead><tbody>
                {pageItems.map((g) => <tr key={g.gateway_peer_id}><td title={g.gateway_peer_id}>{short(g.gateway_peer_id, 10)}</td><td>{sat(g.pool_remaining_satoshi)}</td><td>{sat(g.server_amount_satoshi)}</td><td>{g.remaining_cycle_seconds}</td><td>{sat(g.single_cycle_fee_satoshi)}</td><td>{sat(g.single_publish_fee_satoshi)}</td></tr>)}
              </tbody></table></div>
              <Pager total={list.length} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/wallet/gateway-events" && gatewayEvents ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const gatewayPeerID = route.query.get("gateway_peer_id") || "";
          const action = route.query.get("action") || "";
          return (
            <section className="panel">
              <div className="panel-head">
                <h3>网关事件历史（含动作时间）</h3>
                <div className="filters">
                  <input className="input" defaultValue={gatewayPeerID} placeholder="gateway_peer_id" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ gateway_peer_id: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <input className="input" defaultValue={action} placeholder="action" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ action: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>网关</th><th>动作</th><th>金额</th><th>pool</th><th>msg</th></tr></thead><tbody>
                {gatewayEvents.items.map((e) => (
                  <tr key={e.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === e.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: e.id })}>
                    <td>{t(e.created_at_unix)}</td>
                    <td title={e.gateway_peer_id}>{short(e.gateway_peer_id, 8)}</td>
                    <td>{e.action}</td>
                    <td>{sat(e.amount_satoshi)}</td>
                    <td>{e.pool_id || "-"}</td>
                    <td>{e.msg_id || "-"}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={gatewayEvents.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {gatewayEventDetail ? <pre className="detail-pre">{JSON.stringify(gatewayEventDetail, null, 2)}</pre> : <div className="hint">点击某条记录查看事件详情 payload</div>}
            </section>
          );
        })() : null}

        {route.path === "/files" && seeds && sales ? (
          <>
            <section className="stats-grid">
              <article className="stat"><p>种子总数</p><h3>{seeds.total}</h3></article>
              <article className="stat"><p>售卖记录总数</p><h3>{sales.total}</h3></article>
              <article className="stat"><p>模块</p><h3>文件管理</h3></article>
            </section>
            <section className="panel">
              <div className="panel-head">
                <h3>Get File（按 hash 拉取）</h3>
                <div className="filters">
                  <button className="btn btn-light" onClick={() => void loadGetFileJob()}>刷新进度</button>
                </div>
              </div>
              <div className="filters">
                <input className="input" placeholder="seed_hash" value={getFileSeedHash} onChange={(e) => setGetFileSeedHash(e.target.value)} />
                <input className="input small" type="number" min={1} value={getFileChunkCount} onChange={(e) => setGetFileChunkCount(e.target.value)} />
                <button className="btn" onClick={() => void startGetFileJob()}>启动拉取任务</button>
              </div>
              {getFileJob ? (
                <div className="job-box">
                  <div className="hint">job_id: {getFileJob.id} | status: {getFileJob.status} | gateway: {getFileJob.gateway_peer_id || "-"}</div>
                  {getFileJob.output_file_path ? <div className="hint">output: {getFileJob.output_file_path}</div> : null}
                  {getFileJob.error ? <div className="err-inline">error: {getFileJob.error}</div> : null}
                  <div className="timeline">
                    {getFileJob.steps.map((st) => (
                      <div key={`${getFileJob.id}-${st.index}`} className={`step ${st.status}`}>
                        <div className="step-head">
                          <strong>{st.index + 1}. {st.name}</strong>
                          <span>{st.status}</span>
                        </div>
                        <div className="hint">{t(st.started_at_unix)} {st.ended_at_unix ? `-> ${t(st.ended_at_unix)}` : ""}</div>
                        {st.detail ? <pre className="detail-pre">{JSON.stringify(st.detail, null, 2)}</pre> : null}
                      </div>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="hint">输入 seed_hash + chunk_count 后启动任务。可通过 URL 参数 `job_id` 刷新恢复进度视图。</div>
              )}
            </section>
            <section className="panel"><h3>最近售卖摘要</h3><div className="table-wrap"><table><thead><tr><th>时间</th><th>Seed</th><th>Session</th><th>成交额</th></tr></thead><tbody>
              {sales.items.map((x) => <tr key={x.id}><td>{t(x.created_at_unix)}</td><td title={x.seed_hash}>{short(x.seed_hash, 10)}</td><td title={x.session_id}>{short(x.session_id, 8)}</td><td className="up">{sat(x.amount_satoshi)}</td></tr>)}
            </tbody></table></div></section>
          </>
        ) : null}

        {(route.path === "/files/seeds" || route.path === "/files/pricing") && seeds ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const like = route.query.get("seed_hash_like") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>{route.path === "/files/pricing" ? "价格管理" : "种子列表"}</h3>
                <div className="filters">
                  <input className="input" defaultValue={like} placeholder="seed_hash_like" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ seed_hash_like: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>Seed</th><th>Chunk</th><th>文件大小</th><th>当前单价</th><th>底价</th><th>BPS</th>{route.path === "/files/pricing" ? <th>单独改价</th> : null}</tr></thead><tbody>
                {seeds.items.map((s) => {
                  const d = seedDrafts[s.seed_hash] ?? { floor: String(s.floor_unit_price_sat_per_64k || s.unit_price_sat_per_64k || 10), discount: String(s.resale_discount_bps || 8000) };
                  return <tr key={s.seed_hash}><td title={s.seed_hash}>{short(s.seed_hash, 12)}</td><td>{s.chunk_count}</td><td>{s.file_size.toLocaleString()} B</td><td>{sat(s.unit_price_sat_per_64k)}</td><td>{sat(s.floor_unit_price_sat_per_64k)}</td><td>{s.resale_discount_bps}</td>{route.path === "/files/pricing" ? <td><div className="filters"><input className="input small" type="number" value={d.floor} onChange={(e) => setSeedDraft(s, { floor: e.target.value })} /><input className="input small" type="number" value={d.discount} onChange={(e) => setSeedDraft(s, { discount: e.target.value })} /><button className="btn" disabled={!!d.saving} onClick={() => void saveSeedPrice(s)}>{d.saving ? "保存中" : "保存"}</button></div>{d.message ? <div className="hint">{d.message}</div> : null}</td> : null}</tr>;
                })}
              </tbody></table></div>
              <Pager total={seeds.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}

        {route.path === "/files/sales" && sales ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const seed = route.query.get("seed_hash") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>售卖记录</h3>
                <div className="filters">
                  <input className="input" defaultValue={seed} placeholder="seed_hash" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ seed_hash: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>时间</th><th>Seed</th><th>Session</th><th>Chunk</th><th>单价</th><th>成交额</th><th>买方网关</th></tr></thead><tbody>
                {sales.items.map((s) => (
                  <tr key={s.id} className={toInt(route.query.get("detailId"), 0, 0, 1_000_000_000) === s.id ? "selected-row" : ""} onClick={() => updateQuery({ detailId: s.id })}>
                    <td>{t(s.created_at_unix)}</td>
                    <td title={s.seed_hash}>{short(s.seed_hash, 10)}</td>
                    <td title={s.session_id}>{short(s.session_id, 8)}</td>
                    <td>{s.chunk_index}</td>
                    <td>{sat(s.unit_price_sat_per_64k)}</td>
                    <td className="up">{sat(s.amount_satoshi)}</td>
                    <td title={s.buyer_gateway_peer_id}>{short(s.buyer_gateway_peer_id, 8)}</td>
                  </tr>
                ))}
              </tbody></table></div>
              <Pager total={sales.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
              {saleDetail ? <pre className="detail-pre">{JSON.stringify(saleDetail, null, 2)}</pre> : <div className="hint">点击某条售卖记录查看详细字段</div>}
            </section>
          );
        })() : null}

        {route.path === "/files/index" && files ? (() => {
          const page = toInt(route.query.get("page"), 1);
          const pageSize = toInt(route.query.get("pageSize"), 20);
          const pathLike = route.query.get("path_like") || "";
          return (
            <section className="panel">
              <div className="panel-head"><h3>文件索引</h3>
                <div className="filters">
                  <input className="input" defaultValue={pathLike} placeholder="path_like" onKeyDown={(e) => { if (e.key === "Enter") updateQuery({ path_like: (e.target as HTMLInputElement).value, page: 1 }); }} />
                  <button className="btn" onClick={() => updateQuery({ page: 1 })}>查询</button>
                </div>
              </div>
              <div className="table-wrap"><table><thead><tr><th>路径</th><th>文件大小</th><th>Seed</th><th>更新时间</th></tr></thead><tbody>
                {files.items.map((f) => <tr key={f.path}><td title={f.path}>{f.path}</td><td>{f.file_size.toLocaleString()} B</td><td title={f.seed_hash}>{short(f.seed_hash, 10)}</td><td>{t(f.updated_at_unix)}</td></tr>)}
              </tbody></table></div>
              <Pager total={files.total} page={page} pageSize={pageSize} onPage={(p) => updateQuery({ page: p })} onPageSize={(s) => updateQuery({ pageSize: s, page: 1 })} />
            </section>
          );
        })() : null}
      </main>
    </div>
  );
}
