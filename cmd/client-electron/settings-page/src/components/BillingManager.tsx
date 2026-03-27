import { useEffect, useState } from "react";

import type { ShellState } from "../bitfs";
import { getWalletFundFlowDetail, getWalletFundFlows } from "../api";
import type { WalletFundFlowItem, WalletFundFlowListResp, WalletSummary } from "../types";
import { formatSat, formatTime, shortHex } from "../utils";
import { billingPurposeOptions, defaultPurposePlaceholder, formatPurposeLabel } from "../purpose";
import { Modal } from "./Modal";

type BillingManagerProps = {
  walletSummary: WalletSummary | null;
  shellBusy: boolean;
  shellState: ShellState | null;
};

type BillingQuery = {
  visitID: string;
  direction: string;
  purpose: string;
  stage: string;
  flowType: string;
  q: string;
};

const pageSize = 20;

export function BillingManager({ walletSummary, shellBusy, shellState }: BillingManagerProps) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [page, setPage] = useState(0);
  const [query, setQuery] = useState<BillingQuery>({
    visitID: "",
    direction: "",
    purpose: "",
    stage: "",
    flowType: "",
    q: ""
  });
  const [draftQuery, setDraftQuery] = useState<BillingQuery>({
    visitID: "",
    direction: "",
    purpose: "",
    stage: "",
    flowType: "",
    q: ""
  });
  const [listResp, setListResp] = useState<WalletFundFlowListResp | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [detail, setDetail] = useState<WalletFundFlowItem | null>(null);

  async function loadFlows(nextQuery: BillingQuery, nextPage: number) {
    setLoading(true);
    setError("");
    try {
      const resp = await getWalletFundFlows({
        limit: pageSize,
        offset: nextPage * pageSize,
        visitID: nextQuery.visitID,
        direction: nextQuery.direction,
        purpose: nextQuery.purpose,
        stage: nextQuery.stage,
        flowType: nextQuery.flowType,
        q: nextQuery.q
      });
      setListResp(resp);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadFlows(query, page);
  }, [page, query]);

  useEffect(() => {
    void loadFlows(query, page);
  }, [walletSummary?.wallet_fund_flow_count, walletSummary?.total_out_satoshi, walletSummary?.total_used_satoshi]);

  const items = listResp?.items || [];
  const total = Number(listResp?.total || 0);
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentVisit = shellState?.currentVisit;
  const currentVisitID = String(currentVisit?.visitID || "").trim();
  const isFilteringCurrentVisit = query.visitID !== "" && query.visitID === currentVisitID;

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <p className="panel-kicker">Billing</p>
          <h2>账务明细</h2>
        </div>
        <div className="panel-actions">
          <button
            className="ghost-button"
            type="button"
            disabled={shellBusy || loading}
            onClick={() => void loadFlows(query, page)}
          >
            刷新
          </button>
        </div>
      </div>

      <div className="metric-grid">
        <article className="metric-card">
          <span>流水笔数</span>
          <strong>{Number(walletSummary?.wallet_fund_flow_count || 0)}</strong>
          <p>这里显示客户端账务流水总笔数。</p>
        </article>
        <article className="metric-card">
          <span>累计流入</span>
          <strong>{formatSat(Number(walletSummary?.total_in_satoshi || 0))}</strong>
          <p>所有入账流水总和。</p>
        </article>
        <article className="metric-card">
          <span>累计流出</span>
          <strong>{formatSat(Number(walletSummary?.total_out_satoshi || 0))}</strong>
          <p>所有出账流水总和。</p>
        </article>
        <article className="metric-card">
          <span>累计已用</span>
          <strong>{formatSat(Number(walletSummary?.total_used_satoshi || 0))}</strong>
          <p>真实被业务消耗掉的金额。</p>
        </article>
        <article className="metric-card">
          <span>累计退回</span>
          <strong>{formatSat(Number(walletSummary?.total_returned_satoshi || 0))}</strong>
          <p>从业务流程里退回的钱。</p>
        </article>
        <article className="metric-card">
          <span>账面净值</span>
          <strong>{formatSat(Number(walletSummary?.ledger_net_satoshi || 0))}</strong>
          <p>当前账面净入账结果。</p>
        </article>
      </div>

      <section className="path-strip">
        <span>当前壳访问会话</span>
        <strong>{currentVisit?.locator || "当前没有访问会话"}</strong>
        <p className="helper-copy">
          {currentVisitID
            ? `visit_id: ${currentVisitID} | 状态: ${formatVisitStatus(currentVisit?.status)} | 已花费: ${formatSat(Number(currentVisit?.totalUsedSatoshi || 0))}`
            : "如果你要把账务明细和当前访问真正对上，可以直接使用下面的“筛选当前访问”。"}
        </p>
        <div className="panel-actions">
          <button
            className="ghost-button"
            type="button"
            disabled={shellBusy || loading || currentVisitID === ""}
            onClick={() => {
              const next = { ...draftQuery, visitID: currentVisitID };
              setDraftQuery(next);
              setQuery(next);
              setPage(0);
            }}
          >
            筛选当前访问
          </button>
          <button
            className="ghost-button"
            type="button"
            disabled={shellBusy || loading || query.visitID === ""}
            onClick={() => {
              const next = { ...draftQuery, visitID: "" };
              setDraftQuery(next);
              setQuery(next);
              setPage(0);
            }}
          >
            清除访问筛选
          </button>
        </div>
      </section>

      <form
        className="billing-filter-grid"
        onSubmit={(event) => {
          event.preventDefault();
          setPage(0);
          setQuery({ ...draftQuery });
        }}
      >
        <label>
          <span>访问会话</span>
          <input
            className="text-input"
            value={draftQuery.visitID}
            onChange={(event) => setDraftQuery({ ...draftQuery, visitID: event.target.value })}
            placeholder="visit_id"
          />
        </label>
        <label>
          <span>方向</span>
          <select
            className="text-input"
            value={draftQuery.direction}
            onChange={(event) => setDraftQuery({ ...draftQuery, direction: event.target.value })}
          >
            <option value="">全部</option>
            <option value="in">流入</option>
            <option value="out">流出</option>
          </select>
        </label>
        <label>
          <span>用途</span>
          <input
            className="text-input"
            list="billing-purpose-options"
            value={draftQuery.purpose}
            onChange={(event) => setDraftQuery({ ...draftQuery, purpose: event.target.value })}
            placeholder={defaultPurposePlaceholder}
          />
        </label>
        <label>
          <span>阶段</span>
          <input
            className="text-input"
            value={draftQuery.stage}
            onChange={(event) => setDraftQuery({ ...draftQuery, stage: event.target.value })}
            placeholder="use_node_reachability_query"
          />
        </label>
        <label>
          <span>类型</span>
          <input
            className="text-input"
            value={draftQuery.flowType}
            onChange={(event) => setDraftQuery({ ...draftQuery, flowType: event.target.value })}
            placeholder="fee_pool"
          />
        </label>
        <label className="billing-filter-search">
          <span>关键字</span>
          <input
            className="text-input"
            value={draftQuery.q}
            onChange={(event) => setDraftQuery({ ...draftQuery, q: event.target.value })}
            placeholder="flow_id / note / txid"
          />
        </label>
        <div className="panel-actions billing-filter-actions">
          <button className="primary-button" type="submit" disabled={shellBusy || loading}>应用筛选</button>
          <button
            className="ghost-button"
            type="button"
            disabled={shellBusy || loading}
            onClick={() => {
              const empty = { visitID: "", direction: "", purpose: "", stage: "", flowType: "", q: "" };
              setDraftQuery(empty);
              setQuery(empty);
              setPage(0);
            }}
          >
            清空
          </button>
        </div>
      </form>

      <datalist id="billing-purpose-options">
        {billingPurposeOptions.map((option) => (
          <option key={option.value} value={option.value}>{option.label}</option>
        ))}
      </datalist>

      {isFilteringCurrentVisit ? (
        <section className="path-strip">
          <span>当前正在查看</span>
          <strong>{currentVisit?.locator || query.visitID}</strong>
          <p className="helper-copy">这份流水已经按当前访问会话过滤，只会显示同一次访问链路里落下来的账务记录。</p>
        </section>
      ) : null}

      {error ? <section className="error-banner"><strong>账务查询失败</strong><span>{error}</span></section> : null}

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>方向</th>
              <th>用途</th>
              <th>阶段</th>
              <th>类型</th>
              <th>金额</th>
              <th>已用</th>
              <th>退回</th>
              <th>访问</th>
              <th>引用</th>
              <th>相关交易</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={12} className="empty-cell">正在读取账务流水...</td>
              </tr>
            ) : items.length === 0 ? (
              <tr>
                <td colSpan={12} className="empty-cell">当前筛选条件下没有账务记录。</td>
              </tr>
            ) : items.map((item) => (
              <tr key={item.id}>
                <td>{formatTime(item.created_at_unix)}</td>
                <td>{formatDirection(item.direction)}</td>
                <td title={item.purpose}>{formatPurposeLabel(item.purpose)}</td>
                <td title={item.stage}>{item.stage || "-"}</td>
                <td>{item.flow_type || "-"}</td>
                <td>{formatSignedSat(item.amount_satoshi)}</td>
                <td>{formatSat(item.used_satoshi)}</td>
                <td>{formatSat(item.returned_satoshi)}</td>
                <td title={item.visit_locator || item.visit_id || "-"}>{item.visit_id ? shortHex(item.visit_id, 8, 8) : "-"}</td>
                <td title={item.ref_id}>{item.ref_id ? shortHex(item.ref_id, 8, 8) : "-"}</td>
                <td title={item.related_txid}>{item.related_txid ? shortHex(item.related_txid, 8, 8) : "-"}</td>
                <td className="row-actions">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => {
                      setDetailLoading(true);
                      setDetailError("");
                      setDetail(null);
                      void getWalletFundFlowDetail(item.id).then((resp) => {
                        setDetail(resp);
                      }).catch((nextError) => {
                        setDetailError(nextError instanceof Error ? nextError.message : String(nextError));
                      }).finally(() => {
                        setDetailLoading(false);
                      });
                    }}
                  >
                    详情
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel-actions billing-pagination">
        <span className="helper-copy">共 {total} 条，第 {Math.min(page + 1, totalPages)} / {totalPages} 页</span>
        <button className="ghost-button" type="button" disabled={shellBusy || loading || page <= 0} onClick={() => setPage((current) => Math.max(0, current - 1))}>上一页</button>
        <button className="ghost-button" type="button" disabled={shellBusy || loading || (page + 1) >= totalPages} onClick={() => setPage((current) => current + 1)}>下一页</button>
      </div>

      <Modal title="账务明细" open={detailLoading || Boolean(detail) || detailError !== ""} onClose={() => {
        setDetailLoading(false);
        setDetailError("");
        setDetail(null);
      }}>
        {detailLoading ? (
          <div className="loading-card inline-card">正在读取账务详情...</div>
        ) : detailError ? (
          <section className="error-banner"><strong>账务详情失败</strong><span>{detailError}</span></section>
        ) : detail ? (
          <div className="form-stack">
            <div className="detail-grid">
              <div><dt>ID</dt><dd>{detail.id}</dd></div>
              <div><dt>时间</dt><dd>{formatTime(detail.created_at_unix)}</dd></div>
              <div><dt>访问 ID</dt><dd title={detail.visit_id}>{detail.visit_id || "-"}</dd></div>
              <div><dt>访问地址</dt><dd title={detail.visit_locator}>{detail.visit_locator || "-"}</dd></div>
              <div><dt>方向</dt><dd>{formatDirection(detail.direction)}</dd></div>
              <div><dt>类型</dt><dd>{detail.flow_type || "-"}</dd></div>
              <div><dt>用途</dt><dd>{formatPurposeLabel(detail.purpose)} ({detail.purpose || "-"})</dd></div>
              <div><dt>阶段</dt><dd>{detail.stage || "-"}</dd></div>
              <div><dt>金额</dt><dd>{formatSignedSat(detail.amount_satoshi)}</dd></div>
              <div><dt>已用</dt><dd>{formatSat(detail.used_satoshi)}</dd></div>
              <div><dt>退回</dt><dd>{formatSat(detail.returned_satoshi)}</dd></div>
              <div><dt>Flow ID</dt><dd title={detail.flow_id}>{detail.flow_id || "-"}</dd></div>
              <div><dt>Ref ID</dt><dd title={detail.ref_id}>{detail.ref_id || "-"}</dd></div>
              <div><dt>相关交易</dt><dd title={detail.related_txid}>{detail.related_txid || "-"}</dd></div>
            </div>

            <label>
              <span>说明</span>
              <textarea className="payload-box" readOnly value={detail.note || "-"} />
            </label>
            <label>
              <span>Payload JSON</span>
              <textarea className="payload-box" readOnly value={safeJSONStringify(detail.payload)} />
            </label>
            <div className="modal-actions">
              <button className="primary-button" type="button" onClick={() => void navigator.clipboard.writeText(safeJSONStringify(detail))}>复制完整记录</button>
              <button className="ghost-button" type="button" onClick={() => setDetail(null)}>关闭</button>
            </div>
          </div>
        ) : null}
      </Modal>
    </section>
  );
}

function formatDirection(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "in") {
    return "流入";
  }
  if (normalized === "out") {
    return "流出";
  }
  return normalized || "-";
}

function formatSignedSat(value: number): string {
  const amount = Math.trunc(Number(value || 0));
  if (!Number.isFinite(amount)) {
    return "-";
  }
  if (amount > 0) {
    return `+${amount} sat`;
  }
  return `${amount} sat`;
}

function formatVisitStatus(value: string | undefined): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "opening") {
    return "打开中";
  }
  if (normalized === "open") {
    return "已打开";
  }
  if (normalized === "failed") {
    return "已失败";
  }
  return normalized || "空闲";
}

function safeJSONStringify(value: unknown): string {
  try {
    return JSON.stringify(value ?? null, null, 2);
  } catch {
    return String(value || "");
  }
}
