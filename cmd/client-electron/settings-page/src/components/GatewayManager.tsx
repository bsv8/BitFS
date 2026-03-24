import { useState } from "react";

import type { Gateway } from "../types";
import { shortHex } from "../utils";
import { Modal } from "./Modal";

type GatewayManagerProps = {
  items: Gateway[];
  busy: boolean;
  onSave: (id: number | null, data: Gateway) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
};

type GatewayDraft = {
  id: number | null;
  data: Gateway;
};

export function GatewayManager({ items, busy, onSave, onDelete }: GatewayManagerProps) {
  const [draft, setDraft] = useState<GatewayDraft | null>(null);

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <p className="panel-kicker">Network</p>
          <h2>网关</h2>
        </div>
        <button
          className="primary-button"
          type="button"
          disabled={busy}
          onClick={() => setDraft({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}
        >
          添加网关
        </button>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>启用</th>
              <th>连接</th>
              <th>费用池</th>
              <th>主网关</th>
              <th>地址</th>
              <th>公钥</th>
              <th>错误</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr>
                <td colSpan={9} className="empty-cell">当前没有网关。</td>
              </tr>
            ) : items.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.enabled ? "是" : "否"}</td>
                <td>{item.connectedness || "-"}</td>
                <td>{item.fee_pool_ready ? "就绪" : "未就绪"}</td>
                <td>{item.is_master ? "是" : "-"}</td>
                <td title={item.addr}>{item.addr}</td>
                <td title={item.pubkey}>{shortHex(item.pubkey)}</td>
                <td title={item.last_runtime_error || item.last_error || "-"}>
                  {item.last_runtime_error
                    ? `${item.last_runtime_error_stage || "runtime"}: ${item.last_runtime_error}`
                    : item.last_error || "-"}
                </td>
                <td className="row-actions">
                  <button className="ghost-button" type="button" onClick={() => setDraft({ id: item.id, data: { ...item } })}>编辑</button>
                  {!item.enabled ? (
                    <button className="ghost-button danger" type="button" onClick={() => void onDelete(item.id)}>删除</button>
                  ) : null}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Modal title={draft?.id === null ? "添加网关" : "编辑网关"} open={Boolean(draft)} onClose={() => setDraft(null)}>
        {draft ? (
          <form
            className="form-stack"
            onSubmit={(event) => {
              event.preventDefault();
              void onSave(draft.id, draft.data).then(() => setDraft(null));
            }}
          >
            <label className="check-row">
              <input
                type="checkbox"
                checked={draft.data.enabled}
                onChange={(event) => setDraft({ ...draft, data: { ...draft.data, enabled: event.target.checked } })}
              />
              <span>启用</span>
            </label>
            <label>
              <span>多地址</span>
              <input
                className="text-input"
                value={draft.data.addr}
                onChange={(event) => setDraft({ ...draft, data: { ...draft.data, addr: event.target.value } })}
                placeholder="/ip4/.../tcp/.../p2p/..."
              />
            </label>
            <label>
              <span>网关公钥 hex</span>
              <input
                className="text-input"
                value={draft.data.pubkey}
                onChange={(event) => setDraft({ ...draft, data: { ...draft.data, pubkey: event.target.value } })}
                placeholder="02..."
              />
            </label>
            <div className="modal-actions">
              <button className="primary-button" type="submit">保存</button>
              <button className="ghost-button" type="button" onClick={() => setDraft(null)}>取消</button>
            </div>
          </form>
        ) : null}
      </Modal>
    </section>
  );
}
