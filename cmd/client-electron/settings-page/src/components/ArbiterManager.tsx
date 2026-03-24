import { useState } from "react";

import type { Arbiter } from "../types";
import { shortHex } from "../utils";
import { Modal } from "./Modal";

type ArbiterManagerProps = {
  items: Arbiter[];
  busy: boolean;
  onSave: (id: number | null, data: Arbiter) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
};

type ArbiterDraft = {
  id: number | null;
  data: Arbiter;
};

export function ArbiterManager({ items, busy, onSave, onDelete }: ArbiterManagerProps) {
  const [draft, setDraft] = useState<ArbiterDraft | null>(null);

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <p className="panel-kicker">Network</p>
          <h2>仲裁</h2>
        </div>
        <button
          className="primary-button"
          type="button"
          disabled={busy}
          onClick={() => setDraft({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}
        >
          添加仲裁节点
        </button>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>启用</th>
              <th>地址</th>
              <th>公钥</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr>
                <td colSpan={5} className="empty-cell">当前没有仲裁节点。</td>
              </tr>
            ) : items.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.enabled ? "是" : "否"}</td>
                <td title={item.addr}>{item.addr}</td>
                <td title={item.pubkey}>{shortHex(item.pubkey)}</td>
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

      <Modal title={draft?.id === null ? "添加仲裁节点" : "编辑仲裁节点"} open={Boolean(draft)} onClose={() => setDraft(null)}>
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
              <span>仲裁公钥 hex</span>
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
