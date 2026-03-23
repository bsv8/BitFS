import React, { useState } from "react";
import type { Arbiter, ArbitersResp } from "../types";
import { short } from "../utils";
import { Modal } from "./Modal";

/**
 * 仲裁服务器管理页面组件
 * 
 * 功能：
 * - 显示仲裁服务器列表（ID、启用状态、地址、公钥）
 * - 添加/编辑仲裁服务器
 * - 删除已禁用仲裁服务器
 */
interface ArbiterManagerPageProps {
  arbiters: ArbitersResp | null;
  onSave: (id: number | null, data: Arbiter) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
}

export function ArbiterManagerPage({ arbiters, onSave, onDelete }: ArbiterManagerPageProps) {
  const [editing, setEditing] = useState<{ id: number | null; data: Arbiter } | null>(null);

  if (!arbiters) return null;

  return (
    <section className="panel">
      <div className="panel-head">
        <h3>仲裁服务器管理</h3>
        <button className="btn" onClick={() => setEditing({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}>添加仲裁服务器</button>
      </div>
      <div className="table-wrap"><table><thead><tr><th>ID</th><th>状态</th><th>地址</th><th>Pubkey</th><th>操作</th></tr></thead><tbody>
        {arbiters.items.map((a) => (
          <tr key={a.id}>
            <td>{a.id}</td>
            <td>{a.enabled ? "✅" : "⛔"}</td>
            <td title={a.addr}>{short(a.addr, 30)}</td>
            <td title={a.pubkey}>{short(a.pubkey, 12)}</td>
            <td>
              <button className="btn btn-light" onClick={() => setEditing({ id: a.id, data: { ...a } })}>编辑</button>
              {!a.enabled && <button className="btn btn-light" onClick={() => onDelete(a.id).catch(e => alert(e.message))}>删除</button>}
            </td>
          </tr>
        ))}
      </tbody></table></div>
      <Modal
        title={editing?.id === null ? "添加仲裁服务器" : "编辑仲裁服务器"}
        isOpen={!!editing}
        onClose={() => setEditing(null)}
      >
        {editing && (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
                <input type="checkbox" checked={editing.data.enabled} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, enabled: e.target.checked } })} />
                <span>启用</span>
              </label>
              <input className="input" placeholder="多地址 (/ip4/.../tcp/.../p2p/...)" value={editing.data.addr} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, addr: e.target.value } })} />
              <input className="input" placeholder="公钥 (hex)" value={editing.data.pubkey} onChange={(e) => setEditing({ ...editing, data: { ...editing.data, pubkey: e.target.value } })} />
            </div>
            <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
              <button className="btn" onClick={() => {
                onSave(editing.id, editing.data).then(() => setEditing(null)).catch(e => alert(e.message));
              }}>保存</button>
              <button className="btn btn-light" onClick={() => setEditing(null)}>取消</button>
            </div>
          </>
        )}
      </Modal>
    </section>
  );
}
