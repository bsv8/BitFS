import React, { useState } from "react";
import type { Gateway, GatewaysResp } from "../types";
import { short } from "../utils";
import { Modal } from "./Modal";

/**
 * 网关管理页面组件
 * 
 * 功能：
 * - 显示网关列表（ID、启用状态、网络连接、健康状态、主网关标记、错误信息、地址、公钥）
 * - 添加/编辑网关
 * - 删除已禁用网关
 */
interface GatewayManagerPageProps {
  gateways: GatewaysResp | null;
  onSave: (id: number | null, data: Gateway) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
}

export function GatewayManagerPage({ gateways, onSave, onDelete }: GatewayManagerPageProps) {
  const [editing, setEditing] = useState<{ id: number | null; data: Gateway } | null>(null);

  if (!gateways) return null;

  return (
    <section className="panel">
      <div className="panel-head">
        <h3>网关管理</h3>
        <button className="btn" onClick={() => setEditing({ id: null, data: { id: 0, enabled: true, addr: "", pubkey: "" } })}>添加网关</button>
      </div>
      <div className="table-wrap"><table><thead><tr><th>ID</th><th>启用</th><th>网络</th><th>健康</th><th>主</th><th>错误</th><th>地址</th><th>Pubkey</th><th>操作</th></tr></thead><tbody>
        {gateways.items.map((g) => {
          // 网络连接状态图标
          let connIcon = "⚫";
          let connTitle = "未知";
          if (g.connectedness === "connected") {
            connIcon = "🟢";
            connTitle = "已连接";
          } else if (g.connectedness === "can_connect") {
            connIcon = "🟡";
            connTitle = "可连接";
          } else if (g.connectedness && g.connectedness !== "not_connected") {
            connIcon = "🔴";
            connTitle = g.connectedness;
          } else if (g.connectedness === "not_connected") {
            connIcon = "🔴";
            connTitle = "未连接";
          }
          // 费用池就绪状态图标（比网络健康更严格）
          const healthIcon = g.fee_pool_ready ? "💚" : (g.last_runtime_error ? "⚠️" : "💔");
          const healthTitle = g.fee_pool_ready ? "费用池就绪" : (g.last_runtime_error ? `费用池异常: ${g.last_runtime_error_stage || "unknown"}` : "未就绪");
          return (
            <tr key={g.id}>
              <td>{g.id}</td>
              <td title={g.enabled ? "已启用" : "已禁用"}>{g.enabled ? "✅" : "⛔"}</td>
              <td title={`${connTitle}${g.last_error ? " | 连接错误: " + g.last_error : ""}${g.last_runtime_error ? " | 运行错误(" + (g.last_runtime_error_stage || "unknown") + "): " + g.last_runtime_error : ""}`}>{connIcon}</td>
              <td title={healthTitle}>{healthIcon}</td>
              <td title={g.is_master ? "主网关(优先下载)" : ""}>{g.is_master ? "👑" : ""}</td>
              <td title={`${g.last_runtime_error_stage ? "[" + g.last_runtime_error_stage + "] " : ""}${g.last_runtime_error || g.last_error || ""}`}>{short((g.last_runtime_error ? `${g.last_runtime_error_stage || "runtime"}: ${g.last_runtime_error}` : (g.last_error || "-")), 28)}</td>
              <td title={g.addr}>{short(g.addr, 30)}</td>
              <td title={g.pubkey}>{short(g.pubkey, 12)}</td>
              <td>
                <button className="btn btn-light" onClick={() => setEditing({ id: g.id, data: { ...g } })}>编辑</button>
                {!g.enabled && <button className="btn btn-light" onClick={() => onDelete(g.id).catch(e => alert(e.message))}>删除</button>}
              </td>
            </tr>
          );
        })}
      </tbody></table></div>
      <Modal
        title={editing?.id === null ? "添加网关" : "编辑网关"}
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
