import React, { useState, useRef } from "react";
import type { StaticTreeResp } from "../types";
import { short, t, formatBytes, sat } from "../utils";
import { Modal } from "./Modal";

/**
 * 静态文件管理组件
 * 
 * 功能：
 * - 资源管理器样式的文件浏览
 * - 目录导航（根目录、返回上级、路径面包屑）
 * - 新建文件夹
 * - 文件上传
 * - 设置文件价格（底价、折扣）
 * - 删除文件/文件夹
 * - 多选支持
 */
interface StaticFileManagerProps {
  staticTree: StaticTreeResp | null;
  staticPathHistory: string[];
  staticCurrentPath: string;
  selectedStaticItems: Set<string>;
  onNavigate: (path: string) => void;
  onNavigateBack: () => void;
  onRefresh: () => void;
  onCreateDir: (name: string) => void;
  onDelete: (path: string) => void;
  onSetPrice: (path: string, floor: number, discount: number) => void;
  onToggleSelection: (path: string) => void;
  onUpload: (file: File, targetDir: string, overwrite: boolean) => Promise<void>;
}

export function StaticFileManager({
  staticTree,
  staticPathHistory,
  staticCurrentPath,
  selectedStaticItems,
  onNavigate,
  onNavigateBack,
  onRefresh,
  onCreateDir,
  onDelete,
  onSetPrice,
  onToggleSelection,
  onUpload
}: StaticFileManagerProps) {
  const [newDirName, setNewDirName] = useState("");
  const [showNewDir, setShowNewDir] = useState(false);
  const [editingPrice, setEditingPrice] = useState<{ path: string; floor: string; discount: string } | null>(null);
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      await onUpload(file, staticCurrentPath, false);
      onRefresh();
    } catch (err) {
      alert(err instanceof Error ? err.message : "上传失败");
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleCreateDir = () => {
    if (newDirName) {
      onCreateDir(newDirName);
      setNewDirName("");
      setShowNewDir(false);
    }
  };

  const handleSavePrice = () => {
    if (editingPrice) {
      onSetPrice(editingPrice.path, Number(editingPrice.floor), Number(editingPrice.discount));
      setEditingPrice(null);
    }
  };

  if (!staticTree) return null;

  return (
    <section className="panel">
      <div className="panel-head">
        <h3>静态文件管理</h3>
        <div className="filters">
          <button className="btn btn-light" onClick={() => onNavigate("/")}>根目录</button>
          <button className="btn btn-light" disabled={staticCurrentPath === "/"} onClick={onNavigateBack}>返回上级</button>
          <button className="btn" onClick={() => setShowNewDir(true)}>新建文件夹</button>
          <button className="btn" disabled={uploading} onClick={() => fileInputRef.current?.click()}>{uploading ? "上传中..." : "上传文件"}</button>
          <input ref={fileInputRef} type="file" style={{ display: "none" }} onChange={handleFileSelect} />
          <button className="btn btn-light" onClick={onRefresh}>刷新</button>
        </div>
      </div>

      {/* 路径导航 */}
      <div className="path-bar">
        <span>当前路径: </span>
        {staticPathHistory.map((p, i) => (
          <span key={p}>
            {i > 0 && <span style={{ margin: "0 4px" }}>/</span>}
            <button className="btn btn-ghost" style={{ padding: "2px 6px", fontSize: 12 }} onClick={() => onNavigate(p)}>
              {p === "/" ? "根目录" : p.split("/").pop()}
            </button>
          </span>
        ))}
      </div>

      {/* 文件列表 */}
      <div className="table-wrap"><table><thead><tr><th style={{ width: 30 }}></th><th>名称</th><th>类型</th><th>大小</th><th>修改时间</th><th>Seed</th><th>底价</th><th>操作</th></tr></thead><tbody>
        {staticTree.items.length === 0 ? (
          <tr><td colSpan={8} style={{ textAlign: "center", color: "#666" }}>空目录</td></tr>
        ) : (
          staticTree.items.map((item) => (
            <tr key={item.path} className={selectedStaticItems.has(item.path) ? "selected-row" : ""}>
              <td><input type="checkbox" checked={selectedStaticItems.has(item.path)} onChange={() => onToggleSelection(item.path)} /></td>
              <td>
                <span style={{ marginRight: 6 }} className={item.type === "dir" ? "dir-icon" : "file-icon"}>{item.type === "dir" ? "📁" : "📄"}</span>
                {item.type === "dir" ? (
                  <button className="file-name-btn" onClick={() => onNavigate(item.path)}>{item.name}</button>
                ) : (
                  <span style={{ color: "#1a2a3d" }}>{item.name}</span>
                )}
              </td>
              <td>{item.type === "dir" ? "文件夹" : "文件"}</td>
              <td>{item.type === "file" ? formatBytes(item.size) : "-"}</td>
              <td>{t(item.mtime_unix)}</td>
              <td title={item.seed_hash || "-"}>{item.seed_hash ? short(item.seed_hash, 8) : "-"}</td>
              <td>{item.floor_unit_price_sat_per_64k ? sat(item.floor_unit_price_sat_per_64k) : "-"}</td>
              <td>
                {item.type === "file" && (
                  <button className="btn btn-light" onClick={() => setEditingPrice({ path: item.path, floor: String(item.floor_unit_price_sat_per_64k || 10), discount: String(item.resale_discount_bps || 8000) })}>设置价格</button>
                )}
                <button className="btn btn-light" onClick={() => onDelete(item.path)}>删除</button>
              </td>
            </tr>
          ))
        )}
      </tbody></table></div>

      {/* 新建文件夹对话框 */}
      <Modal title="新建文件夹" isOpen={showNewDir} onClose={() => { setShowNewDir(false); setNewDirName(""); }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <label style={{ fontSize: 13, color: "#5f6f85" }}>在当前目录下创建新文件夹</label>
          <input
            className="input"
            placeholder="文件夹名称"
            value={newDirName}
            onChange={(e) => setNewDirName(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") handleCreateDir(); }}
          />
        </div>
        <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
          <button className="btn" onClick={handleCreateDir}>创建</button>
          <button className="btn btn-light" onClick={() => { setShowNewDir(false); setNewDirName(""); }}>取消</button>
        </div>
      </Modal>

      {/* 设置价格对话框 */}
      <Modal
        title={editingPrice ? `设置价格: ${short(editingPrice.path, 30)}` : "设置价格"}
        isOpen={!!editingPrice}
        onClose={() => setEditingPrice(null)}
      >
        {editingPrice && (
          <>
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div>
                <label style={{ display: "block", fontSize: 12, color: "#5f6f85", marginBottom: 4 }}>底价 (sat/64k)</label>
                <input
                  className="input"
                  type="number"
                  value={editingPrice.floor}
                  onChange={(e) => setEditingPrice({ ...editingPrice, floor: e.target.value })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleSavePrice(); }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: 12, color: "#5f6f85", marginBottom: 4 }}>折扣 (bps)</label>
                <input
                  className="input"
                  type="number"
                  value={editingPrice.discount}
                  onChange={(e) => setEditingPrice({ ...editingPrice, discount: e.target.value })}
                  onKeyDown={(e) => { if (e.key === "Enter") handleSavePrice(); }}
                />
              </div>
            </div>
            <div className="modal-footer" style={{ margin: "16px -20px -20px", paddingTop: 16 }}>
              <button className="btn" onClick={handleSavePrice}>保存</button>
              <button className="btn btn-light" onClick={() => setEditingPrice(null)}>取消</button>
            </div>
          </>
        )}
      </Modal>

      <div className="hint" style={{ marginTop: 8 }}>当前目录: {staticTree.current_path} | 选中: {selectedStaticItems.size} 项</div>
    </section>
  );
}
