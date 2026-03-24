import { useState } from "react";

import type { StaticItem, StaticTreeResp } from "../types";
import { formatBytes, formatSat, formatTime, shortHex } from "../utils";
import { Modal } from "./Modal";

type StaticFileManagerProps = {
  tree: StaticTreeResp | null;
  busy: boolean;
  onNavigate: (path: string) => void;
  onRefresh: () => Promise<void>;
  onCreateDir: (path: string) => Promise<void>;
  onDelete: (item: StaticItem) => Promise<void>;
  onSetPrice: (path: string, floorPrice: number, discountBps: number) => Promise<void>;
  onUpload: (path: string) => Promise<void>;
};

type PriceDraft = {
  path: string;
  floorPrice: string;
  discountBps: string;
};

export function StaticFileManager({
  tree,
  busy,
  onNavigate,
  onRefresh,
  onCreateDir,
  onDelete,
  onSetPrice,
  onUpload
}: StaticFileManagerProps) {
  const [newDirName, setNewDirName] = useState("");
  const [newDirOpen, setNewDirOpen] = useState(false);
  const [priceDraft, setPriceDraft] = useState<PriceDraft | null>(null);

  const currentPath = tree?.current_path || "/";

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <p className="panel-kicker">Storage</p>
          <h2>静态文件</h2>
        </div>
        <div className="panel-actions">
          <button className="ghost-button" type="button" disabled={busy} onClick={() => onNavigate("/")}>根目录</button>
          <button className="ghost-button" type="button" disabled={busy || currentPath === "/"} onClick={() => onNavigate(tree?.parent_path || "/")}>上一级</button>
          <button className="ghost-button" type="button" disabled={busy} onClick={() => setNewDirOpen(true)}>新建文件夹</button>
          <button className="ghost-button" type="button" disabled={busy} onClick={() => void onUpload(currentPath)}>上传文件</button>
          <button className="ghost-button" type="button" disabled={busy} onClick={() => void onRefresh()}>刷新</button>
        </div>
      </div>

      <div className="path-strip">
        <span>当前目录</span>
        <strong>{currentPath}</strong>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>类型</th>
              <th>大小</th>
              <th>修改时间</th>
              <th>Seed</th>
              <th>底价</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {!tree || tree.items.length === 0 ? (
              <tr>
                <td colSpan={7} className="empty-cell">当前目录为空。</td>
              </tr>
            ) : tree.items.map((item) => (
              <tr key={item.path}>
                <td>
                  {item.type === "dir" ? (
                    <button className="file-link" type="button" onClick={() => onNavigate(item.path)}>
                      {item.name}
                    </button>
                  ) : (
                    <span>{item.name}</span>
                  )}
                </td>
                <td>{item.type === "dir" ? "目录" : "文件"}</td>
                <td>{item.type === "file" ? formatBytes(item.size) : "-"}</td>
                <td>{formatTime(item.mtime_unix)}</td>
                <td title={item.seed_hash || "-"}>{item.seed_hash ? shortHex(item.seed_hash) : "-"}</td>
                <td>{item.floor_unit_price_sat_per_64k ? formatSat(item.floor_unit_price_sat_per_64k) : "-"}</td>
                <td className="row-actions">
                  {item.type === "file" ? (
                    <button
                      className="ghost-button"
                      type="button"
                      onClick={() => setPriceDraft({
                        path: item.path,
                        floorPrice: String(item.floor_unit_price_sat_per_64k || 10),
                        discountBps: String(item.resale_discount_bps || 8000)
                      })}
                    >
                      定价
                    </button>
                  ) : null}
                  <button className="ghost-button danger" type="button" onClick={() => void onDelete(item)}>删除</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Modal title="新建文件夹" open={newDirOpen} onClose={() => setNewDirOpen(false)}>
        <form
          className="form-stack"
          onSubmit={(event) => {
            event.preventDefault();
            const target = `${currentPath.replace(/\/$/, "")}/${newDirName}`.replace(/^$/, "/");
            void onCreateDir(target).then(() => {
              setNewDirOpen(false);
              setNewDirName("");
            });
          }}
        >
          <label>
            <span>文件夹名称</span>
            <input className="text-input" value={newDirName} onChange={(event) => setNewDirName(event.target.value)} />
          </label>
          <div className="modal-actions">
            <button className="primary-button" type="submit">创建</button>
            <button className="ghost-button" type="button" onClick={() => setNewDirOpen(false)}>取消</button>
          </div>
        </form>
      </Modal>

      <Modal title="设置文件价格" open={Boolean(priceDraft)} onClose={() => setPriceDraft(null)}>
        {priceDraft ? (
          <form
            className="form-stack"
            onSubmit={(event) => {
              event.preventDefault();
              void onSetPrice(
                priceDraft.path,
                Math.max(1, Math.floor(Number(priceDraft.floorPrice || 0))),
                Math.max(1, Math.floor(Number(priceDraft.discountBps || 0)))
              ).then(() => setPriceDraft(null));
            }}
          >
            <label>
              <span>底价 sat/64k</span>
              <input
                className="text-input"
                type="number"
                min="1"
                value={priceDraft.floorPrice}
                onChange={(event) => setPriceDraft({ ...priceDraft, floorPrice: event.target.value })}
              />
            </label>
            <label>
              <span>转售价折扣 bps</span>
              <input
                className="text-input"
                type="number"
                min="1"
                max="10000"
                value={priceDraft.discountBps}
                onChange={(event) => setPriceDraft({ ...priceDraft, discountBps: event.target.value })}
              />
            </label>
            <div className="modal-actions">
              <button className="primary-button" type="submit">保存</button>
              <button className="ghost-button" type="button" onClick={() => setPriceDraft(null)}>取消</button>
            </div>
          </form>
        ) : null}
      </Modal>
    </section>
  );
}
