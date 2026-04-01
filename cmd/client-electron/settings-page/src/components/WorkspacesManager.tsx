import { useState } from "react";

import type { Workspace } from "../types";
import { formatBytes, formatTime } from "../utils";
import { Modal } from "./Modal";

type WorkspacesManagerProps = {
  items: Workspace[];
  busy: boolean;
  onAdd: (path: string, maxBytes: number) => Promise<void>;
  onUpdate: (workspacePath: string, maxBytes: number, enabled: boolean) => Promise<void>;
  onDelete: (workspacePath: string) => Promise<void>;
  onPickDirectory: () => Promise<string>;
};

type WorkspaceDraft = {
  mode: "create" | "edit";
  workspacePath: string;
  maxBytes: string;
  enabled: boolean;
};

export function WorkspacesManager({
  items,
  busy,
  onAdd,
  onUpdate,
  onDelete,
  onPickDirectory
}: WorkspacesManagerProps) {
  const [draft, setDraft] = useState<WorkspaceDraft | null>(null);

  return (
    <section className="panel">
      <div className="panel-head">
        <div>
          <p className="panel-kicker">Storage</p>
          <h2>工作区</h2>
        </div>
        <button
          className="primary-button"
          type="button"
          disabled={busy}
          onClick={() => setDraft({ mode: "create", workspacePath: "", maxBytes: String(1024 * 1024 * 1024), enabled: true })}
        >
          添加工作区
        </button>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>路径</th>
              <th>启用</th>
              <th>上限</th>
              <th>创建时间</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {items.length === 0 ? (
              <tr>
                <td colSpan={5} className="empty-cell">当前没有工作区。</td>
              </tr>
            ) : items.map((item) => (
              <tr key={item.workspace_path}>
                <td title={item.workspace_path}>{item.workspace_path}</td>
                <td>{item.enabled ? "是" : "否"}</td>
                <td>{formatBytes(item.max_bytes)}</td>
                <td>{formatTime(item.created_at_unix)}</td>
                <td className="row-actions">
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => setDraft({
                      mode: "edit",
                      workspacePath: item.workspace_path,
                      maxBytes: String(item.max_bytes),
                      enabled: item.enabled
                    })}
                  >
                    编辑
                  </button>
                  <button className="ghost-button danger" type="button" onClick={() => void onDelete(item.workspace_path)}>删除</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Modal title={draft?.mode === "create" ? "添加工作区" : "编辑工作区"} open={Boolean(draft)} onClose={() => setDraft(null)}>
        {draft ? (
          <form
            className="form-stack"
            onSubmit={(event) => {
              event.preventDefault();
              const maxBytes = Math.max(0, Math.floor(Number(draft.maxBytes || 0)));
              if (draft.mode === "create") {
                void onAdd(draft.workspacePath, maxBytes).then(() => setDraft(null));
                return;
              }
              void onUpdate(draft.workspacePath, maxBytes, draft.enabled).then(() => setDraft(null));
            }}
          >
            <label>
              <span>目录路径</span>
              <div className="field-row">
                <input
                  className="text-input"
                  value={draft.workspacePath}
                  disabled={draft.mode !== "create"}
                  onChange={(event) => setDraft({ ...draft, workspacePath: event.target.value })}
                  placeholder="/data/bitfs-workspace"
                />
                {draft.mode === "create" ? (
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => void onPickDirectory().then((pickedPath) => {
                      if (pickedPath) {
                        setDraft((current) => current ? { ...current, workspacePath: pickedPath } : current);
                      }
                    })}
                  >
                    选择目录
                  </button>
                ) : null}
              </div>
            </label>
            <label>
              <span>容量上限字节数</span>
              <input
                className="text-input"
                type="number"
                min="0"
                value={draft.maxBytes}
                onChange={(event) => setDraft({ ...draft, maxBytes: event.target.value })}
              />
            </label>
            {draft.mode === "edit" ? (
              <label className="check-row">
                <input
                  type="checkbox"
                  checked={draft.enabled}
                  onChange={(event) => setDraft({ ...draft, enabled: event.target.checked })}
                />
                <span>启用</span>
              </label>
            ) : null}
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
