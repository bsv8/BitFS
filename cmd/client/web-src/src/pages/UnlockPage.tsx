/**
 * UnlockPage - 私钥解锁/创建/导入/导出页面
 * 
 * 这是新模型认证系统的入口页面，根据 auth 状态显示不同的视图：
 * - checking: 系统初始化中
 * - error: 错误状态，显示错误信息和重试按钮
 * - no_key: 无密钥状态，提供创建或导入私钥的选项
 * - locked: 锁定状态，需要输入密码解锁，也可导出私钥备份
 */

import React, { useState } from "react";
import type { AuthState, KeyStatusResp } from "../types";

// ========== 组件 Props 定义 ==========

interface UnlockPageProps {
  /** 当前认证状态 */
  auth: AuthState;
  /** 认证错误信息 */
  authErr: string;
  /** 是否正在加载中 */
  loading: boolean;
  /** 密钥状态信息 */
  keyStatus: KeyStatusResp | null;
  /** 当前视图 */
  view: "unlock" | "create" | "import" | "export";
  /** 密码输入值 */
  passwordInput: string;
  /** 确认密码输入值 */
  confirmPassword: string;
  /** 导入的密文 JSON */
  importCipher: string;

  // 状态设置器
  setPasswordInput: (v: string) => void;
  setConfirmPassword: (v: string) => void;
  setImportCipher: (v: string) => void;
  setView: (v: "unlock" | "create" | "import" | "export") => void;

  // 动作
  /** 检查密钥状态 */
  checkKeyStatus: () => Promise<void>;
  /** 创建新私钥 */
  createKey: (password: string, confirm: string) => Promise<void>;
  /** 导入私钥 */
  importKey: (cipherJson: string) => Promise<void>;
  /** 解锁私钥 */
  unlock: (password: string) => Promise<void>;
  /** 导出私钥，返回密文 JSON */
  exportKey: () => Promise<string | null>;
}

// ========== 组件实现 ==========

export default function UnlockPage({
  auth,
  authErr,
  loading,
  keyStatus,
  view,
  passwordInput,
  confirmPassword,
  importCipher,
  setPasswordInput,
  setConfirmPassword,
  setImportCipher,
  setView,
  checkKeyStatus,
  createKey,
  importKey,
  unlock,
  exportKey,
}: UnlockPageProps) {
  // 本地状态：导出的密文（用于 export 视图显示）
  const [exportedCipher, setExportedCipher] = useState("");

  // 处理获取并复制密文
  const handleExportAndCopy = async () => {
    const cipher = await exportKey();
    if (cipher) {
      setExportedCipher(cipher);
      // 复制到剪贴板
      navigator.clipboard.writeText(cipher);
      alert("密文已复制到剪贴板");
    }
  };

  // 处理点击文本域获取密文
  const handleExportTextareaClick = async () => {
    if (!exportedCipher) {
      const cipher = await exportKey();
      if (cipher) {
        setExportedCipher(cipher);
      }
    }
  };

  return (
    <div className="login-shell">
      <div className="login-card">
        {/* 应用标题 */}
        <p className="eyebrow">
          BitFS Client {keyStatus?.appname ? `· ${keyStatus.appname}` : ""}
        </p>

        {/* ========== 检查中状态 ========== */}
        {auth === "checking" && (
          <>
            <h1>系统初始化中...</h1>
            <p className="desc">正在检查密钥状态，请稍候</p>
          </>
        )}

        {/* ========== 错误状态 ========== */}
        {auth === "error" && (
          <>
            <h1>⚠️ 出错了</h1>
            <p className="desc">{authErr || "系统初始化失败"}</p>
            <button className="btn" onClick={() => void checkKeyStatus()}>
              重试
            </button>
          </>
        )}

        {/* ========== 无密钥状态 - 创建或导入 ========== */}
        {auth === "no_key" && (
          <>
            {/* 创建私钥视图 */}
            {view === "create" && (
              <>
                <h1>🔐 创建新私钥</h1>
                <p className="desc">
                  系统中没有私钥，请设置密码创建新私钥。
                  <br />
                  密码将用于加密私钥，请妥善保管。
                </p>
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: 12,
                    marginTop: 20,
                  }}
                >
                  <input
                    className="input"
                    type="password"
                    value={passwordInput}
                    onChange={(e) => setPasswordInput(e.target.value)}
                    placeholder="设置密码（至少8位）"
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        void createKey(passwordInput, confirmPassword);
                      }
                    }}
                  />
                  <input
                    className="input"
                    type="password"
                    value={confirmPassword}
                    onChange={(e) => setConfirmPassword(e.target.value)}
                    placeholder="确认密码"
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        void createKey(passwordInput, confirmPassword);
                      }
                    }}
                  />
                  <button
                    className="btn"
                    onClick={() => void createKey(passwordInput, confirmPassword)}
                    disabled={loading}
                  >
                    创建并解锁
                  </button>
                  <button
                    className="btn btn-light"
                    onClick={() => {
                      setView("import");
                    }}
                  >
                    已有私钥？点击导入
                  </button>
                </div>
              </>
            )}

            {/* 导入私钥视图 */}
            {view === "import" && (
              <>
                <h1>📥 导入私钥</h1>
                <p className="desc">
                  请粘贴导出的密文 JSON，导入后原密码仍然有效。
                </p>
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: 12,
                    marginTop: 20,
                  }}
                >
                  <textarea
                    className="input"
                    style={{
                      minHeight: 120,
                      fontFamily: "monospace",
                      fontSize: 12,
                    }}
                    value={importCipher}
                    onChange={(e) => setImportCipher(e.target.value)}
                    placeholder={`{\n  "version": 1,\n  "cipher": "..."\n}`}
                  />
                  <button
                    className="btn"
                    onClick={() => void importKey(importCipher)}
                    disabled={loading}
                  >
                    导入私钥
                  </button>
                  <button
                    className="btn btn-light"
                    onClick={() => {
                      setView("create");
                    }}
                  >
                    返回创建新私钥
                  </button>
                </div>
              </>
            )}
          </>
        )}

        {/* ========== 锁定状态 - 解锁或导出 ========== */}
        {auth === "locked" && (
          <>
            {/* 解锁视图 */}
            {view === "unlock" && (
              <>
                <h1>🔒 系统已锁定</h1>
                <p className="desc">
                  私钥已加密存储，请输入密码解锁以继续使用。
                </p>
                <div className="login-row" style={{ marginTop: 20 }}>
                  <input
                    className="input"
                    type="password"
                    value={passwordInput}
                    onChange={(e) => setPasswordInput(e.target.value)}
                    placeholder="输入密码"
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        void unlock(passwordInput);
                      }
                    }}
                  />
                  <button
                    className="btn"
                    onClick={() => void unlock(passwordInput)}
                    disabled={loading}
                  >
                    解锁
                  </button>
                </div>
                <div
                  style={{
                    marginTop: 16,
                    display: "flex",
                    gap: 8,
                    justifyContent: "center",
                  }}
                >
                  <button
                    className="btn btn-light"
                    onClick={() => {
                      setView("export");
                      setExportedCipher("");
                    }}
                  >
                    导出私钥备份
                  </button>
                </div>
              </>
            )}

            {/* 导出视图 */}
            {view === "export" && (
              <>
                <h1>📤 导出私钥</h1>
                <p className="desc">
                  以下是加密后的私钥密文，请妥善保存。
                  <br />
                  导出操作不会删除原私钥。
                </p>
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: 12,
                    marginTop: 20,
                  }}
                >
                  <textarea
                    className="input"
                    style={{
                      minHeight: 120,
                      fontFamily: "monospace",
                      fontSize: 12,
                    }}
                    readOnly
                    value={exportedCipher || "点击获取密文..."}
                    onClick={handleExportTextareaClick}
                  />
                  <button
                    className="btn"
                    onClick={handleExportAndCopy}
                    disabled={loading}
                  >
                    获取并复制密文
                  </button>
                  <button
                    className="btn btn-light"
                    onClick={() => {
                      setView("unlock");
                      setExportedCipher("");
                    }}
                  >
                    返回解锁
                  </button>
                </div>
              </>
            )}
          </>
        )}

        {/* 错误信息显示 */}
        {authErr ? (
          <p className="err" style={{ marginTop: 16 }}>
            {authErr}
          </p>
        ) : null}
      </div>
    </div>
  );
}
