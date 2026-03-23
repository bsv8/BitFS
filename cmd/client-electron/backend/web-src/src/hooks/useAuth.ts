/**
 * useAuth - 认证状态管理 Hook
 * 
 * 封装私钥认证相关的所有状态和操作：
 * - 检查密钥状态
 * - 创建/导入/导出私钥
 * - 解锁/锁定私钥
 * 
 * 这是新模型认证系统的核心 hook，基于私钥解锁而非 token。
 */

import { useState, useCallback } from "react";
import type { AuthState, KeyStatusResp } from "../types";
import {
  getKeyStatus,
  createKey as apiCreateKey,
  importKey as apiImportKey,
  exportKey as apiExportKey,
  unlockKey,
  lockKey,
} from "../api";

interface UseAuthReturn {
  // 状态
  auth: AuthState;
  authErr: string;
  loading: boolean;
  keyStatus: KeyStatusResp | null;
  
  // 输入状态
  passwordInput: string;
  confirmPassword: string;
  importCipher: string;
  view: "unlock" | "create" | "import" | "export";
  
  // 状态设置器
  setPasswordInput: (v: string) => void;
  setConfirmPassword: (v: string) => void;
  setImportCipher: (v: string) => void;
  setView: (v: "unlock" | "create" | "import" | "export") => void;
  
  // 操作
  checkKeyStatus: () => Promise<void>;
  createKey: (password: string, confirm: string) => Promise<void>;
  importKey: (cipherJson: string) => Promise<void>;
  unlock: (password: string) => Promise<void>;
  lock: () => Promise<void>;
  exportKey: () => Promise<string | null>;
  resetBusinessState: () => void;
}

/**
 * 创建初始的业务状态重置函数（占位符，实际由 App 组件提供）
 * 这是为了在 hook 内部能调用，但实际执行的是 App 组件传入的函数
 */
let _resetBusinessState: () => void = () => {};

export function setResetBusinessState(fn: () => void) {
  _resetBusinessState = fn;
}

export function useAuth(): UseAuthReturn {
  // 认证状态
  const [auth, setAuth] = useState<AuthState>("checking");
  const [authErr, setAuthErr] = useState("");
  const [loading, setLoading] = useState(false);
  
  // 密钥状态
  const [keyStatus, setKeyStatus] = useState<KeyStatusResp | null>(null);
  
  // 密码输入（用于解锁/创建）
  const [passwordInput, setPasswordInput] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  
  // 导入的密文
  const [importCipher, setImportCipher] = useState("");
  
  // 视图状态
  const [view, setView] = useState<"unlock" | "create" | "import" | "export">("unlock");

  /**
   * 检查密钥状态
   * GET /api/v1/key/status
   */
  const checkKeyStatus = useCallback(async (): Promise<void> => {
    setAuth("checking");
    setAuthErr("");
    try {
      const status = await getKeyStatus();
      setKeyStatus(status);
      
      if (!status.has_key) {
        setAuth("no_key");
        setView("create");
      } else if (!status.unlocked) {
        setAuth("locked");
        setView("unlock");
      } else {
        setAuth("unlocked");
      }
    } catch (e) {
      setAuth("error");
      setAuthErr(e instanceof Error ? e.message : "初始化状态检查失败");
    }
  }, []);

  /**
   * 创建新私钥
   * POST /api/v1/key/new
   */
  const createKey = useCallback(async (password: string, confirm: string): Promise<void> => {
    if (!password) {
      setAuthErr("请输入密码");
      return;
    }
    if (password !== confirm) {
      setAuthErr("两次输入的密码不一致");
      return;
    }
    if (password.length < 8) {
      setAuthErr("密码长度至少 8 位");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await apiCreateKey(password);
      // 创建成功后自动解锁
      await unlock(password);
    } catch (e) {
      setAuth("no_key");
      setAuthErr(e instanceof Error ? e.message : "创建私钥失败");
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * 导入私钥
   * POST /api/v1/key/import
   */
  const importKey = useCallback(async (cipherJson: string): Promise<void> => {
    if (!cipherJson.trim()) {
      setAuthErr("请输入密文 JSON");
      return;
    }
    
    let cipher: Record<string, unknown>;
    try {
      cipher = JSON.parse(cipherJson);
    } catch {
      setAuthErr("无效的 JSON 格式");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await apiImportKey(cipher);
      // 导入成功后切换到解锁视图
      setAuth("locked");
      setView("unlock");
      setAuthErr("私钥已导入，请输入密码解锁");
    } catch (e) {
      setAuth("no_key");
      setAuthErr(e instanceof Error ? e.message : "导入私钥失败");
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * 解锁私钥
   * POST /api/v1/key/unlock
   */
  const unlock = useCallback(async (password: string): Promise<void> => {
    if (!password) {
      setAuthErr("请输入密码");
      return;
    }
    
    setLoading(true);
    setAuthErr("");
    try {
      await unlockKey(password);
      setAuth("unlocked");
      setPasswordInput("");
    } catch (e) {
      setAuth("locked");
      setAuthErr(e instanceof Error ? e.message : "解锁失败，密码错误");
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * 锁定/注销
   * POST /api/v1/key/lock
   */
  const lock = useCallback(async (): Promise<void> => {
    setLoading(true);
    try {
      await lockKey();
      setAuth("locked");
      setView("unlock");
      setPasswordInput("");
      // 清空业务状态
      _resetBusinessState();
    } catch (e) {
      // 即使 API 失败，也切换到锁定状态
      setAuth("locked");
      setView("unlock");
      _resetBusinessState();
    } finally {
      setLoading(false);
    }
  }, []);

  /**
   * 导出私钥
   * GET /api/v1/key/export
   */
  const exportKey = useCallback(async (): Promise<string | null> => {
    try {
      const resp = await apiExportKey();
      return JSON.stringify(resp.cipher, null, 2);
    } catch (e) {
      setAuthErr(e instanceof Error ? e.message : "导出私钥失败");
      return null;
    }
  }, []);

  /**
   * 重置业务状态（由 App 组件注入实际的实现）
   */
  const resetBusinessState = useCallback(() => {
    _resetBusinessState();
  }, []);

  return {
    // 状态
    auth,
    authErr,
    loading,
    keyStatus,
    
    // 输入状态
    passwordInput,
    confirmPassword,
    importCipher,
    view,
    
    // 状态设置器
    setPasswordInput,
    setConfirmPassword,
    setImportCipher,
    setView,
    
    // 操作
    checkKeyStatus,
    createKey,
    importKey,
    unlock,
    lock,
    exportKey,
    resetBusinessState,
  };
}
