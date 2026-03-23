import { ipcRenderer } from "electron";
import type { KeyFileActionResult, ShellState } from "../shared/shell_contract";

type Unsubscribe = () => void;

type BitfsShellBridge = {
  getState(): Promise<ShellState>;
  open(raw: string): Promise<string>;
  setBudget(singleMaxSat: number, pageMaxSat: number): Promise<ShellState>;
  approveResource(seedHash: string): Promise<ShellState>;
  createKey(password: string): Promise<ShellState>;
  importKeyFile(): Promise<KeyFileActionResult>;
  exportKeyFile(): Promise<KeyFileActionResult>;
  unlock(password: string): Promise<ShellState>;
  lock(): Promise<ShellState>;
  restartBackend(): Promise<ShellState>;
  setUserHomepage(seedHash: string): Promise<ShellState>;
  clearUserHomepage(): Promise<ShellState>;
  setSidebarLayout(sidebarWidthPx?: number, activePanel?: string): Promise<ShellState>;
  getWalletSummary(): Promise<Record<string, unknown>>;
  noteNavigation(url: string): void;
  debugLog(scope: string, event: string, fields?: Record<string, unknown>): void;
  onState(listener: (state: ShellState) => void): Unsubscribe;
};

declare global {
  interface Window {
    bitfsShell: BitfsShellBridge;
  }
}

// 设计说明：
// - 壳页面和真正的 bitfs 内容页职责不同，壳页面只关心地址栏、右侧面板和浏览状态；
// - preload 暴露稳定的 `window.bitfsShell`，避免 renderer 直接依赖 ipc 细节；
// - 内容页自己的 `window.bitfs` 由 webview preload 提供，和壳页面桥分开更清楚。
window.bitfsShell = {
  getState() {
    return ipcRenderer.invoke("bitfs-shell:get-state") as Promise<ShellState>;
  },
  open(raw: string) {
    return ipcRenderer.invoke("bitfs-shell:open", raw) as Promise<string>;
  },
  setBudget(singleMaxSat: number, pageMaxSat: number) {
    return ipcRenderer.invoke("bitfs-shell:set-budget", { singleMaxSat, pageMaxSat }) as Promise<ShellState>;
  },
  approveResource(seedHash: string) {
    return ipcRenderer.invoke("bitfs-shell:approve-resource", { seedHash }) as Promise<ShellState>;
  },
  createKey(password: string) {
    return ipcRenderer.invoke("bitfs-shell:create-key", { password }) as Promise<ShellState>;
  },
  importKeyFile() {
    return ipcRenderer.invoke("bitfs-shell:import-key-file") as Promise<KeyFileActionResult>;
  },
  exportKeyFile() {
    return ipcRenderer.invoke("bitfs-shell:export-key-file") as Promise<KeyFileActionResult>;
  },
  unlock(password: string) {
    return ipcRenderer.invoke("bitfs-shell:unlock", { password }) as Promise<ShellState>;
  },
  lock() {
    return ipcRenderer.invoke("bitfs-shell:lock") as Promise<ShellState>;
  },
  restartBackend() {
    return ipcRenderer.invoke("bitfs-shell:restart-backend") as Promise<ShellState>;
  },
  setUserHomepage(seedHash: string) {
    return ipcRenderer.invoke("bitfs-shell:set-user-homepage", { seedHash }) as Promise<ShellState>;
  },
  clearUserHomepage() {
    return ipcRenderer.invoke("bitfs-shell:clear-user-homepage") as Promise<ShellState>;
  },
  setSidebarLayout(sidebarWidthPx?: number, activePanel?: string) {
    return ipcRenderer.invoke("bitfs-shell:set-sidebar-layout", {
      sidebarWidthPx,
      activePanel
    }) as Promise<ShellState>;
  },
  getWalletSummary() {
    return ipcRenderer.invoke("bitfs-shell:wallet-summary") as Promise<Record<string, unknown>>;
  },
  noteNavigation(url: string) {
    ipcRenderer.send("bitfs-shell:did-navigate", url);
  },
  debugLog(scope: string, event: string, fields?: Record<string, unknown>) {
    ipcRenderer.send("bitfs-shell:debug-log", { scope, event, fields });
  },
  onState(listener: (state: ShellState) => void): Unsubscribe {
    const handler = (_event: unknown, state: ShellState) => {
      listener(state);
    };
    ipcRenderer.on("bitfs-shell:state", handler);
    return () => {
      ipcRenderer.removeListener("bitfs-shell:state", handler);
    };
  }
};

export {};
