import { BrowserWindow } from "electron";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { debugLogger } from "./debug_logger";
import { isTrustedNavigationURL } from "./navigation_guard";

export function createAppWindow(appRootDir: string): BrowserWindow {
  const preloadPath = path.join(appRootDir, "dist", "preload", "index.js");
  const viewerPreloadPath = path.join(appRootDir, "src", "renderer", "viewer-preload.js");
  const window = new BrowserWindow({
    width: 1440,
    height: 960,
    minWidth: 1080,
    minHeight: 720,
    backgroundColor: "#eaf1f7",
    autoHideMenuBar: true,
    title: "BitFS Browser Shell",
    webPreferences: {
      preload: preloadPath,
      nodeIntegration: true,
      contextIsolation: false,
      sandbox: false,
      webSecurity: true,
      webviewTag: true
    }
  });

  // 设计说明：
  // - 外层壳页面仍由本地可信 renderer 承载，负责地址栏、预算条、锁定层等系统级 UI；
  // - 真正的 bitfs 内容页则放进 webview，并在 will-attach-webview 里强制收回 Electron / Node 直连；
  // - 这样页面世界保留标准浏览器能力和 `window.bitfs`，但无法串到宿主壳。
  window.webContents.on("will-attach-webview", (event, preferences, params) => {
    const preload = normalizePreloadPath(String(params.preload || ""));
    const expectedPreload = path.resolve(viewerPreloadPath);
    debugLogger.log("app_window", "will_attach_webview", {
      preload,
      raw_preload: String(params.preload || ""),
      expected_preload: expectedPreload,
      src: String(params.src || "")
    });
    if (preload !== expectedPreload) {
      debugLogger.log("app_window", "webview_attach_blocked", {
        reason: "unexpected_preload",
        preload,
        raw_preload: String(params.preload || ""),
        expected_preload: expectedPreload,
        src: String(params.src || "")
      });
      event.preventDefault();
      return;
    }
    preferences.preload = viewerPreloadPath;
    preferences.nodeIntegration = false;
    preferences.nodeIntegrationInSubFrames = false;
    preferences.contextIsolation = true;
    preferences.sandbox = true;
    preferences.webSecurity = true;
    delete preferences.enableBlinkFeatures;
    delete preferences.disableBlinkFeatures;
    debugLogger.log("app_window", "webview_attach_allowed", {
      preload: viewerPreloadPath,
      src: String(params.src || "")
    });
  });
  window.webContents.on("will-navigate", (event, targetURL) => {
    if (isTrustedNavigationURL(targetURL)) {
      return;
    }
    event.preventDefault();
  });

  window.webContents.setWindowOpenHandler(({ url }) => {
    if (isTrustedNavigationURL(url)) {
      return { action: "allow" };
    }
    return { action: "deny" };
  });

  void window.loadFile(path.join(appRootDir, "src", "renderer", "index.html"));
  return window;
}

function normalizePreloadPath(raw: string): string {
  const value = String(raw || "").trim();
  if (value === "") {
    return "";
  }
  if (value.startsWith("file://")) {
    try {
      return path.resolve(fileURLToPath(value));
    } catch {
      return value;
    }
  }
  return path.resolve(value);
}
