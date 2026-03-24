import path from "node:path";
import { pathToFileURL } from "node:url";

export type ShellAssetPaths = {
  viewerPreloadPath: string;
  settingsPreloadPath: string;
  settingsPageRootDir: string;
  settingsPageURL: string;
};

// 设计说明：
// - 壳层内部有两类受信 guest 页面：公开 bitfs 内容页与壳内设置页；
// - 这些路径需要在主进程、壳 renderer、webview attach guard 之间保持完全一致；
// - 因此统一集中到一个模块里解析，避免多人以后改路径时只改一半。
export function resolveShellAssetPaths(appRootDir: string): ShellAssetPaths {
  const viewerPreloadPath = path.join(appRootDir, "src", "renderer", "viewer-preload.js");
  const settingsPreloadPath = path.join(appRootDir, "src", "renderer", "settings-preload.js");
  const settingsPageRootDir = path.join(appRootDir, "settings-page", "dist-settings");
  const settingsPageURL = pathToFileURL(path.join(settingsPageRootDir, "index.html")).toString();
  return {
    viewerPreloadPath,
    settingsPreloadPath,
    settingsPageRootDir,
    settingsPageURL
  };
}
