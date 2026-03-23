const trustedNavigationProtocols = new Set(["bitfs:", "file:"]);
const trustedRequestProtocols = new Set(["bitfs:", "file:", "data:", "blob:", "devtools:"]);

// 设计说明：
// - BitFS 内部世界只信任 `bitfs://`；
// - 本地壳页面启动时会先走 `file://`，便于开发期直接加载本地静态资源；
// - `http/https` 对这个应用视为不可信外部世界，统一拒绝。
export function isTrustedNavigationURL(rawURL: string): boolean {
  let url: URL;
  try {
    url = new URL(rawURL);
  } catch {
    return false;
  }
  return trustedNavigationProtocols.has(url.protocol);
}

// 设计说明：
// - 页面世界虽然默认可信，但外部协议仍然要在主进程硬拦；
// - `data/blob` 这类浏览器内部资源保留，不把正常前端能力一起误伤；
// - `http/https` 不论是顶层导航还是子资源请求，都不允许进入应用世界。
export function isTrustedRequestURL(rawURL: string): boolean {
  let url: URL;
  try {
    url = new URL(rawURL);
  } catch {
    return false;
  }
  return trustedRequestProtocols.has(url.protocol);
}
