import type { BitfsBrowserRuntime } from "./browser_runtime";
import { debugLogger } from "./debug_logger";

const seedHashPattern = /^[0-9a-f]{64}$/;

export function createBitfsProtocolHandler(runtime: BitfsBrowserRuntime) {
  return async function handleBitfsProtocol(request: Request): Promise<Response> {
    const target = parseBitfsRequest(request.url);
    debugLogger.log("protocol", "request_start", {
      request_url: request.url,
      destination: String(request.destination || "document"),
      request_seed_hash: target
    });
    if (target === "") {
      debugLogger.log("protocol", "request_invalid", {
        request_url: request.url
      });
      return blockedResponse(request, "invalid bitfs uri");
    }
    try {
      const resolution = await runtime.resolveSeed(target);
      if (!resolution.allowed) {
        debugLogger.log("protocol", "request_requires_approval", {
          request_seed_hash: target,
          max_total_sat: resolution.maxTotalSat
        });
        return blockedResponse(request, `resource ${target} requires approval`);
      }
      const upstream = await runtime.fetchContent(target, resolution.maxTotalSat);
      if (!upstream.ok) {
        const text = await upstream.text();
        debugLogger.log("protocol", "upstream_error", {
          request_seed_hash: target,
          status: upstream.status,
          body: text
        });
        return blockedResponse(request, text || `upstream content fetch failed: ${upstream.status}`);
      }
      debugLogger.log("protocol", "upstream_ok", {
        request_seed_hash: target,
        status: upstream.status,
        content_type: String(upstream.headers.get("content-type") || "")
      });
      return runtime.maybePreplanDocument(target, upstream);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      debugLogger.log("protocol", "request_failed", {
        request_seed_hash: target,
        message
      });
      runtime.setLastError(message);
      return blockedResponse(request, message);
    }
  };
}

// 设计说明：
// - 这里把浏览器真实请求统一转成“预算判断 -> 内容获取”两段式；
// - 是否允许自动加载由 runtime 维护，协议层只负责执行结果；
// - 资源被预算闸门挡住时，壳层右侧资源面板负责展示价格和手动允许动作。
export function parseBitfsRequest(requestURL: string): string {
  let url: URL;
  try {
    url = new URL(requestURL);
  } catch {
    return "";
  }
  if (url.protocol !== "bitfs:") {
    return "";
  }
  const pathSeedHash = normalizeSeedHash(url.pathname.replace(/^\/+/, ""));
  if (pathSeedHash !== "") {
    return pathSeedHash;
  }
  return normalizeSeedHash(url.hostname);
}

function normalizeSeedHash(raw: string): string {
  const value = raw.trim().toLowerCase();
  if (!seedHashPattern.test(value)) {
    return "";
  }
  return value;
}

function blockedResponse(request: Request, message: string): Response {
  const destination = String(request.destination || "");
  debugLogger.log("protocol", "blocked_response", {
    request_url: request.url,
    request_seed_hash: parseBitfsRequest(String(request.url || "")),
    destination: destination || "document",
    message
  });
  if (destination === "style") {
    return new Response("/* bitfs resource blocked */\n", {
      status: 200,
      headers: { "content-type": "text/css; charset=utf-8" }
    });
  }
  if (destination === "script") {
    return new Response("// bitfs resource blocked\n", {
      status: 200,
      headers: { "content-type": "application/javascript; charset=utf-8" }
    });
  }
  if (destination === "image") {
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360"><rect width="100%" height="100%" fill="#dfeaf4"/><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" fill="#31516c" font-family="sans-serif" font-size="20">bitfs resource blocked</text></svg>`;
    return new Response(svg, {
      status: 200,
      headers: { "content-type": "image/svg+xml; charset=utf-8" }
    });
  }
  const requestURL = String(request.url || "").trim();
  const requestSeedHash = parseBitfsRequest(requestURL);
  const reportText = [
    "BitFS resource blocked",
    `request_url: ${requestURL || "-"}`,
    `request_seed_hash: ${requestSeedHash || "-"}`,
    `destination: ${destination || "document"}`,
    `message: ${String(message || "").trim() || "blocked"}`
  ].join("\n");
  const html = `<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>BitFS Resource Blocked</title>
    <style>
      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #edf3f8;
        color: #17324a;
        font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      }
      article {
        width: min(720px, calc(100vw - 32px));
        padding: 28px;
        border-radius: 24px;
        background: white;
        border: 1px solid #c6d5e2;
        box-shadow: 0 20px 48px rgba(18, 41, 63, 0.12);
      }
      h1 { margin: 0 0 14px; }
      p { line-height: 1.7; }
      code {
        font-family: "IBM Plex Mono", monospace;
        background: #eef5fb;
        border-radius: 8px;
        padding: 2px 6px;
      }
      .report-box {
        width: 100%;
        min-height: 160px;
        resize: vertical;
        margin-top: 16px;
        padding: 14px;
        border: 1px solid #c6d5e2;
        border-radius: 14px;
        background: #f8fbfe;
        color: #17324a;
        font: 13px/1.6 "IBM Plex Mono", monospace;
      }
      .actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 14px;
      }
      button {
        min-height: 40px;
        padding: 0 14px;
        border: 1px solid #b8c9d8;
        border-radius: 12px;
        background: linear-gradient(180deg, #ffffff, #eef4f8);
        color: #17324a;
        font: inherit;
        cursor: pointer;
      }
      button.primary {
        border-color: #2b62c9;
        background: linear-gradient(180deg, #2d6cdf, #1e57ba);
        color: #fff;
      }
      .tip {
        margin-top: 10px;
        color: #5d7386;
        font-size: 13px;
      }
    </style>
  </head>
  <body>
    <article>
      <h1>资源暂未放行</h1>
      <p>${escapeHTML(message)}</p>
      <p>如果这是一个超过静态阀值的资源，请回到浏览器壳右侧的资源面板，允许后再重新装载页面。</p>
      <p>当前协议只允许 <code>bitfs://&lt;seed_hash&gt;</code>，不会自动放行传统 URL。</p>
      <textarea id="error-report" class="report-box" readonly>${escapeHTML(reportText)}</textarea>
      <div class="actions">
        <button id="copy-report" class="primary" type="button">复制错误内容</button>
        <button id="select-report" type="button">全选文本</button>
      </div>
      <p id="copy-status" class="tip">如果系统剪贴板不可用，也可以直接选中文本区内容后复制。</p>
    </article>
    <script>
      (function () {
        const textarea = document.getElementById("error-report");
        const copyButton = document.getElementById("copy-report");
        const selectButton = document.getElementById("select-report");
        const status = document.getElementById("copy-status");
        if (!textarea || !copyButton || !selectButton || !status) {
          return;
        }
        function selectAll() {
          textarea.focus();
          textarea.select();
          textarea.setSelectionRange(0, textarea.value.length);
        }
        async function copyReport() {
          const text = textarea.value;
          try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
              await navigator.clipboard.writeText(text);
            } else {
              selectAll();
              if (!document.execCommand("copy")) {
                throw new Error("clipboard unavailable");
              }
            }
            status.textContent = "错误内容已复制到剪贴板。";
          } catch (_error) {
            selectAll();
            status.textContent = "自动复制失败，已帮你选中文本，可直接手动复制。";
          }
        }
        copyButton.addEventListener("click", function () {
          void copyReport();
        });
        selectButton.addEventListener("click", function () {
          selectAll();
          status.textContent = "错误内容已全选。";
        });
      })();
    </script>
  </body>
</html>`;
  return new Response(html, {
    status: 200,
    headers: { "content-type": "text/html; charset=utf-8" }
  });
}

function escapeHTML(src: string): string {
  return src
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}
