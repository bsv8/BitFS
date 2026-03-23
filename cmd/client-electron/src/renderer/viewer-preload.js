(function bootstrapBitfsViewerPreload() {
  const { contextBridge, ipcRenderer } = require("electron");
  const hashPattern = /^[0-9a-f]{64}$/;

  function normalizeHash(raw) {
    const value = String(raw || "").trim().toLowerCase();
    if (!hashPattern.test(value)) {
      return "";
    }
    return value;
  }

  function resolveBitfsRef(raw) {
    const value = String(raw || "").trim();
    if (!value) {
      throw new Error("bitfs target is required");
    }
    if (value.startsWith("bitfs://")) {
      return value;
    }
    if (value.startsWith("/")) {
      const hash = normalizeHash(value.slice(1));
      if (!hash) {
        throw new Error("invalid bitfs hash ref");
      }
      return `bitfs://${hash}`;
    }
    const hash = normalizeHash(value);
    if (hash) {
      return `bitfs://${hash}`;
    }
    throw new Error("invalid bitfs ref");
  }

  function onShellState(listener) {
    const handler = function handleShellState(_event, state) {
      listener(state);
    };
    ipcRenderer.on("bitfs-shell:state", handler);
    return function unsubscribe() {
      ipcRenderer.removeListener("bitfs-shell:state", handler);
    };
  }

  const bitfsBridge = {
    version: "0.1.0",
    trustedProtocol: "bitfs://",
    navigation: {
      open(raw) {
        window.location.assign(resolveBitfsRef(raw));
      },
      reload() {
        window.location.reload();
      }
    },
    resource: {
      resolve(raw) {
        return resolveBitfsRef(raw);
      },
      async plan(rawRefs) {
        const refs = Array.isArray(rawRefs) ? rawRefs : [rawRefs];
        return ipcRenderer.invoke("bitfs-shell:plan-resources", {
          refs,
          baseURL: window.location.href
        });
      }
    },
    files: {
      async plan(rawRefs) {
        const refs = Array.isArray(rawRefs) ? rawRefs : [rawRefs];
        return ipcRenderer.invoke("bitfs-shell:plan-resources", {
          refs,
          baseURL: window.location.href
        });
      },
      async status(raw) {
        return ipcRenderer.invoke("bitfs-shell:file-status", {
          ref: resolveBitfsRef(raw),
          baseURL: window.location.href
        });
      },
      async ensure(raw, options) {
        const opts = options || {};
        return ipcRenderer.invoke("bitfs-shell:ensure-resource", {
          ref: resolveBitfsRef(raw),
          baseURL: window.location.href,
          maxTotalSat: Number(opts.maxTotalSat || 0)
        });
      }
    },
    client: {
      async info() {
        return ipcRenderer.invoke("bitfs-shell:client-info");
      },
      async getStatus() {
        const state = await ipcRenderer.invoke("bitfs-shell:get-state");
        return {
          trusted_protocol: "bitfs://",
          current_url: state.currentURL,
          static_single_max_sat: state.staticSingleMaxSat,
          static_page_max_sat: state.staticPageMaxSat,
          auto_spent_sat: state.autoSpentSat,
          pending_count: Array.isArray(state.pending) ? state.pending.length : 0,
          last_error: state.lastError || "",
          backend_phase: state.backend?.phase || "",
          backend_has_key: Boolean(state.backend?.hasKey),
          backend_unlocked: Boolean(state.backend?.unlocked),
          client_api_base: state.backend?.apiBase || state.clientAPIBase || "",
          backend_default_home_seed_hash: state.backend?.defaultHomeSeedHash || "",
          user_home_seed_hash: state.userHomeSeedHash || ""
        };
      },
      async setStaticBudget(singleMaxSat, pageMaxSat) {
        return ipcRenderer.invoke("bitfs-shell:set-budget", {
          singleMaxSat: Number(singleMaxSat || 0),
          pageMaxSat: Number(pageMaxSat || 0)
        });
      },
      async approvePending() {
        return ipcRenderer.invoke("bitfs-shell:approve-pending");
      },
      onState(listener) {
        if (typeof listener !== "function") {
          throw new Error("state listener is required");
        }
        return onShellState(listener);
      }
    },
    wallet: {
      async summary() {
        return ipcRenderer.invoke("bitfs-shell:wallet-summary");
      }
    },
    live: {
      async latest(streamID) {
        return ipcRenderer.invoke("bitfs-shell:live-latest", {
          streamID: String(streamID || "")
        });
      },
      async plan(streamID, haveSegmentIndex) {
        return ipcRenderer.invoke("bitfs-shell:live-plan", {
          streamID: String(streamID || ""),
          haveSegmentIndex: Number(haveSegmentIndex || 0)
        });
      }
    },
    events: {
      subscribe(name, handler) {
        if (name !== "state") {
          throw new Error("unsupported event");
        }
        if (typeof handler !== "function") {
          throw new Error("event handler is required");
        }
        return onShellState(handler);
      }
    }
  };
  // 设计说明：
  // - 内容页运行在标准浏览器隔离上下文里，不再直接拿 Electron / Node；
  // - 这里只把稳定的 `window.bitfs` 能力桥暴露给页面，React/Vue 等传统框架仍按普通浏览器心智工作；
  // - 页面即便完全可信，也不应该拥有壳层 renderer 的宿主控制权。
  contextBridge.exposeInMainWorld("bitfs", bitfsBridge);

})();
