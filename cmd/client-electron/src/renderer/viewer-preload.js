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
    ipcRenderer.on("bitfs-viewer:state", handler);
    return function unsubscribe() {
      ipcRenderer.removeListener("bitfs-viewer:state", handler);
    };
  }

  function onViewerEvent(topics, listener) {
    const allowedTopics = normalizeTopics(topics);
    const handler = function handleViewerEvent(_event, runtimeEvent) {
      if (allowedTopics.size > 0 && !allowedTopics.has(String(runtimeEvent && runtimeEvent.topic || ""))) {
        return;
      }
      listener(runtimeEvent);
    };
    ipcRenderer.on("bitfs-viewer:event", handler);
    return function unsubscribe() {
      ipcRenderer.removeListener("bitfs-viewer:event", handler);
    };
  }

  function normalizeTopics(raw) {
    const values = Array.isArray(raw) ? raw : [raw];
    const out = new Set();
    for (const value of values) {
      const topic = String(value || "").trim();
      if (topic) {
        out.add(topic);
      }
    }
    return out;
  }

  function normalizeTarget(raw) {
    const value = String(raw || "").trim();
    if (!value) {
      throw new Error("bitfs target is required");
    }
    return value;
  }

  function normalizeRoute(raw, allowEmpty) {
    const value = String(raw || "").trim();
    if (!value && !allowEmpty) {
      throw new Error("bitfs route is required");
    }
    return value;
  }

  function normalizePeerCallInput(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw new Error("peer call payload is required");
    }
    const payload = {
      to: normalizeTarget(input.to),
      route: normalizeRoute(input.route, false),
      content_type: String(input.contentType || input.content_type || "").trim()
    };
    if (!payload.content_type) {
      throw new Error("bitfs contentType is required");
    }
    if (input.body instanceof Uint8Array) {
      payload.body_base64 = Buffer.from(input.body).toString("base64");
      return payload;
    }
    if (input.body instanceof ArrayBuffer) {
      payload.body_base64 = Buffer.from(new Uint8Array(input.body)).toString("base64");
      return payload;
    }
    if (input.body != null) {
      payload.body = input.body;
    }
    return payload;
  }

  function normalizeLocatorInput(raw) {
    const locator = String(raw || "").trim();
    if (!locator) {
      throw new Error("bitfs locator is required");
    }
    return { locator };
  }

  const bitfsBridge = {
    version: "0.2.0",
    trustedProtocol: "bitfs://",
    navigation: {
      open(raw) {
        window.location.assign(resolveBitfsRef(raw));
      },
      reload() {
        window.location.reload();
      }
    },
    locator: {
      async resolve(raw) {
        return ipcRenderer.invoke("bitfs-viewer:locator-resolve", normalizeLocatorInput(raw));
      }
    },
    peer: {
      async call(input) {
        return ipcRenderer.invoke("bitfs-viewer:peer-call", normalizePeerCallInput(input));
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
        return ipcRenderer.invoke("bitfs-viewer:client-info");
      },
      async getStatus() {
        return ipcRenderer.invoke("bitfs-viewer:client-status");
      },
      async setStaticBudget(singleMaxSat, pageMaxSat) {
        return ipcRenderer.invoke("bitfs-shell:set-budget", {
          singleMaxSat: Number(singleMaxSat || 0),
          pageMaxSat: Number(pageMaxSat || 0)
        });
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
        return ipcRenderer.invoke("bitfs-viewer:wallet-summary");
      },
      async addresses() {
        return ipcRenderer.invoke("bitfs-viewer:wallet-addresses");
      },
      history: {
        async list(query) {
          const payload = query && typeof query === "object" ? query : {};
          return ipcRenderer.invoke("bitfs-viewer:wallet-history", {
            limit: Number(payload.limit || 0),
            offset: Number(payload.offset || 0),
            direction: String(payload.direction || "")
          });
        }
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
        if (typeof handler !== "function") {
          throw new Error("event handler is required");
        }
        if (name === "state") {
          return onShellState(handler);
        }
        return onViewerEvent(name, handler);
      }
    }
  };
  // 设计说明：
  // - 内容页运行在标准浏览器隔离上下文里，不再直接拿 Electron / Node；
  // - 这里只把稳定的 `window.bitfs` 能力桥暴露给页面，React/Vue 等传统框架仍按普通浏览器心智工作；
  // - 页面即便完全可信，也不应该拥有壳层 renderer 的宿主控制权。
  contextBridge.exposeInMainWorld("bitfs", bitfsBridge);

})();
