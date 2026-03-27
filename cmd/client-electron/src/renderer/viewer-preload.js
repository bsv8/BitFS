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
    if (typeof input.payment_mode === "string" && input.payment_mode.trim() !== "") {
      payload.payment_mode = input.payment_mode.trim();
    }
    if (typeof input.payment_quote_base64 === "string" && input.payment_quote_base64.trim() !== "") {
      payload.payment_quote_base64 = input.payment_quote_base64.trim();
    }
    return payload;
  }

  function normalizePeerCallOptions(options) {
    if (options == null) {
      return { onQuote: null };
    }
    if (typeof options !== "object" || Array.isArray(options)) {
      throw new Error("peer call options must be an object");
    }
    if (options.onQuote != null && typeof options.onQuote !== "function") {
      throw new Error("peer call onQuote must be a function");
    }
    return {
      onQuote: options.onQuote || null
    };
  }

  function normalizeLocatorInput(raw) {
    const locator = String(raw || "").trim();
    if (!locator) {
      throw new Error("bitfs locator is required");
    }
    return { locator };
  }

  function normalizeWalletBusinessInput(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw new Error("wallet business payload is required");
    }
    const signerPubkeyHex = String(input.signerPubkeyHex || input.signer_pubkey_hex || "").trim().toLowerCase();
    if (!signerPubkeyHex) {
      throw new Error("signer pubkey hex is required");
    }
    if (!Object.prototype.hasOwnProperty.call(input, "signedEnvelope") && !Object.prototype.hasOwnProperty.call(input, "signed_envelope")) {
      throw new Error("signed envelope is required");
    }
    return {
      signer_pubkey_hex: signerPubkeyHex,
      signed_envelope: Object.prototype.hasOwnProperty.call(input, "signedEnvelope") ? input.signedEnvelope : input.signed_envelope
    };
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
      async call(input, options) {
        const normalized = normalizePeerCallInput(input);
        const normalizedOptions = normalizePeerCallOptions(options);
        if (!normalizedOptions.onQuote) {
          return ipcRenderer.invoke("bitfs-viewer:peer-call", normalized);
        }
        const quoted = await ipcRenderer.invoke("bitfs-viewer:peer-call", {
          ...normalized,
          payment_mode: "quote"
        });
        if (String(quoted && quoted.code || "").trim().toUpperCase() !== "PAYMENT_QUOTED") {
          return quoted;
        }
        const option = Array.isArray(quoted && quoted.payment_options) && quoted.payment_options.length > 0 ? quoted.payment_options[0] : null;
        const approved = await normalizedOptions.onQuote({
          to: normalized.to,
          route: normalized.route,
          payment_scheme: String(quoted && quoted.payment_quote_scheme || option && option.scheme || ""),
          payment_domain: String(option && option.payment_domain || ""),
          quote_status: String(option && option.quote_status || ""),
          amount_satoshi: Number(option && option.amount_satoshi || 0),
          quantity: Number(option && option.service_quantity || 0),
          quantity_unit: String(option && option.service_quantity_unit || ""),
          quote: Object.prototype.hasOwnProperty.call(quoted || {}, "payment_quote") ? quoted.payment_quote : null,
          quoted_response: quoted
        });
        if (!approved) {
          return {
            ...quoted,
            ok: false,
            code: "QUOTE_REJECTED",
            message: "payment quote rejected by caller"
          };
        }
        return ipcRenderer.invoke("bitfs-viewer:peer-call", {
          ...normalized,
          payment_mode: "pay",
          payment_quote_base64: String(quoted && quoted.payment_quote_base64 || "")
        });
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
      async signBusinessRequest(input) {
        return ipcRenderer.invoke("bitfs-viewer:wallet-sign-business", normalizeWalletBusinessInput(input));
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
