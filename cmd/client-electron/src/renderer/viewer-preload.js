(function bootstrapBitfsViewerPreload() {
  const { contextBridge, ipcRenderer, webFrame } = require("electron");
  const hashPattern = /^[0-9a-f]{64}$/;

  function trimString(raw) {
    return String(raw || "").trim();
  }

  function formatErrorDetail(message, stack, extraLines) {
    const lines = [];
    const normalizedMessage = trimString(message);
    const normalizedStack = trimString(stack);
    if (normalizedMessage) {
      lines.push(`message: ${normalizedMessage}`);
    }
    if (Array.isArray(extraLines)) {
      for (const line of extraLines) {
        const value = trimString(line);
        if (value) {
          lines.push(value);
        }
      }
    }
    if (normalizedStack && normalizedStack !== normalizedMessage) {
      lines.push("");
      lines.push(normalizedStack);
    }
    return lines.join("\n").trim();
  }

  function noteViewerRuntimeEvent(name, fields) {
    const normalizedName = trimString(name);
    if (!normalizedName) {
      return;
    }
    try {
      ipcRenderer.send("bitfs-shell:debug-log", {
        scope: "viewer.preload",
        event: normalizedName,
        fields: fields && typeof fields === "object" ? fields : {}
      });
    } catch {
      // 设计说明：
      // - 这里是纯观测日志，不允许影响真实页面逻辑；
      // - 如果 debug log 自身发不出去，直接静默即可。
    }
  }

  function reportViewerRuntimeError(title, message, detail) {
    try {
      emitViewerE2EEvent("viewer.preload.error_captured", {
        report_title: trimString(title) || "当前页面 JS 错误",
        report_message: trimString(message) || "viewer runtime error",
        report_page_url: trimString(window.location && window.location.href)
      });
      ipcRenderer.send("bitfs-shell:debug-log", {
        scope: "viewer.preload",
        event: "runtime_error_captured",
        fields: {
          title: trimString(title),
          message: trimString(message),
          page_url: trimString(window.location && window.location.href)
        }
      });
      ipcRenderer.send("bitfs-shell:report-error", {
        source: "viewer",
        title: trimString(title) || "当前页面 JS 错误",
        message: trimString(message) || "viewer runtime error",
        detail: trimString(detail),
        page_url: trimString(window.location && window.location.href),
        occurred_at_unix: Math.floor(Date.now() / 1000),
        can_stop_current_page: true
      });
    } catch {
      // 设计说明：
      // - viewer 错误上报失败时，不要在 guest 页面里继续抛二次异常；
      // - 原始错误至少还会留在 DevTools / 控制台里，壳层链路不应反过来放大故障。
    }
  }

  function installMainWorldRuntimeErrorHooks() {
    if (!webFrame || typeof webFrame.executeJavaScript !== "function") {
      noteViewerRuntimeEvent("main_world_runtime_hooks_skipped", {
        reason: "web_frame_unavailable"
      });
      return;
    }
    const code = `
      (() => {
        const reporter = window.__bitfsViewerRuntimeReporter;
        if (!reporter || typeof reporter.reportRuntimeError !== "function") {
          return "bridge_missing";
        }
        if (window.__bitfsViewerRuntimeHooksInstalled === true) {
          return "already_installed";
        }
        window.__bitfsViewerRuntimeHooksInstalled = true;
        if (typeof reporter.noteRuntimeEvent === "function") {
          reporter.noteRuntimeEvent("main_world_runtime_hooks_installed", {
            page_url: String(window.location && window.location.href || "")
          });
        }
        window.addEventListener("error", function handleMainWorldError(event) {
          if (event && typeof event.preventDefault === "function") {
            event.preventDefault();
          }
          const error = event && event.error;
          reporter.reportRuntimeError({
            title: "当前页面 JS 错误",
            message: String((event && event.message) || (error && error.message) || "viewer uncaught error"),
            detail: {
              message: String((event && event.message) || (error && error.message) || ""),
              stack: String((error && error.stack) || ""),
              filename: String((event && event.filename) || ""),
              lineno: Number(event && event.lineno || 0),
              colno: Number(event && event.colno || 0)
            }
          });
        }, true);
        window.addEventListener("unhandledrejection", function handleMainWorldUnhandledRejection(event) {
          if (event && typeof event.preventDefault === "function") {
            event.preventDefault();
          }
          const reason = event && event.reason;
          const message = reason instanceof Error
            ? String(reason.message || "")
            : String((reason && reason.message) || reason || "viewer unhandled rejection");
          const stack = reason instanceof Error ? String(reason.stack || "") : String((reason && reason.stack) || "");
          reporter.reportRuntimeError({
            title: "当前页面 Promise 未处理拒绝",
            message,
            detail: {
              message,
              stack,
              filename: "",
              lineno: 0,
              colno: 0
            }
          });
        });
        return "installed";
      })();
    `;
    noteViewerRuntimeEvent("main_world_runtime_hooks_install_start", {
      page_url: trimString(window.location && window.location.href)
    });
    void webFrame.executeJavaScript(code, false)
      .then((result) => {
        noteViewerRuntimeEvent("main_world_runtime_hooks_install_done", {
          result: trimString(result),
          page_url: trimString(window.location && window.location.href)
        });
      })
      .catch((error) => {
        noteViewerRuntimeEvent("main_world_runtime_hooks_install_failed", {
          message: trimString(error && error.message) || "main world runtime hook install failed",
          page_url: trimString(window.location && window.location.href)
        });
      });
  }

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

  function emitViewerE2EEvent(name, fields) {
    if (process.env.BITFS_ELECTRON_E2E !== "1") {
      return;
    }
    ipcRenderer.send("bitfs-viewer:e2e-event", {
      name: String(name || ""),
      fields: fields && typeof fields === "object" ? fields : {}
    });
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
      },
      tokens: {
        balances: {
          async list(query) {
            const payload = query && typeof query === "object" ? query : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-balances", {
              limit: Number(payload.limit || 0),
              offset: Number(payload.offset || 0),
              standard: String(payload.standard || "")
            });
          }
        },
        outputs: {
          async list(query) {
            const payload = query && typeof query === "object" ? query : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-outputs", {
              limit: Number(payload.limit || 0),
              offset: Number(payload.offset || 0),
              standard: String(payload.standard || ""),
              assetKey: String(payload.assetKey || payload.asset_key || "")
            });
          },
          async get(query) {
            const payload = query && typeof query === "object" ? query : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-output-detail", {
              standard: String(payload.standard || ""),
              utxoID: String(payload.utxoID || payload.utxo_id || ""),
              assetKey: String(payload.assetKey || payload.asset_key || "")
            });
          },
          events: {
            async list(query) {
              const payload = query && typeof query === "object" ? query : {};
              return ipcRenderer.invoke("bitfs-viewer:wallet-token-events", {
                limit: Number(payload.limit || 0),
                offset: Number(payload.offset || 0),
                standard: String(payload.standard || ""),
                utxoID: String(payload.utxoID || payload.utxo_id || ""),
                assetKey: String(payload.assetKey || payload.asset_key || "")
              });
            }
          }
        },
        send: {
          async preview(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-send-preview", {
              tokenStandard: String(payload.tokenStandard || payload.token_standard || ""),
              assetKey: String(payload.assetKey || payload.asset_key || ""),
              amountText: String(payload.amountText || payload.amount_text || ""),
              toAddress: String(payload.toAddress || payload.to_address || "")
            });
          },
          async sign(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-send-sign", {
              tokenStandard: String(payload.tokenStandard || payload.token_standard || ""),
              assetKey: String(payload.assetKey || payload.asset_key || ""),
              amountText: String(payload.amountText || payload.amount_text || ""),
              toAddress: String(payload.toAddress || payload.to_address || ""),
              expectedPreviewHash: String(payload.expectedPreviewHash || payload.expected_preview_hash || "")
            });
          },
          async submit(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-token-send-submit", {
              signedTxHex: String(payload.signedTxHex || payload.signed_tx_hex || "")
            });
          }
        }
      },
      ordinals: {
        async list(query) {
          const payload = query && typeof query === "object" ? query : {};
          return ipcRenderer.invoke("bitfs-viewer:wallet-ordinals", {
            limit: Number(payload.limit || 0),
            offset: Number(payload.offset || 0)
          });
        },
        async get(query) {
          const payload = query && typeof query === "object" ? query : {};
          return ipcRenderer.invoke("bitfs-viewer:wallet-ordinal-detail", {
            utxoID: String(payload.utxoID || payload.utxo_id || ""),
            assetKey: String(payload.assetKey || payload.asset_key || "")
          });
        },
        events: {
          async list(query) {
            const payload = query && typeof query === "object" ? query : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-ordinal-events", {
              limit: Number(payload.limit || 0),
              offset: Number(payload.offset || 0),
              utxoID: String(payload.utxoID || payload.utxo_id || ""),
              assetKey: String(payload.assetKey || payload.asset_key || "")
            });
          }
        },
        transfer: {
          async preview(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-ordinal-transfer-preview", {
              utxoID: String(payload.utxoID || payload.utxo_id || ""),
              assetKey: String(payload.assetKey || payload.asset_key || ""),
              toAddress: String(payload.toAddress || payload.to_address || "")
            });
          },
          async sign(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-ordinal-transfer-sign", {
              utxoID: String(payload.utxoID || payload.utxo_id || ""),
              assetKey: String(payload.assetKey || payload.asset_key || ""),
              toAddress: String(payload.toAddress || payload.to_address || ""),
              expectedPreviewHash: String(payload.expectedPreviewHash || payload.expected_preview_hash || "")
            });
          },
          async submit(input) {
            const payload = input && typeof input === "object" ? input : {};
            return ipcRenderer.invoke("bitfs-viewer:wallet-ordinal-transfer-submit", {
              signedTxHex: String(payload.signedTxHex || payload.signed_tx_hex || "")
            });
          }
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
  if (process.env.BITFS_ELECTRON_E2E === "1") {
    bitfsBridge.e2e = {
      enabled: true,
      emit(name, fields) {
        emitViewerE2EEvent(name, fields);
      }
    };
  }
  // 设计说明：
  // - 内容页运行在标准浏览器隔离上下文里，不再直接拿 Electron / Node；
  // - 这里只把稳定的 `window.bitfs` 能力桥暴露给页面，React/Vue 等传统框架仍按普通浏览器心智工作；
  // - 页面即便完全可信，也不应该拥有壳层 renderer 的宿主控制权。
  contextBridge.exposeInMainWorld("bitfs", bitfsBridge);
  // 设计说明：
  // - `window.bitfs` 是公开合同；错误上报桥不是产品 API，因此单独放在内部私有命名空间；
  // - viewer 真正抛错的是主世界脚本，而 preload 运行在隔离世界里，必须显式把主世界错误再送回壳层。
  contextBridge.exposeInMainWorld("__bitfsViewerRuntimeReporter", {
    reportRuntimeError(payload) {
      const detail = payload && typeof payload === "object" ? payload.detail : null;
      const message = trimString(payload && payload.message);
      const title = trimString(payload && payload.title) || "当前页面 JS 错误";
      reportViewerRuntimeError(
        title,
        message || "viewer runtime error",
        formatErrorDetail(
          message,
          trimString(detail && detail.stack),
          [
            trimString(detail && detail.filename) ? `file: ${trimString(detail.filename)}` : "",
            Number(detail && detail.lineno) > 0 ? `line: ${Number(detail.lineno)}` : "",
            Number(detail && detail.colno) > 0 ? `column: ${Number(detail.colno)}` : ""
          ]
        )
      );
    },
    noteRuntimeEvent(name, fields) {
      noteViewerRuntimeEvent(name, fields);
    }
  });
  installMainWorldRuntimeErrorHooks();

})();
