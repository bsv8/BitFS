try {
  const { contextBridge, ipcRenderer } = require("electron");

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

  function reportSettingsRuntimeError(title, message, detail) {
    try {
      ipcRenderer.send("bitfs-shell:report-error", {
        source: "settings",
        title: trimString(title) || "设置页 JS 错误",
        message: trimString(message) || "settings runtime error",
        detail: trimString(detail),
        page_url: trimString(window.location && window.location.href),
        occurred_at_unix: Math.floor(Date.now() / 1000),
        can_stop_current_page: true
      });
    } catch {
      // 设计说明：
      // - 设置页已经是壳内受信页面，但错误上报本身仍然要 best effort；
      // - 这里不能为了再报一次错，把设置页彻底锁死。
    }
  }

  window.addEventListener("error", (event) => {
    event.preventDefault();
    const error = event && event.error;
    reportSettingsRuntimeError(
      "设置页 JS 错误",
      trimString(event && event.message) || trimString(error && error.message) || "settings uncaught error",
      formatErrorDetail(
        trimString(event && event.message) || trimString(error && error.message),
        trimString(error && error.stack),
        [
          trimString(event && event.filename) ? `file: ${trimString(event.filename)}` : "",
          Number(event && event.lineno) > 0 ? `line: ${Number(event.lineno)}` : "",
          Number(event && event.colno) > 0 ? `column: ${Number(event.colno)}` : ""
        ]
      )
    );
  });

  window.addEventListener("unhandledrejection", (event) => {
    event.preventDefault();
    const reason = event && event.reason;
    const message = reason instanceof Error
      ? trimString(reason.message)
      : trimString(reason && reason.message) || trimString(reason) || "settings unhandled rejection";
    const stack = reason instanceof Error ? trimString(reason.stack) : trimString(reason && reason.stack);
    reportSettingsRuntimeError(
      "设置页 Promise 未处理拒绝",
      message,
      formatErrorDetail(message, stack, [])
    );
  });

  function normalizeTopics(raw) {
    const values = Array.isArray(raw) ? raw : [raw];
    const topics = new Set();
    for (const value of values) {
      const topic = String(value || "").trim();
      if (topic) {
        topics.add(topic);
      }
    }
    return topics;
  }

  function onShellState(listener) {
    const handler = (_event, state) => {
      listener(state);
    };
    ipcRenderer.on("bitfs-shell:state", handler);
    return () => {
      ipcRenderer.removeListener("bitfs-shell:state", handler);
    };
  }

  function onShellEvent(topics, listener) {
    const allowedTopics = normalizeTopics(topics);
    const handler = (_event, runtimeEvent) => {
      if (allowedTopics.size > 0 && !allowedTopics.has(String(runtimeEvent && runtimeEvent.topic || ""))) {
        return;
      }
      listener(runtimeEvent);
    };
    ipcRenderer.on("bitfs-shell:event", handler);
    return () => {
      ipcRenderer.removeListener("bitfs-shell:event", handler);
    };
  }

  const settingsBridge = {
    getShellState() {
      return ipcRenderer.invoke("bitfs-shell:get-state");
    },
    getWalletSummary() {
      return ipcRenderer.invoke("bitfs-shell:wallet-summary");
    },
    setUserHomepage(seedHash) {
      return ipcRenderer.invoke("bitfs-shell:set-user-homepage", { seedHash });
    },
    clearUserHomepage() {
      return ipcRenderer.invoke("bitfs-shell:clear-user-homepage");
    },
    lock() {
      return ipcRenderer.invoke("bitfs-shell:lock");
    },
    restartBackend() {
      return ipcRenderer.invoke("bitfs-shell:restart-backend");
    },
    exportKeyFile() {
      return ipcRenderer.invoke("bitfs-shell:export-key-file");
    },
    request(method, path, body) {
      return ipcRenderer.invoke("bitfs-settings:request", { method, path, body });
    },
    pickDirectory() {
      return ipcRenderer.invoke("bitfs-settings:pick-directory");
    },
    uploadStaticFile(targetDir, overwrite) {
      return ipcRenderer.invoke("bitfs-settings:upload-static-file", { targetDir, overwrite });
    },
    onShellState(listener) {
      if (typeof listener !== "function") {
        throw new Error("state listener is required");
      }
      return onShellState(listener);
    },
    events: {
      subscribe(topics, listener) {
        if (typeof listener !== "function") {
          throw new Error("event listener is required");
        }
        return onShellEvent(topics, listener);
      }
    }
  };

  // 设计说明：
  // - 设置页是壳内受信页面，但仍然不直接暴露 ipcRenderer；
  // - 这里把“managed API 白名单访问”和“桌面原生选择器能力”收口成稳定桥；
  // - 这样设置页仍然保留页面心智，而权限边界继续由 preload 与主进程把住。
  contextBridge.exposeInMainWorld("bitfsSettings", settingsBridge);
} catch (error) {
  console.error("settings preload init failed:", error);
}
