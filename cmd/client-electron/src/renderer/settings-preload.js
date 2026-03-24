try {
  const { contextBridge, ipcRenderer } = require("electron");

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
