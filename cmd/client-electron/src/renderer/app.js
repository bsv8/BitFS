(function bootstrapRenderer() {
  const bridge = window.bitfsShell;
  const browserShell = document.getElementById("browser-shell");
  const addressForm = document.getElementById("address-form");
  const addressInput = document.getElementById("address-input");
  const backButton = document.getElementById("back-button");
  const forwardButton = document.getElementById("forward-button");
  const reloadButton = document.getElementById("reload-button");
  const goHomeButton = document.getElementById("go-home-button");
  const openButton = document.getElementById("open-button");
  const homeButton = document.getElementById("home-button");
  const lockButton = document.getElementById("lock-button");
  const budgetForm = document.getElementById("budget-form");
  const singleBudgetInput = document.getElementById("single-budget-input");
  const pageBudgetInput = document.getElementById("page-budget-input");
  const currentURL = document.getElementById("current-url");
  const currentRootSeed = document.getElementById("current-root-seed");
  const pendingCount = document.getElementById("pending-count");
  const clientAPIBase = document.getElementById("client-api-base");
  const homeSource = document.getElementById("home-source");
  const autoSpentTotal = document.getElementById("auto-spent-total");
  const resourceTotalBadge = document.getElementById("resource-total-badge");
  const resourcePendingBadge = document.getElementById("resource-pending-badge");
  const resourceListNote = document.getElementById("resource-list-note");
  const resourceList = document.getElementById("resource-list");
  const panelTabResources = document.getElementById("panel-tab-resources");
  const panelTabWallet = document.getElementById("panel-tab-wallet");
  const panelResources = document.getElementById("panel-resources");
  const panelWallet = document.getElementById("panel-wallet");
  const sidebarResizeHandle = document.getElementById("sidebar-resize-handle");
  const sidebarDragOverlay = document.getElementById("sidebar-drag-overlay");
  const walletRefreshButton = document.getElementById("wallet-refresh-button");
  const walletSummaryNote = document.getElementById("wallet-summary-note");
  const walletDiagnosticNote = document.getElementById("wallet-diagnostic-note");
  const walletCopyDiagnosticsButton = document.getElementById("wallet-copy-diagnostics-button");
  const walletAddress = document.getElementById("wallet-address");
  const walletBalance = document.getElementById("wallet-balance");
  const walletBalanceSource = document.getElementById("wallet-balance-source");
  const walletLedgerNet = document.getElementById("wallet-ledger-net");
  const walletTotalIn = document.getElementById("wallet-total-in");
  const walletTotalOut = document.getElementById("wallet-total-out");
  const walletBackendPhase = document.getElementById("wallet-backend-phase");
  const walletClientAPIBase = document.getElementById("wallet-client-api-base");
  const walletChainType = document.getElementById("wallet-chain-type");
  const walletChainBaseURL = document.getElementById("wallet-chain-base-url");
  const walletSummaryUpdatedAt = document.getElementById("wallet-summary-updated-at");
  const walletRuntimeStartedAt = document.getElementById("wallet-runtime-started-at");
  const walletSyncUpdatedAt = document.getElementById("wallet-sync-updated-at");
  const walletSyncStale = document.getElementById("wallet-sync-stale");
  const walletSyncStaleReason = document.getElementById("wallet-sync-stale-reason");
  const walletSyncTrigger = document.getElementById("wallet-sync-trigger");
  const walletSyncDuration = document.getElementById("wallet-sync-duration");
  const walletSyncSchedulerStatus = document.getElementById("wallet-sync-scheduler-status");
  const walletSyncSchedulerInFlight = document.getElementById("wallet-sync-scheduler-in-flight");
  const walletSyncSchedulerStartedAt = document.getElementById("wallet-sync-scheduler-started-at");
  const walletSyncSchedulerEndedAt = document.getElementById("wallet-sync-scheduler-ended-at");
  const walletSyncSchedulerError = document.getElementById("wallet-sync-scheduler-error");
  const chainMaintQueueLength = document.getElementById("chain-maint-queue-length");
  const chainMaintInFlight = document.getElementById("chain-maint-in-flight");
  const chainMaintInFlightTaskType = document.getElementById("chain-maint-in-flight-task-type");
  const chainMaintLastTaskStartedAt = document.getElementById("chain-maint-last-task-started-at");
  const chainMaintLastTaskEndedAt = document.getElementById("chain-maint-last-task-ended-at");
  const chainMaintLastError = document.getElementById("chain-maint-last-error");
  const walletSyncRoundID = document.getElementById("wallet-sync-round-id");
  const walletSyncFailedStep = document.getElementById("wallet-sync-failed-step");
  const walletSyncUpstreamPath = document.getElementById("wallet-sync-upstream-path");
  const walletSyncHTTPStatus = document.getElementById("wallet-sync-http-status");
  const walletOnchainError = document.getElementById("wallet-onchain-error");
  const walletSyncError = document.getElementById("wallet-sync-error");
  const errorBanner = document.getElementById("error-banner");
  const errorBannerText = document.getElementById("error-banner-text");
  const errorBannerCopyButton = document.getElementById("error-banner-copy-button");
  const webview = document.getElementById("content-view");
  const backendGate = document.getElementById("backend-gate");
  const backendTitle = document.getElementById("backend-title");
  const backendSummary = document.getElementById("backend-summary");
  const backendDetail = document.getElementById("backend-detail");
  const backendStepNote = document.getElementById("backend-step-note");
  const backendErrorBanner = document.getElementById("backend-error-banner");
  const backendErrorBannerText = document.getElementById("backend-error-banner-text");
  const backendErrorCopyButton = document.getElementById("backend-error-copy-button");
  const choosePanel = document.getElementById("backend-choose-panel");
  const chooseCreateButton = document.getElementById("choose-create-button");
  const chooseImportButton = document.getElementById("choose-import-button");
  const createKeyForm = document.getElementById("create-key-form");
  const createPasswordInput = document.getElementById("create-password-input");
  const createPasswordConfirmInput = document.getElementById("create-password-confirm-input");
  const createBackButton = document.getElementById("create-back-button");
  const unlockForm = document.getElementById("unlock-form");
  const unlockPasswordInput = document.getElementById("unlock-password-input");
  const exportKeyButton = document.getElementById("export-key-button");
  const restartPanel = document.getElementById("backend-restart-panel");
  const restartBackendButton = document.getElementById("restart-backend-button");
  const shellErrorModal = document.getElementById("shell-error-modal");
  const shellErrorTitle = document.getElementById("shell-error-title");
  const shellErrorSummary = document.getElementById("shell-error-summary");
  const shellErrorText = document.getElementById("shell-error-text");
  const shellErrorCopyButton = document.getElementById("shell-error-copy-button");
  const shellErrorSelectButton = document.getElementById("shell-error-select-button");
  const shellErrorCloseButton = document.getElementById("shell-error-close-button");

  if (
    !bridge || !browserShell || !addressForm || !addressInput || !backButton || !forwardButton || !reloadButton ||
    !goHomeButton || !openButton || !homeButton || !lockButton || !budgetForm || !singleBudgetInput ||
    !pageBudgetInput || !currentURL || !currentRootSeed || !pendingCount || !clientAPIBase || !homeSource ||
    !autoSpentTotal || !resourceTotalBadge || !resourcePendingBadge || !resourceListNote || !resourceList ||
    !panelTabResources || !panelTabWallet || !panelResources || !panelWallet || !sidebarResizeHandle ||
    !sidebarDragOverlay ||
    !walletRefreshButton || !walletSummaryNote || !walletDiagnosticNote || !walletCopyDiagnosticsButton ||
    !walletAddress || !walletBalance || !walletBalanceSource || !walletLedgerNet || !walletTotalIn ||
    !walletTotalOut || !walletBackendPhase || !walletClientAPIBase || !walletChainType || !walletChainBaseURL ||
    !walletSummaryUpdatedAt || !walletRuntimeStartedAt || !walletSyncUpdatedAt || !walletSyncStale || !walletSyncStaleReason ||
    !walletSyncTrigger || !walletSyncDuration || !walletSyncSchedulerStatus || !walletSyncSchedulerInFlight ||
    !walletSyncSchedulerStartedAt || !walletSyncSchedulerEndedAt || !walletSyncSchedulerError ||
    !chainMaintQueueLength || !chainMaintInFlight || !chainMaintInFlightTaskType ||
    !chainMaintLastTaskStartedAt || !chainMaintLastTaskEndedAt || !chainMaintLastError ||
    !walletSyncRoundID || !walletSyncFailedStep || !walletSyncUpstreamPath || !walletSyncHTTPStatus ||
    !walletOnchainError || !walletSyncError || !errorBanner || !errorBannerText || !errorBannerCopyButton ||
    !webview || !backendGate || !backendTitle || !backendSummary || !backendDetail ||
    !backendStepNote || !backendErrorBanner || !backendErrorBannerText || !backendErrorCopyButton ||
    !choosePanel || !chooseCreateButton || !chooseImportButton || !createKeyForm || !createPasswordInput ||
    !createPasswordConfirmInput || !createBackButton || !unlockForm || !unlockPasswordInput || !exportKeyButton ||
    !restartPanel || !restartBackendButton || !shellErrorModal || !shellErrorTitle || !shellErrorSummary ||
    !shellErrorText || !shellErrorCopyButton || !shellErrorSelectButton || !shellErrorCloseButton
  ) {
    throw new Error("bitfs shell bootstrap failed");
  }

  const PANEL_RESOURCES = "resources";
  const PANEL_WALLET = "wallet";
  const DEFAULT_SIDEBAR_WIDTH = 388;
  const WALLET_AUTO_REFRESH_INTERVAL_MS = 3000;

  let viewerDOMReady = false;
  let currentState = null;
  let onboardingStep = "choose-init";
  let lastBackendPhase = "";
  let lastRenderedURL = "";
  let lastPendingCount = -1;
  let lastShellError = "";
  let activePanel = PANEL_RESOURCES;
  let sidebarWidthPx = DEFAULT_SIDEBAR_WIDTH;
  let resizeState = null;
  let walletFetchSequence = 0;
  let walletAutoRefreshTimer = 0;
  const walletState = {
    loading: false,
    loaded: false,
    error: "",
    data: null,
    loadedAtISO: ""
  };

  function debugLog(scope, event, fields) {
    if (!bridge || typeof bridge.debugLog !== "function") {
      return;
    }
    bridge.debugLog(scope, event, fields || {});
  }

  function shouldAutoRefreshWallet() {
    if (!currentState || !isBackendReady(currentState) || walletState.loading) {
      return false;
    }
    if (activePanel === PANEL_WALLET) {
      return true;
    }
    if (!walletState.loaded || walletState.error) {
      return true;
    }
    const payload = walletState.data || {};
    const onchainError = String(payload.onchain_balance_error || "").trim();
    const syncError = String(payload.wallet_utxo_sync_last_error || "").trim();
    return onchainError !== "" || syncError !== "" || Boolean(payload.wallet_utxo_sync_state_is_stale);
  }

  function ensureWalletAutoRefreshLoop() {
    if (walletAutoRefreshTimer !== 0) {
      return;
    }
    // 设计说明：
    // - 钱包摘要第一次读取可能撞上 utxo 同步还没收敛；
    // - 自动刷新只在“未加载 / 有错误 / 仍旧 stale / 用户正停留在钱包页”时触发，
    //   让侧栏最终追上真实余额，又避免长期无意义轮询。
    walletAutoRefreshTimer = window.setInterval(() => {
      if (!shouldAutoRefreshWallet()) {
        return;
      }
      void loadWalletSummary(false);
    }, WALLET_AUTO_REFRESH_INTERVAL_MS);
  }

  function stopWalletAutoRefreshLoop() {
    if (walletAutoRefreshTimer === 0) {
      return;
    }
    window.clearInterval(walletAutoRefreshTimer);
    walletAutoRefreshTimer = 0;
  }

  async function copyPlainText(text) {
    const value = String(text || "");
    if (!value) {
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(value);
      return;
    }
    shellErrorText.value = value;
    shellErrorText.focus();
    shellErrorText.select();
    shellErrorText.setSelectionRange(0, shellErrorText.value.length);
    if (!document.execCommand("copy")) {
      throw new Error("clipboard unavailable");
    }
  }

  function selectShellErrorText() {
    shellErrorText.focus();
    shellErrorText.select();
    shellErrorText.setSelectionRange(0, shellErrorText.value.length);
  }

  function hideShellError() {
    shellErrorModal.classList.add("is-hidden");
    debugLog("shell", "error_modal_hidden");
  }

  function showShellError(message, title) {
    shellErrorTitle.textContent = String(title || "操作失败");
    shellErrorSummary.textContent = "错误内容已经整理成可复制文本。";
    shellErrorText.value = String(message || "").trim() || "unknown error";
    shellErrorModal.classList.remove("is-hidden");
    selectShellErrorText();
    debugLog("shell", "error_modal_shown", {
      title: shellErrorTitle.textContent,
      message: shellErrorText.value
    });
  }

  function normalizeTarget(raw) {
    const value = String(raw || "").trim().toLowerCase();
    if (value === "") {
      return "";
    }
    if (/^bitfs:\/\/[0-9a-f]{64}$/.test(value)) {
      return value.slice("bitfs://".length);
    }
    if (/^[0-9a-f]{64}$/.test(value)) {
      return value;
    }
    return "";
  }

  function clampSidebarWidth(raw) {
    const minWidth = window.innerWidth < 720 ? 220 : 280;
    const maxWidth = Math.max(minWidth, Math.min(560, window.innerWidth - 240));
    const next = Number(raw || 0);
    if (!Number.isFinite(next)) {
      return Math.min(maxWidth, Math.max(minWidth, DEFAULT_SIDEBAR_WIDTH));
    }
    return Math.min(maxWidth, Math.max(minWidth, Math.floor(next)));
  }

  function applySidebarWidth(width) {
    sidebarWidthPx = clampSidebarWidth(width);
    document.documentElement.style.setProperty("--sidebar-width", `${sidebarWidthPx}px`);
  }

  function persistSidebarLayout() {
    if (!currentState) {
      return;
    }
    void bridge.setSidebarLayout(sidebarWidthPx, activePanel).then((state) => {
      renderState(state);
    }).catch((error) => {
      showShellError(error instanceof Error ? error.message : String(error), "保存侧栏失败");
    });
  }

  function beginSidebarResize(startX) {
    resizeState = {
      startX,
      startWidth: sidebarWidthPx
    };
    sidebarResizeHandle.classList.add("is-dragging");
    document.body.classList.add("is-resizing-sidebar");
    debugLog("shell", "sidebar_resize_start", {
      width: sidebarWidthPx
    });
  }

  function updateSidebarResize(clientX, buttons) {
    if (!resizeState) {
      return;
    }
    if (buttons === 0) {
      endSidebarResize();
      return;
    }
    applySidebarWidth(resizeState.startWidth + (clientX - resizeState.startX));
  }

  function endSidebarResize() {
    if (!resizeState) {
      return;
    }
    resizeState = null;
    sidebarResizeHandle.classList.remove("is-dragging");
    document.body.classList.remove("is-resizing-sidebar");
    debugLog("shell", "sidebar_resize_end", {
      width: sidebarWidthPx
    });
    persistSidebarLayout();
  }

  function setActivePanel(panel, options) {
    const nextPanel = panel === PANEL_WALLET ? PANEL_WALLET : PANEL_RESOURCES;
    const persist = options && options.persist === true;
    const syncState = options && options.syncState === true;
    const changed = activePanel !== nextPanel;
    activePanel = nextPanel;
    panelTabResources.classList.toggle("is-active", nextPanel === PANEL_RESOURCES);
    panelTabWallet.classList.toggle("is-active", nextPanel === PANEL_WALLET);
    panelResources.classList.toggle("is-hidden", nextPanel !== PANEL_RESOURCES);
    panelWallet.classList.toggle("is-hidden", nextPanel !== PANEL_WALLET);
    if (changed || persist) {
      debugLog("shell", "panel_changed", {
        panel: nextPanel,
        persisted: persist
      });
    }
    if (nextPanel === PANEL_WALLET) {
      void loadWalletSummary(false);
    }
    if (persist && !syncState) {
      persistSidebarLayout();
    }
  }

  function isBackendReady(state) {
    return String(state?.backend?.phase || "") === "ready";
  }

  function getEffectiveHomeSeedHash(state) {
    return String(state?.userHomeSeedHash || state?.backend?.defaultHomeSeedHash || "").trim();
  }

  function formatBytes(size) {
    const value = Number(size || 0);
    if (!Number.isFinite(value) || value <= 0) {
      return "未知大小";
    }
    if (value < 1024) {
      return `${value} B`;
    }
    if (value < 1024 * 1024) {
      return `${(value / 1024).toFixed(1)} KB`;
    }
    return `${(value / (1024 * 1024)).toFixed(2)} MB`;
  }

  function formatSat(value) {
    const sat = Number(value || 0);
    if (!Number.isFinite(sat)) {
      return "-";
    }
    return `${Math.max(0, Math.floor(sat))} sat`;
  }

  function formatUnixTime(value) {
    const raw = Number(value || 0);
    if (!Number.isFinite(raw) || raw <= 0) {
      return "-";
    }
    try {
      return new Date(raw * 1000).toLocaleString("zh-CN", { hour12: false });
    } catch {
      return String(raw);
    }
  }

  function formatHTTPStatus(value) {
    const raw = Number(value || 0);
    if (!Number.isFinite(raw) || raw <= 0) {
      return "-";
    }
    return String(Math.floor(raw));
  }

  function formatBoolCN(value) {
    return value ? "是" : "否";
  }

  function escapeHTML(raw) {
    return String(raw || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function getModePresentation(resource) {
    if (resource.mode === "local") {
      return { icon: "L", title: "本地已有" };
    }
    if (resource.mode === "auto") {
      return { icon: "A", title: "未超过阀值，已自动允许下载" };
    }
    if (resource.mode === "approved") {
      return { icon: "M", title: "已手动允许下载" };
    }
    if (resource.mode === "pending") {
      return { icon: "!", title: "价格超过阀值，等待手动允许" };
    }
    return { icon: "X", title: "当前不可下载" };
  }

  function getTypePresentation(resource) {
    const mime = String(resource.mimeHint || "").toLowerCase();
    if (resource.isRoot || mime.includes("html")) {
      return { icon: "D", title: "文档资源" };
    }
    if (mime.includes("css")) {
      return { icon: "C", title: "样式资源" };
    }
    if (mime.includes("javascript") || mime.includes("ecmascript")) {
      return { icon: "J", title: "脚本资源" };
    }
    if (mime.startsWith("image/")) {
      return { icon: "I", title: "图片资源" };
    }
    if (mime.startsWith("audio/")) {
      return { icon: "A", title: "音频资源" };
    }
    if (mime.startsWith("video/")) {
      return { icon: "V", title: "视频资源" };
    }
    if (mime.includes("font")) {
      return { icon: "F", title: "字体资源" };
    }
    return { icon: "B", title: "二进制资源" };
  }

  function renderResourceItem(resource) {
    const modeView = getModePresentation(resource);
    const typeView = getTypePresentation(resource);
    const title = resource.isRoot
      ? "当前页面根文档"
      : String(resource.recommendedFileName || "").trim() || resource.seedHash;
    const details = [
      resource.mimeHint ? escapeHTML(resource.mimeHint) : "未知 MIME",
      escapeHTML(formatBytes(resource.fileSize)),
      `${Number(resource.chunkCount || 0)} 块`
    ].join(" | ");
    const sourceLine = resource.reason
      ? escapeHTML(resource.reason)
      : escapeHTML(resource.allowed ? "已允许下载" : "等待允许");
    const actionHTML = resource.mode === "pending"
      ? `<button class="secondary-button resource-action" type="button" data-action="approve-resource" data-seed="${resource.seedHash}">允许</button>`
      : "";
    return `
      <article class="resource-row" data-seed="${resource.seedHash}">
        <span class="resource-icon mode-${resource.mode}" title="${escapeHTML(modeView.title)}">${modeView.icon}</span>
        <span class="resource-icon" title="${escapeHTML(typeView.title)}">${typeView.icon}</span>
        <div class="resource-copy">
          <div class="resource-title">${escapeHTML(title)}</div>
          <div class="resource-meta">${details}</div>
          <div class="resource-meta">${sourceLine}</div>
          <div class="resource-hash">${escapeHTML(resource.seedHash)}</div>
        </div>
        <div class="resource-price" title="资源总价">${escapeHTML(formatSat(resource.estimatedTotalSat))}</div>
        ${actionHTML}
      </article>
    `;
  }

  function renderWalletSummary() {
    if (walletState.loading) {
      walletSummaryNote.textContent = "正在读取钱包摘要。";
    } else if (walletState.error) {
      walletSummaryNote.textContent = walletState.error;
    } else if (!walletState.loaded) {
      walletSummaryNote.textContent = "等待读取钱包摘要。";
    } else {
      const onchainError = String(walletState.data?.onchain_balance_error || "").trim();
      walletSummaryNote.textContent = onchainError || "钱包摘要已更新。";
    }
    const payload = walletState.data || {};
    const backendPhase = String(currentState?.backend?.phase || "");
    const onchainError = String(payload.onchain_balance_error || "").trim();
    const syncError = String(payload.wallet_utxo_sync_last_error || "").trim();
    walletAddress.textContent = String(payload.wallet_address || "-");
    walletBalance.textContent = walletState.loaded ? formatSat(payload.onchain_balance_satoshi) : "-";
    walletBalanceSource.textContent = String(payload.balance_source || "-");
    walletLedgerNet.textContent = walletState.loaded ? formatSat(payload.ledger_net_satoshi) : "-";
    walletTotalIn.textContent = walletState.loaded ? formatSat(payload.total_in_satoshi) : "-";
    walletTotalOut.textContent = walletState.loaded ? formatSat(payload.total_out_satoshi) : "-";
    walletBackendPhase.textContent = backendPhase || "-";
    walletClientAPIBase.textContent = String(currentState?.backend?.apiBase || currentState?.clientAPIBase || "-");
    walletChainType.textContent = String(payload.wallet_chain_type || "-");
    walletChainBaseURL.textContent = String(payload.wallet_chain_base_url || "-");
    walletSummaryUpdatedAt.textContent = walletState.loadedAtISO || "-";
    walletRuntimeStartedAt.textContent = formatUnixTime(payload.runtime_started_at_unix);
    walletSyncUpdatedAt.textContent = formatUnixTime(payload.wallet_utxo_sync_updated_at_unix);
    walletSyncStale.textContent = walletState.loaded ? formatBoolCN(Boolean(payload.wallet_utxo_sync_state_is_stale)) : "-";
    walletSyncStaleReason.textContent = String(payload.wallet_utxo_sync_state_stale_reason || "-");
    walletSyncTrigger.textContent = String(payload.wallet_utxo_sync_last_trigger || "-");
    walletSyncDuration.textContent = walletState.loaded ? `${Number(payload.wallet_utxo_sync_last_duration_ms || 0)} ms` : "-";
    walletSyncSchedulerStatus.textContent = String(payload.wallet_utxo_sync_scheduler_status || "-");
    walletSyncSchedulerInFlight.textContent = walletState.loaded ? formatBoolCN(Boolean(payload.wallet_utxo_sync_scheduler_in_flight)) : "-";
    walletSyncSchedulerStartedAt.textContent = formatUnixTime(payload.wallet_utxo_sync_scheduler_last_started_at_unix);
    walletSyncSchedulerEndedAt.textContent = formatUnixTime(payload.wallet_utxo_sync_scheduler_last_ended_at_unix);
    walletSyncSchedulerError.textContent = String(payload.wallet_utxo_sync_scheduler_last_error || "-");
    chainMaintQueueLength.textContent = walletState.loaded ? String(Number(payload.chain_maintainer_queue_length || 0)) : "-";
    chainMaintInFlight.textContent = walletState.loaded ? formatBoolCN(Boolean(payload.chain_maintainer_in_flight)) : "-";
    chainMaintInFlightTaskType.textContent = String(payload.chain_maintainer_in_flight_task_type || "-");
    chainMaintLastTaskStartedAt.textContent = formatUnixTime(payload.chain_maintainer_last_task_started_at_unix);
    chainMaintLastTaskEndedAt.textContent = formatUnixTime(payload.chain_maintainer_last_task_ended_at_unix);
    chainMaintLastError.textContent = String(payload.chain_maintainer_last_error || "-");
    walletSyncRoundID.textContent = String(payload.wallet_utxo_sync_last_round_id || "-");
    walletSyncFailedStep.textContent = String(payload.wallet_utxo_sync_last_failed_step || "-");
    walletSyncUpstreamPath.textContent = String(payload.wallet_utxo_sync_last_upstream_path || "-");
    walletSyncHTTPStatus.textContent = formatHTTPStatus(payload.wallet_utxo_sync_last_http_status);
    walletOnchainError.textContent = onchainError || "-";
    walletSyncError.textContent = syncError || "-";
    walletDiagnosticNote.textContent = !walletState.loaded
      ? "这里会显示钱包摘要链路的关键诊断字段。"
      : payload.wallet_utxo_sync_state_is_stale
        ? "当前钱包同步状态看起来是旧运行时残留，请先对照运行时启动时间和调度状态。"
      : onchainError || syncError
        ? "已检测到钱包摘要退化，请优先复制诊断并对照后端 chain_utxo_worker 日志。"
        : "钱包摘要链路当前没有暴露出错误。";
    walletRefreshButton.disabled = !currentState || !isBackendReady(currentState) || walletState.loading;
    walletCopyDiagnosticsButton.disabled = !walletState.loaded && !walletState.error;
  }

  async function loadWalletSummary(force) {
    if (!currentState || !isBackendReady(currentState)) {
      walletState.loading = false;
      walletState.loaded = false;
      walletState.error = "内置客户端尚未就绪。";
      walletState.data = null;
      walletState.loadedAtISO = "";
      renderWalletSummary();
      return;
    }
    if (walletState.loading && !force) {
      return;
    }
    walletFetchSequence += 1;
    const requestID = walletFetchSequence;
    walletState.loading = true;
    walletState.error = "";
    renderWalletSummary();
    debugLog("wallet", "summary_request", {
      force: Boolean(force),
      request_id: requestID
    });
    try {
      const summary = await bridge.getWalletSummary();
      if (requestID !== walletFetchSequence) {
        return;
      }
      walletState.loading = false;
      walletState.loaded = true;
      walletState.error = "";
      walletState.data = summary || {};
      walletState.loadedAtISO = new Date().toLocaleString("zh-CN", { hour12: false });
      renderWalletSummary();
      const onchainError = String(summary?.onchain_balance_error || "").trim();
      const syncError = String(summary?.wallet_utxo_sync_last_error || "").trim();
      debugLog("wallet", onchainError || syncError ? "summary_loaded_degraded" : "summary_loaded", {
        request_id: requestID,
        wallet_address: String(summary?.wallet_address || ""),
        wallet_chain_base_url: String(summary?.wallet_chain_base_url || ""),
        wallet_chain_type: String(summary?.wallet_chain_type || ""),
        runtime_started_at_unix: Number(summary?.runtime_started_at_unix || 0),
        wallet_utxo_sync_state_is_stale: Boolean(summary?.wallet_utxo_sync_state_is_stale),
        wallet_utxo_sync_state_stale_reason: String(summary?.wallet_utxo_sync_state_stale_reason || ""),
        wallet_utxo_sync_scheduler_status: String(summary?.wallet_utxo_sync_scheduler_status || ""),
        wallet_utxo_sync_scheduler_in_flight: Boolean(summary?.wallet_utxo_sync_scheduler_in_flight),
        chain_maintainer_queue_length: Number(summary?.chain_maintainer_queue_length || 0),
        chain_maintainer_in_flight: Boolean(summary?.chain_maintainer_in_flight),
        chain_maintainer_in_flight_task_type: String(summary?.chain_maintainer_in_flight_task_type || ""),
        wallet_utxo_sync_last_round_id: String(summary?.wallet_utxo_sync_last_round_id || ""),
        wallet_utxo_sync_last_failed_step: String(summary?.wallet_utxo_sync_last_failed_step || ""),
        wallet_utxo_sync_last_upstream_path: String(summary?.wallet_utxo_sync_last_upstream_path || ""),
        wallet_utxo_sync_last_http_status: Number(summary?.wallet_utxo_sync_last_http_status || 0),
        has_onchain_balance_error: onchainError !== "",
        has_wallet_sync_error: syncError !== "",
        onchain_balance_error: onchainError,
        wallet_utxo_sync_last_error: syncError
      });
    } catch (error) {
      if (requestID !== walletFetchSequence) {
        return;
      }
      walletState.loading = false;
      walletState.loaded = false;
      walletState.data = null;
      walletState.loadedAtISO = "";
      walletState.error = error instanceof Error ? error.message : String(error);
      renderWalletSummary();
      debugLog("wallet", "summary_failed", {
        request_id: requestID,
        message: walletState.error
      });
    }
  }

  function syncViewerURL(state) {
    if (!isBackendReady(state)) {
      webview.removeAttribute("src");
      debugLog("shell", "viewer_src_cleared", {
        reason: "backend_not_ready"
      });
      refreshNavigationButtons();
      return;
    }
    const target = String(state.currentURL || "").trim();
    if (!target) {
      webview.removeAttribute("src");
      debugLog("shell", "viewer_src_cleared", {
        reason: "empty_target"
      });
      refreshNavigationButtons();
      return;
    }
    if (webview.getAttribute("src") === target) {
      refreshNavigationButtons();
      return;
    }
    webview.setAttribute("src", target);
    debugLog("shell", "viewer_src_set", {
      url: target
    });
  }

  function setInteractiveEnabled(enabled) {
    addressInput.disabled = !enabled;
    openButton.disabled = !enabled;
    homeButton.disabled = !enabled;
    goHomeButton.disabled = !enabled;
    reloadButton.disabled = !enabled;
    singleBudgetInput.disabled = !enabled;
    pageBudgetInput.disabled = !enabled;
    lockButton.disabled = !enabled;
    walletRefreshButton.disabled = !enabled || walletState.loading;
    refreshNavigationButtons(enabled);
  }

  function setShellMode(mode) {
    const browserVisible = mode === "browser";
    browserShell.classList.toggle("is-hidden", !browserVisible);
    backendGate.classList.toggle("is-hidden", browserVisible);
  }

  function refreshNavigationButtons(assumeBackendReady) {
    const ready = typeof assumeBackendReady === "boolean" ? assumeBackendReady : isBackendReady(currentState);
    const currentViewerURL = viewerDOMReady ? webview.getURL() : "";
    const hasPage = ready && Boolean(currentViewerURL || webview.getAttribute("src"));
    backButton.disabled = !hasPage || !viewerDOMReady || !webview.canGoBack();
    forwardButton.disabled = !hasPage || !viewerDOMReady || !webview.canGoForward();
  }

  function setOnboardingStep(nextStep) {
    onboardingStep = nextStep;
    choosePanel.classList.toggle("is-hidden", onboardingStep !== "choose-init");
    createKeyForm.classList.toggle("is-hidden", onboardingStep !== "create-key");
    unlockForm.classList.toggle("is-hidden", onboardingStep !== "unlock-key");
    backendStepNote.classList.toggle("is-hidden", onboardingStep === "checking");
    debugLog("shell", "onboarding_step_changed", {
      step: onboardingStep
    });
  }

  function resolveLockedStep(backend) {
    return backend.hasKey ? "unlock-key" : onboardingStep === "create-key" ? "create-key" : "choose-init";
  }

  function renderBackendState(state) {
    const backend = state.backend || {};
    const phase = String(backend.phase || "starting");
    const hasKey = Boolean(backend.hasKey);
    if (lastBackendPhase !== phase) {
      lastBackendPhase = phase;
      debugLog("shell", "backend_phase_changed", {
        phase,
        has_key: hasKey,
        unlocked: Boolean(backend.unlocked),
        default_home_seed_hash: String(backend.defaultHomeSeedHash || ""),
        has_system_home_bundle: Boolean(backend.hasSystemHomeBundle)
      });
    }
    lockButton.classList.toggle("is-hidden", phase !== "ready");

    if (phase === "ready") {
      setShellMode("browser");
      backendErrorBanner.classList.add("is-hidden");
      setOnboardingStep("checking");
      setInteractiveEnabled(true);
      if (!walletState.loaded && !walletState.loading) {
        void loadWalletSummary(false);
      } else if (activePanel === PANEL_WALLET && walletState.error) {
        void loadWalletSummary(false);
      }
      return;
    }

    setInteractiveEnabled(false);
    setShellMode("gate");
    setOnboardingStep(resolveLockedStep(backend));
    restartPanel.classList.add("is-hidden");
    backendStepNote.textContent = "";

    if (phase === "starting") {
      setOnboardingStep("checking");
      backendTitle.textContent = "正在启动内置客户端";
      backendSummary.textContent = "Electron 正在拉起 Go 客户端，并等待 managed API 可用。";
      backendStepNote.textContent = "壳层会先读取 Go managed API 状态，再决定显示初始化步骤还是解锁步骤。";
    } else if (phase === "locked" && !hasKey) {
      if (onboardingStep === "create-key") {
        backendTitle.textContent = "新建密文私钥";
        backendSummary.textContent = "这一步会生成新的私钥，并把密文写入当前托管 vault 的 key.json。";
        backendStepNote.textContent = "创建完成后壳层会继续用当前密码启动 Go 运行时，不再停留在单独解锁页。";
      } else {
        backendTitle.textContent = "初始化客户端私钥";
        backendSummary.textContent = "这是第一次启动。先选择新建密文私钥，或导入已经存在的密文私钥。";
        backendStepNote.textContent = "导入与导出都只处理密文私钥 JSON，不处理明文私钥。";
      }
    } else if (phase === "locked") {
      backendTitle.textContent = "客户端已锁定";
      backendSummary.textContent = "输入密码后解锁 Go 客户端，BitFS 页面才会真正装载。";
      backendStepNote.textContent = "如果你还没有备份当前密文私钥，可以先导出到安全位置。";
    } else if (phase === "startup_error") {
      setOnboardingStep("checking");
      backendTitle.textContent = "内置客户端启动失败";
      backendSummary.textContent = "Go 后台没有完成受管资源监听，当前不会进入私钥解锁流程。";
      backendStepNote.textContent = "请先处理端口占用或托管链访问设置，再点击重启后端。";
      restartPanel.classList.remove("is-hidden");
    } else if (phase === "error") {
      setOnboardingStep("checking");
      backendTitle.textContent = "内置客户端启动失败";
      backendSummary.textContent = "Go 子进程没有成功进入可管理状态，可以直接在这里重启。";
      restartPanel.classList.remove("is-hidden");
    } else {
      setOnboardingStep("checking");
      backendTitle.textContent = "内置客户端已停止";
      backendSummary.textContent = "客户端当前没有运行，可以重新拉起。";
      restartPanel.classList.remove("is-hidden");
    }

    backendDetail.textContent = [
      backend.vaultPath ? `vault: ${backend.vaultPath}` : "",
      backend.binaryPath ? `binary: ${backend.binaryPath}` : "",
      backend.apiBase ? `api: ${backend.apiBase}` : "",
      backend.fsHTTPListenAddr ? `fs_http: ${backend.fsHTTPListenAddr}` : "",
      backend.chainAccessMode ? `chain_access: ${backend.chainAccessMode}` : "",
      backend.walletChainBaseURL ? `wallet_chain: ${backend.walletChainBaseURL}` : "",
      backend.wocProxyEnabled && backend.wocProxyListenAddr ? `woc_proxy: ${backend.wocProxyListenAddr}` : "",
      backend.startupErrorService ? `startup_service: ${backend.startupErrorService}` : "",
      backend.startupErrorListenAddr ? `startup_listen: ${backend.startupErrorListenAddr}` : "",
      backend.pid ? `pid: ${backend.pid}` : ""
    ].filter(Boolean).join(" | ");

    const backendError = String(backend.lastError || "").trim();
    backendErrorBannerText.textContent = backendError;
    backendErrorBanner.classList.toggle("is-hidden", backendError === "");
    exportKeyButton.disabled = !hasKey;
  }

  function renderState(state) {
    currentState = state;
    if (lastRenderedURL !== String(state.currentURL || "")) {
      lastRenderedURL = String(state.currentURL || "");
      debugLog("shell", "current_url_changed", {
        url: lastRenderedURL
      });
    }

    applySidebarWidth(state.sidebarWidthPx || sidebarWidthPx);
    if (!isBackendReady(state)) {
      walletState.loading = false;
      walletState.loaded = false;
      walletState.error = "内置客户端尚未就绪。";
      walletState.data = null;
    }

    if (String(state.activePanel || "") !== activePanel) {
      setActivePanel(state.activePanel, { syncState: true });
    }

    currentURL.textContent = state.currentURL || "尚未打开";
    currentRootSeed.textContent = state.currentRootSeedHash || "-";
    clientAPIBase.textContent = String(state.backend?.apiBase || state.clientAPIBase || "-");
    autoSpentTotal.textContent = formatSat(state.autoSpentSat);
    pendingCount.textContent = String(Number(state.pendingCount || 0));
    resourceTotalBadge.textContent = `${Array.isArray(state.resources) ? state.resources.length : 0} 项`;
    resourcePendingBadge.textContent = `${Number(state.pendingCount || 0)} 待批`;

    if (document.activeElement !== addressInput) {
      addressInput.value = state.currentURL || "";
    }
    if (document.activeElement !== singleBudgetInput) {
      singleBudgetInput.value = String(state.staticSingleMaxSat || "");
    }
    if (document.activeElement !== pageBudgetInput) {
      pageBudgetInput.value = String(state.staticPageMaxSat || "");
    }

    if (state.userHomeSeedHash) {
      homeSource.textContent = `浏览器自定义：${state.userHomeSeedHash}`;
    } else if (state.backend?.defaultHomeSeedHash) {
      homeSource.textContent = `客户端默认：${state.backend.defaultHomeSeedHash}`;
    } else {
      homeSource.textContent = "未配置首页";
    }

    const resources = Array.isArray(state.resources) ? state.resources : [];
    if (lastPendingCount !== Number(state.pendingCount || 0)) {
      lastPendingCount = Number(state.pendingCount || 0);
      debugLog("shell", "resource_pending_changed", {
        pending_count: lastPendingCount,
        resource_count: resources.length,
        auto_spent_sat: Number(state.autoSpentSat || 0)
      });
    }
    resourceListNote.textContent = resources.length === 0
      ? "当前没有已规划资源。"
      : `共 ${resources.length} 项，${Number(state.pendingCount || 0)} 项等待手动允许，按价格从高到低排序。`;
    resourceList.innerHTML = resources.map(renderResourceItem).join("");

    const message = String(state.lastError || "").trim();
    if (lastShellError !== message) {
      lastShellError = message;
      if (message) {
        debugLog("shell", "state_error_changed", {
          message
        });
      }
    }
    errorBannerText.textContent = message;
    errorBanner.classList.toggle("is-hidden", message === "");

    const currentSeedHash = normalizeTarget(state.currentURL || "");
    const userHomeSeedHash = String(state.userHomeSeedHash || "");
    homeButton.textContent = userHomeSeedHash && currentSeedHash === userHomeSeedHash ? "清除首页" : "设为首页";
    goHomeButton.disabled = !isBackendReady(state) || getEffectiveHomeSeedHash(state) === "";

    renderBackendState(state);
    renderWalletSummary();
    syncViewerURL(state);
  }

  async function openFromInput() {
    if (!currentState || !isBackendReady(currentState)) {
      showShellError("内置客户端尚未就绪，请先创建或解锁密钥。", "客户端未就绪");
      return;
    }
    const normalized = normalizeTarget(addressInput.value);
    if (!normalized) {
      showShellError("请输入 64 位十六进制 seed hash，或 bitfs://<seed_hash>", "地址无效");
      addressInput.focus();
      return;
    }
    debugLog("shell", "open_from_input", {
      seed_hash: normalized
    });
    await bridge.open(normalized);
    return bridge.getState();
  }

  async function openHome() {
    if (!currentState || !isBackendReady(currentState)) {
      return;
    }
    const homeSeedHash = getEffectiveHomeSeedHash(currentState);
    if (!homeSeedHash) {
      showShellError("当前没有可用首页。", "首页不可用");
      return;
    }
    debugLog("shell", "open_home", {
      seed_hash: homeSeedHash
    });
    await bridge.open(homeSeedHash);
    return bridge.getState();
  }

  async function withAction(action) {
    try {
      const next = await action();
      if (next) {
        renderState(next);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      debugLog("shell", "action_failed", {
        message
      });
      showShellError(message || "operation failed");
    }
  }

  function handleViewerNavigation(url) {
    debugLog("webview", "navigated", {
      url
    });
    bridge.noteNavigation(url);
    window.setTimeout(refreshNavigationButtons, 0);
  }

  async function importEncryptedKeyFile() {
    const result = await bridge.importKeyFile();
    if (!result || result.cancelled) {
      return result?.state;
    }
    showShellError(`已导入密文私钥: ${result.filePath}`, "导入成功");
    onboardingStep = "unlock-key";
    return result.state;
  }

  async function exportEncryptedKeyFile() {
    const result = await bridge.exportKeyFile();
    if (!result || result.cancelled) {
      return result?.state;
    }
    showShellError(`已导出密文私钥: ${result.filePath}`, "导出成功");
    return result.state;
  }

  function resolveApprovalStrategy(resource) {
    const mime = String(resource?.mimeHint || "").toLowerCase();
    if (!resource) {
      return "reload";
    }
    if (resource.isRoot || mime.includes("html")) {
      return "reload";
    }
    if (mime.includes("css")) {
      return "reload";
    }
    if (mime.includes("javascript") || mime.includes("ecmascript")) {
      return "reload";
    }
    if (mime.startsWith("image/") || mime.startsWith("audio/") || mime.startsWith("video/")) {
      return "media";
    }
    return "reload";
  }

  async function hotReloadMediaResource(seedHash) {
    if (!viewerDOMReady) {
      return false;
    }
    const script = `
      (() => {
        const seedHash = "${seedHash}";
        const tags = [
          { selector: "img", attr: "src" },
          { selector: "audio", attr: "src" },
          { selector: "video", attr: "src" },
          { selector: "video", attr: "poster" },
          { selector: "source", attr: "src" }
        ];
        function resolveSeed(raw) {
          try {
            const url = new URL(String(raw || ""), window.location.href);
            if (url.protocol !== "bitfs:") {
              return "";
            }
            const value = String(url.pathname || "").replace(/^\\/+/, "") || String(url.hostname || "");
            return /^[0-9a-f]{64}$/.test(value.toLowerCase()) ? value.toLowerCase() : "";
          } catch {
            return "";
          }
        }
        function bust(raw) {
          const url = new URL(String(raw || ""), window.location.href);
          url.hash = "bitfs-refresh=" + Date.now().toString(36);
          return url.toString();
        }
        let touched = 0;
        for (const entry of tags) {
          const nodes = Array.from(document.querySelectorAll(entry.selector));
          for (const node of nodes) {
            const raw = node.getAttribute(entry.attr) || node[entry.attr] || "";
            if (!raw || resolveSeed(raw) !== seedHash) {
              continue;
            }
            node.setAttribute(entry.attr, bust(raw));
            if (node.tagName === "AUDIO" || node.tagName === "VIDEO") {
              node.load();
            }
            if (node.tagName === "SOURCE" && node.parentElement && typeof node.parentElement.load === "function") {
              node.parentElement.load();
            }
            touched += 1;
          }
        }
        return touched;
      })();
    `;
    const touched = await webview.executeJavaScript(script, true);
    debugLog("webview", "media_hot_reload_attempted", {
      seed_hash: seedHash,
      touched: Number(touched || 0)
    });
    return Number(touched || 0) > 0;
  }

  async function applyApprovalEffect(resource) {
    const strategy = resolveApprovalStrategy(resource);
    if (!viewerDOMReady) {
      return;
    }
    if (strategy === "media") {
      const applied = await hotReloadMediaResource(resource.seedHash);
      if (applied) {
        return;
      }
    }
    debugLog("webview", "reload_after_approve", {
      seed_hash: resource.seedHash,
      strategy
    });
    if (viewerDOMReady && webview.getURL()) {
      webview.reload();
    }
  }

  function getResourceBySeed(seedHash) {
    if (!currentState || !Array.isArray(currentState.resources)) {
      return null;
    }
    return currentState.resources.find((item) => item.seedHash === seedHash) || null;
  }

  function buildWalletDiagnosticsText() {
    const payload = walletState.data || {};
    return [
      `backend_phase: ${String(currentState?.backend?.phase || "") || "-"}`,
      `client_api_base: ${String(currentState?.backend?.apiBase || currentState?.clientAPIBase || "") || "-"}`,
      `wallet_chain_type: ${String(payload.wallet_chain_type || "") || "-"}`,
      `wallet_chain_base_url: ${String(payload.wallet_chain_base_url || "") || "-"}`,
      `wallet_address: ${String(payload.wallet_address || "") || "-"}`,
      `summary_loaded_at: ${walletState.loadedAtISO || "-"}`,
      `runtime_started_at: ${formatUnixTime(payload.runtime_started_at_unix)}`,
      `wallet_utxo_sync_updated_at: ${formatUnixTime(payload.wallet_utxo_sync_updated_at_unix)}`,
      `wallet_utxo_sync_state_is_stale: ${formatBoolCN(Boolean(payload.wallet_utxo_sync_state_is_stale))}`,
      `wallet_utxo_sync_state_stale_reason: ${String(payload.wallet_utxo_sync_state_stale_reason || "") || "-"}`,
      `wallet_utxo_sync_last_trigger: ${String(payload.wallet_utxo_sync_last_trigger || "") || "-"}`,
      `wallet_utxo_sync_last_duration_ms: ${String(payload.wallet_utxo_sync_last_duration_ms || 0)}`,
      `wallet_utxo_sync_scheduler_status: ${String(payload.wallet_utxo_sync_scheduler_status || "") || "-"}`,
      `wallet_utxo_sync_scheduler_in_flight: ${formatBoolCN(Boolean(payload.wallet_utxo_sync_scheduler_in_flight))}`,
      `wallet_utxo_sync_scheduler_last_started_at: ${formatUnixTime(payload.wallet_utxo_sync_scheduler_last_started_at_unix)}`,
      `wallet_utxo_sync_scheduler_last_ended_at: ${formatUnixTime(payload.wallet_utxo_sync_scheduler_last_ended_at_unix)}`,
      `wallet_utxo_sync_scheduler_last_duration_ms: ${String(payload.wallet_utxo_sync_scheduler_last_duration_ms || 0)}`,
      `wallet_utxo_sync_scheduler_last_error: ${String(payload.wallet_utxo_sync_scheduler_last_error || "") || "-"}`,
      `chain_maintainer_queue_length: ${String(Number(payload.chain_maintainer_queue_length || 0))}`,
      `chain_maintainer_in_flight: ${formatBoolCN(Boolean(payload.chain_maintainer_in_flight))}`,
      `chain_maintainer_in_flight_task_type: ${String(payload.chain_maintainer_in_flight_task_type || "") || "-"}`,
      `chain_maintainer_last_task_started_at: ${formatUnixTime(payload.chain_maintainer_last_task_started_at_unix)}`,
      `chain_maintainer_last_task_ended_at: ${formatUnixTime(payload.chain_maintainer_last_task_ended_at_unix)}`,
      `chain_maintainer_last_error: ${String(payload.chain_maintainer_last_error || "") || "-"}`,
      `wallet_utxo_sync_last_round_id: ${String(payload.wallet_utxo_sync_last_round_id || "") || "-"}`,
      `wallet_utxo_sync_last_failed_step: ${String(payload.wallet_utxo_sync_last_failed_step || "") || "-"}`,
      `wallet_utxo_sync_last_upstream_path: ${String(payload.wallet_utxo_sync_last_upstream_path || "") || "-"}`,
      `wallet_utxo_sync_last_http_status: ${formatHTTPStatus(payload.wallet_utxo_sync_last_http_status)}`,
      `onchain_balance_error: ${String(payload.onchain_balance_error || walletState.error || "") || "-"}`,
      `wallet_utxo_sync_last_error: ${String(payload.wallet_utxo_sync_last_error || "") || "-"}`
    ].join("\n");
  }

  webview.addEventListener("did-attach", function handleAttach() {
    viewerDOMReady = false;
    debugLog("webview", "did_attach");
    if (currentState) {
      syncViewerURL(currentState);
    }
    refreshNavigationButtons();
  });

  webview.addEventListener("dom-ready", function handleDOMReady() {
    viewerDOMReady = true;
    debugLog("webview", "dom_ready", {
      url: webview.getAttribute("src") || ""
    });
    refreshNavigationButtons();
  });

  webview.addEventListener("did-stop-loading", function handleStopLoading() {
    debugLog("webview", "did_stop_loading", {
      url: viewerDOMReady ? webview.getURL() : webview.getAttribute("src") || ""
    });
    refreshNavigationButtons();
  });

  webview.addEventListener("did-navigate", function handleNavigate(event) {
    handleViewerNavigation(event.url);
  });

  webview.addEventListener("did-navigate-in-page", function handleInPageNavigate(event) {
    handleViewerNavigation(event.url);
  });

  webview.addEventListener("did-fail-load", function handleLoadFail(event) {
    if (event.errorCode === -3) {
      return;
    }
    debugLog("webview", "did_fail_load", {
      error_code: event.errorCode,
      error_description: event.errorDescription,
      validated_url: event.validatedURL
    });
    handleViewerNavigation((viewerDOMReady ? webview.getURL() : "") || "");
  });

  addressForm.addEventListener("submit", function handleAddressSubmit(event) {
    event.preventDefault();
    debugLog("shell", "address_submit", {
      raw_input: addressInput.value
    });
    void withAction(openFromInput);
  });

  openButton.addEventListener("click", function handleOpenClick(event) {
    event.preventDefault();
    debugLog("shell", "open_button_click", {
      raw_input: addressInput.value
    });
    void withAction(openFromInput);
  });

  backButton.addEventListener("click", function handleGoBack() {
    if (viewerDOMReady && webview.canGoBack()) {
      debugLog("webview", "go_back");
      webview.goBack();
    }
  });

  forwardButton.addEventListener("click", function handleGoForward() {
    if (viewerDOMReady && webview.canGoForward()) {
      debugLog("webview", "go_forward");
      webview.goForward();
    }
  });

  reloadButton.addEventListener("click", function handleReloadClick() {
    if (!currentState || !isBackendReady(currentState)) {
      return;
    }
    if (viewerDOMReady && webview.getURL()) {
      debugLog("webview", "reload", {
        url: webview.getURL()
      });
      webview.reload();
      return;
    }
    void withAction(openFromInput);
  });

  goHomeButton.addEventListener("click", function handleGoHome() {
    debugLog("shell", "go_home_click");
    void withAction(openHome);
  });

  lockButton.addEventListener("click", function handleLockClick() {
    debugLog("shell", "lock_click");
    void withAction(() => bridge.lock());
  });

  homeButton.addEventListener("click", function handleHomeClick() {
    if (!currentState || !isBackendReady(currentState)) {
      return;
    }
    const currentSeedHash = normalizeTarget(currentState.currentURL || addressInput.value || "");
    if (!currentSeedHash) {
      showShellError("先打开一个 bitfs 页面，才能把它设成浏览器首页。", "无法设置首页");
      return;
    }
    const userHomeSeedHash = String(currentState.userHomeSeedHash || "");
    if (userHomeSeedHash && userHomeSeedHash === currentSeedHash) {
      debugLog("shell", "clear_user_homepage_click", {
        seed_hash: currentSeedHash
      });
      void withAction(() => bridge.clearUserHomepage());
      return;
    }
    debugLog("shell", "set_user_homepage_click", {
      seed_hash: currentSeedHash
    });
    void withAction(() => bridge.setUserHomepage(currentSeedHash));
  });

  budgetForm.addEventListener("submit", function handleBudgetSubmit(event) {
    event.preventDefault();
    const singleMaxSat = Number(singleBudgetInput.value || 0);
    const pageMaxSat = Number(pageBudgetInput.value || 0);
    debugLog("shell", "budget_submit", {
      single_max_sat: singleMaxSat,
      page_max_sat: pageMaxSat
    });
    void withAction(() => bridge.setBudget(singleMaxSat, pageMaxSat));
  });

  resourceList.addEventListener("click", function handleResourceAction(event) {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (target.dataset.action !== "approve-resource") {
      return;
    }
    const seedHash = String(target.dataset.seed || "").trim().toLowerCase();
    const resource = getResourceBySeed(seedHash);
    if (!resource) {
      return;
    }
    debugLog("shell", "approve_resource_click", {
      seed_hash: seedHash,
      strategy: resolveApprovalStrategy(resource)
    });
    void withAction(async function approveSingleResource() {
      const next = await bridge.approveResource(seedHash);
      await applyApprovalEffect(resource);
      return next;
    });
  });

  panelTabResources.addEventListener("click", function handleOpenResourcesPanel() {
    setActivePanel(PANEL_RESOURCES, { persist: true });
  });

  panelTabWallet.addEventListener("click", function handleOpenWalletPanel() {
    setActivePanel(PANEL_WALLET, { persist: true });
  });

  walletRefreshButton.addEventListener("click", function handleWalletRefresh() {
    void loadWalletSummary(true);
  });

  walletCopyDiagnosticsButton.addEventListener("click", function handleWalletCopyDiagnostics() {
    void copyPlainText(buildWalletDiagnosticsText()).catch((error) => {
      showShellError(error instanceof Error ? error.message : String(error), "复制失败");
    });
  });

  sidebarResizeHandle.addEventListener("mousedown", function handleResizeStart(event) {
    event.preventDefault();
    beginSidebarResize(event.clientX);
  });

  sidebarDragOverlay.addEventListener("mousemove", function handleResizeMove(event) {
    updateSidebarResize(event.clientX, event.buttons);
  });

  sidebarDragOverlay.addEventListener("mouseup", function handleResizeEnd() {
    endSidebarResize();
  });

  window.addEventListener("blur", function handleResizeAbort() {
    endSidebarResize();
  });

  chooseCreateButton.addEventListener("click", function handleChooseCreate() {
    debugLog("shell", "choose_create_key");
    setOnboardingStep("create-key");
    createPasswordInput.focus();
  });

  chooseImportButton.addEventListener("click", function handleChooseImport() {
    debugLog("shell", "choose_import_key");
    void withAction(importEncryptedKeyFile);
  });

  createKeyForm.addEventListener("submit", function handleCreateKey(event) {
    event.preventDefault();
    const password = String(createPasswordInput.value || "");
    const confirm = String(createPasswordConfirmInput.value || "");
    if (password === "") {
      showShellError("请输入新密码", "密码不能为空");
      createPasswordInput.focus();
      return;
    }
    if (password !== confirm) {
      showShellError("两次密码不一致", "密码不一致");
      createPasswordConfirmInput.focus();
      return;
    }
    debugLog("shell", "create_key_submit");
    void withAction(async function createKey() {
      await bridge.createKey(password);
      const next = await bridge.unlock(password);
      createPasswordInput.value = "";
      createPasswordConfirmInput.value = "";
      return next;
    });
  });

  createBackButton.addEventListener("click", function handleCreateBack() {
    debugLog("shell", "create_key_back");
    setOnboardingStep("choose-init");
  });

  unlockForm.addEventListener("submit", function handleUnlock(event) {
    event.preventDefault();
    const password = String(unlockPasswordInput.value || "");
    if (password === "") {
      showShellError("请输入解锁密码", "密码不能为空");
      unlockPasswordInput.focus();
      return;
    }
    debugLog("shell", "unlock_submit");
    void withAction(async function unlock() {
      const next = await bridge.unlock(password);
      unlockPasswordInput.value = "";
      return next;
    });
  });

  exportKeyButton.addEventListener("click", function handleExportKey() {
    debugLog("shell", "export_key_click");
    void withAction(exportEncryptedKeyFile);
  });

  restartBackendButton.addEventListener("click", function handleRestart() {
    debugLog("shell", "restart_backend_click");
    void withAction(() => bridge.restartBackend());
  });

  errorBannerCopyButton.addEventListener("click", function handleCopyShellBanner() {
    void copyPlainText(errorBannerText.textContent || "").catch((error) => {
      showShellError(error instanceof Error ? error.message : String(error), "复制失败");
    });
  });

  backendErrorCopyButton.addEventListener("click", function handleCopyBackendBanner() {
    void copyPlainText(backendErrorBannerText.textContent || "").catch((error) => {
      showShellError(error instanceof Error ? error.message : String(error), "复制失败");
    });
  });

  shellErrorCopyButton.addEventListener("click", function handleCopyShellError() {
    void copyPlainText(shellErrorText.value || "").catch((error) => {
      shellErrorSummary.textContent = error instanceof Error ? error.message : String(error);
      selectShellErrorText();
    });
  });

  shellErrorSelectButton.addEventListener("click", function handleSelectShellError() {
    selectShellErrorText();
  });

  shellErrorCloseButton.addEventListener("click", function handleCloseShellError() {
    hideShellError();
  });

  shellErrorModal.addEventListener("click", function handleShellErrorBackdrop(event) {
    if (event.target === shellErrorModal) {
      hideShellError();
    }
  });

  const unsubscribe = bridge.onState(renderState);
  ensureWalletAutoRefreshLoop();
  window.addEventListener("resize", function handleResize() {
    applySidebarWidth(sidebarWidthPx);
    refreshNavigationButtons();
  });
  window.addEventListener("beforeunload", function cleanup() {
    debugLog("shell", "beforeunload");
    stopWalletAutoRefreshLoop();
    unsubscribe();
  });

  void bridge.getState().then((state) => {
    if (state.viewerPreloadPath) {
      webview.setAttribute("preload", state.viewerPreloadPath);
      debugLog("shell", "viewer_preload_set", {
        preload: state.viewerPreloadPath
      });
    }
    debugLog("shell", "initial_state_loaded", {
      backend_phase: String(state?.backend?.phase || ""),
      current_url: String(state?.currentURL || "")
    });
    renderState(state);
  });
})();
