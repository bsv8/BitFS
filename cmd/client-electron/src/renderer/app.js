(function bootstrapRenderer() {
  const bridge = window.bitfsShell;
  const addressForm = document.getElementById("address-form");
  const addressInput = document.getElementById("address-input");
  const backButton = document.getElementById("back-button");
  const forwardButton = document.getElementById("forward-button");
  const reloadButton = document.getElementById("reload-button");
  const goHomeButton = document.getElementById("go-home-button");
  const openButton = document.getElementById("open-button");
  const homeButton = document.getElementById("home-button");
  const detailsToggleButton = document.getElementById("details-toggle-button");
  const detailsCloseButton = document.getElementById("details-close-button");
  const detailsDrawer = document.getElementById("details-drawer");
  const lockButton = document.getElementById("lock-button");
  const budgetForm = document.getElementById("budget-form");
  const singleBudgetInput = document.getElementById("single-budget-input");
  const pageBudgetInput = document.getElementById("page-budget-input");
  const currentURL = document.getElementById("current-url");
  const autoSpent = document.getElementById("auto-spent");
  const pendingCount = document.getElementById("pending-count");
  const clientAPIBase = document.getElementById("client-api-base");
  const homeSource = document.getElementById("home-source");
  const permissionBar = document.getElementById("permission-bar");
  const pendingSummary = document.getElementById("pending-summary");
  const drawerPendingNote = document.getElementById("drawer-pending-note");
  const pendingList = document.getElementById("pending-list");
  const approveButton = document.getElementById("approve-button");
  const permissionDetailsButton = document.getElementById("permission-details-button");
  const errorBanner = document.getElementById("error-banner");
  const errorBannerText = document.getElementById("error-banner-text");
  const errorBannerCopyButton = document.getElementById("error-banner-copy-button");
  const webview = document.getElementById("content-view");
  const backendPhaseText = document.getElementById("backend-phase-text");
  const backendOverlay = document.getElementById("backend-overlay");
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
    !bridge || !addressForm || !addressInput || !backButton || !forwardButton || !reloadButton ||
    !goHomeButton || !openButton || !homeButton || !detailsToggleButton || !detailsCloseButton ||
    !detailsDrawer || !budgetForm || !singleBudgetInput || !pageBudgetInput || !currentURL ||
    !autoSpent || !pendingCount || !clientAPIBase || !homeSource || !permissionBar || !pendingSummary ||
    !drawerPendingNote || !pendingList || !approveButton || !permissionDetailsButton || !errorBanner ||
    !errorBannerText || !errorBannerCopyButton || !webview || !backendPhaseText || !backendOverlay ||
    !backendTitle || !backendSummary || !backendDetail || !backendStepNote || !backendErrorBanner ||
    !backendErrorBannerText || !backendErrorCopyButton || !choosePanel || !chooseCreateButton ||
    !chooseImportButton || !createKeyForm || !createPasswordInput || !createPasswordConfirmInput ||
    !createBackButton || !unlockForm || !unlockPasswordInput || !exportKeyButton ||
    !restartPanel || !restartBackendButton || !lockButton || !shellErrorModal || !shellErrorTitle ||
    !shellErrorSummary || !shellErrorText || !shellErrorCopyButton || !shellErrorSelectButton ||
    !shellErrorCloseButton
  ) {
    throw new Error("bitfs shell bootstrap failed");
  }

  let viewerDOMReady = false;
  let drawerOpen = false;
  let currentState = null;
  let onboardingStep = "choose-init";
  let lastBackendPhase = "";
  let lastRenderedURL = "";
  let lastPendingCount = -1;
  let lastShellError = "";

  function debugLog(scope, event, fields) {
    if (!bridge || typeof bridge.debugLog !== "function") {
      return;
    }
    bridge.debugLog(scope, event, fields || {});
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

  function escapeHTML(raw) {
    return String(raw || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;");
  }

  function renderPendingItem(item) {
    const price = Number(item.estimatedTotalSat || 0);
    const mimeHint = String(item.mimeHint || "").trim();
    const fileName = String(item.recommendedFileName || "").trim();
    const sizeLabel = formatBytes(item.fileSize);
    const reason = String(item.reason || "等待许可");
    return `
      <article class="pending-item">
        <div class="pending-copy">
          <strong>${escapeHTML(fileName || item.seedHash)}</strong>
          <span class="pending-hash">${escapeHTML(item.seedHash)}</span>
          <span>${escapeHTML(sizeLabel)} · ${Number(item.chunkCount || 0)} 块${mimeHint ? ` · ${escapeHTML(mimeHint)}` : ""}</span>
          <span>${escapeHTML(reason)}</span>
        </div>
        <div class="pending-price">${price} sat</div>
      </article>
    `;
  }

  function isBackendReady(state) {
    return String(state?.backend?.phase || "") === "ready";
  }

  function getEffectiveHomeSeedHash(state) {
    return String(state?.userHomeSeedHash || state?.backend?.defaultHomeSeedHash || "").trim();
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
    approveButton.disabled = !enabled;
    singleBudgetInput.disabled = !enabled;
    pageBudgetInput.disabled = !enabled;
    lockButton.disabled = !enabled;
    detailsToggleButton.disabled = !enabled;
    permissionDetailsButton.disabled = !enabled;
    refreshNavigationButtons(enabled);
  }

  function setDrawerOpen(open) {
    drawerOpen = Boolean(open);
    detailsDrawer.classList.toggle("is-open", drawerOpen);
    detailsToggleButton.classList.toggle("is-active", drawerOpen);
    debugLog("shell", "drawer_toggled", {
      open: drawerOpen
    });
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
    const phaseTextMap = {
      starting: "内置客户端正在启动",
      locked: hasKey ? "内置客户端已锁定，等待解锁" : "内置客户端未初始化密钥",
      ready: "内置客户端已就绪",
      error: "内置客户端异常退出",
      stopped: "内置客户端已停止"
    };
    backendPhaseText.textContent = phaseTextMap[phase] || "内置客户端状态未知";
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
    clientAPIBase.textContent = String(backend.apiBase || state.clientAPIBase || "-");
    lockButton.classList.toggle("is-hidden", phase !== "ready");

    if (phase === "ready") {
      backendOverlay.classList.add("is-hidden");
      backendErrorBanner.classList.add("is-hidden");
      setOnboardingStep("checking");
      setInteractiveEnabled(true);
      return;
    }

    setInteractiveEnabled(false);
    backendOverlay.classList.remove("is-hidden");
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
        backendStepNote.textContent = "创建完成后还需要再输入密码解锁，才能真正启动 Go 运行时。";
      } else {
        backendTitle.textContent = "初始化客户端私钥";
        backendSummary.textContent = "这是第一次启动。先选择新建密文私钥，或导入已经存在的密文私钥。";
        backendStepNote.textContent = "导入与导出都只处理密文私钥 JSON，不处理明文私钥。";
      }
    } else if (phase === "locked") {
      backendTitle.textContent = "客户端已锁定";
      backendSummary.textContent = "输入密码后解锁 Go 客户端，BitFS 页面才会真正装载。";
      backendStepNote.textContent = "如果你还没有备份当前密文私钥，可以先导出到安全位置。";
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
    currentURL.textContent = state.currentURL || "尚未打开";
    if (document.activeElement !== addressInput) {
      addressInput.value = state.currentURL || "";
    }
    autoSpent.textContent = `${Number(state.autoSpentSat || 0)} sat`;
    const pending = Array.isArray(state.pending) ? state.pending : [];
    if (lastPendingCount !== pending.length) {
      lastPendingCount = pending.length;
      debugLog("shell", "pending_changed", {
        pending_count: pending.length,
        auto_spent_sat: Number(state.autoSpentSat || 0)
      });
    }
    pendingCount.textContent = String(pending.length);

    if (state.userHomeSeedHash) {
      homeSource.textContent = `浏览器自定义：${state.userHomeSeedHash}`;
    } else if (state.backend?.defaultHomeSeedHash) {
      homeSource.textContent = `客户端默认：${state.backend.defaultHomeSeedHash}`;
    } else {
      homeSource.textContent = "未配置首页";
    }

    if (document.activeElement !== singleBudgetInput) {
      singleBudgetInput.value = String(state.staticSingleMaxSat || "");
    }
    if (document.activeElement !== pageBudgetInput) {
      pageBudgetInput.value = String(state.staticPageMaxSat || "");
    }

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

    const total = pending.reduce((sum, item) => sum + Number(item.estimatedTotalSat || 0), 0);
    permissionBar.classList.toggle("is-hidden", pending.length === 0);
    pendingSummary.textContent = pending.length === 0
      ? "当前没有挂起资源"
      : `当前挂起 ${pending.length} 个资源，预计总价 ${total} sat，等待用户许可后继续装载。`;
    drawerPendingNote.textContent = pending.length === 0
      ? "当前没有挂起资源。"
      : `共有 ${pending.length} 个资源挂起，预计总价 ${total} sat。`;
    pendingList.innerHTML = pending.map(renderPendingItem).join("");
    approveButton.disabled = pending.length === 0 || !isBackendReady(state);
    permissionDetailsButton.disabled = !isBackendReady(state);

    const currentSeedHash = normalizeTarget(state.currentURL || "");
    const userHomeSeedHash = String(state.userHomeSeedHash || "");
    homeButton.textContent = userHomeSeedHash && currentSeedHash === userHomeSeedHash ? "清除首页" : "设为首页";
    goHomeButton.disabled = !isBackendReady(state) || getEffectiveHomeSeedHash(state) === "";

    renderBackendState(state);
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

  detailsToggleButton.addEventListener("click", function handleToggleDrawer() {
    setDrawerOpen(!drawerOpen);
  });

  detailsCloseButton.addEventListener("click", function handleCloseDrawer() {
    setDrawerOpen(false);
  });

  permissionDetailsButton.addEventListener("click", function handleOpenPermissionDetails() {
    setDrawerOpen(true);
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

  approveButton.addEventListener("click", function handleApproveClick() {
    debugLog("shell", "approve_pending_click");
    void withAction(async function approvePending() {
      const next = await bridge.approvePending();
      if (viewerDOMReady && webview.getURL()) {
        debugLog("webview", "reload_after_approve", {
          url: webview.getURL()
        });
        webview.reload();
      }
      return next;
    });
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
      const next = await bridge.createKey(password);
      createPasswordInput.value = "";
      createPasswordConfirmInput.value = "";
      onboardingStep = "unlock-key";
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
  window.addEventListener("beforeunload", function cleanup() {
    debugLog("shell", "beforeunload");
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
