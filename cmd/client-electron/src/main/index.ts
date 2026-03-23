import { app, protocol, session } from "electron";
import path from "node:path";

import { createAppWindow } from "./app_window";
import { BrowserSettingsStore } from "./browser_settings";
import { BitfsBrowserRuntime } from "./browser_runtime";
import { createBitfsProtocolHandler } from "./bitfs_protocol";
import { ManagedClientSupervisor } from "./client_supervisor";
import { debugLogger } from "./debug_logger";
import { registerShellIPC } from "./ipc_bridge";
import { isTrustedNavigationURL, isTrustedRequestURL } from "./navigation_guard";

let mainWindowCreated = false;
let runtime: BitfsBrowserRuntime | null = null;
let supervisor: ManagedClientSupervisor | null = null;
let settings: BrowserSettingsStore | null = null;

function installTrustedWorldGuards(): void {
  app.on("web-contents-created", (_event, contents) => {
    contents.on("will-navigate", (navEvent, url) => {
      if (isTrustedNavigationURL(url)) {
        debugLogger.log("guard", "navigation_allowed", {
          url,
          contents_id: contents.id
        });
        return;
      }
      debugLogger.log("guard", "navigation_blocked", {
        url,
        contents_id: contents.id
      });
      navEvent.preventDefault();
    });
    contents.setWindowOpenHandler(({ url }) => {
      if (isTrustedNavigationURL(url)) {
        debugLogger.log("guard", "window_open_allowed", {
          url,
          contents_id: contents.id
        });
        return { action: "allow" };
      }
      debugLogger.log("guard", "window_open_blocked", {
        url,
        contents_id: contents.id
      });
      return { action: "deny" };
    });
  });
  const currentSession = session.defaultSession;
  currentSession.webRequest.onBeforeRequest((details, callback) => {
    const allowed = isTrustedRequestURL(details.url);
    if (!allowed) {
      debugLogger.log("guard", "request_blocked", {
        url: details.url,
        resource_type: details.resourceType,
        method: details.method,
        web_contents_id: details.webContentsId
      });
    }
    callback({ cancel: !allowed });
  });
}

async function bootstrap(): Promise<void> {
  await app.whenReady();
  debugLogger.init({ userDataDir: app.getPath("userData") });
  debugLogger.log("bootstrap", "app_ready", {
    app_path: app.getAppPath(),
    user_data_dir: app.getPath("userData"),
    packaged: app.isPackaged,
    log_file_path: debugLogger.getLogFilePath()
  });
  installTrustedWorldGuards();
  const viewerPreloadPath = path.join(app.getAppPath(), "src", "renderer", "viewer-preload.js");
  settings = new BrowserSettingsStore(app.getPath("userData"));
  debugLogger.log("bootstrap", "settings_ready", {
    viewer_preload_path: viewerPreloadPath
  });
  supervisor = await ManagedClientSupervisor.create({
    appRootDir: app.getAppPath(),
    packaged: app.isPackaged,
    userDataDir: app.getPath("userData")
  });
  debugLogger.log("bootstrap", "supervisor_created", supervisor.snapshot());
  runtime = new BitfsBrowserRuntime(supervisor.snapshot().apiBase, viewerPreloadPath);
  protocol.handle("bitfs", createBitfsProtocolHandler(runtime));
  debugLogger.log("bootstrap", "protocol_registered", {
    scheme: "bitfs"
  });
  if (!mainWindowCreated) {
    const window = createAppWindow(app.getAppPath());
    registerShellIPC(window, runtime, supervisor, settings);
    mainWindowCreated = true;
    debugLogger.log("bootstrap", "main_window_created", {
      window_id: window.id
    });
  }
  const maybeOpenInitialHomepage = () => {
    if (!runtime || !supervisor || !settings) {
      return;
    }
    if (supervisor.snapshot().phase !== "ready") {
      return;
    }
    if (runtime.snapshot().currentURL !== "") {
      return;
    }
    const target = settings.snapshot().userHomeSeedHash || supervisor.snapshot().defaultHomeSeedHash;
    if (target === "") {
      debugLogger.log("bootstrap", "initial_homepage_missing");
      return;
    }
    debugLogger.log("bootstrap", "initial_homepage_open", {
      seed_hash: target,
      source: settings.snapshot().userHomeSeedHash ? "user" : "client"
    });
    runtime.openRoot(target);
  };
  supervisor.on("state", maybeOpenInitialHomepage);
  settings.on("change", maybeOpenInitialHomepage);
  debugLogger.log("bootstrap", "supervisor_start_requested");
  void supervisor.start();
}

app.on("window-all-closed", () => {
  debugLogger.log("bootstrap", "window_all_closed", {
    platform: process.platform
  });
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", (event) => {
  if (!supervisor) {
    return;
  }
  event.preventDefault();
  const current = supervisor;
  supervisor = null;
  debugLogger.log("bootstrap", "before_quit_stop_supervisor");
  void current.stop().finally(() => {
    debugLogger.log("bootstrap", "app_exit");
    app.exit(0);
  });
});

app.on("activate", () => {
  if (mainWindowCreated || runtime === null || supervisor === null || settings === null) {
    return;
  }
  const window = createAppWindow(app.getAppPath());
  registerShellIPC(window, runtime, supervisor, settings);
  mainWindowCreated = true;
  debugLogger.log("bootstrap", "main_window_recreated", {
    window_id: window.id
  });
});

void bootstrap().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  debugLogger.log("bootstrap", "bootstrap_failed", {
    message
  });
  console.error("electron bootstrap failed:", message);
  app.exit(1);
});
