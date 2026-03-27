import { debugLogger } from "./debug_logger";
import type { ElectronE2EEvent, ElectronE2EEventSource, ElectronE2EObserverState } from "../shared/shell_contract";

const maxBufferedEvents = 256;

// ElectronE2EObserver 统一承接 shell / viewer / main 的测试观测消息。
// 设计说明：
// - e2e 不应该靠翻日志猜“错误框有没有出来”，而是读取正式事件流和状态快照；
// - 这里故意只维护观测状态，不承接任何业务决策，避免测试通道反过来影响产品逻辑；
// - 事件和状态同时保留：事件给时序判断，状态给最终断言和失败排查。
export class ElectronE2EObserver {
  private readonly enabled: boolean;
  private nextSeq = 1;
  private readonly events: ElectronE2EEvent[] = [];
  private readonly state: ElectronE2EObserverState;

  constructor(enabled: boolean) {
    this.enabled = enabled;
    this.state = {
      enabled,
      last_seq: 0,
      report_count: 0,
      last_report_source: "",
      last_report_title: "",
      last_report_message: "",
      last_report_page_url: "",
      modal_visible: false,
      stop_current_page_visible: false,
      viewer_frozen: false,
      viewer_frozen_url: "",
      viewer_frozen_reason: ""
    };
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  snapshot(): ElectronE2EObserverState {
    return {
      ...this.state
    };
  }

  eventsAfter(afterSeq: number): ElectronE2EEvent[] {
    if (!this.enabled) {
      return [];
    }
    const normalizedAfterSeq = Math.max(0, Math.floor(Number(afterSeq || 0)));
    return this.events
      .filter((event) => event.seq > normalizedAfterSeq)
      .map((event) => ({
        ...event,
        fields: { ...event.fields }
      }));
  }

  emit(source: ElectronE2EEventSource, name: string, fields?: Record<string, unknown>): ElectronE2EEvent | null {
    if (!this.enabled) {
      return null;
    }
    const event: ElectronE2EEvent = {
      seq: this.nextSeq++,
      source,
      name: String(name || "").trim() || "unknown",
      occurred_at_unix: Math.floor(Date.now() / 1000),
      fields: normalizeFields(fields)
    };
    this.events.push(event);
    if (this.events.length > maxBufferedEvents) {
      this.events.shift();
    }
    this.state.last_seq = event.seq;
    this.applyState(event);
    debugLogger.log("e2e.event", event.name, {
      seq: event.seq,
      source: event.source,
      fields: event.fields
    });
    return event;
  }

  private applyState(event: ElectronE2EEvent): void {
    if (event.name === "shell.error.reported" || event.name === "shell.error.modal.shown") {
      const source = readStringField(event.fields, "report_source");
      const title = readStringField(event.fields, "report_title");
      const message = readStringField(event.fields, "report_message");
      const pageURL = readStringField(event.fields, "report_page_url");
      if (source === "main-process" || source === "shell-renderer" || source === "viewer" || source === "settings") {
        this.state.last_report_source = source;
      }
      this.state.last_report_title = title;
      this.state.last_report_message = message;
      this.state.last_report_page_url = pageURL;
      if (event.name === "shell.error.reported") {
        this.state.report_count += 1;
      }
    }
    if (event.name === "shell.error.modal.shown") {
      this.state.modal_visible = true;
      this.state.stop_current_page_visible = readBooleanField(event.fields, "can_stop_current_page");
      return;
    }
    if (event.name === "shell.error.modal.hidden") {
      this.state.modal_visible = false;
      this.state.stop_current_page_visible = false;
      return;
    }
    if (event.name === "shell.viewer.frozen") {
      this.state.viewer_frozen = true;
      this.state.viewer_frozen_url = readStringField(event.fields, "viewer_url");
      this.state.viewer_frozen_reason = readStringField(event.fields, "viewer_reason");
      return;
    }
    if (event.name === "shell.viewer.unfrozen") {
      this.state.viewer_frozen = false;
      this.state.viewer_frozen_url = "";
      this.state.viewer_frozen_reason = "";
    }
  }
}

function normalizeFields(fields?: Record<string, unknown>): Record<string, unknown> {
  if (!fields) {
    return {};
  }
  return { ...fields };
}

function readStringField(fields: Record<string, unknown>, key: string): string {
  const value = fields[key];
  if (typeof value === "string") {
    return value.trim();
  }
  return "";
}

function readBooleanField(fields: Record<string, unknown>, key: string): boolean {
  const value = fields[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "1" || normalized === "true" || normalized === "yes";
  }
  return false;
}
