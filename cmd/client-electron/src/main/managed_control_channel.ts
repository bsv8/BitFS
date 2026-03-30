import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";

import type { BitfsRuntimeEvent } from "../shared/shell_contract";

type ManagedControlHello = {
  type?: string;
  token?: string;
  runtime_epoch?: string;
};

// 设计说明：
// - 控制消息现在走独立通道，stdout/stderr 只保留给人类日志；
// - Linux/macOS 优先用 unix socket，Windows 再退回到 loopback tcp；
// - 通道建立后先发 hello 做最小鉴权，后续只接受同一连接上的 NDJSON 事件帧。
export class ManagedControlChannel extends EventEmitter {
  private readonly server: net.Server;
  private readonly endpoint: string;
  private readonly token: string;
  private readonly socketDir: string;
  private clientSocket: net.Socket | null = null;
  private handshakeDone = false;
  private remainder = "";
  private closed = false;

  private constructor(server: net.Server, endpoint: string, token: string, socketDir: string) {
    super();
    this.server = server;
    this.endpoint = endpoint;
    this.token = token;
    this.socketDir = socketDir;
  }

  static async create(): Promise<ManagedControlChannel> {
    const token = crypto.randomBytes(16).toString("hex");
    const socketDir = fs.mkdtempSync(path.join(os.tmpdir(), "bitfs-electron-control-"));
    const server = net.createServer();
    const channel = await new Promise<ManagedControlChannel>((resolve, reject) => {
      const cleanupError = (error: Error) => {
        server.removeAllListeners();
        reject(error);
      };
      server.once("error", cleanupError);
      const handleListening = () => {
        server.removeListener("error", cleanupError);
        const address = server.address();
        if (!address) {
          reject(new Error("managed control server address is unavailable"));
          return;
        }
        if (typeof address === "string") {
          resolve(new ManagedControlChannel(server, `unix://${address}`, token, socketDir));
          return;
        }
        resolve(new ManagedControlChannel(server, `tcp://127.0.0.1:${address.port}`, token, socketDir));
      };
      if (process.platform === "win32") {
        server.listen(0, "127.0.0.1", handleListening);
        return;
      }
      const socketPath = path.join(socketDir, "managed-control.sock");
      server.listen(socketPath, handleListening);
    });
    channel.attachServerHandlers();
    return channel;
  }

  getEndpoint(): string {
    return this.endpoint;
  }

  getToken(): string {
    return this.token;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.clientSocket?.destroy();
    this.clientSocket = null;
    this.remainder = "";
    this.handshakeDone = false;
    await new Promise<void>((resolve) => {
      this.server.close(() => resolve());
    });
    await fsPromises.rm(this.socketDir, { recursive: true, force: true });
  }

  private attachServerHandlers(): void {
    this.server.on("connection", (socket) => {
      if (this.clientSocket) {
        socket.destroy();
        return;
      }
      this.clientSocket = socket;
      this.handshakeDone = false;
      this.remainder = "";
      socket.setEncoding("utf8");
      socket.on("data", (chunk: string) => {
        const extracted = extractTextLines(chunk, this.remainder);
        this.remainder = extracted.remainder;
        for (const line of extracted.lines) {
          this.handleLine(line, socket);
        }
      });
      socket.on("close", () => {
        if (this.clientSocket === socket) {
          this.clientSocket = null;
          this.handshakeDone = false;
          this.remainder = "";
        }
      });
      socket.on("error", (error) => {
        this.emit("channel_error", error);
      });
    });
    this.server.on("error", (error) => {
      this.emit("channel_error", error);
    });
  }

  private handleLine(line: string, socket: net.Socket): void {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch (error) {
      this.emit("channel_error", error instanceof Error ? error : new Error(String(error)));
      socket.destroy();
      return;
    }
    if (!this.handshakeDone) {
      if (!isManagedControlHello(parsed) || parsed.token !== this.token) {
        this.emit("channel_error", new Error("managed control hello is invalid"));
        socket.destroy();
        return;
      }
      this.handshakeDone = true;
      this.emit("hello", {
        runtime_epoch: String(parsed.runtime_epoch || "").trim()
      });
      return;
    }
    if (!isRuntimeEvent(parsed)) {
      this.emit("channel_error", new Error("managed control event frame is invalid"));
      socket.destroy();
      return;
    }
    this.emit("event", parsed);
  }
}

function extractTextLines(chunk: string, remainder: string): { lines: string[]; remainder: string } {
  const text = `${remainder}${String(chunk || "")}`;
  const parts = text.split(/\r?\n/);
  const nextRemainder = parts.pop() ?? "";
  return {
    lines: parts.map((line) => line.trim()).filter((line) => line !== ""),
    remainder: nextRemainder
  };
}

function isManagedControlHello(value: unknown): value is ManagedControlHello {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return String(record.type || "").trim() === "hello";
}

function isRuntimeEvent(value: unknown): value is BitfsRuntimeEvent {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return typeof record.topic === "string" && record.topic.trim() !== "" &&
    typeof record.runtime_epoch === "string" &&
    Number.isFinite(Number(record.seq));
}
