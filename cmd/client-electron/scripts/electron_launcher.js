#!/usr/bin/env node

const fs = require("node:fs");
const { spawn } = require("node:child_process");

const GENTOO_ELECTRON_36_WORKAROUND_KEYS = [
  "XDG_CURRENT_DESKTOP",
  "XDG_SESSION_DESKTOP",
  "DESKTOP_SESSION"
];
const GENTOO_ELECTRON_36_WORKAROUND_ARGS = ["--in-process-gpu"];

function isGentooLinux() {
  if (process.platform !== "linux") {
    return false;
  }

  // 设计说明：
  // - 这里优先用 Gentoo 自己稳定存在的标记文件判断；
  // - 不把规避逻辑扩散到所有 Linux，避免未来在别的发行版上误伤正常桌面集成。
  return fs.existsSync("/etc/gentoo-release");
}

function getElectronMajorVersion() {
  const version = require("electron/package.json").version;
  const major = Number.parseInt(version.split(".")[0], 10);
  if (!Number.isInteger(major)) {
    throw new Error(`failed to parse electron version: ${version}`);
  }
  return major;
}

function buildElectronEnv() {
  const env = { ...process.env };

  // 避坑说明：
  // - 某些终端环境会长期带着 ELECTRON_RUN_AS_NODE；
  // - 直接继承它会让 electron 二进制按 node 模式运行，表现成“看起来命令没错，但应用根本没启动”。
  delete env.ELECTRON_RUN_AS_NODE;

  const electronMajor = getElectronMajorVersion();
  if (isGentooLinux() && electronMajor === 36) {
    for (const key of GENTOO_ELECTRON_36_WORKAROUND_KEYS) {
      delete env[key];
    }
    process.stderr.write(
      "[electron-launcher] Applied Gentoo Electron 36 desktop-session workaround.\n"
    );
  }

  return env;
}

function buildElectronArgs(originalArgs) {
  const electronMajor = getElectronMajorVersion();
  if (!isGentooLinux() || electronMajor !== 36) {
    return originalArgs;
  }

  const args = [...originalArgs];
  for (const flag of GENTOO_ELECTRON_36_WORKAROUND_ARGS) {
    if (!args.includes(flag)) {
      args.unshift(flag);
    }
  }
  return args;
}

function main() {
  const electronBinary = require("electron");
  const args = buildElectronArgs(process.argv.slice(2));
  const env = buildElectronEnv();

  const child = spawn(electronBinary, args, {
    stdio: "inherit",
    env
  });

  child.on("error", (error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`failed to launch electron: ${message}`);
    process.exit(1);
  });

  child.on("exit", (code, signal) => {
    if (signal) {
      console.error(`${electronBinary} exited with signal ${signal}`);
      process.exit(1);
    }
    process.exit(code ?? 0);
  });
}

main();
