#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const scriptDir = __dirname;
const electronRoot = path.resolve(scriptDir, "..");
const bitfsRoot = path.resolve(electronRoot, "..", "..");
const preferredGo = "/home/david/.gvm/gos/go1.26.0/bin/go";
const fallbackGo = "/usr/bin/go";

function getBinaryName() {
  return process.platform === "win32" ? "bitfs-client.exe" : "bitfs-client";
}

function getPlatformKey() {
  return `${process.platform}-${process.arch}`;
}

function resolveGoBinary() {
  if (fs.existsSync(preferredGo)) {
    return { command: preferredGo, env: { ...process.env } };
  }
  return {
    command: fallbackGo,
    env: {
      ...process.env,
      GOTOOLCHAIN: process.env.GOTOOLCHAIN || "auto"
    }
  };
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function main() {
  const outputDir = path.join(electronRoot, "resources", "bin", getPlatformKey());
  const outputPath = path.join(outputDir, getBinaryName());
  ensureDir(outputDir);

  const go = resolveGoBinary();
  const args = ["build", "-o", outputPath, "./cmd/client"];
  const child = spawnSync(go.command, args, {
    cwd: bitfsRoot,
    env: go.env,
    stdio: "inherit"
  });
  if (child.error) {
    throw child.error;
  }
  if (child.status !== 0) {
    process.exit(child.status || 1);
  }
  process.stdout.write(`[build-go-client] output: ${outputPath}\n`);
}

main();
