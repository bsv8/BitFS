import { ens_normalize } from "@adraffy/ens-normalize";

export type ParsedBitfsLocator = {
  kind: "bitfs";
  locator: string;
  viewerURL: string;
  seedHash: string;
};

export type ParsedNodeLocator = {
  kind: "node";
  locator: string;
  nodePubkeyHex: string;
  route: string;
};

export type ParsedResolverLocator = {
  kind: "resolver";
  locator: string;
  resolverPubkeyHex: string;
  name: string;
  route: string;
};

export type ParsedLocator = ParsedBitfsLocator | ParsedNodeLocator | ParsedResolverLocator;

export type LocatorVisitContext = {
  visitID: string;
  visitLocator: string;
};

export type LocatorHandlerSet = {
  resolveNodeLocator?: (locator: ParsedNodeLocator, visit: LocatorVisitContext) => Promise<{ seedHash: string }>;
  resolveResolverLocator?: (
    locator: ParsedResolverLocator,
    visit: LocatorVisitContext
  ) => Promise<{ seedHash: string; targetPubkeyHex?: string }>;
};

const seedHashPattern = /^[0-9a-f]{64}$/;
const compressedPubkeyPattern = /^(02|03)[0-9a-f]{64}$/;

// 设计说明：
// - Electron 壳的地址栏不是单纯的 bitfs 内容地址，而是“人类输入入口”；
// - `bitfs:`、`node:`、`<解析服务公钥>:` 都先收敛成统一 locator，再决定后续业务链路；
// - 这里先把语法与规范化固定下来，真正的名字解析协议和节点寻址协议以后继续接。
export function parseLocator(raw: string): ParsedLocator {
  const value = String(raw || "").trim();
  if (value === "") {
    throw new Error("locator is required");
  }

  const bitfsSeedHash = parseBitfsSeedHash(value);
  if (bitfsSeedHash !== "") {
    return {
      kind: "bitfs",
      locator: canonicalBitfsLocator(bitfsSeedHash),
      viewerURL: buildBitfsViewerURL(bitfsSeedHash),
      seedHash: bitfsSeedHash
    };
  }

  const lowerValue = value.toLowerCase();
  if (lowerValue.startsWith("node:")) {
    const body = value.slice("node:".length);
    const { id, route } = splitIDAndRoute(body, "node locator");
    const nodePubkeyHex = normalizeCompressedPubkeyHex(id);
    if (nodePubkeyHex === "") {
      throw new Error("node locator requires a compressed pubkey hex");
    }
    return {
      kind: "node",
      locator: `node:${nodePubkeyHex}${route === "index" ? "" : `/${route}`}`,
      nodePubkeyHex,
      route
    };
  }

  const separatorIndex = value.indexOf(":");
  if (separatorIndex <= 0) {
    throw new Error("invalid locator");
  }
  const resolverPubkeyHex = normalizeCompressedPubkeyHex(value.slice(0, separatorIndex));
  if (resolverPubkeyHex === "") {
    throw new Error("invalid locator");
  }
  const body = value.slice(separatorIndex + 1);
  const { id, route } = splitIDAndRoute(body, "resolver locator");
  const name = normalizeResolverName(id);
  return {
    kind: "resolver",
    locator: `${resolverPubkeyHex}:${name}${route === "index" ? "" : `/${route}`}`,
    resolverPubkeyHex,
    name,
    route
  };
}

export function canonicalBitfsLocator(seedHash: string): string {
  const normalized = normalizeSeedHash(seedHash);
  if (normalized === "") {
    throw new Error("invalid seed hash");
  }
  return `bitfs:${normalized}`;
}

export function buildBitfsViewerURL(seedHash: string): string {
  const normalized = normalizeSeedHash(seedHash);
  if (normalized === "") {
    throw new Error("invalid seed hash");
  }
  return `bitfs://${normalized}`;
}

function parseBitfsSeedHash(raw: string): string {
  const trimmed = String(raw || "").trim();
  if (trimmed === "") {
    return "";
  }
  const directSeedHash = normalizeSeedHash(trimmed);
  if (directSeedHash !== "") {
    return directSeedHash;
  }
  const lowerValue = trimmed.toLowerCase();
  if (!lowerValue.startsWith("bitfs:")) {
    return "";
  }
  const body = trimmed.slice("bitfs:".length).replace(/^\/\//, "").replace(/^\/+/, "");
  const separatorIndex = body.indexOf("/");
  if (separatorIndex >= 0) {
    return "";
  }
  return normalizeSeedHash(body);
}

function splitIDAndRoute(raw: string, label: string): { id: string; route: string } {
  const value = String(raw || "").trim();
  if (value === "") {
    throw new Error(`${label} is required`);
  }
  const separatorIndex = value.indexOf("/");
  if (separatorIndex < 0) {
    return {
      id: value,
      route: "index"
    };
  }
  const id = value.slice(0, separatorIndex).trim();
  const route = value.slice(separatorIndex + 1).trim();
  if (id === "") {
    throw new Error(`${label} target is required`);
  }
  if (route === "") {
    throw new Error(`${label} route is required`);
  }
  return { id, route };
}

function normalizeResolverName(raw: string): string {
  const value = String(raw || "").trim();
  if (value === "") {
    throw new Error("resolver name is required");
  }
  if (value.includes("/")) {
    throw new Error("resolver name must not contain /");
  }
  try {
    const normalized = ens_normalize(value);
    if (normalized.includes("/")) {
      throw new Error("resolver name must not contain /");
    }
    return normalized;
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : "resolver name invalid");
  }
}

function normalizeSeedHash(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!seedHashPattern.test(value)) {
    return "";
  }
  return value;
}

function normalizeCompressedPubkeyHex(raw: string): string {
  const value = String(raw || "").trim().toLowerCase();
  if (!compressedPubkeyPattern.test(value)) {
    return "";
  }
  return value;
}
