import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";

const HASH_PATTERN = /^[0-9a-f]{64}$/;
const BITFS_SEED_BLOCK_SIZE = 64 * 1024;

const MIME_BY_EXTENSION = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "application/javascript; charset=utf-8"],
  [".mjs", "application/javascript; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".gif", "image/gif"],
  [".webp", "image/webp"],
  [".ico", "image/x-icon"],
  [".woff", "font/woff"],
  [".woff2", "font/woff2"],
  [".ttf", "font/ttf"],
  [".otf", "font/otf"],
  [".txt", "text/plain; charset=utf-8"],
  [".webm", "video/webm"],
  [".mp4", "video/mp4"]
]);

function toPosixPath(fileName) {
  return String(fileName || "").replace(/\\/g, "/");
}

function ensureRelativeReference(fileName) {
  const clean = String(fileName || "").replace(/^\/+/, "");
  return clean.startsWith(".") ? clean : `./${clean}`;
}

function sha256Hex(input) {
  return createHash("sha256").update(input).digest("hex");
}

function uint32BE(value) {
  const out = Buffer.allocUnsafe(4);
  out.writeUInt32BE(value >>> 0, 0);
  return out;
}

function uint64BE(value) {
  const out = Buffer.allocUnsafe(8);
  out.writeBigUInt64BE(BigInt(value), 0);
  return out;
}

// 设计说明：
// - Electron 首页 bundle 必须产出和 Go `buildSeedV1()` 完全一致的 seed hash；
// - 否则 manifest 里的 root_entry_hash 只是普通文件 sha256，workspace 扫描后永远对不上；
// - 这里直接在前端构建期复刻 BSE1 seed hash 算法，让桌面内置首页和客户端运行时共用同一身份语义。
function bitfsSeedHash(buffer) {
  const bytes = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);
  const chunkCount = Math.ceil(bytes.length / BITFS_SEED_BLOCK_SIZE);
  const parts = [
    Buffer.from("BSE1", "utf8"),
    Buffer.from([0x01, 0x01]),
    uint32BE(BITFS_SEED_BLOCK_SIZE),
    uint64BE(bytes.length),
    uint32BE(chunkCount)
  ];

  for (let index = 0; index < chunkCount; index += 1) {
    const start = index * BITFS_SEED_BLOCK_SIZE;
    const end = Math.min(start + BITFS_SEED_BLOCK_SIZE, bytes.length);
    const chunk = Buffer.alloc(BITFS_SEED_BLOCK_SIZE, 0);
    bytes.copy(chunk, 0, start, end);
    parts.push(createHash("sha256").update(chunk).digest());
  }

  const seedBytes = Buffer.concat(parts);
  return createHash("sha256").update(seedBytes).digest("hex");
}

function inferMimeType(fileName, kind) {
  const ext = path.posix.extname(fileName).toLowerCase();
  const fromExt = MIME_BY_EXTENSION.get(ext);
  if (fromExt) {
    return fromExt;
  }
  if (kind === "chunk") {
    return "application/javascript; charset=utf-8";
  }
  return "application/octet-stream";
}

function isTextMime(mime) {
  return mime.startsWith("text/") || mime.startsWith("application/javascript") || mime.startsWith("application/json") || mime === "image/svg+xml";
}

function buildReferenceCandidates(sourceName, targetName) {
  const sourceDir = path.posix.dirname(sourceName);
  const relative = toPosixPath(path.posix.relative(sourceDir, targetName));
  const normalizedRelative = relative === "" ? path.posix.basename(targetName) : relative;
  const rootRelative = normalizedRelative.replace(/^\.\/+/, "");
  const targetFromRoot = toPosixPath(targetName).replace(/^\.\/+/, "");
  const candidates = new Set([
    normalizedRelative,
    ensureRelativeReference(normalizedRelative),
    `/${rootRelative}`,
    toPosixPath(targetName),
    ensureRelativeReference(targetName),
    `/${targetFromRoot}`
  ]);
  return Array.from(candidates).sort((left, right) => right.length - left.length);
}

function replaceAllLiteral(text, from, to) {
  if (from === "" || from === to || !text.includes(from)) {
    return text;
  }
  return text.split(from).join(to);
}

function collectBundleFiles(bundle) {
  const files = new Map();
  const orderedNames = [];

  for (const [fileName, item] of Object.entries(bundle)) {
    const normalizedName = toPosixPath(fileName);
    if (normalizedName === "manifest.json") {
      continue;
    }
    if (item.type === "chunk") {
      const mime = inferMimeType(normalizedName, "chunk");
      files.set(normalizedName, {
        oldName: normalizedName,
        originalName: normalizedName,
        kind: "chunk",
        mime,
        isText: true,
        text: String(item.code),
        binary: null,
        rollup: item,
        dependencies: new Set()
      });
      orderedNames.push(normalizedName);
      continue;
    }
    const mime = inferMimeType(normalizedName, "asset");
    const sourceBuffer = Buffer.isBuffer(item.source)
      ? item.source
      : Buffer.from(String(item.source));
    files.set(normalizedName, {
      oldName: normalizedName,
      originalName: normalizedName,
      kind: "asset",
      mime,
      isText: isTextMime(mime),
      text: isTextMime(mime) ? sourceBuffer.toString("utf8") : null,
      binary: isTextMime(mime) ? null : sourceBuffer,
      rollup: item,
      dependencies: new Set()
    });
    orderedNames.push(normalizedName);
  }

  for (const sourceName of orderedNames) {
    const sourceFile = files.get(sourceName);
    if (!sourceFile || !sourceFile.isText || sourceFile.text === null) {
      continue;
    }
    for (const targetName of orderedNames) {
      if (sourceName === targetName) {
        continue;
      }
      const candidates = buildReferenceCandidates(sourceName, targetName);
      if (candidates.some((candidate) => sourceFile.text.includes(candidate))) {
        sourceFile.dependencies.add(targetName);
      }
    }
  }

  return { files, orderedNames };
}

function detectDependencyCycle(files, orderedNames) {
  const visiting = new Set();
  const visited = new Set();
  const stack = [];

  function walk(fileName) {
    if (visited.has(fileName)) {
      return null;
    }
    if (visiting.has(fileName)) {
      const start = stack.indexOf(fileName);
      return stack.slice(start).concat(fileName);
    }
    visiting.add(fileName);
    stack.push(fileName);
    const file = files.get(fileName);
    const deps = file ? Array.from(file.dependencies) : [];
    for (const dep of deps) {
      const cycle = walk(dep);
      if (cycle !== null) {
        return cycle;
      }
    }
    stack.pop();
    visiting.delete(fileName);
    visited.add(fileName);
    return null;
  }

  for (const fileName of orderedNames) {
    const cycle = walk(fileName);
    if (cycle !== null) {
      return cycle;
    }
  }
  return null;
}

function topoSort(files, orderedNames) {
  const visited = new Set();
  const result = [];

  function walk(fileName) {
    if (visited.has(fileName)) {
      return;
    }
    visited.add(fileName);
    const file = files.get(fileName);
    const deps = file ? Array.from(file.dependencies) : [];
    for (const dep of deps) {
      walk(dep);
    }
    result.push(fileName);
  }

  for (const fileName of orderedNames) {
    walk(fileName);
  }
  return result;
}

function buildManifest(files, hashByOldName, finalBuffers, rootEntry) {
  const entries = {};
  const manifestFiles = {};

  for (const [oldName, file] of files) {
    const hash = hashByOldName.get(oldName);
    const bytes = finalBuffers.get(oldName);
    if (!hash || !bytes) {
      continue;
    }
    if (file.mime.startsWith("text/html")) {
      entries[oldName] = hash;
    }
    manifestFiles[hash] = {
      original_name: file.originalName,
      mime: file.mime,
      size: bytes.length,
      kind: file.kind
    };
  }

  const rootEntryHash = entries[rootEntry] || entries["index.html"] || Object.values(entries)[0] || "";
  return {
    version: 1,
    root_entry: rootEntry,
    root_entry_hash: rootEntryHash,
    entries,
    files: manifestFiles
  };
}

async function loadManifest(outDir, manifestName) {
  const manifestPath = path.join(outDir, manifestName);
  const raw = await fs.readFile(manifestPath, "utf8");
  return JSON.parse(raw);
}

function sendResponse(res, status, mime, body) {
  res.statusCode = status;
  res.setHeader("content-type", mime);
  res.setHeader("cache-control", "no-cache");
  res.end(body);
}

export function bitfsHashPlugin(options = {}) {
  const rootEntry = String(options.rootEntry || "index.html");
  const manifestName = String(options.manifestName || "manifest.json");
  let resolvedConfig = null;
  let outDir = "";

  return {
    name: "bitfs-hash-plugin",
    enforce: "post",
    configResolved(config) {
      resolvedConfig = config;
      outDir = path.resolve(config.root, config.build.outDir);
    },
    generateBundle(_outputOptions, bundle) {
      if (!resolvedConfig || resolvedConfig.command !== "build") {
        return;
      }

      const { files, orderedNames } = collectBundleFiles(bundle);
      const cycle = detectDependencyCycle(files, orderedNames);
      if (cycle !== null) {
        const cycleLabel = cycle.join(" -> ");
        throw new Error(`bitfs hash plugin does not support circular output references: ${cycleLabel}`);
      }

      const evaluationOrder = topoSort(files, orderedNames);
      const hashByOldName = new Map();
      const finalBuffers = new Map();
      const finalTexts = new Map();

      for (const fileName of evaluationOrder) {
        const file = files.get(fileName);
        if (!file) {
          continue;
        }
        if (!file.isText || file.text === null) {
          const bytes = file.binary || Buffer.alloc(0);
          finalBuffers.set(fileName, bytes);
          hashByOldName.set(fileName, bitfsSeedHash(bytes));
          continue;
        }

        let text = file.text;
        for (const dependencyName of file.dependencies) {
          const dependencyHash = hashByOldName.get(dependencyName);
          if (!dependencyHash) {
            throw new Error(`missing dependency hash for ${dependencyName}`);
          }
          for (const candidate of buildReferenceCandidates(fileName, dependencyName)) {
            text = replaceAllLiteral(text, candidate, ensureRelativeReference(dependencyHash));
          }
        }
        const bytes = Buffer.from(text, "utf8");
        finalTexts.set(fileName, text);
        finalBuffers.set(fileName, bytes);
        hashByOldName.set(fileName, bitfsSeedHash(bytes));
      }

      const manifest = buildManifest(files, hashByOldName, finalBuffers, rootEntry);
      if (!manifest.root_entry_hash) {
        throw new Error(`bitfs hash plugin could not resolve root html entry: ${rootEntry}`);
      }

      for (const oldName of Object.keys(bundle)) {
        delete bundle[oldName];
      }

      for (const fileName of orderedNames) {
        const file = files.get(fileName);
        const hash = hashByOldName.get(fileName);
        const finalBuffer = finalBuffers.get(fileName);
        if (!file || !hash || !finalBuffer) {
          continue;
        }
        if (file.kind === "chunk") {
          const rewritten = finalTexts.get(fileName) || finalBuffer.toString("utf8");
          bundle[hash] = {
            ...file.rollup,
            fileName: hash,
            code: rewritten,
            map: null,
            imports: [],
            dynamicImports: [],
            implicitlyLoadedBefore: [],
            referencedFiles: []
          };
          continue;
        }
        bundle[hash] = {
          ...file.rollup,
          fileName: hash,
          source: file.isText ? finalBuffer.toString("utf8") : finalBuffer
        };
      }

      bundle[manifestName] = {
        type: "asset",
        fileName: manifestName,
        source: JSON.stringify(manifest, null, 2)
      };
    },
    configurePreviewServer(server) {
      const serveHashFile = async (req, res, next) => {
        try {
          if (!outDir) {
            return next();
          }
          const currentURL = new URL(req.url || "/", "http://bitfs-preview.local");
          const pathname = decodeURIComponent(currentURL.pathname || "/");
          const manifest = await loadManifest(outDir, manifestName).catch(() => null);
          if (!manifest) {
            return next();
          }

          let requestedHash = pathname.replace(/^\/+/, "");
          if (pathname === "/" || pathname === "/index.html") {
            requestedHash = String(manifest.root_entry_hash || "");
          }
          if (!HASH_PATTERN.test(requestedHash)) {
            return next();
          }

          const fileMeta = manifest.files ? manifest.files[requestedHash] : null;
          if (!fileMeta) {
            return next();
          }

          const body = await fs.readFile(path.join(outDir, requestedHash));
          sendResponse(res, 200, String(fileMeta.mime || "application/octet-stream"), body);
        } catch (error) {
          next(error);
        }
      };

      server.middlewares.use(serveHashFile);
    }
  };
}
