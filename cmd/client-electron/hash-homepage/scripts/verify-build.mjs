import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.resolve(currentDir, "..", "dist-hash");
const manifestPath = path.join(distDir, "manifest.json");
const hashPattern = /^[0-9a-f]{64}$/;

function collectHashReferences(text) {
  return Array.from(text.matchAll(/\.\/([0-9a-f]{64})/g), (match) => match[1]);
}

const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"));
if (!hashPattern.test(String(manifest.root_entry_hash || ""))) {
  throw new Error("root_entry_hash missing or invalid");
}

const names = await fs.readdir(distDir);
for (const name of names) {
  if (name === "manifest.json") {
    continue;
  }
  if (!hashPattern.test(name)) {
    throw new Error(`non hash output detected: ${name}`);
  }
}

for (const [hash, meta] of Object.entries(manifest.files || {})) {
  if (!hashPattern.test(hash)) {
    throw new Error(`invalid file hash in manifest: ${hash}`);
  }
  const filePath = path.join(distDir, hash);
  const body = await fs.readFile(filePath);
  if (Number(meta.size || 0) !== body.length) {
    throw new Error(`size mismatch for ${hash}`);
  }
  if (String(meta.mime || "").startsWith("text/") || String(meta.mime || "").startsWith("application/javascript")) {
    const refs = collectHashReferences(body.toString("utf8"));
    for (const ref of refs) {
      if (!(manifest.files || {})[ref]) {
        throw new Error(`missing referenced hash ${ref} from ${hash}`);
      }
    }
  }
}

console.log(`verified hash bundle: root=${manifest.root_entry_hash} files=${Object.keys(manifest.files || {}).length}`);
