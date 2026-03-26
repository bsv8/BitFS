import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { bitfsHashPlugin } from "vite-plugin-bitfs-hash";

export default defineConfig({
  plugins: [
    react(),
    bitfsHashPlugin({
      rootEntry: "index.html"
    })
  ],
  build: {
    outDir: "dist-hash",
    emptyOutDir: true,
    sourcemap: false,
    assetsInlineLimit: 0
  }
});
