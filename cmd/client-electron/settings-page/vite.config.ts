import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    outDir: "dist-settings",
    emptyOutDir: true,
    sourcemap: false,
    assetsInlineLimit: 0
  }
});
