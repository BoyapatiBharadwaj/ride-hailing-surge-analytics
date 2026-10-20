import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to the Python server (python -m genai.ops_server on :8501).
export default defineConfig({
  plugins: [react()],
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 1200 },
  server: { port: 5173, proxy: { "/api": "http://localhost:8501" } },
});
