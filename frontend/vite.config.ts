import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: { "@": path.resolve(__dirname, "./src") },
  },
  server: {
    port: 8080,
    proxy: {
      "/ask":      { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/health":   { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/schema":   { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/sessions": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/export":   { target: "http://127.0.0.1:8000", changeOrigin: true },
    },
  },
});
