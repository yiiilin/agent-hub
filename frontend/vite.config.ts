import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '^/api/': {
        target: process.env.VITE_API_PROXY ?? 'http://localhost:8080',
        changeOrigin: true
      },
      '/healthz': {
        target: process.env.VITE_API_PROXY ?? 'http://localhost:8080',
        changeOrigin: true
      }
    }
  }
});
