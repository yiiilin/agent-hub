import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// 可嵌入 Web Component（<agent-hub-chat>）独立构建：
// 输出单文件 dist/embed/agent-hub-chat.js（含 React 与全部组件），
// 供 <script src=".../embed/agent-hub-chat.js"> 与 iframe Widget 页面共用。
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist/embed',
    emptyOutDir: true,
    cssCodeSplit: false,
    rollupOptions: {
      input: 'src/embed/embed.html',
      output: {
        format: 'iife',
        entryFileNames: 'agent-hub-chat.js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
        inlineDynamicImports: true
      }
    }
  }
});
