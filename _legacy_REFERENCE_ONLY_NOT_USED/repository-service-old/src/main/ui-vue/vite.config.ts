import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import vuetify from 'vite-plugin-vuetify';

// https://vitejs.dev/config/
export default defineConfig({
  base: '/repository/',  // Set base path for assets when served behind reverse proxy
  plugins: [
    vue(),
    vuetify({ autoImport: true })
  ],
  resolve: {
    alias: {
      // Let pnpm handle the resolution to the built packages
    }
  },
  server: {
    port: 33008,
    strictPort: true,
    proxy: {
      // Shared-nav menu items via web-proxy
      '/connect/system-nav': {
        target: 'http://localhost:38106',
        changeOrigin: true,
      },
      // Platform Registration backend (Connect RPC)
      '/platform-registration': {
        target: 'http://localhost:38101',
        changeOrigin: true,
      }
    }
  },
});