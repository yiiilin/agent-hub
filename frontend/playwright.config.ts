import { defineConfig, devices } from '@playwright/test';
import { resolvePlaywrightBaseURL } from './tests/e2e-compose';

export default defineConfig({
  testDir: './tests',
  globalSetup: './tests/global-setup.ts',
  timeout: 60_000,
  expect: { timeout: 15_000 },
  use: {
    baseURL: resolvePlaywrightBaseURL(),
    trace: 'on-first-retry'
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] }
    }
  ]
});
