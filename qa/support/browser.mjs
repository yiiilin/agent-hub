import { existsSync } from 'node:fs';
import { mkdir, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  assertArtifactTreeSafe,
  redactSecrets,
  sanitizeArtifactTree
} from './secrets.mjs';
import { qaSourceIp } from './api.mjs';

const PLAYWRIGHT_MODULE = new URL('../../frontend/node_modules/playwright/index.mjs', import.meta.url);
const FAILURE_MASK_SELECTOR = [
  'input',
  'textarea',
  'select',
  'code',
  '[data-secret]',
  '[data-sensitive]',
  '[class*="secret" i]',
  '[id*="secret" i]',
  '[name*="token" i]',
  '[name*="password" i]',
  '[name*="api-key" i]',
  '[aria-label*="secret" i]',
  '[aria-label*="token" i]',
  '[aria-label*="password" i]'
].join(', ');

function matchesHttpError(response, allowance) {
  const request = response.request();
  const url = new URL(response.url());
  return response.status() === allowance.status
    && request.method() === allowance.method
    && url.pathname === allowance.pathname
    && (allowance.origin === undefined || url.origin === allowance.origin)
    && allowance.remaining > 0;
}

export async function withBrowser(scenarioContext, options, run) {
  const modulePath = fileURLToPath(PLAYWRIGHT_MODULE);
  if (!existsSync(modulePath)) {
    throw new Error('Playwright is not installed in frontend/node_modules. Run: cd frontend && npm ci');
  }
  const { chromium } = await import(PLAYWRIGHT_MODULE.href);
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (error) {
    throw new Error('Chromium is unavailable. Run: cd frontend && npx playwright install chromium', { cause: error });
  }

  await mkdir(scenarioContext.artifactsDir, { recursive: true });
  const context = await browser.newContext({
    baseURL: scenarioContext.baseURL,
    locale: 'en-US',
    viewport: { width: 1280, height: 800 },
    extraHTTPHeaders: { 'x-forwarded-for': qaSourceIp() }
  });
  const startTracing = context.tracing.start.bind(context.tracing);
  // Scenarios may pause and restart tracing; never allow pixel screenshots into trace.zip.
  context.tracing.start = (tracingOptions = {}) => startTracing({
    ...tracingOptions,
    screenshots: false
  });
  await context.tracing.start({ snapshots: true, sources: true });
  const browserErrors = [];
  const baseOrigin = new URL(scenarioContext.baseURL).origin;
  const monitoredOrigins = new Set([baseOrigin, ...(options.monitoredOrigins ?? [])]);
  const allowedHttpErrors = (options.allowedHttpErrors ?? []).map((allowance) => ({
    ...allowance,
    remaining: allowance.times ?? 1
  }));
  const observedPages = new WeakSet();
  const observePage = (observedPage) => {
    if (observedPages.has(observedPage)) return;
    observedPages.add(observedPage);
    observedPage.on('pageerror', (error) => browserErrors.push(`pageerror: ${error.message}`));
    observedPage.on('console', (message) => {
      const location = message.location();
      let locationOrigin = null;
      try {
        locationOrigin = location.url ? new URL(location.url).origin : null;
      } catch {
        locationOrigin = null;
      }
      const isHttpFailureNoise = message.text().startsWith('Failed to load resource:')
        && locationOrigin !== null
        && monitoredOrigins.has(locationOrigin);
      if (message.type() === 'error' && !isHttpFailureNoise) {
        browserErrors.push(`console: ${message.text()}`);
      }
    });
    observedPage.on('response', (response) => {
      const url = new URL(response.url());
      if (!monitoredOrigins.has(url.origin) || response.status() < 400) return;
      const allowance = allowedHttpErrors.find((candidate) => matchesHttpError(response, candidate));
      if (allowance) {
        allowance.remaining -= 1;
        return;
      }
      browserErrors.push(`response: ${response.status()} ${response.request().method()} ${response.url()}`);
    });
    observedPage.on('requestfailed', (request) => {
      if (!monitoredOrigins.has(new URL(request.url()).origin)) return;
      browserErrors.push(`requestfailed: ${request.method()} ${request.url()}: ${request.failure()?.errorText ?? 'unknown error'}`);
    });
  };
  context.on('page', observePage);
  const page = await context.newPage();
  observePage(page);

  let tracingStopped = false;
  try {
    const result = await run({ page, context, request: context.request, browserErrors });
    if (browserErrors.length > 0) {
      throw new Error(`Browser diagnostics reported errors:\n${browserErrors.join('\n')}`);
    }
    await context.tracing.stop();
    tracingStopped = true;
    return result;
  } catch (error) {
    const temporaryScreenshot = join(scenarioContext.artifactsDir, '.failure.tmp.png');
    const temporaryErrors = join(scenarioContext.artifactsDir, '.browser-errors.json.tmp');
    const temporaryTrace = join(scenarioContext.artifactsDir, '.trace.zip.tmp');
    const targetScreenshot = join(scenarioContext.artifactsDir, 'failure.png');
    const targetErrors = join(scenarioContext.artifactsDir, 'browser-errors.json');
    const targetTrace = join(scenarioContext.artifactsDir, 'trace.zip');
    let screenshotSaved = false;
    try {
      screenshotSaved = await page.screenshot({
        path: temporaryScreenshot,
        type: 'png',
        fullPage: true,
        mask: [page.locator(FAILURE_MASK_SELECTOR)],
        maskColor: '#000000'
      }).then(() => true, () => false);
      await writeFile(
        temporaryErrors,
        `${JSON.stringify(redactSecrets(browserErrors), null, 2)}\n`
      );
      await context.tracing.stop({ path: temporaryTrace });
      tracingStopped = true;
      await sanitizeArtifactTree(scenarioContext.artifactsDir);
      await assertArtifactTreeSafe(scenarioContext.artifactsDir);
      if (screenshotSaved) await rename(temporaryScreenshot, targetScreenshot);
      await rename(temporaryErrors, targetErrors);
      await rename(temporaryTrace, targetTrace);
    } catch {
      await Promise.all([
        temporaryScreenshot,
        temporaryErrors,
        temporaryTrace,
        targetScreenshot,
        targetErrors,
        targetTrace
      ].map((path) => rm(path, { force: true }).catch(() => undefined)));
      throw new Error('Browser failure artifact sanitization failed; unsafe artifacts were removed');
    }
    throw error;
  } finally {
    if (!tracingStopped) await context.tracing.stop().catch(() => undefined);
    await context.close().catch(() => undefined);
    await browser.close().catch(() => undefined);
  }
}
