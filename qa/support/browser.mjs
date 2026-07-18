import { existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const PLAYWRIGHT_MODULE = new URL('../../frontend/node_modules/playwright/index.mjs', import.meta.url);

function matchesHttpError(response, allowance) {
  const request = response.request();
  const url = new URL(response.url());
  return response.status() === allowance.status
    && request.method() === allowance.method
    && url.pathname === allowance.pathname
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
    viewport: { width: 1280, height: 800 }
  });
  await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
  const page = await context.newPage();
  const browserErrors = [];
  const baseOrigin = new URL(scenarioContext.baseURL).origin;
  const allowedHttpErrors = (options.allowedHttpErrors ?? []).map((allowance) => ({
    ...allowance,
    remaining: allowance.times ?? 1
  }));
  page.on('pageerror', (error) => browserErrors.push(`pageerror: ${error.message}`));
  page.on('console', (message) => {
    const location = message.location();
    const isHttpFailureNoise = message.text().startsWith('Failed to load resource:')
      && location.url.startsWith(baseOrigin);
    if (message.type() === 'error' && !isHttpFailureNoise) {
      browserErrors.push(`console: ${message.text()}`);
    }
  });
  page.on('response', (response) => {
    if (new URL(response.url()).origin !== baseOrigin || response.status() < 400) return;
    const allowance = allowedHttpErrors.find((candidate) => matchesHttpError(response, candidate));
    if (allowance) {
      allowance.remaining -= 1;
      return;
    }
    browserErrors.push(`response: ${response.status()} ${response.request().method()} ${response.url()}`);
  });
  page.on('requestfailed', (request) => {
    if (new URL(request.url()).origin !== baseOrigin) return;
    browserErrors.push(`requestfailed: ${request.method()} ${request.url()}: ${request.failure()?.errorText ?? 'unknown error'}`);
  });

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
    await page.screenshot({
      path: `${scenarioContext.artifactsDir}/failure.png`,
      fullPage: true
    }).catch(() => undefined);
    await writeFile(
      `${scenarioContext.artifactsDir}/browser-errors.json`,
      `${JSON.stringify(browserErrors, null, 2)}\n`
    );
    await context.tracing.stop({ path: `${scenarioContext.artifactsDir}/trace.zip` }).catch(() => undefined);
    tracingStopped = true;
    throw error;
  } finally {
    if (!tracingStopped) await context.tracing.stop().catch(() => undefined);
    await context.close().catch(() => undefined);
    await browser.close().catch(() => undefined);
  }
}
