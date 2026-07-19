import { expect, test } from '@playwright/test';

function collectBrowserErrors(page: import('@playwright/test').Page) {
  const errors: string[] = [];
  const serverErrors: string[] = [];
  page.on('pageerror', (error) => errors.push(`pageerror: ${error.message}`));
  page.on('console', (message) => { if (message.type() === 'error') errors.push(`console: ${message.text()}`); });
  page.on('response', (response) => { if (response.status() >= 500) serverErrors.push(`${response.status()} ${response.url()}`); });
  return { errors, serverErrors };
}

function largeOpenApiDocument(count = 90) {
  return {
    openapi: '3.1.0',
    info: { title: 'Agent Hub API', version: 'test', description: 'Deterministic large API document.' },
    paths: Object.fromEntries(Array.from({ length: count }, (_, index) => [
      `/api/test/resources/${index}/{resource_id}`,
      { get: { summary: `Inspect deterministic resource ${index}` } }
    ]))
  };
}

async function login(page: import('@playwright/test').Page) {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/sessions$/);
}

test('OpenAPI is public and API Docs is reachable from the sidebar', async ({ page, request }) => {
  const publicResponse = await request.get('/openapi.json');
  expect(publicResponse.status()).toBe(200);
  expect(publicResponse.headers()['content-type']).toContain('application/json');
  const document = await publicResponse.json();
  expect(document.openapi).toBe('3.1.0');
  expect(document.paths['/api/auth/api-keys/{api_key_id}/renew']).toBeTruthy();
  const listApiKeys = document.paths['/api/auth/api-keys'].get;
  expect(listApiKeys.parameters.map((parameter: { name: string }) => parameter.name)).toEqual(['page', 'page_size']);
  expect(listApiKeys.responses['200'].content['application/json'].schema.$ref).toBe('#/components/schemas/ApiKeyListResponse');
  expect(listApiKeys.responses['400']).toBeTruthy();
  expect(document.components.schemas.ApiKeyListResponse.required).toEqual(['items', 'total', 'page', 'page_size']);

  await login(page);
  let docsRequests = 0;
  page.on('request', (request) => { if (new URL(request.url()).pathname === '/openapi.json') docsRequests += 1; });
  await page.getByRole('button', { name: 'API Docs' }).click();
  await expect(page).toHaveURL(/\/docs$/);
  await expect(page.getByRole('button', { name: 'API Docs' })).toHaveAttribute('aria-current', 'page');
  await expect(page.getByRole('heading', { name: 'Agent Hub API', exact: true, level: 1 })).toBeVisible();
  await expect(page.getByText('Authorization: Bearer')).toBeVisible();
  const agentRunEndpoints = page.locator('.endpoint-row').filter({
    has: page.getByText('/api/agents/{agent_id}/runs', { exact: true })
  });
  await expect(agentRunEndpoints).toHaveCount(2);
  await expect(agentRunEndpoints.filter({ hasText: 'GET' })).toBeVisible();
  await expect(agentRunEndpoints.filter({ hasText: 'POST' })).toBeVisible();
  const loadedRequestCount = docsRequests;
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByText('API 参考', { exact: true })).toBeVisible();
  expect(docsRequests).toBe(loadedRequestCount);
  await page.getByLabel('语言').selectOption('en');
  const openApiLink = page.getByRole('link', { name: 'Open OpenAPI JSON' });
  await expect(openApiLink).toHaveAttribute('href', '/openapi.json');
  const linkedResponse = await page.request.get(await openApiLink.getAttribute('href') ?? '');
  expect((await linkedResponse.json()).openapi).toBe('3.1.0');
});

test('desktop sidebar remains viewport-fixed while a deterministic long docs page scrolls', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await login(page);
  const browser = collectBrowserErrors(page);
  await page.route('**/openapi.json', (route) => route.fulfill({ json: largeOpenApiDocument() }));
  await page.goto('/docs');
  await expect(page.getByText('/api/test/resources/89/{resource_id}', { exact: true })).toBeVisible();
  const sidebar = page.locator('.sidebar');
  const before = await sidebar.boundingBox();
  const footerBefore = await page.locator('.sidebar-footer').boundingBox();
  expect(before).not.toBeNull();
  expect(footerBefore).not.toBeNull();
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(0);
  const after = await sidebar.boundingBox();
  const footerAfter = await page.locator('.sidebar-footer').boundingBox();
  expect(after).not.toBeNull();
  expect(footerAfter).not.toBeNull();
  expect(after!.y).toBeCloseTo(before!.y, 0);
  expect(after!.x).toBeCloseTo(before!.x, 0);
  expect(after!.height).toBeCloseTo(720, 0);
  expect(footerAfter!.y).toBeCloseTo(footerBefore!.y, 0);
  expect(footerAfter!.x).toBeCloseTo(footerBefore!.x, 0);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(1280);
  await expect(page.locator('.sidebar-footer')).toBeInViewport();
  await page.screenshot({ path: 'test-results/api-docs-desktop.png', fullPage: false });
  expect(browser.errors).toEqual([]);
  expect(browser.serverErrors).toEqual([]);
});

test('mobile sticky sidebar stays in document flow without covering docs content', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await login(page);
  const browser = collectBrowserErrors(page);
  await page.route('**/openapi.json', (route) => route.fulfill({ json: largeOpenApiDocument(30) }));
  await page.goto('/docs');
  const sidebar = page.locator('.sidebar');
  await expect(sidebar).toHaveCSS('position', 'sticky');
  const sidebarBox = await sidebar.boundingBox();
  const mainBox = await page.locator('main').boundingBox();
  expect(sidebarBox).not.toBeNull();
  expect(mainBox).not.toBeNull();
  expect(mainBox!.y).toBeGreaterThanOrEqual(sidebarBox!.y + sidebarBox!.height - 1);
  await expect(page.getByRole('heading', { name: 'Agent Hub API', exact: true, level: 1 })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  await page.screenshot({ path: 'test-results/api-docs-mobile.png', fullPage: true });
  expect(browser.errors).toEqual([]);
  expect(browser.serverErrors).toEqual([]);
});

test('API Docs ignores an older retry that settles after a newer request', async ({ page }) => {
  await login(page);
  const browser = collectBrowserErrors(page);
  let attempt = 0;
  let releaseOlderRetry!: () => void;
  let markOlderRetryStarted!: () => void;
  let markOlderRetrySettled!: () => void;
  const olderRetryGate = new Promise<void>((resolve) => { releaseOlderRetry = resolve; });
  const olderRetryStarted = new Promise<void>((resolve) => { markOlderRetryStarted = resolve; });
  const olderRetrySettled = new Promise<void>((resolve) => { markOlderRetrySettled = resolve; });
  await page.route('**/openapi.json', async (route) => {
    attempt += 1;
    if (attempt === 1) return route.fulfill({ status: 200, contentType: 'text/html', body: '<html>not json</html>' });
    if (attempt === 2) {
      markOlderRetryStarted();
      await olderRetryGate;
      const stale = largeOpenApiDocument(2);
      stale.paths['/api/stale-generation'] = { get: { summary: 'Must not replace newer docs' } };
      await route.fulfill({ json: stale }).catch(() => undefined);
      markOlderRetrySettled();
      return;
    }
    return route.fulfill({ json: largeOpenApiDocument(4) });
  });
  await page.goto('/docs');
  const endpointsSection = page.locator('.docs-section').filter({
    has: page.getByRole('heading', { name: 'Endpoints', exact: true, level: 2 })
  });
  await expect(page.getByRole('alert')).toContainText('Unable to load API documentation');
  await page.getByRole('button', { name: 'Retry' }).click();
  await olderRetryStarted;
  const loading = page.getByRole('status');
  await expect(loading).toContainText('Loading API documentation...');
  await expect(endpointsSection).toHaveAttribute('aria-busy', 'true');
  await loading.getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByText('/api/test/resources/3/{resource_id}', { exact: true })).toBeVisible();
  releaseOlderRetry();
  await olderRetrySettled;
  await expect(page.getByText('/api/test/resources/3/{resource_id}', { exact: true })).toBeVisible();
  await expect(page.getByText('/api/stale-generation', { exact: true })).toHaveCount(0);
  await expect(page.getByText('Loading API documentation...')).toHaveCount(0);
  await expect(endpointsSection).toHaveAttribute('aria-busy', 'false');
  await expect(page.getByRole('alert')).toHaveCount(0);
  expect(browser.errors).toEqual([]);
  expect(browser.serverErrors).toEqual([]);
});
