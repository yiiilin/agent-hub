import { expect, test, type Page, type Route } from '@playwright/test';

const currentUser = {
  id: '10000000-0000-0000-0000-000000000019',
  email: 'integration-guide@example.com',
  display_name: 'Integration guide tester',
  role: 'admin'
};

async function installAuthenticatedConsole(page: Page) {
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'en'));
  await page.route('**/api/**', async (route: Route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/auth/me') return route.fulfill({ json: currentUser });
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${path}` } });
  });
}

test('Usage Guide precedes API Docs and documents every supported integration mode', async ({ page }) => {
  await installAuthenticatedConsole(page);
  await page.goto('/guide');

  await expect(page).toHaveURL(/\/guide$/);
  await expect(page.getByRole('button', { name: 'Usage Guide', exact: true })).toHaveAttribute('aria-current', 'page');
  const systemNavigation = page.locator('.nav-group').filter({ hasText: 'System' });
  await expect(systemNavigation.getByRole('button')).toHaveText([
    'Runtimes', 'Administration', 'Usage Guide', 'API Docs'
  ]);

  await expect(page.getByRole('heading', { name: 'Third-party integration guide', level: 1 })).toBeVisible();
  await expect(page.getByRole('table', { name: 'Integration modes' }).getByRole('row')).toHaveCount(4);
  await expect(page.getByText('POST /api/client/access', { exact: false }).first()).toBeVisible();
  await expect(page.getByText('connectAnonymous', { exact: false }).first()).toBeVisible();
  await expect(page.getByText('context.toolCallId', { exact: false }).first()).toBeVisible();
  await expect(page.getByText('grant_type: "client_credentials"', { exact: false })).toBeVisible();
  await expect(page.getByText('must generate and verify an unpredictable state value', { exact: false })).toBeVisible();
  await expect(page.getByRole('link', { name: 'Open API reference' })).toHaveAttribute('href', '/docs');

  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByRole('heading', { name: '第三方平台接入指南', level: 1 })).toBeVisible();
  await expect(page.getByRole('button', { name: '使用文档', exact: true })).toHaveAttribute('aria-current', 'page');
  await expect(page.getByRole('heading', { name: '认证浏览器接入', level: 2 })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(1280);
});

test('Usage Guide keeps long code inside its scroller at 390px', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installAuthenticatedConsole(page);
  await page.goto('/guide');

  await expect(page.getByRole('heading', { name: 'Third-party integration guide', level: 1 })).toBeVisible();
  const firstCode = page.locator('.usage-code-example pre').first();
  await expect(firstCode).toBeVisible();
  expect(await firstCode.evaluate((element) => element.scrollWidth > element.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  const sidebar = page.locator('.sidebar');
  const sidebarBox = await sidebar.boundingBox();
  const mainBox = await page.locator('main').boundingBox();
  expect(sidebarBox).not.toBeNull();
  expect(mainBox).not.toBeNull();
  expect(mainBox!.y).toBeGreaterThanOrEqual(sidebarBox!.y + sidebarBox!.height - 1);
});
