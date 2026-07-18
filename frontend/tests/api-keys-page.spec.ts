import { expect, test, type Page, type Route } from '@playwright/test';

const user = {
  id: '10000000-0000-0000-0000-000000000001',
  username: 'api-key-owner',
  email: 'api-key-owner@example.com',
  display_name: 'API key owner',
  role: 'member'
};

const expiringKey = {
  id: '20000000-0000-0000-0000-000000000001',
  name: 'Deployment key',
  prefix: 'ahk_existing',
  last_used_at: null,
  expires_at: '2026-10-15T00:00:00Z',
  created_at: '2026-07-01T00:00:00Z'
};

const permanentKey = {
  ...expiringKey,
  id: '20000000-0000-0000-0000-000000000002',
  name: 'Permanent key',
  prefix: 'ahk_forever',
  expires_at: null
};

async function installApi(page: Page) {
  let keys = [expiringKey, permanentKey];
  const requests: Array<{ method: string; path: string; body: unknown }> = [];

  await page.route('**/api/**', async (route: Route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const method = request.method();
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: user });
    if (path === '/api/auth/api-keys' && method === 'GET') {
      return route.fulfill({ json: { items: keys, total: keys.length, page: 1, page_size: 20 } });
    }
    if (path === '/api/auth/api-keys' && method === 'POST') {
      const body = request.postDataJSON();
      requests.push({ method, path, body });
      const created = {
        ...expiringKey,
        id: '20000000-0000-0000-0000-000000000003',
        name: body.name,
        prefix: 'ahk_created',
        expires_at: '2027-01-13T00:00:00Z'
      };
      keys = [created, ...keys];
      return route.fulfill({ json: { api_key: created, token: 'ahk_created_secret' } });
    }
    if (path === `/api/auth/api-keys/${expiringKey.id}/renew` && method === 'POST') {
      const body = request.postDataJSON();
      requests.push({ method, path, body });
      const renewed = { ...expiringKey, expires_at: body.validity.expires_at };
      keys = keys.map((key) => key.id === renewed.id ? renewed : key);
      return route.fulfill({ json: renewed });
    }
    if (path.startsWith('/api/auth/api-keys/') && method === 'DELETE') {
      requests.push({ method, path, body: null });
      keys = keys.filter((key) => !path.endsWith(key.id));
      return route.fulfill({ status: 204, body: '' });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled ${method} ${path}` } });
  });

  return requests;
}

test('API key validity, same-token renewal, copy, and delete-only controls use the current contract', async ({ page }) => {
  const requests = await installApi(page);
  await page.goto('/api-keys');

  const expiringRow = page.locator('.api-key-row', { hasText: expiringKey.name });
  const permanentRow = page.locator('.api-key-row', { hasText: permanentKey.name });
  await expect(expiringRow.getByRole('button', { name: 'Renew' })).toBeVisible();
  await expect(permanentRow).toContainText('Never expires');
  await expect(permanentRow.getByRole('button', { name: 'Renew' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Revoke' })).toHaveCount(0);

  await page.getByRole('button', { name: 'Create API key' }).click();
  const createDialog = page.getByRole('dialog', { name: 'Create API key' });
  await createDialog.getByLabel('Name').fill('Release key');
  await createDialog.getByLabel('Validity').selectOption('180');
  await createDialog.getByRole('button', { name: 'Create key' }).click();
  expect(requests[0]).toMatchObject({
    method: 'POST',
    path: '/api/auth/api-keys',
    body: { name: 'Release key', validity: { kind: 'days', days: 180 } }
  });

  const secretDialog = page.getByRole('dialog', { name: 'One-time API key' });
  await expect(secretDialog.locator('.secret-token')).toHaveText('ahk_created_secret');
  const copy = secretDialog.getByRole('button', { name: 'Copy API key' });
  await expect(copy).toHaveCSS('opacity', '0');
  await secretDialog.locator('.secret-token-line').hover();
  await expect(copy).toHaveCSS('opacity', '1');
  await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();

  await expiringRow.getByRole('button', { name: 'Renew' }).click();
  const renewDialog = page.getByRole('dialog', { name: 'Renew API key' });
  await expect(renewDialog).toContainText('Renewal keeps the existing token.');
  await expect(renewDialog.locator('.secret-token')).toHaveCount(0);
  await renewDialog.getByRole('button', { name: 'Renew' }).click();
  expect(requests[1]).toMatchObject({
    method: 'POST',
    path: `/api/auth/api-keys/${expiringKey.id}/renew`,
    body: { validity: { kind: 'date' } }
  });
  await expect(page.getByText('API key expiration updated.')).toBeVisible();
  await expect(page.getByRole('dialog', { name: 'One-time API key' })).toHaveCount(0);

  page.once('dialog', (dialog) => dialog.accept());
  await expiringRow.getByRole('button', { name: 'Delete' }).click();
  await expect(expiringRow).toHaveCount(0);
  expect(requests.at(-1)).toMatchObject({
    method: 'DELETE',
    path: `/api/auth/api-keys/${expiringKey.id}`
  });
});

test('API key list and form fit a 390px viewport', async ({ page }) => {
  await installApi(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/api-keys');
  await page.getByRole('button', { name: 'Create API key' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create API key' });
  await expect(dialog).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  const box = await dialog.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.width).toBeLessThanOrEqual(390);
  expect(box!.y).toBeGreaterThanOrEqual(0);
  expect(box!.y + box!.height).toBeLessThanOrEqual(844);
});
