import { expect, test, type Page, type Route } from '@playwright/test';

const currentUser = {
  id: '10000000-0000-0000-0000-000000000001',
  email: 'model-admin@example.com',
  display_name: 'Model Admin',
  role: 'admin'
};

const personalConnection = {
  id: '20000000-0000-0000-0000-000000000001',
  owner_id: currentUser.id,
  scope: 'personal',
  name: 'Personal Provider',
  base_url: 'https://personal.example.test/provider',
  api_type: 'openai_responses',
  allowed_model_ids: ['personal-main', 'personal-mini'],
  status: 'enabled',
  has_api_key: true,
  created_at: '2026-07-18T01:00:00.000Z',
  updated_at: '2026-07-18T01:00:00.000Z'
};

const globalConnection = {
  id: '20000000-0000-0000-0000-000000000002',
  owner_id: null,
  scope: 'global',
  name: 'Global Provider',
  base_url: 'https://global.example.test',
  api_type: 'openai_chat_completions',
  allowed_model_ids: ['global-main', 'global-mini'],
  status: 'enabled',
  has_api_key: true,
  created_at: '2026-07-18T01:00:00.000Z',
  updated_at: '2026-07-18T01:00:00.000Z'
};

type RequestRecord = { method: string; path: string; body?: Record<string, unknown> };
type Stats = { requests: RequestRecord[]; summaryQueries: URL[]; usageQueries: URL[]; errorQueries: URL[] };

async function installModelsApi(page: Page, role = 'admin') {
  const stats: Stats = { requests: [], summaryQueries: [], usageQueries: [], errorQueries: [] };
  let connections = [personalConnection, globalConnection];
  let systemDefault: { connection_id: string; model_id: string } | null = {
    connection_id: globalConnection.id,
    model_id: 'global-main'
  };

  await page.route(/https?:\/\/[^/]+\/api\//, async (route: Route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    if (path === '/api/auth/me') return route.fulfill({ json: { ...currentUser, role } });
    if (path === '/api/model-connections' && method === 'GET') return route.fulfill({ json: connections });
    if (path === '/api/model-connections/system-default' && method === 'GET') return route.fulfill({ json: { selection: systemDefault } });
    if (path === '/api/model-connections' && method === 'POST') {
      const body = request.postDataJSON() as Record<string, unknown>;
      stats.requests.push({ method, path, body });
      const created = {
        id: '20000000-0000-0000-0000-000000000099',
        owner_id: body.scope === 'personal' ? currentUser.id : null,
        scope: body.scope,
        name: body.name,
        base_url: body.base_url,
        api_type: body.api_type,
        allowed_model_ids: body.allowed_model_ids,
        status: 'enabled',
        has_api_key: true,
        created_at: '2026-07-18T06:00:00.000Z',
        updated_at: '2026-07-18T06:00:00.000Z'
      };
      connections = [...connections, created as typeof personalConnection];
      return route.fulfill({ json: created });
    }
    const connection = path.match(/^\/api\/model-connections\/([^/]+)$/);
    if (connection && method === 'PUT') {
      const body = request.postDataJSON() as Record<string, unknown>;
      const requestPath = `${path}${url.search}`;
      stats.requests.push({ method, path: requestPath, body });
      if (
        connection[1] === personalConnection.id
        && url.searchParams.get('force') !== 'true'
        && !(body.allowed_model_ids as string[]).includes('personal-mini')
      ) {
        return route.fulfill({ status: 409, json: { error: 'Model API Connection is referenced' } });
      }
      const current = connections.find((item) => item.id === connection[1])!;
      const updated = { ...current, ...body, updated_at: '2026-07-18T07:00:00.000Z' };
      connections = connections.map((item) => item.id === updated.id ? updated as typeof item : item);
      return route.fulfill({ json: updated });
    }
    const testConnection = path.match(/^\/api\/model-connections\/([^/]+)\/test$/);
    if (testConnection && method === 'POST') {
      stats.requests.push({ method, path, body: request.postDataJSON() as Record<string, unknown> });
      return route.fulfill({ json: { success: true, status_code: 200, error_code: null, message: null, response_text: 'Hello from the test model.', response_time_ms: 42 } });
    }
    if (path === '/api/model-connections/system-default' && method === 'PUT') {
      const body = request.postDataJSON() as { selection: typeof systemDefault };
      stats.requests.push({ method, path, body });
      systemDefault = body.selection;
      return route.fulfill({ json: { selection: systemDefault } });
    }
    if (path === '/api/model-connections/' + personalConnection.id + '/status' && method === 'PUT') {
      stats.requests.push({ method, path, body: request.postDataJSON() as Record<string, unknown> });
      return route.fulfill({ json: { ...personalConnection, status: 'disabled' } });
    }
    if (path === '/api/agents') return route.fulfill({ json: [] });
    if (path === '/api/users') return route.fulfill({ json: [currentUser] });
    const totals = { input_tokens: 0, output_tokens: 0, total_tokens: 0, cached_tokens: 0, reasoning_tokens: 0 };
    if (path === '/api/model-usage/summary') {
      stats.summaryQueries.push(url);
      return route.fulfill({ json: { overall: totals, by_model: [], by_agent: [], by_user: [] } });
    }
    if (path === '/api/model-usage') {
      stats.usageQueries.push(url);
      return route.fulfill({ json: { items: [], next_cursor: null } });
    }
    if (path === '/api/model-call-errors') {
      stats.errorQueries.push(url);
      return route.fulfill({ json: { items: [], next_cursor: null } });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled ${method} ${path}` } });
  });
  return stats;
}

test('Model API Connections use a multi-model access form and final V1 requests', async ({ page }) => {
  const { requests } = await installModelsApi(page);
  await page.goto('/models');

  const table = page.getByRole('table', { name: 'Personal model connection list' });
  await expect(table).toContainText('Personal Provider');
  await expect(table).toContainText('personal-main');
  await expect(table).toContainText('personal-mini');

  await page.getByRole('button', { name: 'Create personal model' }).click();
  let dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await dialog.getByLabel('Connection name').fill('Created Provider');
  await dialog.getByLabel('Base URL').fill('https://created.example.test/base');
  await dialog.getByLabel('API type').selectOption('anthropic_messages');
  await dialog.getByLabel('Allowed Model IDs').fill('created-main\ncreated-mini\ncreated-main');
  await dialog.getByLabel('API key').fill('one-time-provider-secret');
  await dialog.getByRole('button', { name: 'Create model connection' }).click();
  expect(requests.at(-1)).toEqual({
    method: 'POST',
    path: '/api/model-connections',
    body: {
      scope: 'personal',
      name: 'Created Provider',
      base_url: 'https://created.example.test/base',
      api_type: 'anthropic_messages',
      allowed_model_ids: ['created-main', 'created-mini'],
      api_key: 'one-time-provider-secret'
    }
  });
  await expect(page.getByText('one-time-provider-secret')).toHaveCount(0);

  await page.getByRole('button', { name: 'Edit Personal Provider' }).click();
  dialog = page.getByRole('dialog', { name: 'Edit model connection' });
  await dialog.getByLabel('Connection name').fill('Personal Provider Updated');
  await dialog.getByLabel('Allowed Model IDs').fill('personal-main\npersonal-reasoning');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  expect(requests.at(-1)).toEqual({
    method: 'PUT',
    path: `/api/model-connections/${personalConnection.id}`,
    body: {
      name: 'Personal Provider Updated',
      base_url: personalConnection.base_url,
      api_type: 'openai_responses',
      allowed_model_ids: ['personal-main', 'personal-reasoning']
    }
  });
  await expect(dialog.getByRole('alert')).toContainText('Force saving clears affected selections');
  await dialog.getByRole('button', { name: 'Force save changes' }).click();
  expect(requests.at(-1)).toEqual({
    method: 'PUT',
    path: `/api/model-connections/${personalConnection.id}?force=true`,
    body: {
      name: 'Personal Provider Updated',
      base_url: personalConnection.base_url,
      api_type: 'openai_responses',
      allowed_model_ids: ['personal-main', 'personal-reasoning']
    }
  });
  expect(requests.at(-1)?.body).not.toHaveProperty('parameters');
  expect(requests.at(-1)?.body).not.toHaveProperty('request_parameters');

  await page.getByRole('button', { name: 'Test Personal Provider Updated' }).click();
  dialog = page.getByRole('dialog', { name: 'Test model connection' });
  await dialog.getByLabel('Model ID').selectOption('personal-reasoning');
  await expect(dialog.getByLabel('Request')).toHaveValue('hi');
  await dialog.getByRole('button', { name: 'Send test message' }).click();
  expect(requests.at(-1)).toEqual({ method: 'POST', path: `/api/model-connections/${personalConnection.id}/test`, body: { model_id: 'personal-reasoning', message: 'hi' } });
  await expect(dialog.getByLabel('Response')).toHaveText('Hello from the test model.');
  await expect(dialog.getByText('Response time 42 ms')).toBeVisible();

  await dialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();
  await page.getByRole('tab', { name: 'Global Models' }).click();
  await page.getByRole('button', { name: 'Clear Global Provider as system default' }).click();
  dialog = page.getByRole('dialog', { name: 'Clear system default' });
  await dialog.getByRole('button', { name: 'Clear system default' }).click();
  expect(requests.at(-1)).toEqual({ method: 'PUT', path: '/api/model-connections/system-default', body: { selection: null } });

  await page.getByRole('button', { name: 'Set Global Provider as system default' }).click();
  dialog = page.getByRole('dialog', { name: 'Set system default' });
  await dialog.getByLabel('Model ID').selectOption('global-mini');
  await dialog.getByRole('button', { name: 'Set system default' }).click();
  expect(requests.at(-1)).toEqual({
    method: 'PUT',
    path: '/api/model-connections/system-default',
    body: { selection: { connection_id: globalConnection.id, model_id: 'global-mini' } }
  });
});

test('Model API Connection workflow remains inside a 390px viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installModelsApi(page);
  await page.goto('/models');
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);

  await page.getByRole('button', { name: 'Create personal model' }).click();
  let dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await expect(dialog.getByLabel('Allowed Model IDs')).toBeVisible();
  await expect(dialog.getByLabel('API type')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
  await dialog.getByRole('button', { name: 'Cancel' }).click();

  await page.getByRole('button', { name: 'Test Personal Provider' }).click();
  dialog = page.getByRole('dialog', { name: 'Test model connection' });
  await expect(dialog.getByLabel('Request')).toHaveValue('hi');
  await dialog.getByRole('button', { name: 'Send test message' }).click();
  await expect(dialog.getByLabel('Response')).toHaveText('Hello from the test model.');
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
});

test('Models tabs remain scoped to the signed-in role', async ({ page }) => {
  for (const [role, expectedTabs] of [['member', 3], ['admin', 4], ['super_admin', 4]] as const) {
    await page.unroute(/https?:\/\/[^/]+\/api\//).catch(() => undefined);
    await installModelsApi(page, role);
    await page.goto('/models');
    const tabs = page.getByRole('tablist', { name: 'Models' });
    await expect(tabs.getByRole('tab')).toHaveCount(expectedTabs);
    await expect(tabs.getByRole('tab', { name: 'Global Models' })).toHaveCount(role === 'member' ? 0 : 1);
    await expect(page.getByRole('table', { name: 'Personal model connection list' })).toContainText('Personal Provider');
  }
});

test('Model usage uses local half-open millisecond ranges', async ({ page }) => {
  await page.clock.setFixedTime(new Date('2026-07-18T12:34:56.789'));
  const stats = await installModelsApi(page);
  await page.goto('/models');
  await page.getByRole('tab', { name: 'Usage' }).click();
  await expect.poll(() => stats.summaryQueries.length).toBe(1);

  const expected = await page.evaluate(() => {
    const now = new Date();
    const today = new Date(now);
    today.setHours(0, 0, 0, 0);
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    return { now: now.getTime(), today: today.getTime(), yesterday: yesterday.getTime() };
  });
  expect(stats.summaryQueries[0].searchParams.get('from_ms')).toBe(String(expected.today));
  expect(stats.summaryQueries[0].searchParams.get('to_ms')).toBe(String(expected.now));

  await page.getByLabel('Time range').selectOption('yesterday');
  await expect.poll(() => stats.summaryQueries.length).toBe(2);
  expect(stats.summaryQueries[1].searchParams.get('from_ms')).toBe(String(expected.yesterday));
  expect(stats.summaryQueries[1].searchParams.get('to_ms')).toBe(String(expected.today));
  for (const request of [...stats.summaryQueries, ...stats.usageQueries, ...stats.errorQueries]) {
    for (const forbidden of ['session_id', 'run_id', 'turn_id', 'subagent']) expect(request.searchParams.has(forbidden)).toBe(false);
  }
});
