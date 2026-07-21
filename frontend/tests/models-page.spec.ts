import { expect, test, type Page, type Route } from '@playwright/test';

const currentUser = {
  id: '10000000-0000-0000-0000-000000000001',
  username: 'model-admin',
  email: 'model-admin@example.com',
  display_name: 'Model Admin',
  role: 'admin'
};

const personalConnection = {
  id: '20000000-0000-0000-0000-000000000001',
  owner_id: currentUser.id,
  scope: 'personal',
  name: 'Personal GPT',
  base_url: 'https://personal.example.test/provider',
  model_id: 'personal-model',
  upstream_protocol: 'openai_responses',
  status: 'enabled',
  is_system_default: false,
  created_at: '2026-07-18T01:00:00.000Z',
  updated_at: '2026-07-18T01:00:00.000Z'
};

const globalConnection = {
  id: '20000000-0000-0000-0000-000000000002',
  owner_id: null,
  scope: 'global',
  name: 'Global Responses',
  base_url: 'https://global.example.test',
  model_id: 'global-model',
  upstream_protocol: 'anthropic_messages',
  status: 'enabled',
  is_system_default: true,
  created_at: '2026-07-18T01:00:00.000Z',
  updated_at: '2026-07-18T01:00:00.000Z'
};

const disabledGlobalConnection = {
  ...globalConnection,
  id: '20000000-0000-0000-0000-000000000003',
  name: 'Disabled Global',
  model_id: 'disabled-model',
  status: 'disabled',
  is_system_default: false
};

const agent = {
  id: '30000000-0000-0000-0000-000000000001',
  name: 'Release Agent',
  instructions: '',
  visibility: 'private',
  public_to: [],
  runtime_id: null,
  default_model_connection_id: globalConnection.id,
  reasoning_effort: 'medium',
  codex_subagents: [],
  owner_id: currentUser.id,
  is_owner: true,
  can_manage: true,
  can_administer: true,
  can_invoke: true,
  model_policy: {},
  sandbox_policy: {},
  managed_skill_ids: [],
  mcp_allowlist: [],
  created_at: '2026-07-18T01:00:00.000Z',
  updated_at: '2026-07-18T01:00:00.000Z'
};

const totals = {
  input_tokens: 120,
  output_tokens: 40,
  total_tokens: 160,
  cached_tokens: 20,
  reasoning_tokens: 10
};

const usageItem = {
  id: '40000000-0000-0000-0000-000000000001',
  occurred_at: '2026-07-18T05:00:00.123Z',
  response_status: 'completed',
  model: { id: globalConnection.id, scope: 'global', name: globalConnection.name, model_id: globalConnection.model_id, upstream_protocol: globalConnection.upstream_protocol },
  agent: { id: agent.id, name: agent.name },
  subject: { kind: 'user', id: currentUser.id, display_name: currentUser.display_name },
  ...totals
};

const errorItem = {
  id: '50000000-0000-0000-0000-000000000001',
  occurred_at: '2026-07-18T05:05:00.456Z',
  response_status: 'failed',
  model: usageItem.model,
  agent: usageItem.agent,
  subject: usageItem.subject,
  upstream_status: 429,
  error_code: 'rate_limit',
  message: 'Try again later.'
};

type MockStats = {
  requests: Array<{ method: string; path: string; body?: Record<string, unknown> }>;
  summaryQueries: URL[];
  usageQueries: URL[];
  errorQueries: URL[];
};

async function installModelsApi(page: Page, role = 'admin') {
  const stats: MockStats = { requests: [], summaryQueries: [], usageQueries: [], errorQueries: [] };
  let connections = [
    { ...personalConnection },
    { ...globalConnection },
    { ...disabledGlobalConnection }
  ];

  await page.route('**/api/**', async (route: Route) => {
    const request = route.request();
    const url = new URL(request.url());
    const { pathname } = url;
    const method = request.method();
    if (pathname === '/api/auth/me') return route.fulfill({ json: { ...currentUser, role } });
    if (pathname === '/api/model-connections' && method === 'GET') return route.fulfill({ json: connections });
    if (pathname === '/api/model-connections' && method === 'POST') {
      const body = request.postDataJSON() as Record<string, unknown>;
      stats.requests.push({ method, path: pathname, body });
      const created = {
        id: '20000000-0000-0000-0000-000000000099',
        owner_id: body.scope === 'personal' ? currentUser.id : null,
        scope: body.scope,
        name: body.name,
        base_url: body.base_url,
        model_id: body.model_id,
        upstream_protocol: body.upstream_protocol,
        status: 'enabled',
        is_system_default: false,
        created_at: '2026-07-18T06:00:00.000Z',
        updated_at: '2026-07-18T06:00:00.000Z'
      };
      connections = [...connections, created as typeof personalConnection];
      return route.fulfill({ json: created });
    }
    const connectionMatch = pathname.match(/^\/api\/model-connections\/([^/]+)$/);
    if (connectionMatch && method === 'PATCH') {
      const body = request.postDataJSON() as Record<string, unknown>;
      stats.requests.push({ method, path: pathname, body });
      const current = connections.find((connection) => connection.id === connectionMatch[1])!;
      const updated = { ...current, ...body, updated_at: '2026-07-18T07:00:00.000Z' };
      connections = connections.map((connection) => connection.id === updated.id ? updated as typeof connection : connection);
      return route.fulfill({ json: updated });
    }
    const statusMatch = pathname.match(/^\/api\/model-connections\/([^/]+)\/status$/);
    if (statusMatch && method === 'PUT') {
      const body = request.postDataJSON() as Record<string, unknown>;
      stats.requests.push({ method, path: pathname, body });
      const current = connections.find((connection) => connection.id === statusMatch[1])!;
      const updated = { ...current, status: body.status };
      connections = connections.map((connection) => connection.id === updated.id ? updated as typeof connection : connection);
      return route.fulfill({ json: updated });
    }
    const testMatch = pathname.match(/^\/api\/model-connections\/([^/]+)\/test$/);
    if (testMatch && method === 'POST') {
      stats.requests.push({ method, path: pathname });
      return route.fulfill({ json: { success: true, status_code: 200, error_code: null, message: null } });
    }
    const forceMatch = pathname.match(/^\/api\/model-connections\/([^/]+)\/force-delete$/);
    if (forceMatch && method === 'POST') {
      stats.requests.push({ method, path: pathname });
      connections = connections.filter((connection) => connection.id !== forceMatch[1]);
      return route.fulfill({ status: 204 });
    }
    if (connectionMatch && method === 'DELETE') {
      stats.requests.push({ method, path: pathname });
      connections = connections.filter((connection) => connection.id !== connectionMatch[1]);
      return route.fulfill({ status: 204 });
    }
    if (pathname === '/api/model-connections/system-default' && method === 'PUT') {
      const body = request.postDataJSON() as Record<string, unknown>;
      stats.requests.push({ method, path: pathname, body });
      connections = connections.map((connection) => ({ ...connection, is_system_default: connection.id === body.model_connection_id }));
      return route.fulfill({ json: { model_connection_id: body.model_connection_id } });
    }
    if (pathname === '/api/agents' && method === 'GET') return route.fulfill({ json: [agent] });
    if (pathname === '/api/users' && method === 'GET') return route.fulfill({ json: [{ ...currentUser, role }] });
    if (pathname === '/api/model-usage/summary' && method === 'GET') {
      stats.summaryQueries.push(url);
      return route.fulfill({ json: {
        overall: totals,
        by_model: [{ model: usageItem.model, totals }],
        by_agent: [{ agent: usageItem.agent, totals }],
        by_user: [{ user_id: currentUser.id, display_name: currentUser.display_name, totals }]
      } });
    }
    if (pathname === '/api/model-usage' && method === 'GET') {
      stats.usageQueries.push(url);
      const secondPage = url.searchParams.has('cursor_id');
      return route.fulfill({ json: {
        items: [{ ...usageItem, id: secondPage ? '40000000-0000-0000-0000-000000000002' : usageItem.id }],
        next_cursor: secondPage ? null : { occurred_at_ms: 1_752_813_200_123, id: usageItem.id }
      } });
    }
    if (pathname === '/api/model-call-errors' && method === 'GET') {
      stats.errorQueries.push(url);
      const secondPage = url.searchParams.has('cursor_id');
      return route.fulfill({ json: {
        items: [{ ...errorItem, id: secondPage ? '50000000-0000-0000-0000-000000000002' : errorItem.id }],
        next_cursor: secondPage ? null : { occurred_at_ms: 1_752_813_500_456, id: errorItem.id }
      } });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled route: ${method} ${pathname}` } });
  });
  return stats;
}

for (const [role, expectedTabs] of [['member', 3], ['admin', 4], ['super_admin', 4]] as const) {
  test(`${role} sees only the permitted Models tabs`, async ({ page }) => {
    await installModelsApi(page, role);
    await page.goto('/models');
    const tabs = page.getByRole('tablist', { name: 'Models' });
    await expect(tabs.getByRole('tab')).toHaveCount(expectedTabs);
    await expect(tabs.getByRole('tab', { name: 'My Models' })).toBeVisible();
    await expect(tabs.getByRole('tab', { name: 'Available Models' })).toBeVisible();
    await expect(tabs.getByRole('tab', { name: 'Usage' })).toBeVisible();
    await expect(tabs.getByRole('tab', { name: 'Global Models' })).toHaveCount(role === 'member' ? 0 : 1);

    await expect(page.getByRole('table', { name: 'Personal model connection list' })).toContainText('Personal GPT');
    await expect(page.getByRole('table', { name: 'Personal model connection list' })).not.toContainText('Global Responses');
    await tabs.getByRole('tab', { name: 'Available Models' }).click();
    const available = page.getByRole('table', { name: 'Available model list' });
    await expect(available).toContainText('Personal GPT');
    await expect(available).toContainText('Global Responses');
    await expect(available).toContainText('Anthropic Messages');
    await expect(available).not.toContainText('Disabled Global');
  });
}

test('connection action dialogs serialize CRUD, test, status, default, and force-delete requests without exposing keys', async ({ page }) => {
  const stats = await installModelsApi(page);
  await page.goto('/models');

  await page.getByRole('button', { name: 'Create personal model' }).click();
  let dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await dialog.getByLabel('Connection name').fill('Created Personal');
  await dialog.getByLabel('Base URL').fill('https://created.example.test/base');
  await dialog.getByLabel('Model ID').fill('created-model');
  await expect(dialog.getByLabel('Upstream protocol')).toHaveValue('openai_responses');
  await dialog.getByLabel('Upstream protocol').selectOption('anthropic_messages');
  await dialog.getByLabel('API key').fill('one-time-provider-secret');
  await dialog.getByRole('button', { name: 'Create model connection' }).click();
  await expect(page.getByRole('table', { name: 'Personal model connection list' })).toContainText('Created Personal');
  expect(stats.requests.at(-1)).toMatchObject({
    method: 'POST',
    path: '/api/model-connections',
    body: { scope: 'personal', name: 'Created Personal', base_url: 'https://created.example.test/base', model_id: 'created-model', upstream_protocol: 'anthropic_messages', api_key: 'one-time-provider-secret' }
  });
  await expect(page.getByText('one-time-provider-secret')).toHaveCount(0);

  await page.getByRole('button', { name: 'Edit Personal GPT' }).click();
  dialog = page.getByRole('dialog', { name: 'Edit model connection' });
  await expect(dialog.getByLabel('API key')).toHaveValue('');
  await expect(dialog.getByLabel('Upstream protocol')).toHaveValue('openai_responses');
  await dialog.getByLabel('Connection name').fill('Personal GPT Updated');
  await dialog.getByLabel('Upstream protocol').selectOption('anthropic_messages');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  const update = stats.requests.at(-1)!;
  expect(update).toMatchObject({ method: 'PATCH', path: `/api/model-connections/${personalConnection.id}` });
  expect(update.body).toMatchObject({ upstream_protocol: 'anthropic_messages' });
  expect(update.body).not.toHaveProperty('api_key');

  await page.getByRole('button', { name: 'Test Personal GPT Updated' }).click();
  dialog = page.getByRole('dialog', { name: 'Test model connection' });
  await dialog.getByRole('button', { name: 'Run connection test' }).click();
  await expect(dialog.getByText('Connection test succeeded.')).toBeVisible();
  await dialog.locator('.modal-actions').getByRole('button', { name: 'Close' }).click();

  await page.getByRole('button', { name: 'Disable Personal GPT Updated' }).click();
  dialog = page.getByRole('dialog', { name: 'Disable model connection' });
  await dialog.getByRole('button', { name: 'Disable model connection' }).click();
  expect(stats.requests.at(-1)).toMatchObject({ method: 'PUT', path: `/api/model-connections/${personalConnection.id}/status`, body: { status: 'disabled' } });

  await page.getByRole('button', { name: 'Delete Personal GPT Updated', exact: true }).click();
  dialog = page.getByRole('dialog', { name: 'Delete model connection' });
  await dialog.getByRole('button', { name: 'Delete model connection' }).click();
  expect(stats.requests.at(-1)).toMatchObject({ method: 'DELETE', path: `/api/model-connections/${personalConnection.id}` });

  await page.getByRole('tab', { name: 'Global Models' }).click();
  await page.getByRole('button', { name: 'Clear Global Responses as system default' }).click();
  dialog = page.getByRole('dialog', { name: 'Clear system default' });
  await dialog.getByRole('button', { name: 'Clear system default' }).click();
  expect(stats.requests.at(-1)).toMatchObject({ method: 'PUT', path: '/api/model-connections/system-default', body: { model_connection_id: null } });

  await page.getByRole('button', { name: 'Force-delete Disabled Global' }).click();
  dialog = page.getByRole('dialog', { name: 'Force-delete model connection' });
  await dialog.getByRole('button', { name: 'Force-delete model connection' }).click();
  expect(stats.requests.at(-1)).toMatchObject({ method: 'POST', path: `/api/model-connections/${disabledGlobalConnection.id}/force-delete` });
});

test('usage ranges send browser-local half-open millisecond boundaries and omit internal hierarchy filters', async ({ page }) => {
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
    const sevenDays = new Date(today);
    sevenDays.setDate(sevenDays.getDate() - 6);
    return {
      now: now.getTime(),
      today: today.getTime(),
      yesterday: yesterday.getTime(),
      sevenDays: sevenDays.getTime()
    };
  });
  expect(stats.summaryQueries[0].searchParams.get('from_ms')).toBe(String(expected.today));
  expect(stats.summaryQueries[0].searchParams.get('to_ms')).toBe(String(expected.now));

  await page.getByLabel('Time range').selectOption('yesterday');
  await expect.poll(() => stats.summaryQueries.length).toBe(2);
  expect(stats.summaryQueries[1].searchParams.get('from_ms')).toBe(String(expected.yesterday));
  expect(stats.summaryQueries[1].searchParams.get('to_ms')).toBe(String(expected.today));

  await page.getByLabel('Time range').selectOption('7days');
  await expect.poll(() => stats.summaryQueries.length).toBe(3);
  expect(stats.summaryQueries[2].searchParams.get('from_ms')).toBe(String(expected.sevenDays));
  expect(stats.summaryQueries[2].searchParams.get('to_ms')).toBe(String(expected.now));

  await page.getByLabel('Time range').selectOption('all');
  await expect.poll(() => stats.summaryQueries.length).toBe(4);
  expect(stats.summaryQueries[3].searchParams.has('from_ms')).toBe(false);
  expect(stats.summaryQueries[3].searchParams.has('to_ms')).toBe(false);
  for (const url of [...stats.summaryQueries, ...stats.usageQueries, ...stats.errorQueries]) {
    for (const forbidden of ['session_id', 'run_id', 'turn_id', 'subagent']) expect(url.searchParams.has(forbidden)).toBe(false);
  }
});

test('whole-range summary stays fixed while usage and error keysets paginate independently', async ({ page }) => {
  const stats = await installModelsApi(page);
  await page.goto('/models');
  await page.getByRole('tab', { name: 'Usage' }).click();
  await expect(page.getByRole('heading', { name: 'Usage details' })).toBeVisible();
  await expect.poll(() => stats.summaryQueries.length).toBe(1);
  await expect.poll(() => stats.usageQueries.length).toBe(1);
  await expect.poll(() => stats.errorQueries.length).toBe(1);
  const userNameCell = page.getByRole('region', { name: 'By user' }).locator('tbody td').first();
  expect(await userNameCell.evaluate((element) => element.getBoundingClientRect().width)).toBeGreaterThanOrEqual(180);

  await page.getByRole('button', { name: 'Next usage page' }).click();
  await expect.poll(() => stats.usageQueries.length).toBe(2);
  expect(stats.summaryQueries).toHaveLength(1);
  expect(stats.errorQueries).toHaveLength(1);
  expect(stats.usageQueries[1].searchParams.get('cursor_id')).toBe(usageItem.id);

  await page.getByRole('button', { name: 'Next error page' }).click();
  await expect.poll(() => stats.errorQueries.length).toBe(2);
  expect(stats.summaryQueries).toHaveLength(1);
  expect(stats.usageQueries).toHaveLength(2);
  expect(stats.errorQueries[1].searchParams.get('cursor_id')).toBe(errorItem.id);
});

test('Models tabs, ledgers, and dialogs stay inside a 390px viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installModelsApi(page, 'super_admin');
  await page.goto('/models');
  await expect(page.getByRole('tablist', { name: 'Models' }).getByRole('tab')).toHaveCount(4);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  await page.getByRole('tab', { name: 'Usage' }).click();
  await expect(page.getByRole('heading', { name: 'Usage details' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  await page.getByRole('tab', { name: 'Global Models' }).click();
  await page.getByRole('button', { name: 'Create global model' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create model connection' });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByLabel('Upstream protocol')).toBeVisible();
  await expect(dialog.getByLabel('Upstream protocol').locator('option')).toHaveCount(2);
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});
