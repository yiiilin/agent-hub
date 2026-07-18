import { expect, test, type Page } from '@playwright/test';

const superAdmin = {
  id: 'admin-1',
  username: 'admin',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'super_admin'
};

type RuntimeFixture = {
  id: string;
  hostname: string;
  labels: string[];
  codex_version: string;
  capabilities: Record<string, unknown>;
  sandbox_mode: string;
  status: string;
  last_heartbeat_at: string;
  credential_rotation_requested_at: string | null;
};

type EnrollmentFixture = {
  id: string;
  created_by: string | null;
  expires_at: string;
  consumed_at: string | null;
  consumed_by_runtime_id: string | null;
  revoked_at: string | null;
  created_at: string;
};

function runtime(id: string, hostname: string, status = 'online'): RuntimeFixture {
  return {
    id,
    hostname,
    labels: ['linux'],
    codex_version: `codex-${id}`,
    capabilities: { model_proxy: true, driver: 'app-server' },
    sandbox_mode: 'workspace-write',
    status,
    last_heartbeat_at: '2026-07-17T08:00:00.000Z',
    credential_rotation_requested_at: null
  };
}

function agent(id: string, name: string, runtimeId: string | null) {
  return {
    id,
    name,
    instructions: '',
    visibility: 'private',
    public_to: [],
    runtime_id: runtimeId,
    owner_id: superAdmin.id,
    is_owner: true,
    can_manage: true,
    can_administer: true,
    can_invoke: true,
    model_policy: {},
    sandbox_policy: {},
    managed_skill_ids: [],
    mcp_allowlist: [],
    created_at: '2026-07-17T08:00:00.000Z',
    updated_at: '2026-07-17T08:00:00.000Z'
  };
}

async function mockBase(page: Page, runtimes: ReturnType<typeof runtime>[], agents: ReturnType<typeof agent>[] = []) {
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: superAdmin }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: agents }));
}

async function mountRuntimePage(page: Page) {
  const renderErrors: string[] = [];
  page.on('console', (message) => { if (message.type() === 'error') renderErrors.push(message.text()); });
  page.on('pageerror', (error) => renderErrors.push(error.message));
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: { oidc_mock: false } }));
  await page.goto('/runtimes');
  await expect(page.locator('.runtime-workspace')).toBeVisible();
  if (await page.locator('.runtime-enrollment-panel').count() > 0) return;

  // The main route is owned separately. Keep this focused spec runnable until it imports RuntimesPage.
  await page.goto('/login');
  await page.evaluate(async (user) => {
    const load = (path: string) => import(/* @vite-ignore */ path);
    const runtimeResponse = await fetch('/src/runtimes.tsx');
    if (!runtimeResponse.ok || !runtimeResponse.headers.get('content-type')?.includes('text/javascript')) {
      throw new Error('The /runtimes route has not integrated RuntimesPage.');
    }
    const runtimeSource = await runtimeResponse.text();
    const i18nPath = runtimeSource.match(/from "([^"?]*\/src\/i18n\.ts[^"?]*)"/)?.[1]
      ?? runtimeSource.match(/from "([^"]*\/src\/i18n\.ts[^"]*)"/)?.[1];
    if (!i18nPath) throw new Error('Unable to resolve the Runtime page i18n module.');
    const [reactModule, reactDomModule, i18n, runtimes] = await Promise.all([
      load('/node_modules/.vite/deps/react.js'),
      load('/node_modules/.vite/deps/react-dom_client.js'),
      load(i18nPath),
      load('/src/runtimes.tsx')
    ]);
    const react = reactModule.default ?? reactModule;
    const reactDom = reactDomModule.default ?? reactDomModule;
    document.body.innerHTML = '<div id="runtime-test-root"></div>';
    reactDom.createRoot(document.getElementById('runtime-test-root')).render(
      react.createElement(i18n.I18nProvider, null, react.createElement(runtimes.RuntimesPage, { user }))
    );
  }, superAdmin);
  try {
    await expect(page.locator('.runtime-workspace')).toBeVisible();
  } catch {
    const body = await page.locator('body').innerText();
    throw new Error(`Runtime test mount failed: ${renderErrors.join(' | ') || 'no browser error'}; body=${body.slice(0, 500)}`);
  }
}

test('enrollment requires an explicit dialog action and exposes only available tokens on the page', async ({ page }) => {
  const now = Date.now();
  const token = (id: string, overrides: Partial<EnrollmentFixture> = {}): EnrollmentFixture => ({
    id,
    created_by: superAdmin.id,
    expires_at: new Date(now + 30 * 60_000).toISOString(),
    consumed_at: null,
    consumed_by_runtime_id: null,
    revoked_at: null,
    created_at: new Date(now - 60_000).toISOString(),
    ...overrides
  });
  let enrollments = [
    token('available-token'),
    token('consumed-token', { consumed_at: new Date(now - 30_000).toISOString(), consumed_by_runtime_id: 'runtime-a' }),
    token('revoked-token', { revoked_at: new Date(now - 30_000).toISOString() }),
    token('expired-token', { expires_at: new Date(now - 1).toISOString() })
  ];
  let createCalls = 0;
  await mockBase(page, [runtime('runtime-a', 'alpha-runner')]);
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => {
    if (route.request().method() === 'POST') {
      createCalls += 1;
      const enrollment = token('created-token', { created_at: new Date(now).toISOString() });
      enrollments = [enrollment, ...enrollments];
      return route.fulfill({ json: { enrollment, token: 'ahre_visible_once' } });
    }
    return route.fulfill({ json: enrollments });
  });
  await page.route('**/api/admin/runtime-enrollment-tokens/*/revoke', (route) => {
    const id = route.request().url().split('/').at(-2)!;
    const updated = { ...enrollments.find((item) => item.id === id)!, revoked_at: new Date(now).toISOString() };
    enrollments = enrollments.map((item) => item.id === id ? updated : item);
    return route.fulfill({ json: updated });
  });

  await mountRuntimePage(page);
  const availableList = page.getByRole('region', { name: 'Enrollment history' });
  await expect(availableList.getByRole('listitem')).toHaveCount(1);
  await expect(availableList).not.toContainText('available-token');
  await expect(availableList).not.toContainText('consumed-token');
  await expect(availableList).not.toContainText('revoked-token');
  await expect(availableList).not.toContainText('expired-token');
  await expect(availableList.locator('ol')).toHaveCount(0);

  await page.getByRole('button', { name: 'Add runtime node' }).click();
  const dialog = page.getByRole('dialog', { name: 'Add runtime node' });
  await expect(dialog).toContainText('deploy/runtime.Dockerfile');
  await expect(dialog).toContainText('RUNTIME_ENROLLMENT_TOKEN=<token>');
  await expect(dialog).toContainText('docker run');
  expect(createCalls).toBe(0);

  await dialog.getByRole('button', { name: 'Create enrollment token' }).click();
  await expect(dialog.getByTestId('runtime-enrollment-token')).toHaveText('ahre_visible_once');
  expect(createCalls).toBe(1);
  await dialog.locator('footer').getByRole('button', { name: 'Close' }).click();
  await expect(page.getByText('ahre_visible_once', { exact: true })).toHaveCount(0);
  await expect(availableList.getByRole('listitem')).toHaveCount(2);
  await page.getByRole('button', { name: 'Add runtime node' }).click();
  await expect(dialog.getByTestId('runtime-enrollment-token')).toHaveCount(0);
  await dialog.locator('footer').getByRole('button', { name: 'Cancel' }).click();

  await availableList.getByRole('button', { name: 'Revoke token' }).first().click();
  await expect(availableList.getByRole('listitem')).toHaveCount(1);
});

test('drain follows Agent bindings while rotation, cancel, deletion and Session impact remain available', async ({ page }) => {
  let runtimes = [
    runtime('runtime-a', 'alpha-runner'),
    runtime('runtime-b', 'beta-runner'),
    runtime('runtime-c', 'charlie-runner', 'draining')
  ];
  const calls: string[] = [];
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: superAdmin }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [agent('agent-a', 'Release operator', 'runtime-a')] }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/admin/runtimes/runtime-a/credential-rotation', (route) => {
    calls.push('rotate');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, credential_rotation_requested_at: '2026-07-17T08:05:00.000Z' } : item);
    return route.fulfill({ json: runtimes.find((item) => item.id === 'runtime-a') });
  });
  await page.route('**/api/admin/runtimes/runtime-a/drain', (route) => {
    calls.push('drain');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, status: 'draining' } : item);
    return route.fulfill({ json: {
      runtime: runtimes.find((item) => item.id === 'runtime-a'),
      owned_sessions: [{ id: 'session-a', agent_name: 'Affected agent', lifecycle_status: 'saving' }]
    } });
  });
  await page.route('**/api/admin/runtimes/runtime-a/cancel-drain', (route) => {
    calls.push('cancel-drain');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, status: 'online' } : item);
    return route.fulfill({ json: { runtime: runtimes.find((item) => item.id === 'runtime-a'), owned_sessions: [] } });
  });
  await page.route('**/api/admin/runtimes/runtime-a/force-delete', (route) => {
    calls.push('force-delete');
    runtimes = runtimes.filter((item) => item.id !== 'runtime-a');
    return route.fulfill({ json: {
      runtime_id: 'runtime-a',
      recoverable_session_ids: ['session-recoverable'],
      recovery_failed_session_ids: ['session-failed']
    } });
  });
  await page.route('**/api/admin/runtimes/runtime-c', (route) => {
    calls.push('delete');
    runtimes = runtimes.filter((item) => item.id !== 'runtime-c');
    return route.fulfill({ status: 204, body: '' });
  });
  page.on('dialog', (dialog) => dialog.accept());

  await mountRuntimePage(page);
  const detail = page.getByRole('region', { name: 'Runtime details' });

  await page.getByRole('button', { name: /beta-runner/ }).click();
  await expect(detail.getByRole('button', { name: 'Drain runtime' })).toBeDisabled();

  await page.getByRole('button', { name: /alpha-runner/ }).click();
  await expect(detail.getByRole('button', { name: 'Drain runtime' })).toBeEnabled();
  await detail.getByRole('button', { name: 'Rotate credential' }).click();
  await detail.getByRole('button', { name: 'Drain runtime' }).click();
  await expect(detail.getByRole('link', { name: /Affected agent/ })).toHaveAttribute('href', '/sessions');
  await expect(detail).toContainText('saving');
  await detail.getByRole('button', { name: 'Cancel drain' }).click();
  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  await expect(page.locator('.force-result')).toContainText('session-recoverable');
  await expect(page.locator('.force-result')).toContainText('session-failed');

  await page.getByRole('button', { name: /charlie-runner/ }).click();
  await detail.getByRole('button', { name: 'Delete runtime', exact: true }).click();
  await expect(page.getByRole('button', { name: /charlie-runner/ })).toHaveCount(0);
  expect(calls).toEqual(['rotate', 'drain', 'cancel-drain', 'force-delete', 'delete']);
});

test('runtime enrollment and available tokens fit a 390px viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  const consoleErrors: string[] = [];
  const networkErrors: string[] = [];
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('pageerror', (error) => consoleErrors.push(error.message));
  page.on('requestfailed', (request) => networkErrors.push(`${request.method()} ${request.url()}`));
  page.on('response', (response) => { if (response.status() >= 400) networkErrors.push(`${response.status()} ${response.url()}`); });
  await mockBase(page, [runtime('runtime-a', 'alpha-runner')], [agent('agent-a', 'Release operator', 'runtime-a')]);
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [{
    id: 'mobile-token',
    created_by: superAdmin.id,
    expires_at: '2099-07-17T08:30:00.000Z',
    consumed_at: null,
    consumed_by_runtime_id: null,
    revoked_at: null,
    created_at: '2026-07-17T08:00:00.000Z'
  }] }));

  await mountRuntimePage(page);
  await expect(page.getByRole('region', { name: '未使用的注册令牌' }).getByRole('listitem')).toHaveCount(1);
  await page.getByRole('button', { name: '新增运行节点' }).click();
  const dialog = page.getByRole('dialog', { name: '新增运行节点' });
  await expect(dialog).toContainText('RUNTIME_CREDENTIAL_FILE');

  const geometry = await page.evaluate(() => ({
    documentWidth: document.documentElement.scrollWidth,
    dialog: document.querySelector('.runtime-enrollment-dialog')?.getBoundingClientRect().toJSON()
  }));
  expect(geometry.documentWidth).toBeLessThanOrEqual(390);
  expect(geometry.dialog).toBeTruthy();
  expect(geometry.dialog!.x).toBeGreaterThanOrEqual(0);
  expect(geometry.dialog!.x + geometry.dialog!.width).toBeLessThanOrEqual(390);
  expect(consoleErrors).toEqual([]);
  expect(networkErrors).toEqual([]);
});
