import { expect, test, type Page, type Route } from '@playwright/test';

const superAdmin = {
  id: 'user-admin',
  username: 'admin',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'super_admin'
};

const member = { ...superAdmin, id: 'user-member', username: 'member', role: 'member' };

function session(id: string, agentName: string, lifecycleStatus: string, overrides: Record<string, unknown> = {}) {
  return {
    id,
    owner_id: superAdmin.id,
    agent_id: `agent-${id}`,
    agent_name: agentName,
    agent_deleted_at: null,
    origin: { kind: 'hub_native' },
    lifecycle_status: lifecycleStatus,
    native_thread_id: `thread-${id}`,
    active_turn_id: null,
    history_checkpoint: 2,
    configuration_fingerprint: null,
    runtime_owner_id: 'runtime-a',
    ownership_generation: 1,
    recovery_error: null,
    current_bundle: null,
    created_at: '2026-07-15T08:00:00.000Z',
    updated_at: '2026-07-15T09:00:00.000Z',
    ...overrides
  };
}

function message(sessionId: string, sequence: number, role: string, content: string, overrides: Record<string, unknown> = {}) {
  return {
    id: `message-${sessionId}-${sequence}`,
    session_id: sessionId,
    sequence,
    role,
    message_kind: 'message',
    content,
    payload: {},
    delivery_mode: 'next_turn',
    delivery_state: 'delivered',
    client_message_key: null,
    expected_native_turn_id: null,
    turn_id: `turn-${sessionId}`,
    run_id: `run-${sessionId}`,
    accepted_at: `2026-07-15T08:0${sequence}:00.000Z`,
    ...overrides
  };
}

async function routeMe(page: Page, user = superAdmin) {
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
}

test('Session history exposes queued work, explicit stop, recovery failure, and Historical Session read-only state', async ({ page }) => {
  const active = session('active', 'Release agent', 'online', { active_turn_id: 'turn-active' });
  const saving = session('saving', 'Checkpoint agent', 'saving');
  const failed = session('failed', 'Recovery agent', 'recovery_failed', { recovery_error: 'native thread could not resume' });
  const historical = session('historical', 'Deleted agent', 'historical', {
    agent_deleted_at: '2026-07-15T09:30:00.000Z',
    native_thread_id: null,
    runtime_owner_id: null
  });
  const sessions = [active, saving, failed, historical];
  const messages: Record<string, unknown[]> = {
    active: [
      message('active', 1, 'user', 'Inspect the rollout.'),
      message('active', 2, 'assistant', 'The rollout is still active.', { delivery_state: 'delivering' }),
      message('active', 3, 'user', 'Check the Linux runtime first.', { delivery_mode: 'steer', delivery_state: 'queued' })
    ],
    saving: [message('saving', 1, 'user', 'Save this workspace.', { delivery_state: 'queued' })],
    failed: [message('failed', 1, 'user', 'Resume after restore.')],
    historical: [message('historical', 1, 'assistant', 'Retained historical answer.')]
  };
  let stopRequests = 0;
  await routeMe(page);
  await page.route('**/api/sessions', (route) => route.fulfill({ json: sessions }));
  await page.route(/\/api\/sessions\/[^/]+\/messages$/, (route) => {
    const id = new URL(route.request().url()).pathname.split('/')[3];
    return route.fulfill({ json: messages[id] ?? [] });
  });
  await page.route('**/api/runs/run-active/stop', (route) => {
    stopRequests += 1;
    return route.fulfill({ json: { id: 'run-active', status: 'running' } });
  });

  await page.goto('/sessions');
  await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
  await expect(page.getByRole('button', { name: /Checkpoint agent/ })).toContainText('saving');
  await page.getByRole('button', { name: /Release agent/ }).click();
  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the rollout.')).toBeVisible();
  await expect(detail.getByText('Check the Linux runtime first.')).toBeVisible();
  await expect(detail.getByText('queued', { exact: true })).toBeVisible();
  await detail.getByRole('button', { name: 'Stop current run' }).click();
  await expect(detail.getByText('Stop requested. Completed actions are retained.')).toBeVisible();
  expect(stopRequests).toBe(1);

  await page.getByRole('button', { name: /Recovery agent/ }).click();
  await expect(detail).toContainText('native thread could not resume');
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveCount(0);

  await page.getByRole('button', { name: /Deleted agent/ }).click();
  await expect(detail).toContainText('Historical Session');
  await expect(detail.getByText('Retained historical answer.')).toBeVisible();
  await expect(detail.getByRole('button', { name: 'Send' })).toHaveCount(0);
});

test('Session workspace is localized and has no horizontal overflow at 390px', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  await routeMe(page);
  await page.route('**/api/sessions', (route) => route.fulfill({ json: [
    session('saving', '保存中的智能体', 'saving'),
    session('failed', '恢复失败的智能体', 'recovery_failed', { recovery_error: '无法恢复原生线程' })
  ] }));
  await page.route(/\/api\/sessions\/[^/]+\/messages$/, (route) => route.fulfill({ json: [] }));

  await page.goto('/sessions');
  await expect(page.getByRole('heading', { name: '会话' })).toBeVisible();
  await expect(page.getByRole('button', { name: /保存中的智能体/ })).toContainText('保存中');
  await page.getByRole('button', { name: /恢复失败的智能体/ }).click();
  await expect(page.getByRole('region', { name: '会话详情' })).toContainText('恢复失败');
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
});

async function mockAdministration(page: Page, user = superAdmin) {
  await routeMe(page, user);
  let policy = {
    password_registration_enabled: true,
    password_login_enabled: true,
    email_verification_required: false
  };
  let platforms = [{ id: 'platform-1', key: 'github', name: 'GitHub' }];
  let channels = [{ id: 'channel-1', platform_id: 'platform-1', key: 'oauth', name: 'OAuth', enabled: true, trusted_email: true }];
  let rollout = {
    active_version: '0.30.0' as string | null, target_version: null as string | null, status: 'active', error: null, artifacts: [],
    runtimes: [{ runtime_id: 'runtime-a', hostname: 'alpha', os: 'linux', architecture: 'x86_64', current_version: '0.30.0', target_version: null as string | null, status: 'active', error: null, checked_at: null }],
    updated_at: '2026-07-15T09:00:00.000Z'
  };
  const users = [
    { user: superAdmin, email_verified: true, has_password: true, created_at: '2026-07-10T08:00:00.000Z' },
    { user: { id: 'user-2', username: 'alice', email: 'alice@example.com', display_name: 'Alice', role: 'member' }, email_verified: true, has_password: false, created_at: '2026-07-11T08:00:00.000Z' }
  ];
  const erasures: unknown[] = [];
  const bodies: Record<string, unknown[]> = {};
  const record = async (route: Route, key: string) => {
    const body = route.request().postDataJSON();
    (bodies[key] ??= []).push(body);
    return body;
  };
  await page.route('**/api/admin/users', (route) => route.fulfill({ json: users }));
  await page.route(/\/api\/admin\/users\/[^/]+$/, (route) => {
    const userId = new URL(route.request().url()).pathname.split('/').at(-1);
    return route.fulfill({ json: users.find((detail) => detail.user.id === userId) });
  });
  await page.route('**/api/admin/user-erasures', (route) => route.fulfill({ json: erasures }));
  await page.route('**/api/admin/auth-policy', async (route) => {
    if (route.request().method() === 'PATCH') policy = await record(route, 'policy') as typeof policy;
    return route.fulfill({ json: policy });
  });
  await page.route('**/api/admin/external-platforms', async (route) => {
    if (route.request().method() === 'POST') {
      const body = await record(route, 'platform') as { key: string; name: string };
      platforms = [...platforms, { id: 'platform-2', ...body }];
      return route.fulfill({ json: platforms.at(-1) });
    }
    return route.fulfill({ json: platforms });
  });
  await page.route('**/api/admin/external-platforms/*/authentication-channels', async (route) => {
    if (route.request().method() === 'POST') {
      const body = await record(route, 'channel') as Omit<(typeof channels)[number], 'id' | 'platform_id'>;
      channels = [...channels, { id: 'channel-2', platform_id: 'platform-1', ...body }];
      return route.fulfill({ json: channels.at(-1) });
    }
    return route.fulfill({ json: channels });
  });
  await page.route('**/api/admin/authentication-channels/*', async (route) => {
    const body = await record(route, 'channelUpdate') as Pick<(typeof channels)[number], 'name' | 'enabled' | 'trusted_email'>;
    channels = channels.map((channel) => channel.id === 'channel-1' ? { ...channel, ...body } : channel);
    return route.fulfill({ json: channels[0] });
  });
  await page.route('**/api/admin/users/user-2/erase', async (route) => {
    const body = await record(route, 'erasure');
    const result = { user_id: 'user-2', username: 'alice', status: 'completed', requested_at: '2026-07-15T10:00:00.000Z', completed_at: '2026-07-15T10:00:00.000Z' };
    erasures.unshift(result);
    return route.fulfill({ status: 202, json: result });
  });
  await page.route('**/api/admin/codex-version-rollout', (route) => route.fulfill({ json: rollout }));
  await page.route('**/api/admin/codex-version-rollout/target', async (route) => {
    const body = await record(route, 'target') as { version: string };
    rollout = { ...rollout, target_version: body.version, status: 'ready', runtimes: rollout.runtimes.map((runtime) => ({ ...runtime, target_version: body.version, status: 'ready' })) };
    return route.fulfill({ json: rollout });
  });
  await page.route('**/api/admin/codex-version-rollout/promote', async (route) => {
    await record(route, 'promote');
    rollout = { ...rollout, active_version: rollout.target_version, target_version: null, status: 'active' };
    return route.fulfill({ json: rollout });
  });
  return bodies;
}

test('Super Administrator manages identity policy, trusted channels, user erasure, and exact Codex rollout', async ({ page }) => {
  const bodies = await mockAdministration(page);
  await page.goto('/administration');
  await expect(page.getByRole('heading', { name: 'Administration' })).toBeVisible();

  await page.getByLabel('Password registration').uncheck();
  await page.getByRole('button', { name: 'Save authentication policy' }).click();
  expect(bodies.policy).toEqual([{ password_registration_enabled: false, password_login_enabled: true, email_verification_required: false }]);

  await page.getByRole('tab', { name: 'External platforms' }).click();
  await page.getByRole('button', { name: 'Add platform' }).click();
  const createPlatformDialog = page.getByRole('dialog', { name: 'Add platform' });
  await createPlatformDialog.getByLabel('Platform key').fill('slack');
  await createPlatformDialog.getByLabel('Platform name').fill('Slack');
  await createPlatformDialog.getByRole('button', { name: 'Add platform' }).click();
  await expect(page.getByText('Slack', { exact: true })).toBeVisible();

  await page.getByRole('button', { name: 'Edit external platform: GitHub' }).click();
  const editPlatformDialog = page.getByRole('dialog', { name: 'Edit external platform' });
  await editPlatformDialog.getByLabel('Trusted email', { exact: true }).uncheck();
  await editPlatformDialog.getByRole('button', { name: 'Save channel' }).click();
  expect(bodies.channelUpdate).toEqual([{ name: 'OAuth', enabled: true, trusted_email: false }]);
  await editPlatformDialog.getByRole('button', { name: 'Cancel', exact: true }).click();

  await page.getByRole('tab', { name: 'User management' }).click();
  const alice = page.getByRole('row', { name: /alice/ });
  await alice.getByRole('button', { name: 'Delete user: alice' }).click();
  const deleteUserDialog = page.getByRole('dialog', { name: 'Delete user' });
  await deleteUserDialog.getByLabel('Confirm username').fill('alice');
  await deleteUserDialog.getByRole('button', { name: 'Delete user' }).click();
  expect(bodies.erasure).toEqual([{ username: 'alice' }]);
  await expect(page.getByText('completed', { exact: true })).toBeVisible();

  await page.getByRole('tab', { name: 'Codex version' }).click();
  await page.getByLabel('Target Codex version').fill('0.31.0');
  await page.getByRole('button', { name: 'Prepare version' }).click();
  await expect(page.getByText('0.31.0', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Promote ready version' }).click();
  await expect(page.getByText('0.31.0', { exact: true })).toBeVisible();
  expect(bodies.target).toEqual([{ version: '0.31.0' }]);
  expect(bodies.promote).toHaveLength(1);
});

test('Administration navigation is Super Administrator-only', async ({ page }) => {
  await routeMe(page, member);
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: [] }));
  await page.goto('/agents');
  await expect(page.getByRole('button', { name: 'Administration' })).toHaveCount(0);
  await page.goto('/administration');
  await expect(page.getByRole('heading', { name: 'Page not found' })).toBeVisible();
});

test('Administration remains operable in Chinese at 390px without browser or network failures', async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}`));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  await mockAdministration(page);

  await page.goto('/administration');
  await expect(page.getByRole('heading', { name: '管理' })).toBeVisible();
  await expect(page.getByRole('heading', { name: '认证策略' })).toBeVisible();
  const platformsTab = page.getByRole('tab', { name: '外部平台' });
  await platformsTab.click();
  await expect(platformsTab).toHaveAttribute('aria-selected', 'true');
  await expect(page.getByRole('table', { name: '外部平台' })).toBeVisible();
  await page.getByRole('tab', { name: '用户管理' }).click();
  await expect(page.getByRole('heading', { name: '用户管理' })).toBeVisible();
  await page.getByRole('tab', { name: 'Codex 版本' }).click();
  await expect(page.getByRole('heading', { name: 'Codex 版本发布' })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
});
