import { expect, test, type Page } from '@playwright/test';

const superAdmin = {
  id: 'admin-1',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'super_admin'
};

type RuntimeFixture = {
  id: string;
  hostname: string;
  labels: string[];
  engine_version: string;
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
    engine_version: `engine-${id}`,
    capabilities: { model_proxy: true, driver: 'pi' },
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
    secret_declarations: [],
    created_at: '2026-07-17T08:00:00.000Z',
    updated_at: '2026-07-17T08:00:00.000Z'
  };
}

async function mockBase(
  page: Page,
  runtimes: ReturnType<typeof runtime>[],
  agents: ReturnType<typeof agent>[] = [],
  user = superAdmin
) {
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: agents }));
}

async function mountRuntimePage(page: Page) {
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: { password_registration_enabled: false, password_login_enabled: true, ldap_login_enabled: false } }));
  await page.goto('/runtimes');
  await expect(page.locator('.runtime-workspace')).toBeVisible();
}

for (const { role, canAdminister } of [
  { role: 'super_admin', canAdminister: true },
  { role: 'admin', canAdminister: true },
  { role: 'member', canAdminister: false }
] as const) {
  test(`${role} receives the expected Runtime administration controls`, async ({ page }) => {
    const currentUser = { ...superAdmin, id: `user-${role}`, role };
    let enrollmentRequests = 0;
    await mockBase(
      page,
      [runtime('runtime-a', 'alpha-runner')],
      [agent('agent-a', 'Release operator', 'runtime-a')],
      currentUser
    );
    await page.route('**/api/admin/runtime-enrollment-tokens', (route) => {
      enrollmentRequests += 1;
      return route.fulfill({ json: [] });
    });

    await mountRuntimePage(page);
    const detail = page.getByRole('region', { name: 'Runtime details' });

    if (canAdminister) {
      await expect(page.getByRole('button', { name: 'Add runtime node' })).toBeVisible();
      await expect(page.getByRole('region', { name: 'Enrollment history' })).toBeVisible();
      await expect(detail.getByRole('heading', { name: 'Runtime administration' })).toBeVisible();
      await expect(detail.getByRole('button', { name: 'Rotate credential' })).toBeVisible();
      await expect(detail.getByRole('button', { name: 'Drain runtime' })).toBeEnabled();
      await expect(detail.getByRole('button', { name: 'Force-delete runtime' })).toBeVisible();
      expect(enrollmentRequests).toBe(1);
    } else {
      await expect(page.getByRole('button', { name: 'Add runtime node' })).toHaveCount(0);
      await expect(page.getByRole('region', { name: 'Enrollment history' })).toHaveCount(0);
      await expect(detail.getByRole('heading', { name: 'Runtime administration' })).toHaveCount(0);
      expect(enrollmentRequests).toBe(0);
    }
  });
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
    runtime('runtime-b', 'beta-runner')
  ];
  const calls: string[] = [];
  const drainBodies: unknown[] = [];
  let drainAttempts = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: superAdmin }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [agent('agent-a', 'Release operator', 'runtime-a')] }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/admin/runtimes/runtime-a/deletion-impact', (route) => {
    calls.push('preview');
    expect(route.request().method()).toBe('GET');
    return route.fulfill({ json: {
      runtime_id: 'runtime-a',
      hostname: 'alpha-runner',
      affected_sessions: [
        {
          session_id: 'session-owned',
          agent_name: 'Affected agent',
          lifecycle_status: 'saving',
          force_delete_disposition: 'recoverable'
        },
        {
          session_id: 'session-cross-user',
          agent_name: "Other user's Agent",
          lifecycle_status: 'running',
          force_delete_disposition: 'recovery_failed'
        }
      ]
    } });
  });
  await page.route('**/api/admin/runtimes/runtime-a/drain', async (route) => {
    calls.push('drain');
    drainBodies.push(route.request().postDataJSON());
    drainAttempts += 1;
    if (drainAttempts === 1) return route.fulfill({ status: 409, json: { error: 'impact changed' } });
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, status: 'draining' } : item);
    return route.fulfill({ json: {
      runtime: runtimes.find((item) => item.id === 'runtime-a'),
      owned_sessions: [{ id: 'session-a', agent_name: 'Affected agent', lifecycle_status: 'saving' }]
    } });
  });

  await mountRuntimePage(page);
  const detail = page.getByRole('region', { name: 'Runtime details' });

  await page.getByRole('button', { name: /beta-runner/ }).click();
  await expect(detail.getByRole('button', { name: 'Drain runtime' })).toBeDisabled();

  await page.getByRole('button', { name: /alpha-runner/ }).click();
  await expect(detail.getByRole('button', { name: 'Drain runtime' })).toBeEnabled();
  await detail.getByRole('button', { name: 'Drain runtime' }).click();
  const dialog = page.getByRole('dialog', { name: 'Drain runtime' });
  await expect(dialog).toBeVisible();
  expect(calls).toEqual(['preview']);
  await expect(dialog).toContainText('alpha-runner');
  await expect(dialog).toContainText('Affected agent');
  await expect(dialog).toContainText('session-owned');
  await expect(dialog).toContainText("Other user's Agent");
  await expect(dialog).toContainText('session-cross-user');
  await expect(dialog).toContainText('saving');
  await expect(dialog).toContainText('running');

  const confirmation = dialog.getByLabel('Confirm Runtime hostname');
  const submit = dialog.getByRole('button', { name: 'Drain runtime' });
  await expect(submit).toBeDisabled();
  await confirmation.fill('Alpha-runner');
  await expect(submit).toBeDisabled();
  await confirmation.fill('alpha-runner ');
  await expect(submit).toBeDisabled();
  await confirmation.fill('alpha-runner');
  await expect(submit).toBeEnabled();
  await dialog.getByRole('button', { name: 'Cancel' }).click();
  await expect(dialog).toHaveCount(0);
  expect(calls).toEqual(['preview']);
  expect(drainBodies).toEqual([]);

  await detail.getByRole('button', { name: 'Drain runtime' }).click();
  await dialog.getByLabel('Confirm Runtime hostname').fill('alpha-runner');
  await dialog.getByRole('button', { name: 'Drain runtime' }).click();
  await expect(page.getByRole('alert')).toContainText('Runtime administration action failed.');
  await expect(dialog).toBeVisible();
  await dialog.getByRole('button', { name: 'Drain runtime' }).click();
  await expect(detail.getByRole('link', { name: /Affected agent/ })).toHaveAttribute('href', '/sessions');
  await expect(detail).toContainText('saving');
  expect(calls).toEqual(['preview', 'preview', 'drain', 'drain']);
  expect(drainBodies).toEqual([{ hostname: 'alpha-runner' }, { hostname: 'alpha-runner' }]);
});

test('delete previews an empty impact and force-delete shows every Session disposition', async ({ page }) => {
  let runtimes = [
    runtime('runtime-a', 'alpha-runner'),
    runtime('runtime-b', 'beta-runner'),
    runtime('runtime-c', 'charlie-runner', 'draining')
  ];
  const calls: string[] = [];
  const bodies: Record<string, unknown[]> = { delete: [], forceDelete: [] };
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: superAdmin }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [agent('agent-a', 'Release operator', 'runtime-a')] }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/admin/runtimes/*/deletion-impact', (route) => {
    const runtimeId = new URL(route.request().url()).pathname.split('/').at(-2)!;
    calls.push(`preview:${runtimeId}`);
    if (runtimeId === 'runtime-c') {
      return route.fulfill({ json: {
        runtime_id: runtimeId,
        hostname: 'charlie-runner',
        affected_sessions: []
      } });
    }
    return route.fulfill({ json: {
      runtime_id: runtimeId,
      hostname: 'alpha-runner',
      affected_sessions: [
        {
          session_id: 'session-recoverable',
          agent_name: 'Recoverable agent',
          lifecycle_status: 'saving',
          force_delete_disposition: 'recoverable'
        },
        {
          session_id: 'session-failed',
          agent_name: 'Recovery-failed agent',
          lifecycle_status: 'running',
          force_delete_disposition: 'recovery_failed'
        }
      ]
    } });
  });
  await page.route('**/api/admin/runtimes/runtime-c', (route) => {
    calls.push('delete');
    bodies.delete.push(route.request().postDataJSON());
    runtimes = runtimes.filter((item) => item.id !== 'runtime-c');
    return route.fulfill({ status: 204, body: '' });
  });
  await page.route('**/api/admin/runtimes/runtime-a/force-delete', (route) => {
    calls.push('force-delete');
    bodies.forceDelete.push(route.request().postDataJSON());
    runtimes = runtimes.filter((item) => item.id !== 'runtime-a');
    return route.fulfill({ json: {
      runtime_id: 'runtime-a',
      recoverable_session_ids: ['session-recoverable'],
      recovery_failed_session_ids: ['session-failed']
    } });
  });

  await mountRuntimePage(page);
  const detail = page.getByRole('region', { name: 'Runtime details' });
  await page.getByRole('button', { name: /charlie-runner/ }).click();
  await detail.getByRole('button', { name: 'Delete runtime', exact: true }).click();
  const deleteDialog = page.getByRole('dialog', { name: 'Delete runtime' });
  await expect(deleteDialog).toContainText('charlie-runner');
  await expect(deleteDialog).toContainText('This Runtime no longer owns any Sessions.');
  expect(calls).toEqual(['preview:runtime-c']);
  await deleteDialog.getByLabel('Confirm Runtime hostname').fill('charlie-runner');
  await deleteDialog.getByRole('button', { name: 'Delete runtime' }).click();
  await expect(page.getByRole('button', { name: /charlie-runner/ })).toHaveCount(0);
  expect(calls).toEqual(['preview:runtime-c', 'delete']);
  expect(bodies.delete).toEqual([{ hostname: 'charlie-runner' }]);

  await page.getByRole('button', { name: /alpha-runner/ }).click();
  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  const forceDialog = page.getByRole('dialog', { name: 'Force-delete runtime' });
  await expect(forceDialog.getByRole('listitem').filter({ hasText: 'session-recoverable' })).toContainText('Recoverable Sessions');
  await expect(forceDialog.getByRole('listitem').filter({ hasText: 'session-failed' })).toContainText('Recovery-failed Sessions');
  expect(calls).toEqual(['preview:runtime-c', 'delete', 'preview:runtime-a']);
  await forceDialog.getByLabel('Confirm Runtime hostname').fill('alpha-runner');
  await forceDialog.getByRole('button', { name: 'Force-delete runtime' }).click();
  await expect(page.locator('.force-result')).toContainText('session-recoverable');
  await expect(page.locator('.force-result')).toContainText('session-failed');
  expect(calls).toEqual(['preview:runtime-c', 'delete', 'preview:runtime-a', 'force-delete']);
  expect(bodies.forceDelete).toEqual([{ hostname: 'alpha-runner' }]);
  await page.getByRole('button', { name: /beta-runner/ }).click();
  await expect(page.locator('.force-result')).toHaveCount(0);
});

test('switching Runtime aborts stale previews while close and preview failure leave no action state', async ({ page }) => {
  let releaseAlpha!: () => void;
  const alphaGate = new Promise<void>((resolve) => { releaseAlpha = resolve; });
  const calls: string[] = [];
  let writeCalls = 0;
  await mockBase(page, [
    runtime('runtime-a', 'alpha-runner'),
    runtime('runtime-b', 'beta-runner'),
    runtime('runtime-c', 'charlie-runner')
  ]);
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/admin/runtimes/*/deletion-impact', async (route) => {
    const runtimeId = new URL(route.request().url()).pathname.split('/').at(-2)!;
    calls.push(`preview:${runtimeId}`);
    if (runtimeId === 'runtime-a') {
      await alphaGate;
      return route.fulfill({ json: {
        runtime_id: runtimeId,
        hostname: 'alpha-runner',
        affected_sessions: [{
          session_id: 'session-alpha',
          agent_name: 'Alpha agent',
          lifecycle_status: 'running',
          force_delete_disposition: 'recoverable'
        }]
      } });
    }
    if (runtimeId === 'runtime-c') {
      return route.fulfill({ status: 500, json: { error: 'preview failed' } });
    }
    return route.fulfill({ json: {
      runtime_id: runtimeId,
      hostname: 'beta-runner',
      affected_sessions: [{
        session_id: 'session-beta',
        agent_name: 'Beta agent',
        lifecycle_status: 'saving',
        force_delete_disposition: 'recovery_failed'
      }]
    } });
  });
  await page.route('**/api/admin/runtimes/*/force-delete', (route) => {
    writeCalls += 1;
    return route.fulfill({ status: 409, json: { error: 'should not write' } });
  });

  await mountRuntimePage(page);
  const detail = page.getByRole('region', { name: 'Runtime details' });
  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  await expect.poll(() => calls).toEqual(['preview:runtime-a']);
  await page.getByRole('button', { name: /beta-runner/ }).click();
  releaseAlpha();
  await expect(page.getByRole('dialog')).toHaveCount(0);

  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  const betaDialog = page.getByRole('dialog', { name: 'Force-delete runtime' });
  await expect(betaDialog).toContainText('session-beta');
  await expect(betaDialog).not.toContainText('session-alpha');
  const confirmation = betaDialog.getByLabel('Confirm Runtime hostname');
  await expect(confirmation).toHaveValue('');
  await confirmation.fill('beta-runner');
  await betaDialog.getByRole('button', { name: 'Cancel' }).click();
  expect(writeCalls).toBe(0);

  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  await expect(betaDialog.getByLabel('Confirm Runtime hostname')).toHaveValue('');
  await expect(betaDialog).toContainText('session-beta');
  await expect(betaDialog).not.toContainText('session-alpha');
  await betaDialog.getByRole('button', { name: 'Cancel' }).click();

  await page.getByRole('button', { name: /charlie-runner/ }).click();
  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  await expect(page.getByRole('alert')).toContainText('Runtime administration action failed.');
  await expect(page.getByRole('dialog')).toHaveCount(0);
  expect(calls).toEqual(['preview:runtime-a', 'preview:runtime-b', 'preview:runtime-b', 'preview:runtime-c']);
  expect(writeCalls).toBe(0);
});

test('drain, delete, and force-delete previews fit a 390px viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  const consoleErrors: string[] = [];
  const networkErrors: string[] = [];
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('pageerror', (error) => consoleErrors.push(error.message));
  page.on('requestfailed', (request) => networkErrors.push(`${request.method()} ${request.url()}`));
  page.on('response', (response) => { if (response.status() >= 400) networkErrors.push(`${response.status()} ${response.url()}`); });
  await mockBase(page, [
    runtime('runtime-a', 'alpha-runner'),
    runtime('runtime-c', 'charlie-runner', 'draining')
  ], [agent('agent-a', 'Release operator', 'runtime-a')]);
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/admin/runtimes/*/deletion-impact', (route) => {
    const runtimeId = new URL(route.request().url()).pathname.split('/').at(-2)!;
    return route.fulfill({ json: {
      runtime_id: runtimeId,
      hostname: runtimeId === 'runtime-a' ? 'alpha-runner' : 'charlie-runner',
      affected_sessions: [{
        session_id: 'session-with-a-long-cross-user-identifier',
        agent_name: '跨用户影响智能体名称',
        lifecycle_status: 'saving',
        force_delete_disposition: 'recovery_failed'
      }]
    } });
  });

  await mountRuntimePage(page);
  const detail = page.getByRole('region', { name: '运行节点详情' });
  const assertDialogFits = async (name: string) => {
    const dialog = page.getByRole('dialog', { name });
    await expect(dialog).toBeVisible();
    const bounds = await dialog.boundingBox();
    expect(bounds).not.toBeNull();
    expect(bounds!.x).toBeGreaterThanOrEqual(0);
    expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(390);
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
    return dialog;
  };

  await detail.getByRole('button', { name: '排空运行节点' }).click();
  const drainDialog = await assertDialogFits('排空运行节点');
  await expect(drainDialog).toContainText('session-with-a-long-cross-user-identifier');
  await drainDialog.getByRole('button', { name: '取消' }).click();

  await detail.getByRole('button', { name: '强制删除运行节点', exact: true }).click();
  const forceDialog = await assertDialogFits('强制删除运行节点');
  await expect(forceDialog).toContainText('恢复失败会话');
  await forceDialog.getByRole('button', { name: '取消' }).click();

  await page.getByRole('button', { name: /charlie-runner/ }).click();
  await detail.getByRole('button', { name: '删除运行节点', exact: true }).click();
  const deleteDialog = await assertDialogFits('删除运行节点');
  await deleteDialog.getByRole('button', { name: '取消' }).click();

  expect(consoleErrors).toEqual([]);
  expect(networkErrors).toEqual([]);
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
