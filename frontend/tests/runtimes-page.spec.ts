import { expect, test, type Page } from '@playwright/test';

const user = {
  id: 'user-1',
  email: 'admin@example.com',
  display_name: 'Admin',
  role: 'admin'
};

function runtime(id: string, hostname: string, status: string, overrides: Record<string, unknown> = {}) {
  return {
    id,
    hostname,
    labels: ['linux', `zone:${id}`],
    codex_version: `codex-${id}`,
    capabilities: { model_proxy: true, driver: 'app-server' },
    sandbox_mode: 'workspace-write',
    status,
    last_heartbeat_at: '2026-07-11T08:00:00.000Z',
    ...overrides
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
    owner_id: user.id,
    is_owner: true,
    can_manage: true,
    can_administer: true,
    can_invoke: true,
    model_policy: {},
    sandbox_policy: {},
    skills_manifest: [],
    managed_skill_ids: [],
    mcp_allowlist: [],
    created_at: '2026-07-11T08:00:00.000Z',
    updated_at: '2026-07-11T08:00:00.000Z'
  };
}

async function mockRuntimePage(page: Page, runtimeResponses: Array<ReturnType<typeof runtime>[]>) {
  let requestCount = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/agents', (route) => route.fulfill({
    json: [agent('agent-a', 'Release operator', 'runtime-a'), agent('agent-b', 'Unbound agent', null)]
  }));
  await page.route('**/api/runtimes', (route) => {
    const response = runtimeResponses[Math.min(requestCount, runtimeResponses.length - 1)];
    requestCount += 1;
    return route.fulfill({ json: response });
  });
  return () => requestCount;
}

test('runtime workspace filters rows and shows the selected runtime details', async ({ page }) => {
  const alpha = runtime('runtime-a', 'alpha-runner', 'online', {
    capabilities: { model_proxy: true, driver: 'app-server', thread_resume: true, unknown_secret: 'do-not-render' }
  });
  const beta = runtime('runtime-b', 'beta-runner', 'offline', {
    labels: ['linux', 'degraded'],
    sandbox_mode: 'read-only'
  });
  await mockRuntimePage(page, [[alpha, beta]]);

  await page.goto('/runtimes');
  await expect(page.getByRole('heading', { name: 'Runtime Nodes' })).toBeVisible();
  await expect(page.getByRole('button', { name: /alpha-runner/ })).toContainText('codex-runtime-a');
  await expect(page.getByRole('button', { name: /beta-runner/ })).toContainText('offline');

  await page.getByRole('radio', { name: /Issues/ }).click();
  await expect(page.getByRole('button', { name: /alpha-runner/ })).toHaveCount(0);
  await page.getByLabel('Search runtimes').fill('beta');
  await page.getByRole('button', { name: /beta-runner/ }).click();

  const detail = page.getByRole('region', { name: 'Runtime details' });
  await expect(detail.getByRole('heading', { name: 'beta-runner' })).toBeVisible();
  await expect(detail).toContainText('runtime-b');
  await expect(detail).toContainText('read-only');
  await expect(detail).toContainText('linux');
  await expect(detail).toContainText('Model proxy');
  await expect(detail).toContainText('No agents are bound to this runtime.');

  await page.getByRole('radio', { name: /All/ }).click();
  await page.getByLabel('Search runtimes').fill('alpha');
  await page.getByRole('button', { name: /alpha-runner/ }).click();
  await expect(detail.getByRole('link', { name: 'Release operator' })).toHaveAttribute('href', '/agents/agent-a');
  await expect(detail).toContainText('driver');
  await expect(detail).toContainText('thread_resume');
  await expect(detail).not.toContainText('unknown_secret');
  await expect(detail).not.toContainText('do-not-render');
});

test('Online filter supports native radio arrow-key navigation', async ({ page }) => {
  await mockRuntimePage(page, [[
    runtime('runtime-a', 'alpha-runner', 'online'),
    runtime('runtime-b', 'beta-runner', 'offline')
  ]]);
  await page.goto('/runtimes');

  const all = page.getByRole('radio', { name: /All/ });
  const online = page.getByRole('radio', { name: /Online/ });
  await all.focus();
  await all.press('ArrowRight');

  await expect(online).toBeChecked();
  await expect(page.getByRole('button', { name: /alpha-runner/ })).toBeVisible();
  await expect(page.getByRole('button', { name: /beta-runner/ })).toHaveCount(0);
});

test('a response slower than the poll interval eventually renders without overlapping requests', async ({ page }) => {
  let activeRequests = 0;
  let maxActiveRequests = 0;
  let requestCount = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/runtimes', async (route) => {
    requestCount += 1;
    activeRequests += 1;
    maxActiveRequests = Math.max(maxActiveRequests, activeRequests);
    await new Promise((resolve) => setTimeout(resolve, 2_200));
    activeRequests -= 1;
    await route.fulfill({ json: [runtime('runtime-slow', 'slow-runner', 'online')] });
  });

  await page.goto('/runtimes');
  await expect(page.getByRole('status')).toHaveText('Loading runtime nodes...');
  await expect(page.getByRole('button', { name: /slow-runner/ })).toBeVisible({ timeout: 5_000 });
  expect(requestCount).toBe(1);
  expect(maxActiveRequests).toBe(1);
});

test('a fast runtime failure waits for slow agents before scheduling the next poll', async ({ page }) => {
  let resolveFirstAgents!: () => void;
  const firstAgents = new Promise<void>((resolve) => { resolveFirstAgents = resolve; });
  let runtimeRequests = 0;
  let agentRequests = 0;
  let activeAgentRequests = 0;
  let maxActiveAgentRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/runtimes', (route) => {
    runtimeRequests += 1;
    return route.fulfill({ status: 500, json: { error: 'request failed' } });
  });
  await page.route('**/api/agents', async (route) => {
    agentRequests += 1;
    activeAgentRequests += 1;
    maxActiveAgentRequests = Math.max(maxActiveAgentRequests, activeAgentRequests);
    if (agentRequests === 1) await firstAgents;
    activeAgentRequests -= 1;
    await route.fulfill({ json: [] });
  });

  await page.clock.install();
  await page.goto('/runtimes');
  await expect.poll(() => runtimeRequests).toBe(1);
  await page.clock.fastForward(10_000);
  expect(runtimeRequests).toBe(1);
  expect(maxActiveAgentRequests).toBe(1);

  resolveFirstAgents();
  await expect(page.getByRole('alert')).toBeVisible();
  await page.clock.fastForward(1_900);
  expect(runtimeRequests).toBe(1);
  await page.clock.fastForward(200);
  await expect.poll(() => runtimeRequests).toBe(2);
  expect(maxActiveAgentRequests).toBe(1);
});

test('unmounting stops polling and ignores a pending response', async ({ page }) => {
  let resolveResponse!: () => void;
  const pendingResponse = new Promise<void>((resolve) => { resolveResponse = resolve; });
  let runtimeRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/skills', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/runtimes', async (route) => {
    runtimeRequests += 1;
    await pendingResponse;
    await route.fulfill({ json: [runtime('runtime-late', 'late-runner', 'online')] });
  });

  await page.goto('/runtimes');
  await page.getByRole('button', { name: 'Skills' }).click();
  resolveResponse();
  await expect(page.getByRole('heading', { name: 'Skills' })).toBeVisible();
  await page.waitForTimeout(2_200);

  expect(runtimeRequests).toBe(1);
  await expect(page.getByText('late-runner')).toHaveCount(0);
});

test('loading, error retry, and empty states are accessible', async ({ page }) => {
  let runtimeRequests = 0;
  let resolveFirst!: () => void;
  const firstResponse = new Promise<void>((resolve) => { resolveFirst = resolve; });
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: user }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [] }));
  await page.route('**/api/runtimes', async (route) => {
    runtimeRequests += 1;
    if (runtimeRequests === 1) {
      await firstResponse;
      return route.fulfill({ status: 500, json: { error: 'request failed' } });
    }
    return route.fulfill({ json: [] });
  });

  await page.goto('/runtimes');
  await expect(page.getByRole('status')).toHaveText('Loading runtime nodes...');
  resolveFirst();
  const alert = page.getByRole('alert');
  await expect(alert).toContainText('Unable to load runtime nodes. Retry.');
  await alert.getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByText('No runtime nodes are registered.')).toBeVisible();
  await expect(alert).toHaveCount(0);
});

test('polling preserves a valid selection and falls back when it disappears', async ({ page }) => {
  const alpha = runtime('runtime-a', 'alpha-runner', 'online');
  const beta = runtime('runtime-b', 'beta-runner', 'offline');
  const requests = await mockRuntimePage(page, [[alpha, beta], [beta, alpha], [alpha]]);

  await page.clock.install();
  await page.goto('/runtimes');
  await page.getByRole('button', { name: /beta-runner/ }).click();
  const detail = page.getByRole('region', { name: 'Runtime details' });
  await expect(detail.getByRole('heading', { name: 'beta-runner' })).toBeVisible();

  await page.clock.fastForward(2100);
  await expect.poll(requests).toBeGreaterThanOrEqual(2);
  await expect(detail.getByRole('heading', { name: 'beta-runner' })).toBeVisible();

  await page.clock.fastForward(2100);
  await expect.poll(requests).toBeGreaterThanOrEqual(3);
  await expect(detail.getByRole('heading', { name: 'alpha-runner' })).toBeVisible();
});

test('runtime workspace stacks without horizontal overflow and localizes all controls', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.addInitScript(() => localStorage.setItem('agent-hub-language', 'zh-CN'));
  await mockRuntimePage(page, [[runtime('runtime-a', 'alpha-runner', 'online')]]);

  await page.goto('/runtimes');
  await expect(page.getByRole('heading', { name: '运行节点' })).toBeVisible();
  await expect(page.getByLabel('搜索运行节点')).toBeVisible();
  await expect(page.getByRole('radio', { name: /全部/ })).toBeVisible();
  const detailRegion = page.getByRole('region', { name: '运行节点详情' });
  await expect(detailRegion).toBeVisible();
  for (const text of ['标识与状态', '主机名', 'Codex 版本', '最近心跳', '执行环境', '沙箱', '模型代理', '能力', '标签', '绑定的智能体']) {
    await expect(detailRegion.getByText(text, { exact: true })).toBeVisible();
  }
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  const list = await page.locator('.runtime-master').boundingBox();
  const detail = await page.locator('.runtime-detail').boundingBox();
  expect(list).not.toBeNull();
  expect(detail).not.toBeNull();
  expect(detail!.y).toBeGreaterThanOrEqual(list!.y + list!.height - 1);
});

test('desktop keeps navigation fixed and runtime panes independently scrollable', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 720 });
  await mockRuntimePage(page, [[runtime('runtime-a', 'alpha-runner', 'online')]]);
  await page.goto('/runtimes');
  await expect(page.getByRole('button', { name: /alpha-runner/ })).toBeVisible();

  const layout = await page.evaluate(() => ({
    sidebarPosition: getComputedStyle(document.querySelector('.sidebar')!).position,
    listOverflow: getComputedStyle(document.querySelector('.runtime-list')!).overflowY,
    detailOverflow: getComputedStyle(document.querySelector('.runtime-detail')!).overflowY,
    documentHeight: document.documentElement.scrollHeight,
    viewportHeight: window.innerHeight
  }));
  expect(layout.sidebarPosition).toBe('fixed');
  expect(layout.listOverflow).toBe('auto');
  expect(layout.detailOverflow).toBe('auto');
  expect(layout.documentHeight).toBeLessThanOrEqual(layout.viewportHeight);
});

test('Super Administrator enrolls, rotates, drains, and deletes Runtime nodes with affected Session feedback', async ({ page }) => {
  const admin = { ...user, username: 'admin', role: 'super_admin' };
  let runtimes = [
    runtime('runtime-a', 'alpha-runner', 'online', { credential_rotation_requested_at: null }),
    runtime('runtime-b', 'beta-runner', 'draining', { credential_rotation_requested_at: null })
  ];
  let enrollments: Record<string, unknown>[] = [];
  const calls: string[] = [];
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: admin }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{
    id: 'agent-bound',
    owner_id: admin.id,
    name: 'Bound agent',
    runtime_id: 'runtime-a'
  }] }));
  await page.route('**/api/runtimes', (route) => route.fulfill({ json: runtimes }));
  await page.route('**/api/admin/runtime-enrollment-tokens', (route) => {
    if (route.request().method() === 'POST') {
      const enrollment = { id: 'enrollment-1', created_by: admin.id, expires_at: '2099-07-15T10:30:00.000Z', consumed_at: null, consumed_by_runtime_id: null, revoked_at: null, created_at: '2026-07-15T10:00:00.000Z' };
      enrollments = [enrollment];
      calls.push('enroll');
      return route.fulfill({ json: { enrollment, token: 'ahre_shown_once' } });
    }
    return route.fulfill({ json: enrollments });
  });
  await page.route('**/api/admin/runtime-enrollment-tokens/*/revoke', (route) => {
    calls.push('revoke');
    enrollments = enrollments.map((item) => ({ ...item, revoked_at: '2026-07-15T10:05:00.000Z' }));
    return route.fulfill({ json: enrollments[0] });
  });
  await page.route('**/api/admin/runtimes/runtime-a/credential-rotation', (route) => {
    calls.push('rotate');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, credential_rotation_requested_at: '2026-07-15T10:00:00.000Z' } : item);
    return route.fulfill({ json: runtimes[0] });
  });
  await page.route('**/api/admin/runtimes/runtime-a/drain', (route) => {
    calls.push('drain');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, status: 'draining' } : item);
    return route.fulfill({ json: {
      runtime: runtimes[0],
      owned_sessions: [{ id: 'session-a', agent_name: 'Affected agent', lifecycle_status: 'saving' }]
    } });
  });
  await page.route('**/api/admin/runtimes/runtime-a/cancel-drain', (route) => {
    calls.push('cancel-drain');
    runtimes = runtimes.map((item) => item.id === 'runtime-a' ? { ...item, status: 'online' } : item);
    return route.fulfill({ json: { runtime: runtimes[0], owned_sessions: [] } });
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
  await page.route('**/api/admin/runtimes/runtime-b', (route) => {
    calls.push('delete');
    runtimes = runtimes.filter((item) => item.id !== 'runtime-b');
    return route.fulfill({ status: 204, body: '' });
  });
  page.on('dialog', (dialog) => dialog.accept());

  await page.goto('/runtimes');
  await page.getByRole('button', { name: 'Add runtime node' }).click();
  const enrollmentDialog = page.getByRole('dialog', { name: 'Add runtime node' });
  await enrollmentDialog.getByRole('button', { name: 'Create enrollment token' }).click();
  await expect(enrollmentDialog.getByText('ahre_shown_once', { exact: true })).toBeVisible();
  await enrollmentDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  await page.getByRole('button', { name: 'Revoke token' }).click();

  await page.getByRole('button', { name: /alpha-runner/ }).click();
  const detail = page.getByRole('region', { name: 'Runtime details' });
  await detail.getByRole('button', { name: 'Rotate credential' }).click();
  await detail.getByRole('button', { name: 'Drain runtime' }).click();
  await expect(detail.getByText('Affected agent')).toBeVisible();
  await expect(detail.getByText('saving', { exact: true })).toBeVisible();
  await detail.getByRole('button', { name: 'Cancel drain' }).click();
  await detail.getByRole('button', { name: 'Force-delete runtime', exact: true }).click();
  const forceResult = page.locator('.force-result');
  await expect(forceResult).toContainText('session-recoverable');
  await expect(forceResult).toContainText('session-failed');

  await page.getByRole('button', { name: /beta-runner/ }).click();
  await detail.getByRole('button', { name: 'Delete runtime', exact: true }).click();
  await expect(page.getByRole('button', { name: /beta-runner/ })).toHaveCount(0);
  expect(calls).toEqual(['enroll', 'revoke', 'rotate', 'drain', 'cancel-drain', 'force-delete', 'delete']);
});
