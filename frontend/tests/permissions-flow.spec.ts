import { expect, test, type Page, type Route } from '@playwright/test';

async function signInWithMockOidc(page: import('@playwright/test').Page, email: string) {
  await page.goto('/login');
  await page.getByLabel('Email').fill(email);
  await page.getByRole('button', { name: 'Sign in with Mock OIDC' }).click();
  await expect(page.getByText(email)).toBeVisible();
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

type ControlledRunEventSourceFixture = {
  emit: (runId: string, event: Record<string, unknown>, includeOtherStreams: boolean) => number;
  emitRaw: (runId: string, data: string) => number;
  states: () => Array<{ readyState: number; url: string }>;
};

async function installControlledRunEventSource(page: Page) {
  await page.addInitScript(() => {
    const sources: ControlledEventSource[] = [];

    class ControlledEventSource {
      static readonly OPEN = 1;
      static readonly CLOSED = 2;
      readonly url: string;
      readonly withCredentials: boolean;
      readyState = ControlledEventSource.OPEN;
      private listeners = new Map<string, EventListenerOrEventListenerObject[]>();

      constructor(url: string | URL, init?: EventSourceInit) {
        this.url = String(url);
        this.withCredentials = init?.withCredentials ?? false;
        sources.push(this);
      }

      addEventListener(type: string, listener: EventListenerOrEventListenerObject | null) {
        if (!listener) return;
        this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
      }

      close() {
        this.readyState = ControlledEventSource.CLOSED;
      }

      emit(type: string, data: string) {
        const event = new MessageEvent(type, { data });
        for (const listener of this.listeners.get(type) ?? []) {
          if (typeof listener === 'function') listener.call(window, event);
          else listener.handleEvent(event);
        }
      }
    }

    const fixture: ControlledRunEventSourceFixture = {
      emit(runId, event, includeOtherStreams) {
        const matchingPath = `/api/runs/${runId}/events/stream`;
        const targets = sources.filter((source) => includeOtherStreams || source.url === matchingPath);
        const data = JSON.stringify(event);
        for (const source of targets) source.emit('run_event', data);
        return targets.length;
      },
      emitRaw(runId, data) {
        const matchingPath = `/api/runs/${runId}/events/stream`;
        const targets = sources.filter((source) => source.url === matchingPath);
        for (const source of targets) source.emit('run_event', data);
        return targets.length;
      },
      states: () => sources.map(({ readyState, url }) => ({ readyState, url }))
    };
    Object.defineProperty(window, 'EventSource', { configurable: true, value: ControlledEventSource });
    Object.defineProperty(window, '__runEventSourceFixture', { configurable: true, value: fixture });
  });
}

function emitMalformedControlledRunEvent(page: Page, runId: string, data: string) {
  return page.evaluate(({ id, raw }) => {
    const fixture = (window as unknown as { __runEventSourceFixture: ControlledRunEventSourceFixture }).__runEventSourceFixture;
    window.setTimeout(() => fixture.emitRaw(id, raw), 0);
  }, { id: runId, raw: data });
}

function controlledRunEventSourceStates(page: Page) {
  return page.evaluate(() => {
    const fixture = (window as unknown as { __runEventSourceFixture: ControlledRunEventSourceFixture }).__runEventSourceFixture;
    return fixture.states();
  });
}

function emitControlledRunEvent(page: Page, runId: string, event: Record<string, unknown>, includeOtherStreams = false) {
  return page.evaluate(({ event: browserEvent, id, includeOther }) => {
    const fixture = (window as unknown as { __runEventSourceFixture: ControlledRunEventSourceFixture }).__runEventSourceFixture;
    return fixture.emit(id, browserEvent, includeOther);
  }, { event, id: runId, includeOther: includeOtherStreams });
}

function waitForBrowserFrame(page: Page) {
  return page.evaluate(() => new Promise<void>((resolve) => window.requestAnimationFrame(() => resolve())));
}

const emptyModelOptions = { items: [], system_default_model_connection_id: null };

type AgentConfiguration = {
  id: string;
  owner_id: string;
  name: string;
  instructions: string;
  visibility: string;
  public_to: string[];
  runtime_id: string | null;
  default_model_connection_id: string | null;
  reasoning_effort: string;
  codex_subagents: unknown[];
  sandbox_policy: Record<string, unknown>;
  managed_skill_ids: string[];
  mcp_allowlist: unknown[];
};

function createAgentRequest(name: string, instructions: string) {
  return {
    name,
    instructions,
    visibility: 'private',
    public_to: [],
    default_model_connection_id: null,
    reasoning_effort: 'default',
    codex_subagents: []
  };
}

function updateAgentRequest(agent: AgentConfiguration, changes: Partial<AgentConfiguration> = {}) {
  const updated = { ...agent, ...changes };
  return {
    name: updated.name,
    instructions: updated.instructions,
    visibility: updated.visibility,
    public_to: updated.public_to,
    runtime_id: updated.runtime_id,
    default_model_connection_id: updated.default_model_connection_id,
    reasoning_effort: updated.reasoning_effort,
    codex_subagents: updated.codex_subagents,
    sandbox_policy: updated.sandbox_policy,
    managed_skill_ids: updated.managed_skill_ids,
    mcp_allowlist: updated.mcp_allowlist
  };
}

function consoleAgentFixture(agentId: string, ownerId: string, now: string) {
  return {
    id: agentId,
    name: 'Run concurrency agent',
    instructions: 'Exercise deterministic run concurrency.',
    visibility: 'private',
    public_to: [],
    runtime_id: null,
    owner_id: ownerId,
    is_owner: false,
    can_manage: false,
    can_administer: false,
    can_invoke: true,
    default_model_connection_id: null,
    reasoning_effort: 'default',
    codex_subagents: [],
    model_policy: {},
    sandbox_policy: {},
    managed_skill_ids: [],
    mcp_allowlist: [],
    created_at: now,
    updated_at: now
  };
}

function runFixture(runId: string, agentId: string, now: string, status: string, initialMessage: string) {
  return {
    id: runId,
    agent_id: agentId,
    automation_id: null,
    integration_session_id: null,
    parent_run_id: null,
    runtime_id: null,
    hub_session_id: null,
    hub_message_id: null,
    hub_turn_id: null,
    session_ownership_generation: null,
    status,
    initial_message: initialMessage,
    session_id: null,
    work_dir_ref: null,
    source: 'console',
    created_at: now,
    updated_at: now
  };
}

function navigateWithinSpa(page: import('@playwright/test').Page, agentId: string) {
  return page.evaluate((id) => {
    window.history.pushState(null, '', `/agents/${id}`);
    window.dispatchEvent(new PopStateEvent('popstate'));
  }, agentId);
}

async function deleteAgentsForCleanup(page: import('@playwright/test').Page, agentIds: Array<string | null>) {
  const results = await Promise.allSettled(agentIds.filter((agentId): agentId is string => Boolean(agentId)).map(async (agentId) => {
    return page.request.delete(`/api/agents/${agentId}`);
  }));
  for (const result of results) {
    if (result.status === 'rejected') throw result.reason;
    expect([204, 404]).toContain(result.value.status());
  }
}

async function createAgentThroughUi(page: Page, name: string, instructions: string, visibility = 'private', sharedWith?: RegExp) {
  await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await dialog.getByLabel('Instructions').fill(instructions);
  await expect(dialog.getByLabel('Default model connection')).not.toHaveValue('');
  await dialog.getByLabel('Visibility').selectOption(visibility);
  if (sharedWith) await dialog.getByRole('checkbox', { name: sharedWith }).check();
  const responsePromise = page.waitForResponse((response) => response.request().method() === 'POST'
    && new URL(response.url()).pathname === '/api/agents');
  await dialog.getByRole('button', { name: 'Create agent' }).click();
  return responsePromise;
}

test('owner, admin, public, and public_to permissions stay isolated', async ({ browser, baseURL }) => {
  const memberEmail = `member-${Date.now()}@example.com`;
  const memberContext = await browser.newContext({ baseURL });
  const memberPage = await memberContext.newPage();
  const adminContext = await browser.newContext({ baseURL });
  const adminPage = await adminContext.newPage();
  const publicAdminContext = await browser.newContext({ baseURL });
  const publicAdminPage = await publicAdminContext.newPage();
  let outsiderContext: import('@playwright/test').BrowserContext | null = null;
  let memberPrivateId: string | null = null;
  let publicAgentId: string | null = null;
  let sharedAgentId: string | null = null;
  try {
    await signInWithMockOidc(memberPage, memberEmail);
    const privateResponse = await memberPage.request.post('/api/agents', {
      data: createAgentRequest(`Member Private ${Date.now()}`, 'Owner-only private controls.')
    });
    expect(privateResponse.ok()).toBeTruthy();
    let memberPrivate = await privateResponse.json() as AgentConfiguration;
    memberPrivateId = memberPrivate.id;
    const memberRuntimes = await (await memberPage.request.get('/api/runtimes')).json();
    const configuredPrivateResponse = await memberPage.request.patch(`/api/agents/${memberPrivate.id}`, {
      data: updateAgentRequest(memberPrivate, {
        runtime_id: memberRuntimes[0]?.id ?? null,
        reasoning_effort: 'high',
        sandbox_policy: { mode: 'workspace-write', network_access: true, private_marker: 'sandbox-secret' },
        mcp_allowlist: [{ name: 'private-mcp', command: 'private-command' }]
      })
    });
    expect(configuredPrivateResponse.ok()).toBeTruthy();
    memberPrivate = await configuredPrivateResponse.json();
    memberPrivateId = memberPrivate.id;

    await adminPage.goto('/login');
    await adminPage.getByLabel('Email').fill('admin@example.com');
    await adminPage.getByLabel('Password').fill('admin123');
    await adminPage.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(adminPage.getByText('admin@example.com')).toBeVisible();

    await signInWithMockOidc(publicAdminPage, `public-admin-${Date.now()}@example.com`);
    const publicAdminUser = await (await publicAdminPage.request.get('/api/auth/me')).json() as { id: string };
    const promoteResponse = await adminPage.request.put(`/api/admin/users/${publicAdminUser.id}/role`, {
      data: { role: 'admin' }
    });
    expect(promoteResponse.ok()).toBeTruthy();

    await adminPage.goto(`/agents/${memberPrivate.id}`);
    await expect(adminPage.getByRole('heading', { name: memberPrivate.name, level: 1 })).toBeVisible();
    await expect(adminPage.getByRole('button', { name: 'Delete agent' })).toBeVisible();
    await expect(adminPage.getByRole('button', { name: 'Start run' })).toHaveCount(0);
    await adminPage.getByRole('tab', { name: 'Instructions' }).click();
    await expect(adminPage.getByLabel('Name', { exact: true })).toBeVisible();
    const adminPrivateView = await (await adminPage.request.get(`/api/agents/${memberPrivate.id}`)).json() as AgentConfiguration & {
      can_administer: boolean;
      can_manage: boolean;
      can_invoke: boolean;
    };
    expect(adminPrivateView.can_administer).toBe(true);
    expect(adminPrivateView.can_manage).toBe(true);
    expect(adminPrivateView.can_invoke).toBe(false);
    expect(adminPrivateView.runtime_id).toBe(memberRuntimes[0]?.id ?? null);
    expect(adminPrivateView.default_model_connection_id).toBe(memberPrivate.default_model_connection_id);
    expect(adminPrivateView.reasoning_effort).toBe('high');
    expect(adminPrivateView.codex_subagents).toEqual([]);
    expect(adminPrivateView.sandbox_policy).toMatchObject({ private_marker: 'sandbox-secret' });
    expect(adminPrivateView.managed_skill_ids).toEqual([]);
    expect(adminPrivateView.mcp_allowlist).toEqual([{ name: 'private-mcp', command: 'private-command' }]);
    const forbiddenPrivateRun = await adminPage.request.post(`/api/agents/${memberPrivate.id}/runs`, {
      data: { message: 'Admin must not borrow private connections.', hub_session_id: null, parent_run_id: null }
    });
    expect(forbiddenPrivateRun.status()).toBe(404);

    await publicAdminPage.goto('/agents');
    const publicName = `Admin Public ${Date.now()}`;
    const publicResponse = await createAgentThroughUi(
      publicAdminPage,
      publicName,
      'Public invocation without control-plane disclosure.',
      'public'
    );
    expect(publicResponse.ok()).toBeTruthy();
    const publicAgent = await publicResponse.json() as AgentConfiguration;
    expect(publicAgent.visibility).toBe('public');
    expect(publicAgent.owner_id).toBe(publicAdminUser.id);
    publicAgentId = publicAgent.id;
    await expect(publicAdminPage).toHaveURL(/\/agents\/[0-9a-f-]{36}$/);

    await publicAdminPage.goto('/agents');
    const sharedName = `Admin Shared ${Date.now()}`;
    const sharedResponse = await createAgentThroughUi(
      publicAdminPage,
      sharedName,
      'Shared with one member.',
      'public_to',
      new RegExp(memberEmail)
    );
    expect(sharedResponse.ok()).toBeTruthy();
    const sharedAgent = await sharedResponse.json() as AgentConfiguration;
    expect(sharedAgent.visibility).toBe('public_to');
    sharedAgentId = sharedAgent.id;
    await expect(publicAdminPage).toHaveURL(/\/agents\/[0-9a-f-]{36}$/);

    await memberPage.goto(`/agents/${publicAgentId}`);
    await expect(memberPage.getByRole('button', { name: 'Start run' })).toBeVisible();
    await expect(memberPage.getByLabel('Name', { exact: true })).toHaveCount(0);
    const publicView = await (await memberPage.request.get(`/api/agents/${publicAgentId}`)).json() as AgentConfiguration & {
      can_invoke: boolean;
      can_manage: boolean;
    };
    expect(publicView.can_invoke).toBe(true);
    expect(publicView.can_manage).toBe(false);
    expect(publicView.default_model_connection_id).toBeNull();
    expect(publicView.reasoning_effort).toBe('default');
    expect(publicView.codex_subagents).toEqual([]);
    expect(publicView.sandbox_policy).toEqual({});
    expect(publicView.managed_skill_ids).toEqual([]);
    expect(publicView.mcp_allowlist).toEqual([]);
    const forbiddenPatch = await memberPage.request.patch(`/api/agents/${publicAgentId}`, {
      data: updateAgentRequest(publicView, { name: 'Unauthorized rename' })
    });
    expect(forbiddenPatch.status()).toBe(404);
    await memberPage.getByLabel('Message').fill('Public Agent run from member');
    const publicRunResponse = memberPage.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === `/api/agents/${publicAgentId}/runs`);
    await memberPage.getByRole('button', { name: 'Start run' }).click();
    const publicRun = await publicRunResponse;
    expect(publicRun.ok()).toBeTruthy();
    expect((await publicRun.json() as { hub_session_id: string | null }).hub_session_id).toBeTruthy();
    await expect(memberPage.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });

    await memberPage.goto(`/agents/${sharedAgentId}`);
    await expect(memberPage.getByRole('heading', { name: sharedName, level: 1 })).toBeVisible();
    await expect(memberPage.getByRole('button', { name: 'Start run' })).toBeVisible();
    await expect(memberPage.getByRole('checkbox', { name: 'Continue selected thread' })).toBeDisabled();
    await expect(memberPage.getByText('Public Agent run from member')).toHaveCount(0);
    await memberPage.getByLabel('Message').fill('Shared Agent run from selected member');
    const sharedRunResponse = memberPage.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === `/api/agents/${sharedAgentId}/runs`);
    await memberPage.getByRole('button', { name: 'Start run' }).click();
    const sharedRun = await sharedRunResponse;
    expect(sharedRun.ok()).toBeTruthy();
    expect((await sharedRun.json() as { hub_session_id: string | null }).hub_session_id).toBeTruthy();
    await expect(memberPage.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });
    await expect(memberPage.getByRole('checkbox', { name: 'Continue selected thread' })).toBeEnabled();

    outsiderContext = await browser.newContext({ baseURL });
    const outsiderPage = await outsiderContext.newPage();
    await signInWithMockOidc(outsiderPage, `outsider-${Date.now()}@example.com`);
    expect((await outsiderPage.request.get(`/api/agents/${sharedAgentId}`)).status()).toBe(404);
    expect((await outsiderPage.request.get(`/api/agents/${memberPrivate.id}`)).status()).toBe(404);
    expect((await outsiderPage.request.get(`/api/agents/${publicAgentId}`)).ok()).toBeTruthy();
  } finally {
    // 每条失败路径都归档本用例已创建资源，避免 interval/并发环境中的后续用例受污染。
    try {
      const cleanupResults = await Promise.allSettled([
        deleteAgentsForCleanup(memberPage, [memberPrivateId]),
        deleteAgentsForCleanup(adminPage, [publicAgentId, sharedAgentId])
      ]);
      const cleanupErrors = cleanupResults.flatMap((result) => result.status === 'rejected' ? [result.reason] : []);
      if (cleanupErrors.length > 0) throw new AggregateError(cleanupErrors, 'Failed to delete permission test agents');
    } finally {
      await outsiderContext?.close();
      await publicAdminContext.close();
      await adminContext.close();
      await memberContext.close();
    }
  }
});

test('agent navigation ignores a stale detail response', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  let firstAgent: { id: string; instructions: string; name: string } | null = null;
  let secondAgent: { id: string; instructions: string; name: string } | null = null;
  let firstDetailRoute: ((route: Route) => Promise<void>) | null = null;
  let patchRoute: ((route: Route) => Promise<void>) | null = null;
  const releaseFirst = deferred();
  try {
    const first = await page.request.post('/api/agents', {
      data: createAgentRequest(`Stale source ${Date.now()}`, 'Delayed source.')
    });
    expect(first.ok()).toBeTruthy();
    const sourceAgent = await first.json() as { id: string; instructions: string; name: string };
    firstAgent = sourceAgent;

    const second = await page.request.post('/api/agents', {
      data: createAgentRequest(`Stale target ${Date.now()}`, 'Current target.')
    });
    expect(second.ok()).toBeTruthy();
    const targetAgent = await second.json() as { id: string; instructions: string; name: string };
    secondAgent = targetAgent;

    const firstIntercepted = deferred();
    let sourceDetailRequests = 0;
    firstDetailRoute = async (route) => {
      // 明确记录 A 详情请求已被暂停，随后才在同一 SPA 文档内切换到 B。
      sourceDetailRequests += 1;
      firstIntercepted.resolve();
      const response = await route.fetch();
      await releaseFirst.promise;
      await route.fulfill({ response }).catch((error) => {
        if (!String(error).includes('Route is already handled')) throw error;
      });
    };
    await page.route(`**/api/agents/${sourceAgent.id}`, firstDetailRoute);

    await page.goto('/agents');
    const documentToken = `batch1-${Date.now()}`;
    await page.locator('html').evaluate((element, token) => {
      element.setAttribute('data-batch1-document-token', token);
    }, documentToken);
    await navigateWithinSpa(page, sourceAgent.id);
    await firstIntercepted.promise;

    // A 的详情仍被暂停时，在同一 document 内进入 B；释放后才等待 A 的真实响应。
    await navigateWithinSpa(page, targetAgent.id);
    await expect(page.locator('html')).toHaveAttribute('data-batch1-document-token', documentToken);
    await expect(page.getByRole('heading', { name: targetAgent.name, level: 1 })).toBeVisible();
    releaseFirst.resolve();
    await expect.poll(() => sourceDetailRequests).toBe(1);
    expect(sourceDetailRequests).toBe(1);
    await expect(page.getByRole('heading', { name: targetAgent.name, level: 1 })).toBeVisible();
    await expect(page.getByText(sourceAgent.name)).toHaveCount(0);

    const patchTargets: string[] = [];
    patchRoute = async (route) => {
      const request = route.request();
      if (request.method() === 'PATCH') patchTargets.push(new URL(request.url()).pathname);
      await route.continue();
    };
    await page.route('**/api/agents/*', patchRoute);
    const renamedTarget = `Stale target renamed ${Date.now()}`;
    await page.getByRole('tab', { name: 'Instructions' }).click();
    const saveResponse = page.waitForResponse((response) => response.request().method() === 'PATCH'
      && new URL(response.url()).pathname === `/api/agents/${targetAgent.id}`);
    await page.getByLabel('Name', { exact: true }).fill(renamedTarget);
    await page.getByRole('button', { name: 'Save agent' }).click();
    const response = await saveResponse;
    expect(response.ok()).toBeTruthy();
    await expect(page.getByRole('heading', { name: renamedTarget, level: 1 })).toBeVisible();
    expect(patchTargets).toEqual([`/api/agents/${targetAgent.id}`]);

    const firstAfterSave = await (await page.request.get(`/api/agents/${sourceAgent.id}`)).json();
    const secondAfterSave = await (await page.request.get(`/api/agents/${targetAgent.id}`)).json();
    expect(firstAfterSave.name).toBe(sourceAgent.name);
    expect(secondAfterSave.name).toBe(renamedTarget);
    expect(secondAfterSave.instructions).toBe(targetAgent.instructions);
  } finally {
    releaseFirst.resolve();
    try {
      await Promise.allSettled([
        patchRoute ? page.unroute('**/api/agents/*', patchRoute) : Promise.resolve(),
        firstDetailRoute && firstAgent ? page.unroute(`**/api/agents/${firstAgent.id}`, firstDetailRoute) : Promise.resolve()
      ]);
    } finally {
      await deleteAgentsForCleanup(page, [firstAgent?.id ?? null, secondAgent?.id ?? null]);
    }
  }
});

test('agent initial load failure is handled and recovers on retry', async ({ page }) => {
  const agentId = '30000000-0000-0000-0000-000000000031';
  const now = new Date().toISOString();
  const user = {
    id: '30000000-0000-0000-0000-000000000032',
    email: 'agent-load-error@example.com',
    display_name: 'Agent load error tester',
    role: 'member'
  };
  const agent = {
    ...consoleAgentFixture(agentId, user.id, now),
    name: `Recovered Agent ${Date.now()}`,
    is_owner: true,
    can_manage: true,
    can_administer: true,
    can_invoke: true,
    created_at: now,
    updated_at: now
  };
  const pageErrors: string[] = [];
  let runtimeRequests = 0;
  page.on('pageerror', (error) => pageErrors.push(error.message));
  await page.addInitScript(() => {
    const rejections: string[] = [];
    Object.defineProperty(window, '__agentLoadRejections', { configurable: true, value: rejections });
    window.addEventListener('unhandledrejection', (event) => {
      rejections.push(event.reason instanceof Error ? event.reason.message : String(event.reason));
    });
  });
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/auth/me') {
      await route.fulfill({ json: user });
    } else if (path === `/api/agents/${agentId}`) {
      await route.fulfill({ json: agent });
    } else if (path === `/api/agents/${agentId}/runs`) {
      await route.fulfill({ json: [] });
    } else if (path === `/api/agents/${agentId}/model-options`) {
      await route.fulfill({ json: emptyModelOptions });
    } else if (path === '/api/runtimes') {
      runtimeRequests += 1;
      if (runtimeRequests === 1) {
        await route.fulfill({ status: 500, json: { error: 'sensitive upstream detail' } });
      } else {
        await route.fulfill({ json: [] });
      }
    } else if (path === '/api/skills' || path === '/api/users') {
      await route.fulfill({ json: [] });
    } else if (path === `/api/agents/${agentId}/oauth-app`) {
      await route.fulfill({ status: 404, json: { error: 'not found' } });
    } else {
      await route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
    }
  });

  await page.goto(`/agents/${agentId}`);
  await expect(page.getByText('Unable to load agent. Try again.', { exact: true })).toBeVisible();
  await expect(page.getByText('sensitive upstream detail')).toHaveCount(0);
  await page.getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByRole('heading', { name: agent.name, level: 1 })).toBeVisible();
  await expect(page.getByText('Unable to load agent. Try again.', { exact: true })).toHaveCount(0);
  await waitForBrowserFrame(page);

  expect(pageErrors).toEqual([]);
  expect(await page.evaluate(() => (window as unknown as { __agentLoadRejections: string[] }).__agentLoadRejections)).toEqual([]);
});

test('agent route change hides stale controls and runs while the next agent loads', async ({ page }) => {
  await page.goto('/login');
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('admin@example.com')).toBeVisible();

  let firstAgent: { id: string; name: string } | null = null;
  let secondAgent: { id: string; name: string } | null = null;
  let secondDetailRoute: ((route: Route) => Promise<void>) | null = null;
  let secondDetailRouteTask: Promise<void> | null = null;
  const releaseSecond = deferred();
  try {
    const first = await page.request.post('/api/agents', {
      data: createAgentRequest(`Stale UI source ${Date.now()}`, 'Visible only before navigation.')
    });
    expect(first.ok()).toBeTruthy();
    const sourceAgent = await first.json() as { id: string; name: string };
    firstAgent = sourceAgent;

    const second = await page.request.post('/api/agents', {
      data: createAgentRequest(`Stale UI target ${Date.now()}`, 'Navigation target.')
    });
    expect(second.ok()).toBeTruthy();
    const targetAgent = await second.json() as { id: string; name: string };
    secondAgent = targetAgent;

    const runMessage = `Old run must not stay actionable ${Date.now()}`;
    const run = await page.request.post(`/api/agents/${sourceAgent.id}/runs`, {
      data: { message: runMessage, hub_session_id: null, parent_run_id: null }
    });
    expect(run.ok()).toBeTruthy();

    await page.goto(`/agents/${sourceAgent.id}`);
    await expect(page.getByRole('heading', { name: sourceAgent.name, level: 1 })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Start run' })).toBeVisible();
    await expect(page.getByText(runMessage).first()).toBeVisible();
    const documentToken = `batch1-${Date.now()}`;
    await page.locator('html').evaluate((element, token) => {
      element.setAttribute('data-batch1-document-token', token);
    }, documentToken);

    const secondIntercepted = deferred();
    secondDetailRoute = async (route) => {
      // 先确认 B 的详情请求进入拦截器，避免加载态断言发生在请求尚未开始时。
      secondIntercepted.resolve();
      secondDetailRouteTask = (async () => {
        await releaseSecond.promise;
        await route.continue();
      })();
      await secondDetailRouteTask;
    };
    await page.route(`**/api/agents/${targetAgent.id}`, secondDetailRoute, { times: 1 });

    // 用 SPA 路由切换复现同一组件实例从 A 切到 B 时的旧状态暂留窗口。
    const delayedSecondResponse = page.waitForResponse((response) => response.request().method() === 'GET'
      && new URL(response.url()).pathname === `/api/agents/${targetAgent.id}`);
    await navigateWithinSpa(page, targetAgent.id);
    await secondIntercepted.promise;
    await expect(page.locator('html')).toHaveAttribute('data-batch1-document-token', documentToken);
    await expect(page.getByText('Loading...', { exact: true })).toBeVisible();
    await expect(page.getByText(sourceAgent.name)).toHaveCount(0);
    await expect(page.getByText(runMessage)).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Save agent' })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Start run' })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Delete agent', exact: true })).toHaveCount(0);
    await expect(page.getByLabel('Name', { exact: true })).toHaveCount(0);
    await expect(page.getByLabel('Message', { exact: true })).toHaveCount(0);

    releaseSecond.resolve();
    const secondResponse = await delayedSecondResponse;
    expect(secondResponse.ok()).toBeTruthy();
    await expect(page.getByRole('heading', { name: targetAgent.name, level: 1 })).toBeVisible();
  } finally {
    releaseSecond.resolve();
    try {
      if (secondDetailRouteTask) await secondDetailRouteTask;
      await Promise.allSettled([
        secondDetailRoute && secondAgent ? page.unroute(`**/api/agents/${secondAgent.id}`, secondDetailRoute) : Promise.resolve()
      ]);
    } finally {
      await deleteAgentsForCleanup(page, [firstAgent?.id ?? null, secondAgent?.id ?? null]);
    }
  }
});

test('run switch ignores delayed history and SSE from the previous run', async ({ page }) => {
  const agentId = '10000000-0000-0000-0000-000000000001';
  const runAId = '20000000-0000-0000-0000-000000000001';
  const runBId = '20000000-0000-0000-0000-000000000002';
  const now = new Date().toISOString();
  const transcriptA = 'Delayed transcript from run A';
  const transcriptB = 'Current transcript from run B';
  const lateRunAStatus = 'late-a-failed';
  const releaseAHistory = deferred();
  const releaseBHistory = deferred();
  const aHistoryStarted = deferred();
  const bHistoryStarted = deferred();
  let runListRequests = 0;
  let aHistoryRequests = 0;
  const user = {
    id: '30000000-0000-0000-0000-000000000001',
    email: 'run-switch@example.com',
    display_name: 'Run switch tester',
    role: 'member'
  };
  const agent = {
    ...consoleAgentFixture(agentId, user.id, now),
    name: 'Run switch agent',
    instructions: 'Exercise run transcript switching.'
  };
  const runA = runFixture(runAId, agentId, now, 'running', 'Run A');
  const runB = { ...runA, id: runBId, status: 'completed', initial_message: 'Run B' };
  const eventA = {
    seq: 1,
    run_id: runAId,
    event_type: 'message',
    role: 'assistant',
    content: transcriptA,
    payload: {},
    created_at: now
  };
  const eventB = { ...eventA, run_id: runBId, content: transcriptB };
  const lateEventA = {
    ...eventA,
    seq: 99,
    event_type: 'status',
    role: null,
    content: lateRunAStatus,
    payload: { status: lateRunAStatus }
  };

  try {
    await installControlledRunEventSource(page);

    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path === '/api/auth/me') {
        await route.fulfill({ json: user });
      } else if (path === `/api/agents/${agentId}`) {
        await route.fulfill({ json: agent });
      } else if (path === `/api/agents/${agentId}/runs`) {
        runListRequests += 1;
        await route.fulfill({ json: [{ ...runA, status: runListRequests === 1 ? 'running' : 'completed' }, runB] });
      } else if (path === `/api/agents/${agentId}/model-options`) {
        await route.fulfill({ json: emptyModelOptions });
      } else if (path === '/api/runtimes' || path === '/api/skills' || path === '/api/users') {
        await route.fulfill({ json: [] });
      } else if (path === `/api/runs/${runAId}/events`) {
        aHistoryRequests += 1;
        aHistoryStarted.resolve();
        await releaseAHistory.promise;
        await route.fulfill({ json: [eventA] });
      } else if (path === `/api/runs/${runBId}/events`) {
        bHistoryStarted.resolve();
        await releaseBHistory.promise;
        await route.fulfill({ json: [eventB] });
      } else {
        await route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
      }
    });

    await page.goto(`/agents/${agentId}`);
    await aHistoryStarted.promise;
    await expect.poll(async () => (await controlledRunEventSourceStates(page))
      .filter((source) => source.url === `/api/runs/${runAId}/events/stream`).length).toBe(1);
    expect(await emitControlledRunEvent(page, runAId, eventA)).toBe(1);
    await expect(page.getByText(transcriptA, { exact: true })).toBeVisible();
    await expect(page.locator('.console-header .status')).toHaveText('running');
    await expect(page.locator('.console-header .status')).toHaveText('completed', { timeout: 5_000 });
    expect(aHistoryRequests).toBe(1);
    expect((await controlledRunEventSourceStates(page))
      .filter((source) => source.url === `/api/runs/${runAId}/events/stream`).length).toBe(1);

    await page.locator(`[data-run-id="${runBId}"]`).click();
    await bHistoryStarted.promise;
    await expect(page.getByText(transcriptA, { exact: true })).toHaveCount(0, { timeout: 1_000 });

    releaseBHistory.resolve();
    await expect(page.getByText(transcriptB, { exact: true })).toBeVisible();
    await expect(page.locator('.console-header .status')).toHaveText('completed');
    expect(await controlledRunEventSourceStates(page)).toEqual([
      { readyState: 2, url: `/api/runs/${runAId}/events/stream` },
      { readyState: 1, url: `/api/runs/${runBId}/events/stream` }
    ]);

    // 同时投递给已关闭的 A source 和活跃的 B source，分别覆盖 active 与 run_id 防护。
    expect(await emitControlledRunEvent(page, runAId, lateEventA, true)).toBe(2);
    await waitForBrowserFrame(page);
    await expect(page.getByText(transcriptB, { exact: true })).toBeVisible();
    await expect(page.locator('.console-header .status')).toHaveText('completed');
    await expect(page.locator('.console').getByText(lateRunAStatus, { exact: true })).toHaveCount(0);
    await expect(page.getByText(transcriptA, { exact: true })).toHaveCount(0);

    releaseAHistory.resolve();
    await waitForBrowserFrame(page);
    await expect(page.getByText(transcriptA, { exact: true })).toHaveCount(0);
    await expect(page.getByText(transcriptB, { exact: true })).toBeVisible();
  } finally {
    releaseAHistory.resolve();
    releaseBHistory.resolve();
  }
});

test('a stale run-list poll cannot replace a newly created run or continuation parent', async ({ page }) => {
  const agentId = '40000000-0000-0000-0000-000000000001';
  const oldRunId = '41000000-0000-0000-0000-000000000001';
  const newRunId = '41000000-0000-0000-0000-000000000002';
  const continuedRunId = '41000000-0000-0000-0000-000000000003';
  const hubSessionId = '43000000-0000-0000-0000-000000000001';
  const now = new Date().toISOString();
  const user = {
    id: '42000000-0000-0000-0000-000000000001',
    email: 'run-list-race@example.com',
    display_name: 'Run list race tester',
    role: 'member'
  };
  const agent = consoleAgentFixture(agentId, user.id, now);
  const oldRun = runFixture(oldRunId, agentId, now, 'completed', 'Existing run');
  const newRun = {
    ...runFixture(newRunId, agentId, now, 'completed', 'New run survives stale poll'),
    hub_session_id: hubSessionId
  };
  const continuedRun = {
    ...runFixture(continuedRunId, agentId, now, 'running', 'Continue the new run'),
    hub_session_id: hubSessionId,
    parent_run_id: newRunId
  };
  const stalePollStarted = deferred();
  const firstCreateStarted = deferred();
  const createBodies: Array<{
    message: string;
    hub_session_id: string | null;
    parent_run_id: string | null;
  }> = [];
  let runListRequests = 0;
  const heldRoutes: { firstCreate?: Route; stalePoll?: Route } = {};

  try {
    await page.clock.install();
    await installControlledRunEventSource(page);
    await page.route('**/api/**', async (route) => {
      const request = route.request();
      const path = new URL(request.url()).pathname;
      if (path === '/api/auth/me') {
        await route.fulfill({ json: user });
      } else if (path === `/api/agents/${agentId}`) {
        await route.fulfill({ json: agent });
      } else if (path === `/api/agents/${agentId}/runs` && request.method() === 'GET') {
        runListRequests += 1;
        if (runListRequests === 2) {
          heldRoutes.stalePoll = route;
          stalePollStarted.resolve();
          return;
        }
        await route.fulfill({ json: createBodies.length >= 2
          ? [continuedRun, newRun, oldRun]
          : createBodies.length === 1 ? [newRun, oldRun] : [oldRun] });
      } else if (path === `/api/agents/${agentId}/runs` && request.method() === 'POST') {
        createBodies.push(request.postDataJSON() as {
          message: string;
          hub_session_id: string | null;
          parent_run_id: string | null;
        });
        if (createBodies.length === 1) {
          heldRoutes.firstCreate = route;
          firstCreateStarted.resolve();
          return;
        }
        await route.fulfill({ json: continuedRun });
      } else if (path === `/api/agents/${agentId}/model-options`) {
        await route.fulfill({ json: emptyModelOptions });
      } else if (path === '/api/runtimes' || path === '/api/skills' || path === '/api/users') {
        await route.fulfill({ json: [] });
      } else if (/^\/api\/runs\/[^/]+\/events$/.test(path)) {
        await route.fulfill({ json: [] });
      } else {
        await route.fulfill({ status: 404, json: { error: `Unhandled test route: ${request.method()} ${path}` } });
      }
    });

    await page.goto(`/agents/${agentId}`);
    await expect(page.locator(`[data-run-id="${oldRunId}"]`)).toHaveClass(/selected/);
    await page.evaluate((message) => {
      const textarea = document.querySelector<HTMLTextAreaElement>('label textarea');
      if (!textarea) throw new Error('run message textarea not found');
      const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')?.set;
      setter?.call(textarea, message);
      textarea.dispatchEvent(new Event('input', { bubbles: true }));
    }, newRun.initial_message);
    await page.evaluate(() => {
      const form = document.querySelector<HTMLTextAreaElement>('label textarea')?.closest('form');
      if (!form) throw new Error('run form not found');
      form.requestSubmit();
    });
    await firstCreateStarted.promise;
    await page.clock.fastForward(2_000);
    await stalePollStarted.promise;

    const delayedCreateRoute = heldRoutes.firstCreate;
    if (!delayedCreateRoute) throw new Error('first run create was not captured');
    await delayedCreateRoute.fulfill({ json: newRun });
    delete heldRoutes.firstCreate;
    await expect(page.locator(`[data-run-id="${newRunId}"]`)).toHaveClass(/selected/);

    const delayedRoute = heldRoutes.stalePoll;
    if (!delayedRoute) throw new Error('stale run-list poll was not captured');
    const stalePollResponse = page.waitForResponse((response) => response.request().method() === 'GET'
      && new URL(response.url()).pathname === `/api/agents/${agentId}/runs`);
    await delayedRoute.fulfill({ json: [oldRun] });
    delete heldRoutes.stalePoll;
    const response = await stalePollResponse;
    await response.finished();
    await page.clock.resume();
    await waitForBrowserFrame(page);

    await expect(page.locator(`[data-run-id="${newRunId}"]`)).toHaveCount(1);
    await expect(page.locator(`[data-run-id="${newRunId}"]`)).toHaveClass(/selected/);
    await page.evaluate((message) => {
      const checkbox = document.querySelector<HTMLInputElement>('input[type="checkbox"]');
      const textarea = document.querySelector<HTMLTextAreaElement>('label textarea');
      if (!checkbox || !textarea) throw new Error('run continuation controls not found');
      if (!checkbox.checked) checkbox.click();
      const setter = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')?.set;
      setter?.call(textarea, message);
      textarea.dispatchEvent(new Event('input', { bubbles: true }));
    }, continuedRun.initial_message);
    await page.evaluate(() => {
      const form = document.querySelector<HTMLTextAreaElement>('label textarea')?.closest('form');
      if (!form) throw new Error('run form not found');
      form.requestSubmit();
    });
    await expect(page.locator(`[data-run-id="${continuedRunId}"]`)).toHaveClass(/selected/);
    expect(createBodies).toEqual([
      { message: newRun.initial_message, hub_session_id: null, parent_run_id: null },
      {
        message: continuedRun.initial_message,
        hub_session_id: hubSessionId,
        parent_run_id: newRunId
      }
    ]);
  } finally {
    await heldRoutes.firstCreate?.fulfill({ json: newRun }).catch(() => undefined);
    await heldRoutes.stalePoll?.fulfill({ json: [oldRun] }).catch(() => undefined);
  }
});

test('older run history cannot regress a newer live terminal event', async ({ page }) => {
  const agentId = '50000000-0000-0000-0000-000000000001';
  const runId = '51000000-0000-0000-0000-000000000001';
  const now = new Date().toISOString();
  const user = {
    id: '52000000-0000-0000-0000-000000000001',
    email: 'run-history-race@example.com',
    display_name: 'Run history race tester',
    role: 'member'
  };
  const agent = consoleAgentFixture(agentId, user.id, now);
  const run = runFixture(runId, agentId, now, 'running', 'History and SSE race');
  const historyStarted = deferred();
  const releaseHistory = deferred();
  const historyEvents = [
    {
      seq: 1,
      run_id: runId,
      event_type: 'message',
      role: 'user',
      content: 'Historical prompt',
      payload: {},
      created_at: now
    },
    {
      seq: 2,
      run_id: runId,
      event_type: 'status',
      role: null,
      content: 'running',
      payload: { status: 'running' },
      created_at: now
    }
  ];
  const liveMessage = {
    seq: 10,
    run_id: runId,
    event_type: 'message',
    role: 'assistant',
    content: 'Live terminal transcript',
    payload: {},
    created_at: now
  };
  const liveCompleted = {
    seq: 11,
    run_id: runId,
    event_type: 'status',
    role: null,
    content: 'completed',
    payload: { status: 'completed' },
    created_at: now
  };

  try {
    await installControlledRunEventSource(page);
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path === '/api/auth/me') {
        await route.fulfill({ json: user });
      } else if (path === `/api/agents/${agentId}`) {
        await route.fulfill({ json: agent });
      } else if (path === `/api/agents/${agentId}/runs`) {
        await route.fulfill({ json: [run] });
      } else if (path === `/api/agents/${agentId}/model-options`) {
        await route.fulfill({ json: emptyModelOptions });
      } else if (path === '/api/runtimes' || path === '/api/skills' || path === '/api/users') {
        await route.fulfill({ json: [] });
      } else if (path === `/api/runs/${runId}/events`) {
        historyStarted.resolve();
        await releaseHistory.promise;
        await route.fulfill({ json: historyEvents });
      } else {
        await route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
      }
    });

    await page.goto(`/agents/${agentId}`);
    await historyStarted.promise;
    await expect.poll(async () => (await controlledRunEventSourceStates(page))
      .filter((source) => source.url === `/api/runs/${runId}/events/stream`).length).toBe(1);
    expect(await emitControlledRunEvent(page, runId, liveMessage)).toBe(1);
    expect(await emitControlledRunEvent(page, runId, liveCompleted)).toBe(1);
    await expect(page.getByText(liveMessage.content, { exact: true })).toBeVisible();
    await expect(page.locator('.console-header .status')).toHaveText('completed');

    const historyResponse = page.waitForResponse((response) => new URL(response.url()).pathname === `/api/runs/${runId}/events`);
    releaseHistory.resolve();
    const response = await historyResponse;
    await response.finished();
    await waitForBrowserFrame(page);

    await expect(page.getByText(liveMessage.content, { exact: true })).toBeVisible();
    await expect(page.locator('.console-header .status')).toHaveText('completed');
  } finally {
    releaseHistory.resolve();
  }
});

test('run history rejection is handled without an unhandled promise', async ({ page }) => {
  const agentId = '60000000-0000-0000-0000-000000000001';
  const runId = '61000000-0000-0000-0000-000000000001';
  const now = new Date().toISOString();
  const user = {
    id: '62000000-0000-0000-0000-000000000001',
    email: 'run-history-error@example.com',
    display_name: 'Run history error tester',
    role: 'member'
  };
  const agent = consoleAgentFixture(agentId, user.id, now);
  const run = runFixture(runId, agentId, now, 'running', 'Rejected history');
  const pageErrors: string[] = [];
  const onPageError = (error: Error) => pageErrors.push(error.message);
  page.on('pageerror', onPageError);

  try {
    await installControlledRunEventSource(page);
    await page.addInitScript(() => {
      const rejections: string[] = [];
      Object.defineProperty(window, '__runHistoryRejections', { configurable: true, value: rejections });
      window.addEventListener('unhandledrejection', (event) => {
        rejections.push(event.reason instanceof Error ? event.reason.message : String(event.reason));
      });
    });
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname;
      if (path === '/api/auth/me') {
        await route.fulfill({ json: user });
      } else if (path === `/api/agents/${agentId}`) {
        await route.fulfill({ json: agent });
      } else if (path === `/api/agents/${agentId}/runs`) {
        await route.fulfill({ json: [run] });
      } else if (path === `/api/agents/${agentId}/model-options`) {
        await route.fulfill({ json: emptyModelOptions });
      } else if (path === '/api/runtimes' || path === '/api/skills' || path === '/api/users') {
        await route.fulfill({ json: [] });
      } else if (path === `/api/runs/${runId}/events`) {
        await route.fulfill({ status: 500, json: { error: 'history unavailable' } });
      } else {
        await route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
      }
    });

    const historyResponse = page.waitForResponse((response) => new URL(response.url()).pathname === `/api/runs/${runId}/events`);
    await page.goto(`/agents/${agentId}`);
    const response = await historyResponse;
    expect(response.status()).toBe(500);
    await response.finished();
    await waitForBrowserFrame(page);
    await waitForBrowserFrame(page);

    expect(pageErrors).toEqual([]);
    expect(await page.evaluate(() => (window as unknown as { __runHistoryRejections: string[] }).__runHistoryRejections)).toEqual([]);
    await expect(page.locator('.console-header .status')).toHaveText('running');
  } finally {
    page.off('pageerror', onPageError);
  }
});

test('RunConsole ignores malformed SSE JSON and continues processing valid events', async ({ page }) => {
  const agentId = '60000000-0000-0000-0000-000000000011';
  const runId = '61000000-0000-0000-0000-000000000011';
  const now = new Date().toISOString();
  const user = { id: '62000000-0000-0000-0000-000000000011', email: 'malformed-sse@example.com', display_name: 'Malformed SSE tester', role: 'member' };
  const agent = consoleAgentFixture(agentId, user.id, now);
  const run = runFixture(runId, agentId, now, 'running', 'Malformed SSE');
  const pageErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  await installControlledRunEventSource(page);
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/auth/me') return route.fulfill({ json: user });
    if (path === `/api/agents/${agentId}`) return route.fulfill({ json: agent });
    if (path === `/api/agents/${agentId}/runs`) return route.fulfill({ json: [run] });
    if (path === `/api/agents/${agentId}/model-options`) return route.fulfill({ json: emptyModelOptions });
    if (path === `/api/runs/${runId}/events`) return route.fulfill({ json: [] });
    if (path === '/api/runtimes' || path === '/api/skills' || path === '/api/users') return route.fulfill({ json: [] });
    return route.fulfill({ status: 404, json: { error: `Unhandled test route: ${path}` } });
  });

  await page.goto(`/agents/${agentId}`);
  await expect.poll(async () => (await controlledRunEventSourceStates(page)).length).toBe(1);
  await emitMalformedControlledRunEvent(page, runId, '{malformed');
  await waitForBrowserFrame(page);
  const validEvent = { seq: 2, run_id: runId, event_type: 'message', role: 'assistant', content: 'Valid event after malformed SSE', payload: {}, created_at: now };
  expect(await emitControlledRunEvent(page, runId, validEvent)).toBe(1);
  await expect(page.getByText(validEvent.content, { exact: true })).toBeVisible();
  expect(pageErrors).toEqual([]);
});
