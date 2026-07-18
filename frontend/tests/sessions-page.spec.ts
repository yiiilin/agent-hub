import { expect, test, type Page, type Route } from '@playwright/test';

const ownerId = '10000000-0000-4000-8000-000000000001';
const activeAgentId = '20000000-0000-4000-8000-000000000001';
const newAgentId = '20000000-0000-4000-8000-000000000002';
const now = '2026-07-17T10:00:00.000Z';

function session(id: string, agentId: string, agentName: string, origin: 'hub_native' | 'external', overrides: Record<string, unknown> = {}) {
  return {
    id,
    owner_id: ownerId,
    agent_id: agentId,
    agent_name: agentName,
    agent_deleted_at: null,
    origin: origin === 'hub_native'
      ? { kind: 'hub_native' }
      : { kind: 'external', platform_id: 'platform-one', tenant_id: 'tenant-one', external_identity_id: 'identity-one' },
    lifecycle_status: 'online',
    native_thread_id: `thread-${id}`,
    active_turn_id: null,
    history_checkpoint: 2,
    configuration_fingerprint: null,
    runtime_owner_id: 'runtime-one',
    ownership_generation: 1,
    recovery_error: null,
    current_bundle: null,
    created_at: now,
    updated_at: now,
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
    accepted_at: now,
    ...overrides
  };
}

async function installSessionApi(page: Page) {
  const active = session('active', activeAgentId, 'Active Agent', 'hub_native', { active_turn_id: 'turn-active' });
  const external = session('external', activeAgentId, 'External Agent', 'external');
  const historical = session('historical', activeAgentId, 'Deleted Agent', 'hub_native', {
    lifecycle_status: 'historical',
    agent_deleted_at: now,
    native_thread_id: null,
    runtime_owner_id: null
  });
  let sessions = [active, external, historical];
  const messages: Record<string, Array<Record<string, unknown>>> = {
    active: [
      message('active', 1, 'user', 'Inspect the deployment.', { accepted_at: '2026-07-17T10:00:00.000Z' }),
      message('active', 2, 'assistant', 'The deployment is running.', { run_id: 'run-active', accepted_at: '2026-07-17T10:00:04.000Z' })
    ],
    external: [message('external', 1, 'user', 'External request')],
    historical: [message('historical', 1, 'assistant', 'Retained answer.')]
  };
  let createBody: Record<string, unknown> | null = null;
  let steerBody: Record<string, unknown> | null = null;
  let stopCount = 0;
  const agents = [
    {
      id: activeAgentId, name: 'Active Agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null,
      owner_id: ownerId, is_owner: true, can_manage: true, can_administer: true, can_invoke: true,
      model_policy: {}, sandbox_policy: {}, managed_skill_ids: [], mcp_allowlist: [], created_at: now, updated_at: now
    },
    {
      id: newAgentId, name: 'New Agent', instructions: '', visibility: 'public', public_to: [], runtime_id: null,
      owner_id: '10000000-0000-4000-8000-000000000099', is_owner: false, can_manage: false, can_administer: false, can_invoke: true,
      model_policy: {}, sandbox_policy: {}, managed_skill_ids: [], mcp_allowlist: [], created_at: now, updated_at: now
    }
  ];

  await page.route('**/api/**', async (route: Route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: { id: ownerId, username: 'session-owner', email: 'session@example.com', display_name: 'Session owner', role: 'member' } });
    if (path === '/api/agents' && request.method() === 'GET') return route.fulfill({ json: agents });
    if (path === '/api/sessions' && request.method() === 'GET') return route.fulfill({ json: sessions });
    if (path === `/api/agents/${newAgentId}/runs` && request.method() === 'POST') {
      createBody = request.postDataJSON() as Record<string, unknown>;
      const created = session('new-session', newAgentId, 'New Agent', 'hub_native', { lifecycle_status: 'waiting_for_runtime', active_turn_id: 'turn-new', updated_at: '2026-07-17T11:00:00.000Z' });
      sessions = [created, ...sessions];
      messages['new-session'] = [message('new-session', 1, 'user', String(createBody.message), { run_id: 'run-new', delivery_state: 'queued' })];
      return route.fulfill({ json: {
        id: 'run-new', agent_id: newAgentId, automation_id: null, integration_session_id: null, hub_session_id: 'new-session', hub_message_id: messages['new-session'][0].id,
        hub_turn_id: 'turn-new', session_ownership_generation: 0, parent_run_id: null, runtime_id: null, status: 'pending', initial_message: createBody.message,
        session_id: null, work_dir_ref: null, source: 'console', created_at: now, updated_at: now
      } });
    }
    const messageMatch = path.match(/^\/api\/sessions\/([^/]+)\/messages$/);
    if (messageMatch && request.method() === 'GET') return route.fulfill({ json: messages[messageMatch[1]] ?? [] });
    if (messageMatch?.[1] === 'active' && request.method() === 'POST') {
      steerBody = request.postDataJSON() as Record<string, unknown>;
      const accepted = message('active', 3, 'user', String(steerBody.content), { delivery_mode: 'steer', delivery_state: 'delivering', run_id: 'run-active', accepted_at: '2026-07-17T10:00:07.000Z' });
      messages.active.push(accepted);
      return route.fulfill({ json: { message: accepted, run: { id: 'run-active', hub_session_id: 'active', status: 'running' } } });
    }
    const streamMatch = path.match(/^\/api\/runs\/([^/]+)\/events\/stream$/);
    if (streamMatch) {
      if (streamMatch[1] !== 'run-active') return route.fulfill({ contentType: 'text/event-stream', body: '' });
      const liveMessage = { seq: 3, run_id: 'run-active', event_type: 'message', role: 'assistant', content: 'Live assistant response.', payload: {}, created_at: '2026-07-17T10:00:05.000Z' };
      const liveTool = { seq: 4, run_id: 'run-active', event_type: 'tool_request', role: null, content: null, payload: { tool_name: 'shell' }, created_at: '2026-07-17T10:00:06.000Z' };
      return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(liveMessage)}\n\nevent: run_event\ndata: ${JSON.stringify(liveTool)}\n\n` });
    }
    const eventsMatch = path.match(/^\/api\/runs\/([^/]+)\/events$/);
    if (eventsMatch) return route.fulfill({ json: eventsMatch[1] === 'run-active' ? [
      { seq: 1, run_id: 'run-active', event_type: 'status', role: null, content: null, payload: { status: 'running' }, created_at: '2026-07-17T10:00:01.000Z' },
      { seq: 2, run_id: 'run-active', event_type: 'item', role: null, content: 'Command started', payload: {}, created_at: '2026-07-17T10:00:03.000Z' }
    ] : [] });
    if (path === '/api/runs/run-active/stop' && request.method() === 'POST') {
      stopCount += 1;
      return route.fulfill({ json: { id: 'run-active', status: 'running' } });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${request.method()} ${path}` } });
  });

  return {
    createBody: () => createBody,
    steerBody: () => steerBody,
    stopCount: () => stopCount
  };
}

test('Session list filters by source and starts a conversation with an Agent chooser', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Origin' }).selectOption('external');
  await expect(list.getByRole('button', { name: /External Agent/ })).toBeVisible();
  await expect(list.getByRole('button', { name: /Active Agent/ })).toHaveCount(0);
  await list.getByRole('combobox', { name: 'Origin' }).selectOption('all');

  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await dialog.getByRole('textbox', { name: 'Initial message' }).fill('Start a focused review.');
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  expect(fixture.createBody()).toEqual({ message: 'Start a focused review.', hub_session_id: null, parent_run_id: null });
  await expect(page.getByRole('region', { name: 'Session details' })).toContainText('New Agent');
  await expect(page.getByText('Start a focused review.', { exact: true })).toBeVisible();
});

test('conversation streams replies, folds technical events, steers, stops, and keeps history read-only', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Live assistant response.', { exact: true })).toBeVisible();
  const timeline = detail.locator('.session-transcript > *');
  await expect(timeline).toHaveCount(4);
  await expect(timeline.nth(0)).toContainText('Inspect the deployment.');
  await expect(timeline.nth(1)).toHaveClass(/session-technical-events/);
  await expect(timeline.nth(2)).toContainText('The deployment is running.');
  await expect(timeline.nth(3)).toContainText('Live assistant response.');

  const technical = detail.locator('details.session-technical-events').first();
  await expect(technical).not.toHaveAttribute('open', '');
  await expect(technical.locator('summary')).toContainText('Technical events · 5 sec');
  await expect(technical.locator('.session-technical-chevron')).toHaveCSS('transform', 'none');
  await technical.locator('summary').click();
  await expect(technical).toHaveAttribute('open', '');
  await expect(technical.locator('.session-technical-chevron')).not.toHaveCSS('transform', 'none');
  await expect(technical.locator('.session-technical-row')).toHaveCount(3);
  await expect(technical).toContainText('Command started');
  await expect(technical).toContainText('tool_request');

  await detail.getByRole('textbox', { name: 'Message' }).fill('Guide the running turn now.');
  await detail.getByRole('button', { name: 'Send' }).click();
  expect(fixture.steerBody()).toEqual({ content: 'Guide the running turn now.' });
  await expect(detail.locator('.session-bubble small').getByText('Guiding the current turn.')).toBeVisible();
  await detail.getByRole('button', { name: 'Stop current run' }).click();
  expect(fixture.stopCount()).toBe(1);

  await page.getByRole('button', { name: /Deleted Agent/ }).click();
  await expect(detail.getByText('Retained answer.', { exact: true })).toBeVisible();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveCount(0);
});

test('mobile conversation keeps the Session list in a dismissible drawer', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(list).toBeHidden();

  await detail.getByRole('button', { name: 'Session list' }).click();
  await expect(list).toBeVisible();
  await list.getByRole('button', { name: /External Agent/ }).click();

  await expect(list).toBeHidden();
  await expect(detail.getByText('External request', { exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
