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
    active_turn_id: null as string | null,
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
  const multiTurn = session('multi-turn', activeAgentId, 'Multi-turn Agent', 'hub_native');
  let created = session('new-session', newAgentId, 'New Agent', 'hub_native', {
    lifecycle_status: 'waiting_for_runtime',
    active_turn_id: null,
    updated_at: '2026-07-17T11:00:00.000Z'
  });
  let newSessionStreamCount = 0;
  let sessions = [active, external, historical, multiTurn];
  const messages: Record<string, Array<Record<string, unknown>>> = {
    active: [
      message('active', 1, 'user', 'Inspect the deployment.', { accepted_at: '2026-07-17T10:00:00.000Z' }),
      message('active', 2, 'assistant', 'The deployment is running.', { run_id: 'run-active', accepted_at: '2026-07-17T10:00:04.000Z' })
    ],
    external: [message('external', 1, 'user', 'External request')],
    historical: [message('historical', 1, 'assistant', 'Retained answer.')],
    'multi-turn': [
      message('multi-turn', 1, 'user', 'First persisted question.', {
        run_id: 'run-first-persisted',
        turn_id: 'turn-first-persisted',
        accepted_at: '2026-07-17T09:00:00.000Z'
      }),
      message('multi-turn', 2, 'user', 'Second persisted question.', {
        run_id: 'run-second-persisted',
        turn_id: 'turn-second-persisted',
        delivery_state: 'queued',
        accepted_at: '2026-07-17T09:01:00.000Z'
      })
    ]
  };
  let createBody: Record<string, unknown> | null = null;
  let steerBody: Record<string, unknown> | null = null;
  let stopCount = 0;
  let releaseDelayedCreate!: () => void;
  let markDelayedCreateStarted!: () => void;
  const delayedCreate = new Promise<void>((resolve) => { releaseDelayedCreate = resolve; });
  const delayedCreateStarted = new Promise<void>((resolve) => { markDelayedCreateStarted = resolve; });
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
      if (createBody.message === 'Keep this failed first message.') {
        return route.fulfill({ status: 502, json: { error: 'Model unavailable' } });
      }
      if (createBody.message === 'Create while viewing another Session.') {
        markDelayedCreateStarted();
        await delayedCreate;
      }
      sessions = [created, ...sessions];
      messages['new-session'] = [message('new-session', 1, 'user', String(createBody.message), { run_id: 'run-new', delivery_state: 'queued' })];
      return route.fulfill({ json: {
        id: 'run-new', agent_id: newAgentId, automation_id: null, integration_session_id: null, hub_session_id: 'new-session', hub_message_id: messages['new-session'][0].id,
        hub_turn_id: 'turn-new', session_ownership_generation: 0, parent_run_id: null, runtime_id: null, status: 'pending', initial_message: createBody.message,
        session_id: null, work_dir_ref: null, source: 'console', created_at: now, updated_at: now
      } });
    }
    if (path === '/api/sessions/new-session' && request.method() === 'GET') {
      return route.fulfill({ json: created });
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
      if (streamMatch[1] === 'run-second-persisted') {
        messages['multi-turn'][1] = {
          ...messages['multi-turn'][1],
          delivery_state: 'delivered'
        };
        const completed = { seq: 2, run_id: 'run-second-persisted', event_type: 'status', role: null, content: 'completed', payload: { status: 'completed' }, created_at: '2026-07-17T09:01:04.000Z' };
        return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(completed)}\n\n` });
      }
      if (streamMatch[1] === 'run-new') {
        newSessionStreamCount += 1;
        if (newSessionStreamCount === 1) {
          created = { ...created, lifecycle_status: 'online', active_turn_id: 'turn-new' };
          const turnStarted = { seq: 1, run_id: 'run-new', event_type: 'turn_started', role: null, content: null, payload: { native_turn_id: 'native-turn-new' }, created_at: now };
          return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(turnStarted)}\n\n` });
        }
        created = { ...created, active_turn_id: null };
        const completed = { seq: 2, run_id: 'run-new', event_type: 'status', role: null, content: 'completed', payload: { status: 'completed' }, created_at: now };
        return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(completed)}\n\n` });
      }
      if (streamMatch[1] !== 'run-active') return route.fulfill({ contentType: 'text/event-stream', body: '' });
      const liveMessage = { seq: 6, run_id: 'run-active', event_type: 'message', role: 'assistant', content: 'Live assistant response.', payload: {}, created_at: '2026-07-17T10:00:05.000Z' };
      const liveTool = { seq: 7, run_id: 'run-active', event_type: 'tool_request', role: null, content: null, payload: { tool_request_id: 'tool-one', tool_name: 'shell' }, created_at: '2026-07-17T10:00:03.500Z' };
      return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(liveMessage)}\n\nevent: run_event\ndata: ${JSON.stringify(liveTool)}\n\n` });
    }
    const eventsMatch = path.match(/^\/api\/runs\/([^/]+)\/events$/);
    if (eventsMatch) {
      const persistedAnswers: Record<string, Array<Record<string, unknown>>> = {
        'run-first-persisted': [
          { seq: 1, run_id: 'run-first-persisted', event_type: 'message', role: 'assistant', content: 'First persisted answer.', payload: {}, created_at: '2026-07-17T09:00:04.000Z' }
        ],
        'run-second-persisted': [
          { seq: 1, run_id: 'run-second-persisted', event_type: 'message', role: 'assistant', content: 'Second persisted answer.', payload: {}, created_at: '2026-07-17T09:01:03.000Z' }
        ]
      };
      return route.fulfill({ json: eventsMatch[1] === 'run-active' ? [
        { seq: 1, run_id: 'run-active', event_type: 'status', role: null, content: null, payload: { status: 'running' }, created_at: '2026-07-17T10:00:01.000Z' },
        { seq: 2, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'reasoning-1', item_type: 'reasoning', phase: 'started', summary: [] }, created_at: '2026-07-17T10:00:01.000Z' },
        { seq: 3, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'reasoning-1', item_type: 'reasoning', phase: 'completed', summary: ['Checked the deployment state.'] }, created_at: '2026-07-17T10:00:02.000Z' },
        { seq: 4, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'command-1', item_type: 'commandExecution', phase: 'completed', command: 'kubectl get deployment', output: 'deployment/api ready', status: 'completed', duration_ms: 2000 }, created_at: '2026-07-17T10:00:03.000Z' },
        { seq: 5, run_id: 'run-active', event_type: 'usage', role: null, content: null, payload: { input_tokens: 12 }, created_at: '2026-07-17T10:00:03.500Z' },
        { seq: 8, run_id: 'run-active', event_type: 'tool_result', role: 'tool', content: 'Tool result for shell: {"api_key":"must-not-render"}', payload: { tool_request_id: 'tool-one', message: { result: { api_key: 'must-not-render' } } }, created_at: '2026-07-17T10:00:03.500Z' }
      ] : persistedAnswers[eventsMatch[1]] ?? [] });
    }
    if (path === '/api/runs/run-active/stop' && request.method() === 'POST') {
      stopCount += 1;
      return route.fulfill({ json: { id: 'run-active', status: 'running' } });
    }
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${request.method()} ${path}` } });
  });

  return {
    createBody: () => createBody,
    steerBody: () => steerBody,
    stopCount: () => stopCount,
    waitForDelayedCreate: () => delayedCreateStarted,
    releaseDelayedCreate
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
  await expect(dialog.getByRole('textbox', { name: 'Initial message' })).toHaveCount(0);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toBeEmpty();
  await expect(list.getByRole('button', { name: /New Agent/ })).toHaveCount(0);
  expect(fixture.createBody()).toBeNull();

  await detail.getByRole('textbox', { name: 'Message' }).fill('Start a focused review.');
  await detail.getByRole('button', { name: 'Send' }).click();
  expect(fixture.createBody()).toEqual({ message: 'Start a focused review.', hub_session_id: null, parent_run_id: null });
  await expect(list.getByRole('button', { name: /New Agent/ })).toBeVisible();
  await expect(detail.getByText('Start a focused review.', { exact: true })).toBeVisible();
});

test('failed first message keeps the Conversation Draft and composer content', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  const composer = detail.getByRole('textbox', { name: 'Message' });
  await composer.fill('Keep this failed first message.');
  await detail.getByRole('button', { name: 'Send' }).click();

  await expect(detail.getByRole('alert')).toContainText('Unable to start the conversation. Retry.');
  await expect(composer).toHaveValue('Keep this failed first message.');
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await expect(page.getByRole('complementary', { name: 'Session list' }).getByRole('button', { name: /New Agent/ })).toHaveCount(0);
  expect(fixture.createBody()).toEqual({ message: 'Keep this failed first message.', hub_session_id: null, parent_run_id: null });
});

test('composer sends with Enter and inserts a newline with Shift+Enter', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  const composer = page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' });
  await composer.fill('Line one');
  await composer.press('Shift+Enter');
  await composer.type('Line two');
  await expect(composer).toHaveValue('Line one\nLine two');
  expect(fixture.createBody()).toBeNull();

  await composer.fill('Send this with Enter.');
  await composer.press('Enter');
  await expect.poll(() => fixture.createBody()).toEqual({ message: 'Send this with Enter.', hub_session_id: null, parent_run_id: null });
});

test('composer grows from two lines to at most five lines', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  const composer = page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' });
  await expect(composer).toHaveAttribute('rows', '2');
  await composer.fill('1');
  const twoLineHeight = await composer.evaluate((element) => element.getBoundingClientRect().height);
  await composer.fill('1\n2\n3\n4\n5');
  const fiveLineHeight = await composer.evaluate((element) => element.getBoundingClientRect().height);
  await composer.fill('1\n2\n3\n4\n5\n6');
  const sixLineHeight = await composer.evaluate((element) => element.getBoundingClientRect().height);
  expect(fiveLineHeight).toBeGreaterThan(twoLineHeight);
  expect(Math.abs(sixLineHeight - fiveLineHeight)).toBeLessThanOrEqual(1);
});

test('a terminal Run refreshes a queued user message to its durable delivery state', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await page.getByRole('button', { name: /Multi-turn Agent/ }).click();
  await expect(detail.getByText('Second persisted answer.', { exact: true })).toBeVisible();
  await expect(detail.getByText('queued', { exact: true })).toHaveCount(0);
});

test('reloading a two-turn Session keeps every assistant answer', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await page.getByRole('button', { name: /Multi-turn Agent/ }).click();
  await expect(detail.getByText('First persisted answer.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Second persisted answer.', { exact: true })).toBeVisible();

  await page.reload();
  await page.getByRole('button', { name: /Multi-turn Agent/ }).click();
  await expect(detail.getByText('First persisted answer.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Second persisted answer.', { exact: true })).toBeVisible();
});

test('opening an existing Session is not overwritten by a pending Conversation Draft request', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await detail.getByRole('textbox', { name: 'Message' }).fill('Create while viewing another Session.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await fixture.waitForDelayedCreate();

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('button', { name: /Active Agent/ }).click();
  await expect(detail.getByRole('heading', { name: 'Active Agent' })).toBeVisible();
  fixture.releaseDelayedCreate();

  await expect(list.getByRole('button', { name: /New Agent/ })).toBeVisible();
  await expect(detail.getByRole('heading', { name: 'Active Agent' })).toBeVisible();
  await expect(detail.getByText('Unable to start the conversation. Retry.')).toHaveCount(0);
});

test('new conversation follows SSE active and terminal Session state without reload', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('button', { name: 'New conversation' }).click();
  const dialog = page.getByRole('dialog', { name: 'New conversation' });
  await dialog.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await dialog.getByRole('button', { name: 'Start conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await detail.getByRole('textbox', { name: 'Message' }).fill('Hold this conversation.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await expect(detail.getByText('Hold this conversation.', { exact: true })).toBeVisible();
  const thinking = detail.getByRole('status', { name: 'Thinking...' });
  await expect(thinking).toBeVisible();
  await expect(thinking.locator('span[aria-hidden="true"]').first()).toHaveCSS('animation-name', 'session-thinking-pulse');
  await expect(detail.getByRole('button', { name: 'Stop current run' })).toBeVisible();
  await expect(detail.getByText('Guiding the current turn.', { exact: true })).toBeVisible();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveAttribute('placeholder', 'Guide the active turn...');

  await page.getByRole('button', { name: /External Agent/ }).click();
  await page.getByRole('button', { name: /New Agent/ }).click();

  await expect(detail.getByRole('button', { name: 'Stop current run' })).toHaveCount(0);
  await expect(detail.getByText('Guiding the current turn.', { exact: true })).toHaveCount(0);
  await expect(thinking).toHaveCount(0);
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveAttribute('placeholder', 'Message the agent...');
});

test('switching Sessions does not leak the previous transcript or Run stream while messages load', async ({ page }) => {
  await installSessionApi(page);
  let releaseExternalMessages = () => {};
  const externalMessagesGate = new Promise<void>((resolve) => { releaseExternalMessages = resolve; });
  let markExternalMessagesRequested = () => {};
  const externalMessagesRequested = new Promise<void>((resolve) => { markExternalMessagesRequested = resolve; });
  const streamedRunIds: string[] = [];
  page.on('request', (request) => {
    const match = new URL(request.url()).pathname.match(/^\/api\/runs\/([^/]+)\/events\/stream$/);
    if (match) streamedRunIds.push(match[1]);
  });
  await page.route('**/api/sessions/external/messages', async (route) => {
    markExternalMessagesRequested();
    await externalMessagesGate;
    await route.fallback();
  });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(detail.locator('.session-activity-events')).toBeVisible();
  const oldStreamCount = streamedRunIds.filter((runId) => runId === 'run-active').length;

  await page.getByRole('button', { name: /External Agent/ }).click();
  await externalMessagesRequested;
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  try {
    await expect(detail.getByRole('heading', { name: 'External Agent' })).toBeVisible();
    await expect(detail.getByText('Inspect the deployment.', { exact: true })).toHaveCount(0);
    await expect(detail.locator('.session-activity-events')).toHaveCount(0);
    expect(streamedRunIds.filter((runId) => runId === 'run-active')).toHaveLength(oldStreamCount);
  } finally {
    releaseExternalMessages();
  }
  await expect(detail.getByText('External request', { exact: true })).toBeVisible();
});

test('conversation streams replies, folds readable activity, steers, stops, and keeps history read-only', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Live assistant response.', { exact: true })).toBeVisible();
  const timeline = detail.locator('.session-transcript > *');
  await expect(timeline).toHaveCount(4);
  await expect(timeline.nth(0)).toContainText('Inspect the deployment.');
  await expect(timeline.nth(1)).toHaveClass(/session-activity-events/);
  await expect(timeline.nth(2)).toContainText('The deployment is running.');
  await expect(timeline.nth(3)).toContainText('Live assistant response.');

  const activity = detail.locator('details.session-activity-events').first();
  await expect(activity).not.toHaveAttribute('open', '');
  await expect(activity.locator('summary')).toContainText('Worked for 2.5 sec');
  await expect(activity.locator('.session-activity-chevron')).toHaveCSS('transform', 'none');
  await activity.locator('summary').click();
  await expect(activity).toHaveAttribute('open', '');
  await expect(activity.locator('.session-activity-chevron')).not.toHaveCSS('transform', 'none');
  await expect(activity.locator('.session-activity-row')).toHaveCount(3);
  await expect(activity).toContainText('Thought');
  await expect(activity).toContainText('Checked the deployment state.');
  await expect(activity).toContainText('Ran command');
  await expect(activity).toContainText('kubectl get deployment');
  await expect(activity).toContainText('deployment/api ready');
  await expect(activity).toContainText('Used tool');
  await expect(activity).toContainText('shell');
  await expect(detail.getByText(/must-not-render/)).toHaveCount(0);
  await expect(detail.getByText('status', { exact: true })).toHaveCount(0);
  await expect(detail.getByText('usage', { exact: true })).toHaveCount(0);

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
