import { expect, test, type Page, type Route } from '@playwright/test';

const ownerId = '10000000-0000-4000-8000-000000000001';
const activeAgentId = '20000000-0000-4000-8000-000000000001';
const newAgentId = '20000000-0000-4000-8000-000000000002';
const deletedAgentId = '20000000-0000-4000-8000-000000000003';
const now = '2026-07-17T10:00:00.000Z';
const draftStorageKey = `agent-hub:conversation-drafts:${ownerId}`;
const selectedAgentStorageKey = `agent-hub:selected-session-agent:${ownerId}`;

function session(id: string, agentId: string, agentName: string, origin: 'hub_native' | 'external', overrides: Record<string, unknown> = {}) {
  return {
    id,
    owner_id: ownerId,
    agent_id: agentId,
    agent_name: agentName,
    agent_deleted_at: null,
    origin_platform_name: origin === 'external' ? 'Support Desk' : null,
    origin: origin === 'hub_native'
      ? { kind: 'hub_native' }
      : { kind: 'external', platform_id: 'platform-one', tenant_id: 'tenant-one', external_identity_id: 'identity-one' },
    lifecycle_status: 'online',
    native_session_id: `session-${id}`,
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

async function installSessionApi(page: Page, options: {
  activeMessages?: Array<Record<string, unknown>>;
  activeEvents?: Array<Record<string, unknown>>;
  activeStreamEvents?: Array<Record<string, unknown>>;
  activeStreamGate?: Promise<void>;
  activeStreamRefreshesSession?: boolean;
  initialSessions?: ReturnType<typeof session>[];
  olderMessagePageGate?: Promise<void>;
} = {}) {
  const active = session('active', activeAgentId, 'Active Agent', 'hub_native', { active_turn_id: 'turn-active' });
  const external = session('external', activeAgentId, 'External Agent', 'external', { active_turn_id: 'turn-external' });
  const historical = session('historical', deletedAgentId, 'Deleted Agent', 'hub_native', {
    lifecycle_status: 'historical',
    agent_deleted_at: now,
    native_session_id: null,
    runtime_owner_id: null
  });
  const multiTurn = session('multi-turn', activeAgentId, 'Multi-turn Agent', 'hub_native');
  let created = session('new-session', newAgentId, 'New Agent', 'hub_native', {
    lifecycle_status: 'waiting_for_runtime',
    active_turn_id: null,
    updated_at: '2026-07-17T11:00:00.000Z'
  });
  let newSessionStreamCount = 0;
  let externalStreamCount = 0;
  let sessions = options.initialSessions ?? [active, external, historical, multiTurn];
  const messages: Record<string, Array<Record<string, unknown>>> = {
    active: options.activeMessages ?? [
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
    const url = new URL(request.url());
    const path = url.pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: { id: ownerId, email: 'session@example.com', display_name: 'Session owner', role: 'member' } });
    if (path === '/api/auth/logout' && request.method() === 'POST') return route.fulfill({ status: 204 });
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
        native_session_id: null, work_dir_ref: null, source: 'console', created_at: now, updated_at: now
      } });
    }
    if (path === '/api/sessions/new-session' && request.method() === 'GET') {
      return route.fulfill({ json: created });
    }
    const sessionMatch = path.match(/^\/api\/sessions\/([^/]+)$/);
    if (sessionMatch && request.method() === 'GET') {
      const selected = sessions.find((item) => item.id === sessionMatch[1]);
      return selected
        ? route.fulfill({ json: selected })
        : route.fulfill({ status: 404, json: { error: 'Session not found' } });
    }
    const messageMatch = path.match(/^\/api\/sessions\/([^/]+)\/messages$/);
    if (messageMatch && request.method() === 'GET') {
      const beforeValue = url.searchParams.get('before_sequence');
      const limitValue = url.searchParams.get('limit');
      const beforeSequence = beforeValue === null ? null : Number(beforeValue);
      const limit = limitValue === null ? null : Number(limitValue);
      if (beforeSequence !== null && options.olderMessagePageGate) await options.olderMessagePageGate;
      const page = (messages[messageMatch[1]] ?? [])
        .filter((item) => beforeSequence === null || Number(item.sequence) < beforeSequence);
      return route.fulfill({
        json: limit === null ? page : page.slice(Math.max(0, page.length - limit))
      });
    }
    if (messageMatch?.[1] === 'active' && request.method() === 'POST') {
      steerBody = request.postDataJSON() as Record<string, unknown>;
      const accepted = message('active', messages.active.length + 1, 'user', String(steerBody.content), { delivery_mode: 'steer', delivery_state: 'delivering', run_id: 'run-active', accepted_at: '2026-07-17T10:00:07.000Z' });
      messages.active.push(accepted);
      return route.fulfill({ json: { message: accepted, run: { id: 'run-active', hub_session_id: 'active', status: 'running' } } });
    }
    const streamMatch = path.match(/^\/api\/runs\/([^/]+)\/events\/stream$/);
    if (streamMatch) {
      if (streamMatch[1] === 'run-external') {
        externalStreamCount += 1;
        const activity = { seq: 1, run_id: 'run-external', event_type: 'item', role: null, content: null, payload: { item_id: 'external-reasoning', item_type: 'reasoning', phase: 'completed', summary: ['Handled by the external platform.'] }, created_at: '2026-07-17T10:00:02.000Z' };
        const reply = { seq: 2, run_id: 'run-external', event_type: 'message', role: 'assistant', content: 'External live response.', payload: {}, created_at: '2026-07-17T10:00:03.000Z' };
        const completed = { seq: 3, run_id: 'run-external', event_type: 'status', role: null, content: 'completed', payload: { status: 'completed' }, created_at: '2026-07-17T10:00:04.000Z' };
        return route.fulfill({ contentType: 'text/event-stream', body: [activity, reply, completed].map((event) => `event: run_event\ndata: ${JSON.stringify(event)}\n\n`).join('') });
      }
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
      if (options.activeStreamEvents !== undefined) {
        return route.fulfill({
          contentType: 'text/event-stream',
          body: options.activeStreamEvents.map((event) => `event: run_event\ndata: ${JSON.stringify(event)}\n\n`).join('')
        });
      }
      await options.activeStreamGate;
      if (options.activeStreamRefreshesSession) {
        const turnStarted = { seq: 6, run_id: 'run-active', event_type: 'turn_started', role: null, content: null, payload: { native_turn_id: 'native-turn-active' }, created_at: '2026-07-17T10:00:05.000Z' };
        return route.fulfill({ contentType: 'text/event-stream', body: `event: run_event\ndata: ${JSON.stringify(turnStarted)}\n\n` });
      }
      const liveMessage = { seq: 6, run_id: 'run-active', event_type: 'message', role: 'assistant', content: 'Live assistant response.', payload: {}, created_at: '2026-07-17T10:00:05.000Z' };
      const liveTool = { seq: 7, run_id: 'run-active', event_type: 'tool_request', role: null, content: null, payload: { tool_call_id: 'tool-one', tool_name: 'open_panel', arguments: { panel: 'deployments' } }, created_at: '2026-07-17T10:00:02.000Z' };
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
      return route.fulfill({ json: eventsMatch[1] === 'run-active' ? options.activeEvents ?? [
        { seq: 1, run_id: 'run-active', event_type: 'status', role: null, content: null, payload: { status: 'running' }, created_at: '2026-07-17T10:00:01.000Z' },
        { seq: 2, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'reasoning-1', item_type: 'reasoning', phase: 'started', summary: [] }, created_at: '2026-07-17T10:00:01.000Z' },
        { seq: 3, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'reasoning-1', item_type: 'reasoning', phase: 'completed', summary: ['Checked the deployment state.'] }, created_at: '2026-07-17T10:00:02.000Z' },
        { seq: 4, run_id: 'run-active', event_type: 'item', role: null, content: null, payload: { item_id: 'command-1', item_type: 'commandExecution', phase: 'completed', command: 'kubectl get deployment', output: 'deployment/api ready', status: 'completed', duration_ms: 2000 }, created_at: '2026-07-17T10:00:03.000Z' },
        { seq: 5, run_id: 'run-active', event_type: 'usage', role: null, content: null, payload: { input_tokens: 12 }, created_at: '2026-07-17T10:00:03.500Z' },
        { seq: 8, run_id: 'run-active', event_type: 'client_tool_result', role: 'tool', content: null, payload: { tool_call_id: 'tool-one', tool_name: 'open_panel', result: { status: 'success', output: { opened: true } }, elapsed_ms: 1500 }, created_at: '2026-07-17T10:00:03.500Z' }
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
    externalStreamCount: () => externalStreamCount,
    waitForDelayedCreate: () => delayedCreateStarted,
    releaseDelayedCreate
  };
}

test('Session list orders conversations by creation time from newest to oldest', async ({ page }) => {
  const oldest = session('oldest', activeAgentId, 'Oldest Session', 'hub_native', {
    created_at: '2026-07-17T08:00:00.000Z',
    updated_at: '2026-07-17T14:00:00.000Z'
  });
  const newest = session('newest', activeAgentId, 'Newest Session', 'hub_native', {
    created_at: '2026-07-17T12:00:00.000Z',
    updated_at: '2026-07-17T09:00:00.000Z'
  });
  const middle = session('middle', activeAgentId, 'Middle Session', 'hub_native', {
    created_at: '2026-07-17T10:00:00.000Z',
    updated_at: '2026-07-17T13:00:00.000Z'
  });
  await installSessionApi(page, { initialSessions: [oldest, newest, middle] });

  await page.goto('/sessions');

  const rows = page.getByRole('complementary', { name: 'Session list' })
    .locator('.session-row strong');
  await expect(rows).toHaveText(['Newest Session', 'Middle Session', 'Oldest Session']);
});

test('Session list uses platform-first and Agent-aware navigation for new Drafts', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  const controls = list.locator('.session-list-controls').locator('select, input');
  await expect(controls).toHaveCount(3);
  await expect(controls.nth(0)).toHaveAccessibleName('Platform');
  await expect(controls.nth(1)).toHaveAccessibleName('Agent');
  await expect(controls.nth(2)).toHaveAccessibleName('Search sessions');

  const platform = list.getByRole('combobox', { name: 'Platform' });
  const agent = list.getByRole('combobox', { name: 'Agent' });
  await expect(platform).toHaveValue('hub_native');
  await expect(platform.locator('option')).toHaveText(['Hub native', 'All platforms', 'Support Desk']);
  await expect(agent).toHaveValue(activeAgentId);
  await expect(agent.locator('option')).toHaveText(['Active Agent', 'New Agent', 'Deleted Agent']);
  await expect(list.getByRole('button', { name: /External Agent/ })).toHaveCount(0);

  await platform.selectOption({ label: 'Support Desk' });
  await expect(list.getByRole('button', { name: /External Agent/ })).toBeVisible();
  await expect(list.getByRole('button', { name: /Active Agent/ })).toHaveCount(0);
  await platform.selectOption('all');
  await expect(list.getByRole('button', { name: /External Agent/ })).toBeVisible();
  await expect(list.getByRole('button', { name: /Active Agent/ })).toBeVisible();
  await agent.selectOption(newAgentId);
  await expect(list.getByRole('button', { name: /External Agent/ })).toHaveCount(0);
  await expect(list.getByRole('button', { name: /Active Agent/ })).toHaveCount(0);
  await agent.selectOption(activeAgentId);

  await agent.selectOption(deletedAgentId);
  await expect(list.getByRole('button', { name: /Deleted Agent/ })).toBeVisible();
  await expect(page.getByRole('button', { name: 'New conversation' })).toBeDisabled();
  await agent.selectOption(activeAgentId);

  await platform.selectOption({ label: 'Support Desk' });
  const search = list.getByRole('textbox', { name: 'Search sessions' });
  await search.fill('old Session query');
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(platform).toHaveValue('hub_native');
  await expect(search).toBeEmpty();

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByRole('heading', { name: 'Active Agent' })).toBeVisible();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toBeEmpty();
  expect(fixture.createBody()).toBeNull();

  await agent.selectOption(newAgentId);
  await expect(list.getByRole('button', { name: /Active Agent/ })).toHaveCount(0);
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await detail.getByRole('textbox', { name: 'Message' }).fill('Start a focused review.');
  await detail.getByRole('button', { name: 'Send' }).click();
  expect(fixture.createBody()).toEqual({ message: 'Start a focused review.', hub_session_id: null, parent_run_id: null });
  await expect(list.getByRole('button', { name: /New Agent/ })).toBeVisible();
  await expect(detail.getByText('Start a focused review.', { exact: true })).toBeVisible();

  await page.reload();
  await expect(list.getByRole('combobox', { name: 'Agent' })).toHaveValue(newAgentId);
  await page.evaluate((key) => localStorage.setItem(key, 'agent-that-no-longer-exists'), selectedAgentStorageKey);
  await page.reload();
  await expect(page.getByRole('complementary', { name: 'Session list' }).getByRole('combobox', { name: 'Agent' })).toHaveValue(activeAgentId);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test('failed first message keeps the Conversation Draft and composer content', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('complementary', { name: 'Session list' }).getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  const composer = detail.getByRole('textbox', { name: 'Message' });
  await composer.fill('Keep this failed first message.');
  await detail.getByRole('button', { name: 'Send' }).click();

  await expect(detail.getByRole('alert')).toContainText('Unable to start the conversation. Retry.');
  await expect(composer).toHaveValue('Keep this failed first message.');
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await expect(page.getByRole('complementary', { name: 'Session list' }).getByRole('button', { name: /New Agent/ })).toHaveCount(0);
  expect(fixture.createBody()).toEqual({ message: 'Keep this failed first message.', hub_session_id: null, parent_run_id: null });

  await page.reload();
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' }))
    .toHaveValue('Keep this failed first message.');
});

test('Conversation Drafts persist per user and Agent and explicit logout clears only that user', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  const agentSelect = list.getByRole('combobox', { name: 'Agent' });
  await agentSelect.selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(page.getByRole('dialog', { name: 'New conversation' })).toHaveCount(0);
  expect(fixture.createBody()).toBeNull();

  let composer = page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' });
  await composer.fill('Draft for New Agent.');
  await page.reload();
  await page.getByRole('button', { name: 'New conversation' }).click();
  composer = page.getByRole('region', { name: 'Session details' }).getByRole('textbox', { name: 'Message' });
  await expect(composer).toHaveValue('Draft for New Agent.');

  await agentSelect.selectOption(activeAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  await composer.fill('Draft for Active Agent.');
  await agentSelect.selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(composer).toHaveValue('Draft for New Agent.');

  await page.getByRole('button', { name: 'Discard draft' }).click();
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(composer).toBeEmpty();
  await agentSelect.selectOption(activeAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(composer).toHaveValue('Draft for Active Agent.');

  const otherUserKey = 'agent-hub:conversation-drafts:10000000-0000-4000-8000-000000000099';
  await page.evaluate((key) => localStorage.setItem(key, JSON.stringify({ other: { content: 'keep' } })), otherUserKey);
  await page.getByRole('button', { name: 'Log out' }).click();
  await expect(page).toHaveURL(/\/login$/);
  await expect.poll(() => page.evaluate((key) => localStorage.getItem(key), draftStorageKey)).toBeNull();
  expect(await page.evaluate((key) => localStorage.getItem(key), otherUserKey)).not.toBeNull();
});

test('successful first message clears the Agent Conversation Draft', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();
  const detail = page.getByRole('region', { name: 'Session details' });
  await detail.getByRole('textbox', { name: 'Message' }).fill('Accepted first message.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await expect.poll(() => fixture.createBody()).toEqual({ message: 'Accepted first message.', hub_session_id: null, parent_run_id: null });
  await expect.poll(() => page.evaluate((key) => localStorage.getItem(key), draftStorageKey)).toBeNull();
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toBeEmpty();
});

test('composer sends with Enter and inserts a newline with Shift+Enter', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  await page.getByRole('complementary', { name: 'Session list' }).getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();

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

test('conversation follows new user and assistant output only while already at the bottom', async ({ page }) => {
  let releaseActiveStream!: () => void;
  const activeStreamGate = new Promise<void>((resolve) => { releaseActiveStream = resolve; });
  await installSessionApi(page, { activeStreamGate });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const scroll = detail.locator('.session-chat-scroll');
  await expect(detail.getByText('The deployment is running.', { exact: true })).toBeVisible();
  await page.addStyleTag({ content: '.session-transcript > * { min-height: 240px; }' });
  await expect.poll(() => scroll.evaluate((element) => element.scrollHeight > element.clientHeight)).toBe(true);

  const bottomDistance = () => scroll.evaluate((element) => (
    element.scrollHeight - element.clientHeight - element.scrollTop
  ));
  await scroll.evaluate((element) => { element.scrollTop = element.scrollHeight; });
  await expect.poll(bottomDistance).toBeLessThanOrEqual(1);

  const composer = detail.getByRole('textbox', { name: 'Message' });
  await composer.fill('Keep following my message.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await expect(detail.getByText('Keep following my message.', { exact: true })).toBeVisible();
  await expect.poll(bottomDistance).toBeLessThanOrEqual(1);

  releaseActiveStream();
  await expect(detail.getByText('Live assistant response.', { exact: true })).toBeVisible();
  await expect.poll(bottomDistance).toBeLessThanOrEqual(1);

  await scroll.hover();
  await page.mouse.wheel(0, -100_000);
  await page.evaluate(() => new Promise<void>((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
  }));
  await expect.poll(() => scroll.evaluate((element) => element.scrollTop)).toBeLessThanOrEqual(1);
  await composer.fill('Do not steal my scroll position.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await expect(detail.getByText('Do not steal my scroll position.', { exact: true })).toBeVisible();
  await expect.poll(() => scroll.evaluate((element) => element.scrollTop)).toBeLessThanOrEqual(1);
});

test('opening history starts at the latest message and prepends older messages without moving the reading position', async ({ page }) => {
  let releaseOlderPage!: () => void;
  const olderMessagePageGate = new Promise<void>((resolve) => { releaseOlderPage = resolve; });
  const activeMessages = Array.from({ length: 60 }, (_, index) => message(
    'active',
    index + 1,
    'user',
    `History message ${index + 1}`,
    { run_id: null, turn_id: null }
  ));
  await installSessionApi(page, { activeMessages, olderMessagePageGate });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const scroll = detail.locator('.session-chat-scroll');
  const anchor = detail.getByText('History message 11', { exact: true });
  await expect(anchor).toHaveCount(1);
  await expect(detail.getByText('History message 1', { exact: true })).toHaveCount(0);
  await expect(detail.getByText('History message 60', { exact: true })).toBeVisible();
  await expect(detail.locator('.session-transcript')).toHaveAttribute('aria-busy', 'false');
  await expect.poll(() => scroll.evaluate((element) => (
    element.scrollHeight - element.clientHeight - element.scrollTop
  ))).toBeLessThanOrEqual(1);

  const olderRequest = page.waitForRequest((request) => {
    const url = new URL(request.url());
    return url.pathname === '/api/sessions/active/messages'
      && url.searchParams.get('before_sequence') === '11';
  });
  await scroll.hover();
  await page.mouse.wheel(0, -100_000);
  await olderRequest;
  await expect(anchor).toBeVisible();
  const anchorTop = await anchor.evaluate((element) => element.getBoundingClientRect().top);
  releaseOlderPage();

  await expect(detail.getByText('History message 1', { exact: true })).toHaveCount(1);
  await expect.poll(async () => Math.abs(
    await anchor.evaluate((element) => element.getBoundingClientRect().top) - anchorTop
  )).toBeLessThanOrEqual(2);
  await expect.poll(() => scroll.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
});

test('an upward gesture loads older Session messages when the current page does not overflow', async ({ page }) => {
  const activeMessages = Array.from({ length: 60 }, (_, index) => message(
    'active',
    index + 1,
    index < 10 ? 'user' : 'assistant',
    index < 10 ? `Earlier short history ${index + 1}` : '',
    { run_id: null, turn_id: null }
  ));
  await installSessionApi(page, { activeMessages });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const scroll = detail.locator('.session-chat-scroll');
  await expect(detail.getByText('Earlier short history 1', { exact: true })).toHaveCount(0);
  expect(await scroll.evaluate((element) => element.scrollHeight <= element.clientHeight)).toBe(true);

  const olderRequest = page.waitForRequest((request) => {
    const url = new URL(request.url());
    return url.pathname === '/api/sessions/active/messages'
      && url.searchParams.get('before_sequence') === '11';
  });
  await scroll.evaluate((element) => {
    const start = new Touch({ identifier: 1, target: element, clientX: 20, clientY: 80 });
    const end = new Touch({ identifier: 1, target: element, clientX: 20, clientY: 140 });
    element.dispatchEvent(new TouchEvent('touchstart', { bubbles: true, touches: [start] }));
    element.dispatchEvent(new TouchEvent('touchend', { bubbles: true, changedTouches: [end] }));
  });
  await olderRequest;
  await expect(detail.getByText('Earlier short history 1', { exact: true })).toBeVisible();
});

test('a live Session refresh clears an invalidated older-message request', async ({ page }) => {
  let releaseOlderPage!: () => void;
  const olderMessagePageGate = new Promise<void>((resolve) => { releaseOlderPage = resolve; });
  let releaseActiveStream!: () => void;
  const activeStreamGate = new Promise<void>((resolve) => { releaseActiveStream = resolve; });
  const activeMessages = Array.from({ length: 60 }, (_, index) => message(
    'active',
    index + 1,
    'user',
    `Concurrent history message ${index + 1}`,
    { run_id: index === 59 ? 'run-active' : null, turn_id: null }
  ));
  await installSessionApi(page, {
    activeMessages,
    activeStreamGate,
    activeStreamRefreshesSession: true,
    olderMessagePageGate
  });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const scroll = detail.locator('.session-chat-scroll');
  const transcript = detail.locator('.session-transcript');
  await expect(transcript).toHaveAttribute('aria-busy', 'false');

  const olderRequest = page.waitForRequest((request) => {
    const url = new URL(request.url());
    return url.pathname === '/api/sessions/active/messages'
      && url.searchParams.get('before_sequence') === '11';
  });
  await scroll.hover();
  await page.mouse.wheel(0, -100_000);
  await olderRequest;
  await expect(transcript).toHaveAttribute('aria-busy', 'true');

  const refreshRequest = page.waitForRequest((request) => {
    const url = new URL(request.url());
    return url.pathname === '/api/sessions/active/messages'
      && url.searchParams.get('before_sequence') === null;
  });
  releaseActiveStream();
  await refreshRequest;
  releaseOlderPage();
  await expect(transcript).toHaveAttribute('aria-busy', 'false');
});

test('mobile history opens at the latest message and loads earlier messages without horizontal overflow', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const activeMessages = Array.from({ length: 60 }, (_, index) => message(
    'active',
    index + 1,
    'user',
    `Mobile history message ${index + 1}`,
    { run_id: null, turn_id: null }
  ));
  await installSessionApi(page, { activeMessages });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const scroll = detail.locator('.session-chat-scroll');
  const anchor = detail.getByText('Mobile history message 11', { exact: true });
  await expect(detail.getByText('Mobile history message 60', { exact: true })).toBeVisible();
  await expect(detail.getByText('Mobile history message 1', { exact: true })).toHaveCount(0);
  await expect(detail.locator('.session-transcript')).toHaveAttribute('aria-busy', 'false');
  await expect.poll(() => scroll.evaluate((element) => (
    element.scrollHeight - element.clientHeight - element.scrollTop
  ))).toBeLessThanOrEqual(1);

  await scroll.hover();
  await page.mouse.wheel(0, -100_000);
  await expect(detail.getByText('Mobile history message 1', { exact: true })).toHaveCount(1);
  await expect(anchor).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test('a terminal Run refreshes a queued user message to its durable delivery state', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await page.getByRole('button', { name: /Multi-turn Agent/ }).click();
  await expect(detail.getByText('Second persisted answer.', { exact: true })).toBeVisible();
  await expect(detail.getByText('queued', { exact: true })).toHaveCount(0);
});

test('a persisted Pi Turn timeout is shown once before and after Session reload', async ({ page }) => {
  const timeout = {
    seq: 6,
    run_id: 'run-active',
    event_type: 'status',
    role: null,
    content: 'failed',
    payload: { status: 'failed', error_code: 'engine_turn_timeout', timeout_seconds: 3600 },
    created_at: '2026-07-17T10:00:05.000Z'
  };
  const terminal = {
    seq: 7,
    run_id: 'run-active',
    event_type: 'status',
    role: null,
    content: 'failed',
    payload: { status: 'failed' },
    created_at: '2026-07-17T10:00:06.000Z'
  };
  await installSessionApi(page, { activeEvents: [timeout, terminal], activeStreamEvents: [] });

  await page.goto('/sessions');
  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByRole('alert')).toHaveText('This turn exceeded 60 minutes and was stopped.');
  await expect(detail.getByRole('alert')).toHaveCount(1);

  await page.reload();
  await expect(detail.getByRole('alert')).toHaveText('This turn exceeded 60 minutes and was stopped.');
  await expect(detail.getByRole('alert')).toHaveCount(1);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(detail.getByRole('alert')).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
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

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await detail.getByRole('textbox', { name: 'Message' }).fill('Create while viewing another Session.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await fixture.waitForDelayedCreate();

  await list.getByRole('combobox', { name: 'Agent' }).selectOption(activeAgentId);
  await list.getByRole('button', { name: /Active Agent/ }).click();
  await expect(detail.getByRole('heading', { name: 'Active Agent' })).toBeVisible();
  fixture.releaseDelayedCreate();

  await expect(detail.getByRole('heading', { name: 'Active Agent' })).toBeVisible();
  await expect(detail.getByText('Unable to start the conversation. Retry.')).toHaveCount(0);
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await expect(list.getByRole('button', { name: /New Agent/ })).toBeVisible();
  await expect.poll(() => page.evaluate((key) => localStorage.getItem(key), draftStorageKey)).toBeNull();
});

test('new conversation follows SSE active and terminal Session state without reload', async ({ page }) => {
  await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
  await page.getByRole('button', { name: 'New conversation' }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByRole('heading', { name: 'New Agent' })).toBeVisible();
  await detail.getByRole('textbox', { name: 'Message' }).fill('Hold this conversation.');
  await detail.getByRole('button', { name: 'Send' }).click();
  await expect(detail.getByText('Hold this conversation.', { exact: true })).toBeVisible();
  const thinking = detail.getByRole('status', { name: 'Thinking...' });
  await expect(thinking).toBeVisible();
  await expect(thinking.locator('span[aria-hidden="true"]').first()).toHaveCSS('animation-name', 'session-thinking-pulse');
  await expect(detail.getByRole('button', { name: 'Stop current run' })).toBeVisible();
  await expect(detail.getByText('Guiding the current turn.', { exact: true })).toHaveCount(0);
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveAttribute('placeholder', 'Guide the active turn...');

  await list.getByRole('combobox', { name: 'Agent' }).selectOption(activeAgentId);
  await list.getByRole('combobox', { name: 'Platform' }).selectOption({ label: 'Support Desk' });

  await expect(detail.getByRole('button', { name: 'Stop current run' })).toHaveCount(0);
  await expect(detail.getByText('Guiding the current turn.', { exact: true })).toHaveCount(0);
  await expect(thinking).toHaveCount(0);
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveCount(0);

  await list.getByRole('combobox', { name: 'Platform' }).selectOption('hub_native');
  await list.getByRole('combobox', { name: 'Agent' }).selectOption(newAgentId);
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
  await page.route('**/api/sessions/external/messages*', async (route) => {
    markExternalMessagesRequested();
    await externalMessagesGate;
    await route.fallback();
  });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(detail.locator('.session-activity-events')).toBeVisible();
  const oldStreamCount = streamedRunIds.filter((runId) => runId === 'run-active').length;

  await page.getByRole('complementary', { name: 'Session list' }).getByRole('combobox', { name: 'Platform' }).selectOption({ label: 'Support Desk' });
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

test('External Session is view-only in Hub while live history continues streaming', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  await list.getByRole('combobox', { name: 'Platform' }).selectOption({ label: 'Support Desk' });
  await list.getByRole('button', { name: /External Agent/ }).click();

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('External request', { exact: true })).toBeVisible();
  await expect(detail.getByText('External live response.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Support Desk', { exact: true })).toBeVisible();
  const timeline = detail.locator('.session-transcript > *');
  await expect(timeline).toHaveCount(3);
  await expect(timeline.nth(0)).toContainText('External request');
  await expect(timeline.nth(1)).toHaveClass(/session-activity-events/);
  await expect(timeline.nth(2)).toContainText('External live response.');
  const activity = detail.locator('details.session-activity-events');
  await expect(activity).toBeVisible();
  await activity.locator('summary').click();
  await expect(activity).toContainText('Handled by the external platform.');
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveCount(0);
  await expect(detail.getByRole('button', { name: 'Stop current run' })).toHaveCount(0);
  expect(fixture.externalStreamCount()).toBeGreaterThan(0);
});

test('processing time is split by visible replies while tool work stays in the surrounding segment', async ({ page }) => {
  await installSessionApi(page, {
    activeMessages: [
      message('active', 1, 'user', 'Complete the staged task.', { accepted_at: '2026-07-17T10:00:00.000Z' }),
      message('active', 2, 'assistant', 'I have finished the first stage.', { accepted_at: '2026-07-17T10:00:04.000Z' }),
      message('active', 3, 'assistant', 'The full task is complete.', { accepted_at: '2026-07-17T10:00:11.000Z' })
    ],
    activeEvents: [
      { seq: 1, run_id: 'run-active', event_type: 'status', role: null, content: 'running', payload: { status: 'running' }, created_at: '2026-07-17T10:00:00.100Z' },
      { seq: 2, run_id: 'run-active', event_type: 'item', role: 'assistant', content: null, payload: { item_id: 'reasoning-one', item_type: 'reasoning', phase: 'completed', summary: ['Prepared the first stage.'] }, created_at: '2026-07-17T10:00:03.000Z' },
      { seq: 3, run_id: 'run-active', event_type: 'item', role: 'assistant', content: null, payload: { item_id: 'tool-one', item_type: 'dynamicToolCall', phase: 'started', tool: 'inspect_state' }, created_at: '2026-07-17T10:00:05.000Z' },
      { seq: 4, run_id: 'run-active', event_type: 'item', role: 'assistant', content: null, payload: { item_id: 'tool-one', item_type: 'dynamicToolCall', phase: 'completed', tool: 'inspect_state', output: 'ready' }, created_at: '2026-07-17T10:00:08.000Z' },
      { seq: 5, run_id: 'run-active', event_type: 'item', role: 'assistant', content: null, payload: { item_id: 'reasoning-two', item_type: 'reasoning', phase: 'completed', summary: ['Prepared the final answer.'] }, created_at: '2026-07-17T10:00:10.000Z' },
      { seq: 6, run_id: 'run-active', event_type: 'status', role: null, content: 'completed', payload: { status: 'completed' }, created_at: '2026-07-17T10:00:12.000Z' }
    ],
    activeStreamEvents: []
  });
  await page.goto('/sessions');

  const groups = page.getByRole('region', { name: 'Session details' }).locator('details.session-activity-events');
  await expect(groups).toHaveCount(2);
  await expect(groups.nth(0).locator('summary')).toContainText('Worked for 4 sec');
  await expect(groups.nth(1).locator('summary')).toContainText('Worked for 7 sec');
  await groups.nth(1).locator('summary').click();
  await expect(groups.nth(1)).toContainText('Used tool');
  await expect(groups.nth(1)).toContainText('inspect_state');
  await expect(groups.nth(1)).toContainText('Prepared the final answer.');
});

test('an unfinished processing segment keeps increasing without a duplicate thinking bubble', async ({ page }) => {
  const startedAt = Date.now() - 250;
  await installSessionApi(page, {
    activeMessages: [
      message('active', 1, 'user', 'Keep processing.', { accepted_at: new Date(startedAt).toISOString() })
    ],
    activeEvents: [
      { seq: 1, run_id: 'run-active', event_type: 'status', role: null, content: 'running', payload: { status: 'running' }, created_at: new Date(startedAt + 20).toISOString() },
      { seq: 2, run_id: 'run-active', event_type: 'item', role: 'assistant', content: null, payload: { item_id: 'reasoning-live', item_type: 'reasoning', phase: 'started' }, created_at: new Date(startedAt + 50).toISOString() }
    ],
    activeStreamEvents: []
  });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const summary = detail.locator('details.session-activity-events summary');
  await expect(summary).toContainText('Worked for');
  const first = Number.parseFloat((await summary.textContent())?.match(/[\d.]+/)?.[0] ?? '0');
  await page.waitForTimeout(1_200);
  const second = Number.parseFloat((await summary.textContent())?.match(/[\d.]+/)?.[0] ?? '0');
  expect(second).toBeGreaterThan(first);
  await expect(detail.locator('.session-thinking')).toHaveCount(0);
});

test('conversation streams replies, folds readable activity, steers, stops, and keeps history read-only', async ({ page }) => {
  const fixture = await installSessionApi(page);
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(detail.getByText('Live assistant response.', { exact: true })).toBeVisible();
  const timeline = detail.locator('.session-transcript > *');
  await expect(timeline).toHaveCount(5);
  await expect(timeline.nth(0)).toContainText('Inspect the deployment.');
  await expect(timeline.nth(1)).toHaveClass(/session-activity-events/);
  await expect(timeline.nth(2)).toContainText('The deployment is running.');
  await expect(timeline.nth(3)).toContainText('Live assistant response.');
  await expect(timeline.nth(4)).toHaveClass(/session-thinking/);

  const activity = detail.locator('details.session-activity-events').first();
  await expect(activity).not.toHaveAttribute('open', '');
  await expect(activity.locator('summary')).toContainText('Worked for 4 sec');
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
  await expect(activity).toContainText('open_panel');
  await expect(activity).toContainText('succeeded');
  await expect(activity).toContainText('1.5 sec');
  await expect(activity).toContainText('"opened": true');
  await expect(detail.getByText('status', { exact: true })).toHaveCount(0);
  await expect(detail.getByText('usage', { exact: true })).toHaveCount(0);

  await detail.getByRole('textbox', { name: 'Message' }).fill('Guide the running turn now.');
  await detail.getByRole('button', { name: 'Send' }).click();
  expect(fixture.steerBody()).toEqual({ content: 'Guide the running turn now.' });
  await expect(detail.getByText('Guiding the current turn.', { exact: true })).toHaveCount(0);
  await detail.getByRole('button', { name: 'Stop current run' }).click();
  expect(fixture.stopCount()).toBe(1);

  await page.getByRole('complementary', { name: 'Session list' }).getByRole('combobox', { name: 'Agent' }).selectOption(deletedAgentId);
  await page.getByRole('button', { name: /Deleted Agent/ }).click();
  await expect(detail.getByText('Retained answer.', { exact: true })).toBeVisible();
  await expect(detail.getByRole('textbox', { name: 'Message' })).toHaveCount(0);
});

test('assistant messages render Streamdown Markdown while user messages stay literal text', async ({ page }) => {
  const diagnostics: string[] = [];
  page.on('pageerror', (error) => diagnostics.push(`page: ${error.message}`));
  page.on('console', (message) => {
    if (message.type() === 'error') diagnostics.push(`console: ${message.text()}`);
  });
  page.on('response', (response) => {
    const path = new URL(response.url()).pathname;
    if (path.startsWith('/api/') && response.status() >= 400) diagnostics.push(`api: ${response.status()} ${path}`);
  });
  const assistantMarkdown = [
    '## Deployment result',
    '',
    '- API is ready',
    '- Worker is ready',
    '',
    '中文~~旧状态~~新状态',
    '',
    '| Service | Status |',
    '| --- | ---: |',
    '| API | Ready |',
    '',
    '```typescript',
    'const deploymentConfiguration = { endpoint: "https://example.com/a/very/long/javascript/path", retries: 3, timeoutMilliseconds: 30_000, preserveConversationHistory: true };',
    '```',
    '',
    '```javascript',
    'const a = 1;',
    'const b = 2;',
    'a + b;',
    '```',
    '',
    'Inline math: $a^2+b^2=c^2$.',
    '',
    '$$',
    'E = mc^2',
    '$$',
    '',
    '```mermaid',
    'flowchart LR',
    '  Request --> Response',
    '```',
    '',
    '<details>',
    '<summary>More details</summary>',
    '',
    '<mark>Highlighted safely</mark> and press <kbd>Enter</kbd>.',
    '',
    '</details>',
    '',
    '<script>window.mustNotRun = true</script>',
    '',
    'Use `status --json` and [Open docs](https://example.com/docs).'
  ].join('\n');
  await installSessionApi(page, { activeMessages: [
    message('active', 1, 'user', '## Keep this user Markdown literal', {
      accepted_at: '2026-07-17T10:00:00.000Z'
    }),
    message(
      'active',
      2,
      'assistant',
      assistantMarkdown,
      { accepted_at: '2026-07-17T10:00:04.000Z' }
    )
  ] });
  await page.goto('/sessions');

  const detail = page.getByRole('region', { name: 'Session details' });
  const userMessage = detail.locator('.session-bubble.role-user').filter({ hasText: 'Keep this user Markdown literal' });
  const assistantMessage = detail.locator('.session-bubble.role-assistant').filter({ hasText: 'Deployment result' });
  await expect(userMessage.locator('h2')).toHaveCount(0);
  await expect(userMessage.locator('.session-message-text')).toContainText('## Keep this user Markdown literal');
  await expect(assistantMessage.locator('h2')).toHaveText('Deployment result');
  await expect(assistantMessage.locator('li')).toHaveText(['API is ready', 'Worker is ready']);
  await expect(assistantMessage.locator('del')).toHaveText('旧状态');
  await expect(assistantMessage.locator('[data-streamdown="table"]')).toContainText('Ready');
  await expect(assistantMessage.locator('[data-streamdown="code-block"][data-language="typescript"]')).toContainText('const deploymentConfiguration');
  await expect(assistantMessage.locator('.katex')).toHaveCount(2);
  await expect(assistantMessage.locator('[data-streamdown="mermaid"] svg')).toBeVisible();
  await expect(assistantMessage.locator('details summary')).toHaveText('More details');
  await expect(assistantMessage.locator('mark')).toHaveText('Highlighted safely');
  await expect(assistantMessage.locator('kbd')).toHaveText('Enter');
  await expect(assistantMessage.locator('script')).toHaveCount(0);
  expect(await page.evaluate(() => Boolean((window as Window & { mustNotRun?: boolean }).mustNotRun))).toBe(false);
  await expect(assistantMessage.locator('[data-streamdown="inline-code"]').filter({ hasText: 'status --json' })).toHaveText('status --json');
  await expect(assistantMessage.getByRole('link', { name: 'Open docs' })).toHaveAttribute('target', '_blank');
  await expect(assistantMessage.getByRole('link', { name: 'Open docs' })).toHaveAttribute('rel', 'noreferrer');
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(assistantMessage.locator('[data-streamdown="mermaid"] svg')).toBeVisible();
  const codeLines = assistantMessage.locator('[data-streamdown="code-block"][data-language="javascript"] pre > code > span');
  await expect(codeLines).toHaveCount(3);
  const lineTops = await codeLines.evaluateAll((lines) => lines.map((line) => line.getBoundingClientRect().top));
  expect(lineTops[1]).toBeGreaterThan(lineTops[0]);
  expect(lineTops[2]).toBeGreaterThan(lineTops[1]);
  const lineNumberContent = await codeLines.evaluateAll((lines) => lines.map((line) => getComputedStyle(line, '::before').content));
  expect(lineNumberContent.every((content) => content.includes('counter('))).toBe(true);
  const codeBlock = assistantMessage.locator('[data-streamdown="code-block"][data-language="typescript"]');
  const codeBody = codeBlock.locator('[data-streamdown="code-block-body"]');
  await expect(codeBlock.locator('pre')).toHaveCSS('white-space', 'pre');
  await expect(codeBody).toHaveCSS('overflow-x', 'auto');
  const codeDimensions = await codeBody.evaluate((element) => ({
    clientWidth: element.clientWidth,
    scrollWidth: element.scrollWidth
  }));
  expect(codeDimensions.scrollWidth).toBeGreaterThan(codeDimensions.clientWidth);
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth
  }));
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
  expect(diagnostics).toEqual([]);
});

test('mobile conversation keeps the Session list in a dismissible drawer', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const diagnostics: string[] = [];
  page.on('pageerror', (error) => diagnostics.push(`page: ${error.message}`));
  page.on('console', (message) => {
    if (message.type() === 'error') diagnostics.push(`console: ${message.text()}`);
  });
  page.on('response', (response) => {
    const path = new URL(response.url()).pathname;
    if (path.startsWith('/api/') && response.status() >= 400) {
      diagnostics.push(`api: ${response.status()} ${path}`);
    }
  });
  await installSessionApi(page);
  await page.goto('/sessions');

  const list = page.getByRole('complementary', { name: 'Session list' });
  const detail = page.getByRole('region', { name: 'Session details' });
  await expect(detail.getByText('Inspect the deployment.', { exact: true })).toBeVisible();
  await expect(list).toBeHidden();

  await detail.getByRole('button', { name: 'Session list' }).click();
  await expect(list).toBeVisible();
  expect(await list.evaluate((element) => element.scrollWidth <= element.clientWidth)).toBe(true);
  await list.getByRole('combobox', { name: 'Platform' }).selectOption({ label: 'Support Desk' });
  await list.getByRole('button', { name: /External Agent/ }).click();

  await expect(list).toBeHidden();
  await expect(detail.getByText('External request', { exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  expect(diagnostics).toEqual([]);
});
