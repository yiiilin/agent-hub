import { expect, test, type Page, type Route } from '@playwright/test';

type AgentFixture = {
  id: string;
  name: string;
  instructions: string;
  visibility: string;
  public_to: string[];
  runtime_id: string | null;
  model_selection: { connection_id: string; model_id: string } | null;
  model_settings: {
    reasoning_effort: 'default' | 'none' | 'minimal' | 'low' | 'medium' | 'high' | 'xhigh' | 'max' | 'ultra';
    reasoning_summary: 'default' | 'auto' | 'concise' | 'detailed' | 'none';
    verbosity: 'default' | 'low' | 'medium' | 'high';
    context_window_tokens: number | null;
    auto_compact_token_limit: number | null;
    reasoning_summary_support: 'auto' | 'supported' | 'unsupported';
    service_tier: string | null;
    request_max_retries: number | null;
    stream_max_retries: number | null;
    stream_idle_timeout_ms: number | null;
    request_settings: { protocol: 'openai_responses' } | { protocol: 'openai_chat_completions'; temperature: number | null; top_p: number | null; max_completion_tokens: number | null } | { protocol: 'anthropic_messages'; temperature: number | null; top_p: number | null; max_tokens: number | null };
  };
  subagents: Array<{
    name: string;
    description: string;
    developer_instructions: string;
    model_selection: AgentFixture['model_selection'];
    model_settings_override: Record<string, unknown>;
    enabled?: boolean;
    disabled_reason?: string | null;
  }>;
  owner_id: string;
  is_owner: boolean;
  can_manage: boolean;
  can_administer: boolean;
  can_invoke: boolean;
  model_policy: Record<string, unknown>;
  sandbox_policy: Record<string, unknown>;
  managed_skill_ids: string[];
  mcp_allowlist: unknown[];
  created_at: string;
  updated_at: string;
};

const currentUser = {
  id: '10000000-0000-0000-0000-000000000001',
  username: 'agents-page-tester',
  email: 'agents-page@example.com',
  display_name: 'Agents page tester',
  role: 'admin'
};

const runtimes = [
  {
    id: '20000000-0000-0000-0000-000000000001',
    hostname: 'alpha-runtime',
    labels: [],
    engine_version: 'test',
    capabilities: {},
    sandbox_mode: 'workspace-write',
    status: 'online',
    last_heartbeat_at: '2026-07-11T02:00:00.000Z'
  },
  {
    id: '20000000-0000-0000-0000-000000000002',
    hostname: 'zulu-runtime',
    labels: [],
    engine_version: 'test',
    capabilities: {},
    sandbox_mode: 'workspace-write',
    status: 'offline',
    last_heartbeat_at: '2026-07-11T01:00:00.000Z'
  }
];

const globalModelId = '21000000-0000-0000-0000-000000000001';
const personalModelId = '21000000-0000-0000-0000-000000000002';
const modelOptions = {
  items: [
    { connection_id: globalModelId, connection_name: 'Global GPT', model_id: 'gpt-global', api_type: 'openai_responses', scope: 'global', status: 'enabled' },
    { connection_id: personalModelId, connection_name: 'Personal GPT', model_id: 'gpt-personal', api_type: 'openai_chat_completions', scope: 'personal', status: 'enabled' }
  ],
  system_default: { connection_id: globalModelId, model_id: 'gpt-global' }
};

const automaticModelSettings: AgentFixture['model_settings'] = {
  reasoning_effort: 'default', reasoning_summary: 'default', verbosity: 'default',
  context_window_tokens: null, auto_compact_token_limit: null, reasoning_summary_support: 'auto',
  service_tier: null, request_max_retries: null, stream_max_retries: null, stream_idle_timeout_ms: null,
  request_settings: { protocol: 'openai_responses' }
};

function agentFixture(overrides: Partial<AgentFixture> & Pick<AgentFixture, 'id' | 'name'>): AgentFixture {
  return {
    instructions: `${overrides.name} instructions`,
    visibility: 'private',
    public_to: [],
    runtime_id: null,
    model_selection: null,
    model_settings: automaticModelSettings,
    subagents: [],
    owner_id: currentUser.id,
    is_owner: true,
    can_manage: true,
    can_administer: true,
    can_invoke: true,
    model_policy: {},
    sandbox_policy: {},
    managed_skill_ids: [],
    mcp_allowlist: [],
    created_at: '2026-07-10T12:00:00.000Z',
    updated_at: '2026-07-10T12:00:00.000Z',
    ...overrides
  };
}

const listAgents = [
  agentFixture({
    id: '30000000-0000-0000-0000-000000000004',
    name: 'Automatic Agent',
    runtime_id: null,
    visibility: 'public',
    managed_skill_ids: ['skill-a'],
    created_at: '2026-07-11T03:00:00.000Z'
  }),
  agentFixture({
    id: '30000000-0000-0000-0000-000000000003',
    name: 'Online Agent',
    runtime_id: runtimes[0].id,
    visibility: 'public_to',
    managed_skill_ids: ['skill-a', 'skill-b'],
    created_at: '2026-07-11T03:00:00.000Z'
  }),
  agentFixture({
    id: '30000000-0000-0000-0000-000000000002',
    name: 'Offline Agent',
    runtime_id: runtimes[1].id,
    created_at: '2026-07-10T03:00:00.000Z'
  }),
  agentFixture({
    id: '30000000-0000-0000-0000-000000000001',
    name: 'Unbound Agent',
    runtime_id: '20000000-0000-0000-0000-000000000099',
    managed_skill_ids: ['skill-a', 'skill-b', 'skill-c'],
    created_at: '2026-07-09T03:00:00.000Z'
  })
];

async function installListApi(page: Page, agents: AgentFixture[], options: {
  failAgents?: () => boolean;
  holdAgents?: Promise<void>;
  currentUser?: typeof currentUser;
  users?: typeof currentUser[];
  createAgent?: (route: Route) => Promise<void>;
} = {}) {
  await page.route('**/api/**', async (route: Route) => {
    const path = new URL(route.request().url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: options.currentUser ?? currentUser });
    if (path === '/api/agents' && route.request().method() === 'POST' && options.createAgent) {
      return options.createAgent(route);
    }
    if (path === '/api/agents') {
      await options.holdAgents;
      if (options.failAgents?.()) return route.fulfill({ status: 503, json: { error: 'private upstream list detail' } });
      return route.fulfill({ json: agents });
    }
    if (path === '/api/runtimes') return route.fulfill({ json: runtimes });
    if (path === '/api/users') return route.fulfill({ json: options.users ?? [currentUser] });
    if (path === '/api/model-connections/options') return route.fulfill({ json: modelOptions });
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${path}` } });
  });
}

function agentRows(page: Page) {
  return page.getByRole('table', { name: 'Agent list' }).locator('tbody tr');
}

test('agent list derives availability, filters, and sorts only the six reliable columns', async ({ page }) => {
  await installListApi(page, listAgents);
  await page.goto('/agents');

  const table = page.getByRole('table', { name: 'Agent list' });
  await expect(table).toBeVisible();
  await expect(table.getByRole('columnheader')).toHaveText([
    'Name', 'Availability', 'Runtime hostname', 'Visibility', 'Managed skills', 'Created'
  ]);
  expect(await agentRows(page).evaluateAll((rows) => rows.map((row) => row.getAttribute('data-agent-id')))).toEqual([
    listAgents[0].id, listAgents[1].id, listAgents[2].id, listAgents[3].id
  ]);
  await expect(agentRows(page).nth(0)).toContainText('Automatic');
  await expect(agentRows(page).nth(1)).toContainText('Online');
  await expect(agentRows(page).nth(2)).toContainText('Offline');
  await expect(agentRows(page).nth(3)).toContainText('Unbound');
  await expect(agentRows(page).nth(1)).toContainText('alpha-runtime');
  await expect(agentRows(page).nth(3)).toContainText('Unbound');

  await page.getByLabel('Search agents').fill('offline');
  await expect(agentRows(page)).toHaveCount(1);
  await expect(agentRows(page)).toContainText('Offline Agent');
  await page.getByLabel('Search agents').fill('');

  await page.getByLabel('Availability', { exact: true }).selectOption('automatic');
  await expect(agentRows(page)).toHaveCount(1);
  await expect(agentRows(page)).toContainText('Automatic Agent');
  await page.getByLabel('Availability', { exact: true }).selectOption('unbound');
  await expect(agentRows(page)).toContainText('Unbound Agent');
  await page.getByLabel('Availability', { exact: true }).selectOption('all');

  await page.getByLabel('Runtime filter').selectOption(runtimes[0].id);
  await expect(agentRows(page)).toContainText('Online Agent');
  await page.getByLabel('Runtime filter').selectOption('all');
  await page.getByLabel('Visibility filter').selectOption('public_to');
  await expect(agentRows(page)).toContainText('Online Agent');
  await page.getByLabel('Visibility filter').selectOption('all');

  for (const heading of ['Name', 'Availability', 'Runtime hostname', 'Visibility', 'Managed skills', 'Created']) {
    await table.getByRole('button', { name: new RegExp(`^Sort by ${heading}`) }).click();
    await expect(table.getByRole('columnheader', { name: new RegExp(heading) })).toHaveAttribute('aria-sort', /ascending|descending/);
  }
  await table.getByRole('button', { name: /^Sort by Name/ }).click();
  await expect(agentRows(page).first()).toContainText('Automatic Agent');

  const automaticAgentButton = agentRows(page).first().getByRole('button', { name: 'Automatic Agent' });
  await automaticAgentButton.focus();
  await expect(automaticAgentButton).toBeFocused();
  await automaticAgentButton.press('Enter');
  await expect(page).toHaveURL(`/agents/${listAgents[0].id}`);
  await page.goto('/agents');
  const automaticAgentButtonAfterReload = agentRows(page).first().getByRole('button', { name: 'Automatic Agent' });
  await automaticAgentButtonAfterReload.focus();
  await automaticAgentButtonAfterReload.press('Space');
  await expect(page).toHaveURL(`/agents/${listAgents[0].id}`);
});

test('agent list has independent loading, redacted error retry, empty, and filtered-empty states', async ({ page }) => {
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  let fail = true;
  await installListApi(page, listAgents, { failAgents: () => fail, holdAgents: held });
  await page.goto('/agents');
  await expect(page.getByText('Loading agents...')).toBeVisible();
  release();
  await expect(page.getByRole('alert')).toContainText('Unable to load agents. Retry.');
  await expect(page.locator('body')).not.toContainText('private upstream list detail');
  fail = false;
  await page.getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByRole('table', { name: 'Agent list' })).toBeVisible();

  await page.getByLabel('Search agents').fill('no-agent-has-this-name');
  await expect(page.getByText('No agents match these filters.')).toBeVisible();

  await page.unroute('**/api/**');
  await installListApi(page, []);
  await page.reload();
  await expect(page.getByText('No agents yet.')).toBeVisible();
  await expect(page.locator('.agents-header').getByRole('button', { name: 'Create Agent' })).toBeVisible();
});

test('create Agent modal traps focus, restores its opener, redacts errors, and serializes submission', async ({ page }) => {
  const nonce = Date.now();
  const userA = { ...currentUser, id: '10000000-0000-0000-0000-000000000002', email: `a-${nonce}@example.com`, display_name: 'User A' };
  const userB = { ...currentUser, id: '10000000-0000-0000-0000-000000000003', email: `b-${nonce}@example.com`, display_name: 'User B' };
  let createRequests = 0;
  const createBodies: Record<string, unknown>[] = [];
  let releaseCreate!: () => void;
  const heldCreate = new Promise<void>((resolve) => { releaseCreate = resolve; });
  const created = agentFixture({
    id: '30000000-0000-0000-0000-000000000099',
    name: `Created Agent ${nonce}`
  });
  await installListApi(page, listAgents, {
    users: [currentUser, userA, userB],
    createAgent: async (route) => {
      createRequests += 1;
      createBodies.push(route.request().postDataJSON() as Record<string, unknown>);
      if (createRequests === 1) return route.fulfill({ status: 500, json: { error: 'private create failure with secret-value' } });
      await heldCreate;
      return route.fulfill({ json: created });
    }
  });
  await page.goto('/agents');

  const opener = page.locator('.agents-header').getByRole('button', { name: 'Create Agent' });
  await opener.click();
  let dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await expect(dialog.getByLabel('Name', { exact: true })).toBeFocused();
  await dialog.getByLabel('Name', { exact: true }).press('Shift+Tab');
  await expect(dialog.getByRole('button', { name: 'Close' })).toBeFocused();
  await dialog.getByRole('button', { name: 'Close' }).click();
  await expect(opener).toBeFocused();

  await opener.click();
  dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await dialog.getByLabel('Name', { exact: true }).press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(opener).toBeFocused();

  await opener.click();
  dialog = page.getByRole('dialog', { name: 'Create Agent' });
  await dialog.getByLabel('Name', { exact: true }).fill(created.name);
  await dialog.getByLabel('Instructions').fill('Created through the modal test.');
  await expect(dialog.getByLabel('Model API Connection and model')).toHaveValue(`${globalModelId}\ngpt-global`);
  await dialog.getByLabel('Reasoning effort').selectOption('high');
  await dialog.getByRole('button', { name: 'Add subagent' }).click();
  const subagentDialog = page.getByRole('dialog', { name: 'Add subagent' });
  await subagentDialog.getByLabel('Subagent name').fill('reviewer');
  await subagentDialog.getByLabel('Description').fill('Reviews the current change.');
  await subagentDialog.getByRole('textbox', { name: 'Developer instructions' }).fill('Review the diff and report blocking findings.');
  await subagentDialog.getByLabel('Model override').selectOption(`${personalModelId}\ngpt-personal`);
  await subagentDialog.getByLabel('Reasoning effort Setting source').selectOption('override');
  await subagentDialog.locator('label').filter({ hasText: 'Reasoning effort' }).locator('select').nth(1).selectOption('max');
  await subagentDialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(dialog.getByRole('table', { name: 'Subagents' })).toContainText('reviewer');
  await dialog.getByLabel('Visibility').selectOption('public_to');
  await expect(dialog.getByRole('checkbox')).toHaveCount(2);
  await expect(dialog.getByText(currentUser.email)).toHaveCount(0);
  await dialog.getByRole('checkbox', { name: new RegExp(userA.email) }).check();

  await dialog.getByRole('button', { name: 'Create agent' }).click();
  await expect(dialog.getByRole('alert')).toContainText('The agent could not be created. Retry.');
  await expect(dialog).not.toContainText('private create failure');
  await expect(dialog).not.toContainText('secret-value');

  await dialog.getByRole('button', { name: 'Create agent' }).click();
  await expect(dialog.getByRole('button', { name: 'Creating...' })).toBeDisabled();
  await expect(dialog.getByRole('button', { name: 'Close' })).toBeDisabled();
  await expect(dialog.getByRole('button', { name: 'Cancel' })).toBeDisabled();
  await dialog.press('Escape');
  await expect(dialog).toBeVisible();
  await expect.poll(() => createRequests).toBe(2);
  releaseCreate();
  await expect(page).toHaveURL(`/agents/${created.id}`);
  expect(createRequests).toBe(2);
  expect(createBodies[1]).toMatchObject({
    model_selection: { connection_id: globalModelId, model_id: 'gpt-global' },
    model_settings: expect.objectContaining({ reasoning_effort: 'high' }),
    subagents: [{
      name: 'reviewer',
      description: 'Reviews the current change.',
      developer_instructions: 'Review the diff and report blocking findings.',
      model_selection: { connection_id: personalModelId, model_id: 'gpt-personal' },
      model_settings_override: { reasoning_effort: 'max' }
    }]
  });
});

test('only admin and super_admin users can select public Agent visibility', async ({ page }) => {
  for (const [index, role] of ['member', 'admin', 'super_admin'].entries()) {
    await page.unroute('**/api/**').catch(() => undefined);
    const authenticatedUser = { ...currentUser, role };
    const agent = ownerDetail({
      id: `50000000-0000-0000-0000-00000000006${index}`,
      visibility: 'private'
    });
    await installDetailApi(page, { agent, currentUser: authenticatedUser, users: [authenticatedUser] });

    await page.goto('/agents');
    await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
    const createDialog = page.getByRole('dialog', { name: 'Create Agent' });
    await expect(createDialog.getByLabel('Visibility').locator('option[value="public"]')).toHaveCount(role === 'member' ? 0 : 1);
    await createDialog.getByRole('button', { name: 'Close' }).click();

    await page.goto(`/agents/${agent.id}`);
    await page.getByRole('tab', { name: 'Access' }).click();
    const access = page.getByRole('tabpanel', { name: 'Access' });
    await expect(access.getByLabel('Visibility').locator('option[value="public"]')).toHaveCount(role === 'member' ? 0 : 1);
  }
});

const detailSkills = [
  {
    id: '40000000-0000-0000-0000-000000000001',
    owner_id: currentUser.id,
    name: 'Repository review',
    description: 'Review changes',
    content: 'Review repository changes.',
    created_at: '2026-07-01T00:00:00.000Z',
    updated_at: '2026-07-02T00:00:00.000Z'
  },
  {
    id: '40000000-0000-0000-0000-000000000002',
    owner_id: currentUser.id,
    name: 'Release notes',
    description: 'Write release notes',
    content: 'Summarize the release.',
    created_at: '2026-07-01T00:00:00.000Z',
    updated_at: '2026-07-02T00:00:00.000Z'
  }
];

function ownerDetail(overrides: Partial<AgentFixture> = {}) {
  return agentFixture({
    id: '50000000-0000-0000-0000-000000000001',
    name: 'Detail Agent',
    instructions: 'Initial detail instructions.',
    runtime_id: runtimes[0].id,
    visibility: 'public_to',
    public_to: ['10000000-0000-0000-0000-000000000002'],
    model_policy: { provider: 'hub-proxy' },
    sandbox_policy: { mode: 'workspace-write' },
    managed_skill_ids: [detailSkills[0].id],
    mcp_allowlist: [{
      name: 'filesystem',
      command: 'fs',
      args: ['--root', '/workspace'],
      secrets: { API_TOKEN: 'filesystem-secret-value' }
    }],
    created_at: '2026-07-01T01:00:00.000Z',
    updated_at: '2026-07-10T02:00:00.000Z',
    ...overrides
  });
}

type DetailApiOptions = {
  agent?: AgentFixture;
  runs?: Array<Record<string, unknown>>;
  currentUser?: typeof currentUser;
  users?: typeof currentUser[];
  onPatch?: (route: Route, body: Record<string, unknown>) => Promise<void>;
  onDelete?: (route: Route) => Promise<void>;
  onRuns?: (route: Route, requestCount: number) => Promise<void>;
};

async function installDetailApi(page: Page, options: DetailApiOptions = {}) {
  let agent = options.agent ?? ownerDetail();
  let runsRequestCount = 0;
  await page.route('**/api/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: options.currentUser ?? currentUser });
    if (path === '/api/agents' && request.method() === 'GET') return route.fulfill({ json: [agent] });
    if (path === `/api/agents/${agent.id}` && request.method() === 'GET') return route.fulfill({ json: agent });
    if (path === `/api/agents/${agent.id}/model-options` && request.method() === 'GET') return route.fulfill({ json: modelOptions });
    if (path === `/api/agents/${agent.id}` && request.method() === 'PATCH') {
      const body = request.postDataJSON() as Record<string, unknown>;
      if (options.onPatch) return options.onPatch(route, body);
      agent = { ...agent, ...body, updated_at: '2026-07-11T04:00:00.000Z' } as AgentFixture;
      return route.fulfill({ json: agent });
    }
    if (path === `/api/agents/${agent.id}` && request.method() === 'DELETE') {
      if (options.onDelete) return options.onDelete(route);
      return route.fulfill({ status: 204 });
    }
    if (path === `/api/agents/${agent.id}/runs` && request.method() === 'GET') {
      runsRequestCount += 1;
      if (options.onRuns) return options.onRuns(route, runsRequestCount);
      return route.fulfill({ json: options.runs ?? [] });
    }
    if (path === '/api/runtimes') return route.fulfill({ json: runtimes });
    if (path === '/api/skills') return route.fulfill({ json: detailSkills });
    if (path === '/api/users') return route.fulfill({ json: options.users ?? [currentUser, { ...currentUser, id: agent.public_to[0] ?? 'other', username: 'other', email: 'other@example.com', display_name: 'Other user' }] });
    if (/^\/api\/runs\/[^/]+\/events$/.test(path)) return route.fulfill({ json: [{ seq: 1, run_id: path.split('/')[3], event_type: 'message', role: 'assistant', content: 'Console result', payload: {}, created_at: '2026-07-11T05:00:01.000Z' }] });
    if (/^\/api\/runs\/[^/]+\/events\/stream$/.test(path)) return route.fulfill({ contentType: 'text/event-stream', body: '' });
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${request.method()} ${path}` } });
  });
  return { agent: () => agent };
}

test('agent detail uses the six-tab IA and stacks the inspector first on mobile', async ({ page }) => {
  const { agent } = await installDetailApi(page);
  await page.goto(`/agents/${agent().id}`);

  const tabs = page.getByRole('tablist', { name: 'Agent detail sections' }).getByRole('tab');
  await expect(tabs).toHaveText(['Activity', 'Instructions', 'Models', 'Skills', 'MCP', 'Access']);
  await expect(tabs.nth(0)).toHaveAttribute('aria-selected', 'true');
  const inspector = page.getByRole('complementary', { name: 'Agent inspector' });
  await expect(inspector).toContainText(agent().name);
  await expect(inspector).toContainText('alpha-runtime');
  await expect(inspector).toContainText('Online');
  await expect(inspector).toContainText('Specific users');
  await expect(inspector).toContainText('Repository review');
  await expect(inspector).toContainText('Created');
  await expect(inspector).toContainText('Updated');

  await page.setViewportSize({ width: 390, height: 844 });
  const positions = await page.evaluate(() => ({
    inspector: document.querySelector<HTMLElement>('.agent-inspector')?.getBoundingClientRect().top,
    tabs: document.querySelector<HTMLElement>('.agent-tabs')?.getBoundingClientRect().top,
    overflow: document.documentElement.scrollWidth - window.innerWidth
  }));
  expect(positions.inspector).toBeLessThan(positions.tabs!);
  expect(positions.overflow).toBeLessThanOrEqual(0);
});

test('models panel edits the Agent default, reasoning, and Subagent definitions', async ({ page }) => {
  const agent = ownerDetail({
    id: '50000000-0000-0000-0000-000000000055',
    model_selection: { connection_id: globalModelId, model_id: 'gpt-global' },
    model_settings: { ...automaticModelSettings, reasoning_effort: 'medium' },
    subagents: [{
      name: 'reviewer',
      description: 'Reviews the current change.',
      developer_instructions: 'Review the diff.',
      model_selection: null,
      model_settings_override: {}
    }]
  });
  const patches: Record<string, unknown>[] = [];
  await installDetailApi(page, {
    agent,
    onPatch: async (route, body) => {
      patches.push(body);
      await route.fulfill({ json: { ...agent, ...body, updated_at: '2026-07-11T07:00:00.000Z' } });
    }
  });

  await page.goto(`/agents/${agent.id}`);
  await page.getByRole('tab', { name: 'Models' }).click();
  const panel = page.getByRole('tabpanel', { name: 'Models' });
  await expect(panel.getByLabel('Model API Connection and model')).toHaveValue(`${globalModelId}\ngpt-global`);
  await expect(panel.getByLabel('Reasoning effort')).toHaveValue('medium');
  await expect(panel.getByLabel('Reasoning effort').locator('option')).toHaveText([
    'default', 'none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra'
  ]);
  await expect(panel.getByRole('table', { name: 'Subagents' })).toContainText('reviewer');

  await panel.getByRole('button', { name: 'Edit subagent: reviewer' }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit subagent: reviewer' });
  await dialog.getByLabel('Developer instructions').fill('Review the diff and identify release blockers.');
  await dialog.getByLabel('Model override').selectOption(`${personalModelId}\ngpt-personal`);
  await dialog.getByLabel('Reasoning effort Setting source').selectOption('override');
  await dialog.locator('label').filter({ hasText: 'Reasoning effort' }).locator('select').nth(1).selectOption('ultra');
  await dialog.getByRole('button', { name: 'Save changes' }).click();

  await panel.getByLabel('Model API Connection and model').selectOption(`${personalModelId}\ngpt-personal`);
  await panel.getByLabel('Reasoning effort').selectOption('max');
  await panel.getByRole('button', { name: 'Save agent' }).click();
  await expect.poll(() => patches.length).toBe(1);
  expect(patches[0]).toMatchObject({
    model_selection: { connection_id: personalModelId, model_id: 'gpt-personal' },
    model_settings: expect.objectContaining({ reasoning_effort: 'max' }),
    subagents: [{
      name: 'reviewer',
      developer_instructions: 'Review the diff and identify release blockers.',
      model_selection: { connection_id: personalModelId, model_id: 'gpt-personal' },
      model_settings_override: { reasoning_effort: 'ultra' }
    }]
  });
  expect(patches[0]).not.toHaveProperty('model_policy');
});

test('skills panel shows enabled managed Skills and edits them through a FormDialog PATCH', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000053' });
  const patches: Record<string, unknown>[] = [];
  await installDetailApi(page, {
    agent,
    onPatch: async (route, body) => {
      patches.push(body);
      await route.fulfill({ json: { ...agent, ...body, updated_at: '2026-07-11T07:00:00.000Z' } });
    }
  });
  await page.goto(`/agents/${agent.id}`);
  await page.getByRole('tab', { name: 'Skills' }).click();

  const panel = page.getByRole('tabpanel', { name: 'Skills' });
  const enabledSkills = panel.locator('.agent-skill-chips');
  await expect(enabledSkills).toContainText('Repository review');
  await expect(enabledSkills).not.toContainText('Release notes');
  await expect(panel.getByRole('checkbox')).toHaveCount(0);

  await panel.getByRole('button', { name: 'Edit managed skills' }).click();
  const dialog = page.getByRole('dialog', { name: 'Edit managed skills' });
  await expect(dialog.getByRole('checkbox', { name: /Repository review/ })).toBeChecked();
  await expect(dialog.getByRole('checkbox', { name: /Release notes/ })).not.toBeChecked();
  await dialog.getByRole('checkbox', { name: /Release notes/ }).check();
  await dialog.getByRole('button', { name: 'Save changes' }).click();

  await expect(dialog).toHaveCount(0);
  await expect.poll(() => patches.length).toBe(1);
  expect(patches[0]).toMatchObject({ managed_skill_ids: [detailSkills[0].id, detailSkills[1].id] });
  await expect(enabledSkills).toContainText('Repository review');
  await expect(enabledSkills).toContainText('Release notes');
});

test('MCP table and subforms redact secrets and PATCH create, edit, and delete operations', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000054' });
  const patches: Record<string, unknown>[] = [];
  await installDetailApi(page, {
    agent,
    onPatch: async (route, body) => {
      patches.push(body);
      await route.fulfill({ json: { ...agent, ...body, updated_at: '2026-07-11T07:00:00.000Z' } });
    }
  });
  await page.goto(`/agents/${agent.id}`);
  await page.getByRole('tab', { name: 'MCP', exact: true }).click();

  const panel = page.getByRole('tabpanel', { name: 'MCP' });
  const table = panel.getByRole('table', { name: 'MCP allowlist' });
  await expect(table).toContainText('filesystem');
  await expect(table).toContainText('--root /workspace');
  await expect(table).toContainText('API_TOKEN=********');
  await expect(table).not.toContainText('filesystem-secret-value');

  await panel.getByRole('button', { name: 'Add MCP entry' }).click();
  let dialog = page.getByRole('dialog', { name: 'Add MCP entry' });
  await dialog.getByLabel('Name', { exact: true }).fill('filesystem');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(dialog.getByRole('alert')).toContainText('MCP entry names must be unique.');
  expect(patches).toHaveLength(0);
  await dialog.getByRole('button', { name: 'Cancel' }).click();

  await panel.getByRole('button', { name: 'Add MCP entry' }).click();
  dialog = page.getByRole('dialog', { name: 'Add MCP entry' });
  await dialog.getByLabel('Name', { exact: true }).fill('github');
  await dialog.getByLabel('Command').fill('github-mcp');
  await dialog.getByLabel('Arguments').fill('--repo\nagent-hub');
  await dialog.getByRole('button', { name: 'Add secret' }).click();
  await dialog.getByLabel('Secret name 1').fill('GITHUB_TOKEN');
  await dialog.getByLabel('Secret value 1').fill('github-secret-value');
  await dialog.getByRole('button', { name: 'Save changes' }).click();

  await expect(dialog).toHaveCount(0);
  await expect.poll(() => patches.length).toBe(1);
  expect(patches[0].mcp_allowlist).toEqual([
    {
      name: 'filesystem',
      command: 'fs',
      args: ['--root', '/workspace'],
      secrets: { API_TOKEN: '********' }
    },
    {
      name: 'github',
      command: 'github-mcp',
      args: ['--repo', 'agent-hub'],
      secrets: { GITHUB_TOKEN: 'github-secret-value' }
    }
  ]);
  await expect(table).toContainText('GITHUB_TOKEN=********');
  await expect(table).not.toContainText('github-secret-value');

  await table.getByRole('button', { name: 'Edit MCP entry: filesystem' }).click();
  dialog = page.getByRole('dialog', { name: 'Edit MCP entry: filesystem' });
  await expect(dialog.getByLabel('Secret value 1')).toHaveValue('********');
  await dialog.getByLabel('Command').fill('fs-next');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect.poll(() => patches.length).toBe(2);
  expect((patches[1].mcp_allowlist as Array<Record<string, unknown>>)[0]).toMatchObject({
    name: 'filesystem',
    command: 'fs-next',
    secrets: { API_TOKEN: '********' }
  });

  await table.getByRole('button', { name: 'Edit MCP entry: filesystem' }).click();
  dialog = page.getByRole('dialog', { name: 'Edit MCP entry: filesystem' });
  await dialog.getByLabel('Name', { exact: true }).fill('filesystem-renamed');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(dialog.getByRole('alert')).toContainText('Enter secret values again after renaming an MCP entry or secret.');
  expect(patches).toHaveLength(2);
  await dialog.getByLabel('Secret value 1').fill('replacement-secret-value');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect.poll(() => patches.length).toBe(3);
  await expect(table).toContainText('filesystem-renamed');
  await expect(table).toContainText('API_TOKEN=********');
  await expect(table).not.toContainText('replacement-secret-value');

  page.once('dialog', (confirmation) => confirmation.accept());
  await table.getByRole('button', { name: 'Delete github' }).click();
  await expect.poll(() => patches.length).toBe(4);
  expect(patches[3].mcp_allowlist).toEqual([
    {
      name: 'filesystem-renamed',
      command: 'fs-next',
      args: ['--root', '/workspace'],
      secrets: { API_TOKEN: '********' }
    }
  ]);
  await expect(table.getByText('github', { exact: true })).toHaveCount(0);
});

test('agent deletion confirms and sends DELETE without viewport regressions', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000052' });
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  const deleteMethods: string[] = [];
  let deleteStarted = false;
  let staleRunRequests = 0;
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}: ${request.failure()?.errorText ?? 'unknown'}`));
  page.on('request', (request) => {
    if (deleteStarted && request.method() === 'GET'
      && new URL(request.url()).pathname === `/api/agents/${agent.id}/runs`) staleRunRequests += 1;
  });
  await installDetailApi(page, {
    agent,
    onDelete: async (route) => {
      deleteMethods.push(route.request().method());
      deleteStarted = true;
      await new Promise((resolve) => setTimeout(resolve, 2_100));
      await route.fulfill({ status: 204, body: '' });
    }
  });

  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto(`/agents/${agent.id}`);
  await expect(page).toHaveURL(`/agents/${agent.id}`);
  await expect(page.getByRole('button', { name: 'Delete agent', exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole('button', { name: 'Delete agent', exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
  page.once('dialog', async (dialog) => {
    expect(dialog.message()).toContain(agent.name);
    await dialog.accept();
  });
  await page.getByRole('button', { name: 'Delete agent', exact: true }).click();
  await expect(page).toHaveURL('/agents');
  expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);

  expect(deleteMethods).toEqual(['DELETE']);
  expect(staleRunRequests).toBe(0);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures.filter((failure) => failure !== `DELETE /api/agents/${agent.id}: net::ERR_ABORTED`)).toEqual([]);
});

test('agent deletion aborts a stalled Run refresh before sending DELETE', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000058' });
  let releaseRefresh!: () => void;
  const heldRefresh = new Promise<void>((resolve) => { releaseRefresh = resolve; });
  let refreshStarted!: () => void;
  const refreshRequest = new Promise<void>((resolve) => { refreshStarted = resolve; });
  let deleteStarted = false;
  await installDetailApi(page, {
    agent,
    onRuns: async (route, requestCount) => {
      if (requestCount === 1) return route.fulfill({ json: [] });
      refreshStarted();
      await heldRefresh;
      if (!route.request().failure()) await route.fulfill({ json: [] });
    },
    onDelete: async (route) => {
      deleteStarted = true;
      releaseRefresh();
      await route.fulfill({ status: 204, body: '' });
    }
  });

  await page.goto(`/agents/${agent.id}`);
  await refreshRequest;
  page.once('dialog', (dialog) => dialog.accept());
  try {
    await page.getByRole('button', { name: 'Delete agent', exact: true }).click();
    await expect.poll(() => deleteStarted, { timeout: 1_000 }).toBe(true);
    await expect(page).toHaveURL('/agents');
  } finally {
    releaseRefresh();
  }
});

test('agent detail keeps tab drafts mounted and blocks dirty or pending navigation', async ({ page }) => {
  let releasePatch!: () => void;
  const heldPatch = new Promise<void>((resolve) => { releasePatch = resolve; });
  let patchStarted!: () => void;
  const started = new Promise<void>((resolve) => { patchStarted = resolve; });
  const detail = ownerDetail();
  await installDetailApi(page, {
    agent: detail,
    onPatch: async (route, body) => {
      patchStarted();
      await heldPatch;
      await route.fulfill({ json: { ...detail, ...body, name: `${body.name} (server)`, updated_at: '2026-07-11T06:00:00.000Z' } });
    }
  });
  await page.goto(`/agents/${detail.id}`);

  await page.getByRole('tab', { name: 'Instructions' }).click();
  const instructionsPanel = page.getByRole('tabpanel', { name: 'Instructions' });
  await instructionsPanel.getByLabel('Name', { exact: true }).fill('Draft name');
  await page.getByRole('tab', { name: 'Skills' }).click();
  await page.getByRole('tab', { name: 'Instructions' }).click();
  await expect(instructionsPanel.getByLabel('Name', { exact: true })).toHaveValue('Draft name');

  page.once('dialog', async (dialog) => {
    expect(dialog.message()).toBe('Discard unsaved agent changes?');
    await dialog.dismiss();
  });
  await page.getByRole('button', { name: 'Skills', exact: true }).first().click();
  await expect(page).toHaveURL(`/agents/${detail.id}`);

  await instructionsPanel.getByRole('button', { name: 'Save agent' }).click();
  await started;
  await expect(instructionsPanel.getByRole('button', { name: 'Saving...' })).toBeDisabled();
  await expect(instructionsPanel.getByLabel('Name', { exact: true })).toBeDisabled();
  await expect(instructionsPanel.getByRole('textbox', { name: 'Instructions' })).toBeDisabled();
  await expect(page.getByRole('tab', { name: 'Skills' })).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Delete agent', exact: true })).toBeDisabled();
  await page.getByRole('button', { name: 'Skills', exact: true }).first().click();
  await expect(page).toHaveURL(`/agents/${detail.id}`);
  releasePatch();
  await expect(page.getByRole('complementary', { name: 'Agent inspector' })).toContainText('Draft name (server)');
  await expect(instructionsPanel.getByLabel('Name', { exact: true })).toHaveValue('Draft name (server)');
  await expect(instructionsPanel.getByRole('button', { name: 'Save agent' })).toBeDisabled();
});

test('agent activity is read-only run history', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000042' });
  const historicalRun = {
    id: '60000000-0000-0000-0000-000000000042', agent_id: agent.id, automation_id: null,
    integration_session_id: null, parent_run_id: null, runtime_id: null, status: 'completed',
    initial_message: 'Historical run from a previous conversation', native_session_id: 'session-history',
    work_dir_ref: null, source: 'console', created_at: agent.created_at, updated_at: agent.updated_at
  };
  await installDetailApi(page, { agent, runs: [historicalRun] });
  await page.goto(`/agents/${agent.id}`);
  const activity = page.getByRole('tabpanel', { name: 'Activity' });
  await expect(activity.getByText(historicalRun.initial_message)).toBeVisible();
  await expect(activity.getByText('Console result')).toBeVisible();
  await expect(activity.getByRole('button', { name: 'Start run' })).toHaveCount(0);
  await expect(activity.getByLabel('Message', { exact: true })).toHaveCount(0);
  await expect(activity.getByRole('checkbox', { name: 'Continue selected thread' })).toHaveCount(0);
});

test('agent detail controls match manage and administer permissions while activity remains read-only', async ({ page }) => {
  const cases = [
    { id: '50000000-0000-0000-0000-000000000011', owner: true, manage: true, administer: true, invoke: true },
    { id: '50000000-0000-0000-0000-000000000012', owner: false, manage: true, administer: true, invoke: false },
    { id: '50000000-0000-0000-0000-000000000013', owner: false, manage: false, administer: true, invoke: false },
    { id: '50000000-0000-0000-0000-000000000014', owner: false, manage: false, administer: false, invoke: true }
  ];
  for (const permission of cases) {
    await page.unroute('**/api/**').catch(() => undefined);
    const agent = ownerDetail({
      id: permission.id,
      name: `Permission ${permission.id.at(-1)}`,
      is_owner: permission.owner,
      can_manage: permission.manage,
      can_administer: permission.administer,
      can_invoke: permission.invoke
    });
    await installDetailApi(page, { agent });
    await page.goto(`/agents/${agent.id}`);
    await expect(page.getByRole('tablist', { name: 'Agent detail sections' }).getByRole('tab')).toHaveCount(6);
    await expect(page.getByRole('button', { name: 'Delete agent', exact: true })).toHaveCount(permission.administer ? 1 : 0);
    await expect(page.getByRole('tabpanel', { name: 'Activity' }).getByRole('button', { name: 'Start run' })).toHaveCount(0);
    await expect(page.getByRole('tabpanel', { name: 'Activity' }).getByLabel('Message', { exact: true })).toHaveCount(0);
    await page.getByRole('tab', { name: 'Instructions' }).click();
    await expect(page.getByRole('tabpanel', { name: 'Instructions' }).getByLabel('Name', { exact: true })).toHaveCount(permission.manage ? 1 : 0);
    await page.getByRole('tab', { name: 'Models' }).click();
    await expect(page.getByRole('tabpanel', { name: 'Models' }).getByLabel('Model API Connection and model')).toHaveCount(permission.manage ? 1 : 0);
    await page.getByRole('tab', { name: 'Skills' }).click();
    await expect(page.getByRole('tabpanel', { name: 'Skills' }).getByRole('button', { name: 'Edit managed skills' })).toHaveCount(permission.manage ? 1 : 0);
    await page.getByRole('tab', { name: 'MCP', exact: true }).click();
    await expect(page.getByRole('tabpanel', { name: 'MCP' }).getByRole('button', { name: 'Add MCP entry' })).toHaveCount(permission.manage ? 1 : 0);
    await page.getByRole('tab', { name: 'Access' }).click();
    await expect(page.getByRole('tabpanel', { name: 'Access' }).getByLabel('Visibility')).toHaveCount(permission.manage ? 1 : 0);
  }
});

test('agent tabs keep real instructions, access, and read-only activity history', async ({ page }) => {
  const patches: Record<string, unknown>[] = [];
  const agent = ownerDetail();
  const historicalRun = {
    id: '60000000-0000-0000-0000-000000000043', agent_id: agent.id, automation_id: null,
    integration_session_id: null, parent_run_id: null, runtime_id: null, status: 'completed',
    initial_message: 'Existing activity history', native_session_id: 'history-session', work_dir_ref: null,
    source: 'console', created_at: agent.created_at, updated_at: agent.updated_at
  };
  await installDetailApi(page, {
    agent,
    runs: [historicalRun],
    onPatch: async (route, body) => {
      patches.push(body);
      await route.fulfill({ json: { ...agent, ...body, updated_at: '2026-07-11T07:00:00.000Z' } });
    }
  });
  await page.goto(`/agents/${agent.id}`);

  await page.getByRole('tab', { name: 'Instructions' }).click();
  let panel = page.getByRole('tabpanel', { name: 'Instructions' });
  await panel.getByLabel('Name', { exact: true }).fill('Edited detail agent');
  await panel.getByLabel('Instructions').fill('Edited instructions.');
  await panel.getByRole('button', { name: 'Save agent' }).click();

  await page.getByRole('tab', { name: 'Access' }).click();
  panel = page.getByRole('tabpanel', { name: 'Access' });
  await panel.getByLabel('Visibility').selectOption('public');
  await panel.getByLabel('Runtime binding').selectOption(runtimes[1].id);
  await panel.getByRole('button', { name: 'Save agent' }).click();

  await page.getByRole('tab', { name: 'Activity' }).click();
  panel = page.getByRole('tabpanel', { name: 'Activity' });
  await expect(panel.getByText(historicalRun.initial_message)).toBeVisible();
  await expect(panel.getByText('Console result')).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Start run' })).toHaveCount(0);

  expect(patches).toHaveLength(2);
  expect(patches[0]).toMatchObject({ name: 'Edited detail agent', instructions: 'Edited instructions.' });
  expect(patches[1]).toMatchObject({ visibility: 'public', public_to: [], runtime_id: runtimes[1].id });
});

test('agent route changes abort stale reads and never restore the previous detail', async ({ page }) => {
  const first = ownerDetail({ id: '50000000-0000-0000-0000-000000000021', name: 'Delayed first agent' });
  const second = ownerDetail({ id: '50000000-0000-0000-0000-000000000022', name: 'Current second agent' });
  let releaseFirst!: () => void;
  const heldFirst = new Promise<void>((resolve) => { releaseFirst = resolve; });
  let firstStarted!: () => void;
  const started = new Promise<void>((resolve) => { firstStarted = resolve; });
  const failedRequests: string[] = [];
  page.on('requestfailed', (request) => failedRequests.push(new URL(request.url()).pathname));
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: currentUser });
    if (path === `/api/agents/${first.id}`) {
      firstStarted();
      await heldFirst;
      return route.fulfill({ json: first }).catch(() => undefined);
    }
    if (path === `/api/agents/${second.id}`) return route.fulfill({ json: second });
    if (path === `/api/agents/${first.id}/model-options` || path === `/api/agents/${second.id}/model-options`) return route.fulfill({ json: modelOptions });
    if (path.endsWith('/runs')) return route.fulfill({ json: [] });
    if (path === '/api/runtimes') return route.fulfill({ json: runtimes });
    if (path === '/api/skills') return route.fulfill({ json: detailSkills });
    if (path === '/api/users') return route.fulfill({ json: [currentUser] });
    return route.fulfill({ status: 404, json: { error: 'unhandled' } });
  });

  await page.goto(`/agents/${first.id}`);
  await started;
  await page.evaluate((agentId) => {
    window.history.pushState({ __agentHubHistoryIndex: 1 }, '', `/agents/${agentId}`);
    window.dispatchEvent(new PopStateEvent('popstate', { state: window.history.state }));
  }, second.id);
  await expect(page.getByText(second.name, { exact: true }).first()).toBeVisible();
  await expect(page.getByText(first.name, { exact: true })).toHaveCount(0);
  releaseFirst();
  await page.waitForTimeout(50);
  await expect(page.getByText(second.name, { exact: true }).first()).toBeVisible();
  await expect(page.getByText(first.name, { exact: true })).toHaveCount(0);
  expect(failedRequests.filter((path) => path === `/api/agents/${first.id}`).length).toBeLessThanOrEqual(1);
});

test('dirty detail confirms browser back and logout before leaving', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000031' });
  let logoutRequests = 0;
  await installDetailApi(page, { agent });
  await page.route('**/api/auth/logout', async (route) => {
    logoutRequests += 1;
    await route.fulfill({ status: 204 });
  });
  await page.goto('/agents');
  await page.getByRole('row', { name: new RegExp(agent.name) }).click();
  await page.getByRole('tab', { name: 'Instructions' }).click();
  await page.getByRole('tabpanel', { name: 'Instructions' }).getByLabel('Instructions').fill('Dirty navigation draft');

  page.once('dialog', (dialog) => dialog.dismiss());
  await page.goBack({ waitUntil: 'commit' }).catch(() => null);
  await expect(page).toHaveURL(`/agents/${agent.id}`);
  await expect(page.getByRole('tabpanel', { name: 'Instructions' }).getByRole('textbox', { name: 'Instructions' })).toHaveText('Dirty navigation draft');

  page.once('dialog', (dialog) => dialog.dismiss());
  await page.getByRole('button', { name: 'Log out' }).click();
  await expect(page).toHaveURL(`/agents/${agent.id}`);
  expect(logoutRequests).toBe(0);

  page.once('dialog', (dialog) => dialog.accept());
  await page.getByRole('button', { name: 'Log out' }).click();
  await expect(page).toHaveURL('/login');
  expect(logoutRequests).toBe(1);
});

test('agent pages localize and stay overflow-free at 1280, 1440, and 390 pixels', async ({ page }) => {
  const agent = ownerDetail({ id: '50000000-0000-0000-0000-000000000051' });
  const localizedRun = {
    id: '60000000-0000-0000-0000-000000000051', agent_id: agent.id, automation_id: null,
    integration_session_id: null, parent_run_id: null, runtime_id: runtimes[0].id, status: 'completed',
    initial_message: 'Localized run', native_session_id: null, work_dir_ref: null, source: 'integration:tool_result',
    created_at: agent.created_at, updated_at: agent.updated_at
  };
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const requestFailures: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('console', (message) => { if (message.type() === 'error') consoleErrors.push(message.text()); });
  page.on('requestfailed', (request) => requestFailures.push(`${request.method()} ${new URL(request.url()).pathname}`));
  await installDetailApi(page, { agent, runs: [localizedRun] });

  for (const width of [1280, 1440, 390]) {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await page.goto(`/agents/${agent.id}`);
    await expect(page.getByRole('tab', { name: 'Activity' })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth)).toBeLessThanOrEqual(0);
    const sidebarPosition = await page.locator('.sidebar').evaluate((element) => getComputedStyle(element).position);
    expect(sidebarPosition).toBe(width === 390 ? 'sticky' : 'fixed');
  }

  const runRow = page.locator(`[data-run-id="${localizedRun.id}"]`);
  await expect(runRow).toContainText('Integration tool result');
  await page.getByRole('tab', { name: 'Access' }).click();
  await expect(page.getByRole('tabpanel', { name: 'Access' }).getByLabel('Runtime binding').locator('option').nth(1)).toContainText('online');
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByRole('tablist', { name: '智能体详情分区' }).getByRole('tab')).toHaveText(['活动', '指令', '模型', '技能', 'MCP', '访问权限']);
  await page.getByRole('tab', { name: '活动' }).click();
  await expect(runRow).toContainText('已完成');
  await expect(runRow).toContainText('集成工具结果');
  await expect(runRow).not.toContainText('completed');
  await expect(runRow).not.toContainText('integration:tool_result');
  await page.getByRole('tab', { name: '访问权限' }).click();
  const runtimeOption = page.getByRole('tabpanel', { name: '访问权限' }).getByLabel('运行节点').locator('option').nth(1);
  await expect(runtimeOption).toContainText('在线');
  await expect(runtimeOption).not.toContainText('online');
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
});
