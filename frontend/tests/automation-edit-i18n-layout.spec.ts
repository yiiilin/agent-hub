import { expect, request, test } from '@playwright/test';
import type { AgentModelSettings } from '../src/api/client';
import { selectLocalPasswordLogin } from './authentication-helpers';

const automaticModelSettings: AgentModelSettings = {
  reasoning_effort: 'default',
  reasoning_summary: 'default',
  verbosity: 'default',
  context_window_tokens: null,
  auto_compact_token_limit: null,
  reasoning_summary_support: 'auto',
  service_tier: null,
  provider_request_timeout_ms: null,
  stream_max_retries: null,
  stream_idle_timeout_ms: null,
  request_settings: { protocol: 'openai_responses' }
};

type OwnedAgentFixture = {
  agent: { id: string; name: string; owner_id: string; [key: string]: unknown };
  modelConnectionId: string;
};

async function createOwnedAgentFixture(page: import('@playwright/test').Page, label: string): Promise<OwnedAgentFixture> {
  const suffix = `${Date.now()}-${test.info().workerIndex}`;
  const modelResponse = await page.request.post('/api/model-connections', { data: {
    scope: 'personal',
    name: `${label} model ${suffix}`,
    base_url: 'http://fake-model-provider:8080',
    api_type: 'openai_responses',
    allowed_model_ids: ['hub-proxy-smoke'],
    api_key: 'dev-model-provider-api-key'
  } });
  expect(modelResponse.ok()).toBeTruthy();
  const model = await modelResponse.json() as { id: string };
  const agentResponse = await page.request.post('/api/agents', { data: {
    name: `${label} agent ${suffix}`,
    instructions: 'Own the Automation test fixtures.',
    visibility: 'private',
    public_to: [],
    model_selection: { connection_id: model.id, model_id: 'hub-proxy-smoke' },
    model_settings: automaticModelSettings,
    subagents: []
  } });
  expect(agentResponse.ok()).toBeTruthy();
  return {
    agent: await agentResponse.json() as OwnedAgentFixture['agent'],
    modelConnectionId: model.id
  };
}

async function cleanupOwnedAgentFixture(page: import('@playwright/test').Page, fixture: OwnedAgentFixture | null) {
  if (!fixture) return;
  const agentResponse = await page.request.delete(`/api/agents/${fixture.agent.id}`);
  expect.soft([204, 404]).toContain(agentResponse.status());
  const modelResponse = await page.request.delete(`/api/model-connections/${fixture.modelConnectionId}`);
  expect.soft([204, 404]).toContain(modelResponse.status());
}

async function createAutomationFixture(
  page: import('@playwright/test').Page,
  agentId: string,
  name: string,
  triggerType: 'manual' | 'webhook' | 'interval' | 'cron',
  schedule: string | null = null
) {
  const response = await page.request.post('/api/automations', { data: {
    agent_id: agentId,
    name,
    trigger_type: triggerType,
    prompt: `${name} prompt`,
    schedule,
    enabled: true
  } });
  const body = await response.text();
  expect(response.ok(), `create automation failed: ${response.status()} ${body}`).toBeTruthy();
  return JSON.parse(body) as { id: string };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

async function installNoopEventSource(page: import('@playwright/test').Page) {
  await page.addInitScript(() => {
    class NoopEventSource {
      static readonly CLOSED = 2;
      readonly readyState = 1;
      constructor(_url: string | URL, _init?: EventSourceInit) {}
      addEventListener() {}
      close() { Object.defineProperty(this, 'readyState', { value: NoopEventSource.CLOSED }); }
    }
    Object.defineProperty(window, 'EventSource', { configurable: true, value: NoopEventSource });
  });
}

function automationRun(id: string, automationId: string, status: string, message: string, source = 'automation:manual') {
  const created = '2026-07-11T08:00:00.000Z';
  return {
    id,
    agent_id: '10000000-0000-4000-8000-000000000001',
    automation_id: automationId,
    integration_session_id: null,
    parent_run_id: null,
    runtime_id: null,
    status,
    initial_message: message,
    native_session_id: null,
    work_dir_ref: null,
    source,
    created_at: created,
    updated_at: '2026-07-11T08:01:00.000Z'
  };
}

test('automation history supports errors, retry, pagination, failed logs, empty state, and mobile layout', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = '10000000-0000-4000-8000-000000000010';
  const agentId = '10000000-0000-4000-8000-000000000001';
  const automationA = '20000000-0000-4000-8000-000000000001';
  const automationB = '20000000-0000-4000-8000-000000000002';
  const runId = '30000000-0000-4000-8000-000000000001';
  const automations = [
    { id: automationA, agent_id: agentId, owner_id: ownerId, name: 'History A', trigger_type: 'manual', prompt: 'A', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' },
    { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'History B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' }
  ];
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'history@example.com', display_name: 'History', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'History agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => route.fulfill({ json: automations }));
  let historyAttempts = 0;
  await page.route('**/api/automations/*/runs?*', async (route) => {
    const url = new URL(route.request().url());
    const automationId = url.pathname.split('/')[3];
    if (automationId === automationB) return route.fulfill({ json: { items: [], total: 0, page: 1, page_size: 20 } });
    historyAttempts += 1;
    if (historyAttempts === 1) return route.fulfill({ status: 503, json: { error: 'history unavailable' } });
    const pageNumber = Number(url.searchParams.get('page'));
    return route.fulfill({ json: {
      items: [automationRun(runId, automationA, 'failed', pageNumber === 1 ? 'Failed history' : 'Second page', 'integration:tool_result')],
      total: 21,
      page: pageNumber,
      page_size: 20
    } });
  });
  let eventAttempts = 0;
  await page.route(`**/api/runs/${runId}/events`, (route) => {
    eventAttempts += 1;
    return eventAttempts === 1
      ? route.fulfill({ status: 500, json: { error: 'events unavailable' } })
      : route.fulfill({ json: [{ seq: 1, event_id: '40000000-0000-4000-8000-000000000001', run_id: runId, event_type: 'status', role: null, content: 'failed', payload: { status: 'failed', error: 'provider quota exhausted' }, created_at: '2026-07-11T08:01:00Z' }] });
  });

  await page.goto('/automations');
  await page.getByRole('button', { name: /^History A\b/ }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('Unable to load run history. Retry.');
  await page.getByRole('region', { name: 'Run history' }).getByRole('button', { name: 'Retry' }).click();
  const historyRow = page.getByRole('button', { name: /failed.*Integration tool result.*Failed history/i });
  await expect(historyRow).toContainText('Created');
  await expect(historyRow).toContainText('Updated');
  await historyRow.click();
  await expect(page.getByRole('region', { name: 'Run events' })).toContainText('Unable to load run events. Retry.');
  await page.getByRole('region', { name: 'Run events' }).getByRole('button', { name: 'Retry' }).click();
  await expect(page.getByRole('region', { name: 'Run events' })).toContainText('provider quota exhausted');
  await page.getByRole('region', { name: 'Run history' }).getByRole('button', { name: 'Next' }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('Page 2 of 2');
  await page.getByRole('button', { name: /^History B\b/ }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('No runs yet.');
  await expect(page.getByRole('region', { name: 'Run history' })).not.toContainText('Page 2 of 2');
  await expect(page.getByRole('region', { name: 'Run events' })).toHaveCount(0);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)).toBe(false);
  await page.getByRole('button', { name: /^History A\b/ }).click();
  const localizedHistoryRow = page.locator(`[data-run-id="${runId}"]`);
  await expect(localizedHistoryRow).toContainText('Integration tool result');
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(localizedHistoryRow).toContainText('集成工具结果');
  await expect(localizedHistoryRow).not.toContainText('integration:tool_result');
});

test('automation history polling is serial and ignores a delayed previous selection', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = '50000000-0000-4000-8000-000000000010';
  const agentId = '50000000-0000-4000-8000-000000000001';
  const automationA = '60000000-0000-4000-8000-000000000001';
  const automationB = '60000000-0000-4000-8000-000000000002';
  const pollStarted = deferred();
  const releasePoll = deferred();
  const pollCompleted = deferred();
  let aRequests = 0;
  let aInFlight = 0;
  let maxAInFlight = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'poll@example.com', display_name: 'Poll', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Poll agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => route.fulfill({ json: [
    { id: automationA, agent_id: agentId, owner_id: ownerId, name: 'Polling A', trigger_type: 'manual', prompt: 'A', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' },
    { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'Polling B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' }
  ] }));
  await page.route('**/api/automations/*/runs?*', async (route) => {
    const automationId = new URL(route.request().url()).pathname.split('/')[3];
    if (automationId === automationB) return route.fulfill({ json: { items: [], total: 0, page: 1, page_size: 20 } });
    aInFlight += 1;
    maxAInFlight = Math.max(maxAInFlight, aInFlight);
    aRequests += 1;
    const requestNumber = aRequests;
    try {
      if (requestNumber === 2) {
        pollStarted.resolve();
        await releasePoll.promise;
      }
      return await route.fulfill({ json: { items: [automationRun(`70000000-0000-4000-8000-00000000000${requestNumber}`, automationA, 'running', `A request ${requestNumber}`)], total: 1, page: 1, page_size: 20 } });
    } finally {
      if (requestNumber === 2) pollCompleted.resolve();
      aInFlight -= 1;
    }
  });

  await page.goto('/automations');
  await page.getByRole('button', { name: /^Polling A\b/ }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('A request 1');
  await pollStarted.promise;
  await page.getByRole('button', { name: /^Polling B\b/ }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('No runs yet.');
  releasePoll.resolve();
  await pollCompleted.promise;
  await expect(page.getByRole('region', { name: 'Run history' })).not.toContainText('A request 2');
  expect(maxAInFlight).toBe(1);
});

test('triggering B from A page two selects B page one and reselecting B preserves history', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = '81000000-0000-4000-8000-000000000010';
  const agentId = '81000000-0000-4000-8000-000000000001';
  const automationA = '82000000-0000-4000-8000-000000000001';
  const automationB = '82000000-0000-4000-8000-000000000002';
  const runA = automationRun('83000000-0000-4000-8000-000000000001', automationA, 'completed', 'A history');
  const runB = automationRun('83000000-0000-4000-8000-000000000002', automationB, 'completed', 'B triggered');
  const automations = [
    { id: automationA, agent_id: agentId, owner_id: ownerId, name: 'Trigger A', trigger_type: 'manual', prompt: 'A', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' },
    { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'Trigger B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' }
  ];
  let bTriggered = false;
  const bHistoryPages: number[] = [];
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'trigger@example.com', display_name: 'Trigger', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Trigger agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => route.fulfill({ json: automations }));
  await page.route(`**/api/automations/${automationB}/trigger`, (route) => {
    bTriggered = true;
    return route.fulfill({ json: runB });
  });
  await page.route('**/api/automations/*/runs?*', (route) => {
    const url = new URL(route.request().url());
    const automationId = url.pathname.split('/')[3];
    const pageNumber = Number(url.searchParams.get('page'));
    if (automationId === automationB) {
      bHistoryPages.push(pageNumber);
      return route.fulfill({ json: { items: bTriggered ? [runB] : [], total: bTriggered ? 1 : 0, page: pageNumber, page_size: 20 } });
    }
    return route.fulfill({ json: { items: [runA], total: 21, page: pageNumber, page_size: 20 } });
  });

  await page.goto('/automations');
  await page.getByRole('button', { name: /^Trigger A\b/ }).click();
  await page.getByRole('region', { name: 'Run history' }).getByRole('button', { name: 'Next' }).click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('Page 2 of 2');
  const automationBRow = page.locator('.automation-list-row').filter({ hasText: 'Trigger B' });
  await automationBRow.getByRole('button', { name: 'Run now' }).click();
  await expect(page.getByRole('region', { name: 'Details' }).getByRole('heading', { name: 'Trigger B' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('B triggered');
  await expect(page.getByRole('region', { name: 'Run history' })).not.toContainText('Page 2 of 2');
  expect(bHistoryPages).toContain(1);
  await automationBRow.locator('.automation-select').click();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('B triggered');
});

test('successful trigger keeps B history when the following Automation list refresh fails', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = '91000000-0000-4000-8000-000000000010';
  const agentId = '91000000-0000-4000-8000-000000000001';
  const automationA = '92000000-0000-4000-8000-000000000001';
  const automationB = '92000000-0000-4000-8000-000000000002';
  const runB = automationRun('93000000-0000-4000-8000-000000000002', automationB, 'completed', 'B survived refresh failure');
  const automations = [
    { id: automationA, agent_id: agentId, owner_id: ownerId, name: 'Refresh A', trigger_type: 'manual', prompt: 'A', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' },
    { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'Refresh B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' }
  ];
  let listRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'refresh@example.com', display_name: 'Refresh', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Refresh agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => {
    listRequests += 1;
    return listRequests === 1 ? route.fulfill({ json: automations }) : route.fulfill({ status: 500, json: { error: 'list unavailable' } });
  });
  await page.route(`**/api/automations/${automationB}/trigger`, (route) => route.fulfill({ json: runB }));
  await page.route('**/api/automations/*/runs?*', (route) => {
    const automationId = new URL(route.request().url()).pathname.split('/')[3];
    return route.fulfill({ json: { items: automationId === automationB ? [runB] : [], total: automationId === automationB ? 1 : 0, page: 1, page_size: 20 } });
  });

  await page.goto('/automations');
  await page.locator('.automation-list-row').filter({ hasText: 'Refresh B' }).getByRole('button', { name: 'Run now' }).click();
  await expect(page.getByRole('alert')).toContainText('Unable to load automations. Retry.');
  await expect(page.getByRole('alert')).not.toContainText('Unable to run automation. Retry.');
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('B survived refresh failure');
});

test('canceling an edit restores form fields without clearing terminal history or the selected RunConsole', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = 'a1000000-0000-4000-8000-000000000010';
  const agentId = 'a1000000-0000-4000-8000-000000000001';
  const automationId = 'a2000000-0000-4000-8000-000000000001';
  const runId = 'a3000000-0000-4000-8000-000000000001';
  const run = automationRun(runId, automationId, 'completed', 'Discard history');
  const automation = { id: automationId, agent_id: agentId, owner_id: ownerId, name: 'Discard A', trigger_type: 'manual', prompt: 'Saved prompt', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' };
  let historyRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'discard@example.com', display_name: 'Discard', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Discard agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => route.fulfill({ json: [automation] }));
  await page.route(`**/api/automations/${automationId}/runs?*`, (route) => {
    historyRequests += 1;
    return route.fulfill({ json: { items: [run], total: 1, page: 1, page_size: 20 } });
  });
  await page.route(`**/api/runs/${runId}/events`, (route) => route.fulfill({ json: [{ seq: 1, event_id: 'a4000000-0000-4000-8000-000000000001', run_id: runId, event_type: 'message', role: 'assistant', content: 'Discard console event', payload: {}, created_at: '2026-07-11T08:01:00Z' }] }));

  await page.goto('/automations');
  await page.getByRole('button', { name: /^Discard A\b/ }).click();
  const historyRow = page.getByRole('region', { name: 'Run history' }).locator(`[data-run-id="${runId}"]`);
  await historyRow.click();
  await expect(page.getByRole('region', { name: 'Run events' })).toContainText('Discard console event');
  expect(historyRequests).toBe(1);
  const editButton = page.getByRole('button', { name: 'Edit Automation Discard A' });
  await editButton.click();
  let editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
  await editDialog.getByLabel('Name').fill('Unsaved name');
  await editDialog.getByRole('textbox', { name: 'Prompt' }).fill('Unsaved prompt');
  await editDialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(editDialog).toHaveCount(0);
  await editButton.click();
  editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
  await expect(editDialog.getByLabel('Name')).toHaveValue('Discard A');
  await expect(editDialog.getByRole('textbox', { name: 'Prompt' })).toContainText('Saved prompt');
  await editDialog.getByRole('button', { name: 'Cancel', exact: true }).click();
  await expect(historyRow).toContainText('Discard history');
  await expect(page.getByRole('region', { name: 'Run events' })).toContainText('Discard console event');
  expect(historyRequests).toBe(1);
});

test('a delayed B trigger does not replace C selected while the request is pending', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = 'b1000000-0000-4000-8000-000000000010';
  const agentId = 'b1000000-0000-4000-8000-000000000001';
  const automationB = 'b2000000-0000-4000-8000-000000000002';
  const automationC = 'b2000000-0000-4000-8000-000000000003';
  const runB = automationRun('b3000000-0000-4000-8000-000000000002', automationB, 'completed', 'Delayed B');
  const automations = [
    { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'Delayed B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' },
    { id: automationC, agent_id: agentId, owner_id: ownerId, name: 'Selected C', trigger_type: 'manual', prompt: 'C', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' }
  ];
  const triggerStarted = deferred();
  const releaseTrigger = deferred();
  let bHistoryRequests = 0;
  let listRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'race-c@example.com', display_name: 'Race C', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Race agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => { listRequests += 1; return route.fulfill({ json: automations }); });
  await page.route(`**/api/automations/${automationB}/trigger`, async (route) => {
    triggerStarted.resolve();
    await releaseTrigger.promise;
    return route.fulfill({ json: runB });
  });
  await page.route('**/api/automations/*/runs?*', (route) => {
    const automationId = new URL(route.request().url()).pathname.split('/')[3];
    if (automationId === automationB) bHistoryRequests += 1;
    return route.fulfill({ json: { items: [], total: 0, page: 1, page_size: 20 } });
  });

  await page.goto('/automations');
  await page.locator('.automation-list-row').filter({ hasText: 'Delayed B' }).getByRole('button', { name: 'Run now' }).click();
  await triggerStarted.promise;
  await page.getByRole('button', { name: /^Selected C\b/ }).click();
  await expect(page.getByRole('region', { name: 'Details' }).getByRole('heading', { name: 'Selected C' })).toBeVisible();
  releaseTrigger.resolve();
  await expect.poll(() => listRequests).toBe(2);
  await expect(page.getByRole('region', { name: 'Details' }).getByRole('heading', { name: 'Selected C' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Run history' })).toContainText('No runs yet.');
  expect(bHistoryRequests).toBe(0);
});

test('a delayed B trigger does not close or replace the New automation draft', async ({ page }) => {
  await installNoopEventSource(page);
  const ownerId = 'c1000000-0000-4000-8000-000000000010';
  const agentId = 'c1000000-0000-4000-8000-000000000001';
  const automationB = 'c2000000-0000-4000-8000-000000000002';
  const runB = automationRun('c3000000-0000-4000-8000-000000000002', automationB, 'completed', 'Delayed B');
  const automation = { id: automationB, agent_id: agentId, owner_id: ownerId, name: 'Delayed New B', trigger_type: 'manual', prompt: 'B', schedule: null, webhook_token: null, enabled: true, last_triggered_at: null, created_at: '2026-07-11T07:00:00Z' };
  const triggerStarted = deferred();
  const releaseTrigger = deferred();
  let listRequests = 0;
  await page.route('**/api/auth/me', (route) => route.fulfill({ json: { id: ownerId, email: 'race-new@example.com', display_name: 'Race New', role: 'member' } }));
  await page.route('**/api/agents', (route) => route.fulfill({ json: [{ id: agentId, owner_id: ownerId, name: 'Race agent', instructions: '', visibility: 'private', public_to: [], runtime_id: null, is_owner: true, can_manage: true, can_administer: true, can_invoke: true, model_policy: {}, sandbox_policy: {}, skills_manifest: [], managed_skill_ids: [], mcp_allowlist: [], created_at: '2026-07-11T07:00:00Z', updated_at: '2026-07-11T07:00:00Z' }] }));
  await page.route('**/api/automations', (route) => { listRequests += 1; return route.fulfill({ json: [automation] }); });
  await page.route(`**/api/automations/${automationB}/trigger`, async (route) => {
    triggerStarted.resolve();
    await releaseTrigger.promise;
    return route.fulfill({ json: runB });
  });
  await page.route('**/api/automations/*/runs?*', (route) => {
    return route.fulfill({ json: { items: [], total: 0, page: 1, page_size: 20 } });
  });

  await page.goto('/automations');
  await page.locator('.automation-list-row').filter({ hasText: 'Delayed New B' }).getByRole('button', { name: 'Run now' }).click();
  await triggerStarted.promise;
  await page.getByRole('button', { name: 'New automation' }).click();
  const createDialog = page.getByRole('dialog', { name: 'Create Automation' });
  await createDialog.getByLabel('Name').fill('Preserved draft');
  releaseTrigger.resolve();
  await expect.poll(() => listRequests).toBe(2);
  await expect(createDialog).toBeVisible();
  await expect(createDialog.getByLabel('Name')).toHaveValue('Preserved draft');
});

test('edits automations, protects webhook tokens, and localizes the operations workspace', async ({ page, baseURL }) => {
  const browserErrors: string[] = [];
  const serverErrors: string[] = [];
  let fixture: OwnedAgentFixture | null = null;
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Email').fill('admin@example.com');
  await page.getByLabel('Password').fill('admin123');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page).toHaveURL(/\/sessions$/);
  page.on('console', (message) => { if (message.type() === 'error') browserErrors.push(message.text()); });
  page.on('pageerror', (error) => browserErrors.push(error.message));
  page.on('response', (response) => { if (response.status() >= 500) serverErrors.push(`${response.status()} ${response.url()}`); });

  try {
    fixture = await createOwnedAgentFixture(page, 'Automation workspace');
    const suffix = Date.now();
    const manualName = `Manual fixture ${suffix}`;
    const webhookName = `Webhook fixture ${suffix}`;
    const intervalName = `Interval fixture ${suffix}`;
    const cronName = `Cron fixture ${suffix}`;
    const webhook = await createAutomationFixture(page, fixture.agent.id, webhookName, 'webhook');
    const interval = await createAutomationFixture(page, fixture.agent.id, intervalName, 'interval', '5m');
    const cron = await createAutomationFixture(page, fixture.agent.id, cronName, 'cron', '0 9 * * 1');

    await page.goto('/automations');
    const list = page.getByRole('region', { name: 'List' });
    await expect(list).toBeVisible();
    await expect(page.getByRole('dialog')).toHaveCount(0);
    await page.getByRole('button', { name: 'New automation' }).click();
    const createDialog = page.getByRole('dialog', { name: 'Create Automation' });
    await expect(createDialog).toBeVisible();
    await expect(list).toBeVisible();
    await createDialog.getByLabel('Agent').selectOption(fixture.agent.id);
    await expect(createDialog.getByLabel('Agent')).toHaveValue(fixture.agent.id);
    await createDialog.getByLabel('Name').fill(manualName);
    await createDialog.getByRole('textbox', { name: 'Prompt' }).fill('Original prompt');
    const createResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/automations');
    await createDialog.getByRole('button', { name: 'Create automation' }).click();
    const createdResponse = await createResponse;
    expect(createdResponse.ok()).toBeTruthy();
    const created = await createdResponse.json() as { id: string };
    await expect(createDialog).toHaveCount(0);
    await expect(page.getByText('Changes saved')).toBeVisible();

    const manualRow = page.locator(`[data-automation-id="${created.id}"]`);
    const webhookRow = page.locator(`[data-automation-id="${webhook.id}"]`);
    const intervalRow = page.locator(`[data-automation-id="${interval.id}"]`);
    const cronRow = page.locator(`[data-automation-id="${cron.id}"]`);
    await expect(manualRow).toContainText('Manual');
    await expect(webhookRow).toContainText('Webhook');
    await expect(intervalRow).toContainText('Interval');
    await expect(cronRow).toContainText('Cron');
    await expect(manualRow.locator('.automation-trigger-config')).toContainText('None');
    await expect(webhookRow.locator('.automation-trigger-config')).toContainText('/api/automations/webhook');
    await expect(intervalRow.locator('.automation-trigger-config')).toContainText('5m');
    await expect(cronRow.locator('.automation-trigger-config')).toContainText('0 9 * * 1');
    await expect(manualRow.getByRole('button', { name: 'Run now' })).toBeVisible();
    for (const row of [webhookRow, intervalRow, cronRow]) {
      await expect(row.getByRole('button', { name: 'Run now' })).toHaveCount(0);
    }

    await page.setViewportSize({ width: 1440, height: 960 });
    const rowMeasurements = await Promise.all([manualRow, webhookRow, intervalRow, cronRow].map(async (row) => {
      const box = await row.boundingBox();
      expect(box).not.toBeNull();
      return box!;
    }));
    expect(Math.max(...rowMeasurements.map((box) => box.height)) - Math.min(...rowMeasurements.map((box) => box.height))).toBeLessThanOrEqual(1);
    expect(Math.max(...rowMeasurements.map((box) => box.width)) - Math.min(...rowMeasurements.map((box) => box.width))).toBeLessThanOrEqual(1);

    await manualRow.getByRole('button', { name: `Edit Automation ${manualName}` }).click();
    let editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
    await expect(editDialog.getByLabel('Agent')).toBeDisabled();
    await editDialog.getByLabel('Name').fill(`Edited ${suffix}`);
    await editDialog.getByRole('textbox', { name: 'Prompt' }).fill('Edited prompt from browser');
    await editDialog.getByLabel('Trigger').selectOption('interval');
    await editDialog.getByLabel('Schedule').fill('15m');
    await editDialog.getByLabel('Enabled').uncheck();
    await editDialog.getByRole('button', { name: 'Save changes' }).click();
    await expect(page.getByText('Changes saved')).toBeVisible();
    await page.reload();
    await page.getByRole('button', { name: `Edit Automation Edited ${suffix}` }).click();
    editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
    await expect(editDialog.getByRole('textbox', { name: 'Prompt' })).toContainText('Edited prompt from browser');
    await expect(editDialog.getByLabel('Schedule')).toHaveValue('15m');
    await expect(editDialog.getByLabel('Enabled')).not.toBeChecked();

    await editDialog.getByLabel('Enabled').check();
    await editDialog.getByLabel('Trigger').selectOption('webhook');
    await editDialog.getByRole('button', { name: 'Save changes' }).click();
    const secretDialog = page.getByRole('dialog', { name: 'One-time webhook token' });
    const token = await secretDialog.getByTestId('webhook-token').textContent();
    expect(token).toMatch(/^ahw_/);
    const listed = await (await page.request.get('/api/automations')).json() as Array<{ id: string; webhook_token: string | null }>;
    expect(listed.find((item) => item.id === created.id)?.webhook_token).toBeNull();
    await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
    await page.getByRole('button', { name: `Edit Automation Edited ${suffix}` }).click();
    await page.getByRole('dialog', { name: 'Edit Automation' }).getByRole('button', { name: 'Save changes' }).click();
    await expect(page.getByTestId('webhook-token')).toHaveCount(0);
    const anonymous = await request.newContext({ baseURL });
    try {
      expect((await anonymous.post('/api/automations/webhook', {
        headers: { 'X-Agent-Hub-Webhook-Token': token! }, data: {}
      })).ok()).toBeTruthy();
    } finally {
      await anonymous.dispose();
    }

    const foreign = await request.newContext({ baseURL });
    try {
      const foreignEmail = `foreign-${suffix}@example.com`;
      const foreignPassword = 'foreign-password';
      const createdUser = await page.request.post('/api/admin/users', { data: {
        email: foreignEmail, password: foreignPassword, role: 'member'
      } });
      expect(createdUser.ok()).toBeTruthy();
      const login = await foreign.post('/api/auth/login', { data: {
        email: foreignEmail, password: foreignPassword
      } });
      expect(login.ok()).toBeTruthy();
      const foreignUpdate = await foreign.patch(`/api/automations/${created.id}`, { data: {
        name: 'Foreign', trigger_type: 'manual', prompt: 'Foreign', schedule: null, enabled: true
      } });
      expect(foreignUpdate.status()).toBe(404);
    } finally {
      await foreign.dispose();
    }

    let languageSwitchLoads = 0;
    await page.route('**/api/automations', async (route) => {
      if (route.request().method() === 'GET') languageSwitchLoads += 1;
      await route.continue();
    });
    await page.getByLabel('Language').selectOption('zh-CN');
    await expect(page.getByRole('heading', { name: '自动化', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'API 密钥' })).toBeVisible();
    await expect(page.getByText('更改已保存', { exact: true })).toBeVisible();
    await expect(intervalRow).toContainText('间隔');
    await expect(cronRow.locator('.automation-trigger-config')).toContainText('0 9 * * 1');
    expect(languageSwitchLoads).toBe(0);
    await page.unroute('**/api/automations');
    await page.reload();
    await expect(page.getByLabel('语言')).toHaveValue('zh-CN');
    await page.getByLabel('语言').selectOption('en');
    await expect(page.getByRole('heading', { name: 'Automations' })).toBeVisible();

    await page.screenshot({ path: 'test-results/automation-workspace-1440.png', fullPage: true });
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.screenshot({ path: 'test-results/automation-workspace-1280.png', fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.getByLabel('Language')).toBeVisible();
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth);
    expect(overflow).toBe(false);
    await page.screenshot({ path: 'test-results/automation-workspace-mobile.png', fullPage: true });
    expect(browserErrors).toEqual([]);
    expect(serverErrors).toEqual([]);
  } finally {
    await cleanupOwnedAgentFixture(page, fixture);
  }
});

test('automation edit dialog serializes submission until a delayed PATCH completes', async ({ page }) => {
  const login = await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  expect(login.ok()).toBeTruthy();
  const gate = deferred();
  let fixture: OwnedAgentFixture | null = null;
  let patchRequests = 0;
  try {
    fixture = await createOwnedAgentFixture(page, 'Delayed edit');
    const suffix = Date.now();
    const created = await createAutomationFixture(page, fixture.agent.id, `Delayed edit ${suffix}`, 'manual');
    await createAutomationFixture(page, fixture.agent.id, `Other automation ${suffix}`, 'manual');
    await page.route(`**/api/automations/${created.id}`, async (route) => {
      if (route.request().method() !== 'PATCH') return route.continue();
      patchRequests += 1;
      await gate.promise;
      await route.continue();
    });
    await page.goto('/automations');
    await page.getByRole('button', { name: `Edit Automation Delayed edit ${suffix}` }).click();
    const editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
    await editDialog.getByLabel('Name').fill(`Saved delayed ${suffix}`);
    await editDialog.getByRole('button', { name: 'Save changes' }).dblclick();
    await expect.poll(() => patchRequests).toBe(1);
    await expect(editDialog).toHaveAttribute('aria-busy', 'true');
    await expect(editDialog.getByLabel('Name')).toBeDisabled();
    await expect(editDialog.getByRole('button', { name: 'Cancel', exact: true })).toBeDisabled();
    await expect(editDialog.getByLabel('Name')).toHaveValue(`Saved delayed ${suffix}`);
    gate.resolve();
    await expect(page.getByText('Changes saved')).toBeVisible();
    await expect(editDialog).toHaveCount(0);
  } finally {
    gate.resolve();
    await cleanupOwnedAgentFixture(page, fixture);
  }
});

test('storage failures do not prevent language switching and unknown triggers have a fallback', async ({ page }) => {
  await page.addInitScript(() => {
    Object.defineProperty(Storage.prototype, 'getItem', { configurable: true, value: () => { throw new DOMException('blocked', 'SecurityError'); } });
    Object.defineProperty(Storage.prototype, 'setItem', { configurable: true, value: () => { throw new DOMException('blocked', 'SecurityError'); } });
  });
  const login = await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  expect(login.ok()).toBeTruthy();
  let fixture: OwnedAgentFixture | null = null;
  try {
    fixture = await createOwnedAgentFixture(page, 'Unknown trigger');
    const automation = await createAutomationFixture(page, fixture.agent.id, `Unknown trigger ${Date.now()}`, 'manual');
    await page.route('**/api/automations', async (route) => {
      if (route.request().method() !== 'GET') return route.continue();
      const response = await route.fetch();
      const body = await response.json() as Array<Record<string, unknown>>;
      await route.fulfill({ response, json: body.map((item) => item.id === automation.id
        ? { ...item, trigger_type: 'future-trigger' }
        : item) });
    });
    await page.goto('/automations');
    await expect(page.getByLabel('Language')).toBeVisible();
    await page.getByLabel('Language').selectOption('zh-CN');
    await expect(page.getByRole('heading', { name: '自动化', exact: true })).toBeVisible();
    await expect(page.locator(`[data-automation-id="${automation.id}"]`)).toContainText('未知触发方式');
    await expect(page.locator('html')).toHaveAttribute('lang', 'zh-CN');
  } finally {
    await cleanupOwnedAgentFixture(page, fixture);
  }
});

test('agent visibility uses localized labels and a visible fallback', async ({ page }) => {
  const login = await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  expect(login.ok()).toBeTruthy();
  let fixture: OwnedAgentFixture | null = null;
  try {
    fixture = await createOwnedAgentFixture(page, 'Localized visibility');
    await page.route('**/api/agents', async (route) => {
      if (route.request().method() !== 'GET') return route.continue();
      await route.fulfill({ json: [
        { ...fixture!.agent, id: 'localized-private-agent', name: 'Localized known agent', visibility: 'private' },
        { ...fixture!.agent, id: 'localized-unknown-agent', name: 'Localized unknown agent', visibility: 'future_visibility' }
      ] });
    });

    await page.goto('/agents');
    await page.getByLabel('Language').selectOption('zh-CN');
    const knownAgentRow = page.locator('[data-agent-id="localized-private-agent"]');
    const unknownAgentRow = page.locator('[data-agent-id="localized-unknown-agent"]');
    await expect(knownAgentRow).toContainText('私有');
    await expect(unknownAgentRow).toContainText('未知可见范围');
    await expect(knownAgentRow).not.toContainText('private');
    await expect(unknownAgentRow).not.toContainText('future_visibility');
  } finally {
    await cleanupOwnedAgentFixture(page, fixture);
  }
});

test('login and automation status messages retranslate after language changes', async ({ page }) => {
  await page.goto('/login');
  await selectLocalPasswordLogin(page);
  await page.getByLabel('Password').fill('incorrect-password');
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await expect(page.getByText('Unable to sign in. Check your credentials and retry.')).toBeVisible();
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByText('无法登录，请检查凭据后重试。')).toBeVisible();

  await page.getByLabel('密码').fill('admin123');
  await page.getByRole('button', { name: '登录', exact: true }).click();
  await expect(page).toHaveURL(/\/sessions$/);
  await page.getByLabel('语言').selectOption('en');

  let fixture: OwnedAgentFixture | null = null;
  try {
    fixture = await createOwnedAgentFixture(page, 'Localized status');
    const suffix = Date.now();
    const automation = await createAutomationFixture(page, fixture.agent.id, `Localized status ${suffix}`, 'manual');

    let failLoad = true;
    await page.route('**/api/automations', async (route) => {
      if (route.request().method() === 'GET' && failLoad) return route.fulfill({ status: 503, json: { error: 'private load detail' } });
      await route.continue();
    });
    await page.goto('/automations');
    await expect(page.getByRole('alert')).toContainText('Unable to load automations. Retry.');
    await page.getByLabel('Language').selectOption('zh-CN');
    await expect(page.getByRole('alert')).toContainText('无法加载自动化，请重试。');
    failLoad = false;
    await page.getByRole('button', { name: '重试', exact: true }).click();
    await page.locator(`[data-automation-id="${automation.id}"]`).locator('button.automation-select').click();
    await page.getByLabel('语言').selectOption('en');

    await page.route(`**/api/automations/${automation.id}`, async (route) => {
      if (route.request().method() === 'PATCH') return route.fulfill({ status: 500, json: { error: 'private save detail' } });
      await route.continue();
    });
    await page.getByRole('button', { name: 'Edit Automation', exact: true }).click();
    const editDialog = page.locator('.automation-form-dialog');
    await editDialog.getByRole('button', { name: 'Save changes' }).click();
    await expect(editDialog.getByRole('alert')).toContainText('Unable to save automation. Check the fields and retry.');
    await page.getByLabel('Language').selectOption('zh-CN', { force: true });
    await expect(editDialog.getByRole('alert')).toContainText('无法保存自动化，请检查字段后重试。');
    await page.unroute(`**/api/automations/${automation.id}`);
    await page.getByLabel('语言').selectOption('en', { force: true });
    await editDialog.getByRole('button', { name: 'Cancel', exact: true }).click();

    await page.route(`**/api/automations/${automation.id}/trigger`, (route) => route.fulfill({ status: 500, json: { error: 'private run detail' } }));
    await page.locator(`[data-automation-id="${automation.id}"]`).getByRole('button', { name: 'Run now' }).click();
    await expect(page.getByRole('alert')).toContainText('Unable to run automation. Retry.');
    await page.getByLabel('Language').selectOption('zh-CN');
    await expect(page.getByRole('alert')).toContainText('无法运行自动化，请重试。');
  } finally {
    await cleanupOwnedAgentFixture(page, fixture);
  }
});

test('agent load errors and creation defaults are localized on first entry', async ({ page }) => {
  const login = await page.request.post('/api/auth/login', { data: { email: 'admin@example.com', password: 'admin123' } });
  expect(login.ok()).toBeTruthy();
  let fixture: OwnedAgentFixture | null = null;
  try {
    fixture = await createOwnedAgentFixture(page, 'Localized defaults');
    const ownedAgent = fixture.agent;
    await page.route(`**/api/agents/${ownedAgent.id}`, (route) => route.fulfill({ status: 500, json: { error: 'private agent detail' } }));
    await page.goto(`/agents/${ownedAgent.id}`);
  await expect(page.getByText('Unable to load agent. Try again.', { exact: true })).toBeVisible();
  await page.getByLabel('Language').selectOption('zh-CN');
  await expect(page.getByText('无法加载智能体，请重试。', { exact: true })).toBeVisible();

  await page.goto('/agents');
  await page.getByRole('button', { name: '创建智能体', exact: true }).click();
  const createAgentDialog = page.getByRole('dialog', { name: '创建智能体' });
  await expect(createAgentDialog.getByRole('textbox', { name: '名称', exact: true })).toHaveValue('规划助手');
  await expect(createAgentDialog.getByRole('textbox', { name: '指令', exact: true })).toContainText('协助检查代码仓库、进行精准修改并说明权衡。');
  await createAgentDialog.getByRole('button', { name: '取消', exact: true }).click();
  await page.getByRole('button', { name: '技能', exact: true }).click();
  await page.locator('.skills-page .page-header').getByRole('button', { name: '创建技能', exact: true }).click();
  const createSkillDialog = page.getByRole('dialog', { name: '创建技能' });
  await expect(createSkillDialog.getByLabel('名称')).toHaveValue('代码仓库审查');
  await expect(createSkillDialog.getByLabel('描述')).toHaveValue('审查代码仓库变更并简洁报告发现。');
  await expect(createSkillDialog.getByLabel('内容')).toContainText('检查代码仓库差异，并在修改代码前总结风险。');
  await createSkillDialog.getByLabel('名称').fill('用户自定义技能名称');
  await page.getByLabel('语言').selectOption('en');
  await expect(page.getByRole('dialog', { name: 'Create skill' }).getByLabel('Name')).toHaveValue('用户自定义技能名称');
  await page.getByLabel('Language').selectOption('zh-CN');
  await page.getByRole('dialog', { name: '创建技能' }).getByRole('button', { name: '关闭' }).click();
  await page.getByRole('button', { name: '自动化', exact: true }).click();
  await page.getByRole('button', { name: '新建自动化', exact: true }).click();
  const createAutomationDialog = page.getByRole('dialog', { name: '创建自动化' });
  await expect(createAutomationDialog.getByLabel('名称')).toHaveValue('手动代码仓库检查');
  await expect(createAutomationDialog.getByRole('textbox', { name: '提示词' })).toContainText('运行自动化代码仓库检查并报告结果。');
  await createAutomationDialog.getByRole('textbox', { name: '提示词' }).fill('用户自定义自动化提示词');
  await page.getByLabel('语言').selectOption('en', { force: true });
  await expect(page.getByRole('dialog', { name: 'Create Automation' }).getByRole('textbox', { name: 'Prompt' })).toContainText('用户自定义自动化提示词');
  await page.getByLabel('Language').selectOption('zh-CN', { force: true });
  await page.getByRole('dialog', { name: '创建自动化' }).getByRole('button', { name: '取消', exact: true }).click();

  await page.getByRole('button', { name: 'API 密钥', exact: true }).click();
  await page.locator('.api-keys-page .page-header').getByRole('button', { name: '创建 API 密钥', exact: true }).click();
  await expect(page.getByLabel('名称')).toHaveValue('本地自动化');
  await page.getByLabel('名称').fill('用户自定义 API 密钥名称');
  await page.getByLabel('语言').selectOption('en');
  await expect(page.getByLabel('Name')).toHaveValue('用户自定义 API 密钥名称');
  await page.getByLabel('Language').selectOption('zh-CN');

  await page.unroute(`**/api/agents/${ownedAgent.id}`);
  await page.goto(`/agents/${ownedAgent.id}`);
  const activityPanel = page.getByRole('tabpanel', { name: '活动', exact: true });
  await expect(activityPanel.getByRole('textbox', { name: '消息', exact: true })).toHaveCount(0);
  await page.getByLabel('语言').selectOption('en');
  await expect(page.getByRole('tabpanel', { name: 'Activity', exact: true }).getByRole('textbox', { name: 'Message', exact: true })).toHaveCount(0);
  await page.getByLabel('Language').selectOption('zh-CN');

  await page.getByRole('button', { name: '集成应用', exact: true }).click();
  await page.getByRole('button', { name: '新建集成应用', exact: true }).click();
  const createIntegrationDialog = page.getByRole('dialog', { name: '新建集成应用' });
  const appNameInput = createIntegrationDialog.getByRole('textbox', { name: '名称', exact: true });
  await expect(appNameInput).toHaveValue('');
  await appNameInput.fill('用户自定义集成应用');
  await page.getByLabel('语言').selectOption('en');
  await expect(page.getByRole('dialog', { name: 'Create Integration App' }).getByRole('textbox', { name: 'Name', exact: true })).toHaveValue('用户自定义集成应用');
  await page.getByRole('dialog', { name: 'Create Integration App' }).getByRole('button', { name: 'Close', exact: true }).click();

    await page.getByLabel('Language').selectOption('zh-CN');
    await page.goto('/widget');
    await expect(page.locator('.widget-form textarea')).toHaveValue('来自嵌入式组件的问候。');
  } finally {
    await cleanupOwnedAgentFixture(page, fixture);
  }
});
