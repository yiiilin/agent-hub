import { expect, test, type Page, type Route } from '@playwright/test';

const automationTestBaseURL = process.env.AUTOMATION_TEST_BASE_URL;

const owner = {
  id: '10000000-0000-4000-8000-000000000001',
  username: 'automation-owner',
  email: 'automation-owner@example.com',
  display_name: 'Automation owner',
  role: 'member'
};

const agent = {
  id: '20000000-0000-4000-8000-000000000001',
  owner_id: owner.id,
  name: 'Repository operator',
  instructions: '',
  visibility: 'private',
  public_to: [],
  runtime_id: null,
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

const manualAutomationFixture = {
  id: '30000000-0000-4000-8000-000000000001',
  agent_id: agent.id,
  owner_id: owner.id,
  name: 'Nightly repository review',
  trigger_type: 'manual',
  prompt: '# Review\n\n- Inspect changes\n- Report risks',
  schedule: null,
  webhook_token: null,
  enabled: true,
  last_triggered_at: '2026-07-17T09:00:00.000Z',
  created_at: '2026-07-17T08:30:00.000Z'
};

const webhookAutomationFixture = {
  ...manualAutomationFixture,
  id: '30000000-0000-4000-8000-000000000002',
  name: 'Inbound review hook',
  trigger_type: 'webhook',
  prompt: 'Review the inbound change.',
  last_triggered_at: null
};

const intervalAutomationFixture = {
  ...manualAutomationFixture,
  id: '30000000-0000-4000-8000-000000000003',
  name: 'Frequent repository sync',
  trigger_type: 'interval',
  schedule: '5m'
};

const cronAutomationFixture = {
  ...manualAutomationFixture,
  id: '30000000-0000-4000-8000-000000000004',
  name: 'Weekday repository report',
  trigger_type: 'cron',
  schedule: '0 9 * * 1-5'
};

function runFixture(id: string, automationId: string, message: string) {
  return {
    id,
    agent_id: agent.id,
    automation_id: automationId,
    integration_session_id: null,
    parent_run_id: null,
    runtime_id: null,
    hub_session_id: null,
    hub_message_id: null,
    hub_turn_id: null,
    session_ownership_generation: null,
    status: 'completed',
    initial_message: message,
    native_session_id: null,
    work_dir_ref: null,
    source: 'automation:manual',
    created_at: '2026-07-17T09:00:00.000Z',
    updated_at: '2026-07-17T09:01:00.000Z'
  };
}

async function installNoopEventSource(page: Page) {
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

async function mountAutomationPage(page: Page) {
  const renderErrors: string[] = [];
  page.on('console', (message) => { if (message.type() === 'error') renderErrors.push(message.text()); });
  page.on('pageerror', (error) => renderErrors.push(error.message));
  await page.goto(automationTestBaseURL ? `${automationTestBaseURL}/automations` : '/automations');
  try {
    await expect(page.locator('.automations-page')).toBeVisible();
  } catch {
    const body = await page.locator('body').innerText();
    throw new Error(`Automation test mount failed: ${renderErrors.join(' | ') || 'no browser error'}; body=${body.slice(0, 500)}`);
  }
}

async function installAutomationApi(page: Page) {
  const manualAutomation = { ...manualAutomationFixture };
  const webhookAutomation = { ...webhookAutomationFixture };
  const intervalAutomation = { ...intervalAutomationFixture };
  const cronAutomation = { ...cronAutomationFixture };
  const automations = [manualAutomation, webhookAutomation, intervalAutomation, cronAutomation];
  const run = runFixture('40000000-0000-4000-8000-000000000001', manualAutomation.id, 'Inspect the release branch');
  const requests: Array<{ method: string; path: string; body: Record<string, unknown> | null }> = [];

  await page.route('**/api/**', async (route: Route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    if (!path.startsWith('/api/')) return route.continue();
    if (path === '/api/auth/me') return route.fulfill({ json: owner });
    if (path === '/api/agents') return route.fulfill({ json: [agent] });
    if (path === '/api/automations' && method === 'GET') return route.fulfill({ json: automations });
    if (path === '/api/automations' && method === 'POST') {
      const body = route.request().postDataJSON() as Record<string, unknown>;
      requests.push({ method, path, body });
      const created = {
        ...manualAutomation,
        id: '30000000-0000-4000-8000-000000000099',
        name: body.name,
        trigger_type: body.trigger_type,
        prompt: body.prompt,
        schedule: body.schedule,
        enabled: body.enabled,
        webhook_token: body.trigger_type === 'webhook' ? 'one-time-webhook-token' : null
      };
      automations.unshift(created as typeof manualAutomation);
      return route.fulfill({ json: created });
    }
    if (path === `/api/automations/${manualAutomation.id}` && method === 'PATCH') {
      const body = route.request().postDataJSON() as Record<string, unknown>;
      requests.push({ method, path, body });
      Object.assign(manualAutomation, {
        name: body.name,
        trigger_type: body.trigger_type,
        prompt: body.prompt,
        schedule: body.schedule,
        enabled: body.enabled
      });
      return route.fulfill({ json: manualAutomation });
    }
    if (path.endsWith('/runs')) {
      const automationId = path.split('/')[3];
      return route.fulfill({ json: {
        items: automationId === manualAutomation.id ? [run] : [],
        total: automationId === manualAutomation.id ? 1 : 0,
        page: Number(url.searchParams.get('page')),
        page_size: Number(url.searchParams.get('page_size'))
      } });
    }
    if (path === `/api/automations/${manualAutomation.id}/trigger`) return route.fulfill({ json: run });
    if (path === `/api/runs/${run.id}/events`) return route.fulfill({ json: [{ seq: 1, run_id: run.id, event_type: 'message', role: 'assistant', content: 'Repository review completed', payload: {}, created_at: run.updated_at }] });
    return route.fulfill({ status: 404, json: { error: `Unhandled route ${method} ${path}` } });
  });

  return { manualAutomation, requests };
}

test('Automation list remains the primary surface while create and edit use dialogs', async ({ page }) => {
  await installNoopEventSource(page);
  const { manualAutomation, requests } = await installAutomationApi(page);
  await mountAutomationPage(page);

  const list = page.getByRole('region', { name: 'List' });
  await expect(list).toBeVisible();
  await expect(list.locator('[data-automation-id]')).toHaveCount(4);
  await expect(list).toContainText('Nightly repository review');
  await expect(list).toContainText('Inbound review hook');
  await expect(page.getByRole('dialog')).toHaveCount(0);

  await page.getByRole('button', { name: 'New automation' }).click();
  let dialog = page.getByRole('dialog', { name: 'Create Automation' });
  await expect(dialog).toBeVisible();
  await expect(list).toBeVisible();
  await dialog.getByLabel('Name').fill('Created webhook review');
  await dialog.getByLabel('Trigger').selectOption('webhook');
  await dialog.getByRole('button', { name: 'Create automation' }).click();
  await expect(page.getByRole('dialog', { name: 'One-time webhook token' })).toContainText('one-time-webhook-token');
  await page.getByRole('dialog', { name: 'One-time webhook token' }).locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
  await expect(list).toContainText('Created webhook review');
  expect(requests.at(-1)?.body).toMatchObject({ name: 'Created webhook review', trigger_type: 'webhook' });

  await list.getByRole('button', { name: `Edit Automation ${manualAutomation.name}` }).click();
  dialog = page.getByRole('dialog', { name: 'Edit Automation' });
  await expect(dialog.getByLabel('Agent')).toBeDisabled();
  await expect(dialog.getByLabel('Name')).toHaveValue(manualAutomation.name);
  await dialog.getByLabel('Name').fill('Edited repository review');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(dialog).toHaveCount(0);
  await expect(list).toContainText('Edited repository review');
  expect(requests.at(-1)?.path).toBe(`/api/automations/${manualAutomation.id}`);
});

test('Automation trigger types use one aligned row structure and show their configuration', async ({ page }) => {
  await installNoopEventSource(page);
  await installAutomationApi(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await mountAutomationPage(page);

  const rows = page.locator('.automation-list-row');
  await expect(rows).toHaveCount(4);
  await expect(rows.filter({ hasText: 'Nightly repository review' }).locator('.automation-trigger-config')).toContainText('None');
  await expect(rows.filter({ hasText: 'Inbound review hook' }).locator('.automation-trigger-config')).toContainText('/api/automations/webhook');
  await expect(rows.filter({ hasText: 'Frequent repository sync' }).locator('.automation-trigger-config')).toContainText('5m');
  await expect(rows.filter({ hasText: 'Weekday repository report' }).locator('.automation-trigger-config')).toContainText('0 9 * * 1-5');

  const measurements = await rows.evaluateAll((elements) => elements.map((element) => {
    const row = element.getBoundingClientRect();
    const select = element.querySelector('.automation-select')?.getBoundingClientRect();
    const actions = element.querySelector('.automation-row-actions')?.getBoundingClientRect();
    return {
      height: row.height,
      selectWidth: select?.width ?? -1,
      actionTop: actions ? actions.top - row.top : -1
    };
  }));
  const spread = (key: keyof (typeof measurements)[number]) => {
    const values = measurements.map((measurement) => measurement[key]);
    return Math.max(...values) - Math.min(...values);
  };
  const diagnostic = JSON.stringify(measurements);
  expect(spread('height'), diagnostic).toBeLessThanOrEqual(1);
  expect(spread('selectWidth'), diagnostic).toBeLessThanOrEqual(1);
  expect(spread('actionTop'), diagnostic).toBeLessThanOrEqual(1);
});

test('Automation prompt switches between Markdown source and rich text without losing content', async ({ page }) => {
  const { manualAutomation } = await installAutomationApi(page);
  await mountAutomationPage(page);
  await page.getByRole('button', { name: `Edit Automation ${manualAutomation.name}` }).click();

  const dialog = page.getByRole('dialog', { name: 'Edit Automation' });
  await dialog.getByRole('radio', { name: 'Source mode' }).click();
  const source = dialog.locator('.cm-content');
  await expect(source).toContainText('# Review');
  await source.fill('# Release review\n\n- Preserve history\n- Explain risks');
  await dialog.getByRole('radio', { name: 'Rich text' }).click();
  const richText = dialog.getByRole('textbox', { name: 'Prompt' });
  await expect(richText).toContainText('Release review');
  await expect(richText).toContainText('Preserve history');
  await dialog.getByRole('radio', { name: 'Source mode' }).click();
  await expect(dialog.locator('.cm-content')).toContainText('# Release review');
  await expect(dialog.locator('.cm-content')).toContainText(/[*-] Explain risks/);
});

test('Automation history opens the supplied Run Console and stays within a 390px viewport', async ({ page }) => {
  await installNoopEventSource(page);
  await installAutomationApi(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await mountAutomationPage(page);

  await page.getByRole('region', { name: 'List' }).locator('.automation-list-row').filter({ hasText: 'Nightly repository review' }).locator('.automation-select').click();
  const history = page.getByRole('region', { name: 'Run history' });
  await expect(history).toContainText('Inspect the release branch');
  await history.locator('[data-run-id]').click();
  await expect(page.getByRole('region', { name: 'Run events' })).toContainText('Repository review completed');
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);

  await page.getByRole('button', { name: 'New automation' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Automation' });
  await expect(dialog).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(390);
  const box = await dialog.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.x).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width).toBeLessThanOrEqual(390);
});
