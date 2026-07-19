import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';
import { withBrowser } from '../../support/browser.mjs';

const TERMINAL_RUN_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted']);
const FAILURE_MESSAGE = 'fixture:model-error';
const FAILURE_REASON = 'runtime execution failed';

async function responseJson(response, label) {
  const body = await response.text();
  assert.equal(response.ok(), true, `${label} returned ${response.status()}: ${body.slice(0, 1_000)}`);
  return JSON.parse(body);
}

async function getJson(request, path, label) {
  return responseJson(await request.get(path), label);
}

async function waitForRun(request, runId, expectedStatus, timeoutMs = 90_000) {
  return poll(
    () => getJson(request, `/api/runs/${runId}`, `Run ${runId}`),
    (run) => run.status === expectedStatus,
    { timeoutMs, description: `Run ${runId} to reach ${expectedStatus}` }
  );
}

async function automationHistory(request, automationId) {
  return getJson(
    request,
    `/api/automations/${automationId}/runs?page=1&page_size=100`,
    `Automation ${automationId} history`
  );
}

async function waitForScheduledRun(request, automationId) {
  const history = await poll(
    () => automationHistory(request, automationId),
    (page) => page.items.some((run) => run.source === 'automation:scheduler'),
    { timeoutMs: 30_000, intervalMs: 250, description: `Automation ${automationId} scheduled Run` }
  );
  const run = history.items.find((candidate) => candidate.source === 'automation:scheduler');
  assert.equal(run.automation_id, automationId);
  return run;
}

async function waitForTerminalHistory(request, automationId, expectedTotal, timeoutMs = 240_000) {
  return poll(
    () => automationHistory(request, automationId),
    (page) => page.total === expectedTotal
      && page.items.length === expectedTotal
      && page.items.every((run) => TERMINAL_RUN_STATUSES.has(run.status)),
    { timeoutMs, intervalMs: 500, description: `${expectedTotal} terminal Runs for Automation ${automationId}` }
  );
}

async function assertNoHorizontalOverflow(page, label, selectors = []) {
  await page.evaluate(() => new Promise((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
  const documentOverflow = await page.evaluate(() => (
    document.documentElement.scrollWidth - document.documentElement.clientWidth
  ));
  assert.ok(documentOverflow <= 1, `${label} document horizontal overflow: ${documentOverflow}px`);
  for (const selector of selectors) {
    const locator = page.locator(selector);
    await locator.waitFor({ state: 'visible' });
    const overflow = await locator.evaluate((element) => element.scrollWidth - element.clientWidth);
    assert.ok(overflow <= 1, `${label} ${selector} horizontal overflow: ${overflow}px`);
  }
}

async function assertDialogFitsViewport(page, dialog, label) {
  const viewport = page.viewportSize();
  const box = await dialog.boundingBox();
  assert.ok(viewport, `${label} must have a viewport`);
  assert.ok(box, `${label} dialog must have geometry`);
  assert.ok(box.x >= -1, `${label} dialog must not escape the left edge`);
  assert.ok(box.x + box.width <= viewport.width + 1, `${label} dialog must not escape the right edge`);
}

function automationResponse(page, method, pathname) {
  return page.waitForResponse((response) => (
    response.request().method() === method
    && new URL(response.url()).pathname === pathname
  ));
}

async function openCreateDialog(page, list, fields) {
  await page.getByRole('button', { name: 'New automation' }).click();
  const dialog = page.getByRole('dialog', { name: 'Create Automation' });
  await dialog.waitFor();
  assert.equal(await list.isVisible(), true, 'Automation list must remain the primary surface behind create dialogs');
  await dialog.getByLabel('Agent').selectOption(fields.agentId);
  await dialog.getByLabel('Name').fill(fields.name);
  await dialog.getByLabel('Trigger').selectOption(fields.triggerType);
  if (fields.markdownSource) {
    await dialog.getByRole('radio', { name: 'Source mode' }).click();
    await dialog.locator('.cm-content').fill(fields.prompt);
    await dialog.getByRole('radio', { name: 'Rich text' }).click();
    const richText = dialog.getByRole('textbox', { name: 'Prompt' });
    await richText.getByText(fields.richTextMarker, { exact: true }).waitFor();
  } else {
    await dialog.getByRole('textbox', { name: 'Prompt' }).fill(fields.prompt);
  }
  if (fields.schedule) await dialog.getByLabel('Schedule').fill(fields.schedule);
  if (fields.enabled === false) await dialog.getByLabel('Enabled').uncheck();
  return dialog;
}

async function createAutomationThroughUi(page, list, fields) {
  const dialog = await openCreateDialog(page, list, fields);
  const responsePromise = automationResponse(page, 'POST', '/api/automations');
  await dialog.getByRole('button', { name: 'Create automation', exact: true }).click();
  const created = await responseJson(await responsePromise, `Create ${fields.triggerType} Automation`);
  await dialog.waitFor({ state: 'detached' });
  assert.equal(created.agent_id, fields.agentId);
  assert.equal(created.trigger_type, fields.triggerType);
  assert.equal(created.schedule, fields.schedule ?? null);
  return created;
}

async function createWebhookThroughUi({
  page,
  context,
  request,
  list,
  scenarioContext,
  fields,
  anonymousMessage
}) {
  const dialog = await openCreateDialog(page, list, fields);
  const responsePromise = automationResponse(page, 'POST', '/api/automations');
  let tracingActive = true;
  let secretDialog;
  let created;
  let anonymousRun;
  await context.tracing.stop();
  tracingActive = false;
  try {
    await dialog.getByRole('button', { name: 'Create automation', exact: true }).click();
    created = await responseJson(await responsePromise, 'Create webhook Automation');
    await dialog.waitFor({ state: 'detached' });
    secretDialog = page.getByRole('dialog', { name: 'One-time webhook token' });
    await secretDialog.waitFor();
    await secretDialog.getByText('This token is shown once.', { exact: true }).waitFor();
    const tokenSurface = secretDialog.getByTestId('webhook-token');
    const token = await tokenSurface.textContent();
    assert.equal(
      typeof token === 'string' && token.startsWith('ahw_') && token.length > 40,
      true,
      'Webhook token must have the expected opaque shape'
    );
    assert.equal(created.webhook_token === token, true, 'Create response and one-time surface must agree');
    assert.equal(await tokenSurface.evaluate((element) => element.tagName), 'CODE');

    await context.grantPermissions(
      ['clipboard-read', 'clipboard-write'],
      { origin: new URL(scenarioContext.baseURL).origin }
    );
    await tokenSurface.selectText();
    await page.keyboard.press('Control+C');
    const copied = await page.evaluate(() => navigator.clipboard.readText());
    assert.equal(copied === token, true, 'The one-time token surface must support copying');

    const listed = await responseJson(await request.get('/api/automations'), 'List Automations after webhook create');
    const listedWebhook = listed.find((automation) => automation.id === created.id);
    assert.ok(listedWebhook, 'Created webhook Automation must appear in the list response');
    assert.equal(listedWebhook.webhook_token, null, 'List responses must never repeat the webhook token');

    const anonymousResponse = await fetch(new URL('/api/automations/webhook', scenarioContext.baseURL), {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'X-Agent-Hub-Webhook-Token': token
      },
      body: JSON.stringify({ message: anonymousMessage })
    });
    const anonymousBody = await anonymousResponse.text();
    assert.equal(
      anonymousResponse.ok,
      true,
      `Unauthenticated webhook returned ${anonymousResponse.status}: ${anonymousBody.slice(0, 500)}`
    );
    anonymousRun = JSON.parse(anonymousBody);
    assert.equal(anonymousRun.automation_id, created.id);
    assert.equal(anonymousRun.source, 'automation:webhook');
    assert.equal(anonymousRun.initial_message, anonymousMessage);

    await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();
    await secretDialog.waitFor({ state: 'detached' });
    assert.equal(await page.getByTestId('webhook-token').count(), 0, 'Closing must remove the one-time token from the DOM');
  } finally {
    if (secretDialog && await secretDialog.isVisible().catch(() => false)) {
      await secretDialog.getByTestId('webhook-token').evaluate((element) => {
        element.textContent = '[redacted]';
      }).catch(() => undefined);
      await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true })
        .click().catch(() => page.keyboard.press('Escape'));
      await secretDialog.waitFor({ state: 'detached' }).catch(() => undefined);
    }
    if (!tracingActive) {
      await context.tracing.start({ screenshots: true, snapshots: true, sources: true });
      tracingActive = true;
    }
  }
  assert.ok(created, 'Webhook Automation must be created');
  assert.ok(anonymousRun, 'Anonymous webhook must create a Run');
  return { automation: created, run: anonymousRun };
}

async function updateAutomation(request, automation, updates) {
  const body = { ...automation, ...updates };
  const updated = await responseJson(await request.patch(`/api/automations/${automation.id}`, {
    data: {
      name: body.name,
      trigger_type: body.trigger_type,
      prompt: body.prompt,
      schedule: body.schedule,
      enabled: body.enabled
    }
  }), `Update Automation ${automation.id}`);
  Object.assign(automation, updated);
  return automation;
}

function rowFor(page, automationId) {
  return page.locator(`[data-automation-id="${automationId}"]`);
}

export default async function automationsBrowserScenario(scenarioContext) {
  const cleanupClient = new ApiClient(scenarioContext.baseURL);
  await loginAsAdmin(cleanupClient);
  const createdAutomations = [];
  let createdAgentId = null;
  let scenarioFailure = null;

  try {
    await withBrowser(scenarioContext, {
      allowedHttpErrors: [
        { method: 'GET', pathname: '/api/auth/me', status: 401, times: 1 }
      ]
    }, async ({ page, context, request, browserErrors }) => {
      await page.goto('/login', { waitUntil: 'domcontentloaded' });
      await page.getByLabel('Email').fill('admin@example.com');
      await page.getByLabel('Password').fill('admin123');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByText('admin@example.com', { exact: true }).waitFor();

      const suiteLabel = scenarioContext.unique('QA Automation Browser');
      const agent = await responseJson(await request.post('/api/agents', {
        data: {
          name: `${suiteLabel} Agent`,
          instructions: 'Exercise Automation browser QA through the real Runtime chain.',
          visibility: 'private',
          public_to: []
        }
      }), 'Create Automation browser Agent');
      createdAgentId = agent.id;

      await page.goto('/automations', { waitUntil: 'domcontentloaded' });
      await page.getByRole('heading', { name: 'Automations', level: 1 }).waitFor();
      const list = page.getByRole('region', { name: 'List' });
      await list.waitFor();
      assert.equal(await page.getByRole('dialog').count(), 0, 'Automation list must be the default primary surface');

      const manualPrompt = '# Release automation review\n\n- Preserve history\n- Report exact failures';
      let manual = await createAutomationThroughUi(page, list, {
        agentId: agent.id,
        name: `${suiteLabel} Manual`,
        triggerType: 'manual',
        prompt: manualPrompt,
        markdownSource: true,
        richTextMarker: 'Release automation review'
      });
      createdAutomations.push(manual);

      const manualRowBeforeEdit = rowFor(page, manual.id);
      await manualRowBeforeEdit.getByRole('button', { name: `Edit Automation ${manual.name}` }).click();
      const editDialog = page.getByRole('dialog', { name: 'Edit Automation' });
      assert.equal(await list.isVisible(), true, 'Automation list must remain visible behind edit dialogs');
      assert.equal(await editDialog.getByLabel('Agent').isDisabled(), true, 'Agent binding must be immutable while editing');
      assert.equal(await editDialog.getByLabel('Agent').inputValue(), agent.id);
      const editedManualName = `${suiteLabel} Manual Edited`;
      const editedManualPrompt = '# Edited automation review\n\n- Keep Markdown\n- Open Run Console';
      await editDialog.getByLabel('Name').fill(editedManualName);
      await editDialog.getByRole('radio', { name: 'Source mode' }).click();
      assert.ok((await editDialog.locator('.cm-content').innerText()).includes('# Release automation review'));
      await editDialog.locator('.cm-content').fill(editedManualPrompt);
      await editDialog.getByRole('radio', { name: 'Rich text' }).click();
      await editDialog.getByRole('textbox', { name: 'Prompt' })
        .getByText('Edited automation review', { exact: true }).waitFor();
      const editResponsePromise = automationResponse(page, 'PATCH', `/api/automations/${manual.id}`);
      await editDialog.getByRole('button', { name: 'Save changes' }).click();
      const editedManual = await responseJson(await editResponsePromise, 'Edit manual Automation');
      await editDialog.waitFor({ state: 'detached' });
      assert.equal(editedManual.agent_id, agent.id);
      assert.equal(editedManual.name, editedManualName);
      assert.equal(editedManual.prompt, editedManualPrompt);
      Object.assign(manual, editedManual);

      const webhookMessage = `${suiteLabel} anonymous webhook message`;
      const webhookResult = await createWebhookThroughUi({
        page,
        context,
        request,
        list,
        scenarioContext,
        fields: {
          agentId: agent.id,
          name: `${suiteLabel} Webhook`,
          triggerType: 'webhook',
          prompt: `${suiteLabel} webhook default prompt`
        },
        anonymousMessage: webhookMessage
      });
      const webhook = webhookResult.automation;
      createdAutomations.push(webhook);

      await rowFor(page, webhook.id).getByRole('button', { name: `Edit Automation ${webhook.name}` }).click();
      const webhookEditDialog = page.getByRole('dialog', { name: 'Edit Automation' });
      assert.equal(await webhookEditDialog.getByLabel('Agent').isDisabled(), true);
      const webhookEditResponsePromise = automationResponse(page, 'PATCH', `/api/automations/${webhook.id}`);
      await webhookEditDialog.getByRole('button', { name: 'Save changes' }).click();
      const unchangedWebhook = await responseJson(await webhookEditResponsePromise, 'Save unchanged webhook Automation');
      await webhookEditDialog.waitFor({ state: 'detached' });
      assert.equal(unchangedWebhook.webhook_token, null, 'Editing webhook to webhook must not reveal another token');
      assert.equal(await page.getByTestId('webhook-token').count(), 0);
      Object.assign(webhook, unchangedWebhook);

      const cron = await createAutomationThroughUi(page, list, {
        agentId: agent.id,
        name: `${suiteLabel} Cron`,
        triggerType: 'cron',
        prompt: `${suiteLabel} cron scheduler prompt`,
        schedule: '* * * * *'
      });
      createdAutomations.push(cron);
      const interval = await createAutomationThroughUi(page, list, {
        agentId: agent.id,
        name: `${suiteLabel} Interval`,
        triggerType: 'interval',
        prompt: `${suiteLabel} interval scheduler prompt`,
        schedule: '2s'
      });
      createdAutomations.push(interval);

      const filter = list.getByRole('textbox', { name: 'Filter automations' });
      await filter.fill(suiteLabel);
      const rows = list.locator('[data-automation-id]');
      assert.equal(await rows.count(), 4, 'Filter must isolate the four scenario Automations');
      const rowStructures = await rows.evaluateAll((elements) => elements.map((element) => {
        const select = element.querySelector('.automation-select');
        const style = select ? getComputedStyle(select) : null;
        return {
          metadataColumns: select?.children.length ?? 0,
          display: style?.display ?? '',
          gridTemplateColumns: style?.gridTemplateColumns ?? ''
        };
      }));
      assert.equal(rowStructures.every((structure) => structure.metadataColumns === 6), true);
      assert.equal(rowStructures.every((structure) => structure.display === 'grid'), true);
      assert.equal(new Set(rowStructures.map((structure) => structure.gridTemplateColumns)).size, 1);
      assert.ok((await rowFor(page, manual.id).innerText()).includes('None'));
      assert.ok((await rowFor(page, webhook.id).innerText()).includes('/api/automations/webhook'));
      assert.ok((await rowFor(page, interval.id).innerText()).includes('2s'));
      assert.ok((await rowFor(page, cron.id).innerText()).includes('* * * * *'));
      assert.equal(await rowFor(page, manual.id).getByRole('button', { name: 'Run now' }).count(), 1);
      assert.equal(await rowFor(page, webhook.id).getByRole('button', { name: 'Run now' }).count(), 0);
      assert.equal(await rowFor(page, interval.id).getByRole('button', { name: 'Run now' }).count(), 0);
      assert.equal(await rowFor(page, cron.id).getByRole('button', { name: 'Run now' }).count(), 0);
      await assertNoHorizontalOverflow(page, 'Automations desktop', ['.automations-page', '.automation-list']);

      const [cronRun, intervalRun] = await Promise.all([
        waitForScheduledRun(request, cron.id),
        waitForScheduledRun(request, interval.id)
      ]);
      await updateAutomation(request, cron, { enabled: false });
      await updateAutomation(request, interval, { enabled: false });
      const cronHistory = await waitForTerminalHistory(request, cron.id, 1);
      assert.equal(cronHistory.items[0].id, cronRun.id);
      const intervalHistory = await poll(
        () => automationHistory(request, interval.id),
        (history) => history.total >= 1
          && history.items.every((run) => TERMINAL_RUN_STATUSES.has(run.status)),
        { timeoutMs: 120_000, intervalMs: 500, description: `terminal interval Runs for ${interval.id}` }
      );
      assert.ok(intervalHistory.items.some((run) => run.id === intervalRun.id));
      await waitForRun(request, webhookResult.run.id, 'completed');

      await page.reload({ waitUntil: 'domcontentloaded' });
      const reloadedList = page.getByRole('region', { name: 'List' });
      await reloadedList.waitFor();
      await reloadedList.getByRole('textbox', { name: 'Filter automations' }).fill(suiteLabel);
      const history = page.getByRole('region', { name: 'Run history' });
      await rowFor(page, cron.id).locator('.automation-select').click();
      await history.getByText('Scheduled automation', { exact: true }).first().waitFor();
      await history.getByText(cron.prompt, { exact: true }).first().waitFor();
      await rowFor(page, interval.id).locator('.automation-select').click();
      await history.getByText('Scheduled automation', { exact: true }).first().waitFor();
      await history.getByText(interval.prompt, { exact: true }).first().waitFor();
      await rowFor(page, webhook.id).locator('.automation-select').click();
      await history.getByText('Webhook automation', { exact: true }).waitFor();
      await history.getByText(webhookMessage, { exact: true }).waitFor();

      const failedRun = await responseJson(await request.post(`/api/automations/${manual.id}/trigger`, {
        data: { message: FAILURE_MESSAGE }
      }), 'Create deterministic failed Automation Run');
      assert.equal(failedRun.automation_id, manual.id);
      assert.equal(failedRun.source, 'automation:manual');
      await waitForRun(request, failedRun.id, 'failed', 120_000);
      const failedEvents = await poll(
        () => getJson(request, `/api/runs/${failedRun.id}/events`, `Failed Run ${failedRun.id} events`),
        (events) => events.some((event) => event.payload?.error === FAILURE_REASON),
        { timeoutMs: 30_000, description: `Run ${failedRun.id} persisted failure event` }
      );
      assert.equal(failedEvents.some((event) => event.payload?.error === FAILURE_REASON), true);

      const manualResponsePromise = automationResponse(page, 'POST', `/api/automations/${manual.id}/trigger`);
      await rowFor(page, manual.id).getByRole('button', { name: 'Run now' }).click();
      const manualUiRun = await responseJson(await manualResponsePromise, 'Trigger manual Automation through UI');
      assert.equal(manualUiRun.automation_id, manual.id);
      assert.equal(manualUiRun.source, 'automation:manual');
      assert.equal(manualUiRun.initial_message, manual.prompt);

      const seededRuns = [];
      for (let index = 1; index <= 19; index += 1) {
        const message = `${suiteLabel} pagination seed ${String(index).padStart(2, '0')}`;
        seededRuns.push(await responseJson(await request.post(`/api/automations/${manual.id}/trigger`, {
          data: { message }
        }), `Create manual pagination Run ${index}`));
      }
      assert.equal(seededRuns.every((run) => run.automation_id === manual.id), true);

      await history.getByText(manual.prompt, { exact: true }).waitFor();
      await rowFor(page, webhook.id).locator('.automation-select').click();
      await history.getByText(webhookMessage, { exact: true }).waitFor();

      let trackManualHistory = false;
      let manualHistoryRequests = 0;
      let maxManualHistoryInFlight = 0;
      const manualHistoryInFlight = new Set();
      const isManualHistoryRequest = (browserRequest) => {
        const url = new URL(browserRequest.url());
        return browserRequest.method() === 'GET'
          && url.pathname === `/api/automations/${manual.id}/runs`;
      };
      page.on('request', (browserRequest) => {
        if (!trackManualHistory || !isManualHistoryRequest(browserRequest)) return;
        manualHistoryRequests += 1;
        manualHistoryInFlight.add(browserRequest);
        maxManualHistoryInFlight = Math.max(maxManualHistoryInFlight, manualHistoryInFlight.size);
      });
      page.on('requestfinished', (browserRequest) => manualHistoryInFlight.delete(browserRequest));
      page.on('requestfailed', (browserRequest) => manualHistoryInFlight.delete(browserRequest));

      trackManualHistory = true;
      await rowFor(page, manual.id).locator('.automation-select').click();
      const historyRows = history.locator('[data-run-id]');
      await poll(() => historyRows.count(), (count) => count === 20, {
        timeoutMs: 10_000,
        description: 'Automation history page one to contain 20 Runs'
      });
      await history.getByText('Page 1 of 2', { exact: true }).waitFor();
      await poll(async () => {
        const statuses = await historyRows.locator('span:first-child > strong').allTextContents();
        return statuses.some((status) => ['pending', 'running', 'waiting for tool'].includes(status));
      }, Boolean, { timeoutMs: 5_000, description: 'active Run on Automation history page one' });
      await poll(() => manualHistoryRequests, (count) => count >= 2, {
        timeoutMs: 10_000,
        intervalMs: 100,
        description: 'Automation history active-run polling'
      });
      trackManualHistory = false;
      assert.equal(maxManualHistoryInFlight, 1, 'Automation history polling must remain serial');

      const next = history.getByRole('button', { name: 'Next' });
      await poll(() => next.isEnabled(), Boolean, { timeoutMs: 5_000, description: 'history Next button' });
      await next.click();
      await history.getByText('Page 2 of 2', { exact: true }).waitFor();
      const failedHistoryRow = history.locator(`[data-run-id="${failedRun.id}"]`);
      await failedHistoryRow.waitFor();
      assert.equal(await history.locator('[data-run-id]').count(), 1);
      await failedHistoryRow.getByText(FAILURE_MESSAGE, { exact: true }).waitFor();
      await failedHistoryRow.click();

      const runEvents = page.getByRole('region', { name: 'Run events' });
      await runEvents.waitFor();
      await runEvents.locator('.event-error').getByText(FAILURE_REASON, { exact: true }).waitFor();
      assert.equal(await runEvents.locator('.event-error').getByText(FAILURE_REASON, { exact: true }).count(), 1);
      assert.ok((await runEvents.innerText()).includes('failed'));

      await page.setViewportSize({ width: 390, height: 844 });
      await assertNoHorizontalOverflow(page, 'Automations 390x844', ['.automations-page']);
      await page.getByLabel('Language').selectOption('zh-CN');
      await page.getByRole('heading', { name: '自动化', exact: true }).waitFor();
      await page.getByRole('region', { name: '运行历史' }).waitFor();
      await page.getByRole('region', { name: '运行事件' })
        .getByText(FAILURE_REASON, { exact: true }).waitFor();
      await assertNoHorizontalOverflow(page, 'Chinese Automations 390x844', ['.automations-page']);
      await page.getByLabel('语言').selectOption('en');

      await rowFor(page, manual.id).getByRole('button', { name: `Edit Automation ${manual.name}` }).click();
      const mobileEditDialog = page.getByRole('dialog', { name: 'Edit Automation' });
      assert.equal(await mobileEditDialog.getByLabel('Agent').isDisabled(), true);
      await assertDialogFitsViewport(page, mobileEditDialog, '390x844 Automation edit');
      await mobileEditDialog.getByRole('button', { name: 'Cancel', exact: true }).click();
      await mobileEditDialog.waitFor({ state: 'detached' });
      await assertNoHorizontalOverflow(page, 'Automations after mobile dialog', ['.automations-page']);

      const persistedManualHistory = await automationHistory(request, manual.id);
      assert.equal(persistedManualHistory.total, 21);
      assert.equal(persistedManualHistory.items.length, 21);
      assert.equal(persistedManualHistory.items.filter((run) => run.status === 'failed').length, 1);
      assert.equal(persistedManualHistory.items.some((run) => run.id === failedRun.id), true);
      assert.equal(persistedManualHistory.items.some((run) => run.id === manualUiRun.id), true);

      const failedStreamUrl = new URL(`/api/runs/${failedRun.id}/events/stream`, scenarioContext.baseURL).href;
      const allowedFailedStreamAbort = `requestfailed: GET ${failedStreamUrl}: net::ERR_ABORTED`;
      const unexpectedBrowserErrors = browserErrors.filter((error) => error !== allowedFailedStreamAbort);
      browserErrors.splice(0, browserErrors.length, ...unexpectedBrowserErrors);
      assert.deepEqual(browserErrors, [], 'Automation browser diagnostics must remain empty');
    });
  } catch (error) {
    scenarioFailure = error;
  }

  const cleanupErrors = [];
  for (const automation of createdAutomations.toReversed()) {
    try {
      await cleanupClient.request(`/api/automations/${automation.id}`, {
        method: 'PATCH',
        body: {
          name: automation.name,
          trigger_type: automation.trigger_type,
          prompt: automation.prompt,
          schedule: automation.schedule,
          enabled: false
        },
        expectedStatus: [200, 404]
      });
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (createdAgentId) {
    try {
      await cleanupClient.delete(`/api/agents/${createdAgentId}`, { expectedStatus: [204, 404] });
      const { data: remainingAutomations } = await cleanupClient.get('/api/automations');
      assert.equal(
        remainingAutomations.some((automation) => createdAutomations.some((created) => created.id === automation.id)),
        false,
        'Agent cleanup must remove every scenario-owned Automation'
      );
    } catch (error) {
      cleanupErrors.push(error);
    }
  }
  if (scenarioFailure && cleanupErrors.length === 0) throw scenarioFailure;
  if (scenarioFailure || cleanupErrors.length > 0) {
    throw new AggregateError(
      [scenarioFailure, ...cleanupErrors].filter(Boolean),
      'Automation browser scenario or cleanup failed'
    );
  }
}
