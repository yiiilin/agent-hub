import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, provisionLocalUser } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const WEBHOOK_TOKEN_PATTERN = /^ahw_[A-Za-z0-9]+$/;

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function automationRequest(automation, overrides = {}) {
  return {
    name: automation.name,
    trigger_type: automation.trigger_type,
    prompt: automation.prompt,
    schedule: automation.schedule,
    enabled: automation.enabled,
    ...overrides
  };
}

async function updateAutomation(client, automation, overrides = {}, extra = {}) {
  const { data } = await client.request(`/api/automations/${automation.id}`, {
    method: 'PATCH',
    body: { ...automationRequest(automation, overrides), ...extra }
  });
  return data;
}

async function automationHistory(client, automationId, page = 1, pageSize = 100) {
  const { data } = await client.get(
    `/api/automations/${automationId}/runs?page=${page}&page_size=${pageSize}`
  );
  return data;
}

async function waitForHistoryRun(client, automationId, runId, statuses, timeoutMs = 60_000) {
  const wanted = new Set(Array.isArray(statuses) ? statuses : [statuses]);
  return poll(async () => {
    const history = await automationHistory(client, automationId);
    return history.items.find((run) => run.id === runId) ?? null;
  }, (run) => run !== null && wanted.has(run.status), {
    timeoutMs,
    intervalMs: 200,
    description: `Automation Run ${runId} to reach ${[...wanted].join(' or ')}`
  });
}

async function waitForHistoryCount(client, automationId, count, timeoutMs = 15_000) {
  return poll(
    () => automationHistory(client, automationId),
    (history) => history.total >= count,
    {
      timeoutMs,
      intervalMs: 100,
      description: `Automation ${automationId} history to contain ${count} Runs`
    }
  );
}

function assertRunAttribution(run, automation, source, message) {
  assert.match(run.id, UUID_PATTERN);
  assert.equal(run.agent_id, automation.agent_id);
  assert.equal(run.automation_id, automation.id);
  assert.equal(run.source, source);
  assert.equal(run.initial_message, message);
}

async function assertEmptyHistory(client, automationId, label) {
  const history = await automationHistory(client, automationId);
  assert.equal(history.total, 0, `${label} must not create a Run`);
  assert.deepEqual(history.items, []);
  assert.equal(history.page, 1);
  assert.equal(history.page_size, 100);
}

async function waitForSafeCronWindow() {
  await poll(
    () => new Date(),
    (now) => now.getUTCSeconds() <= 50,
    {
      timeoutMs: 15_000,
      intervalMs: 100,
      description: 'a safe UTC minute window for cron deduplication'
    }
  );
}

async function cleanupResource(action, errors) {
  try {
    await action();
  } catch (error) {
    errors.push(error);
  }
}

export default async function automationsApiScenario(context) {
  const superClient = new ApiClient(context.baseURL);
  await loginAsAdmin(superClient);

  let ownerClient = null;
  let owner = null;
  const agentIds = [];
  let scenarioError = null;
  const cleanupErrors = [];

  try {
    const ownerAccount = await provisionLocalUser(
      superClient,
      context,
      'qa-automation-owner'
    );
    ownerClient = ownerAccount.client;
    owner = ownerAccount.user;
    assert.equal(owner.role, 'member');

    const { data: primaryAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Automation Agent'),
      instructions: 'Run deterministic public Automation API checks.',
      visibility: 'private',
      public_to: []
    });
    agentIds.push(primaryAgent.id);
    assert.match(primaryAgent.id, UUID_PATTERN);

    const { data: alternateAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Automation Alternate Agent'),
      instructions: 'Prove Automation Agent bindings remain immutable.',
      visibility: 'private',
      public_to: []
    });
    agentIds.push(alternateAgent.id);
    assert.match(alternateAgent.id, UUID_PATTERN);

    await superClient.post('/api/automations', {
      agent_id: primaryAgent.id,
      name: context.unique('Forbidden foreign Automation'),
      trigger_type: 'manual',
      prompt: 'Must not be created.',
      schedule: null,
      enabled: true
    }, { expectedStatus: 403 });

    const markdownPrompt = `# ${context.unique('Release review')}\n\n- Preserve **history**\n- Keep \`agent_id\` stable\n\n> Report risks exactly.`;
    const { data: createdManual } = await ownerClient.post('/api/automations', {
      agent_id: primaryAgent.id,
      name: context.unique('QA Manual Automation'),
      trigger_type: 'manual',
      prompt: markdownPrompt,
      schedule: null,
      enabled: true
    });
    assert.match(createdManual.id, UUID_PATTERN);
    assert.equal(createdManual.owner_id, owner.id);
    assert.equal(createdManual.agent_id, primaryAgent.id);
    assert.equal(createdManual.prompt, markdownPrompt);
    assert.equal(createdManual.webhook_token, null);

    const { data: openapi } = await ownerClient.get('/openapi.json');
    const updateSchema = openapi.components.schemas.UpdateAutomationRequest;
    assert.equal(updateSchema.additionalProperties, false);
    assert.equal(Object.hasOwn(updateSchema.properties, 'agent_id'), false);

    const { data: ownerListAfterCreate } = await ownerClient.get('/api/automations');
    const listedManual = ownerListAfterCreate.find((item) => item.id === createdManual.id);
    assert.equal(listedManual.prompt, markdownPrompt);
    assert.equal(listedManual.webhook_token, null);
    const { data: superList } = await superClient.get('/api/automations');
    assert.equal(superList.some((item) => item.id === createdManual.id), false);

    const editedMarkdown = `## ${context.unique('Edited checklist')}\n\n1. Keep **Markdown** intact.\n2. Preserve blank lines.\n\n\`\`\`text\nowner scoped\n\`\`\``;
    const editedManual = await updateAutomation(ownerClient, createdManual, {
      name: context.unique('QA Edited Manual Automation'),
      prompt: editedMarkdown
    }, {
      agent_id: alternateAgent.id
    });
    assert.equal(editedManual.agent_id, primaryAgent.id, 'PATCH must not change the Agent binding');
    assert.equal(editedManual.prompt, editedMarkdown);
    assert.equal(editedManual.webhook_token, null);

    await superClient.request(`/api/automations/${editedManual.id}`, {
      method: 'PATCH',
      body: automationRequest(editedManual),
      expectedStatus: 404
    });
    await superClient.get(`/api/automations/${editedManual.id}/runs`, { expectedStatus: 404 });

    const { data: disabledAutomation } = await ownerClient.post('/api/automations', {
      agent_id: primaryAgent.id,
      name: context.unique('QA Disabled Automation'),
      trigger_type: 'manual',
      prompt: context.unique('Disabled prompt'),
      schedule: null,
      enabled: false
    });
    await ownerClient.post(`/api/automations/${disabledAutomation.id}/trigger`, {
      message: context.unique('Disabled trigger attempt')
    }, { expectedStatus: 403 });
    await assertEmptyHistory(ownerClient, disabledAutomation.id, 'Disabled manual trigger');

    await ownerClient.request(`/api/automations/${disabledAutomation.id}`, {
      method: 'PATCH',
      body: automationRequest(disabledAutomation, {
        trigger_type: 'unsupported',
        schedule: null
      }),
      expectedStatus: 400
    });
    await assertEmptyHistory(ownerClient, disabledAutomation.id, 'Invalid trigger update');

    const disabledInterval = await updateAutomation(ownerClient, disabledAutomation, {
      trigger_type: 'interval',
      schedule: '2s',
      enabled: false
    });
    await ownerClient.request(`/api/automations/${disabledInterval.id}`, {
      method: 'PATCH',
      body: automationRequest(disabledInterval, { schedule: '0s' }),
      expectedStatus: 400
    });
    await ownerClient.post(`/api/automations/${disabledInterval.id}/trigger`, {
      message: context.unique('Wrong trigger path attempt')
    }, { expectedStatus: 403 });
    await sleep(2_300);
    await assertEmptyHistory(ownerClient, disabledInterval.id, 'Disabled or invalid interval');

    const { data: defaultManualRun } = await ownerClient.post(
      `/api/automations/${editedManual.id}/trigger`,
      { message: null }
    );
    assertRunAttribution(defaultManualRun, editedManual, 'automation:manual', editedMarkdown);
    await waitForHistoryRun(ownerClient, editedManual.id, defaultManualRun.id, 'completed');

    const failureMessage = `fixture:model-error ${context.unique('Automation failure')}`;
    const { data: failedManualRun } = await ownerClient.post(
      `/api/automations/${editedManual.id}/trigger`,
      { message: failureMessage }
    );
    assertRunAttribution(failedManualRun, editedManual, 'automation:manual', failureMessage);
    await waitForHistoryRun(ownerClient, editedManual.id, failedManualRun.id, 'failed');
    const { data: failedEvents } = await ownerClient.get(`/api/runs/${failedManualRun.id}/events`);
    assert.equal(
      failedEvents.some((event) => event.event_type === 'status'
        && event.content === 'failed'
        && event.payload?.error === 'runtime execution failed'),
      true,
      'Failed Automation Run must expose its sanitized Run Console error event'
    );

    const expectedManualPages = [failedManualRun.id, defaultManualRun.id];
    const seenManualRuns = [];
    for (let page = 1; page <= expectedManualPages.length; page += 1) {
      const history = await automationHistory(ownerClient, editedManual.id, page, 1);
      assert.equal(history.total, 2);
      assert.equal(history.page, page);
      assert.equal(history.page_size, 1);
      assert.equal(history.items.length, 1);
      assert.equal(history.items[0].id, expectedManualPages[page - 1]);
      assert.equal(history.items[0].automation_id, editedManual.id);
      seenManualRuns.push(history.items[0].id);
    }
    assert.equal(new Set(seenManualRuns).size, 2, 'History pages must not overlap');
    await ownerClient.get(`/api/automations/${editedManual.id}/runs?page=0&page_size=1`, {
      expectedStatus: 400
    });
    await ownerClient.get(`/api/automations/${editedManual.id}/runs?page=1&page_size=101`, {
      expectedStatus: 400
    });

    const webhookPrompt = context.unique('QA webhook default prompt');
    const { data: createdWebhook } = await ownerClient.post('/api/automations', {
      agent_id: primaryAgent.id,
      name: context.unique('QA Webhook Automation'),
      trigger_type: 'webhook',
      prompt: webhookPrompt,
      schedule: null,
      enabled: true
    });
    const webhookToken = createdWebhook.webhook_token;
    assert.match(webhookToken, WEBHOOK_TOKEN_PATTERN);

    const { data: listWithWebhook } = await ownerClient.get('/api/automations');
    const listedWebhook = listWithWebhook.find((item) => item.id === createdWebhook.id);
    assert.equal(listedWebhook.webhook_token, null);
    assert.equal(JSON.stringify(listWithWebhook).includes(webhookToken), false);

    const unchangedWebhook = await updateAutomation(ownerClient, createdWebhook, {
      name: context.unique('QA Reusable Webhook Automation')
    });
    assert.equal(unchangedWebhook.webhook_token, null);
    assert.equal(JSON.stringify(unchangedWebhook).includes(webhookToken), false);

    const anonymousClient = new ApiClient(context.baseURL);
    assert.equal(anonymousClient.cookies.size, 0);
    const webhookMessages = [
      context.unique('QA anonymous webhook first'),
      context.unique('QA anonymous webhook second')
    ];
    const webhookRuns = [];
    for (const [index, message] of webhookMessages.entries()) {
      const { data: webhookRun } = await anonymousClient.post('/api/automations/webhook', {
        message
      }, {
        headers: { 'x-agent-hub-webhook-token': webhookToken }
      });
      assertRunAttribution(webhookRun, unchangedWebhook, 'automation:webhook', message);
      webhookRuns.push(webhookRun);
      if (index === 0) {
        await waitForHistoryRun(ownerClient, unchangedWebhook.id, webhookRun.id, 'completed');
      }
    }
    const reusableWebhookHistory = await waitForHistoryCount(ownerClient, unchangedWebhook.id, 2);
    assert.equal(reusableWebhookHistory.total, 2);
    assert.deepEqual(
      new Set(reusableWebhookHistory.items.map((run) => run.id)),
      new Set(webhookRuns.map((run) => run.id)),
      'Both anonymous requests must create attributed history with the same webhook token'
    );

    const disabledWebhook = await updateAutomation(ownerClient, unchangedWebhook, { enabled: false });
    await anonymousClient.post('/api/automations/webhook', {
      message: context.unique('Disabled webhook attempt')
    }, {
      headers: { 'x-agent-hub-webhook-token': webhookToken },
      expectedStatus: 401
    });
    await anonymousClient.post('/api/automations/webhook', {
      message: context.unique('Invalid webhook attempt')
    }, {
      headers: { 'x-agent-hub-webhook-token': 'ahw_invalid_automation_scope' },
      expectedStatus: 401
    });
    assert.equal((await automationHistory(ownerClient, disabledWebhook.id)).total, 2);

    await ownerClient.delete(`/api/agents/${primaryAgent.id}`, { expectedStatus: 204 });
    agentIds.splice(agentIds.indexOf(primaryAgent.id), 1);

    const intervalPrompt = context.unique('QA two second interval prompt');
    const { data: intervalAutomation } = await ownerClient.post('/api/automations', {
      agent_id: alternateAgent.id,
      name: context.unique('QA Interval Automation'),
      trigger_type: 'interval',
      prompt: intervalPrompt,
      schedule: '2s',
      enabled: true
    });
    assert.equal(intervalAutomation.schedule, '2s');
    const intervalHistory = await waitForHistoryCount(
      ownerClient,
      intervalAutomation.id,
      2,
      12_000
    );
    const stoppedInterval = await updateAutomation(ownerClient, intervalAutomation, {
      enabled: false
    });
    await sleep(1_200);
    const stableIntervalHistory = await automationHistory(ownerClient, stoppedInterval.id);
    assert.equal(stableIntervalHistory.total, 2, 'A 2s interval must create at most one Run per due time');
    assert.equal(intervalHistory.items.length, 2);
    for (const run of stableIntervalHistory.items) {
      assertRunAttribution(run, intervalAutomation, 'automation:scheduler', intervalPrompt);
    }
    const intervalGap = Date.parse(stableIntervalHistory.items[0].created_at)
      - Date.parse(stableIntervalHistory.items[1].created_at);
    assert.ok(intervalGap >= 1_800, `2s interval Runs were only ${intervalGap}ms apart`);
    for (const run of stableIntervalHistory.items) {
      await waitForHistoryRun(ownerClient, stoppedInterval.id, run.id, 'completed');
    }

    await waitForSafeCronWindow();
    const cronPrompt = context.unique('QA cron prompt');
    const { data: cronAutomation } = await ownerClient.post('/api/automations', {
      agent_id: alternateAgent.id,
      name: context.unique('QA Cron Automation'),
      trigger_type: 'cron',
      prompt: cronPrompt,
      schedule: '* * * * *',
      enabled: true
    });
    assert.equal(cronAutomation.schedule, '* * * * *');
    const firstCronHistory = await waitForHistoryCount(ownerClient, cronAutomation.id, 1);
    assert.equal(firstCronHistory.total, 1);
    assertRunAttribution(firstCronHistory.items[0], cronAutomation, 'automation:scheduler', cronPrompt);
    await sleep(2_200);
    const deduplicatedCronHistory = await automationHistory(ownerClient, cronAutomation.id);
    assert.equal(
      new Date().toISOString().slice(0, 16),
      deduplicatedCronHistory.items[0].created_at.slice(0, 16),
      'Cron deduplication assertion must remain in the same UTC minute'
    );
    assert.equal(deduplicatedCronHistory.total, 1, 'Cron must run at most once in one UTC minute');
    const stoppedCron = await updateAutomation(ownerClient, cronAutomation, { enabled: false });
    await waitForHistoryRun(
      ownerClient,
      stoppedCron.id,
      deduplicatedCronHistory.items[0].id,
      'completed'
    );

    const { data: finalOwnerList } = await ownerClient.get('/api/automations');
    assert.equal(finalOwnerList.every((automation) => automation.owner_id === owner.id), true);
    assert.equal(finalOwnerList.every((automation) => automation.webhook_token === null), true);
    assert.equal(JSON.stringify(finalOwnerList).includes(webhookToken), false);
  } catch (error) {
    scenarioError = error;
  } finally {
    for (const agentId of agentIds.reverse()) {
      await cleanupResource(
        () => ownerClient.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] }),
        cleanupErrors
      );
    }
    if (owner) {
      await cleanupResource(async () => {
        await superClient.post(`/api/admin/users/${owner.id}/erase`, {
          email: owner.email
        }, { expectedStatus: 202 });
        await poll(async () => {
          const { data: erasures } = await superClient.get('/api/admin/user-erasures');
          return erasures.find((item) => item.user_id === owner.id) ?? null;
        }, (erasure) => erasure?.status === 'completed', {
          timeoutMs: 45_000,
          description: 'temporary Automation owner erasure to complete'
        });
      }, cleanupErrors);
    }
  }

  if (scenarioError && cleanupErrors.length > 0) {
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Scenario and cleanup both failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length > 0) throw new AggregateError(cleanupErrors, 'Automation cleanup failed');
}
