import { execFileSync } from 'node:child_process';
import { expect, request, test, type APIRequestContext, type Page } from '@playwright/test';
import type { Agent } from '../src/api/client';
import { composeArgs } from './e2e-compose';

function assertUuid(value: string, label: string) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new Error(`${label} is not a canonical UUID: ${value}`);
  }
}

function runSql(sql: string) {
  return execFileSync('docker', [
    ...composeArgs(), 'exec', '-T', 'postgres',
    'psql', '-U', 'agent_hub', '-d', 'agent_hub', '-v', 'ON_ERROR_STOP=1', '-Atc', sql
  ], { cwd: process.cwd(), encoding: 'utf8' }).trim();
}

function expectReturnedId(output: string, expectedId: string, action: string) {
  if (!output.split('\n').includes(expectedId)) {
    throw new Error(`${action} did not return ${expectedId}: ${output}`);
  }
}

function expireToolRequest(toolRequestId: string) {
  assertUuid(toolRequestId, 'tool request id');
  const output = runSql(`
UPDATE integration_tool_requests
SET expires_at = now() - interval '1 second'
WHERE id = '${toolRequestId}' AND status = 'pending'
RETURNING id;
`);
  expectReturnedId(output, toolRequestId, 'Expiring integration tool request');
}

async function runtimeEnrollmentToken(api: APIRequestContext) {
  const login = await api.post('/api/auth/login', {
    data: { email: 'admin@example.com', password: 'admin123' }
  });
  expect(login.ok()).toBeTruthy();
  const response = await api.post('/api/admin/runtime-enrollment-tokens');
  expect(response.ok()).toBeTruthy();
  return (await response.json() as { token: string }).token;
}

function failPendingFixtureRun(runId: string, sessionId: string) {
  assertUuid(runId, 'fixture run id');
  assertUuid(sessionId, 'integration session id');
  const output = runSql(`
UPDATE runs
SET status = 'failed', updated_at = now()
WHERE id = '${runId}'
  AND integration_session_id = '${sessionId}'
  AND source = 'integration:message'
  AND status = 'pending'
  AND runtime_id IS NULL
RETURNING id;
`);
  expectReturnedId(output, runId, 'Failing controlled pending fixture run');
}

function deleteIntegrationAgentFixture(agentId: string) {
  assertUuid(agentId, 'fixture agent id');
  const output = runSql(`
BEGIN;
CREATE TEMPORARY TABLE integration_fixture_agents ON COMMIT DROP AS
SELECT id
FROM agents
WHERE id = '${agentId}' AND name LIKE 'Integration Agent %';

UPDATE hub_sessions
SET active_turn_id = NULL
WHERE agent_id IN (SELECT id FROM integration_fixture_agents);

UPDATE hub_session_messages
SET run_id = NULL, turn_id = NULL
WHERE session_id IN (
  SELECT id FROM hub_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM integration_attachments
WHERE session_id IN (
  SELECT id FROM integration_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
) OR run_id IN (
  SELECT id FROM runs
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM integration_messages
WHERE session_id IN (
  SELECT id FROM integration_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
) OR run_id IN (
  SELECT id FROM runs
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM run_events
WHERE run_id IN (
  SELECT id FROM runs
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
) OR hub_message_id IN (
  SELECT id FROM hub_session_messages
  WHERE session_id IN (
    SELECT id FROM hub_sessions
    WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
  )
);

DELETE FROM embed_sessions
WHERE hub_session_id IN (
  SELECT id FROM hub_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM integration_sessions
WHERE hub_session_id IN (
  SELECT id FROM hub_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM session_bundle_deletion_queue
WHERE agent_id IN (SELECT id FROM integration_fixture_agents);

DELETE FROM runs
WHERE agent_id IN (SELECT id FROM integration_fixture_agents);

DELETE FROM hub_session_messages
WHERE session_id IN (
  SELECT id FROM hub_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM hub_session_turns
WHERE session_id IN (
  SELECT id FROM hub_sessions
  WHERE agent_id IN (SELECT id FROM integration_fixture_agents)
);

DELETE FROM hub_sessions
WHERE agent_id IN (SELECT id FROM integration_fixture_agents);

DELETE FROM agents
WHERE id IN (SELECT id FROM integration_fixture_agents)
RETURNING id;
COMMIT;
`);
  expectReturnedId(output, agentId, 'Deleting Integration Agent fixture');
}

function deleteIntegrationAppFixture(appId: string) {
  assertUuid(appId, 'integration app id');
  const output = runSql(`
DELETE FROM oauth_apps
WHERE id = '${appId}' AND name LIKE 'Integration App %'
RETURNING id;
`);
  expectReturnedId(output, appId, 'Deleting Integration App fixture');
}

async function waitForIntegrationAgentWorkspaceCleanup(agentId: string) {
  assertUuid(agentId, 'fixture agent id');
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const pending = Number(runSql(`
SELECT count(*)
FROM runtime_session_cleanup_obligations AS cleanup
JOIN hub_sessions AS sessions ON sessions.id = cleanup.session_id
WHERE sessions.agent_id = '${agentId}';
`));
    if (pending === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`Timed out waiting for Runtime workspace cleanup for Integration Agent ${agentId}`);
}

function deleteRuntimeFixture(runtimeId: string) {
  assertUuid(runtimeId, 'fixture runtime id');
  const output = runSql(`
DELETE FROM runtimes
WHERE id = '${runtimeId}' AND hostname LIKE 'integration-e2e-idle-%'
RETURNING id;
`);
  expectReturnedId(output, runtimeId, 'Deleting non-executing runtime fixture');
}

type AgentConfiguration = Pick<Agent,
  | 'name'
  | 'instructions'
  | 'visibility'
  | 'public_to'
  | 'runtime_id'
  | 'model_selection'
  | 'model_settings'
  | 'codex_subagents'
  | 'sandbox_policy'
  | 'managed_skill_ids'
  | 'mcp_allowlist'
>;

async function setAgentRuntime(api: APIRequestContext, agentId: string, runtimeId: string | null) {
  const currentResponse = await api.get(`/api/agents/${agentId}`);
  expect(currentResponse.ok()).toBeTruthy();
  const current = await currentResponse.json() as AgentConfiguration;
  const updateResponse = await api.patch(`/api/agents/${agentId}`, {
    data: {
      name: current.name,
      instructions: current.instructions,
      visibility: current.visibility,
      public_to: current.public_to,
      runtime_id: runtimeId,
      model_selection: current.model_selection,
      model_settings: current.model_settings,
      codex_subagents: current.codex_subagents,
      sandbox_policy: current.sandbox_policy,
      managed_skill_ids: current.managed_skill_ids,
      mcp_allowlist: current.mcp_allowlist
    }
  });
  expect(updateResponse.ok()).toBeTruthy();
  expect((await updateResponse.json() as AgentConfiguration).runtime_id).toBe(runtimeId);
  return current.runtime_id;
}

async function waitForEvent(api: APIRequestContext, sessionId: string, token: string, predicate: (event: any) => boolean) {
  const deadline = Date.now() + 30_000;
  let after = 0;
  let lastEvents: any[] = [];
  while (Date.now() < deadline) {
    const response = await api.get(`/api/integrations/sessions/${sessionId}/events?after=${after}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    expect(response.ok()).toBeTruthy();
    const events = await response.json();
    lastEvents = [...lastEvents, ...events];
    for (const event of events) {
      after = Math.max(after, event.seq);
      if (predicate(event)) return event;
    }
    await new Promise((resolve) => setTimeout(resolve, 700));
  }
  throw new Error(`Timed out waiting for integration event. Last events: ${JSON.stringify(lastEvents)}`);
}

const TOOL_RESULT_MARKER = 'completed integration tool result: ';

async function submitToolResultAndExpectRoundTrip(
  api: APIRequestContext,
  sessionId: string,
  accessToken: string,
  toolRequestId: string,
  parentRunId: string,
  result: unknown
) {
  const response = await api.post(`/api/integrations/tool-requests/${toolRequestId}/result`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    data: { result }
  });
  expect(response.ok()).toBeTruthy();
  const body = await response.json() as { run: { id: string; parent_run_id: string | null } };
  expect(body.run.parent_run_id).toBe(parentRunId);

  const resultEvent = await waitForEvent(
    api,
    sessionId,
    accessToken,
    (event) => event.run_id === body.run.id
      && event.event_type === 'tool_result'
      && event.payload?.message?.tool_request_id === toolRequestId
  );
  expect(resultEvent.payload.message.result).toEqual(result);

  const messageEvent = await waitForEvent(
    api,
    sessionId,
    accessToken,
    (event) => event.run_id === body.run.id
      && event.event_type === 'message'
      && event.content?.includes(TOOL_RESULT_MARKER)
  );
  const resultOffset = messageEvent.content.indexOf(TOOL_RESULT_MARKER);
  expect(JSON.parse(messageEvent.content.slice(resultOffset + TOOL_RESULT_MARKER.length))).toEqual(result);
  await waitForEvent(
    api,
    sessionId,
    accessToken,
    (event) => event.run_id === body.run.id
      && event.event_type === 'status'
      && event.payload?.status === 'completed'
  );
  return body;
}

test('Integration App OAuth API runs messages, attachments, and tool results', async ({ page, baseURL }) => {
  test.setTimeout(120_000);
  const api = await request.newContext({ baseURL });
  let agentId: string | null = null;
  let integrationAppId: string | null = null;
  let runtimeId: string | null = null;
  let originalRuntimeId: string | null | undefined;
  let runtimeFixtureBound = false;
  let widget: Page | null = null;
  let liveSseOutcome: Promise<
    { status: 'fulfilled'; value: string } | { status: 'rejected'; reason: unknown }
  > | null = null;
  let hasPrimaryError = false;
  try {
    await page.goto('/login');
    await page.getByLabel('Email').fill('admin@example.com');
    await page.getByLabel('Password').fill('admin123');
    await page.getByRole('button', { name: 'Sign in', exact: true }).click();
    await expect(page.getByText('admin@example.com')).toBeVisible();

    const externalUserId = 'integration-e2e-admin';
    const tenantId = 'default';
    await page.goto(`/api/auth/oidc/mock/start?email=${encodeURIComponent('admin@example.com')}&sub=${encodeURIComponent(externalUserId)}`);
    await expect(page.getByText('admin@example.com')).toBeVisible();

    await page.goto('/agents');
    const agentName = `Integration Agent ${Date.now()}`;
    await page.locator('.agents-header').getByRole('button', { name: 'Create Agent' }).click();
    const createAgentDialog = page.getByRole('dialog', { name: 'Create Agent' });
    await createAgentDialog.getByLabel('Name', { exact: true }).fill(agentName);
    await createAgentDialog.getByLabel('Instructions').fill('Handle external integration messages.');
    await expect(createAgentDialog.getByLabel('Default model connection')).not.toHaveValue('');
    const createAgentResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/agents');
    await createAgentDialog.getByRole('button', { name: 'Create agent' }).click();
    const createdAgentResponse = await createAgentResponse;
    expect(createdAgentResponse.ok()).toBeTruthy();
    agentId = (await createdAgentResponse.json() as { id: string }).id;
    await expect(page.getByRole('heading', { name: agentName, level: 1 })).toBeVisible();

    const redirectUri = `${baseURL}/oauth/callback`;
    await page.goto('/integrations');
    await page.getByRole('button', { name: 'Create Integration App' }).click();
    const createAppDialog = page.getByRole('dialog', { name: 'Create Integration App' });
    await createAppDialog.getByRole('textbox', { name: 'Name', exact: true }).fill(`Integration App ${Date.now()}`);
    await createAppDialog.getByRole('combobox', { name: 'External platform' }).selectOption({ label: 'Mock OIDC' });
    await createAppDialog.getByRole('combobox', { name: 'Authentication channel' }).selectOption({ label: 'Default' });
    await createAppDialog.getByRole('textbox', { name: 'Redirect URI 1' }).fill(redirectUri);
    await createAppDialog.getByRole('checkbox', { name: `Delegate ${agentName}` }).check();
    const createAppResponse = page.waitForResponse((response) => response.request().method() === 'POST'
      && new URL(response.url()).pathname === '/api/integration-apps');
    await createAppDialog.getByRole('button', { name: 'Create Integration App' }).click();
    const createdAppResponse = await createAppResponse;
    expect(createdAppResponse.ok()).toBeTruthy();
    const createdApp = await createdAppResponse.json() as {
      integration_app: { id: string; client_id: string };
      client_secret: string;
    };
    integrationAppId = createdApp.integration_app.id;
    const secretDialog = page.getByRole('dialog', { name: 'Integration App secret' });
    const clientId = (await secretDialog.locator('.integration-credential-list > div').filter({ hasText: 'Client ID' }).locator('code').innerText()).trim();
    const clientSecret = (await secretDialog.locator('.integration-credential-list > div').filter({ hasText: 'Client secret' }).locator('code').innerText()).trim();
    expect(clientId).toBe(createdApp.integration_app.client_id);
    expect(clientSecret).toBe(createdApp.client_secret);
    expect(clientId).toMatch(/^ahc_/);
    expect(clientSecret).toMatch(/^ahs_/);
    await secretDialog.locator('.modal-actions').getByRole('button', { name: 'Close', exact: true }).click();

    const oauthScope = `profile email external_profile agent:${agentId}`;
    const authorizationUrl = (state: string) => `/api/oauth/authorize?${new URLSearchParams({
      client_id: clientId,
      redirect_uri: redirectUri,
      state,
      scope: oauthScope,
      external_user_id: externalUserId,
      tenant_id: tenantId
    })}`;

    const oauthState = 'state&=#/ value';
    await page.goto(authorizationUrl(oauthState));
    const callbackUrl = new URL(page.url());
    const code = callbackUrl.searchParams.get('code');
    expect(code).toBeTruthy();
    expect(callbackUrl.searchParams.get('state')).toBe(oauthState);

    const tokenResponse = await api.post('/api/oauth/token', {
      form: {
        grant_type: 'authorization_code',
        client_id: clientId,
        client_secret: clientSecret,
        code: code!,
        redirect_uri: redirectUri,
        scope: oauthScope
      }
    });
    expect(tokenResponse.ok()).toBeTruthy();
    const { access_token: accessToken } = await tokenResponse.json();
    expect(accessToken).toMatch(/^aho_/);

    expect((await api.get('/api/auth/me', { headers: { Authorization: `Bearer ${accessToken}` } })).status()).toBe(401);
    expect((await api.get('/api/agents', { headers: { Authorization: `Bearer ${accessToken}` } })).status()).toBe(401);
    const userinfoResponse = await api.get('/api/oauth/userinfo', { headers: { Authorization: `Bearer ${accessToken}` } });
    expect(userinfoResponse.ok()).toBeTruthy();
    expect(await userinfoResponse.json()).toMatchObject({
      email: 'admin@example.com',
      external_profile: { tenant_id: tenantId, external_user_id: externalUserId }
    });

    const clientCredentialsResponse = await api.post('/api/oauth/token', {
      form: {
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
        scope: `agent:${agentId}`
      }
    });
    expect(clientCredentialsResponse.ok()).toBeTruthy();
    const { access_token: appAccessToken } = await clientCredentialsResponse.json();
    expect(appAccessToken).toMatch(/^aho_/);
    expect((await api.get('/api/oauth/userinfo', { headers: { Authorization: `Bearer ${appAccessToken}` } })).status()).toBe(403);

    // 用户级 token 不能降权为 App-only Widget；控制台通过 App owner 入口签发短期 token。
    expect((await api.post('/api/integrations/embed-session', {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { agent_id: agentId }
    })).status()).toBe(403);
    const widgetExchange = await page.request.post(`/api/integration-apps/${integrationAppId}/agents/${agentId}/widget-session`, {
      data: {}
    });
    expect(widgetExchange.ok()).toBeTruthy();
    const { token: widgetToken } = await widgetExchange.json();
    expect(widgetToken).toMatch(/^ahe_/);
    const widgetPage = await page.context().newPage();
    widget = widgetPage;
    await widgetPage.goto(`${baseURL}/widget#token=${widgetToken}`);
    await expect(widgetPage).toHaveURL(`${baseURL}/widget`);
    await expect(widgetPage.getByText(agentName)).toBeVisible();
    await widgetPage.getByRole('button', { name: 'Send' }).click();
    await expect(widgetPage.getByText('Fake Codex completed run')).toBeVisible({ timeout: 30_000 });

    const sessionResponse = await api.post('/api/integrations/sessions', {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: {
        agent_id: agentId,
        external_user_id: externalUserId,
        tenant_id: tenantId,
        tools: [{ name: 'echo', description: 'Echo input', parameters: { type: 'object' } }],
        metadata: { source: 'playwright' }
      }
    });
    expect(sessionResponse.ok()).toBeTruthy();
    const session = await sessionResponse.json();

    // 不同幂等键的并发提交也必须由 session 锁串行化，不能生成 sibling runs。
    await page.goto(authorizationUrl('serialized-session'));
    const serializedCode = new URL(page.url()).searchParams.get('code');
    expect(serializedCode).toBeTruthy();
    const serializedTokenResponse = await api.post('/api/oauth/token', {
      form: {
        grant_type: 'authorization_code',
        client_id: clientId,
        client_secret: clientSecret,
        code: serializedCode!,
        redirect_uri: redirectUri,
        scope: oauthScope
      }
    });
    expect(serializedTokenResponse.ok()).toBeTruthy();
    const { access_token: serializedAccessToken } = await serializedTokenResponse.json();
    expect(serializedAccessToken).toMatch(/^aho_/);
    const serializedSessionResponse = await api.post('/api/integrations/sessions', {
      headers: { Authorization: `Bearer ${serializedAccessToken}` },
      data: {
        agent_id: agentId,
        external_user_id: externalUserId,
        tenant_id: tenantId,
        tools: [],
        metadata: { source: 'playwright-concurrency' }
      }
    });
    expect(serializedSessionResponse.ok()).toBeTruthy();
    const serializedSession = await serializedSessionResponse.json();

    const runtimeHostname = `integration-e2e-idle-${Date.now()}`;
    const registrationResponse = await api.post('/api/runtime/register', {
      headers: { Authorization: `Bearer ${await runtimeEnrollmentToken(api)}` },
      data: {
        hostname: runtimeHostname,
        labels: ['playwright', 'integration-conflict-fixture'],
        codex_version: 'non-executing-e2e-fixture',
        capabilities: { model_proxy: true, mcp_allowlist: false },
        sandbox_mode: 'workspace-write'
      }
    });
    expect(registrationResponse.ok()).toBeTruthy();
    ({ runtime_id: runtimeId } = await registrationResponse.json() as {
      runtime_id: string;
    });

    originalRuntimeId = await setAgentRuntime(page.request, agentId!, runtimeId);
    runtimeFixtureBound = true;
    const serializedRunIds = new Set<string>();
    const serializedMessageKeys = [
      `first-${Date.now()}`,
      `second-${Date.now()}`
    ];
    try {
      const concurrentMessages = await Promise.all([
        api.post(`/api/integrations/sessions/${serializedSession.id}/messages`, {
          headers: { Authorization: `Bearer ${serializedAccessToken}` },
          data: { content: 'First serialized message.', attachments: [], client_message_key: serializedMessageKeys[0] }
        }),
        api.post(`/api/integrations/sessions/${serializedSession.id}/messages`, {
          headers: { Authorization: `Bearer ${serializedAccessToken}` },
          data: { content: 'Second serialized message.', attachments: [], client_message_key: serializedMessageKeys[1] }
        })
      ]);
      const acceptedRuns = [];
      const acceptedMessages = [];
      for (const response of concurrentMessages) {
        expect(response.status()).toBe(200);
        const accepted = await response.json();
        serializedRunIds.add(accepted.run.id);
        acceptedRuns.push(accepted.run);
        acceptedMessages.push(accepted.message);
      }
      expect(concurrentMessages.map((response) => response.status()).sort()).toEqual([200, 200]);
      expect(acceptedRuns).toHaveLength(2);
      expect(acceptedMessages.map((message) => message.client_message_key).sort()).toEqual([...serializedMessageKeys].sort());
      expect(new Set(acceptedMessages.map((message) => message.id)).size).toBe(2);
      expect(serializedRunIds.size).toBe(1);
      for (const acceptedRun of acceptedRuns) {
        expect(acceptedRun).toMatchObject({
          integration_session_id: serializedSession.id,
          status: 'pending',
          runtime_id: null
        });
      }
      const [serializedRunId] = serializedRunIds;
      const persistedSerializedRun = await page.request.get(`/api/runs/${serializedRunId}`);
      expect(persistedSerializedRun.ok()).toBeTruthy();
      expect(await persistedSerializedRun.json()).toMatchObject({
        id: serializedRunId,
        integration_session_id: serializedSession.id,
        status: 'pending',
        runtime_id: null
      });
    } finally {
      const fixtureErrors: unknown[] = [];
      for (const serializedRunId of serializedRunIds) {
        try {
          failPendingFixtureRun(serializedRunId, serializedSession.id);
        } catch (error) {
          fixtureErrors.push(error);
        }
      }
      try {
        await setAgentRuntime(page.request, agentId!, originalRuntimeId);
        runtimeFixtureBound = false;
      } catch (error) {
        fixtureErrors.push(error);
      }
      if (fixtureErrors.length > 0) {
        throw new AggregateError(fixtureErrors, 'Serialized Integration fixture cleanup failed');
      }
    }

    const clientMessageKey = `message-${Date.now()}`;
    const messageContent = 'Please use the echo tool for "quoted" input at C:\\fixtures\\integration.\nKeep [arrays] and {objects} intact.';
    const attachment = {
      kind: 'text',
      name: 'note-"quoted"-path.txt',
      content_type: 'text/plain',
      text: 'attachment "quoted" text with C:\\fixtures\\attachment\nsecond line [1, 2]'
    };
    const messageResponse = await api.post(`/api/integrations/sessions/${session.id}/messages`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: {
        content: messageContent,
        client_message_key: clientMessageKey,
        attachments: [attachment]
      }
    });
    expect(messageResponse.ok()).toBeTruthy();
    const firstMessage = await messageResponse.json();

    const duplicateMessage = await api.post(`/api/integrations/sessions/${session.id}/messages`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: {
        content: messageContent,
        client_message_key: clientMessageKey,
        attachments: [attachment]
      }
    });
    expect(duplicateMessage.ok()).toBeTruthy();
    expect((await duplicateMessage.json()).run.id).toBe(firstMessage.run.id);

    const invalidAttachment = await api.post(`/api/integrations/sessions/${session.id}/messages`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: {
        content: 'Invalid attachment.',
        attachments: [{ kind: 'url', name: 'bad-link' }]
      }
    });
    expect(invalidAttachment.status()).toBe(400);

    const toolRequest = await waitForEvent(
      api,
      session.id,
      accessToken,
      (event) => event.run_id === firstMessage.run.id && event.event_type === 'tool_request'
    );
    const toolRequestId = toolRequest.payload.tool_request_id;
    expect(toolRequest.payload.tool_name).toBe('echo');
    expect(toolRequest.payload.source_id).toBe('platform-tool-call');
    expect(toolRequestId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    expect(toolRequest.payload.arguments.message).toBe(messageContent);
    expect(toolRequest.payload.arguments.attachments).toHaveLength(1);
    expect(toolRequest.payload.arguments.attachments[0]).toMatchObject(attachment);

    await expect.poll(async () => {
      const response = await page.request.get(`/api/runs/${firstMessage.run.id}`);
      expect(response.ok()).toBeTruthy();
      return (await response.json() as { status: string }).status;
    }).toBe('waiting_tool');

    const toolResultPayload = {
      text: 'echo result with "quotes" and C:\\fixtures\\result',
      nested: {
        values: [1, true, null, { line: 'first\nsecond', slash: '\\server\\share' }],
        object: { left: '"quoted"', right: ['array', { depth: 2 }] }
      }
    };
    const nullResult = await api.post(`/api/integrations/tool-requests/${toolRequestId}/result`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { result: null }
    });
    expect(nullResult.status()).toBe(400);

    const firstResult = await submitToolResultAndExpectRoundTrip(
      api,
      session.id,
      accessToken,
      toolRequestId,
      firstMessage.run.id,
      toolResultPayload
    );
    let completedToolResultRunId = firstResult.run.id;

    const oversizedResult = await api.post(`/api/integrations/tool-requests/${toolRequestId}/result`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { result: { text: 'x'.repeat(20_000) } }
    });
    expect(oversizedResult.status()).toBe(400);

    const duplicateResult = await api.post(`/api/integrations/tool-requests/${toolRequestId}/result`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { result: toolResultPayload }
    });
    expect(duplicateResult.ok()).toBeTruthy();
    expect((await duplicateResult.json()).run.id).toBe(firstResult.run.id);

    const additionalToolResults: Array<{ label: string; result: unknown }> = [
      {
        label: 'array',
        result: ['array value', { nested: [1, false, null] }, 'C:\\fixtures\\array']
      },
      {
        label: 'string',
        result: 'string result with "quotes", C:\\fixtures\\string, and\na newline'
      },
      { label: 'number', result: -42.5 },
      { label: 'boolean false', result: false }
    ];
    const seenToolRequestIds = new Set([toolRequestId]);
    for (const toolResultCase of additionalToolResults) {
      const typedMessageResponse = await api.post(`/api/integrations/sessions/${session.id}/messages`, {
        headers: { Authorization: `Bearer ${accessToken}` },
        data: {
          content: `Please use the echo tool for the ${toolResultCase.label} result.`,
          client_message_key: `tool-result-${toolResultCase.label}-${Date.now()}`,
          attachments: []
        }
      });
      expect(typedMessageResponse.ok()).toBeTruthy();
      const typedMessage = await typedMessageResponse.json();
      const typedToolRequest = await waitForEvent(
        api,
        session.id,
        accessToken,
        (event) => event.run_id === typedMessage.run.id && event.event_type === 'tool_request'
      );
      expect(typedToolRequest.payload.source_id).toBe('platform-tool-call');
      expect(seenToolRequestIds.has(typedToolRequest.payload.tool_request_id)).toBeFalsy();
      seenToolRequestIds.add(typedToolRequest.payload.tool_request_id);
      const typedResult = await submitToolResultAndExpectRoundTrip(
        api,
        session.id,
        accessToken,
        typedToolRequest.payload.tool_request_id,
        typedMessage.run.id,
        toolResultCase.result
      );
      completedToolResultRunId = typedResult.run.id;
    }

    const expiringMessage = await api.post(`/api/integrations/sessions/${session.id}/messages`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: {
        content: 'Please use the echo tool for an expiring request.',
        client_message_key: `expiring-${Date.now()}`,
        attachments: []
      }
    });
    expect(expiringMessage.ok()).toBeTruthy();
    const expiringMessageBody = await expiringMessage.json();
    const expiringToolRequest = await waitForEvent(
      api,
      session.id,
      accessToken,
      (event) => event.run_id === expiringMessageBody.run.id && event.event_type === 'tool_request'
    );
    expect(expiringToolRequest.payload.source_id).toBe('platform-tool-call');
    // 两个 run 使用同一个平台 tool id 时，Hub 生成的内部 id 必须带 run 作用域。
    expect(seenToolRequestIds.has(expiringToolRequest.payload.tool_request_id)).toBeFalsy();
    expireToolRequest(expiringToolRequest.payload.tool_request_id);
    const expiredResult = await api.post(`/api/integrations/tool-requests/${expiringToolRequest.payload.tool_request_id}/result`, {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { result: { text: 'too late' } }
    });
    expect(expiredResult.status()).toBe(410);
    const streamedType = await page.evaluate(async ({ sessionId, token }) => {
      const response = await fetch(`/api/integrations/sessions/${sessionId}/events/stream?after=0`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const deadline = Date.now() + 5000;
      while (Date.now() < deadline) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value);
        const match = buffer.match(/event: (integration_event)/);
        if (match) {
          await reader.cancel();
          return match[1];
        }
      }
      await reader.cancel();
      return '';
    }, { sessionId: session.id, token: accessToken });
    expect(streamedType).toBe('integration_event');

    // 先签发 code，再归档 Agent；归档后该 code、现有 token 和新 authorize 均必须失效。
    const pendingCodeResponse = await page.request.get(authorizationUrl('archive-check'), { maxRedirects: 0 });
    expect(pendingCodeResponse.status()).toBeGreaterThanOrEqual(300);
    expect(pendingCodeResponse.status()).toBeLessThan(400);
    const pendingCode = new URL(pendingCodeResponse.headers().location!).searchParams.get('code');
    expect(pendingCode).toBeTruthy();

    await page.goto(`/agents/${session.agent_id}`);
    const completedToolResultRun = page.locator(`[data-run-id="${completedToolResultRunId}"]`);
    await expect(completedToolResultRun).toContainText('completed', { timeout: 15_000 });
    await expect(completedToolResultRun).toContainText('Integration tool result');

    // 连接建立后也需要重新鉴权，归档必须关闭仍存活的 Integration SSE。
    type LiveSseReady = { status: number; contentType: string; eventType: 'integration_event' | null };
    type LiveSseBarrier =
      | { kind: 'ready'; details: LiveSseReady }
      | { kind: 'terminated'; outcome: { status: 'fulfilled'; value: string } | { status: 'rejected'; reason: unknown } };
    let resolveLiveSseBarrier!: (result: LiveSseBarrier) => void;
    let liveSseBarrierSettled = false;
    const liveSseBarrier = new Promise<LiveSseBarrier>((resolve) => {
      resolveLiveSseBarrier = resolve;
    });
    await widgetPage.exposeFunction('__reportIntegrationSseReady', (details: LiveSseReady) => {
      if (liveSseBarrierSettled) return;
      liveSseBarrierSettled = true;
      resolveLiveSseBarrier({ kind: 'ready', details });
    });
    const currentLiveSseOutcome = widgetPage.evaluate(async ({ sessionId, token }) => {
      const controller = new AbortController();
      (window as any).__cancelIntegrationSse = () => controller.abort();
      try {
        const response = await fetch(`/api/integrations/sessions/${sessionId}/events/stream?after=0`, {
          headers: { Authorization: `Bearer ${token}` },
          signal: controller.signal
        });
        const contentType = response.headers.get('content-type') ?? '';
        if (!response.ok || !contentType.includes('text/event-stream') || !response.body) {
          await (window as any).__reportIntegrationSseReady({
            status: response.status,
            contentType,
            eventType: null
          });
          return `invalid-response:${response.status}`;
        }
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let readyReported = false;
        const deadline = Date.now() + 15_000;
        while (Date.now() < deadline) {
          const { value, done } = await reader.read();
          if (done) return 'closed';
          buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
          if (!readyReported && /(?:^|\n)event: integration_event(?:\n|$)/.test(buffer)) {
            readyReported = true;
            await (window as any).__reportIntegrationSseReady({
              status: response.status,
              contentType,
              eventType: 'integration_event'
            });
          }
          if (/(?:^|\n)event: error(?:\n|$)/.test(buffer)) {
            await reader.cancel();
            return 'error';
          }
        }
        await reader.cancel();
        return 'timeout';
      } finally {
        delete (window as any).__cancelIntegrationSse;
      }
    }, { sessionId: session.id, token: accessToken }).then(
      (value) => ({ status: 'fulfilled' as const, value }),
      (reason: unknown) => ({ status: 'rejected' as const, reason })
    );
    liveSseOutcome = currentLiveSseOutcome;
    void currentLiveSseOutcome.then((outcome) => {
      if (liveSseBarrierSettled) return;
      liveSseBarrierSettled = true;
      resolveLiveSseBarrier({ kind: 'terminated', outcome });
    });
    const liveSseBarrierResult = await liveSseBarrier;
    if (liveSseBarrierResult.kind === 'terminated') {
      if (liveSseBarrierResult.outcome.status === 'rejected') throw liveSseBarrierResult.outcome.reason;
      throw new Error(`live Integration SSE terminated before ready: ${liveSseBarrierResult.outcome.value}`);
    }
    expect(liveSseBarrierResult.details).toEqual({
      status: 200,
      contentType: expect.stringContaining('text/event-stream'),
      eventType: 'integration_event'
    });
    page.once('dialog', (dialog) => dialog.accept());
    await page.getByRole('button', { name: 'Delete agent' }).click();

    const liveSseResult = await currentLiveSseOutcome;
    liveSseOutcome = null;
    if (liveSseResult.status === 'rejected') throw liveSseResult.reason;
    expect(liveSseResult.value).toBe('error');
    expect((await api.post('/api/oauth/token', {
      form: {
        grant_type: 'authorization_code',
        client_id: clientId,
        client_secret: clientSecret,
        code: pendingCode!,
        redirect_uri: redirectUri,
        scope: oauthScope
      }
    })).status()).toBe(403);
    expect((await page.request.get(authorizationUrl('after-delete'), { maxRedirects: 0 })).status()).toBe(403);
    expect((await api.post('/api/integrations/embed-session', {
      headers: { Authorization: `Bearer ${accessToken}` },
      data: { agent_id: agentId }
    })).status()).toBe(403);
    expect((await api.get(`/api/integrations/sessions/${session.id}`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    })).status()).toBe(403);
    expect((await api.get('/api/widget/session', {
      headers: { 'X-Agent-Hub-Embed-Token': widgetToken }
    })).status()).toBe(401);

  } catch (error) {
    hasPrimaryError = true;
    throw error;
  } finally {
    const cleanupErrors: unknown[] = [];
    if (runtimeFixtureBound && agentId && originalRuntimeId !== undefined) {
      try {
        await setAgentRuntime(page.request, agentId, originalRuntimeId);
        runtimeFixtureBound = false;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (liveSseOutcome && widget && !widget.isClosed()) {
      try {
        await widget.evaluate(() => (window as any).__cancelIntegrationSse?.());
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (widget) {
      try {
        await widget.close();
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (liveSseOutcome) {
      await liveSseOutcome;
      liveSseOutcome = null;
    }
    let workspaceCleanupComplete = false;
    if (agentId) {
      try {
        const response = await page.request.delete(`/api/agents/${agentId}`);
        expect(response.status()).toBe(204);
        await waitForIntegrationAgentWorkspaceCleanup(agentId);
        workspaceCleanupComplete = true;
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    try {
      await api.dispose();
    } catch (error) {
      cleanupErrors.push(error);
    }
    if (agentId && workspaceCleanupComplete) {
      try {
        deleteIntegrationAgentFixture(agentId);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (integrationAppId) {
      try {
        deleteIntegrationAppFixture(integrationAppId);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (runtimeId) {
      try {
        deleteRuntimeFixture(runtimeId);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (cleanupErrors.length > 0) {
      const cleanupError = new AggregateError(cleanupErrors, 'Integration E2E fixture cleanup failed');
      if (hasPrimaryError) {
        console.error(cleanupError);
      } else {
        throw cleanupError;
      }
    }
  }
});
