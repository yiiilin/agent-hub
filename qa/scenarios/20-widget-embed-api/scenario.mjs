import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { createHmac, randomUUID } from 'node:crypto';
import { ApiClient, loginAsAdmin, poll } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted']);
const HOLD_MARKER = 'fixture:hold';

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Widget model'),
    base_url: 'http://fake-model-provider:8080',
    api_type: 'openai_responses',
    allowed_model_ids: ['hub-proxy-smoke'],
    api_key: 'dev-model-provider-api-key'
  });
  return {
    connectionId: connection.id,
    selection: { connection_id: connection.id, model_id: 'hub-proxy-smoke' }
  };
}

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function assertOpaque(value, prefix, label) {
  assert.equal(
    typeof value === 'string' && value.startsWith(prefix) && value.length > prefix.length + 20,
    true,
    `${label} must use the expected opaque prefix`
  );
}

function embedOptions(token, expectedStatus) {
  return {
    headers: { 'x-agent-hub-embed-token': token },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

function basicWidgetOptions(clientId, clientSecret, expectedStatus) {
  return {
    headers: {
      authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`
    },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

function signEmbedJwt(agentId, ownerId) {
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({
    iss: 'agent-hub-dev',
    aud: 'agent-hub-widget',
    exp: now + 300,
    iat: now,
    jti: randomUUID(),
    sub: 'qa-widget-external-user',
    owner_id: ownerId,
    agent_id: agentId
  })).toString('base64url');
  const signature = createHmac('sha256', 'dev-embed-jwt-secret')
    .update(`${header}.${payload}`)
    .digest('base64url');
  return `${header}.${payload}.${signature}`;
}

async function clientCredentialsToken(baseURL, app, clientSecret, agentId) {
  const response = await fetch(new URL('/api/oauth/token', baseURL), {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: clientSecret,
      scope: `agent:${agentId}`
    })
  });
  assert.equal(response.status, 200, `OAuth client credentials returned ${response.status}`);
  const data = await response.json();
  assert.equal(typeof data.access_token === 'string' && data.access_token.length > 20, true);
  return data.access_token;
}

function parseSseFrame(frame) {
  const parsed = { event: 'message', id: null, data: [] };
  for (const line of frame.split('\n')) {
    if (line.startsWith('event:')) parsed.event = line.slice(6).trim();
    else if (line.startsWith('id:')) parsed.id = line.slice(3).trim();
    else if (line.startsWith('data:')) parsed.data.push(line.slice(5).trimStart());
  }
  return { event: parsed.event, id: parsed.id, data: parsed.data.join('\n') };
}

async function readWidgetSseUntilTurnStarted(baseURL, runId, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  try {
    const response = await fetch(new URL(`/api/runs/${runId}/events/stream?after=0`, baseURL), {
      headers: {
        accept: 'text/event-stream',
        'x-agent-hub-embed-token': token
      },
      signal: controller.signal
    });
    assert.equal(response.status, 200, `Widget SSE returned ${response.status}`);
    assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream\b/);
    assert.ok(response.body, 'Widget SSE must return a body');

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let previousSeq = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
      let separator = buffer.indexOf('\n\n');
      while (separator >= 0) {
        const frame = parseSseFrame(buffer.slice(0, separator));
        buffer = buffer.slice(separator + 2);
        if (frame.event === 'run_event') {
          const event = JSON.parse(frame.data);
          assert.equal(frame.id, String(event.seq));
          assert.equal(event.run_id, runId);
          assert.ok(event.seq > previousSeq, 'Widget SSE event sequence must increase');
          previousSeq = event.seq;
          if (event.event_type === 'turn_started') return event;
        }
        separator = buffer.indexOf('\n\n');
      }
    }
    throw new Error('Widget SSE closed before turn_started');
  } catch (error) {
    if (controller.signal.aborted && error?.name === 'AbortError') {
      throw new Error('Timed out waiting for Widget SSE turn_started');
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    controller.abort();
  }
}

async function readWidgetSessionSseEvent(baseURL, sessionId, token) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  try {
    const response = await fetch(
      new URL(`/api/widget/sessions/${sessionId}/events/stream?after=0`, baseURL),
      {
        headers: {
          accept: 'text/event-stream',
          'x-agent-hub-embed-token': token
        },
        signal: controller.signal
      }
    );
    assert.equal(response.status, 200, `Widget Session SSE returned ${response.status}`);
    assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream\b/);
    assert.ok(response.body, 'Widget Session SSE must return a body');

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
      let separator = buffer.indexOf('\n\n');
      while (separator >= 0) {
        const frame = parseSseFrame(buffer.slice(0, separator));
        buffer = buffer.slice(separator + 2);
        if (frame.event === 'run_event') return JSON.parse(frame.data);
        separator = buffer.indexOf('\n\n');
      }
    }
    throw new Error('Widget Session SSE closed before a Run event');
  } catch (error) {
    if (controller.signal.aborted && error?.name === 'AbortError') {
      throw new Error('Timed out waiting for Widget Session SSE event');
    }
    throw error;
  } finally {
    clearTimeout(timeout);
    controller.abort();
  }
}

async function corsPreflight(baseURL, origin) {
  const response = await fetch(new URL('/api/widget/runs', baseURL), {
    method: 'OPTIONS',
    headers: {
      origin,
      'access-control-request-method': 'POST',
      'access-control-request-headers': 'content-type,x-agent-hub-embed-token'
    }
  });
  return {
    status: response.status,
    allowedOrigin: response.headers.get('access-control-allow-origin')
  };
}

async function cleanupResource(action, errors) {
  try {
    await action();
  } catch (error) {
    errors.push(error);
  }
}

export default async function widgetEmbedApiScenario(context) {
  const ownerClient = new ApiClient(context.baseURL);
  const widgetClient = new ApiClient(context.baseURL);
  const { data: owner } = await loginAsAdmin(ownerClient);
  const agentIds = [];
  let primaryAgent = null;
  let secondaryAgent = null;
  let firstChannel = null;
  let firstChannelDisabled = false;
  let activeRun = null;
  let activeRunToken = null;
  let modelConnectionId = null;
  let scenarioError = null;
  const cleanupErrors = [];

  try {
    const { data: firstPlatform } = await ownerClient.post('/api/admin/external-platforms', {
      key: uniqueSlug(context, 'qa-widget-origin-one'),
      name: context.unique('QA Widget Origin One')
    });
    const { data: secondPlatform } = await ownerClient.post('/api/admin/external-platforms', {
      key: uniqueSlug(context, 'qa-widget-origin-two'),
      name: context.unique('QA Widget Origin Two')
    });
    ({ data: firstChannel } = await ownerClient.post(
      `/api/admin/external-platforms/${firstPlatform.id}/authentication-channels`,
      {
        key: uniqueSlug(context, 'qa-widget-channel-one'),
        name: context.unique('QA Widget Channel One'),
        enabled: true,
        trusted_email: true
      }
    ));
    const { data: secondChannel } = await ownerClient.post(
      `/api/admin/external-platforms/${secondPlatform.id}/authentication-channels`,
      {
        key: uniqueSlug(context, 'qa-widget-channel-two'),
        name: context.unique('QA Widget Channel Two'),
        enabled: true,
        trusted_email: true
      }
    );

    const modelFixture = await createComposeModelFixture(ownerClient, context);
    modelConnectionId = modelFixture.connectionId;

    ({ data: primaryAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Widget Primary Agent'),
      instructions: 'Exercise scoped Widget message, SSE, and stop behavior.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    }));
    ({ data: secondaryAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Widget Secondary Agent'),
      instructions: 'Exercise per-Agent Widget link and isolation behavior.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    }));
    agentIds.push(primaryAgent.id, secondaryAgent.id);
    assertUuid(primaryAgent.id, 'Primary Agent id');
    assertUuid(secondaryAgent.id, 'Secondary Agent id');

    const firstRedirectUri = new URL('/widget-origin-one/callback', context.baseURL).href;
    const secondRedirectUri = new URL('/widget-origin-two/callback', context.baseURL).href;
    const firstAppName = context.unique('QA Widget Origin One App');
    const { data: firstAppSecret } = await ownerClient.post('/api/integration-apps', {
      name: firstAppName,
      external_platform_id: firstPlatform.id,
      authentication_channel_id: firstChannel.id,
      redirect_uris: [firstRedirectUri],
      agent_ids: [primaryAgent.id, secondaryAgent.id],
      widget_history_enabled: true
    });
    const firstApp = firstAppSecret.integration_app;
    const { data: secondAppSecret } = await ownerClient.post('/api/integration-apps', {
      name: context.unique('QA Widget Origin Two App'),
      external_platform_id: secondPlatform.id,
      authentication_channel_id: secondChannel.id,
      redirect_uris: [secondRedirectUri],
      agent_ids: [secondaryAgent.id],
      widget_history_enabled: false
    });
    const secondApp = secondAppSecret.integration_app;
    assertUuid(firstApp.id, 'First Integration App id');
    assertUuid(secondApp.id, 'Second Integration App id');

    const { data: primaryLinkSession } = await ownerClient.post(
      `/api/integration-apps/${firstApp.id}/agents/${primaryAgent.id}/widget-session`
    );
    const { data: secondaryLinkSession } = await ownerClient.post(
      `/api/integration-apps/${firstApp.id}/agents/${secondaryAgent.id}/widget-session`
    );
    const { data: otherOriginSession } = await ownerClient.post(
      `/api/integration-apps/${secondApp.id}/agents/${secondaryAgent.id}/widget-session`
    );
    for (const session of [primaryLinkSession, secondaryLinkSession, otherOriginSession]) {
      assertOpaque(session.token, 'ahe_', 'Per-Agent Widget session token');
    }
    assert.equal(
      new Set([primaryLinkSession.token, secondaryLinkSession.token, otherOriginSession.token]).size,
      3,
      'Per-Agent and per-origin Widget sessions must be distinct'
    );
    const { data: primaryLinkAgent } = await widgetClient.get(
      '/api/widget/session',
      embedOptions(primaryLinkSession.token)
    );
    const { data: secondaryLinkAgent } = await widgetClient.get(
      '/api/widget/session',
      embedOptions(secondaryLinkSession.token)
    );
    const { data: otherOriginAgent } = await widgetClient.get(
      '/api/widget/session',
      embedOptions(otherOriginSession.token)
    );
    assert.deepEqual(Object.keys(primaryLinkAgent).sort(), ['id', 'instructions', 'name']);
    assert.equal(primaryLinkAgent.id, primaryAgent.id);
    assert.equal(secondaryLinkAgent.id, secondaryAgent.id);
    assert.equal(otherOriginAgent.id, secondaryAgent.id);

    const { data: directSession } = await ownerClient.post('/api/embed/sessions', {
      agent_id: primaryAgent.id
    });
    assertOpaque(directSession.token, 'ahe_', 'Owner-issued Widget session token');
    assert.equal(
      (await widgetClient.get('/api/widget/session', embedOptions(directSession.token))).data.id,
      primaryAgent.id
    );

    const embedJwt = signEmbedJwt(primaryAgent.id, owner.id);
    const { data: exchangedSession } = await widgetClient.post('/api/embed/exchange', {
      jwt: embedJwt
    });
    assertOpaque(exchangedSession.token, 'ahe_', 'Exchanged Widget session token');
    assert.equal(
      (await widgetClient.get('/api/widget/session', embedOptions(exchangedSession.token))).data.id,
      primaryAgent.id
    );
    await widgetClient.post('/api/embed/exchange', { jwt: embedJwt }, { expectedStatus: 401 });

    const applicationToken = await clientCredentialsToken(
      context.baseURL,
      firstApp,
      firstAppSecret.client_secret,
      primaryAgent.id
    );
    const { data: integrationSession } = await widgetClient.post(
      '/api/integrations/embed-session',
      { agent_id: primaryAgent.id },
      { headers: { authorization: `Bearer ${applicationToken}` } }
    );
    assertOpaque(integrationSession.token, 'ahe_', 'Integration Widget session token');
    assert.equal(
      (await widgetClient.get('/api/widget/session', embedOptions(integrationSession.token))).data.id,
      primaryAgent.id
    );

    const externalTenantId = uniqueSlug(context, 'qa-widget-tenant');
    const externalUserId = uniqueSlug(context, 'qa-widget-external-user');
    const externalAccessBody = {
      agent_id: primaryAgent.id,
      tenant_id: externalTenantId,
      external_user_id: externalUserId,
      username: `${externalUserId}-name`,
      display_name: 'QA External Widget User',
      email: `${externalUserId}@example.com`,
      attributes: { plan: 'qa', revision: 1 }
    };
    await widgetClient.post(
      '/api/widget/access',
      externalAccessBody,
      basicWidgetOptions(firstApp.client_id, `${firstAppSecret.client_secret}-invalid`, 401)
    );
    const { data: externalAccess } = await widgetClient.post(
      '/api/widget/access',
      externalAccessBody,
      basicWidgetOptions(firstApp.client_id, firstAppSecret.client_secret)
    );
    assertOpaque(externalAccess.token, 'ahw_', 'External Widget credential');
    assert.equal(externalAccess.agent.id, primaryAgent.id);
    assert.equal(externalAccess.history_enabled, true);
    const externalCredentialTtl = Date.parse(externalAccess.expires_at) - Date.now();
    assert.ok(externalCredentialTtl > 10 * 60_000 && externalCredentialTtl <= 16 * 60_000);
    const { data: externalWidgetSession } = await widgetClient.get(
      '/api/widget/session',
      embedOptions(externalAccess.token)
    );
    assert.equal(externalWidgetSession.id, primaryAgent.id);
    assert.equal(externalWidgetSession.history_enabled, true);

    const externalMessage = context.unique('QA external Widget history message');
    const { data: externalRun } = await widgetClient.post('/api/widget/runs', {
      message: externalMessage,
      parent_run_id: null,
      client_message_key: uniqueSlug(context, 'qa-external-widget-message')
    }, embedOptions(externalAccess.token));
    assertUuid(externalRun.id, 'External Widget Run id');
    assertUuid(externalRun.integration_session_id, 'External Widget Integration Session id');
    assertUuid(externalRun.hub_session_id, 'External Widget Hub Session id');
    await poll(
      async () => (await ownerClient.get(`/api/runs/${externalRun.id}`)).data,
      (run) => run.status === 'completed',
      { timeoutMs: 60_000, description: 'external Widget Run to complete through Pi' }
    );

    const { data: externalHistory } = await widgetClient.get(
      '/api/widget/sessions',
      embedOptions(externalAccess.token)
    );
    assert.equal(externalHistory.length, 1);
    assert.equal(externalHistory[0].id, externalRun.integration_session_id);
    assert.equal(externalHistory[0].hub_session_id, externalRun.hub_session_id);
    assert.equal(externalHistory[0].preview, externalMessage);
    const { data: externalMessages } = await widgetClient.get(
      `/api/widget/sessions/${externalRun.integration_session_id}/messages`,
      embedOptions(externalAccess.token)
    );
    assert.equal(
      externalMessages.some((message) => message.role === 'user' && message.content === externalMessage),
      true
    );
    const { data: externalEvents } = await widgetClient.get(
      `/api/widget/sessions/${externalRun.integration_session_id}/events`,
      embedOptions(externalAccess.token)
    );
    assert.equal(externalEvents.some((event) => event.run_id === externalRun.id), true);
    const externalStreamEvent = await readWidgetSessionSseEvent(
      context.baseURL,
      externalRun.integration_session_id,
      externalAccess.token
    );
    assert.equal(externalStreamEvent.run_id, externalRun.id);

    const { data: otherUserAccess } = await widgetClient.post('/api/widget/access', {
      ...externalAccessBody,
      external_user_id: `${externalUserId}-other`,
      username: `${externalUserId}-other`,
      email: `${externalUserId}-other@example.com`
    }, basicWidgetOptions(firstApp.client_id, firstAppSecret.client_secret));
    assert.deepEqual(
      (await widgetClient.get('/api/widget/sessions', embedOptions(otherUserAccess.token))).data,
      []
    );
    await widgetClient.get(
      `/api/widget/sessions/${externalRun.integration_session_id}/messages`,
      embedOptions(otherUserAccess.token, 404)
    );
    const { data: otherTenantAccess } = await widgetClient.post('/api/widget/access', {
      ...externalAccessBody,
      tenant_id: `${externalTenantId}-other`
    }, basicWidgetOptions(firstApp.client_id, firstAppSecret.client_secret));
    assert.deepEqual(
      (await widgetClient.get('/api/widget/sessions', embedOptions(otherTenantAccess.token))).data,
      []
    );
    await widgetClient.get(
      `/api/widget/sessions/${externalRun.integration_session_id}/messages`,
      embedOptions(otherTenantAccess.token, 404)
    );

    const trustedRenewalHeaders = {
      ...embedOptions(externalAccess.token).headers,
      ...basicWidgetOptions(firstApp.client_id, firstAppSecret.client_secret).headers
    };
    const { data: renewedExternalAccess } = await widgetClient.post(
      '/api/widget/session/renew',
      {
        profile: {
          username: `${externalUserId}-name`,
          display_name: 'QA External Widget User Updated',
          email: `${externalUserId}@example.com`,
          attributes: { plan: 'qa', revision: 2 }
        }
      },
      { headers: trustedRenewalHeaders }
    );
    assertOpaque(renewedExternalAccess.token, 'ahw_', 'Renewed external Widget credential');
    assert.notEqual(renewedExternalAccess.token, externalAccess.token);
    await widgetClient.get('/api/widget/session', embedOptions(externalAccess.token, 401));
    const { data: continuedExternalRun } = await widgetClient.post('/api/widget/runs', {
      message: context.unique('QA renewed Widget continuation'),
      integration_session_id: externalRun.integration_session_id,
      hub_session_id: externalRun.hub_session_id,
      parent_run_id: null,
      client_message_key: uniqueSlug(context, 'qa-renewed-widget-message')
    }, embedOptions(renewedExternalAccess.token));
    assert.equal(continuedExternalRun.integration_session_id, externalRun.integration_session_id);
    assert.equal(continuedExternalRun.hub_session_id, externalRun.hub_session_id);
    await poll(
      async () => (await ownerClient.get(`/api/runs/${continuedExternalRun.id}`)).data,
      (run) => run.status === 'completed',
      { timeoutMs: 60_000, description: 'renewed external Widget continuation to complete' }
    );

    const { data: historyDisabledApp } = await ownerClient.request(
      `/api/integration-apps/${firstApp.id}`,
      {
        method: 'PATCH',
        body: {
          name: firstAppName,
          redirect_uris: [firstRedirectUri],
          agent_ids: [primaryAgent.id, secondaryAgent.id],
          widget_history_enabled: false
        }
      }
    );
    assert.equal(historyDisabledApp.widget_history_enabled, false);
    await widgetClient.get(
      '/api/widget/sessions',
      embedOptions(renewedExternalAccess.token, 403)
    );
    const { data: exactHistoryAfterDisable } = await widgetClient.get(
      `/api/widget/sessions/${externalRun.integration_session_id}/messages`,
      embedOptions(renewedExternalAccess.token)
    );
    assert.equal(exactHistoryAfterDisable.length >= 2, true);

    const allowedPreflight = await corsPreflight(context.baseURL, 'http://localhost:5173');
    assert.ok([200, 204].includes(allowedPreflight.status));
    assert.equal(allowedPreflight.allowedOrigin, 'http://localhost:5173');
    const rejectedPreflight = await corsPreflight(context.baseURL, 'https://cross-origin.invalid');
    assert.notEqual(rejectedPreflight.status, 500);
    assert.equal(rejectedPreflight.allowedOrigin, null);

    activeRunToken = primaryLinkSession.token;
    const widgetMessage = context.unique('QA Widget message through fake provider');
    const { data: messageRun } = await widgetClient.post('/api/widget/runs', {
      message: widgetMessage,
      hub_session_id: null,
      parent_run_id: null,
      client_message_key: uniqueSlug(context, 'qa-widget-message')
    }, embedOptions(activeRunToken));
    assertUuid(messageRun.id, 'Widget Run id');
    assertUuid(messageRun.hub_session_id, 'Widget Hub Session id');
    assert.equal(messageRun.status, 'pending');
    assert.equal(messageRun.source, 'widget');
    assert.equal(messageRun.initial_message, widgetMessage);
    await poll(
      async () => (await ownerClient.get(`/api/runs/${messageRun.id}`)).data,
      (run) => run.status === 'completed',
      { timeoutMs: 60_000, description: 'Widget message Run to complete' }
    );
    const messageTurnStarted = await readWidgetSseUntilTurnStarted(
      context.baseURL,
      messageRun.id,
      activeRunToken
    );
    assert.equal(messageTurnStarted.event_type, 'turn_started');

    await widgetClient.post('/api/widget/runs', {
      message: 'Cross-session Widget message must be rejected.',
      hub_session_id: messageRun.hub_session_id,
      parent_run_id: null,
      client_message_key: uniqueSlug(context, 'qa-widget-cross-session')
    }, embedOptions(exchangedSession.token, 400));
    await widgetClient.get(
      `/api/runs/${messageRun.id}/events/stream?after=0`,
      embedOptions(exchangedSession.token, 403)
    );
    await widgetClient.get(
      `/api/runs/${messageRun.id}/events/stream?after=0`,
      embedOptions(otherOriginSession.token, 403)
    );

    const { data: holdAcceptance } = await ownerClient.post(
      `/api/sessions/${messageRun.hub_session_id}/messages`,
      {
        content: HOLD_MARKER,
        client_message_key: uniqueSlug(context, 'qa-widget-hold')
      }
    );
    activeRun = holdAcceptance.run;
    assert.ok(activeRun, 'Console hold message must schedule a Run');
    assert.equal(activeRun.source, 'console');
    assert.equal(activeRun.hub_session_id, messageRun.hub_session_id);
    await poll(async () => {
      const { data: events } = await ownerClient.get(`/api/runs/${activeRun.id}/events`);
      return events.find((event) => event.event_type === 'turn_started') ?? null;
    }, Boolean, {
      timeoutMs: 60_000,
      description: 'Console hold Run to start its native Turn'
    });

    const { data: joinedRun } = await widgetClient.post('/api/widget/runs', {
      message: context.unique('Widget joins the active Turn'),
      hub_session_id: messageRun.hub_session_id,
      parent_run_id: null,
      client_message_key: uniqueSlug(context, 'qa-widget-steer')
    }, embedOptions(activeRunToken));
    assert.equal(joinedRun.id, activeRun.id);
    assert.equal(joinedRun.source, 'console');

    await widgetClient.post(
      `/api/widget/runs/${activeRun.id}/stop`,
      undefined,
      embedOptions(otherOriginSession.token, 404)
    );

    const { data: stopRequested } = await widgetClient.post(
      `/api/widget/runs/${activeRun.id}/stop`,
      undefined,
      embedOptions(activeRunToken)
    );
    assert.equal(stopRequested.id, activeRun.id);
    const interruptedRun = await poll(
      async () => (await ownerClient.get(`/api/runs/${activeRun.id}`)).data,
      (run) => run.status === 'interrupted',
      { timeoutMs: 60_000, description: 'Widget Run to stop' }
    );
    assert.equal(interruptedRun.hub_session_id, messageRun.hub_session_id);

    const { data: delegatedOnlyToSecondary } = await ownerClient.request(
      `/api/integration-apps/${firstApp.id}`,
      {
        method: 'PATCH',
        body: {
          name: firstAppName,
          redirect_uris: [firstRedirectUri],
          agent_ids: [secondaryAgent.id],
          widget_history_enabled: false
        }
      }
    );
    assert.deepEqual(delegatedOnlyToSecondary.agent_ids, [secondaryAgent.id]);
    await widgetClient.get('/api/widget/session', embedOptions(primaryLinkSession.token, 401));
    await widgetClient.get('/api/widget/session', embedOptions(integrationSession.token, 401));
    assert.equal(
      (await widgetClient.get('/api/widget/session', embedOptions(secondaryLinkSession.token))).data.id,
      secondaryAgent.id
    );

    await ownerClient.request(`/api/admin/authentication-channels/${firstChannel.id}`, {
      method: 'PATCH',
      body: {
        name: firstChannel.name,
        enabled: false,
        trusted_email: true
      }
    });
    firstChannelDisabled = true;
    await widgetClient.get('/api/widget/session', embedOptions(secondaryLinkSession.token, 401));
    assert.equal(
      (await widgetClient.get('/api/widget/session', embedOptions(otherOriginSession.token))).data.id,
      secondaryAgent.id,
      'A different External Platform and channel must remain isolated'
    );
  } catch (error) {
    scenarioError = error;
  } finally {
    if (firstChannelDisabled && firstChannel) {
      await cleanupResource(
        () => ownerClient.request(`/api/admin/authentication-channels/${firstChannel.id}`, {
          method: 'PATCH',
          body: {
            name: firstChannel.name,
            enabled: true,
            trusted_email: true
          }
        }),
        cleanupErrors
      );
    }
    if (activeRun && activeRunToken) {
      await cleanupResource(async () => {
        const current = (await ownerClient.get(`/api/runs/${activeRun.id}`)).data;
        if (!TERMINAL_STATUSES.has(current.status)) {
          await widgetClient.post(
            `/api/widget/runs/${activeRun.id}/stop`,
            undefined,
            embedOptions(activeRunToken, [200, 404, 409])
          );
        }
      }, cleanupErrors);
    }
    for (const agentId of agentIds.toReversed()) {
      await cleanupResource(
        () => ownerClient.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] }),
        cleanupErrors
      );
    }
    if (modelConnectionId) {
      await cleanupResource(
        () => ownerClient.delete(`/api/model-connections/${modelConnectionId}`, {
          expectedStatus: [204, 404]
        }),
        cleanupErrors
      );
    }
  }

  if (scenarioError && cleanupErrors.length > 0) {
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Widget scenario and cleanup failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, 'Widget scenario cleanup failed');
  }
}
