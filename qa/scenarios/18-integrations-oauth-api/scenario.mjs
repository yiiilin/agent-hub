import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, provisionLocalUser } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'cancelled', 'interrupted']);

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Integration model'),
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

function bearerOptions(token, expectedStatus) {
  return {
    headers: { authorization: `Bearer ${token}` },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

function embedOptions(token, expectedStatus) {
  return {
    headers: { 'x-agent-hub-embed-token': token },
    ...(expectedStatus === undefined ? {} : { expectedStatus })
  };
}

async function manualRedirect(client, path, expectedStatus = 303) {
  const headers = { accept: 'application/json' };
  const cookie = client.cookieHeader();
  if (cookie) headers.cookie = cookie;
  const response = await fetch(new URL(path, client.baseURL), {
    headers,
    redirect: 'manual'
  });
  client.absorbCookies(response.headers);
  assert.equal(response.status, expectedStatus, `Manual redirect returned ${response.status}`);
  return response.headers.get('location');
}

function authorizationPath({ clientId, redirectUri, externalUserId, tenantId, scope, state }) {
  return `/api/oauth/authorize?${new URLSearchParams({
    client_id: clientId,
    redirect_uri: redirectUri,
    external_user_id: externalUserId,
    tenant_id: tenantId,
    scope,
    state
  })}`;
}

async function authorize(client, parameters, expectedStatus = 303) {
  const location = await manualRedirect(client, authorizationPath(parameters), expectedStatus);
  if (expectedStatus !== 303) return null;
  assert.equal(typeof location, 'string', 'OAuth authorize must return a redirect location');
  const callback = new URL(location);
  assert.equal(callback.searchParams.get('state'), parameters.state);
  const code = callback.searchParams.get('code');
  assert.equal(typeof code === 'string' && code.length > 20, true, 'OAuth redirect must include a code');
  return code;
}

async function oauthToken(baseURL, form, expectedStatus = 200) {
  const response = await fetch(new URL('/api/oauth/token', baseURL), {
    method: 'POST',
    headers: {
      accept: 'application/json',
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: new URLSearchParams(form)
  });
  assert.equal(response.status, expectedStatus, `OAuth token endpoint returned ${response.status}`);
  const text = await response.text();
  return text ? JSON.parse(text) : null;
}

async function waitForRun(client, runId, accept, description, timeoutMs = 90_000) {
  return poll(
    async () => (await client.get(`/api/runs/${runId}`)).data,
    accept,
    { timeoutMs, intervalMs: 250, description }
  );
}

async function waitForEvent(client, sessionId, token, accept, description, timeoutMs = 60_000) {
  let after = 0;
  return poll(async () => {
    const { data: events } = await client.get(
      `/api/integrations/sessions/${sessionId}/events?after=${after}`,
      bearerOptions(token)
    );
    for (const event of events) {
      after = Math.max(after, event.seq);
      if (accept(event)) return event;
    }
    return null;
  }, Boolean, { timeoutMs, intervalMs: 300, description });
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

function openIntegrationSse(baseURL, sessionId, token) {
  const controller = new AbortController();
  let readyResolve;
  let readyReject;
  let readySettled = false;
  const ready = new Promise((resolve, reject) => {
    readyResolve = resolve;
    readyReject = reject;
  });
  const timeout = setTimeout(() => controller.abort(), 20_000);
  const outcome = (async () => {
    try {
      const response = await fetch(
        new URL(`/api/integrations/sessions/${sessionId}/events/stream?after=0`, baseURL),
        { headers: { authorization: `Bearer ${token}` }, signal: controller.signal }
      );
      assert.equal(response.status, 200, `Integration SSE returned ${response.status}`);
      assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream\b/);
      assert.ok(response.body, 'Integration SSE must return a body');
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) return 'closed';
        buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
        let separator = buffer.indexOf('\n\n');
        while (separator >= 0) {
          const frame = parseSseFrame(buffer.slice(0, separator));
          buffer = buffer.slice(separator + 2);
          if (frame.event === 'integration_event' && !readySettled) {
            readySettled = true;
            readyResolve(frame);
          }
          if (frame.event === 'error') return 'error';
          separator = buffer.indexOf('\n\n');
        }
      }
    } catch (error) {
      if (controller.signal.aborted && error?.name === 'AbortError') return 'aborted';
      if (!readySettled) {
        readySettled = true;
        readyReject(error);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  })();
  return { ready, outcome, abort: () => controller.abort() };
}

function updateAgentPayload(agent, overrides = {}) {
  return {
    name: agent.name,
    instructions: agent.instructions,
    visibility: agent.visibility,
    public_to: agent.public_to,
    runtime_id: agent.runtime_id,
    model_selection: agent.model_selection,
    model_settings: agent.model_settings,
    subagents: agent.subagents,
    sandbox_policy: agent.sandbox_policy,
    managed_skill_ids: agent.managed_skill_ids,
    mcp_allowlist: agent.mcp_allowlist,
    ...overrides
  };
}

async function setAgentRuntime(client, agentId, runtimeId) {
  const current = (await client.get(`/api/agents/${agentId}`)).data;
  const updated = (await client.request(`/api/agents/${agentId}`, {
    method: 'PATCH',
    body: updateAgentPayload(current, { runtime_id: runtimeId })
  })).data;
  assert.equal(updated.runtime_id, runtimeId);
  return current.runtime_id;
}

async function cleanupResource(action, errors) {
  try {
    await action();
  } catch (error) {
    errors.push(error);
  }
}

export default async function integrationsOauthApiScenario(context) {
  const ownerClient = new ApiClient(context.baseURL);
  const integrationClient = new ApiClient(context.baseURL);
  const runtimeClient = new ApiClient(context.baseURL);
  const { data: owner } = await loginAsAdmin(ownerClient);
  const externalUserId = uniqueSlug(context, 'qa-integration-owner');
  const tenantId = 'default';

  const agentIds = [];
  let primaryAgent = null;
  let secondaryAgent = null;
  let idleRuntime = null;
  let secondaryOriginalRuntime;
  let secondaryRuntimeChanged = false;
  let concurrencyRun = null;
  let concurrencySession = null;
  let appAccessToken = null;
  let liveSse = null;
  let modelConnectionId = null;
  let scenarioError = null;
  const cleanupErrors = [];

  try {
    const { data: trustedPlatform } = await ownerClient.post('/api/admin/external-platforms', {
      key: uniqueSlug(context, 'qa-integration-platform'),
      name: context.unique('QA Integration Platform')
    });
    const { data: trustedChannel } = await ownerClient.post(
      `/api/admin/external-platforms/${trustedPlatform.id}/authentication-channels`,
      {
        key: uniqueSlug(context, 'qa-integration-channel'),
        name: context.unique('QA Integration Trusted Channel'),
        enabled: true,
        trusted_email: true
      }
    );

    const modelFixture = await createComposeModelFixture(ownerClient, context);
    modelConnectionId = modelFixture.connectionId;

    ({ data: primaryAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Integration Primary Agent'),
      instructions: 'Exercise OAuth, Session continuation, tools, and revocation.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    }));
    ({ data: secondaryAgent } = await ownerClient.post('/api/agents', {
      name: context.unique('QA Integration Secondary Agent'),
      instructions: 'Exercise unaffected delegation and concurrent message serialization.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    }));
    agentIds.push(primaryAgent.id, secondaryAgent.id);
    assertUuid(primaryAgent.id, 'Primary Agent id');
    assertUuid(secondaryAgent.id, 'Secondary Agent id');

    const redirectUri = new URL('/oauth/callback', context.baseURL).href;
    const secondRedirectUri = new URL('/oauth/secondary', context.baseURL).href;
    const appName = context.unique('QA Integration OAuth App');
    const { data: createdSecret } = await ownerClient.post('/api/integration-apps', {
      name: appName,
      external_platform_id: trustedPlatform.id,
      authentication_channel_id: trustedChannel.id,
      redirect_uris: [redirectUri, secondRedirectUri],
      agent_ids: [primaryAgent.id, secondaryAgent.id]
    });
    const app = createdSecret.integration_app;
    const initialSecret = createdSecret.client_secret;
    assertUuid(app.id, 'Integration App id');
    assertOpaque(app.client_id, 'ahc_', 'OAuth client id');
    assertOpaque(initialSecret, 'ahs_', 'OAuth client secret');
    assert.deepEqual(app.agent_ids, [primaryAgent.id, secondaryAgent.id].sort());

    const { data: listedApps } = await ownerClient.get('/api/integration-apps');
    const listedApp = listedApps.find((candidate) => candidate.id === app.id);
    assert.ok(listedApp, 'Created Integration App must be listed for its owner');
    assert.equal(Object.hasOwn(listedApp, 'client_secret'), false);
    assert.equal(JSON.stringify(listedApps).includes(initialSecret), false);
    const { data: fetchedApp } = await ownerClient.get(`/api/integration-apps/${app.id}`);
    assert.equal(Object.hasOwn(fetchedApp, 'client_secret'), false);
    assert.equal(fetchedApp.external_platform_id, trustedPlatform.id);
    assert.equal(fetchedApp.authentication_channel_id, trustedChannel.id);

    const outsider = await provisionLocalUser(ownerClient, context, 'qa-integration-outsider');
    assert.equal((await outsider.client.get('/api/integration-apps')).data.some((item) => item.id === app.id), false);
    await outsider.client.get(`/api/integration-apps/${app.id}`, { expectedStatus: 404 });

    const { data: openapi } = await ownerClient.get('/openapi.json');
    const updateSchema = openapi.components.schemas.UpdateIntegrationAppRequest;
    assert.equal(updateSchema.additionalProperties, false);
    assert.equal(Object.hasOwn(updateSchema.properties, 'external_platform_id'), false);
    assert.equal(Object.hasOwn(updateSchema.properties, 'authentication_channel_id'), false);

    await ownerClient.request(`/api/integration-apps/${app.id}`, {
      method: 'PATCH',
      body: {
        name: appName,
        redirect_uris: [redirectUri, secondRedirectUri],
        agent_ids: [primaryAgent.id, primaryAgent.id]
      },
      expectedStatus: 400
    });
    const editedName = context.unique('QA Integration OAuth App Edited');
    const { data: editedApp } = await ownerClient.request(`/api/integration-apps/${app.id}`, {
      method: 'PATCH',
      body: {
        name: editedName,
        redirect_uris: [secondRedirectUri, redirectUri],
        agent_ids: [primaryAgent.id, secondaryAgent.id]
      }
    });
    assert.equal(editedApp.external_platform_id, trustedPlatform.id);
    assert.equal(editedApp.authentication_channel_id, trustedChannel.id);
    assert.deepEqual(editedApp.redirect_uris, [secondRedirectUri, redirectUri]);

    const bothAgentScope = `agent:${primaryAgent.id} agent:${secondaryAgent.id}`;
    await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: initialSecret,
      scope: ''
    }, 400);
    await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: initialSecret,
      scope: 'agent:not-a-uuid'
    }, 400);
    const { access_token: preRotationToken } = await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: initialSecret,
      scope: bothAgentScope
    });
    assertOpaque(preRotationToken, 'aho_', 'Application access token');
    appAccessToken = preRotationToken;

    const { data: rotatedSecretResponse } = await ownerClient.post(
      `/api/integration-apps/${app.id}/rotate-secret`
    );
    const rotatedSecret = rotatedSecretResponse.client_secret;
    assertOpaque(rotatedSecret, 'ahs_', 'Rotated OAuth client secret');
    assert.equal(rotatedSecret === initialSecret, false, 'Rotation must return a different secret');
    assert.equal(Object.hasOwn(rotatedSecretResponse.integration_app, 'client_secret'), false);
    await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: initialSecret,
      scope: bothAgentScope
    }, 401);
    const { access_token: rotatedAccessToken } = await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      scope: bothAgentScope
    });
    assertOpaque(rotatedAccessToken, 'aho_', 'Rotated-secret access token');
    const afterRotationList = (await ownerClient.get('/api/integration-apps')).data;
    assert.equal(JSON.stringify(afterRotationList).includes(initialSecret), false);
    assert.equal(JSON.stringify(afterRotationList).includes(rotatedSecret), false);

    await integrationClient.get('/api/oauth/userinfo', bearerOptions(appAccessToken, 403));
    await integrationClient.get('/api/auth/me', bearerOptions(appAccessToken, 401));
    await integrationClient.get('/api/agents', bearerOptions(appAccessToken, 401));

    const externalUsername = context.unique('QA external profile username');
    const primarySessionBody = {
      agent_id: primaryAgent.id,
      external_user_id: externalUserId,
      tenant_id: tenantId,
      username: externalUsername,
      display_name: owner.display_name,
      tools: [{ name: 'echo', description: 'Echo integration input', parameters: { type: 'object' } }],
      metadata: { source: 'qa', scenario: 'oauth-tool-continuation' }
    };
    await integrationClient.post(
      '/api/integrations/sessions',
      primarySessionBody,
      bearerOptions(appAccessToken, 400)
    );
    await integrationClient.post(
      '/api/integrations/sessions',
      { ...primarySessionBody, email: 'invalid-email' },
      bearerOptions(appAccessToken, 400)
    );
    const { data: primarySession } = await integrationClient.post(
      '/api/integrations/sessions',
      { ...primarySessionBody, email: owner.email },
      bearerOptions(appAccessToken)
    );
    assertUuid(primarySession.id, 'Primary Integration Session id');
    assert.equal(primarySession.hub_session_id !== primarySession.id, true);
    assert.equal(primarySession.platform_id, trustedPlatform.id);
    assert.equal(primarySession.tenant_id, tenantId);
    assert.equal(primarySession.external_user_id, externalUserId);
    assert.deepEqual(primarySession.metadata, primarySessionBody.metadata);

    const fullScope = `profile email external_profile agent:${primaryAgent.id}`;
    const codeParameters = {
      clientId: app.client_id,
      redirectUri,
      externalUserId,
      tenantId,
      scope: fullScope,
      state: context.unique('oauth-full-state')
    };
    const code = await authorize(ownerClient, codeParameters);
    await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code,
      redirect_uri: secondRedirectUri,
      scope: fullScope
    }, 401);
    await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code,
      redirect_uri: redirectUri,
      scope: `profile agent:${primaryAgent.id}`
    }, 400);
    const userTokenResponse = await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code,
      redirect_uri: redirectUri,
      scope: fullScope
    });
    const userAccessToken = userTokenResponse.access_token;
    assertOpaque(userAccessToken, 'aho_', 'User access token');
    await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code,
      redirect_uri: redirectUri,
      scope: fullScope
    }, 401);

    await integrationClient.get('/api/auth/me', bearerOptions(userAccessToken, 401));
    await integrationClient.get('/api/agents', bearerOptions(userAccessToken, 401));
    const { data: userinfo } = await integrationClient.get(
      '/api/oauth/userinfo',
      bearerOptions(userAccessToken)
    );
    assert.equal(userinfo.sub, owner.id);
    assert.equal(userinfo.email, owner.email);
    assert.equal(Object.hasOwn(userinfo, 'username'), false, 'Hub profile must not expose username');
    assert.equal(userinfo.external_profile.platform_id, trustedPlatform.id);
    assert.equal(userinfo.external_profile.tenant_id, tenantId);
    assert.equal(userinfo.external_profile.external_user_id, externalUserId);
    assert.equal(userinfo.external_profile.username, externalUsername);

    const profileScope = `profile agent:${primaryAgent.id}`;
    const profileCode = await authorize(ownerClient, {
      ...codeParameters,
      scope: profileScope,
      state: context.unique('oauth-profile-state')
    });
    const { access_token: profileToken } = await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code: profileCode,
      redirect_uri: redirectUri,
      scope: profileScope
    });
    const { data: profileUserinfo } = await integrationClient.get(
      '/api/oauth/userinfo',
      bearerOptions(profileToken)
    );
    assert.equal(profileUserinfo.sub, owner.id);
    assert.equal(profileUserinfo.name, owner.display_name);
    assert.equal(Object.hasOwn(profileUserinfo, 'username'), false);
    assert.equal(Object.hasOwn(profileUserinfo, 'email'), false);
    assert.equal(Object.hasOwn(profileUserinfo, 'external_profile'), false);

    const minimalScope = `agent:${primaryAgent.id}`;
    const minimalCode = await authorize(ownerClient, {
      ...codeParameters,
      scope: minimalScope,
      state: context.unique('oauth-minimal-state')
    });
    const { access_token: minimalToken } = await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code: minimalCode,
      redirect_uri: redirectUri,
      scope: minimalScope
    });
    const { data: minimalUserinfo } = await integrationClient.get(
      '/api/oauth/userinfo',
      bearerOptions(minimalToken)
    );
    assert.equal(minimalUserinfo.sub, owner.id);
    assert.equal(Object.hasOwn(minimalUserinfo, 'username'), false);
    assert.equal(Object.hasOwn(minimalUserinfo, 'email'), false);
    assert.equal(Object.hasOwn(minimalUserinfo, 'external_profile'), false);

    const missingTenantBody = { ...primarySessionBody };
    delete missingTenantBody.tenant_id;
    await integrationClient.post(
      '/api/integrations/sessions',
      missingTenantBody,
      bearerOptions(userAccessToken, 400)
    );
    await integrationClient.post('/api/integrations/sessions', {
      ...primarySessionBody,
      external_user_id: context.unique('wrong-external-user')
    }, bearerOptions(userAccessToken, 403));
    await integrationClient.post('/api/integrations/sessions', {
      ...primarySessionBody,
      tenant_id: context.unique('wrong-tenant')
    }, bearerOptions(userAccessToken, 403));
    assert.equal(
      (await integrationClient.get(
        `/api/integrations/sessions/${primarySession.id}`,
        bearerOptions(userAccessToken)
      )).data.id,
      primarySession.id,
      'Authorization-code identity must reuse the trusted-email binding'
    );

    const appOrigins = [
      {
        external_user_id: context.unique('origin-user-x'),
        tenant_id: context.unique('tenant-a'),
        email: `${uniqueSlug(context, 'origin-user-x-a')}@example.com`
      },
      {
        external_user_id: context.unique('origin-user-x'),
        tenant_id: context.unique('tenant-b'),
        email: `${uniqueSlug(context, 'origin-user-x-b')}@example.com`
      },
      {
        external_user_id: context.unique('origin-user-y'),
        tenant_id: context.unique('tenant-a'),
        email: `${uniqueSlug(context, 'origin-user-y-a')}@example.com`
      }
    ];
    const appSessions = [];
    for (const origin of appOrigins) {
      const { data: session } = await integrationClient.post('/api/integrations/sessions', {
        agent_id: primaryAgent.id,
        ...origin,
        tools: [],
        metadata: { source: 'qa-origin-isolation' }
      }, bearerOptions(appAccessToken));
      assert.equal(session.platform_id, trustedPlatform.id);
      assert.equal(session.external_user_id, origin.external_user_id);
      assert.equal(session.tenant_id, origin.tenant_id);
      appSessions.push(session);
    }
    assert.equal(new Set(appSessions.map((session) => session.id)).size, 3);
    assert.equal(new Set(appSessions.map((session) => session.hub_session_id)).size, 3);
    assert.equal(new Set(appSessions.map((session) => session.external_identity_id)).size, 3);
    for (const session of appSessions) {
      const { data: fetched } = await integrationClient.get(
        `/api/integrations/sessions/${session.id}`,
        bearerOptions(appAccessToken)
      );
      assert.equal(fetched.external_identity_id, session.external_identity_id);
      await integrationClient.get(
        `/api/integrations/sessions/${session.id}`,
        bearerOptions(userAccessToken, 404)
      );
    }

    const customPlatformKey = uniqueSlug(context, 'qa-integration-platform');
    const { data: customPlatform } = await ownerClient.post('/api/admin/external-platforms', {
      key: customPlatformKey,
      name: context.unique('QA Integration Custom Platform')
    });
    const { data: customChannel } = await ownerClient.post(
      `/api/admin/external-platforms/${customPlatform.id}/authentication-channels`,
      {
        key: uniqueSlug(context, 'qa-integration-channel'),
        name: context.unique('QA Integration Custom Channel'),
        enabled: true,
        trusted_email: true
      }
    );
    const { data: customSecretResponse } = await ownerClient.post('/api/integration-apps', {
      name: context.unique('QA Cross Platform App'),
      external_platform_id: customPlatform.id,
      authentication_channel_id: customChannel.id,
      redirect_uris: [new URL('/custom/callback', context.baseURL).href],
      agent_ids: [primaryAgent.id]
    });
    const customApp = customSecretResponse.integration_app;
    const { access_token: customToken } = await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: customApp.client_id,
      client_secret: customSecretResponse.client_secret,
      scope: `agent:${primaryAgent.id}`
    });
    const { data: customSession } = await integrationClient.post('/api/integrations/sessions', {
      agent_id: primaryAgent.id,
      external_user_id: appOrigins[0].external_user_id,
      tenant_id: appOrigins[0].tenant_id,
      email: appOrigins[0].email,
      tools: [],
      metadata: { source: 'qa-cross-platform' }
    }, bearerOptions(customToken));
    assert.equal(customSession.platform_id, customPlatform.id);
    assert.notEqual(customSession.external_identity_id, appSessions[0].external_identity_id);
    await integrationClient.get(
      `/api/integrations/sessions/${appSessions[0].id}`,
      bearerOptions(customToken, 404)
    );

    const messageContent = context.unique('Please use the echo tool and preserve attachments');
    const attachments = [
      {
        kind: 'text',
        name: 'qa-note.txt',
        content_type: 'text/plain',
        size_bytes: 32,
        text: 'quoted text, arrays [1, 2], and a second line\nkept exactly'
      },
      {
        kind: 'url',
        name: 'qa-reference',
        content_type: 'text/html',
        size_bytes: 0,
        url: 'https://example.com/reference?source=qa'
      }
    ];
    const messageKey = uniqueSlug(context, 'qa-tool-message');
    const { data: firstAcceptance } = await integrationClient.post(
      `/api/integrations/sessions/${primarySession.id}/messages`,
      { content: messageContent, attachments, client_message_key: messageKey },
      bearerOptions(userAccessToken)
    );
    assert.equal(firstAcceptance.run.integration_session_id, primarySession.id);
    assert.equal(firstAcceptance.run.hub_session_id, primarySession.hub_session_id);
    assert.equal(firstAcceptance.run.source, 'integration:message');
    const { data: duplicateAcceptance } = await integrationClient.post(
      `/api/integrations/sessions/${primarySession.id}/messages`,
      { content: messageContent, attachments, client_message_key: messageKey },
      bearerOptions(userAccessToken)
    );
    assert.equal(duplicateAcceptance.run.id, firstAcceptance.run.id);
    assert.equal(duplicateAcceptance.message.id, firstAcceptance.message.id);
    await integrationClient.post(
      `/api/integrations/sessions/${primarySession.id}/messages`,
      {
        content: 'Invalid URL attachment.',
        attachments: [{ kind: 'url', name: 'bad-url', url: 'file:///tmp/nope' }]
      },
      bearerOptions(userAccessToken, 400)
    );

    const toolRequestEvent = await waitForEvent(
      integrationClient,
      primarySession.id,
      userAccessToken,
      (event) => event.run_id === firstAcceptance.run.id && event.event_type === 'tool_request',
      'Integration tool request event'
    );
    const toolRequestId = toolRequestEvent.payload.tool_request_id;
    assertUuid(toolRequestId, 'Tool request id');
    assert.equal(toolRequestEvent.payload.tool_name, 'echo');
    assert.equal(toolRequestEvent.payload.source_id, 'platform|tool-call');
    assert.equal(toolRequestEvent.payload.arguments.message, messageContent);
    assert.deepEqual(toolRequestEvent.payload.arguments.attachments, [
      { ...attachments[0], url: null },
      { ...attachments[1], text: null }
    ]);
    const waitingToolRun = await waitForRun(
      ownerClient,
      firstAcceptance.run.id,
      (run) => run.status === 'waiting_tool',
      'Integration message Run to wait for a tool result'
    );

    const toolResultPayload = {
      text: 'tool result with quotes and paths',
      nested: { values: [1, true, null, { line: 'first\nsecond' }] }
    };
    await integrationClient.post(
      `/api/integrations/tool-requests/${toolRequestId}/result`,
      { result: null },
      bearerOptions(userAccessToken, 400)
    );
    const { data: toolResultAcceptance } = await integrationClient.post(
      `/api/integrations/tool-requests/${toolRequestId}/result`,
      { result: toolResultPayload },
      bearerOptions(userAccessToken)
    );
    assert.equal(toolResultAcceptance.run.parent_run_id, firstAcceptance.run.id);
    assert.equal(toolResultAcceptance.run.integration_session_id, primarySession.id);
    assert.equal(toolResultAcceptance.run.source, 'integration:tool_result');
    assert.equal(toolResultAcceptance.tool_request.status, 'completed');
    assert.deepEqual(toolResultAcceptance.tool_request.result_payload, toolResultPayload);
    const { data: duplicateToolResult } = await integrationClient.post(
      `/api/integrations/tool-requests/${toolRequestId}/result`,
      { result: toolResultPayload },
      bearerOptions(userAccessToken)
    );
    assert.equal(duplicateToolResult.run.id, toolResultAcceptance.run.id);
    await waitForEvent(
      integrationClient,
      primarySession.id,
      userAccessToken,
      (event) => event.run_id === toolResultAcceptance.run.id
        && event.event_type === 'tool_result'
        && event.payload?.message?.tool_request_id === toolRequestId,
      'Integration tool result event'
    );
    const completedToolRun = await waitForRun(
      ownerClient,
      toolResultAcceptance.run.id,
      (run) => run.status === 'completed',
      'Integration tool-result Run to complete'
    );
    assert.equal(completedToolRun.session_id, waitingToolRun.session_id);
    assert.equal(completedToolRun.work_dir_ref, waitingToolRun.work_dir_ref);
    const firstHubSession = (await ownerClient.get(`/api/sessions/${primarySession.hub_session_id}`)).data;
    assert.equal(typeof firstHubSession.native_session_id, 'string');

    const secondContent = context.unique('Continue the same external Session normally');
    const { data: secondAcceptance } = await integrationClient.post(
      `/api/integrations/sessions/${primarySession.id}/messages`,
      { content: secondContent, attachments: [], client_message_key: uniqueSlug(context, 'qa-next-turn') },
      bearerOptions(userAccessToken)
    );
    assert.equal(secondAcceptance.run.parent_run_id, completedToolRun.id);
    const completedSecondRun = await waitForRun(
      ownerClient,
      secondAcceptance.run.id,
      (run) => TERMINAL_STATUSES.has(run.status),
      'Second Integration Session Run to complete'
    );
    assert.equal(completedSecondRun.status, 'completed', 'Second Integration Session Run must complete');
    assert.equal(completedSecondRun.session_id, completedToolRun.session_id);
    assert.equal(completedSecondRun.work_dir_ref, completedToolRun.work_dir_ref);
    const continuedHubSession = (await ownerClient.get(`/api/sessions/${primarySession.hub_session_id}`)).data;
    assert.equal(continuedHubSession.native_session_id, firstHubSession.native_session_id);

    const { data: integrationMessages } = await integrationClient.get(
      `/api/integrations/sessions/${primarySession.id}/messages`,
      bearerOptions(userAccessToken)
    );
    assert.deepEqual(
      integrationMessages.map((message) => message.sequence),
      integrationMessages.map((_, index) => index + 1)
    );
    assert.equal(integrationMessages.some((message) => message.content === messageContent), true);
    assert.equal(integrationMessages.some((message) => message.content === secondContent), true);
    assert.equal(
      integrationMessages.some((message) => message.message_kind === 'tool_result'),
      true
    );
    const { data: allEvents } = await integrationClient.get(
      `/api/integrations/sessions/${primarySession.id}/events?after=0`,
      bearerOptions(userAccessToken)
    );
    assert.ok(allEvents.length > 0, 'Integration event history must not be empty');
    for (let index = 1; index < allEvents.length; index += 1) {
      assert.ok(allEvents[index].seq > allEvents[index - 1].seq, 'Integration event seq must increase');
    }
    const lastSeq = allEvents.at(-1).seq;
    const { data: afterEvents } = await integrationClient.get(
      `/api/integrations/sessions/${primarySession.id}/events?after=${lastSeq}`,
      bearerOptions(userAccessToken)
    );
    assert.equal(afterEvents.every((event) => event.seq > lastSeq), true);

    const { data: baselineRuntimes } = await ownerClient.get('/api/runtimes');
    const runtimeTemplate = baselineRuntimes.find((runtime) => runtime.status === 'online');
    assert.ok(runtimeTemplate, 'Compose must provide an online Runtime template');
    const { data: enrollment } = await ownerClient.post('/api/admin/runtime-enrollment-tokens');
    assertOpaque(enrollment.token, 'ahre_', 'Runtime enrollment token');
    const idleHostname = uniqueSlug(context, 'qa-integration-idle-runtime');
    const { data: registeredIdle } = await runtimeClient.post('/api/runtime/register', {
      hostname: idleHostname,
      labels: ['qa', 'integration-concurrency'],
      engine_version: runtimeTemplate.engine_version,
      capabilities: structuredClone(runtimeTemplate.capabilities),
      sandbox_mode: runtimeTemplate.sandbox_mode
    }, { headers: { authorization: `Bearer ${enrollment.token}` } });
    idleRuntime = { id: registeredIdle.runtime_id, hostname: idleHostname, deleted: false };
    assertUuid(idleRuntime.id, 'Idle Runtime id');
    assertOpaque(registeredIdle.runtime_credential, 'ahrc_', 'Idle Runtime credential');
    secondaryOriginalRuntime = await setAgentRuntime(ownerClient, secondaryAgent.id, idleRuntime.id);
    secondaryRuntimeChanged = true;

    const { data: serializedSession } = await integrationClient.post('/api/integrations/sessions', {
      agent_id: secondaryAgent.id,
      external_user_id: context.unique('serialized-external-user'),
      tenant_id: context.unique('serialized-tenant'),
      email: `${uniqueSlug(context, 'serialized-external-user')}@example.com`,
      tools: [],
      metadata: { source: 'qa-concurrency' }
    }, bearerOptions(appAccessToken));
    concurrencySession = serializedSession;
    const concurrentKeys = [
      uniqueSlug(context, 'first-concurrent-message'),
      uniqueSlug(context, 'second-concurrent-message')
    ];
    const concurrentAcceptances = await Promise.all(concurrentKeys.map((key, index) => (
      integrationClient.post(
        `/api/integrations/sessions/${serializedSession.id}/messages`,
        {
          content: `Serialized Integration message ${index + 1}`,
          attachments: [],
          client_message_key: key
        },
        bearerOptions(appAccessToken)
      )
    )));
    assert.equal(new Set(concurrentAcceptances.map(({ data }) => data.run.id)).size, 1);
    assert.equal(new Set(concurrentAcceptances.map(({ data }) => data.message.id)).size, 2);
    assert.deepEqual(
      concurrentAcceptances.map(({ data }) => data.message.client_message_key).sort(),
      [...concurrentKeys].sort()
    );
    concurrencyRun = concurrentAcceptances[0].data.run;
    assert.equal(concurrencyRun.status, 'pending');
    secondaryOriginalRuntime = await setAgentRuntime(ownerClient, secondaryAgent.id, secondaryOriginalRuntime);
    secondaryRuntimeChanged = false;
    const completedConcurrencyRun = await waitForRun(
      ownerClient,
      concurrencyRun.id,
      (run) => TERMINAL_STATUSES.has(run.status),
      'Serialized Integration Run to complete after restoring its Runtime'
    );
    assert.equal(completedConcurrencyRun.status, 'completed');
    await ownerClient.post(`/api/admin/runtimes/${idleRuntime.id}/force-delete`, {
      hostname: idleRuntime.hostname
    });
    idleRuntime.deleted = true;

    const { data: widgetSession } = await ownerClient.post(
      `/api/integration-apps/${app.id}/agents/${primaryAgent.id}/widget-session`
    );
    const widgetToken = widgetSession.token;
    assertOpaque(widgetToken, 'ahe_', 'Delegated Agent Widget token');
    const pendingCode = await authorize(ownerClient, {
      ...codeParameters,
      state: context.unique('oauth-revocation-state')
    });
    liveSse = openIntegrationSse(context.baseURL, primarySession.id, userAccessToken);
    const readyFrame = await liveSse.ready;
    assert.equal(readyFrame.event, 'integration_event');

    const { data: revokedApp } = await ownerClient.request(`/api/integration-apps/${app.id}`, {
      method: 'PATCH',
      body: {
        name: editedName,
        redirect_uris: [secondRedirectUri, redirectUri],
        agent_ids: [secondaryAgent.id]
      }
    });
    assert.deepEqual(revokedApp.agent_ids, [secondaryAgent.id]);
    assert.equal(await liveSse.outcome, 'error', 'Live Integration SSE must close with an error after revocation');
    liveSse = null;

    await integrationClient.get(
      `/api/integrations/sessions/${primarySession.id}`,
      bearerOptions(userAccessToken, 403)
    );
    await oauthToken(context.baseURL, {
      grant_type: 'authorization_code',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      code: pendingCode,
      redirect_uri: redirectUri,
      scope: fullScope
    }, 403);
    await authorize(ownerClient, {
      ...codeParameters,
      state: context.unique('oauth-after-revocation')
    }, 403);
    await oauthToken(context.baseURL, {
      grant_type: 'client_credentials',
      client_id: app.client_id,
      client_secret: rotatedSecret,
      scope: `agent:${primaryAgent.id}`
    }, 403);
    await integrationClient.get('/api/widget/session', embedOptions(widgetToken, 401));

    const { data: unaffectedSession } = await integrationClient.get(
      `/api/integrations/sessions/${concurrencySession.id}`,
      bearerOptions(appAccessToken)
    );
    assert.equal(unaffectedSession.agent_id, secondaryAgent.id);
    const { data: finalApps } = await ownerClient.get('/api/integration-apps');
    assert.equal(finalApps.find((candidate) => candidate.id === app.id).agent_ids[0], secondaryAgent.id);
  } catch (error) {
    scenarioError = error;
  } finally {
    if (liveSse) {
      liveSse.abort();
      await liveSse.outcome.catch((error) => cleanupErrors.push(error));
    }
    if (concurrencyRun && concurrencySession && appAccessToken) {
      await cleanupResource(
        () => integrationClient.post(
          `/api/integrations/sessions/${concurrencySession.id}/runs/${concurrencyRun.id}/stop`,
          undefined,
          bearerOptions(appAccessToken, [200, 403, 404, 409])
        ),
        cleanupErrors
      );
    }
    if (secondaryRuntimeChanged && secondaryAgent && secondaryOriginalRuntime !== undefined) {
      await cleanupResource(
        () => setAgentRuntime(ownerClient, secondaryAgent.id, secondaryOriginalRuntime),
        cleanupErrors
      );
    }
    if (idleRuntime && !idleRuntime.deleted) {
      await cleanupResource(
        () => ownerClient.post(
          `/api/admin/runtimes/${idleRuntime.id}/force-delete`,
          { hostname: idleRuntime.hostname },
          { expectedStatus: [200, 404] }
        ),
        cleanupErrors
      );
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
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Integration scenario and cleanup failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, 'Integration scenario cleanup failed');
  }
}
