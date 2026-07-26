import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const PROVIDER_BASE_URL = 'http://fake-model-provider:8080';
const PROVIDER_API_KEY = 'dev-model-provider-api-key';
const WRONG_PROVIDER_API_KEY = 'rotated-provider-key-for-negative-control';
const SUCCESS_MODEL_ID = 'hub-proxy-smoke';
const ERROR_MODEL_ID = 'hub-proxy-error';
const TEST_MESSAGE = 'hi';

const AUTOMATIC_MODEL_SETTINGS = {
  reasoning_effort: 'default',
  reasoning_summary: 'default',
  verbosity: 'default',
  context_window_tokens: null,
  auto_compact_token_limit: null,
  reasoning_summary_support: 'auto',
  service_tier: null,
  request_max_retries: null,
  stream_max_retries: null,
  stream_idle_timeout_ms: null,
  request_settings: { protocol: 'openai_responses' }
};

function modelSettings(apiType, overrides = {}) {
  const requestSettings = apiType === 'openai_chat_completions'
    ? { protocol: apiType, temperature: null, top_p: null, max_completion_tokens: null }
    : apiType === 'anthropic_messages'
      ? { protocol: apiType, temperature: null, top_p: null, max_tokens: null }
      : { protocol: apiType };
  return {
    ...AUTOMATIC_MODEL_SETTINGS,
    ...overrides,
    request_settings: overrides.request_settings ?? requestSettings
  };
}

function connectionRequest(scope, name, apiType, allowedModelIds, apiKey = PROVIDER_API_KEY) {
  return {
    scope,
    name,
    base_url: PROVIDER_BASE_URL,
    api_type: apiType,
    allowed_model_ids: allowedModelIds,
    api_key: apiKey
  };
}

function selection(connection, modelId = SUCCESS_MODEL_ID) {
  return { connection_id: connection.id, model_id: modelId };
}

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function sqlLiteral(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function ledgerPath(path, query = {}) {
  const parameters = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null) parameters.set(key, String(value));
  }
  const encoded = parameters.toString();
  return encoded ? `${path}?${encoded}` : path;
}

function assertSecretAbsent(value, label) {
  const serialized = JSON.stringify(value);
  for (const secret of [PROVIDER_API_KEY, WRONG_PROVIDER_API_KEY]) {
    assert.equal(serialized.includes(secret), false, `${label} must not expose provider credentials`);
  }
  assert.equal(serialized.includes('"api_key"'), false, `${label} must not expose an api_key field`);
}

async function manualRedirect(client, path) {
  const headers = { accept: 'application/json' };
  const cookie = client.cookieHeader();
  if (cookie) headers.cookie = cookie;
  const response = await fetch(new URL(path, client.baseURL), { headers, redirect: 'manual' });
  client.absorbCookies(response.headers);
  assert.equal(response.status, 303, 'Mock OIDC step must redirect');
  const location = response.headers.get('location');
  assert.equal(typeof location, 'string', 'Mock OIDC redirect must include a location');
  return location;
}

async function oidcLogin(context, prefix) {
  const subject = uniqueSlug(context, prefix);
  const client = new ApiClient(context.baseURL);
  const callback = await manualRedirect(
    client,
    `/api/auth/oidc/mock/start?email=${encodeURIComponent(`${subject}@example.com`)}&sub=${encodeURIComponent(subject)}`
  );
  await manualRedirect(client, callback);
  const { data: user } = await client.get('/api/auth/me');
  return { client, user };
}

async function waitForLedgerItems(client, path, filters, count) {
  return poll(async () => {
    const { data } = await client.get(ledgerPath(path, { ...filters, page_size: 100 }));
    return data.items;
  }, (items) => items.length === count, {
    timeoutMs: 20_000,
    description: `${path} to contain ${count} isolated rows`
  });
}

async function createRun(client, agentId, message, expectedStatus = 'completed') {
  const { data: run } = await client.post(`/api/agents/${agentId}/runs`, {
    message,
    hub_session_id: null,
    parent_run_id: null
  });
  assert.match(run.id, UUID_PATTERN);
  await waitForRunStatus(client, agentId, run.id, expectedStatus, 90_000);
  return run;
}

function usageTotals(items) {
  return items.reduce((totals, item) => ({
    input_tokens: totals.input_tokens + item.input_tokens,
    output_tokens: totals.output_tokens + item.output_tokens,
    total_tokens: totals.total_tokens + item.total_tokens,
    cached_tokens: totals.cached_tokens + item.cached_tokens,
    reasoning_tokens: totals.reasoning_tokens + item.reasoning_tokens
  }), {
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    cached_tokens: 0,
    reasoning_tokens: 0
  });
}

export default async function modelConnectionsApiScenario(context) {
  const superClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(superClient);
  const { data: originalDefault } = await superClient.get('/api/model-connections/system-default');
  const createdConnections = [];
  const createdAgents = [];
  let promotedAdminId = null;
  let scenarioError = null;

  const createConnection = async (client, request) => {
    const { data: connection } = await client.post('/api/model-connections', request);
    createdConnections.push({ client, id: connection.id });
    assert.match(connection.id, UUID_PATTERN);
    assertSecretAbsent(connection, 'Model Connection create response');
    return connection;
  };
  const createAgent = async (client, request) => {
    const { data: agent } = await client.post('/api/agents', request);
    createdAgents.push({ client, id: agent.id });
    assert.match(agent.id, UUID_PATTERN);
    return agent;
  };

  try {
    const administrator = await oidcLogin(context, 'qa-model-administrator');
    const owner = await oidcLogin(context, 'qa-model-owner');
    const outsider = await oidcLogin(context, 'qa-model-outsider');
    promotedAdminId = administrator.user.id;
    const { data: promoted } = await superClient.request(
      `/api/admin/users/${administrator.user.id}/role`,
      { method: 'PUT', body: { role: 'admin' } }
    );
    assert.equal(promoted.user.role, 'admin');

    const { data: openapi } = await superClient.get('/openapi.json');
    const createSchema = openapi.components.schemas.CreateModelConnectionRequest;
    const connectionSchema = openapi.components.schemas.ModelConnection;
    assert.deepEqual(createSchema.required, [
      'scope', 'name', 'base_url', 'api_type', 'allowed_model_ids', 'api_key'
    ]);
    assert.equal(createSchema.properties.api_key.writeOnly, true);
    for (const legacyField of ['model_id', 'upstream_protocol', 'parameters', 'request_parameters']) {
      assert.equal(Object.hasOwn(createSchema.properties, legacyField), false);
      assert.equal(Object.hasOwn(connectionSchema.properties, legacyField), false);
    }
    assert.equal(Object.hasOwn(connectionSchema.properties, 'api_key'), false);
    assert.ok(openapi.components.schemas.SystemDefaultModelSelection);
    assert.equal(Object.hasOwn(openapi.components.schemas, 'SystemDefaultModelConnection'), false);
    assert.deepEqual(openapi.components.schemas.ModelUpstreamProtocol.enum, [
      'openai_responses', 'openai_chat_completions', 'anthropic_messages'
    ]);

    await owner.client.post('/api/model-connections', connectionRequest(
      'global',
      context.unique('QA forbidden Global'),
      'openai_responses',
      [SUCCESS_MODEL_ID]
    ), { expectedStatus: 403 });
    await owner.client.post('/api/model-connections', {
      ...connectionRequest(
        'personal',
        context.unique('QA rejected legacy Connection'),
        'openai_responses',
        [SUCCESS_MODEL_ID]
      ),
      model_id: SUCCESS_MODEL_ID
    }, { expectedStatus: 422 });

    const responseConnection = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA multi-model Responses'),
      'openai_responses',
      [` ${SUCCESS_MODEL_ID} `, ERROR_MODEL_ID, SUCCESS_MODEL_ID]
    ));
    assert.deepEqual(responseConnection.allowed_model_ids, [SUCCESS_MODEL_ID, ERROR_MODEL_ID]);
    const chatConnection = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Chat'),
      'openai_chat_completions',
      [SUCCESS_MODEL_ID]
    ));
    const anthropicConnection = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Anthropic'),
      'anthropic_messages',
      [SUCCESS_MODEL_ID]
    ));
    const ownerPersonal = await createConnection(owner.client, connectionRequest(
      'personal',
      context.unique('QA owner Personal'),
      'openai_responses',
      [SUCCESS_MODEL_ID, 'owner-secondary']
    ));
    const outsiderPersonal = await createConnection(outsider.client, connectionRequest(
      'personal',
      context.unique('QA outsider Personal'),
      'openai_responses',
      [SUCCESS_MODEL_ID]
    ));
    const superPersonal = await createConnection(superClient, connectionRequest(
      'personal',
      context.unique('QA super Personal'),
      'openai_responses',
      [SUCCESS_MODEL_ID]
    ));
    assert.equal(superPersonal.owner_id, superAdmin.id);

    const { data: ownerConnections } = await owner.client.get('/api/model-connections');
    assert.equal(ownerConnections.some((item) => item.id === ownerPersonal.id), true);
    assert.equal(ownerConnections.some((item) => item.id === outsiderPersonal.id), false);
    assert.equal(ownerConnections.some((item) => item.id === responseConnection.id), true);
    assertSecretAbsent(ownerConnections, 'Model Connection list');
    await administrator.client.get(`/api/model-connections/${superPersonal.id}`, { expectedStatus: 404 });
    await owner.client.get(`/api/model-connections/${outsiderPersonal.id}`, { expectedStatus: 404 });

    const { data: options } = await owner.client.get('/api/model-connections/options');
    assert.deepEqual(
      options.items
        .filter((item) => item.connection_id === responseConnection.id)
        .map((item) => item.model_id),
      [SUCCESS_MODEL_ID, ERROR_MODEL_ID]
    );
    assert.equal(options.items.some((item) => item.connection_id === ownerPersonal.id), true);
    assert.equal(options.items.some((item) => item.connection_id === outsiderPersonal.id), false);
    assertSecretAbsent(options, 'Model Connection options');

    assert.equal(
      context.compose.psql(`
        SELECT (a.api_key_ciphertext IS NOT NULL)::text || '|' ||
               (a.api_key_nonce IS NOT NULL)::text || '|' ||
               (a.api_key_ciphertext <> b.api_key_ciphertext)::text || '|' ||
               (a.api_key_nonce <> b.api_key_nonce)::text
        FROM model_connections a, model_connections b
        WHERE a.id = ${sqlLiteral(ownerPersonal.id)}
          AND b.id = ${sqlLiteral(outsiderPersonal.id)}
      `),
      'true|true|true|true',
      'Equal plaintext keys must use independently randomized encrypted records'
    );

    const successfulTest = (await administrator.client.post(
      `/api/model-connections/${responseConnection.id}/test`,
      { model_id: SUCCESS_MODEL_ID, message: TEST_MESSAGE }
    )).data;
    assert.equal(successfulTest.success, true);
    assert.equal(successfulTest.status_code, 200);
    assert.equal(successfulTest.error_code, null);
    assert.equal(successfulTest.message, null);
    assert.equal(successfulTest.response_text, 'Fake model completed run through the Hub model proxy.');
    assert.equal(Number.isInteger(successfulTest.response_time_ms), true);
    assert.ok(successfulTest.response_time_ms >= 0);
    const failedModelTest = (await administrator.client.post(
      `/api/model-connections/${responseConnection.id}/test`,
      { model_id: ERROR_MODEL_ID, message: TEST_MESSAGE }
    )).data;
    assert.equal(failedModelTest.success, false);
    assert.equal(failedModelTest.status_code, 200);
    assert.equal(failedModelTest.error_code, 'fake_model_error');
    await administrator.client.post(
      `/api/model-connections/${responseConnection.id}/test`,
      { model_id: 'not-allowed', message: TEST_MESSAGE },
      { expectedStatus: 400 }
    );

    const secretBeforeRotation = context.compose.psql(`
      SELECT md5(api_key_ciphertext) || '|' || encode(api_key_nonce, 'hex')
      FROM model_connections WHERE id = ${sqlLiteral(responseConnection.id)}
    `);
    const { data: rotatedWrong } = await administrator.client.request(
      `/api/model-connections/${responseConnection.id}`,
      {
        method: 'PUT',
        body: {
          name: responseConnection.name,
          base_url: responseConnection.base_url,
          api_type: responseConnection.api_type,
          allowed_model_ids: responseConnection.allowed_model_ids,
          api_key: WRONG_PROVIDER_API_KEY
        }
      }
    );
    assertSecretAbsent(rotatedWrong, 'Rotated Model Connection response');
    assert.notEqual(
      context.compose.psql(`
        SELECT md5(api_key_ciphertext) || '|' || encode(api_key_nonce, 'hex')
        FROM model_connections WHERE id = ${sqlLiteral(responseConnection.id)}
      `),
      secretBeforeRotation,
      'Rotating a provider key must replace its encrypted record'
    );
    const wrongKeyTest = (await administrator.client.post(
      `/api/model-connections/${responseConnection.id}/test`,
      { model_id: SUCCESS_MODEL_ID, message: TEST_MESSAGE }
    )).data;
    assert.equal(wrongKeyTest.success, false);
    assert.equal(wrongKeyTest.status_code, 401, 'The live rotated key must be used immediately');
    await administrator.client.request(`/api/model-connections/${responseConnection.id}`, {
      method: 'PUT',
      body: {
        name: responseConnection.name,
        base_url: responseConnection.base_url,
        api_type: responseConnection.api_type,
        allowed_model_ids: responseConnection.allowed_model_ids,
        api_key: PROVIDER_API_KEY
      }
    });
    assert.equal((await administrator.client.post(
      `/api/model-connections/${responseConnection.id}/test`,
      { model_id: SUCCESS_MODEL_ID, message: TEST_MESSAGE }
    )).data.success, true);

    assert.equal((await administrator.client.post(
      `/api/model-connections/${chatConnection.id}/test`,
      { model_id: SUCCESS_MODEL_ID, message: TEST_MESSAGE }
    )).data.success, true);
    assert.equal((await administrator.client.post(
      `/api/model-connections/${anthropicConnection.id}/test`,
      { model_id: SUCCESS_MODEL_ID, message: TEST_MESSAGE }
    )).data.success, true);

    await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { selection: selection(responseConnection) }
    });
    const copiedDefaultAgent = await createAgent(owner.client, {
      name: context.unique('QA copied System Default'),
      instructions: 'Use the copied System Default model selection.',
      visibility: 'private',
      public_to: [],
      model_selection: null,
      model_settings: modelSettings('openai_responses'),
      subagents: []
    });
    assert.deepEqual(copiedDefaultAgent.model_selection, selection(responseConnection));

    const responseSettings = modelSettings('openai_responses', {
      reasoning_effort: 'medium',
      reasoning_summary: 'concise',
      verbosity: 'high',
      context_window_tokens: 128_000,
      auto_compact_token_limit: 96_000,
      reasoning_summary_support: 'supported',
      service_tier: 'flex',
      request_max_retries: 1,
      stream_max_retries: 3,
      stream_idle_timeout_ms: 300_000
    });
    const responseAgent = await createAgent(owner.client, {
      name: context.unique('QA Responses Agent'),
      instructions: 'Exercise immutable main and subagent model bindings.',
      visibility: 'private',
      public_to: [],
      model_selection: selection(responseConnection),
      model_settings: responseSettings,
      subagents: [{
        name: 'inherit_agent',
        description: 'Inherits the Agent model and settings.',
        developer_instructions: 'Use the Agent model configuration.',
        model_selection: null,
        model_settings_override: {}
      }, {
        name: 'reviewer',
        description: 'Uses the same model with stronger reasoning.',
        developer_instructions: 'Review the response carefully.',
        model_selection: selection(responseConnection),
        model_settings_override: {
          reasoning_effort: 'high',
          request_max_retries: 2
        }
      }]
    });
    assert.deepEqual(responseAgent.model_selection, selection(responseConnection));
    assert.deepEqual(responseAgent.model_settings, responseSettings);
    assert.deepEqual(responseAgent.subagents[0].model_selection, null);
    assert.deepEqual(responseAgent.subagents[0].model_settings_override, {});
    assert.deepEqual(responseAgent.subagents[1].model_settings_override, {
      reasoning_effort: 'high',
      request_max_retries: 2
    });

    await owner.client.post('/api/agents', {
      name: context.unique('QA rejected foreign Personal model'),
      instructions: 'This model selection belongs to another user.',
      visibility: 'private',
      public_to: [],
      model_selection: selection(outsiderPersonal),
      model_settings: modelSettings('openai_responses'),
      subagents: []
    }, { expectedStatus: 400 });

    const responseRun = await createRun(
      owner.client,
      responseAgent.id,
      'Verify the Responses binding and usage snapshots.'
    );
    const bindingRows = context.compose.psql(`
      SELECT binding_key || '|' || model_connection_id::text || '|' || model_id || '|' ||
             (model_settings->>'reasoning_effort') || '|' ||
             COALESCE(model_settings->>'request_max_retries', '<null>')
      FROM run_model_bindings
      WHERE run_id = ${sqlLiteral(responseRun.id)}
      ORDER BY binding_key
    `).split('\n');
    assert.deepEqual(bindingRows, [
      `main|${responseConnection.id}|${SUCCESS_MODEL_ID}|medium|1`,
      `reviewer|${responseConnection.id}|${SUCCESS_MODEL_ID}|high|2`
    ]);

    const chatSettings = modelSettings('openai_chat_completions', {
      reasoning_effort: 'max',
      request_settings: {
        protocol: 'openai_chat_completions',
        temperature: 0.3,
        top_p: 0.8,
        max_completion_tokens: 321
      }
    });
    const chatAgent = await createAgent(owner.client, {
      name: context.unique('QA Chat Agent'),
      instructions: 'Exercise Responses to Chat conversion.',
      visibility: 'private',
      public_to: [],
      model_selection: selection(chatConnection),
      model_settings: chatSettings,
      subagents: []
    });
    await createRun(owner.client, chatAgent.id, 'Verify the Chat conversion path.');

    const anthropicSettings = modelSettings('anthropic_messages', {
      reasoning_effort: 'high',
      request_settings: {
        protocol: 'anthropic_messages',
        temperature: 0.4,
        top_p: null,
        max_tokens: 8_192
      }
    });
    const anthropicAgent = await createAgent(owner.client, {
      name: context.unique('QA Anthropic Agent'),
      instructions: 'Exercise Responses to Anthropic conversion.',
      visibility: 'private',
      public_to: [],
      model_selection: selection(anthropicConnection),
      model_settings: anthropicSettings,
      subagents: []
    });
    await createRun(owner.client, anthropicAgent.id, 'Verify the Anthropic conversion path.');

    const responseAgentUsage = await waitForLedgerItems(owner.client, '/api/model-usage', {
      model_connection_id: responseConnection.id,
      agent_id: responseAgent.id,
      user_id: owner.user.id
    }, 1);
    assert.deepEqual(responseAgentUsage[0].model.request_settings, { protocol: 'openai_responses' });
    assert.equal(responseAgentUsage[0].model.api_type, 'openai_responses');
    const chatUsage = await waitForLedgerItems(owner.client, '/api/model-usage', {
      model_connection_id: chatConnection.id,
      agent_id: chatAgent.id,
      user_id: owner.user.id
    }, 1);
    assert.deepEqual(chatUsage[0].model.request_settings, chatSettings.request_settings);
    assert.equal(chatUsage[0].model.api_type, 'openai_chat_completions');
    const anthropicUsage = await waitForLedgerItems(owner.client, '/api/model-usage', {
      model_connection_id: anthropicConnection.id,
      agent_id: anthropicAgent.id,
      user_id: owner.user.id
    }, 1);
    assert.deepEqual(anthropicUsage[0].model.request_settings, anthropicSettings.request_settings);
    assert.equal(anthropicUsage[0].model.api_type, 'anthropic_messages');

    const connectionUsageBeforeDelete = await waitForLedgerItems(
      administrator.client,
      '/api/model-usage',
      { model_connection_id: responseConnection.id },
      4
    );
    const connectionErrorsBeforeDelete = await waitForLedgerItems(
      administrator.client,
      '/api/model-call-errors',
      { model_connection_id: responseConnection.id },
      2
    );
    assert.equal(connectionUsageBeforeDelete.some((item) => item.model.model_id === ERROR_MODEL_ID), true);
    assert.equal(connectionErrorsBeforeDelete.some((item) => item.error_code === 'fake_model_error'), true);
    assert.equal(JSON.stringify(connectionErrorsBeforeDelete).includes(PROVIDER_API_KEY), false);

    const updateBody = {
      name: responseConnection.name,
      base_url: responseConnection.base_url,
      api_type: responseConnection.api_type,
      allowed_model_ids: [ERROR_MODEL_ID]
    };
    await administrator.client.request(`/api/model-connections/${responseConnection.id}`, {
      method: 'PUT',
      body: updateBody,
      expectedStatus: 409
    });
    assert.deepEqual(
      (await owner.client.get(`/api/agents/${responseAgent.id}`)).data.model_selection,
      selection(responseConnection),
      'A rejected allowlist update must not mutate Agent selection'
    );
    const { data: forceUpdated } = await administrator.client.request(
      `/api/model-connections/${responseConnection.id}?force=true`,
      { method: 'PUT', body: updateBody }
    );
    assert.deepEqual(forceUpdated.allowed_model_ids, [ERROR_MODEL_ID]);
    const { data: unconfiguredAgent } = await owner.client.get(`/api/agents/${responseAgent.id}`);
    assert.equal(unconfiguredAgent.model_selection, null);
    const disabledReviewer = unconfiguredAgent.subagents.find((item) => item.name === 'reviewer');
    assert.equal(disabledReviewer.enabled, false);
    assert.equal(disabledReviewer.disabled_reason, 'model_selection_removed');
    assert.equal(disabledReviewer.model_selection, null);
    assert.equal(
      (await superClient.get('/api/model-connections/system-default')).data.selection,
      null
    );
    await owner.client.post(`/api/agents/${responseAgent.id}/runs`, {
      message: 'A model-unconfigured Agent must not start a new Run.',
      hub_session_id: null,
      parent_run_id: null
    }, { expectedStatus: 409 });

    await administrator.client.delete(`/api/model-connections/${responseConnection.id}`, {
      expectedStatus: 204
    });
    await administrator.client.get(`/api/model-connections/${responseConnection.id}`, {
      expectedStatus: 404
    });
    assert.equal(
      context.compose.psql(`
        SELECT (base_url IS NULL)::text || '|' ||
               (api_key_ciphertext IS NULL)::text || '|' ||
               (api_key_nonce IS NULL)::text || '|' ||
               (deleted_at IS NOT NULL)::text
        FROM model_connections WHERE id = ${sqlLiteral(responseConnection.id)}
      `),
      'true|true|true|true',
      'Deleting a Connection must scrub its live endpoint and credential'
    );

    const { data: retainedUsagePage } = await administrator.client.get(ledgerPath(
      '/api/model-usage',
      { model_connection_id: responseConnection.id, page_size: 100 }
    ));
    const { data: retainedErrorPage } = await administrator.client.get(ledgerPath(
      '/api/model-call-errors',
      { model_connection_id: responseConnection.id, page_size: 100 }
    ));
    assert.equal(retainedUsagePage.items.length, connectionUsageBeforeDelete.length);
    assert.equal(retainedErrorPage.items.length, connectionErrorsBeforeDelete.length);
    for (const item of [...retainedUsagePage.items, ...retainedErrorPage.items]) {
      assert.equal(item.model.id, responseConnection.id);
      assert.equal(item.model.name, responseConnection.name);
      assert.equal(item.model.api_type, 'openai_responses');
      assert.deepEqual(item.model.request_settings, { protocol: 'openai_responses' });
    }
    const { data: retainedSummary } = await administrator.client.get(ledgerPath(
      '/api/model-usage/summary',
      { model_connection_id: responseConnection.id }
    ));
    assert.deepEqual(retainedSummary.overall, usageTotals(retainedUsagePage.items));
    assert.equal(
      retainedSummary.by_model.some((row) => (
        row.model.name === responseConnection.name
        && row.model.api_type === 'openai_responses'
        && row.model.model_id === SUCCESS_MODEL_ID
      )),
      true
    );
    assertSecretAbsent(retainedSummary, 'Retained model usage summary');
  } catch (error) {
    scenarioError = error;
  }

  const cleanupErrors = [];
  const cleanup = async (label, action) => {
    try {
      await action();
    } catch (error) {
      cleanupErrors.push(`${label}: ${error.message}`);
    }
  };
  await cleanup('restore System Default', async () => {
    const { data } = await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { selection: originalDefault.selection }
    });
    assert.deepEqual(data, originalDefault);
  });
  for (const agent of [...createdAgents].reverse()) {
    await cleanup(`delete Agent ${agent.id}`, () => agent.client.delete(
      `/api/agents/${agent.id}`,
      { expectedStatus: [204, 404] }
    ));
  }
  for (const connection of [...createdConnections].reverse()) {
    await cleanup(`force-delete Model Connection ${connection.id}`, () => connection.client.request(
      `/api/model-connections/${connection.id}/force-delete`,
      { method: 'POST', expectedStatus: [204, 404] }
    ));
  }
  if (promotedAdminId) {
    await cleanup('restore Administrator role', () => superClient.request(
      `/api/admin/users/${promotedAdminId}/role`,
      { method: 'PUT', body: { role: 'member' } }
    ));
  }

  if (scenarioError) {
    if (cleanupErrors.length > 0) {
      scenarioError.message += `\nCleanup failures:\n${cleanupErrors.join('\n')}`;
    }
    throw scenarioError;
  }
  if (cleanupErrors.length > 0) {
    throw new Error(`Model scenario cleanup failed:\n${cleanupErrors.join('\n')}`);
  }
}
