import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const PROVIDER_BASE_URL = 'http://fake-model-provider:8080';
const PROVIDER_API_KEY = 'dev-model-provider-api-key';
const SUCCESS_MODEL_ID = 'hub-proxy-smoke';
const ERROR_MODEL_ID = 'hub-proxy-error';

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
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null) params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded ? `${path}?${encoded}` : path;
}

function assertWriteOnly(value, label) {
  const serialized = JSON.stringify(value);
  assert.equal(serialized.includes(PROVIDER_API_KEY), false, `${label} must not expose the provider key`);
  assert.equal(serialized.includes('"api_key"'), false, `${label} must not expose an api_key field`);
}

function connectionRequest(
  scope,
  name,
  modelId = SUCCESS_MODEL_ID,
  upstreamProtocol = 'openai_responses'
) {
  return {
    scope,
    name,
    base_url: PROVIDER_BASE_URL,
    model_id: modelId,
    upstream_protocol: upstreamProtocol,
    api_key: PROVIDER_API_KEY
  };
}

async function manualRedirect(client, path) {
  const headers = { accept: 'application/json' };
  const cookie = client.cookieHeader();
  if (cookie) headers.cookie = cookie;
  const response = await fetch(new URL(path, client.baseURL), {
    headers,
    redirect: 'manual'
  });
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

async function readTwoKeysetPages(client, path, filters) {
  const first = (await client.get(ledgerPath(path, { ...filters, page_size: 1 }))).data;
  assert.equal(first.items.length, 1, `${path} first page must contain one item`);
  assert.ok(first.next_cursor, `${path} first page must expose a next cursor`);
  assert.equal(first.next_cursor.occurred_at_ms, Date.parse(first.items[0].occurred_at));
  assert.match(first.next_cursor.id, UUID_PATTERN);

  const second = (await client.get(ledgerPath(path, {
    ...filters,
    page_size: 1,
    cursor_occurred_at_ms: first.next_cursor.occurred_at_ms,
    cursor_id: first.next_cursor.id
  }))).data;
  assert.equal(second.items.length, 1, `${path} second page must contain one item`);
  assert.notEqual(second.items[0].id, first.items[0].id, `${path} pages must not overlap`);
  assert.equal(second.next_cursor, null, `${path} second page must be terminal for this fixture`);
  return [...first.items, ...second.items];
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

async function waitForLedgerItems(client, path, filters, count) {
  return poll(async () => {
    const { data } = await client.get(ledgerPath(path, { ...filters, page_size: 100 }));
    return data.items;
  }, (items) => items.length === count, {
    timeoutMs: 15_000,
    description: `${path} to contain ${count} isolated rows`
  });
}

export default async function modelConnectionsApiScenario(context) {
  const superClient = new ApiClient(context.baseURL);
  const { data: superAdmin } = await loginAsAdmin(superClient);
  const { data: originalDefault } = await superClient.get('/api/model-connections/system-default');
  let defaultSnapshotTaken = true;
  let promotedAdminId = null;
  const createdAgents = [];
  const createdConnections = [];
  let scenarioFailure = null;

  const trackAgent = (client, agent) => {
    createdAgents.push({ client, id: agent.id });
    return agent;
  };
  const createConnection = async (client, request) => {
    const { data } = await client.post('/api/model-connections', request);
    createdConnections.push({ client, id: data.id });
    assert.match(data.id, UUID_PATTERN);
    assertWriteOnly(data, 'Model Connection create response');
    return data;
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
    assert.equal(openapi.components.schemas.CreateModelConnectionRequest.properties.api_key.writeOnly, true);
    assert.equal(Object.hasOwn(openapi.components.schemas.ModelConnection.properties, 'api_key'), false);
    assert.deepEqual(Object.keys(openapi.paths['/api/model-usage']), ['get']);
    assert.deepEqual(Object.keys(openapi.paths['/api/model-call-errors']), ['get']);

    await owner.client.post('/api/model-connections', connectionRequest(
      'global',
      context.unique('QA forbidden member Global')
    ), { expectedStatus: 403 });

    const superPersonal = await createConnection(superClient, connectionRequest(
      'personal',
      context.unique('QA super Personal')
    ));
    const ownerPersonal = await createConnection(owner.client, connectionRequest(
      'personal',
      context.unique('QA owner Personal')
    ));
    const outsiderPersonal = await createConnection(outsider.client, connectionRequest(
      'personal',
      context.unique('QA outsider Personal')
    ));
    const globalA = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Global A')
    ));
    const globalB = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Global B')
    ));
    const errorGlobal = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Global Error'),
      ERROR_MODEL_ID
    ));
    const anthropicGlobal = await createConnection(administrator.client, connectionRequest(
      'global',
      context.unique('QA Global Anthropic'),
      SUCCESS_MODEL_ID,
      'anthropic_messages'
    ));

    assert.equal(superPersonal.owner_id, superAdmin.id);
    assert.equal(ownerPersonal.owner_id, owner.user.id);
    assert.equal(globalA.owner_id, null);
    assert.equal(globalA.scope, 'global');
    await administrator.client.get(`/api/model-connections/${superPersonal.id}`, { expectedStatus: 404 });
    await administrator.client.request(`/api/model-connections/${superPersonal.id}`, {
      method: 'PATCH',
      body: {
        name: superPersonal.name,
        base_url: superPersonal.base_url,
        model_id: superPersonal.model_id
      },
      expectedStatus: 404
    });
    await administrator.client.get(`/api/model-connections/${ownerPersonal.id}`, { expectedStatus: 404 });
    await outsider.client.get(`/api/model-connections/${ownerPersonal.id}`, { expectedStatus: 404 });
    await superClient.get(`/api/model-connections/${ownerPersonal.id}`, { expectedStatus: 404 });
    await owner.client.request(`/api/model-connections/${globalA.id}`, {
      method: 'PATCH',
      body: { name: globalA.name, base_url: globalA.base_url, model_id: globalA.model_id },
      expectedStatus: 403
    });

    const { data: ownerConnections } = await owner.client.get('/api/model-connections');
    assert.equal(ownerConnections.some((connection) => connection.id === ownerPersonal.id), true);
    assert.equal(ownerConnections.some((connection) => connection.id === outsiderPersonal.id), false);
    assert.equal(ownerConnections.some((connection) => connection.id === superPersonal.id), false);
    assert.equal(ownerConnections.some((connection) => connection.id === globalA.id), true);
    assertWriteOnly(ownerConnections, 'Model Connection list response');
    const { data: ownerOptions } = await owner.client.get('/api/model-connections/options');
    assert.equal(ownerOptions.items.some((connection) => connection.id === ownerPersonal.id), true);
    assert.equal(ownerOptions.items.some((connection) => connection.id === outsiderPersonal.id), false);
    assertWriteOnly(ownerOptions, 'Model Connection options response');

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
    const secretFingerprint = context.compose.psql(`
      SELECT md5(api_key_ciphertext) || '|' || encode(api_key_nonce, 'hex')
      FROM model_connections WHERE id = ${sqlLiteral(ownerPersonal.id)}
    `);
    const updatedPersonalName = context.unique('QA owner Personal Updated');
    const { data: updatedPersonal } = await owner.client.request(
      `/api/model-connections/${ownerPersonal.id}`,
      {
        method: 'PATCH',
        body: {
          name: updatedPersonalName,
          base_url: PROVIDER_BASE_URL,
          model_id: SUCCESS_MODEL_ID
        }
      }
    );
    assert.equal(updatedPersonal.name, updatedPersonalName);
    assertWriteOnly(updatedPersonal, 'Model Connection update response');
    assert.equal(
      context.compose.psql(`
        SELECT md5(api_key_ciphertext) || '|' || encode(api_key_nonce, 'hex')
        FROM model_connections WHERE id = ${sqlLiteral(ownerPersonal.id)}
      `),
      secretFingerprint,
      'Omitting api_key during update must preserve the encrypted secret'
    );

    const ownerTest = (await owner.client.post(`/api/model-connections/${ownerPersonal.id}/test`)).data;
    assert.deepEqual(ownerTest, { success: true, status_code: 200, error_code: null, message: null });
    const superTest = (await superClient.post(`/api/model-connections/${superPersonal.id}/test`)).data;
    assert.equal(superTest.success, true);
    const ownerTestUsage = await waitForLedgerItems(owner.client, '/api/model-usage', {
      model_connection_id: ownerPersonal.id
    }, 1);
    assert.equal(ownerTestUsage[0].subject.id, owner.user.id);
    assert.equal(ownerTestUsage[0].agent.id, null);
    assert.deepEqual(usageTotals(ownerTestUsage), {
      input_tokens: 11,
      output_tokens: 7,
      total_tokens: 18,
      cached_tokens: 3,
      reasoning_tokens: 5
    });
    const anthropicTest = (await administrator.client.post(
      `/api/model-connections/${anthropicGlobal.id}/test`
    )).data;
    assert.deepEqual(anthropicTest, {
      success: true,
      status_code: 200,
      error_code: null,
      message: null
    });
    const anthropicUsage = await waitForLedgerItems(
      administrator.client,
      '/api/model-usage',
      { model_connection_id: anthropicGlobal.id },
      1
    );
    assert.equal(anthropicUsage[0].model.upstream_protocol, 'anthropic_messages');
    assert.deepEqual(usageTotals(anthropicUsage), {
      input_tokens: 13,
      output_tokens: 8,
      total_tokens: 21,
      cached_tokens: 0,
      reasoning_tokens: 0
    });
    const hiddenSuperUsage = (await administrator.client.get(ledgerPath('/api/model-usage', {
      model_connection_id: superPersonal.id,
      page_size: 100
    }))).data;
    assert.deepEqual(hiddenSuperUsage.items, [], 'Administrator must not see super-admin Personal usage');

    await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { model_connection_id: globalA.id }
    });
    assert.equal((await superClient.get(`/api/model-connections/${globalA.id}`)).data.is_system_default, true);

    const agentA = trackAgent(owner.client, (await owner.client.post('/api/agents', {
      name: context.unique('QA copied Global A Agent'),
      instructions: 'Exercise Agent default and subagent model overrides.',
      visibility: 'private',
      public_to: [],
      reasoning_effort: 'high',
      codex_subagents: [{
        name: 'inherit_default',
        description: 'Inherits the Agent default connection.',
        developer_instructions: 'Use the Agent default model configuration.'
      }, {
        name: 'personal_override',
        description: 'Uses the Agent owner Personal connection.',
        developer_instructions: 'Use the explicit Personal model override.',
        model_connection_id: ownerPersonal.id,
        reasoning_effort: 'max'
      }, {
        name: 'global_override',
        description: 'Uses the explicit Global connection.',
        developer_instructions: 'Use the explicit Global model override.',
        model_connection_id: globalA.id,
        reasoning_effort: 'low'
      }]
    })).data);
    assert.equal(agentA.default_model_connection_id, globalA.id);
    assert.equal(agentA.codex_subagents.find((item) => item.name === 'inherit_default')?.model_connection_id, null);
    assert.equal(agentA.codex_subagents.find((item) => item.name === 'personal_override')?.model_connection_id, ownerPersonal.id);
    assert.equal(agentA.codex_subagents.find((item) => item.name === 'global_override')?.model_connection_id, globalA.id);
    const { data: agentOptions } = await owner.client.get(`/api/agents/${agentA.id}/model-options`);
    assert.equal(agentOptions.items.some((item) => item.id === ownerPersonal.id), true);
    assert.equal(agentOptions.items.some((item) => item.id === outsiderPersonal.id), false);
    await owner.client.post('/api/agents', {
      name: context.unique('QA rejected foreign Personal Agent'),
      instructions: 'This payload must be rejected.',
      visibility: 'private',
      public_to: [],
      default_model_connection_id: globalA.id,
      codex_subagents: [{
        name: 'foreign_override',
        description: 'Attempts a foreign Personal connection.',
        developer_instructions: 'This configuration must not be accepted.',
        model_connection_id: outsiderPersonal.id
      }]
    }, { expectedStatus: 400 });

    await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { model_connection_id: globalB.id }
    });
    assert.equal((await owner.client.get(`/api/agents/${agentA.id}`)).data.default_model_connection_id, globalA.id);
    const agentB = trackAgent(owner.client, (await owner.client.post('/api/agents', {
      name: context.unique('QA copied Global B Agent'),
      instructions: 'Copy the replacement System Default.',
      visibility: 'private',
      public_to: []
    })).data);
    assert.equal(agentB.default_model_connection_id, globalB.id);
    await administrator.client.delete(`/api/model-connections/${globalB.id}`, { expectedStatus: 409 });

    await superClient.request('/api/model-connections/system-default', {
      method: 'PUT',
      body: { model_connection_id: null }
    });
    const unconfiguredAgent = trackAgent(owner.client, (await owner.client.post('/api/agents', {
      name: context.unique('QA model unconfigured Agent'),
      instructions: 'Remain model-unconfigured.',
      visibility: 'private',
      public_to: []
    })).data);
    assert.equal(unconfiguredAgent.default_model_connection_id, null);
    await owner.client.post(`/api/agents/${unconfiguredAgent.id}/runs`, {
      message: 'A model-unconfigured Agent must not start.',
      hub_session_id: null,
      parent_run_id: null
    }, { expectedStatus: 409 });

    const disabledA = (await administrator.client.request(`/api/model-connections/${globalA.id}/status`, {
      method: 'PUT',
      body: { status: 'disabled' }
    })).data;
    assert.equal(disabledA.status, 'disabled');
    await owner.client.post(`/api/agents/${agentA.id}/runs`, {
      message: 'A disabled model must reject the next request.',
      hub_session_id: null,
      parent_run_id: null
    }, { expectedStatus: 409 });
    const enabledA = (await administrator.client.request(`/api/model-connections/${globalA.id}/status`, {
      method: 'PUT',
      body: { status: 'enabled' }
    })).data;
    assert.equal(enabledA.status, 'enabled');
    assert.equal((await administrator.client.post(`/api/model-connections/${globalA.id}/test`)).data.success, true);

    const errorFilters = { model_connection_id: errorGlobal.id };
    for (let index = 0; index < 2; index += 1) {
      const result = (await administrator.client.post(`/api/model-connections/${errorGlobal.id}/test`)).data;
      assert.equal(result.success, false);
      assert.equal(result.status_code, 200);
      assert.equal(result.error_code, 'fake_model_error');
      assert.equal(result.message, 'Deterministic fake provider failure.');
    }
    await waitForLedgerItems(administrator.client, '/api/model-usage', errorFilters, 2);
    await waitForLedgerItems(administrator.client, '/api/model-call-errors', errorFilters, 2);
    const pagedTestUsage = await readTwoKeysetPages(administrator.client, '/api/model-usage', errorFilters);
    const pagedTestErrors = await readTwoKeysetPages(administrator.client, '/api/model-call-errors', errorFilters);
    assert.equal(pagedTestUsage.every((item) => item.response_status === 'failed'), true);
    assert.equal(pagedTestErrors.every((item) => item.error_code === 'fake_model_error'), true);
    assert.equal(pagedTestErrors.every((item) => item.agent.id === null), true);
    assert.equal(pagedTestErrors.every((item) => item.subject.id === administrator.user.id), true);
    assert.equal(JSON.stringify(pagedTestErrors).includes(PROVIDER_API_KEY), false);
    await administrator.client.delete(`/api/model-connections/${errorGlobal.id}`, { expectedStatus: 204 });
    await administrator.client.get(`/api/model-connections/${errorGlobal.id}`, { expectedStatus: 404 });
    const errorsAfterOrdinaryDelete = (await administrator.client.get(ledgerPath(
      '/api/model-call-errors',
      { ...errorFilters, page_size: 100 }
    ))).data.items;
    assert.equal(errorsAfterOrdinaryDelete.length, 2);
    assert.equal(errorsAfterOrdinaryDelete.every((item) => item.model.id === errorGlobal.id), true);
    assert.equal(errorsAfterOrdinaryDelete.every((item) => item.model.name === errorGlobal.name), true);

    const rangeStart = Date.now() - 1_000;
    const { data: successfulRun } = await owner.client.post(`/api/agents/${agentA.id}/runs`, {
      message: 'Verify successful Responses usage accounting.',
      hub_session_id: null,
      parent_run_id: null
    });
    await waitForRunStatus(owner.client, agentA.id, successfulRun.id, 'completed', 60_000);
    const { data: failedRun } = await owner.client.post(`/api/agents/${agentA.id}/runs`, {
      message: 'fixture:model-error verify failed Responses usage and error accounting',
      hub_session_id: null,
      parent_run_id: null
    });
    await waitForRunStatus(owner.client, agentA.id, failedRun.id, 'failed', 60_000);
    const rangeEnd = Date.now() + 1_000;
    const agentFilters = {
      from_ms: rangeStart,
      to_ms: rangeEnd,
      model_connection_id: globalA.id,
      agent_id: agentA.id,
      user_id: owner.user.id
    };
    const agentUsage = await waitForLedgerItems(owner.client, '/api/model-usage', agentFilters, 2);
    const agentErrors = await waitForLedgerItems(owner.client, '/api/model-call-errors', agentFilters, 1);
    const completedUsage = agentUsage.find((item) => item.response_status === 'completed');
    const failedUsage = agentUsage.find((item) => item.response_status === 'failed');
    assert.deepEqual(usageTotals([completedUsage]), {
      input_tokens: 11,
      output_tokens: 7,
      total_tokens: 18,
      cached_tokens: 3,
      reasoning_tokens: 5
    });
    assert.deepEqual(usageTotals([failedUsage]), {
      input_tokens: 5,
      output_tokens: 2,
      total_tokens: 7,
      cached_tokens: 1,
      reasoning_tokens: 1
    });
    assert.equal(agentUsage.every((item) => item.agent.id === agentA.id), true);
    assert.equal(agentUsage.every((item) => item.agent.name === agentA.name), true);
    assert.equal(agentUsage.every((item) => item.subject.id === owner.user.id), true);
    assert.equal(agentErrors[0].error_code, 'fake_model_error');
    assert.equal(agentErrors[0].message, 'Deterministic fake provider failure.');

    const { data: summary } = await owner.client.get(ledgerPath('/api/model-usage/summary', agentFilters));
    const expectedTotals = usageTotals(agentUsage);
    assert.deepEqual(summary.overall, expectedTotals);
    assert.deepEqual(summary.by_model.find((item) => item.model.id === globalA.id)?.totals, expectedTotals);
    assert.deepEqual(summary.by_agent.find((item) => item.agent.id === agentA.id)?.totals, expectedTotals);
    assert.deepEqual(summary.by_user.find((item) => item.user_id === owner.user.id)?.totals, expectedTotals);
    const { data: beforeRange } = await owner.client.get(ledgerPath('/api/model-usage/summary', {
      ...agentFilters,
      from_ms: rangeStart - 100_000,
      to_ms: rangeStart
    }));
    assert.equal(beforeRange.overall.total_tokens, 0, 'The millisecond to_ms bound must be exclusive');
    await owner.client.get(ledgerPath('/api/model-usage', {
      ...agentFilters,
      from_ms: rangeEnd,
      to_ms: rangeEnd
    }), { expectedStatus: 400 });
    const pagedAgentUsage = await readTwoKeysetPages(owner.client, '/api/model-usage', agentFilters);
    assert.deepEqual(new Set(pagedAgentUsage.map((item) => item.id)), new Set(agentUsage.map((item) => item.id)));

    const sharedAgent = trackAgent(administrator.client, (await administrator.client.post('/api/agents', {
      name: context.unique('QA shared attribution Agent'),
      instructions: 'Attribute shared-Agent usage to the caller.',
      visibility: 'public',
      public_to: [],
      default_model_connection_id: globalB.id,
      reasoning_effort: 'medium',
      codex_subagents: []
    })).data);
    const { data: sharedRun } = await outsider.client.post(`/api/agents/${sharedAgent.id}/runs`, {
      message: 'Verify shared Agent caller attribution.',
      hub_session_id: null,
      parent_run_id: null
    });
    await waitForRunStatus(outsider.client, sharedAgent.id, sharedRun.id, 'completed', 60_000);
    const sharedUsage = await waitForLedgerItems(outsider.client, '/api/model-usage', {
      model_connection_id: globalB.id,
      agent_id: sharedAgent.id,
      user_id: outsider.user.id
    }, 1);
    assert.equal(sharedUsage[0].subject.id, outsider.user.id);
    assert.equal(sharedUsage[0].agent.id, sharedAgent.id);
    const adminSharedUsage = (await administrator.client.get(ledgerPath('/api/model-usage', {
      model_connection_id: globalB.id,
      agent_id: sharedAgent.id,
      user_id: outsider.user.id,
      page_size: 100
    }))).data.items;
    assert.equal(adminSharedUsage.length, 1);
    assert.equal(adminSharedUsage[0].subject.id, outsider.user.id);

    await administrator.client.delete(`/api/model-connections/${globalA.id}`, { expectedStatus: 409 });
    await administrator.client.post(`/api/model-connections/${globalA.id}/force-delete`, undefined, {
      expectedStatus: 204
    });
    await administrator.client.get(`/api/model-connections/${globalA.id}`, { expectedStatus: 404 });
    const { data: detachedAgent } = await owner.client.get(`/api/agents/${agentA.id}`);
    assert.equal(detachedAgent.default_model_connection_id, null);
    const deletedOverride = detachedAgent.codex_subagents.find((item) => item.name === 'global_override');
    assert.equal(deletedOverride.enabled, false);
    assert.equal(deletedOverride.model_connection_id, null);
    assert.equal(deletedOverride.disabled_reason, 'model_connection_deleted');
    const retainedOverride = detachedAgent.codex_subagents.find((item) => item.name === 'personal_override');
    assert.equal(retainedOverride.enabled ?? true, true);
    assert.equal(retainedOverride.model_connection_id, ownerPersonal.id);
    await owner.client.post(`/api/agents/${agentA.id}/runs`, {
      message: 'Force-deleted default must leave the Agent model-unconfigured.',
      hub_session_id: null,
      parent_run_id: null
    }, { expectedStatus: 409 });
    assert.equal(
      context.compose.psql(`
        SELECT (deleted_at IS NOT NULL)::text || '|' ||
               (base_url IS NULL)::text || '|' ||
               (api_key_ciphertext IS NULL)::text || '|' ||
               (api_key_nonce IS NULL)::text
        FROM model_connections WHERE id = ${sqlLiteral(globalA.id)}
      `),
      'true|true|true|true',
      'Force Delete must remove executable configuration and encrypted secret material'
    );
    const usageAfterModelDelete = (await owner.client.get(ledgerPath(
      '/api/model-usage',
      { ...agentFilters, page_size: 100 }
    ))).data.items;
    const errorsAfterModelDelete = (await owner.client.get(ledgerPath(
      '/api/model-call-errors',
      { ...agentFilters, page_size: 100 }
    ))).data.items;
    assert.equal(usageAfterModelDelete.length, 2);
    assert.equal(errorsAfterModelDelete.length, 1);
    assert.equal(usageAfterModelDelete.every((item) => item.model.id === globalA.id), true);
    assert.equal(usageAfterModelDelete.every((item) => item.model.name === globalA.name), true);
    assert.equal(usageAfterModelDelete.every((item) => item.model.model_id === SUCCESS_MODEL_ID), true);

    await owner.client.delete(`/api/agents/${agentA.id}`, { expectedStatus: 204 });
    const usageAfterAgentDelete = (await owner.client.get(ledgerPath(
      '/api/model-usage',
      { ...agentFilters, page_size: 100 }
    ))).data.items;
    const errorsAfterAgentDelete = (await owner.client.get(ledgerPath(
      '/api/model-call-errors',
      { ...agentFilters, page_size: 100 }
    ))).data.items;
    assert.equal(usageAfterAgentDelete.length, 2);
    assert.equal(errorsAfterAgentDelete.length, 1);
    assert.equal(usageAfterAgentDelete.every((item) => item.agent.name === agentA.name), true);
    assert.equal(errorsAfterAgentDelete[0].agent.name, agentA.name);
  } catch (error) {
    scenarioFailure = error;
  } finally {
    const cleanupErrors = [];
    if (defaultSnapshotTaken) {
      try {
        const { data: restored } = await superClient.request('/api/model-connections/system-default', {
          method: 'PUT',
          body: { model_connection_id: originalDefault.model_connection_id }
        });
        assert.deepEqual(restored, originalDefault);
        const { data: persisted } = await superClient.get('/api/model-connections/system-default');
        assert.deepEqual(persisted, originalDefault);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    for (const resource of createdAgents.toReversed()) {
      try {
        await resource.client.delete(`/api/agents/${resource.id}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    for (const resource of createdConnections.toReversed()) {
      try {
        await resource.client.post(`/api/model-connections/${resource.id}/force-delete`, undefined, {
          expectedStatus: [204, 404]
        });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (promotedAdminId) {
      try {
        const { data: demoted } = await superClient.request(`/api/admin/users/${promotedAdminId}/role`, {
          method: 'PUT',
          body: { role: 'member' }
        });
        assert.equal(demoted.user.role, 'member');
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (scenarioFailure && cleanupErrors.length === 0) throw scenarioFailure;
    if (!scenarioFailure && cleanupErrors.length === 1) throw cleanupErrors[0];
    if (scenarioFailure || cleanupErrors.length > 0) {
      throw new AggregateError(
        [scenarioFailure, ...cleanupErrors].filter(Boolean),
        'Model Connections API scenario or mandatory cleanup failed'
      );
    }
  }
}
