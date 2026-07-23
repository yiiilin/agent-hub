import assert from 'node:assert/strict';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
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
  assert.equal(response.status, 303, 'Mock OIDC must redirect');
  const location = response.headers.get('location');
  assert.equal(typeof location, 'string', 'Mock OIDC redirect must include a location');
  return location;
}

async function oidcLogin(context, prefix) {
  const slug = uniqueSlug(context, prefix);
  const client = new ApiClient(context.baseURL);
  const callback = await manualRedirect(
    client,
    `/api/auth/oidc/mock/start?email=${encodeURIComponent(`${slug}@example.com`)}&sub=${encodeURIComponent(slug)}`,
  );
  await manualRedirect(client, callback);
  const { data: user } = await client.get('/api/auth/me');
  return { client, user };
}

function updatePayload(agent, overrides = {}) {
  return {
    name: agent.name,
    instructions: agent.instructions,
    visibility: agent.visibility,
    public_to: agent.public_to,
    runtime_id: agent.runtime_id,
    model_selection: agent.model_selection,
    model_settings: agent.model_settings,
    codex_subagents: agent.codex_subagents,
    sandbox_policy: agent.sandbox_policy,
    managed_skill_ids: agent.managed_skill_ids,
    mcp_allowlist: agent.mcp_allowlist,
    ...overrides
  };
}

async function updateAgent(client, agent, overrides) {
  return client.request(`/api/agents/${agent.id}`, {
    method: 'PATCH',
    body: updatePayload(agent, overrides)
  });
}

function runtimeProbe(context, workDirRef) {
  const runRoot = dirname(workDirRef);
  const script = String.raw`
set -eu
root="$1"
config="$root/codex/config.toml"
allowlist="$root/codex/mcp-allowlist.json"
test -f "$config"
test -f "$allowlist"
mode="$(stat -c '%a' "$config")"
config_secret=no
allowlist_secret=no
allowlist_redacted=no
skill_v1=no
skill_v2=no
subagent=no
instructions=no
outside_leaks=0
grep -F 'qa-mcp-secret-' "$config" >/dev/null && config_secret=yes || true
grep -F 'qa-mcp-secret-' "$allowlist" >/dev/null && allowlist_secret=yes || true
grep -F '********' "$allowlist" >/dev/null && allowlist_redacted=yes || true
grep -R -F 'QA managed Skill content v1' "$root/codex/skills" >/dev/null 2>&1 && skill_v1=yes || true
grep -R -F 'QA managed Skill content v2' "$root/codex/skills" >/dev/null 2>&1 && skill_v2=yes || true
grep -R -F 'Review the QA change for correctness.' "$root/codex/agents" >/dev/null 2>&1 && subagent=yes || true
grep -F '# QA Agent Instructions' "$root/codex/AGENTS.md" >/dev/null && instructions=yes || true
outside_leaks="$(find "$root" -type f ! -path "$config" -exec grep -Il -F 'qa-mcp-secret-' {} + 2>/dev/null | wc -l | tr -d ' ')"
printf '%s|%s|%s|%s|%s|%s|%s|%s|%s' "$mode" "$config_secret" "$allowlist_secret" "$allowlist_redacted" "$skill_v1" "$skill_v2" "$subagent" "$instructions" "$outside_leaks"
`;
  const output = context.compose.run([
    'exec', '-T', 'runtime', 'sh', '-lc', script, 'qa-probe', runRoot
  ]).stdout.trim();
  const [mode, configSecret, allowlistSecret, allowlistRedacted, skillV1, skillV2, subagent, instructions, outsideLeaks] = output.split('|');
  return {
    runRoot,
    mode,
    configSecret,
    allowlistSecret,
    allowlistRedacted,
    skillV1,
    skillV2,
    subagent,
    instructions,
    outsideLeaks
  };
}

async function waitForRuntimeRootRemoval(context, runRoot) {
  await poll(() => {
    const output = context.compose.run([
      'exec', '-T', 'runtime', 'sh', '-lc',
      'test -e "$1" && printf yes || printf no', 'qa-probe', runRoot
    ]).stdout.trim();
    return output;
  }, (value) => value === 'no', {
    timeoutMs: 15_000,
    description: 'deleted Agent Runtime Session directory cleanup'
  });
}

export default async function agentSkillMcpApiScenario(context) {
  const admin = new ApiClient(context.baseURL);
  await loginAsAdmin(admin);
  const target = await oidcLogin(context, 'qa-agent-target');
  const outsider = await oidcLogin(context, 'qa-agent-outsider');
  const denied = await oidcLogin(context, 'qa-agent-denied');
  const createdAgentIds = [];
  const createdSkillIds = [];
  let scenarioError;
  const cleanupErrors = [];

  try {
    const privateAgentName = context.unique('QA private owner Agent');
    const { data: privateAgent } = await target.client.post('/api/agents', {
      name: privateAgentName,
      instructions: 'Private owner-only instructions.',
      visibility: 'private',
      public_to: []
    });
    createdAgentIds.push(privateAgent.id);
    assert.equal(privateAgent.owner_id, target.user.id);
    await outsider.client.get(`/api/agents/${privateAgent.id}`, { expectedStatus: 404 });
    await target.client.post('/api/agents', {
      name: context.unique('Forbidden public Agent'),
      instructions: 'Members cannot create public Agents.',
      visibility: 'public',
      public_to: []
    }, { expectedStatus: 403 });

    const { data: promotedTarget } = await admin.request(`/api/admin/users/${target.user.id}/role`, {
      method: 'PUT',
      body: { role: 'admin' }
    });
    assert.equal(promotedTarget.user.role, 'admin');

    const { data: publicAgent } = await target.client.post('/api/agents', {
      name: context.unique('QA public Agent'),
      instructions: 'Public invocation boundary.',
      visibility: 'public',
      public_to: []
    });
    createdAgentIds.push(publicAgent.id);
    assert.equal((await denied.client.get(`/api/agents/${publicAgent.id}`)).data.can_invoke, true);

    const skillName = uniqueSlug(context, 'qa-managed-skill');
    const { data: skillV1 } = await target.client.post('/api/skills', {
      name: skillName,
      description: 'Managed Runtime materialization fixture.',
      content: '# QA managed Skill\n\nQA managed Skill content v1'
    });
    createdSkillIds.push(skillV1.id);
    assert.match(skillV1.id, UUID_PATTERN);
    assert.equal(skillV1.owner_id, target.user.id);
    assert.equal(skillV1.revision, 1);
    assert.match(skillV1.content_checksum_sha256, /^[a-f0-9]{64}$/);
    await outsider.client.get(`/api/skills/${skillV1.id}`, { expectedStatus: 404 });

    const { data: bulkOne } = await target.client.post('/api/skills', {
      name: uniqueSlug(context, 'qa-bulk-one'),
      description: 'Atomic delete fixture one.',
      content: 'Bulk one'
    });
    createdSkillIds.push(bulkOne.id);
    const { data: bulkTwo } = await target.client.post('/api/skills', {
      name: uniqueSlug(context, 'qa-bulk-two'),
      description: 'Atomic delete fixture two.',
      content: 'Bulk two'
    });
    createdSkillIds.push(bulkTwo.id);
    await target.client.delete('/api/skills', {
      body: { skill_ids: [bulkOne.id, randomUUID()] },
      expectedStatus: 404
    });
    assert.equal((await target.client.get(`/api/skills/${bulkOne.id}`)).data.id, bulkOne.id);
    assert.equal((await target.client.get(`/api/skills/${bulkTwo.id}`)).data.id, bulkTwo.id);
    const { data: bulkDeleted } = await target.client.delete('/api/skills', {
      body: { skill_ids: [bulkOne.id, bulkTwo.id] }
    });
    assert.deepEqual(new Set(bulkDeleted.deleted_skill_ids), new Set([bulkOne.id, bulkTwo.id]));
    await target.client.get(`/api/skills/${bulkOne.id}`, { expectedStatus: 404 });
    await target.client.get(`/api/skills/${bulkTwo.id}`, { expectedStatus: 404 });

    const { data: modelOptions } = await target.client.get('/api/model-connections/options');
    const fallbackModel = modelOptions.items.find((item) => item.status === 'enabled');
    const defaultSelection = modelOptions.system_default ?? (fallbackModel ? {
      connection_id: fallbackModel.connection_id,
      model_id: fallbackModel.model_id
    } : null);
    assert.match(defaultSelection?.connection_id, UUID_PATTERN,
      'A fake-provider Model Connection must be available');
    assert.equal(typeof defaultSelection?.model_id, 'string');

    const configuredName = context.unique('QA configured Agent');
    const { data: configuredAgent } = await target.client.post('/api/agents', {
      name: configuredName,
      instructions: '# QA Agent Instructions\n\nInitial instructions.',
      visibility: 'public_to',
      public_to: [outsider.user.id],
      model_selection: defaultSelection,
      model_settings: { reasoning_effort: 'high' },
      codex_subagents: [{
        name: 'reviewer',
        description: 'Reviews the current QA change.',
        developer_instructions: 'Review the QA change for correctness.',
        model_selection: defaultSelection,
        model_settings_override: { reasoning_effort: 'max' }
      }]
    });
    createdAgentIds.push(configuredAgent.id);
    assert.equal(configuredAgent.visibility, 'public_to');
    assert.deepEqual(configuredAgent.public_to, [outsider.user.id]);
    assert.deepEqual(configuredAgent.model_selection, defaultSelection);
    assert.equal(configuredAgent.model_settings.reasoning_effort, 'high');
    assert.equal(configuredAgent.codex_subagents[0].name, 'reviewer');
    const { data: configuredOptions } = await target.client.get(`/api/agents/${configuredAgent.id}/model-options`);
    assert.equal(configuredOptions.items.some((item) => (
      item.connection_id === defaultSelection.connection_id
      && item.model_id === defaultSelection.model_id
    )), true);

  const targetView = (await outsider.client.get(`/api/agents/${configuredAgent.id}`)).data;
    assert.equal(targetView.can_invoke, true);
    assert.equal(targetView.can_manage, false);
    assert.deepEqual(targetView.managed_skill_ids, []);
    assert.deepEqual(targetView.mcp_allowlist, []);
    await denied.client.get(`/api/agents/${configuredAgent.id}`, { expectedStatus: 404 });

    const { data: runtimes } = await target.client.get('/api/runtimes');
    const runtime = runtimes.find((candidate) => candidate.status === 'online');
    assert.match(runtime?.id, UUID_PATTERN, 'An online Runtime must be available');
    const mcpSecret = uniqueSlug(context, 'qa-mcp-secret');
    const mcpEntry = {
      name: uniqueSlug(context, 'qa-mcp-server'),
      command: 'qa-mcp-command',
      args: ['--mode', 'read-only'],
      secrets: { QA_TOKEN: mcpSecret }
    };
    const { data: configured } = await updateAgent(target.client, configuredAgent, {
      name: `${configuredName} updated`,
      instructions: '# QA Agent Instructions\n\nUse the managed Skill and MCP entry.',
      runtime_id: runtime.id,
      managed_skill_ids: [skillV1.id],
      mcp_allowlist: [mcpEntry]
    });
    assert.equal(configured.runtime_id, runtime.id);
    assert.deepEqual(configured.managed_skill_ids, [skillV1.id]);
    assert.equal(configured.mcp_allowlist[0].secrets.QA_TOKEN, '********');
    assert.equal(JSON.stringify(configured).includes(mcpSecret), false, 'Agent API must redact MCP plaintext');

    const redactedEntry = structuredClone(configured.mcp_allowlist[0]);
    redactedEntry.args = [...redactedEntry.args, '--verbose'];
    const { data: placeholderRoundTrip } = await updateAgent(target.client, configured, {
      mcp_allowlist: [redactedEntry]
    });
    assert.equal(placeholderRoundTrip.mcp_allowlist[0].secrets.QA_TOKEN, '********');
    assert.deepEqual(placeholderRoundTrip.mcp_allowlist[0].args, ['--mode', 'read-only', '--verbose']);
    assert.equal(JSON.stringify(placeholderRoundTrip).includes(mcpSecret), false, 'Placeholder round-trip must stay redacted');

    const firstMessage = context.unique('QA first materialized Turn');
    const { data: firstRun } = await target.client.post(`/api/agents/${configured.id}/runs`, {
      message: firstMessage,
      hub_session_id: null,
      parent_run_id: null
    });
    const firstCompleted = await waitForRunStatus(target.client, configured.id, firstRun.id, 'completed', 60_000);
    assert.match(firstCompleted.work_dir_ref, /^\//);
    assert.match(firstCompleted.hub_session_id, UUID_PATTERN);

    const firstProbe = runtimeProbe(context, firstCompleted.work_dir_ref);
    assert.deepEqual({
      mode: firstProbe.mode,
      configSecret: firstProbe.configSecret,
      allowlistSecret: firstProbe.allowlistSecret,
      allowlistRedacted: firstProbe.allowlistRedacted,
      skillV1: firstProbe.skillV1,
      subagent: firstProbe.subagent,
      instructions: firstProbe.instructions,
      outsideLeaks: firstProbe.outsideLeaks
    }, {
      mode: '600',
      configSecret: 'yes',
      allowlistSecret: 'no',
      allowlistRedacted: 'yes',
      skillV1: 'yes',
      subagent: 'yes',
      instructions: 'yes',
      outsideLeaks: '0'
    });
    const { data: firstEvents } = await target.client.get(`/api/runs/${firstRun.id}/events`);
    assert.equal(JSON.stringify(firstEvents).includes(mcpSecret), false, 'Run events must not expose MCP plaintext');
    assert.equal(context.compose.logs().includes(mcpSecret), false, 'Compose logs must not expose MCP plaintext');

    const { data: skillV2 } = await target.client.request(`/api/skills/${skillV1.id}`, {
      method: 'PATCH',
      body: {
        name: skillName,
        description: 'Managed Runtime materialization fixture updated.',
        content: '# QA managed Skill\n\nQA managed Skill content v2'
      }
    });
    assert.equal(skillV2.revision, 2);
    assert.notEqual(skillV2.content_checksum_sha256, skillV1.content_checksum_sha256);

    const secondMessage = context.unique('QA refreshed materialized Turn');
    const { data: secondRun } = await target.client.post(`/api/agents/${configured.id}/runs`, {
      message: secondMessage,
      hub_session_id: firstCompleted.hub_session_id,
      parent_run_id: firstRun.id
    });
    const secondCompleted = await waitForRunStatus(target.client, configured.id, secondRun.id, 'completed', 60_000);
    assert.equal(secondCompleted.hub_session_id, firstCompleted.hub_session_id);
    assert.equal(dirname(secondCompleted.work_dir_ref), firstProbe.runRoot);
    const secondProbe = runtimeProbe(context, secondCompleted.work_dir_ref);
    assert.equal(secondProbe.skillV2, 'yes', 'The next Turn must materialize the updated Skill revision');
    assert.equal(secondProbe.skillV1, 'no', 'The previous Skill content must be replaced between Turns');
    assert.equal(secondProbe.outsideLeaks, '0');

    await target.client.delete(`/api/skills/${skillV1.id}`, { expectedStatus: 204 });
    const { data: unboundAgent } = await target.client.get(`/api/agents/${configured.id}`);
    assert.deepEqual(unboundAgent.managed_skill_ids, [], 'Deleting a Skill must remove Agent bindings');

    const { data: messagesBeforeDelete } = await target.client.get(`/api/sessions/${firstCompleted.hub_session_id}/messages`);
    assert.equal(messagesBeforeDelete.some((message) => message.content === firstMessage), true);
    assert.equal(messagesBeforeDelete.some((message) => message.content === secondMessage), true);
    await target.client.delete(`/api/agents/${configured.id}`, { expectedStatus: 204 });

    const { data: historical } = await target.client.get(`/api/sessions/${firstCompleted.hub_session_id}`);
    assert.equal(historical.lifecycle_status, 'historical');
    assert.equal(historical.agent_name, `${configuredName} updated`);
    assert.equal(typeof historical.agent_deleted_at, 'string');
    const { data: historicalMessages } = await target.client.get(`/api/sessions/${historical.id}/messages`);
    assert.equal(historicalMessages.some((message) => message.content === firstMessage), true);
    assert.equal(historicalMessages.some((message) => message.content === secondMessage), true);
    await target.client.post(`/api/sessions/${historical.id}/messages`, {
      content: 'Historical Sessions must not continue.',
      payload: {},
      delivery_mode: 'next_turn',
      client_message_key: context.unique('historical-reject'),
      parent_run_id: secondRun.id
    }, { expectedStatus: 404 });
    await target.client.post(`/api/agents/${configured.id}/runs`, {
      message: 'Deleted Agents must not run.',
      hub_session_id: historical.id,
      parent_run_id: secondRun.id
    }, { expectedStatus: 404 });
    await waitForRuntimeRootRemoval(context, firstProbe.runRoot);

  } catch (error) {
    scenarioError = error;
  } finally {
    for (const agentId of createdAgentIds.toReversed()) {
      try {
        await target.client.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    for (const skillId of createdSkillIds.toReversed()) {
      try {
        await target.client.delete(`/api/skills/${skillId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
  }

  if (scenarioError && cleanupErrors.length > 0) {
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Agent Skill MCP scenario and cleanup failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) {
    throw new AggregateError(cleanupErrors, 'Agent Skill MCP scenario cleanup failed');
  }
}
