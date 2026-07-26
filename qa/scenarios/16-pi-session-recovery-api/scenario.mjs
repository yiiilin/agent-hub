import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const RUNTIME_IDLE_TIMEOUT_SECS = '3';
const RUNTIME_SESSION_ROOT = '/var/lib/agent-hub-runtime/sessions';

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Pi recovery model'),
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

async function waitForRuntime(client, runtimeId, description) {
  return poll(async () => {
    const { data: runtimes } = await client.get('/api/runtimes');
    return runtimes.find((runtime) => runtime.id === runtimeId) ?? null;
  }, (runtime) => runtime?.status === 'online', {
    timeoutMs: 45_000,
    description
  });
}

function recreateRuntime(compose) {
  compose.run(['up', '-d', '--no-deps', '--force-recreate', 'runtime'], {
    capture: false,
    timeoutMs: 120_000
  });
}

function runtimeExec(compose, command, options) {
  return compose.run(['exec', '-T', 'runtime', ...command], options);
}

function sessionPaths(sessionId) {
  const root = `${RUNTIME_SESSION_ROOT}/${sessionId}`;
  return {
    root,
    agent: `${root}/engine-state/.pi/agent`,
    agentsFile: `${root}/engine-state/.pi/agent/AGENTS.md`,
    modelsFile: `${root}/engine-state/.pi/agent/models.json`
  };
}

async function waitForSession(client, sessionId, accept, description) {
  return poll(async () => (await client.get(`/api/sessions/${sessionId}`)).data, accept, {
    timeoutMs: 60_000,
    description
  });
}

async function waitForSessionRoot(compose, root, description) {
  await poll(
    () => runtimeExec(compose, ['test', '-d', root], { allowFailure: true }).status,
    (status) => status === 0,
    { timeoutMs: 30_000, description }
  );
}

export default async function piSessionRecoveryScenario(context) {
  const client = new ApiClient(context.baseURL);
  await loginAsAdmin(client);

  const previousIdleTimeout = context.compose.environment.RUNTIME_SESSION_IDLE_TIMEOUT_SECS;
  let runtimeId = null;
  let agent = null;
  let modelConnectionId = null;
  let runtimeConfigurationRestored = false;
  const cleanupErrors = [];
  let scenarioError;

  const restoreRuntimeConfiguration = async () => {
    if (previousIdleTimeout === undefined) {
      delete context.compose.environment.RUNTIME_SESSION_IDLE_TIMEOUT_SECS;
    } else {
      context.compose.environment.RUNTIME_SESSION_IDLE_TIMEOUT_SECS = previousIdleTimeout;
    }
    recreateRuntime(context.compose);
    await waitForRuntime(client, runtimeId, 'Compose Pi Runtime to return online with its normal idle timeout');
    runtimeConfigurationRestored = true;
  };

  try {
    const { data: initialRuntimes } = await client.get('/api/runtimes');
    const composeRuntime = initialRuntimes.find((runtime) => runtime.hostname === 'compose-runtime-1');
    assert.ok(composeRuntime, 'Compose Pi Runtime must be registered before recovery smoke');
    runtimeId = composeRuntime.id;

    context.compose.environment.RUNTIME_SESSION_IDLE_TIMEOUT_SECS = RUNTIME_IDLE_TIMEOUT_SECS;
    recreateRuntime(context.compose);
    await waitForRuntime(client, runtimeId, 'Compose Pi Runtime to restart with a short idle timeout');

    const modelFixture = await createComposeModelFixture(client, context);
    modelConnectionId = modelFixture.connectionId;
    const { data: createdAgent } = await client.post('/api/agents', {
      name: context.unique('QA Pi Recovery Agent'),
      instructions: 'Initial Pi recovery configuration.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    });
    const { data: boundAgent } = await client.request(`/api/agents/${createdAgent.id}`, {
      method: 'PATCH',
      body: updateAgentPayload(createdAgent, { runtime_id: runtimeId })
    });
    agent = boundAgent;
    assert.equal(agent.runtime_id, runtimeId);

    const { data: firstRun } = await client.post(`/api/agents/${agent.id}/runs`, {
      message: context.unique('Complete first Pi recovery Turn'),
      hub_session_id: null,
      parent_run_id: null
    });
    const sessionId = firstRun.hub_session_id;
    assert.ok(sessionId, 'First Pi Run must create a Hub Session');

    const activeFirstSession = await waitForSession(
      client,
      sessionId,
      (session) => session.lifecycle_status === 'online'
        && typeof session.native_session_id === 'string'
        && session.native_session_id.length > 0,
      'first Pi Turn to expose its native Session id'
    );
    const nativePiSessionId = activeFirstSession.native_session_id;
    const firstOwnershipGeneration = activeFirstSession.ownership_generation;
    const paths = sessionPaths(sessionId);
    const bundleSentinel = context.unique('pi-bundle-config-sentinel');

    await waitForSessionRoot(context.compose, paths.root, 'first Pi Session root materialization');
    await waitForRunStatus(client, agent.id, firstRun.id, 'completed', 60_000);
    runtimeExec(context.compose, [
      'sh', '-ceu',
      'test -f "$1"\ntest -f "$2"\nprintf "%s\\n" "$3" > "$1"',
      'qa-pi-recovery', paths.agentsFile, paths.modelsFile, bundleSentinel
    ]);

    const savedSession = await waitForSession(
      client,
      sessionId,
      (session) => session.lifecycle_status === 'offline'
        && session.runtime_owner_id === null
        && session.current_bundle?.generation >= 1,
      'idle Pi Session to commit a Bundle and release ownership'
    );
    assert.equal(savedSession.native_session_id, nativePiSessionId);
    assert.equal(savedSession.current_bundle.producing_engine_version, composeRuntime.engine_version);

    await poll(
      () => runtimeExec(context.compose, ['test', '!', '-e', paths.root], { allowFailure: true }).status,
      (status) => status === 0,
      { timeoutMs: 30_000, description: 'checkpointed Pi Session root removal' }
    );

    const updatedInstructions = context.unique('Recovered Pi configuration');
    const { data: updatedAgent } = await client.request(`/api/agents/${agent.id}`, {
      method: 'PATCH',
      body: updateAgentPayload(agent, { instructions: updatedInstructions })
    });
    agent = updatedAgent;

    await restoreRuntimeConfiguration();

    const { data: secondAcceptance } = await client.post(`/api/sessions/${sessionId}/messages`, {
      content: context.unique('Complete recovered Pi Turn'),
      client_message_key: context.unique('start-recovered-pi-turn')
    });
    const secondRun = secondAcceptance.run;
    assert.ok(secondRun, 'Recovered Pi Session message must schedule a second Run');
    assert.notEqual(secondRun.id, firstRun.id);

    await waitForSessionRoot(context.compose, paths.root, 'recovered Pi Session root materialization');
    const activeRecoveredSession = await waitForSession(
      client,
      sessionId,
      (session) => session.lifecycle_status === 'online'
        && session.native_session_id === nativePiSessionId,
      'second Pi Turn to restore the native Session id from its Bundle'
    );
    assert.ok(activeRecoveredSession.ownership_generation > firstOwnershipGeneration);
    assert.equal(activeRecoveredSession.current_bundle.generation, savedSession.current_bundle.generation);

    const sentinelCheck = runtimeExec(context.compose, [
      'sh', '-ceu',
      '! grep -Fq "$1" "$2"',
      'qa-pi-recovery', bundleSentinel, paths.agentsFile
    ], { allowFailure: true });
    assert.equal(sentinelCheck.status, 0, 'Bundle sentinel must not survive Pi recovery');
    const instructionsCheck = runtimeExec(context.compose, [
      'sh', '-ceu',
      'grep -Fqx "$1" "$2"',
      'qa-pi-recovery', updatedInstructions, paths.agentsFile
    ], { allowFailure: true });
    assert.equal(instructionsCheck.status, 0, 'Updated Agent instructions must be materialized');
    const modelsCheck = runtimeExec(context.compose, [
      'sh', '-ceu',
      'jq -e \'(.providers | type) == "object" and (.providers | length) > 0\' "$1" >/dev/null',
      'qa-pi-recovery', paths.modelsFile
    ], { allowFailure: true });
    assert.equal(modelsCheck.status, 0, 'Pi models.json must materialize a provider configuration');

    await waitForRunStatus(client, agent.id, secondRun.id, 'completed', 60_000);
    const completedSession = await waitForSession(
      client,
      sessionId,
      (session) => session.active_turn_id === null && session.native_session_id === nativePiSessionId,
      'recovered Pi Session to complete its second Turn'
    );

    await writeFile(
      `${context.artifactsDir}/recovery-evidence.json`,
      `${JSON.stringify({
        hub_session_id: sessionId,
        native_pi_session_id: nativePiSessionId,
        first_run_id: firstRun.id,
        second_run_id: secondRun.id,
        bundle_generation: savedSession.current_bundle.generation,
        first_ownership_generation: firstOwnershipGeneration,
        recovered_ownership_generation: activeRecoveredSession.ownership_generation,
        final_lifecycle_status: completedSession.lifecycle_status,
        configuration_sentinel_absent: true,
        updated_instructions_materialized: true
      }, null, 2)}\n`
    );
  } catch (error) {
    scenarioError = error;
  } finally {
    if (agent) {
      try {
        await client.delete(`/api/agents/${agent.id}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (modelConnectionId) {
      try {
        await client.delete(`/api/model-connections/${modelConnectionId}`, {
          expectedStatus: [204, 404]
        });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (!runtimeConfigurationRestored && runtimeId) {
      try {
        await restoreRuntimeConfiguration();
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
  }

  if (scenarioError && cleanupErrors.length > 0) {
    throw new AggregateError([scenarioError, ...cleanupErrors], 'Pi recovery smoke and cleanup failed');
  }
  if (scenarioError) throw scenarioError;
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1) {
    throw new AggregateError(cleanupErrors, 'Pi recovery smoke cleanup failed');
  }
}
