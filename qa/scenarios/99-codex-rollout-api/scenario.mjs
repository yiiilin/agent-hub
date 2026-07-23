import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const HOLD_MARKER = 'fixture:hold';
const RELEASE_MARKER = 'fixture:release';
const RUNTIME_WORK_ROOT = '/var/lib/agent-hub-runtime';
const REQUIRED_PLATFORMS = [
  ['linux', 'aarch64', 'codex-aarch64-unknown-linux-musl.zst'],
  ['linux', 'x86_64', 'codex-x86_64-unknown-linux-musl.zst']
];

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function updateAgentPayload(agent, runtimeId) {
  return {
    name: agent.name,
    instructions: agent.instructions,
    visibility: agent.visibility,
    public_to: agent.public_to,
    runtime_id: runtimeId,
    model_selection: agent.model_selection,
    model_settings: agent.model_settings,
    codex_subagents: agent.codex_subagents,
    sandbox_policy: agent.sandbox_policy,
    managed_skill_ids: agent.managed_skill_ids,
    mcp_allowlist: agent.mcp_allowlist
  };
}

async function waitForSession(client, sessionId, accept, description, timeoutMs = 60_000) {
  return poll(async () => (await client.get(`/api/sessions/${sessionId}`)).data, accept, {
    timeoutMs,
    description
  });
}

async function waitForTurnStarted(client, runId, description) {
  return poll(async () => {
    const { data: events } = await client.get(`/api/runs/${runId}/events`);
    const started = events.filter((event) => event.event_type === 'turn_started');
    return { events, started };
  }, ({ started }) => started.length === 1
    && typeof started[0].payload?.native_thread_id === 'string'
    && started[0].payload.native_thread_id.length > 0
    && typeof started[0].payload?.native_turn_id === 'string'
    && started[0].payload.native_turn_id.length > 0, {
    timeoutMs: 60_000,
    description
  });
}

async function waitForMessageDelivery(client, sessionId, messageId) {
  return poll(async () => {
    const { data: messages } = await client.get(`/api/sessions/${sessionId}/messages`);
    return messages.find((message) => message.id === messageId) ?? null;
  }, (message) => message?.delivery_state === 'delivered', {
    timeoutMs: 60_000,
    description: `Session Message ${messageId} delivery`
  });
}

function assertArtifactCatalog(rollout, targetVersion) {
  assert.equal(rollout.target_version, targetVersion);
  for (const [os, architecture, artifactName] of REQUIRED_PLATFORMS) {
    const artifact = rollout.artifacts.find((candidate) => candidate.os === os
      && candidate.architecture === architecture);
    assert.ok(artifact, `rollout must publish ${os}/${architecture} artifact metadata`);
    assert.equal(artifact.version, targetVersion);
    assert.equal(artifact.artifact_name, artifactName);
    assert.match(artifact.sha256, SHA256_PATTERN);
    assert.ok(Number.isSafeInteger(artifact.size_bytes) && artifact.size_bytes > 0);
  }
}

function assertHeldIdentity(events, identity) {
  const started = events.filter((event) => event.event_type === 'turn_started');
  assert.equal(started.length, 1, 'held Run must have exactly one native turn_started event');
  assert.equal(started[0].payload.native_thread_id, identity.nativeThreadId);
  assert.equal(started[0].payload.native_turn_id, identity.nativeTurnId);
  assert.equal(events.some((event) => event.event_type === 'status'
    && (event.content === 'interrupted' || event.payload?.status === 'interrupted')), false,
  'held Run must never be interrupted');
}

function runtimeSessionMetadata(context, sessionId) {
  const script = String.raw`
set -eu
metadata="${RUNTIME_WORK_ROOT}/sessions/$1/supervisor/session.json"
test -f "$metadata"
jq -er '[
  .session_id,
  .runtime_id,
  .codex_version,
  .native_thread_id,
  .lifecycle_status,
  (.checkpoint_reason // "")
] | @tsv' "$metadata"
`;
  const output = context.compose.run([
    'exec', '-T', 'runtime', 'sh', '-lc', script, 'qa-rollout-metadata', sessionId
  ]).stdout.trim();
  const [metadataSessionId, runtimeId, codexVersion, nativeThreadId, lifecycleStatus, checkpointReason] = output.split('\t');
  return {
    sessionId: metadataSessionId,
    runtimeId,
    codexVersion,
    nativeThreadId,
    lifecycleStatus,
    checkpointReason: checkpointReason ?? ''
  };
}

function installedCodexVersion(context, targetVersion) {
  const script = String.raw`
set -eu
binary="${RUNTIME_WORK_ROOT}/bin/$1/codex"
test -x "$binary"
"$binary" --version
`;
  return context.compose.run([
    'exec', '-T', 'runtime', 'sh', '-lc', script, 'qa-rollout-binary', targetVersion
  ]).stdout.trim();
}

async function waitForRuntimeSessionRemoval(context, sessionId) {
  await poll(() => context.compose.run([
    'exec', '-T', 'runtime', 'sh', '-lc',
    `test -e "${RUNTIME_WORK_ROOT}/sessions/$1" && printf present || printf absent`,
    'qa-rollout-cleanup', sessionId
  ]).stdout.trim(), (value) => value === 'absent', {
    timeoutMs: 45_000,
    description: `Runtime Session ${sessionId} cleanup`
  });
}

async function registerPlatformCatalogRuntime(client, context, realRuntime, architecture) {
  const { data: enrollment } = await client.post('/api/admin/runtime-enrollment-tokens', {});
  assertUuid(enrollment.enrollment.id, 'platform-catalog enrollment id');
  assert.ok(typeof enrollment.token === 'string' && enrollment.token.length > 0,
    'platform-catalog enrollment secret must be returned once');

  const hostname = context.unique(`qa-rollout-${architecture}`);
  const { data: registration } = await client.post('/api/runtime/register', {
    hostname,
    labels: ['qa', 'rollout-platform-catalog'],
    codex_version: realRuntime.codex_version,
    capabilities: {
      driver: 'app-server',
      codex_source: 'path',
      platform: { os: 'linux', architecture },
      model_proxy: true,
      mcp_allowlist: true,
      thread_resume: true,
      local_skills: false
    },
    sandbox_mode: 'workspace-write+network'
  }, {
    headers: { authorization: `Bearer ${enrollment.token}` }
  });
  assertUuid(registration.runtime_id, 'platform-catalog Runtime id');
  assert.ok(typeof registration.runtime_credential === 'string'
    && registration.runtime_credential.length > 0,
  'platform-catalog Runtime credential must be returned once');
  return { id: registration.runtime_id, hostname };
}

async function deletePlatformCatalogRuntime(client, runtime) {
  const { data: deleted } = await client.post(
    `/api/admin/runtimes/${runtime.id}/force-delete`,
    { hostname: runtime.hostname }
  );
  assert.equal(deleted.runtime_id, runtime.id);
  assert.deepEqual(deleted.recoverable_session_ids, []);
  assert.deepEqual(deleted.recovery_failed_session_ids, []);
}

async function releaseHeldTurnForCleanup(client, agentId, runId, sessionId) {
  const { data: run } = await client.get(`/api/runs/${runId}`);
  if (!['running', 'waiting_tool'].includes(run.status)) return;
  await client.post(`/api/sessions/${sessionId}/messages`, {
    content: RELEASE_MARKER,
    client_message_key: `cleanup-release-${runId}`
  }, { expectedStatus: [200, 404, 409] });
  await waitForRunStatus(client, agentId, runId, ['completed', 'failed', 'interrupted'], 45_000);
}

export default async function codexRolloutApiScenario(context) {
  const client = new ApiClient(context.baseURL);
  await loginAsAdmin(client);

  let agentId = null;
  let sessionId = null;
  let heldRunId = null;
  let heldRunCompleted = false;
  let agentDeleted = false;
  let platformCatalogRuntime = null;

  try {
    const { data: initialRollout } = await client.get('/api/admin/codex-version-rollout');
    const { data: initialRuntimes } = await client.get('/api/runtimes');
    const realRuntime = initialRuntimes.find((runtime) => runtime.status === 'online'
      && runtime.capabilities?.driver === 'app-server'
      && runtime.hostname === 'compose-runtime-1')
      ?? initialRuntimes.find((runtime) => runtime.status === 'online'
        && runtime.capabilities?.driver === 'app-server');
    assert.ok(realRuntime, 'real online Compose app-server Runtime must be available');
    assertUuid(realRuntime.id, 'real Runtime id');
    assert.ok(typeof realRuntime.codex_version === 'string' && realRuntime.codex_version.length > 0);

    const initialReadiness = initialRollout.runtimes.find(
      (runtime) => runtime.runtime_id === realRuntime.id
    );
    assert.ok(initialReadiness, 'initial rollout must expose the real Runtime');
    assert.equal(initialReadiness.os, 'linux');
    assert.ok(['x86_64', 'aarch64'].includes(initialReadiness.architecture));
    assert.equal(initialReadiness.current_version, realRuntime.codex_version);

    const agentName = context.unique('QA Codex Rollout Agent');
    const { data: createdAgent } = await client.post('/api/agents', {
      name: agentName,
      instructions: 'Exercise exact Codex rollout without interrupting an active native Turn.',
      visibility: 'private',
      public_to: []
    });
    agentId = createdAgent.id;
    assertUuid(agentId, 'Agent id');
    const { data: agent } = await client.request(`/api/agents/${agentId}`, {
      method: 'PATCH',
      body: updateAgentPayload(createdAgent, realRuntime.id)
    });
    assert.equal(agent.runtime_id, realRuntime.id);

    const { data: heldRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: HOLD_MARKER,
      hub_session_id: null,
      parent_run_id: null
    });
    heldRunId = heldRun.id;
    sessionId = heldRun.hub_session_id;
    assertUuid(heldRunId, 'held Run id');
    assertUuid(sessionId, 'held Session id');
    assertUuid(heldRun.hub_turn_id, 'held Hub Turn id');
    await waitForRunStatus(client, agentId, heldRunId, 'running', 60_000);

    const activeSession = await waitForSession(client, sessionId, (session) =>
      session.lifecycle_status === 'online'
      && session.runtime_owner_id === realRuntime.id
      && session.active_turn_id === heldRun.hub_turn_id
      && typeof session.native_thread_id === 'string'
      && session.native_thread_id.length > 0,
    'held Session to become online with native Thread identity');
    const started = await waitForTurnStarted(client, heldRunId, 'held native Turn to start');
    const identity = {
      runId: heldRunId,
      hubTurnId: heldRun.hub_turn_id,
      nativeThreadId: activeSession.native_thread_id,
      nativeTurnId: started.started[0].payload.native_turn_id
    };
    assert.equal(started.started[0].payload.native_thread_id, identity.nativeThreadId);
    assertHeldIdentity(started.events, identity);

    const { data: heldRunDetail } = await client.get(`/api/runs/${heldRunId}`);
    assert.equal(heldRunDetail.status, 'running');
    assert.equal(heldRunDetail.runtime_id, realRuntime.id);
    assert.equal(heldRunDetail.hub_session_id, sessionId);
    assert.equal(heldRunDetail.hub_turn_id, identity.hubTurnId);
    const initialMetadata = runtimeSessionMetadata(context, sessionId);
    assert.deepEqual({
      sessionId: initialMetadata.sessionId,
      runtimeId: initialMetadata.runtimeId,
      codexVersion: initialMetadata.codexVersion,
      nativeThreadId: initialMetadata.nativeThreadId,
      lifecycleStatus: initialMetadata.lifecycleStatus,
      checkpointReason: initialMetadata.checkpointReason
    }, {
      sessionId,
      runtimeId: realRuntime.id,
      codexVersion: realRuntime.codex_version,
      nativeThreadId: identity.nativeThreadId,
      lifecycleStatus: 'online',
      checkpointReason: ''
    });

    const otherArchitecture = initialReadiness.architecture === 'x86_64' ? 'aarch64' : 'x86_64';
    platformCatalogRuntime = await registerPlatformCatalogRuntime(
      client,
      context,
      realRuntime,
      otherArchitecture
    );

    const unavailableVersions = new Set([
      realRuntime.codex_version,
      initialRollout.active_version,
      initialRollout.target_version
    ].filter(Boolean));
    const targetVersion = ['0.145.0-qa', '0.146.0-qa']
      .find((candidate) => !unavailableVersions.has(candidate));
    assert.ok(targetVersion, 'a distinct exact QA Codex target must be available');

    const { data: distributing } = await client.request('/api/admin/codex-version-rollout/target', {
      method: 'PUT',
      body: { version: targetVersion }
    });
    assert.equal(distributing.active_version, initialRollout.active_version);
    assert.equal(distributing.status, 'distributing');
    assert.equal(distributing.error, null);
    assertArtifactCatalog(distributing, targetVersion);

    await client.post('/api/admin/codex-version-rollout/promote', undefined, {
      expectedStatus: 409
    });
    await deletePlatformCatalogRuntime(client, platformCatalogRuntime);
    platformCatalogRuntime = null;

    const ready = await poll(async () =>
      (await client.get('/api/admin/codex-version-rollout')).data,
    (rollout) => rollout.status === 'ready', {
      timeoutMs: 90_000,
      description: `Codex rollout ${targetVersion} readiness`
    });
    assert.equal(ready.active_version, initialRollout.active_version);
    assert.equal(ready.error, null);
    assertArtifactCatalog(ready, targetVersion);
    const realReadiness = ready.runtimes.find((runtime) => runtime.runtime_id === realRuntime.id);
    assert.ok(realReadiness, 'ready rollout must include the real Runtime');
    assert.equal(realReadiness.os, 'linux');
    assert.equal(realReadiness.architecture, initialReadiness.architecture);
    assert.equal(realReadiness.current_version, realRuntime.codex_version);
    assert.equal(realReadiness.target_version, targetVersion);
    assert.equal(realReadiness.status, 'ready');
    assert.equal(realReadiness.error, null);
    assert.equal(typeof realReadiness.checked_at, 'string');
    assert.equal(installedCodexVersion(context, targetVersion), `codex-cli ${targetVersion}`);

    const { data: promoted } = await client.post('/api/admin/codex-version-rollout/promote');
    assert.equal(promoted.active_version, targetVersion);
    assert.equal(promoted.target_version, null);
    assert.equal(promoted.status, 'active');
    assert.equal(promoted.error, null);
    assertArtifactCatalog({ ...promoted, target_version: targetVersion }, targetVersion);

    const runtimeAfterPromotion = await poll(async () => {
      const { data: runtimes } = await client.get('/api/runtimes');
      return runtimes.find((runtime) => runtime.id === realRuntime.id) ?? null;
    }, (runtime) => runtime?.codex_version === targetVersion, {
      timeoutMs: 60_000,
      description: `real Runtime to report active Codex ${targetVersion}`
    });
    assert.equal(runtimeAfterPromotion.status, 'online');

    const { data: heldAfterPromotion } = await client.get(`/api/runs/${heldRunId}`);
    assert.equal(heldAfterPromotion.id, identity.runId);
    assert.equal(heldAfterPromotion.status, 'running');
    assert.equal(heldAfterPromotion.hub_session_id, sessionId);
    assert.equal(heldAfterPromotion.hub_turn_id, identity.hubTurnId);
    assert.equal(heldAfterPromotion.runtime_id, realRuntime.id);
    const { data: sessionAfterPromotion } = await client.get(`/api/sessions/${sessionId}`);
    assert.equal(sessionAfterPromotion.lifecycle_status, 'online');
    assert.equal(sessionAfterPromotion.active_turn_id, identity.hubTurnId);
    assert.equal(sessionAfterPromotion.native_thread_id, identity.nativeThreadId);
    assert.equal(sessionAfterPromotion.current_bundle, null,
      'promotion must not checkpoint an active Turn');
    const { data: eventsAfterPromotion } = await client.get(`/api/runs/${heldRunId}/events`);
    assertHeldIdentity(eventsAfterPromotion, identity);
    const metadataAfterPromotion = runtimeSessionMetadata(context, sessionId);
    assert.equal(metadataAfterPromotion.codexVersion, realRuntime.codex_version,
      'held Session must retain the Codex version that started its active Turn');
    assert.equal(metadataAfterPromotion.nativeThreadId, identity.nativeThreadId);
    assert.equal(metadataAfterPromotion.lifecycleStatus, 'online');
    assert.equal(metadataAfterPromotion.checkpointReason, '');

    const { data: releaseAcceptance } = await client.post(`/api/sessions/${sessionId}/messages`, {
      content: RELEASE_MARKER,
      client_message_key: context.unique('qa-rollout-release')
    });
    assert.equal(releaseAcceptance.run.id, identity.runId);
    assert.equal(releaseAcceptance.run.hub_turn_id, identity.hubTurnId);
    assert.equal(releaseAcceptance.message.run_id, identity.runId);
    assert.equal(releaseAcceptance.message.turn_id, identity.hubTurnId);
    assert.equal(releaseAcceptance.message.delivery_mode, 'steer');
    assert.equal(releaseAcceptance.message.expected_native_turn_id, identity.nativeTurnId);
    await waitForMessageDelivery(client, sessionId, releaseAcceptance.message.id);

    const completedHeldRun = await waitForRunStatus(
      client,
      agentId,
      heldRunId,
      'completed',
      60_000
    );
    heldRunCompleted = true;
    assert.equal(completedHeldRun.id, identity.runId);
    assert.equal(completedHeldRun.hub_turn_id, identity.hubTurnId);
    const { data: completedEvents } = await client.get(`/api/runs/${heldRunId}/events`);
    assertHeldIdentity(completedEvents, identity);
    assert.equal(completedEvents.some((event) => event.event_type === 'status'
      && (event.content === 'completed' || event.payload?.status === 'completed')), true);

    const checkpointedSession = await waitForSession(client, sessionId, (session) =>
      session.lifecycle_status === 'offline'
      && session.runtime_owner_id === null
      && session.active_turn_id === null
      && session.current_bundle !== null,
    'version_switch checkpoint to publish the old-version current Bundle', 90_000);
    assert.equal(checkpointedSession.native_thread_id, identity.nativeThreadId);
    assert.equal(checkpointedSession.current_bundle.producing_codex_version, realRuntime.codex_version);
    assert.equal(checkpointedSession.current_bundle.history_checkpoint, releaseAcceptance.message.sequence);
    assert.equal(checkpointedSession.current_bundle.ownership_generation, activeSession.ownership_generation);
    assert.match(checkpointedSession.current_bundle.checksum_sha256, SHA256_PATTERN);
    assert.ok(checkpointedSession.current_bundle.size_bytes > 0);
    assert.ok(Date.parse(checkpointedSession.current_bundle.created_at)
      >= Date.parse(completedHeldRun.updated_at),
    'old-version Bundle must be created only after the held Turn completes');
    const { data: runtimeAfterCheckpoint } = await client.get('/api/runtimes');
    assert.equal(
      runtimeAfterCheckpoint.find((runtime) => runtime.id === realRuntime.id)?.codex_version,
      targetVersion
    );

    const nextContent = context.unique('QA ordinary Turn after Codex promotion');
    const { data: nextAcceptance } = await client.post(`/api/sessions/${sessionId}/messages`, {
      content: nextContent,
      client_message_key: context.unique('qa-rollout-next-turn')
    });
    assert.ok(nextAcceptance.run, 'ordinary continuation must create the next Run');
    assert.notEqual(nextAcceptance.run.id, identity.runId);
    assert.equal(nextAcceptance.run.hub_session_id, sessionId);
    assert.notEqual(nextAcceptance.run.hub_turn_id, identity.hubTurnId);
    assert.equal(nextAcceptance.message.delivery_mode, 'next_turn');
    assert.equal(nextAcceptance.message.run_id, nextAcceptance.run.id);
    assert.equal(nextAcceptance.message.turn_id, nextAcceptance.run.hub_turn_id);

    const nextCompleted = await waitForRunStatus(
      client,
      agentId,
      nextAcceptance.run.id,
      'completed',
      90_000
    );
    assert.equal(nextCompleted.runtime_id, realRuntime.id);
    assert.equal(nextCompleted.hub_session_id, sessionId);
    const nextStarted = await waitForTurnStarted(
      client,
      nextAcceptance.run.id,
      'post-promotion native Turn to start'
    );
    assert.equal(nextStarted.started[0].payload.native_thread_id, identity.nativeThreadId,
      'post-promotion Run must resume the original native Thread');
    assert.notEqual(nextStarted.started[0].payload.native_turn_id, identity.nativeTurnId);

    const { data: finalSession } = await client.get(`/api/sessions/${sessionId}`);
    assert.equal(finalSession.lifecycle_status, 'online');
    assert.equal(finalSession.runtime_owner_id, realRuntime.id);
    assert.equal(finalSession.active_turn_id, null);
    assert.equal(finalSession.native_thread_id, identity.nativeThreadId);
    assert.equal(finalSession.current_bundle.generation, checkpointedSession.current_bundle.generation);
    assert.equal(finalSession.current_bundle.producing_codex_version, realRuntime.codex_version);
    const targetMetadata = runtimeSessionMetadata(context, sessionId);
    assert.equal(targetMetadata.codexVersion, targetVersion);
    assert.equal(targetMetadata.nativeThreadId, identity.nativeThreadId);
    assert.equal(targetMetadata.lifecycleStatus, 'online');
    assert.equal(targetMetadata.checkpointReason, '');
    assert.equal(installedCodexVersion(context, targetVersion), `codex-cli ${targetVersion}`);

    await client.delete(`/api/agents/${agentId}`, { expectedStatus: 204 });
    agentDeleted = true;
    const historical = await waitForSession(client, sessionId, (session) =>
      session.lifecycle_status === 'historical' && session.agent_deleted_at !== null,
    'rollout Agent Session to become historical after cleanup');
    assert.equal(historical.native_thread_id, identity.nativeThreadId);
    await waitForRuntimeSessionRemoval(context, sessionId);
  } finally {
    if (platformCatalogRuntime) {
      await client.post(`/api/admin/runtimes/${platformCatalogRuntime.id}/force-delete`, {
        hostname: platformCatalogRuntime.hostname
      }, { expectedStatus: [200, 404] }).catch(() => {});
    }
    if (agentId && heldRunId && sessionId && !heldRunCompleted) {
      await releaseHeldTurnForCleanup(client, agentId, heldRunId, sessionId).catch(() => {});
      const terminal = await client.get(`/api/runs/${heldRunId}`).catch(() => null);
      heldRunCompleted = terminal !== null && !['running', 'waiting_tool'].includes(terminal.data.status);
    }
    if (agentId && !agentDeleted && (!heldRunId || heldRunCompleted)) {
      await client.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] }).catch(() => {});
    }
  }
}
