import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const BUNDLE_BYTES = Buffer.from('KLUv/QRYTQAAEAAAAQD7hwdYvL1+1g==', 'base64');
const BUNDLE_SHA256 = createHash('sha256').update(BUNDLE_BYTES).digest('hex');
const WRONG_BUNDLE_SHA256 = '0'.repeat(64);
const CHECKPOINT_FAILURE_CODE = 'qa_checkpoint_archive_write_failed';

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function runtimeHeaders(credential, headers = {}) {
  return {
    authorization: `Bearer ${credential}`,
    ...headers
  };
}

async function runtimeJson(client, credential, path, body, expectedStatus = 200) {
  return client.request(path, {
    method: 'POST',
    body,
    headers: runtimeHeaders(credential),
    expectedStatus
  });
}

async function registerRuntime(admin, context, codexVersion, prefix) {
  const hostname = uniqueSlug(context, prefix);
  const { data: enrollment } = await admin.post('/api/admin/runtime-enrollment-tokens', {});
  assertUuid(enrollment.enrollment.id, 'Runtime enrollment id');
  assert.equal(typeof enrollment.token, 'string');
  assert.ok(enrollment.token.length > 20, 'Runtime enrollment token must be secret-once material');

  const client = new ApiClient(admin.baseURL);
  const { data: registered } = await client.post('/api/runtime/register', {
    hostname,
    labels: ['qa', 'bundle-recovery'],
    codex_version: codexVersion,
    capabilities: {
      driver: 'qa-http',
      platform: { os: 'linux', architecture: 'x86_64' },
      model_proxy: true,
      mcp_allowlist: true,
      thread_resume: true
    },
    sandbox_mode: 'workspace-write'
  }, {
    headers: runtimeHeaders(enrollment.token)
  });
  assertUuid(registered.runtime_id, 'Runtime id');
  assert.equal(typeof registered.runtime_credential, 'string');
  assert.ok(registered.runtime_credential.length > 20, 'Runtime credential must be secret material');

  return {
    id: registered.runtime_id,
    hostname,
    credential: registered.runtime_credential,
    client,
    deleted: false
  };
}

async function createBoundAgent(admin, context, runtimeId, prefix) {
  const { data: created } = await admin.post('/api/agents', {
    name: context.unique(prefix),
    instructions: 'Exercise deterministic Bundle and recovery protocol behavior.',
    visibility: 'private',
    public_to: []
  });
  const agent = await bindAgentToRuntime(admin, created, runtimeId);
  assert.equal(agent.runtime_id, runtimeId);
  return agent;
}

async function bindAgentToRuntime(admin, agent, runtimeId) {
  const { data: updated } = await admin.request(`/api/agents/${agent.id}`, {
    method: 'PATCH',
    body: {
      name: agent.name,
      instructions: agent.instructions,
      visibility: agent.visibility,
      public_to: agent.public_to,
      runtime_id: runtimeId,
      default_model_connection_id: agent.default_model_connection_id,
      reasoning_effort: agent.reasoning_effort,
      codex_subagents: agent.codex_subagents,
      sandbox_policy: agent.sandbox_policy,
      managed_skill_ids: agent.managed_skill_ids,
      mcp_allowlist: agent.mcp_allowlist
    }
  });
  return updated;
}

async function claimRun(runtime, expectedRunId) {
  const { data: claim } = await runtimeJson(
    runtime.client,
    runtime.credential,
    '/api/runtime/runs/claim',
    { available_new_session_slots: 1, ready_owned_sessions: [] }
  );
  assert.equal(claim.run.id, expectedRunId);
  assert.ok(claim.session_context, 'Runtime claim must include Session context');
  const generation = claim.session_context.session.ownership_generation;
  assert.ok(Number.isInteger(generation) && generation > 0);
  assert.equal(claim.run.session_ownership_generation, generation);
  assert.equal(claim.session_context.turn.ownership_generation, generation);
  assert.equal(claim.session_context.session.runtime_owner_id, runtime.id);
  assert.equal(claim.session_context.session.lifecycle_status, 'restoring');
  return { claim, generation };
}

async function beginAndCompleteTurn(runtime, claim, generation, nativeThreadId, nativeTurnId) {
  const runId = claim.run.id;
  const { data: begun } = await runtimeJson(
    runtime.client,
    runtime.credential,
    `/api/runtime/runs/${runId}/turn/begin`,
    {
      ownership_generation: generation,
      payload: { configuration_fingerprint: claim.expected_configuration_fingerprint }
    }
  );
  assert.equal(begun.session_id, claim.run.hub_session_id);
  assert.equal(begun.turn_id, claim.run.hub_turn_id);
  assert.equal(begun.ownership_generation, generation);

  const { data: started } = await runtimeJson(
    runtime.client,
    runtime.credential,
    `/api/runtime/runs/${runId}/events`,
    {
      ownership_generation: generation,
      payload: {
        event_type: 'turn_started',
        role: null,
        content: null,
        payload: {
          native_thread_id: nativeThreadId,
          native_turn_id: nativeTurnId
        }
      }
    }
  );
  assert.equal(started.event_type, 'turn_started');
  assert.equal(started.payload.native_thread_id, nativeThreadId);
  assert.equal(started.payload.native_turn_id, nativeTurnId);

  const { data: completed } = await runtimeJson(
    runtime.client,
    runtime.credential,
    `/api/runtime/runs/${runId}/complete`,
    {
      ownership_generation: generation,
      payload: {
        status: 'completed',
        session_id: nativeThreadId,
        work_dir_ref: `/qa/${runId}`
      }
    }
  );
  assert.equal(completed.status, 'completed');
  assert.equal(completed.session_id, nativeThreadId);
  return completed;
}

async function heartbeat(runtime, { ownedSessions = [], cleanedSessions = [] } = {}) {
  const { data } = await runtimeJson(
    runtime.client,
    runtime.credential,
    '/api/runtime/heartbeat',
    {
      accepts_session_commands: true,
      owned_sessions: ownedSessions,
      cleaned_sessions: cleanedSessions
    }
  );
  return data;
}

async function enterSaving(runtime, sessionId, generation, reason = 'idle') {
  const response = await heartbeat(runtime, {
    ownedSessions: [{
      session_id: sessionId,
      ownership_generation: generation,
      lifecycle_status: 'saving',
      checkpoint_reason: reason
    }]
  });
  const session = response.owned_sessions.find((candidate) => candidate.session_id === sessionId);
  assert.ok(session, 'Heartbeat must return the owned Session');
  assert.equal(session.ownership_generation, generation);
  assert.equal(session.lifecycle_status, 'saving');
  return session;
}

async function beginCheckpoint(runtime, sessionId, generation, reason = 'idle') {
  const { data } = await runtimeJson(
    runtime.client,
    runtime.credential,
    `/api/runtime/sessions/${sessionId}/checkpoint/begin`,
    { ownership_generation: generation, reason }
  );
  assertUuid(data.checkpoint_attempt_id, 'Checkpoint attempt id');
  assert.ok(Number.isInteger(data.history_checkpoint) && data.history_checkpoint >= 0);
  assert.ok(Number.isInteger(data.bundle_generation) && data.bundle_generation > 0);
  assert.equal(data.reason, reason);
  return data;
}

function bundleMetadata(attempt, ownershipGeneration, producingCodexVersion, createdAt) {
  return {
    ownershipGeneration,
    checkpointAttemptId: attempt.checkpoint_attempt_id,
    bundleGeneration: attempt.bundle_generation,
    checksum: BUNDLE_SHA256,
    size: BUNDLE_BYTES.length,
    historyCheckpoint: attempt.history_checkpoint,
    producingCodexVersion,
    createdAt
  };
}

function bundleUploadHeaders(credential, metadata, overrides = {}) {
  return runtimeHeaders(credential, {
    'content-type': 'application/zstd',
    'content-length': String(BUNDLE_BYTES.length),
    'x-agent-hub-ownership-generation': String(metadata.ownershipGeneration),
    'x-agent-hub-checkpoint-attempt-id': metadata.checkpointAttemptId,
    'x-agent-hub-bundle-generation': String(metadata.bundleGeneration),
    'x-agent-hub-bundle-sha256': metadata.checksum,
    'x-agent-hub-bundle-size': String(metadata.size),
    'x-agent-hub-history-checkpoint': String(metadata.historyCheckpoint),
    'x-agent-hub-producing-codex-version': metadata.producingCodexVersion,
    'x-agent-hub-bundle-created-at': metadata.createdAt,
    ...overrides
  });
}

async function uploadBundle(context, runtime, sessionId, metadata, {
  expectedStatus = 200,
  headerOverrides = {}
} = {}) {
  const response = await fetch(new URL(`/api/runtime/sessions/${sessionId}/bundle`, context.baseURL), {
    method: 'PUT',
    headers: bundleUploadHeaders(runtime.credential, metadata, headerOverrides),
    body: BUNDLE_BYTES
  });
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  assert.equal(response.status, expectedStatus, `Bundle upload returned ${response.status}`);
  return data;
}

async function downloadBundle(context, runtime, sessionId, generation) {
  const response = await fetch(new URL(`/api/runtime/sessions/${sessionId}/bundle`, context.baseURL), {
    headers: runtimeHeaders(runtime.credential, {
      accept: 'application/zstd',
      'x-agent-hub-ownership-generation': String(generation)
    })
  });
  assert.equal(response.status, 200);
  const bytes = Buffer.from(await response.arrayBuffer());
  return { response, bytes };
}

function assertCurrentBundle(actual, expected, label) {
  assert.deepEqual(actual.current_bundle, expected, `${label} must preserve the current Bundle`);
}

async function acknowledgeCleanup(runtime, sessionId, generation) {
  const notice = await heartbeat(runtime);
  const pendingCleanup = notice.cleanup_sessions ?? [];
  assert.ok(pendingCleanup.some((cleanup) => cleanup.session_id === sessionId
    && cleanup.ownership_generation === generation));
  const acknowledged = await heartbeat(runtime, {
    cleanedSessions: [{ session_id: sessionId, ownership_generation: generation }]
  });
  const remainingCleanup = acknowledged.cleanup_sessions ?? [];
  assert.equal(remainingCleanup.some((cleanup) => cleanup.session_id === sessionId
    && cleanup.ownership_generation === generation), false);
}

async function forceDeleteIfPresent(admin, runtime) {
  const { data: runtimes } = await admin.get('/api/runtimes');
  if (!runtimes.some((candidate) => candidate.id === runtime.id)) return;
  await admin.post(`/api/admin/runtimes/${runtime.id}/force-delete`, {
    hostname: runtime.hostname
  }, { expectedStatus: [200, 404] });
  runtime.deleted = true;
}

export default async function bundleRecoveryApiScenario(context) {
  const admin = new ApiClient(context.baseURL);
  await loginAsAdmin(admin);
  const { data: initialRuntimes } = await admin.get('/api/runtimes');
  const composeRuntimeIds = new Set(initialRuntimes.map((runtime) => runtime.id));
  assert.ok(initialRuntimes.some((runtime) => runtime.status === 'online'));
  const { data: rollout } = await admin.get('/api/admin/codex-version-rollout');
  const codexVersion = rollout.active_version
    ?? initialRuntimes.find((runtime) => runtime.status === 'online')?.codex_version;
  assert.equal(typeof codexVersion, 'string');
  assert.ok(codexVersion.length > 0);

  const runtimes = [];
  const agentIds = [];
  const cleanupErrors = [];
  let scenarioError = null;

  try {
    const firstRuntime = await registerRuntime(
      admin,
      context,
      codexVersion,
      'qa-bundle-runtime-one'
    );
    runtimes.push(firstRuntime);
    const bundleAgent = await createBoundAgent(
      admin,
      context,
      firstRuntime.id,
      'QA Bundle Recovery Agent'
    );
    agentIds.push(bundleAgent.id);

    const firstThreadId = uniqueSlug(context, 'qa-native-thread');
    const { data: firstRun } = await admin.post(`/api/agents/${bundleAgent.id}/runs`, {
      message: context.unique('QA initial Bundle Turn'),
      hub_session_id: null,
      parent_run_id: null
    });
    const firstOwnership = await claimRun(firstRuntime, firstRun.id);
    const sessionId = firstOwnership.claim.run.hub_session_id;
    assertUuid(sessionId, 'Hub Session id');
    assert.equal(firstOwnership.generation, 1);
    assert.equal(firstOwnership.claim.session_context.session.current_bundle, null);
    await beginAndCompleteTurn(
      firstRuntime,
      firstOwnership.claim,
      firstOwnership.generation,
      firstThreadId,
      uniqueSlug(context, 'qa-native-turn-one')
    );

    const { data: onlineSession } = await admin.get(`/api/sessions/${sessionId}`);
    assert.equal(onlineSession.lifecycle_status, 'online');
    assert.equal(onlineSession.native_thread_id, firstThreadId);
    assert.equal(onlineSession.active_turn_id, null);
    assert.equal(onlineSession.runtime_owner_id, firstRuntime.id);
    assert.equal(onlineSession.ownership_generation, firstOwnership.generation);
    assert.equal(onlineSession.current_bundle, null);

    await runtimeJson(
      firstRuntime.client,
      firstRuntime.credential,
      `/api/runtime/sessions/${sessionId}/checkpoint/begin`,
      { ownership_generation: firstOwnership.generation + 1, reason: 'idle' },
      403
    );

    await enterSaving(firstRuntime, sessionId, firstOwnership.generation);
    const firstAttemptStartedAt = Date.now();
    const firstAttempt = await beginCheckpoint(
      firstRuntime,
      sessionId,
      firstOwnership.generation
    );
    assert.equal(firstAttempt.bundle_generation, 1);
    assert.equal(firstAttempt.history_checkpoint, onlineSession.history_checkpoint);

    const { data: queued } = await admin.post(`/api/sessions/${sessionId}/messages`, {
      content: context.unique('QA queued recovery Turn'),
      client_message_key: context.unique('qa-bundle-queued-message')
    });
    assert.ok(queued.run);
    assert.equal(queued.run.status, 'pending');
    assert.equal(queued.message.delivery_state, 'queued');
    const { data: savingWithQueuedWork } = await admin.get(`/api/sessions/${sessionId}`);
    assert.equal(savingWithQueuedWork.lifecycle_status, 'saving');
    assert.ok(savingWithQueuedWork.history_checkpoint > firstAttempt.history_checkpoint);

    const replayedBegin = await beginCheckpoint(
      firstRuntime,
      sessionId,
      firstOwnership.generation
    );
    assert.deepEqual(replayedBegin, firstAttempt, 'checkpoint/begin must preserve the frozen attempt');

    const firstCreatedAt = new Date().toISOString();
    const firstMetadata = bundleMetadata(
      firstAttempt,
      firstOwnership.generation,
      codexVersion,
      firstCreatedAt
    );
    const firstCommit = await uploadBundle(context, firstRuntime, sessionId, firstMetadata);
    const firstCommitFinishedAt = Date.now();
    assert.deepEqual(firstCommit, {
      checkpoint_attempt_id: firstAttempt.checkpoint_attempt_id,
      bundle_generation: 1,
      has_queued_work: true,
      ownership_released: false
    });

    const { data: firstPointerSession } = await admin.get(`/api/sessions/${sessionId}`);
    const firstPointer = structuredClone(firstPointerSession.current_bundle);
    assert.equal(firstPointer.generation, 1);
    assert.equal(
      firstPointer.object_key,
      `sessions/${sessionId}/bundle-1-${firstAttempt.checkpoint_attempt_id}.tar.zst`
    );
    assert.equal(firstPointer.checksum_sha256, BUNDLE_SHA256);
    assert.equal(firstPointer.size_bytes, BUNDLE_BYTES.length);
    assert.equal(firstPointer.history_checkpoint, firstAttempt.history_checkpoint);
    assert.equal(firstPointer.ownership_generation, firstOwnership.generation);
    assert.equal(firstPointer.producing_codex_version, codexVersion);
    assert.ok(Date.parse(firstPointer.created_at) >= firstAttemptStartedAt);
    assert.ok(Date.parse(firstPointer.created_at) <= firstCommitFinishedAt);
    assert.equal(Date.parse(firstPointer.created_at), Date.parse(firstCreatedAt));
    assert.equal(firstPointerSession.lifecycle_status, 'online');
    assert.equal(firstPointerSession.runtime_owner_id, firstRuntime.id);

    const firstReplay = await uploadBundle(context, firstRuntime, sessionId, firstMetadata);
    assert.deepEqual(firstReplay, firstCommit, 'identical attempt replay must be idempotent');
    await uploadBundle(context, firstRuntime, sessionId, {
      ...firstMetadata,
      producingCodexVersion: `${codexVersion}-conflict`
    }, { expectedStatus: 409 });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'conflicting replay'
    );

    await enterSaving(firstRuntime, sessionId, firstOwnership.generation);
    const secondAttemptStartedAt = Date.now();
    const secondAttempt = await beginCheckpoint(
      firstRuntime,
      sessionId,
      firstOwnership.generation
    );
    assert.notEqual(secondAttempt.checkpoint_attempt_id, firstAttempt.checkpoint_attempt_id);
    assert.equal(secondAttempt.bundle_generation, 2);
    assert.equal(secondAttempt.history_checkpoint, savingWithQueuedWork.history_checkpoint);
    const secondCreatedAt = new Date().toISOString();
    const secondMetadata = bundleMetadata(
      secondAttempt,
      firstOwnership.generation,
      codexVersion,
      secondCreatedAt
    );

    await uploadBundle(context, firstRuntime, sessionId, {
      ...secondMetadata,
      checksum: WRONG_BUNDLE_SHA256
    }, { expectedStatus: 502 });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'checksum-failed upload'
    );

    await uploadBundle(context, firstRuntime, sessionId, secondMetadata, {
      expectedStatus: 400,
      headerOverrides: { 'x-agent-hub-bundle-size': String(BUNDLE_BYTES.length + 1) }
    });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'size-rejected upload'
    );

    await uploadBundle(context, firstRuntime, sessionId, {
      ...secondMetadata,
      checkpointAttemptId: randomUUID()
    }, { expectedStatus: 409 });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'wrong-attempt upload'
    );

    await uploadBundle(context, firstRuntime, sessionId, {
      ...secondMetadata,
      bundleGeneration: secondMetadata.bundleGeneration + 1
    }, { expectedStatus: 409 });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'wrong Bundle generation upload'
    );

    await uploadBundle(context, firstRuntime, sessionId, {
      ...secondMetadata,
      ownershipGeneration: secondMetadata.ownershipGeneration + 1
    }, { expectedStatus: 409 });
    assertCurrentBundle(
      (await admin.get(`/api/sessions/${sessionId}`)).data,
      firstPointer,
      'wrong ownership generation upload'
    );

    const secondCommit = await uploadBundle(context, firstRuntime, sessionId, secondMetadata);
    const secondCommitFinishedAt = Date.now();
    assert.deepEqual(secondCommit, {
      checkpoint_attempt_id: secondAttempt.checkpoint_attempt_id,
      bundle_generation: 2,
      has_queued_work: true,
      ownership_released: false
    });
    const { data: secondPointerSession } = await admin.get(`/api/sessions/${sessionId}`);
    const secondPointer = structuredClone(secondPointerSession.current_bundle);
    assert.equal(secondPointer.generation, 2);
    assert.notEqual(secondPointer.object_key, firstPointer.object_key);
    assert.equal(
      secondPointer.object_key,
      `sessions/${sessionId}/bundle-2-${secondAttempt.checkpoint_attempt_id}.tar.zst`
    );
    assert.equal(secondPointer.checksum_sha256, BUNDLE_SHA256);
    assert.equal(secondPointer.size_bytes, BUNDLE_BYTES.length);
    assert.equal(secondPointer.history_checkpoint, secondAttempt.history_checkpoint);
    assert.equal(secondPointer.ownership_generation, firstOwnership.generation);
    assert.equal(secondPointer.producing_codex_version, codexVersion);
    assert.ok(Date.parse(secondPointer.created_at) >= secondAttemptStartedAt);
    assert.ok(Date.parse(secondPointer.created_at) <= secondCommitFinishedAt);
    assert.equal(Date.parse(secondPointer.created_at), Date.parse(secondCreatedAt));

    const { data: released } = await runtimeJson(
      firstRuntime.client,
      firstRuntime.credential,
      `/api/runtime/sessions/${sessionId}/release`,
      { ownership_generation: firstOwnership.generation }
    );
    assert.equal(released.runtime_owner_id, null);
    assert.equal(released.ownership_generation, firstOwnership.generation);
    assert.equal(released.lifecycle_status, 'waiting_for_runtime');
    assert.deepEqual(released.current_bundle, secondPointer);
    await acknowledgeCleanup(firstRuntime, sessionId, firstOwnership.generation);

    const { data: drainedFirst } = await admin.post(
      `/api/admin/runtimes/${firstRuntime.id}/drain`,
      { hostname: firstRuntime.hostname }
    );
    assert.equal(drainedFirst.runtime.status, 'draining');
    assert.deepEqual(drainedFirst.owned_sessions, []);
    await admin.delete(`/api/admin/runtimes/${firstRuntime.id}`, {
      body: { hostname: firstRuntime.hostname },
      expectedStatus: 204
    });
    firstRuntime.deleted = true;

    const secondRuntime = await registerRuntime(
      admin,
      context,
      codexVersion,
      'qa-bundle-runtime-two'
    );
    runtimes.push(secondRuntime);
    const reboundAgent = await bindAgentToRuntime(admin, bundleAgent, secondRuntime.id);
    assert.equal(reboundAgent.runtime_id, secondRuntime.id);

    const restoredOwnership = await claimRun(secondRuntime, queued.run.id);
    assert.equal(restoredOwnership.generation, firstOwnership.generation + 1);
    assert.equal(restoredOwnership.claim.resume.thread_id, firstThreadId);
    assert.equal(restoredOwnership.claim.session_context.session.native_thread_id, firstThreadId);
    assert.deepEqual(restoredOwnership.claim.session_context.session.current_bundle, secondPointer);

    const downloaded = await downloadBundle(
      context,
      secondRuntime,
      sessionId,
      restoredOwnership.generation
    );
    assert.deepEqual(downloaded.bytes, BUNDLE_BYTES);
    assert.match(downloaded.response.headers.get('content-type') ?? '', /^application\/zstd\b/);
    assert.equal(downloaded.response.headers.get('content-length'), String(BUNDLE_BYTES.length));
    assert.equal(
      downloaded.response.headers.get('x-agent-hub-bundle-generation'),
      String(secondPointer.generation)
    );
    assert.equal(downloaded.response.headers.get('x-agent-hub-bundle-sha256'), BUNDLE_SHA256);
    assert.equal(
      downloaded.response.headers.get('x-agent-hub-history-checkpoint'),
      String(secondMetadata.historyCheckpoint)
    );
    assert.equal(
      downloaded.response.headers.get('x-agent-hub-producing-codex-version'),
      codexVersion
    );
    assert.equal(
      Date.parse(downloaded.response.headers.get('x-agent-hub-bundle-created-at')),
      Date.parse(secondMetadata.createdAt)
    );

    await beginAndCompleteTurn(
      secondRuntime,
      restoredOwnership.claim,
      restoredOwnership.generation,
      firstThreadId,
      uniqueSlug(context, 'qa-native-turn-two')
    );
    const { data: restoredSession } = await admin.get(`/api/sessions/${sessionId}`);
    assert.equal(restoredSession.lifecycle_status, 'online');
    assert.equal(restoredSession.native_thread_id, firstThreadId);
    assert.equal(restoredSession.ownership_generation, restoredOwnership.generation);
    assert.equal(restoredSession.runtime_owner_id, secondRuntime.id);
    assert.deepEqual(restoredSession.current_bundle, secondPointer);

    await enterSaving(secondRuntime, sessionId, restoredOwnership.generation);
    const cleanupAttempt = await beginCheckpoint(
      secondRuntime,
      sessionId,
      restoredOwnership.generation
    );
    assert.equal(cleanupAttempt.bundle_generation, 3);
    const cleanupCommit = await uploadBundle(
      context,
      secondRuntime,
      sessionId,
      bundleMetadata(
        cleanupAttempt,
        restoredOwnership.generation,
        codexVersion,
        new Date().toISOString()
      )
    );
    assert.equal(cleanupCommit.ownership_released, true);
    assert.equal(cleanupCommit.has_queued_work, false);
    await acknowledgeCleanup(secondRuntime, sessionId, restoredOwnership.generation);

    const unrecoverableAgent = await createBoundAgent(
      admin,
      context,
      secondRuntime.id,
      'QA Unrecoverable Session Agent'
    );
    agentIds.push(unrecoverableAgent.id);
    const unrecoverableThreadId = uniqueSlug(context, 'qa-unrecoverable-thread');
    const { data: unrecoverableRun } = await admin.post(
      `/api/agents/${unrecoverableAgent.id}/runs`,
      {
        message: context.unique('QA uncheckpointed Turn'),
        hub_session_id: null,
        parent_run_id: null
      }
    );
    const unrecoverableOwnership = await claimRun(secondRuntime, unrecoverableRun.id);
    const unrecoverableSessionId = unrecoverableOwnership.claim.run.hub_session_id;
    await beginAndCompleteTurn(
      secondRuntime,
      unrecoverableOwnership.claim,
      unrecoverableOwnership.generation,
      unrecoverableThreadId,
      uniqueSlug(context, 'qa-unrecoverable-turn')
    );
    const { data: noBundleSession } = await admin.get(`/api/sessions/${unrecoverableSessionId}`);
    assert.equal(noBundleSession.current_bundle, null);
    assert.equal(noBundleSession.lifecycle_status, 'online');

    await enterSaving(secondRuntime, unrecoverableSessionId, unrecoverableOwnership.generation);
    const failedAttempt = await beginCheckpoint(
      secondRuntime,
      unrecoverableSessionId,
      unrecoverableOwnership.generation
    );
    const { data: drainingSecond } = await admin.post(
      `/api/admin/runtimes/${secondRuntime.id}/drain`,
      { hostname: secondRuntime.hostname }
    );
    assert.equal(drainingSecond.runtime.status, 'draining');
    assert.equal(
      drainingSecond.owned_sessions.find((session) => session.id === unrecoverableSessionId)
        ?.lifecycle_status,
      'saving'
    );
    const pointerBeforeFailure = structuredClone(
      (await admin.get(`/api/sessions/${unrecoverableSessionId}`)).data.current_bundle
    );

    const emptyError = await runtimeJson(
      secondRuntime.client,
      secondRuntime.credential,
      `/api/runtime/sessions/${unrecoverableSessionId}/checkpoint/fail`,
      {
        ownership_generation: unrecoverableOwnership.generation,
        checkpoint_attempt_id: failedAttempt.checkpoint_attempt_id,
        error: '   '
      },
      400
    );
    assert.deepEqual(emptyError.data, {
      error: 'Session checkpoint failure requires an error code'
    });

    const failCheckpoint = async () => runtimeJson(
      secondRuntime.client,
      secondRuntime.credential,
      `/api/runtime/sessions/${unrecoverableSessionId}/checkpoint/fail`,
      {
        ownership_generation: unrecoverableOwnership.generation,
        checkpoint_attempt_id: failedAttempt.checkpoint_attempt_id,
        error: CHECKPOINT_FAILURE_CODE
      }
    );
    const { data: failedDisposition } = await failCheckpoint();
    assert.deepEqual(failedDisposition, {
      checkpoint_attempt_id: failedAttempt.checkpoint_attempt_id,
      disposition: 'retry',
      has_queued_work: false
    });
    assert.deepEqual((await failCheckpoint()).data, failedDisposition);
    const checkpointFailureView = (
      await admin.get(`/api/sessions/${unrecoverableSessionId}`)
    ).data;
    assert.equal(checkpointFailureView.lifecycle_status, 'saving');
    assert.deepEqual(checkpointFailureView.current_bundle, pointerBeforeFailure);

    const { data: forceDeleted } = await admin.post(
      `/api/admin/runtimes/${secondRuntime.id}/force-delete`,
      { hostname: secondRuntime.hostname }
    );
    secondRuntime.deleted = true;
    assert.equal(forceDeleted.runtime_id, secondRuntime.id);
    assert.deepEqual(forceDeleted.recoverable_session_ids, []);
    assert.deepEqual(forceDeleted.recovery_failed_session_ids, [unrecoverableSessionId]);

    const { data: recoveryFailed } = await admin.get(`/api/sessions/${unrecoverableSessionId}`);
    assert.equal(recoveryFailed.lifecycle_status, 'recovery_failed');
    assert.equal(
      recoveryFailed.recovery_error,
      'Runtime was force deleted without a restorable current Session Bundle'
    );
    assert.equal(recoveryFailed.runtime_owner_id, null);
    assert.equal(
      recoveryFailed.ownership_generation,
      unrecoverableOwnership.generation + 1
    );
    assert.equal(recoveryFailed.current_bundle, null);
    assert.equal(recoveryFailed.native_thread_id, unrecoverableThreadId);
    const rejectedMessage = await admin.post(`/api/sessions/${unrecoverableSessionId}/messages`, {
      content: context.unique('QA rejected recovery-failed continuation'),
      client_message_key: context.unique('qa-recovery-failed-message')
    }, { expectedStatus: 409 });
    assert.deepEqual(rejectedMessage.data, { error: 'session is read-only' });

    const { data: finalRuntimes } = await admin.get('/api/runtimes');
    assert.ok(finalRuntimes.some((runtime) => runtime.status === 'online'
      && composeRuntimeIds.has(runtime.id)), 'Compose Runtime must remain online');
    assert.equal(finalRuntimes.some((runtime) => runtime.id === firstRuntime.id), false);
    assert.equal(finalRuntimes.some((runtime) => runtime.id === secondRuntime.id), false);
  } catch (error) {
    scenarioError = error;
  } finally {
    for (const agentId of agentIds.reverse()) {
      try {
        await admin.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    for (const runtime of runtimes.reverse()) {
      try {
        await forceDeleteIfPresent(admin, runtime);
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
  }

  if (scenarioError) {
    if (cleanupErrors.length > 0) scenarioError.cleanupErrors = cleanupErrors;
    throw scenarioError;
  }
  if (cleanupErrors.length > 0) {
    throw new AggregateError(cleanupErrors, 'Bundle recovery scenario cleanup failed');
  }
}
