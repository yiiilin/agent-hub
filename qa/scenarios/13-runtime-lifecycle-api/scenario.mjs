import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { ApiClient, loginAsAdmin } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function uniqueSlug(context, prefix) {
  return context.unique(prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '');
}

function bearer(secret) {
  return { authorization: `Bearer ${secret}` };
}

function assertSecretPrefix(secret, prefix, label) {
  assert.equal(
    typeof secret === 'string' && secret.startsWith(prefix) && secret.length > prefix.length,
    true,
    `${label} must use the expected opaque prefix`
  );
}

function assertStrictlyIncreasing(events, label) {
  assert.ok(events.length > 0, `${label} must not be empty`);
  for (let index = 0; index < events.length; index += 1) {
    assert.equal(Number.isInteger(events[index].seq), true, `${label} seq must be an integer`);
    if (index > 0) {
      assert.ok(events[index].seq > events[index - 1].seq, `${label} seq must increase`);
    }
  }
}

function updateAgentPayload(agent, overrides = {}) {
  return {
    name: agent.name,
    instructions: agent.instructions,
    visibility: agent.visibility,
    public_to: agent.public_to,
    runtime_id: agent.runtime_id,
    default_model_connection_id: agent.default_model_connection_id,
    reasoning_effort: agent.reasoning_effort,
    codex_subagents: agent.codex_subagents,
    sandbox_policy: agent.sandbox_policy,
    managed_skill_ids: agent.managed_skill_ids,
    mcp_allowlist: agent.mcp_allowlist,
    ...overrides
  };
}

async function createEnrollment(adminClient, enrollments) {
  const { data } = await adminClient.post('/api/admin/runtime-enrollment-tokens');
  const record = {
    id: data.enrollment.id,
    token: data.token,
    state: 'unused'
  };
  enrollments.push(record);
  assertUuid(record.id, 'Enrollment id');
  assertSecretPrefix(record.token, 'ahre_', 'Enrollment token');
  assert.equal(Object.hasOwn(data.enrollment, 'token'), false);
  assert.equal(Object.hasOwn(data.enrollment, 'token_hash'), false);
  return record;
}

async function revokeEnrollment(adminClient, enrollment) {
  const { data } = await adminClient.post(
    `/api/admin/runtime-enrollment-tokens/${enrollment.id}/revoke`
  );
  enrollment.state = 'revoked';
  assert.equal(data.id, enrollment.id);
  assert.equal(typeof data.revoked_at, 'string');
  assert.equal(Object.hasOwn(data, 'token'), false);
  assert.equal(Object.hasOwn(data, 'token_hash'), false);
}

async function registerRuntime(runtimeClient, enrollment, template, hostname, label) {
  const { data } = await runtimeClient.post('/api/runtime/register', {
    hostname,
    labels: ['qa', label],
    codex_version: template.codex_version,
    capabilities: structuredClone(template.capabilities),
    sandbox_mode: template.sandbox_mode
  }, {
    headers: bearer(enrollment.token)
  });
  enrollment.state = 'consumed';
  assertUuid(data.runtime_id, `${label} Runtime id`);
  assertSecretPrefix(data.runtime_credential, 'ahrc_', `${label} Runtime credential`);
  return {
    id: data.runtime_id,
    hostname,
    credential: data.runtime_credential,
    deleted: false
  };
}

async function expectRegistrationRejected(runtimeClient, token, template, context, label) {
  const response = await runtimeClient.post('/api/runtime/register', {
    hostname: uniqueSlug(context, `qa-rejected-${label}`),
    labels: ['qa', 'rejected'],
    codex_version: template.codex_version,
    capabilities: structuredClone(template.capabilities),
    sandbox_mode: template.sandbox_mode
  }, {
    headers: bearer(token),
    expectedStatus: 401
  });
  assert.equal(response.status, 401);
}

async function heartbeat(runtimeClient, credential, body = {}, expectedStatus) {
  return runtimeClient.post('/api/runtime/heartbeat', body, {
    headers: bearer(credential),
    expectedStatus
  });
}

async function claim(runtimeClient, credential, expectedStatus = 200) {
  return runtimeClient.post('/api/runtime/runs/claim', {
    available_new_session_slots: 1,
    ready_owned_sessions: []
  }, {
    headers: bearer(credential),
    expectedStatus
  });
}

async function driveClaimedRun({
  adminClient,
  runtimeClient,
  credential,
  runtimeId,
  claimData,
  expectedRun,
  context,
  assertFencing
}) {
  assert.equal(claimData.run.id, expectedRun.id);
  assert.equal(claimData.run.runtime_id, runtimeId);
  assert.equal(claimData.run.status, 'running');
  assert.equal(claimData.agent.id, expectedRun.agent_id);
  assert.equal(claimData.agent.runtime_id, runtimeId);
  assert.equal(Boolean(claimData.session_context), true, 'Claim must include Session context');

  const generation = claimData.run.session_ownership_generation;
  assert.equal(Number.isInteger(generation) && generation > 0, true);
  assert.equal(claimData.session_context.session.id, expectedRun.hub_session_id);
  assert.equal(claimData.session_context.session.runtime_owner_id, runtimeId);
  assert.equal(claimData.session_context.session.ownership_generation, generation);
  assert.equal(claimData.session_context.turn.id, expectedRun.hub_turn_id);
  assert.equal(claimData.session_context.turn.ownership_generation, generation);

  const beginBody = {
    ownership_generation: generation,
    payload: {
      configuration_fingerprint: claimData.expected_configuration_fingerprint
    }
  };
  if (assertFencing) {
    const rejected = await runtimeClient.post(
      `/api/runtime/runs/${expectedRun.id}/turn/begin`,
      { ...beginBody, ownership_generation: generation + 1 },
      { headers: bearer(credential), expectedStatus: [403, 409] }
    );
    assert.equal([403, 409].includes(rejected.status), true);
  }

  const { data: begun } = await runtimeClient.post(
    `/api/runtime/runs/${expectedRun.id}/turn/begin`,
    beginBody,
    { headers: bearer(credential) }
  );
  assert.equal(begun.session_id, expectedRun.hub_session_id);
  assert.equal(begun.turn_id, expectedRun.hub_turn_id);
  assert.equal(begun.ownership_generation, generation);
  assert.equal(begun.configuration_fingerprint, claimData.expected_configuration_fingerprint);
  assert.deepEqual(begun.messages.map((message) => message.content), [expectedRun.initial_message]);

  const fencedContent = uniqueSlug(context, 'qa-fenced-event');
  if (assertFencing) {
    const rejected = await runtimeClient.post(
      `/api/runtime/runs/${expectedRun.id}/events`,
      {
        ownership_generation: generation + 1,
        payload: {
          event_type: 'message',
          role: 'assistant',
          content: fencedContent,
          payload: { source: 'qa' }
        }
      },
      { headers: bearer(credential), expectedStatus: [403, 409] }
    );
    assert.equal([403, 409].includes(rejected.status), true);
  }

  const nativeThreadId = uniqueSlug(context, 'qa-native-thread');
  const nativeTurnId = uniqueSlug(context, 'qa-native-turn');
  const { data: turnStarted } = await runtimeClient.post(
    `/api/runtime/runs/${expectedRun.id}/events`,
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
    },
    { headers: bearer(credential) }
  );
  assert.equal(turnStarted.event_type, 'turn_started');
  assert.equal(turnStarted.payload.native_thread_id, nativeThreadId);
  assert.equal(turnStarted.payload.native_turn_id, nativeTurnId);

  const output = uniqueSlug(context, 'qa-runtime-output');
  const { data: messageEvent } = await runtimeClient.post(
    `/api/runtime/runs/${expectedRun.id}/events`,
    {
      ownership_generation: generation,
      payload: {
        event_type: 'message',
        role: 'assistant',
        content: output,
        payload: { source: 'qa-runtime-lifecycle' }
      }
    },
    { headers: bearer(credential) }
  );
  assert.equal(messageEvent.event_type, 'message');
  assert.equal(messageEvent.role, 'assistant');
  assert.equal(messageEvent.content, output);

  const completionBody = {
    ownership_generation: generation,
    payload: {
      status: 'completed',
      session_id: nativeThreadId,
      work_dir_ref: `qa/${uniqueSlug(context, 'runtime-work')}`
    }
  };
  if (assertFencing) {
    const rejected = await runtimeClient.post(
      `/api/runtime/runs/${expectedRun.id}/complete`,
      { ...completionBody, ownership_generation: generation + 1 },
      { headers: bearer(credential), expectedStatus: [403, 409] }
    );
    assert.equal([403, 409].includes(rejected.status), true);
  }

  const { data: completed } = await runtimeClient.post(
    `/api/runtime/runs/${expectedRun.id}/complete`,
    completionBody,
    { headers: bearer(credential) }
  );
  assert.equal(completed.status, 'completed');
  assert.equal(completed.runtime_id, runtimeId);
  assert.equal(completed.session_ownership_generation, generation);
  assert.equal(completed.session_id, nativeThreadId);
  assert.equal(completed.work_dir_ref, completionBody.payload.work_dir_ref);

  const { data: publicRun } = await adminClient.get(`/api/runs/${expectedRun.id}`);
  assert.equal(publicRun.status, 'completed');
  assert.equal(publicRun.runtime_id, runtimeId);
  assert.equal(publicRun.session_ownership_generation, generation);

  const { data: publicEvents } = await adminClient.get(`/api/runs/${expectedRun.id}/events`);
  assertStrictlyIncreasing(publicEvents, 'Public Run events');
  const publicTurnStarted = publicEvents.find((event) => event.event_type === 'turn_started');
  assert.equal(Boolean(publicTurnStarted), true, 'Public events must expose turn_started');
  assert.equal(publicTurnStarted.payload.native_thread_id, nativeThreadId);
  assert.equal(publicTurnStarted.payload.native_turn_id, nativeTurnId);
  assert.equal(
    publicEvents.some((event) => event.event_type === 'message'
      && event.role === 'assistant'
      && event.content === output),
    true,
    'Public events must expose the Runtime message'
  );
  assert.equal(publicEvents.some((event) => event.content === fencedContent), false);
  assert.equal(
    publicEvents.some((event) => event.event_type === 'status'
      && (event.content === 'completed' || event.payload?.status === 'completed')),
    true,
    'Public events must expose terminal completion'
  );

  const { data: publicSession } = await adminClient.get(
    `/api/sessions/${expectedRun.hub_session_id}`
  );
  assert.equal(publicSession.lifecycle_status, 'online');
  assert.equal(publicSession.native_thread_id, nativeThreadId);
  assert.equal(publicSession.runtime_owner_id, runtimeId);
  assert.equal(publicSession.ownership_generation, generation);
  assert.equal(publicSession.active_turn_id, null);
  return { generation, nativeThreadId };
}

async function cleanupResource(action, errors) {
  try {
    await action();
  } catch (error) {
    errors.push(error);
  }
}

export default async function runtimeLifecycleApiScenario(context) {
  const adminClient = new ApiClient(context.baseURL);
  const runtimeClient = new ApiClient(context.baseURL);
  await loginAsAdmin(adminClient);

  const { data: baselineRuntimes } = await adminClient.get('/api/runtimes');
  const template = baselineRuntimes.find((runtime) => runtime.status === 'online');
  assert.equal(Boolean(template), true, 'Compose must provide an online Runtime template');
  const baselineRuntimeIds = new Set(baselineRuntimes.map((runtime) => runtime.id));

  const enrollments = [];
  const runtimes = [];
  let agentId = null;
  let agentDeleted = false;
  let scenarioError = null;
  const cleanupErrors = [];

  try {
    const revokedEnrollment = await createEnrollment(adminClient, enrollments);
    await revokeEnrollment(adminClient, revokedEnrollment);
    await expectRegistrationRejected(
      runtimeClient,
      revokedEnrollment.token,
      template,
      context,
      'revoked'
    );

    const primaryEnrollment = await createEnrollment(adminClient, enrollments);
    const primary = await registerRuntime(
      runtimeClient,
      primaryEnrollment,
      template,
      uniqueSlug(context, 'qa-runtime-primary'),
      'primary'
    );
    runtimes.push(primary);
    await expectRegistrationRejected(
      runtimeClient,
      primaryEnrollment.token,
      template,
      context,
      'consumed'
    );
    const invalidEnrollmentToken = `ahre_${randomUUID().replaceAll('-', '')}`;
    await expectRegistrationRejected(
      runtimeClient,
      invalidEnrollmentToken,
      template,
      context,
      'invalid'
    );

    const { data: listedEnrollments } = await adminClient.get(
      '/api/admin/runtime-enrollment-tokens'
    );
    for (const enrollment of enrollments) {
      assert.equal(JSON.stringify(listedEnrollments).includes(enrollment.token), false);
    }
    for (const listed of listedEnrollments) {
      assert.equal(Object.hasOwn(listed, 'token'), false);
      assert.equal(Object.hasOwn(listed, 'token_hash'), false);
    }
    const listedRevoked = listedEnrollments.find((item) => item.id === revokedEnrollment.id);
    assert.equal(typeof listedRevoked?.revoked_at, 'string');
    const listedConsumed = listedEnrollments.find((item) => item.id === primaryEnrollment.id);
    assert.equal(typeof listedConsumed?.consumed_at, 'string');
    assert.equal(listedConsumed?.consumed_by_runtime_id, primary.id);

    const { data: rotationRequested } = await adminClient.post(
      `/api/admin/runtimes/${primary.id}/credential-rotation`
    );
    assert.equal(rotationRequested.id, primary.id);
    assert.equal(typeof rotationRequested.credential_rotation_requested_at, 'string');
    const { data: observedRotation } = await heartbeat(runtimeClient, primary.credential);
    assert.equal(observedRotation.rotation_requested, true);
    assert.equal(observedRotation.pending_credential_accepted, false);

    const rotatedCredential = `ahrc_${randomUUID().replaceAll('-', '')}${randomUUID().replaceAll('-', '')}`;
    const rotatedHash = createHash('sha256').update(rotatedCredential).digest('hex');
    const { data: stagedRotation } = await heartbeat(runtimeClient, primary.credential, {
      pending_credential_hash: rotatedHash
    });
    assert.equal(stagedRotation.rotation_requested, true);
    assert.equal(stagedRotation.pending_credential_accepted, true);
    assert.equal(stagedRotation.credential_activated, false);
    const { data: activatedRotation } = await heartbeat(runtimeClient, rotatedCredential);
    assert.equal(activatedRotation.rotation_requested, false);
    assert.equal(activatedRotation.credential_activated, true);
    await heartbeat(runtimeClient, primary.credential, {}, 401);
    primary.credential = rotatedCredential;

    const ordinaryEnrollment = await createEnrollment(adminClient, enrollments);
    const ordinary = await registerRuntime(
      runtimeClient,
      ordinaryEnrollment,
      template,
      uniqueSlug(context, 'qa-runtime-ordinary-delete'),
      'ordinary-delete'
    );
    runtimes.push(ordinary);
    const { data: ordinaryHeartbeat } = await heartbeat(runtimeClient, ordinary.credential);
    assert.equal(ordinaryHeartbeat.runtime_status, 'online');
    assert.deepEqual(ordinaryHeartbeat.owned_sessions, []);
    assert.deepEqual(ordinaryHeartbeat.cleanup_sessions ?? [], []);

    const agentName = context.unique('QA Runtime Lifecycle Agent');
    const { data: createdAgent } = await adminClient.post('/api/agents', {
      name: agentName,
      instructions: 'Exercise generation-fenced Runtime HTTP lifecycle behavior.',
      visibility: 'private',
      public_to: []
    });
    agentId = createdAgent.id;
    const { data: agent } = await adminClient.request(`/api/agents/${agentId}`, {
      method: 'PATCH',
      body: updateAgentPayload(createdAgent, { runtime_id: primary.id })
    });
    assert.equal(agent.runtime_id, primary.id);

    const { data: firstRun } = await adminClient.post(`/api/agents/${agentId}/runs`, {
      message: context.unique('QA Runtime fenced Turn'),
      hub_session_id: null,
      parent_run_id: null
    });
    assert.equal(firstRun.status, 'pending');
    assertUuid(firstRun.hub_session_id, 'First Session id');

    const mismatchedClaim = await claim(runtimeClient, ordinary.credential, 204);
    assert.equal(mismatchedClaim.status, 204);
    assert.equal(mismatchedClaim.data, null);
    assert.equal((await adminClient.get(`/api/runs/${firstRun.id}`)).data.status, 'pending');

    const { data: firstClaim } = await claim(runtimeClient, primary.credential);
    const firstLifecycle = await driveClaimedRun({
      adminClient,
      runtimeClient,
      credential: primary.credential,
      runtimeId: primary.id,
      claimData: firstClaim,
      expectedRun: firstRun,
      context,
      assertFencing: true
    });

    const { data: secondRun } = await adminClient.post(`/api/agents/${agentId}/runs`, {
      message: context.unique('QA Runtime post-drain Turn'),
      hub_session_id: null,
      parent_run_id: null
    });
    assert.equal(secondRun.status, 'pending');
    assert.notEqual(secondRun.hub_session_id, firstRun.hub_session_id);

    const wrongHostnameDrain = await adminClient.post(
      `/api/admin/runtimes/${primary.id}/drain`,
      { hostname: `${primary.hostname}-wrong` },
      { expectedStatus: 409 }
    );
    assert.equal(wrongHostnameDrain.status, 409);
    const runtimeAfterRejectedDrain = (await adminClient.get('/api/runtimes')).data
      .find((runtime) => runtime.id === primary.id);
    assert.equal(runtimeAfterRejectedDrain?.status, 'online');

    const { data: drained } = await adminClient.post(
      `/api/admin/runtimes/${primary.id}/drain`,
      { hostname: primary.hostname }
    );
    assert.equal(drained.runtime.id, primary.id);
    assert.equal(drained.runtime.status, 'draining');
    assert.deepEqual(drained.owned_sessions.map((session) => session.id), [firstRun.hub_session_id]);
    assert.equal(drained.owned_sessions[0].lifecycle_status, 'saving');

    const drainingClaim = await claim(runtimeClient, primary.credential, 204);
    assert.equal(drainingClaim.status, 204);
    assert.equal((await adminClient.get(`/api/runs/${secondRun.id}`)).data.status, 'pending');

    const { data: cancelledDrain } = await adminClient.post(
      `/api/admin/runtimes/${primary.id}/cancel-drain`
    );
    assert.equal(cancelledDrain.runtime.status, 'online');
    assert.equal(
      cancelledDrain.owned_sessions.some((session) => session.id === firstRun.hub_session_id),
      true
    );
    const { data: postCancelHeartbeat } = await heartbeat(runtimeClient, primary.credential);
    assert.equal(postCancelHeartbeat.runtime_status, 'online');
    const { data: secondClaim } = await claim(runtimeClient, primary.credential);
    const secondLifecycle = await driveClaimedRun({
      adminClient,
      runtimeClient,
      credential: primary.credential,
      runtimeId: primary.id,
      claimData: secondClaim,
      expectedRun: secondRun,
      context,
      assertFencing: false
    });

    await heartbeat(runtimeClient, ordinary.credential);
    const ordinaryDeleteWhileOnline = await adminClient.request(
      `/api/admin/runtimes/${ordinary.id}`,
      {
        method: 'DELETE',
        body: { hostname: ordinary.hostname },
        expectedStatus: 409
      }
    );
    assert.equal(ordinaryDeleteWhileOnline.status, 409);
    const { data: ordinaryDrained } = await adminClient.post(
      `/api/admin/runtimes/${ordinary.id}/drain`,
      { hostname: ordinary.hostname }
    );
    assert.equal(ordinaryDrained.runtime.status, 'draining');
    assert.deepEqual(ordinaryDrained.owned_sessions, []);
    const { data: ordinaryDrainingHeartbeat } = await heartbeat(
      runtimeClient,
      ordinary.credential
    );
    assert.equal(ordinaryDrainingHeartbeat.runtime_status, 'draining');
    assert.deepEqual(ordinaryDrainingHeartbeat.owned_sessions, []);
    assert.deepEqual(ordinaryDrainingHeartbeat.cleanup_sessions ?? [], []);
    await adminClient.request(`/api/admin/runtimes/${ordinary.id}`, {
      method: 'DELETE',
      body: { hostname: ordinary.hostname },
      expectedStatus: 204
    });
    ordinary.deleted = true;
    await heartbeat(runtimeClient, ordinary.credential, {}, 401);

    const { data: forceDeleted } = await adminClient.post(
      `/api/admin/runtimes/${primary.id}/force-delete`,
      { hostname: primary.hostname }
    );
    primary.deleted = true;
    assert.equal(forceDeleted.runtime_id, primary.id);
    assert.deepEqual(forceDeleted.recoverable_session_ids, []);
    assert.deepEqual(
      [...forceDeleted.recovery_failed_session_ids].sort(),
      [firstRun.hub_session_id, secondRun.hub_session_id].sort()
    );
    await heartbeat(runtimeClient, primary.credential, {}, 401);

    const { data: firstRecoveryFailed } = await adminClient.get(
      `/api/sessions/${firstRun.hub_session_id}`
    );
    assert.equal(firstRecoveryFailed.lifecycle_status, 'recovery_failed');
    assert.equal(firstRecoveryFailed.runtime_owner_id, null);
    assert.equal(firstRecoveryFailed.ownership_generation, firstLifecycle.generation + 1);
    const { data: secondRecoveryFailed } = await adminClient.get(
      `/api/sessions/${secondRun.hub_session_id}`
    );
    assert.equal(secondRecoveryFailed.lifecycle_status, 'recovery_failed');
    assert.equal(secondRecoveryFailed.runtime_owner_id, null);
    assert.equal(secondRecoveryFailed.ownership_generation, secondLifecycle.generation + 1);

    const finalRuntimes = (await adminClient.get('/api/runtimes')).data;
    assert.equal(finalRuntimes.some((runtime) => runtime.id === primary.id), false);
    assert.equal(finalRuntimes.some((runtime) => runtime.id === ordinary.id), false);
    for (const baselineId of baselineRuntimeIds) {
      assert.equal(
        finalRuntimes.some((runtime) => runtime.id === baselineId),
        true,
        'Compose Runtime must remain present'
      );
    }
  } catch (error) {
    scenarioError = error;
  } finally {
    for (const runtime of runtimes) {
      if (!runtime.deleted) {
        await cleanupResource(async () => {
          await adminClient.post(
            `/api/admin/runtimes/${runtime.id}/force-delete`,
            { hostname: runtime.hostname },
            { expectedStatus: [200, 404] }
          );
          runtime.deleted = true;
        }, cleanupErrors);
      }
    }
    if (agentId && !agentDeleted) {
      await cleanupResource(async () => {
        await adminClient.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
        agentDeleted = true;
      }, cleanupErrors);
    }
    for (const enrollment of enrollments) {
      if (enrollment.state === 'unused') {
        await cleanupResource(async () => {
          await adminClient.post(
            `/api/admin/runtime-enrollment-tokens/${enrollment.id}/revoke`,
            undefined,
            { expectedStatus: [200, 409, 404] }
          );
          enrollment.state = 'revoked';
        }, cleanupErrors);
      }
    }
  }

  if (scenarioError) {
    if (cleanupErrors.length > 0) {
      scenarioError.message = `${scenarioError.message}; cleanup failed: ${cleanupErrors
        .map((error) => error.message)
        .join('; ')}`;
    }
    throw scenarioError;
  }
  if (cleanupErrors.length > 0) {
    throw new Error(`Runtime lifecycle cleanup failed: ${cleanupErrors
      .map((error) => error.message)
      .join('; ')}`);
  }
}
