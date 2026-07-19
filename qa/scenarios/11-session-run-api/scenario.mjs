import assert from 'node:assert/strict';
import { ApiClient, loginAsAdmin, poll, waitForRunStatus } from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HOLD_MARKER = 'fixture:hold';

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

function assertStrictlyIncreasing(items, label) {
  assert.ok(items.length > 0, `${label} must not be empty`);
  for (let index = 0; index < items.length; index += 1) {
    assert.ok(Number.isInteger(items[index].seq), `${label}[${index}].seq must be an integer`);
    if (index > 0) {
      assert.ok(
        items[index].seq > items[index - 1].seq,
        `${label} seq must be strictly increasing`
      );
    }
  }
}

function parseSseFrame(frame) {
  const parsed = { event: 'message', id: null, data: [] };
  for (const line of frame.split(/\r?\n/)) {
    if (line.startsWith('event:')) parsed.event = line.slice(6).trimStart();
    else if (line.startsWith('id:')) parsed.id = line.slice(3).trimStart();
    else if (line.startsWith('data:')) parsed.data.push(line.slice(5).trimStart());
  }
  return { event: parsed.event, id: parsed.id, data: parsed.data.join('\n') };
}

async function readSseThroughTurnStarted(client, baseURL, runId) {
  const controller = new AbortController();
  const response = await fetch(new URL(`/api/runs/${runId}/events/stream?after=0`, baseURL), {
    headers: {
      accept: 'text/event-stream',
      cookie: client.cookieHeader()
    },
    signal: controller.signal
  });
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type') ?? '', /^text\/event-stream\b/);
  assert.ok(response.body, 'SSE response must have a body');

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const events = [];
  let buffer = '';
  let sawTurnStarted = false;

  try {
    while (!sawTurnStarted) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let delimiter = buffer.match(/\r?\n\r?\n/);
      while (delimiter) {
        const frame = buffer.slice(0, delimiter.index);
        buffer = buffer.slice(delimiter.index + delimiter[0].length);
        const parsed = parseSseFrame(frame);
        if (parsed.event === 'run_event') {
          const event = JSON.parse(parsed.data);
          assert.equal(parsed.id, String(event.seq));
          events.push(event);
          sawTurnStarted ||= event.event_type === 'turn_started';
        }
        if (sawTurnStarted) break;
        delimiter = buffer.match(/\r?\n\r?\n/);
      }
    }
  } catch (error) {
    if (!(controller.signal.aborted && error?.name === 'AbortError')) throw error;
  } finally {
    controller.abort();
  }

  assert.equal(sawTurnStarted, true, 'SSE must replay turn_started');
  assertStrictlyIncreasing(events, 'SSE events');
  return events;
}

async function waitForSession(client, sessionId, accept, description) {
  return poll(async () => (await client.get(`/api/sessions/${sessionId}`)).data, accept, {
    timeoutMs: 45_000,
    description
  });
}

async function waitForMessage(client, sessionId, messageId, accept, description) {
  return poll(async () => {
    const { data: messages } = await client.get(`/api/sessions/${sessionId}/messages`);
    return messages.find((message) => message.id === messageId) ?? null;
  }, (message) => message !== null && accept(message), {
    timeoutMs: 45_000,
    description
  });
}

async function waitForNativeTurn(context, hubTurnId, expectedStatus) {
  const value = await poll(() => context.compose.psql(`
    SELECT COALESCE(native_turn_id, '') || '|' || status
    FROM hub_session_turns
    WHERE id = '${hubTurnId}'
  `), (current) => {
    const [nativeTurnId, status] = current.split('|');
    return nativeTurnId.length > 0 && status === expectedStatus;
  }, {
    timeoutMs: 45_000,
    description: `Hub Turn ${hubTurnId} to bind a native Turn and reach ${expectedStatus}`
  });
  const [nativeTurnId, status] = value.split('|');
  assert.ok(nativeTurnId.length > 0);
  assert.equal(status, expectedStatus);
  return nativeTurnId;
}

function assertInitialRun(run, agentId) {
  assertUuid(run.id, 'Run id');
  assert.equal(run.agent_id, agentId);
  assert.equal(run.status, 'pending');
  assert.equal(run.source, 'console');
  assert.equal(run.initial_message, HOLD_MARKER);
  assertUuid(run.hub_session_id, 'Hub Session id');
  assertUuid(run.hub_turn_id, 'Hub Turn id');
  assertUuid(run.hub_message_id, 'Hub Message id');
}

function assertMessageHistory(messages, expected) {
  assert.deepEqual(messages.map((message) => message.sequence), expected.map((_, index) => index + 1));
  assert.deepEqual(messages.map((message) => message.content), expected);
  for (const message of messages) {
    assertUuid(message.id, 'Hub Message id');
    assert.equal(message.role, 'user');
    assert.equal(message.message_kind, 'message');
    assert.equal(message.delivery_state, 'delivered');
  }
}

export default async function sessionRunApiScenario(context) {
  const client = new ApiClient(context.baseURL);
  const { data: admin } = await loginAsAdmin(client);
  const agentName = context.unique('QA Session Run Agent');
  let agentId = null;
  let agentDeleted = false;

  try {
    const { data: agent } = await client.post('/api/agents', {
      name: agentName,
      instructions: 'Exercise deterministic Session and native Turn lifecycle behavior.',
      visibility: 'private',
      public_to: []
    });
    agentId = agent.id;
    assertUuid(agentId, 'Agent id');

    const { data: initialRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: HOLD_MARKER,
      hub_session_id: null,
      parent_run_id: null
    });
    assertInitialRun(initialRun, agentId);

    await waitForRunStatus(client, agentId, initialRun.id, 'running', 45_000);
    const activeSession = await waitForSession(
      client,
      initialRun.hub_session_id,
      (session) => session.lifecycle_status === 'online'
        && session.active_turn_id === initialRun.hub_turn_id
        && typeof session.native_thread_id === 'string'
        && session.native_thread_id.length > 0,
      'marker Session to become online with native IDs'
    );
    assert.equal(activeSession.owner_id, admin.id);
    assert.equal(activeSession.agent_id, agentId);
    assert.equal(activeSession.agent_name, agentName);
    assert.deepEqual(activeSession.origin, { kind: 'hub_native' });
    assertUuid(activeSession.active_turn_id, 'active Hub Turn id');
    const firstNativeThreadId = activeSession.native_thread_id;
    const firstNativeTurnId = await waitForNativeTurn(context, initialRun.hub_turn_id, 'running');

    const { data: fetchedInitialRun } = await client.get(`/api/runs/${initialRun.id}`);
    assert.equal(fetchedInitialRun.status, 'running');
    assert.equal(fetchedInitialRun.source, 'console');
    assert.equal(fetchedInitialRun.hub_session_id, initialRun.hub_session_id);
    assert.equal(fetchedInitialRun.hub_turn_id, initialRun.hub_turn_id);
    assert.equal(fetchedInitialRun.hub_message_id, initialRun.hub_message_id);

    const { data: initialEvents } = await client.get(`/api/runs/${initialRun.id}/events`);
    assertStrictlyIncreasing(initialEvents, 'Run events');
    const turnStarted = initialEvents.find((event) => event.event_type === 'turn_started');
    assert.ok(turnStarted, 'event history must include turn_started');
    assert.equal(turnStarted.payload.native_thread_id, firstNativeThreadId);
    assert.equal(turnStarted.payload.native_turn_id, firstNativeTurnId);

    const sseEvents = await readSseThroughTurnStarted(client, context.baseURL, initialRun.id);
    const sseTurnStarted = sseEvents.find((event) => event.event_type === 'turn_started');
    assert.ok(sseTurnStarted, 'SSE must include turn_started');
    assert.equal(sseTurnStarted.payload.native_thread_id, firstNativeThreadId);
    assert.equal(sseTurnStarted.payload.native_turn_id, firstNativeTurnId);

    const { data: initialMessages } = await client.get(
      `/api/sessions/${initialRun.hub_session_id}/messages`
    );
    assertMessageHistory(initialMessages, [HOLD_MARKER]);
    assert.equal(initialMessages[0].id, initialRun.hub_message_id);
    assert.equal(initialMessages[0].run_id, initialRun.id);
    assert.equal(initialMessages[0].turn_id, initialRun.hub_turn_id);
    assert.equal(initialMessages[0].delivery_mode, 'next_turn');
    assert.equal(initialMessages[0].expected_native_turn_id, null);

    const steerContent = context.unique('Steer active Session Turn');
    const { data: steerAcceptance } = await client.post(
      `/api/sessions/${initialRun.hub_session_id}/messages`,
      {
        content: steerContent,
        client_message_key: context.unique('session-steer')
      }
    );
    assert.ok(steerAcceptance.run, 'steering acceptance must remain bound to the active Run');
    assert.equal(steerAcceptance.run.id, initialRun.id);
    assert.equal(steerAcceptance.run.hub_session_id, initialRun.hub_session_id);
    assert.equal(steerAcceptance.run.hub_turn_id, initialRun.hub_turn_id);
    assert.equal(steerAcceptance.message.sequence, 2);
    assert.equal(steerAcceptance.message.run_id, initialRun.id);
    assert.equal(steerAcceptance.message.turn_id, initialRun.hub_turn_id);
    assert.equal(steerAcceptance.message.delivery_mode, 'steer');
    assert.equal(steerAcceptance.message.expected_native_turn_id, firstNativeTurnId);
    assert.ok(['queued', 'delivering', 'delivered'].includes(steerAcceptance.message.delivery_state));

    const deliveredSteer = await waitForMessage(
      client,
      initialRun.hub_session_id,
      steerAcceptance.message.id,
      (message) => message.delivery_state === 'delivered',
      'steering message delivery acknowledgement'
    );
    assert.equal(deliveredSteer.delivery_mode, 'steer');
    assert.equal(deliveredSteer.expected_native_turn_id, firstNativeTurnId);
    assert.equal(deliveredSteer.run_id, initialRun.id);
    assert.equal(deliveredSteer.turn_id, initialRun.hub_turn_id);

    const { data: stopRequested } = await client.post(`/api/runs/${initialRun.id}/stop`);
    assert.equal(stopRequested.id, initialRun.id);
    assert.equal(stopRequested.status, 'running');
    const interruptedRun = await waitForRunStatus(
      client,
      agentId,
      initialRun.id,
      'interrupted',
      45_000
    );
    assert.equal(interruptedRun.hub_session_id, initialRun.hub_session_id);
    assert.equal(interruptedRun.hub_turn_id, initialRun.hub_turn_id);
    assert.equal(await waitForNativeTurn(context, initialRun.hub_turn_id, 'interrupted'), firstNativeTurnId);

    const { data: interruptedMessages } = await client.get(
      `/api/sessions/${initialRun.hub_session_id}/messages`
    );
    assertMessageHistory(interruptedMessages, [HOLD_MARKER, steerContent]);
    assert.equal(interruptedMessages[0].id, initialRun.hub_message_id);
    assert.equal(interruptedMessages[1].id, steerAcceptance.message.id);
    assert.equal(interruptedMessages[1].delivery_mode, 'steer');
    assert.equal(interruptedMessages[1].expected_native_turn_id, firstNativeTurnId);

    const { data: interruptedEvents } = await client.get(`/api/runs/${initialRun.id}/events`);
    assertStrictlyIncreasing(interruptedEvents, 'interrupted Run events');
    assert.ok(interruptedEvents.some((event) => event.event_type === 'status'
      && (event.content === 'interrupted' || event.payload?.status === 'interrupted')));
    assert.equal(
      interruptedEvents.find((event) => event.event_type === 'turn_started').payload.native_turn_id,
      firstNativeTurnId
    );

    const nextContent = context.unique('Complete ordinary next Turn');
    const { data: nextAcceptance } = await client.post(
      `/api/sessions/${initialRun.hub_session_id}/messages`,
      {
        content: nextContent,
        client_message_key: context.unique('session-next-turn')
      }
    );
    assert.ok(nextAcceptance.run);
    assert.notEqual(nextAcceptance.run.id, initialRun.id);
    assert.equal(nextAcceptance.run.status, 'pending');
    assert.equal(nextAcceptance.run.source, 'console');
    assert.equal(nextAcceptance.run.hub_session_id, initialRun.hub_session_id);
    assert.notEqual(nextAcceptance.run.hub_turn_id, initialRun.hub_turn_id);
    assert.equal(nextAcceptance.message.sequence, 3);
    assert.equal(nextAcceptance.message.run_id, nextAcceptance.run.id);
    assert.equal(nextAcceptance.message.turn_id, nextAcceptance.run.hub_turn_id);
    assert.equal(nextAcceptance.message.delivery_mode, 'next_turn');

    await waitForRunStatus(client, agentId, nextAcceptance.run.id, 'completed', 60_000);
    const firstSessionAfterNextTurn = await client.get(
      `/api/sessions/${initialRun.hub_session_id}`
    );
    assert.equal(firstSessionAfterNextTurn.data.native_thread_id, firstNativeThreadId);
    assert.equal(firstSessionAfterNextTurn.data.active_turn_id, null);
    const secondNativeTurnId = await waitForNativeTurn(
      context,
      nextAcceptance.run.hub_turn_id,
      'completed'
    );
    assert.notEqual(secondNativeTurnId, firstNativeTurnId);

    const { data: firstSessionMessages } = await client.get(
      `/api/sessions/${initialRun.hub_session_id}/messages`
    );
    assertMessageHistory(firstSessionMessages, [HOLD_MARKER, steerContent, nextContent]);
    assert.equal(firstSessionMessages[2].run_id, nextAcceptance.run.id);
    assert.equal(firstSessionMessages[2].turn_id, nextAcceptance.run.hub_turn_id);

    const isolatedContent = context.unique('Complete isolated Session Turn');
    const { data: isolatedRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: isolatedContent,
      hub_session_id: null,
      parent_run_id: null
    });
    assert.equal(isolatedRun.status, 'pending');
    assert.equal(isolatedRun.source, 'console');
    assertUuid(isolatedRun.hub_session_id, 'isolated Hub Session id');
    assert.notEqual(isolatedRun.hub_session_id, initialRun.hub_session_id);
    assertUuid(isolatedRun.hub_turn_id, 'isolated Hub Turn id');
    assertUuid(isolatedRun.hub_message_id, 'isolated Hub Message id');
    await waitForRunStatus(client, agentId, isolatedRun.id, 'completed', 60_000);

    const { data: isolatedSession } = await client.get(`/api/sessions/${isolatedRun.hub_session_id}`);
    assert.ok(isolatedSession.native_thread_id);
    assert.notEqual(isolatedSession.native_thread_id, firstNativeThreadId);
    assert.equal(isolatedSession.active_turn_id, null);
    const isolatedNativeTurnId = await waitForNativeTurn(
      context,
      isolatedRun.hub_turn_id,
      'completed'
    );
    assert.notEqual(isolatedNativeTurnId, firstNativeTurnId);
    assert.notEqual(isolatedNativeTurnId, secondNativeTurnId);
    const { data: isolatedMessages } = await client.get(
      `/api/sessions/${isolatedRun.hub_session_id}/messages`
    );
    assertMessageHistory(isolatedMessages, [isolatedContent]);
    assert.equal(isolatedMessages[0].run_id, isolatedRun.id);
    assert.equal(isolatedMessages[0].turn_id, isolatedRun.hub_turn_id);

    const memberClient = new ApiClient(context.baseURL);
    const memberEmail = `${context.unique('qa-session-member')
      .toLowerCase()
      .replace(/[^a-z0-9-]/g, '')}@example.com`;
    const { data: registration } = await memberClient.post('/api/auth/register', {
      email: memberEmail,
      password: `${context.unique('SessionMember')}!Aa9`
    });
    assert.equal(registration.user.role, 'member');
    const { data: memberSessions } = await memberClient.get('/api/sessions');
    const protectedSessionIds = [initialRun.hub_session_id, isolatedRun.hub_session_id];
    assert.ok(memberSessions.every((session) => !protectedSessionIds.includes(session.id)));
    for (const sessionId of protectedSessionIds) {
      await memberClient.get(`/api/sessions/${sessionId}`, { expectedStatus: 404 });
      await memberClient.get(`/api/sessions/${sessionId}/messages`, { expectedStatus: 404 });
    }
    for (const runId of [initialRun.id, nextAcceptance.run.id, isolatedRun.id]) {
      await memberClient.get(`/api/runs/${runId}`, { expectedStatus: 404 });
    }

    await client.delete(`/api/agents/${agentId}`, { expectedStatus: 204 });
    agentDeleted = true;
    const historicalFirst = await waitForSession(
      client,
      initialRun.hub_session_id,
      (session) => session.lifecycle_status === 'historical' && session.agent_deleted_at !== null,
      'first Session to become historical after Agent deletion'
    );
    assert.equal(historicalFirst.agent_name, agentName);
    assert.equal(historicalFirst.native_thread_id, firstNativeThreadId);
    const historicalIsolated = await waitForSession(
      client,
      isolatedRun.hub_session_id,
      (session) => session.lifecycle_status === 'historical' && session.agent_deleted_at !== null,
      'isolated Session to become historical after Agent deletion'
    );
    assert.equal(historicalIsolated.agent_name, agentName);
    assert.equal(historicalIsolated.native_thread_id, isolatedSession.native_thread_id);

    const { data: historicalMessages } = await client.get(
      `/api/sessions/${initialRun.hub_session_id}/messages`
    );
    assertMessageHistory(historicalMessages, [HOLD_MARKER, steerContent, nextContent]);
    const { data: historicalIsolatedMessages } = await client.get(
      `/api/sessions/${isolatedRun.hub_session_id}/messages`
    );
    assertMessageHistory(historicalIsolatedMessages, [isolatedContent]);
    await client.post(`/api/sessions/${initialRun.hub_session_id}/messages`, {
      content: 'must not continue after Agent deletion'
    }, { expectedStatus: 404 });
  } finally {
    if (agentId && !agentDeleted) {
      await client.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
    }
  }
}
