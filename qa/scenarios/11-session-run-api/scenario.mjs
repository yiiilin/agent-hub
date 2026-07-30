import assert from 'node:assert/strict';
import {
  ApiClient,
  loginAsAdmin,
  poll,
  provisionLocalUser,
  waitForRunStatus
} from '../../support/api.mjs';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function assertUuid(value, label) {
  assert.match(value, UUID_PATTERN, `${label} must be a UUID`);
}

async function createComposeModelFixture(client, context) {
  const { data: connection } = await client.post('/api/model-connections', {
    scope: 'personal',
    name: context.unique('QA Pi Session model'),
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

function assertStrictlyIncreasing(items, label) {
  assert.ok(items.length > 0, `${label} must not be empty`);
  for (let index = 0; index < items.length; index += 1) {
    assert.ok(Number.isInteger(items[index].seq), `${label}[${index}].seq must be an integer`);
    if (index > 0) {
      assert.ok(items[index].seq > items[index - 1].seq,
        `${label} seq must be strictly increasing`);
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
    headers: { accept: 'text/event-stream', cookie: client.cookieHeader() },
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

function userMessages(messages) {
  return messages.filter((message) => message.role === 'user' && message.message_kind === 'message');
}

function assertUserMessageHistory(messages, expectedContents) {
  const history = userMessages(messages);
  assert.deepEqual(history.map((message) => message.content), expectedContents);
  for (const message of history) {
    assertUuid(message.id, 'Hub Message id');
    assert.equal(message.delivery_state, 'delivered');
  }
  return history;
}

function turnStarted(events, label) {
  const started = events.find((event) => event.event_type === 'turn_started');
  assert.ok(started, `${label} must include turn_started`);
  assert.equal(typeof started.payload?.native_session_id, 'string');
  assert.ok(started.payload.native_session_id.length > 0);
  assert.equal(typeof started.payload?.native_turn_id, 'string');
  assert.ok(started.payload.native_turn_id.length > 0);
  return started;
}

export default async function sessionRunApiScenario(context) {
  const client = new ApiClient(context.baseURL);
  const { data: admin } = await loginAsAdmin(client);
  const agentName = context.unique('QA Pi Session Run Agent');
  let agentId = null;
  let agentDeleted = false;
  let modelConnectionId = null;

  try {
    const modelFixture = await createComposeModelFixture(client, context);
    modelConnectionId = modelFixture.connectionId;
    const { data: agent } = await client.post('/api/agents', {
      name: agentName,
      instructions: 'Exercise completed Pi Session and native Turn lifecycle behavior.',
      visibility: 'private',
      public_to: [],
      model_selection: modelFixture.selection
    });
    agentId = agent.id;
    assertUuid(agentId, 'Agent id');

    const initialContent = context.unique('Complete first Pi Session Turn');
    const { data: initialRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: initialContent,
      hub_session_id: null,
      parent_run_id: null
    });
    assertUuid(initialRun.id, 'Run id');
    assert.equal(initialRun.agent_id, agentId);
    assert.equal(initialRun.source, 'console');
    assert.equal(initialRun.initial_message, initialContent);
    assertUuid(initialRun.hub_session_id, 'Hub Session id');
    assertUuid(initialRun.hub_turn_id, 'Hub Turn id');
    assertUuid(initialRun.hub_message_id, 'Hub Message id');
    await waitForRunStatus(client, agentId, initialRun.id, 'completed', 60_000);

    const firstSession = await waitForSession(
      client,
      initialRun.hub_session_id,
      (session) => session.lifecycle_status === 'online'
        && session.active_turn_id === null
        && typeof session.native_session_id === 'string'
        && session.native_session_id.length > 0,
      'completed Pi Session to expose its native Session id'
    );
    assert.equal(firstSession.owner_id, admin.id);
    assert.equal(firstSession.agent_id, agentId);
    assert.equal(firstSession.agent_name, agentName);
    assert.deepEqual(firstSession.origin, { kind: 'hub_native' });
    const nativePiSessionId = firstSession.native_session_id;

    const { data: fetchedInitialRun } = await client.get(`/api/runs/${initialRun.id}`);
    assert.equal(fetchedInitialRun.status, 'completed');
    assert.equal(fetchedInitialRun.hub_session_id, initialRun.hub_session_id);
    assert.equal(fetchedInitialRun.hub_turn_id, initialRun.hub_turn_id);
    const { data: initialEvents } = await client.get(`/api/runs/${initialRun.id}/events`);
    assertStrictlyIncreasing(initialEvents, 'first Pi Run events');
    const firstTurnStarted = turnStarted(initialEvents, 'first Pi Run events');
    assert.equal(firstTurnStarted.payload.native_session_id, nativePiSessionId);
    const sseEvents = await readSseThroughTurnStarted(client, context.baseURL, initialRun.id);
    assert.equal(turnStarted(sseEvents, 'first Pi Run SSE').payload.native_turn_id,
      firstTurnStarted.payload.native_turn_id);

    const { data: initialMessages } = await client.get(`/api/sessions/${initialRun.hub_session_id}/messages`);
    const initialUsers = assertUserMessageHistory(initialMessages, [initialContent]);
    assert.equal(initialUsers[0].id, initialRun.hub_message_id);
    assert.equal(initialUsers[0].run_id, initialRun.id);
    assert.equal(initialUsers[0].turn_id, initialRun.hub_turn_id);
    assert.equal(initialUsers[0].delivery_mode, 'next_turn');

    const nextContent = context.unique('Complete second Pi Session Turn');
    const { data: nextAcceptance } = await client.post(
      `/api/sessions/${initialRun.hub_session_id}/messages`,
      { content: nextContent, client_message_key: context.unique('pi-session-next-turn') }
    );
    assert.ok(nextAcceptance.run, 'continuation must schedule a Run');
    assert.notEqual(nextAcceptance.run.id, initialRun.id);
    assert.equal(nextAcceptance.run.hub_session_id, initialRun.hub_session_id);
    assert.notEqual(nextAcceptance.run.hub_turn_id, initialRun.hub_turn_id);
    assert.equal(nextAcceptance.message.delivery_mode, 'next_turn');
    await waitForRunStatus(client, agentId, nextAcceptance.run.id, 'completed', 60_000);
    const secondSession = await waitForSession(
      client,
      initialRun.hub_session_id,
      (session) => session.active_turn_id === null && session.native_session_id === nativePiSessionId,
      'second completed Pi Turn to retain the native Session id'
    );
    assert.equal(secondSession.native_session_id, nativePiSessionId);
    const { data: nextEvents } = await client.get(`/api/runs/${nextAcceptance.run.id}/events`);
    const secondTurnStarted = turnStarted(nextEvents, 'second Pi Run events');
    assert.equal(secondTurnStarted.payload.native_session_id, nativePiSessionId);
    assert.notEqual(secondTurnStarted.payload.native_turn_id, firstTurnStarted.payload.native_turn_id);
    const { data: continuedMessages } = await client.get(`/api/sessions/${initialRun.hub_session_id}/messages`);
    const continuedUsers = assertUserMessageHistory(continuedMessages, [initialContent, nextContent]);
    assert.equal(continuedUsers[1].run_id, nextAcceptance.run.id);
    assert.equal(continuedUsers[1].turn_id, nextAcceptance.run.hub_turn_id);

    const isolatedContent = context.unique('Complete isolated Pi Session Turn');
    const { data: isolatedRun } = await client.post(`/api/agents/${agentId}/runs`, {
      message: isolatedContent,
      hub_session_id: null,
      parent_run_id: null
    });
    await waitForRunStatus(client, agentId, isolatedRun.id, 'completed', 60_000);
    const isolatedSession = await waitForSession(
      client,
      isolatedRun.hub_session_id,
      (session) => session.active_turn_id === null && typeof session.native_session_id === 'string',
      'isolated Pi Session to complete'
    );
    assert.notEqual(isolatedSession.native_session_id, nativePiSessionId);
    const { data: isolatedEvents } = await client.get(`/api/runs/${isolatedRun.id}/events`);
    assert.notEqual(turnStarted(isolatedEvents, 'isolated Pi Run events').payload.native_session_id,
      nativePiSessionId);

    const member = await provisionLocalUser(client, context, 'qa-pi-session-member');
    const memberClient = member.client;
    assert.equal(member.user.role, 'member');
    for (const sessionId of [initialRun.hub_session_id, isolatedRun.hub_session_id]) {
      await memberClient.get(`/api/sessions/${sessionId}`, { expectedStatus: 404 });
      await memberClient.get(`/api/sessions/${sessionId}/messages`, { expectedStatus: 404 });
    }

    await client.delete(`/api/agents/${agentId}`, { expectedStatus: 204 });
    agentDeleted = true;
    const historicalFirst = await waitForSession(
      client,
      initialRun.hub_session_id,
      (session) => session.lifecycle_status === 'historical' && session.agent_deleted_at !== null,
      'first Pi Session to become historical after Agent deletion'
    );
    assert.equal(historicalFirst.native_session_id, nativePiSessionId);
    const historicalIsolated = await waitForSession(
      client,
      isolatedRun.hub_session_id,
      (session) => session.lifecycle_status === 'historical' && session.agent_deleted_at !== null,
      'isolated Pi Session to become historical after Agent deletion'
    );
    assert.equal(historicalIsolated.native_session_id, isolatedSession.native_session_id);
    await client.post(`/api/sessions/${initialRun.hub_session_id}/messages`, {
      content: 'must not continue after Agent deletion'
    }, { expectedStatus: 404 });
  } finally {
    if (agentId && !agentDeleted) {
      await client.delete(`/api/agents/${agentId}`, { expectedStatus: [204, 404] });
    }
    if (modelConnectionId) {
      await client.delete(`/api/model-connections/${modelConnectionId}`, {
        expectedStatus: [204, 404]
      });
    }
  }
}
