import assert from "node:assert/strict";
import test from "node:test";

import { MemoryToolJournalStorage, connect } from "../dist/index.js";

class MemoryStorage {
  #values = new Map();
  get length() { return this.#values.size; }
  clear() { this.#values.clear(); }
  getItem(key) { return this.#values.get(key) ?? null; }
  key(index) { return [...this.#values.keys()][index] ?? null; }
  removeItem(key) { this.#values.delete(key); }
  setItem(key, value) { this.#values.set(key, String(value)); }
}

const SESSION_ID = "11111111-1111-4111-8111-111111111111";
const TOOL_CALL_ID = "22222222-2222-4222-8222-222222222222";

async function makeTestHarness({ journalEntries = [], eventPayload = [] } = {}) {
  const calls = { claim: 0, result: 0, events: 0 };
  const executed = [];
  const storage = new MemoryStorage();
  // 固定 Client Instance（模拟刷新后 sessionStorage 保留的实例 ID）
  storage.setItem("agent-hub:client-instance-id", "client-1");
  const journal = new MemoryToolJournalStorage();
  for (const entry of journalEntries) {
    await journal.put(entry);
  }

  const fetchMock = async (input, init = {}) => {
    const url = String(input);
    const method = init.method ?? "GET";
    const respond = (status, body) => ({
      ok: status >= 200 && status < 300,
      status,
      json: async () => body,
      text: async () => JSON.stringify(body),
      body: null,
    });

    if (url.endsWith("/api/client/tool-calls/" + TOOL_CALL_ID + "/claim")) {
      calls.claim += 1;
      return respond(200, { status: "claimed", terminal: false });
    }
    if (url.endsWith("/api/client/tool-calls/" + TOOL_CALL_ID + "/result")) {
      calls.result += 1;
      return respond(200, {});
    }
    if (url.endsWith("/api/client/renew")) {
      return respond(200, {
        access_token: "ahw_renewed",
        expires_at: Date.now() + 3_600_000,
        client_instance_id: "client-1",
        tool_names: ["get_page_state", "click_element_by_index"],
      });
    }
    if (url.includes("/api/client/sessions/" + SESSION_ID + "/events")) {
      calls.events += 1;
      return respond(200, eventPayload);
    }
    if (url.endsWith("/api/client/sessions")) {
      return respond(200, { items: [] });
    }
    throw new Error(`unexpected fetch: ${method} ${url}`);
  };

  const handlers = {
    get_page_state: async () => ({ url: "u", title: "t", content: "[]" }),
    click_element_by_index: async (input) => {
      executed.push(input);
      return { message: `clicked ${input.index}` };
    },
  };

  return {
    storage,
    journal,
    fetchMock,
    handlers,
    calls,
    executed,
    makeClient: () =>
      connect({
        baseUrl: "http://hub.test",
        fetch: fetchMock,
        sessionStorage: storage,
        storage: journal,
        handlers,
        authorize: async ({ clientInstanceId }) => ({
          access_token: "ahw_test",
          expires_at: Date.now() + 3_600_000,
          client_instance_id: clientInstanceId,
          agent: { id: "agent-1", name: "test" },
          history_enabled: false,
          tool_names: ["get_page_state", "click_element_by_index"],
        }),
      }),
  };
}

test("recoverPendingTools resumes a journal entry left executing across a refresh", async () => {
  const h = await makeTestHarness({
    journalEntries: [
      {
        clientInstanceId: "client-1",
        toolCallId: TOOL_CALL_ID,
        sessionId: SESSION_ID,
        runId: "run-1",
        toolName: "click_element_by_index",
        input: { index: 3 },
        state: "executing",
        createdAt: Date.now() - 1000,
        updatedAt: Date.now() - 1000,
      },
    ],
  });

  const client = await h.makeClient();
  const session = client.sessions.existing(SESSION_ID);
  await session.recoverPendingTools();

  assert.equal(h.calls.claim, 1, "claim must be replayed");
  assert.equal(h.calls.result, 1, "result must be submitted");
  assert.deepEqual(h.executed, [{ index: 3 }], "handler must run with stored input");

  const entry = await h.journal.get("client-1", TOOL_CALL_ID);
  assert.equal(entry.state, "acknowledged");
  client.dispose();
});

test("recoverPendingTools discovers tool requests missing results in the event stream", async () => {
  const h = await makeTestHarness({
    eventPayload: [
      {
        seq: 1,
        event_id: "e1",
        run_id: "run-1",
        event_type: "tool_request",
        role: "assistant",
        content: "requested",
        payload: {
          tool_call_id: TOOL_CALL_ID,
          tool_name: "click_element_by_index",
          arguments: { index: 7 },
          batch_id: "batch-1",
        },
        created_at: new Date().toISOString(),
      },
    ],
  });

  const client = await h.makeClient();
  const session = client.sessions.existing(SESSION_ID);
  await session.recoverPendingTools();

  assert.equal(h.calls.claim, 1, "claim must be replayed for the discovered call");
  assert.equal(h.calls.result, 1, "result must be submitted");
  assert.deepEqual(h.executed, [{ index: 7 }], "handler must run with event arguments");
  client.dispose();
});

test("recoverPendingTools skips tool requests that already have results", async () => {
  const h = await makeTestHarness({
    eventPayload: [
      {
        seq: 1,
        event_id: "e1",
        run_id: "run-1",
        event_type: "tool_request",
        role: "assistant",
        content: "requested",
        payload: {
          tool_call_id: TOOL_CALL_ID,
          tool_name: "click_element_by_index",
          arguments: { index: 1 },
          batch_id: "batch-1",
        },
        created_at: new Date().toISOString(),
      },
      {
        seq: 2,
        event_id: "e2",
        run_id: "run-1",
        event_type: "tool_result",
        role: "tool",
        content: null,
        payload: {
          tool_call_id: TOOL_CALL_ID,
          tool_name: "click_element_by_index",
          result: { status: "success", output: "done" },
        },
        created_at: new Date().toISOString(),
      },
    ],
  });

  const client = await h.makeClient();
  const session = client.sessions.existing(SESSION_ID);
  await session.recoverPendingTools();

  assert.equal(h.calls.claim, 0, "completed calls must not be replayed");
  assert.equal(h.calls.result, 0);
  assert.equal(h.executed.length, 0);
  client.dispose();
});
