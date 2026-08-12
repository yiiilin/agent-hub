import assert from "node:assert/strict";
import test from "node:test";
import { Worker } from "node:worker_threads";

import {
  AgentHubError,
  ClientToolError,
  MemoryToolJournalStorage,
  connect,
  connectAnonymous,
} from "../dist/index.js";

class MemoryStorage {
  #values = new Map();

  get length() {
    return this.#values.size;
  }

  clear() {
    this.#values.clear();
  }

  getItem(key) {
    return this.#values.get(key) ?? null;
  }

  key(index) {
    return [...this.#values.keys()][index] ?? null;
  }

  removeItem(key) {
    this.#values.delete(key);
  }

  setItem(key, value) {
    this.#values.set(key, String(value));
  }

  entries() {
    return [...this.#values.entries()];
  }
}

function credential(token, tools = [], lifetimeMs = 60 * 60_000) {
  return {
    token,
    expires_at: new Date(Date.now() + lifetimeMs).toISOString(),
    authorized_tools: tools,
  };
}

function json(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function pathOf(input) {
  return new URL(String(input)).pathname;
}

function waitFor(check, timeoutMs = 2_000) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const poll = () => {
      if (check()) resolve();
      else if (Date.now() - started >= timeoutMs) reject(new Error("condition was not reached"));
      else setTimeout(poll, 5);
    };
    poll();
  });
}

async function connectFromClonedTab(storedInstanceId) {
  const moduleUrl = new URL("../dist/index.js", import.meta.url).href;
  const worker = new Worker(`
    const { parentPort, workerData } = require("node:worker_threads");

    class MemoryStorage {
      constructor(entries) { this.values = new Map(entries); }
      get length() { return this.values.size; }
      clear() { this.values.clear(); }
      getItem(key) { return this.values.get(key) ?? null; }
      key(index) { return [...this.values.keys()][index] ?? null; }
      removeItem(key) { this.values.delete(key); }
      setItem(key, value) { this.values.set(key, String(value)); }
    }

    (async () => {
      const { connect, MemoryToolJournalStorage } = await import(workerData.moduleUrl);
      const storage = new MemoryStorage([
        ["agent-hub:client-instance-id", workerData.storedInstanceId],
      ]);
      const client = await connect({
        baseUrl: "https://hub.example",
        sessionStorage: storage,
        storage: new MemoryToolJournalStorage(),
        authorize: async () => ({
          token: "cloned-tab-token",
          expires_at: new Date(Date.now() + 60_000).toISOString(),
          authorized_tools: [],
        }),
        fetch: async () => new Response("[]", {
          headers: { "Content-Type": "application/json" },
        }),
      });
      parentPort.postMessage({ clientInstanceId: client.clientInstanceId });
      parentPort.once("message", () => {
        client.dispose();
        parentPort.close();
      });
    })().catch((error) => {
      parentPort.postMessage({ error: error?.stack ?? String(error) });
    });
  `, {
    eval: true,
    workerData: { moduleUrl, storedInstanceId },
  });
  try {
    const result = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("cloned tab did not initialize")), 2_000);
      worker.once("error", reject);
      worker.once("message", (message) => {
        clearTimeout(timeout);
        if (message.error) reject(new Error(message.error));
        else resolve(message.clientInstanceId);
      });
    });
    worker.postMessage("dispose");
    return result;
  } finally {
    await worker.terminate();
  }
}

function sseResponse(frames, signal, keepOpen = false) {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(frames.join("")));
      if (keepOpen) {
        signal?.addEventListener("abort", () => {
          controller.error(new DOMException("Aborted", "AbortError"));
        }, { once: true });
      } else {
        controller.close();
      }
    },
  });
  return new Response(stream, { headers: { "Content-Type": "text/event-stream" } });
}

function toolFrame(sequence, toolCallId, toolName, batchId = "batch-1", expiresAt) {
  return [
    "event: tool_request\n",
    `id: ${sequence}\n`,
    `data: ${JSON.stringify({
      seq: sequence,
      type: "tool_request",
      tool_call_id: toolCallId,
      tool_name: toolName,
      batch_id: batchId,
      arguments: { sequence },
      ...(expiresAt ? { expires_at: expiresAt } : {}),
    })}\n\n`,
  ].join("");
}

test("authenticated clients reuse a tab ID and never persist credentials", async () => {
  const tabStorage = new MemoryStorage();
  const journal = new MemoryToolJournalStorage();
  const authorizeCalls = [];
  const requests = [];
  const authorize = async (request) => {
    authorizeCalls.push(request);
    return credential("secret-client-token");
  };
  const fetch = async (input, init = {}) => {
    requests.push({ input: String(input), init });
    return json([{ id: "session-1", preview: "hello" }]);
  };

  const first = await connect({
    baseUrl: "https://hub.example",
    authorize,
    fetch,
    sessionStorage: tabStorage,
    storage: journal,
  });
  const sessions = await first.sessions.list({ limit: 10 });
  assert.equal(sessions[0].id, "session-1");

test("checked tool results pass through verbatim (Agent Hub archives large results)", async () => {
  const { checkedToolResult } = await import("../dist/client.js");

  // 大数组：不再截断，原样保留（后端归档机制接管大结果）
  const bigArray = Array.from({ length: 2000 }, (_, i) => ({ id: i, name: `item-${i}-${"x".repeat(20)}` }));
  const passed = checkedToolResult({ status: "success", output: bigArray });
  assert.equal(passed.status, "success");
  assert.equal(passed.truncated, undefined, "no truncation marker");
  assert.equal(passed.output.length, 2000, "full array preserved");

  // 大字符串：原样保留
  const bigString = checkedToolResult({ status: "success", output: "x".repeat(100_000) });
  assert.equal(bigString.status, "success");
  assert.equal(bigString.output.length, 100_000, "full string preserved");

  // 小结果不受影响
  const small = checkedToolResult({ status: "success", output: { ok: true } });
  assert.equal(small.status, "success");
  assert.equal(small.truncated, undefined);

  // 非 JSON 输出仍显式报错
  const notJson = checkedToolResult({ status: "success", output: () => 1 });
  assert.equal(notJson.status, "error");
  assert.equal(notJson.error.code, "tool_result_not_json");
});

test("sessions.delete issues a DELETE request and resolves on 204", async () => {
  const requests = [];
  const authorize = async () => credential("secret-client-token");
  const fetch = async (input, init = {}) => {
    requests.push({ input: String(input), init });
    return new Response(null, { status: 204 });
  };

  const client = await connect({
    baseUrl: "https://hub.example",
    authorize,
    fetch,
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
  });
  await client.sessions.delete("session-9");
  assert.equal(requests.length, 1);
  assert.equal(pathOf(requests[0].input), "/api/client/sessions/session-9");
  assert.equal(requests[0].init.method, "DELETE");
  assert.equal(new Headers(requests[0].init.headers).get("Authorization"), "Bearer secret-client-token");
  client.dispose();
});

  assert.equal(pathOf(requests[0].input), "/api/client/sessions");
  assert.equal(new Headers(requests[0].init.headers).get("Authorization"), "Bearer secret-client-token");
  const firstInstanceId = first.clientInstanceId;
  assert.match(firstInstanceId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  first.dispose();

  const second = await connect({
    baseUrl: "https://hub.example",
    authorize,
    fetch,
    sessionStorage: tabStorage,
    storage: journal,
  });
  assert.equal(second.clientInstanceId, firstInstanceId);
  assert.deepEqual(authorizeCalls.map((call) => call.clientInstanceId), [firstInstanceId, firstInstanceId]);
  assert.equal(tabStorage.entries().some(([, value]) => value.includes("secret-client-token")), false);
  second.dispose();
});

test("a cloned live tab replaces the copied Client Instance ID", async () => {
  const tabStorage = new MemoryStorage();
  const client = await connect({
    baseUrl: "https://hub.example",
    authorize: async () => credential("original-tab-token"),
    fetch: async () => json([]),
    sessionStorage: tabStorage,
    storage: new MemoryToolJournalStorage(),
  });

  const clonedInstanceId = await connectFromClonedTab(client.clientInstanceId);
  const originalInstanceId = client.clientInstanceId;
  client.dispose();
  assert.notEqual(clonedInstanceId, originalInstanceId);
  assert.equal(
    tabStorage.getItem("agent-hub:client-instance-id"),
    originalInstanceId,
  );
});

test("connection metadata stays available when renewal omits unchanged fields", async () => {
  let renew = false;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    renewalWindowMs: 0,
    authorize: async () => ({
      ...credential("metadata-token", ["show_notice"], 20),
      agent: { id: "agent-1", name: "Metadata Agent", instructions: "Help the user." },
      history_enabled: true,
    }),
    fetch: async (input) => {
      const path = pathOf(input);
      if (path === "/api/client/renew") {
        renew = true;
        return json(credential("renewed-token", [], 60_000));
      }
      if (path === "/api/client/sessions") return json([]);
      throw new Error(`unexpected path ${path}`);
    },
  });

  assert.deepEqual(client.agent, {
    id: "agent-1",
    name: "Metadata Agent",
    instructions: "Help the user.",
  });
  assert.equal(client.historyEnabled, true);
  await new Promise((resolve) => setTimeout(resolve, 30));
  await client.listSessions();
  assert.equal(renew, true);
  assert.equal(client.agent?.name, "Metadata Agent");
  assert.equal(client.historyEnabled, true);
  client.dispose();
});

test("renewal 401 invokes authorize once and reauthorize explicitly replaces the grant", async () => {
  let authorizeCount = 0;
  let renewCount = 0;
  const listTokens = [];
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    renewalWindowMs: 0,
    authorize: async () => {
      authorizeCount += 1;
      return credential(`authorized-${authorizeCount}`, authorizeCount === 3 ? ["latest"] : [], authorizeCount === 1 ? 20 : 60_000);
    },
    fetch: async (input, init = {}) => {
      const path = pathOf(input);
      if (path === "/api/client/renew") {
        renewCount += 1;
        return json({ code: "credential_expired", message: "expired" }, 401);
      }
      if (path === "/api/client/sessions") {
        listTokens.push(new Headers(init.headers).get("Authorization"));
        return json([]);
      }
      throw new Error(`unexpected path ${path}`);
    },
  });

  await waitFor(() => authorizeCount === 2);
  await client.listSessions();
  assert.deepEqual(listTokens, ["Bearer authorized-2"]);
  assert.equal(renewCount, 1);

  await client.reauthorize();
  assert.equal(authorizeCount, 3);
  assert.deepEqual([...client.authorizedToolNames], ["latest"]);
  client.dispose();
});

test("draft send retries with one stable client_message_key and materializes the Session", async () => {
  const posted = [];
  let attempts = 0;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    requestRetryDelayMs: 0,
    authorize: async () => credential("send-token"),
    fetch: async (input, init = {}) => {
      assert.equal(pathOf(input), "/api/client/runs");
      posted.push(JSON.parse(init.body));
      attempts += 1;
      if (attempts === 1) return json({ message: "temporary" }, 503);
      return json({ id: "run-1", status: "pending", session_id: "session-created" });
    },
  });

  const draft = client.draft();
  assert.equal(draft.isDraft, true);
  const sent = await draft.send("hello");
  assert.equal(draft.id, "session-created");
  assert.equal(sent.sessionId, "session-created");
  assert.equal(posted.length, 2);
  assert.equal(posted[0].client_message_key, posted[1].client_message_key);
  assert.equal("session_id" in posted[0], false);
  assert.equal(client.existing("session-created"), draft);
  client.dispose();
});

test("concurrent sends on one draft create one Session and then steer that Session", async () => {
  const posted = [];
  let requestCount = 0;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    authorize: async () => credential("concurrent-send-token"),
    fetch: async (_input, init = {}) => {
      const body = JSON.parse(init.body);
      posted.push(body);
      requestCount += 1;
      return json({
        id: `run-${requestCount}`,
        status: "pending",
        session_id: "one-session",
      });
    },
  });

  const draft = client.draft();
  await Promise.all([draft.send("first"), draft.send("second")]);
  assert.equal("session_id" in posted[0], false);
  assert.equal(posted[1].session_id, "one-session");
  client.dispose();
  await assert.rejects(draft.send("after dispose"), /Session is disposed/);
});

test("Session events reads persisted events through the current credential", async () => {
  const requests = [];
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    authorize: async () => credential("event-list-token"),
    fetch: async (input, init = {}) => {
      requests.push({ url: String(input), headers: new Headers(init.headers) });
      return json([{
        seq: 4,
        event_id: "event-4",
        run_id: "run-4",
        event_type: "status",
        content: "failed",
        payload: { error_code: "engine_turn_timeout", timeout_seconds: 3600 },
        created_at: "2026-07-30T08:00:00.000Z",
      }]);
    },
  });

  const events = await client.existing("session-history").events({ after: 3 });
  const request = new URL(requests[0].url);
  assert.equal(request.pathname, "/api/client/sessions/session-history/events");
  assert.equal(request.searchParams.get("after"), "3");
  assert.equal(requests[0].headers.get("Authorization"), "Bearer event-list-token");
  assert.deepEqual(events.map((event) => [event.type, event.sequence, event.runId]), [["event", 4, "run-4"]]);
  assert.equal(events[0].raw.payload.error_code, "engine_turn_timeout");
  client.dispose();
});

test("Session SSE reconnects from the last cursor and emits typed events", async () => {
  const streamRequests = [];
  let streamCount = 0;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    authorize: async () => credential("stream-token"),
    fetch: async (input, init = {}) => {
      streamRequests.push({ url: String(input), headers: new Headers(init.headers) });
      streamCount += 1;
      const sequence = streamCount;
      return sseResponse([
        `event: session_event\nid: ${sequence}\ndata: ${JSON.stringify({
          seq: sequence,
          event_type: "assistant",
          content: `answer-${sequence}`,
        })}\n\n`,
      ], init.signal, streamCount >= 2);
    },
  });

  const events = [];
  let subscription;
  subscription = client.existing("session-1").subscribe((event) => {
    events.push(event);
    if (event.sequence === 2) subscription.dispose();
  }, { reconnectDelayMs: 0 });
  await subscription.closed;

  assert.deepEqual(events.map((event) => [event.type, event.sequence]), [["assistant", 1], ["assistant", 2]]);
  const secondUrl = new URL(streamRequests[1].url);
  assert.equal(secondUrl.pathname, "/api/client/sessions/session-1/events/stream");
  assert.equal(secondUrl.searchParams.get("after"), "1");
  assert.equal(streamRequests[1].headers.get("Last-Event-ID"), "1");
  client.dispose();
});

test("Session SSE reports HTTP failures before reconnecting", async () => {
  let streamCount = 0;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    authorize: async () => credential("stream-error-token"),
    fetch: async (_input, init = {}) => {
      streamCount += 1;
      if (streamCount === 1) {
        return json({ error: { code: "upstream_unavailable", message: "Stream unavailable" } }, 502);
      }
      return sseResponse([
        `event: session_event\nid: 1\ndata: ${JSON.stringify({
          seq: 1,
          event_type: "assistant",
          content: "reconnected",
        })}\n\n`,
      ], init.signal, true);
    },
  });

  const events = [];
  let subscription;
  subscription = client.existing("session-stream-error").subscribe((event) => {
    events.push(event);
    if (event.type === "assistant") subscription.dispose();
  }, { reconnectDelayMs: 0 });
  await subscription.closed;
  client.dispose();

  assert.deepEqual(events.map((event) => event.type), ["error", "assistant"]);
  assert.equal(events[0].code, "upstream_unavailable");
  assert.equal(events[0].retryable, true);
  assert.equal(streamCount, 2);
});

test("Client Tool handlers execute serially, missing handlers are terminal, and redelivery uses cached results", async () => {
  const journal = new MemoryToolJournalStorage();
  const trace = [];
  const claims = [];
  const submitted = [];
  let streamOpened = false;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: journal,
    requestRetryDelayMs: 0,
    authorize: async () => credential("tool-token", ["first", "missing", "last"]),
    handlers: {
      first: async () => {
        trace.push("first:start");
        await new Promise((resolve) => setTimeout(resolve, 15));
        trace.push("first:end");
        return { value: "first" };
      },
      last: async () => {
        trace.push("last:start");
        trace.push("last:end");
        return { value: "last" };
      },
    },
    fetch: async (input, init = {}) => {
      const path = pathOf(input);
      if (path.endsWith("/events/stream")) {
        assert.equal(streamOpened, false);
        streamOpened = true;
        return sseResponse([
          toolFrame(1, "call-1", "first"),
          toolFrame(2, "call-2", "missing"),
          toolFrame(3, "call-3", "last"),
          toolFrame(4, "call-1", "first"),
        ], init.signal, true);
      }
      if (path.endsWith("/claim")) {
        const toolCallId = path.split("/").at(-2);
        assert.equal((await journal.get(client.clientInstanceId, toolCallId)).state, "recorded");
        claims.push(path);
        return json({ status: "claimed" });
      }
      if (path.endsWith("/result")) {
        const toolCallId = path.split("/").at(-2);
        assert.match(
          (await journal.get(client.clientInstanceId, toolCallId)).state,
          /^(completed|acknowledged)$/,
        );
        submitted.push({ path, body: JSON.parse(init.body) });
        return json({ accepted: true });
      }
      throw new Error(`unexpected path ${path}`);
    },
  });

  const subscription = client.existing("session-tools").subscribe(() => {}, { reconnectDelayMs: 0 });
  await waitFor(() => submitted.length === 4);
  subscription.dispose();
  await subscription.closed;

  assert.deepEqual(trace, ["first:start", "first:end", "last:start", "last:end"]);
  assert.equal(claims.length, 3);
  assert.equal(submitted[1].body.result.error.code, "tool_handler_not_registered");
  assert.deepEqual(submitted[0].body, submitted[3].body);
  const entries = await journal.list(client.clientInstanceId);
  assert.equal(entries.length, 3);
  assert.equal(entries.every((entry) => entry.state === "acknowledged"), true);
  client.dispose();
});

test("Client Tool failures are structured and oversized or non-JSON outputs are explicit", async () => {
  const submitted = [];
  const circular = {};
  circular.self = circular;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
    authorize: async () => credential("failure-token", ["oversized", "rejected", "failed", "invalid"]),
    handlers: {
      oversized: async () => "x".repeat(20_000),
      rejected: async () => { throw new ClientToolError("user_rejected", "Not approved"); },
      failed: async () => { throw new Error("Application handler failed"); },
      invalid: async () => circular,
    },
    fetch: async (input, init = {}) => {
      const path = pathOf(input);
      if (path.endsWith("/events/stream")) {
        return sseResponse([
          toolFrame(1, "failure-1", "oversized", "failure-batch"),
          toolFrame(2, "failure-2", "rejected", "failure-batch"),
          toolFrame(3, "failure-3", "failed", "failure-batch"),
          toolFrame(4, "failure-4", "invalid", "failure-batch"),
        ], init.signal, true);
      }
      if (path.endsWith("/claim")) return json({ status: "claimed" });
      if (path.endsWith("/result")) {
        submitted.push(JSON.parse(init.body).result);
        return json({ accepted: true });
      }
      throw new Error(`unexpected path ${path}`);
    },
  });

  const subscription = client.existing("session-failures").subscribe(() => {});
  await waitFor(() => submitted.length === 4);
  subscription.dispose();
  await subscription.closed;
  // 超限结果原样提交（后端归档机制接管大结果），不再截断、不再报 too_large
  assert.equal(submitted[0].status, "success");
  assert.equal(submitted[0].truncated, undefined);
  assert.equal(String(submitted[0].output).length, 20_000);
  assert.deepEqual(submitted.slice(1).map((result) => result.error.code), [
    "user_rejected",
    "tool_handler_failed",
    "tool_result_not_json",
  ]);
  assert.equal("stack" in submitted[2].error, false);
  client.dispose();
});

test("an interrupted Client Tool remains unknown and stops the rest of its batch", async () => {
  const journal = new MemoryToolJournalStorage();
  const claims = [];
  const results = [];
  let handlerCalls = 0;
  const expired = new Date(Date.now() - 1_000).toISOString();
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: journal,
    authorize: async () => credential("timeout-token", ["slow", "later"]),
    handlers: {
      slow: async () => {
        handlerCalls += 1;
        return new Promise(() => {});
      },
      later: async () => ({ shouldNotRun: true }),
    },
    fetch: async (input, init = {}) => {
      const path = pathOf(input);
      if (path.endsWith("/events/stream")) {
        return sseResponse([
          toolFrame(1, "timeout-1", "slow", "timeout-batch", expired),
          toolFrame(2, "timeout-2", "later", "timeout-batch"),
        ], init.signal, true);
      }
      if (path.endsWith("/claim")) {
        claims.push(path);
        return json({ status: "claimed" });
      }
      if (path.endsWith("/result")) {
        results.push(path);
        return json({ accepted: true });
      }
      throw new Error(`unexpected path ${path}`);
    },
  });

  let subscription;
  const events = [];
  subscription = client.existing("session-timeout").subscribe((event) => {
    events.push(event);
    if (event.type === "timeout") subscription.dispose();
  });
  await subscription.closed;
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.equal(handlerCalls, 1);
  assert.equal(claims.length, 1);
  assert.equal(results.length, 0);
  assert.equal(events.some((event) => event.type === "timeout"), true);
  const entry = await journal.get(client.clientInstanceId, "timeout-1");
  assert.equal(entry.state, "unknown");
  client.dispose();
});

test("an uncertain Client Tool result submission remains unknown and stops the rest of its batch", async () => {
  const journal = new MemoryToolJournalStorage();
  const handlerCalls = [];
  const claims = [];
  let resultAttempts = 0;
  const client = await connect({
    baseUrl: "https://hub.example",
    sessionStorage: new MemoryStorage(),
    storage: journal,
    requestRetryDelayMs: 0,
    authorize: async () => credential("uncertain-result-token", ["first", "later"]),
    handlers: {
      first: async () => {
        handlerCalls.push("first");
        return { sideEffectApplied: true };
      },
      later: async () => {
        handlerCalls.push("later");
        return { shouldNotRun: true };
      },
    },
    fetch: async (input, init = {}) => {
      const path = pathOf(input);
      if (path.endsWith("/events/stream")) {
        return sseResponse([
          toolFrame(1, "uncertain-1", "first", "uncertain-batch"),
          toolFrame(2, "uncertain-2", "later", "uncertain-batch"),
        ], init.signal, true);
      }
      if (path.endsWith("/claim")) {
        claims.push(path);
        return json({ status: "claimed" });
      }
      if (path.endsWith("/result")) {
        resultAttempts += 1;
        return json({ code: "temporary", message: "response was lost" }, 503);
      }
      throw new Error(`unexpected path ${path}`);
    },
  });

  let subscription;
  const events = [];
  subscription = client.existing("session-uncertain-result").subscribe((event) => {
    events.push(event);
    if (event.type === "error") subscription.dispose();
  }, { reconnectDelayMs: 0 });
  await subscription.closed;
  await new Promise((resolve) => setTimeout(resolve, 10));

  assert.deepEqual(handlerCalls, ["first"]);
  assert.equal(claims.length, 1);
  assert.equal(resultAttempts, 3);
  assert.equal(events.some((event) => event.type === "error"), true);
  const entry = await journal.get(client.clientInstanceId, "uncertain-1");
  client.dispose();
  assert.equal(entry.state, "unknown");
});

test("anonymous clients persist only visitor/current Session identity and share the Session API", async () => {
  const localStorage = new MemoryStorage();
  const accessBodies = [];
  let accessCount = 0;
  const fetch = async (input, init = {}) => {
    assert.equal(pathOf(input), "/api/client/anonymous/access");
    accessBodies.push(JSON.parse(init.body));
    accessCount += 1;
    return json({
      ...credential(`anonymous-token-${accessCount}`),
      session_id: "anonymous-session",
    });
  };

  const first = await connectAnonymous({
    baseUrl: "https://hub.example",
    clientId: "public-app",
    fetch,
    localStorage,
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
  });
  assert.equal(first.currentSession().id, "anonymous-session");
  assert.equal(first.draft().id, "anonymous-session");
  await assert.rejects(first.listSessions(), (error) => (
    error instanceof AgentHubError && error.code === "anonymous_history_disabled"
  ));

  const second = await connectAnonymous({
    baseUrl: "https://hub.example",
    clientId: "public-app",
    fetch,
    localStorage,
    sessionStorage: new MemoryStorage(),
    storage: new MemoryToolJournalStorage(),
  });
  assert.equal(accessBodies[0].visitor_key, accessBodies[1].visitor_key);
  assert.notEqual(accessBodies[0].client_instance_id, accessBodies[1].client_instance_id);
  assert.equal(accessBodies[1].session_id, "anonymous-session");
  assert.equal(localStorage.entries().some(([, value]) => value.includes("anonymous-token")), false);
  first.dispose();
  second.dispose();
});

test("journal cleanup removes only acknowledged entries older than 24 hours", async () => {
  const sessionStorage = new MemoryStorage();
  sessionStorage.setItem("agent-hub:client-instance-id", "11111111-1111-4111-8111-111111111111");
  const journal = new MemoryToolJournalStorage();
  const old = Date.now() - 25 * 60 * 60_000;
  const base = {
    clientInstanceId: "11111111-1111-4111-8111-111111111111",
    sessionId: "session-1",
    toolName: "tool",
    input: {},
    createdAt: old,
    updatedAt: old,
  };
  await journal.put({ ...base, toolCallId: "old-ack", state: "acknowledged", acknowledgedAt: old });
  await journal.put({ ...base, toolCallId: "old-unknown", state: "unknown" });

  const client = await connect({
    sessionStorage,
    storage: journal,
    authorize: async () => credential("cleanup-token"),
    fetch: async () => json([]),
  });
  assert.deepEqual(
    (await journal.list("11111111-1111-4111-8111-111111111111")).map((entry) => entry.toolCallId),
    ["old-unknown"],
  );
  client.dispose();
});
