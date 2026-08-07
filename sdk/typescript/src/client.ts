import { AgentHubError, ClientToolError, SecretGrantsRequiredError } from "./errors.js";
import { IndexedDbToolJournalStorage } from "./storage.js";
import type {
  AgentHubClientOptions,
  AnonymousClientOptions,
  Authorize,
  ClientAgent,
  ClientCredential,
  ErrorSessionEvent,
  EventListOptions,
  JsonValue,
  MessagePage,
  MessagePageOptions,
  Run,
  SendOptions,
  SendResult,
  SecretGrantRequirement,
  SessionEvent,
  SessionEventListener,
  SessionListOptions,
  SessionMessage,
  SessionSummary,
  SubscribeOptions,
  ToolHandler,
  ToolHandlers,
  ToolJournalEntry,
  ToolJournalStorage,
  ToolRequestEvent,
  ToolResult,
  ToolTimeoutEvent,
} from "./types.js";

const CLIENT_INSTANCE_STORAGE_KEY = "agent-hub:client-instance-id";
const CLIENT_INSTANCE_CHANNEL_NAME = "agent-hub:client-instance:v1";
const CLIENT_INSTANCE_PROBE_MS = 40;
const CLIENT_INSTANCE_OWNER_KEY = Symbol.for("@agent-hub/client:instance-owner");
const JOURNAL_RETENTION_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_RENEWAL_WINDOW_MS = 60_000;
const DEFAULT_REQUEST_RETRY_DELAY_MS = 100;
const DEFAULT_RECONNECT_DELAY_MS = 500;
const MAX_TOOL_RESULT_BYTES = 16_000;

const PATHS = {
  anonymousAccess: "/api/client/anonymous/access",
  renew: "/api/client/renew",
  runs: "/api/client/runs",
  sessions: "/api/client/sessions",
} as const;

interface NormalizedCredential {
  token: string;
  expiresAt: number;
  authorizedToolNames: Set<string>;
  agent: ClientAgent | null;
  historyEnabled: boolean;
}

interface AuthenticatedMode {
  kind: "authenticated";
  authorize: Authorize;
}

interface AnonymousMode {
  kind: "anonymous";
  clientId: string;
  localStorage: Storage;
  visitorKey: string;
  currentSessionId: string | null;
}

type ClientMode = AuthenticatedMode | AnonymousMode;

interface SessionOperations {
  messages(sessionId: string, options: MessagePageOptions): Promise<MessagePage>;
  events(sessionId: string, options: EventListOptions): Promise<SessionEvent[]>;
  send(session: ClientSession, content: string, options: SendOptions): Promise<SendResult>;
  stop(runId: string, signal?: AbortSignal): Promise<Run>;
  openStream(sessionId: string, after: number, signal: AbortSignal): Promise<Response>;
  handleToolRequest(sessionId: string, event: ToolRequestEvent, signal: AbortSignal): Promise<ToolDispatchOutcome>;
}

interface ToolDispatchOutcome {
  blocked: boolean;
  event?: ToolTimeoutEvent | ErrorSessionEvent;
}

interface SseFrame {
  event: string;
  id?: string;
  data: string;
}

interface RequestOptions {
  signal?: AbortSignal | undefined;
  transientRetries?: number;
}

interface SessionListResponse {
  items?: unknown[];
}

interface ClaimResponse {
  status?: string;
  claim_status?: string;
  terminal?: boolean;
  result?: ToolResult;
}

interface ClientInstanceReservation {
  id: string;
  channel?: BroadcastChannel;
}

interface ClientInstanceChannelMessage {
  type: "probe" | "occupied";
  clientInstanceId: string;
  ownerToken: string;
  nonce: string;
}

function browserStorage(provided: Storage | undefined, name: "sessionStorage" | "localStorage"): Storage {
  if (provided) return provided;
  const storage = globalThis[name];
  if (!storage) throw new Error(`${name} is unavailable`);
  return storage;
}

function randomId(prefix = ""): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return `${prefix}${globalThis.crypto.randomUUID()}`;
  }
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  return `${prefix}${uuid}`;
}

function clientInstanceOwnerToken(): string {
  const scope = globalThis as unknown as Record<PropertyKey, unknown>;
  const existing = scope[CLIENT_INSTANCE_OWNER_KEY];
  if (typeof existing === "string") return existing;
  const created = randomId("owner-");
  scope[CLIENT_INSTANCE_OWNER_KEY] = created;
  return created;
}

function isClientInstanceChannelMessage(value: unknown): value is ClientInstanceChannelMessage {
  if (!value || typeof value !== "object") return false;
  const message = value as Record<string, unknown>;
  return (message.type === "probe" || message.type === "occupied")
    && typeof message.clientInstanceId === "string"
    && typeof message.ownerToken === "string"
    && typeof message.nonce === "string";
}

async function reserveClientInstanceId(storage: Storage): Promise<ClientInstanceReservation> {
  const stored = storage.getItem(CLIENT_INSTANCE_STORAGE_KEY)?.trim();
  let id = stored || randomId();
  if (!stored) storage.setItem(CLIENT_INSTANCE_STORAGE_KEY, id);
  if (typeof globalThis.BroadcastChannel !== "function") return { id };

  let channel: BroadcastChannel;
  try {
    channel = new globalThis.BroadcastChannel(CLIENT_INSTANCE_CHANNEL_NAME);
  } catch {
    return { id };
  }
  const ownerToken = clientInstanceOwnerToken();
  let probeNonce: string | undefined;
  let occupiedByAnotherTab = false;
  channel.addEventListener("message", (event: MessageEvent<unknown>) => {
    if (!isClientInstanceChannelMessage(event.data)) return;
    const message = event.data;
    if (message.type === "probe") {
      if (message.clientInstanceId === id && message.ownerToken !== ownerToken) {
        channel.postMessage({
          type: "occupied",
          clientInstanceId: id,
          ownerToken,
          nonce: message.nonce,
        } satisfies ClientInstanceChannelMessage);
      }
      return;
    }
    if (message.clientInstanceId === id
      && message.nonce === probeNonce
      && message.ownerToken !== ownerToken) {
      occupiedByAnotherTab = true;
    }
  });

  if (stored) {
    probeNonce = randomId("probe-");
    channel.postMessage({
      type: "probe",
      clientInstanceId: id,
      ownerToken,
      nonce: probeNonce,
    } satisfies ClientInstanceChannelMessage);
    await new Promise((resolve) => globalThis.setTimeout(resolve, CLIENT_INSTANCE_PROBE_MS));
    if (occupiedByAnotherTab) {
      id = randomId();
      storage.setItem(CLIENT_INSTANCE_STORAGE_KEY, id);
    }
  }
  return { id, channel };
}

function anonymousStorageKey(clientId: string, suffix: "visitor" | "session"): string {
  return `agent-hub:anonymous:${encodeURIComponent(clientId)}:${suffix}`;
}

function anonymousVisitorKey(storage: Storage, clientId: string): string {
  const key = anonymousStorageKey(clientId, "visitor");
  const stored = storage.getItem(key)?.trim();
  if (stored) return stored;
  const created = randomId("ahv_");
  storage.setItem(key, created);
  return created;
}

function normalizeCredential(
  value: ClientCredential,
  now: number,
  fallback?: NormalizedCredential,
): NormalizedCredential {
  const token = value.accessToken ?? value.access_token ?? value.token;
  if (!token?.trim()) throw new Error("authorize() did not return a Client Access Credential");

  const rawExpiration = value.expiresAt ?? value.expires_at;
  let expiresAt = typeof rawExpiration === "number" ? rawExpiration : Date.parse(rawExpiration ?? "");
  if (!Number.isFinite(expiresAt)) {
    const expiresIn = value.expiresIn ?? value.expires_in;
    if (typeof expiresIn === "number" && Number.isFinite(expiresIn) && expiresIn > 0) {
      expiresAt = now + expiresIn * 1_000;
    }
  }
  if (!Number.isFinite(expiresAt) || expiresAt <= now) {
    throw new Error("authorize() returned an invalid credential expiration");
  }

  const tools = value.authorizedTools ?? value.authorized_tools ?? value.tools ?? value.tool_names;
  const authorizedToolNames = tools
    ? new Set(tools.map((tool) => typeof tool === "string" ? tool : tool.name))
    : new Set(fallback?.authorizedToolNames ?? []);
  return {
    token,
    expiresAt,
    authorizedToolNames,
    agent: value.agent ?? fallback?.agent ?? null,
    historyEnabled: value.historyEnabled ?? value.history_enabled ?? fallback?.historyEnabled ?? false,
  };
}

function pathWithQuery(path: string, values: Record<string, string | number | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined) query.set(key, String(value));
  }
  const serialized = query.toString();
  return serialized ? `${path}?${serialized}` : path;
}

function responseMessage(body: unknown, fallback: string): { code: string; message: string } {
  if (body && typeof body === "object") {
    const record = body as Record<string, unknown>;
    const nested = record.error && typeof record.error === "object"
      ? record.error as Record<string, unknown>
      : undefined;
    const code = record.code ?? nested?.code;
    const message = record.message ?? nested?.message;
    return {
      code: typeof code === "string" ? code : "request_failed",
      message: typeof message === "string" ? message : fallback,
    };
  }
  return { code: "request_failed", message: fallback };
}

function secretGrantRequirements(
  status: number,
  body: unknown,
): SecretGrantRequirement[] | undefined {
  if (status !== 428 || !isRecord(body) || !isRecord(body.details)) return undefined;
  const raw = body.details.secret_grants_required;
  if (!Array.isArray(raw)) return undefined;
  const requirements: SecretGrantRequirement[] = [];
  for (const item of raw) {
    if (!isRecord(item) || typeof item.name !== "string" || typeof item.kind !== "string") {
      return undefined;
    }
    const description = item.description;
    requirements.push({
      name: item.name,
      kind: item.kind,
      ...(typeof description === "string" ? { description } : {}),
    });
  }
  return requirements.length > 0 ? requirements : undefined;
}

async function parseBody(response: Response): Promise<unknown> {
  if (response.status === 204) return undefined;
  const text = await response.text();
  if (!text) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

function combinedSignal(...signals: (AbortSignal | undefined)[]): AbortSignal {
  const available = signals.filter((signal): signal is AbortSignal => signal !== undefined);
  if (available.length === 1) return available[0]!;
  return AbortSignal.any(available);
}

function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = globalThis.setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      globalThis.clearTimeout(timer);
      reject(signal.reason);
    }, { once: true });
  });
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function notifyListener(listener: SessionEventListener, event: SessionEvent): void {
  try {
    listener(event);
  } catch {
    // Subscriber failures do not change stream cursors or Client Tool execution.
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function stringField(record: Record<string, unknown>, ...keys: string[]): string | undefined {
  for (const key of keys) {
    if (typeof record[key] === "string") return record[key];
  }
  return undefined;
}

function numberField(record: Record<string, unknown>, ...keys: string[]): number | undefined {
  for (const key of keys) {
    if (typeof record[key] === "number" && Number.isFinite(record[key])) return record[key];
  }
  return undefined;
}

function sessionIdFromResponse(value: unknown): string | undefined {
  if (!isRecord(value)) return undefined;
  const direct = stringField(value, "session_id", "sessionId", "integration_session_id", "hub_session_id");
  if (direct) return direct;
  if (isRecord(value.session)) return stringField(value.session, "id", "session_id");
  if (isRecord(value.run)) return sessionIdFromResponse(value.run);
  return undefined;
}

function runFromResponse(value: unknown): Run {
  const candidate = isRecord(value) && isRecord(value.run) ? value.run : value;
  if (!isRecord(candidate) || typeof candidate.id !== "string") {
    throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Run");
  }
  return candidate as unknown as Run;
}

function messageFromResponse(value: unknown): SessionMessage | undefined {
  if (!isRecord(value) || !isRecord(value.message)) return undefined;
  return value.message as unknown as SessionMessage;
}

function eventRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function eventPayload(record: Record<string, unknown>): Record<string, unknown> {
  return isRecord(record.payload) ? record.payload : {};
}

function normalizeEvent(frame: SseFrame, cursor: number): SessionEvent {
  let decoded: unknown;
  try {
    decoded = JSON.parse(frame.data) as unknown;
  } catch {
    return {
      type: "error",
      sequence: cursor,
      code: "invalid_sse_event",
      message: "Agent Hub sent an invalid event payload",
      retryable: false,
      raw: frame.data,
    };
  }
  const record = eventRecord(decoded);
  const payload = eventPayload(record);
  const sequence = (numberField(record, "seq", "sequence") ?? Number(frame.id)) || cursor;
  const eventId = stringField(record, "event_id") ?? frame.id;
  const runId = stringField(record, "run_id") ?? stringField(payload, "run_id");
  const rawType = stringField(record, "type", "event_type") ?? frame.event;
  const common = {
    sequence,
    ...(eventId ? { eventId } : {}),
    ...(runId ? { runId } : {}),
    ...(typeof record.created_at === "string" ? { createdAt: record.created_at } : {}),
    raw: decoded,
  };

  if (["tool_request", "client_tool_request", "integration_tool_request"].includes(rawType)) {
    const toolCallId = stringField(record, "tool_call_id") ?? stringField(payload, "tool_call_id", "id");
    const toolName = stringField(record, "tool_name", "name") ?? stringField(payload, "tool_name", "name");
    if (!toolCallId || !toolName) {
      return {
        ...common,
        type: "error",
        code: "invalid_tool_request",
        message: "Agent Hub sent an incomplete Client Tool request",
        retryable: false,
      };
    }
    const input = (record.input ?? record.arguments ?? payload.input ?? payload.arguments ?? {}) as JsonValue;
    const batchId = stringField(record, "batch_id") ?? stringField(payload, "batch_id");
    const expiresAt = stringField(record, "expires_at") ?? stringField(payload, "expires_at");
    return {
      ...common,
      type: "tool_request",
      toolCallId,
      toolName,
      input,
      ...(batchId ? { batchId } : {}),
      ...(expiresAt ? { expiresAt } : {}),
    };
  }

  if (["tool_result", "client_tool_result"].includes(rawType)) {
    const toolCallId = stringField(record, "tool_call_id") ?? stringField(payload, "tool_call_id", "id") ?? "";
    const result = (record.result ?? payload.result) as ToolResult;
    const toolName = stringField(record, "tool_name") ?? stringField(payload, "tool_name");
    const elapsedMs = numberField(record, "elapsed_ms") ?? numberField(payload, "elapsed_ms");
    return {
      ...common,
      type: "tool_result",
      toolCallId,
      result,
      ...(toolName ? { toolName } : {}),
      ...(elapsedMs !== undefined ? { elapsedMs } : {}),
    };
  }

  if (["timeout", "tool_timeout", "client_tool_timeout"].includes(rawType)) {
    const toolName = stringField(record, "tool_name") ?? stringField(payload, "tool_name");
    return {
      ...common,
      type: "timeout",
      toolCallId: stringField(record, "tool_call_id") ?? stringField(payload, "tool_call_id") ?? "",
      message: stringField(record, "message") ?? stringField(payload, "message") ?? "Client Tool invocation timed out",
      ...(toolName ? { toolName } : {}),
    };
  }

  if (rawType === "error" || rawType.endsWith("_error")) {
    return {
      ...common,
      type: "error",
      code: stringField(record, "code") ?? stringField(payload, "code") ?? rawType,
      message: stringField(record, "message", "content") ?? stringField(payload, "message") ?? "Agent Hub reported an error",
      retryable: Boolean(record.retryable ?? payload.retryable),
    };
  }

  if (rawType === "message" || rawType === "assistant") {
    const role = stringField(record, "role") ?? stringField(payload, "role");
    return {
      ...common,
      type: rawType,
      content: stringField(record, "content") ?? stringField(payload, "content") ?? null,
      ...(role ? { role } : {}),
    };
  }

  return {
    ...common,
    type: "event",
    eventType: rawType,
    ...(typeof record.content === "string" || record.content === null ? { content: record.content } : {}),
  };
}

async function* readSse(response: Response): AsyncGenerator<SseFrame> {
  if (!response.ok) {
    const body = await parseBody(response);
    const error = responseMessage(body, `Session event stream failed with status ${response.status}`);
    throw new AgentHubError(response.status, error.code, error.message, body);
  }
  if (!response.body) throw new AgentHubError(response.status, "stream_unavailable", "Session event stream has no body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n").replaceAll("\r", "\n");
      let boundary = buffer.indexOf("\n\n");
      while (boundary >= 0) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const frame = parseSseBlock(block);
        if (frame) yield frame;
        boundary = buffer.indexOf("\n\n");
      }
      if (done) {
        const frame = parseSseBlock(buffer);
        if (frame) yield frame;
        break;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

function parseSseBlock(block: string): SseFrame | undefined {
  if (!block.trim()) return undefined;
  let event = "message";
  let id: string | undefined;
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.startsWith(":")) continue;
    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    const rawValue = separator < 0 ? "" : line.slice(separator + 1);
    const value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
    if (field === "event") event = value;
    else if (field === "id") id = value;
    else if (field === "data") data.push(value);
  }
  if (data.length === 0) return undefined;
  return { event, ...(id !== undefined ? { id } : {}), data: data.join("\n") };
}

function isToolResult(value: unknown): value is ToolResult {
  if (!isRecord(value)) return false;
  if (value.status === "success") return "output" in value;
  return value.status === "error"
    && isRecord(value.error)
    && typeof value.error.code === "string"
    && typeof value.error.message === "string"
    && typeof value.error.retryable === "boolean";
}

function isJsonValue(value: unknown, ancestors = new Set<object>()): value is JsonValue {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object") return false;
  if (ancestors.has(value)) return false;
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    return false;
  }
  ancestors.add(value);
  const valid = Array.isArray(value)
    ? value.every((item) => isJsonValue(item, ancestors))
    : Object.values(value).every((item) => isJsonValue(item, ancestors));
  ancestors.delete(value);
  return valid;
}

function handlerError(error: unknown): ToolResult {
  if (error instanceof ClientToolError) {
    return {
      status: "error",
      error: { code: error.code, message: error.message, retryable: error.retryable },
    };
  }
  return {
    status: "error",
    error: {
      code: "tool_handler_failed",
      message: error instanceof Error ? error.message : "Client Tool handler failed",
      retryable: false,
    },
  };
}

function checkedToolResult(value: unknown): ToolResult {
  let result: ToolResult;
  if (isToolResult(value)) {
    result = value;
  } else {
    if (isJsonValue(value)) {
      result = { status: "success", output: value as JsonValue };
    } else {
      result = {
        status: "error",
        error: { code: "tool_result_not_json", message: "Client Tool result is not valid JSON", retryable: false },
      };
    }
  }
  if (!isJsonValue(result)) {
    result = {
      status: "error",
      error: { code: "tool_result_not_json", message: "Client Tool result is not valid JSON", retryable: false },
    };
  }
  const serialized = JSON.stringify(result);
  if (serialized === undefined) {
    return {
      status: "error",
      error: { code: "tool_result_not_json", message: "Client Tool result is not valid JSON", retryable: false },
    };
  }
  if (new TextEncoder().encode(serialized).byteLength > MAX_TOOL_RESULT_BYTES) {
    return {
      status: "error",
      error: {
        code: "tool_result_too_large",
        message: `Client Tool result exceeds ${MAX_TOOL_RESULT_BYTES} bytes`,
        retryable: false,
      },
    };
  }
  return result;
}

export class SessionSubscription {
  readonly closed: Promise<void>;
  readonly #controller: AbortController;

  constructor(controller: AbortController, closed: Promise<void>) {
    this.#controller = controller;
    this.closed = closed;
  }

  dispose(): void {
    this.#controller.abort(new DOMException("Subscription disposed", "AbortError"));
  }

  unsubscribe(): void {
    this.dispose();
  }
}

export class ClientSession {
  #id: string | null;
  readonly #operations: SessionOperations;
  readonly #subscriptions = new Set<SessionSubscription>();
  readonly #disposeController = new AbortController();
  readonly #blockedBatches = new Set<string>();
  #toolQueue = Promise.resolve();
  #sendQueue = Promise.resolve();
  #disposed = false;

  constructor(operations: SessionOperations, id: string | null) {
    this.#operations = operations;
    this.#id = id;
  }

  get id(): string | null {
    return this.#id;
  }

  get isDraft(): boolean {
    return this.#id === null;
  }

  async messages(options: MessagePageOptions = {}): Promise<SessionMessage[]> {
    return (await this.messagePage(options)).items;
  }

  async messagePage(options: MessagePageOptions = {}): Promise<MessagePage> {
    this.#assertUsable();
    if (this.#id === null) return { items: [], nextBeforeSequence: null };
    return this.#operations.messages(this.#id, {
      ...options,
      signal: combinedSignal(this.#disposeController.signal, options.signal),
    });
  }

  async events(options: EventListOptions = {}): Promise<SessionEvent[]> {
    this.#assertUsable();
    if (this.#id === null) return [];
    return this.#operations.events(this.#id, {
      ...options,
      signal: combinedSignal(this.#disposeController.signal, options.signal),
    });
  }

  async send(content: string, options: SendOptions = {}): Promise<SendResult> {
    this.#assertUsable();
    const pending = this.#sendQueue.then(() => this.#operations.send(this, content, {
      ...options,
      signal: combinedSignal(this.#disposeController.signal, options.signal),
    }));
    this.#sendQueue = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async stop(runId: string, signal?: AbortSignal): Promise<Run> {
    this.#assertUsable();
    return this.#operations.stop(runId, combinedSignal(this.#disposeController.signal, signal));
  }

  subscribe(listener: SessionEventListener, options: SubscribeOptions = {}): SessionSubscription {
    this.#assertUsable();
    if (this.#id === null) throw new Error("A draft Session cannot subscribe before its first message");
    const sessionId = this.#id;
    const controller = new AbortController();
    const signal = combinedSignal(this.#disposeController.signal, controller.signal, options.signal);
    const reconnectDelayMs = options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS;
    let cursor = options.after ?? 0;

    const closed = (async () => {
      while (!signal.aborted) {
        try {
          const response = await this.#operations.openStream(sessionId, cursor, signal);
          for await (const frame of readSse(response)) {
            if (signal.aborted) break;
            const event = normalizeEvent(frame, cursor);
            if (event.sequence > 0 && event.sequence <= cursor) continue;
            cursor = Math.max(cursor, event.sequence);
            notifyListener(listener, event);
            if (event.type === "tool_request") this.#enqueueTool(event, listener);
          }
        } catch (error) {
          if (signal.aborted || isAbort(error)) break;
          const status = error instanceof AgentHubError ? error.status : 0;
          notifyListener(listener, {
            type: "error",
            sequence: cursor,
            code: error instanceof AgentHubError ? error.code : "stream_disconnected",
            message: error instanceof Error ? error.message : "Session event stream disconnected",
            retryable: status === 0 || status >= 500,
            raw: error,
          });
          if (status >= 400 && status < 500) break;
        }
        if (!signal.aborted) await abortableDelay(reconnectDelayMs, signal).catch(() => undefined);
      }
    })().finally(() => {
      this.#subscriptions.delete(subscription);
    });
    const subscription = new SessionSubscription(controller, closed);
    this.#subscriptions.add(subscription);
    return subscription;
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#disposeController.abort(new DOMException("Session disposed", "AbortError"));
    for (const subscription of this.#subscriptions) subscription.dispose();
    this.#subscriptions.clear();
  }

  /** @internal */
  materialize(id: string): void {
    if (this.#id !== null && this.#id !== id) throw new Error("Session ID cannot change");
    this.#id = id;
  }

  #enqueueTool(event: ToolRequestEvent, listener: SessionEventListener): void {
    const batchId = event.batchId ?? event.runId ?? event.toolCallId;
    this.#toolQueue = this.#toolQueue.catch(() => undefined).then(async () => {
      if (this.#blockedBatches.has(batchId)) return;
      try {
        const outcome = await this.#operations.handleToolRequest(
          this.#id!,
          event,
          this.#disposeController.signal,
        );
        if (outcome.event) notifyListener(listener, outcome.event);
        if (outcome.blocked) this.#blockedBatches.add(batchId);
      } catch (error) {
        notifyListener(listener, {
          type: "error",
          sequence: event.sequence,
          code: "tool_dispatch_failed",
          message: error instanceof Error ? error.message : "Client Tool dispatch failed",
          retryable: false,
          raw: error,
        });
        this.#blockedBatches.add(batchId);
      }
    });
  }

  #assertUsable(): void {
    if (this.#disposed) throw new Error("Session is disposed");
  }
}

export { ClientSession as Session };

export class AgentHubClient {
  readonly clientInstanceId: string;
  readonly sessions: {
    list: (options?: SessionListOptions) => Promise<SessionSummary[]>;
    existing: (sessionId: string) => ClientSession;
    draft: () => ClientSession;
  };

  readonly #baseUrl: string;
  readonly #fetch: typeof fetch;
  readonly #mode: ClientMode;
  readonly #journal: ToolJournalStorage;
  readonly #handlers = new Map<string, ToolHandler>();
  readonly #sessionCache = new Map<string, ClientSession>();
  readonly #allSessions = new Set<ClientSession>();
  readonly #toolOperations = new Map<string, Promise<ToolDispatchOutcome>>();
  readonly #lifetime = new AbortController();
  readonly #renewalWindowMs: number;
  readonly #requestRetryDelayMs: number;
  readonly #sessionOperations: SessionOperations;
  readonly #clientInstanceChannel: BroadcastChannel | undefined;
  #credential: NormalizedCredential | undefined;
  #credentialOperation: Promise<void> | undefined;
  #renewalTimer?: number;
  #disposed = false;

  private constructor(
    options: AgentHubClientOptions | AnonymousClientOptions,
    mode: ClientMode,
    journal: ToolJournalStorage,
    instanceId: string,
    instanceChannel?: BroadcastChannel,
  ) {
    this.clientInstanceId = instanceId;
    this.#baseUrl = options.baseUrl?.replace(/\/+$/, "") ?? "";
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#mode = mode;
    this.#journal = journal;
    this.#clientInstanceChannel = instanceChannel;
    this.#renewalWindowMs = options.renewalWindowMs ?? DEFAULT_RENEWAL_WINDOW_MS;
    this.#requestRetryDelayMs = options.requestRetryDelayMs ?? DEFAULT_REQUEST_RETRY_DELAY_MS;
    this.#sessionOperations = {
      messages: (sessionId, requestOptions) => this.#messages(sessionId, requestOptions),
      events: (sessionId, requestOptions) => this.#events(sessionId, requestOptions),
      send: (session, content, requestOptions) => this.#send(session, content, requestOptions),
      stop: (runId, signal) => this.stop(runId, signal),
      openStream: (sessionId, after, signal) => this.#openStream(sessionId, after, signal),
      handleToolRequest: (sessionId, event, signal) => this.#handleToolRequest(sessionId, event, signal),
    };
    this.sessions = {
      list: (requestOptions) => this.listSessions(requestOptions),
      existing: (sessionId) => this.existing(sessionId),
      draft: () => this.draft(),
    };
  }

  static async connect(options: AgentHubClientOptions): Promise<AgentHubClient> {
    const instanceStorage = browserStorage(options.sessionStorage, "sessionStorage");
    const reservation = await reserveClientInstanceId(instanceStorage);
    const journal = options.storage ?? new IndexedDbToolJournalStorage();
    const client = new AgentHubClient(
      options,
      { kind: "authenticated", authorize: options.authorize },
      journal,
      reservation.id,
      reservation.channel,
    );
    try {
      await client.#initialize(options.handlers);
      return client;
    } catch (error) {
      client.dispose();
      throw error;
    }
  }

  static async connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient> {
    if (!options.clientId.trim()) throw new Error("clientId is required");
    const instanceStorage = browserStorage(options.sessionStorage, "sessionStorage");
    const localStorage = browserStorage(options.localStorage, "localStorage");
    const reservation = await reserveClientInstanceId(instanceStorage);
    const visitorKey = anonymousVisitorKey(localStorage, options.clientId);
    const currentSessionId = localStorage.getItem(anonymousStorageKey(options.clientId, "session"));
    const journal = options.storage ?? new IndexedDbToolJournalStorage();
    const client = new AgentHubClient(
      options,
      {
        kind: "anonymous",
        clientId: options.clientId,
        localStorage,
        visitorKey,
        currentSessionId,
      },
      journal,
      reservation.id,
      reservation.channel,
    );
    try {
      await client.#initialize(options.handlers);
      return client;
    } catch (error) {
      client.dispose();
      throw error;
    }
  }

  get authorizedToolNames(): ReadonlySet<string> {
    return new Set(this.#credential?.authorizedToolNames ?? []);
  }

  get accessToken(): string | null {
    return this.#credential?.token ?? null;
  }

  get agent(): ClientAgent | null {
    const agent = this.#credential?.agent;
    return agent ? { ...agent } : null;
  }

  get historyEnabled(): boolean {
    return this.#credential?.historyEnabled ?? false;
  }

  get isAnonymous(): boolean {
    return this.#mode.kind === "anonymous";
  }

  async listSessions(options: SessionListOptions = {}): Promise<SessionSummary[]> {
    this.#assertUsable();
    if (this.#mode.kind === "anonymous") {
      throw new AgentHubError(403, "anonymous_history_disabled", "Anonymous clients cannot list Sessions");
    }
    const value = await this.#requestJson<unknown>(pathWithQuery(PATHS.sessions, {
      cursor: options.cursor,
      limit: options.limit,
    }), {}, { signal: options.signal });
    const items = Array.isArray(value) ? value : (value as SessionListResponse | undefined)?.items;
    if (!Array.isArray(items)) throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Session list");
    return items.map((item) => {
      if (!isRecord(item)) throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Session");
      const id = stringField(item, "id", "session_id");
      if (!id) throw new AgentHubError(500, "invalid_response", "Agent Hub returned a Session without an ID");
      return { ...item, id } as SessionSummary;
    });
  }

  existing(sessionId: string): ClientSession {
    this.#assertUsable();
    const normalized = sessionId.trim();
    if (!normalized) throw new Error("sessionId is required");
    let session = this.#sessionCache.get(normalized);
    if (!session) {
      session = new ClientSession(this.#sessionOperations, normalized);
      this.#sessionCache.set(normalized, session);
      this.#allSessions.add(session);
    }
    return session;
  }

  draft(): ClientSession {
    this.#assertUsable();
    if (this.#mode.kind === "anonymous" && this.#mode.currentSessionId) {
      return this.existing(this.#mode.currentSessionId);
    }
    const session = new ClientSession(this.#sessionOperations, null);
    this.#allSessions.add(session);
    return session;
  }

  currentSession(): ClientSession | null {
    this.#assertUsable();
    if (this.#mode.kind !== "anonymous" || !this.#mode.currentSessionId) return null;
    return this.existing(this.#mode.currentSessionId);
  }

  registerTool(name: string, handler: ToolHandler): void {
    this.#assertUsable();
    if (!this.#credential?.authorizedToolNames.has(name)) {
      throw new Error(`Client Tool \"${name}\" is not authorized`);
    }
    this.#handlers.set(name, handler);
  }

  registerTools(handlers: ToolHandlers): void {
    for (const [name, handler] of Object.entries(handlers)) this.registerTool(name, handler);
  }

  unregisterTool(name: string): void {
    this.#handlers.delete(name);
  }

  async reauthorize(): Promise<void> {
    this.#assertUsable();
    if (this.#credentialOperation) await this.#credentialOperation;
    await this.#startCredentialOperation(() => this.#authorizeFresh());
  }

  async stop(runId: string, signal?: AbortSignal): Promise<Run> {
    this.#assertUsable();
    if (!runId.trim()) throw new Error("runId is required");
    const value = await this.#requestJson<unknown>(
      `${PATHS.runs}/${encodeURIComponent(runId)}/stop`,
      { method: "POST", body: "{}" },
      { signal },
    );
    return runFromResponse(value);
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#credential = undefined;
    if (this.#renewalTimer !== undefined) globalThis.clearTimeout(this.#renewalTimer);
    this.#clientInstanceChannel?.close();
    this.#lifetime.abort(new DOMException("Client disposed", "AbortError"));
    for (const session of this.#allSessions) session.dispose();
    this.#sessionCache.clear();
    this.#allSessions.clear();
    this.#handlers.clear();
  }

  async #initialize(handlers: ToolHandlers | undefined): Promise<void> {
    await this.#authorizeFresh();
    if (handlers) this.registerTools(handlers);
    await this.#cleanupJournal();
  }

  async #authorizeFresh(): Promise<void> {
    let value: ClientCredential;
    if (this.#mode.kind === "authenticated") {
      value = await this.#mode.authorize({
        clientInstanceId: this.clientInstanceId,
        signal: this.#lifetime.signal,
      });
    } else {
      const body: Record<string, string> = {
        client_id: this.#mode.clientId,
        visitor_key: this.#mode.visitorKey,
        client_instance_id: this.clientInstanceId,
      };
      if (this.#mode.currentSessionId) body.session_id = this.#mode.currentSessionId;
      value = await this.#publicRequest<ClientCredential>(PATHS.anonymousAccess, {
        method: "POST",
        body: JSON.stringify(body),
        signal: this.#lifetime.signal,
      });
      const recoveredSessionId = value.sessionId ?? value.session_id ?? value.hub_session_id;
      if (recoveredSessionId) this.#rememberAnonymousSession(recoveredSessionId);
    }
    this.#credential = normalizeCredential(value, Date.now());
    this.#scheduleRenewal();
  }

  async #renewCredential(): Promise<void> {
    const current = this.#credential;
    if (!current) return this.#authorizeFresh();
    const response = await this.#fetch(this.#url(PATHS.renew), {
      method: "POST",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${current.token}`,
        "Content-Type": "application/json",
      },
      body: "{}",
      credentials: "omit",
      signal: this.#lifetime.signal,
    });
    if (response.status === 401) {
      await parseBody(response);
      await this.#authorizeFresh();
      return;
    }
    const value = await this.#checkedBody<ClientCredential>(response);
    this.#credential = normalizeCredential(value, Date.now(), current);
    this.#scheduleRenewal();
  }

  #startCredentialOperation(operation: () => Promise<void>): Promise<void> {
    if (this.#credentialOperation) return this.#credentialOperation;
    const pending = operation().finally(() => {
      if (this.#credentialOperation === pending) this.#credentialOperation = undefined;
    });
    this.#credentialOperation = pending;
    return pending;
  }

  async #ensureCredential(): Promise<void> {
    const credential = this.#credential;
    if (!credential) {
      await this.#startCredentialOperation(() => this.#authorizeFresh());
      return;
    }
    if (credential.expiresAt - Date.now() <= this.#renewalWindowMs) {
      await this.#startCredentialOperation(() => this.#renewCredential());
    }
  }

  #scheduleRenewal(): void {
    if (this.#renewalTimer !== undefined) globalThis.clearTimeout(this.#renewalTimer);
    const credential = this.#credential;
    if (!credential || this.#disposed) return;
    const delay = Math.max(0, credential.expiresAt - Date.now() - this.#renewalWindowMs);
    this.#renewalTimer = globalThis.setTimeout(() => {
      void this.#startCredentialOperation(() => this.#renewCredential()).catch(() => undefined);
    }, delay);
  }

  async #requestJson<T>(path: string, init: RequestInit, options: RequestOptions = {}): Promise<T> {
    const response = await this.#authorizedFetch(path, init, options);
    return this.#checkedBody<T>(response);
  }

  async #authorizedFetch(path: string, init: RequestInit, options: RequestOptions): Promise<Response> {
    const transientRetries = options.transientRetries ?? 0;
    let transientAttempt = 0;
    let authorizationRetried = false;
    while (true) {
      await this.#ensureCredential();
      const credential = this.#credential;
      if (!credential) throw new Error("Client Access Credential is unavailable");
      let response: Response;
      try {
        response = await this.#fetch(this.#url(path), {
          ...init,
          headers: {
            Accept: "application/json",
            ...(init.body !== undefined ? { "Content-Type": "application/json" } : {}),
            ...init.headers,
            Authorization: `Bearer ${credential.token}`,
          },
          credentials: "omit",
          signal: combinedSignal(this.#lifetime.signal, options.signal, init.signal ?? undefined),
        });
      } catch (error) {
        if (transientAttempt >= transientRetries || isAbort(error)) throw error;
        transientAttempt += 1;
        await abortableDelay(this.#requestRetryDelayMs, combinedSignal(this.#lifetime.signal, options.signal));
        continue;
      }
      if (response.status === 401 && !authorizationRetried) {
        authorizationRetried = true;
        await parseBody(response);
        await this.#startCredentialOperation(() => this.#renewCredential());
        continue;
      }
      if ([408, 425, 429].includes(response.status) || response.status >= 500) {
        if (transientAttempt < transientRetries) {
          transientAttempt += 1;
          await parseBody(response);
          await abortableDelay(this.#requestRetryDelayMs, combinedSignal(this.#lifetime.signal, options.signal));
          continue;
        }
      }
      return response;
    }
  }

  async #publicRequest<T>(path: string, init: RequestInit): Promise<T> {
    const response = await this.#fetch(this.#url(path), {
      ...init,
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        ...init.headers,
      },
      credentials: "omit",
    });
    return this.#checkedBody<T>(response);
  }

  async #checkedBody<T>(response: Response): Promise<T> {
    const body = await parseBody(response);
    if (!response.ok) {
      const error = responseMessage(body, `Agent Hub request failed with status ${response.status}`);
      const requirements = secretGrantRequirements(response.status, body);
      if (requirements) {
        throw new SecretGrantsRequiredError(error.message, requirements, body);
      }
      throw new AgentHubError(response.status, error.code, error.message, body);
    }
    return body as T;
  }

  async #messages(sessionId: string, options: MessagePageOptions): Promise<MessagePage> {
    const limit = options.limit ?? 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error("message limit must be between 1 and 100");
    const value = await this.#requestJson<unknown>(pathWithQuery(
      `${PATHS.sessions}/${encodeURIComponent(sessionId)}/messages`,
      { before_sequence: options.beforeSequence, limit },
    ), {}, { signal: options.signal });
    const rawItems = Array.isArray(value) ? value : isRecord(value) && Array.isArray(value.items) ? value.items : undefined;
    if (!rawItems) throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid message page");
    const items = rawItems as SessionMessage[];
    const minimum = items.reduce<number | null>((current, item) => (
      typeof item.sequence === "number" && (current === null || item.sequence < current) ? item.sequence : current
    ), null);
    return { items, nextBeforeSequence: items.length === limit ? minimum : null };
  }

  async #events(sessionId: string, options: EventListOptions): Promise<SessionEvent[]> {
    const after = options.after ?? 0;
    if (!Number.isInteger(after) || after < 0) throw new Error("event cursor must be a non-negative integer");
    const value = await this.#requestJson<unknown>(pathWithQuery(
      `${PATHS.sessions}/${encodeURIComponent(sessionId)}/events`,
      { after: after > 0 ? after : undefined },
    ), {}, { signal: options.signal });
    if (!Array.isArray(value)) {
      throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid event list");
    }
    return value.map((event) => normalizeEvent({
      event: "session_event",
      data: JSON.stringify(event),
    }, after));
  }

  async #send(session: ClientSession, content: string, options: SendOptions): Promise<SendResult> {
    const normalized = content.trim();
    if (!normalized) throw new Error("message content is required");
    const key = options.clientMessageKey?.trim() || randomId("msg_");
    const body: Record<string, string | string[]> = { message: normalized, client_message_key: key };
    if (session.id) body.session_id = session.id;
    if (options.attachmentIds && options.attachmentIds.length > 0) body.attachment_ids = [...options.attachmentIds];
    const value = await this.#requestJson<unknown>(PATHS.runs, {
      method: "POST",
      body: JSON.stringify(body),
    }, { signal: options.signal, transientRetries: 1 });
    const run = runFromResponse(value);
    const resolvedSessionId = session.id ?? sessionIdFromResponse(value) ?? sessionIdFromResponse(run);
    if (!resolvedSessionId) {
      throw new AgentHubError(500, "invalid_response", "Agent Hub did not return the created Session ID");
    }
    session.materialize(resolvedSessionId);
    this.#sessionCache.set(resolvedSessionId, session);
    if (this.#mode.kind === "anonymous") this.#rememberAnonymousSession(resolvedSessionId);
    const message = messageFromResponse(value);
    return {
      run,
      ...(message ? { message } : {}),
      sessionId: resolvedSessionId,
      clientMessageKey: key,
      raw: value,
    };
  }

  async #openStream(sessionId: string, after: number, signal: AbortSignal): Promise<Response> {
    const path = pathWithQuery(`${PATHS.sessions}/${encodeURIComponent(sessionId)}/events/stream`, {
      after: after > 0 ? after : undefined,
    });
    return this.#authorizedFetch(path, {
      headers: after > 0 ? { "Last-Event-ID": String(after) } : {},
    }, { signal });
  }

  #handleToolRequest(
    sessionId: string,
    event: ToolRequestEvent,
    signal: AbortSignal,
  ): Promise<ToolDispatchOutcome> {
    const active = this.#toolOperations.get(event.toolCallId);
    if (active) return active;
    const operation = this.#dispatchTool(sessionId, event, signal).finally(() => {
      if (this.#toolOperations.get(event.toolCallId) === operation) this.#toolOperations.delete(event.toolCallId);
    });
    this.#toolOperations.set(event.toolCallId, operation);
    return operation;
  }

  async #dispatchTool(
    sessionId: string,
    event: ToolRequestEvent,
    signal: AbortSignal,
  ): Promise<ToolDispatchOutcome> {
    const existing = await this.#journal.get(this.clientInstanceId, event.toolCallId);
    if (existing?.result && (existing.state === "completed" || existing.state === "acknowledged")) {
      await this.#submitToolResult(event.toolCallId, existing.result);
      await this.#acknowledgeEntry(existing);
      return { blocked: false };
    }
    if (existing?.state === "acknowledged") return { blocked: false };

    const now = Date.now();
    const entry: ToolJournalEntry = existing ?? {
      clientInstanceId: this.clientInstanceId,
      toolCallId: event.toolCallId,
      sessionId,
      ...(event.runId ? { runId: event.runId } : {}),
      toolName: event.toolName,
      input: event.input,
      state: "recorded",
      createdAt: now,
      updatedAt: now,
    };
    if (!existing) await this.#journal.put(entry);

    let claim: ClaimResponse;
    try {
      claim = await this.#requestJson<ClaimResponse>(
        `/api/client/tool-calls/${encodeURIComponent(event.toolCallId)}/claim`,
        { method: "POST", body: "{}" },
        { signal, transientRetries: 1 },
      );
    } catch (error) {
      return {
        blocked: true,
        event: {
          type: "error",
          sequence: event.sequence,
          code: error instanceof AgentHubError ? error.code : "tool_claim_failed",
          message: error instanceof Error ? error.message : "Client Tool claim failed",
          retryable: true,
          raw: error,
          toolCallId: event.toolCallId,
        } as ErrorSessionEvent,
      };
    }

    const claimStatus = claim.status ?? claim.claim_status ?? (claim.terminal ? "terminal" : "claimed");
    if (claim.terminal || ["completed", "failed", "expired", "timed_out", "terminal"].includes(claimStatus)) {
      const terminalEntry: ToolJournalEntry = {
        ...entry,
        ...(claim.result ? { result: claim.result } : {}),
        state: "acknowledged",
        updatedAt: Date.now(),
        acknowledgedAt: Date.now(),
      };
      await this.#journal.put(terminalEntry);
      return { blocked: false };
    }

    if (existing && ["executing", "unknown"].includes(existing.state)) {
      return { blocked: true };
    }

    const executing: ToolJournalEntry = { ...entry, state: "executing", updatedAt: Date.now() };
    await this.#journal.put(executing);
    const handler = this.#handlers.get(event.toolName);
    let result: ToolResult;
    if (!handler) {
      result = {
        status: "error",
        error: {
          code: "tool_handler_not_registered",
          message: `No handler is registered for Client Tool \"${event.toolName}\"`,
          retryable: false,
        },
      };
    } else {
      const controller = new AbortController();
      const expiresAt = event.expiresAt ? Date.parse(event.expiresAt) : Date.now() + 5 * 60_000;
      const remaining = Math.max(0, expiresAt - Date.now());
      let deadlineTimer: number | undefined;
      const deadline = new Promise<never>((_resolve, reject) => {
        deadlineTimer = globalThis.setTimeout(() => {
          controller.abort(new DOMException("Client Tool deadline reached", "TimeoutError"));
          reject(new DOMException("Client Tool deadline reached", "TimeoutError"));
        }, remaining);
      });
      const interrupted = new Promise<never>((_resolve, reject) => {
        const interrupt = () => reject(new DOMException("Client Tool execution interrupted", "AbortError"));
        if (signal.aborted || this.#lifetime.signal.aborted) interrupt();
        else {
          signal.addEventListener("abort", interrupt, { once: true });
          this.#lifetime.signal.addEventListener("abort", interrupt, { once: true });
        }
      });
      try {
        const output = await Promise.race([
          handler(event.input, {
            toolCallId: event.toolCallId,
            sessionId,
            ...(event.runId ? { runId: event.runId } : {}),
            signal: combinedSignal(controller.signal, signal, this.#lifetime.signal),
          }),
          deadline,
          interrupted,
        ]);
        controller.abort();
        result = checkedToolResult(output);
      } catch (error) {
        controller.abort();
        if (error instanceof DOMException && error.name === "TimeoutError") {
          await this.#journal.put({ ...executing, state: "unknown", updatedAt: Date.now() });
          return {
            blocked: true,
            event: {
              type: "timeout",
              sequence: event.sequence,
              toolCallId: event.toolCallId,
              toolName: event.toolName,
              message: "Client Tool invocation reached its deadline",
              raw: event.raw,
              ...(event.runId ? { runId: event.runId } : {}),
            },
          };
        }
        if (isAbort(error)) {
          await this.#journal.put({ ...executing, state: "unknown", updatedAt: Date.now() });
          return {
            blocked: true,
            event: {
              type: "error",
              sequence: event.sequence,
              code: "tool_execution_interrupted",
              message: "Client Tool execution was interrupted and will not be replayed automatically",
              retryable: false,
              raw: event.raw,
              ...(event.runId ? { runId: event.runId } : {}),
            },
          };
        }
        result = checkedToolResult(handlerError(error));
      } finally {
        if (deadlineTimer !== undefined) globalThis.clearTimeout(deadlineTimer);
      }
    }

    const completed: ToolJournalEntry = {
      ...executing,
      state: "completed",
      result,
      updatedAt: Date.now(),
    };
    await this.#journal.put(completed);
    try {
      await this.#submitToolResult(event.toolCallId, result);
      await this.#acknowledgeEntry(completed);
      return { blocked: false };
    } catch (error) {
      await this.#journal.put({ ...completed, state: "unknown", updatedAt: Date.now() });
      return {
        blocked: true,
        event: {
          type: "error",
          sequence: event.sequence,
          code: error instanceof AgentHubError ? error.code : "tool_result_submission_failed",
          message: error instanceof Error ? error.message : "Client Tool result submission failed",
          retryable: true,
          raw: error,
        },
      };
    }
  }

  async #submitToolResult(toolCallId: string, result: ToolResult): Promise<void> {
    await this.#requestJson<unknown>(
      `/api/client/tool-calls/${encodeURIComponent(toolCallId)}/result`,
      { method: "POST", body: JSON.stringify({ result }) },
      { transientRetries: 2 },
    );
  }

  async #acknowledgeEntry(entry: ToolJournalEntry): Promise<void> {
    const now = Date.now();
    await this.#journal.put({ ...entry, state: "acknowledged", updatedAt: now, acknowledgedAt: now });
  }

  async #cleanupJournal(): Promise<void> {
    const cutoff = Date.now() - JOURNAL_RETENTION_MS;
    const entries = await this.#journal.list(this.clientInstanceId);
    await Promise.all(entries
      .filter((entry) => entry.state === "acknowledged" && (entry.acknowledgedAt ?? entry.updatedAt) <= cutoff)
      .map((entry) => this.#journal.delete(this.clientInstanceId, entry.toolCallId)));
  }

  #rememberAnonymousSession(sessionId: string): void {
    if (this.#mode.kind !== "anonymous") return;
    this.#mode.currentSessionId = sessionId;
    this.#mode.localStorage.setItem(anonymousStorageKey(this.#mode.clientId, "session"), sessionId);
  }

  #url(path: string): string {
    return `${this.#baseUrl}${path}`;
  }

  #assertUsable(): void {
    if (this.#disposed) throw new Error("AgentHubClient is disposed");
  }
}

export function connect(options: AgentHubClientOptions): Promise<AgentHubClient> {
  return AgentHubClient.connect(options);
}

export function connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient> {
  return AgentHubClient.connectAnonymous(options);
}
