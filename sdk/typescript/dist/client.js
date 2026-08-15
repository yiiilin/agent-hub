import { AgentHubError, ClientToolError, SecretGrantsRequiredError } from "./errors.js";
import { IndexedDbToolJournalStorage } from "./storage.js";
const CLIENT_INSTANCE_STORAGE_KEY = "agent-hub:client-instance-id";
const CLIENT_INSTANCE_CHANNEL_NAME = "agent-hub:client-instance:v1";
const CLIENT_INSTANCE_PROBE_MS = 40;
const CLIENT_INSTANCE_OWNER_KEY = Symbol.for("@agent-hub/client:instance-owner");
const JOURNAL_RETENTION_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_RENEWAL_WINDOW_MS = 60_000;
const DEFAULT_REQUEST_RETRY_DELAY_MS = 100;
const DEFAULT_RECONNECT_DELAY_MS = 500;
/** 工具绝对期限前保留的提交余量：deadline 提前触发，确保结果/错误能提交。 */
const SUBMIT_GRACE_MS = 5_000;
/** 执行状态未知时的收尾结果：不重跑 handler（副作用窗口未知），提交错误告知模型。 */
const UNKNOWN_RESULT = {
    status: "error",
    error: {
        code: "tool_result_unknown",
        message: "Previous Client Tool execution did not record a result; rerun the operation to confirm its outcome",
        retryable: false,
    },
};
/**
 * 恢复扫描的统一筛选：recorded（从未执行，可安全重跑）、executing/unknown
 * （副作用窗口未知，无 result 时以 tool_result_unknown 收尾）、completed
 * （handler 已完成、提交未确认，cached result 直接重提）。acknowledged
 * 已被 Hub 确认，跳过。
 */
function isRecoverableJournalState(state) {
    return ["recorded", "executing", "unknown", "completed"].includes(state);
}
const PATHS = {
    anonymousAccess: "/api/client/anonymous/access",
    renew: "/api/client/renew",
    runs: "/api/client/runs",
    sessions: "/api/client/sessions",
};
function browserStorage(provided, name) {
    if (provided)
        return provided;
    const storage = globalThis[name];
    if (!storage)
        throw new Error(`${name} is unavailable`);
    return storage;
}
function randomId(prefix = "") {
    if (typeof globalThis.crypto?.randomUUID === "function") {
        return `${prefix}${globalThis.crypto.randomUUID()}`;
    }
    const bytes = new Uint8Array(16);
    globalThis.crypto.getRandomValues(bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    return `${prefix}${uuid}`;
}
function clientInstanceOwnerToken() {
    const scope = globalThis;
    const existing = scope[CLIENT_INSTANCE_OWNER_KEY];
    if (typeof existing === "string")
        return existing;
    const created = randomId("owner-");
    scope[CLIENT_INSTANCE_OWNER_KEY] = created;
    return created;
}
function isClientInstanceChannelMessage(value) {
    if (!value || typeof value !== "object")
        return false;
    const message = value;
    return (message.type === "probe" || message.type === "occupied")
        && typeof message.clientInstanceId === "string"
        && typeof message.ownerToken === "string"
        && typeof message.nonce === "string";
}
async function reserveClientInstanceId(storage) {
    const stored = storage.getItem(CLIENT_INSTANCE_STORAGE_KEY)?.trim();
    let id = stored || randomId();
    if (!stored)
        storage.setItem(CLIENT_INSTANCE_STORAGE_KEY, id);
    if (typeof globalThis.BroadcastChannel !== "function")
        return { id };
    let channel;
    try {
        channel = new globalThis.BroadcastChannel(CLIENT_INSTANCE_CHANNEL_NAME);
    }
    catch {
        return { id };
    }
    const ownerToken = clientInstanceOwnerToken();
    let probeNonce;
    let occupiedByAnotherTab = false;
    channel.addEventListener("message", (event) => {
        if (!isClientInstanceChannelMessage(event.data))
            return;
        const message = event.data;
        if (message.type === "probe") {
            if (message.clientInstanceId === id && message.ownerToken !== ownerToken) {
                channel.postMessage({
                    type: "occupied",
                    clientInstanceId: id,
                    ownerToken,
                    nonce: message.nonce,
                });
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
        });
        await new Promise((resolve) => globalThis.setTimeout(resolve, CLIENT_INSTANCE_PROBE_MS));
        if (occupiedByAnotherTab) {
            id = randomId();
            storage.setItem(CLIENT_INSTANCE_STORAGE_KEY, id);
        }
    }
    return { id, channel };
}
function anonymousStorageKey(clientId, suffix) {
    return `agent-hub:anonymous:${encodeURIComponent(clientId)}:${suffix}`;
}
function anonymousVisitorKey(storage, clientId) {
    const key = anonymousStorageKey(clientId, "visitor");
    const stored = storage.getItem(key)?.trim();
    if (stored)
        return stored;
    const created = randomId("ahv_");
    storage.setItem(key, created);
    return created;
}
function normalizeCredential(value, now, fallback) {
    const token = value.accessToken ?? value.access_token ?? value.token;
    if (!token?.trim())
        throw new Error("authorize() did not return a Client Access Credential");
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
function pathWithQuery(path, values) {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(values)) {
        if (value !== undefined)
            query.set(key, String(value));
    }
    const serialized = query.toString();
    return serialized ? `${path}?${serialized}` : path;
}
function responseMessage(body, fallback) {
    if (body && typeof body === "object") {
        const record = body;
        const nested = record.error && typeof record.error === "object"
            ? record.error
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
function secretGrantRequirements(status, body) {
    if (status !== 428 || !isRecord(body) || !isRecord(body.details))
        return undefined;
    const raw = body.details.secret_grants_required;
    if (!Array.isArray(raw))
        return undefined;
    const requirements = [];
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
async function parseBody(response) {
    if (response.status === 204)
        return undefined;
    const text = await response.text();
    if (!text)
        return undefined;
    try {
        return JSON.parse(text);
    }
    catch {
        return text;
    }
}
function combinedSignal(...signals) {
    const available = signals.filter((signal) => signal !== undefined);
    if (available.length === 1)
        return available[0];
    return AbortSignal.any(available);
}
function abortableDelay(milliseconds, signal) {
    if (signal.aborted)
        return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
        const timer = globalThis.setTimeout(resolve, milliseconds);
        signal.addEventListener("abort", () => {
            globalThis.clearTimeout(timer);
            reject(signal.reason);
        }, { once: true });
    });
}
function isAbort(error) {
    return error instanceof DOMException && error.name === "AbortError";
}
function notifyListener(listener, event) {
    try {
        listener(event);
    }
    catch {
        // Subscriber failures do not change stream cursors or Client Tool execution.
    }
}
function isRecord(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}
function stringField(record, ...keys) {
    for (const key of keys) {
        if (typeof record[key] === "string")
            return record[key];
    }
    return undefined;
}
function numberField(record, ...keys) {
    for (const key of keys) {
        if (typeof record[key] === "number" && Number.isFinite(record[key]))
            return record[key];
    }
    return undefined;
}
function sessionIdFromResponse(value) {
    if (!isRecord(value))
        return undefined;
    const direct = stringField(value, "session_id", "sessionId", "integration_session_id", "hub_session_id");
    if (direct)
        return direct;
    if (isRecord(value.session))
        return stringField(value.session, "id", "session_id");
    if (isRecord(value.run))
        return sessionIdFromResponse(value.run);
    return undefined;
}
function runFromResponse(value) {
    const candidate = isRecord(value) && isRecord(value.run) ? value.run : value;
    if (!isRecord(candidate) || typeof candidate.id !== "string") {
        throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Run");
    }
    return candidate;
}
function messageFromResponse(value) {
    if (!isRecord(value) || !isRecord(value.message))
        return undefined;
    return value.message;
}
function eventRecord(value) {
    return isRecord(value) ? value : {};
}
function eventPayload(record) {
    return isRecord(record.payload) ? record.payload : {};
}
function normalizeEvent(frame, cursor) {
    let decoded;
    try {
        decoded = JSON.parse(frame.data);
    }
    catch {
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
        const input = (record.input ?? record.arguments ?? payload.input ?? payload.arguments ?? {});
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
        const result = (record.result ?? payload.result);
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
async function* readSse(response) {
    if (!response.ok) {
        const body = await parseBody(response);
        const error = responseMessage(body, `Session event stream failed with status ${response.status}`);
        throw new AgentHubError(response.status, error.code, error.message, body);
    }
    if (!response.body)
        throw new AgentHubError(response.status, "stream_unavailable", "Session event stream has no body");
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
                if (frame)
                    yield frame;
                boundary = buffer.indexOf("\n\n");
            }
            if (done) {
                const frame = parseSseBlock(buffer);
                if (frame)
                    yield frame;
                break;
            }
        }
    }
    finally {
        reader.releaseLock();
    }
}
function parseSseBlock(block) {
    if (!block.trim())
        return undefined;
    let event = "message";
    let id;
    const data = [];
    for (const line of block.split("\n")) {
        if (line.startsWith(":"))
            continue;
        const separator = line.indexOf(":");
        const field = separator < 0 ? line : line.slice(0, separator);
        const rawValue = separator < 0 ? "" : line.slice(separator + 1);
        const value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
        if (field === "event")
            event = value;
        else if (field === "id")
            id = value;
        else if (field === "data")
            data.push(value);
    }
    if (data.length === 0)
        return undefined;
    return { event, ...(id !== undefined ? { id } : {}), data: data.join("\n") };
}
function isToolResult(value) {
    if (!isRecord(value))
        return false;
    if (value.status === "success")
        return "output" in value;
    return value.status === "error"
        && isRecord(value.error)
        && typeof value.error.code === "string"
        && typeof value.error.message === "string"
        && typeof value.error.retryable === "boolean";
}
function isJsonValue(value, ancestors = new Set()) {
    if (value === null || typeof value === "string" || typeof value === "boolean")
        return true;
    if (typeof value === "number")
        return Number.isFinite(value);
    if (typeof value !== "object")
        return false;
    if (ancestors.has(value))
        return false;
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
function handlerError(error) {
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
export function checkedToolResult(value) {
    let result;
    if (isToolResult(value)) {
        result = value;
    }
    else {
        if (isJsonValue(value)) {
            result = { status: "success", output: value };
        }
        else {
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
    // 大结果不再在此截断：Agent Hub 后端对超大结果归档到对象存储
    // （≤32KB 原样入库；32KB..上限归档 S3 + 上下文 32KB 摘要 + read 工具读全文；
    //  超硬上限截断且标记 over_hard_limit，提交永不失败）。
    // 接入端无需自行截断，完整结果原样提交即可。
    return result;
}
export class SessionSubscription {
    closed;
    #controller;
    constructor(controller, closed) {
        this.#controller = controller;
        this.closed = closed;
    }
    dispose() {
        this.#controller.abort(new DOMException("Subscription disposed", "AbortError"));
    }
    unsubscribe() {
        this.dispose();
    }
}
export class ClientSession {
    #id;
    #operations;
    #subscriptions = new Set();
    #disposeController = new AbortController();
    #blockedBatches = new Set();
    #toolQueue = Promise.resolve();
    #sendQueue = Promise.resolve();
    #disposed = false;
    constructor(operations, id) {
        this.#operations = operations;
        this.#id = id;
    }
    get id() {
        return this.#id;
    }
    get isDraft() {
        return this.#id === null;
    }
    async messages(options = {}) {
        return (await this.messagePage(options)).items;
    }
    async messagePage(options = {}) {
        this.#assertUsable();
        if (this.#id === null)
            return { items: [], nextBeforeSequence: null };
        return this.#operations.messages(this.#id, {
            ...options,
            signal: combinedSignal(this.#disposeController.signal, options.signal),
        });
    }
    async events(options = {}) {
        this.#assertUsable();
        if (this.#id === null)
            return [];
        return this.#operations.events(this.#id, {
            ...options,
            signal: combinedSignal(this.#disposeController.signal, options.signal),
        });
    }
    /**
     * 恢复本会话中未完成的客户端工具调用，让页面刷新/重连后的操作无缝续上。
     * 覆盖两类场景：
     * 1. journal 中遗留的 executing/unknown 条目（工具执行中刷新）；
     * 2. 页面关闭后 Hub 才发出的工具请求（事件流中无对应结果的 tool_request）。
     * Hub 端已结束或属于其他 Client Instance 的调用会被跳过。
     */
    async recoverPendingTools(options = {}) {
        this.#assertUsable();
        const sessionId = this.#id;
        if (sessionId === null)
            return;
        const signal = combinedSignal(this.#disposeController.signal, options.signal);
        const seen = new Set();
        // 1. journal 遗留条目
        let entries = [];
        try {
            entries = await this.#operations.journal.list(this.#operations.clientInstanceId);
        }
        catch {
            entries = [];
        }
        for (const entry of entries) {
            if (entry.sessionId !== sessionId)
                continue;
            if (!isRecoverableJournalState(entry.state))
                continue;
            seen.add(entry.toolCallId);
            await this.#operations.recoverToolInvocation(entry, signal);
        }
        // 2. 事件流扫描：tool_request 无对应 tool_result/timeout 的调用
        let events;
        try {
            events = await this.events({ limit: 200, signal });
        }
        catch {
            return;
        }
        const completedCalls = new Set();
        for (const event of events) {
            if (event.type === "tool_result" || event.type === "timeout") {
                completedCalls.add(event.toolCallId);
            }
        }
        for (const event of events) {
            if (event.type !== "tool_request")
                continue;
            if (seen.has(event.toolCallId) || completedCalls.has(event.toolCallId))
                continue;
            seen.add(event.toolCallId);
            await this.#operations.recoverToolInvocation({
                clientInstanceId: this.#operations.clientInstanceId,
                toolCallId: event.toolCallId,
                sessionId,
                ...(event.runId ? { runId: event.runId } : {}),
                toolName: event.toolName,
                input: event.input,
                ...(event.expiresAt ? { expiresAt: event.expiresAt } : {}),
                state: "recorded",
                createdAt: Date.now(),
                updatedAt: Date.now(),
            }, signal);
        }
    }
    async send(content, options = {}) {
        this.#assertUsable();
        const pending = this.#sendQueue.then(() => this.#operations.send(this, content, {
            ...options,
            signal: combinedSignal(this.#disposeController.signal, options.signal),
        }));
        this.#sendQueue = pending.then(() => undefined, () => undefined);
        return pending;
    }
    async stop(runId, signal) {
        this.#assertUsable();
        return this.#operations.stop(runId, combinedSignal(this.#disposeController.signal, signal));
    }
    subscribe(listener, options = {}) {
        this.#assertUsable();
        if (this.#id === null)
            throw new Error("A draft Session cannot subscribe before its first message");
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
                        if (signal.aborted)
                            break;
                        const event = normalizeEvent(frame, cursor);
                        if (event.sequence > 0 && event.sequence <= cursor)
                            continue;
                        cursor = Math.max(cursor, event.sequence);
                        notifyListener(listener, event);
                        if (event.type === "tool_request")
                            this.#enqueueTool(event, listener);
                    }
                }
                catch (error) {
                    if (signal.aborted || isAbort(error))
                        break;
                    const status = error instanceof AgentHubError ? error.status : 0;
                    notifyListener(listener, {
                        type: "error",
                        sequence: cursor,
                        code: error instanceof AgentHubError ? error.code : "stream_disconnected",
                        message: error instanceof Error ? error.message : "Session event stream disconnected",
                        retryable: status === 0 || status >= 500,
                        raw: error,
                    });
                    if (status >= 400 && status < 500)
                        break;
                }
                if (!signal.aborted)
                    await abortableDelay(reconnectDelayMs, signal).catch(() => undefined);
            }
        })().finally(() => {
            this.#subscriptions.delete(subscription);
        });
        const subscription = new SessionSubscription(controller, closed);
        this.#subscriptions.add(subscription);
        return subscription;
    }
    dispose() {
        if (this.#disposed)
            return;
        this.#disposed = true;
        this.#disposeController.abort(new DOMException("Session disposed", "AbortError"));
        for (const subscription of this.#subscriptions)
            subscription.dispose();
        this.#subscriptions.clear();
    }
    /** @internal */
    materialize(id) {
        if (this.#id !== null && this.#id !== id)
            throw new Error("Session ID cannot change");
        this.#id = id;
    }
    #enqueueTool(event, listener) {
        const batchId = event.batchId ?? event.runId ?? event.toolCallId;
        this.#toolQueue = this.#toolQueue.catch(() => undefined).then(async () => {
            if (this.#blockedBatches.has(batchId))
                return;
            try {
                const outcome = await this.#operations.handleToolRequest(this.#id, event, this.#disposeController.signal);
                if (outcome.event)
                    notifyListener(listener, outcome.event);
                if (outcome.blocked)
                    this.#blockedBatches.add(batchId);
            }
            catch (error) {
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
    #assertUsable() {
        if (this.#disposed)
            throw new Error("Session is disposed");
    }
}
export { ClientSession as Session };
export class AgentHubClient {
    clientInstanceId;
    sessions;
    #baseUrl;
    #fetch;
    #mode;
    #journal;
    #handlers = new Map();
    #sessionCache = new Map();
    #allSessions = new Set();
    #toolOperations = new Map();
    #lifetime = new AbortController();
    #renewalWindowMs;
    #requestRetryDelayMs;
    #sessionOperations;
    #clientInstanceChannel;
    #credential;
    #credentialOperation;
    #renewalTimer;
    #disposed = false;
    constructor(options, mode, journal, instanceId, instanceChannel) {
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
            clientInstanceId: this.clientInstanceId,
            journal: this.#journal,
            recoverToolInvocation: (entry, signal) => this.#recoverToolInvocation(entry, signal),
        };
        this.sessions = {
            list: (requestOptions) => this.listSessions(requestOptions),
            existing: (sessionId) => this.existing(sessionId),
            draft: () => this.draft(),
            delete: (sessionId, requestOptions) => this.deleteSession(sessionId, requestOptions),
        };
    }
    static async connect(options) {
        const instanceStorage = browserStorage(options.sessionStorage, "sessionStorage");
        const reservation = await reserveClientInstanceId(instanceStorage);
        const journal = options.storage ?? new IndexedDbToolJournalStorage();
        const client = new AgentHubClient(options, { kind: "authenticated", authorize: options.authorize }, journal, reservation.id, reservation.channel);
        try {
            await client.#initialize(options.handlers);
            return client;
        }
        catch (error) {
            client.dispose();
            throw error;
        }
    }
    static async connectAnonymous(options) {
        if (!options.clientId.trim())
            throw new Error("clientId is required");
        const instanceStorage = browserStorage(options.sessionStorage, "sessionStorage");
        const localStorage = browserStorage(options.localStorage, "localStorage");
        const reservation = await reserveClientInstanceId(instanceStorage);
        const visitorKey = anonymousVisitorKey(localStorage, options.clientId);
        const currentSessionId = localStorage.getItem(anonymousStorageKey(options.clientId, "session"));
        const journal = options.storage ?? new IndexedDbToolJournalStorage();
        const client = new AgentHubClient(options, {
            kind: "anonymous",
            clientId: options.clientId,
            localStorage,
            visitorKey,
            currentSessionId,
        }, journal, reservation.id, reservation.channel);
        try {
            await client.#initialize(options.handlers);
            return client;
        }
        catch (error) {
            client.dispose();
            throw error;
        }
    }
    get authorizedToolNames() {
        return new Set(this.#credential?.authorizedToolNames ?? []);
    }
    get accessToken() {
        return this.#credential?.token ?? null;
    }
    get agent() {
        const agent = this.#credential?.agent;
        return agent ? { ...agent } : null;
    }
    get historyEnabled() {
        return this.#credential?.historyEnabled ?? false;
    }
    get isAnonymous() {
        return this.#mode.kind === "anonymous";
    }
    async listSessions(options = {}) {
        this.#assertUsable();
        if (this.#mode.kind === "anonymous") {
            throw new AgentHubError(403, "anonymous_history_disabled", "Anonymous clients cannot list Sessions");
        }
        const value = await this.#requestJson(pathWithQuery(PATHS.sessions, {
            cursor: options.cursor,
            limit: options.limit,
        }), {}, { signal: options.signal });
        const items = Array.isArray(value) ? value : value?.items;
        if (!Array.isArray(items))
            throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Session list");
        return items.map((item) => {
            if (!isRecord(item))
                throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid Session");
            const id = stringField(item, "id", "session_id");
            if (!id)
                throw new AgentHubError(500, "invalid_response", "Agent Hub returned a Session without an ID");
            return { ...item, id };
        });
    }
    existing(sessionId) {
        this.#assertUsable();
        const normalized = sessionId.trim();
        if (!normalized)
            throw new Error("sessionId is required");
        let session = this.#sessionCache.get(normalized);
        if (!session) {
            session = new ClientSession(this.#sessionOperations, normalized);
            this.#sessionCache.set(normalized, session);
            this.#allSessions.add(session);
        }
        return session;
    }
    draft() {
        this.#assertUsable();
        if (this.#mode.kind === "anonymous" && this.#mode.currentSessionId) {
            return this.existing(this.#mode.currentSessionId);
        }
        const session = new ClientSession(this.#sessionOperations, null);
        this.#allSessions.add(session);
        return session;
    }
    currentSession() {
        this.#assertUsable();
        if (this.#mode.kind !== "anonymous" || !this.#mode.currentSessionId)
            return null;
        return this.existing(this.#mode.currentSessionId);
    }
    registerTool(name, handler) {
        this.#assertUsable();
        if (!this.#credential?.authorizedToolNames.has(name)) {
            throw new Error(`Client Tool \"${name}\" is not authorized`);
        }
        this.#handlers.set(name, handler);
    }
    registerTools(handlers) {
        for (const [name, handler] of Object.entries(handlers))
            this.registerTool(name, handler);
    }
    unregisterTool(name) {
        this.#handlers.delete(name);
    }
    async deleteSession(sessionId, options = {}) {
        this.#assertUsable();
        if (!sessionId.trim())
            throw new AgentHubError(400, "invalid_session_id", "sessionId is required");
        if (this.#mode.kind === "anonymous") {
            throw new AgentHubError(403, "anonymous_history_disabled", "Anonymous clients cannot delete Sessions");
        }
        await this.#requestJson(`${PATHS.sessions}/${encodeURIComponent(sessionId)}`, { method: "DELETE" }, { signal: options.signal, transientRetries: 1 });
    }
    async reauthorize() {
        this.#assertUsable();
        if (this.#credentialOperation)
            await this.#credentialOperation;
        await this.#startCredentialOperation(() => this.#authorizeFresh());
    }
    async stop(runId, signal) {
        this.#assertUsable();
        if (!runId.trim())
            throw new Error("runId is required");
        const value = await this.#requestJson(`${PATHS.runs}/${encodeURIComponent(runId)}/stop`, { method: "POST", body: "{}" }, { signal });
        return runFromResponse(value);
    }
    dispose() {
        if (this.#disposed)
            return;
        this.#disposed = true;
        this.#credential = undefined;
        if (this.#renewalTimer !== undefined)
            globalThis.clearTimeout(this.#renewalTimer);
        this.#clientInstanceChannel?.close();
        this.#lifetime.abort(new DOMException("Client disposed", "AbortError"));
        for (const session of this.#allSessions)
            session.dispose();
        this.#sessionCache.clear();
        this.#allSessions.clear();
        this.#handlers.clear();
    }
    async #initialize(handlers) {
        await this.#authorizeFresh();
        if (handlers)
            this.registerTools(handlers);
        await this.#cleanupJournal();
        await this.#recoverPendingTools();
    }
    /**
     * 刷新/重连后恢复未完成的客户端工具调用：页面在工具执行中关闭/刷新时，
     * journal 会遗留 executing/unknown 状态的条目。重新认领（Hub 幂等）并
     * 执行 handler、提交结果，让操作无缝续上；Hub 侧已结束或属于其他
     * Client Instance 的调用会被跳过或清理。
     */
    async #recoverPendingTools() {
        let entries;
        try {
            entries = await this.#journal.list(this.clientInstanceId);
        }
        catch {
            return;
        }
        const pending = entries.filter((entry) => isRecoverableJournalState(entry.state));
        for (const entry of pending) {
            await this.#recoverToolInvocation(entry);
        }
    }
    /** 认领并收尾一个遗留的工具调用（幂等；Hub 侧已结束或属于其他实例则跳过）。 */
    async #recoverToolInvocation(entry, signal) {
        try {
            const claim = await this.#requestJson(`/api/client/tool-calls/${encodeURIComponent(entry.toolCallId)}/claim`, { method: "POST", body: "{}" }, { transientRetries: 1 });
            const claimStatus = claim.status ?? claim.claim_status ?? (claim.terminal ? "terminal" : "claimed");
            if (claim.terminal || ["completed", "failed", "expired", "timed_out", "terminal"].includes(claimStatus)) {
                await this.#journal.delete(this.clientInstanceId, entry.toolCallId);
                return;
            }
            if (entry.state === "acknowledged") {
                return;
            }
            // cached result（completed/unknown/executing 遗留）：直接重提，绝不重跑
            // handler——副作用可能已经发生，重跑会重复写。
            if (entry.result) {
                await this.#submitToolResult(entry.toolCallId, entry.result);
                const completed = { ...entry, state: "completed", updatedAt: Date.now() };
                await this.#journal.put(completed);
                await this.#acknowledgeEntry(completed);
                return;
            }
            // 仅 recorded 可执行（从未开始，副作用安全）；executing/unknown 无
            // result 一律以 tool_result_unknown 收尾，不重跑。
            if (entry.state !== "recorded") {
                await this.#submitToolResult(entry.toolCallId, UNKNOWN_RESULT);
                const completed = {
                    ...entry,
                    result: UNKNOWN_RESULT,
                    state: "completed",
                    updatedAt: Date.now(),
                };
                await this.#journal.put(completed);
                await this.#acknowledgeEntry(completed);
                return;
            }
            const handler = this.#handlers.get(entry.toolName);
            let result;
            if (!handler) {
                result = {
                    status: "error",
                    error: {
                        code: "tool_handler_not_registered",
                        message: `No handler is registered for Client Tool "${entry.toolName}"`,
                        retryable: false,
                    },
                };
            }
            else {
                result = await this.#executeToolHandler(entry, handler, combinedSignal(this.#lifetime.signal, signal), true);
            }
            // 先落 journal（completed+result）再提交：提交失败时恢复重提 cached
            // result，不会重跑 handler。
            const completed = { ...entry, result, state: "completed", updatedAt: Date.now() };
            await this.#journal.put(completed);
            try {
                await this.#submitToolResult(entry.toolCallId, result);
                await this.#acknowledgeEntry(completed);
            }
            catch (error) {
                if (error instanceof AgentHubError && [404, 409, 410].includes(error.status)) {
                    // Hub 已结束/结果不匹配：清理，下次不再恢复。
                    await this.#journal.delete(this.clientInstanceId, entry.toolCallId).catch(() => undefined);
                }
                // transient：completed+result 已落盘，下次恢复重提。
            }
        }
        catch (error) {
            if (error instanceof AgentHubError && (error.status === 404 || error.status === 403)) {
                await this.#journal.delete(this.clientInstanceId, entry.toolCallId).catch(() => undefined);
                return;
            }
            // claim 网络失败：保留原状态，下次恢复重试。
            await this.#journal.put(entry).catch(() => undefined);
        }
    }
    /**
     * 统一执行 handler：按 entry 绝对期限（expiresAt）减提交余量设置 deadline，
     * 超时/中断生成错误结果（不静默）。期限缺失或不足时**不调用** handler
     * （Promise.race 会先求值 handler，不能靠 deadline reject 阻止副作用）。
     */
    async #executeToolHandler(entry, handler, signal, recovering) {
        const expiresAt = entry.expiresAt ? Date.parse(entry.expiresAt) : NaN;
        const remaining = Number.isFinite(expiresAt)
            ? expiresAt - Date.now() - SUBMIT_GRACE_MS
            : NaN;
        if (!Number.isFinite(remaining) || remaining <= 0) {
            return {
                status: "error",
                error: {
                    code: Number.isFinite(expiresAt) ? "tool_timeout" : "tool_result_unknown",
                    message: Number.isFinite(expiresAt)
                        ? "Client Tool invocation reached its deadline"
                        : "Tool deadline is unknown; execution was not attempted",
                    retryable: false,
                },
            };
        }
        const controller = new AbortController();
        let deadlineTimer;
        const deadline = new Promise((_resolve, reject) => {
            deadlineTimer = globalThis.setTimeout(() => {
                controller.abort(new DOMException("Client Tool deadline reached", "TimeoutError"));
                reject(new DOMException("Client Tool deadline reached", "TimeoutError"));
            }, Math.min(remaining, 5 * 60_000));
        });
        try {
            const output = await Promise.race([
                handler(entry.input, {
                    toolCallId: entry.toolCallId,
                    sessionId: entry.sessionId,
                    ...(entry.runId ? { runId: entry.runId } : {}),
                    signal: combinedSignal(controller.signal, signal),
                    recovering,
                }),
                deadline,
            ]);
            controller.abort();
            return checkedToolResult(output);
        }
        catch (error) {
            controller.abort();
            if (error instanceof DOMException && error.name === "TimeoutError") {
                return {
                    status: "error",
                    error: {
                        code: "tool_timeout",
                        message: "Client Tool invocation reached its deadline",
                        retryable: false,
                    },
                };
            }
            if (isAbort(error)) {
                return {
                    status: "error",
                    error: {
                        code: "tool_execution_interrupted",
                        message: "Client Tool execution was interrupted",
                        retryable: false,
                    },
                };
            }
            return checkedToolResult(handlerError(error));
        }
        finally {
            if (deadlineTimer !== undefined)
                globalThis.clearTimeout(deadlineTimer);
        }
    }
    async #authorizeFresh() {
        let value;
        if (this.#mode.kind === "authenticated") {
            value = await this.#mode.authorize({
                clientInstanceId: this.clientInstanceId,
                signal: this.#lifetime.signal,
            });
        }
        else {
            const body = {
                client_id: this.#mode.clientId,
                visitor_key: this.#mode.visitorKey,
                client_instance_id: this.clientInstanceId,
            };
            if (this.#mode.currentSessionId)
                body.session_id = this.#mode.currentSessionId;
            value = await this.#publicRequest(PATHS.anonymousAccess, {
                method: "POST",
                body: JSON.stringify(body),
                signal: this.#lifetime.signal,
            });
            const recoveredSessionId = value.sessionId ?? value.session_id ?? value.hub_session_id;
            if (recoveredSessionId)
                this.#rememberAnonymousSession(recoveredSessionId);
        }
        this.#credential = normalizeCredential(value, Date.now());
        this.#scheduleRenewal();
    }
    async #renewCredential() {
        const current = this.#credential;
        if (!current)
            return this.#authorizeFresh();
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
        const value = await this.#checkedBody(response);
        this.#credential = normalizeCredential(value, Date.now(), current);
        this.#scheduleRenewal();
    }
    #startCredentialOperation(operation) {
        if (this.#credentialOperation)
            return this.#credentialOperation;
        const pending = operation().finally(() => {
            if (this.#credentialOperation === pending)
                this.#credentialOperation = undefined;
        });
        this.#credentialOperation = pending;
        return pending;
    }
    async #ensureCredential() {
        const credential = this.#credential;
        if (!credential) {
            await this.#startCredentialOperation(() => this.#authorizeFresh());
            return;
        }
        if (credential.expiresAt - Date.now() <= this.#renewalWindowMs) {
            await this.#startCredentialOperation(() => this.#renewCredential());
        }
    }
    #scheduleRenewal() {
        if (this.#renewalTimer !== undefined)
            globalThis.clearTimeout(this.#renewalTimer);
        const credential = this.#credential;
        if (!credential || this.#disposed)
            return;
        const delay = Math.max(0, credential.expiresAt - Date.now() - this.#renewalWindowMs);
        this.#renewalTimer = globalThis.setTimeout(() => {
            void this.#startCredentialOperation(() => this.#renewCredential()).catch(() => undefined);
        }, delay);
    }
    async #requestJson(path, init, options = {}) {
        const response = await this.#authorizedFetch(path, init, options);
        return this.#checkedBody(response);
    }
    async #authorizedFetch(path, init, options) {
        const transientRetries = options.transientRetries ?? 0;
        let transientAttempt = 0;
        let authorizationRetried = false;
        while (true) {
            await this.#ensureCredential();
            const credential = this.#credential;
            if (!credential)
                throw new Error("Client Access Credential is unavailable");
            let response;
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
            }
            catch (error) {
                if (transientAttempt >= transientRetries || isAbort(error))
                    throw error;
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
    async #publicRequest(path, init) {
        const response = await this.#fetch(this.#url(path), {
            ...init,
            headers: {
                Accept: "application/json",
                "Content-Type": "application/json",
                ...init.headers,
            },
            credentials: "omit",
        });
        return this.#checkedBody(response);
    }
    async #checkedBody(response) {
        const body = await parseBody(response);
        if (!response.ok) {
            const error = responseMessage(body, `Agent Hub request failed with status ${response.status}`);
            const requirements = secretGrantRequirements(response.status, body);
            if (requirements) {
                throw new SecretGrantsRequiredError(error.message, requirements, body);
            }
            throw new AgentHubError(response.status, error.code, error.message, body);
        }
        return body;
    }
    async #messages(sessionId, options) {
        const limit = options.limit ?? 50;
        if (!Number.isInteger(limit) || limit < 1 || limit > 100)
            throw new Error("message limit must be between 1 and 100");
        const value = await this.#requestJson(pathWithQuery(`${PATHS.sessions}/${encodeURIComponent(sessionId)}/messages`, { before_sequence: options.beforeSequence, limit }), {}, { signal: options.signal });
        const rawItems = Array.isArray(value) ? value : isRecord(value) && Array.isArray(value.items) ? value.items : undefined;
        if (!rawItems)
            throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid message page");
        const items = rawItems;
        const minimum = items.reduce((current, item) => (typeof item.sequence === "number" && (current === null || item.sequence < current) ? item.sequence : current), null);
        return { items, nextBeforeSequence: items.length === limit ? minimum : null };
    }
    async #events(sessionId, options) {
        const after = options.after ?? 0;
        if (!Number.isInteger(after) || after < 0)
            throw new Error("event cursor must be a non-negative integer");
        const limit = options.limit;
        const value = await this.#requestJson(pathWithQuery(`${PATHS.sessions}/${encodeURIComponent(sessionId)}/events`, {
            after: after > 0 ? after : undefined,
            ...(limit !== undefined ? { limit: String(limit) } : {}),
        }), {}, { signal: options.signal });
        if (!Array.isArray(value)) {
            throw new AgentHubError(500, "invalid_response", "Agent Hub returned an invalid event list");
        }
        return value.map((event) => normalizeEvent({
            event: "session_event",
            data: JSON.stringify(event),
        }, after));
    }
    async #send(session, content, options) {
        const normalized = content.trim();
        if (!normalized)
            throw new Error("message content is required");
        const key = options.clientMessageKey?.trim() || randomId("msg_");
        const body = { message: normalized, client_message_key: key };
        if (session.id)
            body.session_id = session.id;
        if (options.attachmentIds && options.attachmentIds.length > 0)
            body.attachment_ids = [...options.attachmentIds];
        const value = await this.#requestJson(PATHS.runs, {
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
        if (this.#mode.kind === "anonymous")
            this.#rememberAnonymousSession(resolvedSessionId);
        const message = messageFromResponse(value);
        return {
            run,
            ...(message ? { message } : {}),
            sessionId: resolvedSessionId,
            clientMessageKey: key,
            raw: value,
        };
    }
    async #openStream(sessionId, after, signal) {
        const path = pathWithQuery(`${PATHS.sessions}/${encodeURIComponent(sessionId)}/events/stream`, {
            after: after > 0 ? after : undefined,
        });
        return this.#authorizedFetch(path, {
            headers: after > 0 ? { "Last-Event-ID": String(after) } : {},
        }, { signal });
    }
    #handleToolRequest(sessionId, event, signal) {
        const active = this.#toolOperations.get(event.toolCallId);
        if (active)
            return active;
        const operation = this.#dispatchTool(sessionId, event, signal).finally(() => {
            if (this.#toolOperations.get(event.toolCallId) === operation)
                this.#toolOperations.delete(event.toolCallId);
        });
        this.#toolOperations.set(event.toolCallId, operation);
        return operation;
    }
    async #dispatchTool(sessionId, event, signal) {
        const existing = await this.#journal.get(this.clientInstanceId, event.toolCallId);
        // cached result（completed/unknown/executing 遗留，含已 acknowledged 后
        // Hub 重发 SSE 的重复帧）：幂等重提缓存结果，绝不重跑 handler（副作用
        // 可能已发生）。Hub 明确结束（terminal）则清理并放行批次。
        if (existing?.result) {
            try {
                await this.#submitToolResult(event.toolCallId, existing.result);
                if (existing.state !== "acknowledged") {
                    await this.#acknowledgeEntry(existing);
                }
            }
            catch (error) {
                if (error instanceof AgentHubError && [404, 409, 410].includes(error.status)) {
                    await this.#journal.delete(this.clientInstanceId, event.toolCallId).catch(() => undefined);
                    return { blocked: false };
                }
                return {
                    blocked: true,
                    event: {
                        type: "error",
                        sequence: event.sequence,
                        code: error instanceof AgentHubError ? error.code : "tool_result_submission_failed",
                        message: error instanceof Error ? error.message : "Client Tool result submission failed",
                        retryable: true,
                        raw: error,
                        toolCallId: event.toolCallId,
                    },
                };
            }
            return { blocked: false };
        }
        if (existing?.state === "acknowledged")
            return { blocked: false };
        // 无 result 的 executing/unknown（副作用窗口未知）：不重跑，提交
        // tool_result_unknown 收尾，模型明确收到失败而非静默。
        if (existing && !existing.result && ["executing", "unknown"].includes(existing.state)) {
            try {
                await this.#submitToolResult(event.toolCallId, UNKNOWN_RESULT);
                await this.#acknowledgeEntry({ ...existing, result: UNKNOWN_RESULT });
            }
            catch {
                // transient：保留 journal，恢复路径会重试收尾。
            }
            return { blocked: false };
        }
        const now = Date.now();
        const entry = existing ?? {
            clientInstanceId: this.clientInstanceId,
            toolCallId: event.toolCallId,
            sessionId,
            ...(event.runId ? { runId: event.runId } : {}),
            toolName: event.toolName,
            input: event.input,
            ...(event.expiresAt ? { expiresAt: event.expiresAt } : {}),
            state: "recorded",
            createdAt: now,
            updatedAt: now,
        };
        if (!existing)
            await this.#journal.put(entry);
        let claim;
        try {
            claim = await this.#requestJson(`/api/client/tool-calls/${encodeURIComponent(event.toolCallId)}/claim`, { method: "POST", body: "{}" }, { signal, transientRetries: 1 });
        }
        catch (error) {
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
                },
            };
        }
        const claimStatus = claim.status ?? claim.claim_status ?? (claim.terminal ? "terminal" : "claimed");
        if (claim.terminal || ["completed", "failed", "expired", "timed_out", "terminal"].includes(claimStatus)) {
            const terminalEntry = {
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
        const executing = { ...entry, state: "executing", updatedAt: Date.now() };
        await this.#journal.put(executing);
        const handler = this.#handlers.get(event.toolName);
        let result;
        if (!handler) {
            result = {
                status: "error",
                error: {
                    code: "tool_handler_not_registered",
                    message: `No handler is registered for Client Tool "${event.toolName}"`,
                    retryable: false,
                },
            };
        }
        else {
            // 统一执行：按 entry 绝对期限减提交余量；超时/中断生成错误结果提交
            // （不静默 blocked），期限缺失/不足时不调用 handler。
            result = await this.#executeToolHandler(entry, handler, combinedSignal(signal, this.#lifetime.signal), false);
        }
        // 先落 journal（completed+result）再提交：transient 失败保留 completed，
        // 恢复路径重提 cached result（不重跑 handler）。
        const completed = {
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
        }
        catch (error) {
            if (error instanceof AgentHubError && [404, 409, 410].includes(error.status)) {
                // Hub 已结束/结果不匹配：清理，批次放行。
                await this.#journal.delete(this.clientInstanceId, event.toolCallId).catch(() => undefined);
                return { blocked: false };
            }
            // transient：completed+result 保留，恢复路径重提；阻塞同批后续工具，
            // 避免前一个结果未达 Hub 时继续产生副作用。
            return {
                blocked: true,
                event: {
                    type: "error",
                    sequence: event.sequence,
                    code: error instanceof AgentHubError ? error.code : "tool_result_submission_failed",
                    message: error instanceof Error ? error.message : "Client Tool result submission failed",
                    retryable: true,
                    raw: error,
                    toolCallId: event.toolCallId,
                },
            };
        }
    }
    async #submitToolResult(toolCallId, result) {
        await this.#requestJson(`/api/client/tool-calls/${encodeURIComponent(toolCallId)}/result`, { method: "POST", body: JSON.stringify({ result }) }, { transientRetries: 2 });
    }
    async #acknowledgeEntry(entry) {
        const now = Date.now();
        await this.#journal.put({ ...entry, state: "acknowledged", updatedAt: now, acknowledgedAt: now });
    }
    async #cleanupJournal() {
        const cutoff = Date.now() - JOURNAL_RETENTION_MS;
        const entries = await this.#journal.list(this.clientInstanceId);
        await Promise.all(entries
            .filter((entry) => entry.state === "acknowledged" && (entry.acknowledgedAt ?? entry.updatedAt) <= cutoff)
            .map((entry) => this.#journal.delete(this.clientInstanceId, entry.toolCallId)));
    }
    #rememberAnonymousSession(sessionId) {
        if (this.#mode.kind !== "anonymous")
            return;
        this.#mode.currentSessionId = sessionId;
        this.#mode.localStorage.setItem(anonymousStorageKey(this.#mode.clientId, "session"), sessionId);
    }
    #url(path) {
        return `${this.#baseUrl}${path}`;
    }
    #assertUsable() {
        if (this.#disposed)
            throw new Error("AgentHubClient is disposed");
    }
}
export function connect(options) {
    return AgentHubClient.connect(options);
}
export function connectAnonymous(options) {
    return AgentHubClient.connectAnonymous(options);
}
//# sourceMappingURL=client.js.map