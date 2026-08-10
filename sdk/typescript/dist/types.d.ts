export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | {
    [key: string]: JsonValue;
};
export interface ClientToolDefinition {
    name: string;
    description: string;
    input_schema: {
        [key: string]: JsonValue;
    };
}
export interface ClientAgent {
    id: string;
    name: string;
    instructions?: string;
}
export interface AuthorizeRequest {
    clientInstanceId: string;
    signal: AbortSignal;
}
export interface ClientCredential {
    accessToken?: string;
    access_token?: string;
    token?: string;
    expiresAt?: string | number;
    expires_at?: string | number;
    expiresIn?: number;
    expires_in?: number;
    tools?: readonly (string | ClientToolDefinition)[];
    authorizedTools?: readonly (string | ClientToolDefinition)[];
    authorized_tools?: readonly (string | ClientToolDefinition)[];
    tool_names?: readonly string[];
    agent?: ClientAgent;
    historyEnabled?: boolean;
    history_enabled?: boolean;
    sessionId?: string | null;
    session_id?: string | null;
    hub_session_id?: string | null;
}
export type Authorize = (request: AuthorizeRequest) => Promise<ClientCredential>;
export interface SessionSummary {
    id: string;
    hub_session_id?: string;
    created_at?: string;
    updated_at?: string;
    preview?: string | null;
    [key: string]: unknown;
}
export interface SessionListOptions {
    cursor?: string;
    limit?: number;
    signal?: AbortSignal;
}
export interface HubSessionAttachment {
    id: string;
    session_id: string;
    name: string;
    content_type: string;
    size_bytes: number;
    created_at: string;
}
export interface SessionMessage {
    id: string;
    session_id: string;
    sequence: number;
    role: string;
    message_kind: string;
    content: string | null;
    payload: unknown;
    delivery_mode?: string;
    delivery_state?: string;
    client_message_key?: string | null;
    run_id?: string | null;
    accepted_at?: string;
    attachments?: HubSessionAttachment[];
    [key: string]: unknown;
}
export interface MessagePageOptions {
    beforeSequence?: number;
    limit?: number;
    signal?: AbortSignal;
}
export interface MessagePage {
    items: SessionMessage[];
    nextBeforeSequence: number | null;
}
export interface EventListOptions {
    after?: number;
    /** 最多返回的事件条数（用于历史恢复时限制拉取量；省略则返回全部）。 */
    limit?: number;
    signal?: AbortSignal;
}
export interface Run {
    id: string;
    status: string;
    hub_session_id?: string | null;
    integration_session_id?: string | null;
    [key: string]: unknown;
}
export interface SendOptions {
    clientMessageKey?: string;
    attachmentIds?: readonly string[];
    signal?: AbortSignal;
}
export interface SendResult {
    run: Run;
    message?: SessionMessage;
    sessionId: string;
    clientMessageKey: string;
    raw: unknown;
}
export interface SecretGrantRequirement {
    name: string;
    kind: string;
    description?: string;
}
export interface SessionEventBase {
    sequence: number;
    eventId?: string;
    runId?: string;
    createdAt?: string;
    raw: unknown;
}
export interface MessageSessionEvent extends SessionEventBase {
    type: "message" | "assistant";
    role?: string;
    content: string | null;
}
export interface ToolRequestEvent extends SessionEventBase {
    type: "tool_request";
    toolCallId: string;
    toolName: string;
    input: JsonValue;
    batchId?: string;
    expiresAt?: string;
}
export interface ToolResultEvent extends SessionEventBase {
    type: "tool_result";
    toolCallId: string;
    toolName?: string;
    result: ToolResult;
    elapsedMs?: number;
}
export interface ToolTimeoutEvent extends SessionEventBase {
    type: "timeout";
    toolCallId: string;
    toolName?: string;
    message: string;
}
export interface ErrorSessionEvent extends SessionEventBase {
    type: "error";
    code: string;
    message: string;
    retryable: boolean;
}
export interface GenericSessionEvent extends SessionEventBase {
    type: "event";
    eventType: string;
    content?: string | null;
}
export type SessionEvent = MessageSessionEvent | ToolRequestEvent | ToolResultEvent | ToolTimeoutEvent | ErrorSessionEvent | GenericSessionEvent;
export interface SubscribeOptions {
    after?: number;
    signal?: AbortSignal;
    reconnectDelayMs?: number;
}
export type SessionEventListener = (event: SessionEvent) => void;
export type ToolResult = ToolSuccessResult | ToolErrorResult;
export interface ToolSuccessResult {
    status: "success";
    output: JsonValue;
}
export interface ToolErrorResult {
    status: "error";
    error: {
        code: string;
        message: string;
        retryable: boolean;
    };
}
export interface ToolHandlerContext {
    toolCallId: string;
    sessionId: string;
    runId?: string;
    signal: AbortSignal;
    /** 是否为会话恢复（刷新/重连）时补执行遗留调用；此时 handler 不应阻塞等待用户交互。 */
    recovering?: boolean;
}
export type ToolHandler = (input: JsonValue, context: ToolHandlerContext) => JsonValue | ToolResult | Promise<JsonValue | ToolResult>;
export type ToolHandlers = Readonly<Record<string, ToolHandler>>;
export type ToolJournalState = "recorded" | "executing" | "completed" | "unknown" | "acknowledged";
export interface ToolJournalEntry {
    clientInstanceId: string;
    toolCallId: string;
    sessionId: string;
    runId?: string;
    toolName: string;
    input: JsonValue;
    state: ToolJournalState;
    result?: ToolResult;
    createdAt: number;
    updatedAt: number;
    acknowledgedAt?: number;
}
export interface ToolJournalStorage {
    get(clientInstanceId: string, toolCallId: string): Promise<ToolJournalEntry | undefined>;
    put(entry: ToolJournalEntry): Promise<void>;
    delete(clientInstanceId: string, toolCallId: string): Promise<void>;
    list(clientInstanceId: string): Promise<ToolJournalEntry[]>;
}
export interface CommonClientOptions {
    baseUrl?: string;
    fetch?: typeof fetch;
    sessionStorage?: Storage;
    storage?: ToolJournalStorage;
    handlers?: ToolHandlers;
    renewalWindowMs?: number;
    requestRetryDelayMs?: number;
}
export interface AgentHubClientOptions extends CommonClientOptions {
    authorize: Authorize;
}
export interface AnonymousClientOptions extends CommonClientOptions {
    clientId: string;
    localStorage?: Storage;
}
export interface SessionDeleteOptions {
    signal?: AbortSignal;
}
//# sourceMappingURL=types.d.ts.map