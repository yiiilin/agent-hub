import type { AgentHubClientOptions, AnonymousClientOptions, ClientAgent, ErrorSessionEvent, EventListOptions, MessagePage, MessagePageOptions, Run, SendOptions, SendResult, SessionEvent, SessionDeleteOptions, SessionEventListener, SessionListOptions, SessionMessage, SessionSummary, SubscribeOptions, ToolHandler, ToolHandlers, ToolJournalEntry, ToolJournalStorage, ToolRequestEvent, ToolTimeoutEvent } from "./types.js";
interface SessionOperations {
    messages(sessionId: string, options: MessagePageOptions): Promise<MessagePage>;
    events(sessionId: string, options: EventListOptions): Promise<SessionEvent[]>;
    send(session: ClientSession, content: string, options: SendOptions): Promise<SendResult>;
    stop(runId: string, signal?: AbortSignal): Promise<Run>;
    openStream(sessionId: string, after: number, signal: AbortSignal): Promise<Response>;
    handleToolRequest(sessionId: string, event: ToolRequestEvent, signal: AbortSignal): Promise<ToolDispatchOutcome>;
    clientInstanceId: string;
    journal: ToolJournalStorage;
    recoverToolInvocation(entry: ToolJournalEntry, signal?: AbortSignal): Promise<void>;
}
interface ToolDispatchOutcome {
    blocked: boolean;
    event?: ToolTimeoutEvent | ErrorSessionEvent;
}
export declare class SessionSubscription {
    #private;
    readonly closed: Promise<void>;
    constructor(controller: AbortController, closed: Promise<void>);
    dispose(): void;
    unsubscribe(): void;
}
export declare class ClientSession {
    #private;
    constructor(operations: SessionOperations, id: string | null);
    get id(): string | null;
    get isDraft(): boolean;
    messages(options?: MessagePageOptions): Promise<SessionMessage[]>;
    messagePage(options?: MessagePageOptions): Promise<MessagePage>;
    events(options?: EventListOptions): Promise<SessionEvent[]>;
    /**
     * 恢复本会话中未完成的客户端工具调用，让页面刷新/重连后的操作无缝续上。
     * 覆盖两类场景：
     * 1. journal 中遗留的 executing/unknown 条目（工具执行中刷新）；
     * 2. 页面关闭后 Hub 才发出的工具请求（事件流中无对应结果的 tool_request）。
     * Hub 端已结束或属于其他 Client Instance 的调用会被跳过。
     */
    recoverPendingTools(options?: {
        signal?: AbortSignal;
    }): Promise<void>;
    send(content: string, options?: SendOptions): Promise<SendResult>;
    stop(runId: string, signal?: AbortSignal): Promise<Run>;
    subscribe(listener: SessionEventListener, options?: SubscribeOptions): SessionSubscription;
    dispose(): void;
    /** @internal */
    materialize(id: string): void;
}
export { ClientSession as Session };
export declare class AgentHubClient {
    #private;
    readonly clientInstanceId: string;
    readonly sessions: {
        list: (options?: SessionListOptions) => Promise<SessionSummary[]>;
        existing: (sessionId: string) => ClientSession;
        draft: () => ClientSession;
        delete: (sessionId: string, options?: SessionDeleteOptions) => Promise<void>;
    };
    private constructor();
    static connect(options: AgentHubClientOptions): Promise<AgentHubClient>;
    static connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient>;
    get authorizedToolNames(): ReadonlySet<string>;
    get accessToken(): string | null;
    get agent(): ClientAgent | null;
    get historyEnabled(): boolean;
    get isAnonymous(): boolean;
    listSessions(options?: SessionListOptions): Promise<SessionSummary[]>;
    existing(sessionId: string): ClientSession;
    draft(): ClientSession;
    currentSession(): ClientSession | null;
    registerTool(name: string, handler: ToolHandler): void;
    registerTools(handlers: ToolHandlers): void;
    unregisterTool(name: string): void;
    deleteSession(sessionId: string, options?: SessionDeleteOptions): Promise<void>;
    reauthorize(): Promise<void>;
    stop(runId: string, signal?: AbortSignal): Promise<Run>;
    dispose(): void;
}
export declare function connect(options: AgentHubClientOptions): Promise<AgentHubClient>;
export declare function connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient>;
//# sourceMappingURL=client.d.ts.map