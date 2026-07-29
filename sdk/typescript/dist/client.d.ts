import type { AgentHubClientOptions, AnonymousClientOptions, ClientAgent, ErrorSessionEvent, MessagePage, MessagePageOptions, Run, SendOptions, SendResult, SessionEventListener, SessionListOptions, SessionMessage, SessionSummary, SubscribeOptions, ToolHandler, ToolHandlers, ToolRequestEvent, ToolTimeoutEvent } from "./types.js";
interface SessionOperations {
    messages(sessionId: string, options: MessagePageOptions): Promise<MessagePage>;
    send(session: ClientSession, content: string, options: SendOptions): Promise<SendResult>;
    stop(runId: string, signal?: AbortSignal): Promise<Run>;
    openStream(sessionId: string, after: number, signal: AbortSignal): Promise<Response>;
    handleToolRequest(sessionId: string, event: ToolRequestEvent, signal: AbortSignal): Promise<ToolDispatchOutcome>;
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
    };
    private constructor();
    static connect(options: AgentHubClientOptions): Promise<AgentHubClient>;
    static connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient>;
    get authorizedToolNames(): ReadonlySet<string>;
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
    reauthorize(): Promise<void>;
    stop(runId: string, signal?: AbortSignal): Promise<Run>;
    dispose(): void;
}
export declare function connect(options: AgentHubClientOptions): Promise<AgentHubClient>;
export declare function connectAnonymous(options: AnonymousClientOptions): Promise<AgentHubClient>;
//# sourceMappingURL=client.d.ts.map