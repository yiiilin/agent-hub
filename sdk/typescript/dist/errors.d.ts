export declare class AgentHubError extends Error {
    readonly status: number;
    readonly code: string;
    readonly details: unknown;
    constructor(status: number, code: string, message: string, details?: unknown);
}
export declare class ClientToolError extends Error {
    readonly code: string;
    readonly retryable: boolean;
    constructor(code: string, message: string, retryable?: boolean);
}
//# sourceMappingURL=errors.d.ts.map