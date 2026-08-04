import type { SecretGrantRequirement } from "./types.js";
export declare class AgentHubError extends Error {
    readonly status: number;
    readonly code: string;
    readonly details: unknown;
    constructor(status: number, code: string, message: string, details?: unknown);
}
export declare class SecretGrantsRequiredError extends AgentHubError {
    readonly requirements: SecretGrantRequirement[];
    constructor(message: string, requirements: SecretGrantRequirement[], details?: unknown);
}
export declare function isSecretGrantsRequiredError(error: unknown): error is SecretGrantsRequiredError;
export declare class ClientToolError extends Error {
    readonly code: string;
    readonly retryable: boolean;
    constructor(code: string, message: string, retryable?: boolean);
}
//# sourceMappingURL=errors.d.ts.map