export class AgentHubError extends Error {
    status;
    code;
    details;
    constructor(status, code, message, details) {
        super(message);
        this.name = "AgentHubError";
        this.status = status;
        this.code = code;
        this.details = details;
    }
}
export class ClientToolError extends Error {
    code;
    retryable;
    constructor(code, message, retryable = false) {
        super(message);
        this.name = "ClientToolError";
        this.code = code;
        this.retryable = retryable;
    }
}
//# sourceMappingURL=errors.js.map