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
export class SecretGrantsRequiredError extends AgentHubError {
    requirements;
    constructor(message, requirements, details) {
        super(428, "secret_grants_required", message, details);
        this.name = "SecretGrantsRequiredError";
        this.requirements = requirements;
    }
}
export function isSecretGrantsRequiredError(error) {
    return error instanceof SecretGrantsRequiredError
        || (error instanceof AgentHubError
            && error.status === 428
            && error.code === "secret_grants_required");
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