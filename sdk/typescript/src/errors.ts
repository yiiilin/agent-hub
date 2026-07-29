export class AgentHubError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: unknown;

  constructor(status: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = "AgentHubError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export class ClientToolError extends Error {
  readonly code: string;
  readonly retryable: boolean;

  constructor(code: string, message: string, retryable = false) {
    super(message);
    this.name = "ClientToolError";
    this.code = code;
    this.retryable = retryable;
  }
}
