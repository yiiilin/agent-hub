import type { SecretGrantRequirement } from "./types.js";

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

export class SecretGrantsRequiredError extends AgentHubError {
  readonly requirements: SecretGrantRequirement[];

  constructor(message: string, requirements: SecretGrantRequirement[], details?: unknown) {
    super(428, "secret_grants_required", message, details);
    this.name = "SecretGrantsRequiredError";
    this.requirements = requirements;
  }
}

export function isSecretGrantsRequiredError(
  error: unknown,
): error is SecretGrantsRequiredError {
  return error instanceof SecretGrantsRequiredError
    || (error instanceof AgentHubError
      && error.status === 428
      && error.code === "secret_grants_required");
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
