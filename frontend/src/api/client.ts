export type UserRole = 'member' | 'admin' | 'super_admin';

export type User = {
  id: string;
  username: string;
  email: string | null;
  display_name: string;
  role: UserRole;
};

export type AdminUserDetail = {
  user: User;
  email_verified: boolean;
  has_password: boolean;
  created_at: string;
};

export type AdminSetUserPasswordRequest = {
  password: string;
};

export type AdminSetUserRoleRequest = {
  role: UserRole;
};

export type ReasoningEffort =
  | 'default'
  | 'none'
  | 'minimal'
  | 'low'
  | 'medium'
  | 'high'
  | 'xhigh'
  | 'max'
  | 'ultra';

export type ModelSelection = {
  connection_id: string;
  model_id: string;
};

export type ModelReasoningSummary = 'default' | 'auto' | 'concise' | 'detailed' | 'none';

export type ModelVerbosity = 'default' | 'low' | 'medium' | 'high';

export type ModelReasoningSummarySupport = 'auto' | 'supported' | 'unsupported';

export type ModelUpstreamProtocol =
  | 'openai_responses'
  | 'openai_chat_completions'
  | 'anthropic_messages';

export type ModelRequestSettings =
  | { protocol: 'openai_responses' }
  | {
      protocol: 'openai_chat_completions';
      temperature: number | null;
      top_p: number | null;
      max_completion_tokens: number | null;
    }
  | {
      protocol: 'anthropic_messages';
      temperature: number | null;
      top_p: number | null;
      max_tokens: number | null;
    };

export type AgentModelSettings = {
  reasoning_effort: ReasoningEffort;
  reasoning_summary: ModelReasoningSummary;
  verbosity: ModelVerbosity;
  context_window_tokens: number | null;
  auto_compact_token_limit: number | null;
  reasoning_summary_support: ModelReasoningSummarySupport;
  service_tier: string | null;
  request_max_retries: number | null;
  stream_max_retries: number | null;
  stream_idle_timeout_ms: number | null;
  request_settings: ModelRequestSettings;
};

export type AgentModelSettingsOverride = Partial<{
  reasoning_effort: ReasoningEffort | null;
  reasoning_summary: ModelReasoningSummary | null;
  verbosity: ModelVerbosity | null;
  context_window_tokens: number | null;
  auto_compact_token_limit: number | null;
  reasoning_summary_support: ModelReasoningSummarySupport | null;
  service_tier: string | null;
  request_max_retries: number | null;
  stream_max_retries: number | null;
  stream_idle_timeout_ms: number | null;
  request_settings: ModelRequestSettings | null;
}>;

export type CodexSubagentDefinition = {
  name: string;
  description: string;
  developer_instructions: string;
  model_selection: ModelSelection | null;
  model_settings_override: AgentModelSettingsOverride;
  enabled?: boolean;
  disabled_reason?: string | null;
};

export type Agent = {
  id: string;
  name: string;
  instructions: string;
  visibility: string;
  public_to: string[];
  runtime_id: string | null;
  model_selection: ModelSelection | null;
  model_settings: AgentModelSettings;
  codex_subagents: CodexSubagentDefinition[];
  owner_id: string;
  is_owner: boolean;
  can_manage: boolean;
  can_administer: boolean;
  can_invoke: boolean;
  model_policy: Record<string, unknown>;
  sandbox_policy: Record<string, unknown>;
  managed_skill_ids: string[];
  mcp_allowlist: unknown[];
  created_at: string;
  updated_at: string;
};

export type WidgetAgent = Pick<Agent, 'id' | 'name' | 'instructions'>;

export type Skill = {
  id: string;
  owner_id: string;
  name: string;
  description: string;
  content: string;
  revision: number;
  content_checksum_sha256: string;
  created_at: string;
  updated_at: string;
};

export type Runtime = {
  id: string;
  hostname: string;
  labels: string[];
  codex_version: string;
  capabilities: Record<string, unknown>;
  sandbox_mode: string;
  status: string;
  last_heartbeat_at: string;
  credential_rotation_requested_at: string | null;
};

export type RuntimeEnrollmentToken = {
  id: string;
  created_by: string | null;
  expires_at: string;
  consumed_at: string | null;
  consumed_by_runtime_id: string | null;
  revoked_at: string | null;
  created_at: string;
};

export type RuntimeEnrollmentTokenCreated = {
  enrollment: RuntimeEnrollmentToken;
  token: string;
};

export type Run = {
  id: string;
  agent_id: string;
  automation_id: string | null;
  integration_session_id: string | null;
  parent_run_id: string | null;
  runtime_id: string | null;
  hub_session_id: string | null;
  hub_message_id: string | null;
  hub_turn_id: string | null;
  session_ownership_generation: number | null;
  status: string;
  initial_message: string;
  session_id: string | null;
  work_dir_ref: string | null;
  source: string;
  created_at: string;
  updated_at: string;
};

export type HubSessionOrigin =
  | { kind: 'hub_native' }
  | {
      kind: 'external';
      platform_id: string;
      tenant_id: string;
      external_identity_id: string;
    };

export type CurrentSessionBundle = {
  generation: number;
  object_key: string;
  checksum_sha256: string;
  size_bytes: number;
  history_checkpoint: number;
  ownership_generation: number;
  producing_codex_version: string;
  created_at: string;
};

export type HubSession = {
  id: string;
  owner_id: string;
  agent_id: string;
  agent_name: string;
  agent_deleted_at: string | null;
  origin_platform_name: string | null;
  origin: HubSessionOrigin;
  lifecycle_status: string;
  native_thread_id: string | null;
  active_turn_id: string | null;
  history_checkpoint: number;
  configuration_fingerprint: string | null;
  runtime_owner_id: string | null;
  ownership_generation: number;
  recovery_error: string | null;
  current_bundle: CurrentSessionBundle | null;
  created_at: string;
  updated_at: string;
};

export type RuntimeDrainResponse = {
  runtime: Runtime;
  owned_sessions: HubSession[];
};

export type RuntimeDeletionImpactSession = {
  session_id: string;
  agent_name: string;
  lifecycle_status: string;
  force_delete_disposition: 'recoverable' | 'recovery_failed';
};

export type RuntimeDeletionImpact = {
  runtime_id: string;
  hostname: string;
  affected_sessions: RuntimeDeletionImpactSession[];
};

export type ForceDeleteRuntimeResponse = {
  runtime_id: string;
  recoverable_session_ids: string[];
  recovery_failed_session_ids: string[];
};

export type HubSessionMessage = {
  id: string;
  session_id: string;
  sequence: number;
  role: string;
  message_kind: string;
  content: string | null;
  payload: unknown;
  delivery_mode: string;
  delivery_state: string;
  client_message_key: string | null;
  expected_native_turn_id: string | null;
  turn_id: string | null;
  run_id: string | null;
  accepted_at: string;
};

export type CreateHubSessionMessage = {
  content: string;
  payload?: unknown;
  delivery_mode?: string | null;
  client_message_key?: string | null;
  parent_run_id?: string | null;
};

export type SessionMessageAcceptance = {
  message: HubSessionMessage;
  run: Run | null;
};

export type AuthPolicy = {
  password_registration_enabled: boolean;
  password_login_enabled: boolean;
  email_verification_required: boolean;
};

export type ExternalPlatform = {
  id: string;
  key: string;
  name: string;
};

export type UpdateExternalPlatformRequest = Pick<ExternalPlatform, 'name'>;

export type AuthenticationChannel = {
  id: string;
  platform_id: string;
  key: string;
  name: string;
  enabled: boolean;
  trusted_email: boolean;
};

export type IntegrationAppOptions = {
  external_platforms: ExternalPlatform[];
  authentication_channels: AuthenticationChannel[];
};

export type UserErasure = {
  user_id: string;
  username: string | null;
  status: string;
  requested_at: string;
  completed_at: string | null;
};

export type CodexVersionArtifact = {
  version: string;
  os: string;
  architecture: string;
  artifact_name: string;
  sha256: string;
  size_bytes: number;
};

export type CodexRuntimeReadiness = {
  runtime_id: string;
  hostname: string;
  os: string;
  architecture: string;
  current_version: string;
  target_version: string | null;
  status: string;
  error: string | null;
  checked_at: string | null;
};

export type CodexVersionRollout = {
  active_version: string | null;
  target_version: string | null;
  status: string;
  error: string | null;
  artifacts: CodexVersionArtifact[];
  runtimes: CodexRuntimeReadiness[];
  updated_at: string;
};

export type RunListResponse = {
  items: Run[];
  total: number;
  page: number;
  page_size: number;
};

export type RunEvent = {
  seq: number;
  run_id: string;
  event_type: string;
  role: string | null;
  content: string | null;
  payload: Record<string, unknown>;
  created_at: string;
};

export type Automation = {
  id: string;
  agent_id: string;
  owner_id: string;
  name: string;
  trigger_type: string;
  prompt: string;
  schedule: string | null;
  webhook_token: string | null;
  enabled: boolean;
  last_triggered_at: string | null;
  created_at: string;
};

export type ApiKey = {
  id: string;
  name: string;
  prefix: string;
  last_used_at: string | null;
  expires_at: string | null;
  created_at: string;
};

export type ApiKeyValidity =
  | { kind: 'days'; days: number }
  | { kind: 'date'; expires_at: string }
  | { kind: 'never' };

export type CreateApiKeyRequest = {
  name: string;
  validity?: ApiKeyValidity;
};

export type RenewApiKeyRequest = {
  validity: ApiKeyValidity;
};

export type CreateApiKeyResponse = {
  api_key: ApiKey;
  token: string;
};

export type ApiKeyListResponse = {
  items: ApiKey[];
  total: number;
  page: number;
  page_size: number;
};

export type ModelConnectionScope = 'global' | 'personal';

export type ModelConnectionStatus = 'enabled' | 'disabled';

export type ModelConnection = {
  id: string;
  owner_id: string | null;
  scope: ModelConnectionScope;
  name: string;
  base_url: string;
  api_type: ModelUpstreamProtocol;
  allowed_model_ids: string[];
  status: ModelConnectionStatus;
  has_api_key: boolean;
  created_at: string;
  updated_at: string;
};

export type CreateModelConnectionRequest = {
  scope: ModelConnectionScope;
  name: string;
  base_url: string;
  api_type: ModelUpstreamProtocol;
  allowed_model_ids: string[];
  api_key: string;
};

export type CreateConfiguredAgentRequest = {
  name: string;
  instructions: string;
  visibility: string;
  public_to: string[];
  model_selection: ModelSelection | null;
  model_settings: AgentModelSettings;
  codex_subagents: CodexSubagentDefinition[];
};

export type UpdateModelConnectionRequest = Pick<
  CreateModelConnectionRequest,
  | 'name'
  | 'base_url'
  | 'api_type'
  | 'allowed_model_ids'
> & { api_key?: string };

export type ModelConnectionOption = {
  connection_id: string;
  connection_name: string;
  model_id: string;
  api_type: ModelUpstreamProtocol;
  scope: ModelConnectionScope;
  status: ModelConnectionStatus;
};

export type ModelConnectionOptions = {
  items: ModelConnectionOption[];
  system_default: ModelSelection | null;
};

export type ModelConnectionTestResult = {
  success: boolean;
  status_code: number | null;
  error_code: string | null;
  message: string | null;
  response_text: string | null;
  response_time_ms: number;
};

export type SystemDefaultModelSelection = {
  selection: ModelSelection | null;
};

export type ModelConnectionSnapshot = {
  id: string | null;
  scope: ModelConnectionScope;
  name: string;
  model_id: string;
  api_type: ModelUpstreamProtocol;
};

export type ModelAgentSnapshot = {
  id: string | null;
  name: string;
};

export type ModelUsageSubject = {
  kind: 'user' | 'integration_app' | 'system';
  id: string | null;
  display_name: string | null;
};

export type ModelTokenUsageTotals = {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  cached_tokens: number;
  reasoning_tokens: number;
};

export type ModelTokenUsage = ModelTokenUsageTotals & {
  id: string;
  occurred_at: string;
  response_status: string;
  model: ModelConnectionSnapshot;
  agent: ModelAgentSnapshot;
  subject: ModelUsageSubject;
};

export type ModelCallError = {
  id: string;
  occurred_at: string;
  response_status: string;
  model: ModelConnectionSnapshot;
  agent: ModelAgentSnapshot;
  subject: ModelUsageSubject;
  upstream_status: number | null;
  error_code: string | null;
  message: string | null;
};

export type ModelUsageSummary = {
  overall: ModelTokenUsageTotals;
  by_model: Array<{ model: ModelConnectionSnapshot; totals: ModelTokenUsageTotals }>;
  by_agent: Array<{ agent: ModelAgentSnapshot; totals: ModelTokenUsageTotals }>;
  by_user: Array<{ user_id: string | null; display_name: string | null; totals: ModelTokenUsageTotals }>;
};

export type ModelLedgerCursor = {
  occurred_at_ms: number;
  id: string;
};

export type ModelTokenUsagePage = {
  items: ModelTokenUsage[];
  next_cursor: ModelLedgerCursor | null;
};

export type ModelCallErrorPage = {
  items: ModelCallError[];
  next_cursor: ModelLedgerCursor | null;
};

export type ModelLedgerQuery = {
  from_ms?: number;
  to_ms?: number;
  model_connection_id?: string;
  agent_id?: string;
  user_id?: string;
  cursor_occurred_at_ms?: number;
  cursor_id?: string;
  page_size?: number;
};

export type IntegrationApp = {
  id: string;
  owner_id: string;
  name: string;
  client_id: string;
  external_platform_id: string;
  authentication_channel_id: string;
  redirect_uris: string[];
  agent_ids: string[];
  created_at: string;
  updated_at: string;
};

export type CreateIntegrationAppRequest = {
  name: string;
  external_platform_id: string;
  authentication_channel_id: string;
  redirect_uris: string[];
  agent_ids: string[];
};

export type UpdateIntegrationAppRequest = Pick<
  CreateIntegrationAppRequest,
  'name' | 'redirect_uris' | 'agent_ids'
>;

export type IntegrationAppSecretResponse = {
  integration_app: IntegrationApp;
  client_secret: string;
};

export type WidgetSessionToken = {
  token: string;
};

export type OAuthTokenResponse = {
  access_token: string;
  token_type: string;
  expires_in: number;
  scope: string;
};

export type OAuthExternalProfile = {
  platform_id: string;
  tenant_id: string;
  external_identity_id: string;
  external_user_id: string;
  username?: string;
  email?: string;
};

export type OAuthUserInfo = {
  sub: string;
  username?: string;
  name?: string;
  email?: string;
  external_profile?: OAuthExternalProfile;
};

export type CreateIntegrationSessionRequest = {
  agent_id: string;
  external_user_id: string;
  tenant_id?: string | null;
  tools: unknown;
  metadata: unknown;
};

export type IntegrationSession = {
  id: string;
  hub_session_id: string;
  agent_id: string;
  owner_id: string;
  platform_id: string;
  tenant_id: string;
  external_identity_id: string;
  external_user_id: string;
  tool_definitions: unknown;
  metadata: unknown;
  created_at: string;
};

export type BulkDeleteSkillsResponse = {
  deleted_skill_ids: string[];
};

export type BulkDeleteSkillsRequest = {
  skill_ids: string[];
};

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, {
    ...init,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(init.headers ?? {})
    }
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null) as { error?: unknown } | null;
    const code = body?.error === 'MCP redacted secret cannot be saved without an existing value'
      ? 'mcp_redacted_secret_missing'
      : 'request_failed';
    throw new ApiError(response.status, code);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json();
}

function modelLedgerPath(path: string, query: ModelLedgerQuery = {}) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined) params.set(key, String(value));
  }
  const serialized = params.toString();
  return serialized ? `${path}?${serialized}` : path;
}

export class ApiError extends Error {
  constructor(public readonly status: number, public readonly code: 'request_failed' | 'mcp_redacted_secret_missing') {
    super('Request failed');
    this.name = 'ApiError';
  }
}

async function streamRunEventsWithHeaders(
  runId: string,
  headers: HeadersInit,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void
) {
  const response = await fetch(`/api/runs/${runId}/events/stream`, {
    credentials: 'include',
    headers,
    signal
  });
  if (!response.ok || !response.body) throw new Error('Failed to open widget event stream');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true }).replaceAll('\r\n', '\n');
    let boundary = buffer.indexOf('\n\n');
    while (boundary >= 0) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const eventName = block.split('\n').find((line) => line.startsWith('event:'))?.slice(6).trim();
      const data = block.split('\n').filter((line) => line.startsWith('data:')).map((line) => line.slice(5).trimStart()).join('\n');
      if (eventName === 'run_event' && data) {
        try {
          onEvent(JSON.parse(data) as RunEvent);
        } catch {
          // A malformed frame must not terminate the remaining event stream.
        }
      }
      boundary = buffer.indexOf('\n\n');
    }
  }
}

async function streamRunEvents(
  runId: string,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void
) {
  return streamRunEventsWithHeaders(runId, {}, signal, onEvent);
}

async function streamWidgetRunEvents(
  runId: string,
  token: string,
  signal: AbortSignal,
  onEvent: (event: RunEvent) => void
) {
  return streamRunEventsWithHeaders(
    runId,
    { 'X-Agent-Hub-Embed-Token': token },
    signal,
    onEvent
  );
}

export const api = {
  login: (email: string, password: string) =>
    request<{ user: User }>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password })
    }),
  me: () => request<User>('/api/auth/me'),
  users: (signal?: AbortSignal) => request<User[]>('/api/users', { signal }),
  authProviders: () => request<{
    oidc_mock: boolean;
    password_registration_enabled: boolean;
    password_login_enabled: boolean;
    email_verification_required: boolean;
  }>('/api/auth/providers'),
  authPolicy: (signal?: AbortSignal) => request<AuthPolicy>('/api/admin/auth-policy', { signal }),
  updateAuthPolicy: (policy: AuthPolicy, signal?: AbortSignal) =>
    request<AuthPolicy>('/api/admin/auth-policy', {
      method: 'PATCH',
      body: JSON.stringify(policy),
      signal
    }),
  externalPlatforms: (signal?: AbortSignal) =>
    request<ExternalPlatform[]>('/api/admin/external-platforms', { signal }),
  createExternalPlatform: (key: string, name: string, signal?: AbortSignal) =>
    request<ExternalPlatform>('/api/admin/external-platforms', {
      method: 'POST',
      body: JSON.stringify({ key, name }),
      signal
    }),
  updateExternalPlatform: (platformId: string, name: string, signal?: AbortSignal) =>
    request<ExternalPlatform>(`/api/admin/external-platforms/${platformId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name }),
      signal
    }),
  authenticationChannels: (platformId: string, signal?: AbortSignal) =>
    request<AuthenticationChannel[]>(`/api/admin/external-platforms/${platformId}/authentication-channels`, { signal }),
  createAuthenticationChannel: (
    platformId: string,
    channel: Omit<AuthenticationChannel, 'id' | 'platform_id'>,
    signal?: AbortSignal
  ) => request<AuthenticationChannel>(`/api/admin/external-platforms/${platformId}/authentication-channels`, {
    method: 'POST',
    body: JSON.stringify(channel),
    signal
  }),
  updateAuthenticationChannel: (
    channelId: string,
    channel: Pick<AuthenticationChannel, 'name' | 'enabled' | 'trusted_email'>,
    signal?: AbortSignal
  ) => request<AuthenticationChannel>(`/api/admin/authentication-channels/${channelId}`, {
    method: 'PATCH',
    body: JSON.stringify(channel),
    signal
  }),
  userErasures: (signal?: AbortSignal) =>
    request<UserErasure[]>('/api/admin/user-erasures', { signal }),
  adminUsers: (signal?: AbortSignal) =>
    request<AdminUserDetail[]>('/api/admin/users', { signal }),
  adminUser: (userId: string, signal?: AbortSignal) =>
    request<AdminUserDetail>(`/api/admin/users/${userId}`, { signal }),
  setAdminUserPassword: (userId: string, password: string, signal?: AbortSignal) =>
    request<AdminUserDetail>(`/api/admin/users/${userId}/password`, {
      method: 'PUT',
      body: JSON.stringify({ password }),
      signal
    }),
  setAdminUserRole: (userId: string, role: UserRole, signal?: AbortSignal) =>
    request<AdminUserDetail>(`/api/admin/users/${userId}/role`, {
      method: 'PUT',
      body: JSON.stringify({ role } satisfies AdminSetUserRoleRequest),
      signal
    }),
  eraseUser: (userId: string, username: string, signal?: AbortSignal) =>
    request<UserErasure>(`/api/admin/users/${userId}/erase`, {
      method: 'POST',
      body: JSON.stringify({ username }),
      signal
    }),
  codexVersionRollout: (signal?: AbortSignal) =>
    request<CodexVersionRollout>('/api/admin/codex-version-rollout', { signal }),
  setCodexTargetVersion: (version: string, signal?: AbortSignal) =>
    request<CodexVersionRollout>('/api/admin/codex-version-rollout/target', {
      method: 'PUT',
      body: JSON.stringify({ version }),
      signal
    }),
  promoteCodexTargetVersion: (signal?: AbortSignal) =>
    request<CodexVersionRollout>('/api/admin/codex-version-rollout/promote', {
      method: 'POST',
      signal
    }),
  logout: () => request<void>('/api/auth/logout', { method: 'POST' }),
  apiKeys: (page = 1, pageSize = 20, signal?: AbortSignal) =>
    request<ApiKeyListResponse>(`/api/auth/api-keys?page=${page}&page_size=${pageSize}`, { signal }),
  createApiKey: (name: string, validity?: ApiKeyValidity, signal?: AbortSignal) =>
    request<CreateApiKeyResponse>('/api/auth/api-keys', {
      method: 'POST',
      body: JSON.stringify({ name, ...(validity ? { validity } : {}) }),
      signal
    }),
  renewApiKey: (apiKeyId: string, validity: ApiKeyValidity, signal?: AbortSignal) =>
    request<ApiKey>(`/api/auth/api-keys/${apiKeyId}/renew`, {
      method: 'POST',
      body: JSON.stringify({ validity }),
      signal
    }),
  deleteApiKey: (apiKeyId: string, signal?: AbortSignal) =>
    request<void>(`/api/auth/api-keys/${apiKeyId}`, { method: 'DELETE', signal }),
  modelConnections: (signal?: AbortSignal) =>
    request<ModelConnection[]>('/api/model-connections', { signal }),
  modelConnectionOptions: (signal?: AbortSignal) =>
    request<ModelConnectionOptions>('/api/model-connections/options', { signal }),
  modelConnection: (modelConnectionId: string, signal?: AbortSignal) =>
    request<ModelConnection>(`/api/model-connections/${modelConnectionId}`, { signal }),
  createModelConnection: (connection: CreateModelConnectionRequest, signal?: AbortSignal) =>
    request<ModelConnection>('/api/model-connections', {
      method: 'POST',
      body: JSON.stringify(connection),
      signal
    }),
  updateModelConnection: (
    modelConnectionId: string,
    connection: UpdateModelConnectionRequest,
    force = false,
    signal?: AbortSignal
  ) => request<ModelConnection>(`/api/model-connections/${modelConnectionId}${force ? '?force=true' : ''}`, {
    method: 'PUT',
    body: JSON.stringify(connection),
    signal
  }),
  setModelConnectionStatus: (
    modelConnectionId: string,
    status: ModelConnectionStatus,
    signal?: AbortSignal
  ) => request<ModelConnection>(`/api/model-connections/${modelConnectionId}/status`, {
    method: 'PUT',
    body: JSON.stringify({ status }),
    signal
  }),
  testModelConnection: (modelConnectionId: string, modelId: string, message: string, signal?: AbortSignal) =>
    request<ModelConnectionTestResult>(`/api/model-connections/${modelConnectionId}/test`, {
      method: 'POST',
      body: JSON.stringify({ model_id: modelId, message }),
      signal
    }),
  deleteModelConnection: (modelConnectionId: string, signal?: AbortSignal) =>
    request<void>(`/api/model-connections/${modelConnectionId}`, {
      method: 'DELETE',
      signal
    }),
  forceDeleteModelConnection: (modelConnectionId: string, signal?: AbortSignal) =>
    request<void>(`/api/model-connections/${modelConnectionId}/force-delete`, {
      method: 'POST',
      signal
    }),
  systemDefaultModelSelection: (signal?: AbortSignal) =>
    request<SystemDefaultModelSelection>('/api/model-connections/system-default', { signal }),
  setSystemDefaultModelSelection: (
    selection: ModelSelection | null,
    signal?: AbortSignal
  ) => request<SystemDefaultModelSelection>('/api/model-connections/system-default', {
    method: 'PUT',
    body: JSON.stringify({ selection }),
    signal
  }),
  modelUsageSummary: (query: ModelLedgerQuery = {}, signal?: AbortSignal) =>
    request<ModelUsageSummary>(modelLedgerPath('/api/model-usage/summary', query), { signal }),
  modelTokenUsage: (query: ModelLedgerQuery = {}, signal?: AbortSignal) =>
    request<ModelTokenUsagePage>(modelLedgerPath('/api/model-usage', query), { signal }),
  modelCallErrors: (query: ModelLedgerQuery = {}, signal?: AbortSignal) =>
    request<ModelCallErrorPage>(modelLedgerPath('/api/model-call-errors', query), { signal }),
  agents: (signal?: AbortSignal) => request<Agent[]>('/api/agents', { signal }),
  createAgent: (name: string, instructions: string, visibility: string, publicTo: string[] = [], signal?: AbortSignal) =>
    request<Agent>('/api/agents', {
      method: 'POST',
      body: JSON.stringify({ name, instructions, visibility, public_to: publicTo }),
      signal
    }),
  createConfiguredAgent: (agent: CreateConfiguredAgentRequest, signal?: AbortSignal) =>
    request<Agent>('/api/agents', {
      method: 'POST',
      body: JSON.stringify(agent),
      signal
    }),
  agent: (agentId: string, signal?: AbortSignal) => request<Agent>(`/api/agents/${agentId}`, { signal }),
  agentModelConnectionOptions: (agentId: string, signal?: AbortSignal) =>
    request<ModelConnectionOptions>(`/api/agents/${agentId}/model-options`, { signal }),
  updateAgent: (agentId: string, agent: Agent, signal?: AbortSignal) =>
    request<Agent>(`/api/agents/${agentId}`, {
      method: 'PATCH',
      body: JSON.stringify({
        name: agent.name,
        instructions: agent.instructions,
        visibility: agent.visibility,
        public_to: agent.public_to,
        runtime_id: agent.runtime_id,
        model_selection: agent.model_selection,
        model_settings: agent.model_settings,
        codex_subagents: agent.codex_subagents,
        sandbox_policy: agent.sandbox_policy,
        managed_skill_ids: agent.managed_skill_ids,
        mcp_allowlist: agent.mcp_allowlist
      }),
      signal
    }),
  deleteAgent: (agentId: string, signal?: AbortSignal) =>
    request<void>(`/api/agents/${agentId}`, { method: 'DELETE', signal }),
  integrationAppOptions: (signal?: AbortSignal) =>
    request<IntegrationAppOptions>('/api/integration-app-options', { signal }),
  integrationApps: (signal?: AbortSignal) =>
    request<IntegrationApp[]>('/api/integration-apps', { signal }),
  integrationApp: (integrationAppId: string, signal?: AbortSignal) =>
    request<IntegrationApp>(`/api/integration-apps/${integrationAppId}`, { signal }),
  createIntegrationApp: (integrationApp: CreateIntegrationAppRequest, signal?: AbortSignal) =>
    request<IntegrationAppSecretResponse>('/api/integration-apps', {
      method: 'POST',
      body: JSON.stringify(integrationApp),
      signal
    }),
  updateIntegrationApp: (
    integrationAppId: string,
    integrationApp: UpdateIntegrationAppRequest,
    signal?: AbortSignal
  ) => request<IntegrationApp>(`/api/integration-apps/${integrationAppId}`, {
    method: 'PATCH',
    body: JSON.stringify(integrationApp),
    signal
  }),
  rotateIntegrationAppSecret: (integrationAppId: string, signal?: AbortSignal) =>
    request<IntegrationAppSecretResponse>(`/api/integration-apps/${integrationAppId}/rotate-secret`, {
      method: 'POST',
      signal
    }),
  createIntegrationAppWidgetSession: (
    integrationAppId: string,
    agentId: string,
    signal?: AbortSignal
  ) => request<WidgetSessionToken>(
    `/api/integration-apps/${integrationAppId}/agents/${agentId}/widget-session`,
    { method: 'POST', signal }
  ),
  skills: (signal?: AbortSignal) => request<Skill[]>('/api/skills', { signal }),
  skill: (skillId: string, signal?: AbortSignal) =>
    request<Skill>(`/api/skills/${skillId}`, { signal }),
  createSkill: (name: string, description: string, content: string) =>
    request<Skill>('/api/skills', {
      method: 'POST',
      body: JSON.stringify({ name, description, content })
    }),
  updateSkill: (skillId: string, name: string, description: string, content: string) =>
    request<Skill>(`/api/skills/${skillId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name, description, content })
    }),
  deleteSkill: (skillId: string, signal?: AbortSignal) =>
    request<void>(`/api/skills/${skillId}`, { method: 'DELETE', signal }),
  bulkDeleteSkills: (skillIds: string[], signal?: AbortSignal) =>
    request<BulkDeleteSkillsResponse>('/api/skills', {
      method: 'DELETE',
      body: JSON.stringify({ skill_ids: skillIds }),
      signal
    }),
  runs: (agentId: string, signal?: AbortSignal) => request<Run[]>(`/api/agents/${agentId}/runs`, { signal }),
  sessions: (signal?: AbortSignal) => request<HubSession[]>('/api/sessions', { signal }),
  session: (sessionId: string, signal?: AbortSignal) =>
    request<HubSession>(`/api/sessions/${sessionId}`, { signal }),
  sessionMessages: (sessionId: string, signal?: AbortSignal) =>
    request<HubSessionMessage[]>(`/api/sessions/${sessionId}/messages`, { signal }),
  createSessionMessage: (sessionId: string, message: CreateHubSessionMessage, signal?: AbortSignal) =>
    request<SessionMessageAcceptance>(`/api/sessions/${sessionId}/messages`, {
      method: 'POST',
      body: JSON.stringify(message),
      signal
    }),
  stopRun: (runId: string, signal?: AbortSignal) =>
    request<Run>(`/api/runs/${runId}/stop`, { method: 'POST', signal }),
  createRun: (
    agentId: string,
    message: string,
    hubSessionId: string | null = null,
    parentRunId: string | null = null,
    signal?: AbortSignal
  ) =>
    request<Run>(`/api/agents/${agentId}/runs`, {
      method: 'POST',
      body: JSON.stringify({ message, hub_session_id: hubSessionId, parent_run_id: parentRunId }),
      signal
    }),
  runEvents: (runId: string, signal?: AbortSignal) => request<RunEvent[]>(`/api/runs/${runId}/events`, { signal }),
  runtimes: (signal?: AbortSignal) => request<Runtime[]>('/api/runtimes', { signal }),
  runtimeEnrollmentTokens: (signal?: AbortSignal) =>
    request<RuntimeEnrollmentToken[]>('/api/admin/runtime-enrollment-tokens', { signal }),
  createRuntimeEnrollmentToken: () =>
    request<RuntimeEnrollmentTokenCreated>('/api/admin/runtime-enrollment-tokens', {
      method: 'POST'
    }),
  revokeRuntimeEnrollmentToken: (enrollmentId: string) =>
    request<RuntimeEnrollmentToken>(`/api/admin/runtime-enrollment-tokens/${enrollmentId}/revoke`, {
      method: 'POST'
    }),
  requestRuntimeCredentialRotation: (runtimeId: string) =>
    request<Runtime>(`/api/admin/runtimes/${runtimeId}/credential-rotation`, {
      method: 'POST'
    }),
  runtimeDeletionImpact: (runtimeId: string, signal?: AbortSignal) =>
    request<RuntimeDeletionImpact>(`/api/admin/runtimes/${runtimeId}/deletion-impact`, { signal }),
  drainRuntime: (runtimeId: string, hostname: string) =>
    request<RuntimeDrainResponse>(`/api/admin/runtimes/${runtimeId}/drain`, {
      method: 'POST',
      body: JSON.stringify({ hostname })
    }),
  cancelRuntimeDrain: (runtimeId: string) =>
    request<RuntimeDrainResponse>(`/api/admin/runtimes/${runtimeId}/cancel-drain`, {
      method: 'POST'
    }),
  deleteRuntime: (runtimeId: string, hostname: string) =>
    request<void>(`/api/admin/runtimes/${runtimeId}`, {
      method: 'DELETE',
      body: JSON.stringify({ hostname })
    }),
  forceDeleteRuntime: (runtimeId: string, hostname: string) =>
    request<ForceDeleteRuntimeResponse>(`/api/admin/runtimes/${runtimeId}/force-delete`, {
      method: 'POST',
      body: JSON.stringify({ hostname })
    }),
  automations: () => request<Automation[]>('/api/automations'),
  automationRuns: (automationId: string, page = 1, pageSize = 20, signal?: AbortSignal) =>
    request<RunListResponse>(`/api/automations/${automationId}/runs?page=${page}&page_size=${pageSize}`, { signal }),
  createAutomation: (agentId: string, name: string, triggerType: string, prompt: string, schedule: string, enabled: boolean) =>
    request<Automation>('/api/automations', {
      method: 'POST',
      body: JSON.stringify({ agent_id: agentId, name, trigger_type: triggerType, prompt, schedule: schedule || null, enabled })
    }),
  updateAutomation: (automationId: string, name: string, triggerType: string, prompt: string, schedule: string, enabled: boolean) =>
    request<Automation>(`/api/automations/${automationId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name, trigger_type: triggerType, prompt, schedule: schedule || null, enabled })
    }),
  triggerAutomation: (automationId: string, message?: string) =>
    request<Run>(`/api/automations/${automationId}/trigger`, {
      method: 'POST',
      body: JSON.stringify({ message })
    }),
  triggerAutomationWebhook: (token: string, message?: string) =>
    request<Run>('/api/automations/webhook', {
      method: 'POST',
      headers: { 'X-Agent-Hub-Webhook-Token': token },
      body: JSON.stringify({ message })
    }),
  createEmbedSession: (agentId: string, signal?: AbortSignal) =>
    request<{ token: string }>('/api/embed/sessions', {
      method: 'POST',
      body: JSON.stringify({ agent_id: agentId }),
      signal
    }),
  exchangeEmbedJwt: (jwt: string) =>
    request<{ token: string }>('/api/embed/exchange', {
      method: 'POST',
      body: JSON.stringify({ jwt })
    }),
  oauthUserInfo: (accessToken: string, signal?: AbortSignal) =>
    request<OAuthUserInfo>('/api/oauth/userinfo', {
      headers: { Authorization: `Bearer ${accessToken}` },
      signal
    }),
  createIntegrationSession: (
    accessToken: string,
    integrationSession: CreateIntegrationSessionRequest,
    signal?: AbortSignal
  ) => request<IntegrationSession>('/api/integrations/sessions', {
    method: 'POST',
    headers: { Authorization: `Bearer ${accessToken}` },
    body: JSON.stringify(integrationSession),
    signal
  }),
  widgetAgent: (token: string) => request<WidgetAgent>('/api/widget/session', {
    headers: { 'X-Agent-Hub-Embed-Token': token }
  }),
  createWidgetRun: (token: string, message: string) =>
    request<Run>('/api/widget/runs', {
      method: 'POST',
      headers: { 'X-Agent-Hub-Embed-Token': token },
      body: JSON.stringify({ message, parent_run_id: null })
    }),
  streamRunEvents,
  streamWidgetRunEvents
};
