use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use uuid::Uuid;

pub const ATOMIC_WAITING_TOOL_BATCH_CAPABILITY: &str = "atomic_waiting_tool_batch";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDto {
    pub id: Uuid,
    pub username: String,
    pub email: Option<String>,
    pub display_name: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminUserDetailDto {
    pub user: UserDto,
    pub email_verified: bool,
    pub has_password: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdminSetUserPasswordRequest {
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdminSetUserRoleRequest {
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EraseUserRequest {
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserErasureDto {
    pub user_id: Uuid,
    pub username: Option<String>,
    pub status: String,
    pub requested_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthPolicyDto {
    pub password_registration_enabled: bool,
    pub password_login_enabled: bool,
    pub email_verification_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthenticationChannelDto {
    pub id: Uuid,
    pub platform_id: Uuid,
    pub key: String,
    pub name: String,
    pub enabled: bool,
    pub trusted_email: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalPlatformDto {
    pub id: Uuid,
    pub key: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntegrationAppOptionsDto {
    pub external_platforms: Vec<ExternalPlatformDto>,
    pub authentication_channels: Vec<AuthenticationChannelDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateExternalPlatformRequest {
    pub key: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateExternalPlatformRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAuthenticationChannelRequest {
    pub key: String,
    pub name: String,
    pub enabled: bool,
    pub trusted_email: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAuthenticationChannelRequest {
    pub name: String,
    pub enabled: bool,
    pub trusted_email: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HubSessionOriginDto {
    HubNative,
    External {
        platform_id: Uuid,
        tenant_id: String,
        external_identity_id: Uuid,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CurrentSessionBundleDto {
    pub generation: i64,
    pub object_key: String,
    pub checksum_sha256: String,
    pub size_bytes: i64,
    pub history_checkpoint: i64,
    pub ownership_generation: i64,
    pub producing_codex_version: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubSessionDto {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub agent_deleted_at: Option<DateTime<Utc>>,
    pub origin: HubSessionOriginDto,
    pub lifecycle_status: String,
    pub native_thread_id: Option<String>,
    pub active_turn_id: Option<Uuid>,
    pub history_checkpoint: i64,
    pub configuration_fingerprint: Option<String>,
    pub runtime_owner_id: Option<Uuid>,
    pub ownership_generation: i64,
    pub recovery_error: Option<String>,
    pub current_bundle: Option<CurrentSessionBundleDto>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubSessionTurnDto {
    pub id: Uuid,
    pub session_id: Uuid,
    pub native_turn_id: Option<String>,
    pub status: String,
    pub configuration_fingerprint: Option<String>,
    pub ownership_generation: i64,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubSessionMessageDto {
    pub id: Uuid,
    pub session_id: Uuid,
    pub sequence: i64,
    pub role: String,
    pub message_kind: String,
    pub content: Option<String>,
    pub payload: Value,
    pub delivery_mode: String,
    pub delivery_state: String,
    #[serde(default)]
    pub client_message_key: Option<String>,
    pub expected_native_turn_id: Option<String>,
    pub turn_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelConnectionScope {
    Global,
    Personal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelConnectionStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionDto {
    pub id: Uuid,
    pub owner_id: Option<Uuid>,
    pub scope: ModelConnectionScope,
    pub name: String,
    pub base_url: String,
    pub model_id: String,
    pub status: ModelConnectionStatus,
    pub is_system_default: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateModelConnectionRequest {
    pub scope: ModelConnectionScope,
    pub name: String,
    pub base_url: String,
    pub model_id: String,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateModelConnectionRequest {
    pub name: String,
    pub base_url: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateModelConnectionStatusRequest {
    pub status: ModelConnectionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionOptionDto {
    pub id: Uuid,
    pub name: String,
    pub model_id: String,
    pub scope: ModelConnectionScope,
    pub status: ModelConnectionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionOptionsDto {
    pub items: Vec<ModelConnectionOptionDto>,
    pub system_default_model_connection_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionTestResultDto {
    pub success: bool,
    pub status_code: Option<u16>,
    pub error_code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SystemDefaultModelConnectionDto {
    pub model_connection_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SetSystemDefaultModelConnectionRequest {
    pub model_connection_id: Option<Uuid>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenUsageQueryDto {
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub model_connection_id: Option<Uuid>,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub cursor_occurred_at_ms: Option<i64>,
    pub cursor_id: Option<Uuid>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelCallErrorQueryDto {
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub model_connection_id: Option<Uuid>,
    pub agent_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub cursor_occurred_at_ms: Option<i64>,
    pub cursor_id: Option<Uuid>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelLedgerCursorDto {
    pub occurred_at_ms: i64,
    pub id: Uuid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelUsageSubjectKind {
    User,
    IntegrationApp,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageSubjectDto {
    pub kind: ModelUsageSubjectKind,
    pub id: Option<Uuid>,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionSnapshotDto {
    pub id: Option<Uuid>,
    pub scope: ModelConnectionScope,
    pub name: String,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelAgentSnapshotDto {
    pub id: Option<Uuid>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenUsageTotalsDto {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenUsageDto {
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub response_status: String,
    pub model: ModelConnectionSnapshotDto,
    pub agent: ModelAgentSnapshotDto,
    pub subject: ModelUsageSubjectDto,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelCallErrorDto {
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub response_status: String,
    pub model: ModelConnectionSnapshotDto,
    pub agent: ModelAgentSnapshotDto,
    pub subject: ModelUsageSubjectDto,
    pub upstream_status: Option<u16>,
    pub error_code: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageModelSummaryDto {
    pub model: ModelConnectionSnapshotDto,
    pub totals: ModelTokenUsageTotalsDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageAgentSummaryDto {
    pub agent: ModelAgentSnapshotDto,
    pub totals: ModelTokenUsageTotalsDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageUserSummaryDto {
    pub user_id: Option<Uuid>,
    pub display_name: Option<String>,
    pub totals: ModelTokenUsageTotalsDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelUsageSummaryDto {
    pub overall: ModelTokenUsageTotalsDto,
    pub by_model: Vec<ModelUsageModelSummaryDto>,
    pub by_agent: Vec<ModelUsageAgentSummaryDto>,
    pub by_user: Vec<ModelUsageUserSummaryDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenUsagePageDto {
    pub items: Vec<ModelTokenUsageDto>,
    pub next_cursor: Option<ModelLedgerCursorDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelCallErrorPageDto {
    pub items: Vec<ModelCallErrorDto>,
    pub next_cursor: Option<ModelLedgerCursorDto>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    #[default]
    Default,
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
    Ultra,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CodexSubagentDefinition {
    pub name: String,
    pub description: String,
    pub developer_instructions: String,
    #[serde(default)]
    pub model_connection_id: Option<Uuid>,
    #[serde(default)]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_reason: Option<String>,
}

fn default_true() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDto {
    pub id: Uuid,
    pub name: String,
    pub instructions: String,
    pub visibility: String,
    pub public_to: Vec<Uuid>,
    pub runtime_id: Option<Uuid>,
    #[serde(default)]
    pub default_model_connection_id: Option<Uuid>,
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default)]
    pub codex_subagents: Vec<CodexSubagentDefinition>,
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub managed_skill_ids: Vec<Uuid>,
    pub mcp_allowlist: Value,
    pub owner_id: Uuid,
    pub is_owner: bool,
    pub can_manage: bool,
    pub can_administer: bool,
    pub can_invoke: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetAgentDto {
    pub id: Uuid,
    pub name: String,
    pub instructions: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDto {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub description: String,
    pub content: String,
    pub revision: i64,
    pub content_checksum_sha256: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentExecutionConfigurationDto {
    pub revision: i64,
    pub instructions: String,
    #[serde(default)]
    pub default_model_connection_id: Option<Uuid>,
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default)]
    pub codex_subagents: Vec<CodexSubagentDefinition>,
    #[serde(default)]
    pub model_connections: Vec<ModelConnectionOptionDto>,
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub skills: Vec<AgentExecutionSkillDto>,
    pub mcp_allowlist: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentExecutionSkillDto {
    pub source: String,
    pub source_id: Option<Uuid>,
    pub name: String,
    pub description: String,
    pub content: String,
    pub revision: i64,
    pub content_checksum_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionConfigurationError(&'static str);

impl fmt::Display for ExecutionConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ExecutionConfigurationError {}

pub fn execution_configuration_fingerprint(
    configuration: &AgentExecutionConfigurationDto,
) -> Result<String, ExecutionConfigurationError> {
    if configuration.revision <= 0 {
        return Err(ExecutionConfigurationError(
            "execution configuration revision must be positive",
        ));
    }

    let mut model_connection_ids = BTreeSet::new();
    let mut model_connections = configuration.model_connections.clone();
    for connection in &model_connections {
        if !model_connection_ids.insert(connection.id) {
            return Err(ExecutionConfigurationError(
                "Model Connection ids must be unique",
            ));
        }
        if connection.name.trim().is_empty() || connection.model_id.trim().is_empty() {
            return Err(ExecutionConfigurationError(
                "Model Connection name and model id are required",
            ));
        }
    }
    if configuration
        .default_model_connection_id
        .is_some_and(|id| !model_connection_ids.contains(&id))
    {
        return Err(ExecutionConfigurationError(
            "default Model Connection must be included in the execution configuration",
        ));
    }
    model_connections.sort_by_key(|connection| connection.id);
    let model_connection_metadata = model_connections
        .iter()
        .map(|connection| {
            json!({
                "id": connection.id,
                "name": connection.name,
                "model_id": connection.model_id,
                "scope": connection.scope,
                "status": connection.status,
            })
        })
        .collect::<Vec<_>>();

    let mut subagent_names = BTreeSet::new();
    let mut codex_subagents = configuration.codex_subagents.clone();
    for subagent in &codex_subagents {
        let normalized_name = subagent.name.trim().to_lowercase();
        if normalized_name.is_empty()
            || subagent.description.trim().is_empty()
            || subagent.developer_instructions.trim().is_empty()
        {
            return Err(ExecutionConfigurationError(
                "Codex Subagent name, description, and developer instructions are required",
            ));
        }
        if !subagent_names.insert(normalized_name) {
            return Err(ExecutionConfigurationError(
                "Codex Subagent names must be unique ignoring case",
            ));
        }
        match (subagent.enabled, subagent.disabled_reason.as_deref()) {
            (true, None) => {}
            (false, Some(reason)) if !reason.trim().is_empty() => {}
            _ => {
                return Err(ExecutionConfigurationError(
                    "Codex Subagent enabled and disabled reason shape is invalid",
                ));
            }
        }
        if subagent
            .model_connection_id
            .is_some_and(|id| !model_connection_ids.contains(&id))
        {
            return Err(ExecutionConfigurationError(
                "Codex Subagent Model Connection must be included in the execution configuration",
            ));
        }
    }
    codex_subagents.sort_by_key(|subagent| subagent.name.trim().to_lowercase());

    let mut names = BTreeSet::new();
    let mut skills = configuration.skills.clone();
    for skill in &skills {
        if skill.revision <= 0 {
            return Err(ExecutionConfigurationError(
                "Skill revision must be positive",
            ));
        }
        if skill.name.trim().is_empty() || skill.content.trim().is_empty() {
            return Err(ExecutionConfigurationError(
                "Skill name and content are required",
            ));
        }
        if !names.insert(skill.name.clone()) {
            return Err(ExecutionConfigurationError(
                "effective Skill names must be unique",
            ));
        }
        if skill.source != "managed" || skill.source_id.is_none() {
            return Err(ExecutionConfigurationError(
                "only managed Skills with a source id are supported",
            ));
        }
        if sha256_hex(skill.content.as_bytes()) != skill.content_checksum_sha256 {
            return Err(ExecutionConfigurationError(
                "Skill content checksum does not match",
            ));
        }
    }
    skills.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.source.cmp(&right.source))
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    let skill_metadata = skills
        .iter()
        .map(|skill| {
            json!({
                "source": skill.source,
                "source_id": skill.source_id,
                "name": skill.name,
                "description": skill.description,
                "revision": skill.revision,
                "content_checksum_sha256": skill.content_checksum_sha256,
            })
        })
        .collect::<Vec<_>>();
    let mcp_allowlist = normalized_redacted_mcp(&configuration.mcp_allowlist)?;
    let value = json!({
        "revision": configuration.revision,
        "instructions": configuration.instructions,
        "default_model_connection_id": configuration.default_model_connection_id,
        "reasoning_effort": configuration.reasoning_effort,
        "codex_subagents": codex_subagents,
        "model_connections": model_connection_metadata,
        "model_policy": configuration.model_policy,
        "sandbox_policy": configuration.sandbox_policy,
        "skills": skill_metadata,
        "mcp_allowlist": mcp_allowlist,
    });
    Ok(format!(
        "sha256:{}",
        sha256_hex(canonical_json(&value).as_bytes())
    ))
}

fn normalized_redacted_mcp(value: &Value) -> Result<Value, ExecutionConfigurationError> {
    let Some(servers) = value.as_array() else {
        return Err(ExecutionConfigurationError(
            "MCP allowlist must be an array",
        ));
    };
    let mut servers = servers.clone();
    for server in &mut servers {
        let Some(server) = server.as_object_mut() else {
            return Err(ExecutionConfigurationError("MCP entries must be objects"));
        };
        if let Some(secrets) = server.get_mut("secrets").and_then(Value::as_object_mut) {
            for secret in secrets.values_mut() {
                *secret = Value::String("<redacted>".into());
            }
        }
    }
    servers.sort_by_key(canonical_json);
    Ok(Value::Array(servers))
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical_json(value: &Value) -> String {
    fn write(value: &Value, output: &mut String) {
        match value {
            Value::Null => output.push_str("null"),
            Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
            Value::Number(value) => output.push_str(&value.to_string()),
            Value::String(value) => output.push_str(
                &serde_json::to_string(value).expect("JSON strings are always serializable"),
            ),
            Value::Array(values) => {
                output.push('[');
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write(value, output);
                }
                output.push(']');
            }
            Value::Object(values) => {
                output.push('{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort_unstable();
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    output.push_str(
                        &serde_json::to_string(key).expect("JSON object keys are serializable"),
                    );
                    output.push(':');
                    write(&values[key], output);
                }
                output.push('}');
            }
        }
    }

    let mut output = String::new();
    write(value, &mut output);
    output
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDto {
    pub id: Uuid,
    pub hostname: String,
    pub labels: Vec<String>,
    pub codex_version: String,
    pub capabilities: Value,
    pub sandbox_mode: String,
    pub status: String,
    pub last_heartbeat_at: DateTime<Utc>,
    pub credential_rotation_requested_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SetCodexTargetVersionRequest {
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexVersionArtifactDto {
    pub version: String,
    pub os: String,
    pub architecture: String,
    pub artifact_name: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCodexStatusDto {
    pub current_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeCodexRolloutCommandDto {
    pub active_version: Option<String>,
    pub target_artifact: Option<CodexVersionArtifactDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexRuntimeReadinessDto {
    pub runtime_id: Uuid,
    pub hostname: String,
    pub os: String,
    pub architecture: String,
    pub current_version: String,
    pub target_version: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub checked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexVersionRolloutDto {
    pub active_version: Option<String>,
    pub target_version: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub artifacts: Vec<CodexVersionArtifactDto>,
    pub runtimes: Vec<CodexRuntimeReadinessDto>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeEnrollmentTokenDto {
    pub id: Uuid,
    pub created_by: Option<Uuid>,
    pub expires_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub consumed_by_runtime_id: Option<Uuid>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRuntimeEnrollmentTokenResponse {
    pub enrollment: RuntimeEnrollmentTokenDto,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunDto {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub automation_id: Option<Uuid>,
    pub integration_session_id: Option<Uuid>,
    pub parent_run_id: Option<Uuid>,
    pub runtime_id: Option<Uuid>,
    #[serde(default)]
    pub hub_session_id: Option<Uuid>,
    #[serde(default)]
    pub hub_message_id: Option<Uuid>,
    #[serde(default)]
    pub hub_turn_id: Option<Uuid>,
    #[serde(default)]
    pub session_ownership_generation: Option<i64>,
    pub status: String,
    pub initial_message: String,
    pub session_id: Option<String>,
    pub work_dir_ref: Option<String>,
    pub source: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunListResponse {
    pub items: Vec<RunDto>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResumeDto {
    pub thread_id: String,
    pub work_dir_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEventDto {
    pub seq: i64,
    pub event_id: Uuid,
    pub run_id: Uuid,
    pub event_type: String,
    pub role: Option<String>,
    pub content: Option<String>,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationDto {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub trigger_type: String,
    pub prompt: String,
    pub schedule: Option<String>,
    pub webhook_token: Option<String>,
    pub enabled: bool,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyDto {
    pub id: Uuid,
    pub name: String,
    pub prefix: String,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyListResponse {
    pub items: Vec<ApiKeyDto>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationAppDto {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub client_id: String,
    pub external_platform_id: Uuid,
    pub authentication_channel_id: Uuid,
    pub redirect_uris: Value,
    pub agent_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationSessionDto {
    pub id: Uuid,
    pub hub_session_id: Uuid,
    pub agent_id: Uuid,
    pub owner_id: Uuid,
    pub platform_id: Uuid,
    pub tenant_id: String,
    pub external_identity_id: Uuid,
    pub external_user_id: String,
    pub tool_definitions: Value,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationToolRequestDto {
    pub id: Uuid,
    pub session_id: Uuid,
    pub run_id: Uuid,
    pub tool_name: String,
    pub arguments: Value,
    pub status: String,
    pub result_payload: Option<Value>,
    pub follow_up_run_id: Option<Uuid>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationContextDto {
    pub tools: Value,
    pub attachments: Value,
    pub tool_result: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub user: UserDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordRegistrationRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordRegistrationResponse {
    pub user: UserDto,
    pub verification_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApiKeyValidity {
    Days { days: u32 },
    Date { expires_at: DateTime<Utc> },
    Never,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateApiKeyRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validity: Option<ApiKeyValidity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RenewApiKeyRequest {
    pub validity: ApiKeyValidity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateApiKeyResponse {
    pub api_key: ApiKeyDto,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthProvidersResponse {
    pub oidc_mock: bool,
    pub password_registration_enabled: bool,
    pub password_login_enabled: bool,
    pub email_verification_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateIntegrationAppRequest {
    pub name: String,
    pub external_platform_id: Uuid,
    pub authentication_channel_id: Uuid,
    pub redirect_uris: Value,
    pub agent_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateIntegrationAppRequest {
    pub name: String,
    pub redirect_uris: Value,
    pub agent_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationAppSecretResponse {
    pub integration_app: IntegrationAppDto,
    pub client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: i64,
    pub scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthExternalProfileDto {
    pub platform_id: Uuid,
    pub tenant_id: String,
    pub external_identity_id: Uuid,
    pub external_user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthUserInfoDto {
    pub sub: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_profile: Option<OAuthExternalProfileDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAgentRequest {
    pub name: String,
    pub instructions: String,
    pub visibility: String,
    #[serde(default)]
    pub public_to: Vec<Uuid>,
    #[serde(default)]
    pub default_model_connection_id: Option<Uuid>,
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default)]
    pub codex_subagents: Vec<CodexSubagentDefinition>,
}

fn legacy_hub_proxy_model_policy() -> Value {
    json!({ "provider": "hub-proxy" })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAgentRequest {
    pub name: String,
    pub instructions: String,
    pub visibility: String,
    #[serde(default)]
    pub public_to: Vec<Uuid>,
    pub runtime_id: Option<Uuid>,
    #[serde(default)]
    pub default_model_connection_id: Option<Uuid>,
    #[serde(default)]
    pub reasoning_effort: ReasoningEffort,
    #[serde(default)]
    pub codex_subagents: Vec<CodexSubagentDefinition>,
    #[doc(hidden)]
    #[serde(skip, default = "legacy_hub_proxy_model_policy")]
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub managed_skill_ids: Vec<Uuid>,
    pub mcp_allowlist: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSkillRequest {
    pub name: String,
    pub description: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSkillRequest {
    pub name: String,
    pub description: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BulkDeleteSkillsRequest {
    pub skill_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BulkDeleteSkillsResponse {
    pub deleted_skill_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRunRequest {
    pub message: String,
    #[serde(default)]
    pub hub_session_id: Option<Uuid>,
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
    #[serde(default)]
    pub client_message_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIntegrationSessionRequest {
    pub agent_id: Uuid,
    pub external_user_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub tools: Value,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIntegrationMessageRequest {
    pub content: String,
    pub attachments: Value,
    pub client_message_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationMessageResponse {
    pub run: RunDto,
    pub message: HubSessionMessageDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateHubSessionMessageRequest {
    pub content: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub delivery_mode: Option<String>,
    #[serde(default)]
    pub client_message_key: Option<String>,
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMessageAcceptanceDto {
    pub message: HubSessionMessageDto,
    pub run: Option<RunDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitToolResultRequest {
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitToolResultResponse {
    pub run: RunDto,
    pub tool_request: IntegrationToolRequestDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAutomationRequest {
    pub agent_id: Uuid,
    pub name: String,
    pub trigger_type: String,
    pub prompt: String,
    pub schedule: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAutomationRequest {
    pub name: String,
    pub trigger_type: String,
    pub prompt: String,
    pub schedule: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerAutomationRequest {
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEmbedSessionRequest {
    pub agent_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEmbedSessionResponse {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeEmbedJwtRequest {
    pub jwt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRegisterRequest {
    pub hostname: String,
    pub labels: Vec<String>,
    pub codex_version: String,
    pub capabilities: Value,
    pub sandbox_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRegisterResponse {
    pub runtime_id: Uuid,
    pub runtime_credential: String,
    #[serde(default)]
    pub protocol_capabilities: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeHeartbeatRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_credential_hash: Option<String>,
    #[serde(default)]
    pub accepts_session_commands: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owned_sessions: Vec<RuntimeOwnedSessionStateRequest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cleaned_sessions: Vec<RuntimeOwnedSessionGenerationDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_status: Option<RuntimeCodexStatusDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOwnedSessionStateRequest {
    pub session_id: Uuid,
    pub ownership_generation: i64,
    pub lifecycle_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeHeartbeatResponse {
    pub rotation_requested: bool,
    pub pending_credential_accepted: bool,
    pub credential_activated: bool,
    pub runtime_status: String,
    #[serde(default)]
    pub owned_sessions: Vec<RuntimeOwnedSessionSnapshotDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cleanup_sessions: Vec<RuntimeOwnedSessionGenerationDto>,
    pub session_commands: Vec<RuntimeSessionCommandDto>,
    #[serde(default)]
    pub codex_rollout: RuntimeCodexRolloutCommandDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOwnedSessionSnapshotDto {
    pub session_id: Uuid,
    pub ownership_generation: i64,
    pub lifecycle_status: String,
    pub native_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_run_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOwnedSessionGenerationDto {
    pub session_id: Uuid,
    pub ownership_generation: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeClaimRunRequest {
    pub available_new_session_slots: u32,
    pub ready_owned_sessions: Vec<RuntimeOwnedSessionGenerationDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionCommandDto {
    pub command_id: Uuid,
    pub session_id: Uuid,
    pub ownership_generation: i64,
    pub command: String,
    pub run_id: Option<Uuid>,
    pub turn_id: Option<Uuid>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub message: Option<RuntimeSteeringMessageDto>,
    pub configuration_revision: Option<i64>,
    pub fingerprint: Option<String>,
    pub execution_configuration: Option<AgentExecutionConfigurationDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSteeringMessageDto {
    pub id: Uuid,
    pub sequence: i64,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CompleteRuntimeSessionCommandRequest {
    pub command: String,
    pub outcome: String,
    pub revision: Option<i64>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompleteRuntimeSessionCommandResponse {
    pub command_id: Uuid,
    pub outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRunResponse {
    pub run: RunDto,
    pub agent: AgentDto,
    pub execution_configuration: AgentExecutionConfigurationDto,
    pub expected_configuration_fingerprint: String,
    pub integration_context: Option<IntegrationContextDto>,
    pub resume: Option<RunResumeDto>,
    pub model_proxy_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_context: Option<ClaimSessionContextDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimSessionContextDto {
    pub session: HubSessionDto,
    pub turn: HubSessionTurnDto,
    pub messages: Vec<HubSessionMessageDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BeginRuntimeTurnRequest {
    pub configuration_fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginRuntimeTurnResponse {
    pub session_id: Uuid,
    pub turn_id: Uuid,
    pub ownership_generation: i64,
    pub configuration_fingerprint: String,
    pub messages: Vec<HubSessionMessageDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSessionWriteRequest<T> {
    pub ownership_generation: i64,
    pub payload: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseRuntimeSessionRequest {
    pub ownership_generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BeginRuntimeSessionCheckpointRequest {
    pub ownership_generation: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionCheckpointAttemptDto {
    pub checkpoint_attempt_id: Uuid,
    pub history_checkpoint: i64,
    pub bundle_generation: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionBundleCommitResponseDto {
    pub checkpoint_attempt_id: Uuid,
    pub bundle_generation: i64,
    pub has_queued_work: bool,
    pub ownership_released: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FailRuntimeSessionCheckpointRequest {
    pub ownership_generation: i64,
    pub checkpoint_attempt_id: Uuid,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSessionCheckpointDispositionDto {
    pub checkpoint_attempt_id: Uuid,
    pub disposition: String,
    pub has_queued_work: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfirmRuntimeHostnameRequest {
    pub hostname: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDrainResponse {
    pub runtime: RuntimeDto,
    pub owned_sessions: Vec<HubSessionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeDeletionImpactDto {
    pub runtime_id: Uuid,
    pub hostname: String,
    pub affected_sessions: Vec<RuntimeDeletionImpactSessionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeDeletionImpactSessionDto {
    pub session_id: Uuid,
    pub agent_name: String,
    pub lifecycle_status: String,
    pub force_delete_disposition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForceDeleteRuntimeResponse {
    pub runtime_id: Uuid,
    pub recoverable_session_ids: Vec<Uuid>,
    pub recovery_failed_session_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendRunEventRequest {
    pub event_type: String,
    pub role: Option<String>,
    pub content: Option<String>,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_tool: Option<WaitingToolRunTransition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingToolRunTransition {
    pub session_id: String,
    pub work_dir_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalizeToolRequestEvent {
    pub role: Option<String>,
    pub content: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FinalizeToolRequestsRequest {
    pub integration_session_id: Uuid,
    pub session_id: String,
    pub work_dir_ref: String,
    pub tool_requests: Vec<FinalizeToolRequestEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRunRequest {
    pub status: String,
    pub session_id: Option<String>,
    pub work_dir_ref: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_contracts_do_not_expose_direct_model_capability() {
        let now = DateTime::parse_from_rfc3339("2026-07-18T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let runtime = RuntimeDto {
            id: Uuid::from_u128(1),
            hostname: "runtime-1".into(),
            labels: vec!["linux".into()],
            codex_version: "0.1.0".into(),
            capabilities: json!({ "model_proxy": true }),
            sandbox_mode: "workspace-write".into(),
            status: "online".into(),
            last_heartbeat_at: now,
            credential_rotation_requested_at: None,
        };
        let runtime_value = serde_json::to_value(runtime).unwrap();
        assert_eq!(runtime_value["capabilities"]["model_proxy"], true);
        assert!(runtime_value.get("direct_model_enabled").is_none());

        let register = RuntimeRegisterRequest {
            hostname: "runtime-1".into(),
            labels: vec!["linux".into()],
            codex_version: "0.1.0".into(),
            capabilities: json!({ "model_proxy": true }),
            sandbox_mode: "workspace-write".into(),
        };
        let register_value = serde_json::to_value(register).unwrap();
        assert_eq!(register_value["capabilities"]["model_proxy"], true);
        assert!(register_value.get("direct_model_enabled").is_none());
    }

    #[test]
    fn runtime_deletion_impact_contract_is_narrow_and_serializes_stably() {
        let impact = RuntimeDeletionImpactDto {
            runtime_id: Uuid::from_u128(1),
            hostname: "runtime-1".into(),
            affected_sessions: vec![RuntimeDeletionImpactSessionDto {
                session_id: Uuid::from_u128(2),
                agent_name: "Agent One".into(),
                lifecycle_status: "online".into(),
                force_delete_disposition: "recoverable".into(),
            }],
        };
        let value = serde_json::to_value(&impact).unwrap();
        assert_eq!(
            value,
            json!({
                "runtime_id": Uuid::from_u128(1),
                "hostname": "runtime-1",
                "affected_sessions": [{
                    "session_id": Uuid::from_u128(2),
                    "agent_name": "Agent One",
                    "lifecycle_status": "online",
                    "force_delete_disposition": "recoverable"
                }]
            })
        );
        assert_eq!(
            serde_json::from_value::<RuntimeDeletionImpactDto>(value).unwrap(),
            impact
        );
    }

    #[test]
    fn model_reasoning_effort_serializes_every_supported_value() {
        let cases = [
            (ReasoningEffort::Default, "default"),
            (ReasoningEffort::None, "none"),
            (ReasoningEffort::Minimal, "minimal"),
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Xhigh, "xhigh"),
            (ReasoningEffort::Max, "max"),
            (ReasoningEffort::Ultra, "ultra"),
        ];

        for (effort, wire_value) in cases {
            assert_eq!(serde_json::to_value(effort).unwrap(), json!(wire_value));
            assert_eq!(
                serde_json::from_value::<ReasoningEffort>(json!(wire_value)).unwrap(),
                effort
            );
        }
        assert_eq!(ReasoningEffort::default(), ReasoningEffort::Default);
    }

    #[test]
    fn model_connection_contracts_keep_api_keys_write_only() {
        let connection_id = Uuid::from_u128(101);
        let owner_id = Uuid::from_u128(102);
        let now = DateTime::parse_from_rfc3339("2026-07-18T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let create: CreateModelConnectionRequest = serde_json::from_value(json!({
            "scope": "personal",
            "name": "Local Responses",
            "base_url": "http://127.0.0.1:8080/provider",
            "model_id": "gpt-test",
            "api_key": "create-secret"
        }))
        .unwrap();
        assert_eq!(create.scope, ModelConnectionScope::Personal);
        assert_eq!(create.api_key, "create-secret");

        let update: UpdateModelConnectionRequest = serde_json::from_value(json!({
            "name": "Updated Responses",
            "base_url": "https://models.example.test",
            "model_id": "gpt-updated"
        }))
        .unwrap();
        assert!(update.api_key.is_none());
        assert!(serde_json::to_value(update)
            .unwrap()
            .get("api_key")
            .is_none());

        let connection = ModelConnectionDto {
            id: connection_id,
            owner_id: Some(owner_id),
            scope: ModelConnectionScope::Personal,
            name: "Local Responses".into(),
            base_url: "http://127.0.0.1:8080/provider".into(),
            model_id: "gpt-test".into(),
            status: ModelConnectionStatus::Enabled,
            is_system_default: false,
            created_at: now,
            updated_at: now,
        };
        let read_value = serde_json::to_value(&connection).unwrap();
        assert_eq!(read_value["scope"], "personal");
        assert_eq!(read_value["status"], "enabled");
        assert!(read_value.get("api_key").is_none());
        assert!(serde_json::from_value::<ModelConnectionDto>(json!({
            "id": connection_id,
            "owner_id": owner_id,
            "scope": "personal",
            "name": "Local Responses",
            "base_url": "http://127.0.0.1:8080/provider",
            "model_id": "gpt-test",
            "status": "enabled",
            "is_system_default": false,
            "created_at": now,
            "updated_at": now,
            "api_key": "leaked-secret"
        }))
        .is_err());

        let public_responses = [
            serde_json::to_value(ModelConnectionOptionsDto {
                items: vec![ModelConnectionOptionDto {
                    id: connection_id,
                    name: "Local Responses".into(),
                    model_id: "gpt-test".into(),
                    scope: ModelConnectionScope::Personal,
                    status: ModelConnectionStatus::Enabled,
                }],
                system_default_model_connection_id: None,
            })
            .unwrap(),
            serde_json::to_value(ModelConnectionTestResultDto {
                success: true,
                status_code: Some(200),
                error_code: None,
                message: None,
            })
            .unwrap(),
            serde_json::to_value(SystemDefaultModelConnectionDto {
                model_connection_id: Some(connection_id),
            })
            .unwrap(),
        ];
        for value in public_responses {
            assert!(!value.to_string().contains("api_key"));
            assert!(!value.to_string().contains("secret"));
        }

        assert_eq!(
            serde_json::to_value(UpdateModelConnectionStatusRequest {
                status: ModelConnectionStatus::Disabled,
            })
            .unwrap(),
            json!({ "status": "disabled" })
        );
        assert_eq!(
            serde_json::to_value(SetSystemDefaultModelConnectionRequest {
                model_connection_id: None,
            })
            .unwrap(),
            json!({ "model_connection_id": null })
        );
    }

    #[test]
    fn model_agent_mutations_use_typed_defaults_and_subagents() {
        let connection_id = Uuid::from_u128(201);
        let create: CreateAgentRequest = serde_json::from_value(json!({
            "name": "Typed Agent",
            "instructions": "Use the configured model",
            "visibility": "private"
        }))
        .unwrap();
        assert_eq!(create.default_model_connection_id, None);
        assert_eq!(create.reasoning_effort, ReasoningEffort::Default);
        assert!(create.codex_subagents.is_empty());

        let update_value = json!({
            "name": "Typed Agent",
            "instructions": "Use the configured model",
            "visibility": "private",
            "public_to": [],
            "runtime_id": null,
            "default_model_connection_id": connection_id,
            "reasoning_effort": "high",
            "codex_subagents": [{
                "name": "reviewer",
                "description": "Reviews implementation changes",
                "developer_instructions": "# Review\nCheck correctness first.",
                "model_connection_id": null,
                "reasoning_effort": "max"
            }],
            "sandbox_policy": {},
            "managed_skill_ids": [],
            "mcp_allowlist": []
        });
        let update: UpdateAgentRequest = serde_json::from_value(update_value.clone()).unwrap();
        assert_eq!(update.default_model_connection_id, Some(connection_id));
        assert_eq!(update.reasoning_effort, ReasoningEffort::High);
        assert_eq!(update.codex_subagents[0].name, "reviewer");
        assert_eq!(
            update.codex_subagents[0].reasoning_effort,
            Some(ReasoningEffort::Max)
        );
        assert!(update.codex_subagents[0].enabled);
        assert_eq!(update.codex_subagents[0].disabled_reason, None);
        assert_eq!(update.model_policy, json!({ "provider": "hub-proxy" }));
        let serialized_update = serde_json::to_value(update).unwrap();
        assert!(serialized_update.get("model_policy").is_none());
        assert!(serialized_update["codex_subagents"][0]
            .get("enabled")
            .is_none());
        assert!(serialized_update["codex_subagents"][0]
            .get("disabled_reason")
            .is_none());
        assert!(serialized_update["codex_subagents"][0].get("id").is_none());

        let disabled: CodexSubagentDefinition = serde_json::from_value(json!({
            "name": "reviewer",
            "description": "Reviews implementation changes",
            "developer_instructions": "# Review\nCheck correctness first.",
            "enabled": false,
            "disabled_reason": "model_connection_deleted"
        }))
        .unwrap();
        let serialized_disabled = serde_json::to_value(disabled).unwrap();
        assert_eq!(serialized_disabled["enabled"], false);
        assert_eq!(
            serialized_disabled["disabled_reason"],
            "model_connection_deleted"
        );
        assert!(serialized_disabled.get("id").is_none());

        let mut legacy = update_value;
        legacy["model_policy"] = json!({ "provider": "hub-proxy" });
        assert!(serde_json::from_value::<UpdateAgentRequest>(legacy).is_err());
    }

    #[test]
    fn model_execution_configuration_defaults_new_fields_for_legacy_json() {
        let configuration: AgentExecutionConfigurationDto = serde_json::from_value(json!({
            "revision": 1,
            "instructions": "Legacy configuration",
            "model_policy": {},
            "sandbox_policy": {},
            "skills": [],
            "mcp_allowlist": []
        }))
        .unwrap();

        assert_eq!(configuration.default_model_connection_id, None);
        assert_eq!(configuration.reasoning_effort, ReasoningEffort::Default);
        assert!(configuration.codex_subagents.is_empty());
        assert!(configuration.model_connections.is_empty());
    }

    #[test]
    fn model_usage_and_error_contracts_use_ranges_summaries_and_keyset_pages() {
        let usage_id = Uuid::from_u128(301);
        let agent_id = Uuid::from_u128(302);
        let connection_id = Uuid::from_u128(303);
        let user_id = Uuid::from_u128(304);
        let occurred_at = DateTime::parse_from_rfc3339("2026-07-18T08:09:10.123Z")
            .unwrap()
            .with_timezone(&Utc);
        let query: ModelTokenUsageQueryDto = serde_json::from_value(json!({
            "from_ms": 1_752_826_800_000_i64,
            "to_ms": 1_752_913_200_000_i64,
            "model_connection_id": connection_id,
            "agent_id": agent_id,
            "user_id": user_id,
            "cursor_occurred_at_ms": 1_752_859_750_123_i64,
            "cursor_id": usage_id,
            "page_size": 50
        }))
        .unwrap();
        assert_eq!(query.page_size, Some(50));
        assert_eq!(query.cursor_id, Some(usage_id));

        let model = ModelConnectionSnapshotDto {
            id: Some(connection_id),
            scope: ModelConnectionScope::Global,
            name: "Global Responses".into(),
            model_id: "gpt-test".into(),
        };
        let agent = ModelAgentSnapshotDto {
            id: Some(agent_id),
            name: "Ledger Agent".into(),
        };
        let subject = ModelUsageSubjectDto {
            kind: ModelUsageSubjectKind::User,
            id: Some(user_id),
            display_name: Some("Member".into()),
        };
        let totals = ModelTokenUsageTotalsDto {
            input_tokens: 10,
            output_tokens: 4,
            total_tokens: 14,
            cached_tokens: 3,
            reasoning_tokens: 2,
        };
        let usage = ModelTokenUsageDto {
            id: usage_id,
            occurred_at,
            response_status: "completed".into(),
            model: model.clone(),
            agent: agent.clone(),
            subject: subject.clone(),
            input_tokens: 10,
            output_tokens: 4,
            total_tokens: 14,
            cached_tokens: 3,
            reasoning_tokens: 2,
        };
        let summary = ModelUsageSummaryDto {
            overall: totals.clone(),
            by_model: vec![ModelUsageModelSummaryDto {
                model: model.clone(),
                totals: totals.clone(),
            }],
            by_agent: vec![ModelUsageAgentSummaryDto {
                agent: agent.clone(),
                totals: totals.clone(),
            }],
            by_user: vec![ModelUsageUserSummaryDto {
                user_id: Some(user_id),
                display_name: Some("Member".into()),
                totals: totals.clone(),
            }],
        };
        assert_eq!(
            serde_json::to_value(&summary).unwrap()["overall"]["total_tokens"],
            14
        );

        let next_cursor = ModelLedgerCursorDto {
            occurred_at_ms: 1_752_859_750_123,
            id: usage_id,
        };
        let usage_page = ModelTokenUsagePageDto {
            items: vec![usage],
            next_cursor: Some(next_cursor.clone()),
        };
        let usage_value = serde_json::to_value(usage_page).unwrap();
        assert_eq!(usage_value["next_cursor"]["id"], usage_id.to_string());
        assert!(usage_value.get("total").is_none());

        let error_page = ModelCallErrorPageDto {
            items: vec![ModelCallErrorDto {
                id: Uuid::from_u128(305),
                occurred_at,
                response_status: "failed".into(),
                model,
                agent,
                subject,
                upstream_status: Some(429),
                error_code: Some("rate_limit".into()),
                message: Some("Provider rejected the request".into()),
            }],
            next_cursor: Some(next_cursor),
        };
        let error_value = serde_json::to_value(error_page).unwrap();
        assert_eq!(error_value["items"][0]["upstream_status"], 429);
        for forbidden in ["api_key", "prompt", "raw_body", "headers"] {
            assert!(!error_value.to_string().contains(forbidden));
        }

        let error_query: ModelCallErrorQueryDto = serde_json::from_value(json!({
            "from_ms": 1_752_826_800_000_i64,
            "to_ms": 1_752_913_200_000_i64,
            "page_size": 20
        }))
        .unwrap();
        assert_eq!(error_query.from_ms, Some(1_752_826_800_000));
        assert_eq!(error_query.to_ms, Some(1_752_913_200_000));
    }

    #[test]
    fn identity_and_auth_policy_contracts_serialize_stably() {
        let user: UserDto = serde_json::from_value(json!({
            "id": Uuid::nil(),
            "username": "member",
            "email": null,
            "display_name": "Member",
            "role": "member"
        }))
        .unwrap();
        assert_eq!(user.username, "member");
        assert_eq!(user.email, None);

        let request: EraseUserRequest = serde_json::from_value(json!({
            "username": "member"
        }))
        .unwrap();
        assert_eq!(request.username, "member");
        let erasure = UserErasureDto {
            user_id: Uuid::nil(),
            username: Some("member".into()),
            status: "pending".into(),
            requested_at: Utc::now(),
            completed_at: None,
        };
        assert_eq!(serde_json::to_value(&erasure).unwrap()["status"], "pending");

        let policy = AuthPolicyDto {
            password_registration_enabled: true,
            password_login_enabled: false,
            email_verification_required: true,
        };
        assert_eq!(
            serde_json::to_value(policy).unwrap(),
            json!({
                "password_registration_enabled": true,
                "password_login_enabled": false,
                "email_verification_required": true
            })
        );

        let channel: AuthenticationChannelDto = serde_json::from_value(json!({
            "id": Uuid::nil(),
            "platform_id": Uuid::from_u128(1),
            "key": "oidc",
            "name": "OIDC",
            "enabled": true,
            "trusted_email": true
        }))
        .unwrap();
        assert!(channel.enabled);
        assert!(channel.trusted_email);

        let platform = ExternalPlatformDto {
            id: Uuid::nil(),
            key: "github".into(),
            name: "GitHub".into(),
        };
        assert_eq!(
            serde_json::to_value(&platform).unwrap(),
            json!({ "id": Uuid::nil(), "key": "github", "name": "GitHub" })
        );
        let options = IntegrationAppOptionsDto {
            external_platforms: vec![platform],
            authentication_channels: vec![channel],
        };
        assert_eq!(
            serde_json::to_value(options).unwrap()["authentication_channels"][0]["id"],
            Uuid::nil().to_string()
        );
    }

    #[test]
    fn management_contracts_serialize_new_public_shapes() {
        let now = DateTime::parse_from_rfc3339("2026-07-17T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let create_key: CreateApiKeyRequest = serde_json::from_value(json!({
            "name": "automation"
        }))
        .unwrap();
        assert!(create_key.validity.is_none());
        assert_eq!(
            serde_json::to_value(create_key).unwrap(),
            json!({ "name": "automation" })
        );
        assert_eq!(
            serde_json::to_value(RenewApiKeyRequest {
                validity: ApiKeyValidity::Days { days: 180 },
            })
            .unwrap(),
            json!({ "validity": { "kind": "days", "days": 180 } })
        );
        assert_eq!(
            serde_json::to_value(RenewApiKeyRequest {
                validity: ApiKeyValidity::Date { expires_at: now },
            })
            .unwrap()["validity"]["kind"],
            "date"
        );
        assert_eq!(
            serde_json::to_value(RenewApiKeyRequest {
                validity: ApiKeyValidity::Never,
            })
            .unwrap(),
            json!({ "validity": { "kind": "never" } })
        );

        let api_key = ApiKeyDto {
            id: Uuid::from_u128(1),
            name: "automation".into(),
            prefix: "ahk_test".into(),
            last_used_at: None,
            expires_at: Some(now),
            created_at: now,
        };
        let api_key_value = serde_json::to_value(api_key).unwrap();
        assert!(api_key_value.get("revoked_at").is_none());
        assert_eq!(
            api_key_value["expires_at"],
            serde_json::to_value(now).unwrap()
        );

        let app = IntegrationAppDto {
            id: Uuid::from_u128(2),
            owner_id: Uuid::from_u128(3),
            name: "Support Console".into(),
            client_id: "client-1".into(),
            external_platform_id: Uuid::from_u128(4),
            authentication_channel_id: Uuid::from_u128(5),
            redirect_uris: json!(["https://example.test/callback"]),
            agent_ids: vec![Uuid::from_u128(6), Uuid::from_u128(7)],
            created_at: now,
            updated_at: now,
        };
        let listed = serde_json::to_value(&app).unwrap();
        assert!(listed.get("agent_id").is_none());
        assert!(listed.get("client_secret").is_none());
        assert_eq!(listed["agent_ids"].as_array().unwrap().len(), 2);
        let secret = serde_json::to_value(IntegrationAppSecretResponse {
            integration_app: app,
            client_secret: "one-time-secret".into(),
        })
        .unwrap();
        assert_eq!(secret["client_secret"], "one-time-secret");
        assert!(secret["integration_app"].get("client_secret").is_none());

        let skill_ids = vec![Uuid::from_u128(8), Uuid::from_u128(9)];
        let delete_request = BulkDeleteSkillsRequest {
            skill_ids: skill_ids.clone(),
        };
        assert_eq!(
            serde_json::from_value::<BulkDeleteSkillsRequest>(
                serde_json::to_value(delete_request).unwrap()
            )
            .unwrap()
            .skill_ids,
            skill_ids
        );
        assert_eq!(
            serde_json::to_value(BulkDeleteSkillsResponse {
                deleted_skill_ids: skill_ids.clone(),
            })
            .unwrap()["deleted_skill_ids"],
            json!(skill_ids)
        );
        assert!(
            serde_json::from_value::<UpdateExternalPlatformRequest>(json!({
                "name": "Updated",
                "key": "immutable"
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<AdminSetUserPasswordRequest>(json!({
                "password": "new-password",
                "unexpected": true
            }))
            .is_err()
        );
    }

    #[test]
    fn oauth_userinfo_omits_fields_without_their_scopes() {
        let subject = Uuid::from_u128(20);
        let minimal = OAuthUserInfoDto {
            sub: subject,
            username: None,
            name: None,
            email: None,
            external_profile: None,
        };
        assert_eq!(
            serde_json::to_value(minimal).unwrap(),
            json!({ "sub": subject })
        );

        let full = OAuthUserInfoDto {
            sub: subject,
            username: Some("member".into()),
            name: Some("Member".into()),
            email: Some("member@example.com".into()),
            external_profile: Some(OAuthExternalProfileDto {
                platform_id: Uuid::from_u128(21),
                tenant_id: "tenant-1".into(),
                external_identity_id: Uuid::from_u128(22),
                external_user_id: "external-1".into(),
                username: Some("external-member".into()),
                email: Some("member@example.com".into()),
            }),
        };
        let value = serde_json::to_value(full).unwrap();
        assert_eq!(value["username"], "member");
        assert_eq!(value["external_profile"]["tenant_id"], "tenant-1");
    }

    #[test]
    fn refresh_configuration_command_carries_verifiable_configuration() {
        let content = "managed content";
        let configuration = AgentExecutionConfigurationDto {
            revision: 8,
            instructions: "Use the current managed Skills".into(),
            default_model_connection_id: None,
            reasoning_effort: ReasoningEffort::Default,
            codex_subagents: Vec::new(),
            model_connections: Vec::new(),
            model_policy: json!({ "model": "gpt-test" }),
            sandbox_policy: json!({ "mode": "workspace-write" }),
            skills: vec![AgentExecutionSkillDto {
                source: "managed".into(),
                source_id: Some(Uuid::from_u128(10)),
                name: "review".into(),
                description: "Review changes".into(),
                content: content.into(),
                revision: 3,
                content_checksum_sha256: sha256_hex(content.as_bytes()),
            }],
            mcp_allowlist: json!([]),
        };
        let fingerprint = execution_configuration_fingerprint(&configuration).unwrap();
        let command = RuntimeSessionCommandDto {
            command_id: Uuid::from_u128(11),
            session_id: Uuid::from_u128(12),
            ownership_generation: 4,
            command: "refresh_configuration".into(),
            run_id: None,
            turn_id: None,
            native_thread_id: Some("thread-12".into()),
            native_turn_id: None,
            message: None,
            configuration_revision: Some(configuration.revision),
            fingerprint: Some(fingerprint.clone()),
            execution_configuration: Some(configuration),
        };
        let value = serde_json::to_value(&command).unwrap();
        assert_eq!(value["command"], "refresh_configuration");
        assert_eq!(value["configuration_revision"], 8);
        assert_eq!(value["fingerprint"], fingerprint);
        let round_trip: RuntimeSessionCommandDto = serde_json::from_value(value).unwrap();
        let carried_configuration = round_trip.execution_configuration.unwrap();
        assert_eq!(
            execution_configuration_fingerprint(&carried_configuration).unwrap(),
            round_trip.fingerprint.unwrap()
        );

        let completion = CompleteRuntimeSessionCommandRequest {
            command: "refresh_configuration".into(),
            outcome: "applied".into(),
            revision: Some(carried_configuration.revision),
            fingerprint: Some(execution_configuration_fingerprint(&carried_configuration).unwrap()),
        };
        let value = serde_json::to_value(completion).unwrap();
        assert_eq!(value["revision"], 8);
        assert!(value["fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("sha256:"));
    }

    #[test]
    fn session_model_contracts_serialize_origin_turn_message_and_bundle() {
        let now = DateTime::parse_from_rfc3339("2026-07-15T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let session_id = Uuid::from_u128(1);
        let turn_id = Uuid::from_u128(2);
        let run_id = Uuid::from_u128(3);
        let session = HubSessionDto {
            id: session_id,
            owner_id: Uuid::from_u128(4),
            agent_id: Uuid::from_u128(5),
            agent_name: "Test Agent".into(),
            agent_deleted_at: None,
            origin: HubSessionOriginDto::External {
                platform_id: Uuid::from_u128(6),
                tenant_id: "workspace-1".into(),
                external_identity_id: Uuid::from_u128(7),
            },
            lifecycle_status: "offline".into(),
            native_thread_id: Some("thread-1".into()),
            active_turn_id: None,
            history_checkpoint: 12,
            configuration_fingerprint: Some("sha256:config".into()),
            runtime_owner_id: None,
            ownership_generation: 3,
            recovery_error: None,
            current_bundle: Some(CurrentSessionBundleDto {
                generation: 2,
                object_key: "sessions/1/bundle-2.tar.zst".into(),
                checksum_sha256: "abc123".into(),
                size_bytes: 4096,
                history_checkpoint: 12,
                ownership_generation: 3,
                producing_codex_version: "0.42.0".into(),
                created_at: now,
            }),
            created_at: now,
            updated_at: now,
        };
        let value = serde_json::to_value(session).unwrap();
        assert_eq!(value["origin"]["kind"], "external");
        assert_eq!(value["origin"]["tenant_id"], "workspace-1");
        assert_eq!(value["current_bundle"]["generation"], 2);
        assert_eq!(value["history_checkpoint"], 12);

        let native: HubSessionDto = serde_json::from_value(json!({
            "id": session_id,
            "owner_id": Uuid::from_u128(4),
            "agent_id": Uuid::from_u128(5),
            "agent_name": "Test Agent",
            "agent_deleted_at": null,
            "origin": { "kind": "hub_native" },
            "lifecycle_status": "waiting_for_runtime",
            "native_thread_id": null,
            "active_turn_id": null,
            "history_checkpoint": 0,
            "configuration_fingerprint": null,
            "runtime_owner_id": null,
            "ownership_generation": 0,
            "recovery_error": null,
            "current_bundle": null,
            "created_at": now,
            "updated_at": now
        }))
        .unwrap();
        assert!(matches!(native.origin, HubSessionOriginDto::HubNative));

        let turn = HubSessionTurnDto {
            id: turn_id,
            session_id,
            native_turn_id: Some("turn-native-1".into()),
            status: "completed".into(),
            configuration_fingerprint: None,
            ownership_generation: 3,
            started_at: Some(now),
            ended_at: Some(now),
            created_at: now,
            updated_at: now,
        };
        let value = serde_json::to_value(turn).unwrap();
        assert_eq!(value["native_turn_id"], "turn-native-1");
        assert!(value["configuration_fingerprint"].is_null());

        let message = HubSessionMessageDto {
            id: Uuid::from_u128(8),
            session_id,
            sequence: 13,
            role: "user".into(),
            message_kind: "message".into(),
            content: Some("Please adjust the current turn".into()),
            payload: json!({}),
            delivery_mode: "steer".into(),
            delivery_state: "delivered".into(),
            client_message_key: Some("request-1".into()),
            expected_native_turn_id: Some("turn-native-1".into()),
            turn_id: Some(turn_id),
            run_id: Some(run_id),
            accepted_at: now,
        };
        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["sequence"], 13);
        assert_eq!(value["expected_native_turn_id"], "turn-native-1");
        assert_eq!(value["client_message_key"], "request-1");
        assert_eq!(value["turn_id"], turn_id.to_string());
        assert_eq!(value["run_id"], run_id.to_string());
    }

    #[test]
    fn runtime_enrollment_and_rotation_contracts_expose_secrets_only_once() {
        let now = DateTime::parse_from_rfc3339("2026-07-15T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let enrollment = RuntimeEnrollmentTokenDto {
            id: Uuid::from_u128(1),
            created_by: Some(Uuid::from_u128(2)),
            expires_at: now,
            consumed_at: None,
            consumed_by_runtime_id: None,
            revoked_at: None,
            created_at: now,
        };
        let listed = serde_json::to_value(&enrollment).unwrap();
        assert!(listed.get("token").is_none());
        assert!(listed.get("token_hash").is_none());

        let created = CreateRuntimeEnrollmentTokenResponse {
            enrollment,
            token: "ahre_one-time".into(),
        };
        assert_eq!(
            serde_json::to_value(created).unwrap()["token"],
            "ahre_one-time"
        );

        let enrolled = RuntimeRegisterResponse {
            runtime_id: Uuid::from_u128(3),
            runtime_credential: "ahrc_one-time".into(),
            protocol_capabilities: vec![ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()],
        };
        assert_eq!(
            serde_json::to_value(enrolled).unwrap()["runtime_credential"],
            "ahrc_one-time"
        );
        assert!(serde_json::from_value::<RuntimeHeartbeatRequest>(json!({
            "pending_credential_hash": null,
            "unexpected": true
        }))
        .is_err());
    }

    #[test]
    fn runtime_session_fencing_contracts_carry_generation_both_directions() {
        let session_id = Uuid::from_u128(11);
        let write = RuntimeSessionWriteRequest {
            ownership_generation: 7,
            payload: CompleteRunRequest {
                status: "completed".into(),
                session_id: Some("thread-1".into()),
                work_dir_ref: Some("session-11".into()),
            },
        };
        let value = serde_json::to_value(write).unwrap();
        assert_eq!(value["ownership_generation"], 7);
        assert_eq!(value["payload"]["status"], "completed");

        let heartbeat = RuntimeHeartbeatResponse {
            rotation_requested: false,
            pending_credential_accepted: false,
            credential_activated: false,
            runtime_status: "draining".into(),
            owned_sessions: vec![RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 7,
                lifecycle_status: "online".into(),
                native_thread_id: Some("thread-11".into()),
                active_run_id: Some(Uuid::from_u128(13)),
            }],
            cleanup_sessions: Vec::new(),
            session_commands: vec![RuntimeSessionCommandDto {
                command_id: Uuid::from_u128(12),
                session_id,
                ownership_generation: 7,
                command: "checkpoint".into(),
                run_id: None,
                turn_id: None,
                native_thread_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }],
            codex_rollout: RuntimeCodexRolloutCommandDto::default(),
        };
        let value = serde_json::to_value(heartbeat).unwrap();
        assert_eq!(value["runtime_status"], "draining");
        assert_eq!(value["owned_sessions"][0]["ownership_generation"], 7);
        assert_eq!(value["owned_sessions"][0]["native_thread_id"], "thread-11");
        assert_eq!(
            value["owned_sessions"][0]["active_run_id"],
            Uuid::from_u128(13).to_string()
        );
        assert_eq!(
            value["session_commands"][0]["session_id"],
            session_id.to_string()
        );
        assert_eq!(value["session_commands"][0]["ownership_generation"], 7);
        assert_eq!(value["session_commands"][0]["command"], "checkpoint");
        assert_eq!(
            value["session_commands"][0]["command_id"],
            Uuid::from_u128(12).to_string()
        );
        assert!(value["session_commands"][0]["message"].is_null());

        let checkpoint_attempt_id = Uuid::from_u128(13);
        let saving = RuntimeHeartbeatRequest {
            pending_credential_hash: None,
            accepts_session_commands: true,
            owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                session_id,
                ownership_generation: 7,
                lifecycle_status: "saving".into(),
                checkpoint_reason: Some("version_switch".into()),
            }],
            cleaned_sessions: Vec::new(),
            codex_status: None,
        };
        let value = serde_json::to_value(saving).unwrap();
        assert_eq!(
            value["owned_sessions"][0]["checkpoint_reason"],
            "version_switch"
        );
        assert_eq!(
            serde_json::to_value(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 7,
                reason: "idle".into(),
            })
            .unwrap(),
            json!({ "ownership_generation": 7, "reason": "idle" })
        );
        assert_eq!(
            serde_json::to_value(RuntimeSessionCheckpointAttemptDto {
                checkpoint_attempt_id,
                history_checkpoint: 21,
                bundle_generation: 4,
                reason: "idle".into(),
            })
            .unwrap(),
            json!({
                "checkpoint_attempt_id": checkpoint_attempt_id,
                "history_checkpoint": 21,
                "bundle_generation": 4,
                "reason": "idle"
            })
        );
        assert_eq!(
            serde_json::to_value(RuntimeSessionBundleCommitResponseDto {
                checkpoint_attempt_id,
                bundle_generation: 4,
                has_queued_work: true,
                ownership_released: false,
            })
            .unwrap(),
            json!({
                "checkpoint_attempt_id": checkpoint_attempt_id,
                "bundle_generation": 4,
                "has_queued_work": true,
                "ownership_released": false
            })
        );
        assert_eq!(
            serde_json::to_value(FailRuntimeSessionCheckpointRequest {
                ownership_generation: 7,
                checkpoint_attempt_id,
                error: "bundle_transport_unavailable".into(),
            })
            .unwrap(),
            json!({
                "ownership_generation": 7,
                "checkpoint_attempt_id": checkpoint_attempt_id,
                "error": "bundle_transport_unavailable"
            })
        );
        assert_eq!(
            serde_json::to_value(RuntimeSessionCheckpointDispositionDto {
                checkpoint_attempt_id,
                disposition: "retry".into(),
                has_queued_work: true,
            })
            .unwrap(),
            json!({
                "checkpoint_attempt_id": checkpoint_attempt_id,
                "disposition": "retry",
                "has_queued_work": true
            })
        );

        let command_completion = RuntimeSessionWriteRequest {
            ownership_generation: 7,
            payload: CompleteRuntimeSessionCommandRequest {
                command: "interrupt".into(),
                outcome: "interrupted".into(),
                revision: None,
                fingerprint: None,
            },
        };
        let value = serde_json::to_value(command_completion).unwrap();
        assert_eq!(value["ownership_generation"], 7);
        assert_eq!(value["payload"]["command"], "interrupt");
        assert_eq!(value["payload"]["outcome"], "interrupted");

        let claim = RuntimeClaimRunRequest {
            available_new_session_slots: 2,
            ready_owned_sessions: vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 7,
            }],
        };
        let value = serde_json::to_value(claim).unwrap();
        assert_eq!(value["available_new_session_slots"], 2);
        assert_eq!(
            value["ready_owned_sessions"][0]["session_id"],
            session_id.to_string()
        );
        assert_eq!(value["ready_owned_sessions"][0]["ownership_generation"], 7);
        assert!(serde_json::from_value::<RuntimeClaimRunRequest>(json!({
            "available_new_session_slots": 1
        }))
        .is_err());
        assert!(serde_json::from_value::<RuntimeClaimRunRequest>(json!({
            "ready_owned_sessions": []
        }))
        .is_err());
    }

    #[test]
    fn runtime_cleanup_acknowledgement_contract_carries_the_cleaned_generation() {
        let session_id = Uuid::from_u128(14);
        let request: RuntimeHeartbeatRequest = serde_json::from_value(json!({
            "accepts_session_commands": true,
            "cleaned_sessions": [{
                "session_id": session_id,
                "ownership_generation": 9
            }]
        }))
        .unwrap();

        assert_eq!(
            request.cleaned_sessions,
            vec![RuntimeOwnedSessionGenerationDto {
                session_id,
                ownership_generation: 9,
            }]
        );
        assert_eq!(
            serde_json::to_value(&request).unwrap()["cleaned_sessions"],
            json!([{
                "session_id": session_id,
                "ownership_generation": 9
            }])
        );
        assert!(serde_json::from_value::<RuntimeHeartbeatRequest>(json!({}))
            .unwrap()
            .cleaned_sessions
            .is_empty());

        let response: RuntimeHeartbeatResponse = serde_json::from_value(json!({
            "rotation_requested": false,
            "pending_credential_accepted": false,
            "credential_activated": false,
            "runtime_status": "online",
            "owned_sessions": [],
            "session_commands": [],
            "cleanup_sessions": [{
                "session_id": session_id,
                "ownership_generation": 9
            }]
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_value(response).unwrap()["cleanup_sessions"],
            json!([{
                "session_id": session_id,
                "ownership_generation": 9
            }])
        );
    }

    #[test]
    fn codex_version_rollout_contract_carries_exact_artifact_and_runtime_status() {
        let artifact = CodexVersionArtifactDto {
            version: "0.144.5".into(),
            os: "linux".into(),
            architecture: "x86_64".into(),
            artifact_name: "codex-x86_64-unknown-linux-musl.zst".into(),
            sha256: "a".repeat(64),
            size_bytes: 42,
        };
        let request: RuntimeHeartbeatRequest = serde_json::from_value(json!({
            "codex_status": {
                "current_version": "0.143.0",
                "candidate_version": "0.144.5",
                "candidate_status": "ready",
                "candidate_error": null
            }
        }))
        .unwrap();
        assert_eq!(
            request.codex_status.unwrap().candidate_status.as_deref(),
            Some("ready")
        );

        let command = RuntimeCodexRolloutCommandDto {
            active_version: Some("0.143.0".into()),
            target_artifact: Some(artifact.clone()),
        };
        assert_eq!(
            serde_json::to_value(&command).unwrap()["target_artifact"],
            serde_json::to_value(artifact).unwrap()
        );
        let heartbeat: RuntimeHeartbeatResponse = serde_json::from_value(json!({
            "rotation_requested": false,
            "pending_credential_accepted": false,
            "credential_activated": false,
            "runtime_status": "online",
            "owned_sessions": [],
            "session_commands": [],
            "codex_rollout": command
        }))
        .unwrap();
        assert_eq!(
            heartbeat
                .codex_rollout
                .target_artifact
                .unwrap()
                .architecture,
            "x86_64"
        );

        let target: SetCodexTargetVersionRequest = serde_json::from_value(json!({
            "version": "0.144.5"
        }))
        .unwrap();
        assert_eq!(target.version, "0.144.5");

        let now = Utc::now();
        let rollout = CodexVersionRolloutDto {
            active_version: Some("0.143.0".into()),
            target_version: Some("0.144.5".into()),
            status: "ready".into(),
            error: None,
            artifacts: vec![CodexVersionArtifactDto {
                version: "0.144.5".into(),
                os: "linux".into(),
                architecture: "x86_64".into(),
                artifact_name: "codex-x86_64-unknown-linux-musl.zst".into(),
                sha256: "a".repeat(64),
                size_bytes: 42,
            }],
            runtimes: vec![CodexRuntimeReadinessDto {
                runtime_id: Uuid::nil(),
                hostname: "runtime-1".into(),
                os: "linux".into(),
                architecture: "x86_64".into(),
                current_version: "0.143.0".into(),
                target_version: Some("0.144.5".into()),
                status: "ready".into(),
                error: None,
                checked_at: Some(now),
            }],
            updated_at: now,
        };
        let serialized = serde_json::to_value(rollout).unwrap();
        assert_eq!(serialized["status"], "ready");
        assert_eq!(serialized["runtimes"][0]["current_version"], "0.143.0");
    }

    #[test]
    fn execution_configuration_fingerprint_is_deterministic_complete_and_secret_safe() {
        let managed = AgentExecutionSkillDto {
            source: "managed".into(),
            source_id: Some(Uuid::from_u128(11)),
            name: "managed-review".into(),
            description: "Review managed changes".into(),
            content: "managed content".into(),
            revision: 4,
            content_checksum_sha256:
                "0a10dad816a1435e3bd058fc127f7ad79cf8f73a589e9e9f534e7d17c2df4018".into(),
        };
        let managed_docs = AgentExecutionSkillDto {
            source: "managed".into(),
            source_id: Some(Uuid::from_u128(12)),
            name: "managed-docs".into(),
            description: "Review documentation".into(),
            content: "managed docs content".into(),
            revision: 7,
            content_checksum_sha256:
                "22c40e1525f701e38d3cf62ae490a603c1af79dab5ebffa4d73a2d220e25cbdc".into(),
        };
        let configuration = AgentExecutionConfigurationDto {
            revision: 7,
            instructions: "Follow repository guidance".into(),
            default_model_connection_id: Some(Uuid::from_u128(21)),
            reasoning_effort: ReasoningEffort::High,
            codex_subagents: vec![
                CodexSubagentDefinition {
                    name: "reviewer".into(),
                    description: "Reviews implementation changes".into(),
                    developer_instructions: "# Review\n\nCheck correctness first.".into(),
                    model_connection_id: Some(Uuid::from_u128(22)),
                    reasoning_effort: Some(ReasoningEffort::Max),
                    enabled: true,
                    disabled_reason: None,
                },
                CodexSubagentDefinition {
                    name: "researcher".into(),
                    description: "Researches primary sources".into(),
                    developer_instructions: "# Research\n\nCite sources.".into(),
                    model_connection_id: None,
                    reasoning_effort: None,
                    enabled: false,
                    disabled_reason: Some("model_connection_deleted".into()),
                },
            ],
            model_connections: vec![
                ModelConnectionOptionDto {
                    id: Uuid::from_u128(21),
                    name: "Agent Default".into(),
                    model_id: "gpt-test".into(),
                    scope: ModelConnectionScope::Personal,
                    status: ModelConnectionStatus::Enabled,
                },
                ModelConnectionOptionDto {
                    id: Uuid::from_u128(22),
                    name: "Reviewer Override".into(),
                    model_id: "gpt-review".into(),
                    scope: ModelConnectionScope::Global,
                    status: ModelConnectionStatus::Enabled,
                },
            ],
            model_policy: json!({ "provider": "hub-proxy", "model": "gpt-test" }),
            sandbox_policy: json!({ "network_access": false, "mode": "workspace-write" }),
            skills: vec![managed.clone(), managed_docs.clone()],
            mcp_allowlist: json!([
                { "name": "github", "command": "gh-mcp", "secrets": { "TOKEN": "secret-a" } },
                { "name": "files", "command": "fs-mcp", "args": ["--root", "."] }
            ]),
        };
        let fingerprint = execution_configuration_fingerprint(&configuration).unwrap();

        let mut reordered = configuration.clone();
        reordered.skills = vec![managed_docs, managed];
        reordered.mcp_allowlist.as_array_mut().unwrap().reverse();
        reordered.codex_subagents.reverse();
        reordered.model_connections.reverse();
        assert_eq!(
            execution_configuration_fingerprint(&reordered).unwrap(),
            fingerprint
        );

        let mut shared_skill_edit = configuration.clone();
        shared_skill_edit.skills[0].revision += 1;
        assert_ne!(
            execution_configuration_fingerprint(&shared_skill_edit).unwrap(),
            fingerprint
        );

        let mut model_id_edit = configuration.clone();
        model_id_edit.model_connections[0].model_id = "gpt-test-v2".into();
        assert_ne!(
            execution_configuration_fingerprint(&model_id_edit).unwrap(),
            fingerprint
        );

        let mut reasoning_edit = configuration.clone();
        reasoning_edit.reasoning_effort = ReasoningEffort::Ultra;
        assert_ne!(
            execution_configuration_fingerprint(&reasoning_edit).unwrap(),
            fingerprint
        );

        let mut subagent_edit = configuration.clone();
        subagent_edit.codex_subagents[0].developer_instructions =
            "# Review\n\nCheck correctness and security.".into();
        assert_ne!(
            execution_configuration_fingerprint(&subagent_edit).unwrap(),
            fingerprint
        );

        let mut secret_only_without_revision = configuration.clone();
        secret_only_without_revision.mcp_allowlist[0]["secrets"]["TOKEN"] = json!("guessable");
        assert_eq!(
            execution_configuration_fingerprint(&secret_only_without_revision).unwrap(),
            fingerprint
        );
        secret_only_without_revision.revision += 1;
        assert_ne!(
            execution_configuration_fingerprint(&secret_only_without_revision).unwrap(),
            fingerprint
        );
    }

    #[test]
    fn execution_configuration_rejects_invalid_model_references_and_subagents() {
        let connection_id = Uuid::from_u128(31);
        let configuration = AgentExecutionConfigurationDto {
            revision: 1,
            instructions: "Use typed model configuration".into(),
            default_model_connection_id: Some(connection_id),
            reasoning_effort: ReasoningEffort::Medium,
            codex_subagents: vec![CodexSubagentDefinition {
                name: "reviewer".into(),
                description: "Reviews implementation changes".into(),
                developer_instructions: "# Review\n\nCheck correctness first.".into(),
                model_connection_id: Some(connection_id),
                reasoning_effort: None,
                enabled: true,
                disabled_reason: None,
            }],
            model_connections: vec![ModelConnectionOptionDto {
                id: connection_id,
                name: "Agent Default".into(),
                model_id: "gpt-test".into(),
                scope: ModelConnectionScope::Personal,
                status: ModelConnectionStatus::Enabled,
            }],
            model_policy: json!({ "provider": "hub-proxy" }),
            sandbox_policy: json!({}),
            skills: Vec::new(),
            mcp_allowlist: json!([]),
        };
        assert!(execution_configuration_fingerprint(&configuration).is_ok());

        let mut duplicate_connection = configuration.clone();
        duplicate_connection
            .model_connections
            .push(duplicate_connection.model_connections[0].clone());
        assert!(execution_configuration_fingerprint(&duplicate_connection).is_err());

        let mut empty_connection_name = configuration.clone();
        empty_connection_name.model_connections[0].name = "  ".into();
        assert!(execution_configuration_fingerprint(&empty_connection_name).is_err());

        let mut empty_model_id = configuration.clone();
        empty_model_id.model_connections[0].model_id = String::new();
        assert!(execution_configuration_fingerprint(&empty_model_id).is_err());

        let mut missing_default = configuration.clone();
        missing_default.default_model_connection_id = Some(Uuid::from_u128(32));
        assert!(execution_configuration_fingerprint(&missing_default).is_err());

        let mut missing_override = configuration.clone();
        missing_override.codex_subagents[0].model_connection_id = Some(Uuid::from_u128(32));
        assert!(execution_configuration_fingerprint(&missing_override).is_err());

        let mut duplicate_subagent = configuration.clone();
        let mut duplicate = duplicate_subagent.codex_subagents[0].clone();
        duplicate.name = " REVIEWER ".into();
        duplicate_subagent.codex_subagents.push(duplicate);
        assert!(execution_configuration_fingerprint(&duplicate_subagent).is_err());

        let mut empty_subagent_name = configuration.clone();
        empty_subagent_name.codex_subagents[0].name = " ".into();
        assert!(execution_configuration_fingerprint(&empty_subagent_name).is_err());

        let mut empty_description = configuration.clone();
        empty_description.codex_subagents[0].description = " ".into();
        assert!(execution_configuration_fingerprint(&empty_description).is_err());

        let mut empty_instructions = configuration.clone();
        empty_instructions.codex_subagents[0].developer_instructions = String::new();
        assert!(execution_configuration_fingerprint(&empty_instructions).is_err());

        let mut enabled_with_reason = configuration.clone();
        enabled_with_reason.codex_subagents[0].disabled_reason = Some("unexpected".into());
        assert!(execution_configuration_fingerprint(&enabled_with_reason).is_err());

        let mut disabled_without_reason = configuration.clone();
        disabled_without_reason.codex_subagents[0].enabled = false;
        assert!(execution_configuration_fingerprint(&disabled_without_reason).is_err());

        let mut disabled_with_empty_reason = configuration.clone();
        disabled_with_empty_reason.codex_subagents[0].enabled = false;
        disabled_with_empty_reason.codex_subagents[0].disabled_reason = Some(" ".into());
        assert!(execution_configuration_fingerprint(&disabled_with_empty_reason).is_err());

        let mut disabled_without_override = configuration;
        disabled_without_override.codex_subagents[0].enabled = false;
        disabled_without_override.codex_subagents[0].disabled_reason =
            Some("model_connection_deleted".into());
        disabled_without_override.codex_subagents[0].model_connection_id = None;
        assert!(execution_configuration_fingerprint(&disabled_without_override).is_ok());
    }

    #[test]
    fn execution_configuration_rejects_inline_skills() {
        let content = "inline content";
        let configuration = AgentExecutionConfigurationDto {
            revision: 1,
            instructions: String::new(),
            default_model_connection_id: None,
            reasoning_effort: ReasoningEffort::Default,
            codex_subagents: Vec::new(),
            model_connections: Vec::new(),
            model_policy: json!({}),
            sandbox_policy: json!({}),
            skills: vec![AgentExecutionSkillDto {
                source: "inline".into(),
                source_id: None,
                name: "inline-review".into(),
                description: String::new(),
                content: content.into(),
                revision: 1,
                content_checksum_sha256: sha256_hex(content.as_bytes()),
            }],
            mcp_allowlist: json!([]),
        };

        assert!(execution_configuration_fingerprint(&configuration).is_err());
    }
}
