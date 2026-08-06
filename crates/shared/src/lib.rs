use chrono::{DateTime, Utc};
use serde::{de::Deserializer, Deserialize, Serialize, Serializer};
use serde_json::{json, Number, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use uuid::Uuid;

pub const ATOMIC_WAITING_TOOL_BATCH_CAPABILITY: &str = "atomic_waiting_tool_batch";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDto {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminUserDetailDto {
    pub user: UserDto,
    pub has_password: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdminCreateUserRequest {
    pub email: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdminUpdateUserRequest {
    pub email: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateCurrentUserRequest {
    pub display_name: String,
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
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserErasureDto {
    pub user_id: Uuid,
    pub email: Option<String>,
    pub status: String,
    pub requested_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthPolicyDto {
    pub password_registration_enabled: bool,
    pub password_login_enabled: bool,
    pub ldap_login_enabled: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LdapSecurityMode {
    Ldaps,
    Starttls,
    Plain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LdapConfigurationDto {
    pub url: String,
    pub security: LdapSecurityMode,
    pub base_dn: String,
    pub bind_identity_template: String,
    pub user_filter: String,
    pub email_attribute: String,
    pub display_name_attribute: String,
    pub allow_insecure: bool,
    pub skip_tls_verify: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TestLdapConfigurationRequest {
    pub configuration: LdapConfigurationDto,
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestLdapConfigurationResponse {
    pub email: String,
    pub display_name: String,
    pub duration_ms: i64,
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
    PublicWidget,
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
    pub producing_engine_version: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubSessionDto {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub agent_id: Uuid,
    pub agent_name: String,
    pub agent_deleted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub origin_platform_name: Option<String>,
    pub origin: HubSessionOriginDto,
    pub lifecycle_status: String,
    pub native_session_id: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllowedModelIdsError(&'static str);

impl fmt::Display for AllowedModelIdsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AllowedModelIdsError {}

pub fn normalize_allowed_model_ids(
    model_ids: Vec<String>,
) -> Result<Vec<String>, AllowedModelIdsError> {
    let mut seen = BTreeSet::new();
    let mut normalized = Vec::new();

    for model_id in model_ids {
        let model_id = model_id.trim();
        let length = model_id.chars().count();
        if length == 0 || length > 255 || model_id.chars().any(char::is_control) {
            return Err(AllowedModelIdsError(
                "model ids must contain 1 to 255 non-control characters",
            ));
        }
        if seen.insert(model_id.to_owned()) {
            normalized.push(model_id.to_owned());
            if normalized.len() > 256 {
                return Err(AllowedModelIdsError(
                    "a Model API Connection supports at most 256 model ids",
                ));
            }
        }
    }

    if normalized.is_empty() {
        return Err(AllowedModelIdsError(
            "a Model API Connection requires at least one model id",
        ));
    }
    Ok(normalized)
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelUpstreamProtocol {
    #[default]
    OpenaiResponses,
    OpenaiChatCompletions,
    AnthropicMessages,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelReasoningSummary {
    #[default]
    Default,
    Auto,
    Concise,
    Detailed,
    None,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelVerbosity {
    #[default]
    Default,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelReasoningSummarySupport {
    #[default]
    Auto,
    Supported,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionDto {
    pub id: Uuid,
    pub owner_id: Option<Uuid>,
    pub scope: ModelConnectionScope,
    pub name: String,
    pub base_url: String,
    pub api_type: ModelUpstreamProtocol,
    pub allowed_model_ids: Vec<String>,
    pub status: ModelConnectionStatus,
    pub has_api_key: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CreateModelConnectionRequest {
    pub scope: ModelConnectionScope,
    pub name: String,
    pub base_url: String,
    pub api_type: ModelUpstreamProtocol,
    pub allowed_model_ids: Vec<String>,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateModelConnectionRequest {
    pub name: String,
    pub base_url: String,
    pub api_type: ModelUpstreamProtocol,
    pub allowed_model_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TestModelConnectionRequest {
    pub model_id: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelSelectionDto {
    pub connection_id: Uuid,
    pub model_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionOptionDto {
    pub connection_id: Uuid,
    pub connection_name: String,
    pub model_id: String,
    pub api_type: ModelUpstreamProtocol,
    pub scope: ModelConnectionScope,
    pub status: ModelConnectionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionOptionsDto {
    pub items: Vec<ModelConnectionOptionDto>,
    pub system_default: Option<ModelSelectionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SystemDefaultModelSelectionDto {
    pub selection: Option<ModelSelectionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SetSystemDefaultModelSelectionRequest {
    pub selection: Option<ModelSelectionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateModelConnectionStatusRequest {
    pub status: ModelConnectionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelConnectionTestResultDto {
    pub success: bool,
    pub status_code: Option<u16>,
    pub error_code: Option<String>,
    pub message: Option<String>,
    pub response_text: Option<String>,
    pub response_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "protocol", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelRequestSettings {
    OpenaiResponses {},
    OpenaiChatCompletions {
        #[serde(default)]
        temperature: Option<Number>,
        #[serde(default)]
        top_p: Option<Number>,
        #[serde(default)]
        max_completion_tokens: Option<u32>,
    },
    AnthropicMessages {
        #[serde(default)]
        temperature: Option<Number>,
        #[serde(default)]
        top_p: Option<Number>,
        #[serde(default)]
        max_tokens: Option<u32>,
    },
}

impl ModelRequestSettings {
    pub fn for_protocol(protocol: ModelUpstreamProtocol) -> Self {
        match protocol {
            ModelUpstreamProtocol::OpenaiResponses => Self::OpenaiResponses {},
            ModelUpstreamProtocol::OpenaiChatCompletions => Self::OpenaiChatCompletions {
                temperature: None,
                top_p: None,
                max_completion_tokens: None,
            },
            ModelUpstreamProtocol::AnthropicMessages => Self::AnthropicMessages {
                temperature: None,
                top_p: None,
                max_tokens: None,
            },
        }
    }

    pub fn protocol(&self) -> ModelUpstreamProtocol {
        match self {
            Self::OpenaiResponses { .. } => ModelUpstreamProtocol::OpenaiResponses,
            Self::OpenaiChatCompletions { .. } => ModelUpstreamProtocol::OpenaiChatCompletions,
            Self::AnthropicMessages { .. } => ModelUpstreamProtocol::AnthropicMessages,
        }
    }
}

impl Default for ModelRequestSettings {
    fn default() -> Self {
        Self::for_protocol(ModelUpstreamProtocol::OpenaiResponses)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct AgentModelSettings {
    pub reasoning_effort: ReasoningEffort,
    pub reasoning_summary: ModelReasoningSummary,
    pub verbosity: ModelVerbosity,
    pub context_window_tokens: Option<u64>,
    pub auto_compact_token_limit: Option<u64>,
    pub reasoning_summary_support: ModelReasoningSummarySupport,
    pub service_tier: Option<String>,
    pub provider_request_timeout_ms: Option<u64>,
    pub stream_max_retries: Option<u32>,
    pub stream_idle_timeout_ms: Option<u64>,
    pub request_settings: ModelRequestSettings,
}

impl Default for AgentModelSettings {
    fn default() -> Self {
        Self {
            reasoning_effort: ReasoningEffort::Default,
            reasoning_summary: ModelReasoningSummary::Default,
            verbosity: ModelVerbosity::Default,
            context_window_tokens: None,
            auto_compact_token_limit: None,
            reasoning_summary_support: ModelReasoningSummarySupport::Auto,
            service_tier: None,
            provider_request_timeout_ms: None,
            stream_max_retries: None,
            stream_idle_timeout_ms: None,
            request_settings: ModelRequestSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ModelSettingOverride<T> {
    #[default]
    Inherit,
    Automatic,
    Value(T),
}

impl<T> ModelSettingOverride<T> {
    fn is_inherit(&self) -> bool {
        matches!(self, Self::Inherit)
    }
}

impl<T> Serialize for ModelSettingOverride<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Inherit | Self::Automatic => serializer.serialize_none(),
            Self::Value(value) => value.serialize(serializer),
        }
    }
}

impl<'de, T> Deserialize<'de> for ModelSettingOverride<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match Option::<T>::deserialize(deserializer)? {
            Some(value) => Self::Value(value),
            None => Self::Automatic,
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct AgentModelSettingsOverride {
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub reasoning_effort: ModelSettingOverride<ReasoningEffort>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub reasoning_summary: ModelSettingOverride<ModelReasoningSummary>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub verbosity: ModelSettingOverride<ModelVerbosity>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub context_window_tokens: ModelSettingOverride<u64>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub auto_compact_token_limit: ModelSettingOverride<u64>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub reasoning_summary_support: ModelSettingOverride<ModelReasoningSummarySupport>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub service_tier: ModelSettingOverride<String>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub provider_request_timeout_ms: ModelSettingOverride<u64>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub stream_max_retries: ModelSettingOverride<u32>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub stream_idle_timeout_ms: ModelSettingOverride<u64>,
    #[serde(skip_serializing_if = "ModelSettingOverride::is_inherit")]
    pub request_settings: ModelSettingOverride<ModelRequestSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunModelBindingDto {
    pub id: Uuid,
    pub run_id: Uuid,
    pub binding_key: String,
    pub model_connection_id: Uuid,
    pub connection_name_snapshot: String,
    pub connection_scope_snapshot: ModelConnectionScope,
    pub model_id: String,
    pub api_type: ModelUpstreamProtocol,
    pub model_settings: AgentModelSettings,
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
    pub api_type: ModelUpstreamProtocol,
    pub request_settings: ModelRequestSettings,
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
pub struct SubagentDefinition {
    pub name: String,
    pub description: String,
    pub developer_instructions: String,
    #[serde(default)]
    pub model_selection: Option<ModelSelectionDto>,
    #[serde(default)]
    pub model_settings_override: AgentModelSettingsOverride,
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
    pub model_selection: Option<ModelSelectionDto>,
    #[serde(default)]
    pub model_settings: AgentModelSettings,
    #[serde(default)]
    pub subagents: Vec<SubagentDefinition>,
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub managed_skill_ids: Vec<Uuid>,
    #[serde(default)]
    pub secret_declarations: Vec<AgentSecretDeclarationDto>,
    pub mcp_allowlist: Value,
    #[serde(default = "default_agent_tool_allowlist")]
    pub tool_allowlist: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WidgetUserProfileDto {
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub attributes: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExternalUserContextDto {
    pub external_user_id: String,
    pub tenant_id: String,
    pub username: Option<String>,
    pub display_name: Option<String>,
    pub email: Option<String>,
    pub attributes: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientToolDefinitionDto {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientToolErrorDto {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ClientToolResultDto {
    Success { output: Value },
    Error { error: ClientToolErrorDto },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ClientToolContinuationResultDto {
    pub tool_call_id: Uuid,
    pub tool_name: String,
    pub result: ClientToolResultDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetSessionDto {
    #[serde(flatten)]
    pub agent: WidgetAgentDto,
    pub expires_at: DateTime<Utc>,
    pub history_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetAccessResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub agent: WidgetAgentDto,
    pub history_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientAccessResponse {
    pub access_token: String,
    pub expires_at: DateTime<Utc>,
    pub expires_in: i64,
    pub client_instance_id: Uuid,
    pub session_id: Option<Uuid>,
    pub agent: WidgetAgentDto,
    pub history_enabled: bool,
    pub tool_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePublicWidgetAccessRequest {
    pub client_id: String,
    pub visitor_key: String,
    pub client_instance_id: Uuid,
    #[serde(default)]
    pub session_id: Option<Uuid>,
}

pub type CreateAnonymousClientAccessRequest = CreatePublicWidgetAccessRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicWidgetAccessResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub widget_session_id: Uuid,
    pub hub_session_id: Option<Uuid>,
    pub agent: WidgetAgentDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetTokenResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WidgetHistorySessionDto {
    pub id: Uuid,
    pub hub_session_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub preview: Option<String>,
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
    #[serde(default)]
    pub package: Option<SkillPackageDto>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillPackageFileDto {
    pub path: String,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub executable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillPackageDto {
    pub id: Uuid,
    pub format_version: u32,
    pub size_bytes: u64,
    pub checksum_sha256: String,
    pub files: Vec<SkillPackageFileDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSecretDeclarationDto {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserSecretDto {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size_bytes: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sha256: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUserSecretRequest {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub file_base64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateUserSecretRequest {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub file_base64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretGrantDto {
    pub user_id: Uuid,
    pub agent_id: Uuid,
    pub secret_name: String,
    pub granted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSecretGrantRequest {
    pub agent_id: Uuid,
    pub secret_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretGrantRequirementDto {
    pub name: String,
    pub kind: String,
    pub description: String,
}

pub const MAX_SKILL_PACKAGE_ARCHIVE_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_SKILL_PACKAGE_EXPANDED_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_SKILL_PACKAGE_FILES: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentExecutionConfigurationDto {
    pub revision: i64,
    pub instructions: String,
    #[serde(default)]
    pub secret_declarations: Vec<AgentSecretDeclarationDto>,
    #[serde(default)]
    pub model_selection: Option<ModelSelectionDto>,
    #[serde(default)]
    pub model_settings: AgentModelSettings,
    #[serde(default)]
    pub subagents: Vec<SubagentDefinition>,
    #[serde(default)]
    pub model_bindings: Vec<RunModelBindingDto>,
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub skills: Vec<AgentExecutionSkillDto>,
    pub mcp_allowlist: Value,
    #[serde(default = "default_agent_tool_allowlist")]
    pub tool_allowlist: Vec<String>,
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
    #[serde(default)]
    pub package: Option<SkillPackageDto>,
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

    let mut binding_ids = BTreeSet::new();
    let mut binding_keys = BTreeSet::new();
    let mut model_bindings = configuration.model_bindings.clone();
    for binding in &model_bindings {
        let normalized_key = binding.binding_key.trim().to_lowercase();
        if !binding_ids.insert(binding.id) || !binding_keys.insert(normalized_key) {
            return Err(ExecutionConfigurationError(
                "Run Model Binding ids and keys must be unique",
            ));
        }
        if binding.binding_key.trim().is_empty()
            || binding.connection_name_snapshot.trim().is_empty()
            || binding.model_id.trim().is_empty()
            || binding.model_settings.request_settings.protocol() != binding.api_type
        {
            return Err(ExecutionConfigurationError(
                "Run Model Binding metadata and request settings are invalid",
            ));
        }
    }
    let main_binding = model_bindings
        .iter()
        .find(|binding| binding.binding_key.eq_ignore_ascii_case("main"));
    match (&configuration.model_selection, main_binding) {
        (None, None) => {}
        (Some(selection), Some(binding))
            if selection.connection_id == binding.model_connection_id
                && selection.model_id == binding.model_id
                && configuration.model_settings == binding.model_settings => {}
        _ => {
            return Err(ExecutionConfigurationError(
                "main Run Model Binding must match the Agent model selection and settings",
            ));
        }
    }
    model_bindings.sort_by(|left, right| {
        left.binding_key
            .trim()
            .to_lowercase()
            .cmp(&right.binding_key.trim().to_lowercase())
            .then_with(|| left.id.cmp(&right.id))
    });
    let model_binding_metadata = model_bindings
        .iter()
        .map(|binding| {
            json!({
                "binding_key": binding.binding_key,
                "model_connection_id": binding.model_connection_id,
                "connection_name_snapshot": binding.connection_name_snapshot,
                "connection_scope_snapshot": binding.connection_scope_snapshot,
                "model_id": binding.model_id,
                "api_type": binding.api_type,
                "model_settings": binding.model_settings,
            })
        })
        .collect::<Vec<_>>();

    let mut subagent_names = BTreeSet::new();
    let mut subagents = configuration.subagents.clone();
    for subagent in &subagents {
        let normalized_name = subagent.name.trim().to_lowercase();
        if normalized_name.is_empty()
            || subagent.description.trim().is_empty()
            || subagent.developer_instructions.trim().is_empty()
        {
            return Err(ExecutionConfigurationError(
                "Subagent name, description, and developer instructions are required",
            ));
        }
        if !subagent_names.insert(normalized_name.clone()) {
            return Err(ExecutionConfigurationError(
                "Subagent names must be unique ignoring case",
            ));
        }
        match (subagent.enabled, subagent.disabled_reason.as_deref()) {
            (true, None) => {}
            (false, Some(reason)) if !reason.trim().is_empty() => {}
            _ => {
                return Err(ExecutionConfigurationError(
                    "Subagent enabled and disabled reason shape is invalid",
                ));
            }
        }
        if subagent.enabled
            && (subagent.model_selection.is_some()
                || subagent.model_settings_override != AgentModelSettingsOverride::default())
            && !binding_keys.contains(&normalized_name)
        {
            return Err(ExecutionConfigurationError(
                "Subagent override must have a matching Run Model Binding",
            ));
        }
    }
    subagents.sort_by_key(|subagent| subagent.name.trim().to_lowercase());

    let mut secret_declarations = configuration.secret_declarations.clone();
    secret_declarations.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    let mut declaration_names = BTreeSet::new();
    for declaration in &secret_declarations {
        let valid_name = !declaration.name.is_empty()
            && declaration.name.len() <= 128
            && declaration
                .name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            && (declaration
                .name
                .starts_with(|byte: char| byte.is_ascii_uppercase())
                || declaration.name.starts_with('_'));
        if !valid_name
            || !matches!(declaration.kind.as_str(), "value" | "file")
            || declaration.description.len() > 512
            || !declaration_names.insert(declaration.name.clone())
        {
            return Err(ExecutionConfigurationError(
                "Agent Secret Declarations must have unique valid names and kinds",
            ));
        }
    }

    let mut names = BTreeSet::new();
    let mut skills = configuration.skills.clone();
    for skill in &mut skills {
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
        if let Some(package) = &mut skill.package {
            if package.id.is_nil()
                || package.format_version != 1
                || package.size_bytes == 0
                || package.size_bytes > MAX_SKILL_PACKAGE_ARCHIVE_BYTES
                || !is_lowercase_sha256(&package.checksum_sha256)
                || package.files.is_empty()
                || package.files.len() > MAX_SKILL_PACKAGE_FILES
            {
                return Err(ExecutionConfigurationError(
                    "Skill package metadata is invalid",
                ));
            }
            package
                .files
                .sort_by(|left, right| left.path.cmp(&right.path));
            let mut paths = BTreeSet::new();
            let mut expanded_bytes = 0_u64;
            for file in &package.files {
                let path_is_safe = !file.path.is_empty()
                    && !file.path.starts_with('/')
                    && !file.path.contains('\\')
                    && file.path != "SKILL.md"
                    && file.path.split('/').all(|component| {
                        !component.is_empty() && component != "." && component != ".."
                    });
                expanded_bytes = expanded_bytes.saturating_add(file.size_bytes);
                if !path_is_safe
                    || !paths.insert(file.path.clone())
                    || !is_lowercase_sha256(&file.checksum_sha256)
                    || !file.executable
                    || expanded_bytes > MAX_SKILL_PACKAGE_EXPANDED_BYTES
                {
                    return Err(ExecutionConfigurationError(
                        "Skill package file metadata is invalid",
                    ));
                }
            }
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
                "package": skill.package,
            })
        })
        .collect::<Vec<_>>();
    let mcp_allowlist = normalized_redacted_mcp(&configuration.mcp_allowlist)?;
    let tool_allowlist = normalized_agent_tool_allowlist(&configuration.tool_allowlist)?;
    let value = json!({
        "revision": configuration.revision,
        "instructions": configuration.instructions,
        "model_selection": configuration.model_selection,
        "model_settings": configuration.model_settings,
        "subagents": subagents,
        "model_bindings": model_binding_metadata,
        "model_policy": configuration.model_policy,
        "sandbox_policy": configuration.sandbox_policy,
        "skills": skill_metadata,
        "secret_declarations": secret_declarations,
        "mcp_allowlist": mcp_allowlist,
        "tool_allowlist": tool_allowlist,
    });
    Ok(format!(
        "sha256:{}",
        sha256_hex(canonical_json(&value).as_bytes())
    ))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn normalized_agent_tool_allowlist(
    tools: &[String],
) -> Result<Vec<String>, ExecutionConfigurationError> {
    if tools.is_empty() {
        return Err(ExecutionConfigurationError(
            "Agent tool allowlist must not be empty",
        ));
    }
    let requested = tools
        .iter()
        .map(|tool| tool.trim())
        .collect::<BTreeSet<_>>();
    if requested.len() != tools.len()
        || requested
            .iter()
            .any(|tool| !AGENT_TOOL_NAMES.contains(tool))
    {
        return Err(ExecutionConfigurationError(
            "Agent tool allowlist is invalid",
        ));
    }
    Ok(AGENT_TOOL_NAMES
        .iter()
        .filter(|tool| requested.contains(**tool))
        .map(|tool| (*tool).to_owned())
        .collect())
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
    pub engine_version: String,
    pub capabilities: Value,
    pub sandbox_mode: String,
    pub status: String,
    pub last_heartbeat_at: DateTime<Utc>,
    pub credential_rotation_requested_at: Option<DateTime<Utc>>,
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
    pub native_session_id: Option<String>,
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
    pub native_session_id: String,
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
    pub widget_history_enabled: bool,
    #[serde(default = "default_true")]
    pub login_required: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub tool_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub client_tool_definitions: Vec<ClientToolDefinitionDto>,
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
    pub session_id: Option<Uuid>,
    pub hub_session_id: Uuid,
    pub run_id: Uuid,
    pub position: i32,
    pub tool_name: String,
    pub arguments: Value,
    pub status: String,
    pub claimed_by_client_instance_id: Option<Uuid>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub result_payload: Option<Value>,
    pub follow_up_run_id: Option<Uuid>,
    pub expires_at: DateTime<Utc>,
    pub responded_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationContextDto {
    pub tools: Value,
    pub attachments: Value,
    pub tool_result: Option<Value>,
    #[serde(default)]
    pub tool_results: Vec<ClientToolContinuationResultDto>,
    #[serde(default)]
    pub external_user: Option<ExternalUserContextDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginResponse {
    pub user: UserDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordRegistrationRequest {
    pub email: String,
    pub password: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordRegistrationResponse {
    pub user: UserDto,
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
    pub password_registration_enabled: bool,
    pub password_login_enabled: bool,
    pub ldap_login_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateIntegrationAppRequest {
    pub name: String,
    pub external_platform_id: Uuid,
    pub authentication_channel_id: Uuid,
    pub redirect_uris: Value,
    pub agent_ids: Vec<Uuid>,
    #[serde(default)]
    pub widget_history_enabled: bool,
    #[serde(default = "default_true")]
    pub login_required: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub tool_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub client_tool_definitions: Vec<ClientToolDefinitionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateIntegrationAppRequest {
    pub name: String,
    pub redirect_uris: Value,
    pub agent_ids: Vec<Uuid>,
    #[serde(default)]
    pub widget_history_enabled: bool,
    #[serde(default = "default_true")]
    pub login_required: bool,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub tool_allowlist: Option<Vec<String>>,
    #[serde(default)]
    pub client_tool_definitions: Vec<ClientToolDefinitionDto>,
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
    pub model_selection: Option<ModelSelectionDto>,
    #[serde(default)]
    pub model_settings: Option<AgentModelSettings>,
    #[serde(default)]
    pub subagents: Vec<SubagentDefinition>,
    #[serde(default = "default_agent_tool_allowlist")]
    pub tool_allowlist: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_declarations: Option<Vec<AgentSecretDeclarationDto>>,
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
    pub model_selection: Option<ModelSelectionDto>,
    #[serde(default)]
    pub model_settings: AgentModelSettings,
    #[serde(default)]
    pub subagents: Vec<SubagentDefinition>,
    #[doc(hidden)]
    #[serde(skip, default = "legacy_hub_proxy_model_policy")]
    pub model_policy: Value,
    pub sandbox_policy: Value,
    pub managed_skill_ids: Vec<Uuid>,
    pub mcp_allowlist: Value,
    #[serde(default = "default_agent_tool_allowlist")]
    pub tool_allowlist: Vec<String>,
    /// 省略/传 null 表示保留现有声明，只有显式数组才替换。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_declarations: Option<Vec<AgentSecretDeclarationDto>>,
}

pub const AGENT_TOOL_NAMES: &[&str] = &[
    "read",
    "grep",
    "find",
    "ls",
    "edit",
    "write",
    "bash",
    "skill_exec",
    "integration",
];

pub const PUBLIC_WIDGET_TOOL_NAMES: &[&str] = &["read", "grep", "find", "ls", "integration"];

pub fn default_agent_tool_allowlist() -> Vec<String> {
    AGENT_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect()
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
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    pub tools: Value,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWidgetAccessRequest {
    pub agent_id: Uuid,
    pub client_instance_id: Uuid,
    pub external_user_id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    pub email: String,
    #[serde(default)]
    pub attributes: Value,
    #[serde(default)]
    pub client_tools: Vec<ClientToolDefinitionDto>,
}

pub type CreateClientAccessRequest = CreateWidgetAccessRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewWidgetSessionRequest {
    #[serde(default)]
    pub profile: Option<WidgetUserProfileDto>,
}

pub type RenewClientAccessRequest = RenewWidgetSessionRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateWidgetRunRequest {
    pub message: String,
    #[serde(default)]
    pub session_id: Option<Uuid>,
    #[serde(default)]
    pub integration_session_id: Option<Uuid>,
    #[serde(default)]
    pub hub_session_id: Option<Uuid>,
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
    #[serde(default)]
    pub client_message_key: Option<String>,
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
#[serde(deny_unknown_fields)]
pub struct SubmitClientToolResultRequest {
    pub result: ClientToolResultDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientToolClaimResponse {
    pub status: String,
    pub terminal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ClientToolResultDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitClientToolResultResponse {
    pub run: Option<RunDto>,
    pub tool_request: IntegrationToolRequestDto,
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
    pub engine_version: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOwnedSessionSnapshotDto {
    pub session_id: Uuid,
    pub ownership_generation: i64,
    pub lifecycle_status: String,
    pub native_session_id: Option<String>,
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
    pub native_session_id: Option<String>,
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
    #[serde(default)]
    pub secret_values: Vec<RunSecretValueDto>,
    #[serde(default)]
    pub secret_files: Vec<RunSecretFileDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_context: Option<ClaimSessionContextDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSecretValueDto {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunSecretFileDto {
    pub name: String,
    pub size_bytes: i64,
    pub sha256: String,
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
    #[serde(default)]
    pub force: bool,
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
    #[serde(default = "random_append_event_id")]
    pub event_id: Uuid,
    pub event_type: String,
    pub role: Option<String>,
    pub content: Option<String>,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_tool: Option<WaitingToolRunTransition>,
}

fn random_append_event_id() -> Uuid {
    Uuid::new_v4()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitingToolRunTransition {
    pub native_session_id: String,
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
    #[serde(default)]
    pub integration_session_id: Option<Uuid>,
    pub native_session_id: String,
    pub work_dir_ref: String,
    pub tool_requests: Vec<FinalizeToolRequestEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteRunRequest {
    pub status: String,
    pub native_session_id: Option<String>,
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
            engine_version: "0.1.0".into(),
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
            engine_version: "0.1.0".into(),
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
    fn allowed_model_ids_are_normalized_once_and_remain_case_sensitive() {
        let normalized = normalize_allowed_model_ids(vec![
            "  gpt-5.6  ".into(),
            "GPT-5.6".into(),
            "gpt-5.6".into(),
            "\u{3000}model-two\u{3000}".into(),
        ])
        .unwrap();

        assert_eq!(normalized, vec!["gpt-5.6", "GPT-5.6", "model-two"]);
        assert!(normalize_allowed_model_ids(Vec::new()).is_err());
        assert!(normalize_allowed_model_ids(vec![" \t ".into()]).is_err());
        assert!(normalize_allowed_model_ids(vec!["bad\nmodel".into()]).is_err());
        assert!(normalize_allowed_model_ids(vec!["x".repeat(256)]).is_err());
        assert!(normalize_allowed_model_ids(
            (0..257).map(|index| format!("model-{index}")).collect()
        )
        .is_err());
    }

    #[test]
    fn model_api_connection_contract_owns_access_and_multiple_model_ids_only() {
        let create: CreateModelConnectionRequest = serde_json::from_value(json!({
            "scope": "personal",
            "name": "Provider access",
            "base_url": "https://models.example.test",
            "api_type": "openai_chat_completions",
            "allowed_model_ids": ["model-a", "model-b"],
            "api_key": "write-only-secret"
        }))
        .unwrap();
        assert_eq!(create.allowed_model_ids, vec!["model-a", "model-b"]);
        assert_eq!(
            create.api_type,
            ModelUpstreamProtocol::OpenaiChatCompletions
        );

        assert!(
            serde_json::from_value::<CreateModelConnectionRequest>(json!({
                "scope": "personal",
                "name": "Legacy shape",
                "base_url": "https://models.example.test",
                "model_id": "model-a",
                "parameters": {},
                "api_key": "secret"
            }))
            .is_err()
        );

        let now = DateTime::parse_from_rfc3339("2026-07-22T08:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let dto = ModelConnectionDto {
            id: Uuid::from_u128(101),
            owner_id: Some(Uuid::from_u128(102)),
            scope: ModelConnectionScope::Personal,
            name: "Provider access".into(),
            base_url: "https://models.example.test".into(),
            api_type: ModelUpstreamProtocol::OpenaiChatCompletions,
            allowed_model_ids: vec!["model-a".into(), "model-b".into()],
            status: ModelConnectionStatus::Enabled,
            has_api_key: true,
            created_at: now,
            updated_at: now,
        };
        let value = serde_json::to_value(dto).unwrap();
        assert_eq!(value["allowed_model_ids"], json!(["model-a", "model-b"]));
        assert_eq!(value["api_type"], "openai_chat_completions");
        assert!(value.get("api_key").is_none());
        assert!(value.get("model_id").is_none());
        assert!(value.get("parameters").is_none());
        assert!(value.get("request_parameters").is_none());
    }

    #[test]
    fn agent_model_settings_and_subagent_overrides_preserve_intent() {
        let settings: AgentModelSettings = serde_json::from_value(json!({
            "reasoning_effort": "high",
            "reasoning_summary": "detailed",
            "verbosity": "low",
            "context_window_tokens": 200000,
            "auto_compact_token_limit": 160000,
            "reasoning_summary_support": "supported",
            "service_tier": "priority",
            "provider_request_timeout_ms": 300000,
            "stream_max_retries": 9,
            "stream_idle_timeout_ms": 420000,
            "request_settings": {
                "protocol": "openai_chat_completions",
                "temperature": 0.7,
                "top_p": 0.9,
                "max_completion_tokens": 4096
            }
        }))
        .unwrap();
        assert_eq!(settings.reasoning_effort, ReasoningEffort::High);
        assert_eq!(settings.provider_request_timeout_ms, Some(300000));
        assert_eq!(
            settings.request_settings.protocol(),
            ModelUpstreamProtocol::OpenaiChatCompletions
        );

        let overrides: AgentModelSettingsOverride = serde_json::from_value(json!({
            "reasoning_effort": "default",
            "context_window_tokens": null,
            "stream_max_retries": 4
        }))
        .unwrap();
        assert_eq!(
            overrides.reasoning_effort,
            ModelSettingOverride::Value(ReasoningEffort::Default)
        );
        assert_eq!(
            overrides.context_window_tokens,
            ModelSettingOverride::Automatic
        );
        assert_eq!(overrides.stream_max_retries, ModelSettingOverride::Value(4));
        assert_eq!(overrides.verbosity, ModelSettingOverride::Inherit);
        assert_eq!(
            serde_json::to_value(overrides).unwrap(),
            json!({
                "reasoning_effort": "default",
                "context_window_tokens": null,
                "stream_max_retries": 4
            })
        );
    }

    #[test]
    fn model_selection_system_default_and_run_binding_use_complete_pairs() {
        let connection_id = Uuid::from_u128(201);
        let run_id = Uuid::from_u128(202);
        let selection: ModelSelectionDto = serde_json::from_value(json!({
            "connection_id": connection_id,
            "model_id": "model-a"
        }))
        .unwrap();
        assert_eq!(selection.model_id, "model-a");
        assert!(serde_json::from_value::<ModelSelectionDto>(json!({
            "connection_id": connection_id
        }))
        .is_err());

        let system_default = SystemDefaultModelSelectionDto {
            selection: Some(selection.clone()),
        };
        assert_eq!(
            serde_json::to_value(system_default).unwrap(),
            json!({ "selection": selection })
        );

        let binding = RunModelBindingDto {
            id: Uuid::from_u128(203),
            run_id,
            binding_key: "main".into(),
            model_connection_id: connection_id,
            connection_name_snapshot: "Provider access".into(),
            connection_scope_snapshot: ModelConnectionScope::Global,
            model_id: "model-a".into(),
            api_type: ModelUpstreamProtocol::OpenaiResponses,
            model_settings: AgentModelSettings::default(),
        };
        let value = serde_json::to_value(binding).unwrap();
        assert_eq!(value["id"], Uuid::from_u128(203).to_string());
        assert_eq!(value["binding_key"], "main");
        assert_eq!(value["model_id"], "model-a");
        assert!(value.get("base_url").is_none());
        assert!(value.get("api_key").is_none());
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
            api_type: ModelUpstreamProtocol::OpenaiResponses,
            request_settings: ModelRequestSettings::default(),
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
        let user_value = json!({
            "id": Uuid::nil(),
            "email": "member@example.com",
            "display_name": "Member",
            "role": "member"
        });
        let user: UserDto = serde_json::from_value(user_value.clone()).unwrap();
        assert_eq!(serde_json::to_value(user).unwrap(), user_value);

        let request: EraseUserRequest = serde_json::from_value(json!({
            "email": "member@example.com"
        }))
        .unwrap();
        assert_eq!(request.email, "member@example.com");
        let erasure = UserErasureDto {
            user_id: Uuid::nil(),
            email: Some("member@example.com".into()),
            status: "pending".into(),
            requested_at: Utc::now(),
            completed_at: None,
        };
        assert_eq!(serde_json::to_value(&erasure).unwrap()["status"], "pending");

        let policy = AuthPolicyDto {
            password_registration_enabled: true,
            password_login_enabled: false,
            ldap_login_enabled: true,
        };
        assert_eq!(
            serde_json::to_value(policy).unwrap(),
            json!({
                "password_registration_enabled": true,
                "password_login_enabled": false,
                "ldap_login_enabled": true
            })
        );

        let ldap_configuration: LdapConfigurationDto = serde_json::from_value(json!({
            "url": "ldap://directory.example.com:389",
            "security": "starttls",
            "base_dn": "ou=people,dc=example,dc=com",
            "bind_identity_template": "uid={email},ou=people,dc=example,dc=com",
            "user_filter": "(mail={email})",
            "email_attribute": "mail",
            "display_name_attribute": "displayName",
            "allow_insecure": false,
            "skip_tls_verify": false
        }))
        .unwrap();
        assert_eq!(
            ldap_configuration.bind_identity_template,
            "uid={email},ou=people,dc=example,dc=com"
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
            widget_history_enabled: false,
            login_required: true,
            allowed_origins: Vec::new(),
            tool_allowlist: None,
            client_tool_definitions: Vec::new(),
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
        assert!(value.get("username").is_none());
        assert_eq!(value["external_profile"]["tenant_id"], "tenant-1");
    }

    #[test]
    fn refresh_configuration_command_carries_verifiable_configuration() {
        let content = "managed content";
        let configuration = AgentExecutionConfigurationDto {
            revision: 8,
            instructions: "Use the current managed Skills".into(),
            secret_declarations: Vec::new(),
            model_selection: None,
            model_settings: AgentModelSettings::default(),
            subagents: Vec::new(),
            model_bindings: Vec::new(),
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
                package: None,
            }],
            mcp_allowlist: json!([]),
            tool_allowlist: default_agent_tool_allowlist(),
        };
        let fingerprint = execution_configuration_fingerprint(&configuration).unwrap();
        let command = RuntimeSessionCommandDto {
            command_id: Uuid::from_u128(11),
            session_id: Uuid::from_u128(12),
            ownership_generation: 4,
            command: "refresh_configuration".into(),
            run_id: None,
            turn_id: None,
            native_session_id: Some("thread-12".into()),
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
            origin_platform_name: Some("Trusted Platform".into()),
            origin: HubSessionOriginDto::External {
                platform_id: Uuid::from_u128(6),
                tenant_id: "workspace-1".into(),
                external_identity_id: Uuid::from_u128(7),
            },
            lifecycle_status: "offline".into(),
            native_session_id: Some("thread-1".into()),
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
                producing_engine_version: "0.42.0".into(),
                created_at: now,
            }),
            created_at: now,
            updated_at: now,
        };
        let value = serde_json::to_value(session).unwrap();
        assert_eq!(value["origin"]["kind"], "external");
        assert!(value["origin"].get("platform_name").is_none());
        assert_eq!(value["origin_platform_name"], "Trusted Platform");
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
            "native_session_id": null,
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
        assert!(native.origin_platform_name.is_none());

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
                native_session_id: Some("thread-1".into()),
                work_dir_ref: Some("session-11".into()),
            },
        };
        let value = serde_json::to_value(write).unwrap();
        assert_eq!(value["ownership_generation"], 7);
        assert_eq!(value["payload"]["status"], "completed");
        assert_eq!(value["payload"]["native_session_id"], "thread-1");
        assert!(value["payload"].get("session_id").is_none());

        let heartbeat = RuntimeHeartbeatResponse {
            rotation_requested: false,
            pending_credential_accepted: false,
            credential_activated: false,
            runtime_status: "draining".into(),
            owned_sessions: vec![RuntimeOwnedSessionSnapshotDto {
                session_id,
                ownership_generation: 7,
                lifecycle_status: "online".into(),
                native_session_id: Some("thread-11".into()),
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
                native_session_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }],
        };
        let value = serde_json::to_value(heartbeat).unwrap();
        assert_eq!(value["runtime_status"], "draining");
        assert_eq!(value["owned_sessions"][0]["ownership_generation"], 7);
        assert_eq!(value["owned_sessions"][0]["native_session_id"], "thread-11");
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
                checkpoint_reason: Some("idle".into()),
            }],
            cleaned_sessions: Vec::new(),
        };
        let value = serde_json::to_value(saving).unwrap();
        assert_eq!(value["owned_sessions"][0]["checkpoint_reason"], "idle");
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
    fn execution_configuration_fingerprint_uses_effective_run_bindings() {
        let connection_id = Uuid::from_u128(20);
        let run_id = Uuid::from_u128(21);
        let selection = ModelSelectionDto {
            connection_id,
            model_id: "model-a".into(),
        };
        let main_settings = AgentModelSettings {
            reasoning_effort: ReasoningEffort::High,
            ..AgentModelSettings::default()
        };
        let mut reviewer_settings = main_settings.clone();
        reviewer_settings.reasoning_effort = ReasoningEffort::Max;
        let main_binding = RunModelBindingDto {
            id: Uuid::from_u128(22),
            run_id,
            binding_key: "main".into(),
            model_connection_id: connection_id,
            connection_name_snapshot: "Provider".into(),
            connection_scope_snapshot: ModelConnectionScope::Global,
            model_id: "model-a".into(),
            api_type: ModelUpstreamProtocol::OpenaiResponses,
            model_settings: main_settings.clone(),
        };
        let reviewer_binding = RunModelBindingDto {
            id: Uuid::from_u128(23),
            run_id,
            binding_key: "reviewer".into(),
            model_settings: reviewer_settings,
            ..main_binding.clone()
        };
        let configuration = AgentExecutionConfigurationDto {
            revision: 3,
            instructions: "Use the configured model".into(),
            secret_declarations: Vec::new(),
            model_selection: Some(selection),
            model_settings: main_settings,
            subagents: vec![SubagentDefinition {
                name: "reviewer".into(),
                description: "Reviews changes".into(),
                developer_instructions: "# Review".into(),
                model_selection: None,
                model_settings_override: AgentModelSettingsOverride {
                    reasoning_effort: ModelSettingOverride::Value(ReasoningEffort::Max),
                    ..AgentModelSettingsOverride::default()
                },
                enabled: true,
                disabled_reason: None,
            }],
            model_bindings: vec![main_binding, reviewer_binding],
            model_policy: json!({ "provider": "hub-proxy" }),
            sandbox_policy: json!({ "mode": "workspace-write" }),
            skills: Vec::new(),
            mcp_allowlist: json!([{
                "name": "repo",
                "secrets": { "TOKEN": "first-secret" }
            }]),
            tool_allowlist: default_agent_tool_allowlist(),
        };

        let fingerprint = execution_configuration_fingerprint(&configuration).unwrap();
        let mut reordered = configuration.clone();
        reordered.model_bindings.reverse();
        reordered.model_bindings[0].id = Uuid::from_u128(30);
        reordered.model_bindings[0].run_id = Uuid::from_u128(31);
        reordered.mcp_allowlist[0]["secrets"]["TOKEN"] = json!("second-secret");
        assert_eq!(
            execution_configuration_fingerprint(&reordered).unwrap(),
            fingerprint
        );

        let mut changed = configuration.clone();
        changed.model_settings.reasoning_effort = ReasoningEffort::Ultra;
        changed.model_bindings[0].model_settings.reasoning_effort = ReasoningEffort::Ultra;
        assert_ne!(
            execution_configuration_fingerprint(&changed).unwrap(),
            fingerprint
        );

        let mut missing_main = configuration;
        missing_main.model_bindings.remove(0);
        assert!(execution_configuration_fingerprint(&missing_main).is_err());
    }

    #[test]
    fn execution_configuration_rejects_inline_skills() {
        let content = "inline content";
        let configuration = AgentExecutionConfigurationDto {
            revision: 1,
            instructions: String::new(),
            secret_declarations: Vec::new(),
            model_selection: None,
            model_settings: AgentModelSettings::default(),
            subagents: Vec::new(),
            model_bindings: Vec::new(),
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
                package: None,
            }],
            mcp_allowlist: json!([]),
            tool_allowlist: default_agent_tool_allowlist(),
        };

        assert!(execution_configuration_fingerprint(&configuration).is_err());
    }
}
