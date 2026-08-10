//! 数据库行到 DTO 的转换。

use agent_hub_shared::*;
use chrono::DateTime;
use serde_json::json;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

pub(crate) fn user_from_row(row: sqlx::postgres::PgRow) -> UserDto {
    UserDto {
        id: row.get("id"),
        email: row.get("email"),
        display_name: row.get("display_name"),
        role: row.get("role"),
    }
}

pub(crate) fn admin_user_detail_from_row(row: sqlx::postgres::PgRow) -> AdminUserDetailDto {
    AdminUserDetailDto {
        user: UserDto {
            id: row.get("id"),
            email: row.get("email"),
            display_name: row.get("display_name"),
            role: row.get("role"),
        },
        has_password: row.get("has_password"),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn auth_policy_from_row(row: sqlx::postgres::PgRow) -> AuthPolicyDto {
    AuthPolicyDto {
        password_registration_enabled: row.get("password_registration_enabled"),
        password_login_enabled: row.get("password_login_enabled"),
        ldap_login_enabled: row.get("ldap_login_enabled"),
        email_placeholder: row.get("email_placeholder"),
        password_placeholder: row.get("password_placeholder"),
    }
}

pub(crate) fn external_platform_from_row(row: sqlx::postgres::PgRow) -> ExternalPlatformDto {
    ExternalPlatformDto {
        id: row.get("id"),
        key: row.get("key"),
        name: row.get("name"),
    }
}

pub(crate) fn authentication_channel_from_row(
    row: sqlx::postgres::PgRow,
) -> AuthenticationChannelDto {
    AuthenticationChannelDto {
        id: row.get("id"),
        platform_id: row.get("platform_id"),
        key: row.get("key"),
        name: row.get("name"),
        enabled: row.get("enabled"),
        trusted_email: row.get("trusted_email"),
    }
}

pub(crate) fn agent_from_row(row: sqlx::postgres::PgRow) -> AgentDto {
    let connection_id: Option<Uuid> = row.try_get("model_connection_id").unwrap_or_default();
    let model_id: Option<String> = row.try_get("model_id").unwrap_or_default();
    AgentDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        owner_email: row.get("owner_email"),
        name: row.get("name"),
        instructions: row.get("instructions"),
        visibility: row.get("visibility"),
        public_to: row.get("public_to"),
        runtime_id: row.get("runtime_id"),
        model_selection: connection_id
            .zip(model_id)
            .map(|(connection_id, model_id)| ModelSelectionDto {
                connection_id,
                model_id,
            }),
        model_settings: row
            .try_get::<Value, _>("model_settings")
            .ok()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default(),
        subagents: Vec::new(),
        model_policy: row.get("model_policy"),
        sandbox_policy: row.get("sandbox_policy"),
        managed_skill_ids: row.try_get("managed_skill_ids").unwrap_or_default(),
        secret_declarations: Vec::new(),
        mcp_allowlist: row.get("mcp_allowlist"),
        tool_allowlist: row
            .try_get::<Value, _>("tool_allowlist")
            .ok()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_else(default_agent_tool_allowlist),
        is_owner: false,
        can_manage: false,
        can_administer: false,
        can_invoke: false,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn skill_from_row(row: sqlx::postgres::PgRow) -> SkillDto {
    let package_id = row.try_get::<Option<Uuid>, _>("package_id").ok().flatten();
    SkillDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        owner_email: row.get("owner_email"),
        name: row.get("name"),
        description: row.get("description"),
        content: row.get("content"),
        visibility: row.get("visibility"),
        public_to: row.get("public_to"),
        revision: row.get("revision"),
        content_checksum_sha256: row.get("content_checksum_sha256"),
        package: package_id.map(|id| SkillPackageDto {
            id,
            format_version: u32::try_from(row.get::<i32, _>("package_format_version"))
                .expect("Skill package format version is constrained"),
            size_bytes: u64::try_from(row.get::<i64, _>("package_size_bytes"))
                .expect("Skill package size is constrained"),
            checksum_sha256: row.get("package_checksum_sha256"),
            files: serde_json::from_value(row.get("package_files"))
                .expect("Skill package file manifest is constrained"),
        }),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn runtime_from_row(row: sqlx::postgres::PgRow) -> RuntimeDto {
    let capabilities: Value = row.get("capabilities");
    RuntimeDto {
        id: row.get("id"),
        hostname: row.get("hostname"),
        labels: row.get("labels"),
        engine_version: row.get("engine_version"),
        capabilities: console_runtime_capabilities(&capabilities),
        sandbox_mode: row.get("sandbox_mode"),
        status: row.get("status"),
        last_heartbeat_at: row.get("last_heartbeat_at"),
        credential_rotation_requested_at: row.get("rotation_requested_at"),
    }
}

pub(crate) fn runtime_enrollment_from_row(row: sqlx::postgres::PgRow) -> RuntimeEnrollmentTokenDto {
    RuntimeEnrollmentTokenDto {
        id: row.get("id"),
        created_by: row.get("created_by"),
        expires_at: row.get("expires_at"),
        consumed_at: row.get("consumed_at"),
        consumed_by_runtime_id: row.get("consumed_by_runtime_id"),
        revoked_at: row.get("revoked_at"),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn console_runtime_capabilities(capabilities: &Value) -> Value {
    let Some(source) = capabilities.as_object() else {
        return json!({});
    };
    let mut visible = serde_json::Map::new();
    for key in ["driver", "engine_source", "sandbox_downgrade_reason"] {
        if let Some(value @ Value::String(_)) = source.get(key) {
            visible.insert(key.into(), value.clone());
        }
    }
    for key in [
        "model_proxy",
        "mcp_allowlist",
        "native_session_resume",
        "local_skills",
        "sandbox_downgraded",
    ] {
        if let Some(value @ Value::Bool(_)) = source.get(key) {
            visible.insert(key.into(), value.clone());
        }
    }
    Value::Object(visible)
}

pub(crate) fn automation_from_row(row: sqlx::postgres::PgRow) -> AutomationDto {
    let trigger_type: String = row.get("trigger_type");
    let webhook_token = if trigger_type == "webhook" {
        row.get("webhook_token")
    } else {
        None
    };
    AutomationDto {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        trigger_type,
        prompt: row.get("prompt"),
        schedule: row.get("schedule"),
        webhook_token,
        enabled: row.get("enabled"),
        last_triggered_at: row.get("last_triggered_at"),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn api_key_from_row(row: sqlx::postgres::PgRow) -> ApiKeyDto {
    ApiKeyDto {
        id: row.get("id"),
        name: row.get("name"),
        prefix: row.get("prefix"),
        last_used_at: row.get("last_used_at"),
        expires_at: row.get("expires_at"),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn integration_app_from_row(
    row: sqlx::postgres::PgRow,
    agent_ids: Vec<Uuid>,
) -> IntegrationAppDto {
    IntegrationAppDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        client_id: row.get("client_id"),
        external_platform_id: row.get("external_platform_id"),
        authentication_channel_id: row.get("authentication_channel_id"),
        redirect_uris: row.get("redirect_uris"),
        agent_ids,
        widget_history_enabled: row.get("widget_history_enabled"),
        login_required: row.get("login_required"),
        allowed_origins: serde_json::from_value(row.get("allowed_origins"))
            .expect("Integration App origins are constrained"),
        tool_allowlist: row
            .get::<Option<Value>, _>("tool_allowlist")
            .map(serde_json::from_value)
            .transpose()
            .expect("Integration App tool policy is constrained"),
        client_tool_definitions: serde_json::from_value(
            row.try_get::<Value, _>("client_tool_definitions")
                .unwrap_or_else(|_| json!([])),
        )
        .expect("Integration App Client Tool definitions are constrained"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn integration_session_from_row(row: sqlx::postgres::PgRow) -> IntegrationSessionDto {
    IntegrationSessionDto {
        id: row.get("id"),
        hub_session_id: row.try_get("hub_session_id").unwrap_or_default(),
        agent_id: row.get("agent_id"),
        owner_id: row.try_get("owner_id").unwrap_or_default(),
        platform_id: row.try_get("platform_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        external_identity_id: row.try_get("external_identity_id").unwrap_or_default(),
        external_user_id: row.get("external_user_id"),
        tool_definitions: row.get("tool_definitions"),
        metadata: row.get("metadata"),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn tool_request_from_row(row: sqlx::postgres::PgRow) -> IntegrationToolRequestDto {
    IntegrationToolRequestDto {
        id: row.get("id"),
        session_id: row.get("session_id"),
        hub_session_id: row.try_get("hub_session_id").unwrap_or_default(),
        run_id: row.get("run_id"),
        position: row.try_get("position").unwrap_or_default(),
        tool_name: row.get("tool_name"),
        arguments: row.get("arguments"),
        status: row.get("status"),
        claimed_by_client_instance_id: row
            .try_get("claimed_by_client_instance_id")
            .unwrap_or_default(),
        claimed_at: row.try_get("claimed_at").unwrap_or_default(),
        result_payload: row.get("result_payload"),
        follow_up_run_id: row.get("follow_up_run_id"),
        expires_at: row.get("expires_at"),
        responded_at: row.try_get("responded_at").unwrap_or_default(),
        created_at: row.get("created_at"),
    }
}

pub(crate) fn run_from_row(row: sqlx::postgres::PgRow) -> RunDto {
    RunDto {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        automation_id: row.get("automation_id"),
        integration_session_id: row.get("integration_session_id"),
        parent_run_id: row.get("parent_run_id"),
        runtime_id: row.get("runtime_id"),
        hub_session_id: row.get("hub_session_id"),
        hub_message_id: row.get("hub_message_id"),
        hub_turn_id: row.get("hub_turn_id"),
        session_ownership_generation: row.get("session_ownership_generation"),
        status: row.get("status"),
        initial_message: row.get("initial_message"),
        native_session_id: row.get("native_session_id"),
        work_dir_ref: row.get("work_dir_ref"),
        source: row.get("source"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn hub_message_from_row(row: sqlx::postgres::PgRow) -> HubSessionMessageDto {
    HubSessionMessageDto {
        id: row.get("id"),
        session_id: row.get("session_id"),
        sequence: row.get("sequence"),
        role: row.get("role"),
        message_kind: row.get("message_kind"),
        content: row.get("content"),
        payload: row.get("payload"),
        attachments: Vec::new(),
        delivery_mode: row.get("delivery_mode"),
        delivery_state: row.get("delivery_state"),
        client_message_key: row.get("client_message_key"),
        expected_native_turn_id: row.get("expected_native_turn_id"),
        turn_id: row.get("turn_id"),
        run_id: row.get("run_id"),
        accepted_at: row.get("accepted_at"),
    }
}

pub(crate) fn hub_session_from_row(row: sqlx::postgres::PgRow) -> HubSessionDto {
    let origin = match row.get::<String, _>("origin_kind").as_str() {
        "hub_native" => HubSessionOriginDto::HubNative,
        "public_widget" => HubSessionOriginDto::PublicWidget,
        "external" => HubSessionOriginDto::External {
            platform_id: row.get("origin_platform_id"),
            tenant_id: row.get("origin_tenant_id"),
            external_identity_id: row.get("origin_external_identity_id"),
        },
        _ => unreachable!("hub_sessions origin_kind is constrained"),
    };
    let current_bundle = row
        .get::<Option<i64>, _>("current_bundle_generation")
        .map(|generation| CurrentSessionBundleDto {
            generation,
            object_key: row.get("current_bundle_object_key"),
            checksum_sha256: row.get("current_bundle_checksum_sha256"),
            size_bytes: row.get("current_bundle_size_bytes"),
            history_checkpoint: row.get("current_bundle_history_checkpoint"),
            ownership_generation: row.get("current_bundle_ownership_generation"),
            producing_engine_version: row.get("current_bundle_producing_engine_version"),
            created_at: row.get("current_bundle_created_at"),
        });
    HubSessionDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        agent_id: row.get("agent_id"),
        agent_name: row.get("agent_name"),
        agent_deleted_at: row.get("agent_deleted_at"),
        title: row.get("title"),
        origin_platform_name: row.get("origin_platform_name"),
        origin,
        lifecycle_status: row.get("lifecycle_status"),
        native_session_id: row.get("native_session_id"),
        active_turn_id: row.get("active_turn_id"),
        history_checkpoint: row.get("history_checkpoint"),
        configuration_fingerprint: row.get("configuration_fingerprint"),
        runtime_owner_id: row.get("runtime_owner_id"),
        ownership_generation: row.get("ownership_generation"),
        recovery_error: row.get("recovery_error"),
        current_bundle,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn hub_turn_from_row(row: sqlx::postgres::PgRow) -> HubSessionTurnDto {
    HubSessionTurnDto {
        id: row.get("id"),
        session_id: row.get("session_id"),
        native_turn_id: row.get("native_turn_id"),
        status: row.get("status"),
        configuration_fingerprint: row.get("configuration_fingerprint"),
        ownership_generation: row.get("ownership_generation"),
        started_at: row.get("started_at"),
        ended_at: row.get("ended_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn event_from_row(row: sqlx::postgres::PgRow) -> RunEventDto {
    RunEventDto {
        seq: row.get("seq"),
        event_id: row.get("event_id"),
        run_id: row.get("run_id"),
        event_type: row.get("event_type"),
        role: row.get("role"),
        content: row.get("content"),
        payload: row.get("payload"),
        created_at: row.get("created_at"),
    }
}
