//! 智能体领域。

use super::*;

use crate::accept_session_message_tx;
use crate::ensure_agent_can_start_run_tx;
use crate::ensure_skills_visible_by_user;
use crate::generate_session_title_in_background;
use crate::insert_hub_native_session_tx;
use crate::insert_run_event_tx;
use crate::load_agent_secret_declarations;
use crate::load_managed_skill_ids;
use crate::missing_secret_grants;
use crate::model_connection_scope_name;
use crate::model_upstream_protocol_from_name;
use crate::model_upstream_protocol_name;
use crate::normalize_agent_tool_allowlist;
use crate::record_runtime_session_cleanup_tx;
use crate::replace_agent_secret_declarations_tx;
use crate::validate_model_request_settings;
use crate::AcceptSessionMessage;
use crate::REDACTED_SECRET;
use agent_hub_shared::*;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tracing::warn;
use uuid::Uuid;

pub(crate) fn validate_agent_model_settings(
    mut settings: AgentModelSettings,
    protocol: ModelUpstreamProtocol,
) -> Result<AgentModelSettings, ApiError> {
    let max_database_integer =
        u64::try_from(i64::MAX).expect("i64 maximum is representable as u64");
    for (name, value) in [
        ("context window", settings.context_window_tokens),
        ("automatic compact limit", settings.auto_compact_token_limit),
        (
            "provider request timeout",
            settings.provider_request_timeout_ms,
        ),
        ("stream idle timeout", settings.stream_idle_timeout_ms),
    ] {
        if value.is_some_and(|value| value == 0 || value > max_database_integer) {
            return Err(ApiError::bad_request(format!(
                "Agent Model Settings {name} must be a positive signed 64-bit integer"
            )));
        }
    }
    if settings
        .context_window_tokens
        .zip(settings.auto_compact_token_limit)
        .is_some_and(|(context_window, compact_limit)| compact_limit > context_window)
    {
        return Err(ApiError::bad_request(
            "Agent Model Settings automatic compact limit cannot exceed the context window",
        ));
    }
    if settings.stream_max_retries.is_some_and(|value| value > 100) {
        return Err(ApiError::bad_request(
            "Agent Model Settings retry counts must be between 0 and 100",
        ));
    }
    settings.service_tier = settings.service_tier.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    });
    if settings
        .service_tier
        .as_ref()
        .is_some_and(|value| value.chars().count() > 64 || value.chars().any(char::is_control))
    {
        return Err(ApiError::bad_request(
            "Agent Model Settings service tier must not exceed 64 characters or contain controls",
        ));
    }
    settings.request_settings =
        validate_model_request_settings(protocol, settings.request_settings)?;
    Ok(settings)
}

pub(crate) async fn list_agents(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT a.id, a.owner_id, owner.email AS owner_email, a.name, a.instructions, a.visibility, a.public_to,
                a.runtime_id, a.model_connection_id, a.model_id, a.model_settings,
                a.model_policy, a.sandbox_policy, a.mcp_allowlist, a.tool_allowlist,
                a.created_at, a.updated_at
         FROM agents AS a
         JOIN users AS owner ON owner.id = a.owner_id
         WHERE a.deleted_at IS NULL
           AND (
               a.owner_id = $1
               OR a.visibility = 'public'
               OR (a.visibility = 'public_to' AND $1 = ANY(a.public_to))
               OR owner.role <> 'super_admin'
               OR $2 IN ('admin', 'super_admin')
           )
           AND (a.owner_id = $1 OR a.visibility = 'public'
                OR (a.visibility = 'public_to' AND $1 = ANY(a.public_to))
                OR $2 IN ('admin', 'super_admin'))
         ORDER BY a.created_at DESC",
    )
    .bind(user.id)
    .bind(&user.role)
    .fetch_all(&state.pool)
    .await?;
    let mut agents = Vec::with_capacity(rows.len());
    for row in rows {
        let mut agent = agent_from_row(row);
        if agent.owner_id == user.id || is_admin_role(&user.role) {
            hydrate_agent_configuration(&state.pool, &mut agent).await?;
        }
        agents.push(apply_agent_access(agent, &user));
    }
    Ok(Json(agents))
}

pub(crate) async fn create_agent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(mut req): Json<CreateAgentRequest>,
) -> Result<Json<AgentDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("agent name is required"));
    }
    let visibility = normalize_visibility(&req.visibility)?;
    validate_public_visibility_role(visibility, &user.role)?;
    validate_public_to(&state.pool, visibility, &req.public_to, user.id).await?;
    validate_subagent_definitions(&req.subagents)?;
    let tool_allowlist = normalize_agent_tool_allowlist(&req.tool_allowlist)?;
    let model_policy = json!({ "provider": "hub-proxy" });
    let id = Uuid::new_v4();
    let mut tx = state.pool.begin().await?;
    let model_selection = match req.model_selection {
        Some(selection) => Some(selection),
        None => sqlx::query(
            "SELECT model_connection_id, model_id
             FROM system_default_model_selection WHERE singleton = true",
        )
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| ModelSelectionDto {
            connection_id: row.get("model_connection_id"),
            model_id: row.get("model_id"),
        }),
    };
    let mut model_settings = match req.model_settings {
        Some(settings) => settings,
        None => {
            let mut settings = AgentModelSettings::default();
            if let Some(selection) = model_selection.as_ref() {
                let api_type =
                    load_permitted_model_selection_api_type_tx(&mut tx, user.id, selection).await?;
                settings.request_settings = ModelRequestSettings::for_protocol(api_type);
            }
            settings
        }
    };
    model_settings = validate_agent_model_configuration_tx(
        &mut tx,
        user.id,
        model_selection.as_ref(),
        model_settings,
        &mut req.subagents,
    )
    .await?;
    let model_connection_id = model_selection
        .as_ref()
        .map(|selection| selection.connection_id);
    let model_id = model_selection
        .as_ref()
        .map(|selection| selection.model_id.as_str());
    let model_settings_value = serde_json::to_value(&model_settings)
        .map_err(|_| ApiError::internal("Agent Model Settings could not be encoded"))?;
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility, public_to, model_policy,
              model_connection_id, model_id, model_settings, tool_allowlist)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(id)
    .bind(user.id)
    .bind(req.name.trim())
    .bind(req.instructions.trim())
    .bind(visibility)
    .bind(req.public_to)
    .bind(model_policy)
    .bind(model_connection_id)
    .bind(model_id)
    .bind(model_settings_value)
    .bind(
        serde_json::to_value(tool_allowlist)
            .map_err(|_| ApiError::internal("Agent tool policy could not be encoded"))?,
    )
    .execute(&mut *tx)
    .await?;
    replace_subagents_tx(&mut tx, id, &req.subagents).await?;
    replace_agent_secret_declarations_tx(
        &mut tx,
        id,
        req.secret_declarations.as_deref().unwrap_or_default(),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(load_agent_for_user(&state.pool, id, &user).await?))
}

pub(crate) async fn get_agent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
) -> Result<Json<AgentDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(
        load_agent_for_user(&state.pool, agent_id, &user).await?,
    ))
}

pub(crate) async fn get_agent_model_connection_options(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
) -> Result<Json<ModelConnectionOptionsDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let agent = load_agent_manageable_by_user(&state.pool, agent_id, &user).await?;
    let owner_role: String = sqlx::query_scalar("SELECT role FROM users WHERE id = $1")
        .bind(agent.owner_id)
        .fetch_one(&state.pool)
        .await?;
    // Personal connections stay visible to the Agent owner and super admins;
    // an ordinary admin managing another user's Agent only sees Global
    // connections (mirrors the Model Connection administration boundary).
    let include_owner_personal =
        user.id == agent.owner_id || user.role == "super_admin" || owner_role != "super_admin";
    let rows = sqlx::query(
        "SELECT id, name, api_type, allowed_model_ids, scope, enabled
         FROM model_connections
         WHERE deleted_at IS NULL
           AND (scope = 'global' OR (owner_id = $1 AND $2))
         ORDER BY scope, lower(name), id",
    )
    .bind(agent.owner_id)
    .bind(include_owner_personal)
    .fetch_all(&state.pool)
    .await?;
    let items = rows
        .into_iter()
        .flat_map(|row| {
            let connection_id = row.get("id");
            let connection_name: String = row.get("name");
            let api_type = model_upstream_protocol_from_name(&row.get::<String, _>("api_type"));
            let scope = if row.get::<String, _>("scope") == "global" {
                ModelConnectionScope::Global
            } else {
                ModelConnectionScope::Personal
            };
            let status = if row.get("enabled") {
                ModelConnectionStatus::Enabled
            } else {
                ModelConnectionStatus::Disabled
            };
            row.get::<Vec<String>, _>("allowed_model_ids")
                .into_iter()
                .map(move |model_id| ModelConnectionOptionDto {
                    connection_id,
                    connection_name: connection_name.clone(),
                    model_id,
                    api_type,
                    scope,
                    status,
                })
        })
        .collect();
    let system_default = sqlx::query(
        "SELECT model_connection_id, model_id
         FROM system_default_model_selection WHERE singleton = true",
    )
    .fetch_optional(&state.pool)
    .await?
    .map(|row| ModelSelectionDto {
        connection_id: row.get("model_connection_id"),
        model_id: row.get("model_id"),
    });
    Ok(Json(ModelConnectionOptionsDto {
        items,
        system_default,
    }))
}

pub(crate) async fn update_agent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
    Json(mut req): Json<UpdateAgentRequest>,
) -> Result<Json<AgentDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let existing_agent = load_agent_manageable_by_user(&state.pool, agent_id, &user).await?;
    let secret_declarations = req
        .secret_declarations
        .clone()
        .unwrap_or_else(|| existing_agent.secret_declarations.clone());
    req.mcp_allowlist = merge_mcp_secrets(&existing_agent.mcp_allowlist, &req.mcp_allowlist);
    req.tool_allowlist = normalize_agent_tool_allowlist(&req.tool_allowlist)?;
    validate_agent_payload(&req)?;
    if let Some(runtime_id) = req.runtime_id {
        ensure_runtime_online(&state.pool, runtime_id).await?;
    }
    ensure_skills_visible_by_user(&state.pool, &req.managed_skill_ids, existing_agent.owner_id)
        .await?;
    validate_subagent_definitions(&req.subagents)?;
    let execution_configuration_changed =
        agent_execution_configuration_changed(&existing_agent, &req);

    let visibility = normalize_visibility(&req.visibility)?;
    validate_public_visibility_role(visibility, &user.role)?;
    validate_public_to(
        &state.pool,
        visibility,
        &req.public_to,
        existing_agent.owner_id,
    )
    .await?;
    let mut tx = state.pool.begin().await?;
    req.model_settings = validate_agent_model_configuration_tx(
        &mut tx,
        existing_agent.owner_id,
        req.model_selection.as_ref(),
        req.model_settings,
        &mut req.subagents,
    )
    .await?;
    if user.id != existing_agent.owner_id && user.role != "super_admin" {
        enforce_admin_agent_model_selection_tx(
            &mut tx,
            &existing_agent,
            req.model_selection.as_ref(),
            &req.subagents,
        )
        .await?;
    }
    let model_connection_id = req
        .model_selection
        .as_ref()
        .map(|selection| selection.connection_id);
    let model_id = req
        .model_selection
        .as_ref()
        .map(|selection| selection.model_id.as_str());
    let model_settings_value = serde_json::to_value(&req.model_settings)
        .map_err(|_| ApiError::internal("Agent Model Settings could not be encoded"))?;
    let updated = sqlx::query(
        "UPDATE agents
         SET name = $1, instructions = $2, visibility = $3, public_to = $4, runtime_id = $5,
             model_connection_id = $6, model_id = $7, model_settings = $8,
             model_policy = $9, sandbox_policy = $10, mcp_allowlist = $11,
             tool_allowlist = $12,
             execution_config_revision = execution_config_revision
                 + CASE WHEN $13 THEN 1 ELSE 0 END,
             updated_at = now()
         WHERE id = $14 AND deleted_at IS NULL
         ",
    )
    .bind(req.name.trim())
    .bind(req.instructions.trim())
    .bind(visibility)
    .bind(req.public_to)
    .bind(req.runtime_id)
    .bind(model_connection_id)
    .bind(model_id)
    .bind(model_settings_value)
    .bind(req.model_policy)
    .bind(req.sandbox_policy)
    .bind(req.mcp_allowlist)
    .bind(
        serde_json::to_value(&req.tool_allowlist)
            .map_err(|_| ApiError::internal("Agent tool policy could not be encoded"))?,
    )
    .bind(execution_configuration_changed)
    .bind(agent_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(ApiError::not_found("agent not found"));
    }
    if execution_configuration_changed {
        sqlx::query(
            "UPDATE hub_sessions AS sessions
             SET configuration_refresh_revision = GREATEST(
                     sessions.configuration_refresh_revision,
                     agents.execution_config_revision
                 )
             FROM agents
             WHERE sessions.agent_id = agents.id
               AND agents.id = $1
               AND sessions.runtime_owner_id IS NOT NULL
               AND sessions.lifecycle_status IN ('restoring', 'online')",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM agent_skills WHERE agent_id = $1")
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
    for skill_id in &req.managed_skill_ids {
        sqlx::query(
            "INSERT INTO agent_skills (agent_id, skill_id)
             VALUES ($1, $2)
             ON CONFLICT DO NOTHING",
        )
        .bind(agent_id)
        .bind(skill_id)
        .execute(&mut *tx)
        .await?;
    }
    replace_subagents_tx(&mut tx, agent_id, &req.subagents).await?;
    replace_agent_secret_declarations_tx(&mut tx, agent_id, &secret_declarations).await?;
    tx.commit().await?;
    Ok(Json(
        load_agent_for_user(&state.pool, agent_id, &user).await?,
    ))
}

pub(crate) async fn delete_agent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let agent = sqlx::query(
        "SELECT agents.owner_id, agents.deleted_at
         FROM agents
         WHERE agents.id = $1
         FOR UPDATE",
    )
    .bind(agent_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(agent) = agent else {
        return Err(ApiError::not_found("agent not found"));
    };
    let owner_id: Uuid = agent.get("owner_id");
    if owner_id != user.id && !is_admin_role(&user.role) {
        return Err(ApiError::forbidden(
            "agent management permission is required",
        ));
    }
    if agent
        .get::<Option<DateTime<Utc>>, _>("deleted_at")
        .is_none()
    {
        sqlx::query(
            "INSERT INTO session_bundle_deletion_queue (object_key, agent_id, session_id)
             SELECT current_bundle_object_key, agent_id, id
             FROM hub_sessions
             WHERE agent_id = $1 AND current_bundle_object_key IS NOT NULL
             ON CONFLICT (object_key) DO NOTHING",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM integration_app_agents WHERE agent_id = $1")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM agent_skills WHERE agent_id = $1")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM subagent_definitions WHERE agent_id = $1")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM automations WHERE agent_id = $1")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM embed_sessions WHERE agent_id = $1")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE integration_tool_requests AS requests
             SET status = 'cancelled', responded_at = COALESCE(responded_at, now())
             FROM integration_sessions AS integration
             WHERE requests.session_id = integration.id
               AND integration.agent_id = $1
               AND requests.status <> 'completed'",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        let interrupted = sqlx::query(
            "UPDATE runs
             SET status = 'failed', runtime_id = NULL,
                 model_proxy_token_hash = NULL, work_dir_ref = NULL, updated_at = now()
             WHERE agent_id = $1 AND status IN ('pending', 'running', 'waiting_tool')
             RETURNING id",
        )
        .bind(agent_id)
        .fetch_all(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE runs
             SET model_proxy_token_hash = NULL, work_dir_ref = NULL, updated_at = now()
             WHERE agent_id = $1",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        for row in interrupted {
            insert_run_event_tx(
                &mut tx,
                row.get("id"),
                "status".into(),
                None,
                Some("failed".into()),
                json!({ "status": "failed", "reason": "agent deleted" }),
            )
            .await?;
        }
        sqlx::query(
            "UPDATE hub_session_turns AS turns
             SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
             FROM hub_sessions AS sessions
             WHERE turns.session_id = sessions.id
               AND sessions.agent_id = $1
               AND turns.status NOT IN ('completed', 'failed', 'interrupted')",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE hub_session_messages AS messages
             SET delivery_state = 'failed'
             FROM hub_sessions AS sessions
             WHERE messages.session_id = sessions.id
               AND sessions.agent_id = $1
               AND messages.delivery_state IN ('queued', 'deferred', 'delivering')",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        // Runtime writes lock Runtime before Session; keep the same order before
        // the cleanup obligation foreign key and owned Session row locks below.
        sqlx::query(
            "SELECT runtimes.id
             FROM runtimes
             JOIN hub_sessions AS sessions ON sessions.runtime_owner_id = runtimes.id
             WHERE sessions.agent_id = $1
             ORDER BY runtimes.id
             FOR UPDATE OF runtimes",
        )
        .bind(agent_id)
        .fetch_all(&mut *tx)
        .await?;
        let owned_sessions = sqlx::query(
            "SELECT id, runtime_owner_id, ownership_generation
             FROM hub_sessions
             WHERE agent_id = $1 AND runtime_owner_id IS NOT NULL
             ORDER BY id FOR UPDATE",
        )
        .bind(agent_id)
        .fetch_all(&mut *tx)
        .await?;
        for session in owned_sessions {
            record_runtime_session_cleanup_tx(
                &mut tx,
                session.get("runtime_owner_id"),
                session.get("id"),
                session.get("ownership_generation"),
                None,
            )
            .await?;
        }
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'historical', active_turn_id = NULL,
                 configuration_fingerprint = NULL,
                 runtime_owner_id = NULL, ownership_generation = ownership_generation + 1,
                 recovery_error = NULL,
                 current_bundle_generation = NULL, current_bundle_object_key = NULL,
                 current_bundle_checksum_sha256 = NULL, current_bundle_size_bytes = NULL,
                 current_bundle_history_checkpoint = NULL,
                 current_bundle_ownership_generation = NULL,
                 current_bundle_producing_engine_version = NULL,
                 current_bundle_created_at = NULL, current_bundle_runtime_id = NULL,
                 current_bundle_checkpoint_attempt_id = NULL,
                 saving_history_checkpoint = NULL, saving_ownership_generation = NULL,
                 saving_reason = NULL, saving_checkpoint_attempt_id = NULL,
                 last_checkpoint_attempt_id = NULL,
                 last_checkpoint_ownership_generation = NULL,
                 last_checkpoint_disposition = NULL,
                 last_checkpoint_has_queued_work = NULL
             WHERE agent_id = $1",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE agents
             SET instructions = '', visibility = 'private', public_to = '{}',
                 runtime_id = NULL, model_policy = '{}'::jsonb,
                 model_connection_id = NULL, model_id = NULL,
                 model_settings = '{\"reasoning_effort\":\"default\",\"reasoning_summary\":\"default\",\"verbosity\":\"default\",\"context_window_tokens\":null,\"auto_compact_token_limit\":null,\"reasoning_summary_support\":\"auto\",\"service_tier\":null,\"provider_request_timeout_ms\":null,\"stream_max_retries\":null,\"stream_idle_timeout_ms\":null,\"request_settings\":{\"protocol\":\"openai_responses\"}}'::jsonb,
                 sandbox_policy = '{}'::jsonb, mcp_allowlist = '[]'::jsonb,
                 execution_config_revision = execution_config_revision + 1,
                 deleted_at = now(), updated_at = now()
             WHERE id = $1 AND deleted_at IS NULL",
        )
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    delete_queued_agent_bundles(&state, agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn delete_queued_agent_bundles(
    state: &AppState,
    agent_id: Uuid,
) -> Result<(), ApiError> {
    let object_keys = sqlx::query_scalar::<_, String>(
        "SELECT object_key
         FROM session_bundle_deletion_queue
         WHERE agent_id = $1
         ORDER BY created_at, object_key",
    )
    .bind(agent_id)
    .fetch_all(&state.pool)
    .await?;
    if object_keys.is_empty() {
        return Ok(());
    }
    let store = state.session_bundle_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("Session Bundle object storage is not configured")
    })?;
    let mut first_error = None;
    for object_key in object_keys {
        match store.delete(&object_key).await {
            Ok(()) => {
                sqlx::query(
                    "DELETE FROM session_bundle_deletion_queue
                     WHERE object_key = $1 AND agent_id = $2",
                )
                .bind(&object_key)
                .bind(agent_id)
                .execute(&state.pool)
                .await?;
            }
            Err(error) => {
                sqlx::query(
                    "UPDATE session_bundle_deletion_queue
                     SET attempts = attempts + 1, last_error = $1, updated_at = now()
                     WHERE object_key = $2 AND agent_id = $3",
                )
                .bind("object store delete failed")
                .bind(&object_key)
                .bind(agent_id)
                .execute(&state.pool)
                .await?;
                warn!(agent_id = %agent_id, object_key = %object_key, error = %error,
                    "failed to delete historical Session Bundle object");
                first_error.get_or_insert_with(|| {
                    ApiError::bad_gateway("failed to delete one or more Session Bundle objects")
                });
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) async fn list_agent_runs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
) -> Result<Json<Vec<RunDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    load_agent_for_user(&state.pool, agent_id, &user).await?;
    let rows = sqlx::query(
        "SELECT runs.id, runs.agent_id, runs.automation_id, runs.integration_session_id,
                runs.parent_run_id, runs.runtime_id, runs.hub_session_id,
                runs.hub_message_id, runs.hub_turn_id,
                runs.session_ownership_generation, runs.status, runs.initial_message,
                runs.native_session_id, runs.work_dir_ref, runs.source, runs.created_at,
                runs.updated_at
         FROM runs
         JOIN users AS run_owner ON run_owner.id = runs.owner_id
         WHERE runs.agent_id = $1
           AND (runs.owner_id = $2 OR $3 IN ('admin', 'super_admin'))
           AND (runs.owner_id = $2 OR run_owner.role <> 'super_admin'
                OR $3 IN ('admin', 'super_admin'))
         ORDER BY runs.created_at DESC LIMIT 50",
    )
    .bind(agent_id)
    .bind(user.id)
    .bind(&user.role)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(run_from_row).collect()))
}

pub(crate) async fn create_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(agent_id): Path<Uuid>,
    Json(req): Json<CreateRunRequest>,
) -> Result<Json<RunDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let requested_origin_kind: Option<String> = match req.hub_session_id {
        Some(session_id) => {
            sqlx::query_scalar(
                "SELECT origin_kind
             FROM hub_sessions
             WHERE id = $1 AND owner_id = $2 AND agent_id = $3",
            )
            .bind(session_id)
            .bind(user.id)
            .bind(agent_id)
            .fetch_optional(&state.pool)
            .await?
        }
        None => match req.parent_run_id {
            Some(parent_run_id) => {
                sqlx::query_scalar(
                    "SELECT sessions.origin_kind
                 FROM runs
                 JOIN hub_sessions AS sessions ON sessions.id = runs.hub_session_id
                 WHERE runs.id = $1 AND runs.owner_id = $2 AND runs.agent_id = $3
                   AND sessions.owner_id = $2 AND sessions.agent_id = $3",
                )
                .bind(parent_run_id)
                .bind(user.id)
                .bind(agent_id)
                .fetch_optional(&state.pool)
                .await?
            }
            None => None,
        },
    };
    if requested_origin_kind
        .as_deref()
        .is_some_and(|origin_kind| origin_kind != "hub_native")
    {
        return Err(ApiError::conflict(
            "External Sessions are read-only in the Hub console",
        ));
    }
    let agent = load_agent_for_user(&state.pool, agent_id, &user).await?;
    let missing_grants = missing_secret_grants(&state.pool, user.id, agent_id).await?;
    if !missing_grants.is_empty() {
        return Err(ApiError::requires_secret_grants(missing_grants));
    }
    let mut tx = state.pool.begin().await?;
    let existing_session_id = match req.hub_session_id {
        Some(session_id) => Some(session_id),
        None => match req.parent_run_id {
            Some(parent_run_id) => Some(
                sqlx::query_scalar(
                    "SELECT hub_session_id
                 FROM runs
                 WHERE id = $1 AND owner_id = $2 AND agent_id = $3",
                )
                .bind(parent_run_id)
                .bind(user.id)
                .bind(agent.id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(ApiError::bad_request("resume parent run is not available"))?,
            ),
            None => None,
        },
    };
    let is_new_session = existing_session_id.is_none();
    let first_message = req.message.clone();
    let session_id = if let Some(session_id) = existing_session_id {
        let origin_kind: String = sqlx::query_scalar(
            "SELECT origin_kind
             FROM hub_sessions
             WHERE id = $1 AND owner_id = $2 AND agent_id = $3",
        )
        .bind(session_id)
        .bind(user.id)
        .bind(agent.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::bad_request("Session is not available"))?;
        if origin_kind != "hub_native" {
            return Err(ApiError::conflict(
                "External Sessions are read-only in the Hub console",
            ));
        }
        ensure_agent_can_start_run_tx(&mut tx, agent.id, user.id).await?;
        session_id
    } else {
        ensure_agent_can_start_run_tx(&mut tx, agent.id, user.id).await?;
        insert_hub_native_session_tx(&mut tx, user.id, agent.id).await?
    };
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id,
            agent_id: agent.id,
            owner_id: user.id,
            content: req.message,
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: req.client_message_key,
            source: "console".into(),
            automation_id: None,
            integration_session_id: None,
            parent_run_id: req.parent_run_id,
            continuation_turn_id: None,
            model_subject_type: "user".into(),
            model_subject_user_id: Some(user.id),
            model_source_integration_app_id: None,
            external_user_context: None,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    let run = accepted
        .run
        .ok_or(ApiError::internal("console message did not schedule a run"))?;
    tx.commit().await?;
    if is_new_session {
        let background_state = state.clone();
        let background_session_id = session_id;
        let background_agent_id = agent.id;
        let background_user_id = user.id;
        let background_message = first_message;
        tokio::spawn(async move {
            generate_session_title_in_background(
                background_state,
                background_session_id,
                background_agent_id,
                background_user_id,
                background_message,
            )
            .await;
        });
    }
    Ok(Json(run))
}

pub(crate) fn normalize_visibility(visibility: &str) -> Result<&'static str, ApiError> {
    match visibility.trim() {
        "private" => Ok("private"),
        "public_to" => Ok("public_to"),
        "public" => Ok("public"),
        _ => Err(ApiError::bad_request("unsupported visibility")),
    }
}

pub(crate) fn validate_public_visibility_role(
    visibility: &str,
    role: &str,
) -> Result<(), ApiError> {
    if visibility == "public" && !is_admin_role(role) {
        return Err(ApiError::forbidden(
            "administrator permission is required for public agents",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_public_to(
    pool: &PgPool,
    visibility: &str,
    public_to: &[Uuid],
    owner_id: Uuid,
) -> Result<(), ApiError> {
    if visibility != "public_to" && !public_to.is_empty() {
        return Err(ApiError::bad_request(
            "public_to users require public_to visibility",
        ));
    }
    if visibility == "public_to" && public_to.is_empty() {
        return Err(ApiError::bad_request(
            "public_to visibility requires at least one user",
        ));
    }
    let unique = public_to.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != public_to.len() || unique.contains(&owner_id) {
        return Err(ApiError::bad_request(
            "public_to users must be unique and exclude the owner",
        ));
    }
    if unique.is_empty() {
        return Ok(());
    }
    let ids = unique.into_iter().collect::<Vec<_>>();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE id = ANY($1)")
        .bind(&ids)
        .fetch_one(pool)
        .await?;
    if count != ids.len() as i64 {
        return Err(ApiError::bad_request("public_to user does not exist"));
    }
    Ok(())
}

pub(crate) fn validate_agent_payload(req: &UpdateAgentRequest) -> Result<(), ApiError> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("agent name is required"));
    }
    normalize_agent_tool_allowlist(&req.tool_allowlist)?;
    let Some(model_policy) = req.model_policy.as_object() else {
        return Err(ApiError::bad_request("model policy must be a JSON object"));
    };
    match model_policy
        .get("provider")
        .and_then(|value| value.as_str())
    {
        Some("hub-proxy") => {}
        Some(_) => return Err(ApiError::bad_request("unsupported model provider")),
        None => return Err(ApiError::bad_request("model provider is required")),
    }
    if let Some(base_url) = model_policy.get("base_url") {
        if base_url.as_str().is_none_or(str::is_empty) {
            return Err(ApiError::bad_request("model base_url must be a string"));
        }
    }
    let Some(sandbox_policy) = req.sandbox_policy.as_object() else {
        return Err(ApiError::bad_request(
            "sandbox policy must be a JSON object",
        ));
    };
    match sandbox_policy.get("mode").and_then(|value| value.as_str()) {
        Some("workspace-write") | Some("read-only") => {}
        Some(_) => return Err(ApiError::bad_request("unsupported sandbox mode")),
        None => return Err(ApiError::bad_request("sandbox mode is required")),
    }
    if !sandbox_policy
        .get("network_access")
        .is_some_and(|value| value.is_boolean())
    {
        return Err(ApiError::bad_request(
            "sandbox network_access must be a boolean",
        ));
    }
    let Some(mcp_servers) = req.mcp_allowlist.as_array() else {
        return Err(ApiError::bad_request("MCP allowlist must be a JSON array"));
    };
    let mut mcp_names = BTreeSet::new();
    for server in mcp_servers {
        let Some(server) = server.as_object() else {
            return Err(ApiError::bad_request("MCP entries must be JSON objects"));
        };
        for key in server.keys() {
            if !matches!(key.as_str(), "name" | "command" | "args" | "secrets") {
                return Err(ApiError::bad_request(
                    "MCP entries only support name, command, args, and secrets",
                ));
            }
        }
        let Some(name) = server
            .get("name")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Err(ApiError::bad_request("MCP name is required"));
        };
        if !mcp_names.insert(name.to_owned()) {
            return Err(ApiError::bad_request("MCP names must be unique"));
        }
        if server
            .get("command")
            .is_some_and(|value| value.as_str().is_none_or(str::is_empty))
        {
            return Err(ApiError::bad_request("MCP command must be a string"));
        }
        if let Some(args) = server.get("args") {
            let Some(args) = args.as_array() else {
                return Err(ApiError::bad_request("MCP args must be a JSON array"));
            };
            if args.iter().any(|value| value.as_str().is_none()) {
                return Err(ApiError::bad_request("MCP args must be strings"));
            }
        }
        if let Some(secrets) = server.get("secrets") {
            let Some(secrets) = secrets.as_object() else {
                return Err(ApiError::bad_request("MCP secrets must be a JSON object"));
            };
            for value in secrets.values() {
                let Some(value) = value.as_str() else {
                    return Err(ApiError::bad_request("MCP secret values must be strings"));
                };
                if value == REDACTED_SECRET {
                    return Err(ApiError::bad_request(
                        "MCP redacted secret cannot be saved without an existing value",
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn load_agent_for_user(
    pool: &PgPool,
    agent_id: Uuid,
    user: &UserDto,
) -> Result<AgentDto, ApiError> {
    let row = sqlx::query(
        "SELECT a.id, a.owner_id, owner.email AS owner_email, a.name, a.instructions, a.visibility, a.public_to,
                a.runtime_id, a.model_connection_id, a.model_id, a.model_settings,
                a.model_policy, a.sandbox_policy, a.mcp_allowlist, a.tool_allowlist,
                a.created_at, a.updated_at
         FROM agents AS a
         JOIN users AS owner ON owner.id = a.owner_id
         WHERE a.id = $1 AND a.deleted_at IS NULL
           AND (
               a.owner_id = $2
               OR a.visibility = 'public'
               OR (a.visibility = 'public_to' AND $2 = ANY(a.public_to))
               OR owner.role <> 'super_admin'
               OR $3 IN ('admin', 'super_admin')
           )
           AND (a.owner_id = $2 OR a.visibility = 'public'
                OR (a.visibility = 'public_to' AND $2 = ANY(a.public_to))
                OR $3 IN ('admin', 'super_admin'))",
    )
    .bind(agent_id)
    .bind(user.id)
    .bind(&user.role)
    .fetch_optional(pool)
    .await?;
    let row = row.ok_or(ApiError::not_found("agent not found"))?;
    let mut agent = agent_from_row(row);
    if agent.owner_id == user.id || is_admin_role(&user.role) {
        hydrate_agent_configuration(pool, &mut agent).await?;
    }
    Ok(apply_agent_access(agent, user))
}

pub(crate) async fn load_agent_owned_by_user(
    pool: &PgPool,
    agent_id: Uuid,
    user_id: Uuid,
) -> Result<AgentDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, owner_id, name, instructions, visibility, public_to, runtime_id,
                model_connection_id, model_id, model_settings,
                model_policy, sandbox_policy, mcp_allowlist, tool_allowlist, created_at, updated_at
         FROM agents
         WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL",
    )
    .bind(agent_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    let row = row.ok_or(ApiError::not_found("agent not found"))?;
    let mut agent = agent_from_row(row);
    hydrate_agent_configuration(pool, &mut agent).await?;
    Ok(agent)
}

pub(crate) async fn load_agent_manageable_by_user(
    pool: &PgPool,
    agent_id: Uuid,
    user: &UserDto,
) -> Result<AgentDto, ApiError> {
    let row = sqlx::query(
        "SELECT a.id, a.owner_id,
                (SELECT email FROM users WHERE id = a.owner_id) AS owner_email,
                a.name, a.instructions, a.visibility, a.public_to,
                a.runtime_id, a.model_connection_id, a.model_id, a.model_settings,
                a.model_policy, a.sandbox_policy, a.mcp_allowlist, a.tool_allowlist,
                a.created_at, a.updated_at
         FROM agents AS a
         WHERE a.id = $1 AND a.deleted_at IS NULL
           AND (a.owner_id = $2 OR $3 IN ('admin', 'super_admin'))",
    )
    .bind(agent_id)
    .bind(user.id)
    .bind(&user.role)
    .fetch_optional(pool)
    .await?;
    let row = row.ok_or(ApiError::not_found("agent not found"))?;
    let mut agent = agent_from_row(row);
    hydrate_agent_configuration(pool, &mut agent).await?;
    Ok(agent)
}

pub(crate) async fn ensure_runtime_online(pool: &PgPool, runtime_id: Uuid) -> Result<(), ApiError> {
    let exists: Option<(Uuid,)> =
        sqlx::query_as("SELECT id FROM runtimes WHERE id = $1 AND status = 'online'")
            .bind(runtime_id)
            .fetch_optional(pool)
            .await?;
    exists
        .map(|_| ())
        .ok_or(ApiError::bad_request("runtime is not online"))
}

pub(crate) fn validate_subagent_definitions(
    definitions: &[SubagentDefinition],
) -> Result<(), ApiError> {
    if definitions.len() > 32 {
        return Err(ApiError::bad_request(
            "an Agent supports at most 32 Subagents",
        ));
    }
    let mut names = BTreeSet::new();
    for definition in definitions {
        let name = definition.name.trim();
        if name.is_empty()
            || name.len() > 64
            || !name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err(ApiError::bad_request(
                "Subagent name must use 1 to 64 letters, digits, hyphens, or underscores",
            ));
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(ApiError::bad_request("Subagent names must be unique"));
        }
        if name.eq_ignore_ascii_case("main") {
            return Err(ApiError::bad_request("Subagent name 'main' is reserved"));
        }
        let description = definition.description.trim();
        if description.is_empty()
            || description.chars().count() > 512
            || description.chars().any(char::is_control)
        {
            return Err(ApiError::bad_request(
                "Subagent description must be 1 to 512 characters",
            ));
        }
        if definition.developer_instructions.trim().is_empty()
            || definition.developer_instructions.len() > 100_000
        {
            return Err(ApiError::bad_request(
                "Subagent developer instructions are required and must not exceed 100000 bytes",
            ));
        }
        if definition.enabled {
            if definition.disabled_reason.is_some() {
                return Err(ApiError::bad_request(
                    "enabled Subagents cannot have a disabled reason",
                ));
            }
        } else if definition.model_selection.is_some()
            || !matches!(
                definition.disabled_reason.as_deref(),
                Some("model_connection_deleted" | "model_selection_removed")
            )
        {
            return Err(ApiError::bad_request(
                "disabled Subagents must retain the deleted-model reason without an override",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn load_permitted_model_selection_api_type_tx(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
    selection: &ModelSelectionDto,
) -> Result<ModelUpstreamProtocol, ApiError> {
    let api_type: String = sqlx::query_scalar(
        "SELECT api_type FROM model_connections
         WHERE id = $1 AND enabled = true AND deleted_at IS NULL
           AND $2 = ANY(allowed_model_ids)
           AND (scope = 'global' OR owner_id = $3)
         FOR SHARE",
    )
    .bind(selection.connection_id)
    .bind(&selection.model_id)
    .bind(owner_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| {
        ApiError::bad_request(
            "Agent model selection must be an allowed model on an enabled Global or Agent-owner Personal Model API Connection",
        )
    })?;
    Ok(model_upstream_protocol_from_name(&api_type))
}

pub(crate) fn apply_enum_model_setting_override<T: Clone + Default>(
    value: &mut T,
    setting_override: &ModelSettingOverride<T>,
) {
    match setting_override {
        ModelSettingOverride::Inherit => {}
        ModelSettingOverride::Automatic => *value = T::default(),
        ModelSettingOverride::Value(overridden) => *value = overridden.clone(),
    }
}

pub(crate) fn apply_optional_model_setting_override<T: Clone>(
    value: &mut Option<T>,
    setting_override: &ModelSettingOverride<T>,
) {
    match setting_override {
        ModelSettingOverride::Inherit => {}
        ModelSettingOverride::Automatic => *value = None,
        ModelSettingOverride::Value(overridden) => *value = Some(overridden.clone()),
    }
}

pub(crate) fn effective_subagent_model_settings(
    parent: &AgentModelSettings,
    overrides: &AgentModelSettingsOverride,
    protocol: ModelUpstreamProtocol,
    selection_protocol_changed: bool,
) -> Result<AgentModelSettings, ApiError> {
    let mut effective = parent.clone();
    apply_enum_model_setting_override(&mut effective.reasoning_effort, &overrides.reasoning_effort);
    apply_enum_model_setting_override(
        &mut effective.reasoning_summary,
        &overrides.reasoning_summary,
    );
    apply_enum_model_setting_override(&mut effective.verbosity, &overrides.verbosity);
    apply_optional_model_setting_override(
        &mut effective.context_window_tokens,
        &overrides.context_window_tokens,
    );
    apply_optional_model_setting_override(
        &mut effective.auto_compact_token_limit,
        &overrides.auto_compact_token_limit,
    );
    apply_enum_model_setting_override(
        &mut effective.reasoning_summary_support,
        &overrides.reasoning_summary_support,
    );
    apply_optional_model_setting_override(&mut effective.service_tier, &overrides.service_tier);
    apply_optional_model_setting_override(
        &mut effective.provider_request_timeout_ms,
        &overrides.provider_request_timeout_ms,
    );
    apply_optional_model_setting_override(
        &mut effective.stream_max_retries,
        &overrides.stream_max_retries,
    );
    apply_optional_model_setting_override(
        &mut effective.stream_idle_timeout_ms,
        &overrides.stream_idle_timeout_ms,
    );
    match &overrides.request_settings {
        ModelSettingOverride::Inherit if selection_protocol_changed => {
            effective.request_settings = ModelRequestSettings::for_protocol(protocol);
        }
        ModelSettingOverride::Inherit => {}
        ModelSettingOverride::Automatic => {
            effective.request_settings = ModelRequestSettings::for_protocol(protocol);
        }
        ModelSettingOverride::Value(settings) => effective.request_settings = settings.clone(),
    }
    validate_agent_model_settings(effective, protocol)
}

pub(crate) async fn check_admin_agent_model_selection_tx(
    tx: &mut Transaction<'_, Postgres>,
    retained: &[(Uuid, String)],
    selection: Option<&ModelSelectionDto>,
) -> Result<(), ApiError> {
    let Some(selection) = selection else {
        return Ok(());
    };
    if retained.iter().any(|(connection_id, model_id)| {
        *connection_id == selection.connection_id && model_id == &selection.model_id
    }) {
        return Ok(());
    }
    let global: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM model_connections
             WHERE id = $1 AND scope = 'global' AND enabled = true AND deleted_at IS NULL
               AND $2 = ANY(allowed_model_ids)
         )",
    )
    .bind(selection.connection_id)
    .bind(&selection.model_id)
    .fetch_one(&mut **tx)
    .await?;
    if global {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "admin model changes are limited to Global connections",
        ))
    }
}

pub(crate) async fn enforce_admin_agent_model_selection_tx(
    tx: &mut Transaction<'_, Postgres>,
    existing: &AgentDto,
    requested: Option<&ModelSelectionDto>,
    subagents: &[SubagentDefinition],
) -> Result<(), ApiError> {
    // An ordinary admin may retain the Agent owner's existing Personal model
    // selections, but may not point the Agent at a different Personal
    // connection: the owner's Personal connections stay owner/super-only.
    let mut retained = Vec::new();
    if let Some(selection) = existing.model_selection.as_ref() {
        retained.push((selection.connection_id, selection.model_id.clone()));
    }
    for subagent in &existing.subagents {
        if let Some(selection) = subagent.model_selection.as_ref() {
            retained.push((selection.connection_id, selection.model_id.clone()));
        }
    }
    check_admin_agent_model_selection_tx(tx, &retained, requested).await?;
    for subagent in subagents.iter().filter(|subagent| subagent.enabled) {
        check_admin_agent_model_selection_tx(tx, &retained, subagent.model_selection.as_ref())
            .await?;
    }
    Ok(())
}

pub(crate) async fn validate_agent_model_configuration_tx(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
    model_selection: Option<&ModelSelectionDto>,
    model_settings: AgentModelSettings,
    subagents: &mut [SubagentDefinition],
) -> Result<AgentModelSettings, ApiError> {
    let parent_protocol = match model_selection {
        Some(selection) => {
            load_permitted_model_selection_api_type_tx(tx, owner_id, selection).await?
        }
        None => model_settings.request_settings.protocol(),
    };
    let model_settings = validate_agent_model_settings(model_settings, parent_protocol)?;
    for subagent in subagents.iter_mut().filter(|subagent| subagent.enabled) {
        let protocol = match subagent.model_selection.as_ref() {
            Some(selection) => {
                load_permitted_model_selection_api_type_tx(tx, owner_id, selection).await?
            }
            None => parent_protocol,
        };
        let selection_protocol_changed =
            subagent.model_selection.is_some() && protocol != parent_protocol;
        if let ModelSettingOverride::Value(service_tier) =
            &mut subagent.model_settings_override.service_tier
        {
            *service_tier = service_tier.trim().to_owned();
        }
        effective_subagent_model_settings(
            &model_settings,
            &subagent.model_settings_override,
            protocol,
            selection_protocol_changed,
        )?;
    }
    Ok(model_settings)
}

pub(crate) async fn replace_subagents_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    definitions: &[SubagentDefinition],
) -> Result<(), ApiError> {
    let existing = sqlx::query(
        "SELECT id, lower(btrim(name)) AS normalized_name
         FROM subagent_definitions
         WHERE agent_id = $1 FOR UPDATE",
    )
    .bind(agent_id)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>("normalized_name"),
            row.get::<Uuid, _>("id"),
        )
    })
    .collect::<BTreeMap<_, _>>();
    sqlx::query("DELETE FROM subagent_definitions WHERE agent_id = $1")
        .bind(agent_id)
        .execute(&mut **tx)
        .await?;
    for definition in definitions {
        let id = existing
            .get(&definition.name.trim().to_ascii_lowercase())
            .copied()
            .unwrap_or_else(Uuid::new_v4);
        sqlx::query(
            "INSERT INTO subagent_definitions
                 (id, agent_id, name, description, developer_instructions,
                  model_connection_id, model_id, model_settings_override,
                  enabled, disabled_reason)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(id)
        .bind(agent_id)
        .bind(definition.name.trim())
        .bind(definition.description.trim())
        .bind(definition.developer_instructions.trim())
        .bind(
            definition
                .model_selection
                .as_ref()
                .map(|selection| selection.connection_id),
        )
        .bind(
            definition
                .model_selection
                .as_ref()
                .map(|selection| selection.model_id.as_str()),
        )
        .bind(
            serde_json::to_value(&definition.model_settings_override)
                .map_err(|_| ApiError::internal("subagent Model Settings could not be encoded"))?,
        )
        .bind(definition.enabled)
        .bind(definition.disabled_reason.as_deref())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) fn subagent_from_row(row: sqlx::postgres::PgRow) -> SubagentDefinition {
    let connection_id: Option<Uuid> = row.get("model_connection_id");
    let model_id: Option<String> = row.get("model_id");
    SubagentDefinition {
        name: row.get("name"),
        description: row.get("description"),
        developer_instructions: row.get("developer_instructions"),
        model_selection: connection_id
            .zip(model_id)
            .map(|(connection_id, model_id)| ModelSelectionDto {
                connection_id,
                model_id,
            }),
        model_settings_override: serde_json::from_value(row.get("model_settings_override"))
            .expect("subagent Model Settings are constrained"),
        enabled: row.get("enabled"),
        disabled_reason: row.get("disabled_reason"),
    }
}

pub(crate) async fn load_subagents(
    pool: &PgPool,
    agent_id: Uuid,
) -> Result<Vec<SubagentDefinition>, ApiError> {
    let rows = sqlx::query(
        "SELECT name, description, developer_instructions, model_connection_id,
                model_id, model_settings_override, enabled, disabled_reason
         FROM subagent_definitions
         WHERE agent_id = $1
         ORDER BY lower(name), id",
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(subagent_from_row).collect())
}

pub(crate) async fn load_subagents_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
) -> Result<Vec<SubagentDefinition>, ApiError> {
    let rows = sqlx::query(
        "SELECT name, description, developer_instructions, model_connection_id,
                model_id, model_settings_override, enabled, disabled_reason
         FROM subagent_definitions
         WHERE agent_id = $1
         ORDER BY lower(name), id",
    )
    .bind(agent_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(subagent_from_row).collect())
}

pub(crate) async fn hydrate_agent_configuration(
    pool: &PgPool,
    agent: &mut AgentDto,
) -> Result<(), ApiError> {
    agent.managed_skill_ids = load_managed_skill_ids(pool, agent.id).await?;
    agent.secret_declarations = load_agent_secret_declarations(pool, agent.id).await?;
    agent.subagents = load_subagents(pool, agent.id).await?;
    Ok(())
}

pub(crate) fn normalized_subagents(definitions: &[SubagentDefinition]) -> Vec<String> {
    let mut definitions = definitions
        .iter()
        .map(|definition| {
            serde_json::to_value(definition)
                .map(|value| canonical_json(&value))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    definitions.sort();
    definitions
}

pub(crate) fn agent_execution_configuration_changed(
    existing: &AgentDto,
    request: &UpdateAgentRequest,
) -> bool {
    let existing_skill_ids = existing
        .managed_skill_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let requested_skill_ids = request
        .managed_skill_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    existing.instructions != request.instructions.trim()
        || existing.model_selection != request.model_selection
        || existing.model_settings != request.model_settings
        || normalized_subagents(&existing.subagents) != normalized_subagents(&request.subagents)
        || existing.model_policy != request.model_policy
        || existing.sandbox_policy != request.sandbox_policy
        || normalized_unordered_entries(&existing.mcp_allowlist)
            != normalized_unordered_entries(&request.mcp_allowlist)
        || existing.tool_allowlist != request.tool_allowlist
        || existing_skill_ids != requested_skill_ids
        || request
            .secret_declarations
            .as_ref()
            .is_some_and(|requested| {
                normalized_secret_declarations(&existing.secret_declarations)
                    != normalized_secret_declarations(requested)
            })
}

pub(crate) fn normalized_secret_declarations(
    declarations: &[AgentSecretDeclarationDto],
) -> Vec<String> {
    let mut entries = declarations
        .iter()
        .map(|declaration| {
            canonical_json(&json!({
                "name": declaration.name,
                "kind": declaration.kind,
                "description": declaration.description,
            }))
        })
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

pub(crate) fn normalized_unordered_entries(value: &Value) -> Vec<String> {
    let mut entries = value
        .as_array()
        .into_iter()
        .flatten()
        .map(canonical_json)
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AgentAccess {
    pub(crate) can_manage: bool,
    pub(crate) can_administer: bool,
    pub(crate) can_invoke: bool,
}

pub(crate) fn agent_access(agent: &AgentDto, user: &UserDto) -> AgentAccess {
    let is_owner = agent.owner_id == user.id;
    AgentAccess {
        can_manage: is_owner || is_admin_role(&user.role),
        can_administer: is_owner || is_admin_role(&user.role),
        can_invoke: is_owner
            || agent.visibility == "public"
            || (agent.visibility == "public_to" && agent.public_to.contains(&user.id)),
    }
}

#[cfg(test)]
pub(crate) fn widget_agent_from_agent(agent: AgentDto) -> WidgetAgentDto {
    // iframe widget 只需要展示和发起消息，不暴露控制面的私有配置。
    WidgetAgentDto {
        id: agent.id,
        name: agent.name,
        instructions: agent.instructions,
    }
}

pub(crate) fn apply_agent_access(mut agent: AgentDto, user: &UserDto) -> AgentDto {
    let access = agent_access(&agent, user);
    agent.is_owner = agent.owner_id == user.id;
    agent.can_manage = access.can_manage;
    agent.can_administer = access.can_administer;
    agent.can_invoke = access.can_invoke;
    if !agent.is_owner && !is_admin_role(&user.role) {
        agent.public_to.clear();
        agent.runtime_id = None;
        agent.model_selection = None;
        agent.model_settings = AgentModelSettings::default();
        agent.subagents.clear();
        agent.model_policy = json!({});
        agent.sandbox_policy = json!({});
        agent.managed_skill_ids.clear();
        agent.mcp_allowlist = json!([]);
        return agent;
    }
    agent.mcp_allowlist = redact_mcp_secrets(&agent.mcp_allowlist);
    agent
}

pub(crate) fn redact_mcp_secrets(value: &Value) -> Value {
    let Some(servers) = value.as_array() else {
        return json!([]);
    };
    Value::Array(
        servers
            .iter()
            .map(|server| {
                let mut server = server.clone();
                if let Some(secrets) = server.get_mut("secrets").and_then(Value::as_object_mut) {
                    for value in secrets.values_mut() {
                        *value = json!(REDACTED_SECRET);
                    }
                }
                server
            })
            .collect(),
    )
}

pub(crate) fn merge_mcp_secrets(existing: &Value, incoming: &Value) -> Value {
    let Some(incoming_servers) = incoming.as_array() else {
        return incoming.clone();
    };
    let mut merged = Vec::with_capacity(incoming_servers.len());
    for incoming_server in incoming_servers {
        let mut server = incoming_server.clone();
        let name = server
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(secrets) = server.get_mut("secrets").and_then(Value::as_object_mut) {
            for (key, value) in secrets.iter_mut() {
                if value.as_str() == Some(REDACTED_SECRET) {
                    if let Some(existing_value) =
                        existing_mcp_secret(existing, name.as_deref(), key)
                    {
                        *value = json!(existing_value);
                    }
                }
            }
        }
        merged.push(server);
    }
    Value::Array(merged)
}

pub(crate) fn existing_mcp_secret<'a>(
    existing: &'a Value,
    server_name: Option<&str>,
    key: &str,
) -> Option<&'a str> {
    let server_name = server_name?;
    existing.as_array()?.iter().find_map(|server| {
        if server.get("name").and_then(Value::as_str) != Some(server_name) {
            return None;
        }
        server
            .get("secrets")
            .and_then(Value::as_object)?
            .get(key)?
            .as_str()
    })
}

pub(crate) fn build_agent_execution_configuration(
    agent: &AgentDto,
    revision: i64,
    managed_rows: Vec<sqlx::postgres::PgRow>,
) -> Result<AgentExecutionConfigurationDto, ApiError> {
    let mut skills = std::collections::BTreeMap::new();
    for row in managed_rows {
        let name: String = row.get("name");
        let description: String = row.get("description");
        skills.insert(
            name.clone(),
            AgentExecutionSkillDto {
                source: "managed".into(),
                source_id: Some(row.get("id")),
                name: name.clone(),
                description: if description.trim().is_empty() {
                    name
                } else {
                    description
                },
                content: row.get("content"),
                revision: row.get("revision"),
                content_checksum_sha256: row.get("content_checksum_sha256"),
                package: execution_skill_package_from_row(&row),
            },
        );
    }
    Ok(AgentExecutionConfigurationDto {
        revision,
        instructions: agent.instructions.clone(),
        model_selection: agent.model_selection.clone(),
        model_settings: agent.model_settings.clone(),
        subagents: agent.subagents.clone(),
        model_bindings: Vec::new(),
        model_policy: agent.model_policy.clone(),
        sandbox_policy: agent.sandbox_policy.clone(),
        skills: skills.into_values().collect(),
        secret_declarations: agent.secret_declarations.clone(),
        mcp_allowlist: agent.mcp_allowlist.clone(),
        tool_allowlist: agent.tool_allowlist.clone(),
    })
}

pub(crate) fn execution_skill_package_from_row(
    row: &sqlx::postgres::PgRow,
) -> Option<SkillPackageDto> {
    let id = row
        .try_get::<Option<Uuid>, _>("package_id")
        .ok()
        .flatten()?;
    Some(SkillPackageDto {
        id,
        format_version: u32::try_from(row.get::<i32, _>("package_format_version"))
            .expect("Skill package format version is constrained"),
        size_bytes: u64::try_from(row.get::<i64, _>("package_size_bytes"))
            .expect("Skill package size is constrained"),
        checksum_sha256: row.get("package_checksum_sha256"),
        files: serde_json::from_value(row.get("package_files"))
            .expect("Skill package file manifest is constrained"),
    })
}

#[derive(Debug)]
pub(crate) struct SessionToolPolicy {
    pub(crate) public_widget: bool,
    pub(crate) app_tool_allowlist: Option<Vec<String>>,
}

pub(crate) async fn load_session_tool_policy_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
) -> Result<SessionToolPolicy, ApiError> {
    let row = sqlx::query(
        "SELECT hub.origin_kind,
                COALESCE(
                    (
                        SELECT app.tool_allowlist
                        FROM integration_sessions AS integration
                        JOIN oauth_apps AS app ON app.id = integration.oauth_app_id
                        WHERE integration.hub_session_id = hub.id
                          AND app.deleted_at IS NULL
                        ORDER BY integration.created_at DESC
                        LIMIT 1
                    ),
                    (
                        SELECT app.tool_allowlist
                        FROM embed_sessions AS embed
                        JOIN oauth_apps AS app ON app.id = embed.oauth_app_id
                        WHERE embed.hub_session_id = hub.id
                          AND app.deleted_at IS NULL
                        LIMIT 1
                    )
                ) AS app_tool_allowlist
         FROM hub_sessions AS hub
         WHERE hub.id = $1",
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("Hub Session not found"))?;
    let origin_kind: String = row.get("origin_kind");
    let app_tool_allowlist = row
        .get::<Option<Value>, _>("app_tool_allowlist")
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| ApiError::internal("stored App tool policy is invalid"))?;
    Ok(SessionToolPolicy {
        public_widget: origin_kind == "public_widget",
        app_tool_allowlist,
    })
}

pub(crate) fn apply_session_tool_policy(
    tools: &mut Vec<String>,
    sandbox_policy: &mut Value,
    mcp_allowlist: &mut Value,
    policy: &SessionToolPolicy,
) -> Result<(), ApiError> {
    let mut effective = normalize_agent_tool_allowlist(tools)?;
    if let Some(app_tool_allowlist) = policy.app_tool_allowlist.as_deref() {
        effective.retain(|tool| app_tool_allowlist.iter().any(|allowed| allowed == tool));
    }
    if policy.public_widget {
        effective.retain(|tool| PUBLIC_WIDGET_TOOL_NAMES.contains(&tool.as_str()));
        if effective.is_empty() {
            return Err(ApiError::conflict(
                "public Widget Agent must enable at least one read-only file tool",
            ));
        }
        *sandbox_policy = json!({ "mode": "read-only", "network_access": false });
        *mcp_allowlist = json!([]);
    }
    if effective.is_empty() {
        return Err(ApiError::conflict("effective Agent tool policy is empty"));
    }
    *tools = effective;
    Ok(())
}

pub(crate) async fn apply_session_tool_policy_to_agent_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    agent: &mut AgentDto,
) -> Result<(), ApiError> {
    let policy = load_session_tool_policy_tx(tx, session_id).await?;
    apply_session_tool_policy(
        &mut agent.tool_allowlist,
        &mut agent.sandbox_policy,
        &mut agent.mcp_allowlist,
        &policy,
    )
}

pub(crate) async fn apply_session_tool_policy_to_configuration_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    configuration: &mut AgentExecutionConfigurationDto,
) -> Result<(), ApiError> {
    let policy = load_session_tool_policy_tx(tx, session_id).await?;
    apply_session_tool_policy(
        &mut configuration.tool_allowlist,
        &mut configuration.sandbox_policy,
        &mut configuration.mcp_allowlist,
        &policy,
    )
}

pub(crate) async fn load_agent_execution_configuration_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
) -> Result<AgentExecutionConfigurationDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, owner_id,
                (SELECT email FROM users WHERE id = agents.owner_id) AS owner_email,
                name, instructions, visibility, public_to, runtime_id,
                model_connection_id, model_id, model_settings,
                model_policy, sandbox_policy, mcp_allowlist, tool_allowlist, execution_config_revision,
                created_at, updated_at
         FROM agents
         WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(agent_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("agent not found"))?;
    let revision: i64 = row.get("execution_config_revision");
    let mut agent = agent_from_row(row);
    agent.subagents = load_subagents_tx(tx, agent.id).await?;
    let skill_rows = sqlx::query(
        "SELECT skills.id, skills.name, skills.description, skills.content,
                skills.revision, skills.content_checksum_sha256,
                packages.id AS package_id, packages.format_version AS package_format_version,
                packages.size_bytes AS package_size_bytes,
                packages.checksum_sha256 AS package_checksum_sha256,
                packages.files AS package_files
         FROM agent_skills
         JOIN skills ON skills.id = agent_skills.skill_id
         LEFT JOIN skill_packages AS packages ON packages.id = skills.current_package_id
         WHERE agent_skills.agent_id = $1 AND skills.owner_id = $2
         ORDER BY skills.name, skills.id",
    )
    .bind(agent.id)
    .bind(agent.owner_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut configuration = build_agent_execution_configuration(&agent, revision, skill_rows)?;
    // A configuration refresh occurs between Runs. It must not manufacture a
    // new binding or replace the immutable provider route already materialized
    // for the online Session; the next claimed Run supplies fresh bindings.
    configuration.model_selection = None;
    configuration.model_settings = AgentModelSettings::default();
    for subagent in &mut configuration.subagents {
        subagent.model_selection = None;
        subagent.model_settings_override = AgentModelSettingsOverride::default();
    }
    Ok(configuration)
}

pub(crate) async fn create_run_model_binding_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    binding_key: &str,
    owner_id: Uuid,
    selection: &ModelSelectionDto,
    settings: &AgentModelSettings,
) -> Result<RunModelBindingDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, name, scope, api_type
         FROM model_connections
         WHERE id = $1 AND enabled = true AND deleted_at IS NULL
           AND $2 = ANY(allowed_model_ids)
           AND (scope = 'global' OR owner_id = $3)
         FOR SHARE",
    )
    .bind(selection.connection_id)
    .bind(&selection.model_id)
    .bind(owner_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::conflict(
        "Agent model configuration is unavailable",
    ))?;
    let api_type = model_upstream_protocol_from_name(&row.get::<String, _>("api_type"));
    let settings = validate_agent_model_settings(settings.clone(), api_type)?;
    let binding = RunModelBindingDto {
        id: Uuid::new_v4(),
        run_id,
        binding_key: binding_key.to_owned(),
        model_connection_id: selection.connection_id,
        connection_name_snapshot: row.get("name"),
        connection_scope_snapshot: if row.get::<String, _>("scope") == "global" {
            ModelConnectionScope::Global
        } else {
            ModelConnectionScope::Personal
        },
        model_id: selection.model_id.clone(),
        api_type,
        model_settings: settings,
    };
    sqlx::query(
        "INSERT INTO run_model_bindings
             (id, run_id, binding_key, model_connection_id,
              connection_name_snapshot, connection_scope_snapshot,
              model_id, api_type, model_settings)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(binding.id)
    .bind(run_id)
    .bind(&binding.binding_key)
    .bind(binding.model_connection_id)
    .bind(&binding.connection_name_snapshot)
    .bind(model_connection_scope_name(
        binding.connection_scope_snapshot,
    ))
    .bind(&binding.model_id)
    .bind(model_upstream_protocol_name(binding.api_type))
    .bind(
        serde_json::to_value(&binding.model_settings)
            .map_err(|_| ApiError::internal("Run Model Binding could not be encoded"))?,
    )
    .execute(&mut **tx)
    .await?;
    Ok(binding)
}

pub(crate) async fn load_run_model_bindings_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> Result<Vec<RunModelBindingDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, run_id, binding_key, model_connection_id,
                connection_name_snapshot, connection_scope_snapshot,
                model_id, api_type, model_settings
         FROM run_model_bindings
         WHERE run_id = $1
         ORDER BY lower(binding_key), id",
    )
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| RunModelBindingDto {
            id: row.get("id"),
            run_id: row.get("run_id"),
            binding_key: row.get("binding_key"),
            model_connection_id: row.get("model_connection_id"),
            connection_name_snapshot: row.get("connection_name_snapshot"),
            connection_scope_snapshot: match row
                .get::<String, _>("connection_scope_snapshot")
                .as_str()
            {
                "global" => ModelConnectionScope::Global,
                "personal" => ModelConnectionScope::Personal,
                _ => unreachable!("Model Connection scope is constrained"),
            },
            model_id: row.get("model_id"),
            api_type: model_upstream_protocol_from_name(&row.get::<String, _>("api_type")),
            model_settings: serde_json::from_value(row.get("model_settings"))
                .expect("Run Model Binding settings are constrained"),
        })
        .collect())
}

pub(crate) async fn create_run_model_bindings_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    agent: &AgentDto,
) -> Result<Vec<RunModelBindingDto>, ApiError> {
    let main_selection = agent
        .model_selection
        .as_ref()
        .ok_or(ApiError::conflict("Agent has no configured model"))?;
    let mut bindings = vec![
        create_run_model_binding_tx(
            tx,
            run_id,
            "main",
            agent.owner_id,
            main_selection,
            &agent.model_settings,
        )
        .await?,
    ];
    for subagent in agent.subagents.iter().filter(|subagent| {
        subagent.enabled
            && (subagent.model_selection.is_some()
                || subagent.model_settings_override != AgentModelSettingsOverride::default())
    }) {
        let selection = subagent.model_selection.as_ref().unwrap_or(main_selection);
        let protocol =
            load_permitted_model_selection_api_type_tx(tx, agent.owner_id, selection).await?;
        let settings = effective_subagent_model_settings(
            &agent.model_settings,
            &subagent.model_settings_override,
            protocol,
            subagent.model_selection.is_some()
                && protocol != agent.model_settings.request_settings.protocol(),
        )?;
        bindings.push(
            create_run_model_binding_tx(
                tx,
                run_id,
                subagent.name.trim(),
                agent.owner_id,
                selection,
                &settings,
            )
            .await?,
        );
    }
    Ok(bindings)
}

