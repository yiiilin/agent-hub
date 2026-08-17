//! models 领域模块：Model API Connection / 模型用量台账 的 handler 与私有辅助函数。

use super::*;
use crate::{send_model_upstream_request, ModelUpstreamForwardRequest, REDACTED_SECRET};
use std::{collections::BTreeSet, sync::Arc, time::Instant};

use agent_hub_shared::*;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Number, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

pub(crate) async fn list_model_connections(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ModelConnectionDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT c.id, c.owner_id,
                (SELECT email FROM users WHERE id = c.owner_id) AS owner_email,
                c.scope, c.name, c.base_url, c.api_type,
                c.allowed_model_ids, c.enabled, c.vision_model_id,
                (c.api_key_ciphertext IS NOT NULL) AS has_api_key,
                c.created_at, c.updated_at
         FROM model_connections c
         WHERE c.deleted_at IS NULL
           AND (c.scope = 'global' OR c.owner_id = $1)
         ORDER BY c.scope, lower(c.name), c.id",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| model_connection_from_row(&row))
            .collect(),
    ))
}

pub(crate) async fn get_model_connection_options(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ModelConnectionOptionsDto>, ApiError> {
    let items = list_model_connections(State(state.clone()), headers)
        .await?
        .0
        .into_iter()
        .flat_map(|connection| {
            connection
                .allowed_model_ids
                .into_iter()
                .map(move |model_id| ModelConnectionOptionDto {
                    connection_id: connection.id,
                    connection_name: connection.name.clone(),
                    model_id,
                    api_type: connection.api_type,
                    scope: connection.scope,
                    status: connection.status,
                })
        })
        .collect();
    let system_default = sqlx::query(
        "SELECT model_connection_id, model_id
         FROM system_default_model_selection
         WHERE singleton = true",
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

pub(crate) fn validate_vision_model_id(
    vision_model_id: Option<String>,
    allowed_model_ids: &[String],
) -> Result<Option<String>, ApiError> {
    let Some(vision_model_id) = vision_model_id else {
        return Ok(None);
    };
    let vision_model_id = vision_model_id.trim();
    if vision_model_id.is_empty() {
        return Ok(None);
    }
    if vision_model_id.chars().count() > 255 || vision_model_id.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "vision model id must contain 1 to 255 non-control characters",
        ));
    }
    if !allowed_model_ids
        .iter()
        .any(|model_id| model_id == vision_model_id)
    {
        return Err(ApiError::bad_request(
            "vision model id must be one of the allowed model ids",
        ));
    }
    Ok(Some(vision_model_id.to_owned()))
}

pub(crate) async fn create_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateModelConnectionRequest>,
) -> Result<Json<ModelConnectionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    if req.scope == ModelConnectionScope::Global && !is_admin_role(&user.role) {
        return Err(ApiError::forbidden(
            "administrator permission is required for Global Model API Connections",
        ));
    }
    let allowed_model_ids = normalize_allowed_model_ids(req.allowed_model_ids)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let fields = validate_model_connection_fields(
        &req.name,
        &req.base_url,
        allowed_model_ids,
        Some(&req.api_key),
    )?;
    let vision_model_id = validate_vision_model_id(req.vision_model_id, &fields.allowed_model_ids)?;
    let encrypted = state
        .model_secret_cipher
        .encrypt(&req.api_key)
        .map_err(|_| ApiError::internal("model secret encryption failed"))?;
    let id = Uuid::new_v4();
    let (scope, owner_id) = match req.scope {
        ModelConnectionScope::Global => ("global", None),
        ModelConnectionScope::Personal => ("personal", Some(user.id)),
    };
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, owner_id, name, base_url, api_type,
              allowed_model_ids, vision_model_id,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(id)
    .bind(scope)
    .bind(owner_id)
    .bind(fields.name)
    .bind(fields.base_url)
    .bind(model_upstream_protocol_name(req.api_type))
    .bind(fields.allowed_model_ids)
    .bind(vision_model_id.as_deref())
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .bind(user.id)
    .execute(&state.pool)
    .await
    .map_err(map_model_connection_write_error)?;
    Ok(Json(
        load_visible_model_connection(&state.pool, id, user.id).await?,
    ))
}

pub(crate) async fn get_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
) -> Result<Json<ModelConnectionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(
        load_visible_model_connection(&state.pool, model_connection_id, user.id).await?,
    ))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct UpdateModelConnectionQuery {
    #[serde(default)]
    pub(crate) force: bool,
}

pub(crate) async fn update_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
    Query(query): Query<UpdateModelConnectionQuery>,
    Json(req): Json<UpdateModelConnectionRequest>,
) -> Result<Json<ModelConnectionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let allowed_model_ids = normalize_allowed_model_ids(req.allowed_model_ids)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let fields = validate_model_connection_fields(
        &req.name,
        &req.base_url,
        allowed_model_ids,
        req.api_key.as_deref(),
    )?;
    let vision_model_id = validate_vision_model_id(req.vision_model_id, &fields.allowed_model_ids)?;
    let encrypted = req
        .api_key
        .as_deref()
        .map(|api_key| state.model_secret_cipher.encrypt(api_key))
        .transpose()
        .map_err(|_| ApiError::internal("model secret encryption failed"))?;
    let mut tx = state.pool.begin().await?;
    sqlx::query(
        "LOCK TABLE system_default_model_selection, agents, subagent_definitions
         IN SHARE ROW EXCLUSIVE MODE",
    )
    .execute(&mut *tx)
    .await?;
    load_mutable_model_connection_tx(&mut tx, model_connection_id, &user).await?;
    let previous = sqlx::query(
        "SELECT name, api_type, allowed_model_ids
         FROM model_connections WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(model_connection_id)
    .fetch_one(&mut *tx)
    .await?;
    let previous_name: String = previous.get("name");
    let previous_api_type: String = previous.get("api_type");
    let previous_allowed_model_ids: Vec<String> = previous.get("allowed_model_ids");
    let api_type_name = model_upstream_protocol_name(req.api_type);
    let api_type_changed = previous_api_type != api_type_name;
    let allowed = fields
        .allowed_model_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let removed_model_ids = previous_allowed_model_ids
        .into_iter()
        .filter(|model_id| !allowed.contains(model_id))
        .collect::<Vec<_>>();
    if query.force && (api_type_changed || !removed_model_ids.is_empty()) {
        clear_model_selection_references_tx(
            &mut tx,
            model_connection_id,
            &removed_model_ids,
            api_type_changed,
            "model_selection_removed",
        )
        .await?;
    }
    let (api_key_ciphertext, api_key_nonce) = encrypted
        .map(|encrypted| (Some(encrypted.ciphertext), Some(encrypted.nonce)))
        .unwrap_or((None, None));
    let updated = sqlx::query(
        "UPDATE model_connections
         SET name = $1, base_url = $2, api_type = $3, allowed_model_ids = $4,
             vision_model_id = $5,
             api_key_ciphertext = COALESCE($6, api_key_ciphertext),
             api_key_nonce = COALESCE($7, api_key_nonce),
             updated_at = CURRENT_TIMESTAMP(3)
         WHERE id = $8 AND deleted_at IS NULL",
    )
    .bind(&fields.name)
    .bind(&fields.base_url)
    .bind(api_type_name)
    .bind(&fields.allowed_model_ids)
    .bind(vision_model_id.as_deref())
    .bind(api_key_ciphertext)
    .bind(api_key_nonce)
    .bind(model_connection_id)
    .execute(&mut *tx)
    .await
    .map_err(map_model_connection_write_error)?;
    if updated.rows_affected() == 0 {
        return Err(ApiError::not_found("model connection not found"));
    }
    if previous_name != fields.name || api_type_changed {
        bump_agents_for_model_connection_tx(&mut tx, model_connection_id).await?;
    }
    tx.commit().await?;
    Ok(Json(
        load_visible_model_connection(&state.pool, model_connection_id, user.id).await?,
    ))
}

pub(crate) async fn update_model_connection_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
    Json(req): Json<UpdateModelConnectionStatusRequest>,
) -> Result<Json<ModelConnectionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    load_mutable_model_connection_tx(&mut tx, model_connection_id, &user).await?;
    let enabled = req.status == ModelConnectionStatus::Enabled;
    sqlx::query(
        "UPDATE model_connections
         SET enabled = $1, updated_at = CURRENT_TIMESTAMP(3)
         WHERE id = $2 AND deleted_at IS NULL",
    )
    .bind(enabled)
    .bind(model_connection_id)
    .execute(&mut *tx)
    .await?;
    bump_agents_for_model_connection_tx(&mut tx, model_connection_id).await?;
    if !enabled {
        sqlx::query("DELETE FROM system_default_model_selection WHERE model_connection_id = $1")
            .bind(model_connection_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(Json(
        load_visible_model_connection(&state.pool, model_connection_id, user.id).await?,
    ))
}

pub(crate) async fn delete_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    delete_model_connection_impl(&state, &headers, model_connection_id, false).await
}

pub(crate) async fn force_delete_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    delete_model_connection_impl(&state, &headers, model_connection_id, true).await
}

pub(crate) async fn bump_agents_for_model_connection_tx(
    tx: &mut Transaction<'_, Postgres>,
    model_connection_id: Uuid,
) -> Result<(), ApiError> {
    sqlx::query(
        "UPDATE agents AS agent
         SET execution_config_revision = execution_config_revision + 1,
             updated_at = CURRENT_TIMESTAMP(3)
         WHERE agent.deleted_at IS NULL
           AND (
               agent.model_connection_id = $1
               OR EXISTS (
                   SELECT 1 FROM subagent_definitions AS subagent
                   WHERE subagent.agent_id = agent.id
                     AND subagent.model_connection_id = $1
               )
           )",
    )
    .bind(model_connection_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn clear_model_selection_references_tx(
    tx: &mut Transaction<'_, Postgres>,
    model_connection_id: Uuid,
    removed_model_ids: &[String],
    all_models: bool,
    disabled_reason: &str,
) -> Result<(), ApiError> {
    if !all_models && removed_model_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "DELETE FROM system_default_model_selection
         WHERE model_connection_id = $1
           AND ($3 OR model_id = ANY($2))",
    )
    .bind(model_connection_id)
    .bind(removed_model_ids)
    .bind(all_models)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE agents AS agent
         SET model_connection_id = NULL, model_id = NULL,
             execution_config_revision = execution_config_revision + 1,
             updated_at = CURRENT_TIMESTAMP(3)
         WHERE agent.deleted_at IS NULL
           AND agent.model_connection_id = $1
           AND ($3 OR agent.model_id = ANY($2))",
    )
    .bind(model_connection_id)
    .bind(removed_model_ids)
    .bind(all_models)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE agents AS agent
         SET execution_config_revision = execution_config_revision + 1,
             updated_at = CURRENT_TIMESTAMP(3)
         WHERE agent.deleted_at IS NULL
           AND EXISTS (
               SELECT 1 FROM subagent_definitions AS subagent
               WHERE subagent.agent_id = agent.id
                 AND subagent.model_connection_id = $1
                 AND ($3 OR subagent.model_id = ANY($2))
           )",
    )
    .bind(model_connection_id)
    .bind(removed_model_ids)
    .bind(all_models)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE subagent_definitions
         SET model_connection_id = NULL, model_id = NULL, enabled = false,
             disabled_reason = $4, updated_at = CURRENT_TIMESTAMP(3)
         WHERE model_connection_id = $1
           AND ($3 OR model_id = ANY($2))",
    )
    .bind(model_connection_id)
    .bind(removed_model_ids)
    .bind(all_models)
    .bind(disabled_reason)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn delete_model_connection_impl(
    state: &AppState,
    headers: &HeaderMap,
    model_connection_id: Uuid,
    force: bool,
) -> Result<StatusCode, ApiError> {
    let user = require_user(state, headers).await?;
    let mut tx = state.pool.begin().await?;
    sqlx::query(
        "LOCK TABLE system_default_model_selection, agents, subagent_definitions
         IN SHARE ROW EXCLUSIVE MODE",
    )
    .execute(&mut *tx)
    .await?;
    load_mutable_model_connection_tx(&mut tx, model_connection_id, &user).await?;
    let is_system_default: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM system_default_model_selection
             WHERE model_connection_id = $1
         )",
    )
    .bind(model_connection_id)
    .fetch_one(&mut *tx)
    .await?;
    let agent_references: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM agents
         WHERE model_connection_id = $1 AND deleted_at IS NULL",
    )
    .bind(model_connection_id)
    .fetch_one(&mut *tx)
    .await?;
    let subagent_references: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM subagent_definitions
         WHERE model_connection_id = $1",
    )
    .bind(model_connection_id)
    .fetch_one(&mut *tx)
    .await?;
    if !force && (is_system_default || agent_references > 0 || subagent_references > 0) {
        return Err(ApiError::conflict(format!(
            "Model API Connection is referenced by System Default ({}), Agents ({}), subagents ({})",
            i64::from(is_system_default),
            agent_references,
            subagent_references
        )));
    }
    if force {
        clear_model_selection_references_tx(
            &mut tx,
            model_connection_id,
            &[],
            true,
            "model_connection_deleted",
        )
        .await?;
    }
    sqlx::query(
        "UPDATE model_connections
         SET base_url = NULL, api_key_ciphertext = NULL, api_key_nonce = NULL,
             enabled = false, deleted_at = CURRENT_TIMESTAMP(3),
             updated_at = CURRENT_TIMESTAMP(3)
         WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(model_connection_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn get_system_default_model_selection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<SystemDefaultModelSelectionDto>, ApiError> {
    require_user(&state, &headers).await?;
    let selection = sqlx::query(
        "SELECT model_connection_id, model_id
         FROM system_default_model_selection
         WHERE singleton = true",
    )
    .fetch_optional(&state.pool)
    .await?
    .map(|row| ModelSelectionDto {
        connection_id: row.get("model_connection_id"),
        model_id: row.get("model_id"),
    });
    Ok(Json(SystemDefaultModelSelectionDto { selection }))
}

pub(crate) async fn set_system_default_model_selection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SetSystemDefaultModelSelectionRequest>,
) -> Result<Json<SystemDefaultModelSelectionDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    match req.selection.as_ref() {
        Some(selection) => {
            let valid = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM model_connections
                 WHERE id = $1 AND scope = 'global' AND enabled = true
                   AND deleted_at IS NULL AND $2 = ANY(allowed_model_ids)
                 FOR UPDATE",
            )
            .bind(selection.connection_id)
            .bind(&selection.model_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                ApiError::bad_request(
                    "System Default must be an allowed model on an enabled Global Model API Connection",
                )
            })?;
            debug_assert_eq!(valid, selection.connection_id);
            sqlx::query(
                "INSERT INTO system_default_model_selection
                     (singleton, model_connection_id, model_id, updated_by)
                 VALUES (true, $1, $2, $3)
                 ON CONFLICT (singleton) DO UPDATE
                 SET model_connection_id = EXCLUDED.model_connection_id,
                     model_id = EXCLUDED.model_id,
                     updated_by = EXCLUDED.updated_by,
                     updated_at = CURRENT_TIMESTAMP(3)",
            )
            .bind(selection.connection_id)
            .bind(&selection.model_id)
            .bind(administrator.id)
            .execute(&mut *tx)
            .await
            .map_err(map_model_connection_write_error)?;
        }
        None => {
            sqlx::query("DELETE FROM system_default_model_selection WHERE singleton = true")
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok(Json(SystemDefaultModelSelectionDto {
        selection: req.selection,
    }))
}

pub(crate) async fn test_model_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(model_connection_id): Path<Uuid>,
    Json(req): Json<TestModelConnectionRequest>,
) -> Result<Json<ModelConnectionTestResultDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let connection =
        load_model_connection_secret_for_test(&state.pool, model_connection_id, &user).await?;
    let model_id = req.model_id.trim();
    if !connection
        .dto
        .allowed_model_ids
        .iter()
        .any(|allowed| allowed == model_id)
    {
        return Err(ApiError::bad_request(
            "Model ID is not allowed by this Model API Connection",
        ));
    }
    let message = req.message.trim();
    if message.is_empty() || message.chars().count() > 4_000 {
        return Err(ApiError::bad_request(
            "Model Connection test message must be 1 to 4000 characters",
        ));
    }
    let request_settings = ModelRequestSettings::for_protocol(connection.dto.api_type);
    let api_key = Zeroizing::new(
        state
            .model_secret_cipher
            .decrypt(&connection.ciphertext, &connection.nonce)
            .map_err(|_| ApiError::internal("model secret decryption failed"))?,
    );
    let request_id = Uuid::new_v4();
    let ledger_context = ModelTestLedgerContext {
        pool: &state.pool,
        request_id,
        connection: &connection.dto,
        model_id,
        request_settings: &request_settings,
        user: &user,
    };
    let request_body =
        build_model_request_body(model_id, message, 256, 1.0, connection.dto.api_type)
            .map_err(|_| ApiError::internal("failed to encode Model Connection test request"))?;
    let request_headers = HeaderMap::new();
    let started_at = Instant::now();
    let response = send_model_upstream_request(
        &state,
        ModelUpstreamForwardRequest {
            upstream_protocol: connection.dto.api_type,
            upstream_url: &connection.dto.base_url,
            path: connection.dto.api_type.upstream_path(),
            query: None,
            headers: &request_headers,
            body: &request_body,
            api_key: &api_key,
        },
    )
    .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            let (message, error_code) = match error {
                ModelUpstreamSendError::InvalidAuthHeader => (
                    "connection credential cannot be represented as an HTTP header",
                    "invalid_credential_header",
                ),
                ModelUpstreamSendError::Request(_) => {
                    ("connection request failed", "transport_error")
                }
            };
            record_model_test_error(
                &ledger_context,
                "transport_error",
                None,
                "transport_error",
                Some(error_code),
                message,
            )
            .await?;
            return Ok(Json(ModelConnectionTestResultDto {
                success: false,
                status_code: None,
                error_code: Some(error_code.into()),
                message: Some(message.into()),
                response_text: None,
                response_time_ms: model_test_response_time_ms(started_at),
            }));
        }
    };
    let status = response.status();
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => {
            let error_code = "response_body_error";
            let message = "failed to read Model Connection test response";
            record_model_test_error(
                &ledger_context,
                "transport_error",
                Some(status.as_u16()),
                "response_body",
                Some(error_code),
                message,
            )
            .await?;
            return Ok(Json(ModelConnectionTestResultDto {
                success: false,
                status_code: Some(status.as_u16()),
                error_code: Some(error_code.into()),
                message: Some(message.into()),
                response_text: None,
                response_time_ms: model_test_response_time_ms(started_at),
            }));
        }
    };
    let response_time_ms = model_test_response_time_ms(started_at);
    let value = serde_json::from_slice::<Value>(&body).ok();
    let response_status =
        model_response_status_for(value.as_ref(), status.is_success(), connection.dto.api_type);
    let usage = value
        .as_ref()
        .and_then(|value| extract_model_usage_for(value, connection.dto.api_type));
    if let Some(usage) = usage.as_ref() {
        record_model_test_usage(&ledger_context, response_status, usage).await?;
    }
    let completed = status.is_success() && response_status == "completed" && usage.is_some();
    if completed {
        return Ok(Json(ModelConnectionTestResultDto {
            success: true,
            status_code: Some(status.as_u16()),
            error_code: None,
            message: None,
            response_text: value
                .as_ref()
                .and_then(|value| model_test_response_text_for(value, connection.dto.api_type)),
            response_time_ms,
        }));
    }
    let error_code = value
        .as_ref()
        .and_then(|body| body.pointer("/error/code"))
        .and_then(Value::as_str)
        .map(|code| sanitize_model_error_message(code, &api_key))
        .or_else(|| {
            if status.is_success() {
                Some("protocol_error".into())
            } else {
                Some("upstream_error".into())
            }
        });
    let message = value
        .as_ref()
        .and_then(|body| body.pointer("/error/message"))
        .and_then(Value::as_str)
        .map(|message| sanitize_model_error_message(message, &api_key))
        .unwrap_or_else(|| {
            if status.is_success() {
                "completed response did not include valid usage".into()
            } else {
                format!("upstream returned HTTP {}", status.as_u16())
            }
        });
    let ledger_status = if status.is_success() {
        if matches!(response_status, "failed" | "incomplete" | "cancelled") {
            response_status
        } else {
            "protocol_error"
        }
    } else {
        "failed"
    };
    record_model_test_error(
        &ledger_context,
        ledger_status,
        Some(status.as_u16()),
        if status.is_success() {
            "protocol_error"
        } else {
            "upstream_http"
        },
        error_code.as_deref(),
        &message,
    )
    .await?;
    Ok(Json(ModelConnectionTestResultDto {
        success: false,
        status_code: Some(status.as_u16()),
        error_code,
        message: Some(message),
        response_text: None,
        response_time_ms,
    }))
}

pub(crate) fn model_test_response_time_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn model_test_response_text(value: &Value) -> Option<String> {
    if let Some(text) = value
        .get("output_text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        return Some(text.to_owned());
    }

    let mut chunks = Vec::new();
    for item in value.get("output").and_then(Value::as_array)? {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in content {
            let text = match part.get("type").and_then(Value::as_str) {
                Some("output_text" | "text") => part.get("text").and_then(Value::as_str),
                Some("refusal") => part.get("refusal").and_then(Value::as_str),
                _ => None,
            };
            if let Some(text) = text.filter(|text| !text.trim().is_empty()) {
                chunks.push(text);
            }
        }
    }
    (!chunks.is_empty()).then(|| chunks.join("\n"))
}

/// 按协议构造 Hub 自发的模型请求体（会话标题、连接测试），匹配上游协议格式。
pub(crate) fn build_model_request_body(
    model_id: &str,
    prompt: &str,
    max_tokens: u32,
    temperature: f64,
    protocol: ModelUpstreamProtocol,
) -> Result<Vec<u8>, serde_json::Error> {
    let body = match protocol {
        ModelUpstreamProtocol::OpenaiResponses => json!({
            "model": model_id,
            "input": prompt,
            "max_output_tokens": max_tokens,
            "temperature": temperature,
        }),
        ModelUpstreamProtocol::OpenaiChatCompletions => json!({
            "model": model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_completion_tokens": max_tokens,
            "temperature": temperature,
        }),
        ModelUpstreamProtocol::AnthropicMessages => json!({
            "model": model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }),
    };
    serde_json::to_vec(&body)
}

/// 按协议提取 usage（Responses: input_tokens；Chat: prompt_tokens；Anthropic: 含 cache 口径）。
pub(crate) fn extract_model_usage_for(
    value: &Value,
    protocol: ModelUpstreamProtocol,
) -> Option<ObservedModelUsage> {
    match protocol {
        ModelUpstreamProtocol::OpenaiResponses => extract_model_usage(value),
        ModelUpstreamProtocol::OpenaiChatCompletions => extract_chat_usage(value),
        ModelUpstreamProtocol::AnthropicMessages => extract_anthropic_usage(value),
    }
}

pub(crate) fn model_test_response_text_for(
    value: &Value,
    protocol: ModelUpstreamProtocol,
) -> Option<String> {
    match protocol {
        ModelUpstreamProtocol::OpenaiResponses => model_test_response_text(value),
        ModelUpstreamProtocol::OpenaiChatCompletions => chat_response_text(value),
        ModelUpstreamProtocol::AnthropicMessages => anthropic_response_text(value),
    }
}

fn chat_response_text(value: &Value) -> Option<String> {
    let message = value.pointer("/choices/0/message")?;
    if let Some(text) = message
        .get("content")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    {
        return Some(text.to_owned());
    }
    let mut chunks = Vec::new();
    for part in message.get("content").and_then(Value::as_array)? {
        if let Some(text) = part
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        {
            chunks.push(text);
        }
    }
    (!chunks.is_empty()).then(|| chunks.join("\n"))
}

fn anthropic_response_text(value: &Value) -> Option<String> {
    let mut chunks = Vec::new();
    for block in value.get("content").and_then(Value::as_array)? {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            continue;
        }
        if let Some(text) = block
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
        {
            chunks.push(text);
        }
    }
    (!chunks.is_empty()).then(|| chunks.join("\n"))
}

#[derive(Debug)]
pub(crate) struct ValidatedModelConnectionFields {
    pub(crate) name: String,
    pub(crate) base_url: String,
    pub(crate) allowed_model_ids: Vec<String>,
}

pub(crate) fn validate_model_connection_fields(
    name: &str,
    base_url: &str,
    allowed_model_ids: Vec<String>,
    api_key: Option<&str>,
) -> Result<ValidatedModelConnectionFields, ApiError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > 128 || name.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "Model Connection name must be 1 to 128 characters",
        ));
    }
    if api_key.is_some_and(|value| value.trim().is_empty()) {
        return Err(ApiError::bad_request(
            "Model Connection API Key is required",
        ));
    }
    let mut parsed = Url::parse(base_url.trim())
        .map_err(|_| ApiError::bad_request("Model Connection Base URL is invalid"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(ApiError::bad_request(
            "Model Connection Base URL must use HTTP or HTTPS",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(ApiError::bad_request(
            "Model Connection Base URL cannot include a query or fragment",
        ));
    }
    let path_without_trailing_slash = parsed.path().trim_end_matches('/').to_owned();
    if path_without_trailing_slash
        .rsplit('/')
        .next()
        .is_some_and(|segment| segment.eq_ignore_ascii_case("v1"))
    {
        return Err(ApiError::bad_request(
            "Model Connection Base URL must not include the /v1 suffix",
        ));
    }
    parsed.set_path(&path_without_trailing_slash);
    Ok(ValidatedModelConnectionFields {
        name: name.into(),
        base_url: parsed.to_string().trim_end_matches('/').to_owned(),
        allowed_model_ids,
    })
}
pub(crate) fn validate_model_request_settings(
    protocol: ModelUpstreamProtocol,
    settings: ModelRequestSettings,
) -> Result<ModelRequestSettings, ApiError> {
    if settings.protocol() != protocol {
        return Err(ApiError::bad_request(
            "Agent request settings protocol must match the selected API Type",
        ));
    }
    match &settings {
        ModelRequestSettings::OpenaiResponses {} => {}
        ModelRequestSettings::OpenaiChatCompletions {
            temperature,
            top_p,
            max_completion_tokens,
        } => {
            validate_model_request_number("temperature", temperature.as_ref(), 2.0)?;
            validate_model_request_number("top_p", top_p.as_ref(), 1.0)?;
            validate_model_request_token_limit("max_completion_tokens", *max_completion_tokens)?;
        }
        ModelRequestSettings::AnthropicMessages {
            temperature,
            top_p,
            max_tokens,
        } => {
            if temperature.is_some() && top_p.is_some() {
                return Err(ApiError::bad_request(
                    "Anthropic request settings cannot set both temperature and top_p",
                ));
            }
            validate_model_request_number("temperature", temperature.as_ref(), 1.0)?;
            validate_model_request_number("top_p", top_p.as_ref(), 1.0)?;
            validate_model_request_token_limit("max_tokens", *max_tokens)?;
        }
    }
    Ok(settings)
}

pub(crate) fn validate_model_request_number(
    name: &str,
    value: Option<&Number>,
    maximum: f64,
) -> Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    let value = value.as_f64().filter(|value| value.is_finite());
    if !value.is_some_and(|value| (0.0..=maximum).contains(&value)) {
        return Err(ApiError::bad_request(format!(
            "Agent request setting {name} must be a finite number between 0 and {maximum}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_model_request_token_limit(
    name: &str,
    value: Option<u32>,
) -> Result<(), ApiError> {
    if value == Some(0) {
        return Err(ApiError::bad_request(format!(
            "Agent request setting {name} must be a positive integer"
        )));
    }
    Ok(())
}

pub(crate) fn model_connection_from_row(row: &sqlx::postgres::PgRow) -> ModelConnectionDto {
    ModelConnectionDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        owner_email: row.get("owner_email"),
        scope: match row.get::<String, _>("scope").as_str() {
            "global" => ModelConnectionScope::Global,
            _ => ModelConnectionScope::Personal,
        },
        name: row.get("name"),
        base_url: row.get("base_url"),
        api_type: model_upstream_protocol_from_name(&row.get::<String, _>("api_type")),
        allowed_model_ids: row.get("allowed_model_ids"),
        vision_model_id: row.get("vision_model_id"),
        status: if row.get("enabled") {
            ModelConnectionStatus::Enabled
        } else {
            ModelConnectionStatus::Disabled
        },
        has_api_key: row.get("has_api_key"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn model_request_settings_value(settings: &ModelRequestSettings) -> Value {
    serde_json::to_value(settings).expect("validated Model request settings are serializable")
}

pub(crate) async fn load_visible_model_connection(
    pool: &PgPool,
    model_connection_id: Uuid,
    user_id: Uuid,
) -> Result<ModelConnectionDto, ApiError> {
    let row = sqlx::query(
        "SELECT c.id, c.owner_id,
                (SELECT email FROM users WHERE id = c.owner_id) AS owner_email,
                c.scope, c.name, c.base_url, c.api_type,
                c.allowed_model_ids, c.enabled, c.vision_model_id,
                (c.api_key_ciphertext IS NOT NULL) AS has_api_key,
                c.created_at, c.updated_at
         FROM model_connections c
         WHERE c.id = $1 AND c.deleted_at IS NULL
           AND (c.scope = 'global' OR c.owner_id = $2)",
    )
    .bind(model_connection_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.as_ref()
        .map(model_connection_from_row)
        .ok_or(ApiError::not_found("model connection not found"))
}

pub(crate) async fn authorize_model_connection_mutation(
    pool: &PgPool,
    model_connection_id: Uuid,
    user: &UserDto,
) -> Result<(), ApiError> {
    let scope: String = sqlx::query_scalar(
        "SELECT scope FROM model_connections
         WHERE id = $1 AND deleted_at IS NULL
           AND (scope = 'global' OR owner_id = $2)",
    )
    .bind(model_connection_id)
    .bind(user.id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("model connection not found"))?;
    if scope == "global" && !is_admin_role(&user.role) {
        return Err(ApiError::forbidden(
            "administrator permission is required for Global Model Connections",
        ));
    }
    Ok(())
}

pub(crate) async fn load_mutable_model_connection_tx(
    tx: &mut Transaction<'_, Postgres>,
    model_connection_id: Uuid,
    user: &UserDto,
) -> Result<(), ApiError> {
    let scope: String = sqlx::query_scalar(
        "SELECT scope FROM model_connections
         WHERE id = $1 AND deleted_at IS NULL
           AND (scope = 'global' OR owner_id = $2)
         FOR UPDATE",
    )
    .bind(model_connection_id)
    .bind(user.id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("model connection not found"))?;
    if scope == "global" && !is_admin_role(&user.role) {
        return Err(ApiError::forbidden(
            "administrator permission is required for Global Model Connections",
        ));
    }
    Ok(())
}

pub(crate) struct ModelConnectionSecretRecord {
    pub(crate) dto: ModelConnectionDto,
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) nonce: Vec<u8>,
}

pub(crate) async fn load_model_connection_secret_for_test(
    pool: &PgPool,
    model_connection_id: Uuid,
    user: &UserDto,
) -> Result<ModelConnectionSecretRecord, ApiError> {
    authorize_model_connection_mutation(pool, model_connection_id, user).await?;
    let row = sqlx::query(
        "SELECT c.id, c.owner_id,
                (SELECT email FROM users WHERE id = c.owner_id) AS owner_email,
                c.scope, c.name, c.base_url, c.api_type,
                c.allowed_model_ids, c.enabled, c.vision_model_id,
                (c.api_key_ciphertext IS NOT NULL) AS has_api_key,
                c.created_at, c.updated_at,
                c.api_key_ciphertext, c.api_key_nonce
         FROM model_connections c
         WHERE c.id = $1 AND c.deleted_at IS NULL",
    )
    .bind(model_connection_id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("model connection not found"))?;
    Ok(ModelConnectionSecretRecord {
        dto: model_connection_from_row(&row),
        ciphertext: row.get("api_key_ciphertext"),
        nonce: row.get("api_key_nonce"),
    })
}

pub(crate) fn map_model_connection_write_error(error: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(database) = &error {
        return match database.code().as_deref() {
            Some("23505") => ApiError::conflict("Model Connection name already exists"),
            Some("23503") => ApiError::conflict("Model API Connection is still referenced"),
            Some("23514") => ApiError::bad_request("invalid Model Connection reference"),
            _ => {
                tracing::error!(error = %error, "database error");
                ApiError::internal("database error")
            }
        };
    }
    tracing::error!(error = %error, "database error");
    ApiError::internal("database error")
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedModelUsage {
    pub(crate) input_tokens: i64,
    pub(crate) output_tokens: i64,
    pub(crate) total_tokens: i64,
    pub(crate) cached_tokens: i64,
    pub(crate) reasoning_tokens: i64,
}

pub(crate) struct ModelTestLedgerContext<'a> {
    pub(crate) pool: &'a PgPool,
    pub(crate) request_id: Uuid,
    pub(crate) connection: &'a ModelConnectionDto,
    pub(crate) model_id: &'a str,
    pub(crate) request_settings: &'a ModelRequestSettings,
    pub(crate) user: &'a UserDto,
}

pub(crate) fn extract_model_usage(response: &Value) -> Option<ObservedModelUsage> {
    let usage = response.get("usage")?;
    let observed = ObservedModelUsage {
        input_tokens: usage.get("input_tokens")?.as_i64()?,
        output_tokens: usage.get("output_tokens")?.as_i64()?,
        total_tokens: usage.get("total_tokens")?.as_i64()?,
        cached_tokens: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0),
    };
    (observed.input_tokens >= 0
        && observed.output_tokens >= 0
        && observed.total_tokens == observed.input_tokens + observed.output_tokens
        && (0..=observed.input_tokens).contains(&observed.cached_tokens)
        && (0..=observed.output_tokens).contains(&observed.reasoning_tokens))
    .then_some(observed)
}

/// Chat Completions usage：`prompt_tokens` / `completion_tokens` / `total_tokens`。
pub(crate) fn extract_chat_usage(response: &Value) -> Option<ObservedModelUsage> {
    let usage = response.get("usage")?;
    let input_tokens = usage.get("prompt_tokens")?.as_i64()?;
    let output_tokens = usage.get("completion_tokens")?.as_i64()?;
    let total_tokens = usage
        .get("total_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(input_tokens + output_tokens);
    let cached_tokens = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let reasoning_tokens = usage
        .pointer("/completion_tokens_details/reasoning_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    (input_tokens >= 0
        && output_tokens >= 0
        && total_tokens == input_tokens + output_tokens
        && (0..=input_tokens).contains(&cached_tokens)
        && (0..=output_tokens).contains(&reasoning_tokens))
    .then_some(ObservedModelUsage {
        input_tokens,
        output_tokens,
        total_tokens,
        cached_tokens,
        reasoning_tokens,
    })
}

/// Anthropic Messages usage：总 input = input_tokens + cache_creation + cache_read，
/// cached_tokens 只记 cache_read（命中），creation 计入 input 不计命中。
pub(crate) fn extract_anthropic_usage(response: &Value) -> Option<ObservedModelUsage> {
    let usage = response.get("usage")?;
    let raw_input = usage.get("input_tokens")?.as_i64()?;
    let output_tokens = usage.get("output_tokens")?.as_i64()?;
    let creation = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let read = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let input_tokens = raw_input + creation + read;
    let reasoning_tokens = usage
        .pointer("/output_tokens_details/thinking_tokens")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    (input_tokens >= 0
        && output_tokens >= 0
        && (0..=input_tokens).contains(&read)
        && (0..=output_tokens).contains(&reasoning_tokens))
    .then_some(ObservedModelUsage {
        input_tokens,
        output_tokens,
        total_tokens: input_tokens + output_tokens,
        cached_tokens: read,
        reasoning_tokens,
    })
}

pub(crate) fn model_response_status(response: Option<&Value>, http_success: bool) -> &'static str {
    match response
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
    {
        Some("completed") => "completed",
        Some("failed") => "failed",
        Some("incomplete") => "incomplete",
        Some("cancelled") => "cancelled",
        _ if http_success => "completed",
        _ => "failed",
    }
}

/// 按上游协议归一化终态，与 Runtime 旁路记账的映射保持一致：
/// Chat 看 `choices[0].finish_reason`，Anthropic 看 `stop_reason`。
pub(crate) fn model_response_status_for(
    response: Option<&Value>,
    http_success: bool,
    protocol: ModelUpstreamProtocol,
) -> &'static str {
    match protocol {
        ModelUpstreamProtocol::OpenaiResponses => model_response_status(response, http_success),
        ModelUpstreamProtocol::OpenaiChatCompletions => response
            .and_then(|value| value.pointer("/choices/0/finish_reason"))
            .and_then(Value::as_str)
            .map(chat_finish_reason_status)
            .unwrap_or(if http_success { "completed" } else { "failed" }),
        ModelUpstreamProtocol::AnthropicMessages => response
            .and_then(|value| value.get("stop_reason"))
            .and_then(Value::as_str)
            .map(anthropic_stop_reason_status)
            .unwrap_or(if http_success { "completed" } else { "failed" }),
    }
}

pub(crate) async fn record_model_test_usage(
    context: &ModelTestLedgerContext<'_>,
    response_status: &str,
    usage: &ObservedModelUsage,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO model_token_usage
             (id, request_id, response_status, model_connection_id,
              model_connection_scope_snapshot, model_connection_name_snapshot,
              model_id_snapshot, api_type_snapshot,
              request_settings_snapshot,
              agent_id, agent_name_snapshot,
              subject_type, subject_user_id, subject_display_name_snapshot,
              input_tokens, output_tokens, total_tokens, cached_tokens,
              reasoning_tokens)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL,
                 'user', $10, $11, $12, $13, $14, $15, $16)",
    )
    .bind(Uuid::new_v4())
    .bind(context.request_id)
    .bind(response_status)
    .bind(context.connection.id)
    .bind(model_connection_scope_name(context.connection.scope))
    .bind(&context.connection.name)
    .bind(context.model_id)
    .bind(model_upstream_protocol_name(context.connection.api_type))
    .bind(model_request_settings_value(context.request_settings))
    .bind(context.user.id)
    .bind(&context.user.display_name)
    .bind(usage.input_tokens)
    .bind(usage.output_tokens)
    .bind(usage.total_tokens)
    .bind(usage.cached_tokens)
    .bind(usage.reasoning_tokens)
    .execute(context.pool)
    .await?;
    Ok(())
}

pub(crate) async fn record_model_test_error(
    context: &ModelTestLedgerContext<'_>,
    response_status: &str,
    upstream_http_status: Option<u16>,
    error_kind: &str,
    error_code: Option<&str>,
    message: &str,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO model_call_errors
             (id, request_id, response_status, upstream_http_status,
              error_kind, error_code, message, model_connection_id,
              model_connection_scope_snapshot, model_connection_name_snapshot,
              model_id_snapshot, api_type_snapshot,
              request_settings_snapshot,
              agent_id, agent_name_snapshot,
              subject_type, subject_user_id, subject_display_name_snapshot)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                 $13, NULL, NULL, 'user', $14, $15)",
    )
    .bind(Uuid::new_v4())
    .bind(context.request_id)
    .bind(response_status)
    .bind(upstream_http_status.map(i32::from))
    .bind(error_kind)
    .bind(error_code.map(|value| value.chars().take(256).collect::<String>()))
    .bind(message.chars().take(2048).collect::<String>())
    .bind(context.connection.id)
    .bind(model_connection_scope_name(context.connection.scope))
    .bind(&context.connection.name)
    .bind(context.model_id)
    .bind(model_upstream_protocol_name(context.connection.api_type))
    .bind(model_request_settings_value(context.request_settings))
    .bind(context.user.id)
    .bind(&context.user.display_name)
    .execute(context.pool)
    .await?;
    Ok(())
}

pub(crate) fn model_connection_scope_name(scope: ModelConnectionScope) -> &'static str {
    match scope {
        ModelConnectionScope::Global => "global",
        ModelConnectionScope::Personal => "personal",
    }
}

pub(crate) fn model_upstream_protocol_name(protocol: ModelUpstreamProtocol) -> &'static str {
    match protocol {
        ModelUpstreamProtocol::OpenaiResponses => "openai_responses",
        ModelUpstreamProtocol::OpenaiChatCompletions => "openai_chat_completions",
        ModelUpstreamProtocol::AnthropicMessages => "anthropic_messages",
    }
}

pub(crate) fn model_upstream_protocol_from_name(value: &str) -> ModelUpstreamProtocol {
    match value {
        "openai_responses" => ModelUpstreamProtocol::OpenaiResponses,
        "openai_chat_completions" => ModelUpstreamProtocol::OpenaiChatCompletions,
        "anthropic_messages" => ModelUpstreamProtocol::AnthropicMessages,
        _ => unreachable!("model upstream protocol is constrained"),
    }
}

pub(crate) fn sanitize_model_error_message(message: &str, secret: &str) -> String {
    message
        .replace(secret, REDACTED_SECRET)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(2048)
        .collect::<String>()
        .trim()
        .to_owned()
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedModelLedgerQuery {
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) to: Option<DateTime<Utc>>,
    pub(crate) model_connection_id: Option<Uuid>,
    pub(crate) agent_id: Option<Uuid>,
    pub(crate) user_id: Option<Uuid>,
    pub(crate) cursor_occurred_at: Option<DateTime<Utc>>,
    pub(crate) cursor_id: Option<Uuid>,
    pub(crate) page_size: i64,
}

pub(crate) struct ModelLedgerQueryInput {
    pub(crate) from_ms: Option<i64>,
    pub(crate) to_ms: Option<i64>,
    pub(crate) model_connection_id: Option<Uuid>,
    pub(crate) agent_id: Option<Uuid>,
    pub(crate) user_id: Option<Uuid>,
    pub(crate) cursor_occurred_at_ms: Option<i64>,
    pub(crate) cursor_id: Option<Uuid>,
    pub(crate) page_size: Option<u32>,
}

impl From<ModelTokenUsageQueryDto> for ModelLedgerQueryInput {
    fn from(query: ModelTokenUsageQueryDto) -> Self {
        Self {
            from_ms: query.from_ms,
            to_ms: query.to_ms,
            model_connection_id: query.model_connection_id,
            agent_id: query.agent_id,
            user_id: query.user_id,
            cursor_occurred_at_ms: query.cursor_occurred_at_ms,
            cursor_id: query.cursor_id,
            page_size: query.page_size,
        }
    }
}

impl From<ModelCallErrorQueryDto> for ModelLedgerQueryInput {
    fn from(query: ModelCallErrorQueryDto) -> Self {
        Self {
            from_ms: query.from_ms,
            to_ms: query.to_ms,
            model_connection_id: query.model_connection_id,
            agent_id: query.agent_id,
            user_id: query.user_id,
            cursor_occurred_at_ms: query.cursor_occurred_at_ms,
            cursor_id: query.cursor_id,
            page_size: query.page_size,
        }
    }
}

pub(crate) fn validate_model_ledger_query(
    query: ModelLedgerQueryInput,
    user: &UserDto,
) -> Result<ValidatedModelLedgerQuery, ApiError> {
    let parse_ms = |value: Option<i64>, name: &str| {
        value
            .map(|value| {
                DateTime::<Utc>::from_timestamp_millis(value)
                    .ok_or_else(|| ApiError::bad_request(format!("invalid {name}")))
            })
            .transpose()
    };
    let from = parse_ms(query.from_ms, "from_ms")?;
    let to = parse_ms(query.to_ms, "to_ms")?;
    if from.zip(to).is_some_and(|(from, to)| from >= to) {
        return Err(ApiError::bad_request("from_ms must be earlier than to_ms"));
    }
    let (cursor_occurred_at, cursor_id) = match (query.cursor_occurred_at_ms, query.cursor_id) {
        (None, None) => (None, None),
        (Some(occurred_at_ms), Some(id)) => (
            Some(
                DateTime::<Utc>::from_timestamp_millis(occurred_at_ms)
                    .ok_or(ApiError::bad_request("invalid cursor_occurred_at_ms"))?,
            ),
            Some(id),
        ),
        _ => {
            return Err(ApiError::bad_request(
                "cursor_occurred_at_ms and cursor_id must be provided together",
            ))
        }
    };
    if !is_admin_role(&user.role)
        && query
            .user_id
            .is_some_and(|requested_user_id| requested_user_id != user.id)
    {
        return Err(ApiError::forbidden(
            "another user's model usage is not visible",
        ));
    }
    let page_size = i64::from(query.page_size.unwrap_or(50));
    if !(1..=100).contains(&page_size) {
        return Err(ApiError::bad_request(
            "model ledger page_size must be between 1 and 100",
        ));
    }
    Ok(ValidatedModelLedgerQuery {
        from,
        to,
        model_connection_id: query.model_connection_id,
        agent_id: query.agent_id,
        user_id: query.user_id,
        cursor_occurred_at,
        cursor_id,
        page_size,
    })
}

pub(crate) fn model_ledger_source(table: &str, include_owned_agent_aggregates: bool) -> String {
    let member_visibility = if include_owned_agent_aggregates {
        "(ledger.subject_user_id = $1 OR agent.owner_id = $1)"
    } else {
        "ledger.subject_user_id = $1"
    };
    format!(
        "FROM {table} AS ledger
         LEFT JOIN users AS subject_user ON subject_user.id = ledger.subject_user_id
         LEFT JOIN agents AS agent ON agent.id = ledger.agent_id
         LEFT JOIN users AS agent_owner ON agent_owner.id = agent.owner_id
         LEFT JOIN model_connections AS model
           ON model.id = ledger.model_connection_id
         LEFT JOIN users AS model_owner ON model_owner.id = model.owner_id
         LEFT JOIN oauth_apps AS source_app
           ON source_app.id = ledger.source_integration_app_id
         LEFT JOIN users AS source_app_owner
           ON source_app_owner.id = source_app.owner_id
         WHERE ($3::timestamptz IS NULL OR ledger.occurred_at >= $3)
           AND ($4::timestamptz IS NULL OR ledger.occurred_at < $4)
           AND ($5::uuid IS NULL OR ledger.model_connection_id = $5)
           AND ($6::uuid IS NULL OR ledger.agent_id = $6)
           AND ($7::uuid IS NULL OR ledger.subject_user_id = $7)
           AND (
               $2 = 'super_admin'
               OR (
                   $2 = 'admin'
                   AND ledger.super_admin_protected = false
                   AND COALESCE(subject_user.role, '') <> 'super_admin'
                   AND COALESCE(agent_owner.role, '') <> 'super_admin'
                   AND COALESCE(model_owner.role, '') <> 'super_admin'
                   AND COALESCE(source_app_owner.role, '') <> 'super_admin'
               )
               OR ($2 NOT IN ('admin', 'super_admin') AND {member_visibility})
           )"
    )
}

pub(crate) async fn get_model_usage_summary(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ModelTokenUsageQueryDto>,
) -> Result<Json<ModelUsageSummaryDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let query = validate_model_ledger_query(query.into(), &user)?;
    let source = model_ledger_source("model_token_usage", true);
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;

    let overall_sql = format!(
        "SELECT COALESCE(sum(ledger.input_tokens), 0)::bigint AS input_tokens,
                COALESCE(sum(ledger.output_tokens), 0)::bigint AS output_tokens,
                COALESCE(sum(ledger.total_tokens), 0)::bigint AS total_tokens,
                COALESCE(sum(ledger.cached_tokens), 0)::bigint AS cached_tokens,
                COALESCE(sum(ledger.reasoning_tokens), 0)::bigint AS reasoning_tokens
         {source}"
    );
    let overall = sqlx::query(&overall_sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .fetch_one(&mut *tx)
        .await?;

    let by_model_sql = format!(
        "SELECT ledger.model_connection_id,
                ledger.model_connection_scope_snapshot,
                ledger.model_connection_name_snapshot,
                ledger.model_id_snapshot,
                ledger.api_type_snapshot,
                ledger.request_settings_snapshot,
                sum(ledger.input_tokens)::bigint AS input_tokens,
                sum(ledger.output_tokens)::bigint AS output_tokens,
                sum(ledger.total_tokens)::bigint AS total_tokens,
                sum(ledger.cached_tokens)::bigint AS cached_tokens,
                sum(ledger.reasoning_tokens)::bigint AS reasoning_tokens
         {source}
         GROUP BY ledger.model_connection_id,
                  ledger.model_connection_scope_snapshot,
                  ledger.model_connection_name_snapshot,
                  ledger.model_id_snapshot,
                  ledger.api_type_snapshot,
                  ledger.request_settings_snapshot
         ORDER BY total_tokens DESC, ledger.model_connection_name_snapshot,
                  ledger.model_id_snapshot, ledger.model_connection_id NULLS LAST"
    );
    let by_model = sqlx::query(&by_model_sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| ModelUsageModelSummaryDto {
            model: model_connection_snapshot_from_row(&row),
            totals: model_token_totals_from_row(&row),
        })
        .collect();

    let by_agent_sql = format!(
        "SELECT ledger.agent_id,
                COALESCE(ledger.agent_name_snapshot, 'Model Connection test')
                    AS agent_name_snapshot,
                sum(ledger.input_tokens)::bigint AS input_tokens,
                sum(ledger.output_tokens)::bigint AS output_tokens,
                sum(ledger.total_tokens)::bigint AS total_tokens,
                sum(ledger.cached_tokens)::bigint AS cached_tokens,
                sum(ledger.reasoning_tokens)::bigint AS reasoning_tokens
         {source}
         GROUP BY ledger.agent_id,
                  COALESCE(ledger.agent_name_snapshot, 'Model Connection test')
         ORDER BY total_tokens DESC, agent_name_snapshot, ledger.agent_id NULLS LAST"
    );
    let by_agent = sqlx::query(&by_agent_sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| ModelUsageAgentSummaryDto {
            agent: ModelAgentSnapshotDto {
                id: row.get("agent_id"),
                name: row.get("agent_name_snapshot"),
            },
            totals: model_token_totals_from_row(&row),
        })
        .collect();

    let by_user_sql = format!(
        "SELECT CASE
                    WHEN $2 NOT IN ('admin', 'super_admin')
                         AND ledger.subject_user_id IS DISTINCT FROM $1
                    THEN NULL
                    ELSE ledger.subject_user_id
                END AS grouped_user_id,
                CASE
                    WHEN $2 NOT IN ('admin', 'super_admin')
                         AND ledger.subject_user_id IS DISTINCT FROM $1
                    THEN NULL
                    ELSE ledger.subject_display_name_snapshot
                END AS grouped_display_name,
                sum(ledger.input_tokens)::bigint AS input_tokens,
                sum(ledger.output_tokens)::bigint AS output_tokens,
                sum(ledger.total_tokens)::bigint AS total_tokens,
                sum(ledger.cached_tokens)::bigint AS cached_tokens,
                sum(ledger.reasoning_tokens)::bigint AS reasoning_tokens
         {source}
           AND ledger.subject_type = 'user'
         GROUP BY grouped_user_id, grouped_display_name
         ORDER BY total_tokens DESC, grouped_display_name NULLS LAST,
                  grouped_user_id NULLS LAST"
    );
    let by_user = sqlx::query(&by_user_sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| ModelUsageUserSummaryDto {
            user_id: row.get("grouped_user_id"),
            display_name: row.get("grouped_display_name"),
            totals: model_token_totals_from_row(&row),
        })
        .collect();
    tx.commit().await?;

    Ok(Json(ModelUsageSummaryDto {
        overall: model_token_totals_from_row(&overall),
        by_model,
        by_agent,
        by_user,
    }))
}

pub(crate) async fn list_model_token_usage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ModelTokenUsageQueryDto>,
) -> Result<Json<ModelTokenUsagePageDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let query = validate_model_ledger_query(query.into(), &user)?;
    let source = model_ledger_source("model_token_usage", false);
    let sql = format!(
        "SELECT ledger.id, ledger.occurred_at, ledger.response_status,
                ledger.model_connection_id,
                ledger.model_connection_scope_snapshot,
                ledger.model_connection_name_snapshot, ledger.model_id_snapshot,
                ledger.api_type_snapshot, ledger.request_settings_snapshot,
                ledger.agent_id, ledger.agent_name_snapshot,
                ledger.subject_type, ledger.subject_user_id,
                ledger.subject_display_name_snapshot,
                ledger.source_integration_app_id,
                ledger.source_integration_app_name_snapshot,
                ledger.input_tokens, ledger.output_tokens, ledger.total_tokens,
                ledger.cached_tokens, ledger.reasoning_tokens
         {source}
           AND ($8::timestamptz IS NULL
                OR (ledger.occurred_at, ledger.id) < ($8, $9))
         ORDER BY ledger.occurred_at DESC, ledger.id DESC
         LIMIT $10"
    );
    let mut rows = sqlx::query(&sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .bind(query.cursor_occurred_at)
        .bind(query.cursor_id)
        .bind(query.page_size + 1)
        .fetch_all(&state.pool)
        .await?;
    let has_more = rows.len() > query.page_size as usize;
    if has_more {
        rows.pop();
    }
    let items = rows
        .into_iter()
        .map(model_token_usage_from_row)
        .collect::<Vec<_>>();
    let next_cursor = has_more.then(|| {
        let last = items.last().expect("a page with more rows is non-empty");
        ModelLedgerCursorDto {
            occurred_at_ms: last.occurred_at.timestamp_millis(),
            id: last.id,
        }
    });
    Ok(Json(ModelTokenUsagePageDto { items, next_cursor }))
}

pub(crate) async fn list_model_call_errors(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ModelCallErrorQueryDto>,
) -> Result<Json<ModelCallErrorPageDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let query = validate_model_ledger_query(query.into(), &user)?;
    let source = model_ledger_source("model_call_errors", false);
    let sql = format!(
        "SELECT ledger.id, ledger.occurred_at, ledger.response_status,
                ledger.upstream_http_status, ledger.error_code, ledger.message,
                ledger.model_connection_id,
                ledger.model_connection_scope_snapshot,
                ledger.model_connection_name_snapshot, ledger.model_id_snapshot,
                ledger.api_type_snapshot, ledger.request_settings_snapshot,
                ledger.agent_id, ledger.agent_name_snapshot,
                ledger.subject_type, ledger.subject_user_id,
                ledger.subject_display_name_snapshot,
                ledger.source_integration_app_id,
                ledger.source_integration_app_name_snapshot
         {source}
           AND ($8::timestamptz IS NULL
                OR (ledger.occurred_at, ledger.id) < ($8, $9))
         ORDER BY ledger.occurred_at DESC, ledger.id DESC
         LIMIT $10"
    );
    let mut rows = sqlx::query(&sql)
        .bind(user.id)
        .bind(&user.role)
        .bind(query.from)
        .bind(query.to)
        .bind(query.model_connection_id)
        .bind(query.agent_id)
        .bind(query.user_id)
        .bind(query.cursor_occurred_at)
        .bind(query.cursor_id)
        .bind(query.page_size + 1)
        .fetch_all(&state.pool)
        .await?;
    let has_more = rows.len() > query.page_size as usize;
    if has_more {
        rows.pop();
    }
    let items = rows
        .into_iter()
        .map(model_call_error_from_row)
        .collect::<Vec<_>>();
    let next_cursor = has_more.then(|| {
        let last = items.last().expect("a page with more rows is non-empty");
        ModelLedgerCursorDto {
            occurred_at_ms: last.occurred_at.timestamp_millis(),
            id: last.id,
        }
    });
    Ok(Json(ModelCallErrorPageDto { items, next_cursor }))
}

pub(crate) fn model_token_totals_from_row(row: &sqlx::postgres::PgRow) -> ModelTokenUsageTotalsDto {
    ModelTokenUsageTotalsDto {
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        total_tokens: row.get("total_tokens"),
        cached_tokens: row.get("cached_tokens"),
        reasoning_tokens: row.get("reasoning_tokens"),
    }
}

pub(crate) fn model_connection_snapshot_from_row(
    row: &sqlx::postgres::PgRow,
) -> ModelConnectionSnapshotDto {
    ModelConnectionSnapshotDto {
        id: row.get("model_connection_id"),
        scope: match row
            .get::<String, _>("model_connection_scope_snapshot")
            .as_str()
        {
            "global" => ModelConnectionScope::Global,
            "personal" => ModelConnectionScope::Personal,
            _ => unreachable!("model ledger scope is constrained"),
        },
        name: row.get("model_connection_name_snapshot"),
        model_id: row.get("model_id_snapshot"),
        api_type: model_upstream_protocol_from_name(&row.get::<String, _>("api_type_snapshot")),
        request_settings: serde_json::from_value(row.get("request_settings_snapshot"))
            .expect("model ledger request settings are constrained"),
    }
}

pub(crate) fn model_agent_snapshot_from_row(row: &sqlx::postgres::PgRow) -> ModelAgentSnapshotDto {
    ModelAgentSnapshotDto {
        id: row.get("agent_id"),
        name: row
            .get::<Option<String>, _>("agent_name_snapshot")
            .unwrap_or_else(|| "Model Connection test".into()),
    }
}

pub(crate) fn model_usage_subject_from_row(row: &sqlx::postgres::PgRow) -> ModelUsageSubjectDto {
    match row.get::<String, _>("subject_type").as_str() {
        "user" => ModelUsageSubjectDto {
            kind: ModelUsageSubjectKind::User,
            id: row.get("subject_user_id"),
            display_name: row.get("subject_display_name_snapshot"),
        },
        "integration_app" => ModelUsageSubjectDto {
            kind: ModelUsageSubjectKind::IntegrationApp,
            id: row.get("source_integration_app_id"),
            display_name: row.get("source_integration_app_name_snapshot"),
        },
        "system" => ModelUsageSubjectDto {
            kind: ModelUsageSubjectKind::System,
            id: None,
            display_name: row.get("subject_display_name_snapshot"),
        },
        _ => unreachable!("model ledger subject type is constrained"),
    }
}

pub(crate) fn model_token_usage_from_row(row: sqlx::postgres::PgRow) -> ModelTokenUsageDto {
    ModelTokenUsageDto {
        id: row.get("id"),
        occurred_at: row.get("occurred_at"),
        response_status: row.get("response_status"),
        model: model_connection_snapshot_from_row(&row),
        agent: model_agent_snapshot_from_row(&row),
        subject: model_usage_subject_from_row(&row),
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        total_tokens: row.get("total_tokens"),
        cached_tokens: row.get("cached_tokens"),
        reasoning_tokens: row.get("reasoning_tokens"),
    }
}

pub(crate) fn model_call_error_from_row(row: sqlx::postgres::PgRow) -> ModelCallErrorDto {
    ModelCallErrorDto {
        id: row.get("id"),
        occurred_at: row.get("occurred_at"),
        response_status: row.get("response_status"),
        model: model_connection_snapshot_from_row(&row),
        agent: model_agent_snapshot_from_row(&row),
        subject: model_usage_subject_from_row(&row),
        upstream_status: row
            .get::<Option<i32>, _>("upstream_http_status")
            .map(|status| status as u16),
        error_code: row.get("error_code"),
        message: row.get::<String, _>("message").into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use std::time::Duration;

    use crate::{build_router, DEFAULT_MODEL_PROXY_TIMEOUT, VISION_PROXY_HEADER};
    use agent_hub_backend::ModelSecretCipher;
    use axum::{
        body::{Body, Bytes},
        http::{header, HeaderName, HeaderValue, Method},
        response::{IntoResponse, Response},
        routing::post,
        Router,
    };
    use base64::Engine;
    use chrono::Duration as ChronoDuration;
    use futures_util::StreamExt;
    use tower::ServiceExt;

    #[test]
    fn model_attribution_distinguishes_user_and_app_only_integration_calls() {
        let represented_user_id = Uuid::new_v4();
        let app_id = Uuid::new_v4();
        let mut principal = IntegrationPrincipal {
            oauth_app_id: app_id,
            grant_type: "authorization_code".into(),
            subject_user_id: Some(represented_user_id),
            agent_id: Uuid::new_v4(),
            agent_owner_id: Uuid::new_v4(),
            external_platform_id: Uuid::new_v4(),
            authentication_channel_id: Uuid::new_v4(),
            origin_tenant_id: Some("tenant".into()),
            origin_external_identity_id: Some(Uuid::new_v4()),
        };
        let user = integration_run_model_attribution(&principal);
        assert_eq!(user.subject_type, "user");
        assert_eq!(user.subject_user_id, Some(represented_user_id));
        assert_eq!(user.source_integration_app_id, Some(app_id));

        principal.grant_type = "client_credentials".into();
        principal.subject_user_id = None;
        let app = integration_run_model_attribution(&principal);
        assert_eq!(app.subject_type, "integration_app");
        assert_eq!(app.subject_user_id, None);
        assert_eq!(app.source_integration_app_id, Some(app_id));
    }

    #[test]
    fn model_ledger_query_validation_rejects_invalid_ranges_cursors_and_foreign_users() {
        let user = test_user(Uuid::new_v4(), "member");
        let invalid_range = validate_model_ledger_query(
            ModelLedgerQueryInput {
                from_ms: Some(10),
                to_ms: Some(10),
                model_connection_id: None,
                agent_id: None,
                user_id: None,
                cursor_occurred_at_ms: None,
                cursor_id: None,
                page_size: None,
            },
            &user,
        )
        .unwrap_err();
        assert_eq!(invalid_range.status, StatusCode::BAD_REQUEST);

        let invalid_cursor = validate_model_ledger_query(
            ModelLedgerQueryInput {
                from_ms: None,
                to_ms: None,
                model_connection_id: None,
                agent_id: None,
                user_id: None,
                cursor_occurred_at_ms: Some(10),
                cursor_id: None,
                page_size: None,
            },
            &user,
        )
        .unwrap_err();
        assert_eq!(invalid_cursor.status, StatusCode::BAD_REQUEST);

        let foreign_user = validate_model_ledger_query(
            ModelLedgerQueryInput {
                from_ms: None,
                to_ms: None,
                model_connection_id: None,
                agent_id: None,
                user_id: Some(Uuid::new_v4()),
                cursor_occurred_at_ms: None,
                cursor_id: None,
                page_size: None,
            },
            &user,
        )
        .unwrap_err();
        assert_eq!(foreign_user.status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn model_request_settings_validate_protocol_and_ranges() {
        let chat: ModelRequestSettings = serde_json::from_value(json!({
            "protocol": "openai_chat_completions",
            "temperature": 1.5,
            "top_p": 0.8,
            "max_completion_tokens": 4096
        }))
        .unwrap();
        assert!(validate_model_request_settings(
            ModelUpstreamProtocol::OpenaiChatCompletions,
            chat,
        )
        .is_ok());

        let mismatched = validate_model_request_settings(
            ModelUpstreamProtocol::AnthropicMessages,
            ModelRequestSettings::default(),
        )
        .unwrap_err();
        assert_eq!(mismatched.status, StatusCode::BAD_REQUEST);

        let invalid_temperature: ModelRequestSettings = serde_json::from_value(json!({
            "protocol": "anthropic_messages",
            "temperature": 1.1,
            "top_p": null,
            "max_tokens": null
        }))
        .unwrap();
        let invalid_temperature = validate_model_request_settings(
            ModelUpstreamProtocol::AnthropicMessages,
            invalid_temperature,
        )
        .unwrap_err();
        assert_eq!(invalid_temperature.status, StatusCode::BAD_REQUEST);

        let mutually_exclusive_sampling: ModelRequestSettings = serde_json::from_value(json!({
            "protocol": "anthropic_messages",
            "temperature": 0.4,
            "top_p": 0.9,
            "max_tokens": 4096
        }))
        .unwrap();
        let mutually_exclusive_sampling = validate_model_request_settings(
            ModelUpstreamProtocol::AnthropicMessages,
            mutually_exclusive_sampling,
        )
        .unwrap_err();
        assert_eq!(mutually_exclusive_sampling.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn model_proxy_only_supports_responses_path() {
        for supported in ["responses", "chat/completions", "messages"] {
            assert!(model_proxy_path_supported(supported));
        }
        for unsupported in ["models", "embeddings", "completions", ""] {
            assert!(!model_proxy_path_supported(unsupported));
        }
        assert_eq!(
            model_proxy_protocol_from_path("responses"),
            Some(ModelUpstreamProtocol::OpenaiResponses)
        );
        assert_eq!(
            model_proxy_protocol_from_path("chat/completions"),
            Some(ModelUpstreamProtocol::OpenaiChatCompletions)
        );
        assert_eq!(
            model_proxy_protocol_from_path("messages"),
            Some(ModelUpstreamProtocol::AnthropicMessages)
        );
        assert_eq!(model_proxy_protocol_from_path("models"), None);
    }

    #[test]
    fn model_proxy_timeout_configuration_is_positive_and_bounded() {
        assert_eq!(
            parse_model_proxy_timeout(None).unwrap(),
            DEFAULT_MODEL_PROXY_TIMEOUT
        );
        assert_eq!(
            parse_model_proxy_timeout(Some(" 30 ")).unwrap(),
            Duration::from_secs(30)
        );
        assert!(parse_model_proxy_timeout(Some("0")).is_err());
        assert!(parse_model_proxy_timeout(Some("901")).is_err());
        assert!(parse_model_proxy_timeout(Some("forever")).is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn model_dev_seed_is_encrypted_idempotent_and_system_default(pool: PgPool) {
        let encoded_key = base64::engine::general_purpose::STANDARD.encode([42_u8; 32]);
        let cipher = ModelSecretCipher::from_env_value(Some(&encoded_key)).unwrap();
        let first = seed_dev_model_connection(
            &pool,
            &cipher,
            "http://fake-model-provider:8080",
            vec!["hub-proxy-smoke".into(), "hub-proxy-smoke-fast".into()],
            "development-provider-key",
        )
        .await
        .unwrap();
        let second = seed_dev_model_connection(
            &pool,
            &cipher,
            "http://fake-model-provider:8080",
            vec!["hub-proxy-smoke".into(), "hub-proxy-smoke-fast".into()],
            "development-provider-key",
        )
        .await
        .unwrap();

        assert_eq!(first, second);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_connections WHERE name = 'Compose Responses'",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        let row = sqlx::query(
            "SELECT scope, owner_id, base_url, api_type, allowed_model_ids,
                    enabled, deleted_at, api_key_ciphertext, api_key_nonce
             FROM model_connections WHERE id = $1",
        )
        .bind(first)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("scope"), "global");
        assert_eq!(row.get::<Option<Uuid>, _>("owner_id"), None);
        assert_eq!(
            row.get::<String, _>("base_url"),
            "http://fake-model-provider:8080"
        );
        assert_eq!(row.get::<String, _>("api_type"), "openai_responses");
        assert_eq!(
            row.get::<Vec<String>, _>("allowed_model_ids"),
            vec!["hub-proxy-smoke", "hub-proxy-smoke-fast"]
        );
        assert!(row.get::<bool, _>("enabled"));
        assert_eq!(row.get::<Option<DateTime<Utc>>, _>("deleted_at"), None);
        assert_eq!(
            cipher
                .decrypt(
                    &row.get::<Vec<u8>, _>("api_key_ciphertext"),
                    &row.get::<Vec<u8>, _>("api_key_nonce"),
                )
                .unwrap(),
            "development-provider-key"
        );
        assert_eq!(
            sqlx::query_as::<_, (Uuid, String)>(
                "SELECT model_connection_id, model_id
                 FROM system_default_model_selection WHERE singleton = true",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            (first, "hub-proxy-smoke".into())
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_routes_selected_connection_streams_and_records_usage(pool: PgPool) {
        #[derive(Clone)]
        struct CapturedRequest {
            uri: axum::http::Uri,
            headers: HeaderMap,
            body: Bytes,
        }

        let captured = Arc::new(std::sync::Mutex::new(None::<CapturedRequest>));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured_request = Arc::clone(&captured);
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/provider/v1/responses",
                post(move |uri: axum::http::Uri, headers: HeaderMap, body: Bytes| {
                    let captured_request = Arc::clone(&captured_request);
                    async move {
                        *captured_request.lock().unwrap() = Some(CapturedRequest {
                            uri,
                            headers,
                            body,
                        });
                        let stream = async_stream::stream! {
                            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                                b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
                            ));
                            tokio::time::sleep(Duration::from_millis(150)).await;
                            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                                b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":11,\"output_tokens\":7,\"total_tokens\":18,\"input_tokens_details\":{\"cached_tokens\":3},\"output_tokens_details\":{\"reasoning_tokens\":5}}}}\n\n",
                            ));
                            std::future::pending::<()>().await;
                        };
                        let mut response = Response::new(Body::from_stream(stream));
                        response.headers_mut().insert(
                            header::CONTENT_TYPE,
                            HeaderValue::from_static("text/event-stream"),
                        );
                        response.headers_mut().insert(
                            HeaderName::from_static("x-provider-trace"),
                            HeaderValue::from_static("trace-123"),
                        );
                        response.headers_mut().insert(
                            header::SET_COOKIE,
                            HeaderValue::from_static("provider_session=secret"),
                        );
                        response.headers_mut().insert(
                            header::AUTHORIZATION,
                            HeaderValue::from_static("Bearer provider-response-secret"),
                        );
                        response.headers_mut().insert(
                            HeaderName::from_static("x-api-key"),
                            HeaderValue::from_static("provider-response-secret"),
                        );
                        for name in [
                            "x-provider-token",
                            "x-provider-api-key",
                            "x-provider-api_key",
                            "x-provider-secret",
                        ] {
                            response.headers_mut().insert(
                                HeaderName::from_static(name),
                                HeaderValue::from_static("provider-response-secret"),
                            );
                        }
                        response.headers_mut().insert(
                            header::CONNECTION,
                            HeaderValue::from_static("keep-alive, x-provider-hop"),
                        );
                        response.headers_mut().insert(
                            HeaderName::from_static("x-provider-hop"),
                            HeaderValue::from_static("drop-me"),
                        );
                        response
                    }
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE model_connections SET base_url = $1 WHERE id = $2")
            .bind(format!("http://{address}/provider"))
            .bind(fixture.model_connection_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let main_binding_id = model_binding_id(&claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let original = Bytes::from_static(
            br#" { "model": "runtime-claim-model", "input": [], "stream": true } "#,
        );
        let state = (*fixture.state).clone();
        let app = build_router(state);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/runtime/model-proxy/v1/responses?include=usage&trace=1")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", claim.model_proxy_token),
                    )
                    .header("x-agent-hub-run-id", fixture.run_id.to_string())
                    .header(MODEL_PROXY_BINDING_ID_HEADER, main_binding_id.to_string())
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-client-feature", "preserve-me")
                    .header(header::COOKIE, "runtime_cookie=drop-me")
                    .header(header::CONNECTION, "x-client-hop")
                    .header("x-client-hop", "drop-me")
                    .body(Body::from(original.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        assert_eq!(response.headers()["x-provider-trace"], "trace-123");
        assert!(response.headers().get(header::SET_COOKIE).is_none());
        assert!(response.headers().get(header::AUTHORIZATION).is_none());
        assert!(response.headers().get("x-api-key").is_none());
        assert!(response.headers().get("x-provider-token").is_none());
        assert!(response.headers().get("x-provider-api-key").is_none());
        assert!(response.headers().get("x-provider-api_key").is_none());
        assert!(response.headers().get("x-provider-secret").is_none());
        assert!(response.headers().get(header::CONNECTION).is_none());
        assert!(response.headers().get("x-provider-hop").is_none());
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_millis(100), body.next())
            .await
            .expect("the first SSE chunk must not wait for terminal usage")
            .unwrap()
            .unwrap();
        assert!(first.starts_with(b"event: response.created"));
        let second = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(second.starts_with(b"event: response.completed"));
        drop(body);

        let captured = captured.lock().unwrap().clone().unwrap();
        assert_eq!(captured.uri.path(), "/provider/v1/responses");
        assert_eq!(captured.uri.query(), Some("include=usage&trace=1"));
        assert_eq!(
            captured.headers[header::AUTHORIZATION],
            "Bearer runtime-claim-secret"
        );
        assert_eq!(captured.body, original);
        assert_eq!(
            captured.headers.get("x-client-feature").unwrap(),
            "preserve-me"
        );
        for filtered in [
            "cookie",
            "x-agent-hub-run-id",
            MODEL_PROXY_BINDING_ID_HEADER,
            "x-client-hop",
        ] {
            assert!(!captured.headers.contains_key(filtered));
        }

        let usage: (Uuid, String, i64, i64, i64, i64, i64, Uuid) = sqlx::query_as(
            "SELECT request_id, response_status, input_tokens, output_tokens, total_tokens,
                    cached_tokens, reasoning_tokens, agent_id
             FROM model_token_usage WHERE model_connection_id = $1",
        )
        .bind(fixture.model_connection_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_ne!(usage.0, Uuid::nil());
        assert_eq!(
            (usage.1, usage.2, usage.3, usage.4, usage.5, usage.6, usage.7),
            ("completed".into(), 11, 7, 18, 3, 5, fixture.agent_id)
        );
        for table_and_column in [
            ("run_model_bindings", "model_settings->'request_settings'"),
            ("model_token_usage", "request_settings_snapshot"),
        ] {
            let sql = format!(
                "SELECT {} FROM {} WHERE model_connection_id = $1",
                table_and_column.1, table_and_column.0
            );
            let snapshot: Value = sqlx::query_scalar(&sql)
                .bind(fixture.model_connection_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
            assert_eq!(snapshot, json!({ "protocol": "openai_responses" }));
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_call_errors WHERE model_connection_id = $1",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_records_each_terminal_outcome_and_real_retry_once(pool: PgPool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/provider/v1/responses",
                post(|uri: axum::http::Uri, _body: Bytes| async move {
                    let case = uri
                        .query()
                        .and_then(|query| query.strip_prefix("case="))
                        .unwrap_or("retry");
                    if case == "stream_error" {
                        let mut response = Response::new(Body::from(
                            "event: response.created\ndata: {\"type\":\"response.created\"}\n\n\
                             event: error\ndata: {\"type\":\"error\",\"code\":\"stream_overloaded\",\"message\":\"provider stream failed\"}\n\n",
                        ));
                        response.headers_mut().insert(
                            header::CONTENT_TYPE,
                            HeaderValue::from_static("text/event-stream"),
                        );
                        return response;
                    }
                    let (status, value) = match case {
                        "failed" => (
                            StatusCode::OK,
                            json!({
                                "status": "failed",
                                "error": { "code": "provider_failed", "message": "provider rejected request" },
                                "usage": {
                                    "input_tokens": 2,
                                    "output_tokens": 1,
                                    "total_tokens": 3,
                                    "input_tokens_details": { "cached_tokens": 1 },
                                    "output_tokens_details": { "reasoning_tokens": 1 }
                                }
                            }),
                        ),
                        "incomplete" => (
                            StatusCode::OK,
                            json!({
                                "status": "incomplete",
                                "incomplete_details": { "reason": "max_output_tokens" }
                            }),
                        ),
                        "cancelled" => (
                            StatusCode::OK,
                            json!({
                                "status": "cancelled",
                                "usage": {
                                    "input_tokens": 4,
                                    "output_tokens": 2,
                                    "total_tokens": 6
                                }
                            }),
                        ),
                        "no_usage" => (
                            StatusCode::OK,
                            json!({ "status": "completed", "output": [] }),
                        ),
                        "rate" => (
                            StatusCode::TOO_MANY_REQUESTS,
                            json!({
                                "status": "failed",
                                "error": { "code": "rate_limit", "message": "request throttled" },
                                "usage": {
                                    "input_tokens": 5,
                                    "output_tokens": 3,
                                    "total_tokens": 8
                                }
                            }),
                        ),
                        _ => (
                            StatusCode::OK,
                            json!({
                                "status": "completed",
                                "usage": {
                                    "input_tokens": 1,
                                    "output_tokens": 1,
                                    "total_tokens": 2
                                }
                            }),
                        ),
                    };
                    let mut response = Json(value).into_response();
                    *response.status_mut() = status;
                    response
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE model_connections SET base_url = $1 WHERE id = $2")
            .bind(format!("http://{address}/provider"))
            .bind(fixture.model_connection_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let main_binding_id = model_binding_id(&claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let state = (*fixture.state).clone();
        let app = build_router(state);

        for (case, expected_status) in [
            ("failed", StatusCode::OK),
            ("incomplete", StatusCode::OK),
            ("cancelled", StatusCode::OK),
            ("no_usage", StatusCode::OK),
            ("rate", StatusCode::TOO_MANY_REQUESTS),
            ("stream_error", StatusCode::OK),
            ("retry", StatusCode::OK),
            ("retry", StatusCode::OK),
        ] {
            let response = model_proxy_test_http_request(
                &app,
                &fixture,
                &claim.model_proxy_token,
                main_binding_id,
                &format!("case={case}"),
                "runtime-claim-model",
            )
            .await;
            assert_eq!(response.status(), expected_status);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!body.is_empty());
        }

        let usage: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT response_status, count(*), sum(total_tokens)::bigint
             FROM model_token_usage
             WHERE model_connection_id = $1
             GROUP BY response_status
             ORDER BY response_status",
        )
        .bind(fixture.model_connection_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            usage,
            vec![
                ("cancelled".into(), 1, 6),
                ("completed".into(), 2, 4),
                ("failed".into(), 2, 11),
            ]
        );
        let errors: Vec<(String, i64)> = sqlx::query_as(
            "SELECT response_status, count(*)
             FROM model_call_errors
             WHERE model_connection_id = $1
             GROUP BY response_status
             ORDER BY response_status",
        )
        .bind(fixture.model_connection_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            errors,
            vec![
                ("cancelled".into(), 1),
                ("failed".into(), 3),
                ("incomplete".into(), 1),
                ("protocol_error".into(), 1),
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(DISTINCT request_id) FROM model_token_usage
                 WHERE model_connection_id = $1",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            5
        );
        let persisted_errors: String = sqlx::query_scalar(
            "SELECT string_agg(COALESCE(error_code, '') || ' ' || message, ' ')
             FROM model_call_errors WHERE model_connection_id = $1",
        )
        .bind(fixture.model_connection_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert!(!persisted_errors.contains("runtime-claim-secret"));
        assert!(persisted_errors.contains("stream_overloaded provider stream failed"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM (
                     SELECT api_type_snapshot FROM model_token_usage
                     WHERE model_connection_id = $1
                     UNION ALL
                     SELECT api_type_snapshot FROM model_call_errors
                     WHERE model_connection_id = $1
                 ) AS snapshots
                 WHERE api_type_snapshot <> 'openai_responses'",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_fails_closed_before_upstream_for_invalid_execution_scope(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let main_binding_id = model_binding_id(&claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let unassigned_binding_id = Uuid::new_v4();
        let app = build_router((*fixture.state).clone());

        let mismatch = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=mismatch",
            "another-model",
        )
        .await;
        assert_eq!(mismatch.status(), StatusCode::BAD_REQUEST);

        let bad_token = model_proxy_test_http_request(
            &app,
            &fixture,
            "invalid-token",
            main_binding_id,
            "case=bad-token",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(bad_token.status(), StatusCode::UNAUTHORIZED);

        let unassigned = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            unassigned_binding_id,
            "case=unassigned",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(unassigned.status(), StatusCode::UNAUTHORIZED);

        sqlx::query("UPDATE model_connections SET enabled = false WHERE id = $1")
            .bind(fixture.model_connection_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let disabled = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=disabled",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(disabled.status(), StatusCode::UNAUTHORIZED);
        sqlx::query("UPDATE model_connections SET enabled = true WHERE id = $1")
            .bind(fixture.model_connection_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        sqlx::query("UPDATE runs SET status = 'waiting_tool' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let inactive_run = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=inactive-run",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(inactive_run.status(), StatusCode::UNAUTHORIZED);
        sqlx::query("UPDATE runs SET status = 'running' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        sqlx::query("UPDATE runtimes SET status = 'offline' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let offline_runtime = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=offline-runtime",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(offline_runtime.status(), StatusCode::UNAUTHORIZED);
        sqlx::query(
            "UPDATE runtimes
             SET status = 'online', last_heartbeat_at = now() - interval '1 minute'
             WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let stale_runtime = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=stale-runtime",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(stale_runtime.status(), StatusCode::UNAUTHORIZED);

        let legacy_connection_header = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/runtime/model-proxy/v1/responses")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", claim.model_proxy_token),
                    )
                    .header("x-agent-hub-run-id", fixture.run_id.to_string())
                    .header(
                        "x-agent-hub-model-connection-id",
                        fixture.model_connection_id.to_string(),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(br#"{"model":"runtime-claim-model"}"#.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy_connection_header.status(), StatusCode::BAD_REQUEST);

        let unsupported = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/runtime/model-proxy/v1/models")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", claim.model_proxy_token),
                    )
                    .header("x-agent-hub-run-id", fixture.run_id.to_string())
                    .header(MODEL_PROXY_BINDING_ID_HEADER, main_binding_id.to_string())
                    .body(Body::from(br#"{"model":"runtime-claim-model"}"#.as_slice()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsupported.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_token_usage
                 WHERE model_connection_id = $1",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_call_errors
                 WHERE model_connection_id = $1",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_uses_turn_selection_snapshot_and_fresh_connection_secret(pool: PgPool) {
        let captured = Arc::new(std::sync::Mutex::new(
            None::<(HeaderMap, axum::http::Uri, Bytes)>,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured_request = Arc::clone(&captured);
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/rotated/v1/responses",
                post(
                    move |uri: axum::http::Uri, headers: HeaderMap, body: Bytes| {
                        let captured_request = Arc::clone(&captured_request);
                        async move {
                            *captured_request.lock().unwrap() = Some((headers, uri, body));
                            Json(json!({
                                "status": "completed",
                                "usage": {
                                    "input_tokens": 3,
                                    "output_tokens": 2,
                                    "total_tokens": 5
                                }
                            }))
                        }
                    },
                ),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let main_binding_id = model_binding_id(&claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let rotated_secret = fixture
            .state
            .model_secret_cipher
            .encrypt("rotated-provider-secret")
            .unwrap();
        sqlx::query(
            "UPDATE model_connections
             SET base_url = $1, api_key_ciphertext = $2, api_key_nonce = $3
             WHERE id = $4",
        )
        .bind(format!("http://{address}/rotated"))
        .bind(rotated_secret.ciphertext)
        .bind(rotated_secret.nonce)
        .bind(fixture.model_connection_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE agents
             SET model_connection_id = NULL, model_id = NULL,
                 execution_config_revision = execution_config_revision + 1
             WHERE id = $1",
        )
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let state = (*fixture.state).clone();
        let app = build_router(state);

        let current_turn = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=current-turn",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(current_turn.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(current_turn.into_body(), usize::MAX)
            .await
            .unwrap();
        let (headers, uri, body) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            headers[header::AUTHORIZATION],
            "Bearer rotated-provider-secret"
        );
        assert_eq!(uri.path(), "/rotated/v1/responses");
        let request_body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request_body["model"], "runtime-claim-model");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT model_id_snapshot FROM model_token_usage
                 WHERE model_connection_id = $1",
            )
            .bind(fixture.model_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "runtime-claim-model"
        );

        let future_model = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            main_binding_id,
            "case=future-turn",
            "rotated-model",
        )
        .await;
        assert_eq!(future_model.status(), StatusCode::BAD_REQUEST);
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_allows_only_claimed_subagent_override_connection(pool: PgPool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/subagent/v1/responses",
                post(|_body: Bytes| async {
                    Json(json!({
                        "status": "completed",
                        "usage": {
                            "input_tokens": 8,
                            "output_tokens": 5,
                            "total_tokens": 13
                        }
                    }))
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let subagent_connection_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO model_connections
                 (id, scope, owner_id, name, base_url, api_type, allowed_model_ids,
                  api_key_ciphertext, api_key_nonce, created_by)
             SELECT $1, scope, owner_id, 'Subagent Override Model', $2,
                    api_type, ARRAY['subagent-model'], api_key_ciphertext,
                    api_key_nonce, created_by
             FROM model_connections WHERE id = $3",
        )
        .bind(subagent_connection_id)
        .bind(format!("http://{address}/subagent"))
        .bind(fixture.model_connection_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO subagent_definitions
                 (id, agent_id, name, description, developer_instructions,
                  model_connection_id, model_id, model_settings_override)
             VALUES ($1, $2, 'reviewer', 'Reviews changes', 'Review carefully.',
                     $3, 'subagent-model', '{\"reasoning_effort\":\"high\"}'::jsonb)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.agent_id)
        .bind(subagent_connection_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let reviewer_binding_id = model_binding_id(&claim, "reviewer");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let state = (*fixture.state).clone();
        let app = build_router(state);

        let response = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            reviewer_binding_id,
            "case=subagent",
            "subagent-model",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (String, Option<Uuid>, i64)>(
                "SELECT model_id_snapshot, agent_id, total_tokens
                 FROM model_token_usage WHERE model_connection_id = $1",
            )
            .bind(subagent_connection_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("subagent-model".into(), Some(fixture.agent_id), 13)
        );

        sqlx::query("UPDATE model_connections SET enabled = false WHERE id = $1")
            .bind(subagent_connection_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let disabled = model_proxy_test_http_request(
            &app,
            &fixture,
            &claim.model_proxy_token,
            reviewer_binding_id,
            "case=disabled-subagent",
            "subagent-model",
        )
        .await;
        assert_eq!(disabled.status(), StatusCode::UNAUTHORIZED);
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_proxy_accounts_header_and_stream_transport_timeouts(pool: PgPool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new()
                .route(
                    "/header/v1/responses",
                    post(|_uri: axum::http::Uri| async move {
                        std::future::pending::<Response>().await
                    }),
                )
                .route(
                    "/body/v1/responses",
                    post(|_uri: axum::http::Uri| async move {
                        let stream = async_stream::stream! {
                            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                                b"event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
                            ));
                            std::future::pending::<()>().await;
                        };
                        let mut response = Response::new(Body::from_stream(stream));
                        response.headers_mut().insert(
                            header::CONTENT_TYPE,
                            HeaderValue::from_static("text/event-stream"),
                        );
                        response
                    }),
                );
            axum::serve(listener, app).await.unwrap();
        });

        let header_fixture =
            runtime_claim_fixture(pool.clone(), "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE model_connections SET base_url = $1 WHERE id = $2")
            .bind(format!("http://{address}/header"))
            .bind(header_fixture.model_connection_id)
            .execute(&header_fixture.state.pool)
            .await
            .unwrap();
        let header_claim =
            claim_runtime_run(&header_fixture.state, &header_fixture.runtime_token).await;
        let header_binding_id = model_binding_id(&header_claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(header_fixture.turn_id)
        .bind(header_fixture.hub_session_id)
        .execute(&header_fixture.state.pool)
        .await
        .unwrap();
        let mut header_state = (*header_fixture.state).clone();
        header_state.model_proxy_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(100))
            .timeout(Duration::from_millis(100))
            .read_timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let header_app = build_router(header_state);
        let header_timeout = model_proxy_test_http_request(
            &header_app,
            &header_fixture,
            &header_claim.model_proxy_token,
            header_binding_id,
            "case=header-timeout",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(header_timeout.status(), StatusCode::GATEWAY_TIMEOUT);
        let _ = axum::body::to_bytes(header_timeout.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (String, String, Option<i32>)>(
                "SELECT response_status, error_kind, upstream_http_status
                 FROM model_call_errors WHERE model_connection_id = $1",
            )
            .bind(header_fixture.model_connection_id)
            .fetch_one(&header_fixture.state.pool)
            .await
            .unwrap(),
            ("transport_error".into(), "timeout".into(), None)
        );

        let body_fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE model_connections SET base_url = $1 WHERE id = $2")
            .bind(format!("http://{address}/body"))
            .bind(body_fixture.model_connection_id)
            .execute(&body_fixture.state.pool)
            .await
            .unwrap();
        let body_claim = claim_runtime_run(&body_fixture.state, &body_fixture.runtime_token).await;
        let body_binding_id = model_binding_id(&body_claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(body_fixture.turn_id)
        .bind(body_fixture.hub_session_id)
        .execute(&body_fixture.state.pool)
        .await
        .unwrap();
        let mut body_state = (*body_fixture.state).clone();
        body_state.model_proxy_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(100))
            .timeout(Duration::from_millis(150))
            .read_timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let body_app = build_router(body_state);
        let body_timeout = model_proxy_test_http_request(
            &body_app,
            &body_fixture,
            &body_claim.model_proxy_token,
            body_binding_id,
            "case=body-timeout",
            "runtime-claim-model",
        )
        .await;
        assert_eq!(body_timeout.status(), StatusCode::OK);
        let mut body = body_timeout.into_body().into_data_stream();
        assert!(body.next().await.unwrap().unwrap().starts_with(b"event:"));
        let failure = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap();
        assert!(failure.is_err());
        assert_eq!(
            sqlx::query_as::<_, (String, String, Option<i32>)>(
                "SELECT response_status, error_kind, upstream_http_status
                 FROM model_call_errors WHERE model_connection_id = $1",
            )
            .bind(body_fixture.model_connection_id)
            .fetch_one(&body_fixture.state.pool)
            .await
            .unwrap(),
            ("transport_error".into(), "timeout".into(), Some(200))
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_token_usage
                 WHERE model_connection_id IN ($1, $2)",
            )
            .bind(header_fixture.model_connection_id)
            .bind(body_fixture.model_connection_id)
            .fetch_one(&body_fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        server.abort();
    }

    #[tokio::test]
    async fn model_proxy_streams_sse_chunks_before_the_upstream_response_finishes() {
        use axum::response::IntoResponse;
        use futures_util::StreamExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/responses", post(slow_sse_model_upstream)),
            )
            .await
            .unwrap();
        });
        let state = test_model_proxy_state();

        let response = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}"),
            "responses",
            Bytes::from_static(br#"{"model":"test-model","stream":true}"#),
        )
        .await
        .expect("the proxy should pass through an SSE response")
        .into_response();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );
        assert!(response.headers().get(header::CONNECTION).is_none());
        assert!(response.headers().get("x-upstream-hop").is_none());

        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_millis(150), body.next())
            .await
            .expect("the first SSE chunk should arrive before the delayed second chunk")
            .expect("the streamed response should contain a first chunk")
            .expect("the first SSE chunk should not fail");
        assert_eq!(first, Bytes::from_static(b"data: first\n\n"));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), body.next())
                .await
                .is_err()
        );
        let second = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .expect("the delayed second SSE chunk should arrive")
            .expect("the streamed response should contain a second chunk")
            .expect("the second SSE chunk should not fail");
        assert_eq!(second, Bytes::from_static(b"data: second\n\n"));

        server.abort();
    }

    #[tokio::test]
    async fn model_proxy_preserves_non_success_upstream_status_and_body() {
        use axum::response::IntoResponse;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/responses", post(model_upstream_rate_limited)),
            )
            .await
            .unwrap();
        });
        let state = test_model_proxy_state();

        let response = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}"),
            "responses",
            Bytes::from_static(br#"{"model":"test-model","input":[]}"#),
        )
        .await
        .expect("the proxy should preserve an upstream non-success response")
        .into_response();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, Bytes::from_static(b"provider rate limit"));

        server.abort();
    }

    #[tokio::test]
    async fn model_proxy_maps_upstream_header_timeout_to_gateway_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/responses", post(model_upstream_never_sends_headers)),
            )
            .await
            .unwrap();
        });
        let state = test_model_proxy_state_with_timeout(Duration::from_millis(100));

        let error = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}"),
            "responses",
            Bytes::from_static(br#"{"model":"test-model","input":[]}"#),
        )
        .await
        .expect_err("an upstream header timeout must fail before response headers are sent");

        assert_eq!(error.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error.message, "model upstream timed out");
        server.abort();
    }

    #[tokio::test]
    async fn model_proxy_maps_non_timeout_upstream_transport_failure_to_bad_gateway() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let state = test_model_proxy_state_with_timeout(Duration::from_millis(100));

        let error = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}"),
            "responses",
            Bytes::from_static(br#"{"model":"test-model","input":[]}"#),
        )
        .await
        .expect_err("a refused upstream connection must fail before response headers are sent");

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.message, "model upstream request failed");
    }

    #[tokio::test]
    async fn model_proxy_terminates_a_body_that_stalls_after_the_first_chunk() {
        use futures_util::StreamExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/responses",
                    post(model_upstream_stalls_after_first_chunk),
                ),
            )
            .await
            .unwrap();
        });
        let state = test_model_proxy_state_with_timeout(Duration::from_millis(150));
        let response = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}"),
            "responses",
            Bytes::from_static(br#"{"model":"test-model","stream":true}"#),
        )
        .await
        .expect("the first upstream response headers must be forwarded");
        let mut body = response.into_body().into_data_stream();

        let first = tokio::time::timeout(Duration::from_millis(100), body.next())
            .await
            .expect("the first chunk must remain streaming")
            .expect("the first chunk must exist")
            .expect("the first chunk must be valid");
        assert_eq!(first, Bytes::from_static(b"data: first\n\n"));
        let stalled = tokio::time::timeout(Duration::from_millis(500), body.next())
            .await
            .expect("a stalled upstream body must terminate within the configured timeout")
            .expect("the proxy body must report the upstream transport failure");
        assert!(stalled.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn model_proxy_forwards_original_json_bytes_to_upstream() {
        use axum::response::IntoResponse;

        let captured = Arc::new(std::sync::Mutex::new(None));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let captured_request = Arc::clone(&captured);
        let server = tokio::spawn(async move {
            let handler = move |headers: HeaderMap, uri: axum::http::Uri, body: Bytes| {
                let captured_request = Arc::clone(&captured_request);
                async move {
                    *captured_request.lock().unwrap() = Some((headers, uri, body.clone()));
                    let mut response = Response::new(Body::from(body));
                    response.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    response
                }
            };
            axum::serve(
                listener,
                Router::new().route("/custom/v1/responses", post(handler)),
            )
            .await
            .unwrap();
        });
        let state = test_model_proxy_state();
        let original = Bytes::from_static(br#" { "model": "test-model", "input": [] } "#);

        let response = proxy_model_request_to_upstream(
            &state,
            &format!("http://{address}/custom"),
            "responses",
            original.clone(),
        )
        .await
        .unwrap()
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(body, original);
        let (headers, uri, body) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(
            headers[header::AUTHORIZATION],
            "Bearer test-provider-secret"
        );
        assert_eq!(uri.path(), "/custom/v1/responses");
        assert_eq!(uri.query(), None);
        assert_eq!(body, original);
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_connection_crud_encrypts_secrets_and_enforces_scope(pool: PgPool) {
        let member_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let member_headers = session_headers(&member_token);
        let admin_headers = session_headers(&admin_token);
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));

        let personal = create_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Personal,
                name: "Local Responses".into(),
                base_url: "http://169.254.169.254/latest".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec![
                    " local-model ".into(),
                    "local-model-2".into(),
                    "local-model".into(),
                ],
                api_key: "personal-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(personal.scope, ModelConnectionScope::Personal);
        assert_eq!(personal.status, ModelConnectionStatus::Enabled);
        assert_eq!(personal.api_type, ModelUpstreamProtocol::OpenaiResponses);
        assert_eq!(
            personal.allowed_model_ids,
            vec!["local-model".to_owned(), "local-model-2".to_owned()]
        );
        assert!(personal.has_api_key);
        assert!(!serde_json::to_string(&personal)
            .unwrap()
            .contains("personal-secret"));

        let stored_before: (Vec<u8>, Vec<u8>) = sqlx::query_as(
            "SELECT api_key_ciphertext, api_key_nonce
             FROM model_connections WHERE id = $1",
        )
        .bind(personal.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(stored_before.0, b"personal-secret");
        assert_eq!(stored_before.1.len(), 12);

        let updated = update_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Path(personal.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: "Local Responses Updated".into(),
                base_url: "https://models.internal.example/business".into(),
                api_type: ModelUpstreamProtocol::AnthropicMessages,
                allowed_model_ids: vec!["local-model-2".into(), "claude-test".into()],
                api_key: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated.name, "Local Responses Updated");
        assert_eq!(updated.api_type, ModelUpstreamProtocol::AnthropicMessages);
        assert_eq!(
            updated.allowed_model_ids,
            vec!["local-model-2".to_owned(), "claude-test".to_owned()]
        );
        let preserved = update_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Path(personal.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: "Local Anthropic Updated".into(),
                base_url: updated.base_url.clone(),
                api_type: updated.api_type,
                allowed_model_ids: updated.allowed_model_ids.clone(),
                api_key: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(preserved.api_type, ModelUpstreamProtocol::AnthropicMessages);
        assert_eq!(preserved.allowed_model_ids, updated.allowed_model_ids);
        let stored_after: (Vec<u8>, Vec<u8>) = sqlx::query_as(
            "SELECT api_key_ciphertext, api_key_nonce
             FROM model_connections WHERE id = $1",
        )
        .bind(personal.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored_before, stored_after);
        let _ = update_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Path(personal.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: preserved.name.clone(),
                base_url: preserved.base_url.clone(),
                api_type: preserved.api_type,
                allowed_model_ids: preserved.allowed_model_ids.clone(),
                api_key: Some("rotated-personal-secret".into()),
            }),
        )
        .await
        .unwrap();
        let stored_rotated: (Vec<u8>, Vec<u8>) = sqlx::query_as(
            "SELECT api_key_ciphertext, api_key_nonce
             FROM model_connections WHERE id = $1",
        )
        .bind(personal.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(stored_rotated, stored_before);
        assert_eq!(
            state
                .model_secret_cipher
                .decrypt(&stored_rotated.0, &stored_rotated.1)
                .unwrap(),
            "rotated-personal-secret"
        );

        let forbidden = create_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Global,
                name: "Forbidden Global".into(),
                base_url: "https://example.com".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["global-model".into()],
                api_key: "global-secret".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);

        let global = create_model_connection(
            State(state.clone()),
            admin_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Global,
                name: "Global Responses".into(),
                base_url: "https://example.com/provider".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["global-model".into(), "global-mini".into()],
                api_key: "global-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;

        let _ = set_system_default_model_selection(
            State(state.clone()),
            admin_headers.clone(),
            Json(SetSystemDefaultModelSelectionRequest {
                selection: Some(test_model_selection(&global)),
            }),
        )
        .await
        .unwrap();
        let disabled = update_model_connection_status(
            State(state.clone()),
            admin_headers.clone(),
            Path(global.id),
            Json(UpdateModelConnectionStatusRequest {
                status: ModelConnectionStatus::Disabled,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(disabled.status, ModelConnectionStatus::Disabled);
        assert_eq!(
            get_system_default_model_selection(State(state.clone()), admin_headers.clone())
                .await
                .unwrap()
                .0
                .selection,
            None
        );
        let enabled = update_model_connection_status(
            State(state.clone()),
            admin_headers.clone(),
            Path(global.id),
            Json(UpdateModelConnectionStatusRequest {
                status: ModelConnectionStatus::Enabled,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(enabled.status, ModelConnectionStatus::Enabled);

        let member_list = list_model_connections(State(state.clone()), member_headers.clone())
            .await
            .unwrap()
            .0;
        assert!(member_list.iter().any(|item| item.id == personal.id));
        assert!(member_list.iter().any(|item| item.id == global.id));
        let admin_list = list_model_connections(State(state.clone()), admin_headers.clone())
            .await
            .unwrap()
            .0;
        assert!(!admin_list.iter().any(|item| item.id == personal.id));
        assert!(admin_list.iter().any(|item| item.id == global.id));

        let hidden = get_model_connection(State(state.clone()), admin_headers, Path(personal.id))
            .await
            .unwrap_err();
        assert_eq!(hidden.status, StatusCode::NOT_FOUND);

        for invalid_url in ["ftp://example.com", "https://example.com/v1"] {
            let error = update_model_connection(
                State(state.clone()),
                member_headers.clone(),
                Path(personal.id),
                Query(UpdateModelConnectionQuery::default()),
                Json(UpdateModelConnectionRequest {
                    vision_model_id: None,
                    name: "Invalid URL".into(),
                    base_url: invalid_url.into(),
                    api_type: preserved.api_type,
                    allowed_model_ids: preserved.allowed_model_ids.clone(),
                    api_key: None,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }

        for allowed_model_ids in [Vec::new(), vec!["bad\nmodel".into()]] {
            let error = update_model_connection(
                State(state.clone()),
                member_headers.clone(),
                Path(personal.id),
                Query(UpdateModelConnectionQuery::default()),
                Json(UpdateModelConnectionRequest {
                    vision_model_id: None,
                    name: personal.name.clone(),
                    base_url: personal.base_url.clone(),
                    api_type: personal.api_type,
                    allowed_model_ids,
                    api_key: None,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_connection_update_requires_force_for_referenced_models_and_api_type(
        pool: PgPool,
    ) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let headers = session_headers(&admin_token);
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let connection = create_model_connection(
            State(state.clone()),
            headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Global,
                name: "Force Update Global".into(),
                base_url: "https://models.example.test".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["model-a".into(), "model-b".into()],
                api_key: "force-update-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let selection_a = ModelSelectionDto {
            connection_id: connection.id,
            model_id: "model-a".into(),
        };
        let selection_b = ModelSelectionDto {
            connection_id: connection.id,
            model_id: "model-b".into(),
        };
        let _ = set_system_default_model_selection(
            State(state.clone()),
            headers.clone(),
            Json(SetSystemDefaultModelSelectionRequest {
                selection: Some(selection_a.clone()),
            }),
        )
        .await
        .unwrap();
        let agent = create_agent(
            State(state.clone()),
            headers.clone(),
            Json(CreateAgentRequest {
                name: "Force Update Agent".into(),
                instructions: "Use the selected model.".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
                endpoint_exposure: vec![
                    "console".into(),
                    "integration".into(),
                    "automation".into(),
                ],
                model_selection: Some(selection_a),
                model_settings: Some(AgentModelSettings::default()),
                subagents: vec![SubagentDefinition {
                    name: "reviewer".into(),
                    description: "Reviews output".into(),
                    developer_instructions: "Review carefully.".into(),
                    model_selection: Some(selection_b),
                    model_settings_override: AgentModelSettingsOverride::default(),
                    enabled: true,
                    disabled_reason: None,
                }],
                secret_declarations: Some(Vec::new()),
                tool_allowlist: default_agent_tool_allowlist(),
            }),
        )
        .await
        .unwrap()
        .0;

        let remove_referenced = update_model_connection(
            State(state.clone()),
            headers.clone(),
            Path(connection.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: connection.name.clone(),
                base_url: connection.base_url.clone(),
                api_type: connection.api_type,
                allowed_model_ids: vec!["model-b".into()],
                api_key: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(remove_referenced.status, StatusCode::CONFLICT);

        let _ = update_model_connection(
            State(state.clone()),
            headers.clone(),
            Path(connection.id),
            Query(UpdateModelConnectionQuery { force: true }),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: connection.name.clone(),
                base_url: connection.base_url.clone(),
                api_type: connection.api_type,
                allowed_model_ids: vec!["model-b".into()],
                api_key: None,
            }),
        )
        .await
        .unwrap();
        let after_model_removal = get_agent(State(state.clone()), headers.clone(), Path(agent.id))
            .await
            .unwrap()
            .0;
        assert_eq!(after_model_removal.model_selection, None);
        assert!(after_model_removal.subagents[0].enabled);
        assert_eq!(
            after_model_removal.subagents[0]
                .model_selection
                .as_ref()
                .map(|selection| selection.model_id.as_str()),
            Some("model-b")
        );
        assert_eq!(
            get_system_default_model_selection(State(state.clone()), headers.clone())
                .await
                .unwrap()
                .0
                .selection,
            None
        );

        let change_referenced_type = update_model_connection(
            State(state.clone()),
            headers.clone(),
            Path(connection.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: connection.name.clone(),
                base_url: connection.base_url.clone(),
                api_type: ModelUpstreamProtocol::AnthropicMessages,
                allowed_model_ids: vec!["model-b".into()],
                api_key: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(change_referenced_type.status, StatusCode::CONFLICT);

        let changed = update_model_connection(
            State(state.clone()),
            headers.clone(),
            Path(connection.id),
            Query(UpdateModelConnectionQuery { force: true }),
            Json(UpdateModelConnectionRequest {
                vision_model_id: None,
                name: connection.name,
                base_url: connection.base_url,
                api_type: ModelUpstreamProtocol::AnthropicMessages,
                allowed_model_ids: vec!["model-b".into()],
                api_key: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(changed.api_type, ModelUpstreamProtocol::AnthropicMessages);
        let after_type_change = get_agent(State(state), headers, Path(agent.id))
            .await
            .unwrap()
            .0;
        assert!(!after_type_change.subagents[0].enabled);
        assert_eq!(after_type_change.subagents[0].model_selection, None);
        assert_eq!(
            after_type_change.subagents[0].disabled_reason.as_deref(),
            Some("model_selection_removed")
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_connection_delete_reports_references_and_force_delete_scrubs_execution(
        pool: PgPool,
    ) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let admin_headers = session_headers(&admin_token);
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let administrator = require_user(&state, &admin_headers).await.unwrap();
        let connection = create_model_connection(
            State(state.clone()),
            admin_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Global,
                name: "Referenced Global".into(),
                base_url: "http://127.0.0.1:1".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["referenced-model".into(), "other-model".into()],
                api_key: "referenced-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let _ = set_system_default_model_selection(
            State(state.clone()),
            admin_headers.clone(),
            Json(SetSystemDefaultModelSelectionRequest {
                selection: Some(test_model_selection(&connection)),
            }),
        )
        .await
        .unwrap();

        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility,
                  model_connection_id, model_id)
             VALUES ($1, $2, 'Model Agent', '# Instructions', 'private', $3, $4)",
        )
        .bind(agent_id)
        .bind(administrator.id)
        .bind(connection.id)
        .bind(&connection.allowed_model_ids[0])
        .execute(&pool)
        .await
        .unwrap();
        let subagent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO subagent_definitions
                 (id, agent_id, name, description, developer_instructions,
                  model_connection_id, model_id)
             VALUES ($1, $2, 'researcher', 'Researches', '# Research', $3, $4)",
        )
        .bind(subagent_id)
        .bind(agent_id)
        .bind(connection.id)
        .bind(&connection.allowed_model_ids[0])
        .execute(&pool)
        .await
        .unwrap();

        let conflict = delete_model_connection(
            State(state.clone()),
            admin_headers.clone(),
            Path(connection.id),
        )
        .await
        .unwrap_err();
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert!(conflict.message.contains("System Default"));
        assert!(conflict.message.contains("Agent"));
        assert!(conflict.message.contains("subagent"));

        assert_eq!(
            force_delete_model_connection(
                State(state.clone()),
                admin_headers.clone(),
                Path(connection.id),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        let scrubbed = sqlx::query(
            "SELECT base_url, api_key_ciphertext, api_key_nonce, enabled,
                    deleted_at IS NOT NULL AS deleted
             FROM model_connections WHERE id = $1",
        )
        .bind(connection.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(scrubbed.get::<Option<String>, _>("base_url"), None);
        assert_eq!(
            scrubbed.get::<Option<Vec<u8>>, _>("api_key_ciphertext"),
            None
        );
        assert_eq!(scrubbed.get::<Option<Vec<u8>>, _>("api_key_nonce"), None);
        assert!(!scrubbed.get::<bool, _>("enabled"));
        assert!(scrubbed.get::<bool, _>("deleted"));
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT model_connection_id FROM agents WHERE id = $1",
            )
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            None
        );
        let subagent: (bool, Option<String>, Option<Uuid>) = sqlx::query_as(
            "SELECT enabled, disabled_reason, model_connection_id
             FROM subagent_definitions WHERE id = $1",
        )
        .bind(subagent_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!subagent.0);
        assert_eq!(subagent.1.as_deref(), Some("model_connection_deleted"));
        assert_eq!(subagent.2, None);
        assert_eq!(
            get_system_default_model_selection(State(state), admin_headers)
                .await
                .unwrap()
                .0
                .selection,
            None
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_connection_test_calls_responses_and_attributes_usage_and_errors(pool: PgPool) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = Router::new().fallback(
            |headers: HeaderMap, uri: axum::http::Uri, body: Bytes| async move {
                if headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer provider-secret")
                {
                    return StatusCode::UNAUTHORIZED.into_response();
                }
                if uri.path().starts_with("/ok") {
                    let body: Value = serde_json::from_slice(&body).unwrap();
                    if body.get("model").and_then(Value::as_str) != Some("test-model")
                        || body.get("input").and_then(Value::as_str) != Some("hi")
                        || body.get("max_output_tokens").and_then(Value::as_u64) != Some(256)
                    {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({ "error": { "code": "bad_test_request" } })),
                        )
                            .into_response();
                    }
                    return (
                        StatusCode::OK,
                        Json(json!({
                            "id": "resp_test",
                            "object": "response",
                            "status": "completed",
                            "output": [{
                                "type": "message",
                                "role": "assistant",
                                "content": [{
                                    "type": "output_text",
                                    "text": "Hello from the model"
                                }]
                            }],
                            "usage": {
                                "input_tokens": 11,
                                "output_tokens": 7,
                                "total_tokens": 18,
                                "input_tokens_details": { "cached_tokens": 3 },
                                "output_tokens_details": { "reasoning_tokens": 5 }
                            }
                        })),
                    )
                        .into_response();
                }
                if uri.path().starts_with("/fail") {
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        Json(json!({
                            "error": {
                                "code": "rate_provider-secret_limit",
                                "message": "provider provider-secret rejected the request"
                            }
                        })),
                    )
                        .into_response();
                }
                let stream = async_stream::stream! {
                    yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                        br#"{"id":"partial""#,
                    ));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    yield Err(std::io::Error::other("upstream body failed"));
                };
                let mut response = Response::new(Body::from_stream(stream));
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                response
            },
        );
        let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

        let member_token = create_user_session_with_role(&pool, "member").await;
        let member_headers = session_headers(&member_token);
        let test_state = test_state_with_browser_session_auth(pool.clone());
        let state = Arc::new(test_state);
        let member = require_user(&state, &member_headers).await.unwrap();
        let successful = create_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Personal,
                name: "Test Success".into(),
                base_url: format!("http://{address}/ok"),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["test-model".into(), "test-model-2".into()],
                api_key: "provider-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let result = test_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Path(successful.id),
            Json(TestModelConnectionRequest {
                model_id: "test-model".into(),
                message: "hi".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(result.success);
        assert_eq!(result.status_code, Some(200));
        assert_eq!(
            result.response_text.as_deref(),
            Some("Hello from the model")
        );
        let usage: (Option<Uuid>, Option<Uuid>, i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT subject_user_id, agent_id, input_tokens, output_tokens,
                    total_tokens, cached_tokens, reasoning_tokens
             FROM model_token_usage WHERE model_connection_id = $1",
        )
        .bind(successful.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(usage, (Some(member.id), None, 11, 7, 18, 3, 5));

        let failed = create_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Personal,
                name: "Test Failure".into(),
                base_url: format!("http://{address}/fail"),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["test-model".into()],
                api_key: "provider-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let failure = test_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Path(failed.id),
            Json(TestModelConnectionRequest {
                model_id: "test-model".into(),
                message: "hi".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(!failure.success);
        assert_eq!(failure.status_code, Some(429));
        assert!(!failure
            .error_code
            .as_deref()
            .unwrap()
            .contains("provider-secret"));
        assert!(!failure
            .message
            .as_deref()
            .unwrap()
            .contains("provider-secret"));
        let recorded_failure: (Option<String>, String) = sqlx::query_as(
            "SELECT error_code, message FROM model_call_errors
             WHERE model_connection_id = $1",
        )
        .bind(failed.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!recorded_failure
            .0
            .as_deref()
            .unwrap()
            .contains("provider-secret"));
        assert!(!recorded_failure.1.contains("provider-secret"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_call_errors WHERE model_connection_id = $1",
            )
            .bind(failed.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM model_token_usage WHERE model_connection_id = $1",
            )
            .bind(failed.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );

        let broken = create_model_connection(
            State(state.clone()),
            member_headers.clone(),
            Json(CreateModelConnectionRequest {
                vision_model_id: None,
                scope: ModelConnectionScope::Personal,
                name: "Test Broken Body".into(),
                base_url: format!("http://{address}/broken"),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["test-model".into()],
                api_key: "provider-secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let body_failure = test_model_connection(
            State(state),
            member_headers,
            Path(broken.id),
            Json(TestModelConnectionRequest {
                model_id: "test-model".into(),
                message: "hi".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(!body_failure.success);
        assert_eq!(body_failure.status_code, Some(200));
        assert_eq!(
            body_failure.error_code.as_deref(),
            Some("response_body_error")
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String, Option<i32>)>(
                "SELECT response_status, error_kind, upstream_http_status
                 FROM model_call_errors WHERE model_connection_id = $1",
            )
            .bind(broken.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            ("transport_error".into(), "response_body".into(), Some(200))
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn model_usage_queries_enforce_ranges_keysets_and_role_visibility(pool: PgPool) {
        let owner_token = create_user_session_with_role(&pool, "member").await;
        let caller_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let owner = require_user(&state, &session_headers(&owner_token))
            .await
            .unwrap();
        let caller = require_user(&state, &session_headers(&caller_token))
            .await
            .unwrap();
        let super_admin = require_user(&state, &session_headers(&super_token))
            .await
            .unwrap();
        let connection = create_test_model_connection_for_token(
            &state,
            &admin_token,
            ModelConnectionScope::Global,
            "Usage Global",
        )
        .await;
        let owner_agent_id = Uuid::new_v4();
        let protected_agent_id = Uuid::new_v4();
        for (id, owner_id, name) in [
            (owner_agent_id, owner.id, "Owner Agent"),
            (protected_agent_id, super_admin.id, "Protected Agent"),
        ] {
            sqlx::query(
                "INSERT INTO agents
                     (id, owner_id, name, instructions, visibility, model_policy)
                 VALUES ($1, $2, $3, '', 'private',
                         '{\"provider\":\"hub-proxy\"}'::jsonb)",
            )
            .bind(id)
            .bind(owner_id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        }

        let at = DateTime::parse_from_rfc3339("2026-07-18T01:02:03.456Z")
            .unwrap()
            .with_timezone(&Utc);
        let next = at + ChronoDuration::milliseconds(1);
        let usage_ids = [
            Uuid::from_u128(0x10),
            Uuid::from_u128(0x20),
            Uuid::from_u128(0x30),
            Uuid::from_u128(0x40),
        ];
        for (id, occurred_at, agent_id, agent_name, subject, tokens) in [
            (
                usage_ids[0],
                at,
                owner_agent_id,
                "Owner Agent",
                &owner,
                10_i64,
            ),
            (
                usage_ids[1],
                at,
                owner_agent_id,
                "Owner Agent",
                &caller,
                20_i64,
            ),
            (
                usage_ids[2],
                next,
                owner_agent_id,
                "Owner Agent",
                &super_admin,
                30_i64,
            ),
            (
                usage_ids[3],
                next,
                protected_agent_id,
                "Protected Agent",
                &owner,
                40_i64,
            ),
        ] {
            sqlx::query(
                "INSERT INTO model_token_usage
                     (id, request_id, occurred_at, response_status,
                      model_connection_id, model_connection_scope_snapshot,
                      model_connection_name_snapshot, model_id_snapshot,
                      api_type_snapshot, request_settings_snapshot,
                      agent_id, agent_name_snapshot, subject_type,
                      subject_user_id, subject_display_name_snapshot,
                      input_tokens, output_tokens, total_tokens,
                      cached_tokens, reasoning_tokens)
                 VALUES ($1, $2, $3, 'completed', $4, 'global', $5, $6,
                         'openai_responses', '{\"protocol\":\"openai_responses\"}'::jsonb,
                         $7, $8, 'user', $9, $10, $11, 0, $11, 0, 0)",
            )
            .bind(id)
            .bind(Uuid::new_v4())
            .bind(occurred_at)
            .bind(connection.id)
            .bind(&connection.name)
            .bind(&connection.allowed_model_ids[0])
            .bind(agent_id)
            .bind(agent_name)
            .bind(subject.id)
            .bind(&subject.display_name)
            .bind(tokens)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO model_call_errors
                 (id, request_id, occurred_at, response_status,
                  upstream_http_status, error_kind, error_code, message,
                  model_connection_id, model_connection_scope_snapshot,
                  model_connection_name_snapshot, model_id_snapshot,
                  api_type_snapshot, request_settings_snapshot,
                  agent_id, agent_name_snapshot, subject_type,
                  subject_user_id, subject_display_name_snapshot)
             VALUES ($1, $2, $3, 'failed', 429, 'provider_failed',
                     'rate_limit', 'try later', $4, 'global', $5, $6,
                     'openai_responses', '{\"protocol\":\"openai_responses\"}'::jsonb,
                     $7, 'Owner Agent', 'user', $8, $9)",
        )
        .bind(Uuid::from_u128(0x50))
        .bind(Uuid::new_v4())
        .bind(at)
        .bind(connection.id)
        .bind(&connection.name)
        .bind(&connection.allowed_model_ids[0])
        .bind(owner_agent_id)
        .bind(caller.id)
        .bind(&caller.display_name)
        .execute(&pool)
        .await
        .unwrap();

        let owner_summary = get_model_usage_summary(
            State(state.clone()),
            session_headers(&owner_token),
            Query(ModelTokenUsageQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(owner_summary.overall.total_tokens, 100);
        assert_eq!(owner_summary.by_agent.len(), 2);
        assert_eq!(owner_summary.by_user.len(), 2);
        assert!(owner_summary
            .by_user
            .iter()
            .any(|group| group.user_id == Some(owner.id)));
        assert!(owner_summary
            .by_user
            .iter()
            .any(|group| group.user_id.is_none() && group.display_name.is_none()));

        let first_page = list_model_token_usage(
            State(state.clone()),
            session_headers(&owner_token),
            Query(ModelTokenUsageQueryDto {
                from_ms: Some(at.timestamp_millis()),
                to_ms: Some(next.timestamp_millis()),
                page_size: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first_page.items.len(), 1);
        assert_eq!(first_page.items[0].id, usage_ids[0]);
        assert!(first_page.next_cursor.is_none());

        let super_first = list_model_token_usage(
            State(state.clone()),
            session_headers(&super_token),
            Query(ModelTokenUsageQueryDto {
                page_size: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(super_first.items[0].id, usage_ids[3]);
        let super_cursor = super_first.next_cursor.unwrap();
        let super_second = list_model_token_usage(
            State(state.clone()),
            session_headers(&super_token),
            Query(ModelTokenUsageQueryDto {
                cursor_occurred_at_ms: Some(super_cursor.occurred_at_ms),
                cursor_id: Some(super_cursor.id),
                page_size: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(super_second.items[0].id, usage_ids[2]);

        let errors = list_model_call_errors(
            State(state.clone()),
            session_headers(&caller_token),
            Query(ModelCallErrorQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(errors.items.len(), 1);
        assert_eq!(errors.items[0].error_code.as_deref(), Some("rate_limit"));

        let admin_summary = get_model_usage_summary(
            State(state.clone()),
            session_headers(&admin_token),
            Query(ModelTokenUsageQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(admin_summary.overall.total_tokens, 30);
        let super_summary = get_model_usage_summary(
            State(state),
            session_headers(&super_token),
            Query(ModelTokenUsageQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(super_summary.overall.total_tokens, 100);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn model_connection_vision_model_id_is_validated_and_persisted(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let token = format!("ahs_{}", Uuid::new_v4().simple());
        let unique = Uuid::new_v4().simple().to_string();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Vision Owner', 'member')",
        )
        .bind(owner_id)
        .bind(format!("vision-{unique}@example.com"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&token))
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool));

        let created = create_model_connection(
            State(state.clone()),
            session_headers(&token),
            Json(CreateModelConnectionRequest {
                scope: ModelConnectionScope::Personal,
                name: "Vision Connection".into(),
                base_url: "http://models.example.test".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["main-model".into(), "vision-model".into()],
                vision_model_id: Some("vision-model".into()),
                api_key: "secret".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(created.vision_model_id.as_deref(), Some("vision-model"));

        let invalid = create_model_connection(
            State(state.clone()),
            session_headers(&token),
            Json(CreateModelConnectionRequest {
                scope: ModelConnectionScope::Personal,
                name: "Invalid Vision Connection".into(),
                base_url: "http://models.example.test".into(),
                api_type: ModelUpstreamProtocol::OpenaiResponses,
                allowed_model_ids: vec!["main-model".into()],
                vision_model_id: Some("other-model".into()),
                api_key: "secret".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);

        let updated = update_model_connection(
            State(state.clone()),
            session_headers(&token),
            Path(created.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                name: created.name.clone(),
                base_url: created.base_url.clone(),
                api_type: created.api_type,
                allowed_model_ids: created.allowed_model_ids.clone(),
                vision_model_id: Some("main-model".into()),
                api_key: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated.vision_model_id.as_deref(), Some("main-model"));

        let cleared = update_model_connection(
            State(state),
            session_headers(&token),
            Path(created.id),
            Query(UpdateModelConnectionQuery::default()),
            Json(UpdateModelConnectionRequest {
                name: created.name,
                base_url: created.base_url,
                api_type: created.api_type,
                allowed_model_ids: created.allowed_model_ids,
                vision_model_id: Some(String::new()),
                api_key: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(cleared.vision_model_id, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn model_proxy_rewrites_vision_requests_to_the_vision_model(pool: PgPool) {
        let captured = Arc::new(std::sync::Mutex::new(None::<Bytes>));
        let captured_route = Arc::clone(&captured);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/v1/responses",
                post(move |body: Bytes| {
                    let captured_route = Arc::clone(&captured_route);
                    async move {
                        *captured_route.lock().unwrap() = Some(body);
                        Json(json!({ "status": "completed" }))
                    }
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE model_connections
             SET base_url = $2,
                 allowed_model_ids = ARRAY['runtime-claim-model', 'vision-claim-model'],
                 vision_model_id = 'vision-claim-model'
             WHERE id = $1",
        )
        .bind(fixture.model_connection_id)
        .bind(format!("http://{address}"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let binding_id = model_binding_id(&claim, "main");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1,
                 recovery_source = NULL
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let state = (*fixture.state).clone();
        let app = build_router(state);

        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/runtime/model-proxy/v1/responses")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", claim.model_proxy_token),
            )
            .header("x-agent-hub-run-id", fixture.run_id.to_string())
            .header(MODEL_PROXY_BINDING_ID_HEADER, binding_id.to_string())
            .header(VISION_PROXY_HEADER, "1")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                br#"{"model":"runtime-claim-model","input":[],"stream":false}"#.as_slice(),
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = captured.lock().unwrap().take().expect("upstream captured");
        let forwarded: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(forwarded["model"], "vision-claim-model");
        assert_eq!(forwarded["input"], json!([]));
        assert_eq!(forwarded["stream"], json!(false));
        server.abort();
    }
}
