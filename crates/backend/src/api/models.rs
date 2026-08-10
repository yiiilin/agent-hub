//! models 领域模块：Model API Connection / 模型用量台账 的 handler 与私有辅助函数。

use super::*;
use crate::{REDACTED_SECRET, ModelGatewayForwardRequest, send_model_gateway_request};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::Instant,
};

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
    let request_body = serde_json::to_vec(&json!({
        "model": model_id,
        "input": message,
        "max_output_tokens": 256
    }))
    .map_err(|_| ApiError::internal("failed to encode Model Connection test request"))?;
    let request_headers = HeaderMap::new();
    let started_at = Instant::now();
    let response = send_model_gateway_request(
        &state,
        ModelGatewayForwardRequest {
            request_id,
            upstream_protocol: connection.dto.api_type,
            request_settings: &request_settings,
            upstream_url: &connection.dto.base_url,
            query: None,
            headers: &request_headers,
            body: &request_body,
            api_key: &api_key,
        },
    )
    .await;
    let response = match response {
        Ok(response) => response,
        Err(_) => {
            record_model_test_error(
                &ledger_context,
                "transport_error",
                None,
                "transport_error",
                None,
                "connection request failed",
            )
            .await?;
            return Ok(Json(ModelConnectionTestResultDto {
                success: false,
                status_code: None,
                error_code: Some("transport_error".into()),
                message: Some("connection request failed".into()),
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
    let response_status = model_response_status(value.as_ref(), status.is_success());
    let usage = value.as_ref().and_then(extract_model_usage);
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
            response_text: value.as_ref().and_then(model_test_response_text),
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
