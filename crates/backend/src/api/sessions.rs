//! sessions 领域模块：会话、附件、消息与 run/事件处理。

use super::*;
use std::{
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    sync::Arc,
    time::Duration,
};

use agent_hub_shared::*;
use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{Multipart, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Response,
    },
    Json,
};
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use tracing::{info, warn};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::authorize_run_stream;
use crate::extract_model_usage;
use crate::insert_hub_native_session_tx;
use crate::load_agent_for_user;
use crate::load_run_for_user;
use crate::load_run_public_tx;
use crate::load_widget_credential_tx;
use crate::load_widget_scoped_session_tx;
use crate::missing_secret_grants;
use crate::model_connection_scope_name;
use crate::model_request_settings_value;
use crate::model_response_status;
use crate::model_test_response_text;
use crate::model_upstream_protocol_from_name;
use crate::model_upstream_protocol_name;
use crate::normalize_client_message_key;
use crate::record_runtime_session_cleanup_tx;
use crate::send_model_gateway_request;
use crate::widget_session_locator;
use crate::ModelGatewayForwardRequest;
use crate::ObservedModelUsage;
use crate::MAX_ATTACHMENT_BYTES_PER_SESSION;
#[cfg(test)]
use crate::MAX_ATTACHMENT_UPLOAD_BYTES;
use crate::SESSION_MESSAGE_PAGE_SQL;

#[derive(Debug, Clone)]
pub(crate) struct AcceptSessionMessage {
    pub(crate) session_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) owner_id: Uuid,
    pub(crate) content: String,
    pub(crate) payload: Value,
    pub(crate) role: String,
    pub(crate) message_kind: String,
    pub(crate) requested_delivery_mode: String,
    pub(crate) client_message_key: Option<String>,
    pub(crate) source: String,
    pub(crate) automation_id: Option<Uuid>,
    pub(crate) integration_session_id: Option<Uuid>,
    pub(crate) parent_run_id: Option<Uuid>,
    pub(crate) continuation_turn_id: Option<Uuid>,
    pub(crate) model_subject_type: String,
    pub(crate) model_subject_user_id: Option<Uuid>,
    pub(crate) model_source_integration_app_id: Option<Uuid>,
    pub(crate) external_user_context: Option<ExternalUserContextDto>,
    pub(crate) attachment_ids: Vec<Uuid>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct UpdateHubSessionTitleRequest {
    pub(crate) title: String,
}

pub(crate) async fn update_hub_session_title(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<UpdateHubSessionTitleRequest>,
) -> Result<Json<HubSessionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let title = req.title.trim();
    if title.is_empty() || title.chars().count() > 40 {
        return Err(ApiError::bad_request(
            "Session title must be 1 to 40 characters",
        ));
    }
    let updated = sqlx::query("UPDATE hub_sessions SET title = $1 WHERE id = $2 AND owner_id = $3")
        .bind(title)
        .bind(session_id)
        .bind(user.id)
        .execute(&state.pool)
        .await?;
    if updated.rows_affected() == 0 {
        return Err(ApiError::not_found("session not found"));
    }
    get_hub_session(axum::extract::State(state), headers, Path(session_id)).await
}

pub(crate) async fn generate_session_title_in_background(
    state: Arc<AppState>,
    session_id: Uuid,
    agent_id: Uuid,
    user_id: Uuid,
    first_message: String,
) {
    info!(session_id = %session_id, agent_id = %agent_id, "session title generation started");
    let outcome: anyhow::Result<()> = async {
        let model = sqlx::query(
            "SELECT a.name AS agent_name, a.model_connection_id, a.model_id,
                    c.scope, c.name AS connection_name, c.base_url, c.api_type,
                    c.api_key_ciphertext, c.api_key_nonce
             FROM agents a
             JOIN model_connections c ON c.id = a.model_connection_id
             WHERE a.id = $1 AND a.deleted_at IS NULL
               AND c.deleted_at IS NULL AND c.enabled",
        )
        .bind(agent_id)
        .fetch_optional(&state.pool)
        .await?;
        let Some(model) = model else {
            warn!(session_id = %session_id, agent_id = %agent_id,
                "session title generation skipped: no enabled model connection");
            return Ok(());
        };
        let api_type =
            model_upstream_protocol_from_name(&model.get::<String, _>("api_type"));
        if api_type != ModelUpstreamProtocol::OpenaiResponses {
            return Ok(());
        }
        let connection_id: Uuid = model.get("model_connection_id");
        let model_id: String = model.get("model_id");
        let base_url: String = model.get("base_url");
        let connection_name: String = model.get("connection_name");
        let connection_scope = match model.get::<String, _>("scope").as_str() {
            "global" => ModelConnectionScope::Global,
            _ => ModelConnectionScope::Personal,
        };
        let agent_name: String = model.get("agent_name");
        let ciphertext: Vec<u8> = model.get("api_key_ciphertext");
        let nonce: Vec<u8> = model.get("api_key_nonce");
        let api_key = Zeroizing::new(
            state
                .model_secret_cipher
                .decrypt(&ciphertext, &nonce)
                .map_err(|_| anyhow::anyhow!("model secret decryption failed"))?,
        );
        let request_settings = ModelRequestSettings::for_protocol(api_type);
        let prompt = format!(
            "根据用户的第一条消息，为这段对话生成一个简洁的中文标题（15 字以内），概括用户的意图或任务主题。\n要求：不要写问候语或自我介绍；不要写“我能做什么”之类的回应；直接输出标题本身；不要引号，不要解释。\n\n示例：\n用户消息：帮我看看如何排查网络延迟问题\n标题：网络延迟问题排查\n\n用户消息：你好\n标题：日常问候\n\n用户消息：帮我规划一下数据库备份策略\n标题：数据库备份策略规划\n\n用户消息：{}\n标题：",
            first_message.chars().take(400).collect::<String>()
        );
        let request_body = serde_json::to_vec(&json!({
            "model": model_id,
            "input": prompt,
            "max_output_tokens": 64,
            "temperature": 0.3
        }))?;
        let request_id = Uuid::new_v4();
        let response = send_model_gateway_request(
            &state,
            ModelGatewayForwardRequest {
                request_id,
                upstream_protocol: api_type,
                request_settings: &request_settings,
                upstream_url: &base_url,
                query: None,
                headers: &HeaderMap::new(),
                body: &request_body,
                api_key: &api_key,
            },
        )
        .await?;
        let status = response.status();
        let body = response.bytes().await?;
        let value = serde_json::from_slice::<Value>(&body)?;
        if status.is_success() {
            if let Some(usage) = extract_model_usage(&value) {
                record_session_title_usage(
                    &state,
                    request_id,
                    &value,
                    usage,
                    connection_id,
                    &connection_scope,
                    &connection_name,
                    &model_id,
                    &request_settings,
                    agent_id,
                    &agent_name,
                    user_id,
                )
                .await?;
            }
            if let Some(text) = model_test_response_text(&value) {
                let title = sanitize_session_title(&text);
                if !title.is_empty() {
                    sqlx::query(
                        "UPDATE hub_sessions
                         SET title = $1
                         WHERE id = $2 AND title IS NULL",
                    )
                    .bind(&title)
                    .bind(session_id)
                    .execute(&state.pool)
                    .await?;
                    info!(session_id = %session_id, title = %title,
                        "session title generated");
                }
            } else {
                warn!(session_id = %session_id, "session title generation got no text in the model response");
            }
        } else {
            record_session_title_error(
                &state,
                request_id,
                Some(status.as_u16()),
                "upstream_http",
                "upstream_error",
                "Session title generation failed",
                connection_id,
                &connection_scope,
                &connection_name,
                &model_id,
                &request_settings,
                agent_id,
                &agent_name,
                user_id,
            )
            .await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = outcome {
        warn!(session_id = %session_id, agent_id = %agent_id, error = %error,
            "Session title generation failed");
    }
}

pub(crate) fn sanitize_session_title(text: &str) -> String {
    let title = text
        .trim()
        .trim_matches(['"', '\'', '“', '”', '「', '」', '《', '》'])
        .lines()
        .next()
        .unwrap_or("")
        .trim();
    title.chars().take(40).collect()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_session_title_usage(
    state: &AppState,
    request_id: Uuid,
    response: &Value,
    usage: ObservedModelUsage,
    connection_id: Uuid,
    connection_scope: &ModelConnectionScope,
    connection_name: &str,
    model_id: &str,
    request_settings: &ModelRequestSettings,
    agent_id: Uuid,
    agent_name: &str,
    user_id: Uuid,
) -> anyhow::Result<()> {
    let display_name: Option<String> =
        sqlx::query_scalar("SELECT display_name FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&state.pool)
            .await?;
    let response_status = model_response_status(Some(response), true);
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
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                 'user', $12, $13, $14, $15, $16, $17, $18)",
    )
    .bind(Uuid::new_v4())
    .bind(request_id)
    .bind(response_status)
    .bind(connection_id)
    .bind(model_connection_scope_name(*connection_scope))
    .bind(connection_name)
    .bind(model_id)
    .bind(model_upstream_protocol_name(
        ModelUpstreamProtocol::OpenaiResponses,
    ))
    .bind(model_request_settings_value(request_settings))
    .bind(agent_id)
    .bind(agent_name)
    .bind(user_id)
    .bind(display_name)
    .bind(usage.input_tokens)
    .bind(usage.output_tokens)
    .bind(usage.total_tokens)
    .bind(usage.cached_tokens)
    .bind(usage.reasoning_tokens)
    .execute(&state.pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_session_title_error(
    state: &AppState,
    request_id: Uuid,
    upstream_http_status: Option<u16>,
    error_kind: &str,
    error_code: &str,
    message: &str,
    connection_id: Uuid,
    connection_scope: &ModelConnectionScope,
    connection_name: &str,
    model_id: &str,
    request_settings: &ModelRequestSettings,
    agent_id: Uuid,
    agent_name: &str,
    user_id: Uuid,
) -> anyhow::Result<()> {
    let display_name: Option<String> =
        sqlx::query_scalar("SELECT display_name FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&state.pool)
            .await?;
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
                 $13, $14, $15, 'user', $16, $17)",
    )
    .bind(Uuid::new_v4())
    .bind(request_id)
    .bind("failed")
    .bind(upstream_http_status.map(i32::from))
    .bind(error_kind)
    .bind(error_code)
    .bind(message)
    .bind(connection_id)
    .bind(model_connection_scope_name(*connection_scope))
    .bind(connection_name)
    .bind(model_id)
    .bind(model_upstream_protocol_name(
        ModelUpstreamProtocol::OpenaiResponses,
    ))
    .bind(model_request_settings_value(request_settings))
    .bind(agent_id)
    .bind(agent_name)
    .bind(user_id)
    .bind(display_name)
    .execute(&state.pool)
    .await?;
    Ok(())
}

pub(crate) async fn list_hub_sessions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<HubSessionDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, owner_id, agent_id,
                (SELECT name FROM agents WHERE agents.id = hub_sessions.agent_id) AS agent_name,
                (SELECT deleted_at FROM agents WHERE agents.id = hub_sessions.agent_id)
                    AS agent_deleted_at,
                origin_kind, origin_platform_id,
                (SELECT name FROM external_platforms
                 WHERE external_platforms.id = hub_sessions.origin_platform_id)
                    AS origin_platform_name,
                title,
                origin_tenant_id, origin_external_identity_id, lifecycle_status,
                native_session_id, active_turn_id, history_checkpoint,
                configuration_fingerprint, runtime_owner_id, ownership_generation,
                recovery_error, current_bundle_generation,
                current_bundle_object_key, current_bundle_checksum_sha256,
                current_bundle_size_bytes, current_bundle_history_checkpoint,
                current_bundle_ownership_generation,
                current_bundle_producing_engine_version,
                current_bundle_created_at, current_bundle_kind, created_at, updated_at
         FROM hub_sessions
         WHERE owner_id = $1
         ORDER BY created_at DESC, id DESC",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(hub_session_from_row).collect()))
}

pub(crate) struct ParsedMessageMultipart {
    pub(crate) agent_id: Option<Uuid>,
    pub(crate) content: String,
    pub(crate) files: Vec<StagedAttachmentUpload>,
}

pub(crate) async fn parse_message_multipart(
    pool: &PgPool,
    mut multipart: Multipart,
) -> Result<ParsedMessageMultipart, ApiError> {
    let settings = load_system_settings(pool).await?;
    let mut agent_id = None;
    let mut content: Option<String> = None;
    let mut files = Vec::new();
    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(_) => return Err(ApiError::bad_request("message multipart body is invalid")),
        };
        let name = field.name().unwrap_or_default();
        match name {
            "agent_id" => {
                if agent_id.is_some() {
                    return Err(ApiError::bad_request(
                        "message multipart contains more than one agent id",
                    ));
                }
                let mut text = Vec::new();
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad_request("message agent id is invalid"))?
                {
                    if text.len().saturating_add(chunk.len()) > 4096 {
                        return Err(ApiError::bad_request("message agent id is too large"));
                    }
                    text.extend_from_slice(&chunk);
                }
                let text = String::from_utf8(text)
                    .map_err(|_| ApiError::bad_request("message agent id must be UTF-8"))?;
                agent_id = Some(
                    Uuid::parse_str(text.trim())
                        .map_err(|_| ApiError::bad_request("message agent id is invalid"))?,
                );
            }
            "content" => {
                if content.is_some() {
                    return Err(ApiError::bad_request(
                        "message multipart contains more than one content field",
                    ));
                }
                let mut text = Vec::new();
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad_request("message content is invalid"))?
                {
                    if text.len().saturating_add(chunk.len()) > 64 * 1024 {
                        return Err(ApiError::bad_request("message content is too large"));
                    }
                    text.extend_from_slice(&chunk);
                }
                content = Some(
                    String::from_utf8(text)
                        .map_err(|_| ApiError::bad_request("message content must be UTF-8"))?,
                );
            }
            "file" => {
                if files.len() >= 10 {
                    return Err(ApiError::bad_request("too many attachments in one message"));
                }
                let mut bytes = Vec::new();
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                while let Some(chunk) = field.chunk().await.map_err(|_| {
                    if size > settings.max_attachment_upload_bytes as u64 {
                        ApiError::payload_too_large(
                            "message attachment exceeds the upload size limit",
                        )
                    } else {
                        ApiError::bad_request("message attachment body is invalid")
                    }
                })? {
                    size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
                        ApiError::bad_request("message attachment exceeds the 100MB limit")
                    })?;
                    if size > settings.max_attachment_upload_bytes as u64 {
                        return Err(ApiError::payload_too_large(
                            "message attachment exceeds the upload size limit",
                        ));
                    }
                    hasher.update(&chunk);
                    bytes.extend_from_slice(&chunk);
                }
                if bytes.is_empty() {
                    return Err(ApiError::bad_request(
                        "message attachment must not be empty",
                    ));
                }
                files.push(StagedAttachmentUpload {
                    session_id: None,
                    name: sanitize_attachment_file_name(field.file_name())?,
                    content_type: sanitize_attachment_content_type(field.content_type())?,
                    checksum_sha256: format!("{:x}", hasher.finalize()),
                    bytes,
                });
            }
            _ => {
                return Err(ApiError::bad_request(
                    "message multipart contains an unsupported field",
                ));
            }
        }
    }
    let content = content
        .filter(|value| !value.trim().is_empty())
        .ok_or(ApiError::bad_request("message content is required"))?;
    Ok(ParsedMessageMultipart {
        agent_id,
        content,
        files,
    })
}

pub(crate) async fn store_attachments_for_message(
    state: &AppState,
    session_id: Uuid,
    owner_id: Uuid,
    files: Vec<StagedAttachmentUpload>,
) -> Result<Vec<Uuid>, ApiError> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = state.pool.begin().await?;
    let session_owner_id: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(session_id)
            .fetch_optional(&mut *tx)
            .await?;
    let session_owner_id = session_owner_id.ok_or(ApiError::not_found("session not found"))?;
    if session_owner_id != owner_id {
        return Err(ApiError::not_found("session not found"));
    }
    let current_total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(size_bytes), 0)::bigint
         FROM hub_session_attachments WHERE session_id = $1",
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    let incoming_total: i64 = files.iter().try_fold(0i64, |total, file| {
        i64::try_from(file.bytes.len())
            .map_err(|_| ApiError::bad_request("attachment file is too large"))
            .and_then(|size| {
                total
                    .checked_add(size)
                    .ok_or_else(|| ApiError::bad_request("attachment file is too large"))
            })
    })?;
    if current_total
        .checked_add(incoming_total)
        .is_none_or(|total| total > MAX_ATTACHMENT_BYTES_PER_SESSION)
    {
        return Err(ApiError::bad_request(
            "session attachment storage limit exceeded",
        ));
    }
    let store = state.session_bundle_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("Attachment object storage is not configured")
    })?;
    let mut uploaded_keys = Vec::new();
    let mut attachment_ids = Vec::new();
    let result: Result<(), ApiError> = async {
        for file in files {
            let attachment_id = Uuid::new_v4();
            let object_key = format!("attachments/{session_id}/{attachment_id}");
            let body_bytes = Bytes::from(file.bytes);
            let size = body_bytes.len() as u64;
            let checksum = file.checksum_sha256.clone();
            if store
                .put_stream(
                    &object_key,
                    size,
                    &checksum,
                    futures_util::stream::once(async move { Ok::<_, std::io::Error>(body_bytes) }),
                )
                .await
                .is_err()
            {
                return Err(ApiError::bad_gateway("attachment object upload failed"));
            }
            uploaded_keys.push(object_key.clone());
            sqlx::query(
                "INSERT INTO hub_session_attachments
                     (id, session_id, owner_id, name, content_type, size_bytes,
                      object_key, checksum_sha256)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(attachment_id)
            .bind(session_id)
            .bind(owner_id)
            .bind(&file.name)
            .bind(&file.content_type)
            .bind(size as i64)
            .bind(&object_key)
            .bind(&checksum)
            .execute(&mut *tx)
            .await?;
            attachment_ids.push(attachment_id);
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            tx.commit().await?;
            Ok(attachment_ids)
        }
        Err(error) => {
            tx.rollback().await?;
            for object_key in uploaded_keys {
                let _ = store.delete(&object_key).await;
            }
            Err(error)
        }
    }
}

pub(crate) async fn create_session_with_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Result<Json<SessionMessageAcceptanceDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let parsed = parse_message_multipart(&state.pool, multipart).await?;
    let agent_id = parsed
        .agent_id
        .ok_or(ApiError::bad_request("agent_id is required"))?;
    let agent = load_agent_for_user(&state.pool, agent_id, &user).await?;
    if !agent.can_invoke {
        return Err(ApiError::forbidden("agent is not invocable"));
    }
    let mut tx = state.pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent_id, user.id).await?;
    let session_id = insert_hub_native_session_tx(&mut tx, user.id, agent_id).await?;
    tx.commit().await?;
    let attachment_ids =
        match store_attachments_for_message(&state, session_id, user.id, parsed.files).await {
            Ok(ids) => ids,
            Err(error) => {
                let _ = sqlx::query("DELETE FROM hub_sessions WHERE id = $1")
                    .bind(session_id)
                    .execute(&state.pool)
                    .await;
                return Err(error);
            }
        };
    let mut tx = state.pool.begin().await?;
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id,
            agent_id,
            owner_id: user.id,
            content: parsed.content,
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: None,
            source: "console".into(),
            automation_id: None,
            integration_session_id: None,
            parent_run_id: None,
            continuation_turn_id: None,
            model_subject_type: "user".into(),
            model_subject_user_id: Some(user.id),
            model_source_integration_app_id: None,
            external_user_context: None,
            attachment_ids,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(Json(accepted))
}

pub(crate) async fn create_session_message_with_attachments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    multipart: Multipart,
) -> Result<Json<SessionMessageAcceptanceDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let parsed = parse_message_multipart(&state.pool, multipart).await?;
    let session = sqlx::query(
        "SELECT agent_id, origin_kind
         FROM hub_sessions
         WHERE id = $1 AND owner_id = $2",
    )
    .bind(session_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<String, _>("origin_kind") != "hub_native" {
        return Err(ApiError::conflict(
            "External Sessions are read-only in the Hub console",
        ));
    }
    let agent_id: Uuid = session.get("agent_id");
    let missing_grants = missing_secret_grants(&state.pool, user.id, agent_id).await?;
    if !missing_grants.is_empty() {
        return Err(ApiError::requires_secret_grants(missing_grants));
    }
    let attachment_ids =
        store_attachments_for_message(&state, session_id, user.id, parsed.files).await?;
    let mut tx = state.pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent_id, user.id).await?;
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id,
            agent_id,
            owner_id: user.id,
            content: parsed.content,
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: None,
            source: "console".into(),
            automation_id: None,
            integration_session_id: None,
            parent_run_id: None,
            continuation_turn_id: None,
            model_subject_type: "user".into(),
            model_subject_user_id: Some(user.id),
            model_source_integration_app_id: None,
            external_user_context: None,
            attachment_ids,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(Json(accepted))
}

pub(crate) async fn get_hub_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<Json<HubSessionDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let row = sqlx::query(
        "SELECT id, owner_id, agent_id,
                (SELECT name FROM agents WHERE agents.id = hub_sessions.agent_id) AS agent_name,
                (SELECT deleted_at FROM agents WHERE agents.id = hub_sessions.agent_id)
                    AS agent_deleted_at,
                origin_kind, origin_platform_id,
                (SELECT name FROM external_platforms
                 WHERE external_platforms.id = hub_sessions.origin_platform_id)
                    AS origin_platform_name,
                title,
                origin_tenant_id, origin_external_identity_id, lifecycle_status,
                native_session_id, active_turn_id, history_checkpoint,
                configuration_fingerprint, runtime_owner_id, ownership_generation,
                recovery_error, current_bundle_generation,
                current_bundle_object_key, current_bundle_checksum_sha256,
                current_bundle_size_bytes, current_bundle_history_checkpoint,
                current_bundle_ownership_generation,
                current_bundle_producing_engine_version,
                current_bundle_created_at, current_bundle_kind, created_at, updated_at
         FROM hub_sessions
         WHERE id = $1 AND owner_id = $2",
    )
    .bind(session_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    Ok(Json(hub_session_from_row(row)))
}

pub(crate) async fn delete_hub_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let guard = load_session_deletion_guard_tx(&mut tx, session_id, Some(user.id)).await?;
    let outcome = delete_session_rows_tx(&mut tx, session_id, guard).await?;
    tx.commit().await?;
    delete_session_object_store_entries(&state, session_id, &outcome).await;
    Ok(StatusCode::NO_CONTENT)
}

/// 删除前校验所需的会话行快照（调用方必须持有 FOR UPDATE 行锁）。
pub(crate) struct SessionDeletionGuard {
    pub(crate) lifecycle_status: String,
    pub(crate) runtime_owner_id: Option<Uuid>,
    pub(crate) ownership_generation: i64,
    pub(crate) current_bundle_object_key: Option<String>,
}

pub(crate) struct SessionDeletionOutcome {
    pub(crate) attachment_object_keys: Vec<String>,
    pub(crate) tool_result_object_keys: Vec<String>,
    pub(crate) bundle_object_key: Option<String>,
}

/// 按 owner 查询会话删除守卫行（用户态 /api/sessions 删除路径）。
pub(crate) async fn load_session_deletion_guard_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    owner_id: Option<Uuid>,
) -> Result<SessionDeletionGuard, ApiError> {
    let row = sqlx::query(
        "SELECT lifecycle_status, runtime_owner_id, ownership_generation,
                current_bundle_object_key
         FROM hub_sessions
         WHERE id = $1 AND ($2::uuid IS NULL OR owner_id = $2)
         FOR UPDATE",
    )
    .bind(session_id)
    .bind(owner_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| ApiError::not_found("session not found"))?;
    Ok(SessionDeletionGuard {
        lifecycle_status: row.get("lifecycle_status"),
        runtime_owner_id: row.get("runtime_owner_id"),
        ownership_generation: row.get("ownership_generation"),
        current_bundle_object_key: row.get("current_bundle_object_key"),
    })
}

/// 删除会话的全部关联数据（runs、消息、附件、集成记录、turns 等）。
/// 调用方负责：先锁定会话行并通过 load_session_deletion_guard_tx 构造守卫，
/// 随后在本函数返回后提交事务，再调用 delete_session_object_store_entries 清理对象存储。
pub(crate) async fn delete_session_rows_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    guard: SessionDeletionGuard,
) -> Result<SessionDeletionOutcome, ApiError> {
    let lifecycle_status: &str = guard.lifecycle_status.as_str();
    if matches!(
        lifecycle_status,
        "waiting_for_runtime" | "restoring" | "online" | "saving"
    ) {
        return Err(ApiError::conflict(
            "session is active and cannot be deleted",
        ));
    }
    let has_active_run: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs
             WHERE hub_session_id = $1
               AND status NOT IN ('completed', 'failed', 'cancelled', 'interrupted')
         )",
    )
    .bind(session_id)
    .fetch_one(&mut **tx)
    .await?;
    if has_active_run {
        return Err(ApiError::conflict("session has an active run"));
    }
    if let Some(runtime_id) = guard.runtime_owner_id {
        record_runtime_session_cleanup_tx(
            &mut *tx,
            runtime_id,
            session_id,
            guard.ownership_generation,
            None,
        )
        .await?;
    }

    sqlx::query("UPDATE hub_sessions SET active_turn_id = NULL WHERE id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "UPDATE hub_session_messages
         SET run_id = NULL, turn_id = NULL
         WHERE session_id = $1",
    )
    .bind(session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM integration_attachments
         WHERE hub_message_id IN (
             SELECT id FROM hub_session_messages WHERE session_id = $1
         ) OR run_id IN (
             SELECT id FROM runs WHERE hub_session_id = $1
         )",
    )
    .bind(session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM integration_messages
         WHERE hub_message_id IN (
             SELECT id FROM hub_session_messages WHERE session_id = $1
         ) OR run_id IN (
             SELECT id FROM runs WHERE hub_session_id = $1
         )",
    )
    .bind(session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM embed_sessions WHERE hub_session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    let tool_result_object_keys = sqlx::query_scalar::<_, String>(
        "SELECT 'tool-results/' || run_id::text || '/' || artifact_id::text
         FROM integration_tool_requests
         WHERE (hub_session_id = $1
            OR run_id IN (SELECT id FROM runs WHERE hub_session_id = $1))
           AND artifact_id IS NOT NULL",
    )
    .bind(session_id)
    .fetch_all(&mut **tx)
    .await?;
    sqlx::query(
        "DELETE FROM integration_tool_requests
         WHERE hub_session_id = $1
            OR run_id IN (SELECT id FROM runs WHERE hub_session_id = $1)",
    )
    .bind(session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM integration_sessions WHERE hub_session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM session_bundle_deletion_queue WHERE session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM runs WHERE hub_session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    let attachment_object_keys = sqlx::query_scalar::<_, String>(
        "SELECT object_key
         FROM hub_session_attachments WHERE session_id = $1
         ORDER BY object_key",
    )
    .bind(session_id)
    .fetch_all(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM hub_session_attachments WHERE session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM hub_session_messages WHERE session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM hub_session_turns WHERE session_id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM hub_sessions WHERE id = $1")
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
    Ok(SessionDeletionOutcome {
        attachment_object_keys,
        tool_result_object_keys,
        bundle_object_key: guard.current_bundle_object_key,
    })
}

/// 事务提交后清理会话关联的对象存储条目（bundle 与附件）。
pub(crate) async fn delete_session_object_store_entries(
    state: &Arc<AppState>,
    session_id: Uuid,
    outcome: &SessionDeletionOutcome,
) {
    if let (Some(object_key), Some(store)) = (
        outcome.bundle_object_key.as_deref(),
        state.session_bundle_store.as_ref(),
    ) {
        if let Err(error) = store.delete(object_key).await {
            warn!(session_id = %session_id, object_key = %object_key, error = %error,
                "failed to delete Session Bundle object after Session deletion");
        }
    }
    if let Some(store) = state.session_bundle_store.as_ref() {
        for object_key in &outcome.attachment_object_keys {
            if let Err(error) = store.delete(object_key).await {
                warn!(session_id = %session_id, object_key = %object_key, error = %error,
                    "failed to delete Attachment object after Session deletion");
            }
        }
        for object_key in &outcome.tool_result_object_keys {
            if let Err(error) = store.delete(object_key).await {
                warn!(session_id = %session_id, object_key = %object_key, error = %error,
                    "failed to delete Tool Result artifact after Session deletion");
            }
        }
    }
}

/// Client（Widget）侧删除会话：按 client 凭证作用域定位会话后执行删除。
pub(crate) async fn delete_widget_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    if !credential.history_enabled {
        return Err(ApiError::forbidden("Widget history is disabled"));
    }
    // Client 会话的规范 id 是 integration_session_id（external 模式），
    // 与 list/messages/events 等端点保持一致，否则删除时作用域定位会 404。
    let (integration_session_id, hub_session_id) = widget_session_locator(&credential, session_id);
    let scoped = load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        integration_session_id,
        hub_session_id,
        true,
    )
    .await?;
    let guard = load_session_deletion_guard_tx(&mut tx, scoped.hub_session_id, None).await?;
    let outcome = delete_session_rows_tx(&mut tx, scoped.hub_session_id, guard).await?;
    tx.commit().await?;
    delete_session_object_store_entries(&state, scoped.hub_session_id, &outcome).await;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SessionMessageListQuery {
    pub(crate) before_sequence: Option<i64>,
    pub(crate) limit: Option<i64>,
}

impl SessionMessageListQuery {
    pub(crate) fn validated(self) -> Result<(Option<i64>, Option<i64>), ApiError> {
        if self.before_sequence.is_some_and(|sequence| sequence < 1)
            || self.limit.is_some_and(|limit| !(1..=100).contains(&limit))
        {
            return Err(ApiError::bad_request("invalid Session message pagination"));
        }
        Ok((self.before_sequence, self.limit))
    }
}

pub(crate) async fn list_hub_session_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<SessionMessageListQuery>,
) -> Result<Json<Vec<HubSessionMessageDto>>, ApiError> {
    let (before_sequence, limit) = query.validated()?;
    let user = require_user(&state, &headers).await?;
    let owned: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM hub_sessions WHERE id = $1 AND owner_id = $2
         )",
    )
    .bind(session_id)
    .bind(user.id)
    .fetch_one(&state.pool)
    .await?;
    if !owned {
        return Err(ApiError::not_found("session not found"));
    }
    let rows = sqlx::query(SESSION_MESSAGE_PAGE_SQL)
        .bind(session_id)
        .bind(before_sequence)
        .bind(limit)
        .fetch_all(&state.pool)
        .await?;
    let mut messages = rows
        .into_iter()
        .map(hub_message_from_row)
        .collect::<Vec<_>>();
    fill_message_attachments(&state.pool, &mut messages).await?;
    Ok(Json(messages))
}

pub(crate) async fn create_hub_session_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<CreateHubSessionMessageRequest>,
) -> Result<Json<SessionMessageAcceptanceDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let session = sqlx::query(
        "SELECT agent_id, origin_kind
         FROM hub_sessions
         WHERE id = $1 AND owner_id = $2",
    )
    .bind(session_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<String, _>("origin_kind") != "hub_native" {
        return Err(ApiError::conflict(
            "External Sessions are read-only in the Hub console",
        ));
    }
    let agent_id: Uuid = session.get("agent_id");
    let missing_grants = missing_secret_grants(&state.pool, user.id, agent_id).await?;
    if !missing_grants.is_empty() {
        return Err(ApiError::requires_secret_grants(missing_grants));
    }
    let mut tx = state.pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent_id, user.id).await?;
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id,
            agent_id,
            owner_id: user.id,
            content: req.content,
            payload: req.payload,
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: req.delivery_mode.unwrap_or_else(|| "next_turn".into()),
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
            attachment_ids: req.attachment_ids,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(Json(accepted))
}

#[derive(Debug, Deserialize)]
pub(crate) struct BindMessageAttachmentsRequest {
    pub(crate) attachment_ids: Vec<Uuid>,
}

pub(crate) async fn bind_message_attachments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((session_id, message_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<BindMessageAttachmentsRequest>,
) -> Result<Json<HubSessionMessageDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let attachment_ids = req.attachment_ids;
    if attachment_ids.is_empty() || attachment_ids.len() > 10 {
        return Err(ApiError::bad_request("bind between 1 and 10 attachments"));
    }
    let mut tx = state.pool.begin().await?;
    let message = sqlx::query(
        "SELECT messages.id, messages.sequence, messages.run_id
         FROM hub_session_messages AS messages
         JOIN hub_sessions AS sessions ON sessions.id = messages.session_id
         WHERE messages.id = $1 AND messages.session_id = $2
           AND sessions.owner_id = $3
         FOR UPDATE OF messages",
    )
    .bind(message_id)
    .bind(session_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("message not found"))?;
    let run_id: Option<Uuid> = message.get("run_id");
    let attachable = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM hub_session_attachments
         WHERE session_id = $1 AND id = ANY($2) AND message_id IS NULL
         FOR UPDATE",
    )
    .bind(session_id)
    .bind(&attachment_ids)
    .fetch_all(&mut *tx)
    .await?;
    if attachable.len() != attachment_ids.len() {
        return Err(ApiError::bad_request(
            "one or more attachments are missing, foreign, or already bound",
        ));
    }
    sqlx::query(
        "UPDATE hub_session_attachments
         SET message_id = $1, run_id = $2
         WHERE session_id = $3 AND id = ANY($4) AND message_id IS NULL",
    )
    .bind(message_id)
    .bind(run_id)
    .bind(session_id)
    .bind(&attachment_ids)
    .execute(&mut *tx)
    .await?;
    let row = sqlx::query(
        "SELECT id, session_id, sequence, role, message_kind, content, payload,
                delivery_mode, delivery_state, client_message_key,
                expected_native_turn_id, turn_id, run_id, accepted_at
         FROM hub_session_messages
         WHERE id = $1 AND session_id = $2",
    )
    .bind(message_id)
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let row = row.ok_or(ApiError::not_found("message not found"))?;
    let mut messages = vec![hub_message_from_row(row)];
    fill_message_attachments(&mut *tx, &mut messages).await?;
    tx.commit().await?;
    Ok(Json(messages.remove(0)))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct AttachmentUploadQuery {
    pub(crate) session_id: Option<Uuid>,
}

pub(crate) struct StagedAttachmentUpload {
    pub(crate) session_id: Option<Uuid>,
    pub(crate) name: String,
    pub(crate) content_type: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) checksum_sha256: String,
}

pub(crate) async fn upload_attachment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AttachmentUploadQuery>,
    multipart: Multipart,
) -> Result<Json<HubSessionAttachmentDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let staged = stage_attachment_upload(&state.pool, multipart).await?;
    let session_id = resolve_attachment_session_id(query.session_id, staged.session_id)?;
    upload_attachment_to_session(&state, session_id, user.id, staged).await
}

pub(crate) async fn upload_widget_attachment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<AttachmentUploadQuery>,
    multipart: Multipart,
) -> Result<Json<HubSessionAttachmentDto>, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    tx.commit().await?;
    let staged = stage_attachment_upload(&state.pool, multipart).await?;
    let session_id = resolve_attachment_session_id(query.session_id, staged.session_id)?;
    let mut tx = state.pool.begin().await?;
    let scoped = load_widget_scoped_session_tx(&mut tx, &credential, None, Some(session_id), false)
        .await
        .map_err(|error| {
            if error.status == StatusCode::NOT_FOUND {
                ApiError::not_found("Widget Session not found")
            } else {
                error
            }
        })?;
    tx.commit().await?;
    upload_attachment_to_session(&state, scoped.hub_session_id, credential.owner_id, staged).await
}

pub(crate) async fn download_attachment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(attachment_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let user = require_user(&state, &headers).await?;
    let row = load_attachment_with_session_owner(&state.pool, attachment_id).await?;
    let session_owner_id: Uuid = row.get("session_owner_id");
    if session_owner_id != user.id && !is_admin_role(&user.role) {
        return Err(ApiError::not_found("attachment not found"));
    }
    serve_attachment_row(&state, row).await
}

pub(crate) async fn download_runtime_attachment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(attachment_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let row = sqlx::query(
        "SELECT a.id, a.session_id, a.name, a.content_type, a.size_bytes, a.object_key
         FROM hub_session_attachments AS a
         JOIN hub_sessions AS s ON s.id = a.session_id
         WHERE a.id = $1 AND s.runtime_owner_id = $2",
    )
    .bind(attachment_id)
    .bind(runtime_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("attachment not found"))?;
    serve_attachment_row(&state, row).await
}

pub(crate) async fn download_widget_attachment(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(attachment_id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    let row = sqlx::query(
        "SELECT a.id, a.session_id, a.name, a.content_type, a.size_bytes, a.object_key
         FROM hub_session_attachments AS a
         WHERE a.id = $1",
    )
    .bind(attachment_id)
    .fetch_optional(&mut *tx)
    .await?;
    let row = row.ok_or(ApiError::not_found("attachment not found"))?;
    let attachment_session_id: Uuid = row.get("session_id");
    load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        None,
        Some(attachment_session_id),
        false,
    )
    .await
    .map_err(|error| {
        if error.status == StatusCode::NOT_FOUND {
            ApiError::not_found("attachment not found")
        } else {
            error
        }
    })?;
    tx.commit().await?;
    serve_attachment_row(&state, row).await
}

pub(crate) async fn stage_attachment_upload(
    pool: &PgPool,
    mut multipart: Multipart,
) -> Result<StagedAttachmentUpload, ApiError> {
    let settings = load_system_settings(pool).await?;
    let mut session_id = None;
    let mut file_name: Option<String> = None;
    let mut content_type: Option<String> = None;
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut checksum_sha256: Option<String> = None;
    loop {
        let mut field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(_) => {
                return Err(ApiError::bad_request(
                    "attachment multipart body is invalid",
                ))
            }
        };
        match field.name() {
            Some("session_id") => {
                if session_id.is_some() {
                    return Err(ApiError::bad_request(
                        "attachment multipart contains more than one session id",
                    ));
                }
                let mut text = Vec::new();
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad_request("attachment session id is invalid"))?
                {
                    if text.len().saturating_add(chunk.len()) > 4096 {
                        return Err(ApiError::bad_request("attachment session id is too large"));
                    }
                    text.extend_from_slice(&chunk);
                }
                let text = String::from_utf8(text)
                    .map_err(|_| ApiError::bad_request("attachment session id must be UTF-8"))?;
                let parsed = Uuid::parse_str(text.trim())
                    .map_err(|_| ApiError::bad_request("attachment session id is invalid"))?;
                session_id = Some(parsed);
            }
            Some("file") => {
                if file_bytes.is_some() {
                    return Err(ApiError::bad_request(
                        "attachment multipart contains more than one file",
                    ));
                }
                let mut bytes = Vec::new();
                let mut hasher = Sha256::new();
                let mut size = 0_u64;
                while let Some(chunk) = field.chunk().await.map_err(|_| {
                    if size > settings.max_attachment_upload_bytes as u64 {
                        ApiError::payload_too_large("attachment file exceeds the upload size limit")
                    } else {
                        ApiError::bad_request("attachment file body is invalid")
                    }
                })? {
                    size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
                        ApiError::bad_request("attachment file exceeds the 100MB limit")
                    })?;
                    if size > settings.max_attachment_upload_bytes as u64 {
                        return Err(ApiError::payload_too_large(
                            "attachment file exceeds the upload size limit",
                        ));
                    }
                    hasher.update(&chunk);
                    bytes.extend_from_slice(&chunk);
                }
                file_name = field.file_name().map(str::to_owned);
                content_type = field.content_type().map(str::to_owned);
                file_bytes = Some(bytes);
                checksum_sha256 = Some(format!("{:x}", hasher.finalize()));
            }
            _ => {
                return Err(ApiError::bad_request(
                    "attachment multipart contains an unsupported field",
                ));
            }
        }
    }
    let bytes = file_bytes.ok_or(ApiError::bad_request("attachment file is required"))?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request("attachment file must not be empty"));
    }
    let name = sanitize_attachment_file_name(file_name.as_deref())?;
    let content_type = sanitize_attachment_content_type(content_type.as_deref())?;
    Ok(StagedAttachmentUpload {
        session_id,
        name,
        content_type,
        bytes,
        checksum_sha256: checksum_sha256.expect("file bytes set the checksum"),
    })
}

pub(crate) fn sanitize_attachment_file_name(value: Option<&str>) -> Result<String, ApiError> {
    let value = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::bad_request("attachment file name is required"))?;
    let value = value.rsplit(['/', '\\']).next().unwrap_or(value);
    if value.is_empty() || value.chars().count() > 255 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request("attachment file name is invalid"));
    }
    Ok(value.to_owned())
}

pub(crate) fn sanitize_attachment_content_type(value: Option<&str>) -> Result<String, ApiError> {
    let value = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream");
    if value.chars().count() > 255 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request("attachment content type is invalid"));
    }
    Ok(value.to_owned())
}

pub(crate) fn resolve_attachment_session_id(
    query_session_id: Option<Uuid>,
    field_session_id: Option<Uuid>,
) -> Result<Uuid, ApiError> {
    match (query_session_id, field_session_id) {
        (Some(query), Some(field)) if query != field => Err(ApiError::bad_request(
            "attachment session id does not match",
        )),
        (Some(query), _) => Ok(query),
        (None, Some(field)) => Ok(field),
        (None, None) => Err(ApiError::bad_request("attachment session id is required")),
    }
}

pub(crate) async fn upload_attachment_to_session(
    state: &AppState,
    session_id: Uuid,
    owner_id: Uuid,
    staged: StagedAttachmentUpload,
) -> Result<Json<HubSessionAttachmentDto>, ApiError> {
    let mut tx = state.pool.begin().await?;
    let session_owner_id: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(session_id)
            .fetch_optional(&mut *tx)
            .await?;
    let session_owner_id = session_owner_id.ok_or(ApiError::not_found("session not found"))?;
    if session_owner_id != owner_id {
        return Err(ApiError::not_found("session not found"));
    }
    let current_total: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(size_bytes), 0)::bigint
         FROM hub_session_attachments WHERE session_id = $1",
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    let size_bytes = i64::try_from(staged.bytes.len())
        .map_err(|_| ApiError::bad_request("attachment file is too large"))?;
    let session_bytes_limit = load_system_settings(&state.pool)
        .await?
        .max_attachment_bytes_per_session;
    if current_total
        .checked_add(size_bytes)
        .is_none_or(|total| total > session_bytes_limit)
    {
        return Err(ApiError::bad_request(
            "session attachment storage limit exceeded",
        ));
    }
    let store = state.session_bundle_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("Attachment object storage is not configured")
    })?;
    let attachment_id = Uuid::new_v4();
    let object_key = format!("attachments/{session_id}/{attachment_id}");
    let body_bytes = Bytes::from(staged.bytes);
    let size = body_bytes.len() as u64;
    let checksum = staged.checksum_sha256.clone();
    if let Err(error) = store
        .put_stream(
            &object_key,
            size,
            &checksum,
            futures_util::stream::once(async move { Ok::<_, std::io::Error>(body_bytes) }),
        )
        .await
    {
        warn!(session_id = %session_id, object_key = %object_key, error = %error,
            "Attachment object upload failed");
        return Err(ApiError::bad_gateway("Attachment object upload failed"));
    }
    let row = sqlx::query(
        "INSERT INTO hub_session_attachments
             (id, session_id, owner_id, name, content_type, size_bytes,
              object_key, checksum_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id, session_id, name, content_type, size_bytes, created_at",
    )
    .bind(attachment_id)
    .bind(session_id)
    .bind(session_owner_id)
    .bind(&staged.name)
    .bind(&staged.content_type)
    .bind(size_bytes)
    .bind(&object_key)
    .bind(&checksum)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(hub_session_attachment_from_row(row)))
}

pub(crate) async fn load_attachment_with_session_owner(
    pool: &PgPool,
    attachment_id: Uuid,
) -> Result<sqlx::postgres::PgRow, ApiError> {
    sqlx::query(
        "SELECT a.id, a.session_id, a.name, a.content_type, a.size_bytes, a.object_key,
                s.owner_id AS session_owner_id
         FROM hub_session_attachments AS a
         JOIN hub_sessions AS s ON s.id = a.session_id
         WHERE a.id = $1",
    )
    .bind(attachment_id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("attachment not found"))
}

pub(crate) async fn serve_attachment_row(
    state: &AppState,
    row: sqlx::postgres::PgRow,
) -> Result<Response, ApiError> {
    let object_key: String = row.get("object_key");
    let name: String = row.get("name");
    let content_type: String = row.get("content_type");
    let store = state.session_bundle_store.as_ref().ok_or_else(|| {
        ApiError::service_unavailable("Attachment object storage is not configured")
    })?;
    let object = match store.get(&object_key).await {
        Ok(object) => object,
        Err(error) => {
            warn!(object_key = %object_key, error = %error,
                "Attachment object download failed");
            return Err(ApiError::not_found("attachment object not found"));
        }
    };
    if !object.status().is_success() {
        return Err(ApiError::not_found("attachment object not found"));
    }
    let mut response = Response::new(Body::from_stream(
        object
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other)),
    ));
    *response.status_mut() = StatusCode::OK;
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let disposition = if content_type.starts_with("image/") {
        "inline".to_owned()
    } else {
        format!(
            "attachment; filename*=UTF-8''{}",
            attachment_filename_encoding(&name)
        )
    };
    response_headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
    );
    if let Ok(size_bytes) = row.try_get::<i64, _>("size_bytes") {
        if let Ok(value) = HeaderValue::from_str(&size_bytes.to_string()) {
            response_headers.insert(header::CONTENT_LENGTH, value);
        }
    }
    Ok(response)
}

pub(crate) fn attachment_filename_encoding(name: &str) -> String {
    let mut encoded = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push_str(&format!("{byte:02X}"));
            }
        }
    }
    encoded
}

pub(crate) fn hub_session_attachment_from_row(
    row: sqlx::postgres::PgRow,
) -> HubSessionAttachmentDto {
    HubSessionAttachmentDto {
        id: row.get("id"),
        session_id: row.get("session_id"),
        name: row.get("name"),
        content_type: row.get("content_type"),
        size_bytes: row.get("size_bytes"),
        created_at: row.get("created_at"),
    }
}

pub(crate) async fn load_attachments_for_session_messages<'e, E>(
    executor: E,
    message_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<HubSessionAttachmentDto>>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    if message_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT id, message_id, session_id, name, content_type, size_bytes, created_at
         FROM hub_session_attachments
         WHERE message_id = ANY($1)
         ORDER BY created_at, id",
    )
    .bind(message_ids)
    .fetch_all(executor)
    .await?;
    let mut by_message: HashMap<Uuid, Vec<HubSessionAttachmentDto>> = HashMap::new();
    for row in rows {
        by_message
            .entry(row.get("message_id"))
            .or_default()
            .push(hub_session_attachment_from_row(row));
    }
    Ok(by_message)
}

pub(crate) async fn fill_message_attachments<'e, E>(
    executor: E,
    messages: &mut [HubSessionMessageDto],
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let message_ids = messages
        .iter()
        .map(|message| message.id)
        .collect::<Vec<_>>();
    let attachments = load_attachments_for_session_messages(executor, &message_ids).await?;
    for message in messages {
        message.attachments = attachments.get(&message.id).cloned().unwrap_or_default();
    }
    Ok(())
}

pub(crate) async fn runtime_attachment_orphan_loop(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(30 * 60));
    loop {
        tick.tick().await;
        if let Err(error) = cleanup_attachment_orphans(&state).await {
            warn!(error = %error, "attachment orphan cleanup failed");
        }
    }
}

pub(crate) async fn cleanup_attachment_orphans(state: &AppState) -> Result<(), anyhow::Error> {
    let mut tx = state.pool.begin().await?;
    let object_keys = sqlx::query_scalar::<_, String>(
        "SELECT object_key
         FROM hub_session_attachments
         WHERE message_id IS NULL AND created_at < now() - interval '24 hours'
         ORDER BY created_at, object_key",
    )
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM hub_session_attachments
         WHERE message_id IS NULL AND created_at < now() - interval '24 hours'",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    if let Some(store) = state.session_bundle_store.as_ref() {
        for object_key in object_keys {
            if let Err(error) = store.delete(&object_key).await {
                warn!(object_key = %object_key, error = %error,
                    "failed to delete orphan Attachment object");
            }
        }
    }
    Ok(())
}

pub(crate) async fn get_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(load_run_for_user(&state.pool, run_id, &user).await?))
}

pub(crate) async fn stop_hub_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let session = sqlx::query(
        "SELECT runs.hub_session_id, sessions.origin_kind
         FROM runs
         JOIN hub_sessions AS sessions ON sessions.id = runs.hub_session_id
         WHERE runs.id = $1 AND sessions.owner_id = $2",
    )
    .bind(run_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("run not found"))?;
    if session.get::<String, _>("origin_kind") != "hub_native" {
        return Err(ApiError::conflict(
            "External Sessions are read-only in the Hub console",
        ));
    }
    let hub_session_id: Uuid = session.get("hub_session_id");
    let run = request_run_interrupt_tx(&mut tx, run_id, hub_session_id).await?;
    tx.commit().await?;
    Ok(Json(run))
}

/// 强制停止（硬停止）：作废当前活动任务 A（interrupted）+ 会话版本号 +1 +
/// 消息 held/ambiguity 义务 + 创建 operation 与 force_stop 命令（NOTIFY 唤醒）。
/// 权限：会话属主或显式授权管理员（super_admin）。request_id 必填且幂等。
pub(crate) async fn force_stop_hub_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<ForceStopRequest>,
) -> Result<(StatusCode, Json<ForceStopOperationDto>), ApiError> {
    let request_id = req.request_id.trim().to_owned();
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(ApiError::bad_request(
            "request_id is required (<=128 chars)",
        ));
    }
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let (hub_session_id, session_owner_id): (Uuid, Uuid) = sqlx::query_as(
        "SELECT runs.hub_session_id, sessions.owner_id
         FROM runs
         JOIN hub_sessions AS sessions ON sessions.id = runs.hub_session_id
         WHERE runs.id = $1
         FOR UPDATE OF runs, sessions",
    )
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("run not found"))?;
    if session_owner_id != user.id && user.role != "super_admin" {
        return Err(ApiError::forbidden(
            "force stop requires the Session owner or an explicit administrator",
        ));
    }
    let (dto, created) = force_stop_run_core_tx(
        &mut tx,
        run_id,
        hub_session_id,
        &request_id,
        req.expected_generation,
        false, // 控制台：仅 hub_native 会话。
    )
    .await?;
    // 新建的 operation 必须绑定确定的目标 runtime（核心已校验归属），
    // 否则回滚（不静默吞掉推送）。
    if created && dto.target_runtime_id.is_none() {
        return Err(ApiError::internal(
            "force stop operation was created without a target runtime",
        ));
    }
    tx.commit().await?;
    // 幂等语义：终态返回首次结果（200）；未完成返回 202（不重复创建）。
    let status = if matches!(
        dto.state.as_str(),
        "succeeded" | "snapshot_lost" | "abandoned"
    ) {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    // 仅本次新建且 target runtime 确定时推送停止命令（连接不在线由上报兜底重推）。
    if created {
        if let Some(target_runtime) = dto.target_runtime_id {
            crate::runtime_ws::push_force_stop_command(
                &state,
                target_runtime,
                dto.operation_id,
                hub_session_id,
                run_id,
            )
            .await;
        }
    }
    Ok((status, Json(dto)))
}

/// force-stop 共享核心（事务内）：校验会话状态/归属代次、幂等、
/// 终结活动 run、创建 operation、会话转 force_stopping。
/// 调用方负责鉴权与事务提交；错误时事务由调用方回滚。
pub(crate) async fn force_stop_run_core_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    hub_session_id: Uuid,
    request_id: &str,
    expected_generation: Option<i64>,
    allow_external: bool,
) -> Result<(ForceStopOperationDto, bool), ApiError> {
    // Ok((dto, created))：created=false 表示幂等命中既有 operation（调用方不推送）。
    let session = sqlx::query(
        "SELECT origin_kind, lifecycle_status, ownership_generation, runtime_owner_id
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(hub_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    let origin_kind: String = session.get("origin_kind");
    if !allow_external && origin_kind != "hub_native" {
        return Err(ApiError::conflict(
            "External Sessions are read-only in the Hub console",
        ));
    }
    let lifecycle: String = session.get("lifecycle_status");
    if matches!(lifecycle.as_str(), "historical" | "recovery_failed") {
        return Err(ApiError::conflict("session is read-only"));
    }
    let current_generation: i64 = session.get("ownership_generation");
    if let Some(expected) = expected_generation {
        if expected != current_generation {
            return Err(ApiError::conflict("expected generation does not match"));
        }
    }
    // 幂等：同 (session, request_id) 已有 operation → 返回首次结果。
    if let Some(existing) = sqlx::query_as::<_, (Uuid, String)>(
        "SELECT operation_id, state FROM force_stop_operation
         WHERE session_id = $1 AND request_id = $2",
    )
    .bind(hub_session_id)
    .bind(request_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let dto = load_force_stop_operation(tx, existing.0).await?;
        return Ok((dto, false));
    }
    // 当前活动任务 A 必须存在且处于活动状态。
    let a = sqlx::query(
        "SELECT id, status, hub_turn_id FROM runs
         WHERE id = $1 AND hub_session_id = $2 AND status IN ('running', 'waiting_tool')
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(hub_session_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(a) = a else {
        return Err(ApiError::conflict("run is not active (already terminal)"));
    };
    let a_turn_id: Uuid = a.get("hub_turn_id");
    // nonterminal 不变量：同会话其他 running/waiting_tool run → fail closed 409。
    let other_active: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs
             WHERE hub_session_id = $1 AND id <> $2
               AND status IN ('running', 'waiting_tool')
         )",
    )
    .bind(hub_session_id)
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;
    if other_active {
        return Err(ApiError::conflict(
            "session has other active runs; refusing force stop (invariant broken)",
        ));
    }
    // 目标 runtime：无归属 → 409 不改库。
    let target_runtime: Option<Uuid> = session.get("runtime_owner_id");
    let Some(target_runtime) = target_runtime else {
        return Err(ApiError::conflict(
            "session is not owned by a runtime; cannot force stop",
        ));
    };
    // 终结全部非 A pending run（held 其 queued 输入）。
    let pending_runs: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, hub_turn_id FROM runs
         WHERE hub_session_id = $1 AND status = 'pending' AND id <> $2
         FOR UPDATE",
    )
    .bind(hub_session_id)
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    // 终结全部非 A pending run（其 queued 消息无需特殊处理——历史在 DB，
    // 恢复时重建包含；消息绑定已终态 run，恢复时按 sequence 重建）。
    for (pending_run, pending_turn) in pending_runs {
        sqlx::query("UPDATE runs SET status = 'failed', updated_at = now() WHERE id = $1")
            .bind(pending_run)
            .execute(&mut **tx)
            .await?;
        insert_run_event_tx(
            tx,
            pending_run,
            "status".into(),
            None,
            Some("failed".into()),
            json!({ "status": "failed", "reason": "superseded by force stop" }),
        )
        .await?;
        sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
             WHERE id = $1 AND session_id = $2",
        )
        .bind(pending_turn)
        .bind(hub_session_id)
        .execute(&mut **tx)
        .await?;
    }
    // A：interrupted + 事件 + 回合终态。
    sqlx::query("UPDATE runs SET status = 'interrupted', updated_at = now() WHERE id = $1")
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    insert_run_event_tx(
        tx,
        run_id,
        "status".into(),
        None,
        Some("interrupted".into()),
        json!({ "status": "interrupted", "reason": "force stopped by user" }),
    )
    .await?;
    sqlx::query(
        "UPDATE hub_session_turns
         SET status = 'interrupted', ended_at = COALESCE(ended_at, now()), updated_at = now()
         WHERE id = $1 AND session_id = $2",
    )
    .bind(a_turn_id)
    .bind(hub_session_id)
    .execute(&mut **tx)
    .await?;
    let operation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO force_stop_operation
             (operation_id, session_id, run_id, request_id, target_runtime_id, state)
         VALUES ($1, $2, $3, $4, $5, 'pending')",
    )
    .bind(operation_id)
    .bind(hub_session_id)
    .bind(run_id)
    .bind(request_id)
    .bind(target_runtime)
    .execute(&mut **tx)
    .await?;
    // 会话：标记"强制停止中"（保持 runtime 归属与 generation——hub 权威，
    // 由 10s 持有上报与非权威抛弃兜底），清 saving/checkpoint 状态。
    sqlx::query(
        "UPDATE hub_sessions
         SET active_turn_id = NULL,
             lifecycle_status = 'force_stopping', recovery_source = NULL,
             saving_history_checkpoint = NULL, saving_ownership_generation = NULL,
             saving_reason = NULL, saving_checkpoint_attempt_id = NULL,
             last_checkpoint_attempt_id = NULL, last_checkpoint_ownership_generation = NULL,
             last_checkpoint_disposition = NULL, last_checkpoint_has_queued_work = NULL,
             updated_at = now()
         WHERE id = $1",
    )
    .bind(hub_session_id)
    .execute(&mut **tx)
    .await?;
    // 取消 pending/claimed tool 请求。
    sqlx::query(
        "UPDATE integration_tool_requests SET status = 'cancelled'
         WHERE hub_session_id = $1 AND run_id = $2 AND status IN ('pending', 'claimed')",
    )
    .bind(hub_session_id)
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    let dto = load_force_stop_operation(tx, operation_id).await?;
    Ok((dto, true))
}

pub(crate) async fn load_force_stop_operation(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: Uuid,
) -> Result<ForceStopOperationDto, ApiError> {
    let row = sqlx::query(
        "SELECT operation_id, session_id, run_id, request_id, target_runtime_id,
                state, created_at, updated_at, snapshot_uploaded_at
         FROM force_stop_operation WHERE operation_id = $1",
    )
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("force stop operation not found"))?;
    Ok(ForceStopOperationDto {
        operation_id: row.get("operation_id"),
        session_id: row.get("session_id"),
        run_id: row.get("run_id"),
        request_id: row.get("request_id"),
        target_runtime_id: row.get("target_runtime_id"),
        state: row.get("state"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        snapshot_uploaded_at: row.get("snapshot_uploaded_at"),
    })
}

pub(crate) async fn list_run_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
) -> Result<Json<Vec<RunEventDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    load_run_for_user(&state.pool, run_id, &user).await?;
    Ok(Json(load_events_after(&state.pool, run_id, 0).await?))
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventStreamQuery {
    pub(crate) after: Option<i64>,
    pub(crate) limit: Option<i64>,
}

pub(crate) async fn stream_run_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Query(query): Query<EventStreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    authorize_run_stream(&state, &headers, run_id).await?;
    let pool = state.pool.clone();
    let authorization_state = state.clone();
    let authorization_headers = headers.clone();
    let bus = state.run_event_bus.clone();
    let mut last_seq = query.after.unwrap_or(0);
    let event_stream = stream! {
         // Catch-up: persisted events after the client's anchor.
         if let Err(err) = load_events_after(&pool, run_id, last_seq).await {
             yield Ok(Event::default().event("error").data(err.message));
         } else if let Ok(events) = load_events_after(&pool, run_id, last_seq).await {
             for event in events {
                 last_seq = event.seq;
                 let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                 yield Ok(Event::default().event("run_event").id(event.seq.to_string()).data(payload));
             }
         }
         let mut rx = bus.subscribe(run_id);
         let mut ticker = tokio::time::interval(Duration::from_millis(700));
         loop {
             tokio::select! {
                 item = rx.recv() => {
                     match item {
                         Ok(item) => {
                             if item.persisted {
                                 if item.event.seq <= last_seq { continue; }
                                 last_seq = item.event.seq;
                             } else if item.event.seq <= last_seq {
                                 continue;
                             }
                             let payload = serde_json::to_string(&item.event).unwrap_or_else(|_| "{}".into());
                             yield Ok(Event::default().event("run_event").id(item.event.seq.to_string()).data(payload));
                         }
                         Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                         Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                             rx = bus.subscribe(run_id);
                         }
                     }
                 }
                 _ = ticker.tick() => {
                     // 长连接也必须持续检查 token、session 和 Agent 的当前授权状态，
                     // 并兜底补齐可能被广播缓冲丢弃的持久化事件。
                     if let Err(err) = authorize_run_stream(&authorization_state, &authorization_headers, run_id).await {
                         yield Ok(Event::default().event("error").data(err.message));
                         break;
                     }
                     match load_events_after(&pool, run_id, last_seq).await {
                         Ok(events) => {
                             for event in events {
                                 last_seq = event.seq;
                                 let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                                 yield Ok(Event::default().event("run_event").id(event.seq.to_string()).data(payload));
                             }
                         }
                         Err(err) => {
                             yield Ok(Event::default().event("error").data(err.message));
                             break;
                         }
                     }
                 }
             }
         }
    };
    Ok(Sse::new(event_stream).keep_alive(KeepAlive::default()))
}

pub(crate) async fn accept_session_message_tx(
    tx: &mut Transaction<'_, Postgres>,
    mut request: AcceptSessionMessage,
) -> Result<SessionMessageAcceptanceDto, ApiError> {
    let mut content = request.content.trim().to_owned();
    sanitize_run_event_text(&mut content);
    request.content = content.clone();
    request.payload = sanitize_run_event_payload(request.payload);
    if content.is_empty() {
        return Err(ApiError::bad_request("message is required"));
    }
    if request.requested_delivery_mode != "next_turn"
        && request.requested_delivery_mode != "later_turn"
    {
        return Err(ApiError::bad_request("unsupported message delivery mode"));
    }
    let client_message_key = normalize_client_message_key(request.client_message_key.as_deref())?;

    let session = sqlx::query(
        "SELECT lifecycle_status, active_turn_id, configuration_fingerprint,
                ownership_generation, recovery_error
         FROM hub_sessions
         WHERE id = $1 AND owner_id = $2 AND agent_id = $3
         FOR UPDATE",
    )
    .bind(request.session_id)
    .bind(request.owner_id)
    .bind(request.agent_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    let lifecycle_status: String = session.get("lifecycle_status");
    if lifecycle_status == "recovery_failed" || lifecycle_status == "historical" {
        return Err(ApiError::conflict("session is read-only"));
    }
    if session.get::<Option<String>, _>("recovery_error").is_some() {
        sqlx::query("UPDATE hub_sessions SET recovery_error = NULL WHERE id = $1")
            .bind(request.session_id)
            .execute(&mut **tx)
            .await?;
    }

    if let Some(client_message_key) = client_message_key.as_deref() {
        if let Some(message) =
            load_hub_message_by_client_key_tx(tx, request.session_id, client_message_key).await?
        {
            let run = match message.run_id {
                Some(run_id) => Some(load_run_public_tx(tx, run_id).await?),
                None => None,
            };
            return Ok(SessionMessageAcceptanceDto { message, run });
        }
    }

    let parent = if let Some(parent_run_id) = request.parent_run_id {
        let parent: Option<(Uuid, Uuid)> = sqlx::query_as(
            "SELECT hub_session_id, hub_turn_id
             FROM runs
             WHERE id = $1 AND owner_id = $2 AND agent_id = $3
             FOR SHARE",
        )
        .bind(parent_run_id)
        .bind(request.owner_id)
        .bind(request.agent_id)
        .fetch_optional(&mut **tx)
        .await?;
        let parent = parent.ok_or(ApiError::bad_request("resume parent run is not available"))?;
        if parent.0 != request.session_id {
            return Err(ApiError::bad_request(
                "resume parent run belongs to another session",
            ));
        }
        Some((parent_run_id, parent.1))
    } else {
        None
    };

    let attachment_ids = request
        .attachment_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if !attachment_ids.is_empty() {
        let matched = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM hub_session_attachments
             WHERE id = ANY($1) AND session_id = $2 AND message_id IS NULL
             FOR UPDATE",
        )
        .bind(&attachment_ids)
        .bind(request.session_id)
        .fetch_all(&mut **tx)
        .await?;
        if matched.len() != attachment_ids.len() {
            return Err(ApiError::bad_request(
                "one or more attachment ids do not belong to this session or are already bound",
            ));
        }
    }

    let mut selected_run_id = None;
    let mut selected_turn_id = None;
    let mut expected_native_turn_id = None;
    let mut delivery_mode = request.requested_delivery_mode.clone();
    let delivery_state = if delivery_mode == "later_turn" {
        "deferred".to_owned()
    } else {
        "queued".to_owned()
    };

    if delivery_mode != "later_turn" {
        if let Some(continuation_turn_id) = request.continuation_turn_id {
            let Some((parent_run_id, parent_turn_id)) = parent else {
                return Err(ApiError::bad_request(
                    "continued message requires a parent run",
                ));
            };
            if continuation_turn_id != parent_turn_id {
                return Err(ApiError::bad_request(
                    "continued message Turn does not match its parent run",
                ));
            }
            selected_turn_id = Some(continuation_turn_id);
            selected_run_id = Some(
                insert_session_run_tx(tx, &request, continuation_turn_id, Some(parent_run_id))
                    .await?,
            );
        } else {
            if let Some(active_turn_id) = session.get::<Option<Uuid>, _>("active_turn_id") {
                let active: (Uuid, Option<String>, Option<DateTime<Utc>>) = sqlx::query_as(
                    "SELECT runs.id, turns.native_turn_id, turns.interrupt_requested_at
                 FROM hub_session_turns AS turns
                 JOIN runs
                   ON runs.hub_turn_id = turns.id
                  AND runs.hub_session_id = turns.session_id
                 AND runs.status IN ('running', 'waiting_tool')
                 WHERE turns.id = $1 AND turns.session_id = $2
                 ORDER BY runs.created_at DESC, runs.id DESC
                 LIMIT 1",
                )
                .bind(active_turn_id)
                .bind(request.session_id)
                .fetch_optional(&mut **tx)
                .await?
                .ok_or(ApiError::conflict(
                    "active Session Turn has no matching active native Turn",
                ))?;
                match active {
                    (_, _, Some(_)) => {}
                    (run_id, Some(native_turn_id), None) => {
                        selected_run_id = Some(run_id);
                        selected_turn_id = Some(active_turn_id);
                        expected_native_turn_id = Some(native_turn_id);
                        delivery_mode = "steer".into();
                    }
                    (_, None, None) => {
                        return Err(ApiError::conflict(
                            "active Session Turn has no matching active native Turn",
                        ));
                    }
                }
            }
            if selected_run_id.is_none() {
                ensure_agent_has_configured_model_tx(tx, request.agent_id).await?;
                if let Some((run_id, turn_id)) = sqlx::query_as(
                    "SELECT runs.id, runs.hub_turn_id
                     FROM runs
                     JOIN hub_session_turns AS turns
                       ON turns.id = runs.hub_turn_id
                      AND turns.session_id = runs.hub_session_id
                     WHERE runs.hub_session_id = $1
                       AND runs.status = 'pending' AND turns.status = 'pending'
                     ORDER BY runs.created_at, runs.id
                     LIMIT 1",
                )
                .bind(request.session_id)
                .fetch_optional(&mut **tx)
                .await?
                {
                    selected_run_id = Some(run_id);
                    selected_turn_id = Some(turn_id);
                } else {
                    let turn_id = Uuid::new_v4();
                    sqlx::query(
                        "INSERT INTO hub_session_turns
                     (id, session_id, status, configuration_fingerprint,
                      ownership_generation)
                 VALUES ($1, $2, 'pending', $3, $4)",
                    )
                    .bind(turn_id)
                    .bind(request.session_id)
                    .bind(session.get::<Option<String>, _>("configuration_fingerprint"))
                    .bind(session.get::<i64, _>("ownership_generation"))
                    .execute(&mut **tx)
                    .await?;
                    selected_turn_id = Some(turn_id);
                    selected_run_id = Some(
                        insert_session_run_tx(tx, &request, turn_id, request.parent_run_id).await?,
                    );
                }
            }
        }
    }

    let message_row = sqlx::query(
        "INSERT INTO hub_session_messages
             (id, session_id, role, message_kind, content, payload,
              delivery_mode, delivery_state, client_message_key,
              expected_native_turn_id, turn_id, run_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
         RETURNING id, session_id, sequence, role, message_kind, content, payload,
                   delivery_mode, delivery_state, client_message_key,
                   expected_native_turn_id, turn_id, run_id, accepted_at",
    )
    .bind(Uuid::new_v4())
    .bind(request.session_id)
    .bind(&request.role)
    .bind(&request.message_kind)
    .bind(&content)
    .bind(&request.payload)
    .bind(&delivery_mode)
    .bind(&delivery_state)
    .bind(client_message_key.as_deref())
    .bind(expected_native_turn_id.as_deref())
    .bind(selected_turn_id)
    .bind(selected_run_id)
    .fetch_one(&mut **tx)
    .await?;
    let mut message = hub_message_from_row(message_row);
    if !attachment_ids.is_empty() {
        let updated = sqlx::query(
            "UPDATE hub_session_attachments
             SET message_id = $1, run_id = $2
             WHERE id = ANY($3) AND session_id = $4 AND message_id IS NULL",
        )
        .bind(message.id)
        .bind(selected_run_id)
        .bind(&attachment_ids)
        .bind(request.session_id)
        .execute(&mut **tx)
        .await?;
        if updated.rows_affected() != attachment_ids.len() as u64 {
            return Err(ApiError::conflict(
                "one or more attachments became unavailable while sending the message",
            ));
        }
    }
    message.attachments = load_attachments_for_session_messages(&mut **tx, &[message.id])
        .await?
        .remove(&message.id)
        .unwrap_or_default();

    if let Some(run_id) = selected_run_id {
        sqlx::query(
            "UPDATE runs
             SET hub_message_id = COALESCE(hub_message_id, $1), updated_at = now()
             WHERE id = $2",
        )
        .bind(message.id)
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO run_events
                 (event_id, run_id, event_type, role, content, payload, hub_message_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(Uuid::new_v4())
        .bind(run_id)
        .bind(if request.message_kind == "tool_result" {
            "tool_result"
        } else {
            "message"
        })
        .bind(&request.role)
        .bind(&content)
        .bind(json!({ "source": request.source, "message": request.payload }))
        .bind(message.id)
        .execute(&mut **tx)
        .await?;
    }

    let refresh_session_activity = request.role == "user" && request.message_kind == "message";
    sqlx::query(
        "UPDATE hub_sessions
         SET history_checkpoint = $1,
             updated_at = CASE WHEN $2 THEN now() ELSE updated_at END
         WHERE id = $3",
    )
    .bind(message.sequence)
    .bind(refresh_session_activity)
    .bind(request.session_id)
    .execute(&mut **tx)
    .await?;

    let run = match selected_run_id {
        Some(run_id) => Some(load_run_public_tx(tx, run_id).await?),
        None => None,
    };
    Ok(SessionMessageAcceptanceDto { message, run })
}

pub(crate) async fn request_run_interrupt_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    expected_session_id: Uuid,
) -> Result<RunDto, ApiError> {
    let run_session_id: Uuid = sqlx::query_scalar(
        "SELECT hub_session_id
         FROM runs
         WHERE id = $1 AND hub_session_id = $2",
    )
    .bind(run_id)
    .bind(expected_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("run not found"))?;

    let session_active_turn_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT active_turn_id
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(run_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;

    let run = sqlx::query(
        "SELECT status, hub_turn_id
         FROM runs
         WHERE id = $1 AND hub_session_id = $2
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(run_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("run not found"))?;
    let run_status: String = run.get("status");
    let turn_id: Uuid = run.get("hub_turn_id");
    if !matches!(run_status.as_str(), "running" | "waiting_tool") {
        // Idempotent stop: a Run that already reached a terminal state has
        // nothing to interrupt. The Run finished between the UI showing the
        // stop button and the request landing, so return it instead of a 409
        // that would leave the client's UI stuck on a stale active Run.
        if matches!(
            run_status.as_str(),
            "completed" | "failed" | "interrupted" | "cancelled"
        ) {
            return load_run_public_tx(tx, run_id).await;
        }
        return Err(ApiError::conflict("run has no active Turn to stop"));
    }
    if session_active_turn_id != Some(turn_id) {
        return Err(ApiError::conflict("run is not the active Session Turn"));
    }

    let turn = sqlx::query(
        "SELECT native_turn_id, status
         FROM hub_session_turns
         WHERE id = $1 AND session_id = $2
         FOR UPDATE",
    )
    .bind(turn_id)
    .bind(run_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::conflict("active Session Turn is missing"))?;
    let native_turn_id: Option<String> = turn.get("native_turn_id");
    let turn_status: String = turn.get("status");
    let has_native_turn = native_turn_id
        .as_deref()
        .is_some_and(|native_turn_id| !native_turn_id.trim().is_empty());
    if turn_status != "starting"
        && !(matches!(turn_status.as_str(), "running" | "in_progress") && has_native_turn)
    {
        return Err(ApiError::conflict("run has no active native Turn to stop"));
    }

    sqlx::query(
        "UPDATE hub_session_turns
         SET interrupt_requested_at = COALESCE(interrupt_requested_at, now()),
             updated_at = now()
         WHERE id = $1 AND session_id = $2",
    )
    .bind(turn_id)
    .bind(run_session_id)
    .execute(&mut **tx)
    .await?;
    load_run_public_tx(tx, run_id).await
}

pub(crate) async fn move_queued_steers_to_next_turn_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    terminal_run_id: Uuid,
    terminal_turn_id: Uuid,
    ownership_generation: i64,
) -> Result<(), ApiError> {
    let queued = sqlx::query(
        "SELECT id, content
         FROM hub_session_messages
         WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
           AND delivery_mode = 'steer' AND delivery_state = 'queued'
         ORDER BY sequence
         FOR UPDATE",
    )
    .bind(session_id)
    .bind(terminal_turn_id)
    .bind(terminal_run_id)
    .fetch_all(&mut **tx)
    .await?;
    let Some(first) = queued.first() else {
        return Ok(());
    };
    let message_ids = queued
        .iter()
        .map(|message| message.get::<Uuid, _>("id"))
        .collect::<Vec<_>>();
    let first_message_id: Uuid = first.get("id");
    let first_content = first
        .get::<Option<String>, _>("content")
        .unwrap_or_default();

    let pending: Option<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT runs.id, runs.hub_turn_id
         FROM runs
         JOIN hub_session_turns AS turns
           ON turns.id = runs.hub_turn_id
          AND turns.session_id = runs.hub_session_id
         WHERE runs.hub_session_id = $1
           AND runs.status = 'pending' AND turns.status = 'pending'
         ORDER BY runs.created_at, runs.id
         LIMIT 1
         FOR UPDATE OF runs, turns",
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?;
    let (next_run_id, next_turn_id) = if let Some(pending) = pending {
        pending
    } else {
        let next_turn_id = Uuid::new_v4();
        let next_run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, configuration_fingerprint,
                  ownership_generation)
             SELECT $1, id, 'pending', configuration_fingerprint, $2
             FROM hub_sessions WHERE id = $3",
        )
        .bind(next_turn_id)
        .bind(ownership_generation)
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
        let inserted = sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  model_subject_type, model_subject_user_id,
                  model_source_integration_app_id,
                  automation_id, integration_session_id, parent_run_id,
                  widget_session_id, external_user_context,
                  client_instance_id, client_tool_snapshot,
                  hub_session_id, hub_message_id, hub_turn_id,
                  session_ownership_generation)
             SELECT $1, agent_id, owner_id, 'pending', $2, source,
                    model_subject_type, model_subject_user_id,
                    model_source_integration_app_id,
                    automation_id, integration_session_id, id,
                    widget_session_id, external_user_context,
                    client_instance_id, client_tool_snapshot,
                    hub_session_id, $3, $4, $5
             FROM runs WHERE id = $6 AND hub_session_id = $7",
        )
        .bind(next_run_id)
        .bind(first_content)
        .bind(first_message_id)
        .bind(next_turn_id)
        .bind(ownership_generation)
        .bind(terminal_run_id)
        .bind(session_id)
        .execute(&mut **tx)
        .await?;
        if inserted.rows_affected() != 1 {
            return Err(ApiError::conflict(
                "terminal Run disappeared before queued Steering Messages were moved",
            ));
        }
        (next_run_id, next_turn_id)
    };

    sqlx::query(
        "UPDATE runs AS next
         SET widget_session_id = COALESCE(next.widget_session_id, previous.widget_session_id),
             integration_session_id = COALESCE(
                 next.integration_session_id,
                 previous.integration_session_id
             ),
             external_user_context = COALESCE(
                 next.external_user_context,
                 previous.external_user_context
             ),
             client_tool_snapshot = CASE
                 WHEN next.client_instance_id IS NULL
                 THEN previous.client_tool_snapshot
                 ELSE next.client_tool_snapshot
             END,
             client_instance_id = COALESCE(
                 next.client_instance_id,
                 previous.client_instance_id
             ),
             updated_at = now()
         FROM runs AS previous
         WHERE next.id = $1 AND previous.id = $2
           AND next.hub_session_id = $3 AND previous.hub_session_id = $3",
    )
    .bind(next_run_id)
    .bind(terminal_run_id)
    .bind(session_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "UPDATE hub_session_messages
         SET delivery_mode = 'next_turn', delivery_state = 'queued',
             expected_native_turn_id = NULL, turn_id = $1, run_id = $2
         WHERE id = ANY($3) AND session_id = $4",
    )
    .bind(next_turn_id)
    .bind(next_run_id)
    .bind(&message_ids)
    .bind(session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE run_events SET run_id = $1
         WHERE hub_message_id = ANY($2) AND run_id = $3",
    )
    .bind(next_run_id)
    .bind(&message_ids)
    .bind(terminal_run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE integration_messages SET run_id = $1
         WHERE hub_message_id = ANY($2) AND run_id = $3",
    )
    .bind(next_run_id)
    .bind(&message_ids)
    .bind(terminal_run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE integration_attachments SET run_id = $1
         WHERE hub_message_id = ANY($2) AND run_id = $3",
    )
    .bind(next_run_id)
    .bind(&message_ids)
    .bind(terminal_run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE runs SET hub_message_id = NULL, updated_at = now()
         WHERE id = $1 AND hub_message_id = ANY($2)",
    )
    .bind(terminal_run_id)
    .bind(&message_ids)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE runs
         SET hub_message_id = COALESCE(hub_message_id, $1), updated_at = now()
         WHERE id = $2",
    )
    .bind(first_message_id)
    .bind(next_run_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn insert_session_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    request: &AcceptSessionMessage,
    turn_id: Uuid,
    parent_run_id: Option<Uuid>,
) -> Result<Uuid, ApiError> {
    let run_id = Uuid::new_v4();
    let external_user_context = request
        .external_user_context
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .map_err(|_| ApiError::internal("external user context could not be encoded"))?;
    let inserted = sqlx::query(
        "INSERT INTO runs
             (id, agent_id, owner_id, status, initial_message, source,
              model_subject_type, model_subject_user_id,
              model_source_integration_app_id, automation_id,
              integration_session_id, parent_run_id,
              hub_session_id, hub_turn_id, session_ownership_generation,
              external_user_context)
         SELECT $1, sessions.agent_id, sessions.owner_id, 'pending', $2, $3,
                $4, $5, $6, $7, $8, $9, sessions.id, $10,
                sessions.ownership_generation, $11
         FROM hub_sessions AS sessions
         WHERE sessions.id = $12
           AND sessions.owner_id = $13
           AND sessions.agent_id = $14",
    )
    .bind(run_id)
    .bind(request.content.trim())
    .bind(&request.source)
    .bind(&request.model_subject_type)
    .bind(request.model_subject_user_id)
    .bind(request.model_source_integration_app_id)
    .bind(request.automation_id)
    .bind(request.integration_session_id)
    .bind(parent_run_id)
    .bind(turn_id)
    .bind(external_user_context)
    .bind(request.session_id)
    .bind(request.owner_id)
    .bind(request.agent_id)
    .execute(&mut **tx)
    .await?;
    if inserted.rows_affected() != 1 {
        return Err(ApiError::not_found("session not found"));
    }
    Ok(run_id)
}

pub(crate) async fn load_hub_message_by_client_key_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    client_message_key: &str,
) -> Result<Option<HubSessionMessageDto>, ApiError> {
    let row = sqlx::query(
        "SELECT id, session_id, sequence, role, message_kind, content, payload,
                delivery_mode, delivery_state, client_message_key,
                expected_native_turn_id, turn_id, run_id, accepted_at
         FROM hub_session_messages
         WHERE session_id = $1 AND client_message_key = $2",
    )
    .bind(session_id)
    .bind(client_message_key)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut message = hub_message_from_row(row);
    message.attachments = load_attachments_for_session_messages(&mut **tx, &[message.id])
        .await?
        .remove(&message.id)
        .unwrap_or_default();
    Ok(Some(message))
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)] // Keep every optional run association explicit at call sites.
pub(crate) async fn create_run_for_agent(
    pool: &PgPool,
    agent_id: Uuid,
    owner_id: Uuid,
    message: String,
    source: &str,
    automation_id: Option<Uuid>,
    integration_session_id: Option<Uuid>,
    parent_run_id: Option<Uuid>,
) -> Result<RunDto, ApiError> {
    let mut tx = pool.begin().await?;
    let run = insert_run_for_agent_tx(
        &mut tx,
        agent_id,
        owner_id,
        message,
        source,
        automation_id,
        integration_session_id,
        parent_run_id,
    )
    .await?;
    tx.commit().await?;
    Ok(run)
}

#[allow(clippy::too_many_arguments)] // Keep every optional run association explicit at call sites.
pub(crate) async fn insert_run_for_agent_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    owner_id: Uuid,
    message: String,
    source: &str,
    automation_id: Option<Uuid>,
    integration_session_id: Option<Uuid>,
    parent_run_id: Option<Uuid>,
) -> Result<RunDto, ApiError> {
    if message.trim().is_empty() {
        return Err(ApiError::bad_request("message is required"));
    }
    ensure_agent_can_start_run_tx(tx, agent_id, owner_id).await?;
    if integration_session_id.is_some() {
        return Err(ApiError::internal(
            "integration runs require an external Hub Session",
        ));
    }
    let session_id = match parent_run_id {
        Some(parent_run_id) => sqlx::query_scalar(
            "SELECT hub_session_id FROM runs
             WHERE id = $1 AND agent_id = $2 AND owner_id = $3
               AND status IN ('completed', 'waiting_tool')",
        )
        .bind(parent_run_id)
        .bind(agent_id)
        .bind(owner_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::bad_request("resume parent run is not available"))?,
        None => insert_hub_native_session_tx(tx, owner_id, agent_id).await?,
    };
    let accepted = accept_session_message_tx(
        tx,
        AcceptSessionMessage {
            session_id,
            agent_id,
            owner_id,
            content: message,
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: None,
            source: source.into(),
            automation_id,
            integration_session_id: None,
            parent_run_id,
            continuation_turn_id: None,
            model_subject_type: "user".into(),
            model_subject_user_id: Some(owner_id),
            model_source_integration_app_id: None,
            external_user_context: None,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    accepted
        .run
        .ok_or(ApiError::internal("message did not schedule a run"))
}

pub(crate) async fn ensure_agent_can_start_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    caller_id: Uuid,
) -> Result<(), ApiError> {
    let exists: Option<(Uuid,)> = sqlx::query_as(
        "SELECT id
         FROM agents
         WHERE id = $1 AND deleted_at IS NULL
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         FOR UPDATE",
    )
    .bind(agent_id)
    .bind(caller_id)
    .fetch_optional(&mut **tx)
    .await?;
    exists.ok_or(ApiError::not_found("agent not found"))?;
    ensure_agent_has_configured_model_tx(tx, agent_id).await
}

pub(crate) async fn ensure_agent_has_configured_model_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
) -> Result<(), ApiError> {
    let configured: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM agents AS agent
             JOIN model_connections AS model
               ON model.id = agent.model_connection_id
             WHERE agent.id = $1 AND agent.deleted_at IS NULL
               AND model.enabled = true AND model.deleted_at IS NULL
               AND agent.model_id = ANY(model.allowed_model_ids)
               AND (agent.model_settings->'request_settings')->>'protocol' = model.api_type
               AND (model.scope = 'global' OR model.owner_id = agent.owner_id)
         )",
    )
    .bind(agent_id)
    .fetch_one(&mut **tx)
    .await?;
    if !configured {
        return Err(ApiError::conflict("Agent has no configured model"));
    }
    Ok(())
}

pub(crate) async fn insert_run_event_for_active_runtime(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    runtime_id: Uuid,
    ownership_generation: i64,
    request: AppendRunEventRequest,
) -> Result<RunEventDto, ApiError> {
    let AppendRunEventRequest {
        event_id,
        event_type,
        role,
        content,
        payload,
        waiting_tool,
    } = request;
    if event_type == "tool_request" {
        return Err(ApiError::bad_request(
            "tool requests must use atomic batch finalize",
        ));
    }
    if waiting_tool.is_some() {
        return Err(ApiError::bad_request(
            "waiting tool state is only valid for atomic batch finalize",
        ));
    }
    let session_id =
        lock_owned_session_for_run_tx(tx, run_id, runtime_id, ownership_generation).await?;
    let owned = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT runs.id, runs.hub_turn_id
         FROM runs
         WHERE runs.id = $1 AND runs.runtime_id = $2
           AND runs.session_ownership_generation = $3
           AND runs.status = 'running'
           AND runs.hub_session_id = $4
         FOR UPDATE OF runs",
    )
    .bind(run_id)
    .bind(runtime_id)
    .bind(ownership_generation)
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?;
    let (_, hub_turn_id) = owned.ok_or(ApiError::forbidden(
        "runtime does not own the Session generation for this active run",
    ))?;
    if event_type == "turn_started" {
        let native_session_id = payload
            .get("native_session_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 512)
            .ok_or(ApiError::bad_request("valid native Session id is required"))?;
        let native_turn_id = payload
            .get("native_turn_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 512)
            .ok_or(ApiError::bad_request("valid native Turn id is required"))?;
        if role.is_some() || content.is_some() {
            return Err(ApiError::bad_request(
                "turn_started event cannot contain message content",
            ));
        }
        let turn = sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = $1, status = 'running',
                 started_at = COALESCE(started_at, now()), updated_at = now()
             WHERE id = $2 AND session_id = $3 AND ownership_generation = $4
               AND status IN ('pending', 'starting', 'running')
               AND (native_turn_id IS NULL OR native_turn_id = $1)",
        )
        .bind(native_turn_id)
        .bind(hub_turn_id)
        .bind(session_id)
        .bind(ownership_generation)
        .execute(&mut **tx)
        .await?;
        if turn.rows_affected() != 1 {
            return Err(ApiError::conflict(
                "native Turn binding does not match the owned Hub Turn",
            ));
        }
        let session = sqlx::query(
            "UPDATE hub_sessions
             SET native_session_id = $1, active_turn_id = $2,
                 lifecycle_status = 'online',
                 recovery_source = NULL
             WHERE id = $3 AND runtime_owner_id = $4 AND ownership_generation = $5
               AND (native_session_id IS NULL OR native_session_id = $1)
               AND (active_turn_id IS NULL OR active_turn_id = $2)",
        )
        .bind(native_session_id)
        .bind(hub_turn_id)
        .bind(session_id)
        .bind(runtime_id)
        .bind(ownership_generation)
        .execute(&mut **tx)
        .await?;
        if session.rows_affected() != 1 {
            return Err(ApiError::conflict(
                "native Session or active Turn binding changed before acknowledgement",
            ));
        }
        sqlx::query(
            "UPDATE hub_session_messages
             SET delivery_state = 'delivered'
             WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
               AND delivery_state = 'delivering'
               AND delivery_mode = 'next_turn'",
        )
        .bind(session_id)
        .bind(hub_turn_id)
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "UPDATE hub_session_messages
             SET delivery_mode = 'steer', expected_native_turn_id = $4
             WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
               AND delivery_state = 'queued' AND delivery_mode = 'next_turn'",
        )
        .bind(session_id)
        .bind(hub_turn_id)
        .bind(run_id)
        .bind(native_turn_id)
        .execute(&mut **tx)
        .await?;
    }
    let refresh_session_activity = event_type == "message"
        && role.as_deref() == Some("assistant")
        && content
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
    let event =
        insert_run_event_with_id_tx(tx, event_id, run_id, event_type, role, content, payload)
            .await?;
    if refresh_session_activity {
        sqlx::query("UPDATE hub_sessions SET updated_at = now() WHERE id = $1")
            .bind(session_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(event)
}

pub(crate) fn sanitize_run_event_text(text: &mut String) {
    if text.contains('\0') {
        *text = text.replace('\0', "\u{FFFD}");
    }
}

pub(crate) fn sanitize_run_event_payload(value: Value) -> Value {
    match value {
        Value::String(mut text) => {
            sanitize_run_event_text(&mut text);
            Value::String(text)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(sanitize_run_event_payload).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(mut key, value)| {
                    sanitize_run_event_text(&mut key);
                    (key, sanitize_run_event_payload(value))
                })
                .collect(),
        ),
        other => other,
    }
}

pub(crate) async fn insert_run_event_with_id_tx(
    tx: &mut Transaction<'_, Postgres>,
    event_id: Uuid,
    run_id: Uuid,
    mut event_type: String,
    mut role: Option<String>,
    mut content: Option<String>,
    payload: Value,
) -> Result<RunEventDto, ApiError> {
    sanitize_run_event_text(&mut event_type);
    if let Some(text) = role.as_mut() {
        sanitize_run_event_text(text);
    }
    if let Some(text) = content.as_mut() {
        sanitize_run_event_text(text);
    }
    let payload = sanitize_run_event_payload(payload);
    let row = sqlx::query(
        "INSERT INTO run_events (event_id, run_id, event_type, role, content, payload)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (event_id) DO NOTHING
         RETURNING seq, event_id, run_id, event_type, role, content, payload, created_at",
    )
    .bind(event_id)
    .bind(run_id)
    .bind(event_type)
    .bind(role)
    .bind(content)
    .bind(payload)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = row {
        return Ok(event_from_row(row));
    }
    let existing = sqlx::query(
        "SELECT seq, event_id, run_id, event_type, role, content, payload, created_at
         FROM run_events WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(event_from_row(existing))
}

pub(crate) async fn lock_owned_session_for_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    runtime_id: Uuid,
    ownership_generation: i64,
) -> Result<Uuid, ApiError> {
    let session_id: Uuid = sqlx::query_scalar("SELECT hub_session_id FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::forbidden("run does not belong to a Session"))?;
    let owned: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM hub_sessions
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3
         FOR UPDATE",
    )
    .bind(session_id)
    .bind(runtime_id)
    .bind(ownership_generation)
    .fetch_optional(&mut **tx)
    .await?;
    owned.ok_or(ApiError::forbidden(
        "runtime does not own this Session generation",
    ))
}

pub(crate) async fn insert_run_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    mut event_type: String,
    mut role: Option<String>,
    mut content: Option<String>,
    payload: Value,
) -> Result<RunEventDto, ApiError> {
    sanitize_run_event_text(&mut event_type);
    if let Some(text) = role.as_mut() {
        sanitize_run_event_text(text);
    }
    if let Some(text) = content.as_mut() {
        sanitize_run_event_text(text);
    }
    let payload = sanitize_run_event_payload(payload);
    let row = sqlx::query(
        "INSERT INTO run_events (event_id, run_id, event_type, role, content, payload)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING seq, event_id, run_id, event_type, role, content, payload, created_at",
    )
    .bind(Uuid::new_v4())
    .bind(run_id)
    .bind(event_type)
    .bind(role)
    .bind(content)
    .bind(payload)
    .fetch_one(&mut **tx)
    .await?;
    Ok(event_from_row(row))
}

pub(crate) async fn load_events_after(
    pool: &PgPool,
    run_id: Uuid,
    after: i64,
) -> Result<Vec<RunEventDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT seq, event_id, run_id, event_type, role, content, payload, created_at
         FROM run_events WHERE run_id = $1 AND seq > $2 ORDER BY seq ASC",
    )
    .bind(run_id)
    .bind(after)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(event_from_row).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use crate::{build_router, openapi_document};
    use axum::http::Method;
    use axum::response::IntoResponse;
    use tower::ServiceExt;

    /// Shared helper living in main.rs's `mod tests`; mirrored here so session tests can use it.
    /// The main agent plans to consolidate it into `api/support/test_util.rs`.
    fn runtime_write_generation<T>(
        ownership_generation: i64,
        payload: T,
    ) -> Json<RuntimeSessionWriteRequest<T>> {
        Json(RuntimeSessionWriteRequest {
            ownership_generation,
            payload,
        })
    }

    #[test]
    fn session_message_pagination_validates_bounds_and_is_documented() {
        assert_eq!(
            SessionMessageListQuery {
                before_sequence: Some(42),
                limit: Some(51),
            }
            .validated()
            .unwrap(),
            (Some(42), Some(51))
        );
        for query in [
            SessionMessageListQuery {
                before_sequence: Some(0),
                limit: None,
            },
            SessionMessageListQuery {
                before_sequence: None,
                limit: Some(101),
            },
        ] {
            assert_eq!(
                query.validated().unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
        }

        let document = openapi_document();
        for path in [
            "/api/sessions/{session_id}/messages",
            "/api/widget/sessions/{session_id}/messages",
        ] {
            let parameters = document["paths"][path]["get"]["parameters"]
                .as_array()
                .unwrap();
            assert!(parameters
                .iter()
                .any(|parameter| parameter["name"] == "before_sequence"));
            assert!(parameters
                .iter()
                .any(|parameter| parameter["name"] == "limit"));
            assert!(document["paths"][path]["get"]["responses"]
                .get("400")
                .is_some());
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_model_schema_enforces_origin_immutability_and_bundle_shape(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("session-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let other = create_hub_user(
            &pool,
            Some("other-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        let other_agent_id = Uuid::new_v4();
        for (id, name) in [(agent_id, "Session Agent"), (other_agent_id, "Other Agent")] {
            sqlx::query(
                "INSERT INTO agents (id, owner_id, name, instructions, visibility)
                 VALUES ($1, $2, $3, 'test', 'private')",
            )
            .bind(id)
            .bind(owner.id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        }
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let identity_id = Uuid::new_v4();
        sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, 'teams', 'Teams')")
            .bind(platform_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email)
             VALUES ($1, $2, 'default', 'Default', true, true)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO external_identities
                 (id, platform_id, external_user_id, user_id, authentication_channel_id)
             VALUES ($1, $2, 'external-1', $3, $4)",
        )
        .bind(identity_id)
        .bind(platform_id)
        .bind(owner.id)
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();

        let native_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
        )
        .bind(native_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("UPDATE hub_sessions SET native_session_id = 'thread-native' WHERE id = $1")
            .bind(native_session_id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'thread-other' WHERE id = $1"
        )
        .bind(native_session_id)
        .execute(&pool)
        .await
        .is_err());
        assert!(
            sqlx::query("UPDATE hub_sessions SET native_session_id = NULL WHERE id = $1")
                .bind(native_session_id)
                .execute(&pool)
                .await
                .is_err()
        );

        sqlx::query("UPDATE hub_sessions SET history_checkpoint = 2 WHERE id = $1")
            .bind(native_session_id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            sqlx::query("UPDATE hub_sessions SET history_checkpoint = 1 WHERE id = $1")
                .bind(native_session_id)
                .execute(&pool)
                .await
                .is_err()
        );
        let external_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, origin_platform_id,
                  origin_tenant_id, origin_external_identity_id, lifecycle_status)
             VALUES ($1, $2, $3, 'external', $4, 'tenant-1', $5, 'offline')",
        )
        .bind(external_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .bind(platform_id)
        .bind(identity_id)
        .execute(&pool)
        .await
        .unwrap();

        let partial_origin = sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, origin_platform_id, lifecycle_status)
             VALUES ($1, $2, $3, 'external', $4, 'offline')",
        )
        .bind(Uuid::new_v4())
        .bind(owner.id)
        .bind(agent_id)
        .bind(platform_id)
        .execute(&pool)
        .await;
        assert!(partial_origin.is_err());

        let mismatched_owner = sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, origin_platform_id,
                  origin_tenant_id, origin_external_identity_id, lifecycle_status)
             VALUES ($1, $2, $3, 'external', $4, 'tenant-1', $5, 'offline')",
        )
        .bind(Uuid::new_v4())
        .bind(other.id)
        .bind(agent_id)
        .bind(platform_id)
        .bind(identity_id)
        .execute(&pool)
        .await;
        assert!(mismatched_owner.is_err());

        for mutation in [
            format!("owner_id = '{}'", other.id),
            format!("agent_id = '{other_agent_id}'"),
            "origin_tenant_id = 'tenant-2'".into(),
        ] {
            let result = sqlx::query(&format!("UPDATE hub_sessions SET {mutation} WHERE id = $1"))
                .bind(external_session_id)
                .execute(&pool)
                .await;
            assert!(result.is_err(), "immutable mutation unexpectedly succeeded");
        }

        let partial_bundle =
            sqlx::query("UPDATE hub_sessions SET current_bundle_generation = 1 WHERE id = $1")
                .bind(native_session_id)
                .execute(&pool)
                .await;
        assert!(partial_bundle.is_err());
        sqlx::query(
            "UPDATE hub_sessions
             SET current_bundle_generation = 1,
                 current_bundle_kind = 'checkpoint',
                 current_bundle_object_key = 'sessions/native/bundle-1.tar.zst',
                 current_bundle_checksum_sha256 = 'abc123',
                 current_bundle_size_bytes = 4096,
                 current_bundle_history_checkpoint = 0,
                 current_bundle_ownership_generation = 0,
                 current_bundle_producing_engine_version = '0.42.0',
                 current_bundle_created_at = now()
             WHERE id = $1",
        )
        .bind(native_session_id)
        .execute(&pool)
        .await
        .unwrap();

        let runtime_one = Uuid::new_v4();
        let runtime_two = Uuid::new_v4();
        for (id, hostname) in [(runtime_one, "runtime-one"), (runtime_two, "runtime-two")] {
            sqlx::query(
                "INSERT INTO runtimes
                     (id, token_hash, hostname, engine_version, sandbox_mode, status)
                 VALUES ($1, $2, $3, '0.42.0', 'workspace-write', 'online')",
            )
            .bind(id)
            .bind(format!("token-{id}"))
            .bind(hostname)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1
             WHERE id = $2",
        )
        .bind(runtime_one)
        .bind(native_session_id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            sqlx::query("UPDATE hub_sessions SET ownership_generation = 0 WHERE id = $1",)
                .bind(native_session_id)
                .execute(&pool)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("UPDATE hub_sessions SET runtime_owner_id = $1 WHERE id = $2",)
                .bind(runtime_two)
                .bind(native_session_id)
                .execute(&pool)
                .await
                .is_err()
        );

        let active_pointer_session_id = Uuid::new_v4();
        let active_pointer_turn_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'online')",
        )
        .bind(active_pointer_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, configuration_fingerprint,
                  ownership_generation)
             VALUES ($1, $2, 'in_progress', 'sha256:pointer', 0)",
        )
        .bind(active_pointer_turn_id)
        .bind(active_pointer_session_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
            .bind(active_pointer_turn_id)
            .bind(active_pointer_session_id)
            .execute(&pool)
            .await
            .unwrap();

        let active_turn_constraint: (bool, bool) = sqlx::query_as(
            "SELECT condeferrable, condeferred
             FROM pg_constraint
             WHERE conname = 'hub_sessions_active_turn_session_fk'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(active_turn_constraint, (true, true));
        assert!(sqlx::query("DELETE FROM hub_session_turns WHERE id = $1")
            .bind(active_pointer_turn_id)
            .execute(&pool)
            .await
            .is_err());
        sqlx::query("DELETE FROM hub_sessions WHERE id = $1")
            .bind(active_pointer_session_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_model_messages_are_strictly_ordered_and_session_linked(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("message-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Message Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let session_one = Uuid::new_v4();
        let session_two = Uuid::new_v4();
        for session_id in [session_one, session_two] {
            sqlx::query(
                "INSERT INTO hub_sessions
                     (id, owner_id, agent_id, origin_kind, lifecycle_status)
                 VALUES ($1, $2, $3, 'hub_native', 'online')",
            )
            .bind(session_id)
            .bind(owner.id)
            .bind(agent_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let turn_one = Uuid::new_v4();
        let turn_two = Uuid::new_v4();
        for (turn_id, session_id) in [(turn_one, session_one), (turn_two, session_two)] {
            sqlx::query(
                "INSERT INTO hub_session_turns
                     (id, session_id, status, configuration_fingerprint,
                      ownership_generation)
                 VALUES ($1, $2, 'in_progress', 'sha256:config', 1)",
            )
            .bind(turn_id)
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
            .bind(turn_one)
            .bind(session_one)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
                .bind(turn_one)
                .bind(session_two)
                .execute(&pool)
                .await
                .is_err()
        );

        let run_one = Uuid::new_v4();
        let run_two = Uuid::new_v4();
        for (run_id, session_id, turn_id) in [
            (run_one, session_one, turn_one),
            (run_two, session_two, turn_two),
        ] {
            sqlx::query(
                "INSERT INTO runs
                     (id, agent_id, owner_id, status, initial_message, source,
                      hub_session_id, hub_turn_id, session_ownership_generation)
                 VALUES ($1, $2, $3, 'running', 'hello', 'console', $4, $5, $6)",
            )
            .bind(run_id)
            .bind(agent_id)
            .bind(owner.id)
            .bind(session_id)
            .bind(turn_id)
            .bind(1_i64)
            .execute(&pool)
            .await
            .unwrap();
        }

        let message_one = Uuid::new_v4();
        let sequence_one: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'first', 'next_turn',
                     'delivered', $3, $4)
             RETURNING sequence",
        )
        .bind(message_one)
        .bind(session_one)
        .bind(turn_one)
        .bind(run_one)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(sequence_one, 1);

        let skipped_sequence = sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, sequence, role, message_kind, content,
                  delivery_mode, delivery_state)
             VALUES ($1, $2, 3, 'user', 'message', 'skip', 'next_turn', 'queued')",
        )
        .bind(Uuid::new_v4())
        .bind(session_one)
        .execute(&pool)
        .await;
        assert!(skipped_sequence.is_err());

        let message_two = Uuid::new_v4();
        let sequence_two: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content,
                  delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'second', 'later_turn', 'deferred')
             RETURNING sequence",
        )
        .bind(message_two)
        .bind(session_one)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(sequence_two, 2);

        let concurrent_insert = |content: &'static str| {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content,
                      delivery_mode, delivery_state)
                 VALUES ($1, $2, 'user', 'message', $3, 'next_turn', 'queued')
                 RETURNING sequence",
            )
            .bind(Uuid::new_v4())
            .bind(session_one)
            .bind(content)
            .fetch_one(&pool)
        };
        let (sequence_three, sequence_four) =
            tokio::join!(concurrent_insert("third"), concurrent_insert("fourth"));
        let mut sequences = vec![sequence_three.unwrap(), sequence_four.unwrap()];
        sequences.sort_unstable();
        assert_eq!(sequences, vec![3, 4]);

        assert!(
            sqlx::query("UPDATE hub_session_messages SET content = 'changed' WHERE id = $1")
                .bind(message_one)
                .execute(&pool)
                .await
                .is_err()
        );
        sqlx::query("UPDATE hub_session_messages SET delivery_state = 'delivered' WHERE id = $1")
            .bind(message_two)
            .execute(&pool)
            .await
            .unwrap();

        assert!(sqlx::query(
            "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content,
                      delivery_mode, delivery_state, run_id)
                 VALUES ($1, $2, 'user', 'message', 'wrong run',
                         'next_turn', 'queued', $3)",
        )
        .bind(Uuid::new_v4())
        .bind(session_one)
        .bind(run_two)
        .execute(&pool)
        .await
        .is_err());
        assert!(sqlx::query(
            "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content,
                      delivery_mode, delivery_state)
                 VALUES ($1, $2, 'user', 'message', 'invalid steer',
                         'steer', 'queued')",
        )
        .bind(Uuid::new_v4())
        .bind(session_one)
        .execute(&pool)
        .await
        .is_err());

        sqlx::query("UPDATE runs SET hub_message_id = $1 WHERE id = $2")
            .bind(message_one)
            .bind(run_one)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            sqlx::query("UPDATE runs SET hub_message_id = $1 WHERE id = $2")
                .bind(message_one)
                .bind(run_two)
                .execute(&pool)
                .await
                .is_err()
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_acceptance_is_ordered_idempotent_and_reuses_one_run(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("routing-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Routing Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        attach_test_model_connection(&pool, agent_id, owner.id, "routing-agent-model").await;
        let session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
        )
        .bind(session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();

        let first = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "first",
            Some("request-1"),
            "next_turn",
        )
        .await
        .unwrap();
        let first_run = first.run.as_ref().unwrap();
        assert_eq!(first.message.sequence, 1);
        assert_eq!(first.message.delivery_mode, "next_turn");
        assert_eq!(first.message.run_id, Some(first_run.id));
        assert_eq!(first.message.turn_id, first_run.hub_turn_id);

        let duplicate = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "ignored duplicate body",
            Some("request-1"),
            "next_turn",
        )
        .await
        .unwrap();
        assert_eq!(duplicate.message.id, first.message.id);
        assert_eq!(duplicate.run.as_ref().unwrap().id, first_run.id);

        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'restoring' WHERE id = $1")
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();
        let (second, third) = tokio::join!(
            accept_test_session_message(
                &pool,
                session_id,
                agent_id,
                owner.id,
                "second",
                None,
                "next_turn",
            ),
            accept_test_session_message(
                &pool,
                session_id,
                agent_id,
                owner.id,
                "third",
                None,
                "next_turn",
            ),
        );
        let second = second.unwrap();
        let third = third.unwrap();
        let mut sequences = vec![second.message.sequence, third.message.sequence];
        sequences.sort_unstable();
        assert_eq!(sequences, vec![2, 3]);
        assert_eq!(second.run.as_ref().unwrap().id, first_run.id);
        assert_eq!(third.run.as_ref().unwrap().id, first_run.id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runs
                 WHERE hub_session_id = $1 AND status IN ('pending', 'running')",
            )
            .bind(session_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );

        let later = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "later",
            None,
            "later_turn",
        )
        .await
        .unwrap();
        assert!(later.run.is_none());
        assert_eq!(later.message.delivery_mode, "later_turn");
        assert_eq!(later.message.delivery_state, "deferred");
        assert!(later.message.run_id.is_none());
        assert!(later.message.turn_id.is_none());

        let turn_id = first_run.hub_turn_id.unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'in_progress', native_turn_id = 'native-turn-1'
             WHERE id = $1",
        )
        .bind(turn_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE runs SET status = 'running' WHERE id = $1")
            .bind(first_run.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', active_turn_id = $1
             WHERE id = $2",
        )
        .bind(turn_id)
        .bind(session_id)
        .execute(&pool)
        .await
        .unwrap();

        let steer = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "steer now",
            None,
            "next_turn",
        )
        .await
        .unwrap();
        assert_eq!(steer.run.as_ref().unwrap().id, first_run.id);
        assert_eq!(steer.message.delivery_mode, "steer");
        assert_eq!(
            steer.message.expected_native_turn_id.as_deref(),
            Some("native-turn-1")
        );

        sqlx::query("UPDATE hub_sessions SET active_turn_id = NULL WHERE id = $1")
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(first_run.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE hub_session_turns SET status = 'completed' WHERE id = $1")
            .bind(turn_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'recovery_failed' WHERE id = $1")
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();
        let rejected = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "must reject",
            None,
            "next_turn",
        )
        .await
        .unwrap_err();
        assert_eq!(rejected.status, StatusCode::CONFLICT);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_waiting_tool_active_turn_accepts_steer_without_new_run(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("waiting-tool-steer@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Waiting Tool Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        attach_test_model_connection(&pool, agent_id, owner.id, "waiting-tool-agent-model").await;
        let session_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'online')",
        )
        .bind(session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, native_turn_id, status, ownership_generation)
             VALUES ($1, $2, 'native-waiting-tool', 'in_progress', 0)",
        )
        .bind(turn_id)
        .bind(session_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  hub_session_id, hub_turn_id, session_ownership_generation)
             VALUES ($1, $2, $3, 'waiting_tool', 'waiting for tool', 'console',
                     $4, $5, 0)",
        )
        .bind(run_id)
        .bind(agent_id)
        .bind(owner.id)
        .bind(session_id)
        .bind(turn_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
            .bind(turn_id)
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();

        let accepted = accept_test_session_message(
            &pool,
            session_id,
            agent_id,
            owner.id,
            "guide the waiting turn",
            Some("waiting-tool-steer-1"),
            "next_turn",
        )
        .await
        .expect("waiting_tool still belongs to the active native Turn");
        assert_eq!(accepted.run.as_ref().unwrap().id, run_id);
        assert_eq!(accepted.message.run_id, Some(run_id));
        assert_eq!(accepted.message.turn_id, Some(turn_id));
        assert_eq!(accepted.message.delivery_mode, "steer");
        assert_eq!(
            accepted.message.expected_native_turn_id.as_deref(),
            Some("native-waiting-tool")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE hub_session_id = $1")
                .bind(session_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_hub_owner_api_reuses_and_isolates_sessions(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("hub-session-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let other = create_hub_user(
            &pool,
            Some("hub-session-other@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let owner_token = "hub-session-owner-token";
        let other_token = "hub-session-other-token";
        for (token, user_id) in [(owner_token, owner.id), (other_token, other.id)] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Hub Session Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        attach_test_model_connection(&pool, agent_id, owner.id, "hub-session-agent-model").await;
        let app = build_router(test_state_with_browser_session_auth(pool.clone()));

        let first_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/agents/{agent_id}/runs"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"message":"first","client_message_key":"one"}"#,
            ))
            .unwrap();
        let first_response = app.clone().oneshot(first_request).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let hub_session_id = first
            .hub_session_id
            .expect("console run must own a Session");

        let second_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/agents/{agent_id}/runs"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"second","hub_session_id":"{hub_session_id}","client_message_key":"two"}}"#
            )))
            .unwrap();
        let second_response = app.clone().oneshot(second_request).await.unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second.id, first.id, "upcoming Turn must reuse one Run");

        let run_list_request = axum::http::Request::builder()
            .uri(format!("/api/agents/{agent_id}/runs"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let run_list_response = app.clone().oneshot(run_list_request).await.unwrap();
        assert_eq!(run_list_response.status(), StatusCode::OK);
        let runs: Vec<RunDto> = serde_json::from_slice(
            &axum::body::to_bytes(run_list_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].hub_session_id, Some(hub_session_id));
        assert_eq!(runs[0].hub_message_id, first.hub_message_id);
        assert_eq!(runs[0].hub_turn_id, first.hub_turn_id);
        assert_eq!(runs[0].session_ownership_generation, Some(0));

        let get_run_request = axum::http::Request::builder()
            .uri(format!("/api/runs/{}", first.id))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let get_run_response = app.clone().oneshot(get_run_request).await.unwrap();
        assert_eq!(get_run_response.status(), StatusCode::OK);
        let fetched_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(get_run_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(fetched_run.hub_session_id, Some(hub_session_id));
        assert_eq!(fetched_run.hub_message_id, first.hub_message_id);
        assert_eq!(fetched_run.hub_turn_id, first.hub_turn_id);
        assert_eq!(fetched_run.session_ownership_generation, Some(0));

        let list_request = axum::http::Request::builder()
            .uri("/api/sessions")
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let listed = app.clone().oneshot(list_request).await.unwrap();
        assert_eq!(listed.status(), StatusCode::OK);
        let listed: Vec<HubSessionDto> = serde_json::from_slice(
            &axum::body::to_bytes(listed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, hub_session_id);

        let messages_request = axum::http::Request::builder()
            .uri(format!("/api/sessions/{hub_session_id}/messages"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let messages = app.clone().oneshot(messages_request).await.unwrap();
        assert_eq!(messages.status(), StatusCode::OK);
        let messages: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(messages.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content.as_deref(), Some("first"));
        assert_eq!(messages[1].content.as_deref(), Some("second"));

        let latest_request = axum::http::Request::builder()
            .uri(format!("/api/sessions/{hub_session_id}/messages?limit=1"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let latest_response = app.clone().oneshot(latest_request).await.unwrap();
        assert_eq!(latest_response.status(), StatusCode::OK);
        let latest: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(latest_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].content.as_deref(), Some("second"));

        let older_request = axum::http::Request::builder()
            .uri(format!(
                "/api/sessions/{hub_session_id}/messages?before_sequence={}&limit=1",
                latest[0].sequence
            ))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let older_response = app.clone().oneshot(older_request).await.unwrap();
        assert_eq!(older_response.status(), StatusCode::OK);
        let older: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(older_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(older.len(), 1);
        assert_eq!(older[0].content.as_deref(), Some("first"));

        let forbidden_request = axum::http::Request::builder()
            .uri(format!("/api/sessions/{hub_session_id}"))
            .header(header::COOKIE, format!("agent_hub_session={other_token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(forbidden_request)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        let other_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
        )
        .bind(other_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
        let cross_parent_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/agents/{agent_id}/runs"))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"cross","hub_session_id":"{other_session_id}","parent_run_id":"{}"}}"#,
                first.id
            )))
            .unwrap();
        assert_eq!(
            app.oneshot(cross_parent_request).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_widget_token_is_bound_to_one_session(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("widget-session-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let owner_token = "widget-session-owner-token";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(owner_token))
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let model_connection_id = Uuid::new_v4();
        let model_id = format!("widget-session-model-{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO model_connections
                 (id, scope, name, base_url, api_type, allowed_model_ids,
                  api_key_ciphertext, api_key_nonce, created_by)
             VALUES ($1, 'global', 'Widget Session Model', 'https://models.example.test',
                     'openai_responses', $2, $3, $4, $5)",
        )
        .bind(model_connection_id)
        .bind(vec![model_id.clone()])
        .bind(vec![1_u8; 17])
        .bind(vec![2_u8; 12])
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility,
                  model_connection_id, model_id)
             VALUES ($1, $2, 'Widget Session Agent', 'test', 'private', $3, $4)",
        )
        .bind(agent_id)
        .bind(owner.id)
        .bind(model_connection_id)
        .bind(&model_id)
        .execute(&pool)
        .await
        .unwrap();
        let app = build_router(test_state_with_browser_session_auth(pool.clone()));

        let issue_token = || {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/embed/sessions")
                .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"agent_id":"{agent_id}"}}"#)))
                .unwrap()
        };
        let first_token_response = app.clone().oneshot(issue_token()).await.unwrap();
        assert_eq!(first_token_response.status(), StatusCode::OK);
        let first_token: CreateEmbedSessionResponse = serde_json::from_slice(
            &axum::body::to_bytes(first_token_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let second_token_response = app.clone().oneshot(issue_token()).await.unwrap();
        assert_eq!(second_token_response.status(), StatusCode::OK);
        let second_token: CreateEmbedSessionResponse = serde_json::from_slice(
            &axum::body::to_bytes(second_token_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let first_session_id: Uuid =
            sqlx::query_scalar("SELECT hub_session_id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&first_token.token))
                .fetch_one(&pool)
                .await
                .unwrap();
        let second_session_id: Uuid =
            sqlx::query_scalar("SELECT hub_session_id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&second_token.token))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_ne!(first_session_id, second_session_id);

        let first_run_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/widget/runs")
            .header("x-agent-hub-embed-token", &first_token.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"first","hub_session_id":"{first_session_id}","client_message_key":"widget-1"}}"#
            )))
            .unwrap();
        let first_run_response = app.clone().oneshot(first_run_request).await.unwrap();
        assert_eq!(first_run_response.status(), StatusCode::OK);
        let first_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(first_run_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first_run.hub_session_id, Some(first_session_id));

        let second_message_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/widget/runs")
            .header("x-agent-hub-embed-token", &first_token.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"message":"second","client_message_key":"widget-2"}"#,
            ))
            .unwrap();
        let second_message_response = app.clone().oneshot(second_message_request).await.unwrap();
        assert_eq!(second_message_response.status(), StatusCode::OK);
        let second_message_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(second_message_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second_message_run.id, first_run.id);

        let cross_session_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/widget/runs")
            .header("x-agent-hub-embed-token", &first_token.token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"cross","hub_session_id":"{second_session_id}"}}"#
            )))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(cross_session_request)
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );

        let foreign_stream_request = axum::http::Request::builder()
            .uri(format!("/api/runs/{}/events/stream", first_run.id))
            .header("x-agent-hub-embed-token", &second_token.token)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(foreign_stream_request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_integration_origin_and_message_idempotency(pool: PgPool) {
        let agent_owner = create_hub_user(
            &pool,
            Some("integration-origin-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let first_agent = Uuid::new_v4();
        let second_agent = Uuid::new_v4();
        for (agent_id, name) in [
            (first_agent, "First Origin Agent"),
            (second_agent, "Second Origin Agent"),
        ] {
            sqlx::query(
                "INSERT INTO agents (id, owner_id, name, instructions, visibility)
                 VALUES ($1, $2, $3, 'test', 'private')",
            )
            .bind(agent_id)
            .bind(agent_owner.id)
            .bind(name)
            .execute(&pool)
            .await
            .unwrap();
        }
        attach_test_model_connection(
            &pool,
            first_agent,
            agent_owner.id,
            "integration-origin-first-model",
        )
        .await;
        attach_test_model_connection(
            &pool,
            second_agent,
            agent_owner.id,
            "integration-origin-second-model",
        )
        .await;
        let first_app = Uuid::new_v4();
        let second_app = Uuid::new_v4();
        let first_platform = Uuid::new_v4();
        let second_platform = Uuid::new_v4();
        let first_channel = Uuid::new_v4();
        let second_channel = Uuid::new_v4();
        let first_token = "aho_integration_origin_first";
        let second_token = "aho_integration_origin_second";
        let same_app_other_origin_token = "aho_integration_origin_same_app_other";
        let same_app_unbound_token = "aho_integration_origin_same_app_unbound";
        for (index, platform_id, channel_id, app_id, agent_id, token) in [
            (
                "first",
                first_platform,
                first_channel,
                first_app,
                first_agent,
                first_token,
            ),
            (
                "second",
                second_platform,
                second_channel,
                second_app,
                second_agent,
                second_token,
            ),
        ] {
            sqlx::query(
                "INSERT INTO external_platforms (id, key, name)
                 VALUES ($1, $2, $3)",
            )
            .bind(platform_id)
            .bind(format!("integration-origin-{index}"))
            .bind(format!("Integration Origin {index}"))
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO authentication_channels
                     (id, platform_id, key, name, enabled, trusted_email, created_by)
                 VALUES ($1, $2, 'oauth-app', 'OAuth App', true, true, $3)",
            )
            .bind(channel_id)
            .bind(platform_id)
            .bind(agent_owner.id)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO oauth_apps
                     (id, owner_id, name, client_id, client_secret_hash,
                      redirect_uris, external_platform_id, authentication_channel_id)
                 VALUES ($1, $2, $3, $4, 'unused', '[]'::jsonb, $5, $6)",
            )
            .bind(app_id)
            .bind(agent_owner.id)
            .bind(format!("{index} app"))
            .bind(format!("{index}-client"))
            .bind(platform_id)
            .bind(channel_id)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO integration_app_agents (app_id, agent_id) VALUES ($1, $2)")
                .bind(app_id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO oauth_access_tokens
                     (id, oauth_app_id, token_hash, expires_at, grant_type, scopes)
                 VALUES ($1, $2, $3, now() + interval '1 hour',
                         'client_credentials', $4)",
            )
            .bind(Uuid::new_v4())
            .bind(app_id)
            .bind(sha256_hex(token))
            .bind(vec![format!("agent:{agent_id}")])
            .execute(&pool)
            .await
            .unwrap();
        }
        for token in [same_app_other_origin_token, same_app_unbound_token] {
            sqlx::query(
                "INSERT INTO oauth_access_tokens
                     (id, oauth_app_id, token_hash, expires_at, grant_type, scopes)
                 VALUES ($1, $2, $3, now() + interval '1 hour',
                         'client_credentials', $4)",
            )
            .bind(Uuid::new_v4())
            .bind(first_app)
            .bind(sha256_hex(token))
            .bind(vec![format!("agent:{first_agent}")])
            .execute(&pool)
            .await
            .unwrap();
        }
        let app = build_router(test_state_with_pool(pool.clone()));

        let create_session = |token: &'static str,
                              agent_id: Uuid,
                              tenant: &'static str,
                              external_user: &'static str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/integrations/sessions")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"agent_id":"{agent_id}","external_user_id":"{external_user}","tenant_id":"{tenant}","email":"{external_user}-{tenant}@example.com","tools":[],"metadata":{{}}}}"#
                )))
                .unwrap()
        };
        let first_response = app
            .clone()
            .oneshot(create_session(
                first_token,
                first_agent,
                "tenant-a",
                "external-42",
            ))
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_session: IntegrationSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first_session.platform_id, first_platform);
        assert_eq!(first_session.tenant_id, "tenant-a");
        assert_ne!(first_session.owner_id, agent_owner.id);

        let second_response = app
            .clone()
            .oneshot(create_session(
                second_token,
                second_agent,
                "tenant-b",
                "external-42",
            ))
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_session: IntegrationSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second_session.platform_id, second_platform);
        assert_ne!(first_session.owner_id, second_session.owner_id);

        let same_app_other_origin_response = app
            .clone()
            .oneshot(create_session(
                same_app_other_origin_token,
                first_agent,
                "tenant-b",
                "external-99",
            ))
            .await
            .unwrap();
        assert_eq!(same_app_other_origin_response.status(), StatusCode::OK);
        let same_app_other_origin_session: IntegrationSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(same_app_other_origin_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_ne!(
            first_session.external_identity_id,
            same_app_other_origin_session.external_identity_id
        );

        let unbound_cross_origin = axum::http::Request::builder()
            .uri(format!("/api/integrations/sessions/{}", first_session.id))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {same_app_unbound_token}"),
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(unbound_cross_origin)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let same_app_cross_origin = axum::http::Request::builder()
            .uri(format!("/api/integrations/sessions/{}", first_session.id))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {same_app_other_origin_token}"),
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(same_app_cross_origin)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let same_app_cross_message = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/integrations/sessions/{}/messages",
                first_session.id
            ))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {same_app_other_origin_token}"),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"content":"same app can continue","attachments":[]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(same_app_cross_message)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        assert_eq!(
            app.clone()
                .oneshot(create_session(
                    first_token,
                    first_agent,
                    "tenant-a",
                    "external-42",
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone()
                .oneshot(create_session(
                    first_token,
                    first_agent,
                    "tenant-c",
                    "external-100",
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            sqlx::query_as::<_, (Option<String>, Option<Uuid>)>(
                "SELECT origin_tenant_id, origin_external_identity_id
                 FROM oauth_access_tokens WHERE token_hash = $1"
            )
            .bind(sha256_hex(first_token))
            .fetch_one(&pool)
            .await
            .unwrap(),
            (None, None)
        );

        let cross_origin = axum::http::Request::builder()
            .uri(format!("/api/integrations/sessions/{}", first_session.id))
            .header(header::AUTHORIZATION, format!("Bearer {second_token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(cross_origin).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let message_request = |content: &'static str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/api/integrations/sessions/{}/messages",
                    first_session.id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {first_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"content":"{content}","attachments":[{{"kind":"text","name":"note.txt","content_type":"text/plain","size_bytes":4,"text":"note"}}],"client_message_key":"integration-message-1"}}"#
                )))
                .unwrap()
        };
        let accepted = app.clone().oneshot(message_request("hello")).await.unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let accepted: IntegrationMessageResponse = serde_json::from_slice(
            &axum::body::to_bytes(accepted.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let accepted_message = accepted.message;
        assert_eq!(
            accepted.run.hub_session_id,
            Some(first_session.hub_session_id)
        );
        assert_eq!(
            accepted_message.payload["attachments"][0]["name"],
            "note.txt"
        );
        let same_app_cross_stop = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/integrations/sessions/{}/runs/{}/stop",
                first_session.id, accepted.run.id
            ))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {same_app_other_origin_token}"),
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(same_app_cross_stop)
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );

        let duplicate = app
            .clone()
            .oneshot(message_request("ignored duplicate"))
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate: IntegrationMessageResponse = serde_json::from_slice(
            &axum::body::to_bytes(duplicate.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(duplicate.run.id, accepted.run.id);
        assert_eq!(duplicate.message.id, accepted_message.id);

        let messages = axum::http::Request::builder()
            .uri(format!(
                "/api/integrations/sessions/{}/messages",
                first_session.id
            ))
            .header(header::AUTHORIZATION, format!("Bearer {first_token}"))
            .body(Body::empty())
            .unwrap();
        let messages = app.oneshot(messages).await.unwrap();
        assert_eq!(messages.status(), StatusCode::OK);
        let messages: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(messages.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0].content.as_deref(),
            Some("same app can continue")
        );
        assert_eq!(messages[1].id, accepted_message.id);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_routing_runtime_claim_includes_ordered_session_context(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let context = claim
            .session_context
            .expect("universal Run claim must include Session context");
        assert_eq!(claim.run.hub_session_id, Some(fixture.hub_session_id));
        assert_eq!(claim.run.hub_turn_id, Some(fixture.turn_id));
        assert_eq!(context.session.id, fixture.hub_session_id);
        assert_eq!(context.turn.id, fixture.turn_id);
        assert_eq!(
            context
                .messages
                .iter()
                .map(|message| message.content.as_deref().unwrap())
                .collect::<Vec<_>>(),
            vec!["claim message one", "claim message two"]
        );
        assert!(context
            .messages
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_ownership_competing_runtime_claims_have_one_generation_winner(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let second_runtime_id = Uuid::new_v4();
        let second_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{\"model_proxy\":true}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(second_runtime_id)
        .bind(sha256_hex(&second_runtime_token))
        .bind(format!("runtime-claim-second-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE agents SET runtime_id = NULL WHERE id = $1")
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let first = tokio::spawn({
            let state = fixture.state.clone();
            let token = fixture.runtime_token.clone();
            async move {
                runtime_claim_run(
                    State(state),
                    bearer_headers(&token),
                    runtime_claim_request(1, Vec::new()),
                )
                .await
                .map(IntoResponse::into_response)
            }
        });
        let second = tokio::spawn({
            let state = fixture.state.clone();
            async move {
                runtime_claim_run(
                    State(state),
                    bearer_headers(&second_runtime_token),
                    runtime_claim_request(1, Vec::new()),
                )
                .await
                .map(IntoResponse::into_response)
            }
        });
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        let statuses = [first.status(), second.status()];
        assert_eq!(
            statuses
                .iter()
                .filter(|status| **status == StatusCode::OK)
                .count(),
            1
        );
        assert_eq!(
            statuses
                .iter()
                .filter(|status| **status == StatusCode::NO_CONTENT)
                .count(),
            1
        );

        let successful_response = if first.status() == StatusCode::OK {
            first
        } else {
            second
        };
        let body = axum::body::to_bytes(successful_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let claim: ClaimRunResponse = serde_json::from_slice(&body).unwrap();
        let session = claim.session_context.unwrap().session;
        let (owner_id, generation): (Option<Uuid>, i64) = sqlx::query_as(
            "SELECT runtime_owner_id, ownership_generation
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let (run_generation, turn_generation): (i64, i64) = sqlx::query_as(
            "SELECT runs.session_ownership_generation, turns.ownership_generation
             FROM runs
             JOIN hub_session_turns AS turns ON turns.id = runs.hub_turn_id
             WHERE runs.id = $1",
        )
        .bind(fixture.run_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();

        assert_eq!(owner_id, claim.run.runtime_id);
        assert_eq!(generation, 1);
        assert_eq!(claim.run.session_ownership_generation, Some(generation));
        assert_eq!(session.runtime_owner_id, owner_id);
        assert_eq!(session.ownership_generation, generation);
        assert_eq!(run_generation, generation);
        assert_eq!(turn_generation, generation);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_ownership_generation_is_retained_for_owner_and_incremented_after_release(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let first = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(first.run.session_ownership_generation, Some(1));

        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(first.run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let same_owner_run =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        let same_owner = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(same_owner.run.id, same_owner_run);
        assert_eq!(same_owner.run.session_ownership_generation, Some(1));

        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(same_owner.run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = NULL, lifecycle_status = 'offline'
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let reacquired_run =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        let reacquired = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(reacquired.run.id, reacquired_run);
        assert_eq!(reacquired.run.session_ownership_generation, Some(2));
        assert_eq!(
            reacquired
                .session_context
                .unwrap()
                .session
                .ownership_generation,
            2
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_updated_at_tracks_only_conversation_input_and_output(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let baseline = "2026-07-17T08:00:00Z".parse::<DateTime<Utc>>().unwrap();
        sqlx::query("UPDATE hub_sessions SET updated_at = $1 WHERE id = $2")
            .bind(baseline)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let _ = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "reasoning".into(),
                    role: Some("assistant".into()),
                    content: Some("technical progress".into()),
                    payload: json!({ "source": "pi" }),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap();
        let after_technical_event: DateTime<Utc> =
            sqlx::query_scalar("SELECT updated_at FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(after_technical_event, baseline);

        let _ = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "message".into(),
                    role: Some("assistant".into()),
                    content: Some("final assistant output".into()),
                    payload: json!({ "source": "pi", "stop_reason": "stop" }),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap();
        let after_output: DateTime<Utc> =
            sqlx::query_scalar("SELECT updated_at FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert!(after_output > baseline);

        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("activity-native-session".into()),
                    work_dir_ref: Some("activity-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let after_completion: DateTime<Utc> =
            sqlx::query_scalar("SELECT updated_at FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(after_completion, after_output);

        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "new user input".into(),
                attachments: json!([]),
                client_message_key: Some("activity-user-input".into()),
            }),
        )
        .await
        .unwrap();
        let after_input: DateTime<Utc> =
            sqlx::query_scalar("SELECT updated_at FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert!(after_input > after_completion);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_generation_fences_event_tool_finalize_and_complete(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query("UPDATE hub_sessions SET ownership_generation = 2 WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let event = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "message".into(),
                    role: Some("assistant".into()),
                    content: Some("stale event".into()),
                    payload: json!({}),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(event.status, StatusCode::FORBIDDEN);

        let finalize = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(1, tool_request_batch(&fixture, [fixture.tool_request_id])),
        )
        .await
        .unwrap_err();
        assert_eq!(finalize.status, StatusCode::FORBIDDEN);

        let completion = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("stale-session".into()),
                    work_dir_ref: Some("stale-workdir".into()),
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(completion.status, StatusCode::FORBIDDEN);
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        assert_eq!(
            tool_request_count(&fixture.state.pool, fixture.tool_request_id).await,
            0
        );
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            ("running".into(), Some(fixture.runtime_id), None, None, None)
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_message_rejects_dangling_active_turn_instead_of_starting_next_turn(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let error = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "must not hide active Turn drift".into(),
                attachments: json!([]),
                client_message_key: Some("dangling-active-turn".into()),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE hub_session_id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_recovery_notice_cleared_after_first_message(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'offline', runtime_owner_id = NULL,
                 recovery_error = '服务端发生意外，导致 agent 环境数据丢失，但对话历史还在'
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let mut tx = fixture.state.pool.begin().await.unwrap();
        let _ = accept_session_message_tx(
            &mut tx,
            AcceptSessionMessage {
                session_id: fixture.hub_session_id,
                agent_id: fixture.agent_id,
                owner_id: sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
                    .bind(fixture.hub_session_id)
                    .fetch_one(&fixture.state.pool)
                    .await
                    .unwrap(),
                content: "继续对话".into(),
                payload: json!({}),
                role: "user".into(),
                message_kind: "message".into(),
                requested_delivery_mode: "next_turn".into(),
                client_message_key: None,
                source: "console".into(),
                automation_id: None,
                integration_session_id: None,
                parent_run_id: None,
                continuation_turn_id: None,
                model_subject_type: "user".into(),
                model_subject_user_id: None,
                model_source_integration_app_id: None,
                external_user_context: None,
                attachment_ids: Vec::new(),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let notice: Option<String> =
            sqlx::query_scalar("SELECT recovery_error FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(notice, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_old_runtime_owner_cannot_write_new_generation(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 2
             WHERE id = $2",
        )
        .bind(fixture.other_runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let error = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                2,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "message".into(),
                    role: Some("assistant".into()),
                    content: Some("old owner".into()),
                    payload: json!({}),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_bundle_commit_uses_the_frozen_checkpoint_with_newer_queued_history(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("frozen-bundle-thread".into()),
                    work_dir_ref: Some("frozen-bundle-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                reason: "idle".into(),
            }),
        )
        .await
        .unwrap()
        .0;

        let mut latest_sequence = attempt.history_checkpoint;
        for (content, delivery_mode, delivery_state) in [
            ("queued during save", "next_turn", "queued"),
            ("deferred during save", "later_turn", "deferred"),
        ] {
            latest_sequence = sqlx::query_scalar(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
                 VALUES ($1, $2, 'user', 'message', $3, $4, $5)
                 RETURNING sequence",
            )
            .bind(Uuid::new_v4())
            .bind(fixture.hub_session_id)
            .bind(content)
            .bind(delivery_mode)
            .bind(delivery_state)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        }
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(latest_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let mut tx = fixture.state.pool.begin().await.unwrap();
        let committed = commit_session_bundle_metadata_tx(
            &mut tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/frozen-checkpoint.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "frozen-checkpoint".into(),
                size_bytes: 1024,
                history_checkpoint: attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(committed.history_checkpoint, latest_sequence);
        assert_eq!(
            committed.current_bundle.unwrap().history_checkpoint,
            attempt.history_checkpoint
        );
        let _ = insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        let released = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(released.lifecycle_status, "waiting_for_runtime");
        assert_eq!(released.runtime_owner_id, None);
        let saving_state: (Option<i64>, Option<i64>, Option<String>, Option<Uuid>) =
            sqlx::query_as(
                "SELECT saving_history_checkpoint, saving_ownership_generation,
                        saving_reason, saving_checkpoint_attempt_id
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(saving_state, (None, None, None, None));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_bundle_commit_fences_attempt_and_replays_identical_metadata(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("attempt-fence-thread".into()),
                    work_dir_ref: Some("attempt-fence-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let begin = || BeginRuntimeSessionCheckpointRequest {
            ownership_generation: 1,
            reason: "idle".into(),
        };
        let prior_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin()),
        )
        .await
        .unwrap()
        .0;
        let _ = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: true,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    checkpoint_reason: None,
                }],
                cleaned_sessions: Vec::new(),
            }),
        )
        .await
        .unwrap();
        let current_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            prior_attempt.history_checkpoint,
            current_attempt.history_checkpoint
        );
        assert_ne!(
            prior_attempt.checkpoint_attempt_id,
            current_attempt.checkpoint_attempt_id
        );

        let mut stale_tx = fixture.state.pool.begin().await.unwrap();
        let stale = commit_session_bundle_metadata_tx(
            &mut stale_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/prior-attempt.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: prior_attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "prior-attempt".into(),
                size_bytes: 1024,
                history_checkpoint: prior_attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::CONFLICT);
        stale_tx.rollback().await.unwrap();

        let created_at = Utc::now();
        let metadata = SessionBundleCommitMetadata {
            checkpoint_attempt_id: current_attempt.checkpoint_attempt_id,
            bundle_generation: 1,
            checksum_sha256: "current-attempt".into(),
            size_bytes: 2048,
            history_checkpoint: current_attempt.history_checkpoint,
            producing_engine_version: "test".into(),
            created_at,
        };
        let mut commit_tx = fixture.state.pool.begin().await.unwrap();
        let committed = commit_session_bundle_metadata_tx(
            &mut commit_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/current-attempt.tar.zst",
            &metadata,
        )
        .await
        .unwrap();
        commit_tx.commit().await.unwrap();

        let mut replay_tx = fixture.state.pool.begin().await.unwrap();
        let replayed = commit_session_bundle_metadata_tx(
            &mut replay_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/current-attempt.tar.zst",
            &metadata,
        )
        .await
        .unwrap();
        replay_tx.commit().await.unwrap();
        assert_eq!(replayed.current_bundle, committed.current_bundle);

        let mut changed_tx = fixture.state.pool.begin().await.unwrap();
        let changed = commit_session_bundle_metadata_tx(
            &mut changed_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/changed-metadata.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: current_attempt.checkpoint_attempt_id,
                bundle_generation: 2,
                checksum_sha256: "changed-metadata".into(),
                size_bytes: 4096,
                history_checkpoint: current_attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(changed.status, StatusCode::CONFLICT);
        changed_tx.rollback().await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_bundle_commit_preserves_current_pointer_after_unreplayable_history(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("unreplayable-thread".into()),
                    work_dir_ref: Some("unreplayable-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let begin = || BeginRuntimeSessionCheckpointRequest {
            ownership_generation: 1,
            reason: "idle".into(),
        };
        let initial_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin()),
        )
        .await
        .unwrap()
        .0;
        let mut initial_tx = fixture.state.pool.begin().await.unwrap();
        let initial = commit_session_bundle_metadata_tx(
            &mut initial_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/last-safe.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: initial_attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "last-safe".into(),
                size_bytes: 1024,
                history_checkpoint: initial_attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        initial_tx.commit().await.unwrap();
        let initial_bundle = initial.current_bundle.unwrap();

        let _ = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: true,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    lifecycle_status: "online".into(),
                    checkpoint_reason: None,
                }],
                cleaned_sessions: Vec::new(),
            }),
        )
        .await
        .unwrap();
        let stale_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin()),
        )
        .await
        .unwrap()
        .0;
        let delivered_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'already reached the execution engine',
                     'record_only', 'delivered')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(delivered_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let mut stale_tx = fixture.state.pool.begin().await.unwrap();
        let stale = commit_session_bundle_metadata_tx(
            &mut stale_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/stale-after-delivery.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: stale_attempt.checkpoint_attempt_id,
                bundle_generation: 2,
                checksum_sha256: "stale-after-delivery".into(),
                size_bytes: 2048,
                history_checkpoint: stale_attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::CONFLICT);
        stale_tx.rollback().await.unwrap();

        let mut pointer_tx = fixture.state.pool.begin().await.unwrap();
        let after_stale = load_hub_session_tx(&mut pointer_tx, fixture.hub_session_id)
            .await
            .unwrap();
        pointer_tx.rollback().await.unwrap();
        assert_eq!(after_stale.current_bundle, Some(initial_bundle));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_bundle_commit_and_release_are_generation_fenced(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("bundle-thread".into()),
                    work_dir_ref: Some("bundle-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();

        let missing_bundle = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(missing_bundle.status, StatusCode::CONFLICT);

        let attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                reason: "idle".into(),
            }),
        )
        .await
        .unwrap()
        .0;

        let mut tx = fixture.state.pool.begin().await.unwrap();
        commit_session_bundle_metadata_tx(
            &mut tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/checkpoint-3.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "checkpoint-3".into(),
                size_bytes: 1024,
                history_checkpoint: 3,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        let initial = load_hub_session_tx(&mut tx, fixture.hub_session_id)
            .await
            .unwrap();
        let initial_bundle = initial.current_bundle.clone();
        tx.commit().await.unwrap();

        let new_checkpoint: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'accepted while saving',
                     'record_only', 'delivered')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(new_checkpoint, 4);
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(new_checkpoint)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let mut stale_tx = fixture.state.pool.begin().await.unwrap();
        let stale_commit = commit_session_bundle_metadata_tx(
            &mut stale_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/stale.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 2,
                checksum_sha256: "stale".into(),
                size_bytes: 2048,
                history_checkpoint: 3,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(stale_commit.status, StatusCode::CONFLICT);
        stale_tx.rollback().await.unwrap();
        let mut pointer_tx = fixture.state.pool.begin().await.unwrap();
        let after_stale = load_hub_session_tx(&mut pointer_tx, fixture.hub_session_id)
            .await
            .unwrap();
        pointer_tx.rollback().await.unwrap();
        assert_eq!(after_stale.current_bundle, initial_bundle);

        let behind_history = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(behind_history.status, StatusCode::CONFLICT);

        let mut stale_owner_tx = fixture.state.pool.begin().await.unwrap();
        let stale_owner = commit_session_bundle_metadata_tx(
            &mut stale_owner_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            2,
            "hub/bundles/stale-owner.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 2,
                checksum_sha256: "stale-owner".into(),
                size_bytes: 2048,
                history_checkpoint: 4,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(stale_owner.status, StatusCode::CONFLICT);
        stale_owner_tx.rollback().await.unwrap();

        sqlx::query(
            "UPDATE hub_session_messages SET delivery_state = 'failed'
             WHERE session_id = $1 AND sequence = $2",
        )
        .bind(fixture.hub_session_id)
        .bind(new_checkpoint)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_messages SET delivery_state = 'failed'
             WHERE session_id = $1 AND delivery_state = 'queued'",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs SET status = 'failed'
             WHERE hub_session_id = $1 AND status = 'pending'",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let released = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(released.runtime_owner_id, None);
        assert_eq!(released.ownership_generation, 1);
        assert_eq!(released.lifecycle_status, "offline");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_session_cleanup_obligations
                 WHERE runtime_id = $1 AND session_id = $2 AND ownership_generation = 1",
            )
            .bind(fixture.runtime_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn attachment_upload_and_download_enforce_ownership_and_content_type(pool: PgPool) {
        let fixture = attachment_fixture(pool).await;
        let (objects, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let uploaded = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Query(AttachmentUploadQuery::default()),
            attachment_multipart(
                "attachment-upload-test",
                Some(fixture.session_id),
                "report.pdf",
                "application/pdf",
                b"pdf-bytes",
            )
            .await,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(uploaded.name, "report.pdf");
        assert_eq!(uploaded.content_type, "application/pdf");
        assert_eq!(uploaded.size_bytes, 9);
        assert_eq!(uploaded.session_id, fixture.session_id);
        let row = sqlx::query_as::<_, (Option<Uuid>, Option<Uuid>, String, String)>(
            "SELECT message_id, run_id, object_key, checksum_sha256
             FROM hub_session_attachments WHERE id = $1",
        )
        .bind(uploaded.id)
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(row.0, None);
        assert_eq!(row.1, None);
        assert!(row
            .2
            .starts_with(&format!("attachments/{}/", fixture.session_id)));
        assert_eq!(row.3, format!("{:x}", Sha256::digest(b"pdf-bytes")));
        let object_key = row.2;
        assert_eq!(
            objects.lock().unwrap().get(&object_key).unwrap(),
            b"pdf-bytes"
        );

        let response = download_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(uploaded.id),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/pdf"
        );
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            "attachment; filename*=UTF-8''report.pdf"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, Bytes::from_static(b"pdf-bytes"));

        let foreign_error = download_attachment(
            State(state.clone()),
            session_headers(&fixture.foreign_token),
            Path(uploaded.id),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign_error.status, StatusCode::NOT_FOUND);

        let image = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart("image-upload", None, "photo.png", "image/png", b"png-bytes")
                .await,
        )
        .await
        .unwrap()
        .0;
        let image_response = download_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(image.id),
        )
        .await
        .unwrap();
        assert_eq!(
            image_response
                .headers()
                .get(header::CONTENT_DISPOSITION)
                .unwrap(),
            "inline"
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn attachment_upload_enforces_file_and_session_limits_and_rejects_foreign_owner(
        pool: PgPool,
    ) {
        let fixture = attachment_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let app = build_router((*state).clone());
        let huge = vec![0_u8; MAX_ATTACHMENT_UPLOAD_BYTES as usize + 1];
        let mut huge_body = Vec::new();
        huge_body.extend_from_slice(b"--huge-upload\r\n");
        huge_body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"huge.bin\"\r\n",
        );
        huge_body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        huge_body.extend_from_slice(&huge);
        huge_body.extend_from_slice(b"\r\n--huge-upload--\r\n");
        let huge_request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/attachments?session_id={}",
                fixture.session_id
            ))
            .header(
                header::COOKIE,
                format!("agent_hub_session={}", fixture.owner_token),
            )
            .header(
                header::CONTENT_TYPE,
                "multipart/form-data; boundary=huge-upload",
            )
            .body(Body::from(huge_body))
            .unwrap();
        let huge_response = app.oneshot(huge_request).await.unwrap();
        assert_eq!(huge_response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        sqlx::query(
            "INSERT INTO hub_session_attachments
                 (id, session_id, owner_id, name, content_type, size_bytes,
                  object_key, checksum_sha256)
             VALUES ($1, $2, $3, 'seeded', 'application/octet-stream', $4,
                     'attachments/seeded', $5)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.session_id)
        .bind(fixture.owner_id)
        .bind(MAX_ATTACHMENT_BYTES_PER_SESSION)
        .bind("a".repeat(64))
        .execute(&state.pool)
        .await
        .unwrap();
        let over_total = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart(
                "over-total",
                None,
                "one.bin",
                "application/octet-stream",
                b"1",
            )
            .await,
        )
        .await
        .unwrap_err();
        assert_eq!(over_total.status, StatusCode::BAD_REQUEST);
        assert!(over_total.message.contains("storage limit"));

        let foreign = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.foreign_token),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart(
                "foreign-upload",
                None,
                "foreign.bin",
                "application/octet-stream",
                b"foreign",
            )
            .await,
        )
        .await
        .unwrap_err();
        assert_eq!(foreign.status, StatusCode::NOT_FOUND);
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn attachment_message_binding_returns_attachments_and_rejects_rebinding(pool: PgPool) {
        let fixture = attachment_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let attachment = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart(
                "binding-upload",
                None,
                "bound.txt",
                "text/plain",
                b"bound-bytes",
            )
            .await,
        )
        .await
        .unwrap()
        .0;
        let accepted = create_hub_session_message(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(fixture.session_id),
            Json(CreateHubSessionMessageRequest {
                content: "message with attachment".into(),
                payload: json!({}),
                attachment_ids: vec![attachment.id],
                delivery_mode: None,
                client_message_key: None,
                parent_run_id: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(accepted.message.attachments.len(), 1);
        assert_eq!(accepted.message.attachments[0].id, attachment.id);
        let bound: (Option<Uuid>, Option<Uuid>) =
            sqlx::query_as("SELECT message_id, run_id FROM hub_session_attachments WHERE id = $1")
                .bind(attachment.id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(bound.0, Some(accepted.message.id));
        assert_eq!(bound.1, Some(accepted.run.as_ref().unwrap().id));

        let listed = list_hub_session_messages(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(fixture.session_id),
            Query(SessionMessageListQuery::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(listed.iter().any(|message| {
            message.id == accepted.message.id
                && message
                    .attachments
                    .iter()
                    .any(|item| item.id == attachment.id)
        }));

        let rebound = create_hub_session_message(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(fixture.session_id),
            Json(CreateHubSessionMessageRequest {
                content: "second message".into(),
                payload: json!({}),
                attachment_ids: vec![attachment.id],
                delivery_mode: None,
                client_message_key: None,
                parent_run_id: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(rebound.status, StatusCode::BAD_REQUEST);
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_deletion_removes_attachment_rows_and_objects(pool: PgPool) {
        let fixture = attachment_fixture(pool).await;
        let (objects, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'offline' WHERE id = $1")
            .bind(fixture.session_id)
            .execute(&state.pool)
            .await
            .unwrap();
        let attachment = upload_attachment(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart(
                "delete-upload",
                None,
                "delete.bin",
                "application/octet-stream",
                b"delete-bytes",
            )
            .await,
        )
        .await
        .unwrap()
        .0;
        assert!(!objects.lock().unwrap().is_empty());

        let deleted = delete_hub_session(
            State(state.clone()),
            session_headers(&fixture.owner_token),
            Path(fixture.session_id),
        )
        .await
        .unwrap();
        assert_eq!(deleted, StatusCode::NO_CONTENT);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM hub_session_attachments WHERE id = $1"
            )
            .bind(attachment.id)
            .fetch_one(&state.pool)
            .await
            .unwrap(),
            0
        );
        assert!(objects.lock().unwrap().is_empty());
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn stop_on_terminal_run_is_idempotent_and_returns_the_run(pool: PgPool) {
        let fixture =
            runtime_claim_fixture(pool.clone(), "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let run = request_run_interrupt_tx(&mut tx, fixture.run_id, fixture.hub_session_id)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(run.status, "completed");
        let requested = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT interrupt_requested_at FROM hub_session_turns WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(requested.is_none());
    }
}
