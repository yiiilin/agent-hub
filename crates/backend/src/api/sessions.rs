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
use crate::model_test_response_text;
use crate::send_model_gateway_request;
use crate::load_agent_for_user;
use crate::insert_hub_native_session_tx;
use crate::load_run_for_user;
use crate::load_run_public_tx;
use crate::load_widget_credential_tx;
use crate::load_widget_scoped_session_tx;
use crate::missing_secret_grants;
use crate::model_connection_scope_name;
use crate::model_request_settings_value;
use crate::model_upstream_protocol_name;
use crate::MAX_ATTACHMENT_BYTES_PER_SESSION;
use crate::MAX_ATTACHMENT_UPLOAD_BYTES;
use crate::ModelGatewayForwardRequest;
use crate::model_response_status;
use crate::model_upstream_protocol_from_name;
use crate::normalize_client_message_key;
use crate::ObservedModelUsage;
use crate::record_runtime_session_cleanup_tx;
use crate::SESSION_MESSAGE_PAGE_SQL;
use crate::widget_session_locator;

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
                current_bundle_created_at, created_at, updated_at
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
    mut multipart: Multipart,
) -> Result<ParsedMessageMultipart, ApiError> {
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
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad_request("message attachment body is invalid"))?
                {
                    size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
                        ApiError::bad_request("message attachment exceeds the 100MB limit")
                    })?;
                    if size > MAX_ATTACHMENT_UPLOAD_BYTES {
                        return Err(ApiError::payload_too_large(
                            "message attachment exceeds the 100MB limit",
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
    let parsed = parse_message_multipart(multipart).await?;
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
    let parsed = parse_message_multipart(multipart).await?;
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
                current_bundle_created_at, created_at, updated_at
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
    let staged = stage_attachment_upload(multipart).await?;
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
    let staged = stage_attachment_upload(multipart).await?;
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
    mut multipart: Multipart,
) -> Result<StagedAttachmentUpload, ApiError> {
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
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|_| ApiError::bad_request("attachment file body is invalid"))?
                {
                    size = size.checked_add(chunk.len() as u64).ok_or_else(|| {
                        ApiError::bad_request("attachment file exceeds the 100MB limit")
                    })?;
                    if size > MAX_ATTACHMENT_UPLOAD_BYTES {
                        return Err(ApiError::payload_too_large(
                            "attachment file exceeds the 100MB limit",
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
    if current_total
        .checked_add(size_bytes)
        .is_none_or(|total| total > MAX_ATTACHMENT_BYTES_PER_SESSION)
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
                 lifecycle_status = 'online'
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

