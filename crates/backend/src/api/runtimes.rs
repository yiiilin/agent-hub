//! runtimes 领域模块。

use super::*;
use crate::*;
use agent_hub_shared::*;

pub(crate) const MODEL_PROXY_BINDING_ID_HEADER: &str = "x-agent-hub-model-binding-id";

pub(crate) const MODEL_PROXY_OBSERVER_MAX_BYTES: usize = 2 * 1024 * 1024;

pub(crate) const MODEL_PROXY_SSE_LINE_MAX_BYTES: usize = 64 * 1024;

pub(crate) const MAX_RUNTIME_EVENT_BYTES: usize = 256 * 1024;

#[allow(dead_code)] // Task 11 calls this only after Hub-managed object upload succeeds.
pub(crate) struct SessionBundleCommitMetadata {
    pub(crate) checkpoint_attempt_id: Uuid,
    pub(crate) bundle_generation: i64,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: i64,
    pub(crate) history_checkpoint: i64,
    pub(crate) producing_engine_version: String,
    pub(crate) created_at: DateTime<Utc>,
}

pub(crate) async fn list_runtimes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<RuntimeDto>>, ApiError> {
    require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, hostname, labels, engine_version, capabilities, sandbox_mode,
                status, last_heartbeat_at, rotation_requested_at
         FROM runtimes ORDER BY last_heartbeat_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(runtime_from_row).collect()))
}

pub(crate) async fn create_runtime_enrollment_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<CreateRuntimeEnrollmentTokenResponse>, ApiError> {
    let creator = require_administrator(&state, &headers).await?;
    let token = opaque_secret("ahre_");
    let row = sqlx::query(
        "INSERT INTO runtime_enrollment_tokens
             (id, token_hash, created_by, expires_at)
         VALUES ($1, $2, $3, now() + interval '30 minutes')
         RETURNING id, created_by, expires_at, consumed_at, consumed_by_runtime_id,
                   revoked_at, created_at",
    )
    .bind(Uuid::new_v4())
    .bind(sha256_hex(&token))
    .bind(creator.id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(CreateRuntimeEnrollmentTokenResponse {
        enrollment: runtime_enrollment_from_row(row),
        token,
    }))
}

pub(crate) async fn list_runtime_enrollment_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<RuntimeEnrollmentTokenDto>>, ApiError> {
    require_administrator(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, created_by, expires_at, consumed_at, consumed_by_runtime_id,
                revoked_at, created_at
         FROM runtime_enrollment_tokens
         ORDER BY created_at DESC",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(
        rows.into_iter().map(runtime_enrollment_from_row).collect(),
    ))
}

pub(crate) async fn revoke_runtime_enrollment_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(enrollment_id): Path<Uuid>,
) -> Result<Json<RuntimeEnrollmentTokenDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    let row = sqlx::query(
        "UPDATE runtime_enrollment_tokens
         SET revoked_at = now()
         WHERE id = $1 AND consumed_at IS NULL AND revoked_at IS NULL
         RETURNING id, created_by, expires_at, consumed_at, consumed_by_runtime_id,
                   revoked_at, created_at",
    )
    .bind(enrollment_id)
    .fetch_optional(&state.pool)
    .await?;
    if let Some(row) = row {
        return Ok(Json(runtime_enrollment_from_row(row)));
    }
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runtime_enrollment_tokens WHERE id = $1)")
            .bind(enrollment_id)
            .fetch_one(&state.pool)
            .await?;
    if exists {
        Err(ApiError::conflict(
            "runtime enrollment token is already consumed or revoked",
        ))
    } else {
        Err(ApiError::not_found("runtime enrollment token not found"))
    }
}

pub(crate) async fn request_runtime_credential_rotation(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
) -> Result<Json<RuntimeDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    let row = sqlx::query(
        "UPDATE runtimes
         SET rotation_requested_at = COALESCE(rotation_requested_at, now())
         WHERE id = $1 AND credential_revoked_at IS NULL
         RETURNING id, hostname, labels, engine_version, capabilities, sandbox_mode,
                   status, last_heartbeat_at, rotation_requested_at",
    )
    .bind(runtime_id)
    .fetch_optional(&state.pool)
    .await?;
    row.map(|row| Json(runtime_from_row(row)))
        .ok_or(ApiError::not_found("runtime not found"))
}

pub(crate) async fn drain_runtime(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
    Json(req): Json<ConfirmRuntimeHostnameRequest>,
) -> Result<Json<RuntimeDrainResponse>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let runtime = sqlx::query("SELECT hostname FROM runtimes WHERE id = $1 FOR UPDATE")
        .bind(runtime_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::not_found("runtime not found"))?;
    confirm_runtime_hostname(runtime.get("hostname"), &req.hostname)?;
    require_runtime_session_authority_tx(&mut tx, runtime_id, &administrator).await?;
    sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE hub_sessions AS sessions
         SET lifecycle_status = 'saving',
             saving_history_checkpoint = COALESCE(
                 saving_history_checkpoint, history_checkpoint
             ),
             saving_ownership_generation = COALESCE(
                 saving_ownership_generation, ownership_generation
             ),
             saving_reason = 'drain',
             saving_checkpoint_attempt_id = COALESCE(
                 saving_checkpoint_attempt_id, gen_random_uuid()
             ),
             last_checkpoint_attempt_id = NULL,
             last_checkpoint_ownership_generation = NULL,
             last_checkpoint_disposition = NULL,
             last_checkpoint_has_queued_work = NULL
         WHERE sessions.runtime_owner_id = $1
           AND NOT EXISTS (
             SELECT 1 FROM runs
             WHERE runs.hub_session_id = sessions.id
               AND runs.status IN ('running', 'waiting_tool')
           )",
    )
    .bind(runtime_id)
    .execute(&mut *tx)
    .await?;
    let response = load_runtime_drain_response_tx(&mut tx, runtime_id).await?;
    tx.commit().await?;
    Ok(Json(response))
}

pub(crate) async fn cancel_runtime_drain(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
) -> Result<Json<RuntimeDrainResponse>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT id FROM runtimes WHERE id = $1 FOR UPDATE")
        .bind(runtime_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::not_found("runtime not found"))?;
    require_runtime_session_authority_tx(&mut tx, runtime_id, &administrator).await?;
    let updated = sqlx::query(
        "UPDATE runtimes
         SET status = CASE
             WHEN last_heartbeat_at >= now() - interval '30 seconds' THEN 'online'
             ELSE 'offline'
         END
         WHERE id = $1 AND status = 'draining'
         RETURNING id",
    )
    .bind(runtime_id)
    .fetch_optional(&mut *tx)
    .await?;
    if updated.is_none() {
        return Err(ApiError::conflict("runtime is not draining"));
    }
    let response = load_runtime_drain_response_tx(&mut tx, runtime_id).await?;
    tx.commit().await?;
    Ok(Json(response))
}

pub(crate) async fn delete_drained_runtime(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
    Json(req): Json<ConfirmRuntimeHostnameRequest>,
) -> Result<StatusCode, ApiError> {
    require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let runtime = sqlx::query("SELECT hostname, status FROM runtimes WHERE id = $1 FOR UPDATE")
        .bind(runtime_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::not_found("runtime not found"))?;
    confirm_runtime_hostname(runtime.get("hostname"), &req.hostname)?;
    if runtime.get::<String, _>("status") != "draining" {
        return Err(ApiError::conflict(
            "runtime must be draining before ordinary deletion",
        ));
    }
    let owned_sessions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM hub_sessions WHERE runtime_owner_id = $1")
            .bind(runtime_id)
            .fetch_one(&mut *tx)
            .await?;
    if owned_sessions != 0 {
        return Err(ApiError::conflict(
            "runtime still owns Sessions that must checkpoint and release",
        ));
    }
    let pending_cleanups: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM runtime_session_cleanup_obligations
         WHERE runtime_id = $1",
    )
    .bind(runtime_id)
    .fetch_one(&mut *tx)
    .await?;
    if pending_cleanups != 0 {
        return Err(ApiError::conflict(
            "runtime still has Session Workspace cleanups to confirm",
        ));
    }
    sqlx::query("DELETE FROM runtimes WHERE id = $1")
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

struct RuntimeDeletionImpactSessionState {
    impact: RuntimeDeletionImpactSessionDto,
    ownership_generation: i64,
    active_turn_id: Option<Uuid>,
    recoverable: bool,
}

pub(crate) async fn require_runtime_session_authority_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
    administrator: &UserDto,
) -> Result<(), ApiError> {
    if administrator.role == "super_admin" {
        return Ok(());
    }
    let has_protected_sessions: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1
             FROM hub_sessions AS sessions
             LEFT JOIN users AS session_owner ON session_owner.id = sessions.owner_id
             LEFT JOIN agents ON agents.id = sessions.agent_id
             LEFT JOIN users AS agent_owner ON agent_owner.id = agents.owner_id
             WHERE sessions.runtime_owner_id = $1
               AND (session_owner.role = 'super_admin'
                    OR agent_owner.role = 'super_admin')
         )",
    )
    .bind(runtime_id)
    .fetch_one(&mut **tx)
    .await?;
    if has_protected_sessions {
        return Err(ApiError::forbidden(
            "super administrator permission is required for a Runtime that owns protected Sessions",
        ));
    }
    Ok(())
}

pub(crate) async fn get_runtime_deletion_impact(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
) -> Result<Json<RuntimeDeletionImpactDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let runtime = sqlx::query("SELECT hostname FROM runtimes WHERE id = $1")
        .bind(runtime_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::not_found("runtime not found"))?;
    require_runtime_session_authority_tx(&mut tx, runtime_id, &administrator).await?;
    let affected_sessions = load_runtime_deletion_impact_sessions_tx(&mut tx, runtime_id, false)
        .await?
        .into_iter()
        .map(|session| session.impact)
        .collect();
    let impact = RuntimeDeletionImpactDto {
        runtime_id,
        hostname: runtime.get("hostname"),
        affected_sessions,
    };
    tx.commit().await?;
    Ok(Json(impact))
}

pub(crate) async fn load_runtime_deletion_impact_sessions_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
    lock_for_update: bool,
) -> Result<Vec<RuntimeDeletionImpactSessionState>, ApiError> {
    let query = if lock_for_update {
        "SELECT sessions.id,
                (SELECT name FROM agents WHERE agents.id = sessions.agent_id) AS agent_name,
                sessions.lifecycle_status, sessions.current_bundle_history_checkpoint,
                sessions.ownership_generation, sessions.active_turn_id
         FROM hub_sessions AS sessions
         WHERE sessions.runtime_owner_id = $1
         ORDER BY sessions.created_at, sessions.id
         FOR UPDATE OF sessions"
    } else {
        "SELECT sessions.id,
                (SELECT name FROM agents WHERE agents.id = sessions.agent_id) AS agent_name,
                sessions.lifecycle_status, sessions.current_bundle_history_checkpoint,
                sessions.ownership_generation, sessions.active_turn_id
         FROM hub_sessions AS sessions
         WHERE sessions.runtime_owner_id = $1
         ORDER BY sessions.created_at, sessions.id"
    };
    let rows = sqlx::query(query)
        .bind(runtime_id)
        .fetch_all(&mut **tx)
        .await?;
    let mut sessions = Vec::with_capacity(rows.len());
    for row in rows {
        let session_id: Uuid = row.get("id");
        let bundle_checkpoint: Option<i64> = row.get("current_bundle_history_checkpoint");
        let has_unreplayable_history = if let Some(bundle_checkpoint) = bundle_checkpoint {
            sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM hub_session_messages
                     WHERE session_id = $1
                       AND sequence > $2
                       AND delivery_state IN ('delivering', 'delivered')
                 )",
            )
            .bind(session_id)
            .bind(bundle_checkpoint)
            .fetch_one(&mut **tx)
            .await?
        } else {
            true
        };
        let recoverable = bundle_checkpoint.is_some() && !has_unreplayable_history;
        sessions.push(RuntimeDeletionImpactSessionState {
            impact: RuntimeDeletionImpactSessionDto {
                session_id,
                agent_name: row.get("agent_name"),
                lifecycle_status: row.get("lifecycle_status"),
                force_delete_disposition: if recoverable {
                    "recoverable".into()
                } else {
                    "recovery_failed".into()
                },
            },
            ownership_generation: row.get("ownership_generation"),
            active_turn_id: row.get("active_turn_id"),
            recoverable,
        });
    }
    Ok(sessions)
}

pub(crate) async fn force_delete_runtime(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(runtime_id): Path<Uuid>,
    Json(req): Json<ConfirmRuntimeHostnameRequest>,
) -> Result<Json<ForceDeleteRuntimeResponse>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let runtime = sqlx::query("SELECT hostname FROM runtimes WHERE id = $1 FOR UPDATE")
        .bind(runtime_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::not_found("runtime not found"))?;
    confirm_runtime_hostname(runtime.get("hostname"), &req.hostname)?;
    require_runtime_session_authority_tx(&mut tx, runtime_id, &administrator).await?;
    let owned_sessions =
        load_runtime_deletion_impact_sessions_tx(&mut tx, runtime_id, true).await?;
    let mut recoverable_session_ids = Vec::new();
    let mut recovery_failed_session_ids = Vec::new();
    for session in owned_sessions {
        let session_id = session.impact.session_id;
        let recoverable = session.recoverable;
        if recoverable && session.impact.lifecycle_status == "restoring" {
            let released_ownership_generation = session.ownership_generation + 1;
            let requeued_runs = sqlx::query(
                "UPDATE runs AS recoverable_runs
                 SET status = 'pending', runtime_id = NULL,
                     model_proxy_token_hash = NULL,
                     session_ownership_generation = $2,
                     updated_at = now()
                 FROM hub_session_turns AS pending_turns
                 WHERE recoverable_runs.hub_session_id = $1
                   AND recoverable_runs.status = 'running'
                   AND pending_turns.id = recoverable_runs.hub_turn_id
                   AND pending_turns.session_id = recoverable_runs.hub_session_id
                   AND pending_turns.status = 'pending'
                   AND pending_turns.native_turn_id IS NULL
                 RETURNING recoverable_runs.id, recoverable_runs.hub_turn_id",
            )
            .bind(session_id)
            .bind(released_ownership_generation)
            .fetch_all(&mut *tx)
            .await?;
            for run in requeued_runs {
                sqlx::query(
                    "UPDATE hub_session_turns
                     SET status = 'pending', native_turn_id = NULL,
                         ownership_generation = $1,
                         started_at = NULL, ended_at = NULL, updated_at = now()
                     WHERE id = $2 AND session_id = $3",
                )
                .bind(released_ownership_generation)
                .bind(run.get::<Uuid, _>("hub_turn_id"))
                .bind(session_id)
                .execute(&mut *tx)
                .await?;
                insert_run_event_tx(
                    &mut tx,
                    run.get("id"),
                    "status".into(),
                    None,
                    Some("pending".into()),
                    json!({
                        "status": "pending",
                        "reason": "runtime force deleted before native Turn started"
                    }),
                )
                .await?;
            }
        }

        let failed_runs = sqlx::query(
            "UPDATE runs
             SET status = 'failed', runtime_id = NULL, model_proxy_token_hash = NULL,
                 updated_at = now()
             WHERE hub_session_id = $1
               AND (status IN ('running', 'waiting_tool') OR ($2 = false AND status = 'pending'))
             RETURNING id",
        )
        .bind(session_id)
        .bind(recoverable)
        .fetch_all(&mut *tx)
        .await?;
        for run in failed_runs {
            insert_run_event_tx(
                &mut tx,
                run.get("id"),
                "status".into(),
                None,
                Some("failed".into()),
                json!({ "status": "failed", "reason": "runtime force deleted" }),
            )
            .await?;
        }
        if let Some(active_turn_id) = session.active_turn_id {
            sqlx::query(
                "UPDATE hub_session_turns
                 SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
                 WHERE id = $1 AND session_id = $2",
            )
            .bind(active_turn_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = NULL,
                 ownership_generation = ownership_generation + 1,
                 active_turn_id = NULL,
                 lifecycle_status = CASE
                     WHEN $2 = false THEN 'recovery_failed'
                     WHEN EXISTS (
                         SELECT 1 FROM runs
                         WHERE hub_session_id = $1 AND status = 'pending'
                     ) OR EXISTS (
                         SELECT 1 FROM hub_session_messages
                         WHERE session_id = $1 AND delivery_state = 'queued'
                     ) THEN 'waiting_for_runtime'
                     ELSE 'offline'
                 END,
                 recovery_error = CASE
                     WHEN $2 = false THEN 'Runtime was force deleted without a restorable current Session Bundle'
                     ELSE NULL
                 END,
                 saving_history_checkpoint = NULL,
                 saving_ownership_generation = NULL,
                 saving_reason = NULL,
                 saving_checkpoint_attempt_id = NULL
             WHERE id = $1 AND runtime_owner_id = $3",
        )
        .bind(session_id)
        .bind(recoverable)
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
        if recoverable {
            recoverable_session_ids.push(session_id);
        } else {
            recovery_failed_session_ids.push(session_id);
        }
    }
    sqlx::query(
        "UPDATE runs
         SET status = 'failed', runtime_id = NULL, model_proxy_token_hash = NULL,
             updated_at = now()
         WHERE runtime_id = $1 AND status IN ('running', 'waiting_tool')",
    )
    .bind(runtime_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM runtimes WHERE id = $1")
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Json(ForceDeleteRuntimeResponse {
        runtime_id,
        recoverable_session_ids,
        recovery_failed_session_ids,
    }))
}

pub(crate) fn confirm_runtime_hostname(expected: &str, supplied: &str) -> Result<(), ApiError> {
    if expected == supplied {
        Ok(())
    } else {
        Err(ApiError::conflict(
            "runtime hostname confirmation does not match exactly",
        ))
    }
}

pub(crate) async fn load_runtime_drain_response_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
) -> Result<RuntimeDrainResponse, ApiError> {
    let runtime = sqlx::query(
        "SELECT id, hostname, labels, engine_version, capabilities, sandbox_mode,
                status, last_heartbeat_at, rotation_requested_at
         FROM runtimes WHERE id = $1",
    )
    .bind(runtime_id)
    .fetch_optional(&mut **tx)
    .await?
    .map(runtime_from_row)
    .ok_or(ApiError::not_found("runtime not found"))?;
    let session_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM hub_sessions
         WHERE runtime_owner_id = $1
         ORDER BY created_at, id",
    )
    .bind(runtime_id)
    .fetch_all(&mut **tx)
    .await?;
    let mut owned_sessions = Vec::with_capacity(session_ids.len());
    for session_id in session_ids {
        owned_sessions.push(load_hub_session_tx(tx, session_id).await?);
    }
    Ok(RuntimeDrainResponse {
        runtime,
        owned_sessions,
    })
}

pub(crate) async fn runtime_register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RuntimeRegisterRequest>,
) -> Result<Json<RuntimeRegisterResponse>, ApiError> {
    let enrollment_token = bearer_token(&headers)
        .filter(|token| !token.trim().is_empty())
        .ok_or(ApiError::unauthorized("missing runtime enrollment token"))?;
    if req.hostname.trim().is_empty() {
        return Err(ApiError::bad_request("runtime hostname is required"));
    }
    let runtime_credential = opaque_secret("ahrc_");
    let runtime_credential_hash = sha256_hex(&runtime_credential);
    let runtime_id = Uuid::new_v4();
    let mut tx = state.pool.begin().await?;
    let enrollment_id = sqlx::query_scalar::<_, Uuid>(
        "UPDATE runtime_enrollment_tokens
         SET consumed_at = now()
         WHERE token_hash = $1
           AND consumed_at IS NULL
           AND revoked_at IS NULL
           AND expires_at > now()
         RETURNING id",
    )
    .bind(sha256_hex(&enrollment_token))
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::unauthorized(
        "invalid, expired, or consumed runtime enrollment token",
    ))?;
    sqlx::query(
        "INSERT INTO runtimes
             (id, token_hash, hostname, labels, engine_version, capabilities,
              sandbox_mode, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'online')",
    )
    .bind(runtime_id)
    .bind(&runtime_credential_hash)
    .bind(req.hostname.trim())
    .bind(req.labels)
    .bind(req.engine_version)
    .bind(req.capabilities)
    .bind(req.sandbox_mode)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE runtime_enrollment_tokens
         SET consumed_by_runtime_id = $1
         WHERE id = $2",
    )
    .bind(runtime_id)
    .bind(enrollment_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Json(RuntimeRegisterResponse {
        runtime_id,
        runtime_credential,
        protocol_capabilities: vec![ATOMIC_WAITING_TOOL_BATCH_CAPABILITY.into()],
    }))
}

pub(crate) async fn runtime_keepalive(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let token =
        bearer_token(&headers).ok_or(ApiError::unauthorized("missing runtime credential"))?;
    let credential_hash = sha256_hex(&token);
    let updated = sqlx::query(
        "UPDATE runtimes
         SET status = CASE WHEN status = 'draining' THEN status ELSE 'online' END,
             last_heartbeat_at = now()
         WHERE credential_revoked_at IS NULL
           AND (token_hash = $1 OR pending_token_hash = $1)",
    )
    .bind(&credential_hash)
    .execute(&state.pool)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::unauthorized("invalid runtime credential"));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// 强制停止快照上传（force-stop 专用）：runtime 杀进程后打包上传工作区快照。
/// 鉴权：operation 的 target_runtime 必须等于本 runtime 且 state='pending'。
/// 上传成功提交：bundle 元数据 + operation→succeeded + 会话释放 owner 转 offline
///（用户重发消息 → accept 创建新任务 → claim 走 bundle 恢复）。
pub(crate) async fn runtime_upload_force_stop_bundle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(operation_id): Path<Uuid>,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let checksum = required_header(&headers, "x-agent-hub-bundle-sha256")?;
    validate_sha256_hex(&checksum).map_err(|_| ApiError::bad_request("invalid bundle checksum"))?;
    let size_bytes: u64 = parse_required_header(&headers, "x-agent-hub-bundle-size")?;
    if size_bytes == 0 || size_bytes > state.session_bundle_max_bytes {
        return Err(ApiError::bad_request("invalid Session Bundle size"));
    }
    let store =
        state
            .session_bundle_store
            .as_ref()
            .cloned()
            .ok_or(ApiError::service_unavailable(
                "Session Bundle object storage is not configured",
            ))?;
    let mut tx = state.pool.begin().await?;
    let op = sqlx::query(
        "SELECT session_id, run_id, target_runtime_id, state
         FROM force_stop_operation WHERE operation_id = $1 FOR UPDATE",
    )
    .bind(operation_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("force stop operation not found"))?;
    let session_id: Uuid = op.get("session_id");
    let run_id: Uuid = op.get("run_id");
    let target: Option<Uuid> = op.get("target_runtime_id");
    let op_state: String = op.get("state");
    let session_ownership_generation: i64 =
        sqlx::query_scalar("SELECT ownership_generation FROM hub_sessions WHERE id = $1")
            .bind(session_id)
            .fetch_one(&mut *tx)
            .await?;
    if target != Some(runtime_id) {
        return Err(ApiError::forbidden(
            "runtime does not own this force stop operation",
        ));
    }
    if op_state != "pending" {
        return Err(ApiError::conflict("force stop operation is not pending"));
    }
    let session_state: String =
        sqlx::query_scalar("SELECT lifecycle_status FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(session_id)
            .fetch_one(&mut *tx)
            .await?;
    if session_state != "force_stopping" {
        return Err(ApiError::conflict("session is not force stopping"));
    }
    let object_key = format!("sessions/{session_id}/force-stop-{operation_id}.tar.zst");
    tx.commit().await?;

    // 边传边算实际字节数/SHA-256：SigV4 不会校验流式 body，声明必须与实物一致。
    let (hashing, accumulator) = HashingStream::new(body.into_data_stream());
    store
        .put_stream(&object_key, size_bytes, &checksum, hashing)
        .await
        .map_err(|error| {
            tracing::warn!(%operation_id, error = %error, "force stop bundle upload failed");
            ApiError::bad_gateway("Session Bundle upload failed")
        })?;
    let (actual_sha256, actual_size) = accumulator.lock().unwrap().digest();
    if actual_sha256 != checksum || actual_size != size_bytes {
        let _ = store.delete(&object_key).await;
        return Err(ApiError::bad_request(
            "Session Bundle body does not match its declared checksum/size",
        ));
    }
    if actual_sha256 != checksum || actual_size != size_bytes {
        let _ = store.delete(&object_key).await;
        return Err(ApiError::bad_request(
            "Session Bundle body does not match its declared checksum/size",
        ));
    }

    // 二次事务：锁会话与 operation 重验证（pending + force_stopping + target owner），
    // bundle generation = 当前 + 1（不覆盖既有），失败则删除已上传对象。
    let commit_result = async {
        let mut tx = state.pool.begin().await?;
        let session_state: String = sqlx::query_scalar(
            "SELECT lifecycle_status FROM hub_sessions WHERE id = $1 FOR UPDATE",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?;
        if session_state != "force_stopping" {
            return Err(ApiError::conflict("session is not force stopping"));
        }
        let op_state: String = sqlx::query_scalar(
            "SELECT state FROM force_stop_operation WHERE operation_id = $1 FOR UPDATE",
        )
        .bind(operation_id)
        .fetch_one(&mut *tx)
        .await?;
        if op_state != "pending" {
            return Err(ApiError::conflict("force stop operation is not pending"));
        }
        // 会话行已在上面 FOR UPDATE 锁定；一并校验 target/owner/generation 未变。
        let holder: (
            Option<Uuid>,
            Option<Uuid>,
            i64,
            Option<i64>,
            Option<String>,
            String,
        ) = sqlx::query_as(
            "SELECT runtime_owner_id, current_bundle_runtime_id,
                        ownership_generation, current_bundle_generation,
                        current_bundle_object_key,
                        COALESCE(current_bundle_kind, 'checkpoint')
                 FROM hub_sessions WHERE id = $1",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?;
        // 旧 bundle 一律被新快照替换（用户定稿：新 bundle 上传成功即删旧 bundle）。
        let old_object_key = holder.4.clone();
        if holder.0 != Some(runtime_id) || holder.2 != session_ownership_generation {
            return Err(ApiError::conflict(
                "session ownership changed during upload",
            ));
        }
        let current_generation = holder.3.unwrap_or(0);
        let old_object_key = old_object_key.filter(|k| k != &object_key);
        if current_generation == i64::MAX {
            return Err(ApiError::internal("bundle generation overflow"));
        }
        let updated = sqlx::query(
            "UPDATE hub_sessions
             SET current_bundle_generation = $1,
                 current_bundle_object_key = $2,
                 current_bundle_checksum_sha256 = $3,
                 current_bundle_size_bytes = $4,
                 current_bundle_history_checkpoint = history_checkpoint,
                 current_bundle_ownership_generation = ownership_generation,
                 current_bundle_producing_engine_version = $5,
                 current_bundle_created_at = now(),
                 current_bundle_runtime_id = $6,
                 current_bundle_kind = 'force_stop',
                 current_bundle_checkpoint_attempt_id = NULL,
                 runtime_owner_id = NULL,
                 lifecycle_status = 'offline',
                 updated_at = now()
             WHERE id = $7",
        )
        .bind(current_generation + 1)
        .bind(&object_key)
        .bind(&checksum)
        .bind(size_bytes as i64)
        .bind(Option::<String>::None)
        .bind(runtime_id)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(ApiError::conflict("session state changed during upload"));
        }
        sqlx::query(
            "UPDATE force_stop_operation
             SET state = 'succeeded', snapshot_uploaded_at = now(), updated_at = now()
             WHERE operation_id = $1",
        )
        .bind(operation_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE runs SET status = 'interrupted', updated_at = now() WHERE id = $1")
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok::<((), Option<String>), ApiError>(((), old_object_key))
    }
    .await;
    let ((), old_object_key) = match commit_result {
        Ok(ok) => ok,
        Err(error) => {
            let _ = store.delete(&object_key).await;
            return Err(error);
        }
    };
    // 新快照已生效：删除被替换的旧 force-stop 快照对象（锁内捕获的旧 key，
    // 提交后按对象 key 前缀确认属 force-stop 再删；checkpoint 快照不动）。
    if let Some(old_key) = old_object_key {
        let _ = store.delete(&old_key).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn runtime_heartbeat(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RuntimeHeartbeatRequest>,
) -> Result<Json<RuntimeHeartbeatResponse>, ApiError> {
    let RuntimeHeartbeatRequest {
        pending_credential_hash,
        accepts_session_commands,
        owned_sessions,
        cleaned_sessions,
    } = req;
    let credential = bearer_token(&headers)
        .filter(|token| !token.trim().is_empty())
        .ok_or(ApiError::unauthorized("missing runtime credential"))?;
    let credential_hash = sha256_hex(&credential);
    if let Some(pending_hash) = pending_credential_hash.as_deref() {
        validate_sha256_hex(pending_hash)?;
    }
    let mut reported_session_ids = BTreeSet::new();
    for owned in &owned_sessions {
        validate_ownership_generation(owned.ownership_generation)?;
        if !matches!(
            owned.lifecycle_status.as_str(),
            "restoring" | "online" | "saving"
        ) {
            return Err(ApiError::bad_request(
                "invalid Runtime-owned Session lifecycle status",
            ));
        }
        match (
            owned.lifecycle_status.as_str(),
            owned.checkpoint_reason.as_deref(),
        ) {
            ("saving", Some(reason)) if checkpoint_reason_priority(reason).is_some() => {}
            ("saving", _) => {
                return Err(ApiError::bad_request(
                    "saving Runtime-owned Session requires a checkpoint reason",
                ));
            }
            (_, None) => {}
            _ => {
                return Err(ApiError::bad_request(
                    "checkpoint reason is only valid while saving",
                ));
            }
        }
        if !reported_session_ids.insert(owned.session_id) {
            return Err(ApiError::bad_request(
                "Runtime-owned Session state must be unique per heartbeat",
            ));
        }
    }
    let mut cleaned_session_generations = BTreeSet::new();
    for cleaned in &cleaned_sessions {
        validate_ownership_generation(cleaned.ownership_generation)?;
        if !cleaned_session_generations.insert((cleaned.session_id, cleaned.ownership_generation)) {
            return Err(ApiError::bad_request(
                "cleaned Session generation must be unique per heartbeat",
            ));
        }
    }

    let mut tx = state.pool.begin().await?;
    let row = sqlx::query(
        "SELECT id, token_hash, pending_token_hash, rotation_requested_at
         FROM runtimes
         WHERE credential_revoked_at IS NULL
           AND (token_hash = $1 OR pending_token_hash = $1)
         FOR UPDATE",
    )
    .bind(&credential_hash)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::unauthorized("invalid runtime credential"))?;
    let runtime_id: Uuid = row.get("id");
    let current_hash: String = row.get("token_hash");
    let existing_pending_hash: Option<String> = row.get("pending_token_hash");
    let rotation_requested_at: Option<DateTime<Utc>> = row.get("rotation_requested_at");
    let authenticated_pending = existing_pending_hash.as_deref() == Some(&credential_hash);

    for cleaned in cleaned_sessions {
        sqlx::query(
            "DELETE FROM runtime_session_cleanup_obligations
             WHERE runtime_id = $1 AND session_id = $2
               AND ownership_generation = $3",
        )
        .bind(runtime_id)
        .bind(cleaned.session_id)
        .bind(cleaned.ownership_generation)
        .execute(&mut *tx)
        .await?;
    }

    let mut credential_activated = false;
    let mut pending_credential_accepted = false;
    if authenticated_pending {
        sqlx::query(
            "UPDATE runtimes
             SET token_hash = $1, pending_token_hash = NULL,
                 pending_token_created_at = NULL, rotation_requested_at = NULL
             WHERE id = $2",
        )
        .bind(&credential_hash)
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
        credential_activated = true;
    } else {
        debug_assert_eq!(current_hash, credential_hash);
        if let Some(pending_hash) = pending_credential_hash {
            if rotation_requested_at.is_none() {
                return Err(ApiError::conflict(
                    "runtime credential rotation was not requested",
                ));
            }
            if existing_pending_hash
                .as_deref()
                .is_some_and(|existing| existing != pending_hash)
            {
                return Err(ApiError::conflict(
                    "a different runtime credential is already pending",
                ));
            }
            sqlx::query(
                "UPDATE runtimes
                 SET pending_token_hash = $1,
                     pending_token_created_at = COALESCE(pending_token_created_at, now())
                 WHERE id = $2",
            )
            .bind(pending_hash)
            .bind(runtime_id)
            .execute(&mut *tx)
            .await?;
            pending_credential_accepted = true;
        }
    }

    let runtime_status: String = sqlx::query_scalar(
        "UPDATE runtimes
         SET status = CASE WHEN status = 'draining' THEN status ELSE 'online' END,
             last_heartbeat_at = now()
         WHERE id = $1
         RETURNING status",
    )
    .bind(runtime_id)
    .fetch_one(&mut *tx)
    .await?;

    for owned in owned_sessions {
        let checkpoint_reason = if owned.lifecycle_status == "saving" {
            Some(if runtime_status == "draining" {
                "drain"
            } else {
                owned.checkpoint_reason.as_deref().unwrap()
            })
        } else {
            None
        };
        let updated = sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = CASE
                     WHEN $6 = 'draining'
                          AND lifecycle_status = 'saving'
                          AND saving_reason = 'drain'
                     THEN lifecycle_status
                     WHEN $1 = 'online' AND lifecycle_status = 'restoring'
                     THEN lifecycle_status
                     ELSE $1
                 END,
                 saving_history_checkpoint = CASE
                     WHEN $6 = 'draining'
                          AND lifecycle_status = 'saving'
                          AND saving_reason = 'drain'
                     THEN saving_history_checkpoint
                     WHEN $1 = 'saving' THEN COALESCE(
                         saving_history_checkpoint, history_checkpoint
                     )
                     ELSE NULL
                 END,
                 saving_ownership_generation = CASE
                     WHEN $6 = 'draining'
                          AND lifecycle_status = 'saving'
                          AND saving_reason = 'drain'
                     THEN saving_ownership_generation
                     WHEN $1 = 'saving' THEN COALESCE(
                         saving_ownership_generation, ownership_generation
                     )
                     ELSE NULL
                 END,
                 saving_reason = CASE
                     WHEN $6 = 'draining'
                          AND lifecycle_status = 'saving'
                          AND saving_reason = 'drain'
                     THEN saving_reason
                     WHEN $1 = 'saving' AND saving_reason = 'drain' THEN 'drain'
                     WHEN $1 = 'saving' THEN $5
                     ELSE NULL
                 END,
                 saving_checkpoint_attempt_id = CASE
                     WHEN $6 = 'draining'
                          AND lifecycle_status = 'saving'
                          AND saving_reason = 'drain'
                     THEN saving_checkpoint_attempt_id
                     WHEN $1 = 'saving' THEN COALESCE(
                         saving_checkpoint_attempt_id, gen_random_uuid()
                     )
                     ELSE NULL
                 END,
                 last_checkpoint_attempt_id = CASE
                     WHEN $1 = 'saving' AND lifecycle_status <> 'saving' THEN NULL
                     ELSE last_checkpoint_attempt_id
                 END,
                 last_checkpoint_ownership_generation = CASE
                     WHEN $1 = 'saving' AND lifecycle_status <> 'saving' THEN NULL
                     ELSE last_checkpoint_ownership_generation
                 END,
                 last_checkpoint_disposition = CASE
                     WHEN $1 = 'saving' AND lifecycle_status <> 'saving' THEN NULL
                     ELSE last_checkpoint_disposition
                 END,
                 last_checkpoint_has_queued_work = CASE
                     WHEN $1 = 'saving' AND lifecycle_status <> 'saving' THEN NULL
                     ELSE last_checkpoint_has_queued_work
                 END
             WHERE id = $2 AND runtime_owner_id = $3
               AND ownership_generation = $4
               AND (
                   $1 <> 'saving'
                   OR lifecycle_status = 'saving'
                   OR (
                       lifecycle_status = 'online'
                       AND active_turn_id IS NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM runs AS active_runs
                           WHERE active_runs.hub_session_id = hub_sessions.id
                             AND active_runs.status IN ('running', 'waiting_tool')
                       )
                   )
               )",
        )
        .bind(&owned.lifecycle_status)
        .bind(owned.session_id)
        .bind(runtime_id)
        .bind(owned.ownership_generation)
        .bind(checkpoint_reason)
        .bind(&runtime_status)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            let still_owned: Option<(Option<Uuid>, i64)> = sqlx::query_as(
                "SELECT runtime_owner_id, ownership_generation
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(owned.session_id)
            .fetch_optional(&mut *tx)
            .await?;
            let owner_matches = still_owned.is_some_and(|(owner, generation)| {
                owner == Some(runtime_id) && generation == owned.ownership_generation
            });
            if !owner_matches {
                // This Runtime no longer owns that generation (released,
                // reclaimed, or fenced). Do not fail the whole heartbeat:
                // the owned-session snapshot below reflects Hub truth and
                // lets the Runtime reconcile and drop its stale local copy.
                continue;
            }
            let hub_fenced: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                     SELECT 1 FROM hub_sessions
                     WHERE id = $1
                       AND lifecycle_status IN ('historical', 'recovery_failed')
                       AND runtime_owner_id IS NULL
                       AND ownership_generation > $2
                 )",
            )
            .bind(owned.session_id)
            .bind(owned.ownership_generation)
            .fetch_one(&mut *tx)
            .await?;
            if !hub_fenced {
                return Err(ApiError::conflict(
                    "Runtime-owned Session state has a stale owner or generation",
                )
                .with_code("stale_session_generation"));
            }
        }
    }

    let reported_session_ids = reported_session_ids.into_iter().collect::<Vec<_>>();
    sqlx::query(
        "UPDATE hub_sessions
         SET runtime_owner_id = NULL,
             lifecycle_status = 'offline',
             recovery_source = NULL,
             active_turn_id = NULL,
             ownership_generation = ownership_generation + 1
         WHERE runtime_owner_id = $1
           AND NOT (id = ANY($2))
           AND lifecycle_status IN ('restoring', 'online')
           AND active_turn_id IS NULL
           AND NOT EXISTS (
               SELECT 1 FROM runs
               WHERE runs.hub_session_id = hub_sessions.id
                 AND runs.status IN ('running', 'waiting_tool')
           )",
    )
    .bind(runtime_id)
    .bind(&reported_session_ids)
    .execute(&mut *tx)
    .await?;

    let owned_sessions = sqlx::query(
        "SELECT sessions.id, sessions.ownership_generation,
                sessions.lifecycle_status, sessions.native_session_id,
                active_runs.id AS active_run_id
         FROM hub_sessions AS sessions
         LEFT JOIN LATERAL (
             SELECT runs.id
             FROM runs
             WHERE runs.hub_session_id = sessions.id
               AND runs.runtime_id = $1
               AND runs.status IN ('running', 'waiting_tool')
             ORDER BY runs.updated_at DESC, runs.id DESC
             LIMIT 1
         ) AS active_runs ON true
         WHERE sessions.runtime_owner_id = $1
         ORDER BY sessions.created_at, sessions.id",
    )
    .bind(runtime_id)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| RuntimeOwnedSessionSnapshotDto {
        session_id: row.get("id"),
        ownership_generation: row.get("ownership_generation"),
        lifecycle_status: row.get("lifecycle_status"),
        native_session_id: row.get("native_session_id"),
        active_run_id: row.get("active_run_id"),
    })
    .collect();

    let cleanup_sessions = sqlx::query(
        "SELECT session_id, ownership_generation
         FROM runtime_session_cleanup_obligations
         WHERE runtime_id = $1
         ORDER BY created_at, session_id, ownership_generation",
    )
    .bind(runtime_id)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| RuntimeOwnedSessionGenerationDto {
        session_id: row.get("session_id"),
        ownership_generation: row.get("ownership_generation"),
    })
    .collect();

    let mut session_commands = Vec::new();
    let mut interrupt_commands = Vec::new();
    if accepts_session_commands {
        interrupt_commands.extend(
            sqlx::query(
                "SELECT turns.id AS command_id, sessions.id AS session_id,
                        sessions.ownership_generation, runs.id AS run_id,
                        turns.id AS turn_id, sessions.native_session_id,
                        turns.native_turn_id
                 FROM hub_sessions AS sessions
                 JOIN hub_session_turns AS turns
                   ON turns.id = sessions.active_turn_id
                  AND turns.session_id = sessions.id
                 JOIN LATERAL (
                     SELECT active_runs.id
                     FROM runs AS active_runs
                     WHERE active_runs.hub_session_id = sessions.id
                       AND active_runs.hub_turn_id = turns.id
                       AND active_runs.status IN ('running', 'waiting_tool')
                     ORDER BY active_runs.updated_at DESC, active_runs.id DESC
                     LIMIT 1
                 ) AS runs ON true
                 WHERE sessions.runtime_owner_id = $1
                   AND turns.interrupt_requested_at IS NOT NULL
                   AND turns.interrupt_acknowledged_at IS NULL
                   AND sessions.native_session_id IS NOT NULL
                   AND turns.native_turn_id IS NOT NULL
                 ORDER BY sessions.created_at, sessions.id",
            )
            .bind(runtime_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| RuntimeSessionCommandDto {
                command_id: row.get("command_id"),
                session_id: row.get("session_id"),
                ownership_generation: row.get("ownership_generation"),
                command: "interrupt".into(),
                run_id: Some(row.get("run_id")),
                turn_id: Some(row.get("turn_id")),
                native_session_id: row.get("native_session_id"),
                native_turn_id: row.get("native_turn_id"),
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }),
        );
    }
    if accepts_session_commands && runtime_status == "draining" {
        session_commands.extend(
            sqlx::query(
                "SELECT id, ownership_generation
             FROM hub_sessions
             WHERE runtime_owner_id = $1
               AND NOT EXISTS (
                   SELECT 1 FROM hub_session_turns AS turns
                   WHERE turns.id = hub_sessions.active_turn_id
                     AND turns.session_id = hub_sessions.id
                     AND turns.interrupt_requested_at IS NOT NULL
                     AND turns.interrupt_acknowledged_at IS NULL
               )
             ORDER BY created_at, id",
            )
            .bind(runtime_id)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| RuntimeSessionCommandDto {
                command_id: row.get("id"),
                session_id: row.get("id"),
                ownership_generation: row.get("ownership_generation"),
                command: "checkpoint".into(),
                run_id: None,
                turn_id: None,
                native_session_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            })
            .collect::<Vec<_>>(),
        );
    }
    if accepts_session_commands {
        sqlx::query(
            "UPDATE hub_session_messages AS messages
             SET delivery_state = 'delivering'
             FROM hub_sessions AS sessions, hub_session_turns AS turns
             WHERE messages.session_id = sessions.id
               AND turns.id = sessions.active_turn_id
               AND turns.session_id = sessions.id
               AND turns.interrupt_requested_at IS NULL
               AND sessions.runtime_owner_id = $1
               AND messages.turn_id = turns.id
               AND messages.delivery_mode = 'steer'
               AND messages.delivery_state = 'queued'
               AND messages.expected_native_turn_id = turns.native_turn_id",
        )
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
        let mut steer_commands = sqlx::query(
            "SELECT messages.id AS command_id, messages.session_id,
                        sessions.ownership_generation, messages.run_id,
                        turns.id AS turn_id, sessions.native_session_id,
                        turns.native_turn_id, messages.sequence, messages.content
                 FROM hub_session_messages AS messages
                 JOIN hub_sessions AS sessions ON sessions.id = messages.session_id
                 JOIN hub_session_turns AS turns
                   ON turns.id = messages.turn_id
                  AND turns.session_id = messages.session_id
                 WHERE sessions.runtime_owner_id = $1
                   AND sessions.active_turn_id = turns.id
                   AND messages.delivery_mode = 'steer'
                   AND messages.delivery_state = 'delivering'
                   AND messages.expected_native_turn_id = turns.native_turn_id
                   AND messages.run_id IS NOT NULL
                   AND messages.content IS NOT NULL
                 ORDER BY sessions.created_at, messages.sequence, messages.id",
        )
        .bind(runtime_id)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .map(|row| {
            let command_id = row.get("command_id");
            RuntimeSessionCommandDto {
                command_id,
                session_id: row.get("session_id"),
                ownership_generation: row.get("ownership_generation"),
                command: "steer".into(),
                run_id: row.get("run_id"),
                turn_id: Some(row.get("turn_id")),
                native_session_id: row.get("native_session_id"),
                native_turn_id: row.get("native_turn_id"),
                message: Some(RuntimeSteeringMessageDto {
                    id: command_id,
                    sequence: row.get("sequence"),
                    content: row.get("content"),
                    attachments: Vec::new(),
                }),
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }
        })
        .collect::<Vec<_>>();
        for command in &mut steer_commands {
            if let Some(message) = &mut command.message {
                message.attachments =
                    load_attachments_for_session_messages(&mut *tx, &[message.id])
                        .await?
                        .remove(&message.id)
                        .unwrap_or_default();
            }
        }
        session_commands.extend(steer_commands);
    }
    if accepts_session_commands {
        let refresh_rows = sqlx::query(
            "SELECT id, agent_id, ownership_generation, native_session_id,
                    configuration_refresh_revision
             FROM hub_sessions
             WHERE runtime_owner_id = $1
               AND lifecycle_status IN ('restoring', 'online')
               AND configuration_refresh_revision > configuration_applied_revision
             ORDER BY created_at, id",
        )
        .bind(runtime_id)
        .fetch_all(&mut *tx)
        .await?;
        for row in refresh_rows {
            let session_id: Uuid = row.get("id");
            let target_revision: i64 = row.get("configuration_refresh_revision");
            let mut execution_configuration =
                load_agent_execution_configuration_tx(&mut tx, row.get("agent_id")).await?;
            apply_session_tool_policy_to_configuration_tx(
                &mut tx,
                session_id,
                &mut execution_configuration,
            )
            .await?;
            if execution_configuration.revision != target_revision {
                continue;
            }
            let fingerprint = execution_configuration_fingerprint(&execution_configuration)
                .map_err(|error| ApiError::internal(error.to_string()))?;
            session_commands.push(RuntimeSessionCommandDto {
                command_id: configuration_command_id(session_id, target_revision),
                session_id,
                ownership_generation: row.get("ownership_generation"),
                command: "refresh_configuration".into(),
                run_id: None,
                turn_id: None,
                native_session_id: row.get("native_session_id"),
                native_turn_id: None,
                message: None,
                configuration_revision: Some(target_revision),
                fingerprint: Some(fingerprint),
                execution_configuration: Some(execution_configuration),
            });
        }
    }
    session_commands.extend(interrupt_commands);
    let salvage_sessions = sqlx::query(
        "UPDATE runtime_session_salvage_obligations
         SET attempts = attempts + 1,
             next_attempt_at = now() + interval '30 seconds',
             updated_at = now()
         WHERE (runtime_id, session_id, ownership_generation) IN (
             SELECT runtime_id, session_id, ownership_generation
             FROM runtime_session_salvage_obligations
             WHERE runtime_id = $1 AND next_attempt_at <= now()
             ORDER BY created_at, session_id
             LIMIT 50
         )
         RETURNING session_id, ownership_generation, history_checkpoint, bundle_generation",
    )
    .bind(runtime_id)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|row| RuntimeSalvageSessionDto {
        session_id: row.get("session_id"),
        ownership_generation: row.get("ownership_generation"),
        history_checkpoint: row.get("history_checkpoint"),
        bundle_generation: row.get("bundle_generation"),
    })
    .collect();
    tx.commit().await?;
    Ok(Json(RuntimeHeartbeatResponse {
        rotation_requested: rotation_requested_at.is_some() && !credential_activated,
        pending_credential_accepted,
        credential_activated,
        runtime_status,
        owned_sessions,
        cleanup_sessions,
        salvage_sessions,
        session_commands,
    }))
}

pub(crate) fn configuration_command_id(session_id: Uuid, revision: i64) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(session_id.as_bytes());
    hasher.update(revision.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

pub(crate) async fn runtime_release_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<ReleaseRuntimeSessionRequest>,
) -> Result<Json<HubSessionDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    release_session_ownership_tx(
        &mut tx,
        runtime_id,
        session_id,
        req.ownership_generation,
        req.force,
    )
    .await?;
    let released = load_hub_session_tx(&mut tx, session_id).await?;
    tx.commit().await?;
    Ok(Json(released))
}

pub(crate) async fn release_session_ownership_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
    session_id: Uuid,
    ownership_generation: i64,
    force: bool,
) -> Result<(), ApiError> {
    sqlx::query("SELECT id FROM runtimes WHERE id = $1 FOR UPDATE")
        .bind(runtime_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::unauthorized(
            "runtime is not active or its credential is invalid",
        ))?;
    let session = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status, active_turn_id,
                saving_history_checkpoint, saving_ownership_generation,
                saving_checkpoint_attempt_id, current_bundle_history_checkpoint,
                current_bundle_ownership_generation,
                current_bundle_checkpoint_attempt_id, current_bundle_runtime_id
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || session.get::<i64, _>("ownership_generation") != ownership_generation
    {
        // Idempotent acknowledgement: ownership has already moved past this
        // generation (released, re-claimed, or deleted), so the retried
        // release request is already satisfied and must not disturb the
        // current owner.
        return Ok(());
    }
    if session.get::<Option<Uuid>, _>("active_turn_id").is_some() {
        return Err(ApiError::conflict(
            "active Session Turn must finish before ownership release",
        ));
    }
    let unfinished_execution: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs
             WHERE hub_session_id = $1 AND status IN ('running', 'waiting_tool')
         )",
    )
    .bind(session_id)
    .fetch_one(&mut **tx)
    .await?;
    if unfinished_execution {
        return Err(ApiError::conflict(
            "unfinished Session execution must finish before ownership release",
        ));
    }
    if !force {
        let bundle_checkpoint: Option<i64> = session.get("current_bundle_history_checkpoint");
        let bundle_ownership_generation: Option<i64> =
            session.get("current_bundle_ownership_generation");
        let lifecycle_status: String = session.get("lifecycle_status");
        let release_state_is_current = match lifecycle_status.as_str() {
            "online" => true,
            "saving" => {
                session.get::<Option<i64>, _>("saving_history_checkpoint") == bundle_checkpoint
                    && session.get::<Option<i64>, _>("saving_ownership_generation")
                        == Some(ownership_generation)
                    && session
                        .get::<Option<Uuid>, _>("saving_checkpoint_attempt_id")
                        .is_some()
                    && session.get::<Option<Uuid>, _>("current_bundle_checkpoint_attempt_id")
                        == session.get::<Option<Uuid>, _>("saving_checkpoint_attempt_id")
            }
            _ => false,
        };
        if bundle_checkpoint.is_none()
            || bundle_ownership_generation != Some(ownership_generation)
            || session.get::<Option<Uuid>, _>("current_bundle_runtime_id") != Some(runtime_id)
            || !release_state_is_current
        {
            return Err(ApiError::conflict(
                "current Session Bundle does not cover this ownership state and generation",
            ));
        }
        let has_unreplayable_history: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM hub_session_messages
                 WHERE session_id = $1
                   AND sequence > $2
                   AND delivery_state IN ('delivering', 'delivered')
             )",
        )
        .bind(session_id)
        .bind(bundle_checkpoint.unwrap())
        .fetch_one(&mut **tx)
        .await?;
        if has_unreplayable_history {
            return Err(ApiError::conflict(
                "current Session Bundle is older than unreplayable Hub history",
            ));
        }
    }

    record_runtime_session_cleanup_tx(tx, runtime_id, session_id, ownership_generation, None)
        .await?;
    let release_update = if force {
        // force 释放：放弃 bundle 归档（清空全部归档字段含 checkpoint），恢复走消息表全量重放；
        // checkpoint NULL 时 unreplayable 检查自动放行，避免后续 claim 被永久阻塞。
        "UPDATE hub_sessions
         SET runtime_owner_id = NULL,
             lifecycle_status = CASE
                 WHEN EXISTS (
                     SELECT 1 FROM runs
                     WHERE hub_session_id = $1 AND status = 'pending'
                 ) OR EXISTS (
                     SELECT 1 FROM hub_session_messages
                     WHERE session_id = $1 AND delivery_state = 'queued'
                 ) THEN 'waiting_for_runtime'
                 ELSE 'offline'
             END,
             ownership_generation = ownership_generation + $4,
             saving_history_checkpoint = NULL,
             saving_ownership_generation = NULL,
             saving_reason = NULL,
             saving_checkpoint_attempt_id = NULL,
             current_bundle_generation = NULL,
             current_bundle_object_key = NULL,
             current_bundle_checksum_sha256 = NULL,
             current_bundle_size_bytes = NULL,
             current_bundle_history_checkpoint = NULL,
             current_bundle_ownership_generation = NULL,
             current_bundle_producing_engine_version = NULL,
             current_bundle_kind = NULL,
             current_bundle_created_at = NULL,
             current_bundle_runtime_id = NULL,
             recovery_error = NULL
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3"
    } else {
        // 正常释放：非 force 前已校验 bundle 追平（无 unreplayable 历史），
        // 保留 bundle 归档供下次恢复快速加载；仅清理进行中的保存状态。
        "UPDATE hub_sessions
         SET runtime_owner_id = NULL,
             lifecycle_status = CASE
                 WHEN EXISTS (
                     SELECT 1 FROM runs
                     WHERE hub_session_id = $1 AND status = 'pending'
                 ) OR EXISTS (
                     SELECT 1 FROM hub_session_messages
                     WHERE session_id = $1 AND delivery_state = 'queued'
                 ) THEN 'waiting_for_runtime'
                 ELSE 'offline'
             END,
             ownership_generation = ownership_generation + $4,
             saving_history_checkpoint = NULL,
             saving_ownership_generation = NULL,
             saving_reason = NULL,
             saving_checkpoint_attempt_id = NULL
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3"
    };
    sqlx::query(release_update)
        .bind(session_id)
        .bind(runtime_id)
        .bind(ownership_generation)
        .bind(if force { 1 } else { 0 })
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub(crate) fn checkpoint_reason_priority(reason: &str) -> Option<u8> {
    match reason {
        "idle" => Some(0),
        "drain" => Some(1),
        _ => None,
    }
}

pub(crate) async fn runtime_begin_session_checkpoint(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<BeginRuntimeSessionCheckpointRequest>,
) -> Result<Json<RuntimeSessionCheckpointAttemptDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    checkpoint_reason_priority(&req.reason)
        .ok_or(ApiError::bad_request("invalid Session checkpoint reason"))?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let runtime_status: String =
        sqlx::query_scalar("SELECT status FROM runtimes WHERE id = $1 FOR UPDATE")
            .bind(runtime_id)
            .fetch_one(&mut *tx)
            .await?;
    let effective_reason = if runtime_status == "draining" {
        "drain".to_owned()
    } else {
        req.reason
    };
    let requested_priority = checkpoint_reason_priority(&effective_reason).unwrap();
    let session = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                active_turn_id, history_checkpoint, saving_history_checkpoint,
                saving_ownership_generation, saving_reason,
                saving_checkpoint_attempt_id, current_bundle_generation
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || session.get::<i64, _>("ownership_generation") != req.ownership_generation
    {
        return Err(
            ApiError::forbidden("runtime does not own this Session generation")
                .with_code("stale_session_generation"),
        );
    }
    if session.get::<Option<Uuid>, _>("active_turn_id").is_some() {
        return Err(ApiError::conflict(
            "active Session Turn must finish before checkpoint",
        ));
    }
    let unfinished_execution: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs
             WHERE hub_session_id = $1 AND status IN ('running', 'waiting_tool')
         )",
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    if unfinished_execution {
        return Err(ApiError::conflict(
            "unfinished Session execution must finish before checkpoint",
        ));
    }
    let bundle_generation = session
        .get::<Option<i64>, _>("current_bundle_generation")
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(ApiError::conflict("Session Bundle generation overflowed"))?;

    let response = if session.get::<String, _>("lifecycle_status") == "saving" {
        if session.get::<Option<i64>, _>("saving_ownership_generation")
            != Some(req.ownership_generation)
        {
            return Err(ApiError::conflict(
                "Session checkpoint belongs to a stale ownership generation",
            ));
        }
        let current_reason: String = session.get("saving_reason");
        let reason = if requested_priority
            > checkpoint_reason_priority(&current_reason).unwrap_or_default()
        {
            sqlx::query(
                "UPDATE hub_sessions
                 SET saving_reason = $1
                 WHERE id = $2",
            )
            .bind(&effective_reason)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
            effective_reason.clone()
        } else {
            current_reason
        };
        RuntimeSessionCheckpointAttemptDto {
            checkpoint_attempt_id: session.get("saving_checkpoint_attempt_id"),
            history_checkpoint: session.get("saving_history_checkpoint"),
            bundle_generation,
            reason,
        }
    } else {
        if session.get::<String, _>("lifecycle_status") != "online" {
            return Err(ApiError::conflict(
                "Session must be online or saving to begin checkpoint",
            ));
        }
        let checkpoint_attempt_id = Uuid::new_v4();
        let history_checkpoint: i64 = session.get("history_checkpoint");
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'saving',
                 saving_history_checkpoint = $1,
                 saving_ownership_generation = $2,
                 saving_reason = $3,
                 saving_checkpoint_attempt_id = $4,
                 last_checkpoint_attempt_id = NULL,
                 last_checkpoint_ownership_generation = NULL,
                 last_checkpoint_disposition = NULL,
                 last_checkpoint_has_queued_work = NULL
             WHERE id = $5",
        )
        .bind(history_checkpoint)
        .bind(req.ownership_generation)
        .bind(&effective_reason)
        .bind(checkpoint_attempt_id)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
        RuntimeSessionCheckpointAttemptDto {
            checkpoint_attempt_id,
            history_checkpoint,
            bundle_generation,
            reason: effective_reason,
        }
    };
    tx.commit().await?;
    Ok(Json(response))
}

pub(crate) async fn runtime_fail_session_checkpoint(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<FailRuntimeSessionCheckpointRequest>,
) -> Result<Json<RuntimeSessionCheckpointDispositionDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    if req.error.trim().is_empty() {
        return Err(ApiError::bad_request(
            "Session checkpoint failure requires an error code",
        ));
    }
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let runtime_status: String =
        sqlx::query_scalar("SELECT status FROM runtimes WHERE id = $1 FOR UPDATE")
            .bind(runtime_id)
            .fetch_one(&mut *tx)
            .await?;
    let session = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                saving_history_checkpoint, saving_ownership_generation,
                saving_reason, saving_checkpoint_attempt_id,
                last_checkpoint_attempt_id, last_checkpoint_ownership_generation,
                last_checkpoint_disposition, last_checkpoint_has_queued_work
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || session.get::<i64, _>("ownership_generation") != req.ownership_generation
    {
        return Err(
            ApiError::forbidden("runtime does not own this Session generation")
                .with_code("stale_session_generation"),
        );
    }
    if session.get::<Option<Uuid>, _>("last_checkpoint_attempt_id")
        == Some(req.checkpoint_attempt_id)
        && session.get::<Option<i64>, _>("last_checkpoint_ownership_generation")
            == Some(req.ownership_generation)
    {
        let response = RuntimeSessionCheckpointDispositionDto {
            checkpoint_attempt_id: req.checkpoint_attempt_id,
            disposition: session.get("last_checkpoint_disposition"),
            has_queued_work: session.get("last_checkpoint_has_queued_work"),
        };
        tx.commit().await?;
        return Ok(Json(response));
    }
    if session.get::<String, _>("lifecycle_status") != "saving"
        || session.get::<Option<i64>, _>("saving_ownership_generation")
            != Some(req.ownership_generation)
        || session.get::<Option<Uuid>, _>("saving_checkpoint_attempt_id")
            != Some(req.checkpoint_attempt_id)
    {
        return Err(ApiError::conflict(
            "Session checkpoint result belongs to a stale attempt",
        ));
    }
    let saving_history_checkpoint: i64 = session.get("saving_history_checkpoint");
    let has_queued_work: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs
             WHERE hub_session_id = $1 AND status = 'pending'
         ) OR EXISTS(
             SELECT 1 FROM hub_session_messages
             WHERE session_id = $1
               AND sequence > $2
               AND delivery_state = 'queued'
         )",
    )
    .bind(session_id)
    .bind(saving_history_checkpoint)
    .fetch_one(&mut *tx)
    .await?;
    let reason: String = session.get("saving_reason");
    if runtime_status == "draining" && reason != "drain" {
        sqlx::query(
            "UPDATE hub_sessions
             SET saving_reason = 'drain'
             WHERE id = $1",
        )
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
    }
    let disposition = if has_queued_work && runtime_status != "draining" {
        "resume"
    } else {
        "retry"
    };
    if disposition == "resume" {
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online',
                 saving_history_checkpoint = NULL,
                 saving_ownership_generation = NULL,
                 saving_reason = NULL,
                 saving_checkpoint_attempt_id = NULL,
                 last_checkpoint_attempt_id = $1,
                 last_checkpoint_ownership_generation = $2,
                 last_checkpoint_disposition = 'resume',
                 last_checkpoint_has_queued_work = $3
             WHERE id = $4",
        )
        .bind(req.checkpoint_attempt_id)
        .bind(req.ownership_generation)
        .bind(has_queued_work)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
    }
    let response = RuntimeSessionCheckpointDispositionDto {
        checkpoint_attempt_id: req.checkpoint_attempt_id,
        disposition: disposition.into(),
        has_queued_work,
    };
    tx.commit().await?;
    Ok(Json(response))
}

#[derive(Debug, Clone)]
struct SessionBundleUploadHeaders {
    ownership_generation: i64,
    checkpoint_attempt_id: Uuid,
    bundle_generation: i64,
    checksum_sha256: String,
    size_bytes: u64,
    history_checkpoint: i64,
    producing_engine_version: String,
    created_at: DateTime<Utc>,
}

pub(crate) async fn runtime_upload_session_bundle(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<RuntimeSessionBundleCommitResponseDto>, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let metadata = parse_session_bundle_upload_headers(&headers, state.session_bundle_max_bytes)?;
    let store =
        state
            .session_bundle_store
            .as_ref()
            .cloned()
            .ok_or(ApiError::service_unavailable(
                "Session Bundle object storage is not configured",
            ))?;
    let object_key = session_bundle_object_key(
        session_id,
        metadata.bundle_generation,
        metadata.checkpoint_attempt_id,
    );
    let replay = validate_session_bundle_upload_preflight(
        &state.pool,
        runtime_id,
        session_id,
        &object_key,
        &metadata,
    )
    .await?;
    if replay {
        let (response, _, _) = commit_and_finalize_session_bundle(
            &state.pool,
            runtime_id,
            session_id,
            &object_key,
            &metadata,
        )
        .await?;
        return Ok(Json(response));
    }

    let observed = Arc::new(AtomicU64::new(0));
    let observed_stream = Arc::clone(&observed);
    let declared_size = metadata.size_bytes;
    let stream = body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(|error| std::io::Error::other(error.to_string()))?;
        let previous = observed_stream.fetch_add(chunk.len() as u64, Ordering::AcqRel);
        let total = previous.saturating_add(chunk.len() as u64);
        if total > declared_size {
            return Err(std::io::Error::other(
                "Session Bundle body exceeds its declared size",
            ));
        }
        Ok(chunk)
    });
    if let Err(error) = store
        .put_stream(
            &object_key,
            metadata.size_bytes,
            &metadata.checksum_sha256,
            stream,
        )
        .await
    {
        let _ = store.delete(&object_key).await;
        warn!(session_id = %session_id, error = %error, "Session Bundle object upload failed");
        return Err(ApiError::bad_gateway("Session Bundle object upload failed"));
    }
    if observed.load(Ordering::Acquire) != metadata.size_bytes {
        let _ = store.delete(&object_key).await;
        return Err(ApiError::bad_request(
            "Session Bundle body size does not match its declaration",
        ));
    }

    let commit = commit_and_finalize_session_bundle(
        &state.pool,
        runtime_id,
        session_id,
        &object_key,
        &metadata,
    )
    .await;
    let (response, old_object_key, replayed_commit) = match commit {
        Ok(value) => value,
        Err(error) => {
            if !replay {
                let _ = store.delete(&object_key).await;
            }
            return Err(error);
        }
    };
    if !replayed_commit {
        if let Some(old_object_key) = old_object_key.filter(|old| old != &object_key) {
            if let Err(error) = store.delete(&old_object_key).await {
                warn!(
                    session_id = %session_id,
                    object_key = %old_object_key,
                    error = %error,
                    "failed to delete replaced Session Bundle object"
                );
            }
        }
    }
    Ok(Json(response))
}

pub(crate) async fn runtime_salvage_session_bundle(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<RuntimeSessionBundleCommitResponseDto>, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let metadata = parse_session_bundle_upload_headers(&headers, state.session_bundle_max_bytes)?;
    let current = sqlx::query(
        "SELECT current_bundle_generation, current_bundle_checksum_sha256
         FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(&state.pool)
    .await?;
    if current.as_ref().is_some_and(|row| {
        row.get::<Option<i64>, _>("current_bundle_generation") == Some(metadata.bundle_generation)
            && row
                .get::<Option<String>, _>("current_bundle_checksum_sha256")
                .as_deref()
                == Some(metadata.checksum_sha256.as_str())
    }) {
        let mut tx = state.pool.begin().await?;
        sqlx::query(
            "DELETE FROM runtime_session_salvage_obligations
             WHERE runtime_id = $1 AND session_id = $2 AND ownership_generation = $3",
        )
        .bind(runtime_id)
        .bind(session_id)
        .bind(metadata.ownership_generation)
        .execute(&mut *tx)
        .await?;
        let has_queued_work =
            session_has_queued_work_tx(&mut tx, session_id, metadata.history_checkpoint).await?;
        tx.commit().await?;
        return Ok(Json(RuntimeSessionBundleCommitResponseDto {
            checkpoint_attempt_id: metadata.checkpoint_attempt_id,
            bundle_generation: metadata.bundle_generation,
            has_queued_work,
            ownership_released: true,
        }));
    }
    let obligation = sqlx::query(
        "SELECT history_checkpoint, bundle_generation
         FROM runtime_session_salvage_obligations
         WHERE runtime_id = $1 AND session_id = $2 AND ownership_generation = $3",
    )
    .bind(runtime_id)
    .bind(session_id)
    .bind(metadata.ownership_generation)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::conflict("no salvage obligation"))?;
    if metadata.bundle_generation != obligation.get::<i64, _>("bundle_generation")
        || metadata.history_checkpoint < obligation.get::<i64, _>("history_checkpoint")
    {
        return Err(ApiError::conflict(
            "Session Bundle does not match the salvage obligation",
        ));
    }
    let store =
        state
            .session_bundle_store
            .as_ref()
            .cloned()
            .ok_or(ApiError::service_unavailable(
                "Session Bundle object storage is not configured",
            ))?;
    let object_key = session_bundle_object_key(
        session_id,
        metadata.bundle_generation,
        metadata.checkpoint_attempt_id,
    );
    let observed = Arc::new(AtomicU64::new(0));
    let observed_stream = Arc::clone(&observed);
    let declared_size = metadata.size_bytes;
    let stream = body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(|error| std::io::Error::other(error.to_string()))?;
        let previous = observed_stream.fetch_add(chunk.len() as u64, Ordering::AcqRel);
        let total = previous.saturating_add(chunk.len() as u64);
        if total > declared_size {
            return Err(std::io::Error::other(
                "Session Bundle body exceeds its declared size",
            ));
        }
        Ok(chunk)
    });
    if let Err(error) = store
        .put_stream(
            &object_key,
            metadata.size_bytes,
            &metadata.checksum_sha256,
            stream,
        )
        .await
    {
        let _ = store.delete(&object_key).await;
        warn!(session_id = %session_id, error = %error, "Session Bundle object upload failed");
        return Err(ApiError::bad_gateway("Session Bundle object upload failed"));
    }
    if observed.load(Ordering::Acquire) != metadata.size_bytes {
        let _ = store.delete(&object_key).await;
        return Err(ApiError::bad_request(
            "Session Bundle body size does not match its declaration",
        ));
    }

    let mut tx = state.pool.begin().await?;
    let updated = sqlx::query(
        "UPDATE hub_sessions
         SET current_bundle_generation = $1,
             current_bundle_object_key = $2,
             current_bundle_checksum_sha256 = $3,
             current_bundle_size_bytes = $4,
             current_bundle_history_checkpoint = $5,
             current_bundle_ownership_generation = $6,
             current_bundle_producing_engine_version = $7,
             current_bundle_created_at = $8,
             current_bundle_runtime_id = $9,
             current_bundle_kind = 'checkpoint',
             history_checkpoint = GREATEST(history_checkpoint, $5),
             recovery_error = NULL
         WHERE id = $10",
    )
    .bind(metadata.bundle_generation)
    .bind(&object_key)
    .bind(&metadata.checksum_sha256)
    .bind(metadata.size_bytes as i64)
    .bind(metadata.history_checkpoint)
    .bind(metadata.ownership_generation)
    .bind(&metadata.producing_engine_version)
    .bind(metadata.created_at)
    .bind(runtime_id)
    .bind(session_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        let _ = store.delete(&object_key).await;
        return Err(ApiError::conflict(
            "salvage Session no longer accepts this Bundle",
        ));
    }
    let has_queued_work =
        session_has_queued_work_tx(&mut tx, session_id, metadata.history_checkpoint).await?;
    sqlx::query(
        "DELETE FROM runtime_session_salvage_obligations
         WHERE runtime_id = $1 AND session_id = $2 AND ownership_generation = $3",
    )
    .bind(runtime_id)
    .bind(session_id)
    .bind(metadata.ownership_generation)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(RuntimeSessionBundleCommitResponseDto {
        checkpoint_attempt_id: metadata.checkpoint_attempt_id,
        bundle_generation: metadata.bundle_generation,
        has_queued_work,
        ownership_released: true,
    }))
}

pub(crate) async fn runtime_abandon_session_salvage(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<AbandonRuntimeSalvageRequest>,
) -> Result<StatusCode, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    let runtime_id = require_runtime(&state, &headers).await?;
    sqlx::query(
        "DELETE FROM runtime_session_salvage_obligations
         WHERE runtime_id = $1 AND session_id = $2 AND ownership_generation = $3",
    )
    .bind(runtime_id)
    .bind(session_id)
    .bind(req.ownership_generation)
    .execute(&state.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn runtime_download_session_bundle(
    State(state): State<Arc<AppState>>,
    Path(session_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let ownership_generation =
        parse_required_header::<i64>(&headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    let row = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                current_bundle_generation, current_bundle_object_key,
                current_bundle_checksum_sha256, current_bundle_size_bytes,
                current_bundle_history_checkpoint,
                current_bundle_producing_engine_version, current_bundle_created_at,
                current_bundle_kind
         FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if row.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || row.get::<i64, _>("ownership_generation") != ownership_generation
        || row.get::<String, _>("lifecycle_status") != "restoring"
    {
        return Err(ApiError::forbidden(
            "runtime does not own this restoring Session generation",
        ));
    }
    let object_key: Option<String> = row.get("current_bundle_object_key");
    let object_key = object_key.ok_or(ApiError::not_found(
        "restoring Session has no current Bundle",
    ))?;
    let store =
        state
            .session_bundle_store
            .as_ref()
            .cloned()
            .ok_or(ApiError::service_unavailable(
                "Session Bundle object storage is not configured",
            ))?;
    let upstream = store.get(&object_key).await.map_err(|error| {
        warn!(session_id = %session_id, error = %error, "Session Bundle object download failed");
        ApiError::bad_gateway("Session Bundle object download failed")
    })?;
    let mut response = Response::new(Body::from_stream(
        upstream
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other)),
    ));
    *response.status_mut() = StatusCode::OK;
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zstd"),
    );
    insert_response_header(
        response_headers,
        header::CONTENT_LENGTH,
        row.get::<i64, _>("current_bundle_size_bytes"),
    )?;
    let bundle_kind = row
        .get::<Option<String>, _>("current_bundle_kind")
        .unwrap_or_else(|| "checkpoint".to_owned());
    insert_response_header(
        response_headers,
        HeaderName::from_static("x-agent-hub-bundle-kind"),
        bundle_kind,
    )?;
    for (name, value) in [
        (
            "x-agent-hub-bundle-generation",
            row.get::<i64, _>("current_bundle_generation").to_string(),
        ),
        (
            "x-agent-hub-bundle-sha256",
            row.get::<String, _>("current_bundle_checksum_sha256"),
        ),
        (
            "x-agent-hub-history-checkpoint",
            row.get::<i64, _>("current_bundle_history_checkpoint")
                .to_string(),
        ),
        (
            "x-agent-hub-producing-engine-version",
            row.get::<Option<String>, _>("current_bundle_producing_engine_version")
                .unwrap_or_default(),
        ),
        (
            "x-agent-hub-bundle-created-at",
            row.get::<DateTime<Utc>, _>("current_bundle_created_at")
                .to_rfc3339(),
        ),
    ] {
        response_headers.insert(
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ApiError::internal("invalid Bundle response header name"))?,
            HeaderValue::from_str(&value)
                .map_err(|_| ApiError::internal("invalid Bundle response header value"))?,
        );
    }
    Ok(response)
}

pub(crate) fn parse_session_bundle_upload_headers(
    headers: &HeaderMap,
    max_size_bytes: u64,
) -> Result<SessionBundleUploadHeaders, ApiError> {
    let ownership_generation =
        parse_required_header::<i64>(headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    let checkpoint_attempt_id =
        parse_required_header::<Uuid>(headers, "x-agent-hub-checkpoint-attempt-id")?;
    if checkpoint_attempt_id.is_nil() {
        return Err(ApiError::bad_request(
            "Session Bundle checkpoint attempt id must not be nil",
        ));
    }
    let bundle_generation = parse_required_header::<i64>(headers, "x-agent-hub-bundle-generation")?;
    if bundle_generation <= 0 {
        return Err(ApiError::bad_request(
            "Session Bundle generation must be positive",
        ));
    }
    let checksum_sha256 = required_header(headers, "x-agent-hub-bundle-sha256")?;
    validate_bundle_sha256(&checksum_sha256)?;
    let size_bytes = parse_required_header::<u64>(headers, "x-agent-hub-bundle-size")?;
    if size_bytes > max_size_bytes {
        return Err(ApiError::bad_request(
            "Session Bundle exceeds the configured compressed size limit",
        ));
    }
    let content_length = parse_required_header::<u64>(headers, "content-length")?;
    if content_length != size_bytes {
        return Err(ApiError::bad_request(
            "Session Bundle Content-Length does not match its declaration",
        ));
    }
    let history_checkpoint =
        parse_required_header::<i64>(headers, "x-agent-hub-history-checkpoint")?;
    if history_checkpoint < 0 {
        return Err(ApiError::bad_request(
            "Session Bundle history checkpoint must not be negative",
        ));
    }
    let producing_engine_version =
        required_header(headers, "x-agent-hub-producing-engine-version")?;
    if producing_engine_version.len() > 128 {
        return Err(ApiError::bad_request(
            "Session Bundle producing engine version is too long",
        ));
    }
    let created_at =
        DateTime::parse_from_rfc3339(&required_header(headers, "x-agent-hub-bundle-created-at")?)
            .map_err(|_| ApiError::bad_request("invalid Session Bundle creation timestamp"))?
            .with_timezone(&Utc);
    Ok(SessionBundleUploadHeaders {
        ownership_generation,
        checkpoint_attempt_id,
        bundle_generation,
        checksum_sha256,
        size_bytes,
        history_checkpoint,
        producing_engine_version,
        created_at,
    })
}

pub(crate) fn required_header(headers: &HeaderMap, name: &str) -> Result<String, ApiError> {
    let value = headers
        .get(name)
        .ok_or_else(|| ApiError::bad_request(format!("missing required header {name}")))?
        .to_str()
        .map_err(|_| ApiError::bad_request(format!("invalid header {name}")))?
        .trim();
    if value.is_empty() {
        return Err(ApiError::bad_request(format!(
            "required header {name} must not be empty"
        )));
    }
    Ok(value.to_owned())
}

pub(crate) fn parse_required_header<T>(headers: &HeaderMap, name: &str) -> Result<T, ApiError>
where
    T: std::str::FromStr,
{
    required_header(headers, name)?
        .parse()
        .map_err(|_| ApiError::bad_request(format!("invalid header {name}")))
}

pub(crate) fn validate_bundle_sha256(value: &str) -> Result<(), ApiError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "Session Bundle checksum must be lowercase SHA-256 hex",
        ))
    }
}

pub(crate) fn session_bundle_object_key(
    session_id: Uuid,
    bundle_generation: i64,
    checkpoint_attempt_id: Uuid,
) -> String {
    format!("sessions/{session_id}/bundle-{bundle_generation}-{checkpoint_attempt_id}.tar.zst")
}

pub(crate) async fn validate_session_bundle_upload_preflight(
    pool: &PgPool,
    runtime_id: Uuid,
    session_id: Uuid,
    object_key: &str,
    metadata: &SessionBundleUploadHeaders,
) -> Result<bool, ApiError> {
    let row = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                saving_history_checkpoint, saving_ownership_generation,
                saving_checkpoint_attempt_id, current_bundle_checkpoint_attempt_id,
                current_bundle_generation, current_bundle_object_key,
                current_bundle_runtime_id,
                current_bundle_checksum_sha256, current_bundle_size_bytes,
                current_bundle_history_checkpoint, current_bundle_ownership_generation,
                current_bundle_producing_engine_version, current_bundle_created_at
         FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    let replay = row.get::<Option<Uuid>, _>("current_bundle_checkpoint_attempt_id")
        == Some(metadata.checkpoint_attempt_id);
    if replay {
        let identical = row.get::<Option<i64>, _>("current_bundle_generation")
            == Some(metadata.bundle_generation)
            && row
                .get::<Option<String>, _>("current_bundle_object_key")
                .as_deref()
                == Some(object_key)
            && row.get::<Option<Uuid>, _>("current_bundle_runtime_id") == Some(runtime_id)
            && row
                .get::<Option<String>, _>("current_bundle_checksum_sha256")
                .as_deref()
                == Some(metadata.checksum_sha256.as_str())
            && row.get::<Option<i64>, _>("current_bundle_size_bytes")
                == Some(metadata.size_bytes as i64)
            && row.get::<Option<i64>, _>("current_bundle_history_checkpoint")
                == Some(metadata.history_checkpoint)
            && row.get::<Option<i64>, _>("current_bundle_ownership_generation")
                == Some(metadata.ownership_generation)
            && row
                .get::<Option<String>, _>("current_bundle_producing_engine_version")
                .as_deref()
                == Some(metadata.producing_engine_version.as_str())
            && row
                .get::<Option<DateTime<Utc>>, _>("current_bundle_created_at")
                .is_some_and(|created_at| {
                    created_at.timestamp_micros() == metadata.created_at.timestamp_micros()
                });
        if !identical {
            return Err(ApiError::conflict(
                "Session Bundle attempt was already committed with different metadata",
            ));
        }
        return Ok(true);
    }
    if row.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || row.get::<i64, _>("ownership_generation") != metadata.ownership_generation
        || row.get::<String, _>("lifecycle_status") != "saving"
        || row.get::<Option<i64>, _>("saving_history_checkpoint")
            != Some(metadata.history_checkpoint)
        || row.get::<Option<i64>, _>("saving_ownership_generation")
            != Some(metadata.ownership_generation)
        || row.get::<Option<Uuid>, _>("saving_checkpoint_attempt_id")
            != Some(metadata.checkpoint_attempt_id)
    {
        return Err(ApiError::conflict(
            "Session Bundle upload has a stale owner, generation, checkpoint, or attempt",
        ));
    }
    let expected_generation = row
        .get::<Option<i64>, _>("current_bundle_generation")
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(ApiError::conflict("Session Bundle generation overflowed"))?;
    if metadata.bundle_generation != expected_generation {
        return Err(ApiError::conflict(
            "Session Bundle upload does not use the next generation",
        ));
    }
    Ok(false)
}

pub(crate) async fn commit_and_finalize_session_bundle(
    pool: &PgPool,
    runtime_id: Uuid,
    session_id: Uuid,
    object_key: &str,
    metadata: &SessionBundleUploadHeaders,
) -> Result<(RuntimeSessionBundleCommitResponseDto, Option<String>, bool), ApiError> {
    let mut tx = pool.begin().await?;
    let runtime_status: String =
        sqlx::query_scalar("SELECT status FROM runtimes WHERE id = $1 FOR UPDATE")
            .bind(runtime_id)
            .fetch_one(&mut *tx)
            .await?;
    let row = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                saving_reason, current_bundle_checkpoint_attempt_id,
                current_bundle_generation, current_bundle_object_key,
                current_bundle_runtime_id,
                current_bundle_checksum_sha256, current_bundle_size_bytes,
                current_bundle_history_checkpoint, current_bundle_ownership_generation,
                current_bundle_producing_engine_version, current_bundle_created_at
         FROM hub_sessions WHERE id = $1 FOR UPDATE",
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if row.get::<Option<Uuid>, _>("current_bundle_checkpoint_attempt_id")
        == Some(metadata.checkpoint_attempt_id)
    {
        let identical = row.get::<Option<i64>, _>("current_bundle_generation")
            == Some(metadata.bundle_generation)
            && row
                .get::<Option<String>, _>("current_bundle_object_key")
                .as_deref()
                == Some(object_key)
            && row.get::<Option<Uuid>, _>("current_bundle_runtime_id") == Some(runtime_id)
            && row
                .get::<Option<String>, _>("current_bundle_checksum_sha256")
                .as_deref()
                == Some(metadata.checksum_sha256.as_str())
            && row.get::<Option<i64>, _>("current_bundle_size_bytes")
                == Some(metadata.size_bytes as i64)
            && row.get::<Option<i64>, _>("current_bundle_history_checkpoint")
                == Some(metadata.history_checkpoint)
            && row.get::<Option<i64>, _>("current_bundle_ownership_generation")
                == Some(metadata.ownership_generation)
            && row
                .get::<Option<String>, _>("current_bundle_producing_engine_version")
                .as_deref()
                == Some(metadata.producing_engine_version.as_str())
            && row
                .get::<Option<DateTime<Utc>>, _>("current_bundle_created_at")
                .is_some_and(|created_at| {
                    created_at.timestamp_micros() == metadata.created_at.timestamp_micros()
                });
        if !identical {
            return Err(ApiError::conflict(
                "Session Bundle attempt was already committed with different metadata",
            ));
        }
        let has_queued_work =
            session_has_queued_work_tx(&mut tx, session_id, metadata.history_checkpoint).await?;
        let response = RuntimeSessionBundleCommitResponseDto {
            checkpoint_attempt_id: metadata.checkpoint_attempt_id,
            bundle_generation: metadata.bundle_generation,
            has_queued_work,
            ownership_released: row.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id),
        };
        tx.commit().await?;
        return Ok((response, None, true));
    }
    let old_object_key: Option<String> = row.get("current_bundle_object_key");
    let saving_reason: Option<String> = row.get("saving_reason");
    let committed = SessionBundleCommitMetadata {
        checkpoint_attempt_id: metadata.checkpoint_attempt_id,
        bundle_generation: metadata.bundle_generation,
        checksum_sha256: metadata.checksum_sha256.clone(),
        size_bytes: metadata.size_bytes as i64,
        history_checkpoint: metadata.history_checkpoint,
        producing_engine_version: metadata.producing_engine_version.clone(),
        created_at: metadata.created_at,
    };
    let _ = commit_session_bundle_metadata_tx(
        &mut tx,
        runtime_id,
        session_id,
        metadata.ownership_generation,
        object_key,
        &committed,
    )
    .await?;
    let has_queued_work =
        session_has_queued_work_tx(&mut tx, session_id, metadata.history_checkpoint).await?;
    let retain_owner =
        has_queued_work && runtime_status != "draining" && saving_reason.as_deref() == Some("idle");
    if retain_owner {
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online', saving_history_checkpoint = NULL,
                 saving_ownership_generation = NULL, saving_reason = NULL,
                 saving_checkpoint_attempt_id = NULL
             WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3",
        )
        .bind(session_id)
        .bind(runtime_id)
        .bind(metadata.ownership_generation)
        .execute(&mut *tx)
        .await?;
    } else {
        record_runtime_session_cleanup_tx(
            &mut tx,
            runtime_id,
            session_id,
            metadata.ownership_generation,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = NULL,
                 lifecycle_status = CASE WHEN $4 THEN 'waiting_for_runtime' ELSE 'offline' END,
                 saving_history_checkpoint = NULL, saving_ownership_generation = NULL,
                 saving_reason = NULL, saving_checkpoint_attempt_id = NULL
             WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3",
        )
        .bind(session_id)
        .bind(runtime_id)
        .bind(metadata.ownership_generation)
        .bind(has_queued_work)
        .execute(&mut *tx)
        .await?;
    }
    let response = RuntimeSessionBundleCommitResponseDto {
        checkpoint_attempt_id: metadata.checkpoint_attempt_id,
        bundle_generation: metadata.bundle_generation,
        has_queued_work,
        ownership_released: !retain_owner,
    };
    tx.commit().await?;
    Ok((response, old_object_key, false))
}

pub(crate) async fn record_runtime_session_cleanup_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
    session_id: Uuid,
    ownership_generation: i64,
    erasure_user_id: Option<Uuid>,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO runtime_session_cleanup_obligations
             (runtime_id, session_id, ownership_generation, erasure_user_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (runtime_id, session_id, ownership_generation)
         DO UPDATE SET erasure_user_id = COALESCE(
             runtime_session_cleanup_obligations.erasure_user_id,
             EXCLUDED.erasure_user_id
         )",
    )
    .bind(runtime_id)
    .bind(session_id)
    .bind(ownership_generation)
    .bind(erasure_user_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn session_has_queued_work_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    history_checkpoint: i64,
) -> Result<bool, ApiError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runs WHERE hub_session_id = $1 AND status = 'pending'
         ) OR EXISTS(
             SELECT 1 FROM hub_session_messages
             WHERE session_id = $1 AND sequence > $2 AND delivery_state = 'queued'
         )",
    )
    .bind(session_id)
    .bind(history_checkpoint)
    .fetch_one(&mut **tx)
    .await?)
}

pub(crate) fn insert_response_header<T: ToString>(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: T,
) -> Result<(), ApiError> {
    headers.insert(
        name,
        HeaderValue::from_str(&value.to_string())
            .map_err(|_| ApiError::internal("invalid Bundle response header value"))?,
    );
    Ok(())
}

pub(crate) async fn commit_session_bundle_metadata_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
    session_id: Uuid,
    ownership_generation: i64,
    object_key: &str,
    metadata: &SessionBundleCommitMetadata,
) -> Result<HubSessionDto, ApiError> {
    validate_ownership_generation(ownership_generation)?;
    if metadata.bundle_generation <= 0
        || metadata.size_bytes < 0
        || metadata.history_checkpoint < 0
        || metadata.checkpoint_attempt_id.is_nil()
        || object_key.trim().is_empty()
        || metadata.checksum_sha256.trim().is_empty()
        || metadata.producing_engine_version.trim().is_empty()
    {
        return Err(ApiError::bad_request("invalid Session Bundle metadata"));
    }
    let session = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation, lifecycle_status,
                saving_history_checkpoint, saving_ownership_generation,
                saving_checkpoint_attempt_id, current_bundle_checkpoint_attempt_id,
                current_bundle_generation, current_bundle_object_key,
                current_bundle_runtime_id,
                current_bundle_checksum_sha256, current_bundle_size_bytes,
                current_bundle_history_checkpoint, current_bundle_ownership_generation,
                current_bundle_producing_engine_version, current_bundle_created_at
         FROM hub_sessions
         WHERE id = $1
         FOR UPDATE",
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    if session.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || session.get::<i64, _>("ownership_generation") != ownership_generation
        || session.get::<String, _>("lifecycle_status") != "saving"
        || session.get::<Option<i64>, _>("saving_history_checkpoint")
            != Some(metadata.history_checkpoint)
        || session.get::<Option<i64>, _>("saving_ownership_generation")
            != Some(ownership_generation)
        || session.get::<Option<Uuid>, _>("saving_checkpoint_attempt_id")
            != Some(metadata.checkpoint_attempt_id)
    {
        return Err(ApiError::conflict(
            "Session Bundle metadata has a stale owner, generation, checkpoint, or attempt",
        ));
    }
    let has_unreplayable_history: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM hub_session_messages
             WHERE session_id = $1
               AND sequence > $2
               AND delivery_state IN ('delivering', 'delivered')
         )",
    )
    .bind(session_id)
    .bind(metadata.history_checkpoint)
    .fetch_one(&mut **tx)
    .await?;
    if has_unreplayable_history {
        return Err(ApiError::conflict(
            "Session Bundle checkpoint is older than unreplayable Hub history",
        ));
    }

    if session.get::<Option<Uuid>, _>("current_bundle_checkpoint_attempt_id")
        == Some(metadata.checkpoint_attempt_id)
    {
        let identical = session.get::<Option<i64>, _>("current_bundle_generation")
            == Some(metadata.bundle_generation)
            && session
                .get::<Option<String>, _>("current_bundle_object_key")
                .as_deref()
                == Some(object_key.trim())
            && session.get::<Option<Uuid>, _>("current_bundle_runtime_id") == Some(runtime_id)
            && session
                .get::<Option<String>, _>("current_bundle_checksum_sha256")
                .as_deref()
                == Some(metadata.checksum_sha256.trim())
            && session.get::<Option<i64>, _>("current_bundle_size_bytes")
                == Some(metadata.size_bytes)
            && session.get::<Option<i64>, _>("current_bundle_history_checkpoint")
                == Some(metadata.history_checkpoint)
            && session.get::<Option<i64>, _>("current_bundle_ownership_generation")
                == Some(ownership_generation)
            && session
                .get::<Option<String>, _>("current_bundle_producing_engine_version")
                .as_deref()
                == Some(metadata.producing_engine_version.trim())
            && session
                .get::<Option<DateTime<Utc>>, _>("current_bundle_created_at")
                .is_some_and(|created_at| {
                    created_at.timestamp_micros() == metadata.created_at.timestamp_micros()
                });
        if identical {
            return load_hub_session_tx(tx, session_id).await;
        }
        return Err(ApiError::conflict(
            "Session Bundle commit attempt was already recorded with different metadata",
        ));
    }
    if session
        .get::<Option<i64>, _>("current_bundle_generation")
        .is_some_and(|generation| generation >= metadata.bundle_generation)
        || session
            .get::<Option<i64>, _>("current_bundle_history_checkpoint")
            .is_some_and(|checkpoint| checkpoint > metadata.history_checkpoint)
    {
        return Err(ApiError::conflict(
            "Session Bundle metadata does not advance the current Bundle",
        ));
    }

    sqlx::query(
        "UPDATE hub_sessions
         SET current_bundle_generation = $1,
             current_bundle_object_key = $2,
             current_bundle_checksum_sha256 = $3,
             current_bundle_size_bytes = $4,
             current_bundle_history_checkpoint = $5,
             current_bundle_ownership_generation = $6,
             current_bundle_producing_engine_version = $7,
             current_bundle_created_at = $8,
             current_bundle_runtime_id = $10,
             current_bundle_checkpoint_attempt_id = $11,
             current_bundle_kind = 'checkpoint'
         WHERE id = $9
           AND runtime_owner_id = $10
           AND ownership_generation = $6
           AND lifecycle_status = 'saving'
           AND saving_history_checkpoint = $5
           AND saving_ownership_generation = $6
           AND saving_checkpoint_attempt_id = $11",
    )
    .bind(metadata.bundle_generation)
    .bind(object_key.trim())
    .bind(metadata.checksum_sha256.trim())
    .bind(metadata.size_bytes)
    .bind(metadata.history_checkpoint)
    .bind(ownership_generation)
    .bind(metadata.producing_engine_version.trim())
    .bind(metadata.created_at)
    .bind(session_id)
    .bind(runtime_id)
    .bind(metadata.checkpoint_attempt_id)
    .execute(&mut **tx)
    .await?;
    load_hub_session_tx(tx, session_id).await
}

/// 流式包装：边转发边计算实际字节数与 SHA-256（防声明与实际不符）。
/// put_stream 消费后不返回流本体，因此用共享累加器取出实测值。
struct HashAccumulator {
    hasher: Sha256,
    count: u64,
}

impl HashAccumulator {
    fn digest(&self) -> (String, u64) {
        (format!("{:x}", self.hasher.clone().finalize()), self.count)
    }
}

struct HashingStream<S> {
    inner: S,
    shared: Arc<std::sync::Mutex<HashAccumulator>>,
}

impl<S> HashingStream<S> {
    fn new(inner: S) -> (Self, Arc<std::sync::Mutex<HashAccumulator>>) {
        let shared = Arc::new(std::sync::Mutex::new(HashAccumulator {
            hasher: Sha256::new(),
            count: 0,
        }));
        (
            Self {
                inner,
                shared: Arc::clone(&shared),
            },
            shared,
        )
    }
}

impl<S> futures_util::Stream for HashingStream<S>
where
    S: futures_util::Stream<Item = Result<Bytes, axum::Error>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match futures_util::Stream::poll_next(std::pin::Pin::new(&mut self.inner), cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                let mut acc = self.shared.lock().unwrap();
                acc.hasher.update(&chunk);
                acc.count += chunk.len() as u64;
                drop(acc);
                std::task::Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }
}

pub(crate) fn validate_sha256_hex(value: &str) -> Result<(), ApiError> {
    if value.len() == 64
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "pending runtime credential hash must be lowercase SHA-256 hex",
        ))
    }
}

pub(crate) fn validate_ownership_generation(generation: i64) -> Result<(), ApiError> {
    if generation > 0 {
        Ok(())
    } else {
        Err(ApiError::bad_request(
            "ownership generation must be positive",
        ))
    }
}

pub(crate) fn validate_execution_configuration_fingerprint(value: &str) -> Result<(), ApiError> {
    let digest = value.strip_prefix("sha256:").ok_or(ApiError::bad_request(
        "valid execution configuration fingerprint is required",
    ))?;
    validate_sha256_hex(digest)
        .map_err(|_| ApiError::bad_request("valid execution configuration fingerprint is required"))
}

pub(crate) const RUNTIME_CLAIM_SESSION_ELIGIBILITY_SQL: &str = "(($2::bigint > 0
        AND hs.runtime_owner_id IS NULL
        AND hs.lifecycle_status IN ('waiting_for_runtime', 'offline')
      AND NOT EXISTS (
        SELECT 1 FROM hub_session_messages AS unreplayable_messages
        WHERE unreplayable_messages.session_id = hs.id
          AND unreplayable_messages.delivery_state IN ('delivering', 'delivered')
          AND hs.current_bundle_history_checkpoint IS NOT NULL
          AND unreplayable_messages.sequence > hs.current_bundle_history_checkpoint
      ))
      OR
      (hs.runtime_owner_id = $1
        AND hs.lifecycle_status IN ('restoring', 'online')
        AND EXISTS (
          SELECT 1
          FROM unnest($3::uuid[], $4::bigint[])
               AS ready(session_id, ownership_generation)
          WHERE ready.session_id = hs.id
            AND ready.ownership_generation = hs.ownership_generation
        )))
      AND NOT EXISTS (
        SELECT 1 FROM runs AS waiting_tool_runs
        WHERE waiting_tool_runs.hub_session_id = hs.id
          AND waiting_tool_runs.status = 'waiting_tool'
          AND EXISTS (
            SELECT 1 FROM integration_tool_requests AS pending_tool_requests
            WHERE pending_tool_requests.run_id = waiting_tool_runs.id
              AND pending_tool_requests.status = 'pending'
          )
          AND NOT (
            r.source = 'integration:tool_result'
            AND r.parent_run_id = waiting_tool_runs.id
          )
      )";

pub(crate) fn runtime_claim_candidate_sql() -> String {
    format!(
        "SELECT r.id AS run_id, r.agent_id
         FROM runs r
         JOIN agents a ON a.id = r.agent_id
         JOIN runtimes rt ON rt.id = $1
         JOIN hub_sessions hs ON hs.id = r.hub_session_id
         WHERE r.status = 'pending'
           AND a.deleted_at IS NULL
           AND EXISTS (
             SELECT 1 FROM model_connections model
             WHERE model.id = a.model_connection_id
               AND model.enabled = true AND model.deleted_at IS NULL
               AND a.model_id = ANY(model.allowed_model_ids)
               AND (a.model_settings->'request_settings')->>'protocol' = model.api_type
               AND (model.scope = 'global' OR model.owner_id = a.owner_id)
           )
           AND (a.runtime_id IS NULL OR a.runtime_id = $1)
           AND {RUNTIME_CLAIM_SESSION_ELIGIBILITY_SQL}
           {RUNTIME_CAPABILITY_SQL}
         ORDER BY r.created_at ASC
         LIMIT 1"
    )
}

pub(crate) fn runtime_claim_agent_sql() -> String {
    format!(
        "SELECT a.id AS a_id, a.owner_id,
                (SELECT email FROM users WHERE id = a.owner_id) AS owner_email,
                a.name, a.instructions, a.visibility, a.public_to,
                a.runtime_id AS a_runtime_id, a.model_policy AS a_model_policy,
                a.model_connection_id AS a_model_connection_id,
                a.model_id AS a_model_id, a.model_settings AS a_model_settings,
                a.sandbox_policy AS a_sandbox_policy, a.mcp_allowlist AS a_mcp_allowlist,
                a.tool_allowlist AS a_tool_allowlist,
                a.execution_config_revision AS a_execution_config_revision,
                a.created_at AS a_created_at, a.updated_at AS a_updated_at
         FROM agents a
         JOIN runtimes rt ON rt.id = $1
         WHERE a.id = $2
           AND a.deleted_at IS NULL
           AND EXISTS (
             SELECT 1 FROM model_connections model
             WHERE model.id = a.model_connection_id
               AND model.enabled = true AND model.deleted_at IS NULL
               AND a.model_id = ANY(model.allowed_model_ids)
               AND (a.model_settings->'request_settings')->>'protocol' = model.api_type
               AND (model.scope = 'global' OR model.owner_id = a.owner_id)
           )
           AND (a.runtime_id IS NULL OR a.runtime_id = $1)
           {RUNTIME_CAPABILITY_SQL}
         FOR SHARE OF a"
    )
}

pub(crate) fn runtime_claim_run_sql() -> String {
    format!(
        "SELECT r.id, r.agent_id, r.automation_id, r.integration_session_id,
                r.parent_run_id, r.runtime_id, r.hub_session_id, r.hub_message_id,
                r.hub_turn_id, r.session_ownership_generation, r.status,
                r.initial_message, r.native_session_id, r.work_dir_ref, r.source,
                r.created_at, r.updated_at
         FROM runs r
         JOIN hub_sessions hs ON hs.id = r.hub_session_id
         WHERE r.id = $5
           AND r.agent_id = $6
           AND r.status = 'pending'
           AND {RUNTIME_CLAIM_SESSION_ELIGIBILITY_SQL}
         FOR UPDATE OF r, hs SKIP LOCKED"
    )
}

pub(crate) async fn runtime_claim_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<RuntimeClaimRunRequest>,
) -> Result<impl IntoResponse, ApiError> {
    reap_stale_runtimes(&state.pool).await?;
    let token = bearer_token(&headers).ok_or(ApiError::unauthorized("missing runtime token"))?;
    let mut tx = state.pool.begin().await?;
    let runtime_row = sqlx::query(
        "SELECT id, status FROM runtimes
         WHERE token_hash = $1
           AND credential_revoked_at IS NULL
         FOR UPDATE",
    )
    .bind(sha256_hex(&token))
    .fetch_optional(&mut *tx)
    .await?;
    let runtime_row = runtime_row.ok_or(ApiError::unauthorized("invalid runtime credential"))?;
    let runtime_id: Uuid = runtime_row.get("id");
    let runtime_status: String = runtime_row.get("status");
    if runtime_status == "draining" {
        tx.commit().await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    if runtime_status != "online" {
        return Err(ApiError::unauthorized("runtime is not online"));
    }
    let mut unique_ready_session_ids = BTreeSet::new();
    for owned_session in &request.ready_owned_sessions {
        validate_ownership_generation(owned_session.ownership_generation)?;
        if !unique_ready_session_ids.insert(owned_session.session_id) {
            return Err(ApiError::bad_request(
                "ready owned Sessions must not contain duplicate session ids",
            ));
        }
    }
    let ready_session_ids = request
        .ready_owned_sessions
        .iter()
        .map(|owned_session| owned_session.session_id)
        .collect::<Vec<_>>();
    let ready_generations = request
        .ready_owned_sessions
        .iter()
        .map(|owned_session| owned_session.ownership_generation)
        .collect::<Vec<_>>();
    let failed_capability_run_ids =
        fail_capability_mismatched_runs_for_runtime_tx(&mut tx, runtime_id).await?;
    for run_id in failed_capability_run_ids {
        insert_run_event_tx(
            &mut tx,
            run_id,
            "status".into(),
            None,
            Some("failed".into()),
            json!({ "status": "failed", "reason": "runtime capability mismatch" }),
        )
        .await?;
    }
    let candidate_sql = runtime_claim_candidate_sql();
    let candidate = sqlx::query(&candidate_sql)
        .bind(runtime_id)
        .bind(i64::from(request.available_new_session_slots))
        .bind(&ready_session_ids)
        .bind(&ready_generations)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(candidate) = candidate else {
        tx.commit().await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let candidate_run_id: Uuid = candidate.get("run_id");
    let candidate_agent_id: Uuid = candidate.get("agent_id");

    let agent_sql = runtime_claim_agent_sql();
    let agent_row = sqlx::query(&agent_sql)
        .bind(runtime_id)
        .bind(candidate_agent_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(agent_row) = agent_row else {
        tx.commit().await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    };

    let claim_sql = runtime_claim_run_sql();
    let row = sqlx::query(&claim_sql)
        .bind(runtime_id)
        .bind(i64::from(request.available_new_session_slots))
        .bind(&ready_session_ids)
        .bind(&ready_generations)
        .bind(candidate_run_id)
        .bind(candidate_agent_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    };

    let model_proxy_token = format!("ahr_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let run_id: Uuid = row.get("id");
    let hub_session_id: Uuid = row.get("hub_session_id");
    let hub_turn_id: Uuid = row.get("hub_turn_id");
    let source: String = row.get("source");
    // 1. 先锁会话行（claim 事务内），校验归属，避免终结/清理读到并发变化。
    let locked: Option<(Option<Uuid>,)> =
        sqlx::query_as("SELECT runtime_owner_id FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(hub_session_id)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((current_owner,)) = locked else {
        return Err(ApiError::conflict(
            "session ownership changed while claiming",
        ));
    };
    if current_owner.is_some() && current_owner != Some(runtime_id) {
        return Err(ApiError::conflict(
            "session ownership changed while claiming",
        ));
    }
    // 1b. 活动 Turn 仍被 running/waiting_tool Run 引用时（例如同 Session 另一
    //     run 正在执行），不得终结/接管：回滚并返回 NO_CONTENT（run 保持
    //     pending，旧 Turn 保持 running），避免误杀活动执行。
    let active_turn_claimed: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM runs AS active_runs
             WHERE active_runs.hub_session_id = $1
               AND active_runs.hub_turn_id = (
                   SELECT active_turn_id FROM hub_sessions WHERE id = $1
               )
               AND active_runs.status IN ('running', 'waiting_tool')
         )",
    )
    .bind(hub_session_id)
    .fetch_one(&mut *tx)
    .await?;
    if active_turn_claimed && source != "integration:tool_result" {
        // 事务内更早已执行 capability mismatch 标记等合法变更，不能回滚；
        // 本分支未修改 candidate/session/run，提交后返回 NO_CONTENT。
        tx.commit().await?;
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    // 2. 终结旧的残留活动 turn（异常/崩溃路径，此时 active_turn_id 还是旧值），
    //    避免旧 Run 晚到的 completion/heartbeat 造成状态冲突；绝不误标当前
    //    待 claim 的 turn（id <> hub_turn_id），tool_result 续跑保留 active turn。
    if source != "integration:tool_result" {
        sqlx::query(
            "UPDATE hub_session_turns AS old_turns
             SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
             WHERE old_turns.session_id = $1
               AND old_turns.id <> $2
               AND old_turns.id = (SELECT active_turn_id FROM hub_sessions WHERE id = $1)
               AND old_turns.status IN ('pending', 'starting', 'running')
               AND NOT EXISTS (
                   SELECT 1 FROM runs AS active_runs
                   WHERE active_runs.hub_session_id = $1
                     AND active_runs.hub_turn_id = old_turns.id
                     AND active_runs.status IN ('running', 'waiting_tool')
               )",
        )
        .bind(hub_session_id)
        .bind(hub_turn_id)
        .execute(&mut *tx)
        .await?;
    }
    // 3. 会话接管（owner/gen/status），并清理 active_turn 指针（tool_result 除外）。
    let ownership_row = sqlx::query(
        "UPDATE hub_sessions
         SET runtime_owner_id = $1,
             ownership_generation = CASE
                 WHEN runtime_owner_id = $1 THEN ownership_generation
                 ELSE ownership_generation + 1
             END,
             lifecycle_status = CASE
                 WHEN runtime_owner_id = $1 THEN lifecycle_status
                 ELSE 'restoring'
             END,
             recovery_source = CASE
                 WHEN runtime_owner_id = $1 THEN recovery_source
                 ELSE 'bundle'
             END,
             active_turn_id = CASE
                 WHEN $3 = 'integration:tool_result' THEN active_turn_id
                 WHEN EXISTS (
                     SELECT 1 FROM runs AS active_runs
                     WHERE active_runs.hub_session_id = $2
                       AND active_runs.hub_turn_id = hub_sessions.active_turn_id
                       AND active_runs.status IN ('running', 'waiting_tool')
                 ) THEN active_turn_id
                 ELSE NULL
             END
         WHERE id = $2
           AND (runtime_owner_id IS NULL OR runtime_owner_id = $1)
         RETURNING ownership_generation",
    )
    .bind(runtime_id)
    .bind(hub_session_id)
    .bind(&source)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::conflict(
        "session ownership changed while claiming",
    ))?;
    let ownership_generation: i64 = ownership_row.get("ownership_generation");
    let turn_updated = sqlx::query(
        "UPDATE hub_session_turns
         SET ownership_generation = $1, updated_at = now()
         WHERE id = $2 AND session_id = $3 AND status = 'pending'",
    )
    .bind(ownership_generation)
    .bind(hub_turn_id)
    .bind(hub_session_id)
    .execute(&mut *tx)
    .await?;
    if turn_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "session Turn is no longer pending while claiming",
        ));
    }
    let run_row = sqlx::query(
        "UPDATE runs
         SET status = 'running', runtime_id = $1, model_proxy_token_hash = $3,
             session_ownership_generation = $4, updated_at = now()
         WHERE id = $2
         RETURNING id, agent_id, automation_id, integration_session_id, parent_run_id,
                   runtime_id, hub_session_id, hub_message_id, hub_turn_id,
                   session_ownership_generation, status, initial_message, native_session_id,
                   work_dir_ref, source, created_at, updated_at",
    )
    .bind(runtime_id)
    .bind(run_id)
    .bind(sha256_hex(&model_proxy_token))
    .bind(ownership_generation)
    .fetch_one(&mut *tx)
    .await?;
    insert_run_event_tx(
        &mut tx,
        run_id,
        "status".into(),
        None,
        Some("running".into()),
        json!({
            "status": "running",
            "runtime_id": runtime_id,
            "ownership_generation": ownership_generation
        }),
    )
    .await?;
    let mut agent = AgentDto {
        id: agent_row.get("a_id"),
        owner_id: agent_row.get("owner_id"),
        owner_email: agent_row.get("owner_email"),
        name: agent_row.get("name"),
        instructions: agent_row.get("instructions"),
        visibility: agent_row.get("visibility"),
        public_to: agent_row.get("public_to"),
        endpoint_exposure: vec!["console".into(), "integration".into(), "automation".into()],
        runtime_id: agent_row.get("a_runtime_id"),
        model_selection: Some(ModelSelectionDto {
            connection_id: agent_row.get("a_model_connection_id"),
            model_id: agent_row.get("a_model_id"),
        }),
        model_settings: serde_json::from_value(agent_row.get("a_model_settings"))
            .expect("Agent Model Settings are constrained"),
        subagents: Vec::new(),
        model_policy: agent_row.get("a_model_policy"),
        sandbox_policy: agent_row.get("a_sandbox_policy"),
        managed_skill_ids: Vec::new(),
        secret_declarations: Vec::new(),
        mcp_allowlist: agent_row.get("a_mcp_allowlist"),
        tool_allowlist: serde_json::from_value(agent_row.get("a_tool_allowlist"))
            .expect("Agent tool policy is constrained"),
        is_owner: false,
        can_manage: false,
        can_administer: false,
        can_invoke: false,
        created_at: agent_row.get("a_created_at"),
        updated_at: agent_row.get("a_updated_at"),
    };
    agent.subagents = load_subagents_tx(&mut tx, agent.id).await?;
    agent.secret_declarations = load_agent_secret_declarations(&state.pool, agent.id).await?;
    apply_session_tool_policy_to_agent_tx(&mut tx, hub_session_id, &mut agent).await?;
    let execution_config_revision: i64 = agent_row.get("a_execution_config_revision");
    let skill_rows = sqlx::query(
        "SELECT s.id, s.name, s.description, s.content, s.revision,
                s.content_checksum_sha256,
                COALESCE(snapshots.package_id, packages.id) AS package_id,
                COALESCE(snapshots.format_version, packages.format_version)
                    AS package_format_version,
                COALESCE(snapshots.object_key, packages.object_key) AS package_object_key,
                COALESCE(snapshots.size_bytes, packages.size_bytes) AS package_size_bytes,
                COALESCE(snapshots.checksum_sha256, packages.checksum_sha256)
                    AS package_checksum_sha256,
                COALESCE(snapshots.files, packages.files) AS package_files
         FROM agent_skills a_s
         JOIN skills s ON s.id = a_s.skill_id
         LEFT JOIN skill_packages AS packages ON packages.id = s.current_package_id
         LEFT JOIN run_skill_packages AS snapshots
           ON snapshots.run_id = $3 AND snapshots.skill_id = s.id
         LEFT JOIN users AS agent_owner ON agent_owner.id = $2
         WHERE a_s.agent_id = $1
           AND (s.owner_id = $2 OR s.visibility = 'public'
                OR (s.visibility = 'public_to' AND $2 = ANY(s.public_to))
                OR agent_owner.role IN ('admin', 'super_admin'))
         ORDER BY s.name, s.id
         FOR SHARE OF s",
    )
    .bind(agent.id)
    .bind(agent.owner_id)
    .bind(run_id)
    .fetch_all(&mut *tx)
    .await?;
    for skill_row in &skill_rows {
        let Some(package_id) = skill_row.get::<Option<Uuid>, _>("package_id") else {
            continue;
        };
        sqlx::query(
            "INSERT INTO run_skill_packages
                 (run_id, skill_id, package_id, object_key, format_version,
                  size_bytes, checksum_sha256, files)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (run_id, skill_id) DO NOTHING",
        )
        .bind(run_id)
        .bind(skill_row.get::<Uuid, _>("id"))
        .bind(package_id)
        .bind(skill_row.get::<String, _>("package_object_key"))
        .bind(skill_row.get::<i32, _>("package_format_version"))
        .bind(skill_row.get::<i64, _>("package_size_bytes"))
        .bind(skill_row.get::<String, _>("package_checksum_sha256"))
        .bind(skill_row.get::<Value, _>("package_files"))
        .execute(&mut *tx)
        .await?;
    }
    let existing_model_bindings = load_run_model_bindings_tx(&mut tx, run_id).await?;
    let model_bindings = if existing_model_bindings.is_empty() {
        create_run_model_bindings_tx(&mut tx, run_id, &agent).await?
    } else {
        let main_binding = existing_model_bindings
            .iter()
            .find(|binding| binding.binding_key.eq_ignore_ascii_case("main"))
            .ok_or(ApiError::internal(
                "Run Model Binding snapshot has no main binding",
            ))?;
        agent.model_selection = Some(ModelSelectionDto {
            connection_id: main_binding.model_connection_id,
            model_id: main_binding.model_id.clone(),
        });
        agent.model_settings = main_binding.model_settings.clone();
        existing_model_bindings
    };
    let mut execution_configuration =
        build_agent_execution_configuration(&agent, execution_config_revision, skill_rows)?;
    execution_configuration.model_bindings = model_bindings;
    let expected_configuration_fingerprint =
        execution_configuration_fingerprint(&execution_configuration)
            .map_err(|error| ApiError::internal(error.to_string()))?;
    let run = run_from_row(run_row);
    let session_context = Some(load_claim_session_context_tx(&mut tx, &run).await?);
    let canonical_native_session_id = session_context
        .as_ref()
        .and_then(|context| context.session.native_session_id.clone());
    let parent_resume = if let Some(parent_run_id) = run.parent_run_id {
        sqlx::query(
            "SELECT native_session_id, work_dir_ref
             FROM runs
             WHERE id = $1 AND agent_id = $2",
        )
        .bind(parent_run_id)
        .bind(run.agent_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| {
            (
                row.get::<Option<String>, _>("native_session_id"),
                row.get::<Option<String>, _>("work_dir_ref"),
            )
        })
    } else {
        None
    };
    let resume = match (canonical_native_session_id, parent_resume) {
        (Some(native_session_id), parent) => Some(RunResumeDto {
            native_session_id,
            work_dir_ref: parent.and_then(|(_, work_dir_ref)| work_dir_ref),
        }),
        (None, Some((Some(native_session_id), work_dir_ref))) => Some(RunResumeDto {
            native_session_id,
            work_dir_ref,
        }),
        (None, _) => None,
    };
    let mut integration_context = load_integration_context_for_run(&mut tx, &run).await?;
    if !agent
        .tool_allowlist
        .iter()
        .any(|tool| tool == "integration")
    {
        if let Some(context) = &mut integration_context {
            context.tools = json!([]);
        }
    }
    let mut secret_values = Vec::new();
    let mut secret_files = Vec::new();
    {
        let subject_user_id =
            sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM hub_sessions WHERE id = $1")
                .bind(hub_session_id)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(user_id) = subject_user_id {
            for declaration in &agent.secret_declarations {
                let authorized = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(
                        SELECT 1 FROM secret_grants
                        WHERE user_id = $1 AND agent_id = $2 AND secret_name = $3
                     ) AND EXISTS(
                        SELECT 1 FROM user_secrets WHERE owner_id = $1 AND name = $3
                     )",
                )
                .bind(user_id)
                .bind(agent.id)
                .bind(&declaration.name)
                .fetch_one(&mut *tx)
                .await?;
                if authorized {
                    let secret_kind = sqlx::query_scalar::<_, String>(
                        "SELECT kind FROM user_secrets WHERE owner_id = $1 AND name = $2",
                    )
                    .bind(user_id)
                    .bind(&declaration.name)
                    .fetch_one(&mut *tx)
                    .await?;
                    match secret_kind.as_str() {
                        "value" => {
                            let (ciphertext, nonce) = sqlx::query_as::<_, (Vec<u8>, Vec<u8>)>(
                                "SELECT value_ciphertext, value_nonce
                                     FROM user_secrets WHERE owner_id = $1 AND name = $2",
                            )
                            .bind(user_id)
                            .bind(&declaration.name)
                            .fetch_one(&mut *tx)
                            .await?;
                            let value = state
                                .model_secret_cipher
                                .decrypt(&ciphertext, &nonce)
                                .map_err(|_| ApiError::internal("Secret decryption failed"))?;
                            secret_values.push(RunSecretValueDto {
                                name: declaration.name.clone(),
                                value,
                            });
                        }
                        "file" => {
                            let (size_bytes, sha256) = sqlx::query_as::<_, (i64, String)>(
                                "SELECT file_size_bytes, file_sha256
                                     FROM user_secrets WHERE owner_id = $1 AND name = $2",
                            )
                            .bind(user_id)
                            .bind(&declaration.name)
                            .fetch_one(&mut *tx)
                            .await?;
                            secret_files.push(RunSecretFileDto {
                                name: declaration.name.clone(),
                                size_bytes,
                                sha256,
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    tx.commit().await?;

    Ok(Json(ClaimRunResponse {
        run,
        agent,
        execution_configuration,
        expected_configuration_fingerprint,
        integration_context,
        resume,
        model_proxy_token,
        secret_values,
        secret_files,
        session_context,
    })
    .into_response())
}

pub(crate) async fn runtime_download_run_secret_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((run_id, secret_name)): Path<(Uuid, String)>,
) -> Result<Response, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let ownership_generation =
        parse_required_header::<i64>(&headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    if !validate_secret_name(&secret_name) {
        return Err(ApiError::bad_request("secret name is invalid"));
    }
    let row = sqlx::query(
        "SELECT runs.agent_id, sessions.owner_id
         FROM runs
         JOIN hub_sessions AS sessions ON sessions.id = runs.hub_session_id
         WHERE runs.id = $1
           AND runs.runtime_id = $2
           AND runs.session_ownership_generation = $3
           AND runs.status IN ('running', 'waiting_tool')",
    )
    .bind(run_id)
    .bind(runtime_id)
    .bind(ownership_generation)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("Run secret file not found"))?;
    let agent_id: Uuid = row.get("agent_id");
    let owner_id: Uuid = row.get("owner_id");
    let declared = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1 FROM agent_secret_declarations
            WHERE agent_id = $1 AND name = $2
         )",
    )
    .bind(agent_id)
    .bind(&secret_name)
    .fetch_one(&state.pool)
    .await?;
    if !declared {
        return Err(ApiError::not_found("Run secret file not found"));
    }
    let row = sqlx::query(
        "SELECT us.file_ciphertext, us.file_nonce, us.file_name
         FROM user_secrets AS us
         WHERE us.owner_id = $1 AND us.name = $2
           AND EXISTS(
               SELECT 1 FROM secret_grants AS grants
               WHERE grants.user_id = us.owner_id
                 AND grants.agent_id = $3
                 AND grants.secret_name = us.name
           )",
    )
    .bind(owner_id)
    .bind(&secret_name)
    .bind(agent_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("Run secret file not found"))?;
    let ciphertext: Vec<u8> = row.get("file_ciphertext");
    let nonce: Vec<u8> = row.get("file_nonce");
    let file_name: String = row.get("file_name");
    let plaintext = state
        .model_secret_cipher
        .decrypt(&ciphertext, &nonce)
        .map_err(|_| ApiError::internal("Secret file decryption failed"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(plaintext)
        .map_err(|_| ApiError::internal("Secret file plaintext is invalid"))?;
    let content_disposition = format!("attachment; filename=\"{file_name}\"");
    Ok((
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            ),
            (
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&content_disposition)
                    .map_err(|_| ApiError::internal("invalid secret file name"))?,
            ),
        ],
        Body::from(bytes),
    )
        .into_response())
}

pub(crate) async fn runtime_download_run_skill_package(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((run_id, skill_id)): Path<(Uuid, Uuid)>,
) -> Result<Response, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let ownership_generation =
        parse_required_header::<i64>(&headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    let row = sqlx::query(
        "SELECT snapshots.package_id, snapshots.object_key, snapshots.size_bytes,
                snapshots.checksum_sha256, runs.runtime_id,
                runs.session_ownership_generation, runs.status
         FROM run_skill_packages AS snapshots
         JOIN runs ON runs.id = snapshots.run_id
         WHERE snapshots.run_id = $1 AND snapshots.skill_id = $2",
    )
    .bind(run_id)
    .bind(skill_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("Run Skill package not found"))?;
    if row.get::<Option<Uuid>, _>("runtime_id") != Some(runtime_id)
        || row.get::<Option<i64>, _>("session_ownership_generation") != Some(ownership_generation)
        || !matches!(
            row.get::<String, _>("status").as_str(),
            "running" | "waiting_tool"
        )
    {
        return Err(ApiError::forbidden(
            "runtime does not own this active Run generation",
        ));
    }
    skill_package_download_response(
        &state,
        row.get("package_id"),
        row.get("object_key"),
        row.get("size_bytes"),
        row.get("checksum_sha256"),
    )
    .await
}

pub(crate) async fn runtime_download_session_skill_package(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((session_id, skill_id, package_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Response, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let ownership_generation =
        parse_required_header::<i64>(&headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    let row = sqlx::query(
        "SELECT packages.object_key, packages.size_bytes, packages.checksum_sha256,
                sessions.runtime_owner_id, sessions.ownership_generation,
                sessions.lifecycle_status
         FROM hub_sessions AS sessions
         JOIN agents ON agents.id = sessions.agent_id AND agents.deleted_at IS NULL
         JOIN agent_skills ON agent_skills.agent_id = agents.id
         JOIN skills ON skills.id = agent_skills.skill_id
                    AND skills.owner_id = agents.owner_id
         JOIN skill_packages AS packages ON packages.id = skills.current_package_id
         WHERE sessions.id = $1 AND skills.id = $2 AND packages.id = $3",
    )
    .bind(session_id)
    .bind(skill_id)
    .bind(package_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("Session Skill package not found"))?;
    if row.get::<Option<Uuid>, _>("runtime_owner_id") != Some(runtime_id)
        || row.get::<i64, _>("ownership_generation") != ownership_generation
        || !matches!(
            row.get::<String, _>("lifecycle_status").as_str(),
            "restoring" | "online"
        )
    {
        return Err(
            ApiError::forbidden("runtime does not own this Session generation")
                .with_code("stale_session_generation"),
        );
    }
    skill_package_download_response(
        &state,
        package_id,
        row.get("object_key"),
        row.get("size_bytes"),
        row.get("checksum_sha256"),
    )
    .await
}

pub(crate) async fn skill_package_download_response(
    state: &AppState,
    package_id: Uuid,
    object_key: String,
    size_bytes: i64,
    checksum_sha256: String,
) -> Result<Response, ApiError> {
    let store = state
        .skill_package_store
        .as_ref()
        .ok_or(ApiError::service_unavailable(
            "Skill package object storage is not configured",
        ))?;
    let object = store.get(&object_key).await.map_err(|error| {
        warn!(package_id = %package_id, error = %error, "Skill package object download failed");
        ApiError::bad_gateway("Skill package object download failed")
    })?;
    let mut response = Response::new(object.body);
    *response.status_mut() = StatusCode::OK;
    let response_headers = response.headers_mut();
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zstd"),
    );
    insert_response_header(response_headers, header::CONTENT_LENGTH, size_bytes)?;
    for (name, value) in [
        ("x-agent-hub-skill-package-id", package_id.to_string()),
        ("x-agent-hub-skill-package-sha256", checksum_sha256),
    ] {
        response_headers.insert(
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| ApiError::internal("invalid Skill package response header name"))?,
            HeaderValue::from_str(&value)
                .map_err(|_| ApiError::internal("invalid Skill package response header value"))?,
        );
    }
    Ok(response)
}

/// bundle 打包同步状态（管理端）：按 runtime 聚合各状态的会话数量。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BundleSyncStatusResponse {
    pub(crate) runtime_id: Uuid,
    pub(crate) total: i64,
    pub(crate) pending: i64,
    pub(crate) uploading: i64,
    pub(crate) done: i64,
    pub(crate) failed: i64,
}

pub(crate) async fn get_bundle_sync_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<BundleSyncStatusResponse>>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let _ = administrator;
    let rows: Vec<(Uuid, i64, i64, i64, i64, i64)> = sqlx::query_as(
        "SELECT runtime_owner_id,
                count(*) AS total,
                count(*) FILTER (WHERE bundle_sync_status = 'pending') AS pending,
                count(*) FILTER (WHERE bundle_sync_status = 'uploading') AS uploading,
                count(*) FILTER (WHERE bundle_sync_status = 'done') AS done,
                count(*) FILTER (WHERE bundle_sync_status = 'failed') AS failed
         FROM hub_sessions
         WHERE runtime_owner_id IS NOT NULL
         GROUP BY runtime_owner_id
         ORDER BY runtime_owner_id",
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|error| {
        tracing::warn!(%error, "load bundle sync status failed");
        ApiError::internal("load bundle sync status failed")
    })?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(runtime_id, total, pending, uploading, done, failed)| BundleSyncStatusResponse {
                    runtime_id,
                    total,
                    pending,
                    uploading,
                    done,
                    failed,
                },
            )
            .collect(),
    ))
}

/// runtime 更新会话 bundle 打包同步状态（pending/uploading/done/failed）。
#[derive(Debug, Deserialize)]
pub(crate) struct SetBundleSyncStatusRequest {
    pub(crate) status: String,
}

pub(crate) async fn set_session_bundle_sync_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<SetBundleSyncStatusRequest>,
) -> Result<StatusCode, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    if !matches!(
        req.status.as_str(),
        "pending" | "uploading" | "done" | "failed"
    ) {
        return Err(ApiError::bad_request(
            "bundle sync status must be pending/uploading/done/failed",
        ));
    }
    let updated = sqlx::query(
        "UPDATE hub_sessions
         SET bundle_sync_status = $1, bundle_sync_updated_at = now()
         WHERE id = $2 AND runtime_owner_id = $3",
    )
    .bind(&req.status)
    .bind(session_id)
    .bind(runtime_id)
    .execute(&state.pool)
    .await
    .map_err(|error| {
        tracing::warn!(%error, %session_id, "update bundle sync status failed");
        ApiError::internal("update bundle sync status failed")
    })?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::not_found("session is not owned by this runtime"));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// 会话重建事件（恢复用）：返回该会话用于重建 Pi 会话 jsonl 的全部事件，
/// 按事件序号有序。数据源 = run_events 的对话消息（message user/assistant）、
/// 内置工具调用与结果（item dynamicToolCall completed，含 Pi 原始 call|item id
/// 与完整 output）、integration 工具请求与结果（tool_request + client_tool_result，
/// tool_call_id 为 Hub UUID）、模型元数据（model_request/usage，供 assistant
/// 行还原 provider/model/usage）。会话历史以 DB 为唯一事实源。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SessionReplayEventDto {
    pub(crate) seq: i64,
    pub(crate) run_id: Uuid,
    pub(crate) hub_turn_id: Option<Uuid>,
    pub(crate) event_type: String,
    pub(crate) role: Option<String>,
    pub(crate) content: Option<String>,
    pub(crate) payload: Value,
    pub(crate) created_at: DateTime<Utc>,
}

pub(crate) async fn get_session_replay_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<Json<Vec<SessionReplayEventDto>>, ApiError> {
    let runtime_id = require_runtime(&state, &headers).await?;
    let ownership_generation =
        parse_required_header::<i64>(&headers, "x-agent-hub-ownership-generation")?;
    validate_ownership_generation(ownership_generation)?;
    let owned = sqlx::query(
        "SELECT runtime_owner_id, ownership_generation FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(&state.pool)
    .await?;
    if owned.as_ref().and_then(|row| row.get("runtime_owner_id")) != Some(runtime_id)
        || owned
            .as_ref()
            .and_then(|row| row.get("ownership_generation"))
            != Some(ownership_generation)
    {
        return Err(
            ApiError::forbidden("runtime does not own this Session generation")
                .with_code("stale_session_generation"),
        );
    }
    let rows: Vec<(
        i64,
        Uuid,
        Option<Uuid>,
        String,
        Option<String>,
        Option<String>,
        Value,
        DateTime<Utc>,
    )> = sqlx::query_as(
        "SELECT e.seq, e.run_id, r.hub_turn_id, e.event_type, e.role, e.content,
                e.payload, e.created_at
         FROM run_events e
         JOIN runs r ON r.id = e.run_id
         JOIN hub_sessions hs ON hs.id = r.hub_session_id
         WHERE r.hub_session_id = $1
           AND hs.runtime_owner_id = $2
           AND hs.ownership_generation = $3
           AND (
               (e.event_type = 'message' AND e.role IN ('user', 'assistant'))
               OR (e.event_type = 'item'
                   AND e.payload->>'item_type' IN ('dynamicToolCall', 'commandExecution')
                   AND e.payload->>'phase' = 'completed')
               OR e.event_type IN ('tool_request', 'client_tool_result', 'tool_result',
                                   'model_request', 'usage')
           )
         ORDER BY e.seq",
    )
    .bind(session_id)
    .bind(runtime_id)
    .bind(ownership_generation)
    .fetch_all(&state.pool)
    .await
    .map_err(|error| {
        tracing::warn!(%error, %session_id, "load session replay events failed");
        ApiError::internal("load session replay events failed")
    })?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(seq, run_id, hub_turn_id, event_type, role, content, payload, created_at)| {
                    SessionReplayEventDto {
                        seq,
                        run_id,
                        hub_turn_id,
                        event_type,
                        role,
                        content,
                        payload,
                        created_at,
                    }
                },
            )
            .collect(),
    ))
}

pub(crate) async fn runtime_append_event(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<RuntimeSessionWriteRequest<AppendRunEventRequest>>,
) -> Result<Json<RunEventDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    if req.payload.event_type == "tool_request" {
        return Err(ApiError::bad_request(
            "tool requests must use atomic batch finalize",
        ));
    }
    // 写事件前先回收过期 runtime，避免超时 token 继续写入已失败的 run。
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let AppendRunEventRequest {
        event_id,
        event_type,
        role,
        content,
        payload,
        waiting_tool,
    } = req.payload;
    if run_event_bus::is_streaming_delta(&event_type, &payload) {
        if waiting_tool.is_some() {
            return Err(ApiError::bad_request(
                "waiting tool state is only valid for atomic batch finalize",
            ));
        }
        // Deltas exist only for the live stream: validate ownership and the
        // active Run, then fan out through the bus without persisting.
        let mut tx = state.pool.begin().await?;
        let session_id =
            lock_owned_session_for_run_tx(&mut tx, run_id, runtime_id, req.ownership_generation)
                .await?;
        let active: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM runs
             WHERE id = $1 AND runtime_id = $2
               AND session_ownership_generation = $3
               AND hub_session_id = $4 AND status = 'running'
             FOR UPDATE",
        )
        .bind(run_id)
        .bind(runtime_id)
        .bind(req.ownership_generation)
        .bind(session_id)
        .fetch_optional(&mut *tx)
        .await?;
        if active.is_none() {
            let actual_run: Option<(String, i64, Option<Uuid>)> = sqlx::query_as(
                "SELECT status, session_ownership_generation, runtime_id FROM runs WHERE id = $1",
            )
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await
            .ok()
            .flatten();
            let actual_session: Option<(Option<Uuid>, i64, String)> = sqlx::query_as(
                "SELECT runtime_owner_id, ownership_generation, lifecycle_status
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(session_id)
            .fetch_optional(&mut *tx)
            .await
            .ok()
            .flatten();
            tracing::warn!(
                %run_id,
                %session_id,
                %runtime_id,
                expected_generation = req.ownership_generation,
                run = ?actual_run,
                session = ?actual_session,
                event_type = %event_type,
                "Run is not active for streaming event append"
            );
            return Err(ApiError::conflict("Run is not active for event append"));
        }
        tx.commit().await?;
        let event = RunEventDto {
            seq: state.run_event_bus.next_stream_seq(run_id),
            event_id,
            run_id,
            event_type,
            role,
            content,
            payload,
            created_at: chrono::Utc::now(),
        };
        state.run_event_bus.publish(run_id, event.clone(), false);
        return Ok(Json(event));
    }
    // Streaming deltas never persist, so the size guard only applies to events
    // that will be written into run_events.
    let content_bytes = content.as_deref().map(str::len).unwrap_or(0);
    let payload_bytes = serde_json::to_string(&payload)
        .map_err(|_| ApiError::internal("run event payload could not be encoded"))?
        .len();
    if content_bytes.saturating_add(payload_bytes) > MAX_RUNTIME_EVENT_BYTES {
        return Err(ApiError::bad_request("run event exceeds its size limit"));
    }
    let mut tx = state.pool.begin().await?;
    let event = insert_run_event_for_active_runtime(
        &mut tx,
        run_id,
        runtime_id,
        req.ownership_generation,
        AppendRunEventRequest {
            event_id: Uuid::new_v4(),
            event_type,
            role,
            content,
            payload,
            waiting_tool,
        },
    )
    .await?;
    state.run_event_bus.publish(run_id, event.clone(), true);
    tx.commit().await?;
    Ok(Json(event))
}

pub(crate) async fn runtime_begin_turn(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<RuntimeSessionWriteRequest<BeginRuntimeTurnRequest>>,
) -> Result<Json<BeginRuntimeTurnResponse>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    validate_execution_configuration_fingerprint(&req.payload.configuration_fingerprint)?;
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let session_id =
        lock_owned_session_for_run_tx(&mut tx, run_id, runtime_id, req.ownership_generation)
            .await?;
    let run: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT hub_turn_id, status
         FROM runs
         WHERE id = $1 AND runtime_id = $2 AND hub_session_id = $3
           AND session_ownership_generation = $4
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(runtime_id)
    .bind(session_id)
    .bind(req.ownership_generation)
    .fetch_optional(&mut *tx)
    .await?;
    let (turn_id, run_status) = run.ok_or(ApiError::forbidden(
        "runtime does not own the Run for this Session generation",
    ))?;
    if run_status != "running" {
        return Err(ApiError::conflict("Run is not active for Turn begin"));
    }
    let session: (Option<String>, Option<Uuid>) = sqlx::query_as(
        "SELECT configuration_fingerprint, active_turn_id
         FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await?;
    let turn = sqlx::query(
        "SELECT status, delivery_started_at, native_turn_id, configuration_fingerprint
         FROM hub_session_turns
         WHERE id = $1 AND session_id = $2 AND ownership_generation = $3
         FOR UPDATE",
    )
    .bind(turn_id)
    .bind(session_id)
    .bind(req.ownership_generation)
    .fetch_optional(&mut *tx)
    .await?;
    let turn = turn.ok_or(ApiError::conflict(
        "Hub Turn does not match the owned Session generation",
    ))?;
    let turn_status = turn.get::<String, _>("status");
    let delivery_started_at = turn.get::<Option<DateTime<Utc>>, _>("delivery_started_at");
    let native_turn_id = turn.get::<Option<String>, _>("native_turn_id");
    let turn_fingerprint = turn.get::<Option<String>, _>("configuration_fingerprint");
    if native_turn_id.is_some() || (turn_status != "pending" && turn_status != "starting") {
        return Err(ApiError::conflict("Hub Turn has already started"));
    }
    if delivery_started_at.is_none() {
        if session.1.is_some() {
            return Err(ApiError::conflict("Session already has an active Turn"));
        }
        let session_updated = sqlx::query(
            "UPDATE hub_sessions
             SET configuration_fingerprint = $1
             WHERE id = $2 AND runtime_owner_id = $3 AND ownership_generation = $4
               AND active_turn_id IS NULL",
        )
        .bind(&req.payload.configuration_fingerprint)
        .bind(session_id)
        .bind(runtime_id)
        .bind(req.ownership_generation)
        .execute(&mut *tx)
        .await?;
        if session_updated.rows_affected() != 1 {
            return Err(ApiError::conflict(
                "Session configuration binding lost its ownership generation",
            ));
        }
        sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'starting', configuration_fingerprint = $1,
                 delivery_started_at = now(), updated_at = now()
             WHERE id = $2 AND session_id = $3 AND ownership_generation = $4
               AND delivery_started_at IS NULL",
        )
        .bind(&req.payload.configuration_fingerprint)
        .bind(turn_id)
        .bind(session_id)
        .bind(req.ownership_generation)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE hub_session_messages
             SET delivery_state = 'delivering'
             WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
               AND delivery_mode = 'next_turn' AND delivery_state = 'queued'",
        )
        .bind(session_id)
        .bind(turn_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
    } else if session.0.as_deref() != Some(&req.payload.configuration_fingerprint)
        || turn_fingerprint.as_deref() != Some(&req.payload.configuration_fingerprint)
    {
        return Err(ApiError::conflict(
            "Turn begin configuration fingerprint changed after synchronization",
        ));
    }
    let rows = sqlx::query(
        "SELECT id, session_id, sequence, role, message_kind, content, payload,
                delivery_mode, delivery_state, client_message_key,
                expected_native_turn_id, turn_id, run_id, accepted_at
         FROM hub_session_messages
         WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
           AND delivery_mode = 'next_turn' AND delivery_state = 'delivering'
         ORDER BY sequence",
    )
    .bind(session_id)
    .bind(turn_id)
    .bind(run_id)
    .fetch_all(&mut *tx)
    .await?;
    let mut messages = rows
        .into_iter()
        .map(hub_message_from_row)
        .collect::<Vec<_>>();
    fill_message_attachments(&mut *tx, &mut messages).await?;
    tx.commit().await?;
    Ok(Json(BeginRuntimeTurnResponse {
        session_id,
        turn_id,
        ownership_generation: req.ownership_generation,
        configuration_fingerprint: req.payload.configuration_fingerprint,
        messages,
    }))
}

pub(crate) async fn runtime_complete_session_command(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((session_id, command_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<RuntimeSessionWriteRequest<CompleteRuntimeSessionCommandRequest>>,
) -> Result<Json<CompleteRuntimeSessionCommandResponse>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    match req.payload.command.as_str() {
        "steer"
            if !matches!(
                req.payload.outcome.as_str(),
                "applied" | "turn_ended" | "failed"
            ) =>
        {
            return Err(ApiError::bad_request("invalid steer command outcome"));
        }
        "interrupt" if !matches!(req.payload.outcome.as_str(), "interrupted" | "turn_ended") => {
            return Err(ApiError::bad_request("invalid interrupt command outcome"));
        }
        "refresh_configuration"
            if !matches!(req.payload.outcome.as_str(), "applied" | "failed") =>
        {
            return Err(ApiError::bad_request(
                "invalid refresh configuration outcome",
            ));
        }
        "steer" | "interrupt" | "refresh_configuration" => {}
        _ => {
            return Err(ApiError::bad_request(
                "unsupported Session command completion",
            ));
        }
    }
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    if req.payload.command == "refresh_configuration" {
        let requested_revision = req
            .payload
            .revision
            .ok_or(ApiError::bad_request("configuration revision is required"))?;
        let requested_fingerprint =
            req.payload
                .fingerprint
                .as_deref()
                .ok_or(ApiError::bad_request(
                    "configuration fingerprint is required",
                ))?;
        validate_execution_configuration_fingerprint(requested_fingerprint)?;
        let agent_id: Uuid = sqlx::query_scalar(
            "SELECT agent_id FROM hub_sessions
             WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3",
        )
        .bind(session_id)
        .bind(runtime_id)
        .bind(req.ownership_generation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::forbidden(
            "runtime does not own this Session generation",
        ))?;
        sqlx::query("SELECT id FROM agents WHERE id = $1 AND deleted_at IS NULL FOR SHARE")
            .bind(agent_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ApiError::not_found("agent not found"))?;
        let mut configuration = load_agent_execution_configuration_tx(&mut tx, agent_id).await?;
        apply_session_tool_policy_to_configuration_tx(&mut tx, session_id, &mut configuration)
            .await?;
        let current_fingerprint = execution_configuration_fingerprint(&configuration)
            .map_err(|error| ApiError::internal(error.to_string()))?;
        let target_revision: i64 = sqlx::query_scalar(
            "SELECT configuration_refresh_revision
             FROM hub_sessions
             WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3
             FOR UPDATE",
        )
        .bind(session_id)
        .bind(runtime_id)
        .bind(req.ownership_generation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::forbidden(
            "runtime does not own this Session generation",
        ))?;
        if req.payload.outcome == "applied"
            && requested_revision == target_revision
            && requested_revision == configuration.revision
            && requested_fingerprint == current_fingerprint
        {
            sqlx::query(
                "UPDATE hub_sessions
                 SET configuration_applied_revision = GREATEST(
                         configuration_applied_revision,
                         $1
                     ),
                     configuration_fingerprint = $2
                 WHERE id = $3 AND runtime_owner_id = $4
                   AND ownership_generation = $5
                   AND configuration_refresh_revision = $1",
            )
            .bind(requested_revision)
            .bind(requested_fingerprint)
            .bind(session_id)
            .bind(runtime_id)
            .bind(req.ownership_generation)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        return Ok(Json(CompleteRuntimeSessionCommandResponse {
            command_id,
            outcome: req.payload.outcome,
        }));
    }
    let session: Option<Option<String>> = sqlx::query_scalar(
        "SELECT configuration_fingerprint
         FROM hub_sessions
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3
         FOR UPDATE",
    )
    .bind(session_id)
    .bind(runtime_id)
    .bind(req.ownership_generation)
    .fetch_optional(&mut *tx)
    .await?;
    let configuration_fingerprint = session.ok_or(ApiError::forbidden(
        "runtime does not own this Session generation",
    ))?;
    if req.payload.command == "interrupt" {
        let turn = sqlx::query(
            "SELECT interrupt_requested_at
             FROM hub_session_turns
             WHERE id = $1 AND session_id = $2 AND ownership_generation = $3
             FOR UPDATE",
        )
        .bind(command_id)
        .bind(session_id)
        .bind(req.ownership_generation)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::forbidden(
            "interrupt command does not belong to this Session generation",
        ))?;
        if turn
            .get::<Option<DateTime<Utc>>, _>("interrupt_requested_at")
            .is_none()
        {
            return Err(ApiError::conflict("Session Turn has no interrupt request"));
        }
        sqlx::query(
            "UPDATE hub_session_turns
             SET interrupt_acknowledged_at = COALESCE(interrupt_acknowledged_at, now()),
                 updated_at = now()
             WHERE id = $1 AND session_id = $2 AND ownership_generation = $3",
        )
        .bind(command_id)
        .bind(session_id)
        .bind(req.ownership_generation)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(Json(CompleteRuntimeSessionCommandResponse {
            command_id,
            outcome: req.payload.outcome,
        }));
    }
    let message = sqlx::query(
        "SELECT delivery_mode, delivery_state, expected_native_turn_id,
                turn_id, run_id, content
         FROM hub_session_messages
         WHERE id = $1 AND session_id = $2
         FOR UPDATE",
    )
    .bind(command_id)
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("Session command not found"))?;
    let delivery_mode: String = message.get("delivery_mode");
    let delivery_state: String = message.get("delivery_state");
    let old_run_id: Option<Uuid> = message.get("run_id");

    match req.payload.outcome.as_str() {
        "applied" if delivery_state == "delivered" => {}
        "applied" => {
            if delivery_mode != "steer"
                || (delivery_state != "queued" && delivery_state != "delivering")
            {
                return Err(ApiError::conflict(
                    "Steering Message is not awaiting delivery",
                ));
            }
            sqlx::query(
                "UPDATE hub_session_messages
                 SET delivery_state = 'delivered'
                 WHERE id = $1 AND session_id = $2",
            )
            .bind(command_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
        }
        "failed" if delivery_state == "failed" => {}
        "failed" => {
            if delivery_mode != "steer"
                || (delivery_state != "queued" && delivery_state != "delivering")
            {
                return Err(ApiError::conflict(
                    "Steering Message is not awaiting delivery",
                ));
            }
            sqlx::query(
                "UPDATE hub_session_messages
                 SET delivery_state = 'failed'
                 WHERE id = $1 AND session_id = $2",
            )
            .bind(command_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
        }
        "turn_ended" if delivery_mode == "next_turn" && delivery_state == "queued" => {}
        "turn_ended" => {
            if delivery_mode != "steer"
                || (delivery_state != "queued" && delivery_state != "delivering")
            {
                return Err(ApiError::conflict(
                    "Steering Message is not awaiting fallback",
                ));
            }
            let old_run_id = old_run_id.ok_or(ApiError::conflict(
                "Steering Message is missing its active Run",
            ))?;
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
            .fetch_optional(&mut *tx)
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
                     VALUES ($1, $2, 'pending', $3, $4)",
                )
                .bind(next_turn_id)
                .bind(session_id)
                .bind(configuration_fingerprint.as_deref())
                .bind(req.ownership_generation)
                .execute(&mut *tx)
                .await?;
                let inserted = sqlx::query(
                    "INSERT INTO runs
                         (id, agent_id, owner_id, status, initial_message, source,
                          model_subject_type, model_subject_user_id,
                          model_source_integration_app_id,
                          automation_id, integration_session_id, parent_run_id,
                          widget_session_id, external_user_context,
                          hub_session_id, hub_message_id, hub_turn_id,
                          session_ownership_generation)
                     SELECT $1, agent_id, owner_id, 'pending', $2, source,
                            model_subject_type, model_subject_user_id,
                            model_source_integration_app_id,
                            automation_id, integration_session_id, id,
                            widget_session_id, external_user_context,
                            hub_session_id, $3, $4, $5
                     FROM runs WHERE id = $6 AND hub_session_id = $7",
                )
                .bind(next_run_id)
                .bind(
                    message
                        .get::<Option<String>, _>("content")
                        .unwrap_or_default(),
                )
                .bind(command_id)
                .bind(next_turn_id)
                .bind(req.ownership_generation)
                .bind(old_run_id)
                .bind(session_id)
                .execute(&mut *tx)
                .await?;
                if inserted.rows_affected() != 1 {
                    return Err(ApiError::conflict(
                        "active Run disappeared before Steering Message fallback",
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
                     updated_at = now()
                 FROM runs AS previous
                 WHERE next.id = $1 AND previous.id = $2
                   AND next.hub_session_id = $3 AND previous.hub_session_id = $3",
            )
            .bind(next_run_id)
            .bind(old_run_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE hub_session_messages
                 SET delivery_mode = 'next_turn', delivery_state = 'queued',
                     expected_native_turn_id = NULL, turn_id = $1, run_id = $2
                 WHERE id = $3 AND session_id = $4",
            )
            .bind(next_turn_id)
            .bind(next_run_id)
            .bind(command_id)
            .bind(session_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE integration_messages SET run_id = $1
                 WHERE hub_message_id = $2 AND run_id = $3",
            )
            .bind(next_run_id)
            .bind(command_id)
            .bind(old_run_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE integration_attachments SET run_id = $1
                 WHERE hub_message_id = $2 AND run_id = $3",
            )
            .bind(next_run_id)
            .bind(command_id)
            .bind(old_run_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE runs SET hub_message_id = NULL, updated_at = now()
                 WHERE id = $1 AND hub_message_id = $2",
            )
            .bind(old_run_id)
            .bind(command_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE runs
                 SET hub_message_id = COALESCE(hub_message_id, $1), updated_at = now()
                 WHERE id = $2",
            )
            .bind(command_id)
            .bind(next_run_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE run_events SET run_id = $1
                 WHERE hub_message_id = $2 AND run_id = $3",
            )
            .bind(next_run_id)
            .bind(command_id)
            .bind(old_run_id)
            .execute(&mut *tx)
            .await?;
        }
        _ => unreachable!(),
    }
    tx.commit().await?;
    Ok(Json(CompleteRuntimeSessionCommandResponse {
        command_id,
        outcome: req.payload.outcome,
    }))
}

pub(crate) async fn runtime_finalize_tool_requests(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<RuntimeSessionWriteRequest<FinalizeToolRequestsRequest>>,
) -> Result<Json<RunDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    let requests = parse_tool_request_batch(&req.payload)?;
    let fingerprint = tool_request_batch_fingerprint(run_id, &req.payload)?;
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let run = finalize_tool_request_batch_tx(
        &mut tx,
        run_id,
        runtime_id,
        req.ownership_generation,
        &req.payload,
        &requests,
        &fingerprint,
    )
    .await?;
    tx.commit().await?;
    Ok(Json(run))
}

/// 防御（报错防御方案）：runtime 领取任务时发现该会话有活动冲突（异常派发），
/// 调用本端点把冲突任务拒绝掉——只动 B，绝不动被替代任务 A / 回合 / 会话状态。
/// 3a 主路径（A 可验证）：A≠B 且 A 仍是当前版本号活动任务 →
///   不同回合：B failed + 事件 + B 回合终态 + B 的 queued 消息迁移（下一 run 领取，仅一次）；
///   同回合（异常复用）：B failed + 事件，B 的 steer 插话保持归 A，next_turn queued 迁移。
/// 3b 兜底（A 无法验证）：只 B failed + 幂等事件，不动共享回合/session/message，
///   B 的 queued 消息迁移（不滞留，事件即对账记录）。
/// 幂等：B 已终态 → 返回现有状态（不 409）。
pub(crate) async fn runtime_reject_claimed_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<RuntimeSessionWriteRequest<RejectClaimedRunRequest>>,
) -> Result<Json<RunDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let owned_session_id =
        lock_owned_session_for_run_tx(&mut tx, run_id, runtime_id, req.ownership_generation)
            .await?;
    let current = sqlx::query(
        "SELECT runs.status, runs.hub_session_id, runs.hub_turn_id
         FROM runs
         WHERE runs.id = $1 AND runs.runtime_id = $2
           AND runs.session_ownership_generation = $3
           AND runs.hub_session_id = $4
         FOR UPDATE OF runs",
    )
    .bind(run_id)
    .bind(runtime_id)
    .bind(req.ownership_generation)
    .bind(owned_session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(
        ApiError::forbidden("runtime does not own an active run")
            .with_code("stale_session_generation"),
    )?;
    let current_status: String = current.get("status");
    let hub_session_id: Uuid = current.get("hub_session_id");
    let hub_turn_id: Uuid = current.get("hub_turn_id");
    if current_status != "running" {
        // 幂等：B 已终态（异常派发早已被处理）→ 返回现有状态。
        let run = load_run_public_tx(&mut tx, run_id).await?;
        tx.commit().await?;
        return Ok(Json(run));
    }

    // A 可验证性（3a 主路径 vs 3b 兜底）。
    let incumbent: Option<(Uuid, Uuid, String)> = match req.payload.incumbent_run_id {
        Some(incumbent_run_id) if incumbent_run_id != run_id => {
            sqlx::query_as(
                "SELECT id, hub_turn_id, status FROM runs
             WHERE id = $1 AND hub_session_id = $2
             FOR UPDATE",
            )
            .bind(incumbent_run_id)
            .bind(hub_session_id)
            .fetch_optional(&mut *tx)
            .await?
        }
        _ => None,
    };
    let same_turn = incumbent
        .as_ref()
        .is_some_and(|(_, incumbent_turn, _)| *incumbent_turn == hub_turn_id);

    // B 失败 + 幂等事件（3a 与 3b 共用）。
    sqlx::query("UPDATE runs SET status = 'failed', updated_at = now() WHERE id = $1")
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
    insert_run_event_tx(
        &mut tx,
        run_id,
        "status".into(),
        None,
        Some("failed".into()),
        json!({
            "status": "failed",
            "reason": if same_turn {
                "run was rejected: it illegally reused the active Turn of its incumbent"
            } else {
                "run was rejected: session already had an active run (anomalous dispatch)"
            }
        }),
    )
    .await?;

    if let Some((incumbent_run_id, incumbent_turn_id, incumbent_status)) = incumbent {
        // 3a 主路径：A 仍是活动任务（running/waiting_tool）。
        if matches!(incumbent_status.as_str(), "running" | "waiting_tool") {
            if same_turn {
                // 同回合（异常复用 A 的回合）：B 的回合共享 A——不终态回合；
                // B 的 steer 插话归 A（run_id 更新到 A，回合不变，A 继续处理）；
                // B 的 next_turn queued 迁移到 A。
                sqlx::query(
                    "UPDATE hub_session_messages
                     SET run_id = $2
                     WHERE session_id = $1 AND run_id = $3
                       AND delivery_mode = 'steer' AND delivery_state = 'queued'",
                )
                .bind(hub_session_id)
                .bind(incumbent_run_id)
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE hub_session_messages
                     SET delivery_state = 'queued', turn_id = $2, run_id = $3
                     WHERE session_id = $1 AND run_id = $4
                       AND delivery_mode = 'next_turn' AND delivery_state = 'queued'",
                )
                .bind(hub_session_id)
                .bind(incumbent_turn_id)
                .bind(incumbent_run_id)
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
            } else {
                // 不同回合：终态 B 的回合，B 的 queued 消息迁移到下一 run（仅一次）。
                sqlx::query(
                    "UPDATE hub_session_turns
                     SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
                     WHERE id = $1 AND session_id = $2",
                )
                .bind(hub_turn_id)
                .bind(hub_session_id)
                .execute(&mut *tx)
                .await?;
                // next_turn 模式先转为 steer，统一走迁移路径（迁移后转回 next_turn）。
                sqlx::query(
                    "UPDATE hub_session_messages
                     SET delivery_mode = 'steer', expected_native_turn_id = 'reject-migration'
                     WHERE session_id = $1 AND run_id = $2
                       AND delivery_mode = 'next_turn' AND delivery_state = 'queued'",
                )
                .bind(hub_session_id)
                .bind(run_id)
                .execute(&mut *tx)
                .await?;
                move_queued_steers_to_next_turn_tx(
                    &mut tx,
                    hub_session_id,
                    run_id,
                    hub_turn_id,
                    req.ownership_generation,
                )
                .await?;
            }
            let _ = incumbent_run_id;
        } else {
            // A 已终态：B 的回合终态 + 消息迁移（B 不再滞留）。
            sqlx::query(
                "UPDATE hub_session_turns
                 SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
                 WHERE id = $1 AND session_id = $2",
            )
            .bind(hub_turn_id)
            .bind(hub_session_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE hub_session_messages
                 SET delivery_mode = 'steer', expected_native_turn_id = 'reject-migration'
                 WHERE session_id = $1 AND run_id = $2
                   AND delivery_mode = 'next_turn' AND delivery_state = 'queued'",
            )
            .bind(hub_session_id)
            .bind(run_id)
            .execute(&mut *tx)
            .await?;
            move_queued_steers_to_next_turn_tx(
                &mut tx,
                hub_session_id,
                run_id,
                hub_turn_id,
                req.ownership_generation,
            )
            .await?;
        }
    } else {
        // 3b 兜底（A 无法验证）：只失败 B + 消息迁移（事件即对账记录）。
        sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
             WHERE id = $1 AND session_id = $2",
        )
        .bind(hub_turn_id)
        .bind(hub_session_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE hub_session_messages
             SET delivery_mode = 'steer', expected_native_turn_id = 'reject-migration'
             WHERE session_id = $1 AND run_id = $2
               AND delivery_mode = 'next_turn' AND delivery_state = 'queued'",
        )
        .bind(hub_session_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        move_queued_steers_to_next_turn_tx(
            &mut tx,
            hub_session_id,
            run_id,
            hub_turn_id,
            req.ownership_generation,
        )
        .await?;
    }
    let run = load_run_public_tx(&mut tx, run_id).await?;
    tx.commit().await?;
    Ok(Json(run))
}

pub(crate) async fn runtime_complete_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<RuntimeSessionWriteRequest<CompleteRunRequest>>,
) -> Result<Json<RunDto>, ApiError> {
    validate_ownership_generation(req.ownership_generation)?;
    let status = match req.payload.status.as_str() {
        "completed" => "completed",
        "failed" => "failed",
        "interrupted" => "interrupted",
        "waiting_tool" => "waiting_tool",
        _ => return Err(ApiError::bad_request("invalid run completion status")),
    };
    // 完成 run 前也执行同一套在线性检查，避免离线 runtime 改写最终状态。
    reap_stale_runtimes(&state.pool).await?;
    let runtime_id = require_runtime(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let owned_session_id =
        lock_owned_session_for_run_tx(&mut tx, run_id, runtime_id, req.ownership_generation)
            .await?;
    let current = sqlx::query(
        "SELECT runs.status, runs.native_session_id, runs.work_dir_ref,
                runs.hub_session_id, runs.hub_turn_id
         FROM runs
         WHERE runs.id = $1 AND runs.runtime_id = $2
           AND runs.session_ownership_generation = $3
           AND runs.hub_session_id = $4
         FOR UPDATE OF runs",
    )
    .bind(run_id)
    .bind(runtime_id)
    .bind(req.ownership_generation)
    .bind(owned_session_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(
        ApiError::forbidden("runtime does not own an active run")
            .with_code("stale_session_generation"),
    )?;
    let current_status: String = current.get("status");
    let current_native_session_id: Option<String> = current.get("native_session_id");
    let current_work_dir_ref: Option<String> = current.get("work_dir_ref");
    let hub_session_id: Uuid = current.get("hub_session_id");
    let hub_turn_id: Uuid = current.get("hub_turn_id");
    if status == "waiting_tool" && current_status == "running" {
        return Err(ApiError::conflict(
            "waiting tool runs must use atomic batch finalize",
        ));
    }
    if current_status != "running" && !(current_status == "waiting_tool" && status == "interrupted")
    {
        if current_status != status
            || current_native_session_id != req.payload.native_session_id
            || current_work_dir_ref != req.payload.work_dir_ref
        {
            return Err(ApiError::conflict(
                "run completion does not match existing terminal state",
            ));
        }
        let run = load_run_public_tx(&mut tx, run_id).await?;
        tx.commit().await?;
        return Ok(Json(run));
    }
    let turn = sqlx::query(
        "SELECT interrupt_requested_at
         FROM hub_session_turns
         WHERE id = $1 AND session_id = $2 AND ownership_generation = $3
         FOR UPDATE",
    )
    .bind(hub_turn_id)
    .bind(hub_session_id)
    .bind(req.ownership_generation)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::conflict(
        "Run Turn does not match the owned Session generation",
    ))?;
    let interrupt_requested_at: Option<DateTime<Utc>> = turn.get("interrupt_requested_at");
    if status == "interrupted" && interrupt_requested_at.is_none() {
        return Err(ApiError::conflict(
            "Run cannot complete as interrupted without a stop request",
        ));
    }
    let updated = sqlx::query(
        "UPDATE runs
         SET status = $1, native_session_id = $2, work_dir_ref = $3, updated_at = now()
         WHERE id = $4 AND runtime_id = $5
           AND (status = 'running' OR ($1 = 'interrupted' AND status = 'waiting_tool'))
           AND session_ownership_generation = $6
         RETURNING id",
    )
    .bind(status)
    .bind(req.payload.native_session_id)
    .bind(req.payload.work_dir_ref)
    .bind(run_id)
    .bind(runtime_id)
    .bind(req.ownership_generation)
    .fetch_one(&mut *tx)
    .await?;
    let _: Uuid = updated.get("id");
    insert_run_event_tx(
        &mut tx,
        run_id,
        "status".into(),
        None,
        Some(status.into()),
        json!({ "status": status }),
    )
    .await?;
    if matches!(status, "completed" | "failed" | "interrupted") {
        move_queued_steers_to_next_turn_tx(
            &mut tx,
            hub_session_id,
            run_id,
            hub_turn_id,
            req.ownership_generation,
        )
        .await?;
    }
    sqlx::query(
        "UPDATE hub_session_turns
         SET status = $1, ended_at = COALESCE(ended_at, now()), updated_at = now()
         WHERE id = $2 AND session_id = $3 AND ownership_generation = $4",
    )
    .bind(status)
    .bind(hub_turn_id)
    .bind(hub_session_id)
    .bind(req.ownership_generation)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE hub_sessions AS sessions
         SET runtime_owner_id = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 ELSE sessions.runtime_owner_id
             END,
             ownership_generation = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring'
                 THEN sessions.ownership_generation + 1
                 ELSE sessions.ownership_generation
             END,
             active_turn_id = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 WHEN active_turn_id = $4 THEN NULL
                 ELSE active_turn_id
             END,
             lifecycle_status = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring'
                 THEN 'recovery_failed'
                 WHEN runtimes.status = 'draining' THEN 'saving'
                 ELSE 'online'
             END,
             recovery_error = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring'
                 THEN 'Session Bundle restore failed on its assigned Runtime'
                 ELSE sessions.recovery_error
             END,
             recovery_source = CASE
                 WHEN sessions.lifecycle_status = 'restoring' THEN NULL
                 ELSE sessions.recovery_source
             END,
             saving_history_checkpoint = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 WHEN runtimes.status = 'draining' THEN COALESCE(
                     sessions.saving_history_checkpoint, sessions.history_checkpoint
                 )
                 ELSE NULL
             END,
             saving_ownership_generation = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 WHEN runtimes.status = 'draining' THEN COALESCE(
                     sessions.saving_ownership_generation, sessions.ownership_generation
                 )
                 ELSE NULL
             END,
             saving_reason = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 WHEN runtimes.status = 'draining' THEN 'drain'
                 ELSE NULL
             END,
             saving_checkpoint_attempt_id = CASE
                 WHEN $5 = 'failed' AND sessions.lifecycle_status = 'restoring' THEN NULL
                 WHEN runtimes.status = 'draining' THEN COALESCE(
                     sessions.saving_checkpoint_attempt_id, gen_random_uuid()
                 )
                 ELSE NULL
             END,
             last_checkpoint_attempt_id = CASE
                 WHEN runtimes.status = 'draining' THEN NULL
                 ELSE last_checkpoint_attempt_id
             END,
             last_checkpoint_ownership_generation = CASE
                 WHEN runtimes.status = 'draining' THEN NULL
                 ELSE last_checkpoint_ownership_generation
             END,
             last_checkpoint_disposition = CASE
                 WHEN runtimes.status = 'draining' THEN NULL
                 ELSE last_checkpoint_disposition
             END,
             last_checkpoint_has_queued_work = CASE
                 WHEN runtimes.status = 'draining' THEN NULL
                 ELSE last_checkpoint_has_queued_work
             END
         FROM runtimes
         WHERE sessions.id = $1
           AND sessions.runtime_owner_id = runtimes.id
           AND runtimes.id = $2
           AND sessions.ownership_generation = $3
           AND (sessions.active_turn_id = $4 OR sessions.active_turn_id IS NULL)",
    )
    .bind(hub_session_id)
    .bind(runtime_id)
    .bind(req.ownership_generation)
    .bind(hub_turn_id)
    .bind(status)
    .execute(&mut *tx)
    .await?;
    let run = load_run_public_tx(&mut tx, run_id).await?;
    tx.commit().await?;
    Ok(Json(run))
}

pub(crate) async fn runtime_model_proxy(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    headers: HeaderMap,
    Path(path): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    if !model_proxy_path_supported(&path) {
        return Err(ApiError::not_found("unsupported model proxy path"));
    }
    let run_id = headers
        .get("x-agent-hub-run-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiError::bad_request("missing run id"))?;
    let model_binding_id = headers
        .get(MODEL_PROXY_BINDING_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiError::bad_request("missing model binding id"))?;
    let request_json: Value = serde_json::from_slice(&body)
        .map_err(|_| ApiError::bad_request("model request must be JSON"))?;
    let request_model_id = request_json
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ApiError::bad_request(
            "model request must include a model id",
        ))?;
    let resolved = resolve_model_proxy_request(&state, &headers, run_id, model_binding_id).await?;
    if request_model_id != resolved.model_id {
        return Err(ApiError::bad_request(
            "request model does not match the selected Model Connection",
        ));
    }
    let vision_request = headers
        .get(VISION_PROXY_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "1");
    let mut forwarded_body = body;
    if vision_request {
        if let Some(vision_model_id) = resolved.vision_model_id.as_deref() {
            let mut vision_request_json = request_json;
            vision_request_json["model"] = Value::String(vision_model_id.to_owned());
            forwarded_body = Bytes::from(
                serde_json::to_vec(&vision_request_json)
                    .map_err(|_| ApiError::internal("model request could not be re-encoded"))?,
            );
        }
    }
    let request_settings = resolved.accounting.request_settings.clone();
    proxy_model_request_to_upstream_with_options(
        &state,
        ModelProxyForwardRequest {
            upstream_url: resolved.upstream_url,
            upstream_protocol: resolved.accounting.api_type,
            request_settings,
            path,
            query: uri.query().map(str::to_owned),
            headers,
            body: forwarded_body,
            api_key: resolved.api_key,
            accounting: Some(resolved.accounting),
        },
    )
    .await
}

struct ResolvedModelProxyRequest {
    upstream_url: String,
    model_id: String,
    vision_model_id: Option<String>,
    api_key: Zeroizing<String>,
    accounting: ModelProxyAccountingContext,
}

#[derive(Clone)]
struct ModelProxyAccountingContext {
    request_id: Uuid,
    model_connection_id: Uuid,
    model_connection_scope: String,
    model_connection_name: String,
    model_id: String,
    api_type: ModelUpstreamProtocol,
    request_settings: ModelRequestSettings,
    agent_id: Uuid,
    agent_name: String,
    subject_type: String,
    subject_user_id: Option<Uuid>,
    subject_display_name: Option<String>,
    source_integration_app_id: Option<Uuid>,
    source_integration_app_name: Option<String>,
}

pub(crate) async fn resolve_model_proxy_request(
    state: &AppState,
    headers: &HeaderMap,
    run_id: Uuid,
    model_binding_id: Uuid,
) -> Result<ResolvedModelProxyRequest, ApiError> {
    let token = bearer_token(headers).ok_or(ApiError::unauthorized("missing model proxy token"))?;
    let row = sqlx::query(
        "SELECT c.id AS model_connection_id,
                binding.connection_scope_snapshot AS model_connection_scope,
                binding.connection_name_snapshot AS model_connection_name,
                c.base_url, c.vision_model_id,
                binding.model_id, binding.api_type,
                binding.model_settings->'request_settings' AS request_settings,
                c.api_key_ciphertext, c.api_key_nonce,
                a.id AS agent_id, a.name AS agent_name,
                r.model_subject_type,
                subject.id AS subject_user_id,
                CASE
                    WHEN r.model_subject_type = 'integration_app' THEN source_app.name
                    ELSE subject.display_name
                END AS subject_display_name,
                source_app.id AS source_integration_app_id,
                source_app.name AS source_integration_app_name
         FROM runs r
         JOIN runtimes rt ON rt.id = r.runtime_id
         JOIN hub_sessions hs
           ON hs.id = r.hub_session_id
          AND hs.active_turn_id = r.hub_turn_id
          AND hs.runtime_owner_id = r.runtime_id
          AND hs.ownership_generation = r.session_ownership_generation
         JOIN agents a ON a.id = r.agent_id AND a.deleted_at IS NULL
         LEFT JOIN users subject
           ON subject.id = COALESCE(
                  r.model_subject_user_id,
                  CASE WHEN r.model_subject_type = 'user' THEN r.owner_id END
              )
         JOIN run_model_bindings binding
           ON binding.run_id = r.id
          AND binding.id = $3
         JOIN model_connections c ON c.id = binding.model_connection_id
         LEFT JOIN embed_sessions widget ON widget.id = r.widget_session_id
         LEFT JOIN integration_sessions integration
           ON integration.id = r.integration_session_id
         LEFT JOIN oauth_apps source_app
           ON source_app.id = COALESCE(
                  r.model_source_integration_app_id,
                  widget.oauth_app_id,
                  integration.oauth_app_id
              )
         WHERE r.id = $1
           AND r.status = 'running'
           AND r.model_proxy_token_hash = $2
           AND rt.status IN ('online', 'draining')
           AND rt.last_heartbeat_at >= now() - interval '30 seconds'
           AND c.enabled = true
           AND c.deleted_at IS NULL
           AND c.base_url IS NOT NULL
           AND c.api_key_ciphertext IS NOT NULL
           AND c.api_key_nonce IS NOT NULL
           AND (c.scope = 'global' OR c.owner_id = a.owner_id)",
    )
    .bind(run_id)
    .bind(sha256_hex(&token))
    .bind(model_binding_id)
    .fetch_optional(&state.pool)
    .await?;
    let row = row.ok_or(ApiError::unauthorized("invalid model proxy request"))?;
    let ciphertext: Vec<u8> = row.get("api_key_ciphertext");
    let nonce: Vec<u8> = row.get("api_key_nonce");
    let api_key = Zeroizing::new(
        state
            .model_secret_cipher
            .decrypt(&ciphertext, &nonce)
            .map_err(|_| ApiError::internal("Model Connection credential is unavailable"))?,
    );
    let model_id: String = row.get("model_id");
    let api_type = model_upstream_protocol_from_name(&row.get::<String, _>("api_type"));
    let request_settings: ModelRequestSettings =
        serde_json::from_value(row.get("request_settings"))
            .expect("Run Model Binding request settings are constrained");
    Ok(ResolvedModelProxyRequest {
        upstream_url: row.get("base_url"),
        model_id: model_id.clone(),
        vision_model_id: row.get("vision_model_id"),
        api_key,
        accounting: ModelProxyAccountingContext {
            request_id: Uuid::new_v4(),
            model_connection_id: row.get("model_connection_id"),
            model_connection_scope: row.get("model_connection_scope"),
            model_connection_name: row.get("model_connection_name"),
            model_id,
            api_type,
            request_settings,
            agent_id: row.get("agent_id"),
            agent_name: row.get("agent_name"),
            subject_type: row.get("model_subject_type"),
            subject_user_id: row.get("subject_user_id"),
            subject_display_name: row.get("subject_display_name"),
            source_integration_app_id: row.get("source_integration_app_id"),
            source_integration_app_name: row.get("source_integration_app_name"),
        },
    })
}

pub(crate) fn model_proxy_path_supported(path: &str) -> bool {
    path == "responses"
}

struct ModelProxyForwardRequest {
    upstream_url: String,
    upstream_protocol: ModelUpstreamProtocol,
    request_settings: ModelRequestSettings,
    path: String,
    query: Option<String>,
    headers: HeaderMap,
    body: Bytes,
    api_key: Zeroizing<String>,
    accounting: Option<ModelProxyAccountingContext>,
}

#[derive(Serialize)]
struct ModelGatewayRequestEnvelope<'a> {
    request_id: String,
    protocol: ModelUpstreamProtocol,
    request_settings: &'a ModelRequestSettings,
    endpoint: &'a str,
    api_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<&'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, Vec<String>>,
    body_base64: String,
}

pub(crate) async fn send_model_gateway_request(
    state: &AppState,
    request: ModelGatewayForwardRequest<'_>,
) -> Result<reqwest::Response, reqwest::Error> {
    let ModelGatewayForwardRequest {
        request_id,
        upstream_protocol,
        request_settings,
        upstream_url,
        query,
        headers,
        body,
        api_key,
    } = request;
    let mut serialized_headers = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            serialized_headers
                .entry(name.as_str().to_owned())
                .or_default()
                .push(value.to_owned());
        }
    }
    let envelope = ModelGatewayRequestEnvelope {
        request_id: request_id.to_string(),
        protocol: upstream_protocol,
        request_settings,
        endpoint: upstream_url,
        api_key,
        query: query.filter(|query| !query.is_empty()),
        headers: serialized_headers,
        body_base64: base64::engine::general_purpose::STANDARD.encode(body),
    };
    state
        .model_proxy_http
        .post(format!("{}/internal/v1/responses", state.model_gateway_url))
        .bearer_auth(state.model_gateway_auth_token.as_str())
        .json(&envelope)
        .send()
        .await
}

#[cfg(test)]
pub(crate) async fn proxy_model_request_to_upstream(
    state: &AppState,
    upstream_url: &str,
    path: &str,
    request_body: Bytes,
) -> Result<Response, ApiError> {
    proxy_model_request_to_upstream_with_options(
        state,
        ModelProxyForwardRequest {
            upstream_url: upstream_url.to_owned(),
            upstream_protocol: ModelUpstreamProtocol::OpenaiResponses,
            request_settings: ModelRequestSettings::default(),
            path: path.to_owned(),
            query: None,
            headers: HeaderMap::new(),
            body: request_body,
            api_key: Zeroizing::new("test-provider-secret".into()),
            accounting: None,
        },
    )
    .await
}

pub(crate) async fn proxy_model_request_to_upstream_with_options(
    state: &AppState,
    request: ModelProxyForwardRequest,
) -> Result<Response, ApiError> {
    let ModelProxyForwardRequest {
        upstream_url,
        upstream_protocol,
        request_settings,
        path,
        query,
        headers,
        body,
        api_key,
        accounting,
    } = request;
    if !model_proxy_path_supported(&path) {
        return Err(ApiError::not_found("unsupported model proxy path"));
    }
    let mut upstream_headers = filtered_model_request_headers(&headers);
    if !upstream_headers.contains_key(header::CONTENT_TYPE) {
        upstream_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    let request_id = accounting
        .as_ref()
        .map(|accounting| accounting.request_id)
        .unwrap_or_else(Uuid::new_v4);
    let mut upstream = match send_model_gateway_request(
        state,
        ModelGatewayForwardRequest {
            request_id,
            upstream_protocol,
            request_settings: &request_settings,
            upstream_url: &upstream_url,
            query: query.as_deref(),
            headers: &upstream_headers,
            body: &body,
            api_key: &api_key,
        },
    )
    .await
    {
        Ok(upstream) => upstream,
        Err(error) => {
            if let Some(accounting) = accounting.as_ref() {
                let observation = ModelProxyObservation {
                    usage: None,
                    error: Some(ModelProxyErrorObservation {
                        response_status: "transport_error".into(),
                        upstream_http_status: None,
                        error_kind: if error.is_timeout() {
                            "timeout".into()
                        } else {
                            "transport".into()
                        },
                        error_code: None,
                        message: if error.is_timeout() {
                            "model upstream timed out".into()
                        } else {
                            "model upstream request failed".into()
                        },
                    }),
                };
                persist_model_proxy_observation(&state.pool, accounting, observation).await;
            }
            return Err(if error.is_timeout() {
                ApiError::gateway_timeout("model upstream timed out")
            } else {
                ApiError::bad_gateway("model upstream request failed")
            });
        }
    };
    let status = upstream.status();
    let upstream_headers = upstream.headers().clone();
    let is_sse = upstream_headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("text/event-stream"));
    let pool = state.pool.clone();
    let body = Body::from_stream(stream! {
        let mut observer = Some(ModelResponseObserver::new(is_sse));
        loop {
            match upstream.chunk().await {
                Ok(Some(chunk)) => {
                    if let Some(active_observer) = observer.as_mut() {
                        active_observer.push(&chunk);
                    }
                    if observer.as_ref().is_some_and(ModelResponseObserver::has_terminal) {
                        let completed_observer = observer.take().expect("terminal observer exists");
                        if let Some(accounting) = accounting.as_ref() {
                            let observation = completed_observer.finish(
                                status,
                                None,
                                Some(api_key.as_str()),
                            );
                            persist_model_proxy_observation(&pool, accounting, observation).await;
                        }
                    }
                    yield Ok::<Bytes, std::io::Error>(chunk);
                }
                Ok(None) => {
                    if let (Some(accounting), Some(observer)) =
                        (accounting.as_ref(), observer.take())
                    {
                        let observation = observer.finish(
                            status,
                            None,
                            Some(api_key.as_str()),
                        );
                        persist_model_proxy_observation(&pool, accounting, observation).await;
                    }
                    break;
                }
                Err(error) => {
                    warn!(error = %error, "model upstream response stream failed");
                    if let (Some(accounting), Some(observer)) =
                        (accounting.as_ref(), observer.take())
                    {
                        let failure_kind = if error.is_timeout() { "timeout" } else { "transport" };
                        let observation = observer.finish(
                            status,
                            Some(failure_kind),
                            Some(api_key.as_str()),
                        );
                        persist_model_proxy_observation(&pool, accounting, observation).await;
                    }
                    yield Err::<Bytes, std::io::Error>(std::io::Error::other(
                        "model upstream response stream failed",
                    ));
                    break;
                }
            }
        }
    });
    let mut response = Response::new(body);
    *response.status_mut() = status;
    copy_upstream_response_headers(response.headers_mut(), &upstream_headers);
    Ok(response)
}

pub(crate) fn copy_upstream_response_headers(target: &mut HeaderMap, upstream: &HeaderMap) {
    let connection_header_names = connection_header_names(upstream);
    for (name, value) in upstream {
        if !is_hop_by_hop_header(name, &connection_header_names)
            && name != header::CONTENT_LENGTH
            && !is_sensitive_model_response_header(name)
        {
            target.append(name, value.clone());
        }
    }
}

pub(crate) fn filtered_model_request_headers(source: &HeaderMap) -> HeaderMap {
    let connection_header_names = connection_header_names(source);
    let mut filtered = HeaderMap::new();
    for (name, value) in source {
        if is_hop_by_hop_header(name, &connection_header_names)
            || matches!(
                name.as_str(),
                "authorization"
                    | "proxy-authorization"
                    | "host"
                    | "content-length"
                    | "cookie"
                    | "api-key"
                    | "x-api-key"
                    | "forwarded"
                    | "x-forwarded-for"
                    | "x-forwarded-host"
                    | "x-forwarded-port"
                    | "x-forwarded-proto"
            )
            || name.as_str().starts_with("x-agent-hub-")
        {
            continue;
        }
        filtered.append(name, value.clone());
    }
    filtered
}

pub(crate) fn connection_header_names(headers: &HeaderMap) -> BTreeSet<String> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

pub(crate) fn is_sensitive_model_response_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "authentication-info"
            | "proxy-authentication-info"
            | "set-cookie"
            | "api-key"
            | "x-api-key"
    ) || name.starts_with("x-agent-hub-")
        || ["token", "api-key", "api_key", "secret"]
            .iter()
            .any(|marker| name.contains(marker))
}

pub(crate) fn is_hop_by_hop_header(
    name: &HeaderName,
    connection_header_names: &BTreeSet<String>,
) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    ) || connection_header_names.contains(name.as_str())
}

struct ModelResponseObserver {
    is_sse: bool,
    json_buffer: Vec<u8>,
    sse_line: Vec<u8>,
    sse_event_type: Option<String>,
    sse_event_data: Vec<u8>,
    current_event_overflowed: bool,
    overflowed: bool,
    terminal: Option<ModelProxyTerminal>,
}

struct ModelProxyTerminal {
    response_status: String,
    usage: Option<ObservedModelUsage>,
    error_code: Option<String>,
    error_message: Option<String>,
}

struct ModelProxyObservation {
    usage: Option<(String, ObservedModelUsage)>,
    error: Option<ModelProxyErrorObservation>,
}

struct ModelProxyErrorObservation {
    response_status: String,
    upstream_http_status: Option<u16>,
    error_kind: String,
    error_code: Option<String>,
    message: String,
}

impl ModelResponseObserver {
    fn new(is_sse: bool) -> Self {
        Self {
            is_sse,
            json_buffer: Vec::new(),
            sse_line: Vec::new(),
            sse_event_type: None,
            sse_event_data: Vec::new(),
            current_event_overflowed: false,
            overflowed: false,
            terminal: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if self.is_sse {
            self.push_sse(chunk);
        } else if !self.overflowed {
            if self.json_buffer.len().saturating_add(chunk.len()) <= MODEL_PROXY_OBSERVER_MAX_BYTES
            {
                self.json_buffer.extend_from_slice(chunk);
            } else {
                self.json_buffer.clear();
                self.overflowed = true;
            }
        }
    }

    fn has_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    fn push_sse(&mut self, chunk: &[u8]) {
        for byte in chunk {
            if *byte == b'\n' {
                let mut line = std::mem::take(&mut self.sse_line);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                self.process_sse_line(&line);
            } else if self.sse_line.len() < MODEL_PROXY_SSE_LINE_MAX_BYTES {
                self.sse_line.push(*byte);
            } else {
                self.current_event_overflowed = true;
                self.overflowed = true;
            }
        }
    }

    fn process_sse_line(&mut self, line: &[u8]) {
        if line.is_empty() {
            self.dispatch_sse_event();
            return;
        }
        if let Some(value) = line.strip_prefix(b"event:") {
            let value = value.strip_prefix(b" ").unwrap_or(value);
            self.sse_event_type = std::str::from_utf8(value).ok().map(str::to_owned);
            return;
        }
        let Some(value) = line.strip_prefix(b"data:") else {
            return;
        };
        let value = value.strip_prefix(b" ").unwrap_or(value);
        let additional = value.len() + usize::from(!self.sse_event_data.is_empty());
        if self.sse_event_data.len().saturating_add(additional) > MODEL_PROXY_OBSERVER_MAX_BYTES {
            self.current_event_overflowed = true;
            self.overflowed = true;
            self.sse_event_data.clear();
            return;
        }
        if !self.sse_event_data.is_empty() {
            self.sse_event_data.push(b'\n');
        }
        self.sse_event_data.extend_from_slice(value);
    }

    fn dispatch_sse_event(&mut self) {
        if !self.current_event_overflowed
            && !self.sse_event_data.is_empty()
            && self.terminal.is_none()
        {
            if let Ok(value) = serde_json::from_slice::<Value>(&self.sse_event_data) {
                self.terminal =
                    model_proxy_terminal_from_value(&value, self.sse_event_type.as_deref(), None);
            }
        }
        self.sse_event_type = None;
        self.sse_event_data.clear();
        self.current_event_overflowed = false;
    }

    fn finish(
        mut self,
        http_status: StatusCode,
        transport_failure_kind: Option<&str>,
        api_key: Option<&str>,
    ) -> ModelProxyObservation {
        if self.is_sse {
            if !self.sse_line.is_empty() {
                let mut line = std::mem::take(&mut self.sse_line);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                self.process_sse_line(&line);
            }
            if !self.sse_event_data.is_empty() || self.current_event_overflowed {
                self.dispatch_sse_event();
            }
        } else if !self.overflowed && self.terminal.is_none() {
            if let Ok(value) = serde_json::from_slice::<Value>(&self.json_buffer) {
                let fallback_status = if !http_status.is_success() || value.get("error").is_some() {
                    "failed"
                } else {
                    "completed"
                };
                self.terminal =
                    model_proxy_terminal_from_value(&value, None, Some(fallback_status));
            }
        }

        let usage = self.terminal.as_ref().and_then(|terminal| {
            terminal
                .usage
                .clone()
                .map(|usage| (terminal.response_status.clone(), usage))
        });
        let error = if let Some(kind) = transport_failure_kind {
            Some(ModelProxyErrorObservation {
                response_status: "transport_error".into(),
                upstream_http_status: Some(http_status.as_u16()),
                error_kind: kind.into(),
                error_code: None,
                message: if kind == "timeout" {
                    "model upstream response body timed out".into()
                } else {
                    "model upstream response body failed".into()
                },
            })
        } else if let Some(terminal) = self.terminal.as_ref() {
            if terminal.response_status != "completed" {
                let fallback = format!("model upstream reported {}", terminal.response_status);
                Some(ModelProxyErrorObservation {
                    response_status: terminal.response_status.clone(),
                    upstream_http_status: Some(http_status.as_u16()),
                    error_kind: format!("provider_{}", terminal.response_status),
                    error_code: terminal
                        .error_code
                        .as_deref()
                        .and_then(|value| sanitize_model_proxy_text(value, api_key, 256)),
                    message: sanitize_model_proxy_text(
                        terminal.error_message.as_deref().unwrap_or(&fallback),
                        api_key,
                        2048,
                    )
                    .unwrap_or(fallback),
                })
            } else if !http_status.is_success() {
                Some(ModelProxyErrorObservation {
                    response_status: "failed".into(),
                    upstream_http_status: Some(http_status.as_u16()),
                    error_kind: "upstream_http".into(),
                    error_code: terminal
                        .error_code
                        .as_deref()
                        .and_then(|value| sanitize_model_proxy_text(value, api_key, 256)),
                    message: format!("model upstream returned HTTP {}", http_status.as_u16()),
                })
            } else if terminal.usage.is_none() {
                Some(ModelProxyErrorObservation {
                    response_status: "protocol_error".into(),
                    upstream_http_status: Some(http_status.as_u16()),
                    error_kind: "protocol".into(),
                    error_code: None,
                    message: "completed model response did not include valid usage".into(),
                })
            } else {
                None
            }
        } else if http_status.is_success() {
            Some(ModelProxyErrorObservation {
                response_status: "protocol_error".into(),
                upstream_http_status: Some(http_status.as_u16()),
                error_kind: "protocol".into(),
                error_code: None,
                message: if self.overflowed {
                    "model response exceeded the accounting observer limit".into()
                } else {
                    "model response ended without a terminal Responses event".into()
                },
            })
        } else {
            Some(ModelProxyErrorObservation {
                response_status: "failed".into(),
                upstream_http_status: Some(http_status.as_u16()),
                error_kind: "upstream_http".into(),
                error_code: None,
                message: format!("model upstream returned HTTP {}", http_status.as_u16()),
            })
        };
        ModelProxyObservation { usage, error }
    }
}

pub(crate) fn model_proxy_terminal_from_value(
    value: &Value,
    event_type: Option<&str>,
    fallback_status: Option<&str>,
) -> Option<ModelProxyTerminal> {
    let response = value.get("response").unwrap_or(value);
    let status = response
        .get("status")
        .and_then(Value::as_str)
        .and_then(normalized_model_response_status)
        .or_else(|| {
            event_type
                .or_else(|| value.get("type").and_then(Value::as_str))
                .and_then(model_response_status_from_event)
        })
        .or_else(|| fallback_status.and_then(normalized_model_response_status))?;
    let is_error_event =
        event_type == Some("error") || value.get("type").and_then(Value::as_str) == Some("error");
    let error = response
        .get("error")
        .filter(|error| !error.is_null())
        .or_else(|| value.get("error").filter(|error| !error.is_null()));
    let error_code = error
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            response
                .pointer("/incomplete_details/reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            is_error_event
                .then(|| value.get("code").and_then(Value::as_str).map(str::to_owned))
                .flatten()
        });
    let error_message = error
        .and_then(|error| {
            error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
        })
        .map(str::to_owned)
        .or_else(|| {
            is_error_event
                .then(|| {
                    value
                        .get("message")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .flatten()
        });
    Some(ModelProxyTerminal {
        response_status: status.into(),
        usage: extract_model_usage(response).or_else(|| extract_model_usage(value)),
        error_code,
        error_message,
    })
}

pub(crate) fn normalized_model_response_status(value: &str) -> Option<&'static str> {
    match value {
        "completed" => Some("completed"),
        "failed" => Some("failed"),
        "incomplete" => Some("incomplete"),
        "cancelled" => Some("cancelled"),
        _ => None,
    }
}

pub(crate) fn model_response_status_from_event(value: &str) -> Option<&'static str> {
    match value {
        "response.completed" => Some("completed"),
        "response.failed" => Some("failed"),
        "response.incomplete" => Some("incomplete"),
        "response.cancelled" => Some("cancelled"),
        "error" => Some("failed"),
        _ => None,
    }
}

pub(crate) fn sanitize_model_proxy_text(
    value: &str,
    api_key: Option<&str>,
    max_chars: usize,
) -> Option<String> {
    let mut value = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if let Some(api_key) = api_key.filter(|api_key| !api_key.is_empty()) {
        value = value.replace(api_key, "[redacted]");
    }
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.chars().take(max_chars).collect())
    }
}

pub(crate) async fn persist_model_proxy_observation(
    pool: &PgPool,
    context: &ModelProxyAccountingContext,
    observation: ModelProxyObservation,
) {
    if observation.usage.is_none() && observation.error.is_none() {
        return;
    }
    if let Err(error) = try_persist_model_proxy_observation(pool, context, observation).await {
        warn!(
            request_id = %context.request_id,
            error = %error,
            "failed to persist model proxy accounting"
        );
    }
}

pub(crate) async fn try_persist_model_proxy_observation(
    pool: &PgPool,
    context: &ModelProxyAccountingContext,
    observation: ModelProxyObservation,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Some((response_status, usage)) = observation.usage {
        sqlx::query(
            "INSERT INTO model_token_usage
                 (id, request_id, response_status, model_connection_id,
                  model_connection_scope_snapshot, model_connection_name_snapshot,
                  model_id_snapshot, api_type_snapshot,
                  request_settings_snapshot,
                  agent_id, agent_name_snapshot,
                  subject_type, subject_user_id, subject_display_name_snapshot,
                  source_integration_app_id, source_integration_app_name_snapshot,
                  input_tokens, output_tokens, total_tokens, cached_tokens,
                  reasoning_tokens)
             VALUES (
                 $1, $2, $3,
                 (SELECT id FROM model_connections WHERE id = $4),
                 $5, $6, $7, $8, $9,
                 (SELECT id FROM agents WHERE id = $10), $11,
                 $12, (SELECT id FROM users WHERE id = $13), $14,
                 (SELECT id FROM oauth_apps WHERE id = $15), $16,
                 $17, $18, $19, $20, $21
             )
             ON CONFLICT (request_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(context.request_id)
        .bind(response_status)
        .bind(context.model_connection_id)
        .bind(&context.model_connection_scope)
        .bind(&context.model_connection_name)
        .bind(&context.model_id)
        .bind(model_upstream_protocol_name(context.api_type))
        .bind(model_request_settings_value(&context.request_settings))
        .bind(context.agent_id)
        .bind(&context.agent_name)
        .bind(&context.subject_type)
        .bind(context.subject_user_id)
        .bind(&context.subject_display_name)
        .bind(context.source_integration_app_id)
        .bind(&context.source_integration_app_name)
        .bind(usage.input_tokens)
        .bind(usage.output_tokens)
        .bind(usage.total_tokens)
        .bind(usage.cached_tokens)
        .bind(usage.reasoning_tokens)
        .execute(&mut *tx)
        .await?;
    }
    if let Some(error) = observation.error {
        sqlx::query(
            "INSERT INTO model_call_errors
                 (id, request_id, response_status, upstream_http_status,
                  error_kind, error_code, message, model_connection_id,
                  model_connection_scope_snapshot, model_connection_name_snapshot,
                  model_id_snapshot, api_type_snapshot,
                  request_settings_snapshot,
                  agent_id, agent_name_snapshot,
                  subject_type, subject_user_id, subject_display_name_snapshot,
                  source_integration_app_id, source_integration_app_name_snapshot)
             VALUES (
                 $1, $2, $3, $4, $5, $6, $7,
                 (SELECT id FROM model_connections WHERE id = $8),
                 $9, $10, $11, $12, $13,
                 (SELECT id FROM agents WHERE id = $14), $15,
                 $16, (SELECT id FROM users WHERE id = $17), $18,
                 (SELECT id FROM oauth_apps WHERE id = $19), $20
             )
             ON CONFLICT (request_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(context.request_id)
        .bind(error.response_status)
        .bind(error.upstream_http_status.map(i32::from))
        .bind(error.error_kind)
        .bind(error.error_code)
        .bind(error.message)
        .bind(context.model_connection_id)
        .bind(&context.model_connection_scope)
        .bind(&context.model_connection_name)
        .bind(&context.model_id)
        .bind(model_upstream_protocol_name(context.api_type))
        .bind(model_request_settings_value(&context.request_settings))
        .bind(context.agent_id)
        .bind(&context.agent_name)
        .bind(&context.subject_type)
        .bind(context.subject_user_id)
        .bind(&context.subject_display_name)
        .bind(context.source_integration_app_id)
        .bind(&context.source_integration_app_name)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await
}

pub(crate) async fn reap_stale_runtimes(pool: &PgPool) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    let acquired: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_xact_lock(hashtextextended('agent-hub-runtime-reaper', 0))",
    )
    .fetch_one(&mut *tx)
    .await?;
    if !acquired {
        return Ok(());
    }
    let stale_runtime_ids = sqlx::query_scalar::<_, Uuid>(
        "UPDATE runtimes
         SET status = 'offline'
         WHERE status = 'online' AND last_heartbeat_at < now() - interval '90 seconds'
         RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await?;
    for runtime_id in stale_runtime_ids {
        let failed_run_ids = sqlx::query_scalar::<_, Uuid>(
            "UPDATE runs
             SET status = 'failed', updated_at = now()
             WHERE runtime_id = $1 AND status IN ('running', 'waiting_tool')
             RETURNING id",
        )
        .bind(runtime_id)
        .fetch_all(&mut *tx)
        .await?;
        for run_id in failed_run_ids {
            insert_run_event_tx(
                &mut tx,
                run_id,
                "status".into(),
                None,
                Some("failed".into()),
                json!({ "status": "failed", "reason": "runtime went offline" }),
            )
            .await?;
        }
        sqlx::query(
            "INSERT INTO runtime_session_salvage_obligations
                 (runtime_id, session_id, ownership_generation, history_checkpoint,
                  bundle_generation)
             SELECT $1, id, ownership_generation, history_checkpoint,
                    COALESCE(current_bundle_generation, 0) + 1
             FROM hub_sessions
             WHERE runtime_owner_id = $1
             ON CONFLICT (runtime_id, session_id, ownership_generation) DO NOTHING",
        )
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = NULL,
                 lifecycle_status = CASE
                     WHEN EXISTS (
                         SELECT 1 FROM runs
                         WHERE hub_session_id = hub_sessions.id
                           AND status = 'pending'
                     ) OR EXISTS (
                         SELECT 1 FROM hub_session_messages
                         WHERE session_id = hub_sessions.id
                           AND delivery_state = 'queued'
                     ) THEN 'waiting_for_runtime'
                     ELSE 'offline'
                 END,
                 active_turn_id = NULL,
                 ownership_generation = ownership_generation + 1,
                 saving_history_checkpoint = NULL,
                 saving_ownership_generation = NULL,
                 saving_reason = NULL,
                 saving_checkpoint_attempt_id = NULL,
                 last_checkpoint_attempt_id = NULL,
                 last_checkpoint_ownership_generation = NULL,
                 last_checkpoint_disposition = NULL,
                 last_checkpoint_has_queued_work = NULL,
                 -- runtime 下线强制释放：bundle 归档不再有效，清空全部归档字段
                 -- （含 checkpoint），恢复时从消息表全量重放；checkpoint NULL 时
                 -- unreplayable 检查自动放行，避免后续 claim 被永久阻塞。
                 current_bundle_generation = NULL,
                 current_bundle_object_key = NULL,
                 current_bundle_checksum_sha256 = NULL,
                 current_bundle_size_bytes = NULL,
                 current_bundle_history_checkpoint = NULL,
                 current_bundle_kind = NULL,
                 current_bundle_ownership_generation = NULL,
                 current_bundle_producing_engine_version = NULL,
                 current_bundle_created_at = NULL,
                 current_bundle_runtime_id = NULL,
                 recovery_error = CASE
                     WHEN EXISTS (
                         SELECT 1 FROM hub_session_messages
                         WHERE session_id = hub_sessions.id
                           AND delivery_state IN ('delivering', 'delivered')
                           AND (hub_sessions.current_bundle_history_checkpoint IS NULL
                                OR sequence > hub_sessions.current_bundle_history_checkpoint)
                     ) OR current_bundle_history_checkpoint IS NULL
                          OR current_bundle_history_checkpoint < history_checkpoint
                     THEN '服务端发生意外，导致 agent 环境数据丢失，但对话历史还在'
                     ELSE recovery_error
                 END
             WHERE runtime_owner_id = $1",
        )
        .bind(runtime_id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn runtime_reaper_loop(pool: PgPool) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        if let Err(error) = reap_stale_runtimes(&pool).await {
            warn!(error = %error.message, "runtime reaper failed");
        }
        if let Err(error) = reap_expired_client_tool_batches(&pool).await {
            warn!(error = %error.message, "Client Tool timeout reaper failed");
        }
    }
}

pub(crate) async fn reap_expired_client_tool_batches(pool: &PgPool) -> Result<(), ApiError> {
    let run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT DISTINCT request.run_id
         FROM integration_tool_requests AS request
         JOIN runs AS run ON run.id = request.run_id
         WHERE run.client_instance_id IS NOT NULL
           AND request.status IN ('pending', 'claimed', 'unknown')
           AND request.expires_at <= now()
         ORDER BY request.run_id",
    )
    .fetch_all(pool)
    .await?;
    for run_id in run_ids {
        fail_expired_client_tool_batch(pool, run_id).await?;
    }
    Ok(())
}

pub(crate) async fn fail_expired_client_tool_batch(
    pool: &PgPool,
    run_id: Uuid,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    let preview = sqlx::query(
        "SELECT agent_id, hub_session_id FROM runs
         WHERE id = $1 AND client_instance_id IS NOT NULL",
    )
    .bind(run_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(preview) = preview else {
        tx.commit().await?;
        return Ok(());
    };
    let agent_id: Uuid = preview.get("agent_id");
    let hub_session_id: Uuid = preview.get("hub_session_id");
    if sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
        .bind(agent_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        tx.commit().await?;
        return Ok(());
    }
    if sqlx::query_scalar::<_, Uuid>("SELECT id FROM hub_sessions WHERE id = $1 FOR UPDATE")
        .bind(hub_session_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    {
        tx.commit().await?;
        return Ok(());
    }
    let run = sqlx::query(
        "SELECT id, agent_id, owner_id, integration_session_id, hub_session_id,
                hub_turn_id, status, client_instance_id, client_tool_snapshot,
                widget_session_id, external_user_context, model_subject_type,
                model_subject_user_id, model_source_integration_app_id
         FROM runs
         WHERE id = $1 AND hub_session_id = $2 AND client_instance_id IS NOT NULL
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(hub_session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(run) = run else {
        tx.commit().await?;
        return Ok(());
    };
    let expired = sqlx::query(
        "SELECT id FROM integration_tool_requests
         WHERE run_id = $1 AND status IN ('pending', 'claimed', 'unknown')
           AND expires_at <= now()
         ORDER BY position FOR UPDATE",
    )
    .bind(run_id)
    .fetch_all(&mut *tx)
    .await?;
    if expired.is_empty() {
        tx.commit().await?;
        return Ok(());
    }
    let scope = ClientToolRunScope {
        run_id,
        agent_id: run.get("agent_id"),
        owner_id: run.get("owner_id"),
        integration_session_id: run.get("integration_session_id"),
        hub_session_id,
        hub_turn_id: run.get("hub_turn_id"),
        client_instance_id: run.get("client_instance_id"),
        client_tool_snapshot: run.get("client_tool_snapshot"),
        widget_session_id: run.get("widget_session_id"),
        external_user_context: run.get("external_user_context"),
        model_subject_type: run.get("model_subject_type"),
        model_subject_user_id: run.get("model_subject_user_id"),
        model_source_integration_app_id: run.get("model_source_integration_app_id"),
    };
    fail_client_tool_batch_tx(
        &mut tx,
        &scope,
        "timed_out",
        "failed",
        "client_tool_timeout",
        "Client Tool batch reached its deadline",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn fail_capability_mismatched_runs_for_runtime_tx(
    tx: &mut Transaction<'_, Postgres>,
    runtime_id: Uuid,
) -> Result<Vec<Uuid>, ApiError> {
    let agent_sql = format!(
        "SELECT a.id
         FROM agents a
         JOIN runtimes rt ON rt.id = $1
         WHERE a.runtime_id = $1
           AND a.deleted_at IS NULL
           AND EXISTS (
             SELECT 1 FROM runs r WHERE r.agent_id = a.id AND r.status = 'pending'
           )
           AND NOT (TRUE {RUNTIME_CAPABILITY_SQL})
         ORDER BY a.id
         FOR SHARE OF a"
    );
    let agent_ids: Vec<Uuid> = sqlx::query_scalar(&agent_sql)
        .bind(runtime_id)
        .fetch_all(&mut **tx)
        .await?;
    if agent_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "UPDATE runs
         SET status = 'failed', updated_at = now()
         WHERE status = 'pending' AND agent_id = ANY($1)
         RETURNING id",
    )
    .bind(&agent_ids)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use crate::tests::{runtime_write, runtime_write_generation};
    #[test]
    fn run_event_sanitizers_remove_nul_bytes() {
        let mut text = "before\0after".to_owned();
        sanitize_run_event_text(&mut text);
        assert_eq!(text, "before\u{FFFD}after");

        let payload = json!({
            "output": "ELF\0bin\0",
            "nested": ["x\0", {"key": "v\0"}],
            "count": 3,
            "flag": true,
            "nothing": null,
        });
        let sanitized = sanitize_run_event_payload(payload);
        assert_eq!(sanitized["output"], json!("ELF\u{FFFD}bin\u{FFFD}"));
        assert_eq!(sanitized["nested"][0], json!("x\u{FFFD}"));
        assert_eq!(sanitized["nested"][1]["key"], json!("v\u{FFFD}"));
        assert_eq!(sanitized["count"], json!(3));
        assert_eq!(sanitized["flag"], json!(true));
        assert_eq!(sanitized["nothing"], json!(null));

        // Object keys must be sanitized too: jsonb rejects NUL anywhere.
        let mut keyed = serde_json::Map::new();
        keyed.insert("bad\0key".into(), json!("value"));
        keyed.insert("ok".into(), json!("value"));
        let sanitized_keyed = sanitize_run_event_payload(Value::Object(keyed));
        let object = sanitized_keyed.as_object().unwrap();
        assert!(object.contains_key("bad\u{FFFD}key"));
        assert!(!object.keys().any(|key| key.contains('\0')));
        assert_eq!(object["ok"], json!("value"));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_contains_complete_effective_execution_configuration(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let overridden_skill_id = Uuid::new_v4();
        let managed_only_skill_id = Uuid::new_v4();
        for (skill_id, name, content) in [
            (overridden_skill_id, "review", "managed review"),
            (managed_only_skill_id, "testing", "managed testing"),
        ] {
            sqlx::query(
                "INSERT INTO skills
                     (id, owner_id, name, description, content, content_checksum_sha256)
                 SELECT $1, owner_id, $2, $2, $3, $4 FROM agents WHERE id = $5",
            )
            .bind(skill_id)
            .bind(name)
            .bind(content)
            .bind(sha256_hex(content))
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
                .bind(fixture.agent_id)
                .bind(skill_id)
                .execute(&fixture.state.pool)
                .await
                .unwrap();
        }
        sqlx::query(
            "UPDATE agents
             SET instructions = 'Task 9 instructions',
                 mcp_allowlist = $1
             WHERE id = $2",
        )
        .bind(json!([{
            "name": "github",
            "command": "gh-mcp",
            "secrets": { "TOKEN": "claim-only-secret" }
        }]))
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runtimes
             SET capabilities = capabilities || '{\"mcp_allowlist\":true}'::jsonb
             WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let claim_json = serde_json::to_value(&claim).unwrap();
        let configuration: AgentExecutionConfigurationDto =
            serde_json::from_value(claim_json["execution_configuration"].clone())
                .expect("claim must publish a typed execution configuration");
        assert_eq!(configuration.revision, 1);
        assert_eq!(configuration.instructions, "Task 9 instructions");
        assert_eq!(configuration.skills.len(), 2);
        let review = configuration
            .skills
            .iter()
            .find(|skill| skill.name == "review")
            .unwrap();
        assert_eq!(review.source, "managed");
        assert_eq!(review.source_id, Some(overridden_skill_id));
        assert_eq!(review.revision, 1);
        assert_eq!(review.content, "managed review");
        assert_eq!(review.content_checksum_sha256, sha256_hex("managed review"));
        let testing = configuration
            .skills
            .iter()
            .find(|skill| skill.name == "testing")
            .unwrap();
        assert_eq!(testing.source, "managed");
        assert_eq!(testing.source_id, Some(managed_only_skill_id));
        assert_eq!(testing.revision, 1);
        assert_eq!(
            claim_json["expected_configuration_fingerprint"],
            execution_configuration_fingerprint(&configuration).unwrap()
        );
        assert!(!claim_json["expected_configuration_fingerprint"]
            .as_str()
            .unwrap()
            .contains("claim-only-secret"));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_waits_for_atomic_agent_and_skill_configuration_update(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let old_skill_id = Uuid::new_v4();
        let new_skill_id = Uuid::new_v4();
        for (skill_id, name, content) in [
            (old_skill_id, "old-skill", "old content"),
            (new_skill_id, "new-skill", "new content"),
        ] {
            sqlx::query(
                "INSERT INTO skills
                     (id, owner_id, name, description, content, content_checksum_sha256)
                 SELECT $1, owner_id, $2, $2, $3, $4
                 FROM agents WHERE id = $5",
            )
            .bind(skill_id)
            .bind(name)
            .bind(content)
            .bind(sha256_hex(content))
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(old_skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let mut update_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query(
            "UPDATE agents
             SET instructions = 'new atomic instructions',
                 execution_config_revision = execution_config_revision + 1
             WHERE id = $1",
        )
        .bind(fixture.agent_id)
        .execute(&mut *update_tx)
        .await
        .unwrap();
        sqlx::query("DELETE FROM agent_skills WHERE agent_id = $1")
            .bind(fixture.agent_id)
            .execute(&mut *update_tx)
            .await
            .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(new_skill_id)
            .execute(&mut *update_tx)
            .await
            .unwrap();

        let application_name = format!("runtime-claim-config-{}", Uuid::new_v4().simple());
        let claim_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_name).await,
        ));
        let runtime_token = fixture.runtime_token.clone();
        let mut claim_task =
            tokio::spawn(async move { claim_runtime_run(&claim_state, &runtime_token).await });
        let claim_wait_observed = wait_for_application_lock(
            &fixture.state.pool,
            &application_name,
            "SELECT a.id AS a_id",
        )
        .await;
        let pending_state = runtime_claim_run_state(&fixture.state.pool, fixture.run_id).await;

        update_tx.commit().await.unwrap();
        let claim = tokio::time::timeout(Duration::from_secs(3), &mut claim_task)
            .await
            .expect("runtime claim should unblock after Agent update commit")
            .expect("runtime claim task should not panic");

        assert!(
            claim_wait_observed,
            "runtime claim must wait for the complete Agent configuration update"
        );
        assert_eq!(pending_state, ("pending".into(), None, None));
        assert_eq!(claim.agent.instructions, "new atomic instructions");
        assert_eq!(claim.execution_configuration.revision, 2);
        assert_eq!(
            claim.execution_configuration.instructions,
            "new atomic instructions"
        );
        assert_eq!(claim.execution_configuration.skills.len(), 1);
        assert_eq!(
            claim.execution_configuration.skills[0].source_id,
            Some(new_skill_id)
        );
        assert_eq!(claim.execution_configuration.skills[0].name, "new-skill");
        assert_eq!(
            claim.expected_configuration_fingerprint,
            execution_configuration_fingerprint(&claim.execution_configuration).unwrap()
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_resumes_native_session_without_parent_run(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'session-canonical-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        assert_eq!(claim.run.parent_run_id, None);
        let resume = claim
            .resume
            .expect("a Session with a native Session must always resume it");
        assert_eq!(resume.native_session_id, "session-canonical-thread");
        assert_eq!(resume.work_dir_ref, None);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_prefers_native_session_over_conflicting_parent(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let parent_run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  native_session_id, work_dir_ref, hub_session_id, hub_turn_id,
                  session_ownership_generation)
             SELECT $1, agent_id, owner_id, 'completed', 'parent', 'console',
                    'stale-parent-thread', 'parent-work-dir', hub_session_id,
                    hub_turn_id, session_ownership_generation
             FROM runs WHERE id = $2",
        )
        .bind(parent_run_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE runs SET parent_run_id = $1 WHERE id = $2")
            .bind(parent_run_id)
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'session-canonical-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        assert_eq!(claim.run.parent_run_id, Some(parent_run_id));
        let resume = claim
            .resume
            .expect("the canonical native Session must override its parent Run");
        assert_eq!(resume.native_session_id, "session-canonical-thread");
        assert_eq!(resume.work_dir_ref.as_deref(), Some("parent-work-dir"));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_capacity_zero_rejects_new_but_allows_ready_owned_session(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;

        let no_capacity = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(0, Vec::new()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(no_capacity.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            runtime_claim_run_state(&fixture.state.pool, fixture.run_id).await,
            ("pending".into(), None, None)
        );

        let first = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let ownership_generation = first.run.session_ownership_generation.unwrap();
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(first.run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let next_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;

        let ready_owned = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(
                0,
                vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.hub_session_id,
                    ownership_generation,
                }],
            ),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(ready_owned.status(), StatusCode::OK);
        let body = axum::body::to_bytes(ready_owned.into_body(), usize::MAX)
            .await
            .unwrap();
        let claim: ClaimRunResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(claim.run.id, next_run_id);
        assert_eq!(
            claim.run.session_ownership_generation,
            Some(ownership_generation)
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_rejects_stale_foreign_and_duplicate_owned_session_snapshots(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let first = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let ownership_generation = first.run.session_ownership_generation.unwrap();
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(first.run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let next_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;

        let stale = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(
                0,
                vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.hub_session_id,
                    ownership_generation: ownership_generation + 1,
                }],
            ),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(stale.status(), StatusCode::NO_CONTENT);

        let foreign_runtime_id = Uuid::new_v4();
        let foreign_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{\"model_proxy\":true}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(foreign_runtime_id)
        .bind(sha256_hex(&foreign_runtime_token))
        .bind(format!("runtime-foreign-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE agents SET runtime_id = NULL WHERE id = $1")
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let foreign = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&foreign_runtime_token),
            runtime_claim_request(
                0,
                vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.hub_session_id,
                    ownership_generation,
                }],
            ),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(foreign.status(), StatusCode::NO_CONTENT);

        let duplicate = match runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(
                0,
                vec![
                    RuntimeOwnedSessionGenerationDto {
                        session_id: fixture.hub_session_id,
                        ownership_generation,
                    },
                    RuntimeOwnedSessionGenerationDto {
                        session_id: fixture.hub_session_id,
                        ownership_generation: ownership_generation + 1,
                    },
                ],
            ),
        )
        .await
        {
            Ok(_) => panic!("duplicate owned Session snapshot unexpectedly reached claim logic"),
            Err(error) => error,
        };
        assert_eq!(duplicate.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            runtime_claim_run_state(&fixture.state.pool, next_run_id).await,
            ("pending".into(), None, None)
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn run_event_deltas_stream_live_but_only_phases_are_persisted(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let run_id = fixture.run_id;
        let mut rx = fixture.state.run_event_bus.subscribe(run_id);
        let append = |event_type: &str, role: Option<&str>, payload: Value| {
            runtime_append_event(
                State(fixture.state.clone()),
                bearer_headers(&fixture.runtime_token),
                Path(run_id),
                runtime_write_generation(
                    1,
                    AppendRunEventRequest {
                        event_id: Uuid::new_v4(),
                        event_type: event_type.into(),
                        role: role.map(str::to_owned),
                        content: None,
                        payload,
                        waiting_tool: None,
                    },
                ),
            )
        };
        async fn count_run_item_events(pool: &PgPool, run_id: Uuid, item_id: &str) -> i64 {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_events
                 WHERE run_id = $1 AND payload->>'item_id' = $2",
            )
            .bind(run_id)
            .bind(item_id)
            .fetch_one(pool)
            .await
            .unwrap()
        }

        for index in 0..100 {
            let _ = append(
                "item",
                Some("assistant"),
                json!({
                    "phase": "summary_delta",
                    "item_id": "reasoning-1",
                    "item_type": "reasoning",
                    "summary": format!("chunk {index}"),
                }),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            count_run_item_events(&fixture.state.pool, run_id, "reasoning-1").await,
            0,
            "summary deltas must not be persisted"
        );
        for _ in 0..100 {
            let item = rx.recv().await.unwrap();
            assert!(!item.persisted);
        }

        let _ = append(
            "item",
            Some("assistant"),
            json!({
                "phase": "completed",
                "item_id": "reasoning-1",
                "item_type": "reasoning",
                "summary": ["full reasoning"],
            }),
        )
        .await
        .unwrap();
        let completed = rx.recv().await.unwrap();
        assert!(completed.persisted);
        assert_eq!(
            count_run_item_events(&fixture.state.pool, run_id, "reasoning-1").await,
            1
        );

        for index in 0..50 {
            let _ = append(
                "message_delta",
                Some("assistant"),
                json!({ "source": "pi", "stream": true, "content": format!("c{index}") }),
            )
            .await
            .unwrap();
        }
        let _ = append(
            "message",
            Some("assistant"),
            json!({ "source": "pi", "stop_reason": "stop" }),
        )
        .await
        .unwrap();
        let deltas = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM run_events
             WHERE run_id = $1 AND event_type = 'message_delta'",
        )
        .bind(run_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(deltas, 0);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_turn_started_event_binds_native_turn_before_completion(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions
             SET native_session_id = NULL, active_turn_id = NULL
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = NULL, status = 'starting', started_at = NULL
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        for content in ["first queued", "second queued"] {
            sqlx::query(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content, delivery_mode,
                      delivery_state, turn_id, run_id)
                 VALUES ($1, $2, 'user', 'message', $3, 'next_turn', 'delivering', $4, $5)",
            )
            .bind(Uuid::new_v4())
            .bind(fixture.hub_session_id)
            .bind(content)
            .bind(fixture.turn_id)
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }

        let _ = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "turn_started".into(),
                    role: None,
                    content: None,
                    payload: json!({
                        "native_session_id": "native-thread-bound",
                        "native_turn_id": "native-turn-bound"
                    }),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap();

        let session: (Option<String>, Option<Uuid>) = sqlx::query_as(
            "SELECT native_session_id, active_turn_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(session.0.as_deref(), Some("native-thread-bound"));
        assert_eq!(session.1, Some(fixture.turn_id));
        let turn: (Option<String>, String, bool) = sqlx::query_as(
            "SELECT native_turn_id, status, started_at IS NOT NULL
             FROM hub_session_turns WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            turn,
            (Some("native-turn-bound".into()), "running".into(), true)
        );
        let states: Vec<String> = sqlx::query_scalar(
            "SELECT delivery_state FROM hub_session_messages
             WHERE run_id = $1 ORDER BY sequence",
        )
        .bind(fixture.run_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(states, vec!["delivered", "delivered"]);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_begin_turn_generation_fences_synchronized_configuration(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions
             SET native_session_id = NULL, active_turn_id = NULL,
                 configuration_fingerprint = NULL
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = NULL, status = 'pending', started_at = NULL,
                 delivery_started_at = NULL, configuration_fingerprint = NULL
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let invalid = runtime_begin_turn(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                BeginRuntimeTurnRequest {
                    configuration_fingerprint: "sha256:not-a-digest".into(),
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            sqlx::query_as::<_, (Option<String>, String, Option<String>, bool)>(
                "SELECT sessions.configuration_fingerprint, turns.status,
                        turns.configuration_fingerprint,
                        turns.delivery_started_at IS NULL
                 FROM hub_sessions AS sessions
                 JOIN hub_session_turns AS turns ON turns.session_id = sessions.id
                 WHERE sessions.id = $1 AND turns.id = $2"
            )
            .bind(fixture.hub_session_id)
            .bind(fixture.turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (None, "pending".into(), None, true)
        );

        let cross_generation_binding = sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'starting', delivery_started_at = now(),
                 configuration_fingerprint = $1,
                 ownership_generation = ownership_generation + 1
             WHERE id = $2",
        )
        .bind(format!("sha256:{}", "f".repeat(64)))
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await;
        assert!(
            cross_generation_binding.is_err(),
            "configuration binding unexpectedly crossed an ownership generation"
        );

        let configuration_fingerprint = format!("sha256:{}", "a".repeat(64));
        let request = || {
            serde_json::from_value::<BeginRuntimeTurnRequest>(json!({
                "configuration_fingerprint": configuration_fingerprint
            }))
            .unwrap()
        };

        let stale = runtime_begin_turn(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(2, request()),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::FORBIDDEN);
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT configuration_fingerprint FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            None
        );

        let begun = runtime_begin_turn(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(1, request()),
        )
        .await
        .unwrap()
        .0;

        let begun_json = serde_json::to_value(&begun).unwrap();
        assert_eq!(
            begun_json["configuration_fingerprint"],
            configuration_fingerprint
        );
        assert_eq!(
            sqlx::query_as::<_, (Option<String>, Option<String>)>(
                "SELECT sessions.configuration_fingerprint, turns.configuration_fingerprint
                 FROM hub_sessions AS sessions
                 JOIN hub_session_turns AS turns ON turns.session_id = sessions.id
                 WHERE sessions.id = $1 AND turns.id = $2"
            )
            .bind(fixture.hub_session_id)
            .bind(fixture.turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (
                Some(configuration_fingerprint.clone()),
                Some(configuration_fingerprint)
            )
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_begin_turn_linearizes_initial_messages_and_late_message_becomes_steer(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions
             SET native_session_id = NULL, active_turn_id = NULL
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = NULL, status = 'pending', started_at = NULL,
                 delivery_started_at = NULL
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let early_message_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'early', 'next_turn', 'queued', $3, $4)",
        )
        .bind(early_message_id)
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let configuration_fingerprint = format!("sha256:{}", "b".repeat(64));

        let begun = runtime_begin_turn(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                BeginRuntimeTurnRequest {
                    configuration_fingerprint: configuration_fingerprint.clone(),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(begun.session_id, fixture.hub_session_id);
        assert_eq!(begun.turn_id, fixture.turn_id);
        assert_eq!(begun.messages.len(), 1);
        assert_eq!(begun.messages[0].id, early_message_id);
        assert_eq!(begun.messages[0].delivery_state, "delivering");

        let late_message_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'late', 'next_turn', 'queued', $3, $4)",
        )
        .bind(late_message_id)
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let retried = runtime_begin_turn(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                BeginRuntimeTurnRequest {
                    configuration_fingerprint,
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(retried.messages.len(), 1);
        assert_eq!(retried.messages[0].id, early_message_id);

        let _ = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "turn_started".into(),
                    role: None,
                    content: None,
                    payload: json!({
                        "native_session_id": "linear-thread",
                        "native_turn_id": "linear-turn"
                    }),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap();

        let deliveries: Vec<(Uuid, String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, delivery_mode, delivery_state, expected_native_turn_id
             FROM hub_session_messages WHERE run_id = $1 ORDER BY sequence",
        )
        .bind(fixture.run_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            deliveries,
            vec![
                (
                    early_message_id,
                    "next_turn".into(),
                    "delivered".into(),
                    None
                ),
                (
                    late_message_id,
                    "steer".into(),
                    "queued".into(),
                    Some("linear-turn".into())
                )
            ]
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_force_release_reclaims_failed_session_without_bundle_and_clears_outbox(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'online' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "failed".into(),
                    native_session_id: None,
                    work_dir_ref: None,
                },
            ),
        )
        .await
        .unwrap();
        let before: (Option<Uuid>, String, i64) = sqlx::query_as(
            "SELECT runtime_owner_id, lifecycle_status, ownership_generation
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(before.0, Some(fixture.runtime_id));
        assert_eq!(before.1, "online");
        assert_eq!(before.2, 1);

        let released = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: true,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(released.runtime_owner_id, None);
        assert_eq!(released.ownership_generation, 2);
        assert!(matches!(
            released.lifecycle_status.as_str(),
            "offline" | "waiting_for_runtime"
        ));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_released_session_clears_bundle_and_stays_claimable(pool: PgPool) {
        // 回归：runtime 下线/强制释放后，bundle 归档字段必须清空（含 checkpoint），
        // 否则 delivered 消息（sequence > checkpoint）会触发 unreplayable 检查，
        // 导致后续 run 永久 pending（面板一直"正在启动"）。
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        // 造一个"bundle 落后于 delivered 消息"的状态（释放前 runtime 未追平归档）；
        // sequence 由触发器分配（必然 > checkpoint 1）。
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, run_id, role, message_kind, content,
                  delivery_mode, delivery_state, accepted_at)
             VALUES ($1, $2, $3, 'assistant', 'assistant_message', 'early',
                     'next_turn', 'delivered', now()),
                    ($4, $2, $3, 'assistant', 'assistant_message', 'late',
                     'next_turn', 'delivered', now())",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(claim.run.id)
        .bind(Uuid::new_v4())
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'online',
                 current_bundle_generation = 1,
                 current_bundle_kind = 'checkpoint',
                 current_bundle_object_key = 'sessions/bundle-1.tar.zst',
                 current_bundle_checksum_sha256 = 'sha256:deadbeef',
                 current_bundle_size_bytes = 128,
                 current_bundle_history_checkpoint = 1,
                 current_bundle_ownership_generation = 1,
                 current_bundle_producing_engine_version = 'test',
                 current_bundle_created_at = now(),
                 current_bundle_runtime_id = $2
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // 先结束 claim 的 run（release 要求无进行中的执行），再 force 释放。
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "failed".into(),
                    native_session_id: None,
                    work_dir_ref: None,
                },
            ),
        )
        .await
        .unwrap();
        let _ = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: true,
            }),
        )
        .await
        .unwrap();

        // force 释放后：bundle 归档字段全部清空（含 checkpoint）。
        let bundle_state: (Option<i64>, Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT current_bundle_generation, current_bundle_object_key,
                    current_bundle_history_checkpoint
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            bundle_state,
            (None, None, None),
            "force release must clear bundle state"
        );

        // 新 run 应能被正常 claim（unreplayable 检查不再阻塞）。
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM runs WHERE id = $1")
            .bind(claim.run.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let next_turn_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'pending', 0)",
        )
        .bind(next_turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let next_run = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO runs (id, agent_id, owner_id, hub_session_id, hub_turn_id,
                               status, initial_message, source, session_ownership_generation)
             VALUES ($1, $2, $3, $4, $5, 'pending', 'after force release', 'user', 0)
             RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.agent_id)
        .bind(owner_id)
        .bind(fixture.hub_session_id)
        .bind(next_turn_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let reclaimed = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(
            reclaimed.run.id, next_run,
            "session must be claimable after force release"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_reaper_marks_unrecoverable_sessions_and_releases_recoverable_ones(
        pool: PgPool,
    ) {
        let owner_id = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Reaper Test Owner', 'member')",
        )
        .bind(owner_id)
        .bind(format!("reaper-{}@example.com", Uuid::new_v4().simple()))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Reaper Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status, last_heartbeat_at)
             VALUES ($1, $2, 'reaper-runtime', '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online', now() - interval '2 minutes')",
        )
        .bind(runtime_id)
        .bind(sha256_hex("reaper-runtime-token"))
        .execute(&pool)
        .await
        .unwrap();
        let unrecoverable_session = Uuid::new_v4();
        let recoverable_session = Uuid::new_v4();
        for session_id in [unrecoverable_session, recoverable_session] {
            sqlx::query(
                "INSERT INTO hub_sessions
                     (id, owner_id, agent_id, origin_kind, lifecycle_status,
                      runtime_owner_id, ownership_generation)
                 VALUES ($1, $2, $3, 'hub_native', 'online', $4, 1)",
            )
            .bind(session_id)
            .bind(owner_id)
            .bind(agent_id)
            .bind(runtime_id)
            .execute(&pool)
            .await
            .unwrap();
            let turn_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO hub_session_turns
                     (id, session_id, status, ownership_generation)
                 VALUES ($1, $2, 'pending', 1)",
            )
            .bind(turn_id)
            .bind(session_id)
            .execute(&pool)
            .await
            .unwrap();
            let run_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO runs
                     (id, agent_id, owner_id, status, initial_message, source,
                      hub_session_id, hub_turn_id, session_ownership_generation)
                 VALUES ($1, $2, $3, 'pending', 'next', 'console', $4, $5, 1)",
            )
            .bind(run_id)
            .bind(agent_id)
            .bind(owner_id)
            .bind(session_id)
            .bind(turn_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             SELECT $1, id, 'user', 'message', 'delivered without bundle',
                    'next_turn', 'delivered', $2, $3
             FROM hub_sessions WHERE id = $4",
        )
        .bind(Uuid::new_v4())
        .bind(
            sqlx::query_scalar::<_, Uuid>("SELECT hub_turn_id FROM runs WHERE hub_session_id = $1")
                .bind(unrecoverable_session)
                .fetch_one(&pool)
                .await
                .unwrap(),
        )
        .bind(
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM runs WHERE hub_session_id = $1")
                .bind(unrecoverable_session)
                .fetch_one(&pool)
                .await
                .unwrap(),
        )
        .bind(unrecoverable_session)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state)
             SELECT $1, id, 'user', 'message', 'queued and replayable',
                    'next_turn', 'queued'
             FROM hub_sessions WHERE id = $2",
        )
        .bind(Uuid::new_v4())
        .bind(recoverable_session)
        .execute(&pool)
        .await
        .unwrap();

        reap_stale_runtimes(&pool).await.unwrap();

        let unrecoverable: (Option<Uuid>, String, Option<String>) = sqlx::query_as(
            "SELECT runtime_owner_id, lifecycle_status, recovery_error
             FROM hub_sessions WHERE id = $1",
        )
        .bind(unrecoverable_session)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unrecoverable.0, None);
        assert_eq!(unrecoverable.1, "waiting_for_runtime");
        assert_eq!(
            unrecoverable.2.as_deref(),
            Some("服务端发生意外，导致 agent 环境数据丢失，但对话历史还在")
        );
        let recoverable: (Option<Uuid>, String) = sqlx::query_as(
            "SELECT runtime_owner_id, lifecycle_status
             FROM hub_sessions WHERE id = $1",
        )
        .bind(recoverable_session)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(recoverable.0, None);
        assert_eq!(recoverable.1, "waiting_for_runtime");
        let pending_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runs
             WHERE hub_session_id = $1 AND status = 'pending'",
        )
        .bind(unrecoverable_session)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(pending_count, 1);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_reaper_skips_runtimes_within_the_grace_window(pool: PgPool) {
        // reap 阈值 90s（> runtime HTTP 超时 60s）：60s 无心跳的正常 runtime
        // （一次慢请求导致）不能被误杀；90s+ 无心跳（真下线）才回收。
        let mut runtime_ids = Vec::new();
        for (label, heartbeat_age, expect_reaped) in [
            ("recent", "40 seconds", false),
            ("grace", "80 seconds", false),
            ("stale", "120 seconds", true),
        ] {
            let runtime_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO runtimes
                     (id, token_hash, hostname, labels, engine_version, capabilities,
                      sandbox_mode, status, last_heartbeat_at)
                 VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb,
                         'workspace-write', 'online', now() - $4::interval)",
            )
            .bind(runtime_id)
            .bind(sha256_hex(&format!("reaper-{label}-token")))
            .bind(label)
            .bind(heartbeat_age)
            .execute(&pool)
            .await
            .unwrap();
            runtime_ids.push((runtime_id, label, expect_reaped));
        }

        reap_stale_runtimes(&pool).await.unwrap();

        for (runtime_id, label, expect_reaped) in runtime_ids {
            let status: String = sqlx::query_scalar("SELECT status FROM runtimes WHERE id = $1")
                .bind(runtime_id)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(
                status,
                if expect_reaped { "offline" } else { "online" },
                "runtime {label} reap expectation"
            );
        }
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn bundle_sync_status_aggregates_by_runtime(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        for status in ["pending", "uploading", "done"] {
            let session_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO hub_sessions
                     (id, owner_id, agent_id, origin_kind, lifecycle_status,
                      runtime_owner_id, ownership_generation, bundle_sync_status,
                      bundle_sync_updated_at)
                 VALUES ($1, $2, $3, 'hub_native', 'online', $4, 1, $5, now())",
            )
            .bind(session_id)
            .bind(owner_id)
            .bind(fixture.agent_id)
            .bind(fixture.runtime_id)
            .bind(status)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        let admin_token = create_super_admin_session(&fixture.state.pool).await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let response = get_bundle_sync_status(State(admin_state), session_headers(&admin_token))
            .await
            .unwrap()
            .0;
        assert_eq!(response.len(), 1);
        assert_eq!(response[0].runtime_id, fixture.runtime_id);
        assert_eq!(response[0].total, 3);
        assert_eq!(response[0].pending, 1);
        assert_eq!(response[0].uploading, 1);
        assert_eq!(response[0].done, 1);
        assert_eq!(response[0].failed, 0);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_salvage_obligation_recovers_crashed_workspace_bundle(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'online' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE runtimes
             SET last_heartbeat_at = now() - interval '2 minutes'
             WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        reap_stale_runtimes(&fixture.state.pool).await.unwrap();

        let obligation: (i64, i64, i64) = sqlx::query_as(
            "SELECT ownership_generation, history_checkpoint, bundle_generation
             FROM runtime_session_salvage_obligations
             WHERE runtime_id = $1 AND session_id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(obligation, (1, 3, 1));

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            heartbeat.salvage_sessions,
            vec![RuntimeSalvageSessionDto {
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                history_checkpoint: 3,
                bundle_generation: 1,
            }]
        );

        let stored = Arc::new(std::sync::Mutex::new(Vec::new()));
        let object_app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let stored = Arc::clone(&stored);
                move |body: Body| {
                    let stored = Arc::clone(&stored);
                    async move {
                        *stored.lock().unwrap() =
                            axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);

        let bytes = Bytes::from_static(b"salvage bundle body");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let checkpoint_attempt_id = Uuid::new_v4();
        let mut headers = bearer_headers(&fixture.runtime_token);
        for (name, value) in [
            ("content-length", bytes.len().to_string()),
            ("x-agent-hub-ownership-generation", obligation.0.to_string()),
            (
                "x-agent-hub-checkpoint-attempt-id",
                checkpoint_attempt_id.to_string(),
            ),
            ("x-agent-hub-bundle-generation", obligation.2.to_string()),
            ("x-agent-hub-bundle-sha256", checksum.clone()),
            ("x-agent-hub-bundle-size", bytes.len().to_string()),
            ("x-agent-hub-history-checkpoint", obligation.1.to_string()),
            ("x-agent-hub-producing-engine-version", "0.104.0".into()),
            ("x-agent-hub-bundle-created-at", Utc::now().to_rfc3339()),
        ] {
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(&value).unwrap(),
            );
        }

        let response = runtime_salvage_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            headers,
            Body::from(bytes.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(response.checkpoint_attempt_id, checkpoint_attempt_id);
        assert_eq!(response.bundle_generation, 1);
        assert!(response.ownership_released);

        let pointer: (Option<i64>, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT current_bundle_generation, current_bundle_checksum_sha256, recovery_error
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(pointer, (Some(1), Some(checksum), None));
        let bundle_metadata: (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT current_bundle_history_checkpoint, current_bundle_ownership_generation
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(bundle_metadata, (Some(3), Some(1)));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_session_salvage_obligations
                 WHERE runtime_id = $1 AND session_id = $2",
            )
            .bind(fixture.runtime_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(*stored.lock().unwrap(), bytes);
        object_server.abort();
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn reject_claimed_run_terminates_conflict_and_preserves_incumbent(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // 构造第二个回合与任务 B（异常派发），并置 running。
        let conflict_turn = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'running', 1)",
        )
        .bind(conflict_turn)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let conflict_run = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, runtime_id, hub_session_id, hub_turn_id, status,
                  initial_message, source, session_ownership_generation)
             SELECT $1, agent_id, owner_id, $3, id, $2, 'running', '异常任务', 'user',
                    ownership_generation
             FROM hub_sessions WHERE id = $4",
        )
        .bind(conflict_run)
        .bind(conflict_turn)
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO run_events (event_id, run_id, event_type, role, content, payload)
             VALUES ($1, $2, 'message', 'user', '异常任务', '{}'::jsonb)",
        )
        .bind(Uuid::new_v4())
        .bind(conflict_run)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        // B 的 queued 消息。
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, turn_id, run_id, role, message_kind,
                  content, payload, delivery_mode, delivery_state)
             VALUES ($1, $2, $3, $4, 'user', 'message', 'B 的消息', '{}'::jsonb,
                     'next_turn', 'queued')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(conflict_turn)
        .bind(conflict_run)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // reject（incumbent = fixture.run，不同回合）。
        let rejected = runtime_reject_claimed_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(conflict_run),
            runtime_write_generation(
                1,
                RejectClaimedRunRequest {
                    incumbent_run_id: Some(fixture.run_id),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(rejected.status, "failed");

        // B 终态、B 回合终态。
        let (status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(conflict_run)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(status, "failed");
        let (turn_status,): (String,) =
            sqlx::query_as("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(conflict_turn)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(turn_status, "failed");
        // A（incumbent）完全不变：仍 running（已 claim）且未被触碰。
        let (a_status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(a_status, "running");
        // B 的 queued 消息迁移到下一 pending run（不滞留、不丢）。
        let (msg_run,): (Option<Uuid>,) = sqlx::query_as(
            "SELECT run_id FROM hub_session_messages
             WHERE session_id = $1 AND content = 'B 的消息'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert!(
            msg_run.is_some_and(|run| run != conflict_run),
            "queued message must move off the rejected run"
        );

        // 幂等：再次 reject 返回现有状态（failed），不 409。
        let again = runtime_reject_claimed_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(conflict_run),
            runtime_write_generation(
                1,
                RejectClaimedRunRequest {
                    incumbent_run_id: Some(fixture.run_id),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(again.status, "failed");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn reject_claimed_run_same_turn_preserves_incumbent_turn_and_steer(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // B 异常复用 A 的回合（同 turn）。
        let conflict_run = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, runtime_id, hub_session_id, hub_turn_id, status,
                  initial_message, source, session_ownership_generation)
             SELECT $1, agent_id, owner_id, $3, id, $2, 'running', '异常复用回合', 'user',
                    ownership_generation
             FROM hub_sessions WHERE id = $4",
        )
        .bind(conflict_run)
        .bind(fixture.turn_id)
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO run_events (event_id, run_id, event_type, role, content, payload)
             VALUES ($1, $2, 'message', 'user', '异常复用回合', '{}'::jsonb)",
        )
        .bind(Uuid::new_v4())
        .bind(conflict_run)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        // B 的 next_turn queued 消息 + B 的 steer queued 消息。
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, turn_id, run_id, role, message_kind,
                  content, payload, delivery_mode, delivery_state, expected_native_turn_id)
             VALUES ($1, $2, $3, $4, 'user', 'message', 'B 的 next_turn', '{}'::jsonb,
                     'next_turn', 'queued', NULL),
                    ($5, $2, $3, $4, 'user', 'message', 'B 的 steer', '{}'::jsonb,
                     'steer', 'queued', 'native-turn-x')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(conflict_run)
        .bind(Uuid::new_v4())
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let rejected = runtime_reject_claimed_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(conflict_run),
            runtime_write_generation(
                1,
                RejectClaimedRunRequest {
                    incumbent_run_id: Some(fixture.run_id),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(rejected.status, "failed");
        // A 的回合保持（未终态）。
        let (turn_status,): (String,) =
            sqlx::query_as("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(fixture.turn_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(turn_status, "pending");
        // B 的 next_turn 消息迁移到 A；steer 插话保持归 A（回合不变）。
        let (nt_run, nt_turn): (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            "SELECT run_id, turn_id FROM hub_session_messages
             WHERE session_id = $1 AND content = 'B 的 next_turn'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            nt_run,
            Some(fixture.run_id),
            "next_turn message moves to incumbent run"
        );
        assert_eq!(nt_turn, Some(fixture.turn_id));
        let (steer_run,): (Option<Uuid>,) = sqlx::query_as(
            "SELECT run_id FROM hub_session_messages
             WHERE session_id = $1 AND content = 'B 的 steer'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            steer_run,
            Some(fixture.run_id),
            "steer interjection stays with the incumbent Turn"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn reject_claimed_run_fallback_without_incumbent_moves_messages(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // B running，无 incumbent（3b 兜底）。
        let conflict_run = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, runtime_id, hub_session_id, hub_turn_id, status,
                  initial_message, source, session_ownership_generation)
             SELECT $1, agent_id, owner_id, $3, id, $2, 'running', '兜底任务', 'user',
                    ownership_generation
             FROM hub_sessions WHERE id = $4",
        )
        .bind(conflict_run)
        .bind(fixture.turn_id)
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, turn_id, run_id, role, message_kind,
                  content, payload, delivery_mode, delivery_state)
             VALUES ($1, $2, $3, $4, 'user', 'message', '兜底消息', '{}'::jsonb,
                     'next_turn', 'queued')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(conflict_run)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let rejected = runtime_reject_claimed_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(conflict_run),
            runtime_write_generation(
                1,
                RejectClaimedRunRequest {
                    incumbent_run_id: None,
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(rejected.status, "failed");
        let (msg_run,): (Option<Uuid>,) = sqlx::query_as(
            "SELECT run_id FROM hub_session_messages
             WHERE session_id = $1 AND content = '兜底消息'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert!(
            msg_run.is_some_and(|run| run != conflict_run),
            "fallback must not strand messages"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_terminates_run_and_creates_operation_with_held_messages(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // 用户消息（queued steer）挂在 A 上。
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, turn_id, run_id, role, message_kind,
                  content, payload, delivery_mode, delivery_state, expected_native_turn_id)
             VALUES ($1, $2, $3, $4, 'user', 'message', '停止前的消息', '{}'::jsonb,
                     'steer', 'queued', 'native-turn-x')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // 用户认证（session cookie）。
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);

        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers.clone(),
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "force-stop-test-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(dto.run_id, fixture.run_id);
        assert_eq!(dto.state, "pending");
        assert_eq!(dto.target_runtime_id, Some(fixture.runtime_id));

        // A 终态 interrupted、回合终态。
        let (run_status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(run_status, "interrupted");
        let (turn_status,): (String,) =
            sqlx::query_as("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(fixture.turn_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(turn_status, "interrupted");
        // 会话：强制停止中（保持归属与 generation——hub 权威 + 上报兜底）、active_turn 清。
        let (gen, lifecycle, active_turn): (i64, String, Option<Uuid>) = sqlx::query_as(
            "SELECT ownership_generation, lifecycle_status, active_turn_id
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(gen, 1, "force stop does not bump generation");
        assert_eq!(lifecycle, "force_stopping");
        assert_eq!(active_turn, None);
        // operation：pending（命令经 WebSocket 推送；连接不在线由 10 秒上报兜底重推）。
        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "pending");
        // 消息无需特殊处理：仍保持原状态（DB 全量历史，恢复时重建包含）。
        let (msg_state,): (String,) = sqlx::query_as(
            "SELECT delivery_state FROM hub_session_messages
             WHERE session_id = $1 AND content = '停止前的消息'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(msg_state, "queued", "messages stay untouched in DB");

        // 幂等：同 request_id 重复 → 返回同一 operation（不重复创建）。
        let (status2, dto2) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers.clone(),
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "force-stop-test-1".into(),
                expected_generation: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status2, StatusCode::ACCEPTED);
        assert_eq!(dto2.operation_id, dto.operation_id);
        let op_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM force_stop_operation WHERE request_id = 'force-stop-test-1'",
        )
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(op_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_bundle_upload_rejects_mismatched_checksum(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);
        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers,
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "checksum-mismatch-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);

        // mock S3：记录 PUT 对象与 DELETE。
        let objects = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let deleted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let object_app = Router::new()
            .route(
                "/bundle-bucket/{*key}",
                axum::routing::put({
                    let objects = Arc::clone(&objects);
                    move |Path(key): Path<String>, body: Body| {
                        let objects = Arc::clone(&objects);
                        async move {
                            let bytes = axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                            objects.lock().unwrap().insert(key, bytes);
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/bundle-bucket/{*key}",
                axum::routing::delete({
                    let deleted = Arc::clone(&deleted);
                    move |Path(key): Path<String>| {
                        let deleted = Arc::clone(&deleted);
                        async move {
                            deleted.lock().unwrap().push(key);
                            StatusCode::OK
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);

        // 声明 checksum 与实际 body 不符 → 409，对象被删除，状态不变。
        let bytes = Bytes::from_static(b"actual body bytes");
        let mut headers = bearer_headers(&fixture.runtime_token);
        for (name, value) in [
            ("content-length", bytes.len().to_string()),
            (
                "x-agent-hub-bundle-sha256",
                "0".repeat(64), // 错误 checksum
            ),
            ("x-agent-hub-bundle-size", bytes.len().to_string()),
        ] {
            headers.insert(
                name.parse::<HeaderName>().unwrap(),
                value.parse::<HeaderValue>().unwrap(),
            );
        }
        let err = runtime_upload_force_stop_bundle(
            State(state.clone()),
            headers,
            Path(dto.operation_id),
            Body::from(bytes),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            deleted.lock().unwrap().len(),
            1,
            "mismatched object must be deleted"
        );
        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "pending", "mismatched upload must not commit");
        let (lifecycle,): (String,) =
            sqlx::query_as("SELECT lifecycle_status FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(lifecycle, "force_stopping");
        object_server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_snapshot_lost_ack_is_atomic_and_fenced(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);
        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers,
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "ack-fence-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);

        // 另一个 runtime 的 snapshot_lost ack：不得改变任何状态。
        let other_runtime_id = Uuid::new_v4();
        crate::runtime_ws::apply_snapshot_lost(&fixture.state, other_runtime_id, dto.operation_id)
            .await;
        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "pending", "foreign runtime ack must be ignored");
        let (lifecycle,): (String,) =
            sqlx::query_as("SELECT lifecycle_status FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(lifecycle, "force_stopping");

        // 正确 runtime 的 snapshot_lost ack：原子终态。
        crate::runtime_ws::apply_snapshot_lost(
            &fixture.state,
            fixture.runtime_id,
            dto.operation_id,
        )
        .await;
        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "snapshot_lost");
        let (lifecycle, owner): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT lifecycle_status, runtime_owner_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(lifecycle, "offline");
        assert_eq!(owner, None);
        let (run_status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(run_status, "interrupted");

        // 重复 ack（已终态）→ 忽略，状态不变。
        crate::runtime_ws::apply_snapshot_lost(
            &fixture.state,
            fixture.runtime_id,
            dto.operation_id,
        )
        .await;
        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "snapshot_lost", "terminal ack must be idempotent");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_ws_delivers_command_and_commits_snapshot(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        // 用户认证 + 发起强制停止 → operation pending。
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);
        // 真实服务器 + WS 客户端连接（先连再停：命令推送只在连接在线时生效）。
        let app = build_router((*fixture.state).clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .await
                .unwrap();
        });
        let url = format!("ws://{address}/api/runtime/ws");
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", fixture.runtime_token)).unwrap(),
        );
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();

        // 连接建立后发起强制停止 → operation pending + 命令推送。
        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers,
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "ws-test-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(dto.state, "pending");

        // 应收到 force_stop 命令（真实 operation_id）。
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .expect("command timeout")
            .expect("stream closed")
            .unwrap();
        let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
            panic!("expected text command");
        };
        let command: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(command["type"], "command");
        assert_eq!(command["kind"], "force_stop");
        assert_eq!(command["operation_id"], dto.operation_id.to_string());
        assert_eq!(command["session_id"], fixture.hub_session_id.to_string());
        assert_eq!(command["require_snapshot"], true);

        // runtime ack ok（接管）。
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({ "type": "ack", "operation_id": dto.operation_id, "status": "ok" })
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();

        // 上传快照 → 原子提交：operation→succeeded、会话→offline、kind=force_stop。
        let objects = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let object_app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let objects = Arc::clone(&objects);
                move |Path(key): Path<String>, body: Body| {
                    let objects = Arc::clone(&objects);
                    async move {
                        let bytes = axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                        objects.lock().unwrap().insert(key, bytes);
                        StatusCode::OK
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);

        let bytes = Bytes::from_static(b"ws snapshot body");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let mut headers = bearer_headers(&fixture.runtime_token);
        for (name, value) in [
            ("content-length", bytes.len().to_string()),
            ("x-agent-hub-bundle-sha256", checksum),
            ("x-agent-hub-bundle-size", bytes.len().to_string()),
        ] {
            headers.insert(
                name.parse::<HeaderName>().unwrap(),
                value.parse::<HeaderValue>().unwrap(),
            );
        }
        let upload_status = runtime_upload_force_stop_bundle(
            State(state.clone()),
            headers,
            Path(dto.operation_id),
            Body::from(bytes),
        )
        .await
        .unwrap();
        assert_eq!(upload_status, StatusCode::NO_CONTENT);

        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "succeeded");
        let (lifecycle, owner, kind, gen): (String, Option<Uuid>, Option<String>, i64) =
            sqlx::query_as(
                "SELECT lifecycle_status, runtime_owner_id, current_bundle_kind,
                        current_bundle_generation
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(lifecycle, "offline");
        assert_eq!(owner, None);
        assert_eq!(kind.as_deref(), Some("force_stop"));
        assert_eq!(gen, 1);

        // 上报持有列表：hub 对账（会话已 offline，非权威 → abandon 命令）。
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({
                    "type": "owned_sessions",
                    "sessions": [{
                        "session_id": fixture.hub_session_id,
                        "run_id": fixture.run_id,
                    }],
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .expect("abandon timeout")
            .expect("stream closed")
            .unwrap();
        let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
            panic!("expected text");
        };
        let command: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(command["type"], "command");
        assert_eq!(command["kind"], "abandon");
        assert_eq!(command["require_snapshot"], false);

        socket.close(None).await.unwrap();
        server.abort();
        object_server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_snapshot_lost_ack_and_5min_expiry(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);
        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers,
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "ws-test-lost-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);

        // 直接把 created_at 改老，触发 5 分钟兜底。
        sqlx::query(
            "UPDATE force_stop_operation SET created_at = now() - interval '6 minutes'
             WHERE operation_id = $1",
        )
        .bind(dto.operation_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        crate::runtime_ws::expire_stuck_force_stops(&fixture.state).await;

        let (op_state,): (String,) =
            sqlx::query_as("SELECT state FROM force_stop_operation WHERE operation_id = $1")
                .bind(dto.operation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(op_state, "snapshot_lost");
        let (lifecycle, owner): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT lifecycle_status, runtime_owner_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(lifecycle, "offline");
        assert_eq!(owner, None);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_stop_bundle_upload_commits_operation_and_offlines_session(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // 1. 发起强制停止 → operation pending。
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let user_headers = HeaderMap::from_iter([(
            header::COOKIE,
            format!("agent_hub_session={session_token}")
                .parse()
                .unwrap(),
        )]);
        let (status, dto) = force_stop_hub_run(
            State(fixture.state.clone()),
            user_headers.clone(),
            Path(fixture.run_id),
            Json(ForceStopRequest {
                request_id: "force-stop-upload-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(dto.state, "pending");

        // 2. mock S3：PUT 记录对象，DELETE 记录被删 key。
        let objects = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let deleted = Arc::new(std::sync::Mutex::new(Vec::new()));
        let object_app = Router::new()
            .route(
                "/bundle-bucket/{*key}",
                axum::routing::put({
                    let objects = Arc::clone(&objects);
                    move |Path(key): Path<String>, body: Body| {
                        let objects = Arc::clone(&objects);
                        async move {
                            let bytes = axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                            objects.lock().unwrap().insert(key, bytes);
                            StatusCode::OK
                        }
                    }
                }),
            )
            .route(
                "/bundle-bucket/{*key}",
                axum::routing::delete({
                    let deleted = Arc::clone(&deleted);
                    move |Path(key): Path<String>| {
                        let deleted = Arc::clone(&deleted);
                        async move {
                            deleted.lock().unwrap().push(key);
                            StatusCode::OK
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);

        // 3. 预置旧 bundle：DB 指针指向旧对象（kind=checkpoint，验证任意旧 bundle
        // 都会被新快照替换删除）。
        let old_key = format!("sessions/{}/bundle-old.tar.zst", fixture.hub_session_id);
        sqlx::query(
            "UPDATE hub_sessions
             SET current_bundle_generation = 1,
                 current_bundle_object_key = $1,
                 current_bundle_checksum_sha256 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 current_bundle_size_bytes = 3,
                 current_bundle_history_checkpoint = 0,
                 current_bundle_ownership_generation = 1,
                 current_bundle_producing_engine_version = '0.104.0',
                 current_bundle_created_at = now(),
                 current_bundle_runtime_id = $2,
                 current_bundle_kind = 'checkpoint'
             WHERE id = $3",
        )
        .bind(&old_key)
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        objects
            .lock()
            .unwrap()
            .insert(old_key.clone(), b"old".to_vec());

        // 4. 上传停止快照。
        let bytes = Bytes::from_static(b"force stop snapshot body");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let mut headers = bearer_headers(&fixture.runtime_token);
        for (name, value) in [
            ("content-length", bytes.len().to_string()),
            ("x-agent-hub-bundle-sha256", checksum.clone()),
            ("x-agent-hub-bundle-size", bytes.len().to_string()),
        ] {
            headers.insert(
                name.parse::<HeaderName>().unwrap(),
                value.parse::<HeaderValue>().unwrap(),
            );
        }
        let status = runtime_upload_force_stop_bundle(
            State(state.clone()),
            headers,
            Path(dto.operation_id),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        // 5. operation → succeeded；会话 offline、归属释放、kind=force_stop、generation=1。
        let (op_state, uploaded_at): (String, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT state, snapshot_uploaded_at FROM force_stop_operation
                 WHERE operation_id = $1",
            )
            .bind(dto.operation_id)
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(op_state, "succeeded");
        assert!(uploaded_at.is_some());
        let (lifecycle, owner, kind, generation, stored_key): (
            String,
            Option<Uuid>,
            Option<String>,
            i64,
            String,
        ) = sqlx::query_as(
            "SELECT lifecycle_status, runtime_owner_id, current_bundle_kind,
                    current_bundle_generation, current_bundle_object_key
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(lifecycle, "offline");
        assert_eq!(owner, None);
        assert_eq!(kind.as_deref(), Some("force_stop"));
        assert_eq!(
            generation, 2,
            "force stop snapshot advances the bundle generation"
        );
        assert_eq!(
            stored_key,
            format!(
                "sessions/{}/force-stop-{}.tar.zst",
                fixture.hub_session_id, dto.operation_id
            )
        );
        // 对象确实上传成功。
        let stored = objects.lock().unwrap();
        assert_eq!(
            stored.get(&stored_key),
            Some(&bytes.to_vec()),
            "force stop snapshot object must be stored"
        );
        drop(stored);

        // 6. 旧 force-stop 对象已删除（旧 key 与新 key 不同）。
        assert!(
            deleted.lock().unwrap().contains(&old_key),
            "replaced bundle object must be deleted"
        );

        // 7. 重复上传同一 operation（已完成）→ 409，不重复提交。
        let mut headers2 = bearer_headers(&fixture.runtime_token);
        for (name, value) in [
            ("content-length", bytes.len().to_string()),
            ("x-agent-hub-bundle-sha256", checksum.clone()),
            ("x-agent-hub-bundle-size", bytes.len().to_string()),
        ] {
            headers2.insert(
                name.parse::<HeaderName>().unwrap(),
                value.parse::<HeaderValue>().unwrap(),
            );
        }
        let err = runtime_upload_force_stop_bundle(
            State(state.clone()),
            headers2,
            Path(dto.operation_id),
            Body::from(bytes),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        object_server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn run_completion_is_idempotent_and_rejects_conflicting_payload(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // 第一次完成（commit 后响应可能丢失）。
        let first = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("native-1".into()),
                    work_dir_ref: Some("workdir-1".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first.status, "completed");
        // 重试（同 payload）：返回同一结果，不 409、不重复迁移。
        let retry = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("native-1".into()),
                    work_dir_ref: Some("workdir-1".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(retry.status, "completed");
        assert_eq!(retry.native_session_id.as_deref(), Some("native-1"));
        // 冲突 payload（同 status 不同内容）：409。
        let error = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("native-2".into()),
                    work_dir_ref: Some("workdir-2".into()),
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        // status(completed) 事件只写入一次（幂等重试不追加；claim 的 running 事件不计）。
        let (event_count,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM run_events
             WHERE run_id = $1 AND event_type = 'status' AND content = 'completed'",
        )
        .bind(claim.run.id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(event_count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_keepalive_does_not_release_idle_sessions(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'online'
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        assert_eq!(
            runtime_keepalive(
                State(fixture.state.clone()),
                bearer_headers(&fixture.runtime_token),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        let (owner, generation, status): (Option<Uuid>, i64, String) = sqlx::query_as(
            "SELECT runtime_owner_id, ownership_generation, lifecycle_status
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            owner,
            Some(fixture.runtime_id),
            "keepalive must not release owned Sessions"
        );
        assert_eq!(
            generation, 1,
            "keepalive must not bump ownership generation"
        );
        assert_eq!(
            status, "online",
            "keepalive must not change Session lifecycle"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_keepalive_rejects_revoked_credentials(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE runtimes SET credential_revoked_at = now() WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let error = runtime_keepalive(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_keepalive_preserves_draining_and_pending_credentials(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(
            runtime_keepalive(
                State(fixture.state.clone()),
                bearer_headers(&fixture.runtime_token),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        let (status,): (String,) = sqlx::query_as("SELECT status FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(status, "draining", "keepalive must preserve draining");

        // pending token 可认证但不激活（token_hash 不变，pending 保留）。
        let pending = format!("ahrt_pending_{}", Uuid::new_v4().simple());
        sqlx::query(
            "UPDATE runtimes
             SET pending_token_hash = $1, pending_token_created_at = now()
             WHERE id = $2",
        )
        .bind(sha256_hex(&pending))
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            runtime_keepalive(State(fixture.state.clone()), bearer_headers(&pending))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        let (token_hash, pending_hash): (String, Option<String>) =
            sqlx::query_as("SELECT token_hash, pending_token_hash FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let pending_hash_expected = sha256_hex(&pending);
        assert_eq!(
            token_hash,
            sha256_hex(&fixture.runtime_token),
            "pending token must not be activated by keepalive"
        );
        assert_eq!(
            pending_hash.as_deref(),
            Some(pending_hash_expected.as_str()),
            "pending token must remain pending"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn claim_terminates_stale_active_turn_and_clears_pointer(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        // 造一个残留的旧活动 turn（异常/崩溃路径）并挂到会话 active_turn_id。
        let stale_turn_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'running', 1)",
        )
        .bind(stale_turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = $2
             WHERE id = $3",
        )
        .bind(fixture.runtime_id)
        .bind(stale_turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // claim 当前 run（source = user，非 tool_result 续跑）。
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        // 旧残留 turn 被终结为 failed 且结束时间已记录。
        let (status, ended_at): (String, Option<chrono::DateTime<Utc>>) =
            sqlx::query_as("SELECT status, ended_at FROM hub_session_turns WHERE id = $1")
                .bind(stale_turn_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(
            status, "failed",
            "stale active turn must be terminated on claim"
        );
        assert!(
            ended_at.is_some(),
            "stale active turn must record an end time"
        );
        // 会话 active_turn_id 已清（当前 claim 的 turn 尚未 begin）。
        let (active_turn_id, lifecycle): (Option<Uuid>, String) = sqlx::query_as(
            "SELECT active_turn_id, lifecycle_status FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            active_turn_id, None,
            "claim must clear stale active turn pointer"
        );
        // 同一 runtime 重 claim：lifecycle 保持原状态（online），gen 不 bump。
        assert_eq!(lifecycle, "online");
        assert_eq!(claim.run.id, fixture.run_id);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn claim_does_not_terminate_an_active_turn_still_referenced_by_a_running_run(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        // 同 Session 已有 running run（活动执行），其 turn 是会话 active_turn。
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'online',
                 active_turn_id = $2
             WHERE id = $3",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs SET status = 'running', runtime_id = $1,
                    session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_session_turns SET status = 'running' WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        // 新普通 pending run（source=user）。
        let pending_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;

        // 活动 turn 被 running run 引用：claim 被拒（NO_CONTENT），run 保持 pending，
        // 旧 Turn 保持 running、active_turn 指针保留。
        let ready_owned = vec![RuntimeOwnedSessionGenerationDto {
            session_id: fixture.hub_session_id,
            ownership_generation: 1,
        }];
        let response = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, ready_owned),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (run_status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(pending_run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(run_status, "pending", "unclaimed Run must stay pending");
        let (turn_status,): (String,) =
            sqlx::query_as("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(fixture.turn_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(turn_status, "running", "active Turn must not be terminated");
        let (active_turn,): (Option<Uuid>,) =
            sqlx::query_as("SELECT active_turn_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(active_turn, Some(fixture.turn_id));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn tool_result_continuation_may_claim_waiting_parent(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        // 父 run 处于 waiting_tool（等待审批）；tool_result 续跑 run 允许 claim。
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'online',
                 active_turn_id = $2
             WHERE id = $3",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs SET status = 'waiting_tool', runtime_id = $1,
                    session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        // 父 waiting_tool 的 turn 成为活动 turn。
        sqlx::query("UPDATE hub_session_turns SET status = 'running' WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let continuation_id = Uuid::new_v4();
        let continuation_turn = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'pending', 1)",
        )
        .bind(continuation_turn)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, hub_session_id, hub_turn_id, parent_run_id, status,
                  initial_message, source, session_ownership_generation)
             VALUES ($1, $2, $3, $4, $5, $6, 'pending', '工具结果续跑', 'integration:tool_result', 1)",
        )
        .bind(continuation_id)
        .bind(fixture.agent_id)
        .bind(
            sqlx::query_scalar::<_, Uuid>("SELECT owner_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
        )
        .bind(fixture.hub_session_id)
        .bind(continuation_turn)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(
            claim.run.id, continuation_id,
            "integration:tool_result continuation must be claimable while its parent waits"
        );
        // 续跑不清 active turn（父 waiting_tool 仍持有）。
        let (active_turn,): (Option<Uuid>,) =
            sqlx::query_as("SELECT active_turn_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(active_turn, Some(fixture.turn_id));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn tool_result_continuation_with_unrelated_parent_is_rejected(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        // 父 run 处于 waiting_tool（本会话的活动 run）。
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'online',
                 active_turn_id = $2
             WHERE id = $3",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs SET status = 'waiting_tool', runtime_id = $1,
                    session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_session_turns SET status = 'running' WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        // 已完成的历史父 run（非活动）：continuation 的 parent 指向它时例外不匹配，
        // 因为活动 run 是 fixture.run（waiting_tool），claim 必须被拒。
        let historical_parent = Uuid::new_v4();
        let historical_parent_turn = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'completed', 1)",
        )
        .bind(historical_parent_turn)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, hub_session_id, hub_turn_id, status,
                  initial_message, source, session_ownership_generation)
             SELECT $1, agent_id, owner_id, id, $2, 'completed', '历史父任务', 'user',
                    ownership_generation
             FROM hub_sessions WHERE id = $3",
        )
        .bind(historical_parent)
        .bind(historical_parent_turn)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let continuation_turn = Uuid::new_v4();
        // 父 run 处于 waiting_tool 且带 pending integration tool request（阻断条件）。
        sqlx::query(
            "INSERT INTO integration_tool_requests
                 (id, hub_session_id, run_id, position, tool_name, arguments, status, expires_at)
             VALUES ($1, $2, $3, 0, 'run_shelves_operation', '{}'::jsonb, 'pending', now() + interval '5 minutes')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_turns (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'pending', 1)",
        )
        .bind(continuation_turn)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, hub_session_id, hub_turn_id, parent_run_id, status,
                  initial_message, source, session_ownership_generation)
             SELECT $1, agent_id, owner_id, $4, $2, $3, 'pending', '错误父续跑',
                    'integration:tool_result', ownership_generation
             FROM hub_sessions WHERE id = $4",
        )
        .bind(Uuid::new_v4())
        .bind(continuation_turn)
        .bind(historical_parent)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let ready_owned = vec![RuntimeOwnedSessionGenerationDto {
            session_id: fixture.hub_session_id,
            ownership_generation: 1,
        }];
        let response = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, ready_owned),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(
            response.status(),
            StatusCode::NO_CONTENT,
            "tool_result continuation with an unrelated parent must be rejected"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn heartbeat_preserves_restoring_lifecycle_and_saving_fields(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'restoring',
                 recovery_source = 'local_workspace'
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        // runtime 上报 online（本地 metadata 滞后）：Hub 必须保持 restoring。
        let _ = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: false,
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
        let (lifecycle, saving_checkpoint, saving_reason, last_checkpoint): (
            String,
            Option<i64>,
            Option<String>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT lifecycle_status, saving_history_checkpoint, saving_reason,
                    last_checkpoint_ownership_generation
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            lifecycle, "restoring",
            "online heartbeat must not override restoring"
        );
        // 约束保证非 saving 状态下 saving_* 全 NULL；heartbeat 不得破坏该不变式。
        assert_eq!(saving_checkpoint, None);
        assert_eq!(saving_reason, None);
        // last_checkpoint_* 在非 saving 心跳下原样保留（fixture 初始为 NULL）。
        assert_eq!(last_checkpoint, None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn replay_events_are_generation_fenced_and_ordered(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        // 写入两条重建事件（message user + item dynamicToolCall completed）。
        let ts = chrono::Utc::now();
        sqlx::query(
            "INSERT INTO run_events
                 (event_id, run_id, event_type, role, content, payload, created_at)
             VALUES ($1, $2, 'message', 'user', '重建测试', '{}'::jsonb, $3)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.run_id)
        .bind(ts)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO run_events
                 (event_id, run_id, event_type, role, payload, created_at)
             VALUES ($1, $2, 'item', 'assistant',
                     '{\"phase\":\"completed\",\"item_type\":\"dynamicToolCall\",
                       \"item_id\":\"call_00_x|item_y\",\"tool\":\"read\",
                       \"arguments\":{},\"output\":\"结果\",\"success\":true}'::jsonb, $3)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.run_id)
        .bind(ts)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1, lifecycle_status = 'restoring',
                 recovery_source = 'local_workspace'
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // 正确代际可读。
        let mut headers = bearer_headers(&fixture.runtime_token);
        headers.insert("x-agent-hub-ownership-generation", "1".parse().unwrap());
        let events = get_session_replay_events(
            State(fixture.state.clone()),
            headers.clone(),
            Path(fixture.hub_session_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "message");
        assert_eq!(events[0].run_id, fixture.run_id);
        assert_eq!(events[1].event_type, "item");
        assert_eq!(events[1].payload["item_id"], "call_00_x|item_y");

        // 错误代际被拒（403）。
        let mut headers = bearer_headers(&fixture.runtime_token);
        headers.insert("x-agent-hub-ownership-generation", "99".parse().unwrap());
        let error = get_session_replay_events(
            State(fixture.state.clone()),
            headers,
            Path(fixture.hub_session_id),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_tolerates_stale_owned_sessions_instead_of_conflicting(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'online' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "failed".into(),
                    native_session_id: None,
                    work_dir_ref: None,
                },
            ),
        )
        .await
        .unwrap();
        // Release the session out from under the runtime (e.g. crash recovery).
        let _ = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: true,
            }),
        )
        .await
        .unwrap();

        // The runtime still reports the old generation as owned.
        let heartbeat = runtime_heartbeat(
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
        .unwrap()
        .0;
        assert!(
            heartbeat
                .owned_sessions
                .iter()
                .all(|session| session.session_id != fixture.hub_session_id),
            "released Session must not appear in the owned snapshot"
        );
        let runtime_state: (String, chrono::DateTime<Utc>) =
            sqlx::query_as("SELECT status, last_heartbeat_at FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(runtime_state.0, "online");
        assert!(
            runtime_state.1 > Utc::now() - chrono::Duration::seconds(30),
            "heartbeat must keep the Runtime alive"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_reaper_reclaims_saving_sessions_and_clears_checkpoint_fields(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let attempt_id = Uuid::new_v4();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'saving',
                 saving_history_checkpoint = 1,
                 saving_ownership_generation = 1,
                 saving_reason = 'idle',
                 saving_checkpoint_attempt_id = $1
             WHERE id = $2",
        )
        .bind(attempt_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runtimes SET last_heartbeat_at = now() - interval '2 minutes' WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        reap_stale_runtimes(&fixture.state.pool).await.unwrap();

        let session: (Option<Uuid>, String, Option<Uuid>, Option<String>) = sqlx::query_as(
            "SELECT runtime_owner_id, lifecycle_status,
                    saving_checkpoint_attempt_id, saving_reason
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(session.0, None);
        assert!(matches!(
            session.1.as_str(),
            "offline" | "waiting_for_runtime"
        ));
        assert_eq!(session.2, None);
        assert_eq!(session.3, None);
        let runtime_status: String =
            sqlx::query_scalar("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(runtime_status, "offline");
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_reaper_fails_running_runs_and_allows_reclaiming_pending_work(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(claim.run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "running"
        );
        sqlx::query(
            "UPDATE runtimes SET last_heartbeat_at = now() - interval '2 minutes' WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        reap_stale_runtimes(&fixture.state.pool).await.unwrap();

        let failed_run: String = sqlx::query_scalar("SELECT status FROM runs WHERE id = $1")
            .bind(claim.run.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(failed_run, "failed");
        let session_owner: Option<Uuid> =
            sqlx::query_scalar("SELECT runtime_owner_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(session_owner, None);
        let obligation: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM runtime_session_salvage_obligations
             WHERE runtime_id = $1 AND session_id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(obligation, 1);

        // Bring the Runtime back and let it reclaim queued work.
        sqlx::query(
            "UPDATE runtimes SET status = 'online', last_heartbeat_at = now() WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let _ = insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        let reclaimed = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(reclaimed.run.hub_session_id, Some(fixture.hub_session_id));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE status = 'pending'")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_salvage_upload_is_idempotent_and_rejects_mismatched_obligations(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let stored = Arc::new(std::sync::Mutex::new(Vec::new()));
        let object_app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let stored = Arc::clone(&stored);
                move |body: Body| {
                    let stored = Arc::clone(&stored);
                    async move {
                        *stored.lock().unwrap() =
                            axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);

        let bytes = Bytes::from_static(b"idempotent salvage body");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let created_at = Utc::now();
        let checkpoint_attempt_id = Uuid::new_v4();
        let upload_headers = |generation: i64| {
            let mut headers = bearer_headers(&fixture.runtime_token);
            for (name, value) in [
                ("content-length", bytes.len().to_string()),
                ("x-agent-hub-ownership-generation", "2".into()),
                (
                    "x-agent-hub-checkpoint-attempt-id",
                    checkpoint_attempt_id.to_string(),
                ),
                ("x-agent-hub-bundle-generation", generation.to_string()),
                ("x-agent-hub-bundle-sha256", checksum.clone()),
                ("x-agent-hub-bundle-size", bytes.len().to_string()),
                ("x-agent-hub-history-checkpoint", "5".into()),
                ("x-agent-hub-producing-engine-version", "0.104.0".into()),
                ("x-agent-hub-bundle-created-at", created_at.to_rfc3339()),
            ] {
                headers.insert(
                    HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_str(&value).unwrap(),
                );
            }
            headers
        };

        // No obligation yet: upload must be rejected.
        let rejected = runtime_salvage_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            upload_headers(2),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(rejected.status, StatusCode::CONFLICT);

        sqlx::query(
            "INSERT INTO runtime_session_salvage_obligations
                 (runtime_id, session_id, ownership_generation, history_checkpoint,
                  bundle_generation)
             VALUES ($1, $2, 2, 5, 2)",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // Wrong bundle generation: rejected.
        let mismatched = runtime_salvage_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            upload_headers(3),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatched.status, StatusCode::CONFLICT);

        // Correct upload then replay: both succeed, one binding.
        for _ in 0..2 {
            let response = runtime_salvage_session_bundle(
                State(state.clone()),
                Path(fixture.hub_session_id),
                upload_headers(2),
                Body::from(bytes.clone()),
            )
            .await
            .unwrap()
            .0;
            assert_eq!(response.bundle_generation, 2);
        }
        let binding: (Option<i64>, Option<String>) = sqlx::query_as(
            "SELECT current_bundle_generation, current_bundle_checksum_sha256
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(binding, (Some(2), Some(checksum.clone())));
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_session_salvage_obligations
                 WHERE runtime_id = $1 AND session_id = $2",
            )
            .bind(fixture.runtime_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(*stored.lock().unwrap(), bytes);
        object_server.abort();
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_salvage_abandon_is_idempotent(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "INSERT INTO runtime_session_salvage_obligations
                 (runtime_id, session_id, ownership_generation, history_checkpoint,
                  bundle_generation)
             VALUES ($1, $2, 2, 5, 2)",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        for _ in 0..2 {
            let response = runtime_abandon_session_salvage(
                State(fixture.state.clone()),
                Path(fixture.hub_session_id),
                bearer_headers(&fixture.runtime_token),
                Json(AbandonRuntimeSalvageRequest {
                    ownership_generation: 2,
                }),
            )
            .await
            .unwrap();
            assert_eq!(response, StatusCode::NO_CONTENT);
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_session_salvage_obligations
                 WHERE runtime_id = $1 AND session_id = $2",
            )
            .bind(fixture.runtime_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_replays_interrupt_before_steer_for_the_terminating_turn(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query("UPDATE hub_sessions SET native_session_id = 'interrupt-thread' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE hub_session_turns SET interrupt_requested_at = now() WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let steer_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, expected_native_turn_id, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'must wait for next Turn',
                     'steer', 'queued', 'fixture-native-turn', $3, $4)",
        )
        .bind(steer_id)
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let heartbeat_request = RuntimeHeartbeatRequest {
            accepts_session_commands: true,
            ..RuntimeHeartbeatRequest::default()
        };

        let first = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request.clone()),
        )
        .await
        .unwrap()
        .0;
        let replay = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request),
        )
        .await
        .unwrap()
        .0;

        for heartbeat in [first, replay] {
            assert_eq!(heartbeat.session_commands.len(), 1);
            assert_eq!(
                heartbeat.session_commands[0],
                RuntimeSessionCommandDto {
                    command_id: fixture.turn_id,
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    command: "interrupt".into(),
                    run_id: Some(fixture.run_id),
                    turn_id: Some(fixture.turn_id),
                    native_session_id: Some("interrupt-thread".into()),
                    native_turn_id: Some("fixture-native-turn".into()),
                    message: None,
                    configuration_revision: None,
                    fingerprint: None,
                    execution_configuration: None,
                }
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM hub_session_messages WHERE id = $1"
            )
            .bind(steer_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "queued"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn stop_after_claim_requests_interrupt_that_heartbeat_replays(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(claim.run.status, "running");
        sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = 'native-claimed-stop', status = 'starting'
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET native_session_id = 'stop-claimed-thread', active_turn_id = $1
             WHERE id = $2",
        )
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let mut tx = fixture.state.pool.begin().await.unwrap();
        let run = request_run_interrupt_tx(&mut tx, fixture.run_id, fixture.hub_session_id)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(run.status, "running");
        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(heartbeat.session_commands.len(), 1);
        assert_eq!(heartbeat.session_commands[0].command, "interrupt");
        assert_eq!(heartbeat.session_commands[0].run_id, Some(fixture.run_id));
        assert_eq!(
            heartbeat.session_commands[0].native_turn_id.as_deref(),
            Some("native-claimed-stop")
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_interrupt_ack_is_generation_fenced_idempotent_and_stops_replay(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'interrupt-ack-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_session_turns SET interrupt_requested_at = now() WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let stale = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, fixture.turn_id)),
            runtime_write_generation(
                2,
                CompleteRuntimeSessionCommandRequest {
                    command: "interrupt".into(),
                    outcome: "interrupted".into(),
                    revision: None,
                    fingerprint: None,
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::FORBIDDEN);

        let acknowledge = || {
            runtime_complete_session_command(
                State(fixture.state.clone()),
                bearer_headers(&fixture.runtime_token),
                Path((fixture.hub_session_id, fixture.turn_id)),
                runtime_write_generation(
                    1,
                    CompleteRuntimeSessionCommandRequest {
                        command: "interrupt".into(),
                        outcome: "interrupted".into(),
                        revision: None,
                        fingerprint: None,
                    },
                ),
            )
        };
        let _ = acknowledge().await.unwrap();
        let acknowledged_at: DateTime<Utc> = sqlx::query_scalar(
            "SELECT interrupt_acknowledged_at FROM hub_session_turns WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let _ = acknowledge().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, DateTime<Utc>>(
                "SELECT interrupt_acknowledged_at FROM hub_session_turns WHERE id = $1"
            )
            .bind(fixture.turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            acknowledged_at
        );

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(heartbeat
            .session_commands
            .iter()
            .all(|command| command.command != "interrupt"));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_replays_delivering_steer_before_interrupt(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'interrupt-race-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_session_turns SET interrupt_requested_at = now() WHERE id = $1")
            .bind(fixture.turn_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let steer_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, expected_native_turn_id, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'possibly already applied',
                     'steer', 'delivering', 'fixture-native-turn', $3, $4)",
        )
        .bind(steer_id)
        .bind(fixture.hub_session_id)
        .bind(fixture.turn_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(heartbeat.session_commands.len(), 2);
        assert_eq!(heartbeat.session_commands[0].command, "steer");
        assert_eq!(heartbeat.session_commands[0].command_id, steer_id);
        assert_eq!(heartbeat.session_commands[1].command, "interrupt");
        assert_eq!(heartbeat.session_commands[1].command_id, fixture.turn_id);

        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, steer_id)),
            runtime_write_generation(
                1,
                CompleteRuntimeSessionCommandRequest {
                    command: "steer".into(),
                    outcome: "applied".into(),
                    revision: None,
                    fingerprint: None,
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM hub_session_messages WHERE id = $1"
            )
            .bind(steer_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "delivered"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_steer_commands_replay_after_lost_ack_and_complete_in_sequence(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let first_message_id = Uuid::new_v4();
        let second_message_id = Uuid::new_v4();
        for (message_id, content) in [
            (first_message_id, "first steer"),
            (second_message_id, "second steer"),
        ] {
            sqlx::query(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content, delivery_mode,
                      delivery_state, expected_native_turn_id, turn_id, run_id)
                 VALUES ($1, $2, 'user', 'message', $3, 'steer', 'queued',
                         'fixture-native-turn', $4, $5)",
            )
            .bind(message_id)
            .bind(fixture.hub_session_id)
            .bind(content)
            .bind(fixture.turn_id)
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        let heartbeat_request = RuntimeHeartbeatRequest {
            pending_credential_hash: None,
            accepts_session_commands: true,
            owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                checkpoint_reason: None,
            }],
            cleaned_sessions: Vec::new(),
        };

        let first_delivery = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request.clone()),
        )
        .await
        .unwrap()
        .0;
        let replay = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request),
        )
        .await
        .unwrap()
        .0;
        let command_ids = |response: &RuntimeHeartbeatResponse| {
            response
                .session_commands
                .iter()
                .filter(|command| command.command == "steer")
                .map(|command| {
                    (
                        command.command_id,
                        command.message.as_ref().unwrap().sequence,
                    )
                })
                .collect::<Vec<_>>()
        };
        let delivered = command_ids(&first_delivery);
        assert_eq!(delivered.len(), 2);
        assert_eq!(delivered, command_ids(&replay));
        assert_eq!(
            delivered.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            vec![first_message_id, second_message_id]
        );

        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, first_message_id)),
            runtime_write_generation(
                1,
                CompleteRuntimeSessionCommandRequest {
                    command: "steer".into(),
                    outcome: "applied".into(),
                    revision: None,
                    fingerprint: None,
                },
            ),
        )
        .await
        .unwrap();
        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, second_message_id)),
            runtime_write_generation(
                1,
                CompleteRuntimeSessionCommandRequest {
                    command: "steer".into(),
                    outcome: "turn_ended".into(),
                    revision: None,
                    fingerprint: None,
                },
            ),
        )
        .await
        .unwrap();

        let first_state: String =
            sqlx::query_scalar("SELECT delivery_state FROM hub_session_messages WHERE id = $1")
                .bind(first_message_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(first_state, "delivered");
        let fallback: (String, String, Option<String>, Uuid, Uuid) = sqlx::query_as(
            "SELECT delivery_mode, delivery_state, expected_native_turn_id, turn_id, run_id
             FROM hub_session_messages WHERE id = $1",
        )
        .bind(second_message_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(fallback.0, "next_turn");
        assert_eq!(fallback.1, "queued");
        assert_eq!(fallback.2, None);
        assert_ne!(fallback.3, fixture.turn_id);
        assert_ne!(fallback.4, fixture.run_id);
        let pending: (String, String) = sqlx::query_as(
            "SELECT runs.status, turns.status
             FROM runs JOIN hub_session_turns AS turns ON turns.id = runs.hub_turn_id
             WHERE runs.id = $1",
        )
        .bind(fallback.4)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(pending, ("pending".into(), "pending".into()));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_preserves_draining_and_fences_owned_state_ack(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query("UPDATE hub_sessions SET native_session_id = 'thread-current' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _other_runtime_session = insert_idle_owned_session(
            &fixture.state.pool,
            fixture.hub_session_id,
            fixture.other_runtime_id,
        )
        .await;
        sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(heartbeat.runtime_status, "draining");
        assert_eq!(
            heartbeat.owned_sessions,
            vec![RuntimeOwnedSessionSnapshotDto {
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_session_id: Some("thread-current".into()),
                active_run_id: Some(fixture.run_id),
            }]
        );
        assert_eq!(heartbeat.session_commands.len(), 1);
        assert_eq!(
            heartbeat.session_commands[0],
            RuntimeSessionCommandDto {
                command_id: fixture.hub_session_id,
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                command: "checkpoint".into(),
                run_id: None,
                turn_id: None,
                native_session_id: None,
                native_turn_id: None,
                message: None,
                configuration_revision: None,
                fingerprint: None,
                execution_configuration: None,
            }
        );

        let stale = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: false,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 2,
                    lifecycle_status: "saving".into(),
                    checkpoint_reason: Some("drain".into()),
                }],
                cleaned_sessions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stale.runtime_status, "draining");
        assert_eq!(
            stale.owned_sessions,
            vec![RuntimeOwnedSessionSnapshotDto {
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                lifecycle_status: "online".into(),
                native_session_id: Some("thread-current".into()),
                active_run_id: Some(fixture.run_id),
            }]
        );
        assert!(stale.session_commands.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "draining"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT lifecycle_status FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "online"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_exposes_active_run_for_restoring_recovery(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(heartbeat.owned_sessions.len(), 1);
        assert_eq!(
            heartbeat.owned_sessions[0].session_id,
            fixture.hub_session_id
        );
        assert_eq!(heartbeat.owned_sessions[0].lifecycle_status, "restoring");
        assert_eq!(
            heartbeat.owned_sessions[0].active_run_id,
            Some(claim.run.id)
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_dispatches_and_exactly_acknowledges_cleanup_obligations(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let cleanup = RuntimeOwnedSessionGenerationDto {
            session_id: Uuid::new_v4(),
            ownership_generation: 7,
        };
        sqlx::query(
            "INSERT INTO runtime_session_cleanup_obligations
                 (runtime_id, session_id, ownership_generation)
             VALUES ($1, $2, $3)",
        )
        .bind(fixture.runtime_id)
        .bind(cleanup.session_id)
        .bind(cleanup.ownership_generation)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let first = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first.cleanup_sessions, vec![cleanup.clone()]);

        let other_runtime_id = Uuid::new_v4();
        let other_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(other_runtime_id)
        .bind(sha256_hex(&other_runtime_token))
        .bind(format!("cleanup-other-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let other = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&other_runtime_token),
            Json(RuntimeHeartbeatRequest {
                cleaned_sessions: vec![cleanup.clone()],
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(other.cleanup_sessions.is_empty());

        let wrong_generation = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                cleaned_sessions: vec![RuntimeOwnedSessionGenerationDto {
                    session_id: cleanup.session_id,
                    ownership_generation: cleanup.ownership_generation + 1,
                }],
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(wrong_generation.cleanup_sessions, vec![cleanup.clone()]);

        let acknowledged = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                cleaned_sessions: vec![cleanup.clone()],
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(acknowledged.cleanup_sessions.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_session_cleanup_obligations
                 WHERE runtime_id = $1 AND session_id = $2",
            )
            .bind(fixture.runtime_id)
            .bind(cleanup.session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_bundle_upload_streams_and_atomically_commits_pointer(pool: PgPool) {
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
                    native_session_id: Some("bundle-upload-thread".into()),
                    work_dir_ref: Some("bundle-upload-workdir".into()),
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

        let stored = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let stored = Arc::clone(&stored);
                move |body: Body| {
                    let stored = Arc::clone(&stored);
                    async move {
                        *stored.lock().unwrap() =
                            axum::body::to_bytes(body, 1024).await.unwrap().to_vec();
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);
        let bytes = Bytes::from_static(b"test bundle");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let created_at = Utc::now();
        let headers = runtime_bundle_upload_headers(
            &fixture.runtime_token,
            1,
            &attempt,
            &checksum,
            bytes.len(),
            created_at,
        );

        let mut limited_state = (*state).clone();
        limited_state.session_bundle_max_bytes = (bytes.len() - 1) as u64;
        let too_large = runtime_upload_session_bundle(
            State(Arc::new(limited_state)),
            Path(fixture.hub_session_id),
            headers.clone(),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(too_large.status, StatusCode::BAD_REQUEST);
        assert!(stored.lock().unwrap().is_empty());

        let response = runtime_upload_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            headers.clone(),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(
            response.checkpoint_attempt_id,
            attempt.checkpoint_attempt_id
        );
        assert_eq!(response.bundle_generation, attempt.bundle_generation);
        assert!(!response.has_queued_work);
        assert!(response.ownership_released);
        assert_eq!(*stored.lock().unwrap(), bytes);
        let replayed = runtime_upload_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            headers,
            Body::from(bytes.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            replayed.checkpoint_attempt_id,
            response.checkpoint_attempt_id
        );
        assert_eq!(replayed.bundle_generation, response.bundle_generation);
        assert_eq!(replayed.ownership_released, response.ownership_released);
        let other_runtime_id = Uuid::new_v4();
        let other_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
             (id, token_hash, hostname, labels, engine_version, capabilities, sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb, 'workspace-write',
                     'online')",
        )
        .bind(other_runtime_id)
        .bind(sha256_hex(&other_runtime_token))
        .bind(format!("other-runtime-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let foreign_replay = runtime_upload_session_bundle(
            State(state),
            Path(fixture.hub_session_id),
            runtime_bundle_upload_headers(
                &other_runtime_token,
                1,
                &attempt,
                &checksum,
                bytes.len(),
                created_at,
            ),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign_replay.status, StatusCode::CONFLICT);
        let pointer: (Option<i64>, Option<String>, Option<Uuid>, String) = sqlx::query_as(
            "SELECT current_bundle_generation, current_bundle_checksum_sha256,
                    runtime_owner_id, lifecycle_status
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(pointer.0, Some(attempt.bundle_generation));
        assert_eq!(pointer.1.as_deref(), Some(checksum.as_str()));
        assert_eq!(pointer.2, None);
        assert_eq!(pointer.3, "offline");
        server.abort();
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_bundle_download_requires_the_owned_restoring_generation(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let bytes = Bytes::from_static(b"download bundle");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let checkpoint_attempt_id = Uuid::new_v4();
        let object_key =
            session_bundle_object_key(fixture.hub_session_id, 1, checkpoint_attempt_id);
        let created_at = Utc::now();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'restoring', current_bundle_generation = 1,
                 current_bundle_kind = 'checkpoint',
                 current_bundle_object_key = $2, current_bundle_checksum_sha256 = $3,
                 current_bundle_size_bytes = $4, current_bundle_history_checkpoint = 3,
                 current_bundle_ownership_generation = 1,
                 current_bundle_producing_engine_version = '0.104.0',
                 current_bundle_created_at = $5,
                 current_bundle_checkpoint_attempt_id = $6,
                 current_bundle_runtime_id = $7
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .bind(&object_key)
        .bind(&checksum)
        .bind(bytes.len() as i64)
        .bind(created_at)
        .bind(checkpoint_attempt_id)
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let get_count = Arc::new(AtomicU64::new(0));
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            get({
                let get_count = Arc::clone(&get_count);
                let bytes = bytes.clone();
                move || {
                    let get_count = Arc::clone(&get_count);
                    let bytes = bytes.clone();
                    async move {
                        get_count.fetch_add(1, Ordering::SeqCst);
                        bytes
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);
        let download_headers = |token: &str, generation: i64| {
            let mut headers = bearer_headers(token);
            headers.insert(
                "x-agent-hub-ownership-generation",
                HeaderValue::from_str(&generation.to_string()).unwrap(),
            );
            headers
        };

        let stale = runtime_download_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            download_headers(&fixture.runtime_token, 2),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::FORBIDDEN);

        let other_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&other_runtime_token))
        .bind(format!(
            "bundle-download-foreign-{}",
            Uuid::new_v4().simple()
        ))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let foreign = runtime_download_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            download_headers(&other_runtime_token, 1),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign.status, StatusCode::FORBIDDEN);
        assert_eq!(get_count.load(Ordering::SeqCst), 0);

        let response = runtime_download_session_bundle(
            State(state),
            Path(fixture.hub_session_id),
            download_headers(&fixture.runtime_token, 1),
        )
        .await
        .unwrap();
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            bytes.len().to_string()
        );
        assert_eq!(response.headers()["x-agent-hub-bundle-generation"], "1");
        assert_eq!(response.headers()["x-agent-hub-bundle-sha256"], checksum);
        assert!(response
            .headers()
            .get("x-agent-hub-bundle-object-key")
            .is_none());
        assert_eq!(
            axum::body::to_bytes(response.into_body(), 1024)
                .await
                .unwrap(),
            bytes
        );
        assert_eq!(get_count.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_skill_package_download_separates_run_snapshot_from_session_current_package(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let skill_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Download Skill', 'download', 'read package', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("read package"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let object_root = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let store = Arc::new(SkillPackageStore::local(object_root.path().to_path_buf()).unwrap());
        let file_manifest = vec![SkillPackageFileDto {
            path: "bin/client".into(),
            size_bytes: 6,
            checksum_sha256: sha256_hex("client"),
            executable: true,
        }];
        let first_bytes = b"run-package";
        let first_checksum = format!("{:x}", Sha256::digest(first_bytes));
        let first_package_id = Uuid::new_v4();
        let first_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{first_package_id}.tar.zst");
        let first_source = source_root.path().join("first.tar.zst");
        tokio::fs::write(&first_source, first_bytes).await.unwrap();
        store
            .put_file(
                &first_object_key,
                &first_source,
                first_bytes.len() as u64,
                &first_checksum,
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO skill_packages
                 (id, skill_id, owner_id, object_key, format_version,
                  size_bytes, checksum_sha256, files)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7)",
        )
        .bind(first_package_id)
        .bind(skill_id)
        .bind(owner_id)
        .bind(&first_object_key)
        .bind(first_bytes.len() as i64)
        .bind(&first_checksum)
        .bind(serde_json::to_value(&file_manifest).unwrap())
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE skills SET current_package_id = $1 WHERE id = $2")
            .bind(first_package_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(
            claim.execution_configuration.skills[0]
                .package
                .as_ref()
                .map(|package| package.id),
            Some(first_package_id)
        );
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT package_id FROM run_skill_packages
                 WHERE run_id = $1 AND skill_id = $2",
            )
            .bind(claim.run.id)
            .bind(skill_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            first_package_id
        );

        let second_bytes = b"session-package";
        let second_checksum = format!("{:x}", Sha256::digest(second_bytes));
        let second_package_id = Uuid::new_v4();
        let second_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{second_package_id}.tar.zst");
        let second_source = source_root.path().join("second.tar.zst");
        tokio::fs::write(&second_source, second_bytes)
            .await
            .unwrap();
        store
            .put_file(
                &second_object_key,
                &second_source,
                second_bytes.len() as u64,
                &second_checksum,
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO skill_packages
                 (id, skill_id, owner_id, object_key, format_version,
                  size_bytes, checksum_sha256, files)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7)",
        )
        .bind(second_package_id)
        .bind(skill_id)
        .bind(owner_id)
        .bind(&second_object_key)
        .bind(second_bytes.len() as i64)
        .bind(&second_checksum)
        .bind(serde_json::to_value(&file_manifest).unwrap())
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE skills SET current_package_id = $1 WHERE id = $2")
            .bind(second_package_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM skill_packages WHERE id = $1")
            .bind(first_package_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let mut state = (*fixture.state).clone();
        state.skill_package_store = Some(store);
        let state = Arc::new(state);
        let package_headers = |token: &str, generation: i64| {
            let mut headers = bearer_headers(token);
            headers.insert(
                "x-agent-hub-ownership-generation",
                HeaderValue::from_str(&generation.to_string()).unwrap(),
            );
            headers
        };
        let stale_run = runtime_download_run_skill_package(
            State(state.clone()),
            package_headers(&fixture.runtime_token, 2),
            Path((claim.run.id, skill_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(stale_run.status, StatusCode::FORBIDDEN);

        let other_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&other_runtime_token))
        .bind(format!("skill-download-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let foreign_run = runtime_download_run_skill_package(
            State(state.clone()),
            package_headers(&other_runtime_token, 1),
            Path((claim.run.id, skill_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign_run.status, StatusCode::FORBIDDEN);

        let run_response = runtime_download_run_skill_package(
            State(state.clone()),
            package_headers(&fixture.runtime_token, 1),
            Path((claim.run.id, skill_id)),
        )
        .await
        .unwrap();
        assert_eq!(
            run_response.headers()["x-agent-hub-skill-package-id"],
            first_package_id.to_string()
        );
        assert_eq!(
            axum::body::to_bytes(run_response.into_body(), 1024)
                .await
                .unwrap(),
            first_bytes.as_slice()
        );

        let stale_session = runtime_download_session_skill_package(
            State(state.clone()),
            package_headers(&fixture.runtime_token, 2),
            Path((fixture.hub_session_id, skill_id, second_package_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(stale_session.status, StatusCode::FORBIDDEN);
        let old_session = runtime_download_session_skill_package(
            State(state.clone()),
            package_headers(&fixture.runtime_token, 1),
            Path((fixture.hub_session_id, skill_id, first_package_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(old_session.status, StatusCode::NOT_FOUND);

        let session_response = runtime_download_session_skill_package(
            State(state),
            package_headers(&fixture.runtime_token, 1),
            Path((fixture.hub_session_id, skill_id, second_package_id)),
        )
        .await
        .unwrap();
        assert_eq!(
            session_response.headers()["x-agent-hub-skill-package-id"],
            second_package_id.to_string()
        );
        assert_eq!(
            axum::body::to_bytes(session_response.into_body(), 1024)
                .await
                .unwrap(),
            second_bytes.as_slice()
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_bundle_object_failure_keeps_pointer_uncommitted_and_session_saving(
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
                    native_session_id: Some("bundle-failure-thread".into()),
                    work_dir_ref: Some("bundle-failure-workdir".into()),
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
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put(|| async { StatusCode::INTERNAL_SERVER_ERROR })
                .delete(|| async { StatusCode::NO_CONTENT }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{address}").parse().unwrap(),
                bucket: "bundle-bucket".into(),
                region: "us-test-1".into(),
                access_key_id: "test-access".into(),
                secret_access_key: "test-secret".into(),
                session_token: None,
                server_side_encryption: None,
                kms_key_id: None,
                allow_http: true,
            },
        )
        .unwrap();
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        state.session_bundle_max_bytes = 1024;
        let state = Arc::new(state);
        let bytes = Bytes::from_static(b"failed bundle");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let headers = runtime_bundle_upload_headers(
            &fixture.runtime_token,
            1,
            &attempt,
            &checksum,
            bytes.len(),
            Utc::now(),
        );

        let error = runtime_upload_session_bundle(
            State(state),
            Path(fixture.hub_session_id),
            headers,
            Body::from(bytes),
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        let row: (Option<i64>, Option<String>, Option<Uuid>, String) = sqlx::query_as(
            "SELECT current_bundle_generation, current_bundle_object_key,
                    runtime_owner_id, lifecycle_status
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(row.0, None);
        assert_eq!(row.1, None);
        assert_eq!(row.2, Some(fixture.runtime_id));
        assert_eq!(row.3, "saving");
        server.abort();
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_rejects_a_bundle_with_unreplayable_history(pool: PgPool) {
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
                    native_session_id: Some("stale-restore-thread".into()),
                    work_dir_ref: Some("stale-restore-workdir".into()),
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
        let mut commit_tx = fixture.state.pool.begin().await.unwrap();
        commit_session_bundle_metadata_tx(
            &mut commit_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/stale-restore.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "stale-restore".into(),
                size_bytes: 1024,
                history_checkpoint: attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        commit_tx.commit().await.unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = NULL, lifecycle_status = 'waiting_for_runtime',
                 saving_history_checkpoint = NULL,
                 saving_ownership_generation = NULL,
                 saving_reason = NULL,
                 saving_checkpoint_attempt_id = NULL
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let delivered_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'not present in Bundle',
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
        let pending_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        sqlx::query("UPDATE agents SET runtime_id = NULL WHERE id = $1")
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let replacement_runtime_id = Uuid::new_v4();
        let replacement_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, 'stale-restore-replacement', '{}', 'test',
                     '{\"model_proxy\":true}'::jsonb, 'workspace-write', 'online')",
        )
        .bind(replacement_runtime_id)
        .bind(sha256_hex(&replacement_token))
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let response = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&replacement_token),
            runtime_claim_request(1, Vec::new()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            sqlx::query_as::<_, (Option<Uuid>, String)>(
                "SELECT runtime_owner_id, lifecycle_status FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (None, "waiting_for_runtime".into())
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(pending_run_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "pending"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_cannot_idle_checkpoint_an_active_turn(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
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

        let error = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: true,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    lifecycle_status: "saving".into(),
                    checkpoint_reason: Some("idle".into()),
                }],
                cleaned_sessions: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);

        let session: (
            String,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<Uuid>,
        ) = sqlx::query_as(
            "SELECT lifecycle_status, saving_history_checkpoint,
                        saving_ownership_generation, saving_reason,
                        saving_checkpoint_attempt_id
                 FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        // 会话仍处于 restoring（online 心跳不得覆盖恢复中状态；恢复完成后
        // 由 turn_started 置 online）。
        assert_eq!(session, ("restoring".into(), None, None, None, None));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_atomically_freezes_a_new_saving_checkpoint(pool: PgPool) {
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
                    native_session_id: Some("heartbeat-saving-thread".into()),
                    work_dir_ref: Some("heartbeat-saving-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let heartbeat = || RuntimeHeartbeatRequest {
            pending_credential_hash: None,
            accepts_session_commands: true,
            owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                session_id: fixture.hub_session_id,
                ownership_generation: 1,
                lifecycle_status: "saving".into(),
                checkpoint_reason: Some("idle".into()),
            }],
            cleaned_sessions: Vec::new(),
        };

        let _ = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat()),
        )
        .await
        .unwrap();
        let first: (i64, i64, String, Uuid) = sqlx::query_as(
            "SELECT saving_history_checkpoint, saving_ownership_generation,
                    saving_reason, saving_checkpoint_attempt_id
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!((first.0, first.1, first.2.as_str()), (3, 1, "idle"));

        let _ = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat()),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            first.3
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_heartbeat_cannot_undo_admin_drain_checkpoint(pool: PgPool) {
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
                    native_session_id: Some("drain-heartbeat-thread".into()),
                    work_dir_ref: Some("drain-heartbeat-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let admin_token = create_super_admin_session(&fixture.state.pool).await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let _ = drain_runtime(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap();
        let attempt_before: Uuid = sqlx::query_scalar(
            "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();

        let response = runtime_heartbeat(
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
        .unwrap()
        .0;

        assert_eq!(response.owned_sessions[0].lifecycle_status, "saving");
        assert!(response.session_commands.iter().any(|command| {
            command.command == "checkpoint" && command.session_id == fixture.hub_session_id
        }));
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            attempt_before
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_drain_handles_active_and_idle_sessions_and_can_be_cancelled(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let idle_session_id = insert_idle_owned_session(
            &fixture.state.pool,
            fixture.hub_session_id,
            fixture.runtime_id,
        )
        .await;
        let admin_token = create_super_admin_session(&fixture.state.pool).await;
        let member_token = create_user_session_with_role(&fixture.state.pool, "member").await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();

        let forbidden = drain_runtime(
            State(admin_state.clone()),
            session_headers(&member_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);

        let mismatch = drain_runtime(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: format!("{hostname} "),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.status, StatusCode::CONFLICT);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "online"
        );

        let drained = drain_runtime(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(drained.runtime.status, "draining");
        assert_eq!(drained.owned_sessions.len(), 2);
        let states = drained
            .owned_sessions
            .iter()
            .map(|session| (session.id, session.lifecycle_status.as_str()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(states[&fixture.hub_session_id], "online");
        assert_eq!(states[&idle_session_id], "saving");

        let claim_response = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, Vec::new()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claim_response.status(), StatusCode::NO_CONTENT);
        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(heartbeat.runtime_status, "draining");
        assert_eq!(heartbeat.session_commands.len(), 2);
        assert!(heartbeat.session_commands.iter().all(|command| {
            command.command == "checkpoint" && command.ownership_generation == 1
        }));

        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(CompleteRunRequest {
                status: "completed".into(),
                native_session_id: Some("drained-thread".into()),
                work_dir_ref: Some("drained-workdir".into()),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT lifecycle_status FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "saving"
        );

        let cancelled = cancel_runtime_drain(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(cancelled.runtime.status, "online");
        assert!(cancelled
            .owned_sessions
            .iter()
            .all(|session| session.lifecycle_status == "saving"));
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_completion_preserves_an_existing_drain_checkpoint_attempt(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let checkpoint_attempt_id = Uuid::new_v4();
        sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'saving',
                 saving_history_checkpoint = history_checkpoint,
                 saving_ownership_generation = ownership_generation,
                 saving_reason = 'drain', saving_checkpoint_attempt_id = $1
             WHERE id = $2",
        )
        .bind(checkpoint_attempt_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("existing-drain-thread".into()),
                    work_dir_ref: Some("existing-drain-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, String, Uuid)>(
                "SELECT saving_history_checkpoint, saving_ownership_generation,
                        saving_reason, saving_checkpoint_attempt_id
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (3, 1, "drain".into(), checkpoint_attempt_id)
        );

        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("existing-drain-thread".into()),
                    work_dir_ref: Some("existing-drain-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            checkpoint_attempt_id
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_list_is_read_only_and_redacts_unknown_capabilities(pool: PgPool) {
        let user_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        let session_token = "runtime-list-session";
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, 'runtime-list@example.com', 'unused',
                     'Runtime List', 'member')",
        )
        .bind(user_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(session_token))
        .bind(user_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runtimes
             (id, token_hash, hostname, labels, engine_version, capabilities, sandbox_mode,
              status, last_heartbeat_at)
             VALUES ($1, 'unused', 'stale-console-runtime', '{}', 'test', $2,
                     'read-only', 'online', now() - interval '2 minutes')",
        )
        .bind(runtime_id)
        .bind(json!({
            "driver": "pi",
            "model_proxy": true,
            "sandbox_downgraded": true,
            "sandbox_downgrade_reason": "workspace is read-only",
            "unknown_secret": "do-not-return",
            "sandbox": { "mount_token": "secret" }
        }))
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let listed = list_runtimes(State(state), session_headers(session_token))
            .await
            .unwrap()
            .0;

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, "online");
        assert_eq!(listed[0].capabilities["driver"], "pi");
        assert_eq!(listed[0].capabilities["model_proxy"], true);
        assert!(listed[0].capabilities.get("unknown_secret").is_none());
        assert!(listed[0].capabilities.get("sandbox").is_none());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(runtime_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "online"
        );

        reap_stale_runtimes(&pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(runtime_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "offline"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_reaper_preserves_session_owner_and_draining_state(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE runtimes
             SET last_heartbeat_at = now() - interval '2 minutes'
             WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        reap_stale_runtimes(&fixture.state.pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "offline"
        );
        assert_eq!(
            sqlx::query_as::<_, (Option<Uuid>, i64)>(
                "SELECT runtime_owner_id, ownership_generation
                 FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (None, 2)
        );
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id)
                .await
                .0,
            "failed"
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT ownership_generation, history_checkpoint, bundle_generation
                 FROM runtime_session_salvage_obligations
                 WHERE runtime_id = $1 AND session_id = $2",
            )
            .bind(fixture.runtime_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (1, 0, 1)
        );

        let recovered = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(recovered.runtime_status, "online");
        assert!(recovered.session_commands.is_empty());
        assert!(recovered.owned_sessions.is_empty());
        assert_eq!(recovered.salvage_sessions.len(), 1);
        assert_eq!(
            recovered.salvage_sessions[0].session_id,
            fixture.hub_session_id
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT ownership_generation FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );

        sqlx::query(
            "UPDATE runtimes
             SET status = 'draining', last_heartbeat_at = now() - interval '1 minute'
             WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        reap_stale_runtimes(&fixture.state.pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "draining"
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    async fn run_model_bindings_distinguish_same_connection_model_with_different_settings(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "INSERT INTO subagent_definitions
                 (id, agent_id, name, description, developer_instructions,
                  model_settings_override)
             VALUES ($1, $2, 'reviewer', 'Reviews output', 'Review carefully.',
                     '{\"reasoning_effort\":\"high\",\"provider_request_timeout_ms\":120000}'::jsonb)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let main = claim
            .execution_configuration
            .model_bindings
            .iter()
            .find(|binding| binding.binding_key == "main")
            .unwrap();
        let reviewer = claim
            .execution_configuration
            .model_bindings
            .iter()
            .find(|binding| binding.binding_key == "reviewer")
            .unwrap();
        assert_ne!(main.id, reviewer.id);
        assert_eq!(main.model_connection_id, reviewer.model_connection_id);
        assert_eq!(main.model_id, reviewer.model_id);
        assert_eq!(
            main.model_settings.reasoning_effort,
            ReasoningEffort::Default
        );
        assert_eq!(
            reviewer.model_settings.reasoning_effort,
            ReasoningEffort::High
        );
        assert_eq!(main.model_settings.provider_request_timeout_ms, None);
        assert_eq!(
            reviewer.model_settings.provider_request_timeout_ms,
            Some(120_000)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_model_bindings WHERE run_id = $1",
            )
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_ownership_change_wins_without_orphan_tool_request_event(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let mut ownership_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM runs WHERE id = $1 FOR NO KEY UPDATE")
            .bind(fixture.run_id)
            .fetch_one(&mut *ownership_tx)
            .await
            .unwrap();

        let application_name = format!("ownership-tool-append-{}", Uuid::new_v4().simple());
        let append_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_name).await,
        ));
        let append_headers = bearer_headers(&fixture.runtime_token);
        let run_id = fixture.run_id;
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let append = tokio::spawn(async move {
            runtime_finalize_tool_requests(
                State(append_state),
                append_headers,
                Path(run_id),
                runtime_write(batch),
            )
            .await
        });
        let append_wait_observed = wait_for_application_lock(
            &fixture.state.pool,
            &application_name,
            "SELECT r.integration_session_id, r.hub_session_id",
        )
        .await;
        let visible_before_ownership_commit =
            run_event_count(&fixture.state.pool, fixture.run_id).await;

        sqlx::query("UPDATE runs SET runtime_id = $1 WHERE id = $2")
            .bind(fixture.other_runtime_id)
            .bind(fixture.run_id)
            .execute(&mut *ownership_tx)
            .await
            .unwrap();
        ownership_tx.commit().await.unwrap();

        let append_result = tokio::time::timeout(Duration::from_secs(3), append)
            .await
            .expect("runtime append should unblock after ownership changes")
            .expect("runtime append task should not panic");
        assert!(
            append_wait_observed,
            "runtime append must wait on the owned run lock"
        );
        assert_eq!(visible_before_ownership_commit, 0);
        assert!(append_result.is_err());
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        assert_eq!(
            tool_request_count(&fixture.state.pool, fixture.tool_request_id).await,
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_rolls_back_target_when_claim_event_insert_fails(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let (trigger_name, function_name) =
            install_run_event_failure_trigger(&fixture.state.pool, fixture.run_id).await;

        let claim_result = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, Vec::new()),
        )
        .await;
        remove_run_event_failure_trigger(&fixture.state.pool, &trigger_name, &function_name).await;

        assert!(claim_result.is_err());
        assert_eq!(
            runtime_claim_run_state(&fixture.state.pool, fixture.run_id).await,
            ("pending".into(), None, None)
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_claim_rolls_back_capability_mismatch_when_status_event_insert_fails(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "read-only", "workspace-write").await;
        let (trigger_name, function_name) =
            install_run_event_failure_trigger(&fixture.state.pool, fixture.run_id).await;

        let claim_result = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, Vec::new()),
        )
        .await;
        remove_run_event_failure_trigger(&fixture.state.pool, &trigger_name, &function_name).await;

        assert!(claim_result.is_err());
        assert_eq!(
            runtime_claim_run_state(&fixture.state.pool, fixture.run_id).await,
            ("pending".into(), None, None)
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
    }
    #[tokio::test]
    async fn runtime_complete_rejects_invalid_status_before_database_access() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let result = runtime_complete_run(
            State(Arc::new(test_state_with_pool(pool))),
            HeaderMap::new(),
            Path(Uuid::new_v4()),
            runtime_write(CompleteRunRequest {
                status: "cancelled".into(),
                native_session_id: None,
                work_dir_ref: None,
            }),
        )
        .await;

        let error = result.expect_err("invalid completion status must be rejected");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.message, "invalid run completion status");
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_complete_cannot_bypass_atomic_waiting_tool_finalize(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;

        let completion = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(CompleteRunRequest {
                status: "waiting_tool".into(),
                native_session_id: Some("bypass-session".into()),
                work_dir_ref: Some("bypass-workdir".into()),
            }),
        )
        .await;

        let error = completion.expect_err("running runs must use atomic tool-request finalize");
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            ("running".into(), Some(fixture.runtime_id), None, None, None)
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        assert_eq!(
            tool_request_count(&fixture.state.pool, fixture.tool_request_id).await,
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_complete_rolls_back_when_status_event_insert_fails(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE runs
             SET status = 'running', runtime_id = $1, model_proxy_token_hash = 'original-token',
                 native_session_id = 'original-session', work_dir_ref = 'original-workdir'
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let (trigger_name, function_name) =
            install_run_event_failure_trigger(&fixture.state.pool, fixture.run_id).await;

        let completion_result = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(CompleteRunRequest {
                status: "completed".into(),
                native_session_id: Some("new-session".into()),
                work_dir_ref: Some("new-workdir".into()),
            }),
        )
        .await;
        remove_run_event_failure_trigger(&fixture.state.pool, &trigger_name, &function_name).await;

        assert!(completion_result.is_err());
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            (
                "running".into(),
                Some(fixture.runtime_id),
                Some("original-token".into()),
                Some("original-session".into()),
                Some("original-workdir".into())
            )
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_enrollment_is_atomic_single_use_and_hostname_is_not_identity(pool: PgPool) {
        let state = Arc::new(test_state_with_pool(pool));
        let enrollment = format!(
            "ahre_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        sqlx::query(
            "INSERT INTO runtime_enrollment_tokens
                 (id, token_hash, expires_at)
             VALUES ($1, $2, now() + interval '30 minutes')",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&enrollment))
        .execute(&state.pool)
        .await
        .unwrap();
        let request = RuntimeRegisterRequest {
            hostname: "shared-hostname".into(),
            labels: vec!["test".into()],
            engine_version: "test".into(),
            capabilities: json!({}),
            sandbox_mode: "workspace-write".into(),
        };

        let first_state = Arc::clone(&state);
        let first_token = enrollment.clone();
        let first_request = request.clone();
        let first = tokio::spawn(async move {
            runtime_register(
                State(first_state),
                bearer_headers(&first_token),
                Json(first_request),
            )
            .await
        });
        let second_state = Arc::clone(&state);
        let second_token = enrollment.clone();
        let second_request = request.clone();
        let second = tokio::spawn(async move {
            runtime_register(
                State(second_state),
                bearer_headers(&second_token),
                Json(second_request),
            )
            .await
        });
        let results = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let enrolled = results
            .into_iter()
            .find_map(Result::ok)
            .expect("one enrollment must win")
            .0;
        assert_ne!(enrolled.runtime_credential, enrollment);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtimes")
                .fetch_one(&state.pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtimes WHERE token_hash = $1",)
                .bind(sha256_hex(&enrolled.runtime_credential))
                .fetch_one(&state.pool)
                .await
                .unwrap(),
            1
        );
        let stored_plaintext: i64 = sqlx::query_scalar(
            "SELECT
                (SELECT count(*) FROM runtimes WHERE token_hash = $1) +
                (SELECT count(*) FROM runtime_enrollment_tokens WHERE token_hash = $1)",
        )
        .bind(&enrollment)
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(stored_plaintext, 0);

        let second_enrollment = format!(
            "ahre_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        sqlx::query(
            "INSERT INTO runtime_enrollment_tokens
                 (id, token_hash, expires_at)
             VALUES ($1, $2, now() + interval '30 minutes')",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&second_enrollment))
        .execute(&state.pool)
        .await
        .unwrap();
        let second_runtime = runtime_register(
            State(Arc::clone(&state)),
            bearer_headers(&second_enrollment),
            Json(request),
        )
        .await
        .unwrap()
        .0;
        assert_ne!(enrolled.runtime_id, second_runtime.runtime_id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtimes WHERE hostname = 'shared-hostname'",
            )
            .fetch_one(&state.pool)
            .await
            .unwrap(),
            2
        );
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_enrollment_admin_api_expires_revokes_and_redacts_tokens(pool: PgPool) {
        let admin_id = Uuid::new_v4();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Runtime Admin', 'super_admin')",
        )
        .bind(admin_id)
        .bind(format!(
            "runtime-admin-{}@example.com",
            Uuid::new_v4().simple()
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(admin_id)
        .execute(&pool)
        .await
        .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let before = Utc::now();

        let created = create_runtime_enrollment_token(
            State(Arc::clone(&state)),
            session_headers(&session_token),
        )
        .await
        .unwrap()
        .0;

        let lifetime = created.enrollment.expires_at - before;
        assert!(lifetime >= ChronoDuration::minutes(29));
        assert!(lifetime <= ChronoDuration::minutes(31));
        assert_eq!(created.enrollment.created_by, Some(admin_id));
        let stored_hash: String =
            sqlx::query_scalar("SELECT token_hash FROM runtime_enrollment_tokens WHERE id = $1")
                .bind(created.enrollment.id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(stored_hash, sha256_hex(&created.token));
        assert_ne!(stored_hash, created.token);

        let listed = list_runtime_enrollment_tokens(
            State(Arc::clone(&state)),
            session_headers(&session_token),
        )
        .await
        .unwrap()
        .0;
        let listed_json = serde_json::to_string(&listed).unwrap();
        assert!(!listed_json.contains(&created.token));
        assert!(!listed_json.contains("token_hash"));
        assert_eq!(listed.len(), 1);

        let revoked = revoke_runtime_enrollment_token(
            State(Arc::clone(&state)),
            session_headers(&session_token),
            Path(created.enrollment.id),
        )
        .await
        .unwrap()
        .0;
        assert!(revoked.revoked_at.is_some());
        let request = RuntimeRegisterRequest {
            hostname: "revoked-enrollment".into(),
            labels: Vec::new(),
            engine_version: "test".into(),
            capabilities: json!({}),
            sandbox_mode: "workspace-write".into(),
        };
        let revoked_error = runtime_register(
            State(Arc::clone(&state)),
            bearer_headers(&created.token),
            Json(request.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(revoked_error.status, StatusCode::UNAUTHORIZED);

        let expired = format!(
            "ahre_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        sqlx::query(
            "INSERT INTO runtime_enrollment_tokens
                 (id, token_hash, expires_at, created_at)
             VALUES ($1, $2, now() - interval '1 second', now() - interval '31 minutes')",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&expired))
        .execute(&state.pool)
        .await
        .unwrap();
        let expired_error = runtime_register(
            State(Arc::clone(&state)),
            bearer_headers(&expired),
            Json(request),
        )
        .await
        .unwrap_err();
        assert_eq!(expired_error.status, StatusCode::UNAUTHORIZED);
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_rotation_request_remains_pending_while_runtime_is_offline(pool: PgPool) {
        let admin_id = Uuid::new_v4();
        let runtime_id = Uuid::new_v4();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Runtime Admin', 'super_admin')",
        )
        .bind(admin_id)
        .bind(format!(
            "rotation-admin-{}@example.com",
            Uuid::new_v4().simple()
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(admin_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, 'offline-runtime', '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'offline')",
        )
        .bind(runtime_id)
        .bind(sha256_hex("offline-runtime-credential"))
        .execute(&pool)
        .await
        .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool));

        let runtime = request_runtime_credential_rotation(
            State(Arc::clone(&state)),
            session_headers(&session_token),
            Path(runtime_id),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(runtime.status, "offline");
        assert!(runtime.credential_rotation_requested_at.is_some());
        let pending: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT rotation_requested_at FROM runtimes WHERE id = $1")
                .bind(runtime_id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert!(pending.is_some());
    }
    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn runtime_rotation_keeps_old_credential_until_pending_authenticates(pool: PgPool) {
        let state = Arc::new(test_state_with_pool(pool));
        let runtime_id = Uuid::new_v4();
        let old_credential = format!(
            "ahrc_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let new_credential = format!(
            "ahrc_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status, rotation_requested_at)
             VALUES ($1, $2, 'rotation-test', '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'offline', now())",
        )
        .bind(runtime_id)
        .bind(sha256_hex(&old_credential))
        .execute(&state.pool)
        .await
        .unwrap();

        let requested = runtime_heartbeat(
            State(Arc::clone(&state)),
            bearer_headers(&old_credential),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(requested.rotation_requested);

        let staged = runtime_heartbeat(
            State(Arc::clone(&state)),
            bearer_headers(&old_credential),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: Some(sha256_hex(&new_credential)),
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(staged.pending_credential_accepted);
        assert!(require_runtime(&state, &bearer_headers(&old_credential))
            .await
            .is_ok());
        assert!(require_runtime(&state, &bearer_headers(&new_credential))
            .await
            .is_err());

        let activated = runtime_heartbeat(
            State(Arc::clone(&state)),
            bearer_headers(&new_credential),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(activated.credential_activated);
        assert!(require_runtime(&state, &bearer_headers(&old_credential))
            .await
            .is_err());
        assert!(require_runtime(&state, &bearer_headers(&new_credential))
            .await
            .is_ok());

        sqlx::query("DELETE FROM runtimes WHERE id = $1")
            .bind(runtime_id)
            .execute(&state.pool)
            .await
            .unwrap();
        assert!(require_runtime(&state, &bearer_headers(&new_credential))
            .await
            .is_err());
        let deleted_heartbeat = runtime_heartbeat(
            State(Arc::clone(&state)),
            bearer_headers(&new_credential),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(deleted_heartbeat.status, StatusCode::UNAUTHORIZED);
        let deleted_claim = match runtime_claim_run(
            State(Arc::clone(&state)),
            bearer_headers(&new_credential),
            runtime_claim_request(1, Vec::new()),
        )
        .await
        {
            Ok(_) => panic!("deleted Runtime credential unexpectedly claimed a Run"),
            Err(error) => error,
        };
        assert_eq!(deleted_claim.status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn runtime_capability_sql_requires_model_proxy_mcp_subagents_and_effective_sandbox() {
        assert!(RUNTIME_CAPABILITY_SQL.contains(
            "a.model_policy->>'provider' IS DISTINCT FROM 'hub-proxy'\n             OR COALESCE((rt.capabilities->>'model_proxy')::boolean, false) = true"
        ));
        assert!(RUNTIME_CAPABILITY_SQL.contains(
            "subagent.agent_id = a.id AND subagent.enabled = true\n             )\n             OR COALESCE((rt.capabilities->>'subagents')::boolean, false) = true"
        ));
        assert!(RUNTIME_CAPABILITY_SQL.contains(
            "a.sandbox_policy->>'mode' IS DISTINCT FROM 'workspace-write'\n             OR rt.sandbox_mode LIKE 'workspace-write%'"
        ));
        assert!(RUNTIME_CAPABILITY_SQL.contains(
            "a.sandbox_policy->>'mode' IS DISTINCT FROM 'danger-full-access'\n             OR rt.sandbox_mode LIKE 'danger-full-access%'"
        ));
        assert!(!RUNTIME_CAPABILITY_SQL.contains("direct_fallback"));
        assert!(!RUNTIME_CAPABILITY_SQL.contains("direct_model_enabled"));
        assert!(!RUNTIME_CAPABILITY_SQL.contains("rt.sandbox_mode LIKE '%danger%'"));
    }
}
