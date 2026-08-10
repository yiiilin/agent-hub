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
                ));
            }
        }
    }

    let reported_session_ids = reported_session_ids.into_iter().collect::<Vec<_>>();
    sqlx::query(
        "UPDATE hub_sessions
         SET runtime_owner_id = NULL,
             lifecycle_status = 'offline',
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
    sqlx::query(
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
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3",
    )
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
        return Err(ApiError::forbidden(
            "runtime does not own this Session generation",
        ));
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
        return Err(ApiError::forbidden(
            "runtime does not own this Session generation",
        ));
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
                current_bundle_producing_engine_version, current_bundle_created_at
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
            row.get::<String, _>("current_bundle_producing_engine_version"),
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
             current_bundle_checkpoint_attempt_id = $11
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
             END
         WHERE id = $2
           AND (runtime_owner_id IS NULL OR runtime_owner_id = $1)
         RETURNING ownership_generation",
    )
    .bind(runtime_id)
    .bind(hub_session_id)
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
        return Err(ApiError::forbidden(
            "runtime does not own this Session generation",
        ));
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
    .ok_or(ApiError::forbidden("runtime does not own an active run"))?;
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
         WHERE status = 'online' AND last_heartbeat_at < now() - interval '30 seconds'
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
