//! Admin 领域模块：管理员用户管理与用户数据擦除（GDPR 删除）。

use super::*;
use crate::{
    normalize_display_name, normalize_email, record_runtime_session_cleanup_tx, validate_user_role,
};
use agent_hub_shared::*;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;
pub(crate) async fn list_admin_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<AdminUserDetailDto>>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, email, display_name, role,
                password IS NOT NULL AS has_password, created_at
         FROM users
         WHERE deletion_requested_at IS NULL
           AND ($1 = 'super_admin' OR role <> 'super_admin')
         ORDER BY created_at, id",
    )
    .bind(&administrator.role)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(
        rows.into_iter().map(admin_user_detail_from_row).collect(),
    ))
}

pub(crate) async fn create_admin_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AdminCreateUserRequest>,
) -> Result<Json<AdminUserDetailDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let email = normalize_email(&req.email)?;
    let display_name = normalize_display_name(req.display_name.as_deref(), &email)?;
    let role = validate_user_role(&req.role)?;
    if administrator.role != "super_admin" && role != "member" {
        return Err(ApiError::forbidden(
            "only a Super Administrator can create administrator accounts",
        ));
    }
    let password = match req.password.as_deref() {
        Some(password) => {
            if !(8..=1024).contains(&password.len()) {
                return Err(ApiError::bad_request(
                    "password must be between 8 and 1024 bytes",
                ));
            }
            Some(
                password_hash(password)
                    .map_err(|_| ApiError::internal("password hashing failed"))?,
            )
        }
        None => None,
    };

    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let actor_role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM users
         WHERE id = $1 AND role IN ('admin', 'super_admin')
           AND deletion_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(administrator.id)
    .fetch_optional(&mut *tx)
    .await?;
    let actor_role =
        actor_role.ok_or(ApiError::forbidden("administrator permission is required"))?;
    if actor_role != "super_admin" && role != "member" {
        return Err(ApiError::forbidden(
            "only a Super Administrator can create administrator accounts",
        ));
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM users WHERE lower(btrim(email)) = lower(btrim($1))
         )",
    )
    .bind(&email)
    .fetch_one(&mut *tx)
    .await?;
    if exists {
        return Err(ApiError::conflict("email already exists"));
    }
    let row = sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, email, display_name, role,
                   password IS NOT NULL AS has_password, created_at",
    )
    .bind(Uuid::new_v4())
    .bind(email)
    .bind(password)
    .bind(display_name)
    .bind(role)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(admin_user_detail_from_row(row)))
}

pub(crate) async fn get_admin_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
) -> Result<Json<AdminUserDetailDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    Ok(Json(
        load_admin_user(&state.pool, user_id, &administrator.role).await?,
    ))
}

pub(crate) async fn update_admin_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(req): Json<AdminUpdateUserRequest>,
) -> Result<Json<AdminUserDetailDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let email = normalize_email(&req.email)?;
    let display_name = normalize_display_name(Some(&req.display_name), &email)?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let administrator_role = require_administrator_role_tx(&mut tx, administrator.id).await?;
    let target = sqlx::query(
        "SELECT email, role FROM users
         WHERE id = $1 AND deletion_requested_at IS NULL
           AND ($2 = 'super_admin' OR role <> 'super_admin')
         FOR UPDATE",
    )
    .bind(user_id)
    .bind(&administrator_role)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("user not found"))?;
    let existing_email: String = target.get("email");
    let conflict: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM users
             WHERE id <> $1 AND lower(btrim(email)) = lower(btrim($2))
         )",
    )
    .bind(user_id)
    .bind(&email)
    .fetch_one(&mut *tx)
    .await?;
    if conflict {
        return Err(ApiError::conflict("email already exists"));
    }
    let row = sqlx::query(
        "UPDATE users SET email = $1, display_name = $2
         WHERE id = $3
         RETURNING id, email, display_name, role,
                   password IS NOT NULL AS has_password, created_at",
    )
    .bind(&email)
    .bind(display_name)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await?;
    if existing_email != email {
        sqlx::query("DELETE FROM sessions WHERE user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(Json(admin_user_detail_from_row(row)))
}

pub(crate) async fn set_admin_user_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(req): Json<AdminSetUserPasswordRequest>,
) -> Result<Json<AdminUserDetailDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    if !(8..=1024).contains(&req.password.len()) {
        return Err(ApiError::bad_request(
            "password must be between 8 and 1024 bytes",
        ));
    }
    let password =
        password_hash(&req.password).map_err(|_| ApiError::internal("password hashing failed"))?;
    let mut tx = state.pool.begin().await?;
    let administrator_role = require_administrator_role_tx(&mut tx, administrator.id).await?;
    let row = sqlx::query(
        "UPDATE users
         SET password = $1
         WHERE id = $2 AND deletion_requested_at IS NULL
           AND ($3 = 'super_admin' OR role <> 'super_admin')
         RETURNING id, email, display_name, role,
                   password IS NOT NULL AS has_password, created_at",
    )
    .bind(password)
    .bind(user_id)
    .bind(&administrator_role)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("user not found"))?;
    sqlx::query("DELETE FROM sessions WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Json(admin_user_detail_from_row(row)))
}

pub(crate) async fn set_admin_user_role(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(req): Json<AdminSetUserRoleRequest>,
) -> Result<Json<AdminUserDetailDto>, ApiError> {
    let administrator = require_super_admin(&state, &headers).await?;
    let role = match req.role.trim() {
        "member" => "member",
        "admin" => "admin",
        "super_admin" => "super_admin",
        _ => return Err(ApiError::bad_request("unsupported user role")),
    };
    let mut tx = state.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let actor_still_super: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM users
             WHERE id = $1 AND role = 'super_admin'
               AND deletion_requested_at IS NULL
         )",
    )
    .bind(administrator.id)
    .fetch_one(&mut *tx)
    .await?;
    if !actor_still_super {
        return Err(ApiError::forbidden(
            "super administrator permission is required",
        ));
    }
    let existing = sqlx::query(
        "SELECT role FROM users
         WHERE id = $1 AND deletion_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("user not found"))?;
    let existing_role: String = existing.get("role");
    if existing_role == "super_admin" && role != "super_admin" {
        let remaining: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM users
                 WHERE role = 'super_admin'
                   AND deletion_requested_at IS NULL
                   AND id <> $1
             )",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        if !remaining {
            return Err(ApiError::conflict(
                "at least one Super Administrator must remain",
            ));
        }
    }
    let row = sqlx::query(
        "UPDATE users SET role = $1
         WHERE id = $2
         RETURNING id, email, display_name, role,
                   password IS NOT NULL AS has_password, created_at",
    )
    .bind(role)
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(admin_user_detail_from_row(row)))
}

pub(crate) async fn load_admin_user(
    pool: &PgPool,
    user_id: Uuid,
    administrator_role: &str,
) -> Result<AdminUserDetailDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, email, display_name, role,
                password IS NOT NULL AS has_password, created_at
         FROM users
         WHERE id = $1 AND deletion_requested_at IS NULL
           AND ($2 = 'super_admin' OR role <> 'super_admin')",
    )
    .bind(user_id)
    .bind(administrator_role)
    .fetch_optional(pool)
    .await?;
    row.map(admin_user_detail_from_row)
        .ok_or(ApiError::not_found("user not found"))
}

pub(crate) async fn list_user_erasures(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<UserErasureDto>>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT user_id, requested_email, requested_at
         FROM user_erasure_jobs
         WHERE $1 = 'super_admin' OR target_role <> 'super_admin'
         ORDER BY requested_at DESC, user_id",
    )
    .bind(&administrator.role)
    .fetch_all(&state.pool)
    .await?;
    let mut items = rows
        .into_iter()
        .map(|row| UserErasureDto {
            user_id: row.get("user_id"),
            email: Some(row.get("requested_email")),
            status: "pending".into(),
            requested_at: row.get("requested_at"),
            completed_at: None,
        })
        .collect::<Vec<_>>();
    let completed = sqlx::query(
        "SELECT erased_user_id, erased_at
         FROM user_erasure_audit
         WHERE $1 = 'super_admin' OR erased_role <> 'super_admin'
         ORDER BY erased_at DESC, erased_user_id",
    )
    .bind(&administrator.role)
    .fetch_all(&state.pool)
    .await?;
    items.extend(completed.into_iter().map(|row| {
        let erased_at = row.get("erased_at");
        UserErasureDto {
            user_id: row.get("erased_user_id"),
            email: None,
            status: "completed".into(),
            requested_at: erased_at,
            completed_at: Some(erased_at),
        }
    }));
    items.sort_by_key(|item| std::cmp::Reverse((item.requested_at, item.user_id)));
    Ok(Json(items))
}

pub(crate) async fn erase_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(user_id): Path<Uuid>,
    Json(req): Json<EraseUserRequest>,
) -> Result<(StatusCode, Json<UserErasureDto>), ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    begin_user_erasure(&state.pool, administrator.id, user_id, &req.email).await?;
    if let Err(error) = process_user_erasure_job(&state, user_id).await {
        warn!(user_id = %user_id, error = %error.message, "user erasure remains pending");
    }
    let erasure = load_user_erasure(&state.pool, user_id)
        .await?
        .ok_or(ApiError::internal("user erasure status disappeared"))?;
    Ok((StatusCode::ACCEPTED, Json(erasure)))
}

pub(crate) async fn begin_user_erasure(
    pool: &PgPool,
    administrator_id: Uuid,
    user_id: Uuid,
    confirmed_email: &str,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let administrator_role: String = sqlx::query_scalar(
        "SELECT role FROM users
         WHERE id = $1 AND role IN ('admin', 'super_admin')
           AND deletion_requested_at IS NULL",
    )
    .bind(administrator_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::forbidden("administrator permission is required"))?;
    let user = sqlx::query(
        "SELECT email, role, deletion_requested_at
         FROM users WHERE id = $1 FOR UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(user) = user else {
        let completed: Option<String> = sqlx::query_scalar(
            "SELECT erased_role
             FROM user_erasure_audit WHERE erased_user_id = $1",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        return if completed
            .is_some_and(|role| administrator_role == "super_admin" || role != "super_admin")
        {
            Ok(())
        } else {
            Err(ApiError::not_found("user not found"))
        };
    };
    let email: String = user.get("email");
    let target_role: String = user.get("role");
    if administrator_role != "super_admin" && target_role == "super_admin" {
        return Err(ApiError::not_found("user not found"));
    }
    if email != confirmed_email.trim() {
        return Err(ApiError::conflict(
            "email confirmation does not match exactly",
        ));
    }
    if user
        .get::<Option<DateTime<Utc>>, _>("deletion_requested_at")
        .is_some()
    {
        tx.commit().await?;
        return Ok(());
    }
    if target_role == "super_admin" {
        let remaining: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM users
                 WHERE role = 'super_admin'
                   AND deletion_requested_at IS NULL
                   AND id <> $1
             )",
        )
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await?;
        if !remaining {
            return Err(ApiError::conflict(
                "at least one Super Administrator must remain",
            ));
        }
    }

    let agent_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM agents WHERE owner_id = $1 ORDER BY id FOR UPDATE",
    )
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    sqlx::query(
        "SELECT id FROM automations
         WHERE owner_id = $1 OR agent_id = ANY($2)
         ORDER BY id FOR UPDATE",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .fetch_all(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO user_erasure_jobs
             (user_id, requested_by, requested_email, target_role)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (user_id) DO NOTHING",
    )
    .bind(user_id)
    .bind(administrator_id)
    .bind(&email)
    .bind(&target_role)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE users
         SET deletion_requested_at = now(), password = NULL
         WHERE id = $1",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM sessions WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM api_keys WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "INSERT INTO user_erasure_bundle_objects (user_id, object_key)
         SELECT $1, object_key
         FROM (
             SELECT current_bundle_object_key AS object_key
             FROM hub_sessions
             WHERE (owner_id = $1 OR agent_id = ANY($2))
               AND current_bundle_object_key IS NOT NULL
             UNION
             SELECT queue.object_key
             FROM session_bundle_deletion_queue AS queue
             JOIN hub_sessions AS session ON session.id = queue.session_id
             WHERE session.owner_id = $1 OR session.agent_id = ANY($2)
         ) AS objects
         ON CONFLICT (user_id, object_key) DO NOTHING",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "INSERT INTO user_erasure_skill_objects (user_id, object_key)
         SELECT $1, object_key FROM skill_packages WHERE owner_id = $1
         UNION
         SELECT $1, object_key FROM skill_package_deletion_queue WHERE owner_id = $1
         ON CONFLICT (user_id, object_key) DO NOTHING",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE runtime_session_cleanup_obligations AS cleanup
         SET erasure_user_id = $1
         WHERE cleanup.session_id IN (
             SELECT id FROM hub_sessions
             WHERE owner_id = $1 OR agent_id = ANY($2)
         )",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    let owned = sqlx::query(
        "SELECT id, runtime_owner_id, ownership_generation
         FROM hub_sessions
         WHERE (owner_id = $1 OR agent_id = ANY($2))
           AND runtime_owner_id IS NOT NULL
         ORDER BY id FOR UPDATE",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .fetch_all(&mut *tx)
    .await?;
    for session in owned {
        record_runtime_session_cleanup_tx(
            &mut tx,
            session.get("runtime_owner_id"),
            session.get("id"),
            session.get("ownership_generation"),
            Some(user_id),
        )
        .await?;
    }

    sqlx::query(
        "DELETE FROM oauth_authorization_codes AS code
         USING oauth_apps AS app
         WHERE code.oauth_app_id = app.id
           AND (app.owner_id = $1 OR code.subject_user_id = $1)",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM oauth_access_tokens AS token
         USING oauth_apps AS app
         WHERE token.oauth_app_id = app.id
           AND (app.owner_id = $1 OR token.subject_user_id = $1)",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE authentication_channels AS channel
         SET enabled = false, updated_at = now()
         FROM oauth_apps AS app
         WHERE channel.id = app.authentication_channel_id
           AND app.owner_id = $1",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE oauth_apps
         SET client_secret_hash = NULL, redirect_uris = '[]'::jsonb,
             deleted_at = COALESCE(deleted_at, now()), updated_at = now()
         WHERE owner_id = $1",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM integration_app_agents WHERE agent_id = ANY($1)")
        .bind(&agent_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "DELETE FROM automations
         WHERE owner_id = $1 OR agent_id = ANY($2)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM embed_sessions
         WHERE owner_id = $1 OR agent_id = ANY($2)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE integration_tool_requests AS request
         SET status = 'cancelled', responded_at = COALESCE(responded_at, now())
         FROM integration_sessions AS integration
         WHERE request.session_id = integration.id
           AND (integration.owner_id = $1 OR integration.agent_id = ANY($2))
           AND request.status <> 'completed'",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE runs
         SET status = 'failed', runtime_id = NULL,
             model_proxy_token_hash = NULL, work_dir_ref = NULL, updated_at = now()
         WHERE (owner_id = $1 OR agent_id = ANY($2))
           AND status IN ('pending', 'running', 'waiting_tool')",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE runs
         SET model_proxy_token_hash = NULL, work_dir_ref = NULL, updated_at = now()
         WHERE owner_id = $1 OR agent_id = ANY($2)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE hub_session_turns AS turn
         SET status = 'failed', ended_at = COALESCE(ended_at, now()), updated_at = now()
         FROM hub_sessions AS session
         WHERE turn.session_id = session.id
           AND (session.owner_id = $1 OR session.agent_id = ANY($2))
           AND turn.status NOT IN ('completed', 'failed', 'interrupted')",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE hub_session_messages AS message
         SET delivery_state = 'failed'
         FROM hub_sessions AS session
         WHERE message.session_id = session.id
           AND (session.owner_id = $1 OR session.agent_id = ANY($2))
           AND message.delivery_state IN ('queued', 'deferred', 'delivering')",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
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
         WHERE owner_id = $1 OR agent_id = ANY($2)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE agents
         SET instructions = '', visibility = 'private', public_to = '{}',
             runtime_id = NULL, model_policy = '{}'::jsonb,
             sandbox_policy = '{}'::jsonb, mcp_allowlist = '[]'::jsonb,
             execution_config_revision = execution_config_revision + 1,
             deleted_at = COALESCE(deleted_at, now()), updated_at = now()
         WHERE owner_id = $1",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE agents
         SET public_to = array_remove(public_to, $1), updated_at = now()
         WHERE $1 = ANY(public_to)",
    )
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn load_user_erasure(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<UserErasureDto>, ApiError> {
    if let Some(row) = sqlx::query(
        "SELECT requested_email, requested_at
         FROM user_erasure_jobs WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?
    {
        return Ok(Some(UserErasureDto {
            user_id,
            email: Some(row.get("requested_email")),
            status: "pending".into(),
            requested_at: row.get("requested_at"),
            completed_at: None,
        }));
    }
    Ok(sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT erased_at FROM user_erasure_audit WHERE erased_user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?
    .map(|erased_at| UserErasureDto {
        user_id,
        email: None,
        status: "completed".into(),
        requested_at: erased_at,
        completed_at: Some(erased_at),
    }))
}

pub(crate) async fn process_user_erasure_job(
    state: &AppState,
    user_id: Uuid,
) -> Result<(), ApiError> {
    let objects = sqlx::query_scalar::<_, String>(
        "SELECT object_key FROM user_erasure_bundle_objects
         WHERE user_id = $1 ORDER BY created_at, object_key",
    )
    .bind(user_id)
    .fetch_all(&state.pool)
    .await?;
    if !objects.is_empty() {
        let store = state.session_bundle_store.as_ref().ok_or_else(|| {
            ApiError::service_unavailable("Session Bundle object storage is not configured")
        })?;
        for object_key in objects {
            match store.delete(&object_key).await {
                Ok(()) => {
                    sqlx::query(
                        "DELETE FROM user_erasure_bundle_objects
                         WHERE user_id = $1 AND object_key = $2",
                    )
                    .bind(user_id)
                    .bind(&object_key)
                    .execute(&state.pool)
                    .await?;
                }
                Err(error) => {
                    sqlx::query(
                        "UPDATE user_erasure_bundle_objects
                         SET attempts = attempts + 1,
                             last_error = 'object store delete failed', updated_at = now()
                         WHERE user_id = $1 AND object_key = $2",
                    )
                    .bind(user_id)
                    .bind(&object_key)
                    .execute(&state.pool)
                    .await?;
                    warn!(user_id = %user_id, object_key = %object_key, error = %error,
                        "failed to delete erased user's Session Bundle object");
                    return Err(ApiError::bad_gateway(
                        "failed to delete one or more Session Bundle objects",
                    ));
                }
            }
        }
    }
    let skill_objects = sqlx::query_scalar::<_, String>(
        "SELECT object_key FROM user_erasure_skill_objects
         WHERE user_id = $1 ORDER BY created_at, object_key",
    )
    .bind(user_id)
    .fetch_all(&state.pool)
    .await?;
    if !skill_objects.is_empty() {
        let store = state.skill_package_store.as_ref().ok_or_else(|| {
            ApiError::service_unavailable("Skill package object storage is not configured")
        })?;
        for object_key in skill_objects {
            match store.delete(&object_key).await {
                Ok(()) => {
                    let mut tx = state.pool.begin().await?;
                    sqlx::query(
                        "DELETE FROM user_erasure_skill_objects
                         WHERE user_id = $1 AND object_key = $2",
                    )
                    .bind(user_id)
                    .bind(&object_key)
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query("DELETE FROM skill_package_deletion_queue WHERE object_key = $1")
                        .bind(&object_key)
                        .execute(&mut *tx)
                        .await?;
                    tx.commit().await?;
                }
                Err(error) => {
                    sqlx::query(
                        "UPDATE user_erasure_skill_objects
                         SET attempts = attempts + 1,
                             last_error = 'object store delete failed', updated_at = now()
                         WHERE user_id = $1 AND object_key = $2",
                    )
                    .bind(user_id)
                    .bind(&object_key)
                    .execute(&state.pool)
                    .await?;
                    warn!(user_id = %user_id, object_key = %object_key, error = %error,
                        "failed to delete erased user's Skill package object");
                    return Err(ApiError::bad_gateway(
                        "failed to delete one or more Skill package objects",
                    ));
                }
            }
        }
    }
    finalize_user_erasure(state, user_id).await
}

pub(crate) async fn finalize_user_erasure(state: &AppState, user_id: Uuid) -> Result<(), ApiError> {
    let mut tx = state.pool.begin().await?;
    let job = sqlx::query(
        "SELECT requested_by, target_role FROM user_erasure_jobs
         WHERE user_id = $1 FOR UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(job) = job else {
        tx.commit().await?;
        return Ok(());
    };
    let pending_objects: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM user_erasure_bundle_objects WHERE user_id = $1
             UNION ALL
             SELECT 1 FROM user_erasure_skill_objects WHERE user_id = $1
         )",
    )
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await?;
    let pending_workspaces: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM runtime_session_cleanup_obligations
             WHERE erasure_user_id = $1
         )",
    )
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await?;
    if pending_objects || pending_workspaces {
        tx.commit().await?;
        return Ok(());
    }

    let agent_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM agents WHERE owner_id = $1 ORDER BY id FOR UPDATE",
    )
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    let session_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM hub_sessions
         WHERE owner_id = $1 OR agent_id = ANY($2)
         ORDER BY id FOR UPDATE",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .fetch_all(&mut *tx)
    .await?;
    let run_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM runs
         WHERE owner_id = $1 OR agent_id = ANY($2) OR hub_session_id = ANY($3)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .bind(&session_ids)
    .fetch_all(&mut *tx)
    .await?;

    sqlx::query(
        "UPDATE agents
         SET execution_config_revision = execution_config_revision + 1, updated_at = now()
         WHERE id IN (
             SELECT agent_skills.agent_id
             FROM agent_skills
             JOIN skills ON skills.id = agent_skills.skill_id
             WHERE skills.owner_id = $1 AND agent_skills.agent_id <> ALL($2)
         )",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE hub_sessions SET active_turn_id = NULL
         WHERE id = ANY($1)",
    )
    .bind(&session_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE hub_session_messages
         SET run_id = NULL, turn_id = NULL
         WHERE session_id = ANY($1)",
    )
    .bind(&session_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM integration_attachments
         WHERE hub_message_id IN (
             SELECT id FROM hub_session_messages WHERE session_id = ANY($1)
         ) OR run_id = ANY($2)",
    )
    .bind(&session_ids)
    .bind(&run_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM integration_messages
         WHERE hub_message_id IN (
             SELECT id FROM hub_session_messages WHERE session_id = ANY($1)
         ) OR run_id = ANY($2)",
    )
    .bind(&session_ids)
    .bind(&run_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM run_events
         WHERE run_id = ANY($1) OR hub_message_id IN (
             SELECT id FROM hub_session_messages WHERE session_id = ANY($2)
         )",
    )
    .bind(&run_ids)
    .bind(&session_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM embed_sessions WHERE hub_session_id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM integration_sessions WHERE hub_session_id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM session_bundle_deletion_queue WHERE session_id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM runs WHERE id = ANY($1)")
        .bind(&run_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM hub_session_messages WHERE session_id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM hub_session_turns WHERE session_id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM hub_sessions WHERE id = ANY($1)")
        .bind(&session_ids)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "WITH deleted_apps AS (
             DELETE FROM oauth_apps
             WHERE owner_id = $1 OR agent_id = ANY($2)
             RETURNING external_platform_id
         )
         DELETE FROM external_platforms
         WHERE id IN (SELECT external_platform_id FROM deleted_apps)",
    )
    .bind(user_id)
    .bind(&agent_ids)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM agents WHERE id = ANY($1)")
        .bind(&agent_ids)
        .execute(&mut *tx)
        .await?;
    let erased_at = Utc::now();
    sqlx::query(
        "INSERT INTO user_erasure_audit
             (erased_user_id, acting_administrator_id, erased_at, erased_role)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (erased_user_id) DO NOTHING",
    )
    .bind(user_id)
    .bind(job.get::<Uuid, _>("requested_by"))
    .bind(erased_at)
    .bind(job.get::<String, _>("target_role"))
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM user_erasure_jobs WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn user_erasure_loop(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let user_ids = match sqlx::query_scalar::<_, Uuid>(
            "SELECT user_id FROM user_erasure_jobs ORDER BY requested_at, user_id",
        )
        .fetch_all(&state.pool)
        .await
        {
            Ok(user_ids) => user_ids,
            Err(error) => {
                warn!(error = %error, "failed to list pending user erasures");
                continue;
            }
        };
        for user_id in user_ids {
            if let Err(error) = process_user_erasure_job(&state, user_id).await {
                let _ = sqlx::query(
                    "UPDATE user_erasure_jobs
                     SET attempts = attempts + 1, last_error = $1, updated_at = now()
                     WHERE user_id = $2",
                )
                .bind(&error.message)
                .bind(user_id)
                .execute(&state.pool)
                .await;
                warn!(user_id = %user_id, error = %error.message, "user erasure retry failed");
            }
        }
    }
}

pub(crate) const DEFAULT_MAX_ATTACHMENT_UPLOAD_BYTES: i64 = 104_857_600;
pub(crate) const DEFAULT_MAX_ATTACHMENT_BYTES_PER_SESSION: i64 = 524_288_000;

pub(crate) async fn load_system_settings(pool: &PgPool) -> Result<SystemSettingsDto, ApiError> {
    let row = sqlx::query(
        "SELECT max_attachment_upload_bytes, max_attachment_bytes_per_session
         FROM system_settings WHERE singleton = true",
    )
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some(row) => SystemSettingsDto {
            max_attachment_upload_bytes: row.get("max_attachment_upload_bytes"),
            max_attachment_bytes_per_session: row.get("max_attachment_bytes_per_session"),
        },
        None => SystemSettingsDto {
            max_attachment_upload_bytes: DEFAULT_MAX_ATTACHMENT_UPLOAD_BYTES,
            max_attachment_bytes_per_session: DEFAULT_MAX_ATTACHMENT_BYTES_PER_SESSION,
        },
    })
}

pub(crate) async fn get_system_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<SystemSettingsDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    Ok(Json(load_system_settings(&state.pool).await?))
}

pub(crate) async fn update_system_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<UpdateSystemSettingsRequest>,
) -> Result<Json<SystemSettingsDto>, ApiError> {
    let user = require_administrator(&state, &headers).await?;
    if !(1024 * 1024..=1024 * 1024 * 1024).contains(&req.max_attachment_upload_bytes) {
        return Err(ApiError::bad_request(
            "max attachment upload size must be between 1MB and 1GB",
        ));
    }
    if req.max_attachment_bytes_per_session < req.max_attachment_upload_bytes {
        return Err(ApiError::bad_request(
            "max attachment bytes per session must not be smaller than the single upload limit",
        ));
    }
    if req.max_attachment_bytes_per_session > 10_i64 * 1024 * 1024 * 1024 {
        return Err(ApiError::bad_request(
            "max attachment bytes per session must not exceed 10GB",
        ));
    }
    require_administrator_role_tx(&mut state.pool.begin().await?, user.id).await?;
    let row = sqlx::query(
        "UPDATE system_settings
         SET max_attachment_upload_bytes = $1,
             max_attachment_bytes_per_session = $2,
             updated_by = $3, updated_at = now()
         WHERE singleton = true
         RETURNING max_attachment_upload_bytes, max_attachment_bytes_per_session",
    )
    .bind(req.max_attachment_upload_bytes)
    .bind(req.max_attachment_bytes_per_session)
    .bind(user.id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(SystemSettingsDto {
        max_attachment_upload_bytes: row.get("max_attachment_upload_bytes"),
        max_attachment_bytes_per_session: row.get("max_attachment_bytes_per_session"),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use crate::load_run_for_user;

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn administrator_authority_hides_super_admin_accounts_and_opens_global_resources(
        pool: PgPool,
    ) {
        let member_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let member_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&member_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let super_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&super_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();

        assert_eq!(
            list_admin_users(State(state.clone()), session_headers(&member_token))
                .await
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
        let visible = list_admin_users(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap()
            .0;
        assert!(visible.iter().any(|item| item.user.id == member_id));
        assert!(visible.iter().all(|item| item.user.role != "super_admin"));

        let hidden = get_admin_user(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_id),
        )
        .await
        .unwrap_err();
        assert_eq!(hidden.status, StatusCode::NOT_FOUND);
        let hidden_password = set_admin_user_password(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_id),
            Json(AdminSetUserPasswordRequest {
                password: "replacement-password".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(hidden_password.status, StatusCode::NOT_FOUND);

        let _ = get_admin_user(
            State(state.clone()),
            session_headers(&admin_token),
            Path(member_id),
        )
        .await
        .unwrap();
        let _ = get_auth_policy(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap();
        let _ = list_external_platforms(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap();
        let _ = list_runtime_enrollment_tokens(State(state), session_headers(&admin_token))
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn administrator_can_manage_super_admin_agents_and_view_their_runs(pool: PgPool) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let member_token = create_user_session_with_role(&pool, "member").await;
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let member_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&member_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let super_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&super_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let member_agent_id = Uuid::new_v4();
        let super_agent_id = Uuid::new_v4();
        for (agent_id, owner_id, name) in [
            (member_agent_id, member_id, "Member Agent"),
            (super_agent_id, super_id, "Protected Agent"),
        ] {
            sqlx::query(
                "INSERT INTO agents
                     (id, owner_id, name, instructions, visibility, model_policy)
                 VALUES ($1, $2, $3, 'private instructions', 'private',
                         '{\"provider\":\"hub-proxy\"}'::jsonb)",
            )
            .bind(agent_id)
            .bind(owner_id)
            .bind(name)
            .execute(&state.pool)
            .await
            .unwrap();
        }
        let protected_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'offline')",
        )
        .bind(protected_session_id)
        .bind(super_id)
        .bind(super_agent_id)
        .execute(&state.pool)
        .await
        .unwrap();
        let protected_turn_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'completed', 0)",
        )
        .bind(protected_turn_id)
        .bind(protected_session_id)
        .execute(&state.pool)
        .await
        .unwrap();
        let protected_run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  hub_session_id, hub_turn_id, session_ownership_generation)
             VALUES ($1, $2, $3, 'completed', 'private super prompt', 'console',
                     $4, $5, 0)",
        )
        .bind(protected_run_id)
        .bind(super_agent_id)
        .bind(super_id)
        .bind(protected_session_id)
        .bind(protected_turn_id)
        .execute(&state.pool)
        .await
        .unwrap();

        let directory = list_users(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap()
            .0;
        assert!(directory.iter().all(|user| user.id != super_id));
        let agents = list_agents(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap()
            .0;
        assert!(agents.iter().any(|agent| agent.id == member_agent_id));
        assert!(agents.iter().any(|agent| agent.id == super_agent_id));

        let admin = require_user(&state, &session_headers(&admin_token))
            .await
            .unwrap();
        assert!(
            load_agent_manageable_by_user(&state.pool, member_agent_id, &admin)
                .await
                .is_ok()
        );
        assert!(load_agent_for_user(&state.pool, super_agent_id, &admin)
            .await
            .is_ok());
        assert!(
            load_agent_manageable_by_user(&state.pool, super_agent_id, &admin)
                .await
                .is_ok()
        );

        let admin_runs = list_agent_runs(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_agent_id),
        )
        .await
        .unwrap()
        .0;
        assert!(admin_runs.iter().any(|run| run.id == protected_run_id));
        assert!(load_run_for_user(&state.pool, protected_run_id, &admin)
            .await
            .is_ok());

        let member = require_user(&state, &session_headers(&member_token))
            .await
            .unwrap();
        assert_eq!(
            load_agent_for_user(&state.pool, super_agent_id, &member)
                .await
                .unwrap_err()
                .status,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            load_run_for_user(&state.pool, protected_run_id, &member)
                .await
                .unwrap_err()
                .status,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            delete_agent(
                State(state.clone()),
                session_headers(&member_token),
                Path(super_agent_id),
            )
            .await
            .unwrap_err()
            .status,
            StatusCode::FORBIDDEN
        );

        assert_eq!(
            delete_agent(
                State(state.clone()),
                session_headers(&admin_token),
                Path(super_agent_id),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        let super_admin = require_user(&state, &session_headers(&super_token))
            .await
            .unwrap();
        assert!(
            load_run_for_user(&state.pool, protected_run_id, &super_admin)
                .await
                .is_ok()
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn admin_model_options_hide_super_admin_personal_connections(pool: PgPool) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let super_user = require_user(&state, &session_headers(&super_token))
            .await
            .unwrap();
        let global = create_test_model_connection_for_token(
            &state,
            &admin_token,
            ModelConnectionScope::Global,
            "Super Agent Global",
        )
        .await;
        let super_personal = create_test_model_connection_for_token(
            &state,
            &super_token,
            ModelConnectionScope::Personal,
            "Super Agent Personal",
        )
        .await;
        let super_agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, model_policy,
                  model_connection_id, model_id)
             VALUES ($1, $2, 'Protected Model Agent', '', 'private',
                     '{\"provider\":\"hub-proxy\"}'::jsonb, $3, $4)",
        )
        .bind(super_agent_id)
        .bind(super_user.id)
        .bind(super_personal.id)
        .bind(&super_personal.allowed_model_ids[0])
        .execute(&state.pool)
        .await
        .unwrap();

        let admin_options = get_agent_model_connection_options(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_agent_id),
        )
        .await
        .unwrap()
        .0;
        assert!(admin_options
            .items
            .iter()
            .any(|item| item.connection_id == global.id));
        assert!(!admin_options
            .items
            .iter()
            .any(|item| item.connection_id == super_personal.id));

        let super_options = get_agent_model_connection_options(
            State(state.clone()),
            session_headers(&super_token),
            Path(super_agent_id),
        )
        .await
        .unwrap()
        .0;
        assert!(super_options
            .items
            .iter()
            .any(|item| item.connection_id == super_personal.id));

        let mut admin_edit = test_update_agent_request();
        admin_edit.model_selection = Some(test_model_selection(&super_personal));
        let updated = update_agent(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_agent_id),
            Json(admin_edit),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            updated.model_selection,
            Some(test_model_selection(&super_personal))
        );

        // An ordinary admin may retain the owner's existing Personal selection,
        // but must not rebind the Agent to a different Personal connection.
        let other_super_personal = create_test_model_connection_for_token(
            &state,
            &super_token,
            ModelConnectionScope::Personal,
            "Other Super Agent Personal",
        )
        .await;
        let mut blocked = test_update_agent_request();
        blocked.model_selection = Some(test_model_selection(&other_super_personal));
        let error = update_agent(
            State(state),
            session_headers(&admin_token),
            Path(super_agent_id),
            Json(blocked),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn system_settings_require_admin_and_validate_ranges(pool: PgPool) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let member_token = create_user_session_with_role(&pool, "member").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));

        let defaults = get_system_settings(State(state.clone()), session_headers(&admin_token))
            .await
            .unwrap()
            .0;
        assert_eq!(defaults.max_attachment_upload_bytes, 104_857_600);
        assert_eq!(defaults.max_attachment_bytes_per_session, 524_288_000);

        assert!(
            get_system_settings(State(state.clone()), session_headers(&member_token))
                .await
                .is_err()
        );
        assert!(update_system_settings(
            State(state.clone()),
            session_headers(&member_token),
            Json(UpdateSystemSettingsRequest {
                max_attachment_upload_bytes: 10 * 1024 * 1024,
                max_attachment_bytes_per_session: 20 * 1024 * 1024,
            }),
        )
        .await
        .is_err());

        // 单文件上限低于 1MB 拒绝；会话上限小于单文件上限拒绝。
        assert!(update_system_settings(
            State(state.clone()),
            session_headers(&admin_token),
            Json(UpdateSystemSettingsRequest {
                max_attachment_upload_bytes: 512 * 1024,
                max_attachment_bytes_per_session: 1024 * 1024,
            }),
        )
        .await
        .is_err());
        assert!(update_system_settings(
            State(state.clone()),
            session_headers(&admin_token),
            Json(UpdateSystemSettingsRequest {
                max_attachment_upload_bytes: 10 * 1024 * 1024,
                max_attachment_bytes_per_session: 5 * 1024 * 1024,
            }),
        )
        .await
        .is_err());

        let updated = update_system_settings(
            State(state.clone()),
            session_headers(&admin_token),
            Json(UpdateSystemSettingsRequest {
                max_attachment_upload_bytes: 200 * 1024 * 1024,
                max_attachment_bytes_per_session: 1024 * 1024 * 1024,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated.max_attachment_upload_bytes, 200 * 1024 * 1024);
        assert_eq!(
            load_system_settings(&state.pool)
                .await
                .unwrap()
                .max_attachment_upload_bytes,
            200 * 1024 * 1024
        );
    }
}
