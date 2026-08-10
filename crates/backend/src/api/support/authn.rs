//! 认证：principal、provider 实现与请求身份校验。

use super::*;
use crate::{load_auth_policy, verify_embed_jwt_claims};
use crate::{ApplicationPrincipal, IntegrationPrincipal};
use agent_hub_shared::*;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

pub(crate) async fn authenticate_with_providers(
    state: &AppState,
    credential: AuthCredential,
) -> Result<AuthPrincipal, ApiError> {
    for provider in &state.auth_providers {
        if let Some(principal) = provider.authenticate(state, &credential).await? {
            return Ok(principal);
        }
    }
    Err(ApiError::unauthorized("invalid authentication credential"))
}

#[async_trait]
impl AuthProvider for PasswordAuthProvider {
    async fn authenticate(
        &self,
        state: &AppState,
        credential: &AuthCredential,
    ) -> Result<Option<AuthPrincipal>, ApiError> {
        let AuthCredential::Password { email, password } = credential else {
            return Ok(None);
        };
        let policy = load_auth_policy(&state.pool).await?;
        let row = sqlx::query(
            "SELECT id, email, display_name, role, password
             FROM users
             WHERE lower(btrim(email)) = lower(btrim($1))
               AND deletion_requested_at IS NULL",
        )
        .bind(email.trim())
        .fetch_optional(&state.pool)
        .await?;
        let Some(row) = row else {
            return Err(ApiError::unauthorized("invalid credentials"));
        };
        let stored_password: Option<String> = row.get("password");
        let Some(stored_password) = stored_password else {
            return Err(ApiError::unauthorized("invalid credentials"));
        };
        if !verify_password(&stored_password, password) {
            return Err(ApiError::unauthorized("invalid credentials"));
        }
        let role: String = row.get("role");
        if !policy.password_login_enabled && role != "super_admin" {
            return Err(ApiError::forbidden("password login is disabled"));
        }
        let user_id: Uuid = row.get("id");
        if password_needs_upgrade(&stored_password) {
            let upgraded = password_hash(password)
                .map_err(|_| ApiError::internal("password hashing failed"))?;
            sqlx::query(
                "UPDATE users SET password = $1
                 WHERE id = $2 AND deletion_requested_at IS NULL",
            )
            .bind(upgraded)
            .bind(user_id)
            .execute(&state.pool)
            .await?;
        }
        Ok(Some(AuthPrincipal::User {
            user: UserDto {
                id: user_id,
                email: row.get("email"),
                display_name: row.get("display_name"),
                role,
            },
            _provider: "password",
            api_key_id: None,
        }))
    }
}

#[async_trait]
impl AuthProvider for BrowserSessionAuthProvider {
    async fn authenticate(
        &self,
        state: &AppState,
        credential: &AuthCredential,
    ) -> Result<Option<AuthPrincipal>, ApiError> {
        let AuthCredential::Headers(headers) = credential else {
            return Ok(None);
        };
        let Some(token) = session_token_from_headers(headers) else {
            return Ok(None);
        };
        Ok(Some(AuthPrincipal::User {
            user: load_user_by_session(&state.pool, &token).await?,
            _provider: "session",
            api_key_id: None,
        }))
    }
}

#[async_trait]
impl AuthProvider for ApiKeyAuthProvider {
    async fn authenticate(
        &self,
        state: &AppState,
        credential: &AuthCredential,
    ) -> Result<Option<AuthPrincipal>, ApiError> {
        let AuthCredential::Headers(headers) = credential else {
            return Ok(None);
        };
        let Some(token) = bearer_token(headers).filter(|token| token.starts_with("ahk_")) else {
            return Ok(None);
        };
        let (api_key_id, user) = load_user_and_key_id_by_api_key(&state.pool, &token).await?;
        Ok(Some(AuthPrincipal::User {
            user,
            _provider: "api_key",
            api_key_id: Some(api_key_id),
        }))
    }
}

#[async_trait]
impl AuthProvider for EmbedJwtAuthProvider {
    async fn authenticate(
        &self,
        state: &AppState,
        credential: &AuthCredential,
    ) -> Result<Option<AuthPrincipal>, ApiError> {
        let AuthCredential::EmbedJwt(jwt) = credential else {
            return Ok(None);
        };
        verify_embed_jwt_claims(state, jwt).await.map(Some)
    }
}

#[async_trait]
impl SessionIssuer for BrowserSessionIssuer {
    async fn issue(&self, state: &AppState, user_id: Uuid) -> Result<HeaderMap, ApiError> {
        let token = format!("ahs_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let expires_at = Utc::now() + ChronoDuration::days(7);
        let inserted = sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             SELECT $1, id, $3 FROM users
             WHERE id = $2 AND deletion_requested_at IS NULL",
        )
        .bind(sha256_hex(&token))
        .bind(user_id)
        .bind(expires_at)
        .execute(&state.pool)
        .await?;
        if inserted.rows_affected() != 1 {
            return Err(ApiError::unauthorized("user is unavailable"));
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            header::SET_COOKIE,
            cookie_header(&token, state.session_cookie_secure)?,
        );
        Ok(headers)
    }
}

pub(crate) async fn require_user(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserDto, ApiError> {
    match require_principal(state, headers).await? {
        AuthPrincipal::User { user, .. } => Ok(user),
        AuthPrincipal::Embed { .. } => Err(ApiError::forbidden(
            "embed token cannot access control plane",
        )),
    }
}

pub(crate) async fn require_super_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserDto, ApiError> {
    let user = require_user(state, headers).await?;
    if user.role != "super_admin" {
        return Err(ApiError::forbidden(
            "super administrator permission is required",
        ));
    }
    Ok(user)
}

pub(crate) async fn require_administrator(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserDto, ApiError> {
    let user = require_user(state, headers).await?;
    if !is_admin_role(&user.role) {
        return Err(ApiError::forbidden("administrator permission is required"));
    }
    Ok(user)
}

pub(crate) async fn require_administrator_role_tx(
    tx: &mut Transaction<'_, Postgres>,
    user_id: Uuid,
) -> Result<String, ApiError> {
    sqlx::query_scalar(
        "SELECT role FROM users
         WHERE id = $1 AND role IN ('admin', 'super_admin')
           AND deletion_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::forbidden("administrator permission is required"))
}

pub(crate) async fn require_user_with_api_key_id(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(UserDto, Option<Uuid>), ApiError> {
    match require_principal(state, headers).await? {
        AuthPrincipal::User {
            user, api_key_id, ..
        } => Ok((user, api_key_id)),
        AuthPrincipal::Embed { .. } => Err(ApiError::forbidden(
            "embed token cannot access control plane",
        )),
    }
}

pub(crate) async fn require_principal(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthPrincipal, ApiError> {
    authenticate_with_providers(state, AuthCredential::Headers(headers.clone())).await
}

pub(crate) async fn require_integration(
    state: &AppState,
    headers: &HeaderMap,
    agent_id: Uuid,
) -> Result<IntegrationPrincipal, ApiError> {
    let principal = require_application_token(state, headers).await?;
    if !principal.scopes.contains(&format!("agent:{agent_id}")) {
        return Err(ApiError::forbidden(
            "application token is missing agent scope",
        ));
    }
    let agent_owner_id: Uuid = sqlx::query_scalar(
        "SELECT agent.owner_id
         FROM integration_app_agents AS delegated
         JOIN agents AS agent ON agent.id = delegated.agent_id
         WHERE delegated.app_id = $1 AND delegated.agent_id = $2
           AND agent.deleted_at IS NULL
           AND (agent.owner_id = $3 OR agent.visibility = 'public'
                OR (agent.visibility = 'public_to' AND $3 = ANY(agent.public_to)))
           AND ($4::uuid IS NULL OR agent.owner_id = $4
                OR agent.visibility = 'public'
                OR (agent.visibility = 'public_to' AND $4 = ANY(agent.public_to)))",
    )
    .bind(principal.oauth_app_id)
    .bind(agent_id)
    .bind(principal.app_owner_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::forbidden(
        "application agent delegation is no longer valid",
    ))?;
    Ok(IntegrationPrincipal {
        oauth_app_id: principal.oauth_app_id,
        grant_type: principal.grant_type,
        subject_user_id: principal.subject_user_id,
        agent_id,
        agent_owner_id,
        external_platform_id: principal.external_platform_id,
        authentication_channel_id: principal.authentication_channel_id,
        origin_tenant_id: principal.origin_tenant_id,
        origin_external_identity_id: principal.origin_external_identity_id,
    })
}

pub(crate) async fn require_application_token(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<ApplicationPrincipal, ApiError> {
    let token = bearer_token(headers).ok_or(ApiError::unauthorized("missing integration token"))?;
    if !token.starts_with("aho_") {
        return Err(ApiError::unauthorized("invalid integration token"));
    }
    let row = sqlx::query(
        "UPDATE oauth_access_tokens AS token
         SET last_used_at = now()
         FROM oauth_apps AS app, authentication_channels AS channel, users AS owner
         WHERE token.token_hash = $1
           AND token.revoked_at IS NULL
           AND token.expires_at > now()
           AND app.id = token.oauth_app_id
           AND app.deleted_at IS NULL
           AND app.client_secret_hash IS NOT NULL
           AND owner.id = app.owner_id
           AND owner.deletion_requested_at IS NULL
           AND channel.id = app.authentication_channel_id
           AND channel.platform_id = app.external_platform_id
           AND channel.enabled = true
           AND channel.trusted_email = true
           AND (token.subject_user_id IS NULL OR EXISTS (
               SELECT 1 FROM users AS subject
               WHERE subject.id = token.subject_user_id
                 AND subject.deletion_requested_at IS NULL
           ))
         RETURNING token.oauth_app_id, app.owner_id,
                   token.grant_type, token.subject_user_id, token.scopes,
                   app.external_platform_id, app.authentication_channel_id,
                   token.origin_tenant_id, token.origin_external_identity_id",
    )
    .bind(sha256_hex(&token))
    .fetch_optional(&state.pool)
    .await?;
    let row = row.ok_or(ApiError::unauthorized("invalid integration token"))?;
    Ok(ApplicationPrincipal {
        oauth_app_id: row.get("oauth_app_id"),
        app_owner_id: row.get("owner_id"),
        grant_type: row.get("grant_type"),
        subject_user_id: row.get("subject_user_id"),
        scopes: row.get::<Vec<String>, _>("scopes").into_iter().collect(),
        external_platform_id: row.get("external_platform_id"),
        authentication_channel_id: row.get("authentication_channel_id"),
        origin_tenant_id: row.get("origin_tenant_id"),
        origin_external_identity_id: row.get("origin_external_identity_id"),
    })
}

pub(crate) async fn lock_active_integration_agent_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    owner_id: Uuid,
) -> Result<(), ApiError> {
    let active: Option<Uuid> = sqlx::query_scalar(
        "SELECT agents.id FROM agents
         JOIN users ON users.id = agents.owner_id
         WHERE agents.id = $1 AND agents.owner_id = $2
           AND agents.deleted_at IS NULL
           AND users.deletion_requested_at IS NULL
         FOR UPDATE",
    )
    .bind(agent_id)
    .bind(owner_id)
    .fetch_optional(&mut **tx)
    .await?;
    active
        .map(|_| ())
        .ok_or(ApiError::unauthorized("invalid integration credential"))
}

pub(crate) async fn load_user_by_session(pool: &PgPool, token: &str) -> Result<UserDto, ApiError> {
    let row = sqlx::query(
        "SELECT u.id, u.email, u.display_name, u.role
         FROM sessions s
         JOIN users u ON u.id = s.user_id
         WHERE s.token_hash = $1 AND s.expires_at > now()
           AND u.deletion_requested_at IS NULL",
    )
    .bind(sha256_hex(token))
    .fetch_optional(pool)
    .await?;
    row.map(user_from_row)
        .ok_or(ApiError::unauthorized("invalid session"))
}

pub(crate) async fn load_active_user(pool: &PgPool, user_id: Uuid) -> Result<UserDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, email, display_name, role
         FROM users WHERE id = $1 AND deletion_requested_at IS NULL",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.map(user_from_row)
        .ok_or(ApiError::unauthorized("user is unavailable"))
}

pub(crate) async fn load_active_user_tx(
    tx: &mut Transaction<'_, Postgres>,
    user_id: Uuid,
) -> Result<UserDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, email, display_name, role
         FROM users WHERE id = $1 AND deletion_requested_at IS NULL",
    )
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(user_from_row)
        .ok_or(ApiError::unauthorized("user is unavailable"))
}

#[cfg(test)]
pub(crate) async fn load_user_by_api_key(pool: &PgPool, token: &str) -> Result<UserDto, ApiError> {
    load_user_and_key_id_by_api_key(pool, token)
        .await
        .map(|(_, user)| user)
}

pub(crate) async fn load_user_and_key_id_by_api_key(
    pool: &PgPool,
    token: &str,
) -> Result<(Uuid, UserDto), ApiError> {
    let token_hash = sha256_hex(token);
    let row = sqlx::query(
        "UPDATE api_keys
         SET last_used_at = now()
         WHERE token_hash = $1 AND (expires_at IS NULL OR expires_at > now())
         RETURNING id, user_id",
    )
    .bind(token_hash)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Err(ApiError::unauthorized("invalid api key"));
    };
    let api_key_id: Uuid = row.get("id");
    let user_id: Uuid = row.get("user_id");
    let row = sqlx::query(
        "SELECT id, email, display_name, role
         FROM users WHERE id = $1 AND deletion_requested_at IS NULL",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| (api_key_id, user_from_row(row)))
        .ok_or(ApiError::unauthorized("invalid api key"))
}

pub(crate) async fn require_runtime(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Uuid, ApiError> {
    let token = bearer_token(headers).ok_or(ApiError::unauthorized("missing runtime token"))?;
    let row = sqlx::query(
        "SELECT id FROM runtimes
         WHERE token_hash = $1
           AND status IN ('online', 'draining')
           AND credential_revoked_at IS NULL",
    )
    .bind(sha256_hex(&token))
    .fetch_optional(&state.pool)
    .await?;
    row.map(|row| row.get("id")).ok_or(ApiError::unauthorized(
        "runtime is not active or its credential is invalid",
    ))
}

use super::error::ApiError;
use axum::http::{header, HeaderMap, HeaderValue};

pub(crate) fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie.split(';').map(str::trim).find_map(|part| {
        part.strip_prefix("agent_hub_session=")
            .map(ToOwned::to_owned)
    })
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(ToOwned::to_owned)
}

pub(crate) fn embed_token_from_headers(headers: &HeaderMap) -> Option<String> {
    scoped_token_from_headers(headers, "x-agent-hub-embed-token", "Embed ")
}

pub(crate) fn client_access_token_from_headers(headers: &HeaderMap) -> Option<String> {
    embed_token_from_headers(headers).or_else(|| {
        bearer_token(headers).filter(|token| {
            token.starts_with("ahe_") || token.starts_with("ahw_") || token.starts_with("ahp_")
        })
    })
}

pub(crate) fn webhook_token_from_headers(headers: &HeaderMap) -> Option<String> {
    scoped_token_from_headers(headers, "x-agent-hub-webhook-token", "Webhook ")
}

pub(crate) fn scoped_token_from_headers(
    headers: &HeaderMap,
    header_name: &'static str,
    authorization_prefix: &str,
) -> Option<String> {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix(authorization_prefix))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub(crate) fn cookie_header(token: &str, secure: bool) -> Result<HeaderValue, ApiError> {
    // V1 使用 HttpOnly cookie，前端通过 credentials 发送，不把 session 暴露给 JS。
    let secure_attr = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "agent_hub_session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800{secure_attr}"
    ))
    .map_err(|_| ApiError::internal("failed to build session cookie"))
}
