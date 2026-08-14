//! auth 领域模块：登录/注册/会话、认证策略、LDAP、外部平台、认证通道与 API 密钥。

use super::*;
use crate::normalize_origin_tenant;
use agent_hub_shared::*;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use ipnet::IpNet;
use ldap3::{
    dn_escape, ldap_escape, result::LdapError, LdapConnAsync, LdapConnSettings, Scope, SearchEntry,
};
use serde::Deserialize;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

pub(crate) async fn login(
    State(state): State<Arc<AppState>>,
    MaybeConnectInfo(peer_address): MaybeConnectInfo,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let source_ip = login_source_ip(
        &headers,
        peer_address.map(|address| address.ip()),
        state.trusted_proxy_cidrs.as_deref(),
    );
    record_ip_login_attempt(&state.pool, source_ip).await?;
    let rate_email = login_rate_email(&req.email);
    if let Some(email) = rate_email.as_deref() {
        reserve_email_login_attempt(&state.pool, email).await?;
    }
    let principal = match authenticate_with_providers(
        &state,
        AuthCredential::Password {
            email: req.email,
            password: req.password,
        },
    )
    .await
    {
        Ok(principal) => principal,
        Err(error) => return Err(error),
    };
    let AuthPrincipal::User { user, .. } = principal else {
        return Err(ApiError::unauthorized("invalid credentials"));
    };
    if let Some(email) = rate_email.as_deref() {
        clear_email_login_failures(&state.pool, email).await?;
    }
    let headers = state.session_issuer.issue(&state, user.id).await?;
    Ok((headers, Json(LoginResponse { user })))
}

pub(crate) async fn ldap_login(
    State(state): State<Arc<AppState>>,
    MaybeConnectInfo(peer_address): MaybeConnectInfo,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let request_id = Uuid::new_v4();
    let started_at = Instant::now();
    let source_ip = login_source_ip(
        &headers,
        peer_address.map(|address| address.ip()),
        state.trusted_proxy_cidrs.as_deref(),
    );
    record_ip_login_attempt(&state.pool, source_ip).await?;
    let rate_email = login_rate_email(&req.email);
    if let Some(email) = rate_email.as_deref() {
        reserve_email_login_attempt(&state.pool, email).await?;
    }
    if !load_auth_policy(&state.pool).await?.ldap_login_enabled {
        return Err(ApiError::forbidden("LDAP login is disabled"));
    }
    let configuration =
        load_ldap_configuration(&state.pool)
            .await?
            .ok_or(ApiError::service_unavailable(
                "LDAP service is temporarily unavailable",
            ))?;
    let identity = match query_ldap_directory(&configuration, &req.email, &req.password).await {
        Ok(identity) => identity,
        Err(error) => {
            warn!(
                request_id = %request_id,
                stage = error.stage,
                category = error.category,
                duration_ms = started_at.elapsed().as_millis(),
                "LDAP login failed"
            );
            return Err(error.for_login());
        }
    };
    let user = resolve_ldap_user(&state.pool, &identity).await?;
    if let Some(email) = rate_email.as_deref() {
        clear_email_login_failures(&state.pool, email).await?;
    }
    info!(
        request_id = %request_id,
        stage = "complete",
        category = "success",
        duration_ms = started_at.elapsed().as_millis(),
        "LDAP login completed"
    );
    let headers = state.session_issuer.issue(&state, user.id).await?;
    Ok((headers, Json(LoginResponse { user })))
}

pub(crate) async fn register_password_user(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PasswordRegistrationRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let policy = load_auth_policy(&state.pool).await?;
    if !policy.password_registration_enabled {
        return Err(ApiError::forbidden("password registration is disabled"));
    }
    let email = normalize_email(&req.email)?;
    if !(8..=1024).contains(&req.password.len()) {
        return Err(ApiError::bad_request(
            "password must be between 8 and 1024 bytes",
        ));
    }
    let password =
        password_hash(&req.password).map_err(|_| ApiError::internal("password hashing failed"))?;
    let user = create_password_registration_user(
        &state.pool,
        &email,
        req.display_name.as_deref(),
        Some(&password),
    )
    .await?;
    let headers = state.session_issuer.issue(&state, user.id).await?;
    Ok((headers, Json(PasswordRegistrationResponse { user })))
}

pub(crate) async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    if let Some(token) = session_token_from_headers(&headers) {
        sqlx::query("DELETE FROM sessions WHERE token_hash = $1")
            .bind(sha256_hex(&token))
            .execute(&state.pool)
            .await?;
    }
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_static("agent_hub_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
    );
    Ok((response_headers, StatusCode::NO_CONTENT))
}

pub(crate) async fn me(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<UserDto>, ApiError> {
    Ok(Json(require_user(&state, &headers).await?))
}

pub(crate) async fn update_current_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<UpdateCurrentUserRequest>,
) -> Result<Json<UserDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let display_name = normalize_display_name(Some(&req.display_name), &user.email)?;
    let row = sqlx::query(
        "UPDATE users SET display_name = $1
         WHERE id = $2 AND deletion_requested_at IS NULL
         RETURNING id, email, display_name, role",
    )
    .bind(display_name)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::unauthorized("user account is unavailable"))?;
    Ok(Json(user_from_row(row)))
}

pub(crate) async fn list_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<UserDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, email, display_name, role
         FROM users
         WHERE deletion_requested_at IS NULL
           AND (role <> 'super_admin' OR $1 = 'super_admin' OR id = $2)
         ORDER BY display_name, email",
    )
    .bind(&user.role)
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(user_from_row).collect()))
}

pub(crate) async fn auth_providers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<AuthProvidersResponse>, ApiError> {
    let policy = load_auth_policy(&state.pool).await?;
    Ok(Json(AuthProvidersResponse {
        password_registration_enabled: policy.password_registration_enabled,
        password_login_enabled: policy.password_login_enabled,
        ldap_login_enabled: policy.ldap_login_enabled,
        email_placeholder: policy.email_placeholder,
        password_placeholder: policy.password_placeholder,
    }))
}

pub(crate) async fn get_auth_policy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<AuthPolicyDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    Ok(Json(load_auth_policy(&state.pool).await?))
}

pub(crate) async fn update_auth_policy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(policy): Json<AuthPolicyDto>,
) -> Result<Json<AuthPolicyDto>, ApiError> {
    let user = require_administrator(&state, &headers).await?;
    if policy.password_registration_enabled && !policy.password_login_enabled {
        return Err(ApiError::conflict(
            "password registration requires password login",
        ));
    }
    if !policy.password_login_enabled && !policy.ldap_login_enabled {
        return Err(ApiError::conflict(
            "at least one ordinary login method must remain enabled",
        ));
    }
    let mut tx = state.pool.begin().await?;
    require_administrator_role_tx(&mut tx, user.id).await?;
    let current = sqlx::query(
        "SELECT password_login_enabled, ldap_login_enabled
         FROM auth_policy WHERE singleton = true FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    if policy.ldap_login_enabled {
        let configured: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM ldap_configuration WHERE singleton = true)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !configured {
            return Err(ApiError::conflict(
                "LDAP must be configured before LDAP login is enabled",
            ));
        }
    }
    let disables_login_method = (current.get::<bool, _>("password_login_enabled")
        && !policy.password_login_enabled)
        || (current.get::<bool, _>("ldap_login_enabled") && !policy.ldap_login_enabled);
    if disables_login_method {
        let emergency_access_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM users
                 WHERE role = 'super_admin' AND password IS NOT NULL
                   AND deletion_requested_at IS NULL
             )",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !emergency_access_exists {
            return Err(ApiError::conflict(
                "a Super Administrator with a local password is required",
            ));
        }
    }
    let row = sqlx::query(
        "UPDATE auth_policy
         SET password_registration_enabled = $1,
             password_login_enabled = $2,
             ldap_login_enabled = $3,
             email_placeholder = $4,
             password_placeholder = $5,
             updated_by = $6,
             updated_at = now()
         WHERE singleton = true
         RETURNING password_registration_enabled, password_login_enabled,
                   ldap_login_enabled, email_placeholder, password_placeholder",
    )
    .bind(policy.password_registration_enabled)
    .bind(policy.password_login_enabled)
    .bind(policy.ldap_login_enabled)
    .bind(policy.email_placeholder.trim())
    .bind(policy.password_placeholder.trim())
    .bind(user.id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(auth_policy_from_row(row)))
}

pub(crate) async fn get_ldap_configuration(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Option<LdapConfigurationDto>>, ApiError> {
    require_administrator(&state, &headers).await?;
    Ok(Json(load_ldap_configuration(&state.pool).await?))
}

pub(crate) async fn update_ldap_configuration(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(configuration): Json<LdapConfigurationDto>,
) -> Result<Json<LdapConfigurationDto>, ApiError> {
    let administrator = require_administrator(&state, &headers).await?;
    let configuration = validate_ldap_configuration(configuration)?;
    let mut tx = state.pool.begin().await?;
    require_administrator_role_tx(&mut tx, administrator.id).await?;
    sqlx::query(
        "INSERT INTO ldap_configuration
             (singleton, url, security_mode, base_dn, bind_identity_template, user_filter,
              email_attribute, display_name_attribute, allow_insecure,
              skip_tls_verify, updated_by, updated_at)
         VALUES (true, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())
         ON CONFLICT (singleton) DO UPDATE
         SET url = EXCLUDED.url,
             security_mode = EXCLUDED.security_mode,
             base_dn = EXCLUDED.base_dn,
             bind_identity_template = EXCLUDED.bind_identity_template,
             user_filter = EXCLUDED.user_filter,
             email_attribute = EXCLUDED.email_attribute,
             display_name_attribute = EXCLUDED.display_name_attribute,
             allow_insecure = EXCLUDED.allow_insecure,
             skip_tls_verify = EXCLUDED.skip_tls_verify,
             updated_by = EXCLUDED.updated_by,
             updated_at = now()",
    )
    .bind(&configuration.url)
    .bind(ldap_security_mode_name(configuration.security))
    .bind(&configuration.base_dn)
    .bind(&configuration.bind_identity_template)
    .bind(&configuration.user_filter)
    .bind(&configuration.email_attribute)
    .bind(&configuration.display_name_attribute)
    .bind(configuration.allow_insecure)
    .bind(configuration.skip_tls_verify)
    .bind(administrator.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(configuration))
}

pub(crate) async fn test_ldap_configuration(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<TestLdapConfigurationRequest>,
) -> Result<Json<TestLdapConfigurationResponse>, ApiError> {
    require_administrator(&state, &headers).await?;
    let configuration = validate_ldap_configuration(req.configuration)?;
    let rate_email =
        login_rate_email(&req.email).ok_or(ApiError::bad_request("valid email is required"))?;
    reserve_email_login_attempt(&state.pool, &rate_email).await?;
    let request_id = Uuid::new_v4();
    let started_at = Instant::now();
    let identity = match query_ldap_directory(&configuration, &req.email, &req.password).await {
        Ok(identity) => identity,
        Err(error) => {
            warn!(
                request_id = %request_id,
                stage = error.stage,
                category = error.category,
                duration_ms = started_at.elapsed().as_millis(),
                "LDAP configuration test failed"
            );
            return Err(error.for_administrator());
        }
    };
    clear_email_login_failures(&state.pool, &rate_email).await?;
    let display_name = identity
        .display_name
        .unwrap_or_else(|| email_local_part(&identity.email).to_owned());
    Ok(Json(TestLdapConfigurationResponse {
        email: identity.email,
        display_name,
        duration_ms: started_at.elapsed().as_millis().min(i64::MAX as u128) as i64,
    }))
}

pub(crate) async fn list_external_platforms(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<ExternalPlatformDto>>, ApiError> {
    require_administrator(&state, &headers).await?;
    let rows = sqlx::query("SELECT id, key, name FROM external_platforms ORDER BY key")
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(
        rows.into_iter().map(external_platform_from_row).collect(),
    ))
}

pub(crate) async fn create_external_platform(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateExternalPlatformRequest>,
) -> Result<Json<ExternalPlatformDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    let key = validate_identity_key(&req.key, "platform key")?;
    let name = validate_identity_name(&req.name, "platform name")?;
    let row = sqlx::query(
        "INSERT INTO external_platforms (id, key, name)
         VALUES ($1, $2, $3)
         ON CONFLICT (key) DO NOTHING
         RETURNING id, key, name",
    )
    .bind(Uuid::new_v4())
    .bind(key)
    .bind(name)
    .fetch_optional(&state.pool)
    .await?;
    row.map(external_platform_from_row)
        .map(Json)
        .ok_or_else(|| ApiError::conflict("external platform key already exists"))
}

pub(crate) async fn update_external_platform(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(platform_id): Path<Uuid>,
    Json(req): Json<UpdateExternalPlatformRequest>,
) -> Result<Json<ExternalPlatformDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    let name = validate_identity_name(&req.name, "platform name")?;
    let row = sqlx::query(
        "UPDATE external_platforms
         SET name = $1, updated_at = now()
         WHERE id = $2
         RETURNING id, key, name",
    )
    .bind(name)
    .bind(platform_id)
    .fetch_optional(&state.pool)
    .await?;
    row.map(external_platform_from_row)
        .map(Json)
        .ok_or(ApiError::not_found("external platform not found"))
}

pub(crate) async fn list_authentication_channels(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(platform_id): Path<Uuid>,
) -> Result<Json<Vec<AuthenticationChannelDto>>, ApiError> {
    require_administrator(&state, &headers).await?;
    require_external_platform(&state.pool, platform_id).await?;
    let rows = sqlx::query(
        "SELECT id, platform_id, key, name, enabled, trusted_email
         FROM authentication_channels
         WHERE platform_id = $1
         ORDER BY key",
    )
    .bind(platform_id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(authentication_channel_from_row)
            .collect(),
    ))
}

pub(crate) async fn create_authentication_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(platform_id): Path<Uuid>,
    Json(req): Json<CreateAuthenticationChannelRequest>,
) -> Result<Json<AuthenticationChannelDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    require_external_platform(&state.pool, platform_id).await?;
    let key = validate_identity_key(&req.key, "channel key")?;
    let name = validate_identity_name(&req.name, "channel name")?;
    let row = sqlx::query(
        "INSERT INTO authentication_channels
             (id, platform_id, key, name, enabled, trusted_email)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (platform_id, key) DO NOTHING
         RETURNING id, platform_id, key, name, enabled, trusted_email",
    )
    .bind(Uuid::new_v4())
    .bind(platform_id)
    .bind(key)
    .bind(name)
    .bind(req.enabled)
    .bind(req.trusted_email)
    .fetch_optional(&state.pool)
    .await?;
    row.map(authentication_channel_from_row)
        .map(Json)
        .ok_or_else(|| ApiError::conflict("authentication channel key already exists"))
}

pub(crate) async fn update_authentication_channel(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(channel_id): Path<Uuid>,
    Json(req): Json<UpdateAuthenticationChannelRequest>,
) -> Result<Json<AuthenticationChannelDto>, ApiError> {
    require_administrator(&state, &headers).await?;
    let name = validate_identity_name(&req.name, "channel name")?;
    let row = sqlx::query(
        "UPDATE authentication_channels
         SET name = $1, enabled = $2, trusted_email = $3, updated_at = now()
         WHERE id = $4
         RETURNING id, platform_id, key, name, enabled, trusted_email",
    )
    .bind(name)
    .bind(req.enabled)
    .bind(req.trusted_email)
    .bind(channel_id)
    .fetch_optional(&state.pool)
    .await?;
    row.map(authentication_channel_from_row)
        .map(Json)
        .ok_or_else(|| ApiError::not_found("authentication channel not found"))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ApiKeyListQuery {
    pub(crate) page: Option<i64>,
    pub(crate) page_size: Option<i64>,
}

impl ApiKeyListQuery {
    pub(crate) fn validated(self) -> Result<(i64, i64, i64), ApiError> {
        let page = self.page.unwrap_or(1);
        let page_size = self.page_size.unwrap_or(20);
        if page < 1 || !(1..=100).contains(&page_size) {
            return Err(ApiError::bad_request("invalid API key pagination"));
        }
        let offset = page
            .checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .ok_or_else(|| ApiError::bad_request("invalid API key pagination"))?;
        Ok((page, page_size, offset))
    }
}

pub(crate) async fn list_api_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<ApiKeyListQuery>,
) -> Result<Json<ApiKeyListResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let (page, page_size, offset) = query.validated()?;
    let total = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM api_keys WHERE user_id = $1")
        .bind(user.id)
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query(
        "SELECT id, name, prefix, last_used_at, expires_at, created_at
         FROM api_keys
         WHERE user_id = $1
         ORDER BY created_at DESC, id DESC
         LIMIT $2 OFFSET $3",
    )
    .bind(user.id)
    .bind(page_size)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(ApiKeyListResponse {
        items: rows.into_iter().map(api_key_from_row).collect(),
        total,
        page,
        page_size,
    }))
}

pub(crate) async fn create_api_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<Json<CreateApiKeyResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("api key name is required"));
    }
    let token = new_api_key_token();
    let prefix = token.chars().take(12).collect::<String>();
    let token_hash = sha256_hex(&token);
    let expires_at = api_key_expiration(req.validity.as_ref(), Utc::now())?;
    let row = sqlx::query(
        "INSERT INTO api_keys (id, user_id, name, prefix, token_hash, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, name, prefix, last_used_at, expires_at, created_at",
    )
    .bind(Uuid::new_v4())
    .bind(user.id)
    .bind(req.name.trim())
    .bind(prefix)
    .bind(token_hash)
    .bind(expires_at)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(CreateApiKeyResponse {
        api_key: api_key_from_row(row),
        token,
    }))
}

pub(crate) fn new_api_key_token() -> String {
    format!("ahk_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub(crate) async fn renew_api_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(api_key_id): Path<Uuid>,
    Json(req): Json<RenewApiKeyRequest>,
) -> Result<Json<ApiKeyDto>, ApiError> {
    let (user, credential_api_key_id) = require_user_with_api_key_id(&state, &headers).await?;
    if credential_api_key_id == Some(api_key_id) {
        return Err(ApiError::not_found("api key not found"));
    }
    let mut tx = state.pool.begin().await?;
    let current_expiration = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
        "SELECT expires_at FROM api_keys
         WHERE id = $1 AND user_id = $2
         FOR UPDATE",
    )
    .bind(api_key_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("api key not found"))?;
    let expires_at = renewed_api_key_expiration(&req.validity, current_expiration, Utc::now())?;
    let row = sqlx::query(
        "UPDATE api_keys
         SET expires_at = $1
         WHERE id = $2 AND user_id = $3
         RETURNING id, name, prefix, last_used_at, expires_at, created_at",
    )
    .bind(expires_at)
    .bind(api_key_id)
    .bind(user.id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(api_key_from_row(row)))
}

pub(crate) fn api_key_expiration(
    validity: Option<&ApiKeyValidity>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    match validity.unwrap_or(&ApiKeyValidity::Days { days: 90 }) {
        ApiKeyValidity::Days { days } if matches!(days, 30 | 90 | 180 | 365) => {
            Ok(Some(now + ChronoDuration::days(i64::from(*days))))
        }
        ApiKeyValidity::Days { .. } => Err(ApiError::bad_request(
            "api key validity days must be 30, 90, 180, or 365",
        )),
        ApiKeyValidity::Date { expires_at } if *expires_at > now => Ok(Some(*expires_at)),
        ApiKeyValidity::Date { .. } => Err(ApiError::bad_request(
            "api key expiration must be in the future",
        )),
        ApiKeyValidity::Never => Ok(None),
    }
}

pub(crate) fn renewed_api_key_expiration(
    validity: &ApiKeyValidity,
    current_expiration: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let current_expiration = current_expiration.ok_or(ApiError::bad_request(
        "permanent api keys cannot be renewed",
    ))?;
    let requested = api_key_expiration(Some(validity), now)?;
    if requested.is_some_and(|expires_at| expires_at <= current_expiration) {
        return Err(ApiError::bad_request(
            "api key renewal must extend its expiration",
        ));
    }
    Ok(requested)
}

pub(crate) async fn delete_api_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(api_key_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let (user, credential_api_key_id) = require_user_with_api_key_id(&state, &headers).await?;
    if credential_api_key_id == Some(api_key_id) {
        return Err(ApiError::not_found("api key not found"));
    }
    let deleted = sqlx::query("DELETE FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(api_key_id)
        .bind(user.id)
        .execute(&state.pool)
        .await?;
    if deleted.rows_affected() == 0 {
        return Err(ApiError::not_found("api key not found"));
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn load_auth_policy(pool: &PgPool) -> Result<AuthPolicyDto, ApiError> {
    let row = sqlx::query(
        "SELECT password_registration_enabled, password_login_enabled,
                ldap_login_enabled, email_placeholder, password_placeholder
         FROM auth_policy WHERE singleton = true",
    )
    .fetch_one(pool)
    .await?;
    Ok(auth_policy_from_row(row))
}

#[derive(Debug)]
pub(crate) struct LdapDirectoryIdentity {
    pub(crate) email: String,
    pub(crate) display_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct LdapDirectoryFailure {
    pub(crate) stage: &'static str,
    pub(crate) category: &'static str,
    pub(crate) diagnostic: &'static str,
    pub(crate) unavailable: bool,
}

impl LdapDirectoryFailure {
    pub(crate) fn invalid(
        stage: &'static str,
        category: &'static str,
        diagnostic: &'static str,
    ) -> Self {
        Self {
            stage,
            category,
            diagnostic,
            unavailable: false,
        }
    }

    pub(crate) fn unavailable(
        stage: &'static str,
        category: &'static str,
        diagnostic: &'static str,
    ) -> Self {
        Self {
            stage,
            category,
            diagnostic,
            unavailable: true,
        }
    }

    pub(crate) fn for_login(self) -> ApiError {
        if self.unavailable {
            ApiError::service_unavailable("LDAP service is temporarily unavailable")
        } else {
            ApiError::unauthorized("invalid email or password")
        }
    }

    pub(crate) fn for_administrator(self) -> ApiError {
        let message = format!("LDAP {} failed: {}", self.stage, self.diagnostic);
        if self.unavailable {
            ApiError::service_unavailable(message)
        } else {
            ApiError::bad_request(message)
        }
    }
}

pub(crate) async fn load_ldap_configuration(
    pool: &PgPool,
) -> Result<Option<LdapConfigurationDto>, ApiError> {
    let row = sqlx::query(
        "SELECT url, security_mode, base_dn, bind_identity_template, user_filter, email_attribute,
                display_name_attribute, allow_insecure, skip_tls_verify
         FROM ldap_configuration WHERE singleton = true",
    )
    .fetch_optional(pool)
    .await?;
    row.map(ldap_configuration_from_row).transpose()
}

pub(crate) fn ldap_configuration_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<LdapConfigurationDto, ApiError> {
    let security = match row.get::<String, _>("security_mode").as_str() {
        "ldaps" => LdapSecurityMode::Ldaps,
        "starttls" => LdapSecurityMode::Starttls,
        "plain" => LdapSecurityMode::Plain,
        _ => return Err(ApiError::internal("stored LDAP security mode is invalid")),
    };
    Ok(LdapConfigurationDto {
        url: row.get("url"),
        security,
        base_dn: row.get("base_dn"),
        bind_identity_template: row.get("bind_identity_template"),
        user_filter: row.get("user_filter"),
        email_attribute: row.get("email_attribute"),
        display_name_attribute: row.get("display_name_attribute"),
        allow_insecure: row.get("allow_insecure"),
        skip_tls_verify: row.get("skip_tls_verify"),
    })
}

pub(crate) fn ldap_security_mode_name(mode: LdapSecurityMode) -> &'static str {
    match mode {
        LdapSecurityMode::Ldaps => "ldaps",
        LdapSecurityMode::Starttls => "starttls",
        LdapSecurityMode::Plain => "plain",
    }
}

pub(crate) fn validate_ldap_configuration(
    mut configuration: LdapConfigurationDto,
) -> Result<LdapConfigurationDto, ApiError> {
    configuration.url = configuration.url.trim().to_owned();
    configuration.base_dn = configuration.base_dn.trim().to_owned();
    configuration.bind_identity_template = configuration.bind_identity_template.trim().to_owned();
    configuration.user_filter = configuration.user_filter.trim().to_owned();
    configuration.email_attribute = configuration.email_attribute.trim().to_owned();
    configuration.display_name_attribute = configuration.display_name_attribute.trim().to_owned();
    let url =
        Url::parse(&configuration.url).map_err(|_| ApiError::bad_request("LDAP URL is invalid"))?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !(url.path().is_empty() || url.path() == "/")
    {
        return Err(ApiError::bad_request(
            "LDAP URL must contain only a server address without credentials",
        ));
    }
    let expected_scheme = match configuration.security {
        LdapSecurityMode::Ldaps => "ldaps",
        LdapSecurityMode::Starttls | LdapSecurityMode::Plain => "ldap",
    };
    if url.scheme() != expected_scheme {
        return Err(ApiError::bad_request(
            "LDAP URL scheme does not match the security mode",
        ));
    }
    if configuration.security == LdapSecurityMode::Plain && !configuration.allow_insecure {
        return Err(ApiError::bad_request(
            "plain LDAP requires explicit insecure transport approval",
        ));
    }
    if configuration.security == LdapSecurityMode::Plain && configuration.skip_tls_verify {
        return Err(ApiError::bad_request(
            "TLS verification can only be skipped for TLS connections",
        ));
    }
    if configuration.base_dn.is_empty()
        || configuration.base_dn.len() > 4096
        || configuration.base_dn.chars().any(char::is_control)
    {
        return Err(ApiError::bad_request("valid LDAP Base DN is required"));
    }
    if configuration.bind_identity_template.len() > 4096
        || configuration
            .bind_identity_template
            .matches("{email}")
            .count()
            != 1
        || configuration
            .bind_identity_template
            .chars()
            .any(char::is_control)
    {
        return Err(ApiError::bad_request(
            "LDAP Bind identity template must contain exactly one {email} placeholder",
        ));
    }
    if configuration.user_filter.len() > 4096
        || configuration.user_filter.matches("{email}").count() != 1
        || configuration.user_filter.chars().any(char::is_control)
    {
        return Err(ApiError::bad_request(
            "LDAP user filter must contain exactly one {email} placeholder",
        ));
    }
    validate_ldap_attribute_name(&configuration.email_attribute, "email attribute")?;
    validate_ldap_attribute_name(
        &configuration.display_name_attribute,
        "display name attribute",
    )?;
    Ok(configuration)
}

pub(crate) fn validate_ldap_attribute_name(value: &str, field: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > 128
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '.' | '_')
        })
    {
        return Err(ApiError::bad_request(format!(
            "valid LDAP {field} is required"
        )));
    }
    Ok(())
}

pub(crate) async fn query_ldap_directory(
    configuration: &LdapConfigurationDto,
    email: &str,
    password: &str,
) -> Result<LdapDirectoryIdentity, LdapDirectoryFailure> {
    match tokio::time::timeout(
        Duration::from_secs(10),
        query_ldap_directory_once(configuration, email, password),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(LdapDirectoryFailure::unavailable(
            "timeout",
            "deadline_exceeded",
            "the ten-second LDAP deadline was exceeded",
        )),
    }
}

pub(crate) async fn query_ldap_directory_once(
    configuration: &LdapConfigurationDto,
    email: &str,
    password: &str,
) -> Result<LdapDirectoryIdentity, LdapDirectoryFailure> {
    let bind_email = normalize_email(email).map_err(|_| {
        LdapDirectoryFailure::invalid("bind", "invalid_credentials", "email is invalid")
    })?;
    if password.is_empty() {
        return Err(LdapDirectoryFailure::invalid(
            "bind",
            "invalid_credentials",
            "password is empty",
        ));
    }
    let settings = LdapConnSettings::new()
        .set_conn_timeout(Duration::from_secs(5))
        .set_starttls(configuration.security == LdapSecurityMode::Starttls)
        .set_no_tls_verify(configuration.skip_tls_verify);
    let (connection, mut ldap) = LdapConnAsync::with_settings(settings, &configuration.url)
        .await
        .map_err(|_| {
            LdapDirectoryFailure::unavailable(
                "connect",
                "connection_failed",
                "connection or TLS negotiation failed",
            )
        })?;
    ldap3::drive!(connection);
    ldap.with_timeout(Duration::from_secs(5));
    let bind_identity = ldap_bind_identity(&configuration.bind_identity_template, &bind_email);
    let bind_result = ldap
        .simple_bind(&bind_identity, password)
        .await
        .map_err(|error| classify_ldap_bind_error(&error))?;
    bind_result
        .success()
        .map_err(|error| classify_ldap_bind_error(&error))?;

    let filter = ldap_user_filter(&configuration.user_filter, &bind_email);
    let search = ldap
        .search(
            &configuration.base_dn,
            Scope::Subtree,
            &filter,
            vec![
                configuration.email_attribute.as_str(),
                configuration.display_name_attribute.as_str(),
            ],
        )
        .await
        .map_err(|_| {
            LdapDirectoryFailure::unavailable("search", "search_failed", "directory search failed")
        })?;
    let (entries, _) = search.success().map_err(|_| {
        LdapDirectoryFailure::unavailable(
            "search",
            "search_failed",
            "directory search was rejected",
        )
    })?;
    let _ = ldap.unbind().await;
    if entries.len() != 1 {
        return Err(LdapDirectoryFailure::invalid(
            "search",
            "result_cardinality",
            "directory search did not return exactly one entry",
        ));
    }
    let entry = SearchEntry::construct(entries.into_iter().next().expect("one LDAP entry"));
    let email_values = ldap_attribute_values(&entry, &configuration.email_attribute)
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if email_values.len() != 1 {
        return Err(LdapDirectoryFailure::invalid(
            "mapping",
            "email_attribute",
            "email attribute must contain exactly one value",
        ));
    }
    let authoritative_email = normalize_email(email_values[0]).map_err(|_| {
        LdapDirectoryFailure::invalid(
            "mapping",
            "email_attribute",
            "email attribute is not a valid email address",
        )
    })?;
    let display_name = ldap_attribute_values(&entry, &configuration.display_name_attribute)
        .and_then(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .find(|value| !value.is_empty())
        })
        .map(|value| normalize_display_name(Some(value), &authoritative_email))
        .transpose()
        .map_err(|_| {
            LdapDirectoryFailure::invalid(
                "mapping",
                "display_name_attribute",
                "display name attribute is invalid",
            )
        })?;
    Ok(LdapDirectoryIdentity {
        email: authoritative_email,
        display_name,
    })
}

pub(crate) fn ldap_user_filter(template: &str, email: &str) -> String {
    let escaped_email = ldap_escape(email);
    template.replacen("{email}", escaped_email.as_ref(), 1)
}

pub(crate) fn ldap_bind_identity(template: &str, email: &str) -> String {
    let escaped_email = dn_escape(email);
    template.replacen("{email}", escaped_email.as_ref(), 1)
}

pub(crate) fn classify_ldap_bind_error(error: &LdapError) -> LdapDirectoryFailure {
    if matches!(error, LdapError::LdapResult { result } if matches!(result.rc, 32 | 34 | 49)) {
        LdapDirectoryFailure::invalid(
            "bind",
            "invalid_credentials",
            "directory rejected the credentials",
        )
    } else {
        LdapDirectoryFailure::unavailable("bind", "bind_failed", "LDAP Bind failed")
    }
}

pub(crate) fn ldap_attribute_values<'a>(
    entry: &'a SearchEntry,
    name: &str,
) -> Option<&'a Vec<String>> {
    entry
        .attrs
        .iter()
        .find(|(attribute, _)| attribute.eq_ignore_ascii_case(name))
        .map(|(_, values)| values)
}

pub(crate) async fn resolve_ldap_user(
    pool: &PgPool,
    identity: &LdapDirectoryIdentity,
) -> Result<UserDto, ApiError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query(
        "SELECT id, email, display_name, role, deletion_requested_at
         FROM users WHERE lower(btrim(email)) = lower(btrim($1))
         FOR UPDATE",
    )
    .bind(&identity.email)
    .fetch_optional(&mut *tx)
    .await?;
    let user = match row {
        Some(row)
            if row
                .get::<Option<DateTime<Utc>>, _>("deletion_requested_at")
                .is_some() =>
        {
            return Err(ApiError::unauthorized("invalid email or password"));
        }
        Some(row) => {
            if let Some(display_name) = identity.display_name.as_deref() {
                let row = sqlx::query(
                    "UPDATE users SET display_name = $1 WHERE id = $2
                     RETURNING id, email, display_name, role",
                )
                .bind(display_name)
                .bind(row.get::<Uuid, _>("id"))
                .fetch_one(&mut *tx)
                .await?;
                user_from_row(row)
            } else {
                user_from_row(row)
            }
        }
        None => {
            create_hub_user_in_locked_tx(
                &mut tx,
                &identity.email,
                identity.display_name.as_deref(),
                None,
            )
            .await?
        }
    };
    tx.commit().await?;
    Ok(user)
}

pub(crate) fn email_local_part(email: &str) -> &str {
    email
        .split_once('@')
        .map(|(local, _)| local)
        .unwrap_or(email)
}

pub(crate) async fn require_external_platform(
    pool: &PgPool,
    platform_id: Uuid,
) -> Result<(), ApiError> {
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM external_platforms WHERE id = $1)")
            .bind(platform_id)
            .fetch_one(pool)
            .await?;
    if !exists {
        return Err(ApiError::not_found("external platform not found"));
    }
    Ok(())
}

pub(crate) fn validate_external_username(value: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request("valid external username is required"));
    }
    Ok(Some(value.to_owned()))
}

pub(crate) struct ResolvedExternalIdentity {
    pub(crate) user: UserDto,
    pub(crate) identity_id: Uuid,
}

#[cfg(test)]
pub(crate) async fn resolve_external_identity(
    pool: &PgPool,
    platform_id: Uuid,
    channel_id: Uuid,
    tenant_id: &str,
    external_user_id: &str,
    email: Option<&str>,
    external_username: Option<&str>,
) -> Result<UserDto, ApiError> {
    let mut tx = pool.begin().await?;
    let resolved = resolve_external_identity_tx(
        &mut tx,
        platform_id,
        channel_id,
        tenant_id,
        external_user_id,
        email,
        external_username,
    )
    .await?;
    tx.commit().await?;
    Ok(resolved.user)
}

pub(crate) async fn resolve_external_identity_tx(
    tx: &mut Transaction<'_, Postgres>,
    platform_id: Uuid,
    channel_id: Uuid,
    tenant_id: &str,
    external_user_id: &str,
    email: Option<&str>,
    external_username: Option<&str>,
) -> Result<ResolvedExternalIdentity, ApiError> {
    let tenant_id = normalize_origin_tenant(Some(tenant_id))?;
    let external_user_id = external_user_id.trim();
    if external_user_id.is_empty()
        || external_user_id.len() > 128
        || external_user_id.chars().any(char::is_control)
    {
        return Err(ApiError::bad_request("valid external user id is required"));
    }
    let email = email.map(normalize_email).transpose()?;
    let external_username = validate_external_username(external_username)?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut **tx)
        .await?;
    let channel_exists: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM authentication_channels
         WHERE id = $1 AND platform_id = $2
           AND enabled = true AND trusted_email = true
         FOR SHARE",
    )
    .bind(channel_id)
    .bind(platform_id)
    .fetch_optional(&mut **tx)
    .await?;
    if channel_exists.is_none() {
        return Err(ApiError::forbidden("authentication channel is unavailable"));
    }

    let existing = sqlx::query(
        "SELECT i.id AS identity_id, u.id, u.email, u.display_name, u.role,
                u.deletion_requested_at
         FROM external_identities i
         JOIN users u ON u.id = i.user_id
         WHERE i.platform_id = $1 AND i.tenant_id = $2 AND i.external_user_id = $3
         FOR UPDATE OF i, u",
    )
    .bind(platform_id)
    .bind(&tenant_id)
    .bind(external_user_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = existing {
        if row
            .get::<Option<DateTime<Utc>>, _>("deletion_requested_at")
            .is_some()
        {
            return Err(ApiError::forbidden("user account is unavailable"));
        }
        let identity_id = row.get("identity_id");
        sqlx::query(
            "UPDATE external_identities
             SET authentication_channel_id = $1,
                 last_email = COALESCE($2, last_email),
                 last_username = COALESCE($3, last_username), updated_at = now()
             WHERE platform_id = $4 AND tenant_id = $5 AND external_user_id = $6",
        )
        .bind(channel_id)
        .bind(email.as_deref())
        .bind(external_username.as_deref())
        .bind(platform_id)
        .bind(&tenant_id)
        .bind(external_user_id)
        .execute(&mut **tx)
        .await?;
        return Ok(ResolvedExternalIdentity {
            user: user_from_row(row),
            identity_id,
        });
    }

    let matched_user = if let Some(email) = email.as_deref() {
        let row = sqlx::query(
            "SELECT id, email, display_name, role, deletion_requested_at
             FROM users
             WHERE lower(btrim(email)) = lower(btrim($1))
             FOR UPDATE",
        )
        .bind(email)
        .fetch_optional(&mut **tx)
        .await?;
        match row {
            Some(row)
                if row
                    .get::<Option<DateTime<Utc>>, _>("deletion_requested_at")
                    .is_some() =>
            {
                return Err(ApiError::forbidden("user account is unavailable"));
            }
            Some(row) => Some(user_from_row(row)),
            None => None,
        }
    } else {
        None
    };
    let user = match matched_user {
        Some(user) => user,
        None => {
            let email = email
                .as_deref()
                .ok_or(ApiError::bad_request("trusted email is required"))?;
            create_hub_user_in_locked_tx(tx, email, None, None).await?
        }
    };
    let identity_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO external_identities
             (id, platform_id, tenant_id, external_user_id, user_id,
              authentication_channel_id, last_email, last_username)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(identity_id)
    .bind(platform_id)
    .bind(&tenant_id)
    .bind(external_user_id)
    .bind(user.id)
    .bind(channel_id)
    .bind(email.as_deref())
    .bind(external_username.as_deref())
    .execute(&mut **tx)
    .await?;
    Ok(ResolvedExternalIdentity { user, identity_id })
}

pub(crate) async fn update_external_identity_widget_profile_tx(
    tx: &mut Transaction<'_, Postgres>,
    identity_id: Uuid,
    profile: &WidgetUserProfileDto,
) -> Result<(), ApiError> {
    let updated = sqlx::query(
        "UPDATE external_identities
         SET last_email = COALESCE($1, last_email),
             last_username = COALESCE($2, last_username),
             last_display_name = COALESCE($3, last_display_name),
             attributes = $4, updated_at = now()
         WHERE id = $5",
    )
    .bind(profile.email.as_deref())
    .bind(profile.username.as_deref())
    .bind(profile.display_name.as_deref())
    .bind(&profile.attributes)
    .bind(identity_id)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::unauthorized("external identity is unavailable"));
    }
    Ok(())
}

pub(crate) async fn create_password_registration_user(
    pool: &PgPool,
    email: &str,
    display_name: Option<&str>,
    password: Option<&str>,
) -> Result<UserDto, ApiError> {
    let email = normalize_email(email)?;
    let display_name = normalize_display_name(display_name, &email)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
    let registration_enabled: bool = sqlx::query_scalar(
        "SELECT password_registration_enabled
         FROM auth_policy WHERE singleton = true FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    if !registration_enabled {
        return Err(ApiError::forbidden("password registration is disabled"));
    }
    let email_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM users WHERE lower(btrim(email)) = lower(btrim($1))
         )",
    )
    .bind(&email)
    .fetch_one(&mut *tx)
    .await?;
    if email_exists {
        return Err(ApiError::conflict("email already exists"));
    }
    let user = create_hub_user_in_locked_tx(&mut tx, &email, Some(&display_name), password).await?;
    tx.commit().await?;
    Ok(user)
}

#[cfg(test)]
pub(crate) async fn create_hub_user(
    pool: &PgPool,
    email: Option<&str>,
    display_name: Option<&str>,
    password: Option<&str>,
    _test_identity_is_trusted: bool,
) -> Result<UserDto, ApiError> {
    let email = email.ok_or(ApiError::bad_request("trusted email is required"))?;
    let email = normalize_email(email)?;
    let display_name = normalize_display_name(display_name, &email)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
        .execute(&mut *tx)
        .await?;
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
    let user = create_hub_user_in_locked_tx(&mut tx, &email, Some(&display_name), password).await?;
    tx.commit().await?;
    Ok(user)
}

pub(crate) async fn create_hub_user_in_locked_tx(
    tx: &mut Transaction<'_, Postgres>,
    email: &str,
    display_name: Option<&str>,
    password: Option<&str>,
) -> Result<UserDto, ApiError> {
    let email = normalize_email(email)?;
    let display_name = normalize_display_name(display_name, &email)?;
    let user_id = Uuid::new_v4();
    let first_user: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM users)")
        .fetch_one(&mut **tx)
        .await?;
    let role = if first_user { "super_admin" } else { "member" };
    sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(user_id)
    .bind(&email)
    .bind(password)
    .bind(&display_name)
    .bind(role)
    .execute(&mut **tx)
    .await?;
    if first_user {
        sqlx::query(
            "UPDATE auth_policy
             SET password_registration_enabled = false, updated_at = now()
             WHERE singleton = true",
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(UserDto {
        id: user_id,
        email,
        display_name,
        role: role.to_owned(),
    })
}

pub(crate) fn normalize_display_name(value: Option<&str>, email: &str) -> Result<String, ApiError> {
    let fallback = email
        .split_once('@')
        .map(|(local, _)| local)
        .unwrap_or(email);
    let display_name = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback);
    if display_name.len() > 128 || display_name.chars().any(char::is_control) {
        return Err(ApiError::bad_request("valid display name is required"));
    }
    Ok(display_name.to_owned())
}

pub(crate) fn validate_user_role(value: &str) -> Result<&'static str, ApiError> {
    match value.trim() {
        "member" => Ok("member"),
        "admin" => Ok("admin"),
        "super_admin" => Ok("super_admin"),
        _ => Err(ApiError::bad_request("unsupported user role")),
    }
}

pub(crate) fn normalize_email(email: &str) -> Result<String, ApiError> {
    let email = email.trim().to_ascii_lowercase();
    let valid = email.len() <= 254
        && !email.chars().any(char::is_whitespace)
        && email
            .split_once('@')
            .is_some_and(|(local, domain)| !local.is_empty() && domain.contains('.'));
    if !valid {
        return Err(ApiError::bad_request("valid email is required"));
    }
    Ok(email)
}

pub(crate) fn login_rate_email(email: &str) -> Option<String> {
    let email = email.trim().to_ascii_lowercase();
    (!email.is_empty() && email.len() <= 254 && !email.chars().any(char::is_whitespace))
        .then_some(email)
}

pub(crate) fn login_source_ip(
    headers: &HeaderMap,
    peer_ip: Option<IpAddr>,
    trusted_proxy_cidrs: Option<&[IpNet]>,
) -> IpAddr {
    let trust_forwarded = match trusted_proxy_cidrs {
        None => true,
        Some(cidrs) => peer_ip.is_some_and(|ip| cidrs.iter().any(|cidr| cidr.contains(&ip))),
    };
    if trust_forwarded {
        if let Some(ip) = forwarded_client_ip(headers) {
            return ip;
        }
    }
    peer_ip.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

pub(crate) fn forwarded_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(header::FORWARDED)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_forwarded_header)
        .or_else(|| {
            headers
                .get("x-forwarded-for")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .and_then(parse_forwarded_ip)
        })
}

pub(crate) fn parse_forwarded_header(value: &str) -> Option<IpAddr> {
    value
        .split(',')
        .next()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find(|(name, _)| name.eq_ignore_ascii_case("for"))
        .and_then(|(_, value)| parse_forwarded_ip(value))
}

pub(crate) fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    let value = value.trim().trim_matches('"');
    if value.eq_ignore_ascii_case("unknown") || value.starts_with('_') {
        return None;
    }
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Some(address.ip());
    }
    if let Some(rest) = value.strip_prefix('[') {
        let end = rest.find(']')?;
        return rest[..end].parse().ok();
    }
    None
}

pub(crate) async fn record_ip_login_attempt(
    pool: &PgPool,
    source_ip: IpAddr,
) -> Result<(), ApiError> {
    cleanup_expired_login_throttles(pool).await?;
    let row = sqlx::query(
        "INSERT INTO login_ip_attempts
             (source_ip, attempts, window_started_at, updated_at)
         VALUES ($1::inet, 1, now(), now())
         ON CONFLICT (source_ip) DO UPDATE
         SET attempts = CASE
                 WHEN login_ip_attempts.window_started_at <= now() - interval '5 minutes'
                 THEN 1 ELSE login_ip_attempts.attempts + 1 END,
             window_started_at = CASE
                 WHEN login_ip_attempts.window_started_at <= now() - interval '5 minutes'
                 THEN now() ELSE login_ip_attempts.window_started_at END,
             updated_at = now()
         RETURNING attempts, window_started_at",
    )
    .bind(source_ip.to_string())
    .fetch_one(pool)
    .await?;
    let attempts: i32 = row.get("attempts");
    if attempts > 20 {
        return Err(ApiError::too_many_requests(
            "too many login attempts",
            retry_after_seconds(row.get("window_started_at")),
        ));
    }
    Ok(())
}

pub(crate) async fn reserve_email_login_attempt(
    pool: &PgPool,
    email: &str,
) -> Result<(), ApiError> {
    cleanup_expired_login_throttles(pool).await?;
    // Reserve before credential verification so concurrent requests cannot all pass the limit check.
    let row = sqlx::query(
        "INSERT INTO login_email_failures
             (normalized_email, failed_attempts, window_started_at, updated_at)
         VALUES ($1, 1, now(), now())
         ON CONFLICT (normalized_email) DO UPDATE
         SET failed_attempts = CASE
                 WHEN login_email_failures.window_started_at <= now() - interval '5 minutes'
                 THEN 1 ELSE LEAST(login_email_failures.failed_attempts, 3) + 1 END,
             window_started_at = CASE
                 WHEN login_email_failures.window_started_at <= now() - interval '5 minutes'
                 THEN now() ELSE login_email_failures.window_started_at END,
             updated_at = now()
         RETURNING failed_attempts, window_started_at",
    )
    .bind(email)
    .fetch_one(pool)
    .await?;
    if row.get::<i32, _>("failed_attempts") > 3 {
        return Err(ApiError::too_many_requests(
            "too many failed login attempts",
            retry_after_seconds(row.get("window_started_at")),
        ));
    }
    Ok(())
}

pub(crate) async fn clear_email_login_failures(pool: &PgPool, email: &str) -> Result<(), ApiError> {
    sqlx::query("DELETE FROM login_email_failures WHERE normalized_email = $1")
        .bind(email.trim().to_ascii_lowercase())
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn cleanup_expired_login_throttles(pool: &PgPool) -> Result<(), ApiError> {
    sqlx::query(
        "DELETE FROM login_email_failures
         WHERE ctid IN (
             SELECT ctid FROM login_email_failures
             WHERE window_started_at <= now() - interval '1 hour'
             ORDER BY window_started_at LIMIT 100
         )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "DELETE FROM login_ip_attempts
         WHERE ctid IN (
             SELECT ctid FROM login_ip_attempts
             WHERE window_started_at <= now() - interval '1 hour'
             ORDER BY window_started_at LIMIT 100
         )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) fn retry_after_seconds(window_started_at: DateTime<Utc>) -> u64 {
    (window_started_at + ChronoDuration::minutes(5) - Utc::now())
        .num_seconds()
        .max(1) as u64
}

pub(crate) fn validate_identity_key(value: &str, field: &str) -> Result<String, ApiError> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 64
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(ApiError::bad_request(format!("valid {field} is required")));
    }
    Ok(value)
}

pub(crate) fn validate_identity_name(value: &str, field: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request(format!("valid {field} is required")));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use crate::{build_router, openapi_document};
    use axum::body::Body;
    use axum::http::{HeaderName, Method};
    use axum::Router;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicU64, Ordering};
    use tower::ServiceExt;
    #[test]
    fn api_key_openapi_documents_expiration_renewal_and_delete_only() {
        let document = openapi_document();
        assert!(document["paths"]
            .get("/api/auth/api-keys/{api_key_id}/revoke")
            .is_none());
        let renew = &document["paths"]["/api/auth/api-keys/{api_key_id}/renew"]["post"];
        assert_eq!(
            renew["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RenewApiKeyRequest"
        );
        assert_eq!(
            renew["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ApiKey"
        );
        let api_key = &document["components"]["schemas"]["ApiKey"];
        assert!(api_key["properties"].get("revoked_at").is_none());
        assert!(api_key["properties"].get("expires_at").is_some());
        assert!(document["components"]["schemas"]
            .get("ApiKeyValidity")
            .is_some());
    }

    #[test]
    fn api_key_hash_is_stable_and_not_plaintext() {
        let token = "ahk_test_token";

        assert_eq!(sha256_hex(token), sha256_hex(token));
        assert_ne!(sha256_hex(token), token);
    }

    #[test]
    fn password_hash_verifies_and_hides_plaintext() {
        let hash = password_hash("admin123").unwrap();

        assert!(verify_password(&hash, "admin123"));
        assert!(!verify_password(&hash, "wrong"));
        assert!(!hash.contains("admin123"));
        assert!(hash.starts_with("$argon2id$"));
        assert!(!password_needs_upgrade(&hash));
        assert!(password_needs_upgrade("admin123"));
    }

    #[test]
    fn ldap_configuration_and_forwarded_source_are_validated() {
        let configuration = LdapConfigurationDto {
            url: "ldap://directory.example:389".into(),
            security: LdapSecurityMode::Starttls,
            base_dn: "OU=People,DC=example,DC=com".into(),
            bind_identity_template: "uid={email},OU=People,DC=example,DC=com".into(),
            user_filter: "(userPrincipalName={email})".into(),
            email_attribute: "mail".into(),
            display_name_attribute: "displayName".into(),
            allow_insecure: false,
            skip_tls_verify: false,
        };
        assert!(validate_ldap_configuration(configuration.clone()).is_ok());
        assert!(validate_ldap_configuration(LdapConfigurationDto {
            bind_identity_template: "uid=missing,OU=People,DC=example,DC=com".into(),
            ..configuration.clone()
        })
        .is_err());
        assert!(validate_ldap_configuration(LdapConfigurationDto {
            bind_identity_template: "uid={email},CN={email},DC=example,DC=com".into(),
            ..configuration.clone()
        })
        .is_err());
        assert!(validate_ldap_configuration(LdapConfigurationDto {
            security: LdapSecurityMode::Plain,
            ..configuration
        })
        .is_err());
        assert_eq!(
            ldap_bind_identity(
                "uid={email},OU=People,DC=example,DC=com",
                "special+user@example.com",
            ),
            r"uid=special\2buser@example.com,OU=People,DC=example,DC=com"
        );
        assert_eq!(
            ldap_user_filter(
                "(userPrincipalName={email})",
                "special*(user)\\name@example.com",
            ),
            r"(userPrincipalName=special\2a\28user\29\5cname@example.com)"
        );
        let invalid = LdapDirectoryFailure::invalid(
            "search",
            "result_cardinality",
            "directory search did not return exactly one entry",
        )
        .for_login();
        assert_eq!(invalid.status, StatusCode::UNAUTHORIZED);
        assert_eq!(invalid.message, "invalid email or password");
        let unavailable =
            LdapDirectoryFailure::unavailable("connect", "connection_failed", "connection failed")
                .for_login();
        assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            unavailable.message,
            "LDAP service is temporarily unavailable"
        );
        for rc in [32, 34, 49] {
            let error = LdapError::LdapResult {
                result: ldap3::result::LdapResult {
                    rc,
                    matched: String::new(),
                    text: String::new(),
                    refs: Vec::new(),
                    ctrls: Vec::new(),
                },
            };
            let classified = classify_ldap_bind_error(&error);
            assert!(!classified.unavailable);
            assert_eq!(classified.category, "invalid_credentials");
        }
        assert_eq!(
            parse_forwarded_header("for=192.0.2.10;proto=https"),
            Some("192.0.2.10".parse().unwrap())
        );
        assert_eq!(
            parse_forwarded_header("for=\"[2001:db8::1]:443\""),
            Some("2001:db8::1".parse().unwrap())
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::FORWARDED,
            HeaderValue::from_static("for=198.51.100.10;proto=https"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-for"),
            HeaderValue::from_static("198.51.100.11, 10.0.0.1"),
        );
        let peer: IpAddr = "192.0.2.20".parse().unwrap();
        assert_eq!(
            login_source_ip(&headers, Some(peer), None),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            login_source_ip(
                &headers,
                Some(peer),
                Some(&["192.0.2.0/24".parse().unwrap()]),
            ),
            "198.51.100.10".parse::<IpAddr>().unwrap()
        );
        assert_eq!(
            login_source_ip(
                &headers,
                Some(peer),
                Some(&["203.0.113.0/24".parse().unwrap()]),
            ),
            peer
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn user_erasure_requires_exact_administrator_confirmation_and_freezes_immediately(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claimed = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let (target_id, target_email): (Uuid, String) = sqlx::query_as(
            "SELECT users.id, users.email
             FROM users
             JOIN hub_sessions ON hub_sessions.owner_id = users.id
             WHERE hub_sessions.id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let target_session = format!("ahs_target_{}", Uuid::new_v4().simple());
        let target_api_key = format!("ahk_target_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&target_session))
        .bind(target_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO api_keys (id, user_id, name, prefix, token_hash)
             VALUES ($1, $2, 'erasure test', 'ahk_target', $3)",
        )
        .bind(Uuid::new_v4())
        .bind(target_id)
        .bind(sha256_hex(&target_api_key))
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let member_token = create_user_session_with_role(&fixture.state.pool, "member").await;
        let admin_token = create_user_session_with_role(&fixture.state.pool, "admin").await;
        let super_token = create_super_admin_session(&fixture.state.pool).await;
        let state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let member_error = erase_user(
            State(state.clone()),
            session_headers(&member_token),
            Path(target_id),
            Json(EraseUserRequest {
                email: target_email.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(member_error.status, StatusCode::FORBIDDEN);
        let admin_confirmation = erase_user(
            State(state.clone()),
            session_headers(&admin_token),
            Path(target_id),
            Json(EraseUserRequest {
                email: "wrong@example.com".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(admin_confirmation.status, StatusCode::CONFLICT);
        for confirmation in ["wrong@example.com".to_owned(), target_email.to_uppercase()] {
            let error = erase_user(
                State(state.clone()),
                session_headers(&super_token),
                Path(target_id),
                Json(EraseUserRequest {
                    email: confirmation,
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status, StatusCode::CONFLICT);
        }

        for _ in 0..2 {
            let (status, erasure) = erase_user(
                State(state.clone()),
                session_headers(&super_token),
                Path(target_id),
                Json(EraseUserRequest {
                    email: target_email.clone(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(status, StatusCode::ACCEPTED);
            assert_eq!(erasure.status, "pending");
        }

        assert!(load_user_by_session(&fixture.state.pool, &target_session)
            .await
            .is_err());
        assert!(load_user_by_api_key(&fixture.state.pool, &target_api_key)
            .await
            .is_err());
        assert_eq!(
            sqlx::query_as::<_, (Option<String>, bool, i64, i64)>(
                "SELECT password, deletion_requested_at IS NOT NULL,
                        (SELECT count(*) FROM sessions WHERE user_id = users.id),
                        (SELECT count(*) FROM api_keys WHERE user_id = users.id)
                 FROM users WHERE id = $1",
            )
            .bind(target_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (None, true, 0, 0)
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(claimed.run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(fixture.turn_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Option<Uuid>, i64)>(
                "SELECT lifecycle_status, runtime_owner_id, ownership_generation
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("historical".into(), None, 2)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM hub_session_messages
                 WHERE session_id = $1 AND delivery_state = 'failed'",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            3
        );
        let reuse = create_hub_user(
            &fixture.state.pool,
            Some(&target_email),
            None,
            Some("replacement-password"),
            true,
        )
        .await
        .unwrap_err();
        assert_eq!(reuse.status, StatusCode::CONFLICT);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn user_erasure_retries_storage_then_purges_owned_graph_and_minimizes_audit(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        let target_id: Uuid = sqlx::query_scalar(
            "SELECT users.id
             FROM users JOIN agents ON agents.owner_id = users.id
             WHERE agents.id = $1",
        )
        .bind(fixture.agent_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let suffix = &target_id.simple().to_string()[..8];
        let target_display_name = format!("erase-{suffix}");
        let target_email = format!("erase-{suffix}@example.com");
        sqlx::query("UPDATE users SET display_name = $1, email = $2 WHERE id = $3")
            .bind(&target_display_name)
            .bind(&target_email)
            .bind(target_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let external_owner_id: Uuid =
            sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        sqlx::query("UPDATE hub_sessions SET native_session_id = 'erasure-thread' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let unaffected_agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, public_to)
             VALUES ($1, $2, 'Unaffected Agent', 'keep', 'public_to', ARRAY[$3]::uuid[])",
        )
        .bind(unaffected_agent_id)
        .bind(external_owner_id)
        .bind(target_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let owned_skill_id = Uuid::new_v4();
        let skill_content = "private erased skill";
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Erased Skill', 'private', $3, $4)",
        )
        .bind(owned_skill_id)
        .bind(target_id)
        .bind(skill_content)
        .bind(sha256_hex(skill_content))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(unaffected_agent_id)
            .bind(owned_skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let object_key = format!(
            "sessions/{}/bundles/1-erasure.tar.zst",
            fixture.hub_session_id
        );
        sqlx::query(
            "UPDATE hub_sessions
             SET current_bundle_generation = 1, current_bundle_object_key = $2,
                 current_bundle_kind = 'checkpoint',
                 current_bundle_checksum_sha256 = $3, current_bundle_size_bytes = 12,
                 current_bundle_history_checkpoint = history_checkpoint,
                 current_bundle_ownership_generation = ownership_generation,
                 current_bundle_producing_engine_version = '0.104.0',
                 current_bundle_created_at = now(), current_bundle_runtime_id = $4,
                 current_bundle_checkpoint_attempt_id = $5
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .bind(&object_key)
        .bind("a".repeat(64))
        .bind(fixture.runtime_id)
        .bind(Uuid::new_v4())
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let delete_count = Arc::new(AtomicU64::new(0));
        let object_app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::delete({
                let delete_count = Arc::clone(&delete_count);
                move || {
                    let delete_count = Arc::clone(&delete_count);
                    async move {
                        if delete_count.fetch_add(1, Ordering::SeqCst) == 0 {
                            StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            StatusCode::NO_CONTENT
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let object_address = listener.local_addr().unwrap();
        let object_server =
            tokio::spawn(async move { axum::serve(listener, object_app).await.unwrap() });
        let store = crate::session_bundle_store::S3BundleStore::new(
            crate::session_bundle_store::S3BundleStoreConfig {
                endpoint: format!("http://{object_address}").parse().unwrap(),
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
        let super_token = create_super_admin_session(&fixture.state.pool).await;
        let acting_administrator_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&super_token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let mut state = test_state_with_browser_session_auth(fixture.state.pool.clone());
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let (_, pending) = erase_user(
            State(state.clone()),
            session_headers(&super_token),
            Path(target_id),
            Json(EraseUserRequest {
                email: target_email.clone(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(pending.status, "pending");
        assert_eq!(delete_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            sqlx::query_as::<_, (i64, Option<String>)>(
                "SELECT attempts, last_error FROM user_erasure_bundle_objects
                 WHERE user_id = $1 AND object_key = $2",
            )
            .bind(target_id)
            .bind(&object_key)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (1, Some("object store delete failed".into()))
        );

        let cleanup = RuntimeOwnedSessionGenerationDto {
            session_id: fixture.hub_session_id,
            ownership_generation: 1,
        };
        let dispatched = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(dispatched.cleanup_sessions, vec![cleanup.clone()]);
        let acknowledged = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                cleaned_sessions: vec![cleanup],
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(acknowledged.cleanup_sessions.is_empty());

        process_user_erasure_job(&state, target_id).await.unwrap();
        assert_eq!(delete_count.load(Ordering::SeqCst), 2);
        object_server.abort();

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users WHERE id = $1")
                .bind(target_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users WHERE id = $1")
                .bind(external_owner_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            1
        );
        for (table, id) in [
            ("agents", fixture.agent_id),
            ("hub_sessions", fixture.hub_session_id),
            ("runs", fixture.run_id),
            ("integration_sessions", fixture.session_id),
            ("skills", owned_skill_id),
        ] {
            let count: i64 =
                sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE id = $1"))
                    .bind(id)
                    .fetch_one(&fixture.state.pool)
                    .await
                    .unwrap();
            assert_eq!(count, 0, "{table} retained erased ownership data");
        }
        assert_eq!(
            sqlx::query_as::<_, (Vec<Uuid>, i64)>(
                "SELECT public_to, execution_config_revision FROM agents WHERE id = $1",
            )
            .bind(unaffected_agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (Vec::new(), 2)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM agent_skills WHERE agent_id = $1",)
                .bind(unaffected_agent_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_as::<_, (Uuid, Uuid)>(
                "SELECT erased_user_id, acting_administrator_id
                 FROM user_erasure_audit WHERE erased_user_id = $1",
            )
            .bind(target_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (target_id, acting_administrator_id)
        );
        let audit_columns: Vec<String> = sqlx::query_scalar(
            "SELECT column_name FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'user_erasure_audit'
             ORDER BY ordinal_position",
        )
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            audit_columns,
            vec![
                "erased_user_id",
                "acting_administrator_id",
                "erased_at",
                "erased_role"
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM user_erasure_jobs WHERE user_id = $1",
            )
            .bind(target_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );

        let replacement = create_hub_user(
            &fixture.state.pool,
            Some(&target_email),
            Some(&target_display_name),
            Some("replacement-password"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(replacement.display_name, target_display_name);
        assert_eq!(replacement.email, target_email);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_schema_enforces_required_email_and_optional_password(pool: PgPool) {
        let password = password_hash("existing-password").unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, 'Existing@Example.com', $2, 'Existing', 'member')",
        )
        .bind(Uuid::new_v4())
        .bind(&password)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, 'external@example.com', NULL, 'External', 'member')",
        )
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await
        .unwrap();

        let stored: Option<String> =
            sqlx::query_scalar("SELECT password FROM users WHERE email = 'Existing@Example.com'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored.as_deref(), Some(password.as_str()));

        let missing_email = sqlx::query(
            "INSERT INTO users (id, email, display_name, role)
             VALUES ($1, NULL, 'Missing', 'member')",
        )
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await;
        assert!(missing_email.is_err());

        let duplicate_normalized_email = sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, ' existing@example.com ', NULL, 'Other', 'member')",
        )
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await;
        assert!(duplicate_normalized_email.is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_password_registration_bootstraps_and_reserves_email(pool: PgPool) {
        let app = build_router(test_state_with_pool(pool.clone()));
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/register")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":" First.User@Example.com ","password":"correct horse battery staple"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::SET_COOKIE).is_some());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["user"]["email"], "first.user@example.com");
        assert_eq!(body["user"]["display_name"], "first.user");
        assert_eq!(body["user"]["role"], "super_admin");
        assert!(body.get("verification_required").is_none());

        let stored: (String, Option<String>) = sqlx::query_as(
            "SELECT role, password FROM users WHERE email = 'first.user@example.com'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored.0, "super_admin");
        let stored_password = stored.1.unwrap();
        assert!(stored_password.starts_with("$argon2id$"));
        assert!(verify_password(
            &stored_password,
            "correct horse battery staple"
        ));

        let duplicate = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/register")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"first.user@EXAMPLE.COM","password":"replacement password"}"#,
            ))
            .unwrap();
        let duplicate = app.oneshot(duplicate).await.unwrap();
        assert_eq!(duplicate.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        let unchanged: Option<String> =
            sqlx::query_scalar("SELECT password FROM users WHERE email = 'first.user@example.com'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(verify_password(
            unchanged.as_deref().unwrap(),
            "correct horse battery staple"
        ));
        assert!(!sqlx::query_scalar::<_, bool>(
            "SELECT password_registration_enabled FROM auth_policy WHERE singleton = true",
        )
        .fetch_one(&pool)
        .await
        .unwrap());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_password_policies_are_independent_and_null_password_is_safe(pool: PgPool) {
        let password = password_hash("existing password").unwrap();
        let emergency_password = password_hash("emergency password").unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, 'existing@example.com', $2, 'Existing', 'member'),
                    ($3, 'external-only@example.com', NULL, 'External Only', 'member'),
                    ($4, 'emergency@example.com', $5, 'Emergency', 'super_admin')",
        )
        .bind(Uuid::new_v4())
        .bind(password)
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind(emergency_password)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE auth_policy SET password_registration_enabled = false WHERE singleton = true",
        )
        .execute(&pool)
        .await
        .unwrap();
        let app = build_router({
            let mut state = test_state_with_pool(pool.clone());
            state.auth_providers = vec![Arc::new(PasswordAuthProvider)];
            state
        });

        let register = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/register")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"new@example.com","password":"new password"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(register).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let login_request = || {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"email":"existing@example.com","password":"existing password"}"#,
                ))
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(login_request()).await.unwrap().status(),
            StatusCode::OK
        );

        sqlx::query(
            "UPDATE auth_policy
             SET password_login_enabled = false, ldap_login_enabled = true
             WHERE singleton = true",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            app.clone().oneshot(login_request()).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        let emergency_login = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"emergency@example.com","password":"emergency password"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(emergency_login).await.unwrap().status(),
            StatusCode::OK
        );

        sqlx::query(
            "UPDATE auth_policy
             SET password_login_enabled = true, ldap_login_enabled = false
             WHERE singleton = true",
        )
        .execute(&pool)
        .await
        .unwrap();

        let no_password = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"external-only@example.com","password":"anything"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(no_password).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let providers = axum::http::Request::builder()
            .uri("/api/auth/providers")
            .body(Body::empty())
            .unwrap();
        let providers = app.oneshot(providers).await.unwrap();
        assert_eq!(providers.status(), StatusCode::OK);
        let body = axum::body::to_bytes(providers.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["password_registration_enabled"], false);
        assert_eq!(body["password_login_enabled"], true);
        assert_eq!(body["ldap_login_enabled"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn login_throttles_persist_reset_expire_and_limit_concurrent_ips(pool: PgPool) {
        let email = "persistent-limit@example.com";
        for _ in 0..3 {
            reserve_email_login_attempt(&pool, email).await.unwrap();
        }
        let restarted =
            postgres_test_pool_with_application_name(&pool, "login-throttle-restart").await;
        let limited = reserve_email_login_attempt(&restarted, email)
            .await
            .unwrap_err();
        assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
        assert!(limited.retry_after_seconds.is_some_and(|value| value > 0));
        clear_email_login_failures(&restarted, email).await.unwrap();
        reserve_email_login_attempt(&pool, email).await.unwrap();
        clear_email_login_failures(&pool, email).await.unwrap();

        reserve_email_login_attempt(&pool, email).await.unwrap();
        sqlx::query(
            "UPDATE login_email_failures
             SET window_started_at = now() - interval '61 minutes'
             WHERE normalized_email = $1",
        )
        .bind(email)
        .execute(&pool)
        .await
        .unwrap();
        cleanup_expired_login_throttles(&pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM login_email_failures WHERE normalized_email = $1",
            )
            .bind(email)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );

        let source_ip: IpAddr = "198.51.100.25".parse().unwrap();
        let attempts = futures_util::future::join_all(
            (0..21).map(|_| record_ip_login_attempt(&pool, source_ip)),
        )
        .await;
        assert_eq!(attempts.iter().filter(|result| result.is_ok()).count(), 20);
        let limited = attempts
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].status, StatusCode::TOO_MANY_REQUESTS);
        assert!(limited[0]
            .retry_after_seconds
            .is_some_and(|value| value > 0));
        assert_eq!(
            sqlx::query_scalar::<_, i32>(
                "SELECT attempts FROM login_ip_attempts WHERE source_ip = $1::inet",
            )
            .bind(source_ip.to_string())
            .fetch_one(&pool)
            .await
            .unwrap(),
            21
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_trusted_external_login_binds_stably_by_email(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'trusted-test', 'Trusted Test')",
        )
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
        let first = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "identity-1",
            Some("external-one@example.com"),
            Some("External User"),
        )
        .await
        .unwrap();
        assert_eq!(first.email, "external-one@example.com");
        assert_eq!(first.display_name, "external-one");
        assert_eq!(first.role, "super_admin");

        let password = password_hash("bound password").unwrap();
        let bound_user = create_hub_user(
            &pool,
            Some("bound@example.com"),
            None,
            Some(&password),
            false,
        )
        .await
        .unwrap();
        let bound = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "identity-2",
            Some(" BOUND@EXAMPLE.COM "),
            Some("Different External Name"),
        )
        .await
        .unwrap();
        assert_eq!(bound.id, bound_user.id);
        let bound_password: Option<String> =
            sqlx::query_scalar("SELECT password FROM users WHERE id = $1")
                .bind(bound_user.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(bound_password.as_deref(), Some(password.as_str()));
        let changed = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "identity-1",
            Some("changed@example.com"),
            Some("Renamed Externally"),
        )
        .await
        .unwrap();
        assert_eq!(changed.id, first.id);
        assert_eq!(changed.email, "external-one@example.com");

        let collision = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "identity-3",
            Some("external-three@example.com"),
            Some("External User"),
        )
        .await
        .unwrap();
        assert_ne!(collision.id, first.id);
        assert_eq!(collision.email, "external-three@example.com");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_first_user_bootstrap_is_atomic_across_password_and_external(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, 'test', 'Test')")
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

        let password_create = create_hub_user(
            &pool,
            Some("password@example.com"),
            None,
            Some("password-hash"),
            true,
        );
        let external_create = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "external-first",
            Some("external@example.com"),
            Some("External First"),
        );
        let (password_user, external_user) = tokio::join!(password_create, external_create);
        password_user.unwrap();
        external_user.unwrap();

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users WHERE role = 'super_admin'",)
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn ldap_identity_resolution_fences_deleting_users_with_a_generic_error(pool: PgPool) {
        let user = create_hub_user(
            &pool,
            Some("deleting-ldap@example.com"),
            Some("Deleting LDAP User"),
            None,
            true,
        )
        .await
        .unwrap();
        sqlx::query("UPDATE users SET deletion_requested_at = now() WHERE id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        let error = resolve_ldap_user(
            &pool,
            &LdapDirectoryIdentity {
                email: user.email.clone(),
                display_name: Some("Directory Name".into()),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert_eq!(error.message, "invalid email or password");
        assert!(sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT deletion_requested_at FROM users WHERE id = $1",
        )
        .bind(user.id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_concurrent_external_login_creates_one_stable_binding(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, 'test', 'Test')")
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

        let first = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "same-external-id",
            Some("same@example.com"),
            Some("Same User"),
        );
        let second = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "same-external-id",
            Some("changed@example.com"),
            Some("Changed Profile"),
        );
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(first.email, second.email);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM external_identities
                 WHERE platform_id = $1 AND external_user_id = 'same-external-id'",
            )
            .bind(platform_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_same_external_user_on_two_tenants_keeps_distinct_bindings(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, 'tenant-test', 'Tenant Test')")
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

        let first = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "tenant-a",
            "same-user",
            Some("tenant-a@example.com"),
            Some("Tenant A User"),
        )
        .await
        .unwrap();
        let second = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "tenant-b",
            "same-user",
            Some("tenant-b@example.com"),
            Some("Tenant B User"),
        )
        .await
        .unwrap();

        assert_ne!(first.id, second.id);
        let bindings: Vec<(String, Uuid)> = sqlx::query_as(
            "SELECT tenant_id, user_id FROM external_identities
             WHERE platform_id = $1 AND external_user_id = 'same-user'
             ORDER BY tenant_id",
        )
        .bind(platform_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            bindings,
            vec![
                ("tenant-a".into(), first.id),
                ("tenant-b".into(), second.id)
            ]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_external_login_explicitly_rejects_users_being_erased(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, 'test', 'Test')")
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
        let user = resolve_external_identity(
            &pool,
            platform_id,
            channel_id,
            "default",
            "existing-external-id",
            Some("erasing@example.com"),
            Some("Erasing User"),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE users SET deletion_requested_at = now() WHERE id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        for result in [
            resolve_external_identity(
                &pool,
                platform_id,
                channel_id,
                "default",
                "existing-external-id",
                Some("changed@example.com"),
                Some("Changed Profile"),
            )
            .await,
            resolve_external_identity(
                &pool,
                platform_id,
                channel_id,
                "default",
                "new-external-id",
                Some(" ERASING@EXAMPLE.COM "),
                Some("Same Email"),
            )
            .await,
            resolve_external_identity(
                &pool,
                platform_id,
                channel_id,
                "default",
                "existing-external-id",
                None,
                None,
            )
            .await,
        ] {
            assert_eq!(result.unwrap_err().status, StatusCode::FORBIDDEN);
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM external_identities")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_disabled_or_untrusted_channel_cannot_authenticate(pool: PgPool) {
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'disabled-test', 'Disabled Test')",
        )
        .bind(platform_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email)
             VALUES ($1, $2, 'default', 'Default', false, true)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            resolve_external_identity(
                &pool,
                platform_id,
                channel_id,
                "default",
                "disabled-user",
                Some("disabled@example.com"),
                None,
            )
            .await
            .unwrap_err()
            .status,
            StatusCode::FORBIDDEN
        );

        sqlx::query(
            "UPDATE authentication_channels
             SET enabled = true, trusted_email = false
             WHERE id = $1",
        )
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            resolve_external_identity(
                &pool,
                platform_id,
                channel_id,
                "default",
                "untrusted-user",
                Some("untrusted@example.com"),
                None,
            )
            .await
            .unwrap_err()
            .status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_registration_issues_session_without_verification_fields(pool: PgPool) {
        let app = build_router(test_state_with_pool(pool.clone()));
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/register")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"verify@example.com","password":"verify password"}"#,
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::SET_COOKIE).is_some());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert!(body.get("verification_required").is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sessions")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(body["user"]["email"], "verify@example.com");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_email_less_user_creation_is_rejected(pool: PgPool) {
        let error = create_hub_user(&pool, None, Some("External Only"), None, true)
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_dev_seed_is_idempotent_and_can_log_in(pool: PgPool) {
        seed_dev_user(&pool).await.unwrap();
        seed_dev_user(&pool).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        let app = build_router({
            let mut state = test_state_with_pool(pool);
            state.auth_providers = vec![Arc::new(PasswordAuthProvider)];
            state
        });
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"email":"admin@example.com","password":"admin123"}"#,
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["user"]["email"], "admin@example.com");
        assert_eq!(body["user"]["display_name"], "Admin");
        assert_eq!(body["user"]["role"], "super_admin");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn identity_super_admin_manages_auth_policy_platforms_and_channels(pool: PgPool) {
        let super_admin = create_hub_user(
            &pool,
            Some("root@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let member = create_hub_user(
            &pool,
            Some("member@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let super_token = "super-admin-session";
        let member_token = "member-session";
        for (token, user_id) in [(super_token, super_admin.id), (member_token, member.id)] {
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
        let app = build_router(test_state_with_browser_session_auth(pool.clone()));

        let member_request = axum::http::Request::builder()
            .uri("/api/admin/auth-policy")
            .header(header::COOKIE, format!("agent_hub_session={member_token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(member_request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let get_policy = axum::http::Request::builder()
            .uri("/api/admin/auth-policy")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(get_policy).await.unwrap().status(),
            StatusCode::OK
        );

        for payload in [
            r#"{"password_registration_enabled":true,"password_login_enabled":false,"ldap_login_enabled":true}"#,
            r#"{"password_registration_enabled":false,"password_login_enabled":false,"ldap_login_enabled":false}"#,
            r#"{"password_registration_enabled":false,"password_login_enabled":true,"ldap_login_enabled":true}"#,
        ] {
            let request = axum::http::Request::builder()
                .method(Method::PATCH)
                .uri("/api/admin/auth-policy")
                .header(header::COOKIE, format!("agent_hub_session={super_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::CONFLICT
            );
        }

        let configure_ldap = axum::http::Request::builder()
            .method(Method::PUT)
            .uri("/api/admin/ldap-config")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"url":"ldap://directory.example:389","security":"plain","base_dn":"dc=example,dc=com","bind_identity_template":"{email}","user_filter":"(mail={email})","email_attribute":"mail","display_name_attribute":"displayName","allow_insecure":true,"skip_tls_verify":false}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(configure_ldap).await.unwrap().status(),
            StatusCode::OK
        );
        sqlx::query("UPDATE users SET password = NULL WHERE id = $1")
            .bind(super_admin.id)
            .execute(&pool)
            .await
            .unwrap();
        let disable_without_emergency = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri("/api/admin/auth-policy")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"password_registration_enabled":false,"password_login_enabled":false,"ldap_login_enabled":true}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(disable_without_emergency)
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        sqlx::query("UPDATE users SET password = 'emergency-password-hash' WHERE id = $1")
            .bind(super_admin.id)
            .execute(&pool)
            .await
            .unwrap();
        let disable_with_emergency = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri("/api/admin/auth-policy")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"password_registration_enabled":false,"password_login_enabled":false,"ldap_login_enabled":true}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(disable_with_emergency)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let patch_policy = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri("/api/admin/auth-policy")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"password_registration_enabled":false,"password_login_enabled":true,"ldap_login_enabled":false}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(patch_policy).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["password_registration_enabled"], false);
        assert_eq!(body["password_login_enabled"], true);
        assert_eq!(body["ldap_login_enabled"], false);

        let create_platform = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/admin/external-platforms")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"key":"slack","name":"Slack"}"#))
            .unwrap();
        let response = app.clone().oneshot(create_platform).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let platform: Value = serde_json::from_slice(&body).unwrap();
        let platform_id = platform["id"].as_str().unwrap();
        assert_eq!(platform["key"], "slack");

        let list_platforms = axum::http::Request::builder()
            .uri("/api/admin/external-platforms")
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(list_platforms).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let create_channel = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/admin/external-platforms/{platform_id}/authentication-channels"
            ))
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"key":"workspace","name":"Slack Workspace","enabled":true,"trusted_email":true}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(create_channel).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let channel: Value = serde_json::from_slice(&body).unwrap();
        let channel_id = channel["id"].as_str().unwrap();

        let list_channels = axum::http::Request::builder()
            .uri(format!(
                "/api/admin/external-platforms/{platform_id}/authentication-channels"
            ))
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(list_channels).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let disable_channel = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(format!("/api/admin/authentication-channels/{channel_id}"))
            .header(header::COOKIE, format!("agent_hub_session={super_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"name":"Slack Workspace","enabled":false,"trusted_email":true}"#,
            ))
            .unwrap();
        let response = app.oneshot(disable_channel).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let channel: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(channel["enabled"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn api_key_list_paginates_with_stable_order_and_owner_isolation(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        for (id, email) in [
            (owner_id, "page-owner@example.com"),
            (other_id, "page-other@example.com"),
        ] {
            sqlx::query(
                "INSERT INTO users (id, email, password, display_name, role)
                 VALUES ($1, $2, 'x', 'Test', 'member')",
            )
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex("page-owner-session"))
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();

        let created_at = Utc::now();
        let ids = [Uuid::from_u128(1), Uuid::from_u128(2), Uuid::from_u128(3)];
        for (index, id) in ids.into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO api_keys
                 (id, user_id, name, prefix, token_hash, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(id)
            .bind(owner_id)
            .bind(format!("owner-{index}"))
            .bind(format!("prefix-{index}"))
            .bind(format!("hash-{index}"))
            .bind(if index == 0 {
                created_at + ChronoDuration::seconds(1)
            } else {
                created_at
            })
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO api_keys (id, user_id, name, prefix, token_hash, created_at)
             VALUES ($1, $2, 'foreign', 'foreign', 'foreign', $3)",
        )
        .bind(Uuid::from_u128(4))
        .bind(other_id)
        .bind(created_at + ChronoDuration::seconds(1))
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let app = build_router((*state).clone());
        for uri in [
            "/api/auth/api-keys?page=0",
            "/api/auth/api-keys?page=-1",
            "/api/auth/api-keys?page=1.5",
            "/api/auth/api-keys?page_size=0",
            "/api/auth/api-keys?page_size=101",
            "/api/auth/api-keys?page=999999999999999999999999999999999999",
        ] {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(uri)
                        .header(header::COOKIE, "agent_hub_session=page-owner-session")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "URI: {uri}");
        }
        let first = list_api_keys(
            State(state.clone()),
            session_headers("page-owner-session"),
            Query(ApiKeyListQuery {
                page: Some(1),
                page_size: Some(2),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first.total, 3);
        assert_eq!(first.page, 1);
        assert_eq!(first.page_size, 2);
        assert_eq!(
            first.items.iter().map(|key| key.id).collect::<Vec<_>>(),
            [ids[0], ids[2]]
        );

        let second = list_api_keys(
            State(state.clone()),
            session_headers("page-owner-session"),
            Query(ApiKeyListQuery {
                page: Some(2),
                page_size: Some(2),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            second.items.iter().map(|key| key.id).collect::<Vec<_>>(),
            [ids[1]]
        );

        let empty = list_api_keys(
            State(state),
            session_headers("page-owner-session"),
            Query(ApiKeyListQuery {
                page: Some(3),
                page_size: Some(2),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(empty.items.is_empty());
        assert_eq!(empty.total, 3);
    }

    #[test]
    fn api_key_pagination_defaults_and_rejects_invalid_or_overflowing_values() {
        assert_eq!(ApiKeyListQuery::default().validated().unwrap(), (1, 20, 0));
        for query in [
            ApiKeyListQuery {
                page: Some(0),
                page_size: Some(20),
            },
            ApiKeyListQuery {
                page: Some(-1),
                page_size: Some(20),
            },
            ApiKeyListQuery {
                page: Some(1),
                page_size: Some(0),
            },
            ApiKeyListQuery {
                page: Some(1),
                page_size: Some(101),
            },
            ApiKeyListQuery {
                page: Some(i64::MAX),
                page_size: Some(100),
            },
        ] {
            assert_eq!(
                query.validated().unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[test]
    fn api_key_validity_defaults_to_ninety_days_and_renewal_only_moves_forward() {
        let now = Utc::now();
        assert_eq!(
            api_key_expiration(None, now).unwrap(),
            Some(now + ChronoDuration::days(90))
        );
        assert_eq!(
            api_key_expiration(Some(&ApiKeyValidity::Days { days: 30 }), now).unwrap(),
            Some(now + ChronoDuration::days(30))
        );
        assert_eq!(
            api_key_expiration(Some(&ApiKeyValidity::Never), now).unwrap(),
            None
        );
        assert_eq!(
            api_key_expiration(
                Some(&ApiKeyValidity::Date {
                    expires_at: now + ChronoDuration::hours(1),
                }),
                now,
            )
            .unwrap(),
            Some(now + ChronoDuration::hours(1))
        );
        assert_eq!(
            renewed_api_key_expiration(
                &ApiKeyValidity::Days { days: 180 },
                Some(now + ChronoDuration::days(90)),
                now,
            )
            .unwrap(),
            Some(now + ChronoDuration::days(180))
        );
        assert_eq!(
            renewed_api_key_expiration(
                &ApiKeyValidity::Never,
                Some(now + ChronoDuration::days(90)),
                now,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            renewed_api_key_expiration(
                &ApiKeyValidity::Days { days: 30 },
                Some(now + ChronoDuration::days(90)),
                now,
            )
            .unwrap_err()
            .status,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            renewed_api_key_expiration(&ApiKeyValidity::Never, None, now)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn api_key_create_renew_and_delete_preserve_token_and_ownership(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        for (id, email) in [
            (owner_id, "key-owner@example.com"),
            (other_id, "other@example.com"),
        ] {
            sqlx::query(
                "INSERT INTO users (id, email, password, display_name, role)
                 VALUES ($1, $2, 'x', 'Test', 'member')",
            )
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .unwrap();
        }
        for (token, user_id) in [("owner-session", owner_id), ("other-session", other_id)] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at) VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let headers = session_headers("owner-session");
        let created = create_api_key(
            State(state.clone()),
            headers.clone(),
            Json(CreateApiKeyRequest {
                name: "CI key".into(),
                validity: None,
            }),
        )
        .await
        .unwrap()
        .0;
        let key_id = created.api_key.id;
        let created_at = created.api_key.created_at;
        let token = created.token;
        let original_prefix = created.api_key.prefix.clone();
        let original_hash: String =
            sqlx::query_scalar("SELECT token_hash FROM api_keys WHERE id = $1")
                .bind(key_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let default_expiration = created.api_key.expires_at.unwrap();
        assert!(default_expiration >= created_at + ChronoDuration::days(89));

        let foreign = renew_api_key(
            State(state.clone()),
            session_headers("other-session"),
            Path(key_id),
            Json(RenewApiKeyRequest {
                validity: ApiKeyValidity::Days { days: 180 },
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(foreign.status, StatusCode::NOT_FOUND);

        let renewed = renew_api_key(
            State(state.clone()),
            headers.clone(),
            Path(key_id),
            Json(RenewApiKeyRequest {
                validity: ApiKeyValidity::Days { days: 180 },
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(renewed.id, key_id);
        assert_eq!(renewed.name, "CI key");
        assert_eq!(renewed.created_at, created_at);
        assert_eq!(renewed.prefix, original_prefix);
        assert!(renewed.expires_at.unwrap() > default_expiration);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT token_hash FROM api_keys WHERE id = $1")
                .bind(key_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            original_hash
        );
        assert!(load_user_by_api_key(&pool, &token).await.is_ok());

        delete_api_key(State(state), headers, Path(key_id))
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM api_keys WHERE id = $1")
                .bind(key_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(load_user_by_api_key(&pool, &token).await.is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn api_key_mutations_reject_self_and_allow_another_owner_key(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let foreign_id = Uuid::new_v4();
        for (id, email) in [
            (owner_id, "key-owner-2@example.com"),
            (foreign_id, "foreign-key-owner@example.com"),
        ] {
            sqlx::query(
                "INSERT INTO users (id, email, password, display_name, role)
                 VALUES ($1, $2, 'x', 'Test', 'member')",
            )
            .bind(id)
            .bind(email)
            .execute(&pool)
            .await
            .unwrap();
        }
        let target_id = Uuid::new_v4();
        let caller_id = Uuid::new_v4();
        let unaffected_id = Uuid::new_v4();
        let foreign_key_id = Uuid::new_v4();
        let target_token = new_api_key_token();
        let caller_token = new_api_key_token();
        let unaffected_token = new_api_key_token();
        let foreign_token = new_api_key_token();
        for (id, user_id, name, token) in [
            (target_id, owner_id, "target", target_token.as_str()),
            (caller_id, owner_id, "caller", caller_token.as_str()),
            (
                unaffected_id,
                owner_id,
                "unaffected",
                unaffected_token.as_str(),
            ),
            (
                foreign_key_id,
                foreign_id,
                "foreign",
                foreign_token.as_str(),
            ),
        ] {
            sqlx::query(
                "INSERT INTO api_keys (id, user_id, name, prefix, token_hash, expires_at)
                 VALUES ($1, $2, $3, $4, $5, now() + interval '90 days')",
            )
            .bind(id)
            .bind(user_id)
            .bind(name)
            .bind(token.chars().take(12).collect::<String>())
            .bind(sha256_hex(token))
            .execute(&pool)
            .await
            .unwrap();
        }
        let expired_token = new_api_key_token();
        sqlx::query(
            "INSERT INTO api_keys
                 (id, user_id, name, prefix, token_hash, expires_at, created_at)
             VALUES ($1, $2, 'expired', $3, $4,
                     now() - interval '1 day', now() - interval '2 days')",
        )
        .bind(Uuid::new_v4())
        .bind(owner_id)
        .bind(expired_token.chars().take(12).collect::<String>())
        .bind(sha256_hex(&expired_token))
        .execute(&pool)
        .await
        .unwrap();
        assert!(load_user_by_api_key(&pool, &expired_token).await.is_err());
        let mut state = test_state_with_pool(pool.clone());
        state.auth_providers = vec![Arc::new(ApiKeyAuthProvider)];
        let app = build_router(state);

        let self_renew = api_key_http_request(
            &app,
            Method::POST,
            &format!("/api/auth/api-keys/{target_id}/renew"),
            &target_token,
            json!({ "validity": { "kind": "days", "days": 180 } }),
        )
        .await;
        assert_api_key_http_error(self_renew, StatusCode::NOT_FOUND).await;
        assert_eq!(api_key_record_count(&pool, target_id).await, 1);
        assert!(load_user_by_api_key(&pool, &target_token).await.is_ok());

        let self_delete = api_key_http_request(
            &app,
            Method::DELETE,
            &format!("/api/auth/api-keys/{target_id}"),
            &target_token,
            json!({}),
        )
        .await;
        assert_api_key_http_error(self_delete, StatusCode::NOT_FOUND).await;
        assert_eq!(api_key_record_count(&pool, target_id).await, 1);
        assert!(load_user_by_api_key(&pool, &target_token).await.is_ok());

        let foreign_delete = api_key_http_request(
            &app,
            Method::DELETE,
            &format!("/api/auth/api-keys/{target_id}"),
            &foreign_token,
            json!({}),
        )
        .await;
        assert_api_key_http_error(foreign_delete, StatusCode::NOT_FOUND).await;
        assert_eq!(api_key_record_count(&pool, target_id).await, 1);

        let renewed_response = api_key_http_request(
            &app,
            Method::POST,
            &format!("/api/auth/api-keys/{target_id}/renew"),
            &caller_token,
            json!({ "validity": { "kind": "days", "days": 180 } }),
        )
        .await;
        assert_eq!(renewed_response.status(), StatusCode::OK);
        assert_eq!(
            renewed_response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap(),
            "application/json"
        );
        let renewed_body = axum::body::to_bytes(renewed_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let renewed: ApiKeyDto = serde_json::from_slice(&renewed_body).unwrap();
        assert_eq!(renewed.id, target_id);
        assert_eq!(renewed.name, "target");
        assert!(load_user_by_api_key(&pool, &target_token).await.is_ok());
        assert!(load_user_by_api_key(&pool, &caller_token).await.is_ok());
        assert!(load_user_by_api_key(&pool, &unaffected_token).await.is_ok());

        let permanent_response = api_key_http_request(
            &app,
            Method::POST,
            &format!("/api/auth/api-keys/{target_id}/renew"),
            &caller_token,
            json!({ "validity": { "kind": "never" } }),
        )
        .await;
        assert_eq!(permanent_response.status(), StatusCode::OK);
        let permanent_body = axum::body::to_bytes(permanent_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let permanent_json: Value = serde_json::from_slice(&permanent_body).unwrap();
        assert!(permanent_json.get("token").is_none());
        assert!(permanent_json["expires_at"].is_null());
        let permanent_again = api_key_http_request(
            &app,
            Method::POST,
            &format!("/api/auth/api-keys/{target_id}/renew"),
            &caller_token,
            json!({ "validity": { "kind": "days", "days": 365 } }),
        )
        .await;
        assert_eq!(permanent_again.status(), StatusCode::BAD_REQUEST);
        assert!(load_user_by_api_key(&pool, &target_token).await.is_ok());

        let deleted = api_key_http_request(
            &app,
            Method::DELETE,
            &format!("/api/auth/api-keys/{target_id}"),
            &caller_token,
            json!({}),
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        assert!(deleted.headers().get(header::CONTENT_TYPE).is_none());
        assert!(axum::body::to_bytes(deleted.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(api_key_record_count(&pool, target_id).await, 0);
        assert!(load_user_by_api_key(&pool, &target_token).await.is_err());
        assert!(load_user_by_api_key(&pool, &caller_token).await.is_ok());
        assert!(load_user_by_api_key(&pool, &unaffected_token).await.is_ok());
    }
}
