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
