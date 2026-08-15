//! integrations 领域模块：Integration App、OAuth、Client/Widget 会话与工具调用。

use super::*;
use crate::*;
use agent_hub_shared::*;
use async_stream::stream;
use axum::{
    extract::{Form, Path, Query, State},
    http::{header, HeaderMap, HeaderValue},
    response::{
        sse::{Event, KeepAlive, Sse},
        Redirect, Response,
    },
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{Stream, StreamExt};
use hmac::Mac;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use url::Url;
use uuid::Uuid;

pub(crate) async fn list_integration_apps(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<IntegrationAppDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let app_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM oauth_apps
         WHERE owner_id = $1 AND deleted_at IS NULL
         ORDER BY created_at, id",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    let mut apps = Vec::with_capacity(app_ids.len());
    for app_id in app_ids {
        apps.push(load_integration_app(&state.pool, app_id, user.id).await?);
    }
    Ok(Json(apps))
}

pub(crate) async fn get_integration_app_options(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<IntegrationAppOptionsDto>, ApiError> {
    require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT platform.id AS platform_id, platform.key AS platform_key,
                platform.name AS platform_name, channel.id AS channel_id,
                channel.key AS channel_key, channel.name AS channel_name,
                channel.enabled, channel.trusted_email
         FROM external_platforms AS platform
         JOIN authentication_channels AS channel ON channel.platform_id = platform.id
         WHERE channel.enabled = true AND channel.trusted_email = true
         ORDER BY platform.key, platform.id, channel.key, channel.id",
    )
    .fetch_all(&state.pool)
    .await?;
    let mut external_platforms: Vec<ExternalPlatformDto> = Vec::new();
    let mut authentication_channels = Vec::with_capacity(rows.len());
    for row in rows {
        let platform_id: Uuid = row.get("platform_id");
        if external_platforms
            .last()
            .is_none_or(|platform| platform.id != platform_id)
        {
            external_platforms.push(ExternalPlatformDto {
                id: platform_id,
                key: row.get("platform_key"),
                name: row.get("platform_name"),
            });
        }
        authentication_channels.push(AuthenticationChannelDto {
            id: row.get("channel_id"),
            platform_id,
            key: row.get("channel_key"),
            name: row.get("channel_name"),
            enabled: row.get("enabled"),
            trusted_email: row.get("trusted_email"),
        });
    }
    Ok(Json(IntegrationAppOptionsDto {
        external_platforms,
        authentication_channels,
    }))
}

pub(crate) async fn get_integration_app(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
) -> Result<Json<IntegrationAppDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(
        load_integration_app(&state.pool, app_id, user.id).await?,
    ))
}

pub(crate) async fn create_integration_app(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateIntegrationAppRequest>,
) -> Result<Json<IntegrationAppSecretResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    validate_integration_app_payload(&req.name, &req.redirect_uris, &req.agent_ids)?;
    let allowed_origins = normalize_allowed_origins(&req.allowed_origins)?;
    let tool_allowlist = req
        .tool_allowlist
        .as_deref()
        .map(normalize_agent_tool_allowlist)
        .transpose()?;
    let client_tool_definitions = validate_client_tool_definitions(&req.client_tool_definitions)?;
    validate_public_widget_settings(
        req.login_required,
        req.widget_history_enabled,
        &allowed_origins,
        tool_allowlist.as_deref(),
        &req.agent_ids,
        &user.role,
    )?;
    let client_id = format!("ahc_{}", Uuid::new_v4().simple());
    let client_secret = format!("ahs_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let app_id = Uuid::new_v4();
    let mut tx = state.pool.begin().await?;
    require_integration_authentication_channel_tx(
        &mut tx,
        req.external_platform_id,
        req.authentication_channel_id,
    )
    .await?;
    validate_integration_app_agents_tx(&mut tx, &user, &req.agent_ids).await?;
    validate_integration_app_tool_allowlist_tx(&mut tx, &req.agent_ids, tool_allowlist.as_deref())
        .await?;
    sqlx::query(
        "INSERT INTO oauth_apps
              (id, agent_id, owner_id, name, client_id, client_secret_hash,
              redirect_uris, external_platform_id, authentication_channel_id,
              widget_history_enabled, login_required, allowed_origins, tool_allowlist,
              client_tool_definitions)
         VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(app_id)
    .bind(user.id)
    .bind(req.name.trim())
    .bind(&client_id)
    .bind(sha256_hex(&client_secret))
    .bind(&req.redirect_uris)
    .bind(req.external_platform_id)
    .bind(req.authentication_channel_id)
    .bind(req.widget_history_enabled)
    .bind(req.login_required)
    .bind(
        serde_json::to_value(&allowed_origins)
            .map_err(|_| ApiError::internal("Widget origins could not be encoded"))?,
    )
    .bind(
        tool_allowlist
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| ApiError::internal("App tool policy could not be encoded"))?,
    )
    .bind(client_tool_definitions)
    .execute(&mut *tx)
    .await?;
    replace_integration_app_agents_tx(&mut tx, app_id, &req.agent_ids).await?;
    tx.commit().await?;
    Ok(Json(IntegrationAppSecretResponse {
        integration_app: load_integration_app(&state.pool, app_id, user.id).await?,
        client_secret,
    }))
}

pub(crate) async fn update_integration_app(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
    Json(req): Json<UpdateIntegrationAppRequest>,
) -> Result<Json<IntegrationAppDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    validate_integration_app_payload(&req.name, &req.redirect_uris, &req.agent_ids)?;
    let allowed_origins = normalize_allowed_origins(&req.allowed_origins)?;
    let tool_allowlist = req
        .tool_allowlist
        .as_deref()
        .map(normalize_agent_tool_allowlist)
        .transpose()?;
    let client_tool_definitions = validate_client_tool_definitions(&req.client_tool_definitions)?;
    validate_public_widget_settings(
        req.login_required,
        req.widget_history_enabled,
        &allowed_origins,
        tool_allowlist.as_deref(),
        &req.agent_ids,
        &user.role,
    )?;
    let mut tx = state.pool.begin().await?;
    let exists = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM oauth_apps
         WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(app_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?;
    if exists.is_none() {
        return Err(ApiError::not_found("integration app not found"));
    }
    validate_integration_app_agents_tx(&mut tx, &user, &req.agent_ids).await?;
    validate_integration_app_tool_allowlist_tx(&mut tx, &req.agent_ids, tool_allowlist.as_deref())
        .await?;
    sqlx::query(
        "UPDATE oauth_apps
         SET name = $1, redirect_uris = $2, widget_history_enabled = $3,
             login_required = $4, allowed_origins = $5, tool_allowlist = $6,
             client_tool_definitions = $7,
             updated_at = now()
         WHERE id = $8 AND owner_id = $9",
    )
    .bind(req.name.trim())
    .bind(req.redirect_uris)
    .bind(req.widget_history_enabled)
    .bind(req.login_required)
    .bind(
        serde_json::to_value(&allowed_origins)
            .map_err(|_| ApiError::internal("Widget origins could not be encoded"))?,
    )
    .bind(
        tool_allowlist
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| ApiError::internal("App tool policy could not be encoded"))?,
    )
    .bind(client_tool_definitions)
    .bind(app_id)
    .bind(user.id)
    .execute(&mut *tx)
    .await?;
    replace_integration_app_agents_tx(&mut tx, app_id, &req.agent_ids).await?;
    tx.commit().await?;
    Ok(Json(
        load_integration_app(&state.pool, app_id, user.id).await?,
    ))
}

pub(crate) async fn rotate_integration_app_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
) -> Result<Json<IntegrationAppSecretResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let client_secret = format!("ahs_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let updated = sqlx::query(
        "UPDATE oauth_apps
         SET client_secret_hash = $1, updated_at = now()
         WHERE id = $2 AND owner_id = $3 AND deleted_at IS NULL",
    )
    .bind(sha256_hex(&client_secret))
    .bind(app_id)
    .bind(user.id)
    .execute(&state.pool)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::not_found("integration app not found"));
    }
    Ok(Json(IntegrationAppSecretResponse {
        integration_app: load_integration_app(&state.pool, app_id, user.id).await?,
        client_secret,
    }))
}

pub(crate) async fn create_integration_app_widget_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((app_id, agent_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<CreateEmbedSessionResponse>, ApiError> {
    let session_token = session_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("browser session is required"))?;
    let user = load_user_by_session(&state.pool, &session_token).await?;
    let mut tx = state.pool.begin().await?;
    let app_exists: Option<Uuid> = sqlx::query_scalar(
        "SELECT app.id
         FROM oauth_apps AS app
         JOIN users AS owner
           ON owner.id = app.owner_id AND owner.deletion_requested_at IS NULL
         JOIN authentication_channels AS channel
           ON channel.id = app.authentication_channel_id
          AND channel.platform_id = app.external_platform_id
          AND channel.enabled = true AND channel.trusted_email = true
         WHERE app.id = $1 AND app.owner_id = $2
           AND app.deleted_at IS NULL AND app.client_secret_hash IS NOT NULL
         FOR SHARE OF app",
    )
    .bind(app_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?;
    if app_exists.is_none() {
        return Err(ApiError::not_found("integration app not found"));
    }
    let agent_owner_id: Uuid = sqlx::query_scalar(
        "SELECT owner_id
         FROM agents
         WHERE id = $1 AND deleted_at IS NULL
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         FOR UPDATE",
    )
    .bind(agent_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::forbidden(
        "agent invocation permission is required",
    ))?;
    let delegated: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM integration_app_agents
             WHERE app_id = $1 AND agent_id = $2
         )",
    )
    .bind(app_id)
    .bind(agent_id)
    .fetch_one(&mut *tx)
    .await?;
    if !delegated {
        return Err(ApiError::forbidden(
            "agent is not delegated to the integration app",
        ));
    }
    let token = format!("ahe_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    insert_embed_session_tx(
        &mut tx,
        agent_id,
        agent_owner_id,
        Some(app_id),
        &token,
        Utc::now() + ChronoDuration::hours(1),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(CreateEmbedSessionResponse { token }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct OAuthAuthorizeQuery {
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) state: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) external_user_id: String,
    pub(crate) tenant_id: String,
}

pub(crate) async fn oauth_authorize(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<OAuthAuthorizeQuery>,
) -> Result<Redirect, ApiError> {
    let token = session_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("browser session is required"))?;
    let user = load_user_by_session(&state.pool, &token).await?;
    let app = load_oauth_app_by_client_id(&state.pool, &query.client_id).await?;
    if !redirect_uri_allowed(&app.redirect_uris, &query.redirect_uri) {
        return Err(ApiError::bad_request("redirect uri is not allowed"));
    }
    let tenant_id = require_origin_tenant(Some(&query.tenant_id))?;
    let external_user_id = normalize_external_user_id(&query.external_user_id)?;
    let scopes = parse_oauth_scopes(query.scope.as_deref(), true)?;
    let mut tx = state.pool.begin().await?;
    require_integration_authentication_channel_tx(
        &mut tx,
        app.external_platform_id,
        app.authentication_channel_id,
    )
    .await?;
    let external_identity_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM external_identities
         WHERE platform_id = $1 AND tenant_id = $2
           AND external_user_id = $3 AND user_id = $4
         FOR SHARE",
    )
    .bind(app.external_platform_id)
    .bind(&tenant_id)
    .bind(&external_user_id)
    .bind(user.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::forbidden(
        "external identity is not bound to the current user",
    ))?;
    validate_oauth_agent_scopes_tx(&mut tx, &app, &scopes, Some(&user)).await?;
    let code = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO oauth_authorization_codes
             (code_hash, oauth_app_id, redirect_uri, expires_at, subject_user_id,
              external_identity_id, tenant_id, scopes)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(sha256_hex(&code))
    .bind(app.id)
    .bind(&query.redirect_uri)
    .bind(Utc::now() + ChronoDuration::minutes(5))
    .bind(user.id)
    .bind(external_identity_id)
    .bind(&tenant_id)
    .bind(&scopes)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let location = oauth_redirect_location(&query.redirect_uri, &code, query.state.as_deref())?;
    Ok(Redirect::to(&location))
}

#[derive(Debug, Deserialize)]
pub(crate) struct OAuthTokenForm {
    pub(crate) grant_type: String,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) code: Option<String>,
    pub(crate) redirect_uri: Option<String>,
    pub(crate) scope: Option<String>,
}

pub(crate) async fn oauth_token(
    State(state): State<Arc<AppState>>,
    Form(form): Form<OAuthTokenForm>,
) -> Result<Json<OAuthTokenResponse>, ApiError> {
    let app = load_oauth_app_by_client_id(&state.pool, &form.client_id).await?;
    if !constant_time_eq(
        app.client_secret_hash.as_bytes(),
        sha256_hex(&form.client_secret).as_bytes(),
    ) {
        return Err(ApiError::unauthorized("invalid oauth client"));
    }
    let mut tx = state.pool.begin().await?;
    let (subject_user_id, origin_tenant_id, origin_external_identity_id, scopes) =
        match form.grant_type.as_str() {
            "authorization_code" => {
                let code = form
                    .code
                    .as_deref()
                    .ok_or(ApiError::bad_request("authorization code is required"))?;
                let redirect_uri = form
                    .redirect_uri
                    .as_deref()
                    .ok_or(ApiError::bad_request("redirect uri is required"))?;
                let row = sqlx::query(
                    "UPDATE oauth_authorization_codes
                     SET used_at = now()
                     WHERE code_hash = $1 AND oauth_app_id = $2 AND redirect_uri = $3
                       AND used_at IS NULL AND expires_at > now()
                     RETURNING subject_user_id, external_identity_id, tenant_id, scopes",
                )
                .bind(sha256_hex(code))
                .bind(app.id)
                .bind(redirect_uri)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(ApiError::unauthorized("invalid oauth code"))?;
                let scopes: Vec<String> = row.get("scopes");
                if let Some(requested) = form.scope.as_deref() {
                    if parse_oauth_scopes(Some(requested), false)? != scopes {
                        return Err(ApiError::bad_request(
                            "token scope must match the authorization code",
                        ));
                    }
                }
                let subject_user_id: Uuid = row.get("subject_user_id");
                let subject = load_active_user_tx(&mut tx, subject_user_id).await?;
                validate_oauth_agent_scopes_tx(&mut tx, &app, &scopes, Some(&subject)).await?;
                (
                    Some(subject_user_id),
                    Some(row.get::<String, _>("tenant_id")),
                    Some(row.get::<Uuid, _>("external_identity_id")),
                    scopes,
                )
            }
            "client_credentials" => {
                if form.code.is_some() || form.redirect_uri.is_some() {
                    return Err(ApiError::bad_request(
                        "client credentials cannot include authorization code fields",
                    ));
                }
                let scopes = parse_oauth_scopes(form.scope.as_deref(), false)?;
                if scopes.is_empty() || scopes.iter().any(|scope| !scope.starts_with("agent:")) {
                    return Err(ApiError::bad_request(
                        "client credentials require explicit agent scopes",
                    ));
                }
                validate_oauth_agent_scopes_tx(&mut tx, &app, &scopes, None).await?;
                (None, None, None, scopes)
            }
            _ => return Err(ApiError::bad_request("unsupported grant type")),
        };
    let access_token = format!("aho_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_in = 3600_i64;
    sqlx::query(
        "INSERT INTO oauth_access_tokens
             (id, oauth_app_id, agent_id, owner_id, token_hash, expires_at,
              grant_type, subject_user_id, scopes, origin_tenant_id,
              origin_external_identity_id)
         VALUES ($1, $2, NULL, NULL, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(Uuid::new_v4())
    .bind(app.id)
    .bind(sha256_hex(&access_token))
    .bind(Utc::now() + ChronoDuration::seconds(expires_in))
    .bind(&form.grant_type)
    .bind(subject_user_id)
    .bind(&scopes)
    .bind(origin_tenant_id)
    .bind(origin_external_identity_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(OAuthTokenResponse {
        access_token,
        token_type: "Bearer".into(),
        expires_in,
        scope: scopes.join(" "),
    }))
}

pub(crate) async fn oauth_userinfo(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<OAuthUserInfoDto>, ApiError> {
    let principal = require_application_token(&state, &headers).await?;
    let subject_user_id = principal
        .subject_user_id
        .ok_or(ApiError::forbidden("application principal has no userinfo"))?;
    let user = load_active_user(&state.pool, subject_user_id).await?;
    let identity_id = principal
        .origin_external_identity_id
        .ok_or(ApiError::unauthorized("invalid user application token"))?;
    let row = sqlx::query(
        "SELECT id, platform_id, tenant_id, external_user_id,
                last_username, last_email
         FROM external_identities
         WHERE id = $1 AND user_id = $2 AND platform_id = $3 AND tenant_id = $4",
    )
    .bind(identity_id)
    .bind(user.id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::unauthorized("invalid user application token"))?;
    let external = OAuthExternalProfileDto {
        platform_id: row.get("platform_id"),
        tenant_id: row.get("tenant_id"),
        external_identity_id: row.get("id"),
        external_user_id: row.get("external_user_id"),
        username: row.get("last_username"),
        email: row.get("last_email"),
    };
    Ok(Json(project_oauth_userinfo(
        &principal.scopes,
        &user,
        external,
    )))
}

pub(crate) async fn create_embed_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateEmbedSessionRequest>,
) -> Result<Json<CreateEmbedSessionResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let agent = load_agent_owned_by_user(&state.pool, req.agent_id, user.id).await?;
    let token = format!("ahe_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_at = Utc::now() + ChronoDuration::hours(4);
    let mut tx = state.pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent.id, user.id).await?;
    insert_embed_session_tx(&mut tx, agent.id, user.id, None, &token, expires_at).await?;
    tx.commit().await?;
    Ok(Json(CreateEmbedSessionResponse { token }))
}

pub(crate) async fn create_integration_embed_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateEmbedSessionRequest>,
) -> Result<Json<CreateEmbedSessionResponse>, ApiError> {
    let principal = require_integration(&state, &headers, req.agent_id).await?;
    if principal.grant_type != "client_credentials" {
        return Err(ApiError::forbidden(
            "widget exchange requires client credentials",
        ));
    }
    let token = format!("ahe_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut tx = state.pool.begin().await?;
    lock_active_integration_agent_tx(&mut tx, principal.agent_id, principal.agent_owner_id).await?;
    insert_embed_session_tx(
        &mut tx,
        principal.agent_id,
        principal.agent_owner_id,
        Some(principal.oauth_app_id),
        &token,
        Utc::now() + ChronoDuration::hours(1),
    )
    .await?;
    tx.commit().await?;
    Ok(Json(CreateEmbedSessionResponse { token }))
}

pub(crate) async fn exchange_embed_jwt(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExchangeEmbedJwtRequest>,
) -> Result<Json<CreateEmbedSessionResponse>, ApiError> {
    let principal = authenticate_with_providers(&state, AuthCredential::EmbedJwt(req.jwt)).await?;
    let AuthPrincipal::Embed {
        owner_id, agent_id, ..
    } = principal
    else {
        return Err(ApiError::unauthorized("invalid embed principal"));
    };
    let agent = load_agent_owned_by_user(&state.pool, agent_id, owner_id).await?;
    let token = format!("ahe_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_at = Utc::now() + ChronoDuration::hours(4);
    let mut tx = state.pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent.id, agent.owner_id).await?;
    insert_embed_session_tx(&mut tx, agent.id, agent.owner_id, None, &token, expires_at).await?;
    tx.commit().await?;
    Ok(Json(CreateEmbedSessionResponse { token }))
}

pub(crate) async fn create_client_access(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateWidgetAccessRequest>,
) -> Result<Json<ClientAccessResponse>, ApiError> {
    let (access, _) = issue_authenticated_client_access(&state, &headers, req).await?;
    Ok(Json(access))
}

pub(crate) async fn create_widget_access(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateWidgetAccessRequest>,
) -> Result<Json<WidgetAccessResponse>, ApiError> {
    let (access, _) = issue_authenticated_client_access(&state, &headers, req).await?;
    Ok(Json(WidgetAccessResponse {
        token: access.access_token,
        expires_at: access.expires_at,
        agent: access.agent,
        history_enabled: access.history_enabled,
    }))
}

pub(crate) async fn issue_authenticated_client_access(
    state: &AppState,
    headers: &HeaderMap,
    req: CreateWidgetAccessRequest,
) -> Result<(ClientAccessResponse, Uuid), ApiError> {
    let (client_id, client_secret) = widget_client_credentials(headers)?;
    if req.client_instance_id.is_nil() {
        return Err(ApiError::bad_request(
            "valid Client Instance id is required",
        ));
    }
    let client_tool_definitions = validate_client_tool_definitions(&req.client_tools)?;
    let requires_integration_tool = !req.client_tools.is_empty();
    let tool_names = req
        .client_tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let mut tx = state.pool.begin().await?;
    let app = load_oauth_app_by_client_id_tx(&mut tx, &client_id).await?;
    if !constant_time_eq(
        app.client_secret_hash.as_bytes(),
        sha256_hex(&client_secret).as_bytes(),
    ) {
        return Err(ApiError::unauthorized("invalid oauth client"));
    }
    validate_client_request_origin(headers, &app.allowed_origins, false)?;
    let tenant_id = require_origin_tenant(Some(&req.tenant_id))?;
    let external_user_id = normalize_external_user_id(&req.external_user_id)?;
    let profile = normalize_widget_user_profile(WidgetUserProfileDto {
        username: req.username,
        display_name: req.display_name,
        email: Some(req.email),
        attributes: req.attributes,
    })?;
    require_integration_authentication_channel_tx(
        &mut tx,
        app.external_platform_id,
        app.authentication_channel_id,
    )
    .await?;
    validate_oauth_agent_scopes_tx(&mut tx, &app, &[format!("agent:{}", req.agent_id)], None)
        .await?;
    let agent = sqlx::query(
        "SELECT agent.id, agent.owner_id, agent.name, agent.instructions
         FROM integration_app_agents AS delegated
         JOIN agents AS agent ON agent.id = delegated.agent_id
         WHERE delegated.app_id = $1 AND delegated.agent_id = $2
           AND agent.deleted_at IS NULL
           AND 'integration' = ANY(agent.endpoint_exposure)
           AND ($3::boolean = false OR agent.tool_allowlist ? 'integration')
         FOR UPDATE OF agent, delegated",
    )
    .bind(app.id)
    .bind(req.agent_id)
    .bind(requires_integration_tool)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::forbidden(
        "oauth agent scope is not currently delegated",
    ))?;
    let agent_owner_id: Uuid = agent.get("owner_id");
    lock_active_integration_agent_tx(&mut tx, req.agent_id, agent_owner_id).await?;
    let resolved = resolve_external_identity_tx(
        &mut tx,
        app.external_platform_id,
        app.authentication_channel_id,
        &tenant_id,
        &external_user_id,
        profile.email.as_deref(),
        profile.username.as_deref(),
    )
    .await?;
    update_external_identity_widget_profile_tx(&mut tx, resolved.identity_id, &profile).await?;
    let external_user = ExternalUserContextDto {
        external_user_id,
        tenant_id,
        username: profile.username,
        display_name: profile.display_name,
        email: profile.email,
        attributes: profile.attributes,
    };
    let token = format!("ahw_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_at = Utc::now() + ChronoDuration::seconds(CLIENT_ACCESS_TTL_SECONDS);
    let credential_id = insert_widget_access_session_tx(
        &mut tx,
        WidgetAccessSessionInsert {
            oauth_app_id: app.id,
            agent_id: req.agent_id,
            owner_id: resolved.user.id,
            external_identity_id: resolved.identity_id,
            external_user: &external_user,
            client_instance_id: req.client_instance_id,
            client_tool_definitions,
            token: &token,
            expires_at,
        },
    )
    .await?;
    tx.commit().await?;
    Ok((
        ClientAccessResponse {
            access_token: token,
            expires_at,
            expires_in: CLIENT_ACCESS_TTL_SECONDS,
            client_instance_id: req.client_instance_id,
            session_id: None,
            agent: WidgetAgentDto {
                id: agent.get("id"),
                name: agent.get("name"),
                instructions: agent.get("instructions"),
            },
            history_enabled: app.widget_history_enabled,
            tool_names,
        },
        credential_id,
    ))
}

pub(crate) async fn create_anonymous_client_access(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreatePublicWidgetAccessRequest>,
) -> Result<Json<ClientAccessResponse>, ApiError> {
    let (access, _) = issue_anonymous_client_access(&state, &headers, req).await?;
    Ok(Json(access))
}

pub(crate) async fn create_public_widget_access(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreatePublicWidgetAccessRequest>,
) -> Result<Json<PublicWidgetAccessResponse>, ApiError> {
    let (access, widget_session_id) = issue_anonymous_client_access(&state, &headers, req).await?;
    Ok(Json(PublicWidgetAccessResponse {
        token: access.access_token,
        expires_at: access.expires_at,
        widget_session_id,
        hub_session_id: access.session_id,
        agent: access.agent,
    }))
}

pub(crate) async fn issue_anonymous_client_access(
    state: &AppState,
    headers: &HeaderMap,
    req: CreatePublicWidgetAccessRequest,
) -> Result<(ClientAccessResponse, Uuid), ApiError> {
    if req.client_instance_id.is_nil() {
        return Err(ApiError::bad_request(
            "valid Client Instance id is required",
        ));
    }
    let anonymous_key_hash = visitor_key_hash(&req.visitor_key)?;
    let mut tx = state.pool.begin().await?;
    let app = load_public_widget_app_by_client_id_tx(&mut tx, &req.client_id).await?;
    validate_client_request_origin(headers, &app.allowed_origins, true)?;
    let client_tool_definitions = validate_client_tool_definitions(&app.client_tool_definitions)?;
    let requires_integration_tool = !app.client_tool_definitions.is_empty();
    let tool_names = app
        .client_tool_definitions
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let agent = sqlx::query(
        "SELECT agent.id, agent.owner_id, agent.name, agent.instructions
         FROM integration_app_agents AS delegated
         JOIN agents AS agent ON agent.id = delegated.agent_id
         WHERE delegated.app_id = $1 AND agent.deleted_at IS NULL
           AND ($2::boolean = false OR agent.tool_allowlist ? 'integration')
         ORDER BY agent.id
         LIMIT 1",
    )
    .bind(app.id)
    .bind(requires_integration_tool)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("public Widget Agent not found"))?;
    let agent_id: Uuid = agent.get("id");
    let owner_id: Uuid = agent.get("owner_id");
    lock_active_integration_agent_tx(&mut tx, agent_id, owner_id).await?;
    let token = format!("ahp_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_at = Utc::now() + ChronoDuration::seconds(CLIENT_ACCESS_TTL_SECONDS);
    let recovered = sqlx::query(
        "SELECT hub_session_id, last_run_id
         FROM embed_sessions
         WHERE oauth_app_id = $1 AND anonymous = true
           AND anonymous_key_hash = $2 AND agent_id = $3 AND owner_id = $4
           AND ($5::uuid IS NULL OR hub_session_id = $5)
         ORDER BY (client_instance_id = $6) DESC, created_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(app.id)
    .bind(&anonymous_key_hash)
    .bind(agent_id)
    .bind(owner_id)
    .bind(req.session_id)
    .bind(req.client_instance_id)
    .fetch_optional(&mut *tx)
    .await?;
    if req.session_id.is_some() && recovered.is_none() {
        return Err(ApiError::not_found("anonymous Client Session not found"));
    }
    let hub_session_id: Option<Uuid> = recovered.as_ref().and_then(|row| row.get("hub_session_id"));
    let last_run_id: Option<Uuid> = recovered.as_ref().and_then(|row| row.get("last_run_id"));
    let (widget_session_id, hub_session_id): (Uuid, Option<Uuid>) = sqlx::query_as(
        "INSERT INTO embed_sessions
             (token_hash, agent_id, owner_id, oauth_app_id, expires_at,
              anonymous, anonymous_key_hash, client_instance_id,
              client_tool_definitions, hub_session_id, last_run_id)
         VALUES ($1, $2, $3, $4, $5, true, $6, $7, $8, $9, $10)
         ON CONFLICT (oauth_app_id, anonymous_key_hash, client_instance_id)
             WHERE anonymous AND client_instance_id IS NOT NULL
         DO UPDATE SET token_hash = EXCLUDED.token_hash,
                       expires_at = EXCLUDED.expires_at,
                       agent_id = EXCLUDED.agent_id,
                       owner_id = EXCLUDED.owner_id,
                       client_tool_definitions = EXCLUDED.client_tool_definitions,
                       hub_session_id = EXCLUDED.hub_session_id,
                       last_run_id = EXCLUDED.last_run_id
         RETURNING id, hub_session_id",
    )
    .bind(sha256_hex(&token))
    .bind(agent_id)
    .bind(owner_id)
    .bind(app.id)
    .bind(expires_at)
    .bind(&anonymous_key_hash)
    .bind(req.client_instance_id)
    .bind(client_tool_definitions)
    .bind(hub_session_id)
    .bind(last_run_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((
        ClientAccessResponse {
            access_token: token,
            expires_at,
            expires_in: CLIENT_ACCESS_TTL_SECONDS,
            client_instance_id: req.client_instance_id,
            session_id: hub_session_id,
            agent: WidgetAgentDto {
                id: agent_id,
                name: agent.get("name"),
                instructions: agent.get("instructions"),
            },
            history_enabled: false,
            tool_names,
        },
        widget_session_id,
    ))
}

pub(crate) async fn get_widget_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    let agent = sqlx::query(
        "SELECT id, name, instructions FROM agents WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(credential.agent_id)
    .fetch_one(&mut *tx)
    .await?;
    let agent = WidgetAgentDto {
        id: agent.get("id"),
        name: agent.get("name"),
        instructions: agent.get("instructions"),
    };
    tx.commit().await?;
    if credential.is_external() {
        return Ok(Json(WidgetSessionDto {
            agent,
            expires_at: credential.expires_at,
            history_enabled: credential.history_enabled,
        })
        .into_response());
    }
    Ok(Json(agent).into_response())
}

pub(crate) async fn renew_client_access(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RenewWidgetSessionRequest>,
) -> Result<Json<ClientAccessResponse>, ApiError> {
    Ok(Json(rotate_client_access(&state, &headers, req).await?))
}

pub(crate) async fn renew_widget_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RenewWidgetSessionRequest>,
) -> Result<Json<WidgetTokenResponse>, ApiError> {
    let access = rotate_client_access(&state, &headers, req).await?;
    Ok(Json(WidgetTokenResponse {
        token: access.access_token,
        expires_at: access.expires_at,
    }))
}

pub(crate) async fn rotate_client_access(
    state: &AppState,
    headers: &HeaderMap,
    req: RenewWidgetSessionRequest,
) -> Result<ClientAccessResponse, ApiError> {
    let token = client_access_token_from_headers(headers)
        .ok_or(ApiError::unauthorized("invalid Widget credential"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, headers).await?;
    let client_instance_id = credential
        .client_instance_id
        .ok_or(ApiError::unauthorized("invalid Client Access Credential"))?;
    if !credential.is_external() && !credential.is_anonymous() {
        return Err(ApiError::unauthorized("invalid Widget credential"));
    }
    if let Some(profile) = req.profile {
        if credential.is_anonymous() {
            return Err(ApiError::bad_request(
                "anonymous Client Access has no trusted user profile",
            ));
        }
        let (client_id, client_secret) = widget_client_credentials(headers)?;
        let app = load_oauth_app_by_client_id_tx(&mut tx, &client_id).await?;
        if Some(app.id) != credential.oauth_app_id
            || !constant_time_eq(
                app.client_secret_hash.as_bytes(),
                sha256_hex(&client_secret).as_bytes(),
            )
        {
            return Err(ApiError::unauthorized("invalid oauth client"));
        }
        let profile = normalize_widget_user_profile(profile)?;
        let external_identity_id = credential
            .external_identity_id
            .ok_or(ApiError::internal("external Widget identity is missing"))?;
        update_external_identity_widget_profile_tx(&mut tx, external_identity_id, &profile).await?;
        let external_user = ExternalUserContextDto {
            external_user_id: credential
                .external_user_id
                .clone()
                .ok_or(ApiError::internal("external Widget user is missing"))?,
            tenant_id: credential
                .external_tenant_id
                .clone()
                .ok_or(ApiError::internal("external Widget tenant is missing"))?,
            username: profile.username,
            display_name: profile.display_name,
            email: profile.email,
            attributes: profile.attributes,
        };
        let profile_snapshot = serde_json::to_value(external_user)
            .map_err(|_| ApiError::internal("external user profile could not be encoded"))?;
        sqlx::query("UPDATE embed_sessions SET profile_snapshot = $1 WHERE id = $2")
            .bind(profile_snapshot)
            .bind(credential.id)
            .execute(&mut *tx)
            .await?;
    }
    let prefix = if credential.is_anonymous() {
        "ahp_"
    } else {
        "ahw_"
    };
    let renewed_token = format!(
        "{prefix}{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );
    let expires_at = Utc::now() + ChronoDuration::seconds(CLIENT_ACCESS_TTL_SECONDS);
    sqlx::query(
        "UPDATE embed_sessions
         SET token_hash = $1, expires_at = $2
         WHERE id = $3 AND token_hash = $4",
    )
    .bind(sha256_hex(&renewed_token))
    .bind(expires_at)
    .bind(credential.id)
    .bind(sha256_hex(&token))
    .execute(&mut *tx)
    .await?;
    let agent = sqlx::query(
        "SELECT id, name, instructions FROM agents WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(credential.agent_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ClientAccessResponse {
        access_token: renewed_token,
        expires_at,
        expires_in: CLIENT_ACCESS_TTL_SECONDS,
        client_instance_id,
        session_id: credential
            .is_anonymous()
            .then_some(credential.hub_session_id)
            .flatten(),
        agent: WidgetAgentDto {
            id: agent.get("id"),
            name: agent.get("name"),
            instructions: agent.get("instructions"),
        },
        history_enabled: credential.history_enabled,
        tool_names: credential
            .client_tool_definitions
            .into_iter()
            .map(|tool| tool.name)
            .collect(),
    })
}

pub(crate) async fn list_widget_sessions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<WidgetHistorySessionDto>>, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    if !credential.history_enabled {
        return Err(ApiError::forbidden("Widget history is disabled"));
    }
    let (
        oauth_app_id,
        external_platform_id,
        external_tenant_id,
        external_user_id,
        external_identity_id,
    ) = credential.external_scope()?;
    let rows = sqlx::query(
        "SELECT integration.id, hub.id AS hub_session_id, hub.created_at,
                GREATEST(
                    hub.updated_at,
                    COALESCE(
                        (SELECT max(message.accepted_at)
                         FROM hub_session_messages AS message
                         WHERE message.session_id = hub.id),
                        hub.updated_at
                    )
                ) AS updated_at,
                (SELECT message.content
                 FROM hub_session_messages AS message
                 WHERE message.session_id = hub.id AND message.role = 'user'
                   AND message.content IS NOT NULL
                 ORDER BY message.sequence LIMIT 1) AS preview
         FROM integration_sessions AS integration
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.oauth_app_id = $1
           AND integration.agent_id = $2
           AND integration.owner_id = $3
           AND integration.external_user_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $5
           AND hub.origin_tenant_id = $6
           AND hub.origin_external_identity_id = $7
         ORDER BY updated_at DESC, integration.id DESC
         LIMIT 100",
    )
    .bind(oauth_app_id)
    .bind(credential.agent_id)
    .bind(credential.owner_id)
    .bind(external_user_id)
    .bind(external_platform_id)
    .bind(external_tenant_id)
    .bind(external_identity_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| WidgetHistorySessionDto {
                id: row.get("id"),
                hub_session_id: row.get("hub_session_id"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
                preview: row.get("preview"),
            })
            .collect(),
    ))
}

pub(crate) async fn list_widget_session_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<SessionMessageListQuery>,
) -> Result<Json<Vec<HubSessionMessageDto>>, ApiError> {
    let (before_sequence, limit) = query.validated()?;
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    let (integration_session_id, hub_session_id) = widget_session_locator(&credential, session_id);
    let scoped = load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        integration_session_id,
        hub_session_id,
        false,
    )
    .await?;
    let rows = sqlx::query(SESSION_MESSAGE_PAGE_SQL)
        .bind(scoped.hub_session_id)
        .bind(before_sequence)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
    let mut messages = rows
        .into_iter()
        .map(hub_message_from_row)
        .collect::<Vec<_>>();
    fill_message_attachments(&mut *tx, &mut messages).await?;
    tx.commit().await?;
    Ok(Json(messages))
}

pub(crate) async fn list_widget_session_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<EventStreamQuery>,
) -> Result<Json<Vec<RunEventDto>>, ApiError> {
    let after = widget_event_cursor(query.after)?;
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, &headers).await?;
    let (integration_session_id, hub_session_id) = widget_session_locator(&credential, session_id);
    let scoped = load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        integration_session_id,
        hub_session_id,
        false,
    )
    .await?;
    let events = load_widget_session_events_after_tx(&mut tx, &scoped, after, query.limit).await?;
    tx.commit().await?;
    Ok(Json(events))
}

pub(crate) async fn stream_widget_session_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<EventStreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let mut last_seq = widget_event_cursor(query.after)?;
    authorize_widget_session(&state, &headers, session_id).await?;
    let stream_state = state.clone();
    let stream_headers = headers.clone();
    let event_stream = stream! {
        loop {
            let token = match client_access_token_from_headers(&stream_headers) {
                Some(token) => token,
                None => {
                    yield Ok(Event::default().event("error").data("missing embed session"));
                    break;
                }
            };
            let mut tx = match stream_state.pool.begin().await {
                Ok(tx) => tx,
                Err(_) => {
                    yield Ok(Event::default().event("error").data("Widget history is unavailable"));
                    break;
                }
            };
            let loaded = async {
                let credential = load_widget_credential_tx(&mut tx, &token, &stream_headers).await?;
                let (integration_session_id, hub_session_id) =
                    widget_session_locator(&credential, session_id);
                let scoped = load_widget_scoped_session_tx(
                    &mut tx,
                    &credential,
                    integration_session_id,
                    hub_session_id,
                    false,
                )
                .await?;
                let events = load_widget_session_events_after_tx(&mut tx, &scoped, last_seq, None).await?;
                tx.commit().await?;
                Ok::<_, ApiError>(events)
            }.await;
            match loaded {
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
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    };
    Ok(Sse::new(event_stream).keep_alive(KeepAlive::default()))
}

pub(crate) fn widget_event_cursor(after: Option<i64>) -> Result<i64, ApiError> {
    let after = after.unwrap_or(0);
    if after < 0 {
        return Err(ApiError::bad_request("event cursor must be nonnegative"));
    }
    Ok(after)
}

pub(crate) async fn create_widget_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateWidgetRunRequest>,
) -> Result<Json<RunDto>, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, &headers).await?;
    ensure_agent_has_configured_model_tx(&mut tx, credential.agent_id).await?;
    if !credential.is_anonymous() {
        let missing_grants =
            missing_secret_grants(&state.pool, credential.owner_id, credential.agent_id).await?;
        if !missing_grants.is_empty() {
            return Err(ApiError::requires_secret_grants(missing_grants));
        }
    }
    let prepend_instructions = match req.prepend_instructions.as_deref() {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                if trimmed.len() > 65536 {
                    return Err(ApiError::bad_request(
                        "prepend_instructions must not exceed 65536 bytes",
                    ));
                }
                Some(trimmed)
            }
        }
        None => None,
    };
    let client_message_key = normalize_client_message_key(req.client_message_key.as_deref())?;
    let (requested_integration_session_id, requested_hub_session_id) =
        widget_run_session_locator(&credential, &req)?;

    let (hub_session_id, integration_session_id, external_user_context) = if credential
        .is_anonymous()
    {
        let retried_hub_session_id = if let Some(client_message_key) = client_message_key.as_deref()
        {
            sqlx::query_scalar::<_, Uuid>(
                "SELECT runs.hub_session_id
                 FROM runs
                 JOIN hub_session_messages AS message
                   ON message.session_id = runs.hub_session_id
                  AND message.run_id = runs.id
                 WHERE runs.widget_session_id = $1
                   AND runs.source = 'widget'
                   AND message.client_message_key = $2
                 ORDER BY message.accepted_at, message.id
                 LIMIT 1",
            )
            .bind(credential.id)
            .bind(client_message_key)
            .fetch_optional(&mut *tx)
            .await?
        } else {
            None
        };
        let hub_session_id = if let Some(hub_session_id) = retried_hub_session_id {
            hub_session_id
        } else if let Some(hub_session_id) = credential.hub_session_id {
            if requested_hub_session_id.is_some_and(|requested| requested != hub_session_id) {
                return Err(ApiError::bad_request(
                    "public Widget credential is bound to another session",
                ));
            }
            hub_session_id
        } else {
            if requested_hub_session_id.is_some() {
                return Err(ApiError::bad_request(
                    "public Widget has not started this session",
                ));
            }
            let hub_session_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO hub_sessions
                     (id, owner_id, agent_id, origin_kind, lifecycle_status)
                 VALUES ($1, $2, $3, 'public_widget', 'waiting_for_runtime')",
            )
            .bind(hub_session_id)
            .bind(credential.owner_id)
            .bind(credential.agent_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE embed_sessions
                 SET hub_session_id = $1
                 WHERE id = $2 AND hub_session_id IS NULL",
            )
            .bind(hub_session_id)
            .bind(credential.id)
            .execute(&mut *tx)
            .await?;
            hub_session_id
        };
        (hub_session_id, None, None)
    } else if credential.is_external() {
        let oauth_app_id = credential
            .oauth_app_id
            .ok_or(ApiError::internal("external Widget app is missing"))?;
        let external_platform_id = credential
            .external_platform_id
            .ok_or(ApiError::internal("external Widget platform is missing"))?;
        let external_tenant_id = credential
            .external_tenant_id
            .as_deref()
            .ok_or(ApiError::internal("external Widget tenant is missing"))?;
        let external_user_id = credential
            .external_user_id
            .as_deref()
            .ok_or(ApiError::internal("external Widget user is missing"))?;
        let external_identity_id = credential
            .external_identity_id
            .ok_or(ApiError::internal("external Widget identity is missing"))?;
        let external_user =
            serde_json::from_value::<ExternalUserContextDto>(credential.profile_snapshot.clone())
                .map_err(|_| ApiError::internal("external Widget profile is invalid"))?;

        if let Some(prepend) = prepend_instructions {
            if let Some(existing_session_id) = requested_integration_session_id {
                // 不可变：与已存值一致（幂等重放）→ 忽略；不一致 → 400。
                let stored: Option<String> = sqlx::query_scalar(
                    "SELECT prepend_instructions FROM integration_sessions WHERE id = $1",
                )
                .bind(existing_session_id)
                .fetch_one(&mut *tx)
                .await?;
                if stored.as_deref() != Some(prepend) {
                    return Err(ApiError::bad_request(
                        "prepend_instructions is immutable after session creation",
                    ));
                }
            } else if requested_hub_session_id.is_some() {
                return Err(ApiError::bad_request(
                    "prepend_instructions is immutable after session creation",
                ));
            }
        }
        let (selected_hub_session_id, selected_integration_session_id) =
            if requested_integration_session_id.is_some() || requested_hub_session_id.is_some() {
                let selected = load_widget_scoped_session_tx(
                    &mut tx,
                    &credential,
                    requested_integration_session_id,
                    requested_hub_session_id,
                    true,
                )
                .await?;
                (
                    selected.hub_session_id,
                    selected
                        .integration_session_id
                        .expect("external Widget Session has an Integration Session"),
                )
            } else {
                let retried_session =
                    if let Some(client_message_key) = client_message_key.as_deref() {
                        sqlx::query_as::<_, (Uuid, Uuid)>(
                            "SELECT integration.id, integration.hub_session_id
                         FROM runs
                         JOIN hub_session_messages AS message
                           ON message.session_id = runs.hub_session_id
                          AND message.run_id = runs.id
                         JOIN integration_sessions AS integration
                           ON integration.id = runs.integration_session_id
                          AND integration.hub_session_id = runs.hub_session_id
                         WHERE runs.widget_session_id = $1
                           AND runs.source = 'widget'
                           AND message.client_message_key = $2
                         ORDER BY message.accepted_at, message.id
                         LIMIT 1",
                        )
                        .bind(credential.id)
                        .bind(client_message_key)
                        .fetch_optional(&mut *tx)
                        .await?
                    } else {
                        None
                    };
                if let Some(prepend) = prepend_instructions {
                    if let Some((retried_integration_id, _)) = retried_session {
                        let stored: Option<String> = sqlx::query_scalar(
                            "SELECT prepend_instructions FROM integration_sessions WHERE id = $1",
                        )
                        .bind(retried_integration_id)
                        .fetch_one(&mut *tx)
                        .await?;
                        if stored.as_deref() != Some(prepend) {
                            return Err(ApiError::bad_request(
                                "prepend_instructions is immutable after session creation",
                            ));
                        }
                    }
                }
                if let Some((integration_session_id, hub_session_id)) = retried_session {
                    let selected = load_widget_scoped_session_tx(
                        &mut tx,
                        &credential,
                        Some(integration_session_id),
                        Some(hub_session_id),
                        true,
                    )
                    .await?;
                    (
                        selected.hub_session_id,
                        selected
                            .integration_session_id
                            .expect("external Widget Session has an Integration Session"),
                    )
                } else {
                    let hub_session_id = Uuid::new_v4();
                    sqlx::query(
                        "INSERT INTO hub_sessions
                             (id, owner_id, agent_id, origin_kind, origin_platform_id,
                              origin_tenant_id, origin_external_identity_id, lifecycle_status)
                         VALUES ($1, $2, $3, 'external', $4, $5, $6,
                                 'waiting_for_runtime')",
                    )
                    .bind(hub_session_id)
                    .bind(credential.owner_id)
                    .bind(credential.agent_id)
                    .bind(external_platform_id)
                    .bind(external_tenant_id)
                    .bind(external_identity_id)
                    .execute(&mut *tx)
                    .await?;
                    let integration_session_id = Uuid::new_v4();
                    sqlx::query(
                        "INSERT INTO integration_sessions
                             (id, oauth_app_id, agent_id, owner_id, external_user_id,
                              tool_definitions, metadata, hub_session_id, prepend_instructions)
                         VALUES ($1, $2, $3, $4, $5, '[]'::jsonb, '{}'::jsonb, $6, $7)",
                    )
                    .bind(integration_session_id)
                    .bind(oauth_app_id)
                    .bind(credential.agent_id)
                    .bind(credential.owner_id)
                    .bind(external_user_id)
                    .bind(hub_session_id)
                    .bind(prepend_instructions)
                    .execute(&mut *tx)
                    .await?;
                    (hub_session_id, integration_session_id)
                }
            };
        (
            selected_hub_session_id,
            Some(selected_integration_session_id),
            Some(external_user),
        )
    } else {
        let hub_session_id = credential
            .hub_session_id
            .ok_or(ApiError::unauthorized("invalid embed session"))?;
        if requested_hub_session_id.is_some_and(|requested| requested != hub_session_id) {
            return Err(ApiError::bad_request(
                "embed token is bound to another session",
            ));
        }
        (hub_session_id, None, None)
    };
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id: hub_session_id,
            agent_id: credential.agent_id,
            owner_id: credential.owner_id,
            content: req.message,
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key,
            source: "widget".into(),
            automation_id: None,
            integration_session_id,
            parent_run_id: req.parent_run_id,
            continuation_turn_id: None,
            model_subject_type: if credential.oauth_app_id.is_some() {
                "integration_app".into()
            } else {
                "user".into()
            },
            model_subject_user_id: credential
                .oauth_app_id
                .is_none()
                .then_some(credential.owner_id),
            model_source_integration_app_id: credential.oauth_app_id,
            external_user_context,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    let run = accepted
        .run
        .ok_or(ApiError::internal("widget message did not schedule a run"))?;
    if accepted.message.delivery_mode != "steer" {
        if let Some(client_instance_id) = credential.client_instance_id {
            let client_tool_snapshot = serde_json::to_value(&credential.client_tool_definitions)
                .map_err(|_| ApiError::internal("Client Tool Grant could not be encoded"))?;
            sqlx::query(
                "UPDATE runs
                 SET client_instance_id = $1, client_tool_snapshot = $2
                 WHERE id = $3 AND hub_message_id = $4 AND source = 'widget'
                   AND client_instance_id IS NULL",
            )
            .bind(client_instance_id)
            .bind(client_tool_snapshot)
            .bind(run.id)
            .bind(accepted.message.id)
            .execute(&mut *tx)
            .await?;
        }
    }
    sqlx::query(
        "UPDATE runs
         SET widget_session_id = COALESCE(widget_session_id, $1)
         WHERE id = $2",
    )
    .bind(credential.id)
    .bind(run.id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE embed_sessions SET last_run_id = $1 WHERE id = $2")
        .bind(run.id)
        .bind(credential.id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Json(run))
}

pub(crate) async fn stop_widget_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunDto>, ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, &headers).await?;
    let run = locate_widget_run_tx(&mut tx, &credential, run_id).await?;
    let scoped = load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        run.get("integration_session_id"),
        Some(run.get("hub_session_id")),
        true,
    )
    .await?;
    if run.get::<Option<Uuid>, _>("client_instance_id").is_some()
        && run.get::<String, _>("status") == "waiting_tool"
    {
        let scope = ClientToolRunScope {
            run_id,
            agent_id: run.get("agent_id"),
            owner_id: run.get("owner_id"),
            integration_session_id: run.get("integration_session_id"),
            hub_session_id: scoped.hub_session_id,
            hub_turn_id: run.get("hub_turn_id"),
            client_instance_id: run.get("client_instance_id"),
            client_tool_snapshot: run.get("client_tool_snapshot"),
            widget_session_id: run.get("widget_session_id"),
            external_user_context: run.get("external_user_context"),
            model_subject_type: run.get("model_subject_type"),
            model_subject_user_id: run.get("model_subject_user_id"),
            model_source_integration_app_id: run.get("model_source_integration_app_id"),
        };
        let run = fail_client_tool_batch_tx(
            &mut tx,
            &scope,
            "cancelled",
            "interrupted",
            "client_tool_interrupted",
            "Client Tool batch was stopped",
        )
        .await?;
        tx.commit().await?;
        return Ok(Json(run));
    }
    let run = request_run_interrupt_tx(&mut tx, run_id, scoped.hub_session_id).await?;
    tx.commit().await?;
    Ok(Json(run))
}

/// 在 client 凭证作用域内定位 run（与 stop 端点完全同构：client_instance_id
/// 存在时只允许 widget/integration:tool_result 来源，否则必须属于该 widget 会话）。
async fn locate_widget_run_tx(
    tx: &mut Transaction<'_, Postgres>,
    credential: &WidgetCredential,
    run_id: Uuid,
) -> Result<sqlx::postgres::PgRow, ApiError> {
    sqlx::query(
        "SELECT id, agent_id, owner_id, integration_session_id, hub_session_id,
                hub_turn_id, status, client_instance_id, client_tool_snapshot,
                widget_session_id, external_user_context, model_subject_type,
                model_subject_user_id, model_source_integration_app_id
         FROM runs
         WHERE id = $1 AND agent_id = $2 AND owner_id = $3
           AND (($4::boolean = true
                 AND widget_session_id = $5
                 AND source IN ('widget', 'integration:tool_result'))
                OR ($4::boolean = false AND widget_session_id = $5))",
    )
    .bind(run_id)
    .bind(credential.agent_id)
    .bind(credential.owner_id)
    .bind(credential.client_instance_id.is_some())
    .bind(credential.id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("run not found"))
}

/// Client 侧强制停止（第三方/external 会话也允许）：按 client 凭证作用域
/// 定位 run 后执行与控制台一致的 force-stop 核心（杀 Pi + 快照上传）。
pub(crate) async fn force_stop_widget_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(run_id): Path<Uuid>,
    Json(req): Json<ForceStopRequest>,
) -> Result<(StatusCode, Json<ForceStopOperationDto>), ApiError> {
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, &headers).await?;
    let run = locate_widget_run_tx(&mut tx, &credential, run_id).await?;
    let scoped = load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        run.get("integration_session_id"),
        Some(run.get("hub_session_id")),
        true,
    )
    .await?;
    let request_id = req.request_id.trim().to_owned();
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(ApiError::bad_request(
            "request_id is required (<=128 chars)",
        ));
    }
    let (dto, created) = crate::api::sessions::force_stop_run_core_tx(
        &mut tx,
        run_id,
        scoped.hub_session_id,
        &request_id,
        req.expected_generation,
        true, // client 凭证作用域内的会话（含 external）允许强制停止。
    )
    .await?;
    // 新建的 operation 必须绑定确定的目标 runtime，否则回滚。
    if created && dto.target_runtime_id.is_none() {
        return Err(ApiError::internal(
            "force stop operation was created without a target runtime",
        ));
    }
    tx.commit().await?;
    let status = if matches!(
        dto.state.as_str(),
        "succeeded" | "snapshot_lost" | "abandoned"
    ) {
        StatusCode::OK
    } else {
        StatusCode::ACCEPTED
    };
    if created {
        if let Some(target_runtime) = dto.target_runtime_id {
            crate::runtime_ws::push_force_stop_command(
                &state,
                target_runtime,
                dto.operation_id,
                scoped.hub_session_id,
                run_id,
            )
            .await;
        }
    }
    Ok((status, Json(dto)))
}

pub(crate) async fn create_integration_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateIntegrationSessionRequest>,
) -> Result<Json<IntegrationSessionDto>, ApiError> {
    let principal = require_integration(&state, &headers, req.agent_id).await?;
    validate_tool_definitions(&req.tools)?;
    let external_user_id = normalize_external_user_id(&req.external_user_id)?;
    let metadata = if req.metadata.is_object() {
        req.metadata
    } else {
        return Err(ApiError::bad_request("metadata must be a JSON object"));
    };
    let tenant_id = require_origin_tenant(req.tenant_id.as_deref())?;
    let mut tx = state.pool.begin().await?;
    lock_active_integration_agent_tx(&mut tx, principal.agent_id, principal.agent_owner_id).await?;
    let resolved = if principal.grant_type == "authorization_code" {
        let subject_user_id = principal
            .subject_user_id
            .ok_or(ApiError::unauthorized("invalid user application token"))?;
        let origin_tenant_id = principal
            .origin_tenant_id
            .as_deref()
            .ok_or(ApiError::unauthorized("invalid user application token"))?;
        let identity_id = principal
            .origin_external_identity_id
            .ok_or(ApiError::unauthorized("invalid user application token"))?;
        if origin_tenant_id != tenant_id {
            return Err(ApiError::forbidden(
                "application token is bound to another tenant",
            ));
        }
        let identity_matches: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM external_identities
                 WHERE id = $1 AND platform_id = $2 AND tenant_id = $3
                   AND external_user_id = $4 AND user_id = $5
             )",
        )
        .bind(identity_id)
        .bind(principal.external_platform_id)
        .bind(&tenant_id)
        .bind(&external_user_id)
        .bind(subject_user_id)
        .fetch_one(&mut *tx)
        .await?;
        if !identity_matches {
            return Err(ApiError::forbidden(
                "application token is bound to another external identity",
            ));
        }
        ResolvedExternalIdentity {
            user: load_active_user_tx(&mut tx, subject_user_id).await?,
            identity_id,
        }
    } else {
        let email = req
            .email
            .as_deref()
            .ok_or(ApiError::bad_request("trusted email is required"))?;
        let profile = normalize_widget_user_profile(WidgetUserProfileDto {
            username: req.username.clone(),
            display_name: req.display_name.clone(),
            email: Some(email.to_owned()),
            attributes: json!({}),
        })?;
        let resolved = resolve_external_identity_tx(
            &mut tx,
            principal.external_platform_id,
            principal.authentication_channel_id,
            &tenant_id,
            &external_user_id,
            profile.email.as_deref(),
            profile.username.as_deref(),
        )
        .await?;
        update_external_identity_widget_profile_tx(&mut tx, resolved.identity_id, &profile).await?;
        resolved
    };
    let hub_session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, origin_platform_id,
              origin_tenant_id, origin_external_identity_id, lifecycle_status)
         VALUES ($1, $2, $3, 'external', $4, $5, $6, 'waiting_for_runtime')",
    )
    .bind(hub_session_id)
    .bind(resolved.user.id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(&tenant_id)
    .bind(resolved.identity_id)
    .execute(&mut *tx)
    .await?;
    let integration_session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO integration_sessions
             (id, oauth_app_id, agent_id, owner_id, external_user_id,
              tool_definitions, metadata, hub_session_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(integration_session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(resolved.user.id)
    .bind(&external_user_id)
    .bind(req.tools)
    .bind(metadata)
    .bind(hub_session_id)
    .execute(&mut *tx)
    .await?;
    let session = load_integration_session_tx(&mut tx, integration_session_id, &principal).await?;
    tx.commit().await?;
    Ok(Json(session))
}

pub(crate) async fn get_integration_session(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<Json<IntegrationSessionDto>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    Ok(Json(
        load_integration_session(&state.pool, session_id, &principal).await?,
    ))
}

pub(crate) async fn create_integration_message(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Json(req): Json<CreateIntegrationMessageRequest>,
) -> Result<Json<IntegrationMessageResponse>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    validate_integration_attachments(&req.attachments)?;
    let mut tx = state.pool.begin().await?;
    // 所有 Integration 写入都保持 Agent -> session 的固定锁顺序。
    lock_active_integration_agent_tx(&mut tx, principal.agent_id, principal.agent_owner_id).await?;
    let session = sqlx::query(
        "SELECT integration.id, integration.hub_session_id, integration.owner_id
         FROM integration_sessions AS integration
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.id = $1 AND integration.oauth_app_id = $2
           AND integration.agent_id = $3
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   integration.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )
         FOR UPDATE OF integration",
    )
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("integration session not found"))?;
    let hub_session_id: Uuid = session.get("hub_session_id");
    let external_owner_id: Uuid = session.get("owner_id");
    let parent_run_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM runs
         WHERE hub_session_id = $1 AND status IN ('completed', 'waiting_tool')
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(hub_session_id)
    .fetch_optional(&mut *tx)
    .await?;
    let model_attribution = integration_run_model_attribution(&principal);
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id: hub_session_id,
            agent_id: principal.agent_id,
            owner_id: external_owner_id,
            content: req.content.clone(),
            payload: json!({ "attachments": req.attachments }),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: req.client_message_key.clone(),
            source: "integration:message".into(),
            automation_id: None,
            integration_session_id: Some(session_id),
            parent_run_id,
            continuation_turn_id: None,
            model_subject_type: model_attribution.subject_type.into(),
            model_subject_user_id: model_attribution.subject_user_id,
            model_source_integration_app_id: model_attribution.source_integration_app_id,
            external_user_context: None,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    let run = accepted.run.ok_or(ApiError::internal(
        "integration message did not schedule a run",
    ))?;
    let inserted = sqlx::query(
        "INSERT INTO integration_messages
             (id, session_id, run_id, role, content, attachments,
              client_message_key, hub_message_id)
         VALUES ($1, $2, $3, 'user', $4, $5, $6, $7)
         ON CONFLICT DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(session_id)
    .bind(run.id)
    .bind(req.content.trim())
    .bind(&req.attachments)
    .bind(req.client_message_key.as_deref().map(str::trim))
    .bind(accepted.message.id)
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() == 1 {
        insert_integration_attachments_tx(
            &mut tx,
            session_id,
            run.id,
            accepted.message.id,
            &req.attachments,
        )
        .await?;
    }
    tx.commit().await?;
    Ok(Json(IntegrationMessageResponse {
        run,
        message: accepted.message,
    }))
}

pub(crate) async fn stop_integration_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((session_id, run_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<RunDto>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    let mut tx = state.pool.begin().await?;
    lock_active_integration_agent_tx(&mut tx, principal.agent_id, principal.agent_owner_id).await?;
    let hub_session_id: Uuid = sqlx::query_scalar(
        "SELECT integration.hub_session_id
         FROM integration_sessions AS integration
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.id = $1 AND integration.oauth_app_id = $2
           AND integration.agent_id = $3
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   integration.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )
         FOR UPDATE OF integration",
    )
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ApiError::not_found("integration session not found"))?;
    let run = request_run_interrupt_tx(&mut tx, run_id, hub_session_id).await?;
    tx.commit().await?;
    Ok(Json(run))
}

pub(crate) async fn list_integration_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
) -> Result<Json<Vec<HubSessionMessageDto>>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    let rows = sqlx::query(
        "SELECT message.id, message.session_id, message.sequence, message.role,
                message.message_kind, message.content, message.payload,
                message.delivery_mode, message.delivery_state,
                message.client_message_key, message.expected_native_turn_id,
                message.turn_id, message.run_id, message.accepted_at
         FROM hub_session_messages AS message
         JOIN integration_sessions AS integration
           ON integration.hub_session_id = message.session_id
         JOIN oauth_apps AS app ON app.id = integration.oauth_app_id
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.id = $1 AND integration.oauth_app_id = $2
           AND integration.agent_id = $3 AND app.external_platform_id = $4
           AND hub.origin_kind = 'external' AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   integration.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )
         ORDER BY message.sequence",
    )
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_all(&state.pool)
    .await?;
    let mut messages = rows
        .into_iter()
        .map(hub_message_from_row)
        .collect::<Vec<_>>();
    fill_message_attachments(&state.pool, &mut messages).await?;
    Ok(Json(messages))
}

#[derive(Debug, Deserialize)]
pub(crate) struct IntegrationEventsQuery {
    pub(crate) after: Option<i64>,
}

pub(crate) async fn list_integration_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<IntegrationEventsQuery>,
) -> Result<Json<Vec<RunEventDto>>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    load_integration_session(&state.pool, session_id, &principal).await?;
    Ok(Json(
        load_integration_events_after(
            &state.pool,
            session_id,
            query.after.unwrap_or(0),
            &principal,
        )
        .await?,
    ))
}

pub(crate) async fn stream_integration_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(session_id): Path<Uuid>,
    Query(query): Query<IntegrationEventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let agent_id = integration_session_agent_id(&state.pool, session_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    load_integration_session(&state.pool, session_id, &principal).await?;
    let stream_state = state.clone();
    let stream_headers = headers.clone();
    let mut last_seq = query.after.unwrap_or(0);
    let event_stream = stream! {
        loop {
            let principal = match require_integration(&stream_state, &stream_headers, agent_id).await {
                Ok(principal) => principal,
                Err(err) => {
                    yield Ok(Event::default().event("error").data(err.message));
                    break;
                }
            };
            if let Err(err) = load_integration_session(&stream_state.pool, session_id, &principal).await {
                yield Ok(Event::default().event("error").data(err.message));
                break;
            }
            match load_integration_events_after(
                &stream_state.pool,
                session_id,
                last_seq,
                &principal,
            ).await {
                Ok(events) => {
                    for event in events {
                        last_seq = event.seq;
                        let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
                        yield Ok(Event::default().event("integration_event").id(event.seq.to_string()).data(payload));
                    }
                }
                Err(err) => {
                    yield Ok(Event::default().event("error").data(err.message));
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(700)).await;
        }
    };
    Ok(Sse::new(event_stream).keep_alive(KeepAlive::default()))
}

pub(crate) async fn submit_integration_tool_result(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(tool_request_id): Path<Uuid>,
    Json(req): Json<SubmitToolResultRequest>,
) -> Result<Json<SubmitToolResultResponse>, ApiError> {
    let agent_id = integration_tool_request_agent_id(&state.pool, tool_request_id).await?;
    let principal = require_integration(&state, &headers, agent_id).await?;
    let settings = load_system_settings(&state.pool).await?;
    let validation = validate_tool_result(&req.result, settings.max_tool_result_bytes)?;
    let run_id =
        sqlx::query_scalar::<_, Uuid>("SELECT run_id FROM integration_tool_requests WHERE id = $1")
            .bind(tool_request_id)
            .fetch_one(&state.pool)
            .await?;
    let archived = archive_tool_result(&state, run_id, tool_request_id, &validation).await;
    let result = sanitize_run_event_payload(validation.payload);
    let mut tx = state.pool.begin().await?;
    // Integration 写事务统一先锁 Agent，避免与归档形成反向锁顺序。
    lock_active_integration_agent_tx(&mut tx, principal.agent_id, principal.agent_owner_id).await?;
    let locked_session_id =
        lock_tool_request_session_for_update(&mut tx, tool_request_id, &principal).await?;
    let tool_request =
        load_tool_request_for_update(&mut tx, tool_request_id, locked_session_id, &principal)
            .await?;
    if tool_request.status == "completed" {
        let follow_up_run_id = tool_request
            .follow_up_run_id
            .ok_or(ApiError::internal("tool request follow-up run is missing"))?;
        let run = load_run_public_tx(&mut tx, follow_up_run_id).await?;
        tx.commit().await?;
        return Ok(Json(SubmitToolResultResponse { run, tool_request }));
    }
    if tool_request.status == "timed_out" {
        return Err(ApiError::gone("tool request expired"));
    }
    if tool_request.status != "pending" {
        return Err(ApiError::forbidden("tool request is not pending"));
    }
    if tool_request.expires_at <= Utc::now() {
        sqlx::query(
            "UPDATE integration_tool_requests
             SET status = 'timed_out', responded_at = now()
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(tool_request.id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Err(ApiError::gone("tool request expired"));
    }
    let integration_session_id = tool_request.session_id.ok_or(ApiError::forbidden(
        "tool request is not an Integration request",
    ))?;
    let integration_session =
        load_integration_session_tx(&mut tx, integration_session_id, &principal).await?;
    let original_run = load_run_public_tx(&mut tx, tool_request.run_id).await?;
    let client_managed: bool =
        sqlx::query_scalar("SELECT client_instance_id IS NOT NULL FROM runs WHERE id = $1")
            .bind(tool_request.run_id)
            .fetch_one(&mut *tx)
            .await?;
    if client_managed {
        return Err(ApiError::forbidden(
            "Client Tool results require a Client Access Credential",
        ));
    }
    if original_run.hub_session_id != Some(integration_session.hub_session_id) {
        return Err(ApiError::conflict(
            "tool request run belongs to another session",
        ));
    }
    let active_run: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM runs
         WHERE hub_session_id = $1 AND id <> $2 AND status IN ('pending', 'running')
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(integration_session.hub_session_id)
    .bind(tool_request.run_id)
    .fetch_optional(&mut *tx)
    .await?;
    if active_run.is_some() {
        return Err(ApiError::conflict(
            "integration session already has an active run",
        ));
    }
    let content = format!(
        "Tool result for {}: {}",
        tool_request.tool_name,
        compact_json(&result)
    );
    let external_user_context = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT external_user_context FROM runs WHERE id = $1",
    )
    .bind(tool_request.run_id)
    .fetch_one(&mut *tx)
    .await?
    .map(serde_json::from_value::<ExternalUserContextDto>)
    .transpose()
    .map_err(|_| ApiError::internal("Run external user context is invalid"))?;
    let model_attribution = integration_run_model_attribution(&principal);
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id: integration_session.hub_session_id,
            agent_id: principal.agent_id,
            owner_id: integration_session.owner_id,
            content,
            payload: json!({
                "tool_request_id": tool_request.id,
                "result": result.clone()
            }),
            role: "tool".into(),
            message_kind: "tool_result".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: Some(format!("tool-result:{}", tool_request.id)),
            source: "integration:tool_result".into(),
            automation_id: None,
            integration_session_id: tool_request.session_id,
            parent_run_id: Some(tool_request.run_id),
            continuation_turn_id: None,
            model_subject_type: model_attribution.subject_type.into(),
            model_subject_user_id: model_attribution.subject_user_id,
            model_source_integration_app_id: model_attribution.source_integration_app_id,
            external_user_context,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    let run = accepted
        .run
        .ok_or(ApiError::internal("tool result did not schedule a run"))?;
    let result_event_id: Uuid = sqlx::query_scalar(
        "SELECT event_id FROM run_events
         WHERE run_id = $1 AND hub_message_id = $2 AND event_type = 'tool_result'",
    )
    .bind(run.id)
    .bind(accepted.message.id)
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE integration_tool_requests
         SET status = 'completed', result_payload = $1, result_event_id = $2, follow_up_run_id = $3,
             artifact_id = $4, artifact_size_bytes = $5, artifact_reason = $6,
             result_truncated = $7, responded_at = now()
         WHERE id = $8 AND status = 'pending'",
    )
    .bind(&result)
    .bind(result_event_id)
    .bind(run.id)
    .bind(archived.as_ref().and_then(|archived| archived.0))
    .bind(archived.as_ref().map(|archived| archived.1))
    .bind(archived.as_ref().and_then(|archived| archived.2.as_ref()))
    .bind(validation.truncated)
    .bind(tool_request.id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Json(SubmitToolResultResponse {
        run,
        tool_request: load_tool_request(&state.pool, tool_request_id, &principal).await?,
    }))
}

#[derive(Debug)]
pub(crate) struct ClientToolRunScope {
    pub(crate) run_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) owner_id: Uuid,
    pub(crate) integration_session_id: Option<Uuid>,
    pub(crate) hub_session_id: Uuid,
    pub(crate) hub_turn_id: Uuid,
    pub(crate) client_instance_id: Uuid,
    pub(crate) client_tool_snapshot: Value,
    pub(crate) widget_session_id: Option<Uuid>,
    pub(crate) external_user_context: Option<Value>,
    pub(crate) model_subject_type: String,
    pub(crate) model_subject_user_id: Option<Uuid>,
    pub(crate) model_source_integration_app_id: Option<Uuid>,
}

pub(crate) async fn claim_client_tool_call(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(tool_call_id): Path<Uuid>,
) -> Result<Json<ClientToolClaimResponse>, ApiError> {
    let started_at = std::time::Instant::now();
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing Client Access Credential"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, &headers).await?;
    let (scope, batch) = lock_client_tool_batch_tx(&mut tx, &credential, tool_call_id).await?;
    let request = batch
        .iter()
        .find(|row| row.get::<Uuid, _>("id") == tool_call_id)
        .ok_or(ApiError::not_found("Client Tool call not found"))?;
    let status: String = request.get("status");
    if request.get::<DateTime<Utc>, _>("expires_at") <= Utc::now()
        && matches!(status.as_str(), "pending" | "claimed" | "unknown")
    {
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
        return Err(ApiError::gone("Client Tool call timed out"));
    }
    let response = match status.as_str() {
        "pending" => {
            let updated = sqlx::query(
                "UPDATE integration_tool_requests
                 SET status = 'claimed', claimed_by_client_instance_id = $1,
                     claimed_at = COALESCE(claimed_at, now())
                 WHERE id = $2 AND run_id = $3 AND status = 'pending'
                 RETURNING status",
            )
            .bind(scope.client_instance_id)
            .bind(tool_call_id)
            .bind(scope.run_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ApiError::conflict("Client Tool call could not be claimed"))?;
            ClientToolClaimResponse {
                status: updated.get("status"),
                terminal: false,
                result: None,
            }
        }
        "claimed" => {
            if request.get::<Option<Uuid>, _>("claimed_by_client_instance_id")
                != Some(scope.client_instance_id)
            {
                return Err(ApiError::forbidden(
                    "Client Tool call belongs to another Client Instance",
                ));
            }
            ClientToolClaimResponse {
                status,
                terminal: false,
                result: None,
            }
        }
        "completed" | "timed_out" | "unknown" | "cancelled" => ClientToolClaimResponse {
            status,
            terminal: true,
            result: request
                .get::<Option<Value>, _>("result_payload")
                .map(|payload| {
                    // result_payload 是自包含包装层，取内层 result 反序列化。
                    let inner = payload.get("result").cloned().unwrap_or(payload);
                    serde_json::from_value(inner)
                })
                .transpose()
                .map_err(|_| ApiError::internal("stored Client Tool result is invalid"))?,
        },
        _ => return Err(ApiError::internal("stored Client Tool status is invalid")),
    };
    let session_id: Option<Uuid> = batch.first().and_then(|row| row.get("hub_session_id"));
    tx.commit().await?;
    tracing::info!(
        %tool_call_id,
        ?session_id,
        status = %response.status,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "client tool call claimed"
    );
    Ok(Json(response))
}

pub(crate) async fn submit_client_tool_result(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(tool_call_id): Path<Uuid>,
    Json(req): Json<SubmitClientToolResultRequest>,
) -> Result<Json<SubmitClientToolResultResponse>, ApiError> {
    let started_at = std::time::Instant::now();
    let settings = load_system_settings(&state.pool).await?;
    let (validation, checksum) =
        validate_client_tool_result(&req.result, settings.max_tool_result_bytes)?;
    // NUL bytes are rejected by PostgreSQL jsonb; sanitize before persisting
    // while keeping the checksum over the client's original payload so an
    // idempotent resubmit still matches.
    // 超阈值先归档全文（事务外、指数退避），超硬上限则仅截断不归档。
    let request_row =
        sqlx::query("SELECT run_id, tool_name FROM integration_tool_requests WHERE id = $1")
            .bind(tool_call_id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or(ApiError::not_found("Client Tool call not found"))?;
    let run_id: Uuid = request_row.get("run_id");
    let tool_name: String = request_row.get("tool_name");
    let archived = if validation.truncated {
        archive_tool_result(&state, run_id, tool_call_id, &validation).await
    } else {
        None
    };
    // 截断时保留合法 ClientToolResultDto 外壳：续接 run 会按 serde tag="status"
    // 解析 result，纯 {truncated, content} 会反序列化失败导致 submit 500
    // （无日志 internal）。output 直接放前 32KB 截断文本，result 内 truncated
    // 与 artifact_ref（S3 归档位置）标识截断与完整内容。
    // result_payload 存自包含包装层 {tool_call_id, tool_name, result}（与
    // ClientToolContinuationResultDto 同构）；事件写入只用内层 result，
    // 保持 event.result.status 协议不变。身份以行列（id/tool_name）权威。
    let inner_result = if validation.truncated {
        let content = validation
            .payload
            .get("content")
            .cloned()
            .unwrap_or_else(|| Value::String(String::new()));
        let mut payload = serde_json::to_value(&req.result)
            .map_err(|_| ApiError::bad_request("Client Tool result must be JSON"))?;
        match payload.get("status").and_then(Value::as_str) {
            Some("success") => {
                payload["output"] = content;
            }
            Some("error") => {
                let code = payload
                    .pointer("/error/code")
                    .cloned()
                    .unwrap_or_else(|| json!("tool_result_truncated"));
                let retryable = payload
                    .pointer("/error/retryable")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                payload["error"] = json!({
                    "code": code,
                    "message": content.clone(),
                    "retryable": retryable,
                });
            }
            _ => {
                // serde tag 反序列化已保证 status ∈ {success, error}，此分支不可达；
                // 用 400 而非 internal，避免无日志 500。
                return Err(ApiError::bad_request(
                    "Client Tool result has an unknown status variant",
                ));
            }
        }
        payload["truncated"] = json!(true);
        if let Some((Some(artifact_id), _, _)) = &archived {
            payload["artifact_ref"] = json!(format!("tool-results/{run_id}/{artifact_id}"));
        }
        payload
    } else {
        validation.payload
    };
    let inner_result = sanitize_run_event_payload(inner_result);
    let result_payload = json!({
        "tool_call_id": tool_call_id,
        "tool_name": tool_name,
        "result": inner_result.clone(),
    });
    let token = client_access_token_from_headers(&headers)
        .ok_or(ApiError::unauthorized("missing Client Access Credential"))?;
    let mut tx = state.pool.begin().await?;
    let credential = lock_widget_credential_tx(&mut tx, &token, &headers).await?;
    let (scope, batch) = lock_client_tool_batch_tx(&mut tx, &credential, tool_call_id).await?;
    let request = batch
        .iter()
        .find(|row| row.get::<Uuid, _>("id") == tool_call_id)
        .ok_or(ApiError::not_found("Client Tool call not found"))?;
    if request.get::<String, _>("tool_name") != tool_name {
        // 理论不可达（工具名不可变）；用 409 而非 internal，避免无日志 500。
        return Err(ApiError::conflict(
            "Client Tool name changed while submitting",
        ));
    }
    let status: String = request.get("status");
    if status == "completed" {
        if request
            .get::<Option<String>, _>("result_checksum_sha256")
            .as_deref()
            != Some(checksum.as_str())
        {
            return Err(ApiError::conflict(
                "Client Tool result does not match the completed result",
            ));
        }
        let follow_up_run_id: Option<Uuid> = request.get("follow_up_run_id");
        let run = match follow_up_run_id {
            Some(run_id) => Some(load_run_public_tx(&mut tx, run_id).await?),
            None => None,
        };
        let tool_request = load_client_tool_request_tx(&mut tx, tool_call_id).await?;
        tx.commit().await?;
        tracing::info!(
            %tool_call_id,
            elapsed_ms = started_at.elapsed().as_millis() as u64,
            "client tool result resubmitted (idempotent)"
        );
        return Ok(Json(SubmitClientToolResultResponse { run, tool_request }));
    }
    if matches!(status.as_str(), "timed_out" | "unknown" | "cancelled") {
        return Err(ApiError::gone("Client Tool call is terminal"));
    }
    if request.get::<DateTime<Utc>, _>("expires_at") <= Utc::now() {
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
        return Err(ApiError::gone("Client Tool call timed out"));
    }
    if status != "claimed" {
        return Err(ApiError::forbidden(
            "Client Tool call must be claimed before execution",
        ));
    }
    if request.get::<Option<Uuid>, _>("claimed_by_client_instance_id")
        != Some(scope.client_instance_id)
    {
        return Err(ApiError::forbidden(
            "Client Tool call belongs to another Client Instance",
        ));
    }

    let elapsed_ms = Utc::now()
        .signed_duration_since(request.get::<DateTime<Utc>, _>("created_at"))
        .num_milliseconds()
        .max(0);
    let result_event = insert_run_event_tx(
        &mut tx,
        scope.run_id,
        "client_tool_result".into(),
        Some("tool".into()),
        None,
        json!({
            "tool_call_id": tool_call_id,
            "tool_name": request.get::<String, _>("tool_name"),
            "result": inner_result.clone(),
            "elapsed_ms": elapsed_ms,
        }),
    )
    .await?;
    let updated = sqlx::query(
        "UPDATE integration_tool_requests
         SET status = 'completed', result_payload = $1,
             result_checksum_sha256 = $2, result_event_id = $3,
             artifact_id = $4, artifact_size_bytes = $5, artifact_reason = $6,
             result_truncated = $7,
             responded_at = now()
         WHERE id = $8 AND run_id = $9 AND status = 'claimed'
           AND claimed_by_client_instance_id = $10",
    )
    .bind(&result_payload)
    .bind(&checksum)
    .bind(result_event.event_id)
    .bind(archived.as_ref().and_then(|archived| archived.0))
    .bind(archived.as_ref().map(|archived| archived.1))
    .bind(archived.as_ref().and_then(|archived| archived.2.as_ref()))
    .bind(validation.truncated)
    .bind(tool_call_id)
    .bind(scope.run_id)
    .bind(scope.client_instance_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::conflict("Client Tool result was not accepted"));
    }
    let incomplete: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM integration_tool_requests
         WHERE run_id = $1 AND status <> 'completed'",
    )
    .bind(scope.run_id)
    .fetch_one(&mut *tx)
    .await?;
    let run = if incomplete == 0 {
        Some(create_client_tool_continuation_tx(&mut tx, &scope).await?)
    } else {
        None
    };
    let tool_request = load_client_tool_request_tx(&mut tx, tool_call_id).await?;
    tx.commit().await?;
    tracing::info!(
        %tool_call_id,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "client tool result submitted"
    );
    Ok(Json(SubmitClientToolResultResponse { run, tool_request }))
}

pub(crate) fn validate_client_tool_result(
    result: &ClientToolResultDto,
    max_tool_result_bytes: i64,
) -> Result<(ToolResultValidation, String), ApiError> {
    let value = serde_json::to_value(result)
        .map_err(|_| ApiError::bad_request("Client Tool result must be JSON"))?;
    let checksum = sha256_hex(&canonical_json(&value));
    let validation = validate_tool_result_value(&value, max_tool_result_bytes)?;
    Ok((validation, checksum))
}

pub(crate) async fn lock_client_tool_batch_tx(
    tx: &mut Transaction<'_, Postgres>,
    credential: &WidgetCredential,
    tool_call_id: Uuid,
) -> Result<(ClientToolRunScope, Vec<sqlx::postgres::PgRow>), ApiError> {
    let client_instance_id = credential.client_instance_id.ok_or(ApiError::forbidden(
        "legacy Widget credentials cannot execute Client Tools",
    ))?;
    let preview = sqlx::query(
        "SELECT request.run_id, run.integration_session_id, run.hub_session_id
         FROM integration_tool_requests AS request
         JOIN runs AS run ON run.id = request.run_id
         WHERE request.id = $1 AND run.agent_id = $2 AND run.owner_id = $3
           AND run.client_instance_id IS NOT NULL",
    )
    .bind(tool_call_id)
    .bind(credential.agent_id)
    .bind(credential.owner_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("Client Tool call not found"))?;
    let run_id: Uuid = preview.get("run_id");
    let integration_session_id: Option<Uuid> = preview.get("integration_session_id");
    let hub_session_id: Uuid = preview.get("hub_session_id");
    load_widget_scoped_session_tx(
        tx,
        credential,
        integration_session_id,
        Some(hub_session_id),
        true,
    )
    .await?;
    let run = sqlx::query(
        "SELECT id, agent_id, owner_id, integration_session_id, hub_session_id,
                hub_turn_id, status, client_instance_id, client_tool_snapshot,
                widget_session_id, external_user_context, model_subject_type,
                model_subject_user_id, model_source_integration_app_id
         FROM runs
         WHERE id = $1 AND agent_id = $2 AND owner_id = $3
           AND hub_session_id = $4 AND source IN ('widget', 'integration:tool_result')
           AND client_instance_id IS NOT NULL
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(credential.agent_id)
    .bind(credential.owner_id)
    .bind(hub_session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("Client Tool Run not found"))?;
    let executor: Uuid = run.get("client_instance_id");
    if executor != client_instance_id {
        return Err(ApiError::forbidden(
            "Client Tool Run belongs to another Client Instance",
        ));
    }
    let batch = sqlx::query(
        "SELECT id, session_id, hub_session_id, run_id, position, tool_name,
                arguments, status, claimed_by_client_instance_id, claimed_at,
                result_payload, result_checksum_sha256, result_event_id,
                expires_at, responded_at, created_at, follow_up_run_id
         FROM integration_tool_requests
         WHERE run_id = $1 AND hub_session_id = $2
         ORDER BY position
         FOR UPDATE",
    )
    .bind(run_id)
    .bind(hub_session_id)
    .fetch_all(&mut **tx)
    .await?;
    let target = batch
        .iter()
        .find(|row| row.get::<Uuid, _>("id") == tool_call_id)
        .ok_or(ApiError::not_found("Client Tool call not found"))?;
    let tool_name: String = target.get("tool_name");
    let snapshot: Value = run.get("client_tool_snapshot");
    let registered = snapshot.as_array().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| tool.get("name").and_then(Value::as_str) == Some(tool_name.as_str()))
    });
    if !registered {
        return Err(ApiError::forbidden(
            "Client Tool is not present in the Run snapshot",
        ));
    }
    Ok((
        ClientToolRunScope {
            run_id,
            agent_id: run.get("agent_id"),
            owner_id: run.get("owner_id"),
            integration_session_id,
            hub_session_id,
            hub_turn_id: run.get("hub_turn_id"),
            client_instance_id: executor,
            client_tool_snapshot: snapshot,
            widget_session_id: run.get("widget_session_id"),
            external_user_context: run.get("external_user_context"),
            model_subject_type: run.get("model_subject_type"),
            model_subject_user_id: run.get("model_subject_user_id"),
            model_source_integration_app_id: run.get("model_source_integration_app_id"),
        },
        batch,
    ))
}

pub(crate) async fn create_client_tool_continuation_tx(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ClientToolRunScope,
) -> Result<RunDto, ApiError> {
    let rows = sqlx::query(
        "SELECT id, tool_name, result_payload
         FROM integration_tool_requests
         WHERE run_id = $1 AND status = 'completed'
         ORDER BY position",
    )
    .bind(scope.run_id)
    .fetch_all(&mut **tx)
    .await?;
    let results = rows
        .into_iter()
        .map(|row| {
            let payload: Value = row.get("result_payload");
            // 自包含包装层 {tool_call_id, tool_name, result}，身份以行列
            // （id/tool_name）权威；部署前数据迁移已保证无旧纯 DTO 行。
            let result = serde_json::from_value(
                payload
                    .get("result")
                    .ok_or_else(|| ApiError::internal("stored Client Tool result is missing"))?
                    .clone(),
            )
            .map_err(|_| ApiError::internal("stored Client Tool result is invalid"))?;
            Ok(ClientToolContinuationResultDto {
                tool_call_id: row.get("id"),
                tool_name: row.get("tool_name"),
                result,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let external_user_context = scope
        .external_user_context
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| ApiError::internal("Run external user context is invalid"))?;
    let parent_updated = sqlx::query(
        "UPDATE runs
         SET status = 'completed', updated_at = now()
         WHERE id = $1 AND hub_session_id = $2 AND hub_turn_id = $3
           AND status = 'waiting_tool'",
    )
    .bind(scope.run_id)
    .bind(scope.hub_session_id)
    .bind(scope.hub_turn_id)
    .execute(&mut **tx)
    .await?;
    if parent_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "Client Tool parent Run is no longer waiting",
        ));
    }
    insert_run_event_tx(
        tx,
        scope.run_id,
        "status".into(),
        None,
        Some("completed".into()),
        json!({ "status": "completed", "reason": "client_tool_batch_completed" }),
    )
    .await?;
    let turn_updated = sqlx::query(
        "UPDATE hub_session_turns
         SET status = 'completed', ended_at = COALESCE(ended_at, now()),
             updated_at = now()
         WHERE id = $1 AND session_id = $2 AND status = 'waiting_tool'",
    )
    .bind(scope.hub_turn_id)
    .bind(scope.hub_session_id)
    .execute(&mut **tx)
    .await?;
    if turn_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "Client Tool parent Turn is no longer waiting",
        ));
    }
    let session_updated = sqlx::query(
        "UPDATE hub_sessions
         SET active_turn_id = NULL
         WHERE id = $1 AND (active_turn_id IS NULL OR active_turn_id = $2)",
    )
    .bind(scope.hub_session_id)
    .bind(scope.hub_turn_id)
    .execute(&mut **tx)
    .await?;
    if session_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "Client Tool Session changed while continuing",
        ));
    }
    let accepted = accept_session_message_tx(
        tx,
        AcceptSessionMessage {
            session_id: scope.hub_session_id,
            agent_id: scope.agent_id,
            owner_id: scope.owner_id,
            content: "Client Tool batch completed".into(),
            payload: json!({ "tool_results": results }),
            role: "tool".into(),
            message_kind: "tool_result".into(),
            requested_delivery_mode: "next_turn".into(),
            client_message_key: Some(format!("client-tool-batch:{}", scope.run_id)),
            source: "integration:tool_result".into(),
            automation_id: None,
            integration_session_id: scope.integration_session_id,
            parent_run_id: Some(scope.run_id),
            continuation_turn_id: None,
            model_subject_type: scope.model_subject_type.clone(),
            model_subject_user_id: scope.model_subject_user_id,
            model_source_integration_app_id: scope.model_source_integration_app_id,
            external_user_context,
            attachment_ids: Vec::new(),
        },
    )
    .await?;
    let run = accepted.run.ok_or(ApiError::internal(
        "Client Tool results did not schedule a continuation",
    ))?;
    let external_user_context = scope.external_user_context.clone();
    let continuation_updated = sqlx::query(
        "UPDATE runs
         SET parent_run_id = $1, source = 'integration:tool_result',
             integration_session_id = $2, client_instance_id = $3,
             client_tool_snapshot = $4, widget_session_id = $5,
             external_user_context = $6, model_subject_type = $7,
             model_subject_user_id = $8,
             model_source_integration_app_id = $9, updated_at = now()
         WHERE id = $10 AND hub_session_id = $11 AND status = 'pending'",
    )
    .bind(scope.run_id)
    .bind(scope.integration_session_id)
    .bind(scope.client_instance_id)
    .bind(&scope.client_tool_snapshot)
    .bind(scope.widget_session_id)
    .bind(external_user_context)
    .bind(&scope.model_subject_type)
    .bind(scope.model_subject_user_id)
    .bind(scope.model_source_integration_app_id)
    .bind(run.id)
    .bind(scope.hub_session_id)
    .execute(&mut **tx)
    .await?;
    if continuation_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "Client Tool continuation is no longer pending",
        ));
    }
    sqlx::query(
        "UPDATE integration_tool_requests
         SET follow_up_run_id = $1
         WHERE run_id = $2 AND status = 'completed'
           AND follow_up_run_id IS NULL",
    )
    .bind(run.id)
    .bind(scope.run_id)
    .execute(&mut **tx)
    .await?;
    load_run_public_tx(tx, run.id).await
}

pub(crate) async fn fail_client_tool_batch_tx(
    tx: &mut Transaction<'_, Postgres>,
    scope: &ClientToolRunScope,
    request_status: &str,
    run_status: &str,
    event_type: &str,
    message: &str,
) -> Result<RunDto, ApiError> {
    let requests = sqlx::query(
        "UPDATE integration_tool_requests
         SET status = $1, responded_at = COALESCE(responded_at, now())
         WHERE run_id = $2 AND status IN ('pending', 'claimed', 'unknown')
         RETURNING id, tool_name",
    )
    .bind(request_status)
    .bind(scope.run_id)
    .fetch_all(&mut **tx)
    .await?;
    for request in requests {
        insert_run_event_tx(
            tx,
            scope.run_id,
            event_type.into(),
            None,
            Some(message.into()),
            json!({
                "tool_call_id": request.get::<Uuid, _>("id"),
                "tool_name": request.get::<String, _>("tool_name"),
                "status": request_status,
                "message": message,
            }),
        )
        .await?;
    }
    let updated = sqlx::query(
        "UPDATE runs SET status = $1, updated_at = now()
         WHERE id = $2 AND status = 'waiting_tool'",
    )
    .bind(run_status)
    .bind(scope.run_id)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() == 1 {
        insert_run_event_tx(
            tx,
            scope.run_id,
            "status".into(),
            None,
            Some(run_status.into()),
            json!({ "status": run_status, "reason": request_status }),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE hub_session_turns
         SET status = $1, ended_at = COALESCE(ended_at, now()), updated_at = now()
         WHERE id = $2 AND session_id = $3 AND status = 'waiting_tool'",
    )
    .bind(run_status)
    .bind(scope.hub_turn_id)
    .bind(scope.hub_session_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE hub_sessions
         SET active_turn_id = NULL
         WHERE id = $1 AND active_turn_id = $2",
    )
    .bind(scope.hub_session_id)
    .bind(scope.hub_turn_id)
    .execute(&mut **tx)
    .await?;
    load_run_public_tx(tx, scope.run_id).await
}

pub(crate) async fn load_client_tool_request_tx(
    tx: &mut Transaction<'_, Postgres>,
    tool_call_id: Uuid,
) -> Result<IntegrationToolRequestDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, session_id, hub_session_id, run_id, position, tool_name,
                arguments, status, claimed_by_client_instance_id, claimed_at,
                result_payload, follow_up_run_id, expires_at, responded_at, created_at,
                artifact_id, artifact_size_bytes, artifact_reason, result_truncated
         FROM integration_tool_requests WHERE id = $1",
    )
    .bind(tool_call_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("Client Tool call not found"))?;
    Ok(tool_request_from_row(row))
}

pub(crate) async fn insert_hub_native_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
    agent_id: Uuid,
) -> Result<Uuid, ApiError> {
    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, lifecycle_status)
         VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
    )
    .bind(session_id)
    .bind(owner_id)
    .bind(agent_id)
    .execute(&mut **tx)
    .await?;
    Ok(session_id)
}

pub(crate) async fn insert_embed_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    owner_id: Uuid,
    oauth_app_id: Option<Uuid>,
    token: &str,
    expires_at: DateTime<Utc>,
) -> Result<Uuid, ApiError> {
    let hub_session_id = insert_hub_native_session_tx(tx, owner_id, agent_id).await?;
    let embed_session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO embed_sessions
             (token_hash, agent_id, owner_id, oauth_app_id, expires_at, hub_session_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id",
    )
    .bind(sha256_hex(token))
    .bind(agent_id)
    .bind(owner_id)
    .bind(oauth_app_id)
    .bind(expires_at)
    .bind(hub_session_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(embed_session_id)
}

pub(crate) struct WidgetAccessSessionInsert<'a> {
    pub(crate) oauth_app_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) owner_id: Uuid,
    pub(crate) external_identity_id: Uuid,
    pub(crate) external_user: &'a ExternalUserContextDto,
    pub(crate) client_instance_id: Uuid,
    pub(crate) client_tool_definitions: Value,
    pub(crate) token: &'a str,
    pub(crate) expires_at: DateTime<Utc>,
}

pub(crate) async fn insert_widget_access_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: WidgetAccessSessionInsert<'_>,
) -> Result<Uuid, ApiError> {
    let profile_snapshot = serde_json::to_value(session.external_user)
        .map_err(|_| ApiError::internal("external user profile could not be encoded"))?;
    sqlx::query_scalar(
        "INSERT INTO embed_sessions
             (token_hash, agent_id, owner_id, oauth_app_id, expires_at,
              external_tenant_id, external_user_id, external_identity_id,
              profile_snapshot, client_instance_id, client_tool_definitions)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (oauth_app_id, agent_id, external_identity_id, client_instance_id)
             WHERE anonymous = false AND external_identity_id IS NOT NULL
               AND client_instance_id IS NOT NULL
         DO UPDATE SET token_hash = EXCLUDED.token_hash,
                       expires_at = EXCLUDED.expires_at,
                       owner_id = EXCLUDED.owner_id,
                       external_tenant_id = EXCLUDED.external_tenant_id,
                       external_user_id = EXCLUDED.external_user_id,
                       profile_snapshot = EXCLUDED.profile_snapshot,
                       client_tool_definitions = EXCLUDED.client_tool_definitions
         RETURNING id",
    )
    .bind(sha256_hex(session.token))
    .bind(session.agent_id)
    .bind(session.owner_id)
    .bind(session.oauth_app_id)
    .bind(session.expires_at)
    .bind(&session.external_user.tenant_id)
    .bind(&session.external_user.external_user_id)
    .bind(session.external_identity_id)
    .bind(profile_snapshot)
    .bind(session.client_instance_id)
    .bind(session.client_tool_definitions)
    .fetch_one(&mut **tx)
    .await
    .map_err(Into::into)
}

pub(crate) fn normalize_client_message_key(
    value: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "valid client message key is required",
        ));
    }
    Ok(Some(value.to_owned()))
}

pub(crate) fn normalize_origin_tenant(value: Option<&str>) -> Result<String, ApiError> {
    let value = value.map(str::trim).filter(|value| !value.is_empty());
    let value = value.unwrap_or("default");
    if value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "valid external tenant id is required",
        ));
    }
    Ok(value.to_owned())
}

pub(crate) fn require_origin_tenant(value: Option<&str>) -> Result<String, ApiError> {
    let value = value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::bad_request("external tenant id is required"))?;
    normalize_origin_tenant(Some(value))
}

pub(crate) fn normalize_external_user_id(value: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request("valid external user id is required"));
    }
    Ok(value.to_owned())
}

pub(crate) fn widget_client_credentials(headers: &HeaderMap) -> Result<(String, String), ApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .ok_or(ApiError::unauthorized("invalid oauth client"))?;
    let decoded = STANDARD
        .decode(authorization)
        .map_err(|_| ApiError::unauthorized("invalid oauth client"))?;
    let credentials = std::str::from_utf8(&decoded)
        .map_err(|_| ApiError::unauthorized("invalid oauth client"))?;
    let (client_id, client_secret) = credentials
        .split_once(':')
        .ok_or(ApiError::unauthorized("invalid oauth client"))?;
    if client_id.trim().is_empty()
        || client_id.len() > 256
        || client_id.chars().any(char::is_control)
        || client_secret.is_empty()
        || client_secret.len() > 1024
    {
        return Err(ApiError::unauthorized("invalid oauth client"));
    }
    Ok((client_id.to_owned(), client_secret.to_owned()))
}

pub(crate) fn normalize_widget_user_profile(
    mut profile: WidgetUserProfileDto,
) -> Result<WidgetUserProfileDto, ApiError> {
    profile.username = validate_external_username(profile.username.as_deref())?;
    profile.display_name =
        normalize_widget_profile_text(profile.display_name.as_deref(), "display name")?;
    profile.email = profile.email.as_deref().map(normalize_email).transpose()?;
    profile.attributes = normalize_widget_attributes(profile.attributes)?;
    Ok(profile)
}

pub(crate) fn normalize_widget_profile_text(
    value: Option<&str>,
    field: &str,
) -> Result<Option<String>, ApiError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.chars().count() > 256 || value.chars().any(char::is_control) {
        return Err(ApiError::bad_request(format!(
            "valid external {field} is required"
        )));
    }
    Ok(Some(value.to_owned()))
}

pub(crate) fn normalize_widget_attributes(value: Value) -> Result<Value, ApiError> {
    let value = if value.is_null() { json!({}) } else { value };
    if !value.is_object() {
        return Err(ApiError::bad_request(
            "external user attributes must be a JSON object",
        ));
    }
    if serde_json::to_vec(&value)
        .map_err(|_| ApiError::bad_request("external user attributes are invalid"))?
        .len()
        > 8 * 1024
    {
        return Err(ApiError::bad_request(
            "external user attributes are too large",
        ));
    }
    let mut values = 0;
    validate_widget_attribute_value(&value, 0, &mut values)?;
    Ok(value)
}

pub(crate) fn validate_widget_attribute_value(
    value: &Value,
    depth: usize,
    value_count: &mut usize,
) -> Result<(), ApiError> {
    if depth > 4 {
        return Err(ApiError::bad_request(
            "external user attributes are nested too deeply",
        ));
    }
    *value_count += 1;
    if *value_count > 128 {
        return Err(ApiError::bad_request(
            "external user attributes contain too many values",
        ));
    }
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
        Value::String(value) => {
            if value.chars().count() > 1024 || value.chars().any(char::is_control) {
                return Err(ApiError::bad_request(
                    "external user attribute text is invalid",
                ));
            }
            Ok(())
        }
        Value::Array(items) => {
            if items.len() > 32 {
                return Err(ApiError::bad_request(
                    "external user attributes contain too many values",
                ));
            }
            for value in items {
                validate_widget_attribute_value(value, depth + 1, value_count)?;
            }
            Ok(())
        }
        Value::Object(attributes) => {
            if attributes.len() > 32 {
                return Err(ApiError::bad_request(
                    "external user attributes contain too many values",
                ));
            }
            for (key, value) in attributes {
                if key.is_empty() || key.chars().count() > 128 || key.chars().any(char::is_control)
                {
                    return Err(ApiError::bad_request(
                        "external user attribute key is invalid",
                    ));
                }
                validate_widget_attribute_value(value, depth + 1, value_count)?;
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(crate) struct OAuthAppRecord {
    pub(crate) id: Uuid,
    pub(crate) owner_id: Uuid,
    pub(crate) client_secret_hash: String,
    pub(crate) redirect_uris: Value,
    pub(crate) external_platform_id: Uuid,
    pub(crate) authentication_channel_id: Uuid,
    pub(crate) widget_history_enabled: bool,
    pub(crate) login_required: bool,
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) tool_allowlist: Option<Vec<String>>,
    pub(crate) client_tool_definitions: Vec<ClientToolDefinitionDto>,
}

#[derive(Debug)]
pub(crate) struct WidgetCredential {
    pub(crate) id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) agent_owner_id: Uuid,
    pub(crate) owner_id: Uuid,
    pub(crate) hub_session_id: Option<Uuid>,
    pub(crate) oauth_app_id: Option<Uuid>,
    pub(crate) external_platform_id: Option<Uuid>,
    pub(crate) external_tenant_id: Option<String>,
    pub(crate) external_user_id: Option<String>,
    pub(crate) external_identity_id: Option<Uuid>,
    pub(crate) profile_snapshot: Value,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) history_enabled: bool,
    pub(crate) anonymous: bool,
    pub(crate) client_instance_id: Option<Uuid>,
    pub(crate) client_tool_definitions: Vec<ClientToolDefinitionDto>,
    pub(crate) allowed_origins: Vec<String>,
}

impl WidgetCredential {
    pub(crate) fn is_anonymous(&self) -> bool {
        self.anonymous
    }

    pub(crate) fn is_external(&self) -> bool {
        !self.anonymous && self.oauth_app_id.is_some() && self.external_identity_id.is_some()
    }

    pub(crate) fn external_scope(&self) -> Result<(Uuid, Uuid, &str, &str, Uuid), ApiError> {
        Ok((
            self.oauth_app_id
                .ok_or(ApiError::unauthorized("invalid external Widget credential"))?,
            self.external_platform_id
                .ok_or(ApiError::unauthorized("invalid external Widget credential"))?,
            self.external_tenant_id
                .as_deref()
                .ok_or(ApiError::unauthorized("invalid external Widget credential"))?,
            self.external_user_id
                .as_deref()
                .ok_or(ApiError::unauthorized("invalid external Widget credential"))?,
            self.external_identity_id
                .ok_or(ApiError::unauthorized("invalid external Widget credential"))?,
        ))
    }
}

pub(crate) fn widget_session_locator(
    credential: &WidgetCredential,
    session_id: Uuid,
) -> (Option<Uuid>, Option<Uuid>) {
    if credential.is_external() {
        (Some(session_id), None)
    } else {
        (None, Some(session_id))
    }
}

pub(crate) fn merge_client_session_id(
    canonical: Option<Uuid>,
    compatibility: Option<Uuid>,
) -> Result<Option<Uuid>, ApiError> {
    if canonical.is_some() && compatibility.is_some() && canonical != compatibility {
        return Err(ApiError::bad_request("conflicting Client Session ids"));
    }
    Ok(canonical.or(compatibility))
}

pub(crate) fn widget_run_session_locator(
    credential: &WidgetCredential,
    request: &CreateWidgetRunRequest,
) -> Result<(Option<Uuid>, Option<Uuid>), ApiError> {
    if credential.is_external() {
        Ok((
            merge_client_session_id(request.session_id, request.integration_session_id)?,
            request.hub_session_id,
        ))
    } else {
        if request.integration_session_id.is_some() {
            return Err(ApiError::bad_request(
                "this Client does not use Integration Sessions",
            ));
        }
        Ok((
            None,
            merge_client_session_id(request.session_id, request.hub_session_id)?,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct WidgetScopedSession {
    pub(crate) integration_session_id: Option<Uuid>,
    pub(crate) hub_session_id: Uuid,
}

#[derive(Debug)]
pub(crate) struct ApplicationPrincipal {
    pub(crate) oauth_app_id: Uuid,
    pub(crate) app_owner_id: Uuid,
    pub(crate) grant_type: String,
    pub(crate) subject_user_id: Option<Uuid>,
    pub(crate) scopes: BTreeSet<String>,
    pub(crate) external_platform_id: Uuid,
    pub(crate) authentication_channel_id: Uuid,
    pub(crate) origin_tenant_id: Option<String>,
    pub(crate) origin_external_identity_id: Option<Uuid>,
}

#[derive(Debug)]
pub(crate) struct IntegrationPrincipal {
    pub(crate) oauth_app_id: Uuid,
    pub(crate) grant_type: String,
    pub(crate) subject_user_id: Option<Uuid>,
    pub(crate) agent_id: Uuid,
    pub(crate) agent_owner_id: Uuid,
    pub(crate) external_platform_id: Uuid,
    pub(crate) authentication_channel_id: Uuid,
    pub(crate) origin_tenant_id: Option<String>,
    pub(crate) origin_external_identity_id: Option<Uuid>,
}

pub(crate) struct RunModelAttribution {
    pub(crate) subject_type: &'static str,
    pub(crate) subject_user_id: Option<Uuid>,
    pub(crate) source_integration_app_id: Option<Uuid>,
}

pub(crate) fn integration_run_model_attribution(
    principal: &IntegrationPrincipal,
) -> RunModelAttribution {
    if principal.grant_type == "client_credentials" {
        RunModelAttribution {
            subject_type: "integration_app",
            subject_user_id: None,
            source_integration_app_id: Some(principal.oauth_app_id),
        }
    } else {
        RunModelAttribution {
            subject_type: "user",
            subject_user_id: principal.subject_user_id,
            source_integration_app_id: Some(principal.oauth_app_id),
        }
    }
}

pub(crate) fn validate_integration_app_payload(
    name: &str,
    redirect_uris: &Value,
    agent_ids: &[Uuid],
) -> Result<(), ApiError> {
    if name.trim().is_empty() {
        return Err(ApiError::bad_request("integration app name is required"));
    }
    let Some(redirects) = redirect_uris.as_array() else {
        return Err(ApiError::bad_request("redirect uris must be an array"));
    };
    if redirects.is_empty() {
        return Err(ApiError::bad_request("redirect uri is required"));
    }
    for redirect in redirects {
        let Some(value) = redirect.as_str() else {
            return Err(ApiError::bad_request("redirect uri must be a string"));
        };
        validate_oauth_redirect_uri(value)?;
    }
    let unique_agent_ids = agent_ids.iter().copied().collect::<BTreeSet<_>>();
    if unique_agent_ids.len() != agent_ids.len() || agent_ids.len() > 100 {
        return Err(ApiError::bad_request(
            "integration app agent ids must contain at most 100 unique values",
        ));
    }
    Ok(())
}

pub(crate) fn normalize_agent_tool_allowlist(value: &[String]) -> Result<Vec<String>, ApiError> {
    let requested = value
        .iter()
        .map(|name| name.trim())
        .collect::<BTreeSet<_>>();
    if requested.is_empty() {
        return Err(ApiError::bad_request(
            "at least one Agent tool must be enabled",
        ));
    }
    if requested.len() != value.len()
        || requested
            .iter()
            .any(|name| !AGENT_TOOL_NAMES.contains(name))
    {
        return Err(ApiError::bad_request("unsupported or duplicate Agent tool"));
    }
    Ok(AGENT_TOOL_NAMES
        .iter()
        .filter(|name| requested.contains(**name))
        .map(|name| (*name).to_owned())
        .collect())
}

pub(crate) fn normalize_allowed_origins(value: &[String]) -> Result<Vec<String>, ApiError> {
    let mut origins = BTreeSet::new();
    for raw in value {
        let raw = raw.trim();
        if raw.is_empty() || raw.contains('*') {
            return Err(ApiError::bad_request(
                "Widget origin must be an exact HTTP(S) Origin",
            ));
        }
        let url = Url::parse(raw).map_err(|_| ApiError::bad_request("Widget origin is invalid"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ApiError::bad_request(
                "Widget origin must be an exact HTTP(S) Origin without a path",
            ));
        }
        origins.insert(url.origin().ascii_serialization());
    }
    Ok(origins.into_iter().collect())
}

pub(crate) fn request_origin(headers: &HeaderMap) -> Result<Option<String>, ApiError> {
    if headers.contains_key(EMBEDDED_ORIGIN_HEADER) {
        if headers
            .get("sec-fetch-site")
            .and_then(|value| value.to_str().ok())
            != Some("same-origin")
        {
            return Err(ApiError::forbidden(
                "embedded request Origin is not allowed",
            ));
        }
        let mut values = headers.get_all(EMBEDDED_ORIGIN_HEADER).iter();
        let value = values
            .next()
            .ok_or(ApiError::forbidden("embedded request Origin is required"))?;
        if values.next().is_some() {
            return Err(ApiError::forbidden(
                "embedded request Origin is not allowed",
            ));
        }
        return parse_request_origin(value);
    }
    let mut values = headers.get_all(header::ORIGIN).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::forbidden("request Origin is not allowed"));
    }
    parse_request_origin(value)
}

pub(crate) fn parse_request_origin(value: &HeaderValue) -> Result<Option<String>, ApiError> {
    let raw = value
        .to_str()
        .map_err(|_| ApiError::forbidden("request Origin is not allowed"))?;
    let url = Url::parse(raw).map_err(|_| ApiError::forbidden("request Origin is not allowed"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ApiError::forbidden("request Origin is not allowed"));
    }
    Ok(Some(url.origin().ascii_serialization()))
}

pub(crate) fn validate_client_request_origin(
    headers: &HeaderMap,
    allowed_origins: &[String],
    required: bool,
) -> Result<(), ApiError> {
    let Some(origin) = request_origin(headers)? else {
        return if required {
            Err(ApiError::forbidden("request Origin is required"))
        } else {
            Ok(())
        };
    };
    if allowed_origins.is_empty() || allowed_origins.iter().any(|allowed| allowed == &origin) {
        Ok(())
    } else {
        Err(ApiError::forbidden("request Origin is not allowed"))
    }
}

pub(crate) fn validate_client_tool_definitions(
    definitions: &[ClientToolDefinitionDto],
) -> Result<Value, ApiError> {
    if definitions.len() > MAX_CLIENT_TOOL_COUNT {
        return Err(ApiError::bad_request("too many Client Tools"));
    }
    let mut names = BTreeSet::new();
    for definition in definitions {
        if definition.name.is_empty()
            || definition.name.len() > 64
            || !definition
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || !names.insert(definition.name.as_str())
        {
            return Err(ApiError::bad_request(
                "Client Tool names must be unique and contain only letters, digits, '_' or '-'",
            ));
        }
        if definition.description.trim().is_empty() {
            return Err(ApiError::bad_request("Client Tool description is required"));
        }
        let Some(schema) = definition.input_schema.as_object() else {
            return Err(ApiError::bad_request(
                "Client Tool input_schema must be a JSON object",
            ));
        };
        if schema.get("type").and_then(Value::as_str) != Some("object") {
            return Err(ApiError::bad_request(
                "Client Tool input_schema type must be object",
            ));
        }
    }
    let encoded = serde_json::to_vec(definitions)
        .map_err(|_| ApiError::bad_request("Client Tool definitions must be valid JSON"))?;
    if encoded.len() > MAX_CLIENT_TOOL_DEFINITIONS_BYTES {
        return Err(ApiError::bad_request(
            "Client Tool definitions are too large",
        ));
    }
    serde_json::to_value(definitions)
        .map_err(|_| ApiError::internal("Client Tool definitions could not be encoded"))
}

pub(crate) fn validate_public_widget_settings(
    login_required: bool,
    widget_history_enabled: bool,
    allowed_origins: &[String],
    tool_allowlist: Option<&[String]>,
    agent_ids: &[Uuid],
    role: &str,
) -> Result<(), ApiError> {
    if login_required {
        return Ok(());
    }
    if !is_admin_role(role) {
        return Err(ApiError::forbidden(
            "administrator permission is required for public Widgets",
        ));
    }
    if allowed_origins.is_empty() {
        return Err(ApiError::bad_request(
            "public Widget requires at least one allowed Origin",
        ));
    }
    if widget_history_enabled {
        return Err(ApiError::bad_request("public Widget cannot enable history"));
    }
    if agent_ids.len() != 1 {
        return Err(ApiError::bad_request(
            "public Widget requires exactly one Agent",
        ));
    }
    if tool_allowlist.is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| !PUBLIC_WIDGET_TOOL_NAMES.contains(&tool.as_str()))
    }) {
        return Err(ApiError::bad_request(
            "public Widget only permits read-only file tools",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_integration_app_tool_allowlist_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_ids: &[Uuid],
    tool_allowlist: Option<&[String]>,
) -> Result<(), ApiError> {
    let Some(tool_allowlist) = tool_allowlist else {
        return Ok(());
    };
    if agent_ids.is_empty() {
        return Err(ApiError::bad_request(
            "App tool selection requires at least one Agent",
        ));
    }
    let rows = sqlx::query(
        "SELECT id, tool_allowlist FROM agents
         WHERE id = ANY($1) AND deleted_at IS NULL
         FOR SHARE",
    )
    .bind(agent_ids)
    .fetch_all(&mut **tx)
    .await?;
    if rows.len() != agent_ids.len() {
        return Err(ApiError::not_found("agent not found"));
    }
    for row in rows {
        let allowed = normalize_agent_tool_allowlist(
            &serde_json::from_value::<Vec<String>>(row.get("tool_allowlist"))
                .map_err(|_| ApiError::internal("stored Agent tool policy is invalid"))?,
        )?;
        if tool_allowlist
            .iter()
            .any(|tool| !allowed.iter().any(|allowed_tool| allowed_tool == tool))
        {
            return Err(ApiError::bad_request(
                "App tools can only further restrict its Agents",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn require_integration_authentication_channel_tx(
    tx: &mut Transaction<'_, Postgres>,
    platform_id: Uuid,
    channel_id: Uuid,
) -> Result<(), ApiError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM authentication_channels
             WHERE id = $1 AND platform_id = $2
               AND enabled = true AND trusted_email = true
         )",
    )
    .bind(channel_id)
    .bind(platform_id)
    .fetch_one(&mut **tx)
    .await?;
    if !exists {
        return Err(ApiError::bad_request(
            "enabled trusted authentication channel is required",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_integration_app_agents_tx(
    tx: &mut Transaction<'_, Postgres>,
    user: &UserDto,
    agent_ids: &[Uuid],
) -> Result<(), ApiError> {
    if agent_ids.is_empty() {
        return Ok(());
    }
    let mut expected = agent_ids.to_vec();
    expected.sort_unstable();
    let available = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM agents
         WHERE id = ANY($1) AND deleted_at IS NULL
           AND 'integration' = ANY(endpoint_exposure)
           AND (owner_id = $2 OR visibility = 'public'
                OR (visibility = 'public_to' AND $2 = ANY(public_to)))
         ORDER BY id
         FOR SHARE",
    )
    .bind(&expected)
    .bind(user.id)
    .fetch_all(&mut **tx)
    .await?;
    if available != expected {
        return Err(ApiError::not_found("agent not found"));
    }
    Ok(())
}

pub(crate) async fn replace_integration_app_agents_tx(
    tx: &mut Transaction<'_, Postgres>,
    app_id: Uuid,
    agent_ids: &[Uuid],
) -> Result<(), ApiError> {
    sqlx::query("DELETE FROM integration_app_agents WHERE app_id = $1")
        .bind(app_id)
        .execute(&mut **tx)
        .await?;
    if !agent_ids.is_empty() {
        sqlx::query(
            "INSERT INTO integration_app_agents (app_id, agent_id)
             SELECT $1, unnest($2::uuid[])",
        )
        .bind(app_id)
        .bind(agent_ids)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) fn parse_oauth_scopes(
    raw: Option<&str>,
    default_profile: bool,
) -> Result<Vec<String>, ApiError> {
    let mut scopes = BTreeSet::new();
    match raw {
        None if default_profile => {
            scopes.extend(["profile".into(), "email".into(), "external_profile".into()]);
        }
        Some(raw) => {
            for scope in raw.split_ascii_whitespace() {
                match scope {
                    "profile" | "email" | "external_profile" => {
                        scopes.insert(scope.to_owned());
                    }
                    _ => {
                        let agent_id = scope
                            .strip_prefix("agent:")
                            .and_then(|value| Uuid::parse_str(value).ok())
                            .filter(|value| !value.is_nil())
                            .ok_or(ApiError::bad_request("invalid oauth scope"))?;
                        scopes.insert(format!("agent:{agent_id}"));
                    }
                }
            }
        }
        None => {}
    }
    Ok(scopes.into_iter().collect())
}

pub(crate) fn oauth_agent_scope_ids(scopes: &[String]) -> Result<Vec<Uuid>, ApiError> {
    scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("agent:"))
        .map(|value| {
            Uuid::parse_str(value).map_err(|_| ApiError::bad_request("invalid oauth scope"))
        })
        .collect()
}

pub(crate) async fn validate_oauth_agent_scopes_tx(
    tx: &mut Transaction<'_, Postgres>,
    app: &OAuthAppRecord,
    scopes: &[String],
    subject: Option<&UserDto>,
) -> Result<(), ApiError> {
    let app_owner = load_active_user_tx(tx, app.owner_id).await?;
    for agent_id in oauth_agent_scope_ids(scopes)? {
        let allowed: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                 SELECT 1
                 FROM integration_app_agents AS delegated
                 JOIN agents AS agent ON agent.id = delegated.agent_id
                 WHERE delegated.app_id = $1 AND delegated.agent_id = $2
                   AND agent.deleted_at IS NULL
                   AND (agent.owner_id = $3 OR agent.visibility = 'public'
                        OR (agent.visibility = 'public_to' AND $3 = ANY(agent.public_to)))
                   AND ($4::uuid IS NULL OR agent.owner_id = $4
                        OR agent.visibility = 'public'
                        OR (agent.visibility = 'public_to' AND $4 = ANY(agent.public_to)))
             )",
        )
        .bind(app.id)
        .bind(agent_id)
        .bind(app_owner.id)
        .bind(subject.map(|user| user.id))
        .fetch_one(&mut **tx)
        .await?;
        if !allowed {
            return Err(ApiError::forbidden(
                "oauth agent scope is not currently delegated",
            ));
        }
    }
    Ok(())
}

pub(crate) fn project_oauth_userinfo(
    scopes: &BTreeSet<String>,
    user: &UserDto,
    mut external: OAuthExternalProfileDto,
) -> OAuthUserInfoDto {
    let profile = scopes.contains("profile");
    let email = scopes.contains("email");
    if !email {
        external.email = None;
    }
    OAuthUserInfoDto {
        sub: user.id,
        name: profile.then(|| user.display_name.clone()),
        email: email.then(|| user.email.clone()),
        external_profile: scopes.contains("external_profile").then_some(external),
    }
}

pub(crate) fn validate_oauth_redirect_uri(value: &str) -> Result<(), ApiError> {
    let url = Url::parse(value).map_err(|_| ApiError::bad_request("redirect uri is invalid"))?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(ApiError::bad_request(
            "redirect uri cannot contain credentials or a fragment",
        ));
    }
    let secure = url.scheme() == "https";
    let loopback_http = url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
    if !secure && !loopback_http {
        return Err(ApiError::bad_request(
            "redirect uri must use HTTPS or loopback HTTP",
        ));
    }
    Ok(())
}

pub(crate) fn oauth_redirect_location(
    redirect_uri: &str,
    code: &str,
    state: Option<&str>,
) -> Result<String, ApiError> {
    let mut location =
        Url::parse(redirect_uri).map_err(|_| ApiError::bad_request("redirect uri is invalid"))?;
    {
        let mut pairs = location.query_pairs_mut();
        pairs.append_pair("code", code);
        if let Some(state) = state {
            pairs.append_pair("state", state);
        }
    }
    Ok(location.to_string())
}

pub(crate) fn redirect_uri_allowed(redirect_uris: &Value, redirect_uri: &str) -> bool {
    redirect_uris
        .as_array()
        .map(|items| items.iter().any(|item| item.as_str() == Some(redirect_uri)))
        .unwrap_or(false)
}

pub(crate) fn validate_tool_definitions(tools: &Value) -> Result<(), ApiError> {
    let Some(items) = tools.as_array() else {
        return Err(ApiError::bad_request("tools must be an array"));
    };
    for tool in items {
        let Some(name) = tool.get("name").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("tool name is required"));
        };
        if name.trim().is_empty() {
            return Err(ApiError::bad_request("tool name is required"));
        }
    }
    Ok(())
}

pub(crate) fn validate_integration_attachments(attachments: &Value) -> Result<(), ApiError> {
    let Some(items) = attachments.as_array() else {
        return Err(ApiError::bad_request("attachments must be an array"));
    };
    if items.len() > 5 {
        return Err(ApiError::bad_request("too many attachments"));
    }
    for attachment in items {
        let Some(kind) = attachment.get("kind").and_then(Value::as_str) else {
            return Err(ApiError::bad_request("attachment kind is required"));
        };
        if kind != "text" && kind != "url" {
            return Err(ApiError::bad_request("unsupported attachment kind"));
        }
        let name = attachment
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if name.is_none() {
            return Err(ApiError::bad_request("attachment name is required"));
        }
        let size_bytes = attachment
            .get("size_bytes")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        if !(0..=64_000).contains(&size_bytes) {
            return Err(ApiError::bad_request("attachment is too large"));
        }
        match kind {
            "text" => {
                let text = attachment
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(ApiError::bad_request("text attachment content is required"))?;
                if text.len() > 16_000 {
                    return Err(ApiError::bad_request("text attachment is too large"));
                }
            }
            "url" => {
                let url = attachment
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or(ApiError::bad_request("url attachment url is required"))?;
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return Err(ApiError::bad_request(
                        "attachment url must be http or https",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// 工具结果的三档校验结果。
pub(crate) struct ToolResultValidation {
    /// 存入 DB 的 payload：≤32KB 原样；更大为截断包装
    /// `{"truncated": true, "content": "<前 32KB 文本>"}`。
    pub payload: Value,
    /// 原始结果（完整，用于归档 S3）。
    pub original_value: Value,
    /// 原始结果序列化后的字节数。
    pub original_bytes: usize,
    /// 是否超过 32KB 截断阈值。
    pub truncated: bool,
    /// 是否超过归档硬上限（不归档，仅截断）。
    pub over_hard_limit: bool,
}

/// 三档校验：≤32KB 原样；32KB~硬上限 截断+归档；>硬上限 截断不归档。
/// 任何一档都不再拒绝提交，保证会话不因大结果挂掉。
pub(crate) fn validate_tool_result_value(
    result: &Value,
    max_tool_result_bytes: i64,
) -> Result<ToolResultValidation, ApiError> {
    if result.is_null() {
        return Err(ApiError::bad_request("tool result is required"));
    }
    let serialized = serde_json::to_string(result)
        .map_err(|_| ApiError::bad_request("tool result must be JSON"))?;
    let original_bytes = serialized.len();
    if original_bytes <= TOOL_RESULT_TRUNCATE_BYTES {
        return Ok(ToolResultValidation {
            payload: result.clone(),
            original_value: result.clone(),
            original_bytes,
            truncated: false,
            over_hard_limit: false,
        });
    }
    let over_hard_limit = original_bytes as i64 > max_tool_result_bytes;
    // 先回退到字符边界再切片：String::truncate 在边界内会直接 panic，
    // 多字节 UTF-8（中文等）结果在阈值处恰好切进字符中间时线上表现为 500。
    let mut end = TOOL_RESULT_TRUNCATE_BYTES.min(serialized.len());
    while !serialized.is_char_boundary(end) {
        end -= 1;
    }
    let truncated_text = serialized[..end].to_owned();
    Ok(ToolResultValidation {
        payload: json!({ "truncated": true, "content": truncated_text }),
        original_value: result.clone(),
        original_bytes,
        truncated: true,
        over_hard_limit,
    })
}

pub(crate) fn validate_tool_result(
    result: &Value,
    max_tool_result_bytes: i64,
) -> Result<ToolResultValidation, ApiError> {
    validate_tool_result_value(result, max_tool_result_bytes)
}

/// 将超限工具结果全文归档到 S3（指数退避重试 3 次），失败则降级为仅截断。
/// 返回归档元数据；未归档（未超阈值 / 超硬上限 / S3 不可用）返回降级原因。
pub(crate) async fn archive_tool_result(
    state: &AppState,
    run_id: Uuid,
    _tool_request_id: Uuid,
    validation: &ToolResultValidation,
) -> Option<(Option<Uuid>, i64, Option<String>)> {
    if !validation.truncated {
        return None;
    }
    let Some(store) = state.session_bundle_store.clone() else {
        return Some((
            None,
            validation.original_bytes as i64,
            Some("artifact_store_unavailable".into()),
        ));
    };
    if validation.over_hard_limit {
        return Some((
            None,
            validation.original_bytes as i64,
            Some("over_hard_limit".into()),
        ));
    }
    let artifact_id = Uuid::new_v4();
    let object_key = format!("tool-results/{run_id}/{artifact_id}");
    let Ok(serialized) = serde_json::to_string(&validation.original_value) else {
        return Some((
            Some(artifact_id),
            validation.original_bytes as i64,
            Some("artifact_serialize_failed".into()),
        ));
    };
    let checksum = sha256_hex(&serialized);
    let size_bytes = serialized.len() as u64;
    let bytes = axum::body::Bytes::from(serialized);
    let mut delay_secs = 1u64;
    for attempt in 0..3 {
        let chunk = bytes.clone();
        let stream = futures_util::stream::once(async move { Ok::<_, std::io::Error>(chunk) });
        match store
            .put_stream(&object_key, size_bytes, &checksum, stream)
            .await
        {
            Ok(()) => {
                return Some((Some(artifact_id), size_bytes as i64, None));
            }
            Err(_) if attempt < 2 => {
                tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                delay_secs *= 2;
            }
            Err(_) => {
                return Some((
                    Some(artifact_id),
                    size_bytes as i64,
                    Some("artifact_upload_failed".into()),
                ));
            }
        }
    }
    None
}

pub(crate) async fn insert_integration_attachments_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    run_id: Uuid,
    hub_message_id: Uuid,
    attachments: &Value,
) -> Result<(), ApiError> {
    let Some(items) = attachments.as_array() else {
        return Ok(());
    };
    for attachment in items {
        let kind = attachment
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("text");
        let name = attachment
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("attachment");
        let content_type = attachment
            .get("content_type")
            .and_then(Value::as_str)
            .unwrap_or("text/plain");
        let text = attachment.get("text").and_then(Value::as_str);
        let url = attachment.get("url").and_then(Value::as_str);
        let size_bytes = attachment
            .get("size_bytes")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| text.map(|value| value.len() as i64).unwrap_or(0));
        sqlx::query(
            "INSERT INTO integration_attachments
                 (id, session_id, run_id, hub_message_id, kind, name,
                  content_type, size_bytes, text, url)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(Uuid::new_v4())
        .bind(session_id)
        .bind(run_id)
        .bind(hub_message_id)
        .bind(kind)
        .bind(name)
        .bind(content_type)
        .bind(size_bytes)
        .bind(text)
        .bind(url)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) async fn load_integration_app(
    pool: &PgPool,
    app_id: Uuid,
    owner_id: Uuid,
) -> Result<IntegrationAppDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, owner_id, name, client_id, external_platform_id,
                authentication_channel_id, redirect_uris, widget_history_enabled,
                login_required, allowed_origins, tool_allowlist, client_tool_definitions,
                created_at, updated_at
         FROM oauth_apps
         WHERE id = $1 AND owner_id = $2 AND deleted_at IS NULL",
    )
    .bind(app_id)
    .bind(owner_id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("integration app not found"))?;
    let agent_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT agent_id FROM integration_app_agents
         WHERE app_id = $1 ORDER BY agent_id",
    )
    .bind(app_id)
    .fetch_all(pool)
    .await?;
    Ok(integration_app_from_row(row, agent_ids))
}

pub(crate) async fn load_oauth_app_by_client_id(
    pool: &PgPool,
    client_id: &str,
) -> Result<OAuthAppRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, owner_id, client_secret_hash, redirect_uris,
                external_platform_id, authentication_channel_id, widget_history_enabled,
                login_required, allowed_origins, tool_allowlist, client_tool_definitions
         FROM oauth_apps
         WHERE client_id = $1 AND deleted_at IS NULL
           AND client_secret_hash IS NOT NULL",
    )
    .bind(client_id)
    .fetch_optional(pool)
    .await?;
    let row = row.ok_or(ApiError::unauthorized("invalid oauth client"))?;
    oauth_app_record_from_row(row)
}

pub(crate) async fn load_oauth_app_by_client_id_tx(
    tx: &mut Transaction<'_, Postgres>,
    client_id: &str,
) -> Result<OAuthAppRecord, ApiError> {
    let row = sqlx::query(
        "SELECT id, owner_id, client_secret_hash, redirect_uris,
                external_platform_id, authentication_channel_id, widget_history_enabled,
                login_required, allowed_origins, tool_allowlist, client_tool_definitions
         FROM oauth_apps
         WHERE client_id = $1 AND deleted_at IS NULL
           AND client_secret_hash IS NOT NULL
         FOR SHARE",
    )
    .bind(client_id)
    .fetch_optional(&mut **tx)
    .await?;
    let row = row.ok_or(ApiError::unauthorized("invalid oauth client"))?;
    oauth_app_record_from_row(row)
}

pub(crate) fn oauth_app_record_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<OAuthAppRecord, ApiError> {
    Ok(OAuthAppRecord {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        client_secret_hash: row.get("client_secret_hash"),
        redirect_uris: row.get("redirect_uris"),
        external_platform_id: row.get("external_platform_id"),
        authentication_channel_id: row.get("authentication_channel_id"),
        widget_history_enabled: row.get("widget_history_enabled"),
        login_required: row.get("login_required"),
        allowed_origins: serde_json::from_value(row.get("allowed_origins"))
            .map_err(|_| ApiError::internal("stored Widget origins are invalid"))?,
        tool_allowlist: row
            .get::<Option<Value>, _>("tool_allowlist")
            .map(serde_json::from_value)
            .transpose()
            .map_err(|_| ApiError::internal("stored App tool policy is invalid"))?,
        client_tool_definitions: serde_json::from_value(row.get("client_tool_definitions"))
            .map_err(|_| ApiError::internal("stored Client Tool definitions are invalid"))?,
    })
}

pub(crate) fn validate_public_widget_app_record(app: &OAuthAppRecord) -> Result<(), ApiError> {
    if app.login_required {
        return Err(ApiError::not_found("public Widget application not found"));
    }
    if app.allowed_origins.is_empty() {
        return Err(ApiError::conflict("public Widget has no allowed Origins"));
    }
    if app.tool_allowlist.as_ref().is_some_and(|tools| {
        tools
            .iter()
            .any(|tool| !PUBLIC_WIDGET_TOOL_NAMES.contains(&tool.as_str()))
    }) {
        return Err(ApiError::conflict("public Widget tool policy is invalid"));
    }
    Ok(())
}

pub(crate) async fn load_public_widget_app_by_client_id(
    pool: &PgPool,
    client_id: &str,
) -> Result<OAuthAppRecord, ApiError> {
    let app = load_oauth_app_by_client_id(pool, client_id).await?;
    validate_public_widget_app_record(&app)?;
    let agent_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM integration_app_agents AS delegated
         JOIN agents AS agent ON agent.id = delegated.agent_id
         WHERE delegated.app_id = $1 AND agent.deleted_at IS NULL",
    )
    .bind(app.id)
    .fetch_one(pool)
    .await?;
    if agent_count != 1 {
        return Err(ApiError::conflict(
            "public Widget must delegate exactly one active Agent",
        ));
    }
    Ok(app)
}

pub(crate) async fn load_public_widget_app_by_client_id_tx(
    tx: &mut Transaction<'_, Postgres>,
    client_id: &str,
) -> Result<OAuthAppRecord, ApiError> {
    let app = load_oauth_app_by_client_id_tx(tx, client_id).await?;
    validate_public_widget_app_record(&app)?;
    let delegated_agents = sqlx::query_scalar::<_, Uuid>(
        "SELECT agent.id
         FROM integration_app_agents AS delegated
         JOIN agents AS agent ON agent.id = delegated.agent_id
         WHERE delegated.app_id = $1 AND agent.deleted_at IS NULL
         FOR SHARE OF delegated, agent",
    )
    .bind(app.id)
    .fetch_all(&mut **tx)
    .await?;
    if delegated_agents.len() != 1 {
        return Err(ApiError::conflict(
            "public Widget must delegate exactly one active Agent",
        ));
    }
    Ok(app)
}

pub(crate) fn visitor_key_hash(visitor_key: &str) -> Result<String, ApiError> {
    let visitor_key = visitor_key.trim();
    if !(16..=512).contains(&visitor_key.len()) || visitor_key.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "public Widget visitor key is invalid",
        ));
    }
    Ok(sha256_hex(visitor_key))
}

pub(crate) async fn load_claim_session_context_tx(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunDto,
) -> Result<ClaimSessionContextDto, ApiError> {
    let session_id = run
        .hub_session_id
        .ok_or(ApiError::internal("claimed Run Session is missing"))?;
    let turn_id = run
        .hub_turn_id
        .ok_or(ApiError::internal("claimed Run Turn is missing"))?;
    let session_row = sqlx::query(
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
         WHERE id = $1 AND agent_id = $2",
    )
    .bind(session_id)
    .bind(run.agent_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::internal("claimed Run Session is unavailable"))?;
    let turn_row = sqlx::query(
        "SELECT id, session_id, native_turn_id, status, configuration_fingerprint,
                ownership_generation, started_at, ended_at, created_at, updated_at
         FROM hub_session_turns
         WHERE id = $1 AND session_id = $2",
    )
    .bind(turn_id)
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::internal("claimed Run Turn is unavailable"))?;
    let message_rows = sqlx::query(
        "SELECT id, session_id, sequence, role, message_kind, content, payload,
                delivery_mode, delivery_state, client_message_key,
                expected_native_turn_id, turn_id, run_id, accepted_at
         FROM hub_session_messages
         WHERE session_id = $1 AND turn_id = $2 AND run_id = $3
           AND delivery_state = 'queued'
         ORDER BY sequence",
    )
    .bind(session_id)
    .bind(turn_id)
    .bind(run.id)
    .fetch_all(&mut **tx)
    .await?;
    let mut messages = message_rows
        .into_iter()
        .map(hub_message_from_row)
        .collect::<Vec<_>>();
    fill_message_attachments(&mut **tx, &mut messages).await?;
    Ok(ClaimSessionContextDto {
        session: hub_session_from_row(session_row),
        turn: hub_turn_from_row(turn_row),
        messages,
    })
}

pub(crate) async fn load_hub_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
) -> Result<HubSessionDto, ApiError> {
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
         FROM hub_sessions WHERE id = $1",
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("session not found"))?;
    Ok(hub_session_from_row(row))
}

/// 把截断的工具结果 payload 展开为模型可读的摘要：告知原始大小、归档状态与
/// 获取全量的工具指引；未归档（超硬上限/上传失败）明确告知不可获取，防止
/// 模型反复尝试读取不存在的归档。
/// 兼容三种形状：
/// - runtime 侧旧形状 `{truncated, content}`（整行 payload 或 output 直传）；
/// - client 侧 DTO 外壳 `{status, output:{truncated,content}, ...}`（singular
///   路径传入整行 payload）；
/// - plural 路径把 output 对象直传 summarize（顶层 truncated）。
fn summarize_tool_result_payload(
    payload: Value,
    tool_call_id: Uuid,
    artifact_id: Option<Uuid>,
    artifact_size_bytes: Option<i64>,
    artifact_reason: Option<&str>,
    truncated: bool,
) -> Value {
    if !truncated {
        return payload;
    }
    // content 提取兼容三种形状：plural 直传 output（字符串）、runtime 旧形状
    // {truncated, content}、singular 整行 DTO 外壳（output / error.message）。
    let content = payload
        .as_str()
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            payload
                .get("output")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let size = artifact_size_bytes.unwrap_or(content.len() as i64);
    let (heading, hint) = match (artifact_id, artifact_reason) {
        (Some(id), _) => (
            format!("[tool result truncated: {size} bytes total; archived as artifact://{id}]"),
            format!(
                "Read the full result with agent_hub_integration_tool_result_read(tool_call_id=\"{}\", mode=\"size\") or (tool_call_id=\"{}\", mode=\"range\", offset, limit) or (tool_call_id=\"{}\", mode=\"file\")",
                tool_call_id, tool_call_id, tool_call_id
            ),
        ),
        (None, Some("over_hard_limit")) => (
            format!(
                "[tool result exceeded the hard limit and was NOT archived; total size: {size} bytes; only the first {} bytes are available]",
                TOOL_RESULT_TRUNCATE_BYTES
            ),
            String::new(),
        ),
        (None, reason) => (
            format!(
                "[tool result truncated: {size} bytes total; full result unavailable ({})]",
                reason.unwrap_or("not archived")
            ),
            String::new(),
        ),
    };
    json!({
        "truncated": true,
        "content": format!("{heading}\n{hint}\n{}", content),
    })
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToolResultArtifactQuery {
    pub(crate) mode: Option<String>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<i64>,
}

/// 读取已归档的工具结果：`mode=size` 返回元数据；`mode=range` 返回
/// `{content, offset, limit, next_offset, size_bytes}` 文本片段。
/// 鉴权双通道：runtime token（Pi 读取工具）或会话用户（run owner，前端查看）。
pub(crate) async fn get_tool_result_artifact(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((run_id, tool_request_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<ToolResultArtifactQuery>,
) -> Result<Response, ApiError> {
    if bearer_token(&headers).is_some() {
        let runtime_id = require_runtime(&state, &headers).await?;
        let owned: bool = sqlx::query_scalar("SELECT runtime_id = $1 FROM runs WHERE id = $2")
            .bind(runtime_id)
            .bind(run_id)
            .fetch_optional(&state.pool)
            .await?
            .unwrap_or(false);
        if !owned {
            return Err(ApiError::forbidden("run is not owned by this runtime"));
        }
    } else {
        let user = require_user(&state, &headers).await?;
        let owned: bool = sqlx::query_scalar("SELECT owner_id = $1 FROM runs WHERE id = $2")
            .bind(user.id)
            .bind(run_id)
            .fetch_optional(&state.pool)
            .await?
            .unwrap_or(false);
        if !owned {
            return Err(ApiError::forbidden("run is not owned by this user"));
        }
    }
    load_tool_result_artifact_response(&state, run_id, tool_request_id, &query).await
}

/// runtime 专用读取接口：按 tool_request_id 解析 run_id（Pi 读取工具使用，
/// 不要求调用方知道 run_id）。
pub(crate) async fn get_tool_result_artifact_by_request_id(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(tool_request_id): Path<Uuid>,
    Query(query): Query<ToolResultArtifactQuery>,
) -> Result<Response, ApiError> {
    require_runtime(&state, &headers).await?;
    let run_id: Uuid =
        sqlx::query_scalar("SELECT run_id FROM integration_tool_requests WHERE id = $1")
            .bind(tool_request_id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or(ApiError::not_found("tool request not found"))?;
    load_tool_result_artifact_response(&state, run_id, tool_request_id, &query).await
}

async fn load_tool_result_artifact_response(
    state: &AppState,
    run_id: Uuid,
    tool_request_id: Uuid,
    query: &ToolResultArtifactQuery,
) -> Result<Response, ApiError> {
    let row = sqlx::query_as::<_, (Option<Uuid>, Option<i64>, Option<String>, bool)>(
        "SELECT artifact_id, artifact_size_bytes, artifact_reason, result_truncated
         FROM integration_tool_requests
         WHERE id = $1 AND run_id = $2 AND status = 'completed'",
    )
    .bind(tool_request_id)
    .bind(run_id)
    .fetch_optional(&state.pool)
    .await?;
    let Some((artifact_id, artifact_size_bytes, artifact_reason, truncated)) = row else {
        return Err(ApiError::not_found("tool request result not found"));
    };
    match query.mode.as_deref().unwrap_or("size") {
        "size" => Ok(Json(json!({
            "tool_request_id": tool_request_id,
            "size_bytes": artifact_size_bytes.unwrap_or(0),
            "artifact_id": artifact_id,
            "artifact_reason": artifact_reason,
            "truncated": truncated,
        }))
        .into_response()),
        "full" => {
            // 全量流式返回（file 模式 / 下载用），不经内存整体加载。
            let Some(artifact_id) = artifact_id else {
                return Err(ApiError::not_found(
                    "tool result was not archived; check artifact_reason via mode=size",
                ));
            };
            let store = state
                .session_bundle_store
                .clone()
                .ok_or_else(|| ApiError::internal("artifact store is unavailable"))?;
            let object_key = format!("tool-results/{run_id}/{artifact_id}");
            let response = store
                .get(&object_key)
                .await
                .map_err(|_| ApiError::internal("artifact read failed"))?;
            let stream = response
                .bytes_stream()
                .map(|item| item.map_err(std::io::Error::other));
            let body = axum::body::Body::from_stream(stream);
            Ok(Response::builder()
                .header(
                    header::CONTENT_TYPE,
                    "application/json; charset=utf-8".to_owned(),
                )
                .header(header::CONTENT_LENGTH, artifact_size_bytes.unwrap_or(0))
                .body(body)
                .map_err(|_| ApiError::internal("artifact response build failed"))?)
        }
        "range" => {
            let Some(artifact_id) = artifact_id else {
                return Err(ApiError::not_found(
                    "tool result was not archived; check artifact_reason via mode=size",
                ));
            };
            let store = state
                .session_bundle_store
                .clone()
                .ok_or_else(|| ApiError::internal("artifact store is unavailable"))?;
            let offset = query.offset.unwrap_or(0).max(0);
            let limit = query
                .limit
                .unwrap_or(TOOL_RESULT_READ_LIMIT_BYTES as i64)
                .clamp(1, TOOL_RESULT_READ_LIMIT_BYTES as i64);
            let object_key = format!("tool-results/{run_id}/{artifact_id}");
            let range = format!("bytes={}-{}", offset, offset + limit - 1);
            let response = store
                .get_range(&object_key, &range)
                .await
                .map_err(|_| ApiError::internal("artifact read failed"))?;
            let bytes = response
                .bytes()
                .await
                .map_err(|_| ApiError::internal("artifact read failed"))?;
            let content = String::from_utf8_lossy(&bytes).into_owned();
            Ok(Json(json!({
                "content": content,
                "offset": offset,
                "limit": limit,
                "next_offset": offset + content.len() as i64,
                "size_bytes": artifact_size_bytes.unwrap_or(0),
            }))
            .into_response())
        }
        _ => Err(ApiError::bad_request(
            "unsupported tool result artifact mode",
        )),
    }
}

pub(crate) async fn load_integration_context_for_run(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunDto,
) -> Result<Option<IntegrationContextDto>, ApiError> {
    let run_context = sqlx::query(
        "SELECT integration_session_id, external_user_context,
                client_instance_id, client_tool_snapshot
         FROM runs WHERE id = $1",
    )
    .bind(run.id)
    .fetch_one(&mut **tx)
    .await?;
    let integration_session_id: Option<Uuid> = run_context.get("integration_session_id");
    let client_instance_id: Option<Uuid> = run_context.get("client_instance_id");
    if integration_session_id.is_none() && client_instance_id.is_none() {
        return Ok(None);
    }
    let session_tools = if let Some(integration_session_id) = integration_session_id {
        sqlx::query_scalar::<_, Value>(
            "SELECT tool_definitions FROM integration_sessions WHERE id = $1",
        )
        .bind(integration_session_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::internal("Run Integration Session is missing"))?
    } else {
        Value::Array(Vec::new())
    };
    let external_user = run_context
        .get::<Option<Value>, _>("external_user_context")
        .map(serde_json::from_value::<ExternalUserContextDto>)
        .transpose()
        .map_err(|_| ApiError::internal("Run external user context is invalid"))?;
    let attachment_rows = sqlx::query(
        "SELECT kind, name, content_type, size_bytes, text, url
         FROM integration_attachments WHERE run_id = $1 ORDER BY created_at ASC",
    )
    .bind(run.id)
    .fetch_all(&mut **tx)
    .await?;
    let attachments = attachment_rows
        .into_iter()
        .map(|row| {
            json!({
                "kind": row.get::<String, _>("kind"),
                "name": row.get::<String, _>("name"),
                "content_type": row.get::<String, _>("content_type"),
                "size_bytes": row.get::<i64, _>("size_bytes"),
                "text": row.get::<Option<String>, _>("text"),
                "url": row.get::<Option<String>, _>("url")
            })
        })
        .collect::<Vec<_>>();
    let (tool_result, tool_results) = if run.source == "integration:tool_result" {
        let parent_run_id = run
            .parent_run_id
            .ok_or(ApiError::internal("tool-result run parent is missing"))?;
        let hub_session_id = run
            .hub_session_id
            .ok_or(ApiError::internal("tool-result Run Session is missing"))?;
        let result_rows = sqlx::query(
            "SELECT id, tool_name, result_payload, artifact_id, artifact_size_bytes,
                    artifact_reason
             FROM integration_tool_requests
             WHERE follow_up_run_id = $1 AND run_id = $2 AND hub_session_id = $3
               AND status = 'completed'
             ORDER BY position",
        )
        .bind(run.id)
        .bind(parent_run_id)
        .bind(hub_session_id)
        .fetch_all(&mut **tx)
        .await?;
        let tool_result = result_rows
            .last()
            .map(|row| -> Result<Value, ApiError> {
                let payload: Value = row.get("result_payload");
                // 按来源分流：client 路径取包装层内层 result；runtime 路径的
                // payload 本身就是 {truncated, content} 形状，原样使用。
                let result_value = if client_instance_id.is_some() {
                    payload
                        .get("result")
                        .ok_or_else(|| ApiError::internal("stored Client Tool result is missing"))?
                        .clone()
                } else {
                    payload
                };
                let truncated =
                    result_value.get("truncated").and_then(Value::as_bool) == Some(true);
                Ok(summarize_tool_result_payload(
                    result_value,
                    row.get("id"),
                    row.get("artifact_id"),
                    row.get("artifact_size_bytes"),
                    row.get::<Option<String>, _>("artifact_reason").as_deref(),
                    truncated,
                ))
            })
            .transpose()?;
        let tool_results = if client_instance_id.is_some() {
            let total = result_rows.len();
            // 总量预算：最新优先展开（position 升序，倒序迭代即最新在前，
            // 且 singular 与最后一个同源，保证最新结果永远完整展开）；累计
            // 序列化大小超 64KB 预算后，更旧的结果替换为占位 DTO（保留
            // tool_call_id/tool_name 身份与读取指引，模型仍可取全文）。
            let mut budget = CLIENT_TOOL_RESULTS_BUDGET_BYTES;
            let mut expanded = 0usize;
            let mut results = Vec::with_capacity(total);
            for row in result_rows.into_iter().rev() {
                let payload: Value = row.get("result_payload");
                // 包装层内层 result + 内层 truncated；DTO 反序列化会丢弃
                // truncated 字段，先读取标记再解析；身份以行列
                // （id/tool_name）权威。
                let inner = payload
                    .get("result")
                    .ok_or_else(|| ApiError::internal("stored Client Tool result is missing"))?;
                let truncated = inner.get("truncated").and_then(Value::as_bool) == Some(true);
                let result: ClientToolResultDto = serde_json::from_value(inner.clone())
                    .map_err(|_| ApiError::internal("stored Client Tool result is invalid"))?;
                let result = match result {
                    ClientToolResultDto::Success { output, .. } => ClientToolResultDto::Success {
                        output: summarize_tool_result_payload(
                            output,
                            row.get("id"),
                            row.get("artifact_id"),
                            row.get("artifact_size_bytes"),
                            row.get::<Option<String>, _>("artifact_reason").as_deref(),
                            truncated,
                        ),
                        truncated: None,
                    },
                    other => other,
                };
                let dto = ClientToolContinuationResultDto {
                    tool_call_id: row.get("id"),
                    tool_name: row.get("tool_name"),
                    result,
                };
                let size = serde_json::to_string(&dto).map(|s| s.len()).unwrap_or(0);
                if budget >= size {
                    budget -= size;
                    expanded += 1;
                    results.push(dto);
                } else {
                    // 占位：超预算后更旧的结果不再展开（预算置 0 恒占位）。
                    results.push(ClientToolContinuationResultDto {
                        tool_call_id: row.get("id"),
                        tool_name: row.get("tool_name"),
                        result: ClientToolResultDto::Success {
                            output: json!(format!(
                                "[工具结果未展开：本批共 {total} 个结果超总量上限(64KB)，仅展开最近 {expanded} 个；用 agent_hub_integration_tool_result_read(tool_call_id=\"{}\", mode=\"size\") 读取完整内容]",
                                row.get::<Uuid, _>("id")
                            )),
                            truncated: Some(true),
                        },
                    });
                    budget = 0;
                }
            }
            // 恢复 position 升序，模型按时间顺序读取。
            results.reverse();
            results
        } else {
            Vec::new()
        };
        (tool_result, tool_results)
    } else {
        (None, Vec::new())
    };
    Ok(Some(IntegrationContextDto {
        tools: if client_instance_id.is_some() {
            run_context.get("client_tool_snapshot")
        } else {
            session_tools
        },
        attachments: Value::Array(attachments),
        tool_result,
        tool_results,
        external_user,
    }))
}

pub(crate) async fn load_integration_session(
    pool: &PgPool,
    session_id: Uuid,
    principal: &IntegrationPrincipal,
) -> Result<IntegrationSessionDto, ApiError> {
    let row = sqlx::query(
        "SELECT session.id, session.hub_session_id, session.agent_id,
                session.owner_id, hub.origin_platform_id AS platform_id,
                hub.origin_tenant_id AS tenant_id,
                hub.origin_external_identity_id AS external_identity_id,
                session.external_user_id, session.tool_definitions,
                session.metadata, session.created_at
         FROM integration_sessions AS session
         JOIN agents AS agent ON agent.id = session.agent_id AND agent.deleted_at IS NULL
         JOIN oauth_apps AS app ON app.id = session.oauth_app_id
         JOIN hub_sessions AS hub
           ON hub.id = session.hub_session_id
          AND hub.owner_id = session.owner_id
          AND hub.agent_id = session.agent_id
         WHERE session.id = $1 AND session.oauth_app_id = $2
           AND session.agent_id = $3
           AND app.external_platform_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   session.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )",
    )
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(pool)
    .await?;
    row.map(integration_session_from_row)
        .ok_or(ApiError::not_found("integration session not found"))
}

pub(crate) async fn integration_session_agent_id(
    pool: &PgPool,
    session_id: Uuid,
) -> Result<Uuid, ApiError> {
    sqlx::query_scalar("SELECT agent_id FROM integration_sessions WHERE id = $1")
        .bind(session_id)
        .fetch_optional(pool)
        .await?
        .ok_or(ApiError::not_found("integration session not found"))
}

pub(crate) async fn integration_tool_request_agent_id(
    pool: &PgPool,
    tool_request_id: Uuid,
) -> Result<Uuid, ApiError> {
    sqlx::query_scalar(
        "SELECT session.agent_id
         FROM integration_tool_requests AS tool
         JOIN integration_sessions AS session ON session.id = tool.session_id
         WHERE tool.id = $1",
    )
    .bind(tool_request_id)
    .fetch_optional(pool)
    .await?
    .ok_or(ApiError::not_found("tool request not found"))
}

pub(crate) async fn load_integration_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    session_id: Uuid,
    principal: &IntegrationPrincipal,
) -> Result<IntegrationSessionDto, ApiError> {
    let row = sqlx::query(
        "SELECT session.id, session.hub_session_id, session.agent_id,
                session.owner_id, hub.origin_platform_id AS platform_id,
                hub.origin_tenant_id AS tenant_id,
                hub.origin_external_identity_id AS external_identity_id,
                session.external_user_id, session.tool_definitions,
                session.metadata, session.created_at
         FROM integration_sessions AS session
         JOIN oauth_apps AS app ON app.id = session.oauth_app_id
         JOIN hub_sessions AS hub
           ON hub.id = session.hub_session_id
          AND hub.owner_id = session.owner_id
          AND hub.agent_id = session.agent_id
         WHERE session.id = $1 AND session.oauth_app_id = $2
           AND session.agent_id = $3
           AND app.external_platform_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   session.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )",
    )
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(integration_session_from_row)
        .ok_or(ApiError::not_found("integration session not found"))
}

pub(crate) async fn load_run_public_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
) -> Result<RunDto, ApiError> {
    let row = sqlx::query(
        "SELECT id, agent_id, automation_id, integration_session_id, parent_run_id,
                runtime_id, hub_session_id, hub_message_id, hub_turn_id,
                session_ownership_generation, status, initial_message, native_session_id,
                work_dir_ref, source, created_at, updated_at
         FROM runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(&mut **tx)
    .await?;
    Ok(run_from_row(row))
}

pub(crate) async fn load_integration_events_after(
    pool: &PgPool,
    session_id: Uuid,
    after: i64,
    principal: &IntegrationPrincipal,
) -> Result<Vec<RunEventDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT e.seq, e.event_id, e.run_id, e.event_type, e.role, e.content, e.payload, e.created_at
         FROM run_events e
         JOIN runs r ON r.id = e.run_id
         JOIN agents a ON a.id = r.agent_id AND a.deleted_at IS NULL
         JOIN integration_sessions AS integration
           ON integration.id = r.integration_session_id
         JOIN oauth_apps AS app ON app.id = integration.oauth_app_id
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.id = $1 AND e.seq > $2
           AND integration.oauth_app_id = $3 AND integration.agent_id = $4
           AND app.external_platform_id = $5
           AND hub.origin_kind = 'external' AND hub.origin_platform_id = $5
           AND (
               $8::uuid IS NULL
               OR (
                   integration.owner_id = $8
                   AND hub.origin_tenant_id = $6
                   AND hub.origin_external_identity_id = $7
               )
           )
         ORDER BY e.seq ASC",
    )
    .bind(session_id)
    .bind(after)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(event_from_row).collect())
}

pub(crate) async fn load_tool_request_for_update(
    tx: &mut Transaction<'_, Postgres>,
    tool_request_id: Uuid,
    session_id: Uuid,
    principal: &IntegrationPrincipal,
) -> Result<IntegrationToolRequestDto, ApiError> {
    let row = sqlx::query(
        "SELECT t.id, t.session_id, t.hub_session_id, t.run_id, t.position,
                t.tool_name, t.arguments, t.status,
                t.claimed_by_client_instance_id, t.claimed_at,
                t.result_payload, t.follow_up_run_id, t.expires_at,
                t.responded_at, t.created_at,
                t.artifact_id, t.artifact_size_bytes, t.artifact_reason, t.result_truncated
         FROM integration_tool_requests t
         JOIN integration_sessions s ON s.id = t.session_id
         JOIN oauth_apps app ON app.id = s.oauth_app_id
         JOIN hub_sessions hub
           ON hub.id = s.hub_session_id
          AND hub.owner_id = s.owner_id
          AND hub.agent_id = s.agent_id
         WHERE t.id = $1 AND s.id = $2 AND s.oauth_app_id = $3
           AND s.agent_id = $4 AND app.external_platform_id = $5
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $5
           AND (
               $8::uuid IS NULL
               OR (
                   s.owner_id = $8
                   AND hub.origin_tenant_id = $6
                   AND hub.origin_external_identity_id = $7
               )
           )
         FOR UPDATE OF t",
    )
    .bind(tool_request_id)
    .bind(session_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(tool_request_from_row)
        .ok_or(ApiError::not_found("tool request not found"))
}

pub(crate) async fn lock_tool_request_session_for_update(
    tx: &mut Transaction<'_, Postgres>,
    tool_request_id: Uuid,
    principal: &IntegrationPrincipal,
) -> Result<Uuid, ApiError> {
    let session_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT s.id
         FROM integration_sessions s
         JOIN integration_tool_requests t ON t.session_id = s.id
         JOIN oauth_apps app ON app.id = s.oauth_app_id
         JOIN hub_sessions hub
           ON hub.id = s.hub_session_id
          AND hub.owner_id = s.owner_id
          AND hub.agent_id = s.agent_id
         WHERE t.id = $1 AND s.oauth_app_id = $2 AND s.agent_id = $3
           AND app.external_platform_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   s.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )
         FOR UPDATE OF s",
    )
    .bind(tool_request_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(&mut **tx)
    .await?;
    session_id.ok_or(ApiError::not_found("tool request not found"))
}

pub(crate) async fn load_tool_request(
    pool: &PgPool,
    tool_request_id: Uuid,
    principal: &IntegrationPrincipal,
) -> Result<IntegrationToolRequestDto, ApiError> {
    let row = sqlx::query(
        "SELECT t.id, t.session_id, t.hub_session_id, t.run_id, t.position,
                t.tool_name, t.arguments, t.status,
                t.claimed_by_client_instance_id, t.claimed_at,
                t.result_payload, t.follow_up_run_id, t.expires_at,
                t.responded_at, t.created_at,
                t.artifact_id, t.artifact_size_bytes, t.artifact_reason, t.result_truncated
         FROM integration_tool_requests t
         JOIN integration_sessions s ON s.id = t.session_id
         JOIN oauth_apps app ON app.id = s.oauth_app_id
         JOIN hub_sessions hub
           ON hub.id = s.hub_session_id
          AND hub.owner_id = s.owner_id
          AND hub.agent_id = s.agent_id
         WHERE t.id = $1 AND s.oauth_app_id = $2 AND s.agent_id = $3
           AND app.external_platform_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $4
           AND (
               $7::uuid IS NULL
               OR (
                   s.owner_id = $7
                   AND hub.origin_tenant_id = $5
                   AND hub.origin_external_identity_id = $6
               )
           )",
    )
    .bind(tool_request_id)
    .bind(principal.oauth_app_id)
    .bind(principal.agent_id)
    .bind(principal.external_platform_id)
    .bind(principal.origin_tenant_id.as_deref())
    .bind(principal.origin_external_identity_id)
    .bind(principal.subject_user_id)
    .fetch_optional(pool)
    .await?;
    row.map(tool_request_from_row)
        .ok_or(ApiError::not_found("tool request not found"))
}

pub(crate) struct RuntimeToolRequestRegistration {
    pub(crate) request_id: Uuid,
    pub(crate) position: i32,
    pub(crate) tool_name: String,
    pub(crate) arguments: Value,
    pub(crate) event: FinalizeToolRequestEvent,
}

pub(crate) fn parse_tool_request_batch(
    request: &FinalizeToolRequestsRequest,
) -> Result<Vec<RuntimeToolRequestRegistration>, ApiError> {
    if request.native_session_id.trim().is_empty() || request.work_dir_ref.trim().is_empty() {
        return Err(ApiError::bad_request(
            "tool request resume metadata is required",
        ));
    }
    if request.tool_requests.is_empty() {
        return Err(ApiError::bad_request("tool request batch cannot be empty"));
    }
    let mut request_ids = BTreeSet::new();
    request
        .tool_requests
        .iter()
        .enumerate()
        .map(|(position, event)| {
            let tool_name = event
                .payload
                .get("tool_name")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or(ApiError::bad_request("tool request name is required"))?;
            let request_id = event
                .payload
                .get("tool_request_id")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
                .ok_or(ApiError::bad_request("valid tool request id is required"))?;
            if !request_ids.insert(request_id) {
                return Err(ApiError::bad_request(
                    "tool request ids must be unique within a batch",
                ));
            }
            let mut tool_name = tool_name.to_owned();
            sanitize_run_event_text(&mut tool_name);
            Ok(RuntimeToolRequestRegistration {
                request_id,
                position: i32::try_from(position)
                    .map_err(|_| ApiError::bad_request("tool request batch is too large"))?,
                tool_name,
                arguments: sanitize_run_event_payload(
                    event
                        .payload
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                ),
                event: event.clone(),
            })
        })
        .collect()
}

pub(crate) fn tool_request_batch_fingerprint(
    run_id: Uuid,
    request: &FinalizeToolRequestsRequest,
) -> Result<String, ApiError> {
    let value = serde_json::to_value(request)
        .map_err(|_| ApiError::internal("failed to serialize tool request batch"))?;
    Ok(sha256_hex(&format!("{run_id}:{}", canonical_json(&value))))
}

pub(crate) async fn finalize_tool_request_batch_tx(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    runtime_id: Uuid,
    ownership_generation: i64,
    request: &FinalizeToolRequestsRequest,
    requests: &[RuntimeToolRequestRegistration],
    fingerprint: &str,
) -> Result<RunDto, ApiError> {
    let active_agent: Option<Uuid> = sqlx::query_scalar(ACTIVE_RUNTIME_TOOL_REQUEST_AGENT_SQL)
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?;
    if active_agent.is_none() {
        return Err(ApiError::forbidden(
            "agent is deleted or run does not exist",
        ));
    }
    let hub_session_id =
        lock_owned_session_for_run_tx(tx, run_id, runtime_id, ownership_generation).await?;
    let row = sqlx::query(ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL)
        .bind(run_id)
        .bind(runtime_id)
        .bind(ownership_generation)
        .fetch_optional(&mut **tx)
        .await?;
    let row = row.ok_or(ApiError::forbidden("runtime does not own an active run"))?;
    let integration_session_id: Option<Uuid> = row.get("integration_session_id");
    let run_hub_session_id: Uuid = row.get("hub_session_id");
    let hub_turn_id: Uuid = row.get("hub_turn_id");
    let client_instance_id: Option<Uuid> = row.get("client_instance_id");
    if integration_session_id != request.integration_session_id {
        return Err(ApiError::forbidden(
            "tool request batch session does not match its Run",
        ));
    }
    if integration_session_id.is_none() && client_instance_id.is_none() {
        return Err(ApiError::forbidden(
            "tool requests are only allowed for integration or Client Tool runs",
        ));
    }
    let session_tools = if let Some(integration_session_id) = integration_session_id {
        sqlx::query_scalar::<_, Value>(
            "SELECT tool_definitions FROM integration_sessions
             WHERE id = $1 FOR UPDATE",
        )
        .bind(integration_session_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::forbidden("integration run session changed"))?
    } else {
        Value::Array(Vec::new())
    };
    let tools = if client_instance_id.is_some() {
        let integration_enabled: bool = sqlx::query_scalar(
            "SELECT tool_allowlist ? 'integration'
             FROM agents WHERE id = $1 AND deleted_at IS NULL",
        )
        .bind(active_agent.expect("active Agent was checked above"))
        .fetch_one(&mut **tx)
        .await?;
        if !integration_enabled {
            return Err(ApiError::forbidden(
                "Agent does not allow Client Tool execution",
            ));
        }
        row.get::<Value, _>("client_tool_snapshot")
    } else {
        session_tools
    };
    if run_hub_session_id != hub_session_id {
        return Err(ApiError::conflict(
            "tool request Run Session changed while finalizing",
        ));
    }
    for request in requests {
        let registered = tools
            .as_array()
            .map(|items| {
                items.iter().any(|tool| {
                    tool.get("name").and_then(Value::as_str) == Some(request.tool_name.as_str())
                })
            })
            .unwrap_or(false);
        if !registered {
            return Err(ApiError::forbidden("tool is not registered for session"));
        }
    }
    let run_status: String = row.get("status");
    let run_native_session_id: Option<String> = row.get("native_session_id");
    let run_work_dir_ref: Option<String> = row.get("work_dir_ref");
    if run_status == "waiting_tool" {
        if run_native_session_id.as_deref() != Some(request.native_session_id.as_str())
            || run_work_dir_ref.as_deref() != Some(request.work_dir_ref.as_str())
        {
            return Err(ApiError::conflict(
                "tool request resume metadata does not match waiting run",
            ));
        }
        verify_tool_request_batch_replay(tx, run_id, integration_session_id, requests, fingerprint)
            .await?;
        return load_run_public_tx(tx, run_id).await;
    }

    let updated = sqlx::query(
        "UPDATE runs
         SET status = 'waiting_tool', native_session_id = $1, work_dir_ref = $2, updated_at = now()
         WHERE id = $3 AND runtime_id = $4 AND status = 'running'",
    )
    .bind(&request.native_session_id)
    .bind(&request.work_dir_ref)
    .bind(run_id)
    .bind(runtime_id)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::internal("tool request run was not transitioned"));
    }
    let mut status_payload = json!({ "status": "waiting_tool" });
    status_payload[TOOL_REQUEST_BATCH_FINGERPRINT_KEY] = Value::String(fingerprint.to_owned());
    insert_run_event_tx(
        tx,
        run_id,
        "status".into(),
        None,
        Some("waiting_tool".into()),
        status_payload,
    )
    .await?;
    let expires_at = Utc::now()
        + ChronoDuration::minutes(if client_instance_id.is_some() {
            CLIENT_TOOL_DEADLINE_MINUTES
        } else {
            30
        });
    for request in requests {
        let event_payload = if client_instance_id.is_some() {
            json!({
                "tool_call_id": request.request_id,
                "tool_name": request.tool_name,
                "arguments": request.arguments,
                "batch_id": run_id,
                "expires_at": expires_at,
            })
        } else {
            request.event.payload.clone()
        };
        insert_run_event_tx(
            tx,
            run_id,
            "tool_request".into(),
            request.event.role.clone(),
            request.event.content.clone(),
            event_payload,
        )
        .await?;
        record_integration_tool_request(
            tx,
            run_id,
            integration_session_id,
            hub_session_id,
            request,
            expires_at,
        )
        .await?;
    }
    // The execution engine completed the native Turn that produced the tool request. Close
    // the corresponding Hub Turn in the same transaction that makes the tool
    // request visible, so an immediately submitted result starts the next Turn.
    let turn_updated = sqlx::query(
        "UPDATE hub_session_turns
         SET status = 'waiting_tool', ended_at = COALESCE(ended_at, now()), updated_at = now()
         WHERE id = $1 AND session_id = $2 AND ownership_generation = $3
           AND status IN ('starting', 'running', 'in_progress')",
    )
    .bind(hub_turn_id)
    .bind(hub_session_id)
    .bind(ownership_generation)
    .execute(&mut **tx)
    .await?;
    if turn_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "tool request Turn is no longer active while finalizing",
        ));
    }
    let session_updated = sqlx::query(
        "UPDATE hub_sessions
         SET active_turn_id = NULL
         WHERE id = $1 AND runtime_owner_id = $2 AND ownership_generation = $3
           AND (active_turn_id IS NULL OR active_turn_id = $4)",
    )
    .bind(hub_session_id)
    .bind(runtime_id)
    .bind(ownership_generation)
    .bind(hub_turn_id)
    .execute(&mut **tx)
    .await?;
    if session_updated.rows_affected() != 1 {
        return Err(ApiError::conflict(
            "tool request Session changed while finalizing",
        ));
    }
    load_run_public_tx(tx, run_id).await
}

pub(crate) async fn verify_tool_request_batch_replay(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    integration_session_id: Option<Uuid>,
    requests: &[RuntimeToolRequestRegistration],
    fingerprint: &str,
) -> Result<(), ApiError> {
    let status_rows = sqlx::query(
        "SELECT payload
         FROM run_events
         WHERE run_id = $1 AND event_type = 'status'
           AND payload->>'status' = 'waiting_tool'",
    )
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    if status_rows.len() != 1
        || status_rows[0]
            .get::<Value, _>("payload")
            .get(TOOL_REQUEST_BATCH_FINGERPRINT_KEY)
            .and_then(Value::as_str)
            != Some(fingerprint)
    {
        return Err(ApiError::conflict(
            "tool request batch does not match waiting run",
        ));
    }

    let stored_requests = sqlx::query(
        "SELECT id, position, tool_name, arguments
         FROM integration_tool_requests
         WHERE run_id = $1 AND session_id IS NOT DISTINCT FROM $2",
    )
    .bind(run_id)
    .bind(integration_session_id)
    .fetch_all(&mut **tx)
    .await?;
    let stored_events = sqlx::query(
        "SELECT role, content, payload
         FROM run_events
         WHERE run_id = $1 AND event_type = 'tool_request'",
    )
    .bind(run_id)
    .fetch_all(&mut **tx)
    .await?;
    if stored_requests.len() != requests.len() || stored_events.len() != requests.len() {
        return Err(ApiError::conflict(
            "tool request batch is incomplete for waiting run",
        ));
    }
    for request in requests {
        let request_matches = stored_requests.iter().filter(|row| {
            row.get::<Uuid, _>("id") == request.request_id
                && row.get::<i32, _>("position") == request.position
                && row.get::<String, _>("tool_name") == request.tool_name
                && row.get::<Value, _>("arguments") == request.arguments
        });
        let event_matches = stored_events.iter().filter(|row| {
            let payload = row.get::<Value, _>("payload");
            payload
                .get("tool_request_id")
                .or_else(|| payload.get("tool_call_id"))
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
                == Some(request.request_id)
        });
        if request_matches.count() != 1 || event_matches.count() != 1 {
            return Err(ApiError::conflict(
                "tool request batch does not match waiting run",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn record_integration_tool_request(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    integration_session_id: Option<Uuid>,
    hub_session_id: Uuid,
    request: &RuntimeToolRequestRegistration,
    expires_at: DateTime<Utc>,
) -> Result<(), ApiError> {
    let inserted = sqlx::query(INTEGRATION_TOOL_REQUEST_INSERT_SQL)
        .bind(request.request_id)
        .bind(integration_session_id)
        .bind(hub_session_id)
        .bind(run_id)
        .bind(request.position)
        .bind(&request.tool_name)
        .bind(&request.arguments)
        .bind(expires_at)
        .execute(&mut **tx)
        .await?;
    if inserted.rows_affected() != 1 {
        return Err(ApiError::internal(
            "integration tool request was not inserted",
        ));
    }
    Ok(())
}

pub(crate) async fn load_widget_credential_tx(
    tx: &mut Transaction<'_, Postgres>,
    token: &str,
    headers: &HeaderMap,
) -> Result<WidgetCredential, ApiError> {
    let row = sqlx::query(
        "SELECT embed.id, embed.agent_id, agent.owner_id AS agent_owner_id,
                embed.owner_id, embed.hub_session_id, embed.oauth_app_id,
                app.external_platform_id, embed.external_tenant_id,
                embed.external_user_id, embed.external_identity_id,
                embed.profile_snapshot, embed.expires_at,
                COALESCE(app.widget_history_enabled, false) AS history_enabled,
                embed.anonymous, embed.client_instance_id,
                embed.client_tool_definitions,
                COALESCE(app.allowed_origins, '[]'::jsonb) AS allowed_origins
         FROM embed_sessions AS embed
         JOIN agents AS agent ON agent.id = embed.agent_id AND agent.deleted_at IS NULL
         JOIN users AS session_owner
           ON session_owner.id = embed.owner_id
          AND session_owner.deletion_requested_at IS NULL
         LEFT JOIN oauth_apps AS app ON app.id = embed.oauth_app_id
         WHERE embed.token_hash = $1 AND embed.expires_at > now()
           AND (
               embed.oauth_app_id IS NULL
               OR (
                   app.deleted_at IS NULL AND app.client_secret_hash IS NOT NULL
                   AND EXISTS (
                       SELECT 1 FROM users AS app_owner
                       WHERE app_owner.id = app.owner_id
                         AND app_owner.deletion_requested_at IS NULL
                   )
                   AND (embed.anonymous = false OR app.login_required = false)
                   AND EXISTS (
                       SELECT 1 FROM authentication_channels AS channel
                       WHERE channel.id = app.authentication_channel_id
                         AND channel.platform_id = app.external_platform_id
                         AND channel.enabled = true AND channel.trusted_email = true
                   )
                   AND EXISTS (
                       SELECT 1 FROM integration_app_agents AS delegated
                       WHERE delegated.app_id = app.id AND delegated.agent_id = agent.id
                         AND (agent.owner_id = app.owner_id OR agent.visibility = 'public'
                              OR (agent.visibility = 'public_to'
                                  AND app.owner_id = ANY(agent.public_to)))
                   )
                   AND (embed.client_instance_id IS NULL
                        OR jsonb_array_length(embed.client_tool_definitions) = 0
                        OR agent.tool_allowlist ? 'integration')
                   AND (
                       embed.external_identity_id IS NULL
                       OR EXISTS (
                           SELECT 1 FROM external_identities AS identity
                           WHERE identity.id = embed.external_identity_id
                             AND identity.user_id = embed.owner_id
                             AND identity.platform_id = app.external_platform_id
                             AND identity.tenant_id = embed.external_tenant_id
                             AND identity.external_user_id = embed.external_user_id
                       )
                   )
               )
           )
         ",
    )
    .bind(sha256_hex(token))
    .fetch_optional(&mut **tx)
    .await?;
    let credential = row
        .map(widget_credential_from_row)
        .transpose()?
        .ok_or(ApiError::unauthorized("invalid embed session"))?;
    if credential.client_instance_id.is_some() {
        validate_client_request_origin(
            headers,
            &credential.allowed_origins,
            credential.is_anonymous() || !credential.allowed_origins.is_empty(),
        )?;
    }
    Ok(credential)
}

pub(crate) async fn load_widget_scoped_session_tx(
    tx: &mut Transaction<'_, Postgres>,
    credential: &WidgetCredential,
    integration_session_id: Option<Uuid>,
    hub_session_id: Option<Uuid>,
    lock: bool,
) -> Result<WidgetScopedSession, ApiError> {
    if integration_session_id.is_none() && hub_session_id.is_none() {
        return Err(ApiError::bad_request("Widget Session id is required"));
    }
    if credential.is_anonymous() {
        if integration_session_id.is_some() {
            return Err(ApiError::bad_request(
                "public Widget does not use Integration Sessions",
            ));
        }
        let requested_hub_session_id = hub_session_id.ok_or(ApiError::bad_request(
            "public Widget Session id is required",
        ))?;
        let lock_clause = if lock { " FOR UPDATE" } else { "" };
        let statement = format!(
            "SELECT id FROM hub_sessions
             WHERE id = $1 AND id = $2 AND agent_id = $3 AND owner_id = $4
               AND origin_kind = 'public_widget'{lock_clause}"
        );
        let row = sqlx::query(&statement)
            .bind(requested_hub_session_id)
            .bind(credential.hub_session_id)
            .bind(credential.agent_id)
            .bind(credential.owner_id)
            .fetch_optional(&mut **tx)
            .await?
            .ok_or(ApiError::not_found("Widget Session not found"))?;
        return Ok(WidgetScopedSession {
            integration_session_id: None,
            hub_session_id: row.get("id"),
        });
    }
    if !credential.is_external() {
        if integration_session_id.is_some()
            || hub_session_id != credential.hub_session_id
            || credential.hub_session_id.is_none()
        {
            return Err(ApiError::not_found("Widget Session not found"));
        }
        return Ok(WidgetScopedSession {
            integration_session_id: None,
            hub_session_id: credential.hub_session_id.expect("checked above"),
        });
    }
    let (
        oauth_app_id,
        external_platform_id,
        external_tenant_id,
        external_user_id,
        external_identity_id,
    ) = credential.external_scope()?;
    let lock_clause = if lock {
        " FOR UPDATE OF integration"
    } else {
        ""
    };
    let statement = format!(
        "SELECT integration.id, integration.hub_session_id
         FROM integration_sessions AS integration
         JOIN hub_sessions AS hub
           ON hub.id = integration.hub_session_id
          AND hub.owner_id = integration.owner_id
          AND hub.agent_id = integration.agent_id
         WHERE integration.oauth_app_id = $1
           AND integration.agent_id = $2
           AND integration.owner_id = $3
           AND integration.external_user_id = $4
           AND hub.origin_kind = 'external'
           AND hub.origin_platform_id = $5
           AND hub.origin_tenant_id = $6
           AND hub.origin_external_identity_id = $7
           AND ($8::uuid IS NULL OR integration.id = $8)
           AND ($9::uuid IS NULL OR hub.id = $9){lock_clause}"
    );
    let row = sqlx::query(&statement)
        .bind(oauth_app_id)
        .bind(credential.agent_id)
        .bind(credential.owner_id)
        .bind(external_user_id)
        .bind(external_platform_id)
        .bind(external_tenant_id)
        .bind(external_identity_id)
        .bind(integration_session_id)
        .bind(hub_session_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(ApiError::not_found("Widget Session not found"))?;
    Ok(WidgetScopedSession {
        integration_session_id: Some(row.get("id")),
        hub_session_id: row.get("hub_session_id"),
    })
}

pub(crate) async fn authorize_widget_session(
    state: &AppState,
    headers: &HeaderMap,
    session_id: Uuid,
) -> Result<(), ApiError> {
    let token = client_access_token_from_headers(headers)
        .ok_or(ApiError::unauthorized("missing embed session"))?;
    let mut tx = state.pool.begin().await?;
    let credential = load_widget_credential_tx(&mut tx, &token, headers).await?;
    let (integration_session_id, hub_session_id) = widget_session_locator(&credential, session_id);
    load_widget_scoped_session_tx(
        &mut tx,
        &credential,
        integration_session_id,
        hub_session_id,
        false,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn load_widget_session_events_after_tx(
    tx: &mut Transaction<'_, Postgres>,
    session: &WidgetScopedSession,
    after: i64,
    limit: Option<i64>,
) -> Result<Vec<RunEventDto>, ApiError> {
    let rows = if let Some(integration_session_id) = session.integration_session_id {
        let mut query = String::from(
            "SELECT event.seq, event.event_id, event.run_id, event.event_type,
                    event.role, event.content, event.payload, event.created_at
             FROM run_events AS event
             JOIN runs ON runs.id = event.run_id
             WHERE runs.integration_session_id = $1
               AND runs.hub_session_id = $2
               AND event.seq > $3",
        );
        if let Some(limit) = limit {
            // 取最近 n 条（倒序截取后恢复正序），用于历史恢复场景。
            query = format!(
                "SELECT * FROM ({query} ORDER BY event.seq DESC LIMIT {}) AS recent ORDER BY seq ASC",
                limit.clamp(1, 2000)
            );
        } else {
            query.push_str(" ORDER BY event.seq ASC");
        }
        sqlx::query(&query)
            .bind(integration_session_id)
            .bind(session.hub_session_id)
            .bind(after)
            .fetch_all(&mut **tx)
            .await?
    } else {
        let mut query = String::from(
            "SELECT event.seq, event.event_id, event.run_id, event.event_type,
                    event.role, event.content, event.payload, event.created_at
             FROM run_events AS event
             JOIN runs ON runs.id = event.run_id
             WHERE runs.hub_session_id = $1
               AND event.seq > $2",
        );
        if let Some(limit) = limit {
            // 取最近 n 条（倒序截取后恢复正序），用于历史恢复场景。
            query = format!(
                "SELECT * FROM ({query} ORDER BY event.seq DESC LIMIT {}) AS recent ORDER BY seq ASC",
                limit.clamp(1, 2000)
            );
        } else {
            query.push_str(" ORDER BY event.seq ASC");
        }
        sqlx::query(&query)
            .bind(session.hub_session_id)
            .bind(after)
            .fetch_all(&mut **tx)
            .await?
    };
    Ok(rows.into_iter().map(event_from_row).collect())
}

pub(crate) async fn lock_widget_credential_tx(
    tx: &mut Transaction<'_, Postgres>,
    token: &str,
    headers: &HeaderMap,
) -> Result<WidgetCredential, ApiError> {
    // Keep the shared Agent -> Widget credential lock order used by deletion paths.
    let preview = sqlx::query(
        "SELECT embed.agent_id, agent.owner_id AS agent_owner_id
         FROM embed_sessions AS embed
         JOIN agents AS agent ON agent.id = embed.agent_id AND agent.deleted_at IS NULL
         WHERE embed.token_hash = $1 AND embed.expires_at > now()",
    )
    .bind(sha256_hex(token))
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::unauthorized("invalid embed session"))?;
    let agent_id: Uuid = preview.get("agent_id");
    let agent_owner_id: Uuid = preview.get("agent_owner_id");
    lock_active_integration_agent_tx(tx, agent_id, agent_owner_id).await?;
    sqlx::query(
        "SELECT id FROM embed_sessions
         WHERE token_hash = $1 AND expires_at > now()
         FOR UPDATE",
    )
    .bind(sha256_hex(token))
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::unauthorized("invalid embed session"))?;
    let credential = load_widget_credential_tx(tx, token, headers).await?;
    if credential.agent_id != agent_id || credential.agent_owner_id != agent_owner_id {
        return Err(ApiError::internal(
            "Widget credential Agent changed while locking",
        ));
    }
    Ok(credential)
}

pub(crate) fn widget_credential_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<WidgetCredential, ApiError> {
    Ok(WidgetCredential {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        agent_owner_id: row.get("agent_owner_id"),
        owner_id: row.get("owner_id"),
        hub_session_id: row.get("hub_session_id"),
        oauth_app_id: row.get("oauth_app_id"),
        external_platform_id: row.get("external_platform_id"),
        external_tenant_id: row.get("external_tenant_id"),
        external_user_id: row.get("external_user_id"),
        external_identity_id: row.get("external_identity_id"),
        profile_snapshot: row.get("profile_snapshot"),
        expires_at: row.get("expires_at"),
        history_enabled: row.get("history_enabled"),
        anonymous: row.get("anonymous"),
        client_instance_id: row.get("client_instance_id"),
        client_tool_definitions: serde_json::from_value(row.get("client_tool_definitions"))
            .map_err(|_| ApiError::internal("stored Client Tool definitions are invalid"))?,
        allowed_origins: serde_json::from_value(row.get("allowed_origins"))
            .map_err(|_| ApiError::internal("stored Client Origin policy is invalid"))?,
    })
}

pub(crate) async fn verify_embed_jwt_claims(
    state: &AppState,
    jwt: &str,
) -> Result<AuthPrincipal, ApiError> {
    let parts = jwt.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(ApiError::unauthorized("invalid embed jwt"));
    }
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac = HmacSha256::new_from_slice(state.embed_jwt_secret.as_bytes())
        .map_err(|_| ApiError::internal("invalid embed jwt secret"))?;
    mac.update(signing_input.as_bytes());
    let expected = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    if !constant_time_eq(expected.as_bytes(), parts[2].as_bytes()) {
        return Err(ApiError::unauthorized("invalid embed jwt signature"));
    }

    let payload = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| ApiError::unauthorized("invalid embed jwt payload"))?;
    let payload: Value = serde_json::from_slice(&payload)
        .map_err(|_| ApiError::unauthorized("invalid embed jwt payload"))?;
    let header = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|_| ApiError::unauthorized("invalid embed jwt header"))?;
    let header: Value = serde_json::from_slice(&header)
        .map_err(|_| ApiError::unauthorized("invalid embed jwt header"))?;
    if header.get("alg").and_then(Value::as_str) != Some("HS256") {
        return Err(ApiError::unauthorized("invalid embed jwt alg"));
    }
    let issuer = payload
        .get("iss")
        .and_then(Value::as_str)
        .ok_or(ApiError::unauthorized("missing embed jwt issuer"))?;
    if issuer != state.embed_jwt_issuer {
        return Err(ApiError::unauthorized("invalid embed jwt issuer"));
    }
    if !jwt_audience_matches(payload.get("aud"), &state.embed_jwt_audience) {
        return Err(ApiError::unauthorized("invalid embed jwt audience"));
    }
    let exp = payload
        .get("exp")
        .and_then(Value::as_i64)
        .ok_or(ApiError::unauthorized("missing embed jwt expiry"))?;
    if exp <= Utc::now().timestamp() {
        return Err(ApiError::unauthorized("embed jwt expired"));
    }
    validate_embed_jwt_iat(&payload, Utc::now().timestamp())?;
    let jti = payload
        .get("jti")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ApiError::unauthorized("missing embed jwt jti"))?
        .to_owned();
    let agent_id = payload
        .get("agent_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiError::unauthorized("missing embed jwt agent"))?;
    let owner_id = payload
        .get("owner_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or(ApiError::unauthorized("missing embed jwt owner"))?;
    let subject = payload
        .get("sub")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ApiError::unauthorized("missing embed jwt subject"))?
        .to_owned();
    let inserted = sqlx::query(
        "INSERT INTO embed_jwt_replays (jti, expires_at)
         VALUES ($1, to_timestamp($2))
         ON CONFLICT DO NOTHING",
    )
    .bind(&jti)
    .bind(exp as f64)
    .execute(&state.pool)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(ApiError::unauthorized("embed jwt replayed"));
    }
    Ok(AuthPrincipal::Embed {
        owner_id,
        agent_id,
        _subject: subject,
    })
}

pub(crate) fn jwt_audience_matches(audience: Option<&Value>, expected: &str) -> bool {
    match audience {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    }
}

pub(crate) fn validate_embed_jwt_iat(payload: &Value, now: i64) -> Result<(), ApiError> {
    let issued_at = payload
        .get("iat")
        .and_then(Value::as_i64)
        .ok_or(ApiError::unauthorized("missing embed jwt issued-at"))?;
    if issued_at > now + 60 {
        return Err(ApiError::unauthorized("embed jwt issued in the future"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{issue_widget_external_access_for, runtime_write, runtime_write_generation};
    use crate::{
        build_router, ACTIVE_RUNTIME_TOOL_REQUEST_AGENT_SQL, ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL,
        INTEGRATION_TOOL_REQUEST_INSERT_SQL, TOOL_RESULT_TRUNCATE_BYTES,
    };
    use axum::{
        body::Body,
        extract::{Form, Path, Query, State},
        http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
        Json,
    };
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;
    use url::Url;
    use uuid::Uuid;

    #[test]
    fn client_tool_definitions_are_bounded_and_protocol_neutral() {
        let valid = ClientToolDefinitionDto {
            name: "open_panel".into(),
            description: "Open one application panel".into(),
            input_schema: json!({
                "type": "object",
                "properties": { "panel_id": { "type": "string" } },
                "required": ["panel_id"]
            }),
        };
        assert_eq!(
            validate_client_tool_definitions(std::slice::from_ref(&valid)).unwrap(),
            serde_json::to_value([&valid]).unwrap()
        );

        for invalid in [
            ClientToolDefinitionDto {
                name: "bad name".into(),
                ..valid.clone()
            },
            ClientToolDefinitionDto {
                description: " ".into(),
                ..valid.clone()
            },
            ClientToolDefinitionDto {
                input_schema: json!({ "type": "string" }),
                ..valid.clone()
            },
        ] {
            assert_eq!(
                validate_client_tool_definitions(&[invalid])
                    .unwrap_err()
                    .status,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            validate_client_tool_definitions(&[valid.clone(), valid])
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );

        let document = openapi_document();
        assert_eq!(
            document["components"]["schemas"]["ClientToolDefinition"]["properties"]["input_schema"]
                ["type"],
            "object"
        );
        assert_eq!(
            document["components"]["schemas"]["IntegrationApp"]["properties"]
                ["client_tool_definitions"]["maxItems"],
            128
        );
        for path in [
            "/api/client/access",
            "/api/client/anonymous/access",
            "/api/client/renew",
            "/api/client/tool-calls/{tool_call_id}/claim",
            "/api/client/tool-calls/{tool_call_id}/result",
        ] {
            assert!(document["paths"].get(path).is_some(), "missing {path}");
        }
        assert_eq!(
            document["paths"]["/api/client/renew"]["post"]["security"],
            json!([{ "clientAccessBearer": [] }])
        );
        assert_eq!(
            document["components"]["schemas"]["ClientAccessResponse"]["properties"]["expires_in"]
                ["minimum"],
            1
        );
        assert_eq!(
            document["components"]["schemas"]["SubmitClientToolResultRequest"]["properties"]
                ["result"]["$ref"],
            "#/components/schemas/ClientToolResult"
        );
    }

    #[test]
    fn client_request_origin_enforces_exact_optional_and_required_policies() {
        let allowed = vec!["https://app.example.test".to_owned()];
        let mut headers = HeaderMap::new();
        assert!(validate_client_request_origin(&headers, &[], false).is_ok());
        assert_eq!(
            validate_client_request_origin(&headers, &allowed, true)
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://app.example.test"),
        );
        assert!(validate_client_request_origin(&headers, &allowed, true).is_ok());
        assert!(validate_client_request_origin(&headers, &[], false).is_ok());

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://other.example.test"),
        );
        assert_eq!(
            validate_client_request_origin(&headers, &allowed, true)
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );

        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("https://hub.example.test"),
        );
        headers.insert(
            HeaderName::from_static("x-agent-hub-embedded-origin"),
            HeaderValue::from_static("https://app.example.test"),
        );
        headers.insert(
            HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("same-origin"),
        );
        assert!(validate_client_request_origin(&headers, &allowed, true).is_ok());

        headers.insert(
            HeaderName::from_static("sec-fetch-site"),
            HeaderValue::from_static("cross-site"),
        );
        assert_eq!(
            validate_client_request_origin(&headers, &allowed, true)
                .unwrap_err()
                .status,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn cors_reflects_client_origins_without_opening_control_plane_routes() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(20))
            .connect_lazy("postgres://agent-hub:agent-hub@127.0.0.1:1/agent_hub")
            .unwrap();
        let app = build_router(test_state_with_pool(pool));
        let client_preflight = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/client/renew")
                    .header(header::ORIGIN, "https://app.example.test")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            client_preflight
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://app.example.test"))
        );

        let control_preflight = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/agents")
                    .header(header::ORIGIN, "https://app.example.test")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(control_preflight
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());
    }

    #[test]
    fn widget_agent_redaction_returns_minimal_dto() {
        let mut agent = test_agent();
        agent.managed_skill_ids = vec![Uuid::new_v4()];
        let agent_id = agent.id;

        let widget_agent = widget_agent_from_agent(agent);

        assert_eq!(widget_agent.id, agent_id);
        assert_eq!(widget_agent.name, "Test Agent");
        assert_eq!(widget_agent.instructions, "Test instructions");
    }

    #[test]
    fn oauth_redirect_encodes_reserved_state_characters() {
        let location = oauth_redirect_location(
            "https://client.example/callback?existing=1",
            "code value",
            Some("a&b=#value"),
        )
        .unwrap();
        let parsed = Url::parse(&location).unwrap();
        let pairs = parsed
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(pairs.get("existing").map(|value| value.as_ref()), Some("1"));
        assert_eq!(
            pairs.get("code").map(|value| value.as_ref()),
            Some("code value")
        );
        assert_eq!(
            pairs.get("state").map(|value| value.as_ref()),
            Some("a&b=#value")
        );
        assert!(validate_oauth_redirect_uri("http://example.com/callback").is_err());
        assert!(validate_oauth_redirect_uri("http://127.0.0.1:3000/callback").is_ok());
    }

    #[test]
    fn oauth_scopes_are_explicit_and_userinfo_fields_follow_scope_intersection() {
        let agent_id = Uuid::new_v4();
        assert_eq!(
            parse_oauth_scopes(None, true).unwrap(),
            vec!["email", "external_profile", "profile"]
        );
        let scopes = parse_oauth_scopes(
            Some(&format!(
                "profile email external_profile agent:{agent_id} profile"
            )),
            true,
        )
        .unwrap();
        assert_eq!(
            scopes,
            vec![
                format!("agent:{agent_id}"),
                "email".into(),
                "external_profile".into(),
                "profile".into()
            ]
        );
        assert_eq!(
            parse_oauth_scopes(Some("unknown"), false)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );

        let user = UserDto {
            id: Uuid::new_v4(),
            email: "hub@example.com".into(),
            display_name: "Hub User".into(),
            role: "member".into(),
        };
        let external = OAuthExternalProfileDto {
            platform_id: Uuid::new_v4(),
            tenant_id: "tenant-one".into(),
            external_identity_id: Uuid::new_v4(),
            external_user_id: "external-user".into(),
            username: Some("external-name".into()),
            email: Some("external@example.com".into()),
        };
        let external_only = project_oauth_userinfo(
            &BTreeSet::from(["external_profile".to_owned()]),
            &user,
            external.clone(),
        );
        assert_eq!(external_only.sub, user.id);
        assert_eq!(external_only.email, None);
        assert_eq!(external_only.external_profile.unwrap().email, None);

        let full = project_oauth_userinfo(
            &BTreeSet::from([
                "profile".to_owned(),
                "email".to_owned(),
                "external_profile".to_owned(),
            ]),
            &user,
            external,
        );
        assert_eq!(full.name.as_deref(), Some("Hub User"));
        assert_eq!(full.email.as_deref(), Some("hub@example.com"));
        assert_eq!(
            full.external_profile.unwrap().email.as_deref(),
            Some("external@example.com")
        );
    }

    #[test]
    fn integration_attachments_enforce_shape_and_limits() {
        assert!(validate_integration_attachments(&json!([{
            "kind": "text",
            "name": "note.txt",
            "size_bytes": 4,
            "text": "note"
        }]))
        .is_ok());
        assert!(validate_integration_attachments(&json!([{
            "kind": "file",
            "name": "payload.bin",
            "size_bytes": 4
        }]))
        .is_err());
        assert!(validate_integration_attachments(&json!([{
            "kind": "url",
            "name": "local",
            "size_bytes": 0,
            "url": "file:///etc/passwd"
        }]))
        .is_err());
    }

    #[test]
    fn embed_jwt_iat_is_required_and_not_future_dated() {
        assert!(validate_embed_jwt_iat(&json!({ "iat": 1_000 }), 1_000).is_ok());
        assert!(validate_embed_jwt_iat(&json!({ "iat": 1_060 }), 1_000).is_ok());
        assert!(validate_embed_jwt_iat(&json!({}), 1_000).is_err());
        assert!(validate_embed_jwt_iat(&json!({ "iat": 1_061 }), 1_000).is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn integration_app_options_are_available_to_members_and_filter_channels(pool: PgPool) {
        let administrator = create_hub_user(
            &pool,
            Some("integration-options-admin@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let member = create_hub_user(
            &pool,
            Some("integration-options-member@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(member.role, "member");
        let session_token = "integration-options-member-session";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(session_token))
        .bind(member.id)
        .execute(&pool)
        .await
        .unwrap();
        let valid_platform_id = Uuid::new_v4();
        let invalid_platform_id = Uuid::new_v4();
        for (id, key, name) in [
            (valid_platform_id, "options-valid", "Options Valid"),
            (invalid_platform_id, "options-invalid", "Options Invalid"),
        ] {
            sqlx::query("INSERT INTO external_platforms (id, key, name) VALUES ($1, $2, $3)")
                .bind(id)
                .bind(key)
                .bind(name)
                .execute(&pool)
                .await
                .unwrap();
        }
        let valid_channel_id = Uuid::new_v4();
        for (id, platform_id, key, enabled, trusted_email) in [
            (valid_channel_id, valid_platform_id, "valid", true, true),
            (Uuid::new_v4(), valid_platform_id, "disabled", false, true),
            (Uuid::new_v4(), valid_platform_id, "untrusted", true, false),
            (
                Uuid::new_v4(),
                invalid_platform_id,
                "invalid-platform-only",
                false,
                false,
            ),
        ] {
            sqlx::query(
                "INSERT INTO authentication_channels
                     (id, platform_id, key, name, enabled, trusted_email, created_by)
                 VALUES ($1, $2, $3, $3, $4, $5, $6)",
            )
            .bind(id)
            .bind(platform_id)
            .bind(key)
            .bind(enabled)
            .bind(trusted_email)
            .bind(administrator.id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let options = get_integration_app_options(
            State(Arc::new(test_state_with_browser_session_auth(pool))),
            session_headers(session_token),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            options
                .external_platforms
                .iter()
                .map(|platform| platform.id)
                .collect::<Vec<_>>(),
            vec![valid_platform_id]
        );
        assert_eq!(
            options
                .authentication_channels
                .iter()
                .map(|channel| channel.id)
                .collect::<Vec<_>>(),
            vec![valid_channel_id]
        );
        assert!(options
            .authentication_channels
            .iter()
            .all(|channel| channel.enabled && channel.trusted_email));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn integration_app_manages_multiple_agents_and_shows_secrets_once(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("oauth-origin-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let owner_token = "oauth-origin-owner-token";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(owner_token))
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let agent_ids = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, agent_id) in agent_ids.into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO agents (id, owner_id, name, instructions, visibility)
                 VALUES ($1, $2, $3, 'test', 'private')",
            )
            .bind(agent_id)
            .bind(owner.id)
            .bind(format!("Integration Agent {index}"))
            .execute(&pool)
            .await
            .unwrap();
        }
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'integration-test', 'Integration Test')",
        )
        .bind(platform_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email, created_by)
             VALUES ($1, $2, 'default', 'Default', true, true, $3)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));

        let Json(created) = create_integration_app(
            State(state.clone()),
            session_headers(owner_token),
            Json(CreateIntegrationAppRequest {
                name: "Origin App".into(),
                external_platform_id: platform_id,
                authentication_channel_id: channel_id,
                redirect_uris: json!(["https://client.example.com/callback"]),
                agent_ids: agent_ids.to_vec(),
                widget_history_enabled: false,
                login_required: true,
                allowed_origins: Vec::new(),
                tool_allowlist: None,
                client_tool_definitions: Vec::new(),
            }),
        )
        .await
        .unwrap();
        let first_secret = created.client_secret;
        let mut expected_agent_ids = agent_ids.to_vec();
        expected_agent_ids.sort_unstable();
        assert_eq!(created.integration_app.agent_ids, expected_agent_ids);

        let listed = list_integration_apps(State(state.clone()), session_headers(owner_token))
            .await
            .unwrap()
            .0;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.integration_app.id);

        let updated = update_integration_app(
            State(state.clone()),
            session_headers(owner_token),
            Path(created.integration_app.id),
            Json(UpdateIntegrationAppRequest {
                name: "Updated App".into(),
                redirect_uris: json!(["https://client.example.com/new-callback"]),
                agent_ids: vec![agent_ids[1]],
                widget_history_enabled: true,
                login_required: true,
                allowed_origins: Vec::new(),
                tool_allowlist: None,
                client_tool_definitions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated.agent_ids, vec![agent_ids[1]]);
        assert!(updated.widget_history_enabled);

        let rotated = rotate_integration_app_secret(
            State(state.clone()),
            session_headers(owner_token),
            Path(created.integration_app.id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(rotated.integration_app.id, created.integration_app.id);
        assert_ne!(rotated.client_secret, first_secret);
        let old_secret_error = oauth_token(
            State(state.clone()),
            Form(OAuthTokenForm {
                grant_type: "client_credentials".into(),
                client_id: created.integration_app.client_id.clone(),
                client_secret: first_secret,
                code: None,
                redirect_uri: None,
                scope: Some(format!("agent:{}", agent_ids[1])),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(old_secret_error.status, StatusCode::UNAUTHORIZED);
        let new_token = oauth_token(
            State(state),
            Form(OAuthTokenForm {
                grant_type: "client_credentials".into(),
                client_id: created.integration_app.client_id,
                client_secret: rotated.client_secret,
                code: None,
                redirect_uri: None,
                scope: Some(format!("agent:{}", agent_ids[1])),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(new_token.access_token.starts_with("aho_"));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn integration_app_client_credentials_issue_scoped_widget_access_without_creating_a_session(
        pool: PgPool,
    ) {
        let owner = create_hub_user(
            &pool,
            Some("widget-access-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let app_id = Uuid::new_v4();
        let client_id = format!("widget-access-{}", Uuid::new_v4().simple());
        let client_secret = "widget-access-client-secret";
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Widget Access Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, $2, 'Widget Access Platform')",
        )
        .bind(platform_id)
        .bind(format!("widget-access-{}", Uuid::new_v4().simple()))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email, created_by)
             VALUES ($1, $2, 'widget', 'Widget', true, true, $3)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO oauth_apps
                 (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
                  external_platform_id, authentication_channel_id, widget_history_enabled)
             VALUES ($1, $2, 'Widget Access App', $3, $4, '[]'::jsonb, $5, $6, false)",
        )
        .bind(app_id)
        .bind(owner.id)
        .bind(&client_id)
        .bind(sha256_hex(client_secret))
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

        let app = build_router(test_state_with_pool(pool.clone()));
        let basic = base64::engine::general_purpose::STANDARD
            .encode(format!("{client_id}:{client_secret}"));
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/access")
                    .header(header::AUTHORIZATION, format!("Basic {basic}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "agent_id": agent_id,
                            "client_instance_id": Uuid::new_v4(),
                            "tenant_id": "tenant-acme",
                            "external_user_id": "external-user-42",
                            "username": "external-user",
                            "display_name": "External User",
                            "email": "external-user@example.com",
                            "attributes": { "plan": "pro", "locale": "zh-CN" }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let issued: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let token = issued["token"].as_str().unwrap();
        assert!(token.starts_with("ahw_"));
        assert_eq!(issued["agent"]["id"], json!(agent_id));
        assert_eq!(issued["history_enabled"], false);
        assert!(issued["expires_at"].as_str().is_some());

        let stored = sqlx::query(
            "SELECT oauth_app_id, agent_id, owner_id, hub_session_id,
                    external_tenant_id, external_user_id, external_identity_id,
                    profile_snapshot
             FROM embed_sessions WHERE token_hash = $1",
        )
        .bind(sha256_hex(token))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored.get::<Option<Uuid>, _>("oauth_app_id"), Some(app_id));
        assert_eq!(stored.get::<Uuid, _>("agent_id"), agent_id);
        assert!(stored.get::<Option<Uuid>, _>("hub_session_id").is_none());
        assert_eq!(stored.get::<String, _>("external_tenant_id"), "tenant-acme");
        assert_eq!(
            stored.get::<String, _>("external_user_id"),
            "external-user-42"
        );
        assert_eq!(
            stored.get::<Value, _>("profile_snapshot")["display_name"],
            "External User"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hub_sessions")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let identity = sqlx::query(
            "SELECT last_email, last_username, last_display_name, attributes
             FROM external_identities
             WHERE id = $1",
        )
        .bind(stored.get::<Uuid, _>("external_identity_id"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            identity.get::<Option<String>, _>("last_email").as_deref(),
            Some("external-user@example.com")
        );
        assert_eq!(
            identity
                .get::<Option<String>, _>("last_username")
                .as_deref(),
            Some("external-user")
        );
        assert_eq!(
            identity
                .get::<Option<String>, _>("last_display_name")
                .as_deref(),
            Some("External User")
        );
        assert_eq!(identity.get::<Value, _>("attributes")["plan"], "pro");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_tool_continuation_run_supports_claim_stream_and_stop(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action", "second_action"]).await;
        // 第一批工具请求（原 widget run）。
        let _ = runtime_finalize_tool_requests(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run.id),
            runtime_write(FinalizeToolRequestsRequest {
                integration_session_id: fixture.run.integration_session_id,
                native_session_id: "client-tool-native-session".into(),
                work_dir_ref: "client-tool-workdir".into(),
                tool_requests: vec![FinalizeToolRequestEvent {
                    role: Some("assistant".into()),
                    content: Some("first_action requested".into()),
                    payload: json!({
                        "tool_request_id": fixture.tool_call_ids[0],
                        "tool_name": "first_action",
                        "arguments": { "position": 0 }
                    }),
                }],
            }),
        )
        .await
        .unwrap();
        // 浏览器认领并提交工具结果后，Hub 创建续跑 run（integration:tool_result）。
        let first_claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first_claim.status, "claimed");
        let submitted = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!({ "opened": true }),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap()
        .0;
        let continuation = submitted.run.unwrap();
        assert_eq!(continuation.source, "integration:tool_result");
        assert_eq!(continuation.parent_run_id, Some(fixture.run.id));

        // runtime 认领续跑 run 后，模型在续跑 turn 中再次请求工具。
        let generation: i64 =
            sqlx::query_scalar("SELECT ownership_generation FROM hub_sessions WHERE id = $1")
                .bind(fixture.run.hub_session_id.unwrap())
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap();
        let claimed_response = runtime_claim_run(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(
                0,
                vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.run.hub_session_id.unwrap(),
                    ownership_generation: generation,
                }],
            ),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claimed_response.status(), StatusCode::OK);
        let continuation_runtime: Uuid =
            sqlx::query_scalar("SELECT runtime_id FROM runs WHERE id = $1")
                .bind(continuation.id)
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap();
        let agent_runtime: Uuid = sqlx::query_scalar("SELECT runtime_id FROM agents WHERE id = $1")
            .bind(fixture.app.agent_id)
            .fetch_one(&fixture.app.state.pool)
            .await
            .unwrap();
        assert_eq!(continuation_runtime, agent_runtime);

        // 续跑 run 保留 Run Tool Executor（发起方浏览器）。
        let executor: Uuid =
            sqlx::query_scalar("SELECT client_instance_id FROM runs WHERE id = $1")
                .bind(continuation.id)
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap();
        assert_eq!(executor, fixture.executor.client_instance_id);

        // 续跑 run 中模型再次请求工具：runtime 激活续跑 turn 后提交第二个 batch。
        let fingerprint = format!("sha256:{}", "a".repeat(64));
        let _ = runtime_begin_turn(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(continuation.id),
            runtime_write_generation(
                generation,
                BeginRuntimeTurnRequest {
                    configuration_fingerprint: fingerprint,
                },
            ),
        )
        .await
        .unwrap()
        .0;
        let second_call_id = Uuid::new_v4();
        let _ = runtime_finalize_tool_requests(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(continuation.id),
            runtime_write(FinalizeToolRequestsRequest {
                integration_session_id: continuation.integration_session_id,
                native_session_id: "client-tool-native-session".into(),
                work_dir_ref: "client-tool-workdir".into(),
                tool_requests: vec![FinalizeToolRequestEvent {
                    role: Some("assistant".into()),
                    content: Some("second_action requested".into()),
                    payload: json!({
                        "tool_request_id": second_call_id,
                        "tool_name": "second_action",
                        "arguments": { "position": 0 }
                    }),
                }],
            }),
        )
        .await
        .unwrap();

        // 修复前：浏览器 claim 续跑 run 的工具会 404 "Client Tool Run not found"。
        let claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(second_call_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");

        // 续跑 run 的事件流可授权（修复前 forbidden）。
        authorize_run_stream(
            &fixture.app.state,
            &bearer_headers(&fixture.executor.access_token),
            continuation.id,
        )
        .await
        .unwrap();

        // 续跑 run 可被浏览器停止（修复前 404）。
        let stopped = stop_widget_run(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(continuation.id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stopped.status, "interrupted");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_tool_run_freezes_grant_and_rejects_another_instance(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["open_panel"]).await;
        let original_binding: (Uuid, Value) = sqlx::query_as(
            "SELECT client_instance_id, client_tool_snapshot FROM runs WHERE id = $1",
        )
        .bind(fixture.run.id)
        .fetch_one(&fixture.app.state.pool)
        .await
        .unwrap();
        assert_eq!(original_binding.0, fixture.executor.client_instance_id);
        assert_eq!(
            original_binding.1,
            test_client_tool_definitions(&["open_panel"])
        );

        let reauthorized = issue_client_access_for_instance(
            &fixture.app,
            fixture.executor.client_instance_id,
            "client-tool-tenant",
            "client-tool-user",
            test_client_tool_definitions(&["select_row"]),
        )
        .await;
        assert_ne!(reauthorized.access_token, fixture.executor.access_token);
        let next_run = create_canonical_client_run(
            &fixture.app,
            &reauthorized.access_token,
            None,
            "use the latest grant",
        )
        .await;
        let next_binding: (Uuid, Value) = sqlx::query_as(
            "SELECT client_instance_id, client_tool_snapshot FROM runs WHERE id = $1",
        )
        .bind(next_run.id)
        .fetch_one(&fixture.app.state.pool)
        .await
        .unwrap();
        assert_eq!(next_binding.0, reauthorized.client_instance_id);
        assert_eq!(
            next_binding.1,
            test_client_tool_definitions(&["select_row"])
        );
        assert_eq!(
            sqlx::query_scalar::<_, Value>("SELECT client_tool_snapshot FROM runs WHERE id = $1",)
                .bind(fixture.run.id)
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap(),
            test_client_tool_definitions(&["open_panel"])
        );

        let changed_grant_error = finalize_test_client_tool_batch(&fixture, &["select_row"])
            .await
            .unwrap_err();
        assert_eq!(changed_grant_error.status, StatusCode::FORBIDDEN);
        assert_eq!(
            finalize_test_client_tool_batch(&fixture, &["open_panel"])
                .await
                .unwrap()
                .0
                .status,
            "waiting_tool"
        );

        let observer_claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.observer.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap_err();
        assert_eq!(observer_claim.status, StatusCode::FORBIDDEN);
        let observer_result = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.observer.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!({ "ignored": true }),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(observer_result.status, StatusCode::FORBIDDEN);

        for _ in 0..2 {
            let claim = claim_client_tool_call(
                State(fixture.app.state.clone()),
                bearer_headers(&reauthorized.access_token),
                Path(fixture.tool_call_ids[0]),
            )
            .await
            .unwrap()
            .0;
            assert_eq!(claim.status, "claimed");
            assert!(!claim.terminal);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_tool_results_are_bounded_idempotent_and_continue_once_in_order(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action", "second_action"]).await;
        let _ = finalize_test_client_tool_batch(&fixture, &["first_action", "second_action"])
            .await
            .unwrap();
        let other_tab = issue_client_access_for_instance(
            &fixture.app,
            Uuid::new_v4(),
            "client-tool-tenant",
            "client-tool-user",
            test_client_tool_definitions(&["other_tab_action"]),
        )
        .await;
        let queued_during_tool_wait = create_canonical_client_run(
            &fixture.app,
            &other_tab.access_token,
            fixture.run.integration_session_id,
            "guide the waiting tool turn",
        )
        .await;
        assert_eq!(queued_during_tool_wait.status, "pending");
        let ownership_generation: i64 =
            sqlx::query_scalar("SELECT ownership_generation FROM hub_sessions WHERE id = $1")
                .bind(fixture.run.hub_session_id.unwrap())
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap();
        let blocked_claim = runtime_claim_run(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(
                0,
                vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.run.hub_session_id.unwrap(),
                    ownership_generation,
                }],
            ),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(blocked_claim.status(), StatusCode::NO_CONTENT);
        let first_call = fixture.tool_call_ids[0];
        let second_call = fixture.tool_call_ids[1];
        let first_state = fixture.app.state.clone();
        let second_state = fixture.app.state.clone();
        let first_token = fixture.executor.access_token.clone();
        let second_token = fixture.executor.access_token.clone();
        let (claim_a, claim_b) = tokio::join!(
            claim_client_tool_call(
                State(first_state),
                bearer_headers(&first_token),
                Path(first_call),
            ),
            claim_client_tool_call(
                State(second_state),
                bearer_headers(&second_token),
                Path(first_call),
            )
        );
        for claim in [claim_a.unwrap().0, claim_b.unwrap().0] {
            assert_eq!(claim.status, "claimed");
            assert!(!claim.terminal);
        }

        let first_result = ClientToolResultDto::Success {
            output: json!({ "opened": true }),
            truncated: None,
        };
        let first_response = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(first_call),
            Json(SubmitClientToolResultRequest {
                result: first_result.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(first_response.run.is_none());
        let repeated_first = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(first_call),
            Json(SubmitClientToolResultRequest {
                result: first_result,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(repeated_first.run.is_none());
        let divergent = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(first_call),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!({ "opened": false }),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(divergent.status, StatusCode::CONFLICT);

        let second_claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(second_call),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(second_claim.status, "claimed");
        let second_result = ClientToolResultDto::Error {
            error: ClientToolErrorDto {
                code: "user_rejected".into(),
                message: "The user rejected this action".into(),
                retryable: false,
            },
        };
        let completed = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(second_call),
            Json(SubmitClientToolResultRequest {
                result: second_result.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        let continuation = completed.run.unwrap();
        assert_eq!(continuation.id, queued_during_tool_wait.id);
        let continuation_binding: (String, Option<Uuid>, Option<Uuid>, Value, Option<Uuid>) =
            sqlx::query_as(
                "SELECT status, parent_run_id, client_instance_id,
                        client_tool_snapshot, widget_session_id
                 FROM runs WHERE id = $1",
            )
            .bind(continuation.id)
            .fetch_one(&fixture.app.state.pool)
            .await
            .unwrap();
        assert_eq!(continuation_binding.0, "pending");
        assert_eq!(continuation_binding.1, Some(fixture.run.id));
        assert_eq!(
            continuation_binding.2,
            Some(fixture.executor.client_instance_id)
        );
        assert_eq!(
            continuation_binding.3,
            test_client_tool_definitions(&["first_action", "second_action"])
        );
        let executor_credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&fixture.executor.access_token))
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap();
        assert_eq!(continuation_binding.4, Some(executor_credential_id));
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(fixture.run.id)
                .fetch_one(&fixture.app.state.pool)
                .await
                .unwrap(),
            "completed"
        );
        let repeated_second = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(second_call),
            Json(SubmitClientToolResultRequest {
                result: second_result,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(repeated_second.run.unwrap().id, continuation.id);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runs
                 WHERE parent_run_id = $1 AND source = 'integration:tool_result'",
            )
            .bind(fixture.run.id)
            .fetch_one(&fixture.app.state.pool)
            .await
            .unwrap(),
            1
        );
        let follow_up_ids: Vec<Option<Uuid>> = sqlx::query_scalar(
            "SELECT follow_up_run_id FROM integration_tool_requests
             WHERE run_id = $1 ORDER BY position",
        )
        .bind(fixture.run.id)
        .fetch_all(&fixture.app.state.pool)
        .await
        .unwrap();
        assert_eq!(
            follow_up_ids,
            vec![Some(continuation.id), Some(continuation.id)]
        );
        let payload: Value = sqlx::query_scalar(
            "SELECT payload FROM hub_session_messages
             WHERE session_id = $1 AND run_id = $2 AND message_kind = 'tool_result'",
        )
        .bind(fixture.run.hub_session_id.unwrap())
        .bind(continuation.id)
        .fetch_one(&fixture.app.state.pool)
        .await
        .unwrap();
        let results = payload["tool_results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["tool_call_id"], json!(first_call));
        assert_eq!(results[0]["tool_name"], "first_action");
        assert_eq!(results[0]["result"]["status"], "success");
        assert_eq!(results[1]["tool_call_id"], json!(second_call));
        assert_eq!(results[1]["tool_name"], "second_action");
        assert_eq!(results[1]["result"]["status"], "error");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_tool_timeout_stop_and_agent_gate_are_terminal_without_continuation(
        pool: PgPool,
    ) {
        let timed_out = prepare_client_tool_run(pool.clone(), &["slow_action"]).await;
        let _ = finalize_test_client_tool_batch(&timed_out, &["slow_action"])
            .await
            .unwrap();
        sqlx::query(
            "UPDATE integration_tool_requests
             SET status = 'unknown', expires_at = now() - interval '1 second'
             WHERE run_id = $1",
        )
        .bind(timed_out.run.id)
        .execute(&timed_out.app.state.pool)
        .await
        .unwrap();
        reap_expired_client_tool_batches(&timed_out.app.state.pool)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(timed_out.run.id)
                .fetch_one(&timed_out.app.state.pool)
                .await
                .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM integration_tool_requests WHERE id = $1",
            )
            .bind(timed_out.tool_call_ids[0])
            .fetch_one(&timed_out.app.state.pool)
            .await
            .unwrap(),
            "timed_out"
        );
        let timeout_claim = claim_client_tool_call(
            State(timed_out.app.state.clone()),
            bearer_headers(&timed_out.executor.access_token),
            Path(timed_out.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert!(timeout_claim.terminal);
        assert_eq!(timeout_claim.status, "timed_out");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE parent_run_id = $1")
                .bind(timed_out.run.id)
                .fetch_one(&timed_out.app.state.pool)
                .await
                .unwrap(),
            0
        );

        let stopped = prepare_client_tool_run(pool.clone(), &["stop_action"]).await;
        let _ = finalize_test_client_tool_batch(&stopped, &["stop_action"])
            .await
            .unwrap();
        let stopped_run = stop_widget_run(
            State(stopped.app.state.clone()),
            bearer_headers(&stopped.executor.access_token),
            Path(stopped.run.id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stopped_run.status, "interrupted");
        let cancelled_claim = claim_client_tool_call(
            State(stopped.app.state.clone()),
            bearer_headers(&stopped.executor.access_token),
            Path(stopped.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert!(cancelled_claim.terminal);
        assert_eq!(cancelled_claim.status, "cancelled");
        let cancelled_result = submit_client_tool_result(
            State(stopped.app.state.clone()),
            bearer_headers(&stopped.executor.access_token),
            Path(stopped.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!({ "too_late": true }),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(cancelled_result.status, StatusCode::GONE);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE parent_run_id = $1")
                .bind(stopped.run.id)
                .fetch_one(&stopped.app.state.pool)
                .await
                .unwrap(),
            0
        );

        let gated = prepare_client_tool_run(pool, &["gated_action"]).await;
        sqlx::query(
            "UPDATE agents SET tool_allowlist = tool_allowlist - 'integration' WHERE id = $1",
        )
        .bind(gated.app.agent_id)
        .execute(&gated.app.state.pool)
        .await
        .unwrap();
        let gated_error = finalize_test_client_tool_batch(&gated, &["gated_action"])
            .await
            .unwrap_err();
        assert_eq!(gated_error.status, StatusCode::FORBIDDEN);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM integration_tool_requests WHERE run_id = $1",
            )
            .bind(gated.run.id)
            .fetch_one(&gated.app.state.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(gated.run.id)
                .fetch_one(&gated.app.state.pool)
                .await
                .unwrap(),
            "running"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_renewal_rotates_the_same_credential_and_continues_its_selected_session(
        pool: PgPool,
    ) {
        let fixture = widget_external_test_fixture(pool, true).await;
        let issued =
            issue_widget_external_access(&fixture, "tenant-renew", "renew-user", "Renew User")
                .await;
        let credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&issued.token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let first_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"before renewal","client_message_key":"renew-first"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        let renew_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/session/renew")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(renew_response.status(), StatusCode::OK);
        let renewed: WidgetTokenResponse = serde_json::from_slice(
            &axum::body::to_bytes(renew_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(renewed.token.starts_with("ahw_"));
        assert_ne!(renewed.token, issued.token);
        assert!(renewed.expires_at > issued.expires_at);
        let renewed_credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&renewed.token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(renewed_credential_id, credential_id);

        let expired_token_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/session")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(expired_token_response.status(), StatusCode::UNAUTHORIZED);
        let session_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/session")
                    .header("x-agent-hub-embed-token", &renewed.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(session_response.status(), StatusCode::OK);
        let session: WidgetSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(session_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(session.agent.id, fixture.agent_id);
        assert!(session.history_enabled);

        let continue_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &renewed.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "message": "after renewal",
                            "integration_session_id": first_run.integration_session_id,
                            "hub_session_id": first_run.hub_session_id,
                            "client_message_key": "renew-second"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(continue_response.status(), StatusCode::OK);
        let continued: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(continue_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(continued.hub_session_id, first_run.hub_session_id);
        assert_eq!(
            continued.integration_session_id,
            first_run.integration_session_id
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_profile_refresh_requires_app_credentials_and_updates_future_runs(pool: PgPool) {
        let fixture = widget_external_test_fixture(pool, false).await;
        let issued = issue_widget_external_access(
            &fixture,
            "tenant-profile",
            "profile-user",
            "Profile Before",
        )
        .await;
        let profile_body = serde_json::to_vec(&json!({
            "profile": {
                "username": "profile-user",
                "display_name": "Profile After",
                "email": "profile-user@example.com",
                "attributes": { "revision": 2 }
            }
        }))
        .unwrap();
        let untrusted_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/session/renew")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(profile_body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(untrusted_response.status(), StatusCode::UNAUTHORIZED);

        let basic = STANDARD.encode(format!("{}:{}", fixture.client_id, fixture.client_secret));
        let trusted_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/session/renew")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::AUTHORIZATION, format!("Basic {basic}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(profile_body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(trusted_response.status(), StatusCode::OK);
        let renewed: WidgetTokenResponse = serde_json::from_slice(
            &axum::body::to_bytes(trusted_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let run_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &renewed.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"message":"use refreshed profile"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(run_response.status(), StatusCode::OK);
        let run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(run_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let context: Value =
            sqlx::query_scalar("SELECT external_user_context FROM runs WHERE id = $1")
                .bind(run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(context["display_name"], "Profile After");
        assert_eq!(context["attributes"]["revision"], 2);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_history_is_configurable_and_isolated_by_full_external_scope(pool: PgPool) {
        let fixture = widget_external_test_fixture(pool, true).await;
        let primary = issue_widget_external_access(
            &fixture,
            "tenant-history",
            "history-user",
            "History User",
        )
        .await;
        let primary_run =
            create_widget_external_run(&fixture, &primary.token, "primary history message").await;
        let primary_session_id = primary_run.integration_session_id.unwrap();

        let other_user = issue_widget_external_access(
            &fixture,
            "tenant-history",
            "other-history-user",
            "Other History User",
        )
        .await;
        create_widget_external_run(&fixture, &other_user.token, "other user message").await;
        let other_tenant = issue_widget_external_access(
            &fixture,
            "other-history-tenant",
            "history-user",
            "History User",
        )
        .await;
        create_widget_external_run(&fixture, &other_tenant.token, "other tenant message").await;

        let other_app_id = Uuid::new_v4();
        let other_client_id = format!("widget-history-app-{}", Uuid::new_v4().simple());
        let other_client_secret = "widget-history-other-app-secret";
        sqlx::query(
            "INSERT INTO oauth_apps
                 (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
                  external_platform_id, authentication_channel_id,
                  widget_history_enabled)
             SELECT $1, owner_id, 'Other History App', $2, $3, redirect_uris,
                    external_platform_id, authentication_channel_id, true
             FROM oauth_apps WHERE id = $4",
        )
        .bind(other_app_id)
        .bind(&other_client_id)
        .bind(sha256_hex(other_client_secret))
        .bind(fixture.app_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO integration_app_agents (app_id, agent_id) VALUES ($1, $2)")
            .bind(other_app_id)
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let other_app = issue_widget_external_access_for(
            &fixture,
            &other_client_id,
            other_client_secret,
            fixture.agent_id,
            "tenant-history",
            "history-user",
            "History User",
        )
        .await;
        create_widget_external_run(&fixture, &other_app.token, "other app message").await;

        let other_agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, model_policy,
                  runtime_id, mcp_allowlist, sandbox_policy, public_to,
                  model_connection_id, model_id, model_settings)
             SELECT $1, owner_id, 'Other History Agent', instructions, visibility,
                    model_policy, runtime_id, mcp_allowlist, sandbox_policy, public_to,
                    model_connection_id, model_id, model_settings
             FROM agents WHERE id = $2",
        )
        .bind(other_agent_id)
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO integration_app_agents (app_id, agent_id) VALUES ($1, $2)")
            .bind(fixture.app_id)
            .bind(other_agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let other_agent = issue_widget_external_access_for(
            &fixture,
            &fixture.client_id,
            &fixture.client_secret,
            other_agent_id,
            "tenant-history",
            "history-user",
            "History User",
        )
        .await;
        create_widget_external_run(&fixture, &other_agent.token, "other agent message").await;

        let list_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/sessions")
                    .header("x-agent-hub-embed-token", &primary.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let history: Vec<WidgetHistorySessionDto> = serde_json::from_slice(
            &axum::body::to_bytes(list_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].id, primary_session_id);
        assert_eq!(
            history[0].hub_session_id,
            primary_run.hub_session_id.unwrap()
        );
        assert_eq!(
            history[0].preview.as_deref(),
            Some("primary history message")
        );

        let messages_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/widget/sessions/{primary_session_id}/messages?limit=1"
                    ))
                    .header("x-agent-hub-embed-token", &primary.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(messages_response.status(), StatusCode::OK);
        let messages: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(messages_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].content.as_deref(),
            Some("primary history message")
        );

        let events_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/widget/sessions/{primary_session_id}/events"))
                    .header("x-agent-hub-embed-token", &primary.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events_response.status(), StatusCode::OK);
        let events: Vec<RunEventDto> = serde_json::from_slice(
            &axum::body::to_bytes(events_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| event.run_id == primary_run.id));

        for foreign_token in [
            &other_user.token,
            &other_tenant.token,
            &other_app.token,
            &other_agent.token,
        ] {
            let response = fixture
                .router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(format!(
                            "/api/widget/sessions/{primary_session_id}/messages"
                        ))
                        .header("x-agent-hub-embed-token", foreign_token)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }

        sqlx::query("UPDATE oauth_apps SET widget_history_enabled = false WHERE id = $1")
            .bind(fixture.app_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let disabled_list = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/sessions")
                    .header("x-agent-hub-embed-token", &primary.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disabled_list.status(), StatusCode::FORBIDDEN);
        let exact_session_after_disable = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/widget/sessions/{primary_session_id}/messages"
                    ))
                    .header("x-agent-hub-embed-token", &primary.token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(exact_session_after_disable.status(), StatusCode::OK);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn oauth_authorize_only_reads_the_existing_identity_profile(pool: PgPool) {
        let user = create_hub_user(
            &pool,
            Some("oauth-authorize-profile@example.com"),
            Some("authorize-profile"),
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let session_token = "oauth-authorize-profile-session";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(session_token))
        .bind(user.id)
        .execute(&pool)
        .await
        .unwrap();
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let identity_id = Uuid::new_v4();
        let app_id = Uuid::new_v4();
        let client_id = format!("authorize-profile-{}", Uuid::new_v4().simple());
        let client_secret = "authorize-profile-secret";
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, $2, 'Authorize Profile Platform')",
        )
        .bind(platform_id)
        .bind(format!("authorize-profile-{}", Uuid::new_v4().simple()))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email, created_by)
             VALUES ($1, $2, 'authorize', 'Authorize', true, true, $3)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .bind(user.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO oauth_apps
                 (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
                  external_platform_id, authentication_channel_id)
             VALUES ($1, $2, 'Authorize App', $3, $4, $5, $6, $7)",
        )
        .bind(app_id)
        .bind(user.id)
        .bind(&client_id)
        .bind(sha256_hex(client_secret))
        .bind(json!(["https://client.example.com/callback"]))
        .bind(platform_id)
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_identities
                 (id, platform_id, tenant_id, external_user_id, user_id,
                  authentication_channel_id, last_email, last_username)
             VALUES ($1, $2, 'tenant-profile', 'external-profile-user', $3, $4,
                     'external@example.com', 'external-name')",
        )
        .bind(identity_id)
        .bind(platform_id)
        .bind(user.id)
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let authorize = oauth_authorize(
            State(state.clone()),
            session_headers(session_token),
            Query(OAuthAuthorizeQuery {
                client_id: client_id.clone(),
                redirect_uri: "https://client.example.com/callback".into(),
                state: Some("state-value".into()),
                scope: Some("profile external_profile".into()),
                external_user_id: "external-profile-user".into(),
                tenant_id: "tenant-profile".into(),
            }),
        )
        .await
        .unwrap();
        let response = authorize.into_response();
        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap();
        let code = Url::parse(location)
            .unwrap()
            .query_pairs()
            .find_map(|(key, value)| (key == "code").then(|| value.into_owned()))
            .unwrap();
        let token = oauth_token(
            State(state.clone()),
            Form(OAuthTokenForm {
                grant_type: "authorization_code".into(),
                client_id,
                client_secret: client_secret.into(),
                code: Some(code),
                redirect_uri: Some("https://client.example.com/callback".into()),
                scope: Some("profile external_profile".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        let principal = require_application_token(&state, &bearer_headers(&token.access_token))
            .await
            .unwrap();
        assert_eq!(principal.grant_type, "authorization_code");
        assert_eq!(principal.subject_user_id, Some(user.id));
        assert_eq!(
            principal.origin_tenant_id.as_deref(),
            Some("tenant-profile")
        );
        assert_eq!(principal.origin_external_identity_id, Some(identity_id));
        let userinfo = oauth_userinfo(State(state), bearer_headers(&token.access_token))
            .await
            .unwrap()
            .0;
        assert_eq!(userinfo.sub, user.id);
        assert_eq!(userinfo.name.as_deref(), Some("authorize-profile"));
        assert!(userinfo.email.is_none());
        assert!(userinfo.external_profile.unwrap().email.is_none());

        let profile: (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT last_email, last_username FROM external_identities WHERE id = $1",
        )
        .bind(identity_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            profile,
            (
                Some("external@example.com".into()),
                Some("external-name".into())
            )
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM external_identities
                 WHERE platform_id = $1 AND tenant_id = 'tenant-profile'
                   AND external_user_id = 'external-profile-user'",
            )
            .bind(platform_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_credentials_app_reads_multiple_identity_sessions_without_mixing_origins(
        pool: PgPool,
    ) {
        let owner = create_hub_user(
            &pool,
            Some("client-credentials-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let model_connection_id = Uuid::new_v4();
        let model_id = format!("client-credentials-model-{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO model_connections
                 (id, scope, name, base_url, api_type, allowed_model_ids,
                  api_key_ciphertext, api_key_nonce, created_by)
             VALUES ($1, 'global', 'Client Credentials Model',
                     'https://models.example.test', 'openai_responses',
                     $2, $3, $4, $5)",
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
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let app_id = Uuid::new_v4();
        let foreign_app_id = Uuid::new_v4();
        let token = "aho_client_credentials_multi_identity";
        let foreign_token = "aho_client_credentials_foreign_app";
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility,
                  model_connection_id, model_id)
             VALUES ($1, $2, 'Client Credentials Agent', 'test', 'private', $3, $4)",
        )
        .bind(agent_id)
        .bind(owner.id)
        .bind(model_connection_id)
        .bind(&model_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, $2, 'Client Credentials Platform')",
        )
        .bind(platform_id)
        .bind(format!("client-credentials-{}", Uuid::new_v4().simple()))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email, created_by)
             VALUES ($1, $2, 'integration', 'Integration', true, true, $3)",
        )
        .bind(channel_id)
        .bind(platform_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        for (id, client_id) in [(app_id, "multi-app"), (foreign_app_id, "foreign-app")] {
            sqlx::query(
                "INSERT INTO oauth_apps
                     (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
                      external_platform_id, authentication_channel_id)
                 VALUES ($1, $2, $3, $4, 'unused', '[]'::jsonb, $5, $6)",
            )
            .bind(id)
            .bind(owner.id)
            .bind(client_id)
            .bind(format!("{client_id}-{}", Uuid::new_v4().simple()))
            .bind(platform_id)
            .bind(channel_id)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO integration_app_agents (app_id, agent_id) VALUES ($1, $2)")
                .bind(id)
                .bind(agent_id)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (id, app, value) in [
            (Uuid::new_v4(), app_id, token),
            (Uuid::new_v4(), foreign_app_id, foreign_token),
        ] {
            sqlx::query(
                "INSERT INTO oauth_access_tokens
                     (id, oauth_app_id, token_hash, expires_at, grant_type, scopes)
                 VALUES ($1, $2, $3, now() + interval '1 hour',
                         'client_credentials', $4)",
            )
            .bind(id)
            .bind(app)
            .bind(sha256_hex(value))
            .bind(vec![format!("agent:{agent_id}")])
            .execute(&pool)
            .await
            .unwrap();
        }
        let client_id: String =
            sqlx::query_scalar("SELECT client_id FROM oauth_apps WHERE id = $1")
                .bind(app_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let client_secret = "multi-identity-client-secret";
        sqlx::query("UPDATE oauth_apps SET client_secret_hash = $1 WHERE id = $2")
            .bind(sha256_hex(client_secret))
            .bind(app_id)
            .execute(&pool)
            .await
            .unwrap();
        let state = Arc::new(test_state_with_pool(pool));
        let invalid_scope = oauth_token(
            State(state.clone()),
            Form(OAuthTokenForm {
                grant_type: "client_credentials".into(),
                client_id: client_id.clone(),
                client_secret: client_secret.into(),
                code: None,
                redirect_uri: None,
                scope: Some("profile".into()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid_scope.status, StatusCode::BAD_REQUEST);
        let app_token = oauth_token(
            State(state.clone()),
            Form(OAuthTokenForm {
                grant_type: "client_credentials".into(),
                client_id,
                client_secret: client_secret.into(),
                code: None,
                redirect_uri: None,
                scope: Some(format!("agent:{agent_id}")),
            }),
        )
        .await
        .unwrap()
        .0
        .access_token;
        let app_principal = require_application_token(&state, &bearer_headers(&app_token))
            .await
            .unwrap();
        assert_eq!(app_principal.grant_type, "client_credentials");
        assert_eq!(app_principal.subject_user_id, None);
        assert_eq!(app_principal.origin_tenant_id, None);
        assert_eq!(app_principal.origin_external_identity_id, None);
        for email in [None, Some("not-an-email")] {
            let error = create_integration_session(
                State(state.clone()),
                bearer_headers(&app_token),
                Json(CreateIntegrationSessionRequest {
                    agent_id,
                    external_user_id: format!("invalid-email-{}", Uuid::new_v4().simple()),
                    tenant_id: Some("tenant-invalid".into()),
                    username: None,
                    display_name: None,
                    email: email.map(str::to_owned),
                    tools: json!([]),
                    metadata: json!({}),
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }
        let mut created = Vec::new();
        for (tenant_id, external_user_id, email) in [
            ("tenant-one", "external-one", "shared@example.com"),
            ("tenant-two", "external-two", "external-two@example.com"),
            ("tenant-three", "external-three", "shared@example.com"),
        ] {
            created.push(
                create_integration_session(
                    State(state.clone()),
                    bearer_headers(&app_token),
                    Json(CreateIntegrationSessionRequest {
                        agent_id,
                        external_user_id: external_user_id.into(),
                        tenant_id: Some(tenant_id.into()),
                        username: Some(format!("{external_user_id}-name")),
                        display_name: Some(format!("{external_user_id} Display")),
                        email: Some(email.into()),
                        tools: json!([]),
                        metadata: json!({}),
                    }),
                )
                .await
                .unwrap()
                .0,
            );
        }
        assert_ne!(
            created[0].external_identity_id,
            created[1].external_identity_id
        );
        assert_ne!(created[0].owner_id, created[1].owner_id);
        assert_ne!(
            created[0].external_identity_id,
            created[2].external_identity_id
        );
        assert_eq!(created[0].owner_id, created[2].owner_id);
        assert_eq!(created[0].tenant_id, "tenant-one");
        assert_eq!(created[1].tenant_id, "tenant-two");
        assert_eq!(created[2].tenant_id, "tenant-three");
        for session in &created {
            let loaded = get_integration_session(
                State(state.clone()),
                bearer_headers(&app_token),
                Path(session.id),
            )
            .await
            .unwrap()
            .0;
            assert_eq!(loaded.external_identity_id, session.external_identity_id);
            assert_eq!(loaded.owner_id, session.owner_id);
        }
        for (index, session) in created.iter().enumerate() {
            let continued = create_integration_message(
                State(state.clone()),
                bearer_headers(&app_token),
                Path(session.id),
                Json(CreateIntegrationMessageRequest {
                    content: format!("continue identity {index}"),
                    attachments: json!([]),
                    client_message_key: Some(format!("continue-{index}")),
                }),
            )
            .await
            .unwrap()
            .0;
            assert_eq!(continued.run.integration_session_id, Some(session.id));
            assert_eq!(continued.message.session_id, session.hub_session_id);
            assert_eq!(
                sqlx::query_as::<_, (Option<Uuid>, Option<String>, Option<Uuid>)>(
                    "SELECT origin_platform_id, origin_tenant_id, origin_external_identity_id
                     FROM hub_sessions WHERE id = $1",
                )
                .bind(session.hub_session_id)
                .fetch_one(&state.pool)
                .await
                .unwrap(),
                (
                    Some(platform_id),
                    Some(session.tenant_id.clone()),
                    Some(session.external_identity_id),
                )
            );
            assert_eq!(
                sqlx::query_as::<_, (String, String)>(
                    "SELECT tenant_id, external_user_id
                     FROM external_identities WHERE id = $1",
                )
                .bind(session.external_identity_id)
                .fetch_one(&state.pool)
                .await
                .unwrap(),
                (session.tenant_id.clone(), session.external_user_id.clone())
            );
        }
        let widget_token = create_integration_embed_session(
            State(state.clone()),
            bearer_headers(&app_token),
            Json(CreateEmbedSessionRequest { agent_id }),
        )
        .await
        .unwrap()
        .0
        .token;
        let mut widget_headers = HeaderMap::new();
        widget_headers.insert(
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderValue::from_str(&widget_token).unwrap(),
        );
        let _ = get_widget_session(State(state.clone()), widget_headers.clone())
            .await
            .unwrap();
        for session in &created {
            let foreign = get_integration_session(
                State(state.clone()),
                bearer_headers(foreign_token),
                Path(session.id),
            )
            .await
            .unwrap_err();
            assert_eq!(foreign.status, StatusCode::NOT_FOUND);
        }
        sqlx::query("DELETE FROM integration_app_agents WHERE app_id = $1 AND agent_id = $2")
            .bind(app_id)
            .bind(agent_id)
            .execute(&state.pool)
            .await
            .unwrap();
        let revoked = get_integration_session(
            State(state.clone()),
            bearer_headers(&app_token),
            Path(created[0].id),
        )
        .await
        .unwrap_err();
        assert_eq!(revoked.status, StatusCode::FORBIDDEN);
        let widget_revoked = get_widget_session(State(state), widget_headers)
            .await
            .unwrap_err();
        assert_eq!(widget_revoked.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn integration_stop_is_idempotent_origin_scoped_and_routes_late_message_to_next_turn(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'fixture-native-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let app = build_router(test_state_with_pool(fixture.state.pool.clone()));
        let stop_request = |token: &str, session_id: Uuid| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!(
                    "/api/integrations/sessions/{session_id}/runs/{}/stop",
                    fixture.run_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };

        let foreign = app
            .clone()
            .oneshot(stop_request(
                &fixture.foreign_integration_token,
                fixture.session_id,
            ))
            .await
            .unwrap();
        assert_eq!(foreign.status(), StatusCode::FORBIDDEN);

        let first = app
            .clone()
            .oneshot(stop_request(&fixture.integration_token, fixture.session_id))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first_requested_at: DateTime<Utc> = sqlx::query_scalar(
            "SELECT interrupt_requested_at FROM hub_session_turns WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();

        let repeated = app
            .oneshot(stop_request(&fixture.integration_token, fixture.session_id))
            .await
            .unwrap();
        assert_eq!(repeated.status(), StatusCode::OK);
        assert_eq!(
            sqlx::query_scalar::<_, DateTime<Utc>>(
                "SELECT interrupt_requested_at FROM hub_session_turns WHERE id = $1"
            )
            .bind(fixture.turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            first_requested_at
        );

        let accepted = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "continue after stopping".into(),
                attachments: json!([]),
                client_message_key: Some("after-stop".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_ne!(accepted.run.id, fixture.run_id);
        assert_ne!(accepted.message.turn_id, Some(fixture.turn_id));
        assert_eq!(accepted.run.status, "pending");
        assert_eq!(accepted.message.delivery_mode, "next_turn");
        assert_eq!(accepted.message.delivery_state, "queued");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_run_prepend_instructions_written_once_and_immutable(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = widget_external_test_fixture(pool, true).await;
        let tenant_id = format!("tenant-{}", Uuid::new_v4().simple());
        let external_user_id = format!("ext-{}", Uuid::new_v4().simple());
        let access = issue_client_access_for_instance(
            &fixture,
            fixture.client_instance_id,
            &tenant_id,
            &external_user_id,
            test_client_tool_definitions(&["open_panel"]),
        )
        .await;
        let token = access.access_token;

        // 1. 创建会话并带预指令 → 写入 integration_sessions。
        let mut create_headers = HeaderMap::new();
        create_headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let run = create_widget_run(
            State(fixture.state.clone()),
            create_headers,
            Json(CreateWidgetRunRequest {
                message: "create with prepend".into(),
                session_id: None,
                integration_session_id: None,
                hub_session_id: None,
                parent_run_id: None,
                client_message_key: Some("create-with-prepend-1".into()),
                prepend_instructions: Some("业务术语：工程解决方案=应用系统。".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        let integration_session_id = run.integration_session_id.expect("external session id");
        let stored: Option<String> = sqlx::query_scalar(
            "SELECT prepend_instructions FROM integration_sessions WHERE id = $1",
        )
        .bind(integration_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            stored.as_deref(),
            Some("业务术语：工程解决方案=应用系统。"),
            "prepend must be written at session creation"
        );

        // 2. 既有会话再带预指令 → 400（不可变）。
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let err = create_widget_run(
            State(fixture.state.clone()),
            headers,
            Json(CreateWidgetRunRequest {
                message: "second message".into(),
                session_id: Some(integration_session_id),
                integration_session_id: None,
                hub_session_id: None,
                parent_run_id: None,
                client_message_key: Some("immutable-check-1".into()),
                prepend_instructions: Some("改变预指令".into()),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);

        // 2b. 同值重放（幂等）→ 通过，值不变。
        let mut replay_headers = HeaderMap::new();
        replay_headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let _ = create_widget_run(
            State(fixture.state.clone()),
            replay_headers,
            Json(CreateWidgetRunRequest {
                message: "replay message".into(),
                session_id: Some(integration_session_id),
                integration_session_id: None,
                hub_session_id: None,
                parent_run_id: None,
                client_message_key: Some("replay-1".into()),
                prepend_instructions: Some("业务术语：工程解决方案=应用系统。".into()),
            }),
        )
        .await
        .unwrap();
        let stored_after: Option<String> = sqlx::query_scalar(
            "SELECT prepend_instructions FROM integration_sessions WHERE id = $1",
        )
        .bind(integration_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            stored_after.as_deref(),
            Some("业务术语：工程解决方案=应用系统。")
        );

        // 3. 超长 → 400。
        let mut headers2 = HeaderMap::new();
        headers2.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let err = create_widget_run(
            State(fixture.state.clone()),
            headers2,
            Json(CreateWidgetRunRequest {
                message: "third message".into(),
                session_id: None,
                integration_session_id: None,
                hub_session_id: None,
                parent_run_id: None,
                client_message_key: Some("too-long-1".into()),
                prepend_instructions: Some("x".repeat(65537)),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn submit_string_output_client_tool_result_like_run_shelves_operation(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action"]).await;
        finalize_test_client_tool_batch(&fixture, &["first_action"])
            .await
            .unwrap();
        let claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");
        // run_shelves_operation 前端返回 toToolResult(JSON.stringify(result))：
        // output 是 JSON 字符串（可能较大）。
        let output = json!({
            "ok": true,
            "message": "",
            "data": { "code": 200, "msg": "", "total": 5, "data": [ { "id": "abc", "cluster_name": "工程1" } ] }
        });
        let submitted = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!(output.to_string()),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(submitted.run.is_some(), "continuation run must be created");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn plural_tool_results_respect_total_budget(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action", "second_action"]).await;
        // 带 S3 store：截断结果归档，summarize 给出 read 指引。
        let (_, store, server) = attachment_object_store().await;
        let mut app_state = (*fixture.app.state).clone();
        app_state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(app_state);
        let fixture_state = state.clone();
        finalize_test_client_tool_batch(&fixture, &["first_action", "second_action"])
            .await
            .unwrap();
        // 两个工具都提交超过截断阈值的大结果：summarize 后各约 32KB+，
        // 合计超 64KB 总量预算——最新（second_action）展开、旧（first_action）
        // 占位（保留身份与读取指引）。
        let mut continuation_run = None;
        for (idx, tool_name) in ["first_action", "second_action"].iter().enumerate() {
            let claim = claim_client_tool_call(
                State(fixture_state.clone()),
                bearer_headers(&fixture.executor.access_token),
                Path(fixture.tool_call_ids[idx]),
            )
            .await
            .unwrap()
            .0;
            assert_eq!(claim.status, "claimed");
            let big = "x".repeat(TOOL_RESULT_TRUNCATE_BYTES + 1024);
            let submitted = submit_client_tool_result(
                State(fixture_state.clone()),
                bearer_headers(&fixture.executor.access_token),
                Path(fixture.tool_call_ids[idx]),
                Json(SubmitClientToolResultRequest {
                    result: ClientToolResultDto::Success {
                        output: json!(format!("{tool_name}:{big}")),
                        truncated: None,
                    },
                }),
            )
            .await
            .unwrap()
            .0;
            if idx == 1 {
                assert!(
                    submitted.run.is_some(),
                    "continuation run must be created after last tool result"
                );
                continuation_run = submitted.run;
            }
        }
        let continuation_run = continuation_run.expect("continuation run must exist");
        let mut tx = fixture_state.pool.begin().await.unwrap();
        let context = load_integration_context_for_run(&mut tx, &continuation_run)
            .await
            .unwrap()
            .expect("tool-result run must expose integration context");
        tx.commit().await.unwrap();

        assert_eq!(context.tool_results.len(), 2);
        // 旧结果（first_action，position 0）占位：保留身份与 truncated 标记。
        let first = serde_json::to_value(&context.tool_results[0]).unwrap();
        assert_eq!(
            first.get("tool_name").and_then(Value::as_str),
            Some("first_action")
        );
        let first_output = first
            .pointer("/result/output")
            .and_then(Value::as_str)
            .unwrap();
        assert!(
            first_output.contains("工具结果未展开"),
            "older result beyond budget must be a readable placeholder"
        );
        assert!(first_output.contains("agent_hub_integration_tool_result_read(tool_call_id="));
        assert_eq!(
            first.pointer("/result/truncated").and_then(Value::as_bool),
            Some(true)
        );
        // 最新结果（second_action，position 1）完整展开。
        let second = serde_json::to_value(&context.tool_results[1]).unwrap();
        assert_eq!(
            second.get("tool_name").and_then(Value::as_str),
            Some("second_action")
        );
        let second_output = second.pointer("/result/output").unwrap();
        assert_eq!(second_output.get("truncated"), Some(&json!(true)));
        assert!(second_output
            .get("content")
            .and_then(Value::as_str)
            .unwrap()
            .contains("agent_hub_integration_tool_result_read"));

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn re_claim_after_submit_returns_terminal_result(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action"]).await;
        finalize_test_client_tool_batch(&fixture, &["first_action"])
            .await
            .unwrap();
        let claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");
        let output = json!({ "ok": true, "total": 5 });
        let submitted = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!(output.to_string()),
                    truncated: None,
                },
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(submitted.run.is_some());
        // 已完成工具再次 claim：terminal=true 且 result 从包装层内层正确解析。
        let re_claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert!(re_claim.terminal);
        assert_eq!(re_claim.status, "completed");
        let result =
            serde_json::to_value(re_claim.result.expect("terminal claim must return result"))
                .unwrap();
        assert_eq!(
            result.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(
            result.get("output").and_then(Value::as_str),
            Some(output.to_string().as_str())
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn truncated_client_tool_result_keeps_dto_shell_for_continuation(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action"]).await;
        // 带 S3 store：截断全文归档，模型输入应带 artifact 读取指引。
        let (objects, store, server) = attachment_object_store().await;
        let mut app_state = (*fixture.app.state).clone();
        app_state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(app_state);
        finalize_test_client_tool_batch(&fixture, &["first_action"])
            .await
            .unwrap();
        let claim = claim_client_tool_call(
            State(state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");
        // 大结果（超过截断阈值）触发 truncated 路径：续接 run 必须仍能按
        // ClientToolResultDto 解析 result_payload，否则 submit 返回 500。
        let big_content = "x".repeat(TOOL_RESULT_TRUNCATE_BYTES + 1024);
        let submitted = submit_client_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!(big_content),
                    truncated: None,
                },
            }),
        )
        .await
        .expect("truncated Client Tool result must be accepted")
        .0;
        assert!(submitted.run.is_some(), "continuation run must be created");

        // DB 形状：自包含包装层 {tool_call_id, tool_name, result}；result 是
        // 合法 DTO，output 直接是前 32KB 截断文本，result 内 truncated 与
        // artifact_ref 标识截断与归档位置。
        let (tool_call_id, tool_name, status, output, truncated_flag, artifact_ref): (
            String,
            String,
            String,
            Option<String>,
            Option<bool>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT result_payload->>'tool_call_id', result_payload->>'tool_name',
                    result_payload->'result'->>'status', result_payload->'result'->>'output',
                    (result_payload->'result'->>'truncated')::bool,
                    result_payload->'result'->>'artifact_ref'
             FROM integration_tool_requests WHERE id = $1",
        )
        .bind(fixture.tool_call_ids[0])
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(tool_call_id, fixture.tool_call_ids[0].to_string());
        assert_eq!(tool_name, "first_action");
        assert_eq!(status, "success");
        let content = output.expect("output must be the truncated text");
        assert!(content.len() <= TOOL_RESULT_TRUNCATE_BYTES);
        assert_eq!(truncated_flag, Some(true));
        let artifact_ref = artifact_ref.expect("archived result must carry artifact_ref");
        assert!(artifact_ref.starts_with("tool-results/"));

        // 事件协议不变：client_tool_result 事件的 result 是内层 DTO
        // （event.result.status 直接可读），不是包装层。
        let (event_result, event_tool_name): (serde_json::Value, String) = sqlx::query_as(
            "SELECT payload->'result', payload->>'tool_name'
             FROM run_events
             WHERE run_id = $1 AND event_type = 'client_tool_result'
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(fixture.run.id)
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(
            event_result.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(event_tool_name, "first_action");

        // 模型输入（singular）：context.tool_result 感知截断并给出读取指引。
        let mut tx = state.pool.begin().await.unwrap();
        let context = load_integration_context_for_run(&mut tx, submitted.run.as_ref().unwrap())
            .await
            .unwrap()
            .expect("tool-result run must expose integration context");
        tx.commit().await.unwrap();
        let tool_result = context.tool_result.expect("single tool result is emitted");
        assert_eq!(tool_result.get("truncated"), Some(&json!(true)));
        let summarized = tool_result.get("content").and_then(Value::as_str).unwrap();
        assert!(
            summarized.contains("agent_hub_integration_tool_result_read(tool_call_id="),
            "model input must carry full-result read instructions"
        );
        assert!(summarized.contains("artifact://"));

        // 模型输入（plural）：tool_results 的 result 同样保留截断标记与指引。
        assert_eq!(context.tool_results.len(), 1);
        let plural_result = serde_json::to_value(&context.tool_results[0].result).unwrap();
        assert_eq!(
            plural_result.get("status").and_then(Value::as_str),
            Some("success")
        );
        let plural_output = plural_result.get("output").unwrap();
        assert_eq!(plural_output.get("truncated"), Some(&json!(true)));
        let plural_summary = plural_output
            .get("content")
            .and_then(Value::as_str)
            .unwrap();
        assert!(
            plural_summary.contains("agent_hub_integration_tool_result_read(tool_call_id="),
            "plural path must carry full-result read instructions"
        );

        // 完整内容已归档到 S3。
        let stored_key = artifact_ref;
        let stored = objects.lock().unwrap().get(&stored_key).cloned();
        assert!(
            stored.is_some(),
            "archived result must exist in object storage"
        );
        assert!(stored.unwrap().len() > TOOL_RESULT_TRUNCATE_BYTES);

        // 释放测试 S3 server。
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn truncated_client_tool_result_utf8_boundary_does_not_panic(pool: PgPool) {
        let fixture = prepare_client_tool_run(pool, &["first_action"]).await;
        finalize_test_client_tool_batch(&fixture, &["first_action"])
            .await
            .unwrap();
        let claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");
        // "中é" 是 5 字节周期（3+2）：32768 与 5 互质，32768-2 不整除 5，
        // 截断阈值必然落在多字节字符中间；旧的 String::truncate 直接
        // panic（线上表现为无日志 500）。
        let chinese = "中\u{e9}".repeat(16_385); // 2 + 81_925 = 81_927 字节
        let submitted = submit_client_tool_result(
            State(fixture.app.state.clone()),
            bearer_headers(&fixture.executor.access_token),
            Path(fixture.tool_call_ids[0]),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!(chinese),
                    truncated: None,
                },
            }),
        )
        .await
        .expect("UTF-8 boundary truncation must not panic")
        .0;
        assert!(submitted.run.is_some(), "continuation run must be created");
        let (status, content): (String, Option<String>) = sqlx::query_as(
            "SELECT result_payload->'result'->>'status', result_payload->'result'->>'output'
             FROM integration_tool_requests WHERE id = $1",
        )
        .bind(fixture.tool_call_ids[0])
        .fetch_one(&fixture.app.state.pool)
        .await
        .unwrap();
        assert_eq!(status, "success");
        let content = content.unwrap();
        assert!(content.len() <= TOOL_RESULT_TRUNCATE_BYTES);
        assert!(content.is_char_boundary(content.len()));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_force_stop_works_for_external_session_and_is_scoped(pool: PgPool) {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let fixture = widget_external_test_fixture(pool, true).await;
        // 完整 external 身份：OAuth app + 平台/租户/外部用户 + client_instance。
        let tenant_id = format!("tenant-{}", Uuid::new_v4().simple());
        let external_user_id = format!("ext-{}", Uuid::new_v4().simple());
        let instance_id = fixture.client_instance_id;
        let access = issue_client_access_for_instance(
            &fixture,
            instance_id,
            &tenant_id,
            &external_user_id,
            test_client_tool_definitions(&["open_panel"]),
        )
        .await;
        let token = access.access_token;
        let other_access = issue_client_access_for_instance(
            &fixture,
            Uuid::new_v4(),
            &tenant_id,
            &external_user_id,
            test_client_tool_definitions(&["open_panel"]),
        )
        .await;
        let other_token = other_access.access_token;

        // 创建 external 会话 + run（/api/client/runs，canonical helper）。
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let run =
            create_canonical_client_run(&fixture, &token, None, "external force stop test").await;
        let hub_session_id =
            sqlx::query_scalar::<_, Uuid>("SELECT hub_session_id FROM runs WHERE id = $1")
                .bind(run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        // 确认是 external 会话（用户要求的第三方会话场景）。
        let (origin_kind, platform_id, identity_id): (String, Option<Uuid>, Option<Uuid>) =
            sqlx::query_as(
                "SELECT origin_kind, origin_platform_id, origin_external_identity_id
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(origin_kind, "external");
        assert_eq!(platform_id, Some(fixture.platform_id));
        assert!(identity_id.is_some(), "external identity must be recorded");

        // 置为运行中（模拟已被 runtime 领取执行）。
        let runtime_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, 'force-stop-runtime', '{}', 'test',
                     '{"model_proxy":true}'::jsonb, 'workspace-write', 'online')"#,
        )
        .bind(runtime_id)
        .bind(sha256_hex(&format!("rt-{runtime_id}")))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = $2
             WHERE id = $3",
        )
        .bind(runtime_id)
        .bind(run.hub_turn_id)
        .bind(hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs SET runtime_id = $1, status = 'running',
                 session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(runtime_id)
        .bind(run.id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        // 用户强制停止（第三方会话也允许）。
        let (status, dto) = force_stop_widget_run(
            State(fixture.state.clone()),
            headers.clone(),
            Path(run.id),
            Json(ForceStopRequest {
                request_id: "external-force-stop-1".into(),
                expected_generation: Some(1),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(dto.state, "pending");
        assert_eq!(dto.session_id, hub_session_id);
        assert_eq!(dto.target_runtime_id, Some(runtime_id));

        let (lifecycle, gen, owner): (String, i64, Option<Uuid>) = sqlx::query_as(
            "SELECT lifecycle_status, ownership_generation, runtime_owner_id
             FROM hub_sessions WHERE id = $1",
        )
        .bind(hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(lifecycle, "force_stopping");
        assert_eq!(gen, 1);
        assert_eq!(owner, Some(runtime_id));
        let (run_status,): (String,) = sqlx::query_as("SELECT status FROM runs WHERE id = $1")
            .bind(run.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(run_status, "interrupted");

        // 同一 app 但不同 client_instance 的凭证 → 404（作用域隔离）。
        let mut other_headers = HeaderMap::new();
        other_headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {other_token}")).unwrap(),
        );
        let err = force_stop_widget_run(
            State(fixture.state.clone()),
            other_headers,
            Path(run.id),
            Json(ForceStopRequest {
                request_id: "external-force-stop-other".into(),
                expected_generation: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.status,
            StatusCode::NOT_FOUND,
            "foreign instance must not force stop: {:?}",
            err.message
        );

        // 无凭证 → 401。
        let err = force_stop_widget_run(
            State(fixture.state.clone()),
            HeaderMap::new(),
            Path(run.id),
            Json(ForceStopRequest {
                request_id: "external-force-stop-anon".into(),
                expected_generation: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_stop_is_scoped_to_the_embed_session_token(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let widget_session_id = Uuid::new_v4();
        let other_widget_session_id = Uuid::new_v4();
        let other_hub_session_id = Uuid::new_v4();
        let widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        let other_widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
        )
        .bind(other_hub_session_id)
        .bind(owner_id)
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        for (embed_id, token, hub_session_id) in [
            (widget_session_id, &widget_token, fixture.hub_session_id),
            (
                other_widget_session_id,
                &other_widget_token,
                other_hub_session_id,
            ),
        ] {
            sqlx::query(
                "INSERT INTO embed_sessions
                     (id, token_hash, agent_id, owner_id, expires_at, hub_session_id)
                 VALUES ($1, $2, $3, $4, now() + interval '1 hour', $5)",
            )
            .bind(embed_id)
            .bind(sha256_hex(token))
            .bind(fixture.agent_id)
            .bind(owner_id)
            .bind(hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = $2,
                 recovery_source = NULL,
                 native_session_id = 'widget-stop-thread'
             WHERE id = $3",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET native_turn_id = 'widget-stop-turn', status = 'running',
                 ownership_generation = 1
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs
             SET runtime_id = $1, status = 'running', source = 'console',
                 widget_session_id = NULL, session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let mut widget_headers = HeaderMap::new();
        widget_headers.insert(
            "x-agent-hub-embed-token",
            axum::http::HeaderValue::from_str(&widget_token).unwrap(),
        );
        let steered = create_widget_run(
            State(fixture.state.clone()),
            widget_headers,
            Json(CreateWidgetRunRequest {
                message: "widget joins the active console Turn".into(),
                session_id: None,
                integration_session_id: None,
                hub_session_id: Some(fixture.hub_session_id),
                parent_run_id: None,
                client_message_key: Some("widget-active-turn".into()),
                prepend_instructions: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(steered.id, fixture.run_id);
        assert_eq!(steered.source, "console");
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT widget_session_id FROM runs WHERE id = $1"
            )
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            Some(widget_session_id)
        );
        let app = build_router(test_state_with_pool(fixture.state.pool.clone()));
        let stop_request = |token: &str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!("/api/widget/runs/{}/stop", fixture.run_id))
                .header("x-agent-hub-embed-token", token)
                .body(Body::empty())
                .unwrap()
        };

        let stream_request = |token: &str| {
            axum::http::Request::builder()
                .uri(format!("/api/runs/{}/events/stream", fixture.run_id))
                .header("x-agent-hub-embed-token", token)
                .body(Body::empty())
                .unwrap()
        };
        let forbidden_stream = app
            .clone()
            .oneshot(stream_request(&other_widget_token))
            .await
            .unwrap();
        assert_eq!(forbidden_stream.status(), StatusCode::FORBIDDEN);
        let own_stream = app
            .clone()
            .oneshot(stream_request(&widget_token))
            .await
            .unwrap();
        assert_eq!(own_stream.status(), StatusCode::OK);

        let forbidden = app
            .clone()
            .oneshot(stop_request(&other_widget_token))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);
        let stopped = app.oneshot(stop_request(&widget_token)).await.unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);

        let completed = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "interrupted".into(),
                    native_session_id: Some("widget-stop-thread".into()),
                    work_dir_ref: Some("widget-retained-workspace".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(completed.status, "interrupted");
        let rerouted_run_id: Uuid = sqlx::query_scalar(
            "SELECT run_id FROM hub_session_messages
             WHERE session_id = $1 AND client_message_key = 'widget-active-turn'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_ne!(rerouted_run_id, fixture.run_id);
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT widget_session_id FROM runs WHERE id = $1"
            )
            .bind(rerouted_run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            Some(widget_session_id)
        );
    }

    #[test]
    fn integration_tool_request_registration_rechecks_active_agent_and_runtime_ownership() {
        assert!(ACTIVE_RUNTIME_TOOL_REQUEST_AGENT_SQL.contains("a.deleted_at IS NULL"));
        assert!(ACTIVE_RUNTIME_TOOL_REQUEST_AGENT_SQL.contains("FOR UPDATE"));
        assert!(ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL.contains("r.runtime_id = $2"));
        assert!(ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL.contains("r.session_ownership_generation = $3"));
        assert!(
            ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL.contains("status IN ('running', 'waiting_tool')")
        );
        assert!(ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL.contains("FOR UPDATE OF r"));
        assert!(!INTEGRATION_TOOL_REQUEST_INSERT_SQL.contains("ON CONFLICT"));
        assert!(INTEGRATION_TOOL_REQUEST_INSERT_SQL.contains("VALUES ($1, $2, $3"));
        assert_eq!(
            INTEGRATION_TOOL_REQUEST_INSERT_SQL
                .matches("INSERT INTO integration_tool_requests")
                .count(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn integration_sse_tool_request_is_immediately_submittable(pool: PgPool) {
        use futures_util::StreamExt;

        let fixture = integration_runtime_fixture(pool).await;
        let (trigger_name, function_name) =
            install_tool_request_delay_trigger(&fixture.state.pool, fixture.tool_request_id).await;
        let sse = stream_integration_events(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Query(IntegrationEventsQuery { after: Some(0) }),
        )
        .await
        .unwrap()
        .into_response();
        let mut body = sse.into_body().into_data_stream();
        let producer_state = fixture.state.clone();
        let producer_headers = bearer_headers(&fixture.runtime_token);
        let run_id = fixture.run_id;
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let producer = tokio::spawn(async move {
            let _ = runtime_finalize_tool_requests(
                State(producer_state),
                producer_headers,
                Path(run_id),
                runtime_write(batch),
            )
            .await?;
            Ok::<(), ApiError>(())
        });

        let request_id_text = fixture.tool_request_id.to_string();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut received = String::new();
            while let Some(chunk) = body.next().await {
                let chunk = chunk.expect("SSE body chunk should be readable");
                received.push_str(&String::from_utf8_lossy(&chunk));
                if received.contains(&request_id_text) {
                    return;
                }
            }
            panic!("integration SSE ended before the tool request event");
        })
        .await
        .expect("integration SSE should expose the tool request");

        let submit_result = submit_integration_tool_result(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "answer": 42 }),
            }),
        )
        .await;
        let producer_result = tokio::time::timeout(Duration::from_secs(5), producer)
            .await
            .expect("runtime producer should finish")
            .expect("runtime producer task should not panic");
        remove_tool_request_delay_trigger(&fixture.state.pool, &trigger_name, &function_name).await;

        producer_result.expect("runtime should finalize the complete tool batch");
        let submitted =
            submit_result.expect("an SSE-visible tool request should accept a result immediately");
        assert_eq!(submitted.run.hub_session_id, Some(fixture.hub_session_id));
        assert_ne!(submitted.run.hub_turn_id, Some(fixture.turn_id));
        let tool_result_message: (Uuid, Uuid, Option<Uuid>, String) = sqlx::query_as(
            "SELECT session_id, turn_id, run_id, message_kind
             FROM hub_session_messages
             WHERE session_id = $1 AND message_kind = 'tool_result'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(tool_result_message.0, fixture.hub_session_id);
        assert_eq!(tool_result_message.1, submitted.run.hub_turn_id.unwrap());
        assert_ne!(tool_result_message.1, fixture.turn_id);
        assert_eq!(tool_result_message.2, Some(submitted.run.id));
        assert_eq!(tool_result_message.3, "tool_result");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn large_tool_result_is_archived_truncated_and_summarized(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let (objects, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        // runtime 先 finalize 创建 tool_request 行（fixture 不直接插入）。
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .unwrap();
        let big_content = "x".repeat(TOOL_RESULT_TRUNCATE_BYTES + 1024);
        let submitted = submit_integration_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "content": big_content }),
            }),
        )
        .await
        .expect("large tool result must be accepted (not rejected)");
        assert!(submitted.tool_request.result_truncated);
        assert!(submitted.tool_request.artifact_id.is_some());
        assert!(submitted.tool_request.artifact_reason.is_none());
        assert!(
            submitted.tool_request.artifact_size_bytes.unwrap() as usize
                > TOOL_RESULT_TRUNCATE_BYTES
        );

        let (payload, artifact_id): (serde_json::Value, Option<Uuid>) = sqlx::query_as(
            "SELECT result_payload, artifact_id FROM integration_tool_requests WHERE id = $1",
        )
        .bind(fixture.tool_request_id)
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(payload.get("truncated"), Some(&json!(true)));
        let content = payload.get("content").and_then(Value::as_str).unwrap();
        assert!(content.len() <= TOOL_RESULT_TRUNCATE_BYTES);

        // S3 对象已落盘（键 tool-results/{run_id}/{artifact_id}）。
        let object_key = format!("tool-results/{}/{}", fixture.run_id, artifact_id.unwrap());
        let stored = objects.lock().unwrap().get(&object_key).cloned();
        assert!(
            stored.is_some(),
            "archived result must exist in object storage"
        );
        assert!(stored.unwrap().len() > TOOL_RESULT_TRUNCATE_BYTES);

        // context 摘要包含大小与读取指引。
        let mut tx = state.pool.begin().await.unwrap();
        let context = load_integration_context_for_run(&mut tx, &submitted.run)
            .await
            .unwrap()
            .expect("tool-result run must expose integration context");
        tx.commit().await.unwrap();
        let tool_result = context.tool_result.expect("single tool result is emitted");
        assert_eq!(tool_result.get("truncated"), Some(&json!(true)));
        let summary = tool_result.get("content").and_then(Value::as_str).unwrap();
        assert!(
            summary.contains("archived as artifact://"),
            "summary must point at the artifact"
        );
        assert!(
            summary.contains("agent_hub_integration_tool_result_read"),
            "summary must teach the model how to read the full result"
        );
        assert!(
            summary.contains("bytes total"),
            "summary must include the original size"
        );
        assert!(context.tool_results.is_empty());

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn oversized_tool_result_over_hard_limit_is_truncated_without_archive(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .unwrap();
        let huge_content = "y".repeat((4 * 1024 * 1024) + 1);
        let submitted = submit_integration_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "content": huge_content }),
            }),
        )
        .await
        .expect("result over the hard limit must still be accepted");
        assert!(submitted.tool_request.result_truncated);
        assert_eq!(
            submitted.tool_request.artifact_reason.as_deref(),
            Some("over_hard_limit")
        );
        assert!(submitted.tool_request.artifact_id.is_none());
        assert!(submitted.tool_request.artifact_size_bytes.unwrap() as usize > 4 * 1024 * 1024);

        let mut tx = state.pool.begin().await.unwrap();
        let context = load_integration_context_for_run(&mut tx, &submitted.run)
            .await
            .unwrap()
            .unwrap();
        tx.commit().await.unwrap();
        let summary = context
            .tool_result
            .and_then(|value| value.get("content").cloned())
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap();
        assert!(
            summary.contains("was NOT archived"),
            "over-limit summary must tell the model the result is unavailable"
        );
        assert!(!summary.contains("agent_hub_integration_tool_result_read"));

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archived_tool_result_reads_metadata_and_ranges(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .unwrap();
        let big_content = "z".repeat(TOOL_RESULT_TRUNCATE_BYTES + 4096);
        let submitted = submit_integration_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "content": big_content }),
            }),
        )
        .await
        .unwrap();
        let tool_request_id = submitted.tool_request.id;

        // size 元数据（runtime 通道，按 tool_request_id 解析 run）。
        let size = get_tool_result_artifact_by_request_id(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(tool_request_id),
            Query(ToolResultArtifactQuery {
                mode: Some("size".into()),
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .into_response();
        let size_body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(size.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            size_body["size_bytes"].as_i64().unwrap() as usize,
            TOOL_RESULT_TRUNCATE_BYTES + 4096 + "{\"content\":\"".len() + "\"}".len()
        );
        assert!(size_body["artifact_id"].is_string());
        assert_eq!(size_body["artifact_reason"], serde_json::Value::Null);

        // range 读取（64KB 上限内）。
        let range = get_tool_result_artifact_by_request_id(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(tool_request_id),
            Query(ToolResultArtifactQuery {
                mode: Some("range".into()),
                offset: Some(0),
                limit: Some(1024),
            }),
        )
        .await
        .unwrap()
        .into_response();
        let range_body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(range.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let content = range_body["content"].as_str().unwrap();
        assert_eq!(content.len(), 1024);
        assert_eq!(range_body["next_offset"].as_i64().unwrap(), 1024);

        // 未授权（无凭据）拒绝。
        let unauthorized = get_tool_result_artifact_by_request_id(
            State(state.clone()),
            HeaderMap::new(),
            Path(tool_request_id),
            Query(ToolResultArtifactQuery {
                mode: Some("size".into()),
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(unauthorized.status, StatusCode::UNAUTHORIZED);

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archived_tool_result_full_mode_streams_the_whole_result(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .unwrap();
        let big_content = "w".repeat(TOOL_RESULT_TRUNCATE_BYTES + 8192);
        let submitted = submit_integration_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "content": big_content }),
            }),
        )
        .await
        .unwrap();
        let tool_request_id = submitted.tool_request.id;

        let full = get_tool_result_artifact_by_request_id(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(tool_request_id),
            Query(ToolResultArtifactQuery {
                mode: Some("full".into()),
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap()
        .into_response();
        let body = axum::body::to_bytes(full.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            parsed["content"].as_str().unwrap().len(),
            TOOL_RESULT_TRUNCATE_BYTES + 8192
        );

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn session_deletion_removes_tool_result_artifacts(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool.clone()).await;
        let (objects, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .unwrap();
        let big_content = "v".repeat(TOOL_RESULT_TRUNCATE_BYTES + 1024);
        let submitted = submit_integration_tool_result(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "content": big_content }),
            }),
        )
        .await
        .unwrap();
        let artifact_id = submitted.tool_request.artifact_id.unwrap();
        let object_key = format!("tool-results/{}/{}", fixture.run_id, artifact_id);
        assert!(objects.lock().unwrap().contains_key(&object_key));

        // 会话删除走 owner 路径：为 hub_session 的 owner 造一个会话 token。
        // integration fixture 的 state 无 session provider，删除用独立 state。
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&state.pool)
            .await
            .unwrap();
        let owner_token = format!("ahs_owner_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&owner_token))
        .bind(owner_id)
        .execute(&state.pool)
        .await
        .unwrap();
        // 删除守卫要求会话处于非活动状态：置为 historical 并把 run 标终态。
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'historical' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runs SET status = 'completed' WHERE hub_session_id = $1")
            .bind(fixture.hub_session_id)
            .execute(&state.pool)
            .await
            .unwrap();
        let mut session_state = test_state_with_browser_session_auth(pool.clone());
        session_state.session_bundle_store = state.session_bundle_store.clone();
        let session_state = Arc::new(session_state);
        let deleted = delete_hub_session(
            State(session_state.clone()),
            session_headers(&owner_token),
            Path(fixture.hub_session_id),
        )
        .await
        .unwrap();
        assert_eq!(deleted, StatusCode::NO_CONTENT);
        assert!(
            !objects.lock().unwrap().contains_key(&object_key),
            "tool result artifact must be deleted with the session"
        );

        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn widget_attachment_upload_and_download_are_scoped_to_the_embed_token(pool: PgPool) {
        let fixture = attachment_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);

        let other_hub_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
        )
        .bind(other_hub_session_id)
        .bind(fixture.owner_id)
        .bind(fixture.agent_id)
        .execute(&state.pool)
        .await
        .unwrap();
        let widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        let other_widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        for (embed_id, token, hub_session_id) in [
            (Uuid::new_v4(), &widget_token, fixture.session_id),
            (Uuid::new_v4(), &other_widget_token, other_hub_session_id),
        ] {
            sqlx::query(
                "INSERT INTO embed_sessions
                     (id, token_hash, agent_id, owner_id, expires_at, hub_session_id)
                 VALUES ($1, $2, $3, $4, now() + interval '1 hour', $5)",
            )
            .bind(embed_id)
            .bind(sha256_hex(token))
            .bind(fixture.agent_id)
            .bind(fixture.owner_id)
            .bind(hub_session_id)
            .execute(&state.pool)
            .await
            .unwrap();
        }
        let mut widget_headers = HeaderMap::new();
        widget_headers.insert(
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderValue::from_str(&widget_token).unwrap(),
        );
        let uploaded = upload_widget_attachment(
            State(state.clone()),
            widget_headers.clone(),
            Query(AttachmentUploadQuery {
                session_id: Some(fixture.session_id),
            }),
            attachment_multipart(
                "widget-upload",
                None,
                "widget.bin",
                "application/octet-stream",
                b"widget-bytes",
            )
            .await,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(uploaded.session_id, fixture.session_id);

        let downloaded =
            download_widget_attachment(State(state.clone()), widget_headers, Path(uploaded.id))
                .await
                .unwrap();
        assert_eq!(downloaded.status(), StatusCode::OK);

        let mut other_headers = HeaderMap::new();
        other_headers.insert(
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderValue::from_str(&other_widget_token).unwrap(),
        );
        let isolated = download_widget_attachment(
            State(state.clone()),
            other_headers.clone(),
            Path(uploaded.id),
        )
        .await
        .unwrap_err();
        assert_eq!(isolated.status, StatusCode::NOT_FOUND);

        let other_uploaded = upload_widget_attachment(
            State(state.clone()),
            other_headers,
            Query(AttachmentUploadQuery {
                session_id: Some(other_hub_session_id),
            }),
            attachment_multipart(
                "widget-other-upload",
                None,
                "other.bin",
                "application/octet-stream",
                b"other-bytes",
            )
            .await,
        )
        .await
        .unwrap()
        .0;
        assert_eq!(other_uploaded.session_id, other_hub_session_id);
        server.abort();
    }
}
