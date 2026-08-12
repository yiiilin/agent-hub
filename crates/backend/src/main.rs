#![recursion_limit = "1024"]

mod api;
use api::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[cfg(test)]
use axum::extract::{Form, Multipart};
#[cfg(test)]
use chrono::Duration as ChronoDuration;
#[cfg(test)]
use ldap3::result::LdapError;
#[cfg(test)]
use std::net::IpAddr;

use agent_hub_backend::ModelSecretCipher;
use agent_hub_shared::*;
use anyhow::Context;
use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{any, delete, get, post, put},
    Json, Router,
};
use base64::Engine;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use hmac::Hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use tower::ServiceBuilder;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    services::{ServeDir, ServeFile},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
    ServiceExt,
};
use tracing::{info, warn};
use uuid::Uuid;
use zeroize::Zeroizing;

#[cfg(test)]
use crate::skill_package_store::SkillPackageStore;
#[cfg(test)]
use base64::engine::general_purpose::STANDARD;
#[cfg(test)]
use ipnet::IpNet;
#[cfg(test)]
use std::io::Read;
#[cfg(test)]
use url::Url;

pub(crate) mod run_event_bus;
mod session_bundle_store;
mod skill_package_store;

type HmacSha256 = Hmac<Sha256>;
pub(crate) const REDACTED_SECRET: &str = "********";
pub(crate) const DEFAULT_MODEL_PROXY_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const MAX_MODEL_PROXY_TIMEOUT: Duration = Duration::from_secs(900);
pub(crate) const DATABASE_READINESS_TIMEOUT: Duration = Duration::from_millis(500);
pub(crate) const DEFAULT_SESSION_BUNDLE_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;
pub(crate) const DEFAULT_FRONTEND_DIST_DIR: &str = "frontend/dist";
pub(crate) const SKILL_PACKAGE_UPLOAD_BODY_LIMIT: usize =
    MAX_SKILL_PACKAGE_EXPANDED_BYTES as usize + 2 * 1024 * 1024;
pub(crate) const MAX_CLIENT_TOOL_COUNT: usize = 128;
pub(crate) const MAX_CLIENT_TOOL_DEFINITIONS_BYTES: usize = 256_000;
pub(crate) const MAX_CLIENT_TOOL_RESULT_BYTES: usize = 16_000;
pub(crate) const EMBEDDED_ORIGIN_HEADER: &str = "x-agent-hub-embedded-origin";
pub(crate) const MAX_ATTACHMENT_UPLOAD_BYTES: u64 = 104_857_600;
pub(crate) const MAX_ATTACHMENT_BYTES_PER_SESSION: i64 = 524_288_000;
pub(crate) const ATTACHMENT_UPLOAD_BODY_LIMIT: usize = 1024 * 1024 * 1024 + 1024 * 1024;
pub(crate) const VISION_PROXY_HEADER: &str = "x-agent-hub-vision";
pub(crate) const CLIENT_ACCESS_TTL_SECONDS: i64 = 15 * 60;
pub(crate) const CLIENT_TOOL_DEADLINE_MINUTES: i64 = 5;
pub(crate) const TOOL_REQUEST_BATCH_FINGERPRINT_KEY: &str = "tool_request_batch_fingerprint";
pub(crate) const SESSION_MESSAGE_PAGE_SQL: &str =
    "SELECT id, session_id, sequence, role, message_kind, content, payload,
            delivery_mode, delivery_state, client_message_key,
            expected_native_turn_id, turn_id, run_id, accepted_at
     FROM (
         SELECT id, session_id, sequence, role, message_kind, content, payload,
                delivery_mode, delivery_state, client_message_key,
                expected_native_turn_id, turn_id, run_id, accepted_at
         FROM hub_session_messages
         WHERE session_id = $1
           AND ($2::bigint IS NULL OR sequence < $2)
         ORDER BY sequence DESC
         LIMIT COALESCE($3::bigint, 9223372036854775807)
     ) AS message_page
     ORDER BY sequence";
pub(crate) const RUNTIME_CAPABILITY_SQL: &str = "
           AND (
             a.model_policy->>'provider' IS DISTINCT FROM 'hub-proxy'
             OR COALESCE((rt.capabilities->>'model_proxy')::boolean, false) = true
           )
           AND (
             jsonb_typeof(a.mcp_allowlist) IS DISTINCT FROM 'array'
             OR jsonb_array_length(a.mcp_allowlist) = 0
             OR COALESCE((rt.capabilities->>'mcp_allowlist')::boolean, false) = true
           )
           AND (
             NOT EXISTS (
               SELECT 1 FROM subagent_definitions subagent
               WHERE subagent.agent_id = a.id AND subagent.enabled = true
             )
             OR COALESCE((rt.capabilities->>'subagents')::boolean, false) = true
           )
           AND (
             a.sandbox_policy->>'mode' IS DISTINCT FROM 'workspace-write'
             OR rt.sandbox_mode LIKE 'workspace-write%'
           )
           AND (
             a.sandbox_policy->>'mode' IS DISTINCT FROM 'danger-full-access'
             OR rt.sandbox_mode LIKE 'danger-full-access%'
           )";
pub(crate) const ACTIVE_RUNTIME_TOOL_REQUEST_AGENT_SQL: &str = "
    SELECT a.id
    FROM agents a
    WHERE a.id = (SELECT r.agent_id FROM runs r WHERE r.id = $1)
      AND a.deleted_at IS NULL
    FOR UPDATE";
pub(crate) const ACTIVE_RUNTIME_TOOL_REQUEST_RUN_SQL: &str = "
    SELECT r.integration_session_id, r.hub_session_id, r.hub_turn_id,
           r.status, r.native_session_id, r.work_dir_ref,
           r.client_instance_id, r.client_tool_snapshot
    FROM runs r
    WHERE r.id = $1
      AND r.runtime_id = $2
      AND r.session_ownership_generation = $3
      AND r.status IN ('running', 'waiting_tool')
    FOR UPDATE OF r";
pub(crate) const INTEGRATION_TOOL_REQUEST_INSERT_SQL: &str = "
    INSERT INTO integration_tool_requests
        (id, session_id, hub_session_id, run_id, position,
         tool_name, arguments, status, expires_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)";

#[tokio::main]
pub(crate) async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "agent_hub_backend=info,tower_http=info".into()),
        )
        .init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let bind_addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;
    let model_secret_key = env::var("HUB_MODEL_SECRET_KEY").ok();
    let model_secret_cipher = ModelSecretCipher::from_env_value(model_secret_key.as_deref())?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    if env::var("SEED_DEV_USER")
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        seed_dev_user(&pool).await?;
    }
    if env::var("SEED_DEV_MODEL_CONNECTION")
        .map(|value| value == "true")
        .unwrap_or(false)
    {
        seed_dev_model_connection(
            &pool,
            &model_secret_cipher,
            &env::var("DEV_MODEL_PROVIDER_BASE_URL").context(
                "DEV_MODEL_PROVIDER_BASE_URL is required when development model seeding is enabled",
            )?,
            env::var("DEV_MODEL_PROVIDER_MODEL_IDS")
                .context(
                    "DEV_MODEL_PROVIDER_MODEL_IDS is required when development model seeding is enabled",
                )?
                .split(',')
                .map(str::to_owned)
                .collect(),
            &env::var("DEV_MODEL_PROVIDER_API_KEY").context(
                "DEV_MODEL_PROVIDER_API_KEY is required when development model seeding is enabled",
            )?,
        )
        .await?;
    }
    if let Ok(token) = env::var("DEV_RUNTIME_ENROLLMENT_TOKEN") {
        ensure_dev_runtime_enrollment_token(&pool, &token).await?;
    }

    let model_proxy_timeout = model_proxy_timeout_from_env()?;
    let (model_gateway_url, model_gateway_auth_token) = model_gateway_config_from_env()?;
    let model_proxy_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10).min(model_proxy_timeout))
        .timeout(model_proxy_timeout)
        .read_timeout(model_proxy_timeout)
        .build()?;
    let session_bundle_store = session_bundle_store_from_env()?;
    let skill_package_store = skill_package_store_from_env(session_bundle_store.clone())?;
    let session_bundle_max_bytes = session_bundle_max_bytes_from_env()?;
    let state = AppState {
        pool,
        session_cookie_secure: env::var("SESSION_COOKIE_SECURE")
            .map(|v| v == "true")
            .unwrap_or(false),
        embed_jwt_secret: env::var("EMBED_JWT_SECRET").context("EMBED_JWT_SECRET is required")?,
        embed_jwt_issuer: env::var("EMBED_JWT_ISSUER").unwrap_or_else(|_| "agent-hub-dev".into()),
        embed_jwt_audience: env::var("EMBED_JWT_AUDIENCE")
            .unwrap_or_else(|_| "agent-hub-widget".into()),
        trusted_proxy_cidrs: trusted_proxy_cidrs_from_env()?,
        model_secret_cipher,
        model_proxy_http,
        model_gateway_url,
        model_gateway_auth_token,
        session_bundle_store,
        skill_package_store: Some(skill_package_store),
        session_bundle_max_bytes,
        auth_providers: vec![
            Arc::new(PasswordAuthProvider),
            Arc::new(BrowserSessionAuthProvider),
            Arc::new(ApiKeyAuthProvider),
            Arc::new(EmbedJwtAuthProvider),
        ],
        session_issuer: Arc::new(BrowserSessionIssuer),
        run_event_bus: Arc::new(run_event_bus::InMemoryRunEventBus::default()),
    };

    let scheduler_pool = state.pool.clone();
    tokio::spawn(async move {
        automation_scheduler_loop(scheduler_pool).await;
    });
    let reaper_pool = state.pool.clone();
    tokio::spawn(async move {
        runtime_reaper_loop(reaper_pool).await;
    });
    let erasure_state = Arc::new(state.clone());
    tokio::spawn(async move {
        user_erasure_loop(erasure_state).await;
    });
    let skill_package_deletion_state = Arc::new(state.clone());
    tokio::spawn(async move {
        skill_package_deletion_loop(skill_package_deletion_state).await;
    });
    let builtin_skill_state = Arc::new(state.clone());
    tokio::spawn(async move {
        builtin_skill_seed_loop(builtin_skill_state).await;
    });
    let attachment_orphan_state = Arc::new(state.clone());
    tokio::spawn(async move {
        runtime_attachment_orphan_loop(attachment_orphan_state).await;
    });
    let app = build_router(state);
    info!("backend listening on {bind_addr}");
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}

pub(crate) fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin, request| {
            let path = request.uri.path();
            path.starts_with("/api/client/")
                || path.starts_with("/api/widget/")
                || matches!(
                    origin.to_str(),
                    Ok("http://localhost:5173" | "http://127.0.0.1:5173")
                )
        }))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-agent-hub-webhook-token"),
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderName::from_static(EMBEDDED_ORIGIN_HEADER),
        ])
        .allow_credentials(true);

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readiness))
        .route("/openapi.json", get(openapi))
        .route("/api/auth/register", post(register_password_user))
        .route("/api/auth/login", post(login))
        .route("/api/auth/ldap/login", post(ldap_login))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/me", get(me))
        .route("/api/users/me", axum::routing::patch(update_current_user))
        .route("/api/users", get(list_users))
        .route(
            "/api/admin/users",
            get(list_admin_users).post(create_admin_user),
        )
        .route(
            "/api/admin/users/{user_id}",
            get(get_admin_user).patch(update_admin_user),
        )
        .route(
            "/api/admin/users/{user_id}/password",
            put(set_admin_user_password),
        )
        .route("/api/admin/users/{user_id}/role", put(set_admin_user_role))
        .route("/api/admin/user-erasures", get(list_user_erasures))
        .route("/api/admin/users/{user_id}/erase", post(erase_user))
        .route("/api/auth/providers", get(auth_providers))
        .route(
            "/api/admin/auth-policy",
            get(get_auth_policy).patch(update_auth_policy),
        )
        .route(
            "/api/admin/ldap-config",
            get(get_ldap_configuration).put(update_ldap_configuration),
        )
        .route("/api/admin/ldap-config/test", post(test_ldap_configuration))
        .route(
            "/api/admin/system-settings",
            get(get_system_settings).patch(update_system_settings),
        )
        .route(
            "/api/config/attachment-limits",
            get(get_public_attachment_limits),
        )
        .route(
            "/api/admin/external-platforms",
            get(list_external_platforms).post(create_external_platform),
        )
        .route(
            "/api/admin/external-platforms/{platform_id}",
            axum::routing::patch(update_external_platform),
        )
        .route(
            "/api/admin/external-platforms/{platform_id}/authentication-channels",
            get(list_authentication_channels).post(create_authentication_channel),
        )
        .route(
            "/api/admin/authentication-channels/{channel_id}",
            axum::routing::patch(update_authentication_channel),
        )
        .route(
            "/api/auth/api-keys",
            get(list_api_keys).post(create_api_key),
        )
        .route("/api/auth/api-keys/{api_key_id}/renew", post(renew_api_key))
        .route("/api/auth/api-keys/{api_key_id}", delete(delete_api_key))
        .route(
            "/api/secrets",
            get(list_user_secrets).post(create_user_secret),
        )
        .route(
            "/api/secrets/{secret_id}",
            put(update_user_secret).delete(delete_user_secret),
        )
        .route(
            "/api/secret-grants",
            get(list_secret_grants).post(create_secret_grants),
        )
        .route(
            "/api/secret-grants/{agent_id}/{secret_name}",
            delete(delete_secret_grant),
        )
        .route(
            "/api/model-connections",
            get(list_model_connections).post(create_model_connection),
        )
        .route(
            "/api/model-connections/options",
            get(get_model_connection_options),
        )
        .route(
            "/api/model-connections/{model_connection_id}",
            get(get_model_connection)
                .put(update_model_connection)
                .delete(delete_model_connection),
        )
        .route(
            "/api/model-connections/{model_connection_id}/status",
            put(update_model_connection_status),
        )
        .route(
            "/api/model-connections/{model_connection_id}/test",
            post(test_model_connection),
        )
        .route(
            "/api/model-connections/{model_connection_id}/force-delete",
            post(force_delete_model_connection),
        )
        .route(
            "/api/model-connections/system-default",
            get(get_system_default_model_selection).put(set_system_default_model_selection),
        )
        .route("/api/model-usage/summary", get(get_model_usage_summary))
        .route("/api/model-usage", get(list_model_token_usage))
        .route("/api/model-call-errors", get(list_model_call_errors))
        .route("/api/agents", get(list_agents).post(create_agent))
        .route(
            "/api/agents/{agent_id}",
            get(get_agent).patch(update_agent).delete(delete_agent),
        )
        .route(
            "/api/agents/{agent_id}/model-options",
            get(get_agent_model_connection_options),
        )
        .route(
            "/api/integration-app-options",
            get(get_integration_app_options),
        )
        .route(
            "/api/integration-apps",
            get(list_integration_apps).post(create_integration_app),
        )
        .route(
            "/api/integration-apps/{app_id}",
            get(get_integration_app).patch(update_integration_app),
        )
        .route(
            "/api/integration-apps/{app_id}/rotate-secret",
            post(rotate_integration_app_secret),
        )
        .route(
            "/api/integration-apps/{app_id}/agents/{agent_id}/widget-session",
            post(create_integration_app_widget_session),
        )
        .route("/api/oauth/authorize", get(oauth_authorize))
        .route("/api/oauth/token", post(oauth_token))
        .route("/api/oauth/userinfo", get(oauth_userinfo))
        .route(
            "/api/integrations/embed-session",
            post(create_integration_embed_session),
        )
        .route(
            "/api/agents/{agent_id}/runs",
            get(list_agent_runs).post(create_run),
        )
        .route(
            "/api/sessions",
            get(list_hub_sessions)
                .post(create_session_with_message)
                .layer(DefaultBodyLimit::max(ATTACHMENT_UPLOAD_BODY_LIMIT)),
        )
        .route(
            "/api/sessions/{session_id}",
            get(get_hub_session).delete(delete_hub_session),
        )
        .route(
            "/api/sessions/{session_id}/title",
            put(update_hub_session_title),
        )
        .route(
            "/api/sessions/{session_id}/messages",
            get(list_hub_session_messages).post(create_hub_session_message),
        )
        .route(
            "/api/sessions/{session_id}/messages/{message_id}/attachments",
            post(bind_message_attachments),
        )
        .route(
            "/api/sessions/{session_id}/messages/upload",
            post(create_session_message_with_attachments)
                .layer(DefaultBodyLimit::max(ATTACHMENT_UPLOAD_BODY_LIMIT)),
        )
        .route(
            "/api/attachments",
            post(upload_attachment).layer(DefaultBodyLimit::max(ATTACHMENT_UPLOAD_BODY_LIMIT)),
        )
        .route("/api/attachments/{attachment_id}", get(download_attachment))
        .route("/api/runs/{run_id}", get(get_run))
        .route("/api/runs/{run_id}/stop", post(stop_hub_run))
        .route("/api/runs/{run_id}/events", get(list_run_events))
        .route("/api/runs/{run_id}/events/stream", get(stream_run_events))
        .route("/api/runtimes", get(list_runtimes))
        .route(
            "/api/admin/runtime-enrollment-tokens",
            get(list_runtime_enrollment_tokens).post(create_runtime_enrollment_token),
        )
        .route(
            "/api/admin/runtime-enrollment-tokens/{enrollment_id}/revoke",
            post(revoke_runtime_enrollment_token),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}/credential-rotation",
            post(request_runtime_credential_rotation),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}/drain",
            post(drain_runtime),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}/cancel-drain",
            post(cancel_runtime_drain),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}",
            delete(delete_drained_runtime),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}/deletion-impact",
            get(get_runtime_deletion_impact),
        )
        .route(
            "/api/admin/runtimes/{runtime_id}/force-delete",
            post(force_delete_runtime),
        )
        .route(
            "/api/skills",
            get(list_skills)
                .post(create_skill)
                .delete(bulk_delete_skills),
        )
        .route(
            "/api/skills/{skill_id}",
            get(get_skill).patch(update_skill).delete(delete_skill),
        )
        .route(
            "/api/skills/{skill_id}/package",
            put(replace_skill_package)
                .delete(delete_skill_package)
                .layer(DefaultBodyLimit::max(SKILL_PACKAGE_UPLOAD_BODY_LIMIT)),
        )
        .route(
            "/api/automations",
            get(list_automations).post(create_automation),
        )
        .route(
            "/api/automations/{automation_id}",
            axum::routing::patch(update_automation),
        )
        .route(
            "/api/automations/{automation_id}/trigger",
            post(trigger_automation),
        )
        .route(
            "/api/automations/{automation_id}/runs",
            get(list_automation_runs),
        )
        .route("/api/automations/webhook", post(trigger_automation_webhook))
        .route("/api/embed/sessions", post(create_embed_session))
        .route("/api/embed/exchange", post(exchange_embed_jwt))
        .route("/api/client/access", post(create_client_access))
        .route(
            "/api/client/anonymous/access",
            post(create_anonymous_client_access),
        )
        .route("/api/client/renew", post(renew_client_access))
        .route("/api/client/session", get(get_widget_session))
        .route("/api/client/sessions", get(list_widget_sessions))
        .route(
            "/api/client/sessions/{session_id}",
            delete(delete_widget_session),
        )
        .route(
            "/api/client/sessions/{session_id}/messages",
            get(list_widget_session_messages),
        )
        .route(
            "/api/client/sessions/{session_id}/events",
            get(list_widget_session_events),
        )
        .route(
            "/api/client/sessions/{session_id}/events/stream",
            get(stream_widget_session_events),
        )
        .route("/api/client/runs", post(create_widget_run))
        .route("/api/client/runs/{run_id}/stop", post(stop_widget_run))
        .route(
            "/api/client/attachments",
            post(upload_widget_attachment)
                .layer(DefaultBodyLimit::max(ATTACHMENT_UPLOAD_BODY_LIMIT)),
        )
        .route(
            "/api/client/attachments/{attachment_id}",
            get(download_widget_attachment),
        )
        .route(
            "/api/client/tool-calls/{tool_call_id}/claim",
            post(claim_client_tool_call),
        )
        .route(
            "/api/client/tool-calls/{tool_call_id}/result",
            post(submit_client_tool_result),
        )
        .route("/api/widget/access", post(create_widget_access))
        .route(
            "/api/widget/public/access",
            post(create_public_widget_access),
        )
        .route("/api/widget/session", get(get_widget_session))
        .route("/api/widget/session/renew", post(renew_widget_session))
        .route("/api/widget/sessions", get(list_widget_sessions))
        .route(
            "/api/widget/sessions/{session_id}/messages",
            get(list_widget_session_messages),
        )
        .route(
            "/api/widget/sessions/{session_id}/events",
            get(list_widget_session_events),
        )
        .route(
            "/api/widget/sessions/{session_id}/events/stream",
            get(stream_widget_session_events),
        )
        .route("/api/widget/runs", post(create_widget_run))
        .route("/api/widget/runs/{run_id}/stop", post(stop_widget_run))
        .route(
            "/api/widget/attachments",
            post(upload_widget_attachment)
                .layer(DefaultBodyLimit::max(ATTACHMENT_UPLOAD_BODY_LIMIT)),
        )
        .route(
            "/api/widget/attachments/{attachment_id}",
            get(download_widget_attachment),
        )
        .route(
            "/api/integrations/sessions",
            post(create_integration_session),
        )
        .route(
            "/api/integrations/sessions/{session_id}",
            get(get_integration_session),
        )
        .route(
            "/api/integrations/sessions/{session_id}/messages",
            get(list_integration_messages).post(create_integration_message),
        )
        .route(
            "/api/integrations/sessions/{session_id}/runs/{run_id}/stop",
            post(stop_integration_run),
        )
        .route(
            "/api/integrations/sessions/{session_id}/events",
            get(list_integration_events),
        )
        .route(
            "/api/integrations/sessions/{session_id}/events/stream",
            get(stream_integration_events),
        )
        .route(
            "/api/integrations/tool-requests/{tool_request_id}/result",
            post(submit_integration_tool_result),
        )
        .route("/api/runtime/register", post(runtime_register))
        .route("/api/runtime/heartbeat", post(runtime_heartbeat))
        .route("/api/runtime/runs/claim", post(runtime_claim_run))
        .route(
            "/api/runtime/runs/{run_id}/secrets/{secret_name}",
            get(runtime_download_run_secret_file),
        )
        .route(
            "/api/runtime/runs/{run_id}/skills/{skill_id}/package",
            get(runtime_download_run_skill_package),
        )
        .route(
            "/api/runtime/sessions/{session_id}/skills/{skill_id}/packages/{package_id}",
            get(runtime_download_session_skill_package),
        )
        .route(
            "/api/runtime/runs/{run_id}/turn/begin",
            post(runtime_begin_turn),
        )
        .route(
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete",
            post(runtime_complete_session_command),
        )
        .route(
            "/api/runtime/sessions/{session_id}/release",
            post(runtime_release_session),
        )
        .route(
            "/api/runtime/sessions/{session_id}/checkpoint/begin",
            post(runtime_begin_session_checkpoint),
        )
        .route(
            "/api/runtime/sessions/{session_id}/checkpoint/fail",
            post(runtime_fail_session_checkpoint),
        )
        .route(
            "/api/runtime/sessions/{session_id}/bundle",
            put(runtime_upload_session_bundle).get(runtime_download_session_bundle),
        )
        .route(
            "/api/runtime/sessions/{session_id}/salvage-bundle",
            put(runtime_salvage_session_bundle),
        )
        .route(
            "/api/runtime/attachments/{attachment_id}",
            get(download_runtime_attachment),
        )
        .route(
            "/api/runtime/sessions/{session_id}/salvage-abandon",
            post(runtime_abandon_session_salvage),
        )
        .route(
            "/api/runtime/runs/{run_id}/events",
            post(runtime_append_event),
        )
        .route(
            "/api/runtime/runs/{run_id}/tool-requests/finalize",
            post(runtime_finalize_tool_requests),
        )
        .route(
            "/api/runtime/runs/{run_id}/complete",
            post(runtime_complete_run),
        )
        .route(
            "/api/runtime/model-proxy/v1/{*path}",
            post(runtime_model_proxy),
        )
        .route("/widget", get(widget_page));

    with_frontend(router, PathBuf::from(DEFAULT_FRONTEND_DIST_DIR))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(state))
}

pub(crate) fn with_frontend<S>(router: Router<S>, frontend_dist_dir: PathBuf) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let spa = ServiceBuilder::new()
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        ))
        .service(
            ServeDir::new(&frontend_dist_dir)
                .fallback(ServeFile::new(frontend_dist_dir.join("index.html"))),
        );
    router
        .route("/api", any(api_not_found))
        .route("/api/", any(api_not_found))
        .route("/api/{*path}", any(api_not_found))
        .nest_service(
            "/assets",
            ServeDir::new(frontend_dist_dir.join("assets")).append_response_header(
                header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=31536000, immutable"),
            ),
        )
        .fallback_service(spa)
}

#[derive(Debug, Deserialize)]
struct WidgetPageQuery {
    app: Option<String>,
}

pub(crate) async fn widget_page(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WidgetPageQuery>,
) -> Result<Response, ApiError> {
    let contents = r#"<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Agent Hub Widget</title>
  </head>
  <body style="margin: 0">
    <agent-hub-chat mode="fullscreen"></agent-hub-chat>
    <script src="/embed/agent-hub-chat.js"></script>
  </body>
</html>"#;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if let Some(client_id) = query.app {
        let app = load_public_widget_app_by_client_id(&state.pool, &client_id).await?;
        let frame_ancestors = if app.allowed_origins.is_empty() {
            "'none'".to_owned()
        } else {
            app.allowed_origins.join(" ")
        };
        let csp = HeaderValue::from_str(&format!("frame-ancestors {frame_ancestors}"))
            .map_err(|_| ApiError::internal("public Widget CSP could not be encoded"))?;
        headers.insert(HeaderName::from_static("content-security-policy"), csp);
    }
    Ok((headers, contents).into_response())
}

pub(crate) async fn api_not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

pub(crate) async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

pub(crate) async fn openapi() -> Json<Value> {
    Json(openapi_document())
}

pub(crate) fn openapi_document() -> Value {
    let id = |name: &str| {
        json!({
            "name": name, "in": "path", "required": true,
            "schema": { "type": "string", "format": "uuid" }
        })
    };
    let response = |schema: &str| {
        json!({
            "description": "Success",
            "content": { "application/json": { "schema": { "$ref": format!("#/components/schemas/{schema}") } } }
        })
    };
    let list_response = |schema: &str| {
        json!({
            "description": "Success",
            "content": { "application/json": { "schema": { "type": "array", "items": { "$ref": format!("#/components/schemas/{schema}") } } } }
        })
    };
    let body = |schema: &str| {
        json!({
            "required": true,
            "content": { "application/json": { "schema": { "$ref": format!("#/components/schemas/{schema}") } } }
        })
    };
    let required_header = |name: &str, schema: Value| {
        json!({
            "name": name,
            "in": "header",
            "required": true,
            "schema": schema
        })
    };
    let model_ledger_parameters = || {
        json!([
            { "name": "from_ms", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64" } },
            { "name": "to_ms", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64" } },
            { "name": "model_connection_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
            { "name": "agent_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
            { "name": "user_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
            { "name": "cursor_occurred_at_ms", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64" } },
            { "name": "cursor_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } },
            { "name": "page_size", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 50 } }
        ])
    };
    let no_content = || json!({ "description": "Completed" });
    let mut document = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Agent Hub API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "User, agent, run, embed, and external integration APIs exposed by Agent Hub."
        },
        "servers": [{ "url": "/", "description": "This Agent Hub deployment" }],
        "paths": {
            "/api/auth/login": { "post": { "summary": "Sign in with password", "security": [], "requestBody": body("LoginRequest"), "responses": { "200": response("LoginResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "429": { "$ref": "#/components/responses/TooManyRequests" } } } },
            "/api/auth/ldap/login": { "post": { "summary": "Sign in with the global LDAP Directory", "security": [], "requestBody": body("LoginRequest"), "responses": { "200": response("LoginResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "429": { "$ref": "#/components/responses/TooManyRequests" }, "503": { "$ref": "#/components/responses/ServiceUnavailable" } } } },
            "/api/auth/register": { "post": { "summary": "Register with password", "security": [], "requestBody": body("PasswordRegistrationRequest"), "responses": { "200": response("PasswordRegistrationResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/auth/logout": { "post": { "summary": "Clear browser session", "security": [], "responses": { "204": no_content() } } },
            "/api/auth/me": { "get": { "summary": "Get current user", "responses": { "200": response("User"), "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/users/me": { "patch": { "summary": "Update the current user's Display Name", "requestBody": body("UpdateCurrentUserRequest"), "responses": { "200": response("User"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/auth/providers": { "get": { "summary": "List enabled login providers", "security": [], "responses": { "200": response("AuthProvidersResponse") } } },
            "/api/admin/auth-policy": {
                "get": { "summary": "Get authentication policy", "responses": { "200": response("AuthPolicy"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "patch": { "summary": "Update authentication policy", "requestBody": body("AuthPolicy"), "responses": { "200": response("AuthPolicy"), "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/admin/system-settings": {
                "get": { "summary": "Get system-wide settings (attachment limits)", "responses": { "200": response("SystemSettings"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "patch": { "summary": "Update system-wide settings", "requestBody": body("UpdateSystemSettingsRequest"), "responses": { "200": response("SystemSettings"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/admin/ldap-config": {
                "get": { "summary": "Get the optional global LDAP configuration", "responses": { "200": response("NullableLdapConfiguration"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "put": { "summary": "Save the global LDAP configuration", "requestBody": body("LdapConfiguration"), "responses": { "200": response("LdapConfiguration"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/admin/ldap-config/test": { "post": { "summary": "Test an unsaved LDAP configuration with one-time credentials", "requestBody": body("TestLdapConfigurationRequest"), "responses": { "200": response("TestLdapConfigurationResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "429": { "$ref": "#/components/responses/TooManyRequests" }, "503": { "$ref": "#/components/responses/ServiceUnavailable" } } } },
            "/api/admin/users": {
                "get": { "summary": "List Hub users", "responses": { "200": list_response("AdminUserDetail"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "post": { "summary": "Create a Hub user", "requestBody": body("AdminCreateUserRequest"), "responses": { "200": response("AdminUserDetail"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/admin/users/{user_id}": {
                "get": { "summary": "Get Hub user details", "parameters": [id("user_id")], "responses": { "200": response("AdminUserDetail"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "patch": { "summary": "Update a Hub user's Email and Display Name", "parameters": [id("user_id")], "requestBody": body("AdminUpdateUserRequest"), "responses": { "200": response("AdminUserDetail"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/admin/users/{user_id}/password": { "put": { "summary": "Set Hub user password and invalidate browser sessions", "parameters": [id("user_id")], "requestBody": body("AdminSetUserPasswordRequest"), "responses": { "200": response("AdminUserDetail"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/admin/users/{user_id}/role": { "put": { "summary": "Change a Hub user role as a Super Administrator", "parameters": [id("user_id")], "requestBody": body("AdminSetUserRoleRequest"), "responses": { "200": response("AdminUserDetail"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/admin/user-erasures": {
                "get": { "summary": "List pending and completed Hub User erasures", "responses": { "200": list_response("UserErasure"), "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/admin/users/{user_id}/erase": {
                "post": { "summary": "Irreversibly erase a Hub User after exact Email confirmation", "parameters": [id("user_id")], "requestBody": body("EraseUserRequest"), "responses": { "202": response("UserErasure"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/admin/external-platforms": {
                "get": { "summary": "List external platforms", "responses": { "200": list_response("ExternalPlatform"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "post": { "summary": "Create external platform", "requestBody": body("CreateExternalPlatformRequest"), "responses": { "200": response("ExternalPlatform"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/admin/external-platforms/{platform_id}": {
                "patch": { "summary": "Update external platform", "parameters": [id("platform_id")], "requestBody": body("UpdateExternalPlatformRequest"), "responses": { "200": response("ExternalPlatform"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/admin/external-platforms/{platform_id}/authentication-channels": {
                "get": { "summary": "List authentication channels", "parameters": [id("platform_id")], "responses": { "200": list_response("AuthenticationChannel"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "post": { "summary": "Create authentication channel", "parameters": [id("platform_id")], "requestBody": body("CreateAuthenticationChannelRequest"), "responses": { "200": response("AuthenticationChannel"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/admin/authentication-channels/{channel_id}": {
                "patch": { "summary": "Update authentication channel", "parameters": [id("channel_id")], "requestBody": body("UpdateAuthenticationChannelRequest"), "responses": { "200": response("AuthenticationChannel"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/auth/api-keys": {
                "get": { "summary": "List API keys", "parameters": [
                    { "name": "page", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "default": 1 } },
                    { "name": "page_size", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 } }
                ], "responses": { "200": response("ApiKeyListResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create API key", "requestBody": body("CreateApiKeyRequest"), "responses": { "200": response("ApiKeyToken"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } }
            },
            "/api/auth/api-keys/{api_key_id}": {
                "delete": { "summary": "Delete API key", "parameters": [id("api_key_id")], "responses": { "204": no_content(), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/auth/api-keys/{api_key_id}/renew": {
                "post": { "summary": "Extend API key expiration without rotating its token", "parameters": [id("api_key_id")], "requestBody": body("RenewApiKeyRequest"), "responses": { "200": response("ApiKey"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/model-connections": {
                "get": { "summary": "List Global and owned Personal Model Connections", "responses": { "200": list_response("ModelConnection"), "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create a Model Connection", "requestBody": body("CreateModelConnectionRequest"), "responses": { "200": response("ModelConnection"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/model-connections/options": {
                "get": { "summary": "List Model Connection options", "responses": { "200": response("ModelConnectionOptions"), "401": { "$ref": "#/components/responses/Unauthorized" } } }
            },
            "/api/model-connections/{model_connection_id}": {
                "get": { "summary": "Get a visible Model Connection", "parameters": [id("model_connection_id")], "responses": { "200": response("ModelConnection"), "404": { "$ref": "#/components/responses/NotFound" } } },
                "put": { "summary": "Update a Model Connection", "parameters": [id("model_connection_id")], "requestBody": body("UpdateModelConnectionRequest"), "responses": { "200": response("ModelConnection"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } },
                "delete": { "summary": "Delete an unreferenced Model Connection", "parameters": [id("model_connection_id")], "responses": { "204": no_content(), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/model-connections/{model_connection_id}/status": {
                "put": { "summary": "Enable or disable a Model Connection", "parameters": [id("model_connection_id")], "requestBody": body("UpdateModelConnectionStatusRequest"), "responses": { "200": response("ModelConnection"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/model-connections/{model_connection_id}/test": {
                "post": { "summary": "Send a test message through a Model Connection", "parameters": [id("model_connection_id")], "requestBody": body("TestModelConnectionRequest"), "responses": { "200": response("ModelConnectionTestResult"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/model-connections/{model_connection_id}/force-delete": {
                "post": { "summary": "Force delete a Model Connection and clear live references", "parameters": [id("model_connection_id")], "responses": { "204": no_content(), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/model-connections/system-default": {
                "get": { "summary": "Get the System Default Model Selection", "responses": { "200": response("SystemDefaultModelSelection") } },
                "put": { "summary": "Set the enabled Global System Default Model Selection", "requestBody": body("SetSystemDefaultModelSelectionRequest"), "responses": { "200": response("SystemDefaultModelSelection"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/model-usage/summary": {
                "get": { "summary": "Summarize visible model token usage over the full requested range", "parameters": model_ledger_parameters(), "responses": { "200": response("ModelUsageSummary"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/model-usage": {
                "get": { "summary": "List visible model token usage by descending keyset", "parameters": model_ledger_parameters(), "responses": { "200": response("ModelTokenUsagePage"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/model-call-errors": {
                "get": { "summary": "List visible sanitized model call errors by descending keyset", "parameters": model_ledger_parameters(), "responses": { "200": response("ModelCallErrorPage"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/agents": {
                "get": { "summary": "List visible agents", "responses": { "200": list_response("Agent"), "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create agent", "requestBody": body("CreateAgentRequest"), "responses": { "200": response("Agent"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } }
            },
            "/api/agents/{agent_id}": {
                "get": { "summary": "Get agent and effective permissions", "parameters": [id("agent_id")], "responses": { "200": response("Agent"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "patch": { "summary": "Update agent, permissions, skills, and runtime policy", "parameters": [id("agent_id")], "requestBody": body("UpdateAgentRequest"), "responses": { "200": response("Agent"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "delete": { "summary": "Permanently delete agent and retain read-only Session history", "parameters": [id("agent_id")], "responses": { "204": no_content(), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Agent deletion committed but Bundle object cleanup must be retried" }, "503": { "description": "Agent deletion committed but Bundle storage is unavailable" } } }
            },
            "/api/agents/{agent_id}/model-options": {
                "get": { "summary": "List Model Connections assignable to an Agent by its owner scope", "parameters": [id("agent_id")], "responses": { "200": response("ModelConnectionOptions"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/integration-app-options": { "get": { "summary": "List enabled trusted Integration App setup options", "responses": { "200": response("IntegrationAppOptions"), "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/integration-apps": {
                "get": { "summary": "List Integration Apps", "responses": { "200": list_response("IntegrationApp"), "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create Integration App with one-time secret", "requestBody": body("CreateIntegrationAppRequest"), "responses": { "200": response("IntegrationAppSecretResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/integration-apps/{app_id}": {
                "get": { "summary": "Get Integration App", "parameters": [id("app_id")], "responses": { "200": response("IntegrationApp"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "patch": { "summary": "Update Integration App", "parameters": [id("app_id")], "requestBody": body("UpdateIntegrationAppRequest"), "responses": { "200": response("IntegrationApp"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/integration-apps/{app_id}/rotate-secret": { "post": { "summary": "Rotate Integration App secret once", "parameters": [id("app_id")], "responses": { "200": response("IntegrationAppSecretResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/integration-apps/{app_id}/agents/{agent_id}/widget-session": { "post": { "summary": "Issue a one-hour Widget session for a delegated Agent", "security": [{ "sessionCookie": [] }], "parameters": [id("app_id"), id("agent_id")], "responses": { "200": response("TokenResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/agents/{agent_id}/runs": {
                "get": { "summary": "List agent run history", "parameters": [id("agent_id")], "responses": { "200": list_response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "post": { "summary": "Create agent run", "parameters": [id("agent_id")], "requestBody": body("CreateRunRequest"), "responses": { "200": response("Run"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/sessions": { "get": { "summary": "List owned sessions", "responses": { "200": list_response("HubSession"), "401": { "$ref": "#/components/responses/Unauthorized" } } }, "post": { "summary": "Create an empty draft session for an agent", "requestBody": body("CreateDraftSessionRequest"), "responses": { "200": response("HubSession"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/sessions/{session_id}": { "get": { "summary": "Get owned session", "parameters": [id("session_id")], "responses": { "200": response("HubSession"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }, "delete": { "summary": "Delete an owned session", "parameters": [id("session_id")], "responses": { "204": { "description": "Deleted" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/sessions/{session_id}/title": { "put": { "summary": "Rename an owned session", "parameters": [id("session_id")], "requestBody": body("UpdateHubSessionTitleRequest"), "responses": { "200": response("HubSession"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/sessions/{session_id}/messages": {
                "get": { "summary": "List owned session messages", "parameters": [id("session_id"), { "name": "before_sequence", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1 } }, { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1, "maximum": 100 } }], "responses": { "200": list_response("HubSessionMessage"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "post": { "summary": "Send an owned session message", "parameters": [id("session_id")], "requestBody": body("CreateHubSessionMessageRequest"), "responses": { "200": response("SessionMessageAcceptance"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } }
            },
            "/api/attachments": { "post": { "summary": "Upload one attachment to an owned session", "parameters": [{ "name": "session_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } }], "requestBody": { "required": true, "content": { "multipart/form-data": { "schema": { "type": "object", "required": ["file"], "properties": { "file": { "type": "string", "format": "binary" }, "session_id": { "type": "string", "format": "uuid" } } } } } }, "responses": { "200": response("HubSessionAttachment"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "413": { "description": "Attachment exceeds the 100MB upload limit" } } } },
            "/api/attachments/{attachment_id}": { "get": { "summary": "Download an owned attachment", "parameters": [id("attachment_id")], "responses": { "200": { "description": "Attachment bytes", "content": { "application/octet-stream": { "schema": { "type": "string", "format": "binary" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/runs/{run_id}": { "get": { "summary": "Get run", "parameters": [id("run_id")], "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/runs/{run_id}/stop": { "post": { "summary": "Stop an active Turn in an owned Session", "parameters": [id("run_id")], "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runs/{run_id}/events": { "get": { "summary": "List run events", "parameters": [id("run_id")], "responses": { "200": list_response("RunEvent"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/runs/{run_id}/events/stream": { "get": { "summary": "Stream run events", "parameters": [id("run_id")], "responses": { "200": { "description": "Server-sent event stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/skills": {
                "get": { "summary": "List skills", "responses": { "200": list_response("Skill"), "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create skill", "requestBody": body("SkillWriteRequest"), "responses": { "200": response("Skill"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "delete": { "summary": "Bulk delete skills and detach them from Agents", "requestBody": body("BulkDeleteSkillsRequest"), "responses": { "200": response("BulkDeleteSkillsResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/skills/{skill_id}": {
                "get": { "summary": "Get skill", "parameters": [id("skill_id")], "responses": { "200": response("Skill"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "patch": { "summary": "Update skill", "parameters": [id("skill_id")], "requestBody": body("SkillWriteRequest"), "responses": { "200": response("Skill"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "delete": { "summary": "Permanently delete skill and detach it from agents", "parameters": [id("skill_id")], "responses": { "204": no_content(), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/skills/{skill_id}/package": {
                "put": { "summary": "Replace a Skill package from an ordered multipart file manifest", "parameters": [id("skill_id")], "requestBody": { "required": true, "content": { "multipart/form-data": { "schema": { "type": "object", "required": ["manifest"], "properties": { "manifest": { "type": "string", "description": "JSON object with an ordered paths array; following fields are file-0, file-1, and so on." } }, "additionalProperties": { "type": "string", "format": "binary" } } } } }, "responses": { "200": response("Skill"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Skill package object upload failed" }, "503": { "description": "Skill package object storage is not configured" } } },
                "delete": { "summary": "Remove the current Skill package files while retaining SKILL.md content", "parameters": [id("skill_id")], "responses": { "200": response("Skill"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/automations": {
                "get": { "summary": "List automations", "responses": { "200": list_response("Automation"), "401": { "$ref": "#/components/responses/Unauthorized" } } },
                "post": { "summary": "Create automation", "requestBody": body("CreateAutomationRequest"), "responses": { "200": response("Automation"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } }
            },
            "/api/automations/{automation_id}": {
                "patch": { "summary": "Update automation", "parameters": [id("automation_id")], "requestBody": body("UpdateAutomationRequest"), "responses": { "200": response("Automation"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } }
            },
            "/api/automations/{automation_id}/trigger": { "post": { "summary": "Trigger automation", "parameters": [id("automation_id")], "requestBody": body("TriggerAutomationRequest"), "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/automations/{automation_id}/runs": { "get": { "summary": "List automation run history", "parameters": [id("automation_id"),
                { "name": "page", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "default": 1 } },
                { "name": "page_size", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 } }
            ], "responses": { "200": response("RunListResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/automations/webhook": { "post": { "summary": "Trigger webhook automation", "security": [{ "webhookToken": [] }], "requestBody": body("TriggerAutomationRequest"), "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/runtimes": { "get": { "summary": "List runtimes", "responses": { "200": list_response("Runtime"), "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/admin/runtime-enrollment-tokens": {
                "get": { "summary": "List Runtime enrollment tokens without secret material", "responses": { "200": list_response("RuntimeEnrollmentToken"), "403": { "$ref": "#/components/responses/Forbidden" } } },
                "post": { "summary": "Create a 30-minute one-time Runtime enrollment token", "responses": { "200": response("RuntimeEnrollmentTokenCreated"), "403": { "$ref": "#/components/responses/Forbidden" } } }
            },
            "/api/admin/runtime-enrollment-tokens/{enrollment_id}/revoke": { "post": { "summary": "Revoke an unused Runtime enrollment token", "parameters": [id("enrollment_id")], "responses": { "200": response("RuntimeEnrollmentToken"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/admin/runtimes/{runtime_id}/credential-rotation": { "post": { "summary": "Request Runtime-completed credential rotation", "parameters": [id("runtime_id")], "responses": { "200": response("Runtime"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/admin/runtimes/{runtime_id}/drain": { "post": { "summary": "Drain a Runtime after exact hostname confirmation", "parameters": [id("runtime_id")], "requestBody": body("ConfirmRuntimeHostnameRequest"), "responses": { "200": response("RuntimeDrainResponse"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/admin/runtimes/{runtime_id}/cancel-drain": { "post": { "summary": "Cancel Runtime drain without reacquiring released Sessions", "parameters": [id("runtime_id")], "responses": { "200": response("RuntimeDrainResponse"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/admin/runtimes/{runtime_id}": { "delete": { "summary": "Delete a fully drained Runtime after exact hostname confirmation", "parameters": [id("runtime_id")], "requestBody": body("ConfirmRuntimeHostnameRequest"), "responses": { "204": no_content(), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/admin/runtimes/{runtime_id}/deletion-impact": { "get": { "summary": "Preview the current force-delete disposition of every owned Session", "parameters": [id("runtime_id")], "responses": { "200": response("RuntimeDeletionImpact"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/admin/runtimes/{runtime_id}/force-delete": { "post": { "summary": "Force delete a Runtime and invalidate owned Session generations", "parameters": [id("runtime_id")], "requestBody": body("ConfirmRuntimeHostnameRequest"), "responses": { "200": response("ForceDeleteRuntimeResponse"), "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/register": { "post": { "summary": "Consume a one-time enrollment token and create an immutable Runtime identity", "security": [{ "runtimeEnrollmentBearer": [] }], "requestBody": body("RuntimeRegisterRequest"), "responses": { "200": response("RuntimeRegisterResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/runtime/heartbeat": { "post": { "summary": "Heartbeat and complete staged Runtime credential rotation", "security": [{ "runtimeBearer": [] }], "requestBody": body("RuntimeHeartbeatRequest"), "responses": { "200": response("RuntimeHeartbeatResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/runs/claim": { "post": { "summary": "Claim one capacity-fenced Run and its exclusive Session ownership generation", "security": [{ "runtimeBearer": [] }], "requestBody": body("RuntimeClaimRunRequest"), "responses": { "200": response("ClaimRunResponse"), "204": no_content(), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/runtime/runs/{run_id}/skills/{skill_id}/package": { "get": { "summary": "Stream an active Run's snapshotted Skill package to its owning Runtime", "security": [{ "runtimeBearer": [] }], "parameters": [id("run_id"), id("skill_id"), required_header("x-agent-hub-ownership-generation", json!({ "type": "integer", "minimum": 1 }))], "responses": { "200": { "description": "Skill package tar.zst stream", "content": { "application/zstd": { "schema": { "type": "string", "format": "binary" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Skill package object download failed" }, "503": { "description": "Skill package object storage is not configured" } } } },
            "/api/runtime/sessions/{session_id}/skills/{skill_id}/packages/{package_id}": { "get": { "summary": "Stream a current Skill package while refreshing an owned Session", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id"), id("skill_id"), id("package_id"), required_header("x-agent-hub-ownership-generation", json!({ "type": "integer", "minimum": 1 }))], "responses": { "200": { "description": "Skill package tar.zst stream", "content": { "application/zstd": { "schema": { "type": "string", "format": "binary" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Skill package object download failed" }, "503": { "description": "Skill package object storage is not configured" } } } },
            "/api/runtime/model-proxy/v1/responses": { "post": { "summary": "Proxy one run-scoped Responses API request through its selected Model Connection", "security": [{ "modelProxyBearer": [] }], "parameters": [required_header("x-agent-hub-run-id", json!({ "type": "string", "format": "uuid" })), required_header("x-agent-hub-model-binding-id", json!({ "type": "string", "format": "uuid" }))], "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "additionalProperties": true } } } }, "responses": { "200": { "description": "Responses JSON or SSE from the selected upstream protocol" }, "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Model upstream transport failed" }, "504": { "description": "Model upstream timed out" } } } },
            "/api/runtime/runs/{run_id}/turn/begin": { "post": { "summary": "Bind synchronized configuration and begin generation-fenced Turn delivery", "security": [{ "runtimeBearer": [] }], "parameters": [id("run_id")], "requestBody": body("RuntimeBeginTurnRequest"), "responses": { "200": response("BeginRuntimeTurnResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete": { "post": { "summary": "Acknowledge one generation-fenced Session command outcome", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id"), id("command_id")], "requestBody": body("RuntimeCompleteSessionCommandRequest"), "responses": { "200": response("CompleteRuntimeSessionCommandResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/sessions/{session_id}/release": { "post": { "summary": "Release Session ownership after a current Hub-committed Bundle", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id")], "requestBody": body("ReleaseRuntimeSessionRequest"), "responses": { "200": response("HubSession"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/sessions/{session_id}/checkpoint/begin": { "post": { "summary": "Freeze one generation-fenced Session checkpoint attempt", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id")], "requestBody": body("BeginRuntimeSessionCheckpointRequest"), "responses": { "200": response("RuntimeSessionCheckpointAttempt"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/sessions/{session_id}/checkpoint/fail": { "post": { "summary": "Report a generation-fenced Session checkpoint failure", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id")], "requestBody": body("FailRuntimeSessionCheckpointRequest"), "responses": { "200": response("RuntimeSessionCheckpointDisposition"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/sessions/{session_id}/bundle": {
                "put": {
                    "summary": "Stream and commit one generation-fenced Session Bundle",
                    "security": [{ "runtimeBearer": [] }],
                    "parameters": [
                        id("session_id"),
                        required_header("x-agent-hub-ownership-generation", json!({ "type": "integer", "minimum": 1 })),
                        required_header("x-agent-hub-checkpoint-attempt-id", json!({ "type": "string", "format": "uuid" })),
                        required_header("x-agent-hub-bundle-generation", json!({ "type": "integer", "minimum": 1 })),
                        required_header("x-agent-hub-bundle-sha256", json!({ "type": "string", "pattern": "^[0-9a-f]{64}$" })),
                        required_header("x-agent-hub-bundle-size", json!({ "type": "integer", "minimum": 0 })),
                        required_header("x-agent-hub-history-checkpoint", json!({ "type": "integer", "minimum": 0 })),
                        required_header("x-agent-hub-producing-engine-version", json!({ "type": "string" })),
                        required_header("x-agent-hub-bundle-created-at", json!({ "type": "string", "format": "date-time" }))
                    ],
                    "requestBody": { "required": true, "content": { "application/zstd": { "schema": { "type": "string", "format": "binary" } } } },
                    "responses": { "200": response("RuntimeSessionBundleCommitResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "502": { "description": "Object storage transfer failed" }, "503": { "description": "Object storage is not configured" } }
                },
                "get": {
                    "summary": "Stream the current Session Bundle to its restoring Runtime",
                    "security": [{ "runtimeBearer": [] }],
                    "parameters": [id("session_id"), required_header("x-agent-hub-ownership-generation", json!({ "type": "integer", "minimum": 1 }))],
                    "responses": { "200": { "description": "Session Bundle stream", "content": { "application/zstd": { "schema": { "type": "string", "format": "binary" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "502": { "description": "Object storage transfer failed" }, "503": { "description": "Object storage is not configured" } }
                }
            },
            "/api/runtime/sessions/{session_id}/salvage-bundle": {
                "put": {
                    "summary": "Upload a salvaged Session Bundle after a Runtime crash",
                    "security": [{ "runtimeBearer": [] }],
                    "parameters": [
                        id("session_id"),
                        required_header("x-agent-hub-ownership-generation", json!({ "type": "integer", "minimum": 1 })),
                        required_header("x-agent-hub-checkpoint-attempt-id", json!({ "type": "string", "format": "uuid" })),
                        required_header("x-agent-hub-bundle-generation", json!({ "type": "integer", "minimum": 1 })),
                        required_header("x-agent-hub-bundle-sha256", json!({ "type": "string", "pattern": "^[0-9a-f]{64}$" })),
                        required_header("x-agent-hub-bundle-size", json!({ "type": "integer", "minimum": 0 })),
                        required_header("x-agent-hub-history-checkpoint", json!({ "type": "integer", "minimum": 0 })),
                        required_header("x-agent-hub-producing-engine-version", json!({ "type": "string" })),
                        required_header("x-agent-hub-bundle-created-at", json!({ "type": "string", "format": "date-time" }))
                    ],
                    "requestBody": { "required": true, "content": { "application/zstd": { "schema": { "type": "string", "format": "binary" } } } },
                    "responses": { "200": response("RuntimeSessionBundleCommitResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" }, "502": { "description": "Object storage transfer failed" }, "503": { "description": "Object storage is not configured" } }
                }
            },
            "/api/runtime/sessions/{session_id}/salvage-abandon": { "post": { "summary": "Abandon a Session salvage obligation", "security": [{ "runtimeBearer": [] }], "parameters": [id("session_id")], "requestBody": body("AbandonRuntimeSalvageRequest"), "responses": { "204": no_content(), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/runtime/runs/{run_id}/events": { "post": { "summary": "Append a generation-fenced Runtime event", "security": [{ "runtimeBearer": [] }], "parameters": [id("run_id")], "requestBody": body("RuntimeAppendRunEventRequest"), "responses": { "200": response("RunEvent"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/runtime/runs/{run_id}/tool-requests/finalize": { "post": { "summary": "Atomically finalize generation-fenced Runtime tool requests", "security": [{ "runtimeBearer": [] }], "parameters": [id("run_id")], "requestBody": body("RuntimeFinalizeToolRequestsRequest"), "responses": { "200": response("Run"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/runtime/runs/{run_id}/complete": { "post": { "summary": "Complete a generation-fenced Runtime Run", "security": [{ "runtimeBearer": [] }], "parameters": [id("run_id")], "requestBody": body("RuntimeCompleteRunRequest"), "responses": { "200": response("Run"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/embed/sessions": { "post": { "summary": "Create widget embed session", "requestBody": body("CreateEmbedSessionRequest"), "responses": { "200": response("TokenResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/embed/exchange": { "post": { "summary": "Exchange embed JWT", "security": [], "requestBody": body("EmbedExchangeRequest"), "responses": { "200": response("TokenResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/client/access": { "post": { "summary": "Issue or replace one authenticated Client Access Credential", "description": "Trusted backends authenticate with Basic credentials. Origin is optional; when present and the App has an Origin allowlist it must match one exact scheme://host[:port] entry.", "security": [{ "integrationClientBasic": [] }], "parameters": [{ "name": "Origin", "in": "header", "required": false, "description": "Optional exact browser Origin. A missing Origin is accepted for trusted backend calls.", "schema": { "type": "string", "format": "uri" } }], "requestBody": body("CreateClientAccessRequest"), "responses": { "200": response("ClientAccessResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/client/anonymous/access": { "post": { "summary": "Issue or replace one anonymous Client Access Credential", "description": "Anonymous access always requires an exact HTTP(S) Origin configured on the App; wildcard, path, query, and fragment values are rejected.", "security": [], "parameters": [{ "name": "Origin", "in": "header", "required": true, "description": "Exact browser Origin matching the anonymous App allowlist.", "schema": { "type": "string", "format": "uri" } }], "requestBody": body("CreateAnonymousClientAccessRequest"), "responses": { "200": response("ClientAccessResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/client/renew": { "post": { "summary": "Rotate one Client Access Credential without changing its grant", "security": [{ "clientAccessBearer": [] }], "requestBody": body("RenewClientAccessRequest"), "responses": { "200": response("ClientAccessResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/client/session": { "get": { "summary": "Get Client Access metadata and Agent", "security": [{ "clientAccessBearer": [] }], "responses": { "200": response("ClientSessionMetadata"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/client/sessions": { "get": { "summary": "List history-enabled Sessions in this Client Access scope", "security": [{ "clientAccessBearer": [] }], "responses": { "200": list_response("ClientSessionSummary"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/client/sessions/{session_id}/messages": { "get": { "summary": "Page messages from one exact Client Session", "security": [{ "clientAccessBearer": [] }], "parameters": [id("session_id"), { "name": "before_sequence", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1 } }, { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1, "maximum": 100 } }], "responses": { "200": list_response("HubSessionMessage"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/client/sessions/{session_id}/events": { "get": { "summary": "List typed events from one exact Client Session", "security": [{ "clientAccessBearer": [] }], "parameters": [id("session_id")], "responses": { "200": list_response("ClientSessionEvent"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/client/sessions/{session_id}/events/stream": { "get": { "summary": "Resume the typed event stream for one exact Client Session", "security": [{ "clientAccessBearer": [] }], "parameters": [id("session_id"), { "name": "after", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 0 } }], "responses": { "200": { "description": "SSE frames whose data is a ClientSessionEvent JSON object", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/client/runs": { "post": { "summary": "Send a message, creating a Session only for the first accepted message", "security": [{ "clientAccessBearer": [] }], "requestBody": body("CreateClientRunRequest"), "responses": { "200": response("Run"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/client/runs/{run_id}/stop": { "post": { "summary": "Stop an active Run in this Client Access scope", "security": [{ "clientAccessBearer": [] }], "parameters": [id("run_id")], "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/client/tool-calls/{tool_call_id}/claim": { "post": { "summary": "Atomically claim one Run-bound Client Tool call", "security": [{ "clientAccessBearer": [] }], "parameters": [id("tool_call_id")], "responses": { "200": response("ClientToolClaimResponse"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" }, "410": { "$ref": "#/components/responses/Gone" } } } },
            "/api/client/tool-calls/{tool_call_id}/result": { "post": { "summary": "Submit one structured Client Tool result", "security": [{ "clientAccessBearer": [] }], "parameters": [id("tool_call_id")], "requestBody": body("SubmitClientToolResultRequest"), "responses": { "200": response("SubmitClientToolResultResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "description": "A different result was already accepted for this tool_call_id" }, "410": { "$ref": "#/components/responses/Gone" } } } },
            "/api/widget/access": { "post": { "summary": "Issue a short-lived Widget credential for one trusted external user", "deprecated": true, "security": [{ "integrationClientBasic": [] }], "requestBody": body("CreateWidgetAccessRequest"), "responses": { "200": response("WidgetAccessResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/widget/public/access": { "post": { "summary": "Issue or renew an anonymous public Widget credential", "deprecated": true, "security": [], "requestBody": body("CreatePublicWidgetAccessRequest"), "responses": { "200": response("PublicWidgetAccessResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/widget/session": { "get": { "summary": "Get Widget credential metadata and Agent", "deprecated": true, "security": [{ "embedToken": [] }], "responses": { "200": response("WidgetSession"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/widget/session/renew": { "post": { "summary": "Rotate one external Widget credential in place", "deprecated": true, "security": [{ "embedToken": [] }], "requestBody": body("RenewWidgetSessionRequest"), "responses": { "200": response("WidgetTokenResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/widget/sessions": { "get": { "summary": "List history enabled Sessions in the exact external Widget scope", "deprecated": true, "security": [{ "embedToken": [] }], "responses": { "200": list_response("WidgetHistorySession"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/widget/sessions/{session_id}/messages": { "get": { "summary": "List one exact external Widget Session's messages", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [id("session_id"), { "name": "before_sequence", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1 } }, { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "format": "int64", "minimum": 1, "maximum": 100 } }], "responses": { "200": list_response("HubSessionMessage"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/widget/sessions/{session_id}/events": { "get": { "summary": "List one exact external Widget Session's Run events", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [id("session_id")], "responses": { "200": list_response("RunEvent"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/widget/sessions/{session_id}/events/stream": { "get": { "summary": "Stream one exact external Widget Session's Run events", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [id("session_id")], "responses": { "200": { "description": "Server-sent event stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/widget/runs": { "post": { "summary": "Create widget run", "deprecated": true, "security": [{ "embedToken": [] }], "requestBody": body("CreateWidgetRunRequest"), "responses": { "200": response("Run"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/widget/runs/{run_id}/stop": { "post": { "summary": "Stop the active Turn associated with this widget token", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [id("run_id")], "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/widget/attachments": { "post": { "summary": "Upload one attachment to the embed session", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [{ "name": "session_id", "in": "query", "required": false, "schema": { "type": "string", "format": "uuid" } }], "requestBody": { "required": true, "content": { "multipart/form-data": { "schema": { "type": "object", "required": ["file"], "properties": { "file": { "type": "string", "format": "binary" }, "session_id": { "type": "string", "format": "uuid" } } } } } }, "responses": { "200": response("HubSessionAttachment"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "413": { "description": "Attachment exceeds the 100MB upload limit" } } } },
            "/api/widget/attachments/{attachment_id}": { "get": { "summary": "Download an embed session attachment", "deprecated": true, "security": [{ "embedToken": [] }], "parameters": [id("attachment_id")], "responses": { "200": { "description": "Attachment bytes", "content": { "application/octet-stream": { "schema": { "type": "string", "format": "binary" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/oauth/authorize": { "get": { "summary": "Authorize Integration App for an existing external identity", "security": [{ "sessionCookie": [] }], "parameters": [{ "name": "client_id", "in": "query", "required": true, "schema": { "type": "string" } }, { "name": "redirect_uri", "in": "query", "required": true, "schema": { "type": "string", "format": "uri" } }, { "name": "state", "in": "query", "required": false, "schema": { "type": "string" } }, { "name": "scope", "in": "query", "required": false, "schema": { "type": "string" } }, { "name": "external_user_id", "in": "query", "required": true, "schema": { "type": "string" } }, { "name": "tenant_id", "in": "query", "required": true, "schema": { "type": "string" } }], "responses": { "303": { "description": "Redirect with authorization code" }, "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/oauth/token": { "post": { "summary": "Issue authorization_code or client_credentials access token", "security": [], "requestBody": { "required": true, "content": { "application/x-www-form-urlencoded": { "schema": { "$ref": "#/components/schemas/OAuthTokenRequest" } } } }, "responses": { "200": response("OAuthTokenResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/oauth/userinfo": { "get": { "summary": "Get scoped OAuth user information", "security": [{ "integrationBearer": [] }], "responses": { "200": response("OAuthUserInfo"), "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/integrations/embed-session": { "post": { "summary": "Create integration-scoped embed session", "requestBody": body("CreateEmbedSessionRequest"), "responses": { "200": response("TokenResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "403": { "$ref": "#/components/responses/Forbidden" } } } },
            "/api/integrations/sessions": { "post": { "summary": "Create integration session", "requestBody": body("CreateIntegrationSessionRequest"), "responses": { "200": response("IntegrationSession"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/integrations/sessions/{session_id}": { "get": { "summary": "Get integration session", "parameters": [id("session_id")], "responses": { "200": response("IntegrationSession"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } },
            "/api/integrations/sessions/{session_id}/messages": {
                "get": { "summary": "List integration messages", "parameters": [id("session_id")], "responses": { "200": list_response("HubSessionMessage"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } },
                "post": { "summary": "Send integration message", "parameters": [id("session_id")], "requestBody": body("IntegrationMessageRequest"), "responses": { "200": response("IntegrationMessageResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" } } }
            },
            "/api/integrations/sessions/{session_id}/runs/{run_id}/stop": { "post": { "summary": "Stop the active Turn in this integration Session", "parameters": [id("session_id"), id("run_id")], "responses": { "200": response("Run"), "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" }, "409": { "$ref": "#/components/responses/Conflict" } } } },
            "/api/integrations/sessions/{session_id}/events": { "get": { "summary": "List integration events", "parameters": [id("session_id")], "responses": { "200": list_response("RunEvent"), "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/integrations/sessions/{session_id}/events/stream": { "get": { "summary": "Stream integration events", "parameters": [id("session_id")], "responses": { "200": { "description": "Server-sent event stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "401": { "$ref": "#/components/responses/Unauthorized" } } } },
            "/api/integrations/tool-requests/{tool_request_id}/result": { "post": { "summary": "Submit integration tool result", "parameters": [id("tool_request_id")], "requestBody": body("ToolResultRequest"), "responses": { "200": response("ToolResultResponse"), "400": { "$ref": "#/components/responses/BadRequest" }, "401": { "$ref": "#/components/responses/Unauthorized" }, "404": { "$ref": "#/components/responses/NotFound" } } } }
        },
        "components": {
            "securitySchemes": {
                "userBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "Agent Hub API key (ahk_)" },
                "integrationBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "OAuth access token (aho_)" },
                "runtimeEnrollmentBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "One-time Runtime enrollment token (ahre_)" },
                "runtimeBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "Per-Runtime credential (ahrc_)" },
                "modelProxyBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "Run-scoped model proxy token (ahr_)" },
                "clientAccessBearer": { "type": "http", "scheme": "bearer", "bearerFormat": "Opaque Client Access Credential (ahw_ or ahp_)" },
                "integrationClientBasic": { "type": "http", "scheme": "basic", "description": "Integration App client_id and client_secret." },
                "sessionCookie": { "type": "apiKey", "in": "cookie", "name": "agent_hub_session", "description": "HttpOnly browser session cookie issued by password or LDAP login." },
                "embedToken": { "type": "apiKey", "in": "header", "name": "X-Agent-Hub-Embed-Token" },
                "webhookToken": { "type": "apiKey", "in": "header", "name": "X-Agent-Hub-Webhook-Token" }
            },
            "responses": {
                "BadRequest": { "description": "Invalid request", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
                "Unauthorized": { "description": "Missing or invalid credential", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
                "Forbidden": { "description": "Insufficient permission", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
                "Conflict": { "description": "Resource already exists", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
                "NotFound": { "description": "Resource not found", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } },
                "Gone": { "description": "Resource reached a terminal state", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
                ,"TooManyRequests": { "description": "Login rate limit exceeded", "headers": { "Retry-After": { "schema": { "type": "integer", "minimum": 1 } } }, "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
                ,"ServiceUnavailable": { "description": "Authentication dependency is temporarily unavailable", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" } } } }
            },
            "schemas": openapi_schemas()
        }
    });
    apply_openapi_security(&mut document);
    document
}

pub(crate) fn apply_openapi_security(document: &mut Value) {
    let user = json!([{ "sessionCookie": [] }, { "userBearer": [] }]);
    let integration = json!([{ "integrationBearer": [] }]);
    let paths = document["paths"].as_object_mut().expect("OpenAPI paths");
    for (path, methods) in paths {
        for operation in methods
            .as_object_mut()
            .expect("OpenAPI operations")
            .values_mut()
        {
            if operation.get("security").is_some() {
                continue;
            }
            operation["security"] = if path.starts_with("/api/integrations/") {
                integration.clone()
            } else {
                user.clone()
            };
        }
    }
}

pub(crate) fn openapi_schemas() -> Value {
    let uuid = || json!({ "type": "string", "format": "uuid" });
    let date = || json!({ "type": ["string", "null"], "format": "date-time" });
    json!({
        "Error": { "type": "object", "required": ["error"], "properties": { "error": { "type": "string" } } },
        "User": { "type": "object", "required": ["id", "email", "display_name", "role"], "properties": { "id": uuid(), "email": { "type": "string", "format": "email" }, "display_name": { "type": "string" }, "role": { "type": "string" } } },
        "AdminUserDetail": { "type": "object", "additionalProperties": false, "required": ["user", "has_password", "created_at"], "properties": { "user": { "$ref": "#/components/schemas/User" }, "has_password": { "type": "boolean" }, "created_at": { "type": "string", "format": "date-time" } } },
        "AdminCreateUserRequest": { "type": "object", "additionalProperties": false, "required": ["email", "role"], "properties": { "email": { "type": "string", "format": "email" }, "display_name": { "type": ["string", "null"] }, "password": { "type": ["string", "null"], "format": "password", "minLength": 8, "maxLength": 1024 }, "role": { "type": "string", "enum": ["member", "admin", "super_admin"] } } },
        "AdminUpdateUserRequest": { "type": "object", "additionalProperties": false, "required": ["email", "display_name"], "properties": { "email": { "type": "string", "format": "email" }, "display_name": { "type": "string" } } },
        "UpdateCurrentUserRequest": { "type": "object", "additionalProperties": false, "required": ["display_name"], "properties": { "display_name": { "type": "string" } } },
        "AdminSetUserPasswordRequest": { "type": "object", "additionalProperties": false, "required": ["password"], "properties": { "password": { "type": "string", "format": "password", "minLength": 8, "maxLength": 1024 } } },
        "AdminSetUserRoleRequest": { "type": "object", "additionalProperties": false, "required": ["role"], "properties": { "role": { "type": "string", "enum": ["member", "admin", "super_admin"] } } },
        "LoginRequest": { "type": "object", "required": ["email", "password"], "properties": { "email": { "type": "string", "format": "email" }, "password": { "type": "string", "format": "password" } } },
        "LoginResponse": { "type": "object", "required": ["user"], "properties": { "user": { "$ref": "#/components/schemas/User" } } },
        "PasswordRegistrationRequest": { "type": "object", "additionalProperties": false, "required": ["email", "password"], "properties": { "email": { "type": "string", "format": "email" }, "password": { "type": "string", "format": "password" }, "display_name": { "type": ["string", "null"] } } },
        "PasswordRegistrationResponse": { "type": "object", "required": ["user"], "properties": { "user": { "$ref": "#/components/schemas/User" } } },
        "AuthProvidersResponse": { "type": "object", "required": ["password_registration_enabled", "password_login_enabled", "ldap_login_enabled"], "properties": { "password_registration_enabled": { "type": "boolean" }, "password_login_enabled": { "type": "boolean" }, "ldap_login_enabled": { "type": "boolean" } } },
        "AuthPolicy": { "type": "object", "additionalProperties": false, "required": ["password_registration_enabled", "password_login_enabled", "ldap_login_enabled", "email_placeholder", "password_placeholder"], "properties": { "password_registration_enabled": { "type": "boolean" }, "password_login_enabled": { "type": "boolean" }, "ldap_login_enabled": { "type": "boolean" }, "email_placeholder": { "type": "string" }, "password_placeholder": { "type": "string" } } },
        "SystemSettings": { "type": "object", "additionalProperties": false, "required": ["max_attachment_upload_bytes", "max_attachment_bytes_per_session"], "properties": { "max_attachment_upload_bytes": { "type": "integer", "minimum": 1048576, "maximum": 1073741824 }, "max_attachment_bytes_per_session": { "type": "integer", "minimum": 1048576, "maximum": 1.073741824e10 } } },
        "UpdateSystemSettingsRequest": { "type": "object", "additionalProperties": false, "required": ["max_attachment_upload_bytes", "max_attachment_bytes_per_session"], "properties": { "max_attachment_upload_bytes": { "type": "integer", "minimum": 1048576, "maximum": 1073741824 }, "max_attachment_bytes_per_session": { "type": "integer", "minimum": 1048576, "maximum": 1.073741824e10 } } },
        "LdapConfiguration": { "type": "object", "additionalProperties": false, "required": ["url", "security", "base_dn", "bind_identity_template", "user_filter", "email_attribute", "display_name_attribute", "allow_insecure", "skip_tls_verify"], "properties": { "url": { "type": "string", "format": "uri" }, "security": { "type": "string", "enum": ["ldaps", "starttls", "plain"] }, "base_dn": { "type": "string" }, "bind_identity_template": { "type": "string", "default": "{email}", "description": "Bind identity template containing exactly one {email}; the substituted value is escaped as an LDAP DN attribute value." }, "user_filter": { "type": "string", "default": "(userPrincipalName={email})" }, "email_attribute": { "type": "string", "default": "mail" }, "display_name_attribute": { "type": "string", "default": "displayName" }, "allow_insecure": { "type": "boolean", "default": false }, "skip_tls_verify": { "type": "boolean", "default": false } } },
        "NullableLdapConfiguration": { "anyOf": [{ "$ref": "#/components/schemas/LdapConfiguration" }, { "type": "null" }] },
        "TestLdapConfigurationRequest": { "type": "object", "additionalProperties": false, "required": ["configuration", "email", "password"], "properties": { "configuration": { "$ref": "#/components/schemas/LdapConfiguration" }, "email": { "type": "string", "format": "email" }, "password": { "type": "string", "format": "password" } } },
        "TestLdapConfigurationResponse": { "type": "object", "additionalProperties": false, "required": ["email", "display_name", "duration_ms"], "properties": { "email": { "type": "string", "format": "email" }, "display_name": { "type": "string" }, "duration_ms": { "type": "integer", "format": "int64", "minimum": 0 } } },
        "EraseUserRequest": { "type": "object", "additionalProperties": false, "required": ["email"], "properties": { "email": { "type": "string", "format": "email" } } },
        "UserErasure": { "type": "object", "additionalProperties": false, "required": ["user_id", "email", "status", "requested_at", "completed_at"], "properties": { "user_id": uuid(), "email": { "type": ["string", "null"], "format": "email" }, "status": { "type": "string", "enum": ["pending", "completed"] }, "requested_at": { "type": "string", "format": "date-time" }, "completed_at": date() } },
        "ExternalPlatform": { "type": "object", "required": ["id", "key", "name"], "properties": { "id": uuid(), "key": { "type": "string" }, "name": { "type": "string" } } },
        "IntegrationAppOptions": { "type": "object", "additionalProperties": false, "required": ["external_platforms", "authentication_channels"], "properties": { "external_platforms": { "type": "array", "items": { "$ref": "#/components/schemas/ExternalPlatform" } }, "authentication_channels": { "type": "array", "items": { "$ref": "#/components/schemas/AuthenticationChannel" } } } },
        "CreateExternalPlatformRequest": { "type": "object", "additionalProperties": false, "required": ["key", "name"], "properties": { "key": { "type": "string" }, "name": { "type": "string" } } },
        "UpdateExternalPlatformRequest": { "type": "object", "additionalProperties": false, "required": ["name"], "properties": { "name": { "type": "string" } } },
        "AuthenticationChannel": { "type": "object", "required": ["id", "platform_id", "key", "name", "enabled", "trusted_email"], "properties": { "id": uuid(), "platform_id": uuid(), "key": { "type": "string" }, "name": { "type": "string" }, "enabled": { "type": "boolean" }, "trusted_email": { "type": "boolean" } } },
        "CreateAuthenticationChannelRequest": { "type": "object", "additionalProperties": false, "required": ["key", "name", "enabled", "trusted_email"], "properties": { "key": { "type": "string" }, "name": { "type": "string" }, "enabled": { "type": "boolean" }, "trusted_email": { "type": "boolean" } } },
        "UpdateAuthenticationChannelRequest": { "type": "object", "additionalProperties": false, "required": ["name", "enabled", "trusted_email"], "properties": { "name": { "type": "string" }, "enabled": { "type": "boolean" }, "trusted_email": { "type": "boolean" } } },
        "ApiKey": { "type": "object", "required": ["id", "name", "prefix", "last_used_at", "expires_at", "created_at"], "properties": { "id": uuid(), "name": { "type": "string" }, "prefix": { "type": "string" }, "last_used_at": date(), "expires_at": date(), "created_at": { "type": "string", "format": "date-time" } } },
        "ApiKeyListResponse": { "type": "object", "required": ["items", "total", "page", "page_size"], "properties": { "items": { "type": "array", "items": { "$ref": "#/components/schemas/ApiKey" } }, "total": { "type": "integer", "minimum": 0 }, "page": { "type": "integer", "minimum": 1 }, "page_size": { "type": "integer", "minimum": 1, "maximum": 100 } } },
        "ApiKeyToken": { "type": "object", "required": ["api_key", "token"], "properties": { "api_key": { "$ref": "#/components/schemas/ApiKey" }, "token": { "type": "string", "description": "Shown once; store securely." } } },
        "ApiKeyValidity": { "oneOf": [
            { "type": "object", "additionalProperties": false, "required": ["kind", "days"], "properties": { "kind": { "type": "string", "const": "days" }, "days": { "type": "integer", "enum": [30, 90, 180, 365] } } },
            { "type": "object", "additionalProperties": false, "required": ["kind", "expires_at"], "properties": { "kind": { "type": "string", "const": "date" }, "expires_at": { "type": "string", "format": "date-time" } } },
            { "type": "object", "additionalProperties": false, "required": ["kind"], "properties": { "kind": { "type": "string", "const": "never" } } }
        ] },
        "CreateApiKeyRequest": { "type": "object", "additionalProperties": false, "required": ["name"], "properties": { "name": { "type": "string" }, "validity": { "anyOf": [{ "$ref": "#/components/schemas/ApiKeyValidity" }, { "type": "null" }] } } },
        "RenewApiKeyRequest": { "type": "object", "additionalProperties": false, "required": ["validity"], "properties": { "validity": { "$ref": "#/components/schemas/ApiKeyValidity" } } },
        "ModelConnectionScope": { "type": "string", "enum": ["global", "personal"] },
        "ModelConnectionStatus": { "type": "string", "enum": ["enabled", "disabled"] },
        "ModelUpstreamProtocol": { "type": "string", "enum": ["openai_responses", "openai_chat_completions", "anthropic_messages"], "default": "openai_responses" },
        "ModelReasoningSummary": { "type": "string", "enum": ["default", "auto", "concise", "detailed", "none"], "default": "default" },
        "ModelVerbosity": { "type": "string", "enum": ["default", "low", "medium", "high"], "default": "default" },
        "ModelReasoningSummarySupport": { "type": "string", "enum": ["auto", "supported", "unsupported"], "default": "auto" },
        "ModelRequestSettings": { "oneOf": [
            { "type": "object", "additionalProperties": false, "required": ["protocol"], "properties": { "protocol": { "type": "string", "enum": ["openai_responses"] } } },
            { "type": "object", "additionalProperties": false, "required": ["protocol"], "properties": { "protocol": { "type": "string", "enum": ["openai_chat_completions"] }, "temperature": { "type": ["number", "null"], "minimum": 0, "maximum": 2 }, "top_p": { "type": ["number", "null"], "minimum": 0, "maximum": 1 }, "max_completion_tokens": { "type": ["integer", "null"], "minimum": 1, "maximum": 4294967295_u64 } } },
            { "type": "object", "additionalProperties": false, "required": ["protocol"], "properties": { "protocol": { "type": "string", "enum": ["anthropic_messages"] }, "temperature": { "type": ["number", "null"], "minimum": 0, "maximum": 1 }, "top_p": { "type": ["number", "null"], "minimum": 0, "maximum": 1 }, "max_tokens": { "type": ["integer", "null"], "minimum": 1, "maximum": 4294967295_u64 } }, "not": { "required": ["temperature", "top_p"], "properties": { "temperature": { "type": "number" }, "top_p": { "type": "number" } } } }
        ], "discriminator": { "propertyName": "protocol" } },
        "AgentModelSettings": { "type": "object", "additionalProperties": false, "required": ["reasoning_effort", "reasoning_summary", "verbosity", "context_window_tokens", "auto_compact_token_limit", "reasoning_summary_support", "service_tier", "provider_request_timeout_ms", "stream_max_retries", "stream_idle_timeout_ms", "request_settings"], "properties": { "reasoning_effort": { "$ref": "#/components/schemas/ReasoningEffort" }, "reasoning_summary": { "$ref": "#/components/schemas/ModelReasoningSummary" }, "verbosity": { "$ref": "#/components/schemas/ModelVerbosity" }, "context_window_tokens": { "type": ["integer", "null"], "format": "int64", "minimum": 1 }, "auto_compact_token_limit": { "type": ["integer", "null"], "format": "int64", "minimum": 1 }, "reasoning_summary_support": { "$ref": "#/components/schemas/ModelReasoningSummarySupport" }, "service_tier": { "type": ["string", "null"], "minLength": 1, "maxLength": 64 }, "provider_request_timeout_ms": { "type": ["integer", "null"], "format": "int64", "minimum": 1 }, "stream_max_retries": { "type": ["integer", "null"], "minimum": 0, "maximum": 100 }, "stream_idle_timeout_ms": { "type": ["integer", "null"], "format": "int64", "minimum": 1 }, "request_settings": { "$ref": "#/components/schemas/ModelRequestSettings" } } },
        "AgentModelSettingsOverride": { "type": "object", "additionalProperties": false, "properties": { "reasoning_effort": { "anyOf": [{ "$ref": "#/components/schemas/ReasoningEffort" }, { "type": "null" }] }, "reasoning_summary": { "anyOf": [{ "$ref": "#/components/schemas/ModelReasoningSummary" }, { "type": "null" }] }, "verbosity": { "anyOf": [{ "$ref": "#/components/schemas/ModelVerbosity" }, { "type": "null" }] }, "context_window_tokens": { "type": ["integer", "null"], "minimum": 1 }, "auto_compact_token_limit": { "type": ["integer", "null"], "minimum": 1 }, "reasoning_summary_support": { "anyOf": [{ "$ref": "#/components/schemas/ModelReasoningSummarySupport" }, { "type": "null" }] }, "service_tier": { "type": ["string", "null"], "minLength": 1, "maxLength": 64 }, "provider_request_timeout_ms": { "type": ["integer", "null"], "minimum": 1 }, "stream_max_retries": { "type": ["integer", "null"], "minimum": 0, "maximum": 100 }, "stream_idle_timeout_ms": { "type": ["integer", "null"], "minimum": 1 }, "request_settings": { "anyOf": [{ "$ref": "#/components/schemas/ModelRequestSettings" }, { "type": "null" }] } } },
        "ModelSelection": { "type": "object", "additionalProperties": false, "required": ["connection_id", "model_id"], "properties": { "connection_id": uuid(), "model_id": { "type": "string", "minLength": 1, "maxLength": 255 } } },
        "RunModelBinding": { "type": "object", "additionalProperties": false, "required": ["id", "run_id", "binding_key", "model_connection_id", "connection_name_snapshot", "connection_scope_snapshot", "model_id", "api_type", "model_settings"], "properties": { "id": uuid(), "run_id": uuid(), "binding_key": { "type": "string" }, "model_connection_id": uuid(), "connection_name_snapshot": { "type": "string" }, "connection_scope_snapshot": { "$ref": "#/components/schemas/ModelConnectionScope" }, "model_id": { "type": "string" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "model_settings": { "$ref": "#/components/schemas/AgentModelSettings" } } },
        "ModelConnection": { "type": "object", "additionalProperties": false, "required": ["id", "owner_id", "scope", "name", "base_url", "api_type", "allowed_model_ids", "status", "has_api_key", "created_at", "updated_at"], "properties": { "id": uuid(), "owner_id": { "anyOf": [uuid(), { "type": "null" }] }, "owner_email": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "scope": { "$ref": "#/components/schemas/ModelConnectionScope" }, "name": { "type": "string" }, "base_url": { "type": "string", "format": "uri" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "allowed_model_ids": { "type": "array", "minItems": 1, "maxItems": 256, "items": { "type": "string", "minLength": 1, "maxLength": 255 } }, "vision_model_id": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "status": { "$ref": "#/components/schemas/ModelConnectionStatus" }, "has_api_key": { "type": "boolean" }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "CreateModelConnectionRequest": { "type": "object", "additionalProperties": false, "required": ["scope", "name", "base_url", "api_type", "allowed_model_ids", "api_key"], "properties": { "scope": { "$ref": "#/components/schemas/ModelConnectionScope" }, "name": { "type": "string", "minLength": 1, "maxLength": 128 }, "base_url": { "type": "string", "format": "uri" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "allowed_model_ids": { "type": "array", "minItems": 1, "maxItems": 256, "items": { "type": "string", "minLength": 1, "maxLength": 255 } }, "vision_model_id": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "api_key": { "type": "string", "minLength": 1, "format": "password", "writeOnly": true } } },
        "UpdateModelConnectionRequest": { "type": "object", "additionalProperties": false, "required": ["name", "base_url", "api_type", "allowed_model_ids"], "properties": { "name": { "type": "string", "minLength": 1, "maxLength": 128 }, "base_url": { "type": "string", "format": "uri" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "allowed_model_ids": { "type": "array", "minItems": 1, "maxItems": 256, "items": { "type": "string", "minLength": 1, "maxLength": 255 } }, "vision_model_id": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "api_key": { "type": ["string", "null"], "minLength": 1, "format": "password", "writeOnly": true } } },
        "UpdateModelConnectionStatusRequest": { "type": "object", "additionalProperties": false, "required": ["status"], "properties": { "status": { "$ref": "#/components/schemas/ModelConnectionStatus" } } },
        "ModelConnectionOption": { "type": "object", "additionalProperties": false, "required": ["connection_id", "connection_name", "model_id", "api_type", "scope", "status"], "properties": { "connection_id": uuid(), "connection_name": { "type": "string" }, "model_id": { "type": "string" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "scope": { "$ref": "#/components/schemas/ModelConnectionScope" }, "status": { "$ref": "#/components/schemas/ModelConnectionStatus" } } },
        "ModelConnectionOptions": { "type": "object", "additionalProperties": false, "required": ["items", "system_default"], "properties": { "items": { "type": "array", "items": { "$ref": "#/components/schemas/ModelConnectionOption" } }, "system_default": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] } } },
        "TestModelConnectionRequest": { "type": "object", "additionalProperties": false, "required": ["model_id", "message"], "properties": { "model_id": { "type": "string", "minLength": 1, "maxLength": 255 }, "message": { "type": "string", "minLength": 1, "maxLength": 4000 } } },
        "ModelConnectionTestResult": { "type": "object", "additionalProperties": false, "required": ["success", "status_code", "error_code", "message", "response_text", "response_time_ms"], "properties": { "success": { "type": "boolean" }, "status_code": { "type": ["integer", "null"], "minimum": 100, "maximum": 599 }, "error_code": { "type": ["string", "null"] }, "message": { "type": ["string", "null"] }, "response_text": { "type": ["string", "null"] }, "response_time_ms": { "type": "integer", "format": "int64", "minimum": 0 } } },
        "SystemDefaultModelSelection": { "type": "object", "additionalProperties": false, "required": ["selection"], "properties": { "selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] } } },
        "SetSystemDefaultModelSelectionRequest": { "type": "object", "additionalProperties": false, "required": ["selection"], "properties": { "selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] } } },
        "ModelConnectionSnapshot": { "type": "object", "additionalProperties": false, "required": ["id", "scope", "name", "model_id", "api_type", "request_settings"], "properties": { "id": { "anyOf": [uuid(), { "type": "null" }] }, "scope": { "$ref": "#/components/schemas/ModelConnectionScope" }, "name": { "type": "string" }, "model_id": { "type": "string" }, "api_type": { "$ref": "#/components/schemas/ModelUpstreamProtocol" }, "request_settings": { "$ref": "#/components/schemas/ModelRequestSettings" } } },
        "ModelAgentSnapshot": { "type": "object", "additionalProperties": false, "required": ["id", "name"], "properties": { "id": { "anyOf": [uuid(), { "type": "null" }] }, "name": { "type": "string" } } },
        "ModelUsageSubject": { "type": "object", "additionalProperties": false, "required": ["kind", "id", "display_name"], "properties": { "kind": { "type": "string", "enum": ["user", "integration_app", "system"] }, "id": { "anyOf": [uuid(), { "type": "null" }] }, "display_name": { "type": ["string", "null"] } } },
        "ModelTokenUsageTotals": { "type": "object", "additionalProperties": false, "required": ["input_tokens", "output_tokens", "total_tokens", "cached_tokens", "reasoning_tokens"], "properties": { "input_tokens": { "type": "integer", "minimum": 0 }, "output_tokens": { "type": "integer", "minimum": 0 }, "total_tokens": { "type": "integer", "minimum": 0 }, "cached_tokens": { "type": "integer", "minimum": 0 }, "reasoning_tokens": { "type": "integer", "minimum": 0 } } },
        "ModelTokenUsage": { "type": "object", "additionalProperties": false, "required": ["id", "occurred_at", "response_status", "model", "agent", "subject", "input_tokens", "output_tokens", "total_tokens", "cached_tokens", "reasoning_tokens"], "properties": { "id": uuid(), "occurred_at": { "type": "string", "format": "date-time" }, "response_status": { "type": "string" }, "model": { "$ref": "#/components/schemas/ModelConnectionSnapshot" }, "agent": { "$ref": "#/components/schemas/ModelAgentSnapshot" }, "subject": { "$ref": "#/components/schemas/ModelUsageSubject" }, "input_tokens": { "type": "integer", "minimum": 0 }, "output_tokens": { "type": "integer", "minimum": 0 }, "total_tokens": { "type": "integer", "minimum": 0 }, "cached_tokens": { "type": "integer", "minimum": 0 }, "reasoning_tokens": { "type": "integer", "minimum": 0 } } },
        "ModelCallError": { "type": "object", "additionalProperties": false, "required": ["id", "occurred_at", "response_status", "model", "agent", "subject", "upstream_status", "error_code", "message"], "properties": { "id": uuid(), "occurred_at": { "type": "string", "format": "date-time" }, "response_status": { "type": "string" }, "model": { "$ref": "#/components/schemas/ModelConnectionSnapshot" }, "agent": { "$ref": "#/components/schemas/ModelAgentSnapshot" }, "subject": { "$ref": "#/components/schemas/ModelUsageSubject" }, "upstream_status": { "type": ["integer", "null"], "minimum": 100, "maximum": 599 }, "error_code": { "type": ["string", "null"] }, "message": { "type": ["string", "null"] } } },
        "ModelLedgerCursor": { "type": "object", "additionalProperties": false, "required": ["occurred_at_ms", "id"], "properties": { "occurred_at_ms": { "type": "integer", "format": "int64" }, "id": uuid() } },
        "ModelUsageModelSummary": { "type": "object", "additionalProperties": false, "required": ["model", "totals"], "properties": { "model": { "$ref": "#/components/schemas/ModelConnectionSnapshot" }, "totals": { "$ref": "#/components/schemas/ModelTokenUsageTotals" } } },
        "ModelUsageAgentSummary": { "type": "object", "additionalProperties": false, "required": ["agent", "totals"], "properties": { "agent": { "$ref": "#/components/schemas/ModelAgentSnapshot" }, "totals": { "$ref": "#/components/schemas/ModelTokenUsageTotals" } } },
        "ModelUsageUserSummary": { "type": "object", "additionalProperties": false, "required": ["user_id", "display_name", "totals"], "properties": { "user_id": { "anyOf": [uuid(), { "type": "null" }] }, "display_name": { "type": ["string", "null"] }, "totals": { "$ref": "#/components/schemas/ModelTokenUsageTotals" } } },
        "ModelUsageSummary": { "type": "object", "additionalProperties": false, "required": ["overall", "by_model", "by_agent", "by_user"], "properties": { "overall": { "$ref": "#/components/schemas/ModelTokenUsageTotals" }, "by_model": { "type": "array", "items": { "$ref": "#/components/schemas/ModelUsageModelSummary" } }, "by_agent": { "type": "array", "items": { "$ref": "#/components/schemas/ModelUsageAgentSummary" } }, "by_user": { "type": "array", "items": { "$ref": "#/components/schemas/ModelUsageUserSummary" } } } },
        "ModelTokenUsagePage": { "type": "object", "additionalProperties": false, "required": ["items", "next_cursor"], "properties": { "items": { "type": "array", "items": { "$ref": "#/components/schemas/ModelTokenUsage" } }, "next_cursor": { "anyOf": [{ "$ref": "#/components/schemas/ModelLedgerCursor" }, { "type": "null" }] } } },
        "ModelCallErrorPage": { "type": "object", "additionalProperties": false, "required": ["items", "next_cursor"], "properties": { "items": { "type": "array", "items": { "$ref": "#/components/schemas/ModelCallError" } }, "next_cursor": { "anyOf": [{ "$ref": "#/components/schemas/ModelLedgerCursor" }, { "type": "null" }] } } },
        "ReasoningEffort": { "type": "string", "enum": ["default", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"] },
        "AgentToolName": { "type": "string", "enum": ["read", "grep", "find", "ls", "edit", "write", "bash", "skill_exec", "integration"] },
        "SubagentDefinition": { "type": "object", "additionalProperties": false, "required": ["name", "description", "developer_instructions"], "properties": { "name": { "type": "string", "minLength": 1, "maxLength": 64 }, "description": { "type": "string", "minLength": 1, "maxLength": 512 }, "developer_instructions": { "type": "string", "minLength": 1, "maxLength": 100000 }, "model_selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] }, "model_settings_override": { "$ref": "#/components/schemas/AgentModelSettingsOverride" }, "enabled": { "type": "boolean", "default": true }, "disabled_reason": { "type": ["string", "null"] } } },
        "AgentSecretDeclaration": { "type": "object", "additionalProperties": false, "required": ["name", "kind"], "properties": { "name": { "type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Z_][A-Z0-9_]*$" }, "kind": { "type": "string", "enum": ["value", "file"] }, "description": { "type": "string", "maxLength": 512 } } },
        "Agent": { "type": "object", "required": ["id", "owner_id", "name", "instructions", "visibility", "public_to", "runtime_id", "model_selection", "model_settings", "subagents", "model_policy", "sandbox_policy", "managed_skill_ids", "mcp_allowlist", "tool_allowlist", "is_owner", "can_manage", "can_administer", "can_invoke", "created_at", "updated_at"], "properties": { "id": uuid(), "owner_id": uuid(), "owner_email": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "name": { "type": "string" }, "instructions": { "type": "string" }, "visibility": { "type": "string", "enum": ["private", "public", "public_to"] }, "public_to": { "type": "array", "items": uuid() }, "runtime_id": { "anyOf": [uuid(), { "type": "null" }] }, "model_selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] }, "model_settings": { "$ref": "#/components/schemas/AgentModelSettings" }, "subagents": { "type": "array", "items": { "$ref": "#/components/schemas/SubagentDefinition" } }, "model_policy": {}, "sandbox_policy": {}, "managed_skill_ids": { "type": "array", "items": uuid() }, "mcp_allowlist": {}, "tool_allowlist": { "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" } }, "is_owner": { "type": "boolean" }, "can_manage": { "type": "boolean" }, "can_administer": { "type": "boolean" }, "can_invoke": { "type": "boolean" }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "CreateAgentRequest": { "type": "object", "additionalProperties": false, "required": ["name", "instructions", "visibility"], "properties": { "name": { "type": "string" }, "instructions": { "type": "string" }, "visibility": { "type": "string" }, "public_to": { "type": "array", "items": uuid() }, "model_selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] }, "model_settings": { "$ref": "#/components/schemas/AgentModelSettings" }, "subagents": { "type": "array", "maxItems": 32, "items": { "$ref": "#/components/schemas/SubagentDefinition" } }, "secret_declarations": { "anyOf": [{ "type": "array", "items": { "$ref": "#/components/schemas/AgentSecretDeclaration" } }, { "type": "null" }] }, "tool_allowlist": { "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" }, "default": ["read", "grep", "find", "ls", "edit", "write", "bash", "integration"] } } },
        "UpdateAgentRequest": { "type": "object", "additionalProperties": false, "required": ["name", "instructions", "visibility", "public_to", "runtime_id", "model_selection", "model_settings", "subagents", "sandbox_policy", "managed_skill_ids", "mcp_allowlist"], "properties": { "name": { "type": "string" }, "instructions": { "type": "string" }, "visibility": { "type": "string" }, "public_to": { "type": "array", "items": uuid() }, "runtime_id": { "anyOf": [uuid(), { "type": "null" }] }, "model_selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] }, "model_settings": { "$ref": "#/components/schemas/AgentModelSettings" }, "subagents": { "type": "array", "maxItems": 32, "items": { "$ref": "#/components/schemas/SubagentDefinition" } }, "secret_declarations": { "anyOf": [{ "type": "array", "items": { "$ref": "#/components/schemas/AgentSecretDeclaration" } }, { "type": "null" }] }, "sandbox_policy": { "type": "object" }, "managed_skill_ids": { "type": "array", "items": uuid() }, "mcp_allowlist": {}, "tool_allowlist": { "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" }, "default": ["read", "grep", "find", "ls", "edit", "write", "bash", "skill_exec", "integration"] } } },
        "Run": { "type": "object", "required": ["id", "agent_id", "automation_id", "integration_session_id", "parent_run_id", "runtime_id", "hub_session_id", "hub_message_id", "hub_turn_id", "session_ownership_generation", "status", "initial_message", "native_session_id", "work_dir_ref", "source", "created_at", "updated_at"], "properties": { "id": uuid(), "agent_id": uuid(), "automation_id": { "anyOf": [uuid(), { "type": "null" }] }, "integration_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "parent_run_id": { "anyOf": [uuid(), { "type": "null" }] }, "runtime_id": { "anyOf": [uuid(), { "type": "null" }] }, "hub_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "hub_message_id": { "anyOf": [uuid(), { "type": "null" }] }, "hub_turn_id": { "anyOf": [uuid(), { "type": "null" }] }, "session_ownership_generation": { "type": ["integer", "null"] }, "status": { "type": "string" }, "initial_message": { "type": "string" }, "native_session_id": { "type": ["string", "null"] }, "work_dir_ref": { "type": ["string", "null"] }, "source": { "type": "string" }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "RunListResponse": { "type": "object", "required": ["items", "total", "page", "page_size"], "properties": { "items": { "type": "array", "items": { "$ref": "#/components/schemas/Run" } }, "total": { "type": "integer", "minimum": 0 }, "page": { "type": "integer", "minimum": 1 }, "page_size": { "type": "integer", "minimum": 1, "maximum": 100 } } },
        "CreateRunRequest": { "type": "object", "required": ["message"], "properties": { "message": { "type": "string" }, "hub_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "parent_run_id": { "anyOf": [uuid(), { "type": "null" }] }, "client_message_key": { "type": ["string", "null"] } } },
        "HubSessionOrigin": { "oneOf": [
            { "type": "object", "additionalProperties": false, "required": ["kind"], "properties": { "kind": { "type": "string", "const": "hub_native" } } },
            { "type": "object", "additionalProperties": false, "required": ["kind"], "properties": { "kind": { "type": "string", "const": "public_widget" } } },
            { "type": "object", "additionalProperties": false, "required": ["kind", "platform_id", "tenant_id", "external_identity_id"], "properties": { "kind": { "type": "string", "const": "external" }, "platform_id": uuid(), "tenant_id": { "type": "string" }, "external_identity_id": uuid() } }
        ] },
        "CurrentSessionBundle": { "type": "object", "required": ["generation", "object_key", "checksum_sha256", "size_bytes", "history_checkpoint", "ownership_generation", "producing_engine_version", "created_at"], "properties": { "generation": { "type": "integer" }, "object_key": { "type": "string" }, "checksum_sha256": { "type": "string" }, "size_bytes": { "type": "integer", "minimum": 0 }, "history_checkpoint": { "type": "integer", "minimum": 0 }, "ownership_generation": { "type": "integer", "minimum": 0 }, "producing_engine_version": { "type": "string" }, "created_at": { "type": "string", "format": "date-time" } } },
        "HubSession": { "type": "object", "required": ["id", "owner_id", "agent_id", "agent_name", "agent_deleted_at", "origin_platform_name", "origin", "lifecycle_status", "native_session_id", "active_turn_id", "history_checkpoint", "configuration_fingerprint", "runtime_owner_id", "ownership_generation", "recovery_error", "current_bundle", "created_at", "updated_at"], "properties": { "id": uuid(), "owner_id": uuid(), "agent_id": uuid(), "agent_name": { "type": "string" }, "agent_deleted_at": { "type": ["string", "null"], "format": "date-time" }, "title": { "type": ["string", "null"] }, "origin_platform_name": { "type": ["string", "null"] }, "origin": { "$ref": "#/components/schemas/HubSessionOrigin" }, "lifecycle_status": { "type": "string" }, "native_session_id": { "type": ["string", "null"] }, "active_turn_id": { "anyOf": [uuid(), { "type": "null" }] }, "history_checkpoint": { "type": "integer", "minimum": 0 }, "configuration_fingerprint": { "type": ["string", "null"] }, "runtime_owner_id": { "anyOf": [uuid(), { "type": "null" }] }, "ownership_generation": { "type": "integer", "minimum": 0 }, "recovery_error": { "type": ["string", "null"] }, "current_bundle": { "anyOf": [{ "$ref": "#/components/schemas/CurrentSessionBundle" }, { "type": "null" }] }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "HubSessionAttachment": { "type": "object", "additionalProperties": false, "required": ["id", "session_id", "name", "content_type", "size_bytes", "created_at"], "properties": { "id": uuid(), "session_id": uuid(), "name": { "type": "string" }, "content_type": { "type": "string" }, "size_bytes": { "type": "integer", "minimum": 0 }, "created_at": { "type": "string", "format": "date-time" } } },
        "HubSessionMessage": { "type": "object", "required": ["id", "session_id", "sequence", "role", "message_kind", "content", "payload", "delivery_mode", "delivery_state", "client_message_key", "expected_native_turn_id", "turn_id", "run_id", "accepted_at"], "properties": { "id": uuid(), "session_id": uuid(), "sequence": { "type": "integer", "minimum": 1 }, "role": { "type": "string" }, "message_kind": { "type": "string" }, "content": { "type": ["string", "null"] }, "payload": {}, "attachments": { "type": "array", "items": { "$ref": "#/components/schemas/HubSessionAttachment" } }, "delivery_mode": { "type": "string" }, "delivery_state": { "type": "string" }, "client_message_key": { "type": ["string", "null"] }, "expected_native_turn_id": { "type": ["string", "null"] }, "turn_id": { "anyOf": [uuid(), { "type": "null" }] }, "run_id": { "anyOf": [uuid(), { "type": "null" }] }, "accepted_at": { "type": "string", "format": "date-time" } } },
        "CreateHubSessionMessageRequest": { "type": "object", "required": ["content"], "properties": { "content": { "type": "string" }, "payload": {}, "attachment_ids": { "type": "array", "items": uuid() }, "delivery_mode": { "type": ["string", "null"] }, "client_message_key": { "type": ["string", "null"] }, "parent_run_id": { "anyOf": [uuid(), { "type": "null" }] } } },
        "CreateDraftSessionRequest": { "type": "object", "additionalProperties": false, "required": ["agent_id"], "properties": { "agent_id": uuid() } },
        "UpdateHubSessionTitleRequest": { "type": "object", "additionalProperties": false, "required": ["title"], "properties": { "title": { "type": "string", "minLength": 1, "maxLength": 40 } } },
        "SessionMessageAcceptance": { "type": "object", "required": ["message", "run"], "properties": { "message": { "$ref": "#/components/schemas/HubSessionMessage" }, "run": { "anyOf": [{ "$ref": "#/components/schemas/Run" }, { "type": "null" }] } } },
        "RunEvent": { "type": "object", "required": ["seq", "event_id", "run_id", "event_type", "payload", "created_at"], "properties": { "seq": { "type": "integer" }, "event_id": uuid(), "run_id": uuid(), "event_type": { "type": "string" }, "role": { "type": ["string", "null"] }, "content": { "type": ["string", "null"] }, "payload": {}, "created_at": { "type": "string", "format": "date-time" } } },
        "ClientToolRequestEvent": { "allOf": [{ "$ref": "#/components/schemas/RunEvent" }, { "type": "object", "properties": { "event_type": { "type": "string", "const": "tool_request" }, "payload": { "type": "object", "additionalProperties": false, "required": ["tool_call_id", "tool_name", "arguments", "batch_id", "expires_at"], "properties": { "tool_call_id": uuid(), "tool_name": { "type": "string" }, "arguments": {}, "batch_id": uuid(), "expires_at": { "type": "string", "format": "date-time" } } } } }] },
        "ClientToolResultEvent": { "allOf": [{ "$ref": "#/components/schemas/RunEvent" }, { "type": "object", "properties": { "event_type": { "type": "string", "const": "client_tool_result" }, "payload": { "type": "object", "additionalProperties": false, "required": ["tool_call_id", "tool_name", "result", "elapsed_ms"], "properties": { "tool_call_id": uuid(), "tool_name": { "type": "string" }, "result": { "$ref": "#/components/schemas/ClientToolResult" }, "elapsed_ms": { "type": "integer", "format": "int64", "minimum": 0 } } } } }] },
        "ClientToolTimeoutEvent": { "allOf": [{ "$ref": "#/components/schemas/RunEvent" }, { "type": "object", "properties": { "event_type": { "type": "string", "const": "client_tool_timeout" }, "payload": { "type": "object", "additionalProperties": false, "required": ["tool_call_id", "tool_name", "status", "message"], "properties": { "tool_call_id": uuid(), "tool_name": { "type": "string" }, "status": { "type": "string", "const": "timed_out" }, "message": { "type": "string" } } } } }] },
        "ClientToolErrorEvent": { "allOf": [{ "$ref": "#/components/schemas/RunEvent" }, { "type": "object", "properties": { "event_type": { "type": "string", "const": "client_tool_interrupted" }, "payload": { "type": "object", "additionalProperties": false, "required": ["tool_call_id", "tool_name", "status", "message"], "properties": { "tool_call_id": uuid(), "tool_name": { "type": "string" }, "status": { "type": "string", "enum": ["unknown", "cancelled"] }, "message": { "type": "string" } } } } }] },
        "ClientGenericSessionEvent": { "allOf": [{ "$ref": "#/components/schemas/RunEvent" }, { "type": "object", "properties": { "event_type": { "type": "string", "not": { "enum": ["tool_request", "client_tool_result", "client_tool_timeout", "client_tool_interrupted"] } } } }] },
        "ClientSessionEvent": { "oneOf": [{ "$ref": "#/components/schemas/ClientToolRequestEvent" }, { "$ref": "#/components/schemas/ClientToolResultEvent" }, { "$ref": "#/components/schemas/ClientToolTimeoutEvent" }, { "$ref": "#/components/schemas/ClientToolErrorEvent" }, { "$ref": "#/components/schemas/ClientGenericSessionEvent" }] },
        "Skill": { "type": "object", "required": ["id", "owner_id", "name", "description", "content", "visibility", "public_to", "revision", "content_checksum_sha256", "package", "created_at", "updated_at"], "properties": { "id": uuid(), "owner_id": uuid(), "owner_email": { "anyOf": [{ "type": "string" }, { "type": "null" }] }, "name": { "type": "string" }, "description": { "type": "string" }, "content": { "type": "string" }, "visibility": { "type": "string", "enum": ["private", "public", "public_to"] }, "public_to": { "type": "array", "items": uuid() }, "revision": { "type": "integer", "minimum": 1 }, "content_checksum_sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" }, "package": { "anyOf": [{ "$ref": "#/components/schemas/SkillPackage" }, { "type": "null" }] }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "SkillPackage": { "type": "object", "additionalProperties": false, "required": ["id", "format_version", "size_bytes", "checksum_sha256", "files"], "properties": { "id": uuid(), "format_version": { "type": "integer", "enum": [1] }, "size_bytes": { "type": "integer", "minimum": 1, "maximum": 268435456 }, "checksum_sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" }, "files": { "type": "array", "maxItems": 1024, "items": { "$ref": "#/components/schemas/SkillPackageFile" } } } },
        "SkillPackageFile": { "type": "object", "additionalProperties": false, "required": ["path", "size_bytes", "checksum_sha256", "executable"], "properties": { "path": { "type": "string" }, "size_bytes": { "type": "integer", "minimum": 0 }, "checksum_sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" }, "executable": { "type": "boolean" } } },
        "SkillWriteRequest": { "type": "object", "required": ["name", "description", "content", "visibility", "public_to"], "properties": { "name": { "type": "string" }, "description": { "type": "string" }, "content": { "type": "string" }, "visibility": { "type": "string", "enum": ["private", "public", "public_to"] }, "public_to": { "type": "array", "items": uuid() } } },
        "BulkDeleteSkillsRequest": { "type": "object", "additionalProperties": false, "required": ["skill_ids"], "properties": { "skill_ids": { "type": "array", "minItems": 1, "maxItems": 100, "uniqueItems": true, "items": uuid() } } },
        "BulkDeleteSkillsResponse": { "type": "object", "additionalProperties": false, "required": ["deleted_skill_ids"], "properties": { "deleted_skill_ids": { "type": "array", "items": uuid() } } },
        "Automation": { "type": "object", "required": ["id", "agent_id", "name", "trigger_type", "prompt", "enabled"], "properties": { "id": uuid(), "agent_id": uuid(), "name": { "type": "string" }, "trigger_type": { "type": "string" }, "prompt": { "type": "string" }, "schedule": { "type": ["string", "null"] }, "enabled": { "type": "boolean" } } },
        "CreateAutomationRequest": { "type": "object", "required": ["agent_id", "name", "trigger_type", "prompt", "enabled"], "properties": { "agent_id": uuid(), "name": { "type": "string" }, "trigger_type": { "type": "string" }, "prompt": { "type": "string" }, "schedule": { "type": ["string", "null"] }, "enabled": { "type": "boolean" } } },
        "UpdateAutomationRequest": { "type": "object", "additionalProperties": false, "required": ["name", "trigger_type", "prompt", "schedule", "enabled"], "properties": { "name": { "type": "string" }, "trigger_type": { "type": "string" }, "prompt": { "type": "string" }, "schedule": { "type": ["string", "null"] }, "enabled": { "type": "boolean" } } },
        "TriggerAutomationRequest": { "type": "object", "properties": { "message": { "type": ["string", "null"] } } },
        "Runtime": { "type": "object", "required": ["id", "hostname", "labels", "engine_version", "capabilities", "sandbox_mode", "status", "last_heartbeat_at", "credential_rotation_requested_at"], "properties": { "id": uuid(), "hostname": { "type": "string" }, "labels": { "type": "array", "items": { "type": "string" } }, "engine_version": { "type": "string" }, "sandbox_mode": { "type": "string" }, "status": { "type": "string" }, "last_heartbeat_at": { "type": "string", "format": "date-time" }, "credential_rotation_requested_at": date(), "capabilities": { "type": "object", "additionalProperties": false, "properties": {
            "driver": { "type": "string" },
            "engine_source": { "type": "string" },
            "model_proxy": { "type": "boolean" },
            "mcp_allowlist": { "type": "boolean" },
            "subagents": { "type": "boolean" },
            "native_session_resume": { "type": "boolean" },
            "local_skills": { "type": "boolean" },
            "sandbox_downgraded": { "type": "boolean" },
            "sandbox_downgrade_reason": { "type": "string" }
        } } } },
        "RuntimeEnrollmentToken": { "type": "object", "additionalProperties": false, "required": ["id", "created_by", "expires_at", "consumed_at", "consumed_by_runtime_id", "revoked_at", "created_at"], "properties": { "id": uuid(), "created_by": { "anyOf": [uuid(), { "type": "null" }] }, "expires_at": { "type": "string", "format": "date-time" }, "consumed_at": date(), "consumed_by_runtime_id": { "anyOf": [uuid(), { "type": "null" }] }, "revoked_at": date(), "created_at": { "type": "string", "format": "date-time" } } },
        "RuntimeEnrollmentTokenCreated": { "type": "object", "additionalProperties": false, "required": ["enrollment", "token"], "properties": { "enrollment": { "$ref": "#/components/schemas/RuntimeEnrollmentToken" }, "token": { "type": "string", "description": "Shown once; the Hub stores only its SHA-256 hash." } } },
        "RuntimeRegisterRequest": { "type": "object", "required": ["hostname", "labels", "engine_version", "capabilities", "sandbox_mode"], "properties": { "hostname": { "type": "string" }, "labels": { "type": "array", "items": { "type": "string" } }, "engine_version": { "type": "string" }, "capabilities": {}, "sandbox_mode": { "type": "string" } } },
        "RuntimeRegisterResponse": { "type": "object", "additionalProperties": false, "required": ["runtime_id", "runtime_credential", "protocol_capabilities"], "properties": { "runtime_id": uuid(), "runtime_credential": { "type": "string", "description": "Shown only to the newly enrolled Runtime and never returned by list/admin APIs." }, "protocol_capabilities": { "type": "array", "items": { "type": "string" } } } },
        "RuntimeOwnedSessionStateRequest": { "type": "object", "additionalProperties": false, "required": ["session_id", "ownership_generation", "lifecycle_status"], "properties": { "session_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 }, "lifecycle_status": { "type": "string", "enum": ["restoring", "online", "saving"] }, "checkpoint_reason": { "type": ["string", "null"], "enum": ["idle", "drain", null] } } },
        "RuntimeOwnedSessionSnapshot": { "type": "object", "additionalProperties": false, "required": ["session_id", "ownership_generation", "lifecycle_status", "native_session_id"], "properties": { "session_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 }, "lifecycle_status": { "type": "string", "enum": ["restoring", "online", "saving"] }, "native_session_id": { "type": ["string", "null"] }, "active_run_id": { "type": ["string", "null"], "format": "uuid" } } },
        "RuntimeSteeringMessage": { "type": "object", "additionalProperties": false, "required": ["id", "sequence", "content"], "properties": { "id": uuid(), "sequence": { "type": "integer", "minimum": 1 }, "content": { "type": "string" } } },
        "RuntimeSessionCommand": { "type": "object", "additionalProperties": false, "required": ["command_id", "session_id", "ownership_generation", "command", "run_id", "turn_id", "native_session_id", "native_turn_id", "message", "configuration_revision", "fingerprint", "execution_configuration"], "properties": { "command_id": uuid(), "session_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 }, "command": { "type": "string", "enum": ["checkpoint", "steer", "interrupt", "refresh_configuration"] }, "run_id": { "anyOf": [uuid(), { "type": "null" }] }, "turn_id": { "anyOf": [uuid(), { "type": "null" }] }, "native_session_id": { "type": ["string", "null"] }, "native_turn_id": { "type": ["string", "null"] }, "message": { "anyOf": [{ "$ref": "#/components/schemas/RuntimeSteeringMessage" }, { "type": "null" }] }, "configuration_revision": { "type": ["integer", "null"], "minimum": 1 }, "fingerprint": { "type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$" }, "execution_configuration": { "anyOf": [{ "$ref": "#/components/schemas/AgentExecutionConfiguration" }, { "type": "null" }] } } },
        "RuntimeHeartbeatRequest": { "type": "object", "additionalProperties": false, "properties": { "pending_credential_hash": { "type": ["string", "null"], "pattern": "^[0-9a-f]{64}$" }, "accepts_session_commands": { "type": "boolean", "default": false }, "owned_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeOwnedSessionStateRequest" } }, "cleaned_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeOwnedSessionGeneration" } } } },
        "RuntimeHeartbeatResponse": { "type": "object", "additionalProperties": false, "required": ["rotation_requested", "pending_credential_accepted", "credential_activated", "runtime_status", "owned_sessions", "session_commands"], "properties": { "rotation_requested": { "type": "boolean" }, "pending_credential_accepted": { "type": "boolean" }, "credential_activated": { "type": "boolean" }, "runtime_status": { "type": "string" }, "owned_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeOwnedSessionSnapshot" } }, "cleanup_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeOwnedSessionGeneration" } }, "salvage_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeSalvageSession" } }, "session_commands": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeSessionCommand" } } } },
        "RuntimeSalvageSession": { "type": "object", "additionalProperties": false, "required": ["session_id", "ownership_generation", "history_checkpoint", "bundle_generation"], "properties": { "session_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 }, "history_checkpoint": { "type": "integer", "minimum": 0 }, "bundle_generation": { "type": "integer", "minimum": 1 } } },
        "RuntimeOwnedSessionGeneration": { "type": "object", "additionalProperties": false, "required": ["session_id", "ownership_generation"], "properties": { "session_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 } } },
        "RuntimeClaimRunRequest": { "type": "object", "additionalProperties": false, "required": ["available_new_session_slots", "ready_owned_sessions"], "properties": { "available_new_session_slots": { "type": "integer", "minimum": 0 }, "ready_owned_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeOwnedSessionGeneration" } } } },
        "BeginRuntimeTurnRequest": { "type": "object", "additionalProperties": false, "required": ["configuration_fingerprint"], "properties": { "configuration_fingerprint": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" } } },
        "RuntimeBeginTurnRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "payload"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "payload": { "$ref": "#/components/schemas/BeginRuntimeTurnRequest" } } },
        "BeginRuntimeTurnResponse": { "type": "object", "additionalProperties": false, "required": ["session_id", "turn_id", "ownership_generation", "configuration_fingerprint", "messages"], "properties": { "session_id": uuid(), "turn_id": uuid(), "ownership_generation": { "type": "integer", "minimum": 1 }, "configuration_fingerprint": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" }, "messages": { "type": "array", "items": { "$ref": "#/components/schemas/HubSessionMessage" } } } },
        "RunResume": { "type": "object", "additionalProperties": false, "required": ["native_session_id", "work_dir_ref"], "properties": { "native_session_id": { "type": "string" }, "work_dir_ref": { "type": ["string", "null"] } } },
        "ClaimRunResponse": { "type": "object", "required": ["run", "agent", "execution_configuration", "expected_configuration_fingerprint", "integration_context", "resume", "model_proxy_token", "session_context"], "properties": { "run": { "$ref": "#/components/schemas/Run" }, "agent": { "$ref": "#/components/schemas/Agent" }, "execution_configuration": { "$ref": "#/components/schemas/AgentExecutionConfiguration" }, "expected_configuration_fingerprint": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}$" }, "integration_context": {}, "resume": { "anyOf": [{ "$ref": "#/components/schemas/RunResume" }, { "type": "null" }] }, "model_proxy_token": { "type": "string" }, "session_context": {} } },
        "AgentExecutionConfiguration": { "type": "object", "additionalProperties": false, "required": ["revision", "instructions", "model_selection", "model_settings", "subagents", "model_bindings", "model_policy", "sandbox_policy", "skills", "mcp_allowlist", "tool_allowlist"], "properties": { "revision": { "type": "integer", "minimum": 1 }, "instructions": { "type": "string" }, "model_selection": { "anyOf": [{ "$ref": "#/components/schemas/ModelSelection" }, { "type": "null" }] }, "model_settings": { "$ref": "#/components/schemas/AgentModelSettings" }, "subagents": { "type": "array", "items": { "$ref": "#/components/schemas/SubagentDefinition" } }, "model_bindings": { "type": "array", "items": { "$ref": "#/components/schemas/RunModelBinding" } }, "model_policy": {}, "sandbox_policy": {}, "skills": { "type": "array", "items": { "$ref": "#/components/schemas/AgentExecutionSkill" } }, "mcp_allowlist": {}, "tool_allowlist": { "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" } } } },
        "AgentExecutionSkill": { "type": "object", "additionalProperties": false, "required": ["source", "source_id", "name", "description", "content", "revision", "content_checksum_sha256", "package"], "properties": { "source": { "type": "string", "enum": ["managed"] }, "source_id": { "anyOf": [uuid(), { "type": "null" }] }, "name": { "type": "string" }, "description": { "type": "string" }, "content": { "type": "string" }, "revision": { "type": "integer", "minimum": 1 }, "content_checksum_sha256": { "type": "string", "pattern": "^[0-9a-f]{64}$" }, "package": { "anyOf": [{ "$ref": "#/components/schemas/SkillPackage" }, { "type": "null" }] } } },
        "AppendRunEventRequest": { "type": "object", "required": ["event_type", "role", "content", "payload", "waiting_tool"], "properties": { "event_type": { "type": "string" }, "role": { "type": ["string", "null"] }, "content": { "type": ["string", "null"] }, "payload": {}, "waiting_tool": { "anyOf": [{ "$ref": "#/components/schemas/WaitingToolRunTransition" }, { "type": "null" }] } } },
        "WaitingToolRunTransition": { "type": "object", "required": ["native_session_id", "work_dir_ref"], "properties": { "native_session_id": { "type": "string" }, "work_dir_ref": { "type": "string" } } },
        "FinalizeToolRequestsRequest": { "type": "object", "required": ["native_session_id", "work_dir_ref", "tool_requests"], "properties": { "integration_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "native_session_id": { "type": "string" }, "work_dir_ref": { "type": "string" }, "tool_requests": { "type": "array", "items": { "type": "object" } } } },
        "CompleteRuntimeSessionCommandRequest": { "type": "object", "additionalProperties": false, "required": ["command", "outcome", "revision", "fingerprint"], "properties": { "command": { "type": "string", "enum": ["steer", "interrupt", "refresh_configuration"] }, "outcome": { "type": "string", "enum": ["applied", "turn_ended", "failed", "interrupted"] }, "revision": { "type": ["integer", "null"], "minimum": 1 }, "fingerprint": { "type": ["string", "null"], "pattern": "^sha256:[0-9a-f]{64}$" } } },
        "CompleteRuntimeSessionCommandResponse": { "type": "object", "additionalProperties": false, "required": ["command_id", "outcome"], "properties": { "command_id": uuid(), "outcome": { "type": "string", "enum": ["applied", "turn_ended", "failed", "interrupted"] } } },
        "CompleteRunRequest": { "type": "object", "required": ["status", "native_session_id", "work_dir_ref"], "properties": { "status": { "type": "string", "enum": ["completed", "failed", "waiting_tool", "interrupted"] }, "native_session_id": { "type": ["string", "null"] }, "work_dir_ref": { "type": ["string", "null"] } } },
        "RuntimeAppendRunEventRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "payload"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "payload": { "$ref": "#/components/schemas/AppendRunEventRequest" } } },
        "RuntimeFinalizeToolRequestsRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "payload"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "payload": { "$ref": "#/components/schemas/FinalizeToolRequestsRequest" } } },
        "RuntimeCompleteSessionCommandRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "payload"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "payload": { "$ref": "#/components/schemas/CompleteRuntimeSessionCommandRequest" } } },
        "RuntimeCompleteRunRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "payload"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "payload": { "$ref": "#/components/schemas/CompleteRunRequest" } } },
        "ReleaseRuntimeSessionRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 } } },
        "AbandonRuntimeSalvageRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 } } },
        "BeginRuntimeSessionCheckpointRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "reason"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "reason": { "type": "string", "enum": ["idle", "drain"] } } },
        "RuntimeSessionCheckpointAttempt": { "type": "object", "additionalProperties": false, "required": ["checkpoint_attempt_id", "history_checkpoint", "bundle_generation", "reason"], "properties": { "checkpoint_attempt_id": uuid(), "history_checkpoint": { "type": "integer", "minimum": 0 }, "bundle_generation": { "type": "integer", "minimum": 1 }, "reason": { "type": "string", "enum": ["idle", "drain"] } } },
        "RuntimeSessionBundleCommitResponse": { "type": "object", "additionalProperties": false, "required": ["checkpoint_attempt_id", "bundle_generation", "has_queued_work", "ownership_released"], "properties": { "checkpoint_attempt_id": uuid(), "bundle_generation": { "type": "integer", "minimum": 1 }, "has_queued_work": { "type": "boolean" }, "ownership_released": { "type": "boolean" } } },
        "FailRuntimeSessionCheckpointRequest": { "type": "object", "additionalProperties": false, "required": ["ownership_generation", "checkpoint_attempt_id", "error"], "properties": { "ownership_generation": { "type": "integer", "minimum": 1 }, "checkpoint_attempt_id": uuid(), "error": { "type": "string", "minLength": 1 } } },
        "RuntimeSessionCheckpointDisposition": { "type": "object", "additionalProperties": false, "required": ["checkpoint_attempt_id", "disposition", "has_queued_work"], "properties": { "checkpoint_attempt_id": uuid(), "disposition": { "type": "string", "enum": ["resume", "retry"] }, "has_queued_work": { "type": "boolean" } } },
        "ConfirmRuntimeHostnameRequest": { "type": "object", "additionalProperties": false, "required": ["hostname"], "properties": { "hostname": { "type": "string" } } },
        "RuntimeDrainResponse": { "type": "object", "required": ["runtime", "owned_sessions"], "properties": { "runtime": { "$ref": "#/components/schemas/Runtime" }, "owned_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/HubSession" } } } },
        "RuntimeDeletionImpactSession": { "type": "object", "additionalProperties": false, "required": ["session_id", "agent_name", "lifecycle_status", "force_delete_disposition"], "properties": { "session_id": uuid(), "agent_name": { "type": "string" }, "lifecycle_status": { "type": "string" }, "force_delete_disposition": { "type": "string", "enum": ["recoverable", "recovery_failed"] } } },
        "RuntimeDeletionImpact": { "type": "object", "additionalProperties": false, "required": ["runtime_id", "hostname", "affected_sessions"], "properties": { "runtime_id": uuid(), "hostname": { "type": "string" }, "affected_sessions": { "type": "array", "items": { "$ref": "#/components/schemas/RuntimeDeletionImpactSession" } } } },
        "ForceDeleteRuntimeResponse": { "type": "object", "required": ["runtime_id", "recoverable_session_ids", "recovery_failed_session_ids"], "properties": { "runtime_id": uuid(), "recoverable_session_ids": { "type": "array", "items": uuid() }, "recovery_failed_session_ids": { "type": "array", "items": uuid() } } },
        "CreateEmbedSessionRequest": { "type": "object", "required": ["agent_id"], "properties": { "agent_id": uuid() } },
        "EmbedExchangeRequest": { "type": "object", "required": ["jwt"], "properties": { "jwt": { "type": "string" } } },
        "TokenResponse": { "type": "object", "required": ["token"], "properties": { "token": { "type": "string" } } },
        "WidgetAgent": { "type": "object", "required": ["id", "name", "instructions"], "properties": { "id": uuid(), "name": { "type": "string" }, "instructions": { "type": "string" } } },
        "WidgetUserProfile": { "type": "object", "additionalProperties": false, "properties": { "username": { "type": ["string", "null"] }, "display_name": { "type": ["string", "null"] }, "email": { "type": ["string", "null"], "format": "email" }, "attributes": { "type": "object", "additionalProperties": true, "default": {} } } },
        "ClientToolDefinition": { "type": "object", "additionalProperties": false, "required": ["name", "description", "input_schema"], "properties": { "name": { "type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[A-Za-z0-9_-]+$" }, "description": { "type": "string" }, "input_schema": { "type": "object" } } },
        "ClientToolError": { "type": "object", "additionalProperties": false, "required": ["code", "message", "retryable"], "properties": { "code": { "type": "string" }, "message": { "type": "string" }, "retryable": { "type": "boolean" } } },
        "ClientToolResult": { "oneOf": [{ "type": "object", "additionalProperties": false, "required": ["status", "output"], "properties": { "status": { "type": "string", "const": "success" }, "output": {} } }, { "type": "object", "additionalProperties": false, "required": ["status", "error"], "properties": { "status": { "type": "string", "const": "error" }, "error": { "$ref": "#/components/schemas/ClientToolError" } } }] },
        "ClientToolClaimResponse": { "type": "object", "additionalProperties": false, "required": ["status", "terminal"], "properties": { "status": { "type": "string", "enum": ["claimed", "completed", "timed_out", "unknown", "cancelled"] }, "terminal": { "type": "boolean" }, "result": { "anyOf": [{ "$ref": "#/components/schemas/ClientToolResult" }, { "type": "null" }] } } },
        "SubmitClientToolResultRequest": { "type": "object", "additionalProperties": false, "required": ["result"], "properties": { "result": { "$ref": "#/components/schemas/ClientToolResult" } } },
        "SubmitClientToolResultResponse": { "type": "object", "additionalProperties": false, "required": ["run", "tool_request"], "properties": { "run": { "anyOf": [{ "$ref": "#/components/schemas/Run" }, { "type": "null" }] }, "tool_request": { "type": "object" } } },
        "CreateWidgetAccessRequest": { "type": "object", "additionalProperties": false, "required": ["agent_id", "client_instance_id", "external_user_id", "tenant_id", "email"], "properties": { "agent_id": uuid(), "client_instance_id": uuid(), "external_user_id": { "type": "string" }, "tenant_id": { "type": "string" }, "username": { "type": ["string", "null"] }, "display_name": { "type": ["string", "null"] }, "email": { "type": "string", "format": "email" }, "attributes": { "type": "object", "additionalProperties": true, "default": {} }, "client_tools": { "type": "array", "maxItems": 128, "items": { "$ref": "#/components/schemas/ClientToolDefinition" }, "default": [] } } },
        "CreateClientAccessRequest": { "type": "object", "additionalProperties": false, "required": ["agent_id", "client_instance_id", "external_user_id", "tenant_id", "email"], "properties": { "agent_id": uuid(), "client_instance_id": uuid(), "external_user_id": { "type": "string" }, "tenant_id": { "type": "string" }, "username": { "type": ["string", "null"] }, "display_name": { "type": ["string", "null"] }, "email": { "type": "string", "format": "email" }, "attributes": { "type": "object", "additionalProperties": true, "default": {} }, "client_tools": { "type": "array", "maxItems": 128, "items": { "$ref": "#/components/schemas/ClientToolDefinition" }, "default": [] } } },
        "WidgetSession": { "type": "object", "required": ["id", "name", "instructions"], "properties": { "id": uuid(), "name": { "type": "string" }, "instructions": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" }, "history_enabled": { "type": "boolean" } } },
        "WidgetAccessResponse": { "type": "object", "additionalProperties": false, "required": ["token", "expires_at", "agent", "history_enabled"], "properties": { "token": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" }, "agent": { "$ref": "#/components/schemas/WidgetAgent" }, "history_enabled": { "type": "boolean" } } },
        "ClientAccessResponse": { "type": "object", "additionalProperties": false, "required": ["access_token", "expires_at", "expires_in", "client_instance_id", "session_id", "agent", "history_enabled", "tool_names"], "properties": { "access_token": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" }, "expires_in": { "type": "integer", "minimum": 1 }, "client_instance_id": uuid(), "session_id": { "anyOf": [uuid(), { "type": "null" }] }, "agent": { "$ref": "#/components/schemas/WidgetAgent" }, "history_enabled": { "type": "boolean" }, "tool_names": { "type": "array", "uniqueItems": true, "items": { "type": "string" } } } },
        "CreatePublicWidgetAccessRequest": { "type": "object", "additionalProperties": false, "required": ["client_id", "visitor_key", "client_instance_id"], "properties": { "client_id": { "type": "string", "minLength": 1 }, "visitor_key": { "type": "string", "minLength": 16, "maxLength": 512 }, "client_instance_id": uuid(), "session_id": { "anyOf": [uuid(), { "type": "null" }] } } },
        "CreateAnonymousClientAccessRequest": { "type": "object", "additionalProperties": false, "required": ["client_id", "visitor_key", "client_instance_id"], "properties": { "client_id": { "type": "string", "minLength": 1 }, "visitor_key": { "type": "string", "minLength": 16, "maxLength": 512 }, "client_instance_id": uuid(), "session_id": { "anyOf": [uuid(), { "type": "null" }] } } },
        "PublicWidgetAccessResponse": { "type": "object", "additionalProperties": false, "required": ["token", "expires_at", "widget_session_id", "hub_session_id", "agent"], "properties": { "token": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" }, "widget_session_id": uuid(), "hub_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "agent": { "$ref": "#/components/schemas/WidgetAgent" } } },
        "RenewWidgetSessionRequest": { "type": "object", "additionalProperties": false, "properties": { "profile": { "anyOf": [{ "$ref": "#/components/schemas/WidgetUserProfile" }, { "type": "null" }] } } },
        "RenewClientAccessRequest": { "type": "object", "additionalProperties": false, "properties": { "profile": { "anyOf": [{ "$ref": "#/components/schemas/WidgetUserProfile" }, { "type": "null" }] } } },
        "WidgetTokenResponse": { "type": "object", "additionalProperties": false, "required": ["token", "expires_at"], "properties": { "token": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" } } },
        "WidgetHistorySession": { "type": "object", "additionalProperties": false, "required": ["id", "hub_session_id", "created_at", "updated_at", "preview"], "properties": { "id": uuid(), "hub_session_id": uuid(), "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" }, "preview": { "type": ["string", "null"] } } },
        "ClientSessionMetadata": { "type": "object", "additionalProperties": false, "required": ["id", "name", "instructions"], "properties": { "id": uuid(), "name": { "type": "string" }, "instructions": { "type": "string" }, "expires_at": { "type": "string", "format": "date-time" }, "history_enabled": { "type": "boolean" } } },
        "ClientSessionSummary": { "type": "object", "additionalProperties": false, "required": ["id", "hub_session_id", "created_at", "updated_at", "preview"], "properties": { "id": uuid(), "hub_session_id": uuid(), "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" }, "preview": { "type": ["string", "null"] } } },
        "CreateWidgetRunRequest": { "type": "object", "additionalProperties": false, "required": ["message"], "properties": { "message": { "type": "string" }, "session_id": { "anyOf": [uuid(), { "type": "null" }] }, "integration_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "hub_session_id": { "anyOf": [uuid(), { "type": "null" }] }, "parent_run_id": { "anyOf": [uuid(), { "type": "null" }] }, "client_message_key": { "type": ["string", "null"] } } },
        "CreateClientRunRequest": { "type": "object", "additionalProperties": false, "required": ["message"], "properties": { "message": { "type": "string" }, "session_id": { "anyOf": [uuid(), { "type": "null" }] }, "client_message_key": { "type": ["string", "null"] } } },
        "IntegrationApp": { "type": "object", "additionalProperties": false, "required": ["id", "owner_id", "name", "client_id", "external_platform_id", "authentication_channel_id", "redirect_uris", "agent_ids", "widget_history_enabled", "login_required", "allowed_origins", "tool_allowlist", "client_tool_definitions", "created_at", "updated_at"], "properties": { "id": uuid(), "owner_id": uuid(), "name": { "type": "string" }, "client_id": { "type": "string" }, "external_platform_id": uuid(), "authentication_channel_id": uuid(), "redirect_uris": { "type": "array", "items": { "type": "string", "format": "uri" } }, "agent_ids": { "type": "array", "items": uuid() }, "widget_history_enabled": { "type": "boolean" }, "login_required": { "type": "boolean", "default": true }, "allowed_origins": { "type": "array", "items": { "type": "string", "format": "uri" } }, "tool_allowlist": { "anyOf": [{ "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" } }, { "type": "null" }] }, "client_tool_definitions": { "type": "array", "maxItems": 128, "items": { "$ref": "#/components/schemas/ClientToolDefinition" } }, "created_at": { "type": "string", "format": "date-time" }, "updated_at": { "type": "string", "format": "date-time" } } },
        "IntegrationAppSecretResponse": { "type": "object", "additionalProperties": false, "required": ["integration_app", "client_secret"], "properties": { "integration_app": { "$ref": "#/components/schemas/IntegrationApp" }, "client_secret": { "type": "string", "description": "Shown only in this create or rotate response." } } },
        "CreateIntegrationAppRequest": { "type": "object", "additionalProperties": false, "required": ["name", "external_platform_id", "authentication_channel_id", "redirect_uris", "agent_ids"], "properties": { "name": { "type": "string" }, "external_platform_id": uuid(), "authentication_channel_id": uuid(), "redirect_uris": { "type": "array", "items": { "type": "string", "format": "uri" } }, "agent_ids": { "type": "array", "maxItems": 100, "uniqueItems": true, "items": uuid() }, "widget_history_enabled": { "type": "boolean", "default": false }, "login_required": { "type": "boolean", "default": true }, "allowed_origins": { "type": "array", "items": { "type": "string", "format": "uri" }, "default": [] }, "tool_allowlist": { "anyOf": [{ "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" } }, { "type": "null" }], "default": null }, "client_tool_definitions": { "type": "array", "maxItems": 128, "items": { "$ref": "#/components/schemas/ClientToolDefinition" }, "default": [] } } },
        "UpdateIntegrationAppRequest": { "type": "object", "additionalProperties": false, "required": ["name", "redirect_uris", "agent_ids"], "properties": { "name": { "type": "string" }, "redirect_uris": { "type": "array", "items": { "type": "string", "format": "uri" } }, "agent_ids": { "type": "array", "maxItems": 100, "uniqueItems": true, "items": uuid() }, "widget_history_enabled": { "type": "boolean", "default": false }, "login_required": { "type": "boolean", "default": true }, "allowed_origins": { "type": "array", "items": { "type": "string", "format": "uri" }, "default": [] }, "tool_allowlist": { "anyOf": [{ "type": "array", "minItems": 1, "uniqueItems": true, "items": { "$ref": "#/components/schemas/AgentToolName" } }, { "type": "null" }], "default": null }, "client_tool_definitions": { "type": "array", "maxItems": 128, "items": { "$ref": "#/components/schemas/ClientToolDefinition" }, "default": [] } } },
        "OAuthTokenRequest": { "oneOf": [
            { "type": "object", "additionalProperties": false, "required": ["grant_type", "client_id", "client_secret", "code", "redirect_uri"], "properties": { "grant_type": { "type": "string", "const": "authorization_code" }, "client_id": { "type": "string" }, "client_secret": { "type": "string" }, "code": { "type": "string" }, "redirect_uri": { "type": "string", "format": "uri" }, "scope": { "type": "string" } } },
            { "type": "object", "additionalProperties": false, "required": ["grant_type", "client_id", "client_secret", "scope"], "properties": { "grant_type": { "type": "string", "const": "client_credentials" }, "client_id": { "type": "string" }, "client_secret": { "type": "string" }, "scope": { "type": "string" } } }
        ] },
        "OAuthTokenResponse": { "type": "object", "required": ["access_token", "token_type", "expires_in", "scope"], "properties": { "access_token": { "type": "string" }, "token_type": { "type": "string", "const": "Bearer" }, "expires_in": { "type": "integer" }, "scope": { "type": "string" } } },
        "OAuthExternalProfile": { "type": "object", "additionalProperties": false, "required": ["platform_id", "tenant_id", "external_identity_id", "external_user_id"], "properties": { "platform_id": uuid(), "tenant_id": { "type": "string" }, "external_identity_id": uuid(), "external_user_id": { "type": "string" }, "username": { "type": "string" }, "email": { "type": "string", "format": "email" } } },
        "OAuthUserInfo": { "type": "object", "additionalProperties": false, "required": ["sub"], "properties": { "sub": uuid(), "name": { "type": "string" }, "email": { "type": "string", "format": "email" }, "external_profile": { "$ref": "#/components/schemas/OAuthExternalProfile" } } },
        "CreateIntegrationSessionRequest": { "type": "object", "required": ["agent_id", "external_user_id", "tools", "metadata"], "properties": { "agent_id": uuid(), "external_user_id": { "type": "string" }, "tenant_id": { "type": ["string", "null"] }, "username": { "type": ["string", "null"] }, "display_name": { "type": ["string", "null"] }, "email": { "type": ["string", "null"], "format": "email", "description": "Required for client_credentials; ignored for authorization_code." }, "tools": {}, "metadata": {} } },
        "IntegrationSession": { "type": "object", "required": ["id", "hub_session_id", "agent_id", "owner_id", "platform_id", "tenant_id", "external_identity_id", "external_user_id", "tool_definitions", "metadata", "created_at"], "properties": { "id": uuid(), "hub_session_id": uuid(), "agent_id": uuid(), "owner_id": uuid(), "platform_id": uuid(), "tenant_id": { "type": "string" }, "external_identity_id": uuid(), "external_user_id": { "type": "string" }, "tool_definitions": {}, "metadata": {}, "created_at": { "type": "string", "format": "date-time" } } },
        "IntegrationMessageRequest": { "type": "object", "required": ["content", "attachments"], "properties": { "content": { "type": "string" }, "attachments": {}, "client_message_key": { "type": ["string", "null"] } } },
        "IntegrationMessageResponse": { "type": "object", "required": ["run", "message"], "properties": { "run": { "$ref": "#/components/schemas/Run" }, "message": { "$ref": "#/components/schemas/HubSessionMessage" } } },
        "ToolResultRequest": { "type": "object", "required": ["result"], "properties": { "result": {} } },
        "ToolResultResponse": { "type": "object", "required": ["tool_request", "run"], "properties": { "tool_request": { "type": "object" }, "run": { "$ref": "#/components/schemas/Run" } } }
    })
}

pub(crate) async fn readiness(State(state): State<Arc<AppState>>) -> Response {
    readiness_response(&state.pool, DATABASE_READINESS_TIMEOUT).await
}

pub(crate) async fn readiness_response(pool: &PgPool, timeout: Duration) -> Response {
    let ready = matches!(
        tokio::time::timeout(
            timeout,
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool)
        )
        .await,
        Ok(Ok(1))
    );
    if ready {
        Json(json!({ "ok": true })).into_response()
    } else {
        warn!("database readiness check failed");
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false })),
        )
            .into_response()
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]

struct ModelGatewayForwardRequest<'a> {
    request_id: Uuid,
    upstream_protocol: ModelUpstreamProtocol,
    request_settings: &'a ModelRequestSettings,
    upstream_url: &'a str,
    query: Option<&'a str>,
    headers: &'a HeaderMap,
    body: &'a [u8],
    api_key: &'a str,
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)] // Keep every optional run association explicit at call sites.
#[allow(clippy::too_many_arguments)] // Keep every optional run association explicit at call sites.
#[derive(Debug)]
struct OAuthAppRecord {
    id: Uuid,
    owner_id: Uuid,
    client_secret_hash: String,
    redirect_uris: Value,
    external_platform_id: Uuid,
    authentication_channel_id: Uuid,
    widget_history_enabled: bool,
    login_required: bool,
    allowed_origins: Vec<String>,
    tool_allowlist: Option<Vec<String>>,
    client_tool_definitions: Vec<ClientToolDefinitionDto>,
}

#[derive(Debug, Deserialize)]
struct SecretGrantListQuery {
    agent_id: Option<Uuid>,
}

#[cfg(test)]

pub(crate) fn existing_mcp_secret<'a>(
    existing: &'a Value,
    server_name: Option<&str>,
    key: &str,
) -> Option<&'a str> {
    let server_name = server_name?;
    existing.as_array()?.iter().find_map(|server| {
        if server.get("name").and_then(Value::as_str) != Some(server_name) {
            return None;
        }
        server
            .get("secrets")
            .and_then(Value::as_object)?
            .get(key)?
            .as_str()
    })
}

pub(crate) async fn load_run_for_user(
    pool: &PgPool,
    run_id: Uuid,
    user: &UserDto,
) -> Result<RunDto, ApiError> {
    let row = sqlx::query(
        "SELECT r.id, r.agent_id, r.automation_id, r.integration_session_id,
                r.parent_run_id, r.runtime_id, r.hub_session_id, r.hub_message_id,
                r.hub_turn_id, r.session_ownership_generation, r.status,
                r.initial_message, r.native_session_id, r.work_dir_ref, r.source,
                r.created_at, r.updated_at
         FROM runs r
         JOIN agents a ON a.id = r.agent_id
         JOIN users AS run_owner ON run_owner.id = r.owner_id
         WHERE r.id = $1 AND (r.owner_id = $2 OR $3 IN ('admin', 'super_admin'))
           AND (r.owner_id = $2 OR run_owner.role <> 'super_admin'
                OR $3 IN ('admin', 'super_admin'))",
    )
    .bind(run_id)
    .bind(user.id)
    .bind(&user.role)
    .fetch_optional(pool)
    .await?;
    row.map(run_from_row)
        .ok_or(ApiError::not_found("run not found"))
}

pub(crate) async fn authorize_run_stream(
    state: &AppState,
    headers: &HeaderMap,
    run_id: Uuid,
) -> Result<(), ApiError> {
    if let Some(token) = client_access_token_from_headers(headers) {
        let mut tx = state.pool.begin().await?;
        let credential = load_widget_credential_tx(&mut tx, &token, headers).await?;
        let run = sqlx::query(
            "SELECT integration_session_id, hub_session_id FROM runs
             WHERE id = $1 AND agent_id = $2 AND owner_id = $3
               AND (($4::boolean = true AND source IN ('widget', 'integration:tool_result'))
                    OR ($4::boolean = false AND widget_session_id = $5))",
        )
        .bind(run_id)
        .bind(credential.agent_id)
        .bind(credential.owner_id)
        .bind(credential.client_instance_id.is_some())
        .bind(credential.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ApiError::forbidden("embed session cannot access run"))?;
        if let Err(error) = load_widget_scoped_session_tx(
            &mut tx,
            &credential,
            run.get("integration_session_id"),
            Some(run.get("hub_session_id")),
            false,
        )
        .await
        {
            if error.status == StatusCode::NOT_FOUND {
                return Err(ApiError::forbidden("embed session cannot access run"));
            }
            return Err(error);
        }
        tx.commit().await?;
        return Ok(());
    }
    let user = require_user(state, headers).await?;
    load_run_for_user(&state.pool, run_id, &user).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn public_widget_tool_policy_is_read_only_and_app_scoped() {
        let mut tools = default_agent_tool_allowlist();
        let mut sandbox_policy = json!({ "mode": "workspace-write", "network_access": true });
        let mut mcp_allowlist = json!([{ "name": "private-filesystem" }]);
        apply_session_tool_policy(
            &mut tools,
            &mut sandbox_policy,
            &mut mcp_allowlist,
            &SessionToolPolicy {
                public_widget: true,
                app_tool_allowlist: Some(vec!["read".into(), "grep".into()]),
            },
        )
        .unwrap();

        assert_eq!(tools, ["read", "grep"]);
        assert_eq!(
            sandbox_policy,
            json!({ "mode": "read-only", "network_access": false })
        );
        assert_eq!(mcp_allowlist, json!([]));
    }

    #[test]
    fn public_widget_configuration_and_tool_allowlists_fail_closed() {
        assert_eq!(
            normalize_agent_tool_allowlist(&["grep".into(), "read".into()]).unwrap(),
            ["read", "grep"]
        );
        for invalid in [
            Vec::<String>::new(),
            vec!["read".into(), "read".into()],
            vec!["unknown".into()],
        ] {
            assert_eq!(
                normalize_agent_tool_allowlist(&invalid).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
        }

        let agent_id = Uuid::new_v4();
        assert!(validate_public_widget_settings(
            false,
            false,
            &["https://docs.example.test".into()],
            Some(&["read".into(), "grep".into()]),
            &[agent_id],
            "admin",
        )
        .is_ok());
        for error in [
            validate_public_widget_settings(
                false,
                false,
                &["https://docs.example.test".into()],
                None,
                &[agent_id],
                "member",
            ),
            validate_public_widget_settings(false, false, &[], None, &[agent_id], "admin"),
            validate_public_widget_settings(
                false,
                true,
                &["https://docs.example.test".into()],
                None,
                &[agent_id],
                "admin",
            ),
            validate_public_widget_settings(
                false,
                false,
                &["https://docs.example.test".into()],
                None,
                &[agent_id, Uuid::new_v4()],
                "admin",
            ),
            validate_public_widget_settings(
                false,
                false,
                &["https://docs.example.test".into()],
                Some(&["bash".into()]),
                &[agent_id],
                "admin",
            ),
        ] {
            assert!(error.is_err());
        }
    }

    #[test]
    fn canonical_client_openapi_is_distinct_typed_and_documents_origin_policy() {
        let document = openapi_document();
        let schemas = &document["components"]["schemas"];

        for schema_name in [
            "CreateClientAccessRequest",
            "CreateAnonymousClientAccessRequest",
            "RenewClientAccessRequest",
            "CreateClientRunRequest",
            "ClientSessionMetadata",
            "ClientSessionSummary",
        ] {
            assert_eq!(
                schemas[schema_name]["type"], "object",
                "invalid {schema_name}"
            );
            assert!(
                schemas[schema_name].get("$ref").is_none(),
                "aliased {schema_name}"
            );
        }
        assert!(schemas["CreateClientRunRequest"]["properties"]
            .get("integration_session_id")
            .is_none());
        assert!(schemas["CreateClientRunRequest"]["properties"]
            .get("parent_run_id")
            .is_none());
        assert_eq!(
            document["paths"]["/api/client/session"]["get"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/ClientSessionMetadata"
        );
        assert_eq!(
            document["paths"]["/api/client/sessions"]["get"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/ClientSessionSummary"
        );

        assert_eq!(
            document["paths"]["/api/client/access"]["post"]["parameters"][0]["required"],
            false
        );
        assert_eq!(
            document["paths"]["/api/client/anonymous/access"]["post"]["parameters"][0]["required"],
            true
        );
        assert_eq!(
            document["paths"]["/api/client/sessions/{session_id}/events"]["get"]["responses"]
                ["200"]["content"]["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/ClientSessionEvent"
        );
        assert_eq!(
            schemas["ClientSessionEvent"]["oneOf"]
                .as_array()
                .unwrap()
                .len(),
            5
        );
        assert_eq!(
            document["paths"]["/api/client/tool-calls/{tool_call_id}/result"]["post"]["responses"]
                ["409"]["description"],
            "A different result was already accepted for this tool_call_id"
        );

        for (path, item) in document["paths"].as_object().unwrap() {
            if !path.starts_with("/api/widget/") {
                continue;
            }
            for method in ["get", "post", "put", "patch", "delete"] {
                if let Some(operation) = item.get(method) {
                    assert_eq!(
                        operation["deprecated"], true,
                        "{method} {path} is not deprecated"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn frontend_files_and_spa_fallback_do_not_capture_unknown_api_routes() {
        let frontend = tempfile::tempdir().unwrap();
        std::fs::create_dir(frontend.path().join("assets")).unwrap();
        std::fs::write(
            frontend.path().join("index.html"),
            b"<main>Agent Hub</main>",
        )
        .unwrap();
        std::fs::write(
            frontend.path().join("assets/app.js"),
            b"console.log('hub');",
        )
        .unwrap();

        let app = with_frontend(
            Router::new().route("/api/known", get(|| async { StatusCode::NO_CONTENT })),
            frontend.path().to_path_buf(),
        );

        let asset = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/assets/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(asset.status(), StatusCode::OK);
        assert!(asset.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .contains("javascript"));
        assert_eq!(
            axum::body::to_bytes(asset.into_body(), usize::MAX)
                .await
                .unwrap(),
            Bytes::from_static(b"console.log('hub');")
        );

        let deep_link = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/sessions/example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deep_link.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(deep_link.into_body(), usize::MAX)
                .await
                .unwrap(),
            Bytes::from_static(b"<main>Agent Hub</main>")
        );

        let known_api = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/known")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(known_api.status(), StatusCode::NO_CONTENT);

        for path in ["/api", "/api/", "/api/not-a-route"] {
            let unknown_api = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(unknown_api.status(), StatusCode::NOT_FOUND);
        }
    }

    #[test]
    fn extracts_session_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("theme=dark; agent_hub_session=abc; other=1"),
        );
        assert_eq!(
            session_token_from_headers(&headers),
            Some("abc".to_string())
        );
    }

    #[tokio::test]
    async fn openapi_is_public_json_and_only_documents_registered_routes() {
        let document = openapi_document();
        assert_eq!(document["openapi"], "3.1.0");
        assert_eq!(document["info"]["title"], "Agent Hub API");
        assert!(document["servers"]
            .as_array()
            .is_some_and(|servers| !servers.is_empty()));
        for scheme in [
            "sessionCookie",
            "userBearer",
            "integrationBearer",
            "runtimeEnrollmentBearer",
            "runtimeBearer",
            "modelProxyBearer",
            "integrationClientBasic",
            "clientAccessBearer",
        ] {
            assert!(
                document["components"]["securitySchemes"]
                    .get(scheme)
                    .is_some(),
                "missing security scheme {scheme}"
            );
        }
        for path in [
            "/api/auth/register",
            "/api/auth/login",
            "/api/auth/ldap/login",
            "/api/auth/logout",
            "/api/auth/me",
            "/api/users/me",
            "/api/auth/providers",
            "/api/admin/auth-policy",
            "/api/admin/ldap-config",
            "/api/admin/ldap-config/test",
            "/api/admin/users",
            "/api/admin/users/{user_id}",
            "/api/auth/api-keys/{api_key_id}/renew",
            "/api/model-connections",
            "/api/model-connections/options",
            "/api/model-connections/{model_connection_id}",
            "/api/model-connections/{model_connection_id}/status",
            "/api/model-connections/{model_connection_id}/test",
            "/api/model-connections/{model_connection_id}/force-delete",
            "/api/model-connections/system-default",
            "/api/model-usage/summary",
            "/api/model-usage",
            "/api/model-call-errors",
            "/api/agents/{agent_id}",
            "/api/agents/{agent_id}/model-options",
            "/api/sessions",
            "/api/sessions/{session_id}",
            "/api/sessions/{session_id}/title",
            "/api/sessions/{session_id}/messages",
            "/api/sessions/{session_id}/messages/upload",
            "/api/sessions/{session_id}/messages/{message_id}/attachments",
            "/api/attachments",
            "/api/attachments/{attachment_id}",
            "/api/runs/{run_id}/stop",
            "/api/runs/{run_id}/events/stream",
            "/api/client/session",
            "/api/client/sessions",
            "/api/client/sessions/{session_id}/messages",
            "/api/client/sessions/{session_id}/events",
            "/api/client/sessions/{session_id}/events/stream",
            "/api/client/runs",
            "/api/client/runs/{run_id}/stop",
            "/api/client/tool-calls/{tool_call_id}/claim",
            "/api/client/tool-calls/{tool_call_id}/result",
            "/api/widget/access",
            "/api/widget/public/access",
            "/api/widget/session",
            "/api/widget/session/renew",
            "/api/widget/sessions",
            "/api/widget/sessions/{session_id}/messages",
            "/api/widget/sessions/{session_id}/events",
            "/api/widget/sessions/{session_id}/events/stream",
            "/api/widget/attachments",
            "/api/widget/attachments/{attachment_id}",
            "/api/widget/runs/{run_id}/stop",
            "/api/integrations/sessions/{session_id}/runs/{run_id}/stop",
            "/api/integrations/tool-requests/{tool_request_id}/result",
            "/api/admin/user-erasures",
            "/api/admin/users/{user_id}/erase",
            "/api/admin/runtime-enrollment-tokens",
            "/api/admin/runtime-enrollment-tokens/{enrollment_id}/revoke",
            "/api/admin/runtimes/{runtime_id}/credential-rotation",
            "/api/admin/runtimes/{runtime_id}/drain",
            "/api/admin/runtimes/{runtime_id}/cancel-drain",
            "/api/admin/runtimes/{runtime_id}",
            "/api/admin/runtimes/{runtime_id}/deletion-impact",
            "/api/admin/runtimes/{runtime_id}/force-delete",
            "/api/runtime/register",
            "/api/runtime/heartbeat",
            "/api/runtime/model-proxy/v1/responses",
            "/api/runtime/runs/claim",
            "/api/runtime/runs/{run_id}/turn/begin",
            "/api/runtime/sessions/{session_id}/commands/{command_id}/complete",
            "/api/runtime/sessions/{session_id}/release",
            "/api/runtime/sessions/{session_id}/checkpoint/begin",
            "/api/runtime/sessions/{session_id}/checkpoint/fail",
            "/api/runtime/sessions/{session_id}/bundle",
            "/api/runtime/sessions/{session_id}/salvage-bundle",
            "/api/runtime/attachments/{attachment_id}",
            "/api/runtime/sessions/{session_id}/salvage-abandon",
            "/api/runtime/runs/{run_id}/events",
            "/api/runtime/runs/{run_id}/tool-requests/finalize",
            "/api/runtime/runs/{run_id}/complete",
        ] {
            assert!(document["paths"].get(path).is_some(), "missing {path}");
        }
        assert_eq!(
            document["paths"]["/api/auth/login"]["post"]["security"],
            json!([])
        );
        assert_eq!(
            document["paths"]["/api/auth/providers"]["get"]["security"],
            json!([])
        );
        assert_eq!(
            document["paths"]["/api/agents"]["get"]["security"],
            json!([{ "sessionCookie": [] }, { "userBearer": [] }])
        );
        let model_connection_item =
            &document["paths"]["/api/model-connections/{model_connection_id}"];
        assert!(model_connection_item.get("put").is_some());
        assert!(model_connection_item.get("patch").is_none());
        assert!(
            document["components"]["schemas"]["ModelConnection"]["properties"]
                .get("api_key")
                .is_none()
        );
        assert_eq!(
            document["components"]["schemas"]["CreateModelConnectionRequest"]["properties"]
                ["api_key"]["writeOnly"],
            true
        );
        assert_eq!(
            document["components"]["schemas"]["UpdateModelConnectionRequest"]["properties"]
                ["api_key"]["writeOnly"],
            true
        );
        assert_eq!(
            document["components"]["schemas"]["ModelUpstreamProtocol"]["enum"],
            json!([
                "openai_responses",
                "openai_chat_completions",
                "anthropic_messages"
            ])
        );
        assert_eq!(
            document["components"]["schemas"]["ModelReasoningSummary"]["enum"],
            json!(["default", "auto", "concise", "detailed", "none"])
        );
        assert_eq!(
            document["components"]["schemas"]["ModelVerbosity"]["enum"],
            json!(["default", "low", "medium", "high"])
        );
        assert_eq!(
            document["components"]["schemas"]["ModelReasoningSummarySupport"]["enum"],
            json!(["auto", "supported", "unsupported"])
        );
        assert_eq!(
            document["components"]["schemas"]["ModelRequestSettings"]["oneOf"][2]["not"]
                ["required"],
            json!(["temperature", "top_p"])
        );
        let parameters = &document["components"]["schemas"]["AgentModelSettings"];
        assert_eq!(parameters["additionalProperties"], false);
        assert_eq!(
            parameters["properties"]["provider_request_timeout_ms"]["minimum"],
            1
        );
        assert_eq!(
            parameters["properties"]["stream_idle_timeout_ms"]["minimum"],
            1
        );
        for schema in [
            "ModelConnection",
            "CreateModelConnectionRequest",
            "UpdateModelConnectionRequest",
            "ModelConnectionOption",
            "ModelConnectionSnapshot",
        ] {
            assert_eq!(
                document["components"]["schemas"][schema]["properties"]["api_type"]["$ref"],
                "#/components/schemas/ModelUpstreamProtocol",
                "missing API type from {schema}"
            );
        }
        for schema in [
            "ModelConnection",
            "CreateModelConnectionRequest",
            "UpdateModelConnectionRequest",
        ] {
            assert!(document["components"]["schemas"][schema]["properties"]
                .get("allowed_model_ids")
                .is_some());
            assert!(document["components"]["schemas"][schema]["properties"]
                .get("parameters")
                .is_none());
            assert!(document["components"]["schemas"][schema]["properties"]
                .get("request_parameters")
                .is_none());
        }
        let execution_configuration =
            &document["components"]["schemas"]["AgentExecutionConfiguration"];
        assert_eq!(
            execution_configuration["properties"]["model_bindings"]["items"]["$ref"],
            "#/components/schemas/RunModelBinding"
        );
        assert!(execution_configuration["properties"]
            .get("default_model_connection_id")
            .is_none());
        assert_eq!(
            execution_configuration["properties"]["tool_allowlist"]["items"]["$ref"],
            "#/components/schemas/AgentToolName"
        );
        assert_eq!(
            document["components"]["schemas"]["Agent"]["properties"]["tool_allowlist"]["items"]
                ["$ref"],
            "#/components/schemas/AgentToolName"
        );
        let proxy_parameters = document["paths"]["/api/runtime/model-proxy/v1/responses"]["post"]
            ["parameters"]
            .as_array()
            .unwrap();
        assert!(proxy_parameters
            .iter()
            .any(|parameter| parameter["name"] == "x-agent-hub-model-binding-id"));
        assert!(!proxy_parameters
            .iter()
            .any(|parameter| parameter["name"] == "x-agent-hub-model-connection-id"));
        let update_agent = &document["components"]["schemas"]["UpdateAgentRequest"];
        assert_eq!(update_agent["additionalProperties"], false);
        assert!(update_agent["required"]
            .as_array()
            .is_some_and(|required| required.contains(&json!("runtime_id"))));
        assert!(update_agent["properties"].get("model_policy").is_none());
        assert_eq!(
            document["paths"]["/api/integrations/sessions"]["post"]["security"],
            json!([{ "integrationBearer": [] }])
        );
        assert_eq!(
            document["paths"]["/api/runtime/register"]["post"]["security"],
            json!([{ "runtimeEnrollmentBearer": [] }])
        );
        assert_eq!(
            document["paths"]["/api/runtime/heartbeat"]["post"]["security"],
            json!([{ "runtimeBearer": [] }])
        );
        let deletion_impact =
            &document["paths"]["/api/admin/runtimes/{runtime_id}/deletion-impact"]["get"];
        assert_eq!(
            deletion_impact["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeDeletionImpact"
        );
        for status in ["200", "403", "404"] {
            assert!(
                deletion_impact["responses"].get(status).is_some(),
                "missing Runtime deletion impact response {status}"
            );
        }
        assert_eq!(
            document["components"]["schemas"]["RuntimeDeletionImpactSession"]["properties"]
                ["force_delete_disposition"]["enum"],
            json!(["recoverable", "recovery_failed"])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeDeletionImpact"]["properties"]
                ["affected_sessions"]["items"]["$ref"],
            "#/components/schemas/RuntimeDeletionImpactSession"
        );
        assert_eq!(
            document["paths"]["/api/runtime/sessions/{session_id}/release"]["post"]["security"],
            json!([{ "runtimeBearer": [] }])
        );
        assert_eq!(
            document["paths"]["/api/runtime/sessions/{session_id}/checkpoint/begin"]["post"]
                ["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/BeginRuntimeSessionCheckpointRequest"
        );
        assert_eq!(
            document["paths"]["/api/runtime/sessions/{session_id}/checkpoint/fail"]["post"]
                ["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeSessionCheckpointDisposition"
        );
        assert_eq!(
            document["paths"]["/api/runtime/sessions/{session_id}/bundle"]["put"]["responses"]
                ["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeSessionBundleCommitResponse"
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeSessionCheckpointAttempt"]["required"],
            json!([
                "checkpoint_attempt_id",
                "history_checkpoint",
                "bundle_generation",
                "reason"
            ])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeOwnedSessionStateRequest"]["properties"]
                ["checkpoint_reason"]["enum"],
            json!(["idle", "drain", null])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeHeartbeatResponse"]["required"],
            json!([
                "rotation_requested",
                "pending_credential_accepted",
                "credential_activated",
                "runtime_status",
                "owned_sessions",
                "session_commands"
            ])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeSessionCommand"]["required"],
            json!([
                "command_id",
                "session_id",
                "ownership_generation",
                "command",
                "run_id",
                "turn_id",
                "native_session_id",
                "native_turn_id",
                "message",
                "configuration_revision",
                "fingerprint",
                "execution_configuration"
            ])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeSessionCommand"]["properties"]["command"]
                ["enum"],
            json!(["checkpoint", "steer", "interrupt", "refresh_configuration"])
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeHeartbeatRequest"]["properties"]
                ["accepts_session_commands"]["type"],
            "boolean"
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeHeartbeatRequest"]["properties"]
                ["cleaned_sessions"]["items"]["$ref"],
            "#/components/schemas/RuntimeOwnedSessionGeneration"
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeHeartbeatResponse"]["properties"]
                ["cleanup_sessions"]["items"]["$ref"],
            "#/components/schemas/RuntimeOwnedSessionGeneration"
        );
        assert!(
            !document["components"]["schemas"]["RuntimeHeartbeatResponse"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("cleanup_sessions"))
        );
        assert_eq!(
            document["paths"]["/api/admin/users/{user_id}/erase"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/EraseUserRequest"
        );
        assert_eq!(
            document["paths"]["/api/admin/user-erasures"]["get"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/UserErasure"
        );
        assert_eq!(
            document["components"]["schemas"]["EraseUserRequest"]["required"],
            json!(["email"])
        );
        assert_eq!(
            document["components"]["schemas"]["UserErasure"]["required"],
            json!(["user_id", "email", "status", "requested_at", "completed_at"])
        );
        assert!(
            document["components"]["schemas"]["CompleteRunRequest"]["properties"]["status"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("interrupted"))
        );
        assert!(document["components"]["schemas"]["Run"]["properties"]
            .get("native_session_id")
            .is_some());
        assert!(document["components"]["schemas"]["Run"]["properties"]
            .get("session_id")
            .is_none());
        assert_eq!(
            document["components"]["schemas"]["CompleteRunRequest"]["required"],
            json!(["status", "native_session_id", "work_dir_ref"])
        );
        assert_eq!(
            document["components"]["schemas"]["FinalizeToolRequestsRequest"]["required"],
            json!(["native_session_id", "work_dir_ref", "tool_requests"])
        );
        assert_eq!(
            document["components"]["schemas"]["WaitingToolRunTransition"]["required"],
            json!(["native_session_id", "work_dir_ref"])
        );
        assert_eq!(
            document["paths"]["/api/runtime/sessions/{session_id}/commands/{command_id}/complete"]
                ["post"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeCompleteSessionCommandRequest"
        );
        assert_eq!(
            document["paths"]["/api/widget/runs/{run_id}/stop"]["post"]["security"],
            json!([{ "embedToken": [] }])
        );
        assert_eq!(
            document["paths"]["/api/widget/access"]["post"]["security"],
            json!([{ "integrationClientBasic": [] }])
        );
        assert_eq!(
            document["paths"]["/api/widget/public/access"]["post"]["security"],
            json!([])
        );
        assert_eq!(
            document["paths"]["/api/widget/public/access"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/CreatePublicWidgetAccessRequest"
        );
        assert_eq!(
            document["paths"]["/api/widget/public/access"]["post"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/PublicWidgetAccessResponse"
        );
        assert!(
            document["components"]["schemas"]["PublicWidgetAccessResponse"]["properties"]
                .get("hub_session_id")
                .is_some()
        );
        assert_eq!(
            document["paths"]["/api/widget/session/renew"]["post"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/WidgetTokenResponse"
        );
        assert!(
            document["components"]["schemas"]["CreateWidgetRunRequest"]["properties"]
                .get("integration_session_id")
                .is_some()
        );
        assert_eq!(
            document["paths"]["/api/integrations/sessions/{session_id}/runs/{run_id}/stop"]["post"]
                ["security"],
            json!([{ "integrationBearer": [] }])
        );
        assert_eq!(
            document["paths"]["/api/runtime/runs/claim"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeClaimRunRequest"
        );
        assert_eq!(
            document["components"]["schemas"]["RuntimeClaimRunRequest"]["required"],
            json!(["available_new_session_slots", "ready_owned_sessions"])
        );
        assert_eq!(
            document["paths"]["/api/runtime/runs/{run_id}/turn/begin"]["post"]["security"],
            json!([{ "runtimeBearer": [] }])
        );
        assert_eq!(
            document["paths"]["/api/runtime/runs/{run_id}/turn/begin"]["post"]["requestBody"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RuntimeBeginTurnRequest"
        );
        assert_eq!(
            document["paths"]["/api/runtime/runs/{run_id}/turn/begin"]["post"]["responses"]["200"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/BeginRuntimeTurnResponse"
        );
        assert_eq!(
            document["components"]["schemas"]["BeginRuntimeTurnRequest"]["properties"]
                ["configuration_fingerprint"]["pattern"],
            "^sha256:[0-9a-f]{64}$"
        );
        let enrollment_list_schema = &document["components"]["schemas"]["RuntimeEnrollmentToken"];
        assert!(enrollment_list_schema["properties"].get("token").is_none());
        assert!(enrollment_list_schema["properties"]
            .get("token_hash")
            .is_none());
        assert_eq!(
            document["paths"]["/api/integrations/sessions/{session_id}/messages"]["post"]
                ["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/IntegrationMessageResponse"
        );
        assert_eq!(
            document["components"]["schemas"]["IntegrationMessageResponse"]["properties"]["run"]
                ["$ref"],
            "#/components/schemas/Run"
        );
        assert_eq!(
            document["components"]["schemas"]["IntegrationMessageResponse"]["properties"]
                ["message"]["$ref"],
            "#/components/schemas/HubSessionMessage"
        );
        assert_eq!(
            document["paths"]["/api/sessions/{session_id}/messages"]["post"]["responses"]["200"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SessionMessageAcceptance"
        );
        let session_origins = document["components"]["schemas"]["HubSessionOrigin"]["oneOf"]
            .as_array()
            .unwrap();
        assert!(session_origins
            .iter()
            .any(|origin| { origin["properties"]["kind"]["const"] == "public_widget" }));
        let external_origin = session_origins
            .iter()
            .find(|origin| origin["properties"]["kind"]["const"] == "external")
            .unwrap();
        assert!(!external_origin["required"]
            .as_array()
            .is_some_and(|required| required.contains(&json!("platform_name"))));
        assert!(document["components"]["schemas"]["HubSession"]["required"]
            .as_array()
            .is_some_and(|required| required.contains(&json!("origin_platform_name"))));
        assert!(
            document["paths"]["/api/agents/{agent_id}/runs"]["post"]["responses"]
                .get("409")
                .is_some()
        );
        assert!(
            document["paths"]["/api/sessions/{session_id}/messages"]["post"]["responses"]
                .get("409")
                .is_some()
        );
        assert!(
            document["paths"]["/api/runs/{run_id}/stop"]["post"]["responses"]
                .get("409")
                .is_some()
        );
        assert_eq!(
            document["paths"]["/api/integrations/sessions/{session_id}/messages"]["get"]
                ["responses"]["200"]["content"]["application/json"]["schema"]["items"]["$ref"],
            "#/components/schemas/HubSessionMessage"
        );
        assert_eq!(
            document["paths"]["/api/automations/webhook"]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/TriggerAutomationRequest"
        );
        let update_automation = &document["paths"]["/api/automations/{automation_id}"]["patch"];
        assert_eq!(
            update_automation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/UpdateAutomationRequest"
        );
        assert_eq!(
            update_automation["security"],
            json!([{ "sessionCookie": [] }, { "userBearer": [] }])
        );
        for status in ["200", "400", "401", "404"] {
            assert!(
                update_automation["responses"].get(status).is_some(),
                "missing PATCH Automation response {status}"
            );
        }

        let response = openapi().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&body).is_ok());
    }

    #[test]
    fn task_two_management_and_integration_routes_use_the_public_contract_paths() {
        let paths = &openapi_document()["paths"];
        for (path, methods) in [
            ("/api/integration-apps", &["get", "post"][..]),
            ("/api/integration-apps/{app_id}", &["get", "patch"][..]),
            (
                "/api/integration-apps/{app_id}/rotate-secret",
                &["post"][..],
            ),
            ("/api/oauth/userinfo", &["get"][..]),
            (
                "/api/admin/external-platforms/{platform_id}",
                &["patch"][..],
            ),
            ("/api/admin/users", &["get", "post"][..]),
            ("/api/admin/users/{user_id}", &["get", "patch"][..]),
            ("/api/admin/users/{user_id}/password", &["put"][..]),
            ("/api/admin/users/{user_id}/role", &["put"][..]),
            ("/api/skills", &["get", "post", "delete"][..]),
            ("/api/skills/{skill_id}/package", &["put", "delete"][..]),
            (
                "/api/runtime/runs/{run_id}/skills/{skill_id}/package",
                &["get"][..],
            ),
            (
                "/api/runtime/sessions/{session_id}/skills/{skill_id}/packages/{package_id}",
                &["get"][..],
            ),
        ] {
            for method in methods {
                assert!(
                    paths[path].get(*method).is_some(),
                    "missing {method} {path}"
                );
            }
        }
        assert!(paths.get("/api/agents/{agent_id}/oauth-app").is_none());
        assert!(paths
            .get("/api/agents/{agent_id}/oauth-app/rotate-secret")
            .is_none());
        assert_eq!(
            paths["/api/skills"]["delete"]["requestBody"]["content"]["application/json"]["schema"]
                ["$ref"],
            "#/components/schemas/BulkDeleteSkillsRequest"
        );
        assert_eq!(
            paths["/api/admin/users/{user_id}/password"]["put"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/AdminSetUserPasswordRequest"
        );
        assert_eq!(
            paths["/api/admin/users/{user_id}/role"]["put"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/AdminSetUserRoleRequest"
        );
        assert_eq!(
            openapi_document()["components"]["schemas"]["AgentExecutionSkill"]["properties"]
                ["source"]["enum"],
            json!(["managed"])
        );
        assert_eq!(
            openapi_document()["components"]["schemas"]["Skill"]["properties"]["package"]["anyOf"]
                [0]["$ref"],
            "#/components/schemas/SkillPackage"
        );
        assert_eq!(
            openapi_document()["components"]["schemas"]["AgentExecutionSkill"]["properties"]
                ["package"]["anyOf"][0]["$ref"],
            "#/components/schemas/SkillPackage"
        );
        assert!(
            openapi_document()["components"]["schemas"]["AgentToolName"]["enum"]
                .as_array()
                .unwrap()
                .contains(&json!("skill_exec"))
        );
        let execution_configuration =
            &openapi_document()["components"]["schemas"]["AgentExecutionConfiguration"];
        for property in [
            "model_selection",
            "model_settings",
            "subagents",
            "model_bindings",
            "tool_allowlist",
        ] {
            assert!(
                execution_configuration["properties"]
                    .get(property)
                    .is_some(),
                "missing AgentExecutionConfiguration property {property}"
            );
        }
        for legacy_property in [
            "default_model_connection_id",
            "reasoning_effort",
            "model_connections",
        ] {
            assert!(
                execution_configuration["properties"]
                    .get(legacy_property)
                    .is_none(),
                "legacy AgentExecutionConfiguration property {legacy_property} remains"
            );
        }
    }

    #[test]
    fn task_five_a_openapi_documents_options_and_browser_widget_issuance() {
        let document = openapi_document();
        assert_eq!(
            document["paths"]["/api/integration-app-options"]["get"]["responses"]["200"]["content"]
                ["application/json"]["schema"]["$ref"],
            "#/components/schemas/IntegrationAppOptions"
        );
        let widget = &document["paths"]
            ["/api/integration-apps/{app_id}/agents/{agent_id}/widget-session"]["post"];
        assert_eq!(widget["security"], json!([{ "sessionCookie": [] }]));
        assert_eq!(
            widget["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/TokenResponse"
        );
        assert_eq!(
            document["components"]["schemas"]["IntegrationAppOptions"]["required"],
            json!(["external_platforms", "authentication_channels"])
        );
    }

    #[tokio::test]
    async fn every_openapi_operation_is_registered_by_the_real_router() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(20))
            .connect_lazy("postgres://agent-hub:agent-hub@127.0.0.1:1/agent_hub")
            .unwrap();
        let app = build_router(test_state_with_pool(pool));
        let uuid = Uuid::nil().to_string();
        for (path, operations) in openapi_document()["paths"].as_object().unwrap() {
            for method in operations.as_object().unwrap().keys() {
                let uri = path
                    .split('/')
                    .map(|segment| {
                        if segment.starts_with('{') {
                            uuid.as_str()
                        } else {
                            segment
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("/");
                let request = axum::http::Request::builder()
                    .method(method.to_ascii_uppercase().as_str())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap();
                let response = app.clone().oneshot(request).await.unwrap();
                assert_ne!(
                    response.status(),
                    StatusCode::NOT_FOUND,
                    "missing {method} {path}"
                );
                assert_ne!(
                    response.status(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "wrong method {method} {path}"
                );
            }
        }
    }

    #[test]
    fn openapi_covers_all_user_auth_router_paths_and_serialized_contracts() {
        let document = openapi_document();
        for path in [
            "/api/auth/register",
            "/api/auth/login",
            "/api/auth/ldap/login",
            "/api/auth/logout",
            "/api/auth/me",
            "/api/users/me",
            "/api/auth/providers",
            "/api/auth/api-keys",
            "/api/auth/api-keys/{api_key_id}",
            "/api/auth/api-keys/{api_key_id}/renew",
            "/api/admin/auth-policy",
            "/api/admin/ldap-config",
            "/api/admin/ldap-config/test",
            "/api/admin/users",
            "/api/admin/users/{user_id}",
            "/api/admin/users/{user_id}/password",
            "/api/admin/users/{user_id}/role",
            "/api/admin/users/{user_id}/erase",
            "/api/admin/user-erasures",
            "/api/admin/external-platforms",
            "/api/admin/external-platforms/{platform_id}",
            "/api/admin/external-platforms/{platform_id}/authentication-channels",
            "/api/admin/authentication-channels/{channel_id}",
        ] {
            assert!(
                document["paths"].get(path).is_some(),
                "undocumented auth route {path}"
            );
        }
        assert_eq!(
            document["components"]["schemas"]["User"]["properties"]["email"]["type"],
            json!("string")
        );
        assert!(document["components"]["schemas"]["User"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("email")));
        assert!(document["components"]["schemas"]["User"]["properties"]
            .get("username")
            .is_none());
        assert!(document["paths"].get("/api/auth/oidc/mock/start").is_none());
        assert!(document["paths"]
            .get("/api/auth/oidc/mock/callback")
            .is_none());
        assert!(
            document["components"]["schemas"]["AuthProvidersResponse"]["properties"]
                .get("oidc_mock")
                .is_none()
        );
        assert!(
            document["components"]["schemas"]["AuthProvidersResponse"]["properties"]
                .get("email_verification_required")
                .is_none()
        );
        for path in ["/api/auth/register", "/api/auth/login"] {
            assert!(document["paths"][path]["post"]["responses"]
                .get("403")
                .is_some());
        }

        let now = Utc::now();
        let run = RunDto {
            id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            automation_id: None,
            integration_session_id: None,
            parent_run_id: None,
            runtime_id: None,
            hub_session_id: None,
            hub_message_id: None,
            hub_turn_id: None,
            session_ownership_generation: None,
            status: "pending".into(),
            initial_message: "go".into(),
            native_session_id: None,
            work_dir_ref: None,
            source: "integration:message".into(),
            created_at: now,
            updated_at: now,
        };
        let message = HubSessionMessageDto {
            id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 1,
            role: "user".into(),
            message_kind: "message".into(),
            content: Some("go".into()),
            payload: json!({}),
            attachments: Vec::new(),
            delivery_mode: "next_turn".into(),
            delivery_state: "queued".into(),
            client_message_key: Some("message-1".into()),
            expected_native_turn_id: None,
            turn_id: None,
            run_id: Some(run.id),
            accepted_at: now,
        };
        let serialized = serde_json::to_value(IntegrationMessageResponse { run, message }).unwrap();
        assert_eq!(
            serialized.as_object().unwrap().keys().collect::<Vec<_>>(),
            vec!["message", "run"]
        );
        assert!(serialized["run"]
            .as_object()
            .unwrap()
            .contains_key("automation_id"));
        assert!(serialized["run"]["automation_id"].is_null());
        assert_eq!(serialized["message"]["client_message_key"], "message-1");
        for field in [
            "hub_session_id",
            "hub_message_id",
            "hub_turn_id",
            "session_ownership_generation",
        ] {
            assert!(
                document["components"]["schemas"]["Run"]["properties"]
                    .get(field)
                    .is_some(),
                "Run schema is missing {field}"
            );
        }
        for field in [
            "external_platform_id",
            "authentication_channel_id",
            "login_required",
            "allowed_origins",
            "tool_allowlist",
        ] {
            assert!(
                document["components"]["schemas"]["IntegrationApp"]["properties"]
                    .get(field)
                    .is_some(),
                "IntegrationApp schema is missing {field}"
            );
        }
        assert!(
            document["components"]["schemas"]["CreateRunRequest"]["properties"]
                .get("hub_session_id")
                .is_some()
        );
        let automation_runs = &document["paths"]["/api/automations/{automation_id}/runs"]["get"];
        assert_eq!(
            automation_runs["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/RunListResponse"
        );
        assert!(automation_runs["responses"].get("400").is_some());
        assert!(automation_runs["responses"].get("404").is_some());
        assert_eq!(
            serde_json::to_value(TriggerAutomationRequest {
                message: Some("go".into())
            })
            .unwrap(),
            json!({ "message": "go" })
        );
        assert_eq!(
            serde_json::to_value(UpdateAutomationRequest {
                name: "Nightly review".into(),
                trigger_type: "cron".into(),
                prompt: "Review production alerts".into(),
                schedule: Some("0 2 * * *".into()),
                enabled: false,
            })
            .unwrap(),
            json!({
                "name": "Nightly review",
                "trigger_type": "cron",
                "prompt": "Review production alerts",
                "schedule": "0 2 * * *",
                "enabled": false
            })
        );
    }

    #[test]
    fn extracts_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer runtime-token"),
        );
        assert_eq!(bearer_token(&headers), Some("runtime-token".to_string()));
    }

    #[test]
    fn scoped_tokens_are_read_from_headers_without_using_urls() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-agent-hub-embed-token",
            HeaderValue::from_static("embed-secret"),
        );
        assert_eq!(
            embed_token_from_headers(&headers).as_deref(),
            Some("embed-secret")
        );

        headers.clear();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Webhook webhook-secret"),
        );
        assert_eq!(
            webhook_token_from_headers(&headers).as_deref(),
            Some("webhook-secret")
        );

        headers.clear();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer ahe_embed-secret"),
        );
        assert_eq!(
            client_access_token_from_headers(&headers).as_deref(),
            Some("ahe_embed-secret")
        );

        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer unrelated-secret"),
        );
        assert_eq!(client_access_token_from_headers(&headers), None);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn super_admin_can_read_another_users_run_but_member_cannot(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let super_admin = test_user(Uuid::new_v4(), "super_admin");
        let member = test_user(Uuid::new_v4(), "member");

        assert_eq!(
            load_run_for_user(&fixture.state.pool, fixture.run_id, &super_admin)
                .await
                .unwrap()
                .id,
            fixture.run_id
        );
        assert_eq!(
            load_run_for_user(&fixture.state.pool, fixture.run_id, &member)
                .await
                .unwrap_err()
                .status,
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn non_owner_agent_response_hides_configuration_except_from_administrators() {
        let mut agent = test_agent();
        agent.runtime_id = Some(Uuid::new_v4());
        agent.model_policy = json!({ "provider": "hub-proxy" });
        agent.mcp_allowlist = json!([{ "name": "private-mcp", "secrets": { "TOKEN": "x" } }]);
        agent.visibility = "public".into();
        let member = apply_agent_access(agent.clone(), &test_user(Uuid::new_v4(), "member"));
        assert!(member.can_invoke);
        assert!(!member.can_manage);
        assert!(member.runtime_id.is_none());
        assert_eq!(member.model_policy, json!({}));
        assert_eq!(member.mcp_allowlist, json!([]));

        for role in ["admin", "super_admin"] {
            let visible = apply_agent_access(agent.clone(), &test_user(Uuid::new_v4(), role));
            assert!(visible.can_manage);
            assert!(visible.can_administer);
            assert!(visible.runtime_id.is_some());
            assert_eq!(visible.model_policy, json!({ "provider": "hub-proxy" }));
            assert_eq!(
                visible.mcp_allowlist[0]["secrets"]["TOKEN"],
                REDACTED_SECRET
            );
        }
    }

    #[test]
    fn mcp_secrets_are_redacted_and_preserved() {
        let existing = json!([
            {
                "name": "filesystem",
                "command": "fs",
                "secrets": { "API_TOKEN": "secret-token" }
            }
        ]);
        let redacted = redact_mcp_secrets(&existing);

        assert_eq!(
            redacted[0]["secrets"]["API_TOKEN"].as_str(),
            Some(REDACTED_SECRET)
        );

        let incoming = json!([
            {
                "name": "filesystem",
                "command": "fs",
                "secrets": { "API_TOKEN": REDACTED_SECRET }
            }
        ]);
        let merged = merge_mcp_secrets(&existing, &incoming);

        assert_eq!(
            merged[0]["secrets"]["API_TOKEN"].as_str(),
            Some("secret-token")
        );
    }

    #[test]
    fn mcp_validation_rejects_placeholder_without_existing_secret() {
        let existing = json!([]);
        let incoming = json!([
            {
                "name": "filesystem",
                "command": "fs",
                "secrets": { "API_TOKEN": REDACTED_SECRET }
            }
        ]);
        let mut req = test_update_agent_request();
        req.mcp_allowlist = merge_mcp_secrets(&existing, &incoming);

        assert!(validate_agent_payload(&req).is_err());
    }

    #[test]
    fn mcp_validation_rejects_ambiguous_or_unredacted_fields() {
        let mut duplicate = test_update_agent_request();
        duplicate.mcp_allowlist = json!([
            { "name": "filesystem", "command": "fs" },
            { "name": "filesystem", "command": "fs-alt" }
        ]);
        assert!(validate_agent_payload(&duplicate).is_err());

        let mut unsupported = test_update_agent_request();
        unsupported.mcp_allowlist = json!([
            { "name": "github", "command": "gh-mcp", "env": { "TOKEN": "secret" } }
        ]);
        assert!(validate_agent_payload(&unsupported).is_err());
    }

    #[test]
    fn interval_schedule_validates_and_detects_due_automation() {
        assert_eq!(
            parse_interval_schedule("2s").unwrap(),
            ChronoDuration::seconds(2)
        );
        assert_eq!(
            parse_interval_schedule("5m").unwrap(),
            ChronoDuration::minutes(5)
        );
        assert_eq!(
            parse_interval_schedule("1h").unwrap(),
            ChronoDuration::hours(1)
        );
        assert!(parse_interval_schedule("0s").is_err());
        assert!(parse_interval_schedule("5d").is_err());
        assert!(parse_interval_schedule("9223372036854775807h").is_err());

        let created_at = DateTime::parse_from_rfc3339("2026-07-10T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-07-10T12:00:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut automation = test_automation("interval", Some("2s"), created_at);

        assert!(scheduled_automation_due(&automation, now));

        automation.last_triggered_at = Some(now);
        assert!(!scheduled_automation_due(&automation, now));
    }

    #[test]
    fn cron_schedule_matches_once_per_minute() {
        assert!(validate_cron_schedule("* * * * *").is_ok());
        assert!(validate_cron_schedule("* * * * 7").is_ok());
        assert!(validate_cron_schedule("70 * * * *").is_err());
        let sunday = DateTime::parse_from_rfc3339("2026-07-12T12:34:10Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(cron_schedule_matches("34 12 * * 7", sunday));

        let now = DateTime::parse_from_rfc3339("2026-07-10T12:34:10Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut automation = test_automation("cron", Some("34 12 * * *"), now);

        assert!(scheduled_automation_due(&automation, now));

        automation.last_triggered_at = Some(now - ChronoDuration::seconds(5));
        assert!(!scheduled_automation_due(&automation, now));

        automation.last_triggered_at = Some(now - ChronoDuration::minutes(1));
        assert!(scheduled_automation_due(&automation, now));
    }

    #[test]
    fn disabled_scheduled_automation_is_not_due() {
        let now = DateTime::parse_from_rfc3339("2026-07-10T12:00:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut automation =
            test_automation("interval", Some("2s"), now - ChronoDuration::seconds(10));
        automation.enabled = false;

        assert!(!scheduled_automation_due(&automation, now));
    }

    #[test]
    fn legacy_password_formats_verify_only_for_migration() {
        let salt = "legacy-salt";
        let stored = format!("sha256:{salt}:{}", sha256_hex(&format!("{salt}:password")));

        assert!(verify_password(&stored, "password"));
        assert!(password_needs_upgrade(&stored));
        assert!(verify_password("plain-password", "plain-password"));
        assert!(password_needs_upgrade("plain-password"));
    }

    #[test]
    fn jwt_audience_accepts_string_or_array() {
        assert!(jwt_audience_matches(
            Some(&json!("agent-hub-widget")),
            "agent-hub-widget"
        ));
        assert!(jwt_audience_matches(
            Some(&json!(["other", "agent-hub-widget"])),
            "agent-hub-widget"
        ));
        assert!(!jwt_audience_matches(
            Some(&json!("other")),
            "agent-hub-widget"
        ));
    }

    #[test]
    fn constant_time_eq_checks_full_value() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }

    #[test]
    fn trusted_proxy_cidr_configuration_treats_empty_as_unconfigured() {
        assert_eq!(parse_trusted_proxy_cidrs(None).unwrap(), None);
        assert_eq!(parse_trusted_proxy_cidrs(Some("   ")).unwrap(), None);
        assert_eq!(
            parse_trusted_proxy_cidrs(Some("192.0.2.0/24, 2001:db8::/32"))
                .unwrap()
                .unwrap(),
            vec![
                "192.0.2.0/24".parse::<IpNet>().unwrap(),
                "2001:db8::/32".parse::<IpNet>().unwrap(),
            ]
        );
        assert!(parse_trusted_proxy_cidrs(Some("not-a-cidr")).is_err());
    }

    #[test]
    fn uploaded_skill_markdown_uses_yaml_metadata_and_body() {
        let parsed = parse_uploaded_skill_markdown(
            b"---\nname: deploy-client\ndescription: Deploy through the approved client\nmetadata:\n  owner: platform\n---\n\n# Instructions\n\nRun the client.\n",
        )
        .unwrap();
        assert_eq!(parsed.0, "deploy-client");
        assert_eq!(parsed.1, "Deploy through the approved client");
        assert_eq!(parsed.2, "# Instructions\n\nRun the client.");
        assert!(parse_uploaded_skill_markdown(b"not frontmatter").is_err());
        assert!(parse_uploaded_skill_markdown(b"---\nname: empty\n---\n").is_err());
        assert!(parse_uploaded_skill_markdown(&[0xff, 0xfe]).is_err());
    }

    #[tokio::test]
    async fn readiness_is_unavailable_when_postgres_cannot_be_reached() {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(50))
            .connect_lazy("postgres://agent-hub:agent-hub@127.0.0.1:1/agent_hub")
            .unwrap();

        let response = readiness_response(&pool, Duration::from_millis(100)).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, Bytes::from_static(br#"{"ok":false}"#));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn update_agent_omitted_secret_declarations_preserves_and_empty_array_clears(
        pool: PgPool,
    ) {
        let token = create_user_session_with_role(&pool, "member").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let declarations = vec![AgentSecretDeclarationDto {
            name: "API_KEY".into(),
            kind: "value".into(),
            description: "Test key".into(),
        }];
        let agent = create_agent(
            State(state.clone()),
            session_headers(&token),
            Json(CreateAgentRequest {
                name: "Secret Declarations Agent".into(),
                instructions: "Keep declarations".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
                model_selection: None,
                model_settings: Some(AgentModelSettings::default()),
                subagents: Vec::new(),
                secret_declarations: Some(declarations.clone()),
                tool_allowlist: default_agent_tool_allowlist(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(agent.secret_declarations, declarations);

        let mut preserve = update_request_from_agent(&agent);
        preserve.secret_declarations = None;
        let preserved = update_agent(
            State(state.clone()),
            session_headers(&token),
            Path(agent.id),
            Json(preserve),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(preserved.secret_declarations, declarations);

        let mut clear = update_request_from_agent(&preserved);
        clear.secret_declarations = Some(Vec::new());
        let cleared = update_agent(
            State(state.clone()),
            session_headers(&token),
            Path(agent.id),
            Json(clear),
        )
        .await
        .unwrap()
        .0;
        assert!(cleared.secret_declarations.is_empty());
    }

    #[sqlx::test]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn readiness_succeeds_against_an_isolated_postgres_database(pool: PgPool) {
        let response = readiness_response(&pool, Duration::from_millis(500)).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, Bytes::from_static(br#"{"ok":true}"#));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn browser_app_owner_widget_session_enforces_ownership_delegation_and_invalidation(
        pool: PgPool,
    ) {
        let agent_owner = create_hub_user(
            &pool,
            Some("widget-browser-agent-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let app_owner = create_hub_user(
            &pool,
            Some("widget-browser-app-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        assert_eq!(app_owner.role, "member");
        let app_owner_token = "widget-browser-app-owner-session";
        let non_owner_token = "widget-browser-non-owner-session";
        for (token, user_id) in [
            (app_owner_token, app_owner.id),
            (non_owner_token, agent_owner.id),
        ] {
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
        let delegated_agent_id = Uuid::new_v4();
        let undelegated_agent_id = Uuid::new_v4();
        for (id, name) in [
            (delegated_agent_id, "Delegated Widget Agent"),
            (undelegated_agent_id, "Undelegated Widget Agent"),
        ] {
            sqlx::query(
                "INSERT INTO agents
                     (id, owner_id, name, instructions, visibility, public_to)
                 VALUES ($1, $2, $3, 'test', 'public_to', $4)",
            )
            .bind(id)
            .bind(agent_owner.id)
            .bind(name)
            .bind(vec![app_owner.id])
            .execute(&pool)
            .await
            .unwrap();
        }
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'widget-browser', 'Widget Browser')",
        )
        .bind(platform_id)
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
        .bind(agent_owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let app = create_integration_app(
            State(state.clone()),
            session_headers(app_owner_token),
            Json(CreateIntegrationAppRequest {
                name: "Widget Browser App".into(),
                external_platform_id: platform_id,
                authentication_channel_id: channel_id,
                redirect_uris: json!(["https://widget.example.com/callback"]),
                agent_ids: vec![delegated_agent_id],
                widget_history_enabled: false,
                login_required: true,
                allowed_origins: Vec::new(),
                tool_allowlist: Some(vec!["read".into(), "grep".into()]),
                client_tool_definitions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0
        .integration_app;
        let non_owner = create_integration_app_widget_session(
            State(state.clone()),
            session_headers(non_owner_token),
            Path((app.id, delegated_agent_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(non_owner.status, StatusCode::NOT_FOUND);
        let undelegated = create_integration_app_widget_session(
            State(state.clone()),
            session_headers(app_owner_token),
            Path((app.id, undelegated_agent_id)),
        )
        .await
        .unwrap_err();
        assert_eq!(undelegated.status, StatusCode::FORBIDDEN);
        let issued_at = Utc::now();
        let widget_token = create_integration_app_widget_session(
            State(state.clone()),
            session_headers(app_owner_token),
            Path((app.id, delegated_agent_id)),
        )
        .await
        .unwrap()
        .0
        .token;
        assert!(widget_token.starts_with("ahe_"));
        let persisted: (Option<Uuid>, Uuid, Uuid, DateTime<Utc>, Uuid) = sqlx::query_as(
            "SELECT oauth_app_id, agent_id, owner_id, expires_at, hub_session_id
             FROM embed_sessions WHERE token_hash = $1",
        )
        .bind(sha256_hex(&widget_token))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(persisted.0, Some(app.id));
        assert_eq!(persisted.1, delegated_agent_id);
        assert_eq!(persisted.2, agent_owner.id);
        assert!(persisted.3 >= issued_at + ChronoDuration::minutes(59));
        assert!(persisted.3 <= Utc::now() + ChronoDuration::minutes(61));
        let mut configuration_tx = pool.begin().await.unwrap();
        let mut configuration =
            load_agent_execution_configuration_tx(&mut configuration_tx, delegated_agent_id)
                .await
                .unwrap();
        apply_session_tool_policy_to_configuration_tx(
            &mut configuration_tx,
            persisted.4,
            &mut configuration,
        )
        .await
        .unwrap();
        configuration_tx.rollback().await.unwrap();
        assert_eq!(configuration.tool_allowlist, vec!["read", "grep"]);
        let mut widget_headers = HeaderMap::new();
        widget_headers.insert(
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderValue::from_str(&widget_token).unwrap(),
        );
        let widget_response = get_widget_session(State(state.clone()), widget_headers.clone())
            .await
            .unwrap();
        let widget: WidgetAgentDto = serde_json::from_slice(
            &axum::body::to_bytes(widget_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(widget.id, delegated_agent_id);
        sqlx::query("DELETE FROM integration_app_agents WHERE app_id = $1 AND agent_id = $2")
            .bind(app.id)
            .bind(delegated_agent_id)
            .execute(&pool)
            .await
            .unwrap();
        let revoked = get_widget_session(State(state), widget_headers)
            .await
            .unwrap_err();
        assert_eq!(revoked.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn authorization_code_cannot_exchange_a_widget_session(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("widget-authorization-code@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        let platform_id = Uuid::new_v4();
        let channel_id = Uuid::new_v4();
        let identity_id = Uuid::new_v4();
        let app_id = Uuid::new_v4();
        let access_token = "aho_widget_authorization_code";
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Authorization Code Widget Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'widget-authorization-code', 'Widget Authorization Code')",
        )
        .bind(platform_id)
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
                  external_platform_id, authentication_channel_id)
             VALUES ($1, $2, 'Authorization Code App', $3, 'unused', '[]'::jsonb, $4, $5)",
        )
        .bind(app_id)
        .bind(owner.id)
        .bind(format!("widget-auth-code-{}", Uuid::new_v4().simple()))
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
            "INSERT INTO external_identities
                 (id, platform_id, tenant_id, external_user_id, user_id,
                  authentication_channel_id)
             VALUES ($1, $2, 'default', 'widget-auth-code-user', $3, $4)",
        )
        .bind(identity_id)
        .bind(platform_id)
        .bind(owner.id)
        .bind(channel_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO oauth_access_tokens
                 (id, oauth_app_id, token_hash, expires_at, grant_type,
                  subject_user_id, scopes, origin_tenant_id, origin_external_identity_id)
             VALUES ($1, $2, $3, now() + interval '1 hour', 'authorization_code',
                     $4, $5, 'default', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(app_id)
        .bind(sha256_hex(access_token))
        .bind(owner.id)
        .bind(vec![format!("agent:{agent_id}")])
        .bind(identity_id)
        .execute(&pool)
        .await
        .unwrap();
        let error = create_integration_embed_session(
            State(Arc::new(test_state_with_pool(pool.clone()))),
            bearer_headers(access_token),
            Json(CreateEmbedSessionRequest { agent_id }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM embed_sessions WHERE oauth_app_id = $1",
            )
            .bind(app_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
    }

    pub(crate) async fn issue_widget_external_access_for(
        fixture: &WidgetExternalTestFixture,
        client_id: &str,
        client_secret: &str,
        agent_id: Uuid,
        tenant_id: &str,
        external_user_id: &str,
        display_name: &str,
    ) -> WidgetAccessResponse {
        let basic = STANDARD.encode(format!("{client_id}:{client_secret}"));
        let response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/access")
                    .header(header::AUTHORIZATION, format!("Basic {basic}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "agent_id": agent_id,
                            "client_instance_id": fixture.client_instance_id,
                            "tenant_id": tenant_id,
                            "external_user_id": external_user_id,
                            "username": format!("{external_user_id}-name"),
                            "display_name": display_name,
                            "email": format!("{external_user_id}@example.com"),
                            "attributes": { "fixture_version": display_name }
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap()
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn empty_client_grants_do_not_require_the_agent_integration_gate(pool: PgPool) {
        let fixture = widget_external_test_fixture(pool, true).await;
        sqlx::query(
            "UPDATE agents SET tool_allowlist = tool_allowlist - 'integration' WHERE id = $1",
        )
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let empty_access = issue_client_access_for_instance(
            &fixture,
            Uuid::new_v4(),
            "empty-grant-tenant",
            "empty-grant-user",
            json!([]),
        )
        .await;
        let list_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/client/sessions")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", empty_access.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);

        let basic = STANDARD.encode(format!("{}:{}", fixture.client_id, fixture.client_secret));
        let nonempty_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/access")
                    .header(header::AUTHORIZATION, format!("Basic {basic}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "agent_id": fixture.agent_id,
                            "client_instance_id": Uuid::new_v4(),
                            "tenant_id": "nonempty-grant-tenant",
                            "external_user_id": "nonempty-grant-user",
                            "email": "nonempty-grant-user@example.com",
                            "client_tools": test_client_tool_definitions(&["blocked_tool"])
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(nonempty_response.status(), StatusCode::FORBIDDEN);

        let anonymous_origin = "https://anonymous-empty.example.test";
        sqlx::query(
            "UPDATE oauth_apps
             SET login_required = false, widget_history_enabled = false,
                 allowed_origins = $1, client_tool_definitions = '[]'::jsonb
             WHERE id = $2",
        )
        .bind(json!([anonymous_origin]))
        .bind(fixture.app_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let anonymous_response = anonymous_client_access_response(
            &fixture,
            &format!("visitor-key-{}", Uuid::new_v4().simple()),
            Uuid::new_v4(),
            None,
            Some(anonymous_origin),
        )
        .await;
        assert_eq!(anonymous_response.status(), StatusCode::OK);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn anonymous_client_tool_continuation_resumes_native_session_with_run_snapshot(
        pool: PgPool,
    ) {
        let app = widget_external_test_fixture(pool, false).await;
        let origin = "http://anonymous-client-tool.example.test";
        let definitions = test_client_tool_definitions(&["show_notice"]);
        sqlx::query(
            "UPDATE oauth_apps
             SET login_required = false, widget_history_enabled = false,
                 allowed_origins = $1, client_tool_definitions = $2
             WHERE id = $3",
        )
        .bind(json!([origin]))
        .bind(&definitions)
        .bind(app.app_id)
        .execute(&app.state.pool)
        .await
        .unwrap();
        let access_response = anonymous_client_access_response(
            &app,
            &format!("visitor-key-{}", Uuid::new_v4().simple()),
            Uuid::new_v4(),
            None,
            Some(origin),
        )
        .await;
        assert_eq!(access_response.status(), StatusCode::OK);
        let access: ClientAccessResponse = serde_json::from_slice(
            &axum::body::to_bytes(access_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let run_response = app
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", access.access_token),
                    )
                    .header(header::ORIGIN, origin)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"show an anonymous notice","client_message_key":"anonymous-client-tool"}"#,
                    ))
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
        assert!(run.integration_session_id.is_none());
        let runtime_token = bind_client_tool_test_run_to_runtime(&app, &run).await;
        let tool_call_id = Uuid::new_v4();
        let fixture = ClientToolRunTestFixture {
            app,
            executor: access.clone(),
            observer: access,
            run,
            runtime_token,
            tool_call_ids: vec![tool_call_id],
        };
        let waiting = finalize_test_client_tool_batch(&fixture, &["show_notice"])
            .await
            .unwrap()
            .0;
        assert_eq!(waiting.status, "waiting_tool");
        assert!(sqlx::query_scalar::<_, Option<Uuid>>(
            "SELECT session_id FROM integration_tool_requests WHERE id = $1",
        )
        .bind(tool_call_id)
        .fetch_one(&fixture.app.state.pool)
        .await
        .unwrap()
        .is_none());

        let mut client_headers = bearer_headers(&fixture.executor.access_token);
        client_headers.insert(header::ORIGIN, HeaderValue::from_static(origin));
        let claim = claim_client_tool_call(
            State(fixture.app.state.clone()),
            client_headers.clone(),
            Path(tool_call_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(claim.status, "claimed");
        let submitted = submit_client_tool_result(
            State(fixture.app.state.clone()),
            client_headers.clone(),
            Path(tool_call_id),
            Json(SubmitClientToolResultRequest {
                result: ClientToolResultDto::Success {
                    output: json!({ "visible": true }),
                },
            }),
        )
        .await
        .unwrap()
        .0;
        let continuation = submitted.run.unwrap();
        assert!(continuation.integration_session_id.is_none());

        let mut events_tx = fixture.app.state.pool.begin().await.unwrap();
        let credential = load_widget_credential_tx(
            &mut events_tx,
            &fixture.executor.access_token,
            &client_headers,
        )
        .await
        .unwrap();
        let scoped = load_widget_scoped_session_tx(
            &mut events_tx,
            &credential,
            None,
            continuation.hub_session_id,
            false,
        )
        .await
        .unwrap();
        let events = load_widget_session_events_after_tx(&mut events_tx, &scoped, 0, None)
            .await
            .unwrap();
        events_tx.commit().await.unwrap();
        assert!(events.iter().any(|event| event.run_id == continuation.id));

        let claimed = claim_runtime_run(&fixture.app.state, &fixture.runtime_token).await;
        assert_eq!(claimed.run.id, continuation.id);
        assert_eq!(
            claimed.resume.unwrap().native_session_id,
            "client-tool-native-session"
        );
        let context = claimed.integration_context.unwrap();
        assert_eq!(context.tools, definitions);
        assert_eq!(context.tool_results.len(), 1);
        assert_eq!(context.tool_results[0].tool_call_id, tool_call_id);
        assert_eq!(context.tool_results[0].tool_name, "show_notice");
        assert_eq!(
            context.tool_results[0].result,
            ClientToolResultDto::Success {
                output: json!({ "visible": true })
            }
        );
        assert!(context.external_user.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn canonical_client_access_rotates_one_instance_without_invalidating_other_tabs_or_runs(
        pool: PgPool,
    ) {
        let fixture = widget_external_test_fixture(pool, true).await;
        let allowed_origin = "http://client.example.test";
        sqlx::query("UPDATE oauth_apps SET allowed_origins = $1 WHERE id = $2")
            .bind(json!([allowed_origin]))
            .bind(fixture.app_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let first_instance = Uuid::new_v4();
        let second_instance = Uuid::new_v4();
        let first = issue_client_access_for_instance(
            &fixture,
            first_instance,
            "tenant-client",
            "client-user",
            json!([{
                "name": "open_panel",
                "description": "Open one panel",
                "input_schema": { "type": "object" }
            }]),
        )
        .await;
        assert_eq!(first.client_instance_id, first_instance);
        assert_eq!(first.tool_names, vec!["open_panel"]);
        assert_eq!(first.expires_in, CLIENT_ACCESS_TTL_SECONDS);
        let credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&first.access_token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();

        let run_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", first.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"keep this run","client_message_key":"client-run"}"#,
                    ))
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

        let reauthorized = issue_client_access_for_instance(
            &fixture,
            first_instance,
            "tenant-client",
            "client-user",
            json!([{
                "name": "select_row",
                "description": "Select one row",
                "input_schema": { "type": "object" }
            }]),
        )
        .await;
        assert_ne!(reauthorized.access_token, first.access_token);
        assert_eq!(reauthorized.tool_names, vec!["select_row"]);
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&reauthorized.access_token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            credential_id
        );
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>("SELECT widget_session_id FROM runs WHERE id = $1")
                .bind(run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            credential_id
        );

        let second = issue_client_access_for_instance(
            &fixture,
            second_instance,
            "tenant-client",
            "client-user",
            json!([]),
        )
        .await;
        let second_credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&second.access_token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_ne!(second_credential_id, credential_id);

        for origin in [None, Some("http://wrong.example.test")] {
            let mut request = axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/client/renew")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {}", reauthorized.access_token),
                )
                .header(header::CONTENT_TYPE, "application/json");
            if let Some(origin) = origin {
                request = request.header(header::ORIGIN, origin);
            }
            let response = fixture
                .router
                .clone()
                .oneshot(request.body(Body::from("{}")).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }

        let renewed_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/renew")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", reauthorized.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(renewed_response.status(), StatusCode::OK);
        let renewed: ClientAccessResponse = serde_json::from_slice(
            &axum::body::to_bytes(renewed_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(renewed.client_instance_id, first_instance);
        assert_eq!(renewed.tool_names, vec!["select_row"]);

        let replaced = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/session")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", reauthorized.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replaced.status(), StatusCode::UNAUTHORIZED);
        for token in [&renewed.access_token, &second.access_token] {
            let response = fixture
                .router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/api/widget/session")
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .header(header::ORIGIN, allowed_origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM embed_sessions
                 WHERE oauth_app_id = $1 AND external_identity_id IS NOT NULL",
            )
            .bind(fixture.app_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn canonical_client_sessions_keep_two_turns_in_order_and_steer_without_rebinding(
        pool: PgPool,
    ) {
        let fixture = widget_external_test_fixture(pool, true).await;
        let first_instance_id = Uuid::new_v4();
        let second_instance_id = Uuid::new_v4();
        let first = issue_client_access_for_instance(
            &fixture,
            first_instance_id,
            "tenant-session",
            "session-user",
            test_client_tool_definitions(&["first_tab_action"]),
        )
        .await;
        let second = issue_client_access_for_instance(
            &fixture,
            second_instance_id,
            "tenant-session",
            "session-user",
            test_client_tool_definitions(&["second_tab_action"]),
        )
        .await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM hub_sessions")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );

        let first_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", first.access_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"first message","client_message_key":"client-first"}"#,
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
        let session_id = first_run.integration_session_id.unwrap();
        let hub_session_id = first_run.hub_session_id.unwrap();
        let turn_id = first_run.hub_turn_id.unwrap();
        let first_credential_id: Uuid =
            sqlx::query_scalar("SELECT id FROM embed_sessions WHERE token_hash = $1")
                .bind(sha256_hex(&first.access_token))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        sqlx::query("UPDATE runs SET status = 'running' WHERE id = $1")
            .bind(first_run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns
             SET status = 'running', native_turn_id = 'native-client-turn'
             WHERE id = $1",
        )
        .bind(turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET active_turn_id = $1, lifecycle_status = 'online'
             WHERE id = $2",
        )
        .bind(turn_id)
        .bind(hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let second_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "message": "steer this turn",
                            "session_id": session_id,
                            "client_message_key": "client-steer"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second_run.id, first_run.id);
        let steering_message = sqlx::query(
            "SELECT sequence, delivery_mode, expected_native_turn_id
             FROM hub_session_messages
             WHERE session_id = $1 AND client_message_key = 'client-steer'",
        )
        .bind(hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(steering_message.get::<String, _>("delivery_mode"), "steer");
        assert_eq!(
            steering_message
                .get::<Option<String>, _>("expected_native_turn_id")
                .as_deref(),
            Some("native-client-turn")
        );
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>("SELECT widget_session_id FROM runs WHERE id = $1")
                .bind(first_run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            first_credential_id
        );
        let run_tool_binding: (Uuid, Value) = sqlx::query_as(
            "SELECT client_instance_id, client_tool_snapshot FROM runs WHERE id = $1",
        )
        .bind(first_run.id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(run_tool_binding.0, first_instance_id);
        assert_eq!(
            run_tool_binding.1,
            test_client_tool_definitions(&["first_tab_action"])
        );

        let retry_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "message": "steer this turn",
                            "session_id": session_id,
                            "client_message_key": "client-steer"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_response.status(), StatusCode::OK);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM hub_session_messages WHERE session_id = $1",
            )
            .bind(hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );

        let sessions_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/client/sessions")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sessions_response.status(), StatusCode::OK);
        let sessions: Vec<WidgetHistorySessionDto> = serde_json::from_slice(
            &axum::body::to_bytes(sessions_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, session_id);

        let latest_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/client/sessions/{session_id}/messages?limit=1"
                    ))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(latest_response.status(), StatusCode::OK);
        let latest: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(latest_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].content.as_deref(), Some("steer this turn"));
        let older_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/client/sessions/{session_id}/messages?before_sequence={}&limit=1",
                        latest[0].sequence
                    ))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(older_response.status(), StatusCode::OK);
        let older: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(older_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(older[0].content.as_deref(), Some("first message"));

        let other_identity = issue_client_access_for_instance(
            &fixture,
            Uuid::new_v4(),
            "tenant-session",
            "other-session-user",
            json!([]),
        )
        .await;
        let cross_identity_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/client/sessions/{session_id}/messages"))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", other_identity.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_identity_response.status(), StatusCode::NOT_FOUND);

        let first_event_sequence: i64 =
            sqlx::query_scalar("SELECT min(seq) FROM run_events WHERE run_id = $1")
                .bind(first_run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let events_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/client/sessions/{session_id}/events?after={first_event_sequence}"
                    ))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
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
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].content.as_deref(), Some("steer this turn"));

        let stream_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/api/client/sessions/{session_id}/events/stream?after={first_event_sequence}"
                    ))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stream_response.status(), StatusCode::OK);
        assert!(stream_response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream"));

        let stop_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/client/runs/{}/stop", first_run.id))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stop_response.status(), StatusCode::OK);
        assert!(sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT interrupt_requested_at FROM hub_session_turns WHERE id = $1",
        )
        .bind(turn_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap()
        .is_some());

        sqlx::query("UPDATE oauth_apps SET widget_history_enabled = false WHERE id = $1")
            .bind(fixture.app_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let hidden_list = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/client/sessions")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hidden_list.status(), StatusCode::FORBIDDEN);
        let exact_after_hide = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/client/sessions/{session_id}/messages"))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(exact_after_hide.status(), StatusCode::OK);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn anonymous_client_access_requires_origin_and_keeps_tabs_independent(pool: PgPool) {
        let fixture = widget_external_test_fixture(pool, false).await;
        let allowed_origin = "http://public.example.test";
        sqlx::query(
            "UPDATE oauth_apps
             SET login_required = false, allowed_origins = $1,
                 client_tool_definitions = $2
             WHERE id = $3",
        )
        .bind(json!([allowed_origin]))
        .bind(json!([{
            "name": "show_article",
            "description": "Show one article",
            "input_schema": { "type": "object" }
        }]))
        .bind(fixture.app_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let visitor_key = format!("anonymous-client-{}", Uuid::new_v4().simple());
        let first_instance = Uuid::new_v4();
        let second_instance = Uuid::new_v4();

        for origin in [None, Some("http://wrong.example.test")] {
            let response = anonymous_client_access_response(
                &fixture,
                &visitor_key,
                first_instance,
                None,
                origin,
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let first_response = anonymous_client_access_response(
            &fixture,
            &visitor_key,
            first_instance,
            None,
            Some(allowed_origin),
        )
        .await;
        assert_eq!(first_response.status(), StatusCode::OK);
        let first: ClientAccessResponse = serde_json::from_slice(
            &axum::body::to_bytes(first_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.client_instance_id, first_instance);
        assert_eq!(first.tool_names, vec!["show_article"]);
        assert!(first.session_id.is_none());

        let run_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", first.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"message":"anonymous first message"}"#))
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
        let session_id = run.hub_session_id.unwrap();

        let second_response = anonymous_client_access_response(
            &fixture,
            &visitor_key,
            second_instance,
            Some(session_id),
            Some(allowed_origin),
        )
        .await;
        assert_eq!(second_response.status(), StatusCode::OK);
        let second: ClientAccessResponse = serde_json::from_slice(
            &axum::body::to_bytes(second_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(second.client_instance_id, second_instance);
        assert_eq!(second.session_id, Some(session_id));
        assert_ne!(second.access_token, first.access_token);
        let observed_events = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/client/sessions/{session_id}/events"))
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", second.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observed_events.status(), StatusCode::OK);
        let observed_events: Vec<RunEventDto> = serde_json::from_slice(
            &axum::body::to_bytes(observed_events.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(observed_events.len(), 1);
        assert_eq!(
            observed_events[0].content.as_deref(),
            Some("anonymous first message")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM embed_sessions
                 WHERE oauth_app_id = $1 AND anonymous_key_hash = $2",
            )
            .bind(fixture.app_id)
            .bind(visitor_key_hash(&visitor_key).unwrap())
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );

        let invalid_recovery = anonymous_client_access_response(
            &fixture,
            &visitor_key,
            Uuid::new_v4(),
            Some(Uuid::new_v4()),
            Some(allowed_origin),
        )
        .await;
        assert_eq!(invalid_recovery.status(), StatusCode::NOT_FOUND);

        let renewed_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/renew")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", first.access_token),
                    )
                    .header(header::ORIGIN, allowed_origin)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(renewed_response.status(), StatusCode::OK);
        let renewed: ClientAccessResponse = serde_json::from_slice(
            &axum::body::to_bytes(renewed_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(renewed.session_id, Some(session_id));

        for token in [&renewed.access_token, &second.access_token] {
            let response = fixture
                .router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(format!("/api/widget/sessions/{session_id}/messages"))
                        .header(header::AUTHORIZATION, format!("Bearer {token}"))
                        .header(header::ORIGIN, allowed_origin)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn public_widget_access_rotates_visitor_credential_and_restores_its_session(
        pool: PgPool,
    ) {
        let fixture = widget_external_test_fixture(pool, false).await;
        sqlx::query(
            "UPDATE oauth_apps
             SET login_required = false,
                 allowed_origins = '[\"https://docs.example.test\"]'::jsonb,
                 tool_allowlist = '[\"read\",\"grep\"]'::jsonb
             WHERE id = $1",
        )
        .bind(fixture.app_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let visitor_key = format!("public-visitor-{}", Uuid::new_v4().simple());
        let first = issue_public_widget_access(&fixture, &visitor_key).await;
        assert!(first.token.starts_with("ahp_"));
        assert_eq!(first.agent.id, fixture.agent_id);
        assert!(first.hub_session_id.is_none());

        let rotated = issue_public_widget_access(&fixture, &visitor_key).await;
        assert_eq!(rotated.widget_session_id, first.widget_session_id);
        assert!(rotated.hub_session_id.is_none());
        assert_ne!(rotated.token, first.token);

        let replaced_token = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/session")
                    .header("x-agent-hub-embed-token", &first.token)
                    .header(header::ORIGIN, "https://docs.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replaced_token.status(), StatusCode::UNAUTHORIZED);

        let run_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &rotated.token)
                    .header(header::ORIGIN, "https://docs.example.test")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"public widget hello","client_message_key":"public-first"}"#,
                    ))
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
        let hub_session_id = run.hub_session_id.unwrap();
        assert!(run.integration_session_id.is_none());

        let refreshed = issue_public_widget_access(&fixture, &visitor_key).await;
        assert_eq!(refreshed.widget_session_id, first.widget_session_id);
        assert_eq!(refreshed.hub_session_id, Some(hub_session_id));
        assert_ne!(refreshed.token, rotated.token);

        let retry_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &refreshed.token)
                    .header(header::ORIGIN, "https://docs.example.test")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"public widget hello","client_message_key":"public-first"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_response.status(), StatusCode::OK);
        let retried_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(retry_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retried_run.id, run.id);

        let history_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/widget/sessions")
                    .header("x-agent-hub-embed-token", &refreshed.token)
                    .header(header::ORIGIN, "https://docs.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(history_response.status(), StatusCode::FORBIDDEN);

        let transcript_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/widget/sessions/{hub_session_id}/messages"))
                    .header("x-agent-hub-embed-token", &refreshed.token)
                    .header(header::ORIGIN, "https://docs.example.test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(transcript_response.status(), StatusCode::OK);
        let messages: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(transcript_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content.as_deref(), Some("public widget hello"));

        let mut tx = fixture.state.pool.begin().await.unwrap();
        let session = load_hub_session_tx(&mut tx, hub_session_id).await.unwrap();
        tx.rollback().await.unwrap();
        assert!(matches!(session.origin, HubSessionOriginDto::PublicWidget));
        assert_eq!(session.origin_platform_name, None);
        assert_eq!(
            sqlx::query_as::<_, (Option<Uuid>, Option<Uuid>, bool)>(
                "SELECT hub_session_id, last_run_id, anonymous
                 FROM embed_sessions WHERE id = $1",
            )
            .bind(first.widget_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (Some(hub_session_id), Some(run.id), true)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runs WHERE widget_session_id = $1",)
                .bind(first.widget_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            1
        );

        let other_visitor = issue_public_widget_access(
            &fixture,
            &format!("other-public-visitor-{}", Uuid::new_v4().simple()),
        )
        .await;
        assert_ne!(other_visitor.widget_session_id, first.widget_session_id);
        assert!(other_visitor.hub_session_id.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn client_attachments_upload_download_and_auth_gate(pool: PgPool) {
        let fixture = attachment_fixture(pool).await;
        let (_, store, server) = attachment_object_store().await;
        let mut state = (*fixture.state).clone();
        state.session_bundle_store = Some(Arc::new(store));
        let state = Arc::new(state);
        let router = build_router((*state).clone());

        let widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO embed_sessions
                 (id, token_hash, agent_id, owner_id, expires_at, hub_session_id)
             VALUES ($1, $2, $3, $4, now() + interval '1 hour', $5)",
        )
        .bind(Uuid::new_v4())
        .bind(sha256_hex(&widget_token))
        .bind(fixture.agent_id)
        .bind(fixture.owner_id)
        .bind(fixture.session_id)
        .execute(&state.pool)
        .await
        .unwrap();

        let multipart = ("--test-boundary\r\n\
             Content-Disposition: form-data; name=\"session_id\"\r\n\r\n"
            .to_owned()
            + &fixture.session_id.to_string()
            + "\r\n\
             --test-boundary\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"hello.txt\"\r\n\
             Content-Type: text/plain\r\n\r\n\
             hello world\n\r\n\
             --test-boundary--\r\n")
        .into_bytes();

        // 无凭据上传被拒绝。
        let unauthorized = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/attachments")
                    .header(header::CONTENT_TYPE, "multipart/form-data; boundary=test-boundary")
                    .body(Body::from(multipart.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        // 带凭据上传成功，且走的是 /api/client/* 前缀（client 路由别名）。
        let upload = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/client/attachments")
                    .header("x-agent-hub-embed-token", &widget_token)
                    .header(header::CONTENT_TYPE, "multipart/form-data; boundary=test-boundary")
                    .body(Body::from(multipart))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload.status(), StatusCode::OK);
        let attachment: HubSessionAttachmentDto = serde_json::from_slice(
            &axum::body::to_bytes(upload.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(attachment.name, "hello.txt");
        assert_eq!(attachment.content_type, "text/plain");
        assert_eq!(attachment.size_bytes, 12);

        // 附件归属到凭据作用域内的会话。
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT session_id FROM hub_session_attachments WHERE id = $1",
            )
            .bind(attachment.id)
            .fetch_one(&state.pool)
            .await
            .unwrap(),
            Some(fixture.session_id)
        );

        // 凭据可下载原文件。
        let download = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::GET)
                    .uri(format!("/api/client/attachments/{}", attachment.id))
                    .header("x-agent-hub-embed-token", &widget_token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(download.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(download.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"hello world\n");
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn first_external_widget_message_creates_the_scoped_session_and_runtime_user_context(
        pool: PgPool,
    ) {
        let fixture = widget_external_test_fixture(pool, false).await;
        let issued = issue_widget_external_access(
            &fixture,
            "tenant-runtime",
            "runtime-user",
            "Runtime User",
        )
        .await;
        let response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"first external widget message","client_message_key":"widget-first"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let retry_response = fixture
            .router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/widget/runs")
                    .header("x-agent-hub-embed-token", &issued.token)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"message":"first external widget message","client_message_key":"widget-first"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry_response.status(), StatusCode::OK);
        let retried_run: RunDto = serde_json::from_slice(
            &axum::body::to_bytes(retry_response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(retried_run.id, run.id);
        assert_eq!(
            retried_run.integration_session_id,
            run.integration_session_id
        );
        assert_eq!(retried_run.hub_session_id, run.hub_session_id);
        let integration_session_id = run.integration_session_id.unwrap();
        let hub_session_id = run.hub_session_id.unwrap();
        let scoped = sqlx::query(
            "SELECT integration.oauth_app_id, integration.agent_id, integration.owner_id,
                    integration.external_user_id, integration.hub_session_id,
                    hub.origin_kind, hub.origin_platform_id, hub.origin_tenant_id,
                    hub.origin_external_identity_id
             FROM integration_sessions AS integration
             JOIN hub_sessions AS hub ON hub.id = integration.hub_session_id
             WHERE integration.id = $1",
        )
        .bind(integration_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(scoped.get::<Uuid, _>("oauth_app_id"), fixture.app_id);
        assert_eq!(scoped.get::<Uuid, _>("agent_id"), fixture.agent_id);
        assert_eq!(scoped.get::<Uuid, _>("hub_session_id"), hub_session_id);
        assert_eq!(scoped.get::<String, _>("external_user_id"), "runtime-user");
        assert_eq!(scoped.get::<String, _>("origin_kind"), "external");
        assert_eq!(
            scoped.get::<Option<Uuid>, _>("origin_platform_id"),
            Some(fixture.platform_id)
        );
        assert_eq!(
            scoped
                .get::<Option<String>, _>("origin_tenant_id")
                .as_deref(),
            Some("tenant-runtime")
        );
        let run_context: Value =
            sqlx::query_scalar("SELECT external_user_context FROM runs WHERE id = $1")
                .bind(run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_eq!(run_context["display_name"], "Runtime User");
        assert_eq!(run_context["attributes"]["fixture_version"], "Runtime User");
        let mut tx = fixture.state.pool.begin().await.unwrap();
        let context = load_integration_context_for_run(&mut tx, &run)
            .await
            .unwrap()
            .unwrap();
        tx.rollback().await.unwrap();
        let external_user = context.external_user.unwrap();
        assert_eq!(external_user.external_user_id, "runtime-user");
        assert_eq!(external_user.tenant_id, "tenant-runtime");
        assert_eq!(external_user.display_name.as_deref(), Some("Runtime User"));
        for (table, expected) in [
            ("hub_sessions", 1_i64),
            ("integration_sessions", 1_i64),
            ("runs", 1_i64),
            ("hub_session_messages", 1_i64),
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
            assert_eq!(count, expected, "unexpected {table} rows after retry");
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn hub_session_list_orders_by_creation_time(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("session-order-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let session_token = "session-order-owner-token";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(session_token))
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Session Order Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let oldest_id = Uuid::new_v4();
        let middle_id = Uuid::new_v4();
        let newest_id = Uuid::new_v4();
        for (id, created_at, updated_at) in [
            (oldest_id, "2026-07-17T08:00:00Z", "2026-07-17T14:00:00Z"),
            (newest_id, "2026-07-17T12:00:00Z", "2026-07-17T09:00:00Z"),
            (middle_id, "2026-07-17T10:00:00Z", "2026-07-17T13:00:00Z"),
        ] {
            sqlx::query(
                "INSERT INTO hub_sessions
                     (id, owner_id, agent_id, origin_kind, lifecycle_status,
                      created_at, updated_at)
                 VALUES ($1, $2, $3, 'hub_native', 'offline', $4, $5)",
            )
            .bind(id)
            .bind(owner.id)
            .bind(agent_id)
            .bind(created_at.parse::<DateTime<Utc>>().unwrap())
            .bind(updated_at.parse::<DateTime<Utc>>().unwrap())
            .execute(&pool)
            .await
            .unwrap();
        }

        let app = build_router(test_state_with_browser_session_auth(pool));
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/sessions")
                    .header(header::COOKIE, format!("agent_hub_session={session_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let sessions: Vec<HubSessionDto> = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            sessions
                .into_iter()
                .map(|session| session.id)
                .collect::<Vec<_>>(),
            vec![newest_id, middle_id, oldest_id]
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archive_holding_agent_lock_finishes_before_runtime_claim_without_deadlock(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
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
        let automation_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO automations
                 (id, agent_id, owner_id, name, trigger_type, prompt, enabled)
             VALUES ($1, $2, $3, 'claim archive gate', 'manual', 'test', true)",
        )
        .bind(automation_id)
        .bind(fixture.agent_id)
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let mut gate_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM automations WHERE id = $1 FOR UPDATE")
            .bind(automation_id)
            .fetch_one(&mut *gate_tx)
            .await
            .unwrap();

        let archive_application = format!("claim-archive-{}", Uuid::new_v4().simple());
        let archive_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.state.pool, &archive_application)
                .await,
        ));
        let agent_id = fixture.agent_id;
        let archive_headers = session_headers(&session_token);
        let mut archive_task = tokio::spawn(async move {
            delete_agent(State(archive_state), archive_headers, Path(agent_id)).await
        });
        assert!(
            wait_for_application_lock(
                &fixture.state.pool,
                &archive_application,
                "DELETE FROM automations",
            )
            .await,
            "archive must hold the Agent lock while waiting on the Automation gate"
        );

        let claim_application = format!("archive-claim-{}", Uuid::new_v4().simple());
        let claim_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &claim_application).await,
        ));
        let runtime_token = fixture.runtime_token.clone();
        let mut claim_task = tokio::spawn(async move {
            runtime_claim_run(
                State(claim_state),
                bearer_headers(&runtime_token),
                runtime_claim_request(1, Vec::new()),
            )
            .await
            .map(|response| response.into_response().status())
        });
        assert!(
            wait_for_application_lock(
                &fixture.state.pool,
                &claim_application,
                "SELECT a.id AS a_id",
            )
            .await,
            "runtime claim must wait for the Agent before locking its Run and Session"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut claim_task)
                .await
                .is_err(),
            "runtime claim must remain pending while archive holds the Agent lock"
        );

        gate_tx.commit().await.unwrap();
        let archive_status = tokio::time::timeout(Duration::from_secs(3), &mut archive_task)
            .await
            .expect("archive should not deadlock")
            .expect("archive task should not panic")
            .expect("archive should complete normally");
        let claim_status = tokio::time::timeout(Duration::from_secs(3), &mut claim_task)
            .await
            .expect("runtime claim should not deadlock")
            .expect("runtime claim task should not panic")
            .expect("runtime claim should complete normally");

        assert_eq!(archive_status, StatusCode::NO_CONTENT);
        assert_eq!(claim_status, StatusCode::NO_CONTENT);
        assert_eq!(
            runtime_claim_run_state(&fixture.state.pool, fixture.run_id).await,
            ("failed".into(), None, None)
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn deleting_agent_locks_runtime_before_owned_session_without_heartbeat_deadlock(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
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
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online'
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let mut heartbeat_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM runtimes WHERE id = $1 FOR UPDATE")
            .bind(fixture.runtime_id)
            .fetch_one(&mut *heartbeat_tx)
            .await
            .unwrap();

        let delete_application = format!("agent-delete-runtime-{}", Uuid::new_v4().simple());
        let delete_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.state.pool, &delete_application)
                .await,
        ));
        let agent_id = fixture.agent_id;
        let delete_headers = session_headers(&session_token);
        let mut delete_task = tokio::spawn(async move {
            delete_agent(State(delete_state), delete_headers, Path(agent_id)).await
        });
        assert!(
            wait_for_application_lock(&fixture.state.pool, &delete_application, "").await,
            "Agent deletion must wait while heartbeat holds the Runtime lock"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut delete_task)
                .await
                .is_err(),
            "Agent deletion must remain pending until heartbeat releases the Runtime"
        );

        tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::query("UPDATE hub_sessions SET updated_at = now() WHERE id = $1")
                .bind(fixture.hub_session_id)
                .execute(&mut *heartbeat_tx),
        )
        .await
        .expect("heartbeat Session update should not deadlock with Agent deletion")
        .expect("heartbeat Session update should succeed");
        heartbeat_tx.commit().await.unwrap();

        let deleted = tokio::time::timeout(Duration::from_secs(3), &mut delete_task)
            .await
            .expect("Agent deletion should not deadlock with heartbeat")
            .expect("Agent deletion task should not panic")
            .expect("Agent deletion should complete normally");
        assert_eq!(deleted, StatusCode::NO_CONTENT);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn managed_skill_only_update_changes_next_claim_fingerprint(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let skill_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             SELECT $1, owner_id, 'review', 'review', 'first content', $2
             FROM agents WHERE id = $3",
        )
        .bind(skill_id)
        .bind(sha256_hex("first content"))
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

        let first = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(first.execution_configuration.revision, 1);
        assert_eq!(first.execution_configuration.skills[0].revision, 1);
        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(first.run.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE hub_session_turns SET status = 'completed' WHERE id = $1")
            .bind(first.run.hub_turn_id.unwrap())
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE skills
             SET content = 'second content', revision = revision + 1,
                 content_checksum_sha256 = $1
             WHERE id = $2",
        )
        .bind(sha256_hex("second content"))
        .bind(skill_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let next_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;

        let second = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        assert_eq!(second.run.id, next_run_id);
        assert_eq!(second.execution_configuration.revision, 1);
        assert_eq!(second.execution_configuration.skills[0].revision, 2);
        assert_ne!(
            second.expected_configuration_fingerprint,
            first.expected_configuration_fingerprint
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn hub_stop_allows_native_owner_but_rejects_external_session_in_console(pool: PgPool) {
        let fixture =
            runtime_claim_fixture(pool.clone(), "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = $2,
                 native_session_id = 'hub-stop-thread'
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
             SET native_turn_id = 'hub-stop-turn', status = 'running',
                 ownership_generation = 1
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs
             SET runtime_id = $1, status = 'running', session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let other = create_hub_user(
            &fixture.state.pool,
            Some("hub-stop-other@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        for (token, user_id) in [("hub-stop-owner", owner_id), ("hub-stop-other", other.id)] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(user_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        let app = build_router(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let stop_request = |token: &str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!("/api/runs/{}/stop", fixture.run_id))
                .header(header::COOKIE, format!("agent_hub_session={token}"))
                .body(Body::empty())
                .unwrap()
        };

        let forbidden = app
            .clone()
            .oneshot(stop_request("hub-stop-other"))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);
        let stopped = app
            .clone()
            .oneshot(stop_request("hub-stop-owner"))
            .await
            .unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);

        let external = integration_runtime_fixture(pool).await;
        let external_owner_id: Uuid =
            sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
                .bind(external.hub_session_id)
                .fetch_one(&external.state.pool)
                .await
                .unwrap();
        let external_owner_token = "hub-stop-external-owner";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(external_owner_token))
        .bind(external_owner_id)
        .execute(&external.state.pool)
        .await
        .unwrap();
        let external_stop_request = |token: &str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!("/api/runs/{}/stop", external.run_id))
                .header(header::COOKIE, format!("agent_hub_session={token}"))
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone()
                .oneshot(external_stop_request("hub-stop-other"))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let rejected = app
            .oneshot(external_stop_request(external_owner_token))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(rejected.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rejected["error"],
            "External Sessions are read-only in the Hub console"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn hub_console_rejects_messages_for_external_sessions(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let visibility: String = sqlx::query_scalar("SELECT visibility FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        assert_eq!(visibility, "private");
        let owner_token = "hub-console-external-message-owner";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(owner_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let app = build_router(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));

        let get_session = axum::http::Request::builder()
            .uri(format!("/api/sessions/{}", fixture.hub_session_id))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .body(Body::empty())
            .unwrap();
        let visible = app.clone().oneshot(get_session).await.unwrap();
        assert_eq!(visible.status(), StatusCode::OK);
        let visible: HubSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(visible.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            visible.origin,
            HubSessionOriginDto::External { .. }
        ));
        assert_eq!(
            visible.origin_platform_name.as_deref(),
            Some(fixture.platform_name.as_str())
        );

        let list_messages = || {
            axum::http::Request::builder()
                .uri(format!("/api/sessions/{}/messages", fixture.hub_session_id))
                .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
                .body(Body::empty())
                .unwrap()
        };
        let before = app.clone().oneshot(list_messages()).await.unwrap();
        assert_eq!(before.status(), StatusCode::OK);
        let before: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(before.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        let send = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/sessions/{}/messages", fixture.hub_session_id))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"content":"must stay external-only"}"#))
            .unwrap();
        let rejected = app.clone().oneshot(send).await.unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(rejected.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rejected["error"],
            "External Sessions are read-only in the Hub console"
        );

        let parent_continue = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/agents/{}/runs", fixture.agent_id))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"parent must stay external-only","parent_run_id":"{}"}}"#,
                fixture.run_id
            )))
            .unwrap();
        let rejected = app.clone().oneshot(parent_continue).await.unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(rejected.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rejected["error"],
            "External Sessions are read-only in the Hub console"
        );

        let legacy_continue = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!("/api/agents/{}/runs", fixture.agent_id))
            .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(format!(
                r#"{{"message":"must also stay external-only","hub_session_id":"{}"}}"#,
                fixture.hub_session_id
            )))
            .unwrap();
        let rejected = app.clone().oneshot(legacy_continue).await.unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(rejected.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            rejected["error"],
            "External Sessions are read-only in the Hub console"
        );

        let after = app.clone().oneshot(list_messages()).await.unwrap();
        assert_eq!(after.status(), StatusCode::OK);
        let after: Vec<HubSessionMessageDto> = serde_json::from_slice(
            &axum::body::to_bytes(after.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(after.len(), before.len());

        let external_continue = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/integrations/sessions/{}/messages",
                fixture.session_id
            ))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", fixture.integration_token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"content":"continue through the trusted integration","attachments":[],"client_message_key":"external-continue"}"#,
            ))
            .unwrap();
        let continued = app.clone().oneshot(external_continue).await.unwrap();
        assert_eq!(continued.status(), StatusCode::OK);
        let continued: IntegrationMessageResponse = serde_json::from_slice(
            &axum::body::to_bytes(continued.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();

        let external_stop = axum::http::Request::builder()
            .method(Method::POST)
            .uri(format!(
                "/api/integrations/sessions/{}/runs/{}/stop",
                fixture.session_id, continued.run.id
            ))
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", fixture.integration_token),
            )
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(external_stop).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn hub_owner_deletes_own_ended_session_with_references_and_isolation(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("hub-delete-owner@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        let other = create_hub_user(
            &pool,
            Some("hub-delete-other@example.com"),
            None,
            Some("password-hash"),
            true,
        )
        .await
        .unwrap();
        for (token, user_id) in [
            ("hub-delete-owner", owner.id),
            ("hub-delete-other", other.id),
        ] {
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
             VALUES ($1, $2, 'Delete Agent', 'test', 'private')",
        )
        .bind(agent_id)
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();

        let ended_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'offline')",
        )
        .bind(ended_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
        let turn_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'completed', 0)",
        )
        .bind(turn_id)
        .bind(ended_session_id)
        .execute(&pool)
        .await
        .unwrap();
        let run_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  hub_session_id, hub_turn_id, session_ownership_generation)
             VALUES ($1, $2, $3, 'completed', 'hello', 'console', $4, $5, 0)",
        )
        .bind(run_id)
        .bind(agent_id)
        .bind(owner.id)
        .bind(ended_session_id)
        .bind(turn_id)
        .execute(&pool)
        .await
        .unwrap();
        let message_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'hello', 'next_turn',
                     'delivered', $3, $4)",
        )
        .bind(message_id)
        .bind(ended_session_id)
        .bind(turn_id)
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_tool_requests
                 (id, session_id, hub_session_id, run_id, tool_name, status, expires_at)
             VALUES ($1, NULL, $2, $3, 'example_tool', 'completed',
                     now() + interval '1 hour')",
        )
        .bind(Uuid::new_v4())
        .bind(ended_session_id)
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO embed_sessions
                 (token_hash, agent_id, owner_id, expires_at, hub_session_id)
             VALUES ($1, $2, $3, now() + interval '1 hour', $4)",
        )
        .bind(sha256_hex("hub-delete-embed-token"))
        .bind(agent_id)
        .bind(owner.id)
        .bind(ended_session_id)
        .execute(&pool)
        .await
        .unwrap();

        let other_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'offline')",
        )
        .bind(other_session_id)
        .bind(other.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();

        let active_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status)
             VALUES ($1, $2, $3, 'hub_native', 'online')",
        )
        .bind(active_session_id)
        .bind(owner.id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();

        let app = build_router(test_state_with_browser_session_auth(pool.clone()));
        let delete_request = |token: &str, session_id: Uuid| {
            axum::http::Request::builder()
                .method(Method::DELETE)
                .uri(format!("/api/sessions/{session_id}"))
                .header(header::COOKIE, format!("agent_hub_session={token}"))
                .body(Body::empty())
                .unwrap()
        };
        let get_request = |token: &str, session_id: Uuid| {
            axum::http::Request::builder()
                .uri(format!("/api/sessions/{session_id}"))
                .header(header::COOKIE, format!("agent_hub_session={token}"))
                .body(Body::empty())
                .unwrap()
        };

        let forbidden = app
            .clone()
            .oneshot(delete_request("hub-delete-owner", other_session_id))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM hub_sessions WHERE id = $1)"
        )
        .bind(other_session_id)
        .fetch_one(&pool)
        .await
        .unwrap());

        let rejected = app
            .clone()
            .oneshot(delete_request("hub-delete-owner", active_session_id))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::CONFLICT);

        let rename_request = |token: &str, session_id: Uuid, title: &str| {
            axum::http::Request::builder()
                .method(Method::PUT)
                .uri(format!("/api/sessions/{session_id}/title"))
                .header(header::COOKIE, format!("agent_hub_session={token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(r#"{{"title":"{title}"}}"#)))
                .unwrap()
        };
        let renamed = app
            .clone()
            .oneshot(rename_request(
                "hub-delete-owner",
                ended_session_id,
                "Troubleshoot login",
            ))
            .await
            .unwrap();
        assert_eq!(renamed.status(), StatusCode::OK);
        let renamed: HubSessionDto = serde_json::from_slice(
            &axum::body::to_bytes(renamed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(renamed.title.as_deref(), Some("Troubleshoot login"));
        let empty_title = app
            .clone()
            .oneshot(rename_request("hub-delete-owner", ended_session_id, "  "))
            .await
            .unwrap();
        assert_eq!(empty_title.status(), StatusCode::BAD_REQUEST);

        let deleted = app
            .clone()
            .oneshot(delete_request("hub-delete-owner", ended_session_id))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        for (table, column, id) in [
            ("hub_sessions", "id", ended_session_id),
            ("hub_session_turns", "session_id", ended_session_id),
            ("hub_session_messages", "session_id", ended_session_id),
            ("runs", "hub_session_id", ended_session_id),
            ("embed_sessions", "hub_session_id", ended_session_id),
        ] {
            let exists: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} = $1)"
            ))
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(!exists, "{table} should have been removed");
        }
        let after = app
            .oneshot(get_request("hub-delete-owner", ended_session_id))
            .await
            .unwrap();
        assert_eq!(after.status(), StatusCode::NOT_FOUND);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn stop_during_turn_start_waits_for_native_binding_before_interrupt_delivery(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let owner_token = format!("hub-start-stop-{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&owner_token))
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = $2,
                 native_session_id = NULL
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
             SET native_turn_id = NULL, status = 'starting', ownership_generation = 1
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE runs
             SET runtime_id = $1, status = 'running', session_ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let app = build_router(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let stopped = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri(format!("/api/runs/{}/stop", fixture.run_id))
                    .header(header::COOKIE, format!("agent_hub_session={owner_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stopped.status(), StatusCode::OK);
        let accepted = accept_test_session_message(
            &fixture.state.pool,
            fixture.hub_session_id,
            fixture.agent_id,
            owner_id,
            "continue while the stopped Turn is still binding",
            Some("starting-stop-next-turn"),
            "next_turn",
        )
        .await
        .unwrap();
        let next_run = accepted
            .run
            .expect("stopped Turn must route to a pending Run");
        assert_ne!(next_run.id, fixture.run_id);
        assert_ne!(accepted.message.turn_id, Some(fixture.turn_id));
        assert_eq!(accepted.message.delivery_mode, "next_turn");
        assert_eq!(accepted.message.delivery_state, "queued");
        let heartbeat_request = RuntimeHeartbeatRequest {
            accepts_session_commands: true,
            ..RuntimeHeartbeatRequest::default()
        };
        let before_binding = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request.clone()),
        )
        .await
        .unwrap()
        .0;
        assert!(before_binding
            .session_commands
            .iter()
            .all(|command| command.command != "interrupt"));

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
                        "native_session_id": "late-bound-thread",
                        "native_turn_id": "late-bound-turn"
                    }),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap();
        let after_binding = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(heartbeat_request),
        )
        .await
        .unwrap()
        .0;
        let interrupt = after_binding
            .session_commands
            .iter()
            .find(|command| command.command == "interrupt")
            .expect("native binding must make the pending stop deliverable");
        assert_eq!(interrupt.command_id, fixture.turn_id);
        assert_eq!(
            interrupt.native_session_id.as_deref(),
            Some("late-bound-thread")
        );
        assert_eq!(interrupt.native_turn_id.as_deref(), Some("late-bound-turn"));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn interrupted_completion_moves_only_queued_steers_to_one_claimable_next_turn(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'completion-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let delivering = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "possibly applied before stop".into(),
                attachments: json!([{
                    "kind": "text",
                    "name": "delivering.txt",
                    "content_type": "text/plain",
                    "size_bytes": 10,
                    "text": "delivering"
                }]),
                client_message_key: Some("interrupt-delivering".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        let queued = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "never sent before stop".into(),
                attachments: json!([{
                    "kind": "text",
                    "name": "queued.txt",
                    "content_type": "text/plain",
                    "size_bytes": 6,
                    "text": "queued"
                }]),
                client_message_key: Some("interrupt-queued".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        sqlx::query("UPDATE hub_session_messages SET delivery_state = 'delivering' WHERE id = $1")
            .bind(delivering.message.id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _ = stop_integration_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path((fixture.session_id, fixture.run_id)),
        )
        .await
        .unwrap();
        let late = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "new Turn after interrupt".into(),
                attachments: json!([]),
                client_message_key: Some("interrupt-next-turn".into()),
            }),
        )
        .await
        .unwrap()
        .0;

        let completed = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "interrupted".into(),
                    native_session_id: Some("completion-thread".into()),
                    work_dir_ref: Some("retained-workspace".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(completed.status, "interrupted");
        assert_eq!(
            completed.work_dir_ref.as_deref(),
            Some("retained-workspace")
        );
        let queued_state: (String, String, Option<String>, Uuid, Uuid) = sqlx::query_as(
            "SELECT delivery_mode, delivery_state, expected_native_turn_id, turn_id, run_id
             FROM hub_session_messages WHERE id = $1",
        )
        .bind(queued.message.id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(queued_state.0, "next_turn");
        assert_eq!(queued_state.1, "queued");
        assert_eq!(queued_state.2, None);
        assert_eq!(queued_state.3, late.message.turn_id.unwrap());
        assert_eq!(queued_state.4, late.run.id);
        let delivering_state: (String, String, Option<String>, Uuid, Uuid) = sqlx::query_as(
            "SELECT delivery_mode, delivery_state, expected_native_turn_id, turn_id, run_id
             FROM hub_session_messages WHERE id = $1",
        )
        .bind(delivering.message.id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(delivering_state.0, "steer");
        assert_eq!(delivering_state.1, "delivering");
        assert_eq!(delivering_state.3, fixture.turn_id);
        assert_eq!(delivering_state.4, fixture.run_id);
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT run_id FROM integration_messages WHERE hub_message_id = $1"
            )
            .bind(delivering.message.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            fixture.run_id
        );
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT run_id FROM integration_messages WHERE hub_message_id = $1"
            )
            .bind(queued.message.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            late.run.id
        );
        let attachment_runs: Vec<(String, Uuid, Option<Uuid>)> = sqlx::query_as(
            "SELECT name, run_id, hub_message_id FROM integration_attachments
             WHERE session_id = $1 ORDER BY name",
        )
        .bind(fixture.session_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            attachment_runs,
            vec![
                (
                    "delivering.txt".into(),
                    fixture.run_id,
                    Some(delivering.message.id),
                ),
                ("queued.txt".into(), late.run.id, Some(queued.message.id)),
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT run_id FROM run_events WHERE hub_message_id = $1"
            )
            .bind(queued.message.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            late.run.id
        );

        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, delivering.message.id)),
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
        let claimed = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeClaimRunRequest {
                available_new_session_slots: 0,
                ready_owned_sessions: vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                }],
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claimed.status(), StatusCode::OK);
        let claimed: ClaimRunResponse = serde_json::from_slice(
            &axum::body::to_bytes(claimed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claimed.run.id, late.run.id);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn completed_run_moves_queued_steer_to_a_claimable_next_turn(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let model_connection_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO model_connections
                 (id, scope, name, base_url, api_type, allowed_model_ids,
                  api_key_ciphertext, api_key_nonce, created_by)
             VALUES ($1, 'global', 'Completed Race Model', 'https://models.example.test',
                     'openai_responses', ARRAY['completed-race-model'], $2, $3, $4)",
        )
        .bind(model_connection_id)
        .bind(vec![1_u8; 17])
        .bind(vec![2_u8; 12])
        .bind(owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE agents SET model_connection_id = $1, model_id = 'completed-race-model'
             WHERE id = $2",
        )
        .bind(model_connection_id)
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let queued = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "continue after natural completion".into(),
                attachments: json!([]),
                client_message_key: Some("completed-race-next-turn".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(queued.run.id, fixture.run_id);
        assert_eq!(queued.message.delivery_mode, "steer");
        assert_eq!(queued.message.delivery_state, "queued");

        let completed = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("naturally-completed-session".into()),
                    work_dir_ref: Some("retained-workspace".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(completed.status, "completed");

        let moved: (String, String, Option<String>, Uuid, Uuid) = sqlx::query_as(
            "SELECT delivery_mode, delivery_state, expected_native_turn_id, turn_id, run_id
             FROM hub_session_messages WHERE id = $1",
        )
        .bind(queued.message.id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(moved.0, "next_turn");
        assert_eq!(moved.1, "queued");
        assert_eq!(moved.2, None);
        assert_ne!(moved.3, fixture.turn_id);
        assert_ne!(moved.4, fixture.run_id);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(moved.4)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "pending"
        );

        let claimed = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeClaimRunRequest {
                available_new_session_slots: 0,
                ready_owned_sessions: vec![RuntimeOwnedSessionGenerationDto {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                }],
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claimed.status(), StatusCode::OK);
        let claimed: ClaimRunResponse = serde_json::from_slice(
            &axum::body::to_bytes(claimed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claimed.run.id, moved.4);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn natural_completion_remains_authoritative_across_the_stop_race(pool: PgPool) {
        let stop_first = integration_runtime_fixture(pool.clone()).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'stop-first-thread' WHERE id = $1",
        )
        .bind(stop_first.hub_session_id)
        .execute(&stop_first.state.pool)
        .await
        .unwrap();
        let _ = stop_integration_run(
            State(stop_first.state.clone()),
            bearer_headers(&stop_first.integration_token),
            Path((stop_first.session_id, stop_first.run_id)),
        )
        .await
        .unwrap();
        let completed = runtime_complete_run(
            State(stop_first.state.clone()),
            bearer_headers(&stop_first.runtime_token),
            Path(stop_first.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("stop-first-thread".into()),
                    work_dir_ref: Some("naturally-completed-workspace".into()),
                },
            ),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(completed.status, "completed");
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT active_turn_id FROM hub_sessions WHERE id = $1"
            )
            .bind(stop_first.hub_session_id)
            .fetch_one(&stop_first.state.pool)
            .await
            .unwrap(),
            None
        );

        let completion_first = integration_runtime_fixture(pool).await;
        let _ = runtime_complete_run(
            State(completion_first.state.clone()),
            bearer_headers(&completion_first.runtime_token),
            Path(completion_first.run_id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("completion-first-thread".into()),
                    work_dir_ref: Some("completion-first-workspace".into()),
                },
            ),
        )
        .await
        .unwrap();
        let stop_result = stop_integration_run(
            State(completion_first.state.clone()),
            bearer_headers(&completion_first.integration_token),
            Path((completion_first.session_id, completion_first.run_id)),
        )
        .await
        .unwrap();
        // Stopping an already-terminal Run is idempotent: it returns the Run
        // as-is instead of a 409, so a client that raced natural completion
        // still converges on the authoritative terminal state.
        assert_eq!(stop_result.0.status, "completed");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn turn_ended_fallback_moves_integration_message_and_attachment_context(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let accepted = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "fallback with its attachment".into(),
                attachments: json!([{
                    "kind": "text",
                    "name": "fallback.txt",
                    "content_type": "text/plain",
                    "size_bytes": 8,
                    "text": "fallback"
                }]),
                client_message_key: Some("turn-ended-attachment".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(accepted.run.id, fixture.run_id);
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
            .any(|command| command.command_id == accepted.message.id));

        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, accepted.message.id)),
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

        let next_run_id: Uuid =
            sqlx::query_scalar("SELECT run_id FROM hub_session_messages WHERE id = $1")
                .bind(accepted.message.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        assert_ne!(next_run_id, fixture.run_id);
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT run_id FROM integration_messages WHERE hub_message_id = $1"
            )
            .bind(accepted.message.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            next_run_id
        );
        assert_eq!(
            sqlx::query_as::<_, (Uuid, Option<Uuid>)>(
                "SELECT run_id, hub_message_id FROM integration_attachments
                 WHERE name = 'fallback.txt' AND session_id = $1"
            )
            .bind(fixture.session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (next_run_id, Some(accepted.message.id))
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT integration_session_id FROM runs WHERE id = $1"
            )
            .bind(next_run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            Some(fixture.session_id)
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn concurrent_ended_steers_share_one_pending_run_and_busy_session_is_not_claimed(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        let message_ids = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, message_id) in message_ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content, delivery_mode,
                      delivery_state, expected_native_turn_id, turn_id, run_id)
                 VALUES ($1, $2, 'user', 'message', $3, 'steer', 'delivering',
                         'fixture-native-turn', $4, $5)",
            )
            .bind(message_id)
            .bind(fixture.hub_session_id)
            .bind(format!("concurrent steer {index}"))
            .bind(fixture.turn_id)
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }
        let complete = |message_id| {
            runtime_complete_session_command(
                State(fixture.state.clone()),
                bearer_headers(&fixture.runtime_token),
                Path((fixture.hub_session_id, message_id)),
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
        };

        let (first, second) = tokio::join!(complete(message_ids[0]), complete(message_ids[1]));
        let _ = first.unwrap();
        let _ = second.unwrap();

        let routed: Vec<(Uuid, i64, Uuid, Uuid)> = sqlx::query_as(
            "SELECT id, sequence, turn_id, run_id
             FROM hub_session_messages WHERE id = ANY($1)
             ORDER BY sequence",
        )
        .bind(message_ids)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(routed.len(), 2);
        assert_eq!(routed[0].2, routed[1].2);
        assert_eq!(routed[0].3, routed[1].3);
        assert!(routed[0].1 < routed[1].1);
        let pending_count: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM runs JOIN hub_session_turns AS turns ON turns.id = runs.hub_turn_id
             WHERE runs.hub_session_id = $1
               AND runs.status = 'pending' AND turns.status = 'pending'",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(pending_count, 1);

        let claim = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeClaimRunRequest {
                available_new_session_slots: 0,
                ready_owned_sessions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claim.status(), StatusCode::NO_CONTENT);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn public_super_admin_agents_are_visible_to_member_users(pool: PgPool) {
        let member_token = create_user_session_with_role(&pool, "member").await;
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let super_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&super_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let public_super_agent = Uuid::new_v4();
        let private_super_agent = Uuid::new_v4();
        for (agent_id, visibility) in [
            (public_super_agent, "public"),
            (private_super_agent, "private"),
        ] {
            sqlx::query(
                "INSERT INTO agents
                     (id, owner_id, name, instructions, visibility, model_policy)
                 VALUES ($1, $2, $3, 'instructions', $4,
                         '{\"provider\":\"hub-proxy\"}'::jsonb)",
            )
            .bind(agent_id)
            .bind(super_id)
            .bind(format!("Super {visibility} Agent"))
            .bind(visibility)
            .execute(&state.pool)
            .await
            .unwrap();
        }

        let visible = list_agents(State(state.clone()), session_headers(&member_token))
            .await
            .unwrap()
            .0;
        assert!(
            visible.iter().any(|agent| agent.id == public_super_agent),
            "public super_admin Agent must be visible to members"
        );
        assert!(
            visible.iter().all(|agent| agent.id != private_super_agent),
            "private super_admin Agent must stay hidden from members"
        );
        let member = require_user(&state, &session_headers(&member_token))
            .await
            .unwrap();
        let fetched = load_agent_for_user(&state.pool, public_super_agent, &member)
            .await
            .expect("public super_admin Agent must be loadable by a member");
        assert_eq!(fetched.id, public_super_agent);
        assert_eq!(
            load_agent_for_user(&state.pool, private_super_agent, &member)
                .await
                .unwrap_err()
                .status,
            StatusCode::NOT_FOUND
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn only_super_admin_changes_roles_and_the_last_super_admin_is_preserved(pool: PgPool) {
        let super_token = create_user_session_with_role(&pool, "super_admin").await;
        let member_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let (super_id, super_email): (Uuid, String) = sqlx::query_as(
            "SELECT users.id, users.email
             FROM sessions JOIN users ON users.id = sessions.user_id
             WHERE sessions.token_hash = $1",
        )
        .bind(sha256_hex(&super_token))
        .fetch_one(&state.pool)
        .await
        .unwrap();
        let member_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&member_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();

        let protected_erasure = erase_user(
            State(state.clone()),
            session_headers(&admin_token),
            Path(super_id),
            Json(EraseUserRequest {
                email: super_email.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(protected_erasure.status, StatusCode::NOT_FOUND);
        let last_super_erasure = erase_user(
            State(state.clone()),
            session_headers(&super_token),
            Path(super_id),
            Json(EraseUserRequest { email: super_email }),
        )
        .await
        .unwrap_err();
        assert_eq!(last_super_erasure.status, StatusCode::CONFLICT);

        let last_super = set_admin_user_role(
            State(state.clone()),
            session_headers(&super_token),
            Path(super_id),
            Json(AdminSetUserRoleRequest {
                role: "admin".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(last_super.status, StatusCode::CONFLICT);

        let promoted = set_admin_user_role(
            State(state.clone()),
            session_headers(&super_token),
            Path(member_id),
            Json(AdminSetUserRoleRequest {
                role: "super_admin".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(promoted.user.role, "super_admin");

        let demoted = set_admin_user_role(
            State(state.clone()),
            session_headers(&super_token),
            Path(super_id),
            Json(AdminSetUserRoleRequest {
                role: "admin".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(demoted.user.role, "admin");

        let forbidden = set_admin_user_role(
            State(state),
            session_headers(&admin_token),
            Path(member_id),
            Json(AdminSetUserRoleRequest {
                role: "member".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn concurrent_super_admin_demotions_cannot_remove_every_super_admin(pool: PgPool) {
        let first_token = create_user_session_with_role(&pool, "super_admin").await;
        let second_token = create_user_session_with_role(&pool, "super_admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let first_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&first_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let second_id: Uuid =
            sqlx::query_scalar("SELECT user_id FROM sessions WHERE token_hash = $1")
                .bind(sha256_hex(&second_token))
                .fetch_one(&state.pool)
                .await
                .unwrap();

        let mut lock = state.pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('agent-hub-user-create', 0))")
            .execute(&mut *lock)
            .await
            .unwrap();
        let first_state = state.clone();
        let first = tokio::spawn(async move {
            set_admin_user_role(
                State(first_state),
                session_headers(&first_token),
                Path(second_id),
                Json(AdminSetUserRoleRequest {
                    role: "admin".into(),
                }),
            )
            .await
        });
        let second_state = state.clone();
        let second = tokio::spawn(async move {
            set_admin_user_role(
                State(second_state),
                session_headers(&second_token),
                Path(first_id),
                Json(AdminSetUserRoleRequest {
                    role: "admin".into(),
                }),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        lock.commit().await.unwrap();

        let results = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM users
                 WHERE role = 'super_admin' AND deletion_requested_at IS NULL",
            )
            .fetch_one(&state.pool)
            .await
            .unwrap(),
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn retained_owner_bundle_retry_reuses_commit_without_rewriting_current_object(
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
                    native_session_id: Some("retained-owner-thread".into()),
                    work_dir_ref: Some("retained-owner-workdir".into()),
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
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'queued after Bundle checkpoint',
                     'next_turn', 'queued')",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let _ = insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;

        let put_count = Arc::new(AtomicU64::new(0));
        let delete_count = Arc::new(AtomicU64::new(0));
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let put_count = Arc::clone(&put_count);
                move |body: Body| {
                    let put_count = Arc::clone(&put_count);
                    async move {
                        let _ = axum::body::to_bytes(body, 1024).await.unwrap();
                        if put_count.fetch_add(1, Ordering::SeqCst) == 0 {
                            StatusCode::NO_CONTENT
                        } else {
                            StatusCode::INTERNAL_SERVER_ERROR
                        }
                    }
                }
            })
            .delete({
                let delete_count = Arc::clone(&delete_count);
                move || {
                    let delete_count = Arc::clone(&delete_count);
                    async move {
                        delete_count.fetch_add(1, Ordering::SeqCst);
                        StatusCode::NO_CONTENT
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
        let bytes = Bytes::from_static(b"retained owner bundle");
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

        let committed = runtime_upload_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            headers.clone(),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap()
        .0;
        assert!(committed.has_queued_work);
        assert!(!committed.ownership_released);

        let replayed = runtime_upload_session_bundle(
            State(state),
            Path(fixture.hub_session_id),
            headers,
            Body::from(bytes),
        )
        .await
        .expect("committed attempt retry must not rewrite its current object")
        .0;

        assert_eq!(replayed, committed);
        assert_eq!(put_count.load(Ordering::SeqCst), 1);
        assert_eq!(delete_count.load(Ordering::SeqCst), 0);
        type SessionBundleRow = (
            Option<Uuid>,
            String,
            Option<Uuid>,
            Option<i64>,
            Option<i64>,
            Option<Uuid>,
        );
        let session: SessionBundleRow = sqlx::query_as(
            "SELECT runtime_owner_id, lifecycle_status, saving_checkpoint_attempt_id,
                        current_bundle_generation, current_bundle_ownership_generation,
                        current_bundle_runtime_id
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(
            session,
            (
                Some(fixture.runtime_id),
                "online".into(),
                None,
                Some(1),
                Some(1),
                Some(fixture.runtime_id)
            )
        );
        let stale_generation = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 2,
                force: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stale_generation.ownership_generation, 1);
        assert_eq!(stale_generation.runtime_owner_id, Some(fixture.runtime_id));

        let foreign_runtime_id = Uuid::new_v4();
        let foreign_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, $3, '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online')",
        )
        .bind(foreign_runtime_id)
        .bind(sha256_hex(&foreign_runtime_token))
        .bind(format!("release-foreign-{}", Uuid::new_v4().simple()))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let stale_owner = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&foreign_runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(stale_owner.runtime_owner_id, Some(fixture.runtime_id));

        sqlx::query(
            "UPDATE hub_sessions SET current_bundle_ownership_generation = 2 WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let stale_bundle_generation = runtime_release_session(
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
        assert_eq!(stale_bundle_generation.status, StatusCode::CONFLICT);
        sqlx::query(
            "UPDATE hub_sessions SET current_bundle_ownership_generation = 1 WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
            .bind(fixture.turn_id)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let active_turn = runtime_release_session(
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
        assert_eq!(active_turn.status, StatusCode::CONFLICT);
        sqlx::query("UPDATE hub_sessions SET active_turn_id = NULL WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let unreplayable_message_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'delivered after Bundle checkpoint',
                     'record_only', 'delivered')",
        )
        .bind(unreplayable_message_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let unreplayable = runtime_release_session(
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
        assert_eq!(unreplayable.status, StatusCode::CONFLICT);
        sqlx::query("UPDATE hub_session_messages SET delivery_state = 'failed' WHERE id = $1")
            .bind(unreplayable_message_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
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
            0
        );
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
        .expect("a retained owner can explicitly release its current Bundle generation")
        .0;
        assert_eq!(released.runtime_owner_id, None);
        assert_eq!(released.ownership_generation, 1);
        assert_eq!(released.lifecycle_status, "waiting_for_runtime");
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
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn bundle_cleanup_deletes_old_object_only_after_new_pointer_commit(pool: PgPool) {
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
                    native_session_id: Some("cleanup-order-thread".into()),
                    work_dir_ref: Some("cleanup-order-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let old_attempt_id = Uuid::new_v4();
        let old_object_key = session_bundle_object_key(fixture.hub_session_id, 1, old_attempt_id);
        sqlx::query(
            "UPDATE hub_sessions
             SET current_bundle_generation = 1, current_bundle_object_key = $2,
                 current_bundle_checksum_sha256 = $3, current_bundle_size_bytes = 10,
                 current_bundle_history_checkpoint = history_checkpoint,
                 current_bundle_ownership_generation = 1,
                 current_bundle_producing_engine_version = '0.103.0',
                 current_bundle_created_at = now(),
                 current_bundle_checkpoint_attempt_id = $4,
                 current_bundle_runtime_id = $5
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .bind(&old_object_key)
        .bind("0".repeat(64))
        .bind(old_attempt_id)
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let first_attempt = runtime_begin_session_checkpoint(
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
        assert_eq!(first_attempt.bundle_generation, 2);

        let put_count = Arc::new(AtomicU64::new(0));
        let deleted_with_pointer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            axum::routing::put({
                let pool = fixture.state.pool.clone();
                let put_count = Arc::clone(&put_count);
                let session_id = fixture.hub_session_id;
                move |body: Body| {
                    let pool = pool.clone();
                    let put_count = Arc::clone(&put_count);
                    async move {
                        let _ = axum::body::to_bytes(body, 1024).await.unwrap();
                        if put_count.fetch_add(1, Ordering::SeqCst) == 0 {
                            sqlx::query(
                                "UPDATE hub_sessions
                                 SET lifecycle_status = 'online',
                                     saving_history_checkpoint = NULL,
                                     saving_ownership_generation = NULL,
                                     saving_reason = NULL,
                                     saving_checkpoint_attempt_id = NULL
                                 WHERE id = $1",
                            )
                            .bind(session_id)
                            .execute(&pool)
                            .await
                            .unwrap();
                        }
                        StatusCode::NO_CONTENT
                    }
                }
            })
            .delete({
                let pool = fixture.state.pool.clone();
                let deleted_with_pointer = Arc::clone(&deleted_with_pointer);
                let session_id = fixture.hub_session_id;
                move |Path(key): Path<String>| {
                    let pool = pool.clone();
                    let deleted_with_pointer = Arc::clone(&deleted_with_pointer);
                    async move {
                        let pointer: Option<String> = sqlx::query_scalar(
                            "SELECT current_bundle_object_key FROM hub_sessions WHERE id = $1",
                        )
                        .bind(session_id)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                        deleted_with_pointer.lock().unwrap().push((key, pointer));
                        StatusCode::NO_CONTENT
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
        let bytes = Bytes::from_static(b"new bundle bytes");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let first_new_key = session_bundle_object_key(
            fixture.hub_session_id,
            first_attempt.bundle_generation,
            first_attempt.checkpoint_attempt_id,
        );

        let failed = runtime_upload_session_bundle(
            State(state.clone()),
            Path(fixture.hub_session_id),
            runtime_bundle_upload_headers(
                &fixture.runtime_token,
                1,
                &first_attempt,
                &checksum,
                bytes.len(),
                Utc::now(),
            ),
            Body::from(bytes.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(failed.status, StatusCode::CONFLICT);
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT current_bundle_object_key FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            Some(old_object_key.clone())
        );

        let second_attempt = runtime_begin_session_checkpoint(
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
        let second_new_key = session_bundle_object_key(
            fixture.hub_session_id,
            second_attempt.bundle_generation,
            second_attempt.checkpoint_attempt_id,
        );
        let committed = runtime_upload_session_bundle(
            State(state),
            Path(fixture.hub_session_id),
            runtime_bundle_upload_headers(
                &fixture.runtime_token,
                1,
                &second_attempt,
                &checksum,
                bytes.len(),
                Utc::now(),
            ),
            Body::from(bytes),
        )
        .await
        .unwrap()
        .0;
        assert!(committed.ownership_released);

        assert_eq!(
            *deleted_with_pointer.lock().unwrap(),
            vec![
                (first_new_key, Some(old_object_key.clone())),
                (old_object_key, Some(second_new_key)),
            ]
        );
        server.abort();
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn failed_restoring_run_accepts_lost_ack_heartbeat_after_fencing_owner(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        sqlx::query("UPDATE hub_sessions SET lifecycle_status = 'restoring' WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let failed = runtime_complete_run(
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
        .unwrap()
        .0;

        assert_eq!(failed.status, "failed");
        let session: (String, Option<Uuid>, i64, Option<String>) = sqlx::query_as(
            "SELECT lifecycle_status, runtime_owner_id, ownership_generation, recovery_error
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(session.0, "recovery_failed");
        assert_eq!(session.1, None);
        assert_eq!(session.2, 2);
        assert!(session.3.is_some());

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                accepts_session_commands: true,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    lifecycle_status: "restoring".into(),
                    checkpoint_reason: None,
                }],
                ..RuntimeHeartbeatRequest::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(heartbeat.owned_sessions.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn checkpoint_begin_freezes_history_and_fences_replayed_results(pool: PgPool) {
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
                    native_session_id: Some("checkpoint-thread".into()),
                    work_dir_ref: Some("checkpoint-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();

        let begin_request = BeginRuntimeSessionCheckpointRequest {
            ownership_generation: 1,
            reason: "idle".into(),
        };
        let first = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin_request.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(first.history_checkpoint, 3);
        assert_eq!(first.reason, "idle");
        let repeated = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(begin_request),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(repeated, first);

        let queued_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'queued while saving',
                     'next_turn', 'queued')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(queued_sequence, 4);
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(queued_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let fail_request = FailRuntimeSessionCheckpointRequest {
            ownership_generation: 1,
            checkpoint_attempt_id: first.checkpoint_attempt_id,
            error: "bundle_transport_unavailable".into(),
        };
        let failed = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(fail_request.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(failed.disposition, "resume");
        assert!(failed.has_queued_work);
        let replayed = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(fail_request.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(replayed, failed);

        let second = runtime_begin_session_checkpoint(
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
        assert_ne!(second.checkpoint_attempt_id, first.checkpoint_attempt_id);
        assert_eq!(second.history_checkpoint, 4);
        let upgraded = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                reason: "drain".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(upgraded.checkpoint_attempt_id, second.checkpoint_attempt_id);
        assert_eq!(upgraded.reason, "drain");

        let queued_after_begin_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'queued after second begin',
                     'next_turn', 'queued')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(queued_after_begin_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let stale = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(fail_request),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.status, StatusCode::CONFLICT);
        let draining_failure = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(FailRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                checkpoint_attempt_id: second.checkpoint_attempt_id,
                error: "bundle_transport_unavailable".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(draining_failure.disposition, "resume");
        assert!(draining_failure.has_queued_work);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn checkpoint_begin_waits_for_message_commit_before_freezing_history(pool: PgPool) {
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
                    native_session_id: Some("checkpoint-race-thread".into()),
                    work_dir_ref: Some("checkpoint-race-workdir".into()),
                },
            ),
        )
        .await
        .unwrap();
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let mut message_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(fixture.hub_session_id)
            .execute(&mut *message_tx)
            .await
            .unwrap();

        let application_name = format!("checkpoint-message-race-{}", Uuid::new_v4().simple());
        let begin_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_name).await,
        ));
        let begin_headers = bearer_headers(&fixture.runtime_token);
        let session_id = fixture.hub_session_id;
        let mut begin_task = tokio::spawn(async move {
            runtime_begin_session_checkpoint(
                State(begin_state),
                begin_headers,
                Path(session_id),
                Json(BeginRuntimeSessionCheckpointRequest {
                    ownership_generation: 1,
                    reason: "idle".into(),
                }),
            )
            .await
        });
        let begin_wait_observed = wait_for_application_lock(
            &fixture.state.pool,
            &application_name,
            "SELECT runtime_owner_id, ownership_generation, lifecycle_status",
        )
        .await;
        let accepted = accept_session_message_tx(
            &mut message_tx,
            AcceptSessionMessage {
                session_id: fixture.hub_session_id,
                agent_id: fixture.agent_id,
                owner_id,
                content: "committed before checkpoint".into(),
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
                model_subject_user_id: Some(owner_id),
                model_source_integration_app_id: None,
                external_user_context: None,
                attachment_ids: Vec::new(),
            },
        )
        .await
        .unwrap();
        message_tx.commit().await.unwrap();
        let attempt = tokio::time::timeout(Duration::from_secs(3), &mut begin_task)
            .await
            .expect("checkpoint begin should unblock after message commit")
            .expect("checkpoint begin task should not panic")
            .unwrap()
            .0;

        assert!(
            begin_wait_observed,
            "checkpoint begin must serialize with Session message acceptance"
        );
        assert_eq!(accepted.message.sequence, 4);
        assert_eq!(attempt.history_checkpoint, accepted.message.sequence);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT saving_history_checkpoint FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            accepted.message.sequence
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn checkpoint_failure_ignores_deferred_only_and_observes_runtime_drain(pool: PgPool) {
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
                    native_session_id: Some("checkpoint-race-thread".into()),
                    work_dir_ref: Some("checkpoint-race-workdir".into()),
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
        let deferred_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'later only', 'later_turn', 'deferred')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(deferred_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let fail_request = FailRuntimeSessionCheckpointRequest {
            ownership_generation: 1,
            checkpoint_attempt_id: attempt.checkpoint_attempt_id,
            error: "bundle_transport_unavailable".into(),
        };

        let deferred_only = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(fail_request.clone()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(deferred_only.disposition, "retry");
        assert!(!deferred_only.has_queued_work);

        sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let queued_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'queued after drain', 'next_turn', 'queued')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(queued_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let after_drain = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(fail_request),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(after_drain.disposition, "retry");
        assert!(after_drain.has_queued_work);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT saving_reason FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "drain"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn cancelled_drain_checkpoint_resumes_queued_work(pool: PgPool) {
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
                    native_session_id: Some("cancel-drain-thread".into()),
                    work_dir_ref: Some("cancel-drain-workdir".into()),
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
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap();
        let drain_attempt_id: Uuid = sqlx::query_scalar(
            "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let queued_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'resume after cancelled drain',
                     'next_turn', 'queued')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(queued_sequence)
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _ = cancel_runtime_drain(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap();

        let cancelled = runtime_fail_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(FailRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                checkpoint_attempt_id: drain_attempt_id,
                error: "bundle_transport_unavailable".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(cancelled.disposition, "resume");
        assert!(cancelled.has_queued_work);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT lifecycle_status FROM hub_sessions WHERE id = $1",
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
    async fn draining_runtime_claim_is_idle_without_mutating_pending_work(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let admin_token = create_super_admin_session(&fixture.state.pool).await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_before: (Option<Uuid>, i64, String) = sqlx::query_as(
            "SELECT runtime_owner_id, ownership_generation, lifecycle_status
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();

        let drained = drain_runtime(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(drained.runtime.status, "draining");

        let claim = runtime_claim_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            runtime_claim_request(1, Vec::new()),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(claim.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            sqlx::query_as::<_, (String, Option<Uuid>, Option<i64>)>(
                "SELECT status, runtime_id, session_ownership_generation
                 FROM runs WHERE id = $1",
            )
            .bind(fixture.run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("pending".into(), None, Some(0))
        );
        assert_eq!(
            sqlx::query_as::<_, (Option<Uuid>, i64, String)>(
                "SELECT runtime_owner_id, ownership_generation, lifecycle_status
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            session_before
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn ordinary_delete_waits_for_checkpoint_release_and_allows_last_runtime(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
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
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap();

        let mismatch = delete_drained_runtime(
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
        let blocked = delete_drained_runtime(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(blocked.status, StatusCode::CONFLICT);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "draining"
        );

        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(claim.run.id),
            runtime_write(CompleteRunRequest {
                status: "completed".into(),
                native_session_id: Some("delete-thread".into()),
                work_dir_ref: Some("delete-workdir".into()),
            }),
        )
        .await
        .unwrap();
        let checkpoint_attempt_id: Uuid = sqlx::query_scalar(
            "SELECT saving_checkpoint_attempt_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let mut commit_tx = fixture.state.pool.begin().await.unwrap();
        commit_session_bundle_metadata_tx(
            &mut commit_tx,
            fixture.runtime_id,
            fixture.hub_session_id,
            1,
            "hub/bundles/ordinary-delete.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "ordinary-delete".into(),
                size_bytes: 1,
                history_checkpoint: 3,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        commit_tx.commit().await.unwrap();

        let _ = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap();

        let cleanup_blocked = delete_drained_runtime(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(cleanup_blocked.status, StatusCode::CONFLICT);
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

        assert_eq!(
            delete_drained_runtime(
                State(admin_state),
                session_headers(&admin_token),
                Path(fixture.runtime_id),
                Json(ConfirmRuntimeHostnameRequest { hostname }),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtimes")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        let old_credential = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(old_credential.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn explicit_release_locks_runtime_before_session_and_force_delete_does_not_deadlock(
        pool: PgPool,
    ) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let checkpoint_attempt_id = Uuid::new_v4();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, ownership_generation = 1,
                 lifecycle_status = 'online', active_turn_id = NULL,
                 current_bundle_generation = 1,
                 current_bundle_object_key = 'hub/bundles/release-force-delete.tar.zst',
                 current_bundle_checksum_sha256 = $2, current_bundle_size_bytes = 1,
                 current_bundle_history_checkpoint = history_checkpoint,
                 current_bundle_ownership_generation = 1,
                 current_bundle_producing_engine_version = 'test',
                 current_bundle_created_at = now(),
                 current_bundle_checkpoint_attempt_id = $3,
                 current_bundle_runtime_id = $1
             WHERE id = $4",
        )
        .bind(fixture.runtime_id)
        .bind("a".repeat(64))
        .bind(checkpoint_attempt_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let admin_token = create_super_admin_session(&fixture.state.pool).await;

        let mut session_gate = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM hub_sessions WHERE id = $1 FOR UPDATE")
            .bind(fixture.hub_session_id)
            .fetch_one(&mut *session_gate)
            .await
            .unwrap();

        let release_application = format!("release-force-delete-{}", Uuid::new_v4().simple());
        let release_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &release_application)
                .await,
        ));
        let runtime_token = fixture.runtime_token.clone();
        let session_id = fixture.hub_session_id;
        let mut release_task = tokio::spawn(async move {
            runtime_release_session(
                State(release_state),
                bearer_headers(&runtime_token),
                Path(session_id),
                Json(ReleaseRuntimeSessionRequest {
                    ownership_generation: 1,
                    force: false,
                }),
            )
            .await
        });
        let release_waited_for_session = wait_for_application_lock(
            &fixture.state.pool,
            &release_application,
            "SELECT runtime_owner_id, ownership_generation, lifecycle_status",
        )
        .await;

        let delete_application = format!("force-delete-release-{}", Uuid::new_v4().simple());
        let delete_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.state.pool, &delete_application)
                .await,
        ));
        let runtime_id = fixture.runtime_id;
        let mut delete_task = tokio::spawn(async move {
            force_delete_runtime(
                State(delete_state),
                session_headers(&admin_token),
                Path(runtime_id),
                Json(ConfirmRuntimeHostnameRequest { hostname }),
            )
            .await
        });
        let force_delete_waited_for_runtime = wait_for_application_lock(
            &fixture.state.pool,
            &delete_application,
            "SELECT hostname FROM runtimes",
        )
        .await;

        session_gate.commit().await.unwrap();
        let release = tokio::time::timeout(Duration::from_secs(5), &mut release_task)
            .await
            .expect("explicit release should not deadlock with force delete")
            .expect("explicit release task should not panic")
            .expect("explicit release should complete normally")
            .0;
        let deleted = tokio::time::timeout(Duration::from_secs(5), &mut delete_task)
            .await
            .expect("force delete should not deadlock with explicit release")
            .expect("force delete task should not panic")
            .expect("force delete should complete normally")
            .0;

        assert!(
            release_waited_for_session,
            "explicit release must reach and wait for the locked Session"
        );
        assert!(
            force_delete_waited_for_runtime,
            "force delete must wait for Runtime while explicit release waits for Session"
        );
        assert_eq!(release.runtime_owner_id, None);
        assert_eq!(release.ownership_generation, 1);
        assert!(deleted.recoverable_session_ids.is_empty());
        assert!(deleted.recovery_failed_session_ids.is_empty());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn ordinary_administrator_cannot_affect_runtime_with_super_admin_session(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        sqlx::query(
            "UPDATE users
             SET role = 'super_admin'
             WHERE id = (SELECT owner_id FROM hub_sessions WHERE id = $1)",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET runtime_owner_id = $1, lifecycle_status = 'online', ownership_generation = 1
             WHERE id = $2",
        )
        .bind(fixture.runtime_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let admin_token = create_user_session_with_role(&fixture.state.pool, "admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));

        let preview_forbidden = get_runtime_deletion_impact(
            State(state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap_err();
        assert_eq!(preview_forbidden.status, StatusCode::FORBIDDEN);

        let drain_forbidden = drain_runtime(
            State(state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(drain_forbidden.status, StatusCode::FORBIDDEN);

        sqlx::query("UPDATE runtimes SET status = 'draining' WHERE id = $1")
            .bind(fixture.runtime_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let cancel_forbidden = cancel_runtime_drain(
            State(state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap_err();
        assert_eq!(cancel_forbidden.status, StatusCode::FORBIDDEN);

        let force_forbidden = force_delete_runtime(
            State(state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap_err();
        assert_eq!(force_forbidden.status, StatusCode::FORBIDDEN);

        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "draining"
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT runtime_owner_id FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            Some(fixture.runtime_id)
        );

        let super_admin_token = create_super_admin_session(&fixture.state.pool).await;
        let impact = get_runtime_deletion_impact(
            State(state),
            session_headers(&super_admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(impact.affected_sessions.len(), 1);
        assert_eq!(
            impact.affected_sessions[0].session_id,
            fixture.hub_session_id
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_delete_separates_checkpointed_and_uncheckpointed_sessions(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let checkpointed_session_id = insert_idle_owned_session(
            &fixture.state.pool,
            fixture.hub_session_id,
            fixture.runtime_id,
        )
        .await;
        let checkpointed_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(checkpointed_session_id),
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
            checkpointed_session_id,
            1,
            "hub/bundles/force-delete-current.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: checkpointed_attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "force-delete-current".into(),
                size_bytes: 1,
                history_checkpoint: 0,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        commit_tx.commit().await.unwrap();
        let mut queued_history_checkpoint = 0_i64;
        for (content, delivery_mode, delivery_state) in [
            ("queued after Bundle", "next_turn", "queued"),
            ("deferred after Bundle", "later_turn", "deferred"),
        ] {
            queued_history_checkpoint = sqlx::query_scalar(
                "INSERT INTO hub_session_messages
                     (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
                 VALUES ($1, $2, 'user', 'message', $3, $4, $5)
                 RETURNING sequence",
            )
            .bind(Uuid::new_v4())
            .bind(checkpointed_session_id)
            .bind(content)
            .bind(delivery_mode)
            .bind(delivery_state)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        }
        sqlx::query("UPDATE hub_sessions SET history_checkpoint = $1 WHERE id = $2")
            .bind(queued_history_checkpoint)
            .bind(checkpointed_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let stale_bundle_session_id = insert_idle_owned_session(
            &fixture.state.pool,
            fixture.hub_session_id,
            fixture.runtime_id,
        )
        .await;
        let stale_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(stale_bundle_session_id),
            Json(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                reason: "idle".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let mut stale_commit_tx = fixture.state.pool.begin().await.unwrap();
        commit_session_bundle_metadata_tx(
            &mut stale_commit_tx,
            fixture.runtime_id,
            stale_bundle_session_id,
            1,
            "hub/bundles/force-delete-stale.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: stale_attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "force-delete-stale".into(),
                size_bytes: 1,
                history_checkpoint: 0,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        stale_commit_tx.commit().await.unwrap();
        let delivered_history_checkpoint: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'delivered after Bundle',
                     'record_only', 'delivered')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(stale_bundle_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET ownership_generation = 2, history_checkpoint = $1
             WHERE id = $2",
        )
        .bind(delivered_history_checkpoint)
        .bind(stale_bundle_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let foreign_owner_id = Uuid::new_v4();
        let foreign_agent_id = Uuid::new_v4();
        let foreign_unique = Uuid::new_v4().simple().to_string();
        sqlx::query(
            "INSERT INTO users
                 (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Deletion Impact Foreign Owner', 'member')",
        )
        .bind(foreign_owner_id)
        .bind(format!("deletion-impact-{foreign_unique}@example.com"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO agents (id, owner_id, name, instructions, visibility)
             VALUES ($1, $2, 'Deletion Impact Foreign Agent', 'test', 'private')",
        )
        .bind(foreign_agent_id)
        .bind(foreign_owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let delivering_session_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status,
                  runtime_owner_id, ownership_generation)
             VALUES ($1, $2, $3, 'hub_native', 'online', $4, 1)",
        )
        .bind(delivering_session_id)
        .bind(foreign_owner_id)
        .bind(foreign_agent_id)
        .bind(fixture.runtime_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let delivering_attempt = runtime_begin_session_checkpoint(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(delivering_session_id),
            Json(BeginRuntimeSessionCheckpointRequest {
                ownership_generation: 1,
                reason: "idle".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let mut delivering_commit_tx = fixture.state.pool.begin().await.unwrap();
        commit_session_bundle_metadata_tx(
            &mut delivering_commit_tx,
            fixture.runtime_id,
            delivering_session_id,
            1,
            "hub/bundles/force-delete-delivering.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: delivering_attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "force-delete-delivering".into(),
                size_bytes: 1,
                history_checkpoint: 0,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        delivering_commit_tx.commit().await.unwrap();
        let delivering_history_checkpoint: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
             VALUES ($1, $2, 'user', 'message', 'delivering after Bundle',
                     'record_only', 'delivering')
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(delivering_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET history_checkpoint = $1
             WHERE id = $2",
        )
        .bind(delivering_history_checkpoint)
        .bind(delivering_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions SET created_at = '2026-07-18T00:00:00Z'
             WHERE id IN ($1, $2, $3, $4)",
        )
        .bind(fixture.hub_session_id)
        .bind(checkpointed_session_id)
        .bind(stale_bundle_session_id)
        .bind(delivering_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let admin_token = create_user_session_with_role(&fixture.state.pool, "admin").await;
        let member_token = create_user_session_with_role(&fixture.state.pool, "member").await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let forbidden = get_runtime_deletion_impact(
            State(admin_state.clone()),
            session_headers(&member_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
        let missing = get_runtime_deletion_impact(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(Uuid::new_v4()),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.status, StatusCode::NOT_FOUND);

        let runtime_state_before: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                 'hostname', hostname, 'status', status,
                 'last_heartbeat_at', last_heartbeat_at,
                 'rotation_requested_at', rotation_requested_at)
             FROM runtimes WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let session_state_before: Value = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'id', id, 'lifecycle_status', lifecycle_status,
                 'runtime_owner_id', runtime_owner_id,
                 'ownership_generation', ownership_generation,
                 'active_turn_id', active_turn_id,
                 'history_checkpoint', history_checkpoint,
                 'current_bundle_history_checkpoint', current_bundle_history_checkpoint)
                 ORDER BY created_at, id), '[]'::jsonb)
             FROM hub_sessions WHERE runtime_owner_id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let run_state_before: Value = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'id', id, 'status', status, 'runtime_id', runtime_id,
                 'session_ownership_generation', session_ownership_generation)
                 ORDER BY id), '[]'::jsonb)
             FROM runs WHERE runtime_id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let impact = get_runtime_deletion_impact(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(impact.runtime_id, fixture.runtime_id);
        assert_eq!(impact.hostname, hostname);
        let mut expected_session_ids = vec![
            fixture.hub_session_id,
            checkpointed_session_id,
            stale_bundle_session_id,
            delivering_session_id,
        ];
        expected_session_ids.sort_unstable();
        assert_eq!(
            impact
                .affected_sessions
                .iter()
                .map(|session| session.session_id)
                .collect::<Vec<_>>(),
            expected_session_ids
        );
        let dispositions = impact
            .affected_sessions
            .iter()
            .map(|session| {
                (
                    session.session_id,
                    session.force_delete_disposition.as_str(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(dispositions[&fixture.hub_session_id], "recovery_failed");
        assert_eq!(dispositions[&checkpointed_session_id], "recoverable");
        assert_eq!(dispositions[&stale_bundle_session_id], "recovery_failed");
        assert_eq!(dispositions[&delivering_session_id], "recovery_failed");
        assert_eq!(
            impact
                .affected_sessions
                .iter()
                .find(|session| session.session_id == delivering_session_id)
                .unwrap()
                .agent_name,
            "Deletion Impact Foreign Agent"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(DISTINCT owner_id) FROM hub_sessions WHERE runtime_owner_id = $1",
            )
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );
        let runtime_state_after: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                 'hostname', hostname, 'status', status,
                 'last_heartbeat_at', last_heartbeat_at,
                 'rotation_requested_at', rotation_requested_at)
             FROM runtimes WHERE id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let session_state_after: Value = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'id', id, 'lifecycle_status', lifecycle_status,
                 'runtime_owner_id', runtime_owner_id,
                 'ownership_generation', ownership_generation,
                 'active_turn_id', active_turn_id,
                 'history_checkpoint', history_checkpoint,
                 'current_bundle_history_checkpoint', current_bundle_history_checkpoint)
                 ORDER BY created_at, id), '[]'::jsonb)
             FROM hub_sessions WHERE runtime_owner_id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let run_state_after: Value = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object(
                 'id', id, 'status', status, 'runtime_id', runtime_id,
                 'session_ownership_generation', session_ownership_generation)
                 ORDER BY id), '[]'::jsonb)
             FROM runs WHERE runtime_id = $1",
        )
        .bind(fixture.runtime_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(runtime_state_after, runtime_state_before);
        assert_eq!(session_state_after, session_state_before);
        assert_eq!(run_state_after, run_state_before);

        let preview_recoverable_session_ids = impact
            .affected_sessions
            .iter()
            .filter(|session| session.force_delete_disposition == "recoverable")
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        let preview_recovery_failed_session_ids = impact
            .affected_sessions
            .iter()
            .filter(|session| session.force_delete_disposition == "recovery_failed")
            .map(|session| session.session_id)
            .collect::<Vec<_>>();
        let mismatch = force_delete_runtime(
            State(admin_state.clone()),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest {
                hostname: hostname.to_uppercase(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(mismatch.status, StatusCode::CONFLICT);

        let deleted = force_delete_runtime(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            deleted.recoverable_session_ids,
            preview_recoverable_session_ids
        );
        assert_eq!(
            deleted.recovery_failed_session_ids,
            preview_recovery_failed_session_ids
        );

        let rows = sqlx::query(
            "SELECT id, lifecycle_status, runtime_owner_id, ownership_generation,
                    active_turn_id, recovery_error, current_bundle_generation
             FROM hub_sessions
             WHERE id IN ($1, $2, $3, $4)",
        )
        .bind(fixture.hub_session_id)
        .bind(checkpointed_session_id)
        .bind(stale_bundle_session_id)
        .bind(delivering_session_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        for row in rows {
            let session_id: Uuid = row.get("id");
            assert_eq!(row.get::<Option<Uuid>, _>("runtime_owner_id"), None);
            assert_eq!(
                row.get::<i64, _>("ownership_generation"),
                if session_id == stale_bundle_session_id {
                    3
                } else {
                    2
                }
            );
            assert_eq!(row.get::<Option<Uuid>, _>("active_turn_id"), None);
            if session_id == checkpointed_session_id {
                assert_eq!(
                    row.get::<String, _>("lifecycle_status"),
                    "waiting_for_runtime"
                );
                assert_eq!(row.get::<Option<String>, _>("recovery_error"), None);
                assert_eq!(
                    row.get::<Option<i64>, _>("current_bundle_generation"),
                    Some(1)
                );
            } else {
                assert_eq!(row.get::<String, _>("lifecycle_status"), "recovery_failed");
                assert!(row.get::<Option<String>, _>("recovery_error").is_some());
                assert_eq!(
                    row.get::<Option<i64>, _>("current_bundle_generation"),
                    if session_id == fixture.hub_session_id {
                        None
                    } else {
                        Some(1)
                    }
                );
            }
        }
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id)
                .await
                .0,
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM runtimes WHERE id = $1")
                .bind(fixture.runtime_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        let old_credential = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(old_credential.status, StatusCode::UNAUTHORIZED);
        let old_write = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write_generation(
                1,
                AppendRunEventRequest {
                    event_id: Uuid::new_v4(),
                    event_type: "message".into(),
                    role: Some("assistant".into()),
                    content: Some("late".into()),
                    payload: json!({}),
                    waiting_tool: None,
                },
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(old_write.status, StatusCode::UNAUTHORIZED);
        let old_release = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(checkpointed_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(old_release.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn force_delete_requeues_a_claimed_restore_before_the_turn_starts(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let initial_claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(initial_claim.run.id),
            runtime_write_generation(
                1,
                CompleteRunRequest {
                    status: "completed".into(),
                    native_session_id: Some("restore-loss-thread".into()),
                    work_dir_ref: Some("restore-loss-workdir".into()),
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
            "hub/bundles/restore-loss.tar.zst",
            &SessionBundleCommitMetadata {
                checkpoint_attempt_id: attempt.checkpoint_attempt_id,
                bundle_generation: 1,
                checksum_sha256: "restore-loss".into(),
                size_bytes: 1024,
                history_checkpoint: attempt.history_checkpoint,
                producing_engine_version: "test".into(),
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();
        commit_tx.commit().await.unwrap();
        let _ = runtime_release_session(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.hub_session_id),
            Json(ReleaseRuntimeSessionRequest {
                ownership_generation: 1,
                force: false,
            }),
        )
        .await
        .unwrap();

        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let skill_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Restore Skill', 'restore', 'initial', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("initial"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let first_package_id = Uuid::new_v4();
        let first_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{first_package_id}.tar.zst");
        let first_package = staged_skill_package_upload("Restore Skill", "package A", "archive A");
        commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(first_package_id),
            Some(&first_object_key),
            &first_package,
        )
        .await
        .unwrap();

        let restoring_run_id =
            insert_pending_session_run(&fixture.state.pool, fixture.hub_session_id).await;
        let restoring_turn_id: Uuid =
            sqlx::query_scalar("SELECT hub_turn_id FROM runs WHERE id = $1")
                .bind(restoring_run_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let queued_sequence: i64 = sqlx::query_scalar(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'restore then run', 'next_turn',
                     'queued', $3, $4)
             RETURNING sequence",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.hub_session_id)
        .bind(restoring_turn_id)
        .bind(restoring_run_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET lifecycle_status = 'waiting_for_runtime', history_checkpoint = $1
             WHERE id = $2",
        )
        .bind(queued_sequence)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let restoring_claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(restoring_claim.run.id, restoring_run_id);
        assert_eq!(restoring_claim.run.status, "running");
        assert_eq!(
            restoring_claim
                .session_context
                .as_ref()
                .unwrap()
                .session
                .lifecycle_status,
            "restoring"
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT status, native_turn_id FROM hub_session_turns WHERE id = $1",
            )
            .bind(restoring_turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("pending".into(), None)
        );
        let restoring_package = restoring_claim
            .execution_configuration
            .skills
            .iter()
            .find(|skill| skill.source_id == Some(skill_id))
            .and_then(|skill| skill.package.as_ref())
            .unwrap();
        assert_eq!(restoring_package.id, first_package_id);

        let second_package_id = Uuid::new_v4();
        let second_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{second_package_id}.tar.zst");
        let second_package = staged_skill_package_upload("Restore Skill", "package B", "archive B");
        commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(second_package_id),
            Some(&second_object_key),
            &second_package,
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, Uuid>(
                "SELECT package_id FROM run_skill_packages
                 WHERE run_id = $1 AND skill_id = $2",
            )
            .bind(restoring_run_id)
            .bind(skill_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            first_package_id
        );

        let admin_token = create_super_admin_session(&fixture.state.pool).await;
        let admin_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        let hostname: String = sqlx::query_scalar("SELECT hostname FROM runtimes WHERE id = $1")
            .bind(fixture.runtime_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let deleted = force_delete_runtime(
            State(admin_state),
            session_headers(&admin_token),
            Path(fixture.runtime_id),
            Json(ConfirmRuntimeHostnameRequest { hostname }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            deleted.recoverable_session_ids,
            vec![fixture.hub_session_id]
        );
        assert!(deleted.recovery_failed_session_ids.is_empty());
        assert_eq!(
            sqlx::query_as::<_, (String, Option<Uuid>, Option<String>, Option<i64>)>(
                "SELECT status, runtime_id, model_proxy_token_hash,
                        session_ownership_generation
                 FROM runs WHERE id = $1",
            )
            .bind(restoring_run_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("pending".into(), None, None, Some(3))
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT status, native_turn_id FROM hub_session_turns WHERE id = $1",
            )
            .bind(restoring_turn_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("pending".into(), None)
        );

        let replacement_runtime_id = Uuid::new_v4();
        let replacement_token = format!("ahrt_{}", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status)
             VALUES ($1, $2, 'restore-loss-replacement', '{}', 'test',
                     '{\"model_proxy\":true}'::jsonb, 'workspace-write', 'online')",
        )
        .bind(replacement_runtime_id)
        .bind(sha256_hex(&replacement_token))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let reclaimed = claim_runtime_run(&fixture.state, &replacement_token).await;
        assert_eq!(reclaimed.run.id, restoring_run_id);
        assert_eq!(reclaimed.run.status, "running");
        assert_eq!(reclaimed.run.session_ownership_generation, Some(4));
        assert_eq!(
            reclaimed.execution_configuration.model_bindings,
            restoring_claim.execution_configuration.model_bindings
        );
        assert_eq!(
            reclaimed
                .execution_configuration
                .skills
                .iter()
                .find(|skill| skill.source_id == Some(skill_id))
                .and_then(|skill| skill.package.as_ref())
                .map(|package| package.id),
            Some(first_package_id)
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn concurrent_email_login_failures_enforce_the_fourth_attempt(pool: PgPool) {
        let email = "concurrent-limit@example.com";
        let password = password_hash("correct password").unwrap();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, $3, 'Concurrent Limit User', 'member')",
        )
        .bind(Uuid::new_v4())
        .bind(email)
        .bind(password)
        .execute(&pool)
        .await
        .unwrap();
        let app = build_router({
            let mut state = test_state_with_pool(pool.clone());
            state.auth_providers = vec![Arc::new(PasswordAuthProvider)];
            state
        });
        let login_request = |password: &'static str| {
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"email":"{email}","password":"{password}"}}"#
                )))
                .unwrap()
        };
        let responses = futures_util::future::join_all(
            (0..4).map(|_| app.clone().oneshot(login_request("wrong password"))),
        )
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
        assert_eq!(
            responses
                .iter()
                .filter(|response| response.status() == StatusCode::UNAUTHORIZED)
                .count(),
            3
        );
        let limited = responses
            .iter()
            .filter(|response| response.status() == StatusCode::TOO_MANY_REQUESTS)
            .collect::<Vec<_>>();
        assert_eq!(limited.len(), 1);
        assert!(limited[0].headers().get(header::RETRY_AFTER).is_some());

        let correct_while_limited = app
            .clone()
            .oneshot(login_request("correct password"))
            .await
            .unwrap();
        assert_eq!(
            correct_while_limited.status(),
            StatusCode::TOO_MANY_REQUESTS
        );

        let persisted: i32 = sqlx::query_scalar(
            "SELECT failed_attempts FROM login_email_failures
             WHERE normalized_email = $1",
        )
        .bind(email)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(persisted, 4);

        sqlx::query(
            "UPDATE login_email_failures
             SET window_started_at = now() - interval '6 minutes'
             WHERE normalized_email = $1",
        )
        .bind(email)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(
            app.oneshot(login_request("correct password"))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM login_email_failures WHERE normalized_email = $1"
            )
            .bind(email)
            .fetch_one(&pool)
            .await
            .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn super_admin_updates_platform_and_manages_user_password_sessions(pool: PgPool) {
        let administrator = create_hub_user(
            &pool,
            Some("admin-users@example.com"),
            None,
            Some("unused-admin-password"),
            true,
        )
        .await
        .unwrap();
        let target = create_hub_user(
            &pool,
            Some("external-password-target@example.com"),
            Some("External Password Target"),
            None,
            true,
        )
        .await
        .unwrap();
        let admin_token = "admin-users-session";
        for token in [admin_token, "target-session-one", "target-session-two"] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(if token == admin_token {
                administrator.id
            } else {
                target.id
            })
            .execute(&pool)
            .await
            .unwrap();
        }
        let api_key = new_api_key_token();
        sqlx::query(
            "INSERT INTO api_keys
                 (id, user_id, name, prefix, token_hash, expires_at)
             VALUES ($1, $2, 'preserved', $3, $4, now() + interval '90 days')",
        )
        .bind(Uuid::new_v4())
        .bind(target.id)
        .bind(api_key.chars().take(12).collect::<String>())
        .bind(sha256_hex(&api_key))
        .execute(&pool)
        .await
        .unwrap();
        let platform_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, 'admin-edit', 'Before')",
        )
        .bind(platform_id)
        .execute(&pool)
        .await
        .unwrap();
        let app = build_router(test_state_with_browser_session_auth(pool.clone()));

        let patch = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/api/admin/external-platforms/{platform_id}"))
                    .header(header::COOKIE, format!("agent_hub_session={admin_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"After"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(patch.status(), StatusCode::OK);
        let platform: ExternalPlatformDto = serde_json::from_slice(
            &axum::body::to_bytes(patch.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(platform.key, "admin-edit");
        assert_eq!(platform.name, "After");

        let list = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/admin/users")
                    .header(header::COOKIE, format!("agent_hub_session={admin_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let users: Vec<AdminUserDetailDto> = serde_json::from_slice(
            &axum::body::to_bytes(list.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(users.iter().any(|detail| detail.user.id == target.id));

        let detail = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/api/admin/users/{}", target.id))
                    .header(header::COOKIE, format!("agent_hub_session={admin_token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let detail: AdminUserDetailDto = serde_json::from_slice(
            &axum::body::to_bytes(detail.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(!detail.has_password);

        let updated = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/api/admin/users/{}", target.id))
                    .header(header::COOKIE, format!("agent_hub_session={admin_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"email":"updated-target@example.com","display_name":"Updated Target"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(updated.status(), StatusCode::OK);
        let updated: AdminUserDetailDto = serde_json::from_slice(
            &axum::body::to_bytes(updated.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(updated.user.email, "updated-target@example.com");
        assert_eq!(updated.user.display_name, "Updated Target");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sessions WHERE user_id = $1")
                .bind(target.id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(load_user_by_api_key(&pool, &api_key).await.is_ok());
        for token in ["target-session-three", "target-session-four"] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(target.id)
            .execute(&pool)
            .await
            .unwrap();
        }

        let changed = app
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/api/admin/users/{}/password", target.id))
                    .header(header::COOKIE, format!("agent_hub_session={admin_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"password":"new-password-123"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changed.status(), StatusCode::OK);
        let changed: AdminUserDetailDto = serde_json::from_slice(
            &axum::body::to_bytes(changed.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(changed.has_password);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sessions WHERE user_id = $1")
                .bind(target.id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert!(load_user_by_api_key(&pool, &api_key).await.is_ok());
    }

    #[test]
    fn subagent_main_binding_key_is_reserved_case_insensitively() {
        for name in ["main", "MAIN", " Main "] {
            let error = validate_subagent_definitions(&[SubagentDefinition {
                name: name.into(),
                description: "Reviews output".into(),
                developer_instructions: "Review carefully.".into(),
                model_selection: None,
                model_settings_override: AgentModelSettingsOverride::default(),
                enabled: true,
                disabled_reason: None,
            }])
            .unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn console_runtime_capabilities_only_keep_known_non_sensitive_fields() {
        let capabilities = console_runtime_capabilities(&json!({
            "driver": "pi",
            "engine_source": "bundled",
            "model_proxy": true,
            "mcp_allowlist": true,
            "native_session_resume": true,
            "local_skills": false,
            "sandbox_downgraded": true,
            "sandbox_downgrade_reason": "workspace is read-only",
            "sandbox": { "mount_token": "secret" },
            "unknown_secret": "do-not-return"
        }));

        assert_eq!(
            capabilities,
            json!({
                "driver": "pi",
                "engine_source": "bundled",
                "model_proxy": true,
                "mcp_allowlist": true,
                "native_session_resume": true,
                "local_skills": false,
                "sandbox_downgraded": true,
                "sandbox_downgrade_reason": "workspace is read-only"
            })
        );

        let schema =
            &openapi_document()["components"]["schemas"]["Runtime"]["properties"]["capabilities"];
        assert_eq!(schema["additionalProperties"], false);
        let properties = schema["properties"].as_object().unwrap();
        for key in [
            "driver",
            "engine_source",
            "model_proxy",
            "mcp_allowlist",
            "native_session_resume",
            "local_skills",
            "sandbox_downgraded",
            "sandbox_downgrade_reason",
        ] {
            assert!(
                properties.contains_key(key),
                "missing capability schema: {key}"
            );
        }
        for schema_name in ["Runtime", "RuntimeRegisterRequest"] {
            let runtime_schema = &openapi_document()["components"]["schemas"][schema_name];
            assert!(runtime_schema["properties"]
                .get("direct_model_enabled")
                .is_none());
            assert!(!runtime_schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("direct_model_enabled")));
        }
    }

    #[test]
    fn runtime_list_is_read_only_while_status_reaping_remains_background_owned() {
        let source = include_str!("api/runtimes.rs");
        let list = source
            .split_once("async fn list_runtimes(")
            .unwrap()
            .1
            .split_once("async fn create_runtime_enrollment_token(")
            .unwrap()
            .0;
        assert!(!list.contains("reap_stale_runtimes"));

        let background = source
            .split_once("async fn runtime_reaper_loop(")
            .unwrap()
            .1
            .split_once("async fn fail_capability_mismatched_runs_for_runtime_tx(")
            .unwrap()
            .0;
        assert!(background.contains("Duration::from_secs(5)"));
        assert!(background.contains("reap_stale_runtimes(&pool).await"));
    }

    #[test]
    fn atomic_tool_request_batch_rejects_duplicate_request_ids() {
        let request_id = Uuid::new_v4();
        let event = FinalizeToolRequestEvent {
            role: Some("assistant".into()),
            content: Some("lookup requested".into()),
            payload: json!({
                "tool_request_id": request_id,
                "tool_name": "lookup",
                "arguments": {}
            }),
        };
        let batch = FinalizeToolRequestsRequest {
            integration_session_id: Some(Uuid::new_v4()),
            native_session_id: "thread".into(),
            work_dir_ref: "workdir".into(),
            tool_requests: vec![event.clone(), event],
        };

        let error = match parse_tool_request_batch(&batch) {
            Err(error) => error,
            Ok(_) => panic!("duplicate request ids must be rejected before publication"),
        };

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.message,
            "tool request ids must be unique within a batch"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn atomic_tool_request_batch_publishes_two_requests_and_replays_exactly(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let second_request_id = Uuid::new_v4();
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id, second_request_id]);

        let first = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch.clone()),
        )
        .await
        .expect("the complete tool batch should commit");
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            3
        );
        let submitted = submit_integration_tool_result(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "answer": "raced with replay" }),
            }),
        )
        .await
        .expect("a committed request should be immediately submittable");
        let event_count_before_replay = run_event_count(&fixture.state.pool, fixture.run_id).await;
        let replay = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await
        .expect("an identical batch retry should be idempotent");

        assert_eq!(first.id, fixture.run_id);
        assert_eq!(first.status, "waiting_tool");
        assert_eq!(submitted.run.parent_run_id, Some(fixture.run_id));
        assert_eq!(submitted.run.hub_session_id, Some(fixture.hub_session_id));
        assert_ne!(submitted.run.hub_turn_id, Some(fixture.turn_id));
        assert_eq!(submitted.run.source, "integration:tool_result");
        assert_eq!(replay.id, first.id);
        assert_eq!(event_count_before_replay, 3);
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            event_count_before_replay
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_events
                 WHERE run_id = $1 AND event_type = 'tool_result'
                   AND hub_message_id IS NOT NULL",
            )
            .bind(submitted.run.id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            1
        );
        assert_eq!(
            waiting_tool_status_event_count(&fixture.state.pool, fixture.run_id).await,
            1
        );
        for request_id in [fixture.tool_request_id, second_request_id] {
            assert_eq!(tool_request_count(&fixture.state.pool, request_id).await, 1);
        }
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            (
                "waiting_tool".into(),
                Some(fixture.runtime_id),
                None,
                Some("integration-test-session".into()),
                Some("integration-test-workdir".into())
            )
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn atomic_tool_request_batch_rejects_partial_or_changed_replay(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let second_request_id = Uuid::new_v4();
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id, second_request_id]);
        let _ = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch.clone()),
        )
        .await
        .unwrap();

        let mut partial = batch.clone();
        partial.tool_requests.pop();
        let partial_error = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(partial),
        )
        .await
        .expect_err("a partial batch replay must fail closed");
        let mut changed = batch;
        changed.tool_requests[1].payload["arguments"] = json!({ "query": "changed" });
        let changed_error = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(changed),
        )
        .await
        .expect_err("a changed batch replay must fail closed");

        assert_eq!(partial_error.status, StatusCode::CONFLICT);
        assert_eq!(changed_error.status, StatusCode::CONFLICT);
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            3
        );
        assert_eq!(
            waiting_tool_status_event_count(&fixture.state.pool, fixture.run_id).await,
            1
        );
        for request_id in [fixture.tool_request_id, second_request_id] {
            assert_eq!(tool_request_count(&fixture.state.pool, request_id).await, 1);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn invalid_second_tool_request_rolls_back_entire_batch(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let second_request_id = Uuid::new_v4();
        let mut batch = tool_request_batch(&fixture, [fixture.tool_request_id, second_request_id]);
        batch.tool_requests[1].payload["tool_name"] = json!("not-registered");

        let result = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await;

        let error = result.expect_err("every tool in the batch must be registered");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            ("running".into(), Some(fixture.runtime_id), None, None, None)
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        for request_id in [fixture.tool_request_id, second_request_id] {
            assert_eq!(tool_request_count(&fixture.state.pool, request_id).await, 0);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn tool_request_batch_cannot_target_another_integration_session(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let mut batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        batch.integration_session_id = Some(Uuid::new_v4());

        let result = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await;

        let error = result.expect_err("the batch session must match its run");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
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
    async fn second_tool_request_insert_failure_rolls_back_entire_batch(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let second_request_id = Uuid::new_v4();
        let (trigger_name, function_name) =
            install_tool_request_failure_trigger(&fixture.state.pool, second_request_id).await;
        let batch = tool_request_batch(&fixture, [fixture.tool_request_id, second_request_id]);

        let result = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await;
        remove_tool_request_failure_trigger(&fixture.state.pool, &trigger_name, &function_name)
            .await;

        assert!(result.is_err());
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            ("running".into(), Some(fixture.runtime_id), None, None, None)
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        for request_id in [fixture.tool_request_id, second_request_id] {
            assert_eq!(tool_request_count(&fixture.state.pool, request_id).await, 0);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn legacy_single_tool_request_event_is_rejected_without_side_effects(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;

        let result = runtime_append_event(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(tool_request_event(fixture.tool_request_id)),
        )
        .await;

        let error = result.expect_err("legacy runtimes must not bypass atomic batch finalize");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
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
    async fn visible_tool_request_is_submittable_while_regular_completion_is_blocked(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let _ = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(tool_request_batch(&fixture, [fixture.tool_request_id])),
        )
        .await
        .expect("runtime should publish the tool request");

        let principal = require_integration(
            &fixture.state,
            &bearer_headers(&fixture.integration_token),
            fixture.agent_id,
        )
        .await
        .unwrap();
        let visible_events =
            load_integration_events_after(&fixture.state.pool, fixture.session_id, 0, &principal)
                .await
                .unwrap();
        let tool_event_seq = visible_events.iter().find_map(|event| {
            (event.event_type == "tool_request"
                && event.payload["tool_request_id"] == fixture.tool_request_id.to_string())
            .then_some(event.seq)
        });
        let waiting_status_seq = visible_events.iter().find_map(|event| {
            (event.event_type == "status" && event.payload["status"] == "waiting_tool")
                .then_some(event.seq)
        });
        let visible = tool_event_seq.is_some_and(|tool_seq| {
            waiting_status_seq.is_some_and(|status_seq| status_seq < tool_seq)
        });
        let completion_lock_key = fixture.run_id.as_u128() as i64;
        let mut completion_barrier = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(completion_lock_key)
            .execute(&mut *completion_barrier)
            .await
            .unwrap();
        let application_name = format!("blocked-runtime-completion-{}", Uuid::new_v4().simple());
        let completion_state = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_name).await,
        ));
        let completion_headers = bearer_headers(&fixture.runtime_token);
        let run_id = fixture.run_id;
        let mut completion_task = tokio::spawn(async move {
            let mut gate = completion_state.pool.begin().await.unwrap();
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(completion_lock_key)
                .execute(&mut *gate)
                .await
                .unwrap();
            gate.commit().await.unwrap();
            runtime_complete_run(
                State(completion_state),
                completion_headers,
                Path(run_id),
                runtime_write(CompleteRunRequest {
                    status: "waiting_tool".into(),
                    native_session_id: Some("integration-test-session".into()),
                    work_dir_ref: Some("integration-test-workdir".into()),
                }),
            )
            .await
        });
        let completion_wait_observed = wait_for_application_lock(
            &fixture.state.pool,
            &application_name,
            "SELECT pg_advisory_xact_lock",
        )
        .await;

        let submit_result = tokio::time::timeout(
            Duration::from_secs(3),
            submit_integration_tool_result(
                State(fixture.state.clone()),
                bearer_headers(&fixture.integration_token),
                Path(fixture.tool_request_id),
                Json(SubmitToolResultRequest {
                    result: json!({ "answer": "immediate" }),
                }),
            ),
        )
        .await;
        completion_barrier.commit().await.unwrap();
        let completion_outcome =
            match tokio::time::timeout(Duration::from_secs(3), &mut completion_task).await {
                Ok(outcome) => Some(outcome),
                Err(_) => {
                    completion_task.abort();
                    let _ = completion_task.await;
                    None
                }
            };

        assert!(
            visible,
            "the tool request event must be committed and queryable"
        );
        assert!(
            completion_wait_observed,
            "regular completion must be blocked before result submission"
        );
        let submission = submit_result
            .expect("immediate result submission must not hang")
            .expect("an observable tool request must accept a result before regular completion");
        assert_eq!(submission.run.parent_run_id, Some(fixture.run_id));
        assert_eq!(
            submission.run.integration_session_id,
            Some(fixture.session_id)
        );
        assert_eq!(
            submission.tool_request.follow_up_run_id,
            Some(submission.run.id)
        );
        let _ = completion_outcome
            .expect("regular completion should finish after barrier release")
            .expect("regular completion task should not panic")
            .expect("regular completion should remain idempotent");
        let mismatched_completion = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(CompleteRunRequest {
                status: "waiting_tool".into(),
                native_session_id: Some("different-session".into()),
                work_dir_ref: Some("integration-test-workdir".into()),
            }),
        )
        .await
        .expect_err("an idempotent completion must reject different metadata");
        assert_eq!(mismatched_completion.status, StatusCode::CONFLICT);
        assert_eq!(
            waiting_tool_status_event_count(&fixture.state.pool, fixture.run_id).await,
            1
        );
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            (
                "waiting_tool".into(),
                Some(fixture.runtime_id),
                None,
                Some("integration-test-session".into()),
                Some("integration-test-workdir".into())
            )
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn foreign_oauth_app_cannot_submit_tool_result_or_mutate_request(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let _ = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(tool_request_batch(&fixture, [fixture.tool_request_id])),
        )
        .await
        .unwrap();
        let before_request =
            tool_request_storage_state(&fixture.state.pool, fixture.tool_request_id).await;
        let before_run_count = agent_run_count(&fixture.state.pool, fixture.agent_id).await;
        let before_event_count = run_event_count(&fixture.state.pool, fixture.run_id).await;

        let foreign_app_id: Uuid = sqlx::query_scalar(
            "UPDATE oauth_access_tokens
             SET scopes = $1
             WHERE token_hash = $2
             RETURNING oauth_app_id",
        )
        .bind(vec![format!("agent:{}", fixture.agent_id)])
        .bind(sha256_hex(&fixture.foreign_integration_token))
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_app_agents (app_id, agent_id)
             VALUES ($1, $2)",
        )
        .bind(foreign_app_id)
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let result = submit_integration_tool_result(
            State(fixture.state.clone()),
            bearer_headers(&fixture.foreign_integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: json!({ "answer": "must be rejected" }),
            }),
        )
        .await;

        let error = result.expect_err("another OAuth app must not submit this request");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(
            tool_request_storage_state(&fixture.state.pool, fixture.tool_request_id).await,
            before_request
        );
        assert_eq!(
            agent_run_count(&fixture.state.pool, fixture.agent_id).await,
            before_run_count
        );
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            before_event_count
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn tool_request_without_waiting_metadata_is_not_published(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let mut batch = tool_request_batch(&fixture, [fixture.tool_request_id]);
        batch.native_session_id.clear();

        let result = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(batch),
        )
        .await;

        let error = result.expect_err("tool publication must include resume metadata");
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
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
    async fn sibling_tool_results_are_loaded_only_by_their_follow_up_run(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        let second_request_id = Uuid::new_v4();
        let _ = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(tool_request_batch(
                &fixture,
                [fixture.tool_request_id, second_request_id],
            )),
        )
        .await
        .unwrap();
        assert_eq!(
            waiting_tool_status_event_count(&fixture.state.pool, fixture.run_id).await,
            1
        );

        let first_payload = json!({ "request": "first", "value": 1 });
        let first_submission = submit_integration_tool_result(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.tool_request_id),
            Json(SubmitToolResultRequest {
                result: first_payload.clone(),
            }),
        )
        .await
        .unwrap();
        let first_claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(first_claim.run.id, first_submission.run.id);
        assert_eq!(first_claim.run.parent_run_id, Some(fixture.run_id));
        assert_eq!(
            first_claim
                .integration_context
                .as_ref()
                .and_then(|context| context.tool_result.as_ref()),
            Some(&first_payload)
        );
        let first_resume = first_claim
            .resume
            .as_ref()
            .expect("tool-result child must resume its parent session");
        assert_eq!(first_resume.native_session_id, "integration-test-session");
        assert_eq!(
            first_resume.work_dir_ref.as_deref(),
            Some("integration-test-workdir")
        );
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(first_claim.run.id),
            runtime_write(CompleteRunRequest {
                status: "completed".into(),
                native_session_id: Some("first-child-session".into()),
                work_dir_ref: Some("first-child-workdir".into()),
            }),
        )
        .await
        .unwrap();

        let second_payload = json!({ "request": "second", "value": 2 });
        let second_submission = submit_integration_tool_result(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(second_request_id),
            Json(SubmitToolResultRequest {
                result: second_payload.clone(),
            }),
        )
        .await
        .unwrap();
        let second_claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(second_claim.run.id, second_submission.run.id);
        assert_ne!(second_claim.run.id, first_claim.run.id);
        assert_eq!(second_claim.run.parent_run_id, Some(fixture.run_id));
        assert_eq!(
            second_claim
                .integration_context
                .as_ref()
                .and_then(|context| context.tool_result.as_ref()),
            Some(&second_payload)
        );
        assert_ne!(
            second_claim
                .integration_context
                .as_ref()
                .and_then(|context| context.tool_result.as_ref()),
            Some(&first_payload)
        );
        let _ = runtime_complete_run(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(second_claim.run.id),
            runtime_write(CompleteRunRequest {
                status: "completed".into(),
                native_session_id: Some("second-child-session".into()),
                work_dir_ref: Some("second-child-workdir".into()),
            }),
        )
        .await
        .unwrap();

        let mut reload_tx = fixture.state.pool.begin().await.unwrap();
        let first_child = load_run_public_tx(&mut reload_tx, first_claim.run.id)
            .await
            .unwrap();
        let reloaded_first_context = load_integration_context_for_run(&mut reload_tx, &first_child)
            .await
            .unwrap()
            .expect("the earlier child should retain its Integration context");
        reload_tx.commit().await.unwrap();
        assert_eq!(
            reloaded_first_context.tool_result.as_ref(),
            Some(&first_payload)
        );
        assert_ne!(
            reloaded_first_context.tool_result.as_ref(),
            Some(&second_payload)
        );
        assert_eq!(
            tool_request_follow_up_run(&fixture.state.pool, fixture.tool_request_id).await,
            Some(first_claim.run.id)
        );
        assert_eq!(
            tool_request_follow_up_run(&fixture.state.pool, second_request_id).await,
            Some(second_claim.run.id)
        );

        let next_message = create_integration_message(
            State(fixture.state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "continue after tool results".into(),
                attachments: json!([]),
                client_message_key: Some("after-tool-results".into()),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(next_message.run.source, "integration:message");
        assert_eq!(next_message.run.parent_run_id, Some(second_claim.run.id));

        let next_claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        assert_eq!(next_claim.run.id, next_message.run.id);
        assert_eq!(next_claim.run.source, "integration:message");
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archived_agent_wins_against_concurrent_tool_request_without_orphan_event(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        let mut archive_tx = fixture.state.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
            .bind(fixture.agent_id)
            .fetch_one(&mut *archive_tx)
            .await
            .unwrap();

        let application_name = format!("archived-tool-append-{}", Uuid::new_v4().simple());
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
        let append_wait_observed =
            wait_for_application_lock(&fixture.state.pool, &application_name, "SELECT a.id").await;
        let visible_before_archive_commit =
            run_event_count(&fixture.state.pool, fixture.run_id).await;

        sqlx::query("UPDATE agents SET deleted_at = now() WHERE id = $1")
            .bind(fixture.agent_id)
            .execute(&mut *archive_tx)
            .await
            .unwrap();
        sqlx::query("UPDATE runs SET status = 'failed' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&mut *archive_tx)
            .await
            .unwrap();
        archive_tx.commit().await.unwrap();

        let append_result = tokio::time::timeout(Duration::from_secs(3), append)
            .await
            .expect("runtime append should unblock after archive")
            .expect("runtime append task should not panic");
        assert!(
            append_wait_observed,
            "runtime append must wait on the archived Agent lock"
        );
        assert_eq!(visible_before_archive_commit, 0);
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
    async fn deleting_agent_scrubs_execution_config_and_preserves_historical_session(pool: PgPool) {
        let owner_id = Uuid::new_v4();
        let agent_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let turn_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let message_id = Uuid::new_v4();
        let pending_turn_id = Uuid::new_v4();
        let pending_run_id = Uuid::new_v4();
        let queued_message_id = Uuid::new_v4();
        let session_token = format!("ahs_{}", Uuid::new_v4().simple());
        let skill_id = Uuid::new_v4();
        let unique = Uuid::new_v4().simple().to_string();
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', 'Delete Agent Owner', 'member')",
        )
        .bind(owner_id)
        .bind(format!("delete-agent-{unique}@example.com"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(&session_token))
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, model_policy,
                  sandbox_policy, mcp_allowlist)
             VALUES ($1, $2, 'Historical Agent', 'private instructions', 'public',
                     '{\"model\":\"secret-model\"}'::jsonb,
                     '{\"mode\":\"workspace-write\"}'::jsonb,
                     '[{\"name\":\"mcp\",\"secrets\":{\"TOKEN\":\"secret\"}}]'::jsonb)",
        )
        .bind(agent_id)
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'managed', 'managed', 'private skill', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("private skill"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(agent_id)
            .bind(skill_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO hub_sessions
                 (id, owner_id, agent_id, origin_kind, lifecycle_status, history_checkpoint)
             VALUES ($1, $2, $3, 'hub_native', 'offline', 1)",
        )
        .bind(session_id)
        .bind(owner_id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation, started_at, ended_at)
             VALUES ($1, $2, 'completed', 0, now(), now())",
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
             VALUES ($1, $2, $3, 'completed', 'kept prompt', 'console', $4, $5, 0)",
        )
        .bind(run_id)
        .bind(agent_id)
        .bind(owner_id)
        .bind(session_id)
        .bind(turn_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'kept message', 'next_turn',
                     'delivered', $3, $4)",
        )
        .bind(message_id)
        .bind(session_id)
        .bind(turn_id)
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation)
             VALUES ($1, $2, 'pending', 0)",
        )
        .bind(pending_turn_id)
        .bind(session_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  hub_session_id, hub_turn_id, session_ownership_generation)
             VALUES ($1, $2, $3, 'pending', 'cancelled prompt', 'console', $4, $5, 0)",
        )
        .bind(pending_run_id)
        .bind(agent_id)
        .bind(owner_id)
        .bind(session_id)
        .bind(pending_turn_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', 'cancelled message', 'next_turn',
                     'queued', $3, $4)",
        )
        .bind(queued_message_id)
        .bind(session_id)
        .bind(pending_turn_id)
        .bind(pending_run_id)
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let deleted = delete_agent(
            State(state.clone()),
            session_headers(&session_token),
            Path(agent_id),
        )
        .await
        .unwrap();
        assert_eq!(deleted, StatusCode::NO_CONTENT);

        let agent: (String, String, Vec<Uuid>, Option<Uuid>, Value, Value, Value) = sqlx::query_as(
            "SELECT name, instructions, public_to, runtime_id, model_policy,
                        sandbox_policy, mcp_allowlist
                 FROM agents WHERE id = $1",
        )
        .bind(agent_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(agent.0, "Historical Agent");
        assert_eq!(agent.1, "");
        assert!(agent.2.is_empty());
        assert_eq!(agent.3, None);
        assert_eq!(agent.4, json!({}));
        assert_eq!(agent.5, json!({}));
        assert_eq!(agent.6, json!([]));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM agent_skills WHERE agent_id = $1")
                .bind(agent_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skills WHERE id = $1")
                .bind(skill_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );

        let historical = get_hub_session(
            State(state.clone()),
            session_headers(&session_token),
            Path(session_id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(historical.lifecycle_status, "historical");
        assert_eq!(historical.agent_name, "Historical Agent");
        assert!(historical.agent_deleted_at.is_some());
        let messages = list_hub_session_messages(
            State(state.clone()),
            session_headers(&session_token),
            Path(session_id),
            Query(SessionMessageListQuery::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].content.as_deref(), Some("kept message"));
        assert_eq!(messages[1].content.as_deref(), Some("cancelled message"));
        assert_eq!(messages[1].delivery_state, "failed");
        assert_eq!(
            get_run(
                State(state.clone()),
                session_headers(&session_token),
                Path(run_id),
            )
            .await
            .unwrap()
            .0
            .status,
            "completed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(pending_run_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM hub_session_turns WHERE id = $1")
                .bind(pending_turn_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT delivery_state FROM hub_session_messages WHERE id = $1"
            )
            .bind(queued_message_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            "failed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM run_events
                 WHERE run_id = $1 AND event_type = 'status'
                   AND payload->>'reason' = 'agent deleted'"
            )
            .bind(pending_run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );

        let message_error = create_hub_session_message(
            State(state.clone()),
            session_headers(&session_token),
            Path(session_id),
            Json(CreateHubSessionMessageRequest {
                attachment_ids: Vec::new(),
                content: "must not continue".into(),
                payload: json!({}),
                delivery_mode: None,
                client_message_key: None,
                parent_run_id: Some(run_id),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(message_error.status, StatusCode::NOT_FOUND);
        let run_error = create_run(
            State(state.clone()),
            session_headers(&session_token),
            Path(agent_id),
            Json(CreateRunRequest {
                message: "must not start".into(),
                hub_session_id: Some(session_id),
                parent_run_id: Some(run_id),
                client_message_key: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(run_error.status, StatusCode::NOT_FOUND);
        assert_eq!(
            delete_agent(
                State(state),
                session_headers(&session_token),
                Path(agent_id),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn deleting_agent_revokes_delegation_but_keeps_app_and_completed_history(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions SET native_session_id = 'historical-thread' WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let agent_owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let session_owner_id: Uuid =
            sqlx::query_scalar("SELECT owner_id FROM hub_sessions WHERE id = $1")
                .bind(fixture.hub_session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap();
        let oauth_app: (Uuid, Uuid, String) = sqlx::query_as(
            "SELECT id, authentication_channel_id, client_id
             FROM oauth_apps
             WHERE id = (SELECT app_id FROM integration_app_agents WHERE agent_id = $1)",
        )
        .bind(fixture.agent_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        let redirect_uris = json!(["https://client.example/callback"]);
        sqlx::query("UPDATE oauth_apps SET redirect_uris = $1 WHERE id = $2")
            .bind(&redirect_uris)
            .bind(oauth_app.0)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let owner_token = format!("ahs_{}", Uuid::new_v4().simple());
        let session_owner_token = format!("ahs_{}", Uuid::new_v4().simple());
        for (token, user_id) in [
            (&owner_token, agent_owner_id),
            (&session_owner_token, session_owner_id),
        ] {
            sqlx::query(
                "INSERT INTO sessions (token_hash, user_id, expires_at)
                 VALUES ($1, $2, now() + interval '1 hour')",
            )
            .bind(sha256_hex(token))
            .bind(user_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        }

        let skill_id = Uuid::new_v4();
        let skill_content = "managed secret skill";
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Managed secret', 'test', $3, $4)",
        )
        .bind(skill_id)
        .bind(agent_owner_id)
        .bind(skill_content)
        .bind(format!("{:x}", Sha256::digest(skill_content.as_bytes())))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let automation_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO automations
                 (id, agent_id, owner_id, name, trigger_type, prompt,
                  webhook_token_hash, enabled)
             VALUES ($1, $2, $3, 'Delete me', 'webhook', 'secret prompt',
                     'secret-webhook-hash', true)",
        )
        .bind(automation_id)
        .bind(fixture.agent_id)
        .bind(agent_owner_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        let widget_token = format!("ahe_{}", Uuid::new_v4().simple());
        let mut widget_tx = fixture.state.pool.begin().await.unwrap();
        insert_embed_session_tx(
            &mut widget_tx,
            fixture.agent_id,
            agent_owner_id,
            None,
            &widget_token,
            Utc::now() + ChronoDuration::hours(1),
        )
        .await
        .unwrap();
        widget_tx.commit().await.unwrap();
        let external_identity_id: Uuid = sqlx::query_scalar(
            "SELECT origin_external_identity_id FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO oauth_authorization_codes
                 (code_hash, oauth_app_id, redirect_uri, expires_at, subject_user_id,
                  external_identity_id, tenant_id, scopes)
             VALUES ('secret-code-hash', $1, 'https://client.example/callback',
                     now() + interval '5 minutes', $2, $3, 'fixture-tenant', $4)",
        )
        .bind(oauth_app.0)
        .bind(session_owner_id)
        .bind(external_identity_id)
        .bind(vec![format!("agent:{}", fixture.agent_id)])
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let completed_turn_id = Uuid::new_v4();
        let completed_run_id = Uuid::new_v4();
        let completed_message_id = Uuid::new_v4();
        let completed_tool_id = Uuid::new_v4();
        let attachment_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO hub_session_turns
                 (id, session_id, status, ownership_generation, started_at, ended_at)
             VALUES ($1, $2, 'completed', 1, now(), now())",
        )
        .bind(completed_turn_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs
                 (id, agent_id, owner_id, status, initial_message, source,
                  integration_session_id, hub_session_id, hub_turn_id,
                  session_ownership_generation)
             VALUES ($1, $2, $3, 'completed', 'kept integration prompt',
                     'integration:message', $4, $5, $6, 1)",
        )
        .bind(completed_run_id)
        .bind(fixture.agent_id)
        .bind(session_owner_id)
        .bind(fixture.session_id)
        .bind(fixture.hub_session_id)
        .bind(completed_turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, payload,
                  delivery_mode, delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'assistant', 'message', 'kept integration response',
                     '{}'::jsonb, 'record_only', 'delivered', $3, $4)",
        )
        .bind(completed_message_id)
        .bind(fixture.hub_session_id)
        .bind(completed_turn_id)
        .bind(completed_run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE runs SET hub_message_id = $1 WHERE id = $2")
            .bind(completed_message_id)
            .bind(completed_run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO integration_messages
                 (id, session_id, run_id, role, content, attachments, hub_message_id)
             VALUES ($1, $2, $3, 'assistant', 'kept integration response',
                     '[]'::jsonb, $4)",
        )
        .bind(Uuid::new_v4())
        .bind(fixture.session_id)
        .bind(completed_run_id)
        .bind(completed_message_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_attachments
                 (id, session_id, run_id, kind, name, content_type, size_bytes,
                  text, hub_message_id)
             VALUES ($1, $2, $3, 'text', 'history.txt', 'text/plain', 7,
                     'history', $4)",
        )
        .bind(attachment_id)
        .bind(fixture.session_id)
        .bind(completed_run_id)
        .bind(completed_message_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_tool_requests
                 (id, session_id, hub_session_id, run_id, tool_name, arguments, status,
                  result_payload, expires_at, responded_at)
             VALUES ($1, $2, $3, $4, 'lookup', '{\"key\":\"kept\"}'::jsonb,
                     'completed', '{\"value\":\"kept\"}'::jsonb,
                     now() + interval '5 minutes', now())",
        )
        .bind(completed_tool_id)
        .bind(fixture.session_id)
        .bind(fixture.hub_session_id)
        .bind(completed_run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO integration_tool_requests
                 (id, session_id, hub_session_id, run_id, tool_name, arguments, status, expires_at)
             VALUES ($1, $2, $3, $4, 'lookup', '{}'::jsonb, 'pending',
                     now() + interval '5 minutes')",
        )
        .bind(fixture.tool_request_id)
        .bind(fixture.session_id)
        .bind(fixture.hub_session_id)
        .bind(fixture.run_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        assert_eq!(
            delete_agent(
                State(state.clone()),
                session_headers(&owner_token),
                Path(fixture.agent_id),
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        assert_eq!(
            get_run(
                State(state.clone()),
                session_headers(&session_owner_token),
                Path(completed_run_id),
            )
            .await
            .unwrap()
            .0
            .status,
            "completed"
        );
        let history = list_hub_session_messages(
            State(state.clone()),
            session_headers(&session_owner_token),
            Path(fixture.hub_session_id),
            Query(SessionMessageListQuery::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(history.len(), 1);
        assert_eq!(
            history[0].content.as_deref(),
            Some("kept integration response")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT native_session_id FROM hub_sessions WHERE id = $1"
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "historical-thread"
        );
        assert_eq!(
            sqlx::query_as::<_, (String, Value)>(
                "SELECT status, result_payload FROM integration_tool_requests WHERE id = $1"
            )
            .bind(completed_tool_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            ("completed".into(), json!({ "value": "kept" }))
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM integration_tool_requests WHERE id = $1"
            )
            .bind(fixture.tool_request_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "cancelled"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT text FROM integration_attachments WHERE id = $1"
            )
            .bind(attachment_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            "history"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM integration_sessions WHERE id = $1")
                .bind(fixture.session_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM oauth_access_tokens WHERE oauth_app_id = $1"
            )
            .bind(oauth_app.0)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM oauth_authorization_codes WHERE oauth_app_id = $1"
            )
            .bind(oauth_app.0)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            1
        );
        let active_app: (Option<String>, Value, Option<DateTime<Utc>>) = sqlx::query_as(
            "SELECT client_secret_hash, redirect_uris, deleted_at
             FROM oauth_apps WHERE id = $1",
        )
        .bind(oauth_app.0)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(active_app.0.as_deref(), Some("unused"));
        assert_eq!(active_app.1, redirect_uris);
        assert!(active_app.2.is_none());
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT enabled FROM authentication_channels WHERE id = $1"
        )
        .bind(oauth_app.1)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM integration_app_agents
                 WHERE app_id = $1 AND agent_id = $2",
            )
            .bind(oauth_app.0)
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM automations WHERE id = $1")
                .bind(automation_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM agent_skills WHERE agent_id = $1")
                .bind(fixture.agent_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM embed_sessions WHERE agent_id = $1")
                .bind(fixture.agent_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );

        let integration_error = create_integration_message(
            State(state.clone()),
            bearer_headers(&fixture.integration_token),
            Path(fixture.session_id),
            Json(CreateIntegrationMessageRequest {
                content: "must not continue".into(),
                attachments: json!([]),
                client_message_key: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(integration_error.status, StatusCode::FORBIDDEN);
        let authorize_error = oauth_authorize(
            State(state.clone()),
            session_headers(&owner_token),
            Query(OAuthAuthorizeQuery {
                client_id: oauth_app.2.clone(),
                redirect_uri: "https://client.example/callback".into(),
                state: None,
                scope: None,
                external_user_id: "deleted-external-user".into(),
                tenant_id: "deleted-tenant".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(authorize_error.status, StatusCode::FORBIDDEN);
        let token_error = oauth_token(
            State(state.clone()),
            Form(OAuthTokenForm {
                grant_type: "authorization_code".into(),
                client_id: oauth_app.2,
                client_secret: "deleted-secret".into(),
                code: Some("deleted-code".into()),
                redirect_uri: Some("https://client.example/callback".into()),
                scope: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(token_error.status, StatusCode::UNAUTHORIZED);
        let mut widget_headers = HeaderMap::new();
        widget_headers.insert(
            HeaderName::from_static("x-agent-hub-embed-token"),
            HeaderValue::from_str(&widget_token).unwrap(),
        );
        let widget_error = create_widget_run(
            State(state),
            widget_headers,
            Json(CreateWidgetRunRequest {
                message: "must not continue".into(),
                session_id: None,
                integration_session_id: None,
                hub_session_id: None,
                parent_run_id: None,
                client_message_key: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(widget_error.status, StatusCode::UNAUTHORIZED);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn deleted_agent_fences_runtime_ownership_without_trapping_stale_heartbeat(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let claim = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
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
        let delete_state = Arc::new(test_state_with_browser_session_auth(
            fixture.state.pool.clone(),
        ));
        delete_agent(
            State(delete_state),
            session_headers(&session_token),
            Path(fixture.agent_id),
        )
        .await
        .unwrap();

        let heartbeat = runtime_heartbeat(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Json(RuntimeHeartbeatRequest {
                pending_credential_hash: None,
                accepts_session_commands: true,
                owned_sessions: vec![RuntimeOwnedSessionStateRequest {
                    session_id: fixture.hub_session_id,
                    ownership_generation: 1,
                    lifecycle_status: "restoring".into(),
                    checkpoint_reason: None,
                }],
                cleaned_sessions: Vec::new(),
            }),
        )
        .await
        .expect("a fenced historical Session must not trap its former Runtime")
        .0;

        assert!(heartbeat.owned_sessions.is_empty());
        assert!(heartbeat.session_commands.is_empty());
        let session: (String, Option<Uuid>, i64) = sqlx::query_as(
            "SELECT lifecycle_status, runtime_owner_id, ownership_generation
             FROM hub_sessions WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .fetch_one(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(session, ("historical".into(), None, 2));
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM runs WHERE id = $1")
                .bind(claim.run.id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            "failed"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn duplicate_tool_request_rolls_back_its_run_event(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "INSERT INTO integration_tool_requests
              (id, session_id, run_id, hub_session_id, tool_name, arguments, status, expires_at)
              VALUES ($1, $2, $3, $4, 'lookup', '{}'::jsonb, 'pending', now() + interval '30 minutes')",
        )
        .bind(fixture.tool_request_id)
        .bind(fixture.session_id)
        .bind(fixture.run_id)
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let append_result = runtime_finalize_tool_requests(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path(fixture.run_id),
            runtime_write(tool_request_batch(&fixture, [fixture.tool_request_id])),
        )
        .await;

        assert!(append_result.is_err());
        assert_eq!(
            run_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
        assert_eq!(
            tool_request_count(&fixture.state.pool, fixture.tool_request_id).await,
            1
        );
        assert_eq!(
            runtime_completion_run_state(&fixture.state.pool, fixture.run_id).await,
            ("running".into(), Some(fixture.runtime_id), None, None, None)
        );
        assert_eq!(
            waiting_tool_status_event_count(&fixture.state.pool, fixture.run_id).await,
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn stale_webhook_token_cannot_create_run_after_concurrent_disable_or_rotation(
        pool: PgPool,
    ) {
        for mutation in ["disable", "rotate"] {
            let fixture = automation_update_fixture(pool.clone()).await;
            let old_token = format!("ahw_{}", Uuid::new_v4().simple());
            let new_hash = sha256_hex(&format!("ahw_{}", Uuid::new_v4().simple()));
            sqlx::query(
                "UPDATE automations
                 SET trigger_type = 'webhook', webhook_token_hash = $1, enabled = true
                 WHERE id = $2",
            )
            .bind(sha256_hex(&old_token))
            .bind(fixture.automation_id)
            .execute(&fixture.pool)
            .await
            .unwrap();

            let suffix = Uuid::new_v4().simple().to_string();
            let mutation_application = format!("webhook-{mutation}-mutation-{suffix}");
            let webhook_application = format!("webhook-{mutation}-trigger-{suffix}");
            let mutation_pool =
                postgres_test_pool_with_application_name(&fixture.pool, &mutation_application)
                    .await;
            let webhook_state = Arc::new(test_state_with_browser_session_auth(
                postgres_test_pool_with_application_name(&fixture.pool, &webhook_application).await,
            ));
            let mut barrier = fixture.pool.begin().await.unwrap();
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
                .bind(fixture.agent_id)
                .fetch_one(&mut *barrier)
                .await
                .unwrap();

            let agent_id = fixture.agent_id;
            let automation_id = fixture.automation_id;
            let mutation_name = mutation.to_owned();
            let mut mutation_task = tokio::spawn(async move {
                let mut tx = mutation_pool.begin().await.unwrap();
                sqlx::query_scalar::<_, Uuid>(
                    "SELECT id FROM agents WHERE id = $1 AND deleted_at IS NULL FOR UPDATE",
                )
                .bind(agent_id)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                sqlx::query_scalar::<_, Uuid>(
                    "SELECT id FROM automations WHERE id = $1 FOR UPDATE",
                )
                .bind(automation_id)
                .fetch_one(&mut *tx)
                .await
                .unwrap();
                if mutation_name == "disable" {
                    sqlx::query("UPDATE automations SET enabled = false WHERE id = $1")
                        .bind(automation_id)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                } else {
                    sqlx::query("UPDATE automations SET webhook_token_hash = $1 WHERE id = $2")
                        .bind(new_hash)
                        .bind(automation_id)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                }
                tx.commit().await.unwrap();
            });
            assert!(
                wait_for_application_lock(
                    &fixture.pool,
                    &mutation_application,
                    "SELECT id FROM agents",
                )
                .await
            );

            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static("x-agent-hub-webhook-token"),
                HeaderValue::from_str(&old_token).unwrap(),
            );
            let mut webhook_task = tokio::spawn(async move {
                trigger_automation_webhook(
                    State(webhook_state),
                    headers,
                    Json(TriggerAutomationRequest { message: None }),
                )
                .await
            });
            assert!(
                wait_for_application_lock(
                    &fixture.pool,
                    &webhook_application,
                    "SELECT id\n         FROM agents",
                )
                .await
            );
            barrier.commit().await.unwrap();

            tokio::time::timeout(Duration::from_secs(3), &mut mutation_task)
                .await
                .expect("webhook mutation should not deadlock")
                .unwrap();
            let error = tokio::time::timeout(Duration::from_secs(3), &mut webhook_task)
                .await
                .expect("stale webhook trigger should not deadlock")
                .unwrap()
                .expect_err("stale webhook token must be rejected");
            assert_eq!(error.status, StatusCode::UNAUTHORIZED);
            let runs: i64 =
                sqlx::query_scalar("SELECT count(*) FROM runs WHERE automation_id = $1")
                    .bind(fixture.automation_id)
                    .fetch_one(&fixture.pool)
                    .await
                    .unwrap();
            assert_eq!(runs, 0, "{mutation} must not create a webhook run");
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archive_queued_before_automation_update_wins_without_deadlock(pool: PgPool) {
        let fixture = automation_update_fixture(pool).await;
        let suffix = Uuid::new_v4().simple().to_string();
        let archive_application = format!("automation-update-archive-{suffix}");
        let update_application = format!("automation-update-request-{suffix}");
        let archive_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.pool, &archive_application).await,
        ));
        let update_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.pool, &update_application).await,
        ));
        let mut barrier = fixture.pool.begin().await.unwrap();
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
            .bind(fixture.agent_id)
            .fetch_one(&mut *barrier)
            .await
            .unwrap();

        let agent_id = fixture.agent_id;
        let archive_headers = session_headers(&fixture.owner_session);
        let mut archive_task = tokio::spawn(async move {
            delete_agent(State(archive_state), archive_headers, Path(agent_id)).await
        });
        assert!(
            wait_for_application_lock(
                &fixture.pool,
                &archive_application,
                "SELECT agents.owner_id, agents.deleted_at",
            )
            .await
        );

        let automation_id = fixture.automation_id;
        let update_headers = session_headers(&fixture.owner_session);
        let mut update_task = tokio::spawn(async move {
            update_automation(
                State(update_state),
                update_headers,
                Path(automation_id),
                Json(UpdateAutomationRequest {
                    name: "Must not persist".into(),
                    trigger_type: "webhook".into(),
                    prompt: "Must not persist".into(),
                    schedule: None,
                    enabled: true,
                }),
            )
            .await
        });
        assert!(
            wait_for_application_lock(&fixture.pool, &update_application, "SELECT id FROM agents",)
                .await
        );
        barrier.commit().await.unwrap();

        let archive_result = tokio::time::timeout(Duration::from_secs(3), &mut archive_task)
            .await
            .expect("archive should not deadlock")
            .unwrap()
            .unwrap();
        assert_eq!(archive_result, StatusCode::NO_CONTENT);
        let update_error = tokio::time::timeout(Duration::from_secs(3), &mut update_task)
            .await
            .expect("update should not deadlock")
            .unwrap()
            .unwrap_err();
        assert_eq!(update_error.status, StatusCode::NOT_FOUND);
        let automation_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM automations WHERE id = $1")
                .bind(fixture.automation_id)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(automation_count, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn archive_queued_before_scheduler_returns_none_without_creating_run(pool: PgPool) {
        let fixture = scheduler_archive_fixture(pool).await;
        let suffix = Uuid::new_v4().simple().to_string();
        let archive_application = format!("scheduler-archive-{suffix}");
        let scheduler_application = format!("scheduler-trigger-{suffix}");
        let archive_state = Arc::new(test_state_with_browser_session_auth(
            postgres_test_pool_with_application_name(&fixture.pool, &archive_application).await,
        ));
        let scheduler_pool =
            postgres_test_pool_with_application_name(&fixture.pool, &scheduler_application).await;

        let mut barrier = fixture.pool.begin().await.unwrap();
        let locked_agent: Uuid =
            sqlx::query_scalar("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
                .bind(fixture.agent_id)
                .fetch_one(&mut *barrier)
                .await
                .unwrap();
        assert_eq!(locked_agent, fixture.agent_id);

        let agent_id = fixture.agent_id;
        let archive_headers = session_headers(&fixture.session_token);
        let mut archive_task = tokio::spawn(async move {
            delete_agent(State(archive_state), archive_headers, Path(agent_id)).await
        });
        let archive_wait_observed = wait_for_application_lock(
            &fixture.pool,
            &archive_application,
            "SELECT agents.owner_id, agents.deleted_at",
        )
        .await;

        sqlx::query("UPDATE automations SET enabled = true WHERE id = $1")
            .bind(fixture.automation_id)
            .execute(&fixture.pool)
            .await
            .unwrap();
        let automation_id = fixture.automation_id;
        let mut scheduler_task = tokio::spawn(async move {
            trigger_scheduled_automation_if_due(&scheduler_pool, automation_id, Utc::now()).await
        });
        let overlap_observed = wait_for_scheduler_archive_lock_overlap(
            &fixture.pool,
            &archive_application,
            &scheduler_application,
        )
        .await;

        barrier.commit().await.unwrap();
        let archive_outcome =
            match tokio::time::timeout(Duration::from_secs(3), &mut archive_task).await {
                Ok(outcome) => Some(outcome),
                Err(_) => {
                    archive_task.abort();
                    let _ = archive_task.await;
                    None
                }
            };
        let scheduler_outcome =
            match tokio::time::timeout(Duration::from_secs(3), &mut scheduler_task).await {
                Ok(outcome) => Some(outcome),
                Err(_) => {
                    scheduler_task.abort();
                    let _ = scheduler_task.await;
                    None
                }
            };
        let postconditions = scheduler_archive_postconditions(
            &fixture.pool,
            fixture.agent_id,
            fixture.automation_id,
        )
        .await;
        cleanup_scheduler_archive_fixture(&fixture.pool, fixture.owner_id).await;

        assert!(
            archive_wait_observed,
            "archive must queue on the Agent lock before scheduler starts"
        );
        assert!(
            overlap_observed,
            "scheduler and archive transactions must overlap on the Agent lock"
        );
        let archive_result = archive_outcome
            .expect("archive should finish after the barrier is released")
            .expect("archive task should not panic");
        assert_eq!(
            archive_result.expect("archive should complete normally"),
            StatusCode::NO_CONTENT
        );
        let scheduler_result = scheduler_outcome
            .expect("scheduler should finish after archive commits")
            .expect("scheduler task should not panic");
        match scheduler_result {
            Ok(None) => {}
            Ok(Some(run)) => panic!("archive-first scheduler created run {}", run.id),
            Err(error) => panic!("archive-first scheduler returned error: {}", error.message),
        }
        assert!(postconditions.archived);
        assert_eq!(postconditions.enabled_automations, 0);
        assert_eq!(postconditions.scheduler_runs, 0);
        assert_eq!(postconditions.active_runs, 0);
        assert_eq!(postconditions.post_archive_runs, 0);
        assert!(postconditions.last_triggered_at.is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn concurrent_integration_messages_are_preserved_on_one_active_run(pool: PgPool) {
        let fixture = integration_runtime_fixture(pool).await;
        sqlx::query(
            "UPDATE hub_sessions
             SET active_turn_id = NULL, lifecycle_status = 'waiting_for_runtime'
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE hub_session_turns SET status = 'completed', ended_at = now()
             WHERE id = $1",
        )
        .bind(fixture.turn_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("DELETE FROM runs WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let suffix = Uuid::new_v4().simple().to_string();
        let application_a = format!("integration-message-a-{suffix}");
        let application_b = format!("integration-message-b-{suffix}");
        let state_a = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_a).await,
        ));
        let state_b = Arc::new(test_state_with_pool(
            postgres_test_pool_with_application_name(&fixture.state.pool, &application_b).await,
        ));
        let mut barrier = fixture.state.pool.begin().await.unwrap();
        let barrier_agent: Uuid =
            sqlx::query_scalar("SELECT id FROM agents WHERE id = $1 FOR UPDATE")
                .bind(fixture.agent_id)
                .fetch_one(&mut *barrier)
                .await
                .unwrap();
        assert_eq!(barrier_agent, fixture.agent_id);

        let session_id = fixture.session_id;
        let token_a = fixture.integration_token.clone();
        let first_suffix = suffix.clone();
        let first = tokio::spawn(async move {
            create_integration_message(
                State(state_a),
                bearer_headers(&token_a),
                Path(session_id),
                Json(CreateIntegrationMessageRequest {
                    content: "first concurrent message".into(),
                    attachments: json!([]),
                    client_message_key: Some(format!("first-{first_suffix}")),
                }),
            )
            .await
        });
        let session_id = fixture.session_id;
        let token_b = fixture.second_integration_token.clone();
        let second_suffix = suffix.clone();
        let second = tokio::spawn(async move {
            create_integration_message(
                State(state_b),
                bearer_headers(&token_b),
                Path(session_id),
                Json(CreateIntegrationMessageRequest {
                    content: "second concurrent message".into(),
                    attachments: json!([]),
                    client_message_key: Some(format!("second-{second_suffix}")),
                }),
            )
            .await
        });

        let applications = vec![application_a, application_b];
        let overlap_observed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if blocked_test_application_count(&fixture.state.pool, &applications).await == 2 {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false);
        barrier.commit().await.unwrap();
        let first_result = tokio::time::timeout(Duration::from_secs(3), first)
            .await
            .expect("first message should finish after barrier release")
            .expect("first message task should not panic");
        let second_result = tokio::time::timeout(Duration::from_secs(3), second)
            .await
            .expect("second message should finish after barrier release")
            .expect("second message task should not panic");

        assert!(overlap_observed, "both message transactions must overlap");
        let first = first_result
            .expect("first concurrent message must be accepted")
            .0;
        let second = second_result
            .expect("second concurrent message must be accepted")
            .0;
        assert_eq!(first.run.id, second.run.id);
        assert_eq!(first.run.hub_session_id, Some(fixture.hub_session_id));
        assert_eq!(second.run.hub_session_id, Some(fixture.hub_session_id));
        assert_ne!(first.message.id, second.message.id);
        let accepted: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence, client_message_key
             FROM hub_session_messages
             WHERE session_id = $1
             ORDER BY sequence",
        )
        .bind(fixture.hub_session_id)
        .fetch_all(&fixture.state.pool)
        .await
        .unwrap();
        assert_eq!(accepted.len(), 2);
        assert_eq!(
            accepted.iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            accepted
                .iter()
                .map(|row| row.1.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([format!("first-{suffix}"), format!("second-{suffix}"),])
        );
        assert_eq!(
            active_integration_run_count(&fixture.state.pool, fixture.session_id).await,
            1
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn dev_runtime_bootstrap_is_idempotent_but_not_replayable(pool: PgPool) {
        let bootstrap_token = "dev-only-one-time-runtime-enrollment";
        ensure_dev_runtime_enrollment_token(&pool, bootstrap_token)
            .await
            .unwrap();
        ensure_dev_runtime_enrollment_token(&pool, bootstrap_token)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM runtime_enrollment_tokens WHERE token_hash = $1",
            )
            .bind(sha256_hex(bootstrap_token))
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
        let state = Arc::new(test_state_with_pool(pool));
        let request = RuntimeRegisterRequest {
            hostname: "fresh-compose-runtime".into(),
            labels: vec!["development".into()],
            engine_version: "test".into(),
            capabilities: json!({}),
            sandbox_mode: "workspace-write".into(),
        };
        let _enrolled = runtime_register(
            State(Arc::clone(&state)),
            bearer_headers(bootstrap_token),
            Json(request.clone()),
        )
        .await
        .unwrap();

        ensure_dev_runtime_enrollment_token(&state.pool, bootstrap_token)
            .await
            .unwrap();
        let replay = runtime_register(
            State(Arc::clone(&state)),
            bearer_headers(bootstrap_token),
            Json(request),
        )
        .await
        .unwrap_err();
        assert_eq!(replay.status, StatusCode::UNAUTHORIZED);
        let row: (i64, bool) = sqlx::query_as(
            "SELECT count(*), bool_and(consumed_at IS NOT NULL)
             FROM runtime_enrollment_tokens
             WHERE token_hash = $1",
        )
        .bind(sha256_hex(bootstrap_token))
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(row, (1, true));
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn revoked_runtime_credential_cannot_heartbeat_or_claim(pool: PgPool) {
        let state = Arc::new(test_state_with_pool(pool));
        let runtime_id = Uuid::new_v4();
        let credential = format!(
            "ahrc_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        sqlx::query(
            "INSERT INTO runtimes
                 (id, token_hash, hostname, labels, engine_version, capabilities,
                  sandbox_mode, status, credential_revoked_at)
             VALUES ($1, $2, 'revoked-runtime', '{}', 'test', '{}'::jsonb,
                     'workspace-write', 'online', now())",
        )
        .bind(runtime_id)
        .bind(sha256_hex(&credential))
        .execute(&state.pool)
        .await
        .unwrap();

        let heartbeat_error = runtime_heartbeat(
            State(Arc::clone(&state)),
            bearer_headers(&credential),
            Json(RuntimeHeartbeatRequest::default()),
        )
        .await
        .unwrap_err();
        assert_eq!(heartbeat_error.status, StatusCode::UNAUTHORIZED);
        assert!(require_runtime(&state, &bearer_headers(&credential))
            .await
            .is_err());
        let claim_error = match runtime_claim_run(
            State(Arc::clone(&state)),
            bearer_headers(&credential),
            runtime_claim_request(1, Vec::new()),
        )
        .await
        {
            Ok(_) => panic!("revoked Runtime credential unexpectedly claimed a Run"),
            Err(error) => error,
        };
        assert_eq!(claim_error.status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn runtime_claim_queries_lock_agent_before_run_and_session_rows() {
        let candidate_sql = runtime_claim_candidate_sql();
        let agent_sql = runtime_claim_agent_sql();
        let run_sql = runtime_claim_run_sql();

        assert!(candidate_sql.contains("JOIN agents a ON a.id = r.agent_id"));
        assert!(candidate_sql.contains("JOIN runtimes rt ON rt.id = $1"));
        assert!(!candidate_sql.contains("FOR UPDATE"));
        assert!(!candidate_sql.contains("FOR SHARE"));
        assert!(agent_sql.contains("FOR SHARE OF a"));
        assert!(!agent_sql.contains("JOIN runs"));
        assert!(run_sql.contains("JOIN hub_sessions hs ON hs.id = r.hub_session_id"));
        assert!(run_sql.contains("r.id = $5"));
        assert!(run_sql.contains("r.agent_id = $6"));
        assert!(run_sql.contains("$2::bigint > 0"));
        assert!(run_sql.contains("unnest($3::uuid[], $4::bigint[])"));
        assert!(run_sql.contains("FOR UPDATE OF r, hs SKIP LOCKED"));
        assert!(!run_sql.contains("FOR SHARE"));

        let source = include_str!("api/runtimes.rs");
        let mismatch = source
            .split_once("async fn fail_capability_mismatched_runs_for_runtime_tx(")
            .unwrap()
            .1
            .split_once("\n}\n")
            .unwrap()
            .0;
        assert!(
            mismatch.find("FOR SHARE OF a").unwrap() < mismatch.find("UPDATE runs").unwrap(),
            "capability mismatch handling must lock Agents before updating Runs"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn deleted_super_admin_references_remain_hidden_from_administrator_ledgers(pool: PgPool) {
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let protected_token = create_user_session_with_role(&pool, "super_admin").await;
        let observer_token = create_user_session_with_role(&pool, "super_admin").await;
        let member_token = create_user_session_with_role(&pool, "member").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let protected = require_user(&state, &session_headers(&protected_token))
            .await
            .unwrap();
        let member = require_user(&state, &session_headers(&member_token))
            .await
            .unwrap();
        let connection = create_test_model_connection_for_token(
            &state,
            &admin_token,
            ModelConnectionScope::Global,
            "Deletion Protection Model",
        )
        .await;
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, model_policy)
             VALUES ($1, $2, 'Protected Deleted Agent', '', 'private',
                     '{\"provider\":\"hub-proxy\"}'::jsonb)",
        )
        .bind(agent_id)
        .bind(protected.id)
        .execute(&pool)
        .await
        .unwrap();

        let platform = create_external_platform(
            State(state.clone()),
            session_headers(&admin_token),
            Json(CreateExternalPlatformRequest {
                key: format!("protected-ledger-{}", Uuid::new_v4().simple()),
                name: "Protected Ledger Platform".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let channel = create_authentication_channel(
            State(state.clone()),
            session_headers(&admin_token),
            Path(platform.id),
            Json(CreateAuthenticationChannelRequest {
                key: "trusted".into(),
                name: "Trusted".into(),
                enabled: true,
                trusted_email: true,
            }),
        )
        .await
        .unwrap()
        .0;
        let app = create_integration_app(
            State(state.clone()),
            session_headers(&protected_token),
            Json(CreateIntegrationAppRequest {
                name: "Protected Deleted App".into(),
                external_platform_id: platform.id,
                authentication_channel_id: channel.id,
                redirect_uris: json!(["https://example.test/callback"]),
                agent_ids: vec![agent_id],
                widget_history_enabled: false,
                login_required: true,
                allowed_origins: Vec::new(),
                tool_allowlist: None,
                client_tool_definitions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0
        .integration_app;

        let agent_usage_id = Uuid::new_v4();
        let subject_usage_id = Uuid::new_v4();
        for (id, agent_id, subject, tokens) in [
            (agent_usage_id, Some(agent_id), &member, 10_i64),
            (subject_usage_id, None, &protected, 20_i64),
        ] {
            sqlx::query(
                "INSERT INTO model_token_usage
                     (id, request_id, response_status, model_connection_id,
                      model_connection_scope_snapshot,
                      model_connection_name_snapshot, model_id_snapshot,
                      api_type_snapshot, request_settings_snapshot,
                      agent_id, agent_name_snapshot, subject_type,
                      subject_user_id, subject_display_name_snapshot,
                      input_tokens, output_tokens, total_tokens,
                      cached_tokens, reasoning_tokens)
                 VALUES ($1, $2, 'completed', $3, 'global', $4, $5,
                         'openai_responses', '{\"protocol\":\"openai_responses\"}'::jsonb,
                         $6, CASE WHEN $6::uuid IS NULL THEN NULL
                                  ELSE 'Protected Deleted Agent' END,
                         'user', $7, $8, $9, 0, $9, 0, 0)",
            )
            .bind(id)
            .bind(Uuid::new_v4())
            .bind(connection.id)
            .bind(&connection.name)
            .bind(&connection.allowed_model_ids[0])
            .bind(agent_id)
            .bind(subject.id)
            .bind(&subject.display_name)
            .bind(tokens)
            .execute(&pool)
            .await
            .unwrap();
        }
        let app_error_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO model_call_errors
                 (id, request_id, response_status, upstream_http_status,
                  error_kind, error_code, message, model_connection_id,
                  model_connection_scope_snapshot,
                  model_connection_name_snapshot, model_id_snapshot,
                  api_type_snapshot, request_settings_snapshot,
                  subject_type, subject_user_id, subject_display_name_snapshot,
                  source_integration_app_id,
                  source_integration_app_name_snapshot)
             VALUES ($1, $2, 'failed', 429, 'provider_failed', 'rate_limit',
                     'try later', $3, 'global', $4, $5,
                     'openai_responses', '{\"protocol\":\"openai_responses\"}'::jsonb,
                     'user', $6, $7, $8, $9)",
        )
        .bind(app_error_id)
        .bind(Uuid::new_v4())
        .bind(connection.id)
        .bind(&connection.name)
        .bind(&connection.allowed_model_ids[0])
        .bind(member.id)
        .bind(&member.display_name)
        .bind(app.id)
        .bind(&app.name)
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query("DELETE FROM oauth_apps WHERE id = $1")
            .bind(app.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM agents WHERE id = $1")
            .bind(agent_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(protected.id)
            .execute(&pool)
            .await
            .unwrap();

        let protected_usage_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM model_token_usage
             WHERE id IN ($1, $2) AND super_admin_protected = true",
        )
        .bind(agent_usage_id)
        .bind(subject_usage_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(protected_usage_rows, 2);
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT super_admin_protected FROM model_call_errors WHERE id = $1",
        )
        .bind(app_error_id)
        .fetch_one(&pool)
        .await
        .unwrap());

        let admin_summary = get_model_usage_summary(
            State(state.clone()),
            session_headers(&admin_token),
            Query(ModelTokenUsageQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(admin_summary.overall.total_tokens, 0);
        let admin_errors = list_model_call_errors(
            State(state.clone()),
            session_headers(&admin_token),
            Query(ModelCallErrorQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert!(admin_errors.items.is_empty());

        let observer_summary = get_model_usage_summary(
            State(state.clone()),
            session_headers(&observer_token),
            Query(ModelTokenUsageQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(observer_summary.overall.total_tokens, 30);
        let observer_errors = list_model_call_errors(
            State(state),
            session_headers(&observer_token),
            Query(ModelCallErrorQueryDto::default()),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(observer_errors.items.len(), 1);
        assert_eq!(observer_errors.items[0].id, app_error_id);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn runs_preserve_shared_automation_and_app_model_attribution(pool: PgPool) {
        let owner_token = create_user_session_with_role(&pool, "member").await;
        let caller_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let owner = require_user(&state, &session_headers(&owner_token))
            .await
            .unwrap();
        let caller = require_user(&state, &session_headers(&caller_token))
            .await
            .unwrap();
        let connection = create_test_model_connection_for_token(
            &state,
            &admin_token,
            ModelConnectionScope::Global,
            "Attribution Global",
        )
        .await;
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility, public_to,
                  model_policy, model_connection_id, model_id)
             VALUES ($1, $2, 'Attribution Agent', '', 'public_to', $3,
                     '{\"provider\":\"hub-proxy\"}'::jsonb, $4, $5)",
        )
        .bind(agent_id)
        .bind(owner.id)
        .bind(vec![caller.id])
        .bind(connection.id)
        .bind(&connection.allowed_model_ids[0])
        .execute(&pool)
        .await
        .unwrap();

        let shared = create_run_for_agent(
            &pool,
            agent_id,
            caller.id,
            "shared call".into(),
            "console",
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let shared_attribution: (String, Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            "SELECT model_subject_type, model_subject_user_id,
                    model_source_integration_app_id
             FROM runs WHERE id = $1",
        )
        .bind(shared.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(shared_attribution, ("user".into(), Some(caller.id), None));

        let automation = create_automation(
            State(state.clone()),
            session_headers(&owner_token),
            Json(CreateAutomationRequest {
                agent_id,
                name: "Attribution Automation".into(),
                trigger_type: "manual".into(),
                prompt: "scheduled owner call".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .unwrap()
        .0;
        let automation_run = trigger_automation(
            State(state.clone()),
            session_headers(&owner_token),
            Path(automation.id),
            Json(TriggerAutomationRequest { message: None }),
        )
        .await
        .unwrap()
        .0;
        let automation_attribution: (String, Option<Uuid>) = sqlx::query_as(
            "SELECT model_subject_type, model_subject_user_id
             FROM runs WHERE id = $1",
        )
        .bind(automation_run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(automation_attribution, ("user".into(), Some(owner.id)));

        let platform = create_external_platform(
            State(state.clone()),
            session_headers(&admin_token),
            Json(CreateExternalPlatformRequest {
                key: format!("attribution-{}", Uuid::new_v4().simple()),
                name: "Attribution Platform".into(),
            }),
        )
        .await
        .unwrap()
        .0;
        let channel = create_authentication_channel(
            State(state.clone()),
            session_headers(&admin_token),
            Path(platform.id),
            Json(CreateAuthenticationChannelRequest {
                key: "trusted".into(),
                name: "Trusted".into(),
                enabled: true,
                trusted_email: true,
            }),
        )
        .await
        .unwrap()
        .0;
        let app = create_integration_app(
            State(state.clone()),
            session_headers(&owner_token),
            Json(CreateIntegrationAppRequest {
                name: "Attribution App".into(),
                external_platform_id: platform.id,
                authentication_channel_id: channel.id,
                redirect_uris: json!(["https://example.test/callback"]),
                agent_ids: vec![agent_id],
                widget_history_enabled: false,
                login_required: true,
                allowed_origins: Vec::new(),
                tool_allowlist: None,
                client_tool_definitions: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0
        .integration_app;
        let principal = IntegrationPrincipal {
            oauth_app_id: app.id,
            grant_type: "client_credentials".into(),
            subject_user_id: None,
            agent_id,
            agent_owner_id: owner.id,
            external_platform_id: platform.id,
            authentication_channel_id: channel.id,
            origin_tenant_id: None,
            origin_external_identity_id: None,
        };
        let attribution = integration_run_model_attribution(&principal);
        let app_run = {
            let mut tx = pool.begin().await.unwrap();
            let session_id = insert_hub_native_session_tx(&mut tx, caller.id, agent_id)
                .await
                .unwrap();
            let accepted = accept_session_message_tx(
                &mut tx,
                AcceptSessionMessage {
                    session_id,
                    agent_id,
                    owner_id: caller.id,
                    content: "app-only call".into(),
                    payload: json!({}),
                    role: "user".into(),
                    message_kind: "message".into(),
                    requested_delivery_mode: "next_turn".into(),
                    client_message_key: None,
                    source: "integration:message".into(),
                    automation_id: None,
                    integration_session_id: None,
                    parent_run_id: None,
                    continuation_turn_id: None,
                    model_subject_type: attribution.subject_type.into(),
                    model_subject_user_id: attribution.subject_user_id,
                    model_source_integration_app_id: attribution.source_integration_app_id,
                    external_user_context: None,
                    attachment_ids: Vec::new(),
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            accepted.run.unwrap()
        };
        let app_attribution: (String, Option<Uuid>, Option<Uuid>) = sqlx::query_as(
            "SELECT model_subject_type, model_subject_user_id,
                    model_source_integration_app_id
             FROM runs WHERE id = $1",
        )
        .bind(app_run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            app_attribution,
            ("integration_app".into(), None, Some(app.id))
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn member_can_create_edit_and_trigger_automation_for_public_agent(pool: PgPool) {
        let owner_token = create_user_session_with_role(&pool, "member").await;
        let caller_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let owner = require_user(&state, &session_headers(&owner_token))
            .await
            .unwrap();
        let caller = require_user(&state, &session_headers(&caller_token))
            .await
            .unwrap();
        let connection = create_test_model_connection_for_token(
            &state,
            &admin_token,
            ModelConnectionScope::Global,
            "Public Agent Automation",
        )
        .await;
        let agent_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO agents
                 (id, owner_id, name, instructions, visibility,
                  model_policy, model_connection_id, model_id)
             VALUES ($1, $2, 'Public Automation Agent', '', 'public',
                     '{\"provider\":\"hub-proxy\"}'::jsonb, $3, $4)",
        )
        .bind(agent_id)
        .bind(owner.id)
        .bind(connection.id)
        .bind(&connection.allowed_model_ids[0])
        .execute(&pool)
        .await
        .unwrap();

        // 非 owner 的 caller 可以创建、编辑并手动触发公共 Agent 的 automation。
        let automation = create_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Json(CreateAutomationRequest {
                agent_id,
                name: "Shared Manual".into(),
                trigger_type: "manual".into(),
                prompt: "shared manual run".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(automation.agent_id, agent_id);
        assert_eq!(automation.owner_id, caller.id);

        let updated = update_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Path(automation.id),
            Json(UpdateAutomationRequest {
                name: "Shared Manual Renamed".into(),
                trigger_type: "manual".into(),
                prompt: "shared manual run".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(updated.name, "Shared Manual Renamed");

        let manual_run = trigger_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Path(automation.id),
            Json(TriggerAutomationRequest { message: None }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(manual_run.automation_id, Some(automation.id));
        assert_eq!(manual_run.agent_id, agent_id);

        // webhook automation 同样可以创建并触发。
        let webhook = create_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Json(CreateAutomationRequest {
                agent_id,
                name: "Shared Webhook".into(),
                trigger_type: "webhook".into(),
                prompt: "shared webhook run".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .unwrap()
        .0;
        let token = webhook
            .webhook_token
            .expect("webhook token is returned once");
        let mut webhook_headers = HeaderMap::new();
        webhook_headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Webhook {token}")).unwrap(),
        );
        let webhook_run = trigger_automation_webhook(
            State(state.clone()),
            webhook_headers,
            Json(TriggerAutomationRequest { message: None }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(webhook_run.automation_id, Some(webhook.id));

        // 撤销 public 后：新创建、编辑与触发全部被拒；owner 本人不受影响。
        sqlx::query("UPDATE agents SET visibility = 'private' WHERE id = $1")
            .bind(agent_id)
            .execute(&pool)
            .await
            .unwrap();
        let denied = create_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Json(CreateAutomationRequest {
                agent_id,
                name: "Denied".into(),
                trigger_type: "manual".into(),
                prompt: "denied".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .unwrap_err();
        assert!(denied
            .message
            .contains("automation requires an accessible agent"));
        assert!(update_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Path(automation.id),
            Json(UpdateAutomationRequest {
                name: "Denied Rename".into(),
                trigger_type: "manual".into(),
                prompt: "shared manual run".into(),
                schedule: None,
                enabled: true,
            }),
        )
        .await
        .is_err());
        assert!(trigger_automation(
            State(state.clone()),
            session_headers(&caller_token),
            Path(automation.id),
            Json(TriggerAutomationRequest { message: None }),
        )
        .await
        .is_err());
    }

    pub(crate) fn runtime_write<T>(payload: T) -> Json<RuntimeSessionWriteRequest<T>> {
        runtime_write_generation(1, payload)
    }

    pub(crate) fn runtime_write_generation<T>(
        ownership_generation: i64,
        payload: T,
    ) -> Json<RuntimeSessionWriteRequest<T>> {
        Json(RuntimeSessionWriteRequest {
            ownership_generation,
            payload,
        })
    }
}
