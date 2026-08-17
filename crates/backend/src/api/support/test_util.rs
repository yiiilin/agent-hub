//! 测试共享 helper：从 main.rs 的 mod tests 迁移而来，
//! 供各域测试复用（cfg(test) 下经 api::support re-export 回 main.rs）。

use super::*;
use std::{collections::HashMap, sync::Arc, time::Duration};

use agent_hub_backend::ModelSecretCipher;
use agent_hub_shared::*;
use axum::{
    body::{Body, Bytes},
    extract::{Multipart, Path, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tower::ServiceExt;
use uuid::Uuid;

use crate::api::*;
use crate::build_router;
use crate::run_event_bus;
use crate::tests::{issue_widget_external_access_for, runtime_write};
use crate::DEFAULT_SESSION_BUNDLE_MAX_BYTES;

pub(crate) fn staged_skill_package_upload(
    name: &str,
    content: &str,
    archive_contents: &str,
) -> StagedSkillPackageUpload {
    let staging = tempfile::tempdir().unwrap();
    let archive_path = staging.path().join("package.tar.zst");
    std::fs::write(&archive_path, archive_contents).unwrap();
    StagedSkillPackageUpload {
        _staging: staging,
        name: name.into(),
        description: format!("{name} description"),
        content: content.into(),
        archive_path: Some(archive_path),
        archive_size_bytes: Some(archive_contents.len() as u64),
        archive_checksum_sha256: Some(sha256_hex(archive_contents)),
        files: vec![SkillPackageFileDto {
            path: "bin/client".into(),
            size_bytes: 6,
            checksum_sha256: sha256_hex("client"),
            executable: true,
        }],
    }
}

pub(crate) async fn attach_test_model_connection(
    pool: &PgPool,
    agent_id: Uuid,
    owner_id: Uuid,
    model_id: &str,
) {
    let model_connection_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, 'global', $2, 'https://models.example.test',
                 'openai_responses', $3, $4, $5, $6)",
    )
    .bind(model_connection_id)
    .bind(format!("test-model-{}", Uuid::new_v4().simple()))
    .bind(vec![model_id.to_owned()])
    .bind(vec![1_u8; 17])
    .bind(vec![2_u8; 12])
    .bind(owner_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE agents
         SET model_connection_id = $1, model_id = $2,
             model_settings = $3
         WHERE id = $4",
    )
    .bind(model_connection_id)
    .bind(model_id)
    .bind(json!({
        "reasoning_effort": "default",
        "reasoning_summary": "default",
        "verbosity": "default",
        "context_window_tokens": null,
        "auto_compact_token_limit": null,
        "reasoning_summary_support": "auto",
        "service_tier": null,
        "provider_request_timeout_ms": null,
        "stream_max_retries": null,
        "stream_idle_timeout_ms": null,
        "request_settings": { "protocol": "openai_responses" }
    }))
    .bind(agent_id)
    .execute(pool)
    .await
    .unwrap();
}

pub(crate) async fn accept_test_session_message(
    pool: &PgPool,
    session_id: Uuid,
    agent_id: Uuid,
    owner_id: Uuid,
    content: &str,
    client_message_key: Option<&str>,
    requested_delivery_mode: &str,
) -> Result<SessionMessageAcceptanceDto, ApiError> {
    let mut tx = pool.begin().await?;
    ensure_agent_can_start_run_tx(&mut tx, agent_id, owner_id).await?;
    let accepted = accept_session_message_tx(
        &mut tx,
        AcceptSessionMessage {
            session_id,
            agent_id,
            owner_id,
            content: content.to_owned(),
            payload: json!({}),
            role: "user".into(),
            message_kind: "message".into(),
            requested_delivery_mode: requested_delivery_mode.into(),
            client_message_key: client_message_key.map(str::to_owned),
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
    .await?;
    tx.commit().await?;
    Ok(accepted)
}

pub(crate) struct WidgetExternalTestFixture {
    pub(crate) state: Arc<AppState>,
    pub(crate) router: Router,
    pub(crate) app_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) platform_id: Uuid,
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) client_instance_id: Uuid,
}

pub(crate) async fn widget_external_test_fixture(
    pool: PgPool,
    history_enabled: bool,
) -> WidgetExternalTestFixture {
    let owner = create_hub_user(
        &pool,
        Some(&format!(
            "widget-fixture-{}@example.com",
            Uuid::new_v4().simple()
        )),
        None,
        Some("password-hash"),
        true,
    )
    .await
    .unwrap();
    let model_connection_id = Uuid::new_v4();
    let model_id = format!("widget-model-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, 'global', $6, 'https://models.example.test',
                 'openai_responses', $2, $3, $4, $5)",
    )
    .bind(model_connection_id)
    .bind(vec![model_id.clone()])
    .bind(vec![1_u8; 17])
    .bind(vec![2_u8; 12])
    .bind(owner.id)
    .bind(format!("Widget Test Model {}", Uuid::new_v4().simple()))
    .execute(&pool)
    .await
    .unwrap();
    let agent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              model_connection_id, model_id)
         VALUES ($1, $2, 'Widget External Agent', 'test', 'private', $3, $4)",
    )
    .bind(agent_id)
    .bind(owner.id)
    .bind(model_connection_id)
    .bind(&model_id)
    .execute(&pool)
    .await
    .unwrap();
    let platform_id = Uuid::new_v4();
    let channel_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO external_platforms (id, key, name)
         VALUES ($1, $2, 'Widget Fixture Platform')",
    )
    .bind(platform_id)
    .bind(format!("widget-fixture-{}", Uuid::new_v4().simple()))
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
    let app_id = Uuid::new_v4();
    let client_id = format!("widget-fixture-{}", Uuid::new_v4().simple());
    let client_secret = format!("widget-secret-{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO oauth_apps
             (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
              external_platform_id, authentication_channel_id, widget_history_enabled)
         VALUES ($1, $2, 'Widget Fixture App', $3, $4, '[]'::jsonb, $5, $6, $7)",
    )
    .bind(app_id)
    .bind(owner.id)
    .bind(&client_id)
    .bind(sha256_hex(&client_secret))
    .bind(platform_id)
    .bind(channel_id)
    .bind(history_enabled)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO integration_app_agents (app_id, agent_id) VALUES ($1, $2)")
        .bind(app_id)
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
    let state = Arc::new(test_state_with_pool(pool));
    let router = build_router((*state).clone());
    WidgetExternalTestFixture {
        state,
        router,
        app_id,
        agent_id,
        platform_id,
        client_id,
        client_secret,
        client_instance_id: Uuid::new_v4(),
    }
}

pub(crate) async fn issue_widget_external_access(
    fixture: &WidgetExternalTestFixture,
    tenant_id: &str,
    external_user_id: &str,
    display_name: &str,
) -> WidgetAccessResponse {
    issue_widget_external_access_for(
        fixture,
        &fixture.client_id,
        &fixture.client_secret,
        fixture.agent_id,
        tenant_id,
        external_user_id,
        display_name,
    )
    .await
}

pub(crate) async fn issue_client_access_for_instance(
    fixture: &WidgetExternalTestFixture,
    client_instance_id: Uuid,
    tenant_id: &str,
    external_user_id: &str,
    client_tools: Value,
) -> ClientAccessResponse {
    let basic = STANDARD.encode(format!("{}:{}", fixture.client_id, fixture.client_secret));
    let response = fixture
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
                        "client_instance_id": client_instance_id,
                        "tenant_id": tenant_id,
                        "external_user_id": external_user_id,
                        "username": format!("{external_user_id}-name"),
                        "display_name": "Canonical Client User",
                        "email": format!("{external_user_id}@example.com"),
                        "attributes": { "source": "canonical-client-test" },
                        "client_tools": client_tools
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

pub(crate) async fn issue_public_widget_access(
    fixture: &WidgetExternalTestFixture,
    visitor_key: &str,
) -> PublicWidgetAccessResponse {
    let response = fixture
        .router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/widget/public/access")
                .header(header::ORIGIN, "https://docs.example.test")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "client_id": fixture.client_id,
                        "visitor_key": visitor_key,
                        "client_instance_id": fixture.client_instance_id
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

pub(crate) async fn anonymous_client_access_response(
    fixture: &WidgetExternalTestFixture,
    visitor_key: &str,
    client_instance_id: Uuid,
    session_id: Option<Uuid>,
    origin: Option<&str>,
) -> Response {
    let mut request = axum::http::Request::builder()
        .method(Method::POST)
        .uri("/api/client/anonymous/access")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(origin) = origin {
        request = request.header(header::ORIGIN, origin);
    }
    fixture
        .router
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "client_id": fixture.client_id,
                        "visitor_key": visitor_key,
                        "client_instance_id": client_instance_id,
                        "session_id": session_id
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

pub(crate) async fn create_widget_external_run(
    fixture: &WidgetExternalTestFixture,
    token: &str,
    message: &str,
) -> RunDto {
    let response = fixture
        .router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/widget/runs")
                .header("x-agent-hub-embed-token", token)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "message": message,
                        "client_message_key": format!("test-{}", Uuid::new_v4())
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

pub(crate) async fn create_canonical_client_run(
    fixture: &WidgetExternalTestFixture,
    token: &str,
    session_id: Option<Uuid>,
    message: &str,
) -> RunDto {
    let response = fixture
        .router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(Method::POST)
                .uri("/api/client/runs")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "message": message,
                        "session_id": session_id,
                        "client_message_key": format!("client-tool-{}", Uuid::new_v4())
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

pub(crate) fn test_client_tool_definitions(names: &[&str]) -> Value {
    Value::Array(
        names
            .iter()
            .map(|name| {
                json!({
                    "name": name,
                    "description": format!("Execute {name}"),
                    "input_schema": { "type": "object" }
                })
            })
            .collect(),
    )
}

pub(crate) struct ClientToolRunTestFixture {
    pub(crate) app: WidgetExternalTestFixture,
    pub(crate) executor: ClientAccessResponse,
    pub(crate) observer: ClientAccessResponse,
    pub(crate) run: RunDto,
    pub(crate) runtime_token: String,
    pub(crate) tool_call_ids: Vec<Uuid>,
}

pub(crate) async fn bind_client_tool_test_run_to_runtime(
    app: &WidgetExternalTestFixture,
    run: &RunDto,
) -> String {
    let runtime_id = Uuid::new_v4();
    let runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO runtimes
             (id, token_hash, hostname, labels, engine_version, capabilities,
              sandbox_mode, status)
         VALUES ($1, $2, $3, '{}', 'test', '{\"model_proxy\":true}'::jsonb,
                 'workspace-write', 'online')",
    )
    .bind(runtime_id)
    .bind(sha256_hex(&runtime_token))
    .bind(format!("client-tool-runtime-{}", Uuid::new_v4().simple()))
    .execute(&app.state.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE agents SET runtime_id = $1 WHERE id = $2")
        .bind(runtime_id)
        .bind(app.agent_id)
        .execute(&app.state.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE hub_sessions
         SET runtime_owner_id = $1, ownership_generation = 1,
             lifecycle_status = 'online', active_turn_id = $2
         WHERE id = $3",
    )
    .bind(runtime_id)
    .bind(run.hub_turn_id.unwrap())
    .bind(run.hub_session_id.unwrap())
    .execute(&app.state.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE hub_session_turns
         SET status = 'running', ownership_generation = 1
         WHERE id = $1 AND session_id = $2",
    )
    .bind(run.hub_turn_id.unwrap())
    .bind(run.hub_session_id.unwrap())
    .execute(&app.state.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE runs
         SET runtime_id = $1, status = 'running', session_ownership_generation = 1
         WHERE id = $2",
    )
    .bind(runtime_id)
    .bind(run.id)
    .execute(&app.state.pool)
    .await
    .unwrap();
    runtime_token
}

pub(crate) async fn prepare_client_tool_run(
    pool: PgPool,
    tool_names: &[&str],
) -> ClientToolRunTestFixture {
    let app = widget_external_test_fixture(pool, true).await;
    let definitions = test_client_tool_definitions(tool_names);
    let executor = issue_client_access_for_instance(
        &app,
        Uuid::new_v4(),
        "client-tool-tenant",
        "client-tool-user",
        definitions.clone(),
    )
    .await;
    let observer = issue_client_access_for_instance(
        &app,
        Uuid::new_v4(),
        "client-tool-tenant",
        "client-tool-user",
        definitions,
    )
    .await;
    let run =
        create_canonical_client_run(&app, &executor.access_token, None, "execute Client Tools")
            .await;
    let runtime_token = bind_client_tool_test_run_to_runtime(&app, &run).await;
    ClientToolRunTestFixture {
        app,
        executor,
        observer,
        run,
        runtime_token,
        tool_call_ids: tool_names.iter().map(|_| Uuid::new_v4()).collect(),
    }
}

pub(crate) async fn finalize_test_client_tool_batch(
    fixture: &ClientToolRunTestFixture,
    tool_names: &[&str],
) -> Result<Json<RunDto>, ApiError> {
    assert_eq!(fixture.tool_call_ids.len(), tool_names.len());
    runtime_finalize_tool_requests(
        State(fixture.app.state.clone()),
        bearer_headers(&fixture.runtime_token),
        Path(fixture.run.id),
        runtime_write(FinalizeToolRequestsRequest {
            integration_session_id: fixture.run.integration_session_id,
            native_session_id: "client-tool-native-session".into(),
            work_dir_ref: "client-tool-workdir".into(),
            tool_requests: tool_names
                .iter()
                .enumerate()
                .map(|(position, tool_name)| FinalizeToolRequestEvent {
                    role: Some("assistant".into()),
                    content: Some(format!("{tool_name} requested")),
                    payload: json!({
                        "tool_request_id": fixture.tool_call_ids[position],
                        "tool_name": tool_name,
                        "arguments": { "position": position }
                    }),
                })
                .collect(),
        }),
    )
    .await
}

pub(crate) async fn api_key_http_request(
    app: &Router,
    method: Method,
    path: &str,
    token: &str,
    body: Value,
) -> Response {
    app.clone()
        .oneshot(
            axum::http::Request::builder()
                .method(method)
                .uri(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

pub(crate) async fn assert_api_key_http_error(response: Response, expected: StatusCode) {
    assert_eq!(response.status(), expected);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({ "error": "api key not found" })
    );
}

pub(crate) async fn api_key_record_count(pool: &PgPool, api_key_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM api_keys WHERE id = $1")
        .bind(api_key_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) fn test_agent() -> AgentDto {
    AgentDto {
        id: Uuid::new_v4(),
        owner_id: Uuid::new_v4(),
        owner_email: None,
        name: "Test Agent".into(),
        instructions: "Test instructions".into(),
        visibility: "private".into(),
        public_to: Vec::new(),
        endpoint_exposure: vec!["console".into(), "integration".into(), "automation".into()],
        runtime_id: None,
        model_selection: None,
        model_settings: AgentModelSettings::default(),
        subagents: Vec::new(),
        model_policy: json!({}),
        sandbox_policy: json!({}),
        managed_skill_ids: Vec::new(),
        secret_declarations: Vec::new(),
        mcp_allowlist: json!([]),
        tool_allowlist: default_agent_tool_allowlist(),
        is_owner: false,
        can_manage: false,
        can_administer: false,
        can_invoke: false,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

pub(crate) fn test_user(id: Uuid, role: &str) -> UserDto {
    UserDto {
        id,
        email: format!("{id}@example.com"),
        display_name: "Test User".into(),
        role: role.into(),
    }
}

pub(crate) fn test_update_agent_request() -> UpdateAgentRequest {
    UpdateAgentRequest {
        name: "Test Agent".into(),
        instructions: "Test instructions".into(),
        visibility: "private".into(),
        public_to: Vec::new(),
        endpoint_exposure: vec!["console".into(), "integration".into(), "automation".into()],
        runtime_id: None,
        model_selection: None,
        model_settings: AgentModelSettings::default(),
        subagents: Vec::new(),
        model_policy: json!({ "provider": "hub-proxy" }),
        sandbox_policy: json!({ "mode": "workspace-write", "network_access": true }),
        managed_skill_ids: Vec::new(),
        secret_declarations: Some(Vec::new()),
        mcp_allowlist: json!([]),
        tool_allowlist: default_agent_tool_allowlist(),
    }
}

pub(crate) fn test_automation(
    trigger_type: &str,
    schedule: Option<&str>,
    created_at: DateTime<Utc>,
) -> AutomationDto {
    AutomationDto {
        id: Uuid::new_v4(),
        agent_id: Uuid::new_v4(),
        owner_id: Uuid::new_v4(),
        name: "Test automation".into(),
        trigger_type: trigger_type.into(),
        prompt: "Run the test automation".into(),
        schedule: schedule.map(str::to_owned),
        webhook_token: None,
        enabled: true,
        last_triggered_at: None,
        created_at,
    }
}

pub(crate) async fn slow_sse_model_upstream() -> axum::response::Response {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: first\n\n"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: second\n\n"));
    };
    let mut response = axum::response::Response::new(axum::body::Body::from_stream(stream));
    *response.status_mut() = StatusCode::ACCEPTED;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        header::CONNECTION,
        HeaderValue::from_static("keep-alive, x-upstream-hop"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-upstream-hop"),
        HeaderValue::from_static("must-not-be-forwarded"),
    );
    response
}

pub(crate) async fn model_upstream_never_sends_headers() -> axum::response::Response {
    std::future::pending().await
}

pub(crate) async fn model_upstream_stalls_after_first_chunk() -> axum::response::Response {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: first\n\n"));
        std::future::pending::<()>().await;
    };
    let mut response = axum::response::Response::new(axum::body::Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}

pub(crate) async fn model_upstream_rate_limited() -> axum::response::Response {
    let mut response = axum::response::Response::new(axum::body::Body::from("provider rate limit"));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

pub(crate) struct RuntimeClaimFixture {
    pub(crate) state: Arc<AppState>,
    pub(crate) agent_id: Uuid,
    pub(crate) model_connection_id: Uuid,
    pub(crate) runtime_id: Uuid,
    pub(crate) runtime_token: String,
    pub(crate) hub_session_id: Uuid,
    pub(crate) turn_id: Uuid,
    pub(crate) run_id: Uuid,
}

pub(crate) struct SchedulerArchiveFixture {
    pub(crate) pool: PgPool,
    pub(crate) owner_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) automation_id: Uuid,
    pub(crate) session_token: String,
}

pub(crate) struct AutomationUpdateFixture {
    pub(crate) pool: PgPool,
    pub(crate) agent_id: Uuid,
    pub(crate) automation_id: Uuid,
    pub(crate) owner_session: String,
    pub(crate) foreign_session: String,
}

pub(crate) struct SchedulerArchivePostconditions {
    pub(crate) archived: bool,
    pub(crate) enabled_automations: i64,
    pub(crate) scheduler_runs: i64,
    pub(crate) active_runs: i64,
    pub(crate) post_archive_runs: i64,
    pub(crate) last_triggered_at: Option<DateTime<Utc>>,
}

pub(crate) async fn postgres_test_pool_with_application_name(
    pool: &PgPool,
    application_name: &str,
) -> PgPool {
    let connect_options = pool
        .connect_options()
        .as_ref()
        .clone()
        .application_name(application_name);
    PgPoolOptions::new()
        .max_connections(3)
        .connect_with(connect_options)
        .await
        .unwrap()
}

pub(crate) async fn runtime_claim_fixture(
    pool: PgPool,
    runtime_sandbox_mode: &str,
    agent_sandbox_mode: &str,
) -> RuntimeClaimFixture {
    let owner_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let model_connection_id = Uuid::new_v4();
    let runtime_id = Uuid::new_v4();
    let hub_session_id = Uuid::new_v4();
    let turn_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    let runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();

    sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role)
         VALUES ($1, $2, 'unused', 'Runtime Claim Test Owner', 'member')",
    )
    .bind(owner_id)
    .bind(format!("runtime-claim-{unique}@example.com"))
    .execute(&pool)
    .await
    .unwrap();
    let cipher = ModelSecretCipher::from_env_value(Some(
        &base64::engine::general_purpose::STANDARD.encode([42_u8; 32]),
    ))
    .unwrap();
    let encrypted = cipher.encrypt("runtime-claim-secret").unwrap();
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, owner_id, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, 'personal', $2, 'Runtime Claim Model',
                 'http://127.0.0.1:1', 'openai_responses',
                 ARRAY['runtime-claim-model'], $3, $4, $2)",
    )
    .bind(model_connection_id)
    .bind(owner_id)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runtimes
         (id, token_hash, hostname, labels, engine_version, capabilities, sandbox_mode, status)
         VALUES ($1, $2, $3, '{}', 'test',
                 '{\"model_proxy\":true,\"subagents\":true}'::jsonb, $4,
                 'online')",
    )
    .bind(runtime_id)
    .bind(sha256_hex(&runtime_token))
    .bind(format!("runtime-claim-{unique}"))
    .bind(runtime_sandbox_mode)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agents
         (id, owner_id, name, instructions, visibility, model_policy, sandbox_policy,
          runtime_id, model_connection_id, model_id)
         VALUES ($1, $2, 'Runtime Claim Test Agent', 'test', 'private',
                 '{\"provider\":\"hub-proxy\"}'::jsonb,
                 $3, $4, $5, 'runtime-claim-model')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(json!({ "mode": agent_sandbox_mode, "network_access": false }))
    .bind(runtime_id)
    .bind(model_connection_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, lifecycle_status)
         VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
    )
    .bind(hub_session_id)
    .bind(owner_id)
    .bind(agent_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_session_turns
             (id, session_id, status, ownership_generation)
         VALUES ($1, $2, 'pending', 0)",
    )
    .bind(turn_id)
    .bind(hub_session_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runs
             (id, agent_id, owner_id, status, initial_message, source,
              hub_session_id, hub_turn_id, session_ownership_generation)
         VALUES ($1, $2, $3, 'pending', 'claim test', 'console', $4, $5, 0)",
    )
    .bind(run_id)
    .bind(agent_id)
    .bind(owner_id)
    .bind(hub_session_id)
    .bind(turn_id)
    .execute(&pool)
    .await
    .unwrap();
    for content in ["claim message one", "claim message two"] {
        sqlx::query(
            "INSERT INTO hub_session_messages
                 (id, session_id, role, message_kind, content, delivery_mode,
                  delivery_state, turn_id, run_id)
             VALUES ($1, $2, 'user', 'message', $3, 'next_turn', 'queued', $4, $5)",
        )
        .bind(Uuid::new_v4())
        .bind(hub_session_id)
        .bind(content)
        .bind(turn_id)
        .bind(run_id)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO hub_session_messages
             (id, session_id, role, message_kind, content, delivery_mode, delivery_state)
         VALUES ($1, $2, 'user', 'message', 'later message', 'later_turn', 'deferred')",
    )
    .bind(Uuid::new_v4())
    .bind(hub_session_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE hub_sessions SET history_checkpoint = 3 WHERE id = $1")
        .bind(hub_session_id)
        .execute(&pool)
        .await
        .unwrap();

    RuntimeClaimFixture {
        state: Arc::new(test_state_with_browser_session_auth(pool)),
        agent_id,
        model_connection_id,
        runtime_id,
        runtime_token,
        hub_session_id,
        turn_id,
        run_id,
    }
}

pub(crate) async fn scheduler_archive_fixture(pool: PgPool) -> SchedulerArchiveFixture {
    let owner_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let automation_id = Uuid::new_v4();
    let session_token = format!("ahs_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();

    sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role)
         VALUES ($1, $2, 'unused', 'Scheduler Archive Test Owner', 'member')",
    )
    .bind(owner_id)
    .bind(format!("scheduler-archive-{unique}@example.com"))
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
        "INSERT INTO agents (id, owner_id, name, instructions, visibility)
         VALUES ($1, $2, 'Scheduler Archive Test Agent', 'test', 'private')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO automations
         (id, agent_id, owner_id, name, trigger_type, prompt, schedule, enabled, created_at)
         VALUES ($1, $2, $3, 'Archive-first due scheduler', 'interval',
                 'scheduler must not create this run', '1s', false,
                 now() - interval '1 hour')",
    )
    .bind(automation_id)
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();

    SchedulerArchiveFixture {
        pool,
        owner_id,
        agent_id,
        automation_id,
        session_token,
    }
}

pub(crate) async fn automation_update_fixture(pool: PgPool) -> AutomationUpdateFixture {
    let owner_id = Uuid::new_v4();
    let foreign_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let automation_id = Uuid::new_v4();
    let owner_session = format!("ahs_{}", Uuid::new_v4().simple());
    let foreign_session = format!("ahs_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();
    for (id, label) in [(owner_id, "owner"), (foreign_id, "foreign")] {
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', $3, 'member')",
        )
        .bind(id)
        .bind(format!("automation-update-{label}-{unique}@example.com"))
        .bind(label)
        .execute(&pool)
        .await
        .unwrap();
    }
    for (token, user_id) in [(&owner_session, owner_id), (&foreign_session, foreign_id)] {
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
    sqlx::query(
        "INSERT INTO agents (id, owner_id, name, instructions, visibility)
         VALUES ($1, $2, 'Automation Update Agent', 'test', 'private')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO automations
         (id, agent_id, owner_id, name, trigger_type, prompt, enabled)
         VALUES ($1, $2, $3, 'Original', 'manual', 'Original prompt', true)",
    )
    .bind(automation_id)
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();
    AutomationUpdateFixture {
        pool,
        agent_id,
        automation_id,
        owner_session,
        foreign_session,
    }
}

pub(crate) struct IntegrationRuntimeFixture {
    pub(crate) state: Arc<AppState>,
    pub(crate) agent_id: Uuid,
    pub(crate) platform_name: String,
    pub(crate) runtime_id: Uuid,
    pub(crate) runtime_token: String,
    pub(crate) other_runtime_id: Uuid,
    pub(crate) integration_token: String,
    pub(crate) second_integration_token: String,
    pub(crate) foreign_integration_token: String,
    pub(crate) session_id: Uuid,
    pub(crate) hub_session_id: Uuid,
    pub(crate) turn_id: Uuid,
    pub(crate) run_id: Uuid,
    pub(crate) tool_request_id: Uuid,
}

pub(crate) async fn integration_runtime_fixture(pool: PgPool) -> IntegrationRuntimeFixture {
    let owner_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let model_connection_id = Uuid::new_v4();
    let model_id = format!("integration-model-{}", Uuid::new_v4().simple());
    let runtime_id = Uuid::new_v4();
    let other_runtime_id = Uuid::new_v4();
    let oauth_app_id = Uuid::new_v4();
    let foreign_agent_id = Uuid::new_v4();
    let foreign_oauth_app_id = Uuid::new_v4();
    let platform_id = Uuid::new_v4();
    let channel_id = Uuid::new_v4();
    let foreign_platform_id = Uuid::new_v4();
    let foreign_channel_id = Uuid::new_v4();
    let external_owner_id = Uuid::new_v4();
    let external_identity_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let hub_session_id = Uuid::new_v4();
    let turn_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    let tool_request_id = Uuid::new_v4();
    let runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
    let other_runtime_token = format!("ahrt_{}", Uuid::new_v4().simple());
    let integration_token = format!("aho_{}", Uuid::new_v4().simple());
    let second_integration_token = format!("aho_{}", Uuid::new_v4().simple());
    let foreign_integration_token = format!("aho_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();
    let platform_name = format!("integration-{unique}");

    sqlx::query(
        "INSERT INTO users
             (id, email, password, display_name, role)
         VALUES ($1, $2, 'unused', 'Integration Test Owner', 'member')",
    )
    .bind(owner_id)
    .bind(format!("integration-{unique}@example.com"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, 'global', $6, 'https://models.example.test',
                 'openai_responses', $2, $3, $4, $5)",
    )
    .bind(model_connection_id)
    .bind(vec![model_id.clone()])
    .bind(vec![1_u8; 17])
    .bind(vec![2_u8; 12])
    .bind(owner_id)
    .bind(format!("Integration Runtime Model {unique}"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users
             (id, email, password, display_name, role)
         VALUES ($1, $2, NULL, 'External Integration User', 'member')",
    )
    .bind(external_owner_id)
    .bind(format!("external-integration-{unique}@example.com"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runtimes
         (id, token_hash, hostname, labels, engine_version, capabilities, sandbox_mode, status)
         VALUES ($1, $2, $3, '{}', 'test', '{\"model_proxy\":true}'::jsonb,
                 'workspace-write', 'online'),
                ($4, $5, $6, '{}', 'test', '{\"model_proxy\":true}'::jsonb,
                 'workspace-write', 'online')",
    )
    .bind(runtime_id)
    .bind(sha256_hex(&runtime_token))
    .bind(format!("runtime-{unique}"))
    .bind(other_runtime_id)
    .bind(sha256_hex(&other_runtime_token))
    .bind(format!("runtime-other-{unique}"))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agents
         (id, owner_id, name, instructions, visibility, model_policy)
         VALUES ($1, $2, 'Foreign Integration Test Agent', 'test', 'private',
                 '{\"provider\":\"hub-proxy\"}'::jsonb)",
    )
    .bind(foreign_agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agents
         (id, owner_id, name, instructions, visibility, model_policy, runtime_id,
          model_connection_id, model_id)
         VALUES ($1, $2, 'Integration Test Agent', 'test', 'private',
                 '{\"provider\":\"hub-proxy\"}'::jsonb, $3, $4, $5)",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(runtime_id)
    .bind(model_connection_id)
    .bind(&model_id)
    .execute(&pool)
    .await
    .unwrap();
    for (platform, channel, key) in [
        (platform_id, channel_id, platform_name.clone()),
        (
            foreign_platform_id,
            foreign_channel_id,
            format!("foreign-integration-{unique}"),
        ),
    ] {
        sqlx::query(
            "INSERT INTO external_platforms (id, key, name)
             VALUES ($1, $2, $2)",
        )
        .bind(platform)
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO authentication_channels
                 (id, platform_id, key, name, enabled, trusted_email, created_by)
             VALUES ($1, $2, 'oauth-app', 'OAuth App', true, true, $3)",
        )
        .bind(channel)
        .bind(platform)
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO external_identities
             (id, platform_id, tenant_id, external_user_id, user_id,
              authentication_channel_id)
         VALUES ($1, $2, 'fixture-tenant', 'external-test-user', $3, $4)",
    )
    .bind(external_identity_id)
    .bind(platform_id)
    .bind(external_owner_id)
    .bind(channel_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_apps
         (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
          external_platform_id, authentication_channel_id)
         VALUES ($1, $2, 'Foreign Integration Test App', $3, 'unused',
                 '[]'::jsonb, $4, $5)",
    )
    .bind(foreign_oauth_app_id)
    .bind(owner_id)
    .bind(format!("foreign-client-{unique}"))
    .bind(foreign_platform_id)
    .bind(foreign_channel_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO oauth_apps
         (id, owner_id, name, client_id, client_secret_hash, redirect_uris,
          external_platform_id, authentication_channel_id)
         VALUES ($1, $2, 'Integration Test App', $3, 'unused',
                 '[]'::jsonb, $4, $5)",
    )
    .bind(oauth_app_id)
    .bind(owner_id)
    .bind(format!("client-{unique}"))
    .bind(platform_id)
    .bind(channel_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO integration_app_agents (app_id, agent_id)
         VALUES ($1, $2), ($3, $4)",
    )
    .bind(oauth_app_id)
    .bind(agent_id)
    .bind(foreign_oauth_app_id)
    .bind(foreign_agent_id)
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
    .bind(foreign_oauth_app_id)
    .bind(sha256_hex(&foreign_integration_token))
    .bind(vec![format!("agent:{foreign_agent_id}")])
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
    .bind(oauth_app_id)
    .bind(sha256_hex(&integration_token))
    .bind(vec![format!("agent:{agent_id}")])
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
    .bind(oauth_app_id)
    .bind(sha256_hex(&second_integration_token))
    .bind(vec![format!("agent:{agent_id}")])
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, origin_platform_id,
              origin_tenant_id, origin_external_identity_id, lifecycle_status,
              runtime_owner_id, ownership_generation)
         VALUES ($1, $2, $3, 'external', $4, 'fixture-tenant', $5, 'online', $6, 1)",
    )
    .bind(hub_session_id)
    .bind(external_owner_id)
    .bind(agent_id)
    .bind(platform_id)
    .bind(external_identity_id)
    .bind(runtime_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_session_turns
             (id, session_id, native_turn_id, status, ownership_generation)
         VALUES ($1, $2, 'fixture-native-turn', 'in_progress', 1)",
    )
    .bind(turn_id)
    .bind(hub_session_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE hub_sessions SET active_turn_id = $1 WHERE id = $2")
        .bind(turn_id)
        .bind(hub_session_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO integration_sessions
         (id, oauth_app_id, agent_id, owner_id, external_user_id,
          tool_definitions, metadata, hub_session_id)
         VALUES ($1, $2, $3, $4, 'external-test-user',
                 '[{\"name\":\"lookup\"}]'::jsonb, '{}'::jsonb, $5)",
    )
    .bind(session_id)
    .bind(oauth_app_id)
    .bind(agent_id)
    .bind(external_owner_id)
    .bind(hub_session_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runs
         (id, agent_id, owner_id, runtime_id, status, initial_message, source,
          integration_session_id, hub_session_id, hub_turn_id,
          session_ownership_generation)
         VALUES ($1, $2, $3, $4, 'running', 'use lookup tool',
                 'integration:message', $5, $6, $7, 1)",
    )
    .bind(run_id)
    .bind(agent_id)
    .bind(external_owner_id)
    .bind(runtime_id)
    .bind(session_id)
    .bind(hub_session_id)
    .bind(turn_id)
    .execute(&pool)
    .await
    .unwrap();

    IntegrationRuntimeFixture {
        state: Arc::new(test_state_with_pool(pool)),
        agent_id,
        platform_name,
        runtime_id,
        runtime_token,
        other_runtime_id,
        integration_token,
        second_integration_token,
        foreign_integration_token,
        session_id,
        hub_session_id,
        turn_id,
        run_id,
        tool_request_id,
    }
}

pub(crate) async fn create_test_model_connection_for_token(
    state: &Arc<AppState>,
    token: &str,
    scope: ModelConnectionScope,
    name: &str,
) -> ModelConnectionDto {
    create_model_connection(
        State(state.clone()),
        session_headers(token),
        Json(CreateModelConnectionRequest {
            vision_model_id: None,
            scope,
            name: name.into(),
            base_url: format!("http://127.0.0.1:1/{}", Uuid::new_v4()),
            api_type: ModelUpstreamProtocol::OpenaiResponses,
            allowed_model_ids: vec![
                format!("model-{}", Uuid::new_v4().simple()),
                format!("model-{}", Uuid::new_v4().simple()),
            ],
            api_key: "test-provider-secret".into(),
        }),
    )
    .await
    .unwrap()
    .0
}

pub(crate) fn test_model_selection(connection: &ModelConnectionDto) -> ModelSelectionDto {
    ModelSelectionDto {
        connection_id: connection.id,
        model_id: connection.allowed_model_ids[0].clone(),
    }
}

pub(crate) fn update_request_from_agent(agent: &AgentDto) -> UpdateAgentRequest {
    UpdateAgentRequest {
        name: agent.name.clone(),
        instructions: agent.instructions.clone(),
        visibility: agent.visibility.clone(),
        public_to: agent.public_to.clone(),
        endpoint_exposure: agent.endpoint_exposure.clone(),
        runtime_id: agent.runtime_id,
        model_selection: agent.model_selection.clone(),
        model_settings: agent.model_settings.clone(),
        subagents: agent.subagents.clone(),
        model_policy: agent.model_policy.clone(),
        sandbox_policy: agent.sandbox_policy.clone(),
        managed_skill_ids: agent.managed_skill_ids.clone(),
        secret_declarations: Some(agent.secret_declarations.clone()),
        mcp_allowlist: agent.mcp_allowlist.clone(),
        tool_allowlist: agent.tool_allowlist.clone(),
    }
}

pub(crate) async fn load_test_execution_configuration(
    pool: &PgPool,
    agent_id: Uuid,
) -> (AgentExecutionConfigurationDto, String) {
    let mut tx = pool.begin().await.unwrap();
    let configuration = load_agent_execution_configuration_tx(&mut tx, agent_id)
        .await
        .unwrap();
    let fingerprint = execution_configuration_fingerprint(&configuration).unwrap();
    tx.rollback().await.unwrap();
    (configuration, fingerprint)
}

pub(crate) fn test_state_with_pool(pool: PgPool) -> AppState {
    AppState {
        pool,
        session_cookie_secure: false,
        embed_jwt_secret: "test-embed-jwt-secret".into(),
        embed_jwt_issuer: "agent-hub-test".into(),
        embed_jwt_audience: "agent-hub-widget".into(),
        trusted_proxy_cidrs: None,
        model_secret_cipher: ModelSecretCipher::from_env_value(Some(
            &base64::engine::general_purpose::STANDARD.encode([42_u8; 32]),
        ))
        .unwrap(),
        model_proxy_http: reqwest::Client::new(),
        session_bundle_store: None,
        skill_package_store: None,
        session_bundle_max_bytes: DEFAULT_SESSION_BUNDLE_MAX_BYTES,
        auth_providers: Vec::new(),
        session_issuer: Arc::new(BrowserSessionIssuer),
        run_event_bus: Arc::new(run_event_bus::InMemoryRunEventBus::default()),
        runtime_ws: crate::runtime_ws::RuntimeWsRegistry::default(),
    }
}

pub(crate) fn test_state_with_browser_session_auth(pool: PgPool) -> AppState {
    let mut state = test_state_with_pool(pool);
    state.auth_providers = vec![Arc::new(BrowserSessionAuthProvider)];
    state
}

pub(crate) fn bearer_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    headers
}

pub(crate) async fn model_proxy_test_http_request(
    app: &Router,
    fixture: &RuntimeClaimFixture,
    model_proxy_token: &str,
    model_binding_id: Uuid,
    query: &str,
    model_id: &str,
) -> Response {
    app.clone()
        .oneshot(
            axum::http::Request::builder()
                .method(Method::POST)
                .uri(format!("/api/runtime/model-proxy/v1/responses?{query}"))
                .header(header::AUTHORIZATION, format!("Bearer {model_proxy_token}"))
                .header("x-agent-hub-run-id", fixture.run_id.to_string())
                .header(MODEL_PROXY_BINDING_ID_HEADER, model_binding_id.to_string())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "model": model_id,
                        "input": [],
                        "stream": false
                    }))
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

pub(crate) fn model_binding_id(claim: &ClaimRunResponse, binding_key: &str) -> Uuid {
    claim
        .execution_configuration
        .model_bindings
        .iter()
        .find(|binding| binding.binding_key.eq_ignore_ascii_case(binding_key))
        .unwrap_or_else(|| panic!("missing {binding_key} Run Model Binding"))
        .id
}

pub(crate) fn runtime_bundle_upload_headers(
    token: &str,
    ownership_generation: i64,
    attempt: &RuntimeSessionCheckpointAttemptDto,
    checksum: &str,
    size: usize,
    created_at: DateTime<Utc>,
) -> HeaderMap {
    let mut headers = bearer_headers(token);
    for (name, value) in [
        ("content-length", size.to_string()),
        (
            "x-agent-hub-ownership-generation",
            ownership_generation.to_string(),
        ),
        (
            "x-agent-hub-checkpoint-attempt-id",
            attempt.checkpoint_attempt_id.to_string(),
        ),
        (
            "x-agent-hub-bundle-generation",
            attempt.bundle_generation.to_string(),
        ),
        ("x-agent-hub-bundle-sha256", checksum.to_owned()),
        ("x-agent-hub-bundle-size", size.to_string()),
        (
            "x-agent-hub-history-checkpoint",
            attempt.history_checkpoint.to_string(),
        ),
        ("x-agent-hub-producing-engine-version", "0.104.0".into()),
        ("x-agent-hub-bundle-created-at", created_at.to_rfc3339()),
    ] {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(&value).unwrap(),
        );
    }
    headers
}

pub(crate) fn session_headers(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::COOKIE,
        HeaderValue::from_str(&format!("agent_hub_session={token}")).unwrap(),
    );
    headers
}

pub(crate) fn tool_request_event(tool_request_id: Uuid) -> AppendRunEventRequest {
    AppendRunEventRequest {
        event_id: Uuid::new_v4(),
        event_type: "tool_request".into(),
        role: Some("assistant".into()),
        content: Some("lookup requested".into()),
        payload: json!({
            "tool_request_id": tool_request_id,
            "tool_name": "lookup",
            "arguments": { "query": "test" }
        }),
        waiting_tool: Some(WaitingToolRunTransition {
            native_session_id: "integration-test-session".into(),
            work_dir_ref: "integration-test-workdir".into(),
        }),
    }
}

pub(crate) fn tool_request_batch(
    fixture: &IntegrationRuntimeFixture,
    request_ids: impl IntoIterator<Item = Uuid>,
) -> FinalizeToolRequestsRequest {
    FinalizeToolRequestsRequest {
        integration_session_id: Some(fixture.session_id),
        native_session_id: "integration-test-session".into(),
        work_dir_ref: "integration-test-workdir".into(),
        tool_requests: request_ids
            .into_iter()
            .map(|request_id| FinalizeToolRequestEvent {
                role: Some("assistant".into()),
                content: Some("lookup requested".into()),
                payload: json!({
                    "tool_request_id": request_id,
                    "tool_name": "lookup",
                    "arguments": { "query": "test" }
                }),
            })
            .collect(),
    }
}

pub(crate) async fn agent_execution_revision(pool: &PgPool, agent_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT execution_config_revision FROM agents WHERE id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) async fn run_event_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM run_events WHERE run_id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) async fn waiting_tool_status_event_count(pool: &PgPool, run_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM run_events
         WHERE run_id = $1 AND event_type = 'status'
           AND payload->>'status' = 'waiting_tool'",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(crate) async fn agent_run_count(pool: &PgPool, agent_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM runs WHERE agent_id = $1")
        .bind(agent_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) async fn tool_request_storage_state(
    pool: &PgPool,
    tool_request_id: Uuid,
) -> (
    String,
    Option<Value>,
    Option<Uuid>,
    Option<Uuid>,
    Option<DateTime<Utc>>,
) {
    sqlx::query_as(
        "SELECT status, result_payload, result_event_id, follow_up_run_id, responded_at
         FROM integration_tool_requests WHERE id = $1",
    )
    .bind(tool_request_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(crate) async fn tool_request_follow_up_run(
    pool: &PgPool,
    tool_request_id: Uuid,
) -> Option<Uuid> {
    sqlx::query_scalar("SELECT follow_up_run_id FROM integration_tool_requests WHERE id = $1")
        .bind(tool_request_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) fn runtime_claim_request(
    available_new_session_slots: u32,
    ready_owned_sessions: Vec<RuntimeOwnedSessionGenerationDto>,
) -> Json<RuntimeClaimRunRequest> {
    Json(RuntimeClaimRunRequest {
        available_new_session_slots,
        ready_owned_sessions,
    })
}

pub(crate) async fn claim_runtime_run(
    state: &Arc<AppState>,
    runtime_token: &str,
) -> ClaimRunResponse {
    let ready_owned_sessions = sqlx::query_as::<_, (Uuid, i64)>(
        "SELECT hs.id, hs.ownership_generation
         FROM hub_sessions hs
         JOIN runtimes rt ON rt.id = hs.runtime_owner_id
         WHERE rt.token_hash = $1",
    )
    .bind(sha256_hex(runtime_token))
    .fetch_all(&state.pool)
    .await
    .unwrap()
    .into_iter()
    .map(
        |(session_id, ownership_generation)| RuntimeOwnedSessionGenerationDto {
            session_id,
            ownership_generation,
        },
    )
    .collect();
    let response = runtime_claim_run(
        State(state.clone()),
        bearer_headers(runtime_token),
        runtime_claim_request(1, ready_owned_sessions),
    )
    .await
    .unwrap()
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

pub(crate) async fn insert_pending_session_run(pool: &PgPool, session_id: Uuid) -> Uuid {
    let turn_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO hub_session_turns
             (id, session_id, status, ownership_generation)
         SELECT $1, id, 'pending', ownership_generation
         FROM hub_sessions WHERE id = $2",
    )
    .bind(turn_id)
    .bind(session_id)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO runs
             (id, agent_id, owner_id, status, initial_message, source,
              hub_session_id, hub_turn_id, session_ownership_generation)
         SELECT $1, agent_id, owner_id, 'pending', 'next claim', 'console',
                id, $2, ownership_generation
         FROM hub_sessions WHERE id = $3",
    )
    .bind(run_id)
    .bind(turn_id)
    .bind(session_id)
    .execute(pool)
    .await
    .unwrap();
    run_id
}

pub(crate) async fn create_super_admin_session(pool: &PgPool) -> String {
    create_user_session_with_role(pool, "super_admin").await
}

pub(crate) async fn create_user_session_with_role(pool: &PgPool, role: &str) -> String {
    let user_id = Uuid::new_v4();
    let token = format!("ahs_{role}_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();
    sqlx::query(
        "INSERT INTO users
             (id, email, password, display_name, role)
         VALUES ($1, $2, 'unused', 'Task 6 User', $3)",
    )
    .bind(user_id)
    .bind(format!("task6-admin-{unique}@example.com"))
    .bind(role)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO sessions (token_hash, user_id, expires_at)
         VALUES ($1, $2, now() + interval '1 hour')",
    )
    .bind(sha256_hex(&token))
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
    token
}

pub(crate) async fn insert_idle_owned_session(
    pool: &PgPool,
    source_session_id: Uuid,
    runtime_id: Uuid,
) -> Uuid {
    let session_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, lifecycle_status,
              runtime_owner_id, ownership_generation)
         SELECT $1, owner_id, agent_id, 'hub_native', 'online', $2, 1
         FROM hub_sessions WHERE id = $3",
    )
    .bind(session_id)
    .bind(runtime_id)
    .bind(source_session_id)
    .execute(pool)
    .await
    .unwrap();
    session_id
}

pub(crate) async fn runtime_claim_run_state(
    pool: &PgPool,
    run_id: Uuid,
) -> (String, Option<Uuid>, Option<String>) {
    sqlx::query_as("SELECT status, runtime_id, model_proxy_token_hash FROM runs WHERE id = $1")
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) async fn runtime_completion_run_state(
    pool: &PgPool,
    run_id: Uuid,
) -> (
    String,
    Option<Uuid>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    sqlx::query_as(
        "SELECT status, runtime_id, model_proxy_token_hash, native_session_id, work_dir_ref
         FROM runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(crate) async fn blocked_test_application_count(pool: &PgPool, applications: &[String]) -> i64 {
    assert_eq!(applications.len(), 2);
    sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_stat_activity
         WHERE datname = current_database()
           AND application_name IN ($1, $2)
           AND wait_event_type = 'Lock'
           AND query LIKE '%SELECT agents.id FROM agents%'",
    )
    .bind(&applications[0])
    .bind(&applications[1])
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(crate) async fn wait_for_application_lock(
    pool: &PgPool,
    application_name: &str,
    query_fragment: &str,
) -> bool {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND application_name = $1
                       AND wait_event_type = 'Lock'
                       AND query LIKE '%' || $2 || '%'
                 )",
            )
            .bind(application_name)
            .bind(query_fragment)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

pub(crate) async fn wait_for_scheduler_archive_lock_overlap(
    pool: &PgPool,
    archive_application: &str,
    scheduler_application: &str,
) -> bool {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND (
                     (application_name = $1 AND query LIKE '%SELECT agents.owner_id, agents.deleted_at%')
                     OR
                     (application_name = $2 AND query LIKE
                      '%SELECT id FROM agents WHERE id = $1 AND deleted_at IS NULL FOR UPDATE%')
                   )",
            )
            .bind(archive_application)
            .bind(scheduler_application)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

pub(crate) async fn scheduler_archive_postconditions(
    pool: &PgPool,
    agent_id: Uuid,
    automation_id: Uuid,
) -> SchedulerArchivePostconditions {
    let row = sqlx::query(
        "SELECT
             agent.deleted_at IS NOT NULL AS archived,
             (SELECT count(*) FROM automations
              WHERE agent_id = $1 AND enabled = true) AS enabled_automations,
             (SELECT count(*) FROM runs
              WHERE agent_id = $1 AND source = 'automation:scheduler') AS scheduler_runs,
             (SELECT count(*) FROM runs
              WHERE agent_id = $1 AND status IN ('pending', 'running')) AS active_runs,
             (SELECT count(*) FROM runs
              WHERE agent_id = $1 AND created_at > agent.deleted_at) AS post_archive_runs,
             (SELECT last_triggered_at FROM automations WHERE id = $2) AS last_triggered_at
         FROM agents AS agent
         WHERE agent.id = $1",
    )
    .bind(agent_id)
    .bind(automation_id)
    .fetch_one(pool)
    .await
    .unwrap();
    SchedulerArchivePostconditions {
        archived: row.get("archived"),
        enabled_automations: row.get("enabled_automations"),
        scheduler_runs: row.get("scheduler_runs"),
        active_runs: row.get("active_runs"),
        post_archive_runs: row.get("post_archive_runs"),
        last_triggered_at: row.get("last_triggered_at"),
    }
}

pub(crate) async fn cleanup_scheduler_archive_fixture(pool: &PgPool, owner_id: Uuid) {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(owner_id)
        .execute(pool)
        .await
        .unwrap();
}

pub(crate) async fn active_integration_run_count(pool: &PgPool, session_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM runs
         WHERE integration_session_id = $1 AND status IN ('pending', 'running')",
    )
    .bind(session_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(crate) async fn tool_request_count(pool: &PgPool, tool_request_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM integration_tool_requests WHERE id = $1")
        .bind(tool_request_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub(crate) async fn install_tool_request_delay_trigger(
    pool: &PgPool,
    tool_request_id: Uuid,
) -> (String, String) {
    let suffix = Uuid::new_v4().simple().to_string();
    let function_name = format!("test_delay_tool_request_{suffix}");
    let trigger_name = format!("test_delay_tool_request_trigger_{suffix}");
    sqlx::query(&format!(
        "CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.id = '{tool_request_id}'::uuid THEN
             PERFORM pg_sleep(2);
           END IF;
           RETURN NEW;
         END
         $$"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {trigger_name}
         BEFORE INSERT ON integration_tool_requests
         FOR EACH ROW EXECUTE FUNCTION {function_name}()"
    ))
    .execute(pool)
    .await
    .unwrap();
    (trigger_name, function_name)
}

pub(crate) async fn remove_tool_request_delay_trigger(
    pool: &PgPool,
    trigger_name: &str,
    function_name: &str,
) {
    sqlx::query(&format!(
        "DROP TRIGGER IF EXISTS {trigger_name} ON integration_tool_requests"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!("DROP FUNCTION IF EXISTS {function_name}()"))
        .execute(pool)
        .await
        .unwrap();
}

pub(crate) async fn install_tool_request_failure_trigger(
    pool: &PgPool,
    tool_request_id: Uuid,
) -> (String, String) {
    let suffix = Uuid::new_v4().simple().to_string();
    let function_name = format!("test_fail_tool_request_{suffix}");
    let trigger_name = format!("test_fail_tool_request_trigger_{suffix}");
    sqlx::query(&format!(
        "CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.id = '{tool_request_id}'::uuid THEN
             RAISE EXCEPTION 'injected tool request failure';
           END IF;
           RETURN NEW;
         END
         $$"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {trigger_name}
         BEFORE INSERT ON integration_tool_requests
         FOR EACH ROW EXECUTE FUNCTION {function_name}()"
    ))
    .execute(pool)
    .await
    .unwrap();
    (trigger_name, function_name)
}

pub(crate) async fn remove_tool_request_failure_trigger(
    pool: &PgPool,
    trigger_name: &str,
    function_name: &str,
) {
    sqlx::query(&format!(
        "DROP TRIGGER IF EXISTS {trigger_name} ON integration_tool_requests"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!("DROP FUNCTION IF EXISTS {function_name}()"))
        .execute(pool)
        .await
        .unwrap();
}

pub(crate) async fn install_run_event_failure_trigger(
    pool: &PgPool,
    run_id: Uuid,
) -> (String, String) {
    let suffix = Uuid::new_v4().simple().to_string();
    let function_name = format!("test_fail_run_event_{suffix}");
    let trigger_name = format!("test_fail_run_event_trigger_{suffix}");
    sqlx::query(&format!(
        "CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.run_id = '{run_id}'::uuid THEN
             RAISE EXCEPTION 'injected run event failure';
           END IF;
           RETURN NEW;
         END
         $$"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "CREATE TRIGGER {trigger_name}
         BEFORE INSERT ON run_events
         FOR EACH ROW EXECUTE FUNCTION {function_name}()"
    ))
    .execute(pool)
    .await
    .unwrap();
    (trigger_name, function_name)
}

pub(crate) async fn remove_run_event_failure_trigger(
    pool: &PgPool,
    trigger_name: &str,
    function_name: &str,
) {
    sqlx::query(&format!(
        "DROP TRIGGER IF EXISTS {trigger_name} ON run_events"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!("DROP FUNCTION IF EXISTS {function_name}()"))
        .execute(pool)
        .await
        .unwrap();
}

pub(crate) fn test_model_proxy_state() -> AppState {
    test_model_proxy_state_with_timeout(Duration::from_secs(1))
}

pub(crate) fn test_model_proxy_state_with_timeout(timeout: Duration) -> AppState {
    AppState {
        pool: PgPoolOptions::new()
            .connect_lazy("postgres://agent-hub:agent-hub@127.0.0.1/agent_hub")
            .unwrap(),
        session_cookie_secure: false,
        embed_jwt_secret: "test-embed-jwt-secret".into(),
        embed_jwt_issuer: "agent-hub-test".into(),
        embed_jwt_audience: "agent-hub-widget".into(),
        trusted_proxy_cidrs: None,
        model_secret_cipher: ModelSecretCipher::from_env_value(Some(
            &base64::engine::general_purpose::STANDARD.encode([42_u8; 32]),
        ))
        .unwrap(),
        model_proxy_http: reqwest::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .read_timeout(timeout)
            .build()
            .unwrap(),
        session_bundle_store: None,
        skill_package_store: None,
        session_bundle_max_bytes: DEFAULT_SESSION_BUNDLE_MAX_BYTES,
        auth_providers: Vec::new(),
        session_issuer: Arc::new(BrowserSessionIssuer),
        run_event_bus: Arc::new(run_event_bus::InMemoryRunEventBus::default()),
        runtime_ws: crate::runtime_ws::RuntimeWsRegistry::default(),
    }
}

pub(crate) struct AttachmentFixture {
    pub(crate) state: Arc<AppState>,
    pub(crate) owner_id: Uuid,
    pub(crate) owner_token: String,
    pub(crate) foreign_token: String,
    pub(crate) agent_id: Uuid,
    pub(crate) session_id: Uuid,
}

pub(crate) async fn attachment_fixture(pool: PgPool) -> AttachmentFixture {
    let owner_id = Uuid::new_v4();
    let foreign_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let model_connection_id = Uuid::new_v4();
    let owner_token = format!("ahs_{}", Uuid::new_v4().simple());
    let foreign_token = format!("ahs_{}", Uuid::new_v4().simple());
    let unique = Uuid::new_v4().simple().to_string();
    for (id, label) in [(owner_id, "owner"), (foreign_id, "foreign")] {
        sqlx::query(
            "INSERT INTO users (id, email, password, display_name, role)
             VALUES ($1, $2, 'unused', $3, 'member')",
        )
        .bind(id)
        .bind(format!("attachment-{label}-{unique}@example.com"))
        .bind(label)
        .execute(&pool)
        .await
        .unwrap();
    }
    for (token, user_id) in [(&owner_token, owner_id), (&foreign_token, foreign_id)] {
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
    let cipher = ModelSecretCipher::from_env_value(Some(
        &base64::engine::general_purpose::STANDARD.encode([42_u8; 32]),
    ))
    .unwrap();
    let encrypted = cipher.encrypt("attachment-test-secret").unwrap();
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, owner_id, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, created_by)
         VALUES ($1, 'personal', $2, 'Attachment Test Model',
                 'http://models.example.test', 'openai_responses',
                 ARRAY['attachment-model'], $3, $4, $2)",
    )
    .bind(model_connection_id)
    .bind(owner_id)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility, model_policy,
              model_connection_id, model_id, model_settings)
         VALUES ($1, $2, 'Attachment Test Agent', 'test', 'private',
                 '{\"provider\":\"hub-proxy\"}'::jsonb, $3, 'attachment-model',
                 '{\"reasoning_effort\":\"default\",\"reasoning_summary\":\"default\",\"verbosity\":\"default\",\"context_window_tokens\":null,\"auto_compact_token_limit\":null,\"reasoning_summary_support\":\"auto\",\"service_tier\":null,\"provider_request_timeout_ms\":null,\"stream_max_retries\":null,\"stream_idle_timeout_ms\":null,\"request_settings\":{\"protocol\":\"openai_responses\"}}'::jsonb)",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(model_connection_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, lifecycle_status)
         VALUES ($1, $2, $3, 'hub_native', 'waiting_for_runtime')",
    )
    .bind(session_id)
    .bind(owner_id)
    .bind(agent_id)
    .execute(&pool)
    .await
    .unwrap();
    AttachmentFixture {
        state: Arc::new(test_state_with_browser_session_auth(pool)),
        owner_id,
        owner_token,
        foreign_token,
        agent_id,
        session_id,
    }
}

pub(crate) async fn attachment_multipart(
    boundary: &str,
    session_id: Option<Uuid>,
    file_name: &str,
    content_type: &str,
    contents: &[u8],
) -> Multipart {
    use axum::extract::FromRequest;

    let mut body = Vec::new();
    if let Some(session_id) = session_id {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"session_id\"\r\n\r\n{session_id}\r\n")
                .as_bytes(),
        );
    }
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(contents);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let request = axum::http::Request::builder()
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .unwrap();
    Multipart::from_request(request, &()).await.unwrap()
}

pub(crate) type AttachmentObjects = Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>;

pub(crate) async fn attachment_object_store() -> (
    AttachmentObjects,
    crate::session_bundle_store::S3BundleStore,
    tokio::task::JoinHandle<()>,
) {
    let objects = Arc::new(std::sync::Mutex::new(HashMap::<String, Vec<u8>>::new()));
    let route_objects = Arc::clone(&objects);
    let app = Router::new().route(
        "/attachment-bucket/{*key}",
        axum::routing::any(
            move |method: Method, headers: HeaderMap, Path(key): Path<String>, body: Body| {
                let route_objects = Arc::clone(&route_objects);
                async move {
                    match method {
                        Method::PUT => {
                            let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
                            route_objects.lock().unwrap().insert(key, bytes.to_vec());
                            StatusCode::OK.into_response()
                        }
                        Method::GET => {
                            let objects = route_objects.lock().unwrap();
                            match objects.get(&key) {
                                Some(bytes) => {
                                    let range = headers
                                        .get(header::RANGE)
                                        .and_then(|value| value.to_str().ok());
                                    let slice: &[u8] = if let Some(range_text) = range {
                                        let rest = range_text.strip_prefix("bytes=").unwrap_or("");
                                        let mut parts = rest.split('-');
                                        let start: usize =
                                            parts.next().unwrap_or("0").parse().unwrap_or(0);
                                        let end: usize = parts
                                            .next()
                                            .and_then(|value| value.parse().ok())
                                            .unwrap_or(bytes.len().saturating_sub(1));
                                        let end = end.min(bytes.len().saturating_sub(1));
                                        if start > end {
                                            &[]
                                        } else {
                                            &bytes[start..=end]
                                        }
                                    } else {
                                        bytes.as_slice()
                                    };
                                    let mut response = Response::new(Body::from(slice.to_vec()));
                                    response.headers_mut().insert(
                                        header::CONTENT_TYPE,
                                        HeaderValue::from_static("application/octet-stream"),
                                    );
                                    response
                                }
                                None => StatusCode::NOT_FOUND.into_response(),
                            }
                        }
                        Method::DELETE => {
                            route_objects.lock().unwrap().remove(&key);
                            StatusCode::NO_CONTENT.into_response()
                        }
                        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
                    }
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let store = crate::session_bundle_store::S3BundleStore::new(
        crate::session_bundle_store::S3BundleStoreConfig {
            endpoint: format!("http://{address}").parse().unwrap(),
            bucket: "attachment-bucket".into(),
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
    (objects, store, server)
}
