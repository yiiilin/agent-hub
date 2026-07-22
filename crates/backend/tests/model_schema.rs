use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(sqlx::FromRow)]
struct ModelConnectionParameterDefaults {
    reasoning_effort: String,
    reasoning_summary: String,
    verbosity: String,
    context_window_tokens: Option<i64>,
    auto_compact_token_limit: Option<i64>,
    reasoning_summary_support: String,
    service_tier: Option<String>,
    request_max_retries: Option<i32>,
    stream_max_retries: Option<i32>,
    stream_idle_timeout_ms: Option<i64>,
}

async fn insert_user(pool: &PgPool, role: &str) -> Uuid {
    let id = Uuid::new_v4();
    let suffix = id.simple().to_string();
    sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role, username)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id)
    .bind(format!("{suffix}@example.test"))
    .bind("password-hash")
    .bind(format!("User {suffix}"))
    .bind(role)
    .bind(format!("user-{suffix}"))
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn insert_connection(pool: &PgPool, scope: &str, owner_id: Option<Uuid>) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, owner_id, name, base_url, model_id,
              api_key_ciphertext, api_key_nonce)
         VALUES ($1, $2, $3, $4, 'https://models.example.test',
                 'gpt-test', decode(repeat('ab', 32), 'hex'),
                 decode(repeat('cd', 12), 'hex'))",
    )
    .bind(id)
    .bind(scope)
    .bind(owner_id)
    .bind(format!("Model {id}"))
    .execute(pool)
    .await
    .unwrap();
    id
}

#[sqlx::test(migrations = "./migrations")]
async fn initial_schema_omits_direct_model_contract(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let agent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility)
         VALUES ($1, $2, 'Initial Agent', 'Instructions', 'private')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();

    let has_direct_fallback: bool =
        sqlx::query_scalar("SELECT model_policy ? 'direct_fallback' FROM agents WHERE id = $1")
            .bind(agent_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!has_direct_fallback);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name = 'runtimes'
               AND column_name = 'direct_model_enabled'",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn model_schema_enforces_scope_defaults_and_reasoning(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let global_id = insert_connection(&pool, "global", None).await;
    let personal_id = insert_connection(&pool, "personal", Some(owner_id)).await;

    let default_protocol: String =
        sqlx::query_scalar("SELECT upstream_protocol FROM model_connections WHERE id = $1")
            .bind(global_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(default_protocol, "openai_responses");
    let default_request_parameters: Value =
        sqlx::query_scalar("SELECT request_parameters FROM model_connections WHERE id = $1")
            .bind(global_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        default_request_parameters,
        json!({ "protocol": "openai_responses" })
    );

    sqlx::query(
        "UPDATE model_connections
         SET upstream_protocol = 'anthropic_messages',
             request_parameters = '{
                 \"protocol\": \"anthropic_messages\",
                 \"temperature\": 0.4,
                 \"max_tokens\": 8192
             }'::jsonb
         WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await
    .unwrap();

    let mismatched_request_parameters = sqlx::query(
        "UPDATE model_connections
         SET request_parameters = '{\"protocol\":\"openai_responses\"}'::jsonb
         WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(mismatched_request_parameters.is_err());

    let mutually_exclusive_anthropic_sampling = sqlx::query(
        "UPDATE model_connections
         SET request_parameters = '{
             \"protocol\": \"anthropic_messages\",
             \"temperature\": 0.4,
             \"top_p\": 0.9,
             \"max_tokens\": 8192
         }'::jsonb
         WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(mutually_exclusive_anthropic_sampling.is_err());

    sqlx::query(
        "UPDATE model_connections
         SET upstream_protocol = 'openai_chat_completions',
             request_parameters = '{
                 \"protocol\": \"openai_chat_completions\",
                 \"temperature\": 0.7,
                 \"top_p\": 0.8,
                 \"max_completion_tokens\": 4096
             }'::jsonb
         WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await
    .unwrap();

    let invalid_protocol = sqlx::query(
        "UPDATE model_connections
         SET upstream_protocol = 'unsupported_protocol'
         WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(invalid_protocol.is_err());

    let invalid_personal = sqlx::query(
        "INSERT INTO model_connections
             (id, scope, name, base_url, model_id, api_key_ciphertext, api_key_nonce)
         VALUES ($1, 'personal', 'Invalid', 'https://example.test', 'gpt-test',
                 decode(repeat('ab', 32), 'hex'), decode(repeat('cd', 12), 'hex'))",
    )
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;
    assert!(invalid_personal.is_err());

    sqlx::query(
        "INSERT INTO system_default_model_connection
             (singleton, model_connection_id, updated_by)
         VALUES (true, $1, $2)",
    )
    .bind(global_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();

    let personal_default = sqlx::query(
        "UPDATE system_default_model_connection
         SET model_connection_id = $1 WHERE singleton = true",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(personal_default.is_err());

    let agent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              default_model_connection_id, reasoning_effort)
         VALUES ($1, $2, 'Typed Agent', 'Instructions', 'private', $3, 'ultra')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(personal_id)
    .execute(&pool)
    .await
    .unwrap();

    let invalid_reasoning =
        sqlx::query("UPDATE agents SET reasoning_effort = 'extreme' WHERE id = $1")
            .bind(agent_id)
            .execute(&pool)
            .await;
    assert!(invalid_reasoning.is_err());

    let other_owner_id = insert_user(&pool, "member").await;
    let cross_owner_agent = sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              default_model_connection_id)
         VALUES ($1, $2, 'Cross Owner', 'Instructions', 'private', $3)",
    )
    .bind(Uuid::new_v4())
    .bind(other_owner_id)
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(cross_owner_agent.is_err());

    let subagent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO codex_subagent_definitions
             (id, agent_id, name, description, developer_instructions,
              model_connection_id, reasoning_effort)
         VALUES ($1, $2, 'reviewer', 'Reviews changes', '# Review', $3, 'max')",
    )
    .bind(subagent_id)
    .bind(agent_id)
    .bind(global_id)
    .execute(&pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn model_connection_parameter_schema_defaults_and_constraints(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let connection_id = insert_connection(&pool, "personal", Some(owner_id)).await;

    let defaults: ModelConnectionParameterDefaults = sqlx::query_as(
        "SELECT reasoning_effort, reasoning_summary, verbosity,
                context_window_tokens, auto_compact_token_limit,
                reasoning_summary_support, service_tier,
                request_max_retries, stream_max_retries,
                stream_idle_timeout_ms
         FROM model_connections WHERE id = $1",
    )
    .bind(connection_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(defaults.reasoning_effort, "default");
    assert_eq!(defaults.reasoning_summary, "default");
    assert_eq!(defaults.verbosity, "default");
    assert_eq!(defaults.context_window_tokens, None);
    assert_eq!(defaults.auto_compact_token_limit, None);
    assert_eq!(defaults.reasoning_summary_support, "auto");
    assert_eq!(defaults.service_tier, None);
    assert_eq!(defaults.request_max_retries, None);
    assert_eq!(defaults.stream_max_retries, None);
    assert_eq!(defaults.stream_idle_timeout_ms, None);

    sqlx::query(
        "UPDATE model_connections
         SET reasoning_effort = 'high', reasoning_summary = 'detailed',
             verbosity = 'low', context_window_tokens = 200000,
             auto_compact_token_limit = 160000,
             reasoning_summary_support = 'supported', service_tier = 'priority',
             request_max_retries = 7, stream_max_retries = 9,
             stream_idle_timeout_ms = 420000
         WHERE id = $1",
    )
    .bind(connection_id)
    .execute(&pool)
    .await
    .unwrap();

    let excessive_retries =
        sqlx::query("UPDATE model_connections SET request_max_retries = 101 WHERE id = $1")
            .bind(connection_id)
            .execute(&pool)
            .await;
    assert!(excessive_retries.is_err());

    let compact_beyond_context = sqlx::query(
        "UPDATE model_connections
         SET context_window_tokens = 100000, auto_compact_token_limit = 100001
         WHERE id = $1",
    )
    .bind(connection_id)
    .execute(&pool)
    .await;
    assert!(compact_beyond_context.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn model_ledgers_are_precise_append_only_and_anonymized(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let subject_id = insert_user(&pool, "member").await;
    let connection_id = insert_connection(&pool, "global", None).await;
    let agent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agents (id, owner_id, name, instructions, visibility)
         VALUES ($1, $2, 'Ledger Agent', 'Instructions', 'private')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();

    let error_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO model_call_errors
             (id, request_id, occurred_at, response_status, upstream_http_status,
              error_kind, error_code, message, model_connection_id,
              model_connection_scope_snapshot, model_connection_name_snapshot,
              model_id_snapshot, agent_id, agent_name_snapshot, subject_type,
              subject_user_id, subject_display_name_snapshot)
         VALUES ($1, $2, '2026-07-18 01:02:03.987654+00', 'failed', 429,
                 'upstream', 'rate_limit', 'Provider rejected the request', $3,
                 'global', 'Global Model', 'gpt-test', $4, 'Ledger Agent',
                 'user', $5, 'Subject Before Delete')",
    )
    .bind(error_id)
    .bind(Uuid::new_v4())
    .bind(connection_id)
    .bind(agent_id)
    .bind(subject_id)
    .execute(&pool)
    .await
    .unwrap();

    let usage_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO model_token_usage
             (id, request_id, occurred_at, response_status,
              model_connection_id, model_connection_scope_snapshot,
              model_connection_name_snapshot, model_id_snapshot,
              agent_id, agent_name_snapshot, subject_type,
              subject_user_id, subject_display_name_snapshot,
              input_tokens, output_tokens, total_tokens,
              cached_tokens, reasoning_tokens)
         VALUES ($1, $2, '2026-07-18 01:02:03.123456+00', 'completed',
                 $3, 'global', 'Global Model', 'gpt-test', $4, 'Ledger Agent',
                 'user', $5, 'Subject Before Delete', 10, 4, 14, 3, 2)",
    )
    .bind(usage_id)
    .bind(Uuid::new_v4())
    .bind(connection_id)
    .bind(agent_id)
    .bind(subject_id)
    .execute(&pool)
    .await
    .unwrap();

    let millisecond_aligned: bool = sqlx::query_scalar(
        "SELECT occurred_at = date_trunc('milliseconds', occurred_at)
         FROM model_token_usage WHERE id = $1",
    )
    .bind(usage_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(millisecond_aligned);

    let invalid_subset = sqlx::query(
        "INSERT INTO model_token_usage
             (id, request_id, response_status, model_connection_scope_snapshot,
              model_connection_name_snapshot, model_id_snapshot, subject_type,
              input_tokens, output_tokens, total_tokens, cached_tokens,
              reasoning_tokens)
         VALUES ($1, $2, 'completed', 'global', 'Global Model', 'gpt-test',
                 'system', 2, 1, 3, 3, 0)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;
    assert!(invalid_subset.is_err());

    let mutate_tokens = sqlx::query("UPDATE model_token_usage SET total_tokens = 15 WHERE id = $1")
        .bind(usage_id)
        .execute(&pool)
        .await;
    assert!(mutate_tokens.is_err());

    let delete_usage = sqlx::query("DELETE FROM model_token_usage WHERE id = $1")
        .bind(usage_id)
        .execute(&pool)
        .await;
    assert!(delete_usage.is_err());

    let delete_error = sqlx::query("DELETE FROM model_call_errors WHERE id = $1")
        .bind(error_id)
        .execute(&pool)
        .await;
    assert!(delete_error.is_err());

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(subject_id)
        .execute(&pool)
        .await
        .unwrap();

    let row = sqlx::query(
        "SELECT subject_user_id, subject_display_name_snapshot, total_tokens,
                upstream_protocol_snapshot
         FROM model_token_usage WHERE id = $1",
    )
    .bind(usage_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row.try_get::<Option<Uuid>, _>("subject_user_id").unwrap(),
        None
    );
    assert_eq!(
        row.try_get::<Option<String>, _>("subject_display_name_snapshot")
            .unwrap(),
        None
    );
    assert_eq!(row.get::<i64, _>("total_tokens"), 14);
    assert_eq!(
        row.get::<String, _>("upstream_protocol_snapshot"),
        "openai_responses"
    );

    let mutate_protocol = sqlx::query(
        "UPDATE model_token_usage
         SET upstream_protocol_snapshot = 'anthropic_messages'
         WHERE id = $1",
    )
    .bind(usage_id)
    .execute(&pool)
    .await;
    assert!(mutate_protocol.is_err());

    let error_identity = sqlx::query(
        "SELECT subject_user_id, subject_display_name_snapshot
         FROM model_call_errors WHERE id = $1",
    )
    .bind(error_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        error_identity
            .try_get::<Option<Uuid>, _>("subject_user_id")
            .unwrap(),
        None
    );
    assert_eq!(
        error_identity
            .try_get::<Option<String>, _>("subject_display_name_snapshot")
            .unwrap(),
        None
    );

    sqlx::query("UPDATE agents SET name = 'Renamed Agent' WHERE id = $1")
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE model_connections SET name = 'Renamed Model' WHERE id = $1")
        .bind(connection_id)
        .execute(&pool)
        .await
        .unwrap();
    let snapshots: (String, String, i64) = sqlx::query_as(
        "SELECT agent_name_snapshot, model_connection_name_snapshot, total_tokens
         FROM model_token_usage WHERE id = $1",
    )
    .bind(usage_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        snapshots,
        ("Ledger Agent".into(), "Global Model".into(), 14)
    );

    sqlx::query("DELETE FROM agents WHERE id = $1")
        .bind(agent_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM model_connections WHERE id = $1")
        .bind(connection_id)
        .execute(&pool)
        .await
        .unwrap();
    let retained: (Option<Uuid>, Option<Uuid>, String, String, i64) = sqlx::query_as(
        "SELECT agent_id, model_connection_id, agent_name_snapshot,
                model_connection_name_snapshot, total_tokens
         FROM model_token_usage WHERE id = $1",
    )
    .bind(usage_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        retained,
        (None, None, "Ledger Agent".into(), "Global Model".into(), 14,)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM model_call_errors WHERE id = $1")
            .bind(error_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
