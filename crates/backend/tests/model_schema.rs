use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use uuid::Uuid;

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
             (id, scope, owner_id, name, base_url, api_type,
              allowed_model_ids, api_key_ciphertext, api_key_nonce)
         VALUES ($1, $2, $3, $4, 'https://models.example.test',
                 'openai_responses', ARRAY['gpt-test', 'gpt-alt']::text[],
                 decode(repeat('ab', 32), 'hex'),
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
async fn deleting_a_user_redacts_owned_personal_model_connections(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let personal_id = insert_connection(&pool, "personal", Some(owner_id)).await;

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(owner_id)
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        sqlx::query_as::<_, (bool, bool, bool, bool, bool, bool)>(
            "SELECT owner_id IS NULL, base_url IS NULL, api_key_ciphertext IS NULL,
                    api_key_nonce IS NULL, enabled = false, deleted_at IS NOT NULL
             FROM model_connections WHERE id = $1",
        )
        .bind(personal_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        (true, true, true, true, true, true)
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn model_api_connection_schema_enforces_allowlists_and_complete_selections(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let global_id = insert_connection(&pool, "global", None).await;
    let personal_id = insert_connection(&pool, "personal", Some(owner_id)).await;

    let connection: (String, Vec<String>) =
        sqlx::query_as("SELECT api_type, allowed_model_ids FROM model_connections WHERE id = $1")
            .bind(global_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(connection.0, "openai_responses");
    assert_eq!(connection.1, vec!["gpt-test", "gpt-alt"]);

    for invalid_models in [
        Vec::<String>::new(),
        vec!["".into()],
        vec!["duplicate".into(), "duplicate".into()],
        vec![" leading-space".into()],
        vec!["bad\nmodel".into()],
        vec!["x".repeat(256)],
        (0..257).map(|index| format!("model-{index}")).collect(),
    ] {
        let result =
            sqlx::query("UPDATE model_connections SET allowed_model_ids = $1 WHERE id = $2")
                .bind(invalid_models)
                .bind(global_id)
                .execute(&pool)
                .await;
        assert!(result.is_err());
    }

    let invalid_api_type =
        sqlx::query("UPDATE model_connections SET api_type = 'unsupported_protocol' WHERE id = $1")
            .bind(global_id)
            .execute(&pool)
            .await;
    assert!(invalid_api_type.is_err());

    let invalid_personal = sqlx::query(
        "INSERT INTO model_connections
             (id, scope, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce)
         VALUES ($1, 'personal', 'Invalid', 'https://example.test',
                 'openai_responses', ARRAY['gpt-test']::text[],
                 decode(repeat('ab', 32), 'hex'), decode(repeat('cd', 12), 'hex'))",
    )
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await;
    assert!(invalid_personal.is_err());

    sqlx::query(
        "INSERT INTO system_default_model_selection
             (singleton, model_connection_id, model_id, updated_by)
         VALUES (true, $1, 'gpt-alt', $2)",
    )
    .bind(global_id)
    .bind(owner_id)
    .execute(&pool)
    .await
    .unwrap();
    let invalid_default_model = sqlx::query(
        "UPDATE system_default_model_selection
         SET model_id = 'not-allowed' WHERE singleton = true",
    )
    .execute(&pool)
    .await;
    assert!(invalid_default_model.is_err());
    let personal_default = sqlx::query(
        "UPDATE system_default_model_selection
         SET model_connection_id = $1, model_id = 'gpt-test' WHERE singleton = true",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(personal_default.is_err());

    let agent_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              model_connection_id, model_id)
         VALUES ($1, $2, 'Typed Agent', 'Instructions', 'private', $3, 'gpt-alt')",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(personal_id)
    .execute(&pool)
    .await
    .unwrap();

    let half_selection = sqlx::query("UPDATE agents SET model_id = NULL WHERE id = $1")
        .bind(agent_id)
        .execute(&pool)
        .await;
    assert!(half_selection.is_err());
    let invalid_model = sqlx::query("UPDATE agents SET model_id = 'not-allowed' WHERE id = $1")
        .bind(agent_id)
        .execute(&pool)
        .await;
    assert!(invalid_model.is_err());

    let referenced_model_removal = sqlx::query(
        "UPDATE model_connections
         SET allowed_model_ids = ARRAY['gpt-test']::text[] WHERE id = $1",
    )
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(referenced_model_removal.is_err());
    let referenced_type_change =
        sqlx::query("UPDATE model_connections SET api_type = 'anthropic_messages' WHERE id = $1")
            .bind(personal_id)
            .execute(&pool)
            .await;
    assert!(referenced_type_change.is_err());

    let other_owner_id = insert_user(&pool, "member").await;
    let cross_owner_agent = sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              model_connection_id, model_id)
         VALUES ($1, $2, 'Cross Owner', 'Instructions', 'private', $3, 'gpt-test')",
    )
    .bind(Uuid::new_v4())
    .bind(other_owner_id)
    .bind(personal_id)
    .execute(&pool)
    .await;
    assert!(cross_owner_agent.is_err());

    let reserved_binding_key = sqlx::query(
        "INSERT INTO subagent_definitions
             (id, agent_id, name, description, developer_instructions)
         VALUES ($1, $2, 'MAIN', 'Conflicts with the main binding', '# Invalid')",
    )
    .bind(Uuid::new_v4())
    .bind(agent_id)
    .execute(&pool)
    .await;
    assert!(reserved_binding_key.is_err());

    sqlx::query(
        "INSERT INTO subagent_definitions
             (id, agent_id, name, description, developer_instructions,
              model_connection_id, model_id, model_settings_override)
         VALUES ($1, $2, 'reviewer', 'Reviews changes', '# Review', $3,
                 'gpt-test', '{\"reasoning_effort\":\"max\"}'::jsonb)",
    )
    .bind(Uuid::new_v4())
    .bind(agent_id)
    .bind(global_id)
    .execute(&pool)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn agent_model_settings_and_run_bindings_are_typed_and_immutable(pool: PgPool) {
    let owner_id = insert_user(&pool, "member").await;
    let connection_id = insert_connection(&pool, "personal", Some(owner_id)).await;
    let connection_name: String =
        sqlx::query_scalar("SELECT name FROM model_connections WHERE id = $1")
            .bind(connection_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let agent_id = Uuid::new_v4();
    let settings = json!({
        "reasoning_effort": "high",
        "reasoning_summary": "detailed",
        "verbosity": "low",
        "context_window_tokens": 200000,
        "auto_compact_token_limit": 160000,
        "reasoning_summary_support": "supported",
        "service_tier": "priority",
        "request_max_retries": 7,
        "stream_max_retries": 9,
        "stream_idle_timeout_ms": 420000,
        "request_settings": { "protocol": "openai_responses" }
    });
    sqlx::query(
        "INSERT INTO agents
             (id, owner_id, name, instructions, visibility,
              model_connection_id, model_id, model_settings)
         VALUES ($1, $2, 'Settings Agent', 'Instructions', 'private',
                 $3, 'gpt-test', $4)",
    )
    .bind(agent_id)
    .bind(owner_id)
    .bind(connection_id)
    .bind(&settings)
    .execute(&pool)
    .await
    .unwrap();

    let invalid_settings = sqlx::query(
        "UPDATE agents
         SET model_settings = jsonb_set(model_settings, '{request_max_retries}', '101')
         WHERE id = $1",
    )
    .bind(agent_id)
    .execute(&pool)
    .await;
    assert!(invalid_settings.is_err());
    let mismatched_request_protocol = sqlx::query(
        "UPDATE agents
         SET model_settings = jsonb_set(
             model_settings,
             '{request_settings}',
             '{\"protocol\":\"anthropic_messages\"}'::jsonb
         )
         WHERE id = $1",
    )
    .bind(agent_id)
    .execute(&pool)
    .await;
    assert!(mismatched_request_protocol.is_err());

    let run_id = Uuid::new_v4();
    let session_id = Uuid::new_v4();
    let turn_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO hub_sessions
             (id, owner_id, agent_id, origin_kind, lifecycle_status)
         VALUES ($1, $2, $3, 'hub_native', 'online')",
    )
    .bind(session_id)
    .bind(owner_id)
    .bind(agent_id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO hub_session_turns
             (id, session_id, status, ownership_generation)
         VALUES ($1, $2, 'queued', 0)",
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
         VALUES ($1, $2, $3, 'pending', 'test', 'hub', $4, $5, 0)",
    )
    .bind(run_id)
    .bind(agent_id)
    .bind(owner_id)
    .bind(session_id)
    .bind(turn_id)
    .execute(&pool)
    .await
    .unwrap();

    for (binding_key, retries) in [("main", 7), ("reviewer", 2)] {
        let mut binding_settings = settings.clone();
        binding_settings["request_max_retries"] = json!(retries);
        sqlx::query(
            "INSERT INTO run_model_bindings
                 (id, run_id, binding_key, model_connection_id,
                  connection_name_snapshot, connection_scope_snapshot,
                  model_id, api_type, model_settings)
             VALUES ($1, $2, $3, $4, $5, 'personal',
                     'gpt-test', 'openai_responses', $6)",
        )
        .bind(Uuid::new_v4())
        .bind(run_id)
        .bind(binding_key)
        .bind(connection_id)
        .bind(&connection_name)
        .bind(binding_settings)
        .execute(&pool)
        .await
        .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_model_bindings WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    let mutate_binding =
        sqlx::query("UPDATE run_model_bindings SET model_id = 'gpt-alt' WHERE run_id = $1")
            .bind(run_id)
            .execute(&pool)
            .await;
    assert!(mutate_binding.is_err());
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
              model_id_snapshot, api_type_snapshot, request_settings_snapshot,
              agent_id, agent_name_snapshot, subject_type,
              subject_user_id, subject_display_name_snapshot)
         VALUES ($1, $2, '2026-07-18 01:02:03.987654+00', 'failed', 429,
                 'upstream', 'rate_limit', 'Provider rejected the request', $3,
                 'global', 'Global Model', 'gpt-test', 'openai_responses',
                 '{\"protocol\":\"openai_responses\"}'::jsonb, $4, 'Ledger Agent',
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
              api_type_snapshot, request_settings_snapshot,
              agent_id, agent_name_snapshot, subject_type,
              subject_user_id, subject_display_name_snapshot,
              input_tokens, output_tokens, total_tokens,
              cached_tokens, reasoning_tokens)
         VALUES ($1, $2, '2026-07-18 01:02:03.123456+00', 'completed',
                 $3, 'global', 'Global Model', 'gpt-test', 'openai_responses',
                 '{\"protocol\":\"openai_responses\"}'::jsonb, $4, 'Ledger Agent',
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
              model_connection_name_snapshot, model_id_snapshot,
              api_type_snapshot, request_settings_snapshot, subject_type,
              input_tokens, output_tokens, total_tokens, cached_tokens,
              reasoning_tokens)
         VALUES ($1, $2, 'completed', 'global', 'Global Model', 'gpt-test',
                 'openai_responses', '{\"protocol\":\"openai_responses\"}'::jsonb,
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
                api_type_snapshot, request_settings_snapshot
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
        row.get::<String, _>("api_type_snapshot"),
        "openai_responses"
    );
    assert_eq!(
        row.get::<Value, _>("request_settings_snapshot"),
        json!({ "protocol": "openai_responses" })
    );

    let mutate_protocol = sqlx::query(
        "UPDATE model_token_usage
         SET api_type_snapshot = 'anthropic_messages'
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
