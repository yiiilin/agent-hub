use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn initial_schema_seeds_required_control_rows(pool: PgPool) {
    let auth_policy: (bool, bool, bool) = sqlx::query_as(
        "SELECT password_registration_enabled, password_login_enabled,
                ldap_login_enabled
         FROM auth_policy WHERE singleton = true",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(auth_policy, (true, true, false));
}

#[sqlx::test(migrations = "./migrations")]
async fn hub_users_use_required_email_identity(pool: PgPool) {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name, is_nullable
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'users'
           AND column_name IN ('email', 'display_name', 'username', 'email_verified')
         ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        columns,
        vec![
            ("display_name".into(), "NO".into()),
            ("email".into(), "NO".into()),
        ]
    );

    sqlx::query(
        "INSERT INTO users (id, email, display_name, role)
         VALUES (gen_random_uuid(), 'owner@example.com', 'Owner', 'member')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let duplicate = sqlx::query(
        "INSERT INTO users (id, email, display_name, role)
         VALUES (gen_random_uuid(), ' OWNER@example.com ', 'Other', 'member')",
    )
    .execute(&pool)
    .await;
    assert!(duplicate.is_err());

    let empty_display_name = sqlx::query(
        "INSERT INTO users (id, email, display_name, role)
         VALUES (gen_random_uuid(), 'other@example.com', '   ', 'member')",
    )
    .execute(&pool)
    .await;
    assert!(empty_display_name.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn normalized_email_identity_is_concurrency_safe(pool: PgPool) {
    let first = sqlx::query(
        "INSERT INTO users (id, email, display_name, role)
         VALUES (gen_random_uuid(), 'parallel@example.com', 'First', 'member')",
    )
    .execute(&pool);
    let second = sqlx::query(
        "INSERT INTO users (id, email, display_name, role)
         VALUES (gen_random_uuid(), ' PARALLEL@example.com ', 'Second', 'member')",
    )
    .execute(&pool);

    let (first, second) = tokio::join!(first, second);
    assert_ne!(first.is_ok(), second.is_ok());

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM users WHERE lower(btrim(email)) = 'parallel@example.com'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn ldap_configuration_and_login_throttles_have_final_shape(pool: PgPool) {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND (table_name, column_name) IN (
             ('ldap_configuration', 'singleton'),
             ('ldap_configuration', 'url'),
             ('ldap_configuration', 'security_mode'),
             ('ldap_configuration', 'base_dn'),
             ('ldap_configuration', 'bind_identity_template'),
             ('ldap_configuration', 'user_filter'),
             ('ldap_configuration', 'email_attribute'),
             ('ldap_configuration', 'display_name_attribute'),
             ('ldap_configuration', 'allow_insecure'),
             ('ldap_configuration', 'skip_tls_verify'),
             ('login_email_failures', 'normalized_email'),
             ('login_email_failures', 'failed_attempts'),
             ('login_email_failures', 'window_started_at'),
             ('login_ip_attempts', 'source_ip'),
             ('login_ip_attempts', 'attempts'),
             ('login_ip_attempts', 'window_started_at')
           )
         ORDER BY table_name, column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(columns.len(), 16);

    let cleanup_indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname
         FROM pg_indexes
         WHERE schemaname = 'public'
           AND indexname IN (
             'login_email_failures_window_idx',
             'login_ip_attempts_window_idx'
           )
         ORDER BY indexname",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(cleanup_indexes.len(), 2);

    let invalid_email_counter = sqlx::query(
        "INSERT INTO login_email_failures
             (normalized_email, failed_attempts, window_started_at)
         VALUES ('Person@Example.com', 0, now())",
    )
    .execute(&pool)
    .await;
    assert!(invalid_email_counter.is_err());

    let invalid_ip_counter = sqlx::query(
        "INSERT INTO login_ip_attempts (source_ip, attempts, window_started_at)
         VALUES ('192.0.2.1', 0, now())",
    )
    .execute(&pool)
    .await;
    assert!(invalid_ip_counter.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn runs_use_native_session_id_for_execution_engine_session(pool: PgPool) {
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'runs'
           AND column_name IN ('session_id', 'native_session_id')
         ORDER BY column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(columns, vec!["native_session_id"]);
}

#[sqlx::test(migrations = "./migrations")]
async fn client_tool_grants_reuse_credentials_and_snapshot_each_run(pool: PgPool) {
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND (table_name, column_name) IN (
             ('oauth_apps', 'client_tool_definitions'),
             ('embed_sessions', 'client_instance_id'),
             ('embed_sessions', 'client_tool_definitions'),
             ('runs', 'client_instance_id'),
             ('runs', 'client_tool_snapshot'),
             ('integration_tool_requests', 'hub_session_id'),
             ('integration_tool_requests', 'position'),
             ('integration_tool_requests', 'claimed_by_client_instance_id')
           )
         ORDER BY table_name, column_name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(
        columns,
        vec![
            ("embed_sessions".into(), "client_instance_id".into()),
            ("embed_sessions".into(), "client_tool_definitions".into()),
            (
                "integration_tool_requests".into(),
                "claimed_by_client_instance_id".into()
            ),
            ("integration_tool_requests".into(), "hub_session_id".into()),
            ("integration_tool_requests".into(), "position".into()),
            ("oauth_apps".into(), "client_tool_definitions".into()),
            ("runs".into(), "client_instance_id".into()),
            ("runs".into(), "client_tool_snapshot".into()),
        ]
    );

    let separate_grant_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.client_tool_grants') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!separate_grant_table_exists);
}
