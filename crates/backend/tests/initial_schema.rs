use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn initial_schema_seeds_required_control_rows(pool: PgPool) {
    let auth_policy: (bool, bool, bool) = sqlx::query_as(
        "SELECT password_registration_enabled, password_login_enabled,
                email_verification_required
         FROM auth_policy WHERE singleton = true",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(auth_policy, (true, true, false));
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
