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
