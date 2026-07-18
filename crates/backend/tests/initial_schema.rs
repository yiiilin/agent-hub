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

    let codex_rollout: (Option<String>, Option<String>, String) = sqlx::query_as(
        "SELECT active_version, target_version, status
         FROM codex_version_rollout WHERE singleton = true",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(codex_rollout, (None, None, "idle".into()));
}
