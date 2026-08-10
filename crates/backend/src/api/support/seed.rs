//! 开发环境种子数据。

use crate::{
    build_skill_package_archive, parse_uploaded_skill_markdown, validate_model_connection_fields,
    StagedSkillPackageFile,
};
use crate::{
    commit_skill_package_upload, enqueue_skill_package_deletion,
    publish_skill_configuration_change_tx, StagedSkillPackageUpload,
};
use anyhow::Context;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

use super::crypto::{password_hash, sha256_hex};
use crate::AppState;
use agent_hub_backend::ModelSecretCipher;
use agent_hub_shared::*;
use std::sync::Arc;
use tracing::{info, warn};

pub(crate) async fn seed_dev_user(pool: &PgPool) -> anyhow::Result<()> {
    let user_id = Uuid::new_v4();
    let password = password_hash("admin123")?;
    sqlx::query(
        "INSERT INTO users (id, email, password, display_name, role)
         VALUES ($1, 'admin@example.com', $2, 'Admin', 'super_admin')
         ON CONFLICT DO NOTHING",
    )
    .bind(user_id)
    .bind(password)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE auth_policy
         SET password_registration_enabled = false, updated_at = now()
         WHERE singleton = true",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn seed_dev_model_connection(
    pool: &PgPool,
    cipher: &ModelSecretCipher,
    base_url: &str,
    allowed_model_ids: Vec<String>,
    api_key: &str,
) -> anyhow::Result<Uuid> {
    const NAME: &str = "Compose Responses";
    let seeded_id = Uuid::parse_str("de000000-0000-4000-8000-000000000001")?;
    let allowed_model_ids = normalize_allowed_model_ids(allowed_model_ids)
        .context("invalid development Model Connection model ids")?;
    let default_model_id = allowed_model_ids[0].clone();
    let fields = validate_model_connection_fields(NAME, base_url, allowed_model_ids, Some(api_key))
        .map_err(|error| {
            anyhow::anyhow!("invalid development Model Connection: {}", error.message)
        })?;
    let encrypted = cipher.encrypt(api_key)?;
    let mut tx = pool.begin().await?;
    let id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id
         FROM model_connections
         WHERE id = $1
            OR (scope = 'global' AND lower(btrim(name)) = lower($2) AND deleted_at IS NULL)
         ORDER BY (id = $1) DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(seeded_id)
    .bind(NAME)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(seeded_id);

    sqlx::query(
        "INSERT INTO model_connections
             (id, scope, owner_id, name, base_url, api_type, allowed_model_ids,
              api_key_ciphertext, api_key_nonce, enabled, deleted_at)
         VALUES ($1, 'global', NULL, $2, $3, 'openai_responses', $4, $5, $6, true, NULL)
         ON CONFLICT (id) DO UPDATE
         SET scope = 'global', owner_id = NULL, name = EXCLUDED.name,
             base_url = EXCLUDED.base_url, api_type = EXCLUDED.api_type,
             allowed_model_ids = EXCLUDED.allowed_model_ids,
             api_key_ciphertext = EXCLUDED.api_key_ciphertext,
             api_key_nonce = EXCLUDED.api_key_nonce, enabled = true,
             deleted_at = NULL, updated_at = CURRENT_TIMESTAMP(3)",
    )
    .bind(id)
    .bind(fields.name)
    .bind(fields.base_url)
    .bind(fields.allowed_model_ids)
    .bind(encrypted.ciphertext)
    .bind(encrypted.nonce)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO system_default_model_selection
             (singleton, model_connection_id, model_id, updated_by)
         VALUES (true, $1, $2, NULL)
         ON CONFLICT (singleton) DO UPDATE
         SET model_connection_id = EXCLUDED.model_connection_id,
             model_id = EXCLUDED.model_id,
             updated_by = NULL,
             updated_at = CURRENT_TIMESTAMP(3)",
    )
    .bind(id)
    .bind(default_model_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuiltinSkillSeedStatus {
    Seeded,
    Deferred,
    Unavailable,
}

const BUILTIN_MAINTENANCE_SKILL_DIR: &str = "/usr/share/agent-hub/builtin/agent-hub-maintenance";
const BUILTIN_MAINTENANCE_SKILL_NAME: &str = "agent-hub-maintenance";

pub(crate) async fn builtin_skill_seed_loop(state: Arc<AppState>) {
    loop {
        match seed_builtin_maintenance_skill(&state).await {
            Ok(BuiltinSkillSeedStatus::Seeded) => {
                info!("builtin maintenance Skill is present");
                return;
            }
            Ok(BuiltinSkillSeedStatus::Deferred) => {
                info!("builtin maintenance Skill seed deferred until a super_admin user exists");
            }
            Ok(BuiltinSkillSeedStatus::Unavailable) => {
                info!("builtin maintenance Skill files are not bundled; skipping seed");
                return;
            }
            Err(error) => {
                warn!(error = %error, "builtin maintenance Skill seed failed");
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

pub(crate) async fn seed_builtin_maintenance_skill(
    state: &AppState,
) -> anyhow::Result<BuiltinSkillSeedStatus> {
    if std::env::var("HUB_BUILTIN_MAINTENANCE_SKILL")
        .map(|value| value == "false")
        .unwrap_or(false)
    {
        return Ok(BuiltinSkillSeedStatus::Unavailable);
    }
    let dir = std::env::var("HUB_BUILTIN_SKILL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(BUILTIN_MAINTENANCE_SKILL_DIR));
    let skill_md_path = dir.join("SKILL.md");
    let binary_path = dir.join("bin/agent-hub");
    if !skill_md_path.is_file() || !binary_path.is_file() {
        return Ok(BuiltinSkillSeedStatus::Unavailable);
    }

    let owner_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM users WHERE role = 'super_admin' ORDER BY created_at, id LIMIT 1",
    )
    .fetch_optional(&state.pool)
    .await?;
    let Some(owner_id) = owner_id else {
        return Ok(BuiltinSkillSeedStatus::Deferred);
    };

    let markdown = std::fs::read(&skill_md_path)
        .with_context(|| format!("read builtin SKILL.md {}", skill_md_path.display()))?;
    let (name, description, content) = parse_uploaded_skill_markdown(&markdown)
        .map_err(|error| anyhow::anyhow!("invalid builtin SKILL.md: {}", error.message))?;
    anyhow::ensure!(
        name == BUILTIN_MAINTENANCE_SKILL_NAME,
        "builtin SKILL.md name must be {BUILTIN_MAINTENANCE_SKILL_NAME}"
    );
    let content_checksum = sha256_hex(&content);
    let binary_bytes = std::fs::read(&binary_path)
        .with_context(|| format!("read builtin CLI {}", binary_path.display()))?;
    let binary_size = binary_bytes.len() as u64;
    let binary_checksum = format!("{:x}", Sha256::digest(&binary_bytes));

    let existing = sqlx::query_as::<_, (Uuid, String, Option<Uuid>)>(
        "SELECT id, content_checksum_sha256, current_package_id
         FROM skills
         WHERE owner_id = $1 AND name = $2
         LIMIT 1",
    )
    .bind(owner_id)
    .bind(&name)
    .fetch_optional(&state.pool)
    .await?;

    let staging = tempfile::tempdir().context("create builtin Skill staging directory")?;
    let archive_path = staging.path().join("package.tar.zst");
    let staged_file = StagedSkillPackageFile {
        path: "bin/agent-hub".into(),
        staged_path: binary_path.clone(),
        size_bytes: binary_size,
        checksum_sha256: binary_checksum.clone(),
        executable: true,
    };
    let (archive_size_bytes, archive_checksum_sha256) = {
        let archive_path = archive_path.clone();
        let staged_file = staged_file.clone();
        tokio::task::spawn_blocking(move || {
            build_skill_package_archive(&archive_path, &[staged_file])
        })
        .await
        .context("join builtin Skill archive build")?
        .context("build builtin Skill archive")?
    };
    let files = vec![SkillPackageFileDto {
        path: "bin/agent-hub".into(),
        size_bytes: binary_size,
        checksum_sha256: binary_checksum,
        executable: true,
    }];
    let upload = StagedSkillPackageUpload {
        _staging: staging,
        name: name.clone(),
        description,
        content: content.clone(),
        archive_path: Some(archive_path),
        archive_size_bytes: Some(archive_size_bytes),
        archive_checksum_sha256: Some(archive_checksum_sha256.clone()),
        files,
    };

    let (skill_id, package_up_to_date) = match existing {
        Some((skill_id, existing_checksum, current_package_id)) => {
            let package_checksum = match current_package_id {
                Some(package_id) => sqlx::query_scalar::<_, String>(
                    "SELECT checksum_sha256 FROM skill_packages WHERE id = $1 AND skill_id = $2",
                )
                .bind(package_id)
                .bind(skill_id)
                .fetch_optional(&state.pool)
                .await?,
                None => None,
            };
            let up_to_date = existing_checksum == content_checksum
                && package_checksum.as_deref() == Some(archive_checksum_sha256.as_str());
            (skill_id, up_to_date)
        }
        None => {
            let skill_id = Uuid::new_v4();
            let mut tx = state.pool.begin().await?;
            sqlx::query(
                "INSERT INTO skills
                     (id, owner_id, name, description, content, content_checksum_sha256)
                 VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(skill_id)
            .bind(owner_id)
            .bind(&name)
            .bind(&upload.description)
            .bind(&upload.content)
            .bind(content_checksum)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            (skill_id, false)
        }
    };

    if !package_up_to_date {
        let store = state
            .skill_package_store
            .as_ref()
            .cloned()
            .context("Skill package object storage is not configured")?;
        let package_id = Uuid::new_v4();
        let object_key = format!("skill-packages/{owner_id}/{skill_id}/{package_id}.tar.zst");
        let archive_path = upload
            .archive_path
            .as_deref()
            .context("builtin Skill archive path is missing")?;
        store
            .put_file(
                &object_key,
                archive_path,
                archive_size_bytes,
                &archive_checksum_sha256,
            )
            .await
            .context("store builtin Skill package archive")?;
        if let Err(error) = commit_skill_package_upload(
            &state.pool,
            skill_id,
            owner_id,
            Some(package_id),
            Some(&object_key),
            &upload,
        )
        .await
        {
            let _ = enqueue_skill_package_deletion(&state.pool, owner_id, &object_key).await;
            return Err(anyhow::anyhow!(
                "commit builtin Skill package failed: {}",
                error.message
            ));
        }
    }

    if let Ok(value) = std::env::var("AGENT_HUB_MAINTENANCE_AGENT_ID") {
        let Ok(agent_id) = value.trim().parse::<Uuid>() else {
            warn!("AGENT_HUB_MAINTENANCE_AGENT_ID is not a valid UUID; ignoring");
            return Ok(BuiltinSkillSeedStatus::Seeded);
        };
        let agent_owner = sqlx::query_scalar::<_, Uuid>(
            "SELECT owner_id FROM agents WHERE id = $1 AND deleted_at IS NULL",
        )
        .bind(agent_id)
        .fetch_optional(&state.pool)
        .await?;
        if agent_owner == Some(owner_id) {
            let mut tx = state.pool.begin().await?;
            let inserted = sqlx::query(
                "INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
            )
            .bind(agent_id)
            .bind(skill_id)
            .execute(&mut *tx)
            .await?;
            if inserted.rows_affected() > 0 {
                publish_skill_configuration_change_tx(&mut tx, &[agent_id])
                    .await
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            }
            tx.commit().await?;
        } else {
            warn!(
                agent_id = %agent_id,
                "AGENT_HUB_MAINTENANCE_AGENT_ID does not belong to the builtin Skill owner"
            );
        }
    }

    Ok(BuiltinSkillSeedStatus::Seeded)
}

pub(crate) async fn ensure_dev_runtime_enrollment_token(
    pool: &PgPool,
    token: &str,
) -> anyhow::Result<()> {
    let token = token.trim();
    if token.is_empty() {
        anyhow::bail!("DEV_RUNTIME_ENROLLMENT_TOKEN must not be empty");
    }
    sqlx::query(
        "INSERT INTO runtime_enrollment_tokens (id, token_hash, expires_at)
         VALUES ($1, $2, now() + interval '30 minutes')
         ON CONFLICT (token_hash) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(sha256_hex(token))
    .execute(pool)
    .await?;
    Ok(())
}
