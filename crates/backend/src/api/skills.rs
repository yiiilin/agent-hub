//! skills 领域模块。
//!
//! Skill CRUD、Skill 包（package）上传/替换/删除、Skill 包对象删除队列，
//! 以及 Skill 可见性/托管查询辅助函数。

use super::*;
use agent_hub_shared::*;
use anyhow::Context;
use axum::{
    extract::{Multipart, Path, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::{
    collections::BTreeSet,
    io::Read,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::AsyncWriteExt;
use tracing::warn;
use uuid::Uuid;

use crate::normalize_visibility;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkillPackageUploadManifest {
    paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StagedSkillPackageFile {
    pub(crate) path: String,
    pub(crate) staged_path: PathBuf,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_sha256: String,
    pub(crate) executable: bool,
}

pub(crate) struct StagedSkillPackageUpload {
    pub(crate) _staging: tempfile::TempDir,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) content: String,
    pub(crate) archive_path: Option<PathBuf>,
    pub(crate) archive_size_bytes: Option<u64>,
    pub(crate) archive_checksum_sha256: Option<String>,
    pub(crate) files: Vec<SkillPackageFileDto>,
}

#[derive(Debug)]
pub(crate) struct LockedSkillChange {
    affected_agent_ids: Vec<Uuid>,
    current_package_id: Option<Uuid>,
    current_package_object_key: Option<String>,
}

pub(crate) async fn list_skills(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<SkillDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT skills.id, skills.owner_id,
                (SELECT email FROM users WHERE id = skills.owner_id) AS owner_email,
                skills.name, skills.description, skills.visibility, skills.public_to,
                skills.content, skills.revision, skills.content_checksum_sha256,
                skills.created_at, skills.updated_at,
                packages.id AS package_id, packages.format_version AS package_format_version,
                packages.size_bytes AS package_size_bytes,
                packages.checksum_sha256 AS package_checksum_sha256,
                packages.files AS package_files
         FROM skills
         LEFT JOIN skill_packages AS packages ON packages.id = skills.current_package_id
         WHERE skills.owner_id = $1 OR skills.visibility = 'public'
            OR (skills.visibility = 'public_to' AND $1 = ANY(skills.public_to))
            OR EXISTS (
                SELECT 1 FROM users
                WHERE users.id = $1 AND users.role IN ('admin', 'super_admin')
            )
         ORDER BY skills.created_at DESC",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(skill_from_row).collect()))
}

pub(crate) async fn create_skill(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateSkillRequest>,
) -> Result<Json<SkillDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    validate_skill_payload(&req.name, &req.content)?;
    let visibility = normalize_visibility(&req.visibility)?;
    let content = req.content.trim();
    let skill_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO skills
             (id, owner_id, name, description, content, visibility, public_to, content_checksum_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(skill_id)
    .bind(user.id)
    .bind(req.name.trim())
    .bind(req.description.trim())
    .bind(content)
    .bind(visibility)
    .bind(&req.public_to)
    .bind(sha256_hex(content))
    .execute(&state.pool)
    .await?;
    Ok(Json(
        load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
    ))
}

pub(crate) async fn get_skill(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(skill_id): Path<Uuid>,
) -> Result<Json<SkillDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(
        load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
    ))
}

pub(crate) async fn update_skill(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(skill_id): Path<Uuid>,
    Json(req): Json<UpdateSkillRequest>,
) -> Result<Json<SkillDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    validate_skill_payload(&req.name, &req.content)?;
    let visibility = normalize_visibility(&req.visibility)?;
    let content = req.content.trim();
    let mut tx = state.pool.begin().await?;
    let locked = lock_skill_change_tx(&mut tx, skill_id, user.id).await?;
    let updated = sqlx::query(
        "UPDATE skills
         SET name = $1, description = $2, content = $3, visibility = $4, public_to = $5,
             revision = revision + 1, content_checksum_sha256 = $6, updated_at = now()
         WHERE id = $7 AND (owner_id = $8 OR EXISTS (
             SELECT 1 FROM users WHERE users.id = $8 AND users.role IN ('admin', 'super_admin')
         ))",
    )
    .bind(req.name.trim())
    .bind(req.description.trim())
    .bind(content)
    .bind(visibility)
    .bind(&req.public_to)
    .bind(sha256_hex(content))
    .bind(skill_id)
    .bind(user.id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(ApiError::not_found("skill not found"));
    }
    publish_skill_configuration_change_tx(&mut tx, &locked.affected_agent_ids).await?;
    tx.commit().await?;
    Ok(Json(
        load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
    ))
}

pub(crate) async fn replace_skill_package(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(skill_id): Path<Uuid>,
    multipart: Multipart,
) -> Result<Json<SkillDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    load_skill_visible_by_user(&state.pool, skill_id, user.id).await?;
    let upload = stage_skill_package_upload(multipart).await?;
    let store =
        state
            .skill_package_store
            .as_ref()
            .cloned()
            .ok_or(ApiError::service_unavailable(
                "Skill package object storage is not configured",
            ))?;
    let package_id = upload.archive_path.as_ref().map(|_| Uuid::new_v4());
    let object_key = package_id
        .map(|package_id| format!("skill-packages/{}/{skill_id}/{package_id}.tar.zst", user.id));
    if let (Some(path), Some(size_bytes), Some(checksum_sha256), Some(object_key)) = (
        upload.archive_path.as_deref(),
        upload.archive_size_bytes,
        upload.archive_checksum_sha256.as_deref(),
        object_key.as_deref(),
    ) {
        if let Err(error) = store
            .put_file(object_key, path, size_bytes, checksum_sha256)
            .await
        {
            warn!(skill_id = %skill_id, error = %error, "Skill package object upload failed");
            return Err(ApiError::bad_gateway("Skill package object upload failed"));
        }
    }

    if let Err(error) = commit_skill_package_upload(
        &state.pool,
        skill_id,
        user.id,
        package_id,
        object_key.as_deref(),
        &upload,
    )
    .await
    {
        if let Some(object_key) = object_key.as_deref() {
            if let Err(cleanup_error) =
                enqueue_skill_package_deletion(&state.pool, user.id, object_key).await
            {
                warn!(skill_id = %skill_id, error = ?cleanup_error,
                    "failed to queue an uncommitted or ambiguously committed Skill package object");
            }
        }
        return Err(error);
    }
    process_skill_package_deletion_queue(&state).await;
    Ok(Json(
        load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
    ))
}

pub(crate) async fn delete_skill_package(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(skill_id): Path<Uuid>,
) -> Result<Json<SkillDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut tx = state.pool.begin().await?;
    let locked = lock_skill_change_tx(&mut tx, skill_id, user.id).await?;
    let Some(package_id) = locked.current_package_id else {
        tx.commit().await?;
        return Ok(Json(
            load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
        ));
    };
    sqlx::query(
        "UPDATE skills
         SET current_package_id = NULL, revision = revision + 1, updated_at = now()
         WHERE id = $1 AND owner_id = $2",
    )
    .bind(skill_id)
    .bind(user.id)
    .execute(&mut *tx)
    .await?;
    if let Some(object_key) = locked.current_package_object_key.as_deref() {
        enqueue_skill_package_deletion_tx(&mut tx, user.id, object_key).await?;
    }
    sqlx::query("DELETE FROM skill_packages WHERE id = $1")
        .bind(package_id)
        .execute(&mut *tx)
        .await?;
    publish_skill_configuration_change_tx(&mut tx, &locked.affected_agent_ids).await?;
    tx.commit().await?;
    process_skill_package_deletion_queue(&state).await;
    Ok(Json(
        load_skill_visible_by_user(&state.pool, skill_id, user.id).await?,
    ))
}

pub(crate) async fn lock_skill_change_tx(
    tx: &mut Transaction<'_, Postgres>,
    skill_id: Uuid,
    owner_id: Uuid,
) -> Result<LockedSkillChange, ApiError> {
    let affected_agent_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT agent_id FROM agent_skills
         WHERE skill_id = $1 ORDER BY agent_id",
    )
    .bind(skill_id)
    .fetch_all(&mut **tx)
    .await?;
    if !affected_agent_ids.is_empty() {
        sqlx::query("SELECT id FROM agents WHERE id = ANY($1) ORDER BY id FOR UPDATE")
            .bind(&affected_agent_ids)
            .fetch_all(&mut **tx)
            .await?;
    }
    let row = sqlx::query(
        "SELECT skills.current_package_id, packages.object_key
         FROM skills
         LEFT JOIN skill_packages AS packages ON packages.id = skills.current_package_id
         WHERE skills.id = $1 AND (skills.owner_id = $2 OR EXISTS (
             SELECT 1 FROM users WHERE users.id = $2 AND users.role IN ('admin', 'super_admin')
         ))
         FOR UPDATE OF skills",
    )
    .bind(skill_id)
    .bind(owner_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ApiError::not_found("skill not found"))?;
    Ok(LockedSkillChange {
        affected_agent_ids,
        current_package_id: row.get("current_package_id"),
        current_package_object_key: row.get("object_key"),
    })
}

pub(crate) async fn publish_skill_configuration_change_tx(
    tx: &mut Transaction<'_, Postgres>,
    affected_agent_ids: &[Uuid],
) -> Result<(), ApiError> {
    if affected_agent_ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE agents
         SET execution_config_revision = execution_config_revision + 1, updated_at = now()
         WHERE id = ANY($1)",
    )
    .bind(affected_agent_ids)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE hub_sessions AS sessions
         SET configuration_refresh_revision = GREATEST(
                 sessions.configuration_refresh_revision,
                 agents.execution_config_revision
             )
         FROM agents
         WHERE sessions.agent_id = agents.id
           AND agents.id = ANY($1)
           AND sessions.runtime_owner_id IS NOT NULL
           AND sessions.lifecycle_status IN ('restoring', 'online')",
    )
    .bind(affected_agent_ids)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn enqueue_skill_package_deletion_tx(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
    object_key: &str,
) -> Result<(), ApiError> {
    sqlx::query(
        "INSERT INTO skill_package_deletion_queue (object_key, owner_id)
         VALUES ($1, $2) ON CONFLICT (object_key) DO NOTHING",
    )
    .bind(object_key)
    .bind(owner_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn enqueue_skill_package_deletion(
    pool: &PgPool,
    owner_id: Uuid,
    object_key: &str,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    enqueue_skill_package_deletion_tx(&mut tx, owner_id, object_key).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn commit_skill_package_upload(
    pool: &PgPool,
    skill_id: Uuid,
    owner_id: Uuid,
    package_id: Option<Uuid>,
    object_key: Option<&str>,
    upload: &StagedSkillPackageUpload,
) -> Result<(), ApiError> {
    let mut tx = pool.begin().await?;
    let locked = lock_skill_change_tx(&mut tx, skill_id, owner_id).await?;
    if let Some(package_id) = package_id {
        sqlx::query(
            "INSERT INTO skill_packages
                 (id, skill_id, owner_id, object_key, format_version,
                  size_bytes, checksum_sha256, files)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7)",
        )
        .bind(package_id)
        .bind(skill_id)
        .bind(owner_id)
        .bind(object_key.expect("packaged upload has an object key"))
        .bind(
            i64::try_from(upload.archive_size_bytes.expect("packaged upload has size"))
                .map_err(|_| ApiError::bad_request("Skill package archive size is too large"))?,
        )
        .bind(
            upload
                .archive_checksum_sha256
                .as_deref()
                .expect("packaged upload has checksum"),
        )
        .bind(
            serde_json::to_value(&upload.files)
                .map_err(|_| ApiError::internal("Skill package file manifest is invalid"))?,
        )
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "UPDATE skills
         SET name = $1, description = $2, content = $3,
             content_checksum_sha256 = $4, current_package_id = $5,
             revision = revision + 1, updated_at = now()
         WHERE id = $6 AND owner_id = $7",
    )
    .bind(&upload.name)
    .bind(&upload.description)
    .bind(&upload.content)
    .bind(sha256_hex(&upload.content))
    .bind(package_id)
    .bind(skill_id)
    .bind(owner_id)
    .execute(&mut *tx)
    .await?;
    if let Some(old_object_key) = locked.current_package_object_key.as_deref() {
        enqueue_skill_package_deletion_tx(&mut tx, owner_id, old_object_key).await?;
    }
    if let Some(old_package_id) = locked.current_package_id {
        sqlx::query("DELETE FROM skill_packages WHERE id = $1")
            .bind(old_package_id)
            .execute(&mut *tx)
            .await?;
    }
    publish_skill_configuration_change_tx(&mut tx, &locked.affected_agent_ids).await?;
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn stage_skill_package_upload(
    mut multipart: Multipart,
) -> Result<StagedSkillPackageUpload, ApiError> {
    let mut manifest_field = multipart
        .next_field()
        .await
        .map_err(|_| ApiError::bad_request("Skill package multipart body is invalid"))?
        .ok_or(ApiError::bad_request("Skill package manifest is required"))?;
    if manifest_field.name() != Some("manifest") {
        return Err(ApiError::bad_request(
            "Skill package manifest must be the first multipart field",
        ));
    }
    let mut manifest_bytes = Vec::new();
    while let Some(chunk) = manifest_field
        .chunk()
        .await
        .map_err(|_| ApiError::bad_request("Skill package manifest is invalid"))?
    {
        if manifest_bytes.len().saturating_add(chunk.len()) > 1024 * 1024 {
            return Err(ApiError::bad_request("Skill package manifest is too large"));
        }
        manifest_bytes.extend_from_slice(&chunk);
    }
    drop(manifest_field);
    let manifest: SkillPackageUploadManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|_| ApiError::bad_request("Skill package manifest must be valid JSON"))?;
    validate_skill_package_paths(&manifest.paths)?;

    let staging = tempfile::tempdir()
        .map_err(|_| ApiError::internal("failed to create Skill package staging directory"))?;
    let mut staged_files = Vec::with_capacity(manifest.paths.len());
    let mut expanded_size = 0_u64;
    for (index, package_path) in manifest.paths.iter().enumerate() {
        let expected_field_name = format!("file-{index}");
        let mut field = multipart
            .next_field()
            .await
            .map_err(|_| ApiError::bad_request("Skill package multipart body is invalid"))?
            .ok_or(ApiError::bad_request("Skill package file is missing"))?;
        if field.name() != Some(expected_field_name.as_str()) {
            return Err(ApiError::bad_request(
                "Skill package file fields do not match the manifest order",
            ));
        }
        let staged_path = staging.path().join(&expected_field_name);
        let mut output = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&staged_path)
            .await
            .map_err(|_| ApiError::internal("failed to stage Skill package file"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            output
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .await
                .map_err(|_| ApiError::internal("failed to protect staged Skill package file"))?;
        }
        let mut file_size = 0_u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|_| ApiError::bad_request("Skill package file body is invalid"))?
        {
            file_size = file_size
                .checked_add(chunk.len() as u64)
                .ok_or(ApiError::bad_request(
                    "Skill package expanded size is too large",
                ))?;
            expanded_size =
                expanded_size
                    .checked_add(chunk.len() as u64)
                    .ok_or(ApiError::bad_request(
                        "Skill package expanded size is too large",
                    ))?;
            if expanded_size > MAX_SKILL_PACKAGE_EXPANDED_BYTES {
                return Err(ApiError::bad_request(
                    "Skill package expanded size exceeds the limit",
                ));
            }
            hasher.update(&chunk);
            output
                .write_all(&chunk)
                .await
                .map_err(|_| ApiError::internal("failed to stage Skill package file"))?;
        }
        output
            .sync_all()
            .await
            .map_err(|_| ApiError::internal("failed to sync staged Skill package file"))?;
        staged_files.push(StagedSkillPackageFile {
            path: package_path.clone(),
            staged_path,
            size_bytes: file_size,
            checksum_sha256: format!("{:x}", hasher.finalize()),
            executable: true,
        });
    }
    if multipart
        .next_field()
        .await
        .map_err(|_| ApiError::bad_request("Skill package multipart body is invalid"))?
        .is_some()
    {
        return Err(ApiError::bad_request(
            "Skill package contains an undeclared multipart field",
        ));
    }

    let skill_markdown = staged_files
        .iter()
        .find(|file| file.path == "SKILL.md")
        .ok_or(ApiError::bad_request(
            "Skill package must contain a root SKILL.md",
        ))?;
    let markdown = tokio::fs::read(&skill_markdown.staged_path)
        .await
        .map_err(|_| ApiError::internal("failed to read staged SKILL.md"))?;
    let (name, description, content) = parse_uploaded_skill_markdown(&markdown)?;
    let extra_files = staged_files
        .iter()
        .filter(|file| file.path != "SKILL.md")
        .cloned()
        .collect::<Vec<_>>();
    let files = extra_files
        .iter()
        .map(|file| SkillPackageFileDto {
            path: file.path.clone(),
            size_bytes: file.size_bytes,
            checksum_sha256: file.checksum_sha256.clone(),
            executable: file.executable,
        })
        .collect::<Vec<_>>();
    let (archive_path, archive_size_bytes, archive_checksum_sha256) = if extra_files.is_empty() {
        (None, None, None)
    } else {
        let archive_path = staging.path().join("package.tar.zst");
        let build_path = archive_path.clone();
        let (size_bytes, checksum_sha256) = tokio::task::spawn_blocking(move || {
            build_skill_package_archive(&build_path, &extra_files)
        })
        .await
        .map_err(|_| ApiError::internal("Skill package archive task failed"))?
        .map_err(|error| {
            warn!(error = %error, "failed to build Skill package archive");
            ApiError::internal("failed to build Skill package archive")
        })?;
        if size_bytes > MAX_SKILL_PACKAGE_ARCHIVE_BYTES {
            return Err(ApiError::bad_request(
                "Skill package archive size exceeds the limit",
            ));
        }
        (Some(archive_path), Some(size_bytes), Some(checksum_sha256))
    };
    Ok(StagedSkillPackageUpload {
        _staging: staging,
        name,
        description,
        content,
        archive_path,
        archive_size_bytes,
        archive_checksum_sha256,
        files,
    })
}

pub(crate) fn validate_skill_package_paths(paths: &[String]) -> Result<(), ApiError> {
    if paths.is_empty() || paths.len() > MAX_SKILL_PACKAGE_FILES {
        return Err(ApiError::bad_request(
            "Skill package must contain 1 to 1024 files",
        ));
    }
    let mut unique = BTreeSet::new();
    for path in paths {
        let safe = !path.is_empty()
            && !path.starts_with('/')
            && !path.contains('\\')
            && !path.contains('\0')
            && path
                .split('/')
                .all(|component| !component.is_empty() && component != "." && component != "..");
        if !safe || !unique.insert(path.clone()) {
            return Err(ApiError::bad_request(
                "Skill package paths must be unique safe relative paths",
            ));
        }
    }
    if !unique.contains("SKILL.md") {
        return Err(ApiError::bad_request(
            "Skill package must contain a root SKILL.md",
        ));
    }
    let sorted = unique.into_iter().collect::<Vec<_>>();
    for (index, path) in sorted.iter().enumerate() {
        let directory_prefix = format!("{path}/");
        if sorted
            .get(index + 1)
            .is_some_and(|next| next.starts_with(&directory_prefix))
        {
            return Err(ApiError::bad_request(
                "Skill package paths must not overlap files and directories",
            ));
        }
    }
    Ok(())
}

pub(crate) fn parse_uploaded_skill_markdown(
    bytes: &[u8],
) -> Result<(String, String, String), ApiError> {
    #[derive(Deserialize)]
    struct Frontmatter {
        name: String,
        description: Option<String>,
    }

    let markdown =
        std::str::from_utf8(bytes).map_err(|_| ApiError::bad_request("SKILL.md must be UTF-8"))?;
    let markdown = markdown.strip_prefix('\u{feff}').unwrap_or(markdown);
    let mut lines = markdown.split_inclusive('\n');
    let first = lines
        .next()
        .filter(|line| line.trim() == "---")
        .ok_or(ApiError::bad_request(
            "SKILL.md must start with YAML frontmatter",
        ))?;
    let frontmatter_start = first.len();
    let mut offset = frontmatter_start;
    let mut frontmatter_end = None;
    let mut content_start = None;
    for line in lines {
        if line.trim() == "---" {
            frontmatter_end = Some(offset);
            content_start = Some(offset + line.len());
            break;
        }
        offset += line.len();
    }
    let frontmatter_end = frontmatter_end.ok_or(ApiError::bad_request(
        "SKILL.md YAML frontmatter is not closed",
    ))?;
    let parsed: Frontmatter =
        serde_yaml_ng::from_str(&markdown[frontmatter_start..frontmatter_end])
            .map_err(|_| ApiError::bad_request("SKILL.md YAML frontmatter is invalid"))?;
    let name = parsed.name.trim().to_owned();
    let description = parsed
        .description
        .unwrap_or_else(|| name.clone())
        .trim()
        .to_owned();
    let content = markdown[content_start.expect("frontmatter close has content offset")..]
        .trim()
        .to_owned();
    validate_skill_payload(&name, &content)?;
    Ok((
        name.clone(),
        if description.is_empty() {
            name
        } else {
            description
        },
        content,
    ))
}

pub(crate) fn build_skill_package_archive(
    archive_path: &FsPath,
    files: &[StagedSkillPackageFile],
) -> anyhow::Result<(u64, String)> {
    let archive = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(archive_path)
        .context("create Skill package archive")?;
    let encoder = zstd::Encoder::new(archive, 3).context("create Skill package compressor")?;
    let mut builder = tar::Builder::new(encoder);
    for file in files {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(file.size_bytes);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_mode(if file.executable { 0o755 } else { 0o444 });
        header.set_cksum();
        let input = std::fs::File::open(&file.staged_path).context("open staged Skill file")?;
        builder
            .append_data(&mut header, FsPath::new(&file.path), input)
            .context("append Skill package file")?;
    }
    let encoder = builder.into_inner().context("finish Skill package tar")?;
    let archive = encoder
        .finish()
        .context("finish Skill package compression")?;
    archive.sync_all().context("sync Skill package archive")?;
    let size_bytes = archive.metadata()?.len();
    let mut input = std::fs::File::open(archive_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((size_bytes, format!("{:x}", hasher.finalize())))
}

pub(crate) async fn delete_skill(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(skill_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let user = require_user(&state, &headers).await?;
    delete_skills_for_user(&state.pool, user.id, &[skill_id]).await?;
    process_skill_package_deletion_queue(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn bulk_delete_skills(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<BulkDeleteSkillsRequest>,
) -> Result<Json<BulkDeleteSkillsResponse>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let deleted_skill_ids = delete_skills_for_user(&state.pool, user.id, &req.skill_ids).await?;
    process_skill_package_deletion_queue(&state).await;
    Ok(Json(BulkDeleteSkillsResponse { deleted_skill_ids }))
}

pub(crate) async fn delete_skills_for_user(
    pool: &PgPool,
    user_id: Uuid,
    skill_ids: &[Uuid],
) -> Result<Vec<Uuid>, ApiError> {
    let unique_ids = skill_ids.iter().copied().collect::<BTreeSet<_>>();
    if unique_ids.is_empty() || unique_ids.len() != skill_ids.len() || unique_ids.len() > 100 {
        return Err(ApiError::bad_request(
            "skill ids must contain 1 to 100 unique values",
        ));
    }
    let ordered_skill_ids = unique_ids.into_iter().collect::<Vec<_>>();
    let mut tx = pool.begin().await?;
    let affected_agent_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT DISTINCT agent_id
         FROM agent_skills
         WHERE skill_id = ANY($1)
         ORDER BY agent_id",
    )
    .bind(&ordered_skill_ids)
    .fetch_all(&mut *tx)
    .await?;
    if !affected_agent_ids.is_empty() {
        sqlx::query(
            "SELECT id FROM agents
             WHERE id = ANY($1)
             ORDER BY id
             FOR UPDATE",
        )
        .bind(&affected_agent_ids)
        .fetch_all(&mut *tx)
        .await?;
    }
    let owned_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM skills
         WHERE id = ANY($1) AND (owner_id = $2 OR EXISTS (
             SELECT 1 FROM users WHERE users.id = $2 AND users.role IN ('admin', 'super_admin')
         ))
         ORDER BY id
         FOR UPDATE",
    )
    .bind(&ordered_skill_ids)
    .bind(user_id)
    .fetch_all(&mut *tx)
    .await?;
    if owned_ids != ordered_skill_ids {
        return Err(ApiError::not_found("skill not found"));
    }
    sqlx::query(
        "INSERT INTO skill_package_deletion_queue (object_key, owner_id)
         SELECT packages.object_key, packages.owner_id
         FROM skill_packages AS packages
         JOIN skills ON skills.current_package_id = packages.id
         WHERE skills.id = ANY($1)
         ON CONFLICT (object_key) DO NOTHING",
    )
    .bind(&ordered_skill_ids)
    .execute(&mut *tx)
    .await?;
    if !affected_agent_ids.is_empty() {
        sqlx::query(
            "UPDATE agents
             SET execution_config_revision = execution_config_revision + 1,
                 updated_at = now()
             WHERE id = ANY($1)",
        )
        .bind(&affected_agent_ids)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE hub_sessions AS sessions
                 SET configuration_refresh_revision = GREATEST(
                         sessions.configuration_refresh_revision,
                         agents.execution_config_revision
                     )
             FROM agents
             WHERE sessions.agent_id = agents.id
               AND agents.id = ANY($1)
               AND sessions.runtime_owner_id IS NOT NULL
               AND sessions.lifecycle_status IN ('restoring', 'online')",
        )
        .bind(&affected_agent_ids)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("DELETE FROM skills WHERE id = ANY($1)")
        .bind(&ordered_skill_ids)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(ordered_skill_ids)
}

pub(crate) async fn process_skill_package_deletion_queue(state: &AppState) {
    let object_keys = match sqlx::query_scalar::<_, String>(
        "SELECT queue.object_key
         FROM skill_package_deletion_queue AS queue
         WHERE NOT EXISTS (
             SELECT 1
             FROM run_skill_packages AS snapshots
             JOIN runs ON runs.id = snapshots.run_id
             WHERE snapshots.object_key = queue.object_key
               AND runs.status IN ('pending', 'running', 'waiting_tool')
         )
           AND NOT EXISTS (
             SELECT 1 FROM skill_packages
             WHERE skill_packages.object_key = queue.object_key
           )
         ORDER BY queue.created_at, queue.object_key
         LIMIT 100",
    )
    .fetch_all(&state.pool)
    .await
    {
        Ok(object_keys) => object_keys,
        Err(error) => {
            warn!(error = %error, "failed to read Skill package deletion queue");
            return;
        }
    };
    let Some(store) = state.skill_package_store.as_ref() else {
        if !object_keys.is_empty() {
            warn!("Skill package deletion queue cannot run without object storage");
        }
        return;
    };
    for object_key in object_keys {
        match store.delete(&object_key).await {
            Ok(()) => {
                if let Err(error) =
                    sqlx::query("DELETE FROM skill_package_deletion_queue WHERE object_key = $1")
                        .bind(&object_key)
                        .execute(&state.pool)
                        .await
                {
                    warn!(object_key = %object_key, error = %error,
                        "failed to acknowledge Skill package object deletion");
                }
            }
            Err(error) => {
                if let Err(database_error) = sqlx::query(
                    "UPDATE skill_package_deletion_queue
                     SET attempts = attempts + 1,
                         last_error = 'object store delete failed', updated_at = now()
                     WHERE object_key = $1",
                )
                .bind(&object_key)
                .execute(&state.pool)
                .await
                {
                    warn!(object_key = %object_key, error = %database_error,
                        "failed to record Skill package deletion failure");
                }
                warn!(object_key = %object_key, error = %error,
                    "failed to delete queued Skill package object");
            }
        }
    }
}

pub(crate) async fn skill_package_deletion_loop(state: Arc<AppState>) {
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    loop {
        tick.tick().await;
        process_skill_package_deletion_queue(&state).await;
    }
}

pub(crate) fn validate_skill_payload(name: &str, content: &str) -> Result<(), ApiError> {
    if name.trim().is_empty() {
        return Err(ApiError::bad_request("skill name is required"));
    }
    if content.trim().is_empty() {
        return Err(ApiError::bad_request("skill content is required"));
    }
    Ok(())
}

pub(crate) async fn load_skill_visible_by_user(
    pool: &PgPool,
    skill_id: Uuid,
    user_id: Uuid,
) -> Result<SkillDto, ApiError> {
    let row = sqlx::query(
        "SELECT skills.id, skills.owner_id,
                (SELECT email FROM users WHERE id = skills.owner_id) AS owner_email,
                skills.name, skills.description, skills.visibility, skills.public_to,
                skills.content, skills.revision, skills.content_checksum_sha256,
                skills.created_at, skills.updated_at,
                packages.id AS package_id, packages.format_version AS package_format_version,
                packages.size_bytes AS package_size_bytes,
                packages.checksum_sha256 AS package_checksum_sha256,
                packages.files AS package_files
         FROM skills
         LEFT JOIN skill_packages AS packages ON packages.id = skills.current_package_id
         WHERE skills.id = $1
           AND (skills.owner_id = $2 OR skills.visibility = 'public'
                OR (skills.visibility = 'public_to' AND $2 = ANY(skills.public_to))
                OR EXISTS (
                    SELECT 1 FROM users
                    WHERE users.id = $2 AND users.role IN ('admin', 'super_admin')
                ))",
    )
    .bind(skill_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    row.map(skill_from_row)
        .ok_or(ApiError::not_found("skill not found"))
}

pub(crate) async fn ensure_skills_visible_by_user(
    pool: &PgPool,
    skill_ids: &[Uuid],
    user_id: Uuid,
) -> Result<(), ApiError> {
    for skill_id in skill_ids {
        load_skill_visible_by_user(pool, *skill_id, user_id).await?;
    }
    Ok(())
}

pub(crate) async fn load_managed_skill_ids(
    pool: &PgPool,
    agent_id: Uuid,
) -> Result<Vec<Uuid>, ApiError> {
    let rows = sqlx::query(
        "SELECT s.id
         FROM agent_skills a_s
         JOIN agents a ON a.id = a_s.agent_id
         JOIN skills s ON s.id = a_s.skill_id
         LEFT JOIN users AS owner ON owner.id = a.owner_id
         WHERE a_s.agent_id = $1
           AND (s.owner_id = a.owner_id OR s.visibility = 'public'
                OR (s.visibility = 'public_to' AND a.owner_id = ANY(s.public_to))
                OR owner.role IN ('admin', 'super_admin'))
         ORDER BY s.created_at ASC",
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

