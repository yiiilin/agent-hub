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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::support::test_util::*;
    use crate::skill_package_store::SkillPackageStore;
    use axum::body::Body;
    use axum::http::header;

    #[test]
    fn skill_package_paths_require_one_root_manifest_and_reject_ambiguous_paths() {
        assert!(validate_skill_package_paths(&[
            "SKILL.md".into(),
            "bin/client".into(),
            "references/guide.md".into(),
        ])
        .is_ok());
        for paths in [
            vec!["bin/client".into()],
            vec!["SKILL.md".into(), "../secret".into()],
            vec!["SKILL.md".into(), "/absolute".into()],
            vec!["SKILL.md".into(), "bin\\client".into()],
            vec!["SKILL.md".into(), "bin/client".into(), "bin/client".into()],
            vec![
                "SKILL.md".into(),
                "resources".into(),
                "resources/file".into(),
            ],
        ] {
            assert!(validate_skill_package_paths(&paths).is_err(), "{paths:?}");
        }
    }

    #[tokio::test]
    async fn skill_package_multipart_protocol_stages_manifest_files_in_declared_order() {
        use axum::extract::FromRequest;

        let boundary = "agent-hub-skill-package-test";
        let mut body = Vec::new();
        for (name, filename, content_type, contents) in [
            (
                "manifest",
                None,
                "application/json",
                br#"{"paths":["SKILL.md","bin/client","references/guide.md"]}"#.as_slice(),
            ),
            (
                "file-0",
                Some("SKILL.md"),
                "text/markdown",
                b"---\nname: deploy\ndescription: Deploy client\n---\n\nUse bin/client.\n"
                    .as_slice(),
            ),
            (
                "file-1",
                Some("client"),
                "application/octet-stream",
                b"client-bytes".as_slice(),
            ),
            (
                "file-2",
                Some("guide.md"),
                "text/markdown",
                b"guide".as_slice(),
            ),
        ] {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"{name}\"{}\r\n",
                    filename
                        .map_or_else(String::new, |filename| format!("; filename=\"{filename}\""))
                )
                .as_bytes(),
            );
            body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
            body.extend_from_slice(contents);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        let request = axum::http::Request::builder()
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap();
        let multipart = Multipart::from_request(request, &()).await.unwrap();
        let upload = stage_skill_package_upload(multipart).await.unwrap();

        assert_eq!(upload.name, "deploy");
        assert_eq!(upload.description, "Deploy client");
        assert_eq!(upload.content, "Use bin/client.");
        assert_eq!(
            upload
                .files
                .iter()
                .map(|file| (file.path.as_str(), file.executable))
                .collect::<Vec<_>>(),
            [("bin/client", true), ("references/guide.md", true)]
        );
        assert!(upload
            .archive_path
            .as_ref()
            .is_some_and(|path| path.is_file()));
        assert!(upload.archive_size_bytes.is_some_and(|size| size > 0));
        assert!(upload.archive_checksum_sha256.is_some());
    }

    #[test]
    fn skill_package_archive_contains_only_declared_regular_files_with_fixed_modes() {
        let root = tempfile::tempdir().unwrap();
        let binary = root.path().join("binary");
        let reference = root.path().join("reference");
        std::fs::write(&binary, b"binary-bytes").unwrap();
        std::fs::write(&reference, b"reference-bytes").unwrap();
        let files = vec![
            StagedSkillPackageFile {
                path: "bin/client".into(),
                staged_path: binary,
                size_bytes: 12,
                checksum_sha256: format!("{:x}", Sha256::digest(b"binary-bytes")),
                executable: true,
            },
            StagedSkillPackageFile {
                path: "references/guide.md".into(),
                staged_path: reference,
                size_bytes: 15,
                checksum_sha256: format!("{:x}", Sha256::digest(b"reference-bytes")),
                executable: true,
            },
        ];
        let archive_path = root.path().join("package.tar.zst");
        let (size, checksum) = build_skill_package_archive(&archive_path, &files).unwrap();
        assert_eq!(size, std::fs::metadata(&archive_path).unwrap().len());
        assert_eq!(
            checksum,
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(&archive_path).unwrap())
            )
        );

        let decoder = zstd::Decoder::new(std::fs::File::open(archive_path).unwrap()).unwrap();
        let mut archive = tar::Archive::new(decoder);
        let entries = archive
            .entries()
            .unwrap()
            .map(|entry| {
                let mut entry = entry.unwrap();
                assert!(entry.header().entry_type().is_file());
                let path = entry.path().unwrap().to_string_lossy().into_owned();
                let mode = entry.header().mode().unwrap();
                let mut contents = Vec::new();
                entry.read_to_end(&mut contents).unwrap();
                (path, mode, contents)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            entries[0],
            ("bin/client".into(), 0o755, b"binary-bytes".to_vec())
        );
        assert_eq!(
            entries[1],
            (
                "references/guide.md".into(),
                0o755,
                b"reference-bytes".to_vec()
            )
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_create_and_update_publish_atomic_revision_and_checksum(pool: PgPool) {
        let token = create_user_session_with_role(&pool, "member").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool));
        let created = create_skill(
            State(state.clone()),
            session_headers(&token),
            Json(CreateSkillRequest {
                name: "review".into(),
                description: "Review changes".into(),
                content: "check the diff".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        let created_json = serde_json::to_value(&created).unwrap();
        assert_eq!(created_json["revision"], 1);
        assert_eq!(
            created_json["content_checksum_sha256"],
            sha256_hex("check the diff")
        );

        let updated = update_skill(
            State(state.clone()),
            session_headers(&token),
            Path(created.id),
            Json(UpdateSkillRequest {
                name: "review".into(),
                description: "Review carefully".into(),
                content: "check tests too".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        let updated_json = serde_json::to_value(&updated).unwrap();
        assert_eq!(updated_json["revision"], 2);
        assert_eq!(
            updated_json["content_checksum_sha256"],
            sha256_hex("check tests too")
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, String)>(
                "SELECT revision, content_checksum_sha256 FROM skills WHERE id = $1"
            )
            .bind(created.id)
            .fetch_one(&state.pool)
            .await
            .unwrap(),
            (2, sha256_hex("check tests too"))
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_visibility_controls_sharing_and_attachment(pool: PgPool) {
        let owner_token = create_user_session_with_role(&pool, "member").await;
        let caller_token = create_user_session_with_role(&pool, "member").await;
        let other_token = create_user_session_with_role(&pool, "member").await;
        let admin_token = create_user_session_with_role(&pool, "admin").await;
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let caller = require_user(&state, &session_headers(&caller_token))
            .await
            .unwrap();

        // member 可以创建 public 技能（不要求管理员权限）。
        let shared = create_skill(
            State(state.clone()),
            session_headers(&owner_token),
            Json(CreateSkillRequest {
                name: "shared skill".into(),
                description: "shared".into(),
                content: "shared content".into(),
                visibility: "public".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(shared.visibility, "public");

        // 其他用户可见（列表与单查）。
        let listed = list_skills(State(state.clone()), session_headers(&caller_token))
            .await
            .unwrap()
            .0;
        assert!(listed.iter().any(|skill| skill.id == shared.id));
        let fetched = get_skill(
            State(state.clone()),
            session_headers(&caller_token),
            Path(shared.id),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(fetched.id, shared.id);

        // 其他用户可把共享技能挂载到自己的 Agent。
        let agent = create_agent(
            State(state.clone()),
            session_headers(&caller_token),
            Json(CreateAgentRequest {
                name: "Caller Agent".into(),
                instructions: "run".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
                model_selection: None,
                model_settings: Some(AgentModelSettings::default()),
                subagents: Vec::new(),
                secret_declarations: Some(Vec::new()),
                tool_allowlist: default_agent_tool_allowlist(),
            }),
        )
        .await
        .unwrap()
        .0;
        let attached = update_agent(
            State(state.clone()),
            session_headers(&caller_token),
            Path(agent.id),
            Json(UpdateAgentRequest {
                name: agent.name.clone(),
                instructions: agent.instructions.clone(),
                visibility: agent.visibility.clone(),
                public_to: Vec::new(),
                runtime_id: None,
                model_selection: agent.model_selection.clone(),
                model_settings: agent.model_settings.clone(),
                subagents: agent.subagents.clone(),
                model_policy: agent.model_policy.clone(),
                sandbox_policy: agent.sandbox_policy.clone(),
                managed_skill_ids: vec![shared.id],
                secret_declarations: Some(agent.secret_declarations.clone()),
                mcp_allowlist: agent.mcp_allowlist.clone(),
                tool_allowlist: agent.tool_allowlist.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(attached.managed_skill_ids, vec![shared.id]);

        // 非 owner 只有可读权限：不能编辑或删除。
        assert!(update_skill(
            State(state.clone()),
            session_headers(&caller_token),
            Path(shared.id),
            Json(UpdateSkillRequest {
                name: "hijack".into(),
                description: "x".into(),
                content: "y".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .is_err());
        assert!(delete_skill(
            State(state.clone()),
            session_headers(&caller_token),
            Path(shared.id),
        )
        .await
        .is_err());

        // admin 拥有可写权限：可以编辑和删除。
        assert!(update_skill(
            State(state.clone()),
            session_headers(&admin_token),
            Path(shared.id),
            Json(UpdateSkillRequest {
                name: "admin edit".into(),
                description: "shared".into(),
                content: "shared content".into(),
                visibility: "public".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .is_ok());
        assert!(delete_skill(
            State(state.clone()),
            session_headers(&admin_token),
            Path(shared.id),
        )
        .await
        .is_ok());

        // private 技能对其他用户不可见、不可挂载。
        let private_skill = create_skill(
            State(state.clone()),
            session_headers(&owner_token),
            Json(CreateSkillRequest {
                name: "private skill".into(),
                description: "mine".into(),
                content: "secret".into(),
                visibility: "private".into(),
                public_to: Vec::new(),
            }),
        )
        .await
        .unwrap()
        .0;
        let listed_after = list_skills(State(state.clone()), session_headers(&caller_token))
            .await
            .unwrap()
            .0;
        assert!(!listed_after
            .iter()
            .any(|skill| skill.id == private_skill.id));
        assert!(get_skill(
            State(state.clone()),
            session_headers(&caller_token),
            Path(private_skill.id),
        )
        .await
        .is_err());
        assert!(update_agent(
            State(state.clone()),
            session_headers(&caller_token),
            Path(agent.id),
            Json(UpdateAgentRequest {
                name: agent.name.clone(),
                instructions: agent.instructions.clone(),
                visibility: agent.visibility.clone(),
                public_to: Vec::new(),
                runtime_id: None,
                model_selection: agent.model_selection.clone(),
                model_settings: agent.model_settings.clone(),
                subagents: agent.subagents.clone(),
                model_policy: agent.model_policy.clone(),
                sandbox_policy: agent.sandbox_policy.clone(),
                managed_skill_ids: vec![private_skill.id],
                secret_declarations: Some(agent.secret_declarations.clone()),
                mcp_allowlist: agent.mcp_allowlist.clone(),
                tool_allowlist: agent.tool_allowlist.clone(),
            }),
        )
        .await
        .is_err());

        // public_to 技能：指定用户可见，其他用户不可见。
        let targeted = create_skill(
            State(state.clone()),
            session_headers(&owner_token),
            Json(CreateSkillRequest {
                name: "targeted skill".into(),
                description: "targeted".into(),
                content: "targeted content".into(),
                visibility: "public_to".into(),
                public_to: vec![caller.id],
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(get_skill(
            State(state.clone()),
            session_headers(&caller_token),
            Path(targeted.id),
        )
        .await
        .is_ok());
        let listed_other = list_skills(State(state.clone()), session_headers(&other_token))
            .await
            .unwrap()
            .0;
        assert!(!listed_other.iter().any(|skill| skill.id == targeted.id));
        assert!(get_skill(
            State(state.clone()),
            session_headers(&other_token),
            Path(targeted.id),
        )
        .await
        .is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_package_replacement_is_atomic_and_publishes_configuration_refresh(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let skill_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Initial Skill', 'initial', 'initial content', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("initial content"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;

        let first_package_id = Uuid::new_v4();
        let first_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{first_package_id}.tar.zst");
        let first = staged_skill_package_upload("Packaged Skill", "first body", "archive-one");
        commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(first_package_id),
            Some(&first_object_key),
            &first,
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (i64, Option<Uuid>, String, String)>(
                "SELECT revision, current_package_id, name, content
                 FROM skills WHERE id = $1",
            )
            .bind(skill_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (
                2,
                Some(first_package_id),
                "Packaged Skill".into(),
                "first body".into()
            )
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT agents.execution_config_revision,
                        sessions.configuration_refresh_revision
                 FROM agents
                 JOIN hub_sessions AS sessions ON sessions.agent_id = agents.id
                 WHERE agents.id = $1 AND sessions.id = $2",
            )
            .bind(fixture.agent_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (2, 2)
        );

        let failed_package_id = Uuid::new_v4();
        let failed = staged_skill_package_upload("Must Roll Back", "failed body", "archive-fail");
        assert!(commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(failed_package_id),
            Some(&first_object_key),
            &failed,
        )
        .await
        .is_err());
        assert_eq!(
            sqlx::query_as::<_, (i64, Option<Uuid>, i64, i64)>(
                "SELECT skills.revision, skills.current_package_id,
                        agents.execution_config_revision,
                        sessions.configuration_refresh_revision
                 FROM skills
                 JOIN agent_skills ON agent_skills.skill_id = skills.id
                 JOIN agents ON agents.id = agent_skills.agent_id
                 JOIN hub_sessions AS sessions ON sessions.agent_id = agents.id
                 WHERE skills.id = $1 AND sessions.id = $2",
            )
            .bind(skill_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (2, Some(first_package_id), 2, 2)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skill_packages WHERE id = $1",)
                .bind(failed_package_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skill_package_deletion_queue")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );

        let second_package_id = Uuid::new_v4();
        let second_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{second_package_id}.tar.zst");
        let second = staged_skill_package_upload("Packaged Skill", "second body", "archive-two");
        commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(second_package_id),
            Some(&second_object_key),
            &second,
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (i64, Option<Uuid>, i64, i64)>(
                "SELECT skills.revision, skills.current_package_id,
                        agents.execution_config_revision,
                        sessions.configuration_refresh_revision
                 FROM skills
                 JOIN agent_skills ON agent_skills.skill_id = skills.id
                 JOIN agents ON agents.id = agent_skills.agent_id
                 JOIN hub_sessions AS sessions ON sessions.agent_id = agents.id
                 WHERE skills.id = $1 AND sessions.id = $2",
            )
            .bind(skill_id)
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (3, Some(second_package_id), 3, 3)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skill_packages WHERE id = $1",)
                .bind(first_package_id)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT object_key FROM skill_package_deletion_queue",)
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            first_object_key
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_package_deletion_queue_preserves_current_and_active_run_objects(pool: PgPool) {
        let fixture = runtime_claim_fixture(pool, "workspace-write", "workspace-write").await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let skill_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'Queued Skill', 'queued', 'queued content', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("queued content"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();

        let store_root = tempfile::tempdir().unwrap();
        let store = Arc::new(SkillPackageStore::local(store_root.path().to_path_buf()).unwrap());
        let current_package_id = Uuid::new_v4();
        let current_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{current_package_id}.tar.zst");
        let current = staged_skill_package_upload("Queued Skill", "current", "current-object");
        store
            .put_file(
                &current_object_key,
                current.archive_path.as_deref().unwrap(),
                current.archive_size_bytes.unwrap(),
                current.archive_checksum_sha256.as_deref().unwrap(),
            )
            .await
            .unwrap();
        commit_skill_package_upload(
            &fixture.state.pool,
            skill_id,
            owner_id,
            Some(current_package_id),
            Some(&current_object_key),
            &current,
        )
        .await
        .unwrap();

        let _ = claim_runtime_run(&fixture.state, &fixture.runtime_token).await;
        let snapshotted_package_id = Uuid::new_v4();
        let snapshotted_object_key =
            format!("skill-packages/{owner_id}/{skill_id}/{snapshotted_package_id}.tar.zst");
        let snapshotted =
            staged_skill_package_upload("Queued Skill", "snapshot", "snapshot-object");
        store
            .put_file(
                &snapshotted_object_key,
                snapshotted.archive_path.as_deref().unwrap(),
                snapshotted.archive_size_bytes.unwrap(),
                snapshotted.archive_checksum_sha256.as_deref().unwrap(),
            )
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO run_skill_packages
                 (run_id, skill_id, package_id, object_key, format_version,
                  size_bytes, checksum_sha256, files)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7)",
        )
        .bind(fixture.run_id)
        .bind(skill_id)
        .bind(snapshotted_package_id)
        .bind(&snapshotted_object_key)
        .bind(snapshotted.archive_size_bytes.unwrap() as i64)
        .bind(snapshotted.archive_checksum_sha256.as_deref().unwrap())
        .bind(serde_json::to_value(&snapshotted.files).unwrap())
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        enqueue_skill_package_deletion(&fixture.state.pool, owner_id, &current_object_key)
            .await
            .unwrap();
        enqueue_skill_package_deletion(&fixture.state.pool, owner_id, &snapshotted_object_key)
            .await
            .unwrap();

        let mut deletion_state = fixture.state.as_ref().clone();
        deletion_state.skill_package_store = Some(store.clone());
        process_skill_package_deletion_queue(&deletion_state).await;
        assert!(store.get(&current_object_key).await.is_ok());
        assert!(store.get(&snapshotted_object_key).await.is_ok());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skill_package_deletion_queue")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            2
        );

        sqlx::query("DELETE FROM skill_packages WHERE id = $1")
            .bind(current_package_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE runs SET status = 'pending' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        process_skill_package_deletion_queue(&deletion_state).await;
        assert!(store.get(&current_object_key).await.is_err());
        assert!(store.get(&snapshotted_object_key).await.is_ok());
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT object_key FROM skill_package_deletion_queue")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            snapshotted_object_key
        );

        sqlx::query("UPDATE runs SET status = 'completed' WHERE id = $1")
            .bind(fixture.run_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        process_skill_package_deletion_queue(&deletion_state).await;
        assert!(store.get(&snapshotted_object_key).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skill_package_deletion_queue")
                .fetch_one(&fixture.state.pool)
                .await
                .unwrap(),
            0
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_delete_targets_agent_revision_and_stale_refresh_completion_cannot_clear_it(
        pool: PgPool,
    ) {
        let fixture = integration_runtime_fixture(pool).await;
        let owner_id: Uuid = sqlx::query_scalar("SELECT owner_id FROM agents WHERE id = $1")
            .bind(fixture.agent_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let skill_id = Uuid::new_v4();
        sqlx::query("UPDATE agents SET execution_config_revision = 7 WHERE id = $1")
            .bind(fixture.agent_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE hub_sessions
             SET configuration_refresh_revision = 3,
                 configuration_applied_revision = 3
             WHERE id = $1",
        )
        .bind(fixture.hub_session_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO skills
                 (id, owner_id, name, description, content, content_checksum_sha256)
             VALUES ($1, $2, 'refresh-skill', '', 'old', $3)",
        )
        .bind(skill_id)
        .bind(owner_id)
        .bind(sha256_hex("old"))
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO agent_skills (agent_id, skill_id) VALUES ($1, $2)")
            .bind(fixture.agent_id)
            .bind(skill_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        delete_skills_for_user(&fixture.state.pool, owner_id, &[skill_id])
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT configuration_refresh_revision, configuration_applied_revision
                 FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            (8, 3)
        );

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
        let stale = heartbeat
            .session_commands
            .into_iter()
            .find(|command| command.command == "refresh_configuration")
            .unwrap();
        assert_eq!(stale.configuration_revision, Some(8));
        assert_eq!(
            stale.configuration_revision,
            stale
                .execution_configuration
                .as_ref()
                .map(|configuration| configuration.revision)
        );
        let stale_fingerprint = stale.fingerprint.clone().unwrap();

        sqlx::query(
            "UPDATE agents
             SET instructions = 'newer target', execution_config_revision = 9
             WHERE id = $1",
        )
        .bind(fixture.agent_id)
        .execute(&fixture.state.pool)
        .await
        .unwrap();
        sqlx::query("UPDATE hub_sessions SET configuration_refresh_revision = 9 WHERE id = $1")
            .bind(fixture.hub_session_id)
            .execute(&fixture.state.pool)
            .await
            .unwrap();

        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, stale.command_id)),
            Json(RuntimeSessionWriteRequest {
                ownership_generation: 1,
                payload: CompleteRuntimeSessionCommandRequest {
                    command: "refresh_configuration".into(),
                    outcome: "applied".into(),
                    revision: Some(8),
                    fingerprint: Some(stale_fingerprint),
                },
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT configuration_applied_revision FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            3
        );

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
        let current = heartbeat
            .session_commands
            .into_iter()
            .find(|command| command.command == "refresh_configuration")
            .unwrap();
        assert_eq!(current.configuration_revision, Some(9));
        assert_eq!(
            current.configuration_revision,
            current
                .execution_configuration
                .as_ref()
                .map(|configuration| configuration.revision)
        );
        let _ = runtime_complete_session_command(
            State(fixture.state.clone()),
            bearer_headers(&fixture.runtime_token),
            Path((fixture.hub_session_id, current.command_id)),
            Json(RuntimeSessionWriteRequest {
                ownership_generation: 1,
                payload: CompleteRuntimeSessionCommandRequest {
                    command: "refresh_configuration".into(),
                    outcome: "applied".into(),
                    revision: Some(9),
                    fingerprint: current.fingerprint,
                },
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT configuration_applied_revision FROM hub_sessions WHERE id = $1",
            )
            .bind(fixture.hub_session_id)
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap(),
            9
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    #[ignore = "requires DATABASE_URL and PostgreSQL CREATE DATABASE privilege"]
    async fn skill_bulk_delete_is_owner_scoped_and_all_or_nothing(pool: PgPool) {
        let owner = create_hub_user(
            &pool,
            Some("skills-owner@example.com"),
            None,
            Some("x"),
            true,
        )
        .await
        .unwrap();
        let other = create_hub_user(
            &pool,
            Some("skills-other@example.com"),
            None,
            Some("x"),
            true,
        )
        .await
        .unwrap();
        let token = "skills-owner-session";
        sqlx::query(
            "INSERT INTO sessions (token_hash, user_id, expires_at)
             VALUES ($1, $2, now() + interval '1 hour')",
        )
        .bind(sha256_hex(token))
        .bind(owner.id)
        .execute(&pool)
        .await
        .unwrap();
        let owned = [Uuid::new_v4(), Uuid::new_v4()];
        let foreign = Uuid::new_v4();
        for (id, user_id, name) in [
            (owned[0], owner.id, "one"),
            (owned[1], owner.id, "two"),
            (foreign, other.id, "foreign"),
        ] {
            sqlx::query(
                "INSERT INTO skills
                     (id, owner_id, name, description, content, content_checksum_sha256)
                 VALUES ($1, $2, $3, '', 'content', $4)",
            )
            .bind(id)
            .bind(user_id)
            .bind(name)
            .bind(sha256_hex("content"))
            .execute(&pool)
            .await
            .unwrap();
        }
        let state = Arc::new(test_state_with_browser_session_auth(pool.clone()));
        let error = bulk_delete_skills(
            State(state.clone()),
            session_headers(token),
            Json(BulkDeleteSkillsRequest {
                skill_ids: vec![owned[0], foreign],
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM skills WHERE id = ANY($1)")
                .bind(owned)
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );

        let deleted = bulk_delete_skills(
            State(state),
            session_headers(token),
            Json(BulkDeleteSkillsRequest {
                skill_ids: owned.to_vec(),
            }),
        )
        .await
        .unwrap()
        .0;
        let mut expected = owned.to_vec();
        expected.sort_unstable();
        assert_eq!(deleted.deleted_skill_ids, expected);
    }
}
