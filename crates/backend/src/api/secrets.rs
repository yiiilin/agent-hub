//! secrets 领域模块：用户密钥与密钥授权管理。

use super::*;
use crate::{load_agent_for_user, load_widget_credential_tx};
use agent_hub_shared::*;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use base64::Engine;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) fn validate_secret_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        && (name.starts_with(|byte: char| byte.is_ascii_uppercase()) || name.starts_with('_'))
}

pub(crate) fn validate_secret_declarations(
    declarations: &[AgentSecretDeclarationDto],
) -> Result<(), ApiError> {
    let mut names = BTreeSet::new();
    for declaration in declarations {
        if !validate_secret_name(&declaration.name)
            || !matches!(declaration.kind.as_str(), "value" | "file")
            || declaration.description.len() > 512
            || !names.insert(declaration.name.clone())
        {
            return Err(ApiError::bad_request(
                "Agent Secret Declarations must have unique valid names and kinds",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn load_agent_secret_declarations(
    pool: &PgPool,
    agent_id: Uuid,
) -> Result<Vec<AgentSecretDeclarationDto>, ApiError> {
    let rows = sqlx::query(
        "SELECT name, kind, description
         FROM agent_secret_declarations
         WHERE agent_id = $1
         ORDER BY name",
    )
    .bind(agent_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| AgentSecretDeclarationDto {
            name: row.get("name"),
            kind: row.get("kind"),
            description: row.get("description"),
        })
        .collect())
}

pub(crate) async fn replace_agent_secret_declarations_tx(
    tx: &mut Transaction<'_, Postgres>,
    agent_id: Uuid,
    declarations: &[AgentSecretDeclarationDto],
) -> Result<(), ApiError> {
    validate_secret_declarations(declarations)?;
    sqlx::query("DELETE FROM agent_secret_declarations WHERE agent_id = $1")
        .bind(agent_id)
        .execute(&mut **tx)
        .await?;
    for declaration in declarations {
        sqlx::query(
            "INSERT INTO agent_secret_declarations (agent_id, name, kind, description)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(agent_id)
        .bind(declaration.name.trim())
        .bind(&declaration.kind)
        .bind(declaration.description.trim())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(crate) async fn missing_secret_grants(
    pool: &PgPool,
    user_id: Uuid,
    agent_id: Uuid,
) -> Result<Vec<SecretGrantRequirementDto>, ApiError> {
    let declarations = load_agent_secret_declarations(pool, agent_id).await?;
    let mut missing = Vec::new();
    for declaration in declarations {
        let owned = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM user_secrets WHERE owner_id = $1 AND name = $2",
        )
        .bind(user_id)
        .bind(&declaration.name)
        .fetch_optional(pool)
        .await?;
        if owned.is_none() {
            continue;
        }
        let secret_kind =
            sqlx::query_scalar::<_, String>("SELECT kind FROM user_secrets WHERE id = $1")
                .bind(owned.unwrap())
                .fetch_one(pool)
                .await?;
        let granted = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM secret_grants
                WHERE user_id = $1 AND agent_id = $2 AND secret_name = $3
             )",
        )
        .bind(user_id)
        .bind(agent_id)
        .bind(&declaration.name)
        .fetch_one(pool)
        .await?;
        if !granted {
            missing.push(SecretGrantRequirementDto {
                name: declaration.name,
                kind: secret_kind,
                description: declaration.description,
            });
        }
    }
    Ok(missing)
}

pub(crate) fn user_secret_from_row(row: sqlx::postgres::PgRow) -> UserSecretDto {
    UserSecretDto {
        id: row.get("id"),
        owner_id: row.get("owner_id"),
        name: row.get("name"),
        kind: row.get("kind"),
        file_name: row.get("file_name"),
        file_size_bytes: row.get("file_size_bytes"),
        file_sha256: row.get("file_sha256"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) async fn list_user_secrets(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Vec<UserSecretDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let rows = sqlx::query(
        "SELECT id, owner_id, name, kind, file_name, file_size_bytes, file_sha256,
                created_at, updated_at
         FROM user_secrets
         WHERE owner_id = $1
         ORDER BY created_at DESC, id",
    )
    .bind(user.id)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(rows.into_iter().map(user_secret_from_row).collect()))
}

pub(crate) async fn create_user_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateUserSecretRequest>,
) -> Result<Json<UserSecretDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    if !validate_secret_name(&req.name) {
        return Err(ApiError::bad_request("Secret name is invalid"));
    }
    let id = Uuid::new_v4();
    let (
        value_ciphertext,
        value_nonce,
        file_ciphertext,
        file_nonce,
        file_name,
        file_size,
        file_sha256,
    ) = match req.kind.as_str() {
        "value" => {
            let value = req
                .value
                .ok_or(ApiError::bad_request("value is required"))?;
            if value.is_empty() || value.len() > 8192 {
                return Err(ApiError::bad_request(
                    "Secret value must be between 1 and 8192 bytes",
                ));
            }
            let encrypted = state
                .model_secret_cipher
                .encrypt(&value)
                .map_err(|_| ApiError::internal("Secret encryption failed"))?;
            (
                Some(encrypted.ciphertext),
                Some(encrypted.nonce),
                None,
                None,
                None,
                None,
                None,
            )
        }
        "file" => {
            let file_name = req
                .file_name
                .ok_or(ApiError::bad_request("file_name is required"))?
                .trim()
                .to_owned();
            if file_name.is_empty() || file_name.len() > 255 || file_name.contains('/') {
                return Err(ApiError::bad_request("file_name is invalid"));
            }
            let encoded = req
                .file_base64
                .ok_or(ApiError::bad_request("file_base64 is required"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| ApiError::bad_request("file_base64 is invalid"))?;
            if bytes.is_empty() || bytes.len() > 1024 * 1024 {
                return Err(ApiError::bad_request(
                    "Secret file must be between 1 byte and 1 MiB",
                ));
            }
            let sha = format!("{:x}", Sha256::digest(&bytes));
            let plaintext = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let encrypted = state
                .model_secret_cipher
                .encrypt(&plaintext)
                .map_err(|_| ApiError::internal("Secret file encryption failed"))?;
            (
                None,
                None,
                Some(encrypted.ciphertext),
                Some(encrypted.nonce),
                Some(file_name),
                Some(bytes.len() as i64),
                Some(sha),
            )
        }
        _ => return Err(ApiError::bad_request("kind must be value or file")),
    };
    sqlx::query(
        "INSERT INTO user_secrets
             (id, owner_id, name, kind, value_ciphertext, value_nonce,
              file_ciphertext, file_nonce, file_name, file_size_bytes, file_sha256)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(id)
    .bind(user.id)
    .bind(req.name.trim())
    .bind(&req.kind)
    .bind(value_ciphertext)
    .bind(value_nonce)
    .bind(file_ciphertext)
    .bind(file_nonce)
    .bind(file_name)
    .bind(file_size)
    .bind(file_sha256)
    .execute(&state.pool)
    .await?;
    let row = sqlx::query(
        "SELECT id, owner_id, name, kind, file_name, file_size_bytes, file_sha256,
                created_at, updated_at
         FROM user_secrets WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(user_secret_from_row(row)))
}

pub(crate) async fn update_user_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(secret_id): Path<Uuid>,
    Json(req): Json<UpdateUserSecretRequest>,
) -> Result<Json<UserSecretDto>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let row = sqlx::query("SELECT kind FROM user_secrets WHERE id = $1 AND owner_id = $2")
        .bind(secret_id)
        .bind(user.id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(ApiError::not_found("secret not found"))?;
    let kind: String = row.get("kind");
    match kind.as_str() {
        "value" => {
            let value = req
                .value
                .ok_or(ApiError::bad_request("value is required"))?;
            if value.is_empty() || value.len() > 8192 {
                return Err(ApiError::bad_request(
                    "Secret value must be between 1 and 8192 bytes",
                ));
            }
            let encrypted = state
                .model_secret_cipher
                .encrypt(&value)
                .map_err(|_| ApiError::internal("Secret encryption failed"))?;
            sqlx::query(
                "UPDATE user_secrets
                 SET value_ciphertext = $1, value_nonce = $2, updated_at = now()
                 WHERE id = $3 AND owner_id = $4",
            )
            .bind(encrypted.ciphertext)
            .bind(encrypted.nonce)
            .bind(secret_id)
            .bind(user.id)
            .execute(&state.pool)
            .await?;
        }
        "file" => {
            let encoded = req
                .file_base64
                .ok_or(ApiError::bad_request("file_base64 is required"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| ApiError::bad_request("file_base64 is invalid"))?;
            if bytes.is_empty() || bytes.len() > 1024 * 1024 {
                return Err(ApiError::bad_request(
                    "Secret file must be between 1 byte and 1 MiB",
                ));
            }
            let file_name = req
                .file_name
                .ok_or(ApiError::bad_request("file_name is required"))?
                .trim()
                .to_owned();
            if file_name.is_empty() || file_name.len() > 255 || file_name.contains('/') {
                return Err(ApiError::bad_request("file_name is invalid"));
            }
            let sha = format!("{:x}", Sha256::digest(&bytes));
            let plaintext = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let encrypted = state
                .model_secret_cipher
                .encrypt(&plaintext)
                .map_err(|_| ApiError::internal("Secret file encryption failed"))?;
            sqlx::query(
                "UPDATE user_secrets
                 SET file_ciphertext = $1, file_nonce = $2, file_name = $3,
                     file_size_bytes = $4, file_sha256 = $5, updated_at = now()
                 WHERE id = $6 AND owner_id = $7",
            )
            .bind(encrypted.ciphertext)
            .bind(encrypted.nonce)
            .bind(file_name)
            .bind(bytes.len() as i64)
            .bind(sha)
            .bind(secret_id)
            .bind(user.id)
            .execute(&state.pool)
            .await?;
        }
        _ => return Err(ApiError::internal("secret kind is invalid")),
    }
    let row = sqlx::query(
        "SELECT id, owner_id, name, kind, file_name, file_size_bytes, file_sha256,
                created_at, updated_at
         FROM user_secrets WHERE id = $1",
    )
    .bind(secret_id)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(user_secret_from_row(row)))
}

pub(crate) async fn delete_user_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(secret_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let user = require_user(&state, &headers).await?;
    let name = sqlx::query_scalar::<_, String>(
        "SELECT name FROM user_secrets WHERE id = $1 AND owner_id = $2",
    )
    .bind(secret_id)
    .bind(user.id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(ApiError::not_found("secret not found"))?;
    let mut tx = state.pool.begin().await?;
    sqlx::query("DELETE FROM user_secrets WHERE id = $1 AND owner_id = $2")
        .bind(secret_id)
        .bind(user.id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM secret_grants WHERE user_id = $1 AND secret_name = $2")
        .bind(user.id)
        .bind(&name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub(crate) struct SecretGrantListQuery {
    agent_id: Option<Uuid>,
}

pub(crate) async fn list_secret_grants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SecretGrantListQuery>,
) -> Result<Json<Vec<SecretGrantDto>>, ApiError> {
    let user = require_user(&state, &headers).await?;
    let mut builder = sqlx::QueryBuilder::<Postgres>::new(
        "SELECT user_id, agent_id, secret_name, granted_at FROM secret_grants WHERE user_id = ",
    );
    builder.push_bind(user.id);
    if let Some(agent_id) = query.agent_id {
        builder.push(" AND agent_id = ");
        builder.push_bind(agent_id);
    }
    builder.push(" ORDER BY granted_at DESC, secret_name");
    let rows = builder.build().fetch_all(&state.pool).await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| SecretGrantDto {
                user_id: row.get("user_id"),
                agent_id: row.get("agent_id"),
                secret_name: row.get("secret_name"),
                granted_at: row.get("granted_at"),
            })
            .collect(),
    ))
}

pub(crate) async fn create_secret_grants(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateSecretGrantRequest>,
) -> Result<Json<Vec<SecretGrantDto>>, ApiError> {
    let (user, client_agent_id) = require_secret_grant_user(&state, &headers).await?;
    if let Some(allowed_agent_id) = client_agent_id {
        if allowed_agent_id != req.agent_id {
            return Err(ApiError::forbidden(
                "the Widget credential may only grant secrets for its Agent",
            ));
        }
    }
    load_agent_for_user(&state.pool, req.agent_id, &user).await?;
    if req.secret_names.is_empty() || req.secret_names.len() > 128 {
        return Err(ApiError::bad_request(
            "secret_names must contain between 1 and 128 entries",
        ));
    }
    let mut tx = state.pool.begin().await?;
    for name in &req.secret_names {
        if !validate_secret_name(name) {
            return Err(ApiError::bad_request("secret name is invalid"));
        }
        let declared = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM agent_secret_declarations
                WHERE agent_id = $1 AND name = $2
             )",
        )
        .bind(req.agent_id)
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        let owned = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM user_secrets WHERE owner_id = $1 AND name = $2
             )",
        )
        .bind(user.id)
        .bind(name)
        .fetch_one(&mut *tx)
        .await?;
        if !declared || !owned {
            return Err(ApiError::bad_request(format!(
                "secret {name} is not declared by the Agent or not owned by the user"
            )));
        }
        sqlx::query(
            "INSERT INTO secret_grants (user_id, agent_id, secret_name)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
        )
        .bind(user.id)
        .bind(req.agent_id)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    let rows = sqlx::query(
        "SELECT user_id, agent_id, secret_name, granted_at
         FROM secret_grants
         WHERE user_id = $1 AND agent_id = $2
           AND secret_name = ANY($3)
         ORDER BY granted_at DESC, secret_name",
    )
    .bind(user.id)
    .bind(req.agent_id)
    .bind(&req.secret_names)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|row| SecretGrantDto {
                user_id: row.get("user_id"),
                agent_id: row.get("agent_id"),
                secret_name: row.get("secret_name"),
                granted_at: row.get("granted_at"),
            })
            .collect(),
    ))
}

pub(crate) async fn require_secret_grant_user(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(UserDto, Option<Uuid>), ApiError> {
    if let Some(token) = client_access_token_from_headers(headers) {
        let mut tx = state.pool.begin().await?;
        let credential = load_widget_credential_tx(&mut tx, &token, headers).await?;
        if credential.is_anonymous() {
            return Err(ApiError::forbidden(
                "anonymous Widgets cannot grant secrets",
            ));
        }
        let row = sqlx::query("SELECT id, email, display_name, role FROM users WHERE id = $1")
            .bind(credential.owner_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(ApiError::unauthorized("Widget credential user not found"))?;
        tx.commit().await?;
        return Ok((user_from_row(row), Some(credential.agent_id)));
    }
    Ok((require_user(state, headers).await?, None))
}

pub(crate) async fn delete_secret_grant(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((agent_id, secret_name)): Path<(Uuid, String)>,
) -> Result<StatusCode, ApiError> {
    let user = require_user(&state, &headers).await?;
    if !validate_secret_name(&secret_name) {
        return Err(ApiError::bad_request("secret name is invalid"));
    }
    sqlx::query(
        "DELETE FROM secret_grants
         WHERE user_id = $1 AND agent_id = $2 AND secret_name = $3",
    )
    .bind(user.id)
    .bind(agent_id)
    .bind(&secret_name)
    .execute(&state.pool)
    .await?;
    Ok(StatusCode::NO_CONTENT)
}
