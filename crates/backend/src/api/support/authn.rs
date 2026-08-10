//! 认证凭据提取（session/bearer/embed/client/webhook token）。

use agent_hub_shared::*;
use axum::http::{HeaderMap, HeaderValue, header};
use super::error::ApiError;

pub(crate) fn session_token_from_headers(headers: &HeaderMap) -> Option<String> {
    let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
    cookie.split(';').map(str::trim).find_map(|part| {
        part.strip_prefix("agent_hub_session=")
            .map(ToOwned::to_owned)
    })
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(ToOwned::to_owned)
}

pub(crate) fn embed_token_from_headers(headers: &HeaderMap) -> Option<String> {
    scoped_token_from_headers(headers, "x-agent-hub-embed-token", "Embed ")
}

pub(crate) fn client_access_token_from_headers(headers: &HeaderMap) -> Option<String> {
    embed_token_from_headers(headers).or_else(|| {
        bearer_token(headers).filter(|token| {
            token.starts_with("ahe_") || token.starts_with("ahw_") || token.starts_with("ahp_")
        })
    })
}

pub(crate) fn webhook_token_from_headers(headers: &HeaderMap) -> Option<String> {
    scoped_token_from_headers(headers, "x-agent-hub-webhook-token", "Webhook ")
}

pub(crate) fn scoped_token_from_headers(
    headers: &HeaderMap,
    header_name: &'static str,
    authorization_prefix: &str,
) -> Option<String> {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix(authorization_prefix))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

pub(crate) fn cookie_header(token: &str, secure: bool) -> Result<HeaderValue, ApiError> {
    // V1 使用 HttpOnly cookie，前端通过 credentials 发送，不把 session 暴露给 JS。
    let secure_attr = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "agent_hub_session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800{secure_attr}"
    ))
    .map_err(|_| ApiError::internal("failed to build session cookie"))
}

