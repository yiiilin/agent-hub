//! 应用状态与认证基础设施类型。

use super::error::ApiError;
use crate::run_event_bus;
use crate::run_event_bus::RunEventBus;
use crate::session_bundle_store::{S3BundleStore, S3BundleStoreConfig};
use crate::skill_package_store::SkillPackageStore;
use agent_hub_backend::ModelSecretCipher;
use agent_hub_shared::*;
use async_trait::async_trait;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use ipnet::IpNet;
use sqlx::PgPool;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;
use zeroize::Zeroizing;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) pool: PgPool,
    pub(crate) session_cookie_secure: bool,
    pub(crate) embed_jwt_secret: String,
    pub(crate) embed_jwt_issuer: String,
    pub(crate) embed_jwt_audience: String,
    pub(crate) trusted_proxy_cidrs: Option<Vec<IpNet>>,
    pub(crate) model_secret_cipher: ModelSecretCipher,
    pub(crate) model_proxy_http: reqwest::Client,
    pub(crate) model_gateway_url: String,
    pub(crate) model_gateway_auth_token: Arc<Zeroizing<String>>,
    pub(crate) session_bundle_store: Option<Arc<S3BundleStore>>,
    pub(crate) skill_package_store: Option<Arc<SkillPackageStore>>,
    pub(crate) session_bundle_max_bytes: u64,
    pub(crate) auth_providers: Vec<Arc<dyn AuthProvider>>,
    pub(crate) session_issuer: Arc<dyn SessionIssuer>,
    pub(crate) run_event_bus: Arc<dyn run_event_bus::RunEventBus + Send + Sync>,
}

pub(crate) struct MaybeConnectInfo(pub(crate) Option<SocketAddr>);

impl<S> FromRequestParts<S> for MaybeConnectInfo
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(
            parts
                .extensions
                .get::<ConnectInfo<SocketAddr>>()
                .map(|connect_info| connect_info.0),
        ))
    }
}

pub(crate) enum AuthCredential {
    Headers(HeaderMap),
    Password { email: String, password: String },
    EmbedJwt(String),
}

pub(crate) enum AuthPrincipal {
    User {
        user: UserDto,
        _provider: &'static str,
        api_key_id: Option<Uuid>,
    },
    Embed {
        owner_id: Uuid,
        agent_id: Uuid,
        _subject: String,
    },
}

#[async_trait]
pub(crate) trait AuthProvider: Send + Sync {
    async fn authenticate(
        &self,
        state: &AppState,
        credential: &AuthCredential,
    ) -> Result<Option<AuthPrincipal>, ApiError>;
}

#[async_trait]
pub(crate) trait SessionIssuer: Send + Sync {
    async fn issue(&self, state: &AppState, user_id: Uuid) -> Result<HeaderMap, ApiError>;
}

pub(crate) struct BrowserSessionIssuer;
pub(crate) struct PasswordAuthProvider;
pub(crate) struct BrowserSessionAuthProvider;

pub(crate) struct ApiKeyAuthProvider;
pub(crate) struct EmbedJwtAuthProvider;
