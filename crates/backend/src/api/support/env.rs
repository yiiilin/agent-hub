//! 环境变量解析与运行配置。

use crate::session_bundle_store::{S3BundleStore, S3BundleStoreConfig};
use crate::skill_package_store::SkillPackageStore;
use crate::{
    DEFAULT_MODEL_PROXY_TIMEOUT, DEFAULT_SESSION_BUNDLE_MAX_BYTES, MAX_MODEL_PROXY_TIMEOUT,
};
use anyhow::Context;
use ipnet::IpNet;
use std::path::PathBuf;
use std::{env, sync::Arc, time::Duration};
use url::Url;
use zeroize::Zeroizing;

pub(crate) fn model_proxy_timeout_from_env() -> anyhow::Result<Duration> {
    parse_model_proxy_timeout(env::var("HUB_MODEL_PROXY_TIMEOUT_SECS").ok().as_deref())
}

pub(crate) fn trusted_proxy_cidrs_from_env() -> anyhow::Result<Option<Vec<IpNet>>> {
    parse_trusted_proxy_cidrs(env::var("TRUSTED_PROXY_CIDRS").ok().as_deref())
}

pub(crate) fn parse_trusted_proxy_cidrs(value: Option<&str>) -> anyhow::Result<Option<Vec<IpNet>>> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let cidrs = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<IpNet>()
                .with_context(|| format!("invalid TRUSTED_PROXY_CIDRS entry: {value}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(cidrs))
}

pub(crate) fn model_gateway_config_from_env() -> anyhow::Result<(String, Arc<Zeroizing<String>>)> {
    let url = env::var("HUB_MODEL_GATEWAY_URL").context("HUB_MODEL_GATEWAY_URL is required")?;
    let auth_token = env::var("HUB_MODEL_GATEWAY_AUTH_TOKEN")
        .context("HUB_MODEL_GATEWAY_AUTH_TOKEN is required")?;
    validate_model_gateway_config(&url, &auth_token)
}

pub(crate) fn validate_model_gateway_config(
    url: &str,
    auth_token: &str,
) -> anyhow::Result<(String, Arc<Zeroizing<String>>)> {
    let mut parsed = Url::parse(url).context("HUB_MODEL_GATEWAY_URL must be a valid URL")?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "HUB_MODEL_GATEWAY_URL must use http or https"
    );
    anyhow::ensure!(
        parsed.host_str().is_some(),
        "HUB_MODEL_GATEWAY_URL must include a host"
    );
    anyhow::ensure!(
        parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "HUB_MODEL_GATEWAY_URL must not include credentials, query, or fragment"
    );
    anyhow::ensure!(
        parsed.path().is_empty() || parsed.path() == "/",
        "HUB_MODEL_GATEWAY_URL must not include a path"
    );
    anyhow::ensure!(
        !auth_token.is_empty()
            && auth_token.len() <= 4096
            && auth_token
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && !byte.is_ascii_whitespace()),
        "HUB_MODEL_GATEWAY_AUTH_TOKEN must be a non-empty visible ASCII token"
    );
    parsed.set_path("");
    Ok((
        parsed.as_str().trim_end_matches('/').to_owned(),
        Arc::new(Zeroizing::new(auth_token.to_owned())),
    ))
}

pub(crate) fn session_bundle_max_bytes_from_env() -> anyhow::Result<u64> {
    let value = env::var("HUB_SESSION_BUNDLE_MAX_BYTES")
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .context("HUB_SESSION_BUNDLE_MAX_BYTES must be a positive integer")
        })
        .transpose()?
        .unwrap_or(DEFAULT_SESSION_BUNDLE_MAX_BYTES);
    anyhow::ensure!(value > 0, "HUB_SESSION_BUNDLE_MAX_BYTES must be positive");
    anyhow::ensure!(
        value <= i64::MAX as u64,
        "HUB_SESSION_BUNDLE_MAX_BYTES exceeds the supported database range"
    );
    Ok(value)
}

pub(crate) fn session_bundle_store_from_env() -> anyhow::Result<Option<Arc<S3BundleStore>>> {
    let endpoint = match env::var("HUB_BUNDLE_S3_ENDPOINT") {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let required = |name: &str| -> anyhow::Result<String> {
        let value = env::var(name).with_context(|| format!("{name} is required"))?;
        anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
        Ok(value)
    };
    let allow_http = match env::var("HUB_BUNDLE_S3_ALLOW_HTTP") {
        Ok(value) if value == "true" => true,
        Ok(value) if value == "false" => false,
        Err(env::VarError::NotPresent) => false,
        Ok(_) => anyhow::bail!("HUB_BUNDLE_S3_ALLOW_HTTP must be true or false"),
        Err(error) => return Err(error.into()),
    };
    let store = S3BundleStore::new(S3BundleStoreConfig {
        endpoint: endpoint
            .parse()
            .context("HUB_BUNDLE_S3_ENDPOINT must be a valid URL")?,
        bucket: required("HUB_BUNDLE_S3_BUCKET")?,
        region: env::var("HUB_BUNDLE_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_id: required("HUB_BUNDLE_S3_ACCESS_KEY_ID")?,
        secret_access_key: required("HUB_BUNDLE_S3_SECRET_ACCESS_KEY")?,
        session_token: env::var("HUB_BUNDLE_S3_SESSION_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        server_side_encryption: env::var("HUB_BUNDLE_S3_SERVER_SIDE_ENCRYPTION")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        kms_key_id: env::var("HUB_BUNDLE_S3_KMS_KEY_ID")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        allow_http,
    })?;
    Ok(Some(Arc::new(store)))
}

pub(crate) fn skill_package_store_from_env(
    s3_store: Option<Arc<S3BundleStore>>,
) -> anyhow::Result<Arc<SkillPackageStore>> {
    let backend = env::var("HUB_SKILL_PACKAGE_STORAGE").unwrap_or_else(|_| {
        if s3_store.is_some() {
            "s3".into()
        } else {
            "local".into()
        }
    });
    let store = match backend.trim() {
        "s3" => SkillPackageStore::s3(
            s3_store.context("HUB_SKILL_PACKAGE_STORAGE=s3 requires HUB_BUNDLE_S3_ENDPOINT")?,
        ),
        "local" => SkillPackageStore::local(PathBuf::from(
            env::var("HUB_SKILL_PACKAGE_LOCAL_DIR")
                .unwrap_or_else(|_| "/var/lib/agent-hub/skill-packages".into()),
        ))?,
        _ => anyhow::bail!("HUB_SKILL_PACKAGE_STORAGE must be local or s3"),
    };
    Ok(Arc::new(store))
}

pub(crate) fn parse_model_proxy_timeout(value: Option<&str>) -> anyhow::Result<Duration> {
    let Some(value) = value else {
        return Ok(DEFAULT_MODEL_PROXY_TIMEOUT);
    };
    let seconds = value
        .trim()
        .parse::<u64>()
        .map_err(|_| anyhow::anyhow!("HUB_MODEL_PROXY_TIMEOUT_SECS must be an integer"))?;
    let timeout = Duration::from_secs(seconds);
    if timeout.is_zero() || timeout > MAX_MODEL_PROXY_TIMEOUT {
        anyhow::bail!("HUB_MODEL_PROXY_TIMEOUT_SECS must be between 1 and 900");
    }
    Ok(timeout)
}
