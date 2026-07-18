use std::{collections::BTreeSet, path::PathBuf};

use agent_hub_shared::CodexVersionArtifactDto;
use anyhow::Context;
use futures_util::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RuntimePlatform {
    pub os: String,
    pub architecture: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedCodexArtifact {
    pub descriptor: CodexVersionArtifactDto,
    pub storage_path: PathBuf,
}

#[derive(Clone)]
pub(crate) struct CodexReleaseClient {
    http: reqwest::Client,
    api_base: String,
    artifact_root: PathBuf,
    allow_http: bool,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    assets: Vec<GitHubReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct GitHubReleaseAsset {
    name: String,
    browser_download_url: String,
    digest: String,
    size: u64,
}

impl CodexReleaseClient {
    pub(crate) fn new(
        http: reqwest::Client,
        api_base: String,
        artifact_root: PathBuf,
        allow_http: bool,
    ) -> anyhow::Result<Self> {
        let parsed = reqwest::Url::parse(&api_base).context("invalid Codex release API URL")?;
        anyhow::ensure!(
            parsed.scheme() == "https" || (allow_http && parsed.scheme() == "http"),
            "Codex release API must use HTTPS"
        );
        Ok(Self {
            http,
            api_base: api_base.trim_end_matches('/').to_owned(),
            artifact_root,
            allow_http,
        })
    }

    pub(crate) async fn prepare_release(
        &self,
        version: &str,
        platforms: &[RuntimePlatform],
    ) -> anyhow::Result<Vec<PreparedCodexArtifact>> {
        validate_concrete_version(version)?;
        anyhow::ensure!(!platforms.is_empty(), "no registered Runtime platforms");
        let release = self
            .http
            .get(format!("{}/releases/tags/rust-v{version}", self.api_base))
            .header(reqwest::header::USER_AGENT, "agent-hub-codex-rollout")
            .send()
            .await
            .context("fetch Codex release metadata")?
            .error_for_status()
            .context("Codex release metadata request failed")?
            .json::<GitHubRelease>()
            .await
            .context("parse Codex release metadata")?;
        anyhow::ensure!(
            release.tag_name == format!("rust-v{version}"),
            "Codex release tag does not match the requested version"
        );

        let platforms = platforms.iter().cloned().collect::<BTreeSet<_>>();
        let staging = self
            .artifact_root
            .join(format!(".staging-{version}-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&staging)
            .await
            .context("create Codex artifact staging directory")?;
        let prepared = async {
            let mut artifacts = Vec::with_capacity(platforms.len());
            for platform in platforms {
                let asset_name = asset_name_for_platform(&platform.os, &platform.architecture)?;
                let asset = release
                    .assets
                    .iter()
                    .find(|asset| asset.name == asset_name)
                    .with_context(|| format!("Codex release is missing asset {asset_name}"))?;
                let expected_sha256 = asset
                    .digest
                    .strip_prefix("sha256:")
                    .context("Codex release asset has no SHA-256 digest")?;
                validate_sha256(expected_sha256)?;
                let asset_url = reqwest::Url::parse(&asset.browser_download_url)
                    .context("invalid Codex release asset URL")?;
                anyhow::ensure!(
                    asset_url.scheme() == "https"
                        || (self.allow_http && asset_url.scheme() == "http"),
                    "Codex release asset must use HTTPS"
                );
                let staged_path = staging.join(asset_name);
                self.download_asset(asset, expected_sha256, &staged_path)
                    .await?;
                artifacts.push((
                    CodexVersionArtifactDto {
                        version: version.to_owned(),
                        os: platform.os,
                        architecture: platform.architecture,
                        artifact_name: asset_name.to_owned(),
                        sha256: expected_sha256.to_owned(),
                        size_bytes: asset.size,
                    },
                    asset_name.to_owned(),
                ));
            }
            Ok::<_, anyhow::Error>(artifacts)
        }
        .await;

        let artifacts = match prepared {
            Ok(artifacts) => artifacts,
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&staging).await;
                return Err(error);
            }
        };
        tokio::fs::create_dir_all(&self.artifact_root)
            .await
            .context("create Codex artifact root")?;
        let published = self.artifact_root.join(version);
        if tokio::fs::try_exists(&published).await? {
            tokio::fs::remove_dir_all(&published)
                .await
                .context("replace existing Codex artifact version")?;
        }
        tokio::fs::rename(&staging, &published)
            .await
            .context("publish verified Codex artifacts")?;
        Ok(artifacts
            .into_iter()
            .map(|(descriptor, asset_name)| PreparedCodexArtifact {
                descriptor,
                storage_path: published.join(asset_name),
            })
            .collect())
    }

    async fn download_asset(
        &self,
        asset: &GitHubReleaseAsset,
        expected_sha256: &str,
        destination: &PathBuf,
    ) -> anyhow::Result<()> {
        let response = self
            .http
            .get(&asset.browser_download_url)
            .header(reqwest::header::USER_AGENT, "agent-hub-codex-rollout")
            .send()
            .await
            .with_context(|| format!("download Codex release asset {}", asset.name))?
            .error_for_status()
            .with_context(|| format!("Codex release asset request failed: {}", asset.name))?;
        if let Some(length) = response.content_length() {
            anyhow::ensure!(length == asset.size, "Codex release asset size mismatch");
        }
        let mut file = tokio::fs::File::create(destination)
            .await
            .context("create staged Codex artifact")?;
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("stream Codex release asset")?;
            size = size
                .checked_add(chunk.len() as u64)
                .context("Codex release asset size overflow")?;
            anyhow::ensure!(size <= asset.size, "Codex release asset size mismatch");
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .context("write staged Codex artifact")?;
        }
        file.flush().await.context("flush staged Codex artifact")?;
        anyhow::ensure!(size == asset.size, "Codex release asset size mismatch");
        let actual = format!("{:x}", hasher.finalize());
        anyhow::ensure!(
            actual == expected_sha256,
            "Codex release asset SHA-256 mismatch"
        );
        Ok(())
    }
}

pub(crate) fn validate_concrete_version(version: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !version.is_empty()
            && version != "latest"
            && version.len() <= 64
            && version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_')),
        "Codex version must be a concrete release version"
    );
    Ok(())
}

fn validate_sha256(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "Codex release asset has an invalid SHA-256 digest"
    );
    Ok(())
}

pub(crate) fn asset_name_for_platform(
    os: &str,
    architecture: &str,
) -> anyhow::Result<&'static str> {
    match (os, architecture) {
        ("linux", "x86_64") => Ok("codex-x86_64-unknown-linux-musl.zst"),
        ("linux", "aarch64") => Ok("codex-aarch64-unknown-linux-musl.zst"),
        ("macos", "x86_64") => Ok("codex-x86_64-apple-darwin.zst"),
        ("macos", "aarch64") => Ok("codex-aarch64-apple-darwin.zst"),
        ("windows", "x86_64") => Ok("codex-x86_64-pc-windows-msvc.exe.zst"),
        ("windows", "aarch64") => Ok("codex-aarch64-pc-windows-msvc.exe.zst"),
        _ => anyhow::bail!("unsupported Runtime platform {os}/{architecture}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Bytes, extract::State, response::IntoResponse, routing::get, Json, Router};
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[test]
    fn official_codex_release_assets_map_every_supported_runtime_platform() {
        for (os, architecture, expected) in [
            ("linux", "x86_64", "codex-x86_64-unknown-linux-musl.zst"),
            ("linux", "aarch64", "codex-aarch64-unknown-linux-musl.zst"),
            ("macos", "x86_64", "codex-x86_64-apple-darwin.zst"),
            ("macos", "aarch64", "codex-aarch64-apple-darwin.zst"),
            ("windows", "x86_64", "codex-x86_64-pc-windows-msvc.exe.zst"),
            (
                "windows",
                "aarch64",
                "codex-aarch64-pc-windows-msvc.exe.zst",
            ),
        ] {
            assert_eq!(asset_name_for_platform(os, architecture).unwrap(), expected);
        }
        assert!(asset_name_for_platform("freebsd", "x86_64").is_err());
        assert!(asset_name_for_platform("linux", "riscv64").is_err());
    }

    async fn release_fixture(digest: String) -> (String, tokio::task::JoinHandle<()>) {
        #[derive(Clone)]
        struct FixtureState {
            base_url: String,
            digest: String,
        }

        async fn release(State(state): State<FixtureState>) -> Json<serde_json::Value> {
            Json(json!({
                "tag_name": "rust-v0.144.5",
                "assets": [{
                    "name": "codex-x86_64-unknown-linux-musl.zst",
                    "browser_download_url": format!("{}/artifact", state.base_url),
                    "digest": state.digest,
                    "size": 16
                }]
            }))
        }

        async fn artifact() -> impl IntoResponse {
            Bytes::from_static(b"release-artifact")
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/releases/tags/rust-v0.144.5", get(release))
            .route("/artifact", get(artifact))
            .with_state(FixtureState {
                base_url: base_url.clone(),
                digest,
            });
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (base_url, handle)
    }

    #[tokio::test]
    async fn release_download_verifies_github_digest_before_publishing_artifact() {
        let (base_url, server) = release_fixture(
            "sha256:55d98606526de0f88b30c309717deef32c0e061e1319fd1f20f866b49a226174".into(),
        )
        .await;
        let root = tempdir().unwrap();
        let client = CodexReleaseClient::new(
            reqwest::Client::new(),
            base_url,
            root.path().to_path_buf(),
            true,
        )
        .unwrap();

        let prepared = client
            .prepare_release(
                "0.144.5",
                &[RuntimePlatform {
                    os: "linux".into(),
                    architecture: "x86_64".into(),
                }],
            )
            .await
            .unwrap();

        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].descriptor.version, "0.144.5");
        assert_eq!(prepared[0].descriptor.size_bytes, 16);
        assert_eq!(
            tokio::fs::read(&prepared[0].storage_path).await.unwrap(),
            b"release-artifact"
        );
        server.abort();
    }

    #[tokio::test]
    async fn release_download_rejects_digest_mismatch_without_publishing_artifact() {
        let (base_url, server) = release_fixture(format!("sha256:{}", "0".repeat(64))).await;
        let root = tempdir().unwrap();
        let client = CodexReleaseClient::new(
            reqwest::Client::new(),
            base_url,
            root.path().to_path_buf(),
            true,
        )
        .unwrap();

        let error = client
            .prepare_release(
                "0.144.5",
                &[RuntimePlatform {
                    os: "linux".into(),
                    architecture: "x86_64".into(),
                }],
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("SHA-256 mismatch"));
        assert!(!root.path().join("0.144.5").exists());
        server.abort();
    }
}
