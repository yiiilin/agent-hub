use anyhow::{Context, Result};
use axum::body::Bytes;
use chrono::{DateTime, Utc};
use futures_util::TryStream;
use hmac::{Hmac, Mac};
use reqwest::{header::HeaderMap, Method};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, error::Error};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub(crate) struct S3BundleStoreConfig {
    pub endpoint: Url,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub server_side_encryption: Option<String>,
    pub kms_key_id: Option<String>,
    pub allow_http: bool,
}

#[derive(Clone)]
pub(crate) struct S3BundleStore {
    client: reqwest::Client,
    config: S3BundleStoreConfig,
}

impl S3BundleStore {
    pub(crate) fn new(config: S3BundleStoreConfig) -> Result<Self> {
        anyhow::ensure!(
            matches!(config.endpoint.scheme(), "http" | "https"),
            "Bundle S3 endpoint must use HTTP or HTTPS"
        );
        anyhow::ensure!(
            config.endpoint.scheme() != "http" || config.allow_http,
            "HTTP Bundle S3 endpoint requires explicit allow_http"
        );
        anyhow::ensure!(
            config.endpoint.query().is_none() && config.endpoint.fragment().is_none(),
            "Bundle S3 endpoint must not contain query or fragment"
        );
        anyhow::ensure!(
            !config.bucket.trim().is_empty()
                && !config.bucket.contains('/')
                && config.bucket != "."
                && config.bucket != "..",
            "Bundle S3 bucket is invalid"
        );
        for (name, value) in [
            ("region", config.region.as_str()),
            ("access key id", config.access_key_id.as_str()),
            ("secret access key", config.secret_access_key.as_str()),
        ] {
            anyhow::ensure!(!value.trim().is_empty(), "Bundle S3 {name} is required");
        }
        if let Some(value) = config.server_side_encryption.as_deref() {
            anyhow::ensure!(
                matches!(value, "AES256" | "aws:kms"),
                "Bundle S3 server-side encryption must be AES256 or aws:kms"
            );
        }
        anyhow::ensure!(
            config.kms_key_id.is_none()
                || config.server_side_encryption.as_deref() == Some("aws:kms"),
            "Bundle S3 KMS key requires aws:kms encryption"
        );
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .build()
                .context("build Bundle S3 HTTP client")?,
            config,
        })
    }

    pub(crate) async fn put_stream<S, E>(
        &self,
        object_key: &str,
        size_bytes: u64,
        checksum_sha256: &str,
        body: S,
    ) -> Result<()>
    where
        S: TryStream<Ok = Bytes, Error = E> + Send + 'static,
        E: Into<Box<dyn Error + Send + Sync>>,
    {
        validate_sha256(checksum_sha256)?;
        let url = self.object_url(object_key)?;
        let mut extra_headers = BTreeMap::new();
        if let Some(value) = self.config.server_side_encryption.as_deref() {
            extra_headers.insert("x-amz-server-side-encryption".to_owned(), value.to_owned());
        }
        if let Some(value) = self.config.kms_key_id.as_deref() {
            extra_headers.insert(
                "x-amz-server-side-encryption-aws-kms-key-id".to_owned(),
                value.to_owned(),
            );
        }
        let headers = self.sign_headers(
            Method::PUT,
            &url,
            checksum_sha256,
            Utc::now(),
            extra_headers,
        )?;
        let response = self
            .client
            .put(url)
            .headers(headers)
            .header(reqwest::header::CONTENT_TYPE, "application/zstd")
            .header(reqwest::header::CONTENT_LENGTH, size_bytes)
            .body(reqwest::Body::wrap_stream(body))
            .send()
            .await
            .context("stream Session Bundle to S3")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Bundle S3 PUT failed with status {}",
            response.status()
        );
        Ok(())
    }

    pub(crate) async fn get(&self, object_key: &str) -> Result<reqwest::Response> {
        let url = self.object_url(object_key)?;
        let payload_sha256 = format!("{:x}", Sha256::digest([]));
        let headers = self.sign_headers(
            Method::GET,
            &url,
            &payload_sha256,
            Utc::now(),
            BTreeMap::new(),
        )?;
        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .context("read Session Bundle from S3")?;
        anyhow::ensure!(
            response.status().is_success(),
            "Bundle S3 GET failed with status {}",
            response.status()
        );
        Ok(response)
    }

    pub(crate) async fn delete(&self, object_key: &str) -> Result<()> {
        let url = self.object_url(object_key)?;
        let payload_sha256 = format!("{:x}", Sha256::digest([]));
        let headers = self.sign_headers(
            Method::DELETE,
            &url,
            &payload_sha256,
            Utc::now(),
            BTreeMap::new(),
        )?;
        let response = self
            .client
            .delete(url)
            .headers(headers)
            .send()
            .await
            .context("delete Session Bundle from S3")?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND,
            "Bundle S3 DELETE failed with status {}",
            response.status()
        );
        Ok(())
    }

    fn object_url(&self, object_key: &str) -> Result<Url> {
        validate_object_key(object_key)?;
        let mut url = self.config.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("Bundle S3 endpoint cannot contain path segments"))?;
            segments.pop_if_empty();
            segments.push(&self.config.bucket);
            for segment in object_key.split('/') {
                segments.push(segment);
            }
        }
        Ok(url)
    }

    #[cfg(test)]
    async fn create_bucket_for_test(&self) -> Result<()> {
        let url = self.bucket_url()?;
        let payload_sha256 = format!("{:x}", Sha256::digest([]));
        let headers = self.sign_headers(
            Method::PUT,
            &url,
            &payload_sha256,
            Utc::now(),
            BTreeMap::new(),
        )?;
        let response = self
            .client
            .put(url)
            .headers(headers)
            .header(reqwest::header::CONTENT_LENGTH, 0)
            .body(Vec::new())
            .send()
            .await
            .context("create Bundle S3 test bucket")?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == reqwest::StatusCode::CONFLICT,
            "Bundle S3 create bucket failed with status {}",
            response.status()
        );
        Ok(())
    }

    #[cfg(test)]
    async fn delete_bucket_for_test(&self) -> Result<()> {
        let url = self.bucket_url()?;
        let payload_sha256 = format!("{:x}", Sha256::digest([]));
        let headers = self.sign_headers(
            Method::DELETE,
            &url,
            &payload_sha256,
            Utc::now(),
            BTreeMap::new(),
        )?;
        let response = self
            .client
            .delete(url)
            .headers(headers)
            .send()
            .await
            .context("delete Bundle S3 test bucket")?;
        anyhow::ensure!(
            response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND,
            "Bundle S3 delete bucket failed with status {}",
            response.status()
        );
        Ok(())
    }

    #[cfg(test)]
    fn bucket_url(&self) -> Result<Url> {
        let mut url = self.config.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Bundle S3 endpoint cannot contain path segments"))?
            .pop_if_empty()
            .push(&self.config.bucket);
        Ok(url)
    }

    fn sign_headers(
        &self,
        method: Method,
        url: &Url,
        payload_sha256: &str,
        now: DateTime<Utc>,
        mut canonical_headers: BTreeMap<String, String>,
    ) -> Result<HeaderMap> {
        let host = canonical_host(url)?;
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short_date = now.format("%Y%m%d").to_string();
        canonical_headers.insert("host".into(), host);
        canonical_headers.insert("x-amz-content-sha256".into(), payload_sha256.to_owned());
        canonical_headers.insert("x-amz-date".into(), amz_date.clone());
        if let Some(token) = self.config.session_token.as_deref() {
            canonical_headers.insert("x-amz-security-token".into(), token.to_owned());
        }
        let signed_headers = canonical_headers
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(";");
        let canonical_header_block = canonical_headers
            .iter()
            .map(|(name, value)| format!("{name}:{}\n", value.trim()))
            .collect::<String>();
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method.as_str(),
            url.path(),
            canonical_header_block,
            signed_headers,
            payload_sha256
        );
        let scope = format!("{short_date}/{}/s3/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{:x}",
            Sha256::digest(canonical_request.as_bytes())
        );
        let date_key = sign(
            format!("AWS4{}", self.config.secret_access_key).as_bytes(),
            short_date.as_bytes(),
        )?;
        let region_key = sign(&date_key, self.config.region.as_bytes())?;
        let service_key = sign(&region_key, b"s3")?;
        let signing_key = sign(&service_key, b"aws4_request")?;
        let signature = hex(&sign(&signing_key, string_to_sign.as_bytes())?);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.config.access_key_id
        );
        let mut headers = HeaderMap::new();
        for (name, value) in canonical_headers {
            headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())
                    .context("build Bundle S3 header name")?,
                reqwest::header::HeaderValue::from_str(&value)
                    .context("build Bundle S3 header value")?,
            );
        }
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&authorization)
                .context("build Bundle S3 authorization header")?,
        );
        Ok(headers)
    }
}

fn canonical_host(url: &Url) -> Result<String> {
    let host = url.host_str().context("Bundle S3 endpoint has no host")?;
    let include_port = url
        .port()
        .is_some_and(|port| !matches!((url.scheme(), port), ("http", 80) | ("https", 443)));
    Ok(if include_port {
        format!("{host}:{}", url.port().unwrap())
    } else {
        host.to_owned()
    })
}

pub(crate) fn validate_object_key(value: &str) -> Result<()> {
    anyhow::ensure!(!value.is_empty(), "Bundle object key must not be empty");
    anyhow::ensure!(
        value
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != ".."),
        "Bundle object key contains an unsafe path segment"
    );
    Ok(())
}

pub(crate) fn validate_sha256(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "Bundle checksum must be lowercase SHA-256 hex"
    );
    Ok(())
}

fn sign(key: &[u8], value: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).context("initialize Bundle S3 signer")?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{S3BundleStore, S3BundleStoreConfig};
    use axum::{
        body::{to_bytes, Body, Bytes},
        extract::State,
        http::HeaderMap,
        routing::put,
        Router,
    };
    use futures_util::stream;
    use futures_util::StreamExt;
    use sha2::{Digest, Sha256};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    type RecordedPutPayload = Option<(HeaderMap, Vec<u8>)>;

    #[derive(Clone, Default)]
    struct RecordedPut(Arc<Mutex<RecordedPutPayload>>);

    #[tokio::test]
    async fn s3_bundle_store_streams_signed_put_with_optional_server_side_encryption() {
        let recorded = RecordedPut::default();
        let app = Router::new()
            .route(
                "/bundle-bucket/{*key}",
                put(
                    |State(recorded): State<RecordedPut>, headers: HeaderMap, body: Body| async move {
                        let bytes = to_bytes(body, 1024).await.unwrap();
                        *recorded.0.lock().unwrap() = Some((headers, bytes.to_vec()));
                    },
                ),
            )
            .with_state(recorded.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = S3BundleStore::new(S3BundleStoreConfig {
            endpoint: format!("http://{address}").parse().unwrap(),
            bucket: "bundle-bucket".into(),
            region: "us-test-1".into(),
            access_key_id: "test-access".into(),
            secret_access_key: "test-secret".into(),
            session_token: None,
            server_side_encryption: Some("AES256".into()),
            kms_key_id: None,
            allow_http: true,
        })
        .unwrap();
        let body = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ]);

        store
            .put_stream(
                "sessions/session-1/bundle-1.tar.zst",
                11,
                "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
                body,
            )
            .await
            .unwrap();

        let (headers, bytes) = recorded.0.lock().unwrap().clone().unwrap();
        assert_eq!(bytes, b"hello world");
        assert_eq!(headers["content-length"], "11");
        assert_eq!(headers["x-amz-server-side-encryption"], "AES256");
        assert_eq!(
            headers["x-amz-content-sha256"],
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        assert!(headers["authorization"]
            .to_str()
            .unwrap()
            .starts_with("AWS4-HMAC-SHA256 Credential=test-access/"));
        server.abort();
    }

    #[tokio::test]
    async fn s3_bundle_store_forwards_the_first_chunk_before_requesting_the_rest() {
        let first_received = Arc::new(Notify::new());
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            put({
                let first_received = Arc::clone(&first_received);
                let recorded = Arc::clone(&recorded);
                move |body: Body| {
                    let first_received = Arc::clone(&first_received);
                    let recorded = Arc::clone(&recorded);
                    async move {
                        let mut stream = body.into_data_stream();
                        let first = stream.next().await.unwrap().unwrap();
                        recorded.lock().unwrap().extend_from_slice(&first);
                        first_received.notify_one();
                        while let Some(chunk) = stream.next().await {
                            recorded.lock().unwrap().extend_from_slice(&chunk.unwrap());
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = S3BundleStore::new(S3BundleStoreConfig {
            endpoint: format!("http://{address}").parse().unwrap(),
            bucket: "bundle-bucket".into(),
            region: "us-test-1".into(),
            access_key_id: "test-access".into(),
            secret_access_key: "test-secret".into(),
            session_token: None,
            server_side_encryption: None,
            kms_key_id: None,
            allow_http: true,
        })
        .unwrap();
        let first = Bytes::from(vec![1_u8; 64 * 1024]);
        let second = Bytes::from(vec![2_u8; 64 * 1024]);
        let mut digest = Sha256::new();
        digest.update(&first);
        digest.update(&second);
        let checksum = format!("{:x}", digest.finalize());
        let body = async_stream::stream! {
            yield Ok::<Bytes, std::io::Error>(first);
            first_received.notified().await;
            yield Ok::<Bytes, std::io::Error>(second);
        };

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            store.put_stream(
                "sessions/session-1/backpressure.tar.zst",
                128 * 1024,
                &checksum,
                body,
            ),
        )
        .await
        .expect("streaming upload deadlocked by full-body buffering")
        .unwrap();

        let received = recorded.lock().unwrap();
        assert_eq!(received.len(), 128 * 1024);
        assert!(received[..64 * 1024].iter().all(|byte| *byte == 1));
        assert!(received[64 * 1024..].iter().all(|byte| *byte == 2));
        server.abort();
    }

    #[tokio::test]
    async fn s3_bundle_store_reports_an_interrupted_object_write() {
        let app = Router::new().route(
            "/bundle-bucket/{*key}",
            put(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = S3BundleStore::new(S3BundleStoreConfig {
            endpoint: format!("http://{address}").parse().unwrap(),
            bucket: "bundle-bucket".into(),
            region: "us-test-1".into(),
            access_key_id: "test-access".into(),
            secret_access_key: "test-secret".into(),
            session_token: None,
            server_side_encryption: None,
            kms_key_id: None,
            allow_http: true,
        })
        .unwrap();
        let error = store
            .put_stream(
                "sessions/session-1/interrupted.tar.zst",
                4,
                "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a",
                stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(&[
                    1, 2, 3, 4,
                ]))]),
            )
            .await
            .expect_err("object-store failure must fail the Bundle upload");
        assert!(error.to_string().contains("status 500"));
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires a local S3-compatible endpoint"]
    async fn local_s3_compatible_round_trip_streams_put_get_and_delete() {
        let endpoint = std::env::var("BUNDLE_S3_INTEGRATION_ENDPOINT")
            .expect("BUNDLE_S3_INTEGRATION_ENDPOINT is required");
        let access_key_id = std::env::var("BUNDLE_S3_INTEGRATION_ACCESS_KEY_ID")
            .expect("BUNDLE_S3_INTEGRATION_ACCESS_KEY_ID is required");
        let secret_access_key = std::env::var("BUNDLE_S3_INTEGRATION_SECRET_ACCESS_KEY")
            .expect("BUNDLE_S3_INTEGRATION_SECRET_ACCESS_KEY is required");
        let bucket = format!("agent-hub-test-{}", uuid::Uuid::new_v4().simple());
        let allow_http = endpoint.starts_with("http://");
        let store = S3BundleStore::new(S3BundleStoreConfig {
            endpoint: endpoint.parse().unwrap(),
            bucket,
            region: std::env::var("BUNDLE_S3_INTEGRATION_REGION")
                .unwrap_or_else(|_| "us-east-1".into()),
            access_key_id,
            secret_access_key,
            session_token: None,
            server_side_encryption: None,
            kms_key_id: None,
            allow_http,
        })
        .unwrap();
        store.create_bucket_for_test().await.unwrap();
        let bytes = Bytes::from_static(b"local s3-compatible bundle");
        let checksum = format!("{:x}", Sha256::digest(&bytes));
        let object_key = "sessions/test/bundle-1.tar.zst";
        store
            .put_stream(
                object_key,
                bytes.len() as u64,
                &checksum,
                stream::iter(vec![Ok::<_, std::io::Error>(bytes.clone())]),
            )
            .await
            .unwrap();
        let downloaded = store.get(object_key).await.unwrap().bytes().await.unwrap();
        assert_eq!(downloaded, bytes);
        store.delete(object_key).await.unwrap();
        assert!(store.get(object_key).await.is_err());
        store.delete_bucket_for_test().await.unwrap();
    }
}
