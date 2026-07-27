use crate::session_bundle_store::{validate_object_key, validate_sha256, S3BundleStore};
use anyhow::{Context, Result};
use axum::body::Body;
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) enum SkillPackageStore {
    S3(Arc<S3BundleStore>),
    Local(Arc<LocalSkillPackageStore>),
}

pub(crate) struct SkillPackageObject {
    pub body: Body,
}

#[derive(Debug)]
pub(crate) struct LocalSkillPackageStore {
    root: PathBuf,
}

impl SkillPackageStore {
    pub(crate) fn s3(store: Arc<S3BundleStore>) -> Self {
        Self::S3(store)
    }

    pub(crate) fn local(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root).context("create local Skill package store")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .context("protect local Skill package store")?;
        }
        Ok(Self::Local(Arc::new(LocalSkillPackageStore { root })))
    }

    pub(crate) async fn put_file(
        &self,
        object_key: &str,
        source: &Path,
        size_bytes: u64,
        checksum_sha256: &str,
    ) -> Result<()> {
        validate_object_key(object_key)?;
        validate_sha256(checksum_sha256)?;
        match self {
            Self::S3(store) => {
                let file = tokio::fs::File::open(source)
                    .await
                    .context("open staged Skill package")?;
                store
                    .put_stream(
                        object_key,
                        size_bytes,
                        checksum_sha256,
                        ReaderStream::new(file),
                    )
                    .await
            }
            Self::Local(store) => {
                store
                    .put_file(object_key, source, size_bytes, checksum_sha256)
                    .await
            }
        }
    }

    pub(crate) async fn get(&self, object_key: &str) -> Result<SkillPackageObject> {
        validate_object_key(object_key)?;
        match self {
            Self::S3(store) => {
                let response = store.get(object_key).await?;
                Ok(SkillPackageObject {
                    body: Body::from_stream(response.bytes_stream()),
                })
            }
            Self::Local(store) => store.get(object_key).await,
        }
    }

    pub(crate) async fn delete(&self, object_key: &str) -> Result<()> {
        validate_object_key(object_key)?;
        match self {
            Self::S3(store) => store.delete(object_key).await,
            Self::Local(store) => store.delete(object_key).await,
        }
    }
}

impl LocalSkillPackageStore {
    fn object_path(&self, object_key: &str) -> Result<PathBuf> {
        validate_object_key(object_key)?;
        let mut path = self.root.clone();
        for component in object_key.split('/') {
            path.push(component);
        }
        Ok(path)
    }

    async fn put_file(
        &self,
        object_key: &str,
        source: &Path,
        expected_size: u64,
        expected_checksum: &str,
    ) -> Result<()> {
        let target = self.object_path(object_key)?;
        let parent = target
            .parent()
            .context("Skill package object has no parent")?;
        tokio::fs::create_dir_all(parent)
            .await
            .context("create local Skill package object directory")?;
        let temporary = parent.join(format!(".upload-{}.tmp", Uuid::new_v4().simple()));
        let result = async {
            let mut input = tokio::fs::File::open(source)
                .await
                .context("open staged Skill package")?;
            let mut output = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .await
                .context("create local Skill package temporary object")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                output
                    .set_permissions(std::fs::Permissions::from_mode(0o600))
                    .await
                    .context("protect local Skill package temporary object")?;
            }
            let mut hasher = Sha256::new();
            let mut size = 0_u64;
            let mut buffer = vec![0_u8; 64 * 1024];
            loop {
                let read = input
                    .read(&mut buffer)
                    .await
                    .context("read staged Skill package")?;
                if read == 0 {
                    break;
                }
                size = size.saturating_add(read as u64);
                anyhow::ensure!(size <= expected_size, "Skill package exceeds declared size");
                hasher.update(&buffer[..read]);
                output
                    .write_all(&buffer[..read])
                    .await
                    .context("write local Skill package object")?;
            }
            anyhow::ensure!(size == expected_size, "Skill package size does not match");
            anyhow::ensure!(
                format!("{:x}", hasher.finalize()) == expected_checksum,
                "Skill package checksum does not match"
            );
            output
                .sync_all()
                .await
                .context("sync local Skill package object")?;
            tokio::fs::rename(&temporary, &target)
                .await
                .context("commit local Skill package object")?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result
    }

    async fn get(&self, object_key: &str) -> Result<SkillPackageObject> {
        let path = self.object_path(object_key)?;
        let file = tokio::fs::File::open(path)
            .await
            .context("open local Skill package object")?;
        Ok(SkillPackageObject {
            body: Body::from_stream(ReaderStream::new(file)),
        })
    }

    async fn delete(&self, object_key: &str) -> Result<()> {
        let path = self.object_path(object_key)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("delete local Skill package object"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_store_round_trip_and_rejects_unsafe_keys() {
        let root = tempfile::tempdir().unwrap();
        let source_root = tempfile::tempdir().unwrap();
        let source = source_root.path().join("package.tar.zst");
        tokio::fs::write(&source, b"skill-package").await.unwrap();
        let checksum = format!("{:x}", Sha256::digest(b"skill-package"));
        let store = SkillPackageStore::local(root.path().to_path_buf()).unwrap();

        store
            .put_file("skills/one/package.tar.zst", &source, 13, &checksum)
            .await
            .unwrap();
        let object = store.get("skills/one/package.tar.zst").await.unwrap();
        let bytes = axum::body::to_bytes(object.body, 1024).await.unwrap();
        assert_eq!(&bytes[..], b"skill-package");
        store.delete("skills/one/package.tar.zst").await.unwrap();
        assert!(store.get("skills/one/package.tar.zst").await.is_err());
        assert!(store
            .put_file("skills/../escape", &source, 13, &checksum)
            .await
            .is_err());
    }
}
