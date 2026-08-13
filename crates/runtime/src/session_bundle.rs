use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    os::unix::fs::symlink,
    path::{Component, Path, PathBuf},
};
use tar::{Archive, Builder, EntryType, Header};
use uuid::Uuid;
use walkdir::WalkDir;

const BUNDLE_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone)]
pub(crate) struct SessionBundleCreateSpec {
    pub session_id: Uuid,
    pub history_checkpoint: i64,
    pub bundle_generation: i64,
    pub ownership_generation: i64,
    pub producing_engine_version: String,
    pub created_at: DateTime<Utc>,
    pub workspace: PathBuf,
    pub archive_path: PathBuf,
    /// 强制停止快照：无 checkpoint 元数据，manifest 用占位值
    /// （history_checkpoint=0、generation=0、ownership=0、engine="force-stop"），
    /// 恢复时跳过这些字段的比对，只校验 session/checksum/workspace。
    pub force_stop_snapshot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionBundleArtifact {
    pub archive_path: PathBuf,
    pub checksum_sha256: String,
    pub size_bytes: u64,
    pub manifest: SessionBundleManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionBundleManifest {
    pub format_version: u32,
    pub session_id: Uuid,
    pub history_checkpoint: i64,
    pub bundle_generation: i64,
    pub ownership_generation: i64,
    pub producing_engine_version: String,
    pub created_at: DateTime<Utc>,
    pub workspace: BundleTreeDeclaration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BundleTreeDeclaration {
    pub entry_count: u64,
    pub size_bytes: u64,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone)]
struct BundleSourceEntry {
    source: PathBuf,
    relative: PathBuf,
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    written: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            written: 0,
        }
    }

    fn finish(self) -> (W, String, u64) {
        (
            self.inner,
            format!("{:x}", self.hasher.finalize()),
            self.written,
        )
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.written = self.written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) fn create_session_bundle(
    spec: &SessionBundleCreateSpec,
) -> Result<SessionBundleArtifact> {
    validate_create_spec(spec)?;
    let workspace_entries = collect_tree_entries(&spec.workspace, |_, _| true)?;
    let manifest = SessionBundleManifest {
        format_version: BUNDLE_FORMAT_VERSION,
        session_id: spec.session_id,
        history_checkpoint: if spec.force_stop_snapshot {
            0
        } else {
            spec.history_checkpoint
        },
        bundle_generation: if spec.force_stop_snapshot {
            0
        } else {
            spec.bundle_generation
        },
        ownership_generation: if spec.force_stop_snapshot {
            0
        } else {
            spec.ownership_generation
        },
        producing_engine_version: if spec.force_stop_snapshot {
            "force-stop".to_owned()
        } else {
            spec.producing_engine_version.trim().to_owned()
        },
        created_at: spec.created_at,
        workspace: declare_tree(&spec.workspace, &workspace_entries)?,
    };

    let parent = spec
        .archive_path
        .parent()
        .context("Session Bundle path has no parent directory")?;
    fs::create_dir_all(parent).context("create Session Bundle staging directory")?;
    let temporary = parent.join(format!(".bundle-{}.tmp", Uuid::new_v4().simple()));
    let create_result = (|| -> Result<(String, u64)> {
        let file = File::create(&temporary).context("create Session Bundle staging file")?;
        let writer = HashingWriter::new(file);
        let encoder = zstd::Encoder::new(writer, 3).context("create zstd encoder")?;
        let mut archive = Builder::new(encoder);
        archive.follow_symlinks(false);
        archive
            .append_dir("workspace", &spec.workspace)
            .context("append Workspace root")?;
        append_entries(&mut archive, &workspace_entries, Path::new("workspace"))?;

        let manifest_bytes =
            serde_json::to_vec_pretty(&manifest).context("serialize Session Bundle manifest")?;
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o600);
        header.set_mtime(spec.created_at.timestamp().max(0) as u64);
        header.set_size(manifest_bytes.len() as u64);
        header.set_cksum();
        archive
            .append_data(&mut header, "manifest.json", manifest_bytes.as_slice())
            .context("append Session Bundle manifest")?;

        let encoder = archive.into_inner().context("finish tar archive")?;
        let writer = encoder.finish().context("finish zstd stream")?;
        let (file, checksum, size) = writer.finish();
        file.sync_all()
            .context("sync Session Bundle staging file")?;
        Ok((checksum, size))
    })();
    let (checksum_sha256, size_bytes) = match create_result {
        Ok(result) => result,
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
    };
    if spec.archive_path.exists() {
        fs::remove_file(&spec.archive_path).context("replace old staged Session Bundle")?;
    }
    fs::rename(&temporary, &spec.archive_path).context("commit staged Session Bundle")?;
    File::open(parent)
        .context("open Session Bundle staging directory")?
        .sync_all()
        .context("sync Session Bundle staging directory")?;
    Ok(SessionBundleArtifact {
        archive_path: spec.archive_path.clone(),
        checksum_sha256,
        size_bytes,
        manifest,
    })
}

fn validate_create_spec(spec: &SessionBundleCreateSpec) -> Result<()> {
    anyhow::ensure!(!spec.session_id.is_nil(), "Session id must not be nil");

    if !spec.force_stop_snapshot {
        anyhow::ensure!(spec.history_checkpoint >= 0, "invalid history checkpoint");
        anyhow::ensure!(spec.bundle_generation > 0, "invalid Bundle generation");
        anyhow::ensure!(
            spec.ownership_generation > 0,
            "invalid ownership generation"
        );
        anyhow::ensure!(
            !spec.producing_engine_version.trim().is_empty(),
            "producing Engine version must not be empty"
        );
    }
    anyhow::ensure!(
        spec.workspace.is_dir(),
        "Workspace directory is unavailable"
    );
    Ok(())
}

fn collect_tree_entries<F>(root: &Path, include: F) -> Result<Vec<BundleSourceEntry>>
where
    F: Fn(&Path, std::fs::FileType) -> bool,
{
    let mut entries = Vec::new();
    let mut walk = WalkDir::new(root)
        .follow_links(false)
        .sort_by_file_name()
        .into_iter();
    let _ = walk.next();
    for entry in walk {
        let entry = entry.context("walk Session Bundle source")?;
        let relative = entry
            .path()
            .strip_prefix(root)
            .context("Bundle source escaped its root")?
            .to_path_buf();
        validate_relative_path(&relative)?;
        let file_type = entry.file_type();
        if !include(&relative, file_type) {
            continue;
        }
        anyhow::ensure!(
            file_type.is_dir() || file_type.is_file() || file_type.is_symlink(),
            "Session Bundle source contains a special file: {}",
            entry.path().display()
        );
        if file_type.is_symlink() {
            let target = fs::read_link(entry.path()).context("read Bundle symbolic link")?;
            validate_symlink_target(&relative, &target)?;
        }
        entries.push(BundleSourceEntry {
            source: entry.path().to_path_buf(),
            relative,
        });
    }
    Ok(entries)
}

fn append_entries<W: Write>(
    archive: &mut Builder<W>,
    entries: &[BundleSourceEntry],
    prefix: &Path,
) -> Result<()> {
    for entry in entries {
        archive
            .append_path_with_name(&entry.source, prefix.join(&entry.relative))
            .with_context(|| format!("append Session Bundle path {}", entry.source.display()))?;
    }
    Ok(())
}

fn append_virtual_directory<W: Write>(
    archive: &mut Builder<W>,
    path: &str,
    created_at: DateTime<Utc>,
) -> Result<()> {
    let mut header = Header::new_gnu();
    header.set_entry_type(EntryType::Directory);
    header.set_mode(0o700);
    header.set_mtime(created_at.timestamp().max(0) as u64);
    header.set_size(0);
    header.set_cksum();
    archive
        .append_data(&mut header, path, std::io::empty())
        .context("append Session Bundle directory")?;
    Ok(())
}

fn declare_tree(root: &Path, entries: &[BundleSourceEntry]) -> Result<BundleTreeDeclaration> {
    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    for entry in entries {
        let metadata = fs::symlink_metadata(&entry.source).context("read Bundle entry metadata")?;
        let kind = if metadata.file_type().is_dir() {
            b'd'
        } else if metadata.file_type().is_symlink() {
            b'l'
        } else {
            b'f'
        };
        hasher.update([kind]);
        hasher.update(entry.relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        if metadata.file_type().is_file() {
            size_bytes = size_bytes
                .checked_add(metadata.len())
                .context("Session Bundle content size overflowed")?;
            let mut file = File::open(&entry.source).context("open Bundle content")?;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).context("hash Bundle content")?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        } else if metadata.file_type().is_symlink() {
            hasher.update(
                fs::read_link(&entry.source)
                    .context("read Bundle symbolic link")?
                    .to_string_lossy()
                    .as_bytes(),
            );
        }
        hasher.update([0xff]);
    }
    let _ = root;
    Ok(BundleTreeDeclaration {
        entry_count: entries.len() as u64,
        size_bytes,
        checksum_sha256: format!("{:x}", hasher.finalize()),
    })
}

fn checksum_file(path: &Path) -> Result<(String, u64)> {
    let mut file = File::open(path).context("open Session Bundle")?;
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).context("read Session Bundle")?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size
            .checked_add(read as u64)
            .context("Session Bundle size overflowed")?;
    }
    Ok((format!("{:x}", hasher.finalize()), size))
}

pub(crate) fn restore_session_workspace_only(
    archive_path: &Path,
    expected_checksum_sha256: &str,
    expected_size_bytes: u64,
    expected_session_id: Uuid,
    expected_history_checkpoint: i64,
    destination_root: &Path,
    force_stop_snapshot: bool,
) -> Result<SessionBundleManifest> {
    validate_sha256(expected_checksum_sha256)?;
    let (actual_checksum, actual_size) = checksum_file(archive_path)?;
    anyhow::ensure!(
        actual_size == expected_size_bytes,
        "Session Bundle size does not match Hub metadata"
    );
    anyhow::ensure!(
        actual_checksum == expected_checksum_sha256,
        "Session Bundle checksum does not match Hub metadata"
    );
    anyhow::ensure!(
        !destination_root.exists(),
        "Session Bundle restore destination already exists"
    );
    let parent = destination_root
        .parent()
        .context("Session Bundle restore destination has no parent")?;
    fs::create_dir_all(parent).context("create Session Bundle restore parent")?;
    let temporary = parent.join(format!(".restore-{}.tmp", Uuid::new_v4().simple()));
    fs::create_dir(&temporary).context("create Session Bundle restore staging directory")?;

    // 解包与全部校验放入闭包：任何一步失败都清理 staging 目录。
    let restore_result = (|| -> Result<SessionBundleManifest> {
        let file = File::open(archive_path).context("open Session Bundle for restore")?;
        let decoder = zstd::Decoder::new(file).context("decode Session Bundle zstd stream")?;
        let mut archive = Archive::new(decoder);
        let mut seen = BTreeSet::new();
        let mut symlinks = BTreeSet::new();
        let mut manifest: Option<SessionBundleManifest> = None;
        let mut saw_workspace_root = false;
        for entry in archive
            .entries()
            .context("read Session Bundle tar stream")?
        {
            let mut entry = entry.context("read Session Bundle entry")?;
            let archive_path = entry
                .path()
                .context("read Session Bundle entry path")?
                .into_owned();
            validate_relative_path(&archive_path)?;
            anyhow::ensure!(
                seen.insert(archive_path.clone()),
                "Session Bundle contains a duplicate path"
            );
            anyhow::ensure!(
                !archive_path
                    .ancestors()
                    .skip(1)
                    .any(|ancestor| symlinks.contains(ancestor)),
                "Session Bundle entry traverses an archived symbolic link"
            );
            let mut components = archive_path.components();
            let top = components
                .next()
                .context("Session Bundle contains an empty path")?
                .as_os_str();
            let entry_type = entry.header().entry_type();
            anyhow::ensure!(
                matches!(
                    entry_type,
                    EntryType::Regular | EntryType::Directory | EntryType::Symlink
                ),
                "Session Bundle contains an unsupported entry type"
            );
            if top == "manifest.json" {
                anyhow::ensure!(
                    archive_path == Path::new("manifest.json") && entry_type.is_file(),
                    "Session Bundle manifest must be one regular top-level file"
                );
                anyhow::ensure!(manifest.is_none(), "Session Bundle has multiple manifests");
                manifest = Some(
                    serde_json::from_reader(&mut entry).context("parse Session Bundle manifest")?,
                );
                continue;
            }
            if top != "workspace" {
                // 只接受 manifest.json 与 workspace/；其余顶层（旧 native-session、
                // engine-state、秘密目录等）一律拒绝，避免静默接受未知内容。
                anyhow::bail!(
                    "Session Bundle contains an unexpected top-level entry: {}",
                    archive_path.display()
                );
            }
            if archive_path == Path::new("workspace") {
                anyhow::ensure!(
                    entry_type.is_dir(),
                    "Session Bundle workspace/ must be a directory"
                );
                saw_workspace_root = true;
            }
            let destination_path = if archive_path == Path::new("workspace") {
                temporary.join("workspace")
            } else {
                temporary.join("workspace").join(components.as_path())
            };
            if entry_type.is_symlink() {
                let link = entry
                    .link_name()
                    .context("read Session Bundle symlink target")?
                    .context("Session Bundle symlink has no target")?;
                // 与完整恢复一致：目标必须是相对路径且解析结果不得逃逸顶层。
                // workspace-only 恢复额外要求解析结果的首组件是 workspace，
                // 拒绝指向 native-session/engine-state/manifest.json 的链接。
                validate_symlink_target(&archive_path, &link)?;
                let resolved = resolve_symlink_path(&archive_path, &link)?;
                let first = resolved
                    .components()
                    .next()
                    .context("Session Bundle symlink resolves to an empty path")?;
                anyhow::ensure!(
                    matches!(first, Component::Normal(value) if value == "workspace"),
                    "Session Bundle workspace symlink must resolve inside workspace/"
                );
                symlinks.insert(archive_path.clone());
                if let Some(parent) = destination_path.parent() {
                    fs::create_dir_all(parent).context("create Session Bundle symlink parent")?;
                }
                symlink(link, &destination_path)
                    .context("create Session Bundle workspace symlink")?;
                continue;
            }
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent).context("create Session Bundle restore directory")?;
            }
            entry
                .unpack(&destination_path)
                .context("extract Session Bundle workspace entry")?;
        }
        anyhow::ensure!(
            saw_workspace_root,
            "Session Bundle is missing workspace/ directory"
        );
        let manifest = manifest.context("Session Bundle is missing manifest.json")?;
        anyhow::ensure!(
            manifest.format_version == BUNDLE_FORMAT_VERSION,
            "unsupported Session Bundle format version"
        );
        anyhow::ensure!(
            manifest.session_id == expected_session_id,
            "Session Bundle belongs to a different Session"
        );
        if !force_stop_snapshot {
            anyhow::ensure!(
                manifest.history_checkpoint == expected_history_checkpoint,
                "Session Bundle history checkpoint does not match Hub metadata"
            );
        }
        let workspace_entries = collect_tree_entries(&temporary.join("workspace"), |_, _| true)?;
        anyhow::ensure!(
            declare_tree(&temporary.join("workspace"), &workspace_entries)? == manifest.workspace,
            "Session Bundle Workspace declaration does not match its contents"
        );
        Ok(manifest)
    })();
    let manifest = match restore_result {
        Ok(manifest) => manifest,
        Err(error) => {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
    };
    fs::rename(&temporary, destination_root).context("install restored Session workspace")?;
    File::open(parent)
        .context("open Session Bundle restore parent")?
        .sync_all()
        .context("sync Session Bundle restore parent")?;
    Ok(manifest)
}

fn validate_relative_path(path: &Path) -> Result<()> {
    anyhow::ensure!(
        !path.as_os_str().is_empty(),
        "Bundle path must not be empty"
    );
    for component in path.components() {
        anyhow::ensure!(
            matches!(component, Component::Normal(_)),
            "Session Bundle path is absolute or contains traversal"
        );
    }
    Ok(())
}

fn validate_symlink_target(link_path: &Path, target: &Path) -> Result<()> {
    anyhow::ensure!(
        !target.as_os_str().is_empty() && !target.is_absolute(),
        "Session Bundle symbolic link target must be relative"
    );
    let _ = resolve_symlink_path(link_path, target)?;
    Ok(())
}

fn resolve_symlink_path(link_path: &Path, target: &Path) -> Result<PathBuf> {
    let mut normalized = Vec::new();
    for component in link_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .components()
        .chain(target.components())
    {
        match component {
            Component::Normal(value) => normalized.push(value.to_owned()),
            Component::CurDir => {}
            Component::ParentDir => {
                anyhow::ensure!(
                    normalized.pop().is_some(),
                    "Session Bundle symbolic link escapes its root"
                );
            }
            Component::RootDir | Component::Prefix(_) => {
                anyhow::bail!("Session Bundle symbolic link target must be relative")
            }
        }
    }
    anyhow::ensure!(
        !normalized.is_empty(),
        "Session Bundle symbolic link escapes its root"
    );
    Ok(normalized.into_iter().collect())
}

fn validate_sha256(value: &str) -> Result<()> {
    anyhow::ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "Session Bundle checksum must be lowercase SHA-256 hex"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        append_entries, append_virtual_directory, checksum_file, collect_tree_entries,
        create_session_bundle, declare_tree, restore_session_workspace_only, BundleTreeDeclaration,
        SessionBundleCreateSpec, SessionBundleManifest, BUNDLE_FORMAT_VERSION,
    };
    use chrono::{TimeZone, Utc};
    use sha2::Digest;
    use std::{fs, path::Path};
    use uuid::Uuid;

    fn write_single_entry_bundle(
        archive_path: &Path,
        entry_path: &[u8],
        entry_type: tar::EntryType,
        link_name: Option<&str>,
        contents: &[u8],
    ) -> (String, u64) {
        assert!(entry_path.len() < 100);
        let file = fs::File::create(archive_path).unwrap();
        let encoder = zstd::Encoder::new(file, 1).unwrap();
        let mut archive = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(entry_type);
        header.set_mode(0o600);
        header.set_size(contents.len() as u64);
        if let Some(link_name) = link_name {
            header.set_link_name(link_name).unwrap();
        }
        let name = &mut header.as_mut_bytes()[..100];
        name.fill(0);
        name[..entry_path.len()].copy_from_slice(entry_path);
        header.set_cksum();
        archive.append(&header, contents).unwrap();
        archive.into_inner().unwrap().finish().unwrap();
        checksum_file(archive_path).unwrap()
    }

    fn write_workspace_only_bundle(
        archive_path: &Path,
        workspace: &Path,
        manifest: &SessionBundleManifest,
    ) -> (String, u64) {
        let workspace_entries = collect_tree_entries(workspace, |_, _| true).unwrap();
        assert_eq!(
            declare_tree(workspace, &workspace_entries).unwrap(),
            manifest.workspace
        );
        let file = fs::File::create(archive_path).unwrap();
        let encoder = zstd::Encoder::new(file, 1).unwrap();
        let mut archive = tar::Builder::new(encoder);
        archive.append_dir("workspace", workspace).unwrap();
        append_entries(&mut archive, &workspace_entries, Path::new("workspace")).unwrap();
        let manifest_bytes = serde_json::to_vec(manifest).unwrap();
        let mut manifest_header = tar::Header::new_gnu();
        manifest_header.set_entry_type(tar::EntryType::Regular);
        manifest_header.set_mode(0o600);
        manifest_header.set_size(manifest_bytes.len() as u64);
        manifest_header.set_cksum();
        archive
            .append_data(
                &mut manifest_header,
                "manifest.json",
                manifest_bytes.as_slice(),
            )
            .unwrap();
        archive.into_inner().unwrap().finish().unwrap();
        checksum_file(archive_path).unwrap()
    }

    #[test]
    fn session_bundle_round_trip_preserves_only_the_workspace_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(workspace.join("nested")).unwrap();
        fs::write(workspace.join("result.txt"), "saved\n").unwrap();
        fs::write(workspace.join("nested/keep.txt"), "keep\n").unwrap();
        let session_id = Uuid::new_v4();
        let created_at = Utc.with_ymd_and_hms(2026, 7, 16, 1, 2, 3).unwrap();
        let archive_path = temp.path().join("staging/session.tar.zst");
        let artifact = create_session_bundle(&SessionBundleCreateSpec {
            session_id,
            history_checkpoint: 12,
            bundle_generation: 3,
            ownership_generation: 7,
            producing_engine_version: "0.81.1".into(),
            created_at,
            workspace: workspace.clone(),
            archive_path: archive_path.clone(),
            force_stop_snapshot: false,
        })
        .unwrap();

        // 归档只含 workspace + manifest：无 native-session/engine-state 顶层。
        let file = fs::File::open(&archive_path).unwrap();
        let decoder = zstd::Decoder::new(file).unwrap();
        let mut archive = tar::Archive::new(decoder);
        let tops: std::collections::BTreeSet<String> = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .components()
                    .next()
                    .unwrap()
                    .as_os_str()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(
            tops,
            ["manifest.json".to_string(), "workspace".to_string()]
                .into_iter()
                .collect()
        );

        let destination = temp.path().join("restored");
        let manifest = restore_session_workspace_only(
            &archive_path,
            &artifact.checksum_sha256,
            artifact.size_bytes,
            session_id,
            12,
            &destination,
            false,
        )
        .unwrap();
        assert_eq!(manifest.session_id, session_id);
        assert_eq!(
            fs::read_to_string(destination.join("workspace/result.txt")).unwrap(),
            "saved\n"
        );
        assert_eq!(
            fs::read_to_string(destination.join("workspace/nested/keep.txt")).unwrap(),
            "keep\n"
        );
        assert!(!destination.join("engine-state").exists());
    }

    #[test]
    fn session_bundle_workspace_only_rejects_unexpected_top_level_entries() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("bad.tar.zst");
        write_single_entry_bundle(
            &archive_path,
            b"native-session/x.jsonl",
            tar::EntryType::Regular,
            None,
            b"old pi state",
        );
        let (checksum, size) = checksum_file(&archive_path).unwrap();
        let error = restore_session_workspace_only(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("unexpected top-level entry"),
            "unexpected top-level must be rejected: {error}"
        );
        assert!(!temp.path().join("out").exists());
        // 解包后失败必须清理 staging 目录。
        let leftovers: Vec<_> = std::fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".restore-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "failed restore must clean staging: {leftovers:?}"
        );
    }

    #[test]
    fn session_bundle_workspace_only_rejects_path_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("traversal.tar.zst");
        write_single_entry_bundle(
            &archive_path,
            b"workspace/../escape.txt",
            tar::EntryType::Regular,
            None,
            b"escape",
        );
        let (checksum, size) = checksum_file(&archive_path).unwrap();
        let error = restore_session_workspace_only(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(!temp.path().join("out").exists());
        assert!(!temp.path().join("escape.txt").exists());
    }

    #[test]
    fn session_bundle_workspace_only_rejects_escaping_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("symlink.tar.zst");
        write_single_entry_bundle(
            &archive_path,
            b"workspace/link",
            tar::EntryType::Symlink,
            Some("../native-session"),
            b"",
        );
        let (checksum, size) = checksum_file(&archive_path).unwrap();
        let error = restore_session_workspace_only(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("resolve inside workspace"),
            "escaping symlink must be rejected: {error}"
        );
    }

    #[test]
    fn session_bundle_workspace_only_rejects_special_tar_entries() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("special.tar.zst");
        write_single_entry_bundle(
            &archive_path,
            b"workspace/fifo",
            tar::EntryType::Fifo,
            None,
            b"",
        );
        let (checksum, size) = checksum_file(&archive_path).unwrap();
        let error = restore_session_workspace_only(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(!temp.path().join("out").exists());
        assert!(
            error.to_string().contains("unsupported entry type"),
            "special entry must be rejected: {error}"
        );
    }

    #[test]
    fn session_bundle_workspace_only_rejects_checksum_and_size_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(workspace.join("f.txt"), "x\n").unwrap();
        let session_id = Uuid::new_v4();
        let created_at = Utc.with_ymd_and_hms(2026, 7, 16, 1, 2, 3).unwrap();
        let archive_path = temp.path().join("bundle.tar.zst");
        let artifact = create_session_bundle(&SessionBundleCreateSpec {
            session_id,
            history_checkpoint: 0,
            bundle_generation: 1,
            ownership_generation: 1,
            producing_engine_version: "0.81.1".into(),
            created_at,
            workspace: workspace.clone(),
            archive_path: archive_path.clone(),
            force_stop_snapshot: false,
        })
        .unwrap();
        // checksum 不匹配。
        let wrong_checksum = "0".repeat(64);
        let error = restore_session_workspace_only(
            &archive_path,
            &wrong_checksum,
            artifact.size_bytes,
            session_id,
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(!temp.path().join("out").exists());
        assert!(
            error.to_string().contains("checksum"),
            "checksum mismatch must be rejected: {error}"
        );
        // size 不匹配。
        let error = restore_session_workspace_only(
            &archive_path,
            &artifact.checksum_sha256,
            artifact.size_bytes + 1,
            session_id,
            0,
            &temp.path().join("out"),
            false,
        )
        .unwrap_err();
        assert!(!temp.path().join("out").exists());
        assert!(
            error.to_string().contains("size"),
            "size mismatch must be rejected: {error}"
        );
        // 失败路径不留 staging 目录。
        let leftovers: Vec<_> = std::fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(".restore-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "failed restore must clean staging: {leftovers:?}"
        );
    }
}
