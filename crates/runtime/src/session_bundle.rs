use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufRead, BufReader, Read, Write},
    path::{Component, Path, PathBuf},
};
use tar::{Archive, Builder, EntryType, Header};
use uuid::Uuid;
use walkdir::WalkDir;

const BUNDLE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub(crate) struct SessionBundleCreateSpec {
    pub session_id: Uuid,
    pub native_session_id: String,
    pub history_checkpoint: i64,
    pub bundle_generation: i64,
    pub ownership_generation: i64,
    pub producing_engine_version: String,
    pub created_at: DateTime<Utc>,
    pub workspace: PathBuf,
    pub engine_state_root: PathBuf,
    pub archive_path: PathBuf,
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
    pub native_session_id: String,
    pub history_checkpoint: i64,
    pub bundle_generation: i64,
    pub ownership_generation: i64,
    pub producing_engine_version: String,
    pub created_at: DateTime<Utc>,
    pub workspace: BundleTreeDeclaration,
    pub native_session: BundleTreeDeclaration,
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
    let native_session_entries =
        collect_pi_session_entries(&spec.engine_state_root, &spec.native_session_id)?;
    let manifest = SessionBundleManifest {
        format_version: BUNDLE_FORMAT_VERSION,
        session_id: spec.session_id,
        native_session_id: spec.native_session_id.clone(),
        history_checkpoint: spec.history_checkpoint,
        bundle_generation: spec.bundle_generation,
        ownership_generation: spec.ownership_generation,
        producing_engine_version: spec.producing_engine_version.trim().to_owned(),
        created_at: spec.created_at,
        workspace: declare_tree(&spec.workspace, &workspace_entries)?,
        native_session: declare_tree(&spec.engine_state_root, &native_session_entries)?,
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

        append_virtual_directory(&mut archive, "native-session", spec.created_at)?;
        append_entries(
            &mut archive,
            &native_session_entries,
            Path::new("native-session"),
        )?;
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

pub(crate) fn restore_session_bundle(
    archive_path: &Path,
    expected_checksum_sha256: &str,
    expected_size_bytes: u64,
    expected_session_id: Uuid,
    expected_history_checkpoint: i64,
    destination_root: &Path,
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
    let restore_result = restore_archive_into(archive_path, &temporary).and_then(|manifest| {
        anyhow::ensure!(
            manifest.format_version == BUNDLE_FORMAT_VERSION,
            "unsupported Session Bundle format version"
        );
        anyhow::ensure!(
            manifest.session_id == expected_session_id,
            "Session Bundle belongs to a different Session"
        );
        anyhow::ensure!(
            manifest.history_checkpoint == expected_history_checkpoint,
            "Session Bundle history checkpoint does not match Hub metadata"
        );
        let workspace_entries = collect_tree_entries(&temporary.join("workspace"), |_, _| true)?;
        let native_session_entries =
            collect_tree_entries(&temporary.join("engine-state"), |_, _| true)?;
        anyhow::ensure!(
            declare_tree(&temporary.join("workspace"), &workspace_entries)? == manifest.workspace,
            "Session Bundle Workspace declaration does not match its contents"
        );
        anyhow::ensure!(
            declare_tree(&temporary.join("engine-state"), &native_session_entries)?
                == manifest.native_session,
            "Session Bundle Native Session declaration does not match its contents"
        );
        Ok(manifest)
    });
    let manifest = match restore_result {
        Ok(manifest) => manifest,
        Err(error) => {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
    };
    fs::rename(&temporary, destination_root).context("commit restored Session Bundle")?;
    File::open(parent)
        .context("open Session Bundle restore parent")?
        .sync_all()
        .context("sync Session Bundle restore parent")?;
    Ok(manifest)
}

fn validate_create_spec(spec: &SessionBundleCreateSpec) -> Result<()> {
    anyhow::ensure!(!spec.session_id.is_nil(), "Session id must not be nil");
    anyhow::ensure!(
        !spec.native_session_id.trim().is_empty(),
        "native Session id must not be empty"
    );
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
    anyhow::ensure!(
        spec.workspace.is_dir(),
        "Workspace directory is unavailable"
    );
    anyhow::ensure!(
        spec.engine_state_root.is_dir(),
        "Engine state root directory is unavailable"
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

fn collect_pi_session_entries(
    pi_home: &Path,
    expected_native_session_id: &str,
) -> Result<Vec<BundleSourceEntry>> {
    let session_dir = pi_home.join("sessions");
    anyhow::ensure!(
        fs::symlink_metadata(&session_dir)
            .context("inspect Pi Session directory")?
            .file_type()
            .is_dir(),
        "Pi Session directory is unavailable"
    );
    let mut candidates = fs::read_dir(&session_dir)
        .context("read Pi Session directory")?
        .collect::<std::io::Result<Vec<_>>>()?;
    candidates.sort_by_key(|entry| entry.file_name());

    let mut matched = None;
    for entry in candidates {
        let file_type = entry.file_type().context("inspect Pi Session entry")?;
        if !file_type.is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("jsonl")
        {
            continue;
        }
        if read_pi_session_id(&entry.path())?.as_deref() != Some(expected_native_session_id) {
            continue;
        }
        anyhow::ensure!(
            matched.is_none(),
            "multiple Pi Session files have the same id"
        );
        matched = Some(entry.path());
    }

    let matched = matched.context("Pi Session recovery file was not found")?;
    let relative = matched
        .strip_prefix(pi_home)
        .context("Pi Session recovery file escaped its home")?
        .to_path_buf();
    validate_relative_path(&relative)?;
    Ok(vec![
        BundleSourceEntry {
            source: session_dir,
            relative: PathBuf::from("sessions"),
        },
        BundleSourceEntry {
            source: matched,
            relative,
        },
    ])
}

fn read_pi_session_id(path: &Path) -> Result<Option<String>> {
    let file = File::open(path).context("open Pi Session candidate")?;
    let first_line = BufReader::new(file)
        .lines()
        .next()
        .transpose()
        .context("read Pi Session header")?
        .context("Pi Session candidate is empty")?;
    let header: serde_json::Value =
        serde_json::from_str(&first_line).context("parse Pi Session header")?;
    if header.get("type").and_then(serde_json::Value::as_str) != Some("session") {
        return Ok(None);
    }
    Ok(header
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned))
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

fn restore_archive_into(archive_path: &Path, destination: &Path) -> Result<SessionBundleManifest> {
    let file = File::open(archive_path).context("open Session Bundle for restore")?;
    let decoder = zstd::Decoder::new(file).context("decode Session Bundle zstd stream")?;
    let mut archive = Archive::new(decoder);
    let mut seen = BTreeSet::new();
    let mut symlinks = BTreeSet::new();
    let mut manifest = None;
    let mut saw_workspace_root = false;
    let mut saw_native_session_root = false;
    let mut saw_pi_sessions_root = false;
    let mut pi_session_file = None;
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
            entry_type.is_dir() || entry_type.is_file() || entry_type.is_symlink(),
            "Session Bundle contains a special or linked file type"
        );
        if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .context("read Session Bundle symbolic link")?
                .context("Session Bundle symbolic link has no target")?;
            validate_symlink_target(&archive_path, &target)?;
            let resolved_top = resolve_symlink_path(&archive_path, &target)?
                .components()
                .next()
                .context("Session Bundle symbolic link has no resolved path")?
                .as_os_str()
                .to_owned();
            anyhow::ensure!(
                resolved_top == top,
                "Session Bundle symbolic link crosses a top-level boundary"
            );
            symlinks.insert(archive_path.clone());
        }
        if top == "manifest.json" {
            anyhow::ensure!(
                archive_path == Path::new("manifest.json") && entry_type.is_file(),
                "Session Bundle manifest must be one regular top-level file"
            );
            anyhow::ensure!(manifest.is_none(), "Session Bundle has multiple manifests");
            manifest =
                Some(serde_json::from_reader(&mut entry).context("parse Session Bundle manifest")?);
            continue;
        }
        let destination_path = if top == "workspace" {
            if archive_path == Path::new("workspace") {
                anyhow::ensure!(
                    entry_type.is_dir(),
                    "Session Bundle workspace/ must be a directory"
                );
                saw_workspace_root = true;
            }
            destination.join("workspace").join(components.as_path())
        } else if top == "native-session" {
            if archive_path == Path::new("native-session") {
                anyhow::ensure!(
                    entry_type.is_dir(),
                    "Session Bundle native-session/ must be a directory"
                );
                saw_native_session_root = true;
            } else if archive_path == Path::new("native-session/sessions") {
                anyhow::ensure!(
                    entry_type.is_dir(),
                    "Session Bundle Pi sessions/ must be a directory"
                );
                saw_pi_sessions_root = true;
            } else {
                let relative = archive_path
                    .strip_prefix("native-session")
                    .context("read Pi recovery path")?;
                anyhow::ensure!(
                    entry_type.is_file()
                        && relative.parent() == Some(Path::new("sessions"))
                        && relative.extension().and_then(|value| value.to_str()) == Some("jsonl"),
                    "Session Bundle contains an unexpected Pi recovery path"
                );
                anyhow::ensure!(
                    pi_session_file.is_none(),
                    "Session Bundle contains multiple Pi recovery files"
                );
                pi_session_file = Some(destination.join("engine-state").join(relative));
            }
            destination.join("engine-state").join(components.as_path())
        } else {
            anyhow::bail!("Session Bundle contains an unexpected top-level entry");
        };
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent).context("create Session Bundle restore directory")?;
        }
        entry
            .unpack(&destination_path)
            .context("extract Session Bundle entry")?;
    }
    anyhow::ensure!(
        saw_workspace_root,
        "Session Bundle is missing workspace/ directory"
    );
    anyhow::ensure!(
        saw_native_session_root,
        "Session Bundle is missing native-session/ directory"
    );
    anyhow::ensure!(
        saw_pi_sessions_root,
        "Session Bundle is missing Pi sessions/ directory"
    );
    let pi_session_file = pi_session_file.context("Session Bundle is missing Pi recovery file")?;
    let manifest: SessionBundleManifest =
        manifest.context("Session Bundle is missing manifest.json")?;
    anyhow::ensure!(
        read_pi_session_id(&pi_session_file)?.as_deref() == Some(&manifest.native_session_id),
        "Session Bundle Pi recovery file does not match its native Session id"
    );
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
        create_session_bundle, declare_tree, restore_session_bundle, BundleTreeDeclaration,
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

    fn write_declared_bundle(
        archive_path: &Path,
        workspace: &Path,
        engine_state_root: &Path,
        manifest: &SessionBundleManifest,
    ) -> (String, u64) {
        let workspace_entries = collect_tree_entries(workspace, |_, _| true).unwrap();
        let native_session_entries = collect_tree_entries(engine_state_root, |_, _| true).unwrap();
        assert_eq!(
            declare_tree(workspace, &workspace_entries).unwrap(),
            manifest.workspace
        );
        assert_eq!(
            declare_tree(engine_state_root, &native_session_entries).unwrap(),
            manifest.native_session
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

        append_virtual_directory(&mut archive, "native-session", manifest.created_at).unwrap();
        append_entries(
            &mut archive,
            &native_session_entries,
            Path::new("native-session"),
        )
        .unwrap();
        archive.into_inner().unwrap().finish().unwrap();
        checksum_file(archive_path).unwrap()
    }

    #[test]
    fn session_bundle_round_trip_preserves_workspace_and_only_native_session_recovery_data() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let workspace = source.join("workspace");
        let engine_state_root = source.join("engine-state");
        fs::create_dir_all(workspace.join(".git")).unwrap();
        fs::create_dir_all(engine_state_root.join("sessions")).unwrap();
        fs::create_dir_all(engine_state_root.join(".pi/agent/skills/private-skill")).unwrap();
        fs::create_dir_all(engine_state_root.join(".pi/agent/extensions")).unwrap();
        fs::create_dir_all(engine_state_root.join(".pi/agent/cache")).unwrap();
        fs::write(workspace.join("README.md"), "workspace\n").unwrap();
        fs::write(workspace.join(".hidden"), "hidden\n").unwrap();
        fs::write(workspace.join(".git/config"), "[core]\n").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("README.md", workspace.join("readme-link")).unwrap();

        let native_session_id = "019bf9b2-7a4d-7000-8000-000000000001";
        fs::write(
            engine_state_root.join("sessions/pi-session.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .unwrap();
        fs::write(
            engine_state_root.join(format!("sessions/decoy-{native_session_id}.jsonl")),
            "{\"type\":\"session\",\"id\":\"another-session\"}\n",
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/models.json"),
            "model proxy token\n",
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/auth.json"),
            "provider secret\n",
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/settings.json"),
            "settings\n",
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/skills/private-skill/SKILL.md"),
            "regenerated skill\n",
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/extensions/provider.ts"),
            "generated extension\n",
        )
        .unwrap();
        fs::write(engine_state_root.join(".pi/agent/cache/data"), "cache\n").unwrap();

        let session_id = Uuid::new_v4();
        let archive = temp.path().join("staging/session.tar.zst");
        let artifact = create_session_bundle(&SessionBundleCreateSpec {
            session_id,
            native_session_id: native_session_id.to_owned(),
            history_checkpoint: 12,
            bundle_generation: 3,
            ownership_generation: 7,
            producing_engine_version: "0.81.1".into(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 16, 1, 2, 3).unwrap(),
            workspace: workspace.clone(),
            engine_state_root: engine_state_root.clone(),
            archive_path: archive.clone(),
        })
        .unwrap();

        assert_eq!(artifact.archive_path, archive);
        assert_eq!(artifact.checksum_sha256.len(), 64);
        assert_eq!(artifact.size_bytes, fs::metadata(&archive).unwrap().len());
        let restored = temp.path().join("restored");
        let manifest = restore_session_bundle(
            &archive,
            &artifact.checksum_sha256,
            artifact.size_bytes,
            session_id,
            12,
            &restored,
        )
        .unwrap();

        assert_eq!(manifest.session_id, session_id);
        assert_eq!(manifest.native_session_id, native_session_id);
        assert_eq!(
            fs::read_to_string(restored.join("workspace/README.md")).unwrap(),
            "workspace\n"
        );
        assert_eq!(
            fs::read_to_string(restored.join("workspace/.hidden")).unwrap(),
            "hidden\n"
        );
        assert_eq!(
            fs::read_to_string(restored.join("workspace/.git/config")).unwrap(),
            "[core]\n"
        );
        #[cfg(unix)]
        assert_eq!(
            fs::read_link(restored.join("workspace/readme-link")).unwrap(),
            std::path::PathBuf::from("README.md")
        );
        assert!(restored
            .join("engine-state/sessions/pi-session.jsonl")
            .is_file());
        assert!(!restored
            .join(format!(
                "engine-state/sessions/decoy-{native_session_id}.jsonl"
            ))
            .exists());
        assert!(!restored.join("engine-state/.pi").exists());
    }

    #[test]
    fn session_bundle_restore_rejects_regenerable_pi_state_even_when_declared() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("source/workspace");
        let engine_state_root = temp.path().join("source/engine-state");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(engine_state_root.join("sessions")).unwrap();
        fs::create_dir_all(engine_state_root.join(".pi/agent")).unwrap();
        fs::write(workspace.join("result.txt"), "workspace\n").unwrap();
        let session_id = Uuid::new_v4();
        let native_session_id = "pi-native-session";
        fs::write(
            engine_state_root.join("sessions/recovery.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .unwrap();
        fs::write(
            engine_state_root.join(".pi/agent/models.json"),
            "must never restore\n",
        )
        .unwrap();

        let workspace_entries = collect_tree_entries(&workspace, |_, _| true).unwrap();
        let native_session_entries = collect_tree_entries(&engine_state_root, |_, _| true).unwrap();
        let manifest = SessionBundleManifest {
            format_version: BUNDLE_FORMAT_VERSION,
            session_id,
            native_session_id: native_session_id.into(),
            history_checkpoint: 9,
            bundle_generation: 2,
            ownership_generation: 3,
            producing_engine_version: "0.81.1".into(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 23, 1, 2, 3).unwrap(),
            workspace: declare_tree(&workspace, &workspace_entries).unwrap(),
            native_session: declare_tree(&engine_state_root, &native_session_entries).unwrap(),
        };
        let archive_path = temp.path().join("declared-secret.tar.zst");
        let (checksum, size) =
            write_declared_bundle(&archive_path, &workspace, &engine_state_root, &manifest);
        let destination = temp.path().join("restored");

        let error =
            restore_session_bundle(&archive_path, &checksum, size, session_id, 9, &destination)
                .expect_err("regenerable Pi state must be rejected during restore");

        assert!(error.to_string().contains("Pi recovery path"));
        assert!(!destination.exists());
    }

    #[test]
    fn session_bundle_restore_rejects_a_workspace_file_disguised_as_the_top_level_directory() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("malicious.tar.zst");
        let session_id = Uuid::new_v4();
        let empty_tree = BundleTreeDeclaration {
            entry_count: 0,
            size_bytes: 0,
            checksum_sha256: format!("{:x}", sha2::Sha256::digest([])),
        };
        let manifest = SessionBundleManifest {
            format_version: BUNDLE_FORMAT_VERSION,
            session_id,
            native_session_id: "thread-malicious".into(),
            history_checkpoint: 1,
            bundle_generation: 1,
            ownership_generation: 1,
            producing_engine_version: "0.104.0".into(),
            created_at: Utc.with_ymd_and_hms(2026, 7, 16, 1, 2, 3).unwrap(),
            workspace: empty_tree.clone(),
            native_session: empty_tree,
        };
        let file = fs::File::create(&archive_path).unwrap();
        let encoder = zstd::Encoder::new(file, 1).unwrap();
        let mut archive = tar::Builder::new(encoder);
        let mut workspace_header = tar::Header::new_gnu();
        workspace_header.set_entry_type(tar::EntryType::Regular);
        workspace_header.set_mode(0o600);
        workspace_header.set_size(0);
        workspace_header.set_cksum();
        archive
            .append_data(&mut workspace_header, "workspace", std::io::empty())
            .unwrap();
        let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
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
        let mut native_session_header = tar::Header::new_gnu();
        native_session_header.set_entry_type(tar::EntryType::Directory);
        native_session_header.set_mode(0o700);
        native_session_header.set_size(0);
        native_session_header.set_cksum();
        archive
            .append_data(
                &mut native_session_header,
                "native-session",
                std::io::empty(),
            )
            .unwrap();
        archive.into_inner().unwrap().finish().unwrap();
        let (checksum, size) = checksum_file(&archive_path).unwrap();

        let error = restore_session_bundle(
            &archive_path,
            &checksum,
            size,
            session_id,
            1,
            &temp.path().join("restored"),
        )
        .expect_err("workspace must be a top-level directory");

        assert!(error.to_string().contains("workspace/ must be a directory"));
    }

    #[test]
    fn session_bundle_restore_rejects_path_traversal_without_writing_output() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("traversal.tar.zst");
        let (checksum, size) = write_single_entry_bundle(
            &archive_path,
            b"workspace/../../escaped.txt",
            tar::EntryType::Regular,
            None,
            b"escaped",
        );
        let destination = temp.path().join("restored");

        restore_session_bundle(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            1,
            &destination,
        )
        .expect_err("path traversal must be rejected");

        assert!(!destination.exists());
        assert!(!temp.path().join("escaped.txt").exists());
    }

    #[test]
    fn session_bundle_restore_rejects_escaping_symlink_without_writing_output() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("escaping-symlink.tar.zst");
        let (checksum, size) = write_single_entry_bundle(
            &archive_path,
            b"workspace/escape",
            tar::EntryType::Symlink,
            Some("../../outside"),
            b"",
        );
        let destination = temp.path().join("restored");

        restore_session_bundle(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            1,
            &destination,
        )
        .expect_err("escaping symbolic link must be rejected");

        assert!(!destination.exists());
    }

    #[test]
    fn session_bundle_restore_rejects_special_tar_entry_without_writing_output() {
        let temp = tempfile::tempdir().unwrap();
        let archive_path = temp.path().join("special-entry.tar.zst");
        let (checksum, size) = write_single_entry_bundle(
            &archive_path,
            b"workspace/pipe",
            tar::EntryType::Fifo,
            None,
            b"",
        );
        let destination = temp.path().join("restored");

        restore_session_bundle(
            &archive_path,
            &checksum,
            size,
            Uuid::new_v4(),
            1,
            &destination,
        )
        .expect_err("special tar entry must be rejected");

        assert!(!destination.exists());
    }

    #[cfg(unix)]
    #[test]
    fn session_bundle_creation_rejects_escaping_symlinks_and_special_files() {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let engine_state_root = temp.path().join("engine-state");
        let native_session_id = "019bf9b2-7a4d-7000-8000-000000000002";
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(engine_state_root.join("sessions")).unwrap();
        fs::write(
            engine_state_root.join("sessions/recovery.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .unwrap();
        std::os::unix::fs::symlink("../../outside", workspace.join("escape")).unwrap();
        let spec = SessionBundleCreateSpec {
            session_id: Uuid::new_v4(),
            native_session_id: native_session_id.into(),
            history_checkpoint: 1,
            bundle_generation: 1,
            ownership_generation: 1,
            producing_engine_version: "0.104.0".into(),
            created_at: Utc::now(),
            workspace: workspace.clone(),
            engine_state_root: engine_state_root.clone(),
            archive_path: temp.path().join("escape.tar.zst"),
        };
        let error = create_session_bundle(&spec).expect_err("escaping link must be rejected");
        assert!(error.to_string().contains("escapes its root"));

        fs::remove_file(workspace.join("escape")).unwrap();
        let fifo = workspace.join("pipe");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        let error = create_session_bundle(&SessionBundleCreateSpec {
            archive_path: temp.path().join("fifo.tar.zst"),
            ..spec
        })
        .expect_err("FIFO must be rejected");
        assert!(error.to_string().contains("special file"));
    }

    #[test]
    fn session_bundle_restore_rejects_compressed_size_and_checksum_mismatch_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let engine_state_root = temp.path().join("engine-state");
        let native_session_id = "019bf9b2-7a4d-7000-8000-000000000003";
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(engine_state_root.join("sessions")).unwrap();
        fs::write(workspace.join("file.txt"), "content").unwrap();
        fs::write(
            engine_state_root.join("sessions/recovery.jsonl"),
            format!("{{\"type\":\"session\",\"id\":\"{native_session_id}\"}}\n"),
        )
        .unwrap();
        let session_id = Uuid::new_v4();
        let artifact = create_session_bundle(&SessionBundleCreateSpec {
            session_id,
            native_session_id: native_session_id.into(),
            history_checkpoint: 4,
            bundle_generation: 1,
            ownership_generation: 2,
            producing_engine_version: "0.104.0".into(),
            created_at: Utc::now(),
            workspace,
            engine_state_root,
            archive_path: temp.path().join("bundle.tar.zst"),
        })
        .unwrap();
        let wrong_size_destination = temp.path().join("wrong-size");
        let error = restore_session_bundle(
            &artifact.archive_path,
            &artifact.checksum_sha256,
            artifact.size_bytes + 1,
            session_id,
            4,
            &wrong_size_destination,
        )
        .expect_err("wrong compressed size must be rejected");
        assert!(error.to_string().contains("size does not match"));
        assert!(!wrong_size_destination.exists());

        let wrong_checksum_destination = temp.path().join("wrong-checksum");
        let error = restore_session_bundle(
            &artifact.archive_path,
            &"0".repeat(64),
            artifact.size_bytes,
            session_id,
            4,
            &wrong_checksum_destination,
        )
        .expect_err("wrong checksum must be rejected");
        assert!(error.to_string().contains("checksum does not match"));
        assert!(!wrong_checksum_destination.exists());
    }
}
